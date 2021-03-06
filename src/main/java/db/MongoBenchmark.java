/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package db;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;

@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(1)
@State(Scope.Benchmark)
public class MongoBenchmark {
	private static final String DB = "MongoBenchmarks";
	private static final String COLLECTION = "BenchmarkCollection";
	private static final int COLLECTION_SIZE = 1000000;

	@State(Scope.Thread)
	@AuxCounters
	public static class Counters {
		public int updates;
		public int updatedValues;
	}

	private MongoClient client;
	private MongoDatabase db;
	private MongoCollection<Document> collection;

	@Param(value = { "1", "100", "1000" })
	private int rowsPerBatch;

	@Param(value = { "1", "10", "100" })
	private int valuesPerRow;

	@Param(value = { "1", "10", "50" })
	private int indexedValuesPct;

	private int indexedValues;

	@Setup
	public void setup() {
		client = new MongoClient();
		db = client.getDatabase(DB);
		collection = db.getCollection(COLLECTION);
		collection.drop();

		indexedValues = (int) Math.round(Math.ceil(valuesPerRow * indexedValuesPct / 100.0));
		for (int i = 0; i < indexedValues; i++) {
			// creates key in ascending order
			collection.createIndex(new Document("key" + i, 1));
		}
	}

	@TearDown
	public void tearDown() {
		collection.drop();
		client.close();
	}

	private int insertBatch(Counters c) {
		List<Document> inserts = new ArrayList<>(rowsPerBatch);

		for (int i = 0; i < rowsPerBatch; i++) {
			Document doc = new Document();

			for (int j = 0; j < valuesPerRow; j++) {
				doc.append("key" + j, "value" + c.updates);
				c.updatedValues++;
			}

			inserts.add(doc);
			c.updates++;
		}

		collection.insertMany(inserts);
		return c.updates;
	}

	private int upsertBatch(Counters c) {
		ThreadLocalRandom rand = ThreadLocalRandom.current();

		List<ReplaceOneModel<Document>> upserts = new ArrayList<>(rowsPerBatch);
		for (int i = 0; i < rowsPerBatch; i++) {
			int row = rand.nextInt(COLLECTION_SIZE);

			Document doc = new Document();

			List<Bson> indexList = new ArrayList<>(indexedValues);
			for (int k = 0; k < indexedValues; k++) {
				indexList.add(Filters.eq("key" + k, "value" + row));
				doc.append("key" + k, "value" + row);
				c.updatedValues++;
			}

			for (int j = indexedValues; j < valuesPerRow; j++) {
				doc.append("key" + j, "value" + j);
				c.updatedValues++;
			}

			upserts.add(new ReplaceOneModel<Document>(Filters.and(indexList), doc));
			c.updates++;
		}

		collection.bulkWrite(upserts);
		return c.updates;
	}

	@Threads(1)
	@Benchmark
	public int insert01T(Counters c) {
		return insertBatch(c);
	}

	@Threads(2)
	@Benchmark
	public int insert02T(Counters c) {
		return insertBatch(c);
	}

	@Threads(4)
	@Benchmark
	public int insert04T(Counters c) {
		return insertBatch(c);
	}

	@Threads(8)
	@Benchmark
	public int insert08T(Counters c) {
		return insertBatch(c);
	}

	@Threads(1)
	@Benchmark
	public int upsert01T(Counters c) {
		return upsertBatch(c);
	}

	@Threads(2)
	@Benchmark
	public int upsert02T(Counters c) {
		return upsertBatch(c);
	}

	@Threads(4)
	@Benchmark
	public int upsert04T(Counters c) {
		return upsertBatch(c);
	}

	@Threads(8)
	@Benchmark
	public int upsert08T(Counters c) {
		return upsertBatch(c);
	}

	public static void main(String[] args) throws RunnerException {
		Options opts = new OptionsBuilder().include(MongoBenchmark.class.getSimpleName()).build();
		new Runner(opts).run();
	}
}
