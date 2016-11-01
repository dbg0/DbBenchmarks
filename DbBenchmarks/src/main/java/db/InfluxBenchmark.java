package db;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
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

@Warmup(iterations = 10)
@Measurement(iterations = 10)
@Fork(1)
@State(Scope.Benchmark)
public class InfluxBenchmark {
	private static final String DB = "InfluxBenchmarks";
	private static final String MEASUREMENT = "Measurement1";

	@State(Scope.Thread)
	@AuxCounters
	public static class Counters {
		public int updates;
		public int updatedValues;
	}

	public static class Populate {

		@Setup
		public void setup(InfluxBenchmark b) {
			int c = 0;
			while (c < Config.POPULATE_ROWS) {
				int batchSize = Math.min(Config.POPULATE_ROWS_PER_BATCH, Config.POPULATE_ROWS - c);
				insert(b.client, batchSize, b.valuesPerRow);
				c += batchSize;
			}
		}
	}

	@Param(value = { "1", "100", "1000" })
	private int rowsPerBatch;

	@Param(value = { "1", "10", "100" })
	private int valuesPerRow;

	@Param(value = { "0", "1", "10" })
	private double indexedValuesRatio;

	InfluxDB client;

	@Setup
	public void setup() {
		client = InfluxDBFactory.connect("http://localhost:8086", "root", "root");

		client.deleteDatabase(DB);
		client.createDatabase(DB);
	}

	@TearDown
	public void tearDown() {
		client.deleteDatabase(DB);
	}

	@Benchmark
	public int insert(Counters c) {
		c.updatedValues += insert(client, rowsPerBatch, valuesPerRow);
		c.updates += rowsPerBatch;
		return c.updates;
	}

	private static int insert(InfluxDB client, int rowsPerBatch, int valuesPerRow) {
		BatchPoints batch = BatchPoints.database(DB).build();

		int rowValues = 0;
		for (int row = 0; row < rowsPerBatch; row++) {
			Point.Builder b = Point.measurement(MEASUREMENT);
			for (int field = 0; field < valuesPerRow; field++) {
				b.addField("field" + field, "value" + field);
			}
			rowValues += valuesPerRow;
			batch.point(b.build());
		}

		client.write(batch);
		return rowValues;
	}

	@Threads(2)
	@Benchmark
	public int insertT2(Counters c) {
		return insert(c);
	}

	@Threads(4)
	@Benchmark
	public int insertT4(Counters c) {
		return insert(c);
	}

	@Threads(8)
	@Benchmark
	public int insertT8(Counters c) {
		return insert(c);
	}
	


	public static void main(String[] args) throws RunnerException {
		Options opts = new OptionsBuilder().include("InfluxBenchmark").param("batchSize", "1").param("docSize", "1")
				.build();
		new Runner(opts).run();
	}
}
