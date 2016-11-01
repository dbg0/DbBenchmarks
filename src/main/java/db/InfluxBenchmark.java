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

@Warmup(iterations = 5)
@Measurement(iterations = 5)
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

	@Param(value = { "1", "100", "1000" })
	private int rowsPerBatch;

	@Param(value = { "1", "10", "100" })
	private int valuesPerRow;

	@Param(value = { "0", "1", "10" })
	private double indexedValuesPct;

	private int indexedValues;

	InfluxDB client;

	@Setup
	public void setup() {
		client = InfluxDBFactory.connect("http://localhost:8086", "root", "root");

		client.deleteDatabase(DB);
		client.createDatabase(DB);

		indexedValues = (int) Math.round(Math.ceil(valuesPerRow * indexedValuesPct / 100.0));
	}

	@TearDown
	public void tearDown() {
		client.deleteDatabase(DB);
	}

	private int insertBatch(Counters c) {
		BatchPoints inserts = BatchPoints.database(DB).build();

		for (int row = 0; row < rowsPerBatch; row++) {
			Point.Builder insert = Point.measurement(MEASUREMENT);

			for (int tag = 0; tag < indexedValues; tag++) {
				insert.tag("tag" + tag, "value" + c.updates);
				c.updatedValues++;
			}

			for (int field = indexedValues; field < valuesPerRow; field++) {
				insert.addField("field" + field, "value" + field);
				c.updatedValues++;
			}
			inserts.point(insert.build());
			c.updates++;
		}

		client.write(inserts);
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

	public static void main(String[] args) throws RunnerException {
		Options opts = new OptionsBuilder().include(InfluxBenchmark.class.getSimpleName()).build();
		new Runner(opts).run();
	}
}
