package com.nano.tsdb.benchmark;

import com.nano.tsdb.core.EngineConfig;
import com.nano.tsdb.core.NanoDB;
import com.nano.tsdb.core.StorageEngine;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for NanoTSDB write and read latency.
 * <p>
 * Measures:
 * - Write throughput (single-writer path)
 * - Point read latency (memtable + SSTable)
 * - Range scan throughput
 * - P99 tail latency for writes
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class LatencyBenchmark {

    private StorageEngine engine;
    private Path tempDir;
    private long timestampCounter;

    @Param({"1000", "10000"})
    private int dataPoints;

    @Setup
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("nanotsdb-bench-");
        EngineConfig config = EngineConfig.builder()
                .dataDir(tempDir.resolve("data"))
                .walDir(tempDir.resolve("wal"))
                .memtableMaxSizeBytes(4 * 1024 * 1024) // 4MB
                .fsyncPolicy(EngineConfig.FsyncPolicy.NONE) // no fsync for benchmark
                .build();
        engine = NanoDB.open(config);
        timestampCounter = System.currentTimeMillis();
    }

    @TearDown
    public void tearDown() throws IOException {
        if (engine != null) {
            engine.shutdown();
        }
        if (tempDir != null) {
            try (var paths = Files.walk(tempDir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException ignored) {
                            }
                        });
            }
        }
    }

    @Benchmark
    @Group("writes")
    @GroupThreads(1)
    public void writeSingle(Blackhole bh) throws IOException {
        long ts = timestampCounter++;
        engine.write("bench.cpu", ts, 42.5 + (ts % 100));
        bh.consume(ts);
    }

    @Benchmark
    @Group("writes")
    @GroupThreads(1)
    public void writeMultiSeries(Blackhole bh) throws IOException {
        long ts = timestampCounter++;
        String series = "bench.series." + (ts % 10);
        engine.write(series, ts, 72.0 + (ts % 50));
        bh.consume(ts);
    }

    @Benchmark
    public Double readPoint() throws IOException {
        long ts = timestampCounter > 100 ? timestampCounter - 1 - (long) (Math.random() * 100) : 0;
        return engine.read("bench.cpu", ts);
    }

    @Benchmark
    public Object scanRange() throws IOException {
        long now = timestampCounter;
        return engine.scan("bench.cpu", Math.max(0, now - 1000), now);
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(2)
    public void mixedWrite(Blackhole bh) throws IOException {
        long ts = timestampCounter++;
        engine.write("bench.mixed", ts, 50.0);
        bh.consume(ts);
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(1)
    public Double mixedRead() throws IOException {
        long ts = timestampCounter > 10 ? timestampCounter - 1 : 0;
        return engine.read("bench.mixed", ts);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LatencyBenchmark.class.getSimpleName())
                .shouldFailOnError(true)
                .build();
        new Runner(opt).run();
    }
}
