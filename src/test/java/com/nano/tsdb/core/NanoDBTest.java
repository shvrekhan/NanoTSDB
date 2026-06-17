package com.nano.tsdb.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the NanoDB engine facade.
 * Tests the full write → read → scan → flush → shutdown lifecycle.
 */
class NanoDBTest {

    @TempDir
    Path tempDir;

    @Test
    void testWriteAndRead() throws IOException {
        NanoDB engine = openEngine();
        engine.write("cpu.load", 1000, 0.75);
        Double val = engine.read("cpu.load", 1000);
        assertEquals(0.75, val, 1e-9);
        engine.shutdown();
    }

    @Test
    void testReadNonExistent() throws IOException {
        NanoDB engine = openEngine();
        assertNull(engine.read("nonexistent", 0));
        engine.shutdown();
    }

    @Test
    void testOverwrite() throws IOException {
        NanoDB engine = openEngine();
        engine.write("sensor.temp", 500, 25.0);
        engine.write("sensor.temp", 500, 30.0); // overwrite
        Double val = engine.read("sensor.temp", 500);
        assertEquals(30.0, val, 1e-9);
        engine.shutdown();
    }

    @Test
    void testScan() throws IOException {
        NanoDB engine = openEngine();
        for (int i = 0; i < 10; i++) {
            engine.write("scan.test", i, i * 10.0);
        }
        List<DataPoint> results = engine.scan("scan.test", 3, 7);
        assertEquals(5, results.size());
        assertEquals(30.0, results.get(0).value(), 1e-9);
        assertEquals(70.0, results.get(4).value(), 1e-9);
        engine.shutdown();
    }

    @Test
    void testScanEmptyRange() throws IOException {
        NanoDB engine = openEngine();
        engine.write("empty.test", 100, 1.0);
        List<DataPoint> results = engine.scan("empty.test", 200, 300);
        assertTrue(results.isEmpty());
        engine.shutdown();
    }

    @Test
    void testFlushAndRead() throws IOException {
        NanoDB engine = openEngine();
        engine.write("flush.test", 1, 100.0);
        engine.write("flush.test", 2, 200.0);
        engine.flush();

        // Data should still be readable after flush
        assertEquals(100.0, engine.read("flush.test", 1), 1e-9);
        assertEquals(200.0, engine.read("flush.test", 2), 1e-9);
        engine.shutdown();
    }

    @Test
    void testMultipleSeries() throws IOException {
        NanoDB engine = openEngine();
        engine.write("series.1", 10, 1.0);
        engine.write("series.2", 20, 2.0);
        engine.write("series.1", 30, 3.0);

        assertEquals(1.0, engine.read("series.1", 10), 1e-9);
        assertEquals(2.0, engine.read("series.2", 20), 1e-9);
        assertEquals(3.0, engine.read("series.1", 30), 1e-9);
        engine.shutdown();
    }

    @Test
    void testShutdownAndReopen() throws IOException {
        Path dataDir = tempDir.resolve("data");
        Path walDir = tempDir.resolve("wal");

        EngineConfig config = EngineConfig.builder()
                .dataDir(dataDir)
                .walDir(walDir)
                .build();

        NanoDB engine1 = NanoDB.open(config);
        engine1.write("durable", 42, 99.9);
        engine1.shutdown();

        NanoDB engine2 = NanoDB.open(config);
        assertEquals(99.9, engine2.read("durable", 42), 1e-9);
        engine2.shutdown();
    }

    @Test
    void testWriteAfterShutdownThrows() throws IOException {
        NanoDB engine = openEngine();
        engine.shutdown();
        assertThrows(IllegalStateException.class, () -> engine.write("fail", 0, 0.0));
    }

    @Test
    void testReadAfterShutdownThrows() throws IOException {
        NanoDB engine = openEngine();
        engine.shutdown();
        assertThrows(IllegalStateException.class, () -> engine.read("fail", 0));
    }

    @Test
    void testScanAfterShutdownThrows() throws IOException {
        NanoDB engine = openEngine();
        engine.shutdown();
        assertThrows(IllegalStateException.class, () -> engine.scan("fail", 0, 1));
    }

    @Test
    void testLargeNumberOfWrites() throws IOException {
        NanoDB engine = openEngine();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            engine.write("bulk.test", i, i * 1.0);
        }
        for (int i = 0; i < count; i++) {
            Double val = engine.read("bulk.test", i);
            assertNotNull(val, "Missing value at " + i);
            assertEquals(i * 1.0, val, 1e-9);
        }
        engine.shutdown();
    }

    @Test
    void testScanAcrossMultipleSources() throws IOException {
        NanoDB engine = openEngine();
        // Write enough to trigger a flush
        for (int i = 0; i < 500; i++) {
            engine.write("multi.test", i, i * 2.0);
        }
        engine.flush();

        // Write more data after flush (goes to new memtable)
        for (int i = 500; i < 600; i++) {
            engine.write("multi.test", i, i * 2.0);
        }

        // Scan across both memtable and SSTable
        List<DataPoint> results = engine.scan("multi.test", 490, 510);
        assertEquals(21, results.size());
        assertEquals(490 * 2.0, results.get(0).value(), 1e-9);
        assertEquals(510 * 2.0, results.get(20).value(), 1e-9);
        engine.shutdown();
    }

    private NanoDB openEngine() throws IOException {
        Path dataDir = tempDir.resolve("data");
        Path walDir = tempDir.resolve("wal");
        EngineConfig config = EngineConfig.builder()
                .dataDir(dataDir)
                .walDir(walDir)
                .memtableMaxSizeBytes(4 * 1024 * 1024)
                .fsyncPolicy(EngineConfig.FsyncPolicy.EVERY_WRITE)
                .build();
        return NanoDB.open(config);
    }
}
