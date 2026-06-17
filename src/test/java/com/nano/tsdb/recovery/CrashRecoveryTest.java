package com.nano.tsdb.recovery;

import com.nano.tsdb.core.EngineConfig;
import com.nano.tsdb.core.NanoDB;
import com.nano.tsdb.core.StorageEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Crash recovery tests for NanoTSDB.
 * <p>
 * Tests:
 * - Data survives engine restart (graceful shutdown + reopen)
 * - WAL replay rebuilds memtable correctly
 * - Multiple writes before restart are all recovered
 * - Reads work after recovery
 * - Scans work after recovery
 * - Empty engine restart (no data loss)
 * - Flush + restart (SSTable data survives)
 */
class CrashRecoveryTest {

    @TempDir
    Path tempDir;

    private Path dataDir;
    private Path walDir;

    @BeforeEach
    void setUp() {
        dataDir = tempDir.resolve("data");
        walDir = tempDir.resolve("wal");
    }

    @AfterEach
    void tearDown() throws IOException {
        // Cleanup handled by @TempDir
    }

    @Test
    void testDataSurvivesRestart() throws IOException {
        EngineConfig config = makeConfig();

        // Write data
        StorageEngine engine1 = NanoDB.open(config);
        engine1.write("cpu.load", 1000, 0.5);
        engine1.write("cpu.load", 1001, 0.6);
        engine1.write("cpu.load", 1002, 0.7);
        engine1.shutdown();

        // Reopen and verify
        StorageEngine engine2 = NanoDB.open(config);
        assertEquals(0.5, engine2.read("cpu.load", 1000), 1e-9);
        assertEquals(0.6, engine2.read("cpu.load", 1001), 1e-9);
        assertEquals(0.7, engine2.read("cpu.load", 1002), 1e-9);
        engine2.shutdown();
    }

    @Test
    void testMultipleSeriesSurviveRestart() throws IOException {
        EngineConfig config = makeConfig();

        StorageEngine engine = NanoDB.open(config);
        engine.write("series.a", 10, 1.0);
        engine.write("series.b", 20, 2.0);
        engine.write("series.c", 30, 3.0);
        engine.shutdown();

        StorageEngine recovered = NanoDB.open(config);
        assertEquals(1.0, recovered.read("series.a", 10), 1e-9);
        assertEquals(2.0, recovered.read("series.b", 20), 1e-9);
        assertEquals(3.0, recovered.read("series.c", 30), 1e-9);
        recovered.shutdown();
    }

    @Test
    void testScanAfterRestart() throws IOException {
        EngineConfig config = makeConfig();

        StorageEngine engine = NanoDB.open(config);
        for (int i = 0; i < 100; i++) {
            engine.write("scan.test", i, i * 1.5);
        }
        engine.shutdown();

        StorageEngine recovered = NanoDB.open(config);
        List<com.nano.tsdb.core.DataPoint> results = recovered.scan("scan.test", 10, 50);
        assertEquals(41, results.size()); // 10..50 inclusive
        assertEquals(10.0 * 1.5, results.get(0).value(), 1e-9);
        assertEquals(50.0 * 1.5, results.get(results.size() - 1).value(), 1e-9);
        recovered.shutdown();
    }

    @Test
    void testEmptyEngineRestart() throws IOException {
        EngineConfig config = makeConfig();

        StorageEngine engine = NanoDB.open(config);
        engine.shutdown();

        // Reopen — should work with no data
        StorageEngine recovered = NanoDB.open(config);
        assertNull(recovered.read("anything", 0));
        assertTrue(recovered.scan("anything", 0, 100).isEmpty());
        recovered.shutdown();
    }

    @Test
    void testFlushThenRestart() throws IOException {
        EngineConfig config = EngineConfig.builder()
                .dataDir(dataDir)
                .walDir(walDir)
                .memtableMaxSizeBytes(256) // tiny memtable to force flush
                .fsyncPolicy(EngineConfig.FsyncPolicy.EVERY_WRITE)
                .build();

        StorageEngine engine = NanoDB.open(config);
        // Write enough to trigger multiple flushes
        for (int i = 0; i < 500; i++) {
            engine.write("flush.test", i, i * 2.0);
        }
        engine.flush(); // explicit flush
        engine.shutdown();

        // Reopen — data should be in SSTables
        StorageEngine recovered = NanoDB.open(config);
        for (int i = 0; i < 500; i++) {
            Double val = recovered.read("flush.test", i);
            assertNotNull(val, "Missing value at timestamp " + i);
            assertEquals(i * 2.0, val, 1e-9, "Wrong value at timestamp " + i);
        }
        recovered.shutdown();
    }

    @Test
    void testOverwriteAfterRestart() throws IOException {
        EngineConfig config = makeConfig();

        StorageEngine engine = NanoDB.open(config);
        engine.write("overwrite", 100, 1.0);
        engine.shutdown();

        StorageEngine recovered = NanoDB.open(config);
        assertEquals(1.0, recovered.read("overwrite", 100), 1e-9);

        // Overwrite the same point
        recovered.write("overwrite", 100, 99.0);
        assertEquals(99.0, recovered.read("overwrite", 100), 1e-9);
        recovered.shutdown();
    }

    @Test
    void testMultipleRestartCycles() throws IOException {
        EngineConfig config = makeConfig();

        for (int cycle = 0; cycle < 5; cycle++) {
            StorageEngine engine = NanoDB.open(config);
            // Write some new data each cycle
            for (int i = 0; i < 10; i++) {
                engine.write("cycle.test", cycle * 100 + i, cycle + i * 0.1);
            }
            engine.shutdown();
        }

        // Final verification — all data should be present
        StorageEngine engine = NanoDB.open(config);
        for (int cycle = 0; cycle < 5; cycle++) {
            for (int i = 0; i < 10; i++) {
                long ts = cycle * 100 + i;
                Double val = engine.read("cycle.test", ts);
                assertNotNull(val, "Missing cycle=" + cycle + " ts=" + ts);
                assertEquals(cycle + i * 0.1, val, 1e-9, "Wrong value cycle=" + cycle + " ts=" + ts);
            }
        }
        engine.shutdown();
    }

    @Test
    void testConcurrentWritesBeforeRestart() throws IOException {
        EngineConfig config = makeConfig();

        StorageEngine engine = NanoDB.open(config);
        // Write from multiple "series" interleaved
        String[] series = {"alpha", "beta", "gamma", "delta"};
        for (int i = 0; i < 100; i++) {
            engine.write(series[i % series.length], i, i * 1.0);
        }
        engine.shutdown();

        StorageEngine recovered = NanoDB.open(config);
        for (int i = 0; i < 100; i++) {
            String sid = series[i % series.length];
            Double val = recovered.read(sid, i);
            assertNotNull(val, "Missing " + sid + " @" + i);
            assertEquals(i * 1.0, val, 1e-9);
        }
        recovered.shutdown();
    }

    private EngineConfig makeConfig() {
        return EngineConfig.builder()
                .dataDir(dataDir)
                .walDir(walDir)
                .memtableMaxSizeBytes(4 * 1024 * 1024)
                .fsyncPolicy(EngineConfig.FsyncPolicy.EVERY_WRITE)
                .build();
    }
}
