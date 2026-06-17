package com.nano.tsdb.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the WriteProcessor single-writer queue.
 */
class WriteProcessorTest {

    @TempDir
    Path tempDir;

    @Test
    void testSubmitAndComplete() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        NanoDB engine = openEngine();
        WriteProcessor processor = new WriteProcessor(engine, 64);

        CompletableFuture<Void> future = processor.submit("test.series", 100, 42.0);
        future.get(5, TimeUnit.SECONDS); // should complete without error

        Double val = engine.read("test.series", 100);
        assertEquals(42.0, val, 1e-9);

        processor.shutdown();
        engine.shutdown();
    }

    @Test
    void testMultipleWrites() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        NanoDB engine = openEngine();
        WriteProcessor processor = new WriteProcessor(engine, 64);

        CompletableFuture<Void>[] futures = new CompletableFuture[10];
        for (int i = 0; i < 10; i++) {
            futures[i] = processor.submit("multi.test", i, i * 10.0);
        }

        for (int i = 0; i < 10; i++) {
            futures[i].get(5, TimeUnit.SECONDS);
        }

        for (int i = 0; i < 10; i++) {
            Double val = engine.read("multi.test", i);
            assertEquals(i * 10.0, val, 1e-9);
        }

        processor.shutdown();
        engine.shutdown();
    }

    @Test
    void testBatchProcessing() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        NanoDB engine = openEngine();
        // Small batch size to test batching behavior
        WriteProcessor processor = new WriteProcessor(engine, 4);

        CompletableFuture<Void>[] futures = new CompletableFuture[20];
        for (int i = 0; i < 20; i++) {
            futures[i] = processor.submit("batch.test", i, i * 1.0);
        }

        for (int i = 0; i < 20; i++) {
            futures[i].get(5, TimeUnit.SECONDS);
        }

        for (int i = 0; i < 20; i++) {
            Double val = engine.read("batch.test", i);
            assertEquals(i * 1.0, val, 1e-9);
        }

        processor.shutdown();
        engine.shutdown();
    }

    @Test
    void testShutdownFailsPending() throws IOException {
        // Use a mock-like approach: create a slow engine
        NanoDB engine = openEngine();
        WriteProcessor processor = new WriteProcessor(engine, 64);

        processor.submit("pending", 0, 1.0);
        processor.shutdown();

        // After shutdown, new submissions should fail
        CompletableFuture<Void> future = processor.submit("after.shutdown", 1, 2.0);
        assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));

        engine.shutdown();
    }

    private NanoDB openEngine() throws IOException {
        Path dataDir = tempDir.resolve("data");
        Path walDir = tempDir.resolve("wal");
        EngineConfig config = EngineConfig.builder()
                .dataDir(dataDir)
                .walDir(walDir)
                .fsyncPolicy(EngineConfig.FsyncPolicy.NONE)
                .build();
        return NanoDB.open(config);
    }
}
