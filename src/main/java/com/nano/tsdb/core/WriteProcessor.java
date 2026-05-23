package com.nano.tsdb.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Single-writer queue that serializes all mutations through one daemon thread.
 * Provides async write acknowledgments via CompletableFuture.
 */
public class WriteProcessor {

    private final LinkedBlockingQueue<WriteRequest> queue;
    private final StorageEngine engine;
    private final Thread writerThread;
    private final int batchSize;
    private volatile boolean running;

    public WriteProcessor(StorageEngine engine, int batchSize) {
        this.engine = engine;
        this.batchSize = batchSize;
        this.queue = new LinkedBlockingQueue<>();
        this.running = true;
        this.writerThread = new Thread(this::run, "write-processor");
        this.writerThread.setDaemon(true);
        this.writerThread.start();
    }

    public CompletableFuture<Void> submit(String seriesId, long timestamp, double value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        queue.offer(new WriteRequest(seriesId, timestamp, value, future));
        return future;
    }

    private void run() {
        List<WriteRequest> batch = new ArrayList<>(batchSize);
        while (running) {
            try {
                batch.clear();
                batch.add(queue.take()); // block until at least one

                // drain up to batchSize - 1 more
                queue.drainTo(batch, batchSize - 1);

                for (WriteRequest req : batch) {
                    try {
                        engine.write(req.seriesId(), req.timestamp(), req.value());
                        req.future().complete(null);
                    } catch (IOException e) {
                        req.future().completeExceptionally(e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Fail any remaining requests
        WriteRequest req;
        while ((req = queue.poll()) != null) {
            req.future().completeExceptionally(new IllegalStateException("WriteProcessor is shutting down"));
        }
    }

    public void shutdown() {
        running = false;
        writerThread.interrupt();
    }

    public record WriteRequest(String seriesId, long timestamp, double value, CompletableFuture<Void> future) {
    }
}
