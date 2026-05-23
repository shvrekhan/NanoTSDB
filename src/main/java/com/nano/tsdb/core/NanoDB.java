package com.nano.tsdb.core;

import com.nano.tsdb.storage.memtable.MemTable;
import com.nano.tsdb.storage.memtable.OnHeapMemTable;
import com.nano.tsdb.storage.sstable.SSTableManager;
import com.nano.tsdb.storage.sstable.SSTableMetadata;
import com.nano.tsdb.storage.sstable.SSTableScanner;
import com.nano.tsdb.storage.sstable.SSTableWriter;
import com.nano.tsdb.storage.wal.WALEntry;
import com.nano.tsdb.storage.wal.WALManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The main engine facade. Orchestrates WAL → Memtable → SSTable → Compaction.
 */
public class NanoDB implements StorageEngine {

    private final EngineConfig config;
    private final WALManager walManager;
    private final AtomicReference<MemTable> activeMemtable;
    private final CopyOnWriteArrayList<MemTable> immutableMemtables;
    private final SSTableManager sstableManager;
    private final ExecutorService flushExecutor;
    private final AtomicBoolean shutdown;

    private NanoDB(EngineConfig config, WALManager walManager, MemTable recoveredMemtable,
                   SSTableManager sstableManager) {
        this.config = config;
        this.walManager = walManager;
        this.activeMemtable = new AtomicReference<>(recoveredMemtable);
        this.immutableMemtables = new CopyOnWriteArrayList<>();
        this.sstableManager = sstableManager;
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "flush-thread");
            t.setDaemon(true);
            return t;
        });
        this.shutdown = new AtomicBoolean(false);
    }

    public static NanoDB open(EngineConfig config) throws IOException {
        RecoveryResult result = RecoveryManager.recover(config);
        WALManager wal = new WALManager(config);
        SSTableManager sstManager = SSTableManager.discoverExisting(config.getSegmentsDir());
        return new NanoDB(config, wal, result.memtable(), sstManager);
    }

    @Override
    public void write(String seriesId, long timestamp, double value) throws IOException {
        if (shutdown.get()) {
            throw new IllegalStateException("engine is shut down");
        }

        long seq = walManager.nextSequenceNumber();
        WALEntry entry = new WALEntry(seriesId, timestamp, value, seq);
        walManager.append(entry);

        while (true) {
            MemTable active = activeMemtable.get();
            try {
                active.put(seriesId, timestamp, value);
                if (active.isFull(config.getMemtableMaxSizeBytes())) {
                    freezeAndSwap();
                }
                break;
            } catch (IllegalStateException e) {
                if (!e.getMessage().contains("frozen")) {
                    throw e;
                }
                // memtable was frozen between get() and put() — retry
            }
        }
    }

    private void freezeAndSwap() {
        MemTable current = activeMemtable.get();
        if (current.isFrozen()) {
            return; // another thread already swapped
        }

        current.freeze();
        MemTable newActive = new OnHeapMemTable();
        if (!activeMemtable.compareAndSet(current, newActive)) {
            // CAS failed — another thread succeeded
            return;
        }
        immutableMemtables.add(current);
        flushExecutor.submit(() -> {
            try {
                flushMemtable(current);
            } catch (IOException e) {
                // Log error (in a real system, use proper logging)
                System.err.println("Flush failed: " + e.getMessage());
            }
        });
    }

    private void flushMemtable(MemTable memtable) throws IOException {
        List<DataPoint> snapshot = memtable.snapshot();
        if (snapshot.isEmpty()) {
            immutableMemtables.remove(memtable);
            return;
        }

        snapshot.sort(Comparator.naturalOrder());
        String fileName = String.format("segment_%d_%d.sst", System.currentTimeMillis(), walManager.nextSequenceNumber());
        Path outputPath = config.getSegmentsDir().resolve(fileName);

        SSTableWriter writer = new SSTableWriter(outputPath, config.getSparseIndexInterval(),
                config.getBloomExpectedInsertions(), config.getBloomFpp());
        SSTableMetadata metadata = writer.write(snapshot);

        sstableManager.register(metadata);
        immutableMemtables.remove(memtable);
        walManager.truncate();
    }

    @Override
    public Double read(String seriesId, long timestamp) throws IOException {
        if (shutdown.get()) {
            throw new IllegalStateException("engine is shut down");
        }

        // Active memtable (most recent)
        MemTable active = activeMemtable.get();
        Double value = active.get(seriesId, timestamp);
        if (value != null) {
            return value;
        }

        // Immutable memtables (newest first)
        for (int i = immutableMemtables.size() - 1; i >= 0; i--) {
            value = immutableMemtables.get(i).get(seriesId, timestamp);
            if (value != null) {
                return value;
            }
        }

        // SSTables (newest first)
        for (SSTableScanner scanner : sstableManager.getActiveScanners()) {
            value = scanner.get(seriesId, timestamp);
            if (value != null) {
                return value;
            }
        }

        return null;
    }

    @Override
    public List<DataPoint> scan(String seriesId, long from, long to) throws IOException {
        if (shutdown.get()) {
            throw new IllegalStateException("engine is shut down");
        }

        TreeMap<Long, DataPoint> merged = new TreeMap<>();

        // Active memtable
        for (var entry : activeMemtable.get().scan(seriesId, from, to).entrySet()) {
            merged.put(entry.getKey(), new DataPoint(seriesId, entry.getKey(), entry.getValue()));
        }

        // Immutable memtables
        for (MemTable imm : immutableMemtables) {
            for (var entry : imm.scan(seriesId, from, to).entrySet()) {
                merged.put(entry.getKey(), new DataPoint(seriesId, entry.getKey(), entry.getValue()));
            }
        }

        // SSTables
        for (SSTableScanner scanner : sstableManager.getActiveScanners()) {
            for (DataPoint dp : scanner.scan(seriesId, from, to)) {
                merged.put(dp.timestamp(), dp);
            }
        }

        return new ArrayList<>(merged.values());
    }

    @Override
    public void flush() throws IOException {
        if (shutdown.get()) {
            throw new IllegalStateException("engine is shut down");
        }

        MemTable active = activeMemtable.get();
        active.freeze();
        if (activeMemtable.compareAndSet(active, new OnHeapMemTable())) {
            immutableMemtables.add(active);
            flushExecutor.submit(() -> {
                try {
                    flushMemtable(active);
                } catch (IOException e) {
                    System.err.println("Explicit flush failed: " + e.getMessage());
                }
            });
        }
    }

    @Override
    public void shutdown() throws IOException {
        if (shutdown.compareAndSet(false, true)) {
            flushExecutor.shutdown();
            try {
                if (!flushExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    flushExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                flushExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Flush any remaining active memtable
            MemTable active = activeMemtable.get();
            if (active.sizeBytes() > 0) {
                try {
                    flushMemtable(active);
                } catch (IOException ignored) {
                }
            }

            walManager.close();
            sstableManager.close();
        }
    }

    public EngineConfig getConfig() {
        return config;
    }

    public SSTableManager getSstableManager() {
        return sstableManager;
    }

    public int getImmutableMemtableCount() {
        return immutableMemtables.size();
    }
}
