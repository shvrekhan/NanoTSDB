package com.nano.tsdb.storage.memtable;

import com.nano.tsdb.core.DataPoint;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Off-heap memtable — stores actual data points in a DirectByteBuffer to reduce GC pressure.
 * The on-heap index (ConcurrentSkipListMap) maps keys to offsets into the direct buffer.
 *
 * Trade-off: lower GC pauses but more complex code and risk of page faults if
 * the OS decides to swap the direct memory region.
 */
public class OffHeapMemTable implements MemTable {

    // each entry in the direct buffer: [seriesIdLen:4][seriesIdBytes][timestamp:8][value:8]
    private final ByteBuffer directBuffer;
    private final AtomicInteger writePosition;
    private final ReentrantLock writeLock;

    // on-heap index: seriesId -> (timestamp -> offset into directBuffer)
    private final ConcurrentSkipListMap<String, ConcurrentSkipListMap<Long, Integer>> index;
    private volatile boolean frozen;

    public OffHeapMemTable(int capacityBytes) {
        this.directBuffer = ByteBuffer.allocateDirect(capacityBytes);
        this.writePosition = new AtomicInteger(0);
        this.writeLock = new ReentrantLock();
        this.index = new ConcurrentSkipListMap<>();
        this.frozen = false;
    }

    public OffHeapMemTable() {
        this(4 * 1024 * 1024); // 4MB default
    }

    @Override
    public void put(String seriesId, long timestamp, double value) {
        if (frozen) {
            throw new IllegalStateException("memtable is frozen, can't write");
        }

        byte[] idBytes = seriesId.getBytes(StandardCharsets.UTF_8);
        int entrySize = 4 + idBytes.length + 8 + 8;

        writeLock.lock();
        try {
            int offset = writePosition.get();
            if (offset + entrySize > directBuffer.capacity()) {
                throw new IllegalStateException("off-heap buffer full");
            }

            // write to direct buffer at current position
            directBuffer.position(offset);
            directBuffer.putInt(idBytes.length);
            directBuffer.put(idBytes);
            directBuffer.putLong(timestamp);
            directBuffer.putDouble(value);

            writePosition.set(offset + entrySize);

            // update the on-heap index
            index.computeIfAbsent(seriesId, k -> new ConcurrentSkipListMap<>())
                    .put(timestamp, offset);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Double get(String seriesId, long timestamp) {
        ConcurrentSkipListMap<Long, Integer> series = index.get(seriesId);
        if (series == null) return null;

        Integer offset = series.get(timestamp);
        if (offset == null) return null;

        return readValueAt(offset);
    }

    @Override
    public NavigableMap<Long, Double> scan(String seriesId, long from, long to) {
        ConcurrentSkipListMap<Long, Integer> series = index.get(seriesId);
        if (series == null) {
            return Collections.emptyNavigableMap();
        }

        NavigableMap<Long, Double> result = new TreeMap<>();
        for (var entry : series.subMap(from, true, to, true).entrySet()) {
            result.put(entry.getKey(), readValueAt(entry.getValue()));
        }
        return result;
    }

    @Override
    public long sizeBytes() {
        return writePosition.get();
    }

    @Override
    public boolean isFull(long maxSizeBytes) {
        return writePosition.get() >= maxSizeBytes;
    }

    @Override
    public List<DataPoint> snapshot() {
        List<DataPoint> points = new ArrayList<>();
        for (var seriesEntry : index.entrySet()) {
            String sid = seriesEntry.getKey();
            for (var tsEntry : seriesEntry.getValue().entrySet()) {
                double val = readValueAt(tsEntry.getValue());
                points.add(new DataPoint(sid, tsEntry.getKey(), val));
            }
        }
        return points;
    }

    @Override
    public void freeze() {
        this.frozen = true;
    }

    @Override
    public boolean isFrozen() {
        return frozen;
    }

    // reads the value field from a data point stored at the given offset in the direct buffer
    private double readValueAt(int offset) {
        ByteBuffer dup = directBuffer.duplicate();
        dup.position(offset);
        int idLen = dup.getInt();
        dup.position(dup.position() + idLen); // skip seriesId bytes
        dup.getLong(); // skip timestamp
        return dup.getDouble();
    }
}
