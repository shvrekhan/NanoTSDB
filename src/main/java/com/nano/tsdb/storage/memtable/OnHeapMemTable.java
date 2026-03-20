package com.nano.tsdb.storage.memtable;

import com.nano.tsdb.core.DataPoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class OnHeapMemTable implements MemTable {

    // seriesId -> (timestamp -> value), both levels sorted
    private final ConcurrentSkipListMap<String, ConcurrentSkipListMap<Long, Double>> data;
    private final AtomicLong estimatedSize;
    private volatile boolean frozen;

    public OnHeapMemTable() {
        this.data = new ConcurrentSkipListMap<>();
        this.estimatedSize = new AtomicLong(0);
        this.frozen = false;
    }

    @Override
    public void put(String seriesId, long timestamp, double value) {
        if (frozen) {
            throw new IllegalStateException("memtable is frozen, can't write");
        }

        ConcurrentSkipListMap<Long, Double> series = data.computeIfAbsent(seriesId, k -> {
            // account for the new map + key overhead (~64 bytes rough estimate)
            estimatedSize.addAndGet(64 + seriesId.length() * 2L);
            return new ConcurrentSkipListMap<>();
        });

        Double prev = series.put(timestamp, value);
        if (prev == null) {
            // new entry: 8 bytes timestamp key + 8 bytes Double value + ~32 bytes node overhead
            estimatedSize.addAndGet(48);
        }
    }

    @Override
    public Double get(String seriesId, long timestamp) {
        ConcurrentSkipListMap<Long, Double> series = data.get(seriesId);
        return series != null ? series.get(timestamp) : null;
    }

    @Override
    public NavigableMap<Long, Double> scan(String seriesId, long from, long to) {
        ConcurrentSkipListMap<Long, Double> series = data.get(seriesId);
        if (series == null) {
            return Collections.emptyNavigableMap();
        }
        return series.subMap(from, true, to, true);
    }

    @Override
    public long sizeBytes() {
        return estimatedSize.get();
    }

    @Override
    public boolean isFull(long maxSizeBytes) {
        return estimatedSize.get() >= maxSizeBytes;
    }

    @Override
    public List<DataPoint> snapshot() {
        List<DataPoint> points = new ArrayList<>();
        for (var seriesEntry : data.entrySet()) {
            String sid = seriesEntry.getKey();
            for (var tsEntry : seriesEntry.getValue().entrySet()) {
                points.add(new DataPoint(sid, tsEntry.getKey(), tsEntry.getValue()));
            }
        }
        // already sorted because both maps are sorted
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
}
