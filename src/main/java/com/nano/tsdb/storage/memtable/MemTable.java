package com.nano.tsdb.storage.memtable;

import com.nano.tsdb.core.DataPoint;

import java.util.List;
import java.util.NavigableMap;

public interface MemTable {

    void put(String seriesId, long timestamp, double value);

    Double get(String seriesId, long timestamp);

    NavigableMap<Long, Double> scan(String seriesId, long from, long to);

    long sizeBytes();

    boolean isFull(long maxSizeBytes);

    /**
     * Returns all data sorted by (seriesId, timestamp) for flushing to SSTable.
     */
    List<DataPoint> snapshot();

    void freeze();

    boolean isFrozen();
}
