package com.nano.tsdb.core;

/**
 * A single time-series data point.
 */
public record DataPoint(String seriesId, long timestamp, double value) implements Comparable<DataPoint> {

    @Override
    public int compareTo(DataPoint o) {
        int cmp = this.seriesId.compareTo(o.seriesId);
        return cmp != 0 ? cmp : Long.compare(this.timestamp, o.timestamp);
    }
}
