package com.nano.tsdb.storage.wal;

public class WALEntry {
    private final String seriesId;
    private final long timestamp;
    private final double value;
    private final int checksum;

    public WALEntry(String seriesId, long timestamp, double value) {
        this.seriesId  = seriesId;
        this.timestamp = timestamp;
        this.value     = value;
        this.checksum  = computeChecksum(seriesId, timestamp, value);
    }

    private static int computeChecksum(String seriesId, long timestamp, double value) {
        int h = seriesId.hashCode();
        h = 31 * h + Long.hashCode(timestamp);
        h = 31 * h + Double.hashCode(value);
        return h;
    }

    public String getSeriesId() { return seriesId; }
    public long getTimestamp()  { return timestamp; }
    public double getValue()    { return value; }
    public int getChecksum()    { return checksum; }
}
