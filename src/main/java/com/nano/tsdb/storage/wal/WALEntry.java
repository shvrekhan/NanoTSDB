package com.nano.tsdb.storage.wal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

public class WALEntry {
    private final String seriesId;
    private final long timestamp;
    private final double value;
    private final long sequenceNumber;
    private final long checksum;

    public WALEntry(String seriesId, long timestamp, double value, long sequenceNumber) {
        this.seriesId = seriesId;
        this.timestamp = timestamp;
        this.value = value;
        this.sequenceNumber = sequenceNumber;
        this.checksum = computeChecksum(seriesId, timestamp, value, sequenceNumber);
    }

    private static long computeChecksum(String seriesId, long timestamp, double value, long sequenceNumber) {
        CRC32 crc = new CRC32();
        byte[] idBytes = seriesId.getBytes(StandardCharsets.UTF_8);
        crc.update(idBytes, 0, idBytes.length);

        ByteBuffer buf = ByteBuffer.allocate(8 + 8 + 8);
        buf.putLong(timestamp);
        buf.putDouble(value);
        buf.putLong(sequenceNumber);
        crc.update(buf.array(), 0, buf.array().length);
        return crc.getValue();
    }

    public String getSeriesId() { return seriesId; }
    public long getTimestamp()  { return timestamp; }
    public double getValue()    { return value; }
    public long getSequenceNumber() { return sequenceNumber; }
    public long getChecksum()   { return checksum; }
}
