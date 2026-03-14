package com.nano.tsdb.nio;

import com.nano.tsdb.storage.wal.WALEntry;
import java.nio.ByteBuffer;

public class ByteSerializer {

    public static byte[] encode(WALEntry entry) {
        byte[] idBytes = entry.getSeriesId().getBytes();
        ByteBuffer buf = ByteBuffer.allocate(4 + idBytes.length + 8 + 8 + 4);

        buf.putInt(idBytes.length);
        buf.put(idBytes);
        buf.putLong(entry.getTimestamp());
        buf.putDouble(entry.getValue());
        buf.putInt(entry.getChecksum());

        return buf.array();
    }

    public static WALEntry decode(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        int idLen     = buf.getInt();
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        String seriesId = new String(idBytes);
        long timestamp  = buf.getLong();
        double value    = buf.getDouble();
        int checksum    = buf.getInt();

        WALEntry entry = new WALEntry(seriesId, timestamp, value);
        if (entry.getChecksum() != checksum) {
            throw new IllegalStateException("Corrupt WAL entry: checksum mismatch");
        }
        return entry;
    }
}
