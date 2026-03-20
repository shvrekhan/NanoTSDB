package com.nano.tsdb.index;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Sampled key→offset index for SSTable lookups.
 * Binary search narrows the mmap scan window so we don't linear-scan the whole file.
 */
public class SparseIndex {

    public static class Entry implements Comparable<Entry> {
        private final String seriesId;
        private final long timestamp;
        private final long offset;

        public Entry(String seriesId, long timestamp, long offset) {
            this.seriesId = seriesId;
            this.timestamp = timestamp;
            this.offset = offset;
        }

        public String getSeriesId() {
            return seriesId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getOffset() {
            return offset;
        }

        @Override
        public int compareTo(Entry o) {
            int cmp = this.seriesId.compareTo(o.seriesId);
            return cmp != 0 ? cmp : Long.compare(this.timestamp, o.timestamp);
        }
    }

    private final List<Entry> entries;

    public SparseIndex() {
        this.entries = new ArrayList<>();
    }

    private SparseIndex(List<Entry> entries) {
        this.entries = entries;
    }

    // entries must be added in sorted order during SSTable flush
    public void addEntry(String seriesId, long timestamp, long offset) {
        entries.add(new Entry(seriesId, timestamp, offset));
    }

    /**
     * Returns the byte offset of the floor entry (largest entry <= search key).
     * If key is before everything, returns 0 (start of data block).
     */
    public long findOffset(String seriesId, long timestamp) {
        int idx = Collections.binarySearch(entries, new Entry(seriesId, timestamp, 0));

        if (idx >= 0) return entries.get(idx).getOffset();

        int ip = -(idx + 1); // insertion point
        return ip == 0 ? 0 : entries.get(ip - 1).getOffset();
    }

    /**
     * Returns the byte offset of the ceiling entry (smallest entry >= search key).
     * Returns -1 if we're past everything (scan to end of data block).
     */
    public long findEndOffset(String seriesId, long timestamp) {
        int idx = Collections.binarySearch(entries, new Entry(seriesId, timestamp, 0));

        int ip = idx >= 0 ? idx + 1 : -(idx + 1);
        return ip >= entries.size() ? -1 : entries.get(ip).getOffset();
    }

    public int size() {
        return entries.size();
    }

    public List<Entry> getEntries() {
        return Collections.unmodifiableList(entries);
    }

    // serialization: [count:4] then per entry: [idLen:4][idBytes][ts:8][offset:8]

    public byte[] toBytes() {
        int totalSize = 4;
        List<byte[]> encodedIds = new ArrayList<>(entries.size());
        for (Entry e : entries) {
            byte[] idBytes = e.seriesId.getBytes(StandardCharsets.UTF_8);
            encodedIds.add(idBytes);
            totalSize += 4 + idBytes.length + 8 + 8;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putInt(entries.size());
        for (int i = 0; i < entries.size(); i++) {
            byte[] idBytes = encodedIds.get(i);
            buf.putInt(idBytes.length);
            buf.put(idBytes);
            buf.putLong(entries.get(i).timestamp);
            buf.putLong(entries.get(i).offset);
        }
        return buf.array();
    }

    public static SparseIndex fromBytes(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int count = buf.getInt();
        List<Entry> result = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            int idLen = buf.getInt();
            byte[] idBytes = new byte[idLen];
            buf.get(idBytes);
            String sid = new String(idBytes, StandardCharsets.UTF_8);
            long ts = buf.getLong();
            long off = buf.getLong();
            result.add(new Entry(sid, ts, off));
        }
        return new SparseIndex(result);
    }
}
