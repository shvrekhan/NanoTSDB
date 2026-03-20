package com.nano.tsdb.storage.sstable;

import com.nano.tsdb.core.DataPoint;
import com.nano.tsdb.index.BloomFilter;
import com.nano.tsdb.index.SparseIndex;
import com.nano.tsdb.nio.MappedBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Reads an SSTable file using mmap. Uses the bloom filter for quick
 * negative lookups and the sparse index to narrow the scan window.
 */
public class SSTableScanner implements Closeable {

    private final Path path;
    private final MappedBuffer mapped;
    private final BloomFilter bloom;
    private final SparseIndex index;
    private final long dataBlockEnd; // byte offset where data block ends (= index block start)
    private final int entryCount;

    private SSTableScanner(Path path, MappedBuffer mapped, BloomFilter bloom,
                           SparseIndex index, long dataBlockEnd, int entryCount) {
        this.path = path;
        this.mapped = mapped;
        this.bloom = bloom;
        this.index = index;
        this.dataBlockEnd = dataBlockEnd;
        this.entryCount = entryCount;
    }

    public static SSTableScanner open(Path file) throws IOException {
        MappedBuffer mapped = MappedBuffer.open(file);
        long fileSize = mapped.size();

        // read footer (last 24 bytes)
        int footerStart = (int) (fileSize - SSTableWriter.getFooterSize());
        long indexOffset = mapped.getLong(footerStart);
        long bloomOffset = mapped.getLong(footerStart + 8);
        int entryCount = mapped.getInt(footerStart + 16);
        int magic = mapped.getInt(footerStart + 20);

        if (magic != SSTableWriter.getMagic()) {
            mapped.close();
            throw new IOException("invalid SSTable file — bad magic: " + file);
        }

        // load sparse index
        int indexLen = (int) (bloomOffset - indexOffset);
        byte[] indexBytes = mapped.getBytes((int) indexOffset, indexLen);
        SparseIndex sparseIndex = SparseIndex.fromBytes(indexBytes);

        // load bloom filter
        int bloomLen = (int) (footerStart - bloomOffset);
        byte[] bloomBytes = mapped.getBytes((int) bloomOffset, bloomLen);
        BloomFilter bloom = BloomFilter.fromBytes(bloomBytes);

        return new SSTableScanner(file, mapped, bloom, sparseIndex, indexOffset, entryCount);
    }

    public boolean mightContain(String seriesId) {
        return bloom.mightContain(seriesId);
    }

    /**
     * Point lookup — returns the value for a specific (seriesId, timestamp) or null.
     */
    public Double get(String seriesId, long timestamp) {
        if (!bloom.mightContain(seriesId)) return null;

        long startOffset = index.findOffset(seriesId, timestamp);
        int pos = (int) startOffset;

        while (pos < dataBlockEnd) {
            DataPoint dp = readEntryAt(pos);
            if (dp == null) break;

            int cmp = dp.seriesId().compareTo(seriesId);
            if (cmp > 0) break; // passed it
            if (cmp == 0 && dp.timestamp() == timestamp) return dp.value();
            if (cmp == 0 && dp.timestamp() > timestamp) break; // passed it within same series

            pos += entrySize(dp);
        }
        return null;
    }

    /**
     * Range scan — returns all points for a series within [from, to].
     */
    public List<DataPoint> scan(String seriesId, long from, long to) {
        List<DataPoint> results = new ArrayList<>();
        if (!bloom.mightContain(seriesId)) return results;

        long startOffset = index.findOffset(seriesId, from);
        int pos = (int) startOffset;

        while (pos < dataBlockEnd) {
            DataPoint dp = readEntryAt(pos);
            if (dp == null) break;

            int cmp = dp.seriesId().compareTo(seriesId);
            if (cmp > 0) break;
            if (cmp == 0) {
                if (dp.timestamp() > to) break;
                if (dp.timestamp() >= from) results.add(dp);
            }

            pos += entrySize(dp);
        }
        return results;
    }

    /**
     * Full table iterator — used during compaction merge.
     */
    public Iterator<DataPoint> iterator() {
        return new Iterator<>() {
            private int pos = 0;

            @Override
            public boolean hasNext() {
                return pos < dataBlockEnd;
            }

            @Override
            public DataPoint next() {
                if (!hasNext()) throw new NoSuchElementException();
                DataPoint dp = readEntryAt(pos);
                if (dp == null) throw new NoSuchElementException("corrupt entry at offset " + pos);
                pos += entrySize(dp);
                return dp;
            }
        };
    }

    private DataPoint readEntryAt(int offset) {
        try {
            int idLen = mapped.getInt(offset);
            if (idLen <= 0 || idLen > 1024) return null; // sanity check

            byte[] idBytes = mapped.getBytes(offset + 4, idLen);
            String seriesId = new String(idBytes, StandardCharsets.UTF_8);
            long ts = mapped.getLong(offset + 4 + idLen);
            double val = mapped.getDouble(offset + 4 + idLen + 8);

            return new DataPoint(seriesId, ts, val);
        } catch (Exception e) {
            return null; // corrupt or truncated entry
        }
    }

    private int entrySize(DataPoint dp) {
        return 4 + dp.seriesId().getBytes(StandardCharsets.UTF_8).length + 8 + 8;
    }

    public int getEntryCount() {
        return entryCount;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public void close() throws IOException {
        mapped.close();
    }
}
