package com.nano.tsdb.storage.sstable;

import com.nano.tsdb.core.DataPoint;
import com.nano.tsdb.index.BloomFilter;
import com.nano.tsdb.index.SparseIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Writes a sorted list of DataPoints to an SSTable file on disk.
 *
 * File layout:
 *   [data block][sparse index block][bloom filter block][footer]
 *
 * Footer (24 bytes):
 *   [indexOffset:8][bloomOffset:8][entryCount:4][magic:4]
 */
public class SSTableWriter {

    private static final int MAGIC = 0x4E_54_53_44; // "NTSD"
    private static final int FOOTER_SIZE = 24;

    private final Path outputPath;
    private final int sparseIndexInterval;
    private final int bloomExpectedInsertions;
    private final double bloomFpp;

    public SSTableWriter(Path outputPath, int sparseIndexInterval,
                         int bloomExpectedInsertions, double bloomFpp) {
        this.outputPath = outputPath;
        this.sparseIndexInterval = sparseIndexInterval;
        this.bloomExpectedInsertions = bloomExpectedInsertions;
        this.bloomFpp = bloomFpp;
    }

    /**
     * Flushes sorted data points to disk. Returns metadata about the written file.
     * Points must already be sorted by (seriesId, timestamp).
     */
    public SSTableMetadata write(java.util.List<DataPoint> sortedPoints) throws IOException {
        if (sortedPoints.isEmpty()) {
            throw new IllegalArgumentException("nothing to flush");
        }

        BloomFilter bloom = new BloomFilter(
                Math.max(bloomExpectedInsertions, sortedPoints.size()), bloomFpp);
        SparseIndex sparseIndex = new SparseIndex();

        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;

        try (FileChannel ch = FileChannel.open(outputPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            // write data block
            long dataOffset = 0;
            for (int i = 0; i < sortedPoints.size(); i++) {
                DataPoint dp = sortedPoints.get(i);
                byte[] entryBytes = encodeEntry(dp);

                // sample every Nth entry into sparse index
                if (i % sparseIndexInterval == 0) {
                    sparseIndex.addEntry(dp.seriesId(), dp.timestamp(), dataOffset);
                }

                bloom.add(dp.seriesId());

                minTs = Math.min(minTs, dp.timestamp());
                maxTs = Math.max(maxTs, dp.timestamp());

                ch.write(ByteBuffer.wrap(entryBytes));
                dataOffset += entryBytes.length;
            }

            // write sparse index block
            long indexOffset = ch.position();
            byte[] indexBytes = sparseIndex.toBytes();
            ch.write(ByteBuffer.wrap(indexBytes));

            // write bloom filter block
            long bloomOffset = ch.position();
            byte[] bloomBytes = bloom.toBytes();
            ch.write(ByteBuffer.wrap(bloomBytes));

            // write footer
            ByteBuffer footer = ByteBuffer.allocate(FOOTER_SIZE);
            footer.putLong(indexOffset);
            footer.putLong(bloomOffset);
            footer.putInt(sortedPoints.size());
            footer.putInt(MAGIC);
            footer.flip();
            ch.write(footer);

            ch.force(true);
        }

        long fileSize = java.nio.file.Files.size(outputPath);
        return new SSTableMetadata(outputPath, minTs, maxTs, sortedPoints.size(), fileSize);
    }

    // entry format: [seriesIdLen:4][seriesIdBytes][timestamp:8][value:8]
    static byte[] encodeEntry(DataPoint dp) {
        byte[] idBytes = dp.seriesId().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + idBytes.length + 8 + 8);
        buf.putInt(idBytes.length);
        buf.put(idBytes);
        buf.putLong(dp.timestamp());
        buf.putDouble(dp.value());
        return buf.array();
    }

    public static int getFooterSize() {
        return FOOTER_SIZE;
    }

    public static int getMagic() {
        return MAGIC;
    }
}
