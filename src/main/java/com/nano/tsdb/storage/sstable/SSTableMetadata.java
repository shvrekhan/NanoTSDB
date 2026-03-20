package com.nano.tsdb.storage.sstable;

import java.nio.file.Path;

/**
 * Lightweight metadata about an SSTable file — used by compaction and the read path
 * to decide which tables to scan without opening them.
 */
public record SSTableMetadata(
        Path path,
        long minTimestamp,
        long maxTimestamp,
        int entryCount,
        long fileSizeBytes
) {
    public boolean mightContain(long timestamp) {
        return timestamp >= minTimestamp && timestamp <= maxTimestamp;
    }
}
