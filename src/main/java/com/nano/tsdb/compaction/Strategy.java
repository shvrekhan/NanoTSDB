package com.nano.tsdb.compaction;

import com.nano.tsdb.storage.sstable.SSTableMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Size-tiered compaction strategy (STCS).
 * <p>
 * Groups SSTables by similar file sizes, merges when a bucket exceeds
 * {@code minTables} count. Uses a size ratio to determine which tables
 * belong to the same tier.
 */
public class Strategy {

    private final int minTables;
    private final double sizeRatio;

    public Strategy(int minTables, double sizeRatio) {
        if (minTables < 2) {
            throw new IllegalArgumentException("minTables must be >= 2, got " + minTables);
        }
        if (sizeRatio <= 1.0) {
            throw new IllegalArgumentException("sizeRatio must be > 1.0, got " + sizeRatio);
        }
        this.minTables = minTables;
        this.sizeRatio = sizeRatio;
    }

    /**
     * Selects a set of SSTables to compact from the given list of metadata.
     * Returns the chosen candidates, or an empty list if no compaction is needed.
     * <p>
     * Algorithm:
     * 1. Sort tables by file size ascending
     * 2. Walk from smallest to largest, grouping tables where
     *    size(i) * sizeRatio >= size(i+1) (i.e., they're in the same tier)
     * 3. If any group has >= minTables, compact that group
     * 4. Prefer the smallest group (most compaction benefit per byte)
     */
    public List<SSTableMetadata> selectCandidates(List<SSTableMetadata> allTables) {
        if (allTables.size() < minTables) {
            return Collections.emptyList();
        }

        // Sort by file size ascending
        List<SSTableMetadata> sorted = new ArrayList<>(allTables);
        sorted.sort(Comparator.comparingLong(SSTableMetadata::fileSizeBytes));

        // Find the smallest group that meets the threshold
        List<SSTableMetadata> bestGroup = Collections.emptyList();

        int start = 0;
        while (start < sorted.size()) {
            int end = start + 1;
            while (end < sorted.size()) {
                long currentSize = sorted.get(end - 1).fileSizeBytes();
                long nextSize = sorted.get(end).fileSizeBytes();
                // If next table is within sizeRatio of current, they're in the same tier
                if (nextSize <= currentSize * sizeRatio) {
                    end++;
                } else {
                    break;
                }
            }

            int groupSize = end - start;
            if (groupSize >= minTables) {
                List<SSTableMetadata> group = sorted.subList(start, end);
                if (bestGroup.isEmpty() || group.size() > bestGroup.size()) {
                    bestGroup = new ArrayList<>(group);
                }
            }

            start = end;
        }

        return bestGroup;
    }

    public int getMinTables() {
        return minTables;
    }

    public double getSizeRatio() {
        return sizeRatio;
    }
}
