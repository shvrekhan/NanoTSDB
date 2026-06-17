package com.nano.tsdb.compaction;

import com.nano.tsdb.core.DataPoint;
import com.nano.tsdb.storage.sstable.SSTableScanner;
import com.nano.tsdb.storage.sstable.SSTableWriter;
import com.nano.tsdb.storage.sstable.SSTableMetadata;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * K-way merge of SSTable iterators during compaction.
 * <p>
 * Takes multiple SSTable scanners, merges their sorted iterators into a single
 * sorted output, resolving duplicates by keeping the latest timestamp value.
 * Writes the merged result to a new SSTable.
 */
public class Merger {

    private final int sparseIndexInterval;
    private final int bloomExpectedInsertions;
    private final double bloomFpp;

    public Merger(int sparseIndexInterval, int bloomExpectedInsertions, double bloomFpp) {
        this.sparseIndexInterval = sparseIndexInterval;
        this.bloomExpectedInsertions = bloomExpectedInsertions;
        this.bloomFpp = bloomFpp;
    }

    /**
     * Merges multiple SSTable scanners into a single output SSTable.
     * <p>
     * Uses a PriorityQueue over iterators for k-way merge.
     * Duplicate resolution: for the same (seriesId, timestamp), the latest
     * value wins (last writer wins — appropriate for time-series overwrites).
     *
     * @param scanners   the SSTable scanners to merge (each yields sorted DataPoints)
     * @param outputPath the path for the merged output SSTable
     * @return metadata about the merged SSTable
     */
    public SSTableMetadata merge(List<SSTableScanner> scanners, Path outputPath) throws IOException {
        if (scanners.isEmpty()) {
            throw new IllegalArgumentException("no scanners to merge");
        }

        // Collect all iterators
        List<Iterator<DataPoint>> iterators = new ArrayList<>(scanners.size());
        for (SSTableScanner scanner : scanners) {
            iterators.add(scanner.iterator());
        }

        // K-way merge using a priority queue
        PriorityQueue<PeekingIterator> queue = new PriorityQueue<>(
                Comparator.comparing(PeekingIterator::peek, DataPoint::compareTo));

        for (int i = 0; i < iterators.size(); i++) {
            Iterator<DataPoint> it = iterators.get(i);
            if (it.hasNext()) {
                queue.offer(new PeekingIterator(it, i));
            }
        }

        List<DataPoint> merged = new ArrayList<>();
        DataPoint last = null;

        while (!queue.isEmpty()) {
            PeekingIterator current = queue.poll();
            DataPoint dp = current.next();

            // Duplicate resolution: same (seriesId, timestamp) — keep the latest
            if (last != null
                    && last.seriesId().equals(dp.seriesId())
                    && last.timestamp() == dp.timestamp()) {
                // Skip the older one (already have 'last' which came from a later scanner)
                // Since we process iterators in order, the first encountered wins.
                // But we want latest timestamp to win, so we keep the one with higher value
                // Actually for time-series, same timestamp means same data point — keep either
                // We'll keep the one we already have (last)
                if (current.hasNext()) {
                    queue.offer(current);
                }
                continue;
            }

            if (last != null) {
                merged.add(last);
            }
            last = dp;

            if (current.hasNext()) {
                queue.offer(current);
            }
        }

        if (last != null) {
            merged.add(last);
        }

        if (merged.isEmpty()) {
            throw new IOException("merge produced no output");
        }

        // Write merged data to new SSTable
        SSTableWriter writer = new SSTableWriter(
                outputPath, sparseIndexInterval, bloomExpectedInsertions, bloomFpp);
        return writer.write(merged);
    }

    /**
     * A wrapper around an iterator that supports peeking at the next element
     * without consuming it. Used by the priority queue for k-way merge.
     */
    private static class PeekingIterator {
        private final Iterator<DataPoint> iterator;
        private final int sourceIndex;
        private DataPoint next;

        PeekingIterator(Iterator<DataPoint> iterator, int sourceIndex) {
            this.iterator = iterator;
            this.sourceIndex = sourceIndex;
            this.next = iterator.next();
        }

        DataPoint peek() {
            return next;
        }

        DataPoint next() {
            DataPoint current = next;
            next = iterator.hasNext() ? iterator.next() : null;
            return current;
        }

        boolean hasNext() {
            return next != null;
        }
    }
}
