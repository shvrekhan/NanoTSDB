package com.nano.tsdb.compaction;

import com.nano.tsdb.core.DataPoint;
import com.nano.tsdb.storage.sstable.SSTableMetadata;
import com.nano.tsdb.storage.sstable.SSTableScanner;
import com.nano.tsdb.storage.sstable.SSTableWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for compaction components: Strategy and Merger.
 */
class CompactionTest {

    @TempDir
    Path tempDir;

    @Test
    void testStrategySelectsCandidates() {
        Strategy strategy = new Strategy(3, 1.5);

        // Create metadata with varying sizes
        List<SSTableMetadata> tables = new ArrayList<>();
        tables.add(new SSTableMetadata(Path.of("a.sst"), 0, 100, 10, 100));
        tables.add(new SSTableMetadata(Path.of("b.sst"), 0, 100, 10, 110));
        tables.add(new SSTableMetadata(Path.of("c.sst"), 0, 100, 10, 105));
        tables.add(new SSTableMetadata(Path.of("d.sst"), 0, 100, 10, 1000)); // different tier

        List<SSTableMetadata> candidates = strategy.selectCandidates(tables);
        assertFalse(candidates.isEmpty(), "Should select candidates");
        assertTrue(candidates.size() >= 3, "Should select at least 3 tables");
    }

    @Test
    void testStrategyReturnsEmptyWhenBelowThreshold() {
        Strategy strategy = new Strategy(4, 1.5);

        List<SSTableMetadata> tables = new ArrayList<>();
        tables.add(new SSTableMetadata(Path.of("a.sst"), 0, 100, 10, 100));
        tables.add(new SSTableMetadata(Path.of("b.sst"), 0, 100, 10, 110));
        tables.add(new SSTableMetadata(Path.of("c.sst"), 0, 100, 10, 105));

        List<SSTableMetadata> candidates = strategy.selectCandidates(tables);
        assertTrue(candidates.isEmpty(), "Should not compact with only 3 tables when min is 4");
    }

    @Test
    void testStrategyRejectsInvalidParams() {
        assertThrows(IllegalArgumentException.class, () -> new Strategy(1, 1.5));
        assertThrows(IllegalArgumentException.class, () -> new Strategy(3, 1.0));
        assertThrows(IllegalArgumentException.class, () -> new Strategy(3, 0.5));
    }

    @Test
    void testMergeProducesSortedOutput() throws IOException {
        // Create two small SSTables and merge them
        Path sst1 = tempDir.resolve("input1.sst");
        Path sst2 = tempDir.resolve("input2.sst");

        List<DataPoint> data1 = new ArrayList<>();
        data1.add(new DataPoint("cpu", 100, 1.0));
        data1.add(new DataPoint("cpu", 102, 3.0));
        data1.add(new DataPoint("mem", 100, 10.0));

        List<DataPoint> data2 = new ArrayList<>();
        data2.add(new DataPoint("cpu", 101, 2.0));
        data2.add(new DataPoint("cpu", 103, 4.0));
        data2.add(new DataPoint("mem", 101, 20.0));

        SSTableWriter writer1 = new SSTableWriter(sst1, 2, 100, 0.01);
        writer1.write(data1);

        SSTableWriter writer2 = new SSTableWriter(sst2, 2, 100, 0.01);
        writer2.write(data2);

        // Merge
        SSTableScanner scanner1 = SSTableScanner.open(sst1);
        SSTableScanner scanner2 = SSTableScanner.open(sst2);

        Merger merger = new Merger(2, 100, 0.01);
        Path mergedPath = tempDir.resolve("merged.sst");
        SSTableMetadata mergedMeta = merger.merge(List.of(scanner1, scanner2), mergedPath);

        assertNotNull(mergedMeta);
        assertEquals(6, mergedMeta.entryCount(), "Should have 6 merged entries");

        // Verify merged output is sorted
        SSTableScanner mergedScanner = SSTableScanner.open(mergedPath);
        List<DataPoint> allPoints = new ArrayList<>();
        var it = mergedScanner.iterator();
        while (it.hasNext()) {
            allPoints.add(it.next());
        }

        assertEquals(6, allPoints.size());
        for (int i = 1; i < allPoints.size(); i++) {
            DataPoint prev = allPoints.get(i - 1);
            DataPoint curr = allPoints.get(i);
            assertTrue(prev.compareTo(curr) <= 0,
                    "Output should be sorted: " + prev + " > " + curr);
        }

        scanner1.close();
        scanner2.close();
        mergedScanner.close();
    }

    @Test
    void testMergeDeduplicatesSameTimestamp() throws IOException {
        Path sst1 = tempDir.resolve("dup1.sst");
        Path sst2 = tempDir.resolve("dup2.sst");

        List<DataPoint> data1 = new ArrayList<>();
        data1.add(new DataPoint("cpu", 100, 1.0));

        List<DataPoint> data2 = new ArrayList<>();
        data2.add(new DataPoint("cpu", 100, 99.0)); // same key, different value

        SSTableWriter writer1 = new SSTableWriter(sst1, 2, 100, 0.01);
        writer1.write(data1);

        SSTableWriter writer2 = new SSTableWriter(sst2, 2, 100, 0.01);
        writer2.write(data2);

        SSTableScanner scanner1 = SSTableScanner.open(sst1);
        SSTableScanner scanner2 = SSTableScanner.open(sst2);

        Merger merger = new Merger(2, 100, 0.01);
        Path mergedPath = tempDir.resolve("deduped.sst");
        SSTableMetadata mergedMeta = merger.merge(List.of(scanner1, scanner2), mergedPath);

        assertEquals(1, mergedMeta.entryCount(), "Should deduplicate to 1 entry");

        SSTableScanner mergedScanner = SSTableScanner.open(mergedPath);
        Double val = mergedScanner.get("cpu", 100);
        assertNotNull(val);
        // The merge keeps the first encountered (from scanner1)
        assertEquals(1.0, val, 1e-9);

        scanner1.close();
        scanner2.close();
        mergedScanner.close();
    }
}
