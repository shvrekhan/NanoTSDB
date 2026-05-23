package com.nano.tsdb.storage.sstable;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages the lifecycle of SSTable files on disk.
 * Maintains a copy-on-write list of active scanners for lock-free reads.
 */
public class SSTableManager {
    private final CopyOnWriteArrayList<SSTableScanner> scanners;
    private final Path segmentsDir;

    public SSTableManager(Path segmentsDir) {
        this.segmentsDir = segmentsDir;
        this.scanners = new CopyOnWriteArrayList<>();
    }

    public static SSTableManager discoverExisting(Path dir) throws IOException {
        SSTableManager manager = new SSTableManager(dir);
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
            return manager;
        }
        List<Path> sstFiles = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.sst")) {
            for (Path p : stream) {
                sstFiles.add(p);
            }
        }
        // sort by filename descending — newest first (segment_<epoch>_<seq>.sst)
        sstFiles.sort(Comparator.reverseOrder());
        for (Path p : sstFiles) {
            manager.scanners.add(SSTableScanner.open(p));
        }
        return manager;
    }

    public void register(SSTableMetadata metadata) throws IOException {
        scanners.add(0, SSTableScanner.open(metadata.path()));
    }

    public void remove(List<Path> paths) throws IOException {
        for (Path p : paths) {
            scanners.removeIf(s -> s.getPath().equals(p));
            Files.deleteIfExists(p);
        }
    }

    public List<SSTableScanner> getActiveScanners() {
        return new ArrayList<>(scanners);
    }

    public int getSstableCount() {
        return scanners.size();
    }

    public void close() {
        for (SSTableScanner scanner : scanners) {
            try {
                scanner.close();
            } catch (IOException ignored) {
            }
        }
        scanners.clear();
    }
}
