package com.nano.tsdb.core;

import java.nio.file.Path;
import java.nio.file.Paths;

public class EngineConfig {

    public enum FsyncPolicy {
        EVERY_WRITE,
        BATCH,
        NONE
    }

    private final Path dataDir;
    private final Path walDir;
    private final long memtableMaxSizeBytes;
    private final FsyncPolicy fsyncPolicy;
    private final int batchFsyncSize;
    private final int bloomExpectedInsertions;
    private final double bloomFpp;
    private final int sparseIndexInterval;
    private final int compactionMinTables;
    private final double compactionSizeRatio;

    private EngineConfig(Builder b) {
        this.dataDir = b.dataDir;
        this.walDir = b.walDir;
        this.memtableMaxSizeBytes = b.memtableMaxSizeBytes;
        this.fsyncPolicy = b.fsyncPolicy;
        this.batchFsyncSize = b.batchFsyncSize;
        this.bloomExpectedInsertions = b.bloomExpectedInsertions;
        this.bloomFpp = b.bloomFpp;
        this.sparseIndexInterval = b.sparseIndexInterval;
        this.compactionMinTables = b.compactionMinTables;
        this.compactionSizeRatio = b.compactionSizeRatio;
    }

    public Path getDataDir() {
        return dataDir;
    }

    public Path getWalDir() {
        return walDir;
    }

    public long getMemtableMaxSizeBytes() {
        return memtableMaxSizeBytes;
    }

    public FsyncPolicy getFsyncPolicy() {
        return fsyncPolicy;
    }

    public int getBatchFsyncSize() {
        return batchFsyncSize;
    }

    public int getBloomExpectedInsertions() {
        return bloomExpectedInsertions;
    }

    public double getBloomFpp() {
        return bloomFpp;
    }

    public int getSparseIndexInterval() {
        return sparseIndexInterval;
    }

    public int getCompactionMinTables() {
        return compactionMinTables;
    }

    public double getCompactionSizeRatio() {
        return compactionSizeRatio;
    }

    public Path getWalFilePath() {
        return walDir.resolve("wal.log");
    }

    public Path getSegmentsDir() {
        return dataDir.resolve("segments");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static EngineConfig defaults() {
        return new Builder().build();
    }

    public static class Builder {
        private Path dataDir = Paths.get("data");
        private Path walDir = Paths.get("data", "wal");
        private long memtableMaxSizeBytes = 4 * 1024 * 1024; // 4MB
        private FsyncPolicy fsyncPolicy = FsyncPolicy.EVERY_WRITE;
        private int batchFsyncSize = 64;
        private int bloomExpectedInsertions = 10_000;
        private double bloomFpp = 0.01;
        private int sparseIndexInterval = 128;
        private int compactionMinTables = 4;
        private double compactionSizeRatio = 1.5;

        public Builder dataDir(Path dir) {
            this.dataDir = dir;
            return this;
        }

        public Builder walDir(Path dir) {
            this.walDir = dir;
            return this;
        }

        public Builder memtableMaxSizeBytes(long bytes) {
            this.memtableMaxSizeBytes = bytes;
            return this;
        }

        public Builder fsyncPolicy(FsyncPolicy p) {
            this.fsyncPolicy = p;
            return this;
        }

        public Builder batchFsyncSize(int size) {
            this.batchFsyncSize = size;
            return this;
        }

        public Builder bloomExpectedInsertions(int n) {
            this.bloomExpectedInsertions = n;
            return this;
        }

        public Builder bloomFpp(double fpp) {
            this.bloomFpp = fpp;
            return this;
        }

        public Builder sparseIndexInterval(int interval) {
            this.sparseIndexInterval = interval;
            return this;
        }

        public Builder compactionMinTables(int n) {
            this.compactionMinTables = n;
            return this;
        }

        public Builder compactionSizeRatio(double ratio) {
            this.compactionSizeRatio = ratio;
            return this;
        }

        public EngineConfig build() {
            return new EngineConfig(this);
        }
    }

    @Override
    public String toString() {
        return "EngineConfig{dataDir=" + dataDir
                + ", memtableMB=" + (memtableMaxSizeBytes / (1024 * 1024))
                + ", fsync=" + fsyncPolicy
                + ", bloomFpp=" + bloomFpp
                + ", compactionMin=" + compactionMinTables + "}";
    }
}
