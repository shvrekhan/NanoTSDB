package com.nano.tsdb.storage.wal;

import com.nano.tsdb.core.EngineConfig;
import com.nano.tsdb.nio.ByteSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class WALManager {

    private final FileChannel channel;
    private final EngineConfig.FsyncPolicy fsyncPolicy;
    private final int batchFsyncSize;
    private final AtomicLong sequenceNumber;
    private int pendingWrites;

    public WALManager(Path walPath, EngineConfig.FsyncPolicy fsyncPolicy, int batchFsyncSize) throws IOException {
        this.channel = FileChannel.open(walPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.APPEND);
        this.fsyncPolicy = fsyncPolicy;
        this.batchFsyncSize = batchFsyncSize;
        this.sequenceNumber = new AtomicLong(0);
        this.pendingWrites = 0;
    }

    public WALManager(EngineConfig config) throws IOException {
        this(config.getWalFilePath(), config.getFsyncPolicy(), config.getBatchFsyncSize());
    }

    public synchronized void append(WALEntry entry) throws IOException {
        byte[] bytes = ByteSerializer.encode(entry);
        ByteBuffer lenBuf = ByteBuffer.allocate(4).putInt(bytes.length);
        lenBuf.flip();
        channel.write(lenBuf);
        channel.write(ByteBuffer.wrap(bytes));

        pendingWrites++;
        if (fsyncPolicy == EngineConfig.FsyncPolicy.EVERY_WRITE) {
            channel.force(false);
            pendingWrites = 0;
        } else if (fsyncPolicy == EngineConfig.FsyncPolicy.BATCH && pendingWrites >= batchFsyncSize) {
            channel.force(false);
            pendingWrites = 0;
        }
        // NONE: never fsync
    }

    public void fsync() throws IOException {
        channel.force(false);
        pendingWrites = 0;
    }

    public List<WALEntry> replay() throws IOException {
        List<WALEntry> entries = new ArrayList<>();
        channel.position(0);

        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        while (channel.read(lenBuf) == 4) {
            lenBuf.flip();
            int len = lenBuf.getInt();
            lenBuf.clear();

            if (len < 0 || len > 1_048_576) { // 1MB sanity check
                break; // corrupt length prefix
            }

            ByteBuffer dataBuf = ByteBuffer.allocate(len);
            if (channel.read(dataBuf) != len) {
                break; // truncated entry
            }
            try {
                entries.add(ByteSerializer.decode(dataBuf.array()));
            } catch (IllegalStateException e) {
                break; // corrupt entry — stop replay here
            }
        }
        return entries;
    }

    public synchronized void truncate() throws IOException {
        channel.truncate(0);
        channel.position(0);
        pendingWrites = 0;
    }

    public synchronized long nextSequenceNumber() {
        return sequenceNumber.incrementAndGet();
    }

    public void close() throws IOException {
        channel.close();
    }
}
