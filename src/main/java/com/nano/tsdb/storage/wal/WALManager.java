package com.nano.tsdb.storage.wal;

import com.nano.tsdb.nio.ByteSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class WALManager {

    private final FileChannel channel;

    public WALManager(Path walPath) throws IOException {
        this.channel = FileChannel.open(walPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.APPEND);
    }

    public void append(WALEntry entry) throws IOException {
        byte[] bytes = ByteSerializer.encode(entry);
        ByteBuffer lenBuf = ByteBuffer.allocate(4).putInt(bytes.length);
        lenBuf.flip();
        channel.write(lenBuf);
        channel.write(ByteBuffer.wrap(bytes));
    }

    public void fsync() throws IOException {
        channel.force(false);
    }

    public List<WALEntry> replay() throws IOException {
        List<WALEntry> entries = new ArrayList<>();
        channel.position(0);

        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        while (channel.read(lenBuf) == 4) {
            lenBuf.flip();
            int len = lenBuf.getInt();
            lenBuf.clear();

            ByteBuffer dataBuf = ByteBuffer.allocate(len);
            channel.read(dataBuf);
            try {
                entries.add(ByteSerializer.decode(dataBuf.array()));
            } catch (IllegalStateException e) {
                break; // corrupt entry — replay yahan rok do
            }
        }
        return entries;
    }

    public void close() throws IOException {
        channel.close();
    }
}
