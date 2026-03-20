package com.nano.tsdb.nio;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MappedBuffer implements Closeable {

    private final MappedByteBuffer buffer;
    private final FileChannel channel;
    private final long size;

    private MappedBuffer(FileChannel channel, MappedByteBuffer buffer, long size) {
        this.channel = channel;
        this.buffer = buffer;
        this.size = size;
    }

    public static MappedBuffer open(Path file) throws IOException {
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
        long fileSize = ch.size();
        MappedByteBuffer mbb = ch.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
        return new MappedBuffer(ch, mbb, fileSize);
    }

    public static MappedBuffer open(Path file, long offset, long length) throws IOException {
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
        MappedByteBuffer mbb = ch.map(FileChannel.MapMode.READ_ONLY, offset, length);
        return new MappedBuffer(ch, mbb, length);
    }

    public int getInt(int offset) {
        return buffer.getInt(offset);
    }

    public long getLong(int offset) {
        return buffer.getLong(offset);
    }

    public double getDouble(int offset) {
        return buffer.getDouble(offset);
    }

    public byte getByte(int offset) {
        return buffer.get(offset);
    }

    public byte[] getBytes(int offset, int length) {
        byte[] dst = new byte[length];
        ByteBuffer dup = buffer.duplicate();
        dup.position(offset);
        dup.get(dst);
        return dst;
    }

    // returns a read-only view into a region — useful for sequential scans within a data block
    public ByteBuffer slice(int offset, int length) {
        ByteBuffer dup = buffer.duplicate();
        dup.position(offset);
        dup.limit(offset + length);
        return dup.slice().asReadOnlyBuffer();
    }

    public ByteBuffer asByteBuffer() {
        return buffer.duplicate().asReadOnlyBuffer();
    }

    public long size() {
        return size;
    }

    @Override
    public void close() throws IOException {
        // try to unmap eagerly instead of waiting for GC
        try {
            var cleaner = buffer.getClass().getMethod("cleaner");
            cleaner.setAccessible(true);
            Object c = cleaner.invoke(buffer);
            if (c != null) {
                c.getClass().getMethod("clean").invoke(c);
            }
        } catch (Exception ignored) {
            // not all JVMs expose cleaner — GC will handle it eventually
        }
        channel.close();
    }
}
