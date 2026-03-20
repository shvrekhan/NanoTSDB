package com.nano.tsdb.index;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Standard bloom filter with double-hashing.
 * Used on the read path to skip SSTables that definitely don't have a key.
 */
public class BloomFilter {

    private final BitSet bits;
    private final int numBits;
    private final int numHashFunctions;

    public BloomFilter(int expectedInsertions, double fpp) {
        // m = -n * ln(p) / (ln2)^2
        this.numBits = Math.max(64, (int) (-expectedInsertions * Math.log(fpp) / (Math.log(2) * Math.log(2))));
        // k = (m/n) * ln2
        this.numHashFunctions = Math.max(1, (int) Math.round((double) numBits / expectedInsertions * Math.log(2)));
        this.bits = new BitSet(numBits);
    }

    private BloomFilter(BitSet bits, int numBits, int numHashFunctions) {
        this.bits = bits;
        this.numBits = numBits;
        this.numHashFunctions = numHashFunctions;
    }

    public void add(String key) {
        long hash64 = fnvHash(key);
        int h1 = (int) hash64;
        int h2 = (int) (hash64 >>> 32);

        for (int i = 0; i < numHashFunctions; i++) {
            int idx = ((h1 + i * h2) & Integer.MAX_VALUE) % numBits;
            bits.set(idx);
        }
    }

    public boolean mightContain(String key) {
        long hash64 = fnvHash(key);
        int h1 = (int) hash64;
        int h2 = (int) (hash64 >>> 32);

        for (int i = 0; i < numHashFunctions; i++) {
            int idx = ((h1 + i * h2) & Integer.MAX_VALUE) % numBits;
            if (!bits.get(idx)) return false;
        }
        return true;
    }

    // FNV-1a 64-bit — fast, decent distribution, good enough for bloom filters
    private static long fnvHash(String key) {
        long h = 0xcbf29ce484222325L;
        for (byte b : key.getBytes()) {
            h ^= b;
            h *= 0x100000001b3L;
        }
        // avalanche mixing
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    // serialization — layout: [numBits:4][numHash:4][bitset bytes]

    public byte[] toBytes() {
        byte[] bitData = bits.toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(4 + 4 + bitData.length);
        buf.putInt(numBits);
        buf.putInt(numHashFunctions);
        buf.put(bitData);
        return buf.array();
    }

    public static BloomFilter fromBytes(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int m = buf.getInt();
        int k = buf.getInt();
        byte[] bitData = new byte[buf.remaining()];
        buf.get(bitData);
        return new BloomFilter(BitSet.valueOf(bitData), m, k);
    }

    public int getNumBits() {
        return numBits;
    }

    public int getNumHashFunctions() {
        return numHashFunctions;
    }
}
