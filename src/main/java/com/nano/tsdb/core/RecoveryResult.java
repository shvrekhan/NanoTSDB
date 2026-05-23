package com.nano.tsdb.core;

import com.nano.tsdb.storage.memtable.MemTable;

/**
 * Result of crash recovery — contains the rebuilt memtable and replay stats.
 */
public record RecoveryResult(MemTable memtable, int replayedCount) {
}
