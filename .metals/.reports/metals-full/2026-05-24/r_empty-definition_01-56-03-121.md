error id: file://<WORKSPACE>/src/main/java/com/nano/tsdb/core/RecoveryManager.java:com/nano/tsdb/storage/memtable/OnHeapMemTable#
file://<WORKSPACE>/src/main/java/com/nano/tsdb/core/RecoveryManager.java
empty definition using pc, found symbol in pc: com/nano/tsdb/storage/memtable/OnHeapMemTable#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 115
uri: file://<WORKSPACE>/src/main/java/com/nano/tsdb/core/RecoveryManager.java
text:
```scala
package com.nano.tsdb.core;

import com.nano.tsdb.storage.memtable.MemTable;
import com.nano.tsdb.storage.memtable.@@OnHeapMemTable;
import com.nano.tsdb.storage.wal.WALEntry;
import com.nano.tsdb.storage.wal.WALManager;

import java.io.IOException;
import java.nio.file.Files;

/**
 * Orchestrates crash recovery on engine startup.
 */
public class RecoveryManager {

    public static RecoveryResult recover(EngineConfig config) throws IOException {
        createDirectories(config);

        MemTable recoveredMemtable = new OnHeapMemTable();
        int replayedCount = 0;

        if (Files.exists(config.getWalFilePath()) && Files.size(config.getWalFilePath()) > 0) {
            try (WALManager wal = new WALManager(config)) {
                for (WALEntry entry : wal.replay()) {
                    recoveredMemtable.put(entry.getSeriesId(), entry.getTimestamp(), entry.getValue());
                    replayedCount++;
                }
            }
        }

        return new RecoveryResult(recoveredMemtable, replayedCount);
    }

    private static void createDirectories(EngineConfig config) throws IOException {
        Files.createDirectories(config.getWalDir());
        Files.createDirectories(config.getSegmentsDir());
    }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: com/nano/tsdb/storage/memtable/OnHeapMemTable#