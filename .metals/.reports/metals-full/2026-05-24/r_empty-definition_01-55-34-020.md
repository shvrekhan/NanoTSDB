error id: file://<WORKSPACE>/src/main/java/com/nano/tsdb/core/RecoveryResult.java:com/nano/tsdb/storage/memtable/MemTable#
file://<WORKSPACE>/src/main/java/com/nano/tsdb/core/RecoveryResult.java
empty definition using pc, found symbol in pc: com/nano/tsdb/storage/memtable/MemTable#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 67
uri: file://<WORKSPACE>/src/main/java/com/nano/tsdb/core/RecoveryResult.java
text:
```scala
package com.nano.tsdb.core;

import com.nano.tsdb.storage.memtable.@@MemTable;

/**
 * Result of crash recovery — contains the rebuilt memtable and replay stats.
 */
public record RecoveryResult(MemTable memtable, int replayedCount) {
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: com/nano/tsdb/storage/memtable/MemTable#