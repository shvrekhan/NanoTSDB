# NanoTSDB — Sprint-Wise Implementation Plan

> **Assumption**: You have zero prior knowledge of each topic. Every sprint starts with a 📚 **Pre-Reading** section. Read these BEFORE writing code.

---

## Sprint 1: Bug Fixes & Hardening (Day 1)

### 📚 Pre-Reading (1-2 hours)

| Topic | Resource | Why |
|-------|----------|-----|
| CRC32 vs hashCode for checksums | [CRC32 Explained (Computerphile)](https://www.youtube.com/watch?v=izG7qT0EpBw) | Samajhna h ki WAL me CRC32 kyun chahiye, hashCode kyun weak h |
| Java NIO FileChannel & fsync | [Baeldung — Java FileChannel](https://www.baeldung.com/java-filechannel) | WAL me FileChannel use ho rha h, `force()` kya krta h ye samjho |
| Java StandardCharsets | [Java Charset Encoding Issues](https://www.baeldung.com/java-char-encoding) | Kyun `getBytes()` without charset data corrupt kr sakta h |
| DirectByteBuffer cleanup | [Java Off-Heap Memory Guide](https://www.baeldung.com/java-off-heap-memory) | OffHeapMemTable me DirectByteBuffer ko properly release kaise kre |

### Tasks

#### 1.1 Fix ByteSerializer charset bug
- **File**: [ByteSerializer.java](file:///Users/shiva/Desktop/code/NanoTSDB/src/main/java/com/nano/tsdb/nio/ByteSerializer.java)
- Line 9: `getBytes()` → `getBytes(StandardCharsets.UTF_8)`
- Line 27: `new String(idBytes)` → `new String(idBytes, StandardCharsets.UTF_8)`

#### 1.2 WALEntry — CRC32 checksum + sequence number
- **File**: [WALEntry.java](file:///Users/shiva/Desktop/code/NanoTSDB/src/main/java/com/nano/tsdb/storage/wal/WALEntry.java)
- Replace `computeChecksum()` with `java.util.zip.CRC32`
- Add `long sequenceNumber` field for idempotent replay

#### 1.3 WALManager — sync + truncation + fsync policy
- **File**: [WALManager.java](file:///Users/shiva/Desktop/code/NanoTSDB/src/main/java/com/nano/tsdb/storage/wal/WALManager.java)
- Add `synchronized` on `append()`
- Add `truncate()` method — `channel.truncate(0); channel.position(0);`
- Add length sanity check in `replay()`: reject `len > 1MB`
- Accept `EngineConfig.FsyncPolicy` in constructor, apply in `append()`

#### 1.4 OffHeapMemTable — proper cleanup
- **File**: [OffHeapMemTable.java](file:///Users/shiva/Desktop/code/NanoTSDB/src/main/java/com/nano/tsdb/storage/memtable/OffHeapMemTable.java)
- Add `close()` method that releases DirectByteBuffer
- Add `clear()` to reset state

#### 1.5 MappedBuffer — fix cleaner for JDK 21
- **File**: [MappedBuffer.java](file:///Users/shiva/Desktop/code/NanoTSDB/src/main/java/com/nano/tsdb/nio/MappedBuffer.java)
- Add `--add-opens` JVM args in `pom.xml` for `maven-surefire-plugin`
- Or switch to `Unsafe.invokeCleaner()` approach

#### 1.6 Update ByteSerializer for new WALEntry fields
- **File**: [ByteSerializer.java](file:///Users/shiva/Desktop/code/NanoTSDB/src/main/java/com/nano/tsdb/nio/ByteSerializer.java)
- Encode/decode the new sequence number + CRC32 checksum

### ✅ Acceptance Criteria
- [ ] All existing code compiles with `mvn compile`
- [ ] ByteSerializer uses UTF-8 everywhere
- [ ] WALEntry uses CRC32 checksum
- [ ] WALManager has truncate() and synchronized append()

---

## Sprint 2: Engine Orchestration — NanoDB + WriteProcessor (Day 2-3)

> **This is THE most important sprint. This turns components into a working engine.**

### 📚 Pre-Reading (3-4 hours)

| Topic | Resource | Why |
|-------|----------|-----|
| LSM-Tree Architecture | [LSM-Tree Paper (short version)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) — OR better: [Understanding LSM Trees (Ben Stopford)](https://benstopford.com/2015/02/14/log-structured-merge-trees/) | Poora architecture samjhna h — WAL → Memtable → SSTable flow |
| How RocksDB Write Path Works | [RocksDB Wiki — Write Path](https://github.com/facebook/rocksdb/wiki/WriteBatch) | Real-world me write path kaise kaam krta h — inspiration ke liye |
| AtomicReference & CAS in Java | [Baeldung — AtomicReference](https://www.baeldung.com/java-atomic-variables) | Memtable rotation ke liye CAS samjhna zaroori h |
| Java ConcurrentSkipListMap | [Baeldung — ConcurrentSkipListMap](https://www.baeldung.com/java-concurrent-skip-list-map) | Memtable isi se bna h, concurrent reads kaise kaam krte h |
| BlockingQueue & Producer-Consumer | [Baeldung — BlockingQueue](https://www.baeldung.com/java-blocking-queue) | WriteProcessor ka single-writer pattern isi pe based h |
| CompletableFuture | [Baeldung — CompletableFuture Guide](https://www.baeldung.com/java-completablefuture) | Async write acknowledgment ke liye chahiye |
| CopyOnWriteArrayList | [Baeldung — CopyOnWriteArrayList](https://www.baeldung.com/java-copy-on-write-arraylist) | SSTable list manage krne ke liye — readers never block |

### Tasks

#### 2.1 [NEW] `StorageEngine.java` — Interface
- **Path**: `src/main/java/com/nano/tsdb/core/StorageEngine.java`
- Methods: `write()`, `read()`, `scan()`, `flush()`, `shutdown()`

#### 2.2 [NEW] `SSTableManager.java` — SSTable Lifecycle
- **Path**: `src/main/java/com/nano/tsdb/storage/sstable/SSTableManager.java`
- `discoverExisting(Path dir)` — find `.sst` files, open scanners, sort by name (newest first)
- `register(SSTableMetadata)` — add new SSTable after flush
- `remove(List<Path>)` — delete after compaction
- `getActiveScanners()` — return `CopyOnWriteArrayList` for reads
- Naming: `segment_<epochMillis>_<seq>.sst`

#### 2.3 [MODIFY] `NanoDB.java` — The Engine Facade (BIGGEST task)
- **File**: [NanoDB.java](file:///Users/shiva/Desktop/code/NanoTSDB/src/main/java/com/nano/tsdb/core/NanoDB.java)
- **State**:
  - `AtomicReference<MemTable> activeMemtable`
  - `CopyOnWriteArrayList<MemTable> immutableMemtables`
  - `SSTableManager sstableManager`
  - `WALManager walManager`
  - `ExecutorService flushExecutor` (single thread)
  - `EngineConfig config`
- **Write path**: WAL append → memtable put → check full → freeze + CAS swap → submit flush
- **Read path**: active memtable → immutable memtables → SSTables (bloom filter skip)
- **Scan**: collect from all sources → merge by timestamp → dedup
- **Flush**: snapshot memtable → SSTableWriter → register → truncate WAL
- **`open(config)`**: static factory — runs recovery, returns ready engine
- **`shutdown()`**: flush active memtable → close WAL → close SSTables

#### 2.4 [MODIFY] `WriteProcessor.java` — Single-Writer Queue
- **File**: [WriteProcessor.java](file:///Users/shiva/Desktop/code/NanoTSDB/src/main/java/com/nano/tsdb/core/WriteProcessor.java)
- `LinkedBlockingQueue<WriteRequest>` for incoming writes
- Single daemon thread drains queue, calls `engine.write()`
- `WriteRequest` contains: seriesId, timestamp, value, `CompletableFuture<Void>`
- Supports batch drain: drain N entries, single fsync at end
- `submit(seriesId, ts, value)` returns `CompletableFuture<Void>`

#### 2.5 [NEW] `RecoveryManager.java` — Crash Recovery
- **Path**: `src/main/java/com/nano/tsdb/core/RecoveryManager.java`
- `recover(EngineConfig config)`:
  1. Create directories if missing (`data/wal/`, `data/segments/`)
  2. Discover existing SSTables → open scanners
  3. Open WAL → replay valid entries into fresh memtable
  4. Return `RecoveryResult(memtable, sstableList, replayedCount)`

### ✅ Acceptance Criteria
- [ ] Can do: `NanoDB engine = NanoDB.open(config)` → `engine.write("cpu", ts, 72.5)` → `engine.read("cpu", ts)` returns `72.5`
- [ ] Memtable auto-flushes to SSTable when full
- [ ] After flush, reads still work (SSTable path)
- [ ] Restart engine → data survives (recovery works)
- [ ] WriteProcessor accepts async writes via queue

---

## Sprint 3: Compaction (Day 4)

### 📚 Pre-Reading (2-3 hours)

| Topic | Resource | Why |
|-------|----------|-----|
| Size-Tiered Compaction (STCS) | [ScyllaDB — Compaction Strategies](https://opensource.docs.scylladb.com/stable/architecture/compaction/compaction-strategies.html) | STCS kya h, kab trigger hota h, pros/cons |
| Leveled vs Size-Tiered vs FIFO | [Cassandra Compaction Deep Dive (YouTube)](https://www.youtube.com/watch?v=bCAW5kOmKdY) | Different strategies ka comparison — context ke liye |
| K-Way Merge Algorithm | [GeeksforGeeks — K-Way Merge](https://www.geeksforgeeks.org/merge-k-sorted-arrays/) | PriorityQueue se multiple sorted streams merge kaise kre |
| Java PriorityQueue | [Baeldung — PriorityQueue](https://www.baeldung.com/java-priority-queue) | Merger me k-way merge ke liye PriorityQueue use hoga |
| Java ScheduledExecutorService | [Baeldung — ScheduledExecutorService](https://www.baeldung.com/java-executor-service-tutorial#ScheduledExecutorService) | Background compaction periodic kaise schedule kre |

### Tasks

#### 3.1 [MODIFY] `Strategy.java` → Convert to `CompactionStrategy` interface
- **File**: [Strategy.java](file:///Users/shiva/Desktop/code/NanoTSDB/src/main/java/com/nano/tsdb/compaction/Strategy.java)
- Rename to `CompactionStrategy.java`
- Method: `List<List<SSTableMetadata>> selectCompactionGroups(List<SSTableMetadata> tables)`

#### 3.2 [NEW] `SizeTieredStrategy.java`
- **Path**: `src/main/java/com/nano/tsdb/compaction/SizeTieredStrategy.java`
- Group SSTables where file sizes are within `sizeRatio` of each other
- Return groups where `group.size() >= minTables`
- Uses config: `compactionMinTables=4`, `compactionSizeRatio=1.5`

#### 3.3 [MODIFY] `Merger.java` — K-Way Merge
- **File**: [Merger.java](file:///Users/shiva/Desktop/code/NanoTSDB/src/main/java/com/nano/tsdb/compaction/Merger.java)
- Input: `List<SSTableScanner>` (each has `.iterator()`)
- Create `PeekableIterator` wrapper
- `PriorityQueue<PeekableIterator>` ordered by `DataPoint.compareTo()`
- Dedup: same (seriesId, timestamp) → keep from newer SSTable
- Output: sorted `List<DataPoint>` → write via `SSTableWriter`

#### 3.4 [NEW] `CompactionWorker.java`
- **Path**: `src/main/java/com/nano/tsdb/compaction/CompactionWorker.java`
- `ScheduledExecutorService` — runs every 30 seconds
- Flow: `strategy.selectGroups()` → `merger.merge()` → register new SSTable → delete old ones
- Logs: input count, output size, duration

#### 3.5 Wire compaction into NanoDB
- After flush, trigger compaction check
- CompactionWorker calls back into `SSTableManager` to swap tables

### ✅ Acceptance Criteria
- [ ] Write enough data to create 4+ SSTables → compaction merges them into 1
- [ ] Reads still work during and after compaction
- [ ] Old SSTable files deleted after merge
- [ ] Duplicate keys resolved correctly (latest wins)

---

## Sprint 4: Tests (Day 5-6)

### 📚 Pre-Reading (1-2 hours)

| Topic | Resource | Why |
|-------|----------|-----|
| JUnit 5 Basics | [Baeldung — JUnit 5 Guide](https://www.baeldung.com/junit-5) | Test kaise likhte h — `@Test`, `@BeforeEach`, `@TempDir`, assertions |
| Testing with Temp Directories | [Baeldung — JUnit 5 TempDir](https://www.baeldung.com/junit-5-temporary-directory) | WAL/SSTable tests me temporary files chahiye |
| Java Testing Best Practices | [Effective Unit Testing Patterns](https://phauer.com/2019/modern-best-practices-testing-java/) | Achhe tests kaise likhte h — naming, structure, assertions |

### Tasks

#### 4.1 WAL Tests — `src/test/java/com/nano/tsdb/storage/wal/WALManagerTest.java`
- Append 100 entries → replay → verify all recovered
- Append → corrupt last bytes → replay → verify stops at corruption
- CRC32 mismatch detection
- Truncate → verify empty replay
- Fsync policy behavior (EVERY_WRITE vs BATCH)

#### 4.2 Memtable Tests — `src/test/java/com/nano/tsdb/storage/memtable/`
- `OnHeapMemTableTest.java`: put/get, scan range, freeze throws on write, snapshot sorted, size estimation
- `OffHeapMemTableTest.java`: same tests + buffer full detection + cleanup

#### 4.3 SSTable Tests — `src/test/java/com/nano/tsdb/storage/sstable/SSTableRoundtripTest.java`
- Write 1000 sorted points → scanner reads all back
- Bloom filter skips absent keys
- Point lookup correctness
- Range scan correctness
- Iterator order verification

#### 4.4 Index Tests
- `BloomFilterTest.java`: no false negatives, FPP < 2% on 10K keys, serialization roundtrip
- `SparseIndexTest.java`: findOffset/findEndOffset correctness, serialization roundtrip

#### 4.5 Integration Test — `src/test/java/com/nano/tsdb/core/NanoDBIntegrationTest.java`
- Write 1000 points → read each back
- Force flush → verify reads from SSTable
- Scan across memtable + SSTable boundary
- Shutdown → restart → verify data survived

#### 4.6 [MODIFY] CrashRecoveryTest
- Write N → flush some → simulate crash (don't flush rest) → recover → verify all N
- Corrupt WAL tail → recover → verify partial recovery

### ✅ Acceptance Criteria
- [ ] `mvn test` — ALL tests pass
- [ ] Coverage: every public method of WAL, Memtable, SSTable, BloomFilter, SparseIndex, NanoDB

---

## Sprint 5: TCP Server + Observability + Benchmarks (Day 7-8)

### 📚 Pre-Reading (3-4 hours)

| Topic | Resource | Why |
|-------|----------|-----|
| Java NIO Selector & ServerSocketChannel | [Baeldung — Java NIO Selector](https://www.baeldung.com/java-nio-selector) | TCP server NIO se bnaana h — non-blocking I/O |
| Java NIO — Non-Blocking Server | [Jenkov — Java NIO Server](http://tutorials.jenkov.com/java-nio/server-socket-channel.html) | Step-by-step non-blocking server tutorial |
| LongAdder vs AtomicLong | [Baeldung — LongAdder](https://www.baeldung.com/java-longadder-longaccumulator) | Metrics ke liye LongAdder kyun better h high contention me |
| JMH — Java Microbenchmark Harness | [Baeldung — JMH Guide](https://www.baeldung.com/java-microbenchmark-harness) | Benchmarks kaise likhte h — `@Benchmark`, `@State`, `@BenchmarkMode` |
| JMH — Avoiding Common Pitfalls | [Aleksey Shipilev — JMH Samples](https://github.com/openjdk/jmh/tree/master/jmh-samples/src/main/java/org/openjdk/jmh/samples) | JMH ke official samples — dead code elimination, constant folding se kaise bche |

### Tasks

#### 5.1 [NEW] `EngineMetrics.java` — `com.nano.tsdb.core`
- `LongAdder` counters: writesTotal, readsTotal, readHits, readMisses, bloomHits, bloomFalsePositives, flushCount, compactionCount
- `AtomicLong` gauges: activeMemtableSize, sstableCount
- `toString()` for human-readable output
- Wire into NanoDB — increment counters at each operation

#### 5.2 [NEW] `TcpServer.java` — `com.nano.tsdb.server`
- `ServerSocketChannel` + `Selector` based non-blocking server
- Parse line protocol: `WRITE`, `READ`, `SCAN`, `METRICS`
- Route writes through `WriteProcessor`
- Route reads directly to `NanoDB`
- `METRICS` command returns `EngineMetrics.toString()`

#### 5.3 [NEW] `Main.java` — `com.nano.tsdb`
- Boot: `EngineConfig` → `NanoDB.open()` → `TcpServer.start(9091)`
- Shutdown hook: `engine.shutdown()`
- Configure maven-jar-plugin for executable JAR in pom.xml

#### 5.4 [MODIFY] `LatencyBenchmark.java`
- `@Benchmark writeLatency` — single write P50/P99
- `@Benchmark pointReadLatency` — read from memtable vs SSTable
- `@Benchmark rangeScanThroughput` — scan 1000 points
- `@Benchmark heapVsOffHeap` — compare GC pauses
- Setup/teardown with `@State(Scope.Benchmark)` and `@TempDir`

### ✅ Acceptance Criteria
- [ ] `java -jar target/nanotsdb.jar` starts TCP server on port 9091
- [ ] `echo "WRITE cpu 1000 72.5" | nc localhost 9091` → `OK`
- [ ] `echo "READ cpu 1000" | nc localhost 9091` → `VALUE 72.5`
- [ ] `echo "METRICS" | nc localhost 9091` → shows counters
- [ ] JMH benchmarks run and produce latency numbers

---

## Sprint 6: Final Polish (Day 9)

### 📚 Pre-Reading (30 min)

| Topic | Resource | Why |
|-------|----------|-----|
| Writing a Great README | [Make a README](https://www.makeareadme.com/) | README ko professional level pe kaise le jaaye |
| Git commit message conventions | [Conventional Commits](https://www.conventionalcommits.org/) | Clean git history for reviewers |

### Tasks

#### 6.1 README — already updated (verify accuracy)
- Cross-check every claim against actual code
- Add benchmark results table after JMH runs
- Verify Quick Start instructions work end-to-end

#### 6.2 Code cleanup
- Remove any TODO comments
- Add Javadoc to all public classes and methods
- Ensure consistent code formatting
- Add `.gitignore` entries for `data/` directory

#### 6.3 Git history cleanup
- Squash/rebase into clean, logical commits per sprint
- Tag `v1.0` release

#### 6.4 Final verification
```bash
# Full build + test
mvn clean verify

# Start server, write, read, scan, metrics
java -jar target/nanotsdb-1.0-SNAPSHOT.jar &
echo "WRITE cpu.load 1714000000000 72.5" | nc localhost 9091
echo "READ cpu.load 1714000000000" | nc localhost 9091
echo "SCAN cpu.load 1714000000000 1714000060000" | nc localhost 9091
echo "METRICS" | nc localhost 9091

# Run benchmarks
java -jar target/benchmarks.jar
```

### ✅ Acceptance Criteria
- [ ] `mvn clean verify` — zero failures
- [ ] TCP server works end-to-end
- [ ] README matches code reality 100%
- [ ] Someone can clone, build, and run in < 2 minutes

---

## Sprint Overview

| Sprint | Focus | Days | Pre-Reading |
|--------|-------|------|-------------|
| **1** | Bug fixes & hardening | Day 1 | CRC32, FileChannel, Charset, Off-heap memory |
| **2** | Engine orchestration (NanoDB, WriteProcessor, Recovery) | Day 2-3 | LSM-trees, AtomicReference, CAS, BlockingQueue, CopyOnWriteArrayList |
| **3** | Compaction (STCS, K-way merge, background worker) | Day 4 | Compaction strategies, PriorityQueue, ScheduledExecutor |
| **4** | Tests (unit + integration + crash recovery) | Day 5-6 | JUnit 5, TempDir, testing patterns |
| **5** | TCP Server + Metrics + JMH Benchmarks | Day 7-8 | NIO Selector, LongAdder, JMH |
| **6** | Final polish | Day 9 | README best practices |

> [!IMPORTANT]
> **Sprint 2 is the heart of this project.** Don't rush it. Read the LSM-tree and AtomicReference resources carefully before starting. Everything else builds on top of the engine facade.

Shall I start executing Sprint 1?
