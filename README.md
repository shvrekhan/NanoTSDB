# NanoTSDB

A JVM-based Time Series Storage Engine focused on predictable write performance and bounded tail latency.

## 📌 Motivation

Modern time-series workloads are write-heavy, latency-sensitive, and often run in regulated environments where predictability matters more than raw throughput.

Most LSM-based databases optimize for ingestion speed but suffer from:

- Unpredictable P99 latency spikes during compaction
- GC pauses due to heap-heavy write paths
- Read performance degradation during background maintenance

NanoTSDB is an experiment to explore how far we can push predictable, low-latency storage design on the JVM by being explicit about:

- Memory placement (heap vs off-heap)
- Write serialization via single-writer principle
- Compaction isolation from foreground traffic
- Crash consistency via WAL with CRC32 integrity

## 🎯 Design Goals

- Deterministic write path (bounded latency)
- Crash-safe ingestion via Write-Ahead Log
- GC-aware memory layout (heap + off-heap memtable variants)
- Single-writer principle — minimal concurrency, maximum predictability
- Simple storage primitives over features

### Non-goals

- SQL query engine
- Distributed consensus
- High-level analytics
- UI / dashboards

## 🧠 High-Level Architecture

```
                    ┌─────────────────┐
                    │   TCP Server    │
                    │  (line protocol)│
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ WriteProcessor  │
                    │ (single-writer  │
                    │    queue)       │
                    └────────┬────────┘
                             │
              ┌──────────────▼──────────────┐
              │       NanoDB Engine          │
              │                              │
              │  ┌─────────┐  ┌───────────┐ │
              │  │   WAL   │  │  Active    │ │
              │  │(append- │──▶ Memtable  │ │
              │  │  only)  │  │(mutable)  │ │
              │  └─────────┘  └─────┬─────┘ │
              │                     │freeze  │
              │               ┌─────▼─────┐ │
              │               │ Immutable  │ │
              │               │ Memtable   │ │
              │               └─────┬─────┘ │
              │                     │flush   │
              │  ┌──────────────────▼──────┐ │
              │  │     SSTable (disk)      │ │
              │  │  [data][index][bloom]   │ │
              │  │  [footer + magic]       │ │
              │  └──────────┬──────────────┘ │
              │             │                │
              │  ┌──────────▼──────────────┐ │
              │  │   Compaction Worker     │ │
              │  │  (size-tiered, async)   │ │
              │  └─────────────────────────┘ │
              └──────────────────────────────┘
```

Each stage is explicitly isolated to avoid tail-latency amplification.

## 🧱 Core Components

### 1️⃣ Write-Ahead Log (WAL)

- Append-only log with configurable fsync policy (`EVERY_WRITE` / `BATCH` / `NONE`)
- CRC32 checksums per entry for data integrity
- Sequence numbers for idempotent replay
- Guarantees durability before memtable mutation
- Truncation after successful SSTable flush

**Design choices:**
- Single writer thread owns the WAL — no concurrent append contention
- Sequential disk writes via `FileChannel`
- Length-prefixed binary encoding with corruption detection on replay

### 2️⃣ Memtable

- In-memory sorted structure backed by `ConcurrentSkipListMap`
- Two implementations behind the `MemTable` interface:
  - **OnHeapMemTable** — simpler, GC-visible, uses `AtomicLong` estimated size tracking
  - **OffHeapMemTable** — `DirectByteBuffer`-backed, lower GC pressure, on-heap index pointing to off-heap data
- Freeze-on-threshold triggers background flush to SSTable
- Memtable rotation via `AtomicReference` CAS swap — lock-free for readers

**Trade-offs:**
- Heap: simpler code, GC pressure scales with data volume
- Off-heap: near-zero GC impact, but risk of OS page faults and more complex memory management

### 3️⃣ SSTable

- Immutable on-disk sorted table with custom binary format
- Memory-mapped reads (`mmap` via `MappedByteBuffer`)
- File layout: `[data block][sparse index block][bloom filter block][footer]`
- Footer contains: `indexOffset(8) | bloomOffset(8) | entryCount(4) | magic(4)` — magic = `0x4E545344` ("NTSD")
- Sparse index (sampled every N entries) narrows the mmap scan window via binary search
- Bloom filter (FNV-1a + double-hashing) for fast negative lookups

**Why mmap?**
- Zero-copy reads — data goes directly from page cache to application
- OS page cache handles eviction automatically
- Lower syscall overhead compared to `pread()`

### 4️⃣ Compaction

- Size-tiered compaction strategy (STCS)
- Groups SSTables by similar file sizes, merges when bucket exceeds threshold
- K-way merge using `PriorityQueue` over `SSTableScanner` iterators
- Duplicate resolution: latest timestamp wins
- Runs asynchronously on a `ScheduledExecutorService`
- Read isolation: old SSTables remain open until compaction completes, then atomically swapped

**Key focus:**
- Prevent compaction from blocking reads or writes
- Configurable via `compactionMinTables` and `compactionSizeRatio` in EngineConfig

### 5️⃣ TCP Ingestion Server

- Non-blocking NIO server using `ServerSocketChannel` + `Selector`
- Simple line protocol for writes, reads, and range scans
- Routes all mutations through the `WriteProcessor` queue

```
Protocol:
  WRITE <seriesId> <timestamp> <value>\n
  READ <seriesId> <timestamp>\n
  SCAN <seriesId> <from> <to>\n
```

### 6️⃣ Observability

- Lock-free counters via `LongAdder`
- Tracked metrics: write throughput, read hit/miss ratio, bloom filter effectiveness, flush count/duration, compaction count/duration, memtable size, SSTable count

## 🔁 Write Path (Step-by-Step)

1. Client sends write over TCP (or direct API call)
2. `WriteProcessor` accepts into single-writer queue
3. Writer thread drains queue, for each entry:
   - Append to WAL with CRC32 checksum
   - Fsync based on configured policy
   - Mutation applied to active memtable
4. When memtable exceeds size threshold:
   - Freeze active memtable
   - CAS-swap new active memtable via `AtomicReference`
   - Submit flush task to background executor
5. Flush task:
   - Snapshot frozen memtable → sorted `List<DataPoint>`
   - Write to SSTable via `SSTableWriter` (builds bloom filter + sparse index)
   - Register new SSTable with engine
   - Truncate WAL

## 🔍 Read Path

1. Check active memtable (most recent data)
2. Check immutable memtables (pending flush, newest first)
3. For each SSTable (newest first):
   - Bloom filter → skip if key definitely absent
   - Sparse index → binary search to narrow scan window
   - Sequential scan within narrowed mmap window
4. Return first non-null result (point read) or merge results across levels (range scan)

## 💥 Crash Recovery

On restart:
1. **Discover SSTables** — scan segments directory for `.sst` files, open scanners
2. **Replay WAL** — deserialize entries, verify CRC32 checksums, skip corrupt tail
3. **Rebuild memtable** — apply replayed entries to fresh memtable
4. **Resume ingestion** — engine ready to accept new writes

No partial writes, no silent corruption. Corrupt WAL entries are detected and replay stops at the corruption boundary.

## ⚙️ Concurrency Model

- **Single-writer principle**: all mutations flow through one thread via `WriteProcessor` queue
- **Lock-free read path**: readers access memtable (`ConcurrentSkipListMap`) and SSTables (mmap) without locks
- **CAS-based memtable rotation**: `AtomicReference.compareAndSet()` swaps active memtable pointer
- **Background flush**: single-thread executor, isolated from write path
- **Background compaction**: scheduled executor, isolated from both reads and writes
- **Copy-on-write SSTable list**: readers always see a consistent snapshot

Fewer threads, fewer surprises.

## 🧠 JVM & Memory Considerations

- GC tuning experiments (G1, ZGC) — measure pause impact on write latency
- Heap sizing vs mmap reliance trade-offs
- Off-heap `DirectByteBuffer` for memtable to reduce GC scanning
- Eager mmap cleanup to avoid GC-delayed `MappedByteBuffer` unmap
- `LongAdder` over `AtomicLong` for contended counters
- `ConcurrentSkipListMap` chosen over `TreeMap` + locks for better concurrent read throughput

This project treats the JVM as a runtime to be reasoned about, not abstracted away.

## 🧪 Benchmarks & Metrics

NanoTSDB focuses on observability over leaderboard scores.

**Measured via JMH:**
- Single write latency (P50 / P99)
- Batch write throughput (writes/sec)
- Point read latency (memtable vs SSTable)
- Range scan throughput
- Write latency under compaction load
- Heap vs off-heap memtable GC comparison

**Runtime metrics (LongAdder-based):**
- Write/read counters and error rates
- Bloom filter hit rates and false positive ratio
- Flush and compaction duration tracking
- Active memtable size and SSTable count

## 🛠 Tech Stack

- Java 21
- NIO / FileChannel / mmap (`MappedByteBuffer`)
- JMH (benchmarks)
- JUnit 5 (tests)
- Zero runtime dependencies by design

## 📚 What This Project Demonstrates

- Storage engine internals (LSM-tree architecture from scratch)
- JVM internals awareness (GC, off-heap memory, mmap behavior)
- Write-path determinism via single-writer principle
- Custom binary file formats with integrity checks
- Crash recovery and durability guarantees
- Real-world trade-off thinking (heap vs off-heap, fsync policies, bloom filter tuning)
- Systems-level debugging mindset

## 🚧 Current Status

- ✅ WAL (append, fsync, CRC32, replay, truncation)
- ✅ Memtable (on-heap + off-heap variants)
- ✅ SSTable (custom binary format, mmap reads)
- ✅ Bloom Filter (FNV-1a, double-hashing, serializable)
- ✅ Sparse Index (binary search, serializable)
- ✅ Engine Facade (NanoDB — write/read/scan/flush/recovery)
- ✅ Write Processor (single-writer queue)
- ✅ Crash Recovery (WAL replay + SSTable discovery)
- ✅ Compaction (size-tiered, k-way merge, background worker)
- ✅ TCP Server (NIO, line protocol)
- ✅ Observability (LongAdder metrics)
- ✅ JMH Benchmarks (latency, throughput, GC impact)
- ✅ Comprehensive Tests (unit + integration + crash recovery)

## 🚀 Quick Start

```bash
# Build
mvn clean package -DskipTests

# Run (starts TCP server on port 9091)
java -jar target/nanotsdb-1.0-SNAPSHOT.jar

# Write data
echo "WRITE cpu.usage 1714000000000 72.5" | nc localhost 9091

# Read data
echo "READ cpu.usage 1714000000000" | nc localhost 9091

# Range scan
echo "SCAN cpu.usage 1714000000000 1714000060000" | nc localhost 9091
```

## 🧠 Why NanoTSDB Exists

**This is not a production database.**
It is a thinking exercise in building predictable systems on the JVM — exploring the boundaries of deterministic performance, crash safety, and memory-aware design within the constraints of a managed runtime.

## 📄 License

MIT