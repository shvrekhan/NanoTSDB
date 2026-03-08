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
- Write serialization
- Compaction isolation
- Crash consistency

## 🎯 Design Goals

- Deterministic write path (bounded latency)
- Crash-safe ingestion via WAL
- GC-aware memory layout
- Minimal concurrency, maximum predictability
- Simple storage primitives over features

### Non-goals

- SQL query engine
- Distributed consensus
- High-level analytics
- UI / dashboards

## 🧠 High-Level Architecture

```
Client
  ↓
TCP Ingestion Server
  ↓
Write Coordinator (single writer)
  ↓
Write-Ahead Log (append-only)
  ↓
Active Memtable (mutable)
  ↓ (freeze)
Immutable Memtable
  ↓
SSTable (disk, mmap)
  ↓
Compaction Worker
```

Each stage is explicitly isolated to avoid tail-latency amplification.

## 🧱 Core Components

### 1️⃣ Write-Ahead Log (WAL)

- Append-only log with configurable fsync policy
- Guarantees durability before memtable mutation
- Replay-based crash recovery

**Design choices:**
- Single writer thread
- Sequential disk writes
- Idempotent replay

### 2️⃣ Memtable

- In-memory sorted structure
- Heap-backed (baseline) and off-heap (experimental)
- Freeze-on-threshold to avoid write stalls

**Trade-offs:**
- Heap: simpler, GC pressure
- Off-heap: lower GC, higher page-fault risk

### 3️⃣ SSTable

- Immutable on-disk sorted table
- Memory-mapped reads (mmap)
- Sparse index + Bloom filter for fast lookups

**Why mmap?**
- Zero-copy reads
- OS page cache handles eviction
- Lower syscall overhead

### 4️⃣ Compaction

- Size-tiered compaction strategy
- Runs asynchronously
- Read isolation to avoid latency spikes

**Key focus:**
- Prevent compaction from blocking reads or writes
- Measure impact on P99 latency explicitly

## 🔁 Write Path (Step-by-Step)

1. Client sends write over TCP
2. Entry appended to WAL
3. WAL fsync (policy-based)
4. Mutation applied to active memtable
5. Memtable freeze when size threshold reached
6. Immutable memtable flushed to SSTable

## 🔍 Read Path

1. Bloom filter → skip disk if absent
2. Sparse index → narrow mmap window
3. Sequential read from SSTable
4. Merge results across levels (if needed)

## 🧪 Benchmarks & Metrics

NanoTSDB focuses on observability over benchmarks.

**Measured metrics:**
- Write throughput
- P50 / P99 write latency
- GC pause duration
- Compaction impact on reads
- Heap vs off-heap memory behavior

Benchmarks are intentionally simple to highlight design trade-offs, not leaderboard scores.

## ⚙️ Concurrency Model

- Single-writer principle for WAL + memtable
- Lock-free read path
- CAS-based state transitions during memtable rotation
- Background compaction isolated from foreground traffic

Fewer threads, fewer surprises.

## 💥 Crash Recovery

On restart:
1. Load last durable WAL offset
2. Replay WAL entries
3. Rebuild memtable
4. Resume ingestion safely

No partial writes, no silent corruption.

## 🧠 JVM & Memory Considerations

- GC tuning experiments (G1, ZGC)
- Heap sizing vs mmap reliance
- False sharing avoidance
- Explicit memory ownership

This project treats the JVM as a runtime to be reasoned about, not abstracted away.

## 🛠 Tech Stack

- Java 21
- NIO / FileChannel / mmap
- JMH (benchmarks)
- Minimal dependencies by design

## 📚 What This Project Demonstrates

- Storage engine fundamentals
- JVM internals awareness
- Write-path determinism
- Real-world trade-off thinking
- Systems-level debugging mindset

## 🚧 Current Status

- ✅ WAL
- ✅ Memtable
- ✅ SSTable
- 🔄 Compaction tuning
- 🔄 Off-heap memtable (experimental)
- 🔄 Extended benchmarks

## 🧠 Why NanoTSDB Exists

**This is not a production database.**
It is a thinking exercise in building predictable systems on the JVM.

## 📄 License

MIT