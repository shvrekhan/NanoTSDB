# Contributing to NanoTSDB

Thanks for your interest! NanoTSDB is a JVM-based time series storage engine focused on predictable write performance and bounded tail latency.

## Project Structure

```
src/main/java/com/nano/tsdb/
├── compaction/   — Size-tiered compaction (Merger, Strategy)
├── core/         — Engine, config, write path, recovery
├── index/        — Bloom filter, sparse index
├── nio/          — Byte serialization, mmap utilities
└── storage/
    ├── memtable/ — OnHeapMemTable, OffHeapMemTable
    ├── sstable/  — SSTable read/write/scan/manager
    └── wal/      — Write-Ahead Log (WALEntry, WALManager)

src/test/java/com/nano/tsdb/
├── benchmark/    — JMH latency benchmarks
└── recovery/     — Crash recovery integration tests
```

## Prerequisites

- **Java 21** (source/target level)
- **Maven 3.8+**

## Setup

```bash
git clone https://github.com/shvrekhan/NanoTSDB.git
cd NanoTSDB
mvn clean compile
```

## Running Tests

```bash
mvn test
```

Tests use JUnit 5. JMH benchmarks are in `src/test/java/com/nano/tsdb/benchmark/`.

## Code Style

- Java 21 features encouraged (records, sealed classes, pattern matching)
- Keep the single-writer principle in mind for write paths
- Minimize allocations on hot paths — prefer off-heap where appropriate
- No external dependencies beyond JMH and JUnit 5

## Pull Request Process

1. Create a feature branch from `main`
2. Write or update tests for your changes
3. Ensure `mvn clean test` passes
4. Keep PRs focused on a single concern
5. Reference any related issues in the PR description

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
