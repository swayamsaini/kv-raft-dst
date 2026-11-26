# Building TardiDB: Notes From Implementing Raft, an LSM-Tree, and DST in Go

*Swayam Saini*

I built TardiDB to understand how systems like etcd, TiKV, CockroachDB, and FoundationDB actually work below the API layer. Instead of wrapping an existing database, I wanted to write the storage engine, Raft consensus, sharding, transactions, and fault-injection tests from scratch using only the Go standard library.

## 1. Storage First (`pkg/storage`)

Before adding networking, a single node needs to survive a crash. Every `Put` and `Delete` is appended to a binary WAL framed with an IEEE `CRC32` checksum. On recovery, `WAL.Open()` separates two failure cases: a torn write at the tail of the file (truncate back to the last valid offset) versus a checksum mismatch in the middle of the file (return `ErrCorruptRecord`).

Writes go into an in-memory SkipList MemTable (with tombstones for deletes) and flush atomically (`tmp` + `fsync` + `rename`) into immutable SSTables paired with a Bloom filter. Compaction merges SSTables newest-to-oldest and drops tombstones.

## 2. Keeping Raft Pure (`pkg/raft`)

The main design choice was keeping `pkg/raft` free of goroutines, `time.Sleep`, and `net`. The node exposes `Tick()`, `Propose()`, `RequestReadIndex()`, `Snapshot()`, and `HandleMessage()`, and returns a slice of `Action` values (`ActionSendMessage`, `ActionPersist`, `ActionApply`, `ActionApplySnapshot`, `ActionReadReady`).

Because consensus logic has no OS dependencies, the exact same `raft.Node` runs both over real TCP (`pkg/server`) and inside a single-threaded simulator (`pkg/sim`).

## 3. Simulation Testing (`pkg/sim`)

Modeled after FoundationDB and TigerBeetle's VOPR, `pkg/sim` runs 5 Raft+KV nodes inside a min-heap event loop driven by one 64-bit PRNG seed. Every run injects packet drops, duplicates, reordering, asymmetric partitions, node crashes, and snapshots, while checking Election Safety and State Machine Safety after every event. Client history is then checked with a key-partitioned (P-compositionality) linearizability checker.

Across 30 seeded chaos runs on my laptop, the simulator verified 2,694 operations in 0.69s with zero violations.

## 4. Sharding, 2PC, and Failover Numbers

`pkg/shard` adds a shard controller with minimal-movement rebalancing and 4-state live migration (copying both KV entries and client deduplication state). `pkg/txn` implements Percolator-style MVCC 2PC across shards.

Running `cmd/kvbench` on Windows 11 (Ryzen 5 5600H) for 1,000 Puts with a mid-run `SIGKILL` on Leader Node 4 (failing over to Node 1 in Term 2) measured 2,879 ops/sec with 0 errors (`p50=0.00ms`, `p99=8.14ms`, `p999=10.37ms`).