# Building a Fault-Tolerant Distributed KV Store with Raft and Deterministic Simulation Testing

*By Swayam Saini*

Most backend portfolio projects wrap an existing database in HTTP endpoints. I wanted to understand what happens **underneath** databases like etcd, TiKV, CockroachDB, FoundationDB, and TigerBeetle—so I built a sharded, strongly consistent Key-Value store from scratch in Go, complete with its own LSM-tree storage engine, Raft consensus core, Multi-Raft sharding, Percolator-style 2PC transactions, and a single-threaded Deterministic Simulation Testing (DST) harness.

## 1. Starting From Disk: WAL, SkipList & LSM-Tree

Before touching a network socket, every database has to survive a power cut on a single machine. I started with an append-only binary **Write-Ahead Log (WAL)** framing every `Put` and `Delete` with an IEEE `CRC32` checksum (`[CRC32 | TotalLen | OpType | KeyLen | Key | ValLen | Value]`). On startup, `WAL.Open()` distinguishes two very different failure modes:

- **Torn tail write** (power cut mid-append): truncate cleanly back to the last verified offset and keep running.
- **Mid-file checksum mismatch** (disk bit-rot): surface `ErrCorruptRecord` immediately instead of silently dropping committed data.

On top of the WAL, writes go into a sorted **SkipList MemTable** that uses **tombstones** for deletes. When the MemTable hits its byte threshold, it flushes atomically (`tmp` + `fsync` + `rename`) into an immutable **SSTable** with an in-memory binary-search index and a **Bloom filter**, then rotates the WAL. Periodic compaction merges SSTables newest-to-oldest and drops tombstones to reclaim disk space.

## 2. Keeping Raft Pure & Deterministic

The single biggest architectural decision in `pkg/raft` was keeping the consensus core **100% pure**: zero goroutines, zero OS clocks, zero TCP sockets. Instead, the state machine exposes `Tick()`, `Propose()`, `RequestReadIndex()`, `Snapshot()`, and `HandleMessage()`, returning explicit `[]Action` side effects (`ActionSendMessage`, `ActionPersist`, `ActionApply`, `ActionApplySnapshot`, `ActionReadReady`).

That single design choice let the **exact same Raft implementation** run in two completely different worlds:
1. Over real OS TCP sockets (`pkg/server`, passing a live 3-node cluster election + replication test in `0.40s`).
2. Inside a single-threaded, 64-bit-seeded **Deterministic Simulator** (`pkg/sim`).

## 3. Deterministic Simulation Testing & Bugs Caught

Inspired by FoundationDB and TigerBeetle's VOPR, `pkg/sim` drives 5 real Raft+KV nodes using a min-heap discrete-event queue controlled by one 64-bit PRNG seed. It injects packet drops, duplicates, reordering, asymmetric partitions, node crashes/reboots, and aggressive snapshots—asserting Raft's **Election Safety**, **Log Matching**, and **State Machine Safety** after every single event, then verifying the entire client history with a Porcupine-style **P-compositionality linearizability checker**.

Across **30 seeded 5-node chaos simulations**, the simulator verified **2,694 linearizable client operations in 0.69 seconds** with zero safety violations. Along the way, testing caught two concrete systems bugs:
- **Bloom Filter Hash Correlation (`4.30%` FP rate):** Pairing FNV-1 and FNV-1a on sequential ASCII keys caused correlated bit patterns; switching `doubleHash` to independent 64-bit SHA-256 slices dropped false positives to the expected ~0.8%.
- **Reordered `AppendEntriesReply` Safety:** Carrying explicit `MatchIndex` on every reply and updating `matchIndex[peer]` monotonically prevented delayed RPC responses from corrupting leader commit tracking.

## 4. Scaling Out & Measured Failover Performance

To move past the single-Raft throughput ceiling, `pkg/shard` adds a **Shard Controller** with minimal-movement rebalancing (`Join`, `Leave`, `Move`, `Query`) and online shard migration (`StatusPushing -> ExportShard -> InstallShard -> DeleteShard`) that transfers both KV entries and client deduplication state. For atomic multi-key writes across shards, `pkg/txn` implements **Percolator-style MVCC 2PC** with Snapshot Isolation.

On a local Windows 11 laptop, `cmd/kvbench` ran **1,000 Puts** against a 5-node cluster with a mid-run `SIGKILL` injected on Leader Node 4 (triggering automatic failover to Node 1 in Term 2):
- **Throughput:** `2,879 ops/sec` (`0` errors)
- **Tail Latency:** `p50 = 0.00ms`, `p99 = 8.14ms`, `p999 = 10.37ms`, `max = 10.38ms`

Explore the full source, TLA+ specification (`tla/RaftKV.tla`), and tests in [the repository](https://github.com/swayamsaini/tardidb).
