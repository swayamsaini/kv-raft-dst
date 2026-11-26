# Architecture

TardiDB is split into seven packages so the consensus and storage logic can be tested deterministically without real sockets or OS timers.

## Packages

1. `pkg/storage`: Append-only binary WAL (`CRC32 | TotalLen | Op | Key | Val`) with torn-tail truncation on startup, a SkipList MemTable with tombstones, immutable SSTables with SHA-256 double-hashed Bloom filters, and compaction.
2. `pkg/raft`: Deterministic Raft state machine (`Tick`, `Propose`, `RequestReadIndex`, `Snapshot`, `HandleMessage`) that returns `[]Action` structs instead of doing network or disk I/O directly.
3. `pkg/kv`: Key-value state machine with `ClientID + SeqNum` deduplication persisted across Raft snapshots.
4. `pkg/sim`: Single-threaded min-heap event loop driven by a 64-bit PRNG seed. Injects packet drops, duplicates, reordering, asymmetric partitions, node crashes, and snapshots, then checks client history with a P-compositionality linearizability checker.
5. `pkg/shard`: Shard controller (`Join`, `Leave`, `Move`, `Query`) with minimal-movement rebalancing and 4-state online shard migration (`StatusPushing -> ExportShard -> InstallShard -> DeleteShard`).
6. `pkg/txn`: Percolator-style MVCC 2PC coordinator across shards (`Prewrite` -> commit primary -> commit secondaries) with Snapshot Isolation.
7. `pkg/server`, `pkg/obs`, `cmd/`: Real TCP cluster wrapper (`net/rpc` + HTTP), latency percentile tracking, `kvbench`, and `kvdemo`.