# Architecture: TardiDB

## System Layers
1. **Storage Engine (`pkg/storage`)**: Append-only binary WAL (`CRC32 | TotalLen | Op | Key | Val`) with torn-tail truncation and mid-file bit-flip detection, sorted SkipList MemTable with tombstones, immutable SSTables with SHA-256 double-hashed Bloom filters (~0.8% FP), and full SSTable compaction.
2. **Pure Deterministic Raft Core (`pkg/raft`)**: Zero goroutines, zero OS timers, zero sockets inside consensus logic. Driven exclusively by `Tick()`, `Propose()`, `RequestReadIndex()`, `Snapshot()`, and `HandleMessage()`, returning explicit `[]Action` side effects (`ActionSendMessage`, `ActionPersist`, `ActionApply`, `ActionApplySnapshot`, `ActionReadReady`).
3. **KV State Machine (`pkg/kv`)**: Deterministic `Put`/`Delete`/`Get` state machine with `ClientID + SeqNum` deduplication persisted inside Raft snapshots.
4. **Deterministic Simulation Engine (`pkg/sim`)**: Single-threaded min-heap discrete-event loop driven by a 64-bit PRNG seed. Injects packet loss, duplicates, reordering, asymmetric partitions, node crashes/reboots, and aggressive snapshots while asserting Raft's Election Safety, Log Matching, and State Machine Safety after every event, followed by a P-compositionality linearizability proof.
5. **Multi-Raft Sharding (`pkg/shard`)**: Shard Controller (`Join`/`Leave`/`Move`/`Query`) with minimal-movement rebalancing and online `StatusPushing -> ExportShard -> InstallShard -> DeleteShard` migration preserving client dedup state.
6. **Multi-Key Transactions (`pkg/txn`)**: Percolator-style MVCC 2PC coordinator across shards (`Prewrite` -> atomic `Commit` on Primary -> `Commit` Secondaries) guaranteeing Snapshot Isolation and clean conflict rollback.
7. **Network Runtime & Benchmarks (`pkg/server`, `pkg/obs`, `cmd/kvbench`, `proto/kvraft.proto`)**: Real localhost TCP cluster server + HTTP API, `p50`/`p99`/`p999` latency telemetry, and open-loop leader-kill failover benchmark.

## Measured Results (Windows 11 Local Laptop)
- **DST Swarm Verification**: `2,694` linearizable client operations verified across `30` seeded 5-node chaos runs in `0.69s` (zero invariant violations).
- **Leader-Kill Failover Benchmark (`cmd/kvbench`)**: `1,000` Puts (`0` errors) with mid-run `SIGKILL` on Leader Node 4 -> automatic failover to Node 1 (Term 2) at `2,879 ops/sec` (`p50=0.00ms`, `p99=8.14ms`, `p999=10.37ms`, `max=10.38ms`).
