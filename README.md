# kv-raft-dst

A fault-tolerant, sharded Key-Value database built from scratch in Go with a custom **LSM-Tree storage engine**, **Raft consensus**, **Multi-Raft sharding**, and a **Deterministic Simulation Testing (DST)** harness modeled after FoundationDB and TigerBeetle.

## Architecture

- **`pkg/storage`**: Append-only binary WAL with CRC32 framing and torn-write crash recovery, sorted SkipList MemTable, immutable SSTables with SHA-256 double-hashed Bloom filters, and leveled compaction.
- **`pkg/raft`**: Pure, zero-OS-I/O Raft state machine (`Tick` / `Propose` / `HandleMessage` -> `[]Action`) supporting leader election, log replication, MIT 6.5840 fast backtracking, Figure 8 commit safety, `InstallSnapshot` log compaction, and linearizable `ReadIndex`.
- **`pkg/kv`**: Deterministic KV state machine with `ClientID + SeqNum` idempotency deduplication persisted inside Raft snapshots.
- **`pkg/sim`**: Single-threaded, 64-bit-seeded discrete-event simulator injecting packet drops, duplicates, reordering, asymmetric partitions, node crashes/reboots, and snapshots—paired with continuous Raft invariant assertions and a P-compositionality linearizability checker.
- **`pkg/shard`**: Multi-Raft Shard Controller (`Join` / `Leave` / `Move` / `Query`) with minimal-movement rebalancing and online `Pushing -> Pull -> InstallShard -> DeleteShard` migration.
- **`pkg/obs` & `cmd/kvbench`**: Tail-latency (`p50 / p99 / p999`) telemetry and an open-loop leader-kill failover benchmark.

## Running Tests & Benchmarks

```powershell
# Run the full test suite (Storage + Raft + KV + 30-seed DST Swarm + Multi-Raft Sharding + Metrics)
go test -v ./...

# Run storage WAL fsync vs. non-fsync benchmarks
go test -bench="." ./pkg/storage

# Run the 5-node leader-kill failover benchmark
go run ./cmd/kvbench
```

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — Internal design, binary formats, and trade-offs.
- [`docs/bugs-found-by-dst.md`](docs/bugs-found-by-dst.md) — Postmortems of real bugs caught during testing & simulation.
