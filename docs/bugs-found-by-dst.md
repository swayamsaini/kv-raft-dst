# Bugs Caught by Testing & Deterministic Simulation

Maintaining a log of real bugs caught by tests and seeded simulation runs is one of the highest-signal engineering artifacts for systems interviews.

## Bug #1: Bloom Filter Hash Correlation (`4.30%` False Positive Rate)
- **Where:** `pkg/storage/bloom.go` (`doubleHash`)
- **Symptom:** `TestBloomFilterAccuracy` failed with `86/2000 (4.30%)` false positives against a `<3%` target (~0.8% theoretical).
- **Root Cause:** `doubleHash` paired `fnv.New64a()` and `fnv.New64()`. On sequential ASCII keys (`absent-key-0000`, `0001`, ...), FNV-1 and FNV-1a produce strongly correlated bit strides, causing systematic bit collisions.
- **Fix:** Switched `doubleHash` to derive two independent 64-bit hashes from `sha256.Sum256(key)` with an odd stride (`| 1`) so `(h1 + i*h2) % m` covers the bitset uniformly.

## Bug #2: Reordered `AppendEntriesReply` Corrupting `matchIndex`
- **Where:** `pkg/raft/raft.go` (`handleAppendEntriesReply`)
- **Symptom:** Under packet reordering in `pkg/sim`, an `AppendEntriesReply` for an older RPC could arrive after newer entries were appended to the leader's log, risking premature `matchIndex` advancement.
- **Fix:** Added explicit `MatchIndex = PrevLogIndex + len(Entries)` to `AppendEntriesReply` and enforced monotonic updates (`if reply.MatchIndex > n.matchIndex[from]`).
