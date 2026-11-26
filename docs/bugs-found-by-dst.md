# Bugs Caught During Testing

Notes on real bugs that showed up while writing unit tests and running the simulation harness.

## 1. Bloom Filter Hash Correlation (`4.30%` False Positive Rate)
- **File:** `pkg/storage/bloom.go` (`doubleHash`)
- **What broke:** `TestBloomFilterAccuracy` failed with `86/2000` (`4.30%`) false positives against a `<3%` target.
- **Why:** `doubleHash` used `fnv.New64a()` and `fnv.New64()`. On sequential ASCII keys (`absent-key-0000`, `0001`, ...), the two FNV variants produced correlated bit strides and collided repeatedly.
- **Fix:** Switched `doubleHash` to derive two independent 64-bit hashes from `sha256.Sum256(key)` with an odd stride (`| 1`).

## 2. Reordered `AppendEntriesReply` Updating `matchIndex`
- **File:** `pkg/raft/raft.go` (`handleAppendEntriesReply`)
- **What broke:** When `pkg/sim` reordered RPCs, an older `AppendEntriesReply` could arrive out of order after newer entries were already replicated.
- **Fix:** Added explicit `MatchIndex = PrevLogIndex + len(Entries)` to `AppendEntriesReply` and only updated `n.matchIndex[from]` when `reply.MatchIndex > n.matchIndex[from]`.