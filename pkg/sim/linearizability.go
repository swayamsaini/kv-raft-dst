package sim

import (
	"fmt"
	"sort"
)

// OpType represents a client KV operation in a recorded history.
type OpType uint8

const (
	OpPut OpType = iota + 1
	OpGet
	OpDelete
)

// HistoryOp is a single client operation recorded by the simulator for linearizability checking.
// An operation with ReturnTime == 0 is indeterminate (e.g., timed out / leader crashed mid-RPC).
type HistoryOp struct {
	ClientID   uint64
	SeqNum     uint64
	Op         OpType
	Key        string
	Value      string // Input value for Put, or observed value for Get
	Found      bool   // Observed Found flag for Get
	InvokeTime int64  // Virtual ms when client sent request
	ReturnTime int64  // Virtual ms when client received reply (0 = indeterminate/timed out)
}

type keyEvent struct {
	opIdx int
	time  int64
	isRet bool
}

// CheckLinearizable verifies whether a concurrent client history is linearizable
// with respect to a sequential Key-Value map specification.
//
// Implements Porcupine's P-compositionality optimization: because linearizability is
// a local property, a multi-key KV history is linearizable iff every individual key's
// sub-history is independently linearizable.
func CheckLinearizable(history []HistoryOp) (bool, string) {
	byKey := make(map[string][]HistoryOp)
	for _, op := range history {
		byKey[op.Key] = append(byKey[op.Key], op)
	}

	// Deterministic key iteration order.
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if ok, reason := checkSingleKeyLinearizable(byKey[k]); !ok {
			return false, fmt.Sprintf("key %q violated linearizability: %s", k, reason)
		}
	}
	return true, ""
}

type kvState struct {
	val   string
	found bool
}

func checkSingleKeyLinearizable(ops []HistoryOp) (bool, string) {
	if len(ops) == 0 {
		return true, ""
	}

	// Build sorted timeline of invocation and return events.
	// If an op never returned (ReturnTime == 0), treat its return bound as +infinity
	// and allow it to either linearize or be omitted (incomplete operation semantics).
	const infTime = int64(1<<62 - 1)
	events := make([]keyEvent, 0, len(ops)*2)
	for i, op := range ops {
		events = append(events, keyEvent{opIdx: i, time: op.InvokeTime, isRet: false})
		ret := op.ReturnTime
		if ret == 0 {
			ret = infTime
		}
		events = append(events, keyEvent{opIdx: i, time: ret, isRet: true})
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].time != events[j].time {
			return events[i].time < events[j].time
		}
		// Process invocations before returns at identical timestamps.
		if events[i].isRet != events[j].isRet {
			return !events[i].isRet
		}
		return events[i].opIdx < events[j].opIdx
	})

	// Memoize failed (visitedMask, state) pairs to avoid exponential re-exploration.
	type memoKey struct {
		mask  uint64
		val   string
		found bool
	}

	// For histories > 62 ops on a single key, window or use string key; here each DST run
	// generates ~15-40 ops per key, fitting cleanly in a uint64 bitmask.
	if len(ops) > 62 {
		return checkSingleKeyWithStringMemo(ops, events)
	}

	failed := make(map[memoKey]bool)

	var dfs func(completedMask uint64, state kvState) bool
	dfs = func(completedMask uint64, state kvState) bool {
		// All completed (returned) ops must be linearized!
		allCompletedDone := true
		for i, op := range ops {
			if op.ReturnTime != 0 && (completedMask&(1<<uint(i))) == 0 {
				allCompletedDone = false
				break
			}
		}
		if allCompletedDone {
			return true
		}

		mk := memoKey{mask: completedMask, val: state.val, found: state.found}
		if failed[mk] {
			return false
		}

		// Find the earliest return time among all unlinearized completed operations:
		// any operation we linearize NEXT must have InvokeTime <= minUnlinearizedReturnTime,
		// otherwise it would violate real-time ordering!
		minRet := infTime
		for i, op := range ops {
			if (completedMask&(1<<uint(i))) == 0 && op.ReturnTime != 0 && op.ReturnTime < minRet {
				minRet = op.ReturnTime
			}
		}

		for i, op := range ops {
			if (completedMask & (1 << uint(i))) != 0 {
				continue
			}
			// Real-time constraint: cannot linearize an op that started after another op already returned.
			if op.InvokeTime > minRet {
				continue
			}

			nextState, match := stepKV(state, op)
			if !match {
				continue
			}
			if dfs(completedMask|(1<<uint(i)), nextState) {
				return true
			}
		}

		failed[mk] = true
		return false
	}

	if !dfs(0, kvState{val: "", found: false}) {
		return false, fmt.Sprintf("no valid linearization order among %d ops", len(ops))
	}
	return true, ""
}

func checkSingleKeyWithStringMemo(ops []HistoryOp, _ []keyEvent) (bool, string) {
	const infTime = int64(1<<62 - 1)
	done := make([]bool, len(ops))
	failed := make(map[string]bool)

	var dfs func(state kvState) bool
	dfs = func(state kvState) bool {
		allCompleted := true
		minRet := infTime
		for i, op := range ops {
			if !done[i] && op.ReturnTime != 0 {
				allCompleted = false
				if op.ReturnTime < minRet {
					minRet = op.ReturnTime
				}
			}
		}
		if allCompleted {
			return true
		}

		keyBytes := make([]byte, len(ops))
		for i, d := range done {
			if d {
				keyBytes[i] = '1'
			} else {
				keyBytes[i] = '0'
			}
		}
		mk := fmt.Sprintf("%s|%v|%s", string(keyBytes), state.found, state.val)
		if failed[mk] {
			return false
		}

		for i, op := range ops {
			if done[i] || op.InvokeTime > minRet {
				continue
			}
			nextState, match := stepKV(state, op)
			if !match {
				continue
			}
			done[i] = true
			if dfs(nextState) {
				return true
			}
			done[i] = false
		}

		failed[mk] = true
		return false
	}

	if !dfs(kvState{}) {
		return false, fmt.Sprintf("no valid linearization order among %d ops", len(ops))
	}
	return true, ""
}

func stepKV(state kvState, op HistoryOp) (kvState, bool) {
	switch op.Op {
	case OpPut:
		return kvState{val: op.Value, found: true}, true
	case OpDelete:
		return kvState{val: "", found: false}, true
	case OpGet:
		if op.ReturnTime == 0 {
			// Timed-out Get can linearize anywhere without affecting state.
			return state, true
		}
		if state.found != op.Found {
			return state, false
		}
		if state.found && state.val != op.Value {
			return state, false
		}
		return state, true
	default:
		return state, false
	}
}
