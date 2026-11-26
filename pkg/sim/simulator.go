package sim

import (
	"container/heap"
	"fmt"
	"math/rand"

	"github.com/swayamsaini/tardidb/pkg/kv"
	"github.com/swayamsaini/tardidb/pkg/raft"
)

// FaultConfig controls the chaos profile for a single deterministic simulation run.
type FaultConfig struct {
	DropProb          float64 // Probability an RPC is dropped in transit
	DuplicateProb     float64 // Probability an RPC is delivered twice
	MinDelayMs        int64   // Minimum network latency in virtual ms
	MaxDelayMs        int64   // Maximum network latency (causes packet reordering)
	PartitionProb     float64 // Probability of toggling a random network partition per nemesis tick
	CrashProb         float64 // Probability of crashing/rebooting a random node per nemesis tick
	SnapshotThreshold int     // Trigger Raft log compaction every N applied entries
}

type eventKind int

const (
	evTick eventKind = iota
	evMessage
	evNemesis
	evClientStep
)

type simEvent struct {
	vTime int64
	seq   uint64
	kind  eventKind
	node  int
	msg   raft.Message
}

type eventHeap []*simEvent

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].vTime != h[j].vTime {
		return h[i].vTime < h[j].vTime
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x interface{}) { *h = append(*h, x.(*simEvent)) }
func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type pendingClientWrite struct {
	op       HistoryOp
	logIndex uint64
	term     uint64
}

type pendingClientRead struct {
	op HistoryOp
}

// Simulator runs an entire multi-node Raft + KV cluster in a single-threaded
// discrete-event loop driven by one 64-bit seed.
type Simulator struct {
	seed   int64
	rng    *rand.Rand
	faults FaultConfig

	vTime   int64
	nextSeq uint64
	pq      eventHeap

	peers       []int
	nodes       map[int]*raft.Node
	kvMachines  map[int]*kv.StateMachine
	persisted   map[int]*raft.PersistentState
	alive       map[int]bool
	partitioned map[[2]int]bool

	// Invariant Oracle tracking
	appliedCmds  map[uint64]string // global index -> command string (State Machine Safety)
	committedIdx uint64            // max commitIndex seen across cluster

	// Client workload & linearizability history
	nextClientSeq uint64
	pendingWrites map[int]map[uint64]*pendingClientWrite // nodeID -> logIndex -> op
	pendingReads  map[int]map[uint64]*pendingClientRead  // nodeID -> readID -> op
	history       []HistoryOp
}

// NewSimulator initializes a deterministic virtual cluster of numNodes from seed.
func NewSimulator(seed int64, numNodes int, faults FaultConfig) *Simulator {
	rng := rand.New(rand.NewSource(seed))
	peers := make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		peers[i] = i
	}

	s := &Simulator{
		seed:          seed,
		rng:           rng,
		faults:        faults,
		peers:         peers,
		nodes:         make(map[int]*raft.Node),
		kvMachines:    make(map[int]*kv.StateMachine),
		persisted:     make(map[int]*raft.PersistentState),
		alive:         make(map[int]bool),
		partitioned:   make(map[[2]int]bool),
		appliedCmds:   make(map[uint64]string),
		pendingWrites: make(map[int]map[uint64]*pendingClientWrite),
		pendingReads:  make(map[int]map[uint64]*pendingClientRead),
	}

	for _, id := range peers {
		s.nodes[id] = raft.NewNode(id, peers, rng.Int63(), nil)
		s.kvMachines[id] = kv.NewStateMachine()
		s.alive[id] = true
		s.pendingWrites[id] = make(map[uint64]*pendingClientWrite)
		s.pendingReads[id] = make(map[uint64]*pendingClientRead)
		s.schedule(10+int64(id), evTick, id, raft.Message{})
	}

	s.schedule(50, evNemesis, -1, raft.Message{})
	s.schedule(120, evClientStep, -1, raft.Message{})
	return s
}

func (s *Simulator) schedule(delay int64, kind eventKind, node int, msg raft.Message) {
	s.nextSeq++
	heap.Push(&s.pq, &simEvent{
		vTime: s.vTime + delay,
		seq:   s.nextSeq,
		kind:  kind,
		node:  node,
		msg:   msg,
	})
}

// Run executes the simulation for durationMs virtual milliseconds and verifies
// all Raft invariants + Porcupine-style linearizability.
func (s *Simulator) Run(durationMs int64) error {
	for s.pq.Len() > 0 {
		ev := heap.Pop(&s.pq).(*simEvent)
		if ev.vTime > durationMs {
			break
		}
		s.vTime = ev.vTime

		switch ev.kind {
		case evTick:
			if s.alive[ev.node] {
				acts := s.nodes[ev.node].Tick()
				if err := s.processActions(ev.node, acts); err != nil {
					return err
				}
			}
			// Always keep node tick loop alive (if crashed, reboot eventually restores it).
			s.schedule(10, evTick, ev.node, raft.Message{})

		case evMessage:
			if !s.alive[ev.msg.To] {
				continue
			}
			if s.partitioned[[2]int{ev.msg.From, ev.msg.To}] {
				continue
			}
			acts := s.nodes[ev.msg.To].HandleMessage(ev.msg)
			if err := s.processActions(ev.msg.To, acts); err != nil {
				return err
			}

		case evNemesis:
			s.runNemesis()
			s.schedule(80, evNemesis, -1, raft.Message{})

		case evClientStep:
			s.issueClientOp()
			s.schedule(35, evClientStep, -1, raft.Message{})
		}

		if err := s.checkRaftInvariants(); err != nil {
			return fmt.Errorf("[seed=%d, vTime=%dms] %w", s.seed, s.vTime, err)
		}
	}

	// Flush any remaining indeterminate ops into history before linearizability check.
	for _, writes := range s.pendingWrites {
		for _, pw := range writes {
			s.history = append(s.history, pw.op)
		}
	}
	for _, reads := range s.pendingReads {
		for _, pr := range reads {
			s.history = append(s.history, pr.op)
		}
	}

	if ok, reason := CheckLinearizable(s.history); !ok {
		return fmt.Errorf("[seed=%d] LINEARIZABILITY VIOLATION (%d ops): %s", s.seed, len(s.history), reason)
	}
	return nil
}

func (s *Simulator) processActions(nodeID int, actions []raft.Action) error {
	for _, act := range actions {
		switch act.Type {
		case raft.ActionPersist:
			s.persisted[nodeID] = act.State

		case raft.ActionSendMessage:
			if s.rng.Float64() < s.faults.DropProb {
				continue
			}
			delay := s.faults.MinDelayMs
			if s.faults.MaxDelayMs > s.faults.MinDelayMs {
				delay += s.rng.Int63n(s.faults.MaxDelayMs - s.faults.MinDelayMs + 1)
			}
			s.schedule(delay, evMessage, act.Message.To, *act.Message)

			// Optional duplicate packet delivery to stress idempotency.
			if s.rng.Float64() < s.faults.DuplicateProb {
				dupDelay := delay + 1 + s.rng.Int63n(15)
				s.schedule(dupDelay, evMessage, act.Message.To, *act.Message)
			}

		case raft.ActionApply:
			// Invariant 5: State Machine Safety (no two nodes apply different commands at same index).
			cmdStr := string(act.Entry.Command)
			if prev, exists := s.appliedCmds[act.Entry.Index]; exists {
				if prev != cmdStr {
					return fmt.Errorf("StateMachineSafety violated at index %d", act.Entry.Index)
				}
			} else {
				s.appliedCmds[act.Entry.Index] = cmdStr
			}

			rep, err := s.kvMachines[nodeID].Apply(act.Entry)
			if err != nil {
				return err
			}

			// Complete pending client write if this node proposed it.
			if pw, ok := s.pendingWrites[nodeID][act.Entry.Index]; ok {
				if act.Entry.Term == pw.term {
					pw.op.ReturnTime = s.vTime
					pw.op.Found = rep.Found
					s.history = append(s.history, pw.op)
				} else {
					// Entry overwritten by another leader: keep ReturnTime=0 (indeterminate).
					s.history = append(s.history, pw.op)
				}
				delete(s.pendingWrites[nodeID], act.Entry.Index)
			}

			// Periodic log compaction to exercise InstallSnapshot under chaos.
			if s.faults.SnapshotThreshold > 0 && act.Entry.Index%uint64(s.faults.SnapshotThreshold) == 0 {
				snapBytes, err := s.kvMachines[nodeID].Snapshot()
				if err == nil {
					snapActs := s.nodes[nodeID].Snapshot(act.Entry.Index, snapBytes)
					if err := s.processActions(nodeID, snapActs); err != nil {
						return err
					}
				}
			}

		case raft.ActionApplySnapshot:
			if err := s.kvMachines[nodeID].RestoreSnapshot(act.Snapshot); err != nil {
				return err
			}

		case raft.ActionReadReady:
			if pr, ok := s.pendingReads[nodeID][act.ReadReady.ReadID]; ok {
				val, found := s.kvMachines[nodeID].Get(pr.op.Key)
				pr.op.Value = val
				pr.op.Found = found
				pr.op.ReturnTime = s.vTime
				s.history = append(s.history, pr.op)
				delete(s.pendingReads[nodeID], act.ReadReady.ReadID)
			}
		}
	}
	return nil
}

func (s *Simulator) runNemesis() {
	// 1. Heal or inject random asymmetric/symmetric partitions.
	if s.rng.Float64() < s.faults.PartitionProb {
		if len(s.partitioned) > 0 && s.rng.Float64() < 0.6 {
			// Heal all partitions so liveness can recover.
			s.partitioned = make(map[[2]int]bool)
		} else {
			isolated := s.peers[s.rng.Intn(len(s.peers))]
			for _, other := range s.peers {
				if other != isolated {
					s.partitioned[[2]int{isolated, other}] = true
					if s.rng.Float64() < 0.8 {
						s.partitioned[[2]int{other, isolated}] = true
					}
				}
			}
		}
	}

	// 2. Crash or reboot a random node (preserving quorum most of the time).
	if s.rng.Float64() < s.faults.CrashProb {
		target := s.peers[s.rng.Intn(len(s.peers))]
		if s.alive[target] {
			// Count alive nodes; still allow minority crashes.
			aliveCount := 0
			for _, id := range s.peers {
				if s.alive[id] {
					aliveCount++
				}
			}
			if aliveCount > len(s.peers)/2+1 || s.rng.Float64() < 0.25 {
				s.alive[target] = false
				// Fail pending client ops on crashed node as indeterminate.
				for idx, pw := range s.pendingWrites[target] {
					s.history = append(s.history, pw.op)
					delete(s.pendingWrites[target], idx)
				}
				for rid, pr := range s.pendingReads[target] {
					s.history = append(s.history, pr.op)
					delete(s.pendingReads[target], rid)
				}
			}
		} else {
			// Reboot node purely from its durable PersistentState!
			saved := s.persisted[target]
			s.nodes[target] = raft.NewNode(target, s.peers, s.rng.Int63(), saved)
			s.kvMachines[target] = kv.NewStateMachine()
			if saved != nil && saved.Snapshot != nil {
				_ = s.kvMachines[target].RestoreSnapshot(saved.Snapshot)
			}
			s.alive[target] = true
		}
	}
}

func (s *Simulator) issueClientOp() {
	// Pick an alive leader if one exists.
	leaderID := -1
	for _, id := range s.peers {
		if s.alive[id] && s.nodes[id].Role() == raft.Leader {
			leaderID = id
			break
		}
	}
	if leaderID == -1 {
		return
	}

	keys := []string{"alpha", "beta", "gamma"}
	key := keys[s.rng.Intn(len(keys))]
	s.nextClientSeq++
	seq := s.nextClientSeq

	roll := s.rng.Float64()
	if roll < 0.45 {
		// Put
		val := fmt.Sprintf("v-%d", seq)
		raw, _ := kv.EncodeCommand(kv.Command{
			Kind:     kv.OpPut,
			Key:      key,
			Value:    val,
			ClientID: 1,
			SeqNum:   seq,
		})
		idx, term, ok, acts := s.nodes[leaderID].Propose(raw)
		if !ok {
			return
		}
		s.pendingWrites[leaderID][idx] = &pendingClientWrite{
			op: HistoryOp{
				ClientID:   1,
				SeqNum:     seq,
				Op:         OpPut,
				Key:        key,
				Value:      val,
				InvokeTime: s.vTime,
			},
			logIndex: idx,
			term:     term,
		}
		_ = s.processActions(leaderID, acts)
	} else if roll < 0.85 {
		// Linearizable ReadIndex Get
		rid, ok, acts := s.nodes[leaderID].RequestReadIndex()
		if !ok {
			return
		}
		s.pendingReads[leaderID][rid] = &pendingClientRead{
			op: HistoryOp{
				ClientID:   1,
				SeqNum:     seq,
				Op:         OpGet,
				Key:        key,
				InvokeTime: s.vTime,
			},
		}
		_ = s.processActions(leaderID, acts)
	} else {
		// Delete
		raw, _ := kv.EncodeCommand(kv.Command{
			Kind:     kv.OpDelete,
			Key:      key,
			ClientID: 1,
			SeqNum:   seq,
		})
		idx, term, ok, acts := s.nodes[leaderID].Propose(raw)
		if !ok {
			return
		}
		s.pendingWrites[leaderID][idx] = &pendingClientWrite{
			op: HistoryOp{
				ClientID:   1,
				SeqNum:     seq,
				Op:         OpDelete,
				Key:        key,
				InvokeTime: s.vTime,
			},
			logIndex: idx,
			term:     term,
		}
		_ = s.processActions(leaderID, acts)
	}
}

// checkRaftInvariants asserts Raft's formal safety invariants across all live nodes.
func (s *Simulator) checkRaftInvariants() error {
	// 1. Election Safety: at most one leader per term.
	leadersByTerm := make(map[uint64]int)
	for _, id := range s.peers {
		if !s.alive[id] {
			continue
		}
		n := s.nodes[id]
		if n.Role() == raft.Leader {
			term := n.Term()
			if prev, exists := leadersByTerm[term]; exists && prev != id {
				return fmt.Errorf("ElectionSafety violated: term %d has leaders %d and %d", term, prev, id)
			}
			leadersByTerm[term] = id
		}
		if n.CommitIndex() > s.committedIdx {
			s.committedIdx = n.CommitIndex()
		}
	}

	// 2. Log Matching Property: if two logs have an entry with the same index and term,
	//    their logs are identical in all preceding entries (within overlapping non-compacted range).
	logs := make(map[int][]raft.LogEntry)
	for _, id := range s.peers {
		if s.alive[id] {
			logs[id] = s.nodes[id].LogSnapshot()
		}
	}

	for i := 0; i < len(s.peers); i++ {
		for j := i + 1; j < len(s.peers); j++ {
			l1, ok1 := logs[s.peers[i]]
			l2, ok2 := logs[s.peers[j]]
			if !ok1 || !ok2 {
				continue
			}
			if err := verifyLogMatchingPair(s.peers[i], l1, s.peers[j], l2); err != nil {
				return err
			}
		}
	}

	return nil
}

func verifyLogMatchingPair(id1 int, l1 []raft.LogEntry, id2 int, l2 []raft.LogEntry) error {
	if len(l1) == 0 || len(l2) == 0 {
		return nil
	}
	base1 := l1[0].Index
	base2 := l2[0].Index
	start := base1
	if base2 > start {
		start = base2
	}
	end1 := l1[len(l1)-1].Index
	end2 := l2[len(l2)-1].Index
	end := end1
	if end2 < end {
		end = end2
	}
	if start > end {
		return nil
	}

	// Find highest overlapping index with matching term, then verify all prior overlapping entries match.
	matched := false
	for idx := end; idx >= start; idx-- {
		e1 := l1[idx-base1]
		e2 := l2[idx-base2]
		if e1.Term == e2.Term {
			matched = true
		} else if matched {
			return fmt.Errorf("LogMatching violated between node %d and %d at index %d (term %d vs %d)",
				id1, id2, idx, e1.Term, e2.Term)
		}
		if idx == 0 {
			break
		}
	}
	return nil
}

// Stats returns (totalHistoryOps, simulatedTimeMs).
func (s *Simulator) Stats() (int, int64) {
	return len(s.history), s.vTime
}
