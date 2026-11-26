package raft

import (
	"math/rand"
)

const (
	heartbeatIntervalTicks = 3
	minElectionTicks       = 10
	maxElectionTicks       = 20
)

type pendingRead struct {
	readID    uint64
	readIndex uint64
	term      uint64
	acks      map[int]bool
}

// Node is a pure, deterministic Raft consensus state machine (zero OS I/O, zero goroutines).
// Supports: Leader Election, Log Replication, Fast Backtracking, Log Compaction/Snapshots,
// and Linearizable ReadIndex.
type Node struct {
	id    int
	peers []int

	// Persistent state
	currentTerm       uint64
	votedFor          int
	log               []LogEntry // log[0] is dummy/snapshot anchor {Term: lastIncludedTerm, Index: lastIncludedIndex}
	snapshot          []byte
	lastIncludedIndex uint64
	lastIncludedTerm  uint64

	// Volatile state on all servers
	role        Role
	leaderID    int
	commitIndex uint64
	lastApplied uint64

	// Volatile state on leaders
	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	// ReadIndex tracking on leader
	nextReadID   uint64
	pendingReads []*pendingRead
	noopIndex    uint64 // Index of current term's no-op entry (must commit before ReadIndex)

	// Timers
	tickCount       int
	electionTimeout int
	votesReceived   map[int]bool
	rng             *rand.Rand
}

// NewNode creates a Raft node with an explicit deterministic seed for election timeouts.
func NewNode(id int, peers []int, seed int64, initial *PersistentState) *Node {
	n := &Node{
		id:         id,
		peers:      append([]int(nil), peers...),
		role:       Follower,
		leaderID:   -1,
		votedFor:   -1,
		nextIndex:  make(map[int]uint64),
		matchIndex: make(map[int]uint64),
		rng:        rand.New(rand.NewSource(seed)),
	}

	if initial != nil && len(initial.Log) > 0 {
		n.currentTerm = initial.CurrentTerm
		n.votedFor = initial.VotedFor
		n.log = cloneLog(initial.Log)
		n.lastIncludedIndex = initial.LastIncludedIndex
		n.lastIncludedTerm = initial.LastIncludedTerm
		if initial.Snapshot != nil {
			n.snapshot = append([]byte(nil), initial.Snapshot...)
		}
		n.commitIndex = initial.LastIncludedIndex
		n.lastApplied = initial.LastIncludedIndex
	} else {
		n.log = []LogEntry{{Term: 0, Index: 0}}
	}

	n.resetElectionTimeout()
	return n
}

func (n *Node) resetElectionTimeout() {
	n.tickCount = 0
	n.electionTimeout = minElectionTicks + n.rng.Intn(maxElectionTicks-minElectionTicks+1)
}

func (n *Node) ID() int             { return n.id }
func (n *Node) Role() Role          { return n.role }
func (n *Node) Term() uint64        { return n.currentTerm }
func (n *Node) CommitIndex() uint64 { return n.commitIndex }
func (n *Node) LastApplied() uint64 { return n.lastApplied }

func (n *Node) LastLogIndex() uint64 {
	return n.log[len(n.log)-1].Index
}

func (n *Node) LastLogTerm() uint64 {
	return n.log[len(n.log)-1].Term
}

// LogSnapshot returns a copy of the in-memory log slice (index 0 is snapshot anchor).
func (n *Node) LogSnapshot() []LogEntry {
	return cloneLog(n.log)
}

func (n *Node) quorum() int {
	return len(n.peers)/2 + 1
}

// sliceIndex translates a global Raft log index into a slice offset in n.log.
// Returns (offset, true) if idx >= lastIncludedIndex and idx <= LastLogIndex().
func (n *Node) sliceIndex(globalIdx uint64) (int, bool) {
	if globalIdx < n.lastIncludedIndex || globalIdx > n.LastLogIndex() {
		return 0, false
	}
	return int(globalIdx - n.lastIncludedIndex), true
}

func (n *Node) termAt(globalIdx uint64) (uint64, bool) {
	off, ok := n.sliceIndex(globalIdx)
	if !ok {
		return 0, false
	}
	return n.log[off].Term, true
}

// Snapshot compacts all log entries <= index into an immutable snapshot byte slice.
func (n *Node) Snapshot(index uint64, data []byte) []Action {
	if index <= n.lastIncludedIndex || index > n.commitIndex {
		return nil
	}
	off, ok := n.sliceIndex(index)
	if !ok {
		return nil
	}
	snapTerm := n.log[off].Term

	// Keep dummy entry at log[0] representing (index, snapTerm), plus tail entries > index.
	tail := cloneLog(n.log[off+1:])
	newLog := make([]LogEntry, 1, 1+len(tail))
	newLog[0] = LogEntry{Term: snapTerm, Index: index}
	newLog = append(newLog, tail...)

	n.log = newLog
	n.lastIncludedIndex = index
	n.lastIncludedTerm = snapTerm
	n.snapshot = append([]byte(nil), data...)
	if n.lastApplied < index {
		n.lastApplied = index
	}

	return []Action{n.persistAction()}
}

// Tick advances the node's logical clock by one tick.
func (n *Node) Tick() []Action {
	n.tickCount++
	if n.role == Leader {
		if n.tickCount >= heartbeatIntervalTicks {
			n.tickCount = 0
			return n.broadcastAppendOrSnapshot()
		}
		return nil
	}

	if n.tickCount >= n.electionTimeout {
		return n.startElection()
	}
	return nil
}

// Propose appends a client command to the leader's log and broadcasts AppendEntries.
func (n *Node) Propose(cmd []byte) (uint64, uint64, bool, []Action) {
	if n.role != Leader {
		return 0, 0, false, nil
	}
	cp := make([]byte, len(cmd))
	copy(cp, cmd)

	idx := n.LastLogIndex() + 1
	n.log = append(n.log, LogEntry{
		Term:    n.currentTerm,
		Index:   idx,
		Command: cp,
	})
	n.matchIndex[n.id] = idx
	n.nextIndex[n.id] = idx + 1

	actions := []Action{n.persistAction()}
	actions = append(actions, n.broadcastAppendOrSnapshot()...)
	return idx, n.currentTerm, true, actions
}

// RequestReadIndex initiates a linearizable read via Raft's ReadIndex protocol.
// Returns (readID, isLeader, actions). Caller waits for ActionReadReady with matching ReadID.
func (n *Node) RequestReadIndex() (uint64, bool, []Action) {
	if n.role != Leader {
		return 0, false, nil
	}
	// Leader must have committed an entry in its current term before serving ReadIndex.
	if n.commitIndex < n.noopIndex {
		return 0, false, nil
	}

	n.nextReadID++
	rid := n.nextReadID
	pr := &pendingRead{
		readID:    rid,
		readIndex: n.commitIndex,
		term:      n.currentTerm,
		acks:      map[int]bool{n.id: true},
	}

	// Single-node cluster fast-path.
	if len(pr.acks) >= n.quorum() && n.lastApplied >= pr.readIndex {
		return rid, true, []Action{{
			Type:      ActionReadReady,
			ReadReady: &ReadReady{ReadID: rid, ReadIndex: pr.readIndex},
		}}
	}

	n.pendingReads = append(n.pendingReads, pr)
	// Broadcast heartbeat immediately to confirm leadership with a majority.
	return rid, true, n.broadcastAppendOrSnapshot()
}

func (n *Node) startElection() []Action {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = -1
	n.pendingReads = nil
	n.votesReceived = map[int]bool{n.id: true}
	n.resetElectionTimeout()

	actions := []Action{n.persistAction()}

	if len(n.votesReceived) >= n.quorum() {
		return append(actions, n.becomeLeader()...)
	}

	lastIdx := n.LastLogIndex()
	lastTerm := n.LastLogTerm()
	for _, peer := range n.peers {
		if peer == n.id {
			continue
		}
		actions = append(actions, Action{
			Type: ActionSendMessage,
			Message: &Message{
				From: n.id,
				To:   peer,
				ReqVote: &RequestVoteArgs{
					Term:         n.currentTerm,
					CandidateID:  n.id,
					LastLogIndex: lastIdx,
					LastLogTerm:  lastTerm,
				},
			},
		})
	}
	return actions
}

func (n *Node) becomeLeader() []Action {
	n.role = Leader
	n.leaderID = n.id
	n.tickCount = 0
	n.pendingReads = nil

	lastIdx := n.LastLogIndex()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIdx + 1
		n.matchIndex[peer] = 0
	}
	n.matchIndex[n.id] = lastIdx

	// Blank no-op entry in currentTerm (Raft §5.4.2 + ReadIndex prerequisite).
	noopIdx := lastIdx + 1
	n.log = append(n.log, LogEntry{
		Term:    n.currentTerm,
		Index:   noopIdx,
		Command: nil,
	})
	n.noopIndex = noopIdx
	n.matchIndex[n.id] = noopIdx
	n.nextIndex[n.id] = noopIdx + 1

	actions := []Action{n.persistAction()}
	actions = append(actions, n.broadcastAppendOrSnapshot()...)
	return actions
}

func (n *Node) broadcastAppendOrSnapshot() []Action {
	var actions []Action
	for _, peer := range n.peers {
		if peer == n.id {
			continue
		}
		nextIdx := n.nextIndex[peer]
		if nextIdx == 0 {
			nextIdx = 1
		}

		// If follower needs entries that were already compacted, send InstallSnapshot!
		if nextIdx <= n.lastIncludedIndex {
			actions = append(actions, Action{
				Type: ActionSendMessage,
				Message: &Message{
					From: n.id,
					To:   peer,
					ReqSnapshot: &InstallSnapshotArgs{
						Term:              n.currentTerm,
						LeaderID:          n.id,
						LastIncludedIndex: n.lastIncludedIndex,
						LastIncludedTerm:  n.lastIncludedTerm,
						Data:              append([]byte(nil), n.snapshot...),
					},
				},
			})
			continue
		}

		prevIdx := nextIdx - 1
		prevTerm, ok := n.termAt(prevIdx)
		if !ok {
			continue
		}

		var entries []LogEntry
		if nextIdx <= n.LastLogIndex() {
			off, _ := n.sliceIndex(nextIdx)
			entries = cloneLog(n.log[off:])
		}

		actions = append(actions, Action{
			Type: ActionSendMessage,
			Message: &Message{
				From: n.id,
				To:   peer,
				ReqAppend: &AppendEntriesArgs{
					Term:         n.currentTerm,
					LeaderID:     n.id,
					PrevLogIndex: prevIdx,
					PrevLogTerm:  prevTerm,
					Entries:      entries,
					LeaderCommit: n.commitIndex,
				},
			},
		})
	}
	return actions
}

// HandleMessage processes an incoming Raft RPC request or reply.
func (n *Node) HandleMessage(msg Message) []Action {
	switch {
	case msg.ReqVote != nil:
		return n.handleRequestVote(msg.From, msg.ReqVote)
	case msg.RespVote != nil:
		return n.handleRequestVoteReply(msg.From, msg.RespVote)
	case msg.ReqAppend != nil:
		return n.handleAppendEntries(msg.From, msg.ReqAppend)
	case msg.RespAppend != nil:
		return n.handleAppendEntriesReply(msg.From, msg.RespAppend)
	case msg.ReqSnapshot != nil:
		return n.handleInstallSnapshot(msg.From, msg.ReqSnapshot)
	case msg.RespSnapshot != nil:
		return n.handleInstallSnapshotReply(msg.From, msg.RespSnapshot)
	default:
		return nil
	}
}

func (n *Node) stepDownIfHigherTerm(term uint64) bool {
	if term > n.currentTerm {
		n.currentTerm = term
		n.role = Follower
		n.votedFor = -1
		n.leaderID = -1
		n.votesReceived = nil
		n.pendingReads = nil
		return true
	}
	return false
}

func (n *Node) handleRequestVote(from int, args *RequestVoteArgs) []Action {
	var actions []Action
	persisted := n.stepDownIfHigherTerm(args.Term)

	grant := false
	if args.Term >= n.currentTerm {
		canVote := n.votedFor == -1 || n.votedFor == args.CandidateID
		if canVote && n.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
			grant = true
			n.votedFor = args.CandidateID
			n.resetElectionTimeout()
			persisted = true
		}
	}

	if persisted {
		actions = append(actions, n.persistAction())
	}

	actions = append(actions, Action{
		Type: ActionSendMessage,
		Message: &Message{
			From: n.id,
			To:   from,
			RespVote: &RequestVoteReply{
				Term:        n.currentTerm,
				VoteGranted: grant,
			},
		},
	})
	return actions
}

func (n *Node) isUpToDate(candIdx, candTerm uint64) bool {
	myTerm := n.LastLogTerm()
	if candTerm != myTerm {
		return candTerm > myTerm
	}
	return candIdx >= n.LastLogIndex()
}

func (n *Node) handleRequestVoteReply(from int, reply *RequestVoteReply) []Action {
	if n.stepDownIfHigherTerm(reply.Term) {
		return []Action{n.persistAction()}
	}
	if n.role != Candidate || reply.Term != n.currentTerm {
		return nil
	}
	if reply.VoteGranted {
		n.votesReceived[from] = true
		if len(n.votesReceived) >= n.quorum() {
			return n.becomeLeader()
		}
	}
	return nil
}

func (n *Node) handleAppendEntries(from int, args *AppendEntriesArgs) []Action {
	var actions []Action
	persisted := n.stepDownIfHigherTerm(args.Term)

	reply := &AppendEntriesReply{
		Term:    n.currentTerm,
		Success: false,
	}

	if args.Term < n.currentTerm {
		if persisted {
			actions = append(actions, n.persistAction())
		}
		actions = append(actions, Action{
			Type: ActionSendMessage,
			Message: &Message{From: n.id, To: from, RespAppend: reply},
		})
		return actions
	}

	n.role = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimeout()

	// Stale RPC whose PrevLogIndex is already inside our compacted snapshot.
	if args.PrevLogIndex < n.lastIncludedIndex {
		reply.ConflictLen = n.LastLogIndex() + 1
		reply.ConflictIndex = n.lastIncludedIndex + 1
		if persisted {
			actions = append(actions, n.persistAction())
		}
		actions = append(actions, Action{
			Type: ActionSendMessage,
			Message: &Message{From: n.id, To: from, RespAppend: reply},
		})
		return actions
	}

	// Follower log too short.
	if args.PrevLogIndex > n.LastLogIndex() {
		reply.ConflictLen = n.LastLogIndex() + 1
		reply.ConflictIndex = n.LastLogIndex() + 1
		if persisted {
			actions = append(actions, n.persistAction())
		}
		actions = append(actions, Action{
			Type: ActionSendMessage,
			Message: &Message{From: n.id, To: from, RespAppend: reply},
		})
		return actions
	}

	prevTerm, _ := n.termAt(args.PrevLogIndex)
	if prevTerm != args.PrevLogTerm {
		xTerm := prevTerm
		xIdx := args.PrevLogIndex
		for xIdx > n.lastIncludedIndex+1 {
			t, ok := n.termAt(xIdx - 1)
			if !ok || t != xTerm {
				break
			}
			xIdx--
		}
		reply.ConflictTerm = xTerm
		reply.ConflictIndex = xIdx
		reply.ConflictLen = n.LastLogIndex() + 1
		if persisted {
			actions = append(actions, n.persistAction())
		}
		actions = append(actions, Action{
			Type: ActionSendMessage,
			Message: &Message{From: n.id, To: from, RespAppend: reply},
		})
		return actions
	}

	// Append new entries, truncating only on term conflict.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= n.LastLogIndex() {
			existingTerm, _ := n.termAt(idx)
			if existingTerm != entry.Term {
				off, _ := n.sliceIndex(idx)
				n.log = n.log[:off]
				n.log = append(n.log, cloneLog(args.Entries[i:])...)
				persisted = true
				break
			}
		} else {
			n.log = append(n.log, cloneLog(args.Entries[i:])...)
			persisted = true
			break
		}
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew := n.LastLogIndex()
		if args.LeaderCommit < lastNew {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
	}

	reply.Success = true
	reply.MatchIndex = args.PrevLogIndex + uint64(len(args.Entries))
	if persisted {
		actions = append(actions, n.persistAction())
	}
	actions = append(actions, n.applyCommitted()...)
	actions = append(actions, Action{
		Type: ActionSendMessage,
		Message: &Message{From: n.id, To: from, RespAppend: reply},
	})
	return actions
}

func (n *Node) handleAppendEntriesReply(from int, reply *AppendEntriesReply) []Action {
	if n.stepDownIfHigherTerm(reply.Term) {
		return []Action{n.persistAction()}
	}
	if n.role != Leader || reply.Term != n.currentTerm {
		return nil
	}

	if reply.Success {
		// Monotonic update protects against reordered RPC replies in DST!
		if reply.MatchIndex > n.matchIndex[from] {
			n.matchIndex[from] = reply.MatchIndex
			n.nextIndex[from] = reply.MatchIndex + 1
		}
		actions := n.advanceLeaderCommit()
		actions = append(actions, n.ackPendingReads(from)...)
		return actions
	}

	// Fast backtracking using ConflictTerm / ConflictIndex / ConflictLen.
	if reply.ConflictTerm == 0 {
		n.nextIndex[from] = reply.ConflictLen
	} else {
		lastIdxOfXTerm := uint64(0)
		for i := len(n.log) - 1; i >= 1; i-- {
			if n.log[i].Term == reply.ConflictTerm {
				lastIdxOfXTerm = n.log[i].Index
				break
			}
		}
		if lastIdxOfXTerm > 0 {
			n.nextIndex[from] = lastIdxOfXTerm + 1
		} else {
			n.nextIndex[from] = reply.ConflictIndex
		}
	}
	if n.nextIndex[from] < 1 {
		n.nextIndex[from] = 1
	}
	return nil
}

func (n *Node) handleInstallSnapshot(from int, args *InstallSnapshotArgs) []Action {
	var actions []Action
	persisted := n.stepDownIfHigherTerm(args.Term)

	if args.Term < n.currentTerm {
		if persisted {
			actions = append(actions, n.persistAction())
		}
		actions = append(actions, Action{
			Type: ActionSendMessage,
			Message: &Message{
				From:         n.id,
				To:           from,
				RespSnapshot: &InstallSnapshotReply{Term: n.currentTerm},
			},
		})
		return actions
	}

	n.role = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimeout()

	if args.LastIncludedIndex > n.lastIncludedIndex {
		// Retain any suffix of log matching LastIncludedIndex/Term; otherwise reset log.
		var tail []LogEntry
		if t, ok := n.termAt(args.LastIncludedIndex); ok && t == args.LastIncludedTerm {
			off, _ := n.sliceIndex(args.LastIncludedIndex)
			tail = cloneLog(n.log[off+1:])
		}
		newLog := make([]LogEntry, 1, 1+len(tail))
		newLog[0] = LogEntry{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}
		newLog = append(newLog, tail...)

		n.log = newLog
		n.lastIncludedIndex = args.LastIncludedIndex
		n.lastIncludedTerm = args.LastIncludedTerm
		n.snapshot = append([]byte(nil), args.Data...)
		if n.commitIndex < args.LastIncludedIndex {
			n.commitIndex = args.LastIncludedIndex
		}
		if n.lastApplied < args.LastIncludedIndex {
			n.lastApplied = args.LastIncludedIndex
		}
		persisted = true

		actions = append(actions, Action{
			Type:      ActionApplySnapshot,
			Snapshot:  append([]byte(nil), n.snapshot...),
			SnapIndex: n.lastIncludedIndex,
			SnapTerm:  n.lastIncludedTerm,
		})
	}

	if persisted {
		actions = append(actions, n.persistAction())
	}
	actions = append(actions, Action{
		Type: ActionSendMessage,
		Message: &Message{
			From:         n.id,
			To:           from,
			RespSnapshot: &InstallSnapshotReply{Term: n.currentTerm},
		},
	})
	return actions
}

func (n *Node) handleInstallSnapshotReply(from int, reply *InstallSnapshotReply) []Action {
	if n.stepDownIfHigherTerm(reply.Term) {
		return []Action{n.persistAction()}
	}
	if n.role != Leader || reply.Term != n.currentTerm {
		return nil
	}
	if n.lastIncludedIndex > n.matchIndex[from] {
		n.matchIndex[from] = n.lastIncludedIndex
		n.nextIndex[from] = n.lastIncludedIndex + 1
	}
	return nil
}

func (n *Node) advanceLeaderCommit() []Action {
	// Raft §5.4.2 (Figure 8 rule): ONLY advance commitIndex for entries from currentTerm.
	for idx := n.LastLogIndex(); idx > n.commitIndex; idx-- {
		t, ok := n.termAt(idx)
		if !ok || t != n.currentTerm {
			continue
		}
		count := 0
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			break
		}
	}
	return n.applyCommitted()
}

func (n *Node) ackPendingReads(from int) []Action {
	if len(n.pendingReads) == 0 {
		return nil
	}
	var actions []Action
	remaining := n.pendingReads[:0]
	for _, pr := range n.pendingReads {
		if pr.term != n.currentTerm {
			continue
		}
		pr.acks[from] = true
		if len(pr.acks) >= n.quorum() && n.lastApplied >= pr.readIndex {
			actions = append(actions, Action{
				Type:      ActionReadReady,
				ReadReady: &ReadReady{ReadID: pr.readID, ReadIndex: pr.readIndex},
			})
		} else {
			remaining = append(remaining, pr)
		}
	}
	n.pendingReads = remaining
	return actions
}

func (n *Node) applyCommitted() []Action {
	var actions []Action
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		off, ok := n.sliceIndex(n.lastApplied)
		if !ok {
			continue
		}
		entry := n.log[off]
		if entry.Command != nil {
			cp := make([]byte, len(entry.Command))
			copy(cp, entry.Command)
			actions = append(actions, Action{
				Type: ActionApply,
				Entry: &LogEntry{
					Term:    entry.Term,
					Index:   entry.Index,
					Command: cp,
				},
			})
		}
	}
	// Check if any quorum-confirmed ReadIndex was waiting on lastApplied to catch up.
	if n.role == Leader && len(n.pendingReads) > 0 {
		remaining := n.pendingReads[:0]
		for _, pr := range n.pendingReads {
			if pr.term != n.currentTerm {
				continue
			}
			if len(pr.acks) >= n.quorum() && n.lastApplied >= pr.readIndex {
				actions = append(actions, Action{
					Type:      ActionReadReady,
					ReadReady: &ReadReady{ReadID: pr.readID, ReadIndex: pr.readIndex},
				})
			} else {
				remaining = append(remaining, pr)
			}
		}
		n.pendingReads = remaining
	}
	return actions
}

func (n *Node) persistAction() Action {
	var snapCopy []byte
	if n.snapshot != nil {
		snapCopy = append([]byte(nil), n.snapshot...)
	}
	return Action{
		Type: ActionPersist,
		State: &PersistentState{
			CurrentTerm:       n.currentTerm,
			VotedFor:          n.votedFor,
			Log:               cloneLog(n.log),
			Snapshot:          snapCopy,
			LastIncludedIndex: n.lastIncludedIndex,
			LastIncludedTerm:  n.lastIncludedTerm,
		},
	}
}

func cloneLog(in []LogEntry) []LogEntry {
	out := make([]LogEntry, len(in))
	for i, e := range in {
		var cmd []byte
		if e.Command != nil {
			cmd = make([]byte, len(e.Command))
			copy(cmd, e.Command)
		}
		out[i] = LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: cmd,
		}
	}
	return out
}
