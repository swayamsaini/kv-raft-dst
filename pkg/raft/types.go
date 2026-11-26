package raft

// Role represents a Raft node's current state in the cluster.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a single replicated command (or leader no-op when Command == nil).
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte // nil for leader no-op entry
}

// PersistentState holds the Raft fields that MUST be fsynced before any RPC reply.
type PersistentState struct {
	CurrentTerm       uint64
	VotedFor          int // -1 means nil
	Log               []LogEntry
	Snapshot          []byte
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

// RequestVoteArgs corresponds to Raft Figure 2 RequestVote RPC arguments.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply corresponds to Raft Figure 2 RequestVote RPC reply.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs corresponds to Raft Figure 2 AppendEntries RPC arguments.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply includes MIT 6.5840 fast-backtracking hints and MatchIndex for reorder safety.
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	MatchIndex    uint64 // Highest log index confirmed replicated by this RPC
	ConflictTerm  uint64 // XTerm
	ConflictIndex uint64 // XIndex
	ConflictLen   uint64 // XLen (virtual log length including snapshot offset)
}

// InstallSnapshotArgs corresponds to Raft Figure 13 InstallSnapshot RPC.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          int
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotReply corresponds to Raft Figure 13 InstallSnapshot RPC reply.
type InstallSnapshotReply struct {
	Term uint64
}

// HeartbeatPing is used by ReadIndex to confirm leadership via a majority quorum.
type HeartbeatPing struct {
	ReadID uint64
}

// Message wraps an RPC sent between Raft peers in the pure state-machine interface.
type Message struct {
	From         int
	To           int
	ReqVote      *RequestVoteArgs
	RespVote     *RequestVoteReply
	ReqAppend    *AppendEntriesArgs
	RespAppend   *AppendEntriesReply
	ReqSnapshot  *InstallSnapshotArgs
	RespSnapshot *InstallSnapshotReply
}

// ActionType identifies an external side-effect emitted by the pure Raft state machine.
type ActionType int

const (
	ActionSendMessage ActionType = iota
	ActionPersist
	ActionApply
	ActionApplySnapshot
	ActionReadReady
)

// ReadReady signals that a ReadIndex request has been confirmed by a majority quorum
// and the local state machine has applied at least up to ReadIndex.
type ReadReady struct {
	ReadID    uint64
	ReadIndex uint64
}

// Action is an explicit side-effect returned by Node.Step()/Tick()/Propose().
type Action struct {
	Type        ActionType
	Message     *Message
	State       *PersistentState
	Entry       *LogEntry
	Snapshot    []byte
	SnapIndex   uint64
	SnapTerm    uint64
	ReadReady   *ReadReady
}
