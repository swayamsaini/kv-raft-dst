package raft

import (
	"fmt"
	"testing"
)

// miniCluster is a deterministic in-memory harness to test the Raft core before Phase 4's full DST.
type miniCluster struct {
	nodes       map[int]*Node
	alive       map[int]bool
	partitioned map[[2]int]bool
	persisted   map[int]*PersistentState
	applied     map[int][]LogEntry
	queue       []Message
}

func newMiniCluster(numNodes int) *miniCluster {
	peers := make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		peers[i] = i
	}
	c := &miniCluster{
		nodes:       make(map[int]*Node),
		alive:       make(map[int]bool),
		partitioned: make(map[[2]int]bool),
		persisted:   make(map[int]*PersistentState),
		applied:     make(map[int][]LogEntry),
	}
	for _, id := range peers {
		c.nodes[id] = NewNode(id, peers, int64(100+id*37), nil)
		c.alive[id] = true
	}
	return c
}

func (c *miniCluster) runActions(nodeID int, actions []Action) {
	for _, act := range actions {
		switch act.Type {
		case ActionPersist:
			c.persisted[nodeID] = act.State
		case ActionApply:
			c.applied[nodeID] = append(c.applied[nodeID], *act.Entry)
		case ActionSendMessage:
			c.queue = append(c.queue, *act.Message)
		}
	}
}

func (c *miniCluster) stepTicks(ticks int) {
	for t := 0; t < ticks; t++ {
		for id := 0; id < len(c.nodes); id++ {
			if !c.alive[id] {
				continue
			}
			c.runActions(id, c.nodes[id].Tick())
		}
		c.drainMessages()
	}
}

func (c *miniCluster) drainMessages() {
	for len(c.queue) > 0 {
		msg := c.queue[0]
		c.queue = c.queue[1:]
		if !c.alive[msg.From] || !c.alive[msg.To] {
			continue
		}
		if c.partitioned[[2]int{msg.From, msg.To}] {
			continue
		}
		c.runActions(msg.To, c.nodes[msg.To].HandleMessage(msg))
	}
}

func (c *miniCluster) findLeader() (int, uint64, bool) {
	leaderID := -1
	var leaderTerm uint64
	count := 0
	for id, n := range c.nodes {
		if c.alive[id] && n.Role() == Leader {
			leaderID = id
			leaderTerm = n.Term()
			count++
		}
	}
	if count > 1 {
		panic(fmt.Sprintf("SPLIT BRAIN: %d leaders active simultaneously!", count))
	}
	return leaderID, leaderTerm, count == 1
}

func TestRaftInitialElection(t *testing.T) {
	c := newMiniCluster(3)
	c.stepTicks(40)

	leader, term, ok := c.findLeader()
	if !ok {
		t.Fatalf("expected 1 leader after 40 ticks, found none")
	}
	if term == 0 {
		t.Fatalf("leader %d has term 0, expected >= 1", leader)
	}
}

func TestRaftReElectionAfterLeaderCrash(t *testing.T) {
	c := newMiniCluster(3)
	c.stepTicks(40)

	oldLeader, oldTerm, ok := c.findLeader()
	if !ok {
		t.Fatalf("no initial leader")
	}

	// Crash the leader.
	c.alive[oldLeader] = false
	c.stepTicks(50)

	newLeader, newTerm, ok := c.findLeader()
	if !ok {
		t.Fatalf("expected new leader after old leader crashed")
	}
	if newLeader == oldLeader {
		t.Fatalf("crashed leader %d cannot be re-elected", oldLeader)
	}
	if newTerm <= oldTerm {
		t.Fatalf("newTerm %d should be > oldTerm %d", newTerm, oldTerm)
	}
}

func TestRaftLogReplicationAndFastBacktrack(t *testing.T) {
	c := newMiniCluster(3)
	c.stepTicks(40)

	leader, _, ok := c.findLeader()
	if !ok {
		t.Fatalf("no leader")
	}

	// Partition follower 2 while leader commits 5 commands on majority {0, 1}.
	lagging := (leader + 1) % 3
	for id := 0; id < 3; id++ {
		if id != lagging {
			c.partitioned[[2]int{id, lagging}] = true
			c.partitioned[[2]int{lagging, id}] = true
		}
	}

	for i := 1; i <= 5; i++ {
		cmd := []byte(fmt.Sprintf("put:k%d=v%d", i, i))
		_, _, isLeader, actions := c.nodes[leader].Propose(cmd)
		if !isLeader {
			t.Fatalf("node %d lost leadership unexpectedly", leader)
		}
		c.runActions(leader, actions)
		c.drainMessages()
	}
	c.stepTicks(5)

	if len(c.applied[leader]) != 5 {
		t.Fatalf("leader applied %d commands, want 5", len(c.applied[leader]))
	}
	if len(c.applied[lagging]) != 0 {
		t.Fatalf("partitioned follower should have 0 applied commands, got %d", len(c.applied[lagging]))
	}

	// Heal partition and let leader catch up lagging follower.
	c.partitioned = make(map[[2]int]bool)
	c.stepTicks(15)

	if len(c.applied[lagging]) != 5 {
		t.Fatalf("after healing, lagging follower applied %d commands, want 5", len(c.applied[lagging]))
	}
	for i := 0; i < 5; i++ {
		if string(c.applied[lagging][i].Command) != string(c.applied[leader][i].Command) {
			t.Fatalf("log mismatch at %d: %q vs %q", i, c.applied[lagging][i].Command, c.applied[leader][i].Command)
		}
	}
}

func TestRaftCrashRecoveryFromPersistedState(t *testing.T) {
	c := newMiniCluster(3)
	c.stepTicks(40)

	leader, _, ok := c.findLeader()
	if !ok {
		t.Fatalf("no leader")
	}
	_, _, _, actions := c.nodes[leader].Propose([]byte("put:durable=yes"))
	c.runActions(leader, actions)
	c.stepTicks(10)

	// Crash and reboot a follower purely from its ActionPersist snapshot.
	follower := (leader + 1) % 3
	saved := c.persisted[follower]
	peers := []int{0, 1, 2}
	c.nodes[follower] = NewNode(follower, peers, 999, saved)
	c.applied[follower] = nil

	c.stepTicks(15)
	if len(c.applied[follower]) != 1 || string(c.applied[follower][0].Command) != "put:durable=yes" {
		t.Fatalf("rebooted follower did not replay/apply committed command: %+v", c.applied[follower])
	}
}
