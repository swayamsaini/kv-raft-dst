package raft

import (
	"fmt"
	"testing"
)

func TestRaftSnapshotAndInstallSnapshot(t *testing.T) {
	c := newMiniCluster(3)
	c.stepTicks(40)

	leader, _, ok := c.findLeader()
	if !ok {
		t.Fatalf("no leader")
	}

	// Isolate one follower while leader commits 10 entries.
	lagging := (leader + 1) % 3
	for id := 0; id < 3; id++ {
		if id != lagging {
			c.partitioned[[2]int{id, lagging}] = true
			c.partitioned[[2]int{lagging, id}] = true
		}
	}

	for i := 1; i <= 10; i++ {
		_, _, isL, acts := c.nodes[leader].Propose([]byte(fmt.Sprintf("cmd-%d", i)))
		if !isL {
			t.Fatalf("lost leadership")
		}
		c.runActions(leader, acts)
		c.drainMessages()
	}
	c.stepTicks(5)

	// Leader takes a snapshot up to commitIndex, discarding log entries 1..commitIndex!
	snapIdx := c.nodes[leader].CommitIndex()
	snapPayload := []byte("snapshot-state-up-to-10")
	c.runActions(leader, c.nodes[leader].Snapshot(snapIdx, snapPayload))

	if len(c.nodes[leader].LogSnapshot()) != 1 {
		t.Fatalf("expected leader log to compact down to 1 anchor entry, got %d", len(c.nodes[leader].LogSnapshot()))
	}

	// Heal partition: leader MUST send InstallSnapshot to catch up lagging follower.
	c.partitioned = make(map[[2]int]bool)
	var appliedSnap []byte
	for tick := 0; tick < 15; tick++ {
		for id := 0; id < 3; id++ {
			for _, act := range c.nodes[id].Tick() {
				if act.Type == ActionApplySnapshot && id == lagging {
					appliedSnap = act.Snapshot
				}
				c.runActions(id, []Action{act})
			}
		}
		for len(c.queue) > 0 {
			msg := c.queue[0]
			c.queue = c.queue[1:]
			for _, act := range c.nodes[msg.To].HandleMessage(msg) {
				if act.Type == ActionApplySnapshot && msg.To == lagging {
					appliedSnap = act.Snapshot
				}
				c.runActions(msg.To, []Action{act})
			}
		}
	}

	if string(appliedSnap) != string(snapPayload) {
		t.Fatalf("lagging follower did not install snapshot: got %q, want %q", appliedSnap, snapPayload)
	}

	// Propose one more command after snapshot and verify all 3 nodes apply it.
	_, _, _, acts := c.nodes[leader].Propose([]byte("cmd-11"))
	c.runActions(leader, acts)
	c.stepTicks(10)

	lastLagging := c.applied[lagging][len(c.applied[lagging])-1]
	if string(lastLagging.Command) != "cmd-11" {
		t.Fatalf("follower failed to apply post-snapshot entry: got %q", lastLagging.Command)
	}
}

func TestRaftReadIndexLinearizableConfirmation(t *testing.T) {
	c := newMiniCluster(3)
	c.stepTicks(40)

	leader, _, ok := c.findLeader()
	if !ok {
		t.Fatalf("no leader")
	}

	// Commit an entry so leader's no-op + commitIndex are established.
	_, _, _, acts := c.nodes[leader].Propose([]byte("set:x=42"))
	c.runActions(leader, acts)
	c.stepTicks(5)

	// Request ReadIndex on healthy leader -> should succeed after heartbeat quorum.
	rid, isL, rActs := c.nodes[leader].RequestReadIndex()
	if !isL {
		t.Fatalf("RequestReadIndex rejected by leader")
	}
	var ready *ReadReady
	for _, a := range rActs {
		if a.Type == ActionReadReady {
			ready = a.ReadReady
		} else {
			c.runActions(leader, []Action{a})
		}
	}
	for len(c.queue) > 0 {
		msg := c.queue[0]
		c.queue = c.queue[1:]
		for _, a := range c.nodes[msg.To].HandleMessage(msg) {
			if a.Type == ActionReadReady && msg.To == leader {
				ready = a.ReadReady
			} else {
				c.runActions(msg.To, []Action{a})
			}
		}
	}
	if ready == nil || ready.ReadID != rid {
		t.Fatalf("expected ReadReady for ReadID=%d, got %+v", rid, ready)
	}

	// Partition leader from BOTH followers: ReadIndex MUST NOT fire!
	for id := 0; id < 3; id++ {
		if id != leader {
			c.partitioned[[2]int{leader, id}] = true
			c.partitioned[[2]int{id, leader}] = true
		}
	}
	rid2, isL2, rActs2 := c.nodes[leader].RequestReadIndex()
	if !isL2 {
		t.Fatalf("partitioned leader still thinks it is leader until timeout")
	}
	var staleReady *ReadReady
	c.runActions(leader, rActs2)
	c.drainMessages()
	for _, a := range rActs2 {
		if a.Type == ActionReadReady && a.ReadReady.ReadID == rid2 {
			staleReady = a.ReadReady
		}
	}
	if staleReady != nil {
		t.Fatalf("SECURITY BUG: partitioned leader served ReadIndex without majority quorum!")
	}
}
