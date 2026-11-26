package server

import (
	"testing"
	"time"
)

func TestRealTCPThreeNodeClusterElectionAndReplication(t *testing.T) {
	peerIDs := []int{0, 1, 2}
	servers := make([]*NodeServer, 3)
	addrs := make(map[int]string)

	for i, id := range peerIDs {
		s, err := NewNodeServer(id, peerIDs, int64(500+id*17))
		if err != nil {
			t.Fatalf("NewNodeServer(%d) failed: %v", id, err)
		}
		servers[i] = s
		addrs[id] = s.RPCAddr()
	}
	for _, s := range servers {
		s.SetPeers(addrs)
		s.Start()
		defer s.Stop()
	}

	// Wait for real TCP leader election.
	var leader *NodeServer
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if s.IsLeader() {
				leader = s
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if leader == nil {
		t.Fatalf("3-node TCP cluster failed to elect a leader within 3s")
	}

	// Replicate a Put over real TCP sockets.
	if err := leader.Put("hello", "world-over-tcp", 42, 1); err != nil {
		t.Fatalf("leader.Put failed: %v", err)
	}

	// Verify followers replicate and apply the entry over TCP.
	time.Sleep(150 * time.Millisecond)
	replicated := 0
	for _, s := range servers {
		if val, ok := s.GetLocal("hello"); ok && val == "world-over-tcp" {
			replicated++
		}
	}
	if replicated < 2 {
		t.Fatalf("expected majority (>=2) TCP nodes to apply entry, got %d", replicated)
	}
}
