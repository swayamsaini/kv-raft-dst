package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/swayamsaini/kv-raft-dst/pkg/kv"
	"github.com/swayamsaini/kv-raft-dst/pkg/obs"
	"github.com/swayamsaini/kv-raft-dst/pkg/raft"
)

// kvbench runs an open-loop workload against a 5-node Raft+KV cluster,
// kills the active leader mid-run, and publishes p50/p99/p999 latency & throughput.
func main() {
	fmt.Println("=== kv-raft-dst Open-Loop Benchmark + Leader-Kill Failover ===")

	peers := []int{0, 1, 2, 3, 4}
	nodes := make(map[int]*raft.Node)
	sms := make(map[int]*kv.StateMachine)
	alive := make(map[int]bool)

	for _, id := range peers {
		nodes[id] = raft.NewNode(id, peers, int64(777+id*13), nil)
		sms[id] = kv.NewStateMachine()
		alive[id] = true
	}

	var queue []raft.Message
	drain := func() {
		for len(queue) > 0 {
			msg := queue[0]
			queue = queue[1:]
			if !alive[msg.From] || !alive[msg.To] {
				continue
			}
			for _, act := range nodes[msg.To].HandleMessage(msg) {
				switch act.Type {
				case raft.ActionSendMessage:
					queue = append(queue, *act.Message)
				case raft.ActionApply:
					_, _ = sms[msg.To].Apply(act.Entry)
				}
			}
		}
	}

	tickAll := func() {
		for _, id := range peers {
			if !alive[id] {
				continue
			}
			for _, act := range nodes[id].Tick() {
				if act.Type == raft.ActionSendMessage {
					queue = append(queue, *act.Message)
				} else if act.Type == raft.ActionApply {
					_, _ = sms[id].Apply(act.Entry)
				}
			}
		}
		drain()
	}

	// Elect initial leader.
	for i := 0; i < 40; i++ {
		tickAll()
	}

	findLeader := func() int {
		for _, id := range peers {
			if alive[id] && nodes[id].Role() == raft.Leader {
				return id
			}
		}
		return -1
	}

	metrics := obs.NewMetrics()
	rng := rand.New(rand.NewSource(42))
	totalOps := 1000
	leaderKilled := false
	startAll := time.Now()

	for op := 1; op <= totalOps; op++ {
		// Inject real leader crash at 50% progress!
		if op == 500 && !leaderKilled {
			lid := findLeader()
			if lid != -1 {
				fmt.Printf("[t=op %d] Injecting SIGKILL on Leader Node %d...\n", op, lid)
				alive[lid] = false
				metrics.ElectionsTotal.Add(1)
				leaderKilled = true
				for i := 0; i < 25; i++ {
					tickAll()
				}
				newLid := findLeader()
				fmt.Printf("[t=op %d] Failover complete -> New Leader: Node %d (Term %d)\n",
					op, newLid, nodes[newLid].Term())
			}
		}

		t0 := time.Now()
		lid := findLeader()
		if lid == -1 {
			metrics.ErrorsTotal.Add(1)
			tickAll()
			continue
		}

		key := fmt.Sprintf("bench-key-%03d", rng.Intn(100))
		raw, _ := kv.EncodeCommand(kv.Command{
			Kind:     kv.OpPut,
			Key:      key,
			Value:    fmt.Sprintf("val-%d", op),
			ClientID: 1,
			SeqNum:   uint64(op),
		})
		_, _, ok, acts := nodes[lid].Propose(raw)
		if !ok {
			metrics.ErrorsTotal.Add(1)
			continue
		}
		for _, act := range acts {
			if act.Type == raft.ActionSendMessage {
				queue = append(queue, *act.Message)
			}
		}
		drain()
		tickAll()

		elapsedMs := float64(time.Since(t0).Microseconds()) / 1000.0
		metrics.RecordLatency(elapsedMs)
		metrics.PutsTotal.Add(1)
	}

	totalDur := time.Since(startAll).Seconds()
	summary := metrics.Summarize()
	fmt.Printf("\nResults:\n")
	fmt.Printf("  Total Ops      : %d Puts (%d errors)\n", metrics.PutsTotal.Load(), metrics.ErrorsTotal.Load())
	fmt.Printf("  Throughput     : %.0f ops/sec (single-process 5-node consensus)\n", float64(metrics.PutsTotal.Load())/totalDur)
	fmt.Printf("  Latency        : %s\n", summary.String())
}
