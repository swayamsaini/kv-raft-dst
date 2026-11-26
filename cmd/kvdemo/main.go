package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/swayamsaini/tardidb/pkg/server"
)

func main() {
	fmt.Println("==========================================================")
	fmt.Println("  TardiDB: Live Interactive 3-Node TCP Raft Cluster")
	fmt.Println("==========================================================")

	peerIDs := []int{0, 1, 2}
	nodes := make([]*server.NodeServer, 3)
	alive := make([]bool, 3)
	addrs := make(map[int]string)

	for i, id := range peerIDs {
		s, err := server.NewNodeServer(id, peerIDs, int64(500+id*17))
		if err != nil {
			fmt.Printf("Failed to start Node %d: %v\n", id, err)
			return
		}
		nodes[i] = s
		alive[i] = true
		addrs[id] = s.RPCAddr()
	}

	for i, s := range nodes {
		s.SetPeers(addrs)
		s.Start()
		fmt.Printf("  Started Node %d on TCP %s\n", i, s.RPCAddr())
	}
	defer func() {
		for i, s := range nodes {
			if alive[i] {
				s.Stop()
			}
		}
	}()

	fmt.Print("\nWaiting for initial Raft leader election...")
	leader := waitForLeader(nodes, alive, 3*time.Second)
	if leader == -1 {
		fmt.Println(" timed out!")
		return
	}
	fmt.Printf(" Node %d elected LEADER!\n\n", leader)

	printHelp()

	scanner := bufio.NewScanner(os.Stdin)
	var seq uint64 = 1

	for {
		fmt.Print("kv> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "help":
			printHelp()

		case "status":
			for i, s := range nodes {
				if !alive[i] {
					fmt.Printf("  Node %d : [DEAD / CRASHED]\n", i)
					continue
				}
				role := "Follower"
				if s.IsLeader() {
					role = "LEADER (Boss)"
				}
				fmt.Printf("  Node %d : [ALIVE]  Role = %s  (%s)\n", i, role, s.RPCAddr())
			}

		case "put":
			if len(parts) < 3 {
				fmt.Println("  Usage: put <key> <value>")
				continue
			}
			key := parts[1]
			val := strings.Join(parts[2:], " ")
			lid := findLeader(nodes, alive)
			if lid == -1 {
				fmt.Println("  No leader available right now (need at least 2 of 3 nodes alive).")
				continue
			}
			seq++
			if err := nodes[lid].Put(key, val, 1, seq); err != nil {
				fmt.Printf("  Put failed: %v\n", err)
				continue
			}
			// Give followers a tiny moment to finish applying over TCP.
			time.Sleep(80 * time.Millisecond)
			fmt.Printf("  [OK] Committed via Leader Node %d and replicated to quorum: %s = %q\n", lid, key, val)

		case "get":
			if len(parts) < 2 {
				fmt.Println("  Usage: get <key>")
				continue
			}
			key := parts[1]
			for i, s := range nodes {
				if !alive[i] {
					fmt.Printf("  Node %d : [DEAD]\n", i)
					continue
				}
				val, ok := s.GetLocal(key)
				if ok {
					fmt.Printf("  Node %d : %q\n", i, val)
				} else {
					fmt.Printf("  Node %d : (not found)\n", i)
				}
			}

		case "kill":
			if len(parts) < 2 {
				fmt.Println("  Usage: kill <0|1|2>")
				continue
			}
			id, err := strconv.Atoi(parts[1])
			if err != nil || id < 0 || id >= len(nodes) {
				fmt.Println("  Invalid node ID. Use 0, 1, or 2.")
				continue
			}
			if !alive[id] {
				fmt.Printf("  Node %d is already dead.\n", id)
				continue
			}
			wasLeader := nodes[id].IsLeader()
			nodes[id].Stop()
			alive[id] = false
			fmt.Printf("  [CRASHED] Killed Node %d!\n", id)
			if wasLeader {
				fmt.Print("  Leader died! Watching surviving nodes elect a new Leader...")
				newLeader := waitForLeader(nodes, alive, 3*time.Second)
				if newLeader == -1 {
					fmt.Println(" no quorum left (need >= 2 alive nodes).")
				} else {
					fmt.Printf(" Node %d is the NEW LEADER!\n", newLeader)
				}
			}

		case "exit", "quit":
			fmt.Println("Shutting down 3-node cluster...")
			return

		default:
			fmt.Println("  Unknown command. Type 'help' for commands.")
		}
	}
}

func findLeader(nodes []*server.NodeServer, alive []bool) int {
	for i, s := range nodes {
		if alive[i] && s.IsLeader() {
			return i
		}
	}
	return -1
}

func waitForLeader(nodes []*server.NodeServer, alive []bool, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if id := findLeader(nodes, alive); id != -1 {
			return id
		}
		time.Sleep(30 * time.Millisecond)
	}
	return -1
}

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  status           -> See which nodes are ALIVE/DEAD and who is LEADER")
	fmt.Println("  put <key> <val>  -> Save a key-value pair through Raft consensus")
	fmt.Println("  get <key>        -> Read the key from every alive node")
	fmt.Println("  kill <0|1|2>     -> Crash a node (try killing the LEADER!)")
	fmt.Println("  exit             -> Stop cluster and quit")
	fmt.Println()
}
