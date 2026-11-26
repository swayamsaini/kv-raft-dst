package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/swayamsaini/tardidb/pkg/kv"
	"github.com/swayamsaini/tardidb/pkg/raft"
)

// RaftRPC exposes a Raft node's pure HandleMessage over real TCP sockets (net/rpc).
type RaftRPC struct {
	srv *NodeServer
}

func (r *RaftRPC) Deliver(msg raft.Message, _ *struct{}) error {
	r.srv.deliver(msg)
	return nil
}

// NodeServer wraps the pure deterministic Raft + KV state machine with real TCP sockets,
// real OS timers, and an HTTP client API (/put, /get).
type NodeServer struct {
	mu       sync.Mutex
	id       int
	peers    map[int]string // nodeID -> TCP addr
	raftNode *raft.Node
	sm       *kv.StateMachine

	rpcLn  net.Listener
	httpLn net.Listener
	stopCh chan struct{}
	wg     sync.WaitGroup

	// Pending client write waiters: logIndex -> chan struct{}
	waiters map[uint64]chan struct{}
}

// NewNodeServer creates a networked Raft+KV node listening on rpcAddr (use "127.0.0.1:0" for ephemeral port).
func NewNodeServer(id int, peerIDs []int, seed int64) (*NodeServer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	httpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = ln.Close()
		return nil, err
	}

	s := &NodeServer{
		id:       id,
		peers:    make(map[int]string),
		raftNode: raft.NewNode(id, peerIDs, seed, nil),
		sm:       kv.NewStateMachine(),
		rpcLn:    ln,
		httpLn:   httpLn,
		stopCh:   make(chan struct{}),
		waiters:  make(map[uint64]chan struct{}),
	}
	return s, nil
}

func (s *NodeServer) RPCAddr() string  { return s.rpcLn.Addr().String() }
func (s *NodeServer) HTTPAddr() string { return s.httpLn.Addr().String() }

// SetPeers configures the TCP peer address map before Start().
func (s *NodeServer) SetPeers(peers map[int]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range peers {
		s.peers[k] = v
	}
}

// Start launches the TCP RPC listener, HTTP client API, and Raft tick loop.
func (s *NodeServer) Start() {
	rpcSrv := rpc.NewServer()
	_ = rpcSrv.Register(&RaftRPC{srv: s})

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.rpcLn.Accept()
			if err != nil {
				return
			}
			go rpcSrv.ServeConn(conn)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/put", s.handleHTTPPut)
	mux.HandleFunc("/get", s.handleHTTPGet)
	httpSrv := &http.Server{Handler: mux}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = httpSrv.Serve(s.httpLn)
	}()

	// Real-time Raft tick loop (20ms per logical tick).
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCh:
				_ = httpSrv.Close()
				return
			case <-ticker.C:
				s.mu.Lock()
				acts := s.raftNode.Tick()
				s.mu.Unlock()
				s.executeActions(acts)
			}
		}
	}()
}

// Stop shuts down listeners and background goroutines cleanly.
func (s *NodeServer) Stop() {
	select {
	case <-s.stopCh:
		return
	default:
		close(s.stopCh)
	}
	_ = s.rpcLn.Close()
	_ = s.httpLn.Close()
	s.wg.Wait()
}

func (s *NodeServer) IsLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.raftNode.Role() == raft.Leader
}

func (s *NodeServer) deliver(msg raft.Message) {
	s.mu.Lock()
	acts := s.raftNode.HandleMessage(msg)
	s.mu.Unlock()
	s.executeActions(acts)
}

func (s *NodeServer) executeActions(actions []raft.Action) {
	for _, act := range actions {
		switch act.Type {
		case raft.ActionSendMessage:
			msg := *act.Message
			s.mu.Lock()
			addr := s.peers[msg.To]
			s.mu.Unlock()
			if addr == "" {
				continue
			}
			go func(targetAddr string, m raft.Message) {
				client, err := rpc.Dial("tcp", targetAddr)
				if err != nil {
					return
				}
				defer client.Close()
				var empty struct{}
				_ = client.Call("RaftRPC.Deliver", m, &empty)
			}(addr, msg)

		case raft.ActionApply:
			_, _ = s.sm.Apply(act.Entry)
			s.mu.Lock()
			if ch, ok := s.waiters[act.Entry.Index]; ok {
				close(ch)
				delete(s.waiters, act.Entry.Index)
			}
			s.mu.Unlock()

		case raft.ActionApplySnapshot:
			_ = s.sm.RestoreSnapshot(act.Snapshot)
		}
	}
}

// Put proposes a KV write through Raft and blocks until committed or timeout.
func (s *NodeServer) Put(key, value string, clientID, seqNum uint64) error {
	raw, err := kv.EncodeCommand(kv.Command{
		Kind:     kv.OpPut,
		Key:      key,
		Value:    value,
		ClientID: clientID,
		SeqNum:   seqNum,
	})
	if err != nil {
		return err
	}

	s.mu.Lock()
	idx, _, isLeader, acts := s.raftNode.Propose(raw)
	if !isLeader {
		s.mu.Unlock()
		return fmt.Errorf("node %d is not leader", s.id)
	}
	ch := make(chan struct{})
	s.waiters[idx] = ch
	s.mu.Unlock()

	s.executeActions(acts)

	select {
	case <-ch:
		return nil
	case <-time.After(2 * time.Second):
		return fmt.Errorf("put timed out waiting for commit")
	}
}

// GetLocal reads from this node's applied KV state machine.
func (s *NodeServer) GetLocal(key string) (string, bool) {
	return s.sm.Get(key)
}

func (s *NodeServer) handleHTTPPut(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := r.URL.Query().Get("value")
	if err := s.Put(key, val, 1, uint64(time.Now().UnixNano())); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (s *NodeServer) handleHTTPGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val, ok := s.GetLocal(key)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"key": key, "value": val, "found": ok})
}
