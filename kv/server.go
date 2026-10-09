package kv

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/armaghanrashid/quorum-kv/raft"
)

var (
	// ErrNotLeader means the server cannot order the request right now: it
	// is not the leader, lost leadership while waiting, or its proposal was
	// overwritten. The request may or may not take effect later; the client
	// retries with the same sequence number, which is safe.
	ErrNotLeader = errors.New("kv: not leader")
	// ErrStopped means the server has been shut down.
	ErrStopped = errors.New("kv: server stopped")
)

// ServerOption customises a Server.
type ServerOption func(*Server)

// WithSnapshotEvery makes the server snapshot after every n applied log
// entries (0 disables snapshots).
func WithSnapshotEvery(n uint64) ServerOption { return func(s *Server) { s.snapEvery = n } }

type result struct {
	resp Response
	err  error
}

// waiter is a request parked until its log index is applied.
type waiter struct {
	term     uint64
	clientID int64
	seq      int64
	ch       chan result
}

// Server is one replica of the store, driving one raft.Node.
type Server struct {
	node      *raft.Node
	snapEvery uint64

	mu       sync.Mutex
	sm       *stateMachine
	waiters  map[uint64]*waiter
	lastSnap uint64

	done chan struct{}
	wg   sync.WaitGroup
}

// NewServer attaches a server to node and starts consuming node.ApplyCh.
// After a restart the node replays its snapshot and log through that channel,
// which rebuilds the state; nothing else is persisted here.
func NewServer(node *raft.Node, opts ...ServerOption) *Server {
	s := &Server{
		node: node, sm: newStateMachine(),
		waiters: make(map[uint64]*waiter), done: make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	s.wg.Add(1)
	go s.applyLoop()
	return s
}

// Stop shuts the server down and fails every parked request. It does not stop
// the underlying node.
func (s *Server) Stop() {
	s.mu.Lock()
	select {
	case <-s.done:
		s.mu.Unlock()
		return
	default:
	}
	close(s.done)
	for idx, w := range s.waiters {
		w.ch <- result{err: ErrStopped}
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// Node exposes the underlying Raft node.
func (s *Server) Node() *raft.Node { return s.node }

// Do executes req and blocks until it has been applied, ctx expires, or the
// server learns it cannot complete it. Every operation, including Get, goes
// through the log: a leader that answered reads from memory could be a deposed
// leader that does not know it yet, and would serve stale data.
func (s *Server) Do(ctx context.Context, req Request) (Response, error) {
	s.mu.Lock()
	select {
	case <-s.done:
		s.mu.Unlock()
		return Response{}, ErrStopped
	default:
	}
	// A retry of something already applied here is answered from the session
	// table; no new log entry is needed.
	if sess := s.sm.Sessions[req.ClientID]; req.Seq == sess.Seq {
		s.mu.Unlock()
		return Response{Value: sess.Value}, nil
	}
	// Propose and register under s.mu: the apply loop needs s.mu, so the
	// entry cannot be applied before its waiter exists.
	index, term, ok := s.node.Propose(encodeRequest(req))
	if !ok {
		s.mu.Unlock()
		return Response{}, ErrNotLeader
	}
	w := &waiter{term: term, clientID: req.ClientID, seq: req.Seq, ch: make(chan result, 1)}
	if old := s.waiters[index]; old != nil {
		old.ch <- result{err: ErrNotLeader} // an overwritten proposal from a past term
	}
	s.waiters[index] = w
	s.mu.Unlock()

	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case r := <-w.ch:
			return r.resp, r.err
		case <-ctx.Done():
			s.forget(index, w)
			return Response{}, ctx.Err()
		case <-tick.C:
			// If leadership moved on, our entry may never commit; tell the
			// client to look elsewhere rather than make it wait out a timeout.
			if st := s.node.Status(); st.Role != raft.Leader || st.Term != term {
				s.forget(index, w)
				return Response{}, ErrNotLeader
			}
		}
	}
}

func (s *Server) forget(index uint64, w *waiter) {
	s.mu.Lock()
	if s.waiters[index] == w {
		delete(s.waiters, index)
	}
	s.mu.Unlock()
}

func (s *Server) applyLoop() {
	defer s.wg.Done()
	ch := s.node.ApplyCh()
	for {
		select {
		case <-s.done:
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			s.handle(m)
		}
	}
}

func (s *Server) handle(m raft.ApplyMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.SnapshotValid {
		sm, err := restoreStateMachine(m.Snapshot)
		if err != nil {
			panic("kv: corrupt snapshot: " + err.Error())
		}
		s.sm = sm
		s.lastSnap = m.SnapshotIndex
		// Entries the snapshot swallowed will never be applied one by one;
		// their clients retry and are answered from the restored sessions.
		for idx, w := range s.waiters {
			if idx <= m.SnapshotIndex {
				w.ch <- result{err: ErrNotLeader}
				delete(s.waiters, idx)
			}
		}
		return
	}
	if !m.CommandValid {
		return
	}
	if !m.Noop && len(m.Command) > 0 {
		req, err := decodeRequest(m.Command)
		if err != nil {
			panic("kv: corrupt log entry: " + err.Error())
		}
		resp, _ := s.sm.apply(req)
		if w := s.waiters[m.CommandIndex]; w != nil {
			delete(s.waiters, m.CommandIndex)
			if w.term == m.CommandTerm && w.clientID == req.ClientID && w.seq == req.Seq {
				w.ch <- result{resp: resp}
			} else {
				w.ch <- result{err: ErrNotLeader} // someone else's command won this index
			}
		}
	} else if w := s.waiters[m.CommandIndex]; w != nil {
		delete(s.waiters, m.CommandIndex)
		w.ch <- result{err: ErrNotLeader}
	}
	if s.snapEvery > 0 && m.CommandIndex-s.lastSnap >= s.snapEvery {
		s.lastSnap = m.CommandIndex
		s.node.Snapshot(m.CommandIndex, s.sm.snapshot())
	}
}

// Get, Put and Append are convenience wrappers around Do for callers that
// manage their own client id and sequence numbers; most callers use a Clerk.
func (s *Server) Get(ctx context.Context, client, seq int64, key string) (string, error) {
	r, err := s.Do(ctx, Request{Type: OpGet, Key: key, ClientID: client, Seq: seq})
	return r.Value, err
}

func (s *Server) Put(ctx context.Context, client, seq int64, key, value string) error {
	_, err := s.Do(ctx, Request{Type: OpPut, Key: key, Value: value, ClientID: client, Seq: seq})
	return err
}

func (s *Server) Append(ctx context.Context, client, seq int64, key, value string) error {
	_, err := s.Do(ctx, Request{Type: OpAppend, Key: key, Value: value, ClientID: client, Seq: seq})
	return err
}
