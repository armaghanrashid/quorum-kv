// Package transport carries Raft RPCs between nodes: Sim is a deterministic-
// seed in-process network with fault injection for tests and the demo, and
// HTTP is the real thing for cmd/quorum.
package transport

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/armaghanrashid/quorum-kv/raft"
)

// ErrUnreachable is returned for any RPC that did not complete: the request or
// reply was dropped, a partition separated the nodes, or an endpoint is down.
var ErrUnreachable = errors.New("transport: unreachable")

// Sim is a simulated network. Every RPC pays a random delay each way, may be
// dropped, and is cut off by partitions and crashes. Fault decisions are drawn
// from a seeded generator, so a seed reproduces the same fault *rates* and
// partition choices; goroutine scheduling still makes exact interleavings vary
// run to run, which is why tests assert properties, not traces.
type Sim struct {
	mu       sync.Mutex
	rng      *rand.Rand
	handlers map[int]raft.Handler
	down     map[int]bool
	group    map[int]int // partition group per node; nil means fully connected
	dropRate float64
	minDelay time.Duration
	maxDelay time.Duration

	onCrash   func(id int)
	onRestart func(id int)

	sent, dropped int
}

// NewSim returns a fully connected network with no loss and 1-3 ms latency.
func NewSim(seed int64) *Sim {
	return &Sim{
		rng:      rand.New(rand.NewSource(seed)),
		handlers: make(map[int]raft.Handler),
		down:     make(map[int]bool),
		minDelay: time.Millisecond,
		maxDelay: 3 * time.Millisecond,
	}
}

// Register installs the receiver for node id, replacing any previous one (a
// restarted node registers its new incarnation here).
func (s *Sim) Register(id int, h raft.Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[id] = h
}

// SetLifecycle installs callbacks run by Crash and Restart so that whoever
// owns the node objects can stop and recreate them.
func (s *Sim) SetLifecycle(onCrash, onRestart func(id int)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onCrash, s.onRestart = onCrash, onRestart
}

// Partition splits the network into the given groups. Nodes in different
// groups cannot exchange messages in either direction. Nodes not named in any
// group form one extra group together.
func (s *Sim) Partition(groups ...[]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.group = make(map[int]int)
	for gi, g := range groups {
		for _, id := range g {
			s.group[id] = gi + 1
		}
	}
}

// Heal removes every partition.
func (s *Sim) Heal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.group = nil
}

// SetDropRate makes each one-way message (request or reply) vanish
// independently with probability p.
func (s *Sim) SetDropRate(p float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropRate = p
}

// SetDelay sets the uniform one-way latency range.
func (s *Sim) SetDelay(min, max time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if max < min {
		max = min
	}
	s.minDelay, s.maxDelay = min, max
}

// Crash takes node id off the network and, via the lifecycle callback, stops
// it. Messages to or from it fail until Restart.
func (s *Sim) Crash(id int) {
	s.mu.Lock()
	s.down[id] = true
	f := s.onCrash
	s.mu.Unlock()
	if f != nil {
		f(id)
	}
}

// Restart brings node id back: the lifecycle callback builds a fresh
// incarnation from durable storage, then the network lets it talk again.
func (s *Sim) Restart(id int) {
	s.mu.Lock()
	f := s.onRestart
	s.mu.Unlock()
	if f != nil {
		f(id)
	}
	s.mu.Lock()
	s.down[id] = false
	s.mu.Unlock()
}

// IsDown reports whether id is crashed.
func (s *Sim) IsDown(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.down[id]
}

// Stats returns the number of one-way messages attempted and lost so far
// (dropped, partitioned or sent to a crashed node).
func (s *Sim) Stats() (sent, lost int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent, s.dropped
}

// Endpoint returns the Transport used by node from.
func (s *Sim) Endpoint(from int) raft.Transport { return &endpoint{s: s, from: from} }

type endpoint struct {
	s    *Sim
	from int
}

func (e *endpoint) RequestVote(to int, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return roundTrip(e.s, e.from, to, func(h raft.Handler) *raft.RequestVoteReply { return h.HandleRequestVote(a) })
}

func (e *endpoint) AppendEntries(to int, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return roundTrip(e.s, e.from, to, func(h raft.Handler) *raft.AppendEntriesReply { return h.HandleAppendEntries(a) })
}

func (e *endpoint) InstallSnapshot(to int, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return roundTrip(e.s, e.from, to, func(h raft.Handler) *raft.InstallSnapshotReply { return h.HandleInstallSnapshot(a) })
}

// oneWay decides the fate of a single message: how long it takes and whether
// it arrives. Partition and crash state is evaluated at delivery time, after
// the delay, so a fault that begins while a message is in flight kills it.
func (s *Sim) delay() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.minDelay
	if span := s.maxDelay - s.minDelay; span > 0 {
		d += time.Duration(s.rng.Int63n(int64(span) + 1))
	}
	return d
}

func (s *Sim) deliverable(from, to int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent++
	ok := !s.down[from] && !s.down[to]
	if ok && s.group != nil && s.group[from] != s.group[to] {
		ok = false
	}
	if ok && s.dropRate > 0 && s.rng.Float64() < s.dropRate {
		ok = false
	}
	if !ok {
		s.dropped++
	}
	return ok
}

func (s *Sim) handler(id int) raft.Handler {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handlers[id]
}

// roundTrip runs request -> handler -> reply with a delay on each leg. The
// handler executes in the caller's goroutine; the sleeps model the wire. A
// lost reply still means the handler ran: the caller sees an error although
// the remote state changed, exactly the ambiguity real networks create.
func roundTrip[T any](s *Sim, from, to int, call func(raft.Handler) *T) (*T, error) {
	time.Sleep(s.delay())
	if !s.deliverable(from, to) {
		return nil, ErrUnreachable
	}
	h := s.handler(to)
	if h == nil {
		return nil, ErrUnreachable
	}
	resp := call(h)
	time.Sleep(s.delay())
	if !s.deliverable(to, from) {
		return nil, ErrUnreachable
	}
	return resp, nil
}
