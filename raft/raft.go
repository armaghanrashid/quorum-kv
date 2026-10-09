// Package raft implements the Raft consensus algorithm: leader election with
// PreVote, log replication with fast conflict backtracking, durable state,
// and log compaction through snapshots.
//
// A Node owns a replicated log. The application feeds commands in through
// Propose and consumes committed ones, in order, from ApplyCh. All network
// access goes through the Transport interface and all durability through the
// Storage interface, so the same Node runs unchanged on the in-memory
// simulator used by the tests and on the HTTP transport used by cmd/quorum.
package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Role is a node's current role in the protocol.
type Role int

const (
	Follower Role = iota
	// PreCandidate is a node that has timed out and is asking whether it
	// would win an election, without yet touching its term.
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "precandidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// ApplyMsg is delivered on ApplyCh. Exactly one of CommandValid and
// SnapshotValid is true. Messages arrive strictly in log order; a snapshot
// message means "replace your state with this, it covers everything through
// SnapshotIndex", and the next command message will be SnapshotIndex+1.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	// Noop is true for the empty entry a leader appends when it takes office.
	// It occupies an index but carries no command; applications ignore it.
	Noop         bool
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// EventKind classifies an Event.
type EventKind string

const (
	EventRole     EventKind = "role"     // the node changed role
	EventCommit   EventKind = "commit"   // the node's commit index advanced
	EventSnapshot EventKind = "snapshot" // the node compacted or installed a snapshot
	EventHalt     EventKind = "halt"     // the node stopped itself after a storage failure
)

// Event is a notification for tests and tooling. The Observer is invoked
// synchronously with the node's lock held, so it must return quickly and must
// not call back into the Node.
type Event struct {
	Time   time.Time
	Node   int
	Kind   EventKind
	Role   Role
	Term   uint64
	Index  uint64
	Detail string
}

// Config holds the tunables of a Node; set them with Options.
type Config struct {
	ElectionTimeoutMin  time.Duration
	ElectionTimeoutMax  time.Duration
	HeartbeatInterval   time.Duration
	PreVote             bool
	MaxEntriesPerAppend int
	Seed                int64
	Observer            func(Event)
}

func defaultConfig() Config {
	return Config{
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		HeartbeatInterval:   30 * time.Millisecond,
		PreVote:             true,
		MaxEntriesPerAppend: 128,
		Seed:                1,
	}
}

// Option customises a Node.
type Option func(*Config)

// WithElectionTimeout sets the randomised election timeout range. The
// heartbeat interval should be several times smaller than min.
func WithElectionTimeout(min, max time.Duration) Option {
	return func(c *Config) { c.ElectionTimeoutMin, c.ElectionTimeoutMax = min, max }
}

// WithHeartbeat sets how often a leader contacts each follower when idle.
func WithHeartbeat(d time.Duration) Option { return func(c *Config) { c.HeartbeatInterval = d } }

// WithPreVote enables or disables the PreVote phase (enabled by default).
func WithPreVote(on bool) Option { return func(c *Config) { c.PreVote = on } }

// WithSeed seeds the node's election-timeout randomness.
func WithSeed(seed int64) Option { return func(c *Config) { c.Seed = seed } }

// WithObserver installs an event callback; see Event for its constraints.
func WithObserver(f func(Event)) Option { return func(c *Config) { c.Observer = f } }

// Status is a point-in-time snapshot of a node, for tests and tooling.
type Status struct {
	ID            int
	Term          uint64
	Role          Role
	Leader        int // -1 if unknown
	CommitIndex   uint64
	LastApplied   uint64
	LastIndex     uint64
	SnapshotIndex uint64
	Stopped       bool
}

// Node is one member of a Raft group.
type Node struct {
	mu   sync.Mutex
	cond *sync.Cond // signals the applier

	id     int
	peers  []int // every member of the group, including id
	cfg    Config
	tr     Transport
	store  Storage
	rng    *rand.Rand
	applyC chan ApplyMsg

	// Persistent state (see State).
	term     uint64
	votedFor int
	log      *raftLog
	snapshot []byte

	// Volatile state.
	role        Role
	leaderID    int
	commitIndex uint64
	lastApplied uint64
	pendingSnap *ApplyMsg // a snapshot the applier must deliver before anything else
	deadline    time.Time // when to start the next campaign
	lastContact time.Time // last valid message from a current leader
	round       uint64    // identifies the current PreVote/vote round so late replies are ignored
	grants      map[int]bool
	nextIndex   map[int]uint64
	matchIndex  map[int]uint64
	kick        map[int]chan struct{}
	stopped     bool
	stopCh      chan struct{}
	wg          sync.WaitGroup
	haltErr     error
}

// New creates a node, restores any state saved in store, and starts it.
// peers lists the ids of every member of the group including id itself.
// The caller must route incoming RPCs to the returned Node, which implements
// Handler, and must drain ApplyCh.
//
// New panics if store.Load fails: a node that cannot read its own past has no
// safe way to continue.
func New(id int, peers []int, tr Transport, store Storage, opts ...Option) *Node {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	n := &Node{
		id:       id,
		peers:    append([]int(nil), peers...),
		cfg:      cfg,
		tr:       tr,
		store:    store,
		rng:      rand.New(rand.NewSource(cfg.Seed*7919 + int64(id))),
		applyC:   make(chan ApplyMsg, 64),
		votedFor: NoVote,
		leaderID: NoVote,
		log:      newLog(0, 0, nil),
		stopCh:   make(chan struct{}),
	}
	n.cond = sync.NewCond(&n.mu)

	st, ok, err := store.Load()
	if err != nil {
		panic("raft: loading persisted state: " + err.Error())
	}
	if ok {
		n.term, n.votedFor = st.Term, st.VotedFor
		n.log = newLog(st.SnapshotIndex, st.SnapshotTerm, st.Entries)
		n.snapshot = st.Snapshot
		n.commitIndex, n.lastApplied = st.SnapshotIndex, st.SnapshotIndex
		if st.SnapshotIndex > 0 {
			// The application lost its in-memory state with the process; give
			// it the snapshot back before any log entry.
			n.pendingSnap = &ApplyMsg{
				SnapshotValid: true, Snapshot: st.Snapshot,
				SnapshotIndex: st.SnapshotIndex, SnapshotTerm: st.SnapshotTerm,
			}
		}
	}
	n.resetDeadlineLocked()

	n.wg.Add(2)
	go n.ticker()
	go n.applier()
	return n
}

// ApplyCh delivers committed entries and installed snapshots in log order. It
// is closed when the node stops.
func (n *Node) ApplyCh() <-chan ApplyMsg { return n.applyC }

// ID returns the node's id.
func (n *Node) ID() int { return n.id }

// Status returns a snapshot of the node's externally interesting state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID: n.id, Term: n.term, Role: n.role, Leader: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		LastIndex: n.log.lastIndex(), SnapshotIndex: n.log.snapIndex,
		Stopped: n.stopped,
	}
}

// Stop shuts the node down and waits for its goroutines. Whatever has been
// persisted stays persisted; everything else is gone, exactly as in a crash.
func (n *Node) Stop() {
	n.mu.Lock()
	n.stopLocked()
	n.mu.Unlock()
	n.wg.Wait()
}

func (n *Node) stopLocked() {
	if n.stopped {
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.cond.Broadcast()
}

// haltLocked stops the node because its storage failed.
func (n *Node) haltLocked(err error) {
	if n.stopped {
		return
	}
	n.haltErr = err
	n.emitLocked(EventHalt, err.Error(), 0)
	n.stopLocked()
}

// HaltError reports why the node stopped itself, or nil.
func (n *Node) HaltError() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.haltErr
}

func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

func (n *Node) emitLocked(kind EventKind, detail string, index uint64) {
	if n.cfg.Observer == nil {
		return
	}
	n.cfg.Observer(Event{
		Time: time.Now(), Node: n.id, Kind: kind, Role: n.role,
		Term: n.term, Index: index, Detail: detail,
	})
}

func (n *Node) setRoleLocked(r Role) {
	if n.role == r {
		return
	}
	n.role = r
	n.emitLocked(EventRole, r.String(), 0)
}

func (n *Node) resetDeadlineLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	d := n.cfg.ElectionTimeoutMin
	if span > 0 {
		d += time.Duration(n.rng.Int63n(int64(span)))
	}
	n.deadline = time.Now().Add(d)
}

// ticker fires campaigns. Heartbeats are not driven from here; each
// replicator goroutine owns its own heartbeat clock.
func (n *Node) ticker() {
	defer n.wg.Done()
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		}
		n.mu.Lock()
		if !n.stopped && n.role != Leader && time.Now().After(n.deadline) {
			n.campaignLocked()
		}
		n.mu.Unlock()
	}
}
