// Package cluster wires N Raft nodes to a simulated network and gives tests
// and the demo a single handle for crashing, restarting and observing them.
// It is internal because it exists to exercise the library, not to be part of
// its API.
package cluster

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/armaghanrashid/quorum-kv/raft"
	"github.com/armaghanrashid/quorum-kv/transport"
)

// App is whatever consumes a node's ApplyCh (a recorder, a KV server).
type App interface{ Stop() }

// Factory builds the App for a freshly started node. It is called again with a
// new Node, and must rebuild state purely from the ApplyCh, after a restart.
type Factory func(id int, n *raft.Node) App

// Cluster is a set of nodes on one Sim network. Durable storage outlives
// crashes; everything else is rebuilt on restart.
type Cluster struct {
	Sim *transport.Sim
	N   int

	seed    int64
	opts    []raft.Option
	factory Factory

	mu     sync.Mutex
	nodes  []*raft.Node
	apps   []App
	stores []*raft.MemStorage
	incarn []int

	evMu     sync.Mutex
	leaders  map[uint64]map[int]bool // term -> ids that became leader in it
	observer func(raft.Event)
}

// New starts an n-node cluster. opts are applied to every node; the cluster
// supplies its own seed and observer after them.
func New(n int, seed int64, f Factory, opts ...raft.Option) *Cluster {
	c := &Cluster{
		Sim: transport.NewSim(seed), N: n, seed: seed, opts: opts, factory: f,
		nodes: make([]*raft.Node, n), apps: make([]App, n),
		stores: make([]*raft.MemStorage, n), incarn: make([]int, n),
		leaders: make(map[uint64]map[int]bool),
	}
	for i := range n {
		c.stores[i] = raft.NewMemStorage()
	}
	c.Sim.SetLifecycle(c.stopNode, c.startNode)
	for i := range n {
		c.startNode(i)
	}
	return c
}

// SetObserver forwards every node's events to f (in addition to the internal
// bookkeeping). f is called with node locks held; keep it short.
func (c *Cluster) SetObserver(f func(raft.Event)) {
	c.evMu.Lock()
	defer c.evMu.Unlock()
	c.observer = f
}

func (c *Cluster) observe(ev raft.Event) {
	c.evMu.Lock()
	defer c.evMu.Unlock()
	if ev.Kind == raft.EventRole && ev.Role == raft.Leader {
		if c.leaders[ev.Term] == nil {
			c.leaders[ev.Term] = make(map[int]bool)
		}
		c.leaders[ev.Term][ev.Node] = true
	}
	if c.observer != nil {
		c.observer(ev)
	}
}

func (c *Cluster) peerIDs() []int {
	ids := make([]int, c.N)
	for i := range ids {
		ids[i] = i
	}
	return ids
}

func (c *Cluster) startNode(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.incarn[i]++
	opts := append(append([]raft.Option(nil), c.opts...),
		raft.WithSeed(c.seed*131+int64(c.incarn[i])),
		raft.WithObserver(c.observe))
	n := raft.New(i, c.peerIDs(), c.Sim.Endpoint(i), c.stores[i], opts...)
	c.nodes[i] = n
	c.Sim.Register(i, n)
	if c.factory != nil {
		c.apps[i] = c.factory(i, n)
	}
}

func (c *Cluster) stopNode(i int) {
	c.mu.Lock()
	n, a := c.nodes[i], c.apps[i]
	c.apps[i] = nil
	c.mu.Unlock()
	if a != nil {
		a.Stop()
	}
	n.Stop()
}

// Node returns the current incarnation of node i.
func (c *Cluster) Node(i int) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

// App returns node i's application, or nil while it is crashed.
func (c *Cluster) App(i int) App {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.apps[i]
}

// Store returns node i's durable storage.
func (c *Cluster) Store(i int) *raft.MemStorage { return c.stores[i] }

// Close stops everything.
func (c *Cluster) Close() {
	for i := range c.N {
		if !c.Sim.IsDown(i) {
			c.Sim.Crash(i)
		}
	}
}

// Leader returns the live leader with the highest term. A deposed leader that
// has not yet heard about a newer term can still believe it leads; the highest
// term is the real one.
func (c *Cluster) Leader() (id int, term uint64, ok bool) {
	for i := range c.N {
		if c.Sim.IsDown(i) {
			continue
		}
		st := c.Node(i).Status()
		if st.Role == raft.Leader && (!ok || st.Term > term) {
			id, term, ok = i, st.Term, true
		}
	}
	return
}

// WaitFor polls cond every 5 ms until it is true or the timeout passes.
func (c *Cluster) WaitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// WaitLeader waits for any live leader whose term a majority of live nodes
// share, i.e. an established leader rather than a freshly self-declared one.
func (c *Cluster) WaitLeader(timeout time.Duration) (id int, term uint64, ok bool) {
	c.WaitFor(timeout, func() bool {
		lid, lt, lok := c.Leader()
		if !lok {
			return false
		}
		agree, live := 0, 0
		for i := range c.N {
			if c.Sim.IsDown(i) {
				continue
			}
			live++
			if c.Node(i).Status().Term == lt {
				agree++
			}
		}
		if agree > c.N/2 {
			id, term, ok = lid, lt, true
		}
		return ok
	})
	return
}

// Propose sends cmd to whichever live node currently claims leadership. It
// returns the leader, the index and term it was appended at.
func (c *Cluster) Propose(cmd []byte) (leader int, index, term uint64, ok bool) {
	id, _, found := c.Leader()
	if !found {
		return 0, 0, 0, false
	}
	index, term, ok = c.Node(id).Propose(cmd)
	return id, index, term, ok
}

// Violations lists every term in which more than one node became leader, which
// would break Raft's election safety property. It must always be empty.
func (c *Cluster) Violations() []string {
	c.evMu.Lock()
	defer c.evMu.Unlock()
	var out []string
	for term, ids := range c.leaders {
		if len(ids) > 1 {
			var l []int
			for id := range ids {
				l = append(l, id)
			}
			sort.Ints(l)
			out = append(out, fmt.Sprintf("term %d had %d leaders: %v", term, len(l), l))
		}
	}
	sort.Strings(out)
	return out
}

// LeaderTerms returns how many distinct terms have had a leader.
func (c *Cluster) LeaderTerms() int {
	c.evMu.Lock()
	defer c.evMu.Unlock()
	return len(c.leaders)
}
