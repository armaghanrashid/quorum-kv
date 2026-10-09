package raft_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/armaghanrashid/quorum-kv/internal/cluster"
	"github.com/armaghanrashid/quorum-kv/raft"
)

// Timing used by every cluster test. Real wall-clock time keeps the code under
// test identical to production; the ranges are short enough that a full
// election costs a few hundred milliseconds.
var testOpts = []raft.Option{
	raft.WithElectionTimeout(150*time.Millisecond, 300*time.Millisecond),
	raft.WithHeartbeat(30 * time.Millisecond),
}

func newCluster(t *testing.T, n int, seed int64, snapEvery uint64, extra ...raft.Option) *cluster.Cluster {
	t.Helper()
	opts := append(append([]raft.Option(nil), testOpts...), extra...)
	c := cluster.New(n, seed, cluster.RecorderFactory(snapEvery), opts...)
	t.Cleanup(c.Close)
	return c
}

func rec(c *cluster.Cluster, i int) *cluster.Recorder {
	a := c.App(i)
	if a == nil {
		return nil
	}
	return a.(*cluster.Recorder)
}

func mustLeader(t *testing.T, c *cluster.Cluster) (int, uint64) {
	t.Helper()
	id, term, ok := c.WaitLeader(10 * time.Second)
	if !ok {
		t.Fatalf("no stable leader within 10s")
	}
	return id, term
}

// liveRecorders returns the recorders of nodes that are up.
func liveRecorders(c *cluster.Cluster) []*cluster.Recorder {
	var out []*cluster.Recorder
	for i := range c.N {
		if r := rec(c, i); r != nil && !c.Sim.IsDown(i) {
			out = append(out, r)
		}
	}
	return out
}

// commit proposes cmd until some leader accepts it and a majority of nodes
// have applied it at the index it was given, then returns that index. Retrying
// can duplicate a command whose first attempt was committed after the caller
// gave up, so tests use unique command strings and assert "present", not
// "present exactly once".
func commit(t *testing.T, c *cluster.Cluster, cmd string) uint64 {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		_, idx, _, ok := c.Propose([]byte(cmd))
		if !ok {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if c.WaitFor(1500*time.Millisecond, func() bool {
			n := 0
			for _, r := range liveRecorders(c) {
				if v, ok := r.Get(idx); ok && v == cmd {
					n++
				}
			}
			return n > c.N/2
		}) {
			return idx
		}
	}
	t.Fatalf("command %q never committed", cmd)
	return 0
}

// converge waits until every live node has applied through the highest index
// any of them has, then checks they hold identical commands.
func converge(t *testing.T, c *cluster.Cluster) {
	t.Helper()
	ok := c.WaitFor(15*time.Second, func() bool {
		var max uint64
		rs := liveRecorders(c)
		for _, r := range rs {
			if l := r.Last(); l > max {
				max = l
			}
		}
		for _, r := range rs {
			if r.Last() != max {
				return false
			}
		}
		return true
	})
	if !ok {
		for i := range c.N {
			if r := rec(c, i); r != nil {
				t.Logf("node %d applied through %d (%+v)", i, r.Last(), c.Node(i).Status())
			}
		}
		t.Fatalf("live nodes never converged")
	}
	rs := liveRecorders(c)
	if err := cluster.CheckAgreement(rs...); err != nil {
		t.Fatalf("state machine divergence: %v", err)
	}
	want := rs[0].Commands()
	for i, r := range rs[1:] {
		if got := r.Commands(); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("node %d commands differ from node %d:\n%v\n%v", i+1, 0, got, want)
		}
	}
}

func noViolations(t *testing.T, c *cluster.Cluster) {
	t.Helper()
	if v := c.Violations(); len(v) > 0 {
		t.Fatalf("election safety violated: %v", v)
	}
}

// randomPartition splits n nodes into two non-empty groups.
func randomPartition(rng *rand.Rand, n int) [][]int {
	perm := rng.Perm(n)
	k := 1 + rng.Intn(n-1)
	return [][]int{perm[:k], perm[k:]}
}

// stubTransport fails every call; it lets a single node be driven purely
// through its Handler methods.
type stubTransport struct{}

func (stubTransport) RequestVote(int, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, fmt.Errorf("stub")
}
func (stubTransport) AppendEntries(int, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, fmt.Errorf("stub")
}
func (stubTransport) InstallSnapshot(int, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return nil, fmt.Errorf("stub")
}

// quietNode returns a node that never campaigns on its own.
func quietNode(id int, st raft.Storage) *raft.Node {
	return raft.New(id, []int{0, 1, 2}, stubTransport{}, st,
		raft.WithElectionTimeout(time.Hour, 2*time.Hour))
}
