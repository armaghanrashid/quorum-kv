package raft_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/armaghanrashid/quorum-kv/raft"
)

func TestReplicationAndCommit(t *testing.T) {
	c := newCluster(t, 5, 10, 0)
	mustLeader(t, c)
	for i := range 20 {
		commit(t, c, fmt.Sprintf("cmd-%d", i))
	}
	converge(t, c)
	got := rec(c, 0).Commands()
	for i := range 20 {
		if got[i] != fmt.Sprintf("cmd-%d", i) {
			t.Fatalf("command %d = %q", i, got[i])
		}
	}
}

func TestProposeOnFollowerIsRejected(t *testing.T) {
	c := newCluster(t, 3, 11, 0)
	leader, _ := mustLeader(t, c)
	f := (leader + 1) % 3
	if _, _, ok := c.Node(f).Propose([]byte("x")); ok {
		t.Fatal("a follower accepted a proposal")
	}
}

// A leader cut off from the majority can append but must never commit.
func TestMinorityLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 5, 12, 0)
	old, _ := mustLeader(t, c)
	commit(t, c, "base")
	other := (old + 1) % 5
	var majority []int
	for i := range 5 {
		if i != old && i != other {
			majority = append(majority, i)
		}
	}
	c.Sim.Partition([]int{old, other}, majority)

	before := c.Node(old).Status().CommitIndex
	if _, _, ok := c.Node(old).Propose([]byte("doomed")); !ok {
		t.Fatal("old leader should still believe it leads")
	}
	// The majority side elects its own leader and makes progress.
	c.WaitFor(10*time.Second, func() bool {
		id, _, ok := c.Leader()
		return ok && id != old
	})
	_, idx, _, ok := c.Propose([]byte("survivor"))
	if !ok {
		t.Fatal("majority side has no leader")
	}
	if !c.WaitFor(5*time.Second, func() bool {
		v, ok := rec(c, majority[0]).Get(idx)
		return ok && v == "survivor"
	}) {
		t.Fatal("majority side failed to commit")
	}
	time.Sleep(300 * time.Millisecond)
	if got := c.Node(old).Status().CommitIndex; got != before {
		t.Fatalf("minority leader's commit index moved %d -> %d", before, got)
	}
	for _, cmd := range rec(c, old).Commands() {
		if cmd == "doomed" || cmd == "survivor" {
			t.Fatalf("minority applied %q", cmd)
		}
	}
	noViolations(t, c)
}

// After a partition heals, every node ends up with the same log, and the
// uncommitted entries of the deposed leader are overwritten, never applied.
func TestLogAgreementAfterHeal(t *testing.T) {
	c := newCluster(t, 5, 13, 0)
	old, _ := mustLeader(t, c)
	commit(t, c, "base")
	buddy := (old + 1) % 5
	var majority []int
	for i := range 5 {
		if i != old && i != buddy {
			majority = append(majority, i)
		}
	}
	c.Sim.Partition([]int{old, buddy}, majority)
	for i := range 3 {
		c.Node(old).Propose([]byte(fmt.Sprintf("doomed-%d", i)))
	}
	c.WaitFor(10*time.Second, func() bool {
		id, _, ok := c.Leader()
		return ok && id != old
	})
	for i := range 5 {
		commit(t, c, fmt.Sprintf("kept-%d", i))
	}

	c.Sim.Heal()
	commit(t, c, "after-heal")
	converge(t, c)
	for i := range 5 {
		for _, cmd := range rec(c, i).Commands() {
			if len(cmd) >= 6 && cmd[:6] == "doomed" {
				t.Fatalf("node %d applied uncommitted entry %q", i, cmd)
			}
		}
	}
	if st := c.Node(old).Status(); st.Role != raft.Follower {
		t.Fatalf("old leader is still %v", st.Role)
	}
	noViolations(t, c)
}

// Whatever the network does, there is never more than one leader per term and
// all nodes apply the same commands in the same order.
func TestNoSplitBrainUnderRandomPartitions(t *testing.T) {
	for _, seed := range []int64{21, 22, 23} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Parallel()
			c := newCluster(t, 5, seed, 0)
			rng := rand.New(rand.NewSource(seed))
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() { // steady client load
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					c.Propose([]byte(fmt.Sprintf("p%d-%d", seed, i)))
					time.Sleep(10 * time.Millisecond)
				}
			}()
			end := time.Now().Add(5 * time.Second)
			for time.Now().Before(end) {
				switch rng.Intn(4) {
				case 0:
					c.Sim.Heal()
				case 1: // depose the leader by cutting it off
					if l, _, ok := c.Leader(); ok {
						c.Sim.Partition([]int{l})
					}
				default:
					c.Sim.Partition(randomPartition(rng, 5)...)
				}
				time.Sleep(time.Duration(300+rng.Intn(350)) * time.Millisecond)
			}
			close(stop)
			wg.Wait()
			c.Sim.Heal()
			mustLeader(t, c)
			commit(t, c, "final")
			converge(t, c)
			noViolations(t, c)
			t.Logf("seed %d: %d distinct leader terms, %d commands applied", seed, c.LeaderTerms(), len(rec(c, 0).Commands()))
		})
	}
}

// Committed entries survive a power cut: every node dies, comes back from its
// durable state alone, and the group carries on with the same history.
func TestPersistenceAcrossFullRestart(t *testing.T) {
	c := newCluster(t, 3, 14, 0)
	_, term := mustLeader(t, c)
	var want []string
	for i := range 10 {
		cmd := fmt.Sprintf("durable-%d", i)
		commit(t, c, cmd)
		want = append(want, cmd)
	}
	for i := range 3 {
		c.Sim.Crash(i)
	}
	for i := range 3 {
		c.Sim.Restart(i)
	}
	_, term2 := mustLeader(t, c)
	if term2 <= term {
		t.Fatalf("term went backwards or stood still across restart: %d -> %d", term, term2)
	}
	commit(t, c, "post-restart")
	converge(t, c)
	got := rec(c, 0).Commands()
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("command %d after restart = %q, want %q", i, got[i], w)
		}
	}
	noViolations(t, c)
}

// Rolling restarts of one node at a time never lose acknowledged commands.
func TestPersistenceRollingRestarts(t *testing.T) {
	c := newCluster(t, 5, 15, 0)
	mustLeader(t, c)
	var want []string
	for round := range 5 {
		cmd := fmt.Sprintf("round-%d", round)
		commit(t, c, cmd)
		want = append(want, cmd)
		c.Sim.Crash(round)
		c.WaitLeader(10 * time.Second)
		cmd = fmt.Sprintf("during-%d", round)
		commit(t, c, cmd)
		want = append(want, cmd)
		c.Sim.Restart(round)
	}
	converge(t, c)
	have := map[string]bool{}
	for _, cmd := range rec(c, 0).Commands() {
		have[cmd] = true
	}
	for _, w := range want {
		if !have[w] {
			t.Fatalf("acknowledged command %q was lost", w)
		}
	}
	noViolations(t, c)
}
