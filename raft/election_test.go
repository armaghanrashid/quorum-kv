package raft_test

import (
	"testing"
	"time"

	"github.com/armaghanrashid/quorum-kv/raft"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 5, 1, 0)
	id, term := mustLeader(t, c)

	// The leader must stay put: heartbeats should suppress every other
	// election for several timeout periods.
	time.Sleep(700 * time.Millisecond)
	if id2, term2, ok := c.Leader(); !ok || id2 != id || term2 != term {
		t.Fatalf("leadership moved from (%d,%d) to (%d,%d,%v)", id, term, id2, term2, ok)
	}
	leaders := 0
	for i := range c.N {
		st := c.Node(i).Status()
		if st.Term != term {
			t.Errorf("node %d term = %d, want %d", i, st.Term, term)
		}
		switch st.Role {
		case raft.Leader:
			leaders++
		case raft.Follower:
			if st.Leader != id {
				t.Errorf("node %d follows %d, want %d", i, st.Leader, id)
			}
		default:
			t.Errorf("node %d role = %v", i, st.Role)
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders, want 1", leaders)
	}
	noViolations(t, c)
}

func TestReElectionAfterLeaderCrash(t *testing.T) {
	c := newCluster(t, 5, 2, 0)
	old, oldTerm := mustLeader(t, c)
	commit(t, c, "before-crash")

	c.Sim.Crash(old)
	id, term, ok := c.WaitLeader(10 * time.Second)
	if !ok {
		t.Fatal("no new leader after crash")
	}
	if id == old || term <= oldTerm {
		t.Fatalf("new leader (%d,%d) is not a successor of (%d,%d)", id, term, old, oldTerm)
	}
	idx := commit(t, c, "after-crash")

	// The old leader returns as a follower and catches up.
	c.Sim.Restart(old)
	if !c.WaitFor(10*time.Second, func() bool {
		r := rec(c, old)
		v, ok := r.Get(idx)
		return ok && v == "after-crash" && c.Node(old).Status().Role == raft.Follower
	}) {
		t.Fatalf("restarted node did not rejoin: %+v", c.Node(old).Status())
	}
	converge(t, c)
	noViolations(t, c)
}

// A node that is cut off must not inflate its term, and so must not disturb
// the cluster when it comes back. This is the point of PreVote.
func TestPreVoteIsolatedNodeDoesNotDisrupt(t *testing.T) {
	c := newCluster(t, 3, 3, 0)
	leader, term := mustLeader(t, c)
	victim := (leader + 1) % 3

	c.Sim.Partition([]int{victim})
	time.Sleep(1500 * time.Millisecond) // many election timeouts
	if got := c.Node(victim).Status().Term; got != term {
		t.Fatalf("isolated node's term grew to %d (was %d)", got, term)
	}
	c.Sim.Heal()
	time.Sleep(700 * time.Millisecond)
	if id, tm, ok := c.Leader(); !ok || id != leader || tm != term {
		t.Fatalf("rejoin disturbed the cluster: leader (%d,%d,%v), want (%d,%d)", id, tm, ok, leader, term)
	}
	noViolations(t, c)
}

// The control experiment: with PreVote off the same scenario does disrupt.
func TestWithoutPreVoteIsolatedNodeInflatesTerm(t *testing.T) {
	c := newCluster(t, 3, 4, 0, raft.WithPreVote(false))
	leader, term := mustLeader(t, c)
	victim := (leader + 1) % 3

	c.Sim.Partition([]int{victim})
	time.Sleep(1500 * time.Millisecond)
	inflated := c.Node(victim).Status().Term
	if inflated <= term+1 {
		t.Fatalf("expected a runaway term, got %d (leader term %d)", inflated, term)
	}
	c.Sim.Heal()
	if !c.WaitFor(5*time.Second, func() bool {
		_, tm, ok := c.Leader()
		return ok && tm >= inflated
	}) {
		t.Fatalf("rejoining node's term never forced the cluster forward")
	}
	noViolations(t, c)
}

// A node must remember its vote across a crash, or two candidates could both
// win the same term.
func TestVoteSurvivesRestart(t *testing.T) {
	st := raft.NewMemStorage()
	n := quietNode(0, st)
	r := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: 1})
	if !r.Granted || r.Term != 5 {
		t.Fatalf("first vote: %+v", r)
	}
	n.Stop()

	n = quietNode(0, st) // same durable state, new process
	defer n.Stop()
	if r := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: 2}); r.Granted {
		t.Fatal("voted twice in term 5 after a restart")
	}
	if r := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: 1}); !r.Granted {
		t.Fatal("did not repeat the vote for the same candidate")
	}
	if got := n.Status().Term; got != 5 {
		t.Fatalf("term after restart = %d, want 5", got)
	}
	if r := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 6, CandidateID: 2}); !r.Granted {
		t.Fatal("refused a fresh term")
	}
}

// If the vote cannot be made durable it must not be granted, and the node must
// stop making promises it cannot keep.
func TestVoteNotGrantedWhenPersistFails(t *testing.T) {
	st := raft.NewMemStorage()
	n := quietNode(0, st)
	defer n.Stop()
	st.FailAfter(0)
	r := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 3, CandidateID: 1})
	if r.Granted {
		t.Fatal("granted a vote that was never persisted")
	}
	if n.HaltError() == nil {
		t.Fatal("node kept running after its storage failed")
	}
	if loaded, ok, _ := st.Load(); ok && loaded.VotedFor == 1 {
		t.Fatal("vote reached storage despite the failure")
	}
}

func TestStaleAndUpToDateVoteRules(t *testing.T) {
	st := raft.NewMemStorage()
	n := quietNode(0, st)
	defer n.Stop()
	// Give the node a log entry at term 2 via a leader's AppendEntries.
	ar := n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 2, LeaderID: 1, Entries: []raft.Entry{{Term: 2, Command: []byte("x")}},
	})
	if !ar.Success {
		t.Fatalf("append failed: %+v", ar)
	}
	if r := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: 2}); r.Granted || r.Term != 2 {
		t.Fatalf("granted a stale-term vote: %+v", r)
	}
	// Higher term but a log that is behind ours must be refused (election restriction)...
	if r := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 3, CandidateID: 2, LastLogIndex: 5, LastLogTerm: 1}); r.Granted {
		t.Fatal("voted for a candidate with an older last term")
	}
	// ...but still advances our term.
	if got := n.Status().Term; got != 3 {
		t.Fatalf("term = %d, want 3", got)
	}
	if r := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 3, CandidateID: 2, LastLogIndex: 1, LastLogTerm: 2}); !r.Granted {
		t.Fatal("refused a candidate whose log is as up to date as ours")
	}
}
