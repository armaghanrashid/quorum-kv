package raft_test

import (
	"fmt"
	"testing"
	"time"
)

// A follower that falls so far behind that the leader has already compacted
// the entries it needs must be brought up to date with InstallSnapshot.
func TestSnapshotInstallOnLaggingFollower(t *testing.T) {
	c := newCluster(t, 3, 30, 10)
	leader, _ := mustLeader(t, c)
	lag := (leader + 1) % 3
	var rest []int
	for i := range 3 {
		if i != lag {
			rest = append(rest, i)
		}
	}
	c.Sim.Partition([]int{lag}, rest)

	var last uint64
	for i := range 45 {
		last = commit(t, c, fmt.Sprintf("s-%d", i))
	}
	l, _, _ := c.Leader()
	if si := c.Node(l).Status().SnapshotIndex; si == 0 {
		t.Fatal("leader never compacted its log")
	}
	if installed := rec(c, lag).Installed(); installed != 0 {
		t.Fatalf("isolated node somehow installed %d snapshots", installed)
	}

	c.Sim.Heal()
	if !c.WaitFor(15*time.Second, func() bool { return rec(c, lag).Last() >= last }) {
		t.Fatalf("lagging follower stuck at %d, want >= %d", rec(c, lag).Last(), last)
	}
	if rec(c, lag).Installed() == 0 {
		t.Fatal("follower caught up without a snapshot; log was not compacted past it")
	}
	if si := c.Node(lag).Status().SnapshotIndex; si == 0 {
		t.Fatal("follower did not adopt the snapshot index")
	}
	converge(t, c)
	noViolations(t, c)
}

// A node that restarts after compacting rebuilds its application state from
// its own snapshot plus the log suffix.
func TestRestartFromSnapshot(t *testing.T) {
	c := newCluster(t, 3, 31, 8)
	mustLeader(t, c)
	for i := range 30 {
		commit(t, c, fmt.Sprintf("r-%d", i))
	}
	victim := 0
	if l, _, _ := c.Leader(); l == 0 {
		victim = 1
	}
	if c.Node(victim).Status().SnapshotIndex == 0 {
		t.Fatal("victim had not compacted yet")
	}
	c.Sim.Crash(victim)
	commit(t, c, "while-down")
	c.Sim.Restart(victim)
	converge(t, c)
	if got := len(rec(c, victim).Commands()); got < 31 {
		t.Fatalf("restarted node has only %d commands", got)
	}
	noViolations(t, c)
}
