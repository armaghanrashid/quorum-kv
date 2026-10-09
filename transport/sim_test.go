package transport

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/armaghanrashid/quorum-kv/raft"
)

// echo is a Handler that answers every vote request with its own id as the term.
type echo struct{ id int }

func (e echo) HandleRequestVote(*raft.RequestVoteArgs) *raft.RequestVoteReply {
	return &raft.RequestVoteReply{Term: uint64(e.id), Granted: true}
}
func (e echo) HandleAppendEntries(*raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	return &raft.AppendEntriesReply{Term: uint64(e.id)}
}
func (e echo) HandleInstallSnapshot(*raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	return &raft.InstallSnapshotReply{Term: uint64(e.id)}
}

func newEcho(n int) *Sim {
	s := NewSim(1)
	s.SetDelay(0, time.Millisecond)
	for i := range n {
		s.Register(i, echo{i})
	}
	return s
}

func call(s *Sim, from, to int) error {
	_, err := s.Endpoint(from).RequestVote(to, &raft.RequestVoteArgs{})
	return err
}

func TestSimPartitionAndHeal(t *testing.T) {
	s := newEcho(4)
	if err := call(s, 0, 3); err != nil {
		t.Fatalf("connected network failed: %v", err)
	}
	s.Partition([]int{0, 1}, []int{2}) // node 3 is unlisted: its own group
	for _, c := range [][2]int{{0, 2}, {2, 0}, {0, 3}, {3, 1}, {2, 3}} {
		if err := call(s, c[0], c[1]); !errors.Is(err, ErrUnreachable) {
			t.Fatalf("%v crossed the partition: %v", c, err)
		}
	}
	if err := call(s, 0, 1); err != nil {
		t.Fatalf("same-group call failed: %v", err)
	}
	s.Heal()
	if err := call(s, 2, 3); err != nil {
		t.Fatalf("call after heal failed: %v", err)
	}
}

func TestSimDropRate(t *testing.T) {
	s := newEcho(2)
	s.SetDropRate(0.3)
	ok := 0
	const n = 600
	var wg sync.WaitGroup
	var mu sync.Mutex
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if call(s, 0, 1) == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	// A call survives only if both legs do: 0.7*0.7 = 0.49 expected.
	if frac := float64(ok) / n; frac < 0.40 || frac > 0.58 {
		t.Fatalf("success fraction %.2f outside [0.40, 0.58]", frac)
	}
	s.SetDropRate(0)
	if err := call(s, 0, 1); err != nil {
		t.Fatal(err)
	}
}

func TestSimCrashRestartLifecycle(t *testing.T) {
	s := newEcho(3)
	var log []string
	s.SetLifecycle(
		func(id int) { log = append(log, "crash") },
		func(id int) {
			if !s.IsDown(id) {
				t.Error("restart callback ran while the node was still up")
			}
			log = append(log, "restart")
		})
	s.Crash(1)
	if !s.IsDown(1) {
		t.Fatal("not down after Crash")
	}
	if err := call(s, 0, 1); err == nil {
		t.Fatal("call to a crashed node succeeded")
	}
	if err := call(s, 1, 0); err == nil {
		t.Fatal("call from a crashed node succeeded")
	}
	s.Restart(1)
	if s.IsDown(1) {
		t.Fatal("still down after Restart")
	}
	if err := call(s, 0, 1); err != nil {
		t.Fatalf("call after restart failed: %v", err)
	}
	if len(log) != 2 || log[0] != "crash" || log[1] != "restart" {
		t.Fatalf("lifecycle log = %v", log)
	}
}
