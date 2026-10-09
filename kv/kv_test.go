package kv_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/armaghanrashid/quorum-kv/kv"
)

func TestPutGetAppend(t *testing.T) {
	c := newKV(t, 3, 1, 0)
	if _, _, ok := c.WaitLeader(10 * time.Second); !ok {
		t.Fatal("no leader")
	}
	ck := kv.NewClerk(1, endpoints(c))
	x := ctx(t, 20*time.Second)

	if v, err := ck.Get(x, "missing"); err != nil || v != "" {
		t.Fatalf("Get(missing) = %q, %v", v, err)
	}
	if err := ck.Put(x, "k", "a"); err != nil {
		t.Fatal(err)
	}
	if err := ck.Append(x, "k", "b"); err != nil {
		t.Fatal(err)
	}
	if err := ck.Append(x, "k", "c"); err != nil {
		t.Fatal(err)
	}
	if v, _ := ck.Get(x, "k"); v != "abc" {
		t.Fatalf("Get(k) = %q, want abc", v)
	}
	if err := ck.Put(x, "k", "z"); err != nil {
		t.Fatal(err)
	}
	if v, _ := ck.Get(x, "k"); v != "z" {
		t.Fatalf("Get(k) after Put = %q, want z", v)
	}
}

func TestFollowerRefusesRequests(t *testing.T) {
	c := newKV(t, 3, 2, 0)
	leader, _, ok := c.WaitLeader(10 * time.Second)
	if !ok {
		t.Fatal("no leader")
	}
	f := server(c, (leader+1)%3)
	_, err := f.Do(ctx(t, 5*time.Second), kv.Request{Type: kv.OpPut, Key: "k", Value: "v", ClientID: 9, Seq: 1})
	if !errors.Is(err, kv.ErrNotLeader) {
		t.Fatalf("follower returned %v, want ErrNotLeader", err)
	}
}

// Retrying a request must not execute it twice. This is what makes it safe
// for a client to resend after a lost reply or a leader change.
func TestRetriesAreExactlyOnce(t *testing.T) {
	c := newKV(t, 3, 3, 0)
	leader, _, ok := c.WaitLeader(10 * time.Second)
	if !ok {
		t.Fatal("no leader")
	}
	x := ctx(t, 30*time.Second)
	s := server(c, leader)
	const cl = 77
	for i := 0; i < 3; i++ { // the same (client, seq) three times
		if err := s.Append(x, cl, 1, "k", "x"); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Append(x, cl, 2, "k", "y"); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(x, cl, 1, "k", "x"); err != nil { // stale replay of seq 1
		t.Fatal(err)
	}
	if v, err := s.Get(x, cl, 3, "k"); err != nil || v != "xy" {
		t.Fatalf("value = %q, %v; want xy", v, err)
	}
	// A retried Get returns its original answer, not a newer one.
	if err := s.Append(x, 78, 1, "k", "z"); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Get(x, cl, 3, "k"); v != "xy" {
		t.Fatalf("retried Get = %q, want its original answer xy", v)
	}

	// Same guarantee after the leader changes.
	c.Sim.Crash(leader)
	if _, _, ok := c.WaitLeader(10 * time.Second); !ok {
		t.Fatal("no new leader")
	}
	nl, _, _ := c.Leader()
	if err := server(c, nl).Append(x, 78, 1, "k", "z"); err != nil {
		t.Fatal(err)
	}
	if v, err := server(c, nl).Get(x, 79, 1, "k"); err != nil || v != "xyz" {
		t.Fatalf("after failover value = %q, %v; want xyz", v, err)
	}
}

// State machines rebuild from snapshot plus log after the whole cluster is
// power-cycled.
func TestSnapshotAndFullRestart(t *testing.T) {
	c := newKV(t, 3, 4, 10)
	if _, _, ok := c.WaitLeader(10 * time.Second); !ok {
		t.Fatal("no leader")
	}
	ck := kv.NewClerk(1, endpoints(c))
	x := ctx(t, 60*time.Second)
	for i := range 60 {
		if err := ck.Put(x, fmt.Sprintf("key%d", i%7), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	l, _, _ := c.Leader()
	if c.Node(l).Status().SnapshotIndex == 0 {
		t.Fatal("log was never compacted")
	}
	for i := range 3 {
		c.Sim.Crash(i)
	}
	for i := range 3 {
		c.Sim.Restart(i)
	}
	if _, _, ok := c.WaitLeader(10 * time.Second); !ok {
		t.Fatal("no leader after restart")
	}
	ck2 := kv.NewClerk(2, endpoints(c))
	for k := range 7 {
		want := ""
		for i := range 60 { // the last write to this key wins
			if i%7 == k {
				want = fmt.Sprintf("v%d", i)
			}
		}
		if v, err := ck2.Get(x, fmt.Sprintf("key%d", k)); err != nil || v != want {
			t.Fatalf("key%d = %q, %v; want %q", k, v, err, want)
		}
	}
}
