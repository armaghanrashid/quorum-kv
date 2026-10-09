package transport_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/armaghanrashid/quorum-kv/internal/cluster"
	"github.com/armaghanrashid/quorum-kv/raft"
	"github.com/armaghanrashid/quorum-kv/transport"
)

type member struct {
	node *raft.Node
	rec  *cluster.Recorder
	srv  *http.Server
}

func startMember(t *testing.T, id int, addr string, peers map[int]string, dir string) *member {
	t.Helper()
	store, err := raft.NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	n := raft.New(id, []int{0, 1, 2}, transport.NewHTTP(peers, 300*time.Millisecond), store,
		raft.WithElectionTimeout(150*time.Millisecond, 300*time.Millisecond),
		raft.WithHeartbeat(30*time.Millisecond), raft.WithSeed(int64(id)+10))
	m := &member{node: n, rec: cluster.NewRecorder(id, n, 0), srv: &http.Server{Handler: transport.NewHTTPHandler(n)}}
	go m.srv.Serve(ln)
	return m
}

func (m *member) stop() {
	m.rec.Stop()
	m.node.Stop()
	m.srv.Shutdown(context.Background())
}

// The same Node that runs on the simulator also works over real sockets and a
// real on-disk store: elect, replicate, restart one member from its files.
func TestHTTPClusterWithFileStorage(t *testing.T) {
	addrs := make([]string, 3)
	lns := make([]net.Listener, 3)
	peers := map[int]string{}
	for i := range lns {
		ln, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i], addrs[i] = ln, ln.Addr().String()
		peers[i] = "http://" + addrs[i]
	}
	for _, ln := range lns { // reserve ports, then hand them to startMember
		ln.Close()
	}
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	ms := make([]*member, 3)
	for i := range ms {
		ms[i] = startMember(t, i, addrs[i], peers, dirs[i])
	}
	defer func() {
		for _, m := range ms {
			if m != nil {
				m.stop()
			}
		}
	}()

	leader := -1
	deadline := time.Now().Add(10 * time.Second)
	for leader < 0 && time.Now().Before(deadline) {
		for i, m := range ms {
			if m.node.Status().Role == raft.Leader {
				leader = i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader < 0 {
		t.Fatal("no leader over HTTP")
	}
	var last uint64
	for i := range 5 {
		idx, _, ok := ms[leader].node.Propose([]byte(fmt.Sprintf("http-%d", i)))
		if !ok {
			t.Fatal("leader refused proposal")
		}
		last = idx
	}
	waitApplied := func(m *member) {
		t.Helper()
		limit := time.Now().Add(15 * time.Second)
		for m.rec.Last() < last {
			if time.Now().After(limit) {
				t.Fatalf("member stuck at %d, want %d", m.rec.Last(), last)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, m := range ms {
		waitApplied(m)
	}

	// Restart a follower from its on-disk state; it must rejoin and agree.
	f := (leader + 1) % 3
	term := ms[f].node.Status().Term
	ms[f].stop()
	ms[f] = startMember(t, f, addrs[f], peers, dirs[f])
	if got := ms[f].node.Status().Term; got < term {
		t.Fatalf("term regressed across restart: %d -> %d", term, got)
	}
	waitApplied(ms[f])
	if err := cluster.CheckAgreement(ms[0].rec, ms[1].rec, ms[2].rec); err != nil {
		t.Fatal(err)
	}
}
