package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/armaghanrashid/quorum-kv/internal/cluster"
	"github.com/armaghanrashid/quorum-kv/kv"
	"github.com/armaghanrashid/quorum-kv/raft"
)

var testOpts = []raft.Option{
	raft.WithElectionTimeout(150*time.Millisecond, 300*time.Millisecond),
	raft.WithHeartbeat(30 * time.Millisecond),
}

func newKV(t *testing.T, n int, seed int64, snapEvery uint64) *cluster.Cluster {
	t.Helper()
	f := func(id int, nd *raft.Node) cluster.App {
		return kv.NewServer(nd, kv.WithSnapshotEvery(snapEvery))
	}
	c := cluster.New(n, seed, f, testOpts...)
	t.Cleanup(c.Close)
	return c
}

func server(c *cluster.Cluster, i int) *kv.Server {
	if a := c.App(i); a != nil {
		return a.(*kv.Server)
	}
	return nil
}

// endpoints returns one stable Endpoint per node that always resolves to the
// node's current server, so clerks keep working across crash and restart.
func endpoints(c *cluster.Cluster) []kv.Endpoint {
	eps := make([]kv.Endpoint, c.N)
	for i := range eps {
		eps[i] = kv.EndpointFunc(func(ctx context.Context, r kv.Request) (kv.Response, error) {
			s := server(c, i)
			if s == nil {
				return kv.Response{}, kv.ErrStopped
			}
			return s.Do(ctx, r)
		})
	}
	return eps
}

func ctx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return c
}
