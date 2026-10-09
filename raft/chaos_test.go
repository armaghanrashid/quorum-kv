package raft_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// Everything at once: message loss, partitions, crashes and restarts, a live
// workload and aggressive snapshotting. Afterwards the cluster must converge
// to a single history.
func TestChaos(t *testing.T) {
	for _, seed := range []int64{41, 42, 43} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Parallel()
			c := newCluster(t, 5, seed, 12)
			c.Sim.SetDropRate(0.15)
			rng := rand.New(rand.NewSource(seed))
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					c.Propose([]byte(fmt.Sprintf("c%d-%d", seed, i)))
					time.Sleep(8 * time.Millisecond)
				}
			}()
			down := -1
			end := time.Now().Add(6 * time.Second)
			for time.Now().Before(end) {
				switch rng.Intn(6) {
				case 0:
					c.Sim.Heal()
				case 1:
					c.Sim.Partition(randomPartition(rng, 5)...)
				case 2: // depose the leader by cutting it off
					if l, _, ok := c.Leader(); ok {
						c.Sim.Partition([]int{l})
					}
				case 3: // crash a node, the leader about half the time
					if down < 0 {
						down = rng.Intn(5)
						if l, _, ok := c.Leader(); ok && rng.Intn(2) == 0 {
							down = l
						}
						c.Sim.Crash(down)
					}
				default:
					if down >= 0 {
						c.Sim.Restart(down)
						down = -1
					}
				}
				time.Sleep(time.Duration(300+rng.Intn(350)) * time.Millisecond)
			}
			close(stop)
			wg.Wait()
			c.Sim.Heal()
			c.Sim.SetDropRate(0)
			if down >= 0 {
				c.Sim.Restart(down)
			}
			mustLeader(t, c)
			commit(t, c, "final")
			converge(t, c)
			noViolations(t, c)
			sent, lost := c.Sim.Stats()
			t.Logf("seed %d: %d leader terms, %d commands, %d/%d messages lost", seed, c.LeaderTerms(), len(rec(c, 0).Commands()), lost, sent)
		})
	}
}
