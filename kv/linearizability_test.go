package kv_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/armaghanrashid/quorum-kv/kv"
)

type opIn struct {
	Type  kv.OpType
	Key   string
	Value string
}

type opOut struct{ Value string }

// kvModel is the sequential specification of the store: one string register
// per key, where Put overwrites, Append concatenates and Get must return the
// current contents. Partitioning by key lets the checker treat each register
// independently, which is both sound (keys do not interact) and much faster.
var kvModel = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		by := map[string][]porcupine.Operation{}
		for _, op := range h {
			k := op.Input.(opIn).Key
			by[k] = append(by[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(by))
		for _, ops := range by {
			out = append(out, ops)
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		s, in, out := state.(string), input.(opIn), output.(opOut)
		switch in.Type {
		case kv.OpGet:
			return out.Value == s, s
		case kv.OpPut:
			return true, in.Value
		default:
			return true, s + in.Value
		}
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
}

// The checker itself must have teeth: a history where a read returns a value
// that was overwritten before the read began is not linearizable.
func TestCheckerRejectsStaleRead(t *testing.T) {
	put := func(v string, call, ret int64) porcupine.Operation {
		return porcupine.Operation{Input: opIn{kv.OpPut, "k", v}, Output: opOut{}, Call: call, Return: ret}
	}
	get := func(v string, call, ret int64) porcupine.Operation {
		return porcupine.Operation{Input: opIn{Type: kv.OpGet, Key: "k"}, Output: opOut{v}, Call: call, Return: ret}
	}
	good := []porcupine.Operation{put("a", 0, 10), put("b", 20, 30), get("b", 40, 50)}
	if !porcupine.CheckOperations(kvModel, good) {
		t.Fatal("checker rejected a linearizable history")
	}
	stale := []porcupine.Operation{put("a", 0, 10), put("b", 20, 30), get("a", 40, 50)}
	if porcupine.CheckOperations(kvModel, stale) {
		t.Fatal("checker accepted a stale read")
	}
	concurrent := []porcupine.Operation{put("a", 0, 10), put("b", 20, 60), get("a", 40, 50)}
	if !porcupine.CheckOperations(kvModel, concurrent) {
		t.Fatal("checker rejected a read concurrent with the overwriting Put")
	}
}

// TestLinearizabilityUnderFaults drives five concurrent clients against a
// five-node cluster while the network drops 20% of all messages, partitions
// shuffle every few hundred milliseconds, and nodes are crashed and restarted.
// Every completed operation is recorded with wall-clock call and return
// times; porcupine then searches for a legal sequential ordering consistent
// with those real-time bounds. If none exists, the store returned something
// no correct register could have.
func TestLinearizabilityUnderFaults(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			runLinearizability(t, seed)
		})
	}
}

func runLinearizability(t *testing.T, seed int64) {
	const (
		nodes   = 5
		clients = 5
		runFor  = 5 * time.Second
	)
	keys := []string{"a", "b", "c", "d"}
	c := newKV(t, nodes, seed, 25) // small snapshot interval: compaction happens mid-run
	if _, _, ok := c.WaitLeader(10 * time.Second); !ok {
		t.Fatal("no initial leader")
	}
	c.Sim.SetDropRate(0.20)

	var (
		mu      sync.Mutex
		history []porcupine.Operation
		start   = time.Now()
		stop    = make(chan struct{})
		wg      sync.WaitGroup
		failed  = make(chan string, clients)
	)
	for id := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed*100 + int64(id)))
			ck := kv.NewClerk(int64(id+1), endpoints(c))
			ck.SetAttemptTimeout(400 * time.Millisecond)
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				// These clients have no leader affinity (think of a load
				// balancer): every operation starts at a random replica, so a
				// replica that answers without consensus would be caught.
				ck.SetLeaderHint(rng.Intn(nodes))
				key := keys[rng.Intn(len(keys))]
				in := opIn{Key: key}
				switch r := rng.Intn(10); {
				case r < 4:
					in.Type = kv.OpGet
				case r < 7:
					in.Type, in.Value = kv.OpPut, fmt.Sprintf("<%d.%d>", id, n)
				default:
					in.Type, in.Value = kv.OpAppend, fmt.Sprintf("<%d.%d>", id, n)
				}
				// Call time is taken before the first attempt, return time
				// after the last, so retries widen the interval, never narrow it.
				call := time.Since(start).Nanoseconds()
				var (
					out opOut
					err error
				)
				x := ctx(t, 60*time.Second)
				switch in.Type {
				case kv.OpGet:
					out.Value, err = ck.Get(x, in.Key)
				case kv.OpPut:
					err = ck.Put(x, in.Key, in.Value)
				case kv.OpAppend:
					err = ck.Append(x, in.Key, in.Value)
				}
				ret := time.Since(start).Nanoseconds()
				if err != nil {
					failed <- fmt.Sprintf("client %d op %d (%v %s): %v", id, n, in.Type, in.Key, err)
					return
				}
				mu.Lock()
				history = append(history, porcupine.Operation{ClientId: id, Input: in, Call: call, Output: out, Return: ret})
				mu.Unlock()
				time.Sleep(time.Duration(rng.Intn(15)) * time.Millisecond)
			}
		}()
	}

	// Nemesis: break things on a seeded schedule.
	rng := rand.New(rand.NewSource(seed))
	down, partitions, crashes := -1, 0, 0
	end := time.Now().Add(runFor)
	for time.Now().Before(end) {
		switch rng.Intn(6) {
		case 0:
			c.Sim.Heal()
		case 1:
			perm := rng.Perm(nodes)
			k := 1 + rng.Intn(nodes-1)
			c.Sim.Partition(perm[:k], perm[k:])
			partitions++
		case 2:
			if l, _, ok := c.Leader(); ok {
				c.Sim.Partition([]int{l})
				partitions++
			}
		case 3:
			if down < 0 {
				down = rng.Intn(nodes)
				if l, _, ok := c.Leader(); ok && rng.Intn(2) == 0 {
					down = l
				}
				c.Sim.Crash(down)
				crashes++
			}
		default:
			if down >= 0 {
				c.Sim.Restart(down)
				down = -1
			}
		}
		time.Sleep(time.Duration(250+rng.Intn(350)) * time.Millisecond)
	}

	// Stop injecting faults and let in-flight operations finish.
	close(stop)
	c.Sim.Heal()
	c.Sim.SetDropRate(0)
	if down >= 0 {
		c.Sim.Restart(down)
	}
	wg.Wait()
	select {
	case msg := <-failed:
		t.Fatalf("operation did not complete after faults healed: %s", msg)
	default:
	}

	gets := 0
	for _, op := range history {
		if op.Input.(opIn).Type == kv.OpGet {
			gets++
		}
	}
	if len(history) < 40 || gets < 10 {
		t.Fatalf("only %d operations (%d gets) completed; the test would prove nothing", len(history), gets)
	}
	sent, lost := c.Sim.Stats()
	t.Logf("seed %d: %d ops (%d gets), %d partitions, %d crashes, %d leader terms, %d/%d messages lost",
		seed, len(history), gets, partitions, crashes, c.LeaderTerms(), lost, sent)
	if v := c.Violations(); len(v) > 0 {
		t.Fatalf("election safety violated: %v", v)
	}
	if !porcupine.CheckOperations(kvModel, history) {
		t.Fatalf("history of %d operations is NOT linearizable (seed %d)", len(history), seed)
	}
}
