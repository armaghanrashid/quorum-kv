// Command demo runs a five-node cluster on the simulated network and walks
// through the life of a Raft group: election, replicated writes, a leader
// crash and re-election, a network partition and its healing. It prints a
// timeline as it happens and can write the same events to a CSV file, which
// tools/render.py turns into the animation in docs/media.
//
//	go run ./cmd/demo [--no-color] [--seed 7] [--pace 600ms] [--csv events.csv]
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/armaghanrashid/quorum-kv/internal/cluster"
	"github.com/armaghanrashid/quorum-kv/kv"
	"github.com/armaghanrashid/quorum-kv/raft"
)

const nodes = 5

// row is one timeline event, destined for the terminal and the CSV.
type row struct {
	at     time.Time
	node   int // -1 for cluster-wide
	kind   string
	term   uint64
	index  uint64
	detail string
	show   bool // print to the terminal
}

type palette struct{ on bool }

func (p palette) c(code, s string) string {
	if !p.on {
		return s
	}
	return "\x1b[" + code + "m" + s + "\x1b[0m"
}

var nodeColors = []string{"38;5;75", "38;5;179", "38;5;141", "38;5;114", "38;5;210"}

func main() {
	noColor := flag.Bool("no-color", false, "disable ANSI colours")
	seed := flag.Int64("seed", 7, "seed for the simulated network and election timers")
	pace := flag.Duration("pace", 600*time.Millisecond, "pause between scenes so the timeline is readable")
	csvPath := flag.String("csv", "", "also write the events to this CSV file")
	flag.Parse()

	pal := palette{on: !*noColor && os.Getenv("NO_COLOR") == ""}
	d := newDemo(*seed, pal)
	d.run(*pace)
	if *csvPath != "" {
		if err := d.writeCSV(*csvPath); err != nil {
			fmt.Fprintln(os.Stderr, "demo:", err)
			os.Exit(1)
		}
	}
}

type demo struct {
	pal   palette
	c     *cluster.Cluster
	start time.Time

	events chan row
	done   chan struct{}
	mu     sync.Mutex
	rows   []row
	leader int // best known leader, for filtering commit lines
	ldTerm uint64
}

func newDemo(seed int64, pal palette) *demo {
	d := &demo{pal: pal, events: make(chan row, 4096), done: make(chan struct{}), leader: -1}
	d.start = time.Now()
	f := func(id int, n *raft.Node) cluster.App { return kv.NewServer(n, kv.WithSnapshotEvery(40)) }
	d.c = cluster.New(nodes, seed, f,
		raft.WithElectionTimeout(150*time.Millisecond, 300*time.Millisecond),
		raft.WithHeartbeat(30*time.Millisecond))
	d.c.SetObserver(d.observe)
	go d.printer()
	return d
}

// observe runs under a node's lock, so it only enqueues.
func (d *demo) observe(ev raft.Event) {
	r := row{at: ev.Time, node: ev.Node, term: ev.Term}
	switch ev.Kind {
	case raft.EventRole:
		r.kind, r.detail = "role", ev.Role.String()
	case raft.EventCommit:
		r.kind, r.index = "commit", ev.Index
	case raft.EventSnapshot:
		r.kind, r.index, r.detail = "snapshot", ev.Index, ev.Detail
	default:
		return
	}
	d.events <- r
}

func (d *demo) note(node int, kind, detail string, show bool) {
	d.events <- row{at: time.Now(), node: node, kind: kind, detail: detail, show: show}
}

// printer serialises output and decides which events are worth a terminal
// line. Followers' commit events are kept for the CSV but not printed.
func (d *demo) printer() {
	defer close(d.done)
	fmt.Printf("\n  %s\n", d.pal.c("1", "quorum-kv demo: 5-node Raft cluster on a simulated network"))
	fmt.Printf("  %s\n\n", d.pal.c("2", fmt.Sprintf("%-9s %-5s %-18s %s", "TIME", "NODE", "EVENT", "DETAIL")))
	for r := range d.events {
		if r.kind == "role" && r.detail == "leader" && r.term >= d.ldTerm {
			d.leader, d.ldTerm = r.node, r.term
		}
		r.show = r.show || d.worthPrinting(r)
		d.mu.Lock()
		d.rows = append(d.rows, r)
		d.mu.Unlock()
		if r.show {
			d.print(r)
		}
	}
}

func (d *demo) worthPrinting(r row) bool {
	switch r.kind {
	case "role":
		return true
	case "commit":
		return r.node == d.leader
	case "snapshot":
		return r.node == d.leader
	}
	return false
}

func (d *demo) print(r row) {
	t := fmt.Sprintf("%7.3fs", r.at.Sub(d.start).Seconds())
	node := "--"
	if r.node >= 0 {
		node = fmt.Sprintf("n%d", r.node)
	}
	event, detail, code := "", r.detail, ""
	switch r.kind {
	case "role":
		switch r.detail {
		case "precandidate":
			event, detail, code = "timeout", fmt.Sprintf("no leader heard; asking for PreVotes (term stays %d)", r.term), "2"
		case "candidate":
			event, detail, code = "CANDIDATE", fmt.Sprintf("term %d, requesting votes", r.term), "38;5;179"
		case "leader":
			event, detail, code = "LEADER", fmt.Sprintf("term %d, won the election", r.term), "1;38;5;114"
		default:
			event, detail, code = "follower", fmt.Sprintf("term %d", r.term), "2"
		}
	case "commit":
		event, detail, code = "commit", fmt.Sprintf("index %d replicated on a majority", r.index), "38;5;80"
	case "snapshot":
		event, detail, code = "snapshot", fmt.Sprintf("log compacted through index %d", r.index), "2"
	case "start":
		event, code = "cluster", "1"
	case "crash":
		event, code = "CRASH", "1;38;5;203"
	case "restart":
		event, code = "RESTART", "38;5;179"
	case "partition":
		event, code = "PARTITION", "1;38;5;177"
	case "heal":
		event, code = "HEAL", "1;38;5;177"
	case "client":
		event, code = "client", "38;5;252"
	case "summary":
		event, code = "summary", "1"
	}
	nodeCol := "2"
	if r.node >= 0 {
		nodeCol = nodeColors[r.node%len(nodeColors)]
	}
	fmt.Printf("  %s %s %s %s\n",
		d.pal.c("2", t),
		d.pal.c(nodeCol, fmt.Sprintf("%-5s", node)),
		d.pal.c(code, fmt.Sprintf("%-18s", event)),
		detail)
}

func (d *demo) endpoints() []kv.Endpoint {
	eps := make([]kv.Endpoint, nodes)
	for i := range eps {
		eps[i] = kv.EndpointFunc(func(ctx context.Context, r kv.Request) (kv.Response, error) {
			s, _ := d.c.App(i).(*kv.Server)
			if s == nil {
				return kv.Response{}, kv.ErrStopped
			}
			return s.Do(ctx, r)
		})
	}
	return eps
}

func (d *demo) run(pace time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	d.note(-1, "start", fmt.Sprintf("%d nodes up; every message delayed 1-3 ms", nodes), true)

	ck := kv.NewClerk(1, d.endpoints())
	do := func(desc string, f func() (string, error)) {
		out, err := f()
		if err != nil {
			d.note(-1, "client", desc+" -> "+err.Error(), true)
			return
		}
		d.note(-1, "client", desc+" -> "+out, true)
	}
	put := func(k, v string) {
		do(fmt.Sprintf("Put(%s, %q)", k, v), func() (string, error) { return "ok", ck.Put(ctx, k, v) })
	}
	app := func(k, v string) {
		do(fmt.Sprintf("Append(%s, %q)", k, v), func() (string, error) { return "ok", ck.Append(ctx, k, v) })
	}
	get := func(k string) {
		do(fmt.Sprintf("Get(%s)", k), func() (string, error) {
			v, err := ck.Get(ctx, k)
			return strconv.Quote(v), err
		})
	}

	// Scene 1: election and first writes.
	lead, term, _ := d.c.WaitLeader(10 * time.Second)
	time.Sleep(pace)
	put("city", "montreal")
	app("city", "-qc")
	get("city")
	time.Sleep(pace)

	// Scene 2: kill the leader.
	d.note(lead, "crash", fmt.Sprintf("the leader (term %d) is killed", term), true)
	d.c.Sim.Crash(lead)
	d.c.WaitLeader(10 * time.Second)
	put("user", "ada")
	get("city")
	time.Sleep(pace)

	// Scene 3: the old leader returns as a follower and catches up.
	d.note(lead, "restart", "recovers term and log from durable storage", true)
	d.c.Sim.Restart(lead)
	app("user", "-lovelace")
	time.Sleep(pace)

	// Scene 4: partition two followers away; the majority keeps committing.
	cur, _, _ := d.c.Leader()
	var minority, majority []int
	for i := range nodes {
		if i != cur && len(minority) < 2 {
			minority = append(minority, i)
		} else {
			majority = append(majority, i)
		}
	}
	d.note(-1, "partition", fmt.Sprintf("%s | %s", ids(majority), ids(minority)), true)
	d.c.Sim.Partition(majority, minority)
	put("during", "partition")
	app("city", "!")
	time.Sleep(pace)

	// Scene 5: heal; the lagging nodes catch up without disturbing the leader.
	d.note(-1, "heal", "all links restored", true)
	d.c.Sim.Heal()
	put("after", "heal")
	d.c.WaitFor(10*time.Second, d.allApplied)
	time.Sleep(pace / 2)
	get("city")
	get("user")

	// Summary.
	agree := d.c.WaitFor(10*time.Second, d.allApplied)
	st := d.c.Node(0).Status()
	sent, lost := d.c.Sim.Stats()
	viol := d.c.Violations()
	d.note(-1, "summary", fmt.Sprintf("%d nodes applied through index %d, identical: %v | at most one leader per term: %v | %d leader terms | %d messages, %d lost to faults",
		nodes, st.LastApplied, agree, len(viol) == 0, d.c.LeaderTerms(), sent, lost), true)
	time.Sleep(50 * time.Millisecond)
	d.c.Close()
	close(d.events)
	<-d.done
	fmt.Println()
}

// allApplied reports whether every node has applied the same log prefix.
func (d *demo) allApplied() bool {
	var max uint64
	for i := range nodes {
		if l := d.c.Node(i).Status().LastApplied; l > max {
			max = l
		}
	}
	for i := range nodes {
		if d.c.Node(i).Status().LastApplied != max {
			return false
		}
	}
	return true
}

func ids(xs []int) string {
	s := make([]string, len(xs))
	for i, x := range xs {
		s[i] = strconv.Itoa(x)
	}
	return strings.Join(s, ",")
}

// writeCSV dumps every event, sorted by time, for tools/render.py.
func (d *demo) writeCSV(path string) error {
	d.mu.Lock()
	rows := append([]row(nil), d.rows...)
	d.mu.Unlock()
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].at.Before(rows[j].at) })
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"t_ms", "node", "kind", "term", "index", "detail"})
	for _, r := range rows {
		w.Write([]string{
			strconv.FormatInt(r.at.Sub(d.start).Milliseconds(), 10),
			strconv.Itoa(r.node), r.kind,
			strconv.FormatUint(r.term, 10), strconv.FormatUint(r.index, 10), r.detail,
		})
	}
	w.Flush()
	return w.Error()
}
