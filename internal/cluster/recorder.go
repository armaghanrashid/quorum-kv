package cluster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/armaghanrashid/quorum-kv/raft"
)

const noopMarker = "<noop>"

// Recorder is a trivial replicated state machine: it remembers the command
// applied at every log index. Tests compare Recorders across nodes to check
// that all of them applied the same commands in the same order. If snapEvery
// is non-zero it snapshots every snapEvery entries, so the same Recorder
// exercises log compaction and snapshot install.
type Recorder struct {
	id        int
	node      *raft.Node
	snapEvery uint64

	mu        sync.Mutex
	cmds      map[uint64]string
	last      uint64
	installed int

	done chan struct{}
	wg   sync.WaitGroup
}

type recSnap struct {
	Last uint64
	Cmds map[uint64]string
}

// RecorderFactory returns a Factory that attaches a Recorder to every node.
func RecorderFactory(snapEvery uint64) Factory {
	return func(id int, n *raft.Node) App { return NewRecorder(id, n, snapEvery) }
}

func NewRecorder(id int, n *raft.Node, snapEvery uint64) *Recorder {
	r := &Recorder{
		id: id, node: n, snapEvery: snapEvery,
		cmds: make(map[uint64]string), done: make(chan struct{}),
	}
	r.wg.Add(1)
	go r.loop()
	return r
}

func (r *Recorder) loop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.done:
			return
		case m, ok := <-r.node.ApplyCh():
			if !ok {
				return
			}
			r.apply(m)
		}
	}
}

func (r *Recorder) apply(m raft.ApplyMsg) {
	r.mu.Lock()
	if m.SnapshotValid {
		var s recSnap
		if err := gob.NewDecoder(bytes.NewReader(m.Snapshot)).Decode(&s); err != nil {
			r.mu.Unlock()
			panic(fmt.Sprintf("recorder %d: bad snapshot: %v", r.id, err))
		}
		r.cmds, r.last = s.Cmds, s.Last
		r.installed++
		r.mu.Unlock()
		return
	}
	if m.CommandIndex != r.last+1 {
		r.mu.Unlock()
		panic(fmt.Sprintf("recorder %d: applied index %d after %d", r.id, m.CommandIndex, r.last))
	}
	if m.Noop {
		r.cmds[m.CommandIndex] = noopMarker
	} else {
		r.cmds[m.CommandIndex] = string(m.Command)
	}
	r.last = m.CommandIndex
	var data []byte
	if r.snapEvery > 0 && r.last%r.snapEvery == 0 {
		var buf bytes.Buffer
		_ = gob.NewEncoder(&buf).Encode(recSnap{Last: r.last, Cmds: r.cmds})
		data = buf.Bytes()
	}
	idx := r.last
	r.mu.Unlock()
	if data != nil {
		r.node.Snapshot(idx, data)
	}
}

// Stop ends the consuming goroutine.
func (r *Recorder) Stop() {
	close(r.done)
	r.wg.Wait()
}

// Get returns the command applied at index.
func (r *Recorder) Get(index uint64) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.cmds[index]
	return s, ok
}

// Last is the highest index applied.
func (r *Recorder) Last() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// Installed counts snapshots received from a leader (not self-made ones).
func (r *Recorder) Installed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.installed
}

// Commands returns the applied commands in order, without no-ops.
func (r *Recorder) Commands() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for i := uint64(1); i <= r.last; i++ {
		if s := r.cmds[i]; s != noopMarker {
			out = append(out, s)
		}
	}
	return out
}

// copyCmds returns a consistent copy of the applied commands and last index.
func (r *Recorder) copyCmds() (map[uint64]string, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := make(map[uint64]string, len(r.cmds))
	for k, v := range r.cmds {
		m[k] = v
	}
	return m, r.last
}

// CheckAgreement verifies state machine safety: wherever two recorders both
// hold an index, they hold the same command. It returns the first mismatch.
func CheckAgreement(rs ...*Recorder) error {
	type snap struct {
		m map[uint64]string
		l uint64
	}
	ss := make([]snap, len(rs))
	for i, r := range rs {
		ss[i].m, ss[i].l = r.copyCmds()
	}
	for i := range ss {
		for j := i + 1; j < len(ss); j++ {
			for idx, a := range ss[i].m {
				if b, ok := ss[j].m[idx]; ok && a != b {
					return fmt.Errorf("index %d: recorder %d has %q, recorder %d has %q", idx, rs[i].id, a, rs[j].id, b)
				}
			}
		}
	}
	return nil
}
