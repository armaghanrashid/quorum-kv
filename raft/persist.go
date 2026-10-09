package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// NoVote is the VotedFor value meaning "no vote cast in the current term".
const NoVote = -1

// State is everything a Raft node must remember across a crash: the hard
// state (current term and vote), the log suffix that follows the snapshot, and
// the snapshot itself. commitIndex and lastApplied are deliberately absent;
// they are rebuilt from the leader after a restart.
type State struct {
	Term          uint64
	VotedFor      int
	SnapshotIndex uint64
	SnapshotTerm  uint64
	Entries       []Entry
	Snapshot      []byte
}

// Storage is the durable home of a node's State.
//
// Save must be atomic and durable when it returns: the node calls it before
// it answers any RPC whose reply depends on the new state (a granted vote, an
// acknowledged append). Save must not retain State.Entries after returning,
// because the node hands out its live log slice to avoid a copy.
type Storage interface {
	// Load returns the saved state. ok is false when nothing has been saved.
	Load() (st State, ok bool, err error)
	Save(st State) error
}

// MemStorage is an in-memory Storage. It survives a simulated crash because
// the cluster harness keeps it alive while it throws the Node away, which
// models a disk without needing one.
type MemStorage struct {
	mu    sync.Mutex
	st    State
	saved bool
	saves int
	// failAfter, when >= 0, makes the (failAfter+1)-th and later Saves fail.
	failAfter int
}

// NewMemStorage returns an empty MemStorage.
func NewMemStorage() *MemStorage { return &MemStorage{failAfter: -1} }

// ErrInjected is returned by a MemStorage whose failure has been armed.
var ErrInjected = errors.New("raft: injected storage failure")

// FailAfter arms the storage to fail every Save after n further successful
// ones. FailAfter(0) fails the very next Save. Pass a negative n to disarm.
func (m *MemStorage) FailAfter(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n < 0 {
		m.failAfter = -1
		return
	}
	m.failAfter = m.saves + n
}

// Saves reports how many successful Saves have happened.
func (m *MemStorage) Saves() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.saves
}

func (m *MemStorage) Load() (State, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.saved {
		return State{}, false, nil
	}
	return cloneState(m.st), true, nil
}

func (m *MemStorage) Save(st State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failAfter >= 0 && m.saves >= m.failAfter {
		return ErrInjected
	}
	m.st = cloneState(st)
	m.saved = true
	m.saves++
	return nil
}

func cloneState(st State) State {
	st.Entries = append([]Entry(nil), st.Entries...)
	st.Snapshot = append([]byte(nil), st.Snapshot...)
	return st
}

// FileStorage persists State to a single file using the classic
// write-temp, fsync, rename, fsync-directory sequence, so a reader (including
// the next incarnation of this process) sees either the old state or the new
// one and never a torn mixture. Each file carries a CRC so bit rot is
// detected rather than silently replayed.
//
// Rewriting the whole state on every Save is O(log size); a production system
// would append to a write-ahead log and compact it. See the README for why
// this trade-off was taken.
type FileStorage struct {
	dir string
}

const stateFile = "raft-state.bin"

// NewFileStorage uses dir, creating it if necessary.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir}, nil
}

func (f *FileStorage) Load() (State, bool, error) {
	data, err := os.ReadFile(filepath.Join(f.dir, stateFile))
	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	if len(data) < 4 {
		return State{}, false, fmt.Errorf("raft: state file truncated")
	}
	sum, payload := binary.BigEndian.Uint32(data[:4]), data[4:]
	if crc32.ChecksumIEEE(payload) != sum {
		return State{}, false, fmt.Errorf("raft: state file checksum mismatch")
	}
	var st State
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&st); err != nil {
		return State{}, false, fmt.Errorf("raft: decoding state: %w", err)
	}
	return st, true, nil
}

func (f *FileStorage) Save(st State) error {
	var buf bytes.Buffer
	buf.Write([]byte{0, 0, 0, 0})
	if err := gob.NewEncoder(&buf).Encode(&st); err != nil {
		return err
	}
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b[:4], crc32.ChecksumIEEE(b[4:]))

	tmp, err := os.CreateTemp(f.dir, "raft-state-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), filepath.Join(f.dir, stateFile)); err != nil {
		return err
	}
	d, err := os.Open(f.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// persistLocked writes the node's durable state. The caller holds n.mu and
// must not release it, or send any message that depends on the new state,
// until persistLocked has returned nil. That single rule is the whole
// persistence-ordering discipline: state change, then fsync, then reply.
func (n *Node) persistLocked() error {
	return n.store.Save(State{
		Term:          n.term,
		VotedFor:      n.votedFor,
		SnapshotIndex: n.log.snapIndex,
		SnapshotTerm:  n.log.snapTerm,
		Entries:       n.log.entries,
		Snapshot:      n.snapshot,
	})
}

// mustPersistLocked persists or, if the storage fails, halts the node. A node
// that cannot make its promises durable must stop making promises: carrying
// on would let it vote or acknowledge entries it might forget after a crash,
// which is exactly how Raft's safety properties get broken.
func (n *Node) mustPersistLocked() bool {
	if err := n.persistLocked(); err != nil {
		n.haltLocked(err)
		return false
	}
	return true
}
