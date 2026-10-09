package raft

// Entry is one slot of the replicated log.
//
// Command bytes are treated as immutable once an Entry has been created: they
// are shared (not copied) between the log, the transport and the state machine.
type Entry struct {
	Term    uint64
	Command []byte
	// Noop marks the empty entry a new leader appends at the start of its term.
	// Committing it is what lets the leader commit entries from earlier terms
	// (Raft paper section 5.4.2) without waiting for client traffic.
	Noop bool
}

// raftLog is the in-memory log with a compacted prefix.
//
// Indexes are 1-based. Everything up to and including snapIndex has been
// folded into a snapshot; entries[i] holds the entry with index
// snapIndex+1+i. snapTerm is the term of the entry at snapIndex, which is all
// the log needs to remember about the discarded prefix for the AppendEntries
// consistency check.
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry
}

func newLog(snapIndex, snapTerm uint64, entries []Entry) *raftLog {
	return &raftLog{snapIndex: snapIndex, snapTerm: snapTerm, entries: append([]Entry(nil), entries...)}
}

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

// term returns the term of the entry at index i. ok is false if i is outside
// [snapIndex, lastIndex].
func (l *raftLog) term(i uint64) (term uint64, ok bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.lastIndex():
		return 0, false
	default:
		return l.entries[i-l.snapIndex-1].Term, true
	}
}

// entry returns the entry at index i, which must be in (snapIndex, lastIndex].
func (l *raftLog) entry(i uint64) Entry {
	if i <= l.snapIndex || i > l.lastIndex() {
		panic("raft: log index out of range")
	}
	return l.entries[i-l.snapIndex-1]
}

// slice returns a copy of the entries in [lo, hi). lo must be > snapIndex.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo <= l.snapIndex || hi > l.lastIndex()+1 || lo > hi {
		panic("raft: log slice out of range")
	}
	return append([]Entry(nil), l.entries[lo-l.snapIndex-1:hi-l.snapIndex-1]...)
}

func (l *raftLog) append(es ...Entry) { l.entries = append(l.entries, es...) }

// truncateFrom removes the entry at index i and everything after it.
func (l *raftLog) truncateFrom(i uint64) {
	if i <= l.snapIndex {
		panic("raft: truncating into the snapshot")
	}
	if i > l.lastIndex() {
		return
	}
	l.entries = l.entries[:i-l.snapIndex-1]
}

// compactTo discards everything up to and including index, recording term as
// the term of that entry. If index is inside the log the suffix is kept, which
// is the normal case when the application snapshots its own applied state; if
// it is beyond the log (a follower installing a leader's snapshot) the whole
// log is dropped. The suffix is copied so the discarded prefix can be freed.
func (l *raftLog) compactTo(index, term uint64) {
	if index <= l.snapIndex {
		return
	}
	var rest []Entry
	if index < l.lastIndex() {
		rest = append(rest, l.entries[index-l.snapIndex:]...)
	}
	l.snapIndex, l.snapTerm, l.entries = index, term, rest
}

// upToDate implements the election restriction (paper section 5.4.1): a
// candidate's log is at least as up to date as ours if its last term is
// higher, or the terms are equal and its log is at least as long.
func (l *raftLog) upToDate(candLastTerm, candLastIndex uint64) bool {
	t := l.lastTerm()
	if candLastTerm != t {
		return candLastTerm > t
	}
	return candLastIndex >= l.lastIndex()
}

// firstIndexOfTerm returns the lowest index in the log whose entry has the
// given term, starting the scan at hint and walking backwards. Used to build
// the conflict hint that lets a leader skip a whole term per round trip.
func (l *raftLog) firstIndexOfTerm(term, hint uint64) uint64 {
	i := hint
	for i > l.snapIndex+1 {
		if t, _ := l.term(i - 1); t != term {
			break
		}
		i--
	}
	return i
}

// lastIndexOfTerm returns the highest index holding an entry of the given
// term, or 0 if the term does not appear.
func (l *raftLog) lastIndexOfTerm(term uint64) uint64 {
	for i := l.lastIndex(); i > l.snapIndex; i-- {
		t, _ := l.term(i)
		if t == term {
			return i
		}
		if t < term {
			return 0
		}
	}
	return 0
}
