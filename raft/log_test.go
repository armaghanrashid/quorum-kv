package raft

import "testing"

func mk(terms ...uint64) *raftLog {
	l := newLog(0, 0, nil)
	for _, t := range terms {
		l.append(Entry{Term: t})
	}
	return l
}

func TestLogIndexing(t *testing.T) {
	l := mk(1, 1, 2, 3)
	if l.lastIndex() != 4 || l.lastTerm() != 3 {
		t.Fatalf("last = (%d,%d)", l.lastIndex(), l.lastTerm())
	}
	if tm, ok := l.term(0); !ok || tm != 0 {
		t.Fatal("index 0 is the empty prefix")
	}
	if _, ok := l.term(5); ok {
		t.Fatal("term(5) should be out of range")
	}
	if got := l.slice(2, 4); len(got) != 2 || got[0].Term != 1 || got[1].Term != 2 {
		t.Fatalf("slice = %+v", got)
	}
}

func TestLogTruncate(t *testing.T) {
	l := mk(1, 1, 2, 3)
	l.truncateFrom(3)
	if l.lastIndex() != 2 || l.lastTerm() != 1 {
		t.Fatalf("after truncate last = (%d,%d)", l.lastIndex(), l.lastTerm())
	}
	l.truncateFrom(10) // beyond the end is a no-op
	if l.lastIndex() != 2 {
		t.Fatal("truncate beyond end changed the log")
	}
}

func TestLogCompaction(t *testing.T) {
	l := mk(1, 1, 2, 2, 3)
	l.compactTo(3, 2)
	if l.snapIndex != 3 || l.snapTerm != 2 || l.lastIndex() != 5 {
		t.Fatalf("after compact: snap=(%d,%d) last=%d", l.snapIndex, l.snapTerm, l.lastIndex())
	}
	if tm, ok := l.term(3); !ok || tm != 2 {
		t.Fatal("snapshot boundary term lost")
	}
	if _, ok := l.term(2); ok {
		t.Fatal("compacted index still addressable")
	}
	if l.entry(4).Term != 2 || l.entry(5).Term != 3 {
		t.Fatal("suffix entries shifted")
	}
	l.compactTo(2, 1) // older snapshot is ignored
	if l.snapIndex != 3 {
		t.Fatal("compaction went backwards")
	}
	l.compactTo(9, 4) // beyond the log: everything is replaced
	if l.snapIndex != 9 || l.lastIndex() != 9 || l.lastTerm() != 4 {
		t.Fatalf("full replace: snap=%d last=(%d,%d)", l.snapIndex, l.lastIndex(), l.lastTerm())
	}
}

func TestUpToDate(t *testing.T) {
	l := mk(1, 2, 2) // last = (index 3, term 2)
	cases := []struct {
		term, index uint64
		want        bool
	}{
		{3, 1, true},  // higher last term wins regardless of length
		{2, 3, true},  // equal
		{2, 9, true},  // same term, longer
		{2, 2, false}, // same term, shorter
		{1, 99, false} /* lower term loses even if longer */}
	for _, c := range cases {
		if got := l.upToDate(c.term, c.index); got != c.want {
			t.Errorf("upToDate(%d,%d) = %v, want %v", c.term, c.index, got, c.want)
		}
	}
}

func TestConflictHelpers(t *testing.T) {
	l := mk(1, 2, 2, 2, 3)
	if got := l.firstIndexOfTerm(2, 4); got != 2 {
		t.Fatalf("firstIndexOfTerm = %d, want 2", got)
	}
	if got := l.lastIndexOfTerm(2); got != 4 {
		t.Fatalf("lastIndexOfTerm(2) = %d, want 4", got)
	}
	if got := l.lastIndexOfTerm(9); got != 0 {
		t.Fatalf("lastIndexOfTerm(9) = %d, want 0", got)
	}
}
