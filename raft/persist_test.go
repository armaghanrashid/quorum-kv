package raft

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func sampleState() State {
	return State{
		Term: 7, VotedFor: 2, SnapshotIndex: 4, SnapshotTerm: 3,
		Entries:  []Entry{{Term: 3, Command: []byte("a")}, {Term: 7, Noop: true}},
		Snapshot: []byte("snapshot-bytes"),
	}
}

func TestFileStorageRoundTrip(t *testing.T) {
	fs, err := NewFileStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := fs.Load(); ok || err != nil {
		t.Fatalf("empty store: ok=%v err=%v", ok, err)
	}
	want := sampleState()
	if err := fs.Save(want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := fs.Load()
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, want)
	}
	want.Term = 8
	if err := fs.Save(want); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := fs.Load(); got.Term != 8 {
		t.Fatal("second save not visible")
	}
	entries, _ := os.ReadDir(fs.dir)
	if len(entries) != 1 {
		t.Fatalf("temp files left behind: %v", entries)
	}
}

func TestFileStorageDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStorage(dir)
	if err := fs.Save(sampleState()); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, stateFile)
	b, _ := os.ReadFile(p)
	b[len(b)/2] ^= 0xff
	os.WriteFile(p, b, 0o644)
	if _, _, err := fs.Load(); err == nil {
		t.Fatal("corrupted state file was accepted")
	}
	os.WriteFile(p, b[:2], 0o644)
	if _, _, err := fs.Load(); err == nil {
		t.Fatal("truncated state file was accepted")
	}
}

func TestMemStorageIsolation(t *testing.T) {
	m := NewMemStorage()
	st := sampleState()
	m.Save(st)
	st.Entries[0].Term = 99 // caller mutates its slice after Save
	got, _, _ := m.Load()
	if got.Entries[0].Term != 3 {
		t.Fatal("storage aliased the caller's entries")
	}
	m.FailAfter(1)
	if err := m.Save(st); err != nil {
		t.Fatalf("first save after arming should succeed: %v", err)
	}
	if err := m.Save(st); err != ErrInjected {
		t.Fatalf("expected injected failure, got %v", err)
	}
}
