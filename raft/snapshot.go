package raft

import "time"

// Snapshot tells the node that the application has captured, in data, the
// effect of every committed entry through index, so the log prefix up to and
// including index can be discarded. index must not exceed the last entry the
// application has received from ApplyCh; older or repeated calls are ignored.
//
// data is retained and later shipped to lagging followers, so the caller must
// not modify it afterwards.
func (n *Node) Snapshot(index uint64, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || index <= n.log.snapIndex || index > n.lastApplied {
		return
	}
	term, ok := n.log.term(index)
	if !ok {
		return
	}
	n.log.compactTo(index, term)
	n.snapshot = data
	if n.mustPersistLocked() {
		n.emitLocked(EventSnapshot, "compact", index)
	}
}

// HandleInstallSnapshot implements the InstallSnapshot RPC.
func (n *Node) HandleInstallSnapshot(a *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &InstallSnapshotReply{Term: n.term}
	if n.stopped || a.Term < n.term {
		return reply
	}
	if a.Term > n.term || n.role != Follower {
		if !n.becomeFollowerLocked(a.Term, a.LeaderID) {
			return reply
		}
	}
	n.leaderID = a.LeaderID
	n.lastContact = time.Now()
	n.resetDeadlineLocked()
	reply.Term = n.term

	if a.LastIncludedIndex <= n.commitIndex {
		return reply // we already have everything this snapshot says
	}
	// If our log has the snapshot's last entry we keep the suffix after it;
	// otherwise (or if it conflicts) the whole log is superseded.
	if t, ok := n.log.term(a.LastIncludedIndex); ok && t == a.LastIncludedTerm {
		n.log.compactTo(a.LastIncludedIndex, a.LastIncludedTerm)
	} else {
		n.log.snapIndex, n.log.snapTerm, n.log.entries = a.LastIncludedIndex, a.LastIncludedTerm, nil
	}
	n.snapshot = a.Data
	if !n.mustPersistLocked() {
		return reply
	}
	n.pendingSnap = &ApplyMsg{
		SnapshotValid: true, Snapshot: a.Data,
		SnapshotIndex: a.LastIncludedIndex, SnapshotTerm: a.LastIncludedTerm,
	}
	n.setCommitLocked(a.LastIncludedIndex)
	n.emitLocked(EventSnapshot, "install", a.LastIncludedIndex)
	return reply
}

func (n *Node) sendSnapshot(peer int, term uint64, a *InstallSnapshotArgs) bool {
	reply, err := n.tr.InstallSnapshot(peer, a)
	if err != nil || reply == nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return false
	}
	if reply.Term > n.term {
		n.becomeFollowerLocked(reply.Term, NoVote)
		return false
	}
	if n.role != Leader || n.term != term {
		return false
	}
	if a.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = a.LastIncludedIndex
	}
	if a.LastIncludedIndex+1 > n.nextIndex[peer] {
		n.nextIndex[peer] = a.LastIncludedIndex + 1
	}
	n.advanceCommitLocked()
	return n.nextIndex[peer] <= n.log.lastIndex()
}

// applier is the single goroutine that feeds ApplyCh, which is what makes
// delivery order equal log order. A pending snapshot always goes first.
// lastApplied is advanced before the send, and the lock is dropped for the
// send itself, so a slow consumer never blocks RPC handling.
func (n *Node) applier() {
	defer n.wg.Done()
	defer close(n.applyC)
	for {
		n.mu.Lock()
		for !n.stopped && n.pendingSnap == nil && n.lastApplied >= n.commitIndex {
			n.cond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		var msg ApplyMsg
		if n.pendingSnap != nil {
			msg = *n.pendingSnap
			n.pendingSnap = nil
			n.lastApplied = msg.SnapshotIndex
		} else {
			idx := n.lastApplied + 1
			e := n.log.entry(idx)
			n.lastApplied = idx
			msg = ApplyMsg{
				CommandValid: true, Command: e.Command, Noop: e.Noop,
				CommandIndex: idx, CommandTerm: e.Term,
			}
		}
		n.mu.Unlock()

		select {
		case n.applyC <- msg:
		case <-n.stopCh:
			return
		}
	}
}
