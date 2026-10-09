package raft

import "time"

// Propose asks the node to append cmd to the replicated log. If the node is
// not the leader it returns isLeader=false and nothing happens. If it is, the
// entry has been appended and persisted locally and replication has been
// triggered, but the entry is NOT yet committed: the caller learns that by
// seeing the command at the returned index on ApplyCh. A leader that loses
// office before committing may see a different command arrive at that index,
// so callers must compare, not assume.
func (n *Node) Propose(cmd []byte) (index, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.role != Leader {
		return 0, n.term, false
	}
	n.log.append(Entry{Term: n.term, Command: cmd})
	if !n.mustPersistLocked() {
		return 0, n.term, false
	}
	index = n.log.lastIndex()
	n.advanceCommitLocked() // a group of one commits immediately
	n.kickAllLocked()
	return index, n.term, true
}

func (n *Node) kickAllLocked() {
	for _, ch := range n.kick {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// replicator owns the connection from the leader to one follower for the
// duration of one term. It keeps at most one RPC in flight, which keeps replies
// ordered and gives natural back-pressure on slow or partitioned peers, and
// doubles as the heartbeat clock: after each exchange it sleeps for the
// heartbeat interval unless woken early by a new proposal.
func (n *Node) replicator(peer int, term uint64, kick <-chan struct{}) {
	defer n.wg.Done()
	for {
		n.mu.Lock()
		if n.stopped || n.role != Leader || n.term != term {
			n.mu.Unlock()
			return
		}
		ae, snap := n.buildReplicationLocked(peer)
		n.mu.Unlock()

		var again bool
		if snap != nil {
			again = n.sendSnapshot(peer, term, snap)
		} else {
			again = n.sendAppend(peer, term, ae)
		}
		if again {
			continue
		}
		t := time.NewTimer(n.cfg.HeartbeatInterval)
		select {
		case <-n.stopCh:
			t.Stop()
			return
		case <-kick:
		case <-t.C:
		}
		t.Stop()
	}
}

func (n *Node) buildReplicationLocked(peer int) (*AppendEntriesArgs, *InstallSnapshotArgs) {
	next := n.nextIndex[peer]
	if last := n.log.lastIndex(); next > last+1 {
		next = last + 1
	}
	if next <= n.log.snapIndex {
		return nil, &InstallSnapshotArgs{
			Term: n.term, LeaderID: n.id,
			LastIncludedIndex: n.log.snapIndex, LastIncludedTerm: n.log.snapTerm,
			Data: n.snapshot,
		}
	}
	prev := next - 1
	prevTerm, _ := n.log.term(prev)
	hi := n.log.lastIndex() + 1
	if limit := next + uint64(n.cfg.MaxEntriesPerAppend); hi > limit {
		hi = limit
	}
	return &AppendEntriesArgs{
		Term: n.term, LeaderID: n.id,
		PrevLogIndex: prev, PrevLogTerm: prevTerm,
		Entries:      n.log.slice(next, hi),
		LeaderCommit: n.commitIndex,
	}, nil
}

// sendAppend performs one AppendEntries exchange and returns true if the
// replicator should immediately send again (more to ship, or a backed-up
// nextIndex to retry).
func (n *Node) sendAppend(peer int, term uint64, a *AppendEntriesArgs) bool {
	reply, err := n.tr.AppendEntries(peer, a)
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
	if reply.Success {
		match := a.PrevLogIndex + uint64(len(a.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		if match+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = match + 1
		}
		n.advanceCommitLocked()
		return n.nextIndex[peer] <= n.log.lastIndex()
	}
	if n.nextIndex[peer] != a.PrevLogIndex+1 {
		return true // stale reply; rebuild from current state
	}
	// Back up past the whole conflicting term in one step.
	next := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		if li := n.log.lastIndexOfTerm(reply.ConflictTerm); li > 0 {
			next = li + 1
		}
	}
	if next > a.PrevLogIndex {
		next = a.PrevLogIndex // always make progress
	}
	if next < 1 {
		next = 1
	}
	n.nextIndex[peer] = next
	return true
}

// advanceCommitLocked moves commitIndex to the highest index replicated on a
// quorum, subject to the rule from paper section 5.4.2: a leader only commits
// entries from its own term by counting replicas. Earlier entries become
// committed indirectly, by being below such an entry. The no-op appended on
// election exists so this can happen without waiting for a client.
func (n *Node) advanceCommitLocked() {
	for idx := n.log.lastIndex(); idx > n.commitIndex; idx-- {
		if t, _ := n.log.term(idx); t != n.term {
			return
		}
		count := 1 // the leader's own log, already persisted
		for _, p := range n.peers {
			if p != n.id && n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.setCommitLocked(idx)
			n.kickAllLocked() // tell followers the new commit index promptly
			return
		}
	}
}

func (n *Node) setCommitLocked(idx uint64) {
	n.commitIndex = idx
	n.cond.Broadcast()
	n.emitLocked(EventCommit, "", idx)
}

// HandleAppendEntries implements the AppendEntries RPC (also the heartbeat).
func (n *Node) HandleAppendEntries(a *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &AppendEntriesReply{Term: n.term}
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

	prev, prevTerm, entries := a.PrevLogIndex, a.PrevLogTerm, a.Entries
	if prev < n.log.snapIndex {
		// The leader is sending something we already compacted away. Those
		// entries are committed, so they match; skip to what follows.
		skip := n.log.snapIndex - prev
		if skip >= uint64(len(entries)) {
			entries = nil
		} else {
			entries = entries[skip:]
		}
		prev, prevTerm = n.log.snapIndex, n.log.snapTerm
	}
	if prev > n.log.lastIndex() {
		reply.ConflictIndex = n.log.lastIndex() + 1
		return reply
	}
	if t, _ := n.log.term(prev); t != prevTerm {
		reply.ConflictTerm = t
		reply.ConflictIndex = n.log.firstIndexOfTerm(t, prev)
		return reply
	}

	changed := false
	for i, e := range entries {
		idx := prev + 1 + uint64(i)
		if idx <= n.log.lastIndex() {
			if t, _ := n.log.term(idx); t == e.Term {
				continue
			}
			if idx <= n.commitIndex {
				panic("raft: leader asked to overwrite a committed entry")
			}
			n.log.truncateFrom(idx)
		}
		n.log.append(entries[i:]...)
		changed = true
		break
	}
	if changed && !n.mustPersistLocked() {
		return reply
	}

	if a.LeaderCommit > n.commitIndex {
		c := a.LeaderCommit
		if lastNew := prev + uint64(len(entries)); c > lastNew {
			c = lastNew
		}
		if c > n.commitIndex {
			n.setCommitLocked(c)
		}
	}
	reply.Success = true
	return reply
}
