package raft

import "time"

// campaignLocked is called by the ticker when the election deadline passes.
func (n *Node) campaignLocked() {
	if n.cfg.PreVote {
		n.startPreVoteLocked()
	} else {
		n.startElectionLocked()
	}
}

// startPreVoteLocked asks every peer whether it would vote for us at term+1
// without changing any durable state, ours or theirs. Only if a quorum says
// yes do we actually increment the term and run a real election. A node cut
// off from the group therefore keeps asking and never inflates its term, so
// when it rejoins it cannot knock a healthy leader out of office.
func (n *Node) startPreVoteLocked() {
	n.setRoleLocked(PreCandidate)
	n.resetDeadlineLocked()
	n.round++
	n.grants = map[int]bool{n.id: true}
	if len(n.grants) >= n.quorum() {
		n.startElectionLocked()
		return
	}
	args := &RequestVoteArgs{
		Term: n.term + 1, CandidateID: n.id,
		LastLogIndex: n.log.lastIndex(), LastLogTerm: n.log.lastTerm(),
		PreVote: true,
	}
	n.broadcastVoteLocked(args)
}

// startElectionLocked increments the term, votes for itself, and requests
// votes. The term and the self-vote are persisted before the first request
// leaves, otherwise a crash could let us vote twice in one term.
func (n *Node) startElectionLocked() {
	n.term++
	n.votedFor = n.id
	n.leaderID = NoVote
	n.setRoleLocked(Candidate)
	n.resetDeadlineLocked()
	n.round++
	n.grants = map[int]bool{n.id: true}
	if !n.mustPersistLocked() {
		return
	}
	if len(n.grants) >= n.quorum() {
		n.becomeLeaderLocked()
		return
	}
	args := &RequestVoteArgs{
		Term: n.term, CandidateID: n.id,
		LastLogIndex: n.log.lastIndex(), LastLogTerm: n.log.lastTerm(),
	}
	n.broadcastVoteLocked(args)
}

func (n *Node) broadcastVoteLocked(args *RequestVoteArgs) {
	round := n.round
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.wg.Add(1)
		go n.askVote(p, args, round)
	}
}

func (n *Node) askVote(peer int, args *RequestVoteArgs, round uint64) {
	defer n.wg.Done()
	reply, err := n.tr.RequestVote(peer, args)
	if err != nil || reply == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	if reply.Term > n.term {
		n.becomeFollowerLocked(reply.Term, NoVote)
		return
	}
	if n.round != round || !reply.Granted {
		return
	}
	if args.PreVote {
		if n.role != PreCandidate || args.Term != n.term+1 {
			return
		}
		n.grants[peer] = true
		if len(n.grants) >= n.quorum() {
			n.startElectionLocked()
		}
		return
	}
	if n.role != Candidate || args.Term != n.term {
		return
	}
	n.grants[peer] = true
	if len(n.grants) >= n.quorum() {
		n.becomeLeaderLocked()
	}
}

// becomeFollowerLocked steps down (or stays down) at the given term. If the
// term advanced, the vote for the old term is forgotten and the new term is
// persisted. It reports false only if persisting failed and the node halted.
func (n *Node) becomeFollowerLocked(term uint64, leader int) bool {
	advanced := term > n.term
	if advanced {
		n.term = term
		n.votedFor = NoVote
	}
	n.leaderID = leader
	n.setRoleLocked(Follower)
	if advanced {
		return n.mustPersistLocked()
	}
	return true
}

// becomeLeaderLocked takes office: it appends a no-op entry for the new term
// and starts one replicator goroutine per follower.
func (n *Node) becomeLeaderLocked() {
	n.setRoleLocked(Leader)
	n.leaderID = n.id
	last := n.log.lastIndex()
	n.nextIndex = make(map[int]uint64, len(n.peers))
	n.matchIndex = make(map[int]uint64, len(n.peers))
	n.kick = make(map[int]chan struct{}, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.log.append(Entry{Term: n.term, Noop: true})
	if !n.mustPersistLocked() {
		return
	}
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		ch := make(chan struct{}, 1)
		n.kick[p] = ch
		n.wg.Add(1)
		go n.replicator(p, n.term, ch)
	}
	n.advanceCommitLocked()
}

// HandleRequestVote implements the RequestVote RPC, including PreVote.
func (n *Node) HandleRequestVote(a *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &RequestVoteReply{Term: n.term}
	if n.stopped {
		return reply
	}
	if a.PreVote {
		reply.Granted = n.grantPreVoteLocked(a)
		return reply
	}
	if a.Term < n.term {
		return reply
	}
	if a.Term > n.term {
		if !n.becomeFollowerLocked(a.Term, NoVote) {
			return reply
		}
	}
	reply.Term = n.term
	if (n.votedFor == NoVote || n.votedFor == a.CandidateID) && n.log.upToDate(a.LastLogTerm, a.LastLogIndex) {
		if n.votedFor != a.CandidateID {
			// Persist the vote before telling anyone about it.
			n.votedFor = a.CandidateID
			if !n.mustPersistLocked() {
				return reply
			}
		}
		n.resetDeadlineLocked()
		reply.Granted = true
	}
	return reply
}

// grantPreVoteLocked decides a PreVote without mutating any state. We refuse
// if we are the leader, or if we heard from a leader within the minimum
// election timeout: in both cases an election is not needed, and granting
// would let a flapping node disturb a working cluster.
func (n *Node) grantPreVoteLocked(a *RequestVoteArgs) bool {
	if a.Term <= n.term || n.role == Leader {
		return false
	}
	if !n.lastContact.IsZero() && time.Since(n.lastContact) < n.cfg.ElectionTimeoutMin {
		return false
	}
	return n.log.upToDate(a.LastLogTerm, a.LastLogIndex)
}
