package raft

// RequestVoteArgs is used for both the PreVote and the real vote. In a PreVote
// Term is the term the candidate would move to, not one it has adopted.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
}

type RequestVoteReply struct {
	Term    uint64 // the responder's current term
	Granted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// On failure, a hint that lets the leader back up by a whole term.
	// ConflictTerm is 0 when the follower's log is simply too short, in which
	// case ConflictIndex is one past its last entry.
	ConflictTerm  uint64
	ConflictIndex uint64
}

// InstallSnapshotArgs ships the whole snapshot in one message. Chunking is
// omitted; see the README.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          int
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}

// Transport is how a Node sends RPCs. An error means the message or its reply
// was lost, delayed past the transport's timeout, or the peer is down; Raft
// treats all of those identically and simply retries later. Implementations
// are bound to a sender, so the destination id is all a call needs.
type Transport interface {
	RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(to int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Handler is the receiving side of Transport. *Node implements it.
type Handler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
	HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}
