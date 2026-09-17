package raft

// Transport is how a node reaches its peers. Raft logic only talks to this
// interface, so the same code runs over the in-memory test network (memnet.go)
// and, later, over gRPC between real processes.
type Transport interface {
	RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// RPC messages, field-for-field from Figure 2 of the Raft paper.

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int
	LastLogIndex int // with LastLogTerm, lets the voter apply the election restriction (§5.4.1)
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int // voter's currentTerm, so a stale candidate can step down
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
	// PrevLogIndex, PrevLogTerm, Entries, LeaderCommit arrive in phase 3.
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}
