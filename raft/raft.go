package raft

import (
	"math/rand/v2"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[s]
}

const (
	heartbeatInterval = 50 * time.Millisecond
	// Election timeouts must be several heartbeats long, or followers would
	// start elections while a healthy leader is still sending heartbeats.
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	tickInterval       = 10 * time.Millisecond
)

type LogEntry struct {
	Term    int
	Command []byte
}

// Raft is one node. A single mutex guards all fields; this is the simplest
// correct concurrency model. The rule that keeps it deadlock-free: never hold
// mu while making an RPC.
type Raft struct {
	mu    sync.Mutex
	id    int
	peers []int // every other node's ID
	trans Transport

	// Persistent state (Figure 2). Written to disk in phase 4.
	currentTerm int
	votedFor    int        // -1 means "no vote cast this term"
	log         []LogEntry // log[0] is a dummy entry so real entries start at index 1

	state           State
	lastHeard       time.Time // last time we heard from a leader or granted a vote
	electionTimeout time.Duration
	lastHeartbeat   time.Time // leader only: last time heartbeats went out

	stopCh chan struct{}
}

func New(id int, peers []int, trans Transport) *Raft {
	rf := &Raft{
		id:       id,
		peers:    peers,
		trans:    trans,
		votedFor: -1,
		log:      []LogEntry{{Term: 0}},
		stopCh:   make(chan struct{}),
	}
	rf.resetElectionTimer()
	return rf
}

func (rf *Raft) Start() { go rf.run() }
func (rf *Raft) Stop()  { close(rf.stopCh) }

// GetState reports the current term and whether this node believes it is leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// run is the node's clock: it starts elections and sends heartbeats.
func (rf *Raft) run() {
	t := time.NewTicker(tickInterval)
	defer t.Stop()
	for {
		select {
		case <-rf.stopCh:
			return
		case <-t.C:
		}
		rf.mu.Lock()
		switch {
		case rf.state == Leader && time.Since(rf.lastHeartbeat) >= heartbeatInterval:
			rf.broadcastHeartbeats()
		case rf.state != Leader && time.Since(rf.lastHeard) >= rf.electionTimeout:
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// Randomizing each timeout makes it unlikely that two followers time out at the
// same moment and split the vote (§5.2).
func (rf *Raft) resetElectionTimer() {
	rf.lastHeard = time.Now()
	rf.electionTimeout = electionTimeoutMin + rand.N(electionTimeoutMax-electionTimeoutMin)
}

// stepDown applies the rule for all servers: seeing a higher term means our
// term is stale, so adopt it and go back to being a follower. The election
// timer is deliberately NOT reset: a stale message isn't proof of a live leader.
func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
}

func (rf *Raft) lastLogIndexTerm() (int, int) {
	i := len(rf.log) - 1
	return i, rf.log[i].Term
}

func (rf *Raft) majority() int { return (len(rf.peers)+1)/2 + 1 }

// startElection is called with mu held.
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.id
	rf.resetElectionTimer() // if this election splits, a new one starts after another random timeout

	lastIdx, lastTerm := rf.lastLogIndexTerm()
	args := &RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.id, LastLogIndex: lastIdx, LastLogTerm: lastTerm}
	votes := 1 // our own vote

	for _, p := range rf.peers {
		go func(p int) {
			reply, err := rf.trans.RequestVote(p, args)
			if err != nil {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
				return
			}
			// Ignore replies that arrive after this election is over: we may have
			// already won, lost, or moved on to a later term.
			if rf.state != Candidate || rf.currentTerm != args.Term || !reply.VoteGranted {
				return
			}
			votes++
			if votes >= rf.majority() {
				rf.state = Leader
				rf.broadcastHeartbeats() // announce ourselves before anyone else times out
			}
		}(p)
	}
}

// broadcastHeartbeats is called with mu held. Empty AppendEntries tells
// followers a leader exists, which keeps them from starting elections.
func (rf *Raft) broadcastHeartbeats() {
	rf.lastHeartbeat = time.Now()
	args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.id}
	for _, p := range rf.peers {
		go func(p int) {
			reply, err := rf.trans.AppendEntries(p, args)
			if err != nil {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term) // a newer leader exists; we were partitioned
			}
		}(p)
	}
}

func (rf *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	reply := &RequestVoteReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply // stale candidate
	}

	// Election restriction (§5.4.1): only vote for a candidate whose log is at
	// least as up to date as ours, so a leader always holds every committed entry.
	lastIdx, lastTerm := rf.lastLogIndexTerm()
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	// One vote per term is what guarantees at most one leader per term.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
	return reply
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	reply := &AppendEntriesReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply // a deposed leader; our reply's Term tells it to step down
	}

	// A valid leader exists for our term. A candidate in the same term has lost.
	rf.state = Follower
	rf.resetElectionTimer()
	reply.Success = true // log consistency check arrives in phase 3
	return reply
}
