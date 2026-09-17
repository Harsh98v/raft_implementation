package raft

import (
	"errors"
	"sync"
)

var errUnreachable = errors.New("memnet: peer unreachable")

// Network is an in-memory stand-in for the real network. Tests (and later the
// chaos harness) cut nodes off with SetConnected to simulate crashes and
// partitions, deterministically and without sockets.
type Network struct {
	mu        sync.Mutex
	nodes     map[int]*Raft
	connected map[int]bool
}

func NewNetwork() *Network {
	return &Network{nodes: map[int]*Raft{}, connected: map[int]bool{}}
}

func (n *Network) Register(id int, rf *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = rf
	n.connected[id] = true
}

func (n *Network) SetConnected(id int, up bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = up
}

func (n *Network) IsConnected(id int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.connected[id]
}

// Transport returns the view of the network as seen from node `from`.
func (n *Network) Transport(from int) Transport { return &memTransport{n, from} }

// target returns the destination node if both ends are currently connected.
func (n *Network) target(from, to int) (*Raft, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.connected[from] || !n.connected[to] {
		return nil, errUnreachable
	}
	return n.nodes[to], nil
}

type memTransport struct {
	net  *Network
	from int
}

// call delivers a request and its reply. Connectivity is checked on both legs,
// so a reply is dropped if the link was cut while the request was in flight,
// just like a real network.
func call[A, R any](t *memTransport, to int, args A, handle func(*Raft, A) R) (R, error) {
	var zero R
	rf, err := t.net.target(t.from, to)
	if err != nil {
		return zero, err
	}
	reply := handle(rf, args)
	if _, err := t.net.target(to, t.from); err != nil {
		return zero, err
	}
	return reply, nil
}

func (t *memTransport) RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return call(t, to, args, (*Raft).HandleRequestVote)
}

func (t *memTransport) AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return call(t, to, args, (*Raft).HandleAppendEntries)
}
