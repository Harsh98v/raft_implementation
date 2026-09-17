package raft

import (
	"testing"
	"time"
)

func makeCluster(t *testing.T, n int) (*Network, []*Raft) {
	t.Helper()
	net := NewNetwork()
	nodes := make([]*Raft, n)
	for i := range n {
		var peers []int
		for j := range n {
			if j != i {
				peers = append(peers, j)
			}
		}
		nodes[i] = New(i, peers, net.Transport(i))
		net.Register(i, nodes[i])
	}
	for _, rf := range nodes {
		rf.Start()
	}
	t.Cleanup(func() {
		for _, rf := range nodes {
			rf.Stop()
		}
	})
	return net, nodes
}

// checkOneLeader waits for a leader among connected nodes and fails if any term
// ever has two leaders, which is Raft's core election safety property.
func checkOneLeader(t *testing.T, net *Network, nodes []*Raft) int {
	t.Helper()
	for range 10 {
		time.Sleep(500 * time.Millisecond)
		leaders := map[int][]int{} // term -> leader IDs
		for id, rf := range nodes {
			if !net.IsConnected(id) {
				continue
			}
			if term, isLeader := rf.GetState(); isLeader {
				leaders[term] = append(leaders[term], id)
			}
		}
		latest := -1
		for term, ids := range leaders {
			if len(ids) > 1 {
				t.Fatalf("term %d has %d leaders: %v", term, len(ids), ids)
			}
			latest = max(latest, term)
		}
		if latest != -1 {
			return leaders[latest][0]
		}
	}
	t.Fatal("no leader elected")
	return -1
}

func checkNoLeader(t *testing.T, net *Network, nodes []*Raft) {
	t.Helper()
	for id, rf := range nodes {
		if _, isLeader := rf.GetState(); isLeader && net.IsConnected(id) {
			t.Fatalf("node %d is leader but should not be", id)
		}
	}
}

func TestInitialElection(t *testing.T) {
	net, nodes := makeCluster(t, 3)
	checkOneLeader(t, net, nodes)

	term, _ := nodes[0].GetState()
	for id, rf := range nodes {
		if tm, _ := rf.GetState(); tm != term {
			t.Fatalf("node %d at term %d, node 0 at term %d", id, tm, term)
		}
	}

	// With a healthy leader, nobody should start a new election.
	time.Sleep(2 * electionTimeoutMax)
	if tm, _ := nodes[0].GetState(); tm != term {
		t.Fatalf("term changed from %d to %d without any failure", term, tm)
	}
}

func TestReElection(t *testing.T) {
	net, nodes := makeCluster(t, 3)
	leader1 := checkOneLeader(t, net, nodes)

	// Leader fails: the other two must elect a new one.
	net.SetConnected(leader1, false)
	if leader2 := checkOneLeader(t, net, nodes); leader2 == leader1 {
		t.Fatalf("disconnected node %d is still the leader", leader1)
	}

	// Old leader returns: it must step down, still leaving exactly one leader.
	net.SetConnected(leader1, true)
	leader2 := checkOneLeader(t, net, nodes)

	// Two of three nodes gone: no majority, so no leader can be elected.
	net.SetConnected(leader2, false)
	net.SetConnected((leader2+1)%3, false)
	time.Sleep(2 * electionTimeoutMax)
	checkNoLeader(t, net, nodes)

	// A majority returns: a leader is elected again.
	net.SetConnected((leader2+1)%3, true)
	checkOneLeader(t, net, nodes)
}
