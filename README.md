# raftkv

A distributed, fault-tolerant key-value store built on a from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go.

The goal is a correct, well-tested, observable Raft rather than a textbook minimum: linearizable reads, client request deduplication, leader redirects, Prometheus metrics, and a chaos-testing harness that checks fault-tolerance claims instead of asserting them.

## Status

| Component | Status |
|---|---|
| Leader election | Done |
| Log replication | Planned |
| Persistence (term, vote, log) | Planned |
| KV state machine (`Get` / `Put` / `Delete`) | Planned |
| Request deduplication, linearizable reads | Planned |
| gRPC transport, node server, CLI client | Planned |
| Chaos-testing harness | Planned |
| Metrics and structured logging | Planned |

## Architecture

- **Cluster:** an odd number of nodes (3 or 5), each an independent process. Every node is a Follower, Candidate, or Leader.
- **Leader election:** randomized election timeouts reduce split votes; `RequestVote` carries the candidate's last log index/term so only up-to-date nodes can win (§5.4.1).
- **Log replication:** the leader replicates entries with `AppendEntries` (which doubles as a heartbeat), tracks `nextIndex`/`matchIndex` per follower, and commits once a majority stores an entry.
- **State machine:** the KV store changes only by applying committed log entries in order.
- **Clients:** any node accepts requests; followers redirect to the leader. Requests carry a client ID and request ID so retries are never applied twice. Reads use ReadIndex, so a leader that has been partitioned away cannot serve stale data.
- **Transport:** Raft talks to peers through a `Transport` interface. Real clusters use gRPC; tests use an in-memory network that can drop links on demand.

## Design decisions

- **Persist term, vote, and log from the start.** Raft is only safe if these survive a restart. A node that forgets its vote can vote twice in one term and create two leaders.
- **Pluggable transport.** Simulating partitions and delay in memory is fast and deterministic, so the same Raft code is tested under failures without sockets.
- **Redirect instead of proxy.** It is simpler, and clients learn who the leader is.
- **ReadIndex instead of leases.** Leases depend on bounded clock drift; ReadIndex only needs the leader to confirm with a majority that it is still leader.
- **One mutex per node, never held during an RPC.** This is the simplest concurrency model that is correct, and it rules out cross-node deadlocks.

## Layout

```
raft/
  raft.go            node state, election, heartbeats
  transport.go       Transport interface and RPC messages
  memnet.go          in-memory network for tests
  election_test.go   election tests
```

## Running tests

Requires Go 1.22+.

```sh
go test -race ./...
```

`-race` enables Go's data race detector (on Windows it needs cgo and a C compiler).

## Roadmap

1. Log replication, including conflicting-entry repair and the Figure 8 commit rule
2. Persistence and crash recovery
3. KV layer with deduplication and linearizable reads
4. gRPC transport, node binary, CLI client
5. Chaos harness: node kills and restarts, network partitions, latency, randomized runs checking invariants (at most one leader per term, no committed entry lost, all nodes converge)
6. Metrics (election count and duration, replication lag, p50/p95/p99 latency, commit progress) and structured logs tagged with node, term, and role
7. Stretch goals: snapshotting, membership changes, benchmarks under normal load and during failures

## References

- Diego Ongaro and John Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/)

## License

MIT, see [LICENSE](LICENSE).
