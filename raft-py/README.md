# raft-py

A Raft consensus implementation in Python: leader election, log replication,
SQLite-backed persistence, and a real TCP transport so nodes can run as
separate processes/machines and talk to each other and to clients over
sockets.

## Requirements

- Python 3.13+
- [uv](https://github.com/astral-sh/uv)

## Use cases

What you get here is leader election + a replicated log + a pluggable
state machine -- that combination is the building block behind most
"distributed coordination" problems, not just distributed key-value
stores:

- **Replicated config / feature-flag store.** Every node applies the same
  ordered SET/DELETE commands, so all replicas agree on current config
  even if some are down or partitioned.
- **Leader election / singleton-worker coordination.** Point a fleet of
  worker processes at the cluster and have only the current Raft leader
  run a job (cron-like tasks, a scheduler, a lock holder) -- `get_status`
  tells any node whether it's the leader right now.
- **Distributed counters / sequence generators.** `CounterStateMachine`
  (below) gives monotonic, crash-safe counters for rate limiting, unique
  ID allocation, or replicated tallies.
- **Small embedded metadata store.** A service that needs a handful of
  other processes to agree on "who owns shard 3" or "what's the current
  epoch" without standing up etcd/ZooKeeper/Consul.
- **Command log / audit trail.** Every applied command is a durable,
  ordered log entry (see `src/persistence.py`'s `log_entries` table) --
  useful as an append-only event log independent of what the state
  machine does with it.
- **Teaching/reference implementation.** Small enough to read end to end
  (`node.py`, `rpc.py`) while being wired to real sockets and real
  multi-process tests, not a single-process toy.

It is **not** a drop-in replacement for etcd/Consul/ZooKeeper for anything
serious -- see [Known limitations](#known-limitations) (no snapshotting,
no membership changes, no transport security) before depending on it for
production traffic.

## Running a cluster

Each node is its own process. A node's `--node-id` is also its listen
address (`host:port`), and `--peers` is the comma-separated list of the
other nodes' addresses.

Start a 3-node cluster locally, each in its own terminal:

```bash
uv run python -m src.main \
  --node-id 127.0.0.1:9001 \
  --peers 127.0.0.1:9002,127.0.0.1:9003 \
  --db-path node1.db

uv run python -m src.main \
  --node-id 127.0.0.1:9002 \
  --peers 127.0.0.1:9001,127.0.0.1:9003 \
  --db-path node2.db

uv run python -m src.main \
  --node-id 127.0.0.1:9003 \
  --peers 127.0.0.1:9001,127.0.0.1:9002 \
  --db-path node3.db
```

Each node binds a TCP server on its own address for both peer RPCs
(RequestVote/AppendEntries) and client requests. Stop a node with Ctrl+C
(SIGINT) or SIGTERM; it shuts down its server and closes its database
connection cleanly.

### CLI flags

| Flag | Default | Description |
|---|---|---|
| `--node-id` | *(required)* | This node's address, e.g. `127.0.0.1:9001` |
| `--peers` | `""` | Comma-separated peer addresses |
| `--db-path` | `./raft.db` | Path to this node's SQLite persistence file |
| `--state-machine` | `src.state_machine:KeyValueStateMachine` | `module.path:ClassName` of the `StateMachine` to run |
| `--election-timeout-min-ms` | `150` | Lower bound of the randomized election timeout |
| `--election-timeout-max-ms` | `300` | Upper bound of the randomized election timeout |
| `--heartbeat-timeout-ms` | `50` | Leader heartbeat interval |

## Talking to a cluster

Nodes speak newline-delimited JSON over TCP: one request per line in,
one response per line out, on the same connection. The same port serves
both peer RPCs (RequestVote/AppendEntries) and client requests -- there is
no separate admin port.

```bash
printf '{"type": "submit_command", "payload": {"op": "SET", "key": "x", "value": "42"}}\n' \
  | nc 127.0.0.1 9001

printf '{"type": "submit_command", "payload": {"op": "GET", "key": "x"}}\n' \
  | nc 127.0.0.1 9001

printf '{"type": "get_status", "payload": {}}\n' \
  | nc 127.0.0.1 9001
```

### Message types

**`submit_command`** -- append a command to the log and wait (up to 5s)
for it to commit. `payload` is passed straight to the state machine; the
built-in `KeyValueStateMachine` understands:

```jsonc
{"op": "SET", "key": "x", "value": "42"}
{"op": "GET", "key": "x"}
{"op": "DELETE", "key": "x"}
```

Response on success:

```jsonc
{"type": "submit_command_response",
 "payload": {"ok": true, "result": {"op": "GET", "key": "x", "result": "42"}}}
```

Response when the node you asked isn't the leader -- `leader_id` is the
address of the node currently believed to be leader (`null` if unknown,
e.g. mid-election):

```jsonc
{"type": "submit_command_response",
 "payload": {"ok": false, "error": "Not leader", "leader_id": "127.0.0.1:9002"}}
```

**`get_status`** -- point-in-time debug/monitoring snapshot of a single
node (no consensus round-trip):

```jsonc
{"type": "get_status_response",
 "payload": {
   "node_id": "127.0.0.1:9001", "state": "leader", "current_term": 3,
   "voted_for": "127.0.0.1:9001", "commit_index": 5, "last_applied": 5,
   "log_length": 5, "last_log_term": 3
 }}
```

Note the line-based framing means a single message must fit on one line
(`asyncio.StreamReader.readline()`'s default 64KiB buffer) -- fine for
typical KV-sized commands, but not for bulk/blob payloads.

## Extending: custom state machines

The state machine is the one piece meant to be swapped out per use case --
Raft itself (election, replication, persistence, transport) doesn't
change. Everything hangs off `StateMachine` in `src/state_machine.py`:

```python
class StateMachine(ABC):
    @abstractmethod
    def apply(self, command: dict[str, Any]) -> Any:
        """Apply a committed command, return the result handed back to the caller."""

    @abstractmethod
    def get_state(self) -> dict[str, Any]:
        """Full state, for snapshotting (not yet wired up -- see Known limitations)."""

    @abstractmethod
    def restore_state(self, snapshot: dict[str, Any]) -> None:
        """Restore from a snapshot produced by get_state()."""
```

`apply()` is called exactly once per committed log entry, in log order, on
every node -- so it must be deterministic given the same command (no
wall-clock time, no randomness, no I/O to anything outside the state
machine itself). Whatever it returns becomes the `result` of the matching
`submit_command` call.

Two implementations ship in `src/state_machine.py`:

- `KeyValueStateMachine` -- `SET` / `GET` / `DELETE` (the default)
- `CounterStateMachine` -- `INCR` / `DECR` / `GET`, for the counter/rate-limiter
  use case above:

  ```bash
  uv run python -m src.main --node-id 127.0.0.1:9001 \
    --state-machine src.state_machine:CounterStateMachine

  printf '{"type": "submit_command", "payload": {"op": "INCR", "name": "hits", "by": 5}}\n' \
    | nc 127.0.0.1 9001
  ```

To write your own: subclass `StateMachine` anywhere importable (in this
repo or your own package on `PYTHONPATH`), then point every node in the
cluster at it with `--state-machine your.module:YourClass`:

```python
# my_state_machines.py
from typing import Any, override
from src.state_machine import StateMachine

class LockStateMachine(StateMachine):
    """A distributed mutex: ACQUIRE succeeds only if unheld or already yours."""

    def __init__(self) -> None:
        self.holder: str | None = None

    @override
    def apply(self, command: dict[str, Any]) -> dict[str, Any]:
        op, owner = command.get("op"), command.get("owner")

        if op == "ACQUIRE":
            ok = self.holder in (None, owner)
            if ok:
                self.holder = owner
            command["result"] = ok
        elif op == "RELEASE":
            if self.holder == owner:
                self.holder = None
            command["result"] = self.holder is None
        elif op == "STATUS":
            command["result"] = self.holder
        else:
            raise ValueError(f"Invalid command operation: {op}")

        return command

    @override
    def get_state(self) -> dict[str, Any]:
        return {"holder": self.holder}

    @override
    def restore_state(self, snapshot: dict[str, Any]) -> None:
        self.holder = snapshot.get("holder")
```

```bash
uv run python -m src.main --node-id 127.0.0.1:9001 \
  --state-machine my_state_machines:LockStateMachine
```

Every node in the cluster must run the **same** state machine class --
Raft only guarantees every node applies the same sequence of commands, not
that mismatched state machines produce compatible state.

## Running the tests

```bash
# In-process cluster over real sockets: election, replication, failover,
# restart/persistence.
uv run python -m src.tests.test_cluster_integration

# Spawns real `python -m src.main` subprocesses and drives them purely
# over sockets -- a genuine multi-process simulation.
uv run python -m src.tests.test_multiprocess_cluster
```

## How it works

Standard Raft (Ongaro & Ousterhout):

- **Terms & elections.** Each node is a `follower`, `candidate`, or
  `leader`. A follower that hears nothing from a leader within a randomized
  election timeout (`--election-timeout-min-ms`..`--election-timeout-max-ms`)
  becomes a candidate, bumps its term, votes for itself, and requests votes
  from peers. A candidate that wins a majority becomes leader; one that
  hears from a legitimate leader (equal-or-higher term) steps back down to
  follower, even mid-election.
- **Replication.** The leader sends `AppendEntries` to every follower --
  empty ones as heartbeats (`--heartbeat-timeout-ms` apart), non-empty ones
  carrying new log entries. A follower rejects entries that don't chain
  onto a matching `(prev_log_index, prev_log_term)`, forcing the leader to
  walk `next_index` back until logs agree, truncating conflicting suffixes.
- **Commitment.** An entry commits once it's stored on a majority *and*
  belongs to the leader's current term (entries from older terms only
  commit indirectly, by being covered by a newer entry that itself
  commits -- see the single-node restart case below). Once committed, it's
  applied to the state machine in order and the result is handed back to
  whoever called `submit_command`.
- **Persistence.** `current_term`, `voted_for`, and the log itself survive
  restarts (SQLite, one file per node via `--db-path`). `commit_index` and
  `last_applied` are volatile by design -- a restarted node re-derives them
  through normal Raft operation rather than trusting stale disk state. A
  practical consequence for a *single-node* "cluster": after a restart, old
  entries won't reach the state machine until you submit one more command
  in the new term, which indirectly re-commits everything before it in one
  shot (see `test_log_persistence_across_restart`).

## Known limitations

Things a production deployment would still need that this repo doesn't do:

- No log compaction / snapshotting -- the `snapshots` table exists in the
  schema but nothing writes to it, so the log grows unboundedly.
- No cluster membership changes (adding/removing nodes) -- the peer set is
  fixed at process start via `--peers`.
- No transport security -- RPCs and client traffic are plaintext TCP with
  no auth, so don't expose a node's port beyond a trusted network.
- No read-lease/read-index optimization -- every read goes through
  `submit_command`, i.e. through the log and only on the leader.
- Client and peer traffic share one port/protocol; there's no rate
  limiting or backpressure if a node is flooded with client requests.

## Layout

- `src/node.py` -- core Raft state machine: elections, replication,
  commit/apply, client-facing `submit_command`
- `src/rpc.py` -- RequestVote/AppendEntries rule implementations
- `src/log.py` / `src/persistence.py` -- log entries and term/vote state,
  persisted to SQLite
- `src/backends/socket_backend.py` -- TCP transport (peer RPCs + client
  requests) wired into `RaftNode`
- `src/state_machine.py` -- pluggable application state; ships with a
  key-value example
- `src/main.py` -- CLI entry point to run one node per process
