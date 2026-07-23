import asyncio
import random
import time
from typing import Any, Optional
import threading

from .rtypes import (
    NodeState, LogEntry, RequestVoteRPC, RequestVoteResponse,
    AppendEntriesRPC, AppendEntriesResponse
)
from .persistence import PersistenceLayer
from .log import RaftLog
from .state_machine import StateMachine
from .rpc import RPCHandler
from .backends import SocketBackend


class RaftNode:
    """
    Main Raft Node implementation.
    Manages state, log replication, and consensus.
    """

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        state_machine: StateMachine,
        db_path: str = "./raft.db",
        election_timeout_min_ms: int = 150,
        election_timeout_max_ms: int = 300,
        heartbeat_timeout_ms: int = 50,
        vote_timeout_ms: int = 300,
    ):
        # Node identity
        self.node_id = node_id
        self.peers = [p for p in peers if p != node_id]  # Remove self from peers
        self.state_machine = state_machine

        # Persistence
        self.persistence = PersistenceLayer(db_path)
        self.log = RaftLog(self.persistence)

        # Persistent state (restored from disk on startup)
        self.current_term = self.persistence.load_term()
        self.voted_for = self.persistence.load_voted_for()

        # Volatile state
        self.state = NodeState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0

        # Leader state (only valid if state == LEADER)
        self.next_index = {peer: self.log.last_index() + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}

        # node_id of the peer we believe is the current leader (None if unknown)
        self.leader_id: Optional[str] = None

        # Timers
        self.election_timeout_min_ms = election_timeout_min_ms
        self.election_timeout_max_ms = election_timeout_max_ms
        self.heartbeat_timeout_ms = heartbeat_timeout_ms
        self.vote_timeout_ms = vote_timeout_ms

        # Timer tasks
        self.election_timer_task: Optional[asyncio.Task] = None
        self.heartbeat_timer_task: Optional[asyncio.Task] = None

        # Thread safety
        self.lock = threading.RLock()

        # RPC handler
        self.rpc_handler = RPCHandler(self)

        # Network transport
        self.backend = SocketBackend(self)

        # Running state
        self.running = False

        # Pending client requests (waiting for commitment)
        self.pending_requests: dict[int, asyncio.Future] = {}

    # ===== STATE TRANSITIONS =====

    def become_follower(self, term: int) -> None:
        """Transition to Follower state and update term if necessary"""
        with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.persistence.save_term(term)
                self.persistence.save_voted_for(None)

            self.state = NodeState.FOLLOWER
            self.reset_election_timer()

    def become_candidate(self) -> None:
        """Transition to Candidate state and start election"""
        with self.lock:
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None
            self.persistence.save_term(self.current_term)
            self.persistence.save_voted_for(self.node_id)
            self.reset_election_timer()

        # Start election in background
        if self.running:
            asyncio.create_task(self.conduct_election())

    def become_leader(self) -> None:
        """Transition to Leader state and initialize leader state"""
        with self.lock:
            self.state = NodeState.LEADER
            self.leader_id = self.node_id
            # Initialize next_index and match_index for all peers
            self.next_index = {peer: self.log.last_index() + 1 for peer in self.peers}
            self.match_index = {peer: 0 for peer in self.peers}
            # Cancel election timer when becoming leader
            if self.election_timer_task:
                self.election_timer_task.cancel()
                self.election_timer_task = None
            if self.heartbeat_timer_task:
                self.heartbeat_timer_task.cancel()
                self.heartbeat_timer_task = None

        # Start sending heartbeats
        if self.running:
            self.heartbeat_timer_task = asyncio.create_task(self.send_heartbeats())

    # ===== ELECTION =====

    async def conduct_election(self) -> None:
        """
        Conduct election: send RequestVote RPC to all peers.
        Become leader if get majority votes.
        """
        with self.lock:
            current_term = self.current_term
            last_log_index = self.log.last_index()
            last_log_term = self.log.last_term()

        # Send vote requests to all peers
        vote_tasks = []
        for peer in self.peers:
            rpc = RequestVoteRPC(
                term=current_term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
            )
            vote_tasks.append(self.send_request_vote(peer, rpc))

        # Wait for responses (with timeout)
        try:
            responses = await asyncio.wait_for(
                asyncio.gather(*vote_tasks, return_exceptions=True),
                timeout=self.vote_timeout_ms / 1000.0
            )
        except asyncio.TimeoutError:
            responses = []

        # Count votes
        votes_received = 1  # Vote for self
        for response in responses:
            if isinstance(response, RequestVoteResponse):
                if response.term > current_term:
                    # Higher term seen, become follower
                    self.become_follower(response.term)
                    return
                if response.vote_granted:
                    votes_received += 1

        # Check if won majority
        majority = len(self.peers) + 1  # +1 for self
        if votes_received > majority // 2:
            self.become_leader()

    async def send_request_vote(self, peer: str, rpc: RequestVoteRPC) -> Optional[RequestVoteResponse]:
        """Send RequestVote RPC to peer. Return response or None on failure."""
        return await self.backend.send_request_vote(peer, rpc)

    def reset_election_timer(self) -> None:
        """Reset election timeout with random jitter"""
        if self.election_timer_task:
            self.election_timer_task.cancel()

        timeout_ms = random.randint(
            self.election_timeout_min_ms,
            self.election_timeout_max_ms
        )
        self.election_timer_task = asyncio.create_task(
            self.election_timeout_handler(timeout_ms)
        )

    async def election_timeout_handler(self, timeout_ms: int) -> None:
        """Wait for election timeout, then start election if still follower"""
        try:
            await asyncio.sleep(timeout_ms / 1000.0)

            with self.lock:
                if self.state == NodeState.FOLLOWER:
                    # Timeout fired while still follower, start election
                    pass

            self.become_candidate()
        except asyncio.CancelledError:
            pass  # Timer was reset

    # ===== HEARTBEATS & LOG REPLICATION =====

    async def send_heartbeats(self) -> None:
        """Leader: periodically send heartbeats (empty AppendEntries) to followers"""
        while self.running:
            try:
                with self.lock:
                    if self.state != NodeState.LEADER:
                        break

                # Send AppendEntries to all peers
                tasks = []
                for peer in self.peers:
                    tasks.append(self.send_append_entries(peer))

                await asyncio.gather(*tasks, return_exceptions=True)

                # Wait before next heartbeat
                await asyncio.sleep(self.heartbeat_timeout_ms / 1000.0)

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def send_append_entries(self, peer: str) -> None:
        """Send AppendEntries RPC to a specific peer"""
        with self.lock:
            if self.state != NodeState.LEADER:
                return

            next_index = self.next_index.get(peer, 1)
            prev_log_index = next_index - 1
            prev_log_term = self.log.get_term(prev_log_index) if prev_log_index > 0 else 0

            # Get entries to send
            entries = self.log.get_entries(next_index, self.log.last_index() + 1)

            rpc = AppendEntriesRPC(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index,
            )

        # Send RPC (Phase 2: actual network call)
        response = await self.send_append_entries_rpc(peer, rpc)

        if response:
            self.handle_append_entries_response(peer, response)

    async def send_append_entries_rpc(self, peer: str, rpc: AppendEntriesRPC) -> Optional[AppendEntriesResponse]:
        """Send AppendEntries RPC to peer. Return response or None on failure."""
        return await self.backend.send_append_entries(peer, rpc)

    def handle_append_entries_response(self, peer: str, response: AppendEntriesResponse) -> None:
        """Handle AppendEntries response from follower"""
        with self.lock:
            if self.state != NodeState.LEADER:
                return

            if response.term > self.current_term:
                self.become_follower(response.term)
                if self.heartbeat_timer_task:
                    self.heartbeat_timer_task.cancel()
                    self.heartbeat_timer_task = None
                return

            if response.success:
                # Replication succeeded
                self.match_index[peer] = response.last_log_index
                self.next_index[peer] = response.last_log_index + 1
                self.update_commit_index()
            else:
                # Replication failed, decrement next_index
                self.next_index[peer] = max(1, self.next_index[peer] - 1)

    # ===== LOG COMMITMENT & APPLICATION =====

    def update_commit_index(self) -> None:
        """
        Leader: update commitIndex.
        If N > commitIndex and majority has N in logs and log[N].term == currentTerm,
        set commitIndex = N
        """
        for n in range(self.log.last_index(), self.commit_index, -1):
            # Count how many nodes have this entry
            count = 1  # Count self
            for peer in self.peers:
                if self.match_index.get(peer, 0) >= n:
                    count += 1

            # Check if majority
            majority = len(self.peers) + 1
            if count > majority // 2:
                # Check if entry is from current term
                entry_term = self.log.get_term(n)
                if entry_term == self.current_term:
                    self.commit_index = n
                    self.apply_committed_entries()
                    break

    def apply_committed_entries(self) -> None:
        """Apply all committed entries to state machine that haven't been applied yet"""
        with self.lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log.get_entry(self.last_applied)

                if entry:
                    try:
                        result = self.state_machine.apply(entry.command)
                        # Store result for client response
                        if self.last_applied in self.pending_requests:
                            future = self.pending_requests.pop(self.last_applied)
                            if not future.done():
                                future.set_result(result)
                    except Exception as e:
                        print(f"Error applying entry {self.last_applied}: {e}")

    # ===== CLIENT INTERFACE =====

    async def submit_command(self, command: dict[str, Any]) -> Any:
        """
        Submit command to Raft cluster.
        If leader, append to log and wait for commitment.
        If not leader, return error.
        """
        with self.lock:
            if self.state != NodeState.LEADER:
                raise Exception("Not leader")

            # Create log entry
            entry = LogEntry(
                term=self.current_term,
                index=self.log.last_index() + 1,
                command=command,
                timestamp=time.time(),
            )

            # Append to log
            self.log.append([entry])

            # Create future for response
            future: asyncio.Future = asyncio.Future()
            self.pending_requests[entry.index] = future

            # Update match_index for self and try to advance commit index
            # (handles single-node cluster where no peer responses ever arrive)
            self.update_commit_index()

        # Wait for commitment (with timeout)
        try:
            result = await asyncio.wait_for(future, timeout=5.0)
            return result
        except asyncio.TimeoutError:
            with self.lock:
                self.pending_requests.pop(entry.index, None)
            raise Exception("Command commitment timeout")

    def get_status(self) -> dict[str, Any]:
        """Return node status for debugging/monitoring"""
        with self.lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "current_term": self.current_term,
                "voted_for": self.voted_for,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "log_length": self.log.last_index(),
                "last_log_term": self.log.last_term(),
            }

    # ===== RPC HANDLERS =====

    def handle_request_vote(self, rpc: RequestVoteRPC) -> RequestVoteResponse:
        """Handle RequestVote RPC from candidate"""
        with self.lock:
            # Update term BEFORE delegating the grant/deny decision: voted_for
            # from a stale term must not block granting a vote in a newer one.
            if rpc.term > self.current_term:
                self.current_term = rpc.term
                self.voted_for = None
                self.persistence.save_term(rpc.term)
                self.persistence.save_voted_for(None)
                self.state = NodeState.FOLLOWER
                self.leader_id = None

            response = self.rpc_handler.handle_request_vote(rpc)

            # Reset the election timer only when we actually grant the vote,
            # so we don't suppress our own timeout for a candidate we reject.
            if response.vote_granted:
                self.reset_election_timer()

            return response

    def handle_append_entries(self, rpc: AppendEntriesRPC) -> AppendEntriesResponse:
        """Handle AppendEntries RPC from leader"""
        with self.lock:
            response = self.rpc_handler.handle_append_entries(rpc)

            # Update term if necessary
            if rpc.term > self.current_term:
                self.current_term = rpc.term
                self.voted_for = None
                self.persistence.save_term(rpc.term)
                self.persistence.save_voted_for(None)

            # A candidate (or a stale leader) that hears from a legitimate
            # leader in a term at least as high as its own must step down and
            # recognize that leader -- even if this particular AppendEntries
            # failed the log-consistency check (Rule 2/3), and even when the
            # term didn't just increase (Raft "Rules for Servers": candidates
            # and leaders convert to follower).
            if rpc.term >= self.current_term:
                if self.state != NodeState.FOLLOWER:
                    self.state = NodeState.FOLLOWER
                    if self.heartbeat_timer_task:
                        self.heartbeat_timer_task.cancel()
                        self.heartbeat_timer_task = None

                self.leader_id = rpc.leader_id
                self.reset_election_timer()

            # Apply committed entries
            self.apply_committed_entries()

            return response

    # ===== LIFECYCLE =====

    async def start(self) -> None:
        """Start the node's event loop"""
        self.running = True
        await self.backend.start()
        self.reset_election_timer()

        try:
            while self.running:
                await asyncio.sleep(1)
        except Exception as e:
            print(f"Node error: {e}")
        finally:
            self.running = False

    async def stop(self) -> None:
        """Stop the node"""
        self.running = False
        if self.election_timer_task:
            self.election_timer_task.cancel()
        if self.heartbeat_timer_task:
            self.heartbeat_timer_task.cancel()
        await self.backend.stop()
        self.persistence.close()