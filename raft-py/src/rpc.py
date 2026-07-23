from typing import TYPE_CHECKING
from .rtypes import RequestVoteRPC, RequestVoteResponse, AppendEntriesRPC, AppendEntriesResponse

if TYPE_CHECKING:
    from .node import RaftNode


class RPCHandler:
    """
    Handles Raft RPC logic.
    Implements the core Raft algorithm rules.
    """

    def __init__(self, node: 'RaftNode'):
        self.node = node

    def handle_request_vote(self, rpc: RequestVoteRPC) -> RequestVoteResponse:
        """
        Handle RequestVote RPC from candidate.

        Rules (from Raft paper):
        1. If term < currentTerm, return false
        2. If votedFor is null or candidateId, and lastLogIndex/lastLogTerm match, grant vote
        3. Grant vote if candidate's log is at least as up-to-date as ours
        """
        # Rule 1: Check term
        if rpc.term < self.node.current_term:
            return RequestVoteResponse(
                term=self.node.current_term,
                vote_granted=False,
                voter_id=self.node.node_id,
            )

        # At this point, rpc.term >= self.node.current_term
        # (Term update happens in node.handle_request_vote)

        # Rule 2: Check if we haven't voted or voted for this candidate
        if self.node.voted_for is not None and self.node.voted_for != rpc.candidate_id:
            return RequestVoteResponse(
                term=self.node.current_term,
                vote_granted=False,
                voter_id=self.node.node_id,
            )

        # Rule 3: Check if candidate's log is at least as up-to-date as ours
        # "At least as up-to-date" means:
        # - Candidate's last term > our last term, OR
        # - Same last term AND candidate's last index >= our last index

        our_last_term = self.node.log.last_term()
        our_last_index = self.node.log.last_index()

        candidate_is_up_to_date = (
            rpc.last_log_term > our_last_term or
            (rpc.last_log_term == our_last_term and rpc.last_log_index >= our_last_index)
        )

        if not candidate_is_up_to_date:
            return RequestVoteResponse(
                term=self.node.current_term,
                vote_granted=False,
                voter_id=self.node.node_id,
            )

        # Grant vote
        self.node.voted_for = rpc.candidate_id
        self.node.persistence.save_voted_for(rpc.candidate_id)

        return RequestVoteResponse(
            term=self.node.current_term,
            vote_granted=True,
            voter_id=self.node.node_id,
        )

    def handle_append_entries(self, rpc: AppendEntriesRPC) -> AppendEntriesResponse:
        """
        Handle AppendEntries RPC from leader.

        Rules (from Raft paper):
        1. If term < currentTerm, return false
        2. If log doesn't have entry at prevLogIndex with prevLogTerm, return false
        3. If existing entry conflicts with new one, delete it and all after
        4. Append new entries that aren't in the log
        5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, last log index)
        """

        # Rule 1: Check term
        if rpc.term < self.node.current_term:
            return AppendEntriesResponse(
                term=self.node.current_term,
                success=False,
                follower_id=self.node.node_id,
                last_log_index=self.node.log.last_index(),
            )

        # Rule 2: Check if prevLogIndex/prevLogTerm match
        if rpc.prev_log_index > 0:
            # We need an entry at prev_log_index with matching term
            prev_entry = self.node.log.get_entry(rpc.prev_log_index)

            if prev_entry is None:
                # Entry at prevLogIndex doesn't exist
                return AppendEntriesResponse(
                    term=self.node.current_term,
                    success=False,
                    follower_id=self.node.node_id,
                    last_log_index=self.node.log.last_index(),
                )

            if prev_entry.term != rpc.prev_log_term:
                # Term mismatch at prevLogIndex
                # Truncate log from prevLogIndex to handle conflict
                self.node.log.truncate(rpc.prev_log_index)
                return AppendEntriesResponse(
                    term=self.node.current_term,
                    success=False,
                    follower_id=self.node.node_id,
                    last_log_index=self.node.log.last_index(),
                )

        # Rule 3 & 4: Handle entries
        if rpc.entries:
            # Get the index where new entries start
            first_new_index = rpc.prev_log_index + 1

            # Check for conflicts and remove if necessary
            for i, new_entry in enumerate(rpc.entries):
                entry_index = first_new_index + i

                existing_entry = self.node.log.get_entry(entry_index)

                if existing_entry and existing_entry.term != new_entry.term:
                    # Conflict: truncate from this point and append new entries
                    self.node.log.truncate(entry_index)
                    # Append remaining new entries
                    self.node.log.append(rpc.entries[i:])
                    break
                elif existing_entry is None:
                    # Entry doesn't exist, append from here
                    self.node.log.append(rpc.entries[i:])
                    break

        # Rule 5: Update commitIndex
        if rpc.leader_commit > self.node.commit_index:
            # Set to min of leader's commit and our last log index
            self.node.commit_index = min(rpc.leader_commit, self.node.log.last_index())

        return AppendEntriesResponse(
            term=self.node.current_term,
            success=True,
            follower_id=self.node.node_id,
            last_log_index=self.node.log.last_index(),
        )