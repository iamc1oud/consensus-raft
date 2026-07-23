from typing import Any
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class NodeState(StrEnum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

@dataclass
class LogEntry:
    """
    Log entry
    """
    term: int
    index: int
    command: dict[str, Any]
    timestamp: float


@dataclass
class RequestVoteRPC:
    """
    Invoked by candidate to gather votes
    """
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool
    voter_id: str


@dataclass
class AppendEntriesRPC:
    """
    Invoked by leader to replicate log entries
    Also used as heartbeat
    """
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    follower_id: str
    last_log_index: int
