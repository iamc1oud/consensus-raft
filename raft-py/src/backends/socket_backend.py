import asyncio
import contextlib
import json
import logging
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Optional

from ..rtypes import (
    AppendEntriesRPC,
    AppendEntriesResponse,
    LogEntry,
    RequestVoteRPC,
    RequestVoteResponse,
)

if TYPE_CHECKING:
    from ..node import RaftNode

logger = logging.getLogger(__name__)


def _parse_address(node_id: str) -> tuple[str, int]:
    host, port = node_id.rsplit(":", 1)
    return host, int(port)


def _encode(obj: dict) -> bytes:
    # json.dumps escapes control chars (incl. newlines) inside strings, so a
    # single line per message is a safe framing for arbitrary command payloads.
    return (json.dumps(obj) + "\n").encode()


class SocketBackend:
    """
    TCP transport for a RaftNode: a server that accepts peer RPCs and client
    requests, and a client that dials peers to send RequestVote/AppendEntries.

    Wire format: newline-delimited JSON, one request or response per line.
    A node's address IS its node_id ("host:port"), so no separate address
    book is needed.
    """

    def __init__(self, node: 'RaftNode', connect_timeout: float = 0.2, request_timeout: float = 0.5):
        self.node = node
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self._server: Optional[asyncio.base_events.Server] = None
        self._conns: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._conn_locks: dict[str, asyncio.Lock] = {}

    # ===== SERVER =====

    async def start(self) -> None:
        host, port = _parse_address(self.node.node_id)
        self._server = await asyncio.start_server(self._handle_connection, host, port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        for _, writer in list(self._conns.values()):
            writer.close()
        self._conns.clear()
        self._conn_locks.clear()

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break

                try:
                    request = json.loads(line)
                    response = await self._dispatch(request)
                except Exception as e:
                    response = {"type": "error", "payload": {"error": str(e)}}

                writer.write(_encode(response))
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _dispatch(self, request: dict) -> dict:
        msg_type = request.get("type")
        payload = request.get("payload", {})

        if msg_type == "request_vote":
            rpc = RequestVoteRPC(**payload)
            response = self.node.handle_request_vote(rpc)
            return {"type": "request_vote_response", "payload": asdict(response)}

        if msg_type == "append_entries":
            entries = [LogEntry(**e) for e in payload.get("entries", [])]
            rpc = AppendEntriesRPC(**{**payload, "entries": entries})
            response = self.node.handle_append_entries(rpc)
            return {"type": "append_entries_response", "payload": asdict(response)}

        if msg_type == "submit_command":
            try:
                result = await self.node.submit_command(payload)
                return {"type": "submit_command_response", "payload": {"ok": True, "result": result}}
            except Exception as e:
                return {
                    "type": "submit_command_response",
                    "payload": {"ok": False, "error": str(e), "leader_id": self.node.leader_id},
                }

        if msg_type == "get_status":
            return {"type": "get_status_response", "payload": self.node.get_status()}

        return {"type": "error", "payload": {"error": f"unknown message type: {msg_type}"}}

    # ===== CLIENT =====

    async def _get_connection(self, peer: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        conn = self._conns.get(peer)
        if conn is not None:
            return conn

        host, port = _parse_address(peer)
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=self.connect_timeout
        )
        self._conns[peer] = (reader, writer)
        return reader, writer

    async def _call(self, peer: str, msg_type: str, payload: dict) -> Optional[dict]:
        lock = self._conn_locks.setdefault(peer, asyncio.Lock())
        async with lock:
            try:
                reader, writer = await self._get_connection(peer)
                writer.write(_encode({"type": msg_type, "payload": payload}))
                await writer.drain()

                line = await asyncio.wait_for(reader.readline(), timeout=self.request_timeout)
                if not line:
                    raise ConnectionError("peer closed connection")

                return json.loads(line)
            except (OSError, asyncio.TimeoutError, ConnectionError) as e:
                logger.debug("RPC %s to %s failed: %s", msg_type, peer, e)
                self._conns.pop(peer, None)
                return None

    async def send_request_vote(self, peer: str, rpc: RequestVoteRPC) -> Optional[RequestVoteResponse]:
        response = await self._call(peer, "request_vote", asdict(rpc))
        if response is None:
            return None
        return RequestVoteResponse(**response["payload"])

    async def send_append_entries(self, peer: str, rpc: AppendEntriesRPC) -> Optional[AppendEntriesResponse]:
        response = await self._call(peer, "append_entries", asdict(rpc))
        if response is None:
            return None
        return AppendEntriesResponse(**response["payload"])
