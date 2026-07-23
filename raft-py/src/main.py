import argparse
import asyncio
import signal

from src.node import RaftNode
from src.state_machine import KeyValueStateMachine


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a single Raft cluster node")
    parser.add_argument(
        "--node-id", required=True,
        help="This node's address, e.g. 127.0.0.1:9001 (also its listen address)",
    )
    parser.add_argument(
        "--peers", default="",
        help="Comma-separated peer addresses, e.g. 127.0.0.1:9002,127.0.0.1:9003",
    )
    parser.add_argument("--db-path", default="./raft.db", help="Path to this node's SQLite persistence file")
    parser.add_argument("--election-timeout-min-ms", type=int, default=150)
    parser.add_argument("--election-timeout-max-ms", type=int, default=300)
    parser.add_argument("--heartbeat-timeout-ms", type=int, default=50)
    return parser.parse_args()


async def run() -> None:
    args = parse_args()
    peers = [p for p in args.peers.split(",") if p]

    node = RaftNode(
        node_id=args.node_id,
        peers=peers,
        state_machine=KeyValueStateMachine(),
        db_path=args.db_path,
        election_timeout_min_ms=args.election_timeout_min_ms,
        election_timeout_max_ms=args.election_timeout_max_ms,
        heartbeat_timeout_ms=args.heartbeat_timeout_ms,
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    node_task = asyncio.create_task(node.start())
    print(f"[{node.node_id}] listening, peers={node.peers}")

    await stop_event.wait()
    await node.stop()
    node_task.cancel()


if __name__ == "__main__":
    asyncio.run(run())
