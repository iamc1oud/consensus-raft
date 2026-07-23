import argparse
import asyncio
import importlib
import signal

from src.node import RaftNode
from src.state_machine import StateMachine


def load_state_machine(spec: str) -> StateMachine:
    """Instantiate a StateMachine from a 'module.path:ClassName' spec."""
    module_name, sep, class_name = spec.partition(":")
    if not sep:
        raise ValueError(f"--state-machine must be 'module.path:ClassName', got {spec!r}")

    cls = getattr(importlib.import_module(module_name), class_name)
    if not (isinstance(cls, type) and issubclass(cls, StateMachine)):
        raise TypeError(f"{spec} is not a StateMachine subclass")

    return cls()


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
    parser.add_argument(
        "--state-machine", default="src.state_machine:KeyValueStateMachine",
        help="StateMachine subclass to run, as 'module.path:ClassName' "
             "(e.g. src.state_machine:CounterStateMachine, or your own module)",
    )
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
        state_machine=load_state_machine(args.state_machine),
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
