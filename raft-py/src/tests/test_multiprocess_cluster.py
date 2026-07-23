"""
Multi-process smoke test: launches real `python -m src.main` node processes
and drives them purely over their socket protocol -- no shared Python
objects, no imports across the process boundary. This is the "real
multi-process simulation" the in-process tests in test_cluster_integration.py
can't fully prove (they share one OS process for speed/simplicity).

Run directly:
    uv run python -m src.tests.test_multiprocess_cluster
"""

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
BASE_PORT = 8200
CLUSTER_SIZE = 3


async def rpc_call(address: str, msg_type: str, payload: dict, timeout: float = 1.0) -> dict:
    host, port = address.rsplit(":", 1)
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, int(port)), timeout=timeout)
    try:
        writer.write((json.dumps({"type": msg_type, "payload": payload}) + "\n").encode())
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        return json.loads(line)
    finally:
        writer.close()


async def get_status(address: str) -> Optional[dict]:
    try:
        response = await rpc_call(address, "get_status", {})
        return response["payload"]
    except (OSError, asyncio.TimeoutError):
        return None


def spawn_node(node_id: str, peers: list[str], db_path: Path) -> subprocess.Popen:
    return subprocess.Popen(
        [
            sys.executable, "-m", "src.main",
            "--node-id", node_id,
            "--peers", ",".join(peers),
            "--db-path", str(db_path),
        ],
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


async def wait_for_leader(addresses: list[str], timeout: float = 8.0) -> str:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        statuses = [await get_status(a) for a in addresses]
        leaders = [a for a, s in zip(addresses, statuses) if s and s["state"] == "leader"]
        if len(leaders) == 1:
            return leaders[0]
        await asyncio.sleep(0.1)
    raise AssertionError("no leader elected across subprocesses within timeout")


async def wait_until(predicate, timeout: float = 5.0, interval: float = 0.1) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


async def run() -> None:
    addresses = [f"127.0.0.1:{BASE_PORT + i}" for i in range(CLUSTER_SIZE)]
    procs = []

    with tempfile.TemporaryDirectory() as tmp:
        try:
            for i, addr in enumerate(addresses):
                peers = [a for a in addresses if a != addr]
                procs.append(spawn_node(addr, peers, Path(tmp) / f"node{i}.db"))

            leader = await wait_for_leader(addresses)
            print(f"leader elected: {leader}")

            set_response = await rpc_call(leader, "submit_command", {"op": "SET", "key": "x", "value": "hello"})
            assert set_response["payload"]["ok"], set_response

            async def replicated() -> bool:
                statuses = [await get_status(a) for a in addresses]
                return all(s and s["log_length"] >= 1 and s["commit_index"] >= 1 for s in statuses)

            await wait_until(replicated)

            get_response = await rpc_call(leader, "submit_command", {"op": "GET", "key": "x"})
            assert get_response["payload"]["ok"], get_response
            assert get_response["payload"]["result"]["result"] == "hello", get_response

            print("All multi-process assertions passed.")
        finally:
            for p in procs:
                p.terminate()
            for p in procs:
                p.wait(timeout=5)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        print(f"FAILED: {e}")
        sys.exit(1)
