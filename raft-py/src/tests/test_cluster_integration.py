"""
Real integration tests for the Raft cluster over the socket backend.

Each node in these tests is a genuine asyncio TCP server + client (real
sockets on localhost, distinct ports) -- the only thing not "real" here is
that they share one OS process. See test_multiprocess_cluster.py for the
same guarantees across actual separate processes.

Run directly:
    uv run python -m src.tests.test_cluster_integration
"""

import asyncio
import sys
import tempfile
from pathlib import Path

from src.node import RaftNode
from src.rtypes import NodeState
from src.state_machine import KeyValueStateMachine


def make_node(node_index: int, cluster_size: int, db_dir: Path, base_port: int, **overrides) -> RaftNode:
    addresses = [f"127.0.0.1:{base_port + i}" for i in range(cluster_size)]
    node_id = addresses[node_index]
    peers = [a for a in addresses if a != node_id]
    return RaftNode(
        node_id=node_id,
        peers=peers,
        state_machine=KeyValueStateMachine(),
        db_path=str(db_dir / f"node{node_index}.db"),
        **overrides,
    )


async def start_all(nodes: list[RaftNode]) -> list[asyncio.Task]:
    return [asyncio.create_task(n.start()) for n in nodes]


async def stop_all(nodes: list[RaftNode], tasks: list[asyncio.Task]) -> None:
    await asyncio.gather(*(n.stop() for n in nodes))
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def wait_for_leader(nodes: list[RaftNode], timeout: float = 3.0) -> RaftNode:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        leaders = [n for n in nodes if n.state == NodeState.LEADER]
        if len(leaders) == 1:
            return leaders[0]
        if len(leaders) > 1:
            raise AssertionError(f"split brain: {[l.node_id for l in leaders]}")
        await asyncio.sleep(0.02)
    raise AssertionError("no leader elected within timeout")


async def wait_until(predicate, timeout: float = 2.0, interval: float = 0.02) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


async def test_election() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        nodes = [make_node(i, 3, Path(tmp), base_port=8100) for i in range(3)]
        tasks = await start_all(nodes)
        try:
            leader = await wait_for_leader(nodes)
            followers = [n for n in nodes if n is not leader]
            assert len(followers) == 2
            assert all(n.state == NodeState.FOLLOWER for n in followers)

            # Give a couple of heartbeats time to propagate leader_id to followers.
            await wait_until(lambda: all(n.leader_id == leader.node_id for n in nodes))
        finally:
            await stop_all(nodes, tasks)


async def test_command_replication() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        nodes = [make_node(i, 3, Path(tmp), base_port=8110) for i in range(3)]
        tasks = await start_all(nodes)
        try:
            leader = await wait_for_leader(nodes)

            set_result = await leader.submit_command({"op": "SET", "key": "x", "value": "42"})
            assert set_result["result"] is None

            await wait_until(lambda: all(n.state_machine.data.get("x") == "42" for n in nodes))

            get_result = await leader.submit_command({"op": "GET", "key": "x"})
            assert get_result["result"] == "42"
        finally:
            await stop_all(nodes, tasks)


async def test_follower_rejects_write() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        nodes = [make_node(i, 3, Path(tmp), base_port=8120) for i in range(3)]
        tasks = await start_all(nodes)
        try:
            leader = await wait_for_leader(nodes)
            follower = next(n for n in nodes if n is not leader)

            try:
                await follower.submit_command({"op": "SET", "key": "y", "value": "1"})
                raise AssertionError("expected follower to reject the write")
            except AssertionError:
                raise
            except Exception as e:
                assert "Not leader" in str(e)
        finally:
            await stop_all(nodes, tasks)


async def test_leader_failover() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        nodes = [make_node(i, 3, Path(tmp), base_port=8130) for i in range(3)]
        tasks = await start_all(nodes)
        try:
            leader = await wait_for_leader(nodes)
            await leader.submit_command({"op": "SET", "key": "z", "value": "before"})

            # Simulate a crash: tear down the leader's server + connections.
            await leader.stop()
            tasks[nodes.index(leader)].cancel()

            remaining = [n for n in nodes if n is not leader]
            new_leader = await wait_for_leader(remaining, timeout=5.0)
            assert new_leader.node_id != leader.node_id

            get_result = await new_leader.submit_command({"op": "GET", "key": "z"})
            assert get_result["result"] == "before"
        finally:
            await stop_all(nodes, tasks)


async def test_log_persistence_across_restart() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "solo.db")
        addr = "127.0.0.1:8140"

        node = RaftNode(node_id=addr, peers=[], state_machine=KeyValueStateMachine(), db_path=db_path)
        task = asyncio.create_task(node.start())
        await wait_for_leader([node])
        await node.submit_command({"op": "SET", "key": "a", "value": "1"})
        await node.submit_command({"op": "SET", "key": "b", "value": "2"})
        log_length_before = node.log.last_index()

        await node.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        # Restart: fresh RaftNode + fresh state machine, same db file.
        node2 = RaftNode(node_id=addr, peers=[], state_machine=KeyValueStateMachine(), db_path=db_path)
        assert node2.log.last_index() == log_length_before, "log entries were not persisted across restart"

        task2 = asyncio.create_task(node2.start())
        try:
            await wait_for_leader([node2])
            # commit_index/last_applied are volatile and reset to 0 on restart
            # (correct per Raft: a leader can't commit entries from a prior
            # term on log length alone). Submitting one command in the new
            # term indirectly commits everything before it, replaying the
            # whole persisted log into the fresh state machine in one shot.
            await node2.submit_command({"op": "SET", "key": "c", "value": "3"})
            assert node2.state_machine.data.get("a") == "1"
            assert node2.state_machine.data.get("b") == "2"
            assert node2.state_machine.data.get("c") == "3"
        finally:
            await node2.stop()
            task2.cancel()
            await asyncio.gather(task2, return_exceptions=True)


TESTS = [
    test_election,
    test_command_replication,
    test_follower_rejects_write,
    test_leader_failover,
    test_log_persistence_across_restart,
]


async def main() -> None:
    failures = []
    for test in TESTS:
        print(f"{test.__name__} ... ", end="", flush=True)
        try:
            await test()
        except Exception as e:
            print(f"FAIL: {e}")
            failures.append(test.__name__)
        else:
            print("ok")

    if failures:
        print(f"\n{len(failures)}/{len(TESTS)} tests failed: {', '.join(failures)}")
        sys.exit(1)

    print(f"\nAll {len(TESTS)} tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
