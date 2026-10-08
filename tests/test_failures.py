"""Task C3: failure-injection tests (route a: process control).

Each replica is a real OS process started with subprocess.Popen and killed
with proc.kill(). Every scenario asserts an externally observable invariant.
Event logs of every replica and of the client are kept in
logs/failures/<scenario>/ as evidence for the report.

Run:  python -m pytest tests/test_failures.py -v
"""
import os
import shutil
import socket
import subprocess
import sys
import threading
import time

import grpc
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from client import CounterClient, QuorumClient  # noqa: E402

EVIDENCE_DIR = os.path.join(ROOT, "logs", "failures")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ProcCluster:
    """Three replica processes plus a quorum client."""

    def __init__(self, scenario, extra_args=None):
        extra_args = extra_args or {}
        self.dir = os.path.join(EVIDENCE_DIR, scenario)
        shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir)

        self.names = ["replica-A", "replica-B", "replica-C"]
        self.ports = [_free_port() for _ in self.names]
        self.addrs = [f"127.0.0.1:{p}" for p in self.ports]
        self.procs = []
        self._direct = []
        self.client = None

        for i, name in enumerate(self.names):
            cmd = [
                sys.executable, os.path.join(ROOT, "server.py"),
                "--port", str(self.ports[i]), "--name", name,
                "--log-file", os.path.join(self.dir, f"{name}.log"),
            ] + extra_args.get(i, [])
            self.procs.append(subprocess.Popen(
                cmd, cwd=ROOT,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            ))
        for addr in self.addrs:
            channel = grpc.insecure_channel(addr)
            grpc.channel_ready_future(channel).result(timeout=20)
            channel.close()

    def make_client(self, deadline=2.0):
        self.client = QuorumClient(
            self.addrs, deadline=deadline, name="client-F",
            log_path=os.path.join(self.dir, "client-F.log"),
        )
        return self.client

    def kill(self, index):
        self.procs[index].kill()
        self.procs[index].wait(timeout=10)

    def value_on(self, index, counter_id):
        direct = CounterClient(self.addrs[index], name="inspector")
        self._direct.append(direct)
        reply = direct.get(counter_id)
        return reply.value if reply.found else None

    def close(self):
        if self.client:
            self.client.close()
        for direct in self._direct:
            direct.close()
        for proc in self.procs:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=10)


@pytest.fixture
def make_cluster():
    created = []

    def factory(scenario, extra_args=None):
        cluster = ProcCluster(scenario, extra_args)
        created.append(cluster)
        return cluster

    yield factory
    for cluster in created:
        cluster.close()


def test_replica_crash_mid_request(make_cluster):
    """Invariant: killing one replica while a write is in flight does not stop
    the commit (2 remaining acks) and no exception reaches the caller."""
    # replica-B holds every request for 1.5 s, so the write is in flight
    cluster = make_cluster("crash_mid_request",
                           extra_args={1: ["--delay-ms", "1500"]})
    client = cluster.make_client()
    outcome = {}

    def write():
        try:
            outcome["result"] = client.incr("x", 1)
        except Exception as exc:  # noqa: BLE001
            outcome["error"] = exc

    writer = threading.Thread(target=write)
    writer.start()
    time.sleep(0.3)
    cluster.kill(1)  # crash replica-B mid-request
    writer.join(timeout=30)

    assert not writer.is_alive()
    assert "error" not in outcome  # nothing escaped to the caller
    result = outcome["result"]
    assert result.committed is True
    assert result.acks == 2
    assert cluster.value_on(0, "x") == 1
    assert cluster.value_on(2, "x") == 1


def test_request_duplication(make_cluster):
    """Invariant: the same key sent twice moves the value once and the second
    response carries was_duplicate = true."""
    cluster = make_cluster("request_duplication")
    client = cluster.make_client()

    first = client.incr("x", 1, key="dup-key")
    second = client.incr("x", 1, key="dup-key")

    assert first.committed and first.duplicate is False
    assert second.committed and second.duplicate is True
    assert second.value == 1
    assert [cluster.value_on(i, "x") for i in range(3)] == [1, 1, 1]


def test_induced_timeout_with_retry(make_cluster):
    """Invariant: when a replica answers slower than the client deadline, the
    retry succeeds and the counter moves exactly once on every replica."""
    # replica-B delays only its FIRST request, by 1.5 s (> 0.5 s deadline)
    cluster = make_cluster(
        "induced_timeout_retry",
        extra_args={1: ["--delay-ms", "1500", "--delay-first-n", "1"]},
    )
    client = cluster.make_client(deadline=0.5)

    result = client.incr("x", 1, key="timeout-key")
    time.sleep(1.8)  # let the slow first attempt on replica-B finish

    assert result.committed is True
    assert result.acks == 3  # the retry to replica-B succeeded
    assert [cluster.value_on(i, "x") for i in range(3)] == [1, 1, 1]


def test_two_crashes_below_majority(make_cluster):
    """Invariant: with two of three replicas dead the write is reported as
    NOT committed (below majority)."""
    cluster = make_cluster("below_majority")
    client = cluster.make_client()
    cluster.kill(1)
    cluster.kill(2)

    result = client.incr("x", 1)

    assert result.committed is False
    assert result.acks == 1
