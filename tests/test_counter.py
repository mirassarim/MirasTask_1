"""Task C2: unit and integration tests.

Servers are started on ephemeral ports inside fixtures and shut down
afterwards. No manual steps. Every test: arrange - act - assert.

Run:  python -m pytest tests/test_counter.py -v
"""
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from client import CounterClient, QuorumClient  # noqa: E402
from server import make_server  # noqa: E402


# --------------------------------------------------------------------------
# Fixtures / helpers
# --------------------------------------------------------------------------
class RunningServer:
    """One replica on an ephemeral port, started in-process."""

    def __init__(self, name="replica-T", delay_ms=0, delay_first_n=None):
        self.server, self.port = make_server(
            0, name=name, delay_ms=delay_ms, delay_first_n=delay_first_n
        )
        self.server.start()
        self.addr = f"127.0.0.1:{self.port}"
        self._clients = []
        self._stopped = False

    def new_client(self, deadline=2.0, name="client-T"):
        client = CounterClient(self.addr, deadline=deadline, name=name)
        self._clients.append(client)
        return client

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        for client in self._clients:
            client.close()
        self.server.stop(grace=0).wait()


class Cluster:
    """Three independent replicas plus a quorum client."""

    def __init__(self):
        self.replicas = [RunningServer(name=f"replica-{c}") for c in "ABC"]
        self.client = QuorumClient([r.addr for r in self.replicas],
                                   name="client-Q")
        self._direct = []

    def stop_replica(self, index):
        self.replicas[index].stop()

    def value_on(self, index, counter_id):
        """Read straight from ONE replica, bypassing the quorum client."""
        direct = CounterClient(self.replicas[index].addr, name="inspector")
        self._direct.append(direct)
        reply = direct.get(counter_id)
        return reply.value if reply.found else None

    def stop(self):
        self.client.close()
        for direct in self._direct:
            direct.close()
        for replica in self.replicas:
            replica.stop()


@pytest.fixture
def running_server():
    srv = RunningServer()
    yield srv
    srv.stop()


@pytest.fixture
def cluster():
    c = Cluster()
    yield c
    c.stop()


# --------------------------------------------------------------------------
# Mandated tests (Task C2)
# --------------------------------------------------------------------------
def test_increment_applies_delta(running_server):
    client = running_server.new_client()

    reply = client.incr("x", 7, key="k-delta")

    assert reply.new_value == 7
    assert reply.was_duplicate is False
    assert client.get("x").value == 7


def test_duplicate_key_not_reapplied(running_server):
    client = running_server.new_client()

    r1 = client.incr("x", 5, key="k-1")
    r2 = client.incr("x", 5, key="k-1")  # retry with the same key

    assert r1.new_value == 5
    assert r2.new_value == 5 and r2.was_duplicate  # not 10
    assert client.get("x").value == 5


def test_concurrent_increments_exact(running_server):
    clients = [running_server.new_client(name=f"client-{i}") for i in (1, 2)]
    errors = []

    def worker(client):
        try:
            for _ in range(1000):
                client.incr("x", 1)  # fresh uuid4 key for each call
        except Exception as exc:  # noqa: BLE001 - report in the main thread
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(c,)) for c in clients]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert clients[0].get("x").value == 2000


def test_get_missing_counter(running_server):
    client = running_server.new_client()

    reply = client.get("does-not-exist")

    assert reply.found is False
    assert reply.value == 0


def test_retry_after_timeout_is_safe():
    # Arrange: the first request is delayed 1.5 s, the client deadline is 0.5 s.
    srv = RunningServer(delay_ms=1500, delay_first_n=1)
    try:
        client = srv.new_client(deadline=0.5)

        # Act: attempt 1 times out, the retry (same key) succeeds.
        reply = client.incr("x", 1, key="k-timeout")
        time.sleep(1.8)  # let the slow first attempt finish on the server

        # Assert: the counter moved exactly once.
        assert reply.new_value == 1
        reader = srv.new_client(deadline=2.0, name="reader")
        assert reader.get("x").value == 1
    finally:
        srv.stop()


def test_majority_commit_two_acks(cluster):
    cluster.stop_replica(2)  # one replica down

    result = cluster.client.incr("x", 1)

    assert result.committed is True
    assert result.acks == 2 and result.total == 3
    assert result.value == 1


def test_no_commit_below_majority(cluster):
    cluster.stop_replica(1)
    cluster.stop_replica(2)  # two replicas down

    result = cluster.client.incr("x", 1)

    assert result.committed is False  # NOT presented as committed
    assert result.acks == 1


def test_replicas_converge(cluster):
    keys = ["a", "b", "c"]

    for i in range(30):
        result = cluster.client.incr(keys[i % 3], 1)
        assert result.committed is True

    for key in keys:
        values = [cluster.value_on(i, key) for i in range(3)]
        assert values == [10, 10, 10]


# --------------------------------------------------------------------------
# Self-designed edge-case tests
# --------------------------------------------------------------------------
def test_counter_ids_are_isolated(running_server):
    client = running_server.new_client()

    client.incr("x", 3)
    client.incr("y", 10)

    assert client.get("x").value == 3
    assert client.get("y").value == 10


def test_same_key_through_quorum_is_applied_once(cluster):
    r1 = cluster.client.incr("x", 5, key="k-q")
    r2 = cluster.client.incr("x", 5, key="k-q")

    assert r1.committed and r2.committed
    assert r1.duplicate is False
    assert r2.duplicate is True  # every replica recognised the key
    assert [cluster.value_on(i, "x") for i in range(3)] == [5, 5, 5]


def test_failed_write_may_partially_apply(cluster):
    """Documents the anomaly from Task C1: below majority the client reports
    failure, yet the surviving replica DID apply the write."""
    cluster.stop_replica(1)
    cluster.stop_replica(2)

    result = cluster.client.incr("x", 1)

    assert result.committed is False
    assert cluster.value_on(0, "x") == 1  # applied on the survivor anyway


def test_negative_delta_decrements(running_server):
    client = running_server.new_client()

    client.incr("x", 10)
    reply = client.incr("x", -4)

    assert reply.new_value == 6