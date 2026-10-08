"""Counter client (Parts A, B and C).

Single replica:
    python3 client.py incr likes:post-42 --by 5
    python3 client.py incr likes:post-42 --by 5 --key <same-key>   # duplicate
    python3 client.py get  likes:post-42

Three replicas, majority commit (Part C):
    python3 client.py --addrs 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053 incr likes:post-42 --by 1

Failure handling: every call has a deadline; on DEADLINE_EXCEEDED or
UNAVAILABLE the call is retried at most 3 times with exponential backoff,
ALWAYS reusing the same idempotency key (generated once per logical operation).

Part B: every SEND/RECV is stamped with a Lamport clock and logged.
Part C: QuorumClient sends each Increment to all replicas and reports it as
committed only when a majority (2 of 3) acknowledge.
"""
import argparse
import sys
import time
import uuid
from concurrent import futures
from dataclasses import dataclass, field
from typing import List, Optional

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import Process

DEADLINE_S = 2.0
MAX_RETRIES = 3
BACKOFF_S = (0.2, 0.4, 0.8)
RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


class CounterClient:
    """Client for ONE replica."""

    def __init__(self, target, deadline=DEADLINE_S, name="client-1",
                 log_path=None, echo=False, process=None):
        self._channel = grpc.insecure_channel(target)
        self._stub = counter_pb2_grpc.CounterStub(self._channel)
        self._deadline = deadline
        # A QuorumClient passes its own Process so that all its replica
        # connections share ONE Lamport clock and ONE log.
        self._owns_proc = process is None
        self._proc = process or Process(name, log_path=log_path, echo=echo)

    def incr(self, counter_id, delta, key=None):
        clock, log = self._proc.clock, self._proc.log
        # One key per logical operation, NOT per attempt.
        key = key or str(uuid.uuid4())
        desc = f"Increment(counter={counter_id}, delta={delta})"
        attempt = 0
        while True:
            # Each attempt is a new SEND event (new L) but the SAME key.
            l_send = clock.tick()
            request = counter_pb2.IncrementRequest(
                counter_id=counter_id, delta=delta, idempotency_key=key,
                lamport_time=l_send,
            )
            log.send(desc, l_send)
            try:
                reply = self._stub.Increment(request, timeout=self._deadline)
            except grpc.RpcError as err:
                if err.code() not in RETRYABLE or attempt >= MAX_RETRIES:
                    raise
                time.sleep(BACKOFF_S[attempt])
                attempt += 1
                continue
            l_recv = clock.receive(reply.lamport_time)
            log.recv(f"IncrementReply(new_value={reply.new_value})",
                     l_recv, reply.lamport_time)
            return reply

    def get(self, counter_id):
        clock, log = self._proc.clock, self._proc.log
        l_send = clock.tick()
        request = counter_pb2.GetRequest(counter_id=counter_id,
                                         lamport_time=l_send)
        log.send(f"Get(counter={counter_id})", l_send)
        reply = self._stub.Get(request, timeout=self._deadline)
        l_recv = clock.receive(reply.lamport_time)
        log.recv(f"GetReply(value={reply.value}, found={reply.found})",
                 l_recv, reply.lamport_time)
        return reply

    def close(self):
        self._channel.close()
        if self._owns_proc:
            self._proc.log.close()


@dataclass
class QuorumResult:
    committed: bool          # True only if acks >= majority
    value: Optional[int]     # value reported by the replicas that acked
    acks: int                # replicas that acknowledged
    total: int               # replicas contacted
    duplicate: bool          # every acking replica said was_duplicate
    errors: List = field(default_factory=list)  # one RpcError per failed replica


class QuorumClient:
    """Sends every Increment to ALL replicas; commit = majority acknowledged.

    No leader, no log shipping: replicas receive writes independently, and the
    per-replica dedup store plus the idempotency key keep them convergent.
    The call waits for every replica to answer or exhaust its retries, which
    keeps behaviour deterministic (all live replicas have applied the write
    when incr() returns).
    """

    def __init__(self, targets, deadline=DEADLINE_S, name="client-1",
                 log_path=None, echo=False):
        self._proc = Process(name, log_path=log_path, echo=echo)
        self._replicas = [
            CounterClient(t, deadline=deadline, process=self._proc)
            for t in targets
        ]
        self._majority = len(self._replicas) // 2 + 1
        self._pool = futures.ThreadPoolExecutor(max_workers=len(self._replicas))

    @property
    def majority(self):
        return self._majority

    def incr(self, counter_id, delta, key=None):
        # ONE key for the whole logical operation, shared by every replica
        # and every retry.
        key = key or str(uuid.uuid4())

        def call(replica):
            try:
                return replica.incr(counter_id, delta, key=key), None
            except grpc.RpcError as err:
                return None, err

        outcomes = list(self._pool.map(call, self._replicas))
        replies = [r for r, _ in outcomes if r is not None]
        errors = [e for _, e in outcomes if e is not None]

        acks = len(replies)
        value = None
        if replies:
            values = [r.new_value for r in replies]
            value = max(set(values), key=lambda v: (values.count(v), v))
        return QuorumResult(
            committed=acks >= self._majority,
            value=value,
            acks=acks,
            total=len(self._replicas),
            duplicate=bool(replies) and all(r.was_duplicate for r in replies),
            errors=errors,
        )

    def get(self, counter_id):
        """Read from ANY single replica: the first one that answers.

        Consistency risk: that replica may have missed a recent committed
        write (stale read) or hold a write that never reached a majority.
        """
        last_error = None
        for replica in self._replicas:
            try:
                return replica.get(counter_id)
            except grpc.RpcError as err:
                last_error = err
        raise last_error

    def close(self):
        self._pool.shutdown(wait=False)
        for replica in self._replicas:
            replica.close()
        self._proc.log.close()


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--addr", default="127.0.0.1:50051",
                        help="single replica address")
    parser.add_argument("--addrs", default=None,
                        help="comma-separated replica addresses "
                             "(enables quorum mode)")
    parser.add_argument("--name", default="client-1",
                        help="process name used in the event log")
    parser.add_argument("--log-file", default=None,
                        help="append event log here (default: stdout)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_incr = sub.add_parser("incr")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None,
                        help="idempotency key (default: random uuid4)")

    p_get = sub.add_parser("get")
    p_get.add_argument("counter_id")

    args = parser.parse_args()
    echo = args.log_file is None
    quorum = args.addrs is not None
    if quorum:
        client = QuorumClient(args.addrs.split(","), name=args.name,
                              log_path=args.log_file, echo=echo)
    else:
        client = CounterClient(args.addr, name=args.name,
                               log_path=args.log_file, echo=echo)
    try:
        if args.cmd == "incr":
            r = client.incr(args.counter_id, args.by, key=args.key)
            if quorum:
                if r.committed:
                    dup = "yes" if r.duplicate else "no"
                    print(f"OK committed value={r.value} "
                          f"(replicas acked: {r.acks}/{r.total}, "
                          f"duplicate: {dup})")
                else:
                    print(f"FAILED not committed "
                          f"(replicas acked: {r.acks}/{r.total}, "
                          f"majority is {client.majority})", file=sys.stderr)
                    sys.exit(1)
            else:
                dup = "yes" if r.was_duplicate else "no"
                print(f"OK committed value={r.new_value} (duplicate: {dup})")
        else:
            r = client.get(args.counter_id)
            if r.found:
                print(f"value={r.value}")
            else:
                print("not found")
    except grpc.RpcError as err:
        print(f"FAILED: {err.code().name}: {err.details()}", file=sys.stderr)
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()