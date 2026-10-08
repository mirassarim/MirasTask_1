"""Counter replica (Parts A and B).

One process = one replica with its own state. Run:
    python3 server.py --port 50051 --name replica-A

The shared state (_values and _seen) is protected by ONE lock (self._lock).
The deduplication check and the mutation happen inside the same critical
section, so two concurrent retries with the same idempotency key cannot both
pass the check.

Part B: every event is stamped with a Lamport clock and logged.
"""
import argparse
import threading
import time
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import Process


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica", log_path=None, echo=False,
                 delay_ms=0, delay_first_n=None):
        self._lock = threading.Lock()
        self._values = {}  # counter_id -> int
        self._seen = {}    # idempotency_key -> (counter_id, resulting value)

        self._proc = Process(name, log_path=log_path, echo=echo)

        # Test helper: artificially slow responses (used by timeout tests).
        # delay_first_n=None -> delay every request; N -> delay only the first N.
        self._delay_ms = delay_ms
        self._delay_left = delay_first_n
        self._delay_lock = threading.Lock()

    def _maybe_delay(self):
        if self._delay_ms <= 0:
            return
        with self._delay_lock:
            if self._delay_left is not None:
                if self._delay_left <= 0:
                    return
                self._delay_left -= 1
        time.sleep(self._delay_ms / 1000.0)

    def Increment(self, request, context):
        clock, log = self._proc.clock, self._proc.log
        desc = (f"Increment(counter={request.counter_id}, "
                f"delta={request.delta})")

        # Rule 2: on receive, L = max(own, received) + 1.
        l_recv = clock.receive(request.lamport_time)
        log.recv(desc, l_recv, request.lamport_time)

        # The delay happens BEFORE the critical section, so a slow request does
        # not block other clients, and the mutation still happens (the client
        # may already have given up -> exactly the "reply lost" situation).
        self._maybe_delay()

        with self._lock:
            key = request.idempotency_key
            if key in self._seen:
                _, stored_value = self._seen[key]
                l_dup = clock.tick()
                log.local("DEDUP", f"key={key} -> {stored_value}", l_dup)
                reply = counter_pb2.IncrementReply(
                    new_value=stored_value, was_duplicate=True
                )
            else:
                new_value = self._values.get(request.counter_id, 0) + request.delta
                self._values[request.counter_id] = new_value
                self._seen[key] = (request.counter_id, new_value)
                l_apply = clock.tick()  # Rule 1: local event
                log.local("APPLY",
                          f"counter={request.counter_id} -> {new_value}",
                          l_apply)
                reply = counter_pb2.IncrementReply(
                    new_value=new_value, was_duplicate=False
                )

        # Rule 1 + 3: tick before sending, attach the value.
        reply.lamport_time = clock.tick()
        log.send(f"IncrementReply(new_value={reply.new_value})",
                 reply.lamport_time)
        return reply

    def Get(self, request, context):
        clock, log = self._proc.clock, self._proc.log
        l_recv = clock.receive(request.lamport_time)
        log.recv(f"Get(counter={request.counter_id})", l_recv,
                 request.lamport_time)

        with self._lock:
            if request.counter_id in self._values:
                reply = counter_pb2.GetReply(
                    value=self._values[request.counter_id], found=True
                )
            else:
                reply = counter_pb2.GetReply(value=0, found=False)

        reply.lamport_time = clock.tick()
        log.send(f"GetReply(value={reply.value}, found={reply.found})",
                 reply.lamport_time)
        return reply


def make_server(port=0, name="replica", log_path=None, echo=False,
                delay_ms=0, delay_first_n=None, max_workers=8):
    """Create (but do not start) a server. port=0 -> ephemeral port.

    Returns (server, bound_port). Tests use this to start replicas on
    ephemeral ports inside fixtures.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(name=name, log_path=log_path, echo=echo,
                        delay_ms=delay_ms, delay_first_n=delay_first_n),
        server,
    )
    bound_port = server.add_insecure_port(f"127.0.0.1:{port}")
    return server, bound_port


def main():
    parser = argparse.ArgumentParser(description="Counter replica")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="replica-A",
                        help="process name used in the event log")
    parser.add_argument("--log-file", default=None,
                        help="append event log here (default: stdout)")
    parser.add_argument("--delay-ms", type=int, default=0,
                        help="artificial delay before handling Increment")
    parser.add_argument("--delay-first-n", type=int, default=None,
                        help="apply --delay-ms only to the first N requests")
    args = parser.parse_args()

    server, port = make_server(
        args.port, name=args.name, log_path=args.log_file,
        echo=args.log_file is None,
        delay_ms=args.delay_ms, delay_first_n=args.delay_first_n,
    )
    server.start()
    print(f"{args.name} listening on 127.0.0.1:{port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=0)


if __name__ == "__main__":
    main()
