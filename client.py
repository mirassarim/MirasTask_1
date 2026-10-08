"""Counter client (Parts A and B).

    python3 client.py incr likes:post-42 --by 5
    python3 client.py incr likes:post-42 --by 5 --key <same-key>   # duplicate
    python3 client.py get  likes:post-42

Failure handling: every call has a deadline; on DEADLINE_EXCEEDED or
UNAVAILABLE the call is retried at most 3 times with exponential backoff,
ALWAYS reusing the same idempotency key (generated once per logical operation).

Part B: every SEND/RECV is stamped with a Lamport clock and logged.
"""
import argparse
import sys
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import Process

DEADLINE_S = 2.0
MAX_RETRIES = 3
BACKOFF_S = (0.2, 0.4, 0.8)
RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


class CounterClient:
    def __init__(self, target, deadline=DEADLINE_S, name="client-1",
                 log_path=None, echo=False):
        self._channel = grpc.insecure_channel(target)
        self._stub = counter_pb2_grpc.CounterStub(self._channel)
        self._deadline = deadline
        self._proc = Process(name, log_path=log_path, echo=echo)

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
        self._proc.log.close()


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--addr", default="127.0.0.1:50051")
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
    client = CounterClient(args.addr, name=args.name, log_path=args.log_file,
                           echo=args.log_file is None)
    try:
        if args.cmd == "incr":
            r = client.incr(args.counter_id, args.by, key=args.key)
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
