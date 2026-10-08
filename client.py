"""Counter client (Part A).

    python3 client.py incr likes:post-42 --by 5
    python3 client.py incr likes:post-42 --by 5 --key <same-key>   # duplicate
    python3 client.py get  likes:post-42

Failure handling: every call has a deadline; on DEADLINE_EXCEEDED or
UNAVAILABLE the call is retried at most 3 times with exponential backoff,
ALWAYS reusing the same idempotency key (generated once per logical operation).
"""
import argparse
import sys
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc

DEADLINE_S = 2.0
MAX_RETRIES = 3
BACKOFF_S = (0.2, 0.4, 0.8)
RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


class CounterClient:
    def __init__(self, target, deadline=DEADLINE_S):
        self._channel = grpc.insecure_channel(target)
        self._stub = counter_pb2_grpc.CounterStub(self._channel)
        self._deadline = deadline

    def incr(self, counter_id, delta, key=None):
        # One key per logical operation, NOT per attempt.
        key = key or str(uuid.uuid4())
        request = counter_pb2.IncrementRequest(
            counter_id=counter_id, delta=delta, idempotency_key=key
        )
        attempt = 0
        while True:
            try:
                return self._stub.Increment(request, timeout=self._deadline)
            except grpc.RpcError as err:
                if err.code() not in RETRYABLE or attempt >= MAX_RETRIES:
                    raise
                time.sleep(BACKOFF_S[attempt])
                attempt += 1  # same `request` object -> same idempotency key

    def get(self, counter_id):
        request = counter_pb2.GetRequest(counter_id=counter_id)
        return self._stub.Get(request, timeout=self._deadline)

    def close(self):
        self._channel.close()


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--addr", default="127.0.0.1:50051")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_incr = sub.add_parser("incr")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None,
                        help="idempotency key (default: random uuid4)")

    p_get = sub.add_parser("get")
    p_get.add_argument("counter_id")

    args = parser.parse_args()
    client = CounterClient(args.addr)
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
