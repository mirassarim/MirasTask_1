# Replicated Counter Service

A small distributed system built in three stages:

1. **Part A** - gRPC counter service with idempotency keys and duplicate suppression.
2. **Part B** - Lamport logical clocks and event logs.
3. **Part C** - three replicas with majority-acknowledged writes, plus tests,
   failure-injection and a latency benchmark.

Python 3.9 or later is required. All commands below are meant to be copy-pasted
from the project root (the folder that contains `server.py`).

## 1. Setup (once)

Linux / macOS:

```bash
python3 --version
python3 -m venv .venv
source .venv/bin/activate
pip install grpcio grpcio-tools pytest
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

Windows (cmd):

```cmd
python --version
python -m venv .venv
.venv\Scripts\activate
pip install grpcio grpcio-tools pytest
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

The last command generates `counter_pb2.py` and `counter_pb2_grpc.py`
(never edit them by hand; regenerate after any change to `counter.proto`).

On the commands below use `python3` on Linux/macOS and `python` on Windows.
Every terminal needs the virtual environment activated.

## 2. Start one replica

```bash
python3 server.py --port 50051 --name replica-A
```

In a second terminal:

```bash
python3 client.py incr likes:post-42 --by 5
python3 client.py incr likes:post-42 --by 5 --key abc
python3 client.py incr likes:post-42 --by 5 --key abc    # duplicate: value does not change
python3 client.py get likes:post-42
```

Expected: `value=5`, then `value=10 (duplicate: no)`, then `value=10 (duplicate: yes)`,
then `value=10`. Every process prints its Lamport event log to the terminal
(use `--log-file path` to write it to a file instead).

## 3. Start three replicas

One terminal per replica:

```bash
python3 server.py --port 50051 --name replica-A
python3 server.py --port 50052 --name replica-B
python3 server.py --port 50053 --name replica-C
```

Client in quorum mode (a write is committed only when at least 2 of 3 replicas acknowledge):

```bash
python3 client.py --addrs 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053 incr likes:post-42 --by 1
python3 client.py --addrs 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053 get likes:post-42
```

Expected: `OK committed value=1 (replicas acked: 3/3, duplicate: no)`.
Stop one replica (Ctrl+C) and repeat: the write still commits with `2/3`.
Stop two: the client prints `FAILED not committed (replicas acked: 1/3 ...)`.

Useful replica flags for fault injection: `--delay-ms N` (delay every request
by N ms) and `--delay-first-n K` (apply the delay to the first K requests only).

## 4. Run the tests

Full suite (21 tests, about 1.5 minutes):

```bash
python3 -m pytest tests -v
```

Individual files:

```bash
python3 -m pytest tests/test_clocks.py -v      # Lamport clock unit tests (Task B1)
python3 -m pytest tests/test_counter.py -v     # unit + integration tests (Task C2)
python3 -m pytest tests/test_failures.py -v    # failure injection, replicas as processes (Task C3)
```

Tests start replicas on ephemeral ports and shut them down afterwards; no manual
steps and no servers need to be running. `test_failures.py` writes the event logs
of every scenario to `logs/failures/<scenario>/`.

## 5. Task B2 scenario (ordering analysis)

```bash
python3 tests/run_scenario_b2.py
```

client-1 increments `x` twice, client-2 increments `y` twice, client-3 only issues
`Get`. The per-process logs are written to `logs/` (`replica-A.log`,
`client-1.log`, `client-2.log`, `client-3.log`). Every run produces a different
interleaving; the report analyses one captured run.

## 6. Run the benchmark (Task C4)

```bash
python3 tests/perf_benchmark.py            # 3 runs x 2000 requests per configuration
python3 tests/perf_benchmark.py --runs 1   # quick check
```

Prints the latency table (single replica vs 3-replica quorum, 1 and 16 clients)
and saves it to `logs/perf_results.md`. Close heavy programs while it runs.

## 7. Capture the environment

```bash
pip freeze > logs/pip_freeze.txt
```

## Repository layout

```
counter.proto                  service definition (Parts A and B fields)
counter_pb2.py, counter_pb2_grpc.py   generated stubs
server.py                      replica (flags: --port, --name, --log-file, --delay-ms, --delay-first-n)
client.py                      incr / get, retries, QuorumClient (--addrs)
clocks.py                      Lamport clock and event log
tests/test_clocks.py           Lamport clock unit tests
tests/test_counter.py          Task C2 tests
tests/test_failures.py         Task C3 failure-injection tests
tests/run_scenario_b2.py       Task B2 scenario
tests/perf_benchmark.py        Task C4 measurement script
logs/                          captured event logs, failure evidence, benchmark results
report.pdf                     test report (Task C5)
```

## Design notes

* **Idempotency:** the client generates one `uuid4` key per logical operation and reuses it
  on every retry; the server checks the key and mutates the counter inside one critical
  section (one lock), so a retried request is never applied twice.
* **Quorum:** the client sends each Increment to all replicas and reports success only if a
  majority (2 of 3) acknowledges. No leader election, no log shipping. Each replica keeps
  its own state and dedup store.
* **State is in memory:** a restarted replica starts empty (see the report's limitations).
