"""Task C4: Increment latency, single replica vs three-replica quorum.

    python tests/perf_benchmark.py                 # 3 runs x 2000 requests
    python tests/perf_benchmark.py --runs 1        # quick check

Method
  * Replicas are separate OS processes (no GIL sharing with the load clients).
    Their event logs go to the null device, so logging cost is included but
    no disk growth.
  * 4 configurations: {1 replica, 3-replica quorum} x {1, 16} concurrent clients.
  * Each run issues >= 2000 Increment calls per configuration; each client
    thread has its own channel and does a warm-up (not recorded) first.
  * Latency = wall time of one client.incr() call, measured with
    time.perf_counter(). For the quorum path this includes the fan-out to all
    three replicas and waiting for every replica to answer.
  * Median = statistics.median. p95 = the ceil(0.95 * n)-th smallest value of
    the sorted latencies (nearest-rank method; index ceil(0.95*n) - 1 in
    Python's 0-based indexing).
  * --runs repeats every configuration; the main table pools all runs, and a
    second table shows the run-to-run variance and throughput.
Results are printed and saved to logs/perf_results.md.
"""
import argparse
import math
import os
import platform
import socket
import statistics
import subprocess
import sys
import threading
import time

import grpc

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from client import CounterClient, QuorumClient  # noqa: E402

WARMUP_REQUESTS = 20

CONFIGS = [
    ("Single replica, 1 client", 1, 1),
    ("Single replica, 16 clients", 1, 16),
    ("Quorum (3 replicas), 1 client", 3, 1),
    ("Quorum (3 replicas), 16 clients", 3, 16),
]


def p95(values):
    ordered = sorted(values)
    return ordered[math.ceil(0.95 * len(ordered)) - 1]


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_replicas(count):
    procs, addrs = [], []
    for i in range(count):
        port = _free_port()
        cmd = [
            sys.executable, os.path.join(ROOT, "server.py"),
            "--port", str(port), "--name", f"bench-replica-{i}",
            "--log-file", os.devnull,
        ]
        procs.append(subprocess.Popen(
            cmd, cwd=ROOT,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        ))
        addrs.append(f"127.0.0.1:{port}")
    for addr in addrs:
        channel = grpc.insecure_channel(addr)
        grpc.channel_ready_future(channel).result(timeout=20)
        channel.close()
    return procs, addrs


def stop_replicas(procs):
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)


def run_load(addrs, n_clients, total_requests):
    """One run. Returns (latencies_ms, wall_seconds, failures)."""
    per_client = math.ceil(total_requests / n_clients)
    barrier = threading.Barrier(n_clients + 1)
    results = [None] * n_clients
    errors = []

    def make_client(i):
        if len(addrs) == 1:
            return CounterClient(addrs[0], name=f"bench-client-{i}")
        return QuorumClient(addrs, name=f"bench-client-{i}")

    def worker(i):
        client = make_client(i)
        latencies, failures = [], 0
        try:
            for _ in range(WARMUP_REQUESTS):
                client.incr("warmup", 1)
            barrier.wait()
            for _ in range(per_client):
                t0 = time.perf_counter()
                r = client.incr("bench", 1)
                latencies.append((time.perf_counter() - t0) * 1000.0)
                if hasattr(r, "committed") and not r.committed:
                    failures += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
            barrier.abort()
        finally:
            client.close()
        results[i] = (latencies, failures)

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(n_clients)]
    for t in threads:
        t.start()
    barrier.wait()
    start = time.perf_counter()
    for t in threads:
        t.join()
    wall = time.perf_counter() - start

    if errors:
        raise RuntimeError(f"benchmark worker failed: {errors[0]!r}")
    latencies = [x for r in results for x in r[0]]
    failures = sum(r[1] for r in results)
    return latencies, wall, failures


def main():
    parser = argparse.ArgumentParser(description="Increment latency benchmark")
    parser.add_argument("--requests", type=int, default=2000,
                        help="Increment calls per configuration per run")
    parser.add_argument("--runs", type=int, default=3,
                        help="repetitions of every configuration")
    args = parser.parse_args()

    summary = []   # (label, pooled latencies, per-run stats)
    for label, n_replicas, n_clients in CONFIGS:
        print(f"-> {label} ...", flush=True)
        procs, addrs = start_replicas(n_replicas)
        pooled, per_run = [], []
        try:
            for run in range(args.runs):
                lat, wall, failures = run_load(addrs, n_clients, args.requests)
                if failures:
                    print(f"   WARNING: {failures} uncommitted writes",
                          flush=True)
                pooled.extend(lat)
                per_run.append((statistics.median(lat), p95(lat),
                                len(lat) / wall))
                print(f"   run {run + 1}: median={per_run[-1][0]:.2f} ms "
                      f"p95={per_run[-1][1]:.2f} ms "
                      f"throughput={per_run[-1][2]:.0f} req/s", flush=True)
        finally:
            stop_replicas(procs)
        summary.append((label, pooled, per_run))

    lines = []
    lines.append("## Increment latency\n")
    lines.append(f"{args.runs} run(s) x >= {args.requests} requests per "
                 "configuration, all runs pooled.\n")
    lines.append("| Configuration | Median latency (ms) | p95 latency (ms) "
                 "| Requests |")
    lines.append("|---|---|---|---|")
    for label, pooled, _ in summary:
        lines.append(f"| {label} | {statistics.median(pooled):.2f} "
                     f"| {p95(pooled):.2f} | {len(pooled)} |")

    lines.append("\n## Run-to-run variation and throughput\n")
    lines.append("| Configuration | Median ms (mean +/- sd over runs) "
                 "| p95 ms (mean +/- sd) | Throughput req/s (mean +/- sd) |")
    lines.append("|---|---|---|---|")
    for label, _, per_run in summary:
        def ms(idx):
            vals = [r[idx] for r in per_run]
            sd = statistics.pstdev(vals) if len(vals) > 1 else 0.0
            return f"{statistics.mean(vals):.2f} +/- {sd:.2f}"
        thr = [r[2] for r in per_run]
        thr_sd = statistics.pstdev(thr) if len(thr) > 1 else 0.0
        lines.append(f"| {label} | {ms(0)} | {ms(1)} "
                     f"| {statistics.mean(thr):.0f} +/- {thr_sd:.0f} |")

    lines.append("\nEnvironment: "
                 f"{platform.system()} {platform.release()}, "
                 f"Python {platform.python_version()}, "
                 f"{os.cpu_count()} logical CPUs; server max_workers=8; "
                 "client and replicas on the same machine (127.0.0.1).")

    report = "\n".join(lines)
    print("\n" + report)
    out_dir = os.path.join(ROOT, "logs")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "perf_results.md"), "w",
              encoding="utf-8") as f:
        f.write(report + "\n")
    print("\nSaved to logs/perf_results.md")


if __name__ == "__main__":
    main()
