## Increment latency

3 run(s) x >= 2000 requests per configuration, all runs pooled.

| Configuration | Median latency (ms) | p95 latency (ms) | Requests |
|---|---|---|---|
| Single replica, 1 client | 0.33 | 0.36 | 6000 |
| Single replica, 16 clients | 2.99 | 3.15 | 6000 |
| Quorum (3 replicas), 1 client | 0.59 | 0.62 | 6000 |
| Quorum (3 replicas), 16 clients | 7.42 | 10.49 | 6000 |

## Run-to-run variation and throughput

| Configuration | Median ms (mean +/- sd over runs) | p95 ms (mean +/- sd) | Throughput req/s (mean +/- sd) |
|---|---|---|---|
| Single replica, 1 client | 0.33 +/- 0.00 | 0.36 +/- 0.00 | 3010 +/- 12 |
| Single replica, 16 clients | 2.99 +/- 0.01 | 3.16 +/- 0.03 | 5336 +/- 15 |
| Quorum (3 replicas), 1 client | 0.59 +/- 0.00 | 0.62 +/- 0.00 | 1695 +/- 6 |
| Quorum (3 replicas), 16 clients | 7.07 +/- 0.85 | 10.78 +/- 1.57 | 2164 +/- 160 |

Environment: Windows 10, Python 3.10.9, 12 logical CPUs; server max_workers=8; client and replicas on the same machine (127.0.0.1).
