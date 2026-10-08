"""Task B2 scenario: produces the event logs used in the ordering analysis.

    python tests/run_scenario_b2.py

client-1 increments counter x twice; concurrently client-2 increments
counter y twice; client-3 only issues Get calls. One replica (replica-A).
Each process writes its own log file into logs/ (not merged: interleaving
differs on every run, which is the point of the exercise).
"""
import os
import sys
import threading
import time

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, ROOT)

from client import CounterClient  # noqa: E402
from server import make_server  # noqa: E402

LOG_DIR = os.path.join(ROOT, "logs")


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    names = ["replica-A", "client-1", "client-2", "client-3"]
    paths = {n: os.path.join(LOG_DIR, f"{n}.log") for n in names}
    for p in paths.values():  # fresh logs for every run
        if os.path.exists(p):
            os.remove(p)

    server, port = make_server(0, name="replica-A", log_path=paths["replica-A"])
    server.start()
    addr = f"127.0.0.1:{port}"

    c1 = CounterClient(addr, name="client-1", log_path=paths["client-1"])
    c2 = CounterClient(addr, name="client-2", log_path=paths["client-2"])
    c3 = CounterClient(addr, name="client-3", log_path=paths["client-3"])

    def run_incr(client, counter):
        for _ in range(2):
            client.incr(counter, 1)

    def run_gets():
        for counter in ("x", "y", "x", "y"):
            c3.get(counter)
            time.sleep(0.01)

    threads = [
        threading.Thread(target=run_incr, args=(c1, "x")),
        threading.Thread(target=run_incr, args=(c2, "y")),
        threading.Thread(target=run_gets),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for c in (c1, c2, c3):
        c.close()
    server.stop(grace=0)
    server.wait_for_termination()

    for n in names:
        print(f"===== {n} =====")
        with open(paths[n], encoding="utf-8") as f:
            print(f.read(), end="")


if __name__ == "__main__":
    main()
