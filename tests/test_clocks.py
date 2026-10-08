"""Unit tests for the Lamport clock and event log (Task B1). Pure Python."""
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from clocks import EventLog, LamportClock  # noqa: E402


def test_tick_increments_by_one():
    clock = LamportClock()
    assert clock.tick() == 1
    assert clock.tick() == 2


def test_receive_takes_max_plus_one_when_remote_is_ahead():
    clock = LamportClock(start=3)
    assert clock.receive(10) == 11


def test_receive_still_increments_when_remote_is_behind():
    clock = LamportClock(start=7)
    assert clock.receive(2) == 8  # max(7, 2) + 1


def test_clock_never_goes_backwards_under_threads():
    clock = LamportClock()

    def work():
        for _ in range(1000):
            clock.tick()

    threads = [threading.Thread(target=work) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert clock.time == 4000


def test_log_line_formats_match_assignment(tmp_path):
    path = tmp_path / "events.log"
    log = EventLog("replica-A", str(path))
    log.recv("Increment(counter=likes:post-42, delta=1)", 5, 4)
    log.local("APPLY", "counter=likes:post-42 -> 12", 6)
    log.send("IncrementReply(new_value=12)", 7)
    log.close()
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines == [
        "[replica-A] RECV Increment(counter=likes:post-42, delta=1) L=5 (received L=4)",
        "[replica-A] APPLY counter=likes:post-42 -> 12 L=6",
        "[replica-A] SEND IncrementReply(new_value=12) L=7",
    ]
