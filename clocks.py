"""Lamport logical clock and event log (Part B).

Rules (Lamport 1978):
  1. Before every local event (send, apply) increment your own counter.
  2. On receiving a message set counter = max(own, received) + 1,
     then process the message.
  3. Attach the counter value to every outgoing message.

Each process (client, replica) owns ONE LamportClock and ONE EventLog.
Log lines have exactly this format:
  [client-1]  SEND Increment(counter=likes:post-42, delta=1) L=4
  [replica-A] RECV Increment(counter=likes:post-42, delta=1) L=5 (received L=4)
"""
import sys
import threading


class LamportClock:
    """Thread-safe Lamport counter."""

    def __init__(self, start=0):
        self._time = start
        self._lock = threading.Lock()

    def tick(self):
        """Local event (send, apply): increment and return the new value."""
        with self._lock:
            self._time += 1
            return self._time

    def receive(self, received):
        """Message receipt: set to max(own, received) + 1 and return it."""
        with self._lock:
            self._time = max(self._time, received) + 1
            return self._time

    @property
    def time(self):
        with self._lock:
            return self._time


class EventLog:
    """Writes one line per event in the assignment's exact format.

    path   -> append lines to this file (None: no file)
    stream -> also write to this stream, e.g. sys.stdout (None: no echo)
    """

    def __init__(self, name, path=None, stream=None):
        self.name = name
        self._stream = stream
        self._file = open(path, "a", encoding="utf-8") if path else None
        self._lock = threading.Lock()

    def _write(self, line):
        with self._lock:
            if self._file:
                self._file.write(line + "\n")
                self._file.flush()
            if self._stream:
                print(line, file=self._stream, flush=True)

    def send(self, description, lamport):
        self._write(f"[{self.name}] SEND {description} L={lamport}")

    def recv(self, description, lamport, received):
        self._write(
            f"[{self.name}] RECV {description} L={lamport} (received L={received})"
        )

    def local(self, kind, description, lamport):
        """Local event such as APPLY or DEDUP."""
        self._write(f"[{self.name}] {kind} {description} L={lamport}")

    def close(self):
        if self._file:
            self._file.close()
            self._file = None


class Process:
    """A clock plus a log: what every client and replica holds."""

    def __init__(self, name, log_path=None, echo=False):
        self.name = name
        self.clock = LamportClock()
        self.log = EventLog(name, log_path, sys.stdout if echo else None)
