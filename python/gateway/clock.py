"""Clocks.

All rate-limit maths uses integer nanoseconds from a monotonic clock:

- monotonic, so a wall-clock jump (NTP, daylight saving) can't shrink or stretch a window;
- integers, so "is this send older than 60 s?" is exact. With float seconds, waiting
  "exactly until the oldest send expires" can land a few ulps short of the boundary, and
  the limiter would spin.

Tests use FakeClock so a five-minute scenario runs in milliseconds and is deterministic.
"""

import time
from typing import Protocol

NS_PER_SECOND = 1_000_000_000


class Clock(Protocol):
    def now_ns(self) -> int: ...


class MonotonicClock:
    def now_ns(self) -> int:
        return time.monotonic_ns()


class FakeClock:
    def __init__(self, start_ns: int = 0) -> None:
        self._now = start_ns

    def now_ns(self) -> int:
        return self._now

    def advance(self, ns: int) -> None:
        if ns < 0:
            raise ValueError("time only moves forward")
        self._now += ns

    def advance_s(self, seconds: float) -> None:
        self.advance(round(seconds * NS_PER_SECOND))
