"""Per-model rate limiter.

A request may be sent only when two independent checks agree:

1. SlidingWindow — the hard guarantee. It keeps an exact log of every send in the last
   60 seconds and refuses a send that would put the trailing window over RPM or TPM.
   This is what makes "no 60-second period exceeds the limit" true by construction.

2. Pacer — smoothness. A token bucket refilling at limit/60 per second with ~1 s of burst.
   Without it, the window alone would allow all 50k requests in the first second and then
   nothing for 59 s: legal, but bursty and hard on the provider.

Why not just one of them?
- A minute counter that resets at :00 lets 2x through across the boundary.
- A token bucket holding a full minute of credit lets bucket + refill = ~2x through in one window.
- The window alone is correct but bursty; the pacer alone is smooth but not provably correct.

Rule enforced (see docs/ARCHITECTURE.md §4.1): at every send t,
    requests in (t-60s, t] <= RPM cap   and   tokens in (t-60s, t] <= TPM cap
where the caps are the *current* limits times a utilisation target (default 0.97).
"""

import math
from collections import deque
from dataclasses import dataclass

from gateway.clock import NS_PER_SECOND, Clock

WINDOW_NS = 60 * NS_PER_SECOND


@dataclass(frozen=True)
class Limits:
    rpm: int
    tpm: int

    def __post_init__(self) -> None:
        if self.rpm < 0 or self.tpm < 0:
            raise ValueError("limits must be >= 0")


class SlidingWindow:
    """Exact record of what was sent in the trailing window."""

    def __init__(self, window_ns: int = WINDOW_NS) -> None:
        self._window_ns = window_ns
        # (sent_at_ns, requests, tokens). requests is 0 for token-only corrections.
        self._log: deque[tuple[int, int, int]] = deque()
        self.requests = 0
        self.tokens = 0

    def _expire(self, now_ns: int) -> None:
        # An entry sent at time s is inside (now-W, now] while s > now-W.
        cutoff = now_ns - self._window_ns
        log = self._log
        while log and log[0][0] <= cutoff:
            _, requests, tokens = log.popleft()
            self.requests -= requests
            self.tokens -= tokens

    def usage(self, now_ns: int) -> tuple[int, int]:
        self._expire(now_ns)
        return self.requests, self.tokens

    def record(self, now_ns: int, requests: int, tokens: int) -> None:
        self._expire(now_ns)
        self._log.append((now_ns, requests, tokens))
        self.requests += requests
        self.tokens += tokens

    def wait_ns(self, now_ns: int, tokens: int, max_requests: int, max_tokens: int) -> int | None:
        """Nanoseconds until one more request of `tokens` fits.

        0 means it fits now. None means it can never fit under these caps, even with an
        empty window (cap is 0, or the request alone is bigger than the TPM cap).
        """
        if max_requests < 1 or tokens > max_tokens:
            return None
        self._expire(now_ns)
        excess_requests = self.requests + 1 - max_requests
        excess_tokens = self.tokens + tokens - max_tokens
        if excess_requests <= 0 and excess_tokens <= 0:
            return 0

        # Walk oldest-first until enough has expired for both limits. After a limit drop
        # this can be a long walk, but it only happens while we are blocked anyway.
        freed_requests = freed_tokens = 0
        for sent_at, requests, tokens_sent in self._log:
            freed_requests += requests
            freed_tokens += tokens_sent
            if freed_requests >= excess_requests and freed_tokens >= excess_tokens:
                # At sent_at + W this entry (and everything older) has left the window.
                return sent_at + self._window_ns - now_ns
        raise AssertionError("an empty window always fits a request that passed the cap checks")


class Pacer:
    """Token bucket that spreads sends evenly. Smoothness only; the window is the guarantee."""

    def __init__(self, per_minute: float, burst_s: float, now_ns: int) -> None:
        self._burst_s = burst_s
        self._last_ns = now_ns
        self._rate_per_ns = 0.0
        self._capacity = 1.0
        self._level = 0.0
        self.set_rate(per_minute, now_ns)
        self._level = self._capacity  # start full: one burst's worth is available immediately

    def set_rate(self, per_minute: float, now_ns: int) -> None:
        self._refill(now_ns)  # credit earned so far is earned at the old rate
        self._rate_per_ns = per_minute / WINDOW_NS
        self._capacity = max(1.0, per_minute / 60 * self._burst_s)
        self._level = min(self._level, self._capacity)  # a limit drop also drops saved-up burst

    def _refill(self, now_ns: int) -> None:
        elapsed = now_ns - self._last_ns
        if elapsed > 0:
            self._level = min(self._capacity, self._level + elapsed * self._rate_per_ns)
            self._last_ns = now_ns

    def wait_ns(self, now_ns: int, cost: float) -> int | None:
        if self._rate_per_ns == 0:
            return None
        self._refill(now_ns)
        # A request bigger than the whole bucket would otherwise never pass. Let it through
        # once the bucket is full; take() then drives the level negative (debt) and the
        # following requests wait it off.
        need = min(cost, self._capacity)
        deficit = need - self._level
        if deficit <= 1e-9:
            return 0
        return max(1, math.ceil(deficit / self._rate_per_ns))

    def take(self, cost: float) -> None:
        self._level -= cost


class RateLimiter:
    """RPM + TPM limiter for one model. Not thread-safe; owned by one scheduler task."""

    def __init__(
        self,
        limits: Limits,
        clock: Clock,
        utilization: float = 0.97,
        burst_s: float = 1.0,
    ) -> None:
        if not 0 < utilization <= 1:
            raise ValueError("utilization must be in (0, 1]")
        self._clock = clock
        self._utilization = utilization
        now = clock.now_ns()
        self._window = SlidingWindow()
        self._limits = limits
        rpm_cap, tpm_cap = self.caps
        self._request_pacer = Pacer(rpm_cap, burst_s, now)
        self._token_pacer = Pacer(tpm_cap, burst_s, now)

    @property
    def limits(self) -> Limits:
        return self._limits

    @property
    def caps(self) -> tuple[int, int]:
        """The limits we actually aim for: configured limits x utilisation target."""
        return self._cap(self._limits.rpm), self._cap(self._limits.tpm)

    def _cap(self, limit: int) -> int:
        # Never round a non-zero limit down to 0, or a tiny limit would block forever.
        return max(1, math.floor(limit * self._utilization)) if limit > 0 else 0

    def set_limits(self, limits: Limits) -> None:
        """Takes effect for the very next decision. Sends already in the window still count,
        so after a drop we pause until enough old sends age out."""
        self._limits = limits
        now = self._clock.now_ns()
        rpm_cap, tpm_cap = self.caps
        self._request_pacer.set_rate(rpm_cap, now)
        self._token_pacer.set_rate(tpm_cap, now)

    def can_ever_fit(self, tokens: int) -> bool:
        rpm_cap, tpm_cap = self.caps
        return rpm_cap >= 1 and 0 < tokens <= tpm_cap

    def try_acquire(self, tokens: int) -> int | None:
        """Try to send one request of `tokens`.

        Returns 0 if granted (and records the send), a positive number of nanoseconds to
        wait before asking again, or None if it can't be sent under the current limits at
        all. In that case wait for a limit change or the request's deadline.
        """
        now = self._clock.now_ns()
        rpm_cap, tpm_cap = self.caps
        waits = (
            self._window.wait_ns(now, tokens, rpm_cap, tpm_cap),
            self._request_pacer.wait_ns(now, 1),
            self._token_pacer.wait_ns(now, tokens),
        )
        if None in waits:
            return None
        wait = max(waits)
        if wait > 0:
            return wait
        self._window.record(now, 1, tokens)
        self._request_pacer.take(1)
        self._token_pacer.take(tokens)
        return 0

    def reconcile(self, estimated_tokens: int, actual_tokens: int) -> None:
        """The provider reported actual usage. Charge any overshoot now.

        Undershoot is not refunded: the estimate was already counted at send time, and
        handing tokens back to an old log entry would be fiddly for little gain.
        """
        extra = actual_tokens - estimated_tokens
        if extra > 0:
            self._window.record(self._clock.now_ns(), 0, extra)
            self._token_pacer.take(extra)

    def usage(self) -> tuple[int, int]:
        """(requests, tokens) sent in the trailing 60 s."""
        return self._window.usage(self._clock.now_ns())
