"""Drive a RateLimiter with saturating traffic on a fake clock, and check the result
with a brute-force checker that shares no code with the limiter."""

from collections.abc import Callable
from dataclasses import dataclass

from gateway.clock import NS_PER_SECOND, FakeClock
from gateway.limiter import WINDOW_NS, Limits, RateLimiter


@dataclass(frozen=True)
class Send:
    at_ns: int
    tokens: int
    rpm_cap: int  # caps in effect at the moment of the send
    tpm_cap: int


def saturate(
    limiter: RateLimiter,
    clock: FakeClock,
    duration_s: float,
    tokens_for: Callable[[int], int],
    limit_changes: list[tuple[float, Limits]] = (),
) -> list[Send]:
    """Always have a request ready (demand > capacity) and send whenever allowed.

    tokens_for(i) gives the token size of the i-th request. limit_changes is a list of
    (at_seconds, new_limits), applied exactly at that time.
    """
    end_ns = round(duration_s * NS_PER_SECOND)
    changes = sorted(
        ((round(t * NS_PER_SECOND), lim) for t, lim in limit_changes), key=lambda c: c[0]
    )
    sends: list[Send] = []
    i = 0
    while clock.now_ns() < end_ns:
        while changes and changes[0][0] <= clock.now_ns():
            limiter.set_limits(changes.pop(0)[1])
        next_event = min(end_ns, changes[0][0]) if changes else end_ns

        tokens = tokens_for(i)
        wait = limiter.try_acquire(tokens)
        if wait == 0:
            rpm_cap, tpm_cap = limiter.caps
            sends.append(Send(clock.now_ns(), tokens, rpm_cap, tpm_cap))
            i += 1
            continue
        # Blocked: sleep until the limiter says, or until the next limit change.
        target = next_event if wait is None else min(clock.now_ns() + wait, next_event)
        clock.advance(max(target - clock.now_ns(), 0) or 1)
    return sends


def violations(sends: list[Send], window_ns: int = WINDOW_NS) -> list[str]:
    """Check the rule at every send: the trailing window (t-60s, t] is within the caps
    in effect at t. Two-pointer sweep over the sorted sends."""
    problems = []
    lo = 0
    tokens_in_window = 0
    for hi, send in enumerate(sends):
        tokens_in_window += send.tokens
        while sends[lo].at_ns <= send.at_ns - window_ns:
            tokens_in_window -= sends[lo].tokens
            lo += 1
        requests_in_window = hi - lo + 1
        if requests_in_window > send.rpm_cap:
            problems.append(f"t={send.at_ns / 1e9:.3f}s requests {requests_in_window} > {send.rpm_cap}")
        if tokens_in_window > send.tpm_cap:
            problems.append(f"t={send.at_ns / 1e9:.3f}s tokens {tokens_in_window} > {send.tpm_cap}")
    return problems


def sends_between(sends: list[Send], start_s: float, end_s: float) -> list[Send]:
    lo, hi = start_s * NS_PER_SECOND, end_s * NS_PER_SECOND
    return [s for s in sends if lo <= s.at_ns < hi]
