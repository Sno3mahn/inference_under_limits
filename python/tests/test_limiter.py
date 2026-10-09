import pytest
from harness import saturate, sends_between, violations

from gateway.clock import NS_PER_SECOND, FakeClock
from gateway.limiter import WINDOW_NS, Limits, Pacer, RateLimiter, SlidingWindow

S = NS_PER_SECOND


# --- SlidingWindow -----------------------------------------------------------------


def test_window_entry_leaves_exactly_60s_later():
    w = SlidingWindow()
    w.record(0, 1, 100)
    assert w.usage(WINDOW_NS - 1) == (1, 100)  # still inside (t-60s, t]
    assert w.usage(WINDOW_NS) == (0, 0)  # sent at exactly t-60s: outside


def test_window_wait_is_until_oldest_send_expires():
    w = SlidingWindow()
    for t in range(3):
        w.record(t * S, 1, 10)
    # cap of 3 requests is full; the send at t=0 expires at t=60s
    assert w.wait_ns(10 * S, 10, max_requests=3, max_tokens=1000) == 50 * S


def test_window_wait_for_tokens_may_need_several_expiries():
    w = SlidingWindow()
    w.record(0 * S, 1, 400)
    w.record(1 * S, 1, 400)
    w.record(2 * S, 1, 400)
    # 1200 used of 1500; an 800-token request needs 500 more room -> two must expire
    assert w.wait_ns(5 * S, 800, max_requests=100, max_tokens=1500) == 61 * S - 5 * S
    # a 700-token request needs 400 -> the first expiry is enough
    assert w.wait_ns(5 * S, 700, max_requests=100, max_tokens=1500) == 60 * S - 5 * S


def test_window_never_fits_returns_none():
    w = SlidingWindow()
    assert w.wait_ns(0, 2000, max_requests=10, max_tokens=1000) is None
    assert w.wait_ns(0, 1, max_requests=0, max_tokens=1000) is None


def test_token_only_corrections_do_not_count_as_requests():
    w = SlidingWindow()
    w.record(0, 0, 500)
    assert w.usage(1) == (0, 500)


# --- Pacer ---------------------------------------------------------------------------


def test_pacer_spreads_sends_at_the_configured_rate():
    p = Pacer(per_minute=60, burst_s=1, now_ns=0)  # 1/s, burst of 1
    assert p.wait_ns(0, 1) == 0
    p.take(1)
    assert p.wait_ns(0, 1) == pytest.approx(S, abs=1)


def test_pacer_lets_an_oversized_request_through_then_charges_the_debt():
    p = Pacer(per_minute=600, burst_s=1, now_ns=0)  # 10/s, capacity 10
    assert p.wait_ns(0, 25) == 0  # bigger than the bucket, but the bucket is full
    p.take(25)  # level is now -15
    assert p.wait_ns(0, 1) == pytest.approx(1.6 * S, abs=1)  # wait off 15 debt + 1


def test_pacer_rate_drop_discards_saved_burst():
    p = Pacer(per_minute=6000, burst_s=1, now_ns=0)  # capacity 100
    p.set_rate(60, 0)  # capacity 1
    assert p.wait_ns(0, 1) == 0
    p.take(1)
    assert p.wait_ns(0, 1) > 0


# --- RateLimiter ---------------------------------------------------------------------


def test_caps_apply_utilisation_and_never_round_to_zero():
    clock = FakeClock()
    assert RateLimiter(Limits(rpm=50_000, tpm=100_000_000), clock).caps == (48_500, 97_000_000)
    assert RateLimiter(Limits(rpm=1, tpm=10), clock).caps == (1, 9)
    assert RateLimiter(Limits(rpm=0, tpm=10), clock).caps == (0, 9)


def test_paused_model_blocks_until_limits_change():
    clock = FakeClock()
    limiter = RateLimiter(Limits(rpm=0, tpm=1000), clock)
    assert limiter.try_acquire(10) is None
    limiter.set_limits(Limits(rpm=60, tpm=1000))
    assert limiter.try_acquire(10) == 0


def test_request_bigger_than_tpm_can_never_fit():
    limiter = RateLimiter(Limits(rpm=100, tpm=1000), FakeClock())
    assert not limiter.can_ever_fit(1000)  # cap is 970
    assert limiter.can_ever_fit(970)
    assert limiter.try_acquire(1000) is None


def test_scenario1_shape_reaches_90_percent_without_exceeding():
    """50k RPM / 100M TPM, 1k-token requests, demand above capacity for 5 minutes."""
    clock = FakeClock()
    limiter = RateLimiter(Limits(rpm=50_000, tpm=100_000_000), clock)
    sends = saturate(limiter, clock, duration_s=300, tokens_for=lambda i: 1_000)

    assert violations(sends) == []
    for minute in range(1, 5):  # after one minute of warm-up
        sent = len(sends_between(sends, minute * 60, (minute + 1) * 60))
        assert sent >= 0.9 * 50_000, f"minute {minute}: {sent}"


def test_tpm_binds_when_requests_are_large():
    """10k-token requests: 100M TPM allows only 10k RPM even though RPM is 50k."""
    clock = FakeClock()
    limiter = RateLimiter(Limits(rpm=50_000, tpm=100_000_000), clock)
    sends = saturate(limiter, clock, duration_s=180, tokens_for=lambda i: 10_000)

    assert violations(sends) == []
    sent = len(sends_between(sends, 60, 120))
    assert 0.9 * 10_000 <= sent <= 10_000


def test_limit_drop_pauses_then_runs_at_new_rate_then_recovers():
    """Scenario 2 shape: Model A 30k -> 5k RPM at t=120s, back to 30k at t=240s."""
    clock = FakeClock()
    full, reduced = Limits(rpm=30_000, tpm=60_000_000), Limits(rpm=5_000, tpm=60_000_000)
    limiter = RateLimiter(full, clock)
    sends = saturate(
        limiter,
        clock,
        duration_s=420,
        tokens_for=lambda i: 1_000,
        limit_changes=[(120, reduced), (240, full)],
    )

    assert violations(sends) == []
    # Right after the drop the trailing minute holds ~29k sends, far over the new cap of
    # 4,850. Nothing may be sent until only ~10 s of old sends remain, i.e. ~50 s later.
    assert sends_between(sends, 120, 165) == []
    # Once drained, it runs close to the reduced rate.
    assert len(sends_between(sends, 185, 240)) == pytest.approx(55 / 60 * 4_850, rel=0.05)
    # After restoring, it climbs back to full rate (no restart, no burst over the cap).
    assert len(sends_between(sends, 300, 360)) >= 0.9 * 30_000


def test_reconcile_charges_overshoot_against_tpm():
    clock = FakeClock()
    limiter = RateLimiter(Limits(rpm=1_000, tpm=10_000), clock, utilization=1.0)
    assert limiter.try_acquire(5_000) == 0
    limiter.reconcile(estimated_tokens=5_000, actual_tokens=9_000)
    assert limiter.usage() == (1, 9_000)
    clock.advance(10 * S)
    assert limiter.try_acquire(5_000) > 0  # only 1k left in the window
