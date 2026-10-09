"""Gateway core with a scripted in-process provider, on the real clock (short runs)."""

import asyncio

import pytest
from fakes import BAD_REQUEST, OK, THROTTLED, UNAVAILABLE, ScriptedProvider, async_test, wait_until

from gateway.clock import MonotonicClock
from gateway.core import Gateway, GatewaySettings, InvalidSubmission, OverCapacity, RequestItem
from gateway.limiter import Limits
from gateway.scheduler import SchedulerSettings
from gateway.tracker import IllegalTransition, State


def fast_settings(**scheduler_overrides) -> GatewaySettings:
    scheduler = SchedulerSettings(backoff_base_s=0.01, backoff_cap_s=0.05, sweep_interval_s=0.05)
    for name, value in scheduler_overrides.items():
        setattr(scheduler, name, value)
    return GatewaySettings(scheduler=scheduler)


async def make_gateway(limits: dict[str, Limits], provider, settings=None) -> Gateway:
    gw = Gateway(limits, provider, MonotonicClock(), settings or fast_settings())
    await gw.start()
    return gw


def items(n: int, model: str = "a", tokens: int = 10, prefix: str = "r", **kw) -> list[RequestItem]:
    return [RequestItem(f"{prefix}{i}", model, tokens, **kw) for i in range(n)]


FAST = Limits(rpm=600_000, tpm=10**9)


# --- happy path and accounting -------------------------------------------------------


@async_test
async def test_batch_completes_and_every_request_is_accounted_for():
    provider = ScriptedProvider()
    gw = await make_gateway({"a": FAST, "b": FAST}, provider)
    reqs = gw.submit_batch("b1", items(100, "a") + items(100, "b", prefix="x"))
    await wait_until(lambda: all(r.state.is_final for r in reqs))

    assert gw.tracker.totals()["succeeded"] == 200
    assert sum(gw.tracker.totals().values()) == 200
    assert len({key for _, _, key, _ in provider.calls}) == 200
    await gw.stop()


@async_test
async def test_pacing_spreads_sends_out():
    provider = ScriptedProvider()
    # 600 RPM x 0.97 = 9.7/s with a 1 s burst: ~10 at once, then ~9.7 per second
    gw = await make_gateway({"a": Limits(rpm=600, tpm=10**9)}, provider)
    gw.submit_batch("b1", items(100))
    await asyncio.sleep(2.0)
    assert 20 <= len(provider.calls) <= 31
    await gw.stop()


# --- failures and retries ------------------------------------------------------------


@async_test
async def test_transient_failure_is_retried_then_succeeds():
    provider = ScriptedProvider(lambda r: UNAVAILABLE if r.attempts == 1 else OK)
    gw = await make_gateway({"a": FAST}, provider)
    (req,) = gw.submit_batch("b1", items(1))
    await wait_until(lambda: req.state.is_final)
    assert req.state is State.SUCCEEDED and req.attempts == 2 and req.error is None
    await gw.stop()


@async_test
async def test_permanent_failure_is_not_retried():
    provider = ScriptedProvider(lambda r: BAD_REQUEST)
    gw = await make_gateway({"a": FAST}, provider)
    (req,) = gw.submit_batch("b1", items(1))
    await wait_until(lambda: req.state.is_final)
    assert req.state is State.FAILED and req.attempts == 1 and req.error == "http_400"
    await gw.stop()


@async_test
async def test_retries_are_capped():
    provider = ScriptedProvider(lambda r: UNAVAILABLE)
    gw = await make_gateway({"a": FAST}, provider, fast_settings(max_attempts=3))
    (req,) = gw.submit_batch("b1", items(1))
    await wait_until(lambda: req.state.is_final)
    assert req.state is State.FAILED and req.attempts == 3
    assert req.error == "retries_exhausted:http_503"
    await gw.stop()


@async_test
async def test_provider_429_pauses_the_whole_model():
    provider = ScriptedProvider(lambda r: THROTTLED if r.request_id == "r0" and r.attempts == 1 else OK)
    gw = await make_gateway({"a": FAST}, provider, fast_settings(max_in_flight=1))
    reqs = gw.submit_batch("b1", items(5))
    await wait_until(lambda: all(r.state.is_final for r in reqs))
    times = [t for t, *_ in provider.calls]
    assert times[1] - times[0] >= 0.25  # nothing sent during the 0.3 s pause
    await gw.stop()


# --- deadlines, overload, isolation --------------------------------------------------


@async_test
async def test_requests_that_wait_too_long_expire():
    provider = ScriptedProvider()
    gw = await make_gateway({"a": Limits(rpm=60, tpm=10**9)}, provider)  # ~1 per second
    reqs = gw.submit_batch("b1", items(5, deadline_s=0.3))
    await wait_until(lambda: all(r.state.is_final for r in reqs))
    states = [r.state for r in reqs]
    assert states.count(State.SUCCEEDED) == 1 and states.count(State.EXPIRED) == 4
    assert reqs[-1].error == "deadline_exceeded_in_queue"
    await gw.stop()


@async_test
async def test_full_queue_rejects_new_work_and_counts_it():
    provider = ScriptedProvider()
    settings = fast_settings(queue_minutes=0, min_queue=10)
    gw = await make_gateway({"a": Limits(rpm=60, tpm=10**9)}, provider, settings)
    gw.submit_batch("b1", items(10))
    with pytest.raises(OverCapacity) as exc:
        gw.submit_batch("b2", items(3))
    assert exc.value.retry_after_s >= 1
    assert gw.tracker.totals()["rejected"] == 3
    await gw.stop()


@async_test
async def test_batch_admission_is_all_or_nothing_across_models():
    provider = ScriptedProvider()
    settings = fast_settings(queue_minutes=0, min_queue=5)
    gw = await make_gateway({"a": Limits(rpm=60, tpm=10**9), "b": FAST}, provider, settings)
    with pytest.raises(OverCapacity):
        gw.submit_batch("b1", items(6, "a") + items(2, "b", prefix="x"))  # a doesn't fit
    assert gw.requests == {}  # nothing from the batch was queued, not even model b's part
    await gw.stop()


@async_test
async def test_slow_model_does_not_block_other_models():
    provider = ScriptedProvider()
    gw = await make_gateway({"a": Limits(rpm=60, tpm=10**9), "b": FAST}, provider)
    gw.submit_batch("b1", items(50, "a"))  # a backlog that takes ~50 s
    b_reqs = gw.submit_batch("b2", items(50, "b"))
    await wait_until(lambda: all(r.state.is_final for r in b_reqs), timeout_s=2)
    assert gw.model_status("a")["queue"]["waiting"] > 40
    await gw.stop()


@async_test
async def test_limit_change_takes_effect_without_restart():
    provider = ScriptedProvider()
    gw = await make_gateway({"a": Limits(rpm=60, tpm=10**9)}, provider)
    reqs = gw.submit_batch("b1", items(50))
    await asyncio.sleep(0.3)
    assert sum(r.state.is_final for r in reqs) <= 2
    gw.set_limits("a", FAST)
    await wait_until(lambda: all(r.state.is_final for r in reqs), timeout_s=2)
    assert gw.limit_history[-1]["rpm"] == FAST.rpm
    await gw.stop()


@async_test
async def test_in_flight_cap_is_respected():
    provider = ScriptedProvider(latency_s=0.05)
    gw = await make_gateway({"a": FAST}, provider, fast_settings(max_in_flight=3))
    reqs = gw.submit_batch("b1", items(30))
    await wait_until(lambda: all(r.state.is_final for r in reqs))
    assert provider.max_concurrent == 3
    await gw.stop()


# --- validation and idempotency ------------------------------------------------------


@async_test
async def test_batch_validation_reports_every_problem():
    gw = await make_gateway({"a": Limits(rpm=100, tpm=1_000)}, ScriptedProvider())
    bad = [
        RequestItem("dup", "a", 10),
        RequestItem("dup", "a", 10),
        RequestItem("m", "nope", 10),
        RequestItem("t", "a", 0),
        RequestItem("big", "a", 1_000),  # TPM cap is 970
    ]
    with pytest.raises(InvalidSubmission) as exc:
        gw.submit_batch("b1", bad)
    text = " ".join(exc.value.details)
    for expected in ("duplicate request_id 'dup'", "unknown model", "positive integer", "can never fit"):
        assert expected in text
    with pytest.raises(InvalidSubmission):
        gw.submit_batch("b2", [])
    await gw.stop()


@async_test
async def test_single_request_is_idempotent_and_can_be_awaited():
    provider = ScriptedProvider()
    gw = await make_gateway({"a": FAST}, provider)
    first, created = gw.submit_single(RequestItem("same", "a", 10))
    again, created_again = gw.submit_single(RequestItem("same", "a", 10))
    assert created and not created_again and first is again
    assert await gw.wait_final(first, timeout_s=2)
    assert first.state is State.SUCCEEDED and len(provider.calls) == 1
    await gw.stop()


def test_a_final_request_cannot_change_again():
    from gateway.tracker import InferenceRequest, RequestTracker

    tracker = RequestTracker()
    req = InferenceRequest("k", "r", None, "a", 10, None, submitted_ns=0, deadline_ns=10)
    tracker.add(req)
    tracker.transition(req, State.IN_FLIGHT, 1)
    assert tracker.transition(req, State.SUCCEEDED, 2)
    assert not tracker.transition(req, State.FAILED, 3)  # late duplicate: ignored
    assert req.state is State.SUCCEEDED and tracker.totals()["succeeded"] == 1
    with pytest.raises(IllegalTransition):
        fresh = InferenceRequest("k2", "r2", None, "a", 10, None, submitted_ns=0, deadline_ns=10)
        tracker.add(fresh)
        tracker.transition(fresh, State.SUCCEEDED, 1)  # must go through IN_FLIGHT
