import asyncio
import json
import statistics

import pytest
from aiohttp.test_utils import TestClient, TestServer

from gateway.clock import NS_PER_SECOND, FakeClock
from simulator.calllog import CallLog
from simulator.config import LatencyConfig, ModelConfig, SimConfig
from simulator.engine import ProviderSim
from simulator.server import make_app

S = NS_PER_SECOND


def make_sim(clock=None, log=None, seed=42, **model_kwargs) -> ProviderSim:
    model = ModelConfig(**({"rpm": 1_000_000, "tpm": 10**12} | model_kwargs))
    model.validate()
    return ProviderSim(SimConfig(models={"m": model}, seed=seed), clock or FakeClock(), log)


# --- rate limits ---------------------------------------------------------------------


def test_rpm_is_enforced_over_the_trailing_minute():
    clock = FakeClock()
    sim = make_sim(clock, rpm=3)
    assert [sim.handle("m", f"r{i}", 10).status for i in range(3)] == [200, 200, 200]

    limited = sim.handle("m", "r3", 10)
    assert limited.status == 429 and limited.retry_after_s == 60

    clock.advance(60 * S)  # the first three leave the window
    assert sim.handle("m", "r4", 10).status == 200


def test_tpm_is_enforced_and_rejections_do_not_use_capacity():
    clock = FakeClock()
    sim = make_sim(clock, tpm=1_000)
    assert sim.handle("m", "a", 600).status == 200
    assert sim.handle("m", "b", 600).status == 429
    assert sim.stats()["m"]["window_tokens"] == 600  # the 429 wasn't charged
    assert sim.handle("m", "c", 400).status == 200


def test_live_limit_change_applies_to_the_next_call():
    sim = make_sim(rpm=100)
    for i in range(10):
        sim.handle("m", f"r{i}", 1)
    sim.update_model("m", {"rpm": 5})  # window already holds 10 > 5
    assert sim.handle("m", "next", 1).status == 429


# --- outcomes ------------------------------------------------------------------------


def test_outcomes_are_a_function_of_the_request_not_the_call_order():
    kwargs = {"transient": {"503": 0.2}, "permanent": {"400": 0.1}}
    ids = [f"req-{i}" for i in range(500)]
    forward = {i: make_sim(seed=7, **kwargs).handle("m", i, 10).kind for i in ids}
    sim = make_sim(seed=7, **kwargs)
    backward = {i: sim.handle("m", i, 10).kind for i in reversed(ids)}
    assert forward == backward


def test_same_request_id_in_different_batches_gets_independent_outcomes():
    # Request ids are only unique per batch; outcomes are keyed on the idempotency key.
    sim = make_sim(permanent={"400": 0.5})
    kinds = {sim.handle("m", "req-1", 10, idempotency_key=f"batch{b}:req-1").kind for b in range(50)}
    assert kinds == {"success", "permanent:400"}


def test_permanent_failures_are_sticky_and_transient_ones_are_not():
    sim = make_sim(transient={"503": 0.5}, permanent={"400": 0.5})
    ids = [f"req-{i}" for i in range(200)]
    first = {i: sim.handle("m", i, 10, attempt=1).kind for i in ids}
    second = {i: sim.handle("m", i, 10, attempt=2).kind for i in ids}

    permanent = [i for i in ids if first[i] == "permanent:400"]
    assert permanent and all(second[i] == "permanent:400" for i in permanent)

    transient = [i for i in ids if first[i] == "transient:503"]
    assert any(second[i] != "transient:503" for i in transient)  # some retries recover


def test_outcome_mix_matches_configured_rates():
    sim = make_sim(transient={"503": 0.03, "500": 0.01}, permanent={"400": 0.02})
    n = 40_000
    kinds = [sim.handle("m", f"r{i}", 10).kind for i in range(n)]
    assert kinds.count("permanent:400") / n == pytest.approx(0.02, abs=0.003)
    # transient rates apply to the requests that weren't permanent failures
    assert kinds.count("transient:503") / n == pytest.approx(0.98 * 0.03, abs=0.004)
    assert kinds.count("transient:500") / n == pytest.approx(0.98 * 0.01, abs=0.002)


def test_latency_follows_configured_p50_and_p99():
    sim = make_sim(latency=LatencyConfig(p50_ms=100, p99_ms=800))
    ms = sorted(sim.handle("m", f"r{i}", 10).latency_ns / 1e6 for i in range(20_000))
    assert statistics.median(ms) == pytest.approx(100, rel=0.05)
    assert ms[int(0.99 * len(ms))] == pytest.approx(800, rel=0.1)


def test_actual_tokens_jitter_stays_within_bounds():
    sim = make_sim(actual_tokens_jitter=0.1)
    actual = [sim.handle("m", f"r{i}", 1_000).actual_tokens for i in range(2_000)]
    assert min(actual) >= 900 and max(actual) <= 1_100 and len(set(actual)) > 50


# --- idempotency and timeouts --------------------------------------------------------


def test_repeat_of_a_completed_request_is_replayed_and_not_charged():
    sim = make_sim(rpm=10)
    assert sim.handle("m", "r1", 10, idempotency_key="b1:r1").kind == "success"
    replay = sim.handle("m", "r1", 10, attempt=2, idempotency_key="b1:r1")
    assert replay.kind == "replay" and replay.status == 200
    assert sim.stats()["m"]["window_requests"] == 1


def test_timeout_does_the_work_so_the_retry_gets_a_replay():
    sim = make_sim(transient={"timeout": 1.0})
    hung = sim.handle("m", "r1", 10, attempt=1)
    assert hung.hang and hung.latency_ns == 30 * S
    assert sim.handle("m", "r1", 10, attempt=2).kind == "replay"


# --- admin and validation ------------------------------------------------------------


def test_bad_inputs_are_rejected_without_using_capacity():
    sim = make_sim(rpm=10)
    assert sim.handle("nope", "r1", 10).status == 404
    assert sim.handle("m", "r1", 0).status == 400
    assert sim.stats()["m"]["window_requests"] == 0


def test_update_model_validates():
    sim = make_sim()
    with pytest.raises(ValueError):
        sim.update_model("m", {"rmp": 5})  # typo
    with pytest.raises(ValueError):
        sim.update_model("m", {"transient": {"503": 0.9, "500": 0.2}})  # sums to > 1
    assert sim.update_model("m", {"latency": {"p50_ms": 10, "p99_ms": 20}}).latency.p50_ms == 10


def test_call_log_records_calls_and_limit_changes(tmp_path):
    path = tmp_path / "calls.jsonl"
    log = CallLog(path)
    sim = make_sim(log=log, rpm=1)
    sim.handle("m", "r1", 10)
    sim.handle("m", "r2", 10)
    sim.update_model("m", {"rpm": 5})
    log.close()

    lines = [json.loads(line) for line in path.read_text().splitlines()]
    assert [line["type"] for line in lines] == ["start", "limits", "call", "call", "limits"]
    assert [(c["kind"], c["counted"]) for c in lines if c["type"] == "call"] == [
        ("success", True),
        ("rate_limited", False),
    ]


# --- HTTP ----------------------------------------------------------------------------


def test_http_round_trip():
    async def run():
        sim = make_sim(rpm=1, latency=LatencyConfig(p50_ms=1, p99_ms=2))
        async with TestClient(TestServer(make_app(sim, CallLog(None)))) as client:
            body = {"request_id": "r1", "model": "m", "estimated_tokens": 10}
            ok = await client.post("/v1/infer", json=body)
            assert ok.status == 200
            assert (await ok.json())["usage"]["actual_tokens"] == 10

            limited = await client.post("/v1/infer", json=body | {"request_id": "r2"})
            assert limited.status == 429 and limited.headers["Retry-After"] == "60"

            bad = await client.post("/v1/infer", data="not json")
            assert bad.status == 400

            changed = await client.put("/admin/models/m", json={"rpm": 7})
            assert (await changed.json())["m"]["rpm"] == 7

    asyncio.run(run())
