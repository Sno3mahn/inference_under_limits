"""The verifier must catch what it claims to catch: tested on synthetic runs."""

from pathlib import Path

from verify import checks
from verify.data import LimitTimeline, ModelCalls, RequestRow, RunData


def run_with(calls: dict[str, ModelCalls], limits: dict[str, LimitTimeline], **extra) -> RunData:
    load = {"started_wall": 0.0, "ended_wall": extra.pop("load_end", 300.0),
            "counts": extra.pop("counts", {"submitted": 0, "accepted": 0, "rejected": 0, "invalid": 0, "errors": 0}),
            "batches": []}
    return RunData(Path("."), extra.pop("run", {}), 0.0, calls, limits, extra.pop("requests", []), [], [], load, [])


def steady(rate_per_s: float, seconds: float, start: float = 0.0, tokens: int = 1) -> ModelCalls:
    n = int(rate_per_s * seconds)
    return ModelCalls(t=[start + i / rate_per_s for i in range(n)], tokens=[tokens] * n, attempts=[1] * n)


def limit(rpm: int, tpm: int = 10**12, changes=()) -> LimitTimeline:
    lt = LimitTimeline([float("-inf")], [rpm], [tpm])
    for t, r in changes:
        lt.t.append(t)
        lt.rpm.append(r)
        lt.tpm.append(tpm)
    return lt


def test_window_check_passes_at_the_limit_and_fails_just_over():
    # 8/s keeps every timestamp exactly representable, so the window holds exactly 480.
    ok = checks.window_limits(run_with({"m": steady(8, 120)}, {"m": limit(480)}))
    assert ok[0].passed and ok[0].data["peak_requests_60s"] == 480

    over = checks.window_limits(run_with({"m": steady(8, 120)}, {"m": limit(479)}))
    assert not over[0].passed and over[0].data["violations"] > 0


def test_window_check_catches_a_burst_across_a_minute_boundary():
    # 600 at the end of minute 1 and 600 at the start of minute 2: each calendar minute is fine,
    # but the 60 s window around the boundary holds 1,200.
    burst = ModelCalls(t=[59 + i / 1000 for i in range(600)] + [60 + i / 1000 for i in range(600)],
                       tokens=[1] * 1200, attempts=[1] * 1200)
    result = checks.window_limits(run_with({"m": burst}, {"m": limit(600)}))
    assert not result[0].passed


def test_window_check_uses_the_limit_in_effect_at_each_send():
    # 10/s, limit drops to 300 at t=60: sends right after the drop break the new limit.
    result = checks.window_limits(run_with({"m": steady(10, 90)}, {"m": limit(600, changes=[(60, 300)])}))
    assert not result[0].passed


def test_window_check_counts_tokens():
    result = checks.window_limits(run_with({"m": steady(1, 60, tokens=100)}, {"m": limit(1000, tpm=5_000)}))
    assert not result[0].passed and result[0].data["max_tpm_fraction"] > 1


def test_accounting_fails_when_a_request_is_missing():
    counts = {"submitted": 10, "accepted": 10, "rejected": 0, "invalid": 0, "errors": 0}
    snapshot = {"requests": {"succeeded": 9, "failed": 0, "queued": 0, "in_flight": 0, "retry_wait": 0,
                             "expired": 0, "rejected": 0}}
    rows = [RequestRow("m", "succeeded", None, f"r{i}", 1, 0.0, 1.0) for i in range(10)]
    result = checks.accounting(run_with({}, {}, counts=counts, requests=rows, run={"snapshot_drained": snapshot}))
    assert not result.passed

    snapshot["requests"]["succeeded"] = 10
    result = checks.accounting(run_with({}, {}, counts=counts, requests=rows, run={"snapshot_drained": snapshot}))
    assert result.passed


def test_capacity_check_needs_every_minute_after_warmup():
    done = [RequestRow("m", "succeeded", None, f"r{i}", 1, 0.0, i / 10) for i in range(10 * 180)]
    data = run_with({"m": steady(10, 180)}, {"m": limit(600)}, requests=done, load_end=180.0)
    result = checks.capacity(data, "m", warmup_s=60, min_fraction=0.9)
    assert result.passed and len(result.data["windows"]) == 2

    sparse = [r for r in done if not 120 <= r.finished < 150]  # a 30 s stall in the last minute
    data = run_with({"m": steady(10, 180)}, {"m": limit(600)}, requests=sparse, load_end=180.0)
    assert not checks.capacity(data, "m", warmup_s=60, min_fraction=0.9).passed
