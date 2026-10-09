"""Pass/fail checks for the validation scenarios, computed from the run's evidence.

Every check returns the numbers behind its verdict, so the report shows evidence, not
just a tick.
"""

import bisect
from dataclasses import dataclass, field
from itertools import accumulate

from verify.data import RunData

WINDOW_S = 60.0
FINAL = ("succeeded", "failed", "expired")
COMPLETED = ("succeeded", "failed")  # reached the provider and got a final answer


@dataclass
class Check:
    name: str
    passed: bool
    detail: str
    data: dict = field(default_factory=dict)


# --- helpers -------------------------------------------------------------------------


def trailing_counts(times: list[float], weights: list[int] | None, at: list[float]) -> list[float]:
    """For each time in `at`, the count (or weight sum) of events in (t-60, t]."""
    prefix = [0, *accumulate(weights)] if weights is not None else None
    out = []
    for t in at:
        hi = bisect.bisect_right(times, t)
        lo = bisect.bisect_right(times, t - WINDOW_S)
        out.append(prefix[hi] - prefix[lo] if prefix is not None else hi - lo)
    return out


def completions(data: RunData, model: str) -> list[float]:
    return sorted(r.finished for r in data.requests
                  if r.model == model and r.state in COMPLETED and r.finished is not None)


def count_between(times: list[float], a: float, b: float) -> int:
    return bisect.bisect_left(times, b) - bisect.bisect_left(times, a)


def percentiles(values: list[float]) -> dict:
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}
    values = sorted(values)

    def pick(q):
        return round(values[min(len(values) - 1, int(q * len(values)))], 1)

    return {"count": len(values), "p50": pick(0.5), "p95": pick(0.95), "p99": pick(0.99), "max": round(values[-1], 1)}


def latency_ms(data: RunData, model: str | None = None) -> dict:
    return percentiles([
        (r.finished - r.submitted) * 1000 for r in data.requests
        if r.state in COMPLETED and r.finished is not None and (model is None or r.model == model)
    ])


# --- checks used by every scenario ---------------------------------------------------


def window_limits(data: RunData) -> list[Check]:
    """At every call the provider counted, the trailing 60 s must be within the limits in
    effect at that moment. This reads the provider's log, not the gateway's numbers."""
    checks = []
    for model, mc in sorted(data.calls.items()):
        lim = data.provider_limits[model]
        lo = tokens = violations = 0
        worst_r = worst_t = 0.0
        peak_requests = peak_tokens = 0
        first_bad = None
        for hi, t in enumerate(mc.t):
            tokens += mc.tokens[hi]
            while mc.t[lo] <= t - WINDOW_S:
                tokens -= mc.tokens[lo]
                lo += 1
            n = hi - lo + 1
            rpm, tpm = lim.at(t)
            peak_requests, peak_tokens = max(peak_requests, n), max(peak_tokens, tokens)
            worst_r = max(worst_r, n / rpm if rpm else float("inf"))
            worst_t = max(worst_t, tokens / tpm if tpm else float("inf"))
            if n > rpm or tokens > tpm:
                violations += 1
                first_bad = first_bad or f"t={t:.2f}s: {n} req / {tokens} tok vs {rpm} / {tpm}"
        checks.append(Check(
            f"limits:{model}",
            violations == 0,
            f"{len(mc.t):,} counted calls; peak trailing-60s use {worst_r:.1%} of RPM, {worst_t:.1%} of TPM; "
            f"{violations} violations" + (f" (first: {first_bad})" if first_bad else ""),
            {"calls": len(mc.t), "violations": violations, "max_rpm_fraction": round(worst_r, 4),
             "max_tpm_fraction": round(worst_t, 4), "peak_requests_60s": peak_requests,
             "peak_tokens_60s": peak_tokens, "provider_429s": mc.rate_limited, "kinds": mc.kinds},
        ))
    return checks


def accounting(data: RunData) -> Check:
    """Every submitted request is completed, waiting, expired or rejected, at the end of
    the load and again after draining. Nothing may be unaccounted for."""
    load = data.load["counts"]
    snapshots = {name: data.run.get(name) for name in ("snapshot_load_end", "snapshot_drained")}
    rows = {}
    ok = load["errors"] == 0 and load["invalid"] == 0
    for name, snap in snapshots.items():
        if snap is None:
            continue
        r = snap["requests"]
        row = {
            "completed": r["succeeded"] + r["failed"],
            "waiting": r["queued"] + r["in_flight"] + r["retry_wait"],
            "expired": r["expired"],
            "rejected": r["rejected"],
        }
        row["total"] = sum(row.values())
        rows[name] = row
        ok &= row["total"] == load["submitted"]
        ok &= r["rejected"] == load["rejected"]
    persisted = len(data.requests)
    ok &= persisted == load["accepted"]
    detail = (f"submitted {load['submitted']:,} = accepted {load['accepted']:,} + rejected {load['rejected']:,} "
              f"(+ invalid {load['invalid']}, transport errors {load['errors']}); "
              + "; ".join(f"{k}: " + ", ".join(f"{a} {b:,}" for a, b in v.items()) for k, v in rows.items())
              + f"; {persisted:,} request rows persisted")
    return Check("accounting", ok, detail, {"loadgen": load, "gateway": rows, "persisted": persisted})


# --- scenario 1 ----------------------------------------------------------------------


def capacity(data: RunData, model: str, warmup_s: float, min_fraction: float) -> Check:
    """Each full minute after warm-up must complete >= min_fraction of the RPM limit."""
    done = completions(data, model)
    sends = data.calls[model].t
    windows = []
    start = warmup_s
    while start + WINDOW_S <= data.load_end + 1e-6:
        rpm, _ = data.provider_limits[model].at(start)
        n_done = count_between(done, start, start + WINDOW_S)
        n_sent = count_between(sends, start, start + WINDOW_S)
        windows.append({"from_s": round(start, 1), "limit": rpm, "completed": n_done, "sent": n_sent,
                        "completed_fraction": round(n_done / rpm, 4), "sent_fraction": round(n_sent / rpm, 4)})
        start += WINDOW_S
    ok = bool(windows) and all(w["completed_fraction"] >= min_fraction for w in windows)
    worst = min((w["completed_fraction"] for w in windows), default=0)
    detail = (f"{len(windows)} full minutes after a {warmup_s:.0f}s warm-up; lowest completed {worst:.1%} "
              f"of the RPM limit (need >= {min_fraction:.0%})")
    return Check(f"capacity:{model}", ok, detail, {"windows": windows})


# --- scenario 2 ----------------------------------------------------------------------


def adjusts_to_limit_changes(data: RunData, model: str, min_fraction: float = 0.8) -> Check:
    """After each change (once the old window has drained, i.e. 60 s later), the model
    keeps running close to its new limit, and the gateway was never restarted."""
    changes = [c for c in data.run.get("limit_changes_applied", []) if c["model"] == model]
    sends = data.calls[model].t
    phases = []
    ok = bool(changes)
    for i, change in enumerate(changes):
        start = change["t"] + WINDOW_S
        end = changes[i + 1]["t"] if i + 1 < len(changes) else data.load_end
        if end - start < 20:
            phases.append({"change_at_s": round(change["t"], 1), "note": "phase too short to measure"})
            continue
        rate = count_between(sends, start, end) / (end - start) * 60
        first_send = sends[bisect.bisect_right(sends, change["t"])] if sends else None
        fraction = rate / change["rpm"] if change["rpm"] else 0
        ok &= fraction >= min_fraction
        phases.append({
            "change_at_s": round(change["t"], 1), "new_rpm": change["rpm"],
            "measured_from_s": round(start, 1), "measured_to_s": round(end, 1),
            "observed_rpm": round(rate), "fraction_of_new_limit": round(fraction, 4),
            "first_send_after_change_s": None if first_send is None else round(first_send - change["t"], 2),
        })
    restarts = data.run.get("gateway_restarts", 0)
    ok &= restarts == 0
    detail = "; ".join(
        f"after change at {p['change_at_s']}s to {p.get('new_rpm')} RPM: {p.get('observed_rpm')} RPM "
        f"({p.get('fraction_of_new_limit', 0):.0%})" for p in phases if "observed_rpm" in p
    ) + f"; gateway restarts: {restarts}"
    return Check(f"adjusts:{model}", ok, detail, {"phases": phases, "gateway_restarts": restarts})


def keeps_running(data: RunData, model: str, warmup_s: float, min_fraction: float) -> Check:
    """Model B: every full minute after warm-up stays near its own (unchanged) limit,
    including while model A is constrained."""
    check = capacity(data, model, warmup_s, min_fraction)
    check.name = f"isolation:{model}"
    return check


# --- scenario 3 ----------------------------------------------------------------------


def batch_callbacks(data: RunData, max_ack_ms: float, expect_rejections: int) -> list[Check]:
    checks = []
    statuses = data.json_file("batch_status.json") or {}
    results = data.json_file("batch_results.json") or {}
    acks = {s["batch_id"]: s for s in data.load["batches"]}
    by_batch: dict[str, list] = {}
    for r in data.requests:
        if r.batch_id:
            by_batch.setdefault(r.batch_id, []).append(r)

    for batch in data.batches:
        bid = batch["batch_id"]
        reqs = by_batch.get(bid, [])
        deliveries = sorted((d for d in data.callbacks if d["body"].get("batch_id") == bid), key=lambda d: d["t"])
        last_final = max((r.finished for r in reqs if r.finished is not None), default=None)
        ack_ms = acks.get(bid, {}).get("ack_ms")

        checks.append(Check(f"ack:{bid}", ack_ms is not None and ack_ms < max_ack_ms,
                            f"acknowledged in {ack_ms:.0f} ms (limit {max_ack_ms:.0f} ms)" if ack_ms else "no ack",
                            {"ack_ms": ack_ms}))

        all_final = len(reqs) == batch["total"] and all(r.state in FINAL for r in reqs)
        first_t = deliveries[0]["t"] if deliveries else None
        after = all_final and first_t is not None and last_final is not None and first_t >= last_final
        checks.append(Check(
            f"callback_after_final:{bid}", after,
            f"last request final at {last_final:.3f}s, first callback attempt at {first_t:.3f}s "
            f"(+{(first_t - last_final) * 1000:.0f} ms)" if after else
            f"all final: {all_final}; last final {last_final}; first callback {first_t}",
            {"last_final_s": last_final, "first_callback_s": first_t},
        ))

        answers = [d["answered"] for d in deliveries]
        delivered = bool(answers) and answers[-1] == 200
        rejected_first = len(answers) > expect_rejections and all(a != 200 for a in answers[:expect_rejections])
        same_body = len({repr(sorted(d["body"].items())) for d in deliveries}) <= 1
        same_event = len({d["event_id"] for d in deliveries}) <= 1
        timing = [round(d["t"] - (first_t or 0), 2) for d in deliveries]
        checks.append(Check(
            f"callback_retry:{bid}", delivered and rejected_first and same_body and same_event,
            f"answers {answers} at +{timing}s after the first attempt; identical body: {same_body}; "
            f"same event id: {same_event}",
            {"answers": answers, "offsets_s": timing},
        ))

        status = statuses.get(bid)
        fields = ("batch_id", "status", "total", "succeeded", "failed", "expired")
        body = deliveries[-1]["body"] if deliveries else {}
        mismatched = [f for f in fields if status is None or body.get(f) != status.get(f)]
        checks.append(Check(
            f"callback_matches_api:{bid}", not mismatched,
            "callback summary equals GET /v1/batches/{id}: " + ", ".join(f"{f}={body.get(f)}" for f in fields[1:])
            if not mismatched else f"mismatched fields: {mismatched}",
            {"callback": {f: body.get(f) for f in fields}, "api": {f: (status or {}).get(f) for f in fields}},
        ))

        ids = results.get(bid, [])
        expected = set(data.run.get("expected_ids", {}).get(bid, []))
        exactly_once = len(ids) == batch["total"] == len(set(ids)) and (not expected or set(ids) == expected)
        checks.append(Check(
            f"results_exactly_once:{bid}", exactly_once,
            f"{len(ids):,} result rows, {len(set(ids)):,} unique ids, batch total {batch['total']:,}"
            + (", same set as submitted" if expected and set(ids) == expected else ""),
            {"rows": len(ids), "unique": len(set(ids))},
        ))
    return checks
