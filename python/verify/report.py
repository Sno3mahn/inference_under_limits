"""Write report.json, report.md and charts/*.png for a run.

Charts follow a few rules: one y-axis per panel (measures with different scales get their
own panel), configured limits drawn as neutral reference lines rather than series colors,
series colors taken in fixed order from a colorblind-validated palette, and a legend on
every multi-series panel.
"""

import json
import platform
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

from verify.checks import Check, completions, latency_ms, trailing_counts  # noqa: E402
from verify.data import RunData  # noqa: E402

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"
LIMIT = "#2c2c2a"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]  # categorical slots 1-3 (validated all-pairs)

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_2, "xtick.color": INK_2, "ytick.color": INK_2,
    "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 9, "legend.frameon": False,
    "lines.linewidth": 1.6,
})


def _seconds(data: RunData) -> list[float]:
    end = max([data.load_end, *(s["t"] for s in data.timeline)] or [data.load_end])
    return [float(s) for s in range(0, int(end) + 2)]


def _human(value, _pos=None) -> str:
    for size, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if abs(value) >= size:
            return f"{value / size:g}{suffix}"
    return f"{value:g}"


def _legend(ax) -> None:
    """Legend in a row above the panel, so it never covers the data."""
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=4, fontsize=8, borderaxespad=0.3)


def _limit_steps(timeline, at: list[float], index: int) -> list[float]:
    return [timeline.at(t)[index] for t in at]


def _decorate(ax, data: RunData) -> None:
    warmup = data.run.get("warmup_s", 0)
    if warmup:
        ax.axvspan(0, warmup, color=GRID, alpha=0.45, lw=0)
    for change in data.run.get("limit_changes_applied", []):
        ax.axvline(change["t"], color=INK_2, lw=0.8, ls=(0, (2, 2)))
    ax.axvline(data.load_end, color=INK_2, lw=0.8)


def model_chart(data: RunData, model: str, out: Path, utilization: float) -> Path:
    xs = _seconds(data)
    calls = data.calls.get(model)
    lim = data.provider_limits[model]
    fig, (ax_r, ax_t, ax_q) = plt.subplots(3, 1, figsize=(9, 8.5), sharex=True,
                                           gridspec_kw={"height_ratios": [3, 2, 2]})
    fig.suptitle(f"{model}: configured limits vs observed throughput", x=0.02, ha="left", fontsize=11)

    rpm = _limit_steps(lim, xs, 0)
    ax_r.step(xs, rpm, where="post", color=LIMIT, lw=1.2, label="RPM limit (provider)")
    ax_r.step(xs, [v * utilization for v in rpm], where="post", color=LIMIT, lw=0.9, ls=(0, (1, 2)),
              label=f"gateway target ({utilization:.0%})")
    if calls:
        ax_r.plot(xs, trailing_counts(calls.t, None, xs), color=SERIES[0], label="sent, trailing 60 s (provider log)")
    ax_r.plot(xs, trailing_counts(completions(data, model), None, xs), color=SERIES[1],
              label="completed, trailing 60 s (gateway)")
    ax_r.set_ylabel("requests / 60 s")
    ax_r.set_ylim(bottom=0)
    _legend(ax_r)

    tpm = _limit_steps(lim, xs, 1)
    ax_t.step(xs, tpm, where="post", color=LIMIT, lw=1.2, label="TPM limit")
    if calls:
        ax_t.plot(xs, trailing_counts(calls.t, calls.tokens, xs), color=SERIES[0], label="tokens sent, trailing 60 s")
    ax_t.set_ylabel("tokens / 60 s")
    ax_t.set_ylim(bottom=0)
    _legend(ax_t)

    ts = [s["t"] for s in data.timeline if model in s["models"]]
    ax_q.plot(ts, [s["models"][model]["waiting"] for s in data.timeline if model in s["models"]],
              color=SERIES[0], label="queued")
    ax_q.plot(ts, [s["models"][model]["in_flight"] for s in data.timeline if model in s["models"]],
              color=SERIES[1], label="in flight")
    ax_q.set_ylabel("requests")
    ax_q.set_xlabel("seconds since load started  (shaded: warm-up · dashed: limit change · solid: load stopped)")
    ax_q.set_ylim(bottom=0)
    _legend(ax_q)

    for ax in (ax_r, ax_t, ax_q):
        _decorate(ax, data)
        ax.yaxis.set_major_formatter(FuncFormatter(_human))
    fig.tight_layout()
    path = out / f"{model}.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return path


def latency_chart(data: RunData, out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(9, 3.2))
    for i, model in enumerate(sorted(data.calls)[:3]):
        values = sorted((r.finished - r.submitted) * 1000 for r in data.requests
                        if r.model == model and r.finished is not None and r.state in ("succeeded", "failed"))
        if values:
            ys = [(k + 1) / len(values) for k in range(len(values))]
            ax.plot(values, ys, color=SERIES[i], label=model)
    ax.set_xscale("log")
    ax.set_xlabel("end-to-end latency, submit to final (ms, log scale)")
    ax.set_ylabel("fraction of requests")
    ax.set_title("Latency distribution (includes time spent queued)", loc="left", fontsize=10)
    ax.legend(fontsize=8)
    fig.tight_layout()
    path = out / "latency.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return path


def _fmt(n) -> str:
    return f"{n:,}" if isinstance(n, int) else str(n)


def write_report(data: RunData, checks: list[Check]) -> Path:
    out = data.run_dir
    charts = out / "charts"
    charts.mkdir(exist_ok=True)
    utilization = data.run.get("utilization", 0.97)
    models = sorted(data.calls)
    chart_paths = [model_chart(data, m, charts, utilization) for m in models]
    chart_paths.append(latency_chart(data, charts))

    load = data.load
    duration = data.load_end
    done_rows = [r for r in data.requests if r.state in ("succeeded", "failed") and r.finished is not None]
    last_done = max((r.finished for r in done_rows), default=0.0)
    per_model = {}
    for m in models:
        lim = data.provider_limits[m]
        lim_check = next((c for c in checks if c.name == f"limits:{m}"), None)
        per_model[m] = {
            "configured_initial": {"rpm": lim.rpm[0], "tpm": lim.tpm[0]},
            "limit_changes": [{"t_s": round(t, 1), "rpm": r, "tpm": k}
                              for t, r, k in zip(lim.t[1:], lim.rpm[1:], lim.tpm[1:], strict=True)],
            "observed_peak_60s": {"requests": lim_check.data["peak_requests_60s"] if lim_check else None,
                                  "tokens": lim_check.data["peak_tokens_60s"] if lim_check else None},
            "provider_calls": data.calls[m].kinds,
            "latency_ms": latency_ms(data, m),
        }
    summary = {
        "scenario": data.run["scenario"],
        "passed": all(c.passed for c in checks),
        "checks": [{"name": c.name, "passed": c.passed, "detail": c.detail, "data": c.data} for c in checks],
        "load": {"spec": load["spec"], "counts": load["counts"], "ack_ms": load["ack_ms"],
                 "achieved_offer_rate": load["achieved_offer_rate"], "duration_s": round(duration, 1),
                 "max_schedule_lag_ms": load["max_schedule_lag_ms"]},
        "completed": len(done_rows),
        "last_completion_s": round(last_done, 1),
        "completion_rate_per_s": round(len(done_rows) / last_done, 1) if last_done else 0,
        "latency_ms": latency_ms(data),
        "models": per_model,
        "environment": {"python": sys.version.split()[0], "platform": platform.platform(),
                        "machine": platform.machine(), **data.run.get("environment", {})},
    }
    (out / "report.json").write_text(json.dumps(summary, indent=2, default=str))

    lines = [
        f"# {data.run['scenario']}: {'PASS' if summary['passed'] else 'FAIL'}",
        "",
        data.run.get("description", ""),
        "",
        "## Checks",
        "",
        "| Check | Result | Evidence |",
        "|---|---|---|",
        *[f"| `{c.name}` | {'PASS' if c.passed else '**FAIL**'} | {c.detail} |" for c in checks],
        "",
        "## Measured",
        "",
        f"- Load: {load['spec']['rate']:,} req/s offered for {load['spec']['duration_s']:.0f} s"
        + (f" (achieved {load['achieved_offer_rate']:,} req/s)" if load["spec"]["duration_s"] >= 5 else "")
        + f", batch size {load['spec']['batch_size']}, {load['spec']['tokens']:,} tokens per request, "
        f"models {load['spec']['models']}",
        "- Requests: " + ", ".join(f"{k} {_fmt(v)}" for k, v in load["counts"].items()),
        f"- Completed {summary['completed']:,} requests (succeeded or failed) by t={summary['last_completion_s']} s: "
        f"{summary['completion_rate_per_s']:,} requests/s ({summary['completion_rate_per_s'] * 60:,.0f}/min) "
        "averaged from the start of the load to the last completion",
        f"- Submission ack latency (client side): p50 {load['ack_ms']['p50']} ms, "
        f"p99 {load['ack_ms']['p99']} ms",
        f"- End-to-end latency, submit to final, all models: p50 {summary['latency_ms']['p50']} ms, "
        f"p95 {summary['latency_ms']['p95']} ms, p99 {summary['latency_ms']['p99']} ms",
        "",
        "| Model | Configured RPM / TPM | Peak sent in any 60 s window (requests / tokens) "
        "| Provider 429s | p50 / p95 / p99 latency (ms) |",
        "|---|---|---|---|---|",
    ]
    for m, info in per_model.items():
        cfg = info["configured_initial"]
        changes = "".join(f" → {c['rpm']:,} @ {c['t_s']}s" for c in info["limit_changes"])
        peak = info["observed_peak_60s"]
        lat = info["latency_ms"]
        lines.append(
            f"| {m} | {cfg['rpm']:,}{changes} / {cfg['tpm']:,} | {_fmt(peak['requests'])} / {_fmt(peak['tokens'])} "
            f"| {data.calls[m].rate_limited} | {lat['p50']} / {lat['p95']} / {lat['p99']} |"
        )
    lines += ["", "## Charts", ""]
    lines += [f"![{p.stem}](charts/{p.name})" for p in chart_paths]
    lines += [
        "",
        "## Assumptions (simulation, not measurement)",
        "",
        "- The provider is simulated (`python -m simulator`): latency is log-normal fitted to the configured "
        "p50/p99, and failure outcomes are drawn deterministically from a seeded hash of each request's "
        "idempotency key and attempt.",
        "- The provider counts **estimated** tokens when it admits a call. "
        "Rate-limited (429) calls don't use capacity.",
        "- Limits are checked at every call the provider counted, over the trailing 60 s, against the limit "
        "in effect at that moment (docs/ARCHITECTURE.md §4.1).",
        "- Latency is end to end: it includes time spent queued, which dominates whenever demand exceeds capacity.",
        f"- Environment: {summary['environment']}",
        "",
    ]
    (out / "report.md").write_text("\n".join(lines))
    return out / "report.md"
