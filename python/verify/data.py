"""Load everything a run left behind in its directory.

  provider-calls.jsonl   every call the simulated provider saw (the primary evidence)
  gateway.db             every request's final state and timing
  gateway_timeline.jsonl the gateway's own metrics, sampled each second
  loadgen.json           what was offered, and what the gateway answered
  callbacks.jsonl        every callback delivery attempt the receiver saw
  run.json               scenario config, start time, limit changes as applied

All times are converted to seconds since the run started (t0), using wall-clock time,
since each process has its own monotonic clock.
"""

import bisect
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class ModelCalls:
    """Calls that counted against the provider's limits, sorted by time."""

    t: list[float] = field(default_factory=list)
    tokens: list[int] = field(default_factory=list)
    attempts: list[int] = field(default_factory=list)
    kinds: dict[str, int] = field(default_factory=dict)  # every call, counted or not
    rate_limited: int = 0


@dataclass
class LimitTimeline:
    """Step function of (t, rpm, tpm); the limit in effect at time t is the last step <= t."""

    t: list[float] = field(default_factory=list)
    rpm: list[int] = field(default_factory=list)
    tpm: list[int] = field(default_factory=list)

    def at(self, t: float) -> tuple[int, int]:
        i = max(0, bisect.bisect_right(self.t, t) - 1)
        return self.rpm[i], self.tpm[i]


@dataclass
class RequestRow:
    model: str
    state: str
    batch_id: str | None
    request_id: str
    attempts: int
    submitted: float  # seconds since t0
    finished: float | None


@dataclass
class RunData:
    run_dir: Path
    run: dict
    t0: float  # wall time the load started
    calls: dict[str, ModelCalls]
    provider_limits: dict[str, LimitTimeline]
    requests: list[RequestRow]
    batches: list[dict]
    timeline: list[dict]
    load: dict
    callbacks: list[dict]

    @property
    def load_end(self) -> float:
        return self.load["ended_wall"] - self.t0

    def json_file(self, name: str) -> dict | list | None:
        path = self.run_dir / name
        return json.loads(path.read_text()) if path.exists() else None


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def load_run(run_dir: str | Path) -> RunData:
    run_dir = Path(run_dir)
    run = json.loads((run_dir / "run.json").read_text())
    load = json.loads((run_dir / "loadgen.json").read_text())
    t0 = load["started_wall"]

    calls: dict[str, ModelCalls] = {}
    limits: dict[str, LimitTimeline] = {}
    start_wall = start_ns = None
    with open(run_dir / "provider-calls.jsonl") as f:
        for line in f:
            rec = json.loads(line)
            kind = rec["type"]
            if kind == "start":
                start_wall = datetime.fromisoformat(rec["wall_time"]).timestamp()
                start_ns = rec["t_ns"]
                continue
            t = start_wall + (rec["t_ns"] - start_ns) / 1e9 - t0
            model = rec["model"]
            if kind == "limits":
                lim = limits.setdefault(model, LimitTimeline())
                lim.t.append(t)
                lim.rpm.append(rec["rpm"])
                lim.tpm.append(rec["tpm"])
                continue
            mc = calls.setdefault(model, ModelCalls())
            mc.kinds[rec["kind"]] = mc.kinds.get(rec["kind"], 0) + 1
            if rec["kind"] == "rate_limited":
                mc.rate_limited += 1
            if rec["counted"]:
                mc.t.append(t)
                mc.tokens.append(rec["tokens"])
                mc.attempts.append(rec["attempt"])
    for lim in limits.values():
        lim.t[0] = float("-inf")  # the initial limits apply from the beginning of time

    db = sqlite3.connect(run_dir / "gateway.db")
    db.row_factory = sqlite3.Row
    requests = [
        RequestRow(
            r["model"], r["state"], r["batch_id"], r["request_id"], r["attempts"],
            r["submitted_at"] - t0, None if r["finished_at"] is None else r["finished_at"] - t0,
        )
        for r in db.execute(
            "SELECT model, state, batch_id, request_id, attempts, submitted_at, finished_at FROM requests"
        )
    ]
    batches = [dict(r) for r in db.execute(
        "SELECT b.*, c.status AS callback_status, c.attempts AS callback_attempts,"
        " c.delivered_at AS callback_delivered_at FROM batches b LEFT JOIN callbacks c USING (batch_id)"
    )]
    db.close()

    timeline = _read_jsonl(run_dir / "gateway_timeline.jsonl")
    for sample in timeline:
        sample["t"] = sample["t_wall"] - t0
    callbacks = _read_jsonl(run_dir / "callbacks.jsonl")
    for delivery in callbacks:
        delivery["t"] = delivery["t_wall"] - t0

    return RunData(run_dir, run, t0, calls, limits, requests, batches, timeline, load, callbacks)
