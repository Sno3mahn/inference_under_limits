"""Open-loop load generator.

Open-loop means submissions follow a fixed schedule (request k is due at start + k/rate)
regardless of how fast the gateway answers. A closed-loop generator ("send the next one
when the last returns") quietly lowers the load as soon as the system slows down. That is
called coordinated omission, and it makes overload look far milder than it is.

batch_size == 1 uses POST /v1/requests; batch_size > 1 sends POST /v1/batches.
"""

import asyncio
import json
import logging
import random
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import aiohttp

log = logging.getLogger("loadgen")


@dataclass
class LoadSpec:
    rate: float  # requests per second offered (not batches)
    duration_s: float
    models: dict[str, float]  # model -> weight
    batch_size: int = 1
    tokens: int = 1_000
    token_jitter: float = 0.0  # 0.2 = token sizes uniform within +/-20%
    callback_url: str | None = None  # batches only
    deadline_s: float | None = None
    seed: int = 1
    max_outstanding: int = 4_000  # submissions in flight before we start lagging the schedule
    id_prefix: str = "lg"


@dataclass
class Submission:
    t_wall: float  # when it was sent
    n: int  # requests in it
    status: int | None  # HTTP status, None on transport error
    ack_ms: float
    batch_id: str | None = None
    models: dict[str, int] = field(default_factory=dict)
    index: int = 0  # k: request ids in it are f"{id_prefix}-{k}" or f"{id_prefix}-{k}-{j}"


@dataclass
class LoadResult:
    spec: LoadSpec
    started_wall: float
    ended_wall: float
    submissions: list[Submission]
    max_schedule_lag_ms: float

    def counts(self) -> dict[str, int]:
        out = {"submitted": 0, "accepted": 0, "rejected": 0, "invalid": 0, "errors": 0}
        for s in self.submissions:
            out["submitted"] += s.n
            if s.status in (200, 202):
                out["accepted"] += s.n
            elif s.status == 429:
                out["rejected"] += s.n
            elif s.status is not None and 400 <= s.status < 500:
                out["invalid"] += s.n
            else:
                out["errors"] += s.n
        return out

    def to_json(self) -> dict:
        acks = sorted(s.ack_ms for s in self.submissions)

        def pct(q: float) -> float | None:
            return round(acks[min(len(acks) - 1, int(q * len(acks)))], 3) if acks else None

        return {
            "spec": asdict(self.spec),
            "started_wall": self.started_wall,
            "ended_wall": self.ended_wall,
            "achieved_offer_rate": round(
                sum(s.n for s in self.submissions) / max(1e-9, self.ended_wall - self.started_wall), 1
            ),
            "max_schedule_lag_ms": round(self.max_schedule_lag_ms, 1),
            "counts": self.counts(),
            "ack_ms": {"p50": pct(0.5), "p95": pct(0.95), "p99": pct(0.99), "max": acks[-1] if acks else None},
            "batches": [asdict(s) for s in self.submissions if s.batch_id is not None],
        }


class LoadGenerator:
    def __init__(self, gateway_url: str, spec: LoadSpec) -> None:
        self._url = gateway_url.rstrip("/")
        self._spec = spec
        self._rng = random.Random(spec.seed)
        names = list(spec.models)
        self._models = names
        self._weights = [spec.models[m] for m in names]

    def _tokens(self) -> int:
        jitter = self._spec.token_jitter
        if jitter == 0:
            return self._spec.tokens
        return max(1, round(self._spec.tokens * self._rng.uniform(1 - jitter, 1 + jitter)))

    def _item(self, request_id: str) -> dict:
        item = {
            "request_id": request_id,
            "model": self._rng.choices(self._models, self._weights)[0],
            "estimated_tokens": self._tokens(),
            "payload": {"prompt": f"load test {request_id}"},
        }
        if self._spec.deadline_s is not None:
            item["deadline_s"] = self._spec.deadline_s
        return item

    async def run(self) -> LoadResult:
        spec = self._spec
        interval = spec.batch_size / spec.rate
        total = max(1, round(spec.duration_s * spec.rate / spec.batch_size))
        submissions: list[Submission] = []
        tasks: set[asyncio.Task] = set()
        slots = asyncio.Semaphore(spec.max_outstanding)
        max_lag = 0.0

        connector = aiohttp.TCPConnector(limit=spec.max_outstanding)
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            started_wall, start = time.time(), time.monotonic()
            for k in range(total):
                due = start + k * interval
                delay = due - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                else:
                    max_lag = max(max_lag, -delay * 1000)
                await slots.acquire()
                if spec.batch_size == 1:
                    coro = self._single(session, k)
                else:
                    coro = self._batch(session, k)
                task = asyncio.create_task(coro)
                tasks.add(task)
                task.add_done_callback(lambda t: (tasks.discard(t), slots.release(), submissions.append(t.result())))
            if tasks:
                await asyncio.gather(*tasks)
            ended_wall = time.time()
        return LoadResult(spec, started_wall, ended_wall, submissions, max_lag)

    async def _post(self, session: aiohttp.ClientSession, path: str, body: dict, n: int, models: dict, k: int):
        sent = time.time()
        t0 = time.monotonic()
        try:
            async with session.post(self._url + path, json=body) as resp:
                data = await resp.json(content_type=None)
                status = resp.status
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            log.debug("submission failed: %r", exc)
            return Submission(sent, n, None, (time.monotonic() - t0) * 1000, models=models, index=k)
        batch_id = data.get("batch_id") if isinstance(data, dict) and status in (200, 202) else None
        return Submission(sent, n, status, (time.monotonic() - t0) * 1000, batch_id, models, k)

    async def _single(self, session: aiohttp.ClientSession, k: int) -> Submission:
        item = self._item(f"{self._spec.id_prefix}-{k}")
        return await self._post(session, "/v1/requests", item, 1, {item["model"]: 1}, k)

    async def _batch(self, session: aiohttp.ClientSession, k: int) -> Submission:
        items = [self._item(f"{self._spec.id_prefix}-{k}-{j}") for j in range(self._spec.batch_size)]
        models: dict[str, int] = {}
        for item in items:
            models[item["model"]] = models.get(item["model"], 0) + 1
        body: dict = {"requests": items}
        if self._spec.callback_url:
            body["callback_url"] = self._spec.callback_url
        return await self._post(session, "/v1/batches", body, len(items), models, k)


class MetricsSampler:
    """Polls GET /v1/metrics once a second and appends a compact line to a JSONL file:
    the gateway's own view of limits, trailing-60 s usage, queue and in-flight over time."""

    def __init__(self, gateway_url: str, path: Path, interval_s: float = 1.0) -> None:
        self._url = gateway_url.rstrip("/") + "/v1/metrics"
        self._path = path
        self._interval = interval_s
        self.last: dict | None = None

    async def run(self) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            with open(self._path, "a") as out:
                while True:
                    t = time.time()
                    try:
                        async with session.get(self._url) as resp:
                            metrics = await resp.json()
                    except (aiohttp.ClientError, TimeoutError, ValueError):
                        metrics = None
                    if metrics is not None:
                        self.last = metrics
                        out.write(json.dumps({"t_wall": t, **compact(metrics)}) + "\n")
                        out.flush()
                    await asyncio.sleep(max(0.0, self._interval - (time.time() - t)))


def compact(metrics: dict) -> dict:
    return {
        "requests": metrics["requests"],
        "callbacks_pending": metrics.get("callbacks_pending", 0),
        "models": {
            name: {
                "limits": m["limits"],
                "last_60s": m["last_60s"],
                "waiting": m["queue"]["waiting"],
                "in_flight": m["in_flight"],
                "requests": m["requests"],
            }
            for name, m in metrics["models"].items()
        },
    }


async def drain(gateway_url: str, timeout_s: float, need_callbacks: bool = False) -> bool:
    """Wait until the gateway has no queued, in-flight or retrying work (and, optionally,
    no undelivered callbacks). Returns False if that didn't happen within the timeout."""
    url = gateway_url.rstrip("/") + "/v1/metrics"
    end = time.monotonic() + timeout_s
    async with aiohttp.ClientSession() as session:
        while time.monotonic() < end:
            async with session.get(url) as resp:
                m = await resp.json()
            r = m["requests"]
            busy = r["queued"] + r["in_flight"] + r["retry_wait"]
            if busy == 0 and (not need_callbacks or m["callbacks_pending"] == 0):
                return True
            await asyncio.sleep(0.5)
    return False
