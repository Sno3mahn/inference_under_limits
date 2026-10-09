"""Wires everything together: gateway core, store, batches, callbacks, metrics."""

import asyncio
import contextlib
import logging
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from gateway.batches import BatchManager
from gateway.callbacks import CallbackSender, CallbackSettings
from gateway.clock import NS_PER_SECOND, Clock, MonotonicClock
from gateway.core import Gateway, GatewaySettings
from gateway.limiter import Limits
from gateway.provider import HttpProvider, Provider
from gateway.store import Store
from gateway.tracker import InferenceRequest, State

log = logging.getLogger("gateway")


@dataclass
class ServiceConfig:
    models: dict[str, Limits]
    provider_url: str = "http://127.0.0.1:9000"
    provider_timeout_s: float = 10.0
    db_path: str = "../runs/gateway.db"
    gateway: GatewaySettings = field(default_factory=GatewaySettings)
    callbacks: CallbackSettings = field(default_factory=CallbackSettings)
    single_retention_s: float = 600.0  # how long finished single requests stay in memory


def load_model_limits(path: str | Path) -> dict[str, Limits]:
    """Reads rpm/tpm per model from the same YAML the simulator uses."""
    raw = yaml.safe_load(Path(path).read_text())
    return {name: Limits(rpm=int(m["rpm"]), tpm=int(m["tpm"])) for name, m in raw["models"].items()}


class LatencyStats:
    """End-to-end latency (submit -> final) of recent finished requests, per model."""

    def __init__(self, keep: int = 500_000) -> None:
        self._keep = keep
        self._samples: dict[str, deque[float]] = {}

    def add(self, model: str, latency_ms: float) -> None:
        self._samples.setdefault(model, deque(maxlen=self._keep)).append(latency_ms)

    def percentiles(self, model: str | None = None) -> dict:
        if model is None:
            values = [v for samples in self._samples.values() for v in samples]
        else:
            values = list(self._samples.get(model, ()))
        if not values:
            return {"count": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None}
        values.sort()

        def pick(q: float) -> float:
            return round(values[min(len(values) - 1, int(q * len(values)))], 3)

        return {"count": len(values), "p50_ms": pick(0.50), "p95_ms": pick(0.95), "p99_ms": pick(0.99)}


class Service:
    def __init__(self, config: ServiceConfig, provider: Provider | None = None, clock: Clock | None = None):
        self.config = config
        self.clock = clock or MonotonicClock()
        self.store = Store(config.db_path, self.clock)
        self.provider = provider or HttpProvider(config.provider_url, config.provider_timeout_s)
        self.gateway = Gateway(config.models, self.provider, self.clock, config.gateway)
        self.latency = LatencyStats()
        # Order matters: persist a request's final state before the batch listener below
        # can complete its batch, so the request row is queued ahead of the batch row.
        self.gateway.tracker.listeners.append(self._persist_and_measure)
        self.sender = CallbackSender(self.store, self.clock, config.callbacks, on_status=self._callback_status)
        self.batches = BatchManager(self.gateway, self.store, self.sender, self.clock)
        self._singles_done: deque[InferenceRequest] = deque()
        self._evict_task: asyncio.Task | None = None
        self.started_ns = self.clock.now_ns()

    def _callback_status(self, batch_id: str, fields: dict) -> None:
        self.batches.on_callback_status(batch_id, fields)

    def _persist_and_measure(self, request: InferenceRequest, old: State | None) -> None:
        if old is None:
            if request.batch_id is None:  # batch requests are inserted with their batch
                self.store.insert_request(request)
            return
        if not request.state.is_final:
            return
        self.store.finish_request(request)
        if request.state in (State.SUCCEEDED, State.FAILED):
            self.latency.add(request.model, (request.finished_ns - request.submitted_ns) / 1e6)
        # Finished batch requests are served from SQLite from now on, so memory doesn't
        # grow with every request ever processed. Singles stay a while (idempotency window).
        if request.batch_id is not None:
            self.gateway.requests.pop(request.key, None)
        else:
            self._singles_done.append(request)

    async def _evict_singles(self) -> None:
        while True:
            await asyncio.sleep(5)
            cutoff = self.clock.now_ns() - round(self.config.single_retention_s * NS_PER_SECOND)
            while self._singles_done and self._singles_done[0].finished_ns <= cutoff:
                self.gateway.requests.pop(self._singles_done.popleft().key, None)

    async def start(self) -> None:
        self.store.start()
        if isinstance(self.provider, HttpProvider):
            await self.provider.start()
        restored = self.batches.restore()
        if any(restored.values()):
            log.info("recovered after restart: %s", restored)
        await self.gateway.start()
        await self.sender.start()
        self._evict_task = asyncio.create_task(self._evict_singles())

    async def stop(self) -> None:
        if self._evict_task is not None:
            self._evict_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._evict_task
        await self.sender.stop()
        await self.gateway.stop()
        if isinstance(self.provider, HttpProvider):
            await self.provider.close()
        await asyncio.to_thread(self.store.close)

    async def find_single(self, request_id: str) -> dict | None:
        request = self.gateway.requests.get(f"single:{request_id}")
        if request is not None:
            return request.summary()
        rows = await self.store.read(
            "SELECT request_id, model, state, attempts, error, result, submitted_at, finished_at"
            " FROM requests WHERE key=?", (f"single:{request_id}",)
        )
        if not rows:
            return None
        row = rows[0]
        row["status"] = row.pop("state")
        row["batch_id"] = None
        return row

    def metrics(self) -> dict:
        now = self.clock.now_ns()
        return {
            "uptime_s": round((now - self.started_ns) / NS_PER_SECOND, 3),
            "requests": self.gateway.tracker.totals(),
            "latency": self.latency.percentiles(),
            "models": {
                model: self.gateway.model_status(model) | {"latency": self.latency.percentiles(model)}
                for model in self.gateway.schedulers
            },
            "limit_history": [
                {**entry, "t_s": round((entry["t_ns"] - self.started_ns) / NS_PER_SECOND, 3)}
                for entry in self.gateway.limit_history
            ],
            "batches": {
                "total": len(self.batches.batches),
                "completed": sum(b.completed_ns is not None for b in self.batches.batches.values()),
            },
            "callbacks_pending": self.sender.pending,
            "store": {"writes": self.store.writes, "commits": self.store.commits},
        }
