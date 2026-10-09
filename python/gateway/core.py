"""The gateway: validation, admission, per-model schedulers, live limits.

This is the layer the HTTP API calls. It has no HTTP in it, so it can be tested directly.
"""

import asyncio
import contextlib
import math
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from gateway.clock import NS_PER_SECOND, Clock
from gateway.limiter import Limits
from gateway.provider import Provider
from gateway.scheduler import ModelScheduler, SchedulerSettings
from gateway.tracker import InferenceRequest, RequestTracker, State


@dataclass(frozen=True)
class RequestItem:
    request_id: str
    model: str
    estimated_tokens: int
    payload: Any = None
    deadline_s: float | None = None  # overrides the default queue deadline


@dataclass
class GatewaySettings:
    scheduler: SchedulerSettings = field(default_factory=SchedulerSettings)
    default_deadline_s: float = 120.0
    max_deadline_s: float = 3_600.0
    max_batch_size: int = 50_000


class SubmissionError(Exception):
    status = 400

    def __init__(self, message: str, details: list[str] | None = None) -> None:
        super().__init__(message)
        self.details = details or []


class InvalidSubmission(SubmissionError):
    status = 400


class OverCapacity(SubmissionError):
    status = 429

    def __init__(self, message: str, retry_after_s: int) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


class Gateway:
    def __init__(
        self,
        limits: dict[str, Limits],
        provider: Provider,
        clock: Clock,
        settings: GatewaySettings | None = None,
    ) -> None:
        self.clock = clock
        self.settings = settings or GatewaySettings()
        self.tracker = RequestTracker()
        self.tracker.listeners.append(self._on_transition)
        self.schedulers = {
            model: ModelScheduler(model, lim, provider, self.tracker, clock, self.settings.scheduler)
            for model, lim in limits.items()
        }
        self.requests: dict[str, InferenceRequest] = {}  # key -> request
        self.limit_history: list[dict] = [
            {"t_ns": clock.now_ns(), "model": m, "rpm": lim.rpm, "tpm": lim.tpm} for m, lim in limits.items()
        ]
        self._waiters: dict[str, list[asyncio.Future]] = {}

    async def start(self) -> None:
        for scheduler in self.schedulers.values():
            scheduler.start()

    async def stop(self) -> None:
        for scheduler in self.schedulers.values():
            await scheduler.stop()

    # --- submission --------------------------------------------------------------

    def submit_single(self, item: RequestItem) -> tuple[InferenceRequest, bool]:
        """Returns (request, created). Submitting a request_id that already exists returns
        the existing request instead of running it twice (idempotent retries)."""
        key = f"single:{item.request_id}"
        if key in self.requests:
            return self.requests[key], False
        errors = self._validate([item])
        if errors:
            raise InvalidSubmission("invalid request", errors)
        self._admit({item.model: 1})
        request = self._create(key, None, item)
        self.enqueue([request])
        return request, True

    def submit_batch(
        self, batch_id: str, items: list[RequestItem], enqueue: bool = True
    ) -> list[InferenceRequest]:
        """All or nothing: either every request is accepted, or none is.

        With enqueue=False the requests are accepted (and hold their queue slots) but not
        handed to the schedulers yet; call enqueue() once they're safely persisted."""
        if not items:
            raise InvalidSubmission("a batch needs at least one request")
        if len(items) > self.settings.max_batch_size:
            raise InvalidSubmission(f"batch larger than {self.settings.max_batch_size}")
        errors = self._validate(items)
        seen: Counter = Counter(item.request_id for item in items)
        errors += [f"duplicate request_id {rid!r} ({n}x)" for rid, n in seen.items() if n > 1]
        if errors:
            raise InvalidSubmission("invalid batch", errors[:100])
        self._admit(Counter(item.model for item in items))
        requests = [self._create(f"{batch_id}:{item.request_id}", batch_id, item) for item in items]
        if enqueue:
            self.enqueue(requests)
        return requests

    def enqueue(self, requests: list[InferenceRequest]) -> None:
        for request in requests:
            self.schedulers[request.model].enqueue(request)

    def restore(self, request: InferenceRequest) -> None:
        """Re-queue a request recovered from the store after a restart."""
        self.requests[request.key] = request
        self.tracker.add(request)
        if request.model in self.schedulers:
            self.schedulers[request.model].enqueue(request)
        else:  # model was removed from the config since: it can never run
            self.tracker.transition(request, State.EXPIRED, self.clock.now_ns(), error="model_no_longer_configured")

    def _validate(self, items: list[RequestItem]) -> list[str]:
        errors = []
        for item in items:
            where = f"request {item.request_id!r}"
            if not item.request_id:
                errors.append("request_id must be non-empty")
            scheduler = self.schedulers.get(item.model)
            if scheduler is None:
                errors.append(f"{where}: unknown model {item.model!r}")
                continue
            tokens = item.estimated_tokens
            if not isinstance(tokens, int) or isinstance(tokens, bool) or tokens <= 0:
                errors.append(f"{where}: estimated_tokens must be a positive integer")
            elif not scheduler.limiter.can_ever_fit(tokens):
                # Otherwise it would sit at the front of the queue forever, blocking the model.
                errors.append(f"{where}: {tokens} tokens can never fit model {item.model!r}'s current TPM")
            if item.deadline_s is not None and not 0 < item.deadline_s <= self.settings.max_deadline_s:
                errors.append(f"{where}: deadline_s must be in (0, {self.settings.max_deadline_s}]")
        return errors

    def _admit(self, needed: dict[str, int]) -> None:
        """Check every model has queue room before accepting anything."""
        short = {
            model: n for model, n in needed.items() if self.schedulers[model].free_queue_slots() < n
        }
        if not short:
            return
        for model, n in needed.items():
            self.tracker.count_rejected(model, n)
        retry_after = max(self._drain_estimate_s(model, n) for model, n in short.items())
        raise OverCapacity(f"queue full for {sorted(short)}", retry_after)

    def _drain_estimate_s(self, model: str, n: int) -> int:
        """Rough seconds until `n` queue slots free up, for the Retry-After header."""
        scheduler = self.schedulers[model]
        rpm = scheduler.limiter.limits.rpm
        if rpm == 0:
            return 60
        overflow = scheduler.waiting + n - scheduler.queue_capacity()
        return max(1, min(60, math.ceil(overflow / (rpm / 60))))

    def _create(self, key: str, batch_id: str | None, item: RequestItem) -> InferenceRequest:
        now = self.clock.now_ns()
        deadline_s = item.deadline_s or self.settings.default_deadline_s
        request = InferenceRequest(
            key=key,
            request_id=item.request_id,
            batch_id=batch_id,
            model=item.model,
            estimated_tokens=item.estimated_tokens,
            payload=item.payload,
            submitted_ns=now,
            deadline_ns=now + round(deadline_s * NS_PER_SECOND),
        )
        self.requests[key] = request
        self.tracker.add(request)
        return request

    # --- waiting for a result ----------------------------------------------------

    async def wait_final(self, request: InferenceRequest, timeout_s: float) -> bool:
        """Wait until the request is final. Returns False on timeout."""
        if request.state.is_final:
            return True
        future = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(request.key, []).append(future)
        try:
            await asyncio.wait_for(asyncio.shield(future), timeout_s)
            return True
        except TimeoutError:
            return False
        finally:
            waiters = self._waiters.get(request.key, [])
            with contextlib.suppress(ValueError):
                waiters.remove(future)
            if not waiters:
                self._waiters.pop(request.key, None)

    def _on_transition(self, request: InferenceRequest, old: State | None) -> None:
        if request.state.is_final:
            for future in self._waiters.get(request.key, []):
                if not future.done():
                    future.set_result(None)

    # --- limits and status -------------------------------------------------------

    def set_limits(self, model: str, limits: Limits) -> None:
        self.schedulers[model].set_limits(limits)  # KeyError for unknown models
        self.limit_history.append(
            {"t_ns": self.clock.now_ns(), "model": model, "rpm": limits.rpm, "tpm": limits.tpm}
        )

    def model_status(self, model: str) -> dict:
        scheduler = self.schedulers[model]
        rpm_cap, tpm_cap = scheduler.limiter.caps
        window_requests, window_tokens = scheduler.limiter.usage()
        return {
            "limits": {"rpm": scheduler.limiter.limits.rpm, "tpm": scheduler.limiter.limits.tpm},
            "targets": {"rpm": rpm_cap, "tpm": tpm_cap},
            "last_60s": {"requests": window_requests, "tokens": window_tokens},
            "queue": {"waiting": scheduler.waiting, "capacity": scheduler.queue_capacity()},
            "in_flight": scheduler.in_flight,
            "requests": self.tracker.totals(model),
        }
