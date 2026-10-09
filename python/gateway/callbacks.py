"""Delivering batch-completion callbacks, with retries.

Delivery is at-least-once: if the receiver processes our POST but its reply gets lost, we
send it again. So every attempt carries the same body and the same X-Event-Id, and the
receiver can dedupe on that. X-Callback-Attempt says which attempt this is.

  2xx                        -> delivered
  408, 429, 5xx, timeout,    -> retry with exponential backoff (1 s, 2 s, 4 s ... capped)
  connection error
  any other 4xx              -> give up (the receiver says the request itself is wrong)

Callbacks run on their own workers, so a slow or dead receiver never slows inference.
"""

import asyncio
import contextlib
import heapq
import itertools
import json
import logging
import random
from collections.abc import Callable
from dataclasses import dataclass

import aiohttp

from gateway.clock import NS_PER_SECOND, Clock
from gateway.store import Store

log = logging.getLogger("gateway.callbacks")

RETRYABLE = {408, 429}


@dataclass
class CallbackSettings:
    timeout_s: float = 5.0
    max_attempts: int = 10
    backoff_base_s: float = 1.0
    backoff_cap_s: float = 60.0
    workers: int = 50


@dataclass
class CallbackJob:
    batch_id: str
    url: str
    body: bytes  # serialised once at completion, so every attempt is byte-identical
    event_id: str
    attempts: int = 0


# (batch_id, fields) -> None; lets the batch manager mirror delivery status
StatusListener = Callable[[str, dict], None]


class CallbackSender:
    def __init__(
        self,
        store: Store,
        clock: Clock,
        settings: CallbackSettings | None = None,
        on_status: StatusListener | None = None,
    ) -> None:
        self._store = store
        self._clock = clock
        self._settings = settings or CallbackSettings()
        self._on_status = on_status or (lambda batch_id, fields: None)
        self._due: list[tuple[int, int, CallbackJob]] = []  # heap of (due_ns, seq, job)
        self._seq = itertools.count()
        self._wake = asyncio.Event()
        self._session: aiohttp.ClientSession | None = None
        self._slots = asyncio.Semaphore(self._settings.workers)
        self._tasks: set[asyncio.Task] = set()
        self._loop_task: asyncio.Task | None = None

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._settings.timeout_s))
        self._loop_task = asyncio.create_task(self._run(), name="callbacks")

    async def stop(self) -> None:
        for task in [t for t in (self._loop_task, *self._tasks) if t is not None]:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._session is not None:
            await self._session.close()

    def schedule(self, job: CallbackJob, due_ns: int | None = None) -> None:
        due = self._clock.now_ns() if due_ns is None else due_ns
        heapq.heappush(self._due, (due, next(self._seq), job))
        self._wake.set()

    @property
    def pending(self) -> int:
        return len(self._due) + len(self._tasks)

    async def _run(self) -> None:
        while True:
            self._wake.clear()
            now = self._clock.now_ns()
            if self._due and self._due[0][0] <= now:
                _, _, job = heapq.heappop(self._due)
                await self._slots.acquire()
                task = asyncio.create_task(self._deliver(job))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
                continue
            timeout = None if not self._due else (self._due[0][0] - now) / NS_PER_SECOND
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout)

    async def _deliver(self, job: CallbackJob) -> None:
        try:
            if job.attempts == 0:
                # The batch's final request rows may still be in the writer's queue. Wait
                # until they're committed, so a client that fetches results the moment
                # the callback lands sees all of them.
                await self._store.barrier()
            job.attempts += 1
            outcome, error = await self._post(job)
            self._record(job, outcome, error)
        finally:
            self._slots.release()

    async def _post(self, job: CallbackJob) -> tuple[str, str | None]:
        """Returns (outcome, error): outcome is 'delivered', 'retry' or 'failed'."""
        assert self._session is not None
        headers = {
            "Content-Type": "application/json",
            "X-Event-Id": job.event_id,
            "X-Callback-Attempt": str(job.attempts),
        }
        try:
            async with self._session.post(job.url, data=job.body, headers=headers) as resp:
                await resp.read()
                if 200 <= resp.status < 300:
                    return "delivered", None
                if resp.status in RETRYABLE or resp.status >= 500:
                    return "retry", f"http_{resp.status}"
                return "failed", f"http_{resp.status}"
        except TimeoutError:
            return "retry", "timeout"
        except (aiohttp.ClientError, OSError) as exc:
            return "retry", f"connection_error:{type(exc).__name__}"

    def _record(self, job: CallbackJob, outcome: str, error: str | None) -> None:
        now = self._clock.now_ns()
        fields: dict = {"attempts": job.attempts, "last_error": error}
        if outcome == "delivered":
            fields |= {"status": "delivered", "delivered_at": self._store.wall(now), "next_attempt_at": None}
            log.info("callback %s delivered on attempt %d", job.batch_id, job.attempts)
        elif outcome == "retry" and job.attempts < self._settings.max_attempts:
            delay_s = min(self._settings.backoff_cap_s, self._settings.backoff_base_s * 2 ** (job.attempts - 1))
            delay_s *= random.uniform(0.9, 1.1)  # small jitter so many callbacks don't retry in lockstep
            due = now + round(delay_s * NS_PER_SECOND)
            fields |= {"status": "pending", "next_attempt_at": self._store.wall(due)}
            self.schedule(job, due)
            log.info("callback %s attempt %d failed (%s), retrying in %.1fs",
                     job.batch_id, job.attempts, error, delay_s)
        else:
            fields |= {"status": "failed", "next_attempt_at": None}
            log.warning("callback %s gave up after %d attempts (%s)", job.batch_id, job.attempts, error)
        self._store.update_callback(job.batch_id, **fields)
        self._on_status(job.batch_id, fields)


def encode(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
