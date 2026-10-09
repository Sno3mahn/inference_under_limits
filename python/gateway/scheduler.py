"""One scheduler per model: decides *when* each request is sent.

The loop, in plain words:

    pick the next request   (a retry whose backoff is over, else the oldest new request)
    if it's past its deadline       -> EXPIRED, next
    wait for a free in-flight slot  (so a slow provider can't make us open endless calls)
    ask the limiter                 -> send now, or sleep until it says / something changes
    send it in the background and loop again

Each model has its own loop, queue and limiter, so a throttled model A never holds up
model B (no head-of-line blocking between models).

The loop sleeps on an asyncio.Event with a timeout, so it wakes early when anything that
could change the answer happens: a new request, a limit change, a retry becoming due,
or an in-flight slot freeing up.
"""

import asyncio
import contextlib
import heapq
import itertools
import logging
import random
from collections import deque
from dataclasses import dataclass

from gateway.clock import NS_PER_SECOND, Clock
from gateway.limiter import Limits, RateLimiter
from gateway.provider import Provider, ProviderResult, Verdict
from gateway.tracker import InferenceRequest, RequestTracker, State

log = logging.getLogger("gateway.scheduler")


@dataclass
class SchedulerSettings:
    utilization: float = 0.97  # aim for this fraction of the configured limits
    burst_s: float = 1.0  # pacer burst, in seconds of traffic
    max_in_flight: int = 2_000  # per model; ~ rate x p99 latency x 2 (Little's law)
    max_attempts: int = 5  # total attempts per request, including the first
    backoff_base_s: float = 0.2
    backoff_cap_s: float = 10.0
    max_provider_pause_s: float = 5.0  # longest we pause a model after a provider 429
    queue_minutes: float = 1.0  # queue bound = this many minutes of work at current RPM
    min_queue: int = 1_000  # ...but never smaller than this
    sweep_interval_s: float = 1.0  # how often queued requests are checked for deadlines


class ModelScheduler:
    def __init__(
        self,
        model: str,
        limits: Limits,
        provider: Provider,
        tracker: RequestTracker,
        clock: Clock,
        settings: SchedulerSettings,
    ) -> None:
        self.model = model
        self.limiter = RateLimiter(limits, clock, settings.utilization, settings.burst_s)
        self._provider = provider
        self._tracker = tracker
        self._clock = clock
        self._settings = settings

        self._fresh: deque[InferenceRequest] = deque()
        self._retries: list[tuple[int, int, InferenceRequest]] = []  # heap of (retry_at, seq, req)
        self._seq = itertools.count()
        self._in_flight = 0
        self._paused_until_ns = 0
        self._wake = asyncio.Event()
        self._tasks: set[asyncio.Task] = set()
        self._loop_task: asyncio.Task | None = None
        self._sweep_task: asyncio.Task | None = None

    # --- control -----------------------------------------------------------------

    def start(self) -> None:
        self._loop_task = asyncio.create_task(self._run(), name=f"scheduler:{self.model}")
        self._sweep_task = asyncio.create_task(self._sweep(), name=f"sweeper:{self.model}")

    async def stop(self) -> None:
        tasks = [t for t in (self._loop_task, self._sweep_task, *self._tasks) if t is not None]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

    def set_limits(self, limits: Limits) -> None:
        self.limiter.set_limits(limits)
        self._wake.set()

    # --- intake ------------------------------------------------------------------

    @property
    def waiting(self) -> int:
        return self._tracker.counts[self.model, State.QUEUED]

    @property
    def in_flight(self) -> int:
        return self._in_flight

    def queue_capacity(self) -> int:
        """How many QUEUED requests we accept. Scales with the current limit, so after a
        limit drop the queue stops growing past what we could drain in time."""
        rpm = self.limiter.limits.rpm
        return max(self._settings.min_queue, int(self._settings.queue_minutes * rpm))

    def free_queue_slots(self) -> int:
        return max(0, self.queue_capacity() - self.waiting)

    def enqueue(self, request: InferenceRequest) -> None:
        """Caller has already added the request to the tracker as QUEUED."""
        self._fresh.append(request)
        self._wake.set()

    # --- the loop ----------------------------------------------------------------

    async def _run(self) -> None:
        while True:
            self._wake.clear()
            now = self._clock.now_ns()
            request, from_retries = self._peek(now)

            if request is None:
                await self._sleep_until(self._next_retry_ns())
                continue

            if now >= request.deadline_ns:
                self._pop(from_retries)
                self._tracker.transition(request, State.EXPIRED, now, error=self._expiry_reason(request))
                continue

            if now < self._paused_until_ns:
                await self._sleep_until(self._paused_until_ns)
                continue

            # Take a slot *before* charging the limiter: otherwise we'd use up rate
            # capacity on a request we can't actually send yet.
            if self._in_flight >= self._settings.max_in_flight:
                await self._sleep_until(None)  # woken when a call finishes
                continue

            wait = self.limiter.try_acquire(request.estimated_tokens)
            if wait == 0:
                self._pop(from_retries)
                self._send(request, now)
                continue

            # Blocked. wait=None means it can't fit under the current limits at all
            # (limit is 0, or the request is bigger than TPM): wait for a limit change.
            until = request.deadline_ns if wait is None else min(now + wait, request.deadline_ns)
            await self._sleep_until(until)

    def _peek(self, now: int) -> tuple[InferenceRequest | None, bool]:
        """Next request to consider. Due retries go first: they belong to work someone is
        already waiting on, and they're capped by max_attempts so they can't take over.
        Requests the sweeper already expired are dropped here (lazy deletion)."""
        while self._retries and self._retries[0][2].state is not State.RETRY_WAIT:
            heapq.heappop(self._retries)
        if self._retries and self._retries[0][0] <= now:
            return self._retries[0][2], True
        while self._fresh and self._fresh[0].state is not State.QUEUED:
            self._fresh.popleft()
        if self._fresh:
            return self._fresh[0], False
        return None, False

    def _pop(self, from_retries: bool) -> None:
        if from_retries:
            heapq.heappop(self._retries)
        else:
            self._fresh.popleft()

    def _next_retry_ns(self) -> int | None:
        return self._retries[0][0] if self._retries else None

    async def _sleep_until(self, until_ns: int | None) -> None:
        """Sleep until `until_ns` (None = indefinitely), or until woken."""
        timeout = None
        if until_ns is not None:
            timeout = max(0, until_ns - self._clock.now_ns()) / NS_PER_SECOND
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout)

    # --- sending and handling results --------------------------------------------

    def _send(self, request: InferenceRequest, now: int) -> None:
        self._tracker.transition(request, State.IN_FLIGHT, now, attempts=request.attempts + 1)
        self._in_flight += 1
        task = asyncio.create_task(self._call(request))
        self._tasks.add(task)  # keep a reference so the task isn't garbage-collected
        task.add_done_callback(self._tasks.discard)

    async def _call(self, request: InferenceRequest) -> None:
        try:
            result = await self._provider.call(request)
        except Exception as exc:  # providers shouldn't raise, but a bug must not lose a request
            log.exception("provider call raised for %s", request.key)
            result = ProviderResult(Verdict.TRANSIENT, None, f"internal_error:{type(exc).__name__}")
        finally:
            self._in_flight -= 1
            self._wake.set()
        self._handle(request, result)

    def _handle(self, request: InferenceRequest, result: ProviderResult) -> None:
        now = self._clock.now_ns()
        if result.verdict is Verdict.SUCCESS:
            if result.actual_tokens is not None:
                self.limiter.reconcile(request.estimated_tokens, result.actual_tokens)
            self._tracker.transition(
                request, State.SUCCEEDED, now,
                result=result.body, actual_tokens=result.actual_tokens, error=None,
            )
            return

        if result.verdict is Verdict.PERMANENT:
            self._tracker.transition(request, State.FAILED, now, error=result.reason)
            return

        # Transient.
        if result.status == 429:
            # Our limiter thought there was room but the provider disagrees: its view of
            # capacity is the real one. Pause the whole model briefly, not just this request.
            pause_s = min(result.retry_after_s or 1.0, self._settings.max_provider_pause_s)
            self._paused_until_ns = max(self._paused_until_ns, now + round(pause_s * NS_PER_SECOND))
            log.warning("%s: provider 429, pausing %.1fs", self.model, pause_s)

        if request.attempts >= self._settings.max_attempts:
            self._tracker.transition(
                request, State.FAILED, now, error=f"retries_exhausted:{result.reason}"
            )
            return

        delay_s = self._backoff_s(request.attempts)
        if result.retry_after_s is not None:
            delay_s = max(delay_s, result.retry_after_s)
        retry_at = now + round(delay_s * NS_PER_SECOND)
        self._tracker.transition(request, State.RETRY_WAIT, now, retry_at_ns=retry_at, error=result.reason)
        heapq.heappush(self._retries, (retry_at, next(self._seq), request))
        self._wake.set()

    def _backoff_s(self, attempts_so_far: int) -> float:
        """Exponential backoff with full jitter: random in [0, min(cap, base * 2^(n-1))].
        The jitter spreads retries out so failures that happened together don't all
        retry together."""
        ceiling = min(self._settings.backoff_cap_s, self._settings.backoff_base_s * 2 ** (attempts_so_far - 1))
        return random.uniform(0, ceiling)

    # --- deadlines ---------------------------------------------------------------

    async def _sweep(self) -> None:
        """Expire waiting requests whose deadline passed, even if they aren't at the front.
        Without this, a request stuck behind others would show as 'queued' long after it
        should have been reported as expired."""
        while True:
            await asyncio.sleep(self._settings.sweep_interval_s)
            now = self._clock.now_ns()
            for request in itertools.chain(self._fresh, (entry[2] for entry in self._retries)):
                if request.state in (State.QUEUED, State.RETRY_WAIT) and now >= request.deadline_ns:
                    self._tracker.transition(request, State.EXPIRED, now, error=self._expiry_reason(request))

    @staticmethod
    def _expiry_reason(request: InferenceRequest) -> str:
        if request.state is State.RETRY_WAIT:
            return f"deadline_exceeded_during_retry:{request.error}"
        return "deadline_exceeded_in_queue"
