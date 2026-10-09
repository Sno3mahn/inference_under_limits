"""Request records and the state machine.

Every state change goes through RequestTracker.transition(). That one function enforces:

- only legal moves happen (e.g. nothing leaves a final state);
- a request becomes final exactly once, so a late or duplicate provider response can't
  count it twice. This is what makes "every request id appears exactly once" hold;
- the per-state counters always add up to the number of requests ever submitted
  (Scenario 1: every request is completed, waiting, expired or rejected).

            ┌────────────── deadline ─────────────▶ EXPIRED
 submit ─▶ QUEUED ─▶ IN_FLIGHT ─▶ SUCCEEDED
   │          ▲          ├──────▶ FAILED
   │          │          └──────▶ RETRY_WAIT ─▶ (back to IN_FLIGHT, or EXPIRED)
   └─▶ REJECTED (never queued; counted, not stored)
"""

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class State(StrEnum):
    QUEUED = "queued"
    IN_FLIGHT = "in_flight"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    EXPIRED = "expired"
    REJECTED = "rejected"

    @property
    def is_final(self) -> bool:
        return self in FINAL_STATES


FINAL_STATES = frozenset({State.SUCCEEDED, State.FAILED, State.EXPIRED, State.REJECTED})

ALLOWED = {
    State.QUEUED: {State.IN_FLIGHT, State.EXPIRED},
    State.IN_FLIGHT: {State.SUCCEEDED, State.FAILED, State.RETRY_WAIT},
    State.RETRY_WAIT: {State.IN_FLIGHT, State.EXPIRED},
}


@dataclass(slots=True, eq=False)
class InferenceRequest:
    key: str  # unique inside the gateway: "<batch_id>:<request_id>" or "single:<request_id>"
    request_id: str  # the client's id
    batch_id: str | None
    model: str
    estimated_tokens: int
    payload: Any
    submitted_ns: int
    deadline_ns: int  # must be sent (an attempt started) before this
    state: State = State.QUEUED
    attempts: int = 0
    retry_at_ns: int = 0
    finished_ns: int | None = None
    result: Any = None
    error: str | None = None  # why it failed / expired / was rejected
    actual_tokens: int | None = None

    def summary(self) -> dict:
        return {
            "request_id": self.request_id,
            "batch_id": self.batch_id,
            "model": self.model,
            "status": self.state.value,
            "attempts": self.attempts,
            "error": self.error,
            "result": self.result,
            "latency_ms": None if self.finished_ns is None
            else round((self.finished_ns - self.submitted_ns) / 1e6, 3),
        }


class IllegalTransition(Exception):
    pass


Listener = Callable[[InferenceRequest, State | None], None]  # (request, previous state or None if new)


@dataclass
class RequestTracker:
    counts: Counter = field(default_factory=Counter)  # (model, state) -> number of requests
    listeners: list[Listener] = field(default_factory=list)

    def add(self, request: InferenceRequest) -> None:
        """Start tracking an accepted request."""
        if request.state is not State.QUEUED:
            raise IllegalTransition(f"new request can't start as {request.state}")
        self.counts[request.model, State.QUEUED] += 1
        self._notify(request, None)

    def count_rejected(self, model: str, n: int = 1) -> None:
        """Requests refused at submission (queue full) are counted but not stored, so a
        client retrying the same request_id after a 429 isn't seen as a duplicate."""
        self.counts[model, State.REJECTED] += n

    def transition(self, request: InferenceRequest, new: State, now_ns: int, **fields) -> bool:
        """Move a request to `new`. Returns False (and changes nothing) if the request is
        already final; that's a late duplicate, not a bug. Raises on any other illegal move."""
        old = request.state
        if old.is_final:
            return False
        if new not in ALLOWED[old]:
            raise IllegalTransition(f"{request.key}: {old} -> {new}")
        for name, value in fields.items():
            setattr(request, name, value)
        request.state = new
        if new.is_final:
            request.finished_ns = now_ns
        self.counts[request.model, old] -= 1
        self.counts[request.model, new] += 1
        self._notify(request, old)
        return True

    def totals(self, model: str | None = None) -> dict[str, int]:
        out = {s.value: 0 for s in State}
        for (m, state), n in self.counts.items():
            if model is None or m == model:
                out[state.value] += n
        return out

    def _notify(self, request: InferenceRequest, old: State | None) -> None:
        for listener in self.listeners:
            listener(request, old)
