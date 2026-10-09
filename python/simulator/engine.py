"""The simulated provider's decisions, with no I/O, so it can be tested on a fake clock.

For each call it decides: rate limited or admitted? If admitted, success or which failure?
How long does it take? How many tokens did it really use?

Two details make runs repeatable and realistic:

- Randomness comes from hash(seed, idempotency key, attempt), not a shared RNG. With hundreds
  of calls in flight the arrival order changes between runs; a shared RNG would then give
  different outcomes to the same request. Hashing makes outcomes a function of the request.
- Permanent failures depend only on the key, so retrying a bad request always fails again.
  Transient failures also depend on the attempt number, so a retry can succeed.
"""

import hashlib
import math
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from statistics import NormalDist

from gateway.clock import NS_PER_SECOND, Clock
from simulator.calllog import CallLog
from simulator.config import ModelConfig, SimConfig, model_from_dict

WINDOW_NS = 60 * NS_PER_SECOND
Z99 = NormalDist().inv_cdf(0.99)  # ~2.326: how many standard deviations p99 sits above p50
MAX_IDEMPOTENCY_ENTRIES = 2_000_000


@dataclass(frozen=True)
class Outcome:
    status: int  # HTTP status the provider answers with
    kind: str  # success | replay | rate_limited | transient:<k> | permanent:<k> | invalid | unknown_model
    latency_ns: int = 0
    actual_tokens: int = 0
    retry_after_s: int | None = None
    hang: bool = False  # "timeout": the work happens, but the answer comes far too late


class _Window:
    """The provider's own trailing-window counter.

    Deliberately written separately from gateway.limiter.SlidingWindow, so the two act as
    independent checks of each other: if the gateway sends too fast, this one says 429.
    """

    def __init__(self) -> None:
        self.entries: deque[tuple[int, int]] = deque()  # (t_ns, tokens)
        self.tokens = 0

    def expire(self, now_ns: int) -> None:
        while self.entries and self.entries[0][0] <= now_ns - WINDOW_NS:
            self.tokens -= self.entries.popleft()[1]

    def admits(self, now_ns: int, tokens: int, rpm: int, tpm: int) -> bool:
        self.expire(now_ns)
        return len(self.entries) + 1 <= rpm and self.tokens + tokens <= tpm

    def add(self, now_ns: int, tokens: int) -> None:
        self.entries.append((now_ns, tokens))
        self.tokens += tokens

    def retry_after_s(self, now_ns: int) -> int:
        if not self.entries:
            return 1
        return max(1, math.ceil((self.entries[0][0] + WINDOW_NS - now_ns) / NS_PER_SECOND))


@dataclass
class _ModelState:
    config: ModelConfig
    window: _Window = field(default_factory=_Window)
    counts: dict[str, int] = field(default_factory=dict)


class ProviderSim:
    def __init__(self, config: SimConfig, clock: Clock, log: CallLog | None = None) -> None:
        self._seed = config.seed
        self._hang_ns = round(config.hang_s * NS_PER_SECOND)
        self._clock = clock
        self._log = log or CallLog(None)
        self._models = {name: _ModelState(cfg) for name, cfg in config.models.items()}
        # idempotency key -> actual tokens of the successful call (LRU-bounded)
        self._completed: OrderedDict[str, int] = OrderedDict()
        self._log.start(clock.now_ns())
        for name, state in self._models.items():
            self._log_limits(name, state.config)

    # --- admin ---------------------------------------------------------------------

    def models(self) -> dict[str, ModelConfig]:
        return {name: state.config for name, state in self._models.items()}

    def update_model(self, name: str, changes: dict) -> ModelConfig:
        """Change a model's config while running (limits, latency, failure rates)."""
        if name in self._models:
            state = self._models[name]
            state.config = state.config.updated(changes)
        else:
            state = self._models[name] = _ModelState(model_from_dict(changes))
        self._log_limits(name, state.config)
        return state.config

    def stats(self) -> dict:
        now = self._clock.now_ns()
        out = {}
        for name, state in self._models.items():
            state.window.expire(now)
            out[name] = {
                "rpm": state.config.rpm,
                "tpm": state.config.tpm,
                "window_requests": len(state.window.entries),
                "window_tokens": state.window.tokens,
                "counts": dict(state.counts),
            }
        return out

    # --- the call ------------------------------------------------------------------

    def handle(
        self,
        model: str,
        request_id: str,
        estimated_tokens: int,
        attempt: int = 1,
        idempotency_key: str | None = None,
    ) -> Outcome:
        now = self._clock.now_ns()
        state = self._models.get(model)

        def finish(outcome: Outcome, counted: bool) -> Outcome:
            if state is not None:
                state.counts[outcome.kind] = state.counts.get(outcome.kind, 0) + 1
            self._log.write({
                "type": "call", "t_ns": now, "model": model, "request_id": request_id,
                "attempt": attempt, "tokens": estimated_tokens, "status": outcome.status,
                "kind": outcome.kind, "counted": counted,
            })
            return outcome

        if state is None:
            return finish(Outcome(404, "unknown_model"), counted=False)
        if not isinstance(estimated_tokens, int) or estimated_tokens <= 0:
            return finish(Outcome(400, "invalid"), counted=False)

        key = idempotency_key or request_id
        if key in self._completed:
            # Seen and finished before (e.g. the gateway timed out and retried). Return the
            # original result without doing, or charging, the work again.
            self._completed.move_to_end(key)
            replay = Outcome(200, "replay", latency_ns=1_000_000, actual_tokens=self._completed[key])
            return finish(replay, counted=False)

        cfg = state.config
        if not state.window.admits(now, estimated_tokens, cfg.rpm, cfg.tpm):
            limited = Outcome(429, "rate_limited", retry_after_s=state.window.retry_after_s(now))
            return finish(limited, counted=False)
        state.window.add(now, estimated_tokens)

        # Randomness is keyed on the idempotency key, not request_id: request ids are only
        # unique inside a batch, so two batches both containing "req-1" must not share fate.
        latency_ns = self._latency_ns(cfg, key, attempt)
        permanent = self._pick(cfg.permanent, self._uniform(key, "permanent"))
        if permanent is not None:
            return finish(Outcome(int(permanent), f"permanent:{permanent}", latency_ns), counted=True)

        transient = self._pick(cfg.transient, self._uniform(key, attempt, "transient"))
        if transient is not None and transient != "timeout":
            retry_after = 1 if transient == "429" else None
            failed = Outcome(int(transient), f"transient:{transient}", latency_ns,
                             retry_after_s=retry_after)
            return finish(failed, counted=True)

        # Success, or a timeout: the work is done either way, but a timeout answers so late
        # that the gateway gives up. Its retry then gets a replay of this result.
        actual = self._actual_tokens(cfg, key, estimated_tokens)
        self._remember(key, actual)
        if transient == "timeout":
            return finish(Outcome(200, "transient:timeout", self._hang_ns, actual, hang=True), counted=True)
        return finish(Outcome(200, "success", latency_ns, actual), counted=True)

    # --- helpers -------------------------------------------------------------------

    def _log_limits(self, name: str, cfg: ModelConfig) -> None:
        self._log.write({"type": "limits", "t_ns": self._clock.now_ns(), "model": name,
                         "rpm": cfg.rpm, "tpm": cfg.tpm})

    def _remember(self, key: str, actual_tokens: int) -> None:
        self._completed[key] = actual_tokens
        if len(self._completed) > MAX_IDEMPOTENCY_ENTRIES:
            self._completed.popitem(last=False)

    def _uniform(self, *parts) -> float:
        """Deterministic number in (0, 1) derived from the seed and the given parts."""
        text = "|".join(map(str, (self._seed, *parts))).encode()
        n = int.from_bytes(hashlib.blake2b(text, digest_size=8).digest(), "big")
        return (n + 0.5) / 2**64

    @staticmethod
    def _pick(probabilities: dict[str, float], u: float) -> str | None:
        cumulative = 0.0
        for kind, p in probabilities.items():
            cumulative += p
            if u < cumulative:
                return kind
        return None

    def _latency_ns(self, cfg: ModelConfig, request_id: str, attempt: int) -> int:
        """Log-normal latency fitted to the configured p50 and p99."""
        mu = math.log(cfg.latency.p50_ms)
        sigma = (math.log(cfg.latency.p99_ms) - mu) / Z99
        z = NormalDist().inv_cdf(self._uniform(request_id, attempt, "latency"))
        ms = min(math.exp(mu + sigma * z), 10 * cfg.latency.p99_ms)  # clip the extreme tail
        return round(ms * 1_000_000)

    def _actual_tokens(self, cfg: ModelConfig, request_id: str, estimated: int) -> int:
        if cfg.actual_tokens_jitter == 0:
            return estimated
        u = self._uniform(request_id, "tokens")
        return max(1, round(estimated * (1 + cfg.actual_tokens_jitter * (2 * u - 1))))
