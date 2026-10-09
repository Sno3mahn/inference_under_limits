"""Simulator configuration, loaded from YAML (see config/models.yaml)."""

from dataclasses import dataclass, field, fields
from pathlib import Path

import yaml

# Failure kinds the simulator knows how to produce.
TRANSIENT_KINDS = {"429", "500", "502", "503", "timeout"}
PERMANENT_KINDS = {"400", "413"}


@dataclass
class LatencyConfig:
    p50_ms: float = 150.0
    p99_ms: float = 600.0

    def validate(self) -> None:
        if not 0 < self.p50_ms <= self.p99_ms:
            raise ValueError("latency needs 0 < p50_ms <= p99_ms")


@dataclass
class ModelConfig:
    rpm: int
    tpm: int
    latency: LatencyConfig = field(default_factory=LatencyConfig)
    # kind -> probability per attempt, e.g. {"503": 0.02, "timeout": 0.005}
    transient: dict[str, float] = field(default_factory=dict)
    # kind -> probability per request id (sticky: retrying never helps)
    permanent: dict[str, float] = field(default_factory=dict)
    # actual tokens = estimate x (1 +/- jitter); 0 means actual == estimate
    actual_tokens_jitter: float = 0.0

    def validate(self) -> None:
        if self.rpm < 0 or self.tpm < 0:
            raise ValueError("rpm and tpm must be >= 0")
        self.latency.validate()
        for kinds, allowed in ((self.transient, TRANSIENT_KINDS), (self.permanent, PERMANENT_KINDS)):
            unknown = set(kinds) - allowed
            if unknown:
                raise ValueError(f"unknown failure kinds {sorted(unknown)}; allowed {sorted(allowed)}")
            if any(p < 0 for p in kinds.values()) or sum(kinds.values()) > 1:
                raise ValueError("failure probabilities must be >= 0 and sum to <= 1")
        if not 0 <= self.actual_tokens_jitter < 1:
            raise ValueError("actual_tokens_jitter must be in [0, 1)")

    def updated(self, changes: dict) -> "ModelConfig":
        """A copy with some fields replaced (used by the admin API). Validated."""
        known = {f.name for f in fields(self)}
        unknown = set(changes) - known
        if unknown:
            raise ValueError(f"unknown fields {sorted(unknown)}")
        merged = {f.name: getattr(self, f.name) for f in fields(self)} | changes
        if isinstance(merged["latency"], dict):
            merged["latency"] = LatencyConfig(**merged["latency"])
        cfg = ModelConfig(**merged)
        cfg.validate()
        return cfg


@dataclass
class SimConfig:
    models: dict[str, ModelConfig]
    seed: int = 42
    # How long a "timeout" outcome hangs before answering. It should be longer than the
    # gateway's request timeout so the gateway really does give up.
    hang_s: float = 30.0


def model_from_dict(raw: dict) -> ModelConfig:
    raw = dict(raw)
    raw["latency"] = LatencyConfig(**raw.get("latency", {}))
    raw["transient"] = {str(k): v for k, v in raw.get("transient", {}).items()}
    raw["permanent"] = {str(k): v for k, v in raw.get("permanent", {}).items()}
    cfg = ModelConfig(**raw)
    cfg.validate()
    return cfg


def load_config(path: str | Path) -> SimConfig:
    raw = yaml.safe_load(Path(path).read_text())
    models = {name: model_from_dict(m) for name, m in raw["models"].items()}
    return SimConfig(models=models, seed=raw.get("seed", 42), hang_s=raw.get("hang_s", 30.0))
