"""Pick and run the checks a scenario asks for (the `checks:` list in its YAML)."""

from verify import checks as c
from verify.data import RunData


def run_checks(data: RunData) -> list[c.Check]:
    spec = data.run.get("checks", {})
    warmup = data.run.get("warmup_s", 60)
    results: list[c.Check] = []
    results += c.window_limits(data)
    results.append(c.accounting(data))
    for item in spec.get("capacity", []):
        results.append(c.capacity(data, item["model"], warmup, item.get("min_fraction", 0.9)))
    for item in spec.get("adjusts", []):
        results.append(c.adjusts_to_limit_changes(data, item["model"], item.get("min_fraction", 0.8)))
    for item in spec.get("isolation", []):
        results.append(c.keeps_running(data, item["model"], warmup, item.get("min_fraction", 0.9)))
    if "batch" in spec:
        batch = spec["batch"]
        results += c.batch_callbacks(data, batch.get("max_ack_ms", 1000), batch.get("rejected_callbacks", 0))
    return results
