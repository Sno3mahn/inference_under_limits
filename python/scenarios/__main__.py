"""uv run python -m scenarios ../config/scenarios/scenario1.yaml [--duration 120] [--rate 900]"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from scenarios.runner import REPO, ScenarioRun, load_scenario


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a validation scenario end to end")
    parser.add_argument("scenario", help="path to a scenario YAML")
    parser.add_argument("--duration", type=float, help="override load duration (s)")
    parser.add_argument("--rate", type=float, help="override offered rate (req/s)")
    parser.add_argument("--runs-dir", default=str(REPO / "runs"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    scenario = load_scenario(args.scenario, {"duration_s": args.duration, "rate": args.rate})
    ok = asyncio.run(ScenarioRun(scenario, Path(args.runs_dir)).run())
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
