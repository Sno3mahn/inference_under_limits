"""Re-verify a finished run:  uv run python -m verify ../runs/<run-dir>"""

import argparse
import sys

from verify.data import load_run
from verify.report import write_report
from verify.run_checks import run_checks


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify a run directory and write its report")
    parser.add_argument("run_dir")
    args = parser.parse_args()
    data = load_run(args.run_dir)
    checks = run_checks(data)
    report = write_report(data, checks)
    for check in checks:
        print(f"{'PASS' if check.passed else 'FAIL'}  {check.name}: {check.detail}")
    print(f"\nreport: {report}")
    sys.exit(0 if all(c.passed for c in checks) else 1)


if __name__ == "__main__":
    main()
