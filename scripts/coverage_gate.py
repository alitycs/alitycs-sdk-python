#!/usr/bin/env python3
"""Fail non-zero when coverage.json misses the contract's gates.

Reads ``coverage.json`` as written by ``pytest --cov-report=json`` and enforces:
total (line) coverage >= 90% and branch coverage >= 85%. Branch stands in for the
contract's "functions >= 85%" — coverage.py has no function metric (see
.agents/plans/sdk-python.md). Exits 1 with a message on either gate or on a
missing/unparseable report, so CI and ``conformance coverage`` both fail loudly.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

TOTAL_GATE = 90.0
BRANCH_GATE = 85.0


def main(argv: list[str]) -> int:
    report_path = Path(argv[1]) if len(argv) > 1 else Path("coverage.json")
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"coverage gate: {report_path} not found — run pytest --cov-report=json first", file=sys.stderr)
        return 1
    except json.JSONDecodeError as exc:
        print(f"coverage gate: {report_path} is not valid JSON: {exc}", file=sys.stderr)
        return 1

    totals = report.get("totals", {})
    total_percent = totals.get("percent_covered")
    branch_percent = totals.get("percent_covered_branches")
    if branch_percent is None:
        # coverage.py does not emit a branch percentage; derive it from its counts.
        num_branches = totals.get("num_branches")
        covered_branches = totals.get("covered_branches")
        if not isinstance(num_branches, (int, float)) or not isinstance(covered_branches, (int, float)):
            print(
                "coverage gate: coverage.json lacks branch data — run pytest with --cov-branch",
                file=sys.stderr,
            )
            return 1
        if num_branches == 0:
            print("coverage gate: no branches recorded — run pytest with --cov-branch", file=sys.stderr)
            return 1
        branch_percent = covered_branches * 100.0 / num_branches
    if total_percent is None:
        print("coverage gate: coverage.json is missing totals.percent_covered", file=sys.stderr)
        return 1

    failures = []
    if float(total_percent) < TOTAL_GATE:
        failures.append(f"total coverage {total_percent:.2f}% < {TOTAL_GATE}%")
    if float(branch_percent) < BRANCH_GATE:
        failures.append(f"branch coverage {branch_percent:.2f}% < {BRANCH_GATE}%")

    if failures:
        print("coverage gate FAILED: " + "; ".join(failures), file=sys.stderr)
        return 1

    print(f"coverage gate ok: total {total_percent:.2f}% >= {TOTAL_GATE}%, branch {branch_percent:.2f}% >= {BRANCH_GATE}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
