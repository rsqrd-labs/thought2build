"""Scheduled, credential-free policy maintenance gate; never changes review dates."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path


def check(policy: dict, today: date, warning_days: int = 7) -> str | None:
    try:
        reviewed = date.fromisoformat(policy["last_reviewed"])
        remaining = int(policy["max_age_days"]) - (today - reviewed).days
    except (KeyError, ValueError, TypeError):
        return "Technology policy has invalid review metadata."
    if reviewed > today:
        return "Technology policy review date is in the future."
    if remaining <= warning_days:
        return (
            f"Technology policy needs review: {remaining} days until expiry. "
            "Review provider evidence and publish updated policy; "
            "do not only change the date."
        )
    return None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warning-days", type=int, default=7)
    args = parser.parse_args(argv)
    path = (
        Path(__file__).resolve().parents[1]
        / "services/pipeline/tech_safety_policy.json"
    )
    problem = check(
        json.loads(path.read_text()), datetime.now(UTC).date(), args.warning_days
    )
    print(problem or "Technology policy review window is healthy.")
    return bool(problem)


if __name__ == "__main__":
    raise SystemExit(main())
