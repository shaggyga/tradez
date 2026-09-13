#!/usr/bin/env python3
"""Validate the durable Forex issue register without changing its state."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
REGISTER = ROOT / "FOREX_ISSUE_REGISTER_CURRENT.json"
ALLOWED_STATUSES = {
    "open",
    "implemented_collecting",
    "blocked_external",
    "complete",
    "permanently_invalid",
    "superseded_collecting",
}
TERMINAL_VALIDATED_STATUSES = {"complete", "permanently_invalid"}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_utc(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp_without_timezone")
    return parsed.astimezone(dt.timezone.utc)


def validate_register(path: Path = REGISTER, *, root: Path = ROOT) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    errors: list[str] = []
    if payload.get("schema_version") != "forex_issue_register_v1":
        errors.append("invalid_schema_version")
    issues = payload.get("issues")
    if not isinstance(issues, list):
        return {"valid": False, "errors": ["issues_not_list"], "issue_count": 0}
    seen: set[str] = set()
    for issue in issues:
        if not isinstance(issue, Mapping):
            errors.append("issue_not_object")
            continue
        issue_id = str(issue.get("issue_id") or "")
        prefix = issue_id or "missing_issue_id"
        if not issue_id or issue_id in seen:
            errors.append(f"{prefix}:missing_or_duplicate_issue_id")
        seen.add(issue_id)
        status = str(issue.get("status") or "")
        if status not in ALLOWED_STATUSES:
            errors.append(f"{prefix}:invalid_status:{status}")
        for required in ("title", "priority", "owner_component", "acceptance_tests"):
            if not issue.get(required):
                errors.append(f"{prefix}:missing_{required}")
        history = issue.get("status_history") or []
        if not history or history[-1].get("status") != status:
            errors.append(f"{prefix}:status_history_mismatch")
        previous: dt.datetime | None = None
        for row in history:
            try:
                observed = parse_utc(str(row.get("at_utc") or ""))
            except (TypeError, ValueError):
                errors.append(f"{prefix}:invalid_history_timestamp")
                continue
            if previous is not None and observed < previous:
                errors.append(f"{prefix}:nonmonotonic_status_history")
            previous = observed
        if status == "blocked_external" and not issue.get("blocker"):
            errors.append(f"{prefix}:blocked_without_blocker")
        if status == "superseded_collecting" and not issue.get("superseding_cohort"):
            errors.append(f"{prefix}:superseded_without_cohort")
        artifacts = issue.get("validation_artifacts") or []
        if status in TERMINAL_VALIDATED_STATUSES and not artifacts:
            errors.append(f"{prefix}:terminal_without_validation_artifact")
        for artifact in artifacts:
            relative = str(artifact.get("path") or "")
            target = root / relative
            if not relative or not target.is_file():
                errors.append(f"{prefix}:missing_validation_artifact:{relative}")
                continue
            expected = str(artifact.get("sha256") or "")
            if status in TERMINAL_VALIDATED_STATUSES and not expected:
                errors.append(f"{prefix}:terminal_artifact_without_hash:{relative}")
            elif expected and sha256_file(target) != expected:
                errors.append(f"{prefix}:validation_hash_mismatch:{relative}")
        for relative in issue.get("evidence_paths") or []:
            if not (root / str(relative)).exists():
                errors.append(f"{prefix}:missing_evidence:{relative}")
    return {
        "valid": not errors,
        "errors": errors,
        "issue_count": len(issues),
        "status_counts": {
            status: sum(1 for row in issues if row.get("status") == status)
            for status in sorted(ALLOWED_STATUSES)
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--register", type=Path, default=REGISTER)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    result = validate_register(args.register, root=args.root)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
