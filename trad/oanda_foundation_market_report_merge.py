#!/usr/bin/env python3
"""Merge disjoint foundation-model market reports into one audit input."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_report(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"report must contain a JSON object: {path}")
    if not isinstance(payload.get("results"), list):
        raise ValueError(f"report has no results list: {path}")
    return payload


def merge_reports(paths: list[Path]) -> dict[str, Any]:
    if not paths:
        raise ValueError("at least one input report is required")

    fixtures: list[str] = []
    records: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    seen_models: set[str] = set()
    sources: list[dict[str, Any]] = []

    for raw_path in paths:
        path = raw_path.resolve()
        report = load_report(path)
        sources.append(
            {
                "path": str(path),
                "sha256": sha256_file(path),
                "generated_utc": report.get("generated_utc"),
            }
        )
        for fixture in report.get("fixtures") or []:
            fixture_text = str(fixture)
            if fixture_text not in fixtures:
                fixtures.append(fixture_text)
        records.extend(row for row in report.get("records") or [] if isinstance(row, dict))
        for row in report["results"]:
            if not isinstance(row, dict) or not row.get("model"):
                raise ValueError(f"invalid result row in {path}")
            model = str(row["model"])
            if model in seen_models:
                raise ValueError(f"duplicate model across reports: {model}")
            seen_models.add(model)
            results.append(row)

    results.sort(key=lambda row: str(row["model"]))
    ranking = sorted(
        (str(row["model"]) for row in results),
        key=lambda model: (
            -float(
                next(row for row in results if row["model"] == model)
                .get("summary", {})
                .get("avg_net_pips", float("-inf"))
            ),
            model,
        ),
    )
    scored = sum(row.get("status") == "bounded_market_scored" for row in results)
    production_eligible = sum(bool(row.get("production_eligible")) for row in results)
    account_wired = sum(bool(row.get("account_wired")) for row in results)
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "scope": "merged foundation-model bounded-market evidence",
        "status": "complete" if scored == len(results) else "partial",
        "execution_policy": "shadow_only_no_account_wiring",
        "source_reports": sources,
        "fixtures": fixtures,
        "records": records,
        "results": results,
        "ranking": ranking,
        "summary": {
            "requested": len(results),
            "scored": scored,
            "failed_or_blocked": len(results) - scored,
            "production_eligible": production_eligible,
            "account_wired": account_wired,
        },
        "account_wired": False,
    }


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = merge_reports(args.input)
    atomic_json(args.output.resolve(), report)
    print(json.dumps(report["summary"], indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
