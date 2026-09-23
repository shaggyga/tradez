#!/usr/bin/env python3
"""Permanent deterministic replay contract for the ten audited miss cases."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def classify_case(case: dict[str, Any]) -> str:
    path = case["price_path"]
    if not bool(path.get("exact_target_available")):
        return "invalid_maturity"
    endpoint = path.get("canonical_endpoint_net_pips")
    best = path.get("best_net_pips")
    if endpoint is None:
        return "invalid_maturity"
    if float(best or 0.0) > 0.0 and float(endpoint) <= 0.0:
        return "giveback"
    if float(endpoint) <= 0.0:
        return "bad_entry"
    return "after_cost_win"


def replay_contract(payload: dict[str, Any]) -> dict[str, Any]:
    cases = []
    failures = []
    for case in payload.get("cases") or []:
        result = classify_case(case)
        hashes = {
            "model_contract_sha256": stable_hash(case["model"]),
            "price_path_sha256": stable_hash(case["price_path"]),
            "source_evidence_sha256": stable_hash(case["source"]),
        }
        row = {
            "case_id": case["case_id"], "classification": result,
            "expected_class": case["expected_class"], "hashes": hashes,
            "passed": result == case["expected_class"],
        }
        if not row["passed"]:
            failures.append(row)
        cases.append(row)
    counts: dict[str, int] = {}
    for row in cases:
        counts[row["classification"]] = counts.get(row["classification"], 0) + 1
    return {
        "contract_id": payload.get("contract_id"),
        "fixture_sha256": stable_hash(payload),
        "status": "pass" if not failures else "fail",
        "case_count": len(cases), "classification_counts": counts,
        "cases": cases, "failures": failures,
        "historical_records_rewritten": False,
        "execution_authorized": False,
    }


def main() -> int:
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, default=root / "config" / "historical_miss_cases_v1.json")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    payload = json.loads(args.contract.read_text(encoding="utf-8"))
    result = replay_contract(payload)
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
