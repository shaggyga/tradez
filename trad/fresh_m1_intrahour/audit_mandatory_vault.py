#!/usr/bin/env python3
"""Audit mandatory-model coverage in an immutable forex vault build."""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.mandatory_model_registry import MANDATORY_MODEL_NAMES


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def audit(build: Path, source_report: Path, output_dir: Path) -> Path:
    build = build.resolve()
    source_report = source_report.resolve()
    build_validation = json.loads((build / "BUILD_VALIDATION.json").read_text(encoding="utf-8"))
    build_summary = json.loads((build / "build_summary.json").read_text(encoding="utf-8"))
    source_summary = json.loads(
        (source_report / "MANDATORY_MODEL_FINAL_SUMMARY.json").read_text(encoding="utf-8")
    )
    source_marker = source_report.name
    connection = sqlite3.connect(build / "catalog" / "catalog.sqlite")
    rows = connection.execute(
        "SELECT record_json FROM runs WHERE record_type = 'evaluation' AND record_json LIKE ?",
        (f"%{source_marker}/MANDATORY_MODEL_RESULTS.json%",),
    ).fetchall()
    connection.close()
    required = set(MANDATORY_MODEL_NAMES)
    records = []
    for (raw,) in rows:
        record = json.loads(raw)
        if record.get("specification", {}).get("model_name") in required:
            records.append(record)
    names = [record["specification"]["model_name"] for record in records]
    grades = Counter(record.get("reproducibility", {}).get("grade") for record in records)
    statuses = Counter(record.get("validation_status") for record in records)
    checks = {
        "build_validation_passed": bool(build_validation.get("passed")),
        "exact_required_count": len(records) == len(MANDATORY_MODEL_NAMES),
        "exact_unique_names": set(names) == required and len(names) == len(set(names)),
        "source_suite_passed": source_summary.get("verdict") == "PASS",
        "source_test_complete": source_summary.get("test_complete") == len(MANDATORY_MODEL_NAMES),
        "all_reconstructable": all(
            str(record.get("reproducibility", {}).get("grade", "")).startswith(("A_", "B_"))
            for record in records
        ),
        "all_have_commands": all(
            bool(record.get("reproducibility", {}).get("rerun_command")) for record in records
        ),
        "all_have_date_coverage": all(bool(record.get("date_coverage")) for record in records),
        "no_unresolved_dependencies": all(
            not record.get("reproducibility", {}).get("unresolved_declared_paths")
            for record in records
        ),
        "execution_disabled": not any(source_summary.get("safety", {}).values())
        and not any(build_summary.get("safety", {}).values()),
        "no_canonical_claim": source_summary.get("canonical_models") == 0,
    }
    payload = {
        "audit_version": "mandatory_vault_coverage_v1",
        "audited_at_utc": datetime.now(timezone.utc).isoformat(),
        "build_id": build.name,
        "build_path": str(build),
        "source_report_path": str(source_report),
        "coverage_verdict": "PASS" if all(checks.values()) else "FAIL",
        "profitability_verdict": "FAIL" if source_summary.get("research_leads") == 0 else "RESEARCH_LEADS_PRESENT",
        "checks": checks,
        "required_models": len(MANDATORY_MODEL_NAMES),
        "catalogued_models": len(records),
        "missing_models": sorted(required - set(names)),
        "extra_models": sorted(set(names) - required),
        "reproducibility_grades": dict(sorted(grades.items())),
        "validation_statuses": dict(sorted(statuses.items())),
        "source_summary": {
            "verdict": source_summary.get("verdict"),
            "test_complete": source_summary.get("test_complete"),
            "research_leads": source_summary.get("research_leads"),
            "validated_failures": source_summary.get("validated_failures"),
            "canonical_models": source_summary.get("canonical_models"),
            "fit_diagnostics": source_summary.get("fit_diagnostics"),
            "safety": source_summary.get("safety"),
        },
        "build_safety": build_summary.get("safety"),
        "model_names": sorted(names),
    }
    output = output_dir / f"MANDATORY_MODEL_COVERAGE_{build.name}.json"
    write_json(output, payload)
    markdown = [
        f"# Mandatory Model Coverage - {payload['coverage_verdict']}",
        "",
        f"- Build: `{build.name}`",
        f"- Mandatory names: `{len(MANDATORY_MODEL_NAMES)}`",
        f"- Exact records: `{len(records)}`",
        f"- Reproducibility: `{dict(grades)}`",
        f"- Validation statuses: `{dict(statuses)}`",
        f"- Profitability verdict: `{payload['profitability_verdict']}`",
        f"- Canonical models: `{source_summary.get('canonical_models')}`",
        "- Execution: `disabled`",
    ]
    output.with_suffix(".md").write_text("\n".join(markdown) + "\n", encoding="utf-8")
    if payload["coverage_verdict"] != "PASS":
        raise RuntimeError(f"mandatory vault coverage failed: {payload}")
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--source-report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(audit(args.build, args.source_report, args.output_dir))
