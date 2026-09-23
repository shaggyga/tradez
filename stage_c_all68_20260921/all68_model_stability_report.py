"""Consolidate immutable all-68 endpoint-only model reports without refitting."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RUNS = {
    "ridge": [
        ("july", ROOT / "ALL68_CALENDAR_BASELINE_REPORT.json"),
        ("august", ROOT / "ALL68_CALENDAR_BASELINE_calendar_aug_2024_v1_REPORT.json"),
        ("september", ROOT / "ALL68_CALENDAR_BASELINE_calendar_sep_2024_v1_REPORT.json"),
    ],
    "hist_gradient_boosting": [
        ("july", ROOT / "ALL68_CALENDAR_BASELINE_hgb_jul_2024_v1_REPORT.json"),
        ("august", ROOT / "ALL68_CALENDAR_BASELINE_hgb_aug_2024_v1_REPORT.json"),
    ],
}
SCORE_KEYS = {"ridge": "regularized_pooled", "hist_gradient_boosting": "hist_gradient_boosting"}
OUTPUT = ROOT / "ALL68_MODEL_STABILITY_REPORT.json"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    results, source_hashes = {}, {}
    for model, runs in RUNS.items():
        per_block, target_summary = [], {str(days): [] for days in (1, 2, 5)}
        for block, path in runs:
            report = json.loads(path.read_text(encoding="utf-8"))
            source_hashes[str(path)] = digest(path)
            for days in (1, 2, 5):
                row = report["results"][str(days)]
                model_score = row[SCORE_KEYS[model]]
                baseline = row["no_change"]
                delta = model_score["mae_bps"] - baseline["mae_bps"]
                entry = {"block": block, "trading_days": days, "test_rows": row["test_rows"], "mae_delta_vs_no_change_bps": delta, "directional_accuracy": model_score["directional_accuracy_on_nonzero_forecasts"]}
                per_block.append(entry)
                target_summary[str(days)].append(entry)
        target_decisions = {}
        for days, entries in target_summary.items():
            improvements = sum(item["mae_delta_vs_no_change_bps"] < 0 for item in entries)
            target_decisions[days] = {"blocks": len(entries), "mae_improvements": improvements, "mean_mae_delta_vs_no_change_bps": sum(item["mae_delta_vs_no_change_bps"] for item in entries) / len(entries), "status": "not_admitted"}
        results[model] = {"blocks": per_block, "target_summary": target_decisions, "overall_status": "retired_negative_benchmark"}
    payload = {"schema": "all68_model_stability_report_v1", "scope": "endpoint-only forecast skill; no execution or policy claim", "results": results, "source_report_sha256": source_hashes}
    temporary = OUTPUT.with_suffix(OUTPUT.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, OUTPUT)
    print("model stability report complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
