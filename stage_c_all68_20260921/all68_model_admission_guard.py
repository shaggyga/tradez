"""Fail closed when endpoint-only research reports are proposed for admission."""
from __future__ import annotations

import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
STABILITY = ROOT / "ALL68_MODEL_STABILITY_REPORT.json"
STRICT_PATH = ROOT / "ALL68_CALENDAR_STRICT_PATH_AUDIT.json"
OUTPUT = ROOT / "ALL68_MODEL_ADMISSION_GUARD.json"


def main() -> int:
    stability = json.loads(STABILITY.read_text(encoding="utf-8"))
    path = json.loads(STRICT_PATH.read_text(encoding="utf-8"))
    strict = path["aggregate_state_counts"]
    complete_multiday = strict.get("2:strict_calendar_minute_path_available", 0) + strict.get("5:strict_calendar_minute_path_available", 0)
    decisions = []
    for model, result in stability["results"].items():
        decisions.append({"model": model, "admitted": False, "reasons": ["status=" + result["overall_status"], "target_contract=endpoint_only_not_execution", "complete_multiday_strict_paths=" + str(complete_multiday)]})
    payload = {"schema": "all68_model_admission_guard_v1", "status": "denied", "admission_allowed": False, "decisions": decisions, "global_reasons": ["all evaluated models are retired negative benchmarks", "exact calendar-minute audit found zero complete 2/5-day paths; session closures remain unclassified", "no historical execution accounting campaign is available"], "inputs": {"stability_report": str(STABILITY), "strict_path_audit": str(STRICT_PATH)}}
    temporary = OUTPUT.with_suffix(OUTPUT.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, OUTPUT)
    print("model admission denied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
