"""Verify the authoritative Stage C handoff artifacts without any model fit or I/O."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def load(name: str) -> dict:
    return json.loads((ROOT / name).read_text(encoding="utf-8"))


def main() -> int:
    preflight = load("ALL68_RUN_PREFLIGHT.json")
    stability = load("ALL68_MODEL_STABILITY_REPORT.json")
    strict = load("ALL68_CALENDAR_STRICT_PATH_AUDIT.json")
    guard = load("ALL68_MODEL_ADMISSION_GUARD.json")
    assert preflight["status"] == "passed_for_offline_research_only"
    assert preflight["checks"]["archive_member_count"] == 68
    assert preflight["checks"]["instrument_sets_match"] is True
    assert preflight["checks"]["execution_ready"] is False
    assert strict["aggregate_state_counts"].get("2:strict_calendar_minute_path_available", 0) == 0
    assert strict["aggregate_state_counts"].get("5:strict_calendar_minute_path_available", 0) == 0
    assert all(value["overall_status"] == "retired_negative_benchmark" for value in stability["results"].values())
    assert guard["admission_allowed"] is False
    assert all(item["admitted"] is False for item in guard["decisions"])
    print("stage_c_handoff_verified: offline only; models denied; multiday paths incomplete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
