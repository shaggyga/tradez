from __future__ import annotations

import ast
from pathlib import Path

import oanda_spike_blurb_fomc_response_generalization_v1 as v1
import oanda_spike_blurb_fomc_response_generalization_v2 as module


def test_detection_coverage_uses_presence_only() -> None:
    paths = {
        "EUR_USD": {100: {"mid_o": 1.0}},
        "USD_JPY": {100: {"mid_o": 1.0}},
        "GBP_USD": {99: {"mid_o": 1.0}},
    }
    assert module.detection_leg_count(paths, 100) == 2


def test_fallbacks_are_predeclared_same_weekday_candidates() -> None:
    events = v1.validate_schedule()
    invalid = [{
        "source_event_id": v1.event_id("2024-12-18"),
        "control_slot": 1,
        "original_clock_id": "closed",
        "original_day_offset": 7,
        "detection_leg_count": 0,
        "availability_reason": "closed",
    }]
    rows = module.build_fallback_clocks(events, invalid)
    assert [row["actual_day_offset"] for row in rows] == [14, 21, 28]
    assert [row["clock_local"][:10] for row in rows] == ["2025-01-01", "2025-01-08", "2025-01-15"]
    assert all(row["clock_local"][11:16] == "14:00" for row in rows)


def test_candidate_identity_and_safety_are_unchanged() -> None:
    assert module.v1.LOCKED_ARM_ID == v1.LOCKED_ARM_ID
    assert module.v1.LOCKED_ARM == v1.LOCKED_ARM
    assert module.v1.HOLD_MINUTES == 15
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert "requests" not in imports
    assert "subprocess" not in imports
    assert "api-fxtrade.oanda.com" not in source
    assert '"POST"' not in source
    assert "authorization_id" not in source
