import copy
import datetime as dt
import json

import pytest

import oanda_current_week_move_case_audit_v1 as audit


UTC = dt.timezone.utc


def _point(at: str, bid_open: float, ask_open: float, bid_close: float, ask_close: float):
    return {
        "timestamp": audit.parse_utc(at),
        "bid_open": bid_open,
        "ask_open": ask_open,
        "bid_close": bid_close,
        "ask_close": ask_close,
        "mid_open": (bid_open + ask_open) / 2,
        "mid_close": (bid_close + ask_close) / 2,
    }


def test_config_is_exactly_ten_current_week_cases_and_inert():
    config = json.loads(audit.CONFIG.read_text(encoding="utf-8"))
    audit.validate_config(config)
    assert len(config["cases"]) == 10
    assert config["research_only"] is True
    assert config["execution_eligible"] is False
    duplicate = copy.deepcopy(config)
    duplicate["cases"][1]["case_id"] = duplicate["cases"][0]["case_id"]
    with pytest.raises(audit.WeeklyCaseError, match="case_id_invalid"):
        audit.validate_config(duplicate)


def test_pair_path_uses_executable_sides():
    panel = {
        "EUR_USD": [
            _point("2026-08-17T12:00:00Z", 1.1000, 1.1002, 1.1000, 1.1002),
            _point("2026-08-17T12:05:00Z", 1.1010, 1.1012, 1.1010, 1.1012),
        ]
    }
    result = audit._pair_path(
        panel,
        "EUR_USD",
        dt.datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
        dt.datetime(2026, 8, 17, 12, 5, tzinfo=UTC),
    )
    assert result["observed_side"] == "long"
    assert result["midpoint_move_pips"] == pytest.approx(10.0)
    assert result["long_after_cost_pips"] == pytest.approx(8.0)


def test_factor_scores_deduplicate_into_currency_legs():
    panel = {
        "EUR_USD": [
            _point("2026-08-17T12:00:00Z", 1.0, 1.0, 1.0, 1.0),
            _point("2026-08-17T12:05:00Z", 1.01, 1.01, 1.01, 1.01),
        ],
        "GBP_USD": [
            _point("2026-08-17T12:00:00Z", 1.0, 1.0, 1.0, 1.0),
            _point("2026-08-17T12:05:00Z", 1.005, 1.005, 1.005, 1.005),
        ],
    }
    result = audit._factor_scores(
        panel,
        dt.datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
        dt.datetime(2026, 8, 17, 12, 5, tzinfo=UTC),
    )
    assert result["usable_pair_count"] == 2
    assert result["scores_bps"]["EUR"] > result["scores_bps"]["GBP"]
    assert result["scores_bps"]["USD"] < 0


def test_compiler_requires_complete_68_pair_panel(tmp_path):
    config = json.loads(audit.CONFIG.read_text(encoding="utf-8"))
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(audit.WeeklyCaseError, match="exact_68_pair_panel_required"):
        audit.compile_audit(config_path, tmp_path, audit.NEWS_DB)


def test_live_week_batch_compiles_fail_closed():
    payload = audit.compile_audit()
    assert payload["case_count"] == 10
    assert payload["pair_universe_count"] == 68
    assert payload["research_only"] is True
    assert payload["execution_eligible"] is False
    assert payload["can_place_orders"] is False
    assert payload["execution_decision"] == "no_trade"
    assert all(case["forecast_proof_eligible"] is False for case in payload["cases"])
    assert any(case["attribution_state"] == "no_causal_source_candidate" for case in payload["cases"])
    assert any(case["attribution_state"] == "source_hypothesis_opposed" for case in payload["cases"])
    # The registered universe is exactly 68, while event-time breadth may be
    # lower when regional instruments are closed or stale.  Preserve that
    # distinction instead of manufacturing complete simultaneous coverage.
    assert all(case["factor_response"]["usable_pair_count"] >= 55 for case in payload["cases"])
    assert all(case["factor_response"]["currency_count"] >= 19 for case in payload["cases"])
