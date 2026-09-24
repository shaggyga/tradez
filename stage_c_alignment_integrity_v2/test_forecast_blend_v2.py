import copy
import pytest
from forecast_blend_v2 import build_chunk, mature_rows, summarize, block_sensitivity


def row(method, prediction, record_id="EUR_USD:1", available=2):
    return {"record_id": record_id, "group": "legacy26", "method": method, "procedure": "frozen", "selected_fit_id": "fit", "selected_fit_cutoff": 0,
            "forecast": {"schema_version": "forecast.v2", "forecast_id": method + record_id, "instrument": "EUR_USD", "decision_epoch": 1,
            "available_epoch": available, "model_id": method, "model_ready_epoch": 0, "training_view_fingerprint": "past", "target_id": "technical_endpoint_midpoint_elapsed_15m", "prediction": prediction, "coverage_reason": "eligible"}}


def coverage(method, reason="eligible"):
    return {"record_id": "EUR_USD:1", "instrument": "EUR_USD", "decision_epoch": 1, "target_id": "technical_endpoint_midpoint_elapsed_15m", "group": "legacy26", "method": method, "procedure": "frozen", "reason": reason, "selected_fit_id": "fit", "selected_fit_cutoff": 0}


def contract():
    return {"group": "legacy26", "horizon_minutes": 15, "procedure": "frozen", "assessment_asof_epoch": 10,
            "outcomes": {("EUR_USD:1", "technical_endpoint_midpoint_elapsed_15m"): {"value": 2.0, "available_epoch": 5, "label_end_epoch": 5}}}


def test_fixed_blend_and_metrics_keep_outcome_out_of_payload():
    rows, cov = build_chunk([row("ridge", 1), row("recovered_hgb", 3)], [coverage("ridge"), coverage("recovered_hgb")], contract()["outcomes"], contract())
    assert rows[0]["blend_prediction_bps"] == 2 and rows[0]["outcomes_revealed"] is False
    summary = summarize(rows, contract())
    assert summary["mature_rows"] == 1 and summary["overall"]["fixed_equal_half_blend"]["mae_bps"] == 0


def test_missing_partner_or_identity_drift_refused():
    with pytest.raises(ValueError, match="missing"):
        build_chunk([row("ridge", 1)], [coverage("ridge"), coverage("recovered_hgb")], contract()["outcomes"], contract())
    broken = row("recovered_hgb", 3);broken["forecast"]["available_epoch"] = 3
    with pytest.raises(ValueError, match="identity"):
        build_chunk([row("ridge", 1), broken], [coverage("ridge"), coverage("recovered_hgb")], contract()["outcomes"], contract())
    broken = row("recovered_hgb", 3);broken["forecast"]["model_ready_epoch"] = -1
    with pytest.raises(ValueError, match="identity"):
        build_chunk([row("ridge", 1), broken], [coverage("ridge"), coverage("recovered_hgb")], contract()["outcomes"], contract())


def test_future_outcome_cannot_be_scored_and_invalid_clock_refused():
    rows, _ = build_chunk([row("ridge", 1), row("recovered_hgb", 3)], [coverage("ridge"), coverage("recovered_hgb")], contract()["outcomes"], contract())
    future = copy.deepcopy(contract());future["outcomes"][("EUR_USD:1", "technical_endpoint_midpoint_elapsed_15m")]["available_epoch"] = 11
    assert mature_rows(rows, future["outcomes"], 10) == []
    invalid = copy.deepcopy(contract());invalid["outcomes"][("EUR_USD:1", "technical_endpoint_midpoint_elapsed_15m")]["available_epoch"] = 4
    with pytest.raises(ValueError, match="clock"):
        mature_rows(rows, invalid["outcomes"], 10)


def test_block_sensitivity_is_explicit_when_support_is_insufficient():
    rows, _ = build_chunk([row("ridge", 1), row("recovered_hgb", 3)], [coverage("ridge"), coverage("recovered_hgb")], contract()["outcomes"], contract())
    results = block_sensitivity(rows, contract())
    assert all(x["status"] == "insufficient_distinct_origins" for x in results)
