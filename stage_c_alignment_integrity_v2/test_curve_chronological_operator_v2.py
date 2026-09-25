import json

from contracts import fingerprint
from curve_chronological_operator_v2 import _anchor_controls, _score, _variant_curve_rows


def row(pair, origin, horizon, variant, value):
    item = {
        "record_id": f"{pair}:{origin}",
        "instrument": pair,
        "decision_epoch": origin,
        "origin_epoch": origin,
        "target_epoch": origin + horizon * 60,
        "target_id": "target_360",
        "horizon_minutes": horizon,
        "base_method": "ridge",
        "variant": variant,
        "available_epoch": origin + 2,
        "prediction_bps": value,
    }
    item["forecast_id"] = fingerprint(item)
    return item


def test_variant_curve_requires_complete_horizon_panel():
    contract = {"feature_definition": {"horizons_minutes": [360, 720], "anchor_horizon_minutes": 360}}
    rows = [row("EUR_USD", 1000, 360, "raw_matched_expanding", 1.0)]
    assert _variant_curve_rows(rows, contract, "raw_matched_expanding") == []
    rows.append(row("EUR_USD", 1000, 720, "raw_matched_expanding", 2.0))
    curves = _variant_curve_rows(rows, contract, "raw_matched_expanding")
    assert len(curves) == 1
    assert curves[0]["values"] == {"360": 1.0, "720": 2.0}


def test_score_compares_curve_to_all_anchor_controls():
    controls = []
    for variant, value in (
        ("raw_matched_expanding", 1.0),
        ("signed_only_expanding", 2.0),
        ("magnitude_interaction_expanding", 4.0),
    ):
        controls.append(row("EUR_USD", 1000, 360, variant, value))
    learned = [{**controls[0], "variant": "curve_shape", "prediction_bps": 3.0, "forecast_id": "learned"}]
    outcomes = {("EUR_USD:1000", "target_360"): {
        "record_id": "EUR_USD:1000", "target_id": "target_360",
        "label_end_epoch": 1000 + 360 * 60, "available_epoch": 1000 + 360 * 60 + 1,
        "value": 5.0,
    }}
    result = _score(learned, _anchor_controls(controls, 360), outcomes, 10**9)
    assert {item["control_variant"] for item in result if item["stratum"] == "overall"} == {
        "raw_matched_expanding", "signed_only_expanding", "magnitude_interaction_expanding"}
    assert next(item for item in result if item["control_variant"] == "magnitude_interaction_expanding")["mae_delta_bps"] == 1.0


def test_score_ignores_unmatured_labels():
    controls = [row("EUR_USD", 1000, 360, "raw_matched_expanding", 1.0)]
    learned = [{**controls[0], "variant": "curve_shape", "prediction_bps": 3.0, "forecast_id": "learned"}]
    outcomes = {("EUR_USD:1000", "target_360"): {
        "record_id": "EUR_USD:1000", "target_id": "target_360",
        "label_end_epoch": 1000 + 360 * 60, "available_epoch": 10**9 + 1,
        "value": 5.0,
    }}
    assert _score(learned, _anchor_controls(controls, 360), outcomes, 10**9) == []
