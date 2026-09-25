from contracts import fingerprint
from curve_shape_layer_v2 import apply, features, fit_snapshot, join_curve


def row(pair, origin, horizon, value):
    r = {"instrument": pair, "record_id": pair + ":" + str(origin), "target_id": "target_" + str(horizon),
         "decision_epoch": origin, "horizon_minutes": horizon, "available_epoch": origin + 1,
         "base_method": "ridge", "prediction_bps": value}
    r["forecast_id"] = fingerprint(r)
    return r


def test_causal_complete_curve_fit_and_apply():
    raw, outcomes = [], {}
    for origin in range(86400, 86400 * 10, 86400):
        for pair in ("A", "B", "C"):
            raw += [row(pair, origin, 60, 1), row(pair, origin, 120, 3)]
            outcomes[pair + ":" + str(origin), "target_60"] = {"available_epoch": origin + 1000, "value": 2.0}
    curves = join_curve(raw, [60, 120])
    contract = {"minimum_distinct_origins": 8, "minimum_distinct_utc_days": 3, "minimum_distinct_pairs": 3, "ridge_lambda": 1.0}
    snapshot = fit_snapshot(curves, outcomes, 777600, contract)
    assert snapshot["status"] == "fitted"
    assert apply(curves[-1], snapshot)["outcomes_revealed"] is False


def test_incomplete_curve_is_not_imputed():
    assert join_curve([row("A", 1, 60, 1)], [60, 120]) == []


def test_explicit_anchor_controls_target_and_features():
    curve = join_curve([row("A", 1, 60, 2), row("A", 1, 120, 8)], [60, 120], 120)[0]
    assert curve["target_id"] == "target_120"
    assert curve["anchor_horizon_minutes"] == 120
    assert features(curve) == [8, -6]
