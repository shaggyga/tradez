from copy import deepcopy

import pytest

from contracts import fingerprint
from currency_projection_residual_layer_v2 import apply, fit_snapshot, score


CONTRACT = {"minimum_distinct_origins": 8, "minimum_distinct_utc_days": 3, "minimum_distinct_pairs": 20,
            "minimum_residual_weight": 0.0, "maximum_residual_weight": 1.0}


def fixture():
    rows, outcomes = [], {}
    for origin in range(86400, 86400 * 10, 86400):
        for pair in ("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "USD_CAD", "EUR_JPY", "EUR_GBP", "GBP_JPY", "AUD_JPY", "NZD_USD", "CAD_JPY", "CHF_JPY", "EUR_CHF", "GBP_CHF", "AUD_CAD", "NZD_CAD", "EUR_CAD", "GBP_CAD", "AUD_NZD"):
            record = pair + ":" + str(origin)
            for base in ("ridge", "recovered_hgb"):
                for variant, value in (("direct", 10.0), ("currency_projection", 2.0), ("half_residual", 6.0)):
                    row = {"record_id": record, "instrument": pair, "base_method": base, "variant": variant,
                           "method": base + "__" + variant, "origin_epoch": origin, "target_epoch": origin + 360 * 60,
                           "target_id": "technical_endpoint_midpoint_elapsed_360m", "horizon_minutes": 360,
                           "available_epoch": origin + 1, "prediction_bps": value}
                    row["forecast_id"] = fingerprint(row)
                    rows.append(row)
            outcomes[record, "technical_endpoint_midpoint_elapsed_360m"] = {"record_id": record,
                "target_id": "technical_endpoint_midpoint_elapsed_360m", "label_end_epoch": origin + 360 * 60,
                "available_epoch": origin + 360 * 60 + 1, "value": 10.0}
    return rows, outcomes


def test_prequential_fit_has_bounded_weight_and_uses_only_earlier_mature_origins():
    rows, outcomes = fixture()
    snapshots = fit_snapshot(rows, outcomes, 777600, CONTRACT)
    assert set(snapshots) == {("ridge", 360), ("recovered_hgb", 360)}
    assert all(snapshot["status"] == "fitted" and 0 <= snapshot["residual_weight"] <= 1 for snapshot in snapshots.values())
    learned = apply([row for row in rows if row["origin_epoch"] == 777600], snapshots)
    assert len(learned) == 40
    assert all(row["outcomes_revealed"] is False for row in learned)
    assert all(2 < row["prediction_bps"] < 10.01 for row in learned)


def test_future_outcomes_cannot_change_earlier_snapshot():
    rows, outcomes = fixture()
    before = fit_snapshot(rows, outcomes, 432000, CONTRACT)
    changed = deepcopy(outcomes)
    for (record, _), value in changed.items():
        if int(record.rsplit(":", 1)[1]) >= 777600:
            value["value"] = -999.0
    assert before == fit_snapshot(rows, changed, 432000, CONTRACT)


@pytest.mark.parametrize("mutation", ("forecast", "label", "clock", "missing_control"))
def test_identity_clock_and_control_failures_refuse(mutation):
    rows, outcomes = fixture()
    if mutation == "forecast":
        rows[0]["prediction_bps"] = 99.0
    elif mutation == "label":
        outcomes[next(iter(outcomes))]["label_end_epoch"] -= 1
    elif mutation == "clock":
        rows[0]["available_epoch"] = rows[0]["target_epoch"]
        rows[0]["forecast_id"] = fingerprint({k: v for k, v in rows[0].items() if k != "forecast_id"})
    else:
        rows.pop()
    with pytest.raises(ValueError):
        fit_snapshot(rows, outcomes, 432000, CONTRACT)


def test_score_keeps_matched_forecast_support():
    rows, outcomes = fixture()
    snapshots = fit_snapshot(rows, outcomes, 777600, CONTRACT)
    learned = apply([row for row in rows if row["origin_epoch"] == 777600], snapshots)
    result = score(learned, outcomes, 10**9)
    assert len(result) == 2
    assert all(item["mature_rows"] == 20 and item["distinct_pairs"] == 20 for item in result)


@pytest.mark.parametrize("mutation", ("backdated_forecast", "embedded_outcome_key", "cross_scope_snapshot"))
def test_semantic_identity_failures_refuse(mutation):
    rows, outcomes = fixture()
    snapshots = fit_snapshot(rows, outcomes, 777600, CONTRACT)
    if mutation == "backdated_forecast":
        row = rows[0]
        row["available_epoch"] = row["origin_epoch"] - 1
        row["forecast_id"] = fingerprint({k: v for k, v in row.items() if k != "forecast_id"})
        with pytest.raises(ValueError):
            fit_snapshot(rows, outcomes, 777600, CONTRACT)
    elif mutation == "embedded_outcome_key":
        value = outcomes[next(iter(outcomes))]
        value["target_id"] = "wrong-target"
        with pytest.raises(ValueError):
            fit_snapshot(rows, outcomes, 777600, CONTRACT)
    else:
        snapshots["ridge", 360]["base_method"] = "wrong-base"
        with pytest.raises(ValueError):
            apply([row for row in rows if row["origin_epoch"] == 777600], snapshots)


def test_same_day_rows_cannot_replace_inherited_support():
    rows, outcomes = fixture()
    cutoff = 777600
    same_day = [row for row in rows if row["origin_epoch"] >= cutoff - 3 * 86400]
    snapshots = fit_snapshot(same_day, outcomes, cutoff, CONTRACT)
    assert all(item["status"] == "insufficient_distinct_support" for item in snapshots.values())


def test_application_refuses_cross_target_controls():
    rows, outcomes = fixture()
    snapshots = fit_snapshot(rows, outcomes, 777600, CONTRACT)
    current = [row for row in rows if row["origin_epoch"] == 777600]
    changed = next(row for row in current if row["variant"] == "half_residual")
    changed["target_id"] = "different-target"
    changed["forecast_id"] = fingerprint({key: value for key, value in changed.items() if key != "forecast_id"})
    with pytest.raises(ValueError, match="application_control_mismatch"):
        apply(current, snapshots)
