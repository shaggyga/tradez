import copy
import pytest
from forecast_blend_v2 import build_chunk, mature_rows, summarize, block_sensitivity
from contracts import fingerprint


def row(method, prediction, record_id="EUR_USD:1", available=2):
    return {"record_id": record_id, "group": "legacy26", "method": method, "procedure": "frozen", "selected_fit_id": "fit", "selected_fit_cutoff": 0,
            "forecast": {"schema_version": "forecast.v2", "forecast_id": method + record_id, "instrument": "EUR_USD", "decision_epoch": 1,
            "available_epoch": available, "model_id": fingerprint({"fit_id": "fit", "method": method}), "model_ready_epoch": 0, "training_view_fingerprint": "past", "target_id": "technical_endpoint_midpoint_elapsed_15m", "prediction": prediction, "coverage_reason": "eligible"}}


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
    broken = row("recovered_hgb", 3);broken["forecast"]["model_ready_epoch"] = 1
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


def test_future_forecast_is_not_scored_even_when_label_is_available():
    rows, _ = build_chunk([row("ridge", 1, available=11), row("recovered_hgb", 3, available=11)],
                          [coverage("ridge"), coverage("recovered_hgb")], {}, contract())
    assert mature_rows(rows, contract()["outcomes"], 10) == []


@pytest.mark.parametrize("reason", [None, "missing_features"])
def test_orphan_and_ineligible_forecasts_are_refused(reason):
    cov = [] if reason is None else [coverage(m, reason) for m in ("ridge", "recovered_hgb")]
    with pytest.raises(ValueError, match="coverage"):
        build_chunk([row("ridge", 1), row("recovered_hgb", 3)], cov, {}, contract())


def test_eligible_coverage_keeps_selected_fit_identity():
    _, cov = build_chunk([row("ridge", 1), row("recovered_hgb", 3)],
                         [coverage("ridge"), coverage("recovered_hgb")], {}, contract())
    assert cov[0]["selected_fit_id"] == "fit"


def test_outcome_perturbation_cannot_change_blend_or_coverage():
    args = ([row("ridge", 1), row("recovered_hgb", 3)], [coverage("ridge"), coverage("recovered_hgb")])
    original = build_chunk(*args, contract()["outcomes"], contract())
    mutated = copy.deepcopy(contract())
    mutated["outcomes"] = {"adversarial": {"value": 1e200}}
    assert original == build_chunk(*args, mutated["outcomes"], mutated)


def panel(n=20):
    rows, outcomes = [], {}
    for i in range(n):
        epoch = 1 + i * 21600
        rid = "EUR_USD:" + str(epoch)
        rows.append({"record_id": rid, "instrument": "EUR_USD", "target_id": "target",
                     "decision_epoch": epoch, "available_epoch": epoch + 2,
                     "ridge_prediction_bps": float(i), "recovered_hgb_prediction_bps": -float(i),
                     "blend_prediction_bps": 0.0, "zero_prediction_bps": 0.0})
        outcomes[rid, "target"] = {"value": 2.0, "available_epoch": epoch + 60, "label_end_epoch": epoch + 60}
    return rows, {"outcomes": outcomes, "assessment_asof_epoch": 10**9}


def test_full_length_circular_block_is_not_presented_as_uncertainty():
    rows, c = panel()
    output = block_sensitivity(rows, c)
    assert output[-1]["status"] == "single_circular_block_no_resampling_variation"
    assert output[-1]["interval"] is None
    assert output[0]["seed"] == 20260926


def test_irregular_support_has_no_interval():
    rows, c = panel()
    rows.pop(5)
    assert all(x["interval"] is None for x in block_sensitivity(rows, c))


def test_block_aggregation_matches_direct_row_sampling_oracle():
    import math
    import random
    rows, c = panel()
    actual = block_sensitivity(rows, c)[0]
    # Unequal row counts exercise row weights rather than origin averaging.
    rows.append({**rows[0], "instrument": "GBP_USD"})
    actual = block_sensitivity(rows, c)[0]
    rng = random.Random(20260926)
    origins = sorted({r["decision_epoch"] for r in rows})
    expected = []
    for _ in range(2000):
        draw = []
        while len(draw) < len(origins):
            start = rng.randrange(len(origins))
            draw.extend(origins[(start + j) % len(origins)] for j in range(4))
        selected = [r for o in draw[:len(origins)] for r in rows if r["decision_epoch"] == o]
        expected.append(math.fsum(abs(r["blend_prediction_bps"] - 2) - abs(r["ridge_prediction_bps"] - 2)
                                  for r in selected) / len(selected))
    expected.sort()
    assert actual["blend_vs_ridge_mae_delta_bps_interval_10_90"] == pytest.approx([expected[199], expected[1799]])


@pytest.mark.parametrize("field,value", [("model_id", "wrong"), ("training_view_fingerprint", "in_sample"),
                                         ("coverage_reason", "unavailable")])
def test_model_and_training_identity_mismatch_refused(field, value):
    broken = row("recovered_hgb", 3)
    broken["forecast"][field] = value
    with pytest.raises(ValueError):
        build_chunk([row("ridge", 1), broken], [coverage("ridge"), coverage("recovered_hgb")], {}, contract())


def test_resource_caps_are_enforced(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import forecast_blend_runner_v2 as runner
    monkeypatch.setattr(runner.time, "monotonic", lambda: 2)
    fake = SimpleNamespace(memory_info=lambda: SimpleNamespace(rss=100), children=lambda **kw: [], is_running=lambda: True)
    monkeypatch.setattr(runner.psutil, "Process", lambda: fake)
    config = {"main_wall_seconds": 300, "max_rss_bytes": 1024**3, "max_scratch_bytes": 1024**3}
    assert runner.check_resources(tmp_path, 1, config, "test")["aggregate_rss_bytes"] == 100
    for key, value, message in [("main_wall_seconds", 0, "wall"), ("max_rss_bytes", 99, "rss"),
                                ("max_scratch_bytes", 1, "scratch")]:
        with pytest.raises(ValueError, match=message):
            runner.check_resources(tmp_path, 1, {**config, key: value}, "test")


def test_failed_acceptance_cannot_publish_completion(tmp_path, monkeypatch):
    import forecast_blend_runner_v2 as runner
    # Empty scope reaches the real final acceptance guard quickly; it must fail
    # before publication even though individual output writes succeeded.
    monkeypatch.setattr(runner, "HORIZONS", ())
    monkeypatch.setattr(runner, "CampaignReader", lambda *a: None)
    monkeypatch.setattr(runner, "outcome_map", lambda *a: {})
    recipe = {"run_id": "refuse-invalid", "sources": {}, "dependencies": {},
              "configuration": {"universe": [], "main_wall_seconds": 300,
                                "max_rss_bytes": 1024**3, "max_scratch_bytes": 1024**3}}
    with pytest.raises((ValueError, KeyError)):
        runner.run({}, recipe, tmp_path)
    assert not (tmp_path / "refuse-invalid" / "COMPLETION_MANIFEST.json").exists()
