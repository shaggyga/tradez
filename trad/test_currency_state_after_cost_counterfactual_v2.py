from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

try:
    from forex_system.contracts.currency_state import stable_hash
    from forex_system.contracts.signed_currency_exposure import canonical_factor_ids
    from forex_system.research.currency_state_after_cost_counterfactual_v2 import AfterCostV2Error, build_after_cost_counterfactual_v2
    from forex_system.research.currency_state_response_timing_arms import build_response_timing_arms
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import stable_hash
    from src.forex_system.contracts.signed_currency_exposure import canonical_factor_ids
    from src.forex_system.research.currency_state_after_cost_counterfactual_v2 import AfterCostV2Error, build_after_cost_counterfactual_v2
    from src.forex_system.research.currency_state_response_timing_arms import build_response_timing_arms

from test_currency_state_response_timing_arms import SEMANTIC_LINEAGE, arm_contract, envelope, official_context_snapshot


ROOT = Path(__file__).resolve().parent
H = "a" * 64


def contract(): return json.loads((ROOT / "config" / "currency_state_after_cost_counterfactual_v2.json").read_text())


def manifest():
    c = contract()
    return {"manifest_id": c["required_frozen_manifest_id"], "contract_id": c["contract_id"], "cohort_id": c["counterfactual_cohort_id"], "artifacts": {label: {"relative_path": f"fixture/{label}", "sha256": H} for label in c["required_manifest_artifacts"]}}


def official(instrument="EUR_USD", forecast="forecast-1"):
    base, quote = instrument.split("_")
    return {"thesis_id": f"thesis-{instrument}", "instrument": instrument, "horizon_sec": 3600, "known_at_utc": "2026-01-01T00:45:00+00:00", "direction": "buy", "direction_basis": "versioned_semantic_thesis", **SEMANTIC_LINEAGE, "expected_absolute_move_bps": 8.0, "source_ids": [f"official:{base}", f"official:{quote}"], "source_fact_ids": [f"fixture:{base}", f"fixture:{quote}"], "independent_factor_ids": [canonical_factor_ids(instrument, "buy")[0]], "forecast_id": forecast}


def response(instruments=("EUR_USD",)):
    state_contract, source = official_context_snapshot(); records = [official(x, f"forecast-{i}") for i, x in enumerate(instruments)]
    metadata = source["component_context"]["fact_metadata_by_id"]
    for record in records:
        for fact_id in record["source_fact_ids"]:
            currency = fact_id.split(":")[1]
            metadata[fact_id] = {"fact_id": fact_id, "currency": currency, "basis_eligibility": {"versioned_semantic_thesis": True}}
    payload = envelope(source["decision_cutoff_utc"], contract_id="thesis-v1", cohort_id="thesis-c1", records=records)
    built = build_response_timing_arms(source, state_contract=state_contract, arm_contract=arm_contract(), official_theses=payload)
    for arm in built["arms"].values():
        for row in arm["records"]:
            ref = (row.get("lineage") or {}).get("official_thesis") or {}
            if ref.get("record_id"):
                ref["cited_fact_clocks"] = [{"fact_id": fact_id, "known_at_utc": "2026-01-01T00:40:00+00:00", "raw_payload_sha256": H} for fact_id in ref["source_fact_ids"]]
    return built


def env(cutoff, records, kind):
    return {"payload_id": f"{kind}-p1", "contract_id": f"{kind}-v1", "contract_sha256": H, "cohort_id": f"{kind}-c1", "cohort_sha256": H, "frozen_at_utc": cutoff, "decision_cutoff_utc": cutoff, "records": records, "research_only": True, "execution_eligible": False, "can_place_orders": False, "supported_execution_decision": "no_trade"}


def find_response(r, instrument="EUR_USD", arm="official_context_only"):
    return next(x for x in r["arms"][arm]["records"] if x["instrument"] == instrument and x["horizon_sec"] == 3600)


def quote(r, instrument="EUR_USD", record_id=None, stale=False):
    cutoff = r["decision_cutoff_utc"]
    return {"quote_record_id": record_id or f"quote-{instrument}", "instrument": instrument, "venue": "OANDA", "environment": "practice", "account_id": "101-001-37981792-007", "bid": 1.1000, "ask": 1.1002, "pip": 0.0001, "quote_time_utc": "2026-01-01T00:50:00+00:00" if stale else "2026-01-01T01:01:58+00:00", "broker_time_utc": "2026-01-01T01:01:58+00:00", "observed_at_utc": "2026-01-01T01:01:59+00:00", "retrieved_at_utc": "2026-01-01T01:01:59.500000+00:00", "immutable_payload_sha256": H, "cutoff": cutoff}


def verifier(r, forecast_id="forecast-0", evidence_id="verify-1"):
    return {"verifier_evidence_id": evidence_id, "forecast_id": forecast_id, "response_snapshot_id": r["snapshot_id"], "response_snapshot_sha256": stable_hash(r), "verified_at_utc": "2026-01-01T01:00:00+00:00", "calibration_data_cutoff_utc": "2025-12-31T23:00:00+00:00", "calibration_state": "out_of_fold_locked", "effective_n": 75, "calibrated_direction_probability": 0.7, "calibrated_cost_clear_probability": 0.65, "verifier_contract_id": "independent-verifier-v1", "verifier_contract_sha256": H, "verifier_module_sha256": H, "calibration_dataset_sha256": H, "episode_dedup_sha256": H, "factor_dedup_sha256": H, "outcome_ledger_sha256": H}


def economics(r, instrument="EUR_USD", forecast_id="forecast-0", evidence_id="verify-1", quote_id=None):
    source = find_response(r, instrument); base, q = instrument.split("_")
    return {"economics_record_id": f"econ-{instrument}", "arm_id": "official_context_only", "instrument": instrument, "horizon_sec": 3600, "response_snapshot_id": r["snapshot_id"], "response_snapshot_sha256": stable_hash(r), "response_record_hash": stable_hash(source), "forecast_decision_cutoff_utc": r["decision_cutoff_utc"], "known_at_utc": "2026-01-01T01:01:00+00:00", "forecast_direction": "buy", "forecast_id": forecast_id, "entry_quote_record_id": quote_id or f"quote-{instrument}", "entry_quote_record_hash": None, "verifier_evidence_id": evidence_id, "verifier_evidence_hash": None, "model_contract_id": "model-v1", "model_contract_sha256": H, "model_cohort_id": "model-c1", "model_cohort_sha256": H, "feature_contract_id": "features-v1", "feature_contract_sha256": H, "magnitude_model_id": "magnitude-v1", "magnitude_model_sha256": H, "cost_model_contract_id": "cost-v1", "cost_model_contract_sha256": H, "source_ids": [f"official:{base}", f"official:{q}"], "source_fact_ids": [f"fixture:{base}", f"fixture:{q}"], "independent_factor_ids": [canonical_factor_ids(instrument, "buy")[0]], "magnitude_known_at_utc": "2026-01-01T01:00:00+00:00", "expected_favorable_move_bps": 8.0, "expected_adverse_move_bps": 3.0, "expected_exit_half_spread_bps": 0.4, "entry_slippage_bps": 0.1, "expected_exit_slippage_bps": 0.1, "entry_latency_bps": 0.05, "expected_exit_latency_bps": 0.05, "rotation_close_cost_bps": 0.0, "expected_exit_spread_known_at_utc": "2026-01-01T01:00:00+00:00", "slippage_known_at_utc": "2026-01-01T01:00:00+00:00", "latency_known_at_utc": "2026-01-01T01:00:00+00:00", "rotation_close_cost_known_at_utc": "2026-01-01T01:00:00+00:00", "rotation_state_id": "flat"}


def valid_inputs(r, instrument="EUR_USD"):
    q = quote(r, instrument); v = verifier(r); e = economics(r, instrument); e["entry_quote_record_hash"] = stable_hash(q); e["verifier_evidence_hash"] = stable_hash(v)
    cutoff = r["decision_cutoff_utc"]
    return env(cutoff, [q], "quotes"), env(cutoff, [v], "verifier"), env(cutoff, [e], "economics")


def build(r, q=None, v=None, e=None, h=None): return build_after_cost_counterfactual_v2(r, contract=contract(), frozen_manifest=manifest(), venue_quotes=q, verifier_evidence=v, economics_inputs=e, hold_switch_inputs=h)


def find(result, instrument="EUR_USD"): return next(x for x in result["records"] if x["arm_id"] == "official_context_only" and x["instrument"] == instrument and x["horizon_sec"] == 3600)


def test_zero_input_full_grid_is_no_trade_and_sorted_json_roundtrip_is_accepted():
    r = response(); canonical = json.loads(json.dumps(r, sort_keys=True, separators=(",", ":")))
    result = build(canonical)
    assert result["summary"]["row_count"] == 2720
    assert result["summary"]["economics_admissible_count"] == 0
    assert result["supported_execution_decision"] == "no_trade"
    assert all(x["paper_decision"] == "no_trade" and x["expected_after_cost_ev_bps"] is None for x in result["records"])


def test_valid_quote_verifier_and_bound_economics_calculate_decomposed_ev():
    r = response(); q, v, e = valid_inputs(r); result = build(r, q, v, e); row = find(result)
    assert row["economics_admissible"] is True and row["rank_within_arm_horizon"] == 1
    assert set(row["expected_costs_bps"]) == {"entry_half_spread", "expected_exit_half_spread_bps", "entry_slippage_bps", "expected_exit_slippage_bps", "entry_latency_bps", "expected_exit_latency_bps", "rotation_close_cost_bps", "total"}
    assert row["expected_after_cost_ev_bps"] is not None


def test_economics_predating_cited_fact_is_rejected():
    r = response(); q, v, e = valid_inputs(r); record = e["records"][0]; record["known_at_utc"] = "2026-01-01T00:30:00+00:00"
    result = build(r, q, v, e); assert find(result)["expected_after_cost_ev_bps"] is None
    assert "economics_predates_cited_fact_or_thesis_clock" in find(result)["blockers"]


def test_stale_quote_is_rejected_and_completed_minute_edge_is_not_substituted():
    r = response(); q, v, e = valid_inputs(r); stale = quote(r, stale=True); q["records"] = [stale]; e["records"][0]["entry_quote_record_hash"] = stable_hash(stale)
    result = build(r, q, v, e); assert find(result)["expected_after_cost_ev_bps"] is None
    assert result["summary"]["admissible_quote_count"] == 0


@pytest.mark.parametrize("field", ["response_snapshot_sha256", "response_record_hash", "forecast_decision_cutoff_utc"])
def test_stale_or_repackaged_response_binding_is_rejected(field):
    r = response(); q, v, e = valid_inputs(r); e["records"][0][field] = H if field != "forecast_decision_cutoff_utc" else "2026-01-01T00:59:00+00:00"
    result = build(r, q, v, e); assert find(result)["expected_after_cost_ev_bps"] is None


def test_self_attested_calibration_without_verifier_cannot_produce_ev():
    r = response(); q, v, e = valid_inputs(r); e["records"][0]["calibration_effective_n"] = 999999; result = build(r, q, None, e)
    assert find(result)["expected_after_cost_ev_bps"] is None and result["summary"]["admissible_verifier_count"] == 0


def test_hold_switch_requires_and_binds_full_practice_position_and_portfolio_state():
    r = response(("EUR_USD", "GBP_USD")); cutoff = r["decision_cutoff_utc"]
    eur_quote, gbp_quote = quote(r, "EUR_USD"), quote(r, "GBP_USD")
    switch_verifier = verifier(r, "forecast-1", "verify-switch")
    hold_verifier = verifier(r, "hold-forecast", "verify-hold")
    switch = economics(r, "GBP_USD", "forecast-1", "verify-switch")
    switch["entry_quote_record_hash"] = stable_hash(gbp_quote); switch["verifier_evidence_hash"] = stable_hash(switch_verifier)
    current_close_cost = ((eur_quote["ask"] - eur_quote["bid"]) / ((eur_quote["ask"] + eur_quote["bid"]) / 2) * 10000) / 2 + 0.1 + 0.1
    switch.update({"rotation_close_cost_bps": current_close_cost, "rotation_state_id": "rotate-position-P1", "rotation_position_id": "P1", "bound_account_snapshot_id": "account-snapshot-1", "bound_position_ledger_snapshot_id": "ledger-snapshot-1"})
    hold_row = find_response(r, "EUR_USD")
    hold = {
        "hold_record_id": "hold-1", "arm_id": "official_context_only", "instrument": "EUR_USD", "horizon_sec": 3600,
        "response_snapshot_id": r["snapshot_id"], "response_snapshot_sha256": stable_hash(r), "response_record_hash": stable_hash(hold_row),
        "account_id": "101-001-37981792-007", "environment": "practice", "account_snapshot_id": "account-snapshot-1", "account_snapshot_sha256": H,
        "position_ledger_snapshot_id": "ledger-snapshot-1", "position_ledger_snapshot_sha256": H, "position_id": "P1", "trade_id": "T1",
        "position_ledger_record_id": "ledger-record-P1", "position_record_sha256": H, "units": 100, "entry_price": 1.09, "hold_direction": "buy",
        "capacity_state_id": "capacity-1", "capacity_state_sha256": H, "conflict_state_id": "conflicts-1", "conflict_state_sha256": H,
        "known_at_utc": "2026-01-01T01:01:00+00:00", "account_snapshot_at_utc": "2026-01-01T01:00:30+00:00", "position_ledger_at_utc": "2026-01-01T01:00:30+00:00", "portfolio_state_known_at_utc": "2026-01-01T01:00:45+00:00",
        "current_close_quote_record_id": eur_quote["quote_record_id"], "current_close_quote_record_hash": stable_hash(eur_quote),
        "hold_verifier_evidence_id": "verify-hold", "hold_verifier_evidence_hash": stable_hash(hold_verifier), "hold_forecast_id": "hold-forecast",
        "source_fact_ids": ["fixture:EUR", "fixture:USD"], "independent_factor_ids": [canonical_factor_ids("EUR_USD", "buy")[0]],
        "switch_allowed_instruments": ["GBP_USD"], "conflicted_factor_ids": [], "maximum_switch_units": 1000,
        "expected_hold_favorable_move_bps": 2.0, "expected_hold_adverse_move_bps": 2.0, "expected_hold_exit_half_spread_bps": 0.5,
        "expected_hold_exit_slippage_bps": 0.1, "expected_hold_exit_latency_bps": 0.1, "current_close_slippage_bps": 0.1, "current_close_latency_bps": 0.1,
        "hold_costs_known_at_utc": "2026-01-01T01:00:00+00:00", "hold_magnitude_known_at_utc": "2026-01-01T01:00:00+00:00"
    }
    result = build(r, env(cutoff, [eur_quote, gbp_quote], "quotes"), env(cutoff, [switch_verifier, hold_verifier], "verifier"), env(cutoff, [switch], "economics"), env(cutoff, [hold], "hold"))
    assert result["summary"]["hold_switch_count"] == 1
    comparison = result["hold_switch_counterfactuals"][0]
    assert comparison["account_snapshot_id"] == "account-snapshot-1" and comparison["position_id"] == "P1"
    assert comparison["best_switch_candidate"]["instrument"] == "GBP_USD"
    assert comparison["supported_execution_decision"] == "no_trade"


def test_incomplete_hold_state_never_emits_comparison():
    r = response(); result = build(r, h=env(r["decision_cutoff_utc"], [{"hold_record_id": "broken"}], "hold"))
    assert result["summary"]["hold_switch_count"] == 0 and result["summary"]["input_rejection_count"] >= 1


def test_manifest_artifact_set_and_universe_are_immutable():
    r = response(); bad = manifest(); bad["artifacts"].pop(next(iter(bad["artifacts"])))
    with pytest.raises(AfterCostV2Error, match="artifact set"): build_after_cost_counterfactual_v2(r, contract=contract(), frozen_manifest=bad)
    bad_contract = contract(); bad_contract["expected_instruments"] = list(reversed(bad_contract["expected_instruments"]))
    with pytest.raises(AfterCostV2Error, match="universe hash"): build_after_cost_counterfactual_v2(r, contract=bad_contract, frozen_manifest=manifest())
