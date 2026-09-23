from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

try:
    from forex_system.contracts.currency_state import stable_hash
    from forex_system.contracts.signed_currency_exposure import canonical_factor_ids
    from forex_system.research.after_cost_input_envelopes_v3 import (
        canonical_sha256,
        envelope_sha256,
        seal_envelope,
        seal_record,
    )
    from forex_system.research.currency_state_after_cost_counterfactual_v3 import (
        AfterCostV3Error,
        build_after_cost_counterfactual_v3,
    )
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import stable_hash
    from src.forex_system.contracts.signed_currency_exposure import canonical_factor_ids
    from src.forex_system.research.after_cost_input_envelopes_v3 import (
        canonical_sha256,
        envelope_sha256,
        seal_envelope,
        seal_record,
    )
    from src.forex_system.research.currency_state_after_cost_counterfactual_v3 import (
        AfterCostV3Error,
        build_after_cost_counterfactual_v3,
    )

from test_currency_state_after_cost_counterfactual_v2 import find_response, response


ROOT = Path(__file__).resolve().parent
H = "a" * 64
EXACT_FIELDS = (
    "model_contract_id", "model_contract_sha256", "model_cohort_id",
    "model_cohort_sha256", "feature_contract_id", "feature_contract_sha256",
    "magnitude_model_id", "magnitude_model_sha256", "cost_model_contract_id",
    "cost_model_contract_sha256",
)


def contract():
    return json.loads((ROOT / "config" / "currency_state_after_cost_counterfactual_v3.json").read_text())


def manifest():
    c = contract()
    return {
        "manifest_id": c["required_frozen_manifest_id"],
        "contract_id": c["contract_id"],
        "cohort_id": c["counterfactual_cohort_id"],
        "artifacts": {
            label: {"relative_path": f"fixture/{label}", "sha256": H}
            for label in c["required_manifest_artifacts"]
        },
    }


def envelope(name, cutoff, records):
    c = contract()
    binding = c["canonical_envelope_contract"]["producer_bindings"][name]
    sealed = [seal_record(record) for record in records]
    return seal_envelope({
        "payload_id": f"{name}-payload-1",
        "contract_id": binding["producer_contract_id"],
        "contract_sha256": H,
        "cohort_id": binding["producer_cohort_id"],
        "cohort_sha256": binding["producer_cohort_sha256"],
        "producer_contract_id": binding["producer_contract_id"],
        "producer_module_id": binding["manifest_artifact"],
        "producer_module_sha256": H,
        "frozen_at_utc": cutoff,
        "decision_cutoff_utc": cutoff,
        "records": sealed,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    })


def quote_record(instrument="EUR_USD"):
    raw = {
        "quote_record_id": f"quote-{instrument}", "instrument": instrument,
        "venue": "OANDA", "environment": "practice",
        "account_id": "101-001-37981792-007", "bid": 1.1000,
        "ask": 1.1002, "pip": 0.0001,
        "quote_time_utc": "2026-01-01T01:01:58+00:00",
        "broker_time_utc": "2026-01-01T01:01:58+00:00",
        "observed_at_utc": "2026-01-01T01:01:59+00:00",
        "retrieved_at_utc": "2026-01-01T01:01:59.500000+00:00",
    }
    raw["immutable_payload"] = copy.deepcopy(raw)
    raw["immutable_payload_sha256"] = canonical_sha256(raw["immutable_payload"])
    return raw


def model_fields():
    return {
        "model_contract_id": "model-v3", "model_contract_sha256": "1" * 64,
        "model_cohort_id": "model-cohort-v3", "model_cohort_sha256": "2" * 64,
        "feature_contract_id": "features-v3", "feature_contract_sha256": "3" * 64,
        "magnitude_model_id": "magnitude-v3", "magnitude_model_sha256": "4" * 64,
        "cost_model_contract_id": "cost-v3", "cost_model_contract_sha256": "5" * 64,
    }


def forecast_record(r, instrument="EUR_USD", forecast_id=None):
    source = find_response(r, instrument)
    return {
        "forecast_record_id": forecast_id or f"forecast-v3-{instrument}",
        "arm_id": "official_context_only", "instrument": instrument,
        "horizon_sec": 3600, "research_direction": source["research_direction"],
        "response_snapshot_id": r["snapshot_id"],
        "response_snapshot_sha256": stable_hash(r),
        "response_record_hash": stable_hash(source),
        "forecast_decision_cutoff_utc": r["decision_cutoff_utc"],
        **model_fields(),
    }


def verifier_record(r, forecast, evidence_id=None):
    return {
        "verifier_evidence_id": evidence_id or f"verify-{forecast['instrument']}",
        "forecast_record_id": forecast["forecast_record_id"],
        "forecast_record_sha256": canonical_sha256(forecast),
        "arm_id": forecast["arm_id"], "instrument": forecast["instrument"],
        "horizon_sec": forecast["horizon_sec"],
        "research_direction": forecast["research_direction"],
        "response_snapshot_id": forecast["response_snapshot_id"],
        "response_snapshot_sha256": forecast["response_snapshot_sha256"],
        "response_record_hash": forecast["response_record_hash"],
        "response_bound_at_utc": r["decision_cutoff_utc"],
        "verified_at_utc": r["decision_cutoff_utc"],
        "calibration_data_cutoff_utc": "2025-12-31T23:00:00+00:00",
        "calibration_state": "out_of_fold_locked", "effective_n": 75,
        "calibrated_direction_probability": 0.7,
        "calibrated_cost_clear_probability": 0.65,
        "verifier_contract_id": "independent-verifier-v3",
        "verifier_contract_sha256": "6" * 64,
        "verifier_module_sha256": H,
        "calibration_dataset_sha256": "7" * 64,
        "episode_dedup_sha256": "8" * 64,
        "factor_dedup_sha256": "9" * 64,
        "outcome_ledger_sha256": "b" * 64,
        **model_fields(),
    }


def flat_account_records(instruments=("EUR_USD",)):
    common = {"account_id": "101-001-37981792-007", "environment": "practice", "known_at_utc": "2026-01-01T01:01:30+00:00"}
    return [
        {"account_state_record_id": "account-record-1", "record_type": "account", "account_snapshot_id": "account-snapshot-1", "open_position_count": 0, **common},
        {"account_state_record_id": "capacity-record-1", "record_type": "capacity", "capacity_state_id": "capacity-1", "switch_allowed_instruments": list(instruments), "maximum_switch_units": 1000, **common},
        {"account_state_record_id": "conflict-record-1", "record_type": "conflict", "conflict_state_id": "conflict-1", "conflicted_factor_ids": [], **common},
    ]


def economics_record(r, forecast, quote, verifier, accounts):
    instrument = forecast["instrument"]
    base, counter = instrument.split("_")
    account = next(x for x in accounts if x["record_type"] == "account")
    capacity = next(x for x in accounts if x["record_type"] == "capacity")
    conflict = next(x for x in accounts if x["record_type"] == "conflict")
    return {
        "economics_record_id": f"economics-{instrument}",
        "arm_id": forecast["arm_id"], "instrument": instrument,
        "horizon_sec": forecast["horizon_sec"],
        "response_snapshot_id": forecast["response_snapshot_id"],
        "response_snapshot_sha256": forecast["response_snapshot_sha256"],
        "response_record_hash": forecast["response_record_hash"],
        "forecast_decision_cutoff_utc": r["decision_cutoff_utc"],
        "known_at_utc": r["decision_cutoff_utc"],
        "economics_evaluated_at_utc": r["decision_cutoff_utc"],
        "forecast_direction": forecast["research_direction"],
        "forecast_record_id": forecast["forecast_record_id"],
        "forecast_record_sha256": canonical_sha256(forecast),
        "forecast_record": copy.deepcopy(forecast),
        "entry_quote_record_id": quote["quote_record_id"],
        "entry_quote_record_hash": quote["canonical_record_sha256"],
        "verifier_evidence_id": verifier["verifier_evidence_id"],
        "verifier_evidence_hash": verifier["canonical_record_sha256"],
        **model_fields(),
        "source_ids": [f"official:{base}", f"official:{counter}"],
        "source_fact_ids": [f"fixture:{base}", f"fixture:{counter}"],
        "independent_factor_ids": [canonical_factor_ids(instrument, forecast["research_direction"])[0]],
        "magnitude_known_at_utc": "2026-01-01T01:01:30+00:00",
        "expected_favorable_move_bps": 8.0, "expected_adverse_move_bps": 3.0,
        "expected_exit_half_spread_bps": 0.4, "entry_slippage_bps": 0.1,
        "expected_exit_slippage_bps": 0.1, "entry_latency_bps": 0.05,
        "expected_exit_latency_bps": 0.05, "rotation_close_cost_bps": 0.0,
        "expected_exit_spread_known_at_utc": "2026-01-01T01:01:30+00:00",
        "slippage_known_at_utc": "2026-01-01T01:01:30+00:00",
        "latency_known_at_utc": "2026-01-01T01:01:30+00:00",
        "rotation_close_cost_known_at_utc": "2026-01-01T01:01:30+00:00",
        "rotation_state_id": "flat:account-snapshot-1", "rotation_position_id": None,
        "proposed_units": 100,
        "bound_account_record_id": account["account_state_record_id"],
        "bound_account_record_sha256": account["canonical_record_sha256"],
        "bound_capacity_record_id": capacity["account_state_record_id"],
        "bound_capacity_record_sha256": capacity["canonical_record_sha256"],
        "bound_conflict_record_id": conflict["account_state_record_id"],
        "bound_conflict_record_sha256": conflict["canonical_record_sha256"],
    }


def valid_inputs(r, instrument="EUR_USD"):
    cutoff = r["decision_cutoff_utc"]
    q_env = envelope("venue_quotes", cutoff, [quote_record(instrument)])
    forecast = forecast_record(r, instrument)
    v_env = envelope("verifier_evidence", cutoff, [verifier_record(r, forecast)])
    a_env = envelope("account_state", cutoff, flat_account_records((instrument,)))
    econ = economics_record(r, forecast, q_env["records"][0], v_env["records"][0], a_env["records"])
    e_env = envelope("economics_inputs", cutoff, [econ])
    return q_env, v_env, e_env, a_env


def build(r, q=None, v=None, e=None, h=None, a=None):
    return build_after_cost_counterfactual_v3(
        r, contract=contract(), frozen_manifest=manifest(), venue_quotes=q,
        verifier_evidence=v, economics_inputs=e, hold_switch_inputs=h,
        account_state=a,
    )


def find(result, instrument="EUR_USD"):
    return next(row for row in result["records"] if row["arm_id"] == "official_context_only" and row["instrument"] == instrument and row["horizon_sec"] == 3600)


def open_account_records(instrument="EUR_USD", units=100):
    direction = "buy" if units > 0 else "sell"
    common = {"account_id": "101-001-37981792-007", "environment": "practice", "known_at_utc": "2026-01-01T01:01:30+00:00"}
    return [
        {"account_state_record_id": "account-record-open", "record_type": "account", "account_snapshot_id": "account-snapshot-open", "open_position_count": 1, **common},
        {"account_state_record_id": "position-record-1", "record_type": "position", "position_id": "P1", "trade_id": "T1", "instrument": instrument, "units": units, "entry_price": 1.09, "direction": direction, **common},
        {"account_state_record_id": "ledger-record-1", "record_type": "ledger", "ledger_snapshot_id": "ledger-snapshot-1", "position_ledger_record_id": "ledger-position-1", "position_id": "P1", "trade_id": "T1", "instrument": instrument, **common},
        {"account_state_record_id": "capacity-record-open", "record_type": "capacity", "capacity_state_id": "capacity-open", "switch_allowed_instruments": [], "maximum_switch_units": 1000, **common},
        {"account_state_record_id": "conflict-record-open", "record_type": "conflict", "conflict_state_id": "conflict-open", "conflicted_factor_ids": [], **common},
    ]


def hold_record(r, forecast, quote, verifier, accounts, hold_direction="buy"):
    by_type = {row["record_type"]: row for row in accounts}
    return {
        "hold_record_id": "hold-1", "arm_id": "official_context_only",
        "instrument": forecast["instrument"], "horizon_sec": 3600,
        "response_snapshot_id": r["snapshot_id"], "response_snapshot_sha256": stable_hash(r),
        "response_record_hash": forecast["response_record_hash"],
        "account_record_id": by_type["account"]["account_state_record_id"], "account_record_sha256": by_type["account"]["canonical_record_sha256"],
        "position_record_id": by_type["position"]["account_state_record_id"], "position_record_sha256": by_type["position"]["canonical_record_sha256"],
        "ledger_record_id": by_type["ledger"]["account_state_record_id"], "ledger_record_sha256": by_type["ledger"]["canonical_record_sha256"],
        "capacity_record_id": by_type["capacity"]["account_state_record_id"], "capacity_record_sha256": by_type["capacity"]["canonical_record_sha256"],
        "conflict_record_id": by_type["conflict"]["account_state_record_id"], "conflict_record_sha256": by_type["conflict"]["canonical_record_sha256"],
        "hold_direction": hold_direction, "hold_evaluated_at_utc": r["decision_cutoff_utc"],
        "current_close_quote_record_id": quote["quote_record_id"], "current_close_quote_record_hash": quote["canonical_record_sha256"],
        "hold_verifier_evidence_id": verifier["verifier_evidence_id"], "hold_verifier_evidence_hash": verifier["canonical_record_sha256"],
        "hold_forecast_id": forecast["forecast_record_id"], "hold_forecast_sha256": canonical_sha256(forecast), "hold_forecast_record": copy.deepcopy(forecast),
        "source_fact_ids": ["fixture:EUR", "fixture:USD"], "independent_factor_ids": [canonical_factor_ids("EUR_USD", "buy")[0]],
        "expected_hold_favorable_move_bps": 2.0, "expected_hold_adverse_move_bps": 2.0,
        "expected_hold_exit_half_spread_bps": 0.5, "expected_hold_exit_slippage_bps": 0.1,
        "expected_hold_exit_latency_bps": 0.1, "current_close_slippage_bps": 0.1,
        "current_close_latency_bps": 0.1, "hold_costs_known_at_utc": "2026-01-01T01:01:30+00:00",
        "hold_magnitude_known_at_utc": "2026-01-01T01:01:30+00:00",
    }


def test_zero_input_full_grid_is_sorted_research_only_no_trade():
    r = json.loads(json.dumps(response(), sort_keys=True, separators=(",", ":")))
    result = build(r)
    assert result["summary"]["row_count"] == 2720
    assert result["summary"]["economics_admissible_count"] == 0
    assert result["supported_execution_decision"] == "no_trade"
    assert result["schema_version"] == 3


def test_valid_exact_cell_economics_preserves_all_canonical_input_hashes():
    r = response(); q, v, e, a = valid_inputs(r)
    result = build(r, q, v, e, a=a); row = find(result)
    assert row["economics_admissible"] is True
    assert row["economics_canonical_record_sha256"] == e["records"][0]["canonical_record_sha256"]
    assert row["verifier_canonical_record_sha256"] == v["records"][0]["canonical_record_sha256"]
    assert row["quote_canonical_record_sha256"] == q["records"][0]["canonical_record_sha256"]
    assert result["admissible_input_hashes"]["envelopes"]["economics_inputs"] == e["canonical_envelope_sha256"]
    assert result["allocations"]["official_context_only"]["3600"]["top_one"]["forecast_record_sha256"] == e["records"][0]["forecast_record_sha256"]


def test_v2_cross_pair_verifier_reuse_exploit_is_rejected():
    r = response(("EUR_USD", "GBP_USD")); cutoff = r["decision_cutoff_utc"]
    q = envelope("venue_quotes", cutoff, [quote_record("EUR_USD"), quote_record("GBP_USD")])
    eur_forecast = forecast_record(r, "EUR_USD")
    v = envelope("verifier_evidence", cutoff, [verifier_record(r, eur_forecast)])
    a = envelope("account_state", cutoff, flat_account_records(("EUR_USD", "GBP_USD")))
    gbp_forecast = forecast_record(r, "GBP_USD")
    econ = economics_record(r, gbp_forecast, q["records"][1], v["records"][0], a["records"])
    econ["verifier_evidence_id"] = v["records"][0]["verifier_evidence_id"]
    econ["verifier_evidence_hash"] = v["records"][0]["canonical_record_sha256"]
    e = envelope("economics_inputs", cutoff, [econ])
    result = build(r, q, v, e, a=a)
    assert find(result, "GBP_USD")["economics_admissible"] is False
    assert any(x["reason"] == "v2_cross_pair_verifier_reuse_exploit_rejected" for x in result["strict_input_rejections"])


def test_invented_forecast_record_exploit_is_rejected_even_when_envelope_is_resealed():
    r = response(); q, v, e, a = valid_inputs(r)
    raw = copy.deepcopy(e["records"][0]); raw.pop("canonical_record_sha256")
    raw["forecast_record"]["research_direction"] = "sell"
    e = envelope("economics_inputs", r["decision_cutoff_utc"], [raw])
    result = build(r, q, v, e, a=a)
    assert find(result)["economics_admissible"] is False
    assert any(x["reason"] == "invented_or_repackaged_forecast_record_rejected" for x in result["strict_input_rejections"])


def test_economics_cannot_be_evaluated_before_quote_retrieval():
    r = response(); q, v, e, a = valid_inputs(r)
    raw = copy.deepcopy(e["records"][0]); raw.pop("canonical_record_sha256")
    raw["economics_evaluated_at_utc"] = "2026-01-01T01:01:00+00:00"
    raw["known_at_utc"] = "2026-01-01T01:01:00+00:00"
    e = envelope("economics_inputs", r["decision_cutoff_utc"], [raw])
    result = build(r, q, v, e, a=a)
    assert any(x["reason"] == "economics_evaluated_before_quote_or_verifier_or_after_cutoff" for x in result["strict_input_rejections"])


def test_verifier_cannot_claim_response_before_response_cutoff():
    r = response(); q, v, e, a = valid_inputs(r)
    raw = copy.deepcopy(v["records"][0]); raw.pop("canonical_record_sha256")
    raw["response_bound_at_utc"] = "2026-01-01T01:00:00+00:00"
    raw["verified_at_utc"] = "2026-01-01T01:00:00+00:00"
    v = envelope("verifier_evidence", r["decision_cutoff_utc"], [raw])
    result = build(r, q, v, e, a=a)
    assert any(x["reason"] == "verifier_claimed_response_before_response_existed" for x in result["strict_input_rejections"])


def test_arbitrary_flat_rotation_state_cannot_enter_ranked_population():
    r = response(); q, v, e, a = valid_inputs(r)
    raw = copy.deepcopy(e["records"][0]); raw.pop("canonical_record_sha256")
    raw["rotation_state_id"] = "flat"
    e = envelope("economics_inputs", r["decision_cutoff_utc"], [raw])
    result = build(r, q, v, e, a=a)
    assert find(result)["economics_admissible"] is False
    assert any(x["reason"] == "arbitrary_flat_rotation_state_or_cost_rejected" for x in result["strict_input_rejections"])


def test_flat_account_claim_cannot_hide_a_position_record():
    r = response(); q, v, e, a = valid_inputs(r)
    hidden = open_account_records()[1]
    raw_records = [copy.deepcopy(row) for row in a["records"]]
    for row in raw_records:
        row.pop("canonical_record_sha256")
    raw_records.append(hidden)
    a = envelope("account_state", r["decision_cutoff_utc"], raw_records)
    result = build(r, q, v, e, a=a)
    assert find(result)["economics_admissible"] is False
    assert any(x["reason"] == "account_open_position_count_or_ledger_inventory_mismatch" for x in result["strict_input_rejections"])


def malformed_account_constituents(case, base):
    account = next(row for row in base if row["record_type"] == "account")
    capacity = next(row for row in base if row["record_type"] == "capacity")
    conflict = next(row for row in base if row["record_type"] == "conflict")
    common = {
        "account_id": "101-001-37981792-007", "environment": "practice",
        "known_at_utc": "2026-01-01T01:01:30+00:00",
    }
    if case == "malformed_position":
        return [{"account_state_record_id": "bad-position", "record_type": "position", "position_id": "", "trade_id": "T-bad", "instrument": "EUR_USD", "units": 100, "entry_price": 1.09, "direction": "buy", **common}]
    if case == "malformed_ledger":
        return [{"account_state_record_id": "bad-ledger", "record_type": "ledger", "ledger_snapshot_id": "L-bad", "position_ledger_record_id": "LR-bad", "position_id": "P-bad", "trade_id": "", "instrument": "EUR_USD", **common}]
    if case == "malformed_capacity":
        return [{"account_state_record_id": "bad-capacity", "record_type": "capacity", "capacity_state_id": "C-bad", "switch_allowed_instruments": ["EUR_USD"], "maximum_switch_units": -1, **common}]
    if case == "malformed_conflict":
        return [{"account_state_record_id": "bad-conflict", "record_type": "conflict", "conflict_state_id": "X-bad", "conflicted_factor_ids": "not-a-list", **common}]
    if case == "wrong_environment":
        return [{"account_state_record_id": "wrong-environment", "record_type": "account", "account_snapshot_id": "wrong-env", "open_position_count": 0, **{**common, "environment": "live"}}]
    if case == "wrong_account":
        return [{"account_state_record_id": "wrong-account", "record_type": "account", "account_snapshot_id": "wrong-account", "open_position_count": 0, **{**common, "account_id": "other"}}]
    if case == "unrecognized_type":
        return [{"account_state_record_id": "unknown-type", "record_type": "wallet", **common}]
    if case == "duplicate_contradictory_account":
        return [{**account, "open_position_count": 99}]
    if case == "duplicate_contradictory_capacity":
        return [{**capacity, "maximum_switch_units": 0}]
    if case == "duplicate_contradictory_conflict":
        return [{**conflict, "conflicted_factor_ids": ["EUR:long"]}]
    if case == "malformed_position_and_ledger":
        return [
            {"account_state_record_id": "bad-position-2", "record_type": "position", "position_id": "P-bad", "trade_id": "T-bad", "instrument": "", "units": 100, "entry_price": 1.09, "direction": "buy", **common},
            {"account_state_record_id": "bad-ledger-2", "record_type": "ledger", "ledger_snapshot_id": "", "position_ledger_record_id": "LR-bad", "position_id": "P-bad", "trade_id": "T-bad", "instrument": "EUR_USD", **common},
        ]
    if case == "zero_unit_position":
        return [{"account_state_record_id": "zero-position", "record_type": "position", "position_id": "P-zero", "trade_id": "T-zero", "instrument": "EUR_USD", "units": 0, "entry_price": 1.09, "direction": "buy", **common}]
    raise AssertionError(case)


def test_v2_sell_position_buy_response_hold_exploit_is_rejected():
    r = response(); cutoff = r["decision_cutoff_utc"]
    q = envelope("venue_quotes", cutoff, [quote_record()])
    f = forecast_record(r); v = envelope("verifier_evidence", cutoff, [verifier_record(r, f)])
    a = envelope("account_state", cutoff, open_account_records(units=-100))
    h = envelope("hold_switch_inputs", cutoff, [hold_record(r, f, q["records"][0], v["records"][0], a["records"], hold_direction="buy")])
    result = build(r, q, v, h=h, a=a)
    assert result["summary"]["hold_switch_count"] == 0
    assert any(x["reason"] == "v2_sell_position_buy_response_direction_exploit_rejected" for x in result["strict_input_rejections"])


def test_valid_hold_recomputes_practice_state_and_outputs_all_hashes():
    r = response(); cutoff = r["decision_cutoff_utc"]
    q = envelope("venue_quotes", cutoff, [quote_record()])
    f = forecast_record(r); v = envelope("verifier_evidence", cutoff, [verifier_record(r, f)])
    a = envelope("account_state", cutoff, open_account_records(units=100))
    h = envelope("hold_switch_inputs", cutoff, [hold_record(r, f, q["records"][0], v["records"][0], a["records"])])
    result = build(r, q, v, h=h, a=a)
    assert result["summary"]["hold_switch_count"] == 1
    item = result["hold_switch_counterfactuals"][0]
    for field in ("hold_canonical_record_sha256", "account_canonical_record_sha256", "position_canonical_record_sha256", "ledger_canonical_record_sha256", "capacity_canonical_record_sha256", "conflict_canonical_record_sha256", "verifier_canonical_record_sha256", "quote_canonical_record_sha256"):
        assert len(item[field]) == 64


def test_record_tamper_is_detected_even_if_attacker_rehashes_outer_envelope():
    r = response(); q, _, _, _ = valid_inputs(r)
    q["records"][0]["bid"] = 9.0
    q["canonical_envelope_sha256"] = envelope_sha256(q)
    with pytest.raises(AfterCostV3Error, match="canonical record hash mismatch"):
        build(r, q=q)


def test_envelope_tamper_and_unfrozen_producer_hash_are_rejected():
    r = response(); q, _, _, _ = valid_inputs(r)
    q["payload_id"] = "tampered"
    with pytest.raises(AfterCostV3Error, match="canonical envelope hash mismatch"):
        build(r, q=q)
    q, _, _, _ = valid_inputs(r)
    q["producer_module_sha256"] = "f" * 64
    q["canonical_envelope_sha256"] = envelope_sha256(q)
    with pytest.raises(AfterCostV3Error, match="not frozen manifest bytes"):
        build(r, q=q)


def test_fake_immutable_quote_payload_hash_is_rejected():
    r = response(); raw = quote_record(); raw["immutable_payload_sha256"] = "f" * 64
    q = envelope("venue_quotes", r["decision_cutoff_utc"], [raw])
    result = build(r, q=q)
    assert result["summary"]["admissible_quote_count"] == 0
    assert any(x["reason"] == "immutable_quote_payload_hash_mismatch" for x in result["strict_input_rejections"])


def test_manifest_artifact_set_and_exact_68_universe_remain_immutable():
    r = response(); bad = manifest(); bad["artifacts"].pop(next(iter(bad["artifacts"])))
    with pytest.raises(AfterCostV3Error, match="artifact set"):
        build_after_cost_counterfactual_v3(r, contract=contract(), frozen_manifest=bad)
    bad_contract = contract(); bad_contract["expected_instruments"] = list(reversed(bad_contract["expected_instruments"]))
    with pytest.raises(AfterCostV3Error, match="universe hash"):
        build_after_cost_counterfactual_v3(r, contract=bad_contract, frozen_manifest=manifest())
