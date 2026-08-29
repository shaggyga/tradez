from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

try:
    from forex_system.contracts.currency_state import stable_hash
    from forex_system.research.currency_state_after_cost_counterfactual_v4 import (
        AfterCostV4Error,
        EXPECTED_ARTIFACT_PATHS,
        build_after_cost_counterfactual_v4,
    )
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import stable_hash
    from src.forex_system.research.currency_state_after_cost_counterfactual_v4 import (
        AfterCostV4Error,
        EXPECTED_ARTIFACT_PATHS,
        build_after_cost_counterfactual_v4,
    )

from test_currency_state_after_cost_counterfactual_v2 import find_response, response
from test_currency_state_after_cost_counterfactual_v3 import (
    H,
    economics_record,
    envelope,
    flat_account_records,
    forecast_record,
    hold_record,
    malformed_account_constituents,
    manifest as upstream_manifest,
    open_account_records,
    quote_record,
    verifier_record,
    contract as upstream_contract,
)


ROOT = Path(__file__).resolve().parent


def contract():
    return json.loads(
        (ROOT / "config" / "currency_state_after_cost_counterfactual_v4.json").read_text()
    )


def manifest():
    c = contract()
    return {
        "manifest_id": c["required_frozen_manifest_id"],
        "contract_id": c["contract_id"],
        "cohort_id": c["counterfactual_cohort_id"],
        "artifacts": {
            label: {"relative_path": path, "sha256": H}
            for label, path in EXPECTED_ARTIFACT_PATHS.items()
        },
    }


def build(r, q=None, v=None, e=None, h=None, a=None, *, m=None, c=None):
    return build_after_cost_counterfactual_v4(
        r,
        contract=c or contract(),
        frozen_manifest=m or manifest(),
        upstream_v3_contract=upstream_contract(),
        upstream_v3_manifest=upstream_manifest(),
        venue_quotes=q,
        verifier_evidence=v,
        economics_inputs=e,
        hold_switch_inputs=h,
        account_state=a,
    )


def find(result, instrument="EUR_USD"):
    return next(
        row for row in result["records"]
        if row["arm_id"] == "official_context_only"
        and row["instrument"] == instrument
        and row["horizon_sec"] == 3600
    )


def valid_inputs(r, instrument="EUR_USD"):
    cutoff = r["decision_cutoff_utc"]
    q = envelope("venue_quotes", cutoff, [quote_record(instrument)])
    forecast = forecast_record(r, instrument)
    v = envelope("verifier_evidence", cutoff, [verifier_record(r, forecast)])
    a = envelope("account_state", cutoff, flat_account_records((instrument,)))
    econ = economics_record(r, forecast, q["records"][0], v["records"][0], a["records"])
    e = envelope("economics_inputs", cutoff, [econ])
    return q, v, e, a


def reseal(name, cutoff, records):
    raw = []
    for record in records:
        item = copy.deepcopy(record)
        item.pop("canonical_record_sha256", None)
        raw.append(item)
    return envelope(name, cutoff, raw)


def open_rotation_inputs(r, *, include_close_binding=True, reported_cost=None):
    cutoff = r["decision_cutoff_utc"]
    q = envelope(
        "venue_quotes", cutoff,
        [quote_record("EUR_USD"), quote_record("GBP_USD")],
    )
    forecast = forecast_record(r, "GBP_USD")
    v = envelope("verifier_evidence", cutoff, [verifier_record(r, forecast)])
    raw_accounts = open_account_records("EUR_USD", units=100)
    next(row for row in raw_accounts if row["record_type"] == "capacity")[
        "switch_allowed_instruments"
    ] = ["GBP_USD"]
    a = envelope("account_state", cutoff, raw_accounts)
    econ = economics_record(r, forecast, q["records"][1], v["records"][0], a["records"])
    by_type = {row["record_type"]: row for row in a["records"]}
    econ.update({
        "bound_position_record_id": by_type["position"]["account_state_record_id"],
        "bound_position_record_sha256": by_type["position"]["canonical_record_sha256"],
        "bound_ledger_record_id": by_type["ledger"]["account_state_record_id"],
        "bound_ledger_record_sha256": by_type["ledger"]["canonical_record_sha256"],
        "rotation_state_id": "rotate:P1:ledger-snapshot-1",
        "rotation_position_id": "P1",
        "rotation_close_cost_known_at_utc": cutoff,
    })
    close_quote = q["records"][0]
    midpoint = (close_quote["bid"] + close_quote["ask"]) / 2.0
    recomputed = (close_quote["ask"] - close_quote["bid"]) / (2 * midpoint) * 10_000 + 0.1 + 0.1
    econ["rotation_close_cost_bps"] = recomputed if reported_cost is None else reported_cost
    if include_close_binding:
        econ.update({
            "rotation_close_quote_record_id": close_quote["quote_record_id"],
            "rotation_close_quote_record_hash": close_quote["canonical_record_sha256"],
            "rotation_close_side": "sell",
            "rotation_close_slippage_bps": 0.1,
            "rotation_close_latency_bps": 0.1,
        })
    e = envelope("economics_inputs", cutoff, [econ])
    return q, v, e, a


def test_zero_input_grid_is_v4_research_only_no_trade():
    result = build(response())
    assert result["schema_version"] == 4
    assert result["summary"]["row_count"] == 2720
    assert result["summary"]["economics_admissible_count"] == 0
    assert result["supported_execution_decision"] == "no_trade"
    assert result["decision_fingerprint"]


def test_valid_flat_economics_remains_admissible_with_exact_hash_lineage():
    r = response(); q, v, e, a = valid_inputs(r)
    result = build(r, q, v, e, a=a)
    row = find(result)
    assert row["economics_admissible"] is True
    assert row["rank_within_arm_horizon"] == 1
    assert result["input_envelope_dispositions"]["economics_inputs"]["admitted_whole"] is True


ACCOUNT_CASES = (
    "malformed_position", "malformed_ledger", "malformed_capacity",
    "malformed_conflict", "wrong_environment", "wrong_account",
    "unrecognized_type", "duplicate_contradictory_account",
    "duplicate_contradictory_capacity", "duplicate_contradictory_conflict",
    "malformed_position_and_ledger", "zero_unit_position",
)


@pytest.mark.parametrize("case", ACCOUNT_CASES)
@pytest.mark.parametrize("reverse", [False, True])
def test_any_bad_account_constituent_atomically_invalidates_whole_snapshot(case, reverse):
    r = response(); q, v, e, a = valid_inputs(r)
    records = list(a["records"]) + malformed_account_constituents(case, a["records"])
    if reverse:
        records.reverse()
    bad_a = reseal("account_state", r["decision_cutoff_utc"], records)
    result = build(r, q, v, e, a=bad_a)
    assert result["input_envelope_dispositions"]["account_state"]["invalidated_whole"] is True
    assert result["summary"]["economics_admissible_count"] == 0
    assert result["summary"]["ranked_count"] == 0
    assert result["summary"]["selectable_count"] == 0
    assert result["summary"]["hold_switch_count"] == 0


def test_open_count_is_strict_nonbool_integer_and_multiple_positions_fail_closed():
    r = response(); _, _, _, base = valid_inputs(r)
    for bad_count in (True, 0.9):
        records = copy.deepcopy(base["records"])
        next(row for row in records if row["record_type"] == "account")["open_position_count"] = bad_count
        result = build(r, a=reseal("account_state", r["decision_cutoff_utc"], records))
        assert result["input_envelope_dispositions"]["account_state"]["invalidated_whole"] is True
    records = open_account_records("EUR_USD")
    second = copy.deepcopy(records[1]); second.update({
        "account_state_record_id": "position-record-2", "position_id": "P2",
        "trade_id": "T2", "instrument": "GBP_USD",
    })
    second_ledger = copy.deepcopy(records[2]); second_ledger.update({
        "account_state_record_id": "ledger-record-2", "ledger_snapshot_id": "ledger-snapshot-2",
        "position_ledger_record_id": "ledger-position-2", "position_id": "P2",
        "trade_id": "T2", "instrument": "GBP_USD",
    })
    next(row for row in records if row["record_type"] == "account")["open_position_count"] = 2
    records += [second, second_ledger]
    result = build(r, a=envelope("account_state", r["decision_cutoff_utc"], records))
    assert result["input_envelope_dispositions"]["account_state"]["invalidated_whole"] is True
    assert any(x["reason"] == "multiple_open_positions_outside_v4_single_position_policy" for x in result["v4_input_rejections"])


@pytest.mark.parametrize("reverse", [False, True])
def test_forecast_id_equivocation_atomically_invalidates_verifier_envelope(reverse):
    r = response(("EUR_USD", "GBP_USD")); cutoff = r["decision_cutoff_utc"]
    forecasts = [
        forecast_record(r, "EUR_USD", "equivocated-forecast"),
        forecast_record(r, "GBP_USD", "equivocated-forecast"),
    ]
    rows = [verifier_record(r, forecasts[0], "v-eur"), verifier_record(r, forecasts[1], "v-gbp")]
    if reverse:
        rows.reverse()
    result = build(r, v=envelope("verifier_evidence", cutoff, rows))
    assert result["input_envelope_dispositions"]["verifier_evidence"]["invalidated_whole"] is True
    assert result["summary"]["admissible_verifier_count"] == 0


@pytest.mark.parametrize("name", ["quote_id", "instrument"])
def test_quote_identity_collision_has_no_first_or_last_winner(name):
    r = response(("EUR_USD", "GBP_USD")); cutoff = r["decision_cutoff_utc"]
    one, two = quote_record("EUR_USD"), quote_record("GBP_USD")
    if name == "quote_id":
        two["quote_record_id"] = one["quote_record_id"]
    else:
        two = copy.deepcopy(one)
        two["quote_record_id"] = "quote-other-id"
    forward = build(r, q=envelope("venue_quotes", cutoff, [one, two]))
    reverse = build(r, q=envelope("venue_quotes", cutoff, [two, one]))
    assert forward["summary"]["admissible_quote_count"] == reverse["summary"]["admissible_quote_count"] == 0
    assert forward["decision_fingerprint"] == reverse["decision_fingerprint"]


def test_economics_id_collision_across_cells_invalidates_all_before_mapping():
    r = response(("EUR_USD", "GBP_USD")); cutoff = r["decision_cutoff_utc"]
    quotes = envelope("venue_quotes", cutoff, [quote_record("EUR_USD"), quote_record("GBP_USD")])
    forecasts = [forecast_record(r, pair) for pair in ("EUR_USD", "GBP_USD")]
    verifiers = envelope("verifier_evidence", cutoff, [verifier_record(r, f) for f in forecasts])
    accounts = envelope("account_state", cutoff, flat_account_records(("EUR_USD", "GBP_USD")))
    rows = [
        economics_record(r, forecasts[i], quotes["records"][i], verifiers["records"][i], accounts["records"])
        for i in range(2)
    ]
    rows[1]["economics_record_id"] = rows[0]["economics_record_id"]
    result = build(r, quotes, verifiers, envelope("economics_inputs", cutoff, rows), a=accounts)
    assert result["input_envelope_dispositions"]["economics_inputs"]["invalidated_whole"] is True
    assert result["summary"]["economics_admissible_count"] == 0


def test_cross_envelope_forecast_equivocation_invalidates_both_roles():
    r = response(); q, v, e, a = valid_inputs(r)
    raw = copy.deepcopy(e["records"][0]); raw.pop("canonical_record_sha256")
    raw["forecast_record_sha256"] = "f" * 64
    bad_e = envelope("economics_inputs", r["decision_cutoff_utc"], [raw])
    result = build(r, q, v, bad_e, a=a)
    assert result["input_envelope_dispositions"]["verifier_evidence"]["invalidated_whole"] is True
    assert result["input_envelope_dispositions"]["economics_inputs"]["invalidated_whole"] is True
    assert result["summary"]["admissible_verifier_count"] == 0
    assert result["summary"]["economics_admissible_count"] == 0
    assert sum(x["reason"] == "cross_envelope_forecast_id_equivocation" for x in result["v4_input_rejections"]) == 2


def test_duplicate_verifier_id_or_cell_invalidates_complete_envelope():
    r = response(); cutoff = r["decision_cutoff_utc"]
    forecast = forecast_record(r)
    one = verifier_record(r, forecast, "verify-one")
    two = verifier_record(r, forecast, "verify-two")
    result = build(r, v=envelope("verifier_evidence", cutoff, [one, two]))
    assert result["input_envelope_dispositions"]["verifier_evidence"]["invalidated_whole"] is True
    assert result["summary"]["admissible_verifier_count"] == 0


def test_duplicate_hold_identity_and_observed_price_hold_both_fail_closed():
    r = response(); cutoff = r["decision_cutoff_utc"]
    q = envelope("venue_quotes", cutoff, [quote_record()])
    forecast = forecast_record(r); v = envelope("verifier_evidence", cutoff, [verifier_record(r, forecast)])
    a = envelope("account_state", cutoff, open_account_records())
    hold = hold_record(r, forecast, q["records"][0], v["records"][0], a["records"])
    duplicate = copy.deepcopy(hold); duplicate["hold_record_id"] = "hold-2"
    result = build(r, q, v, h=envelope("hold_switch_inputs", cutoff, [hold, duplicate]), a=a)
    assert result["summary"]["hold_switch_count"] == 0
    assert result["input_envelope_dispositions"]["hold_switch_inputs"]["invalidated_whole"] is True

    observed = copy.deepcopy(forecast)
    source = find_response(r, "EUR_USD", "observed_price_only")
    observed.update({
        "arm_id": "observed_price_only", "research_direction": source["research_direction"],
        "response_record_hash": stable_hash(source),
    })
    observed_v = envelope("verifier_evidence", cutoff, [verifier_record(r, observed, "observed-v")])
    observed_hold = hold_record(r, observed, q["records"][0], observed_v["records"][0], a["records"])
    observed_hold["arm_id"] = "observed_price_only"
    result = build(r, q, observed_v, h=envelope("hold_switch_inputs", cutoff, [observed_hold]), a=a)
    assert result["summary"]["hold_switch_count"] == 0
    assert any("hold_arm_not_forward" in x["reason"] for x in result["v4_input_rejections"])


def test_hold_nested_contract_hashes_must_be_strict_sha256():
    r = response(); cutoff = r["decision_cutoff_utc"]
    q = envelope("venue_quotes", cutoff, [quote_record()])
    forecast = forecast_record(r); v = envelope("verifier_evidence", cutoff, [verifier_record(r, forecast)])
    a = envelope("account_state", cutoff, open_account_records())
    hold = hold_record(r, forecast, q["records"][0], v["records"][0], a["records"])
    hold["hold_forecast_record"]["model_contract_sha256"] = "not-a-sha"
    result = build(r, q, v, h=envelope("hold_switch_inputs", cutoff, [hold]), a=a)
    assert result["summary"]["hold_switch_count"] == 0
    assert any(x["reason"] == "hold_model_feature_magnitude_or_cost_sha256_invalid" for x in result["v4_input_rejections"])


def test_open_rotation_requires_exact_close_quote_and_recomputed_cost():
    r = response(("EUR_USD", "GBP_USD"))
    q, v, e, a = open_rotation_inputs(r, include_close_binding=False, reported_cost=1e-12)
    result = build(r, q, v, e, a=a)
    assert result["summary"]["ranked_count"] == 0
    assert any(x["reason"] == "rotation_close_exact_executable_quote_missing" for x in result["v4_input_rejections"])

    q, v, e, a = open_rotation_inputs(r, include_close_binding=True, reported_cost=1e-12)
    result = build(r, q, v, e, a=a)
    assert result["summary"]["ranked_count"] == 0
    assert any(x["reason"] == "rotation_close_cost_not_recomputed_from_executable_quote" for x in result["v4_input_rejections"])

    q, v, e, a = open_rotation_inputs(r)
    result = build(r, q, v, e, a=a)
    assert find(result, "GBP_USD")["economics_admissible"] is True
    assert result["summary"]["ranked_count"] == 1


def test_submitted_lineage_preserves_ordered_duplicate_members_without_dict_collapse():
    r = response(); cutoff = r["decision_cutoff_utc"]
    one = quote_record(); two = copy.deepcopy(one)
    q = envelope("venue_quotes", cutoff, [one, two])
    result = build(r, q=q)
    lineage = result["submitted_input_hashes"]["records"]["venue_quotes"]
    assert [row["index"] for row in lineage] == [0, 1]
    assert len(lineage) == 2
    assert result["input_envelope_dispositions"]["venue_quotes"]["invalidated_whole"] is True


def test_alternate_manifest_same_labels_pointing_to_readme_is_rejected():
    bad = manifest()
    for row in bad["artifacts"].values():
        row["relative_path"] = "README.md"
    with pytest.raises(AfterCostV4Error, match="label-to-path"):
        build(response(), m=bad)


@pytest.mark.parametrize("field,value", [
    ("proposed_units", True),
    ("expected_favorable_move_bps", "not-finite"),
    ("rotation_close_cost_bps", False),
])
def test_nonfinite_or_bool_economics_invalidates_whole_envelope(field, value):
    r = response(); q, v, e, a = valid_inputs(r)
    raw = copy.deepcopy(e["records"][0]); raw.pop("canonical_record_sha256"); raw[field] = value
    bad = envelope("economics_inputs", r["decision_cutoff_utc"], [raw])
    result = build(r, q, v, bad, a=a)
    assert result["summary"]["economics_admissible_count"] == 0
    assert result["input_envelope_dispositions"]["economics_inputs"]["invalidated_whole"] is True
