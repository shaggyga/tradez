import copy
import math
import random

import numpy as np
import pytest

try:
    from forex_system.contracts.currency_state import (
        load_contract,
        stable_hash,
        validate_contract,
    )
    from forex_system.features.currency_state_engine import (
        PairObservation,
        build_currency_state_snapshot,
        pair_observations_from_history,
        solve_weighted_currency_state,
    )
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import (
        load_contract,
        stable_hash,
        validate_contract,
    )
    from src.forex_system.features.currency_state_engine import (
        PairObservation,
        build_currency_state_snapshot,
        pair_observations_from_history,
        solve_weighted_currency_state,
    )


def synthetic_history():
    contract = load_contract()
    states = {"EUR": 4.0, "GBP": 2.0, "USD": -2.0, "AUD": -3.0, "NZD": -1.0}
    instruments = (
        "EUR_USD",
        "GBP_USD",
        "EUR_GBP",
        "AUD_USD",
        "EUR_AUD",
        "GBP_AUD",
        "NZD_USD",
        "EUR_NZD",
        "GBP_NZD",
        "AUD_NZD",
    )
    start = 1767225600  # 2026-01-01T00:00:00Z
    rows = []
    for minute in range(61):
        fraction = minute / 60.0
        for instrument in instruments:
            base, quote = instrument.split("_")
            move_bps = (states[base] - states[quote]) * fraction
            mid = math.exp(move_bps / 10_000.0)
            rows.append(
                {
                    "instrument": instrument,
                    "minute_epoch": start + minute * 60,
                    "first_epoch": start + minute * 60 + 1,
                    "last_epoch": start + minute * 60 + 2,
                    "open_bid": mid - 0.00005,
                    "open_ask": mid + 0.00005,
                    "close_bid": mid - 0.00005,
                    "close_ask": mid + 0.00005,
                    "high_mid": mid,
                    "low_mid": mid,
                    "pip": 0.0001,
                }
            )
    return contract, {
        "generated_utc": "2026-01-01T01:01:00+00:00",
        "rows": rows,
    }


def observation(instrument, value, weight=1.0):
    base, quote = instrument.split("_")
    return PairObservation(
        instrument=instrument,
        base_currency=base,
        quote_currency=quote,
        return_bps=float(value),
        spread_bps=1.0 / weight,
        weight=float(weight),
        start_epoch=0,
        end_epoch=60,
        actual_observation_duration_sec=60,
        endpoint_age_sec=0.0,
        start_alignment_sec=0.0,
        bid=1.0,
        ask=1.0001,
        pip=0.0001,
    )


def replace_with_inverse_leg(contract, history, direct="EUR_USD", inverse="USD_EUR"):
    """Return a test-only universe with one correctly inverted bid/ask leg."""

    inverted_contract = copy.deepcopy(contract)
    inverted_contract["contract_id"] = "currency_state_inverse_fixture_v1"
    inverted_contract["instruments"] = [
        inverse if instrument == direct else instrument
        for instrument in inverted_contract["instruments"]
    ]
    inverted_history = copy.deepcopy(history)
    for row in inverted_history["rows"]:
        if row["instrument"] != direct:
            continue
        row["instrument"] = inverse
        for prefix in ("open", "close"):
            direct_bid = row[f"{prefix}_bid"]
            direct_ask = row[f"{prefix}_ask"]
            # A reciprocal executable quote must swap sides.  Using 1/bid as
            # the inverse bid would manufacture a crossed market.
            row[f"{prefix}_bid"] = 1.0 / direct_ask
            row[f"{prefix}_ask"] = 1.0 / direct_bid
        direct_high = row["high_mid"]
        direct_low = row["low_mid"]
        row["high_mid"] = 1.0 / direct_low
        row["low_mid"] = 1.0 / direct_high
    return inverted_contract, inverted_history


def test_contract_is_exact_connected_safe_universe():
    contract = load_contract()
    assert contract["contract_id"] == "currency_state_engine_v2_20260816"
    assert contract["identification"] == "zero_sum_primary_connected_component_only"
    assert len(contract["currencies"]) == 21
    assert len(contract["instruments"]) == 68
    assert contract["research_only"] is True
    assert contract["execution_eligible"] is False
    assert contract["can_place_orders"] is False
    assert contract["supported_execution_decision"] == "no_trade"


def test_contract_rejects_execution_eligibility():
    contract = load_contract()
    unsafe = dict(contract)
    unsafe["execution_eligible"] = True
    with pytest.raises(ValueError):
        validate_contract(unsafe)


def test_snapshot_always_emits_21_nodes_and_68_edges():
    contract, history = synthetic_history()
    snapshot = build_currency_state_snapshot(history, contract=contract)
    assert snapshot["currency_count"] == 21
    assert snapshot["instrument_count"] == 68
    assert snapshot["execution_eligible"] is False
    for payload in snapshot["horizons"].values():
        assert len(payload["currencies"]) == 21
        assert len(payload["pair_edges"]) == 68
        assert payload["currencies"]["TRY"]["state"] == "unavailable"


def test_currency_vector_is_zero_sum_and_pair_is_base_minus_quote():
    contract, history = synthetic_history()
    snapshot = build_currency_state_snapshot(history, contract=contract)
    payload = snapshot["horizons"]["3600"]
    observed = {
        currency: row["observed_currency_return_bps"]
        for currency, row in payload["currencies"].items()
        if row["observed_currency_return_bps"] is not None
    }
    assert abs(sum(observed.values())) < 1e-8
    edge = payload["pair_edges"]["EUR_USD"]
    assert edge["observed_factor_move_bps"] == pytest.approx(
        observed["EUR"] - observed["USD"], abs=1e-9
    )
    assert edge["forecast_mean_bps"] is None
    assert edge["expected_net_pips"] is None


def test_snapshot_is_ingestion_order_invariant():
    contract, history = synthetic_history()
    first = build_currency_state_snapshot(history, contract=contract, input_refs={"x": "y"})
    shuffled = copy.deepcopy(history)
    random.Random(20260816).shuffle(shuffled["rows"])
    second = build_currency_state_snapshot(shuffled, contract=contract, input_refs={"x": "y"})
    assert first["snapshot_id"] == second["snapshot_id"]
    assert first["horizons"] == second["horizons"]


def test_knowledge_cutoff_is_distinct_from_completed_bar_cutoff():
    contract, history = synthetic_history()
    snapshot = build_currency_state_snapshot(
        history,
        contract=contract,
        decision_cutoff_utc="2026-01-01T01:01:37+00:00",
    )
    assert snapshot["decision_cutoff_utc"] == "2026-01-01T01:01:37+00:00"
    assert snapshot["completed_bar_cutoff_utc"] == "2026-01-01T01:00:00+00:00"
    assert snapshot["horizons"]["60"]["decision_cutoff_utc"] == snapshot["decision_cutoff_utc"]
    assert snapshot["horizons"]["60"]["completed_bar_cutoff_utc"] == snapshot["completed_bar_cutoff_utc"]


def test_snapshot_identity_uses_content_digest_not_machine_path():
    contract, history = synthetic_history()
    first = build_currency_state_snapshot(
        history,
        contract=contract,
        input_refs={"quote_history_path": "C:/first/location.json", "quote_history_sha256": "abc"},
    )
    second = build_currency_state_snapshot(
        history,
        contract=contract,
        input_refs={"quote_history_path": "D:/restored/location.json", "quote_history_sha256": "abc"},
    )
    assert first["input_refs"] != second["input_refs"]
    assert first["input_identity"] == second["input_identity"]
    assert first["snapshot_id"] == second["snapshot_id"]


def test_approximate_horizon_emits_actual_duration_and_fails_closed_for_forecast_use():
    contract, history = synthetic_history()
    target = 1767229140
    history["rows"] = [
        row
        for row in history["rows"]
        if not (row["instrument"] == "EUR_USD" and row["minute_epoch"] == target)
    ]
    snapshot = build_currency_state_snapshot(history, contract=contract)
    edge = snapshot["horizons"]["60"]["pair_edges"]["EUR_USD"]
    assert edge["declared_horizon_sec"] == 60
    assert edge["actual_observation_duration_sec"] == 120
    assert edge["exact_horizon_observation"] is False
    assert edge["forecast_mean_bps"] is None
    assert edge["execution_eligible"] is False


def test_clipped_solver_and_raw_edge_residuals_are_separately_labeled():
    contract, history = synthetic_history()
    end_epoch = 1767229200
    for row in history["rows"]:
        if row["instrument"] == "EUR_USD" and row["minute_epoch"] == end_epoch:
            mid = math.exp(100.0 / 10_000.0)
            row["close_bid"] = mid - 0.00005
            row["close_ask"] = mid + 0.00005
    snapshot = build_currency_state_snapshot(history, contract=contract)
    edge = snapshot["horizons"]["3600"]["pair_edges"]["EUR_USD"]
    assert edge["observed_return_was_clipped"] is True
    assert edge["observed_residual_basis"] == "raw_observed_return_minus_factor"
    assert edge["observed_solver_residual_basis"] == "clipped_solver_return_minus_factor"
    assert edge["observed_residual_bps"] != edge["observed_solver_residual_bps"]


def test_frozen_synthetic_snapshot_is_reconstructible_byte_for_byte():
    contract, history = synthetic_history()
    snapshot = build_currency_state_snapshot(history, contract=contract)

    assert snapshot["snapshot_id"] == "currency_state_snapshot_90c0c4c7e95cbdde4547d98d"
    assert stable_hash(snapshot["horizons"]) == (
        "9a7aa1dd5d9b30182c600d877ddb1df54f03a31bd8a2c3ae3ee0b63a73baf062"
    )


def test_correct_bid_ask_inverse_recovers_the_same_currency_state():
    contract, history = synthetic_history()
    inverse_contract, inverse_history = replace_with_inverse_leg(contract, history)
    cutoff = 1767229200

    direct_rows, direct_rejected = pair_observations_from_history(
        history["rows"],
        horizon_sec=3600,
        cutoff_epoch=cutoff,
        contract=contract,
    )
    inverse_rows, inverse_rejected = pair_observations_from_history(
        inverse_history["rows"],
        horizon_sec=3600,
        cutoff_epoch=cutoff,
        contract=inverse_contract,
    )
    direct = {row.instrument: row for row in direct_rows}
    inverse = {row.instrument: row for row in inverse_rows}

    assert direct_rejected["missing_instrument"] == 58
    assert inverse_rejected["missing_instrument"] == 58
    assert inverse["USD_EUR"].bid < inverse["USD_EUR"].ask
    assert inverse["USD_EUR"].return_bps == pytest.approx(
        -direct["EUR_USD"].return_bps, abs=1e-6
    )
    assert inverse["USD_EUR"].spread_bps == pytest.approx(
        direct["EUR_USD"].spread_bps, abs=1e-9
    )

    direct_state = solve_weighted_currency_state(
        direct_rows,
        currencies=contract["currencies"],
        policy=contract["price_response"],
    )
    inverse_state = solve_weighted_currency_state(
        inverse_rows,
        currencies=inverse_contract["currencies"],
        policy=inverse_contract["price_response"],
    )
    assert inverse_state["strengths_bps"] == pytest.approx(
        direct_state["strengths_bps"], abs=1e-6
    )


def test_stale_endpoints_fail_closed_instead_of_becoming_zero():
    contract, history = synthetic_history()
    cutoff = 1767229200
    old_rows = [row for row in history["rows"] if row["minute_epoch"] <= cutoff - 600]
    observations, rejected = pair_observations_from_history(
        old_rows,
        horizon_sec=300,
        cutoff_epoch=cutoff,
        contract=contract,
    )
    assert observations == []
    assert rejected["stale_endpoint"] == 10


def test_missing_pair_remains_unavailable_while_latent_edge_is_diagnostic_only():
    contract, history = synthetic_history()
    history["rows"] = [
        row for row in history["rows"] if row["instrument"] != "EUR_GBP"
    ]
    snapshot = build_currency_state_snapshot(history, contract=contract)
    payload = snapshot["horizons"]["3600"]
    edge = payload["pair_edges"]["EUR_GBP"]

    assert payload["currencies"]["EUR"]["observed_currency_return_bps"] is not None
    assert payload["currencies"]["GBP"]["observed_currency_return_bps"] is not None
    assert edge["state"] == "unavailable"
    assert edge["research_observable"] is False
    assert edge["observed_pair_return_bps"] is None
    assert edge["bid"] is None and edge["ask"] is None and edge["pip"] is None
    assert edge["observed_factor_move_bps"] == pytest.approx(2.0, abs=1e-9)
    assert edge["execution_eligible"] is False
    assert edge["abstention_reasons"] == ["missing_or_stale_pair_observation"]

    unavailable = payload["currencies"]["TRY"]
    assert unavailable["observed_currency_return_bps"] is None
    assert unavailable["observed_response_uncertainty_bps"] is None
    assert all(component["state"] == "unavailable" for component in unavailable["components"])


def test_duplicate_pair_cannot_inflate_observation_count():
    contract = load_contract()
    rows = [
        observation("EUR_USD", 6),
        observation("GBP_USD", 4),
        observation("EUR_GBP", 2),
        observation("AUD_USD", -1),
        observation("EUR_AUD", 7),
        observation("GBP_AUD", 5),
    ]
    baseline = solve_weighted_currency_state(
        rows, currencies=contract["currencies"], policy=contract["price_response"]
    )
    duplicated = solve_weighted_currency_state(
        rows + [rows[0]],
        currencies=contract["currencies"],
        policy=contract["price_response"],
    )
    assert baseline["observation_count"] == duplicated["observation_count"] == 6
    assert duplicated["duplicate_observation_count"] == 1
    assert baseline["strengths_bps"] == duplicated["strengths_bps"]


def test_disconnected_component_is_explicitly_unavailable():
    contract = load_contract()
    primary = [
        observation("EUR_USD", 6),
        observation("GBP_USD", 4),
        observation("EUR_GBP", 2),
        observation("AUD_USD", -1),
        observation("EUR_AUD", 7),
        observation("GBP_AUD", 5),
    ]
    separate = [
        observation("CAD_CHF", 1),
        observation("CAD_JPY", 2),
        observation("CHF_JPY", 1),
    ]
    result = solve_weighted_currency_state(
        primary + separate,
        currencies=contract["currencies"],
        policy=contract["price_response"],
    )
    assert result["status"] == "degraded_disconnected"
    assert set(result["active_currencies"]) == {"AUD", "EUR", "GBP", "USD"}
    assert {"CAD", "CHF", "JPY"}.issubset(result["unavailable_currencies"])


def test_contradictory_triangle_increases_visible_uncertainty():
    contract = load_contract()
    exact = [
        observation("EUR_USD", 6),
        observation("GBP_USD", 4),
        observation("EUR_GBP", 2),
        observation("AUD_USD", -1),
        observation("EUR_AUD", 7),
        observation("GBP_AUD", 5),
    ]
    noisy = list(exact)
    noisy[2] = observation("EUR_GBP", 12)
    first = solve_weighted_currency_state(
        exact, currencies=contract["currencies"], policy=contract["price_response"]
    )
    second = solve_weighted_currency_state(
        noisy, currencies=contract["currencies"], policy=contract["price_response"]
    )
    assert second["weighted_residual_sigma_bps"] > first["weighted_residual_sigma_bps"]
    assert np.mean(list(second["uncertainty_bps"].values())) > np.mean(
        list(first["uncertainty_bps"].values())
    )


def test_leave_one_edge_out_diagnostic_does_not_self_confirm():
    contract, history = synthetic_history()
    snapshot = build_currency_state_snapshot(history, contract=contract)
    edge = snapshot["horizons"]["3600"]["pair_edges"]["EUR_USD"]
    assert edge["leave_one_edge_out_factor_bps"] is not None
    assert edge["self_influence_bps"] is not None
    assert edge["execution_eligible"] is False


def test_covariance_is_symmetric_and_edge_variance_is_nonnegative():
    contract, history = synthetic_history()
    snapshot = build_currency_state_snapshot(history, contract=contract)
    solver = snapshot["horizons"]["3600"]["solver"]
    covariance = np.asarray(solver["covariance_bps2"])
    np.testing.assert_allclose(covariance, covariance.T, atol=1e-12)
    eigenvalues = np.linalg.eigvalsh(covariance)
    assert float(np.min(eigenvalues)) >= -1e-10
    edge = snapshot["horizons"]["3600"]["pair_edges"]["EUR_USD"]
    edge_uncertainty = edge["observed_edge_uncertainty_bps"]
    assert edge_uncertainty >= 0

    currencies = solver["covariance_currencies"]
    base_index = currencies.index("EUR")
    quote_index = currencies.index("USD")
    expected_variance = (
        covariance[base_index, base_index]
        + covariance[quote_index, quote_index]
        - 2.0 * covariance[base_index, quote_index]
    )
    assert edge_uncertainty**2 == pytest.approx(expected_variance, abs=1e-12)
    inverse_variance = (
        covariance[quote_index, quote_index]
        + covariance[base_index, base_index]
        - 2.0 * covariance[quote_index, base_index]
    )
    assert inverse_variance == pytest.approx(expected_variance, abs=1e-12)
