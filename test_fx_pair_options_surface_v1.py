from __future__ import annotations

import copy
import math

import pytest

from src.forex_system.ingestion import fx_pair_options_surface_v1 as options


def cme_snapshot(**updates):
    row = {
        "schema_version": 1,
        "source_contract_id": options.CONTRACT_ID,
        "source_cohort_id": options.COHORT_ID,
        "snapshot_id": "cme:EUVL:20260829",
        "source_id": "cme_cvol_eod",
        "provider": "CME Group",
        "access_method": "licensed_rest_api",
        "access_proof_id": "future-permitted-connection-id",
        "license_class": "licensed_internal_research",
        "raw_pair": "EUR_USD",
        "normalized_pair": "EUR_USD",
        "orientation_transform": "identity",
        "normalization_method": "identity",
        "pair_information_class": "direct_pair_surface",
        "source_measurement_type": "cme_cvol_30d",
        "quote_convention": "quote_currency_per_base_currency",
        "delta_convention": "not_delta_quoted",
        "volatility_quote_convention": "annualized_percent",
        "tenor_days": 30,
        "expiry_utc": None,
        "event_id": None,
        "published_at_utc": "2026-08-29T15:45:00Z",
        "first_seen_at_utc": "2026-08-29T15:45:01Z",
        "retrieved_at_utc": "2026-08-29T15:45:02Z",
        "effective_from_utc": "2026-08-29T15:45:02Z",
        "decision_cutoff_utc": "2026-08-29T15:45:02Z",
        "cvol_index": 8.25,
        "atm_iv_annualized_pct": 7.9,
        "source_up_variance_annualized_pct": 8.0,
        "source_down_variance_annualized_pct": 8.5,
        "source_skew_indicator": -0.5,
        "source_rr_25d_vol_points": None,
        "up_variance_annualized_pct": 8.0,
        "down_variance_annualized_pct": 8.5,
        "skew_indicator": -0.5,
        "convexity_indicator": 1.04,
        "rr_25d_vol_points": None,
        "bf_25d_vol_points": None,
        "event_implied_move_pct": None,
        "event_implied_move_method": None,
        "raw_payload_sha256": "a" * 64,
        "prospective_clock": True,
        "research_only": True,
        "shadow_only": True,
        "proof_eligible": False,
        "confirmation_eligible": False,
        "promotion_eligible": False,
        "authorization_eligible": False,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }
    row.update(updates)
    return row


def direct_snapshot(**updates):
    row = cme_snapshot(
        snapshot_id="direct:EUR_GBP:20260829T154502Z",
        source_id="permitted_direct_pair_surface",
        provider="UNCONNECTED",
        source_measurement_type="direct_pair_surface",
        delta_convention="premium_adjusted_spot_delta",
        raw_pair="EUR_GBP",
        normalized_pair="EUR_GBP",
        tenor_days=7,
        cvol_index=None,
        up_variance_annualized_pct=None,
        down_variance_annualized_pct=None,
        skew_indicator=None,
        convexity_indicator=None,
        source_up_variance_annualized_pct=None,
        source_down_variance_annualized_pct=None,
        source_skew_indicator=None,
        source_rr_25d_vol_points=-0.35,
        rr_25d_vol_points=-0.35,
        bf_25d_vol_points=0.12,
    )
    row.update(updates)
    return row


def test_contract_is_exact_68_and_all_sources_start_fail_closed() -> None:
    manifest = options.load_manifest()
    status = options.readiness_status(manifest=manifest)
    assert len(manifest["oanda_instrument_universe"]) == 68
    assert status["oanda_pairs"] == 68
    assert status["cme_cvol_pair_proxies"] == 7
    assert status["cme_cvol_coverage_pct"] == pytest.approx(10.294)
    assert status["direct_full_surface_pairs"] == 0
    assert sum(status["pair_information_class_counts"].values()) == 68
    assert status["pair_information_class_counts"]["direct_pair_surface"] == 3
    assert status["pair_information_class_counts"]["inverted_direct_surface"] == 4
    assert status["pair_information_class_counts"]["two_leg_proxy"] > 0
    assert status["pair_information_class_counts"]["unavailable"] > 0
    assert status["sources_connected"] == 0
    assert status["current_observations"] == 0
    assert status["implementation_state"] == "schema_validator_only"
    assert status["immutable_collector_implemented"] is False
    assert status["append_only_ledger_implemented"] is False
    assert status["collection_ready"] is False
    assert status["execution_eligible"] is False


@pytest.mark.parametrize(
    ("raw_pair", "normalized_pair", "transform"),
    [
        ("EUR_USD", "EUR_USD", "identity"),
        ("JPY_USD", "USD_JPY", "inverse"),
        ("CAD_USD", "USD_CAD", "inverse"),
        ("CHF_USD", "USD_CHF", "inverse"),
        ("MXN_USD", "USD_MXN", "inverse"),
    ],
)
def test_cme_pair_orientation_is_explicit(raw_pair, normalized_pair, transform) -> None:
    assert options.pair_orientation(raw_pair) == {
        "raw_pair": raw_pair,
        "normalized_pair": normalized_pair,
        "orientation_transform": transform,
        "normalization_method": (
            "identity" if transform == "identity" else "reciprocal_call_put_swap_v1"
        ),
        "pair_information_class": (
            "direct_pair_surface" if transform == "identity" else "inverted_direct_surface"
        ),
    }


def test_inverse_orientation_swaps_variance_and_flips_directional_skew() -> None:
    value = options.normalize_directional_metrics(
        orientation_transform="inverse",
        up_variance=9.0,
        down_variance=12.0,
        skew=-3.0,
        rr_25d=-0.6,
    )
    assert value == {
        "up_variance": 12.0,
        "down_variance": 9.0,
        "skew": 3.0,
        "rr_25d": 0.6,
    }


def test_inverse_snapshot_requires_proved_oanda_oriented_directional_values() -> None:
    row = cme_snapshot(
        snapshot_id="cme:JPVL:20260829",
        raw_pair="JPY_USD",
        normalized_pair="USD_JPY",
        orientation_transform="inverse",
        normalization_method="reciprocal_call_put_swap_v1",
        pair_information_class="inverted_direct_surface",
        source_up_variance_annualized_pct=8.0,
        source_down_variance_annualized_pct=8.5,
        source_skew_indicator=-0.5,
        up_variance_annualized_pct=8.5,
        down_variance_annualized_pct=8.0,
        skew_indicator=0.5,
    )
    assert options.validate_snapshot(row) == row
    with pytest.raises(options.FxPairOptionsSurfaceError, match="normalization_mismatch"):
        options.validate_snapshot(dict(row, skew_indicator=-0.5))


def test_cross_pair_two_leg_proxy_is_not_mislabeled_direct_surface() -> None:
    coverage = options.pair_information_coverage()
    assert coverage["EUR_JPY"] == "two_leg_proxy"
    assert coverage["AUD_CAD"] == "two_leg_proxy"
    assert coverage["NZD_JPY"] == "unavailable"


def test_cme_snapshot_validates_but_cannot_be_admitted_without_access() -> None:
    row = cme_snapshot()
    assert options.validate_snapshot(row) == row
    with pytest.raises(options.FxPairOptionsSurfaceError, match="source_not_connected"):
        options.admit_snapshot(row)


def test_cme_30d_skew_cannot_masquerade_as_25d_or_event_move() -> None:
    with pytest.raises(options.FxPairOptionsSurfaceError, match="field_not_available"):
        options.validate_snapshot(cme_snapshot(rr_25d_vol_points=-0.4))
    with pytest.raises(options.FxPairOptionsSurfaceError, match="field_not_available"):
        options.validate_snapshot(
            cme_snapshot(
                event_id="fomc-20260916",
                event_implied_move_pct=0.7,
                event_implied_move_method="vendor_event_straddle",
            )
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("cvol_index", 0.0),
        ("cvol_index", -0.1),
        ("up_variance_annualized_pct", 0.0),
        ("down_variance_annualized_pct", -1.0),
        ("source_up_variance_annualized_pct", 0.0),
        ("source_down_variance_annualized_pct", -1.0),
        ("convexity_indicator", 0.0),
    ],
)
def test_nonpositive_cme_variance_family_metrics_are_rejected(field, value) -> None:
    with pytest.raises(options.FxPairOptionsSurfaceError, match="not_positive"):
        options.validate_snapshot(cme_snapshot(**{field: value}))


def test_direct_pair_surface_supports_pair_tenor_rr_bf_and_proven_event_move() -> None:
    row = direct_snapshot(
        expiry_utc="2026-09-05T15:45:02Z",
        event_id="ecb-20260903",
        event_implied_move_pct=0.65,
        event_implied_move_method="vendor_event_straddle",
    )
    assert options.validate_snapshot(row) == row
    with pytest.raises(options.FxPairOptionsSurfaceError, match="source_not_connected"):
        options.admit_snapshot(row)


def test_event_move_requires_event_expiry_and_declared_method() -> None:
    with pytest.raises(options.FxPairOptionsSurfaceError, match="provenance_incomplete"):
        options.validate_snapshot(direct_snapshot(event_implied_move_pct=0.5))


@pytest.mark.parametrize("value", [0.0, -0.25])
def test_nonpositive_event_implied_move_is_rejected(value) -> None:
    with pytest.raises(options.FxPairOptionsSurfaceError, match="event_implied_move_not_positive"):
        options.validate_snapshot(
            direct_snapshot(
                expiry_utc="2026-09-05T15:45:02Z",
                event_id="ecb-20260903",
                event_implied_move_pct=value,
                event_implied_move_method="vendor_event_straddle",
            )
        )


@pytest.mark.parametrize("value", [0.0, -0.01])
def test_nonpositive_atm_iv_is_rejected_for_direct_surface(value) -> None:
    with pytest.raises(options.FxPairOptionsSurfaceError, match="atm_iv_not_positive"):
        options.validate_snapshot(direct_snapshot(atm_iv_annualized_pct=value))


def test_signed_skew_rr_and_butterfly_are_not_given_invented_bounds() -> None:
    row = direct_snapshot(
        source_rr_25d_vol_points=-2.5,
        rr_25d_vol_points=-2.5,
        bf_25d_vol_points=-0.1,
    )
    assert options.validate_snapshot(row) == row


def test_constant_vol_expected_move_is_only_a_named_proxy() -> None:
    result = options.constant_vol_expected_move_proxy_pct(
        atm_iv_annualized_pct=10.0,
        horizon_days=30.0,
    )
    assert result == pytest.approx(10.0 * math.sqrt(30.0 / 365.0))


def test_knowledge_time_and_prospective_clock_fail_closed() -> None:
    with pytest.raises(options.FxPairOptionsSurfaceError, match="knowledge_time_order"):
        options.validate_snapshot(
            cme_snapshot(first_seen_at_utc="2026-08-29T15:44:59Z")
        )
    with pytest.raises(options.FxPairOptionsSurfaceError, match="prospective_clock"):
        options.validate_snapshot(cme_snapshot(prospective_clock=False))


def test_schema_rejects_direction_and_execution_self_certification() -> None:
    row = cme_snapshot()
    row["direction"] = "buy"
    with pytest.raises(options.FxPairOptionsSurfaceError, match="schema_mismatch"):
        options.validate_snapshot(row)
    with pytest.raises(options.FxPairOptionsSurfaceError, match="execution_guard"):
        options.validate_snapshot(cme_snapshot(execution_eligible=True))


def test_manifest_cannot_be_locally_switched_on_without_new_contract() -> None:
    manifest = options.load_manifest()
    altered = copy.deepcopy(manifest)
    altered["sources"]["cme_cvol_eod"]["collection_enabled"] = True
    with pytest.raises(options.FxPairOptionsSurfaceError, match="must_start_disabled"):
        options.validate_manifest(altered)


@pytest.mark.parametrize(
    "field",
    ["immutable_collector_implemented", "append_only_ledger_implemented", "collection_ready"],
)
def test_manifest_cannot_claim_unimplemented_collection_capability(field) -> None:
    manifest = options.load_manifest()
    altered = copy.deepcopy(manifest)
    altered[field] = True
    with pytest.raises(options.FxPairOptionsSurfaceError, match="must_be_false"):
        options.validate_manifest(altered)
