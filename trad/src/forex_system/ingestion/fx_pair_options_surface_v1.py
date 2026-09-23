"""Governed, fail-closed FX pair-options surface import contract.

This module intentionally performs no network collection.  CME publishes an
official CVOL methodology and offers end-of-day/live delivery through licensed
channels, but the project has no permitted connection.  Public visualizers are
not scraped.  The contract therefore validates point-in-time snapshots and
pair orientation while ``admit_snapshot`` remains closed until a source is
connected through a new material cohort.

CVOL's 30-day whole-surface skew is not relabeled as a 25-delta risk reversal.
Likewise, a constant-volatility expected-move proxy is never called an
event-implied move.  These distinctions are material to causal evidence.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = 1
CONTRACT_ID = "fx_pair_options_surface_v1_point_in_time_shadow_20260829"
COHORT_ID = "fx_pair_options_surface_v1_20260829a"
ACTIVATION_UTC = "2026-08-29T15:45:00Z"
MANIFEST_PATH = "config/fx_pair_options_surface_v1.json"
SUPPORTED_EXECUTION_DECISION = "no_trade"

ROOT = Path(__file__).resolve().parents[3]
UTC = dt.timezone.utc
SHA256_HEX = frozenset("0123456789abcdef")

SNAPSHOT_FIELDS = frozenset(
    {
        "schema_version",
        "source_contract_id",
        "source_cohort_id",
        "snapshot_id",
        "source_id",
        "provider",
        "access_method",
        "access_proof_id",
        "license_class",
        "raw_pair",
        "normalized_pair",
        "orientation_transform",
        "normalization_method",
        "pair_information_class",
        "source_measurement_type",
        "quote_convention",
        "delta_convention",
        "volatility_quote_convention",
        "tenor_days",
        "expiry_utc",
        "event_id",
        "published_at_utc",
        "first_seen_at_utc",
        "retrieved_at_utc",
        "effective_from_utc",
        "decision_cutoff_utc",
        "cvol_index",
        "atm_iv_annualized_pct",
        "source_up_variance_annualized_pct",
        "source_down_variance_annualized_pct",
        "source_skew_indicator",
        "source_rr_25d_vol_points",
        "up_variance_annualized_pct",
        "down_variance_annualized_pct",
        "skew_indicator",
        "convexity_indicator",
        "rr_25d_vol_points",
        "bf_25d_vol_points",
        "event_implied_move_pct",
        "event_implied_move_method",
        "raw_payload_sha256",
        "prospective_clock",
        "research_only",
        "shadow_only",
        "proof_eligible",
        "confirmation_eligible",
        "promotion_eligible",
        "authorization_eligible",
        "execution_eligible",
        "can_place_orders",
        "supported_execution_decision",
    }
)

FALSE_GUARDS = (
    "proof_eligible",
    "confirmation_eligible",
    "promotion_eligible",
    "authorization_eligible",
    "execution_eligible",
    "can_place_orders",
)

NUMERIC_FIELDS = (
    "cvol_index",
    "atm_iv_annualized_pct",
    "source_up_variance_annualized_pct",
    "source_down_variance_annualized_pct",
    "source_skew_indicator",
    "source_rr_25d_vol_points",
    "up_variance_annualized_pct",
    "down_variance_annualized_pct",
    "skew_indicator",
    "convexity_indicator",
    "rr_25d_vol_points",
    "bf_25d_vol_points",
    "event_implied_move_pct",
)


class FxPairOptionsSurfaceError(RuntimeError):
    """Raised when source, chronology, or measurement integrity fails."""


def _parse_utc(value: Any, *, label: str, nullable: bool = False) -> dt.datetime | None:
    if nullable and value is None:
        return None
    if type(value) is not str or not value:
        raise FxPairOptionsSurfaceError(f"{label}_not_timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FxPairOptionsSurfaceError(f"{label}_invalid_timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FxPairOptionsSurfaceError(f"{label}_timezone_missing")
    return parsed.astimezone(UTC)


def _number(value: Any, *, label: str, nullable: bool = True) -> float | None:
    if nullable and value is None:
        return None
    if type(value) not in {int, float}:
        raise FxPairOptionsSurfaceError(f"{label}_not_number")
    result = float(value)
    if not math.isfinite(result):
        raise FxPairOptionsSurfaceError(f"{label}_not_finite")
    return result


def _string(value: Any, *, label: str, nullable: bool = False) -> str | None:
    if nullable and value is None:
        return None
    if type(value) is not str or not value.strip():
        raise FxPairOptionsSurfaceError(f"{label}_not_string")
    return value


def _closed_mapping(value: Any, *, fields: frozenset[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FxPairOptionsSurfaceError(f"{label}_not_mapping")
    keys = set(value)
    if keys != set(fields):
        raise FxPairOptionsSurfaceError(
            f"{label}_schema_mismatch:missing={sorted(set(fields)-keys)}:"
            f"extra={sorted(keys-set(fields))}"
        )
    return value


def _sha256(value: Any, *, label: str) -> str:
    value = _string(value, label=label)
    assert value is not None
    if len(value) != 64 or any(char not in SHA256_HEX for char in value):
        raise FxPairOptionsSurfaceError(f"{label}_invalid_sha256")
    return value


def load_manifest(path: Path | None = None) -> dict[str, Any]:
    manifest_path = path or ROOT / MANIFEST_PATH
    try:
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FxPairOptionsSurfaceError("manifest_unreadable") from exc
    validate_manifest(value)
    return value


def validate_manifest(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise FxPairOptionsSurfaceError("manifest_not_mapping")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise FxPairOptionsSurfaceError("manifest_schema_version_mismatch")
    if value.get("contract_id") != CONTRACT_ID:
        raise FxPairOptionsSurfaceError("manifest_contract_id_mismatch")
    if value.get("cohort_id") != COHORT_ID:
        raise FxPairOptionsSurfaceError("manifest_cohort_id_mismatch")
    if value.get("activation_utc") != ACTIVATION_UTC:
        raise FxPairOptionsSurfaceError("manifest_activation_mismatch")
    if value.get("implementation_state") != "schema_validator_only":
        raise FxPairOptionsSurfaceError("manifest_implementation_state_mismatch")
    for field in (
        "immutable_collector_implemented",
        "append_only_ledger_implemented",
        "collection_ready",
    ):
        if value.get(field) is not False:
            raise FxPairOptionsSurfaceError(f"manifest_{field}_must_be_false")
    universe = value.get("oanda_instrument_universe")
    if not isinstance(universe, list) or len(universe) != 68 or len(set(universe)) != 68:
        raise FxPairOptionsSurfaceError("manifest_universe_not_exact_68")
    sources = value.get("sources")
    if not isinstance(sources, Mapping) or set(sources) != {
        "cme_cvol_eod",
        "permitted_direct_pair_surface",
    }:
        raise FxPairOptionsSurfaceError("manifest_sources_mismatch")
    cme = sources["cme_cvol_eod"]
    mapping = cme.get("raw_to_oanda_pair") if isinstance(cme, Mapping) else None
    if not isinstance(mapping, Mapping) or len(mapping) != 7:
        raise FxPairOptionsSurfaceError("manifest_cme_pair_map_mismatch")
    if set(mapping.values()) - set(universe):
        raise FxPairOptionsSurfaceError("manifest_cme_pair_outside_universe")
    if cme.get("collection_enabled") is not False:
        raise FxPairOptionsSurfaceError("manifest_cme_must_start_disabled")
    if cme.get("public_page_scraping_permitted") is not False:
        raise FxPairOptionsSurfaceError("manifest_public_scraping_must_be_disabled")
    direct = sources["permitted_direct_pair_surface"]
    if direct.get("collection_enabled") is not False:
        raise FxPairOptionsSurfaceError("manifest_direct_surface_must_start_disabled")
    guards = value.get("guards")
    if not isinstance(guards, Mapping):
        raise FxPairOptionsSurfaceError("manifest_guards_missing")
    if guards.get("research_only") is not True or guards.get("shadow_only") is not True:
        raise FxPairOptionsSurfaceError("manifest_shadow_guards_invalid")
    if any(guards.get(field) is not False for field in FALSE_GUARDS):
        raise FxPairOptionsSurfaceError("manifest_execution_guard_invalid")
    if guards.get("supported_execution_decision") != SUPPORTED_EXECUTION_DECISION:
        raise FxPairOptionsSurfaceError("manifest_decision_guard_invalid")


def pair_orientation(raw_pair: str, *, manifest: Mapping[str, Any] | None = None) -> dict[str, str]:
    contract = dict(manifest or load_manifest())
    mapping = contract["sources"]["cme_cvol_eod"]["raw_to_oanda_pair"]
    if raw_pair not in mapping:
        raise FxPairOptionsSurfaceError("raw_pair_not_in_cme_contract")
    normalized = mapping[raw_pair]
    return {
        "raw_pair": raw_pair,
        "normalized_pair": normalized,
        "orientation_transform": "identity" if raw_pair == normalized else "inverse",
        "normalization_method": (
            "identity" if raw_pair == normalized else "reciprocal_call_put_swap_v1"
        ),
        "pair_information_class": (
            "direct_pair_surface" if raw_pair == normalized else "inverted_direct_surface"
        ),
    }


def pair_information_coverage(*, manifest: Mapping[str, Any] | None = None) -> dict[str, str]:
    """Classify all 68 pairs without promoting two-leg proxies to surfaces."""

    contract = dict(manifest or load_manifest())
    validate_manifest(contract)
    mapping = contract["sources"]["cme_cvol_eod"]["raw_to_oanda_pair"]
    direct = {pair for raw, pair in mapping.items() if raw == pair}
    inverted = {pair for raw, pair in mapping.items() if raw != pair}
    covered_currencies = {"USD"}
    for raw_pair in mapping:
        covered_currencies.update(raw_pair.split("_"))
    result: dict[str, str] = {}
    for pair in contract["oanda_instrument_universe"]:
        if pair in direct:
            result[pair] = "direct_pair_surface"
        elif pair in inverted:
            result[pair] = "inverted_direct_surface"
        elif set(pair.split("_")).issubset(covered_currencies):
            result[pair] = "two_leg_proxy"
        else:
            result[pair] = "unavailable"
    return result


def normalize_directional_metrics(
    *,
    orientation_transform: str,
    up_variance: float | None,
    down_variance: float | None,
    skew: float | None,
    rr_25d: float | None,
) -> dict[str, float | None]:
    if orientation_transform not in {"identity", "inverse"}:
        raise FxPairOptionsSurfaceError("orientation_transform_invalid")
    values = {
        "up_variance": _number(up_variance, label="up_variance"),
        "down_variance": _number(down_variance, label="down_variance"),
        "skew": _number(skew, label="skew"),
        "rr_25d": _number(rr_25d, label="rr_25d"),
    }
    if orientation_transform == "identity":
        return values
    return {
        "up_variance": values["down_variance"],
        "down_variance": values["up_variance"],
        "skew": None if values["skew"] is None else -values["skew"],
        "rr_25d": None if values["rr_25d"] is None else -values["rr_25d"],
    }


def constant_vol_expected_move_proxy_pct(*, atm_iv_annualized_pct: float, horizon_days: float) -> float:
    """Return a square-root-of-time proxy, never an event-implied move."""

    iv = _number(atm_iv_annualized_pct, label="atm_iv_annualized_pct", nullable=False)
    days = _number(horizon_days, label="horizon_days", nullable=False)
    assert iv is not None and days is not None
    if iv <= 0 or days <= 0:
        raise FxPairOptionsSurfaceError("expected_move_inputs_must_be_positive")
    return iv * math.sqrt(days / 365.0)


def validate_snapshot(value: Any, *, manifest: Mapping[str, Any] | None = None) -> dict[str, Any]:
    row = dict(_closed_mapping(value, fields=SNAPSHOT_FIELDS, label="snapshot"))
    contract = dict(manifest or load_manifest())
    validate_manifest(contract)

    if row["schema_version"] != SCHEMA_VERSION:
        raise FxPairOptionsSurfaceError("snapshot_schema_version_mismatch")
    if row["source_contract_id"] != CONTRACT_ID or row["source_cohort_id"] != COHORT_ID:
        raise FxPairOptionsSurfaceError("snapshot_cohort_mismatch")
    for field in (
        "snapshot_id", "source_id", "provider", "access_method", "access_proof_id",
        "license_class", "raw_pair", "normalized_pair", "orientation_transform",
        "normalization_method", "pair_information_class", "source_measurement_type", "quote_convention",
        "delta_convention", "volatility_quote_convention",
    ):
        _string(row[field], label=field)
    _sha256(row["raw_payload_sha256"], label="raw_payload_sha256")
    source_id = row["source_id"]
    sources = contract["sources"]
    if source_id not in sources:
        raise FxPairOptionsSurfaceError("snapshot_source_not_contracted")
    source = sources[source_id]
    if row["provider"] != source["provider"]:
        raise FxPairOptionsSurfaceError("snapshot_provider_mismatch")
    if row["source_measurement_type"] != source["source_measurement_type"]:
        raise FxPairOptionsSurfaceError("snapshot_measurement_type_mismatch")

    published = _parse_utc(row["published_at_utc"], label="published_at_utc")
    first_seen = _parse_utc(row["first_seen_at_utc"], label="first_seen_at_utc")
    retrieved = _parse_utc(row["retrieved_at_utc"], label="retrieved_at_utc")
    effective = _parse_utc(row["effective_from_utc"], label="effective_from_utc")
    decision_cutoff = _parse_utc(row["decision_cutoff_utc"], label="decision_cutoff_utc")
    expiry = _parse_utc(row["expiry_utc"], label="expiry_utc", nullable=True)
    assert published and first_seen and retrieved and effective and decision_cutoff
    if not published <= first_seen <= retrieved:
        raise FxPairOptionsSurfaceError("snapshot_knowledge_time_order_invalid")
    if effective != retrieved:
        raise FxPairOptionsSurfaceError("snapshot_effective_time_must_equal_retrieval")
    if decision_cutoff < effective:
        raise FxPairOptionsSurfaceError("snapshot_decision_cutoff_precedes_knowledge")
    if expiry is not None and expiry <= retrieved:
        raise FxPairOptionsSurfaceError("snapshot_expiry_not_future")
    activation = _parse_utc(contract["activation_utc"], label="activation_utc")
    assert activation
    if row["prospective_clock"] is not (retrieved >= activation):
        raise FxPairOptionsSurfaceError("snapshot_prospective_clock_mismatch")

    if type(row["tenor_days"]) is not int or row["tenor_days"] <= 0:
        raise FxPairOptionsSurfaceError("snapshot_tenor_days_invalid")
    for field in NUMERIC_FIELDS:
        _number(row[field], label=field)
    if row["atm_iv_annualized_pct"] is not None and row["atm_iv_annualized_pct"] <= 0:
        raise FxPairOptionsSurfaceError("snapshot_atm_iv_not_positive")
    for field in (
        "cvol_index",
        "up_variance_annualized_pct",
        "down_variance_annualized_pct",
        "source_up_variance_annualized_pct",
        "source_down_variance_annualized_pct",
        "convexity_indicator",
    ):
        if row[field] is not None and row[field] <= 0:
            raise FxPairOptionsSurfaceError(f"snapshot_{field}_not_positive")
    if row["event_implied_move_pct"] is not None and row["event_implied_move_pct"] <= 0:
        raise FxPairOptionsSurfaceError("snapshot_event_implied_move_not_positive")

    universe = set(contract["oanda_instrument_universe"])
    if row["normalized_pair"] not in universe:
        raise FxPairOptionsSurfaceError("snapshot_pair_outside_universe")
    if row["orientation_transform"] not in {"identity", "inverse"}:
        raise FxPairOptionsSurfaceError("snapshot_orientation_invalid")
    if row["quote_convention"] != "quote_currency_per_base_currency":
        raise FxPairOptionsSurfaceError("snapshot_quote_convention_invalid")
    if row["volatility_quote_convention"] != "annualized_percent":
        raise FxPairOptionsSurfaceError("snapshot_volatility_quote_convention_invalid")

    if source_id == "cme_cvol_eod":
        expected = pair_orientation(row["raw_pair"], manifest=contract)
        if any(row[key] != expected[key] for key in expected):
            raise FxPairOptionsSurfaceError("snapshot_cme_orientation_mismatch")
        if row["delta_convention"] != "not_delta_quoted":
            raise FxPairOptionsSurfaceError("snapshot_cme_delta_convention_invalid")
        if row["tenor_days"] != 30 or row["expiry_utc"] is not None:
            raise FxPairOptionsSurfaceError("snapshot_cme_not_constant_30d")
        for field in (
            "event_id", "rr_25d_vol_points", "bf_25d_vol_points",
            "event_implied_move_pct", "event_implied_move_method",
        ):
            if row[field] is not None:
                raise FxPairOptionsSurfaceError(f"snapshot_cme_field_not_available:{field}")
        if all(row[field] is None for field in source["available_metrics"]):
            raise FxPairOptionsSurfaceError("snapshot_cme_has_no_measurement")
    else:
        if row["raw_pair"] != row["normalized_pair"] or row["orientation_transform"] != "identity":
            raise FxPairOptionsSurfaceError("snapshot_direct_surface_must_use_oanda_orientation")
        if row["normalization_method"] != "identity":
            raise FxPairOptionsSurfaceError("snapshot_direct_normalization_invalid")
        if row["pair_information_class"] != "direct_pair_surface":
            raise FxPairOptionsSurfaceError("snapshot_direct_surface_class_invalid")
        if row["delta_convention"] not in {
            "spot_delta", "forward_delta", "premium_adjusted_spot_delta",
            "premium_adjusted_forward_delta",
        }:
            raise FxPairOptionsSurfaceError("snapshot_direct_delta_convention_invalid")
        if row["atm_iv_annualized_pct"] is None:
            raise FxPairOptionsSurfaceError("snapshot_direct_surface_missing_atm")
        has_event_move = row["event_implied_move_pct"] is not None
        if has_event_move:
            if row["event_id"] is None or row["event_implied_move_method"] not in {
                "vendor_event_straddle", "expiry_stripping_pre_registered"
            } or row["expiry_utc"] is None:
                raise FxPairOptionsSurfaceError("snapshot_event_move_provenance_incomplete")
        elif row["event_implied_move_method"] is not None:
            raise FxPairOptionsSurfaceError("snapshot_event_move_method_without_value")

    normalized_directional = normalize_directional_metrics(
        orientation_transform=row["orientation_transform"],
        up_variance=row["source_up_variance_annualized_pct"],
        down_variance=row["source_down_variance_annualized_pct"],
        skew=row["source_skew_indicator"],
        rr_25d=row["source_rr_25d_vol_points"],
    )
    expected_normalized = {
        "up_variance": row["up_variance_annualized_pct"],
        "down_variance": row["down_variance_annualized_pct"],
        "skew": row["skew_indicator"],
        "rr_25d": row["rr_25d_vol_points"],
    }
    if normalized_directional != expected_normalized:
        raise FxPairOptionsSurfaceError("snapshot_directional_normalization_mismatch")

    if row["research_only"] is not True or row["shadow_only"] is not True:
        raise FxPairOptionsSurfaceError("snapshot_shadow_guards_invalid")
    if any(row[field] is not False for field in FALSE_GUARDS):
        raise FxPairOptionsSurfaceError("snapshot_execution_guard_invalid")
    if row["supported_execution_decision"] != SUPPORTED_EXECUTION_DECISION:
        raise FxPairOptionsSurfaceError("snapshot_decision_guard_invalid")
    return row


def admit_snapshot(value: Any, *, manifest: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Validate a record, then fail closed while its source is unconnected."""

    contract = dict(manifest or load_manifest())
    row = validate_snapshot(value, manifest=contract)
    source = contract["sources"][row["source_id"]]
    if source["collection_enabled"] is not True:
        raise FxPairOptionsSurfaceError(
            f"source_not_connected:{row['source_id']}:{source['access_state']}"
        )
    return row


def readiness_status(*, manifest: Mapping[str, Any] | None = None) -> dict[str, Any]:
    contract = dict(manifest or load_manifest())
    validate_manifest(contract)
    cme = contract["sources"]["cme_cvol_eod"]
    universe = contract["oanda_instrument_universe"]
    cme_pairs = sorted(set(cme["raw_to_oanda_pair"].values()))
    coverage = pair_information_coverage(manifest=contract)
    coverage_counts = {
        label: sum(value == label for value in coverage.values())
        for label in (
            "direct_pair_surface", "inverted_direct_surface", "two_leg_proxy", "unavailable"
        )
    }
    direct = contract["sources"]["permitted_direct_pair_surface"]
    manifest_bytes = (ROOT / MANIFEST_PATH).read_bytes()
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activation_utc": ACTIVATION_UTC,
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "oanda_pairs": len(universe),
        "cme_cvol_pair_proxies": len(cme_pairs),
        "cme_cvol_coverage_pct": round(100.0 * len(cme_pairs) / len(universe), 3),
        "direct_full_surface_pairs": 0,
        "pair_information_class_counts": coverage_counts,
        "sources_connected": sum(
            source["collection_enabled"] is True for source in contract["sources"].values()
        ),
        "cme_access_state": cme["access_state"],
        "direct_surface_access_state": direct["access_state"],
        "current_observations": 0,
        "implementation_state": "schema_validator_only",
        "immutable_collector_implemented": False,
        "append_only_ledger_implemented": False,
        "collection_ready": False,
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }
