"""Governed v2 after-cost counterfactuals for CurrencyState response arms.

Pure research code: no filesystem, database, broker, lifecycle, authorization,
supervisor, or execution imports. Missing immutable quote, verifier, response,
fact-clock, cost, account, or portfolio evidence yields null EV and no_trade.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from ..contracts.currency_state import stable_hash
from ..contracts.signed_currency_exposure import canonical_factor_ids, split_currency_pair
from ..features.currency_state_engine import parse_epoch


UTC = dt.timezone.utc
SHA = re.compile(r"^[0-9a-f]{64}$")


class AfterCostV2Error(ValueError):
    pass


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _utc(value: Any, field: str) -> str:
    epoch = parse_epoch(value)
    if epoch is None:
        raise AfterCostV2Error(f"{field} must be a timestamp")
    return dt.datetime.fromtimestamp(epoch, tz=UTC).isoformat()


def _same_time(left: Any, right: Any) -> bool:
    a, b = parse_epoch(left), parse_epoch(right)
    return a is not None and b is not None and math.isclose(a, b, abs_tol=1e-6)


def _hash(value: Any) -> str:
    text = str(value or "").lower()
    if not SHA.fullmatch(text):
        raise AfterCostV2Error("required SHA-256 is missing or malformed")
    return text


def _strings(value: Any, field: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise AfterCostV2Error(f"{field} must be a sequence")
    return sorted({str(item) for item in value if str(item)})


def _universe_hash(instruments: Sequence[str]) -> str:
    raw = json.dumps(list(instruments), separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(raw).hexdigest()


def validate_contract(contract: Mapping[str, Any]) -> None:
    required = {
        "contract_id", "counterfactual_cohort_id", "required_frozen_manifest_id",
        "snapshot_schema", "required_response_contract_id",
        "required_response_snapshot_schema", "required_state_contract_id",
        "required_basis_eligibility_contract_id", "expected_instrument_count",
        "expected_horizons_sec", "expected_instruments", "instrument_universe_sha256",
        "expected_arm_ids", "forward_admissible_arm_ids", "never_forward_arm_ids",
        "required_manifest_artifacts", "quote_contract", "verifier_contract",
        "economics_contract", "selection", "hold_switch",
    }
    missing = sorted(required - set(contract))
    if missing:
        raise AfterCostV2Error(f"contract missing keys: {missing}")
    if contract.get("research_only") is not True or contract.get("execution_eligible") is not False:
        raise AfterCostV2Error("contract must remain research-only")
    if contract.get("can_place_orders") is not False or contract.get("supported_execution_decision") != "no_trade":
        raise AfterCostV2Error("contract crossed execution boundary")
    if contract.get("material_logic_cost_schema_or_manifest_change_requires_new_cohort") is not True:
        raise AfterCostV2Error("material changes must require a new cohort")
    instruments = [str(value) for value in contract["expected_instruments"]]
    if len(instruments) != 68 or len(set(instruments)) != 68:
        raise AfterCostV2Error("instrument universe must be exactly 68 unique pairs")
    if _universe_hash(instruments) != contract["instrument_universe_sha256"]:
        raise AfterCostV2Error("instrument universe hash mismatch")
    forward = set(contract["forward_admissible_arm_ids"])
    never = set(contract["never_forward_arm_ids"])
    if set(contract["expected_arm_ids"]) != forward | never or forward & never:
        raise AfterCostV2Error("arm eligibility must exactly partition the arm set")
    if "observed_price_only" not in never:
        raise AfterCostV2Error("observed-price arm must be never-forward")


def _validate_manifest(manifest: Mapping[str, Any], contract: Mapping[str, Any]) -> dict[str, Any]:
    if manifest.get("manifest_id") != contract["required_frozen_manifest_id"]:
        raise AfterCostV2Error("frozen manifest id mismatch")
    if manifest.get("contract_id") != contract["contract_id"]:
        raise AfterCostV2Error("manifest contract id mismatch")
    if manifest.get("cohort_id") != contract["counterfactual_cohort_id"]:
        raise AfterCostV2Error("manifest cohort id mismatch")
    artifacts = manifest.get("artifacts") or {}
    required = set(contract["required_manifest_artifacts"])
    if set(artifacts) != required:
        raise AfterCostV2Error("manifest artifact set is not exact")
    normalized = {}
    for label in sorted(required):
        row = artifacts[label]
        path = str(row.get("relative_path") or "")
        if not path or PathLikeUnsafe(path):
            raise AfterCostV2Error(f"manifest path invalid for {label}")
        normalized[label] = {"relative_path": path, "sha256": _hash(row.get("sha256"))}
    payload = {
        "manifest_id": manifest["manifest_id"], "contract_id": manifest["contract_id"],
        "cohort_id": manifest["cohort_id"], "artifacts": normalized,
    }
    payload["manifest_sha256"] = stable_hash(payload)
    return payload


def PathLikeUnsafe(value: str) -> bool:
    normalized = value.replace("\\", "/")
    return normalized.startswith("/") or ":" in normalized or ".." in normalized.split("/")


def _envelope(payload: Mapping[str, Any] | None, name: str, cutoff: str) -> tuple[dict[str, Any] | None, list[Mapping[str, Any]]]:
    if payload is None:
        return None, []
    if not isinstance(payload, Mapping):
        raise AfterCostV2Error(f"{name} must be a mapping")
    for field in ("contract_id", "contract_sha256", "cohort_id", "cohort_sha256", "frozen_at_utc", "decision_cutoff_utc"):
        if not str(payload.get(field) or ""):
            raise AfterCostV2Error(f"{name}.{field} required")
    _hash(payload["contract_sha256"]); _hash(payload["cohort_sha256"])
    if not _same_time(payload["decision_cutoff_utc"], cutoff):
        raise AfterCostV2Error(f"{name} cutoff mismatch")
    frozen = parse_epoch(payload["frozen_at_utc"])
    if frozen is None or frozen > parse_epoch(cutoff):
        raise AfterCostV2Error(f"{name} frozen after cutoff")
    if payload.get("research_only") is not True or payload.get("execution_eligible") is not False:
        raise AfterCostV2Error(f"{name} is not research-only")
    if payload.get("can_place_orders", False) is not False or payload.get("supported_execution_decision") != "no_trade":
        raise AfterCostV2Error(f"{name} crossed execution boundary")
    records = payload.get("records") or []
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)) or any(not isinstance(r, Mapping) for r in records):
        raise AfterCostV2Error(f"{name}.records invalid")
    identity = {
        "contract_id": str(payload["contract_id"]), "contract_sha256": _hash(payload["contract_sha256"]),
        "cohort_id": str(payload["cohort_id"]), "cohort_sha256": _hash(payload["cohort_sha256"]),
        "frozen_at_utc": _utc(payload["frozen_at_utc"], f"{name}.frozen_at_utc"),
        "payload_id": str(payload.get("payload_id") or ""),
    }
    return identity, list(records)


def _response_grid(snapshot: Mapping[str, Any], contract: Mapping[str, Any]) -> tuple[str, str, dict[tuple[str, str, int], Mapping[str, Any]]]:
    if snapshot.get("snapshot_schema") != contract["required_response_snapshot_schema"] or snapshot.get("arm_contract_id") != contract["required_response_contract_id"]:
        raise AfterCostV2Error("response contract/schema mismatch")
    if snapshot.get("input_contract_id") != contract["required_state_contract_id"]:
        raise AfterCostV2Error("CurrencyState contract mismatch")
    if snapshot.get("research_only") is not True or snapshot.get("execution_eligible") is not False or snapshot.get("can_place_orders") is not False:
        raise AfterCostV2Error("response snapshot is not research-only")
    cutoff = _utc(snapshot.get("decision_cutoff_utc"), "decision_cutoff_utc")
    arms = snapshot.get("arms") or {}
    if set(arms) != set(contract["expected_arm_ids"]):
        raise AfterCostV2Error("response arm set mismatch")
    horizons = sorted(int(x) for x in snapshot.get("horizons_sec") or [])
    if horizons != sorted(int(x) for x in contract["expected_horizons_sec"]):
        raise AfterCostV2Error("response horizon set mismatch")
    universe = set(contract["expected_instruments"])
    expected_grid = {(instrument, horizon) for instrument in universe for horizon in horizons}
    index = {}
    for arm_id in contract["expected_arm_ids"]:
        records = (arms[arm_id] or {}).get("records") or []
        actual = set()
        for row in records:
            instrument, horizon = str(row.get("instrument") or ""), int(row.get("horizon_sec") or 0)
            key = (instrument, horizon)
            if key in actual:
                raise AfterCostV2Error(f"duplicate response key in {arm_id}")
            actual.add(key); split_currency_pair(instrument)
            if row.get("execution_eligible") is not False or row.get("can_place_orders") is not False:
                raise AfterCostV2Error("response record crossed execution boundary")
            index[(arm_id, instrument, horizon)] = row
        if actual != expected_grid:
            raise AfterCostV2Error(f"response grid mismatch in {arm_id}")
    response_sha = stable_hash(snapshot)
    return cutoff, response_sha, index


def _reject(target: list[dict[str, Any]], kind: str, record_id: str, key: tuple[str, str, int] | None, reason: str) -> None:
    arm, instrument, horizon = key or ("", "", 0)
    target.append({"kind": kind, "record_id": record_id, "arm_id": arm, "instrument": instrument, "horizon_sec": horizon, "reason": reason})


def _quotes(records: Sequence[Mapping[str, Any]], identity: Mapping[str, Any] | None, cutoff: str, contract: Mapping[str, Any]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    result, rejected, seen = {}, [], set()
    frozen = parse_epoch(None if identity is None else identity["frozen_at_utc"])
    cutoff_epoch = parse_epoch(cutoff); policy = contract["quote_contract"]
    for raw in records:
        record_id, instrument = str(raw.get("quote_record_id") or ""), str(raw.get("instrument") or "")
        key = ("", instrument, 0)
        if instrument in seen:
            raise AfterCostV2Error(f"duplicate venue quote for {instrument}")
        seen.add(instrument)
        def no(reason: str) -> None: _reject(rejected, "venue_quote", record_id, key, reason)
        if instrument not in set(contract["expected_instruments"]): no("quote_outside_universe"); continue
        if not record_id: no("missing_quote_record_id"); continue
        if raw.get("venue") != policy["venue"] or raw.get("environment") != policy["environment"] or raw.get("account_id") != policy["account_id"]:
            no("quote_venue_environment_or_account_mismatch"); continue
        clocks = {name: parse_epoch(raw.get(name)) for name in ("quote_time_utc", "broker_time_utc", "observed_at_utc", "retrieved_at_utc")}
        if any(value is None for value in clocks.values()): no("missing_quote_clock"); continue
        if not (clocks["quote_time_utc"] <= clocks["observed_at_utc"] <= clocks["retrieved_at_utc"] <= frozen <= cutoff_epoch):
            no("invalid_quote_clock_order"); continue
        if clocks["broker_time_utc"] > clocks["observed_at_utc"]:
            no("broker_clock_after_observation"); continue
        if abs(clocks["broker_time_utc"] - clocks["quote_time_utc"]) > float(policy["maximum_broker_quote_clock_delta_sec"]):
            no("broker_quote_clock_delta_exceeded"); continue
        if cutoff_epoch - clocks["quote_time_utc"] > float(policy["maximum_quote_age_sec"]): no("stale_venue_quote"); continue
        if clocks["retrieved_at_utc"] - clocks["quote_time_utc"] > float(policy["maximum_transport_lag_sec"]): no("quote_transport_lag_exceeded"); continue
        try: payload_hash = _hash(raw.get("immutable_payload_sha256"))
        except AfterCostV2Error: no("missing_immutable_quote_payload_hash"); continue
        bid, ask, pip = _finite(raw.get("bid")), _finite(raw.get("ask")), _finite(raw.get("pip"))
        if bid is None or ask is None or pip is None or bid <= 0 or ask < bid or pip <= 0: no("invalid_executable_quote"); continue
        mid = (bid + ask) / 2.0; full = (ask - bid) / mid * 10_000.0
        result[instrument] = {
            "quote_record_id": record_id, "quote_record_hash": stable_hash(raw),
            "immutable_payload_sha256": payload_hash, "bid": bid, "ask": ask, "pip": pip,
            "mid": mid, "full_spread_bps": full, "entry_half_spread_bps": full / 2.0,
            "quote_time_utc": _utc(raw["quote_time_utc"], "quote_time_utc"),
            "broker_time_utc": _utc(raw["broker_time_utc"], "broker_time_utc"),
            "observed_at_utc": _utc(raw["observed_at_utc"], "observed_at_utc"),
            "retrieved_at_utc": _utc(raw["retrieved_at_utc"], "retrieved_at_utc"),
        }
    return result, sorted(rejected, key=lambda r: (r["instrument"], r["record_id"], r["reason"]))


def _verifier(records: Sequence[Mapping[str, Any]], identity: Mapping[str, Any] | None, cutoff: str, response_id: str, response_sha: str, contract: Mapping[str, Any], manifest: Mapping[str, Any]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    result, rejected, seen = {}, [], set(); cutoff_epoch = parse_epoch(cutoff)
    frozen = parse_epoch(None if identity is None else identity["frozen_at_utc"]); policy = contract["verifier_contract"]
    for raw in records:
        record_id = str(raw.get("verifier_evidence_id") or "")
        def no(reason: str) -> None: _reject(rejected, "verifier", record_id, None, reason)
        if not record_id: no("missing_verifier_evidence_id"); continue
        if record_id in seen: raise AfterCostV2Error(f"duplicate verifier evidence {record_id}")
        seen.add(record_id)
        if raw.get("response_snapshot_id") != response_id or raw.get("response_snapshot_sha256") != response_sha:
            no("verifier_response_snapshot_binding_mismatch"); continue
        known, data_cutoff = parse_epoch(raw.get("verified_at_utc")), parse_epoch(raw.get("calibration_data_cutoff_utc"))
        if known is None or data_cutoff is None or data_cutoff > known or known > frozen or known > cutoff_epoch:
            no("invalid_verifier_knowledge_clock"); continue
        if str(raw.get("calibration_state") or "") not in policy["allowed_calibration_states"]:
            no("verifier_calibration_state_not_allowed"); continue
        effective_n = _finite(raw.get("effective_n"))
        if effective_n is None or effective_n < float(policy["minimum_effective_n"]): no("verifier_effective_n_below_floor"); continue
        pdir, pclear = _finite(raw.get("calibrated_direction_probability")), _finite(raw.get("calibrated_cost_clear_probability"))
        if pdir is None or pclear is None or not (0.5 <= pdir <= 1 and 0 <= pclear <= 1): no("invalid_verifier_probability"); continue
        hashes = {}
        try:
            for field in policy["required_hash_fields"]: hashes[field] = _hash(raw.get(field))
            verifier_contract_sha = _hash(raw.get("verifier_contract_sha256"))
        except AfterCostV2Error: no("missing_verifier_evidence_hash"); continue
        if hashes["verifier_module_sha256"] != manifest["artifacts"]["independent_verifier_producer"]["sha256"]:
            no("verifier_module_not_frozen_manifest_producer"); continue
        if not str(raw.get("verifier_contract_id") or ""):
            no("missing_verifier_contract_id"); continue
        result[record_id] = {
            "verifier_evidence_id": record_id, "verifier_evidence_hash": stable_hash(raw),
            "forecast_id": str(raw.get("forecast_id") or ""), "effective_n": effective_n,
            "calibrated_direction_probability": pdir, "calibrated_cost_clear_probability": pclear,
            "calibration_state": raw["calibration_state"], "verified_at_utc": _utc(raw["verified_at_utc"], "verified_at_utc"),
            "calibration_data_cutoff_utc": _utc(raw["calibration_data_cutoff_utc"], "calibration_data_cutoff_utc"),
            "verifier_contract_id": str(raw["verifier_contract_id"]), "verifier_contract_sha256": verifier_contract_sha,
            **hashes,
        }
    return result, sorted(rejected, key=lambda r: (r["record_id"], r["reason"]))


def _basis_and_clocks(row: Mapping[str, Any], contract: Mapping[str, Any]) -> tuple[list[str], list[str], list[str], float | None, str | None]:
    official = ((row.get("lineage") or {}).get("official_thesis") or {})
    if official.get("basis_eligible_for_after_cost") is not True or official.get("proof_eligible_lineage") is not True:
        return [], [], [], None, "response_basis_or_proof_marker_missing"
    if official.get("basis_eligibility_contract_id") != contract["required_basis_eligibility_contract_id"]:
        return [], [], [], None, "response_basis_contract_mismatch"
    sources = _strings(official.get("source_ids"), "official.source_ids")
    facts = _strings(official.get("source_fact_ids"), "official.source_fact_ids")
    factors = _strings(official.get("independent_factor_ids"), "official.independent_factor_ids")
    clocks = official.get("cited_fact_clocks") or []
    if not isinstance(clocks, Sequence) or isinstance(clocks, (str, bytes)):
        return sources, facts, factors, None, "cited_fact_clocks_missing"
    clock_ids, epochs = set(), []
    for fact in clocks:
        if not isinstance(fact, Mapping): return sources, facts, factors, None, "cited_fact_clock_invalid"
        clock_ids.add(str(fact.get("fact_id") or "")); epoch = parse_epoch(fact.get("known_at_utc"))
        try: _hash(fact.get("raw_payload_sha256"))
        except AfterCostV2Error: return sources, facts, factors, None, "cited_fact_payload_hash_missing"
        if epoch is None: return sources, facts, factors, None, "cited_fact_clock_invalid"
        epochs.append(epoch)
    if set(facts) != clock_ids or not facts:
        return sources, facts, factors, None, "cited_fact_clock_set_mismatch"
    thesis_epochs = [parse_epoch(official.get("known_at_utc")), parse_epoch(official.get("semantic_known_at_utc"))]
    if any(x is None for x in thesis_epochs): return sources, facts, factors, None, "thesis_clock_missing"
    return sources, facts, factors, max(epochs + thesis_epochs), None


def _economics(records: Sequence[Mapping[str, Any]], identity: Mapping[str, Any] | None, cutoff: str, response_id: str, response_sha: str, response_index: Mapping[tuple[str, str, int], Mapping[str, Any]], quotes: Mapping[str, Mapping[str, Any]], verified: Mapping[str, Mapping[str, Any]], contract: Mapping[str, Any]) -> tuple[dict[tuple[str, str, int], dict[str, Any]], list[dict[str, Any]]]:
    result, rejected, seen = {}, [], set(); cutoff_epoch = parse_epoch(cutoff)
    frozen = parse_epoch(None if identity is None else identity["frozen_at_utc"])
    forward, policy = set(contract["forward_admissible_arm_ids"]), contract["economics_contract"]
    for raw in records:
        record_id = str(raw.get("economics_record_id") or ""); arm = str(raw.get("arm_id") or "")
        instrument = str(raw.get("instrument") or "")
        try: horizon = int(raw.get("horizon_sec"))
        except (TypeError, ValueError): horizon = 0
        key = (arm, instrument, horizon)
        def no(reason: str) -> None: _reject(rejected, "economics", record_id, key, reason)
        if key in seen: raise AfterCostV2Error(f"duplicate economics record {key}")
        seen.add(key)
        row = response_index.get(key)
        if not record_id: no("missing_economics_record_id"); continue
        if row is None: no("outside_response_grid"); continue
        if arm not in forward: no("arm_not_forward_admissible"); continue
        if row.get("paper_candidate") is not True or row.get("forward_hypothesis") is not True: no("response_arm_has_no_forward_candidate"); continue
        if raw.get("response_snapshot_id") != response_id or raw.get("response_snapshot_sha256") != response_sha:
            no("economics_response_snapshot_binding_mismatch"); continue
        expected_row_hash = stable_hash(row)
        if raw.get("response_record_hash") != expected_row_hash: no("economics_response_record_binding_mismatch"); continue
        if not _same_time(raw.get("forecast_decision_cutoff_utc"), cutoff): no("forecast_decision_cutoff_mismatch"); continue
        known = parse_epoch(raw.get("known_at_utc"))
        if known is None or known > frozen or known > cutoff_epoch: no("economics_known_after_freeze_or_cutoff"); continue
        sources, facts, factors, minimum_known, basis_error = _basis_and_clocks(row, contract)
        if basis_error: no(basis_error); continue
        if known < minimum_known: no("economics_predates_cited_fact_or_thesis_clock"); continue
        direction = str(raw.get("forecast_direction") or "").lower()
        if direction != row.get("research_direction") or direction not in {"buy", "sell"}: no("economics_direction_mismatch"); continue
        input_sources = _strings(raw.get("source_ids"), "economics.source_ids")
        input_facts = _strings(raw.get("source_fact_ids"), "economics.source_fact_ids")
        input_factors = _strings(raw.get("independent_factor_ids"), "economics.independent_factor_ids")
        if not set(sources).issubset(input_sources) or not set(facts).issubset(input_facts) or input_factors != factors:
            no("economics_source_fact_or_factor_lineage_mismatch"); continue
        quote = quotes.get(instrument)
        if quote is None: no("missing_admissible_point_in_time_venue_quote"); continue
        quote_id = str(raw.get("entry_quote_record_id") or "")
        if quote_id != quote["quote_record_id"] or raw.get("entry_quote_record_hash") != quote["quote_record_hash"]:
            no("economics_entry_quote_binding_mismatch"); continue
        verifier_id = str(raw.get("verifier_evidence_id") or ""); evidence = verified.get(verifier_id)
        if evidence is None: no("missing_admissible_verifier_evidence"); continue
        if raw.get("verifier_evidence_hash") != evidence["verifier_evidence_hash"] or evidence["forecast_id"] != str(raw.get("forecast_id") or ""):
            no("verifier_evidence_binding_mismatch"); continue
        missing_hash = False
        for field in policy["required_hash_fields"]:
            try: _hash(raw.get(field))
            except AfterCostV2Error: missing_hash = True; break
        if missing_hash: no("missing_model_feature_magnitude_or_cost_hash"); continue
        if not all(str(raw.get(field) or "") for field in ("forecast_id", "model_contract_id", "model_cohort_id", "feature_contract_id", "magnitude_model_id", "cost_model_contract_id")):
            no("missing_model_feature_magnitude_or_cost_id"); continue
        magnitudes = {field: _finite(raw.get(field)) for field in ("expected_favorable_move_bps", "expected_adverse_move_bps")}
        costs = {field: _finite(raw.get(field)) for field in policy["required_cost_fields"]}
        if any(x is None or x < 0 for x in [*magnitudes.values(), *costs.values()]): no("missing_or_negative_magnitude_or_cost"); continue
        clock_bad = False
        for field in ["magnitude_known_at_utc", *policy["required_cost_clock_fields"]]:
            epoch = parse_epoch(raw.get(field))
            if epoch is None or epoch > known or epoch > frozen or epoch > cutoff_epoch: clock_bad = True; break
        if clock_bad: no("magnitude_or_cost_clock_invalid"); continue
        rotation_state = str(raw.get("rotation_state_id") or "")
        if not rotation_state: no("missing_rotation_state_id"); continue
        pdir, pclear = evidence["calibrated_direction_probability"], evidence["calibrated_cost_clear_probability"]
        gross = pdir * magnitudes["expected_favorable_move_bps"] - (1 - pdir) * magnitudes["expected_adverse_move_bps"]
        total = quote["entry_half_spread_bps"] + sum(costs.values())
        net = gross - total; bps_to_pips = quote["mid"] * 0.0001 / quote["pip"]
        result[key] = {
            "economics_record_id": record_id, "economics_record_hash": stable_hash(raw),
            "forecast_id": evidence["forecast_id"], "known_at_utc": _utc(raw["known_at_utc"], "known_at_utc"),
            "research_direction": direction, "source_ids": input_sources, "source_fact_ids": input_facts,
            "independent_factor_ids": input_factors, "verifier_evidence_id": verifier_id,
            "verifier_evidence_hash": evidence["verifier_evidence_hash"], "effective_n": evidence["effective_n"],
            "calibrated_direction_probability": pdir, "calibrated_cost_clear_probability": pclear,
            **magnitudes,
            "costs_bps": {"entry_half_spread": quote["entry_half_spread_bps"], **costs, "total": total},
            "expected_gross_ev_bps": gross, "expected_after_cost_ev_bps": net,
            "expected_after_cost_ev_pips": net * bps_to_pips, "quote_record_id": quote["quote_record_id"],
            "rotation_state_id": rotation_state,
            "rotation_position_id": str(raw.get("rotation_position_id") or "") or None,
            "bound_account_snapshot_id": str(raw.get("bound_account_snapshot_id") or "") or None,
            "bound_position_ledger_snapshot_id": str(raw.get("bound_position_ledger_snapshot_id") or "") or None,
        }
    return result, sorted(rejected, key=lambda r: (r["arm_id"], r["horizon_sec"], r["instrument"], r["record_id"], r["reason"]))


def _allocate(rows: list[dict[str, Any]], contract: Mapping[str, Any]) -> dict[str, Any]:
    policy, forward = contract["selection"], set(contract["forward_admissible_arm_ids"]); output = {}
    for arm in contract["expected_arm_ids"]:
        output[arm] = {}
        for horizon in contract["expected_horizons_sec"]:
            group = [r for r in rows if r["arm_id"] == arm and r["horizon_sec"] == horizon and r["economics_admissible"]]
            group.sort(key=lambda r: (-r["expected_after_cost_ev_bps"], r["instrument"]))
            for rank, row in enumerate(group, 1):
                row["rank_within_arm_horizon"] = rank
                row["selectable_counterfactual"] = row["expected_after_cost_ev_bps"] >= float(policy["minimum_after_cost_ev_bps"]) and row["calibrated_cost_clear_probability"] >= float(policy["minimum_cost_clear_probability"])
            selectable = [r for r in group if r["selectable_counterfactual"]]
            basket, currencies, factors = [], set(), set()
            if arm in forward:
                for row in selectable:
                    pair_ccy = set(split_currency_pair(row["instrument"])); row_factors = set(row["independent_factor_ids"])
                    if pair_ccy & currencies or row_factors & factors: continue
                    basket.append({k: row[k] for k in ("instrument", "research_direction", "horizon_sec", "expected_after_cost_ev_bps", "rank_within_arm_horizon", "economics_record_id", "quote_record_id", "independent_factor_ids")})
                    currencies |= pair_ccy; factors |= row_factors
                    if len(basket) >= int(policy["maximum_disjoint_positions"]): break
            output[arm][str(horizon)] = {
                "state": "paper_counterfactual_available" if selectable and arm in forward else "unavailable",
                "top_one": basket[0] if basket else None, "disjoint_basket": basket,
                "admissible_ranked_count": len(group), "selectable_count": len(selectable),
                "research_only": True, "execution_eligible": False, "can_place_orders": False,
                "supported_execution_decision": "no_trade",
            }
    return output


def _hold_switch(records: Sequence[Mapping[str, Any]], identity: Mapping[str, Any] | None, cutoff: str, response_id: str, response_sha: str, response_index: Mapping[tuple[str, str, int], Mapping[str, Any]], quotes: Mapping[str, Mapping[str, Any]], verified: Mapping[str, Mapping[str, Any]], rows: Sequence[Mapping[str, Any]], contract: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    output, rejected, seen = [], [], set(); cutoff_epoch = parse_epoch(cutoff)
    frozen = parse_epoch(None if identity is None else identity["frozen_at_utc"]); policy = contract["hold_switch"]
    by_group = defaultdict(list)
    for row in rows:
        if row["selectable_counterfactual"]: by_group[(row["arm_id"], row["horizon_sec"])].append(row)
    for raw in records:
        record_id, arm, instrument = str(raw.get("hold_record_id") or ""), str(raw.get("arm_id") or ""), str(raw.get("instrument") or "")
        try: horizon = int(raw.get("horizon_sec"))
        except (TypeError, ValueError): horizon = 0
        key = (arm, instrument, horizon)
        def no(reason: str) -> None: _reject(rejected, "hold", record_id, key, reason)
        if (arm, horizon) in seen: raise AfterCostV2Error(f"duplicate hold group {(arm, horizon)}")
        seen.add((arm, horizon))
        response_row = response_index.get(key)
        if not record_id or response_row is None: no("missing_hold_id_or_response_grid_record"); continue
        if raw.get("response_snapshot_id") != response_id or raw.get("response_snapshot_sha256") != response_sha or raw.get("response_record_hash") != stable_hash(response_row):
            no("hold_response_binding_mismatch"); continue
        if raw.get("account_id") != policy["required_account_id"] or raw.get("environment") != policy["required_environment"]:
            no("hold_account_or_environment_mismatch"); continue
        required_ids = ("account_snapshot_id", "position_ledger_snapshot_id", "position_id", "trade_id", "position_ledger_record_id", "capacity_state_id", "conflict_state_id")
        required_hashes = ("account_snapshot_sha256", "position_ledger_snapshot_sha256", "position_record_sha256", "capacity_state_sha256", "conflict_state_sha256")
        if any(not str(raw.get(field) or "") for field in required_ids): no("missing_account_position_trade_or_portfolio_id"); continue
        try:
            for field in required_hashes: _hash(raw.get(field))
        except AfterCostV2Error: no("missing_account_position_trade_or_portfolio_hash"); continue
        units, entry_price = _finite(raw.get("units")), _finite(raw.get("entry_price"))
        direction = str(raw.get("hold_direction") or "").lower()
        if units is None or units == 0 or entry_price is None or entry_price <= 0 or direction not in {"buy", "sell"}:
            no("invalid_position_units_entry_or_direction"); continue
        if (units > 0) != (direction == "buy"): no("position_units_direction_mismatch"); continue
        known = parse_epoch(raw.get("known_at_utc")); clocks = [parse_epoch(raw.get(field)) for field in ("account_snapshot_at_utc", "position_ledger_at_utc", "portfolio_state_known_at_utc")]
        if known is None or any(x is None for x in clocks) or max(clocks) > known or known > frozen or known > cutoff_epoch:
            no("hold_account_ledger_or_portfolio_clock_invalid"); continue
        quote = quotes.get(instrument)
        if quote is None or raw.get("current_close_quote_record_id") != quote["quote_record_id"] or raw.get("current_close_quote_record_hash") != quote["quote_record_hash"]:
            no("hold_current_close_quote_binding_mismatch"); continue
        evidence = verified.get(str(raw.get("hold_verifier_evidence_id") or ""))
        if evidence is None or raw.get("hold_verifier_evidence_hash") != evidence["verifier_evidence_hash"] or evidence["forecast_id"] != str(raw.get("hold_forecast_id") or ""):
            no("hold_verifier_binding_mismatch"); continue
        sources, facts, factors, minimum_known, basis_error = _basis_and_clocks(response_row, contract)
        if basis_error or known < minimum_known: no(basis_error or "hold_predates_cited_fact_or_thesis_clock"); continue
        if not set(facts).issubset(set(_strings(raw.get("source_fact_ids"), "hold.source_fact_ids"))) or _strings(raw.get("independent_factor_ids"), "hold.independent_factor_ids") != factors:
            no("hold_fact_or_factor_lineage_mismatch"); continue
        allowed = set(_strings(raw.get("switch_allowed_instruments"), "switch_allowed_instruments")); conflicts = set(_strings(raw.get("conflicted_factor_ids"), "conflicted_factor_ids"))
        maximum_units = _finite(raw.get("maximum_switch_units"))
        if maximum_units is None or maximum_units < abs(units): no("portfolio_capacity_insufficient_or_missing"); continue
        hold_values = {field: _finite(raw.get(field)) for field in ("expected_hold_favorable_move_bps", "expected_hold_adverse_move_bps", "expected_hold_exit_half_spread_bps", "expected_hold_exit_slippage_bps", "expected_hold_exit_latency_bps", "current_close_slippage_bps", "current_close_latency_bps")}
        if any(x is None or x < 0 for x in hold_values.values()): no("hold_close_or_eventual_exit_cost_missing"); continue
        cost_clock = parse_epoch(raw.get("hold_costs_known_at_utc")); magnitude_clock = parse_epoch(raw.get("hold_magnitude_known_at_utc"))
        if cost_clock is None or magnitude_clock is None or max(cost_clock, magnitude_clock) > known: no("hold_cost_or_magnitude_clock_invalid"); continue
        p = evidence["calibrated_direction_probability"]
        hold_gross = p * hold_values["expected_hold_favorable_move_bps"] - (1-p) * hold_values["expected_hold_adverse_move_bps"]
        hold_cost = hold_values["expected_hold_exit_half_spread_bps"] + hold_values["expected_hold_exit_slippage_bps"] + hold_values["expected_hold_exit_latency_bps"]
        hold_net = hold_gross - hold_cost
        current_close_cost = quote["entry_half_spread_bps"] + hold_values["current_close_slippage_bps"] + hold_values["current_close_latency_bps"]
        alternatives = []
        for candidate in by_group[(arm, horizon)]:
            if candidate["instrument"] not in allowed or set(candidate["independent_factor_ids"]) & conflicts: continue
            economics = candidate["_economics"]
            if economics.get("rotation_position_id") != raw["position_id"] or economics.get("bound_account_snapshot_id") != raw["account_snapshot_id"] or economics.get("bound_position_ledger_snapshot_id") != raw["position_ledger_snapshot_id"]: continue
            if not math.isclose(economics["costs_bps"]["rotation_close_cost_bps"], current_close_cost, abs_tol=1e-9): continue
            alternatives.append(candidate)
        alternatives.sort(key=lambda r: (-r["expected_after_cost_ev_bps"], r["instrument"])); best = alternatives[0] if alternatives else None
        incremental = None if best is None else best["expected_after_cost_ev_bps"] - hold_net
        switch = best is not None and incremental >= float(policy["minimum_incremental_switch_ev_bps"])
        output.append({
            "hold_record_id": record_id, "account_id": raw["account_id"], "account_snapshot_id": raw["account_snapshot_id"],
            "position_ledger_snapshot_id": raw["position_ledger_snapshot_id"], "position_id": raw["position_id"], "trade_id": raw["trade_id"],
            "units": units, "entry_price": entry_price, "instrument": instrument, "hold_direction": direction,
            "expected_hold_after_cost_ev_bps": hold_net, "current_close_cost_bps": current_close_cost,
            "state": "paper_switch" if switch else "paper_hold", "incremental_switch_ev_bps": incremental,
            "best_switch_candidate": None if best is None else {k: best[k] for k in ("instrument", "research_direction", "horizon_sec", "expected_after_cost_ev_bps", "economics_record_id", "quote_record_id")},
            "research_only": True, "execution_eligible": False, "can_place_orders": False, "supported_execution_decision": "no_trade",
        })
    return output, sorted(rejected, key=lambda r: (r["arm_id"], r["horizon_sec"], r["record_id"], r["reason"]))


def build_after_cost_counterfactual_v2(response_snapshot: Mapping[str, Any], *, contract: Mapping[str, Any], frozen_manifest: Mapping[str, Any], venue_quotes: Mapping[str, Any] | None = None, verifier_evidence: Mapping[str, Any] | None = None, economics_inputs: Mapping[str, Any] | None = None, hold_switch_inputs: Mapping[str, Any] | None = None) -> dict[str, Any]:
    validate_contract(contract); manifest = _validate_manifest(frozen_manifest, contract)
    cutoff, response_sha, response_index = _response_grid(response_snapshot, contract)
    response_id = str(response_snapshot.get("snapshot_id") or "")
    if not response_id: raise AfterCostV2Error("response snapshot id missing")
    quote_identity, quote_records = _envelope(venue_quotes, "venue_quotes", cutoff)
    verifier_identity, verifier_records = _envelope(verifier_evidence, "verifier_evidence", cutoff)
    economics_identity, economics_records = _envelope(economics_inputs, "economics_inputs", cutoff)
    hold_identity, hold_records = _envelope(hold_switch_inputs, "hold_switch_inputs", cutoff)
    quotes, quote_rejections = _quotes(quote_records, quote_identity, cutoff, contract)
    verified, verifier_rejections = _verifier(verifier_records, verifier_identity, cutoff, response_id, response_sha, contract, manifest)
    economics, economics_rejections = _economics(economics_records, economics_identity, cutoff, response_id, response_sha, response_index, quotes, verified, contract)
    rejection_lookup = defaultdict(list)
    for item in economics_rejections: rejection_lookup[(item["arm_id"], item["instrument"], item["horizon_sec"])].append(item["reason"])
    rows = []
    for arm in contract["expected_arm_ids"]:
        for horizon in contract["expected_horizons_sec"]:
            for instrument in contract["expected_instruments"]:
                key = (arm, instrument, int(horizon)); source = response_index[key]; econ = economics.get(key)
                if arm == "observed_price_only": blockers = ["observed_price_hindsight_forbidden_as_forward_forecast"]
                elif arm in set(contract["never_forward_arm_ids"]): blockers = ["arm_not_directional_forward_hypothesis"]
                elif source.get("paper_candidate") is not True or source.get("forward_hypothesis") is not True: blockers = ["response_arm_has_no_forward_candidate"]
                elif econ is None: blockers = rejection_lookup.get(key) or ["missing_admissible_economics_record"]
                else: blockers = []
                sources, facts, factors, _, _ = _basis_and_clocks(source, contract) if source.get("paper_candidate") else ([], [], [], None, None)
                row = {
                    "arm_id": arm, "instrument": instrument, "horizon_sec": int(horizon),
                    "research_direction": source.get("research_direction"), "response_record_hash": stable_hash(source),
                    "source_ids": sources, "source_fact_ids": facts, "independent_factor_ids": factors,
                    "economics_record_id": None if econ is None else econ["economics_record_id"],
                    "quote_record_id": None if econ is None else econ["quote_record_id"],
                    "verifier_evidence_id": None if econ is None else econ["verifier_evidence_id"],
                    "economics_admissible": econ is not None and not blockers, "blockers": list(blockers),
                    "calibrated_direction_probability": None if econ is None else econ["calibrated_direction_probability"],
                    "calibrated_cost_clear_probability": None if econ is None else econ["calibrated_cost_clear_probability"],
                    "expected_gross_ev_bps": None if econ is None else econ["expected_gross_ev_bps"],
                    "expected_costs_bps": None if econ is None else econ["costs_bps"],
                    "expected_after_cost_ev_bps": None if econ is None else econ["expected_after_cost_ev_bps"],
                    "expected_after_cost_ev_pips": None if econ is None else econ["expected_after_cost_ev_pips"],
                    "rank_within_arm_horizon": None, "selectable_counterfactual": False,
                    "paper_decision": "no_trade", "research_only": True, "execution_eligible": False,
                    "can_place_orders": False, "supported_execution_decision": "no_trade",
                    "_economics": econ,
                }
                rows.append(row)
    allocations = _allocate(rows, contract)
    holds, hold_rejections = _hold_switch(hold_records, hold_identity, cutoff, response_id, response_sha, response_index, quotes, verified, rows, contract)
    for row in rows: row.pop("_economics", None)
    all_rejections = quote_rejections + verifier_rejections + economics_rejections + hold_rejections
    blockers = Counter(value for row in rows for value in row["blockers"])
    summary = {
        "row_count": len(rows), "venue_quote_input_count": len(quote_records), "admissible_quote_count": len(quotes),
        "verifier_input_count": len(verifier_records), "admissible_verifier_count": len(verified),
        "economics_input_count": len(economics_records), "economics_admissible_count": sum(r["economics_admissible"] for r in rows),
        "ranked_count": sum(r["rank_within_arm_horizon"] is not None for r in rows),
        "selectable_count": sum(r["selectable_counterfactual"] for r in rows), "hold_switch_count": len(holds),
        "input_rejection_count": len(all_rejections), "blocker_counts": dict(sorted(blockers.items())),
    }
    output = {
        "schema_version": 2, "snapshot_schema": contract["snapshot_schema"],
        "counterfactual_contract_id": contract["contract_id"], "counterfactual_cohort_id": contract["counterfactual_cohort_id"],
        "supersedes_contract_id": contract["supersedes_contract_id"], "supersedes_cohort_id": contract["supersedes_cohort_id"],
        "frozen_manifest": manifest, "decision_cutoff_utc": cutoff, "response_snapshot_id": response_id,
        "response_snapshot_sha256": response_sha, "response_arm_contract_id": response_snapshot["arm_contract_id"],
        "instrument_universe_sha256": contract["instrument_universe_sha256"], "instrument_count": 68,
        "horizons_sec": list(contract["expected_horizons_sec"]), "arm_ids": list(contract["expected_arm_ids"]),
        "quote_envelope": quote_identity, "verifier_envelope": verifier_identity, "economics_envelope": economics_identity,
        "hold_switch_envelope": hold_identity, "records": rows, "allocations": allocations,
        "hold_switch_counterfactuals": holds, "input_rejections": all_rejections, "summary": summary,
        "limitations": ["completed-minute CurrencyState bid/ask is never called a current executable quote", "verifier-produced evidence is mandatory and self-attestation is ignored", "hold/switch requires immutable Practice-007 position and portfolio state", "all outputs are research-only no_trade"],
        "research_only": True, "execution_eligible": False, "can_place_orders": False, "supported_execution_decision": "no_trade",
    }
    output["snapshot_id"] = "currency_state_after_cost_v2_" + stable_hash({k: v for k, v in output.items() if k != "snapshot_id"})[:24]
    if len(rows) != 8 * 68 * 5 or any(r["paper_decision"] != "no_trade" for r in rows): raise AfterCostV2Error("output grid/safety invariant failed")
    return output


__all__ = ["AfterCostV2Error", "build_after_cost_counterfactual_v2", "validate_contract"]
