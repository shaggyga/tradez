"""Disabled prospective event-response/timing successor V6 R1.

This module is deliberately inert.  It binds an exact independently reviewed
OfficialFact/CurrencyState V5 R3 dependency closure and can create only
deterministic test-fixture diagnostics.  It cannot initialize a canonical
ledger, create proof evidence, choose a direction, rank a pair, promote,
authorize, place an order, or start a worker.

The response surface records exact executable bid/ask chronology for later
research.  Replay and prospective-capture fixtures are explicitly separate
diagnostic partitions; neither is prospective proof.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..contracts.currency_state import stable_hash
from ..features.currency_state_official_context_v5 import (
    CONTEXT_CONTRACT_ID as UPSTREAM_CONTEXT_CONTRACT_ID,
    FACT_BASIS_ELIGIBILITY_CONTRACT_ID as UPSTREAM_BASIS_CONTRACT_ID,
    validate_currency_state_official_context_v5_snapshot,
)
from .official_fact_adapter_v5 import (
    ADAPTER_CONTRACT_ID as UPSTREAM_ADAPTER_CONTRACT_ID,
    validate_official_fact_v5_snapshot,
)


UTC = dt.timezone.utc
PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_VERSION = 6
SNAPSHOT_SCHEMA = "prospective_event_response_diagnostic_snapshot_v6"
CONTRACT_ID = "prospective_event_response_capture_v6_20260817_r1"
COHORT_ID = "prospective_official_event_response_v6_20260817_r1"
RESEARCH_GENERATION = "prospective_official_event_response_capture_v6_r1"
REVIEW_CERTIFICATE_ID = "official_fact_currency_state_v5_r3_independent_review_20260817"
SUPPORTED_EXECUTION_DECISION = "no_trade"

CONTRACT_PATH = PROJECT_ROOT / "config" / "prospective_event_response_v6_contract.json"
CONTRACT_SHA256 = "d5aae12a41bb4fd7638b7b9c40a97bb086f5f98f7916d13368589d164af7527c"
CONTRACT_BYTES = 4_173
REVIEW_CERTIFICATE_PATH = PROJECT_ROOT / "config" / "official_fact_currency_state_v5_r3_review_certificate.json"
REVIEW_CERTIFICATE_SHA256 = "0a61948a9fc2f2a9a06d30557b838d7fb389958c9ef2b1845bc8b06e70c7b299"
REVIEW_CERTIFICATE_BYTES = 6_003

CANONICAL_CURRENCIES = (
    "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD", "HUF", "JPY",
    "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB", "TRY", "USD", "ZAR",
)
CANONICAL_INSTRUMENTS = (
    "AUD_CAD", "AUD_CHF", "AUD_HKD", "AUD_JPY", "AUD_NZD", "AUD_SGD", "AUD_USD",
    "CAD_CHF", "CAD_HKD", "CAD_JPY", "CAD_SGD", "CHF_HKD", "CHF_JPY", "CHF_ZAR",
    "EUR_AUD", "EUR_CAD", "EUR_CHF", "EUR_CZK", "EUR_DKK", "EUR_GBP", "EUR_HKD",
    "EUR_HUF", "EUR_JPY", "EUR_NOK", "EUR_NZD", "EUR_PLN", "EUR_SEK", "EUR_SGD",
    "EUR_TRY", "EUR_USD", "EUR_ZAR", "GBP_AUD", "GBP_CAD", "GBP_CHF", "GBP_HKD",
    "GBP_JPY", "GBP_NZD", "GBP_PLN", "GBP_SGD", "GBP_USD", "GBP_ZAR", "HKD_JPY",
    "NZD_CAD", "NZD_CHF", "NZD_HKD", "NZD_JPY", "NZD_SGD", "NZD_USD", "SGD_CHF",
    "SGD_JPY", "TRY_JPY", "USD_CAD", "USD_CHF", "USD_CNH", "USD_CZK", "USD_DKK",
    "USD_HKD", "USD_HUF", "USD_JPY", "USD_MXN", "USD_NOK", "USD_PLN", "USD_SEK",
    "USD_SGD", "USD_THB", "USD_TRY", "USD_ZAR", "ZAR_JPY",
)
HORIZONS_SEC = (60, 300, 900, 1800, 3600)
INSTRUMENT_UNIVERSE_SHA256 = "b6abe559dcaa9faa0a1e48217f97a12dc9043eb9595d2b946bf80f93035be142"
DIAGNOSTIC_MODES = frozenset({"prospective_capture_test_fixture", "replay_diagnostic_test_fixture"})


class ProspectiveEventResponseV6Error(RuntimeError):
    """Raised when a V6 identity, chronology, or safety invariant fails."""


_PINNED_R3_ARTIFACTS = {
    "src/forex_system/ingestion/official_fact_adapter_v5.py": ("e35c9d2f0ad0972ae0abe2c75d2c47ac6c884c33eebd04cc2866c8bf1f5e5125", 53_778),
    "src/forex_system/features/currency_state_official_context_v5.py": ("ee98edb94a7cd4f8e19dd7826f6362ca422b2847ca9fd8fa8cd08b0ed5d14cfc", 45_524),
    "config/official_fact_adapter_contract_v5.json": ("d790e0d5fff7a9279772cd3c305db0e47d8f964e88f29c98e13fc730470897f4", 3_651),
    "config/official_fact_adapter_v5_manifest.json": ("3b4a6589253c8e6a6974840527760eda5b24ec87c109bb0e7c694d266b21b332", 4_685),
    "config/currency_state_official_context_contract_v5.json": ("7ccde8da950cbb5a229b774653b373a4c81b95761b67cb37b9f927a32d99dfb6", 3_335),
    "config/currency_state_official_context_v5_manifest.json": ("45f74389add35b2c2435f9f0907819899f81e402ff06a37b91c94171ab632974", 5_652),
    "test_official_fact_adapter_v5.py": ("63a7c1bb9f141165a294ad7b4b0596a394adf661b2ec28f0d552cc66f3a8e860", 31_262),
    "test_oanda_currency_state_official_context.py": ("386d65a0f90abefea09bf29c23f654a940797cc6c7bca86fd203a1596105bede", 7_604),
    "src/forex_system/__init__.py": ("9deb11a769210d2ac83704efeff1606a800c009bb89a627eceb7b671013f6438", 112),
    "config/currency_state_engine_v2.json": ("20138fe4328ee88b47b8edce2b3bd6081191cda35dd4124a9a457ea9b2efa400", 3_272),
    "config/currency_state_official_context_contract_v2.json": ("a8ec43b1a7a2ea1dff505b1eef2fdbf574c54ca286d185fe8fc1ca5e866800bf", 1_686),
    "config/official_fact_adapter_contract_v1.json": ("c6ab4e326e10dfdb7b150a18f6aea50ebdb06c96d5f999c1962e924b9d867583", 631),
    "config/official_fact_adapter_contract_v2.json": ("0d04d53fd10ebee9ba5db1098c2751443dc555bf7ea2a0a46c977cd55cdcbc0b", 1_705),
    "config/official_policy_statement_baselines_v2_20260816.json": ("08bbf63aec7b0655f14140fd5995c26d80ee32595d9c9c7ce2ac9a1688b91dcd", 47_794),
    "config/rates_policy_repricing_v1.json": ("8d5645dbe2fa16fc879810a9a7f4ceada28ff414bea3f50b9ab233df09921c08", 764),
    "data/oanda_training_manager/source_archives/official_fact_adapter_v4/immutable_event_clock_v2_a5276c696eb6ff3.sqlite": ("a5276c696eb6ff3c46adfcff41201288c2c966a76a52240897f5f0f25d30a740", 909_312),
    "src/forex_system/contracts/currency_state.py": ("6d7c625af12c2af45a45ecd832ad92c600c986f57193278da6a0f83231ea3139", 4_073),
    "src/forex_system/features/currency_state_engine.py": ("af52e1c18d66ff38082e0f383deb9183628b6f06c8fc73c446a49a038fe6e049", 30_200),
    "src/forex_system/features/currency_state_official_context.py": ("beca86b5fec03a4cfe233680fd27560478ec5edaa3f23e2a0cd140fe52869418", 16_109),
    "src/forex_system/features/currency_state_official_context_v2.py": ("beca8eadc36333b3e0966cddf245fb5b367766ccf3fc3bb99202b754c8ab45df", 22_860),
    "src/forex_system/ingestion/immutable_event_clock.py": ("c9f82aa767793ab280cd66c05f409cb9e067ca546ee5fb7eaec8cca22dae9e04", 89_792),
    "src/forex_system/ingestion/official_fact_adapter.py": ("8b6a1605b95ea21dfdffde42a502eb7fa1b5f592d4c3dbc2af501e9cc1c5a6e4", 49_410),
    "src/forex_system/ingestion/official_fact_adapter_v2.py": ("0bc31d08e1c7e5429fc7e3a78a3a6f253cd081614cd7e9845ab212baab825662", 36_341),
}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _strict_json(payload: bytes, *, label: str) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ProspectiveEventResponseV6Error(f"{label}_duplicate_json_key:{key}")
            result[key] = value
        return result

    def invalid(value: str) -> None:
        raise ProspectiveEventResponseV6Error(f"{label}_nonfinite_json:{value}")

    try:
        return json.loads(payload.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProspectiveEventResponseV6Error(f"{label}_invalid_json") from exc


def _is_sha256(value: Any) -> bool:
    return type(value) is str and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _utc(value: Any, *, path: str) -> dt.datetime:
    if type(value) is not str or not value:
        raise ProspectiveEventResponseV6Error(f"{path}_must_be_utc_string")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise ProspectiveEventResponseV6Error(f"{path}_invalid_utc") from exc
    if parsed.tzinfo is None:
        raise ProspectiveEventResponseV6Error(f"{path}_naive_utc")
    return parsed.astimezone(UTC)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _number(value: Any, *, path: str, positive: bool = False) -> float:
    if type(value) not in {int, float}:
        raise ProspectiveEventResponseV6Error(f"{path}_must_be_number")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0.0):
        raise ProspectiveEventResponseV6Error(f"{path}_invalid_number")
    return result


def _exact_mapping(value: Any, fields: set[str], *, path: str) -> Mapping[str, Any]:
    if type(value) is not dict or set(value) != fields:
        raise ProspectiveEventResponseV6Error(f"{path}_closed_schema_mismatch")
    return value


def _safe_path(relative_path: str) -> Path:
    if type(relative_path) is not str or not relative_path or "\\" in relative_path:
        raise ProspectiveEventResponseV6Error("artifact_path_invalid")
    candidate = PROJECT_ROOT.joinpath(*relative_path.split("/"))
    try:
        resolved = candidate.resolve(strict=True)
        root = PROJECT_ROOT.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise ProspectiveEventResponseV6Error(f"artifact_path_escape_or_missing:{relative_path}") from exc
    if candidate.is_symlink() or resolved != candidate.resolve():
        raise ProspectiveEventResponseV6Error(f"artifact_symlink_forbidden:{relative_path}")
    return candidate


def _read_exact(relative_path: str, sha256: str, byte_length: int) -> bytes:
    path = _safe_path(relative_path)
    before = path.stat()
    payload = path.read_bytes()
    after = path.stat()
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_identity != after_identity:
        raise ProspectiveEventResponseV6Error(f"artifact_identity_changed:{relative_path}")
    if after.st_nlink != 1:
        raise ProspectiveEventResponseV6Error(f"artifact_hardlink_forbidden:{relative_path}")
    if len(payload) != byte_length or _sha256_bytes(payload) != sha256:
        raise ProspectiveEventResponseV6Error(f"artifact_content_mismatch:{relative_path}")
    return payload


def verify_dependency_closure() -> dict[str, Any]:
    """Reopen and verify the full reviewed R3 closure and approval certificate."""

    contract = _strict_json(
        _read_exact("config/prospective_event_response_v6_contract.json", CONTRACT_SHA256, CONTRACT_BYTES),
        label="v6_contract",
    )
    certificate = _strict_json(
        _read_exact(
            "config/official_fact_currency_state_v5_r3_review_certificate.json",
            REVIEW_CERTIFICATE_SHA256,
            REVIEW_CERTIFICATE_BYTES,
        ),
        label="r3_review_certificate",
    )
    if contract.get("contract_id") != CONTRACT_ID or contract.get("cohort_id") != COHORT_ID:
        raise ProspectiveEventResponseV6Error("contract_identity_mismatch")
    if contract.get("enabled") is not None:
        # The state lives in the closed nested state object, not a permissive top-level alias.
        raise ProspectiveEventResponseV6Error("contract_unexpected_activation_alias")
    state = contract.get("state")
    if type(state) is not dict or state != {
        "enabled": False,
        "registered": False,
        "research_worker_enabled": False,
        "collector_enabled": False,
        "independent_review_state": "pending",
        "candidate_frozen_utc": None,
        "runtime_started_utc": None,
        "canonical_output_initialized": False,
    }:
        raise ProspectiveEventResponseV6Error("contract_state_not_inert")
    for field in ("research_only", "execution_eligible", "can_place_orders", "can_promote", "can_authorize"):
        expected = field == "research_only"
        if type(contract.get(field)) is not bool or contract[field] is not expected:
            raise ProspectiveEventResponseV6Error(f"unsafe_contract_guard:{field}")
    if contract.get("supported_execution_decision") != SUPPORTED_EXECUTION_DECISION:
        raise ProspectiveEventResponseV6Error("unsafe_contract_decision")
    if contract.get("prospective_proof_mode_allowed") is not False:
        raise ProspectiveEventResponseV6Error("proof_mode_must_be_disabled")
    if contract.get("currencies") != list(CANONICAL_CURRENCIES):
        raise ProspectiveEventResponseV6Error("currency_universe_mismatch")
    if contract.get("instrument_universe") != list(CANONICAL_INSTRUMENTS):
        raise ProspectiveEventResponseV6Error("instrument_universe_mismatch")
    if contract.get("horizons_sec") != list(HORIZONS_SEC):
        raise ProspectiveEventResponseV6Error("horizon_universe_mismatch")
    if set(contract.get("diagnostic_modes") or ()) != DIAGNOSTIC_MODES:
        raise ProspectiveEventResponseV6Error("diagnostic_modes_mismatch")
    expected_review = contract.get("required_review_certificate")
    if expected_review != {
        "relative_path": "config/official_fact_currency_state_v5_r3_review_certificate.json",
        "sha256": REVIEW_CERTIFICATE_SHA256,
        "byte_length": REVIEW_CERTIFICATE_BYTES,
        "review_certificate_id": REVIEW_CERTIFICATE_ID,
    }:
        raise ProspectiveEventResponseV6Error("review_certificate_contract_mismatch")
    if certificate.get("review_certificate_id") != REVIEW_CERTIFICATE_ID:
        raise ProspectiveEventResponseV6Error("review_certificate_identity_mismatch")
    if certificate.get("review_disposition") != "independent_approve_disabled_unregistered_candidate":
        raise ProspectiveEventResponseV6Error("review_certificate_not_approved")
    results = certificate.get("review_results")
    if type(results) is not dict or any(type(results.get(key)) is not int for key in results):
        raise ProspectiveEventResponseV6Error("review_results_type_mismatch")
    if any(results.get(key) != 0 for key in ("p0_findings", "p1_findings", "p2_findings")):
        raise ProspectiveEventResponseV6Error("review_certificate_material_findings")
    certificate_artifacts = certificate.get("reviewed_artifacts")
    if type(certificate_artifacts) is not list:
        raise ProspectiveEventResponseV6Error("reviewed_artifacts_missing")
    certificate_map = {
        row.get("relative_path"): (row.get("sha256"), row.get("byte_length"))
        for row in certificate_artifacts if type(row) is dict
    }
    if certificate_map != _PINNED_R3_ARTIFACTS:
        raise ProspectiveEventResponseV6Error("reviewed_artifact_set_mismatch")
    for relative_path, (sha256, byte_length) in _PINNED_R3_ARTIFACTS.items():
        _read_exact(relative_path, sha256, byte_length)
    if UPSTREAM_ADAPTER_CONTRACT_ID != "official_fact_adapter_v5_20260817_r3":
        raise ProspectiveEventResponseV6Error("imported_adapter_contract_drift")
    if UPSTREAM_CONTEXT_CONTRACT_ID != "currency_state_official_context_v5_20260817_r3":
        raise ProspectiveEventResponseV6Error("imported_context_contract_drift")
    if UPSTREAM_BASIS_CONTRACT_ID != "official_fact_basis_eligibility_v5_20260817_r3":
        raise ProspectiveEventResponseV6Error("imported_basis_contract_drift")
    return {
        "contract_sha256": CONTRACT_SHA256,
        "review_certificate_sha256": REVIEW_CERTIFICATE_SHA256,
        "review_certificate_id": REVIEW_CERTIFICATE_ID,
        "pinned_r3_artifact_count": len(_PINNED_R3_ARTIFACTS),
        "closure_verified": True,
    }


def verify_candidate_manifest() -> dict[str, Any]:
    """Verify the inert successor's own byte closure without self-approval."""

    manifest_path = _safe_path("config/prospective_event_response_v6_manifest.json")
    manifest = _strict_json(manifest_path.read_bytes(), label="v6_manifest")
    if manifest.get("manifest_id") != "prospective_event_response_v6_manifest_20260817_r1":
        raise ProspectiveEventResponseV6Error("candidate_manifest_identity_mismatch")
    if manifest.get("contract_id") != CONTRACT_ID or manifest.get("cohort_id") != COHORT_ID:
        raise ProspectiveEventResponseV6Error("candidate_manifest_contract_mismatch")
    if manifest.get("review_state") != "pending_independent_review":
        raise ProspectiveEventResponseV6Error("candidate_manifest_cannot_self_approve")
    state = manifest.get("state")
    if state != {
        "enabled": False,
        "registered": False,
        "research_worker_enabled": False,
        "collector_enabled": False,
        "canonical_output_initialized": False,
    }:
        raise ProspectiveEventResponseV6Error("candidate_manifest_not_inert")
    output = manifest.get("output_surface")
    if type(output) is not dict or any(output.get(field) is not None for field in (
        "canonical_ledger_path", "canonical_state_path", "canonical_report_path",
        "worker_entrypoint", "collector_entrypoint",
    )):
        raise ProspectiveEventResponseV6Error("candidate_manifest_canonical_or_worker_surface")
    artifacts = manifest.get("artifacts")
    required = {
        "runtime_core", "contract", "adversarial_tests",
        "upstream_independent_review_certificate", "official_fact_v5_r3_manifest",
        "currency_state_context_v5_r3_manifest",
    }
    if type(artifacts) is not dict or set(artifacts) != required:
        raise ProspectiveEventResponseV6Error("candidate_manifest_artifact_set_mismatch")
    fixed = {
        "contract": ("config/prospective_event_response_v6_contract.json", CONTRACT_SHA256, CONTRACT_BYTES),
        "upstream_independent_review_certificate": (
            "config/official_fact_currency_state_v5_r3_review_certificate.json",
            REVIEW_CERTIFICATE_SHA256, REVIEW_CERTIFICATE_BYTES,
        ),
        "official_fact_v5_r3_manifest": (
            "config/official_fact_adapter_v5_manifest.json",
            "3b4a6589253c8e6a6974840527760eda5b24ec87c109bb0e7c694d266b21b332", 4_685,
        ),
        "currency_state_context_v5_r3_manifest": (
            "config/currency_state_official_context_v5_manifest.json",
            "45f74389add35b2c2435f9f0907819899f81e402ff06a37b91c94171ab632974", 5_652,
        ),
    }
    for name, (relative_path, sha256, byte_length) in fixed.items():
        if artifacts[name] != {
            "relative_path": relative_path, "sha256": sha256, "byte_length": byte_length
        }:
            raise ProspectiveEventResponseV6Error(f"candidate_manifest_fixed_artifact_mismatch:{name}")
        _read_exact(relative_path, sha256, byte_length)
    for name, expected_path in (
        ("runtime_core", "src/forex_system/ingestion/prospective_event_response_v6.py"),
        ("adversarial_tests", "test_prospective_event_response_v6.py"),
    ):
        spec = artifacts[name]
        if type(spec) is not dict or set(spec) != {"relative_path", "sha256", "byte_length"}:
            raise ProspectiveEventResponseV6Error(f"candidate_manifest_artifact_schema:{name}")
        if spec["relative_path"] != expected_path or not _is_sha256(spec["sha256"]) or type(spec["byte_length"]) is not int:
            raise ProspectiveEventResponseV6Error(f"candidate_manifest_artifact_identity:{name}")
        _read_exact(expected_path, spec["sha256"], spec["byte_length"])
    certificate = _strict_json(
        _read_exact(
            "config/official_fact_currency_state_v5_r3_review_certificate.json",
            REVIEW_CERTIFICATE_SHA256, REVIEW_CERTIFICATE_BYTES,
        ),
        label="r3_review_certificate",
    )
    reviewed = sorted(certificate["reviewed_artifacts"], key=lambda row: row["relative_path"])
    closure_hash = _sha256_json(reviewed)
    transitive = manifest.get("transitive_upstream_closure")
    if type(transitive) is not dict or transitive.get("artifact_count") != len(reviewed):
        raise ProspectiveEventResponseV6Error("candidate_manifest_transitive_count_mismatch")
    if transitive.get("canonical_artifact_list_sha256") != closure_hash:
        raise ProspectiveEventResponseV6Error("candidate_manifest_transitive_hash_mismatch")
    return {
        "manifest_id": manifest["manifest_id"],
        "artifact_count": len(artifacts),
        "transitive_upstream_artifact_count": len(reviewed),
        "candidate_closure_verified": True,
        "review_state": "pending_independent_review",
    }


_QUOTE_FIELDS = {
    "event_id", "event_version_id", "currency", "instrument", "horizon_sec",
    "event_scheduled_utc", "start_target_utc", "end_target_utc",
    "start_quote_time_utc", "end_quote_time_utc", "start_known_utc", "end_known_utc",
    "start_bid", "start_ask", "end_bid", "end_ask", "pip",
    "start_source_payload_sha256", "end_source_payload_sha256",
    "start_source_record_id", "end_source_record_id",
}


def _guard() -> dict[str, Any]:
    return {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "can_authorize": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }


def _source_binding(context: Mapping[str, Any], base: Mapping[str, Any], official: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "review_certificate_id": REVIEW_CERTIFICATE_ID,
        "review_certificate_sha256": REVIEW_CERTIFICATE_SHA256,
        "official_fact_adapter_contract_id": UPSTREAM_ADAPTER_CONTRACT_ID,
        "official_fact_snapshot_id": official["snapshot_id"],
        "official_fact_snapshot_sha256": stable_hash(dict(official)),
        "currency_state_official_context_contract_id": UPSTREAM_CONTEXT_CONTRACT_ID,
        "currency_state_official_context_snapshot_id": context["snapshot_id"],
        "currency_state_official_context_snapshot_sha256": stable_hash(dict(context)),
        "base_currency_state_snapshot_id": base["snapshot_id"],
        "base_currency_state_snapshot_sha256": stable_hash(dict(base)),
        "base_currency_state_input_identity_sha256": stable_hash(dict(base["input_identity"])),
        "context_input_identity_sha256": stable_hash(dict(context["input_identity"])),
    }


def _validate_upstream(context: Mapping[str, Any], base: Mapping[str, Any], official: Mapping[str, Any]) -> None:
    verify_candidate_manifest()
    verify_dependency_closure()
    validate_official_fact_v5_snapshot(official)
    validate_currency_state_official_context_v5_snapshot(
        context, base_snapshot=base, official_snapshot=official
    )
    if context.get("official_fact_snapshot_id") != official.get("snapshot_id"):
        raise ProspectiveEventResponseV6Error("context_official_identity_mismatch")
    if context.get("base_currency_state_snapshot_id") != base.get("snapshot_id"):
        raise ProspectiveEventResponseV6Error("context_base_identity_mismatch")
    if context.get("official_fact_adapter_contract_id") != UPSTREAM_ADAPTER_CONTRACT_ID:
        raise ProspectiveEventResponseV6Error("context_adapter_contract_mismatch")
    if context.get("component_context_contract_id") != UPSTREAM_CONTEXT_CONTRACT_ID:
        raise ProspectiveEventResponseV6Error("context_contract_mismatch")
    for row in (context, base, official):
        if row.get("research_only") is not True or row.get("execution_eligible") is not False:
            raise ProspectiveEventResponseV6Error("upstream_guard_mismatch")
        if row.get("can_place_orders") is not False or row.get("supported_execution_decision") != "no_trade":
            raise ProspectiveEventResponseV6Error("upstream_execution_guard_mismatch")


def _event_map(official: Mapping[str, Any], *, upstream_cutoff: dt.datetime) -> dict[tuple[str, str], Mapping[str, Any]]:
    result: dict[tuple[str, str], Mapping[str, Any]] = {}
    for event in official["upcoming_events"]:
        key = (event["event_id"], event["event_version_id"])
        if key in result:
            raise ProspectiveEventResponseV6Error("duplicate_upstream_event_identity")
        scheduled = _utc(event["scheduled_utc"], path="event.scheduled_utc")
        known = _utc(event["known_from_snapshot_utc"], path="event.known_from_snapshot_utc")
        ledger_known = _utc(event["ledger_effective_known_utc"], path="event.ledger_effective_known_utc")
        if ledger_known > known or known > upstream_cutoff or upstream_cutoff > scheduled:
            raise ProspectiveEventResponseV6Error("upstream_event_causal_chronology_invalid")
        result[key] = event
    return result


def _response_row(
    observation: Mapping[str, Any], *, event: Mapping[str, Any], capture_cutoff: dt.datetime,
    quote_policy: Mapping[str, Any], mode: str, source_binding_sha256: str,
) -> dict[str, Any]:
    row = _exact_mapping(observation, _QUOTE_FIELDS, path="executable_response")
    if row["event_id"] != event["event_id"] or row["event_version_id"] != event["event_version_id"]:
        raise ProspectiveEventResponseV6Error("response_event_identity_mismatch")
    if row["currency"] != event["currency"]:
        raise ProspectiveEventResponseV6Error("response_event_currency_mismatch")
    instrument = row["instrument"]
    if instrument not in CANONICAL_INSTRUMENTS or row["currency"] not in instrument.split("_"):
        raise ProspectiveEventResponseV6Error("response_instrument_mapping_mismatch")
    horizon = row["horizon_sec"]
    if type(horizon) is not int or horizon not in HORIZONS_SEC:
        raise ProspectiveEventResponseV6Error("response_horizon_mismatch")
    scheduled = _utc(row["event_scheduled_utc"], path="response.event_scheduled_utc")
    if _iso(scheduled) != _iso(_utc(event["scheduled_utc"], path="event.scheduled_utc")):
        raise ProspectiveEventResponseV6Error("response_event_schedule_mismatch")
    start_target = _utc(row["start_target_utc"], path="response.start_target_utc")
    end_target = _utc(row["end_target_utc"], path="response.end_target_utc")
    start_quote = _utc(row["start_quote_time_utc"], path="response.start_quote_time_utc")
    end_quote = _utc(row["end_quote_time_utc"], path="response.end_quote_time_utc")
    start_known = _utc(row["start_known_utc"], path="response.start_known_utc")
    end_known = _utc(row["end_known_utc"], path="response.end_known_utc")
    if start_target != scheduled or end_target != scheduled + dt.timedelta(seconds=horizon):
        raise ProspectiveEventResponseV6Error("response_target_chronology_mismatch")
    start_alignment = (start_quote - start_target).total_seconds()
    end_alignment = (end_quote - end_target).total_seconds()
    if start_alignment < 0 or start_alignment > float(quote_policy["maximum_start_alignment_sec"]):
        raise ProspectiveEventResponseV6Error("response_start_alignment_invalid")
    if end_alignment < 0 or end_alignment > float(quote_policy["maximum_end_alignment_sec"]):
        raise ProspectiveEventResponseV6Error("response_end_alignment_invalid")
    max_known_lag = float(quote_policy["maximum_quote_known_lag_sec"])
    for quote_time, known_time, label in (
        (start_quote, start_known, "start"), (end_quote, end_known, "end")
    ):
        lag = (known_time - quote_time).total_seconds()
        if lag < 0 or lag > max_known_lag or known_time > capture_cutoff:
            raise ProspectiveEventResponseV6Error(f"response_{label}_knowledge_clock_invalid")
    if start_known > end_known or start_quote > end_quote:
        raise ProspectiveEventResponseV6Error("response_clock_order_invalid")
    start_bid = _number(row["start_bid"], path="response.start_bid", positive=True)
    start_ask = _number(row["start_ask"], path="response.start_ask", positive=True)
    end_bid = _number(row["end_bid"], path="response.end_bid", positive=True)
    end_ask = _number(row["end_ask"], path="response.end_ask", positive=True)
    pip = _number(row["pip"], path="response.pip", positive=True)
    if start_ask <= start_bid or end_ask <= end_bid:
        raise ProspectiveEventResponseV6Error("response_bid_ask_invalid")
    start_spread = (start_ask - start_bid) / pip
    end_spread = (end_ask - end_bid) / pip
    maximum_spread = float(quote_policy["maximum_spread_pips"])
    if start_spread > maximum_spread or end_spread > maximum_spread:
        raise ProspectiveEventResponseV6Error("response_spread_sanity_failed")
    for field in ("start_source_payload_sha256", "end_source_payload_sha256"):
        if not _is_sha256(row[field]):
            raise ProspectiveEventResponseV6Error(f"response_{field}_invalid")
    for field in ("start_source_record_id", "end_source_record_id"):
        if type(row[field]) is not str or not row[field]:
            raise ProspectiveEventResponseV6Error(f"response_{field}_invalid")
    start_mid = (start_bid + start_ask) / 2.0
    end_mid = (end_bid + end_ask) / 2.0
    gross_long = (end_mid - start_mid) / pip
    gross_short = -gross_long
    long_after = (end_bid - start_ask) / pip
    short_after = (start_bid - end_ask) / pip
    identity = {
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "mode": mode,
        "source_binding_sha256": source_binding_sha256,
        "event_version_id": row["event_version_id"],
        "instrument": instrument,
        "horizon_sec": horizon,
        "start_source_record_id": row["start_source_record_id"],
        "end_source_record_id": row["end_source_record_id"],
    }
    return {
        "response_id": "event_response_v6_" + _sha256_json(identity)[:40],
        "partition": "diagnostic_replay" if mode.startswith("replay_") else "diagnostic_prospective_capture",
        "event_id": row["event_id"],
        "event_version_id": row["event_version_id"],
        "currency": row["currency"],
        "instrument": instrument,
        "horizon_sec": horizon,
        "event_scheduled_utc": _iso(scheduled),
        "start_target_utc": _iso(start_target),
        "end_target_utc": _iso(end_target),
        "start_quote_time_utc": _iso(start_quote),
        "end_quote_time_utc": _iso(end_quote),
        "start_known_utc": _iso(start_known),
        "end_known_utc": _iso(end_known),
        "start_alignment_sec": start_alignment,
        "end_alignment_sec": end_alignment,
        "actual_quote_duration_sec": (end_quote - start_quote).total_seconds(),
        "start_bid": start_bid,
        "start_ask": start_ask,
        "end_bid": end_bid,
        "end_ask": end_ask,
        "pip": pip,
        "start_spread_pips": start_spread,
        "end_spread_pips": end_spread,
        "gross_long_mid_pips": gross_long,
        "gross_short_mid_pips": gross_short,
        "long_after_executable_spread_pips": long_after,
        "short_after_executable_spread_pips": short_after,
        "long_execution_cost_pips": gross_long - long_after,
        "short_execution_cost_pips": gross_short - short_after,
        "start_source_payload_sha256": row["start_source_payload_sha256"],
        "end_source_payload_sha256": row["end_source_payload_sha256"],
        "start_source_record_id": row["start_source_record_id"],
        "end_source_record_id": row["end_source_record_id"],
        "direction_selected": None,
        "forecast_mean_pips": None,
        "forecast_probability": None,
        "expected_value_pips": None,
        "allocator_rank": None,
        "basis_eligible": False,
        "proof_eligible": False,
        **_guard(),
    }


def build_test_snapshot(
    *, context_snapshot: Mapping[str, Any], base_snapshot: Mapping[str, Any],
    official_snapshot: Mapping[str, Any], executable_responses: Sequence[Mapping[str, Any]],
    capture_cutoff_utc: str | dt.datetime, mode: str,
) -> dict[str, Any]:
    """Build a deterministic, noncanonical, test-only diagnostic snapshot."""

    if mode not in DIAGNOSTIC_MODES:
        raise ProspectiveEventResponseV6Error("only_explicit_test_diagnostic_modes_allowed")
    if type(executable_responses) not in {list, tuple}:
        raise ProspectiveEventResponseV6Error("executable_responses_must_be_sequence")
    _validate_upstream(context_snapshot, base_snapshot, official_snapshot)
    capture_cutoff = (
        capture_cutoff_utc.astimezone(UTC)
        if isinstance(capture_cutoff_utc, dt.datetime)
        else _utc(capture_cutoff_utc, path="capture_cutoff_utc")
    )
    upstream_cutoff = _utc(context_snapshot["decision_cutoff_utc"], path="context.decision_cutoff_utc")
    if capture_cutoff < upstream_cutoff:
        raise ProspectiveEventResponseV6Error("capture_precedes_upstream_knowledge")
    event_map = _event_map(official_snapshot, upstream_cutoff=upstream_cutoff)
    source_binding = _source_binding(context_snapshot, base_snapshot, official_snapshot)
    source_binding_sha256 = stable_hash(source_binding)
    contract = _strict_json(
        _read_exact("config/prospective_event_response_v6_contract.json", CONTRACT_SHA256, CONTRACT_BYTES),
        label="v6_contract",
    )
    quote_policy = contract["quote_response_contract"]
    responses: list[dict[str, Any]] = []
    seen: set[tuple[str, str, int]] = set()
    for raw in executable_responses:
        if type(raw) is not dict:
            raise ProspectiveEventResponseV6Error("executable_response_must_be_mapping")
        key = (raw.get("event_id"), raw.get("event_version_id"))
        event = event_map.get(key)
        if event is None:
            raise ProspectiveEventResponseV6Error("response_event_not_in_upstream_snapshot")
        row = _response_row(
            raw, event=event, capture_cutoff=capture_cutoff,
            quote_policy=quote_policy, mode=mode, source_binding_sha256=source_binding_sha256,
        )
        natural = (row["event_version_id"], row["instrument"], row["horizon_sec"])
        if natural in seen:
            raise ProspectiveEventResponseV6Error("duplicate_response_cell")
        seen.add(natural)
        responses.append(row)
    responses.sort(key=lambda row: (row["event_scheduled_utc"], row["event_version_id"], row["instrument"], row["horizon_sec"]))

    # Preserve the entire 21-currency / 68-pair / 5-horizon context surface.
    # These rows are descriptive only.  No missing upstream prediction is
    # converted into a downstream prediction, EV, rank, or basis claim.
    matrix: list[dict[str, Any]] = []
    for horizon in HORIZONS_SEC:
        upstream_horizon = context_snapshot["horizons"][str(horizon)]
        for instrument in CANONICAL_INSTRUMENTS:
            edge = upstream_horizon["pair_edges"][instrument]
            matrix.append({
                "instrument": instrument,
                "base_currency": instrument.split("_")[0],
                "quote_currency": instrument.split("_")[1],
                "horizon_sec": horizon,
                "upstream_research_observable": edge["research_observable"],
                "upstream_observed_pair_return_bps": edge["observed_pair_return_bps"],
                "upstream_observed_factor_move_bps": edge["observed_factor_move_bps"],
                "upstream_observed_residual_bps": edge["observed_residual_bps"],
                "forecast_mean_bps": None,
                "forecast_absolute_move_bps": None,
                "cost_clear_probability": None,
                "expected_value_pips": None,
                "allocator_rank": None,
                "basis_eligible": False,
                "proof_eligible": False,
                **_guard(),
            })

    plans: list[dict[str, Any]] = []
    for event in event_map.values():
        currency = event["currency"]
        affected = [instrument for instrument in CANONICAL_INSTRUMENTS if currency in instrument.split("_")]
        for instrument in affected:
            for horizon in HORIZONS_SEC:
                plans.append({
                    "event_id": event["event_id"],
                    "event_version_id": event["event_version_id"],
                    "currency": currency,
                    "instrument": instrument,
                    "horizon_sec": horizon,
                    "event_scheduled_utc": event["scheduled_utc"],
                    "direction_selected": None,
                    "forecast_mean_pips": None,
                    "expected_value_pips": None,
                    "allocator_rank": None,
                    "basis_eligible": False,
                    "proof_eligible": False,
                    **_guard(),
                })
    plans.sort(key=lambda row: (row["event_scheduled_utc"], row["event_version_id"], row["instrument"], row["horizon_sec"]))

    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_schema": SNAPSHOT_SCHEMA,
        "snapshot_id": None,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "research_generation": RESEARCH_GENERATION,
        "mode": mode,
        "partition": "diagnostic_only_not_proof",
        "test_only": True,
        "canonical_output": False,
        "capture_cutoff_utc": _iso(capture_cutoff),
        "upstream_decision_cutoff_utc": _iso(upstream_cutoff),
        "source_binding": source_binding,
        "source_binding_sha256": source_binding_sha256,
        "currency_count": 21,
        "instrument_count": 68,
        "horizon_count": 5,
        "event_count": len(event_map),
        "plan_count": len(plans),
        "diagnostic_response_count": len(responses),
        "proof_row_count": 0,
        "plans": plans,
        "response_diagnostics": responses,
        "context_matrix": matrix,
        "limitations": [
            "disabled_unregistered_test_fixture_only",
            "diagnostics_are_not_prospective_proof",
            "upstream_direction_policy_is_abstain",
            "no_forecast_expected_value_rank_or_basis_is_created",
            "independent_review_required_before_new_cohort_registration",
        ],
        **_guard(),
    }
    material = {key: value for key, value in result.items() if key != "snapshot_id"}
    result["snapshot_id"] = "prospective_event_response_v6_test_" + stable_hash(material)[:32]
    _validate_output_guards(result)
    return result


def _validate_output_guards(snapshot: Mapping[str, Any]) -> None:
    if snapshot.get("test_only") is not True or snapshot.get("canonical_output") is not False:
        raise ProspectiveEventResponseV6Error("output_not_test_only")
    if snapshot.get("proof_row_count") != 0 or type(snapshot.get("proof_row_count")) is not int:
        raise ProspectiveEventResponseV6Error("proof_rows_forbidden")
    for key, expected in _guard().items():
        if snapshot.get(key) != expected or type(snapshot.get(key)) is not type(expected):
            raise ProspectiveEventResponseV6Error(f"unsafe_output_guard:{key}")
    if len(snapshot.get("context_matrix") or ()) != len(CANONICAL_INSTRUMENTS) * len(HORIZONS_SEC):
        raise ProspectiveEventResponseV6Error("context_matrix_coverage_mismatch")
    guarded_rows = list(snapshot.get("plans") or ()) + list(snapshot.get("response_diagnostics") or ()) + list(snapshot.get("context_matrix") or ())
    for row in guarded_rows:
        for field in ("forecast_mean_pips", "forecast_mean_bps", "forecast_absolute_move_bps", "forecast_probability", "cost_clear_probability", "expected_value_pips", "allocator_rank", "direction_selected"):
            if field in row and row[field] is not None:
                raise ProspectiveEventResponseV6Error(f"downstream_invented_field:{field}")
        if row.get("basis_eligible") is not False or row.get("proof_eligible") is not False:
            raise ProspectiveEventResponseV6Error("downstream_invented_basis_or_proof")


def validate_test_snapshot(
    snapshot: Mapping[str, Any], *, context_snapshot: Mapping[str, Any],
    base_snapshot: Mapping[str, Any], official_snapshot: Mapping[str, Any],
    executable_responses: Sequence[Mapping[str, Any]], capture_cutoff_utc: str | dt.datetime,
    mode: str,
) -> None:
    """Rebuild a test snapshot from exact inputs and require byte-equivalence."""

    expected = build_test_snapshot(
        context_snapshot=context_snapshot,
        base_snapshot=base_snapshot,
        official_snapshot=official_snapshot,
        executable_responses=executable_responses,
        capture_cutoff_utc=capture_cutoff_utc,
        mode=mode,
    )
    if dict(snapshot) != expected:
        raise ProspectiveEventResponseV6Error("snapshot_reconstruction_mismatch")


__all__ = [
    "COHORT_ID", "CONTRACT_ID", "HORIZONS_SEC", "ProspectiveEventResponseV6Error",
    "build_test_snapshot", "validate_test_snapshot", "verify_candidate_manifest",
    "verify_dependency_closure",
]
