"""Strictly typed, research-only OfficialFact successor.

V3 is deliberately disabled from runtime registration.  It composes the
existing V2 collector and then validates every accepted leaf with a closed,
recursive, exact-type contract.  Booleans are never accepted as integers,
numeric values must be finite, and containers can never occupy scalar slots.

Clock V1 is historical evidence only.  V3 has no caller-selectable Clock V1
fingerprint: the one canonical retirement record and database are independently
verified byte-for-byte before the adapter can be instantiated.  Prospective
clock data continue to come exclusively from immutable Clock V2.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .immutable_event_clock import (
    CONTRACT_ID as IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
    SCHEMA_VERSION as IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
)
from .official_fact_adapter import CANONICAL_CURRENCIES
from .official_fact_adapter_v2 import (
    ADAPTER_CONTRACT_ID as PARENT_ADAPTER_CONTRACT_ID,
    OFFICIAL_CLOCK_ATTESTATION_FIELDS,
    OFFICIAL_CLOCK_FIELDS,
    OFFICIAL_CURRENCY_EVIDENCE_FIELDS,
    OFFICIAL_EVENT_CLOCK_PROVENANCE_FIELDS,
    OFFICIAL_FACT_EVIDENCE_CLASSES,
    OFFICIAL_FACT_FIELDS_BY_TYPE,
    OFFICIAL_FACT_TYPES,
    OFFICIAL_FACT_V2_TOP_LEVEL_FIELDS,
    OFFICIAL_GLOBAL_GAP_FIELDS,
    OFFICIAL_INTRADAY_RATE_CONTRACT_FIELDS,
    OFFICIAL_INTRADAY_RATE_FIELDS,
    OFFICIAL_SOURCE_HEALTH_FIELDS,
    OFFICIAL_SOURCE_ISSUE_FIELDS,
    OFFICIAL_UPCOMING_EVENT_FIELDS,
    OfficialFactAdapterV2,
    OfficialFactPathsV2,
)


ADAPTER_SCHEMA_VERSION = 3
ADAPTER_CONTRACT_ID = "official_fact_adapter_v3_20260817"
PARENT_ADAPTER_SCHEMA_VERSION = 2
FACT_BASIS_ELIGIBILITY_CONTRACT_ID = "official_fact_basis_eligibility_v3_20260817"
CLOCK_DEPENDENCY_POLICY = "strict_clock_v2_only_canonical_clock_v1_retirement_pinned"

SOURCE_ROOT = Path(__file__).resolve().parents[3]
CANONICAL_CLOCK_V1_DATABASE = (
    SOURCE_ROOT
    / "data"
    / "oanda_training_manager"
    / "research_ledgers"
    / "immutable_event_clock_v1.sqlite"
)
CANONICAL_CLOCK_V1_RETIREMENT = (
    SOURCE_ROOT / "config" / "immutable_event_clock_v1_retirement.json"
)
CANONICAL_CLOCK_V1_DATABASE_BYTES = 1_384_448
CANONICAL_CLOCK_V1_DATABASE_SHA256 = (
    "d45a8fd09077cfb1c733003900d2e6ef6ae7180dffd6e09f66ab4b0412aff874"
)
CANONICAL_CLOCK_V1_RETIREMENT_BYTES = 1_617
CANONICAL_CLOCK_V1_RETIREMENT_SHA256 = (
    "4914b5f17accc1532891172254d2f39469eac98307e4d415e0a7e7d77f950c6d"
)
CANONICAL_CLOCK_V1_ROW_COUNTS = {
    "event_clock_capture_attestations": 2,
    "event_clock_observations": 3,
    "event_clock_snapshots": 3,
    "event_clock_versions": 501,
    "snapshot_events": 593,
}

OFFICIAL_FACT_V3_TOP_LEVEL_FIELDS = OFFICIAL_FACT_V2_TOP_LEVEL_FIELDS
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_CLOCK_OBSERVATION_ID = re.compile(r"event_clock_observation_[0-9a-f]{32}\Z")
_CLOCK_SNAPSHOT_ID = re.compile(r"event_clock_snapshot_[0-9a-f]{32}\Z")


class OfficialFactAdapterV3Error(RuntimeError):
    """Raised when any V3 identity, type, count, or causal claim is invalid."""


@dataclass(frozen=True)
class OfficialFactPathsV3(OfficialFactPathsV2):
    """V3 has no Clock V1 path or caller-selectable retirement fingerprint."""


def _fail(path: str, expected: str) -> None:
    raise OfficialFactAdapterV3Error(f"typed_schema:{path}:{expected}")


def _exact_str(value: Any, *, path: str, nonempty: bool = False) -> str:
    if type(value) is not str or (nonempty and not value):
        _fail(path, "exact_string" + ("_nonempty" if nonempty else ""))
    return value


def _optional_str(value: Any, *, path: str) -> str | None:
    if value is None:
        return None
    return _exact_str(value, path=path)


def _exact_bool(value: Any, *, path: str) -> bool:
    if type(value) is not bool:
        _fail(path, "exact_bool")
    return value


def _exact_int(value: Any, *, path: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        _fail(path, f"exact_int_min_{minimum}")
    return value


def _finite_number(
    value: Any, *, path: str, nullable: bool = False, minimum: float | None = None
) -> int | float | None:
    if value is None and nullable:
        return None
    if type(value) not in (int, float) or not math.isfinite(float(value)):
        _fail(path, "finite_number" + ("_or_null" if nullable else ""))
    if minimum is not None and float(value) < minimum:
        _fail(path, f"finite_number_min_{minimum}")
    return value


def _timestamp(value: Any, *, path: str, nullable: bool = False) -> dt.datetime | None:
    if value is None and nullable:
        return None
    text = _exact_str(value, path=path, nonempty=True)
    if not text.endswith("+00:00"):
        _fail(path, "canonical_utc_timestamp")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        _fail(path, "canonical_utc_timestamp")
    if parsed.tzinfo != dt.timezone.utc or parsed.isoformat() != text:
        _fail(path, "canonical_utc_timestamp")
    return parsed


def _date(value: Any, *, path: str) -> None:
    text = _exact_str(value, path=path, nonempty=True)
    try:
        parsed = dt.date.fromisoformat(text)
    except ValueError:
        _fail(path, "iso_date")
    if parsed.isoformat() != text:
        _fail(path, "iso_date")


def _hash64(value: Any, *, path: str, allow_empty: bool = False) -> str:
    text = _exact_str(value, path=path)
    if allow_empty and not text:
        return text
    if _HEX64.fullmatch(text) is None:
        _fail(path, "lowercase_sha256")
    return text


def _string_list(value: Any, *, path: str) -> list[str]:
    if type(value) is not list:
        _fail(path, "exact_list_of_strings")
    for index, item in enumerate(value):
        _exact_str(item, path=f"{path}[{index}]")
    return value


def _mapping(
    value: Any,
    fields: frozenset[str],
    *,
    path: str,
    required: frozenset[str] | None = None,
) -> Mapping[str, Any]:
    if type(value) is not dict:
        _fail(path, "exact_mapping")
    for key in value:
        if type(key) is not str:
            _fail(path, "string_keys")
    unknown = set(value) - fields
    if unknown:
        raise OfficialFactAdapterV3Error(
            f"typed_schema:{path}:unknown_fields:{','.join(sorted(unknown))}"
        )
    missing = (required or frozenset()) - set(value)
    if missing:
        raise OfficialFactAdapterV3Error(
            f"typed_schema:{path}:missing_fields:{','.join(sorted(missing))}"
        )
    return value


def _canonical_hash(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise OfficialFactAdapterV3Error("snapshot_material_not_canonical_json") from exc
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def verify_canonical_clock_v1_retirement() -> dict[str, Any]:
    """Verify the one pinned historical Clock V1 artifact; accepts no paths."""

    for path, size, digest, label in (
        (
            CANONICAL_CLOCK_V1_RETIREMENT,
            CANONICAL_CLOCK_V1_RETIREMENT_BYTES,
            CANONICAL_CLOCK_V1_RETIREMENT_SHA256,
            "retirement",
        ),
        (
            CANONICAL_CLOCK_V1_DATABASE,
            CANONICAL_CLOCK_V1_DATABASE_BYTES,
            CANONICAL_CLOCK_V1_DATABASE_SHA256,
            "database",
        ),
    ):
        if not path.is_file() or path.stat().st_size != size:
            raise OfficialFactAdapterV3Error(f"canonical_clock_v1_{label}_size_mismatch")
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise OfficialFactAdapterV3Error(f"canonical_clock_v1_{label}_hash_mismatch")
    try:
        record = json.loads(CANONICAL_CLOCK_V1_RETIREMENT.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise OfficialFactAdapterV3Error("canonical_clock_v1_retirement_invalid") from exc
    if type(record) is not dict or record.get("status") != "retired_terminal":
        raise OfficialFactAdapterV3Error("canonical_clock_v1_retirement_not_terminal")
    database = record.get("database")
    if type(database) is not dict:
        raise OfficialFactAdapterV3Error("canonical_clock_v1_retirement_database_missing")
    if (
        database.get("sha256") != CANONICAL_CLOCK_V1_DATABASE_SHA256
        or database.get("byte_length") != CANONICAL_CLOCK_V1_DATABASE_BYTES
        or database.get("preserve_bytes") is not True
        or database.get("read_only_historical_evidence") is not True
    ):
        raise OfficialFactAdapterV3Error("canonical_clock_v1_retirement_claim_mismatch")
    connection = sqlite3.connect(
        f"file:{CANONICAL_CLOCK_V1_DATABASE.resolve().as_posix()}?mode=ro", uri=True
    )
    try:
        connection.execute("PRAGMA query_only=ON")
        quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        counts = {
            table: int(
                connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            for table in CANONICAL_CLOCK_V1_ROW_COUNTS
        }
    finally:
        connection.close()
    if quick_check != "ok" or counts != CANONICAL_CLOCK_V1_ROW_COUNTS:
        raise OfficialFactAdapterV3Error("canonical_clock_v1_integrity_mismatch")
    return {
        "database_sha256": CANONICAL_CLOCK_V1_DATABASE_SHA256,
        "retirement_sha256": CANONICAL_CLOCK_V1_RETIREMENT_SHA256,
        "quick_check": quick_check,
        "row_counts": counts,
    }


def _rule_string(value: Any, path: str) -> None:
    _exact_str(value, path=path)


def _rule_nonempty_string(value: Any, path: str) -> None:
    _exact_str(value, path=path, nonempty=True)


def _rule_optional_string(value: Any, path: str) -> None:
    _optional_str(value, path=path)


def _rule_bool(value: Any, path: str) -> None:
    _exact_bool(value, path=path)


def _rule_int(value: Any, path: str) -> None:
    _exact_int(value, path=path)


def _rule_number(value: Any, path: str) -> None:
    _finite_number(value, path=path)


def _rule_optional_number(value: Any, path: str) -> None:
    _finite_number(value, path=path, nullable=True)


def _rule_timestamp(value: Any, path: str) -> None:
    _timestamp(value, path=path)


def _rule_optional_timestamp(value: Any, path: str) -> None:
    _timestamp(value, path=path, nullable=True)


def _rule_hash(value: Any, path: str) -> None:
    _hash64(value, path=path)


def _rule_hash_or_empty(value: Any, path: str) -> None:
    _hash64(value, path=path, allow_empty=True)


def _rule_string_list(value: Any, path: str) -> None:
    _string_list(value, path=path)


Rule = Callable[[Any, str], None]

_FACT_COMMON_RULES: dict[str, Rule] = {
    "fact_id": _rule_nonempty_string,
    "currency": _rule_nonempty_string,
    "fact_type": _rule_nonempty_string,
    "series_id": _rule_string,
    "consensus_value": _rule_optional_number,
    "consensus_causal": _rule_bool,
    "published_at_utc": _rule_optional_timestamp,
    "first_seen_at_utc": _rule_optional_timestamp,
    "retrieved_at_utc": _rule_optional_timestamp,
    "effective_from_utc": _rule_timestamp,
    "source_id": _rule_string,
    "source_name": _rule_string,
    "source_contract_id": _rule_string,
    "source_cohort_id": _rule_string,
    "raw_payload_sha256": _rule_hash,
    "evidence_class": _rule_nonempty_string,
    "degradation_reasons": _rule_string_list,
    "direction_policy": _rule_nonempty_string,
}
_FACT_EXTRA_RULES: dict[str, dict[str, Rule]] = {
    "official_macro_actual": {
        "event_name": _rule_string,
        "release_key": _rule_string,
        "reference_period": _rule_string,
        "reference_date": _rule_string,
        "unit": _rule_string,
        "importance": _rule_string,
        "actual_value": _rule_optional_number,
        "previous_value": _rule_optional_number,
        "revised_previous_value": _rule_optional_number,
        "noncausal_consensus_value": _rule_optional_number,
        "surprise_raw": _rule_optional_number,
        "standardized_surprise": _rule_optional_number,
        "noncanonical_ledger_standardized_surprise": _rule_optional_number,
        "scheduled_utc": _rule_optional_timestamp,
        "ledger_recorded_at_utc": _rule_timestamp,
        "source_event_id": _rule_string,
        "source_url": _rule_string,
        "collector_contract_id": _rule_string,
        "collector_cohort_id": _rule_string,
    },
    "official_daily_rate": {
        "rate_date": lambda value, path: _date(value, path=path),
        "rate_pct": _rule_number,
        "observation_kind": _rule_string,
    },
    "internal_macro_expectation": {
        "event_name": _rule_string,
        "unit": _rule_string,
        "expected_value": _rule_number,
        "training_episode_count": _rule_int,
        "training_cutoff_utc": _rule_timestamp,
        "expires_utc": _rule_timestamp,
    },
    "official_policy_document_context": {
        "event_name": _rule_string,
        "text_excerpt": _rule_string,
        "payload_ref": _rule_string,
        "source_url": _rule_string,
    },
}
FACT_RULES_BY_TYPE = {
    fact_type: {**_FACT_COMMON_RULES, **_FACT_EXTRA_RULES[fact_type]}
    for fact_type in OFFICIAL_FACT_TYPES
}
for _fact_type, _rules in FACT_RULES_BY_TYPE.items():
    if set(_rules) != set(OFFICIAL_FACT_FIELDS_BY_TYPE[_fact_type]):
        raise RuntimeError(f"V3 fact rule coverage mismatch for {_fact_type}")


def _validate_fact(row: Any, *, path: str, cutoff: dt.datetime) -> Mapping[str, Any]:
    if type(row) is not dict:
        _fail(path, "exact_mapping")
    fact_type = row.get("fact_type")
    if type(fact_type) is not str or fact_type not in OFFICIAL_FACT_TYPES:
        _fail(f"{path}.fact_type", "known_exact_string")
    rules = FACT_RULES_BY_TYPE[fact_type]
    value = _mapping(
        row,
        OFFICIAL_FACT_FIELDS_BY_TYPE[fact_type],
        path=path,
        required=frozenset(rules),
    )
    for key, rule in rules.items():
        rule(value[key], f"{path}.{key}")
    if value["currency"] not in CANONICAL_CURRENCIES:
        _fail(f"{path}.currency", "canonical_currency")
    if value["direction_policy"] != "abstain":
        _fail(f"{path}.direction_policy", "literal_abstain")
    if value["evidence_class"] not in OFFICIAL_FACT_EVIDENCE_CLASSES:
        _fail(f"{path}.evidence_class", "known_evidence_class")
    effective = _timestamp(value["effective_from_utc"], path=f"{path}.effective_from_utc")
    if effective is None or effective > cutoff:
        _fail(f"{path}.effective_from_utc", "known_no_later_than_cutoff")
    return value


_EVENT_RULES: dict[str, Rule] = {
    "event_id": _rule_nonempty_string,
    "event_version_id": _rule_nonempty_string,
    "currency": _rule_nonempty_string,
    "category": _rule_string,
    "scheduled_utc": _rule_timestamp,
    "schedule_window_end_utc": _rule_optional_timestamp,
    "timing_precision": _rule_string,
    "policy_event": _rule_bool,
    "policy_dependency": _rule_bool,
    "direct_event_currency": lambda value, path: (
        None if value is None else _exact_bool(value, path=path)
    ),
    "driver_currency": _rule_nonempty_string,
    "headline": _rule_string,
    "fact_type": _rule_nonempty_string,
    "known_from_snapshot_utc": _rule_timestamp,
    "ledger_effective_known_utc": _rule_timestamp,
    "clock_provenance_state": _rule_nonempty_string,
    "clock_snapshot_id": _rule_nonempty_string,
    "clock_source_contract_id": _rule_nonempty_string,
    "consensus_causal": _rule_bool,
    "evidence_class": _rule_nonempty_string,
    "degradation_reasons": _rule_string_list,
    "direction_policy": _rule_nonempty_string,
    "execution_eligible": _rule_bool,
    "can_place_orders": _rule_bool,
    "raw_payload_sha256": _rule_hash,
}
if set(_EVENT_RULES) != set(OFFICIAL_UPCOMING_EVENT_FIELDS):
    raise RuntimeError("V3 event rule coverage mismatch")


def _validate_event(row: Any, *, path: str, cutoff: dt.datetime) -> Mapping[str, Any]:
    value = _mapping(
        row,
        OFFICIAL_UPCOMING_EVENT_FIELDS,
        path=path,
        required=OFFICIAL_UPCOMING_EVENT_FIELDS,
    )
    for key, rule in _EVENT_RULES.items():
        rule(value[key], f"{path}.{key}")
    if value["currency"] not in CANONICAL_CURRENCIES or value["driver_currency"] not in CANONICAL_CURRENCIES:
        _fail(f"{path}.currency", "canonical_currency")
    if value["fact_type"] != "official_event_clock":
        _fail(f"{path}.fact_type", "literal_official_event_clock")
    if value["direction_policy"] != "abstain":
        _fail(f"{path}.direction_policy", "literal_abstain")
    if value["consensus_causal"] is not False:
        _fail(f"{path}.consensus_causal", "literal_false")
    if value["execution_eligible"] is not False or value["can_place_orders"] is not False:
        _fail(path, "execution_flags_false")
    scheduled = _timestamp(value["scheduled_utc"], path=f"{path}.scheduled_utc")
    known = _timestamp(value["ledger_effective_known_utc"], path=f"{path}.ledger_effective_known_utc")
    if scheduled is None or scheduled < cutoff or known is None or known > cutoff:
        _fail(path, "causal_event_time_order")
    return value


def _validate_source_health(value: Any, *, path: str) -> None:
    row = _mapping(
        value,
        OFFICIAL_SOURCE_HEALTH_FIELDS,
        path=path,
        required=OFFICIAL_SOURCE_HEALTH_FIELDS,
    )
    _exact_str(row["state"], path=f"{path}.state", nonempty=True)
    _timestamp(row["as_of_utc"], path=f"{path}.as_of_utc", nullable=True)
    counts = []
    for field in (
        "configured_source_count",
        "operational_source_count",
        "healthy_direct_source_count",
        "degraded_recent_direct_source_count",
    ):
        counts.append(_exact_int(row[field], path=f"{path}.{field}"))
    configured, operational, healthy, degraded = counts
    if operational > configured or healthy + degraded > operational:
        _fail(path, "reconciled_source_health_counts")
    issues = row["official_source_issues"]
    if type(issues) is not list:
        _fail(f"{path}.official_source_issues", "exact_list")
    for index, issue in enumerate(issues):
        issue_path = f"{path}.official_source_issues[{index}]"
        if type(issue) is str:
            continue
        item = _mapping(
            issue,
            OFFICIAL_SOURCE_ISSUE_FIELDS,
            path=issue_path,
            required=OFFICIAL_SOURCE_ISSUE_FIELDS,
        )
        for field in ("source_id", "role", "health_state", "runtime_status"):
            _exact_str(item[field], path=f"{issue_path}.{field}")
        for field in ("operational", "usable_recent_success"):
            _exact_bool(item[field], path=f"{issue_path}.{field}")


def _validate_global_gaps(value: Any, *, path: str) -> None:
    if type(value) is not list:
        _fail(path, "exact_list")
    for index, raw in enumerate(value):
        gap_path = f"{path}[{index}]"
        row = _mapping(
            raw,
            OFFICIAL_GLOBAL_GAP_FIELDS,
            path=gap_path,
            required=frozenset({"code"}),
        )
        _exact_str(row["code"], path=f"{gap_path}.code", nonempty=True)
        for field in ("artifact_name", "error_class"):
            if field in row:
                _exact_str(row[field], path=f"{gap_path}.{field}")
        for field in ("observation_age_sec", "maximum_observation_age_sec"):
            if field in row:
                _finite_number(row[field], path=f"{gap_path}.{field}", nullable=True, minimum=0.0)


def _validate_attestation(value: Any, *, path: str, require_complete: bool) -> None:
    required = OFFICIAL_CLOCK_ATTESTATION_FIELDS if require_complete else frozenset()
    row = _mapping(value, OFFICIAL_CLOCK_ATTESTATION_FIELDS, path=path, required=required)
    string_fields = ("state", "artifact_name", "status")
    bool_fields = (
        "attested",
        "timestamp_normalization_trusted",
        "host_clock_synchronized",
        "source_fresh",
    )
    for field in string_fields:
        if field in row:
            _exact_str(row[field], path=f"{path}.{field}", nonempty=True)
    if "generated_utc" in row:
        _timestamp(row["generated_utc"], path=f"{path}.generated_utc")
    if "payload_sha256" in row:
        _hash64(row["payload_sha256"], path=f"{path}.payload_sha256")
    for field in ("age_at_capture_sec", "maximum_age_sec"):
        if field in row:
            _finite_number(row[field], path=f"{path}.{field}", minimum=0.0)
    for field in bool_fields:
        if field in row:
            _exact_bool(row[field], path=f"{path}.{field}")
    if require_complete:
        if (
            row["state"] != "fresh_trusted"
            or row["status"] != "ok"
            or row["attested"] is not True
            or row["timestamp_normalization_trusted"] is not True
            or row["host_clock_synchronized"] is not True
            or row["source_fresh"] is not True
        ):
            _fail(path, "complete_fresh_trusted_attestation")


def _validate_provenance(value: Any, *, path: str) -> Mapping[str, Any]:
    row = _mapping(
        value,
        OFFICIAL_EVENT_CLOCK_PROVENANCE_FIELDS,
        path=path,
        required=frozenset(
            {
                "state",
                "schema_version",
                "contract_id",
                "clock_ready_for_cutoff",
                "fallback_used",
                "mutable_fallback_forbidden",
                "complete_snapshot_at_cutoff",
            }
        ),
    )
    for field in ("state", "schema_version", "contract_id"):
        _exact_str(row[field], path=f"{path}.{field}", nonempty=True)
    for field in ("fallback_used", "mutable_fallback_forbidden", "clock_ready_for_cutoff", "complete_snapshot_at_cutoff"):
        _exact_bool(row[field], path=f"{path}.{field}")
    ready = row["clock_ready_for_cutoff"]
    complete = row["complete_snapshot_at_cutoff"]
    if ready is not complete:
        _fail(path, "ready_complete_exact_match")
    if row["fallback_used"] is not False or row["mutable_fallback_forbidden"] is not True:
        _fail(path, "no_mutable_fallback")
    if row["schema_version"] != IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION or row["contract_id"] != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID:
        _fail(path, "clock_v2_identity")
    for field in ("snapshot_captured_utc", "clock_observed_utc", "source_generated_utc"):
        if field in row:
            _timestamp(row[field], path=f"{path}.{field}")
    if "snapshot_id" in row:
        text = _exact_str(row["snapshot_id"], path=f"{path}.snapshot_id", nonempty=True)
        if _CLOCK_SNAPSHOT_ID.fullmatch(text) is None:
            _fail(f"{path}.snapshot_id", "clock_snapshot_id")
    if "clock_observation_id" in row:
        text = _exact_str(row["clock_observation_id"], path=f"{path}.clock_observation_id", nonempty=True)
        if _CLOCK_OBSERVATION_ID.fullmatch(text) is None:
            _fail(f"{path}.clock_observation_id", "clock_observation_id")
    for field in ("events_sha256", "manifest_sha256"):
        if field in row:
            _hash64(row[field], path=f"{path}.{field}")
    for field in ("observation_age_sec", "maximum_observation_age_sec"):
        if field in row:
            _finite_number(row[field], path=f"{path}.{field}", nullable=True, minimum=0.0)
    if "degradation_reason" in row:
        _exact_str(row["degradation_reason"], path=f"{path}.degradation_reason", nonempty=True)
    if "clock_attestation" in row:
        _validate_attestation(row["clock_attestation"], path=f"{path}.clock_attestation", require_complete=ready)
    elif ready:
        _fail(f"{path}.clock_attestation", "required_complete_attestation")
    if ready:
        required_ready = {
            "snapshot_id",
            "snapshot_captured_utc",
            "clock_observation_id",
            "clock_observed_utc",
            "source_generated_utc",
            "events_sha256",
            "manifest_sha256",
            "clock_attestation",
            "observation_age_sec",
            "maximum_observation_age_sec",
        }
        missing = required_ready - set(row)
        if missing:
            raise OfficialFactAdapterV3Error(
                f"typed_schema:{path}:ready_provenance_missing:{','.join(sorted(missing))}"
            )
        if row["state"] != "immutable_v2_fresh_attested_at_cutoff":
            _fail(f"{path}.state", "ready_state")
    return row


def _macro_identity(row: Mapping[str, Any]) -> tuple[str, ...] | None:
    if row["fact_type"] != "official_macro_actual":
        return None
    reference = str(
        row.get("reference_period")
        or row.get("reference_date")
        or row.get("release_key")
        or row.get("fact_id")
        or ""
    )
    return (str(row["currency"]), str(row["series_id"]), reference, str(row["unit"]))


def validate_official_fact_v3_snapshot(snapshot: Mapping[str, Any]) -> None:
    """Validate the entire accepted V3 snapshot and all aggregate claims."""

    row = _mapping(
        snapshot,
        OFFICIAL_FACT_V3_TOP_LEVEL_FIELDS,
        path="root",
        required=OFFICIAL_FACT_V3_TOP_LEVEL_FIELDS,
    )
    if row["schema_version"] != ADAPTER_SCHEMA_VERSION or type(row["schema_version"]) is not int:
        _fail("root.schema_version", "literal_int_3")
    literal_strings = {
        "adapter_contract_id": ADAPTER_CONTRACT_ID,
        "parent_adapter_contract_id": PARENT_ADAPTER_CONTRACT_ID,
        "fact_count_semantics": "normalized_provenance_records_not_independent_events",
        "supported_execution_decision": "no_trade",
        "immutable_event_clock_schema_version": IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
        "immutable_event_clock_contract_id": IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
        "clock_dependency_policy": CLOCK_DEPENDENCY_POLICY,
    }
    for field, expected in literal_strings.items():
        _exact_str(row[field], path=f"root.{field}", nonempty=True)
        if row[field] != expected:
            _fail(f"root.{field}", f"literal_{expected}")
    cutoff = _timestamp(row["decision_cutoff_utc"], path="root.decision_cutoff_utc")
    assert cutoff is not None
    for field in (
        "currency_count",
        "fact_count",
        "macro_fact_record_count",
        "distinct_macro_observation_count",
        "upcoming_event_count",
        "causal_consensus_count",
        "quarantined_source_event_count",
    ):
        _exact_int(row[field], path=f"root.{field}")
    _finite_number(
        row["maximum_clock_observation_age_sec"],
        path="root.maximum_clock_observation_age_sec",
        minimum=0.0,
    )
    for field, expected in (
        ("research_only", True),
        ("execution_eligible", False),
        ("can_place_orders", False),
        ("can_promote", False),
        ("can_authorize", False),
    ):
        _exact_bool(row[field], path=f"root.{field}")
        if row[field] is not expected:
            _fail(f"root.{field}", f"literal_{str(expected).lower()}")
    _exact_str(row["status"], path="root.status", nonempty=True)
    if type(row["facts"]) is not list or type(row["upcoming_events"]) is not list:
        _fail("root.facts_or_events", "exact_lists")
    facts = [
        _validate_fact(raw, path=f"root.facts[{index}]", cutoff=cutoff)
        for index, raw in enumerate(row["facts"])
    ]
    events = [
        _validate_event(raw, path=f"root.upcoming_events[{index}]", cutoff=cutoff)
        for index, raw in enumerate(row["upcoming_events"])
    ]
    fact_ids = [fact["fact_id"] for fact in facts]
    if len(set(fact_ids)) != len(fact_ids):
        _fail("root.facts", "unique_fact_ids")
    event_ids = [(event["event_version_id"], event["currency"]) for event in events]
    if len(set(event_ids)) != len(event_ids):
        _fail("root.upcoming_events", "unique_event_version_currency")

    evidence = row["currency_evidence"]
    if type(evidence) is not dict:
        _fail("root.currency_evidence", "exact_mapping")
    if not evidence or any(type(key) is not str or key not in CANONICAL_CURRENCIES for key in evidence):
        _fail("root.currency_evidence", "nonempty_canonical_currency_map")
    if row["currency_count"] != len(evidence):
        _fail("root.currency_count", "reconciled_currency_count")
    if any(fact["currency"] not in evidence for fact in facts) or any(event["currency"] not in evidence for event in events):
        _fail("root.currency_evidence", "covers_all_fact_and_event_currencies")
    total_summary_facts = total_summary_events = total_summary_consensus = 0
    total_summary_macro = total_summary_distinct = 0
    for currency, raw_summary in evidence.items():
        path = f"root.currency_evidence.{currency}"
        summary = _mapping(
            raw_summary,
            OFFICIAL_CURRENCY_EVIDENCE_FIELDS,
            path=path,
            required=OFFICIAL_CURRENCY_EVIDENCE_FIELDS,
        )
        _exact_str(summary["currency"], path=f"{path}.currency", nonempty=True)
        if summary["currency"] != currency:
            _fail(f"{path}.currency", "map_key_identity")
        for field in (
            "fact_count",
            "macro_fact_record_count",
            "distinct_macro_observation_count",
            "causal_consensus_count",
            "upcoming_event_count",
        ):
            _exact_int(summary[field], path=f"{path}.{field}")
        fact_types = _string_list(summary["fact_types"], path=f"{path}.fact_types")
        if len(set(fact_types)) != len(fact_types) or set(fact_types) - OFFICIAL_FACT_TYPES:
            _fail(f"{path}.fact_types", "unique_known_fact_types")
        _string_list(summary["missing_or_degraded"], path=f"{path}.missing_or_degraded")
        _validate_source_health(summary["source_health"], path=f"{path}.source_health")
        currency_facts = [fact for fact in facts if fact["currency"] == currency]
        currency_events = [event for event in events if event["currency"] == currency]
        macro = [fact for fact in currency_facts if fact["fact_type"] == "official_macro_actual"]
        distinct = {_macro_identity(fact) for fact in macro}
        expected = {
            "fact_count": len(currency_facts),
            "macro_fact_record_count": len(macro),
            "distinct_macro_observation_count": len(distinct),
            "causal_consensus_count": sum(fact["consensus_causal"] is True for fact in currency_facts),
            "upcoming_event_count": len(currency_events),
        }
        for field, count in expected.items():
            if summary[field] != count:
                _fail(f"{path}.{field}", "reconciled_collection_count")
        if fact_types != sorted({str(fact["fact_type"]) for fact in currency_facts}):
            _fail(f"{path}.fact_types", "reconciled_fact_types")
        total_summary_facts += summary["fact_count"]
        total_summary_events += summary["upcoming_event_count"]
        total_summary_consensus += summary["causal_consensus_count"]
        total_summary_macro += summary["macro_fact_record_count"]
        total_summary_distinct += summary["distinct_macro_observation_count"]

    expected_totals = {
        "fact_count": len(facts),
        "upcoming_event_count": len(events),
        "causal_consensus_count": sum(fact["consensus_causal"] is True for fact in facts),
        "macro_fact_record_count": sum(fact["fact_type"] == "official_macro_actual" for fact in facts),
        "distinct_macro_observation_count": len(
            {identity for fact in facts if (identity := _macro_identity(fact)) is not None}
        ),
    }
    summary_totals = {
        "fact_count": total_summary_facts,
        "upcoming_event_count": total_summary_events,
        "causal_consensus_count": total_summary_consensus,
        "macro_fact_record_count": total_summary_macro,
        "distinct_macro_observation_count": total_summary_distinct,
    }
    for field, count in expected_totals.items():
        if row[field] != count or summary_totals[field] != count:
            _fail(f"root.{field}", "reconciled_top_and_nested_counts")

    _validate_global_gaps(row["global_gaps"], path="root.global_gaps")
    provenance = _validate_provenance(row["event_clock_provenance"], path="root.event_clock_provenance")
    if not provenance["clock_ready_for_cutoff"] and events:
        _fail("root.upcoming_events", "empty_when_clock_not_ready")
    if provenance["clock_ready_for_cutoff"]:
        for index, event in enumerate(events):
            if event["clock_snapshot_id"] != provenance["snapshot_id"]:
                _fail(f"root.upcoming_events[{index}].clock_snapshot_id", "provenance_snapshot_match")
            if event["clock_source_contract_id"] != provenance["contract_id"]:
                _fail(f"root.upcoming_events[{index}].clock_source_contract_id", "provenance_contract_match")

    clock = _mapping(row["clock"], OFFICIAL_CLOCK_FIELDS, path="root.clock", required=frozenset({"state", "trusted_for_prospective_evidence"}))
    _exact_str(clock["state"], path="root.clock.state", nonempty=True)
    _exact_bool(clock["trusted_for_prospective_evidence"], path="root.clock.trusted_for_prospective_evidence")
    for field in ("source_artifact",):
        if field in clock:
            _exact_str(clock[field], path=f"root.clock.{field}")
    if "as_of_utc" in clock:
        _timestamp(clock["as_of_utc"], path="root.clock.as_of_utc", nullable=True)
    if "host_clock_synchronized" in clock:
        _exact_bool(clock["host_clock_synchronized"], path="root.clock.host_clock_synchronized")
    if "reasons" in clock:
        _string_list(clock["reasons"], path="root.clock.reasons")

    intraday = _mapping(row["intraday_rates"], OFFICIAL_INTRADAY_RATE_FIELDS, path="root.intraday_rates", required=frozenset({"state", "connected", "as_of_utc", "contract", "blocker"}))
    _exact_str(intraday["state"], path="root.intraday_rates.state", nonempty=True)
    _exact_bool(intraday["connected"], path="root.intraday_rates.connected")
    _timestamp(intraday["as_of_utc"], path="root.intraday_rates.as_of_utc", nullable=True)
    _exact_str(intraday["blocker"], path="root.intraday_rates.blocker")
    contract = _mapping(intraday["contract"], OFFICIAL_INTRADAY_RATE_CONTRACT_FIELDS, path="root.intraday_rates.contract", required=OFFICIAL_INTRADAY_RATE_CONTRACT_FIELDS)
    _string_list(contract["required_fields"], path="root.intraday_rates.contract.required_fields")
    for field in ("causal_rule", "no_data_policy", "material_change_policy"):
        _exact_str(contract[field], path=f"root.intraday_rates.contract.{field}", nonempty=True)

    expected_status = "degraded" if row["global_gaps"] or any(summary["missing_or_degraded"] for summary in evidence.values()) else "ready"
    if row["status"] != expected_status:
        _fail("root.status", "reconciled_status")
    material = {key: value for key, value in row.items() if key != "snapshot_id"}
    expected_id = "official_fact_v3_snapshot_" + _canonical_hash(material)[:24]
    _exact_str(row["snapshot_id"], path="root.snapshot_id", nonempty=True)
    if row["snapshot_id"] != expected_id:
        raise OfficialFactAdapterV3Error("official_fact_v3_snapshot_id_mismatch")


class OfficialFactAdapterV3(OfficialFactAdapterV2):
    """Generate a V3 snapshot without exposing any registration capability."""

    def __init__(
        self,
        paths: OfficialFactPathsV3 | None = None,
        *,
        sqlite_timeout_sec: float = 5.0,
        maximum_rows_per_input: int = 5_000,
        timely_release_latency_sec: float = 300.0,
        maximum_clock_observation_age_sec: float = 300.0,
    ) -> None:
        for value, name in (
            (sqlite_timeout_sec, "sqlite_timeout_sec"),
            (timely_release_latency_sec, "timely_release_latency_sec"),
            (maximum_clock_observation_age_sec, "maximum_clock_observation_age_sec"),
        ):
            _finite_number(value, path=f"constructor.{name}", minimum=0.0)
        _exact_int(maximum_rows_per_input, path="constructor.maximum_rows_per_input", minimum=1)
        verify_canonical_clock_v1_retirement()
        actual_paths = paths or OfficialFactPathsV3()
        clock_path = actual_paths.immutable_event_clock_db
        if clock_path is not None and clock_path.resolve() == CANONICAL_CLOCK_V1_DATABASE.resolve():
            raise OfficialFactAdapterV3Error("clock_v1_cannot_feed_adapter_v3")
        super().__init__(
            paths=actual_paths,
            sqlite_timeout_sec=float(sqlite_timeout_sec),
            maximum_rows_per_input=maximum_rows_per_input,
            timely_release_latency_sec=float(timely_release_latency_sec),
            maximum_clock_observation_age_sec=float(maximum_clock_observation_age_sec),
        )

    def as_of(
        self,
        decision_cutoff_utc: str | dt.datetime,
        *,
        currencies: Sequence[str] = CANONICAL_CURRENCIES,
    ) -> dict[str, Any]:
        snapshot = super().as_of(decision_cutoff_utc, currencies=currencies)
        snapshot["schema_version"] = ADAPTER_SCHEMA_VERSION
        snapshot["adapter_contract_id"] = ADAPTER_CONTRACT_ID
        snapshot["parent_adapter_contract_id"] = PARENT_ADAPTER_CONTRACT_ID
        snapshot["clock_dependency_policy"] = CLOCK_DEPENDENCY_POLICY
        snapshot["can_promote"] = False
        snapshot["can_authorize"] = False
        material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
        snapshot["snapshot_id"] = "official_fact_v3_snapshot_" + _canonical_hash(material)[:24]
        validate_official_fact_v3_snapshot(snapshot)
        return snapshot


__all__ = [
    "ADAPTER_CONTRACT_ID",
    "ADAPTER_SCHEMA_VERSION",
    "CANONICAL_CLOCK_V1_DATABASE_SHA256",
    "CANONICAL_CLOCK_V1_RETIREMENT_SHA256",
    "CLOCK_DEPENDENCY_POLICY",
    "FACT_BASIS_ELIGIBILITY_CONTRACT_ID",
    "OFFICIAL_FACT_V3_TOP_LEVEL_FIELDS",
    "OfficialFactAdapterV3",
    "OfficialFactAdapterV3Error",
    "OfficialFactPathsV3",
    "validate_official_fact_v3_snapshot",
    "verify_canonical_clock_v1_retirement",
]
