"""OfficialFactAdapter V2 with an explicit immutable Clock V2 dependency.

The V1 adapter and Clock V1 database are frozen historical artifacts.  This
successor reuses the bounded non-clock normalization code from the frozen V1
adapter, but replaces its clock boundary with a strict V2-only reader.  A
missing, V1, malformed, or backdated clock is rejected; mutable event-preflight
fallback is deliberately unavailable.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .immutable_event_clock import (
    CONTRACT_ID as IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
    SCHEMA_VERSION as IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
    EventClockLedgerError,
    reconstruct_as_of as reconstruct_event_clock_as_of,
)
from .official_fact_adapter import (
    CANONICAL_CURRENCIES,
    DATA_ROOT,
    OfficialFactAdapter as OfficialFactAdapterV1,
    OfficialFactPaths,
    _iso,
    _parse_utc,
    _stable_hash,
)


ADAPTER_SCHEMA_VERSION = 2
ADAPTER_CONTRACT_ID = "official_fact_adapter_v2_20260817"
PARENT_ADAPTER_CONTRACT_ID = "official_fact_adapter_v1_20260817"
DEFAULT_MAXIMUM_CLOCK_OBSERVATION_AGE_SEC = 300.0
REQUIRED_CLOCK_ATTESTATION_STATE = "fresh_trusted"


class OfficialFactAdapterV2Error(RuntimeError):
    """Raised when the V2 causal boundary cannot be proved exactly."""


@dataclass(frozen=True)
class OfficialFactPathsV2(OfficialFactPaths):
    """V2 uses only the dedicated V2 immutable-clock database."""

    immutable_event_clock_db: Path | None = (
        DATA_ROOT / "research_ledgers" / "immutable_event_clock_v2.sqlite"
    )


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


OFFICIAL_FACT_TYPES = frozenset(
    {
        "official_macro_actual",
        "official_daily_rate",
        "internal_macro_expectation",
        "official_policy_document_context",
    }
)
OFFICIAL_FACT_EVIDENCE_CLASSES = frozenset(
    {
        "bootstrap_context",
        "clock_inconsistent",
        "inferred_publication_context",
        "internal_baseline_nonconsensus",
        "late_observation_context",
        "policy_context_only",
        "prospective_causal",
        "prospective_daily_context",
        "publication_time_unavailable",
        "unverified_source_context",
    }
)

OFFICIAL_FACT_V2_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "adapter_contract_id",
        "parent_adapter_contract_id",
        "snapshot_id",
        "decision_cutoff_utc",
        "currency_count",
        "fact_count",
        "fact_count_semantics",
        "macro_fact_record_count",
        "distinct_macro_observation_count",
        "upcoming_event_count",
        "causal_consensus_count",
        "facts",
        "upcoming_events",
        "event_clock_provenance",
        "currency_evidence",
        "global_gaps",
        "clock",
        "intraday_rates",
        "quarantined_source_event_count",
        "status",
        "research_only",
        "execution_eligible",
        "can_place_orders",
        "supported_execution_decision",
        "immutable_event_clock_schema_version",
        "immutable_event_clock_contract_id",
        "clock_dependency_policy",
        "maximum_clock_observation_age_sec",
        "can_promote",
        "can_authorize",
    }
)

_FACT_COMMON_FIELDS = frozenset(
    {
        "fact_id",
        "currency",
        "fact_type",
        "series_id",
        "consensus_value",
        "consensus_causal",
        "published_at_utc",
        "first_seen_at_utc",
        "retrieved_at_utc",
        "effective_from_utc",
        "source_id",
        "source_name",
        "source_contract_id",
        "source_cohort_id",
        "raw_payload_sha256",
        "evidence_class",
        "degradation_reasons",
        "direction_policy",
    }
)
OFFICIAL_FACT_FIELDS_BY_TYPE = {
    "official_macro_actual": frozenset(
        {
            *_FACT_COMMON_FIELDS,
            "event_name",
            "release_key",
            "reference_period",
            "reference_date",
            "unit",
            "importance",
            "actual_value",
            "previous_value",
            "revised_previous_value",
            "noncausal_consensus_value",
            "surprise_raw",
            "standardized_surprise",
            "noncanonical_ledger_standardized_surprise",
            "scheduled_utc",
            "ledger_recorded_at_utc",
            "source_event_id",
            "source_url",
            "collector_contract_id",
            "collector_cohort_id",
        }
    ),
    "official_daily_rate": frozenset(
        {*_FACT_COMMON_FIELDS, "rate_date", "rate_pct", "observation_kind"}
    ),
    "internal_macro_expectation": frozenset(
        {
            *_FACT_COMMON_FIELDS,
            "event_name",
            "unit",
            "expected_value",
            "training_episode_count",
            "training_cutoff_utc",
            "expires_utc",
        }
    ),
    "official_policy_document_context": frozenset(
        {
            *_FACT_COMMON_FIELDS,
            "event_name",
            "text_excerpt",
            "payload_ref",
            "source_url",
        }
    ),
}

OFFICIAL_UPCOMING_EVENT_FIELDS = frozenset(
    {
        "event_id",
        "event_version_id",
        "currency",
        "category",
        "scheduled_utc",
        "schedule_window_end_utc",
        "timing_precision",
        "policy_event",
        "policy_dependency",
        "direct_event_currency",
        "driver_currency",
        "headline",
        "fact_type",
        "known_from_snapshot_utc",
        "ledger_effective_known_utc",
        "clock_provenance_state",
        "clock_snapshot_id",
        "clock_source_contract_id",
        "consensus_causal",
        "evidence_class",
        "degradation_reasons",
        "direction_policy",
        "execution_eligible",
        "can_place_orders",
        "raw_payload_sha256",
    }
)
OFFICIAL_CURRENCY_EVIDENCE_FIELDS = frozenset(
    {
        "currency",
        "fact_count",
        "macro_fact_record_count",
        "distinct_macro_observation_count",
        "fact_types",
        "causal_consensus_count",
        "upcoming_event_count",
        "source_health",
        "missing_or_degraded",
    }
)
OFFICIAL_SOURCE_HEALTH_FIELDS = frozenset(
    {
        "state",
        "as_of_utc",
        "configured_source_count",
        "operational_source_count",
        "healthy_direct_source_count",
        "degraded_recent_direct_source_count",
        "official_source_issues",
    }
)
OFFICIAL_SOURCE_ISSUE_FIELDS = frozenset(
    {
        "source_id",
        "role",
        "health_state",
        "runtime_status",
        "operational",
        "usable_recent_success",
    }
)
OFFICIAL_GLOBAL_GAP_FIELDS = frozenset(
    {
        "code",
        "artifact_name",
        "error_class",
        "observation_age_sec",
        "maximum_observation_age_sec",
    }
)
OFFICIAL_CLOCK_FIELDS = frozenset(
    {
        "state",
        "as_of_utc",
        "source_artifact",
        "trusted_for_prospective_evidence",
        "host_clock_synchronized",
        "reasons",
    }
)
OFFICIAL_INTRADAY_RATE_FIELDS = frozenset(
    {"state", "connected", "as_of_utc", "contract", "blocker"}
)
OFFICIAL_INTRADAY_RATE_CONTRACT_FIELDS = frozenset(
    {"required_fields", "causal_rule", "no_data_policy", "material_change_policy"}
)
OFFICIAL_EVENT_CLOCK_PROVENANCE_FIELDS = frozenset(
    {
        "state",
        "schema_version",
        "contract_id",
        "snapshot_id",
        "snapshot_captured_utc",
        "clock_observation_id",
        "clock_observed_utc",
        "source_generated_utc",
        "events_sha256",
        "manifest_sha256",
        "clock_attestation",
        "clock_ready_for_cutoff",
        "observation_age_sec",
        "maximum_observation_age_sec",
        "fallback_used",
        "mutable_fallback_forbidden",
        "complete_snapshot_at_cutoff",
        "degradation_reason",
    }
)
OFFICIAL_CLOCK_ATTESTATION_FIELDS = frozenset(
    {
        "state",
        "attested",
        "artifact_name",
        "generated_utc",
        "age_at_capture_sec",
        "payload_sha256",
        "maximum_age_sec",
        "status",
        "timestamp_normalization_trusted",
        "host_clock_synchronized",
        "source_fresh",
    }
)


def _closed_mapping(
    value: Any,
    allowed: frozenset[str],
    *,
    path: str,
    required: frozenset[str] = frozenset(),
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OfficialFactAdapterV2Error(f"closed_schema_mapping_required:{path}")
    if any(not isinstance(key, str) for key in value):
        raise OfficialFactAdapterV2Error(f"closed_schema_non_string_key:{path}")
    keys = set(value)
    unknown = sorted(keys - allowed)
    if unknown:
        raise OfficialFactAdapterV2Error(
            f"closed_schema_unknown_fields:{path}:{','.join(unknown)}"
        )
    missing = sorted(required - keys)
    if missing:
        raise OfficialFactAdapterV2Error(
            f"closed_schema_missing_fields:{path}:{','.join(missing)}"
        )
    return value


def _string_list(value: Any, *, path: str) -> None:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise OfficialFactAdapterV2Error(f"closed_schema_string_list_required:{path}")


def validate_official_source_health(value: Any, *, path: str) -> None:
    row = _closed_mapping(value, OFFICIAL_SOURCE_HEALTH_FIELDS, path=path)
    issues = row.get("official_source_issues", [])
    if not isinstance(issues, list):
        raise OfficialFactAdapterV2Error(f"closed_schema_list_required:{path}.official_source_issues")
    for index, issue in enumerate(issues):
        if isinstance(issue, str):
            continue
        _closed_mapping(
            issue,
            OFFICIAL_SOURCE_ISSUE_FIELDS,
            path=f"{path}.official_source_issues[{index}]",
        )


def validate_official_global_gaps(value: Any, *, path: str = "root.global_gaps") -> None:
    if not isinstance(value, list):
        raise OfficialFactAdapterV2Error(f"closed_schema_list_required:{path}")
    for index, gap in enumerate(value):
        _closed_mapping(
            gap,
            OFFICIAL_GLOBAL_GAP_FIELDS,
            path=f"{path}[{index}]",
            required=frozenset({"code"}),
        )


def validate_official_event_clock_provenance(
    value: Any, *, path: str = "root.event_clock_provenance"
) -> None:
    row = _closed_mapping(
        value,
        OFFICIAL_EVENT_CLOCK_PROVENANCE_FIELDS,
        path=path,
        required=frozenset(
            {
                "state",
                "schema_version",
                "contract_id",
                "fallback_used",
                "clock_ready_for_cutoff",
                "complete_snapshot_at_cutoff",
            }
        ),
    )
    if "clock_attestation" in row:
        _closed_mapping(
            row["clock_attestation"],
            OFFICIAL_CLOCK_ATTESTATION_FIELDS,
            path=f"{path}.clock_attestation",
        )


def _validate_fact(row: Any, *, index: int) -> None:
    path = f"root.facts[{index}]"
    if not isinstance(row, Mapping):
        raise OfficialFactAdapterV2Error(f"closed_schema_mapping_required:{path}")
    fact_type = str(row.get("fact_type") or "")
    if fact_type not in OFFICIAL_FACT_TYPES:
        raise OfficialFactAdapterV2Error(f"closed_schema_unknown_fact_type:{path}")
    _closed_mapping(
        row,
        OFFICIAL_FACT_FIELDS_BY_TYPE[fact_type],
        path=path,
        required=frozenset(
            {
                "fact_id",
                "currency",
                "fact_type",
                "evidence_class",
                "effective_from_utc",
                "consensus_causal",
                "direction_policy",
            }
        ),
    )
    if row.get("direction_policy") != "abstain":
        raise OfficialFactAdapterV2Error(f"non_abstaining_direction_policy:{path}")
    evidence_class = str(row.get("evidence_class") or "")
    if evidence_class not in OFFICIAL_FACT_EVIDENCE_CLASSES:
        raise OfficialFactAdapterV2Error(f"closed_schema_unknown_evidence_class:{path}")
    if "degradation_reasons" in row:
        _string_list(row["degradation_reasons"], path=f"{path}.degradation_reasons")
    for key, child in row.items():
        if key != "degradation_reasons" and isinstance(child, (Mapping, list, tuple)):
            raise OfficialFactAdapterV2Error(
                f"closed_schema_nested_value_forbidden:{path}.{key}"
            )


def _validate_upcoming_event(row: Any, *, index: int) -> None:
    path = f"root.upcoming_events[{index}]"
    value = _closed_mapping(
        row,
        OFFICIAL_UPCOMING_EVENT_FIELDS,
        path=path,
        required=frozenset({"event_id", "currency", "scheduled_utc"}),
    )
    if "direction_policy" in value and value.get("direction_policy") != "abstain":
        raise OfficialFactAdapterV2Error(f"non_abstaining_direction_policy:{path}")
    for field in ("execution_eligible", "can_place_orders"):
        if field in value and value.get(field) is not False:
            raise OfficialFactAdapterV2Error(f"positive_execution_surface:{path}.{field}")
    if "degradation_reasons" in value:
        _string_list(
            value["degradation_reasons"], path=f"{path}.degradation_reasons"
        )
    for key, child in value.items():
        if key != "degradation_reasons" and isinstance(child, (Mapping, list, tuple)):
            raise OfficialFactAdapterV2Error(
                f"closed_schema_nested_value_forbidden:{path}.{key}"
            )


def _validate_closed_official_fact_snapshot(snapshot: Mapping[str, Any]) -> None:
    _closed_mapping(
        snapshot,
        OFFICIAL_FACT_V2_TOP_LEVEL_FIELDS,
        path="root",
        required=frozenset(
            {
                "schema_version",
                "adapter_contract_id",
                "parent_adapter_contract_id",
                "snapshot_id",
                "decision_cutoff_utc",
                "fact_count",
                "upcoming_event_count",
                "causal_consensus_count",
                "facts",
                "upcoming_events",
                "event_clock_provenance",
                "currency_evidence",
                "global_gaps",
                "intraday_rates",
                "research_only",
                "execution_eligible",
                "can_place_orders",
                "supported_execution_decision",
                "immutable_event_clock_schema_version",
                "immutable_event_clock_contract_id",
                "clock_dependency_policy",
                "maximum_clock_observation_age_sec",
                "can_promote",
                "can_authorize",
            }
        ),
    )
    facts = snapshot.get("facts")
    events = snapshot.get("upcoming_events")
    if not isinstance(facts, list):
        raise OfficialFactAdapterV2Error("closed_schema_list_required:root.facts")
    if not isinstance(events, list):
        raise OfficialFactAdapterV2Error("closed_schema_list_required:root.upcoming_events")
    for index, row in enumerate(facts):
        _validate_fact(row, index=index)
    for index, row in enumerate(events):
        _validate_upcoming_event(row, index=index)

    evidence = snapshot.get("currency_evidence")
    if not isinstance(evidence, Mapping):
        raise OfficialFactAdapterV2Error("closed_schema_mapping_required:root.currency_evidence")
    for currency, raw in evidence.items():
        if not isinstance(currency, str) or currency not in CANONICAL_CURRENCIES:
            raise OfficialFactAdapterV2Error(
                f"closed_schema_unknown_currency:root.currency_evidence.{currency}"
            )
        path = f"root.currency_evidence.{currency}"
        row = _closed_mapping(raw, OFFICIAL_CURRENCY_EVIDENCE_FIELDS, path=path)
        if "fact_types" in row:
            _string_list(row["fact_types"], path=f"{path}.fact_types")
            if not set(row["fact_types"]).issubset(OFFICIAL_FACT_TYPES):
                raise OfficialFactAdapterV2Error(f"closed_schema_unknown_fact_type:{path}.fact_types")
        if "missing_or_degraded" in row:
            _string_list(row["missing_or_degraded"], path=f"{path}.missing_or_degraded")
        if "source_health" in row:
            validate_official_source_health(row["source_health"], path=f"{path}.source_health")

    validate_official_global_gaps(snapshot.get("global_gaps"))
    validate_official_event_clock_provenance(snapshot.get("event_clock_provenance"))
    if "clock" in snapshot:
        clock = _closed_mapping(snapshot["clock"], OFFICIAL_CLOCK_FIELDS, path="root.clock")
        if "reasons" in clock:
            _string_list(clock["reasons"], path="root.clock.reasons")
    intraday = _closed_mapping(
        snapshot.get("intraday_rates"),
        OFFICIAL_INTRADAY_RATE_FIELDS,
        path="root.intraday_rates",
        required=frozenset({"connected"}),
    )
    if "contract" in intraday:
        contract = _closed_mapping(
            intraday["contract"],
            OFFICIAL_INTRADAY_RATE_CONTRACT_FIELDS,
            path="root.intraday_rates.contract",
        )
        if "required_fields" in contract:
            _string_list(
                contract["required_fields"],
                path="root.intraday_rates.contract.required_fields",
            )


class OfficialFactAdapterV2(OfficialFactAdapterV1):
    """Build a bounded research snapshot from exact Clock V2 knowledge."""

    def __init__(
        self,
        paths: OfficialFactPathsV2 | None = None,
        *,
        sqlite_timeout_sec: float = 5.0,
        maximum_rows_per_input: int = 5_000,
        timely_release_latency_sec: float = 300.0,
        maximum_clock_observation_age_sec: float = (
            DEFAULT_MAXIMUM_CLOCK_OBSERVATION_AGE_SEC
        ),
    ) -> None:
        super().__init__(
            paths=paths or OfficialFactPathsV2(),
            sqlite_timeout_sec=sqlite_timeout_sec,
            maximum_rows_per_input=maximum_rows_per_input,
            timely_release_latency_sec=timely_release_latency_sec,
        )
        self.maximum_clock_observation_age_sec = max(
            0.0, min(float(maximum_clock_observation_age_sec), 3600.0)
        )

    def _clock_freshness(
        self,
        snapshot: Mapping[str, Any],
        *,
        cutoff: dt.datetime,
    ) -> dict[str, Any]:
        observation_id = str(snapshot.get("clock_observation_id") or "")
        observed = _parse_utc(snapshot.get("clock_observed_utc"))
        observation_generated = _parse_utc(
            snapshot.get("clock_observation_source_generated_utc")
        )
        snapshot_captured = _parse_utc(snapshot.get("snapshot_captured_utc"))
        snapshot_generated = _parse_utc(snapshot.get("source_generated_utc"))
        attestation = snapshot.get("clock_attestation")
        if not observation_id or observed is None or observation_generated is None:
            return {
                "ready": False,
                "state": "immutable_v2_observation_unavailable_at_cutoff",
                "reason": "latest_clock_observation_missing",
                "observation_age_sec": None,
            }
        if snapshot_captured is None or snapshot_generated is None:
            return {
                "ready": False,
                "state": "immutable_v2_observation_invalid_at_cutoff",
                "reason": "clock_snapshot_timestamps_missing",
                "observation_age_sec": None,
            }
        if (
            observed < snapshot_captured
            or observed < snapshot_generated
            or observed < observation_generated
            or observed > cutoff
        ):
            raise OfficialFactAdapterV2Error(
                "immutable_event_clock_v2_observation_clock_order_invalid"
            )
        observation_age = (cutoff - observed).total_seconds()
        if not isinstance(attestation, Mapping):
            return {
                "ready": False,
                "state": "immutable_v2_unattested_at_cutoff",
                "reason": "latest_clock_observation_attestation_missing",
                "observation_age_sec": observation_age,
            }
        if attestation.get("attested") is not True or str(
            attestation.get("state") or ""
        ) != REQUIRED_CLOCK_ATTESTATION_STATE:
            return {
                "ready": False,
                "state": "immutable_v2_unattested_at_cutoff",
                "reason": "latest_clock_observation_not_fresh_trusted",
                "observation_age_sec": observation_age,
            }
        attestation_generated = _parse_utc(attestation.get("generated_utc"))
        if attestation_generated is None or attestation_generated > observed:
            return {
                "ready": False,
                "state": "immutable_v2_unattested_at_cutoff",
                "reason": "latest_clock_attestation_time_invalid",
                "observation_age_sec": observation_age,
            }
        if observation_age > self.maximum_clock_observation_age_sec:
            return {
                "ready": False,
                "state": "immutable_v2_stale_at_cutoff",
                "reason": "latest_attested_clock_observation_stale",
                "observation_age_sec": observation_age,
            }
        return {
            "ready": True,
            "state": "immutable_v2_fresh_attested_at_cutoff",
            "reason": "",
            "observation_age_sec": observation_age,
        }

    def _upcoming_events(
        self,
        cutoff: dt.datetime,
        currencies: set[str],
        gaps: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        path = self.paths.immutable_event_clock_db
        if path is None:
            raise OfficialFactAdapterV2Error("immutable_event_clock_v2_not_configured")
        if not path.is_file() or path.stat().st_size <= 0:
            raise OfficialFactAdapterV2Error("immutable_event_clock_v2_unavailable")
        try:
            snapshot = reconstruct_event_clock_as_of(path, cutoff)
        except (OSError, sqlite3.Error, EventClockLedgerError, ValueError) as exc:
            raise OfficialFactAdapterV2Error(
                "immutable_event_clock_v2_identity_or_integrity_failure"
            ) from exc
        if snapshot.get("schema_version") != IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION:
            raise OfficialFactAdapterV2Error("immutable_event_clock_v2_schema_mismatch")
        if snapshot.get("contract_id") != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID:
            raise OfficialFactAdapterV2Error("immutable_event_clock_v2_contract_mismatch")
        snapshot_id = str(snapshot.get("snapshot_id") or "")
        if not snapshot_id:
            raise OfficialFactAdapterV2Error("immutable_event_clock_v2_no_snapshot_at_cutoff")
        if snapshot.get("research_only") is not True:
            raise OfficialFactAdapterV2Error("immutable_event_clock_v2_not_research_only")
        if snapshot.get("execution_eligible") is not False:
            raise OfficialFactAdapterV2Error("immutable_event_clock_v2_execution_eligible")
        if snapshot.get("supported_execution_decision") != "no_trade":
            raise OfficialFactAdapterV2Error("immutable_event_clock_v2_decision_mismatch")

        capture_attestation = dict(snapshot.get("clock_attestation") or {})
        freshness = self._clock_freshness(snapshot, cutoff=cutoff)
        if freshness["ready"] is not True:
            gaps.append(
                {
                    **self._gap(str(freshness["reason"]), path),
                    "observation_age_sec": freshness["observation_age_sec"],
                    "maximum_observation_age_sec": (
                        self.maximum_clock_observation_age_sec
                    ),
                }
            )
            return [], {
                "state": freshness["state"],
                "schema_version": IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
                "contract_id": IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
                "snapshot_id": snapshot_id,
                "snapshot_captured_utc": str(
                    snapshot.get("snapshot_captured_utc") or ""
                ),
                "clock_observation_id": str(
                    snapshot.get("clock_observation_id") or ""
                ),
                "clock_observed_utc": str(snapshot.get("clock_observed_utc") or ""),
                "source_generated_utc": str(
                    snapshot.get("source_generated_utc") or ""
                ),
                "events_sha256": str(snapshot.get("events_sha256") or ""),
                "manifest_sha256": str(snapshot.get("manifest_sha256") or ""),
                "clock_attestation": capture_attestation,
                "clock_ready_for_cutoff": False,
                "observation_age_sec": freshness["observation_age_sec"],
                "maximum_observation_age_sec": self.maximum_clock_observation_age_sec,
                "fallback_used": False,
                "mutable_fallback_forbidden": True,
                "complete_snapshot_at_cutoff": False,
                "degradation_reason": freshness["reason"],
            }
        output: list[dict[str, Any]] = []
        for row in snapshot.get("events") or []:
            if not isinstance(row, Mapping):
                raise OfficialFactAdapterV2Error("immutable_event_clock_v2_event_not_mapping")
            availability_text = str(row.get("ledger_effective_known_utc") or "")
            availability = _parse_utc(availability_text) if availability_text else None
            if availability is None or availability > cutoff:
                raise OfficialFactAdapterV2Error(
                    "immutable_event_clock_v2_event_not_known_at_cutoff"
                )
            scheduled = _parse_utc(row.get("scheduled_utc"))
            if scheduled is None:
                raise OfficialFactAdapterV2Error(
                    "immutable_event_clock_v2_event_missing_schedule"
                )
            if scheduled < cutoff:
                continue
            direct_currencies = {
                str(value).upper()
                for value in row.get("direct_currencies") or []
                if str(value).upper() in currencies
            }
            event_currencies = sorted(
                {
                    str(value).upper()
                    for value in row.get("currencies") or []
                    if str(value).upper() in currencies
                }
            )
            for currency in event_currencies:
                material = {
                    "event_id": str(row.get("upstream_event_id") or ""),
                    "event_version_id": str(row.get("event_version_id") or ""),
                    "currency": currency,
                    "category": str(row.get("category") or ""),
                    "scheduled_utc": _iso(scheduled),
                    "schedule_window_end_utc": (
                        str(row.get("schedule_window_end_utc") or "") or None
                    ),
                    "timing_precision": str(row.get("timing_precision") or ""),
                    "policy_event": self._is_policy_clock(row),
                    "policy_dependency": False,
                    "direct_event_currency": (
                        currency in direct_currencies if direct_currencies else None
                    ),
                    "driver_currency": currency,
                    "headline": str(row.get("headline") or ""),
                }
                output.append(
                    {
                        **material,
                        "fact_type": "official_event_clock",
                        "known_from_snapshot_utc": str(
                            snapshot.get("snapshot_captured_utc") or ""
                        ),
                        "ledger_effective_known_utc": _iso(availability),
                        "clock_provenance_state": "immutable_v2_ledger_snapshot",
                        "clock_snapshot_id": snapshot_id,
                        "clock_source_contract_id": IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
                        "consensus_causal": False,
                        "evidence_class": "scheduled_clock_only",
                        "degradation_reasons": ["direction_unknown_until_release"],
                        "direction_policy": "abstain",
                        "execution_eligible": False,
                        "can_place_orders": False,
                        "raw_payload_sha256": _stable_hash(material),
                    }
                )
                if len(output) >= self.maximum_rows_per_input:
                    gaps.append(self._gap("immutable_event_clock_v2_limit_reached", path))
                    break
            if len(output) >= self.maximum_rows_per_input:
                break
        provenance = {
            "state": freshness["state"],
            "schema_version": IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
            "contract_id": IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
            "snapshot_id": snapshot_id,
            "snapshot_captured_utc": str(snapshot.get("snapshot_captured_utc") or ""),
            "clock_observation_id": str(snapshot.get("clock_observation_id") or ""),
            "clock_observed_utc": str(snapshot.get("clock_observed_utc") or ""),
            "source_generated_utc": str(snapshot.get("source_generated_utc") or ""),
            "events_sha256": str(snapshot.get("events_sha256") or ""),
            "manifest_sha256": str(snapshot.get("manifest_sha256") or ""),
            "clock_attestation": capture_attestation,
            "clock_ready_for_cutoff": True,
            "observation_age_sec": freshness["observation_age_sec"],
            "maximum_observation_age_sec": self.maximum_clock_observation_age_sec,
            "fallback_used": False,
            "mutable_fallback_forbidden": True,
            "complete_snapshot_at_cutoff": True,
        }
        return output, provenance

    def as_of(
        self,
        decision_cutoff_utc: str | dt.datetime,
        *,
        currencies: Sequence[str] = CANONICAL_CURRENCIES,
    ) -> dict[str, Any]:
        snapshot = super().as_of(decision_cutoff_utc, currencies=currencies)
        provenance = dict(snapshot.get("event_clock_provenance") or {})
        if provenance.get("contract_id") != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID:
            raise OfficialFactAdapterV2Error("official_fact_v2_clock_contract_mismatch")
        if provenance.get("schema_version") != IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION:
            raise OfficialFactAdapterV2Error("official_fact_v2_clock_schema_mismatch")
        if provenance.get("fallback_used") is not False:
            raise OfficialFactAdapterV2Error("official_fact_v2_mutable_fallback_forbidden")

        snapshot["schema_version"] = ADAPTER_SCHEMA_VERSION
        snapshot["adapter_contract_id"] = ADAPTER_CONTRACT_ID
        snapshot["parent_adapter_contract_id"] = PARENT_ADAPTER_CONTRACT_ID
        snapshot["immutable_event_clock_schema_version"] = (
            IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION
        )
        snapshot["immutable_event_clock_contract_id"] = (
            IMMUTABLE_EVENT_CLOCK_CONTRACT_ID
        )
        snapshot["clock_dependency_policy"] = "strict_v2_only_no_mutable_fallback"
        snapshot["maximum_clock_observation_age_sec"] = (
            self.maximum_clock_observation_age_sec
        )
        snapshot["can_promote"] = False
        snapshot["can_authorize"] = False
        material = {
            key: value for key, value in snapshot.items() if key != "snapshot_id"
        }
        snapshot["snapshot_id"] = "official_fact_v2_snapshot_" + _canonical_hash(
            material
        )[:24]
        validate_official_fact_v2_snapshot(snapshot)
        return snapshot


def validate_official_fact_v2_snapshot(snapshot: Mapping[str, Any]) -> None:
    """Validate exact V2 branding, identity, and every closed data surface."""

    _validate_closed_official_fact_snapshot(snapshot)

    if snapshot.get("schema_version") != ADAPTER_SCHEMA_VERSION:
        raise OfficialFactAdapterV2Error("official_fact_v2_schema_mismatch")
    if snapshot.get("adapter_contract_id") != ADAPTER_CONTRACT_ID:
        raise OfficialFactAdapterV2Error("official_fact_v2_contract_mismatch")
    if snapshot.get("parent_adapter_contract_id") != PARENT_ADAPTER_CONTRACT_ID:
        raise OfficialFactAdapterV2Error("official_fact_v2_parent_contract_mismatch")
    if (
        snapshot.get("immutable_event_clock_schema_version")
        != IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION
    ):
        raise OfficialFactAdapterV2Error("official_fact_v2_clock_schema_mismatch")
    if (
        snapshot.get("immutable_event_clock_contract_id")
        != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID
    ):
        raise OfficialFactAdapterV2Error("official_fact_v2_clock_contract_mismatch")
    if snapshot.get("research_only") is not True:
        raise OfficialFactAdapterV2Error("official_fact_v2_not_research_only")
    for field in ("execution_eligible", "can_place_orders", "can_promote", "can_authorize"):
        if snapshot.get(field) is not False:
            raise OfficialFactAdapterV2Error(f"official_fact_v2_{field}_not_false")
    if snapshot.get("supported_execution_decision") != "no_trade":
        raise OfficialFactAdapterV2Error("official_fact_v2_decision_mismatch")
    facts = snapshot.get("facts")
    events = snapshot.get("upcoming_events")
    if not isinstance(facts, list) or int(snapshot.get("fact_count") or 0) != len(facts):
        raise OfficialFactAdapterV2Error("official_fact_v2_fact_count_mismatch")
    if not isinstance(events, list) or int(snapshot.get("upcoming_event_count") or 0) != len(
        events
    ):
        raise OfficialFactAdapterV2Error("official_fact_v2_event_count_mismatch")
    provenance = snapshot.get("event_clock_provenance")
    if not isinstance(provenance, Mapping):
        raise OfficialFactAdapterV2Error("official_fact_v2_provenance_missing")
    if provenance.get("schema_version") != IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION:
        raise OfficialFactAdapterV2Error("official_fact_v2_provenance_schema_mismatch")
    if provenance.get("contract_id") != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID:
        raise OfficialFactAdapterV2Error("official_fact_v2_provenance_contract_mismatch")
    if provenance.get("fallback_used") is not False:
        raise OfficialFactAdapterV2Error("official_fact_v2_mutable_fallback_forbidden")
    ready = provenance.get("clock_ready_for_cutoff") is True
    if ready != (provenance.get("complete_snapshot_at_cutoff") is True):
        raise OfficialFactAdapterV2Error("official_fact_v2_completeness_claim_mismatch")
    if not ready and events:
        raise OfficialFactAdapterV2Error("official_fact_v2_stale_clock_exposed_events")
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    expected_id = "official_fact_v2_snapshot_" + _canonical_hash(material)[:24]
    if snapshot.get("snapshot_id") != expected_id:
        raise OfficialFactAdapterV2Error("official_fact_v2_snapshot_id_mismatch")


__all__ = [
    "ADAPTER_CONTRACT_ID",
    "ADAPTER_SCHEMA_VERSION",
    "CANONICAL_CURRENCIES",
    "OFFICIAL_CURRENCY_EVIDENCE_FIELDS",
    "OFFICIAL_EVENT_CLOCK_PROVENANCE_FIELDS",
    "OFFICIAL_FACT_EVIDENCE_CLASSES",
    "OFFICIAL_FACT_FIELDS_BY_TYPE",
    "OFFICIAL_FACT_TYPES",
    "OFFICIAL_FACT_V2_TOP_LEVEL_FIELDS",
    "OFFICIAL_GLOBAL_GAP_FIELDS",
    "OFFICIAL_SOURCE_HEALTH_FIELDS",
    "OFFICIAL_UPCOMING_EVENT_FIELDS",
    "OfficialFactAdapterV2",
    "OfficialFactAdapterV2Error",
    "OfficialFactPathsV2",
    "validate_official_event_clock_provenance",
    "validate_official_fact_v2_snapshot",
    "validate_official_global_gaps",
    "validate_official_source_health",
]
