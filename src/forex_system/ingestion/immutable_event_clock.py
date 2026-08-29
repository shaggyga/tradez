"""Append-only point-in-time ledger for scheduled event clocks.

The live news catalog publishes ``events_latest.json`` as a mutable view.  It
is useful for current monitoring, but it cannot prove what the process knew at
an earlier decision cutoff.  This module records exact, content-addressed
snapshots of that view in an insert-only SQLite ledger and reconstructs the
last snapshot that was *observed* by a requested cutoff.

The ledger deliberately assigns no direction, forecast, eligibility, or
authorization.  An upstream ``first_known_utc`` is retained as provenance but
can never move knowledge earlier than this ledger's own capture time.
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
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo


UTC = dt.timezone.utc
SCHEMA_VERSION = "immutable_event_clock_ledger_v2"
CONTRACT_ID = "immutable_point_in_time_event_clock_v2_20260817"
EXPECTED_SOURCE_PIPELINE_VERSION = "all_pair_news_event_tags_v3"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_MAXIMUM_CLOCK_ATTESTATION_AGE_SEC = 300.0
ENGINEERING_ONLY_SOURCE_IDS = {
    "hungary_ksh_release_calendar_exact_v1",
    "hungary_ksh_headline_cpi_release_clock_exact_v2",
}
HUF_KSH_VERIFIED_SOURCE_ID = "hungary_ksh_headline_cpi_release_clock_exact_v3"
HUF_KSH_VERIFIED_CONTRACT_ID = (
    "hungary_ksh_headline_cpi_release_clock_exact_v3_verified_policy_bytes_20260817"
)
HUF_KSH_POLICY_SHA256 = (
    "617f513efcccfd07fa159b0fdf9fc9e0e3243095e20deb50d88e88495fbd4030"
)
HUF_KSH_POLICY_ARCHIVE = (
    "ksh_dissemination_policy_2024_617f513efcccfd07.pdf"
)
HUF_KSH_CALENDAR_URL = "https://www.ksh.hu/prices?lang=en"
HUF_KSH_SOURCE_NAME = (
    "Hungarian Central Statistical Office headline CPI exact public-release clock"
)


class EventClockLedgerError(RuntimeError):
    """Raised when a source snapshot cannot satisfy the causal contract."""


@dataclass(frozen=True)
class SourceSnapshot:
    """A stable byte-for-byte read of the mutable upstream catalog."""

    events: tuple[Mapping[str, Any], ...]
    events_sha256: str
    manifest_sha256: str
    source_generated_utc: str
    source_pipeline_version: str
    source_event_count: int
    events_artifact_name: str
    manifest_artifact_name: str


def _parse_utc(value: Any, *, field: str, required: bool = False) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        if required:
            raise EventClockLedgerError(f"missing_{field}")
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise EventClockLedgerError(f"invalid_{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise EventClockLedgerError(f"naive_{field}:{value}")
    return parsed.astimezone(UTC)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _normalized_strings(values: Any) -> list[str]:
    if not isinstance(values, (list, tuple, set)):
        return []
    return sorted({str(value).strip().upper() for value in values if str(value).strip()})


def build_clock_attestation(
    path: Path | None,
    captured_utc: dt.datetime,
    *,
    maximum_age_sec: float = DEFAULT_MAXIMUM_CLOCK_ATTESTATION_AGE_SEC,
) -> dict[str, Any]:
    """Record whether host/broker clock integrity was fresh at capture.

    An unavailable or stale artifact does not prevent the research snapshot;
    it is recorded as explicitly un-attested and must not later be upgraded in
    place.  This keeps collection fail-descriptive without forging timing
    confidence.
    """

    capture = captured_utc.astimezone(UTC)
    maximum_age = max(0.0, float(maximum_age_sec))
    if path is None:
        return {
            "state": "unattested_not_provided",
            "attested": False,
            "artifact_name": None,
            "generated_utc": None,
            "age_at_capture_sec": None,
            "payload_sha256": None,
            "maximum_age_sec": maximum_age,
        }
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8-sig"))
    except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return {
            "state": "unattested_unavailable",
            "attested": False,
            "artifact_name": path.name,
            "generated_utc": None,
            "age_at_capture_sec": None,
            "payload_sha256": None,
            "maximum_age_sec": maximum_age,
        }
    if not isinstance(payload, Mapping):
        return {
            "state": "unattested_invalid",
            "attested": False,
            "artifact_name": path.name,
            "generated_utc": None,
            "age_at_capture_sec": None,
            "payload_sha256": _sha256_bytes(raw),
            "maximum_age_sec": maximum_age,
        }
    try:
        generated = _parse_utc(
            payload.get("generated_utc"), field="clock_generated_utc"
        )
    except EventClockLedgerError:
        generated = None
    age = (capture - generated).total_seconds() if generated is not None else None
    # Integrity flags are a typed contract, not generic truthy inputs.  JSON
    # strings such as ``"false"`` and integers such as ``1`` must not be
    # promoted into a trusted clock attestation.
    trusted = payload.get("timestamp_normalization_trusted") is True
    synchronized = payload.get("host_clock_synchronized") is True
    source_fresh = payload.get("source_fresh") is True
    status_ok = str(payload.get("status") or "").lower() == "ok"
    if generated is None:
        state = "unattested_missing_generated_time"
    elif age is not None and age < 0:
        state = "unattested_future_clock_record"
    elif age is not None and age > maximum_age:
        state = "unattested_stale"
    elif not (trusted and synchronized and source_fresh and status_ok):
        state = "unattested_not_trusted"
    else:
        state = "fresh_trusted"
    return {
        "state": state,
        "attested": state == "fresh_trusted",
        "artifact_name": path.name,
        "generated_utc": _iso(generated) if generated else None,
        "age_at_capture_sec": round(age, 6) if age is not None else None,
        "payload_sha256": _sha256_bytes(raw),
        "maximum_age_sec": maximum_age,
        "status": str(payload.get("status") or ""),
        "timestamp_normalization_trusted": trusted,
        "host_clock_synchronized": synchronized,
        "source_fresh": source_fresh,
    }


def _validate_clock_attestation(
    attestation: Mapping[str, Any],
    *,
    captured_utc: dt.datetime,
    error_prefix: str,
) -> None:
    """Recompute the semantics of a claimed trusted clock observation."""

    state = str(attestation.get("state") or "")
    attested_value = attestation.get("attested")
    if type(attested_value) is not bool:  # bool is intentional, not truthiness.
        raise EventClockLedgerError(f"{error_prefix}_attested_type_invalid")
    if (state == "fresh_trusted") != bool(attested_value):
        raise EventClockLedgerError(f"{error_prefix}_state_mismatch")
    if state != "fresh_trusted":
        return

    artifact_name = attestation.get("artifact_name")
    if not isinstance(artifact_name, str) or not artifact_name.strip():
        raise EventClockLedgerError(f"{error_prefix}_artifact_name_invalid")
    payload_sha256 = attestation.get("payload_sha256")
    if not isinstance(payload_sha256, str) or not SHA256_PATTERN.fullmatch(
        payload_sha256
    ):
        raise EventClockLedgerError(f"{error_prefix}_payload_sha256_invalid")
    generated = _parse_utc(
        attestation.get("generated_utc"),
        field=f"{error_prefix}_generated_utc",
        required=True,
    )
    assert generated is not None
    capture = captured_utc.astimezone(UTC)
    calculated_age = (capture - generated).total_seconds()
    maximum_age_raw = attestation.get("maximum_age_sec")
    age_raw = attestation.get("age_at_capture_sec")
    if isinstance(maximum_age_raw, bool) or isinstance(age_raw, bool):
        raise EventClockLedgerError(f"{error_prefix}_age_type_invalid")
    try:
        maximum_age = float(maximum_age_raw)
        declared_age = float(age_raw)
    except (TypeError, ValueError) as exc:
        raise EventClockLedgerError(f"{error_prefix}_age_invalid") from exc
    if (
        not math.isfinite(maximum_age)
        or not math.isfinite(declared_age)
        or maximum_age < 0.0
        or maximum_age > DEFAULT_MAXIMUM_CLOCK_ATTESTATION_AGE_SEC
        or calculated_age < 0.0
        or calculated_age > maximum_age
        or round(calculated_age, 6) != round(declared_age, 6)
    ):
        raise EventClockLedgerError(f"{error_prefix}_age_semantics_invalid")
    if (
        str(attestation.get("status") or "").lower() != "ok"
        or attestation.get("timestamp_normalization_trusted") is not True
        or attestation.get("host_clock_synchronized") is not True
        or attestation.get("source_fresh") is not True
    ):
        raise EventClockLedgerError(f"{error_prefix}_trust_semantics_invalid")


def _validate_snapshot_clock_attestation(snapshot: Mapping[str, Any]) -> None:
    captured = _parse_utc(
        snapshot.get("captured_utc"), field="snapshot_captured_utc", required=True
    )
    assert captured is not None
    source_generated = _parse_utc(
        snapshot.get("source_generated_utc"),
        field="snapshot_source_generated_utc",
        required=True,
    )
    assert source_generated is not None
    if source_generated > captured:
        raise EventClockLedgerError("snapshot_source_generated_after_capture")
    attestation = snapshot.get("clock_attestation")
    if not isinstance(attestation, Mapping):
        raise EventClockLedgerError("snapshot_clock_attestation_invalid")
    _validate_clock_attestation(
        attestation,
        captured_utc=captured,
        error_prefix="snapshot_clock_attestation",
    )


def read_stable_source(
    events_path: Path,
    manifest_path: Path,
    *,
    maximum_attempts: int = 3,
) -> SourceSnapshot:
    """Read the mutable source without accepting an in-progress publication.

    The producer writes the events artifact before its manifest.  Requiring a
    stable manifest, matching event count, and an events mtime no newer than
    the manifest prevents the common half-publication window from entering
    the immutable ledger.
    """

    problem = "source_changed_during_read"
    for _ in range(max(1, maximum_attempts)):
        try:
            manifest_before = manifest_path.read_bytes()
            manifest_before_stat = manifest_path.stat()
            events_bytes = events_path.read_bytes()
            events_stat = events_path.stat()
            manifest_after = manifest_path.read_bytes()
            manifest_after_stat = manifest_path.stat()
        except OSError as exc:
            raise EventClockLedgerError(f"source_read_failed:{exc}") from exc
        if (
            manifest_before != manifest_after
            or manifest_before_stat.st_mtime_ns != manifest_after_stat.st_mtime_ns
            or manifest_before_stat.st_size != manifest_after_stat.st_size
        ):
            problem = "manifest_changed_during_read"
            continue
        if events_stat.st_mtime_ns > manifest_after_stat.st_mtime_ns:
            problem = "events_newer_than_manifest"
            continue
        try:
            manifest = json.loads(manifest_after.decode("utf-8-sig"))
            events_value = json.loads(events_bytes.decode("utf-8-sig"))
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
            raise EventClockLedgerError(f"source_json_invalid:{exc}") from exc
        if isinstance(events_value, Mapping):
            events_value = events_value.get("events") or []
        if not isinstance(events_value, list) or not all(
            isinstance(row, Mapping) for row in events_value
        ):
            raise EventClockLedgerError("events_source_is_not_a_mapping_list")
        if not isinstance(manifest, Mapping):
            raise EventClockLedgerError("manifest_source_is_not_a_mapping")
        if int(manifest.get("manifest_schema_version") or 0) != 2:
            raise EventClockLedgerError("manifest_schema_version_mismatch")
        if str(manifest.get("pipeline_version") or "") != EXPECTED_SOURCE_PIPELINE_VERSION:
            raise EventClockLedgerError("source_pipeline_version_mismatch")
        declared_count = manifest.get("event_count")
        if declared_count is None or int(declared_count) != len(events_value):
            problem = "manifest_event_count_mismatch"
            continue
        actual_events_sha256 = _sha256_bytes(events_bytes)
        declared_events_sha256 = str(manifest.get("events_sha256") or "").lower()
        if (
            not SHA256_PATTERN.fullmatch(declared_events_sha256)
            or declared_events_sha256 != actual_events_sha256
        ):
            raise EventClockLedgerError("manifest_events_sha256_mismatch")
        if str(manifest.get("events_artifact_name") or "") != events_path.name:
            raise EventClockLedgerError("manifest_events_artifact_name_mismatch")
        source_generated = _parse_utc(
            manifest.get("generated_utc"),
            field="source_generated_utc",
            required=True,
        )
        assert source_generated is not None
        return SourceSnapshot(
            events=tuple(events_value),
            events_sha256=actual_events_sha256,
            manifest_sha256=_sha256_bytes(manifest_after),
            source_generated_utc=_iso(source_generated),
            source_pipeline_version=str(manifest.get("pipeline_version") or ""),
            source_event_count=len(events_value),
            events_artifact_name=events_path.name,
            manifest_artifact_name=manifest_path.name,
        )
    raise EventClockLedgerError(problem)


def _clock_semantic_material(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return only fields that change the meaning of a calendar clock."""

    return {
        "upstream_event_id": str(row.get("upstream_event_id") or ""),
        "scheduled_utc": str(row.get("scheduled_utc") or ""),
        "event_utc": str(row.get("event_utc") or ""),
        "headline": str(row.get("headline") or ""),
        "category": str(row.get("category") or ""),
        "source_name": str(row.get("source_name") or ""),
        "source_id": str(row.get("source_id") or ""),
        "source_contract_id": str(row.get("source_contract_id") or ""),
        "source_cohort_id": str(row.get("source_cohort_id") or ""),
        "material_content_sha256": str(row.get("material_content_sha256") or ""),
        "release_time_rule_observed_sha256": str(
            row.get("release_time_rule_observed_sha256") or ""
        ),
        "release_time_rule_archive_name": str(
            row.get("release_time_rule_archive_name") or ""
        ),
        "release_rule_bytes_verified": bool(
            row.get("release_rule_bytes_verified")
        ),
        "source_url": str(row.get("source_url") or ""),
        "source_type": str(row.get("source_type") or ""),
        "source_verified": bool(row.get("source_verified")),
        "currencies": list(row.get("currencies") or []),
        "direct_currencies": list(row.get("direct_currencies") or []),
        "timing_precision": str(row.get("timing_precision") or ""),
        "clock_semantics": str(row.get("clock_semantics") or ""),
        "independent_domestic_event": bool(
            row.get("independent_domestic_event")
        ),
        "linked_policy_factor": bool(row.get("linked_policy_factor")),
        "schedule_window_end_utc": str(row.get("schedule_window_end_utc") or ""),
        "event_time_basis": str(row.get("event_time_basis") or ""),
    }


def _semantic_clock_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    semantic_ids = sorted(
        "clock_semantic_" + _sha256_json(_clock_semantic_material(row))[:32]
        for row in rows
    )
    return _sha256_json(semantic_ids)


def _normalize_clock(raw: Mapping[str, Any]) -> dict[str, Any] | None:
    source_id = str(raw.get("source_id") or "").strip()
    if source_id in ENGINEERING_ONLY_SOURCE_IDS:
        raise EventClockLedgerError(f"engineering_only_source_quarantined:{source_id}")
    if source_id == HUF_KSH_VERIFIED_SOURCE_ID and not (
        str(raw.get("source_contract_id") or "").strip()
        == HUF_KSH_VERIFIED_CONTRACT_ID
        and str(raw.get("source_cohort_id") or "").strip()
        == HUF_KSH_VERIFIED_CONTRACT_ID
        and str(raw.get("release_time_rule_observed_sha256") or "").strip().lower()
        == HUF_KSH_POLICY_SHA256
        and raw.get("release_rule_bytes_verified") is True
        and str(raw.get("release_time_rule_archive_name") or "").strip()
        == HUF_KSH_POLICY_ARCHIVE
        and str(raw.get("source_name") or "").strip() == HUF_KSH_SOURCE_NAME
        and str(raw.get("source_url") or "").strip() == HUF_KSH_CALENDAR_URL
        and raw.get("source_verified") is True
        and str(raw.get("source_type") or "").strip() == "live_news_watch"
        and _normalized_strings(raw.get("currencies")) == ["HUF"]
        and _normalized_strings(raw.get("direct_currencies")) == ["HUF"]
        and raw.get("direct_currencies_known") is True
        and str(raw.get("timing_precision") or "").strip() == "minute"
        and str(raw.get("clock_semantics") or "").strip()
        == "domestic_official_statistical_release"
        and raw.get("independent_domestic_event") is True
        and raw.get("linked_policy_factor") is False
        and str(raw.get("event_time_basis") or "").strip()
        == "scheduled_release"
        and str(raw.get("headline") or "").strip()
        == "Hungary headline consumer price inflation"
        and str(raw.get("category") or "").strip() == "inflation_context"
        and str(raw.get("scope") or "").strip() == "currency"
        and SHA256_PATTERN.fullmatch(
            str(raw.get("material_content_sha256") or "").strip().lower()
        )
        is not None
        and raw.get("context_only") is True
        and raw.get("research_only") is True
        and raw.get("directional_research_only") is True
        and raw.get("execution_eligible") is False
        and raw.get("can_place_orders") is False
        and not raw.get("currency_bias")
        and not raw.get("pair_bias")
    ):
        raise EventClockLedgerError("huf_ksh_verified_rule_provenance_missing")
    scheduled = _parse_utc(raw.get("scheduled_utc"), field="scheduled_utc")
    if scheduled is None:
        return None
    upstream_first_known = _parse_utc(
        raw.get("first_known_utc"), field="upstream_first_known_utc"
    )
    upstream_updated = _parse_utc(
        raw.get("updated_utc"), field="upstream_updated_utc"
    )
    event_utc = _parse_utc(raw.get("event_utc"), field="event_utc")
    if (
        source_id == HUF_KSH_VERIFIED_SOURCE_ID
        and event_utc != scheduled
    ):
        raise EventClockLedgerError("huf_ksh_event_clock_mismatch")
    if source_id == HUF_KSH_VERIFIED_SOURCE_ID:
        local_release = scheduled.astimezone(ZoneInfo("Europe/Budapest"))
        if (
            local_release.hour,
            local_release.minute,
            local_release.second,
            local_release.microsecond,
        ) != (8, 30, 0, 0):
            raise EventClockLedgerError("huf_ksh_local_release_time_mismatch")
    event_id = str(raw.get("event_id") or "").strip()
    if not event_id:
        event_id = "clock_" + _sha256_json(
            {
                "scheduled_utc": _iso(scheduled),
                "headline": str(raw.get("headline") or ""),
                "source_url": str(raw.get("source_url") or ""),
            }
        )[:24]
    raw_payload = raw.get("raw") if isinstance(raw.get("raw"), Mapping) else {}
    timing_precision = str(
        raw.get("timing_precision")
        or raw_payload.get("timing_precision")
        or ""
    )
    clock_semantics = str(
        raw.get("clock_semantics")
        or raw_payload.get("clock_semantics")
        or ""
    ).strip()
    independent_domestic_event = (
        raw.get("independent_domestic_event")
        if "independent_domestic_event" in raw
        else raw_payload.get("independent_domestic_event")
    )
    window_end = _parse_utc(
        raw.get("schedule_window_end_utc")
        or raw_payload.get("schedule_window_end_utc"),
        field="schedule_window_end_utc",
    )
    row = {
        "upstream_event_id": event_id,
        "scheduled_utc": _iso(scheduled),
        "event_utc": _iso(event_utc) if event_utc else "",
        "upstream_first_known_utc": (
            _iso(upstream_first_known) if upstream_first_known else ""
        ),
        "upstream_updated_utc": _iso(upstream_updated) if upstream_updated else "",
        "headline": str(raw.get("headline") or "").strip(),
        "category": str(raw.get("category") or "").strip(),
        "source_name": str(raw.get("source_name") or "").strip(),
        "source_id": str(raw.get("source_id") or "").strip(),
        "source_contract_id": str(raw.get("source_contract_id") or "").strip(),
        "source_cohort_id": str(raw.get("source_cohort_id") or "").strip(),
        "material_content_sha256": str(
            raw.get("material_content_sha256") or ""
        ).strip(),
        "release_time_rule_observed_sha256": str(
            raw.get("release_time_rule_observed_sha256") or ""
        ).strip().lower(),
        "release_time_rule_archive_name": str(
            raw.get("release_time_rule_archive_name") or ""
        ).strip(),
        "release_rule_bytes_verified": raw.get("release_rule_bytes_verified")
        is True,
        "source_url": str(raw.get("source_url") or "").strip(),
        "source_type": str(raw.get("source_type") or "").strip(),
        "source_verified": bool(raw.get("source_verified")),
        "currencies": _normalized_strings(raw.get("currencies")),
        "direct_currencies": _normalized_strings(raw.get("direct_currencies")),
        "timing_precision": timing_precision,
        "clock_semantics": clock_semantics,
        "independent_domestic_event": independent_domestic_event is True,
        "linked_policy_factor": raw.get("linked_policy_factor") is True,
        "schedule_window_end_utc": _iso(window_end) if window_end else "",
        "event_time_basis": str(raw.get("event_time_basis") or "").strip(),
    }
    row["clock_semantic_id"] = (
        "clock_semantic_" + _sha256_json(_clock_semantic_material(row))[:32]
    )
    row["event_version_id"] = "clock_version_" + _sha256_json(row)[:32]
    return row


def _verify_stored_event(
    raw_json: str,
    *,
    expected_event_version_id: str | None = None,
    expected_upstream_event_id: str | None = None,
    expected_scheduled_utc: str | None = None,
) -> dict[str, Any]:
    """Recompute content addresses before trusting an immutable-ledger row."""

    try:
        event = json.loads(raw_json)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise EventClockLedgerError("stored_event_json_invalid") from exc
    if not isinstance(event, dict):
        raise EventClockLedgerError("stored_event_json_not_mapping")
    claimed_semantic_id = str(event.get("clock_semantic_id") or "")
    calculated_semantic_id = (
        "clock_semantic_" + _sha256_json(_clock_semantic_material(event))[:32]
    )
    if claimed_semantic_id != calculated_semantic_id:
        raise EventClockLedgerError("stored_event_clock_semantic_id_mismatch")
    claimed_version_id = str(event.get("event_version_id") or "")
    version_material = dict(event)
    version_material.pop("event_version_id", None)
    calculated_version_id = "clock_version_" + _sha256_json(version_material)[:32]
    if claimed_version_id != calculated_version_id:
        raise EventClockLedgerError("stored_event_version_id_mismatch")
    if expected_event_version_id is not None and claimed_version_id != str(
        expected_event_version_id
    ):
        raise EventClockLedgerError("stored_event_table_version_id_mismatch")
    if expected_upstream_event_id is not None and str(
        event.get("upstream_event_id") or ""
    ) != str(expected_upstream_event_id):
        raise EventClockLedgerError("stored_event_table_upstream_id_mismatch")
    if expected_scheduled_utc is not None and str(
        event.get("scheduled_utc") or ""
    ) != str(expected_scheduled_utc):
        raise EventClockLedgerError("stored_event_table_schedule_mismatch")
    return event


def _verify_snapshot_row(snapshot: sqlite3.Row) -> dict[str, Any]:
    try:
        payload = json.loads(str(snapshot["snapshot_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise EventClockLedgerError("stored_snapshot_json_invalid") from exc
    if not isinstance(payload, dict):
        raise EventClockLedgerError("stored_snapshot_json_not_mapping")
    if str(snapshot["schema_version"] or "") != SCHEMA_VERSION:
        raise EventClockLedgerError("stored_snapshot_schema_version_mismatch")
    if str(snapshot["contract_id"] or "") != CONTRACT_ID:
        raise EventClockLedgerError("stored_snapshot_contract_id_mismatch")
    compared = {
        "snapshot_id": "snapshot_id",
        "schema_version": "schema_version",
        "contract_id": "contract_id",
        "captured_utc": "captured_utc",
        "source_generated_utc": "source_generated_utc",
        "source_pipeline_version": "source_pipeline_version",
        "events_sha256": "events_sha256",
        "manifest_sha256": "manifest_sha256",
        "events_artifact_name": "events_artifact_name",
        "manifest_artifact_name": "manifest_artifact_name",
    }
    for payload_key, column in compared.items():
        if str(payload.get(payload_key) or "") != str(snapshot[column] or ""):
            raise EventClockLedgerError(f"stored_snapshot_{payload_key}_mismatch")
    captured = _parse_utc(
        payload.get("captured_utc"), field="snapshot_captured_utc", required=True
    )
    source_generated = _parse_utc(
        payload.get("source_generated_utc"),
        field="snapshot_source_generated_utc",
        required=True,
    )
    assert captured is not None and source_generated is not None
    if source_generated > captured:
        raise EventClockLedgerError("stored_snapshot_source_generated_after_capture")
    event_versions = payload.get("event_versions")
    if not isinstance(event_versions, list):
        raise EventClockLedgerError("stored_snapshot_event_versions_invalid")
    if len(event_versions) != int(snapshot["source_scheduled_clock_count"]):
        raise EventClockLedgerError("stored_snapshot_event_count_mismatch")
    semantic = _semantic_clock_hash(event_versions)
    if semantic != str(payload.get("semantic_clock_sha256") or ""):
        raise EventClockLedgerError("stored_snapshot_semantic_hash_mismatch")
    identity = {
        "contract_id": str(payload.get("contract_id") or ""),
        "captured_utc": str(payload.get("captured_utc") or ""),
        "source_generated_utc": str(payload.get("source_generated_utc") or ""),
        "events_sha256": str(payload.get("events_sha256") or ""),
        "manifest_sha256": str(payload.get("manifest_sha256") or ""),
        "event_version_ids": [
            str(row.get("event_version_id") or "")
            for row in event_versions
            if isinstance(row, Mapping)
        ],
        "clock_attestation": dict(payload.get("clock_attestation") or {}),
    }
    expected_snapshot_id = "event_clock_snapshot_" + _sha256_json(identity)[:32]
    if str(payload.get("snapshot_id") or "") != expected_snapshot_id:
        raise EventClockLedgerError("stored_snapshot_content_address_mismatch")
    return payload


def _verify_snapshot_events_closure(
    connection: sqlite3.Connection,
    *,
    snapshot: sqlite3.Row,
    snapshot_payload: Mapping[str, Any],
) -> list[sqlite3.Row]:
    """Prove that normalized membership exactly closes the snapshot payload.

    ``snapshot_json`` is the content-addressed source of truth.  The normalized
    relation is only an index over that immutable payload; missing, extra,
    reordered, or backdated rows must never change an as-of reconstruction.
    """

    expected_versions = snapshot_payload.get("event_versions")
    if not isinstance(expected_versions, list):
        raise EventClockLedgerError("stored_snapshot_event_versions_invalid")
    rows = connection.execute(
        """SELECT se.snapshot_id,se.event_version_id,se.ordinal,
                  se.effective_known_utc,v.upstream_event_id,v.scheduled_utc,
                  v.event_json
           FROM snapshot_events se
           LEFT JOIN event_clock_versions v
             ON v.event_version_id=se.event_version_id
           WHERE se.snapshot_id=?
           ORDER BY se.ordinal,se.event_version_id""",
        (str(snapshot["snapshot_id"]),),
    ).fetchall()
    if len(rows) != len(expected_versions):
        raise EventClockLedgerError("stored_snapshot_membership_count_mismatch")

    actual_ids = [str(row["event_version_id"] or "") for row in rows]
    expected_ids = [
        str(row.get("event_version_id") or "")
        if isinstance(row, Mapping)
        else ""
        for row in expected_versions
    ]
    if set(actual_ids) != set(expected_ids):
        raise EventClockLedgerError("stored_snapshot_membership_set_mismatch")

    captured = _parse_utc(
        snapshot["captured_utc"], field="snapshot_captured_utc", required=True
    )
    assert captured is not None
    for expected_ordinal, (row, expected) in enumerate(zip(rows, expected_versions)):
        if not isinstance(expected, Mapping):
            raise EventClockLedgerError("stored_snapshot_event_version_not_mapping")
        if int(row["ordinal"]) != expected_ordinal:
            raise EventClockLedgerError("stored_snapshot_membership_ordinal_mismatch")
        if str(row["event_version_id"] or "") != str(
            expected.get("event_version_id") or ""
        ):
            raise EventClockLedgerError("stored_snapshot_membership_order_mismatch")
        if row["event_json"] is None:
            raise EventClockLedgerError("stored_snapshot_event_version_missing")
        stored_event = _verify_stored_event(
            str(row["event_json"]),
            expected_event_version_id=str(row["event_version_id"]),
            expected_upstream_event_id=str(row["upstream_event_id"]),
            expected_scheduled_utc=str(row["scheduled_utc"]),
        )
        if _canonical_json(stored_event) != _canonical_json(dict(expected)):
            raise EventClockLedgerError("stored_snapshot_event_payload_mismatch")
        upstream_known = _parse_utc(
            stored_event.get("upstream_first_known_utc"),
            field="upstream_first_known_utc",
        )
        expected_known = max(
            value for value in (captured, upstream_known) if value is not None
        )
        if str(row["effective_known_utc"] or "") != _iso(expected_known):
            raise EventClockLedgerError(
                "stored_snapshot_membership_effective_known_mismatch"
            )
    return rows


def _verified_attestation(
    row: sqlite3.Row | None,
    *,
    captured_utc: dt.datetime,
) -> dict[str, Any] | None:
    if row is None:
        return None
    try:
        attestation = json.loads(str(row["attestation_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise EventClockLedgerError("stored_snapshot_attestation_invalid") from exc
    if not isinstance(attestation, dict):
        raise EventClockLedgerError("stored_snapshot_attestation_invalid")
    if str(attestation.get("state") or "") != str(
        row["attestation_state"] or ""
    ) or int(bool(attestation.get("attested"))) != int(row["attested"]):
        raise EventClockLedgerError("stored_snapshot_attestation_mismatch")
    if str(attestation.get("payload_sha256") or "") != str(
        row["payload_sha256"] or ""
    ):
        raise EventClockLedgerError("stored_snapshot_attestation_hash_mismatch")
    for key in ("generated_utc", "artifact_name"):
        payload_value = attestation.get(key)
        row_value = row[key]
        if (None if payload_value is None else str(payload_value)) != (
            None if row_value is None else str(row_value)
        ):
            raise EventClockLedgerError(
                f"stored_snapshot_attestation_{key}_mismatch"
            )
    _validate_clock_attestation(
        attestation,
        captured_utc=captured_utc,
        error_prefix="stored_snapshot_attestation",
    )
    return attestation


def _verify_observation_row(observation: sqlite3.Row) -> dict[str, Any]:
    try:
        material = json.loads(str(observation["observation_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise EventClockLedgerError("stored_observation_json_invalid") from exc
    if not isinstance(material, dict):
        raise EventClockLedgerError("stored_observation_json_not_mapping")
    expected_id = "event_clock_observation_" + _sha256_json(material)[:32]
    if str(observation["observation_id"]) != expected_id:
        raise EventClockLedgerError("stored_observation_content_address_mismatch")
    for key in (
        "snapshot_id",
        "observed_utc",
        "source_generated_utc",
        "semantic_clock_sha256",
        "events_sha256",
        "manifest_sha256",
    ):
        if str(material.get(key) or "") != str(observation[key] or ""):
            raise EventClockLedgerError(f"stored_observation_{key}_mismatch")
    observed = _parse_utc(
        material.get("observed_utc"), field="observation_observed_utc", required=True
    )
    source_generated = _parse_utc(
        material.get("source_generated_utc"),
        field="observation_source_generated_utc",
        required=True,
    )
    assert observed is not None and source_generated is not None
    if source_generated > observed:
        raise EventClockLedgerError("stored_observation_source_generated_after_observed")
    attestation = material.get("clock_attestation")
    if not isinstance(attestation, Mapping):
        raise EventClockLedgerError("stored_observation_attestation_invalid")
    if str(attestation.get("state") or "") != str(
        observation["attestation_state"] or ""
    ) or int(bool(attestation.get("attested"))) != int(observation["attested"]):
        raise EventClockLedgerError("stored_observation_attestation_mismatch")
    if str(attestation.get("payload_sha256") or "") != str(
        observation["attestation_payload_sha256"] or ""
    ):
        raise EventClockLedgerError("stored_observation_attestation_hash_mismatch")
    _validate_clock_attestation(
        attestation,
        captured_utc=observed,
        error_prefix="stored_observation_attestation",
    )
    return material


def build_snapshot(
    source: SourceSnapshot,
    captured_utc: dt.datetime,
    *,
    clock_attestation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if source.source_pipeline_version != EXPECTED_SOURCE_PIPELINE_VERSION:
        raise EventClockLedgerError("source_pipeline_version_mismatch")
    capture = captured_utc.astimezone(UTC)
    source_generated = _parse_utc(
        source.source_generated_utc,
        field="source_generated_utc",
        required=True,
    )
    assert source_generated is not None
    if source_generated > capture:
        raise EventClockLedgerError("source_generated_after_capture")
    versions: dict[str, dict[str, Any]] = {}
    rejected = 0
    for raw in source.events:
        is_scheduled_clock = (
            str(raw.get("event_time_basis") or "").strip() == "scheduled_release"
            or bool(str(raw.get("scheduled_utc") or "").strip())
        )
        try:
            row = _normalize_clock(raw)
        except EventClockLedgerError as exc:
            if is_scheduled_clock:
                raise EventClockLedgerError(
                    f"malformed_scheduled_clock:{raw.get('event_id') or ''}:{exc}"
                ) from exc
            rejected += 1
            continue
        if row is None:
            if is_scheduled_clock:
                raise EventClockLedgerError(
                    f"malformed_scheduled_clock:{raw.get('event_id') or ''}:missing_scheduled_utc"
                )
            continue
        versions[str(row["event_version_id"])] = row
    ordered = sorted(
        versions.values(),
        key=lambda row: (
            str(row["scheduled_utc"]),
            str(row["upstream_event_id"]),
            str(row["event_version_id"]),
        ),
    )
    attestation = dict(clock_attestation or {
        "state": "unattested_not_provided",
        "attested": False,
        "artifact_name": None,
        "generated_utc": None,
        "age_at_capture_sec": None,
        "payload_sha256": None,
        "maximum_age_sec": DEFAULT_MAXIMUM_CLOCK_ATTESTATION_AGE_SEC,
    })
    _validate_clock_attestation(
        attestation,
        captured_utc=capture,
        error_prefix="snapshot_clock_attestation",
    )
    semantic_clock_sha256 = _semantic_clock_hash(ordered)
    identity = {
        "contract_id": CONTRACT_ID,
        "captured_utc": _iso(capture),
        "source_generated_utc": source.source_generated_utc,
        "events_sha256": source.events_sha256,
        "manifest_sha256": source.manifest_sha256,
        "event_version_ids": [row["event_version_id"] for row in ordered],
        "clock_attestation": attestation,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "snapshot_id": "event_clock_snapshot_" + _sha256_json(identity)[:32],
        "captured_utc": _iso(capture),
        "source_generated_utc": source.source_generated_utc,
        "source_pipeline_version": source.source_pipeline_version,
        "source_event_count": source.source_event_count,
        "source_scheduled_clock_count": len(ordered),
        "rejected_malformed_clock_count": rejected,
        "events_sha256": source.events_sha256,
        "manifest_sha256": source.manifest_sha256,
        "events_artifact_name": source.events_artifact_name,
        "manifest_artifact_name": source.manifest_artifact_name,
        "semantic_clock_sha256": semantic_clock_sha256,
        "clock_attestation": attestation,
        "event_versions": ordered,
        "policy": {
            "append_only": True,
            "upstream_first_known_cannot_precede_ledger_capture": True,
            "calendar_is_not_direction": True,
            "research_only": True,
            "can_place_orders": False,
            "can_promote": False,
        },
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }


_IMMUTABLE_TABLES = (
    "event_clock_ledger_identity",
    "event_clock_snapshots",
    "event_clock_versions",
    "snapshot_events",
    "event_clock_capture_attestations",
    "event_clock_observations",
)


def _preflight_existing_ledger_identity(database_path: Path) -> None:
    """Reject a misrouted database before SQLite can mutate it.

    V1 and V2 deliberately use similar normalized tables.  Default-path
    separation is therefore insufficient: an explicitly wrong ``--database``
    must fail before schema creation, journal creation, or inserts can touch a
    preserved legacy artifact.
    """

    if not database_path.exists() or database_path.stat().st_size == 0:
        return
    uri = f"file:{database_path.resolve().as_posix()}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=10.0)
        connection.row_factory = sqlite3.Row
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "event_clock_snapshots" not in tables:
            raise EventClockLedgerError("existing_database_not_event_clock_ledger")
        identity_rows: list[sqlite3.Row] = []
        if "event_clock_ledger_identity" in tables:
            identity_rows = connection.execute(
                """SELECT singleton,schema_version,contract_id,source_pipeline_version
                   FROM event_clock_ledger_identity"""
            ).fetchall()
            if len(identity_rows) != 1 or int(identity_rows[0]["singleton"]) != 1:
                raise EventClockLedgerError("existing_ledger_identity_invalid")
            identity = identity_rows[0]
            if (
                str(identity["schema_version"] or "") != SCHEMA_VERSION
                or str(identity["contract_id"] or "") != CONTRACT_ID
                or str(identity["source_pipeline_version"] or "")
                != EXPECTED_SOURCE_PIPELINE_VERSION
            ):
                raise EventClockLedgerError("existing_ledger_identity_mismatch")
        snapshot_identities = connection.execute(
            """SELECT DISTINCT schema_version,contract_id
               FROM event_clock_snapshots"""
        ).fetchall()
        if not snapshot_identities and not identity_rows:
            raise EventClockLedgerError("existing_ledger_identity_unproven")
        for row in snapshot_identities:
            if (
                str(row["schema_version"] or "") != SCHEMA_VERSION
                or str(row["contract_id"] or "") != CONTRACT_ID
            ):
                raise EventClockLedgerError("existing_ledger_identity_mismatch")
    except sqlite3.Error as exc:
        raise EventClockLedgerError(f"existing_ledger_identity_read_failed:{exc}") from exc
    finally:
        try:
            connection.close()
        except UnboundLocalError:
            pass


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        f"""
        PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS event_clock_ledger_identity (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            schema_version TEXT NOT NULL,
            contract_id TEXT NOT NULL,
            source_pipeline_version TEXT NOT NULL
        );
        INSERT OR IGNORE INTO event_clock_ledger_identity(
            singleton,schema_version,contract_id,source_pipeline_version
        ) VALUES (
            1,'{SCHEMA_VERSION}','{CONTRACT_ID}','{EXPECTED_SOURCE_PIPELINE_VERSION}'
        );
        CREATE TABLE IF NOT EXISTS event_clock_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            contract_id TEXT NOT NULL,
            captured_utc TEXT NOT NULL,
            source_generated_utc TEXT NOT NULL,
            source_pipeline_version TEXT NOT NULL,
            source_event_count INTEGER NOT NULL,
            source_scheduled_clock_count INTEGER NOT NULL,
            rejected_malformed_clock_count INTEGER NOT NULL,
            events_sha256 TEXT NOT NULL,
            manifest_sha256 TEXT NOT NULL,
            events_artifact_name TEXT NOT NULL,
            manifest_artifact_name TEXT NOT NULL,
            snapshot_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_event_clock_snapshots_cutoff
            ON event_clock_snapshots(captured_utc, source_generated_utc);

        CREATE TABLE IF NOT EXISTS event_clock_versions (
            event_version_id TEXT PRIMARY KEY,
            upstream_event_id TEXT NOT NULL,
            scheduled_utc TEXT NOT NULL,
            event_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_event_clock_versions_event
            ON event_clock_versions(upstream_event_id, scheduled_utc);

        CREATE TABLE IF NOT EXISTS snapshot_events (
            snapshot_id TEXT NOT NULL,
            event_version_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            effective_known_utc TEXT NOT NULL,
            PRIMARY KEY(snapshot_id, event_version_id),
            FOREIGN KEY(snapshot_id) REFERENCES event_clock_snapshots(snapshot_id),
            FOREIGN KEY(event_version_id) REFERENCES event_clock_versions(event_version_id)
        );
        CREATE INDEX IF NOT EXISTS idx_snapshot_events_known
            ON snapshot_events(snapshot_id, effective_known_utc);

        CREATE TABLE IF NOT EXISTS event_clock_capture_attestations (
            snapshot_id TEXT PRIMARY KEY,
            attestation_state TEXT NOT NULL,
            attested INTEGER NOT NULL,
            generated_utc TEXT,
            artifact_name TEXT,
            payload_sha256 TEXT,
            attestation_json TEXT NOT NULL,
            FOREIGN KEY(snapshot_id) REFERENCES event_clock_snapshots(snapshot_id)
        );
        CREATE TABLE IF NOT EXISTS event_clock_observations (
            observation_id TEXT PRIMARY KEY,
            snapshot_id TEXT NOT NULL,
            observed_utc TEXT NOT NULL,
            source_generated_utc TEXT NOT NULL,
            semantic_clock_sha256 TEXT NOT NULL,
            events_sha256 TEXT NOT NULL,
            manifest_sha256 TEXT NOT NULL,
            attestation_state TEXT NOT NULL,
            attested INTEGER NOT NULL,
            attestation_payload_sha256 TEXT,
            observation_json TEXT NOT NULL,
            FOREIGN KEY(snapshot_id) REFERENCES event_clock_snapshots(snapshot_id)
        );
        CREATE INDEX IF NOT EXISTS idx_event_clock_observations_cutoff
            ON event_clock_observations(observed_utc, source_generated_utc);
        """
    )
    identity = connection.execute(
        """SELECT singleton,schema_version,contract_id,source_pipeline_version
           FROM event_clock_ledger_identity"""
    ).fetchall()
    if (
        len(identity) != 1
        or int(identity[0][0]) != 1
        or str(identity[0][1] or "") != SCHEMA_VERSION
        or str(identity[0][2] or "") != CONTRACT_ID
        or str(identity[0][3] or "") != EXPECTED_SOURCE_PIPELINE_VERSION
    ):
        raise EventClockLedgerError("ledger_identity_mismatch")
    for table in _IMMUTABLE_TABLES:
        connection.execute(
            f"""CREATE TRIGGER IF NOT EXISTS immutable_{table}_update
                BEFORE UPDATE ON {table}
                BEGIN SELECT RAISE(ABORT, 'append_only_ledger'); END"""
        )
        connection.execute(
            f"""CREATE TRIGGER IF NOT EXISTS immutable_{table}_delete
                BEFORE DELETE ON {table}
                BEGIN SELECT RAISE(ABORT, 'append_only_ledger'); END"""
        )


def append_snapshot(database_path: Path, snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Append one immutable source snapshot, idempotently by snapshot ID."""

    if str(snapshot.get("source_pipeline_version") or "") != EXPECTED_SOURCE_PIPELINE_VERSION:
        raise EventClockLedgerError("snapshot_pipeline_version_mismatch")

    _validate_snapshot_clock_attestation(snapshot)
    _preflight_existing_ledger_identity(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path, timeout=30.0)
    try:
        _create_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        existing = connection.execute(
            "SELECT snapshot_json FROM event_clock_snapshots WHERE snapshot_id=?",
            (str(snapshot["snapshot_id"]),),
        ).fetchone()
        canonical_snapshot = _canonical_json(snapshot)
        if existing is not None:
            if str(existing[0]) != canonical_snapshot:
                raise EventClockLedgerError("snapshot_id_content_collision")
            connection.rollback()
            return {
                "status": "already_present",
                "snapshot_id": snapshot["snapshot_id"],
                "event_count": int(snapshot["source_scheduled_clock_count"]),
            }
        for row in snapshot.get("event_versions") or []:
            event_json = _canonical_json(row)
            prior = connection.execute(
                "SELECT event_json FROM event_clock_versions WHERE event_version_id=?",
                (str(row["event_version_id"]),),
            ).fetchone()
            if prior is not None and str(prior[0]) != event_json:
                raise EventClockLedgerError("event_version_id_content_collision")
            connection.execute(
                """INSERT OR IGNORE INTO event_clock_versions(
                       event_version_id, upstream_event_id, scheduled_utc, event_json
                   ) VALUES (?, ?, ?, ?)""",
                (
                    str(row["event_version_id"]),
                    str(row["upstream_event_id"]),
                    str(row["scheduled_utc"]),
                    event_json,
                ),
            )
        connection.execute(
            """INSERT INTO event_clock_snapshots(
                   snapshot_id, schema_version, contract_id, captured_utc,
                   source_generated_utc, source_pipeline_version,
                   source_event_count, source_scheduled_clock_count,
                   rejected_malformed_clock_count, events_sha256, manifest_sha256,
                   events_artifact_name, manifest_artifact_name, snapshot_json
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(snapshot["snapshot_id"]),
                str(snapshot["schema_version"]),
                str(snapshot["contract_id"]),
                str(snapshot["captured_utc"]),
                str(snapshot["source_generated_utc"]),
                str(snapshot.get("source_pipeline_version") or ""),
                int(snapshot["source_event_count"]),
                int(snapshot["source_scheduled_clock_count"]),
                int(snapshot["rejected_malformed_clock_count"]),
                str(snapshot["events_sha256"]),
                str(snapshot["manifest_sha256"]),
                str(snapshot["events_artifact_name"]),
                str(snapshot["manifest_artifact_name"]),
                canonical_snapshot,
            ),
        )
        captured = _parse_utc(
            snapshot["captured_utc"], field="captured_utc", required=True
        )
        assert captured is not None
        for ordinal, row in enumerate(snapshot.get("event_versions") or []):
            upstream_known = _parse_utc(
                row.get("upstream_first_known_utc"),
                field="upstream_first_known_utc",
            )
            effective_known = max(
                value for value in (captured, upstream_known) if value is not None
            )
            connection.execute(
                """INSERT INTO snapshot_events(
                       snapshot_id, event_version_id, ordinal, effective_known_utc
                   ) VALUES (?, ?, ?, ?)""",
                (
                    str(snapshot["snapshot_id"]),
                    str(row["event_version_id"]),
                    ordinal,
                    _iso(effective_known),
                ),
            )
        attestation = dict(snapshot.get("clock_attestation") or {})
        connection.execute(
            """INSERT INTO event_clock_capture_attestations(
                   snapshot_id, attestation_state, attested, generated_utc,
                   artifact_name, payload_sha256, attestation_json
               ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                str(snapshot["snapshot_id"]),
                str(attestation.get("state") or "unattested_not_provided"),
                int(bool(attestation.get("attested"))),
                attestation.get("generated_utc"),
                attestation.get("artifact_name"),
                attestation.get("payload_sha256"),
                _canonical_json(attestation),
            ),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {
        "status": "appended",
        "snapshot_id": snapshot["snapshot_id"],
        "event_count": int(snapshot["source_scheduled_clock_count"]),
    }


def _validate_observation_reference(
    referenced_snapshot: Mapping[str, Any],
    attempted_snapshot: Mapping[str, Any],
) -> None:
    """Prove an observation cannot make a later snapshot visible earlier."""

    referenced_capture = _parse_utc(
        referenced_snapshot.get("captured_utc"),
        field="referenced_snapshot_captured_utc",
        required=True,
    )
    referenced_source_generated = _parse_utc(
        referenced_snapshot.get("source_generated_utc"),
        field="referenced_snapshot_source_generated_utc",
        required=True,
    )
    observed = _parse_utc(
        attempted_snapshot.get("captured_utc"),
        field="observation_observed_utc",
        required=True,
    )
    assert (
        referenced_capture is not None
        and referenced_source_generated is not None
        and observed is not None
    )
    if observed < referenced_capture:
        raise EventClockLedgerError("observation_precedes_referenced_snapshot_capture")
    if observed < referenced_source_generated:
        raise EventClockLedgerError("observation_precedes_referenced_source_generation")
    referenced_semantic = str(
        referenced_snapshot.get("semantic_clock_sha256") or ""
    )
    attempted_semantic = str(attempted_snapshot.get("semantic_clock_sha256") or "")
    if not attempted_semantic or attempted_semantic != referenced_semantic:
        raise EventClockLedgerError("observation_semantic_snapshot_mismatch")


def append_capture_observation(
    database_path: Path,
    *,
    snapshot_id: str,
    attempted_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Append proof that an unchanged semantic clock was observed again.

    Repeated polling must not create fake semantic revisions, but downstream
    consumers still need immutable evidence that the current view and host
    clock were freshly re-observed.  This table separates those concepts.
    """

    if (
        str(attempted_snapshot.get("source_pipeline_version") or "")
        != EXPECTED_SOURCE_PIPELINE_VERSION
    ):
        raise EventClockLedgerError("snapshot_pipeline_version_mismatch")
    _validate_snapshot_clock_attestation(attempted_snapshot)
    _preflight_existing_ledger_identity(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    try:
        _create_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT * FROM event_clock_snapshots WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone()
        if row is None:
            raise EventClockLedgerError("observation_snapshot_missing")
        referenced = _verify_snapshot_row(row)
        _validate_observation_reference(referenced, attempted_snapshot)
        referenced_semantic = str(referenced.get("semantic_clock_sha256") or "")
        attempted_semantic = str(attempted_snapshot.get("semantic_clock_sha256") or "")
        if not attempted_semantic or referenced_semantic != attempted_semantic:
            raise EventClockLedgerError("observation_semantic_snapshot_mismatch")
        attestation = dict(attempted_snapshot.get("clock_attestation") or {})
        material = {
            "snapshot_id": snapshot_id,
            "observed_utc": str(attempted_snapshot.get("captured_utc") or ""),
            "source_generated_utc": str(
                attempted_snapshot.get("source_generated_utc") or ""
            ),
            "semantic_clock_sha256": attempted_semantic,
            "events_sha256": str(attempted_snapshot.get("events_sha256") or ""),
            "manifest_sha256": str(attempted_snapshot.get("manifest_sha256") or ""),
            "clock_attestation": attestation,
        }
        observation_id = "event_clock_observation_" + _sha256_json(material)[:32]
        canonical = _canonical_json(material)
        existing = connection.execute(
            "SELECT observation_json FROM event_clock_observations WHERE observation_id=?",
            (observation_id,),
        ).fetchone()
        if existing is not None:
            if str(existing[0]) != canonical:
                raise EventClockLedgerError("observation_id_content_collision")
            connection.rollback()
            return {
                "status": "observation_already_present",
                "observation_id": observation_id,
                "snapshot_id": snapshot_id,
                "observed_utc": material["observed_utc"],
            }
        connection.execute(
            """INSERT INTO event_clock_observations(
                   observation_id,snapshot_id,observed_utc,source_generated_utc,
                   semantic_clock_sha256,events_sha256,manifest_sha256,
                   attestation_state,attested,attestation_payload_sha256,
                   observation_json
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                observation_id,
                snapshot_id,
                material["observed_utc"],
                material["source_generated_utc"],
                attempted_semantic,
                material["events_sha256"],
                material["manifest_sha256"],
                str(attestation.get("state") or "unattested_not_provided"),
                int(bool(attestation.get("attested"))),
                attestation.get("payload_sha256"),
                canonical,
            ),
        )
        connection.commit()
        return {
            "status": "observation_appended",
            "observation_id": observation_id,
            "snapshot_id": snapshot_id,
            "observed_utc": material["observed_utc"],
            "clock_attestation_state": str(attestation.get("state") or ""),
            "clock_attested": bool(attestation.get("attested")),
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _insert_snapshot_rows_atomic(
    connection: sqlite3.Connection,
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Insert one snapshot using the caller's already-open transaction."""

    if str(snapshot.get("source_pipeline_version") or "") != EXPECTED_SOURCE_PIPELINE_VERSION:
        raise EventClockLedgerError("snapshot_pipeline_version_mismatch")
    _validate_snapshot_clock_attestation(snapshot)

    snapshot_id = str(snapshot["snapshot_id"])
    canonical_snapshot = _canonical_json(snapshot)
    existing = connection.execute(
        "SELECT snapshot_json FROM event_clock_snapshots WHERE snapshot_id=?",
        (snapshot_id,),
    ).fetchone()
    if existing is not None:
        if str(existing[0]) != canonical_snapshot:
            raise EventClockLedgerError("snapshot_id_content_collision")
        return {
            "status": "already_present",
            "snapshot_id": snapshot_id,
            "event_count": int(snapshot["source_scheduled_clock_count"]),
        }
    for row in snapshot.get("event_versions") or []:
        event_json = _canonical_json(row)
        prior = connection.execute(
            "SELECT event_json FROM event_clock_versions WHERE event_version_id=?",
            (str(row["event_version_id"]),),
        ).fetchone()
        if prior is not None and str(prior[0]) != event_json:
            raise EventClockLedgerError("event_version_id_content_collision")
        connection.execute(
            """INSERT OR IGNORE INTO event_clock_versions(
                   event_version_id, upstream_event_id, scheduled_utc, event_json
               ) VALUES (?, ?, ?, ?)""",
            (
                str(row["event_version_id"]),
                str(row["upstream_event_id"]),
                str(row["scheduled_utc"]),
                event_json,
            ),
        )
    connection.execute(
        """INSERT INTO event_clock_snapshots(
               snapshot_id, schema_version, contract_id, captured_utc,
               source_generated_utc, source_pipeline_version,
               source_event_count, source_scheduled_clock_count,
               rejected_malformed_clock_count, events_sha256, manifest_sha256,
               events_artifact_name, manifest_artifact_name, snapshot_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            snapshot_id,
            str(snapshot["schema_version"]),
            str(snapshot["contract_id"]),
            str(snapshot["captured_utc"]),
            str(snapshot["source_generated_utc"]),
            str(snapshot.get("source_pipeline_version") or ""),
            int(snapshot["source_event_count"]),
            int(snapshot["source_scheduled_clock_count"]),
            int(snapshot["rejected_malformed_clock_count"]),
            str(snapshot["events_sha256"]),
            str(snapshot["manifest_sha256"]),
            str(snapshot["events_artifact_name"]),
            str(snapshot["manifest_artifact_name"]),
            canonical_snapshot,
        ),
    )
    captured = _parse_utc(snapshot["captured_utc"], field="captured_utc", required=True)
    assert captured is not None
    for ordinal, row in enumerate(snapshot.get("event_versions") or []):
        upstream_known = _parse_utc(
            row.get("upstream_first_known_utc"), field="upstream_first_known_utc"
        )
        effective_known = max(
            value for value in (captured, upstream_known) if value is not None
        )
        connection.execute(
            """INSERT INTO snapshot_events(
                   snapshot_id, event_version_id, ordinal, effective_known_utc
               ) VALUES (?, ?, ?, ?)""",
            (
                snapshot_id,
                str(row["event_version_id"]),
                ordinal,
                _iso(effective_known),
            ),
        )
    attestation = dict(snapshot.get("clock_attestation") or {})
    connection.execute(
        """INSERT INTO event_clock_capture_attestations(
               snapshot_id, attestation_state, attested, generated_utc,
               artifact_name, payload_sha256, attestation_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            snapshot_id,
            str(attestation.get("state") or "unattested_not_provided"),
            int(bool(attestation.get("attested"))),
            attestation.get("generated_utc"),
            attestation.get("artifact_name"),
            attestation.get("payload_sha256"),
            _canonical_json(attestation),
        ),
    )
    return {
        "status": "appended",
        "snapshot_id": snapshot_id,
        "event_count": int(snapshot["source_scheduled_clock_count"]),
    }


def _insert_observation_row_atomic(
    connection: sqlite3.Connection,
    *,
    snapshot_id: str,
    attempted_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Insert one observation using the caller's already-open transaction."""

    row = connection.execute(
        "SELECT * FROM event_clock_snapshots WHERE snapshot_id=?",
        (snapshot_id,),
    ).fetchone()
    if row is None:
        raise EventClockLedgerError("observation_snapshot_missing")
    referenced = _verify_snapshot_row(row)
    _validate_observation_reference(referenced, attempted_snapshot)
    referenced_semantic = str(referenced.get("semantic_clock_sha256") or "")
    attempted_semantic = str(attempted_snapshot.get("semantic_clock_sha256") or "")
    if not attempted_semantic or referenced_semantic != attempted_semantic:
        raise EventClockLedgerError("observation_semantic_snapshot_mismatch")
    attestation = dict(attempted_snapshot.get("clock_attestation") or {})
    material = {
        "snapshot_id": snapshot_id,
        "observed_utc": str(attempted_snapshot.get("captured_utc") or ""),
        "source_generated_utc": str(attempted_snapshot.get("source_generated_utc") or ""),
        "semantic_clock_sha256": attempted_semantic,
        "events_sha256": str(attempted_snapshot.get("events_sha256") or ""),
        "manifest_sha256": str(attempted_snapshot.get("manifest_sha256") or ""),
        "clock_attestation": attestation,
    }
    observation_id = "event_clock_observation_" + _sha256_json(material)[:32]
    canonical = _canonical_json(material)
    existing = connection.execute(
        "SELECT observation_json FROM event_clock_observations WHERE observation_id=?",
        (observation_id,),
    ).fetchone()
    if existing is not None:
        if str(existing[0]) != canonical:
            raise EventClockLedgerError("observation_id_content_collision")
        return {
            "status": "observation_already_present",
            "observation_id": observation_id,
            "snapshot_id": snapshot_id,
            "observed_utc": material["observed_utc"],
        }
    connection.execute(
        """INSERT INTO event_clock_observations(
               observation_id,snapshot_id,observed_utc,source_generated_utc,
               semantic_clock_sha256,events_sha256,manifest_sha256,
               attestation_state,attested,attestation_payload_sha256,
               observation_json
           ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (
            observation_id,
            snapshot_id,
            material["observed_utc"],
            material["source_generated_utc"],
            attempted_semantic,
            material["events_sha256"],
            material["manifest_sha256"],
            str(attestation.get("state") or "unattested_not_provided"),
            int(bool(attestation.get("attested"))),
            attestation.get("payload_sha256"),
            canonical,
        ),
    )
    return {
        "status": "observation_appended",
        "observation_id": observation_id,
        "snapshot_id": snapshot_id,
        "observed_utc": material["observed_utc"],
        "clock_attestation_state": str(attestation.get("state") or ""),
        "clock_attested": bool(attestation.get("attested")),
    }


def append_snapshot_and_observation_atomic(
    database_path: Path,
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Atomically append the semantic snapshot decision and its observation."""

    _validate_snapshot_clock_attestation(snapshot)
    _preflight_existing_ledger_identity(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    try:
        _create_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        prior = connection.execute(
            """SELECT s.snapshot_id,s.captured_utc,s.snapshot_json,
                      COALESCE(a.attested,0) AS attested
               FROM event_clock_snapshots s
               LEFT JOIN event_clock_capture_attestations a
                 ON a.snapshot_id=s.snapshot_id
               ORDER BY s.captured_utc DESC,s.snapshot_id DESC LIMIT 1"""
        ).fetchone()
        attestation = dict(snapshot.get("clock_attestation") or {})
        semantic = str(snapshot.get("semantic_clock_sha256") or "")
        retain_prior = False
        if prior is not None:
            prior_payload = json.loads(str(prior["snapshot_json"]))
            prior_semantic = str(prior_payload.get("semantic_clock_sha256") or "")
            retain_prior = prior_semantic == semantic and not (
                bool(attestation.get("attested")) and not bool(prior["attested"])
            )
        if retain_prior:
            snapshot_result = {
                "status": "semantically_unchanged",
                "snapshot_id": str(prior["snapshot_id"]),
                "retained_snapshot_captured_utc": str(prior["captured_utc"]),
                "event_count": int(snapshot["source_scheduled_clock_count"]),
            }
        else:
            snapshot_result = _insert_snapshot_rows_atomic(connection, snapshot)
        observation = _insert_observation_row_atomic(
            connection,
            snapshot_id=str(snapshot_result["snapshot_id"]),
            attempted_snapshot=snapshot,
        )
        connection.commit()
        return {
            **snapshot_result,
            "capture_attempt_utc": str(snapshot.get("captured_utc") or ""),
            "clock_observation_id": observation["observation_id"],
            "clock_observed_utc": observation["observed_utc"],
            "clock_attestation_state": str(attestation.get("state") or ""),
            "clock_attested": bool(attestation.get("attested")),
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def reconstruct_as_of(database_path: Path, cutoff_utc: dt.datetime) -> dict[str, Any]:
    """Reconstruct the last complete clock view captured by ``cutoff_utc``."""

    _preflight_existing_ledger_identity(database_path)
    cutoff = cutoff_utc.astimezone(UTC)
    uri = f"file:{database_path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=10.0)
    connection.row_factory = sqlite3.Row
    try:
        observation_exists = connection.execute(
            """SELECT 1 FROM sqlite_master
               WHERE type='table' AND name='event_clock_observations'"""
        ).fetchone()
        observation = (
            connection.execute(
                """SELECT * FROM event_clock_observations
                   WHERE observed_utc<=? AND source_generated_utc<=?
                   ORDER BY observed_utc DESC,observation_id DESC LIMIT 1""",
                (_iso(cutoff), _iso(cutoff)),
            ).fetchone()
            if observation_exists is not None
            else None
        )
        if observation is not None:
            snapshot = connection.execute(
                "SELECT * FROM event_clock_snapshots WHERE snapshot_id=?",
                (str(observation["snapshot_id"]),),
            ).fetchone()
            if snapshot is None:
                raise EventClockLedgerError("observation_referenced_snapshot_missing")
        else:
            snapshot = connection.execute(
                """SELECT * FROM event_clock_snapshots
                   WHERE captured_utc <= ? AND source_generated_utc <= ?
                   ORDER BY captured_utc DESC, snapshot_id DESC LIMIT 1""",
                (_iso(cutoff), _iso(cutoff)),
            ).fetchone()
        if snapshot is None:
            return {
                "schema_version": SCHEMA_VERSION,
                "contract_id": CONTRACT_ID,
                "decision_cutoff_utc": _iso(cutoff),
                "snapshot_id": None,
                "events": [],
                "event_count": 0,
                "research_only": True,
                "execution_eligible": False,
                "supported_execution_decision": "no_trade",
            }
        snapshot_payload = _verify_snapshot_row(snapshot)
        if str(snapshot["source_pipeline_version"] or "") != EXPECTED_SOURCE_PIPELINE_VERSION:
            raise EventClockLedgerError("stored_snapshot_pipeline_version_mismatch")
        selected_membership = _verify_snapshot_events_closure(
            connection,
            snapshot=snapshot,
            snapshot_payload=snapshot_payload,
        )
        observation_material = (
            _verify_observation_row(observation)
            if observation is not None
            else None
        )
        if observation_material is not None:
            _validate_observation_reference(
                snapshot_payload,
                {
                    "captured_utc": observation_material["observed_utc"],
                    "source_generated_utc": observation_material[
                        "source_generated_utc"
                    ],
                    "semantic_clock_sha256": observation_material[
                        "semantic_clock_sha256"
                    ],
                },
            )
        attestation_row = connection.execute(
            """SELECT *
               FROM event_clock_capture_attestations WHERE snapshot_id=?""",
            (str(snapshot["snapshot_id"]),),
        ).fetchone() if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            ("event_clock_capture_attestations",),
        ).fetchone() else None
        snapshot_capture = _parse_utc(
            snapshot["captured_utc"], field="snapshot_captured_utc", required=True
        )
        assert snapshot_capture is not None
        verified_snapshot_attestation = _verified_attestation(
            attestation_row,
            captured_utc=snapshot_capture,
        )
        if verified_snapshot_attestation is None:
            raise EventClockLedgerError("stored_snapshot_attestation_missing")
        if _canonical_json(verified_snapshot_attestation) != _canonical_json(
            dict(snapshot_payload.get("clock_attestation") or {})
        ):
            raise EventClockLedgerError("stored_snapshot_attestation_payload_mismatch")
        snapshot_clock_attestation = verified_snapshot_attestation
        clock_attestation = (
            dict(observation_material.get("clock_attestation") or {})
            if isinstance(observation_material, Mapping)
            else snapshot_clock_attestation
        )
        # Preserve when each parser/source contract first became observable in
        # this immutable ledger.  A new parser cohort may legitimately retain
        # an older fact timestamp, but it may never be reconstructed as if the
        # new contract existed before its first post-deployment snapshot.
        history_snapshots = connection.execute(
            """SELECT * FROM event_clock_snapshots
               WHERE captured_utc<=? AND source_generated_utc<=?
               ORDER BY captured_utc,snapshot_id""",
            (_iso(cutoff), _iso(cutoff)),
        ).fetchall()
        contract_first_observed: dict[tuple[str, str, str], dt.datetime] = {}
        event_version_first_observed: dict[str, dt.datetime] = {}
        contract_first_trusted_observed: dict[
            tuple[str, str, str], dt.datetime
        ] = {}
        event_version_first_trusted_observed: dict[str, dt.datetime] = {}
        for history_snapshot in history_snapshots:
            history_payload = _verify_snapshot_row(history_snapshot)
            if (
                str(history_snapshot["source_pipeline_version"] or "")
                != EXPECTED_SOURCE_PIPELINE_VERSION
            ):
                raise EventClockLedgerError(
                    "stored_history_snapshot_pipeline_version_mismatch"
                )
            history_membership = _verify_snapshot_events_closure(
                connection,
                snapshot=history_snapshot,
                snapshot_payload=history_payload,
            )
            captured = _parse_utc(
                history_snapshot["captured_utc"],
                field="source_contract_snapshot_captured_utc",
                required=True,
            )
            assert captured is not None
            history_attestation_row = connection.execute(
                """SELECT * FROM event_clock_capture_attestations
                   WHERE snapshot_id=?""",
                (str(history_snapshot["snapshot_id"]),),
            ).fetchone()
            history_attestation = _verified_attestation(
                history_attestation_row,
                captured_utc=captured,
            )
            if history_attestation is None:
                raise EventClockLedgerError("stored_snapshot_attestation_missing")
            if _canonical_json(history_attestation) != _canonical_json(
                dict(history_payload.get("clock_attestation") or {})
            ):
                raise EventClockLedgerError(
                    "stored_snapshot_attestation_payload_mismatch"
                )
            trusted = bool(
                history_attestation
                and history_attestation.get("attested") is True
                and str(history_attestation.get("state") or "") == "fresh_trusted"
            )
            for history_row in history_membership:
                history_event = json.loads(str(history_row["event_json"]))
                event_version_id = str(
                    history_event.get("event_version_id") or ""
                )
                prior_event = event_version_first_observed.get(event_version_id)
                if event_version_id and (
                    prior_event is None or captured < prior_event
                ):
                    event_version_first_observed[event_version_id] = captured
                if trusted:
                    prior_trusted_event = event_version_first_trusted_observed.get(
                        event_version_id
                    )
                    if event_version_id and (
                        prior_trusted_event is None or captured < prior_trusted_event
                    ):
                        event_version_first_trusted_observed[event_version_id] = captured
                key = (
                    str(history_event.get("source_id") or ""),
                    str(history_event.get("source_contract_id") or ""),
                    str(history_event.get("source_cohort_id") or ""),
                )
                if not key[1] or not key[2]:
                    continue
                prior = contract_first_observed.get(key)
                if prior is None or captured < prior:
                    contract_first_observed[key] = captured
                if trusted:
                    prior_trusted = contract_first_trusted_observed.get(key)
                    if prior_trusted is None or captured < prior_trusted:
                        contract_first_trusted_observed[key] = captured
        rows = [
            row
            for row in selected_membership
            if (
                _parse_utc(
                    row["effective_known_utc"],
                    field="effective_known_utc",
                    required=True,
                )
                <= cutoff
            )
        ]
        events = []
        for row in rows:
            event = _verify_stored_event(
                str(row["event_json"]),
                expected_event_version_id=str(row["event_version_id"]),
                expected_upstream_event_id=str(row["upstream_event_id"]),
                expected_scheduled_utc=str(row["scheduled_utc"]),
            )
            original_fact_known = _parse_utc(
                event.get("upstream_first_known_utc"),
                field="original_fact_known_utc",
            )
            selected_snapshot_observed = _parse_utc(
                snapshot["captured_utc"], field="event_snapshot_captured_utc", required=True
            )
            assert selected_snapshot_observed is not None
            snapshot_observed = event_version_first_observed.get(
                str(event.get("event_version_id") or ""),
                selected_snapshot_observed,
            )
            contract_key = (
                str(event.get("source_id") or ""),
                str(event.get("source_contract_id") or ""),
                str(event.get("source_cohort_id") or ""),
            )
            contract_observed = contract_first_observed.get(contract_key)
            raw_availability = max(
                value
                for value in (
                    original_fact_known,
                    snapshot_observed,
                    contract_observed,
                )
                if value is not None
            )
            snapshot_trusted_observed = event_version_first_trusted_observed.get(
                str(event.get("event_version_id") or "")
            )
            contract_trusted_observed = contract_first_trusted_observed.get(
                contract_key
            )
            availability = (
                max(
                    value
                    for value in (
                        original_fact_known,
                        snapshot_trusted_observed,
                        contract_trusted_observed,
                    )
                    if value is not None
                )
                if snapshot_trusted_observed is not None
                and contract_trusted_observed is not None
                else None
            )
            event["original_fact_known_utc"] = (
                _iso(original_fact_known) if original_fact_known else ""
            )
            event["event_snapshot_first_observed_utc"] = _iso(snapshot_observed)
            event["source_contract_first_observed_utc"] = (
                _iso(contract_observed) if contract_observed else ""
            )
            event["raw_event_availability_utc"] = _iso(raw_availability)
            event["event_snapshot_first_trusted_observed_utc"] = (
                _iso(snapshot_trusted_observed) if snapshot_trusted_observed else ""
            )
            event["source_contract_first_trusted_observed_utc"] = (
                _iso(contract_trusted_observed) if contract_trusted_observed else ""
            )
            event["event_availability_utc"] = (
                _iso(availability) if availability else ""
            )
            event["ledger_effective_known_utc"] = (
                _iso(availability) if availability else ""
            )
            event["trusted_for_prospective_evidence"] = availability is not None
            events.append(event)
        return {
            "schema_version": SCHEMA_VERSION,
            "contract_id": CONTRACT_ID,
            "decision_cutoff_utc": _iso(cutoff),
            "snapshot_id": str(snapshot["snapshot_id"]),
            "snapshot_captured_utc": str(snapshot["captured_utc"]),
            "clock_observation_id": (
                str(observation["observation_id"])
                if observation is not None
                else None
            ),
            "clock_observed_utc": (
                str(observation["observed_utc"])
                if observation is not None
                else str(snapshot["captured_utc"])
            ),
            "clock_observation_source_generated_utc": (
                str(observation["source_generated_utc"])
                if observation is not None
                else str(snapshot["source_generated_utc"])
            ),
            "clock_observation_events_sha256": (
                str(observation["events_sha256"])
                if observation is not None
                else str(snapshot["events_sha256"])
            ),
            "clock_observation_manifest_sha256": (
                str(observation["manifest_sha256"])
                if observation is not None
                else str(snapshot["manifest_sha256"])
            ),
            "clock_observation_semantic_clock_sha256": (
                str(observation["semantic_clock_sha256"])
                if observation is not None
                else str(
                    snapshot_payload.get(
                        "semantic_clock_sha256"
                    )
                    or ""
                )
            ),
            "source_generated_utc": str(snapshot["source_generated_utc"]),
            "source_pipeline_version": str(snapshot["source_pipeline_version"]),
            "events_sha256": str(snapshot["events_sha256"]),
            "manifest_sha256": str(snapshot["manifest_sha256"]),
            "semantic_snapshot_events_sha256": str(snapshot["events_sha256"]),
            "semantic_snapshot_manifest_sha256": str(snapshot["manifest_sha256"]),
            "clock_attestation": clock_attestation,
            "snapshot_clock_attestation": snapshot_clock_attestation,
            "event_count": len(events),
            "events": events,
            "policy": {
                "calendar_is_not_direction": True,
                "as_of_uses_last_observed_complete_snapshot": True,
                "source_contract_versions_cannot_be_backdated": True,
                "event_availability_is_maximum_causal_clock": True,
                "proof_availability_requires_fresh_trusted_capture": True,
                "research_only": True,
                "can_place_orders": False,
                "can_promote": False,
            },
            "research_only": True,
            "execution_eligible": False,
            "supported_execution_decision": "no_trade",
        }
    finally:
        connection.close()


def _latest_snapshot_summary(database_path: Path) -> dict[str, Any] | None:
    if not database_path.is_file():
        return None
    uri = f"file:{database_path.resolve().as_posix()}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=10.0)
        connection.row_factory = sqlite3.Row
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_clock_snapshots'"
        ).fetchone()
        if exists is None:
            return None
        row = connection.execute(
            """SELECT snapshot_id, captured_utc, snapshot_json
               FROM event_clock_snapshots
               ORDER BY captured_utc DESC, snapshot_id DESC LIMIT 1"""
        ).fetchone()
        if row is None:
            return None
        payload = json.loads(str(row["snapshot_json"]))
        semantic_hash = str(payload.get("semantic_clock_sha256") or "")
        if not semantic_hash:
            semantic_hash = _semantic_clock_hash(payload.get("event_versions") or [])
        attestation_exists = connection.execute(
            """SELECT 1 FROM sqlite_master
               WHERE type='table' AND name='event_clock_capture_attestations'"""
        ).fetchone()
        attestation_row = (
            connection.execute(
                """SELECT attestation_state, attested
                   FROM event_clock_capture_attestations WHERE snapshot_id=?""",
                (str(row["snapshot_id"]),),
            ).fetchone()
            if attestation_exists is not None
            else None
        )
        return {
            "snapshot_id": str(row["snapshot_id"]),
            "captured_utc": str(row["captured_utc"]),
            "semantic_clock_sha256": semantic_hash,
            "attestation_state": (
                str(attestation_row["attestation_state"])
                if attestation_row is not None
                else "legacy_unattested"
            ),
            "attested": bool(attestation_row["attested"])
            if attestation_row is not None
            else False,
        }
    except (OSError, sqlite3.Error, ValueError, json.JSONDecodeError):
        return None
    finally:
        try:
            connection.close()
        except UnboundLocalError:
            pass


def collect_to_ledger(
    *,
    events_path: Path,
    manifest_path: Path,
    database_path: Path,
    captured_utc: dt.datetime | None = None,
    clock_integrity_path: Path | None = None,
    maximum_clock_attestation_age_sec: float = 300.0,
) -> dict[str, Any]:
    capture = (captured_utc or dt.datetime.now(UTC)).astimezone(UTC)
    source = read_stable_source(events_path, manifest_path)
    attestation = build_clock_attestation(
        clock_integrity_path,
        capture,
        maximum_age_sec=maximum_clock_attestation_age_sec,
    )
    snapshot = build_snapshot(source, capture, clock_attestation=attestation)
    prior = _latest_snapshot_summary(database_path)
    if (
        prior is not None
        and prior["semantic_clock_sha256"] == snapshot["semantic_clock_sha256"]
        and not (bool(attestation.get("attested")) and not bool(prior.get("attested")))
    ):
        observation = append_capture_observation(
            database_path,
            snapshot_id=str(prior["snapshot_id"]),
            attempted_snapshot=snapshot,
        )
        return {
            "status": "semantically_unchanged",
            "snapshot_id": prior["snapshot_id"],
            "retained_snapshot_captured_utc": prior["captured_utc"],
            "capture_attempt_utc": snapshot["captured_utc"],
            "clock_observation_id": observation["observation_id"],
            "clock_observed_utc": observation["observed_utc"],
            "event_count": int(snapshot["source_scheduled_clock_count"]),
            "clock_attestation_state": str(attestation.get("state") or ""),
            "clock_attested": bool(attestation.get("attested")),
        }
    result = append_snapshot(database_path, snapshot)
    observation = append_capture_observation(
        database_path,
        snapshot_id=str(snapshot["snapshot_id"]),
        attempted_snapshot=snapshot,
    )
    return {
        **result,
        "captured_utc": snapshot["captured_utc"],
        "clock_observation_id": observation["observation_id"],
        "clock_observed_utc": observation["observed_utc"],
        "clock_attestation_state": str(attestation.get("state") or ""),
        "clock_attested": bool(attestation.get("attested")),
    }


def snapshot_count(database_path: Path) -> int:
    if not database_path.exists():
        return 0
    uri = f"file:{database_path.resolve().as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=10.0) as connection:
        row = connection.execute("SELECT COUNT(*) FROM event_clock_snapshots").fetchone()
    return int(row[0] if row else 0)
