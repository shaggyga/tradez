"""Strict read-only reader for the retired immutable event-clock V1 ledger.

V1 is historical evidence.  It must never be opened through the V2 writer or
silently reinterpreted as a V2 cohort.  This module consequently exposes only
identity inspection and point-in-time reconstruction.  SQLite is opened with
both ``mode=ro`` and ``immutable=1``; there is no schema creation, migration,
append, checkpoint, or journal code in this module.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping


UTC = dt.timezone.utc
SCHEMA_VERSION = "immutable_event_clock_ledger_v1"
CONTRACT_ID = "immutable_point_in_time_event_clock_v1_20260817"
EXPECTED_SOURCE_PIPELINE_VERSION = "all_pair_news_event_tags_v3"
SOURCE_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RETIREMENT_FINGERPRINT = (
    SOURCE_ROOT / "config" / "immutable_event_clock_v1_retirement.json"
)
RETIREMENT_RECORD_ID = "immutable_event_clock_v1_retirement_20260817"


class ImmutableEventClockV1ReadError(RuntimeError):
    """Raised when a database is not exact, intact V1 historical evidence."""


def _parse_utc(value: Any, *, field: str) -> dt.datetime:
    text = str(value or "").strip()
    if not text:
        raise ImmutableEventClockV1ReadError(f"missing_{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise ImmutableEventClockV1ReadError(f"invalid_{field}") from exc
    if parsed.tzinfo is None:
        raise ImmutableEventClockV1ReadError(f"naive_{field}")
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


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _load_retirement_fingerprint(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ImmutableEventClockV1ReadError(
            "v1_retirement_fingerprint_unavailable"
        ) from exc
    if not isinstance(payload, dict):
        raise ImmutableEventClockV1ReadError("v1_retirement_fingerprint_invalid")
    if payload.get("schema_version") != 1:
        raise ImmutableEventClockV1ReadError("v1_retirement_schema_mismatch")
    if payload.get("record_type") != "immutable_event_clock_retirement_fingerprint":
        raise ImmutableEventClockV1ReadError("v1_retirement_record_type_mismatch")
    if payload.get("status") != "retired_terminal":
        raise ImmutableEventClockV1ReadError("v1_retirement_status_mismatch")
    if payload.get("clock_schema_version") != SCHEMA_VERSION:
        raise ImmutableEventClockV1ReadError("v1_retirement_clock_schema_mismatch")
    if payload.get("clock_contract_id") != CONTRACT_ID:
        raise ImmutableEventClockV1ReadError("v1_retirement_clock_contract_mismatch")
    if payload.get("source_pipeline_version") not in (
        None,
        EXPECTED_SOURCE_PIPELINE_VERSION,
    ):
        raise ImmutableEventClockV1ReadError("v1_retirement_pipeline_mismatch")
    if not str(payload.get("frozen_at_utc") or ""):
        raise ImmutableEventClockV1ReadError("v1_retirement_frozen_time_missing")
    _parse_utc(payload["frozen_at_utc"], field="retirement_frozen_at_utc")
    if "database_last_write_utc" in payload:
        _parse_utc(
            payload["database_last_write_utc"],
            field="retirement_database_last_write_utc",
        )
    database = payload.get("database")
    if not isinstance(database, Mapping):
        raise ImmutableEventClockV1ReadError("v1_retirement_database_fingerprint_missing")
    if database.get("preserve_bytes") is not True:
        raise ImmutableEventClockV1ReadError("v1_retirement_preservation_not_required")
    if database.get("quick_check") != "ok":
        raise ImmutableEventClockV1ReadError("v1_retirement_quick_check_not_frozen_ok")
    counts = database.get("row_counts")
    if not isinstance(counts, Mapping):
        raise ImmutableEventClockV1ReadError("v1_retirement_row_counts_missing")
    policy = payload.get("policy")
    if policy is not None:
        if not isinstance(policy, Mapping) or any(
            policy.get(field) is not True
            for field in (
                "writer_retired",
                "no_append",
                "no_checkpoint",
                "no_migration",
                "no_backfill",
            )
        ):
            raise ImmutableEventClockV1ReadError("v1_retirement_policy_invalid")
    return payload


def _connect(
    database_path: Path,
    *,
    retirement_fingerprint_path: Path,
) -> sqlite3.Connection:
    path = Path(database_path)
    if not path.is_file() or path.stat().st_size <= 0:
        raise ImmutableEventClockV1ReadError("v1_ledger_unavailable")
    fingerprint_path = Path(retirement_fingerprint_path)
    fingerprint = _load_retirement_fingerprint(fingerprint_path)
    if fingerprint_path.resolve() == DEFAULT_RETIREMENT_FINGERPRINT.resolve():
        if fingerprint.get("record_id") != RETIREMENT_RECORD_ID:
            raise ImmutableEventClockV1ReadError("v1_retirement_record_id_mismatch")
        expected_path = SOURCE_ROOT / str(
            (fingerprint.get("database") or {}).get("relative_path") or ""
        )
        if path.resolve() != expected_path.resolve():
            raise ImmutableEventClockV1ReadError("v1_retirement_database_path_mismatch")
    database_fingerprint = dict(fingerprint["database"])
    if path.stat().st_size != int(database_fingerprint.get("byte_length") or -1):
        raise ImmutableEventClockV1ReadError("v1_retirement_byte_length_mismatch")
    if _sha256_path(path) != str(database_fingerprint.get("sha256") or ""):
        raise ImmutableEventClockV1ReadError("v1_retirement_sha256_mismatch")
    uri = f"file:{path.resolve().as_posix()}?mode=ro&immutable=1"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        if quick_check != str(database_fingerprint["quick_check"]):
            raise ImmutableEventClockV1ReadError("v1_retirement_quick_check_mismatch")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        for table, expected in dict(database_fingerprint["row_counts"]).items():
            if table not in tables:
                raise ImmutableEventClockV1ReadError(
                    f"v1_retirement_row_count_table_missing:{table}"
                )
            observed = int(
                connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            if observed != int(expected):
                raise ImmutableEventClockV1ReadError(
                    f"v1_retirement_row_count_mismatch:{table}"
                )
        return connection
    except sqlite3.Error as exc:
        if connection is not None:
            connection.close()
        raise ImmutableEventClockV1ReadError("v1_ledger_open_failed") from exc
    except Exception:
        if connection is not None:
            connection.close()
        raise


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def _validate_identity(connection: sqlite3.Connection) -> dict[str, Any]:
    tables = _table_names(connection)
    required = {
        "event_clock_snapshots",
        "event_clock_versions",
        "snapshot_events",
    }
    if not required.issubset(tables):
        raise ImmutableEventClockV1ReadError("v1_required_schema_missing")

    if "event_clock_ledger_identity" in tables:
        rows = connection.execute(
            """SELECT singleton,schema_version,contract_id,source_pipeline_version
                 FROM event_clock_ledger_identity"""
        ).fetchall()
        if len(rows) != 1 or int(rows[0]["singleton"]) != 1:
            raise ImmutableEventClockV1ReadError("v1_identity_row_invalid")
        identity = rows[0]
        if (
            str(identity["schema_version"] or "") != SCHEMA_VERSION
            or str(identity["contract_id"] or "") != CONTRACT_ID
            or str(identity["source_pipeline_version"] or "")
            != EXPECTED_SOURCE_PIPELINE_VERSION
        ):
            raise ImmutableEventClockV1ReadError("v1_identity_mismatch")

    identities = connection.execute(
        """SELECT DISTINCT schema_version,contract_id,source_pipeline_version
             FROM event_clock_snapshots"""
    ).fetchall()
    if not identities and "event_clock_ledger_identity" not in tables:
        raise ImmutableEventClockV1ReadError("v1_identity_unproven")
    for row in identities:
        if (
            str(row["schema_version"] or "") != SCHEMA_VERSION
            or str(row["contract_id"] or "") != CONTRACT_ID
            or str(row["source_pipeline_version"] or "")
            != EXPECTED_SOURCE_PIPELINE_VERSION
        ):
            raise ImmutableEventClockV1ReadError("v1_identity_mismatch")
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "source_pipeline_version": EXPECTED_SOURCE_PIPELINE_VERSION,
        "snapshot_count": int(
            connection.execute("SELECT COUNT(*) FROM event_clock_snapshots").fetchone()[0]
        ),
        "read_only": True,
        "historical_only": True,
    }


def inspect_v1_identity(
    database_path: Path,
    *,
    retirement_fingerprint_path: Path = DEFAULT_RETIREMENT_FINGERPRINT,
) -> dict[str, Any]:
    """Return the exact V1 identity without changing any database byte."""

    connection = _connect(
        Path(database_path), retirement_fingerprint_path=retirement_fingerprint_path
    )
    try:
        return _validate_identity(connection)
    except sqlite3.Error as exc:
        raise ImmutableEventClockV1ReadError("v1_identity_read_failed") from exc
    finally:
        connection.close()


def _snapshot_payload(row: sqlite3.Row) -> dict[str, Any]:
    try:
        payload = json.loads(str(row["snapshot_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ImmutableEventClockV1ReadError("v1_snapshot_json_invalid") from exc
    if not isinstance(payload, dict):
        raise ImmutableEventClockV1ReadError("v1_snapshot_json_not_mapping")
    exact = {
        "snapshot_id": str(row["snapshot_id"]),
        "schema_version": str(row["schema_version"]),
        "contract_id": str(row["contract_id"]),
        "captured_utc": str(row["captured_utc"]),
        "source_generated_utc": str(row["source_generated_utc"]),
        "source_pipeline_version": str(row["source_pipeline_version"]),
        "source_event_count": int(row["source_event_count"]),
        "source_scheduled_clock_count": int(row["source_scheduled_clock_count"]),
        "rejected_malformed_clock_count": int(row["rejected_malformed_clock_count"]),
        "events_sha256": str(row["events_sha256"]),
        "manifest_sha256": str(row["manifest_sha256"]),
        "events_artifact_name": str(row["events_artifact_name"]),
        "manifest_artifact_name": str(row["manifest_artifact_name"]),
    }
    for key, expected in exact.items():
        observed = payload.get(key)
        if isinstance(expected, int):
            try:
                observed = int(observed)
            except (TypeError, ValueError):
                raise ImmutableEventClockV1ReadError(
                    f"v1_snapshot_{key}_mismatch"
                ) from None
        else:
            observed = str(observed or "")
        if observed != expected:
            raise ImmutableEventClockV1ReadError(f"v1_snapshot_{key}_mismatch")
    if payload.get("research_only") is not True:
        raise ImmutableEventClockV1ReadError("v1_snapshot_not_research_only")
    if payload.get("execution_eligible") is not False:
        raise ImmutableEventClockV1ReadError("v1_snapshot_execution_eligible")
    if payload.get("supported_execution_decision") != "no_trade":
        raise ImmutableEventClockV1ReadError("v1_snapshot_decision_mismatch")
    if not isinstance(payload.get("event_versions"), list):
        raise ImmutableEventClockV1ReadError("v1_snapshot_events_invalid")
    return payload


def _observation_reference(
    connection: sqlite3.Connection,
    *,
    cutoff: dt.datetime,
) -> dict[str, Any] | None:
    if "event_clock_observations" not in _table_names(connection):
        return None
    row = connection.execute(
        """SELECT *
             FROM event_clock_observations
            WHERE observed_utc<=? AND source_generated_utc<=?
            ORDER BY observed_utc DESC,observation_id DESC LIMIT 1""",
        (_iso(cutoff), _iso(cutoff)),
    ).fetchone()
    if row is None:
        return None
    try:
        material = json.loads(str(row["observation_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ImmutableEventClockV1ReadError("v1_observation_json_invalid") from exc
    if not isinstance(material, dict):
        raise ImmutableEventClockV1ReadError("v1_observation_json_not_mapping")
    expected_id = "event_clock_observation_" + hashlib.sha256(
        _canonical_json(material).encode("utf-8")
    ).hexdigest()[:32]
    if str(row["observation_id"]) != expected_id:
        raise ImmutableEventClockV1ReadError("v1_observation_content_address_mismatch")
    for key in (
        "snapshot_id",
        "observed_utc",
        "source_generated_utc",
        "semantic_clock_sha256",
        "events_sha256",
        "manifest_sha256",
    ):
        if str(material.get(key) or "") != str(row[key] or ""):
            raise ImmutableEventClockV1ReadError(f"v1_observation_{key}_mismatch")
    observed = _parse_utc(material["observed_utc"], field="observation_observed_utc")
    generated = _parse_utc(
        material["source_generated_utc"], field="observation_source_generated_utc"
    )
    if generated > observed:
        raise ImmutableEventClockV1ReadError(
            "v1_observation_source_generated_after_observed"
        )
    attestation = material.get("clock_attestation")
    if not isinstance(attestation, Mapping):
        raise ImmutableEventClockV1ReadError("v1_observation_attestation_invalid")
    if "attestation_state" in row.keys() and str(attestation.get("state") or "") != str(
        row["attestation_state"] or ""
    ):
        raise ImmutableEventClockV1ReadError("v1_observation_attestation_state_mismatch")
    if "attested" in row.keys() and int(bool(attestation.get("attested"))) != int(
        row["attested"]
    ):
        raise ImmutableEventClockV1ReadError("v1_observation_attested_mismatch")
    if "attestation_payload_sha256" in row.keys() and str(
        attestation.get("payload_sha256") or ""
    ) != str(row["attestation_payload_sha256"] or ""):
        raise ImmutableEventClockV1ReadError("v1_observation_attestation_hash_mismatch")
    return {
        "observation_id": str(row["observation_id"]),
        **material,
        "observed_datetime": observed,
        "source_generated_datetime": generated,
    }


_HISTORICAL_EVENT_STRING_FIELDS = frozenset(
    {
        "category",
        "clock_semantic_id",
        "event_time_basis",
        "event_utc",
        "event_version_id",
        "headline",
        "schedule_window_end_utc",
        "scheduled_utc",
        "source_name",
        "source_type",
        "source_url",
        "timing_precision",
        "upstream_event_id",
        "upstream_first_known_utc",
        "upstream_updated_utc",
    }
)
_HISTORICAL_EVENT_STRING_LIST_FIELDS = frozenset(
    {"currencies", "direct_currencies"}
)
_HISTORICAL_EVENT_BOOLEAN_FIELDS = frozenset({"source_verified"})
HISTORICAL_EVENT_SAFE_SOURCE_FIELDS = frozenset(
    {
        *_HISTORICAL_EVENT_STRING_FIELDS,
        *_HISTORICAL_EVENT_STRING_LIST_FIELDS,
        *_HISTORICAL_EVENT_BOOLEAN_FIELDS,
    }
)
HISTORICAL_EVENT_SAFE_OUTPUT_FIELDS = frozenset(
    {
        *HISTORICAL_EVENT_SAFE_SOURCE_FIELDS,
        "ledger_effective_known_utc",
        "event_availability_utc",
        "historical_v1_only",
        "research_only",
        "direction_policy",
        "execution_eligible",
        "can_place_orders",
        "can_promote",
        "can_authorize",
    }
)


def _historical_event_safe_view(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return the closed, neutral schema exposed by the retired V1 reader.

    The immutable ledger retains its original bytes, including any historical
    diagnostics an upstream producer may have written.  Consumers do not get a
    recursively filtered copy of that arbitrary payload: they get this fixed
    allowlisted clock/provenance projection only.  Consequently a new spelling
    for direction, side, units, notional, authorization, or execution cannot
    evade the boundary.
    """

    output: dict[str, Any] = {}
    for field in sorted(_HISTORICAL_EVENT_STRING_FIELDS):
        raw = value.get(field)
        if raw is not None and not isinstance(raw, str):
            raise ImmutableEventClockV1ReadError(
                f"v1_historical_event_field_type_invalid:{field}"
            )
        output[field] = raw
    for field in sorted(_HISTORICAL_EVENT_STRING_LIST_FIELDS):
        raw = value.get(field, [])
        if not isinstance(raw, list) or not all(
            isinstance(item, str) for item in raw
        ):
            raise ImmutableEventClockV1ReadError(
                f"v1_historical_event_field_type_invalid:{field}"
            )
        output[field] = list(raw)
    for field in sorted(_HISTORICAL_EVENT_BOOLEAN_FIELDS):
        raw = value.get(field)
        if raw is not None and type(raw) is not bool:
            raise ImmutableEventClockV1ReadError(
                f"v1_historical_event_field_type_invalid:{field}"
            )
        output[field] = raw
    output.update(
        {
            "research_only": True,
            "direction_policy": "abstain",
            "execution_eligible": False,
            "can_place_orders": False,
            "can_promote": False,
            "can_authorize": False,
        }
    )
    return output


def reconstruct_v1_as_of(
    database_path: Path,
    cutoff_utc: dt.datetime,
    *,
    retirement_fingerprint_path: Path = DEFAULT_RETIREMENT_FINGERPRINT,
) -> dict[str, Any]:
    """Reconstruct V1 knowledge at ``cutoff_utc`` without any write surface."""

    if cutoff_utc.tzinfo is None:
        raise ImmutableEventClockV1ReadError("naive_decision_cutoff_utc")
    cutoff = cutoff_utc.astimezone(UTC)
    connection = _connect(
        Path(database_path), retirement_fingerprint_path=retirement_fingerprint_path
    )
    try:
        _validate_identity(connection)
        observation = _observation_reference(connection, cutoff=cutoff)
        snapshot_id = str(observation.get("snapshot_id") or "") if observation else ""
        if snapshot_id:
            snapshot = connection.execute(
                "SELECT * FROM event_clock_snapshots WHERE snapshot_id=?",
                (snapshot_id,),
            ).fetchone()
            if snapshot is None:
                raise ImmutableEventClockV1ReadError(
                    "v1_observation_referenced_snapshot_missing"
                )
        else:
            snapshot = connection.execute(
                """SELECT * FROM event_clock_snapshots
                    WHERE captured_utc<=? AND source_generated_utc<=?
                    ORDER BY captured_utc DESC,snapshot_id DESC LIMIT 1""",
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
                "historical_only": True,
                "research_only": True,
                "execution_eligible": False,
                "can_place_orders": False,
                "can_promote": False,
                "can_authorize": False,
                "supported_execution_decision": "no_trade",
            }

        payload = _snapshot_payload(snapshot)
        captured = _parse_utc(snapshot["captured_utc"], field="snapshot_captured_utc")
        generated = _parse_utc(
            snapshot["source_generated_utc"], field="snapshot_source_generated_utc"
        )
        if generated > captured or captured > cutoff:
            raise ImmutableEventClockV1ReadError("v1_snapshot_clock_order_invalid")
        if observation is not None:
            observed = observation["observed_datetime"]
            observation_generated = observation["source_generated_datetime"]
            if observed < captured:
                raise ImmutableEventClockV1ReadError(
                    "v1_observation_precedes_referenced_snapshot_capture"
                )
            if observed < generated or observed < observation_generated:
                raise ImmutableEventClockV1ReadError(
                    "v1_observation_precedes_referenced_source_generation"
                )
            if str(observation.get("semantic_clock_sha256") or "") != str(
                payload.get("semantic_clock_sha256") or ""
            ):
                raise ImmutableEventClockV1ReadError(
                    "v1_observation_semantic_snapshot_mismatch"
                )

        expected_events = list(payload["event_versions"])
        membership = connection.execute(
            """SELECT se.ordinal,se.event_version_id,se.effective_known_utc,
                      ev.upstream_event_id,ev.scheduled_utc,ev.event_json
                 FROM snapshot_events AS se
                 JOIN event_clock_versions AS ev
                   ON ev.event_version_id=se.event_version_id
                WHERE se.snapshot_id=? ORDER BY se.ordinal""",
            (str(snapshot["snapshot_id"]),),
        ).fetchall()
        if len(membership) != len(expected_events):
            raise ImmutableEventClockV1ReadError("v1_snapshot_membership_count_mismatch")

        output_events: list[dict[str, Any]] = []
        for ordinal, (row, expected) in enumerate(zip(membership, expected_events)):
            if int(row["ordinal"]) != ordinal:
                raise ImmutableEventClockV1ReadError(
                    "v1_snapshot_membership_order_mismatch"
                )
            try:
                stored = json.loads(str(row["event_json"]))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ImmutableEventClockV1ReadError(
                    "v1_event_json_invalid"
                ) from exc
            if not isinstance(stored, dict) or _canonical_json(stored) != _canonical_json(
                expected
            ):
                raise ImmutableEventClockV1ReadError(
                    "v1_snapshot_event_payload_mismatch"
                )
            if str(row["event_version_id"]) != str(stored.get("event_version_id") or ""):
                raise ImmutableEventClockV1ReadError("v1_event_version_id_mismatch")
            if str(row["upstream_event_id"]) != str(
                stored.get("upstream_event_id") or ""
            ):
                raise ImmutableEventClockV1ReadError("v1_upstream_event_id_mismatch")
            if str(row["scheduled_utc"]) != str(stored.get("scheduled_utc") or ""):
                raise ImmutableEventClockV1ReadError("v1_scheduled_utc_mismatch")
            upstream_known_text = str(stored.get("upstream_first_known_utc") or "")
            upstream_known = (
                _parse_utc(upstream_known_text, field="upstream_first_known_utc")
                if upstream_known_text
                else captured
            )
            effective = max(captured, upstream_known)
            if str(row["effective_known_utc"] or "") != _iso(effective):
                raise ImmutableEventClockV1ReadError(
                    "v1_event_effective_known_mismatch"
                )
            if effective > cutoff:
                continue
            event = _historical_event_safe_view(stored)
            event["ledger_effective_known_utc"] = _iso(effective)
            event["event_availability_utc"] = _iso(effective)
            event["historical_v1_only"] = True
            event["direction_policy"] = "abstain"
            event["execution_eligible"] = False
            event["can_place_orders"] = False
            event["can_promote"] = False
            event["can_authorize"] = False
            output_events.append(event)

        return {
            "schema_version": SCHEMA_VERSION,
            "contract_id": CONTRACT_ID,
            "decision_cutoff_utc": _iso(cutoff),
            "snapshot_id": str(snapshot["snapshot_id"]),
            "snapshot_captured_utc": str(snapshot["captured_utc"]),
            "source_generated_utc": str(snapshot["source_generated_utc"]),
            "source_pipeline_version": str(snapshot["source_pipeline_version"]),
            "events_sha256": str(snapshot["events_sha256"]),
            "manifest_sha256": str(snapshot["manifest_sha256"]),
            "clock_observation_id": (
                str(observation.get("observation_id") or "") if observation else None
            ),
            "events": output_events,
            "event_count": len(output_events),
            "historical_only": True,
            "policy": {
                "exact_v1_identity_required": True,
                "v2_ledgers_rejected": True,
                "no_writer_exposed": True,
                "calendar_is_not_direction": True,
            },
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "can_promote": False,
            "can_authorize": False,
            "supported_execution_decision": "no_trade",
        }
    except sqlite3.Error as exc:
        raise ImmutableEventClockV1ReadError("v1_reconstruction_failed") from exc
    finally:
        connection.close()


class ImmutableEventClockV1Reader:
    """Small object wrapper retaining a frozen historical ledger path."""

    def __init__(
        self,
        database_path: Path,
        *,
        retirement_fingerprint_path: Path = DEFAULT_RETIREMENT_FINGERPRINT,
    ) -> None:
        self.database_path = Path(database_path)
        self.retirement_fingerprint_path = Path(retirement_fingerprint_path)

    def inspect_identity(self) -> dict[str, Any]:
        return inspect_v1_identity(
            self.database_path,
            retirement_fingerprint_path=self.retirement_fingerprint_path,
        )

    def as_of(self, cutoff_utc: dt.datetime) -> dict[str, Any]:
        return reconstruct_v1_as_of(
            self.database_path,
            cutoff_utc,
            retirement_fingerprint_path=self.retirement_fingerprint_path,
        )


__all__ = [
    "CONTRACT_ID",
    "DEFAULT_RETIREMENT_FINGERPRINT",
    "EXPECTED_SOURCE_PIPELINE_VERSION",
    "HISTORICAL_EVENT_SAFE_OUTPUT_FIELDS",
    "HISTORICAL_EVENT_SAFE_SOURCE_FIELDS",
    "ImmutableEventClockV1ReadError",
    "ImmutableEventClockV1Reader",
    "SCHEMA_VERSION",
    "inspect_v1_identity",
    "reconstruct_v1_as_of",
]
