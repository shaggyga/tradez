"""Clean, immutable, disabled OfficialFact V5 research successor.

V5 deliberately skips the rejected V3/V4 runtime lineage.  It composes the
accepted V2 normalization contract only from three exact, content-addressed inputs: the
immutable Clock V2 archive, the frozen official-policy baseline, and the
negative intraday-rate contract.  Six mutable V2 inputs and the inherited
event-preflight fallback path are structurally absent, not merely unused.

The accepted V5 surface is narrower than V2: policy-document context and
scheduled clock events only.  Macro actuals, daily rates, internal
expectations, source-governance quarantine state, current source health, and
the mutable host-clock diagnostic are explicitly unavailable.  This is a
research proof candidate, not a collector, registry, allocator, or executor.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math
import os
import re
import sqlite3
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .immutable_event_clock import (
    CONTRACT_ID as IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
    EventClockLedgerError,
    SCHEMA_VERSION as IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
    reconstruct_as_of as reconstruct_event_clock_as_of,
)
from .official_fact_adapter import CANONICAL_CURRENCIES
from .official_fact_adapter_v2 import (
    ADAPTER_CONTRACT_ID as V2_ADAPTER_CONTRACT_ID,
    PARENT_ADAPTER_CONTRACT_ID as V1_ADAPTER_CONTRACT_ID,
    OFFICIAL_CLOCK_ATTESTATION_FIELDS,
    OFFICIAL_CLOCK_FIELDS,
    OFFICIAL_CURRENCY_EVIDENCE_FIELDS,
    OFFICIAL_EVENT_CLOCK_PROVENANCE_FIELDS,
    OFFICIAL_FACT_FIELDS_BY_TYPE,
    OFFICIAL_FACT_V2_TOP_LEVEL_FIELDS,
    OFFICIAL_GLOBAL_GAP_FIELDS,
    OFFICIAL_INTRADAY_RATE_CONTRACT_FIELDS,
    OFFICIAL_INTRADAY_RATE_FIELDS,
    OFFICIAL_SOURCE_HEALTH_FIELDS,
    OFFICIAL_UPCOMING_EVENT_FIELDS,
    OfficialFactAdapterV2,
    OfficialFactPathsV2,
)


ADAPTER_SCHEMA_VERSION = 5
ADAPTER_CONTRACT_ID = "official_fact_adapter_v5_20260817_r3"
PARENT_ADAPTER_CONTRACT_ID = V2_ADAPTER_CONTRACT_ID
FACT_BASIS_ELIGIBILITY_CONTRACT_ID = "official_fact_basis_eligibility_v5_20260817_r3"
CLOCK_DEPENDENCY_POLICY = (
    "strict_clock_v2_content_addressed_archive_"
    "six_mutable_v2_inputs_and_event_preflight_path_eliminated"
)
OFFICIAL_FACT_V5_TOP_LEVEL_FIELDS = OFFICIAL_FACT_V2_TOP_LEVEL_FIELDS
ALLOWED_V5_FACT_TYPES = frozenset({"official_policy_document_context"})

SOURCE_ROOT = Path(__file__).resolve().parents[3]
ARCHIVE_ROOT = (
    SOURCE_ROOT
    / "data"
    / "oanda_training_manager"
    / "source_archives"
    / "official_fact_adapter_v4"
)
ARCHIVED_CLOCK_V2_DATABASE = (
    ARCHIVE_ROOT / "immutable_event_clock_v2_a5276c696eb6ff3.sqlite"
)
FIXED_POLICY_BASELINE = (
    SOURCE_ROOT / "config" / "official_policy_statement_baselines_v2_20260816.json"
)
FIXED_INTRADAY_RATE_CONTRACT = SOURCE_ROOT / "config" / "rates_policy_repricing_v1.json"

ARCHIVED_CLOCK_V2_DATABASE_BYTES = 909_312
ARCHIVED_CLOCK_V2_DATABASE_SHA256 = (
    "a5276c696eb6ff3c46adfcff41201288c2c966a76a52240897f5f0f25d30a740"
)
ARCHIVED_CLOCK_V2_ROW_COUNTS = {
    "event_clock_capture_attestations": 1,
    "event_clock_ledger_identity": 1,
    "event_clock_observations": 1,
    "event_clock_snapshots": 1,
    "event_clock_versions": 251,
    "snapshot_events": 251,
}
FIXED_POLICY_BASELINE_BYTES = 47_794
FIXED_POLICY_BASELINE_SHA256 = (
    "08bbf63aec7b0655f14140fd5995c26d80ee32595d9c9c7ce2ac9a1688b91dcd"
)
FIXED_INTRADAY_RATE_CONTRACT_BYTES = 764
FIXED_INTRADAY_RATE_CONTRACT_SHA256 = (
    "8d5645dbe2fa16fc879810a9a7f4ceada28ff414bea3f50b9ab233df09921c08"
)

FIXED_SQLITE_TIMEOUT_SEC = 5.0
FIXED_MAXIMUM_ROWS_PER_INPUT = 5_000
FIXED_TIMELY_RELEASE_LATENCY_SEC = 300.0
FIXED_MAXIMUM_CLOCK_OBSERVATION_AGE_SEC = 300.0
ELIMINATED_MUTABLE_INPUTS = (
    "source_governance_v1.sqlite",
    "macro_surprise_v1.sqlite",
    "official_daily_rate_context_v1.sqlite",
    "internal_macro_expectation_v1.sqlite",
    "source_coverage_latest.json",
    "clock_integrity_v1.json",
    "event_technical_preflight_v1.json",
)
ELIMINATED_PATH_FIELDS = frozenset(
    {
        "source_governance_db",
        "macro_surprise_db",
        "daily_rates_db",
        "internal_expectations_db",
        "event_preflight_json",
        "source_coverage_json",
        "clock_integrity_json",
    }
)

_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_CLOCK_OBSERVATION_ID = re.compile(r"event_clock_observation_[0-9a-f]{32}\Z")
_CLOCK_SNAPSHOT_ID = re.compile(r"event_clock_snapshot_[0-9a-f]{32}\Z")


class OfficialFactAdapterV5Error(RuntimeError):
    """Raised when a fixed input or accepted V5 claim is invalid."""


@dataclass(frozen=True)
class PinnedArtifactSpec:
    path: Path
    root: Path
    byte_length: int
    sha256: str
    require_read_only: bool = True
    sqlite_row_counts: Mapping[str, int] | None = None


def _norm_path(path: Path) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        int(value.st_dev),
        int(value.st_ino),
        int(value.st_mode),
        int(value.st_nlink),
        int(value.st_size),
        int(value.st_mtime_ns),
        int(getattr(value, "st_file_attributes", 0)),
    )


def _is_reparse(value: os.stat_result) -> bool:
    return bool(
        int(getattr(value, "st_file_attributes", 0))
        & int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    )


def _is_read_only(value: os.stat_result) -> bool:
    if os.name == "nt":
        return bool(
            int(getattr(value, "st_file_attributes", 0))
            & int(getattr(stat, "FILE_ATTRIBUTE_READONLY", 0x1))
        )
    return value.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH) == 0


def _assert_no_linked_ancestor(path: Path, root: Path) -> None:
    resolved_root = root.resolve(strict=True)
    try:
        relative = path.relative_to(resolved_root)
    except ValueError as exc:
        raise OfficialFactAdapterV5Error("pinned_artifact_outside_fixed_root") from exc
    cursor = resolved_root
    for part in relative.parts:
        cursor = cursor / part
        metadata = os.lstat(cursor)
        if stat.S_ISLNK(metadata.st_mode) or _is_reparse(metadata):
            raise OfficialFactAdapterV5Error("pinned_artifact_reparse_or_symlink")


def read_pinned_artifact(spec: PinnedArtifactSpec) -> tuple[bytes, dict[str, Any]]:
    if not spec.path.is_absolute() or not spec.root.is_absolute():
        raise OfficialFactAdapterV5Error("pinned_artifact_path_not_absolute")
    lexical = _norm_path(spec.path)
    try:
        resolved = spec.path.resolve(strict=True)
        resolved_root = spec.root.resolve(strict=True)
    except OSError as exc:
        raise OfficialFactAdapterV5Error("pinned_artifact_missing") from exc
    if lexical != _norm_path(resolved):
        raise OfficialFactAdapterV5Error("pinned_artifact_lexical_resolved_mismatch")
    try:
        if os.path.commonpath((lexical, _norm_path(resolved_root))) != _norm_path(
            resolved_root
        ):
            raise OfficialFactAdapterV5Error("pinned_artifact_outside_fixed_root")
    except ValueError as exc:
        raise OfficialFactAdapterV5Error("pinned_artifact_outside_fixed_root") from exc
    _assert_no_linked_ancestor(resolved, resolved_root)
    before = os.lstat(spec.path)
    if not stat.S_ISREG(before.st_mode):
        raise OfficialFactAdapterV5Error("pinned_artifact_not_regular_file")
    if before.st_nlink != 1:
        raise OfficialFactAdapterV5Error("pinned_artifact_hardlink_count_invalid")
    if spec.require_read_only and not _is_read_only(before):
        raise OfficialFactAdapterV5Error("pinned_artifact_not_read_only")
    if before.st_size != spec.byte_length:
        raise OfficialFactAdapterV5Error("pinned_artifact_byte_length_mismatch")
    try:
        with spec.path.open("rb", buffering=0) as handle:
            opened_before = os.fstat(handle.fileno())
            if _stat_identity(opened_before) != _stat_identity(before):
                raise OfficialFactAdapterV5Error("pinned_artifact_open_race")
            payload = handle.read()
            opened_after = os.fstat(handle.fileno())
            path_after = os.lstat(spec.path)
    except OSError as exc:
        raise OfficialFactAdapterV5Error("pinned_artifact_read_failed") from exc
    if (
        _stat_identity(opened_before) != _stat_identity(opened_after)
        or _stat_identity(opened_after) != _stat_identity(path_after)
        or _norm_path(spec.path.resolve(strict=True)) != lexical
    ):
        raise OfficialFactAdapterV5Error("pinned_artifact_identity_changed_during_read")
    digest = hashlib.sha256(payload).hexdigest()
    if len(payload) != spec.byte_length or digest != spec.sha256:
        raise OfficialFactAdapterV5Error("pinned_artifact_content_mismatch")
    result: dict[str, Any] = {
        "resolved_path": str(resolved),
        "byte_length": len(payload),
        "sha256": digest,
        "read_only": _is_read_only(path_after),
        "link_count": int(path_after.st_nlink),
    }
    if spec.sqlite_row_counts is not None:
        connection = sqlite3.connect(":memory:")
        try:
            connection.deserialize(payload)
            quick = str(connection.execute("PRAGMA quick_check").fetchone()[0])
            counts = {
                table: int(
                    connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                )
                for table in spec.sqlite_row_counts
            }
        except sqlite3.Error as exc:
            raise OfficialFactAdapterV5Error("pinned_sqlite_invalid") from exc
        finally:
            connection.close()
        if quick != "ok" or counts != dict(spec.sqlite_row_counts):
            raise OfficialFactAdapterV5Error("pinned_sqlite_integrity_mismatch")
        result.update({"quick_check": quick, "row_counts": counts})
    return payload, result


@contextmanager
def hold_pinned_artifact(spec: PinnedArtifactSpec):
    """Hold a Windows read-only path identity across SQLite reconstruction."""

    if os.name != "nt":  # pragma: no cover
        payload, metadata = read_pinned_artifact(spec)
        with spec.path.open("rb", buffering=0) as handle:
            opened = _stat_identity(os.fstat(handle.fileno()))
            if opened != _stat_identity(os.lstat(spec.path)):
                raise OfficialFactAdapterV5Error("pinned_artifact_guard_open_race")
            yield payload, metadata
            if opened != _stat_identity(os.lstat(spec.path)):
                raise OfficialFactAdapterV5Error("pinned_artifact_guard_close_race")
        return
    import ctypes
    import msvcrt
    from ctypes import wintypes

    create_file = ctypes.WinDLL("kernel32", use_last_error=True).CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    handle_value = create_file(
        str(spec.path), 0x80000000, 0x00000001, None, 3, 0x00200000, None
    )
    if handle_value == ctypes.c_void_p(-1).value:
        raise OfficialFactAdapterV5Error(
            f"pinned_artifact_guard_open_failed:{ctypes.get_last_error()}"
        )
    guarded = None
    fd = -1
    try:
        fd = msvcrt.open_osfhandle(
            int(handle_value), os.O_RDONLY | getattr(os, "O_BINARY", 0)
        )
        guarded = os.fdopen(fd, "rb", buffering=0)
        fd = -1
        locked = _stat_identity(os.fstat(guarded.fileno()))
        if locked != _stat_identity(os.lstat(spec.path)):
            raise OfficialFactAdapterV5Error("pinned_artifact_guard_open_race")
        payload, metadata = read_pinned_artifact(spec)
        yield payload, metadata
        if (
            locked != _stat_identity(os.fstat(guarded.fileno()))
            or locked != _stat_identity(os.lstat(spec.path))
        ):
            raise OfficialFactAdapterV5Error("pinned_artifact_guard_close_race")
    finally:
        if guarded is not None:
            guarded.close()
        elif fd >= 0:
            os.close(fd)
        else:
            ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(handle_value)


_CLOCK_SPEC = PinnedArtifactSpec(
    ARCHIVED_CLOCK_V2_DATABASE,
    ARCHIVE_ROOT,
    ARCHIVED_CLOCK_V2_DATABASE_BYTES,
    ARCHIVED_CLOCK_V2_DATABASE_SHA256,
    sqlite_row_counts=ARCHIVED_CLOCK_V2_ROW_COUNTS,
)
_POLICY_SPEC = PinnedArtifactSpec(
    FIXED_POLICY_BASELINE,
    SOURCE_ROOT / "config",
    FIXED_POLICY_BASELINE_BYTES,
    FIXED_POLICY_BASELINE_SHA256,
    require_read_only=False,
)
_RATE_SPEC = PinnedArtifactSpec(
    FIXED_INTRADAY_RATE_CONTRACT,
    SOURCE_ROOT / "config",
    FIXED_INTRADAY_RATE_CONTRACT_BYTES,
    FIXED_INTRADAY_RATE_CONTRACT_SHA256,
    require_read_only=False,
)


def _strict_json(payload: bytes, *, label: str) -> dict[str, Any]:
    def reject_constant(value: str) -> None:
        raise OfficialFactAdapterV5Error(f"{label}_nonfinite_json:{value}")

    def reject_duplicate(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise OfficialFactAdapterV5Error(f"{label}_duplicate_json_key:{key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            payload.decode("utf-8"),
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicate,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OfficialFactAdapterV5Error(f"{label}_invalid_json") from exc
    if type(value) is not dict:
        raise OfficialFactAdapterV5Error(f"{label}_not_mapping")
    return value


def verify_official_fact_v5_inputs() -> dict[str, Any]:
    paths = _fixed_paths()
    path_values = vars(paths)
    if set(path_values).intersection(ELIMINATED_PATH_FIELDS) != ELIMINATED_PATH_FIELDS:
        raise OfficialFactAdapterV5Error("v5_eliminated_path_fields_missing")
    if any(path_values[field] is not None for field in ELIMINATED_PATH_FIELDS):
        raise OfficialFactAdapterV5Error("v5_eliminated_path_field_not_null")
    _, clock = read_pinned_artifact(_CLOCK_SPEC)
    policy_bytes, policy = read_pinned_artifact(_POLICY_SPEC)
    rate_bytes, rate = read_pinned_artifact(_RATE_SPEC)
    _strict_json(policy_bytes, label="fixed_policy_baseline")
    _strict_json(rate_bytes, label="fixed_intraday_rate_contract")
    return {
        "clock_v2": clock,
        "policy_baseline": policy,
        "intraday_rate_contract": rate,
        "eliminated_mutable_inputs": list(ELIMINATED_MUTABLE_INPUTS),
        "structurally_eliminated_path_fields": sorted(ELIMINATED_PATH_FIELDS),
        "canonical_mutable_files_opened": False,
    }


def _parse_utc(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(dt.timezone.utc)


def _iso(value: dt.datetime | None) -> str | None:
    return value.astimezone(dt.timezone.utc).isoformat() if value is not None else None


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
        raise OfficialFactAdapterV5Error("snapshot_material_not_canonical_json") from exc
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _fail(path: str, expected: str) -> None:
    raise OfficialFactAdapterV5Error(f"typed_schema:{path}:{expected}")


def _exact_str(value: Any, *, path: str, nonempty: bool = False) -> str:
    if type(value) is not str or (nonempty and not value):
        _fail(path, "exact_string" + ("_nonempty" if nonempty else ""))
    return value


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


def _hash64(value: Any, *, path: str, allow_empty: bool = False) -> str:
    text = _exact_str(value, path=path)
    if not (allow_empty and not text) and _HEX64.fullmatch(text) is None:
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
    if any(type(key) is not str for key in value):
        _fail(path, "string_keys")
    unknown = set(value) - fields
    missing = (required or frozenset()) - set(value)
    if unknown:
        _fail(path, "unknown_fields:" + ",".join(sorted(unknown)))
    if missing:
        _fail(path, "missing_fields:" + ",".join(sorted(missing)))
    return value


class _PinnedV2Parent(OfficialFactAdapterV2):
    """Accepted V2 normalization with every mutable path structurally absent."""

    def _connect(self, path):
        """Forbid inherited SQLite readers even if a path is injected later."""

        raise OfficialFactAdapterV5Error("v5_inherited_sqlite_connect_forbidden")

    def _eliminated(self, gaps: list[dict[str, Any]], name: str) -> None:
        gaps.append({"code": f"v5_mutable_input_eliminated:{name}"})

    def _quarantined_source_events(self, gaps):
        self._eliminated(gaps, "source_governance")
        return set()

    def _macro_facts(self, cutoff, currencies, quarantined, gaps):
        self._eliminated(gaps, "macro_surprise")
        return []

    def _rate_facts(self, cutoff, currencies, gaps):
        self._eliminated(gaps, "daily_rates")
        return []

    def _expectation_facts(self, cutoff, currencies, gaps):
        self._eliminated(gaps, "internal_expectations")
        return []

    def _source_health(self, cutoff, currencies, gaps):
        self._eliminated(gaps, "source_coverage")
        return {
            currency: {
                "state": "eliminated_from_v5_fixed_scope",
                "as_of_utc": None,
                "configured_source_count": 0,
                "operational_source_count": 0,
                "healthy_direct_source_count": 0,
                "degraded_recent_direct_source_count": 0,
                "official_source_issues": ["mutable_source_coverage_excluded"],
            }
            for currency in currencies
        }

    def _clock_state(self, cutoff, gaps):
        self._eliminated(gaps, "host_clock_integrity")
        return {
            "state": "eliminated_from_v5_fixed_scope",
            "trusted_for_prospective_evidence": False,
        }

    def _upcoming_events(self, cutoff, currencies, gaps):
        """Read only causally complete rows from the fixed Clock V2 archive."""

        self._eliminated(gaps, "event_preflight")

        try:
            snapshot = reconstruct_event_clock_as_of(
                ARCHIVED_CLOCK_V2_DATABASE, cutoff
            )
        except (OSError, sqlite3.Error, EventClockLedgerError, ValueError) as exc:
            raise OfficialFactAdapterV5Error(
                "immutable_event_clock_v2_identity_or_integrity_failure"
            ) from exc
        if (
            snapshot.get("schema_version") != IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION
            or snapshot.get("contract_id") != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID
        ):
            raise OfficialFactAdapterV5Error("immutable_event_clock_v2_identity_mismatch")
        if (
            snapshot.get("research_only") is not True
            or snapshot.get("execution_eligible") is not False
            or snapshot.get("supported_execution_decision") != "no_trade"
        ):
            raise OfficialFactAdapterV5Error("immutable_event_clock_v2_safety_mismatch")
        snapshot_id = str(snapshot.get("snapshot_id") or "")
        if not snapshot_id:
            raise OfficialFactAdapterV5Error("immutable_event_clock_v2_snapshot_missing")
        freshness = self._clock_freshness(snapshot, cutoff=cutoff)
        attestation = dict(snapshot.get("clock_attestation") or {})
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
            "clock_attestation": attestation,
            "clock_ready_for_cutoff": bool(freshness["ready"]),
            "observation_age_sec": freshness["observation_age_sec"],
            "maximum_observation_age_sec": self.maximum_clock_observation_age_sec,
            "fallback_used": False,
            "mutable_fallback_forbidden": True,
            "complete_snapshot_at_cutoff": bool(freshness["ready"]),
        }
        if freshness["ready"] is not True:
            provenance["degradation_reason"] = freshness["reason"]
            gaps.append(
                {
                    "code": str(freshness["reason"]),
                    "observation_age_sec": freshness["observation_age_sec"],
                    "maximum_observation_age_sec": self.maximum_clock_observation_age_sec,
                }
            )
            return [], provenance
        output: list[dict[str, Any]] = []
        incomplete = 0
        for row in snapshot.get("events") or []:
            if type(row) is not dict:
                incomplete += 1
                continue
            availability = _parse_utc(row.get("ledger_effective_known_utc"))
            scheduled = _parse_utc(row.get("scheduled_utc"))
            if availability is None or availability > cutoff or scheduled is None:
                incomplete += 1
                continue
            if scheduled < cutoff:
                continue
            direct = {
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
                    "schedule_window_end_utc": str(row.get("schedule_window_end_utc") or "") or None,
                    "timing_precision": str(row.get("timing_precision") or ""),
                    "policy_event": self._is_policy_clock(row),
                    "policy_dependency": False,
                    "direct_event_currency": currency in direct if direct else None,
                    "driver_currency": currency,
                    "headline": str(row.get("headline") or ""),
                }
                output.append(
                    {
                        **material,
                        "fact_type": "official_event_clock",
                        "known_from_snapshot_utc": str(snapshot.get("snapshot_captured_utc") or ""),
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
                        "raw_payload_sha256": _canonical_hash(material),
                    }
                )
                if len(output) >= self.maximum_rows_per_input:
                    gaps.append({"code": "immutable_event_clock_v2_limit_reached"})
                    break
            if len(output) >= self.maximum_rows_per_input:
                break
        if incomplete:
            gaps.append({"code": "immutable_event_clock_v2_incomplete_rows_excluded"})
        return output, provenance

    def _policy_facts(self, cutoff, currencies, gaps):
        payload_bytes, _ = read_pinned_artifact(_POLICY_SPEC)
        payload = _strict_json(payload_bytes, label="fixed_policy_baseline")
        created = _parse_utc(payload.get("created_utc"))
        output: list[dict[str, Any]] = []
        for index, raw in enumerate(payload.get("baselines") or []):
            if type(raw) is not dict:
                raise OfficialFactAdapterV5Error("fixed_policy_baseline_row_not_mapping")
            currency = str(raw.get("currency") or "").upper()
            if currency not in currencies:
                continue
            known = _parse_utc(raw.get("known_utc"))
            detail = _parse_utc(raw.get("detail_available_utc"))
            published = _parse_utc(raw.get("published_utc"))
            candidates = [item for item in (known, detail, published, created) if item]
            if not candidates:
                raise OfficialFactAdapterV5Error("fixed_policy_baseline_time_missing")
            effective = max(candidates)
            if effective > cutoff:
                continue
            degradation = [
                "prior_policy_context_only",
                "semantic_direction_not_assigned",
            ]
            if raw.get("source_listing_bootstrap") is True:
                degradation.append("source_listing_bootstrap")
            output.append(
                {
                    "fact_id": f"{raw.get('event_id') or index}:{currency}",
                    "currency": currency,
                    "fact_type": "official_policy_document_context",
                    "series_id": str(raw.get("document_class") or ""),
                    "event_name": str(raw.get("headline") or ""),
                    "text_excerpt": str(raw.get("summary") or "")[:512],
                    "payload_ref": f"config/{FIXED_POLICY_BASELINE.name}#baseline={index}",
                    "consensus_value": None,
                    "consensus_causal": False,
                    "published_at_utc": _iso(published),
                    "first_seen_at_utc": _iso(known),
                    "retrieved_at_utc": _iso(detail or known),
                    "effective_from_utc": _iso(effective),
                    "source_id": str(raw.get("source_id") or ""),
                    "source_name": "official_policy_publisher",
                    "source_url": str(raw.get("source_url") or ""),
                    "source_contract_id": str(payload.get("contract_id") or ""),
                    "source_cohort_id": str(payload.get("contract_id") or ""),
                    "raw_payload_sha256": str(raw.get("raw_payload_sha256") or ""),
                    "evidence_class": "policy_context_only",
                    "degradation_reasons": degradation,
                    "direction_policy": "abstain",
                }
            )
        return output

    def _intraday_rate_state(self, cutoff, gaps):
        payload_bytes, _ = read_pinned_artifact(_RATE_SPEC)
        payload = _strict_json(payload_bytes, label="fixed_intraday_rate_contract")
        state = str(payload.get("state") or "unavailable")
        known = _parse_utc(payload.get("generated_utc") or payload.get("created_utc"))
        claims_connected = state in {"connected", "ready", "ok"} and bool(
            payload.get("currencies")
        )
        connected = bool(claims_connected and known is not None and known <= cutoff)
        if claims_connected and not connected:
            gaps.append({"code": "intraday_rate_config_not_known_at_cutoff"})
        contract = payload.get("prospective_contract")
        if type(contract) is not dict:
            contract = {
                "required_fields": [],
                "causal_rule": "unavailable",
                "no_data_policy": "abstain",
                "material_change_policy": "new_source_cohort_required",
            }
        return {
            "state": state,
            "connected": connected,
            "as_of_utc": _iso(known) if known and known <= cutoff else None,
            "contract": contract,
            "blocker": "" if connected else str(payload.get("blocker") or "not_connected"),
        }


def _fixed_paths() -> OfficialFactPathsV2:
    return OfficialFactPathsV2(
        source_governance_db=None,
        macro_surprise_db=None,
        daily_rates_db=None,
        internal_expectations_db=None,
        policy_baselines_json=FIXED_POLICY_BASELINE,
        event_preflight_json=None,
        immutable_event_clock_db=ARCHIVED_CLOCK_V2_DATABASE,
        source_coverage_json=None,
        clock_integrity_json=None,
        intraday_rates_config_json=FIXED_INTRADAY_RATE_CONTRACT,
    )


def _new_v2_parent() -> _PinnedV2Parent:
    return _PinnedV2Parent(
        paths=_fixed_paths(),
        sqlite_timeout_sec=FIXED_SQLITE_TIMEOUT_SEC,
        maximum_rows_per_input=FIXED_MAXIMUM_ROWS_PER_INPUT,
        timely_release_latency_sec=FIXED_TIMELY_RELEASE_LATENCY_SEC,
        maximum_clock_observation_age_sec=FIXED_MAXIMUM_CLOCK_OBSERVATION_AGE_SEC,
    )


def _brand(snapshot: dict[str, Any], *, version: int) -> None:
    if version == 2:
        snapshot["schema_version"] = 2
        snapshot["adapter_contract_id"] = V2_ADAPTER_CONTRACT_ID
        snapshot["parent_adapter_contract_id"] = V1_ADAPTER_CONTRACT_ID
        snapshot["clock_dependency_policy"] = "strict_v2_only_no_mutable_fallback"
        prefix = "official_fact_v2_snapshot_"
    elif version == 5:
        snapshot["schema_version"] = ADAPTER_SCHEMA_VERSION
        snapshot["adapter_contract_id"] = ADAPTER_CONTRACT_ID
        snapshot["parent_adapter_contract_id"] = PARENT_ADAPTER_CONTRACT_ID
        snapshot["clock_dependency_policy"] = CLOCK_DEPENDENCY_POLICY
        prefix = "official_fact_v5_snapshot_"
    else:
        raise OfficialFactAdapterV5Error("unsupported_brand_version")
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    snapshot["snapshot_id"] = prefix + _canonical_hash(material)[:24]


def _build_expected(cutoff: str | dt.datetime) -> dict[str, Any]:
    verify_official_fact_v5_inputs()
    with hold_pinned_artifact(_CLOCK_SPEC):
        parent = _new_v2_parent().as_of(cutoff, currencies=CANONICAL_CURRENCIES)
    verify_official_fact_v5_inputs()
    result = copy.deepcopy(parent)
    result["can_promote"] = False
    result["can_authorize"] = False
    _brand(result, version=5)
    return result


def _validate_policy_fact(row: Any, *, path: str, cutoff: dt.datetime) -> Mapping[str, Any]:
    fields = OFFICIAL_FACT_FIELDS_BY_TYPE["official_policy_document_context"]
    value = _mapping(row, fields, path=path, required=fields)
    string_fields = (
        "fact_id", "currency", "fact_type", "series_id", "event_name",
        "text_excerpt", "payload_ref", "source_id", "source_name", "source_url",
        "source_contract_id", "source_cohort_id", "evidence_class", "direction_policy",
    )
    for field in string_fields:
        _exact_str(value[field], path=f"{path}.{field}", nonempty=field in {"fact_id", "currency", "fact_type"})
    if value["currency"] not in CANONICAL_CURRENCIES:
        _fail(f"{path}.currency", "canonical_currency")
    if value["fact_type"] != "official_policy_document_context":
        _fail(f"{path}.fact_type", "v5_policy_only")
    if value["consensus_value"] is not None or value["consensus_causal"] is not False:
        _fail(path, "no_consensus_claim")
    _exact_bool(value["consensus_causal"], path=f"{path}.consensus_causal")
    published = _timestamp(value["published_at_utc"], path=f"{path}.published_at_utc", nullable=True)
    first = _timestamp(value["first_seen_at_utc"], path=f"{path}.first_seen_at_utc", nullable=True)
    retrieved = _timestamp(value["retrieved_at_utc"], path=f"{path}.retrieved_at_utc", nullable=True)
    effective = _timestamp(value["effective_from_utc"], path=f"{path}.effective_from_utc")
    assert effective is not None
    if effective > cutoff or any(item is not None and item > effective for item in (published, first, retrieved)):
        _fail(path, "causal_fact_chronology")
    if published is not None and any(item is not None and item < published for item in (first, retrieved)):
        _fail(path, "observation_before_publication")
    if first is not None and retrieved is not None and first > retrieved:
        _fail(path, "first_seen_after_retrieval")
    _hash64(value["raw_payload_sha256"], path=f"{path}.raw_payload_sha256")
    _string_list(value["degradation_reasons"], path=f"{path}.degradation_reasons")
    if value["direction_policy"] != "abstain":
        _fail(f"{path}.direction_policy", "literal_abstain")
    return value


def _validate_event(row: Any, *, path: str, cutoff: dt.datetime) -> Mapping[str, Any]:
    value = _mapping(
        row,
        OFFICIAL_UPCOMING_EVENT_FIELDS,
        path=path,
        required=OFFICIAL_UPCOMING_EVENT_FIELDS,
    )
    for field in (
        "event_id", "event_version_id", "currency", "category", "timing_precision",
        "driver_currency", "headline", "fact_type", "clock_provenance_state",
        "clock_snapshot_id", "clock_source_contract_id", "evidence_class", "direction_policy",
    ):
        _exact_str(value[field], path=f"{path}.{field}", nonempty=field in {"event_id", "event_version_id", "currency", "driver_currency", "fact_type"})
    for field in ("policy_event", "policy_dependency", "consensus_causal", "execution_eligible", "can_place_orders"):
        _exact_bool(value[field], path=f"{path}.{field}")
    if value["direct_event_currency"] is not None:
        _exact_bool(value["direct_event_currency"], path=f"{path}.direct_event_currency")
    scheduled = _timestamp(value["scheduled_utc"], path=f"{path}.scheduled_utc")
    window_end = _timestamp(value["schedule_window_end_utc"], path=f"{path}.schedule_window_end_utc", nullable=True)
    known = _timestamp(value["known_from_snapshot_utc"], path=f"{path}.known_from_snapshot_utc")
    ledger = _timestamp(value["ledger_effective_known_utc"], path=f"{path}.ledger_effective_known_utc")
    assert scheduled and known and ledger
    if scheduled < cutoff or ledger > known or known > cutoff or (window_end and window_end < scheduled):
        _fail(path, "causal_event_chronology")
    if value["fact_type"] != "official_event_clock" or value["direction_policy"] != "abstain":
        _fail(path, "event_identity_or_direction")
    if value["consensus_causal"] is not False or value["execution_eligible"] is not False or value["can_place_orders"] is not False:
        _fail(path, "event_execution_or_consensus_claim")
    _string_list(value["degradation_reasons"], path=f"{path}.degradation_reasons")
    _hash64(value["raw_payload_sha256"], path=f"{path}.raw_payload_sha256")
    return value


def _validate_source_health(value: Any, *, path: str) -> None:
    row = _mapping(value, OFFICIAL_SOURCE_HEALTH_FIELDS, path=path, required=OFFICIAL_SOURCE_HEALTH_FIELDS)
    _exact_str(row["state"], path=f"{path}.state", nonempty=True)
    _timestamp(row["as_of_utc"], path=f"{path}.as_of_utc", nullable=True)
    counts = []
    for field in ("configured_source_count", "operational_source_count", "healthy_direct_source_count", "degraded_recent_direct_source_count"):
        counts.append(_exact_int(row[field], path=f"{path}.{field}"))
    if counts != [0, 0, 0, 0]:
        _fail(path, "v5_eliminated_source_counts_zero")
    issues = _string_list(row["official_source_issues"], path=f"{path}.official_source_issues")
    if issues != ["mutable_source_coverage_excluded"]:
        _fail(path, "v5_eliminated_source_issue")


def _validate_gaps(value: Any, *, path: str) -> None:
    if type(value) is not list:
        _fail(path, "exact_list")
    codes: list[str] = []
    for index, raw in enumerate(value):
        item_path = f"{path}[{index}]"
        row = _mapping(raw, OFFICIAL_GLOBAL_GAP_FIELDS, path=item_path, required=frozenset({"code"}))
        codes.append(_exact_str(row["code"], path=f"{item_path}.code", nonempty=True))
        for field in ("artifact_name", "error_class"):
            if field in row:
                _exact_str(row[field], path=f"{item_path}.{field}")
        for field in ("observation_age_sec", "maximum_observation_age_sec"):
            if field in row:
                _finite_number(row[field], path=f"{item_path}.{field}", nullable=True, minimum=0.0)
    if len(codes) != len(set(codes)):
        _fail(path, "unique_gap_codes")


def _validate_attestation(value: Any, *, path: str, required: bool) -> None:
    row = _mapping(
        value,
        OFFICIAL_CLOCK_ATTESTATION_FIELDS,
        path=path,
        required=OFFICIAL_CLOCK_ATTESTATION_FIELDS if required else frozenset(),
    )
    for field in ("state", "artifact_name", "status"):
        if field in row:
            _exact_str(row[field], path=f"{path}.{field}", nonempty=True)
    if "generated_utc" in row:
        _timestamp(row["generated_utc"], path=f"{path}.generated_utc")
    if "payload_sha256" in row:
        _hash64(row["payload_sha256"], path=f"{path}.payload_sha256")
    for field in ("age_at_capture_sec", "maximum_age_sec"):
        if field in row:
            _finite_number(row[field], path=f"{path}.{field}", minimum=0.0)
    for field in ("attested", "timestamp_normalization_trusted", "host_clock_synchronized", "source_fresh"):
        if field in row:
            _exact_bool(row[field], path=f"{path}.{field}")
    if required and not (
        row["state"] == "fresh_trusted"
        and row["status"] == "ok"
        and row["attested"] is True
        and row["timestamp_normalization_trusted"] is True
        and row["host_clock_synchronized"] is True
        and row["source_fresh"] is True
    ):
        _fail(path, "complete_fresh_trusted_attestation")


def _validate_provenance(value: Any, *, path: str, cutoff: dt.datetime) -> Mapping[str, Any]:
    row = _mapping(
        value,
        OFFICIAL_EVENT_CLOCK_PROVENANCE_FIELDS,
        path=path,
        required=frozenset({"state", "schema_version", "contract_id", "clock_ready_for_cutoff", "fallback_used", "mutable_fallback_forbidden", "complete_snapshot_at_cutoff"}),
    )
    for field in ("state", "schema_version", "contract_id"):
        _exact_str(row[field], path=f"{path}.{field}", nonempty=True)
    for field in ("clock_ready_for_cutoff", "fallback_used", "mutable_fallback_forbidden", "complete_snapshot_at_cutoff"):
        _exact_bool(row[field], path=f"{path}.{field}")
    ready = row["clock_ready_for_cutoff"]
    if ready is not row["complete_snapshot_at_cutoff"] or row["fallback_used"] is not False or row["mutable_fallback_forbidden"] is not True:
        _fail(path, "clock_readiness_or_fallback_claim")
    if row["schema_version"] != IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION or row["contract_id"] != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID:
        _fail(path, "clock_v2_identity")
    snapshot_captured = _timestamp(row.get("snapshot_captured_utc"), path=f"{path}.snapshot_captured_utc")
    observed = _timestamp(row.get("clock_observed_utc"), path=f"{path}.clock_observed_utc")
    generated = _timestamp(row.get("source_generated_utc"), path=f"{path}.source_generated_utc")
    assert snapshot_captured and observed and generated
    if generated > snapshot_captured or snapshot_captured > observed or observed > cutoff:
        _fail(path, "clock_source_snapshot_observation_cutoff_order")
    snapshot_id = _exact_str(row.get("snapshot_id"), path=f"{path}.snapshot_id", nonempty=True)
    observation_id = _exact_str(row.get("clock_observation_id"), path=f"{path}.clock_observation_id", nonempty=True)
    if _CLOCK_SNAPSHOT_ID.fullmatch(snapshot_id) is None or _CLOCK_OBSERVATION_ID.fullmatch(observation_id) is None:
        _fail(path, "clock_identity_format")
    for field in ("events_sha256", "manifest_sha256"):
        _hash64(row.get(field), path=f"{path}.{field}")
    recorded_age = _finite_number(row.get("observation_age_sec"), path=f"{path}.observation_age_sec", minimum=0.0)
    maximum_age = _finite_number(row.get("maximum_observation_age_sec"), path=f"{path}.maximum_observation_age_sec", minimum=0.0)
    recomputed_age = (cutoff - observed).total_seconds()
    if not math.isclose(float(recorded_age), recomputed_age, abs_tol=1e-6, rel_tol=0.0):
        _fail(path, "observation_age_reconciliation")
    _validate_attestation(row.get("clock_attestation"), path=f"{path}.clock_attestation", required=ready)
    if ready:
        attestation = row["clock_attestation"]
        attested_at = _timestamp(attestation["generated_utc"], path=f"{path}.clock_attestation.generated_utc")
        assert attested_at
        if generated > attested_at or attested_at > snapshot_captured:
            _fail(path, "clock_attestation_order")
        recomputed_attestation_age = (snapshot_captured - attested_at).total_seconds()
        if not math.isclose(float(attestation["age_at_capture_sec"]), recomputed_attestation_age, abs_tol=1e-6, rel_tol=0.0):
            _fail(path, "clock_attestation_age_reconciliation")
        if recomputed_age > float(maximum_age) or row["state"] != "immutable_v2_fresh_attested_at_cutoff":
            _fail(path, "ready_clock_claim")
    elif row["complete_snapshot_at_cutoff"] is not False:
        _fail(path, "not_ready_clock_claim")
    return row


def _validate_surface(snapshot: Mapping[str, Any]) -> None:
    row = _mapping(snapshot, OFFICIAL_FACT_V5_TOP_LEVEL_FIELDS, path="root", required=OFFICIAL_FACT_V5_TOP_LEVEL_FIELDS)
    if type(row["schema_version"]) is not int or row["schema_version"] != ADAPTER_SCHEMA_VERSION:
        _fail("root.schema_version", "literal_int_5")
    literals = {
        "adapter_contract_id": ADAPTER_CONTRACT_ID,
        "parent_adapter_contract_id": PARENT_ADAPTER_CONTRACT_ID,
        "fact_count_semantics": "normalized_provenance_records_not_independent_events",
        "supported_execution_decision": "no_trade",
        "immutable_event_clock_schema_version": IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
        "immutable_event_clock_contract_id": IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
        "clock_dependency_policy": CLOCK_DEPENDENCY_POLICY,
    }
    for field, expected in literals.items():
        _exact_str(row[field], path=f"root.{field}", nonempty=True)
        if row[field] != expected:
            _fail(f"root.{field}", f"literal_{expected}")
    cutoff = _timestamp(row["decision_cutoff_utc"], path="root.decision_cutoff_utc")
    assert cutoff
    for field in ("currency_count", "fact_count", "macro_fact_record_count", "distinct_macro_observation_count", "upcoming_event_count", "causal_consensus_count", "quarantined_source_event_count"):
        _exact_int(row[field], path=f"root.{field}")
    for field, expected in (("research_only", True), ("execution_eligible", False), ("can_place_orders", False), ("can_promote", False), ("can_authorize", False)):
        _exact_bool(row[field], path=f"root.{field}")
        if row[field] is not expected:
            _fail(f"root.{field}", f"literal_{expected}")
    _finite_number(row["maximum_clock_observation_age_sec"], path="root.maximum_clock_observation_age_sec", minimum=0.0)
    if row["maximum_clock_observation_age_sec"] != FIXED_MAXIMUM_CLOCK_OBSERVATION_AGE_SEC:
        _fail("root.maximum_clock_observation_age_sec", "fixed_300")
    if type(row["facts"]) is not list or type(row["upcoming_events"]) is not list:
        _fail("root.collections", "exact_lists")
    facts = [_validate_policy_fact(item, path=f"root.facts[{index}]", cutoff=cutoff) for index, item in enumerate(row["facts"])]
    events = [_validate_event(item, path=f"root.upcoming_events[{index}]", cutoff=cutoff) for index, item in enumerate(row["upcoming_events"])]
    if len({item["fact_id"] for item in facts}) != len(facts):
        _fail("root.facts", "unique_fact_ids")
    if len({(item["event_version_id"], item["currency"]) for item in events}) != len(events):
        _fail("root.upcoming_events", "unique_event_version_currency")
    if facts != sorted(facts, key=lambda item: (item["currency"], item["fact_type"], item["effective_from_utc"], item["fact_id"])):
        _fail("root.facts", "canonical_order")
    if events != sorted(events, key=lambda item: (item["scheduled_utc"], item["event_id"])):
        _fail("root.upcoming_events", "canonical_order")
    if any(item["fact_type"] not in ALLOWED_V5_FACT_TYPES for item in facts):
        _fail("root.facts", "v5_policy_only")
    evidence = row["currency_evidence"]
    if type(evidence) is not dict or tuple(evidence) != tuple(CANONICAL_CURRENCIES):
        _fail("root.currency_evidence", "exact_ordered_21_currency_map")
    totals = {"fact_count": 0, "upcoming_event_count": 0}
    for currency in CANONICAL_CURRENCIES:
        path = f"root.currency_evidence.{currency}"
        summary = _mapping(evidence[currency], OFFICIAL_CURRENCY_EVIDENCE_FIELDS, path=path, required=OFFICIAL_CURRENCY_EVIDENCE_FIELDS)
        if summary["currency"] != currency:
            _fail(f"{path}.currency", "map_identity")
        for field in ("fact_count", "macro_fact_record_count", "distinct_macro_observation_count", "causal_consensus_count", "upcoming_event_count"):
            _exact_int(summary[field], path=f"{path}.{field}")
        currency_facts = [item for item in facts if item["currency"] == currency]
        currency_events = [item for item in events if item["currency"] == currency]
        expected = {
            "fact_count": len(currency_facts),
            "macro_fact_record_count": 0,
            "distinct_macro_observation_count": 0,
            "causal_consensus_count": 0,
            "upcoming_event_count": len(currency_events),
        }
        if any(summary[key] != value for key, value in expected.items()):
            _fail(path, "reconciled_counts")
        fact_types = _string_list(summary["fact_types"], path=f"{path}.fact_types")
        if fact_types != sorted({item["fact_type"] for item in currency_facts}):
            _fail(f"{path}.fact_types", "reconciled_types")
        _string_list(summary["missing_or_degraded"], path=f"{path}.missing_or_degraded")
        _validate_source_health(summary["source_health"], path=f"{path}.source_health")
        totals["fact_count"] += summary["fact_count"]
        totals["upcoming_event_count"] += summary["upcoming_event_count"]
    if row["currency_count"] != 21 or row["fact_count"] != len(facts) or row["fact_count"] != totals["fact_count"] or row["upcoming_event_count"] != len(events) or row["upcoming_event_count"] != totals["upcoming_event_count"]:
        _fail("root.counts", "top_nested_reconciliation")
    for field in ("macro_fact_record_count", "distinct_macro_observation_count", "causal_consensus_count", "quarantined_source_event_count"):
        if row[field] != 0:
            _fail(f"root.{field}", "v5_eliminated_zero")
    _validate_gaps(row["global_gaps"], path="root.global_gaps")
    provenance = _validate_provenance(row["event_clock_provenance"], path="root.event_clock_provenance", cutoff=cutoff)
    for index, event in enumerate(events):
        if event["clock_snapshot_id"] != provenance["snapshot_id"] or event["clock_source_contract_id"] != provenance["contract_id"] or event["known_from_snapshot_utc"] != provenance["snapshot_captured_utc"]:
            _fail(f"root.upcoming_events[{index}]", "clock_provenance_identity")
    clock = _mapping(row["clock"], OFFICIAL_CLOCK_FIELDS, path="root.clock", required=frozenset({"state", "trusted_for_prospective_evidence"}))
    if clock != {"state": "eliminated_from_v5_fixed_scope", "trusted_for_prospective_evidence": False}:
        _fail("root.clock", "v5_eliminated_clock_state")
    intraday = _mapping(row["intraday_rates"], OFFICIAL_INTRADAY_RATE_FIELDS, path="root.intraday_rates", required=OFFICIAL_INTRADAY_RATE_FIELDS)
    _exact_str(intraday["state"], path="root.intraday_rates.state", nonempty=True)
    _exact_bool(intraday["connected"], path="root.intraday_rates.connected")
    if intraday["connected"] is not False:
        _fail("root.intraday_rates.connected", "literal_false")
    _timestamp(intraday["as_of_utc"], path="root.intraday_rates.as_of_utc", nullable=True)
    _exact_str(intraday["blocker"], path="root.intraday_rates.blocker")
    contract = _mapping(intraday["contract"], OFFICIAL_INTRADAY_RATE_CONTRACT_FIELDS, path="root.intraday_rates.contract", required=OFFICIAL_INTRADAY_RATE_CONTRACT_FIELDS)
    _string_list(contract["required_fields"], path="root.intraday_rates.contract.required_fields")
    for field in ("causal_rule", "no_data_policy", "material_change_policy"):
        _exact_str(contract[field], path=f"root.intraday_rates.contract.{field}", nonempty=True)
    expected_status = "degraded" if row["global_gaps"] or any(item["missing_or_degraded"] for item in evidence.values()) else "ready"
    if row["status"] != expected_status:
        _fail("root.status", "reconciled_status")
    material = {key: value for key, value in row.items() if key != "snapshot_id"}
    expected_id = "official_fact_v5_snapshot_" + _canonical_hash(material)[:24]
    if row["snapshot_id"] != expected_id:
        _fail("root.snapshot_id", "full_material_identity")


def validate_official_fact_v5_snapshot(snapshot: Mapping[str, Any]) -> None:
    """Validate types/chronology/counts, then independently reconstruct bytes."""

    verify_official_fact_v5_inputs()
    _validate_surface(snapshot)
    expected = _build_expected(snapshot["decision_cutoff_utc"])
    if dict(snapshot) != expected:
        raise OfficialFactAdapterV5Error("official_fact_v5_fixed_reconstruction_mismatch")


class OfficialFactAdapterV5(_PinnedV2Parent):
    """Generate only the exact full-universe, disabled V5 research view."""

    def __init__(self) -> None:
        verify_official_fact_v5_inputs()
        super().__init__(
            paths=_fixed_paths(),
            sqlite_timeout_sec=FIXED_SQLITE_TIMEOUT_SEC,
            maximum_rows_per_input=FIXED_MAXIMUM_ROWS_PER_INPUT,
            timely_release_latency_sec=FIXED_TIMELY_RELEASE_LATENCY_SEC,
            maximum_clock_observation_age_sec=FIXED_MAXIMUM_CLOCK_OBSERVATION_AGE_SEC,
        )

    def as_of(
        self,
        decision_cutoff_utc: str | dt.datetime,
        *,
        currencies: Sequence[str] = CANONICAL_CURRENCIES,
    ) -> dict[str, Any]:
        if type(currencies) not in (tuple, list) or tuple(currencies) != tuple(CANONICAL_CURRENCIES):
            raise OfficialFactAdapterV5Error("official_fact_v5_requires_exact_full_currency_universe")
        snapshot = _build_expected(decision_cutoff_utc)
        _validate_surface(snapshot)
        return snapshot


__all__ = [
    "ADAPTER_CONTRACT_ID",
    "ADAPTER_SCHEMA_VERSION",
    "ALLOWED_V5_FACT_TYPES",
    "CLOCK_DEPENDENCY_POLICY",
    "ELIMINATED_MUTABLE_INPUTS",
    "FACT_BASIS_ELIGIBILITY_CONTRACT_ID",
    "OfficialFactAdapterV5",
    "OfficialFactAdapterV5Error",
    "PinnedArtifactSpec",
    "read_pinned_artifact",
    "validate_official_fact_v5_snapshot",
    "verify_official_fact_v5_inputs",
]
