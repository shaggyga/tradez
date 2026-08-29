"""Fail-closed, research-only OfficialFact V4 proof adapter.

V4 deliberately has no caller-selectable inputs.  It regenerates the complete
V2 parent from fixed sources and accepts a V4 snapshot only when projection
back to that independently regenerated parent is byte-for-byte equivalent.
The historical Clock V1 retirement evidence and the Clock V2 input are
separate, content-addressed, read-only archive copies; canonical ledgers are
never opened by this module.

This module exposes no registration, promotion, authorization, broker, or
execution API.  Consensus remains non-causal until a separately versioned
pre-release capture contract can prove when the value was known.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .immutable_event_clock import (
    CONTRACT_ID as IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
    SCHEMA_VERSION as IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
)
from .official_fact_adapter import CANONICAL_CURRENCIES
from .official_fact_adapter_v2 import (
    ADAPTER_CONTRACT_ID as V2_ADAPTER_CONTRACT_ID,
    PARENT_ADAPTER_CONTRACT_ID as V1_ADAPTER_CONTRACT_ID,
    OFFICIAL_FACT_V2_TOP_LEVEL_FIELDS,
    OfficialFactAdapterV2,
    OfficialFactPathsV2,
    validate_official_fact_v2_snapshot,
)
from .official_fact_adapter_v3 import (
    ADAPTER_CONTRACT_ID as V3_ADAPTER_CONTRACT_ID,
    CLOCK_DEPENDENCY_POLICY as V3_CLOCK_DEPENDENCY_POLICY,
    OfficialFactAdapterV3Error,
    _canonical_hash,
    _exact_bool,
    _exact_int,
    _exact_str,
    _finite_number,
    _timestamp,
    validate_official_fact_v3_snapshot,
)


ADAPTER_SCHEMA_VERSION = 4
ADAPTER_CONTRACT_ID = "official_fact_adapter_v4_20260817"
PARENT_ADAPTER_CONTRACT_ID = V3_ADAPTER_CONTRACT_ID
FACT_BASIS_ELIGIBILITY_CONTRACT_ID = (
    "official_fact_basis_eligibility_v4_20260817"
)
CLOCK_DEPENDENCY_POLICY = (
    "strict_clock_v2_content_addressed_read_only_archive_"
    "clock_v1_retirement_archive_pinned"
)
OFFICIAL_FACT_V4_TOP_LEVEL_FIELDS = OFFICIAL_FACT_V2_TOP_LEVEL_FIELDS

SOURCE_ROOT = Path(__file__).resolve().parents[3]
ARCHIVE_ROOT = (
    SOURCE_ROOT
    / "data"
    / "oanda_training_manager"
    / "source_archives"
    / "official_fact_adapter_v4"
)
ARCHIVED_CLOCK_V1_DATABASE = (
    ARCHIVE_ROOT / "immutable_event_clock_v1_d45a8fd09077cfb1c.sqlite"
)
ARCHIVED_CLOCK_V1_RETIREMENT = (
    ARCHIVE_ROOT / "immutable_event_clock_v1_retirement_4914b5f17accc153.json"
)
ARCHIVED_CLOCK_V2_DATABASE = (
    ARCHIVE_ROOT / "immutable_event_clock_v2_a5276c696eb6ff3.sqlite"
)

ARCHIVED_CLOCK_V1_DATABASE_BYTES = 1_384_448
ARCHIVED_CLOCK_V1_DATABASE_SHA256 = (
    "d45a8fd09077cfb1c733003900d2e6ef6ae7180dffd6e09f66ab4b0412aff874"
)
ARCHIVED_CLOCK_V1_RETIREMENT_BYTES = 1_617
ARCHIVED_CLOCK_V1_RETIREMENT_SHA256 = (
    "4914b5f17accc1532891172254d2f39469eac98307e4d415e0a7e7d77f950c6d"
)
ARCHIVED_CLOCK_V2_DATABASE_BYTES = 909_312
ARCHIVED_CLOCK_V2_DATABASE_SHA256 = (
    "a5276c696eb6ff3c46adfcff41201288c2c966a76a52240897f5f0f25d30a740"
)

ARCHIVED_CLOCK_V1_ROW_COUNTS = {
    "event_clock_capture_attestations": 2,
    "event_clock_observations": 3,
    "event_clock_snapshots": 3,
    "event_clock_versions": 501,
    "snapshot_events": 593,
}
ARCHIVED_CLOCK_V2_ROW_COUNTS = {
    "event_clock_capture_attestations": 1,
    "event_clock_ledger_identity": 1,
    "event_clock_observations": 1,
    "event_clock_snapshots": 1,
    "event_clock_versions": 251,
    "snapshot_events": 251,
}

FIXED_SQLITE_TIMEOUT_SEC = 5.0
FIXED_MAXIMUM_ROWS_PER_INPUT = 5_000
FIXED_TIMELY_RELEASE_LATENCY_SEC = 300.0
FIXED_MAXIMUM_CLOCK_OBSERVATION_AGE_SEC = 300.0


class OfficialFactAdapterV4Error(RuntimeError):
    """Raised when fixed evidence, chronology, or parent closure is invalid."""


@dataclass(frozen=True)
class PinnedArtifactSpec:
    """An exact immutable input contract used by the secure reader."""

    path: Path
    root: Path
    byte_length: int
    sha256: str
    require_read_only: bool = True
    sqlite_row_counts: Mapping[str, int] | None = None


def _norm_path(path: Path) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    # Windows may expose a handle-specific ctime tick after opening an
    # otherwise unchanged read-only file.  Device/inode/mode/link-count/size,
    # content mtime, attributes, full bytes, and digest form the stable proof.
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
    reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(int(getattr(value, "st_file_attributes", 0)) & int(reparse))


def _is_read_only(value: os.stat_result) -> bool:
    if os.name == "nt":
        readonly = getattr(stat, "FILE_ATTRIBUTE_READONLY", 0x1)
        return bool(int(getattr(value, "st_file_attributes", 0)) & int(readonly))
    return value.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH) == 0


def _assert_no_linked_ancestor(path: Path, root: Path) -> None:
    resolved_root = root.resolve(strict=True)
    if _norm_path(path).startswith(_norm_path(resolved_root) + os.sep) is False:
        raise OfficialFactAdapterV4Error("pinned_artifact_outside_fixed_root")
    relative = path.relative_to(resolved_root)
    cursor = resolved_root
    for part in relative.parts:
        cursor = cursor / part
        metadata = os.lstat(cursor)
        if stat.S_ISLNK(metadata.st_mode) or _is_reparse(metadata):
            raise OfficialFactAdapterV4Error("pinned_artifact_reparse_or_symlink")


def read_pinned_artifact(spec: PinnedArtifactSpec) -> tuple[bytes, dict[str, Any]]:
    """Read exact bytes while proving lexical, resolved, link, and race identity.

    The public V4 adapters call this only with compile-time fixed specs.  It is
    intentionally exposed for adversarial tests and for the fixed
    CurrencyState dependency verifier; accepting a spec is not an adapter input.
    """

    path = spec.path
    root = spec.root
    if not path.is_absolute() or not root.is_absolute():
        raise OfficialFactAdapterV4Error("pinned_artifact_path_not_absolute")
    lexical = _norm_path(path)
    try:
        resolved = path.resolve(strict=True)
        resolved_root = root.resolve(strict=True)
    except OSError as exc:
        raise OfficialFactAdapterV4Error("pinned_artifact_missing") from exc
    if lexical != _norm_path(resolved):
        raise OfficialFactAdapterV4Error("pinned_artifact_lexical_resolved_mismatch")
    try:
        if os.path.commonpath((lexical, _norm_path(resolved_root))) != _norm_path(
            resolved_root
        ):
            raise OfficialFactAdapterV4Error("pinned_artifact_outside_fixed_root")
    except ValueError as exc:
        raise OfficialFactAdapterV4Error("pinned_artifact_outside_fixed_root") from exc
    _assert_no_linked_ancestor(resolved, resolved_root)

    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode):
        raise OfficialFactAdapterV4Error("pinned_artifact_not_regular_file")
    if before.st_nlink != 1:
        raise OfficialFactAdapterV4Error("pinned_artifact_hardlink_count_invalid")
    if spec.require_read_only and not _is_read_only(before):
        raise OfficialFactAdapterV4Error("pinned_artifact_not_read_only")
    if before.st_size != spec.byte_length:
        raise OfficialFactAdapterV4Error("pinned_artifact_byte_length_mismatch")

    try:
        with path.open("rb", buffering=0) as handle:
            opened_before = os.fstat(handle.fileno())
            if _stat_identity(opened_before) != _stat_identity(before):
                raise OfficialFactAdapterV4Error("pinned_artifact_open_race")
            payload = handle.read()
            opened_after = os.fstat(handle.fileno())
            path_after = os.lstat(path)
    except OSError as exc:
        raise OfficialFactAdapterV4Error("pinned_artifact_read_failed") from exc

    if (
        _stat_identity(opened_before) != _stat_identity(opened_after)
        or _stat_identity(opened_after) != _stat_identity(path_after)
        or _norm_path(path.resolve(strict=True)) != lexical
    ):
        raise OfficialFactAdapterV4Error("pinned_artifact_identity_changed_during_read")
    if len(payload) != spec.byte_length:
        raise OfficialFactAdapterV4Error("pinned_artifact_byte_length_mismatch")
    digest = hashlib.sha256(payload).hexdigest()
    if digest != spec.sha256:
        raise OfficialFactAdapterV4Error("pinned_artifact_sha256_mismatch")

    result: dict[str, Any] = {
        "path": str(path),
        "resolved_path": str(resolved),
        "byte_length": len(payload),
        "sha256": digest,
        "read_only": _is_read_only(path_after),
        "link_count": int(path_after.st_nlink),
    }
    if spec.sqlite_row_counts is not None:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        try:
            connection.deserialize(payload)
            quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
            if quick_check != "ok":
                raise OfficialFactAdapterV4Error("pinned_sqlite_quick_check_failed")
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if not set(spec.sqlite_row_counts).issubset(tables):
                raise OfficialFactAdapterV4Error("pinned_sqlite_table_missing")
            counts = {
                table: int(
                    connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                )
                for table in spec.sqlite_row_counts
            }
            if counts != dict(spec.sqlite_row_counts):
                raise OfficialFactAdapterV4Error("pinned_sqlite_row_count_mismatch")
            result["quick_check"] = quick_check
            result["row_counts"] = counts
        except sqlite3.Error as exc:
            raise OfficialFactAdapterV4Error("pinned_sqlite_invalid") from exc
        finally:
            connection.close()
    return payload, result


@contextmanager
def hold_pinned_artifact(spec: PinnedArtifactSpec):
    """Hold an OS proof lock while a path-based consumer reads fixed bytes.

    On Windows the handle permits only other readers, denying write, delete,
    rename, and replacement for the full V2 SQLite reconstruction.  The path
    and handle identity are checked before and after the consumer runs.  This
    closes the gap between secure byte verification and SQLite's path open.
    """

    if os.name != "nt":  # pragma: no cover - repository runtime is Windows
        payload, metadata = read_pinned_artifact(spec)
        handle = spec.path.open("rb", buffering=0)
        try:
            opened = os.fstat(handle.fileno())
            if _stat_identity(opened) != _stat_identity(os.lstat(spec.path)):
                raise OfficialFactAdapterV4Error("pinned_artifact_guard_open_race")
            yield payload, metadata
            if _stat_identity(opened) != _stat_identity(os.lstat(spec.path)):
                raise OfficialFactAdapterV4Error("pinned_artifact_guard_close_race")
        finally:
            handle.close()
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
    generic_read = 0x80000000
    share_read = 0x00000001
    open_existing = 3
    open_reparse_point = 0x00200000
    handle_value = create_file(
        str(spec.path),
        generic_read,
        share_read,
        None,
        open_existing,
        open_reparse_point,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if handle_value == invalid_handle:
        raise OfficialFactAdapterV4Error(
            f"pinned_artifact_guard_open_failed:{ctypes.get_last_error()}"
        )
    fd = -1
    guarded = None
    try:
        fd = msvcrt.open_osfhandle(
            int(handle_value), os.O_RDONLY | getattr(os, "O_BINARY", 0)
        )
        guarded = os.fdopen(fd, "rb", buffering=0)
        fd = -1  # ownership transferred to guarded
        locked_identity = _stat_identity(os.fstat(guarded.fileno()))
        if locked_identity != _stat_identity(os.lstat(spec.path)):
            raise OfficialFactAdapterV4Error("pinned_artifact_guard_open_race")
        payload, metadata = read_pinned_artifact(spec)
        if locked_identity != _stat_identity(os.lstat(spec.path)):
            raise OfficialFactAdapterV4Error("pinned_artifact_guard_verify_race")
        yield payload, metadata
        if (
            locked_identity != _stat_identity(os.fstat(guarded.fileno()))
            or locked_identity != _stat_identity(os.lstat(spec.path))
        ):
            raise OfficialFactAdapterV4Error("pinned_artifact_guard_close_race")
    finally:
        if guarded is not None:
            guarded.close()
        elif fd >= 0:
            os.close(fd)
        else:
            ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(handle_value)


_CLOCK_V1_DB_SPEC = PinnedArtifactSpec(
    ARCHIVED_CLOCK_V1_DATABASE,
    ARCHIVE_ROOT,
    ARCHIVED_CLOCK_V1_DATABASE_BYTES,
    ARCHIVED_CLOCK_V1_DATABASE_SHA256,
    sqlite_row_counts=ARCHIVED_CLOCK_V1_ROW_COUNTS,
)
_CLOCK_V1_RETIREMENT_SPEC = PinnedArtifactSpec(
    ARCHIVED_CLOCK_V1_RETIREMENT,
    ARCHIVE_ROOT,
    ARCHIVED_CLOCK_V1_RETIREMENT_BYTES,
    ARCHIVED_CLOCK_V1_RETIREMENT_SHA256,
)
_CLOCK_V2_DB_SPEC = PinnedArtifactSpec(
    ARCHIVED_CLOCK_V2_DATABASE,
    ARCHIVE_ROOT,
    ARCHIVED_CLOCK_V2_DATABASE_BYTES,
    ARCHIVED_CLOCK_V2_DATABASE_SHA256,
    sqlite_row_counts=ARCHIVED_CLOCK_V2_ROW_COUNTS,
)


def _strict_json(payload: bytes, *, label: str) -> dict[str, Any]:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise OfficialFactAdapterV4Error(f"{label}_duplicate_json_key")
            result[key] = value
        return result

    def nonfinite(value: str) -> None:
        raise OfficialFactAdapterV4Error(f"{label}_nonfinite_json_number:{value}")

    try:
        parsed = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=nonfinite,
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise OfficialFactAdapterV4Error(f"{label}_invalid_json") from exc
    if type(parsed) is not dict:
        raise OfficialFactAdapterV4Error(f"{label}_not_exact_mapping")
    return parsed


def verify_official_fact_v4_archives() -> dict[str, Any]:
    """Verify three distinct archive copies; never open canonical ledgers."""

    _, v1_database = read_pinned_artifact(_CLOCK_V1_DB_SPEC)
    retirement_bytes, v1_retirement = read_pinned_artifact(
        _CLOCK_V1_RETIREMENT_SPEC
    )
    _, v2_database = read_pinned_artifact(_CLOCK_V2_DB_SPEC)
    retirement = _strict_json(retirement_bytes, label="clock_v1_retirement")
    database = retirement.get("database")
    policy = retirement.get("policy")
    if (
        retirement.get("status") != "retired_terminal"
        or type(database) is not dict
        or database.get("sha256") != ARCHIVED_CLOCK_V1_DATABASE_SHA256
        or database.get("byte_length") != ARCHIVED_CLOCK_V1_DATABASE_BYTES
        or database.get("row_counts") != ARCHIVED_CLOCK_V1_ROW_COUNTS
        or database.get("preserve_bytes") is not True
        or database.get("read_only_historical_evidence") is not True
        or type(policy) is not dict
        or policy.get("writer_retired") is not True
        or policy.get("no_append") is not True
        or policy.get("no_checkpoint") is not True
        or policy.get("no_migration") is not True
        or policy.get("no_backfill") is not True
        or policy.get("research_only") is not True
        or policy.get("execution_eligible") is not False
        or policy.get("can_place_orders") is not False
        or policy.get("can_promote") is not False
        or policy.get("can_authorize") is not False
        or policy.get("supported_execution_decision") != "no_trade"
    ):
        raise OfficialFactAdapterV4Error("clock_v1_retirement_claim_mismatch")
    return {
        "canonical_files_opened": False,
        "archive_root": str(ARCHIVE_ROOT),
        "clock_v1_database": v1_database,
        "clock_v1_retirement": v1_retirement,
        "clock_v2_database": v2_database,
    }


def _fixed_paths() -> OfficialFactPathsV2:
    return OfficialFactPathsV2(immutable_event_clock_db=ARCHIVED_CLOCK_V2_DATABASE)


def _new_v2_parent() -> OfficialFactAdapterV2:
    return OfficialFactAdapterV2(
        paths=_fixed_paths(),
        sqlite_timeout_sec=FIXED_SQLITE_TIMEOUT_SEC,
        maximum_rows_per_input=FIXED_MAXIMUM_ROWS_PER_INPUT,
        timely_release_latency_sec=FIXED_TIMELY_RELEASE_LATENCY_SEC,
        maximum_clock_observation_age_sec=(
            FIXED_MAXIMUM_CLOCK_OBSERVATION_AGE_SEC
        ),
    )


def _brand(snapshot: dict[str, Any], *, version: int) -> None:
    if version == 2:
        snapshot["schema_version"] = 2
        snapshot["adapter_contract_id"] = V2_ADAPTER_CONTRACT_ID
        snapshot["parent_adapter_contract_id"] = V1_ADAPTER_CONTRACT_ID
        snapshot["clock_dependency_policy"] = "strict_v2_only_no_mutable_fallback"
        prefix = "official_fact_v2_snapshot_"
    elif version == 3:
        snapshot["schema_version"] = 3
        snapshot["adapter_contract_id"] = V3_ADAPTER_CONTRACT_ID
        snapshot["parent_adapter_contract_id"] = V2_ADAPTER_CONTRACT_ID
        snapshot["clock_dependency_policy"] = V3_CLOCK_DEPENDENCY_POLICY
        prefix = "official_fact_v3_snapshot_"
    elif version == 4:
        snapshot["schema_version"] = ADAPTER_SCHEMA_VERSION
        snapshot["adapter_contract_id"] = ADAPTER_CONTRACT_ID
        snapshot["parent_adapter_contract_id"] = PARENT_ADAPTER_CONTRACT_ID
        snapshot["clock_dependency_policy"] = CLOCK_DEPENDENCY_POLICY
        prefix = "official_fact_v4_snapshot_"
    else:  # pragma: no cover - internal programming guard
        raise OfficialFactAdapterV4Error("unsupported_brand_version")
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    snapshot["snapshot_id"] = prefix + _canonical_hash(material)[:24]


def _project(snapshot: Mapping[str, Any], *, version: int) -> dict[str, Any]:
    result = copy.deepcopy(dict(snapshot))
    _brand(result, version=version)
    return result


def _at_or_before(
    value: Any, *, path: str, upper: dt.datetime, nullable: bool = True
) -> dt.datetime | None:
    parsed = _timestamp(value, path=path, nullable=nullable)
    if parsed is not None and parsed > upper:
        raise OfficialFactAdapterV4Error(f"causal_chronology:{path}:after_upper_bound")
    return parsed


def _validate_fact_chronology(snapshot: Mapping[str, Any], cutoff: dt.datetime) -> None:
    for index, fact in enumerate(snapshot["facts"]):
        path = f"root.facts[{index}]"
        effective = _at_or_before(
            fact["effective_from_utc"],
            path=f"{path}.effective_from_utc",
            upper=cutoff,
            nullable=False,
        )
        assert effective is not None
        times: dict[str, dt.datetime | None] = {}
        for field in ("published_at_utc", "first_seen_at_utc", "retrieved_at_utc"):
            times[field] = _at_or_before(
                fact[field], path=f"{path}.{field}", upper=effective
            )
        published = times["published_at_utc"]
        if published is not None:
            for field in ("first_seen_at_utc", "retrieved_at_utc"):
                value = times[field]
                if value is not None and value < published:
                    raise OfficialFactAdapterV4Error(
                        f"causal_chronology:{path}.{field}:before_publication"
                    )
        if fact["fact_type"] == "official_macro_actual":
            scheduled = _at_or_before(
                fact["scheduled_utc"],
                path=f"{path}.scheduled_utc",
                upper=effective,
            )
            ledger = _at_or_before(
                fact["ledger_recorded_at_utc"],
                path=f"{path}.ledger_recorded_at_utc",
                upper=effective,
                nullable=False,
            )
            if published is not None and scheduled is not None and scheduled > published:
                raise OfficialFactAdapterV4Error(
                    f"causal_chronology:{path}.scheduled_utc:after_publication"
                )
            if ledger is None:
                raise OfficialFactAdapterV4Error(
                    f"causal_chronology:{path}.ledger_recorded_at_utc:missing"
                )
        elif fact["fact_type"] == "internal_macro_expectation":
            training = _at_or_before(
                fact["training_cutoff_utc"],
                path=f"{path}.training_cutoff_utc",
                upper=effective,
                nullable=False,
            )
            expires = _timestamp(fact["expires_utc"], path=f"{path}.expires_utc")
            if training is None or expires is None or effective > expires or cutoff > expires:
                raise OfficialFactAdapterV4Error(
                    f"causal_chronology:{path}:expectation_outside_frozen_window"
                )

        # This schema has no consensus capture timestamp or independently
        # verified pre-release provenance.  No row may claim causal consensus.
        if fact["consensus_causal"] is not False:
            raise OfficialFactAdapterV4Error(
                f"causal_consensus:{path}:pre_release_proof_unavailable"
            )

    if snapshot["causal_consensus_count"] != 0:
        raise OfficialFactAdapterV4Error("causal_consensus:top_level_must_be_zero")
    for currency, summary in snapshot["currency_evidence"].items():
        if summary["causal_consensus_count"] != 0:
            raise OfficialFactAdapterV4Error(
                f"causal_consensus:currency_evidence.{currency}:must_be_zero"
            )


def _validate_clock_chronology(snapshot: Mapping[str, Any], cutoff: dt.datetime) -> None:
    provenance = snapshot["event_clock_provenance"]
    snapshot_captured = _at_or_before(
        provenance.get("snapshot_captured_utc"),
        path="root.event_clock_provenance.snapshot_captured_utc",
        upper=cutoff,
        nullable=False,
    )
    observed = _at_or_before(
        provenance.get("clock_observed_utc"),
        path="root.event_clock_provenance.clock_observed_utc",
        upper=cutoff,
        nullable=False,
    )
    source_generated = _at_or_before(
        provenance.get("source_generated_utc"),
        path="root.event_clock_provenance.source_generated_utc",
        upper=cutoff,
        nullable=False,
    )
    assert snapshot_captured and observed and source_generated
    if source_generated > snapshot_captured or snapshot_captured > observed:
        raise OfficialFactAdapterV4Error("clock_chronology:source_snapshot_observation")

    attestation = provenance.get("clock_attestation")
    if type(attestation) is not dict:
        raise OfficialFactAdapterV4Error("clock_chronology:attestation_missing")
    attestation_generated = _at_or_before(
        attestation.get("generated_utc"),
        path="root.event_clock_provenance.clock_attestation.generated_utc",
        upper=snapshot_captured,
        nullable=False,
    )
    assert attestation_generated is not None
    if attestation_generated < source_generated:
        raise OfficialFactAdapterV4Error("clock_chronology:attestation_before_source")

    recorded_attestation_age = _finite_number(
        attestation.get("age_at_capture_sec"),
        path="root.event_clock_provenance.clock_attestation.age_at_capture_sec",
        minimum=0.0,
    )
    maximum_attestation_age = _finite_number(
        attestation.get("maximum_age_sec"),
        path="root.event_clock_provenance.clock_attestation.maximum_age_sec",
        minimum=0.0,
    )
    recomputed_attestation_age = (
        snapshot_captured - attestation_generated
    ).total_seconds()
    if not math.isclose(
        float(recorded_attestation_age),
        recomputed_attestation_age,
        rel_tol=0.0,
        abs_tol=1e-6,
    ) or recomputed_attestation_age > float(maximum_attestation_age):
        raise OfficialFactAdapterV4Error("clock_chronology:attestation_age_invalid")

    recorded_observation_age = _finite_number(
        provenance.get("observation_age_sec"),
        path="root.event_clock_provenance.observation_age_sec",
        minimum=0.0,
    )
    maximum_observation_age = _finite_number(
        provenance.get("maximum_observation_age_sec"),
        path="root.event_clock_provenance.maximum_observation_age_sec",
        minimum=0.0,
    )
    recomputed_observation_age = (cutoff - observed).total_seconds()
    if not math.isclose(
        float(recorded_observation_age),
        recomputed_observation_age,
        rel_tol=0.0,
        abs_tol=1e-6,
    ):
        raise OfficialFactAdapterV4Error("clock_chronology:observation_age_invalid")
    if not math.isclose(
        float(maximum_observation_age),
        float(snapshot["maximum_clock_observation_age_sec"]),
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise OfficialFactAdapterV4Error("clock_chronology:maximum_age_mismatch")

    ready = provenance["clock_ready_for_cutoff"]
    if ready:
        if (
            recomputed_observation_age > float(maximum_observation_age)
            or provenance["complete_snapshot_at_cutoff"] is not True
            or provenance["state"] != "immutable_v2_fresh_attested_at_cutoff"
        ):
            raise OfficialFactAdapterV4Error("clock_chronology:ready_claim_invalid")
    else:
        if (
            recomputed_observation_age <= float(maximum_observation_age)
            or provenance["complete_snapshot_at_cutoff"] is not False
            or provenance.get("degradation_reason")
            != "latest_attested_clock_observation_stale"
            or snapshot["upcoming_events"]
        ):
            raise OfficialFactAdapterV4Error("clock_chronology:stale_claim_invalid")

    for index, event in enumerate(snapshot["upcoming_events"]):
        known = _timestamp(
            event["known_from_snapshot_utc"],
            path=f"root.upcoming_events[{index}].known_from_snapshot_utc",
        )
        ledger = _timestamp(
            event["ledger_effective_known_utc"],
            path=f"root.upcoming_events[{index}].ledger_effective_known_utc",
        )
        scheduled = _timestamp(
            event["scheduled_utc"],
            path=f"root.upcoming_events[{index}].scheduled_utc",
        )
        if (
            known != snapshot_captured
            or ledger is None
            or ledger > cutoff
            or scheduled is None
            or scheduled < cutoff
            or event["consensus_causal"] is not False
        ):
            raise OfficialFactAdapterV4Error(
                f"clock_chronology:event_{index}_causal_identity_invalid"
            )

    legacy_clock_as_of = snapshot["clock"].get("as_of_utc")
    if legacy_clock_as_of is not None:
        _at_or_before(
            legacy_clock_as_of,
            path="root.clock.as_of_utc",
            upper=cutoff,
            nullable=False,
        )
    intraday_as_of = snapshot["intraday_rates"].get("as_of_utc")
    if intraday_as_of is not None:
        _at_or_before(
            intraday_as_of,
            path="root.intraday_rates.as_of_utc",
            upper=cutoff,
            nullable=False,
        )


def validate_official_fact_v4_snapshot(snapshot: Mapping[str, Any]) -> None:
    """Reopen fixed artifacts, regenerate V2, and close every V4 claim."""

    verify_official_fact_v4_archives()
    if type(snapshot) is not dict or set(snapshot) != set(
        OFFICIAL_FACT_V4_TOP_LEVEL_FIELDS
    ):
        raise OfficialFactAdapterV4Error("official_fact_v4_top_level_not_exact")
    if type(snapshot.get("schema_version")) is not int or snapshot.get(
        "schema_version"
    ) != ADAPTER_SCHEMA_VERSION:
        raise OfficialFactAdapterV4Error("official_fact_v4_schema_mismatch")
    for field, expected in (
        ("adapter_contract_id", ADAPTER_CONTRACT_ID),
        ("parent_adapter_contract_id", PARENT_ADAPTER_CONTRACT_ID),
        ("clock_dependency_policy", CLOCK_DEPENDENCY_POLICY),
    ):
        if type(snapshot.get(field)) is not str or snapshot.get(field) != expected:
            raise OfficialFactAdapterV4Error(f"official_fact_v4_{field}_mismatch")
    if snapshot.get("can_promote") is not False or snapshot.get("can_authorize") is not False:
        raise OfficialFactAdapterV4Error("official_fact_v4_governance_flags_invalid")

    v3_projection = _project(snapshot, version=3)
    try:
        validate_official_fact_v3_snapshot(v3_projection)
    except OfficialFactAdapterV3Error as exc:
        raise OfficialFactAdapterV4Error(
            f"official_fact_v4_recursive_v3_integrity:{exc}"
        ) from exc

    cutoff = _timestamp(
        snapshot["decision_cutoff_utc"], path="root.decision_cutoff_utc"
    )
    assert cutoff is not None
    _validate_fact_chronology(snapshot, cutoff)
    _validate_clock_chronology(snapshot, cutoff)

    # This is the decisive proof closure: all fact rows, source health,
    # quarantine/gap counts, events, hashes, IDs, attestations, and summaries
    # must equal a fresh fixed-source reconstruction rather than caller claims.
    with hold_pinned_artifact(_CLOCK_V2_DB_SPEC):
        regenerated = _new_v2_parent().as_of(
            snapshot["decision_cutoff_utc"], currencies=CANONICAL_CURRENCIES
        )
    verify_official_fact_v4_archives()
    projected_v2 = _project(snapshot, version=2)
    validate_official_fact_v2_snapshot(projected_v2)
    if projected_v2 != regenerated:
        raise OfficialFactAdapterV4Error(
            "official_fact_v4_independent_parent_reconstruction_mismatch"
        )

    expected = copy.deepcopy(dict(snapshot))
    _brand(expected, version=4)
    if expected != dict(snapshot):
        raise OfficialFactAdapterV4Error("official_fact_v4_snapshot_id_mismatch")


class OfficialFactAdapterV4(OfficialFactAdapterV2):
    """Generate only the fixed, full-universe, disabled V4 research view."""

    def __init__(self) -> None:
        verify_official_fact_v4_archives()
        super().__init__(
            paths=_fixed_paths(),
            sqlite_timeout_sec=FIXED_SQLITE_TIMEOUT_SEC,
            maximum_rows_per_input=FIXED_MAXIMUM_ROWS_PER_INPUT,
            timely_release_latency_sec=FIXED_TIMELY_RELEASE_LATENCY_SEC,
            maximum_clock_observation_age_sec=(
                FIXED_MAXIMUM_CLOCK_OBSERVATION_AGE_SEC
            ),
        )

    def as_of(
        self,
        decision_cutoff_utc: str | dt.datetime,
        *,
        currencies: Sequence[str] = CANONICAL_CURRENCIES,
    ) -> dict[str, Any]:
        if type(currencies) not in (tuple, list) or tuple(currencies) != tuple(
            CANONICAL_CURRENCIES
        ):
            raise OfficialFactAdapterV4Error(
                "official_fact_v4_requires_exact_full_currency_universe"
            )
        verify_official_fact_v4_archives()
        with hold_pinned_artifact(_CLOCK_V2_DB_SPEC):
            snapshot = super().as_of(
                decision_cutoff_utc, currencies=CANONICAL_CURRENCIES
            )
        verify_official_fact_v4_archives()
        snapshot["can_promote"] = False
        snapshot["can_authorize"] = False
        _brand(snapshot, version=4)
        validate_official_fact_v4_snapshot(snapshot)
        return snapshot


__all__ = [
    "ADAPTER_CONTRACT_ID",
    "ADAPTER_SCHEMA_VERSION",
    "ARCHIVED_CLOCK_V1_DATABASE",
    "ARCHIVED_CLOCK_V1_DATABASE_SHA256",
    "ARCHIVED_CLOCK_V1_RETIREMENT",
    "ARCHIVED_CLOCK_V1_RETIREMENT_SHA256",
    "ARCHIVED_CLOCK_V2_DATABASE",
    "ARCHIVED_CLOCK_V2_DATABASE_SHA256",
    "CLOCK_DEPENDENCY_POLICY",
    "FACT_BASIS_ELIGIBILITY_CONTRACT_ID",
    "OFFICIAL_FACT_V4_TOP_LEVEL_FIELDS",
    "OfficialFactAdapterV4",
    "OfficialFactAdapterV4Error",
    "PinnedArtifactSpec",
    "hold_pinned_artifact",
    "read_pinned_artifact",
    "validate_official_fact_v4_snapshot",
    "verify_official_fact_v4_archives",
]
