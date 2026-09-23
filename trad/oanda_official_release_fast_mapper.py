"""Map the V4 official-release fast lane into shadow currency semantics.

This worker consumes the append-only raw observation ledger and applies the
current, version-bound news classifier at the original first-seen clock.  It
publishes a separate research-only mapping ledger and snapshot.  It cannot
place orders, authorize candidates, promote hypotheses or alter the canonical
news database/watchlist.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Mapping

import oanda_local_news_sentiment as news
import oanda_official_release_fast_lane as fast_lane
from oanda_official_release_fast_lane_contract import (
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
)


ROOT = Path(__file__).resolve().parent
LOCAL_NEWS_ROOT = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
INPUT_DATABASE = LOCAL_NEWS_ROOT / "official_release_fast_lane_v4.sqlite"
OUTPUT_DATABASE = LOCAL_NEWS_ROOT / "official_release_fast_mapping_v3.sqlite"
SNAPSHOT_PATH = LOCAL_NEWS_ROOT / "official_release_fast_mapping_latest_v3.json"
HEARTBEAT_PATH = LOCAL_NEWS_ROOT / "official_release_fast_mapping_heartbeat_v3.json"

SCHEMA_VERSION = "official_release_fast_mapping_v3"
CONTRACT_ID = "official_release_fast_mapping_v3_semantic_vs_publish_gate_20260824"
COHORT_ID = "official_release_fast_mapping_v3_20260824"
REQUIRED_INPUT_CONTRACT = fast_lane.CONTRACT_ID
RETAINED_PRIOR_INPUT_CONTRACTS = {
    (
        OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
        OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
    )
}
REQUIRED_CLASSIFICATION_VERSION = news.CLASSIFICATION_VERSION
PRE_MAP_QUOTE_CONTRACT_ID = (
    "official_release_fast_mapper_pre_semantic_quote_v1_20260828"
)
MINIMUM_QUOTE_COUNT = fast_lane.EXPECTED_QUOTE_COUNT

_INTEGRITY_CACHE: dict[str, tuple[tuple[Any, ...], str]] = {}
_DIAGNOSTIC_CACHE: dict[
    tuple[str, str, str],
    tuple[tuple[Any, ...], dict[str, int], list[dict[str, Any]], str],
] = {}
_CAUGHT_UP_CACHE: dict[
    tuple[str, str, str, str],
    tuple[int, int, tuple[Any, ...], tuple[Any, ...]],
] = {}


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime | None = None) -> str:
    current = value or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=dt.timezone.utc)
    return current.astimezone(dt.timezone.utc).isoformat()


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                return
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(min(0.5, 0.01 * (2**attempt)))
    finally:
        temporary.unlink(missing_ok=True)


def _database_fingerprint(path: Path) -> tuple[Any, ...]:
    """Return a durable-content fingerprint for a SQLite database.

    SQLite touches ``-shm`` and may recreate an empty ``-wal`` whenever a
    reader opens or closes the database.  Those clock-only changes do not
    alter durable database content, but including them invalidated the
    integrity/diagnostic caches on every mapper poll and caused a full
    ``quick_check`` plus aggregate rebuild roughly every five seconds.

    The main database remains size/mtime bound.  A non-empty WAL is also
    size/mtime bound because it contains uncheckpointed durable frames.  An
    empty WAL is represented by size alone, and the transient SHM file is
    deliberately excluded.  This preserves invalidation for every SQLite
    content change while allowing exact-output reuse across no-input-change
    cycles.
    """

    values: list[Any] = []
    candidates = (path, path.with_name(path.name + "-wal"))
    for index, candidate in enumerate(candidates):
        try:
            stat = candidate.stat()
            values.extend(
                (
                    str(candidate.resolve()),
                    stat.st_size,
                    stat.st_mtime_ns if stat.st_size > 0 else 0,
                )
            )
        except OSError:
            # SQLite freely creates and removes an empty WAL as connections
            # come and go.  Missing and zero-byte WALs are the same durable
            # content state.  The main database must still remain present.
            if index == 1:
                values.extend((str(candidate.resolve()), 0, 0))
            else:
                values.extend((str(candidate.resolve()), None, None))
    return tuple(values)


def verify_database_integrity(
    connection: sqlite3.Connection, path: Path
) -> str:
    fingerprint = _database_fingerprint(path)
    key = str(path.resolve()).lower()
    cached = _INTEGRITY_CACHE.get(key)
    if cached is not None and cached[0] == fingerprint:
        return cached[1]
    integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    _INTEGRITY_CACHE[key] = (fingerprint, integrity)
    return integrity


def input_database_state(
    path: Path,
) -> tuple[int, int, tuple[Any, ...], str, bool]:
    """Read an append-only high-water mark and verify its stable snapshot."""

    fingerprint_before = _database_fingerprint(path)
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(path, timeout=10.0)
        state_row = connection.execute(
            "SELECT COUNT(*), COALESCE(MAX(rowid), 0) "
            "FROM official_release_observation"
        ).fetchone()
        input_count = int(state_row[0])
        highwater = int(state_row[1])
        integrity = verify_database_integrity(connection, path)
    finally:
        if connection is not None:
            connection.close()
    fingerprint_after = _database_fingerprint(path)
    return (
        highwater,
        input_count,
        fingerprint_after,
        integrity,
        fingerprint_before == fingerprint_after,
    )


def pre_map_quote_snapshot_from_raw_capture(
    observation: Mapping[str, Any],
    capture: Mapping[str, Any],
) -> dict[str, Any]:
    """Adapt one immutable raw-boundary sidecar; never read or recapture quotes."""

    exact = bool(
        capture.get("timing_quality") == "prospective_exact_live_quote"
        and int(capture.get("proof_quote_count") or 0) == MINIMUM_QUOTE_COUNT
        and isinstance(capture.get("quotes"), Mapping)
        and len(capture.get("quotes") or {}) == MINIMUM_QUOTE_COUNT
    )
    return {
        # Keep the frozen downstream adapter contract while exposing the new
        # origin contract/cohort explicitly.  The downstream response worker
        # still revalidates every venue timestamp and bid/ask.
        "capture_contract_id": PRE_MAP_QUOTE_CONTRACT_ID,
        "capture_origin": "official_release_raw_append_boundary",
        "raw_capture_contract_id": str(
            capture.get("capture_contract_id") or ""
        ),
        "raw_capture_cohort_id": str(capture.get("capture_cohort_id") or ""),
        "raw_capture_activated_utc": str(
            capture.get("capture_activated_utc") or ""
        ),
        "observation_id": str(observation.get("observation_id") or ""),
        "event_first_known_utc": str(
            capture.get("event_first_known_utc") or ""
        ),
        "captured_utc": str(capture.get("captured_utc") or ""),
        "capture_latency_seconds": capture.get("capture_latency_seconds"),
        "timing_quality": str(capture.get("timing_quality") or ""),
        "invalid_reason": str(capture.get("invalid_reason") or ""),
        "quote_count": MINIMUM_QUOTE_COUNT if exact else 0,
        "quotes": dict(capture.get("quotes") or {}) if exact else {},
        "observed_valid_quote_count": int(
            capture.get("observed_valid_quote_count") or 0
        ),
        "missing_instruments": list(capture.get("missing_instruments") or []),
        "invalid_instruments": dict(capture.get("invalid_instruments") or {}),
        "instrument_universe_sha256": str(
            capture.get("instrument_universe_sha256") or ""
        ),
        "connection_generation": capture.get("connection_generation"),
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_promote": False,
    }


def open_output_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS official_release_mapping (
            mapping_id TEXT PRIMARY KEY,
            observation_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_contract_id TEXT NOT NULL,
            first_seen_utc TEXT NOT NULL,
            input_prospective_observation INTEGER NOT NULL CHECK (input_prospective_observation IN (0, 1)),
            input_listing_bootstrap INTEGER NOT NULL CHECK (input_listing_bootstrap IN (0, 1)),
            input_publisher_time_eligible INTEGER NOT NULL CHECK (input_publisher_time_eligible IN (0, 1)),
            classification_version TEXT NOT NULL,
            semantic_direction_available INTEGER NOT NULL CHECK (semantic_direction_available IN (0, 1)),
            prospective_semantic_candidate INTEGER NOT NULL CHECK (prospective_semantic_candidate IN (0, 1)),
            publish_eligible_forward_candidate INTEGER NOT NULL CHECK (publish_eligible_forward_candidate IN (0, 1)),
            forward_shadow_candidate INTEGER NOT NULL CHECK (forward_shadow_candidate IN (0, 1)),
            mapping_payload_json TEXT NOT NULL,
            mapped_utc TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK (research_only = 1),
            execution_eligible INTEGER NOT NULL CHECK (execution_eligible = 0),
            can_authorize INTEGER NOT NULL CHECK (can_authorize = 0),
            can_promote INTEGER NOT NULL CHECK (can_promote = 0),
            mapper_contract_id TEXT NOT NULL,
            mapper_cohort_id TEXT NOT NULL,
            UNIQUE(observation_id, classification_version)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_fast_mapping_candidate_seen "
        "ON official_release_mapping(forward_shadow_candidate, first_seen_utc)"
    )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_fast_mapping_no_update
        BEFORE UPDATE ON official_release_mapping
        BEGIN
            SELECT RAISE(ABORT, 'official release mapping is append-only');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_fast_mapping_no_delete
        BEFORE DELETE ON official_release_mapping
        BEGIN
            SELECT RAISE(ABORT, 'official release mapping is append-only');
        END
        """
    )
    connection.commit()
    return connection


def read_observations(
    input_database: Path,
    output_connection: sqlite3.Connection,
    *,
    limit: int = 5000,
    maximum_input_rowid: int | None = None,
) -> list[dict[str, Any]]:
    mapped = {
        str(row[0])
        for row in output_connection.execute(
            """
            SELECT observation_id
            FROM official_release_mapping
            WHERE classification_version = ? AND mapper_contract_id = ?
            """,
            (REQUIRED_CLASSIFICATION_VERSION, CONTRACT_ID),
        ).fetchall()
    }
    input_connection: sqlite3.Connection | None = None
    try:
        input_connection = sqlite3.connect(input_database, timeout=10.0)
        has_capture_sidecar = bool(
            input_connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='official_release_quote_capture'"
            ).fetchone()
        )
        capture_projection = (
            "q.capture_payload_json, q.capture_contract_id, "
            "q.capture_cohort_id, q.capture_activation_eligible"
            if has_capture_sidecar
            else "NULL, NULL, NULL, NULL"
        )
        capture_join = (
            "LEFT JOIN official_release_quote_capture q "
            "ON q.observation_id = o.observation_id"
            if has_capture_sidecar
            else ""
        )
        rowid_clause = (
            "WHERE o.rowid <= ?" if maximum_input_rowid is not None else ""
        )
        parameters: tuple[Any, ...]
        if maximum_input_rowid is None:
            parameters = (max(limit + len(mapped), limit),)
        else:
            parameters = (
                max(0, int(maximum_input_rowid)),
                max(limit + len(mapped), limit),
            )
        rows = input_connection.execute(
            f"""
            SELECT o.observation_id, o.source_id, o.source_contract_id,
                   o.first_seen_utc, o.prospective_observation,
                   o.listing_bootstrap, o.publisher_time_eligible,
                   o.raw_payload_json, o.collector_contract_id,
                   o.collector_cohort_id, {capture_projection}
            FROM official_release_observation o
            {capture_join}
            {rowid_clause}
            ORDER BY o.first_seen_utc, o.observation_id
            LIMIT ?
            """,
            parameters,
        ).fetchall()
        output: list[dict[str, Any]] = []
        for row in rows:
            if str(row[0]) in mapped:
                continue
            collector_contract_id = str(row[8])
            collector_cohort_id = str(row[9])
            current_input_contract = bool(
                collector_contract_id == REQUIRED_INPUT_CONTRACT
                and collector_cohort_id == fast_lane.COLLECTOR_COHORT_ID
            )
            retained_prior_contract = (
                collector_contract_id,
                collector_cohort_id,
            ) in RETAINED_PRIOR_INPUT_CONTRACTS
            if not current_input_contract and not retained_prior_contract:
                raise ValueError("fast-lane input contract mismatch")
            try:
                payload = json.loads(str(row[7]))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError("fast-lane raw payload is invalid JSON") from exc
            if not isinstance(payload, dict):
                raise ValueError("fast-lane raw payload must be an object")
            upstream_prospective = bool(row[4])
            raw_capture: dict[str, Any] | None = None
            capture_current_cohort = False
            capture_activation_eligible = False
            capture_state = "missing_historical_diagnostic"
            if row[10] is not None:
                try:
                    decoded_capture = json.loads(str(row[10]))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise ValueError(
                        "fast-lane raw quote capture is invalid JSON"
                    ) from exc
                if not isinstance(decoded_capture, dict):
                    raise ValueError(
                        "fast-lane raw quote capture must be an object"
                    )
                raw_capture = decoded_capture
                if str(raw_capture.get("observation_id") or "") != str(row[0]):
                    raise ValueError("fast-lane quote capture observation mismatch")
                if str(raw_capture.get("event_first_known_utc") or "") != str(
                    row[3]
                ):
                    raise ValueError("fast-lane quote capture event clock mismatch")
                if str(raw_capture.get("capture_contract_id") or "") != str(
                    row[11] or ""
                ) or str(raw_capture.get("capture_cohort_id") or "") != str(
                    row[12] or ""
                ):
                    raise ValueError("fast-lane quote capture lineage mismatch")
                if bool(raw_capture.get("capture_activation_eligible")) != bool(
                    row[13]
                ):
                    raise ValueError("fast-lane quote capture activation mismatch")
                capture_current_cohort = bool(
                    str(row[11] or "") == fast_lane.QUOTE_CAPTURE_CONTRACT_ID
                    and str(row[12] or "") == fast_lane.QUOTE_CAPTURE_COHORT_ID
                    and str(
                        raw_capture.get("instrument_universe_sha256") or ""
                    )
                    == fast_lane.EXPECTED_QUOTE_UNIVERSE_SHA256
                )
                capture_activation_eligible = bool(
                    capture_current_cohort and row[13]
                )
                capture_state = (
                    "current_prospective_attachment"
                    if capture_activation_eligible
                    else "current_diagnostic"
                    if capture_current_cohort
                    else "retained_prior_capture_cohort"
                )
            effective_prospective = bool(
                upstream_prospective
                and current_input_contract
                and capture_activation_eligible
            )
            observation = {
                "observation_id": str(row[0]),
                "source_id": str(row[1]),
                "source_contract_id": str(row[2]),
                "first_seen_utc": str(row[3]),
                # Raw prospectivity remains visible, but only the newly
                # activated append-boundary capture cohort may carry it into
                # semantic response research.  Missing historical sidecars
                # are diagnostics and are never reconstructed.
                "prospective_observation": effective_prospective,
                "upstream_prospective_observation": upstream_prospective,
                "listing_bootstrap": bool(row[5]),
                "publisher_time_eligible": bool(row[6]),
                "collector_contract_id": collector_contract_id,
                "collector_cohort_id": collector_cohort_id,
                "retained_prior_collector_contract": retained_prior_contract,
                "raw_quote_capture_state": capture_state,
                "raw_quote_capture_current_cohort": capture_current_cohort,
                "raw_quote_capture_activation_eligible": (
                    capture_activation_eligible
                ),
                "raw_quote_capture": raw_capture,
                "raw": payload,
            }
            if raw_capture is not None and capture_activation_eligible:
                observation["pre_map_quote_snapshot"] = (
                    pre_map_quote_snapshot_from_raw_capture(
                        observation, raw_capture
                    )
                )
            output.append(observation)
            if len(output) >= limit:
                break

        integrity = verify_database_integrity(input_connection, input_database)
        if integrity != "ok":
            raise ValueError(f"fast-lane input integrity failed: {integrity}")
        return output
    finally:
        if input_connection is not None:
            input_connection.close()


def classify_observation(observation: Mapping[str, Any]) -> dict[str, Any]:
    first_seen = news.parse_datetime(observation.get("first_seen_utc"))
    if first_seen is None:
        raise ValueError("fast-lane observation has invalid first_seen_utc")
    classified = news.classify_article(
        dict(observation.get("raw") or {}), first_seen=first_seen
    )
    primary_scores = classified.get("currency_scores") or {}
    research_scores = classified.get("research_currency_scores") or {}
    if not isinstance(primary_scores, Mapping):
        primary_scores = {}
    if not isinstance(research_scores, Mapping):
        research_scores = {}
    semantic_direction_available = any(
        abs(news.safe_float(value, 0.0)) > 0.0
        for value in [*primary_scores.values(), *research_scores.values()]
    )
    input_prospective = bool(observation.get("prospective_observation"))
    upstream_prospective = bool(
        observation.get(
            "upstream_prospective_observation",
            observation.get("prospective_observation"),
        )
    )
    retained_prior_contract = bool(
        observation.get("retained_prior_collector_contract")
    )
    classification_candidate_activation_eligible = bool(
        classified.get("issuer_bound_policy_communication") is not True
        or classified.get(
            "issuer_bound_policy_communication_activation_eligible"
        )
        is True
    )
    prospective_semantic_candidate = bool(
        input_prospective
        and semantic_direction_available
        and classified.get("source_direct") is True
        and classified.get("source_verified") is True
        and classification_candidate_activation_eligible
    )
    publish_eligible_forward_candidate = bool(
        prospective_semantic_candidate
        and classified.get("directional_publish_eligible") is True
    )
    # This ledger is shadow-only. Preserve every direct, verified,
    # prospectively clocked semantic hypothesis for response testing while
    # recording the stricter publish gate independently. Missing consensus or
    # rate confirmation must not erase a research observation, and can never
    # make it executable.
    forward_shadow_candidate = prospective_semantic_candidate
    # Bootstrap, backfill and late old-feed rows can retain their research
    # mapping for diagnostics, but can never become forward candidates.
    if not input_prospective:
        classified["directional_publish_eligible"] = False
        classified["forward_signal_timely"] = False
        classified["execution_eligible"] = False
        classified["can_place_orders"] = False
    classified.update(
        {
            "fast_lane_observation_id": str(observation.get("observation_id") or ""),
            "fast_lane_input_contract_id": REQUIRED_INPUT_CONTRACT,
            "fast_lane_observation_collector_contract_id": str(
                observation.get("collector_contract_id") or ""
            ),
            "fast_lane_observation_collector_cohort_id": str(
                observation.get("collector_cohort_id") or ""
            ),
            "fast_lane_retained_prior_collector_contract": (
                retained_prior_contract
            ),
            "fast_lane_upstream_prospective_observation": (
                upstream_prospective
            ),
            "fast_lane_mapper_contract_id": CONTRACT_ID,
            "fast_lane_first_seen_utc": iso_utc(first_seen),
            "fast_lane_prospective_observation": input_prospective,
            "fast_lane_listing_bootstrap": bool(observation.get("listing_bootstrap")),
            "fast_lane_publisher_time_eligible": bool(
                observation.get("publisher_time_eligible")
            ),
            "fast_lane_raw_quote_capture_state": str(
                observation.get("raw_quote_capture_state") or ""
            ),
            "fast_lane_raw_quote_capture_current_cohort": bool(
                observation.get("raw_quote_capture_current_cohort")
            ),
            "fast_lane_raw_quote_capture_activation_eligible": bool(
                observation.get("raw_quote_capture_activation_eligible")
            ),
            "fast_lane_raw_quote_capture_contract_id": (
                str(
                    (observation.get("raw_quote_capture") or {}).get(
                        "capture_contract_id"
                    )
                    or ""
                )
                if isinstance(observation.get("raw_quote_capture"), Mapping)
                else ""
            ),
            "fast_lane_raw_quote_capture_cohort_id": (
                str(
                    (observation.get("raw_quote_capture") or {}).get(
                        "capture_cohort_id"
                    )
                    or ""
                )
                if isinstance(observation.get("raw_quote_capture"), Mapping)
                else ""
            ),
            "semantic_direction_available": semantic_direction_available,
            "classification_candidate_activation_eligible": (
                classification_candidate_activation_eligible
            ),
            "prospective_semantic_candidate": prospective_semantic_candidate,
            "publish_eligible_forward_candidate": (
                publish_eligible_forward_candidate
            ),
            "forward_shadow_candidate": forward_shadow_candidate,
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "can_authorize": False,
            "can_promote": False,
            "supported_decision": "shadow_observation_only",
        }
    )
    pre_map_quote_snapshot = observation.get("pre_map_quote_snapshot")
    if isinstance(pre_map_quote_snapshot, Mapping):
        classified["fast_lane_pre_map_quote_snapshot"] = dict(
            pre_map_quote_snapshot
        )
    return classified


def insert_mapping(
    connection: sqlite3.Connection,
    *,
    observation: Mapping[str, Any],
    classified: Mapping[str, Any],
    mapped_utc: dt.datetime,
) -> bool:
    observation_id = str(observation.get("observation_id") or "")
    mapping_id = hashlib.sha256(
        f"{observation_id}|{REQUIRED_CLASSIFICATION_VERSION}|{CONTRACT_ID}".encode(
            "utf-8"
        )
    ).hexdigest()
    payload_json = json.dumps(
        dict(classified), sort_keys=True, separators=(",", ":"), default=str
    )
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO official_release_mapping (
            mapping_id, observation_id, source_id, source_contract_id,
            first_seen_utc, input_prospective_observation,
            input_listing_bootstrap, input_publisher_time_eligible,
            classification_version, semantic_direction_available,
            prospective_semantic_candidate,publish_eligible_forward_candidate,
            forward_shadow_candidate, mapping_payload_json, mapped_utc,
            research_only, execution_eligible, can_authorize, can_promote,
            mapper_contract_id, mapper_cohort_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            mapping_id,
            observation_id,
            str(observation.get("source_id") or ""),
            str(observation.get("source_contract_id") or ""),
            str(observation.get("first_seen_utc") or ""),
            int(bool(observation.get("prospective_observation"))),
            int(bool(observation.get("listing_bootstrap"))),
            int(bool(observation.get("publisher_time_eligible"))),
            REQUIRED_CLASSIFICATION_VERSION,
            int(bool(classified.get("semantic_direction_available"))),
            int(bool(classified.get("prospective_semantic_candidate"))),
            int(bool(classified.get("publish_eligible_forward_candidate"))),
            int(bool(classified.get("forward_shadow_candidate"))),
            payload_json,
            iso_utc(mapped_utc),
            1,
            0,
            0,
            0,
            CONTRACT_ID,
            COHORT_ID,
        ),
    )
    connection.commit()
    return bool(cursor.rowcount)


def mapping_counts(connection: sqlite3.Connection) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT COUNT(*),
               COALESCE(SUM(input_prospective_observation), 0),
               COALESCE(SUM(semantic_direction_available), 0),
               COALESCE(SUM(prospective_semantic_candidate), 0),
               COALESCE(SUM(publish_eligible_forward_candidate), 0),
               COALESCE(SUM(forward_shadow_candidate), 0),
               COUNT(DISTINCT source_id)
        FROM official_release_mapping
        WHERE classification_version = ? AND mapper_contract_id = ?
        """,
        (REQUIRED_CLASSIFICATION_VERSION, CONTRACT_ID),
    ).fetchone()
    retained = connection.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT classification_version)
        FROM official_release_mapping
        WHERE mapper_contract_id = ?
        """,
        (CONTRACT_ID,),
    ).fetchone()
    return {
        "mappings": int(row[0]),
        "prospective_inputs": int(row[1]),
        "semantic_direction_available": int(row[2]),
        "prospective_semantic_candidates": int(row[3]),
        "publish_eligible_forward_candidates": int(row[4]),
        "forward_shadow_candidates": int(row[5]),
        "mapped_sources": int(row[6]),
        "retained_mappings_all_classifier_versions": int(retained[0]),
        "retained_classifier_versions": int(retained[1]),
    }


def latest_candidates(connection: sqlite3.Connection, limit: int = 25) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT mapping_payload_json
        FROM official_release_mapping
        WHERE forward_shadow_candidate = 1
          AND classification_version = ?
          AND mapper_contract_id = ?
        ORDER BY first_seen_utc DESC, mapping_id DESC
        LIMIT ?
        """,
        (REQUIRED_CLASSIFICATION_VERSION, CONTRACT_ID, limit),
    ).fetchall()
    return [json.loads(str(row[0])) for row in rows]


def mapping_diagnostics(
    connection: sqlite3.Connection,
    output_database: Path,
) -> tuple[dict[str, int], list[dict[str, Any]], str]:
    """Reuse global diagnostics while the append-only database is unchanged."""

    fingerprint = _database_fingerprint(output_database)
    key = (
        str(output_database.resolve()).lower(),
        REQUIRED_CLASSIFICATION_VERSION,
        CONTRACT_ID,
    )
    cached = _DIAGNOSTIC_CACHE.get(key)
    if cached is not None and cached[0] == fingerprint:
        return cached[1], cached[2], cached[3]
    counts = mapping_counts(connection)
    candidates = latest_candidates(connection)
    integrity = verify_database_integrity(connection, output_database)
    _DIAGNOSTIC_CACHE[key] = (fingerprint, counts, candidates, integrity)
    return counts, candidates, integrity


def rebind_output_caches_after_close(output_database: Path) -> None:
    """Bind verified logical output state to its post-checkpoint files.

    An inserting cycle verifies the complete SQLite view while frames may
    still reside in the WAL.  Closing the final connection can checkpoint
    those same verified frames into the main file, changing its filesystem
    fingerprint without changing logical content.  Rebinding after a clean
    close avoids one redundant full verification on the next unchanged
    cycle.  It never creates a cache entry and therefore cannot certify an
    unverified database.
    """

    fingerprint = _database_fingerprint(output_database)
    integrity_key = str(output_database.resolve()).lower()
    cached_integrity = _INTEGRITY_CACHE.get(integrity_key)
    if cached_integrity is not None:
        _INTEGRITY_CACHE[integrity_key] = (
            fingerprint,
            cached_integrity[1],
        )
    diagnostic_key = (
        integrity_key,
        REQUIRED_CLASSIFICATION_VERSION,
        CONTRACT_ID,
    )
    cached_diagnostics = _DIAGNOSTIC_CACHE.get(diagnostic_key)
    if cached_diagnostics is not None:
        _DIAGNOSTIC_CACHE[diagnostic_key] = (
            fingerprint,
            cached_diagnostics[1],
            cached_diagnostics[2],
            cached_diagnostics[3],
        )


def next_cycle_sleep_seconds(interval_sec: float, elapsed_sec: float) -> float:
    """Treat interval as start-to-start cadence, not extra post-cycle delay."""

    target = max(5.0, float(interval_sec))
    return max(0.1, target - max(0.0, float(elapsed_sec)))


def publish_heartbeat(
    *,
    path: Path,
    status: str,
    phase: str,
    cycle_started: dt.datetime,
    details: Mapping[str, Any],
) -> None:
    write_json_atomic(
        path,
        {
            "schema_version": SCHEMA_VERSION,
            "mapper_contract_id": CONTRACT_ID,
            "mapper_cohort_id": COHORT_ID,
            "required_input_contract_id": REQUIRED_INPUT_CONTRACT,
            "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
            "raw_quote_capture_contract_id": fast_lane.QUOTE_CAPTURE_CONTRACT_ID,
            "raw_quote_capture_cohort_id": fast_lane.QUOTE_CAPTURE_COHORT_ID,
            "raw_quote_capture_activated_utc": fast_lane.iso_utc(
                fast_lane.QUOTE_CAPTURE_ACTIVATED_UTC
            ),
            "expected_quote_count": fast_lane.EXPECTED_QUOTE_COUNT,
            "quote_universe_sha256": fast_lane.EXPECTED_QUOTE_UNIVERSE_SHA256,
            "generated_utc": iso_utc(),
            "heartbeat_utc": iso_utc(),
            "cycle_started_utc": iso_utc(cycle_started),
            "cycle_in_progress": status == "running_cycle",
            "status": status,
            "phase": phase,
            "details": dict(details),
            "policy": {
                "research_only": True,
                "execution_eligible": False,
                "can_authorize": False,
                "can_promote": False,
                "broker_access": False,
            },
        },
    )


def run_cycle(
    *,
    input_database: Path = INPUT_DATABASE,
    output_database: Path = OUTPUT_DATABASE,
    snapshot_path: Path = SNAPSHOT_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
    limit: int = 5000,
) -> dict[str, Any]:
    cycle_started = utc_now()
    publish_heartbeat(
        path=heartbeat_path,
        status="running_cycle",
        phase="loading_observations",
        cycle_started=cycle_started,
        details={},
    )
    (
        input_highwater,
        input_count,
        input_fingerprint,
        input_integrity,
        input_state_stable,
    ) = input_database_state(input_database)
    if input_integrity != "ok":
        raise ValueError(f"fast-lane input integrity failed: {input_integrity}")
    connection = open_output_database(output_database)
    inserted = 0
    cache_key = (
        str(input_database.resolve()).lower(),
        str(output_database.resolve()).lower(),
        REQUIRED_CLASSIFICATION_VERSION,
        CONTRACT_ID,
    )
    caught_up = _CAUGHT_UP_CACHE.get(cache_key)
    output_fingerprint_before = _database_fingerprint(output_database)
    reuse_caught_up = bool(
        input_state_stable
        and caught_up is not None
        and caught_up[0] == input_highwater
        and caught_up[1] == input_count
        and caught_up[2] == input_fingerprint
        and caught_up[3] == output_fingerprint_before
    )
    try:
        if reuse_caught_up:
            observations: list[dict[str, Any]] = []
        else:
            observations = read_observations(
                input_database,
                connection,
                limit=max(1, limit),
                maximum_input_rowid=input_highwater,
            )
        publish_heartbeat(
            path=heartbeat_path,
            status="running_cycle",
            phase="mapping_observations",
            cycle_started=cycle_started,
            details={"pending_observations": len(observations)},
        )
        for observation in observations:
            classified = classify_observation(observation)
            inserted += int(
                insert_mapping(
                    connection,
                    observation=observation,
                    classified=classified,
                    mapped_utc=utc_now(),
                )
            )
        captured_pre_map = sum(
            int(isinstance(row.get("pre_map_quote_snapshot"), Mapping))
            for row in observations
        )
        valid_pre_map = sum(
            int(
                str(
                    (row.get("pre_map_quote_snapshot") or {}).get(
                        "timing_quality"
                    )
                )
                == "prospective_exact_live_quote"
            )
            for row in observations
        )
        counts, candidates, integrity = mapping_diagnostics(
            connection, output_database
        )
    finally:
        connection.close()
    rebind_output_caches_after_close(output_database)
    if input_state_stable and int(counts.get("mappings") or 0) == input_count:
        _CAUGHT_UP_CACHE[cache_key] = (
            input_highwater,
            input_count,
            input_fingerprint,
            _database_fingerprint(output_database),
        )
    else:
        _CAUGHT_UP_CACHE.pop(cache_key, None)
    result = {
        "schema_version": SCHEMA_VERSION,
        "mapper_contract_id": CONTRACT_ID,
        "mapper_cohort_id": COHORT_ID,
        "required_input_contract_id": REQUIRED_INPUT_CONTRACT,
        "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
        "generated_utc": iso_utc(),
        "inserted_mappings": inserted,
        "pre_map_quote_capture": {
            "capture_contract_id": PRE_MAP_QUOTE_CONTRACT_ID,
            "raw_capture_contract_id": fast_lane.QUOTE_CAPTURE_CONTRACT_ID,
            "raw_capture_cohort_id": fast_lane.QUOTE_CAPTURE_COHORT_ID,
            "raw_capture_activated_utc": fast_lane.iso_utc(
                fast_lane.QUOTE_CAPTURE_ACTIVATED_UTC
            ),
            "expected_quote_count": fast_lane.EXPECTED_QUOTE_COUNT,
            "instrument_universe_sha256": (
                fast_lane.EXPECTED_QUOTE_UNIVERSE_SHA256
            ),
            "capture_origin": "official_release_raw_append_boundary",
            "attempted": captured_pre_map,
            "valid_exact": valid_pre_map,
            "invalid_or_late": captured_pre_map - valid_pre_map,
        },
        "counts": counts,
        "latest_forward_shadow_candidates": candidates,
        "sqlite_integrity": integrity,
        "policy": {
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "can_authorize": False,
            "can_promote": False,
            "broker_access": False,
            "watchlist_mutation": False,
            "supported_decision": "shadow_observation_only",
        },
    }
    write_json_atomic(snapshot_path, result)
    publish_heartbeat(
        path=heartbeat_path,
        status="cycle_complete",
        phase="cycle_complete",
        cycle_started=cycle_started,
        details={"inserted_mappings": inserted, **counts},
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-database", type=Path, default=INPUT_DATABASE)
    parser.add_argument("--output-database", type=Path, default=OUTPUT_DATABASE)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT_PATH)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress the cycle payload on stdout; reports and heartbeats are unchanged.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    while True:
        cycle_monotonic = time.monotonic()
        result = run_cycle(
            input_database=args.input_database,
            output_database=args.output_database,
            snapshot_path=args.snapshot,
            heartbeat_path=args.heartbeat,
            limit=args.limit,
        )
        if not args.quiet:
            print(json.dumps(result, sort_keys=True), flush=True)
        if args.once:
            return 0
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(
            next_cycle_sleep_seconds(
                args.interval_sec, time.monotonic() - cycle_monotonic
            )
        )


if __name__ == "__main__":
    raise SystemExit(main())
