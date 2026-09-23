#!/usr/bin/env python3
"""Build an append-only, causal macro-release and surprise ledger.

The ledger separates scheduled/actual official releases from narrative news.
It records source revisions and leaves directional interpretation unavailable
when series semantics or a genuine consensus value are missing.  It has no
broker or execution capability.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from oanda_worker_heartbeat import WorkerHeartbeat
import oanda_macro_consensus_prospective as prospective_consensus


UTC = timezone.utc
ROOT = Path(__file__).resolve().parent
NEWS_DB = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
STATE = ROOT / "data" / "oanda_training_manager" / "state"
INGEST_CONTRACT_ID = "macro_surprise_causal_readiness_v3_20260824"
CONSENSUS_IMPORT_CONTRACT_ID = "macro_consensus_import_v2_response_complete_20260829"
HEARTBEAT = STATE / "macro_surprise_heartbeat_v1.json"
CONSENSUS_ARCHIVE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "source_archives"
    / "macro_consensus_prospective_v1"
)
LOWER_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def finite_or_none(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def open_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=10.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=10000")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS macro_release_revisions (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            revision_id TEXT NOT NULL UNIQUE,
            release_key TEXT NOT NULL,
            source_event_id TEXT NOT NULL,
            recorded_utc TEXT NOT NULL,
            causal_known_utc TEXT NOT NULL,
            scheduled_utc TEXT NOT NULL,
            source_reported_update_utc TEXT NOT NULL,
            event_series_id TEXT NOT NULL,
            event_name TEXT NOT NULL,
            event_country TEXT NOT NULL,
            currencies_json TEXT NOT NULL,
            reference_period TEXT NOT NULL,
            reference_date TEXT NOT NULL,
            importance TEXT NOT NULL,
            unit TEXT NOT NULL,
            actual_text TEXT NOT NULL,
            actual_value REAL,
            consensus_text TEXT NOT NULL,
            consensus_value REAL,
            previous_text TEXT NOT NULL,
            previous_value REAL,
            revised_previous_text TEXT NOT NULL,
            revised_previous_value REAL,
            surprise_raw REAL,
            standardized_surprise REAL,
            standardization_prior_n INTEGER NOT NULL,
            standardization_prior_mean REAL,
            standardization_prior_std REAL,
            directional_interpretation TEXT NOT NULL,
            release_importance TEXT NOT NULL,
            first_market_reaction_pips REAL,
            subsequent_reaction_pips REAL,
            reaction_class TEXT NOT NULL,
            reaction_status TEXT NOT NULL,
            known_before_recorded_timestamp INTEGER NOT NULL,
            source_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_verified INTEGER NOT NULL,
            source_direct INTEGER NOT NULL,
            payload_sha256 TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS macro_release_scope
            ON macro_release_revisions(event_series_id, causal_known_utc);
        CREATE INDEX IF NOT EXISTS macro_release_key
            ON macro_release_revisions(release_key, row_id);
        CREATE TRIGGER IF NOT EXISTS macro_release_revisions_no_update
        BEFORE UPDATE ON macro_release_revisions
        BEGIN SELECT RAISE(ABORT, 'macro release revisions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS macro_release_revisions_no_delete
        BEFORE DELETE ON macro_release_revisions
        BEGIN SELECT RAISE(ABORT, 'macro release revisions are immutable'); END;
        CREATE VIEW IF NOT EXISTS macro_release_latest AS
        SELECT r.* FROM macro_release_revisions r
        WHERE r.row_id = (
            SELECT MAX(r2.row_id) FROM macro_release_revisions r2
            WHERE r2.release_key=r.release_key
        );
        CREATE TABLE IF NOT EXISTS macro_consensus_observations (
            observation_id TEXT PRIMARY KEY,
            release_key TEXT NOT NULL,
            event_series_id TEXT NOT NULL,
            scheduled_utc TEXT NOT NULL,
            captured_utc TEXT NOT NULL,
            source_timestamp_utc TEXT NOT NULL,
            consensus_text TEXT NOT NULL,
            consensus_value REAL NOT NULL,
            source_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_verified INTEGER NOT NULL,
            causal_valid INTEGER NOT NULL,
            rejection_reason TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS macro_consensus_scope
            ON macro_consensus_observations(event_series_id,scheduled_utc,captured_utc);
        CREATE TRIGGER IF NOT EXISTS macro_consensus_no_update
        BEFORE UPDATE ON macro_consensus_observations
        BEGIN SELECT RAISE(ABORT, 'macro consensus observations are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS macro_consensus_no_delete
        BEFORE DELETE ON macro_consensus_observations
        BEGIN SELECT RAISE(ABORT, 'macro consensus observations are immutable'); END;
        CREATE TABLE IF NOT EXISTS macro_reaction_samples (
            sample_id TEXT PRIMARY KEY,
            release_key TEXT NOT NULL,
            instrument TEXT NOT NULL,
            target_offset_sec INTEGER NOT NULL,
            scheduled_utc TEXT NOT NULL,
            sampled_utc TEXT NOT NULL,
            quote_time_utc TEXT NOT NULL,
            bid REAL NOT NULL,
            ask REAL NOT NULL,
            mid REAL NOT NULL,
            pip REAL NOT NULL,
            distance_from_target_sec REAL NOT NULL,
            payload_sha256 TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS macro_reaction_scope
            ON macro_reaction_samples(release_key,instrument,target_offset_sec);
        CREATE TRIGGER IF NOT EXISTS macro_reaction_no_update
        BEFORE UPDATE ON macro_reaction_samples
        BEGIN SELECT RAISE(ABORT, 'macro reaction samples are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS macro_reaction_no_delete
        BEFORE DELETE ON macro_reaction_samples
        BEGIN SELECT RAISE(ABORT, 'macro reaction samples are immutable'); END;
        """
    )
    connection.commit()
    return connection


def current_consensus_contract() -> dict[str, Any]:
    config = prospective_consensus.read_json(prospective_consensus.CONFIG)
    contract = prospective_consensus.cohort_contract(config)
    return {
        "source_contract_id": contract["source_contract_id"],
        "cohort_id": contract["cohort_id"],
        "cohort_start_utc": contract["cohort_start_utc"],
        "capture_contract_id": contract["capture_contract_id"],
        "observation_clock_contract_id": contract[
            "observation_clock_contract_id"
        ],
        "provider": contract["provider"],
    }


def archived_snapshot_matches(
    payload: Mapping[str, Any], archive_path: Path | None
) -> bool:
    snapshot_sha = str(payload.get("provider_snapshot_sha256") or "")
    if archive_path is None or LOWER_SHA256.fullmatch(snapshot_sha) is None:
        return False
    raw_path = archive_path / f"{snapshot_sha}.json"
    try:
        raw = raw_path.read_bytes()
    except OSError:
        return False
    if hashlib.sha256(raw).hexdigest() != snapshot_sha:
        return False
    try:
        rows = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(rows, list):
        return False
    expected_event = str(payload.get("provider_event_id") or "")
    expected_series = str(payload.get("event_series_id") or "")
    expected_schedule = parse_time(payload.get("scheduled_utc"))
    expected_update = parse_time(payload.get("source_timestamp_utc"))
    expected_value = finite_or_none(payload.get("consensus_value"))
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        event_id = str(row.get("CalendarId") or row.get("CalendarID") or "").strip()
        if event_id != expected_event:
            continue
        if prospective_consensus.event_series_id(row) != expected_series:
            continue
        if parse_time(row.get("Date")) != expected_schedule:
            continue
        if parse_time(row.get("LastUpdate")) != expected_update:
            continue
        if finite_or_none(row.get("ForecastValue")) != expected_value:
            continue
        if str(row.get("DateSpan") if row.get("DateSpan") is not None else "") not in {
            "0", "0.0"
        }:
            continue
        if (
            finite_or_none(row.get("ActualValue")) is not None
            or prospective_consensus.populated(row.get("Actual"))
        ):
            continue
        return True
    return False


def validate_consensus_projection_row(
    payload: Mapping[str, Any],
    *,
    expected_contract: Mapping[str, Any],
    archive_path: Path | None,
) -> str:
    series = str(payload.get("event_series_id") or "")
    scheduled = parse_time(payload.get("scheduled_utc"))
    captured = parse_time(payload.get("captured_utc"))
    source_time = parse_time(payload.get("source_timestamp_utc"))
    request_started = parse_time(payload.get("request_started_utc"))
    response_completed = parse_time(payload.get("response_completed_utc"))
    activation = parse_time(expected_contract.get("cohort_start_utc"))
    value = finite_or_none(payload.get("consensus_value"))
    if (
        not series
        or value is None
        or scheduled is None
        or captured is None
        or source_time is None
        or request_started is None
        or response_completed is None
        or activation is None
    ):
        return "missing_identity_value_or_timestamp"
    exact_bindings = {
        "source_contract_id": expected_contract.get("source_contract_id"),
        "cohort_id": expected_contract.get("cohort_id"),
        "capture_contract_id": expected_contract.get("capture_contract_id"),
        "capture_clock_contract_id": expected_contract.get(
            "observation_clock_contract_id"
        ),
    }
    if any(str(payload.get(key) or "") != str(value or "") for key, value in exact_bindings.items()):
        return "frozen_consensus_contract_mismatch"
    if not str(payload.get("observation_id") or ""):
        return "missing_observation_id"
    if not str(payload.get("provider_event_id") or ""):
        return "missing_provider_calendar_id"
    if str(payload.get("provider_event_version") or "") != str(
        payload.get("source_timestamp_utc") or ""
    ):
        return "provider_event_version_mismatch"
    snapshot_sha = str(payload.get("provider_snapshot_sha256") or "")
    if LOWER_SHA256.fullmatch(snapshot_sha) is None:
        return "invalid_provider_snapshot_sha256"
    if captured < activation:
        return "captured_before_cohort_activation"
    if response_completed != captured:
        return "response_completion_not_capture_boundary"
    if request_started > response_completed:
        return "request_started_after_response_completed"
    if payload.get("capture_clock_trusted") is not True:
        return "capture_clock_untrusted"
    if str(payload.get("release_time_precision") or "") != "exact":
        return "release_time_not_exact"
    if payload.get("actual_present_at_capture") is not False:
        return "actual_present_or_state_unknown_at_capture"
    if captured >= scheduled:
        return "captured_at_or_after_release"
    if source_time > captured:
        return "source_timestamp_after_capture"
    if not bool(payload.get("source_verified")):
        return "source_not_verified"
    if not archived_snapshot_matches(payload, archive_path):
        return "provider_snapshot_archive_missing_or_mismatch"
    return ""


def ingest_consensus_jsonl(
    path: Path | None,
    ledger: sqlite3.Connection,
    *,
    archive_path: Path | None = CONSENSUS_ARCHIVE,
    expected_contract: Mapping[str, Any] | None = None,
) -> dict[str, int]:
    """Import timestamped expectations without allowing post-release leakage."""

    counts = {"inspected": 0, "inserted": 0, "causal_valid": 0, "rejected": 0}
    if path is None or not path.is_file():
        return counts
    contract = dict(expected_contract or current_consensus_contract())
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        counts["inspected"] += 1
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            counts["rejected"] += 1
            continue
        if not isinstance(payload, dict):
            counts["rejected"] += 1
            continue
        series = str(payload.get("event_series_id") or "")
        scheduled = str(payload.get("scheduled_utc") or "")
        captured = str(payload.get("captured_utc") or "")
        source_time = str(payload.get("source_timestamp_utc") or captured)
        value = finite_or_none(payload.get("consensus_value"))
        rejection = validate_consensus_projection_row(
            payload, expected_contract=contract, archive_path=archive_path
        )
        if value is None:
            counts["rejected"] += 1
            continue
        causal = not rejection
        key = str(payload.get("release_key") or release_key("", payload))
        material = {
            "release_key": key,
            "event_series_id": series,
            "scheduled_utc": scheduled,
            "captured_utc": captured,
            "request_started_utc": str(payload.get("request_started_utc") or ""),
            "response_completed_utc": str(payload.get("response_completed_utc") or ""),
            "source_timestamp_utc": source_time,
            "consensus_text": str(payload.get("consensus") or payload.get("consensus_text") or ""),
            "consensus_value": value,
            "source_id": str(payload.get("source_id") or ""),
            "source_name": str(payload.get("source_name") or ""),
            "source_url": str(payload.get("source_url") or ""),
            "source_verified": bool(payload.get("source_verified")),
            "source_contract_id": str(payload.get("source_contract_id") or ""),
            "cohort_id": str(payload.get("cohort_id") or ""),
            "provider_event_id": str(payload.get("provider_event_id") or ""),
            "provider_snapshot_sha256": str(
                payload.get("provider_snapshot_sha256") or ""
            ),
            "capture_contract_id": str(payload.get("capture_contract_id") or ""),
            "capture_clock_contract_id": str(
                payload.get("capture_clock_contract_id") or ""
            ),
            "capture_clock_trusted": payload.get("capture_clock_trusted") is True,
            "release_time_precision": str(
                payload.get("release_time_precision") or ""
            ),
            "actual_present_at_capture": payload.get(
                "actual_present_at_capture"
            ),
            "consensus_import_contract_id": CONSENSUS_IMPORT_CONTRACT_ID,
            "causal_valid": causal,
            "rejection_reason": rejection,
        }
        digest = stable_hash(material)
        cursor = ledger.execute(
            """
            INSERT OR IGNORE INTO macro_consensus_observations VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            (
                "consensus_" + digest[:40], key, series, scheduled, captured,
                source_time, material["consensus_text"], value,
                material["source_id"], material["source_name"], material["source_url"],
                int(material["source_verified"]), int(causal), rejection, digest,
                json.dumps(material, sort_keys=True),
            ),
        )
        if cursor.rowcount > 0:
            counts["inserted"] += 1
            counts["causal_valid" if causal else "rejected"] += 1
    ledger.commit()
    return counts


def consensus_for_release(
    ledger: sqlite3.Connection,
    *,
    series_id: str,
    scheduled_utc: str,
    release_key_value: str,
    expected_contract: Mapping[str, Any] | None = None,
) -> tuple[float | None, str, bool]:
    contract = dict(expected_contract or current_consensus_contract())
    rows = ledger.execute(
        """
        SELECT consensus_value,consensus_text,source_verified,payload_json
        FROM macro_consensus_observations
        WHERE causal_valid=1 AND (release_key=? OR (event_series_id=? AND scheduled_utc=?))
        ORDER BY captured_utc DESC
        """,
        (release_key_value, series_id, scheduled_utc),
    ).fetchall()
    expected = {
        "source_contract_id": str(contract.get("source_contract_id") or ""),
        "cohort_id": str(contract.get("cohort_id") or ""),
        "capture_contract_id": str(contract.get("capture_contract_id") or ""),
        "capture_clock_contract_id": str(
            contract.get("observation_clock_contract_id") or ""
        ),
        "consensus_import_contract_id": CONSENSUS_IMPORT_CONTRACT_ID,
    }
    for value, text, verified, raw_json in rows:
        try:
            material = json.loads(str(raw_json))
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(material, Mapping):
            continue
        if any(str(material.get(key) or "") != required for key, required in expected.items()):
            continue
        return float(value), str(text), bool(verified)
    return None, "", False


def ingest_reaction_samples(
    ledger: sqlite3.Connection, quote_snapshot: Path | None
) -> dict[str, int]:
    counts = {"eligible": 0, "inserted": 0}
    if quote_snapshot is None or not quote_snapshot.is_file():
        return counts
    try:
        snapshot = json.loads(quote_snapshot.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return counts
    sampled = parse_time(snapshot.get("generated_utc"))
    quotes = snapshot.get("quotes") or {}
    if sampled is None or not isinstance(quotes, dict):
        return counts
    offsets = (0, 60, 300, 3600)
    for key, scheduled_text, currencies_json in ledger.execute(
        "SELECT release_key,scheduled_utc,currencies_json FROM macro_release_latest WHERE scheduled_utc<>''"
    ):
        scheduled = parse_time(scheduled_text)
        if scheduled is None:
            continue
        try:
            currencies = set(json.loads(currencies_json))
        except (TypeError, json.JSONDecodeError):
            currencies = set()
        for instrument, quote in quotes.items():
            if not isinstance(quote, dict):
                continue
            pair_currencies = set(str(instrument).split("_"))
            if currencies and not pair_currencies.intersection(currencies):
                continue
            bid, ask, pip = (
                finite_or_none(quote.get("bid")), finite_or_none(quote.get("ask")),
                finite_or_none(quote.get("pip")),
            )
            if bid is None or ask is None or pip is None or pip <= 0 or ask < bid:
                continue
            elapsed = (sampled - scheduled).total_seconds()
            for offset in offsets:
                distance = elapsed - offset
                if distance < 0 or distance > 120:
                    continue
                counts["eligible"] += 1
                material = [key, instrument, offset, snapshot.get("generated_utc"), bid, ask, pip]
                digest = stable_hash(material)
                cursor = ledger.execute(
                    """
                    INSERT OR IGNORE INTO macro_reaction_samples VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                    """,
                    (
                        "reaction_" + stable_hash([key, instrument, offset])[:40], key,
                        instrument, offset, str(scheduled_text), str(snapshot.get("generated_utc") or ""),
                        str(quote.get("time") or ""), bid, ask, (bid + ask) / 2.0, pip,
                        distance, digest,
                    ),
                )
                counts["inserted"] += int(cursor.rowcount > 0)
    ledger.commit()
    return counts


def is_unclocked_state_snapshot(payload: Mapping[str, Any]) -> bool:
    """Identify a polled current-state observation that is not a release event."""

    if not bool(payload.get("context_only")):
        return False
    if str(payload.get("published_utc") or "") and not bool(
        payload.get("published_time_inferred")
    ):
        return False
    first_seen = str(payload.get("first_seen_utc") or "")
    scheduled = str(payload.get("scheduled_utc") or "")
    reported = str(payload.get("source_reported_update_utc") or "")
    if not bool(payload.get("published_time_inferred")) and (
        (scheduled and scheduled != first_seen)
        or (reported and reported != first_seen)
    ):
        return False
    return True


def release_key(event_id: str, payload: Mapping[str, Any]) -> str:
    if is_unclocked_state_snapshot(payload):
        # Current-state endpoints and repeatedly enriched article snapshots are
        # append-only revisions of one observation source, not new releases on
        # every poll.  A later exact source clock naturally creates a normal
        # release identity instead of reusing this state identity.
        identity = {
            "observation_class": "unclocked_state_snapshot",
            "series": str(payload.get("event_series_id") or ""),
            "source": str(payload.get("source_id") or ""),
            "source_url": str(payload.get("source_url") or ""),
        }
        if not any((identity["series"], identity["source"], identity["source_url"])):
            identity["source_event_id"] = event_id
        return "macro_state_" + stable_hash(identity)[:32]
    identity = {
        "series": str(payload.get("event_series_id") or ""),
        "name": str(payload.get("event_name") or ""),
        "country": str(payload.get("event_country") or ""),
        "scheduled": str(payload.get("scheduled_utc") or ""),
        "reference_period": str(payload.get("reference_period") or ""),
        "reference_date": str(payload.get("reference_date") or ""),
    }
    if not any(identity.values()):
        identity["source_event_id"] = event_id
    return "macro_" + stable_hash(identity)[:32]


def material_payload(event_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    material = {
        key: payload.get(key)
        for key in (
            "source_id",
            "source_name",
            "source_url",
            "source_verified",
            "source_direct",
            "first_seen_utc",
            "causal_known_utc",
            "scheduled_utc",
            "source_reported_update_utc",
            "numeric_causal_known_utc",
            "numeric_extraction_contract_id",
            "numeric_direction_policy",
            "source_listing_bootstrap",
            "forward_signal_timely",
            "context_only",
            "context_reason",
            "structured_event",
            "source_contract_id",
            "source_cohort_id",
            "collector_contract_id",
            "collector_cohort_id",
            "event_series_id",
            "event_name",
            "event_country",
            "currencies",
            "reference_period",
            "reference_date",
            "importance",
            "unit",
            "actual",
            "actual_value",
            "consensus",
            "consensus_value",
            "previous",
            "previous_value",
            "revised_previous",
            "revised_previous_value",
            "surprise_raw",
            "revision_raw",
            "directional_surprise_interpretation",
        )
    } | {
        "source_event_id": event_id,
        "macro_ingest_contract_id": INGEST_CONTRACT_ID,
    }
    # Preserve an explicit unknown-publication-time marker only when present.
    # Making this conditional avoids rewriting the full immutable ledger merely
    # to add a false/None default, while a genuinely inferred clock receives a
    # distinct append-only revision and can never masquerade as prospective.
    if bool(payload.get("published_time_inferred")):
        material["published_time_inferred"] = True
        material["knowledge_time_provenance_contract_id"] = (
            "macro_publication_clock_provenance_v1_20260816"
        )
    elif str(payload.get("published_utc") or ""):
        # Exact publisher/schedule time is historical provenance. It does not
        # replace numeric_causal_known_utc for prospective collection.
        material["source_native_published_utc"] = str(payload["published_utc"])
        material["knowledge_time_provenance_contract_id"] = (
            "macro_publication_clock_provenance_v1_20260816"
        )
    return material


def causal_standardization(
    connection: sqlite3.Connection,
    *,
    series_id: str,
    current_release_key: str,
    current_known_utc: str,
) -> tuple[int, float | None, float | None]:
    if not series_id:
        return 0, None, None
    values = [
        float(row[0])
        for row in connection.execute(
            """
            SELECT surprise_raw FROM macro_release_latest
            WHERE event_series_id=? AND release_key<>?
              AND causal_known_utc<? AND surprise_raw IS NOT NULL
            ORDER BY causal_known_utc
            """,
            (series_id, current_release_key, current_known_utc),
        )
    ]
    if not values:
        return 0, None, None
    mean = statistics.fmean(values)
    std = statistics.stdev(values) if len(values) >= 2 else None
    return len(values), mean, std


def ingest(
    news_database: Path,
    ledger_database: Path,
    *,
    consensus_jsonl: Path | None = None,
    consensus_archive: Path | None = CONSENSUS_ARCHIVE,
    expected_consensus_contract: Mapping[str, Any] | None = None,
    quote_snapshot: Path | None = None,
    heartbeat: WorkerHeartbeat | None = None,
) -> dict[str, Any]:
    if heartbeat is not None:
        heartbeat.update(phase="opening_databases")
    source = sqlite3.connect(
        f"file:{news_database.resolve().as_posix()}?mode=ro", uri=True, timeout=10.0
    )
    source.execute("PRAGMA query_only=ON")
    ledger = open_ledger(ledger_database)
    consensus_import = ingest_consensus_jsonl(
        consensus_jsonl,
        ledger,
        archive_path=consensus_archive,
        expected_contract=expected_consensus_contract,
    )
    if heartbeat is not None:
        heartbeat.update(phase="querying_structured_candidates")
    inspected = int(source.execute("SELECT COUNT(*) FROM articles").fetchone()[0])
    try:
        rows = source.execute(
            """
            SELECT event_id,first_seen_utc,payload_json
            FROM articles
            WHERE CASE WHEN json_valid(payload_json) THEN (
                COALESCE(json_extract(payload_json,'$.structured_event'),0) <> 0
                OR NULLIF(json_extract(payload_json,'$.actual_value'),'') IS NOT NULL
                OR NULLIF(json_extract(payload_json,'$.consensus_value'),'') IS NOT NULL
                OR NULLIF(json_extract(payload_json,'$.previous_value'),'') IS NOT NULL
            ) ELSE 0 END
            ORDER BY first_seen_utc,event_id
            """
        ).fetchall()
    except sqlite3.OperationalError:
        # Compatibility fallback for SQLite builds without JSON1.  Production
        # uses JSON1; the Python predicate below remains authoritative.
        rows = source.execute(
            "SELECT event_id,first_seen_utc,payload_json FROM articles "
            "ORDER BY first_seen_utc,event_id"
        ).fetchall()
    eligible = inserted = 0
    if heartbeat is not None:
        heartbeat.mark_progress(
            phase="processing_structured_candidates",
            source_articles=inspected,
            candidate_articles=len(rows),
            candidate_articles_processed=0,
        )
    for row_number, (event_id, first_seen, payload_text) in enumerate(rows, 1):
        if heartbeat is not None and row_number % 100 == 0:
            heartbeat.mark_progress(
                phase="processing_structured_candidates",
                source_articles=inspected,
                candidate_articles=len(rows),
                candidate_articles_processed=row_number,
            )
        try:
            payload = json.loads(payload_text)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        has_numeric_release = any(
            payload.get(key) not in (None, "")
            for key in ("actual_value", "consensus_value", "previous_value")
        )
        if not bool(payload.get("structured_event")) and not has_numeric_release:
            continue
        eligible += 1
        key = release_key(str(event_id), payload)
        known_utc = str(
            payload.get("numeric_causal_known_utc")
            or payload.get("causal_known_utc")
            or payload.get("first_seen_utc")
            or first_seen
            or ""
        )
        series_id = str(payload.get("event_series_id") or payload.get("event_name") or "")
        actual = finite_or_none(payload.get("actual_value"))
        # Inline consensus on a release page is retained as a diagnostic value,
        # but source verification alone does not prove it was known before the
        # release.  Only the separately clocked immutable consensus ledger can
        # certify causal availability and override an inline value.
        consensus = finite_or_none(payload.get("consensus_value"))
        consensus_text = str(payload.get("consensus") or "")
        consensus_verified = False
        imported_consensus, imported_text, imported_verified = consensus_for_release(
            ledger,
            series_id=series_id,
            scheduled_utc=str(payload.get("scheduled_utc") or ""),
            release_key_value=key,
            expected_contract=expected_consensus_contract,
        )
        if imported_consensus is not None:
            consensus = imported_consensus
            consensus_text = imported_text
            consensus_verified = imported_verified
        material = material_payload(str(event_id), payload)
        material["effective_consensus"] = consensus_text
        material["effective_consensus_value"] = consensus
        material["consensus_causally_verified"] = bool(
            consensus is not None and consensus_verified
        )
        payload_hash = stable_hash(material)
        revision_id = "macro_rev_" + payload_hash[:40]
        surprise = (
            actual - consensus if actual is not None and consensus is not None else None
        )
        prior_n, prior_mean, prior_std = causal_standardization(
            ledger,
            series_id=series_id,
            current_release_key=key,
            current_known_utc=known_utc,
        )
        standardized = (
            (surprise - prior_mean) / prior_std
            if surprise is not None
            and consensus_verified
            and prior_mean is not None
            and prior_std is not None
            and prior_std > 0.0
            else None
        )
        cursor = ledger.execute(
            """
            INSERT OR IGNORE INTO macro_release_revisions (
                revision_id,release_key,source_event_id,recorded_utc,
                causal_known_utc,scheduled_utc,source_reported_update_utc,
                event_series_id,event_name,event_country,currencies_json,
                reference_period,reference_date,importance,unit,
                actual_text,actual_value,consensus_text,consensus_value,
                previous_text,previous_value,revised_previous_text,
                revised_previous_value,surprise_raw,standardized_surprise,
                standardization_prior_n,standardization_prior_mean,
                standardization_prior_std,directional_interpretation,
                release_importance,first_market_reaction_pips,
                subsequent_reaction_pips,reaction_class,reaction_status,
                known_before_recorded_timestamp,source_id,source_name,
                source_url,source_verified,source_direct,payload_sha256,payload_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                revision_id,
                key,
                str(event_id),
                utc_iso(),
                known_utc,
                str(payload.get("scheduled_utc") or ""),
                str(payload.get("source_reported_update_utc") or ""),
                series_id,
                str(payload.get("event_name") or ""),
                str(payload.get("event_country") or ""),
                json.dumps(payload.get("currencies") or [], sort_keys=True),
                str(payload.get("reference_period") or ""),
                str(payload.get("reference_date") or ""),
                str(payload.get("importance") or ""),
                str(payload.get("unit") or ""),
                str(payload.get("actual") or ""),
                actual,
                consensus_text,
                consensus,
                str(payload.get("previous") or ""),
                finite_or_none(payload.get("previous_value")),
                str(payload.get("revised_previous") or ""),
                finite_or_none(payload.get("revised_previous_value")),
                surprise,
                standardized,
                prior_n,
                prior_mean,
                prior_std,
                str(
                    payload.get("directional_surprise_interpretation")
                    or ("pending_series_semantics" if surprise is not None else "unavailable")
                ),
                str(payload.get("importance") or "unknown"),
                None,
                None,
                "unobserved",
                "pending_executable_quote_join",
                int(consensus is not None and consensus_verified),
                str(payload.get("source_id") or ""),
                str(payload.get("source_name") or ""),
                str(payload.get("source_url") or ""),
                int(bool(payload.get("source_verified"))),
                int(bool(payload.get("source_direct"))),
                payload_hash,
                json.dumps(material, sort_keys=True),
            ),
        )
        inserted += int(cursor.rowcount > 0)
    ledger.commit()
    reaction_import = ingest_reaction_samples(ledger, quote_snapshot)
    latest = ledger.execute(
        """
        SELECT COUNT(*),
               SUM(actual_value IS NOT NULL),
               SUM(consensus_value IS NOT NULL),
               SUM(actual_value IS NOT NULL AND consensus_value IS NOT NULL),
               SUM(standardized_surprise IS NOT NULL),
               SUM(scheduled_utc<>''),
               SUM(actual_value IS NOT NULL AND consensus_value IS NOT NULL
                   AND known_before_recorded_timestamp=1)
        FROM macro_release_latest
        """
    ).fetchone()
    revisions = int(ledger.execute("SELECT COUNT(*) FROM macro_release_revisions").fetchone()[0])
    consensus_observations, valid_consensus_observations = ledger.execute(
        "SELECT COUNT(*),SUM(causal_valid) FROM macro_consensus_observations"
    ).fetchone()
    reaction_samples = int(ledger.execute("SELECT COUNT(*) FROM macro_reaction_samples").fetchone()[0])
    source.close()
    ledger.close()
    return {
        "schema_version": 1,
        "ingest_contract_id": INGEST_CONTRACT_ID,
        "generated_utc": utc_iso(),
        "research_only": True,
        "can_place_orders": False,
        "source_database": str(news_database.resolve()),
        "ledger_database": str(ledger_database.resolve()),
        "articles_inspected": inspected,
        "candidate_articles_loaded": len(rows),
        "structured_articles_seen": eligible,
        "new_revisions": inserted,
        "total_revisions": revisions,
        "release_count": int(latest[0] or 0),
        "actual_count": int(latest[1] or 0),
        "consensus_count": int(latest[2] or 0),
        "actual_and_consensus_count": int(latest[3] or 0),
        "causal_actual_and_consensus_count": int(latest[6] or 0),
        "noncausal_actual_and_consensus_count": max(
            0, int(latest[3] or 0) - int(latest[6] or 0)
        ),
        "standardized_surprise_count": int(latest[4] or 0),
        "scheduled_release_count": int(latest[5] or 0),
        "consensus_observation_count": int(consensus_observations or 0),
        "causal_consensus_observation_count": int(valid_consensus_observations or 0),
        "consensus_import": consensus_import,
        "reaction_sample_count": reaction_samples,
        "reaction_import": reaction_import,
        "status": (
            "ready"
            if int(latest[6] or 0)
            else "causal_consensus_source_unavailable"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news-database", type=Path, default=NEWS_DB)
    parser.add_argument("--ledger-database", type=Path, default=STATE / "macro_surprise_v1.sqlite")
    parser.add_argument("--state", type=Path, default=STATE / "macro_surprise_v1.json")
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT)
    parser.add_argument("--consensus-jsonl", type=Path)
    parser.add_argument("--consensus-archive", type=Path, default=CONSENSUS_ARCHIVE)
    parser.add_argument(
        "--quote-snapshot", type=Path, default=STATE / "practice_007_market_quotes_v1.json"
    )
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    stop_at = time.monotonic() + args.duration_sec if args.duration_sec > 0 else None
    with WorkerHeartbeat(
        args.heartbeat,
        worker="macro_surprise_ledger",
        role="research_ledger",
        interval_sec=5.0,
    ) as heartbeat:
        cycles_completed = 0
        while True:
            heartbeat.update(phase="ingesting", cycles_completed=cycles_completed)
            result = ingest(
                args.news_database,
                args.ledger_database,
                consensus_jsonl=args.consensus_jsonl,
                consensus_archive=args.consensus_archive,
                quote_snapshot=args.quote_snapshot,
                heartbeat=heartbeat,
            )
            atomic_json(args.state, result)
            cycles_completed += 1
            heartbeat.mark_progress(
                phase="sleeping",
                cycles_completed=cycles_completed,
                candidate_articles=result.get("candidate_articles_loaded"),
                structured_articles=result.get("structured_articles_seen"),
            )
            if stop_at is None or time.monotonic() >= stop_at:
                print(json.dumps(result, sort_keys=True))
                return 0
            time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
