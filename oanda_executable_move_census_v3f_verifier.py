#!/usr/bin/env python3
"""Independent verifier binding for executable-move Cohort F.

The verifier never imports the producer.  It binds the new prospective cohort
to the retrying, incident-logged V3 producer, quote schema 3, and zero imported
history.  Cohort E remains untouched and excluded after its 20:24 UTC gap.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import datetime as dt
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator, Mapping

import oanda_executable_move_census_v2d_verifier as prior


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG_PATH = ROOT / "config" / "executable_move_census_v3_20260901f.json"
DATABASE_PATH = STATE / "executable_move_census_v3_20260901f.sqlite"
LATEST_PATH = STATE / "executable_move_census_latest_v3_20260901f.json"
PRODUCER_PATH = ROOT / "oanda_executable_move_census_v3.py"
SOURCE_PRODUCER_PATH = ROOT / "oanda_practice_top_signal_executor.py"
QUOTE_TRANSPORT_PATH = ROOT / "oanda_quote_transport.py"
OUTPUT_PATH = STATE / "executable_move_census_verifier_latest_v3_20260901f.json"
CHECKPOINT_PATH = (
    STATE / "executable_move_census_verifier_checkpoint_v3_20260901f.json"
)

COHORT_ID = "all68_executable_move_census_v3_20260901f"
ACTIVATION_UTC = dt.datetime(2026, 9, 1, 21, 0, tzinfo=prior.base.UTC)
CAPTURE_CONTRACT_ID = "all68_exact_snapshot_capture_v3_retry_incident_logged"
CAPTURE_COHORT_ID = "all68_exact_snapshot_capture_20260901f"
SOURCE_SCHEMA_VERSION = "3"
FROZEN_CONFIG_SHA256 = (
    "5e98d3d89882d969025e7c21935e3ad0481ee01613564fd439228ae47a946b77"
)
FROZEN_PRODUCER_SHA256 = (
    "648d35fcfd0d39f51c330be36cf4761f3660d18f3ee5db2b885c0169c3c9c130"
)
FROZEN_SOURCE_PRODUCER_SHA256 = (
    "b98151ab60e2e0e3253d0cfaef1229bb1e2a4bf777a1365aa579be4f1bd294f6"
)
FROZEN_QUOTE_TRANSPORT_SHA256 = (
    "27ea11bc79a3a85cc09f00a88df697f0ecf8bba29b230c309cdf83e8967d8987"
)
# Cohort F's append-only surface now takes several minutes to reconstruct.
# This is an operational freshness budget for the producer projection, not an
# evidence or quote-age relaxation: quote, capture-delay, and market-calendar
# limits remain frozen in the cohort configuration and are rechecked below.
MAXIMUM_LATEST_AGE_SEC = 600.0


def expected_config() -> dict[str, Any]:
    payload = prior._BASE_EXPECTED_CONFIG()
    payload.update({
        "parent_cohort_id": "all68_executable_move_census_v2_20260901e",
        "parent_evidence_status": "permanently_invalid",
        "supersedes_reason": (
            "cohort_e_missed_open_frame_20260901T202400Z_"
            "and_capture_error_was_not_durable"
        ),
        "historical_row_import_count": 0,
    })
    return payload


@contextmanager
def cohort_f_contract() -> Iterator[None]:
    replacements = {
        "CONFIG_PATH": CONFIG_PATH,
        "DATABASE_PATH": DATABASE_PATH,
        "LATEST_PATH": LATEST_PATH,
        "PRODUCER_PATH": PRODUCER_PATH,
        "SOURCE_PRODUCER_PATH": SOURCE_PRODUCER_PATH,
        "QUOTE_TRANSPORT_PATH": QUOTE_TRANSPORT_PATH,
        "OUTPUT_PATH": OUTPUT_PATH,
        "CHECKPOINT_PATH": CHECKPOINT_PATH,
        "COHORT_ID": COHORT_ID,
        "ACTIVATION_UTC": ACTIVATION_UTC,
        "CAPTURE_CONTRACT_ID": CAPTURE_CONTRACT_ID,
        "CAPTURE_COHORT_ID": CAPTURE_COHORT_ID,
        "FROZEN_CONFIG_SHA256": FROZEN_CONFIG_SHA256,
        "FROZEN_PRODUCER_SHA256": FROZEN_PRODUCER_SHA256,
        "FROZEN_SOURCE_PRODUCER_SHA256": FROZEN_SOURCE_PRODUCER_SHA256,
        "FROZEN_QUOTE_TRANSPORT_SHA256": FROZEN_QUOTE_TRANSPORT_SHA256,
        "expected_config": expected_config,
    }
    previous = {name: getattr(prior, name) for name in replacements}
    previous_schema = prior.base.SOURCE_SCHEMA_VERSION
    previous_latest_age = prior.base.MAXIMUM_LATEST_AGE_SEC
    try:
        prior.base.SOURCE_SCHEMA_VERSION = SOURCE_SCHEMA_VERSION
        prior.base.MAXIMUM_LATEST_AGE_SEC = MAXIMUM_LATEST_AGE_SEC
        for name, value in replacements.items():
            setattr(prior, name, value)
        yield
    finally:
        for name, value in previous.items():
            setattr(prior, name, value)
        prior.base.SOURCE_SCHEMA_VERSION = previous_schema
        prior.base.MAXIMUM_LATEST_AGE_SEC = previous_latest_age


def semantic_validity_diagnostics(
    database_path: Path,
    *,
    verified_frame_count: int | None = None,
) -> dict[str, int]:
    """Measure semantics over exactly the verifier's pinned frame prefix.

    The producer appends one frame per minute.  Running this diagnostic on a
    second unbounded connection allowed a newly appended frame to be counted
    after the independent verifier had already frozen its frame total.  Scope
    every query to the same deterministic append-only prefix and pin the read
    transaction so a live suffix cannot rewrite the completed audit.
    """

    diagnostics = {
        "valid_quote_rows": 0,
        "invalid_quote_rows": 0,
        "source_identity_mismatch_rows": 0,
        "zero_valid_open_market_frames": 0,
        "verified_frame_scope": 0,
        "current_database_frames": 0,
    }
    if not database_path.exists():
        return diagnostics
    connection = sqlite3.connect(
        f"file:{database_path.as_posix()}?mode=ro",
        uri=True,
    )
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        current_frames = int(
            connection.execute("SELECT COUNT(*) FROM frames").fetchone()[0]
        )
        scope = current_frames if verified_frame_count is None else max(
            0, min(int(verified_frame_count), current_frames)
        )
        diagnostics["current_database_frames"] = current_frames
        diagnostics["verified_frame_scope"] = scope
        quote_scope = (
            "WITH verified_frames AS ("
            " SELECT frame_id FROM frames"
            " ORDER BY scheduled_utc, frame_id LIMIT ?"
            ") "
        )
        diagnostics["valid_quote_rows"] = int(
            connection.execute(
                quote_scope
                + "SELECT COUNT(*) FROM frame_quotes q "
                "JOIN verified_frames f ON f.frame_id=q.frame_id "
                "WHERE q.state='valid'",
                (scope,),
            ).fetchone()[0]
        )
        diagnostics["invalid_quote_rows"] = int(
            connection.execute(
                quote_scope
                + "SELECT COUNT(*) FROM frame_quotes q "
                "JOIN verified_frames f ON f.frame_id=q.frame_id "
                "WHERE q.state!='valid'",
                (scope,),
            ).fetchone()[0]
        )
        diagnostics["source_identity_mismatch_rows"] = int(
            connection.execute(
                quote_scope
                + "SELECT COUNT(*) FROM frame_quotes q "
                "JOIN verified_frames f ON f.frame_id=q.frame_id "
                "WHERE q.reason='source_identity_mismatch'",
                (scope,),
            ).fetchone()[0]
        )
        diagnostics["zero_valid_open_market_frames"] = int(
            connection.execute(
                "SELECT COUNT(*) FROM ("
                " SELECT market_open,valid_count FROM frames"
                " ORDER BY scheduled_utc,frame_id LIMIT ?"
                ") WHERE market_open=1 AND valid_count=0",
                (scope,),
            ).fetchone()[0]
        )
    finally:
        connection.close()
    return diagnostics


def verify(
    *,
    database_path: Path = DATABASE_PATH,
    config_path: Path = CONFIG_PATH,
    latest_path: Path = LATEST_PATH,
    producer_path: Path = PRODUCER_PATH,
    source_producer_path: Path = SOURCE_PRODUCER_PATH,
    quote_transport_path: Path = QUOTE_TRANSPORT_PATH,
    now: dt.datetime | None = None,
    runtime_cache: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with cohort_f_contract():
        payload = prior.verify(
            database_path=database_path,
            config_path=config_path,
            latest_path=latest_path,
            producer_path=producer_path,
            source_producer_path=source_producer_path,
            quote_transport_path=quote_transport_path,
            now=now,
            runtime_cache=runtime_cache,
        )
    diagnostics = semantic_validity_diagnostics(
        database_path,
        verified_frame_count=int((payload.get("counts") or {}).get("frames") or 0),
    )
    payload["semantic_validity"] = diagnostics
    failures = list(payload.get("failures") or [])
    if diagnostics["source_identity_mismatch_rows"]:
        failures.append("quotes:source_identity_mismatch")
    if diagnostics["zero_valid_open_market_frames"]:
        failures.append("quotes:no_valid_open_market_rows")
    payload["failures"] = sorted(set(failures))
    payload["failure_count"] = len(payload["failures"])
    payload["verified"] = not payload["failures"]
    payload["supported_execution_decision"] = "no_trade"
    return payload


def latest_refresh_race_only(payload: Mapping[str, Any]) -> bool:
    return prior.latest_refresh_race_only(payload)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE_PATH)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--latest", type=Path, default=LATEST_PATH)
    parser.add_argument("--producer", type=Path, default=PRODUCER_PATH)
    parser.add_argument("--source-producer", type=Path, default=SOURCE_PRODUCER_PATH)
    parser.add_argument("--quote-transport", type=Path, default=QUOTE_TRANSPORT_PATH)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--checkpoint", type=Path, default=CHECKPOINT_PATH)
    parser.add_argument("--checkpoint-interval-sec", type=float, default=900.0)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    with cohort_f_contract():
        runtime_cache = prior.base.load_runtime_checkpoint(args.checkpoint)
    last_checkpoint_elapsed: float | None = None
    payload: dict[str, Any] = {}
    while True:
        for attempt in range(6):
            payload = verify(
                database_path=args.database,
                config_path=args.config,
                latest_path=args.latest,
                producer_path=args.producer,
                source_producer_path=args.source_producer,
                quote_transport_path=args.quote_transport,
                runtime_cache=runtime_cache,
            )
            if not latest_refresh_race_only(payload) or attempt == 5:
                break
            time.sleep(5.0)
        prior.base.write_json_atomic(args.output, payload)
        elapsed = time.monotonic() - started
        finishing = args.duration_sec <= 0 or elapsed >= args.duration_sec
        checkpoint_due = (
            last_checkpoint_elapsed is None
            or elapsed - last_checkpoint_elapsed
            >= max(60.0, args.checkpoint_interval_sec)
            or finishing
        )
        if payload.get("verified") and checkpoint_due:
            with cohort_f_contract():
                prior.base.write_runtime_checkpoint(args.checkpoint, runtime_cache)
            last_checkpoint_elapsed = elapsed
        if finishing:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0 if payload.get("verified") else 1


if __name__ == "__main__":
    raise SystemExit(main())
