#!/usr/bin/env python3
"""Independent verifier binding for executable-move Cohort G.

Cohort G begins with zero imported rows after Cohort F permanently missed the
2026-09-02 10:46 UTC open-market frame.  The missing frame is preserved in F;
G additionally binds the quote contract to the executor-owned schema-3 source
after eliminating the strategy-lab writer collision.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import datetime as dt
import json
from pathlib import Path
import time
from typing import Any, Iterator, Mapping

import oanda_executable_move_census_v3f_verifier as previous


base = previous.prior
ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG_PATH = ROOT / "config" / "executable_move_census_v3_20260902g.json"
DATABASE_PATH = STATE / "executable_move_census_v3_20260902g.sqlite"
LATEST_PATH = STATE / "executable_move_census_latest_v3_20260902g.json"
PRODUCER_PATH = ROOT / "oanda_executable_move_census_v3.py"
SOURCE_PRODUCER_PATH = ROOT / "oanda_practice_top_signal_executor.py"
QUOTE_TRANSPORT_PATH = ROOT / "oanda_quote_transport.py"
OUTPUT_PATH = STATE / "executable_move_census_verifier_latest_v3_20260902g.json"
CHECKPOINT_PATH = (
    STATE / "executable_move_census_verifier_checkpoint_v3_20260902g.json"
)

COHORT_ID = "all68_executable_move_census_v3_20260902g"
ACTIVATION_UTC = dt.datetime(2026, 9, 2, 11, 30, tzinfo=base.base.UTC)
CAPTURE_CONTRACT_ID = (
    "all68_exact_snapshot_capture_v3_retry_incident_logged_exclusive_quote_owner"
)
CAPTURE_COHORT_ID = "all68_exact_snapshot_capture_20260902g"
SOURCE_SCHEMA_VERSION = "3"
FROZEN_CONFIG_SHA256 = (
    "24680eec4cb83e43a665afba28583d3614a3e70dcf5137a62d46b5479bf034e0"
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
MAXIMUM_LATEST_AGE_SEC = 600.0


def expected_config() -> dict[str, Any]:
    payload = base._BASE_EXPECTED_CONFIG()
    payload.update({
        "parent_cohort_id": "all68_executable_move_census_v3_20260901f",
        "parent_evidence_status": "permanently_invalid",
        "supersedes_reason": (
            "cohort_f_missed_open_frame_20260902T104600Z_after_"
            "canonical_quote_snapshot_ownership_collision"
        ),
        "historical_row_import_count": 0,
    })
    return payload


@contextmanager
def cohort_g_contract() -> Iterator[None]:
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
    prior_values = {name: getattr(base, name) for name in replacements}
    prior_schema = base.base.SOURCE_SCHEMA_VERSION
    prior_latest_age = base.base.MAXIMUM_LATEST_AGE_SEC
    try:
        base.base.SOURCE_SCHEMA_VERSION = SOURCE_SCHEMA_VERSION
        base.base.MAXIMUM_LATEST_AGE_SEC = MAXIMUM_LATEST_AGE_SEC
        for name, value in replacements.items():
            setattr(base, name, value)
        yield
    finally:
        for name, value in prior_values.items():
            setattr(base, name, value)
        base.base.SOURCE_SCHEMA_VERSION = prior_schema
        base.base.MAXIMUM_LATEST_AGE_SEC = prior_latest_age


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
    with cohort_g_contract():
        payload = base.verify(
            database_path=database_path,
            config_path=config_path,
            latest_path=latest_path,
            producer_path=producer_path,
            source_producer_path=source_producer_path,
            quote_transport_path=quote_transport_path,
            now=now,
            runtime_cache=runtime_cache,
        )
    diagnostics = previous.semantic_validity_diagnostics(
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
    return previous.latest_refresh_race_only(payload)


def checkpoint_pins() -> dict[str, str]:
    """Return the exact Cohort-G binding for its disposable performance cache."""

    return {
        "config_sha256": FROZEN_CONFIG_SHA256,
        "producer_sha256": FROZEN_PRODUCER_SHA256,
        "source_producer_sha256": FROZEN_SOURCE_PRODUCER_SHA256,
        "quote_transport_sha256": FROZEN_QUOTE_TRANSPORT_SHA256,
        "verifier_sha256": base.base.sha(Path(__file__).read_bytes()),
    }


def load_runtime_checkpoint(path: Path) -> dict[str, Any]:
    """Load only a self-hashed checkpoint bound to this verifier and cohort."""

    try:
        payload = json.loads(path.read_bytes())
        content_hash = payload.pop("content_sha256_excluding_this_field")
        if content_hash != base.base.sha(base.base.canonical(payload)):
            return {}
        if payload.get("schema_version") != (
            "executable_move_census_verifier_checkpoint_v1"
        ):
            return {}
        if payload.get("cohort_id") != COHORT_ID:
            return {}
        if payload.get("pins") != checkpoint_pins():
            return {}
        evaluations = {
            (str(row[0]), int(row[1])): str(row[2])
            for row in payload.get("evaluation_rows", [])
            if isinstance(row, list) and len(row) == 3
        }
        episodes = {
            str(key): str(value)
            for key, value in dict(payload.get("factor_episodes") or {}).items()
        }
        return {
            "database_path": payload.get("database_path"),
            "frame_count": int(payload.get("frame_count") or 0),
            "frame_chain_sha256": payload.get("frame_chain_sha256"),
            "last_generated_utc": payload.get("last_generated_utc"),
            "evaluation_rows": evaluations,
            "factor_episodes": episodes,
        }
    except (OSError, ValueError, TypeError, KeyError):
        return {}


def write_runtime_checkpoint(
    path: Path,
    runtime_cache: Mapping[str, Any],
) -> None:
    """Persist the cache with Cohort-G identity rather than inherited globals."""

    payload = {
        "schema_version": "executable_move_census_verifier_checkpoint_v1",
        "cohort_id": COHORT_ID,
        "pins": checkpoint_pins(),
        "database_path": runtime_cache.get("database_path"),
        "frame_count": runtime_cache.get("frame_count", 0),
        "frame_chain_sha256": runtime_cache.get("frame_chain_sha256"),
        "last_generated_utc": runtime_cache.get("last_generated_utc"),
        "evaluation_rows": [
            [key[0], key[1], value]
            for key, value in sorted(
                dict(runtime_cache.get("evaluation_rows") or {}).items()
            )
        ],
        "factor_episodes": dict(runtime_cache.get("factor_episodes") or {}),
    }
    payload["content_sha256_excluding_this_field"] = base.base.sha(
        base.base.canonical(payload)
    )
    base.base.write_json_atomic(path, payload)


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
    runtime_cache = load_runtime_checkpoint(args.checkpoint)
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
        base.base.write_json_atomic(args.output, payload)
        elapsed = time.monotonic() - started
        finishing = args.duration_sec <= 0 or elapsed >= args.duration_sec
        checkpoint_due = (
            last_checkpoint_elapsed is None
            or elapsed - last_checkpoint_elapsed >= max(60.0, args.checkpoint_interval_sec)
            or finishing
        )
        if payload.get("verified") and checkpoint_due:
            write_runtime_checkpoint(args.checkpoint, runtime_cache)
            last_checkpoint_elapsed = elapsed
        if finishing:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0 if payload.get("verified") else 1


if __name__ == "__main__":
    raise SystemExit(main())
