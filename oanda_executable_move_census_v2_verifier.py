#!/usr/bin/env python3
"""Independent verifier binding for separated prospective cohort C.

The verifier imports only the prior independent verification implementation,
never the producer.  All cohort, clock, contract, and source hashes are frozen
here as literals after the V2 producer and configuration were finalized.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import datetime as dt
from pathlib import Path
import time
from typing import Any, Iterator, Mapping

import oanda_executable_move_census_v1_verifier as base


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG_PATH = ROOT / "config" / "executable_move_census_v2_20260901c.json"
DATABASE_PATH = STATE / "executable_move_census_v2_20260901c.sqlite"
LATEST_PATH = STATE / "executable_move_census_latest_v2_20260901c.json"
PRODUCER_PATH = ROOT / "oanda_executable_move_census_v2.py"
SOURCE_PRODUCER_PATH = ROOT / "oanda_practice_top_signal_executor.py"
QUOTE_TRANSPORT_PATH = ROOT / "oanda_quote_transport.py"
OUTPUT_PATH = STATE / "executable_move_census_verifier_latest_v2_20260901c.json"
CHECKPOINT_PATH = STATE / "executable_move_census_verifier_checkpoint_v2_20260901c.json"

COHORT_ID = "all68_executable_move_census_v2_20260901c"
ACTIVATION_UTC = dt.datetime(2026, 9, 1, 2, 30, tzinfo=base.UTC)
CAPTURE_CONTRACT_ID = "all68_exact_snapshot_capture_v2_cold_work_separated"
CAPTURE_COHORT_ID = "all68_exact_snapshot_capture_20260901c"
FROZEN_CONFIG_SHA256 = (
    "a32dec1034bccde2c48d453a2a1963b4557f40717de8c2d6803f1f124f2d67c0"
)
FROZEN_PRODUCER_SHA256 = (
    "fdb4802a95e39f96140be210192fd98eb2a05a33b429dfaf46f27a9ce21d2ff8"
)
FROZEN_SOURCE_PRODUCER_SHA256 = (
    "a7d14b1f7ea8e1490cb83262afe7473e5b56a45808503c186f379208e92befed"
)
FROZEN_QUOTE_TRANSPORT_SHA256 = (
    "27ea11bc79a3a85cc09f00a88df697f0ecf8bba29b230c309cdf83e8967d8987"
)
_BASE_EXPECTED_CONFIG = base.expected_config


def expected_config() -> dict[str, Any]:
    payload = _BASE_EXPECTED_CONFIG()
    payload.update({
        "parent_cohort_id": "all68_executable_move_census_v1_20260830b",
        "parent_evidence_status": "permanently_invalid",
        "supersedes_reason": (
            "minute_capture_separated_from_cold_evaluation_after_verified_"
            "schedule_and_clock_failures"
        ),
        "historical_row_import_count": 0,
    })
    return payload


@contextmanager
def cohort_c_contract() -> Iterator[None]:
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
    previous = {name: getattr(base, name) for name in replacements}
    try:
        for name, value in replacements.items():
            setattr(base, name, value)
        yield
    finally:
        for name, value in previous.items():
            setattr(base, name, value)


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
    with cohort_c_contract():
        return base.verify(
            database_path=database_path,
            config_path=config_path,
            latest_path=latest_path,
            producer_path=producer_path,
            source_producer_path=source_producer_path,
            quote_transport_path=quote_transport_path,
            now=now,
            runtime_cache=runtime_cache,
        )


def latest_refresh_race_only(payload: Mapping[str, Any]) -> bool:
    failures = set(payload.get("failures") or [])
    return bool(failures) and failures.issubset({
        "evaluations:scheduled_key_coverage",
        "latest:frame_count:mismatch",
        "latest:latest_frame_utc:mismatch",
        "latest:schedule_census:mismatch",
        "latest:horizons:mismatch",
        "latest:top_cleared_paths:mismatch",
        "latest:review_queue:mismatch",
        "factor_finalizations:episode_coverage",
        "factor_summaries:exact_row_set",
    })


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
    with cohort_c_contract():
        runtime_cache = base.load_runtime_checkpoint(args.checkpoint)
    last_checkpoint_elapsed: float | None = None
    payload: dict[str, Any] = {}
    while True:
        # Capture commits at absolute minute +1 second. The isolated evaluator
        # may still be refreshing the latest dashboard when this process wakes.
        # Retry only that exact two-field freshness race; never retry away a
        # clock, schedule, hash, lineage, or row-integrity failure.
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
        base.write_json_atomic(args.output, payload)
        elapsed = time.monotonic() - started
        finishing = args.duration_sec <= 0 or elapsed >= args.duration_sec
        checkpoint_due = (
            last_checkpoint_elapsed is None
            or elapsed - last_checkpoint_elapsed
            >= max(60.0, args.checkpoint_interval_sec)
            or finishing
        )
        if payload.get("verified") and checkpoint_due:
            with cohort_c_contract():
                base.write_runtime_checkpoint(args.checkpoint, runtime_cache)
            last_checkpoint_elapsed = elapsed
        if finishing:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0 if payload.get("verified") else 1


if __name__ == "__main__":
    raise SystemExit(main())
