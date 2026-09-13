#!/usr/bin/env python3
"""Independent verifier binding for prospective executable-move Cohort D.

The verifier imports only the independent V1 verification implementation,
never the producer. Cohort, clock, contract, and source hashes are frozen as
literals after the Cohort D producer and configuration were finalized.
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
CONFIG_PATH = ROOT / "config" / "executable_move_census_v2_20260901d.json"
DATABASE_PATH = STATE / "executable_move_census_v2_20260901d.sqlite"
LATEST_PATH = STATE / "executable_move_census_latest_v2_20260901d.json"
PRODUCER_PATH = ROOT / "oanda_executable_move_census_v2.py"
SOURCE_PRODUCER_PATH = ROOT / "oanda_practice_top_signal_executor.py"
QUOTE_TRANSPORT_PATH = ROOT / "oanda_quote_transport.py"
OUTPUT_PATH = STATE / "executable_move_census_verifier_latest_v2_20260901d.json"
CHECKPOINT_PATH = STATE / "executable_move_census_verifier_checkpoint_v2_20260901d.json"

COHORT_ID = "all68_executable_move_census_v2_20260901d"
ACTIVATION_UTC = dt.datetime(2026, 9, 1, 18, 30, tzinfo=base.UTC)
CAPTURE_CONTRACT_ID = "all68_exact_snapshot_capture_v2_cold_work_separated"
CAPTURE_COHORT_ID = "all68_exact_snapshot_capture_20260901d"
FROZEN_CONFIG_SHA256 = (
    "91be9adc330b6743c556c78a89853b252276f791aecab0c429c9e8671103ba1a"
)
FROZEN_PRODUCER_SHA256 = (
    "543e5d13e42fe1e7908883e8f208454324c9d80b0d3d03417e6e7317a16b8324"
)
FROZEN_SOURCE_PRODUCER_SHA256 = (
    "b98151ab60e2e0e3253d0cfaef1229bb1e2a4bf777a1365aa579be4f1bd294f6"
)
FROZEN_QUOTE_TRANSPORT_SHA256 = (
    "27ea11bc79a3a85cc09f00a88df697f0ecf8bba29b230c309cdf83e8967d8987"
)
_BASE_EXPECTED_CONFIG = base.expected_config


def expected_config() -> dict[str, Any]:
    payload = _BASE_EXPECTED_CONFIG()
    payload.update({
        "parent_cohort_id": "all68_executable_move_census_v2_20260901c",
        "parent_evidence_status": "superseded_collecting",
        "supersedes_reason": (
            "upstream_quote_source_producer_hash_changed_after_"
            "tradeability_contract_fix"
        ),
        "historical_row_import_count": 0,
    })
    return payload


@contextmanager
def cohort_d_contract() -> Iterator[None]:
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
    with cohort_d_contract():
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
    with cohort_d_contract():
        runtime_cache = base.load_runtime_checkpoint(args.checkpoint)
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
            with cohort_d_contract():
                base.write_runtime_checkpoint(args.checkpoint, runtime_cache)
            last_checkpoint_elapsed = elapsed
        if finishing:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0 if payload.get("verified") else 1


if __name__ == "__main__":
    raise SystemExit(main())
