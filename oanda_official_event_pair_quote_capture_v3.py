#!/usr/bin/env python3
"""Prospective dual-ledger official-event quotes with an honest read clock.

V2 proved that the dual-ledger identity path works, but its first untouched
event exposed that it passed the worker-cycle start into the quote builder.
The continuously refreshed quote snapshot could therefore be generated after
the timestamp labelled ``captured_utc``.  V3 is a zero-import cohort.  It
retains V2 as immutable evidence and records the wall clock immediately after
the quote snapshot has been read as the actual capture clock.

This wrapper reuses the frozen V2 identity/storage implementation under pinned
hashes.  It has no semantic, lifecycle, authorization, allocation, execution,
or real-money capability.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import datetime as dt
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Iterator, Mapping, Sequence

import oanda_official_event_pair_quote_capture_v2 as v2


ROOT = Path(__file__).resolve().parent
LOCAL_NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG_PATH = ROOT / "config" / "official_event_pair_quote_capture_v3.json"
FAST_DATABASE = LOCAL_NEWS / "official_release_fast_lane_v4.sqlite"
MAIN_DATABASE = LOCAL_NEWS / "local_news_sentiment_v1.sqlite"
QUOTE_PATH = STATE / "practice_007_market_quotes_v1.json"
OUTPUT_DATABASE = LOCAL_NEWS / "official_event_pair_quote_capture_v3.sqlite"
SNAPSHOT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_latest_v3.json"
HEARTBEAT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_heartbeat_v3.json"

SCHEMA_VERSION = "official_event_pair_quote_capture_v3"
CONTRACT_ID = "official_event_pair_quote_capture_v3_quote_read_clock_20260904"
COHORT_ID = "official_event_pair_quote_capture_v3_20260904a"
ACTIVATED_UTC = dt.datetime(2026, 9, 4, 16, 20, tzinfo=dt.timezone.utc)
REQUIRED_V2_PRODUCER_SHA256 = (
    "cf784eee8f5ede455c18a4d8886bd02867dc175cdb4b550e85eea6c6386bb3fb"
)
REQUIRED_V1_PRODUCER_SHA256 = v2.REQUIRED_V1_PRODUCER_SHA256
POLICY = {
    **v2.POLICY,
    "capture_clock_source": "quote_snapshot_read_observed_utc",
    "cycle_start_is_capture_clock": False,
    "capture_clock_may_precede_quote_snapshot_generated": False,
}

_BASE_RUN_CYCLE = v2.run_cycle
_BASE_BUILD_CAPTURE = v2.build_capture


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime | None = None) -> str:
    return v2.iso_utc(value)


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    payload = v2.read_json(path)
    expected = {
        "schema_version": 3,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "required_v2_producer_sha256": REQUIRED_V2_PRODUCER_SHA256,
        "required_v1_producer_sha256": REQUIRED_V1_PRODUCER_SHA256,
        "required_source_map_sha256": v2.REQUIRED_SOURCE_MAP_SHA256,
        "required_source_map_contract_id": v2.REQUIRED_SOURCE_MAP_CONTRACT_ID,
        "required_fast_collector_contract_id": v2.REQUIRED_FAST_COLLECTOR_CONTRACT_ID,
        "required_fast_collector_cohort_id": v2.REQUIRED_FAST_COLLECTOR_COHORT_ID,
        "required_main_collector_contract_id": v2.REQUIRED_MAIN_COLLECTOR_CONTRACT_ID,
        "required_main_collector_cohort_id": v2.REQUIRED_MAIN_COLLECTOR_COHORT_ID,
        "expected_source_count": 42,
        "expected_instrument_count": v2.EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": v2.EXPECTED_UNIVERSE_SHA256,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ValueError(f"V3 dual-ledger capture config mismatch:{key}")
    if _file_sha256(ROOT / "oanda_official_event_pair_quote_capture_v2.py") != REQUIRED_V2_PRODUCER_SHA256:
        raise ValueError("pinned V2 identity/storage producer hash mismatch; start a new cohort")
    if _file_sha256(ROOT / "oanda_official_event_pair_quote_capture_v1.py") != REQUIRED_V1_PRODUCER_SHA256:
        raise ValueError("pinned V1 quote builder hash mismatch; start a new cohort")
    if list(payload.get("source_ids") or []) != list(v2.source_ids_from_frozen_map()):
        raise ValueError("V3 dual-ledger capture source universe mismatch")
    configured_policy = payload.get("policy")
    configured_policy = configured_policy if isinstance(configured_policy, Mapping) else {}
    for key, value in POLICY.items():
        if configured_policy.get(key) != value:
            raise ValueError(f"V3 dual-ledger capture config mismatch:policy.{key}")
    return payload


def build_capture(
    alias: Mapping[str, Any],
    quote_payload: Mapping[str, Any],
    captured_utc: dt.datetime,
) -> dict[str, Any]:
    """Build a V3 capture at the instant the quote payload was actually read."""
    captured = captured_utc.astimezone(dt.timezone.utc)
    result = _BASE_BUILD_CAPTURE(alias, quote_payload, captured)
    result.update(
        {
            "schema_version": SCHEMA_VERSION,
            "capture_id": v2.sha256_text(
                f"{alias['canonical_event_id']}|{CONTRACT_ID}|{COHORT_ID}"
            ),
            "contract_id": CONTRACT_ID,
            "cohort_id": COHORT_ID,
            "activated_utc": iso_utc(ACTIVATED_UTC),
            "capture_clock_source": "quote_snapshot_read_observed_utc",
            "quote_snapshot_read_observed_utc": iso_utc(captured),
        }
    )
    generated = v2.parse_time(result.get("quote_snapshot_generated_utc"))
    clock_precedes_snapshot = bool(generated is not None and captured < generated)
    result["capture_clock_precedes_quote_snapshot_generated"] = clock_precedes_snapshot
    if clock_precedes_snapshot:
        reasons = list(result.get("metadata_invalid_reasons") or [])
        reasons.append("capture_clock_precedes_quote_snapshot_generated")
        result["metadata_invalid_reasons"] = sorted(set(reasons))
        result["eligible_quote_count"] = 0
        result["eligible_instruments"] = []
        result["eligible_currencies"] = []
        result["eligible_currency_count"] = 0
        result["currency_graph_components"] = []
        result["eligible_currency_graph_connected"] = False
        result["eligible_quotes"] = {}
        result["exact_all_68_available"] = False
        result["timing_quality"] = "invalid_capture_clock_precedes_quote_snapshot"
        for row in (result.get("pair_rows") or {}).values():
            if isinstance(row, dict):
                row["eligible"] = False
    return result


def _build_at_quote_read(
    alias: Mapping[str, Any],
    quote_payload: Mapping[str, Any],
    _cycle_started_utc: dt.datetime,
) -> dict[str, Any]:
    # The base runner calls this only after load_quote_snapshot has returned.
    return build_capture(alias, quote_payload, utc_now())


@contextmanager
def _configured_base() -> Iterator[None]:
    replacements = {
        "CONFIG_PATH": CONFIG_PATH,
        "OUTPUT_DATABASE": OUTPUT_DATABASE,
        "SNAPSHOT_PATH": SNAPSHOT_PATH,
        "HEARTBEAT_PATH": HEARTBEAT_PATH,
        "SCHEMA_VERSION": SCHEMA_VERSION,
        "CONTRACT_ID": CONTRACT_ID,
        "COHORT_ID": COHORT_ID,
        "ACTIVATED_UTC": ACTIVATED_UTC,
        "POLICY": POLICY,
        "validate_config": validate_config,
        "build_capture": _build_at_quote_read,
    }
    previous = {key: getattr(v2, key) for key in replacements}
    try:
        for key, value in replacements.items():
            setattr(v2, key, value)
        yield
    finally:
        for key, value in previous.items():
            setattr(v2, key, value)


def run_cycle(
    *,
    fast_database: Path = FAST_DATABASE,
    main_database: Path = MAIN_DATABASE,
    quote_path: Path = QUOTE_PATH,
    output_database: Path = OUTPUT_DATABASE,
    snapshot_path: Path = SNAPSHOT_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
    observed_utc: dt.datetime | None = None,
) -> dict[str, Any]:
    with _configured_base():
        result = _BASE_RUN_CYCLE(
            fast_database=fast_database,
            main_database=main_database,
            quote_path=quote_path,
            output_database=output_database,
            snapshot_path=snapshot_path,
            heartbeat_path=heartbeat_path,
            observed_utc=observed_utc,
        )
    # Publish after the base transaction is complete.  This timestamp is never
    # reused as the quote capture clock.
    result["generated_utc"] = iso_utc(utc_now())
    v2.write_json_atomic(snapshot_path, result)
    v2.write_json_atomic(
        heartbeat_path,
        {
            "schema_version": 1,
            "worker": "oanda_official_event_pair_quote_capture_v3",
            "status": "running" if result.get("status") == "ok" else "error",
            "updated_at": result["generated_utc"],
            "phase": "polling_dual_raw_official_ledgers_quote_read_clock",
            "details": {
                "contract_id": CONTRACT_ID,
                "cohort_id": COHORT_ID,
                **dict(result.get("counts") or {}),
                "last_error": str(result.get("error") or ""),
                "research_only": True,
                "execution_eligible": False,
            },
        },
    )
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fast-database", type=Path, default=FAST_DATABASE)
    parser.add_argument("--main-database", type=Path, default=MAIN_DATABASE)
    parser.add_argument("--quote-path", type=Path, default=QUOTE_PATH)
    parser.add_argument("--output-database", type=Path, default=OUTPUT_DATABASE)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT_PATH)
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    while True:
        result = run_cycle(
            fast_database=args.fast_database,
            main_database=args.main_database,
            quote_path=args.quote_path,
            output_database=args.output_database,
            snapshot_path=args.snapshot,
            heartbeat_path=args.heartbeat,
        )
        if not args.quiet:
            print(json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)
        if args.once or (
            args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec
        ):
            return 0
        time.sleep(max(0.25, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())

