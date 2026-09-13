#!/usr/bin/env python3
"""Independent verifier for the V3 official-event quote-read clock cohort.

The verifier imports no producer or broker module.  It reuses the frozen V2
verifier's raw-ledger and append-only reconstruction, then independently proves
that every V3 capture clock is the recorded quote-read clock and never precedes
the quote snapshot or any admitted pair quote.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import datetime as dt
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator, Mapping, Sequence

import oanda_official_event_pair_quote_capture_v2_verifier as v2_verifier


ROOT = Path(__file__).resolve().parent
LOCAL_NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
FAST_DATABASE = LOCAL_NEWS / "official_release_fast_lane_v4.sqlite"
MAIN_DATABASE = LOCAL_NEWS / "local_news_sentiment_v1.sqlite"
CAPTURE_DATABASE = LOCAL_NEWS / "official_event_pair_quote_capture_v3.sqlite"
CAPTURE_STATE = LOCAL_NEWS / "official_event_pair_quote_capture_latest_v3.json"
OUTPUT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_verifier_latest_v3.json"
HEARTBEAT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_verifier_heartbeat_v3.json"

SCHEMA_VERSION = "official_event_pair_quote_capture_v3_verifier"
VERIFIER_CONTRACT_ID = "official_event_pair_quote_capture_v3_read_clock_independent_verifier_20260904"
CAPTURE_SCHEMA_VERSION = "official_event_pair_quote_capture_v3"
CAPTURE_CONTRACT_ID = "official_event_pair_quote_capture_v3_quote_read_clock_20260904"
CAPTURE_COHORT_ID = "official_event_pair_quote_capture_v3_20260904a"
ACTIVATED_UTC = dt.datetime(2026, 9, 4, 16, 20, tzinfo=dt.timezone.utc)

_BASE_VERIFY = v2_verifier.verify


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime | None = None) -> str:
    return v2_verifier.iso_utc(value)


def parse_time(value: Any) -> dt.datetime | None:
    return v2_verifier.parse_time(value)


@contextmanager
def _configured_base() -> Iterator[None]:
    replacements = {
        "CAPTURE_DATABASE": CAPTURE_DATABASE,
        "CAPTURE_STATE": CAPTURE_STATE,
        "OUTPUT_PATH": OUTPUT_PATH,
        "HEARTBEAT_PATH": HEARTBEAT_PATH,
        "SCHEMA_VERSION": SCHEMA_VERSION,
        "VERIFIER_CONTRACT_ID": VERIFIER_CONTRACT_ID,
        "CAPTURE_SCHEMA_VERSION": CAPTURE_SCHEMA_VERSION,
        "CAPTURE_CONTRACT_ID": CAPTURE_CONTRACT_ID,
        "CAPTURE_COHORT_ID": CAPTURE_COHORT_ID,
        "ACTIVATED_UTC": ACTIVATED_UTC,
    }
    previous = {key: getattr(v2_verifier, key) for key in replacements}
    try:
        for key, value in replacements.items():
            setattr(v2_verifier, key, value)
        v2_verifier._PREACTIVATION_CACHE.clear()
        yield
    finally:
        for key, value in previous.items():
            setattr(v2_verifier, key, value)
        v2_verifier._PREACTIVATION_CACHE.clear()


def _read_clock_failures(capture_database: Path) -> list[str]:
    failures: list[str] = []
    try:
        connection = sqlite3.connect(f"file:{capture_database.as_posix()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT capture_id,captured_utc,actionable_event_utc,capture_payload_json "
            "FROM official_event_pair_quote_capture ORDER BY captured_utc,capture_id"
        ).fetchall()
        connection.close()
    except (OSError, sqlite3.Error) as exc:
        return [f"V3_read_clock_database_error:{type(exc).__name__}:{exc}"]
    for row in rows:
        prefix = f"capture:{row['capture_id']}"
        try:
            payload = json.loads(str(row["capture_payload_json"]))
        except (TypeError, ValueError):
            failures.append(f"{prefix}:V3_invalid_payload")
            continue
        captured = parse_time(row["captured_utc"])
        actionable = parse_time(row["actionable_event_utc"])
        snapshot_generated = parse_time(payload.get("quote_snapshot_generated_utc"))
        read_observed = parse_time(payload.get("quote_snapshot_read_observed_utc"))
        if payload.get("capture_clock_source") != "quote_snapshot_read_observed_utc":
            failures.append(f"{prefix}:V3_capture_clock_source_mismatch")
        if captured is None or read_observed != captured:
            failures.append(f"{prefix}:V3_read_observed_clock_mismatch")
        if captured is None or snapshot_generated is None or captured < snapshot_generated:
            failures.append(f"{prefix}:V3_capture_precedes_snapshot_generation")
        if bool(payload.get("capture_clock_precedes_quote_snapshot_generated")):
            failures.append(f"{prefix}:V3_capture_precedes_snapshot_flag")
        if captured is not None and snapshot_generated is not None:
            expected_age = (captured - snapshot_generated).total_seconds()
            observed_age = payload.get("quote_snapshot_age_seconds")
            if not isinstance(observed_age, (int, float)) or not math.isclose(
                float(observed_age), expected_age, abs_tol=1e-6
            ):
                failures.append(f"{prefix}:V3_snapshot_age_mismatch")
        pair_rows = payload.get("pair_rows")
        pair_rows = pair_rows if isinstance(pair_rows, Mapping) else {}
        for instrument, pair in pair_rows.items():
            pair = pair if isinstance(pair, Mapping) else {}
            quote_time = parse_time(pair.get("quote_time_utc"))
            if captured is None or quote_time is None:
                continue
            expected_quote_age = (captured - quote_time).total_seconds()
            observed_quote_age = pair.get("quote_age_seconds")
            if not isinstance(observed_quote_age, (int, float)) or not math.isclose(
                float(observed_quote_age), expected_quote_age, abs_tol=1e-6
            ):
                failures.append(f"{prefix}:{instrument}:V3_quote_age_mismatch")
            if actionable is not None:
                expected_offset = (quote_time - actionable).total_seconds()
                observed_offset = pair.get("event_offset_seconds")
                if not isinstance(observed_offset, (int, float)) or not math.isclose(
                    float(observed_offset), expected_offset, abs_tol=1e-6
                ):
                    failures.append(f"{prefix}:{instrument}:V3_event_offset_mismatch")
    return failures


def verify(
    *,
    fast_database: Path = FAST_DATABASE,
    main_database: Path = MAIN_DATABASE,
    capture_database: Path = CAPTURE_DATABASE,
    capture_state_path: Path = CAPTURE_STATE,
    now_utc: dt.datetime | None = None,
) -> dict[str, Any]:
    with _configured_base():
        result = _BASE_VERIFY(
            fast_database=fast_database,
            main_database=main_database,
            capture_database=capture_database,
            capture_state_path=capture_state_path,
            now_utc=now_utc,
        )
    failures = sorted(set(list(result.get("failures") or []) + _read_clock_failures(capture_database)))
    result.update(
        {
            "schema_version": SCHEMA_VERSION,
            "verifier_contract_id": VERIFIER_CONTRACT_ID,
            "capture_contract_id": CAPTURE_CONTRACT_ID,
            "capture_cohort_id": CAPTURE_COHORT_ID,
            "verified": not failures,
            "status": "verified" if not failures else "failed",
            "failure_count": len(failures),
            "failures": failures,
        }
    )
    return result


def _write_outputs(
    result: Mapping[str, Any], output_path: Path, heartbeat_path: Path
) -> None:
    v2_verifier.write_json_atomic(output_path, result)
    v2_verifier.write_json_atomic(
        heartbeat_path,
        {
            "schema_version": 1,
            "worker": "oanda_official_event_pair_quote_capture_v3_verifier",
            "status": result["status"],
            "updated_at": result["verified_at_utc"],
            "phase": "independent_dual_ledger_quote_read_clock_verification",
            "details": {
                "verified": result["verified"],
                "failure_count": result["failure_count"],
                "capture_contract_id": CAPTURE_CONTRACT_ID,
                "capture_cohort_id": CAPTURE_COHORT_ID,
                **dict(result.get("counts") or {}),
            },
        },
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fast-database", type=Path, default=FAST_DATABASE)
    parser.add_argument("--main-database", type=Path, default=MAIN_DATABASE)
    parser.add_argument("--capture-database", type=Path, default=CAPTURE_DATABASE)
    parser.add_argument("--capture-state", type=Path, default=CAPTURE_STATE)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT_PATH)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    exit_code = 0
    while True:
        result = verify(
            fast_database=args.fast_database,
            main_database=args.main_database,
            capture_database=args.capture_database,
            capture_state_path=args.capture_state,
        )
        _write_outputs(result, args.output, args.heartbeat)
        if not args.quiet:
            print(json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)
        if not result["verified"]:
            exit_code = 1
        if args.once or (
            args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec
        ):
            return exit_code
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())

