#!/usr/bin/env python3
"""Independent verifier for the per-pair official-event quote cohort.

This module intentionally imports no producer or broker code.  It rebuilds
row identity, source lineage, per-pair eligibility, clocks, counts, safety
flags, append-only triggers, and the live state summary from immutable bytes.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
LOCAL_NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
INPUT_DATABASE = LOCAL_NEWS / "official_release_fast_lane_v4.sqlite"
CAPTURE_DATABASE = LOCAL_NEWS / "official_event_pair_quote_capture_v1.sqlite"
CAPTURE_STATE = LOCAL_NEWS / "official_event_pair_quote_capture_latest_v1.json"
OUTPUT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_verifier_latest_v1.json"
HEARTBEAT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_verifier_heartbeat_v1.json"

SCHEMA_VERSION = "official_event_pair_quote_capture_v1_verifier"
VERIFIER_CONTRACT_ID = (
    "official_event_pair_quote_capture_v1_independent_verifier_20260902"
)
CAPTURE_SCHEMA_VERSION = "official_event_pair_quote_capture_v1"
CAPTURE_CONTRACT_ID = (
    "official_event_pair_quote_capture_v1_per_pair_tradeability_20260902"
)
CAPTURE_COHORT_ID = "official_event_pair_quote_capture_v1_20260902a"
RAW_COLLECTOR_CONTRACT_ID = (
    "official_release_fast_lane_v4_selection_v2_authoritative_communications_"
    "20260828T150000Z"
)
RAW_COLLECTOR_COHORT_ID = (
    "official_release_fast_lane_v4_communications_20260828T150000Z"
)
ACTIVATED_UTC = dt.datetime(2026, 9, 2, 20, 15, tzinfo=dt.timezone.utc)
EXPECTED_INSTRUMENTS = tuple(
    """
    AUD_CAD AUD_CHF AUD_HKD AUD_JPY AUD_NZD AUD_SGD AUD_USD
    CAD_CHF CAD_HKD CAD_JPY CAD_SGD CHF_HKD CHF_JPY CHF_ZAR
    EUR_AUD EUR_CAD EUR_CHF EUR_CZK EUR_DKK EUR_GBP EUR_HKD EUR_HUF
    EUR_JPY EUR_NOK EUR_NZD EUR_PLN EUR_SEK EUR_SGD EUR_TRY EUR_USD
    EUR_ZAR GBP_AUD GBP_CAD GBP_CHF GBP_HKD GBP_JPY GBP_NZD GBP_PLN
    GBP_SGD GBP_USD GBP_ZAR HKD_JPY NZD_CAD NZD_CHF NZD_HKD NZD_JPY
    NZD_SGD NZD_USD SGD_CHF SGD_JPY TRY_JPY USD_CAD USD_CHF USD_CNH
    USD_CZK USD_DKK USD_HKD USD_HUF USD_JPY USD_MXN USD_NOK USD_PLN
    USD_SEK USD_SGD USD_THB USD_TRY USD_ZAR ZAR_JPY
    """.split()
)
EXPECTED_INSTRUMENT_COUNT = 68
EXPECTED_UNIVERSE_SHA256 = (
    "b6abe559dcaa9faa0a1e48217f97a12dc9043eb9595d2b946bf80f93035be142"
)
MAXIMUM_DETECTION_LATENCY_SECONDS = 15.0
MAXIMUM_QUOTE_AGE_SECONDS = 30.0
MAXIMUM_FUTURE_SKEW_SECONDS = 2.0


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime | None = None) -> str:
    current = value or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=dt.timezone.utc)
    return current.astimezone(dt.timezone.utc).isoformat()


def parse_time(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def finite_number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
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


def _raw_rows(path: Path) -> dict[str, dict[str, Any]]:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30.0) as connection:
        connection.execute("PRAGMA busy_timeout=30000")
        rows = connection.execute(
            """
            SELECT observation_id,source_id,source_contract_id,first_seen_utc,
                   prospective_observation,raw_payload_json,
                   collector_contract_id,collector_cohort_id
            FROM official_release_observation
            WHERE first_seen_utc >= ?
            """,
            (iso_utc(ACTIVATED_UTC),),
        ).fetchall()
    return {
        str(row[0]): {
            "source_id": str(row[1]),
            "source_contract_id": str(row[2]),
            "first_seen_utc": str(row[3]),
            "prospective_observation": bool(row[4]),
            "raw_payload_json": str(row[5]),
            "collector_contract_id": str(row[6]),
            "collector_cohort_id": str(row[7]),
        }
        for row in rows
    }


def _verify_capture_row(
    row: Sequence[Any], raw_rows: Mapping[str, Mapping[str, Any]]
) -> list[str]:
    (
        capture_id,
        observation_id,
        source_id,
        source_contract_id,
        event_first_known_utc,
        captured_utc,
        detection_latency_seconds,
        timing_quality,
        eligible_quote_count,
        explicitly_tradeable_quote_count,
        exact_all_68_available,
        raw_observation_payload_sha256,
        quote_snapshot_payload_sha256,
        capture_payload_json,
        research_only,
        execution_eligible,
        can_authorize,
        can_promote,
        contract_id,
        cohort_id,
        activated_utc,
    ) = row
    prefix = str(observation_id)
    failures: list[str] = []
    expected_capture_id = sha256_text(
        f"{observation_id}|{CAPTURE_CONTRACT_ID}|{CAPTURE_COHORT_ID}"
    )
    if str(capture_id) != expected_capture_id:
        failures.append(f"{prefix}:capture_id_mismatch")
    if (
        str(contract_id) != CAPTURE_CONTRACT_ID
        or str(cohort_id) != CAPTURE_COHORT_ID
        or parse_time(activated_utc) != ACTIVATED_UTC
    ):
        failures.append(f"{prefix}:capture_lineage_mismatch")
    if not (
        int(research_only) == 1
        and int(execution_eligible) == 0
        and int(can_authorize) == 0
        and int(can_promote) == 0
    ):
        failures.append(f"{prefix}:unsafe_row_flags")
    try:
        payload = json.loads(str(capture_payload_json))
    except (TypeError, ValueError):
        return failures + [f"{prefix}:invalid_capture_payload_json"]
    if not isinstance(payload, dict):
        return failures + [f"{prefix}:capture_payload_not_object"]
    raw = raw_rows.get(str(observation_id))
    if raw is None:
        failures.append(f"{prefix}:raw_observation_missing")
    else:
        if not raw.get("prospective_observation"):
            failures.append(f"{prefix}:raw_observation_not_prospective")
        if (
            raw.get("collector_contract_id") != RAW_COLLECTOR_CONTRACT_ID
            or raw.get("collector_cohort_id") != RAW_COLLECTOR_COHORT_ID
        ):
            failures.append(f"{prefix}:raw_collector_lineage_mismatch")
        if (
            str(source_id) != raw.get("source_id")
            or str(source_contract_id) != raw.get("source_contract_id")
            or parse_time(event_first_known_utc)
            != parse_time(raw.get("first_seen_utc"))
        ):
            failures.append(f"{prefix}:raw_source_identity_mismatch")
        if sha256_text(str(raw.get("raw_payload_json") or "")) != str(
            raw_observation_payload_sha256
        ):
            failures.append(f"{prefix}:raw_payload_hash_mismatch")
    if str(payload.get("quote_snapshot_payload_sha256") or "") != str(
        quote_snapshot_payload_sha256
    ):
        failures.append(f"{prefix}:quote_snapshot_hash_lineage_mismatch")
    identity = {
        "capture_id": capture_id,
        "observation_id": observation_id,
        "source_id": source_id,
        "source_contract_id": source_contract_id,
        "event_first_known_utc": event_first_known_utc,
        "captured_utc": captured_utc,
        "timing_quality": timing_quality,
        "contract_id": contract_id,
        "cohort_id": cohort_id,
    }
    if any(str(payload.get(key) or "") != str(value) for key, value in identity.items()):
        failures.append(f"{prefix}:row_payload_identity_mismatch")
    if not (
        payload.get("research_only") is True
        and payload.get("execution_eligible") is False
        and payload.get("can_place_orders") is False
        and payload.get("can_authorize") is False
        and payload.get("can_promote") is False
    ):
        failures.append(f"{prefix}:unsafe_payload_flags")
    event_time = parse_time(event_first_known_utc)
    captured_time = parse_time(captured_utc)
    derived_latency = (
        None
        if event_time is None or captured_time is None
        else (captured_time - event_time).total_seconds()
    )
    if (
        event_time is None
        or captured_time is None
        or event_time < ACTIVATED_UTC
        or derived_latency is None
        or abs(float(detection_latency_seconds) - derived_latency) > 1e-6
        or abs(float(payload.get("detection_latency_seconds")) - derived_latency) > 1e-6
    ):
        failures.append(f"{prefix}:capture_clock_mismatch")

    pair_rows = payload.get("pair_rows")
    eligible_quotes = payload.get("eligible_quotes")
    if not isinstance(pair_rows, Mapping) or set(pair_rows) != set(EXPECTED_INSTRUMENTS):
        failures.append(f"{prefix}:pair_row_universe_mismatch")
        pair_rows = {}
    if not isinstance(eligible_quotes, Mapping):
        failures.append(f"{prefix}:eligible_quotes_not_object")
        eligible_quotes = {}
    recomputed_eligible: set[str] = set()
    recomputed_tradeable = 0
    for instrument in EXPECTED_INSTRUMENTS:
        item = pair_rows.get(instrument)
        if not isinstance(item, Mapping):
            continue
        if item.get("tradeable") is True:
            recomputed_tradeable += 1
        if item.get("eligible") is not True:
            continue
        bid = finite_number(item.get("bid"))
        ask = finite_number(item.get("ask"))
        pip = finite_number(item.get("pip"))
        quote_age = finite_number(item.get("quote_age_seconds"))
        if not (
            item.get("tradeable") is True
            and not str(item.get("invalid_reason") or "")
            and bid is not None
            and ask is not None
            and ask > bid
            and pip is not None
            and pip > 0.0
            and quote_age is not None
            and -MAXIMUM_FUTURE_SKEW_SECONDS <= quote_age <= MAXIMUM_QUOTE_AGE_SECONDS
        ):
            failures.append(f"{prefix}:{instrument}:invalid_eligible_pair")
            continue
        recomputed_eligible.add(instrument)
    if set(eligible_quotes) != recomputed_eligible:
        failures.append(f"{prefix}:eligible_quote_set_mismatch")
    if (
        int(eligible_quote_count) != len(recomputed_eligible)
        or int(payload.get("eligible_quote_count") or 0) != len(recomputed_eligible)
    ):
        failures.append(f"{prefix}:eligible_quote_count_mismatch")
    if (
        int(explicitly_tradeable_quote_count) != recomputed_tradeable
        or int(payload.get("explicitly_tradeable_quote_count") or 0)
        != recomputed_tradeable
    ):
        failures.append(f"{prefix}:tradeable_quote_count_mismatch")
    exact = len(recomputed_eligible) == EXPECTED_INSTRUMENT_COUNT
    if bool(exact_all_68_available) != exact or bool(
        payload.get("exact_all_68_available")
    ) != exact:
        failures.append(f"{prefix}:exact_all68_flag_mismatch")
    metadata_reasons = payload.get("metadata_invalid_reasons") or []
    ready = str(timing_quality) == "prospective_per_pair_executable_quotes"
    if ready != bool(not metadata_reasons and recomputed_eligible):
        failures.append(f"{prefix}:timing_quality_mismatch")
    if metadata_reasons and recomputed_eligible:
        failures.append(f"{prefix}:snapshot_invalid_but_pairs_eligible")
    if ready and not (
        derived_latency is not None
        and 0.0 <= derived_latency <= MAXIMUM_DETECTION_LATENCY_SECONDS
    ):
        failures.append(f"{prefix}:ready_capture_latency_invalid")
    return failures


def verify(
    *,
    raw_database: Path = INPUT_DATABASE,
    capture_database: Path = CAPTURE_DATABASE,
    capture_state_path: Path = CAPTURE_STATE,
    now_utc: dt.datetime | None = None,
    maximum_state_age_seconds: float = 30.0,
) -> dict[str, Any]:
    observed = (now_utc or utc_now()).astimezone(dt.timezone.utc)
    state = read_json(capture_state_path)
    failures: list[str] = []
    state_time = parse_time(state.get("generated_utc"))
    state_age = None if state_time is None else (observed - state_time).total_seconds()
    if not (
        state.get("schema_version") == CAPTURE_SCHEMA_VERSION
        and state.get("contract_id") == CAPTURE_CONTRACT_ID
        and state.get("cohort_id") == CAPTURE_COHORT_ID
        and parse_time(state.get("activated_utc")) == ACTIVATED_UTC
    ):
        failures.append("state_contract_mismatch")
    if state.get("status") != "ok" or state.get("sqlite_integrity") != "ok":
        failures.append("producer_state_not_ok")
    if state_age is None or not (-5.0 <= state_age <= maximum_state_age_seconds):
        failures.append("producer_state_stale")
    policy = state.get("policy")
    policy = policy if isinstance(policy, Mapping) else {}
    for key, value in {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "historical_backfill_allowed": False,
        "semantic_mapping_read_before_capture": False,
    }.items():
        if policy.get(key) != value:
            failures.append(f"unsafe_or_missing_policy:{key}")

    raw_rows = _raw_rows(raw_database)
    database_integrity = "missing"
    rows: list[Sequence[Any]] = []
    trigger_count = 0
    try:
        uri = f"file:{capture_database.resolve().as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=30.0) as connection:
            connection.execute("PRAGMA busy_timeout=30000")
            database_integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
            trigger_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM sqlite_master
                    WHERE type='trigger' AND name IN (
                      'pair_quote_capture_no_update',
                      'pair_quote_capture_no_delete'
                    )
                    """
                ).fetchone()[0]
            )
            rows = connection.execute(
                "SELECT * FROM official_event_pair_quote_capture "
                "ORDER BY captured_utc,observation_id"
            ).fetchall()
    except (OSError, sqlite3.Error) as exc:
        failures.append(f"capture_database_error:{type(exc).__name__}:{exc}")
    if database_integrity != "ok":
        failures.append(f"capture_database_integrity:{database_integrity}")
    if trigger_count != 2:
        failures.append("append_only_trigger_mismatch")
    for row in rows:
        failures.extend(_verify_capture_row(row, raw_rows))

    capture_count = len(rows)
    ready_count = sum(
        1 for row in rows if str(row[7]) == "prospective_per_pair_executable_quotes"
    )
    exact_count = sum(int(row[10]) for row in rows)
    eligible_total = sum(int(row[8]) for row in rows)
    state_counts = state.get("counts")
    state_counts = state_counts if isinstance(state_counts, Mapping) else {}
    expected_counts = {
        "capture_count": capture_count,
        "per_pair_ready_count": ready_count,
        "exact_all_68_count": exact_count,
        "eligible_pair_quote_total": eligible_total,
    }
    if any(int(state_counts.get(key) or 0) != value for key, value in expected_counts.items()):
        failures.append("producer_state_count_mismatch")
    unique_failures = sorted(set(failures))
    return {
        "schema_version": SCHEMA_VERSION,
        "verifier_contract_id": VERIFIER_CONTRACT_ID,
        "capture_contract_id": CAPTURE_CONTRACT_ID,
        "capture_cohort_id": CAPTURE_COHORT_ID,
        "verified_at_utc": iso_utc(observed),
        "verified": not unique_failures,
        "status": "verified" if not unique_failures else "failed",
        "failure_count": len(unique_failures),
        "failures": unique_failures,
        "producer_state_age_seconds": (
            None if state_age is None else round(state_age, 6)
        ),
        "database_integrity": database_integrity,
        "append_only_trigger_count": trigger_count,
        "counts": expected_counts,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }


def run_cycle(
    *,
    raw_database: Path = INPUT_DATABASE,
    capture_database: Path = CAPTURE_DATABASE,
    capture_state_path: Path = CAPTURE_STATE,
    output_path: Path = OUTPUT_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
) -> dict[str, Any]:
    result = verify(
        raw_database=raw_database,
        capture_database=capture_database,
        capture_state_path=capture_state_path,
    )
    write_json_atomic(output_path, result)
    write_json_atomic(
        heartbeat_path,
        {
            "schema_version": 1,
            "worker": "oanda_official_event_pair_quote_capture_v1_verifier",
            "status": result["status"],
            "updated_at": result["verified_at_utc"],
            "phase": "independent_verification",
            "details": {
                "verified": result["verified"],
                "failure_count": result["failure_count"],
                "capture_count": result["counts"]["capture_count"],
                "capture_contract_id": CAPTURE_CONTRACT_ID,
                "capture_cohort_id": CAPTURE_COHORT_ID,
            },
        },
    )
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-database", type=Path, default=INPUT_DATABASE)
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
        result = run_cycle(
            raw_database=args.raw_database,
            capture_database=args.capture_database,
            capture_state_path=args.capture_state,
            output_path=args.output,
            heartbeat_path=args.heartbeat,
        )
        if not args.quiet:
            print(json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)
        if not result["verified"]:
            exit_code = 1
        if args.once or (
            args.duration_sec > 0.0
            and time.monotonic() - started >= args.duration_sec
        ):
            break
        time.sleep(max(1.0, args.interval_sec))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

