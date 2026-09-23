#!/usr/bin/env python3
"""Independent verifier for prospective per-pair official-event outcomes."""

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
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
LOCAL_NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
CONFIG_PATH = ROOT / "config" / "official_event_pair_horizon_capture_v1.json"
INPUT_DATABASE = LOCAL_NEWS / "official_event_pair_quote_capture_v1.sqlite"
OUTPUT_DATABASE = LOCAL_NEWS / "official_event_pair_horizon_capture_v1.sqlite"
PRODUCER_STATE = LOCAL_NEWS / "official_event_pair_horizon_capture_latest_v1.json"
STATE_PATH = LOCAL_NEWS / "official_event_pair_horizon_capture_verifier_latest_v1.json"
HEARTBEAT_PATH = LOCAL_NEWS / "official_event_pair_horizon_capture_verifier_heartbeat_v1.json"

SCHEMA_VERSION = "official_event_pair_horizon_capture_v1_verifier"
VERIFIER_CONTRACT_ID = "official_event_pair_horizon_capture_v1_independent_verifier_20260902"
CAPTURE_CONTRACT_ID = "official_event_pair_horizon_capture_v1_per_pair_terminal_20260902"
CAPTURE_COHORT_ID = "official_event_pair_horizon_capture_v1_20260902a"
ENTRY_CONTRACT_ID = "official_event_pair_quote_capture_v1_per_pair_tradeability_20260902"
ENTRY_COHORT_ID = "official_event_pair_quote_capture_v1_20260902a"
ACTIVATED_UTC = dt.datetime(2026, 9, 3, 0, 20, tzinfo=dt.timezone.utc)
HORIZONS_MIN = (1, 5, 15, 30, 60)
SLIPPAGE_STRESS_PIPS = (0.0, 0.25, 0.5)
COMPLETENESS_GRACE_SEC = 60.0
EPSILON = 1e-7


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


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return default


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
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


def _same_number(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    try:
        return math.isclose(float(left), float(right), abs_tol=EPSILON, rel_tol=0.0)
    except (TypeError, ValueError):
        return False


def _verify_config() -> list[str]:
    payload = read_json(CONFIG_PATH, {})
    expected = {
        "schema_version": 1,
        "contract_id": CAPTURE_CONTRACT_ID,
        "cohort_id": CAPTURE_COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "required_entry_capture_contract_id": ENTRY_CONTRACT_ID,
        "required_entry_capture_cohort_id": ENTRY_COHORT_ID,
        "horizons_min": list(HORIZONS_MIN),
    }
    failures = [
        f"config:{key}" for key, value in expected.items()
        if payload.get(key) != value
    ]
    policy = payload.get("policy") or {}
    for key, value in {
        "prospective_only": True,
        "historical_backfill_allowed": False,
        "one_terminal_attempt_per_event_horizon": True,
        "input_entry_quote_reacquisition_allowed": False,
        "invalid_pair_does_not_invalidate_other_pairs": True,
        "both_directions_recorded_without_semantic_claim": True,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }.items():
        if policy.get(key) != value:
            failures.append(f"config_policy:{key}")
    return failures


def verify(
    *,
    input_database: Path = INPUT_DATABASE,
    output_database: Path = OUTPUT_DATABASE,
    producer_state: Path = PRODUCER_STATE,
    observed_utc: dt.datetime | None = None,
) -> dict[str, Any]:
    now = observed_utc or utc_now()
    failures = _verify_config()
    counts = {
        "attempts": 0,
        "outcomes": 0,
        "valid_outcomes": 0,
        "due_attempts": 0,
        "missing_due_attempts": 0,
    }
    if not input_database.exists():
        failures.append("input_database_missing")
    if not output_database.exists():
        failures.append("output_database_missing")
    if failures and (not input_database.exists() or not output_database.exists()):
        return _result(now, failures, counts, "unavailable")

    source = sqlite3.connect(
        f"file:{input_database.resolve().as_posix()}?mode=ro", uri=True, timeout=10
    )
    output = sqlite3.connect(
        f"file:{output_database.resolve().as_posix()}?mode=ro", uri=True, timeout=10
    )
    output.row_factory = sqlite3.Row
    source.row_factory = sqlite3.Row
    try:
        if str(output.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
            failures.append("output_integrity")
        triggers = {
            str(row[0]) for row in output.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
        for name in {
            "pair_horizon_attempt_no_update", "pair_horizon_attempt_no_delete",
            "pair_horizon_outcome_no_update", "pair_horizon_outcome_no_delete",
        }:
            if name not in triggers:
                failures.append(f"append_only_trigger:{name}")

        entry_rows = {
            str(row["capture_id"]): row
            for row in source.execute(
                """SELECT capture_id,event_first_known_utc,capture_payload_json,
                          contract_id,cohort_id
                   FROM official_event_pair_quote_capture
                   WHERE contract_id=? AND cohort_id=?""",
                (ENTRY_CONTRACT_ID, ENTRY_COHORT_ID),
            )
        }
        attempts = output.execute(
            "SELECT * FROM official_event_pair_horizon_attempt ORDER BY attempt_id"
        ).fetchall()
        counts["attempts"] = len(attempts)
        seen: set[tuple[str, int]] = set()
        for row in attempts:
            label = str(row["attempt_id"])
            key = (str(row["input_capture_id"]), int(row["horizon_min"]))
            if key in seen:
                failures.append(f"{label}:duplicate_key")
            seen.add(key)
            if row["contract_id"] != CAPTURE_CONTRACT_ID:
                failures.append(f"{label}:contract")
            if row["cohort_id"] != CAPTURE_COHORT_ID:
                failures.append(f"{label}:cohort")
            if any(int(row[name]) != expected for name, expected in {
                "research_only": 1, "execution_eligible": 0,
                "can_place_orders": 0, "can_authorize": 0, "can_promote": 0,
            }.items()):
                failures.append(f"{label}:unsafe_flags")
            event = parse_time(row["event_first_known_utc"])
            if event is None or event < ACTIVATED_UTC:
                failures.append(f"{label}:preactivation")
            entry = entry_rows.get(key[0])
            if entry is None:
                failures.append(f"{label}:entry_missing")
            elif sha256_text(str(entry["capture_payload_json"])) != row[
                "input_capture_payload_sha256"
            ]:
                failures.append(f"{label}:entry_hash")
            if sha256_text(str(row["payload_json"])) != row["payload_sha256"]:
                failures.append(f"{label}:payload_hash")
            try:
                header_payload = json.loads(str(row["payload_json"]))
            except (TypeError, ValueError):
                failures.append(f"{label}:payload_json")
                header_payload = {}
            for field in (
                "attempt_id", "input_capture_id", "observation_id", "source_id",
                "event_first_known_utc", "horizon_min", "target_utc",
                "input_pair_count", "valid_pair_count", "invalid_pair_count",
                "contract_id", "cohort_id",
            ):
                if header_payload.get(field) != row[field]:
                    failures.append(f"{label}:header_field:{field}")

            outcomes = output.execute(
                "SELECT * FROM official_event_pair_horizon_outcome "
                "WHERE attempt_id=? ORDER BY instrument", (label,)
            ).fetchall()
            counts["outcomes"] += len(outcomes)
            valid_count = sum(int(item["valid"]) for item in outcomes)
            counts["valid_outcomes"] += valid_count
            if len(outcomes) != int(row["input_pair_count"]):
                failures.append(f"{label}:component_count")
            if valid_count != int(row["valid_pair_count"]):
                failures.append(f"{label}:valid_count")
            if len(outcomes) - valid_count != int(row["invalid_pair_count"]):
                failures.append(f"{label}:invalid_count")
            hashes: list[str] = []
            for item in outcomes:
                item_label = str(item["outcome_id"])
                hashes.append(str(item["payload_sha256"]))
                if sha256_text(str(item["payload_json"])) != item["payload_sha256"]:
                    failures.append(f"{item_label}:payload_hash")
                if item["contract_id"] != CAPTURE_CONTRACT_ID or item[
                    "cohort_id"
                ] != CAPTURE_COHORT_ID:
                    failures.append(f"{item_label}:lineage")
                if any(int(item[name]) != expected for name, expected in {
                    "research_only": 1, "execution_eligible": 0,
                    "can_place_orders": 0, "can_authorize": 0, "can_promote": 0,
                }.items()):
                    failures.append(f"{item_label}:unsafe_flags")
                if int(item["valid"]):
                    values = [
                        item["entry_bid"], item["entry_ask"], item["exit_bid"],
                        item["exit_ask"], item["pip"],
                    ]
                    if any(value is None for value in values):
                        failures.append(f"{item_label}:valid_values_missing")
                        continue
                    eb, ea, xb, xa, pip = map(float, values)
                    midpoint = (((xb + xa) - (eb + ea)) / 2.0) / pip
                    buy = (xb - ea) / pip
                    sell = (eb - xa) / pip
                    if not _same_number(midpoint, item["signed_mid_move_pips"]):
                        failures.append(f"{item_label}:mid_arithmetic")
                    if not _same_number(buy, item["buy_executable_pips"]):
                        failures.append(f"{item_label}:buy_arithmetic")
                    if not _same_number(sell, item["sell_executable_pips"]):
                        failures.append(f"{item_label}:sell_arithmetic")
                    stress = json.loads(str(item["cost_stress_json"]))
                    if [float(x.get("round_trip_slippage_pips")) for x in stress] != list(
                        SLIPPAGE_STRESS_PIPS
                    ):
                        failures.append(f"{item_label}:stress_grid")
                    if str(item["invalid_reason"]):
                        failures.append(f"{item_label}:valid_has_reason")
                else:
                    if not str(item["invalid_reason"]):
                        failures.append(f"{item_label}:invalid_without_reason")
                    if any(item[name] is not None for name in (
                        "signed_mid_move_pips", "buy_executable_pips",
                        "sell_executable_pips",
                    )):
                        failures.append(f"{item_label}:invalid_has_economics")
            if sha256_text("".join(hashes)) != row["component_root_sha256"]:
                failures.append(f"{label}:component_root")

        cutoff = now - dt.timedelta(seconds=COMPLETENESS_GRACE_SEC)
        for capture_id, entry in entry_rows.items():
            event = parse_time(entry["event_first_known_utc"])
            if event is None or event < ACTIVATED_UTC:
                continue
            for horizon in HORIZONS_MIN:
                if event + dt.timedelta(minutes=horizon) <= cutoff:
                    counts["due_attempts"] += 1
                    if (capture_id, horizon) not in seen:
                        counts["missing_due_attempts"] += 1
                        failures.append(f"due_missing:{capture_id}:{horizon}")
    except (sqlite3.Error, TypeError, ValueError) as exc:
        failures.append(f"verifier_exception:{type(exc).__name__}:{exc}")
    finally:
        source.close()
        output.close()

    producer = read_json(producer_state, {})
    producer_counts = producer.get("counts") or {}
    for field in ("attempts", "outcomes", "valid_outcomes"):
        if int(producer_counts.get(field, -1)) != counts[field]:
            failures.append(f"producer_state_count:{field}")
    if producer.get("contract_id") != CAPTURE_CONTRACT_ID:
        failures.append("producer_state_contract")
    return _result(now, failures, counts, "verified" if not failures else "failed")


def _result(
    now: dt.datetime, failures: list[str], counts: Mapping[str, int], status: str
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "verified_at_utc": iso_utc(now),
        "status": status,
        "verified": not failures,
        "failure_count": len(failures),
        "failures": failures[:200],
        "counts": dict(counts),
        "capture_contract_id": CAPTURE_CONTRACT_ID,
        "capture_cohort_id": CAPTURE_COHORT_ID,
        "verifier_contract_id": VERIFIER_CONTRACT_ID,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }


def run_cycle() -> dict[str, Any]:
    payload = verify()
    write_json_atomic(STATE_PATH, payload)
    write_json_atomic(
        HEARTBEAT_PATH,
        {
            "worker": "oanda_official_event_pair_horizon_capture_v1_verifier",
            "updated_utc": payload["verified_at_utc"],
            "status": payload["status"],
            "failure_count": payload["failure_count"],
            "attempts": payload["counts"]["attempts"],
            "contract_id": VERIFIER_CONTRACT_ID,
        },
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        payload = run_cycle()
        if not args.quiet:
            print(json.dumps(payload, sort_keys=True), flush=True)
        if args.once or args.duration_sec <= 0:
            return 0 if payload["verified"] else 1
        if time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
