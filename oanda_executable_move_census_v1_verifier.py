#!/usr/bin/env python3
"""Independent verifier for the prospective all-68 executable move census.

This module deliberately does not import the producer.  It reconstructs the
frozen 68-pair universe, every logical long/short terminal arm, review-event
lineage, and the latest dashboard payload directly from append-only quote
frames.  It has no network, order, authorization, or promotion surface.
"""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


UTC = dt.timezone.utc
NY = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "executable_move_census_v1_20260830b.json"
DATABASE_PATH = (
    ROOT / "data" / "oanda_training_manager" / "state"
    / "executable_move_census_v1_20260830b.sqlite"
)
LATEST_PATH = (
    ROOT / "data" / "oanda_training_manager" / "state"
    / "executable_move_census_latest_v1_20260830b.json"
)
PRODUCER_PATH = ROOT / "oanda_executable_move_census_v1.py"
SOURCE_PRODUCER_PATH = ROOT / "oanda_practice_top_signal_executor.py"
QUOTE_TRANSPORT_PATH = ROOT / "oanda_quote_transport.py"
OUTPUT_PATH = (
    ROOT / "data" / "oanda_training_manager" / "state"
    / "executable_move_census_verifier_latest_v1_20260830b.json"
)
CHECKPOINT_PATH = (
    ROOT / "data" / "oanda_training_manager" / "state"
    / "executable_move_census_verifier_checkpoint_v1_20260830b.json"
)

SCHEMA = "executable_move_census_v1"
COHORT_ID = "all68_executable_move_census_v1_20260830b"
ACTIVATION_UTC = dt.datetime(2026, 8, 31, 0, 0, tzinfo=UTC)
CADENCE_SEC = 60
HORIZONS_MIN = (1, 5, 10, 15, 30, 60)
SLIPPAGE_PIPS = 0.25
SLIPPAGE_SEMANTICS = "total_round_trip_subtracted_once"
MAXIMUM_QUOTE_AGE_SEC = 30.0
MAXIMUM_SNAPSHOT_GENERATED_AGE_SEC = 30.0
MAXIMUM_FUTURE_SKEW_SEC = 2.0
MAXIMUM_CAPTURE_DELAY_SEC = 55.0
MAXIMUM_TARGET_OFFSET_SEC = 55.0
MAXIMUM_LATEST_AGE_SEC = 150.0
SOURCE_SCHEMA_VERSION = "2"
SOURCE_PRODUCER = "practice_007_fast_executor_price_stream"
CAPTURE_CONTRACT_ID = "all68_exact_snapshot_capture_v1"
CAPTURE_COHORT_ID = "all68_exact_snapshot_capture_20260830b"
MARKET_CALENDAR_POLICY = "oanda_fx_sunday_1705_friday_1659_ny_daily_break_v1"
DESCRIPTIVE_PATH_CONTRACT = "predeclared_causal_minute_frames_first_clear_max_min_v1"
PATH_MEMBER_DEDUPE = (
    "same_entry_instrument_side_and_peak_exit_collapses_across_nested_"
    "declared_windows"
)
SPECIAL_HOURS = (
    "AUD_NZD", "EUR_NZD", "GBP_NZD", "NZD_CAD", "NZD_CHF", "NZD_HKD",
    "NZD_JPY", "NZD_SGD", "NZD_USD", "EUR_TRY", "TRY_JPY", "USD_TRY",
)

# Filled only after the producer/config freeze.  Keeping the pins as literals
# prevents a mutually edited config, producer, and empty database from
# self-certifying a new experiment under the old cohort name.
FROZEN_CONFIG_SHA256 = (
    "68db4f5232f4d314cccc789580a27d134e4ef88204c0621bf050e5865d6f8ae1"
)
FROZEN_PRODUCER_SHA256 = (
    "38202b73cab409bdebdb2f2cd704a4d57907836244f7cbea0d76cfa7344b0272"
)
FROZEN_SOURCE_PRODUCER_SHA256 = (
    "a7d14b1f7ea8e1490cb83262afe7473e5b56a45808503c186f379208e92befed"
)
FROZEN_QUOTE_TRANSPORT_SHA256 = (
    "27ea11bc79a3a85cc09f00a88df697f0ecf8bba29b230c309cdf83e8967d8987"
)

EXPECTED_PIPS: dict[str, float] = {
    "AUD_CAD": .0001, "AUD_CHF": .0001, "AUD_HKD": .0001, "AUD_JPY": .01,
    "AUD_NZD": .0001, "AUD_SGD": .0001, "AUD_USD": .0001, "CAD_CHF": .0001,
    "CAD_HKD": .0001, "CAD_JPY": .01, "CAD_SGD": .0001, "CHF_HKD": .0001,
    "CHF_JPY": .01, "CHF_ZAR": .0001, "EUR_AUD": .0001, "EUR_CAD": .0001,
    "EUR_CHF": .0001, "EUR_CZK": .0001, "EUR_DKK": .0001, "EUR_GBP": .0001,
    "EUR_HKD": .0001, "EUR_HUF": .01, "EUR_JPY": .01, "EUR_NOK": .0001,
    "EUR_NZD": .0001, "EUR_PLN": .0001, "EUR_SEK": .0001, "EUR_SGD": .0001,
    "EUR_TRY": .0001, "EUR_USD": .0001, "EUR_ZAR": .0001, "GBP_AUD": .0001,
    "GBP_CAD": .0001, "GBP_CHF": .0001, "GBP_HKD": .0001, "GBP_JPY": .01,
    "GBP_NZD": .0001, "GBP_PLN": .0001, "GBP_SGD": .0001, "GBP_USD": .0001,
    "GBP_ZAR": .0001, "HKD_JPY": .0001, "NZD_CAD": .0001, "NZD_CHF": .0001,
    "NZD_HKD": .0001, "NZD_JPY": .01, "NZD_SGD": .0001, "NZD_USD": .0001,
    "SGD_CHF": .0001, "SGD_JPY": .01, "TRY_JPY": .01, "USD_CAD": .0001,
    "USD_CHF": .0001, "USD_CNH": .0001, "USD_CZK": .0001, "USD_DKK": .0001,
    "USD_HKD": .0001, "USD_HUF": .01, "USD_JPY": .01, "USD_MXN": .0001,
    "USD_NOK": .0001, "USD_PLN": .0001, "USD_SEK": .0001, "USD_SGD": .0001,
    "USD_THB": .01, "USD_TRY": .0001, "USD_ZAR": .0001, "ZAR_JPY": .01,
}
EXPECTED_INSTRUMENTS = tuple(EXPECTED_PIPS)

TABLE_COLUMNS = {
    "cohort_manifest": (
        "singleton", "cohort_id", "contract_id", "activation_utc",
        "universe_json", "universe_sha256", "pip_map_json", "pip_map_sha256",
        "horizons_json", "slippage_pips", "slippage_semantics", "cadence_sec",
        "timing_contract_json", "market_calendar_policy", "capture_contract_id",
        "capture_cohort_id", "source_schema_version", "source_producer",
        "source_code_sha256", "quote_transport_sha256", "config_sha256",
        "producer_sha256", "research_only", "can_trade", "can_authorize",
        "can_promote",
    ),
    "frames": (
        "frame_id", "cohort_id", "scheduled_utc", "read_started_utc",
        "captured_utc", "precommit_utc", "market_open", "state", "reason",
        "expected_count", "valid_count", "invalid_count", "raw_payload",
        "raw_payload_sha256", "capture_contract_id", "capture_cohort_id",
        "source_schema_version", "source_producer",
    ),
    "frame_quotes": (
        "frame_id", "instrument", "universe_index", "state", "reason", "bid",
        "ask", "pip", "quote_utc", "quote_age_sec", "component_bytes",
        "component_sha256", "capture_contract_id", "capture_cohort_id",
    ),
    "frame_commit_receipts": (
        "frame_id", "committed_utc", "within_first_horizon",
    ),
    "window_evaluations": (
        "target_scheduled_utc", "declared_window_min", "entry_frame_id",
        "exit_frame_id", "evaluated_utc", "state", "expected_side_count",
        "valid_count", "cleared_count", "path_cleared_count",
        "invalid_count", "pending_count", "terminal_rows_sha256",
        "path_summary_rows_sha256", "terminal_clear_ids_sha256",
        "path_clear_ids_sha256",
    ),
    "factor_episode_summaries": (
        "episode_utc", "factor_currency", "factor_sign", "event_kind",
        "finalized_utc", "member_count", "member_ids_sha256",
        "representative_json",
    ),
    "factor_episode_finalizations": (
        "episode_utc", "finalized_utc", "expected_window_count",
        "evaluated_window_count", "summary_count", "evaluation_keys_sha256",
        "terminal_member_count", "terminal_member_ids_sha256",
        "path_member_count", "path_member_ids_sha256",
    ),
}

TRIGGERS = {
    "manifest_no_update": ("cohort_manifest", "UPDATE"),
    "manifest_no_delete": ("cohort_manifest", "DELETE"),
    "frames_no_update": ("frames", "UPDATE"),
    "frames_no_delete": ("frames", "DELETE"),
    "quotes_no_update": ("frame_quotes", "UPDATE"),
    "quotes_no_delete": ("frame_quotes", "DELETE"),
    "receipts_no_update": ("frame_commit_receipts", "UPDATE"),
    "receipts_no_delete": ("frame_commit_receipts", "DELETE"),
    "evaluations_no_update": ("window_evaluations", "UPDATE"),
    "evaluations_no_delete": ("window_evaluations", "DELETE"),
    "factor_summaries_no_update": ("factor_episode_summaries", "UPDATE"),
    "factor_summaries_no_delete": ("factor_episode_summaries", "DELETE"),
    "factor_finalizations_no_update": (
        "factor_episode_finalizations", "UPDATE",
    ),
    "factor_finalizations_no_delete": (
        "factor_episode_finalizations", "DELETE",
    ),
}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def parse_utc(value: Any) -> dt.datetime | None:
    try:
        result = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def close(a: Any, b: Any, tolerance: float = 1e-9) -> bool:
    left, right = finite(a), finite(b)
    return left is not None and right is not None and abs(left - right) <= tolerance


def record(failures: list[str], condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


def market_open(value: dt.datetime) -> bool:
    local = value.astimezone(NY)
    weekday = local.weekday()
    minute = local.hour * 60 + local.minute
    if (weekday == 4 and minute >= 16 * 60 + 59) or weekday == 5:
        return False
    if weekday == 6 and minute < 17 * 60 + 5:
        return False
    return not (16 * 60 + 59 <= minute < 17 * 60 + 5)


def instrument_market_open(instrument: str, value: dt.datetime) -> bool:
    if not market_open(value):
        return False
    if instrument in SPECIAL_HOURS:
        local = value.astimezone(NY)
        minute = local.hour * 60 + local.minute
        if local.weekday() == 6 and minute < 17 * 60 + 10:
            return False
        if 16 * 60 + 55 <= minute < 17 * 60 + 10:
            return False
    return True


def expected_config() -> dict[str, Any]:
    return {
        "schema_version": "executable_move_census_config_v1",
        "cohort_id": COHORT_ID,
        "activation_utc": iso(ACTIVATION_UTC),
        "no_backfill": True,
        "frame_cadence_sec": CADENCE_SEC,
        "maximum_quote_age_sec": int(MAXIMUM_QUOTE_AGE_SEC),
        "maximum_snapshot_generated_age_sec": int(
            MAXIMUM_SNAPSHOT_GENERATED_AGE_SEC
        ),
        "maximum_future_skew_sec": int(MAXIMUM_FUTURE_SKEW_SEC),
        "maximum_capture_delay_sec": int(MAXIMUM_CAPTURE_DELAY_SEC),
        "maximum_target_offset_sec": int(MAXIMUM_TARGET_OFFSET_SEC),
        "source_schema_version": SOURCE_SCHEMA_VERSION,
        "source_producer": SOURCE_PRODUCER,
        "capture_contract_id": CAPTURE_CONTRACT_ID,
        "capture_cohort_id": CAPTURE_COHORT_ID,
        "market_calendar_policy": MARKET_CALENDAR_POLICY,
        "special_hours_fail_closed_instruments": list(SPECIAL_HOURS),
        "terminal_primary": True,
        "descriptive_path_contract": DESCRIPTIVE_PATH_CONTRACT,
        "path_member_dedupe": PATH_MEMBER_DEDUPE,
        "horizons_min": list(HORIZONS_MIN),
        "slippage_pips": SLIPPAGE_PIPS,
        "slippage_semantics": SLIPPAGE_SEMANTICS,
        "research_only": True,
        "can_trade": False,
        "can_authorize": False,
        "can_promote": False,
        "instruments": dict(EXPECTED_PIPS),
    }


def _read_config(path: Path, failures: list[str]) -> bytes:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except (OSError, ValueError) as exc:
        failures.append(f"config:unreadable:{type(exc).__name__}")
        return b""
    record(failures, sha(raw) == FROZEN_CONFIG_SHA256, "config:file_sha256:mismatch")
    record(failures, payload == expected_config(), "config:literal_contract:mismatch")
    return raw


def _scan_no_execution_surface(path: Path, failures: list[str]) -> bytes:
    try:
        raw = path.read_bytes()
        tree = ast.parse(raw, filename=str(path))
    except (OSError, SyntaxError) as exc:
        failures.append(f"producer:unreadable_or_invalid:{type(exc).__name__}")
        return b""
    record(failures, sha(raw) == FROZEN_PRODUCER_SHA256, "producer:file_sha256:mismatch")
    forbidden_imports = {
        "requests", "httpx", "urllib", "oandapyv20", "aiohttp",
    }
    forbidden_calls = {
        "post", "put", "patch", "delete", "create_order", "place_order",
        "submit_order", "authorize", "promote",
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                record(
                    failures,
                    alias.name.split(".")[0] not in forbidden_imports,
                    f"producer:forbidden_import:{alias.name}",
                )
        elif isinstance(node, ast.ImportFrom):
            module = str(node.module or "").split(".")[0]
            record(
                failures, module not in forbidden_imports,
                f"producer:forbidden_import:{node.module}",
            )
        elif isinstance(node, ast.Call):
            function = node.func
            name = function.attr if isinstance(function, ast.Attribute) else (
                function.id if isinstance(function, ast.Name) else ""
            )
            record(
                failures, name.lower() not in forbidden_calls,
                f"producer:forbidden_call:{name}",
            )
    return raw


def _read_source_identity(
    path: Path, frozen_sha256: str, label: str, failures: list[str]
) -> bytes:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        failures.append(f"{label}:unreadable:{type(exc).__name__}")
        return b""
    record(
        failures,
        sha(raw) == frozen_sha256,
        f"{label}:file_sha256:mismatch",
    )
    return raw


def _open_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=10.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _verify_schema(connection: sqlite3.Connection, failures: list[str]) -> None:
    quick = connection.execute("PRAGMA quick_check").fetchone()
    record(failures, quick is not None and quick[0] == "ok", "database:quick_check")
    record(
        failures,
        not connection.execute("PRAGMA foreign_key_check").fetchall(),
        "database:foreign_key_check",
    )
    observed_tables = {
        row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    }
    record(
        failures, observed_tables == set(TABLE_COLUMNS),
        "schema:table_set_mismatch",
    )
    for table, expected in TABLE_COLUMNS.items():
        columns = tuple(
            row[1] for row in connection.execute(f"PRAGMA table_info({table})")
        )
        record(failures, columns == expected, f"schema:{table}:columns_mismatch")
    expected_foreign_keys = {
        "frame_quotes": {("frame_id", "frames", "frame_id")},
        "frame_commit_receipts": {("frame_id", "frames", "frame_id")},
        "window_evaluations": {
            ("entry_frame_id", "frames", "frame_id"),
            ("exit_frame_id", "frames", "frame_id"),
        },
    }
    for table, expected in expected_foreign_keys.items():
        observed = {
            (row[3], row[2], row[4])
            for row in connection.execute(f"PRAGMA foreign_key_list({table})")
        }
        record(
            failures, observed == expected,
            f"schema:{table}:foreign_keys_mismatch",
        )
    table_sql = {
        row[0]: " ".join(str(row[1] or "").upper().split())
        for row in connection.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='table'"
        )
    }
    required_fragments = {
        "cohort_manifest": (
            "CHECK(SINGLETON=1)", "CHECK(RESEARCH_ONLY=1)",
            "CHECK(CAN_TRADE=0)", "CHECK(CAN_AUTHORIZE=0)",
            "CHECK(CAN_PROMOTE=0)",
        ),
        "frames": (
            "CHECK(EXPECTED_COUNT=68)", "UNIQUE(COHORT_ID,SCHEDULED_UTC)",
        ),
        "frame_quotes": (
            "PRIMARY KEY(FRAME_ID,INSTRUMENT)",
            "UNIQUE(FRAME_ID,UNIVERSE_INDEX)",
        ),
        "window_evaluations": (
            "CHECK(EXPECTED_SIDE_COUNT=136)",
            "PRIMARY KEY(TARGET_SCHEDULED_UTC,DECLARED_WINDOW_MIN)",
        ),
        "factor_episode_summaries": (
            "PRIMARY KEY(EPISODE_UTC,FACTOR_CURRENCY,FACTOR_SIGN,EVENT_KIND)",
        ),
        "factor_episode_finalizations": ("EPISODE_UTC TEXT PRIMARY KEY",),
    }
    for table, fragments in required_fragments.items():
        sql = table_sql.get(table, "")
        for fragment in fragments:
            record(
                failures, fragment in sql,
                f"schema:{table}:constraint_missing:{fragment}",
            )
    triggers = {
        row[0]: (row[1], str(row[2] or ""))
        for row in connection.execute(
            "SELECT name,tbl_name,sql FROM sqlite_master WHERE type='trigger'"
        )
    }
    for name, (table, operation) in TRIGGERS.items():
        observed = triggers.get(name)
        sql = "".join(str(observed[1] if observed else "").upper().split())
        expected_sql = "".join(
            (
                f"CREATE TRIGGER {name} BEFORE {operation} ON {table} "
                "BEGIN SELECT RAISE(ABORT,'append_only'); END"
            ).upper().split()
        )
        record(
            failures,
            observed is not None
            and observed[0] == table
            and sql == expected_sql,
            f"trigger:{name}:missing_or_unsafe",
        )


def _verify_manifest(
    connection: sqlite3.Connection,
    config_raw: bytes,
    producer_raw: bytes,
    source_raw: bytes,
    quote_transport_raw: bytes,
    failures: list[str],
) -> None:
    rows = connection.execute("SELECT * FROM cohort_manifest").fetchall()
    record(failures, len(rows) == 1, "manifest:row_count")
    if len(rows) != 1:
        return
    row = dict(rows[0])
    universe = list(EXPECTED_INSTRUMENTS)
    pips = dict(EXPECTED_PIPS)
    timing = {
        "maximum_quote_age_sec": int(MAXIMUM_QUOTE_AGE_SEC),
        "maximum_snapshot_generated_age_sec": int(
            MAXIMUM_SNAPSHOT_GENERATED_AGE_SEC
        ),
        "maximum_future_skew_sec": int(MAXIMUM_FUTURE_SKEW_SEC),
        "maximum_capture_delay_sec": int(MAXIMUM_CAPTURE_DELAY_SEC),
        "maximum_target_offset_sec": int(MAXIMUM_TARGET_OFFSET_SEC),
    }
    expected = {
        "singleton": 1,
        "cohort_id": COHORT_ID,
        "contract_id": SCHEMA,
        "activation_utc": iso(ACTIVATION_UTC),
        "universe_json": canonical(universe).decode(),
        "universe_sha256": sha(canonical(universe)),
        "pip_map_json": canonical(pips).decode(),
        "pip_map_sha256": sha(canonical(pips)),
        "horizons_json": canonical(list(HORIZONS_MIN)).decode(),
        "slippage_pips": SLIPPAGE_PIPS,
        "slippage_semantics": SLIPPAGE_SEMANTICS,
        "cadence_sec": CADENCE_SEC,
        "timing_contract_json": canonical(timing).decode(),
        "market_calendar_policy": MARKET_CALENDAR_POLICY,
        "capture_contract_id": CAPTURE_CONTRACT_ID,
        "capture_cohort_id": CAPTURE_COHORT_ID,
        "source_schema_version": SOURCE_SCHEMA_VERSION,
        "source_producer": SOURCE_PRODUCER,
        "source_code_sha256": sha(source_raw),
        "quote_transport_sha256": sha(quote_transport_raw),
        "config_sha256": sha(config_raw),
        "producer_sha256": sha(producer_raw),
        "research_only": 1,
        "can_trade": 0,
        "can_authorize": 0,
        "can_promote": 0,
    }
    for key, value in expected.items():
        if isinstance(value, float):
            good = close(row.get(key), value)
        else:
            good = row.get(key) == value
        record(failures, good, f"manifest:{key}:mismatch")


def _expected_quote(
    instrument: str,
    component: Mapping[str, Any],
    captured: dt.datetime,
    scheduled: dt.datetime,
    source_ok: bool,
    snapshot_ok: bool,
) -> dict[str, Any]:
    bid = finite(component.get("bid"))
    ask = finite(component.get("ask"))
    observed = parse_utc(component.get("time"))
    observed_pip = finite(component.get("pip"))
    state, reason, age = "valid", "", None
    if not instrument_market_open(instrument, scheduled):
        state, reason = "excluded_closed", "market_closed_or_special_hours_fail_closed"
    elif not source_ok:
        state, reason = "invalid", "source_identity_mismatch"
    elif not component:
        state, reason = "missing", "missing_instrument"
    elif not snapshot_ok:
        state, reason = "invalid", "snapshot_header_or_universe_mismatch"
    elif bid is None or ask is None or bid <= 0 or ask <= bid:
        state, reason = "invalid", "invalid_bid_ask"
    elif observed_pip is None or abs(observed_pip - EXPECTED_PIPS[instrument]) > 1e-12:
        state, reason = "invalid", "pip_contract_mismatch"
    elif observed is None:
        state, reason = "invalid", "invalid_quote_clock"
    else:
        age = (captured - observed).total_seconds()
        if age < -MAXIMUM_FUTURE_SKEW_SEC:
            state, reason = "invalid", "future_quote"
        elif age > MAXIMUM_QUOTE_AGE_SEC:
            state, reason = "stale", "stale_quote"
        elif abs((observed - scheduled).total_seconds()) > MAXIMUM_TARGET_OFFSET_SEC:
            state, reason = "invalid", "quote_target_offset"
    return {
        "state": state,
        "reason": reason,
        "bid": bid if state == "valid" else None,
        "ask": ask if state == "valid" else None,
        "pip": EXPECTED_PIPS[instrument],
        "quote_utc": iso(observed) if observed else None,
        "quote_age_sec": age,
        "component_bytes": canonical(component),
        "component_sha256": sha(canonical(component)),
    }


def _verify_frames(
    connection: sqlite3.Connection,
    failures: list[str],
    now: dt.datetime,
    prefix_count: int = 0,
    retain_quote_after: dt.datetime | None = None,
) -> tuple[
    list[Mapping[str, Any]], list[Mapping[str, Any]],
    dict[str, dict[str, sqlite3.Row]], int, str | None, str,
]:
    frames: list[Mapping[str, Any]] = []
    receipts = {
        row["frame_id"]: row
        for row in connection.execute("SELECT * FROM frame_commit_receipts")
    }
    quote_cache: dict[str, dict[str, sqlite3.Row]] = {}
    seen_schedule: set[str] = set()
    observed_quote_count = 0
    processed_count = 0
    frame_chain = hashlib.sha256()
    prefix_sha256 = frame_chain.hexdigest() if prefix_count == 0 else None
    for frame in connection.execute("SELECT * FROM frames ORDER BY scheduled_utc"):
        prefix = f"frame:{frame['frame_id']}"
        scheduled = parse_utc(frame["scheduled_utc"])
        read_started = parse_utc(frame["read_started_utc"])
        captured = parse_utc(frame["captured_utc"])
        precommit = parse_utc(frame["precommit_utc"])
        record(failures, scheduled is not None, f"{prefix}:scheduled_clock")
        if scheduled is None:
            continue
        record(
            failures,
            int(scheduled.timestamp()) % CADENCE_SEC == 0,
            f"{prefix}:cadence_alignment",
        )
        record(failures, scheduled >= ACTIVATION_UTC, f"{prefix}:preactivation")
        expected_id = sha(f"{COHORT_ID}|{iso(scheduled)}".encode())
        record(failures, frame["frame_id"] == expected_id, f"{prefix}:id")
        record(failures, frame["cohort_id"] == COHORT_ID, f"{prefix}:cohort")
        record(
            failures,
            frame["scheduled_utc"] not in seen_schedule,
            f"{prefix}:duplicate_schedule",
        )
        seen_schedule.add(frame["scheduled_utc"])
        clocks_ok = (
            read_started is not None
            and captured is not None
            and precommit is not None
            and scheduled <= read_started <= captured <= precommit
            and 0 <= (captured - scheduled).total_seconds() <= MAXIMUM_CAPTURE_DELAY_SEC
            and precommit < scheduled + dt.timedelta(
                seconds=MAXIMUM_CAPTURE_DELAY_SEC
            )
            and precommit <= now + dt.timedelta(seconds=MAXIMUM_FUTURE_SKEW_SEC)
        )
        record(failures, clocks_ok, f"{prefix}:noncausal_clocks")
        raw = bytes(frame["raw_payload"])
        record(failures, sha(raw) == frame["raw_payload_sha256"], f"{prefix}:raw_hash")
        try:
            payload = json.loads(raw)
        except ValueError:
            payload = {}
            failures.append(f"{prefix}:raw_json")
        quotes = payload.get("quotes") if isinstance(payload.get("quotes"), dict) else {}
        source_ok = (
            str(payload.get("schema_version")) == SOURCE_SCHEMA_VERSION
            and str(payload.get("producer")) == SOURCE_PRODUCER
        )
        generated = parse_utc(payload.get("generated_utc"))
        generated_age = (
            (captured - generated).total_seconds()
            if captured is not None and generated is not None else None
        )
        snapshot_ok = (
            generated_age is not None
            and -MAXIMUM_FUTURE_SKEW_SEC <= generated_age
            <= MAXIMUM_SNAPSHOT_GENERATED_AGE_SEC
            and payload.get("quote_count") == len(quotes)
            and not (set(quotes) - set(EXPECTED_INSTRUMENTS))
        )
        record(
            failures,
            set(quotes).issubset(set(EXPECTED_INSTRUMENTS)),
            f"{prefix}:raw_universe",
        )
        record(
            failures,
            payload.get("quote_count") == len(quotes),
            f"{prefix}:raw_quote_count",
        )
        record(
            failures,
            generated_age is not None
            and -MAXIMUM_FUTURE_SKEW_SEC <= generated_age
            <= MAXIMUM_SNAPSHOT_GENERATED_AGE_SEC,
            f"{prefix}:raw_generated_clock",
        )
        record(
            failures,
            frame["capture_contract_id"] == CAPTURE_CONTRACT_ID
            and frame["capture_cohort_id"] == CAPTURE_COHORT_ID
            and frame["source_schema_version"] == SOURCE_SCHEMA_VERSION
            and frame["source_producer"] == SOURCE_PRODUCER,
            f"{prefix}:source_lineage",
        )
        rows = list(
            connection.execute(
                "SELECT * FROM frame_quotes WHERE frame_id=? ORDER BY universe_index",
                (frame["frame_id"],),
            )
        )
        observed_quote_count += len(rows)
        record(failures, len(rows) == 68, f"{prefix}:quote_count")
        observed_by_instrument = {row["instrument"]: row for row in rows}
        record(
            failures,
            set(observed_by_instrument) == set(EXPECTED_INSTRUMENTS),
            f"{prefix}:quote_universe",
        )
        valid = 0
        for index, instrument in enumerate(EXPECTED_INSTRUMENTS):
            row = observed_by_instrument.get(instrument)
            if row is None:
                continue
            qprefix = f"{prefix}:quote:{instrument}"
            record(failures, row["universe_index"] == index, f"{qprefix}:index")
            record(
                failures,
                row["capture_contract_id"] == CAPTURE_CONTRACT_ID
                and row["capture_cohort_id"] == CAPTURE_COHORT_ID,
                f"{qprefix}:lineage",
            )
            expected = _expected_quote(
                instrument,
                quotes.get(instrument) if isinstance(quotes.get(instrument), dict) else {},
                captured or scheduled,
                scheduled,
                source_ok,
                snapshot_ok,
            )
            for key in ("state", "reason", "quote_utc", "component_sha256"):
                record(failures, row[key] == expected[key], f"{qprefix}:{key}")
            for key in ("bid", "ask", "pip", "quote_age_sec"):
                if expected[key] is None:
                    good = row[key] is None
                else:
                    good = close(row[key], expected[key], 1e-9)
                record(failures, good, f"{qprefix}:{key}")
            record(
                failures,
                bytes(row["component_bytes"]) == expected["component_bytes"],
                f"{qprefix}:component_bytes",
            )
            valid += int(expected["state"] == "valid")
        expected_state = (
            "valid" if valid == 68 else (
                "excluded_closed" if not market_open(scheduled) else "partial_invalid"
            )
        )
        expected_reason = (
            "" if valid == 68 else (
                "market_closed" if not market_open(scheduled)
                else "one_or_more_quote_rows_invalid"
            )
        )
        record(failures, frame["expected_count"] == 68, f"{prefix}:expected_count")
        record(failures, frame["valid_count"] == valid, f"{prefix}:valid_count")
        record(failures, frame["invalid_count"] == 68 - valid, f"{prefix}:invalid_count")
        record(failures, frame["state"] == expected_state, f"{prefix}:state")
        record(failures, frame["reason"] == expected_reason, f"{prefix}:reason")
        record(
            failures,
            frame["market_open"] == int(market_open(scheduled)),
            f"{prefix}:market_open",
        )
        if retain_quote_after is None or scheduled >= retain_quote_after:
            quote_cache[frame["frame_id"]] = observed_by_instrument

        receipt = receipts.get(frame["frame_id"])
        record(failures, receipt is not None, f"{prefix}:commit_receipt_missing")
        if receipt is not None:
            committed = parse_utc(receipt["committed_utc"])
            expected_within = int(
                committed is not None
                and committed < scheduled + dt.timedelta(seconds=CADENCE_SEC)
            )
            record(
                failures,
                committed is not None and precommit is not None
                and committed >= precommit
                and committed <= now + dt.timedelta(
                    seconds=MAXIMUM_FUTURE_SKEW_SEC
                ),
                f"{prefix}:commit_receipt_clock",
            )
            record(
                failures,
                receipt["within_first_horizon"] == expected_within,
                f"{prefix}:commit_receipt_flag",
            )
        frame_material = {
            "frame": {
                key: frame[key]
                for key in TABLE_COLUMNS["frames"] if key != "raw_payload"
            },
            "quotes": [
                {
                    key: row[key]
                    for key in TABLE_COLUMNS["frame_quotes"]
                    if key != "component_bytes"
                }
                for row in rows
            ],
            "receipt": dict(receipt) if receipt is not None else None,
        }
        frame_chain.update(canonical(frame_material) + b"\n")
        processed_count += 1
        if processed_count == prefix_count:
            prefix_sha256 = frame_chain.hexdigest()
        frames.append({
            "frame_id": frame["frame_id"],
            "scheduled_utc": frame["scheduled_utc"],
        })

    frame_ids = {row["frame_id"] for row in frames}
    for receipt_id in receipts:
        record(
            failures, receipt_id in frame_ids,
            f"receipt:{receipt_id}:orphan",
        )

    # A missing scheduled market-open minute is not repaired by selecting an
    # older completed window.  It is an explicit integrity/coverage failure.
    eligible = [
        row for row in frames
        if row["frame_id"] in receipts
        and receipts[row["frame_id"]]["within_first_horizon"] == 1
    ]
    if eligible:
        newest = parse_utc(eligible[-1]["scheduled_utc"])
        if newest is not None:
            cursor = ACTIVATION_UTC
            observed = {row["scheduled_utc"] for row in eligible}
            while cursor <= newest:
                if market_open(cursor):
                    record(
                        failures,
                        iso(cursor) in observed,
                        f"schedule:missing_open_minute:{iso(cursor)}",
                    )
                cursor += dt.timedelta(seconds=CADENCE_SEC)
    return (
        frames, eligible, quote_cache, observed_quote_count, prefix_sha256,
        frame_chain.hexdigest(),
    )


def terminal_row(
    entry: sqlite3.Row,
    exit_row: sqlite3.Row | None,
    entry_quote: sqlite3.Row,
    exit_quote: sqlite3.Row | None,
    side: str,
    horizon: int,
) -> dict[str, Any]:
    base = {
        "instrument": entry_quote["instrument"],
        "side": side,
        "horizon_min": horizon,
        "entry_frame_id": entry["frame_id"],
        "exit_frame_id": exit_row["frame_id"] if exit_row else None,
        "entry_scheduled_utc": entry["scheduled_utc"],
        "exit_scheduled_utc": exit_row["scheduled_utc"] if exit_row else None,
        "entry_quote_utc": entry_quote["quote_utc"],
        "exit_quote_utc": exit_quote["quote_utc"] if exit_quote else None,
        "entry_bid": entry_quote["bid"],
        "entry_ask": entry_quote["ask"],
        "exit_bid": exit_quote["bid"] if exit_quote else None,
        "exit_ask": exit_quote["ask"] if exit_quote else None,
        "pip": entry_quote["pip"],
        "slippage_pips": SLIPPAGE_PIPS,
        "slippage_semantics": SLIPPAGE_SEMANTICS,
        "raw_executable_pips": None,
        "net_pips": None,
        "net_bps": None,
        "state": "pending",
        "reason": "exit_frame_pending",
    }
    if exit_row is None:
        return base
    if entry_quote["state"] != "valid" or exit_quote is None or exit_quote["state"] != "valid":
        reason = (
            f"entry_{entry_quote['state']}"
            if entry_quote["state"] != "valid"
            else f"exit_{exit_quote['state'] if exit_quote else 'missing'}"
        )
        base.update(state="invalid", reason=reason)
        return base
    if not close(entry_quote["pip"], exit_quote["pip"], 1e-12):
        base.update(state="invalid", reason="pip_mismatch")
        return base
    entry_target = parse_utc(entry["scheduled_utc"])
    exit_target = parse_utc(exit_row["scheduled_utc"])
    entry_clock = parse_utc(entry_quote["quote_utc"])
    exit_clock = parse_utc(exit_quote["quote_utc"])
    if (
        entry_target is None or entry_clock is None
        or abs((entry_clock - entry_target).total_seconds()) > MAXIMUM_TARGET_OFFSET_SEC
    ):
        base.update(state="invalid", reason="entry_quote_target_offset")
        return base
    if (
        exit_target is None or exit_clock is None
        or abs((exit_clock - exit_target).total_seconds()) > MAXIMUM_TARGET_OFFSET_SEC
    ):
        base.update(state="invalid", reason="exit_quote_target_offset")
        return base
    pip = float(entry_quote["pip"])
    if side == "long":
        price = float(exit_quote["bid"]) - float(entry_quote["ask"])
    else:
        price = float(entry_quote["bid"]) - float(exit_quote["ask"])
    raw = price / pip
    net = raw - SLIPPAGE_PIPS
    entry_mid = (float(entry_quote["bid"]) + float(entry_quote["ask"])) / 2.0
    base.update(
        raw_executable_pips=round(raw, 6),
        net_pips=round(net, 6),
        net_bps=round(net * pip / entry_mid * 10000.0, 6),
        state="cleared" if net > 0.0 else "not_cleared",
        reason="",
    )
    return base


def _build_block(
    frames: Sequence[sqlite3.Row],
    quote_cache: Mapping[str, Mapping[str, sqlite3.Row]],
    target: dt.datetime,
    horizon: int,
    *,
    terminal_missing_is_final: bool = False,
    frame_by_time: Mapping[str, sqlite3.Row] | None = None,
) -> dict[str, Any]:
    by_time = frame_by_time or {
        row["scheduled_utc"]: row for row in frames
    }
    terminal = target.replace(second=0, microsecond=0)
    entry = by_time.get(iso(terminal - dt.timedelta(minutes=horizon)))
    exit_row = by_time.get(iso(terminal))
    entry_quotes = quote_cache.get(entry["frame_id"], {}) if entry else {}
    exit_quotes = quote_cache.get(exit_row["frame_id"], {}) if exit_row else {}
    rows: list[dict[str, Any]] = []
    for instrument in EXPECTED_INSTRUMENTS:
        entry_quote = entry_quotes.get(instrument)
        for side in ("long", "short"):
            if entry_quote is None:
                rows.append({
                    "instrument": instrument, "side": side,
                    "horizon_min": horizon, "state": "invalid",
                    "reason": "no_entry_frame", "net_pips": None,
                    "net_bps": None,
                })
            else:
                rows.append(terminal_row(
                    entry, exit_row, entry_quote,
                    exit_quotes.get(instrument), side, horizon,
                ))

    path_frames: list[tuple[int, sqlite3.Row | None]] = []
    if entry is not None:
        entry_clock = parse_utc(entry["scheduled_utc"])
        if entry_clock is not None:
            for offset in range(1, horizon + 1):
                path_frames.append((
                    offset,
                    by_time.get(iso(entry_clock + dt.timedelta(minutes=offset))),
                ))
    path_quote_cache = {
        frame["frame_id"]: quote_cache.get(frame["frame_id"], {})
        for _, frame in path_frames if frame is not None
    }
    for row in rows:
        entry_quote = entry_quotes.get(row["instrument"])
        points: list[dict[str, Any]] = []
        if entry_quote is not None and entry_quote["state"] == "valid":
            for offset, frame in path_frames:
                if frame is None:
                    continue
                sample = terminal_row(
                    entry, frame, entry_quote,
                    path_quote_cache[frame["frame_id"]].get(row["instrument"]),
                    row["side"], offset,
                )
                if sample["net_pips"] is not None:
                    points.append({
                        "minute": offset,
                        "frame_id": frame["frame_id"],
                        "scheduled_utc": frame["scheduled_utc"],
                        "net_pips": sample["net_pips"],
                        "net_bps": sample["net_bps"],
                    })
        row["path_state"] = "complete" if len(points) == horizon else "incomplete"
        row["path_points"] = points
        row["first_clear_min"] = next(
            (point["minute"] for point in points if point["net_pips"] > 0), None
        )
        if points:
            maximum = max(points, key=lambda point: (point["net_pips"], -point["minute"]))
            minimum = min(points, key=lambda point: (point["net_pips"], point["minute"]))
            row.update(
                path_max_net_pips=maximum["net_pips"],
                path_max_net_bps=maximum["net_bps"],
                path_max_frame_id=maximum["frame_id"],
                path_max_scheduled_utc=maximum["scheduled_utc"],
                path_min_net_pips=minimum["net_pips"],
                path_cleared=maximum["net_pips"] > 0,
            )
        else:
            row.update(
                path_max_net_pips=None, path_max_net_bps=None,
                path_max_frame_id=None, path_max_scheduled_utc=None,
                path_min_net_pips=None, path_cleared=False,
            )
    if terminal_missing_is_final and exit_row is None:
        for row in rows:
            row.update(
                state="invalid", reason="scheduled_exit_frame_missing",
                net_pips=None, net_bps=None,
            )
    cleared = [row for row in rows if row["state"] == "cleared"]
    return {
        "horizon_min": horizon,
        "entry_frame_id": entry["frame_id"] if entry else None,
        "exit_frame_id": exit_row["frame_id"] if exit_row else None,
        "expected_side_count": 136,
        "row_count": len(rows),
        "valid_count": sum(
            row["state"] in ("cleared", "not_cleared") for row in rows
        ),
        "cleared_count": len(cleared),
        "path_cleared_count": sum(row["path_cleared"] is True for row in rows),
        "invalid_count": sum(row["state"] == "invalid" for row in rows),
        "pending_count": sum(row["state"] == "pending" for row in rows),
        "rows": rows,
    }


def _rebuild_blocks(
    frames: Sequence[sqlite3.Row],
    quote_cache: Mapping[str, Mapping[str, sqlite3.Row]],
    generated: dt.datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_time = {row["scheduled_utc"]: row for row in frames}
    blocks = [
        _build_block(
            frames, quote_cache, generated, horizon, frame_by_time=by_time
        )
        for horizon in HORIZONS_MIN
    ]
    top = [
        row for block in blocks for row in block["rows"]
        if row["state"] == "cleared"
    ]
    top.sort(key=lambda row: (
        -float(row["net_bps"]), -float(row["net_pips"]),
        row["instrument"], row["side"], row["horizon_min"],
    ))
    return blocks, top


def _window_digests(block: Mapping[str, Any]) -> tuple[str, str, str, str]:
    terminal: list[dict[str, Any]] = []
    path: list[dict[str, Any]] = []
    terminal_ids: list[str] = []
    path_ids: list[str] = []
    for row in block.get("rows") or []:
        identity = {
            "instrument": row["instrument"], "side": row["side"],
            "declared_window_min": block["horizon_min"],
            "entry_frame_id": row.get("entry_frame_id"),
            "exit_frame_id": row.get("exit_frame_id"),
        }
        terminal.append({
            **identity, "state": row.get("state"), "reason": row.get("reason"),
            "net_pips": row.get("net_pips"), "net_bps": row.get("net_bps"),
        })
        path.append({
            **identity, "path_state": row.get("path_state"),
            "first_clear_min": row.get("first_clear_min"),
            "path_max_net_pips": row.get("path_max_net_pips"),
            "path_max_net_bps": row.get("path_max_net_bps"),
            "path_max_frame_id": row.get("path_max_frame_id"),
            "path_min_net_pips": row.get("path_min_net_pips"),
            "path_cleared": row.get("path_cleared"),
        })
        if row.get("state") == "cleared":
            terminal_ids.append(sha(canonical({"kind": "terminal", **identity})))
        if row.get("path_cleared"):
            path_ids.append(sha(canonical({
                "kind": "path_peak", "instrument": row["instrument"],
                "side": row["side"],
                "entry_frame_id": row.get("entry_frame_id"),
                "exit_frame_id": row.get("path_max_frame_id"),
            })))
    return (
        sha(canonical(terminal)), sha(canonical(path)),
        sha(canonical(sorted(terminal_ids))),
        sha(canonical(sorted(set(path_ids)))),
    )


def _expected_evaluation_keys(cutoff: dt.datetime) -> list[tuple[str, int]]:
    result: list[tuple[str, int]] = []
    target = ACTIVATION_UTC.replace(second=0, microsecond=0)
    while target <= cutoff:
        for horizon in HORIZONS_MIN:
            entry = target - dt.timedelta(minutes=horizon)
            if entry >= ACTIVATION_UTC and market_open(entry):
                result.append((iso(target), horizon))
        target += dt.timedelta(minutes=1)
    return result


def _verify_evaluation_row(
    row: Mapping[str, Any] | None,
    block: Mapping[str, Any],
    target: dt.datetime,
    horizon: int,
    generated: dt.datetime,
    failures: list[str],
) -> None:
    target_text = iso(target)
    if row is None:
        return
    prefix = f"evaluation:{target_text}:{horizon}"
    evaluated = parse_utc(row["evaluated_utc"])
    record(
        failures,
        evaluated is not None
        and target + dt.timedelta(minutes=1) <= evaluated <= generated,
        f"{prefix}:evaluated_clock",
    )
    digests = _window_digests(block)
    expected = {
        "target_scheduled_utc": target_text,
        "declared_window_min": horizon,
        "entry_frame_id": block["entry_frame_id"],
        "exit_frame_id": block["exit_frame_id"],
        "state": (
            "evaluated" if block["entry_frame_id"] and block["exit_frame_id"]
            else "terminal_invalid_missing_frame"
        ),
        "expected_side_count": 136,
        "valid_count": block["valid_count"],
        "cleared_count": block["cleared_count"],
        "path_cleared_count": block["path_cleared_count"],
        "invalid_count": block["invalid_count"],
        "pending_count": block["pending_count"],
        "terminal_rows_sha256": digests[0],
        "path_summary_rows_sha256": digests[1],
        "terminal_clear_ids_sha256": digests[2],
        "path_clear_ids_sha256": digests[3],
    }
    for key, value in expected.items():
        record(failures, row.get(key) == value, f"{prefix}:{key}")
    record(
        failures,
        row["valid_count"] + row["invalid_count"] + row["pending_count"] == 136
        and 0 <= row["cleared_count"] <= row["valid_count"],
        f"{prefix}:count_partition",
    )


def _factor_contract_for_episode(
    episode: dt.datetime,
    block_cache: Mapping[tuple[str, int], Mapping[str, Any]],
) -> tuple[
    list[tuple[str, int]], dict[tuple[str, int, str], dict[str, Any]],
    list[str], list[str],
]:
    expected: list[tuple[str, int]] = []
    groups: dict[tuple[str, int, str], dict[str, Any]] = {}
    for minute in range(15):
        entry = episode + dt.timedelta(minutes=minute)
        if entry < ACTIVATION_UTC or not market_open(entry):
            continue
        for horizon in HORIZONS_MIN:
            key = (iso(entry + dt.timedelta(minutes=horizon)), horizon)
            expected.append(key)
            block = block_cache.get(key)
            if block is None:
                continue
            for row in block["rows"]:
                candidates: list[tuple[str, Any, Any, Any, Any]] = []
                if row.get("state") == "cleared":
                    candidates.append((
                        "terminal", row.get("exit_frame_id"), horizon,
                        row.get("net_pips"), row.get("net_bps"),
                    ))
                if row.get("path_cleared"):
                    offset = next((
                        point.get("minute") for point in row.get("path_points") or []
                        if point.get("frame_id") == row.get("path_max_frame_id")
                    ), None)
                    candidates.append((
                        "path_peak", row.get("path_max_frame_id"), offset,
                        row.get("path_max_net_pips"), row.get("path_max_net_bps"),
                    ))
                base, quote = row["instrument"].split("_", 1)
                base_sign = 1 if row["side"] == "long" else -1
                for kind, exit_id, offset, net_pips, net_bps in candidates:
                    member_id = sha(canonical({
                        "kind": kind, "entry": row.get("entry_frame_id"),
                        "exit": exit_id, "instrument": row["instrument"],
                        "side": row["side"],
                    }))
                    representative = {
                        "member_id": member_id, "instrument": row["instrument"],
                        "side": row["side"], "declared_window_min": horizon,
                        "actual_exit_offset_min": offset,
                        "entry_frame_id": row.get("entry_frame_id"),
                        "exit_frame_id": exit_id, "net_pips": net_pips,
                        "net_bps": net_bps,
                    }
                    for currency, sign in ((base, base_sign), (quote, -base_sign)):
                        group = groups.setdefault(
                            (currency, sign, kind),
                            {"ids": set(), "representative": representative},
                        )
                        group["ids"].add(member_id)
                        incumbent = group["representative"]
                        if (
                            representative["net_bps"], representative["net_pips"],
                            representative["member_id"],
                        ) > (
                            incumbent["net_bps"], incumbent["net_pips"],
                            incumbent["member_id"],
                        ):
                            group["representative"] = representative
    terminal_ids = sorted({
        member for (_, _, kind), group in groups.items() if kind == "terminal"
        for member in group["ids"]
    })
    path_ids = sorted({
        member for (_, _, kind), group in groups.items() if kind == "path_peak"
        for member in group["ids"]
    })
    return expected, groups, terminal_ids, path_ids


def _verify_evidence_streaming(
    connection: sqlite3.Connection,
    frames: Sequence[sqlite3.Row],
    quote_cache: Mapping[str, Mapping[str, sqlite3.Row]],
    generated: dt.datetime,
    failures: list[str],
    runtime_cache: dict[str, Any] | None = None,
) -> tuple[
    list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]],
]:
    evaluations = [dict(row) for row in connection.execute(
        "SELECT * FROM window_evaluations "
        "ORDER BY target_scheduled_utc,declared_window_min"
    )]
    observed_evaluations = {
        (row["target_scheduled_utc"], int(row["declared_window_min"])): row
        for row in evaluations
    }
    cutoff = generated.replace(second=0, microsecond=0) - dt.timedelta(minutes=1)
    expected_keys = _expected_evaluation_keys(cutoff)
    expected_key_set = set(expected_keys)
    cached_evaluations: Mapping[tuple[str, int], str] = (
        runtime_cache.get("evaluation_rows", {}) if runtime_cache is not None
        else {}
    )
    record(
        failures,
        set(cached_evaluations).issubset(expected_key_set),
        "runtime_cache:evaluation_prefix_rollback",
    )
    next_evaluation_cache = {
        key: sha(canonical(observed_evaluations[key]))
        for key in expected_keys if key in observed_evaluations
    }
    record(
        failures, set(observed_evaluations) == expected_key_set,
        "evaluations:scheduled_key_coverage",
    )
    summaries = [dict(row) for row in connection.execute(
        "SELECT * FROM factor_episode_summaries"
    )]
    finalizations = [dict(row) for row in connection.execute(
        "SELECT * FROM factor_episode_finalizations"
    )]
    observed_summaries = {
        (
            row["episode_utc"], row["factor_currency"],
            int(row["factor_sign"]), row["event_kind"],
        ): row for row in summaries
    }
    observed_final = {row["episode_utc"]: row for row in finalizations}
    summaries_by_episode: dict[str, list[dict[str, Any]]] = {}
    for row in summaries:
        summaries_by_episode.setdefault(row["episode_utc"], []).append(row)
    observed_episode_hashes = {
        episode_text: sha(canonical({
            "finalization": observed_final.get(episode_text),
            "summaries": sorted(
                rows,
                key=lambda value: (
                    value["factor_currency"], int(value["factor_sign"]),
                    value["event_kind"],
                ),
            ),
        }))
        for episode_text, rows in summaries_by_episode.items()
    }
    for episode_text, final in observed_final.items():
        observed_episode_hashes.setdefault(
            episode_text,
            sha(canonical({"finalization": final, "summaries": []})),
        )
    cached_episodes: Mapping[str, str] = (
        runtime_cache.get("factor_episodes", {}) if runtime_cache is not None
        else {}
    )
    expected_summary_keys: set[tuple[str, str, int, str]] = set()
    expected_episodes: list[str] = []
    by_time = {row["scheduled_utc"]: row for row in frames}
    activation_episode = dt.datetime.fromtimestamp(
        int(ACTIVATION_UTC.timestamp() // 900) * 900, UTC
    )
    last_candidates: list[dt.datetime] = []
    last_entry = cutoff - dt.timedelta(minutes=min(HORIZONS_MIN))
    if last_entry >= ACTIVATION_UTC:
        last_candidates.append(last_entry)
    last_finalizable = generated - dt.timedelta(minutes=75)
    if last_finalizable >= activation_episode:
        last_candidates.append(last_finalizable)
    if not last_candidates:
        record(
            failures, not observed_final,
            "factor_finalizations:episode_coverage",
        )
        record(
            failures, not observed_summaries,
            "factor_summaries:exact_row_set",
        )
        if runtime_cache is not None and not failures:
            runtime_cache["evaluation_rows"] = next_evaluation_cache
            runtime_cache["factor_episodes"] = {}
            runtime_cache["last_generated_utc"] = iso(generated)
        return evaluations, summaries, finalizations
    last_episode = dt.datetime.fromtimestamp(
        int(max(last_candidates).timestamp() // 900) * 900, UTC
    )
    episode = dt.datetime.fromtimestamp(
        int(ACTIVATION_UTC.timestamp() // 900) * 900, UTC
    )
    finalizable_episode_set: set[str] = set()
    while episode <= last_episode:
        block_cache: dict[tuple[str, int], dict[str, Any]] = {}
        episode_text = iso(episode)
        factor_is_finalizable = episode + dt.timedelta(minutes=75) <= generated
        if factor_is_finalizable:
            finalizable_episode_set.add(episode_text)
        verify_factor = (
            factor_is_finalizable
            and (
                runtime_cache is None
                or cached_episodes.get(episode_text)
                != observed_episode_hashes.get(episode_text)
            )
        )
        for minute in range(15):
            entry = episode + dt.timedelta(minutes=minute)
            if entry < ACTIVATION_UTC or not market_open(entry):
                continue
            for horizon in HORIZONS_MIN:
                target = entry + dt.timedelta(minutes=horizon)
                key = (iso(target), horizon)
                if key not in expected_key_set:
                    continue
                observed_row = observed_evaluations.get(key)
                observed_hash = (
                    sha(canonical(observed_row)) if observed_row is not None else None
                )
                verify_evaluation = (
                    runtime_cache is None
                    or cached_evaluations.get(key) != observed_hash
                )
                if not verify_evaluation and not verify_factor:
                    continue
                block = _build_block(
                    frames, quote_cache, target, horizon,
                    terminal_missing_is_final=True,
                    frame_by_time=by_time,
                )
                if verify_factor:
                    block_cache[key] = block
                if verify_evaluation:
                    _verify_evaluation_row(
                        observed_row, block, target, horizon,
                        generated, failures,
                    )
        if not factor_is_finalizable:
            episode += dt.timedelta(minutes=15)
            continue
        expected_episodes.append(episode_text)
        if not verify_factor:
            expected_summary_keys.update(
                key for key in observed_summaries if key[0] == episode_text
            )
            episode += dt.timedelta(minutes=15)
            continue
        expected, groups, terminal_ids, path_ids = _factor_contract_for_episode(
            episode, block_cache
        )
        final = observed_final.get(episode_text)
        if final is not None:
            prefix = f"factor_finalization:{episode_text}"
            finalized = parse_utc(final["finalized_utc"])
            evaluation_clocks = [
                parse_utc(observed_evaluations[key]["evaluated_utc"])
                for key in expected if key in observed_evaluations
            ]
            maximum_evaluated = (
                episode + dt.timedelta(minutes=75)
                if not expected else (
                    max(value for value in evaluation_clocks if value is not None)
                    if len(evaluation_clocks) == len(expected)
                    and all(value is not None for value in evaluation_clocks)
                    else None
                )
            )
            expected_final = {
                "episode_utc": episode_text,
                "expected_window_count": len(expected),
                "evaluated_window_count": len(expected),
                "summary_count": len(groups),
                "evaluation_keys_sha256": sha(canonical(sorted(expected))),
                "terminal_member_count": len(terminal_ids),
                "terminal_member_ids_sha256": sha(canonical(terminal_ids)),
                "path_member_count": len(path_ids),
                "path_member_ids_sha256": sha(canonical(path_ids)),
            }
            record(
                failures,
                finalized is not None
                and episode + dt.timedelta(minutes=75) <= finalized <= generated
                and maximum_evaluated is not None
                and finalized >= maximum_evaluated,
                f"{prefix}:clock",
            )
            for key, value in expected_final.items():
                record(failures, final.get(key) == value, f"{prefix}:{key}")
        for (currency, sign, kind), group in groups.items():
            key = (episode_text, currency, sign, kind)
            expected_summary_keys.add(key)
            row = observed_summaries.get(key)
            if row is None:
                continue
            ids = sorted(group["ids"])
            expected_row = {
                "episode_utc": episode_text,
                "factor_currency": currency,
                "factor_sign": sign,
                "event_kind": kind,
                "member_count": len(ids),
                "member_ids_sha256": sha(canonical(ids)),
                "representative_json": canonical(group["representative"]).decode(),
            }
            for field, value in expected_row.items():
                record(
                    failures, row.get(field) == value,
                    f"factor_summary:{episode_text}:{currency}:{sign}:{kind}:{field}",
                )
            finalized = parse_utc(row["finalized_utc"])
            record(
                failures,
                finalized is not None
                and episode + dt.timedelta(minutes=75) <= finalized <= generated
                and final is not None
                and row["finalized_utc"] == final["finalized_utc"],
                f"factor_summary:{episode_text}:{currency}:{sign}:{kind}:clock",
            )
        episode += dt.timedelta(minutes=15)
    record(
        failures, set(observed_final) == set(expected_episodes),
        "factor_finalizations:episode_coverage",
    )
    record(
        failures, set(observed_summaries) == expected_summary_keys,
        "factor_summaries:exact_row_set",
    )
    record(
        failures,
        set(cached_episodes).issubset(finalizable_episode_set),
        "runtime_cache:factor_prefix_rollback",
    )
    if runtime_cache is not None and not failures:
        runtime_cache["evaluation_rows"] = next_evaluation_cache
        runtime_cache["factor_episodes"] = {
            key: observed_episode_hashes[key]
            for key in expected_episodes if key in observed_episode_hashes
        }
        runtime_cache["last_generated_utc"] = iso(generated)
    return evaluations, summaries, finalizations


def _review_queue(
    evaluations: Sequence[Mapping[str, Any]],
    summaries: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    representatives = []
    for row in summaries:
        try:
            representative = json.loads(row["representative_json"])
        except (TypeError, ValueError):
            representative = {}
        representatives.append({
            "case_id": sha(
                f"{COHORT_ID}|{row['episode_utc']}|{row['factor_currency']}|"
                f"{row['factor_sign']}|{row['event_kind']}".encode()
            ),
            "episode_utc": row["episode_utc"],
            "factor_currency": row["factor_currency"],
            "factor_sign": row["factor_sign"],
            "event_kind": row["event_kind"],
            "member_count": row["member_count"],
            "member_ids_sha256": row["member_ids_sha256"],
            **representative,
        })
    representatives.sort(key=lambda row: (
        -float(row["net_bps"]), -float(row["net_pips"]), row["case_id"],
    ))
    terminal = sum(int(row["cleared_count"]) for row in evaluations)
    path = sum(int(row["path_cleared_count"]) for row in evaluations)
    return {
        "total_terminal_cleared_arms": terminal,
        "total_path_cleared_arms": path,
        "total_cleared_arm_observations": terminal + path,
        "evaluated_windows": len(evaluations),
        "distinct_factor_episode_cases": len(summaries),
        "storage_contract": (
            "all 136 arms reconstructible from immutable frames and per-window "
            "terminal/path/clear digests; compact factor summaries finalize after "
            "episode end plus 60 minutes"
        ),
        "dedupe_definition": (
            "entry-time 15-minute episode x signed currency factor x event_kind"
        ),
        "representatives": representatives[:50],
    }


def _verify_latest(
    path: Path,
    connection: sqlite3.Connection,
    frames: Sequence[sqlite3.Row],
    quote_cache: Mapping[str, Mapping[str, sqlite3.Row]],
    now: dt.datetime,
    failures: list[str],
    runtime_cache: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    try:
        observed = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        failures.append(f"latest:unreadable:{type(exc).__name__}")
        return {}, [], [], []
    content_hash = observed.get("content_sha256_excluding_this_field")
    unhashed = dict(observed)
    unhashed.pop("content_sha256_excluding_this_field", None)
    record(failures, content_hash == sha(canonical(unhashed)), "latest:content_sha256")
    generated = parse_utc(observed.get("generated_utc"))
    record(failures, generated is not None, "latest:generated_clock")
    if generated is None:
        return observed, [], [], []
    cached_generated = parse_utc(
        runtime_cache.get("last_generated_utc")
        if runtime_cache is not None else None
    )
    record(
        failures,
        cached_generated is None or generated >= cached_generated,
        "runtime_cache:latest_generated_rollback",
    )
    age = (now - generated).total_seconds()
    record(
        failures,
        -MAXIMUM_FUTURE_SKEW_SEC <= age <= MAXIMUM_LATEST_AGE_SEC,
        "latest:freshness",
    )
    evaluations, summaries, finalizations = _verify_evidence_streaming(
        connection, frames, quote_cache, generated, failures, runtime_cache
    )
    blocks, top = _rebuild_blocks(frames, quote_cache, generated)
    newest = frames[-1] if frames else None
    expected_open: list[str] = []
    cursor = ACTIVATION_UTC.replace(second=0, microsecond=0)
    end = generated.replace(second=0, microsecond=0)
    while cursor <= end:
        if market_open(cursor):
            expected_open.append(iso(cursor))
        cursor += dt.timedelta(minutes=1)
    observed_schedules = {row["scheduled_utc"] for row in frames}
    missing = [value for value in expected_open if value not in observed_schedules]
    record(
        failures, not missing,
        "latest:schedule_census:missing_open_frames",
    )
    expected = {
        "schema_version": "executable_move_census_latest_v1",
        "generated_utc": iso(generated),
        "cohort_id": COHORT_ID,
        "research_only": True,
        "can_trade": False,
        "can_authorize": False,
        "can_promote": False,
        "definition": (
            "A move exists only when a later executable exit produces net pips > 0 "
            "after one frozen round-trip slippage deduction; spread is already "
            "embedded in bid/ask endpoints."
        ),
        "measurement_scope": (
            "terminal fixed-window net is primary; descriptive hindsight path "
            "fields report first clear, maximum favorable executable net, and "
            "minimum executable net from predeclared causal minute frames"
        ),
        "instrument_count": 68,
        "side_count": 136,
        "horizons_min": list(HORIZONS_MIN),
        "slippage_pips": SLIPPAGE_PIPS,
        "frame_count": len(frames),
        "latest_frame_utc": newest["scheduled_utc"] if newest else None,
        "schedule_census": {
            "expected_open_frames": len(expected_open),
            "observed_frames": sum(value in observed_schedules for value in expected_open),
            "missing_open_frames": len(missing),
            "recent_missing_scheduled_utc": missing[-120:],
        },
        "horizons": blocks,
        "top_cleared_paths": top[:50],
        "frame_inserted": observed.get("frame_inserted"),
        "review_queue": _review_queue(evaluations, summaries),
    }
    record(
        failures,
        set(observed) == set(expected) | {"content_sha256_excluding_this_field"},
        "latest:top_level_contract",
    )
    for key, value in expected.items():
        record(failures, observed.get(key) == value, f"latest:{key}:mismatch")
    record(
        failures,
        isinstance(observed.get("frame_inserted"), bool),
        "latest:frame_inserted:type",
    )
    return observed, evaluations, summaries, finalizations


def _quote_retention_start(
    connection: sqlite3.Connection,
    runtime_cache: Mapping[str, Any] | None,
    now: dt.datetime,
) -> dt.datetime | None:
    if runtime_cache is None or "evaluation_rows" not in runtime_cache:
        return None
    cached_evaluations = dict(runtime_cache.get("evaluation_rows") or {})
    evaluation_rows = [
        dict(row) for row in connection.execute(
            "SELECT * FROM window_evaluations "
            "ORDER BY target_scheduled_utc,declared_window_min"
        )
    ]
    observed_evaluations = {
        (row["target_scheduled_utc"], int(row["declared_window_min"])):
        sha(canonical(row))
        for row in evaluation_rows
    }
    if any(
        observed_evaluations.get(key) != value
        for key, value in cached_evaluations.items()
    ):
        return None
    candidates = [now - dt.timedelta(minutes=max(HORIZONS_MIN) + 3)]
    for row in evaluation_rows:
        key = (row["target_scheduled_utc"], int(row["declared_window_min"]))
        if key in cached_evaluations:
            continue
        target = parse_utc(row["target_scheduled_utc"])
        if target is not None:
            candidates.append(
                target - dt.timedelta(minutes=int(row["declared_window_min"]))
            )

    summaries = [dict(row) for row in connection.execute(
        "SELECT * FROM factor_episode_summaries"
    )]
    finalizations = [dict(row) for row in connection.execute(
        "SELECT * FROM factor_episode_finalizations"
    )]
    summaries_by_episode: dict[str, list[dict[str, Any]]] = {}
    for row in summaries:
        summaries_by_episode.setdefault(row["episode_utc"], []).append(row)
    final_by_episode = {row["episode_utc"]: row for row in finalizations}
    observed_episode_hashes = {
        episode_text: sha(canonical({
            "finalization": final_by_episode.get(episode_text),
            "summaries": sorted(
                rows,
                key=lambda value: (
                    value["factor_currency"], int(value["factor_sign"]),
                    value["event_kind"],
                ),
            ),
        }))
        for episode_text, rows in summaries_by_episode.items()
    }
    for episode_text, final in final_by_episode.items():
        observed_episode_hashes.setdefault(
            episode_text,
            sha(canonical({"finalization": final, "summaries": []})),
        )
    cached_episodes = dict(runtime_cache.get("factor_episodes") or {})
    if any(
        observed_episode_hashes.get(key) != value
        for key, value in cached_episodes.items()
    ):
        return None
    for episode_text in observed_episode_hashes:
        if episode_text not in cached_episodes:
            episode = parse_utc(episode_text)
            if episode is not None:
                candidates.append(episode)
    return max(
        ACTIVATION_UTC,
        min(candidates).replace(second=0, microsecond=0),
    )


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
    failures: list[str] = []
    verification_now = (now or dt.datetime.now(UTC)).astimezone(UTC)
    cache_database = str(database_path.resolve())
    cached_database = (
        runtime_cache.get("database_path") if runtime_cache is not None else None
    )
    record(
        failures,
        cached_database is None or cached_database == cache_database,
        "runtime_cache:database_identity",
    )
    cached_frame_count = int(
        runtime_cache.get("frame_count", 0)
        if runtime_cache is not None else 0
    )
    config_raw = _read_config(config_path, failures)
    producer_raw = _scan_no_execution_surface(producer_path, failures)
    source_raw = _read_source_identity(
        source_producer_path, FROZEN_SOURCE_PRODUCER_SHA256,
        "source_producer", failures,
    )
    quote_transport_raw = _read_source_identity(
        quote_transport_path, FROZEN_QUOTE_TRANSPORT_SHA256,
        "quote_transport", failures,
    )
    counts = {
        "frames": 0, "frame_quotes": 0, "logical_sides_per_frame": 136,
        "eligible_frames": 0, "window_evaluations": 0,
        "factor_episode_summaries": 0, "factor_episode_finalizations": 0,
    }
    latest: dict[str, Any] = {}
    observed_frame_chain: str | None = None
    observed_frame_count = 0
    if verification_now < ACTIVATION_UTC:
        # Before the frozen activation, absence of the evidence database is the
        # required no-backfill state.  Verify the producer's exact, fresh
        # pre-activation receipt instead of misclassifying that absence as an
        # evidence outage.  At and after activation the normal database path is
        # mandatory and remains fail-closed.
        record(failures, not database_path.exists(), "preactivation:database_exists")
        record(failures, cached_frame_count == 0, "preactivation:cached_frames")
        try:
            latest = json.loads(latest_path.read_bytes())
        except (OSError, ValueError) as exc:
            failures.append(f"preactivation:latest_unreadable:{type(exc).__name__}")
            latest = {}
        generated = parse_utc(latest.get("generated_utc"))
        record(failures, generated is not None, "preactivation:generated_clock")
        if generated is not None:
            age = (verification_now - generated).total_seconds()
            record(
                failures,
                -MAXIMUM_FUTURE_SKEW_SEC <= age <= MAXIMUM_LATEST_AGE_SEC,
                "preactivation:latest_freshness",
            )
            record(
                failures,
                generated < ACTIVATION_UTC,
                "preactivation:generated_after_activation",
            )
        expected = {
            "schema_version": "executable_move_census_latest_v1",
            "generated_utc": iso(generated) if generated is not None else None,
            "cohort_id": COHORT_ID,
            "status": "collecting_pre_activation",
            "activation_utc": iso(ACTIVATION_UTC),
            "instrument_count": 68,
            "side_count": 136,
            "horizons_min": list(HORIZONS_MIN),
            "research_only": True,
            "can_trade": False,
            "can_authorize": False,
            "can_promote": False,
            "frame_count": 0,
            "horizons": [],
            "top_cleared_paths": [],
            "review_queue": {},
        }
        content_hash = latest.get("content_sha256_excluding_this_field")
        unhashed = dict(latest)
        unhashed.pop("content_sha256_excluding_this_field", None)
        record(
            failures,
            content_hash == sha(canonical(unhashed)),
            "preactivation:content_sha256",
        )
        record(
            failures,
            set(latest) == set(expected) | {"content_sha256_excluding_this_field"},
            "preactivation:top_level_contract",
        )
        for key, value in expected.items():
            record(
                failures,
                latest.get(key) == value,
                f"preactivation:{key}:mismatch",
            )
    else:
        try:
            connection = _open_readonly(database_path)
        except sqlite3.Error as exc:
            failures.append(f"database:open:{type(exc).__name__}")
        else:
            try:
                _verify_schema(connection, failures)
                _verify_manifest(
                    connection, config_raw, producer_raw, source_raw,
                    quote_transport_raw, failures,
                )
                retain_quote_after = _quote_retention_start(
                    connection, runtime_cache, verification_now
                )
                (
                    all_frames, frames, quote_cache, observed_quote_count,
                    cached_prefix_chain, observed_frame_chain,
                ) = _verify_frames(
                    connection, failures, verification_now, cached_frame_count,
                    retain_quote_after,
                )
                observed_frame_count = len(all_frames)
                record(
                    failures,
                    cached_frame_count <= observed_frame_count,
                    "runtime_cache:frame_count_rollback",
                )
                if cached_frame_count:
                    record(
                        failures,
                        cached_prefix_chain
                        == runtime_cache.get("frame_chain_sha256"),
                        "runtime_cache:frame_prefix_drift",
                    )
                latest, evaluations, summaries, finalizations = _verify_latest(
                    latest_path, connection, frames, quote_cache,
                    verification_now, failures, runtime_cache,
                )
                counts.update(
                    frames=len(all_frames), eligible_frames=len(frames),
                    frame_quotes=observed_quote_count,
                    window_evaluations=len(evaluations),
                    factor_episode_summaries=len(summaries),
                    factor_episode_finalizations=len(finalizations),
                )
            except (
                sqlite3.Error, KeyError, TypeError, ValueError, OverflowError,
                IndexError,
            ) as exc:
                # Malformed or type-confused durable rows are integrity failures,
                # not reasons for the supervised verifier to crash and leave an
                # apparently fresh prior success on disk.
                failures.append(f"database:contract:{type(exc).__name__}:{exc}")
            finally:
                connection.close()
    if runtime_cache is not None and not failures:
        runtime_cache["database_path"] = cache_database
        runtime_cache["frame_count"] = observed_frame_count
        runtime_cache["frame_chain_sha256"] = observed_frame_chain
    return {
        "schema_version": "executable_move_census_verifier_v1",
        "generated_utc": iso(verification_now),
        "verified": not failures,
        "failure_count": len(failures),
        "failures": failures,
        "counts": counts,
        "latest_generated_utc": latest.get("generated_utc"),
        "cohort_id": COHORT_ID,
        "research_only": True,
        "can_trade": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
        "checkpoint_role": (
            "performance cache only; verifier/source pins, durable row hashes, "
            "and the complete prior frame-prefix chain are rechecked before reuse"
        ),
        "checkpoint_threat_boundary": (
            "malformed, stale, source-drifted, verifier-drifted, or independently "
            "changed checkpoints fall back to a full audit; a coordinated hostile "
            "replacement of both database and self-hashed checkpoint is outside "
            "the local accidental-corruption threat model"
        ),
    }


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_bytes(canonical(payload))
        for attempt in range(5):
            try:
                os.replace(temporary, path)
                return
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.05 * (attempt + 1))
    finally:
        temporary.unlink(missing_ok=True)


def load_runtime_checkpoint(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_bytes())
        content_hash = payload.pop("content_sha256_excluding_this_field")
        if content_hash != sha(canonical(payload)):
            return {}
        if payload.get("schema_version") != "executable_move_census_verifier_checkpoint_v1":
            return {}
        if payload.get("cohort_id") != COHORT_ID or payload.get("pins") != {
            "config_sha256": FROZEN_CONFIG_SHA256,
            "producer_sha256": FROZEN_PRODUCER_SHA256,
            "source_producer_sha256": FROZEN_SOURCE_PRODUCER_SHA256,
            "quote_transport_sha256": FROZEN_QUOTE_TRANSPORT_SHA256,
            "verifier_sha256": sha(Path(__file__).read_bytes()),
        }:
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


def write_runtime_checkpoint(path: Path, runtime_cache: Mapping[str, Any]) -> None:
    payload = {
        "schema_version": "executable_move_census_verifier_checkpoint_v1",
        "cohort_id": COHORT_ID,
        "pins": {
            "config_sha256": FROZEN_CONFIG_SHA256,
            "producer_sha256": FROZEN_PRODUCER_SHA256,
            "source_producer_sha256": FROZEN_SOURCE_PRODUCER_SHA256,
            "quote_transport_sha256": FROZEN_QUOTE_TRANSPORT_SHA256,
            "verifier_sha256": sha(Path(__file__).read_bytes()),
        },
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
    payload["content_sha256_excluding_this_field"] = sha(canonical(payload))
    write_json_atomic(path, payload)


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
        payload = verify(
            database_path=args.database,
            config_path=args.config,
            latest_path=args.latest,
            producer_path=args.producer,
            source_producer_path=args.source_producer,
            quote_transport_path=args.quote_transport,
            runtime_cache=runtime_cache,
        )
        write_json_atomic(args.output, payload)
        elapsed = time.monotonic() - started
        finishing = args.duration_sec <= 0 or elapsed >= args.duration_sec
        checkpoint_due = (
            last_checkpoint_elapsed is None
            or elapsed - last_checkpoint_elapsed
            >= max(60.0, args.checkpoint_interval_sec)
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
