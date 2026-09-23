#!/usr/bin/env python3
"""Independent read-only verifier for official-event horizon quote evidence.

This module intentionally does not import the horizon-capture producer.  It
recomputes identities, clocks, payload hashes, executable quote arithmetic,
and upstream raw-event linkage from the two SQLite ledgers.  It also detects
eligible event/horizon attempts that are missing after their terminal attempt
window.
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
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "official_event_quote_horizon_capture_v1.json"
INPUT_DATABASE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "official_release_fast_lane_v4.sqlite"
)
OUTPUT_DATABASE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "official_event_quote_horizon_capture_v1.sqlite"
)
STATE_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "official_event_quote_horizon_capture_verifier_latest_v1.json"
)
HEARTBEAT_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "official_event_quote_horizon_capture_verifier_heartbeat_v1.json"
)

SCHEMA_VERSION = "official_event_quote_horizon_capture_v1"
CONTRACT_ID = "official_event_quote_horizon_capture_v1_all68_append_only_20260830"
COHORT_ID = "official_event_quote_horizon_capture_v1_20260830a"
ACTIVATED_UTC = dt.datetime(2026, 8, 30, 12, 0, tzinfo=dt.timezone.utc)
REQUIRED_ENTRY_CAPTURE_CONTRACT_ID = (
    "official_release_raw_quote_capture_v1_append_boundary_all68_20260829"
)
REQUIRED_ENTRY_CAPTURE_COHORT_ID = (
    "official_release_raw_quote_capture_v1_20260829a"
)
ENTRY_CAPTURE_ACTIVATED_UTC = dt.datetime(
    2026, 8, 29, 12, 20, tzinfo=dt.timezone.utc
)
ENTRY_MAXIMUM_CAPTURE_LATENCY_SEC = 15.0
ENTRY_MAXIMUM_SNAPSHOT_AGE_SEC = 15.0
ENTRY_MAXIMUM_QUOTE_AGE_SEC = 30.0
ENTRY_MAXIMUM_FUTURE_SKEW_SEC = 2.0
ENTRY_MAXIMUM_EVENT_OFFSET_SEC = 15.0
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
REQUIRED_SNAPSHOT_SCHEMA_VERSION = 2
REQUIRED_SNAPSHOT_PRODUCER = "practice_007_fast_executor_price_stream"
HORIZONS_MIN = (1, 5, 15, 30, 60)
MAXIMUM_ATTEMPT_DELAY_SEC = 20.0
MAXIMUM_SNAPSHOT_AGE_SEC = 15.0
MAXIMUM_QUOTE_AGE_SEC = 30.0
MAXIMUM_FUTURE_SKEW_SEC = 2.0
MAXIMUM_TARGET_OFFSET_SEC = 20.0

_SAFETY = {
    "research_only": True,
    "execution_eligible": False,
    "can_place_orders": False,
    "can_authorize": False,
    "can_promote": False,
}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(dict(payload), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _parse_time(value: Any) -> dt.datetime | None:
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


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _integer(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return int(value)


def _same_number(left: Any, right: Any, tolerance: float = 1e-6) -> bool:
    a = _finite(left)
    b = _finite(right)
    return a is not None and b is not None and abs(a - b) <= tolerance


def _open_read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro",
        uri=True,
        timeout=10.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _record(failures: list[str], condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


def _verify_config(path: Path, failures: list[str]) -> None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError) as exc:
        failures.append(f"config:unreadable:{type(exc).__name__}")
        return
    expected = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": "2026-08-30T12:00:00Z",
        "required_entry_capture_contract_id": REQUIRED_ENTRY_CAPTURE_CONTRACT_ID,
        "required_entry_capture_cohort_id": REQUIRED_ENTRY_CAPTURE_COHORT_ID,
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "required_quote_snapshot_schema_version": REQUIRED_SNAPSHOT_SCHEMA_VERSION,
        "required_quote_snapshot_producer": REQUIRED_SNAPSHOT_PRODUCER,
        "horizons_min": list(HORIZONS_MIN),
    }
    for key, expected_value in expected.items():
        _record(
            failures,
            payload.get(key) == expected_value,
            f"config:{key}:mismatch",
        )
    timing = payload.get("timing")
    timing = timing if isinstance(timing, Mapping) else {}
    for key, expected_value in {
        "maximum_attempt_delay_sec": MAXIMUM_ATTEMPT_DELAY_SEC,
        "maximum_snapshot_age_sec": MAXIMUM_SNAPSHOT_AGE_SEC,
        "maximum_quote_age_sec": MAXIMUM_QUOTE_AGE_SEC,
        "maximum_future_skew_sec": MAXIMUM_FUTURE_SKEW_SEC,
        "maximum_target_offset_sec": MAXIMUM_TARGET_OFFSET_SEC,
    }.items():
        _record(
            failures,
            _same_number(timing.get(key), expected_value),
            f"config:timing:{key}:mismatch",
        )
    cost_stress = payload.get("cost_stress")
    cost_stress = cost_stress if isinstance(cost_stress, Mapping) else {}
    _record(
        failures,
        cost_stress.get("round_trip_slippage_pips") == [0.0, 0.25, 0.5],
        "config:cost_stress:round_trip_slippage_pips:mismatch",
    )
    policy = payload.get("policy")
    policy = policy if isinstance(policy, Mapping) else {}
    required_policy = {
        "prospective_only": True,
        "historical_backfill_allowed": False,
        "one_terminal_attempt_per_event_horizon": True,
        "input_entry_quote_reacquisition_allowed": False,
        "missing_or_late_capture_policy": "persist_terminal_invalid_never_retry",
        **_SAFETY,
        "supported_execution_decision": "no_trade",
    }
    for key, expected_value in required_policy.items():
        _record(
            failures,
            policy.get(key) == expected_value,
            f"config:policy:{key}:unsafe_or_mismatched",
        )


def _verify_triggers(connection: sqlite3.Connection, failures: list[str]) -> None:
    expected = {
        "horizon_capture_no_update": (
            "before update on official_event_horizon_capture",
            "raise(abort",
        ),
        "horizon_capture_no_delete": (
            "before delete on official_event_horizon_capture",
            "raise(abort",
        ),
        "horizon_quote_no_update": (
            "before update on official_event_horizon_quote",
            "raise(abort",
        ),
        "horizon_quote_no_delete": (
            "before delete on official_event_horizon_quote",
            "raise(abort",
        ),
    }
    rows = {
        str(row["name"]): str(row["sql"] or "").lower().replace("\n", " ")
        for row in connection.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger'"
        )
    }
    for name, fragments in expected.items():
        sql = " ".join(rows.get(name, "").split())
        _record(failures, bool(sql), f"schema:trigger:{name}:missing")
        for fragment in fragments:
            _record(
                failures,
                fragment in sql.replace(" ", "")
                if fragment == "raise(abort"
                else fragment in sql,
                f"schema:trigger:{name}:semantics_invalid",
            )


def _verify_schema_columns(
    connection: sqlite3.Connection,
    failures: list[str],
) -> None:
    required = {
        "official_event_horizon_capture": {
            "capture_id",
            "input_capture_id",
            "observation_id",
            "event_first_known_utc",
            "horizon_min",
            "target_utc",
            "capture_read_started_utc",
            "attempted_utc",
            "attempt_delay_sec",
            "timing_quality",
            "invalid_reason",
            "observed_valid_quote_count",
            "proof_quote_count",
            "connection_generation",
            "quote_snapshot_generated_utc",
            "quote_snapshot_sha256",
            "input_capture_payload_sha256",
            "component_root_sha256",
            "payload_json",
            "payload_sha256",
            "research_only",
            "execution_eligible",
            "can_place_orders",
            "can_authorize",
            "can_promote",
            "contract_id",
            "cohort_id",
        },
        "official_event_horizon_quote": {
            "quote_id",
            "capture_id",
            "horizon_min",
            "instrument",
            "bid",
            "ask",
            "pip",
            "quote_utc",
            "quote_age_sec",
            "target_offset_sec",
            "spread_pips",
            "source",
            "connection_generation",
            "payload_json",
            "payload_sha256",
            "research_only",
            "execution_eligible",
            "can_place_orders",
            "can_authorize",
            "can_promote",
            "contract_id",
            "cohort_id",
        },
    }
    for table, expected in required.items():
        columns = {
            str(row["name"])
            for row in connection.execute(f"PRAGMA table_info({table})")
        }
        for missing in sorted(expected - columns):
            failures.append(f"schema:{table}:column_missing:{missing}")


def _verify_source_capture(
    input_connection: sqlite3.Connection,
    header: sqlite3.Row,
    header_payload: Mapping[str, Any],
    failures: list[str],
) -> dict[str, Any] | None:
    label = f"capture:{header['capture_id']}"
    row = input_connection.execute(
        """
        SELECT q.*,o.source_id,o.item_key,o.material_sha256,
               o.first_seen_utc,o.prospective_observation,o.listing_bootstrap,
               o.identity_preexisting,o.publisher_time_eligible,
               o.observation_clock_trusted,o.raw_payload_json AS source_payload_json,
               o.research_only AS observation_research_only,
               o.execution_eligible AS observation_execution_eligible,
               o.can_authorize AS observation_can_authorize
          FROM official_release_quote_capture q
          JOIN official_release_observation o
            ON o.observation_id=q.observation_id
         WHERE q.capture_id=?
        """,
        (str(header["input_capture_id"]),),
    ).fetchone()
    if row is None:
        failures.append(f"{label}:input_capture_link_missing")
        return None

    _record(
        failures,
        str(row["observation_id"]) == str(header["observation_id"]),
        f"{label}:input_observation_link_mismatch",
    )
    _record(
        failures,
        str(row["event_first_known_utc"])
        == str(header["event_first_known_utc"]),
        f"{label}:input_event_clock_mismatch",
    )
    expected_input_id = _sha256_text(
        f"{row['observation_id']}|{REQUIRED_ENTRY_CAPTURE_CONTRACT_ID}|"
        f"{REQUIRED_ENTRY_CAPTURE_COHORT_ID}"
    )
    _record(
        failures,
        str(row["capture_id"]) == expected_input_id,
        f"{label}:input_capture_identity_invalid",
    )
    for field, expected in {
        "capture_contract_id": REQUIRED_ENTRY_CAPTURE_CONTRACT_ID,
        "capture_cohort_id": REQUIRED_ENTRY_CAPTURE_COHORT_ID,
        "timing_quality": "prospective_exact_live_quote",
        "proof_quote_count": EXPECTED_INSTRUMENT_COUNT,
        "input_prospective_observation": 1,
        "capture_activation_eligible": 1,
        "research_only": 1,
        "execution_eligible": 0,
        "can_authorize": 0,
        "can_promote": 0,
        "capture_activated_utc": ENTRY_CAPTURE_ACTIVATED_UTC.isoformat(),
    }.items():
        _record(
            failures,
            row[field] == expected,
            f"{label}:input_capture:{field}:invalid",
        )

    raw_capture = str(row["capture_payload_json"])
    _record(
        failures,
        _sha256_text(raw_capture) == str(header["input_capture_payload_sha256"]),
        f"{label}:input_capture_payload_sha_mismatch",
    )
    try:
        capture_payload = json.loads(raw_capture)
    except (TypeError, ValueError):
        failures.append(f"{label}:input_capture_payload_json_invalid")
        capture_payload = {}
    _record(
        failures,
        raw_capture == _canonical_json(capture_payload),
        f"{label}:input_capture_payload_not_canonical",
    )
    for field, expected in {
        "capture_contract_id": REQUIRED_ENTRY_CAPTURE_CONTRACT_ID,
        "capture_cohort_id": REQUIRED_ENTRY_CAPTURE_COHORT_ID,
        "timing_quality": "prospective_exact_live_quote",
        "proof_quote_count": EXPECTED_INSTRUMENT_COUNT,
        "input_prospective_observation": True,
        "capture_activation_eligible": True,
        "instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_promote": False,
        "capture_activated_utc": ENTRY_CAPTURE_ACTIVATED_UTC.isoformat(),
    }.items():
        _record(
            failures,
            capture_payload.get(field) == expected,
            f"{label}:input_payload:{field}:invalid",
        )
    _record(
        failures,
        str(capture_payload.get("observation_id") or "")
        == str(row["observation_id"]),
        f"{label}:input_payload_observation_mismatch",
    )
    _record(
        failures,
        str(capture_payload.get("event_first_known_utc") or "")
        == str(row["event_first_known_utc"]),
        f"{label}:input_payload_event_clock_mismatch",
    )
    for field in (
        "observation_id",
        "event_first_known_utc",
        "captured_utc",
        "input_prospective_observation",
        "capture_activation_eligible",
        "timing_quality",
        "observed_valid_quote_count",
        "proof_quote_count",
        "research_only",
        "execution_eligible",
        "can_authorize",
        "can_promote",
        "capture_contract_id",
        "capture_cohort_id",
        "capture_activated_utc",
    ):
        payload_value = capture_payload.get(field)
        row_value = row[field]
        equal = _same_number(payload_value, row_value) if field in {
            "input_prospective_observation",
            "capture_activation_eligible",
            "observed_valid_quote_count",
            "proof_quote_count",
            "research_only",
            "execution_eligible",
            "can_authorize",
            "can_promote",
        } else payload_value == row_value
        _record(
            failures,
            equal,
            f"{label}:input_payload_column_mismatch:{field}",
        )

    event = _parse_time(row["event_first_known_utc"])
    captured = _parse_time(row["captured_utc"])
    capture_latency = (
        None
        if event is None or captured is None
        else (captured - event).total_seconds()
    )
    _record(
        failures,
        event is not None and captured is not None,
        f"{label}:input_capture_clock_invalid",
    )
    if capture_latency is not None:
        _record(
            failures,
            0 <= capture_latency <= ENTRY_MAXIMUM_CAPTURE_LATENCY_SEC,
            f"{label}:input_capture_latency_invalid",
        )
        _record(
            failures,
            _same_number(
                capture_payload.get("capture_latency_seconds"), capture_latency
            ),
            f"{label}:input_capture_latency_recompute_mismatch",
        )
    instruments = capture_payload.get("expected_instruments")
    quotes = capture_payload.get("quotes")
    instruments = instruments if isinstance(instruments, list) else []
    quotes = quotes if isinstance(quotes, Mapping) else {}
    _record(
        failures,
        instruments == list(EXPECTED_INSTRUMENTS),
        f"{label}:input_payload_universe_list_mismatch",
    )
    _record(
        failures,
        set(quotes) == set(EXPECTED_INSTRUMENTS) and len(quotes) == 68,
        f"{label}:input_payload_quote_universe_mismatch",
    )
    observed_quotes = capture_payload.get("observed_quotes")
    observed_quotes = (
        observed_quotes if isinstance(observed_quotes, Mapping) else {}
    )
    _record(
        failures,
        observed_quotes == quotes,
        f"{label}:input_payload_observed_proof_quotes_mismatch",
    )
    for field, expected in {
        "expected_quote_count": EXPECTED_INSTRUMENT_COUNT,
        "observed_valid_quote_count": EXPECTED_INSTRUMENT_COUNT,
        "quote_count": EXPECTED_INSTRUMENT_COUNT,
    }.items():
        _record(
            failures,
            capture_payload.get(field) == expected,
            f"{label}:input_payload:{field}:invalid",
        )
    for field, empty in {
        "missing_instruments": [],
        "unexpected_instruments": [],
        "invalid_instruments": {},
    }.items():
        _record(
            failures,
            capture_payload.get(field) == empty,
            f"{label}:input_payload:{field}:not_empty",
        )
    generated = _parse_time(capture_payload.get("quote_snapshot_generated_utc"))
    if captured is not None:
        _record(
            failures,
            generated is not None,
            f"{label}:input_snapshot_clock_missing",
        )
        if generated is not None:
            snapshot_age = (captured - generated).total_seconds()
            _record(
                failures,
                -ENTRY_MAXIMUM_FUTURE_SKEW_SEC
                <= snapshot_age
                <= ENTRY_MAXIMUM_SNAPSHOT_AGE_SEC,
                f"{label}:input_snapshot_clock_stale_or_future",
            )
            _record(
                failures,
                _same_number(
                    capture_payload.get("quote_snapshot_age_seconds"),
                    snapshot_age,
                ),
                f"{label}:input_snapshot_age_recompute_mismatch",
            )
    for instrument in EXPECTED_INSTRUMENTS:
        quote = quotes.get(instrument)
        quote = quote if isinstance(quote, Mapping) else {}
        bid = _finite(quote.get("bid"))
        ask = _finite(quote.get("ask"))
        pip = _finite(quote.get("pip"))
        quote_time = _parse_time(quote.get("quote_time_utc"))
        valid_quote = (
            bid is not None
            and ask is not None
            and pip is not None
            and bid > 0
            and ask > bid
            and pip > 0
            and quote_time is not None
        )
        _record(
            failures,
            valid_quote,
            f"{label}:input_quote:{instrument}:invalid",
        )
        if valid_quote and captured is not None and event is not None:
            assert quote_time is not None
            quote_age = (captured - quote_time).total_seconds()
            event_offset = (quote_time - event).total_seconds()
            _record(
                failures,
                -ENTRY_MAXIMUM_FUTURE_SKEW_SEC
                <= quote_age
                <= ENTRY_MAXIMUM_QUOTE_AGE_SEC,
                f"{label}:input_quote:{instrument}:freshness_invalid",
            )
            _record(
                failures,
                event_offset <= ENTRY_MAXIMUM_EVENT_OFFSET_SEC,
                f"{label}:input_quote:{instrument}:event_offset_invalid",
            )
            _record(
                failures,
                _same_number(quote.get("age_seconds"), quote_age),
                f"{label}:input_quote:{instrument}:age_recompute_mismatch",
            )
            _record(
                failures,
                _same_number(quote.get("event_offset_seconds"), event_offset),
                f"{label}:input_quote:{instrument}:offset_recompute_mismatch",
            )

    source_payload = str(row["source_payload_json"])
    material_sha = _sha256_text(source_payload)
    _record(
        failures,
        material_sha == str(row["material_sha256"]),
        f"{label}:source_raw_payload_sha_mismatch",
    )
    expected_observation_id = _sha256_text(
        f"{row['source_id']}|{row['item_key']}|{material_sha}"
    )
    _record(
        failures,
        expected_observation_id == str(row["observation_id"]),
        f"{label}:source_observation_identity_invalid",
    )
    for field, expected in {
        "prospective_observation": 1,
        "listing_bootstrap": 0,
        "identity_preexisting": 0,
        "publisher_time_eligible": 1,
        "observation_clock_trusted": 1,
        "observation_research_only": 1,
        "observation_execution_eligible": 0,
        "observation_can_authorize": 0,
    }.items():
        _record(
            failures,
            row[field] == expected,
            f"{label}:source_observation:{field}:invalid",
        )
    _record(
        failures,
        str(row["first_seen_utc"]) == str(row["event_first_known_utc"]),
        f"{label}:source_first_seen_clock_mismatch",
    )
    return dict(row)


def _verify_component(
    row: sqlite3.Row,
    header: sqlite3.Row,
    header_payload: Mapping[str, Any],
    failures: list[str],
) -> str:
    label = f"capture:{header['capture_id']}:quote:{row['instrument']}"
    raw_payload = str(row["payload_json"])
    try:
        payload = json.loads(raw_payload)
    except (TypeError, ValueError):
        failures.append(f"{label}:payload_json_invalid")
        payload = {}
    payload_hash = _sha256_text(raw_payload)
    _record(
        failures,
        raw_payload == _canonical_json(payload),
        f"{label}:payload_not_canonical",
    )
    _record(
        failures,
        payload_hash == str(row["payload_sha256"]),
        f"{label}:payload_sha_mismatch",
    )
    expected_quote_id = "official_event_horizon_quote_" + _sha256_text(
        f"{row['capture_id']}|{row['instrument']}|{row['payload_sha256']}"
    )[:32]
    _record(
        failures,
        str(row["quote_id"]) == expected_quote_id,
        f"{label}:quote_identity_invalid",
    )
    for field in (
        "instrument",
        "bid",
        "ask",
        "pip",
        "quote_utc",
        "quote_age_sec",
        "target_offset_sec",
        "spread_pips",
        "source",
        "connection_generation",
    ):
        left = payload.get(field)
        right = row[field]
        equal = _same_number(left, right) if field in {
            "bid",
            "ask",
            "pip",
            "quote_age_sec",
            "target_offset_sec",
            "spread_pips",
            "connection_generation",
        } else left == right
        _record(failures, equal, f"{label}:payload_column_mismatch:{field}")
    for field, expected in {
        "horizon_min": int(header["horizon_min"]),
        "research_only": 1,
        "execution_eligible": 0,
        "can_place_orders": 0,
        "can_authorize": 0,
        "can_promote": 0,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
    }.items():
        _record(failures, row[field] == expected, f"{label}:{field}:invalid")

    bid = _finite(row["bid"])
    ask = _finite(row["ask"])
    pip = _finite(row["pip"])
    valid_prices = (
        bid is not None
        and ask is not None
        and pip is not None
        and bid > 0
        and ask > bid
        and pip > 0
    )
    _record(failures, valid_prices, f"{label}:bid_ask_or_pip_invalid")
    if valid_prices:
        assert bid is not None and ask is not None and pip is not None
        _record(
            failures,
            _same_number(row["spread_pips"], (ask - bid) / pip, 1e-8),
            f"{label}:spread_pips_invalid",
        )
    _record(
        failures,
        bool(str(row["source"] or "").strip()),
        f"{label}:source_missing",
    )
    _record(
        failures,
        _same_number(
            row["connection_generation"], header["connection_generation"]
        ),
        f"{label}:header_generation_mismatch",
    )

    attempted = _parse_time(header["attempted_utc"])
    target = _parse_time(header["target_utc"])
    quoted = _parse_time(row["quote_utc"])
    _record(
        failures,
        attempted is not None and target is not None and quoted is not None,
        f"{label}:quote_clock_invalid",
    )
    if attempted is not None and target is not None and quoted is not None:
        age = (attempted - quoted).total_seconds()
        offset = (quoted - target).total_seconds()
        _record(
            failures,
            _same_number(row["quote_age_sec"], age),
            f"{label}:quote_age_recompute_mismatch",
        )
        _record(
            failures,
            _same_number(row["target_offset_sec"], offset),
            f"{label}:target_offset_recompute_mismatch",
        )
        _record(
            failures,
            -MAXIMUM_FUTURE_SKEW_SEC <= age <= MAXIMUM_QUOTE_AGE_SEC,
            f"{label}:quote_freshness_invalid",
        )
        _record(
            failures,
            abs(offset) <= MAXIMUM_TARGET_OFFSET_SEC,
            f"{label}:target_offset_outside_window",
        )

    quotes = header_payload.get("quotes")
    quotes = quotes if isinstance(quotes, Mapping) else {}
    _record(
        failures,
        quotes.get(str(row["instrument"])) == payload,
        f"{label}:header_component_payload_mismatch",
    )
    snapshot_material = header_payload.get("quote_snapshot_material")
    snapshot_material = (
        snapshot_material if isinstance(snapshot_material, Mapping) else {}
    )
    raw_quotes = snapshot_material.get("quotes")
    raw_quotes = raw_quotes if isinstance(raw_quotes, Mapping) else {}
    raw_quote = raw_quotes.get(str(row["instrument"]))
    raw_quote = raw_quote if isinstance(raw_quote, Mapping) else {}
    for field in ("bid", "ask", "pip"):
        _record(
            failures,
            _same_number(raw_quote.get(field), row[field]),
            f"{label}:raw_snapshot_mismatch:{field}",
        )
    _record(
        failures,
        str(raw_quote.get("source") or "").strip() == str(row["source"]).strip(),
        f"{label}:raw_snapshot_mismatch:source",
    )
    raw_quote_time = _parse_time(raw_quote.get("time"))
    normalized_quote_time = _parse_time(row["quote_utc"])
    _record(
        failures,
        raw_quote_time is not None and raw_quote_time == normalized_quote_time,
        f"{label}:raw_snapshot_mismatch:quote_time",
    )
    return str(row["payload_sha256"])


def _verify_header(
    output_connection: sqlite3.Connection,
    input_connection: sqlite3.Connection,
    header: sqlite3.Row,
    failures: list[str],
) -> tuple[bool, bool]:
    label = f"capture:{header['capture_id']}"
    raw_payload = str(header["payload_json"])
    try:
        payload = json.loads(raw_payload)
    except (TypeError, ValueError):
        failures.append(f"{label}:payload_json_invalid")
        payload = {}
    _record(
        failures,
        raw_payload == _canonical_json(payload),
        f"{label}:payload_not_canonical",
    )
    _record(
        failures,
        _sha256_text(raw_payload) == str(header["payload_sha256"]),
        f"{label}:payload_sha_mismatch",
    )
    expected_capture_id = "official_event_horizon_" + _sha256_text(
        f"{header['input_capture_id']}|{header['horizon_min']}|"
        f"{CONTRACT_ID}|{COHORT_ID}"
    )[:32]
    _record(
        failures,
        str(header["capture_id"]) == expected_capture_id,
        f"{label}:capture_identity_invalid",
    )
    for field in (
        "capture_id",
        "input_capture_id",
        "observation_id",
        "event_first_known_utc",
        "horizon_min",
        "target_utc",
        "capture_read_started_utc",
        "attempted_utc",
        "attempt_delay_sec",
        "timing_quality",
        "invalid_reason",
        "observed_valid_quote_count",
        "proof_quote_count",
        "connection_generation",
        "quote_snapshot_generated_utc",
        "quote_snapshot_sha256",
        "component_root_sha256",
        "research_only",
        "execution_eligible",
        "can_place_orders",
        "can_authorize",
        "can_promote",
        "contract_id",
        "cohort_id",
    ):
        left = payload.get(field)
        right = header[field]
        equal = _same_number(left, right) if field in {
            "horizon_min",
            "attempt_delay_sec",
            "observed_valid_quote_count",
            "proof_quote_count",
            "connection_generation",
            "research_only",
            "execution_eligible",
            "can_place_orders",
            "can_authorize",
            "can_promote",
        } else left == right
        _record(failures, equal, f"{label}:payload_column_mismatch:{field}")
    for field, expected in {
        "research_only": 1,
        "execution_eligible": 0,
        "can_place_orders": 0,
        "can_authorize": 0,
        "can_promote": 0,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
    }.items():
        _record(failures, header[field] == expected, f"{label}:{field}:invalid")
    for field, expected in {
        "schema_version": SCHEMA_VERSION,
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "entry_capture_contract_id": REQUIRED_ENTRY_CAPTURE_CONTRACT_ID,
        "entry_capture_cohort_id": REQUIRED_ENTRY_CAPTURE_COHORT_ID,
        **_SAFETY,
    }.items():
        _record(
            failures,
            payload.get(field) == expected,
            f"{label}:payload:{field}:invalid",
        )

    snapshot_material = payload.get("quote_snapshot_material")
    _record(
        failures,
        isinstance(snapshot_material, Mapping),
        f"{label}:quote_snapshot_material_missing",
    )
    if isinstance(snapshot_material, Mapping):
        material_sha256 = _sha256_text(_canonical_json(snapshot_material))
        _record(
            failures,
            material_sha256 == str(header["quote_snapshot_sha256"]),
            f"{label}:quote_snapshot_material_sha_mismatch",
        )

    horizon = int(header["horizon_min"])
    _record(failures, horizon in HORIZONS_MIN, f"{label}:horizon_invalid")
    event = _parse_time(header["event_first_known_utc"])
    target = _parse_time(header["target_utc"])
    capture_read_started = _parse_time(header["capture_read_started_utc"])
    attempted = _parse_time(header["attempted_utc"])
    _record(
        failures,
        event is not None
        and target is not None
        and capture_read_started is not None
        and attempted is not None,
        f"{label}:event_or_attempt_clock_invalid",
    )
    _record(
        failures,
        bool(str(header["capture_read_started_utc"] or "").strip()),
        f"{label}:capture_read_start_missing",
    )
    if capture_read_started is not None and attempted is not None:
        _record(
            failures,
            capture_read_started <= attempted,
            f"{label}:capture_read_start_after_attempt",
        )
    delay: float | None = None
    if event is not None and target is not None and attempted is not None:
        expected_target = event + dt.timedelta(minutes=horizon)
        delay = (attempted - target).total_seconds()
        _record(failures, target == expected_target, f"{label}:target_clock_invalid")
        _record(failures, delay >= 0, f"{label}:attempt_before_target")
        _record(
            failures,
            _same_number(header["attempt_delay_sec"], delay),
            f"{label}:attempt_delay_recompute_mismatch",
        )

    exact = str(header["timing_quality"]) == "prospective_exact_live_quote"
    proof_count = int(header["proof_quote_count"])
    observed_count = int(header["observed_valid_quote_count"])
    invalid_reason = str(header["invalid_reason"])
    if exact:
        _record(
            failures,
            event is not None and event >= ACTIVATED_UTC,
            f"{label}:exact_before_activation",
        )
        _record(
            failures,
            delay is not None and 0 <= delay <= MAXIMUM_ATTEMPT_DELAY_SEC,
            f"{label}:exact_attempt_clock_invalid",
        )
        _record(failures, proof_count == 68, f"{label}:exact_proof_count_invalid")
        _record(failures, observed_count == 68, f"{label}:exact_observed_count_invalid")
        _record(failures, not invalid_reason, f"{label}:exact_has_invalid_reason")
        _record(
            failures,
            not (payload.get("coverage_gaps") or []),
            f"{label}:exact_has_coverage_gaps",
        )
        _record(
            failures,
            not (payload.get("invalid_instruments") or {}),
            f"{label}:exact_has_invalid_instruments",
        )
        generated = _parse_time(header["quote_snapshot_generated_utc"])
        _record(failures, generated is not None, f"{label}:snapshot_clock_missing")
        if attempted is not None and generated is not None:
            snapshot_age = (attempted - generated).total_seconds()
            _record(
                failures,
                -MAXIMUM_FUTURE_SKEW_SEC <= snapshot_age <= MAXIMUM_SNAPSHOT_AGE_SEC,
                f"{label}:snapshot_clock_stale_or_future",
            )
        material = (
            snapshot_material if isinstance(snapshot_material, Mapping) else {}
        )
        _record(
            failures,
            _integer(material.get("schema_version"))
            == REQUIRED_SNAPSHOT_SCHEMA_VERSION,
            f"{label}:snapshot_schema_version_invalid",
        )
        _record(
            failures,
            str(material.get("producer") or "") == REQUIRED_SNAPSHOT_PRODUCER,
            f"{label}:snapshot_producer_invalid",
        )
        _record(
            failures,
            str(material.get("generated_utc") or "")
            == str(header["quote_snapshot_generated_utc"]),
            f"{label}:snapshot_generated_clock_mismatch",
        )
        coverage = material.get("coverage")
        coverage = coverage if isinstance(coverage, Mapping) else {}
        for field, expected in {
            "quote_count": EXPECTED_INSTRUMENT_COUNT,
        }.items():
            _record(
                failures,
                _integer(material.get(field)) == expected,
                f"{label}:snapshot_{field}_invalid",
            )
        for field, expected in {
            "current_quote_count": EXPECTED_INSTRUMENT_COUNT,
            "last_known_quote_count": EXPECTED_INSTRUMENT_COUNT,
            "retained_last_known_count": 0,
        }.items():
            _record(
                failures,
                _integer(coverage.get(field)) == expected,
                f"{label}:snapshot_coverage_{field}_invalid",
            )
        _record(
            failures,
            _integer(material.get("connection_generation"))
            == header["connection_generation"]
            and _integer(coverage.get("connection_generation"))
            == header["connection_generation"],
            f"{label}:snapshot_generation_mismatch",
        )
    else:
        _record(failures, proof_count == 0, f"{label}:invalid_attempt_has_proof_quotes")
        _record(
            failures,
            bool(invalid_reason),
            f"{label}:invalid_attempt_reason_missing",
        )
        if event is not None and event < ACTIVATED_UTC:
            _record(
                failures,
                str(header["timing_quality"])
                == "diagnostic_pre_horizon_capture_activation",
                f"{label}:preactivation_quality_invalid",
            )
        elif delay is not None and delay > MAXIMUM_ATTEMPT_DELAY_SEC:
            _record(
                failures,
                str(header["timing_quality"]) == "prospective_horizon_clock_missed",
                f"{label}:late_quality_invalid",
            )

    _verify_source_capture(input_connection, header, payload, failures)
    components = output_connection.execute(
        """
        SELECT * FROM official_event_horizon_quote
         WHERE capture_id=? ORDER BY instrument
        """,
        (str(header["capture_id"]),),
    ).fetchall()
    if exact:
        _record(
            failures,
            len(components) == 68,
            f"{label}:exact_component_count_invalid",
        )
        _record(
            failures,
            {str(row["instrument"]) for row in components} == set(EXPECTED_INSTRUMENTS),
            f"{label}:exact_component_universe_invalid",
        )
    else:
        _record(
            failures,
            len(components) == 0,
            f"{label}:invalid_attempt_has_components",
        )
    component_hashes = [
        _verify_component(row, header, payload, failures) for row in components
    ]
    expected_root = _sha256_text("".join(component_hashes))
    _record(
        failures,
        expected_root == str(header["component_root_sha256"]),
        f"{label}:component_root_sha_mismatch",
    )
    quotes = payload.get("quotes")
    quotes = quotes if isinstance(quotes, Mapping) else {}
    _record(
        failures,
        len(quotes) == len(components),
        f"{label}:header_quote_count_component_mismatch",
    )
    if exact:
        actual_universe_hash = _sha256_text(
            json.dumps(sorted(quotes), separators=(",", ":"))
        )
        _record(
            failures,
            actual_universe_hash == EXPECTED_UNIVERSE_SHA256,
            f"{label}:component_universe_sha_mismatch",
        )
    return exact, not exact


def _verify_due_schedule(
    input_connection: sqlite3.Connection,
    output_connection: sqlite3.Connection,
    observed_utc: dt.datetime,
    failures: list[str],
) -> dict[str, int]:
    existing = {
        (str(row[0]), int(row[1]))
        for row in output_connection.execute(
            "SELECT input_capture_id,horizon_min FROM official_event_horizon_capture"
        )
    }
    rows = input_connection.execute(
        """
        SELECT q.capture_id,q.event_first_known_utc
          FROM official_release_quote_capture q
          JOIN official_release_observation o
            ON o.observation_id=q.observation_id
         WHERE q.timing_quality='prospective_exact_live_quote'
           AND q.proof_quote_count=?
           AND q.capture_contract_id=?
           AND q.capture_cohort_id=?
           AND q.input_prospective_observation=1
           AND q.capture_activation_eligible=1
           AND o.prospective_observation=1
        """,
        (
            EXPECTED_INSTRUMENT_COUNT,
            REQUIRED_ENTRY_CAPTURE_CONTRACT_ID,
            REQUIRED_ENTRY_CAPTURE_COHORT_ID,
        ),
    ).fetchall()
    counts = {
        "eligible_input_events": len(rows),
        "due_event_horizons": 0,
        "due_missing_event_horizons": 0,
        "not_yet_due_event_horizons": 0,
    }
    for row in rows:
        event = _parse_time(row["event_first_known_utc"])
        if event is None or event < ACTIVATED_UTC:
            continue
        for horizon in HORIZONS_MIN:
            terminal_deadline = (
                event
                + dt.timedelta(minutes=horizon)
                + dt.timedelta(seconds=MAXIMUM_ATTEMPT_DELAY_SEC)
            )
            key = (str(row["capture_id"]), horizon)
            if observed_utc > terminal_deadline:
                counts["due_event_horizons"] += 1
                if key not in existing:
                    counts["due_missing_event_horizons"] += 1
                    failures.append(
                        f"schedule:due_attempt_missing:{row['capture_id']}:{horizon}m"
                    )
            elif key not in existing:
                counts["not_yet_due_event_horizons"] += 1
    return counts


def verify_horizon_capture_database(
    *,
    output_database: Path = OUTPUT_DATABASE,
    input_database: Path = INPUT_DATABASE,
    config_path: Path = CONFIG_PATH,
    observed_utc: dt.datetime | None = None,
) -> dict[str, Any]:
    """Verify both ledgers without mutating or importing their producer."""

    failures: list[str] = []
    _verify_config(config_path, failures)
    universe_material = json.dumps(
        list(EXPECTED_INSTRUMENTS), separators=(",", ":")
    )
    _record(
        failures,
        len(EXPECTED_INSTRUMENTS) == 68
        and _sha256_text(universe_material) == EXPECTED_UNIVERSE_SHA256,
        "verifier:frozen_universe_contract_invalid",
    )
    now = observed_utc or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    now = now.astimezone(dt.timezone.utc)
    output: sqlite3.Connection | None = None
    source: sqlite3.Connection | None = None
    counts: dict[str, int] = {
        "horizon_attempts": 0,
        "exact_horizon_attempts": 0,
        "terminal_invalid_attempts": 0,
        "quote_components": 0,
        "eligible_input_events": 0,
        "due_event_horizons": 0,
        "due_missing_event_horizons": 0,
        "not_yet_due_event_horizons": 0,
    }
    try:
        output = _open_read_only(output_database)
        source = _open_read_only(input_database)
        for name, connection in (("output", output), ("input", source)):
            integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
            _record(
                failures,
                integrity == "ok",
                f"database:{name}:integrity:{integrity}",
            )
        _verify_schema_columns(output, failures)
        _verify_triggers(output, failures)
        duplicate_rows = output.execute(
            """
            SELECT input_capture_id,horizon_min,COUNT(*) AS n
              FROM official_event_horizon_capture
             GROUP BY input_capture_id,horizon_min HAVING n<>1
            """
        ).fetchall()
        _record(failures, not duplicate_rows, "schema:duplicate_event_horizon")
        observation_duplicates = output.execute(
            """
            SELECT observation_id,horizon_min,COUNT(*) AS n
              FROM official_event_horizon_capture
             GROUP BY observation_id,horizon_min HAVING n<>1
            """
        ).fetchall()
        _record(
            failures,
            not observation_duplicates,
            "schema:duplicate_observation_horizon",
        )
        headers = output.execute(
            "SELECT * FROM official_event_horizon_capture "
            "ORDER BY event_first_known_utc,horizon_min"
        ).fetchall()
        counts["horizon_attempts"] = len(headers)
        for header in headers:
            exact, invalid = _verify_header(output, source, header, failures)
            counts["exact_horizon_attempts"] += int(exact)
            counts["terminal_invalid_attempts"] += int(invalid)
        counts["quote_components"] = int(
            output.execute(
                "SELECT COUNT(*) FROM official_event_horizon_quote"
            ).fetchone()[0]
        )
        orphan_components = int(
            output.execute(
                """
                SELECT COUNT(*) FROM official_event_horizon_quote q
                LEFT JOIN official_event_horizon_capture h
                  ON h.capture_id=q.capture_id
                WHERE h.capture_id IS NULL
                """
            ).fetchone()[0]
        )
        _record(failures, orphan_components == 0, "schema:orphan_quote_components")
        counts.update(_verify_due_schedule(source, output, now, failures))
    except (OSError, sqlite3.Error, TypeError, ValueError, KeyError) as exc:
        failures.append(f"verification_exception:{type(exc).__name__}:{exc}")
    finally:
        if source is not None:
            source.close()
        if output is not None:
            output.close()

    unique_failures = list(dict.fromkeys(failures))
    return {
        "schema_version": "official_event_quote_horizon_capture_v1_verifier",
        "verified": not unique_failures,
        "status": "verified" if not unique_failures else "failed",
        "observed_utc": now.isoformat(),
        "output_database": str(output_database),
        "input_database": str(input_database),
        "config_path": str(config_path),
        "counts": counts,
        "failures": unique_failures,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-database", type=Path, default=OUTPUT_DATABASE)
    parser.add_argument("--input-database", type=Path, default=INPUT_DATABASE)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--state", type=Path, default=STATE_PATH)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT_PATH)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    latest: dict[str, Any] = {}
    while True:
        latest = verify_horizon_capture_database(
            output_database=args.output_database,
            input_database=args.input_database,
            config_path=args.config,
        )
        _write_json_atomic(args.state, latest)
        _write_json_atomic(args.heartbeat, latest)
        print(json.dumps(latest, indent=2, sort_keys=True), flush=True)
        if args.duration_sec <= 0 or time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0 if latest.get("verified") else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["verify_horizon_capture_database"]
