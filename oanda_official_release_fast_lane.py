"""Low-latency, append-only observation lane for official FX releases.

The broad news collector performs expensive enrichment, reclassification and
clustering after it polls sources.  That work is valuable, but it means its
next source poll can be delayed by several minutes.  This worker independently
polls the monetary-policy transports, authoritative central-bank communication
transports, and explicitly mapped official statistical release transports for
the 21-currency universe.  It preserves the parsed source payload plus an exact
first-seen clock in a separate SQLite ledger.  It deliberately performs no
sentiment scoring, prediction, authorization, promotion or broker action.

The main semantic collector can consume this ledger in a separately versioned
change.  Until then it is an immutable latency/provenance observation surface.
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
from typing import Any, Callable, Mapping, Sequence

import oanda_local_news_sentiment as news
from oanda_quote_transport import load_quote_snapshot
from oanda_official_release_fast_lane_contract import (
    OFFICIAL_RELEASE_FAST_LANE_ACTIVATED_UTC,
    OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
    OFFICIAL_RELEASE_FAST_LANE_SCHEMA_VERSION,
)


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "news_sources_v1.json"
CENTRAL_BANK_MAP_PATH = ROOT / "config" / "official_central_bank_source_map_v1.json"
OUTPUT_ROOT = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
DATABASE_PATH = OUTPUT_ROOT / "official_release_fast_lane_v4.sqlite"
STATE_PATH = OUTPUT_ROOT / "official_release_fast_lane_state_v4.json"
SNAPSHOT_PATH = OUTPUT_ROOT / "official_release_fast_lane_latest_v4.json"
HEARTBEAT_PATH = OUTPUT_ROOT / "official_release_fast_lane_heartbeat_v4.json"
QUOTE_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "practice_007_market_quotes_v1.json"
)

SCHEMA_VERSION = OFFICIAL_RELEASE_FAST_LANE_SCHEMA_VERSION
CONTRACT_ID = OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID
COLLECTOR_COHORT_ID = OFFICIAL_RELEASE_FAST_LANE_COHORT_ID
ACTIVATED_UTC = OFFICIAL_RELEASE_FAST_LANE_ACTIVATED_UTC

# This is a new, prospective-only evidence cohort.  Existing raw observations
# are deliberately not backfilled: a quote read after the fact cannot prove
# what was executable when an item was first observed.
QUOTE_CAPTURE_SCHEMA_VERSION = "official_release_raw_quote_capture_v1"
QUOTE_CAPTURE_CONTRACT_ID = (
    "official_release_raw_quote_capture_v1_append_boundary_all68_20260829"
)
QUOTE_CAPTURE_COHORT_ID = "official_release_raw_quote_capture_v1_20260829a"
QUOTE_CAPTURE_ACTIVATED_UTC = dt.datetime(
    2026, 8, 29, 12, 20, tzinfo=dt.timezone.utc
)
QUOTE_CAPTURE_MAX_LATENCY_SECONDS = 15.0
QUOTE_CAPTURE_MAX_SNAPSHOT_AGE_SECONDS = 15.0
QUOTE_CAPTURE_MAX_QUOTE_AGE_SECONDS = 30.0
QUOTE_CAPTURE_MAX_FUTURE_SKEW_SECONDS = 2.0
QUOTE_CAPTURE_MAX_ENTRY_OFFSET_SECONDS = 15.0
EXPECTED_QUOTE_INSTRUMENTS = tuple(
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
EXPECTED_QUOTE_COUNT = 68
EXPECTED_QUOTE_UNIVERSE_SHA256 = (
    "b6abe559dcaa9faa0a1e48217f97a12dc9043eb9595d2b946bf80f93035be142"
)
_EXPECTED_QUOTE_UNIVERSE_MATERIAL = json.dumps(
    list(EXPECTED_QUOTE_INSTRUMENTS), separators=(",", ":")
).encode("utf-8")
if (
    len(EXPECTED_QUOTE_INSTRUMENTS) != EXPECTED_QUOTE_COUNT
    or hashlib.sha256(_EXPECTED_QUOTE_UNIVERSE_MATERIAL).hexdigest()
    != EXPECTED_QUOTE_UNIVERSE_SHA256
):
    raise RuntimeError("frozen all-68 quote universe contract is inconsistent")


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime | None = None) -> str:
    current = value or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=dt.timezone.utc)
    return current.astimezone(dt.timezone.utc).isoformat()


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return default


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                return
            except OSError:
                if attempt == 7:
                    raise
                time.sleep(min(0.5, 0.01 * (2**attempt)))
    finally:
        temporary.unlink(missing_ok=True)


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS official_release_observation (
            observation_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            source_contract_id TEXT NOT NULL,
            source_cohort_id TEXT NOT NULL,
            item_key TEXT NOT NULL,
            material_sha256 TEXT NOT NULL,
            first_seen_utc TEXT NOT NULL,
            prospective_observation INTEGER NOT NULL CHECK (prospective_observation IN (0, 1)),
            listing_bootstrap INTEGER NOT NULL CHECK (listing_bootstrap IN (0, 1)),
            identity_preexisting INTEGER NOT NULL CHECK (identity_preexisting IN (0, 1)),
            publisher_time_eligible INTEGER NOT NULL CHECK (publisher_time_eligible IN (0, 1)),
            observation_clock_trusted INTEGER NOT NULL CHECK (observation_clock_trusted IN (0, 1)),
            observation_clock_source TEXT NOT NULL,
            raw_payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK (research_only = 1),
            execution_eligible INTEGER NOT NULL CHECK (execution_eligible = 0),
            can_authorize INTEGER NOT NULL CHECK (can_authorize = 0),
            collector_contract_id TEXT NOT NULL,
            collector_cohort_id TEXT NOT NULL,
            UNIQUE(source_id, item_key, material_sha256)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_fast_lane_source_seen "
        "ON official_release_observation(source_id, first_seen_utc)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS official_release_quote_capture (
            capture_id TEXT PRIMARY KEY,
            observation_id TEXT NOT NULL UNIQUE,
            event_first_known_utc TEXT NOT NULL,
            captured_utc TEXT NOT NULL,
            input_prospective_observation INTEGER NOT NULL
                CHECK (input_prospective_observation IN (0, 1)),
            capture_activation_eligible INTEGER NOT NULL
                CHECK (capture_activation_eligible IN (0, 1)),
            timing_quality TEXT NOT NULL,
            observed_valid_quote_count INTEGER NOT NULL,
            proof_quote_count INTEGER NOT NULL,
            capture_payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK (research_only = 1),
            execution_eligible INTEGER NOT NULL CHECK (execution_eligible = 0),
            can_authorize INTEGER NOT NULL CHECK (can_authorize = 0),
            can_promote INTEGER NOT NULL CHECK (can_promote = 0),
            capture_contract_id TEXT NOT NULL,
            capture_cohort_id TEXT NOT NULL,
            capture_activated_utc TEXT NOT NULL,
            FOREIGN KEY(observation_id)
                REFERENCES official_release_observation(observation_id)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_fast_lane_quote_capture_quality "
        "ON official_release_quote_capture(timing_quality, captured_utc)"
    )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_fast_lane_quote_capture_no_update
        BEFORE UPDATE ON official_release_quote_capture
        BEGIN
            SELECT RAISE(ABORT, 'official release quote capture is append-only');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_fast_lane_quote_capture_no_delete
        BEFORE DELETE ON official_release_quote_capture
        BEGIN
            SELECT RAISE(ABORT, 'official release quote capture is append-only');
        END
        """
    )
    connection.commit()
    return connection


def central_bank_release_source_ids(mapping: Mapping[str, Any]) -> tuple[str, ...]:
    """Return monetary-authority policy release transports only."""

    source_ids: list[str] = []
    for row in mapping.get("currencies") or []:
        if not isinstance(row, Mapping):
            continue
        for source_id in row.get("release_source_ids") or []:
            normalized = str(source_id or "").strip()
            if normalized and normalized not in source_ids:
                source_ids.append(normalized)
    return tuple(source_ids)


def statistical_release_source_ids(mapping: Mapping[str, Any]) -> tuple[str, ...]:
    """Return explicitly mapped official statistical fast-lane transports."""

    source_ids: list[str] = []
    for row in mapping.get("currencies") or []:
        if not isinstance(row, Mapping):
            continue
        for source_id in row.get("statistical_release_source_ids") or []:
            normalized = str(source_id or "").strip()
            if normalized and normalized not in source_ids:
                source_ids.append(normalized)
    return tuple(source_ids)


def central_bank_communication_source_ids(
    mapping: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return governed authoritative policy-communication transports."""

    source_ids: list[str] = []
    for row in mapping.get("currencies") or []:
        if not isinstance(row, Mapping):
            continue
        for source_id in row.get("communication_source_ids") or []:
            normalized = str(source_id or "").strip()
            if normalized and normalized not in source_ids:
                source_ids.append(normalized)
    return tuple(source_ids)


def official_fast_lane_source_ids(mapping: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the stable union of governed official observation transports."""

    source_ids: list[str] = []
    for source_id in (
        *central_bank_release_source_ids(mapping),
        *central_bank_communication_source_ids(mapping),
        *statistical_release_source_ids(mapping),
    ):
        if source_id not in source_ids:
            source_ids.append(source_id)
    return tuple(source_ids)


def selected_sources(
    config: Mapping[str, Any], mapping: Mapping[str, Any]
) -> list[dict[str, Any]]:
    requested = set(official_fast_lane_source_ids(mapping))
    release_requested = {
        *central_bank_release_source_ids(mapping),
        *statistical_release_source_ids(mapping),
    }
    communication_requested = set(
        central_bank_communication_source_ids(mapping)
    )
    configured = {
        str(row.get("source_id") or ""): news.source_config_lineage(row)
        for row in config.get("sources") or []
        if isinstance(row, Mapping) and row.get("source_id")
    }
    missing = sorted(requested - set(configured))
    if missing:
        raise ValueError(f"official fast-lane sources missing from config: {missing}")
    invalid_communications = sorted(
        source_id
        for source_id in communication_requested
        if configured[source_id].get("verified") is not True
        or not news.configured_source_is_direct(configured[source_id])
        or not any(
            str(domain or "").strip()
            for domain in configured[source_id].get("trusted_domains") or []
        )
    )
    if invalid_communications:
        raise ValueError(
            "official fast-lane communications must be direct, verified, and "
            f"domain-bound: {invalid_communications}"
        )
    output = [configured[source_id] for source_id in sorted(requested)]
    if len(release_requested) < 21:
        raise ValueError("official fast lane must bind at least one release source per currency")
    return output


def due_for_fast_poll(
    source: Mapping[str, Any],
    source_state: Mapping[str, Any],
    policy: Mapping[str, Any],
    now: dt.datetime,
) -> bool:
    configured_contract = str(source.get("source_contract_id") or "")
    if configured_contract and configured_contract != str(
        source_state.get("source_contract_id") or ""
    ):
        return True
    last_attempt = news.parse_datetime(source_state.get("last_attempt_utc"))
    if last_attempt is None:
        return True
    default_interval = news.safe_float(policy.get("rss_poll_interval_sec"), 180.0)
    interval = news.safe_float(source.get("poll_interval_sec"), default_interval)
    if news.burst_poll_active(source, now):
        interval = news.safe_float(source.get("burst_poll_interval_sec"), interval)
    errors = int(source_state.get("consecutive_errors") or 0)
    if errors:
        interval = max(interval, min(3600.0, 30.0 * (2 ** min(errors, 7))))
    return (now - last_attempt).total_seconds() >= max(15.0, interval)


VOLATILE_ITEM_FIELDS = {
    "causal_known_utc",
    "first_seen_utc",
    "last_seen_utc",
    "retrieved_at_utc",
    "retrieved_utc",
}


def stable_item_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in row.items()
        if str(key) not in VOLATILE_ITEM_FIELDS
        and not str(key).startswith("source_listing_")
    }


def stable_item_key(row: Mapping[str, Any]) -> str:
    external_id = str(row.get("external_id") or "").strip()
    canonical_url = next(
        (
            str(row.get(key) or "").strip()
            for key in ("source_url", "url", "publisher_url")
            if str(row.get(key) or "").strip()
        ),
        "",
    )
    # Publisher IDs or canonical URLs define story identity. Headline/detail
    # changes at the same URL are revisions of an existing item, not a new
    # release identity. Headline plus publisher timestamp is only a fallback.
    if external_id:
        material = f"external_id|{external_id}"
    elif canonical_url:
        material = f"canonical_url|{canonical_url}"
    else:
        fallback = [
            row.get("headline"),
            row.get("title"),
            row.get("published_utc"),
            row.get("published_at_utc"),
        ]
        material = "fallback|" + "|".join(
            str(value or "").strip() for value in fallback
        )
    if not material.strip("|"):
        material = json.dumps(stable_item_payload(row), sort_keys=True, default=str)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def publisher_time_is_eligible(
    row: Mapping[str, Any], *, first_seen: dt.datetime
) -> bool:
    published = news.parse_datetime(
        row.get("published_utc") or row.get("published_at_utc")
    )
    if published is None:
        return False
    # A feed can expose a previously omitted old item on a later page. New URL
    # identity therefore is not enough: the publisher clock must belong to the
    # fast-lane era. A five-minute activation tolerance covers startup races,
    # not historical backfill.
    if published < ACTIVATED_UTC - dt.timedelta(minutes=5):
        return False
    if published > first_seen + dt.timedelta(minutes=10):
        return False
    if bool(row.get("published_time_inferred")) and not bool(
        row.get("source_listing_new_item")
    ):
        return False
    return True


def observation_batch_is_bootstrap(
    *, initializing_source: bool, rows: Sequence[Mapping[str, Any]]
) -> bool:
    """Keep explicitly quarantined recovery rows bootstrap after state drift."""

    return bool(
        initializing_source
        or any(row.get("engineering_bootstrap_recovery") is True for row in rows)
    )


def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def build_raw_quote_capture(
    *,
    observation_id: str,
    first_seen: dt.datetime,
    input_prospective_observation: bool,
    quote_payload: Mapping[str, Any] | None,
    captured_utc: dt.datetime,
    snapshot_error: str = "",
) -> dict[str, Any]:
    """Freeze the complete executable quote universe at raw append time.

    The publisher snapshot is a current-connection executable cache, so an
    unchanged instrument's venue timestamp may precede the news clock.  The
    local capture clock is the event attachment; every venue timestamp remains
    preserved and must still be fresh at that clock.  No later mapper may
    replace or improve this evidence.
    """

    event_time = first_seen
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=dt.timezone.utc)
    event_time = event_time.astimezone(dt.timezone.utc)
    captured = captured_utc
    if captured.tzinfo is None:
        captured = captured.replace(tzinfo=dt.timezone.utc)
    captured = captured.astimezone(dt.timezone.utc)
    capture_latency = (captured - event_time).total_seconds()
    activation_eligible = bool(
        input_prospective_observation
        and event_time >= QUOTE_CAPTURE_ACTIVATED_UTC
        and captured >= QUOTE_CAPTURE_ACTIVATED_UTC
    )
    payload = dict(quote_payload or {})
    quotes_raw = payload.get("quotes")
    quotes = (
        {str(name).upper(): raw for name, raw in quotes_raw.items()}
        if isinstance(quotes_raw, Mapping)
        else {}
    )
    coverage_raw = payload.get("coverage")
    coverage = coverage_raw if isinstance(coverage_raw, Mapping) else {}
    transport_raw = payload.get("transport")
    transport = transport_raw if isinstance(transport_raw, Mapping) else {}
    generated = news.parse_datetime(payload.get("generated_utc"))
    snapshot_age = (
        None if generated is None else (captured - generated).total_seconds()
    )
    provided = set(quotes)
    expected = set(EXPECTED_QUOTE_INSTRUMENTS)
    missing = sorted(expected - provided)
    unexpected = sorted(provided - expected)
    observed_quotes: dict[str, dict[str, Any]] = {}
    invalid_instruments: dict[str, str] = {}
    for instrument in EXPECTED_QUOTE_INSTRUMENTS:
        raw = quotes.get(instrument)
        if not isinstance(raw, Mapping):
            continue
        bid = _finite_number(raw.get("bid"))
        ask = _finite_number(raw.get("ask"))
        pip = _finite_number(raw.get("pip"))
        quote_time = news.parse_datetime(raw.get("time"))
        reason = ""
        if bid is None or ask is None or ask <= bid:
            reason = "invalid_executable_bid_ask"
        elif pip is None or pip <= 0.0:
            reason = "invalid_pip"
        elif quote_time is None:
            reason = "invalid_quote_time"
        else:
            age = (captured - quote_time).total_seconds()
            event_offset = (quote_time - event_time).total_seconds()
            if age < -QUOTE_CAPTURE_MAX_FUTURE_SKEW_SECONDS:
                reason = "quote_clock_ahead_of_capture"
            elif age > QUOTE_CAPTURE_MAX_QUOTE_AGE_SECONDS:
                reason = "stale_quote"
            elif event_offset > QUOTE_CAPTURE_MAX_ENTRY_OFFSET_SECONDS:
                reason = "quote_after_entry_window"
        if reason:
            invalid_instruments[instrument] = reason
            continue
        assert quote_time is not None
        observed_quotes[instrument] = {
            "bid": bid,
            "ask": ask,
            "pip": pip,
            "quote_time_utc": iso_utc(quote_time),
            "age_seconds": round((captured - quote_time).total_seconds(), 6),
            "event_offset_seconds": round(
                (quote_time - event_time).total_seconds(), 6
            ),
            "source": str(raw.get("source") or ""),
        }

    metadata_reasons: list[str] = []
    payload_quote_count = _finite_number(payload.get("quote_count"))
    current_quote_count = _finite_number(coverage.get("current_quote_count"))
    last_known_quote_count = _finite_number(
        coverage.get("last_known_quote_count")
    )
    retained_quote_count = _finite_number(
        coverage.get("retained_last_known_count")
    )
    connection_generation = _finite_number(payload.get("connection_generation"))
    coverage_generation = _finite_number(coverage.get("connection_generation"))
    if generated is None:
        metadata_reasons.append("snapshot_generated_clock_missing")
    elif snapshot_age is not None and (
        snapshot_age < -QUOTE_CAPTURE_MAX_FUTURE_SKEW_SECONDS
        or snapshot_age > QUOTE_CAPTURE_MAX_SNAPSHOT_AGE_SECONDS
    ):
        metadata_reasons.append(f"snapshot_age_seconds:{snapshot_age:.3f}")
    if payload_quote_count != EXPECTED_QUOTE_COUNT:
        metadata_reasons.append(
            f"payload_quote_count:{payload_quote_count!s}!={EXPECTED_QUOTE_COUNT}"
        )
    if current_quote_count != EXPECTED_QUOTE_COUNT:
        metadata_reasons.append(
            f"current_quote_count:{current_quote_count!s}!={EXPECTED_QUOTE_COUNT}"
        )
    if last_known_quote_count != EXPECTED_QUOTE_COUNT:
        metadata_reasons.append(
            f"last_known_quote_count:{last_known_quote_count!s}!={EXPECTED_QUOTE_COUNT}"
        )
    if retained_quote_count != 0.0:
        metadata_reasons.append(
            f"retained_last_known_count:{retained_quote_count!s}!=0"
        )
    if connection_generation is None or coverage_generation is None:
        metadata_reasons.append("connection_generation_missing")
    elif connection_generation != coverage_generation:
        metadata_reasons.append("connection_generation_mismatch")
    if missing:
        metadata_reasons.append(f"missing_instruments:{len(missing)}")
    if unexpected:
        metadata_reasons.append(f"unexpected_instruments:{len(unexpected)}")
    if invalid_instruments:
        metadata_reasons.append(
            f"invalid_instruments:{len(invalid_instruments)}"
        )

    timing_quality = "prospective_exact_live_quote"
    invalid_reason = ""
    if not input_prospective_observation:
        timing_quality = "diagnostic_nonprospective_observation"
        invalid_reason = "input_not_prospective"
    elif not activation_eligible:
        timing_quality = "diagnostic_pre_capture_activation"
        invalid_reason = "raw_quote_capture_cohort_not_active"
    elif capture_latency < 0.0 or capture_latency > QUOTE_CAPTURE_MAX_LATENCY_SECONDS:
        timing_quality = "prospective_clock_missed"
        invalid_reason = f"capture_latency_seconds:{capture_latency:.3f}"
    elif snapshot_error:
        timing_quality = "prospective_quote_snapshot_unavailable"
        invalid_reason = f"snapshot_read_error:{snapshot_error}"
    elif not payload:
        timing_quality = "prospective_quote_snapshot_unavailable"
        invalid_reason = "snapshot_payload_missing"
    elif metadata_reasons:
        timing_quality = "prospective_quote_coverage_invalid"
        invalid_reason = ";".join(metadata_reasons)

    proof_quotes = (
        observed_quotes
        if timing_quality == "prospective_exact_live_quote"
        and len(observed_quotes) == EXPECTED_QUOTE_COUNT
        else {}
    )
    if timing_quality == "prospective_exact_live_quote" and not proof_quotes:
        timing_quality = "prospective_quote_coverage_invalid"
        invalid_reason = (
            f"valid_quote_count:{len(observed_quotes)}!={EXPECTED_QUOTE_COUNT}"
        )
    return {
        "schema_version": QUOTE_CAPTURE_SCHEMA_VERSION,
        "capture_contract_id": QUOTE_CAPTURE_CONTRACT_ID,
        "capture_cohort_id": QUOTE_CAPTURE_COHORT_ID,
        "capture_activated_utc": iso_utc(QUOTE_CAPTURE_ACTIVATED_UTC),
        "observation_id": str(observation_id),
        "event_first_known_utc": iso_utc(event_time),
        "captured_utc": iso_utc(captured),
        "capture_latency_seconds": round(capture_latency, 6),
        "capture_activation_eligible": activation_eligible,
        "input_prospective_observation": bool(input_prospective_observation),
        "timing_quality": timing_quality,
        "invalid_reason": invalid_reason,
        "expected_quote_count": EXPECTED_QUOTE_COUNT,
        "expected_instruments": list(EXPECTED_QUOTE_INSTRUMENTS),
        "instrument_universe_sha256": EXPECTED_QUOTE_UNIVERSE_SHA256,
        "observed_valid_quote_count": len(observed_quotes),
        "proof_quote_count": len(proof_quotes),
        "quote_count": len(proof_quotes),
        "missing_instruments": missing,
        "unexpected_instruments": unexpected,
        "invalid_instruments": invalid_instruments,
        "observed_quotes": observed_quotes,
        "quotes": proof_quotes,
        "quote_snapshot_generated_utc": (
            "" if generated is None else iso_utc(generated)
        ),
        "quote_snapshot_age_seconds": (
            None if snapshot_age is None else round(snapshot_age, 6)
        ),
        "quote_snapshot_producer": str(payload.get("producer") or ""),
        "quote_transport_source": str(transport.get("source") or ""),
        "quote_transport_sequence": transport.get("sequence"),
        "connection_generation": (
            None if connection_generation is None else int(connection_generation)
        ),
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_promote": False,
    }


def insert_raw_quote_capture(
    connection: sqlite3.Connection,
    capture: Mapping[str, Any],
) -> bool:
    observation_id = str(capture.get("observation_id") or "")
    capture_id = hashlib.sha256(
        f"{observation_id}|{QUOTE_CAPTURE_CONTRACT_ID}|{QUOTE_CAPTURE_COHORT_ID}".encode(
            "utf-8"
        )
    ).hexdigest()
    payload_json = json.dumps(
        dict(capture), sort_keys=True, separators=(",", ":"), default=str
    )
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO official_release_quote_capture (
            capture_id, observation_id, event_first_known_utc, captured_utc,
            input_prospective_observation, capture_activation_eligible,
            timing_quality, observed_valid_quote_count, proof_quote_count,
            capture_payload_json, research_only, execution_eligible,
            can_authorize, can_promote, capture_contract_id,
            capture_cohort_id, capture_activated_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            capture_id,
            observation_id,
            str(capture.get("event_first_known_utc") or ""),
            str(capture.get("captured_utc") or ""),
            int(bool(capture.get("input_prospective_observation"))),
            int(bool(capture.get("capture_activation_eligible"))),
            str(capture.get("timing_quality") or ""),
            int(capture.get("observed_valid_quote_count") or 0),
            int(capture.get("proof_quote_count") or 0),
            payload_json,
            1,
            0,
            0,
            0,
            QUOTE_CAPTURE_CONTRACT_ID,
            QUOTE_CAPTURE_COHORT_ID,
            iso_utc(QUOTE_CAPTURE_ACTIVATED_UTC),
        ),
    )
    return bool(cursor.rowcount)


def append_observations(
    connection: sqlite3.Connection,
    *,
    source: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    first_seen: dt.datetime,
    listing_bootstrap: bool,
    observation_clock: Mapping[str, Any],
    quote_snapshot_loader: Callable[[], Mapping[str, Any]] | None = None,
    quote_captured_utc: dt.datetime | None = None,
) -> tuple[int, int, list[dict[str, Any]]]:
    inserted = 0
    duplicates = 0
    emitted: list[dict[str, Any]] = []
    clock_trusted = bool(news.prospective_clock_attestation(observation_clock))
    clock_source = str(observation_clock.get("source") or "")
    quote_payload: Mapping[str, Any] = {}
    quote_snapshot_error = ""
    quote_snapshot_loaded = False
    capture_clock: dt.datetime | None = quote_captured_utc
    normalized_first_seen = first_seen
    if normalized_first_seen.tzinfo is None:
        normalized_first_seen = normalized_first_seen.replace(
            tzinfo=dt.timezone.utc
        )
    normalized_first_seen = normalized_first_seen.astimezone(dt.timezone.utc)
    if capture_clock is not None:
        if capture_clock.tzinfo is None:
            capture_clock = capture_clock.replace(tzinfo=dt.timezone.utc)
        capture_clock = capture_clock.astimezone(dt.timezone.utc)
    for row in rows:
        item_key = stable_item_key(row)
        # Detail availability is monotonic evidence.  The shared source state
        # deliberately avoids refetching an unchanged official body, so a
        # later RSS poll naturally yields the headline-only representation.
        # Do not append that lossy projection as a new material revision after
        # this lane has already retained the first-party body and its exact
        # availability clock.  A later enriched body remains appendable.
        if not bool(row.get("detail_enriched")):
            prior_payloads = connection.execute(
                """
                SELECT raw_payload_json
                FROM official_release_observation
                WHERE source_id = ? AND item_key = ?
                """,
                (str(source.get("source_id") or ""), item_key),
            ).fetchall()
            if any(
                bool(json.loads(str(value[0])).get("detail_enriched"))
                for value in prior_payloads
            ):
                duplicates += 1
                continue
        payload = stable_item_payload(row)
        payload_json = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=str
        )
        material_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        observation_id = hashlib.sha256(
            f"{source.get('source_id')}|{item_key}|{material_sha256}".encode("utf-8")
        ).hexdigest()
        identity_preexisting = bool(
            connection.execute(
                """
                SELECT 1 FROM official_release_observation
                WHERE source_id = ? AND item_key = ? LIMIT 1
                """,
                (str(source.get("source_id") or ""), item_key),
            ).fetchone()
        )
        publisher_time_eligible = publisher_time_is_eligible(
            row, first_seen=first_seen
        )
        # A changed parser/detail projection for a pre-existing listing item
        # is useful provenance, but is not a new causal release.  Only a
        # publisher item identity absent from the bootstrap ledger can become
        # prospectively eligible.  Revisions need a separately proven native
        # revision clock and therefore remain fail-closed here.
        prospective = bool(
            clock_trusted
            and not listing_bootstrap
            and not identity_preexisting
            and publisher_time_eligible
        )
        values = (
            observation_id,
            str(source.get("source_id") or ""),
            str(source.get("source_contract_id") or ""),
            str(source.get("source_cohort_id") or ""),
            item_key,
            material_sha256,
            iso_utc(first_seen),
            int(prospective),
            int(listing_bootstrap),
            int(identity_preexisting),
            int(publisher_time_eligible),
            int(clock_trusted),
            clock_source,
            payload_json,
            1,
            0,
            0,
            CONTRACT_ID,
            COLLECTOR_COHORT_ID,
        )
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO official_release_observation (
                observation_id, source_id, source_contract_id, source_cohort_id,
                item_key, material_sha256, first_seen_utc,
                prospective_observation, listing_bootstrap, identity_preexisting,
                publisher_time_eligible,
                observation_clock_trusted, observation_clock_source,
                raw_payload_json, research_only, execution_eligible,
                can_authorize, collector_contract_id, collector_cohort_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        if cursor.rowcount:
            if capture_clock is None:
                capture_clock = utc_now()
            capture_activation_eligible = bool(
                prospective
                and normalized_first_seen >= QUOTE_CAPTURE_ACTIVATED_UTC
                and capture_clock >= QUOTE_CAPTURE_ACTIVATED_UTC
            )
            if capture_activation_eligible and not quote_snapshot_loaded:
                quote_snapshot_loaded = True
                if quote_snapshot_loader is None:
                    quote_snapshot_error = "loader_not_configured"
                else:
                    try:
                        loaded = quote_snapshot_loader()
                        if isinstance(loaded, Mapping):
                            quote_payload = loaded
                        else:
                            quote_snapshot_error = "invalid_payload_type"
                    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
                        # Raw news evidence remains available even when the
                        # local quote transport is temporarily unavailable.
                        # Persist the failed attempt; never retry this event.
                        quote_snapshot_error = type(exc).__name__
            capture = build_raw_quote_capture(
                observation_id=observation_id,
                first_seen=normalized_first_seen,
                input_prospective_observation=prospective,
                quote_payload=quote_payload,
                captured_utc=capture_clock,
                snapshot_error=quote_snapshot_error,
            )
            if not insert_raw_quote_capture(connection, capture):
                raise sqlite3.IntegrityError(
                    "new official observation did not create one immutable quote capture"
                )
            inserted += 1
            emitted.append(
                {
                    "observation_id": observation_id,
                    "source_id": str(source.get("source_id") or ""),
                    "first_seen_utc": iso_utc(first_seen),
                    "material_sha256": material_sha256,
                    "prospective_observation": prospective,
                    "listing_bootstrap": bool(listing_bootstrap),
                    "identity_preexisting": identity_preexisting,
                    "publisher_time_eligible": publisher_time_eligible,
                    "headline": str(row.get("headline") or row.get("title") or "")[:300],
                    "source_url": str(
                        row.get("source_url")
                        or row.get("url")
                        or row.get("publisher_url")
                        or ""
                    )[:1000],
                    "quote_capture_contract_id": QUOTE_CAPTURE_CONTRACT_ID,
                    "quote_capture_cohort_id": QUOTE_CAPTURE_COHORT_ID,
                    "quote_capture_timing_quality": str(
                        capture.get("timing_quality") or ""
                    ),
                    "quote_capture_observed_valid_quote_count": int(
                        capture.get("observed_valid_quote_count") or 0
                    ),
                    "quote_capture_proof_quote_count": int(
                        capture.get("proof_quote_count") or 0
                    ),
                }
            )
        else:
            duplicates += 1
    connection.commit()
    return inserted, duplicates, emitted


def database_counts(connection: sqlite3.Connection) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT COUNT(*),
               COALESCE(SUM(prospective_observation), 0),
               COALESCE(SUM(listing_bootstrap), 0),
               COUNT(DISTINCT source_id)
        FROM official_release_observation
        """
    ).fetchone()
    capture_row = connection.execute(
        """
        SELECT COUNT(*),
               COALESCE(SUM(capture_activation_eligible), 0),
               COALESCE(SUM(proof_quote_count = ?), 0),
               COALESCE(SUM(timing_quality != 'prospective_exact_live_quote'), 0)
        FROM official_release_quote_capture
        """,
        (EXPECTED_QUOTE_COUNT,),
    ).fetchone()
    return {
        "observations": int(row[0]),
        "prospective_observations": int(row[1]),
        "bootstrap_observations": int(row[2]),
        "observed_sources": int(row[3]),
        "quote_capture_attempts": int(capture_row[0]),
        "quote_capture_activation_eligible": int(capture_row[1]),
        "quote_capture_exact_all68": int(capture_row[2]),
        "quote_capture_diagnostic_or_invalid": int(capture_row[3]),
    }


def publish_heartbeat(
    path: Path,
    *,
    status: str,
    phase: str,
    cycle_started: dt.datetime,
    progress_sequence: int,
    details: Mapping[str, Any] | None = None,
) -> None:
    now = utc_now()
    try:
        write_json_atomic(
            path,
            {
            "schema_version": SCHEMA_VERSION,
            "collector_contract_id": CONTRACT_ID,
            "collector_cohort_id": COLLECTOR_COHORT_ID,
            "quote_capture_contract_id": QUOTE_CAPTURE_CONTRACT_ID,
            "quote_capture_cohort_id": QUOTE_CAPTURE_COHORT_ID,
            "quote_capture_activated_utc": iso_utc(QUOTE_CAPTURE_ACTIVATED_UTC),
            "expected_quote_count": EXPECTED_QUOTE_COUNT,
            "quote_universe_sha256": EXPECTED_QUOTE_UNIVERSE_SHA256,
            "generated_utc": iso_utc(now),
            "heartbeat_utc": iso_utc(now),
            "cycle_started_utc": iso_utc(cycle_started),
            "cycle_in_progress": status == "running_cycle",
            "status": status,
            "phase": phase,
            "progress_sequence": progress_sequence,
            "details": dict(details or {}),
            "policy": {
                "research_only": True,
                "execution_eligible": False,
                "can_authorize": False,
                "can_promote": False,
                "broker_access": False,
            },
            },
        )
    except OSError:
        # A Windows reader or antivirus scanner can briefly deny replacement
        # of the heartbeat target.  Keep collecting; an old heartbeat remains
        # fail-closed and the supervisor can restart if replacement never
        # recovers.  State/snapshot writes still raise on persistent failure.
        return


def run_cycle(
    *,
    config_path: Path = CONFIG_PATH,
    central_bank_map_path: Path = CENTRAL_BANK_MAP_PATH,
    database_path: Path = DATABASE_PATH,
    state_path: Path = STATE_PATH,
    snapshot_path: Path = SNAPSHOT_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
    quote_path: Path = QUOTE_PATH,
    now: dt.datetime | None = None,
    force: bool = False,
) -> dict[str, Any]:
    cycle_started = now or utc_now()
    config = read_json(config_path, {})
    mapping = read_json(central_bank_map_path, {})
    source_map_contract_id = str(mapping.get("contract_id") or "").strip()
    if not source_map_contract_id:
        raise ValueError("official central-bank source map has no contract_id")
    sources = selected_sources(config, mapping)
    policy = config.get("policy") if isinstance(config.get("policy"), Mapping) else {}
    state = read_json(state_path, {})
    source_states = state.get("sources") if isinstance(state.get("sources"), Mapping) else {}
    connection = open_database(database_path)
    attempted = 0
    completed = 0
    inserted = 0
    duplicates = 0
    latest: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    progress_sequence = 0
    publish_heartbeat(
        heartbeat_path,
        status="running_cycle",
        phase="starting_cycle",
        cycle_started=cycle_started,
        progress_sequence=progress_sequence,
        details={"configured_sources": len(sources)},
    )
    try:
        for source in news.collection_order(sources, cycle_started):
            source_id = str(source.get("source_id") or "")
            prior = news.migrate_derived_source_state(
                source,
                source_states.get(source_id) or {},
                now=cycle_started,
            )
            # Persist a metadata-only derived-lineage adoption even when the
            # source is not due.  Validators, cadence, known identities and
            # detail clocks remain untouched, and no refetch/reset is caused.
            source_states[source_id] = prior
            progress_sequence += 1
            publish_heartbeat(
                heartbeat_path,
                status="running_cycle",
                phase="polling_official_sources",
                cycle_started=cycle_started,
                progress_sequence=progress_sequence,
                details={
                    "source_id": source_id,
                    "completed_sources": completed,
                    "configured_sources": len(sources),
                    "attempted_sources": attempted,
                },
            )
            if news.source_runtime_status(source) != "enabled":
                prior["operational_status"] = news.source_runtime_status(source)
                source_states[source_id] = prior
                completed += 1
                continue
            # A previous code cohort may have persisted ``unsupported`` for a
            # newly introduced source kind.  Once the current runtime accepts
            # the source, clear that stale diagnostic even when the source is
            # not due for another network request yet.
            prior["operational_status"] = "enabled"
            if not force and not due_for_fast_poll(source, prior, policy, cycle_started):
                completed += 1
                continue
            attempted += 1
            observed_start, clock = news.normalized_observation_time(
                cycle_started if now is not None else news.utc_now()
            )
            if not news.prospective_clock_attestation(clock):
                errors.append({"source_id": source_id, "error": "clock_integrity"})
                completed += 1
                continue
            was_bootstrap = not bool(prior.get("fast_lane_bootstrap_complete"))
            rows, updated = news.fetch_source(
                source,
                prior,
                timeout_sec=max(2.0, news.safe_float(policy.get("request_timeout_sec"), 25.0)),
                maximum_bytes=max(
                    100_000,
                    int(news.safe_float(policy.get("maximum_feed_bytes"), 8_000_000)),
                ),
                now=observed_start,
            )
            observed_complete, completion_clock = news.normalized_observation_time(
                observed_start if now is not None else news.utc_now()
            )
            updated["fast_lane_bootstrap_complete"] = bool(
                updated.get("last_success_utc") or prior.get("fast_lane_bootstrap_complete")
            )
            updated["fast_lane_collector_contract_id"] = CONTRACT_ID
            source_states[source_id] = updated
            if updated.get("last_error"):
                errors.append(
                    {"source_id": source_id, "error": str(updated.get("last_error"))[:300]}
                )
            if rows:
                observation_bootstrap = observation_batch_is_bootstrap(
                    initializing_source=was_bootstrap,
                    rows=rows,
                )
                added, repeated, emitted = append_observations(
                    connection,
                    source=source,
                    rows=rows,
                    first_seen=observed_complete,
                    listing_bootstrap=observation_bootstrap,
                    observation_clock=completion_clock,
                    quote_snapshot_loader=lambda: load_quote_snapshot(quote_path),
                    quote_captured_utc=(
                        observed_complete if now is not None else None
                    ),
                )
                inserted += added
                duplicates += repeated
                latest.extend(emitted)
            completed += 1
        counts = database_counts(connection)
    finally:
        connection.close()
    generated = utc_now() if now is None else cycle_started
    state_payload = {
        "schema_version": SCHEMA_VERSION,
        "collector_contract_id": CONTRACT_ID,
        "collector_cohort_id": COLLECTOR_COHORT_ID,
        "quote_capture_contract_id": QUOTE_CAPTURE_CONTRACT_ID,
        "quote_capture_cohort_id": QUOTE_CAPTURE_COHORT_ID,
        "quote_capture_activated_utc": iso_utc(QUOTE_CAPTURE_ACTIVATED_UTC),
        "source_map_contract_id": source_map_contract_id,
        "updated_utc": iso_utc(generated),
        "sources": source_states,
        "policy": {
            "research_only": True,
            "execution_eligible": False,
            "can_authorize": False,
            "can_promote": False,
        },
    }
    write_json_atomic(state_path, state_payload)
    result = {
        "schema_version": SCHEMA_VERSION,
        "collector_contract_id": CONTRACT_ID,
        "collector_cohort_id": COLLECTOR_COHORT_ID,
        "quote_capture_contract_id": QUOTE_CAPTURE_CONTRACT_ID,
        "quote_capture_cohort_id": QUOTE_CAPTURE_COHORT_ID,
        "quote_capture_activated_utc": iso_utc(QUOTE_CAPTURE_ACTIVATED_UTC),
        "expected_quote_count": EXPECTED_QUOTE_COUNT,
        "quote_universe_sha256": EXPECTED_QUOTE_UNIVERSE_SHA256,
        "source_map_contract_id": source_map_contract_id,
        "generated_utc": iso_utc(generated),
        "configured_currency_count": len(mapping.get("currencies") or []),
        "configured_release_source_count": len(sources),
        "attempted_sources": attempted,
        "completed_sources": completed,
        "inserted_observations": inserted,
        "duplicate_observations": duplicates,
        "counts": counts,
        "latest_inserted": latest[-50:],
        "errors": errors,
        "policy": {
            "research_only": True,
            "execution_eligible": False,
            "can_authorize": False,
            "can_promote": False,
            "broker_access": False,
            "classification": "none_raw_observation_only",
        },
    }
    write_json_atomic(snapshot_path, result)
    publish_heartbeat(
        heartbeat_path,
        status="cycle_complete",
        phase="cycle_complete",
        cycle_started=cycle_started,
        progress_sequence=progress_sequence + 1,
        details={
            "configured_sources": len(sources),
            "attempted_sources": attempted,
            "completed_sources": completed,
            "inserted_observations": inserted,
            "errors": len(errors),
        },
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--central-bank-map", type=Path, default=CENTRAL_BANK_MAP_PATH)
    parser.add_argument("--database", type=Path, default=DATABASE_PATH)
    parser.add_argument("--state", type=Path, default=STATE_PATH)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT_PATH)
    parser.add_argument("--quote-snapshot", type=Path, default=QUOTE_PATH)
    parser.add_argument("--interval-sec", type=float, default=15.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    first = True
    while True:
        result = run_cycle(
            config_path=args.config,
            central_bank_map_path=args.central_bank_map,
            database_path=args.database,
            state_path=args.state,
            snapshot_path=args.snapshot,
            heartbeat_path=args.heartbeat,
            quote_path=args.quote_snapshot,
            force=bool(args.force and first),
        )
        print(json.dumps(result, sort_keys=True), flush=True)
        first = False
        if args.once:
            return 0
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(max(5.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
