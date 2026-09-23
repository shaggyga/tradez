#!/usr/bin/env python3
"""Prospective two-sided macro-release breakout evidence.

Numeric releases do not have a defensible currency side without a causally
captured pre-release consensus and an explicit interpretation contract.  This
worker therefore watches both currency directions and records an entry only
after a broad, cost-aware cross-pair confirmation.  It is research-only: it
cannot trade, promote, authorize, or change Practice 007 policy.

The 2026-08-13 US PPI case was used to define this contract and is permanently
excluded from the prospective cohort beginning on 2026-08-15.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from oanda_instrument_pips import fallback_pip_size
from oanda_local_news_sentiment import normalized_observation_time
from oanda_policy_statement_breakout_research import (
    evaluate_breakout_setup,
    finite,
    iso,
    load_quote_rows,
    parse_utc,
)
from oanda_worker_heartbeat import WorkerHeartbeat


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
CONFIG = ROOT / "config" / "news_sources_v1.json"
NEWS_DB = DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
QUOTE_DB = STATE / "practice_007_quote_intensity_shadow_v1.sqlite"
DATABASE = STATE / "macro_release_breakout_research_v1.sqlite"
OUTPUT = STATE / "macro_release_breakout_research_v1.json"
HEARTBEAT = STATE / "macro_release_breakout_research_heartbeat_v1.json"
REPORT = (
    DATA
    / "reports"
    / "major_move_case_audits"
    / "US_PPI_BREAKOUT_20260813.md"
)

UTC = dt.timezone.utc
CONTRACT_ID = "two_sided_macro_release_breakout_v1_20260815"
COHORT_START = dt.datetime(2026, 8, 15, 14, 55, tzinfo=UTC)
COHORTS = {
    5: "two_sided_macro_release_breakout_h5_v1_20260815",
    15: "two_sided_macro_release_breakout_h15_v1_20260815",
    30: "two_sided_macro_release_breakout_h30_v1_20260815",
    60: "two_sided_macro_release_breakout_h60_v1_20260815",
}
LIQUID_USD_CROSSES = {
    "EUR_USD",
    "GBP_USD",
    "AUD_USD",
    "NZD_USD",
    "USD_JPY",
    "USD_CHF",
    "USD_CAD",
}
MIN_CONFIRMING_PAIRS = 4
MIN_STABILIZATION_MINUTES = 3
MIN_PERSISTENT_CONFIRMATION_MINUTES = 2
FROZEN_BLS_CURRENT_RELEASE_SOURCE_IDS = {"bls_ppi_current_release_v1"}


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    os.replace(temporary, path)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def configured_bls_current_release_source_ids(
    config_path: Path = CONFIG,
) -> list[str]:
    """Return frozen and currently configured BLS release source lineages."""

    source_ids = set(FROZEN_BLS_CURRENT_RELEASE_SOURCE_IDS)
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return sorted(source_ids)
    for source in config.get("sources") or []:
        if not isinstance(source, Mapping):
            continue
        if str(source.get("kind") or "") != "bls_current_release":
            continue
        source_id = str(source.get("source_id") or "").strip()
        if source_id:
            source_ids.add(source_id)
    return sorted(source_ids)


def load_structured_macro_events(
    news_db: Path,
    heartbeat: WorkerHeartbeat | None = None,
    *,
    config_path: Path = CONFIG,
) -> list[dict[str, Any]]:
    """Load only causally observed, first-party structured release records."""

    if not news_db.exists():
        return []
    connection = sqlite3.connect(
        f"file:{news_db.as_posix()}?mode=ro", uri=True, timeout=10
    )
    if heartbeat is not None:
        heartbeat.update(phase="querying_bls_candidates")
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(articles)")
    }
    source_ids = configured_bls_current_release_source_ids(config_path)
    try:
        if "source_id" in columns and source_ids:
            clauses = " OR ".join(
                "(source_id=? OR source_id GLOB ?)" for _ in source_ids
            )
            parameters = tuple(
                value
                for source_id in source_ids
                for value in (source_id, f"{source_id}:*")
            )
            rows = connection.execute(
                f"""
                SELECT event_id,payload_json,first_seen_utc FROM articles
                WHERE {clauses}
                ORDER BY first_seen_utc,event_id
                """,
                parameters,
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT event_id,payload_json,first_seen_utc FROM articles
                WHERE CASE WHEN json_valid(payload_json)
                    THEN json_extract(payload_json,'$.source_kind') = 'bls_current_release'
                    ELSE 0 END
                ORDER BY first_seen_utc,event_id
                """
            ).fetchall()
    except sqlite3.OperationalError:
        rows = connection.execute(
            "SELECT event_id,payload_json,first_seen_utc FROM articles "
            "ORDER BY first_seen_utc,event_id"
        ).fetchall()
    connection.close()
    output: list[dict[str, Any]] = []
    if heartbeat is not None:
        heartbeat.mark_progress(
            phase="processing_bls_candidates",
            candidate_articles=len(rows),
            candidate_articles_processed=0,
        )
    for row_number, (event_id, payload_json, first_seen) in enumerate(rows, 1):
        if heartbeat is not None and row_number % 100 == 0:
            heartbeat.mark_progress(
                phase="processing_bls_candidates",
                candidate_articles=len(rows),
                candidate_articles_processed=row_number,
            )
        try:
            payload = json.loads(payload_json or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if (
            str(payload.get("source_kind") or "") != "bls_current_release"
            or not bool(payload.get("source_verified"))
            or not bool(payload.get("source_direct"))
            or bool(payload.get("source_listing_bootstrap"))
        ):
            continue
        known = parse_utc(payload.get("causal_known_utc") or first_seen)
        published = parse_utc(payload.get("published_utc"))
        if known is None or published is None or known < published:
            continue
        output.append({
            "event_id": str(event_id),
            "source_id": str(payload.get("source_parent_id") or payload.get("source_id") or ""),
            "event_series_id": str(payload.get("event_series_id") or ""),
            "event_name": str(payload.get("event_name") or payload.get("headline") or ""),
            "currency": "USD",
            "known_utc": iso(known),
            "published_utc": iso(published),
            "actual_value": payload.get("actual_value"),
            "previous_value": payload.get("previous_value"),
            "consensus_value": payload.get("consensus_value"),
            "reference_period": str(payload.get("reference_period") or ""),
            "source_url": str(payload.get("source_url") or ""),
            "prospective_eligible": known >= COHORT_START,
            "research_only": True,
            "execution_eligible": False,
        })
    return output


def quote_window(
    event: Mapping[str, Any], quote_db: Path
) -> dict[str, dict[int, dict[str, float]]]:
    published = parse_utc(event.get("published_utc"))
    known = parse_utc(event.get("known_utc"))
    if published is None or known is None:
        return {}
    start = int(published.timestamp() // 60 * 60) - 10 * 60
    end = int(known.timestamp() // 60 * 60) + 75 * 60
    all_rows = load_quote_rows(quote_db, "USD", start, end)
    return {
        instrument: rows
        for instrument, rows in all_rows.items()
        if instrument in LIQUID_USD_CROSSES
    }


def evaluate_two_sided(
    event: Mapping[str, Any],
    rows: Mapping[str, Mapping[int, Mapping[str, float]]],
) -> dict[str, Any]:
    """Choose the first persistent four-of-seven USD-factor confirmation."""

    published = parse_utc(event.get("published_utc"))
    if published is None:
        return {"status": "abstain", "reason": "missing_release_clock"}
    event_minute = int(published.timestamp() // 60 * 60)
    candidates: list[dict[str, Any]] = []
    for currency_direction in ("STRENGTHEN", "WEAKEN"):
        probe = {
            **dict(event),
            "delta": {"currency_direction": currency_direction},
        }
        diagnostic_setup = evaluate_breakout_setup(
            probe,
            rows,
            # Force the shared evaluator to return its full diagnostics.  The
            # macro contract applies its own stabilization and persistence
            # rule rather than accepting the first release impulse.
            minimum_confirming_pairs=len(LIQUID_USD_CROSSES) + 1,
        )
        by_minute: dict[int, list[dict[str, Any]]] = {}
        for row in diagnostic_setup.get("diagnostics") or []:
            minute = parse_utc(row.get("confirmation_minute_utc"))
            if minute is None or not bool(row.get("confirmed")):
                continue
            epoch = int(minute.timestamp())
            if epoch < event_minute + MIN_STABILIZATION_MINUTES * 60:
                continue
            by_minute.setdefault(epoch, []).append(row)
        eligible_minutes = sorted(
            minute
            for minute, confirmations in by_minute.items()
            if len(confirmations) >= MIN_CONFIRMING_PAIRS
        )
        for minute in eligible_minutes:
            if not all(
                minute - offset * 60 in eligible_minutes
                for offset in range(MIN_PERSISTENT_CONFIRMATION_MINUTES)
            ):
                continue
            confirmations = by_minute[minute]
            selected = min(
                confirmations,
                key=lambda row: (row["entry_cost_bps"], row["instrument"]),
            )
            candidates.append({
                "status": "triggered",
                "confirmation_minute_utc": selected["confirmation_minute_utc"],
                "entry_minute_utc": selected["entry_minute_utc"],
                "confirmation_count": len(confirmations),
                "confirming_pairs": sorted(
                    row["instrument"] for row in confirmations
                ),
                "selected": selected,
                "currency_direction": currency_direction,
                "criteria": {
                    "minimum_confirming_pairs": MIN_CONFIRMING_PAIRS,
                    "minimum_stabilization_minutes": MIN_STABILIZATION_MINUTES,
                    "minimum_persistent_confirmation_minutes": MIN_PERSISTENT_CONFIRMATION_MINUTES,
                    "maximum_spread_baseline_ratio": 1.75,
                    "minimum_move_spread_multiple": 0.5,
                    "minimum_signed_imbalance_30s": 0.05,
                    "entry_timing": "minute_after_second_persistent_completed_confirmation",
                },
                "diagnostics": diagnostic_setup.get("diagnostics") or [],
                "research_only": True,
                "execution_eligible": False,
            })
            break
    if not candidates:
        return {
            "status": "no_trigger",
            "reason": "neither_currency_direction_reached_four_pair_confirmation",
            "research_only": True,
            "execution_eligible": False,
        }
    candidates.sort(
        key=lambda setup: (
            str(setup.get("confirmation_minute_utc") or ""),
            -int(setup.get("confirmation_count") or 0),
            finite((setup.get("selected") or {}).get("entry_cost_bps"), 999999.0),
        )
    )
    winner = candidates[0]
    winner["two_sided_rule"] = True
    winner["one_usd_factor_counted"] = True
    winner["causal_consensus_available"] = event.get("consensus_value") is not None
    winner["direction_source"] = (
        "price_confirmed_after_release"
        if event.get("consensus_value") is None
        else "price_confirmed_with_causal_surprise_context"
    )
    return winner


def replay_outcomes(
    setup: Mapping[str, Any],
    rows: Mapping[str, Mapping[int, Mapping[str, float]]],
) -> dict[str, Any]:
    if setup.get("status") != "triggered":
        return {}
    selected = setup.get("selected") or {}
    instrument = str(selected.get("instrument") or "")
    entry = parse_utc(selected.get("entry_minute_utc"))
    if entry is None or instrument not in rows:
        return {}
    entry_epoch = int(entry.timestamp())
    entry_row = rows[instrument].get(entry_epoch)
    if not entry_row:
        return {}
    pip = fallback_pip_size(instrument)
    sign = int(selected.get("expected_sign") or 0)
    output: dict[str, Any] = {}
    for horizon in sorted(COHORTS):
        end_row = rows[instrument].get(entry_epoch + horizon * 60)
        if not end_row:
            continue
        gross = sign * (
            finite(end_row.get("mid")) - finite(entry_row.get("mid"))
        ) / pip
        spread = 0.5 * (
            finite(entry_row.get("spread_pips"), 999999.0)
            + finite(end_row.get("spread_pips"), 999999.0)
        )
        output[f"h{horizon}"] = {
            "gross_directional_pips": round(gross, 6),
            "estimated_round_trip_spread_pips": round(spread, 6),
            "estimated_after_spread_pips": round(gross - spread, 6),
            "beat_spread": gross > spread,
        }
    return output


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS macro_release_events (
          event_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL,
          known_utc TEXT NOT NULL, published_utc TEXT NOT NULL,
          source_id TEXT NOT NULL, event_series_id TEXT NOT NULL,
          prospective_eligible INTEGER NOT NULL, payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS prospective_entries (
          entry_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL,
          event_id TEXT NOT NULL, decided_utc TEXT NOT NULL,
          entry_minute_utc TEXT NOT NULL, instrument TEXT NOT NULL,
          direction TEXT NOT NULL, horizon_min INTEGER NOT NULL,
          entry_mid REAL NOT NULL, entry_spread_pips REAL NOT NULL,
          status TEXT NOT NULL, outcome_utc TEXT,
          gross_pips REAL, estimated_after_spread_pips REAL,
          payload_json TEXT NOT NULL
        );
        """
    )
    connection.commit()
    return connection


def persist_event(connection: sqlite3.Connection, event: Mapping[str, Any]) -> int:
    connection.execute(
        "INSERT OR IGNORE INTO macro_release_events VALUES (?,?,?,?,?,?,?,?)",
        (
            event["event_id"],
            CONTRACT_ID,
            event["known_utc"],
            event["published_utc"],
            event["source_id"],
            event["event_series_id"],
            int(bool(event.get("prospective_eligible"))),
            json.dumps(event, sort_keys=True),
        ),
    )
    inserted = int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    connection.commit()
    return inserted


def persist_entries(
    connection: sqlite3.Connection,
    event: Mapping[str, Any],
    setup: Mapping[str, Any],
    observed: dt.datetime,
) -> int:
    if not bool(event.get("prospective_eligible")) or setup.get("status") != "triggered":
        return 0
    selected = setup.get("selected") or {}
    inserted = 0
    for horizon, cohort_id in COHORTS.items():
        identity = "|".join((cohort_id, str(event["event_id"]), str(selected["instrument"])))
        entry_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
        payload = {
            "event": event,
            "setup": setup,
            "cohort_id": cohort_id,
            "horizon_min": horizon,
            "research_only": True,
            "execution_eligible": False,
        }
        connection.execute(
            "INSERT OR IGNORE INTO prospective_entries VALUES "
            "(?,?,?,?,?,?,?,?,?,?,'pending',NULL,NULL,NULL,?)",
            (
                entry_id,
                cohort_id,
                event["event_id"],
                iso(observed),
                setup["entry_minute_utc"],
                selected["instrument"],
                selected["direction"],
                horizon,
                selected["entry_mid"],
                selected["entry_spread_pips"],
                json.dumps(payload, sort_keys=True),
            ),
        )
        inserted += int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    connection.commit()
    return inserted


def mature_entries(connection: sqlite3.Connection, quote_db: Path) -> int:
    pending = connection.execute(
        "SELECT entry_id,entry_minute_utc,instrument,direction,horizon_min,"
        "entry_mid,entry_spread_pips,payload_json FROM prospective_entries "
        "WHERE status='pending'"
    ).fetchall()
    if not quote_db.exists():
        return 0
    quotes = sqlite3.connect(
        f"file:{quote_db.as_posix()}?mode=ro", uri=True, timeout=10
    )
    matured = 0
    for entry_id, entry_text, instrument, direction, horizon, entry_mid, entry_spread, payload_json in pending:
        entry = parse_utc(entry_text)
        if entry is None:
            continue
        outcome_epoch = int(entry.timestamp()) + int(horizon) * 60
        outcome = quotes.execute(
            "SELECT last_mid,average_spread_pips,last_broker_time "
            "FROM quote_intensity_minutes_v1 WHERE instrument=? AND minute_epoch=?",
            (instrument, outcome_epoch),
        ).fetchone()
        if outcome is None:
            continue
        sign = 1 if str(direction) == "long" else -1
        pip = fallback_pip_size(str(instrument))
        gross = sign * (finite(outcome[0]) - finite(entry_mid)) / pip
        cost = 0.5 * (finite(entry_spread) + finite(outcome[1]))
        payload = json.loads(payload_json)
        payload["outcome"] = {
            "outcome_utc": str(outcome[2] or iso(dt.datetime.fromtimestamp(outcome_epoch, UTC))),
            "gross_directional_pips": gross,
            "estimated_after_spread_pips": gross - cost,
        }
        connection.execute(
            "UPDATE prospective_entries SET status='matured',outcome_utc=?,"
            "gross_pips=?,estimated_after_spread_pips=?,payload_json=? WHERE entry_id=?",
            (
                payload["outcome"]["outcome_utc"],
                gross,
                gross - cost,
                json.dumps(payload, sort_keys=True),
                entry_id,
            ),
        )
        matured += 1
    quotes.close()
    connection.commit()
    return matured


def discovery_case(quote_db: Path) -> dict[str, Any]:
    event = {
        "event_id": "discovery_us_ppi_20260813",
        "source_id": "bls_ppi_current_release_v1",
        "event_series_id": "WPSFD4",
        "event_name": "US Producer Price Index Final Demand",
        "currency": "USD",
        "known_utc": "2026-08-13T12:30:00+00:00",
        "published_utc": "2026-08-13T12:30:00+00:00",
        "actual_value": 0.0,
        "previous_value": -0.1,
        "consensus_value": None,
        "reference_period": "2026-07",
        "source_url": "https://www.bls.gov/news.release/ppi.nr0.htm",
        "prospective_eligible": False,
        "evidence_state": "discovery_replay_only",
        "research_only": True,
        "execution_eligible": False,
    }
    rows = quote_window(event, quote_db)
    setup = evaluate_two_sided(event, rows)
    setup["replay_outcomes"] = replay_outcomes(setup, rows)
    return {"event": event, "setup": setup}


def render_report(payload: Mapping[str, Any]) -> str:
    case = payload.get("us_ppi_discovery_replay") or {}
    setup = case.get("setup") or {}
    selected = setup.get("selected") or {}
    outcomes = setup.get("replay_outcomes") or {}
    lines = [
        "# U.S. PPI two-sided breakout case: 2026-08-13",
        "",
        f"Generated: {payload.get('generated_utc')}",
        "",
        "## What happened",
        "",
        "BLS released July PPI at 08:30 America/New_York. Headline final demand",
        "was unchanged after -0.1% in June; final demand less foods, energy, and",
        "trade services rose 0.4% after 0.1%. The initial USD response was mixed",
        "and then USD-positive for roughly two minutes. A blind short-USD entry",
        "at 08:30 would therefore have been wrong.",
        "",
        "The official product page was not being collected at release time. The",
        "existing RSS was a mutable latest-numbers container, while the BLS batch",
        "API was quota-exhausted. The new direct page adapter is prospective only:",
        "its first snapshot is explicitly noncausal bootstrap evidence.",
        "The first meaningful retained secondary PPI story was not seen until",
        "08:39:37, after the robust confirmation. The repaired official adapter",
        "polls the product page every 30 seconds from 08:28 through 08:40 ET.",
        "",
        "Secondary reporting later described headline 0.0% versus 0.2% expected",
        "and ex-food-and-energy 0.2% versus 0.3%, but those expectations were not",
        "captured locally before 08:30 and therefore cannot be backfilled as causal.",
        "The official less-foods-energy-and-trade-services series was 0.4% versus",
        "0.1% prior, showing why the numeric channels must remain separate.",
        "",
        "## Price sequence",
        "",
        "- 08:30: no four-pair side passed the full cost/imbalance rule.",
        "- 08:31: four crosses expressed USD strength; 08:32 had five. A naive",
        "  symmetric one-minute breakout would have entered this false first leg.",
        "- 08:33-08:36: neither direction persisted broadly enough.",
        "- 08:37 and 08:38: four crosses persistently expressed USD weakness.",
        "- 08:39: next-minute research entry, short USD/CAD.",
        "",
        "## Frozen discovery rule",
        "",
        "- Watch both USD directions when causal pre-release consensus is absent.",
        "- Ignore the first three completed minutes, then require four of seven",
        "  liquid USD crosses to break the ten-minute pre-event range in one",
        "  signed USD direction for two consecutive completed minutes.",
        "- Each confirmation must move at least half its current spread, have",
        "  aligned 30-second quote imbalance >= 0.05, and spread <= 1.75x its",
        "  pre-event median.",
        "- Count the seven crosses as one USD factor, then enter the lowest-cost",
        "  confirming pair on the next minute.",
        "",
        f"First confirmation: {setup.get('confirmation_minute_utc')}",
        f"Replay entry: {setup.get('entry_minute_utc')}",
        f"Confirming pairs: {', '.join(setup.get('confirming_pairs') or [])}",
        f"Selected: {selected.get('instrument')} {selected.get('direction')}",
        "",
        "| Horizon | Gross pips | Estimated spread | Estimated after spread |",
        "|---|---:|---:|---:|",
    ]
    for label in ("h5", "h15", "h30", "h60"):
        row = outcomes.get(label) or {}
        lines.append(
            f"| {label.upper()} | {finite(row.get('gross_directional_pips')):.2f} | "
            f"{finite(row.get('estimated_round_trip_spread_pips')):.2f} | "
            f"{finite(row.get('estimated_after_spread_pips')):.2f} |"
        )
    lines.extend([
        "",
        "## Interpretation and governance",
        "",
        "This is a post-case discovery result, not proof. Exact bid/ask fills were",
        "not available for the historical replay, so costs use retained minute",
        "spread observations. The rule begins a new immutable prospective cohort",
        f"at {iso(COHORT_START)}. It cannot place or authorize an order.",
        "",
        "Causal pre-release consensus and contemporaneous rate-market repricing",
        "remain missing. Until those exist, the official release opens a two-sided",
        "watch; price confirmation supplies timing and observed direction.",
        "",
    ])
    return "\n".join(lines)


def run_once(
    *,
    news_db: Path = NEWS_DB,
    quote_db: Path = QUOTE_DB,
    database_path: Path = DATABASE,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
    observed: dt.datetime | None = None,
    heartbeat: WorkerHeartbeat | None = None,
) -> dict[str, Any]:
    if observed is None:
        observed, clock = normalized_observation_time(dt.datetime.now(UTC))
    else:
        observed = observed.astimezone(UTC)
        clock = {"source": "provided", "trusted_for_prospective_evidence": True}
    events = load_structured_macro_events(news_db, heartbeat=heartbeat)
    if heartbeat is not None:
        heartbeat.mark_progress(
            phase="evaluating_events", events_loaded=len(events), events_processed=0
        )
    connection = open_database(database_path)
    inserted_events = 0
    inserted_entries = 0
    setups: list[dict[str, Any]] = []
    for event_number, event in enumerate(events, 1):
        if heartbeat is not None:
            heartbeat.mark_progress(
                phase="evaluating_events",
                events_loaded=len(events),
                events_processed=event_number,
            )
        inserted_events += persist_event(connection, event)
        known = parse_utc(event.get("known_utc"))
        if not bool(event.get("prospective_eligible")) or known is None:
            continue
        if observed > known + dt.timedelta(minutes=90):
            continue
        setup = evaluate_two_sided(event, quote_window(event, quote_db))
        setups.append({"event_id": event["event_id"], "setup": setup})
        inserted_entries += persist_entries(connection, event, setup, observed)
    if heartbeat is not None:
        heartbeat.update(phase="maturing_entries")
    matured = mature_entries(connection, quote_db)
    status_counts = {
        str(status): int(count)
        for status, count in connection.execute(
            "SELECT status,COUNT(*) FROM prospective_entries GROUP BY status"
        ).fetchall()
    }
    connection.close()
    payload = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "generated_utc": iso(observed),
        "observation_clock": clock,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "can_authorize": False,
        "cohort_start_utc": iso(COHORT_START),
        "cohorts": COHORTS,
        "eligible_universe": sorted(LIQUID_USD_CROSSES),
        "minimum_confirming_pairs": MIN_CONFIRMING_PAIRS,
        "minimum_stabilization_minutes": MIN_STABILIZATION_MINUTES,
        "minimum_persistent_confirmation_minutes": MIN_PERSISTENT_CONFIRMATION_MINUTES,
        "prospective_events_loaded": len(events),
        "inserted_events": inserted_events,
        "inserted_entries": inserted_entries,
        "matured_entries": matured,
        "entry_status_counts": status_counts,
        "prospective_setups": setups,
        "us_ppi_discovery_replay": discovery_case(quote_db),
        "policy": {
            "one_usd_factor_per_event": True,
            "two_sided_without_causal_consensus": True,
            "entry_after_completed_confirmation_minute": True,
            "replay_case_excluded_from_prospective_evidence": True,
        },
    }
    atomic_json(output_path, payload)
    atomic_text(report_path, render_report(payload))
    if heartbeat is not None:
        heartbeat.mark_progress(
            phase="cycle_complete",
            events_loaded=len(events),
            inserted_entries=inserted_entries,
            matured_entries=matured,
        )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT)
    args = parser.parse_args()
    started = time.monotonic()
    with WorkerHeartbeat(
        args.heartbeat,
        worker="macro_release_breakout_research",
        role="research_only",
        interval_sec=5.0,
    ) as heartbeat:
        while True:
            heartbeat.update(phase="starting_cycle")
            run_once(heartbeat=heartbeat)
            if args.once or args.duration_sec <= 0:
                return 0
            if time.monotonic() - started >= args.duration_sec:
                return 0
            heartbeat.update(phase="sleeping")
            time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
