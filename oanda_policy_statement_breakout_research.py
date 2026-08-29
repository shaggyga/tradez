#!/usr/bin/env python3
"""Prospective, research-only policy-statement delta and breakout evidence.

The statement side comes from a deterministic comparison with the prior
first-party statement.  Entry timing is deliberately separate: at least two
same-currency pairs must break their pre-event range in the implied direction
after the statement is known, short-term quote imbalance must agree, and the
spread must normalize relative to its own pre-event baseline.

This worker cannot trade, promote, authorize, or alter Practice 007 policy.
The August 2026 Norges case is a discovery replay and is permanently excluded
from the prospective cohort that begins after this implementation.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import re
import sqlite3
import statistics
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from oanda_instrument_pips import fallback_pip_size
from oanda_local_news_sentiment import normalized_observation_time


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
NEWS_DB = DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
QUOTE_DB = STATE / "practice_007_quote_intensity_shadow_v1.sqlite"
BASELINES = ROOT / "config" / "official_policy_statement_baselines_v2_20260816.json"
NEWS_SOURCES = ROOT / "config" / "news_sources_v1.json"
DATABASE = STATE / "policy_statement_breakout_research_v1.sqlite"
OUTPUT = STATE / "policy_statement_breakout_research_v1.json"
REPORT = (
    DATA
    / "reports"
    / "major_move_case_audits"
    / "NORGES_POLICY_STATEMENT_BREAKOUT_20260813.md"
)

UTC = dt.timezone.utc
SUPERSEDES_CONTRACT_ID = "official_policy_statement_delta_breakout_v2_20260816"
CONTRACT_ID = "official_policy_statement_delta_breakout_v3_20260816"
COHORT_START = dt.datetime(2026, 8, 16, 6, 30, tzinfo=UTC)
COHORTS = {
    5: "official_policy_delta_breakout_h5_v3_20260816",
    15: "official_policy_delta_breakout_h15_v3_20260816",
    30: "official_policy_delta_breakout_h30_v3_20260816",
}
DELTA_THRESHOLD = 0.35
PRE_EVENT_MINUTES = 10
MAX_CONFIRMATION_MINUTES = 10
MAX_SPREAD_BASELINE_RATIO = 1.75
MIN_MOVE_SPREAD_MULTIPLE = 0.50
MIN_IMBALANCE_30S = 0.05
MIN_CONFIRMING_PAIRS = 2


def parse_utc(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


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


def current_policy_excerpt(text: Any) -> str:
    """Return current guidance without quoted retrospective meeting language."""

    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    retrospective = re.search(
        r"\bAt the monetary policy meeting in [A-Z][a-z]+\b",
        normalized,
        flags=re.I,
    )
    if retrospective and retrospective.start() >= 50:
        normalized = normalized[: retrospective.start()]
    return normalized[:4_000]


def _matched_score(
    text: str, patterns: Sequence[tuple[str, float, str]]
) -> tuple[float, list[str]]:
    matches: list[tuple[float, str]] = []
    for pattern, score, label in patterns:
        if re.search(pattern, text, flags=re.I):
            matches.append((score, label))
    if not matches:
        return 0.0, []
    strongest = max(matches, key=lambda row: abs(row[0]))[0]
    return strongest, sorted({label for _, label in matches})


def policy_stance_snapshot(text: Any) -> dict[str, Any]:
    excerpt = current_policy_excerpt(text)
    guidance, guidance_evidence = _matched_score(
        excerpt,
        (
            (
                r"\bmay (?:thus )?still (?:become )?necessary to raise\b",
                0.35,
                "conditional_hike_may_still_be_needed",
            ),
            (
                r"\bwill likely be necessary to raise\b|"
                r"\blikely be necessary to raise\b",
                0.85,
                "hike_likely_needed",
            ),
            (
                r"\b(?:policy )?rate will be raised\b|\bwill raise the (?:policy )?rate\b",
                1.0,
                "hike_committed",
            ),
            (
                r"\bmay (?:thus )?still (?:become )?necessary to (?:cut|lower|reduce)\b",
                -0.35,
                "conditional_cut_may_still_be_needed",
            ),
            (
                r"\bwill likely be necessary to (?:cut|lower|reduce)\b|"
                r"\blikely be necessary to (?:cut|lower|reduce)\b",
                -0.85,
                "cut_likely_needed",
            ),
            (
                r"\b(?:policy )?rate will be (?:cut|lowered|reduced)\b|"
                r"\bwill (?:cut|lower|reduce) the (?:policy )?rate\b",
                -1.0,
                "cut_committed",
            ),
        ),
    )
    inflation, inflation_evidence = _matched_score(
        excerpt,
        (
            (
                r"\binflation (?:has )?(?:slowed and been |been )?lower than "
                r"(?:projected|expected|anticipated)\b",
                -0.65,
                "inflation_lower_than_projected",
            ),
            (
                r"\binflation pressures? (?:are |is )?(?:slightly )?"
                r"(?:stronger|higher) than (?:projected|expected|anticipated)\b",
                0.65,
                "inflation_pressure_stronger_than_projected",
            ),
        ),
    )
    activity, activity_evidence = _matched_score(
        excerpt,
        (
            (
                r"\bcapacity utili[sz]ation\b.{0,80}\bdrifting down\b|"
                r"\beconomy (?:is )?(?:cooling|weakening)\b",
                -0.20,
                "activity_drifting_down",
            ),
            (
                r"\bcapacity utili[sz]ation\b.{0,80}\b(?:rising|increasing)\b|"
                r"\beconomy (?:is )?(?:strengthening|overheating)\b",
                0.20,
                "activity_strengthening",
            ),
        ),
    )
    composite = clamp(0.55 * guidance + 0.30 * inflation + 0.15 * activity)
    return {
        "contract_id": CONTRACT_ID,
        "guidance_score": round(guidance, 6),
        "inflation_score": round(inflation, 6),
        "activity_score": round(activity, 6),
        "composite_score": round(composite, 6),
        "evidence": sorted(
            set(guidance_evidence + inflation_evidence + activity_evidence)
        ),
        "current_guidance_excerpt": excerpt,
        "research_only": True,
    }


def policy_stance_delta(current: Mapping[str, Any], prior: Mapping[str, Any]) -> dict[str, Any]:
    delta = finite(current.get("composite_score")) - finite(
        prior.get("composite_score")
    )
    direction = (
        "HAWKISH"
        if delta >= DELTA_THRESHOLD
        else "DOVISH"
        if delta <= -DELTA_THRESHOLD
        else "NEUTRAL"
    )
    return {
        "delta": round(delta, 6),
        "direction": direction,
        "threshold": DELTA_THRESHOLD,
        "currency_direction": (
            "STRENGTHEN" if direction == "HAWKISH" else "WEAKEN" if direction == "DOVISH" else "ABSTAIN"
        ),
        "research_only": True,
        "execution_eligible": False,
    }


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def policy_document_class(headline: Any, url: Any = "") -> str:
    """Keep decision statements, minutes, and speeches in separate chains."""

    text = f"{headline or ''} {url or ''}".lower()
    if "summary of opinions" in text:
        return "summary_of_opinions"
    if "minute" in text or "/accounts/" in text:
        return "minutes_or_account"
    if "monetary policy report" in text or "mpr_" in text:
        return "monetary_policy_report"
    if "speech" in text or "press conference" in text or "with q&a" in text:
        return "press_conference_or_speech"
    if any(
        token in text
        for token in (
            "statement",
            "decision",
            "bank rate",
            "policy rate",
            "press release",
            "/media-releases/",
        )
    ):
        return "decision_statement"
    return "other_policy_document"


def configured_currency_contracts(config_path: Path = NEWS_SOURCES) -> dict[str, set[str]]:
    return {
        str(row.get("source_id") or ""): {
            str(currency).upper() for currency in row.get("currencies") or []
        }
        for row in read_json(config_path).get("sources") or []
        if isinstance(row, Mapping) and row.get("source_id")
    }


def source_native_policy_currencies(
    source_id: str,
    payload_currencies: Sequence[Any],
    contracts: Mapping[str, set[str]],
) -> list[str]:
    """Constrain a first-party policy document to its provider currency scope."""
    observed = {str(currency).upper() for currency in payload_currencies if currency}
    configured = set(contracts.get(source_id) or set())
    if not configured:
        return sorted(observed)
    if len(configured) == 1:
        return sorted(configured)
    return sorted(configured.intersection(observed))


def load_policy_documents(
    news_db: Path,
    baselines_path: Path,
    source_config_path: Path = NEWS_SOURCES,
) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    currency_contracts = configured_currency_contracts(source_config_path)
    for row in read_json(baselines_path).get("baselines") or []:
        if isinstance(row, Mapping):
            documents.append(
                {
                    **dict(row),
                    "document_class": str(
                        row.get("document_class")
                        or policy_document_class(
                            row.get("headline"), row.get("source_url")
                        )
                    ),
                    "baseline": True,
                }
            )
    baseline_identities = {
        (
            str(row.get("source_id") or ""),
            str(row.get("currency") or ""),
            str(row.get("source_url") or ""),
        )
        for row in documents
        if bool(row.get("baseline"))
    }
    if news_db.exists():
        connection = sqlite3.connect(
            f"file:{news_db.as_posix()}?mode=ro", uri=True, timeout=10
        )
        rows = connection.execute(
            "SELECT event_id,payload_json,first_seen_utc FROM articles ORDER BY published_utc,event_id"
        ).fetchall()
        connection.close()
        for event_id, payload_json, first_seen in rows:
            try:
                payload = json.loads(payload_json or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if not bool(payload.get("source_verified")) or not bool(
                payload.get("official_policy_release")
            ):
                continue
            source_id = str(payload.get("source_id") or "")
            known = str(payload.get("causal_known_utc") or first_seen or "")
            currencies = source_native_policy_currencies(
                source_id,
                payload.get("direct_currencies") or payload.get("currencies") or [],
                currency_contracts,
            )
            for currency in currencies:
                if (
                    source_id,
                    str(currency).upper(),
                    str(payload.get("source_url") or ""),
                ) in baseline_identities:
                    continue
                documents.append(
                    {
                        "event_id": str(event_id),
                        "source_id": source_id,
                        "currency": str(currency).upper(),
                        "known_utc": known,
                        "published_utc": str(payload.get("published_utc") or ""),
                        "headline": str(payload.get("headline") or ""),
                        "summary": str(payload.get("summary") or ""),
                        "source_url": str(payload.get("source_url") or ""),
                        "document_class": policy_document_class(
                            payload.get("headline"), payload.get("source_url")
                        ),
                        "source_listing_bootstrap": bool(
                            payload.get("source_listing_bootstrap")
                        ),
                        "baseline": False,
                    }
                )
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for document in sorted(
        documents,
        key=lambda row: (
            str(row.get("known_utc") or ""),
            str(row.get("source_id") or ""),
            str(row.get("currency") or ""),
        ),
    ):
        key = (
            str(document.get("source_id") or ""),
            str(document.get("currency") or ""),
            str(document.get("published_utc") or ""),
            str(document.get("headline") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        item = dict(document)
        item["stance"] = policy_stance_snapshot(item.get("summary"))
        result.append(item)
    return result


def build_policy_delta_events(documents: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    previous: dict[tuple[str, str], dict[str, Any]] = {}
    events: list[dict[str, Any]] = []
    for raw in documents:
        document = dict(raw)
        key = (
            str(document.get("source_id") or ""),
            str(document.get("currency") or ""),
            str(document.get("document_class") or "other_policy_document"),
        )
        prior = previous.get(key)
        previous[key] = document
        if prior is None or bool(document.get("baseline")):
            continue
        delta = policy_stance_delta(document["stance"], prior["stance"])
        known = parse_utc(document.get("known_utc"))
        published = parse_utc(document.get("published_utc"))
        if known is None or published is None:
            continue
        identity = "|".join(
            (
                CONTRACT_ID,
                key[0],
                key[1],
                key[2],
                str(document.get("event_id") or document.get("source_url") or known),
            )
        )
        event_id = "policy_delta_" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
        events.append(
            {
                "event_id": event_id,
                "source_id": key[0],
                "currency": key[1],
                "document_class": key[2],
                "known_utc": iso(known),
                "published_utc": iso(published),
                "headline": str(document.get("headline") or ""),
                "source_url": str(document.get("source_url") or ""),
                "current_stance": document["stance"],
                "prior_headline": str(prior.get("headline") or ""),
                "prior_known_utc": str(prior.get("known_utc") or ""),
                "prior_source_url": str(prior.get("source_url") or ""),
                "prior_stance": prior["stance"],
                "delta": delta,
                "prospective_eligible": bool(
                    known >= COHORT_START
                    and published >= COHORT_START
                    and not bool(document.get("source_listing_bootstrap"))
                ),
                "evidence_state": (
                    "prospective_collecting"
                    if known >= COHORT_START
                    and published >= COHORT_START
                    and not bool(document.get("source_listing_bootstrap"))
                    else "discovery_replay_only"
                ),
                "research_only": True,
                "execution_eligible": False,
            }
        )
    return events


def load_quote_rows(
    database: Path,
    currency: str,
    start_epoch: int,
    end_epoch: int,
) -> dict[str, dict[int, dict[str, float]]]:
    if not database.exists():
        return {}
    connection = sqlite3.connect(
        f"file:{database.as_posix()}?mode=ro", uri=True, timeout=10
    )
    rows = connection.execute(
        """
        SELECT minute_epoch,instrument,last_mid,average_spread_pips,
               imbalance_30s,imbalance_120s
        FROM quote_intensity_minutes_v1
        WHERE minute_epoch BETWEEN ? AND ?
          AND (instrument LIKE ? OR instrument LIKE ?)
        ORDER BY instrument,minute_epoch
        """,
        (start_epoch, end_epoch, f"{currency}_%", f"%_{currency}"),
    ).fetchall()
    connection.close()
    result: dict[str, dict[int, dict[str, float]]] = {}
    for epoch, instrument, mid, spread, imbalance30, imbalance120 in rows:
        result.setdefault(str(instrument), {})[int(epoch)] = {
            "mid": finite(mid),
            "spread_pips": finite(spread, 999999.0),
            "imbalance_30s": finite(imbalance30),
            "imbalance_120s": finite(imbalance120),
        }
    return result


def pair_expected_sign(instrument: str, currency: str, currency_direction: str) -> int:
    base, quote = instrument.split("_", 1)
    currency_sign = 1 if currency_direction == "STRENGTHEN" else -1
    if base == currency:
        return currency_sign
    if quote == currency:
        return -currency_sign
    return 0


def evaluate_breakout_setup(
    event: Mapping[str, Any],
    quote_rows: Mapping[str, Mapping[int, Mapping[str, float]]],
    *,
    minimum_confirming_pairs: int = MIN_CONFIRMING_PAIRS,
) -> dict[str, Any]:
    known = parse_utc(event.get("known_utc"))
    published = parse_utc(event.get("published_utc"))
    currency_direction = str((event.get("delta") or {}).get("currency_direction") or "ABSTAIN")
    if known is None or published is None or currency_direction == "ABSTAIN":
        return {"status": "abstain", "reason": "missing_direction_or_clock"}
    event_minute = int(published.timestamp() // 60 * 60)
    known_minute = int(known.timestamp() // 60 * 60)
    candidates_by_minute: dict[int, list[dict[str, Any]]] = {}
    diagnostics: list[dict[str, Any]] = []
    for instrument, rows in quote_rows.items():
        expected_sign = pair_expected_sign(instrument, str(event.get("currency") or ""), currency_direction)
        if not expected_sign:
            continue
        pre_rows = [
            rows.get(epoch)
            for epoch in range(
                event_minute - PRE_EVENT_MINUTES * 60, event_minute, 60
            )
            if rows.get(epoch)
        ]
        known_row = rows.get(known_minute)
        if len(pre_rows) < max(5, PRE_EVENT_MINUTES // 2) or not known_row:
            continue
        pre_mids = [finite(row.get("mid")) for row in pre_rows]
        pre_spreads = [finite(row.get("spread_pips"), 999999.0) for row in pre_rows]
        baseline_spread = statistics.median(pre_spreads)
        pre_boundary = max(pre_mids) if expected_sign > 0 else min(pre_mids)
        pip = fallback_pip_size(instrument)
        for minute in range(known_minute, known_minute + (MAX_CONFIRMATION_MINUTES + 1) * 60, 60):
            row = rows.get(minute)
            entry_row = rows.get(minute + 60)
            if not row or not entry_row:
                continue
            spread = finite(row.get("spread_pips"), 999999.0)
            spread_ratio = spread / max(baseline_spread, 1e-9)
            breakout_pips = expected_sign * (finite(row.get("mid")) - pre_boundary) / pip
            move_from_known_pips = expected_sign * (
                finite(row.get("mid")) - finite(known_row.get("mid"))
            ) / pip
            imbalance = expected_sign * finite(row.get("imbalance_30s"))
            confirmed = bool(
                spread_ratio <= MAX_SPREAD_BASELINE_RATIO
                and breakout_pips > 0
                and move_from_known_pips >= MIN_MOVE_SPREAD_MULTIPLE * spread
                and imbalance >= MIN_IMBALANCE_30S
            )
            record = {
                "instrument": instrument,
                "confirmation_minute_utc": iso(dt.datetime.fromtimestamp(minute, UTC)),
                "entry_minute_utc": iso(dt.datetime.fromtimestamp(minute + 60, UTC)),
                "direction": "long" if expected_sign > 0 else "short",
                "expected_sign": expected_sign,
                "baseline_spread_pips": round(baseline_spread, 6),
                "confirmation_spread_pips": round(spread, 6),
                "spread_baseline_ratio": round(spread_ratio, 6),
                "breakout_pips": round(breakout_pips, 6),
                "move_from_known_pips": round(move_from_known_pips, 6),
                "signed_imbalance_30s": round(imbalance, 6),
                "confirmed": confirmed,
                "entry_mid": finite(entry_row.get("mid")),
                "entry_spread_pips": finite(entry_row.get("spread_pips"), 999999.0),
                "entry_cost_bps": round(
                    finite(entry_row.get("spread_pips"), 999999.0)
                    * pip
                    / max(finite(entry_row.get("mid")), 1e-9)
                    * 10_000,
                    6,
                ),
            }
            diagnostics.append(record)
            if confirmed:
                candidates_by_minute.setdefault(minute, []).append(record)
    for minute in sorted(candidates_by_minute):
        candidates = candidates_by_minute[minute]
        if len(candidates) < minimum_confirming_pairs:
            continue
        selected = min(candidates, key=lambda row: (row["entry_cost_bps"], row["instrument"]))
        return {
            "status": "triggered",
            "confirmation_minute_utc": selected["confirmation_minute_utc"],
            "entry_minute_utc": selected["entry_minute_utc"],
            "confirmation_count": len(candidates),
            "confirming_pairs": sorted(row["instrument"] for row in candidates),
            "selected": selected,
            "criteria": {
                "delta_threshold": DELTA_THRESHOLD,
                "maximum_spread_baseline_ratio": MAX_SPREAD_BASELINE_RATIO,
                "minimum_move_spread_multiple": MIN_MOVE_SPREAD_MULTIPLE,
                "minimum_signed_imbalance_30s": MIN_IMBALANCE_30S,
                "minimum_confirming_pairs": minimum_confirming_pairs,
                "entry_timing": "first_minute_after_completed_confirmation_minute",
            },
            "diagnostics": diagnostics,
            "research_only": True,
            "execution_eligible": False,
        }
    return {
        "status": "no_trigger",
        "reason": "confirmation_requirements_not_met",
        "diagnostics": diagnostics,
        "research_only": True,
        "execution_eligible": False,
    }


def add_replay_outcomes(
    setup: dict[str, Any],
    quote_rows: Mapping[str, Mapping[int, Mapping[str, float]]],
) -> dict[str, Any]:
    if setup.get("status") != "triggered":
        return setup
    selected = setup["selected"]
    instrument = str(selected["instrument"])
    entry = parse_utc(selected["entry_minute_utc"])
    if entry is None:
        return setup
    rows = quote_rows.get(instrument) or {}
    entry_epoch = int(entry.timestamp())
    entry_row = rows.get(entry_epoch)
    if not entry_row:
        return setup
    pip = fallback_pip_size(instrument)
    sign = int(selected["expected_sign"])
    outcomes: dict[str, Any] = {}
    for horizon in sorted(COHORTS):
        end_row = rows.get(entry_epoch + horizon * 60)
        if not end_row:
            continue
        gross = sign * (
            finite(end_row.get("mid")) - finite(entry_row.get("mid"))
        ) / pip
        cost = 0.5 * (
            finite(entry_row.get("spread_pips"), 999999.0)
            + finite(end_row.get("spread_pips"), 999999.0)
        )
        outcomes[f"h{horizon}"] = {
            "gross_directional_pips": round(gross, 6),
            "estimated_round_trip_spread_pips": round(cost, 6),
            "estimated_after_spread_pips": round(gross - cost, 6),
            "beat_spread": gross > cost,
        }
    return {**setup, "replay_outcomes": outcomes}


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS policy_delta_events (
          event_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL,
          known_utc TEXT NOT NULL, published_utc TEXT NOT NULL,
          source_id TEXT NOT NULL, currency TEXT NOT NULL,
          delta REAL NOT NULL, direction TEXT NOT NULL,
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
        CREATE INDEX IF NOT EXISTS ix_policy_delta_entry_status
          ON prospective_entries(status,entry_minute_utc,horizon_min);
        """
    )
    connection.commit()
    return connection


def persist_events(connection: sqlite3.Connection, events: Sequence[Mapping[str, Any]]) -> int:
    inserted = 0
    for event in events:
        connection.execute(
            "INSERT OR IGNORE INTO policy_delta_events VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                event["event_id"],
                CONTRACT_ID,
                event["known_utc"],
                event["published_utc"],
                event["source_id"],
                event["currency"],
                finite((event.get("delta") or {}).get("delta")),
                str((event.get("delta") or {}).get("direction") or "NEUTRAL"),
                int(bool(event.get("prospective_eligible"))),
                json.dumps(event, sort_keys=True),
            ),
        )
        inserted += int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    connection.commit()
    return inserted


def persist_prospective_entries(
    connection: sqlite3.Connection,
    event: Mapping[str, Any],
    setup: Mapping[str, Any],
    observed: dt.datetime,
) -> int:
    if not bool(event.get("prospective_eligible")) or setup.get("status") != "triggered":
        return 0
    selected = setup["selected"]
    inserted = 0
    for horizon, cohort_id in COHORTS.items():
        identity = "|".join(
            (
                cohort_id,
                str(event["event_id"]),
                str(selected["instrument"]),
                str(selected["direction"]),
            )
        )
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
            """
            INSERT OR IGNORE INTO prospective_entries VALUES
            (?,?,?,?,?,?,?,?,?,?,'pending',NULL,NULL,NULL,?)
            """,
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
    rows = connection.execute(
        """
        SELECT entry_id,entry_minute_utc,instrument,direction,horizon_min,
               entry_mid,entry_spread_pips,payload_json
        FROM prospective_entries WHERE status='pending'
        """
    ).fetchall()
    matured = 0
    if not quote_db.exists():
        return matured
    quotes = sqlite3.connect(
        f"file:{quote_db.as_posix()}?mode=ro", uri=True, timeout=10
    )
    for entry_id, entry_text, instrument, direction, horizon, entry_mid, entry_spread, payload_json in rows:
        entry = parse_utc(entry_text)
        if entry is None:
            continue
        outcome_epoch = int(entry.timestamp()) + int(horizon) * 60
        outcome = quotes.execute(
            "SELECT last_mid,average_spread_pips,last_broker_time FROM quote_intensity_minutes_v1 WHERE instrument=? AND minute_epoch=?",
            (instrument, outcome_epoch),
        ).fetchone()
        if outcome is None:
            continue
        pip = fallback_pip_size(str(instrument))
        sign = 1 if str(direction) == "long" else -1
        gross = sign * (finite(outcome[0]) - finite(entry_mid)) / pip
        cost = 0.5 * (finite(entry_spread) + finite(outcome[1]))
        payload = json.loads(payload_json)
        payload["outcome"] = {
            "outcome_utc": str(outcome[2] or iso(dt.datetime.fromtimestamp(outcome_epoch, UTC))),
            "gross_directional_pips": gross,
            "estimated_after_spread_pips": gross - cost,
        }
        connection.execute(
            """
            UPDATE prospective_entries SET status='matured',outcome_utc=?,
              gross_pips=?,estimated_after_spread_pips=?,payload_json=?
            WHERE entry_id=?
            """,
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


def event_quote_window(event: Mapping[str, Any], quote_db: Path) -> dict[str, dict[int, dict[str, float]]]:
    published = parse_utc(event.get("published_utc"))
    known = parse_utc(event.get("known_utc"))
    if published is None or known is None:
        return {}
    start = int(published.timestamp() // 60 * 60) - PRE_EVENT_MINUTES * 60
    end = int(known.timestamp() // 60 * 60) + 65 * 60
    return load_quote_rows(quote_db, str(event.get("currency") or ""), start, end)


def render_report(payload: Mapping[str, Any]) -> str:
    case = payload.get("norges_discovery_replay") or {}
    event = case.get("event") or {}
    delta = event.get("delta") or {}
    setup = case.get("setup") or {}
    selected = setup.get("selected") or {}
    outcomes = setup.get("replay_outcomes") or {}
    lines = [
        "# Norges policy-statement breakout case: 2026-08-13",
        "",
        f"Generated: {payload.get('generated_utc')}",
        "",
        "## Decision",
        "",
        "The official source transport was timely enough. The statement was first",
        "known locally at 04:02:04 America/New_York. A blind entry at that instant",
        "would not have been defensible because spreads were still abnormal and the",
        "first two minutes whipsawed. The first rule-complete confirmation minute",
        "was 04:03, yielding a replay entry at 04:04 after that minute closed.",
        "",
        "This case is discovery-only and cannot count as prospective evidence.",
        "",
        "## Statement delta",
        "",
        f"- Prior composite stance: {finite((event.get('prior_stance') or {}).get('composite_score')):.4f}",
        f"- Current composite stance: {finite((event.get('current_stance') or {}).get('composite_score')):.4f}",
        f"- Delta: {finite(delta.get('delta')):.4f} ({delta.get('direction')}, NOK {delta.get('currency_direction')})",
        "- June guidance said a hike would likely be necessary; August downgraded",
        "  that to a hike that may still become necessary and reported inflation",
        "  below projection. The relative change is dovish even though both documents",
        "  contain hawkish vocabulary.",
        "",
        "## Frozen setup",
        "",
        f"- Status: {setup.get('status')}",
        f"- Confirmation: {setup.get('confirmation_minute_utc')}",
        f"- Entry minute: {setup.get('entry_minute_utc')}",
        f"- Confirming pairs: {', '.join(setup.get('confirming_pairs') or [])}",
        f"- Selected pair: {selected.get('instrument')} {selected.get('direction')}",
        f"- Entry spread: {finite(selected.get('entry_spread_pips')):.2f} pips ({finite(selected.get('entry_cost_bps')):.2f} bp)",
        "",
        "The rule requires a statement delta beyond 0.35, two same-currency pair",
        "breakouts, expected-direction movement of at least half the current spread,",
        "signed 30-second imbalance of at least 0.05, and spread no more than 1.75x",
        "its own ten-minute pre-event median. One signed NOK factor is counted.",
        "",
        "## Discovery replay outcomes",
        "",
        "| Horizon | Gross directional | Est. spread | Est. after spread |",
        "|---|---:|---:|---:|",
    ]
    for label in ("h5", "h15", "h30"):
        row = outcomes.get(label) or {}
        lines.append(
            f"| {label.upper()} | {finite(row.get('gross_directional_pips')):.1f} | "
            f"{finite(row.get('estimated_round_trip_spread_pips')):.1f} | "
            f"{finite(row.get('estimated_after_spread_pips')):.1f} |"
        )
    lines.extend(
        [
            "",
            "## Governance",
            "",
            f"- Prospective cohort start: {iso(COHORT_START)}",
            "- The Norges replay is excluded from those cohorts.",
            "- No practice or real-money order path exists.",
            "- Missing prior first-party context, a neutral statement delta, fewer",
            "  than two pair confirmations, or abnormal spread causes abstention.",
            "",
        ]
    )
    return "\n".join(lines)


def run_once(
    *,
    news_db: Path = NEWS_DB,
    quote_db: Path = QUOTE_DB,
    baselines_path: Path = BASELINES,
    database_path: Path = DATABASE,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
    observed: dt.datetime | None = None,
) -> dict[str, Any]:
    if observed is None:
        observed, clock = normalized_observation_time(dt.datetime.now(UTC))
    else:
        observed = observed.astimezone(UTC)
        clock = {"source": "provided", "trusted_for_prospective_evidence": True}
    documents = load_policy_documents(news_db, baselines_path)
    events = build_policy_delta_events(documents)
    connection = open_database(database_path)
    inserted_events = persist_events(connection, events)
    inserted_entries = 0
    prospective_setups: list[dict[str, Any]] = []
    for event in events:
        if not bool(event.get("prospective_eligible")):
            continue
        known = parse_utc(event.get("known_utc"))
        if known is None or observed > known + dt.timedelta(minutes=90):
            continue
        setup = evaluate_breakout_setup(event, event_quote_window(event, quote_db))
        prospective_setups.append({"event_id": event["event_id"], "setup": setup})
        inserted_entries += persist_prospective_entries(
            connection, event, setup, observed
        )
    matured_entries = mature_entries(connection, quote_db)
    status_counts = {
        str(status): int(count)
        for status, count in connection.execute(
            "SELECT status,COUNT(*) FROM prospective_entries GROUP BY status"
        ).fetchall()
    }
    connection.close()

    norges_case: dict[str, Any] = {}
    for event in events:
        if (
            event.get("source_id") == "norges_press"
            and str(event.get("published_utc") or "").startswith("2026-08-13T08:00:00")
        ):
            setup = evaluate_breakout_setup(event, event_quote_window(event, quote_db))
            setup = add_replay_outcomes(setup, event_quote_window(event, quote_db))
            norges_case = {"event": event, "setup": setup}
            break
    payload = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "supersedes_contract_id": SUPERSEDES_CONTRACT_ID,
        "generated_utc": iso(observed),
        "observation_clock": clock,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "can_authorize": False,
        "cohort_start_utc": iso(COHORT_START),
        "cohorts": COHORTS,
        "documents_loaded": len(documents),
        "policy_delta_event_count": len(events),
        "inserted_events": inserted_events,
        "inserted_prospective_entries": inserted_entries,
        "matured_prospective_entries": matured_entries,
        "prospective_entry_status_counts": status_counts,
        "prospective_setups": prospective_setups,
        "norges_discovery_replay": norges_case,
        "policy": {
            "one_currency_factor_per_policy_event": True,
            "statement_direction_and_entry_timing_are_separate": True,
            "requires_prior_first_party_context": True,
            "official_documents_use_source_native_currency_contract": True,
            "requires_two_pair_breakout_confirmation": True,
            "requires_spread_normalization": True,
            "replay_case_excluded_from_prospective_evidence": True,
        },
    }
    atomic_json(output_path, payload)
    atomic_text(report_path, render_report(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        run_once()
        if args.once or args.duration_sec <= 0:
            return 0
        if time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
