#!/usr/bin/env python3
"""Read-only monitor joining practice -007 forecasts with local news scores."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import re
import sqlite3
import time
from collections import Counter
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo


UTC = dt.timezone.utc
ROOT = Path(__file__).resolve().parent
STATE_ROOT = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_SIGNAL = STATE_ROOT / "practice_007_signal_snapshot_v1.json"
DEFAULT_ACCOUNT = STATE_ROOT / "account_007_dashboard_v1.json"
DEFAULT_HEARTBEAT = STATE_ROOT / "strategy_lab_heartbeat_v1.json"
DEFAULT_NEWS = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "pair_sentiment_latest.json"
)
DEFAULT_COLLECTOR = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "collector_latest_v1.json"
)
DEFAULT_NEWS_DATABASE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "local_news_sentiment_v1.sqlite"
)
DEFAULT_QUOTES = STATE_ROOT / "practice_007_market_quotes_v1.json"
SCHEMA_VERSION = "practice_007_signal_news_monitor_v7"
OUTCOME_HORIZONS_MIN = (1, 5, 15, 30, 60, 120, 180, 240, 360, 480, 720, 1440)
MAX_OUTCOME_LATENESS_SEC = 180.0
NEWS_MAX_AGE_SECONDS = 300.0
REJECTED_NEWS_MAX_PAIR_LEGS = 3
REJECTED_NEWS_MAX_SPREAD_PIPS = 5.0
REJECTED_NEWS_LIQUID_CURRENCIES = frozenset(
    {"AUD", "CAD", "CHF", "EUR", "GBP", "JPY", "NZD", "USD"}
)


def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def iso_utc(value: dt.datetime | None = None) -> str:
    return (value or utc_now()).astimezone(UTC).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def optional_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def account_snapshot_status(
    account_snapshot: Mapping[str, Any], account: Mapping[str, Any]
) -> dict[str, Any]:
    aggregate = (
        account_snapshot.get("aggregate")
        if isinstance(account_snapshot.get("aggregate"), Mapping)
        else {}
    )
    snapshot_state = str(aggregate.get("snapshot_state") or "").strip().lower()
    explicit_current = account.get(
        "account_values_current", aggregate.get("account_values_current")
    )
    explicit_ok = account.get("ok")
    unavailable_state = snapshot_state in {
        "unavailable",
        "retained_stale_account_values",
        "stale",
        "failed",
    }
    # Older fixtures/snapshots predate the explicit currentness contract. They
    # remain current only when a real account row and account values exist.
    legacy_values_present = any(
        account.get(key) is not None for key in ("NAV", "balance", "pl")
    )
    account_current = bool(account) and not unavailable_state
    if explicit_current is False or explicit_ok is False:
        account_current = False
    elif explicit_current is None and explicit_ok is None:
        account_current = account_current and legacy_values_present
    account_ok = bool(explicit_ok) if explicit_ok is not None else account_current
    return {
        "account_current": account_current,
        "account_ok": account_ok,
        "snapshot_state": snapshot_state
        or ("current" if account_current else "unavailable"),
        "positions_current": bool(
            account_current
            and account.get(
                "positions_current", aggregate.get("positions_current", True)
            )
        ),
        "orders_current": bool(
            account_current
            and account.get("orders_current", aggregate.get("orders_current", True))
        ),
        "last_verified": aggregate.get("last_verified")
        or account.get("last_verified"),
    }


def load_json(path: Path, attempts: int = 3) -> dict[str, Any]:
    for attempt in range(max(1, attempts)):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError, json.JSONDecodeError):
            if attempt + 1 < attempts:
                time.sleep(0.05)
    return {}


def parse_utc(value: Any) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False, default=str)
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def parse_stop(value: str | None) -> dt.datetime:
    if value:
        parsed = dt.datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ZoneInfo("America/New_York"))
        return parsed.astimezone(UTC)
    now_local = dt.datetime.now(tz=ZoneInfo("America/New_York"))
    target = now_local.replace(hour=9, minute=0, second=0, microsecond=0)
    if now_local >= target:
        target += dt.timedelta(days=1)
    return target.astimezone(UTC)


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cycles (
            observed_utc TEXT PRIMARY KEY,
            signal_updated_utc TEXT NOT NULL,
            signal_count INTEGER NOT NULL,
            qualified_count INTEGER NOT NULL,
            selected_json TEXT NOT NULL,
            news_generated_utc TEXT NOT NULL,
            active_news_articles INTEGER NOT NULL,
            account_nav REAL,
            account_balance REAL,
            open_trades INTEGER,
            pending_orders INTEGER,
            bot_status TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    cycle_columns = {
        str(row[1]): int(row[3] or 0)
        for row in connection.execute("PRAGMA table_info(cycles)").fetchall()
    }
    nullable_account_columns = (
        "account_nav",
        "account_balance",
        "open_trades",
        "pending_orders",
    )
    if any(cycle_columns.get(name, 0) for name in nullable_account_columns):
        # The original table encoded broker unavailability as zero because its
        # account fields were NOT NULL. Rebuild transactionally so new cycles
        # can retain SQL NULL and the JSON currentness contract stays truthful.
        connection.commit()
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """
                CREATE TABLE cycles_nullable_migration (
                    observed_utc TEXT PRIMARY KEY,
                    signal_updated_utc TEXT NOT NULL,
                    signal_count INTEGER NOT NULL,
                    qualified_count INTEGER NOT NULL,
                    selected_json TEXT NOT NULL,
                    news_generated_utc TEXT NOT NULL,
                    active_news_articles INTEGER NOT NULL,
                    account_nav REAL,
                    account_balance REAL,
                    open_trades INTEGER,
                    pending_orders INTEGER,
                    bot_status TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO cycles_nullable_migration
                SELECT observed_utc, signal_updated_utc, signal_count,
                       qualified_count, selected_json, news_generated_utc,
                       active_news_articles, account_nav, account_balance,
                       open_trades, pending_orders, bot_status, payload_json
                FROM cycles
                """
            )
            connection.execute("DROP TABLE cycles")
            connection.execute(
                "ALTER TABLE cycles_nullable_migration RENAME TO cycles"
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS observations (
            observed_utc TEXT NOT NULL,
            instrument TEXT NOT NULL,
            signal_direction TEXT NOT NULL,
            signal_confidence REAL NOT NULL,
            signal_expected_net_pips REAL NOT NULL,
            signal_eligible INTEGER NOT NULL,
            signal_blockers_json TEXT NOT NULL,
            news_direction TEXT NOT NULL,
            news_score REAL NOT NULL,
            news_confidence REAL NOT NULL,
            news_event_count INTEGER NOT NULL,
            relationship TEXT NOT NULL,
            bid REAL,
            ask REAL,
            mid REAL,
            signal_horizon_min INTEGER NOT NULL,
            news_horizon_min INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY (observed_utc, instrument)
        )
        """
    )
    existing_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(observations)").fetchall()
    }
    for name in ("bid", "ask"):
        if name not in existing_columns:
            connection.execute(f"ALTER TABLE observations ADD COLUMN {name} REAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS outcomes (
            forecast_utc TEXT NOT NULL,
            instrument TEXT NOT NULL,
            horizon_min INTEGER NOT NULL,
            outcome_utc TEXT NOT NULL,
            realized_min REAL NOT NULL,
            actual_direction TEXT NOT NULL,
            gross_return_bps REAL NOT NULL,
            signal_direction TEXT NOT NULL,
            news_direction TEXT NOT NULL,
            relationship TEXT NOT NULL,
            signal_gross_bps REAL,
            signal_executable_bps REAL,
            news_gross_bps REAL,
            news_executable_bps REAL,
            agreement_gross_bps REAL,
            agreement_executable_bps REAL,
            news_veto_gross_bps REAL,
            news_veto_executable_bps REAL,
            PRIMARY KEY (forecast_utc, instrument, horizon_min)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS independent_news_decisions (
            decision_id TEXT PRIMARY KEY,
            decided_utc TEXT NOT NULL,
            topic_id TEXT NOT NULL,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            confidence REAL NOT NULL,
            horizon_min INTEGER NOT NULL,
            headline TEXT NOT NULL,
            category TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_first_seen_utc TEXT NOT NULL,
            availability_lag_minutes REAL NOT NULL,
            forward_signal_timely INTEGER NOT NULL,
            reports_prior_market_move INTEGER NOT NULL,
            entry_bid REAL NOT NULL,
            entry_ask REAL NOT NULL,
            entry_mid REAL NOT NULL,
            pip REAL NOT NULL,
            entry_spread_pips REAL NOT NULL,
            status TEXT NOT NULL,
            outcome_utc TEXT,
            gross_pips REAL,
            executable_pips REAL,
            direction_hit INTEGER,
            beat_spread INTEGER,
            result_class TEXT,
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS independent_news_decisions_status_idx
        ON independent_news_decisions(status, decided_utc, horizon_min)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS rejected_news_shadows (
            decision_id TEXT PRIMARY KEY,
            decided_utc TEXT NOT NULL,
            topic_id TEXT NOT NULL,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            confidence REAL NOT NULL,
            horizon_min INTEGER NOT NULL,
            headline TEXT NOT NULL,
            category TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_first_seen_utc TEXT NOT NULL,
            rejection_reason TEXT NOT NULL,
            decision_kind TEXT NOT NULL,
            entry_bid REAL NOT NULL,
            entry_ask REAL NOT NULL,
            entry_mid REAL NOT NULL,
            pip REAL NOT NULL,
            entry_spread_pips REAL NOT NULL,
            status TEXT NOT NULL,
            outcome_utc TEXT,
            gross_pips REAL,
            executable_pips REAL,
            direction_hit INTEGER,
            beat_spread INTEGER,
            result_class TEXT,
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS rejected_news_shadows_status_idx
        ON rejected_news_shadows(status, decided_utc, horizon_min)
        """
    )
    connection.commit()
    return connection


def direction(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"buy", "long", "bullish", "up"}:
        return "long"
    if text in {"sell", "short", "bearish", "down"}:
        return "short"
    return "neutral"


def relationship(signal_direction: str, news_direction: str) -> str:
    if signal_direction == "neutral" or news_direction == "neutral":
        return "neutral"
    return "aligned" if signal_direction == news_direction else "opposed"


def rejection_reason_for_event(event: Mapping[str, Any]) -> str:
    if bool(event.get("reports_prior_market_move")):
        return "retrospective_market_move"
    if not bool(event.get("forward_signal_timely", True)):
        return "late_or_stale"
    if not (
        bool(event.get("source_verified"))
        or int(safe_float(event.get("distinct_source_count"), 0.0)) >= 2
    ):
        return "unverified_or_uncorroborated"
    return "no_forward_pair_direction"


def source_news_topic_id(event: Mapping[str, Any]) -> str:
    """Return the collector topic identity across both news output schemas."""

    return str(event.get("topic_id") or event.get("event_id") or "").strip()


def rejected_news_observation_key(event: Mapping[str, Any]) -> str:
    """Return a stable source-observation key across classifier revisions."""

    headline = re.sub(
        r"[^a-z0-9]+",
        " ",
        str(event.get("headline") or "").lower(),
    ).strip()
    source = re.sub(
        r"[^a-z0-9]+",
        " ",
        str(event.get("source_name") or "").lower(),
    ).strip()
    first_seen = str(
        event.get("source_first_seen_utc")
        or event.get("first_seen_utc")
        or ""
    ).strip()
    if not headline or not first_seen:
        return ""
    return hashlib.sha256(
        f"{source}|{headline}|{first_seen}".encode("utf-8")
    ).hexdigest()[:24]


def independent_news_factor_id(event: Mapping[str, Any]) -> str:
    """Collapse retrospective rewrites into one causal macro-factor episode.

    The collector intentionally preserves material updates as separate topics.
    That is useful provenance, but counting every late recap as an independent
    forecast exaggerates both misses and hits.  Only retrospective/non-timely
    intervention and commodity recaps receive this extra factor collapse;
    prospective releases retain their source topic identity.
    """

    source_id = source_news_topic_id(event)
    retrospective = bool(event.get("reports_prior_market_move")) or not bool(
        event.get("forward_signal_timely", True)
    )
    category = str(event.get("category") or "").strip().lower()
    prospective_macro = {
        "inflation": "inflation",
        "inflation_release": "inflation",
        "inflation_context": "inflation",
        "labor_market": "labor",
        "labor_release": "labor",
        "labour_release": "labor",
        "growth": "growth",
        "growth_release": "growth",
        "business_activity_release": "growth",
        "central_bank": "policy",
        "monetary_policy": "policy",
        "interest_rates": "policy",
        "policy_statement": "policy",
    }
    if category in prospective_macro:
        first_seen = parse_utc(event.get("first_seen_utc"))
        identity_day = (first_seen or utc_now()).date().isoformat()
        exposures = sorted(
            str(value).strip().upper()
            for value in event.get("currency_exposure_groups") or []
            if str(value).strip()
        )
        tags = sorted(
            str(tag).strip().lower()
            for tag in event.get("topic_tags") or []
            if str(tag).strip().startswith("#")
        )
        reference = str(
            event.get("reference_period")
            or event.get("event_series")
            or event.get("scheduled_release_utc")
            or identity_day
        ).strip().lower()
        identity = "|".join(
            ["prospective_macro", prospective_macro[category], identity_day,
             ",".join(exposures), ",".join(tags), reference]
        )
        return "factor_" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    if not retrospective or category not in {"fx_intervention", "commodity_shock"}:
        return source_id

    first_seen = parse_utc(event.get("first_seen_utc"))
    identity_day = (first_seen or utc_now()).date().isoformat()
    tags = {
        str(tag).strip().lower()
        for tag in event.get("topic_tags") or []
        if str(tag).strip()
    }
    headline = str(event.get("headline") or "").lower()

    if category == "fx_intervention":
        currencies: set[str] = set()
        directions: set[str] = set()
        for tag in tags:
            match = re.match(
                r"#([a-z]{3})_(?:[a-z_]*_)?(strengthening|weakening|mixed)$",
                tag,
            )
            if match:
                currencies.add(match.group(1).upper())
                directions.add(match.group(2))
        if not currencies and ("yen" in headline or "jpy" in headline):
            currencies.add("JPY")
        if not directions:
            if any(word in headline for word in ("support yen", "boost yen", "yen surge", "yen strengthen")):
                directions.add("strengthening")
            elif any(word in headline for word in ("weaken yen", "yen fall", "yen slide")):
                directions.add("weakening")
        factor_parts = [
            category,
            ",".join(sorted(currencies)) or "unspecified_currency",
            ",".join(sorted(directions)) or "unspecified_direction",
            identity_day,
        ]
    else:
        commodity = "oil" if "oil" in headline or any("oil_" in tag for tag in tags) else "commodity"
        directions: set[str] = set()
        for tag in tags:
            match = re.match(r"#[a-z0-9_]+_(up|down)$", tag)
            if match:
                directions.add(match.group(1))
        if not directions:
            if any(word in headline for word in ("drop", "drops", "fall", "falls", "slide", "slides", "tumble", "tumbles")):
                directions.add("down")
            elif any(word in headline for word in ("rise", "rises", "gain", "gains", "surge", "surges")):
                directions.add("up")
        factor_parts = [
            category,
            commodity,
            ",".join(sorted(directions)) or "unspecified_direction",
            identity_day,
        ]

    digest = hashlib.sha256("|".join(factor_parts).encode("utf-8")).hexdigest()[:24]
    return f"news_factor_{digest}"


def summarize_independent_news_rejections(
    news_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Deduplicate active news topics and explain why they produce no trade.

    Pair sentiment repeats one topic across many mapped instruments.  Reporting
    pair-leg counts would therefore make a single retrospective headline look
    like dozens of independent rejections.  This view gives each topic one
    vote, while retaining its mapped-pair count for coverage diagnostics.
    """

    pairs = news_snapshot.get("pairs") or {}
    topics: dict[str, dict[str, Any]] = {}
    if not isinstance(pairs, Mapping):
        pairs = {}
    for instrument, pair in pairs.items():
        if not isinstance(pair, Mapping):
            continue
        for event in pair.get("events") or []:
            if not isinstance(event, Mapping):
                continue
            source_topic_id = source_news_topic_id(event)
            topic_id = independent_news_factor_id(event)
            headline = str(event.get("headline") or "").strip()
            key = topic_id or source_topic_id or headline
            if not key:
                continue
            row = topics.setdefault(
                key,
                {
                    "topic_id": topic_id,
                    "source_topic_ids": set(),
                    "headline": headline,
                    "first_seen_utc": event.get("first_seen_utc"),
                    "source_name": event.get("source_name"),
                    "estimated_reaction_horizon_label": event.get(
                        "estimated_reaction_horizon_label"
                    ),
                    "mapped_pairs": set(),
                    "any_forward_pair_eligible": False,
                    "reports_prior_market_move": False,
                    "all_forward_signal_timely": True,
                    "any_verified_or_corroborated": False,
                },
            )
            if source_topic_id:
                row["source_topic_ids"].add(source_topic_id)
            row["mapped_pairs"].add(str(instrument))
            row["any_forward_pair_eligible"] = bool(
                row["any_forward_pair_eligible"]
                or event.get("forward_pair_eligible")
            )
            row["reports_prior_market_move"] = bool(
                row["reports_prior_market_move"]
                or event.get("reports_prior_market_move")
            )
            row["all_forward_signal_timely"] = bool(
                row["all_forward_signal_timely"]
                and event.get("forward_signal_timely")
            )
            row["any_verified_or_corroborated"] = bool(
                row["any_verified_or_corroborated"]
                or event.get("source_verified")
                or int(safe_float(event.get("distinct_source_count"), 0.0)) >= 2
            )

    rejected: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    for row in topics.values():
        if row.pop("any_forward_pair_eligible"):
            continue
        reason = rejection_reason_for_event(
            {
                "reports_prior_market_move": row["reports_prior_market_move"],
                "forward_signal_timely": row.pop("all_forward_signal_timely"),
                "source_verified": row.pop("any_verified_or_corroborated"),
                "distinct_source_count": 0,
            }
        )
        row.pop("all_forward_signal_timely", None)
        row.pop("any_verified_or_corroborated", None)
        mapped_pairs = sorted(row.pop("mapped_pairs"))
        source_topic_ids = sorted(row.pop("source_topic_ids"))
        row["mapped_pair_count"] = len(mapped_pairs)
        row["source_topic_ids"] = source_topic_ids
        row["source_topic_count"] = len(source_topic_ids)
        row["reason"] = reason
        reason_counts[reason] += 1
        rejected.append(row)
    rejected.sort(
        key=lambda row: str(row.get("first_seen_utc") or ""), reverse=True
    )
    return {
        "unique_rejected_topics": len(rejected),
        "reason_counts": dict(sorted(reason_counts.items())),
        "topics": rejected[:20],
        "weighting": (
            "one vote per independent news factor; source rewrites and mapped "
            "pairs are not independent votes"
        ),
    }


def compact_observation_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    """Keep causal references without duplicating the canonical news archive."""

    events = [
        event
        for event in row.get("news_events") or []
        if isinstance(event, Mapping)
    ]
    payload = {
        key: value
        for key, value in row.items()
        if key != "news_events"
    }
    payload["news_topic_ids"] = sorted(
        {
            source_news_topic_id(event)
            for event in events
            if source_news_topic_id(event)
        }
    )
    return payload


def compact_live_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return a bounded live row while retaining canonical event references.

    The same active news events are repeated across many mapped instruments.
    Embedding every full event in all 68 live rows produced megabyte snapshots
    and unnecessary serialization/I/O.  The canonical collector database and
    pair-sentiment artifact retain the complete event payloads; this monitor
    only needs their immutable topic identifiers for a causal join.
    """

    return compact_observation_payload(row)


def quote_values(
    signal_snapshot: Mapping[str, Any],
    instrument: str,
) -> tuple[float | None, float | None, float | None]:
    quotes = signal_snapshot.get("market_quotes") or {}
    quote = quotes.get(instrument) if isinstance(quotes, dict) else None
    if not isinstance(quote, dict):
        return None, None, None
    bid = safe_float(quote.get("bid"), math.nan)
    ask = safe_float(quote.get("ask"), math.nan)
    if math.isfinite(bid) and math.isfinite(ask) and bid > 0 and ask > 0:
        return bid, ask, (bid + ask) / 2.0
    mid = safe_float(quote.get("mid"), math.nan)
    valid_mid = mid if math.isfinite(mid) and mid > 0 else None
    return None, None, valid_mid


def quote_values_with_pip(
    signal_snapshot: Mapping[str, Any],
    quote_snapshot: Mapping[str, Any],
    instrument: str,
) -> tuple[float | None, float | None, float | None, float | None]:
    for snapshot in (quote_snapshot, signal_snapshot):
        quotes = snapshot.get("quotes") or snapshot.get("market_quotes") or {}
        quote = quotes.get(instrument) if isinstance(quotes, dict) else None
        if not isinstance(quote, dict):
            continue
        bid = safe_float(quote.get("bid"), math.nan)
        ask = safe_float(quote.get("ask"), math.nan)
        if math.isfinite(bid) and math.isfinite(ask) and bid > 0 and ask > 0:
            pip = safe_float(
                quote.get("pip"),
                0.01 if instrument.endswith("_JPY") else 0.0001,
            )
            return bid, ask, (bid + ask) / 2.0, pip
    bid, ask, mid = quote_values(signal_snapshot, instrument)
    pip = 0.01 if instrument.endswith("_JPY") else 0.0001
    return bid, ask, mid, pip if mid is not None else None


def quote_time_and_freshness(
    signal_snapshot: Mapping[str, Any],
    quote_snapshot: Mapping[str, Any],
    instrument: str,
    observed: dt.datetime,
    maximum_age_sec: float = 180.0,
) -> tuple[str | None, bool]:
    """Use the venue quote timestamp, never the snapshot write timestamp."""
    saw_untimestamped_quote = False
    for snapshot in (quote_snapshot, signal_snapshot):
        quotes = snapshot.get("quotes") or snapshot.get("market_quotes") or {}
        quote = quotes.get(instrument) if isinstance(quotes, dict) else None
        if not isinstance(quote, dict):
            continue
        if safe_float(quote.get("bid"), 0.0) > 0 and safe_float(quote.get("ask"), 0.0) > 0:
            saw_untimestamped_quote = True
        raw = quote.get("time") or quote.get("quote_time_utc") or quote.get("observed_utc")
        parsed = parse_utc(raw)
        if parsed is None:
            # Older in-memory signal snapshots did not duplicate quote time on
            # each instrument. Their generation time is the best available
            # timestamp; the dedicated venue quote snapshot never gets this
            # fallback because it carries per-quote timestamps.
            if snapshot is signal_snapshot:
                parsed = parse_utc(snapshot.get("updated_at") or snapshot.get("generated_utc"))
            if parsed is None:
                continue
        age = (observed - parsed).total_seconds()
        return iso_utc(parsed), -5.0 <= age <= maximum_age_sec
    # Backward-compatible for legacy/test snapshots that never carried any
    # timestamp. Production venue snapshots do carry one and are evaluated
    # strictly above.
    return None, saw_untimestamped_quote


def decision_id(topic_id: str, instrument: str, horizon_min: int) -> str:
    raw = f"{topic_id}|{instrument}|{int(horizon_min)}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


def rejected_news_decision_id(
    topic_id: str,
    instrument: str,
    horizon_min: int,
    rejection_reason: str,
) -> str:
    raw = (
        f"rejected|{topic_id}|{instrument}|{int(horizon_min)}|{rejection_reason}"
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


def rejected_news_liquid_instrument(instrument: str) -> bool:
    parts = str(instrument).split("_")
    return len(parts) == 2 and all(
        currency in REJECTED_NEWS_LIQUID_CURRENCIES for currency in parts
    )


def decision_activation_limit_minutes(horizon_min: int) -> float:
    """Maximum age at which retained news may start a prospective decision."""
    return min(30.0, max(5.0, 0.25 * float(horizon_min)))


def quarantine_late_activation_decisions(
    connection: sqlite3.Connection,
) -> int:
    """Exclude pending rows that were initiated too long after first observation."""
    changed = 0
    pending = connection.execute(
        """
        SELECT decision_id, decided_utc, source_first_seen_utc, horizon_min
        FROM independent_news_decisions
        WHERE status = 'pending'
        """
    ).fetchall()
    for identifier, decided_utc, first_seen_utc, horizon_min in pending:
        decided = parse_utc(decided_utc)
        first_seen = parse_utc(first_seen_utc)
        if decided is None or first_seen is None:
            continue
        activation_age = max(0.0, (decided - first_seen).total_seconds() / 60.0)
        if activation_age <= decision_activation_limit_minutes(int(horizon_min)):
            continue
        connection.execute(
            """
            UPDATE independent_news_decisions
            SET status = 'excluded_late_activation',
                result_class = 'late_activation'
            WHERE decision_id = ? AND status = 'pending'
            """,
            (identifier,),
        )
        changed += int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    return changed


def quarantine_reclassified_independent_news_decisions(
    connection: sqlite3.Connection,
    news_database: Path | None,
    observed: dt.datetime,
) -> int:
    """Exclude pending directional decisions removed by a classifier migration.

    Independent decisions are prospective research evidence, so rows already
    written are never deleted.  However, a later classifier repair can reveal
    that the underlying topic was a retrospective market recap, context-only,
    or no longer had a directional currency mapping.  Such rows must be
    quarantined before maturity or they would create artificial hit/miss data.

    Prefer the consolidated topic row because it reflects deduplication and the
    newest classifier.  Fall back to the retained source article when an older
    decision used a pre-migration topic id.
    """

    if news_database is None or not news_database.exists():
        return 0
    pending = connection.execute(
        """
        SELECT decision_id, topic_id, source_name, headline, payload_json
        FROM independent_news_decisions
        WHERE status = 'pending'
        ORDER BY decision_id
        """
    ).fetchall()
    if not pending:
        return 0
    try:
        source = sqlite3.connect(
            f"file:{news_database.as_posix()}?mode=ro",
            uri=True,
            timeout=1.0,
        )
        source.execute("PRAGMA query_only = ON")
    except sqlite3.Error:
        return 0
    invalidated = 0
    try:
        for decision_id_value, topic_id, source_name, headline, payload_json in pending:
            try:
                payload = json.loads(payload_json or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            source_topic_id = str(
                payload.get("source_topic_id") or topic_id or ""
            ).strip()
            current_id = ""
            current_payload: dict[str, Any] = {}
            try:
                topic = source.execute(
                    "SELECT topic_id, payload_json FROM topic_events WHERE topic_id = ?",
                    (str(topic_id or ""),),
                ).fetchone()
            except sqlite3.Error:
                topic = None
            if topic is not None:
                current_id = str(topic[0] or "")
                try:
                    current_payload = json.loads(topic[1] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    current_payload = {}
            if not current_payload:
                article = source.execute(
                    "SELECT event_id, payload_json FROM articles WHERE event_id = ?",
                    (source_topic_id,),
                ).fetchone()
                if article is None and str(headline or "").strip():
                    article = source.execute(
                        """
                        SELECT event_id, payload_json
                        FROM articles
                        WHERE source_name = ? AND headline = ?
                        ORDER BY first_seen_utc
                        LIMIT 1
                        """,
                        (str(source_name or ""), str(headline or "")),
                    ).fetchone()
                if article is not None:
                    current_id = str(article[0] or "")
                    try:
                        current_payload = json.loads(article[1] or "{}")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        current_payload = {}
            if not current_payload:
                continue
            exclusion_reason = str(
                current_payload.get("exclusion_reason") or ""
            ).strip()
            currency_scores = current_payload.get("currency_scores")
            mapping_removed = isinstance(currency_scores, dict) and not any(
                abs(safe_float(score, 0.0)) > 0.0
                for score in currency_scores.values()
            )
            invalid_reason = ""
            if bool(current_payload.get("reports_prior_market_move")):
                invalid_reason = "reported_market_move_context"
            elif bool(current_payload.get("context_only")):
                invalid_reason = str(
                    current_payload.get("context_reason") or "context_only"
                )
            elif bool(current_payload.get("localized_parallel_currency_market")):
                invalid_reason = "localized_parallel_currency_market"
            elif exclusion_reason:
                invalid_reason = exclusion_reason
            elif mapping_removed and current_payload.get("directional_evidence") is False:
                invalid_reason = "directional_currency_mapping_removed"
            if not invalid_reason:
                continue
            payload.update(
                {
                    "status": "invalidated_classification",
                    "result_class": "excluded_reclassified_news_mapping",
                    "classification_correction": current_payload.get(
                        "classification_version"
                    ),
                    "classification_exclusion_reason": invalid_reason,
                    "replacement_event_id": current_id,
                    "current_context_only": bool(
                        current_payload.get("context_only")
                    ),
                    "current_reports_prior_market_move": bool(
                        current_payload.get("reports_prior_market_move")
                    ),
                    "current_currency_scores": currency_scores,
                    "quarantined_utc": iso_utc(observed),
                }
            )
            connection.execute(
                """
                UPDATE independent_news_decisions
                SET status = 'invalidated_classification', outcome_utc = ?,
                    reports_prior_market_move = ?,
                    result_class = 'excluded_reclassified_news_mapping',
                    payload_json = ?
                WHERE decision_id = ? AND status = 'pending'
                """,
                (
                    iso_utc(observed),
                    int(bool(current_payload.get("reports_prior_market_move"))),
                    json.dumps(payload, sort_keys=True),
                    decision_id_value,
                ),
            )
            invalidated += int(
                connection.execute("SELECT changes()").fetchone()[0] > 0
            )
    except sqlite3.Error:
        return invalidated
    finally:
        source.close()
    return invalidated


def record_independent_news_decisions(
    connection: sqlite3.Connection,
    rows: list[dict[str, Any]],
    observed: dt.datetime,
) -> int:
    inserted = 0
    for row in rows:
        if not bool(row.get("quote_fresh", True)):
            continue
        side = str(row.get("news_direction") or "neutral")
        if side not in {"long", "short"}:
            continue
        bid = safe_float(row.get("bid"), 0.0)
        ask = safe_float(row.get("ask"), 0.0)
        mid = safe_float(row.get("mid"), 0.0)
        pip = safe_float(row.get("pip"), 0.0)
        if min(bid, ask, mid, pip) <= 0.0:
            continue
        events = [
            event
            for event in row.get("news_events") or []
            if isinstance(event, dict)
            and abs(safe_float(event.get("pair_score"), 0.0)) > 0.0
            and bool(event.get("forward_pair_eligible"))
            and bool(event.get("forward_signal_timely", True))
            and not bool(event.get("reports_prior_market_move"))
        ]
        if not events:
            continue
        event = events[0]
        source_topic_id = source_news_topic_id(event)
        topic_id = independent_news_factor_id(event)
        instrument = str(row.get("instrument") or "").strip()
        horizon_min = max(
            1,
            int(
                safe_float(
                    event.get("estimated_reaction_horizon_minutes"),
                    safe_float(row.get("news_horizon_min"), 60.0),
                )
            ),
        )
        if not topic_id or not instrument:
            continue
        first_seen = parse_utc(event.get("first_seen_utc"))
        if first_seen is None:
            continue
        activation_age_minutes = max(
            0.0, (observed - first_seen).total_seconds() / 60.0
        )
        activation_limit_minutes = decision_activation_limit_minutes(horizon_min)
        if activation_age_minutes > activation_limit_minutes:
            continue
        identifier = decision_id(topic_id, instrument, horizon_min)
        payload = {
            "decision_id": identifier,
            "decided_utc": iso_utc(observed),
            "topic_id": topic_id,
            "source_topic_id": source_topic_id,
            "instrument": instrument,
            "direction": side,
            "confidence": safe_float(row.get("news_confidence"), 0.0),
            "horizon_min": horizon_min,
            "headline": str(event.get("headline") or ""),
            "category": str(event.get("category") or ""),
            "source_name": str(event.get("source_name") or ""),
            "source_first_seen_utc": str(event.get("first_seen_utc") or ""),
            "availability_lag_minutes": safe_float(
                event.get("availability_lag_minutes"), 0.0
            ),
            "decision_activation_age_minutes": activation_age_minutes,
            "decision_activation_limit_minutes": activation_limit_minutes,
            "forward_signal_timely": True,
            "reports_prior_market_move": False,
            "entry_bid": bid,
            "entry_ask": ask,
            "entry_mid": mid,
            "pip": pip,
            "entry_spread_pips": (ask - bid) / pip,
            "status": "pending",
        }
        connection.execute(
            """
            INSERT OR IGNORE INTO independent_news_decisions (
                decision_id, decided_utc, topic_id, instrument, direction,
                confidence, horizon_min, headline, category, source_name,
                source_first_seen_utc, availability_lag_minutes,
                forward_signal_timely, reports_prior_market_move,
                entry_bid, entry_ask, entry_mid, pip, entry_spread_pips,
                status, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identifier,
                payload["decided_utc"],
                topic_id,
                instrument,
                side,
                payload["confidence"],
                horizon_min,
                payload["headline"],
                payload["category"],
                payload["source_name"],
                payload["source_first_seen_utc"],
                payload["availability_lag_minutes"],
                1,
                0,
                bid,
                ask,
                mid,
                pip,
                payload["entry_spread_pips"],
                "pending",
                json.dumps(payload, sort_keys=True),
            ),
        )
        inserted += int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    return inserted


def mature_independent_news_decisions(
    connection: sqlite3.Connection,
    rows: list[dict[str, Any]],
    observed: dt.datetime,
) -> int:
    quotes = {
        str(row.get("instrument")): row
        for row in rows
        if bool(row.get("quote_fresh", True)) and min(
            safe_float(row.get("bid"), 0.0),
            safe_float(row.get("ask"), 0.0),
        )
        > 0.0
    }
    matured = 0
    pending = connection.execute(
        """
        SELECT decision_id, decided_utc, instrument, direction, horizon_min,
               entry_bid, entry_ask, entry_mid, pip, payload_json
        FROM independent_news_decisions
        WHERE status = 'pending'
        ORDER BY decided_utc, decision_id
        """
    ).fetchall()
    for prior in pending:
        (
            identifier,
            decided_utc,
            instrument,
            side,
            horizon_min,
            entry_bid,
            entry_ask,
            entry_mid,
            pip,
            payload_json,
        ) = prior
        decided = parse_utc(decided_utc)
        current = quotes.get(str(instrument))
        if (
            decided is None
            or observed < decided + dt.timedelta(minutes=int(horizon_min))
            or current is None
        ):
            continue
        exit_bid = safe_float(current.get("bid"))
        exit_ask = safe_float(current.get("ask"))
        exit_mid = safe_float(current.get("mid"))
        pip = safe_float(pip)
        sign = 1.0 if str(side) == "long" else -1.0
        gross_pips = sign * (exit_mid - safe_float(entry_mid)) / pip
        executable_pips = (
            (exit_bid - safe_float(entry_ask)) / pip
            if str(side) == "long"
            else (safe_float(entry_bid) - exit_ask) / pip
        )
        direction_hit = gross_pips > 0.0
        beat_spread = executable_pips > 0.0
        result_class = (
            "captured_after_cost"
            if beat_spread
            else "right_too_small_for_spread"
            if direction_hit
            else "wrong_direction"
        )
        try:
            payload = json.loads(str(payload_json))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        payload.update(
            {
                "outcome_utc": iso_utc(observed),
                "gross_pips": round(gross_pips, 6),
                "executable_pips": round(executable_pips, 6),
                "direction_hit": direction_hit,
                "beat_spread": beat_spread,
                "result_class": result_class,
            }
        )
        connection.execute(
            """
            UPDATE independent_news_decisions
            SET status = 'matured', outcome_utc = ?, gross_pips = ?,
                executable_pips = ?, direction_hit = ?, beat_spread = ?,
                result_class = ?, payload_json = ?
            WHERE decision_id = ? AND status = 'pending'
            """,
            (
                iso_utc(observed),
                gross_pips,
                executable_pips,
                int(direction_hit),
                int(beat_spread),
                result_class,
                json.dumps(payload, sort_keys=True),
                identifier,
            ),
        )
        matured += int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    return matured


def record_rejected_news_shadows(
    connection: sqlite3.Connection,
    rows: list[dict[str, Any]],
    observed: dt.datetime,
) -> int:
    """Persist bounded counterfactuals for news the live policy rejects.

    The headline may map to dozens of pairs.  To avoid manufacturing sample
    size, retain at most three low-spread representative legs per topic and
    later report the topic as one equal-weight episode.  Retrospective market
    recaps are labelled separately and never counted as prospective misses.
    """

    rows = [row for row in rows if bool(row.get("quote_fresh", True))]
    # Corrected publisher metadata can change a topic from stale/retrospective
    # to timely (or the reverse), which changes its derived factor id.  It is
    # still the same causal source topic and must retain its earliest entry.
    # Quarantine any already-written identity drift rows and prevent a later
    # price from starting a second counterfactual.
    existing_source_topics: set[str] = set()
    existing_observation_keys: set[str] = set()
    canonical_topic_by_source: dict[str, str] = {}
    canonical_topic_by_observation: dict[str, str] = {}
    existing = connection.execute(
        """
        SELECT decision_id, decided_utc, topic_id, status, payload_json
        FROM rejected_news_shadows
        ORDER BY decided_utc, decision_id
        """
    ).fetchall()
    for decision_id, _decided_utc, stored_topic_id, status, payload_json in existing:
        try:
            stored_payload = json.loads(payload_json or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            stored_payload = {}
        source_topic_id = str(
            stored_payload.get("source_topic_id") or stored_topic_id or ""
        ).strip()
        observation_key = rejected_news_observation_key(stored_payload)
        stored_topic = str(stored_topic_id or "")
        duplicate_identity = False
        if source_topic_id:
            existing_source_topics.add(source_topic_id)
            canonical_topic = canonical_topic_by_source.setdefault(
                source_topic_id,
                stored_topic,
            )
            duplicate_identity = stored_topic != canonical_topic
        if observation_key:
            existing_observation_keys.add(observation_key)
            canonical_observation_topic = canonical_topic_by_observation.setdefault(
                observation_key,
                stored_topic,
            )
            duplicate_identity = bool(
                duplicate_identity
                or stored_topic != canonical_observation_topic
            )
        if (
            duplicate_identity
            and str(status or "") in {"pending", "matured"}
        ):
            connection.execute(
                """
                UPDATE rejected_news_shadows
                SET status = 'superseded_duplicate'
                WHERE decision_id = ?
                """,
                (decision_id,),
            )

    by_topic: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        instrument = str(row.get("instrument") or "").strip()
        bid = safe_float(row.get("bid"), 0.0)
        ask = safe_float(row.get("ask"), 0.0)
        mid = safe_float(row.get("mid"), 0.0)
        pip = safe_float(row.get("pip"), 0.0)
        if (
            not rejected_news_liquid_instrument(instrument)
            or min(bid, ask, mid, pip) <= 0.0
        ):
            continue
        spread_pips = (ask - bid) / pip
        if spread_pips < 0.0 or spread_pips > REJECTED_NEWS_MAX_SPREAD_PIPS:
            continue
        for event in row.get("news_events") or []:
            if not isinstance(event, Mapping):
                continue
            if bool(event.get("forward_pair_eligible")):
                continue
            if abs(safe_float(event.get("pair_score"), 0.0)) > 0.0:
                continue
            latent_score = safe_float(event.get("research_pair_score"), 0.0)
            if abs(latent_score) <= 0.0:
                continue
            source_topic_id = source_news_topic_id(event)
            observation_key = rejected_news_observation_key(event)
            if (
                (source_topic_id and source_topic_id in existing_source_topics)
                or (
                    observation_key
                    and observation_key in existing_observation_keys
                )
            ):
                continue
            topic_id = independent_news_factor_id(event)
            first_seen = parse_utc(event.get("first_seen_utc"))
            if not topic_id or first_seen is None:
                continue
            horizon_min = max(
                1,
                int(
                    safe_float(
                        event.get("estimated_reaction_horizon_minutes"),
                        safe_float(row.get("news_horizon_min"), 60.0),
                    )
                ),
            )
            reason = rejection_reason_for_event(event)
            kind = (
                "retrospective_continuation_counterfactual"
                if reason == "retrospective_market_move"
                else "prospective_gate_counterfactual"
            )
            side = "long" if latent_score > 0.0 else "short"
            candidate = {
                "topic_id": topic_id,
                "source_topic_id": source_topic_id,
                "instrument": instrument,
                "direction": side,
                "confidence": min(1.0, abs(latent_score)),
                "latent_score": latent_score,
                "horizon_min": horizon_min,
                "headline": str(event.get("headline") or ""),
                "category": str(event.get("category") or ""),
                "source_name": str(event.get("source_name") or ""),
                "source_first_seen_utc": iso_utc(first_seen),
                "decision_activation_age_minutes": max(
                    0.0, (observed - first_seen).total_seconds() / 60.0
                ),
                "rejection_reason": reason,
                "decision_kind": kind,
                "entry_bid": bid,
                "entry_ask": ask,
                "entry_mid": mid,
                "pip": pip,
                "entry_spread_pips": spread_pips,
                "selection_rank": abs(latent_score) / (1.0 + spread_pips),
            }
            prior = by_topic.setdefault(topic_id, {}).get(instrument)
            if prior is None or candidate["selection_rank"] > prior["selection_rank"]:
                by_topic[topic_id][instrument] = candidate

    inserted = 0
    for topic_id in sorted(by_topic):
        existing_instruments = {
            str(row[0])
            for row in connection.execute(
                "SELECT instrument FROM rejected_news_shadows WHERE topic_id = ?",
                (topic_id,),
            ).fetchall()
        }
        remaining = max(
            0, REJECTED_NEWS_MAX_PAIR_LEGS - len(existing_instruments)
        )
        if remaining <= 0:
            continue
        candidates = sorted(
            (
                item
                for item in by_topic[topic_id].values()
                if item["instrument"] not in existing_instruments
            ),
            key=lambda item: (-safe_float(item["selection_rank"]), item["instrument"]),
        )[:remaining]
        for payload in candidates:
            identifier = rejected_news_decision_id(
                topic_id,
                payload["instrument"],
                payload["horizon_min"],
                payload["rejection_reason"],
            )
            payload = dict(payload)
            payload.pop("selection_rank", None)
            payload.update(
                {
                    "decision_id": identifier,
                    "decided_utc": iso_utc(observed),
                    "status": "pending",
                    "measurement_note": (
                        "latent rejected direction; retrospective recaps are "
                        "continuation-only and excluded from prospective misses"
                    ),
                }
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO rejected_news_shadows (
                    decision_id, decided_utc, topic_id, instrument, direction,
                    confidence, horizon_min, headline, category, source_name,
                    source_first_seen_utc, rejection_reason, decision_kind,
                    entry_bid, entry_ask, entry_mid, pip, entry_spread_pips,
                    status, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    identifier,
                    payload["decided_utc"],
                    topic_id,
                    payload["instrument"],
                    payload["direction"],
                    payload["confidence"],
                    payload["horizon_min"],
                    payload["headline"],
                    payload["category"],
                    payload["source_name"],
                    payload["source_first_seen_utc"],
                    payload["rejection_reason"],
                    payload["decision_kind"],
                    payload["entry_bid"],
                    payload["entry_ask"],
                    payload["entry_mid"],
                    payload["pip"],
                    payload["entry_spread_pips"],
                    "pending",
                    json.dumps(payload, sort_keys=True),
                ),
            )
            inserted += int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    return inserted


def quarantine_reclassified_rejected_news_shadows(
    connection: sqlite3.Connection,
    news_database: Path | None,
    observed: dt.datetime,
) -> int:
    """Quarantine pending counterfactuals invalidated by later news rules.

    The news collector deliberately preserves raw articles while allowing a
    classifier migration to remove a false global-currency mapping.  A shadow
    decision written before that migration must not later mature into the
    research hit/miss statistics.  Match the retained article by event id when
    possible and fall back to exact source/headline identity because classifier
    migrations can also change a legacy event id.
    """

    if news_database is None or not news_database.exists():
        return 0
    pending = connection.execute(
        """
        SELECT decision_id, topic_id, source_name, headline, payload_json
        FROM rejected_news_shadows
        WHERE status = 'pending'
        ORDER BY decision_id
        """
    ).fetchall()
    if not pending:
        return 0
    try:
        source = sqlite3.connect(
            f"file:{news_database.as_posix()}?mode=ro",
            uri=True,
            timeout=1.0,
        )
        source.execute("PRAGMA query_only = ON")
    except sqlite3.Error:
        return 0
    invalidated = 0
    try:
        for decision_id, topic_id, source_name, headline, payload_json in pending:
            try:
                payload = json.loads(payload_json or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            source_topic_id = str(
                payload.get("source_topic_id") or topic_id or ""
            ).strip()
            article = source.execute(
                "SELECT event_id, payload_json FROM articles WHERE event_id = ?",
                (source_topic_id,),
            ).fetchone()
            if article is None and str(headline or "").strip():
                article = source.execute(
                    """
                    SELECT event_id, payload_json
                    FROM articles
                    WHERE source_name = ? AND headline = ?
                    ORDER BY first_seen_utc
                    LIMIT 1
                    """,
                    (str(source_name or ""), str(headline or "")),
                ).fetchone()
            if article is None:
                continue
            try:
                article_payload = json.loads(article[1] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                article_payload = {}
            exclusion_reason = str(
                article_payload.get("exclusion_reason") or ""
            ).strip()
            if not (
                bool(article_payload.get("localized_parallel_currency_market"))
                or exclusion_reason
            ):
                continue
            payload.update(
                {
                    "status": "invalidated_classification",
                    "result_class": "excluded_reclassified_news_mapping",
                    "classification_correction": article_payload.get(
                        "classification_version"
                    ),
                    "classification_exclusion_reason": exclusion_reason,
                    "replacement_event_id": str(article[0] or ""),
                    "quarantined_utc": iso_utc(observed),
                }
            )
            connection.execute(
                """
                UPDATE rejected_news_shadows
                SET status = 'invalidated_classification',
                    result_class = 'excluded_reclassified_news_mapping',
                    payload_json = ?
                WHERE decision_id = ? AND status = 'pending'
                """,
                (json.dumps(payload, sort_keys=True), decision_id),
            )
            invalidated += int(
                connection.execute("SELECT changes()").fetchone()[0] > 0
            )
    except sqlite3.Error:
        return invalidated
    finally:
        source.close()
    return invalidated


def mature_rejected_news_shadows(
    connection: sqlite3.Connection,
    rows: list[dict[str, Any]],
    observed: dt.datetime,
) -> int:
    quotes = {
        str(row.get("instrument")): row
        for row in rows
        if bool(row.get("quote_fresh", True)) and min(
            safe_float(row.get("bid"), 0.0),
            safe_float(row.get("ask"), 0.0),
        )
        > 0.0
    }
    pending = connection.execute(
        """
        SELECT decision_id, decided_utc, instrument, direction, horizon_min,
               entry_bid, entry_ask, entry_mid, pip, rejection_reason,
               decision_kind, payload_json
        FROM rejected_news_shadows
        WHERE status = 'pending'
        ORDER BY decided_utc, decision_id
        """
    ).fetchall()
    matured = 0
    for prior in pending:
        (
            identifier,
            decided_utc,
            instrument,
            side,
            horizon_min,
            entry_bid,
            entry_ask,
            entry_mid,
            pip,
            rejection_reason,
            decision_kind,
            payload_json,
        ) = prior
        decided = parse_utc(decided_utc)
        current = quotes.get(str(instrument))
        if (
            decided is None
            or observed < decided + dt.timedelta(minutes=int(horizon_min))
            or current is None
        ):
            continue
        exit_bid = safe_float(current.get("bid"))
        exit_ask = safe_float(current.get("ask"))
        exit_mid = safe_float(current.get("mid"))
        pip = safe_float(pip)
        sign = 1.0 if str(side) == "long" else -1.0
        gross_pips = sign * (exit_mid - safe_float(entry_mid)) / pip
        executable_pips = (
            (exit_bid - safe_float(entry_ask)) / pip
            if str(side) == "long"
            else (safe_float(entry_bid) - exit_ask) / pip
        )
        direction_hit = gross_pips > 0.0
        beat_spread = executable_pips > 0.0
        if str(decision_kind) == "retrospective_continuation_counterfactual":
            result_class = (
                "post_recap_continuation_after_cost"
                if beat_spread
                else "post_recap_right_but_below_cost"
                if direction_hit
                else "post_recap_reversal"
            )
        else:
            result_class = (
                "missed_after_cost"
                if beat_spread
                else "correct_reject_below_cost"
                if direction_hit
                else "correct_reject_wrong_direction"
            )
        try:
            payload = json.loads(str(payload_json))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        payload.update(
            {
                "outcome_utc": iso_utc(observed),
                "gross_pips": round(gross_pips, 6),
                "executable_pips": round(executable_pips, 6),
                "direction_hit": direction_hit,
                "beat_spread": beat_spread,
                "result_class": result_class,
                "rejection_reason": rejection_reason,
            }
        )
        connection.execute(
            """
            UPDATE rejected_news_shadows
            SET status = 'matured', outcome_utc = ?, gross_pips = ?,
                executable_pips = ?, direction_hit = ?, beat_spread = ?,
                result_class = ?, payload_json = ?
            WHERE decision_id = ? AND status = 'pending'
            """,
            (
                iso_utc(observed),
                gross_pips,
                executable_pips,
                int(direction_hit),
                int(beat_spread),
                result_class,
                json.dumps(payload, sort_keys=True),
                identifier,
            ),
        )
        matured += int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    return matured


def signed_bps(side: str, raw_return_bps: float) -> float | None:
    if side == "long":
        return raw_return_bps
    if side == "short":
        return -raw_return_bps
    return None


def executable_bps(
    side: str,
    *,
    entry_bid: float,
    entry_ask: float,
    entry_mid: float,
    exit_bid: float,
    exit_ask: float,
) -> float | None:
    if side == "long":
        return (exit_bid - entry_ask) / entry_mid * 10_000.0
    if side == "short":
        return (entry_bid - exit_ask) / entry_mid * 10_000.0
    return None


def mature_outcomes(
    connection: sqlite3.Connection,
    rows: list[dict[str, Any]],
    observed: dt.datetime,
) -> int:
    complete_quotes = {
        str(row["instrument"]): row
        for row in rows
        if bool(row.get("quote_fresh", True)) and all(
            safe_float(row.get(key), 0.0) > 0.0
            for key in ("bid", "ask", "mid")
        )
    }
    inserted = 0
    for horizon_min in OUTCOME_HORIZONS_MIN:
        latest = observed - dt.timedelta(minutes=horizon_min)
        earliest = latest - dt.timedelta(seconds=MAX_OUTCOME_LATENESS_SEC)
        pending = connection.execute(
            """
            SELECT o.observed_utc, o.instrument, o.signal_direction,
                   o.news_direction, o.relationship, o.bid, o.ask, o.mid
            FROM observations AS o
            LEFT JOIN outcomes AS x
              ON x.forecast_utc = o.observed_utc
             AND x.instrument = o.instrument
             AND x.horizon_min = ?
            WHERE x.forecast_utc IS NULL
              AND (o.signal_direction != 'neutral' OR o.news_direction != 'neutral')
              AND o.observed_utc >= ?
              AND o.observed_utc <= ?
              AND o.bid IS NOT NULL
              AND o.ask IS NOT NULL
              AND o.mid IS NOT NULL
            """,
            (horizon_min, iso_utc(earliest), iso_utc(latest)),
        ).fetchall()
        for prior in pending:
            (
                forecast_utc,
                instrument,
                signal_side,
                news_side,
                relation,
                entry_bid,
                entry_ask,
                entry_mid,
            ) = prior
            current = complete_quotes.get(str(instrument))
            if not current:
                continue
            forecast_time = parse_utc(forecast_utc)
            if forecast_time is None:
                continue
            exit_bid = safe_float(current["bid"])
            exit_ask = safe_float(current["ask"])
            exit_mid = safe_float(current["mid"])
            entry_bid = safe_float(entry_bid)
            entry_ask = safe_float(entry_ask)
            entry_mid = safe_float(entry_mid)
            raw_return_bps = (exit_mid - entry_mid) / entry_mid * 10_000.0
            signal_gross = signed_bps(str(signal_side), raw_return_bps)
            news_gross = signed_bps(str(news_side), raw_return_bps)
            signal_net = executable_bps(
                str(signal_side),
                entry_bid=entry_bid,
                entry_ask=entry_ask,
                entry_mid=entry_mid,
                exit_bid=exit_bid,
                exit_ask=exit_ask,
            )
            news_net = executable_bps(
                str(news_side),
                entry_bid=entry_bid,
                entry_ask=entry_ask,
                entry_mid=entry_mid,
                exit_bid=exit_bid,
                exit_ask=exit_ask,
            )
            agreement_active = relation == "aligned"
            veto_active = (
                str(signal_side) != "neutral"
                and str(relation) != "opposed"
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO outcomes (
                    forecast_utc, instrument, horizon_min, outcome_utc,
                    realized_min, actual_direction, gross_return_bps,
                    signal_direction, news_direction, relationship,
                    signal_gross_bps, signal_executable_bps,
                    news_gross_bps, news_executable_bps,
                    agreement_gross_bps, agreement_executable_bps,
                    news_veto_gross_bps, news_veto_executable_bps
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    forecast_utc,
                    instrument,
                    horizon_min,
                    iso_utc(observed),
                    (observed - forecast_time).total_seconds() / 60.0,
                    "long" if raw_return_bps > 0 else (
                        "short" if raw_return_bps < 0 else "neutral"
                    ),
                    raw_return_bps,
                    signal_side,
                    news_side,
                    relation,
                    signal_gross,
                    signal_net,
                    news_gross,
                    news_net,
                    signal_gross if agreement_active else None,
                    signal_net if agreement_active else None,
                    signal_gross if veto_active else None,
                    signal_net if veto_active else None,
                ),
            )
            inserted += int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    return inserted


def record_cycle(
    connection: sqlite3.Connection,
    *,
    signal_snapshot: Mapping[str, Any],
    account_snapshot: Mapping[str, Any],
    heartbeat: Mapping[str, Any],
    news_snapshot: Mapping[str, Any],
    collector_snapshot: Mapping[str, Any],
    observed: dt.datetime,
    quote_snapshot: Mapping[str, Any] | None = None,
    news_database: Path | None = None,
) -> dict[str, Any]:
    observed_utc = iso_utc(observed)
    top_signals = [
        row
        for row in signal_snapshot.get("top_signals") or []
        if isinstance(row, dict)
    ]
    news_generated = parse_utc(news_snapshot.get("generated_utc"))
    news_age_sec = (
        max(0.0, (observed - news_generated).total_seconds())
        if news_generated
        else None
    )
    collector_ok = str(collector_snapshot.get("status") or "").lower() == "ok"
    news_fresh = bool(
        collector_ok
        and news_age_sec is not None
        and news_age_sec <= NEWS_MAX_AGE_SECONDS
    )
    news_pairs = (news_snapshot.get("pairs") or {}) if news_fresh else {}
    independent_news_rejections = (
        summarize_independent_news_rejections(news_snapshot)
        if news_fresh
        else {
            "unique_rejected_topics": 0,
            "reason_counts": {},
            "topics": [],
            "weighting": "news unavailable or stale",
        }
    )
    quote_snapshot = quote_snapshot or {}
    quote_pairs = quote_snapshot.get("quotes") or {}
    account = (
        (account_snapshot.get("accounts") or [{}])[0]
        if account_snapshot.get("accounts")
        else {}
    )
    account_state = account_snapshot_status(account_snapshot, account)
    rows: list[dict[str, Any]] = []
    signals_by_instrument = {
        str(signal.get("instrument")): signal
        for signal in top_signals
        if str(signal.get("instrument") or "")
    }
    instruments = sorted(
        set(signals_by_instrument)
        | (set(news_pairs) if isinstance(news_pairs, dict) else set())
        | (set(quote_pairs) if isinstance(quote_pairs, dict) else set())
    )
    persisted_observation_count = 0
    for instrument in instruments:
        signal = signals_by_instrument.get(instrument) or {}
        news = news_pairs.get(instrument) if isinstance(news_pairs, dict) else {}
        if not isinstance(news, dict):
            news = {}
        signal_side = direction(signal.get("direction_state") or signal.get("direction"))
        news_side = direction(news.get("direction"))
        blockers = list(signal.get("signal_blocked_by") or [])
        bid, ask, mid, pip = quote_values_with_pip(
            signal_snapshot,
            quote_snapshot,
            instrument,
        )
        quote_time_utc, quote_fresh = quote_time_and_freshness(
            signal_snapshot, quote_snapshot, instrument, observed
        )
        row = {
            "observed_utc": observed_utc,
            "instrument": instrument,
            "signal_direction": signal_side,
            "signal_confidence": safe_float(signal.get("signal_confidence"), 0.5),
            "signal_expected_net_pips": safe_float(signal.get("projected_net_pips"), 0.0),
            "signal_eligible": bool(signal.get("signal_eligible")),
            "signal_blockers": blockers,
            "signal_horizon_min": int(
                safe_float(
                    signal.get("preferred_horizon_sec"),
                    safe_float(signal.get("execution_horizon_sec"), 0.0),
                )
                / 60.0
            ),
            "news_direction": news_side,
            "news_score": safe_float(news.get("score"), 0.0),
            "news_confidence": safe_float(news.get("confidence"), 0.0),
            "news_event_count": int(news.get("active_event_count") or 0),
            "relationship": relationship(signal_side, news_side),
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "pip": pip,
            "quote_time_utc": quote_time_utc,
            "quote_fresh": quote_fresh,
            "news_events": news.get("events") or [],
            "news_execution_eligible": bool(news.get("execution_eligible")),
            "news_horizon_min": int(
                safe_float(news.get("estimated_reaction_horizon_sec"), 0.0) / 60.0
            ),
        }
        rows.append(row)
        if not (
            signal_side != "neutral"
            or news_side != "neutral"
            or row["signal_eligible"]
        ):
            continue
        connection.execute(
            """
            INSERT OR REPLACE INTO observations (
                observed_utc, instrument, signal_direction, signal_confidence,
                signal_expected_net_pips, signal_eligible, signal_blockers_json,
                news_direction, news_score, news_confidence, news_event_count,
                relationship, bid, ask, mid, signal_horizon_min,
                news_horizon_min, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observed_utc,
                instrument,
                signal_side,
                row["signal_confidence"],
                row["signal_expected_net_pips"],
                int(row["signal_eligible"]),
                json.dumps(blockers, sort_keys=True),
                news_side,
                row["news_score"],
                row["news_confidence"],
                row["news_event_count"],
                row["relationship"],
                row["bid"],
                row["ask"],
                row["mid"],
                row["signal_horizon_min"],
                row["news_horizon_min"],
                json.dumps(compact_observation_payload(row), sort_keys=True),
            ),
        )
        persisted_observation_count += 1
    matured_outcome_count = mature_outcomes(connection, rows, observed)
    excluded_late_activation_decisions = quarantine_late_activation_decisions(
        connection
    )
    invalidated_reclassified_independent_news_decisions = (
        quarantine_reclassified_independent_news_decisions(
            connection,
            news_database,
            observed,
        )
    )
    new_independent_news_decisions = record_independent_news_decisions(
        connection,
        rows,
        observed,
    )
    matured_independent_news_decisions = mature_independent_news_decisions(
        connection,
        rows,
        observed,
    )
    invalidated_reclassified_news_shadows = (
        quarantine_reclassified_rejected_news_shadows(
            connection,
            news_database,
            observed,
        )
    )
    new_rejected_news_shadows = record_rejected_news_shadows(
        connection,
        rows,
        observed,
    )
    matured_rejected_news_shadows = mature_rejected_news_shadows(
        connection,
        rows,
        observed,
    )
    heartbeat_updated = parse_utc(heartbeat.get("updated_at"))
    heartbeat_age_sec = (
        max(0.0, (observed - heartbeat_updated).total_seconds())
        if heartbeat_updated
        else None
    )
    heartbeat_status = str(
        heartbeat.get("status") or "heartbeat_unavailable"
    )
    heartbeat_details = heartbeat.get("details") or {}
    api_errors = heartbeat_details.get("api_errors")
    last_api_error = heartbeat_details.get("last_api_error") or {}
    last_api_error_time = parse_utc(last_api_error.get("time"))
    last_api_error_age_sec = (
        max(0.0, (observed - last_api_error_time).total_seconds())
        if last_api_error_time
        else None
    )
    recent_api_error = bool(
        last_api_error_age_sec is not None
        and last_api_error_age_sec <= 180.0
    )
    unknown_api_error_age = bool(
        int(api_errors or 0) > 0 and last_api_error_time is None
    )
    cycle = {
        "schema_version": SCHEMA_VERSION,
        "observed_utc": observed_utc,
        "signal_updated_utc": signal_snapshot.get("updated_at"),
        "signal_count": len(top_signals),
        "qualified_count": int(
            signal_snapshot.get(
                "nonconflicting_qualified_signal_count",
                signal_snapshot.get("qualified_signal_count") or 0,
            )
            or 0
        ),
        "selected": signal_snapshot.get("selected"),
        "signal_direction_counts": dict(Counter(row["signal_direction"] for row in rows)),
        "news_direction_counts": dict(Counter(row["news_direction"] for row in rows)),
        "relationship_counts": dict(Counter(row["relationship"] for row in rows)),
        "persisted_observation_count": persisted_observation_count,
        "news_generated_utc": news_snapshot.get("generated_utc"),
        "news_age_seconds": round(news_age_sec, 3) if news_age_sec is not None else None,
        "news_fresh": news_fresh,
        "news_hard_blocked": not news_fresh,
        "active_news_articles": (
            int(news_snapshot.get("active_article_count") or 0)
            if news_fresh
            else 0
        ),
        "collector_status": collector_snapshot.get("status") or "missing",
        "collector_openai_calls": (collector_snapshot.get("policy") or {}).get(
            "openai_calls"
        ),
        "matured_outcome_count": matured_outcome_count,
        "new_independent_news_decisions": new_independent_news_decisions,
        "excluded_late_activation_decisions": excluded_late_activation_decisions,
        "invalidated_reclassified_independent_news_decisions": (
            invalidated_reclassified_independent_news_decisions
        ),
        "matured_independent_news_decisions": matured_independent_news_decisions,
        "new_rejected_news_shadows": new_rejected_news_shadows,
        "invalidated_reclassified_news_shadows": (
            invalidated_reclassified_news_shadows
        ),
        "matured_rejected_news_shadows": matured_rejected_news_shadows,
        "independent_news_rejections": independent_news_rejections,
        "account": {
            "suffix": "-007",
            "environment": account.get("env") or account_snapshot.get("environment"),
            "account_current": account_state["account_current"],
            "account_ok": account_state["account_ok"],
            "snapshot_state": account_state["snapshot_state"],
            "positions_current": account_state["positions_current"],
            "orders_current": account_state["orders_current"],
            "nav": (
                optional_float(account.get("NAV"))
                if account_state["account_current"]
                else None
            ),
            "balance": (
                optional_float(account.get("balance"))
                if account_state["account_current"]
                else None
            ),
            "open_trades": (
                optional_int(account.get("openTradeCount"))
                if account_state["positions_current"]
                else None
            ),
            "pending_orders": (
                optional_int(account.get("pendingOrderCount"))
                if account_state["orders_current"]
                else None
            ),
            "last_verified": account_state["last_verified"],
            "status_code": account.get("status_code"),
            "error": account.get("error"),
        },
        "bot": {
            "status": heartbeat_status,
            "phase": heartbeat.get("phase") or "unknown",
            "updated_at": heartbeat.get("updated_at"),
            "heartbeat_age_sec": heartbeat_age_sec,
            "pid": heartbeat.get("pid"),
            "worker": heartbeat.get("worker"),
            "run_label": heartbeat_details.get("run_label"),
            "api_errors": api_errors,
            "last_api_error": last_api_error or None,
            "last_api_error_age_sec": last_api_error_age_sec,
            "recent_api_error": recent_api_error,
            "healthy": bool(
                heartbeat_status == "running"
                and heartbeat_age_sec is not None
                and heartbeat_age_sec <= 180.0
                and not recent_api_error
                and not unknown_api_error_age
            ),
        },
        "news_policy": {
            "research_only": True,
            "execution_eligible": False,
            "matrix_weight": 0.0,
            "maximum_age_seconds": NEWS_MAX_AGE_SECONDS,
            "fresh": news_fresh,
            "hard_blocked": not news_fresh,
        },
        "top_rows": [compact_live_row(row) for row in rows[:68]],
    }
    cycle_storage = {key: value for key, value in cycle.items() if key != "top_rows"}
    connection.execute(
        """
        INSERT OR REPLACE INTO cycles (
            observed_utc, signal_updated_utc, signal_count, qualified_count,
            selected_json, news_generated_utc, active_news_articles,
            account_nav, account_balance, open_trades, pending_orders,
            bot_status, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            observed_utc,
            str(cycle["signal_updated_utc"] or ""),
            cycle["signal_count"],
            cycle["qualified_count"],
            json.dumps(cycle["selected"], sort_keys=True),
            str(cycle["news_generated_utc"] or ""),
            cycle["active_news_articles"],
            cycle["account"]["nav"],
            cycle["account"]["balance"],
            cycle["account"]["open_trades"],
            cycle["account"]["pending_orders"],
            cycle["bot"]["status"],
            json.dumps(cycle_storage, sort_keys=True),
        ),
    )
    connection.commit()
    return cycle


def build_summary(connection: sqlite3.Connection, stop_utc: dt.datetime) -> dict[str, Any]:
    cycles = connection.execute(
        """
        SELECT observed_utc, signal_count, qualified_count, account_nav,
               open_trades, pending_orders, bot_status, payload_json
        FROM cycles ORDER BY observed_utc
        """
    ).fetchall()
    relationships: Counter[str] = Counter()
    directions: Counter[str] = Counter()
    news_directions: Counter[str] = Counter()
    persisted_observations = 0
    latest_independent_news_rejections: dict[str, Any] = {
        "unique_rejected_topics": 0,
        "reason_counts": {},
        "topics": [],
        "weighting": "no cycle payload available",
    }
    for cycle_row in cycles:
        try:
            payload = json.loads(str(cycle_row[7] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            continue
        relationships.update(payload.get("relationship_counts") or {})
        directions.update(payload.get("signal_direction_counts") or {})
        news_directions.update(payload.get("news_direction_counts") or {})
        persisted_observations += int(
            safe_float(payload.get("persisted_observation_count"), 0.0)
        )
        current_rejections = payload.get("independent_news_rejections")
        if isinstance(current_rejections, dict):
            latest_independent_news_rejections = current_rejections
    qualified_rows = connection.execute(
        "SELECT COUNT(*) FROM observations WHERE signal_eligible = 1"
    ).fetchone()[0]
    outcome_rows = connection.execute(
        """
        SELECT x.horizon_min,
               x.signal_gross_bps, x.signal_executable_bps,
               x.news_gross_bps, x.news_executable_bps,
               x.agreement_gross_bps, x.agreement_executable_bps,
               x.news_veto_gross_bps, x.news_veto_executable_bps,
               x.forecast_utc, x.instrument, x.signal_direction,
               x.news_direction, x.relationship,
               o.signal_horizon_min, o.news_horizon_min
        FROM outcomes AS x
        LEFT JOIN observations AS o
          ON o.observed_utc = x.forecast_utc
         AND o.instrument = x.instrument
        ORDER BY x.horizon_min, x.forecast_utc, x.instrument
        """
    ).fetchall()
    outcome_performance: dict[str, Any] = {}
    strategies = {
        "price_signal": (1, 2, 11),
        "news_only": (3, 4, 12),
        "agreement_only": (5, 6, 11),
        "news_veto": (7, 8, 11),
    }

    def summarize_pairs(pairs: list[tuple[float, float]]) -> dict[str, Any]:
        gross = [pair[0] for pair in pairs]
        net = [pair[1] for pair in pairs]
        return {
            "n": len(pairs),
            "direction_accuracy_pct": round(
                100.0 * sum(value > 0.0 for value in gross) / len(gross), 3
            ),
            "positive_after_spread_pct": round(
                100.0 * sum(value > 0.0 for value in net) / len(net), 3
            ),
            "mean_gross_bps": round(sum(gross) / len(gross), 6),
            "mean_executable_bps": round(sum(net) / len(net), 6),
            "total_executable_bps": round(sum(net), 6),
            "mean_cost_drag_bps": round(
                sum(gross_value - net_value for gross_value, net_value in pairs)
                / len(pairs),
                6,
            ),
        }

    cost_bucket_bounds = {
        "low_0_2bps": (0.0, 2.0),
        "medium_2_5bps": (2.0, 5.0),
        "high_5_15bps": (5.0, 15.0),
        "extreme_over_15bps": (15.0, float("inf")),
    }

    def summarize_with_cost_buckets(
        pairs: list[tuple[float, float]],
    ) -> dict[str, Any]:
        result = summarize_pairs(pairs)
        result["cost_buckets"] = {
            bucket: summarize_pairs(bucket_pairs)
            if bucket_pairs
            else {"n": 0}
            for bucket, (lower, upper) in cost_bucket_bounds.items()
            for bucket_pairs in [
                [
                    pair
                    for pair in pairs
                    if lower <= max(0.0, pair[0] - pair[1]) < upper
                ]
            ]
        }
        return result

    def currency_factor_episode_pairs(
        selected: list[tuple[tuple[Any, ...], float, float]],
        horizon_min: int,
        side_index: int,
    ) -> list[tuple[float, float]]:
        """Collapse simultaneous cross-pair propagation into currency episodes."""

        blocks: dict[int, list[tuple[set[str], float, float]]] = {}
        block_seconds = max(60, int(horizon_min) * 60)
        for row, gross_value, net_value in selected:
            forecast = parse_utc(str(row[9]))
            parts = str(row[10]).split("_")
            side = str(row[side_index])
            if forecast is None or len(parts) != 2 or side not in {"long", "short"}:
                continue
            base_side, quote_side = (
                ("long", "short") if side == "long" else ("short", "long")
            )
            exposures = {f"{parts[0]}:{base_side}", f"{parts[1]}:{quote_side}"}
            block = int(forecast.timestamp()) // block_seconds
            blocks.setdefault(block, []).append((exposures, gross_value, net_value))

        episodes: list[tuple[float, float]] = []
        for rows in blocks.values():
            components: list[dict[str, Any]] = []
            for exposures, gross_value, net_value in rows:
                matches = [
                    component
                    for component in components
                    if component["exposures"].intersection(exposures)
                ]
                if not matches:
                    components.append(
                        {
                            "exposures": set(exposures),
                            "values": [(gross_value, net_value)],
                        }
                    )
                    continue
                target = matches[0]
                target["exposures"].update(exposures)
                target["values"].append((gross_value, net_value))
                for extra in matches[1:]:
                    target["exposures"].update(extra["exposures"])
                    target["values"].extend(extra["values"])
                    components.remove(extra)
            for component in components:
                values = component["values"]
                episodes.append(
                    (
                        sum(value[0] for value in values) / len(values),
                        sum(value[1] for value in values) / len(values),
                    )
                )
        return episodes

    def summarize_currency_factor_episodes(
        selected: list[tuple[tuple[Any, ...], float, float]],
        horizon_min: int,
        side_index: int,
    ) -> dict[str, Any]:
        episodes = currency_factor_episode_pairs(
            selected,
            horizon_min,
            side_index,
        )
        if not episodes:
            return {"n": 0}
        # Bucket only after the correlated legs have been collapsed.  Filtering
        # legs first can split one connected currency factor across multiple
        # buckets, so bucket counts can exceed the total episode count and the
        # same macro event is effectively counted twice.
        return summarize_with_cost_buckets(episodes)

    def preferred_horizon_minutes(row: tuple[Any, ...], name: str) -> int:
        index = 15 if name == "news_only" else 14
        return max(0, int(safe_float(row[index], 0.0)))

    for horizon_min in OUTCOME_HORIZONS_MIN:
        horizon_rows = [row for row in outcome_rows if int(row[0]) == horizon_min]
        horizon_result: dict[str, Any] = {}
        for name, (gross_index, net_index, side_index) in strategies.items():
            pairs = [
                (safe_float(row[gross_index]), safe_float(row[net_index]))
                for row in horizon_rows
                if row[gross_index] is not None and row[net_index] is not None
            ]
            if not pairs:
                horizon_result[name] = {"n": 0}
                continue
            result = summarize_with_cost_buckets(pairs)
            last_selected: dict[tuple[str, str], dt.datetime] = {}
            nonoverlapping_pairs: list[tuple[float, float]] = []
            nonoverlapping_selected: list[
                tuple[tuple[Any, ...], float, float]
            ] = []
            minimum_gap = dt.timedelta(minutes=horizon_min)
            for row in horizon_rows:
                if row[gross_index] is None or row[net_index] is None:
                    continue
                forecast = parse_utc(str(row[9]))
                if forecast is None:
                    continue
                key = (str(row[10]), str(row[side_index]))
                previous = last_selected.get(key)
                if previous is not None and forecast - previous < minimum_gap:
                    continue
                last_selected[key] = forecast
                pair = (safe_float(row[gross_index]), safe_float(row[net_index]))
                nonoverlapping_pairs.append(pair)
                nonoverlapping_selected.append((row, pair[0], pair[1]))
            result["nonoverlapping_per_instrument"] = (
                summarize_with_cost_buckets(nonoverlapping_pairs)
                if nonoverlapping_pairs
                else {"n": 0}
            )
            result["currency_factor_episode_weighted"] = (
                summarize_currency_factor_episodes(
                    nonoverlapping_selected,
                    horizon_min,
                    side_index,
                )
            )
            matched_selected = [
                item
                for item in nonoverlapping_selected
                if preferred_horizon_minutes(item[0], name) == horizon_min
            ]
            result["preferred_horizon_match"] = (
                summarize_currency_factor_episodes(
                    matched_selected,
                    horizon_min,
                    side_index,
                )
                if matched_selected
                else {"n": 0}
            )
            horizon_result[name] = result
        outcome_performance[str(horizon_min)] = horizon_result
    independent_rows = connection.execute(
        """
        SELECT topic_id, horizon_min, instrument, gross_pips,
               executable_pips, result_class
        FROM independent_news_decisions
        WHERE status = 'matured'
        ORDER BY horizon_min, topic_id, instrument
        """
    ).fetchall()
    pending_independent = int(
        connection.execute(
            "SELECT COUNT(*) FROM independent_news_decisions WHERE status = 'pending'"
        ).fetchone()[0]
    )
    independent_status_counts = {
        str(status): int(count)
        for status, count in connection.execute(
            """
            SELECT status, COUNT(*)
            FROM independent_news_decisions
            GROUP BY status
            ORDER BY status
            """
        ).fetchall()
    }
    independent_pair_legs: dict[str, Any] = {}
    independent_episodes: dict[str, Any] = {}
    for horizon_min in sorted({int(row[1]) for row in independent_rows}):
        horizon_rows = [row for row in independent_rows if int(row[1]) == horizon_min]
        gross = [safe_float(row[3]) for row in horizon_rows]
        net = [safe_float(row[4]) for row in horizon_rows]
        classes = Counter(str(row[5]) for row in horizon_rows)
        independent_pair_legs[str(horizon_min)] = {
            "n": len(horizon_rows),
            "direction_accuracy_pct": round(
                100.0 * sum(value > 0.0 for value in gross) / len(gross), 3
            ),
            "positive_after_spread_pct": round(
                100.0 * sum(value > 0.0 for value in net) / len(net), 3
            ),
            "mean_gross_pips": round(sum(gross) / len(gross), 6),
            "mean_executable_pips": round(sum(net) / len(net), 6),
            "result_classes": dict(sorted(classes.items())),
        }
        topics: dict[str, list[tuple[float, float]]] = {}
        for topic_id, _, _, gross_pips, executable_pips, _ in horizon_rows:
            topics.setdefault(str(topic_id), []).append(
                (safe_float(gross_pips), safe_float(executable_pips))
            )
        episode_values = [
            (
                sum(value[0] for value in values) / len(values),
                sum(value[1] for value in values) / len(values),
            )
            for values in topics.values()
        ]
        independent_episodes[str(horizon_min)] = {
            "n": len(episode_values),
            "direction_accuracy_pct": round(
                100.0
                * sum(value[0] > 0.0 for value in episode_values)
                / len(episode_values),
                3,
            ),
            "positive_after_spread_pct": round(
                100.0
                * sum(value[1] > 0.0 for value in episode_values)
                / len(episode_values),
                3,
            ),
            "mean_gross_pips": round(
                sum(value[0] for value in episode_values) / len(episode_values),
                6,
            ),
            "mean_executable_pips": round(
                sum(value[1] for value in episode_values) / len(episode_values),
                6,
            ),
            "weighting": "one vote per topic; pair legs averaged",
        }
    rejected_rows = connection.execute(
        """
        SELECT decision_kind, rejection_reason, topic_id, horizon_min,
               instrument, gross_pips, executable_pips, result_class
        FROM rejected_news_shadows
        WHERE status = 'matured'
        ORDER BY decision_kind, rejection_reason, horizon_min, topic_id, instrument
        """
    ).fetchall()
    rejected_status_counts = {
        str(status): int(count)
        for status, count in connection.execute(
            """
            SELECT status, COUNT(*)
            FROM rejected_news_shadows
            GROUP BY status
            ORDER BY status
            """
        ).fetchall()
    }
    rejected_pending = int(
        connection.execute(
            "SELECT COUNT(*) FROM rejected_news_shadows WHERE status = 'pending'"
        ).fetchone()[0]
    )
    rejected_results: dict[str, Any] = {}
    rejected_groups = sorted(
        {
            (str(row[0]), str(row[1]), int(row[3]))
            for row in rejected_rows
        }
    )
    for decision_kind, rejection_reason, horizon_min in rejected_groups:
        selected_rows = [
            row
            for row in rejected_rows
            if str(row[0]) == decision_kind
            and str(row[1]) == rejection_reason
            and int(row[3]) == horizon_min
        ]
        topics: dict[str, list[tuple[float, float]]] = {}
        for row in selected_rows:
            topics.setdefault(str(row[2]), []).append(
                (safe_float(row[5]), safe_float(row[6]))
            )
        episode_values = [
            (
                sum(value[0] for value in values) / len(values),
                sum(value[1] for value in values) / len(values),
            )
            for values in topics.values()
        ]
        n = len(episode_values)
        result = {
            "n_topic_episodes": n,
            "pair_leg_count": len(selected_rows),
            "direction_accuracy_pct": round(
                100.0 * sum(value[0] > 0.0 for value in episode_values) / n,
                3,
            ),
            "mean_gross_pips": round(
                sum(value[0] for value in episode_values) / n,
                6,
            ),
            "mean_executable_pips": round(
                sum(value[1] for value in episode_values) / n,
                6,
            ),
            "weighting": "one vote per topic; up to three low-spread legs averaged",
        }
        if decision_kind == "prospective_gate_counterfactual":
            result["missed_after_cost_pct"] = round(
                100.0 * sum(value[1] > 0.0 for value in episode_values) / n,
                3,
            )
            result["correct_no_trade_pct"] = round(
                100.0 * sum(value[1] <= 0.0 for value in episode_values) / n,
                3,
            )
        else:
            result["post_recap_continuation_after_cost_pct"] = round(
                100.0 * sum(value[1] > 0.0 for value in episode_values) / n,
                3,
            )
        rejected_results.setdefault(decision_kind, {}).setdefault(
            rejection_reason, {}
        )[str(horizon_min)] = result
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": iso_utc(),
        "target_stop_utc": iso_utc(stop_utc),
        "cycle_count": len(cycles),
        "first_cycle_utc": cycles[0][0] if cycles else None,
        "last_cycle_utc": cycles[-1][0] if cycles else None,
        "maximum_signal_count": max((row[1] for row in cycles), default=0),
        "maximum_qualified_count": max((row[2] for row in cycles), default=0),
        "qualified_observation_count": qualified_rows,
        "persisted_observation_count": persisted_observations,
        "signal_direction_counts": dict(directions),
        "news_direction_counts": dict(news_directions),
        "relationship_counts": dict(relationships),
        "maximum_open_trades": max(
            (row[4] for row in cycles if row[4] is not None), default=None
        ),
        "maximum_pending_orders": max(
            (row[5] for row in cycles if row[5] is not None), default=None
        ),
        "bot_status_counts": dict(Counter(row[6] for row in cycles)),
        "starting_nav": cycles[0][3] if cycles else None,
        "ending_nav": cycles[-1][3] if cycles else None,
        "matured_outcome_count": len(outcome_rows),
        "outcome_horizons_min": list(OUTCOME_HORIZONS_MIN),
        "outcome_performance": outcome_performance,
        "independent_news": {
            "pending_decisions": pending_independent,
            "matured_pair_legs": len(independent_rows),
            "status_counts": independent_status_counts,
            "pair_leg_results": independent_pair_legs,
            "episode_weighted_results": independent_episodes,
            "decision_scope": "shadow_only_no_execution_adapter",
            "current_no_trade_audit": latest_independent_news_rejections,
            "rejected_shadow_audit": {
                "pending_pair_legs": rejected_pending,
                "matured_pair_legs": len(rejected_rows),
                "status_counts": rejected_status_counts,
                "results": rejected_results,
                "scope": (
                    "shadow-only latent-direction counterfactual; retrospective "
                    "recaps measure continuation and are never prospective misses"
                ),
                "pair_leg_cap_per_topic": REJECTED_NEWS_MAX_PAIR_LEGS,
                "max_entry_spread_pips": REJECTED_NEWS_MAX_SPREAD_PIPS,
            },
        },
        "outcome_note": (
            "Raw results are overlapping prospective diagnostics sampled each cycle. "
            "Generation v5 stores only directional or eligible observations, keeps "
            "full all-pair counts at cycle level, and references canonical news by "
            "topic ID. Retained v1-v3 databases are excluded from current prospective "
            "accuracy because they predate one or more causal, horizon, or storage fixes. "
            "outcome_performance is a fixed-horizon what-if ablation; "
            "preferred_horizon_match is the forecast-aligned accuracy view. "
            "nonoverlapping_per_instrument retains at most one observation per "
            "instrument, direction, and stated horizon. "
            "Executable values use entry ask/exit bid for long and entry "
            "bid/exit ask for short; no slippage or account sizing is applied."
        ),
        "news_policy": {
            "research_only": True,
            "execution_eligible": False,
            "openai_calls": 0,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signal", type=Path, default=DEFAULT_SIGNAL)
    parser.add_argument("--account", type=Path, default=DEFAULT_ACCOUNT)
    parser.add_argument("--heartbeat", type=Path, default=DEFAULT_HEARTBEAT)
    parser.add_argument("--news", type=Path, default=DEFAULT_NEWS)
    parser.add_argument("--collector", type=Path, default=DEFAULT_COLLECTOR)
    parser.add_argument(
        "--news-database",
        type=Path,
        default=DEFAULT_NEWS_DATABASE,
    )
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "practice_007_signal_news_monitor",
    )
    parser.add_argument("--stop-local")
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument(
        "--summary-interval-sec",
        type=float,
        default=0.0,
        help=(
            "Optional all-history SUMMARY.json refresh interval. The live "
            "LATEST.json and immutable SQLite evidence still update every "
            "cycle; zero keeps the expensive all-history rebuild on-demand."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stop_utc = (
        utc_now() + dt.timedelta(seconds=max(1.0, args.duration_sec))
        if args.duration_sec > 0.0
        else parse_stop(args.stop_local)
    )
    args.output_root.mkdir(parents=True, exist_ok=True)
    # v1 is intentionally retained as an audit artifact: it contains cycles
    # where continuation-context news could be scored as though newly active.
    # Start a clean prospective generation after causal-horizon gating rather
    # than rewriting or deleting that historical evidence.
    # v2 and v3 are also retained: they are brief audit generations before
    # neutral rows, unmatched horizons, and full-payload storage were separated.
    database_path = args.output_root / "signal_news_monitor_v4.sqlite"
    connection = open_database(database_path)
    last_summary_monotonic: float | None = None
    try:
        while utc_now() < stop_utc:
            observed = utc_now()
            cycle = record_cycle(
                connection,
                signal_snapshot=load_json(args.signal),
                account_snapshot=load_json(args.account),
                heartbeat=load_json(args.heartbeat),
                news_snapshot=load_json(args.news),
                collector_snapshot=load_json(args.collector),
                observed=observed,
                quote_snapshot=load_json(args.quotes),
                news_database=args.news_database,
            )
            atomic_write_json(args.output_root / "LATEST.json", cycle)
            if args.summary_interval_sec > 0.0 and (
                last_summary_monotonic is None
                or time.monotonic() - last_summary_monotonic
                >= args.summary_interval_sec
            ):
                atomic_write_json(
                    args.output_root / "SUMMARY.json",
                    build_summary(connection, stop_utc),
                )
                last_summary_monotonic = time.monotonic()
            print(
                json.dumps(
                    {
                        "observed_utc": cycle["observed_utc"],
                        "signal_count": cycle["signal_count"],
                        "qualified_count": cycle["qualified_count"],
                        "signal_direction_counts": cycle["signal_direction_counts"],
                        "news_direction_counts": cycle["news_direction_counts"],
                        "relationship_counts": cycle["relationship_counts"],
                        "active_news_articles": cycle["active_news_articles"],
                        "open_trades": cycle["account"]["open_trades"],
                        "pending_orders": cycle["account"]["pending_orders"],
                        "bot_status": cycle["bot"]["status"],
                        "bot_healthy": cycle["bot"]["healthy"],
                        "matured_outcome_count": cycle["matured_outcome_count"],
                        "new_independent_news_decisions": cycle[
                            "new_independent_news_decisions"
                        ],
                        "invalidated_reclassified_independent_news_decisions": cycle[
                            "invalidated_reclassified_independent_news_decisions"
                        ],
                        "matured_independent_news_decisions": cycle[
                            "matured_independent_news_decisions"
                        ],
                        "new_rejected_news_shadows": cycle[
                            "new_rejected_news_shadows"
                        ],
                        "invalidated_reclassified_news_shadows": cycle[
                            "invalidated_reclassified_news_shadows"
                        ],
                        "matured_rejected_news_shadows": cycle[
                            "matured_rejected_news_shadows"
                        ],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            time.sleep(max(5.0, args.interval_sec))
        atomic_write_json(
            args.output_root / "FINAL_SUMMARY.json",
            build_summary(connection, stop_utc),
        )
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
