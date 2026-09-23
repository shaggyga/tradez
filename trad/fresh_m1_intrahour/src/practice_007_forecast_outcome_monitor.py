from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import statistics
import time
from bisect import bisect_left
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


INTRAHOUR_HORIZONS = (60, 120, 180, 300, 600, 900, 1800, 3600)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def read_json(path: Path) -> dict[str, Any]:
    for attempt in range(3):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, json.JSONDecodeError):
            if attempt < 2:
                time.sleep(0.2)
    return {}


def read_json_cached(
    path: Path,
    cache: dict[Path, tuple[tuple[int, int], dict[str, Any]]],
) -> dict[str, Any]:
    """Reuse an atomically published snapshot until its file identity changes."""

    try:
        stat = path.stat()
    except OSError:
        return {}
    identity = (stat.st_mtime_ns, stat.st_size)
    cached = cache.get(path)
    if cached is not None and cached[0] == identity:
        return cached[1]
    payload = read_json(path)
    if payload:
        cache[path] = (identity, payload)
    return payload


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def executable_net_pips(
    direction: str,
    entry_bid: float,
    entry_ask: float,
    exit_bid: float,
    exit_ask: float,
    pip: float,
) -> float:
    if direction == "buy":
        return (exit_bid - entry_ask) / pip
    if direction == "sell":
        return (entry_bid - exit_ask) / pip
    raise ValueError(f"Unsupported direction: {direction}")


def signed_mid_move_pips(
    direction: str,
    entry_bid: float,
    entry_ask: float,
    exit_bid: float,
    exit_ask: float,
    pip: float,
) -> float:
    entry_mid = (entry_bid + entry_ask) / 2.0
    exit_mid = (exit_bid + exit_ask) / 2.0
    unsigned = (exit_mid - entry_mid) / pip
    return unsigned if direction == "buy" else -unsigned


def stable_generation(
    signal_payload: dict[str, Any],
    minimum_raw_candidates: int,
    minimum_consolidated_signals: int,
) -> bool:
    return (
        int(signal_payload.get("raw_candidate_count") or 0)
        >= minimum_raw_candidates
        and int(signal_payload.get("consolidated_signal_count") or 0)
        >= minimum_consolidated_signals
    )


def projected_gross_pips(
    explicit_gross: Any,
    gross_to_spread: Any,
    entry_spread_pips: float,
) -> float | None:
    explicit = finite_float(explicit_gross)
    if explicit is not None:
        return explicit
    ratio = finite_float(gross_to_spread)
    if ratio is None or ratio < 0.0 or entry_spread_pips < 0.0:
        return None
    return ratio * entry_spread_pips


def connect_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS monitor_samples (
            captured_epoch REAL PRIMARY KEY,
            captured_utc TEXT NOT NULL,
            signal_snapshot_utc TEXT,
            feature_snapshot_utc TEXT,
            signal_age_sec REAL,
            feature_age_sec REAL,
            raw_candidate_count INTEGER NOT NULL,
            consolidated_signal_count INTEGER NOT NULL,
            qualified_signal_count INTEGER NOT NULL,
            stable_generation INTEGER NOT NULL,
            account_balance REAL,
            account_nav REAL,
            account_pl REAL,
            account_unrealized_pl REAL,
            open_trade_count INTEGER,
            pending_order_count INTEGER
        );

        CREATE TABLE IF NOT EXISTS quote_samples (
            quote_epoch REAL NOT NULL,
            captured_epoch REAL NOT NULL,
            instrument TEXT NOT NULL,
            quote_utc TEXT,
            bid REAL NOT NULL,
            ask REAL NOT NULL,
            pip REAL NOT NULL,
            PRIMARY KEY (quote_epoch, instrument)
        );
        CREATE INDEX IF NOT EXISTS idx_quote_instrument_time
            ON quote_samples (instrument, quote_epoch);
        CREATE INDEX IF NOT EXISTS idx_quote_instrument_capture
            ON quote_samples (instrument, captured_epoch);

        CREATE TABLE IF NOT EXISTS technical_samples (
            feature_snapshot_utc TEXT NOT NULL,
            captured_epoch REAL NOT NULL,
            instrument TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            candle_utc TEXT,
            last REAL,
            r1_pips REAL,
            r3_pips REAL,
            r5_pips REAL,
            m1_atr14_pips REAL,
            m5_atr14_pips REAL,
            pos20 REAL,
            cross_strength_r1 REAL,
            cross_strength_r3 REAL,
            cross_strength_r5 REAL,
            cross_breadth_r1 REAL,
            cross_breadth_r3 REAL,
            pair_norm_r3 REAL,
            pair_norm_r5 REAL,
            pair_rank_r3 REAL,
            pair_rank_r5 REAL,
            relative_residual_r3 REAL,
            relative_residual_r5 REAL,
            current_volume REAL,
            volume_ratio_12 REAL,
            volume_ratio_30 REAL,
            median_spread_12_pips REAL,
            current_candle_spread_pips REAL,
            live_spread_pips REAL,
            microprice_offset_pips REAL,
            depth_imbalance REAL,
            order_book_available INTEGER,
            position_book_available INTEGER,
            PRIMARY KEY (
                feature_snapshot_utc, instrument, timeframe
            )
        );
        CREATE INDEX IF NOT EXISTS idx_technical_instrument_time
            ON technical_samples (instrument, timeframe, captured_epoch);

        CREATE TABLE IF NOT EXISTS signal_rows (
            signal_row_id TEXT PRIMARY KEY,
            captured_epoch REAL NOT NULL,
            signal_snapshot_utc TEXT,
            feature_snapshot_utc TEXT,
            instrument TEXT NOT NULL,
            signal_rank INTEGER NOT NULL,
            direction TEXT NOT NULL,
            preferred_horizon_sec INTEGER,
            signal_confidence REAL,
            projected_net_pips REAL,
            instant_projected_net_pips REAL,
            historical_expected_net_pips REAL,
            historical_reliability REAL,
            direction_conflict INTEGER NOT NULL,
            signal_eligible INTEGER NOT NULL,
            blockers_json TEXT NOT NULL,
            selected INTEGER NOT NULL,
            stable_generation INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_signal_rows_time
            ON signal_rows (stable_generation, captured_epoch, instrument);
        CREATE INDEX IF NOT EXISTS idx_signal_rows_capture_join
            ON signal_rows (
                captured_epoch,
                instrument,
                preferred_horizon_sec,
                signal_rank
            );
        CREATE INDEX IF NOT EXISTS idx_signal_rows_forecast_join
            ON signal_rows (
                signal_snapshot_utc,
                feature_snapshot_utc,
                instrument,
                preferred_horizon_sec,
                signal_rank
            );

        CREATE TABLE IF NOT EXISTS account_positions (
            captured_epoch REAL NOT NULL,
            instrument TEXT NOT NULL,
            trade_id TEXT NOT NULL,
            current_units REAL,
            entry_price REAL,
            unrealized_pl REAL,
            open_time_utc TEXT,
            PRIMARY KEY (captured_epoch, instrument, trade_id)
        );

        CREATE TABLE IF NOT EXISTS coverage_samples (
            captured_epoch REAL PRIMARY KEY,
            feed_candidate_count INTEGER NOT NULL,
            fresh_contributors INTEGER NOT NULL,
            registered_contributors INTEGER NOT NULL,
            expected_contributors INTEGER NOT NULL,
            account_eligible_contributors INTEGER NOT NULL,
            contributor_coverage_ratio REAL,
            active_sources_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS forecasts (
            observation_id TEXT PRIMARY KEY,
            captured_epoch REAL NOT NULL,
            captured_utc TEXT NOT NULL,
            signal_snapshot_utc TEXT,
            feature_snapshot_utc TEXT,
            quote_utc TEXT,
            entry_quote_epoch REAL NOT NULL,
            quote_age_sec REAL NOT NULL,
            due_epoch REAL NOT NULL,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            pip REAL NOT NULL,
            entry_bid REAL NOT NULL,
            entry_ask REAL NOT NULL,
            entry_spread_pips REAL NOT NULL,
            signal_confidence REAL,
            raw_probability_up REAL,
            filtered_probability_up REAL,
            execution_probability_up REAL,
            projected_gross_movement_pips REAL,
            projected_net_pips REAL,
            gross_to_spread REAL,
            aligned_weight_pct REAL,
            family_count INTEGER,
            eligible_component_count INTEGER,
            signal_eligible INTEGER NOT NULL,
            direction_conflict INTEGER NOT NULL,
            blockers_json TEXT NOT NULL,
            best_family TEXT,
            best_model_id TEXT,
            raw_candidate_count INTEGER NOT NULL,
            qualified_signal_count INTEGER NOT NULL,
            stable_generation INTEGER NOT NULL,
            matured_epoch REAL,
            matured_utc TEXT,
            exit_quote_utc TEXT,
            exit_bid REAL,
            exit_ask REAL,
            exit_delay_sec REAL,
            signed_mid_move_pips REAL,
            executable_net_pips REAL,
            mfe_pips REAL,
            mae_pips REAL,
            direction_correct INTEGER,
            net_positive INTEGER,
            magnitude_abs_error_pips REAL,
            censored_epoch REAL,
            censor_reason TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_forecasts_pending
            ON forecasts (matured_epoch, due_epoch);
        CREATE INDEX IF NOT EXISTS idx_forecasts_group
            ON forecasts (stable_generation, horizon_sec, instrument);
        CREATE INDEX IF NOT EXISTS idx_forecasts_capture_join
            ON forecasts (
                captured_epoch,
                instrument,
                horizon_sec,
                matured_epoch
            );
        CREATE INDEX IF NOT EXISTS idx_forecasts_signal_join
            ON forecasts (
                signal_snapshot_utc,
                feature_snapshot_utc,
                instrument,
                horizon_sec,
                matured_epoch
            );
        """
    )
    forecast_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(forecasts)").fetchall()
    }
    if "censored_epoch" not in forecast_columns:
        connection.execute(
            "ALTER TABLE forecasts ADD COLUMN censored_epoch REAL"
        )
    if "censor_reason" not in forecast_columns:
        connection.execute(
            "ALTER TABLE forecasts ADD COLUMN censor_reason TEXT"
        )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_forecasts_active_pending
        ON forecasts (matured_epoch, censored_epoch, due_epoch)
        """
    )
    connection.execute(
        """
        UPDATE forecasts
        SET projected_gross_movement_pips =
            gross_to_spread * entry_spread_pips
        WHERE projected_gross_movement_pips IS NULL
          AND gross_to_spread IS NOT NULL
          AND gross_to_spread >= 0
          AND entry_spread_pips >= 0
        """
    )
    connection.commit()
    return connection


def account_row(payload: dict[str, Any]) -> dict[str, Any]:
    accounts = payload.get("accounts") or []
    account = next(
        (
            row
            for row in accounts
            if str(row.get("account_id") or "").endswith("-007")
        ),
        accounts[0] if accounts else {},
    )
    return {
        "balance": finite_float(account.get("balance")),
        "nav": finite_float(account.get("NAV")),
        "pl": finite_float(account.get("pl")),
        "unrealized_pl": finite_float(account.get("unrealizedPL")),
        "open_trade_count": int(account.get("openTradeCount") or 0),
        "pending_order_count": int(account.get("pendingOrderCount") or 0),
        "trades": list(account.get("trades") or []),
    }


def extract_quotes(
    feature_payload: dict[str, Any],
    captured_epoch: float,
    maximum_quote_age_sec: float,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for instrument, row in (feature_payload.get("instruments") or {}).items():
        quote = row.get("quote") or {}
        features = row.get("features") or {}
        bid = finite_float(quote.get("bid"))
        ask = finite_float(quote.get("ask"))
        pip = finite_float(quote.get("pip"))
        if pip is None:
            pip = finite_float(features.get("pip"))
        quote_dt = parse_timestamp(quote.get("time"))
        if (
            bid is None
            or ask is None
            or pip is None
            or pip <= 0
            or ask < bid
            or quote_dt is None
        ):
            continue
        quote_epoch = quote_dt.timestamp()
        quote_age_sec = captured_epoch - quote_epoch
        if quote_age_sec < -5.0 or quote_age_sec > maximum_quote_age_sec:
            continue
        result[str(instrument)] = {
            "bid": bid,
            "ask": ask,
            "pip": pip,
            "quote_utc": quote.get("time"),
            "quote_epoch": quote_epoch,
            "quote_age_sec": quote_age_sec,
        }
    return result


def extract_signal_quotes(
    signal_payload: dict[str, Any],
    feature_payload: dict[str, Any],
    captured_epoch: float,
    maximum_quote_age_sec: float,
) -> dict[str, dict[str, Any]]:
    feature_instruments = feature_payload.get("instruments") or {}
    result: dict[str, dict[str, Any]] = {}
    for instrument, quote in (signal_payload.get("market_quotes") or {}).items():
        bid = finite_float(quote.get("bid"))
        ask = finite_float(quote.get("ask"))
        quote_dt = parse_timestamp(quote.get("time"))
        features = (feature_instruments.get(instrument) or {}).get("features") or {}
        pip = finite_float(features.get("pip"))
        if pip is None:
            pip = 0.01 if str(instrument).endswith("_JPY") else 0.0001
        if (
            bid is None
            or ask is None
            or quote_dt is None
            or pip <= 0
            or ask < bid
        ):
            continue
        quote_epoch = quote_dt.timestamp()
        quote_age_sec = captured_epoch - quote_epoch
        if quote_age_sec < -5.0 or quote_age_sec > maximum_quote_age_sec:
            continue
        result[str(instrument)] = {
            "bid": bid,
            "ask": ask,
            "pip": pip,
            "quote_utc": quote.get("time"),
            "quote_epoch": quote_epoch,
            "quote_age_sec": quote_age_sec,
        }
    return result


def blocker_union(signal: dict[str, Any], horizon: dict[str, Any]) -> list[str]:
    return sorted(
        {
            str(reason)
            for reason in (
                list(signal.get("signal_blocked_by") or [])
                + list(horizon.get("signal_blocked_by") or [])
            )
            if str(reason)
        }
    )


def technical_rows(
    feature_payload: dict[str, Any],
    captured_epoch: float,
) -> list[tuple[Any, ...]]:
    feature_snapshot_utc = str(feature_payload.get("generated_utc") or "")
    if not feature_snapshot_utc:
        return []
    rows: list[tuple[Any, ...]] = []
    for instrument, instrument_row in (
        feature_payload.get("instruments") or {}
    ).items():
        root = instrument_row.get("features") or {}
        timeframes = instrument_row.get("timeframe_features") or {}
        for timeframe in ("M1", "M5", "M15", "M30", "H1", "H4"):
            values = timeframes.get(timeframe) or {}
            if not values:
                continue
            rows.append(
                (
                    feature_snapshot_utc,
                    captured_epoch,
                    str(instrument),
                    timeframe,
                    values.get("candle_time"),
                    finite_float(values.get("last")),
                    finite_float(values.get("r1_pips")),
                    finite_float(values.get("r3_pips")),
                    finite_float(values.get("r5_pips")),
                    finite_float(values.get("m1_atr14_pips")),
                    finite_float(values.get("m5_atr14_pips")),
                    finite_float(values.get("pos20")),
                    finite_float(values.get("cross_strength_r1")),
                    finite_float(values.get("cross_strength_r3")),
                    finite_float(values.get("cross_strength_r5")),
                    finite_float(values.get("cross_breadth_r1")),
                    finite_float(values.get("cross_breadth_r3")),
                    finite_float(values.get("pair_norm_r3")),
                    finite_float(values.get("pair_norm_r5")),
                    finite_float(values.get("pair_rank_r3")),
                    finite_float(values.get("pair_rank_r5")),
                    finite_float(values.get("relative_residual_r3")),
                    finite_float(values.get("relative_residual_r5")),
                    finite_float(values.get("current_volume")),
                    finite_float(values.get("volume_ratio_12")),
                    finite_float(values.get("volume_ratio_30")),
                    finite_float(values.get("median_spread_12_pips")),
                    finite_float(values.get("current_candle_spread_pips")),
                    finite_float(root.get("live_spread_pips")),
                    finite_float(root.get("microprice_offset_pips")),
                    finite_float(root.get("depth_imbalance")),
                    int(bool(root.get("order_book_available"))),
                    int(bool(root.get("position_book_available"))),
                )
            )
    return rows


def record_capture(
    connection: sqlite3.Connection,
    signal_payload: dict[str, Any],
    feature_payload: dict[str, Any],
    market_quote_payload: dict[str, Any],
    account_payload: dict[str, Any],
    captured_epoch: float,
    minimum_raw_candidates: int,
    minimum_consolidated_signals: int,
    maximum_quote_age_sec: float,
) -> dict[str, int | float | str | None]:
    captured = datetime.fromtimestamp(captured_epoch, tz=timezone.utc)
    captured_utc = captured.isoformat()
    signal_utc = signal_payload.get("updated_at")
    feature_utc = feature_payload.get("generated_utc")
    signal_dt = parse_timestamp(signal_utc)
    feature_dt = parse_timestamp(feature_utc)
    is_stable = stable_generation(
        signal_payload,
        minimum_raw_candidates,
        minimum_consolidated_signals,
    )
    account = account_row(account_payload)
    feed = signal_payload.get("contribution_feed") or {}
    raw_count = int(signal_payload.get("raw_candidate_count") or 0)
    consolidated_count = int(signal_payload.get("consolidated_signal_count") or 0)
    qualified_count = int(signal_payload.get("qualified_signal_count") or 0)
    quotes = extract_quotes(
        feature_payload, captured_epoch, maximum_quote_age_sec
    )
    for instrument, quote in extract_signal_quotes(
        signal_payload,
        feature_payload,
        captured_epoch,
        maximum_quote_age_sec,
    ).items():
        existing = quotes.get(instrument)
        if (
            existing is None
            or quote["quote_epoch"] > existing["quote_epoch"]
        ):
            quotes[instrument] = quote
    for instrument, quote in extract_signal_quotes(
        {"market_quotes": market_quote_payload.get("quotes") or {}},
        feature_payload,
        captured_epoch,
        maximum_quote_age_sec,
    ).items():
        existing = quotes.get(instrument)
        if (
            existing is None
            or quote["quote_epoch"] > existing["quote_epoch"]
        ):
            quotes[instrument] = quote

    connection.execute(
        """
        INSERT OR IGNORE INTO monitor_samples VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            captured_epoch,
            captured_utc,
            signal_utc,
            feature_utc,
            None if signal_dt is None else (captured - signal_dt).total_seconds(),
            None if feature_dt is None else (captured - feature_dt).total_seconds(),
            raw_count,
            consolidated_count,
            qualified_count,
            int(is_stable),
            account["balance"],
            account["nav"],
            account["pl"],
            account["unrealized_pl"],
            account["open_trade_count"],
            account["pending_order_count"],
        ),
    )
    fresh_contributors = int(feed.get("fresh_contributors") or 0)
    expected_contributors = int(feed.get("expected_contributors") or 0)
    contributor_coverage_ratio = (
        fresh_contributors / expected_contributors
        if expected_contributors > 0
        else None
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO coverage_samples VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            captured_epoch,
            int(signal_payload.get("feed_candidate_count") or 0),
            fresh_contributors,
            int(feed.get("registered_contributors") or 0),
            expected_contributors,
            int(feed.get("account_eligible_contributors") or 0),
            contributor_coverage_ratio,
            json.dumps(
                feed.get("active_feed_sources") or [],
                separators=(",", ":"),
            ),
        ),
    )
    connection.executemany(
        """
        INSERT OR IGNORE INTO quote_samples
        (quote_epoch, captured_epoch, instrument, quote_utc, bid, ask, pip)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                quote["quote_epoch"],
                captured_epoch,
                instrument,
                quote["quote_utc"],
                quote["bid"],
                quote["ask"],
                quote["pip"],
            )
            for instrument, quote in quotes.items()
        ],
    )
    technical = technical_rows(feature_payload, captured_epoch)
    connection.executemany(
        """
        INSERT OR IGNORE INTO technical_samples VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        technical,
    )
    selected_id = str((signal_payload.get("selected") or {}).get("id") or "")
    signal_rows = []
    for rank, signal in enumerate(
        signal_payload.get("top_signals") or [], start=1
    ):
        instrument = str(signal.get("instrument") or "")
        fingerprint = "|".join(
            (str(signal_utc), str(feature_utc), instrument)
        )
        signal_row_id = hashlib.sha256(
            fingerprint.encode("utf-8")
        ).hexdigest()[:24]
        preferred_sec = int(signal.get("preferred_horizon_sec") or 0)
        preferred = next(
            (
                row
                for row in signal.get("horizon_breakdown") or []
                if int(row.get("horizon_sec") or 0) == preferred_sec
            ),
            {},
        )
        blockers = blocker_union(signal, preferred)
        signal_rows.append(
            (
                signal_row_id,
                captured_epoch,
                signal_utc,
                feature_utc,
                instrument,
                rank,
                str(signal.get("direction") or ""),
                preferred_sec or None,
                finite_float(signal.get("signal_confidence")),
                finite_float(signal.get("projected_net_pips")),
                finite_float(signal.get("instant_projected_net_pips")),
                finite_float(signal.get("historical_expected_net_pips")),
                finite_float(signal.get("historical_reliability")),
                int(bool(signal.get("direction_conflict"))),
                int(bool(preferred.get("signal_eligible"))),
                json.dumps(blockers, separators=(",", ":")),
                int(bool(selected_id and str(signal.get("id") or "") == selected_id)),
                int(is_stable),
            )
        )
    connection.executemany(
        """
        INSERT OR IGNORE INTO signal_rows VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        signal_rows,
    )
    positions = []
    for trade in account["trades"]:
        instrument = str(trade.get("instrument") or "")
        trade_id = str(trade.get("id") or trade.get("trade_id") or "")
        if not instrument:
            continue
        positions.append(
            (
                captured_epoch,
                instrument,
                trade_id,
                finite_float(trade.get("currentUnits")),
                finite_float(trade.get("price")),
                finite_float(trade.get("unrealizedPL")),
                trade.get("openTime"),
            )
        )
    connection.executemany(
        """
        INSERT OR IGNORE INTO account_positions VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        positions,
    )

    inserted = 0
    missing_quote = 0
    skipped_horizon = 0
    for signal in signal_payload.get("top_signals") or []:
        instrument = str(signal.get("instrument") or "")
        quote = quotes.get(instrument)
        if quote is None:
            missing_quote += 1
            continue
        direction_conflict = bool(signal.get("direction_conflict"))
        for horizon in signal.get("horizon_breakdown") or []:
            horizon_sec = int(horizon.get("horizon_sec") or 0)
            if horizon_sec not in INTRAHOUR_HORIZONS:
                skipped_horizon += 1
                continue
            direction = str(
                horizon.get("direction") or signal.get("direction") or ""
            ).lower()
            if direction not in {"buy", "sell"}:
                continue
            fingerprint = "|".join(
                (
                    str(signal_utc),
                    str(feature_utc),
                    instrument,
                    str(horizon_sec),
                    direction,
                )
            )
            observation_id = hashlib.sha256(
                fingerprint.encode("utf-8")
            ).hexdigest()[:24]
            blockers = blocker_union(signal, horizon)
            entry_spread_pips = (
                quote["ask"] - quote["bid"]
            ) / quote["pip"]
            gross_to_spread = finite_float(
                horizon.get("gross_to_spread")
            )
            values = (
                observation_id,
                captured_epoch,
                captured_utc,
                signal_utc,
                feature_utc,
                quote["quote_utc"],
                quote["quote_epoch"],
                quote["quote_age_sec"],
                quote["quote_epoch"] + horizon_sec,
                instrument,
                direction,
                horizon_sec,
                quote["pip"],
                quote["bid"],
                quote["ask"],
                entry_spread_pips,
                finite_float(horizon.get("signal_confidence"))
                or finite_float(signal.get("signal_confidence")),
                finite_float(horizon.get("raw_probability_up")),
                finite_float(horizon.get("filtered_probability_up")),
                finite_float(horizon.get("execution_probability_up")),
                projected_gross_pips(
                    horizon.get("projected_gross_movement_pips"),
                    gross_to_spread,
                    entry_spread_pips,
                ),
                finite_float(horizon.get("projected_net_pips")),
                gross_to_spread,
                finite_float(horizon.get("ensemble_aligned_weight_pct")),
                int(horizon.get("family_count") or 0),
                int(horizon.get("eligible_component_count") or 0),
                int(bool(horizon.get("signal_eligible"))),
                int(direction_conflict),
                json.dumps(blockers, separators=(",", ":")),
                horizon.get("best_family"),
                horizon.get("best_model_id"),
                raw_count,
                qualified_count,
                int(is_stable),
            )
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO forecasts (
                    observation_id, captured_epoch, captured_utc,
                    signal_snapshot_utc, feature_snapshot_utc, quote_utc,
                    entry_quote_epoch, quote_age_sec, due_epoch, instrument,
                    direction, horizon_sec, pip,
                    entry_bid, entry_ask, entry_spread_pips,
                    signal_confidence, raw_probability_up,
                    filtered_probability_up, execution_probability_up,
                    projected_gross_movement_pips, projected_net_pips,
                    gross_to_spread, aligned_weight_pct, family_count,
                    eligible_component_count, signal_eligible,
                    direction_conflict, blockers_json, best_family,
                    best_model_id, raw_candidate_count,
                    qualified_signal_count, stable_generation
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                values,
            )
            inserted += int(cursor.rowcount > 0)
    connection.commit()
    return {
        "captured_utc": captured_utc,
        "raw_candidate_count": raw_count,
        "consolidated_signal_count": consolidated_count,
        "qualified_signal_count": qualified_count,
        "feed_candidate_count": int(
            signal_payload.get("feed_candidate_count") or 0
        ),
        "fresh_contributors": fresh_contributors,
        "expected_contributors": expected_contributors,
        "contributor_coverage_ratio": contributor_coverage_ratio,
        "stable_generation": is_stable,
        "quotes": len(quotes),
        "technical_rows_inserted": len(technical),
        "signal_rows_inserted": len(signal_rows),
        "account_positions_logged": len(positions),
        "forecasts_inserted": inserted,
        "signals_missing_quote": missing_quote,
        "long_horizons_skipped": skipped_horizon,
    }


def mature_forecasts(
    connection: sqlite3.Connection,
    now_epoch: float,
    maximum_exit_delay_sec: float = 180.0,
    maximum_forecasts: int = 4096,
) -> int:
    pending = connection.execute(
        """
        SELECT * FROM forecasts
        WHERE matured_epoch IS NULL
          AND censored_epoch IS NULL
          AND due_epoch <= ?
        ORDER BY due_epoch
        LIMIT ?
        """,
        (now_epoch, max(1, int(maximum_forecasts))),
    ).fetchall()
    if not pending:
        return 0
    quote_rows = connection.execute(
        """
        SELECT quote_epoch, quote_utc, instrument, bid, ask
        FROM quote_samples
        ORDER BY instrument, quote_epoch
        """
    ).fetchall()
    quotes: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for quote in quote_rows:
        quotes[str(quote["instrument"])].append(quote)
    quote_times = {
        instrument: [float(row["quote_epoch"]) for row in rows]
        for instrument, rows in quotes.items()
    }
    matured = 0
    updates: list[tuple[Any, ...]] = []
    censored: list[tuple[float, str, str]] = []
    for forecast in pending:
        instrument = str(forecast["instrument"])
        instrument_quotes = quotes.get(instrument, [])
        times = quote_times.get(instrument, [])
        exit_index = bisect_left(times, float(forecast["due_epoch"]))
        if exit_index >= len(instrument_quotes):
            if now_epoch > forecast["due_epoch"] + maximum_exit_delay_sec:
                censored.append(
                    (
                        now_epoch,
                        "no_exit_quote_within_delay",
                        forecast["observation_id"],
                    )
                )
            continue
        exit_quote = instrument_quotes[exit_index]
        exit_delay = exit_quote["quote_epoch"] - forecast["due_epoch"]
        if exit_delay > maximum_exit_delay_sec:
            censored.append(
                (
                    now_epoch,
                    "exit_quote_delay_exceeded",
                    forecast["observation_id"],
                )
            )
            continue
        entry_index = bisect_left(
            times,
            float(forecast["entry_quote_epoch"]),
        )
        path = instrument_quotes[entry_index : exit_index + 1]
        net_path = [
            executable_net_pips(
                forecast["direction"],
                forecast["entry_bid"],
                forecast["entry_ask"],
                row["bid"],
                row["ask"],
                forecast["pip"],
            )
            for row in path
        ]
        realized_net = executable_net_pips(
            forecast["direction"],
            forecast["entry_bid"],
            forecast["entry_ask"],
            exit_quote["bid"],
            exit_quote["ask"],
            forecast["pip"],
        )
        signed_mid = signed_mid_move_pips(
            forecast["direction"],
            forecast["entry_bid"],
            forecast["entry_ask"],
            exit_quote["bid"],
            exit_quote["ask"],
            forecast["pip"],
        )
        projected_gross = forecast["projected_gross_movement_pips"]
        magnitude_error = (
            None
            if projected_gross is None
            else abs(projected_gross - abs(signed_mid))
        )
        updates.append(
            (
                exit_quote["quote_epoch"],
                datetime.fromtimestamp(
                    exit_quote["quote_epoch"], tz=timezone.utc
                ).isoformat(),
                exit_quote["quote_utc"],
                exit_quote["bid"],
                exit_quote["ask"],
                exit_delay,
                signed_mid,
                realized_net,
                max(net_path) if net_path else realized_net,
                min(net_path) if net_path else realized_net,
                int(signed_mid > 0),
                int(realized_net > 0),
                magnitude_error,
                forecast["observation_id"],
            )
        )
        matured += 1
    connection.executemany(
        """
        UPDATE forecasts SET
            matured_epoch = ?, matured_utc = ?, exit_quote_utc = ?,
            exit_bid = ?, exit_ask = ?, exit_delay_sec = ?,
            signed_mid_move_pips = ?, executable_net_pips = ?,
            mfe_pips = ?, mae_pips = ?, direction_correct = ?,
            net_positive = ?, magnitude_abs_error_pips = ?
        WHERE observation_id = ?
        """,
        updates,
    )
    connection.executemany(
        """
        UPDATE forecasts
        SET censored_epoch = ?, censor_reason = ?
        WHERE observation_id = ?
        """,
        censored,
    )
    connection.commit()
    return matured


def _mean(values: Iterable[float | None]) -> float | None:
    finite = [value for value in values if value is not None and math.isfinite(value)]
    return statistics.fmean(finite) if finite else None


def _median(values: Iterable[float | None]) -> float | None:
    finite = [value for value in values if value is not None and math.isfinite(value)]
    return statistics.median(finite) if finite else None


def _pearson(left: list[float], right: list[float]) -> float | None:
    if len(left) < 3 or len(left) != len(right):
        return None
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    numerator = sum(
        (a - left_mean) * (b - right_mean) for a, b in zip(left, right)
    )
    left_scale = math.sqrt(sum((a - left_mean) ** 2 for a in left))
    right_scale = math.sqrt(sum((b - right_mean) ** 2 for b in right))
    if left_scale == 0 or right_scale == 0:
        return None
    return numerator / (left_scale * right_scale)


def metric_block(rows: list[sqlite3.Row]) -> dict[str, Any]:
    if not rows:
        return {"observations": 0}
    executable = [finite_float(row["executable_net_pips"]) for row in rows]
    signed_mid = [finite_float(row["signed_mid_move_pips"]) for row in rows]
    probabilities = [
        finite_float(row["filtered_probability_up"])
        if finite_float(row["filtered_probability_up"]) is not None
        else finite_float(row["raw_probability_up"])
        for row in rows
    ]
    brier_values = []
    for row, probability in zip(rows, probabilities):
        if probability is None:
            continue
        actual_up = (
            row["signed_mid_move_pips"] > 0
            if row["direction"] == "buy"
            else row["signed_mid_move_pips"] < 0
        )
        brier_values.append((probability - float(actual_up)) ** 2)
    projected_pairs = [
        (
            finite_float(row["projected_net_pips"]),
            finite_float(row["executable_net_pips"]),
        )
        for row in rows
    ]
    projected_pairs = [
        (left, right)
        for left, right in projected_pairs
        if left is not None and right is not None
    ]
    projected_net_errors = [
        abs(left - right) for left, right in projected_pairs
    ]
    projected_net_bias = [
        left - right for left, right in projected_pairs
    ]
    gross_pairs = [
        (
            finite_float(row["projected_gross_movement_pips"]),
            abs(finite_float(row["signed_mid_move_pips"]) or 0.0),
        )
        for row in rows
        if finite_float(row["projected_gross_movement_pips"]) is not None
        and finite_float(row["signed_mid_move_pips"]) is not None
    ]
    spreads = [
        finite_float(row["entry_spread_pips"])
        for row in rows
    ]
    absolute_moves = [
        abs(finite_float(row["signed_mid_move_pips"]) or 0.0)
        for row in rows
    ]
    move_to_spread = [
        move / spread
        for move, spread in zip(absolute_moves, spreads)
        if spread is not None and spread > 0.0
    ]
    direction_correct_count = sum(
        int(row["direction_correct"]) for row in rows
    )
    net_positive_count = sum(int(row["net_positive"]) for row in rows)
    cost_only_misses = sum(
        int(bool(row["direction_correct"]) and not bool(row["net_positive"]))
        for row in rows
    )
    return {
        "observations": len(rows),
        "direction_accuracy": _mean(
            [float(row["direction_correct"]) for row in rows]
        ),
        "net_win_rate": _mean([float(row["net_positive"]) for row in rows]),
        "mean_executable_net_pips": _mean(executable),
        "median_executable_net_pips": _median(executable),
        "sum_executable_net_pips": sum(value or 0.0 for value in executable),
        "mean_signed_mid_move_pips": _mean(signed_mid),
        "mean_absolute_mid_move_pips": _mean(absolute_moves),
        "mean_entry_spread_pips": _mean(spreads),
        "mean_realized_move_to_spread": _mean(move_to_spread),
        "movement_exceeds_entry_spread_rate": _mean(
            [
                float(move > spread)
                for move, spread in zip(absolute_moves, spreads)
                if spread is not None
            ]
        ),
        "cost_only_miss_count": cost_only_misses,
        "cost_only_miss_rate": cost_only_misses / len(rows),
        "post_spread_win_given_correct_direction": (
            None
            if direction_correct_count == 0
            else net_positive_count / direction_correct_count
        ),
        "mean_cost_drag_pips": _mean(
            [
                mid - net
                for mid, net in zip(signed_mid, executable)
                if mid is not None and net is not None
            ]
        ),
        "mean_mfe_pips": _mean(
            [finite_float(row["mfe_pips"]) for row in rows]
        ),
        "mean_mae_pips": _mean(
            [finite_float(row["mae_pips"]) for row in rows]
        ),
        "magnitude_mae_pips": _mean(
            [finite_float(row["magnitude_abs_error_pips"]) for row in rows]
        ),
        "direction_brier": _mean(brier_values),
        "projected_net_actual_correlation": _pearson(
            [pair[0] for pair in projected_pairs],
            [pair[1] for pair in projected_pairs],
        ),
        "projected_net_mae_pips": _mean(projected_net_errors),
        "projected_net_bias_pips": _mean(projected_net_bias),
        "projected_gross_actual_abs_correlation": _pearson(
            [pair[0] for pair in gross_pairs],
            [pair[1] for pair in gross_pairs],
        ),
        "mean_exit_delay_sec": _mean(
            [finite_float(row["exit_delay_sec"]) for row in rows]
        ),
    }


def grouped_metrics(
    rows: list[sqlite3.Row], key: str, minimum_rows: int = 1
) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        groups[str(row[key])].append(row)
    return {
        name: metric_block(group)
        for name, group in sorted(groups.items())
        if len(group) >= minimum_rows
    }


def bucket_metrics(
    rows: list[sqlite3.Row],
    value_key: str,
    buckets: tuple[tuple[str, float, float], ...],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for label, lower, upper in buckets:
        selected = []
        for row in rows:
            value = finite_float(row[value_key])
            if value is not None and lower <= value < upper:
                selected.append(row)
        result[label] = metric_block(selected)
    return result


def build_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    sample_count = connection.execute(
        "SELECT COUNT(*) FROM monitor_samples"
    ).fetchone()[0]
    stable_samples = connection.execute(
        "SELECT COUNT(*) FROM monitor_samples WHERE stable_generation = 1"
    ).fetchone()[0]
    total_forecasts = connection.execute(
        "SELECT COUNT(*) FROM forecasts"
    ).fetchone()[0]
    pending_forecasts = connection.execute(
        """
        SELECT COUNT(*) FROM forecasts
        WHERE matured_epoch IS NULL AND censored_epoch IS NULL
        """
    ).fetchone()[0]
    censored_forecasts = connection.execute(
        "SELECT COUNT(*) FROM forecasts WHERE censored_epoch IS NOT NULL"
    ).fetchone()[0]
    matured_all = connection.execute(
        "SELECT * FROM forecasts WHERE matured_epoch IS NOT NULL"
    ).fetchall()
    matured_stable = [
        row for row in matured_all if int(row["stable_generation"]) == 1
    ]
    coverage_qualified = connection.execute(
        """
        SELECT f.* FROM forecasts AS f
        INNER JOIN coverage_samples AS c
          ON c.captured_epoch = f.captured_epoch
        WHERE f.matured_epoch IS NOT NULL
          AND c.feed_candidate_count >= 1000
          AND c.contributor_coverage_ratio >= 0.45
        """
    ).fetchall()
    preferred_stable = connection.execute(
        """
        SELECT f.* FROM forecasts AS f
        INNER JOIN signal_rows AS s
          ON s.captured_epoch = f.captured_epoch
         AND s.instrument = f.instrument
         AND s.preferred_horizon_sec = f.horizon_sec
        WHERE f.matured_epoch IS NOT NULL
          AND f.stable_generation = 1
        """
    ).fetchall()
    rank_one_preferred_stable = connection.execute(
        """
        SELECT f.* FROM forecasts AS f
        INNER JOIN signal_rows AS s
          ON s.captured_epoch = f.captured_epoch
         AND s.instrument = f.instrument
         AND s.preferred_horizon_sec = f.horizon_sec
        WHERE f.matured_epoch IS NOT NULL
          AND f.stable_generation = 1
          AND s.signal_rank = 1
        """
    ).fetchall()
    preferred_coverage_qualified = connection.execute(
        """
        SELECT f.* FROM forecasts AS f
        INNER JOIN signal_rows AS s
          ON s.captured_epoch = f.captured_epoch
         AND s.instrument = f.instrument
         AND s.preferred_horizon_sec = f.horizon_sec
        INNER JOIN coverage_samples AS c
          ON c.captured_epoch = f.captured_epoch
        WHERE f.matured_epoch IS NOT NULL
          AND c.feed_candidate_count >= 1000
          AND c.contributor_coverage_ratio >= 0.45
        """
    ).fetchall()
    rank_one_preferred_coverage_qualified = connection.execute(
        """
        SELECT f.* FROM forecasts AS f
        INNER JOIN signal_rows AS s
          ON s.captured_epoch = f.captured_epoch
         AND s.instrument = f.instrument
         AND s.preferred_horizon_sec = f.horizon_sec
        INNER JOIN coverage_samples AS c
          ON c.captured_epoch = f.captured_epoch
        WHERE f.matured_epoch IS NOT NULL
          AND s.signal_rank = 1
          AND c.feed_candidate_count >= 1000
          AND c.contributor_coverage_ratio >= 0.45
        """
    ).fetchall()
    stable_eligible = [
        row for row in matured_stable if int(row["signal_eligible"]) == 1
    ]
    stable_blocked = [
        row for row in matured_stable if int(row["signal_eligible"]) == 0
    ]
    no_conflict = [
        row for row in matured_stable if int(row["direction_conflict"]) == 0
    ]
    account_first = connection.execute(
        """
        SELECT * FROM monitor_samples ORDER BY captured_epoch LIMIT 1
        """
    ).fetchone()
    account_last = connection.execute(
        """
        SELECT * FROM monitor_samples ORDER BY captured_epoch DESC LIMIT 1
        """
    ).fetchone()
    coverage = connection.execute(
        "SELECT * FROM coverage_samples ORDER BY captured_epoch"
    ).fetchall()
    blocker_counts: dict[str, int] = defaultdict(int)
    for row in matured_stable:
        for blocker in json.loads(row["blockers_json"]):
            blocker_counts[str(blocker)] += 1
    return {
        "generated_utc": utc_now(),
        "protocol": {
            "read_only": True,
            "oanda_api_called": False,
            "orders_supported": False,
            "cost_basis": "entry ask/exit bid for buys; entry bid/exit ask for sells",
            "sampling_note": (
                "Outcomes use the first sampled quote at or after maturity; "
                "overlapping observations are not independent trades."
            ),
            "stable_generation_rule": (
                "raw_candidate_count >= configured minimum and "
                "consolidated_signal_count >= configured minimum"
            ),
        },
        "counts": {
            "monitor_samples": sample_count,
            "stable_samples": stable_samples,
            "partial_samples": sample_count - stable_samples,
            "total_forecasts": total_forecasts,
            "matured_forecasts": len(matured_all),
            "pending_forecasts": pending_forecasts,
            "censored_forecasts": censored_forecasts,
            "stable_matured_forecasts": len(matured_stable),
            "stable_eligible_forecasts": len(stable_eligible),
            "stable_blocked_forecasts": len(stable_blocked),
            "stable_preferred_forecasts": len(preferred_stable),
            "stable_rank_one_preferred_forecasts": len(
                rank_one_preferred_stable
            ),
            "coverage_qualified_forecasts": len(coverage_qualified),
            "coverage_qualified_preferred_forecasts": len(
                preferred_coverage_qualified
            ),
            "coverage_qualified_rank_one_preferred_forecasts": len(
                rank_one_preferred_coverage_qualified
            ),
        },
        "coverage_qualified_metrics": metric_block(coverage_qualified),
        "coverage_qualified_preferred_metrics": metric_block(
            preferred_coverage_qualified
        ),
        "coverage_qualified_rank_one_preferred_metrics": metric_block(
            rank_one_preferred_coverage_qualified
        ),
        "coverage_qualified_by_horizon": grouped_metrics(
            coverage_qualified, "horizon_sec"
        ),
        "coverage_qualified_by_instrument": grouped_metrics(
            coverage_qualified, "instrument", minimum_rows=5
        ),
        "coverage_qualified_by_best_family": grouped_metrics(
            coverage_qualified, "best_family", minimum_rows=5
        ),
        "coverage_qualified_by_confidence": bucket_metrics(
            coverage_qualified,
            "signal_confidence",
            (
                ("below_0.52", -math.inf, 0.52),
                ("0.52_to_0.535", 0.52, 0.535),
                ("0.535_to_0.55", 0.535, 0.55),
                ("0.55_to_0.60", 0.55, 0.60),
                ("0.60_plus", 0.60, math.inf),
            ),
        ),
        "coverage_qualified_by_gross_to_spread": bucket_metrics(
            coverage_qualified,
            "gross_to_spread",
            (
                ("below_1.0", -math.inf, 1.0),
                ("1.0_to_1.15", 1.0, 1.15),
                ("1.15_to_1.35", 1.15, 1.35),
                ("1.35_plus", 1.35, math.inf),
            ),
        ),
        "stable_metrics": metric_block(matured_stable),
        "stable_preferred_metrics": metric_block(preferred_stable),
        "stable_rank_one_preferred_metrics": metric_block(
            rank_one_preferred_stable
        ),
        "all_metrics": metric_block(matured_all),
        "eligible_metrics": metric_block(stable_eligible),
        "blocked_metrics": metric_block(stable_blocked),
        "no_direction_conflict_metrics": metric_block(no_conflict),
        "by_horizon": grouped_metrics(matured_stable, "horizon_sec"),
        "by_instrument": grouped_metrics(
            matured_stable, "instrument", minimum_rows=5
        ),
        "by_best_family": grouped_metrics(
            matured_stable, "best_family", minimum_rows=5
        ),
        "by_confidence": bucket_metrics(
            matured_stable,
            "signal_confidence",
            (
                ("below_0.52", -math.inf, 0.52),
                ("0.52_to_0.535", 0.52, 0.535),
                ("0.535_to_0.55", 0.535, 0.55),
                ("0.55_to_0.60", 0.55, 0.60),
                ("0.60_plus", 0.60, math.inf),
            ),
        ),
        "by_gross_to_spread": bucket_metrics(
            matured_stable,
            "gross_to_spread",
            (
                ("below_1.0", -math.inf, 1.0),
                ("1.0_to_1.15", 1.0, 1.15),
                ("1.15_to_1.35", 1.15, 1.35),
                ("1.35_plus", 1.35, math.inf),
            ),
        ),
        "partial_generation_metrics": metric_block(
            [
                row
                for row in matured_all
                if int(row["stable_generation"]) == 0
            ]
        ),
        "blocker_counts": dict(
            sorted(
                blocker_counts.items(),
                key=lambda item: item[1],
                reverse=True,
            )
        ),
        "account": {
            "first": None if account_first is None else {
                "captured_utc": account_first["captured_utc"],
                "balance": account_first["account_balance"],
                "nav": account_first["account_nav"],
                "pl": account_first["account_pl"],
                "unrealized_pl": account_first["account_unrealized_pl"],
                "open_trade_count": account_first["open_trade_count"],
            },
            "last": None if account_last is None else {
                "captured_utc": account_last["captured_utc"],
                "balance": account_last["account_balance"],
                "nav": account_last["account_nav"],
                "pl": account_last["account_pl"],
                "unrealized_pl": account_last["account_unrealized_pl"],
                "open_trade_count": account_last["open_trade_count"],
            },
        },
        "coverage": {
            "samples": len(coverage),
            "mean_feed_candidate_count": _mean(
                [finite_float(row["feed_candidate_count"]) for row in coverage]
            ),
            "minimum_feed_candidate_count": (
                None
                if not coverage
                else min(row["feed_candidate_count"] for row in coverage)
            ),
            "maximum_feed_candidate_count": (
                None
                if not coverage
                else max(row["feed_candidate_count"] for row in coverage)
            ),
            "mean_contributor_coverage_ratio": _mean(
                [
                    finite_float(row["contributor_coverage_ratio"])
                    for row in coverage
                ]
            ),
            "minimum_contributor_coverage_ratio": (
                None
                if not coverage
                else min(
                    finite_float(row["contributor_coverage_ratio"]) or 0.0
                    for row in coverage
                )
            ),
            "maximum_contributor_coverage_ratio": (
                None
                if not coverage
                else max(
                    finite_float(row["contributor_coverage_ratio"]) or 0.0
                    for row in coverage
                )
            ),
        },
    }


def markdown_report(summary: dict[str, Any]) -> str:
    counts = summary["counts"]
    metrics = summary["stable_metrics"]
    coverage_metrics = summary["coverage_qualified_metrics"]
    coverage_preferred = summary["coverage_qualified_preferred_metrics"]
    coverage_rank_one = summary[
        "coverage_qualified_rank_one_preferred_metrics"
    ]
    preferred = summary["stable_preferred_metrics"]
    rank_one = summary["stable_rank_one_preferred_metrics"]

    def display(value: Any, digits: int = 4) -> str:
        if value is None:
            return "n/a"
        if isinstance(value, float):
            return f"{value:.{digits}f}"
        return str(value)

    lines = [
        "# Practice 007 Forecast Outcome Monitor",
        "",
        f"Generated: `{summary['generated_utc']}`",
        "",
        "## Protocol",
        "",
        "- Read-only: yes",
        "- OANDA API called by this monitor: no",
        "- Orders supported by this monitor: no",
        "- Outcome economics: executable bid/ask round trip",
        "- Primary analysis: complete/stable signal universes only",
        "- Observations are overlapping forecasts, not an account backtest",
        "",
        "## Coverage",
        "",
        f"- Samples: {counts['monitor_samples']}",
        f"- Stable / partial samples: {counts['stable_samples']} / {counts['partial_samples']}",
        f"- Forecasts logged: {counts['total_forecasts']}",
        f"- Stable matured / pending: {counts['stable_matured_forecasts']} / {counts['pending_forecasts']}",
        f"- Stable eligible / blocked: {counts['stable_eligible_forecasts']} / {counts['stable_blocked_forecasts']}",
        f"- Mean merged feed candidates: {display(summary['coverage']['mean_feed_candidate_count'], 1)}",
        f"- Mean contributor coverage: {display(summary['coverage']['mean_contributor_coverage_ratio'])}",
        "",
        "## Coverage-Qualified Primary Metrics",
        "",
        "- Rule: at least 1,000 merged candidates and 45% contributor coverage",
        f"- Observations: {coverage_metrics.get('observations', 0)}",
        f"- Direction accuracy: {display(coverage_metrics.get('direction_accuracy'))}",
        f"- Post-spread win rate: {display(coverage_metrics.get('net_win_rate'))}",
        f"- Mean executable net: {display(coverage_metrics.get('mean_executable_net_pips'))} pips",
        f"- Preferred observations: {coverage_preferred.get('observations', 0)}",
        f"- Preferred direction accuracy: {display(coverage_preferred.get('direction_accuracy'))}",
        f"- Preferred post-spread win rate: {display(coverage_preferred.get('net_win_rate'))}",
        f"- Preferred mean executable net: {display(coverage_preferred.get('mean_executable_net_pips'))} pips",
        f"- Rank-1 preferred observations: {coverage_rank_one.get('observations', 0)}",
        f"- Rank-1 direction accuracy: {display(coverage_rank_one.get('direction_accuracy'))}",
        f"- Rank-1 post-spread win rate: {display(coverage_rank_one.get('net_win_rate'))}",
        f"- Rank-1 mean executable net: {display(coverage_rank_one.get('mean_executable_net_pips'))} pips",
        "",
        "## Stable Forecast Metrics",
        "",
        f"- Direction accuracy: {display(metrics.get('direction_accuracy'))}",
        f"- Post-spread win rate: {display(metrics.get('net_win_rate'))}",
        f"- Mean executable net: {display(metrics.get('mean_executable_net_pips'))} pips",
        f"- Median executable net: {display(metrics.get('median_executable_net_pips'))} pips",
        f"- Mean absolute mid move: {display(metrics.get('mean_absolute_mid_move_pips'))} pips",
        f"- Mean entry spread: {display(metrics.get('mean_entry_spread_pips'))} pips",
        f"- Mean realized move/spread: {display(metrics.get('mean_realized_move_to_spread'))}",
        f"- Movement exceeded entry spread: {display(metrics.get('movement_exceeds_entry_spread_rate'))}",
        f"- Direction-correct but post-spread losing: {display(metrics.get('cost_only_miss_rate'))}",
        f"- Post-spread win given correct direction: {display(metrics.get('post_spread_win_given_correct_direction'))}",
        f"- Mean cost drag: {display(metrics.get('mean_cost_drag_pips'))} pips",
        f"- Direction Brier score: {display(metrics.get('direction_brier'))}",
        f"- Magnitude MAE: {display(metrics.get('magnitude_mae_pips'))} pips",
        f"- Projected-net/actual correlation: {display(metrics.get('projected_net_actual_correlation'))}",
        f"- Projected-gross/actual-absolute correlation: {display(metrics.get('projected_gross_actual_abs_correlation'))}",
        "",
        "## Allocator-Facing Forecasts",
        "",
        f"- Preferred-horizon observations: {preferred.get('observations', 0)}",
        f"- Preferred direction accuracy: {display(preferred.get('direction_accuracy'))}",
        f"- Preferred post-spread win rate: {display(preferred.get('net_win_rate'))}",
        f"- Preferred mean executable net: {display(preferred.get('mean_executable_net_pips'))} pips",
        f"- Rank-1 preferred observations: {rank_one.get('observations', 0)}",
        f"- Rank-1 direction accuracy: {display(rank_one.get('direction_accuracy'))}",
        f"- Rank-1 post-spread win rate: {display(rank_one.get('net_win_rate'))}",
        f"- Rank-1 mean executable net: {display(rank_one.get('mean_executable_net_pips'))} pips",
        "",
        "## Horizon Detail",
        "",
        "| Horizon | N | Direction | Net win | Mean net pips | Magnitude MAE |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for horizon, block in summary["by_horizon"].items():
        lines.append(
            "| "
            + " | ".join(
                (
                    f"{horizon}s",
                    str(block.get("observations", 0)),
                    display(block.get("direction_accuracy")),
                    display(block.get("net_win_rate")),
                    display(block.get("mean_executable_net_pips")),
                    display(block.get("magnitude_mae_pips")),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "## Leading Blockers",
            "",
        )
    )
    for name, count in list(summary["blocker_counts"].items())[:15]:
        lines.append(f"- `{name}`: {count}")
    lines.extend(
        (
            "",
            "## Account",
            "",
            f"- First snapshot: `{summary['account']['first']}`",
            f"- Last snapshot: `{summary['account']['last']}`",
            "",
        )
    )
    return "\n".join(lines)


def write_reports(output_dir: Path, summary: dict[str, Any]) -> None:
    atomic_json(
        output_dir / "PRACTICE_007_FORECAST_OUTCOME_REPORT.json", summary
    )
    (output_dir / "PRACTICE_007_FORECAST_OUTCOME_REPORT.md").write_text(
        markdown_report(summary), encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only, exact-horizon outcome monitor for practice -007 "
            "forecast snapshots."
        )
    )
    parser.add_argument("--signal-snapshot", type=Path, required=True)
    parser.add_argument("--feature-snapshot", type=Path, required=True)
    parser.add_argument("--market-quote-snapshot", type=Path)
    parser.add_argument("--account-snapshot", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--end-utc")
    parser.add_argument("--duration-sec", type=float)
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--minimum-raw-candidates", type=int, default=500)
    parser.add_argument("--minimum-consolidated-signals", type=int, default=32)
    parser.add_argument("--maximum-quote-age-sec", type=float, default=300.0)
    parser.add_argument(
        "--report-interval-sec",
        type=float,
        default=900.0,
        help="Minimum interval between expensive full report rebuilds.",
    )
    args = parser.parse_args()

    if bool(args.end_utc) == bool(args.duration_sec):
        parser.error("Specify exactly one of --end-utc or --duration-sec")
    end_dt = parse_timestamp(args.end_utc) if args.end_utc else None
    end_epoch = (
        end_dt.timestamp()
        if end_dt is not None
        else time.time() + float(args.duration_sec)
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    connection = connect_database(
        args.output_dir / "PRACTICE_007_FORECAST_OUTCOMES.sqlite"
    )
    state_path = args.output_dir / "PRACTICE_007_FORECAST_MONITOR_STATE.json"
    latest_path = args.output_dir / "PRACTICE_007_FORECAST_MONITOR_LATEST.json"
    started_epoch = time.time()
    sample_count = int(
        connection.execute("SELECT COUNT(*) FROM monitor_samples").fetchone()[0]
    )
    last_report_epoch = started_epoch
    snapshot_cache: dict[
        Path, tuple[tuple[int, int], dict[str, Any]]
    ] = {}

    try:
        while True:
            loop_started = time.time()
            signal_payload = read_json_cached(
                args.signal_snapshot, snapshot_cache
            )
            feature_payload = read_json_cached(
                args.feature_snapshot, snapshot_cache
            )
            market_quote_payload = (
                read_json_cached(
                    args.market_quote_snapshot, snapshot_cache
                )
                if args.market_quote_snapshot is not None
                and args.market_quote_snapshot.exists()
                else {}
            )
            account_payload = read_json_cached(
                args.account_snapshot, snapshot_cache
            )
            capture_summary = record_capture(
                connection,
                signal_payload,
                feature_payload,
                market_quote_payload,
                account_payload,
                loop_started,
                args.minimum_raw_candidates,
                args.minimum_consolidated_signals,
                args.maximum_quote_age_sec,
            )
            matured = mature_forecasts(connection, loop_started)
            sample_count += 1
            latest = {
                **capture_summary,
                "matured_this_cycle": matured,
                "read_only": True,
                "oanda_api_called": False,
                "orders_supported": False,
            }
            atomic_json(latest_path, latest)
            if loop_started - last_report_epoch >= max(
                args.report_interval_sec, args.interval_sec
            ):
                write_reports(args.output_dir, build_summary(connection))
                last_report_epoch = time.time()
            atomic_json(
                state_path,
                {
                    "status": (
                        "complete" if loop_started >= end_epoch else "running"
                    ),
                    "started_utc": datetime.fromtimestamp(
                        started_epoch, tz=timezone.utc
                    ).isoformat(),
                    "updated_utc": utc_now(),
                    "end_utc": datetime.fromtimestamp(
                        end_epoch, tz=timezone.utc
                    ).isoformat(),
                    "sample_count": sample_count,
                    "last_capture": latest,
                    "read_only": True,
                    "oanda_api_called": False,
                    "orders_supported": False,
                },
            )
            if loop_started >= end_epoch:
                break
            sleep_for = min(
                max(0.1, args.interval_sec - (time.time() - loop_started)),
                max(0.1, end_epoch - time.time()),
            )
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        atomic_json(
            state_path,
            {
                "status": "interrupted",
                "started_utc": datetime.fromtimestamp(
                    started_epoch, tz=timezone.utc
                ).isoformat(),
                "updated_utc": utc_now(),
                "end_utc": datetime.fromtimestamp(
                    end_epoch, tz=timezone.utc
                ).isoformat(),
                "sample_count": sample_count,
                "read_only": True,
                "oanda_api_called": False,
                "orders_supported": False,
            },
        )
        return 130
    finally:
        mature_forecasts(connection, time.time())
        write_reports(args.output_dir, build_summary(connection))
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
