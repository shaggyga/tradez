#!/usr/bin/env python3
"""Build immutable executable movement episodes and knowledge-time news maps.

Large moves are selected from recorded BAM bid/ask paths.  News available
before entry is kept separate from news first observed during the move.  The
latter is ex-post attribution and can never become an entry feature.  This is
research-only and has no broker, promotion, or authorization path.
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from oanda_cost_clearance_cross_sectional_research import infer_pip_size


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "movement_news_episode_research_v1.json"
SOURCE = ROOT / "data" / "oanda_training_manager" / "candles"
SOURCE_EVENTS = ROOT / "data" / "oanda_training_manager" / "state" / "source_governance_v1.sqlite"
DATABASE = ROOT / "data" / "oanda_training_manager" / "state" / "movement_news_episode_research_v1.sqlite"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "movement_news_episode_research" / "MOVEMENT_NEWS_EPISODE_RESEARCH_20260809.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "movement_news_episode_research" / "MOVEMENT_NEWS_EPISODE_RESEARCH_20260809.md"


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    material = "|".join(str(part) for part in parts)
    return f"{prefix}_{hashlib.sha256(material.encode()).hexdigest()[:24]}"


def parse_epoch(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return int(parsed.timestamp())


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=60)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS research_contracts (
          contract_id TEXT PRIMARY KEY,
          created_utc TEXT NOT NULL,
          config_sha256 TEXT NOT NULL,
          source_code_sha256 TEXT NOT NULL,
          price_manifest_sha256 TEXT NOT NULL,
          source_event_highwater_utc TEXT,
          contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS movement_episodes (
          episode_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          instrument TEXT NOT NULL,
          base_currency TEXT NOT NULL,
          quote_currency TEXT NOT NULL,
          entry_utc TEXT NOT NULL,
          exit_utc TEXT NOT NULL,
          entry_epoch INTEGER NOT NULL,
          exit_epoch INTEGER NOT NULL,
          horizon_min INTEGER NOT NULL,
          selected_side TEXT NOT NULL,
          signed_currency_factor TEXT NOT NULL,
          market_episode_id TEXT NOT NULL,
          entry_bid REAL NOT NULL,
          entry_ask REAL NOT NULL,
          exit_bid REAL NOT NULL,
          exit_ask REAL NOT NULL,
          entry_spread_pips REAL NOT NULL,
          exit_spread_pips REAL NOT NULL,
          signed_mid_move_pips REAL NOT NULL,
          gross_magnitude_pips REAL NOT NULL,
          endpoint_after_cost_pips REAL NOT NULL,
          mfe_pips REAL NOT NULL,
          mae_pips REAL NOT NULL,
          selection_threshold_pips REAL NOT NULL,
          selection_quantile REAL NOT NULL,
          pre_entry_source_count INTEGER NOT NULL,
          pre_entry_story_count INTEGER NOT NULL,
          pre_entry_official_count INTEGER NOT NULL,
          pre_entry_directional_count INTEGER NOT NULL,
          in_window_source_count INTEGER NOT NULL,
          in_window_story_count INTEGER NOT NULL,
          mapping_json TEXT NOT NULL,
          created_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS episode_source_links (
          link_id TEXT PRIMARY KEY,
          episode_id TEXT NOT NULL,
          source_event_id TEXT NOT NULL,
          relation TEXT NOT NULL,
          effective_from_utc TEXT NOT NULL,
          source_id TEXT NOT NULL,
          source_population TEXT NOT NULL,
          event_type TEXT NOT NULL,
          story_cluster_id TEXT,
          seconds_from_entry INTEGER NOT NULL,
          causal_entry_eligible INTEGER NOT NULL,
          UNIQUE(episode_id, source_event_id, relation)
        );
        CREATE TRIGGER IF NOT EXISTS movement_episodes_no_update
        BEFORE UPDATE ON movement_episodes BEGIN SELECT RAISE(ABORT, 'movement episodes are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS movement_episodes_no_delete
        BEFORE DELETE ON movement_episodes BEGIN SELECT RAISE(ABORT, 'movement episodes are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS episode_source_links_no_update
        BEFORE UPDATE ON episode_source_links BEGIN SELECT RAISE(ABORT, 'episode links are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS episode_source_links_no_delete
        BEFORE DELETE ON episode_source_links BEGIN SELECT RAISE(ABORT, 'episode links are immutable'); END;
        """
    )
    return db


def load_price(path: Path) -> tuple[pd.DataFrame, str, float]:
    columns = [
        "datetime", "instrument", "bid_high", "bid_low", "bid_close",
        "ask_high", "ask_low", "ask_close", "spread_pips",
    ]
    frame = pd.read_csv(path, usecols=columns)
    instrument = str(frame["instrument"].dropna().iloc[0])
    frame["timestamp"] = pd.to_datetime(frame["datetime"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates("timestamp", keep="last").reset_index(drop=True)
    frame["epoch"] = frame["timestamp"].astype("int64") // 1_000_000_000
    frame["mid"] = (frame["bid_close"] + frame["ask_close"]) / 2.0
    return frame, instrument, infer_pip_size(instrument, frame)


def nonoverlapping_local_extremes(candidates: pd.DataFrame, horizon_min: int) -> pd.DataFrame:
    selected: list[int] = []
    occupied: list[int] = []
    distance = horizon_min * 60
    for index, row in candidates.sort_values(["best_after_cost_pips", "epoch"], ascending=[False, True]).iterrows():
        epoch = int(row["epoch"])
        if any(abs(epoch - prior) < distance for prior in occupied):
            continue
        selected.append(index)
        bisect.insort(occupied, epoch)
    return candidates.loc[selected].sort_values("epoch")


def price_episodes(frame: pd.DataFrame, instrument: str, pip: float, horizon_min: int, config: Mapping[str, Any]) -> list[dict[str, Any]]:
    horizon = int(horizon_min)
    future = frame[["epoch", "bid_close", "ask_close", "mid", "spread_pips"]].copy()
    future["exit_position"] = np.arange(len(frame))
    future["future_epoch"] = future["epoch"]
    future["epoch"] = future["epoch"] - horizon * 60
    future = future.rename(columns={
        "bid_close": "future_bid", "ask_close": "future_ask", "mid": "future_mid",
        "spread_pips": "future_spread",
    })
    slippage = float(config.get("modeled_slippage_pips", 0.25))
    work = pd.DataFrame({
        "position": np.arange(len(frame)),
        "epoch": frame["epoch"],
        "entry_bid": frame["bid_close"], "entry_ask": frame["ask_close"],
        "entry_mid": frame["mid"], "entry_spread": frame["spread_pips"],
    }).merge(future, on="epoch", how="left", validate="one_to_one")
    work["long_net_pips"] = (work["future_bid"] - work["entry_ask"]) / pip - slippage
    work["short_net_pips"] = (work["entry_bid"] - work["future_ask"]) / pip - slippage
    work["gross_magnitude_pips"] = ((work["future_mid"] - work["entry_mid"]) / pip).abs()
    work["signed_mid_move_pips"] = (work["future_mid"] - work["entry_mid"]) / pip
    work["actual_cost_pips"] = (work["entry_spread"] + work["future_spread"]) / 2.0 + slippage
    work = work.dropna()
    work["best_after_cost_pips"] = work[["long_net_pips", "short_net_pips"]].max(axis=1)
    positive = work[work["best_after_cost_pips"] > float(config.get("minimum_positive_after_cost_pips", 0.0))]
    if positive.empty:
        return []
    quantile = float(config.get("episode_quantile", 0.99))
    threshold = float(positive["best_after_cost_pips"].quantile(quantile))
    minimum_multiple = float(config.get("minimum_gross_cost_multiple", 2.0))
    candidates = positive[
        (positive["best_after_cost_pips"] >= threshold)
        & (positive["gross_magnitude_pips"] >= minimum_multiple * positive["actual_cost_pips"])
    ]
    selected = nonoverlapping_local_extremes(candidates, horizon)
    base, quote = instrument.split("_")
    rows: list[dict[str, Any]] = []
    for _, candidate in selected.iterrows():
        position = int(candidate["position"])
        entry = frame.iloc[position]
        exit_position = int(candidate["exit_position"])
        exit_row = frame.iloc[exit_position]
        side = "long" if candidate["long_net_pips"] >= candidate["short_net_pips"] else "short"
        path = frame.iloc[position + 1: exit_position + 1]
        if side == "long":
            mfe = float((path["bid_high"].max() - entry["ask_close"]) / pip - slippage)
            mae = float((path["bid_low"].min() - entry["ask_close"]) / pip - slippage)
            factor = f"{base}+|{quote}-"
        else:
            mfe = float((entry["bid_close"] - path["ask_low"].min()) / pip - slippage)
            mae = float((entry["bid_close"] - path["ask_high"].max()) / pip - slippage)
            factor = f"{base}-|{quote}+"
        episode_bucket = int(entry["epoch"]) // max(300, horizon * 60)
        rows.append({
            "instrument": instrument,
            "base_currency": base,
            "quote_currency": quote,
            "entry_epoch": int(entry["epoch"]),
            "exit_epoch": int(exit_row["epoch"]),
            "entry_utc": entry["timestamp"].isoformat(),
            "exit_utc": exit_row["timestamp"].isoformat(),
            "horizon_min": horizon,
            "selected_side": side,
            "signed_currency_factor": factor,
            "market_episode_id": f"market_episode_{horizon}m_{episode_bucket}",
            "entry_bid": float(entry["bid_close"]),
            "entry_ask": float(entry["ask_close"]),
            "exit_bid": float(exit_row["bid_close"]),
            "exit_ask": float(exit_row["ask_close"]),
            "entry_spread_pips": float(entry["spread_pips"]),
            "exit_spread_pips": float(exit_row["spread_pips"]),
            "signed_mid_move_pips": float(candidate["signed_mid_move_pips"]),
            "gross_magnitude_pips": float(candidate["gross_magnitude_pips"]),
            "endpoint_after_cost_pips": float(candidate["best_after_cost_pips"]),
            "mfe_pips": mfe,
            "mae_pips": mae,
            "selection_threshold_pips": threshold,
            "selection_quantile": quantile,
        })
    return rows


def _payload_currencies(payload_json: str) -> set[str]:
    try:
        payload = json.loads(payload_json or "{}")
    except json.JSONDecodeError:
        return set()
    result: set[str] = set()
    for key in ("currencies", "direct_currencies"):
        value = payload.get(key)
        if isinstance(value, list):
            result.update(str(item).upper() for item in value if item)
    raw = payload.get("raw_payload")
    if isinstance(raw, dict):
        for key in ("currencies", "direct_currencies"):
            value = raw.get(key)
            if isinstance(value, list):
                result.update(str(item).upper() for item in value if item)
    return result


def load_source_index(path: Path) -> tuple[dict[str, dict[str, Any]], str | None]:
    db = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=60)
    has_causal_view = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='view' AND name='source_events_causal_v1'"
    ).fetchone() is not None
    source_event_relation = (
        "source_events_causal_v1" if has_causal_view else "source_events"
    )
    rows = db.execute(
        f"""SELECT source_event_id,source_id,source_population,event_type,story_cluster_id,
                  effective_from_utc,valid_until_utc,superseded_at_utc,base_currency,
                  quote_currency,payload_json
           FROM {source_event_relation} ORDER BY effective_from_utc,source_event_id"""
    )
    raw_index: dict[str, list[dict[str, Any]]] = {}
    highwater: str | None = None
    for row in rows:
        effective_epoch = parse_epoch(row[5])
        if effective_epoch is None:
            continue
        currencies = {str(value).upper() for value in (row[8], row[9]) if value}
        currencies.update(_payload_currencies(str(row[10] or "")))
        if not currencies:
            continue
        highwater = max(highwater or str(row[5]), str(row[5]))
        event = {
            "source_event_id": str(row[0]), "source_id": str(row[1]),
            "source_population": str(row[2]), "event_type": str(row[3]),
            "story_cluster_id": str(row[4] or ""), "effective_from_utc": str(row[5]),
            "effective_epoch": effective_epoch, "valid_until_epoch": parse_epoch(row[6]),
            "superseded_epoch": parse_epoch(row[7]), "payload_json": str(row[10] or "{}"),
        }
        for currency in currencies:
            raw_index.setdefault(currency, []).append(event)
    db.close()
    index: dict[str, dict[str, Any]] = {}
    for currency, events in raw_index.items():
        events.sort(key=lambda value: (value["effective_epoch"], value["source_event_id"]))
        index[currency] = {"events": events, "epochs": [int(event["effective_epoch"]) for event in events]}
    return index, highwater


def relevant_source_events(
    source_index: Mapping[str, Any], currencies: Iterable[str],
    start_epoch: int, end_epoch: int, causal_at_entry: bool,
) -> list[dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for currency in currencies:
        indexed = source_index.get(str(currency).upper(), [])
        if isinstance(indexed, Mapping):
            events = list(indexed.get("events") or [])
            epochs = list(indexed.get("epochs") or [])
        else:
            events = list(indexed)
            epochs = [int(event["effective_epoch"]) for event in events]
        left = bisect.bisect_left(epochs, start_epoch)
        right = bisect.bisect_right(epochs, end_epoch)
        for event in events[left:right]:
            if causal_at_entry:
                valid_until = event.get("valid_until_epoch")
                superseded = event.get("superseded_epoch")
                if valid_until is not None and int(valid_until) <= end_epoch:
                    continue
                if superseded is not None and int(superseded) <= end_epoch:
                    continue
            found[event["source_event_id"]] = event
    return sorted(found.values(), key=lambda value: (value["effective_epoch"], value["source_event_id"]))


def _directional(event: Mapping[str, Any]) -> bool:
    try:
        payload = json.loads(str(event.get("payload_json") or "{}"))
    except json.JSONDecodeError:
        return False
    raw = payload.get("raw_payload")
    if isinstance(raw, dict):
        return bool(raw.get("directional_evidence") or raw.get("directional_publish_eligible"))
    return bool(payload.get("directional_evidence"))


def map_episode(episode: Mapping[str, Any], index: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    entry = int(episode["entry_epoch"])
    exit_epoch = int(episode["exit_epoch"])
    lookback = int(config.get("pre_entry_news_lookback_min", 360)) * 60
    currencies = [str(episode["base_currency"]), str(episode["quote_currency"])]
    pre = relevant_source_events(index, currencies, entry - lookback, entry, True)
    during = relevant_source_events(index, currencies, entry + 1, exit_epoch, False)
    pre_stories = {event["story_cluster_id"] or event["source_event_id"] for event in pre}
    during_stories = {event["story_cluster_id"] or event["source_event_id"] for event in during}
    families: dict[str, int] = {}
    sources: dict[str, int] = {}
    for event in pre:
        families[event["event_type"]] = families.get(event["event_type"], 0) + 1
        sources[event["source_id"]] = sources.get(event["source_id"], 0) + 1
    return {
        "pre": pre,
        "during": during,
        "summary": {
            "pre_entry_source_count": len(pre),
            "pre_entry_story_count": len(pre_stories),
            "pre_entry_official_count": sum(str(event["source_population"]).startswith("official") for event in pre),
            "pre_entry_directional_count": sum(_directional(event) for event in pre),
            "in_window_source_count": len(during),
            "in_window_story_count": len(during_stories),
            "pre_entry_event_types": dict(sorted(families.items(), key=lambda item: (-item[1], item[0]))),
            "pre_entry_sources": dict(sorted(sources.items(), key=lambda item: (-item[1], item[0]))),
            "in_window_is_ex_post_attribution_only": True,
        },
    }


def insert_episode(db: sqlite3.Connection, contract_id: str, episode: Mapping[str, Any], mapping: Mapping[str, Any], created_utc: str, maximum_links: int) -> bool:
    episode_id = stable_id("movement_episode", contract_id, episode["instrument"], episode["entry_epoch"], episode["horizon_min"])
    summary = mapping["summary"]
    values = (
            episode_id, contract_id, episode["instrument"], episode["base_currency"], episode["quote_currency"],
            episode["entry_utc"], episode["exit_utc"], episode["entry_epoch"], episode["exit_epoch"], episode["horizon_min"],
            episode["selected_side"], episode["signed_currency_factor"], episode["market_episode_id"],
            episode["entry_bid"], episode["entry_ask"], episode["exit_bid"], episode["exit_ask"],
            episode["entry_spread_pips"], episode["exit_spread_pips"], episode["signed_mid_move_pips"],
            episode["gross_magnitude_pips"], episode["endpoint_after_cost_pips"], episode["mfe_pips"], episode["mae_pips"],
            episode["selection_threshold_pips"], episode["selection_quantile"],
            summary["pre_entry_source_count"], summary["pre_entry_story_count"], summary["pre_entry_official_count"],
            summary["pre_entry_directional_count"], summary["in_window_source_count"], summary["in_window_story_count"],
            json.dumps(summary, sort_keys=True, separators=(",", ":")), created_utc,
        )
    cursor = db.execute(
        f"INSERT OR IGNORE INTO movement_episodes VALUES ({','.join('?' for _ in values)})",
        values,
    )
    inserted = bool(cursor.rowcount)
    if not inserted:
        return False
    for relation, events, causal in (
        ("pre_entry_causal", mapping["pre"], 1),
        ("in_window_ex_post", mapping["during"], 0),
    ):
        ranked = sorted(events, key=lambda value: (abs(int(value["effective_epoch"]) - int(episode["entry_epoch"])), value["source_event_id"]))[:maximum_links]
        for event in ranked:
            link_id = stable_id("episode_link", episode_id, event["source_event_id"], relation)
            db.execute(
                "INSERT OR IGNORE INTO episode_source_links VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    link_id, episode_id, event["source_event_id"], relation, event["effective_from_utc"],
                    event["source_id"], event["source_population"], event["event_type"],
                    event["story_cluster_id"] or None, int(event["effective_epoch"]) - int(episode["entry_epoch"]), causal,
                ),
            )
    return True


def run(
    config_path: Path = CONFIG, source_dir: Path = SOURCE, source_events: Path = SOURCE_EVENTS,
    database: Path = DATABASE, output: Path = OUTPUT, report: Path = REPORT,
) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    paths = sorted(source_dir.glob(str(config.get("source_glob") or "*_M1.csv")))
    if not paths:
        raise FileNotFoundError(f"No M1 CSV inputs in {source_dir}")
    price_manifest = [{"name": path.name, "size_bytes": path.stat().st_size, "sha256": digest(path)} for path in paths]
    price_manifest_sha = hashlib.sha256(json.dumps(price_manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    source_index, source_highwater = load_source_index(source_events)
    contract_material = {
        "config_sha256": digest(config_path), "source_code_sha256": digest(Path(__file__)),
        "price_manifest_sha256": price_manifest_sha, "source_event_highwater_utc": source_highwater,
    }
    contract_id = stable_id("movement_news_contract", json.dumps(contract_material, sort_keys=True))
    created = dt.datetime.now(dt.timezone.utc).isoformat()
    db = open_database(database)
    db.execute(
        "INSERT OR IGNORE INTO research_contracts VALUES (?,?,?,?,?,?,?)",
        (contract_id, created, contract_material["config_sha256"], contract_material["source_code_sha256"],
         price_manifest_sha, source_highwater, json.dumps({**config, **contract_material}, sort_keys=True)),
    )
    inserted = 0
    discovered = 0
    summaries: list[dict[str, Any]] = []
    maximum_links = int(config.get("maximum_links_per_relation", 20))
    for path in paths:
        frame, instrument, pip = load_price(path)
        for horizon in config.get("horizons_min") or []:
            episodes = price_episodes(frame, instrument, pip, int(horizon), config)
            discovered += len(episodes)
            for episode in episodes:
                mapping = map_episode(episode, source_index, config)
                inserted += int(insert_episode(db, contract_id, episode, mapping, created, maximum_links))
                summaries.append({**episode, **mapping["summary"]})
        db.commit()
    db.commit()
    total = db.execute("SELECT COUNT(*) FROM movement_episodes WHERE contract_id=?", (contract_id,)).fetchone()[0]
    links = db.execute(
        "SELECT relation,COUNT(*) FROM episode_source_links WHERE episode_id IN (SELECT episode_id FROM movement_episodes WHERE contract_id=?) GROUP BY relation",
        (contract_id,),
    ).fetchall()
    integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
    db.close()
    frame = pd.DataFrame(summaries)
    horizon_rows: list[dict[str, Any]] = []
    if not frame.empty:
        for horizon, group in frame.groupby("horizon_min"):
            horizon_rows.append({
                "horizon_min": int(horizon), "episodes": len(group),
                "average_endpoint_after_cost_pips": float(group["endpoint_after_cost_pips"].mean()),
                "median_endpoint_after_cost_pips": float(group["endpoint_after_cost_pips"].median()),
                "pre_entry_mapping_rate": float(group["pre_entry_source_count"].gt(0).mean()),
                "pre_entry_official_mapping_rate": float(group["pre_entry_official_count"].gt(0).mean()),
                "in_window_mapping_rate": float(group["in_window_source_count"].gt(0).mean()),
            })
    currency_rows: list[dict[str, Any]] = []
    if not frame.empty:
        currencies = sorted(set(frame["base_currency"]) | set(frame["quote_currency"]))
        for currency in currencies:
            group = frame[(frame["base_currency"] == currency) | (frame["quote_currency"] == currency)]
            currency_rows.append({
                "currency": currency, "episodes": len(group),
                "average_endpoint_after_cost_pips": float(group["endpoint_after_cost_pips"].mean()),
                "pre_entry_source_events": int(group["pre_entry_source_count"].sum()),
                "pre_entry_independent_stories": int(group["pre_entry_story_count"].sum()),
                "pre_entry_mapping_rate": float(group["pre_entry_source_count"].gt(0).mean()),
                "in_window_source_events": int(group["in_window_source_count"].sum()),
            })
    payload = {
        "schema_version": 1, "generated_utc": created, "research_id": config["research_id"],
        "contract_id": contract_id, "research_only": True, "execution_eligible": False,
        "can_place_orders": False, "evidence_class": "ex_post_episode_discovery_with_separate_knowledge_time_mapping",
        "instrument_count": len(paths), "episodes_discovered_this_run": discovered,
        "episodes_inserted_this_run": inserted, "contract_episode_count": total,
        "linked_source_rows_by_relation": dict(links), "source_event_highwater_utc": source_highwater,
        "sqlite_integrity": integrity, "horizons": horizon_rows, "currencies": currency_rows,
        "contracts": contract_material,
        "limitations": [
            "movement-selected sample cannot estimate predictive news value by itself",
            "pre-entry mappings are causal context but not proof of causation or direction",
            "in-window source links are ex-post attribution only",
            "source-free and price-only controls are required before any hypothesis is selected",
            "archive is short and already inspected; any candidate requires an untouched cohort",
        ],
    }
    atomic_text(output, json.dumps(payload, indent=2, sort_keys=True))
    lines = [
        "# Movement/news episode research",
        "",
        "Executable movement discovery with strict separation between pre-entry knowledge and in-window attribution. Research only.",
        "",
        f"- Contract: `{contract_id}`",
        f"- Instruments: {len(paths)}",
        f"- Episodes: {total:,}",
        f"- SQLite integrity: `{integrity}`",
        "",
        "| Horizon | Episodes | Avg endpoint net pips | Median | Pre-entry source mapped | Official mapped | In-window mapped (ex-post) |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in horizon_rows:
        lines.append(
            f"| {row['horizon_min']}m | {row['episodes']:,} | {row['average_endpoint_after_cost_pips']:.3f} | "
            f"{row['median_endpoint_after_cost_pips']:.3f} | {row['pre_entry_mapping_rate']:.1%} | "
            f"{row['pre_entry_official_mapping_rate']:.1%} | {row['in_window_mapping_rate']:.1%} |"
        )
    lines += [
        "", "## Currency mapping census", "",
        "| Currency | Episodes | Avg endpoint net pips | Pre-entry source rows | Independent stories | Mapping rate | In-window rows (ex-post) |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in currency_rows:
        lines.append(
            f"| {row['currency']} | {row['episodes']:,} | {row['average_endpoint_after_cost_pips']:.3f} | "
            f"{row['pre_entry_source_events']:,} | {row['pre_entry_independent_stories']:,} | "
            f"{row['pre_entry_mapping_rate']:.1%} | {row['in_window_source_events']:,} |"
        )
    lines += [
        "", "This is a movement-first attribution corpus, not a news backtest. The next valid comparison is source-only vs price-only vs combined vs placebo on all decision timestamps, including non-spike controls.", "",
    ]
    atomic_text(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--source-dir", type=Path, default=SOURCE)
    parser.add_argument("--source-events", type=Path, default=SOURCE_EVENTS)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    run(args.config, args.source_dir, args.source_events, args.database, args.output, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
