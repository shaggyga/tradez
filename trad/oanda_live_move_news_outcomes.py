#!/usr/bin/env python3
"""Mature quote-bound live mover/news shadow arms against executable M1 paths.

The input cases were frozen when a move first appeared in the live mover/news
diagnostic.  This worker evaluates later bid/ask paths only; it has no broker,
authorization, promotion, or execution surface.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import json
import math
import sqlite3
import statistics
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_live_move_news_snapshot as live
import oanda_major_move_gap_census as census


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
DEFAULT_DATABASE = STATE / "live_move_news_cases_v1.sqlite"
DEFAULT_CANDLES = DATA / "candles"
DEFAULT_OUTPUT = STATE / "live_move_news_outcomes_v1.json"
DEFAULT_REPORT = DATA / "reports" / "live_move_news" / "LIVE_MOVE_NEWS_OUTCOMES_CURRENT.md"
CONTRACT_ID = "live_move_news_forward_outcomes_v1_20260824"
CASE_CONTRACT_ID = live.CONTRACT_ID
HORIZONS_MIN = (5, 15, 30, 60)
CSV_FIELDS = (
    "time", "datetime", "instrument", "granularity", "open", "high", "low", "close",
    "bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low",
    "ask_close", "spread_pips", "volume",
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(path)


def connect(path: Path) -> sqlite3.Connection:
    connection = live.connect_history(path)
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS mover_case_outcomes (
            outcome_id TEXT PRIMARY KEY,
            matured_utc TEXT NOT NULL,
            case_id TEXT NOT NULL,
            case_contract_id TEXT NOT NULL,
            arm TEXT NOT NULL,
            horizon_min INTEGER NOT NULL,
            side INTEGER NOT NULL,
            target_utc TEXT NOT NULL,
            candle_utc TEXT NOT NULL,
            after_cost_pips REAL NOT NULL,
            outcome_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_mover_case_outcomes_case
            ON mover_case_outcomes(case_contract_id,case_id,arm,horizon_min);
        CREATE TRIGGER IF NOT EXISTS mover_case_outcomes_no_update
            BEFORE UPDATE ON mover_case_outcomes BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS mover_case_outcomes_no_delete
            BEFORE DELETE ON mover_case_outcomes BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


def load_cases(
    connection: sqlite3.Connection, *, case_contract_id: str = CASE_CONTRACT_ID
) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT case_id,case_json FROM mover_cases "
        "WHERE json_extract(case_json,'$.contract_id')=? ORDER BY first_recorded_utc,case_id",
        (case_contract_id,),
    ).fetchall()
    output: list[dict[str, Any]] = []
    for case_id, payload in rows:
        try:
            row = json.loads(str(payload))
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        row["case_id"] = str(case_id)
        output.append(row)
    return output


def read_tail_candles(path: Path, maximum_bytes: int = 1_500_000) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    size = path.stat().st_size
    with path.open("rb") as stream:
        start = max(0, size - max(4096, int(maximum_bytes)))
        stream.seek(start)
        payload = stream.read()
    text = payload.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if start > 0 and lines:
        lines = lines[1:]
    rows: list[dict[str, Any]] = []
    for values in csv.reader(lines):
        if len(values) != len(CSV_FIELDS) or values[0] == "time":
            continue
        row = dict(zip(CSV_FIELDS, values))
        epoch = census.parse_epoch(row.get("datetime") or row.get("time"))
        if epoch is None:
            continue
        row["open_epoch"] = int(epoch)
        row["close_epoch"] = int(epoch) + 60
        rows.append(row)
    return rows


def finite(row: Mapping[str, Any], key: str) -> float | None:
    try:
        value = float(row.get(key))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def mature_path(
    case: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    *,
    arm: str,
    side: int,
    horizon_min: int,
    close_epochs: Sequence[int] | None = None,
    outcome_contract_id: str = CONTRACT_ID,
) -> dict[str, Any] | None:
    if side not in {-1, 1} or not case.get("entry_quote_fresh"):
        return None
    observed = census.parse_epoch(case.get("observation_utc"))
    bid = finite(case, "entry_bid")
    ask = finite(case, "entry_ask")
    pip = finite(case, "entry_pip")
    if observed is None or bid is None or ask is None or pip is None or pip <= 0.0:
        return None
    target = int(observed + int(horizon_min) * 60)
    epochs = (
        list(close_epochs)
        if close_epochs is not None
        else [int(row.get("close_epoch") or 0) for row in candles]
    )
    target_index = bisect.bisect_left(epochs, target)
    if target_index >= len(candles):
        return None
    target_row = candles[target_index]
    path_start = bisect.bisect_right(epochs, observed)
    path = candles[path_start : target_index + 1]
    if not path:
        return None
    if side > 0:
        exit_price = finite(target_row, "bid_close")
        signed = (exit_price - ask) / pip if exit_price is not None else None
        favorable = [
            (value - ask) / pip for value in (finite(row, "bid_high") for row in path)
            if value is not None
        ]
        adverse = [
            (value - ask) / pip for value in (finite(row, "bid_low") for row in path)
            if value is not None
        ]
        entry_price = ask
    else:
        exit_price = finite(target_row, "ask_close")
        signed = (bid - exit_price) / pip if exit_price is not None else None
        favorable = [
            (bid - value) / pip for value in (finite(row, "ask_low") for row in path)
            if value is not None
        ]
        adverse = [
            (bid - value) / pip for value in (finite(row, "ask_high") for row in path)
            if value is not None
        ]
        entry_price = bid
    if signed is None or not favorable or not adverse:
        return None
    candle_epoch = int(target_row["close_epoch"])
    return {
        "contract_id": outcome_contract_id,
        "case_id": str(case.get("case_id") or ""),
        "case_contract_id": str(case.get("contract_id") or ""),
        "instrument": str(case.get("instrument") or ""),
        "factor_episode_id": str(case.get("factor_episode_id") or ""),
        "factor_representative": bool(case.get("factor_representative")),
        "explanation_state": str(case.get("explanation_state") or ""),
        "arm": arm,
        "side": int(side),
        "horizon_min": int(horizon_min),
        "observation_utc": str(case.get("observation_utc") or ""),
        "target_utc": census.iso_epoch(target),
        "candle_utc": census.iso_epoch(candle_epoch),
        "maturation_clock_lag_sec": int(candle_epoch - target),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "after_cost_pips": round(float(signed), 6),
        "mfe_pips": round(max(favorable), 6),
        "mae_pips": round(min(adverse), 6),
        "execution_eligible": False,
        "research_only": True,
    }


def outcome_id(row: Mapping[str, Any]) -> str:
    import hashlib

    material = "|".join(
        str(row.get(key) or "")
        for key in ("contract_id", "case_id", "arm", "horizon_min")
    )
    return "live_move_outcome_" + hashlib.sha256(material.encode()).hexdigest()[:32]


def archive_coverage(
    cases: Sequence[Mapping[str, Any]],
    candle_cache: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    generated_epoch: int,
) -> dict[str, Any]:
    """Explain evidence latency without treating missing future paths as losses."""

    latest_by_instrument: dict[str, int] = {}
    for instrument, candles in candle_cache.items():
        latest = max(
            (int(row.get("close_epoch") or 0) for row in candles),
            default=0,
        )
        if latest > 0:
            latest_by_instrument[str(instrument)] = latest
    lags = [max(0, generated_epoch - epoch) for epoch in latest_by_instrument.values()]
    potential = 0
    pending_archive = 0
    for case in cases:
        if not case.get("entry_quote_fresh"):
            continue
        observed = census.parse_epoch(case.get("observation_utc"))
        arms = case.get("forward_shadow_arms") or {}
        if observed is None or not isinstance(arms, Mapping):
            continue
        latest = latest_by_instrument.get(str(case.get("instrument") or ""), 0)
        for raw_side in arms.values():
            try:
                side = int(raw_side)
            except (TypeError, ValueError):
                continue
            if side not in {-1, 1}:
                continue
            for horizon in HORIZONS_MIN:
                potential += 1
                if latest < int(observed + horizon * 60):
                    pending_archive += 1
    return {
        "instrument_count": len(latest_by_instrument),
        "latest_candle_utc": (
            census.iso_epoch(max(latest_by_instrument.values()))
            if latest_by_instrument else None
        ),
        "oldest_instrument_latest_candle_utc": (
            census.iso_epoch(min(latest_by_instrument.values()))
            if latest_by_instrument else None
        ),
        "median_archive_lag_sec": (
            round(float(statistics.median(lags)), 3) if lags else None
        ),
        "maximum_archive_lag_sec": max(lags) if lags else None,
        "potential_arm_horizon_count": potential,
        "pending_archive_arm_horizon_count": pending_archive,
    }


def summarize(
    connection: sqlite3.Connection, *, case_contract_id: str = CASE_CONTRACT_ID
) -> list[dict[str, Any]]:
    rows = connection.execute(
        "WITH joined AS ("
        " SELECT o.outcome_id,o.case_id,o.arm,o.horizon_min,o.after_cost_pips,o.outcome_json,"
        "        c.first_recorded_utc,"
        "        COALESCE(NULLIF(json_extract(c.case_json,'$.factor_episode_id'),''),o.case_id) AS factor_episode_id,"
        "        CASE WHEN json_extract(c.case_json,'$.factor_representative')=1 THEN 1 ELSE 0 END AS stored_representative,"
        "        ABS(CAST(json_extract(c.case_json,'$.move_bps') AS REAL)) AS absolute_move_bps,"
        "        CAST(json_extract(c.case_json,'$.entry_spread_pips') AS REAL) AS spread_pips"
        " FROM mover_case_outcomes o JOIN mover_cases c ON c.case_id=o.case_id"
        " WHERE o.case_contract_id=?"
        "), bucketed AS ("
        " SELECT *,CASE WHEN spread_pips IS NULL OR spread_pips<=0 THEN 'unknown'"
        "  WHEN spread_pips<=2 THEN 'liquid_le_2p'"
        "  WHEN spread_pips<=5 THEN 'moderate_2_5p'"
        "  WHEN spread_pips<=15 THEN 'wide_5_15p' ELSE 'very_wide_gt_15p' END AS cost_bucket"
        " FROM joined), ranked AS ("
        " SELECT *,ROW_NUMBER() OVER ("
        "  PARTITION BY arm,horizon_min,cost_bucket,factor_episode_id"
        "  ORDER BY first_recorded_utc ASC,stored_representative DESC,absolute_move_bps DESC,case_id ASC"
        " ) AS factor_rank FROM bucketed)"
        " SELECT arm,horizon_min,cost_bucket,COUNT(*),"
        " SUM(CASE WHEN after_cost_pips>0 THEN 1 ELSE 0 END),AVG(after_cost_pips),"
        " AVG(CASE WHEN spread_pips>0 THEN after_cost_pips/spread_pips END),"
        " SUM(CASE WHEN factor_rank=1 THEN 1 ELSE 0 END),"
        " SUM(CASE WHEN factor_rank=1 AND after_cost_pips>0 THEN 1 ELSE 0 END),"
        " AVG(CASE WHEN factor_rank=1 THEN after_cost_pips END),"
        " AVG(CASE WHEN factor_rank=1 AND spread_pips>0"
        "          THEN after_cost_pips/spread_pips END)"
        " FROM ranked GROUP BY arm,horizon_min,cost_bucket"
        " ORDER BY arm,horizon_min,cost_bucket",
        (case_contract_id,),
    ).fetchall()
    return [
        {
            "arm": str(arm),
            "horizon_min": int(horizon),
            "cost_bucket": str(cost_bucket),
            "raw_n": int(count),
            "win_rate": round(float(wins) / int(count), 6) if count else None,
            "average_after_cost_pips": round(float(average), 6),
            "average_after_cost_spread_multiple": (
                round(float(average_spread_multiple), 6)
                if average_spread_multiple is not None else None
            ),
            "factor_representative_n": int(factor_n),
            "factor_representative_win_rate": (
                round(float(factor_wins) / int(factor_n), 6) if factor_n else None
            ),
            "factor_representative_average_after_cost_pips": (
                round(float(factor_average), 6) if factor_average is not None else None
            ),
            "factor_representative_average_after_cost_spread_multiple": (
                round(float(factor_average_spread_multiple), 6)
                if factor_average_spread_multiple is not None else None
            ),
        }
        for (
            arm, horizon, cost_bucket, count, wins, average,
            average_spread_multiple, factor_n, factor_wins, factor_average,
            factor_average_spread_multiple,
        ) in rows
    ]


def run(
    *,
    database: Path = DEFAULT_DATABASE,
    candle_root: Path = DEFAULT_CANDLES,
    output: Path = DEFAULT_OUTPUT,
    report: Path = DEFAULT_REPORT,
    case_contract_id: str = CASE_CONTRACT_ID,
    outcome_contract_id: str = CONTRACT_ID,
) -> dict[str, Any]:
    generated = utc_now()
    generated_epoch = int(census.parse_epoch(generated) or time.time())
    connection = connect(database)
    inserted = 0
    skipped_existing = 0
    try:
        cases = load_cases(connection, case_contract_id=case_contract_id)
        existing_outcomes = {
            (str(case_id), str(arm), int(horizon_min))
            for case_id, arm, horizon_min in connection.execute(
                """
                SELECT case_id, arm, horizon_min
                FROM mover_case_outcomes
                WHERE case_contract_id = ?
                """,
                (case_contract_id,),
            ).fetchall()
        }
        candle_cache: dict[str, list[dict[str, Any]]] = {}
        candle_epoch_cache: dict[str, list[int]] = {}
        for case in cases:
            instrument = str(case.get("instrument") or "")
            if instrument not in candle_cache:
                candle_cache[instrument] = read_tail_candles(
                    candle_root / f"{instrument}_M1.csv"
                )
                candle_epoch_cache[instrument] = [
                    int(row.get("close_epoch") or 0)
                    for row in candle_cache[instrument]
                ]
            arms = case.get("forward_shadow_arms") or {}
            if not isinstance(arms, Mapping):
                continue
            for arm, raw_side in arms.items():
                try:
                    side = int(raw_side)
                except (TypeError, ValueError):
                    continue
                for horizon in HORIZONS_MIN:
                    outcome_key = (
                        str(case.get("case_id") or ""),
                        str(arm),
                        int(horizon),
                    )
                    if outcome_key in existing_outcomes:
                        skipped_existing += 1
                        continue
                    matured = mature_path(
                        case, candle_cache[instrument], arm=str(arm), side=side,
                        horizon_min=horizon,
                        close_epochs=candle_epoch_cache[instrument],
                        outcome_contract_id=outcome_contract_id,
                    )
                    if matured is None:
                        continue
                    identifier = outcome_id(matured)
                    before = connection.total_changes
                    connection.execute(
                        "INSERT OR IGNORE INTO mover_case_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            identifier, generated, matured["case_id"], case_contract_id,
                            matured["arm"], matured["horizon_min"], matured["side"],
                            matured["target_utc"], matured["candle_utc"],
                            matured["after_cost_pips"], json.dumps(matured, sort_keys=True),
                        ),
                    )
                    inserted += int(connection.total_changes > before)
                    existing_outcomes.add(outcome_key)
        connection.commit()
        total = int(
            connection.execute(
                "SELECT COUNT(*) FROM mover_case_outcomes WHERE case_contract_id=?",
                (case_contract_id,),
            ).fetchone()[0]
        )
        cells = summarize(connection, case_contract_id=case_contract_id)
        coverage = archive_coverage(
            cases, candle_cache, generated_epoch=generated_epoch
        )
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()
    payload = {
        "schema_version": 1,
        "contract_id": outcome_contract_id,
        "case_contract_id": case_contract_id,
        "generated_utc": generated,
        "case_count": len(cases),
        "inserted_outcome_count": inserted,
        "skipped_existing_outcome_count": skipped_existing,
        "retained_outcome_count": total,
        "cells": cells,
        "candle_archive": coverage,
        "sqlite_integrity": integrity,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "supported_decision": "collect_forward_outcomes",
        "factor_deduplication": (
            "earliest_case_per_factor_episode_arm_horizon_cost_bucket; "
            "stored_representative_breaks_same_clock_ties_only"
        ),
    }
    atomic_json(output, payload)
    lines = [
        "# Live mover/news forward outcomes", "", f"Generated: `{generated}`", "",
        (
            f"Cases: **{len(cases)}**; retained outcomes: **{total}**; "
            f"inserted: **{inserted}**; existing paths skipped before candle "
            f"evaluation: **{skipped_existing}**."
        ), "",
        (
            "Executable M1 archive: "
            f"**{coverage['instrument_count']}** instruments; median lag "
            f"**{coverage['median_archive_lag_sec']}s**; pending archive paths "
            f"**{coverage['pending_archive_arm_horizon_count']} / "
            f"{coverage['potential_arm_horizon_count']}**."
        ), "",
        "| Arm | Horizon | Cost bucket | Raw N | Raw win | Raw avg net | Raw net/spread | Factor N | Factor win | Factor avg net | Factor net/spread |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for cell in cells:
        lines.append(
            f"| {cell['arm']} | {cell['horizon_min']}m | {cell['cost_bucket']} | "
            f"{cell['raw_n']} | {cell['win_rate']} | "
            f"{cell['average_after_cost_pips']} | "
            f"{cell['average_after_cost_spread_multiple']} | "
            f"{cell['factor_representative_n']} | "
            f"{cell['factor_representative_win_rate']} | "
            f"{cell['factor_representative_average_after_cost_pips']} | "
            f"{cell['factor_representative_average_after_cost_spread_multiple']} |"
        )
    lines.extend([
        "",
        (
            "Research-only. Factor rows use the earliest case per signed-currency "
            "episode, arm, horizon, and cost bucket; a later representative change "
            "cannot increase N."
        ),
        "",
    ])
    atomic_text(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--candles", type=Path, default=DEFAULT_CANDLES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.time()
    while True:
        payload = run(
            database=args.database, candle_root=args.candles,
            output=args.output, report=args.report,
        )
        print(json.dumps({
            "generated_utc": payload["generated_utc"],
            "case_count": payload["case_count"],
            "retained_outcome_count": payload["retained_outcome_count"],
        }), flush=True)
        if args.interval_sec <= 0 or (
            args.duration_sec > 0 and time.time() - started >= args.duration_sec
        ):
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
