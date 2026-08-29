#!/usr/bin/env python3
"""Recover executable OANDA paths for legacy outcome-selected move labels.

The collector is read-only at the broker and writes a separate immutable
research archive.  It never modifies the live candle CSVs, authorizes a
candidate, or touches an account/order endpoint.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import json
import math
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Mapping

from oanda_all68_m1_forward_updater import resolve_readonly_oanda_client
from oanda_spike_blurb_factor_reconstruction import (
    LEGACY_TAGS,
    REPORT_ROOT,
    atomic_text,
    canonical_json,
    file_sha256,
    parse_epoch,
    sha256_bytes,
    utc_now,
)
from oanda_spike_blurb_movement_inventory import DATABASE, stable_id


CONTRACT_ID = "spike_blurb_legacy_price_reacquisition_v1_20260819"
OUTPUT_JSON = REPORT_ROOT / "SPIKE_BLURB_LEGACY_PRICE_REACQUISITION_V1.json"
OUTPUT_MD = REPORT_ROOT / "SPIKE_BLURB_LEGACY_PRICE_REACQUISITION_V1.md"
MAX_WORKERS = 4
RESEARCH_ONLY = 1


def pip_size(instrument: str) -> float:
    return 0.01 if instrument.endswith("_JPY") or instrument in {"USD_THB", "USD_HUF", "EUR_HUF"} else 0.0001


def iso_utc(epoch: int) -> str:
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat()


def oanda_time(epoch: int) -> str:
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat().replace("+00:00", "Z")


def load_labels(path: Path) -> list[dict[str, Any]]:
    labels: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            start = parse_epoch(raw.get("start_utc"))
            end = parse_epoch(raw.get("end_utc"))
            instrument = str(raw.get("instrument") or "").upper()
            move_id = str(raw.get("move_id") or raw.get("movement_key") or "")
            if not move_id or "_" not in instrument or start is None or end is None or end <= start:
                continue
            labels.append(
                {
                    "move_id": move_id,
                    "instrument": instrument,
                    "start_epoch": int(start),
                    "end_epoch": int(end),
                    "start_utc": iso_utc(int(start)),
                    "end_utc": iso_utc(int(end)),
                }
            )
    return sorted(labels, key=lambda row: (row["start_epoch"], row["instrument"], row["move_id"]))


def normalize_candles(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for candle in payload.get("candles") or []:
        if candle.get("complete") is False:
            continue
        epoch = parse_epoch(candle.get("time"))
        bid = candle.get("bid") or {}
        ask = candle.get("ask") or {}
        mid = candle.get("mid") or {}
        try:
            row = {
                "epoch": int(epoch),
                "time": iso_utc(int(epoch)),
                "bid_o": float(bid["o"]), "bid_h": float(bid["h"]),
                "bid_l": float(bid["l"]), "bid_c": float(bid["c"]),
                "ask_o": float(ask["o"]), "ask_h": float(ask["h"]),
                "ask_l": float(ask["l"]), "ask_c": float(ask["c"]),
                "mid_o": float(mid["o"]), "mid_h": float(mid["h"]),
                "mid_l": float(mid["l"]), "mid_c": float(mid["c"]),
                "volume": int(candle.get("volume") or 0),
            }
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if all(math.isfinite(value) for key, value in row.items() if key not in {"epoch", "time", "volume"}):
            output.append(row)
    unique = {row["epoch"]: row for row in output}
    return [unique[epoch] for epoch in sorted(unique)]


def summarize_path(label: Mapping[str, Any], candles: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(candles)
    if not rows:
        return {"coverage_state": "no_candles", "candle_count": 0}
    pip = pip_size(str(label["instrument"]))
    first, last = rows[0], rows[-1]
    entry_ask = float(first["ask_o"])
    entry_bid = float(first["bid_o"])
    exit_bid = float(last["bid_c"])
    exit_ask = float(last["ask_c"])
    long_net = (exit_bid - entry_ask) / pip
    short_net = (entry_bid - exit_ask) / pip
    long_mfe = (max(float(row["bid_h"]) for row in rows) - entry_ask) / pip
    long_mae = (min(float(row["bid_l"]) for row in rows) - entry_ask) / pip
    short_mfe = (entry_bid - min(float(row["ask_l"]) for row in rows)) / pip
    short_mae = (entry_bid - max(float(row["ask_h"]) for row in rows)) / pip
    spreads = [(float(row["ask_c"]) - float(row["bid_c"])) / pip for row in rows]
    first_lag = int(first["epoch"]) - int(label["start_epoch"])
    expected_last = int(label["end_epoch"]) - 60
    last_lag = expected_last - int(last["epoch"])
    state = "exact_window" if abs(first_lag) <= 60 and abs(last_lag) <= 60 else "partial_window"
    return {
        "coverage_state": state,
        "candle_count": len(rows),
        "first_epoch": int(first["epoch"]),
        "last_epoch": int(last["epoch"]),
        "first_utc": str(first["time"]),
        "last_utc": str(last["time"]),
        "entry_bid": entry_bid,
        "entry_ask": entry_ask,
        "exit_bid": exit_bid,
        "exit_ask": exit_ask,
        "average_spread_pips": sum(spreads) / len(spreads),
        "maximum_spread_pips": max(spreads),
        "long_net_pips": long_net,
        "short_net_pips": short_net,
        "best_direction": "long" if long_net >= short_net else "short",
        "best_net_pips": max(long_net, short_net),
        "long_mfe_pips": long_mfe,
        "long_mae_pips": long_mae,
        "short_mfe_pips": short_mfe,
        "short_mae_pips": short_mae,
    }


def fetch_label(client: Any, label: Mapping[str, Any], retries: int = 2) -> dict[str, Any]:
    params = {
        "price": "BAM", "granularity": "M1",
        "from": oanda_time(int(label["start_epoch"])),
        "to": oanda_time(int(label["end_epoch"])),
    }
    payload: Mapping[str, Any] = {}
    for attempt in range(retries + 1):
        payload = client.request("GET", f"/v3/instruments/{label['instrument']}/candles", params=params)
        if not payload.get("_error"):
            break
        if attempt < retries:
            time.sleep(0.5 * (attempt + 1))
    candles = normalize_candles(payload)
    summary = summarize_path(label, candles)
    normalized_payload = canonical_json({"instrument": label["instrument"], "candles": candles}).encode("utf-8")
    result = {
        **label,
        **summary,
        "http_status": int(payload.get("_http_status") or 0),
        "latency_ms": int(payload.get("_latency_ms") or 0),
        "error_text": str(payload.get("_error_text") or payload.get("_exception") or "")[:1000],
        "payload_sha256": sha256_bytes(normalized_payload),
        "payload_gzip": gzip.compress(normalized_payload, compresslevel=6),
    }
    if payload.get("_error"):
        result["coverage_state"] = "request_error"
    return result


WINDOW_COLUMNS = [
    "window_id", "contract_id", "move_id", "instrument", "requested_start_utc", "requested_end_utc",
    "coverage_state", "candle_count", "first_utc", "last_utc", "entry_bid", "entry_ask", "exit_bid",
    "exit_ask", "average_spread_pips", "maximum_spread_pips", "long_net_pips", "short_net_pips",
    "best_direction", "best_net_pips", "long_mfe_pips", "long_mae_pips", "short_mfe_pips",
    "short_mae_pips", "http_status", "latency_ms", "error_text", "payload_sha256", "payload_gzip",
    "research_only",
]


def ensure_schema(database: sqlite3.Connection) -> None:
    database.executescript(
        """
        CREATE TABLE IF NOT EXISTS legacy_price_reacquisition_contracts (
          contract_id TEXT PRIMARY KEY, created_utc TEXT NOT NULL, builder_sha256 TEXT NOT NULL,
          label_file_sha256 TEXT NOT NULL, endpoint TEXT NOT NULL, contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS legacy_executable_price_windows (
          window_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, move_id TEXT NOT NULL,
          instrument TEXT NOT NULL, requested_start_utc TEXT NOT NULL, requested_end_utc TEXT NOT NULL,
          coverage_state TEXT NOT NULL, candle_count INTEGER NOT NULL, first_utc TEXT, last_utc TEXT,
          entry_bid REAL, entry_ask REAL, exit_bid REAL, exit_ask REAL, average_spread_pips REAL,
          maximum_spread_pips REAL, long_net_pips REAL, short_net_pips REAL, best_direction TEXT,
          best_net_pips REAL, long_mfe_pips REAL, long_mae_pips REAL, short_mfe_pips REAL,
          short_mae_pips REAL, http_status INTEGER NOT NULL, latency_ms INTEGER NOT NULL,
          error_text TEXT NOT NULL, payload_sha256 TEXT NOT NULL, payload_gzip BLOB NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1), UNIQUE(contract_id,move_id),
          FOREIGN KEY(contract_id) REFERENCES legacy_price_reacquisition_contracts(contract_id)
        );
        CREATE INDEX IF NOT EXISTS legacy_price_windows_state
          ON legacy_executable_price_windows(contract_id,coverage_state,instrument);
        CREATE TRIGGER IF NOT EXISTS legacy_price_contracts_no_update
          BEFORE UPDATE ON legacy_price_reacquisition_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS legacy_price_contracts_no_delete
          BEFORE DELETE ON legacy_price_reacquisition_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS legacy_price_windows_no_update
          BEFORE UPDATE ON legacy_executable_price_windows BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS legacy_price_windows_no_delete
          BEFORE DELETE ON legacy_executable_price_windows BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def db_row(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "window_id": stable_id("legacy_price_window", CONTRACT_ID, result["move_id"]),
        "contract_id": CONTRACT_ID,
        "move_id": result["move_id"],
        "instrument": result["instrument"],
        "requested_start_utc": result["start_utc"],
        "requested_end_utc": result["end_utc"],
        "coverage_state": result["coverage_state"],
        "candle_count": int(result.get("candle_count") or 0),
        "first_utc": result.get("first_utc"), "last_utc": result.get("last_utc"),
        "entry_bid": result.get("entry_bid"), "entry_ask": result.get("entry_ask"),
        "exit_bid": result.get("exit_bid"), "exit_ask": result.get("exit_ask"),
        "average_spread_pips": result.get("average_spread_pips"),
        "maximum_spread_pips": result.get("maximum_spread_pips"),
        "long_net_pips": result.get("long_net_pips"), "short_net_pips": result.get("short_net_pips"),
        "best_direction": result.get("best_direction"), "best_net_pips": result.get("best_net_pips"),
        "long_mfe_pips": result.get("long_mfe_pips"), "long_mae_pips": result.get("long_mae_pips"),
        "short_mfe_pips": result.get("short_mfe_pips"), "short_mae_pips": result.get("short_mae_pips"),
        "http_status": int(result.get("http_status") or 0), "latency_ms": int(result.get("latency_ms") or 0),
        "error_text": str(result.get("error_text") or ""), "payload_sha256": result["payload_sha256"],
        "payload_gzip": result["payload_gzip"], "research_only": RESEARCH_ONLY,
    }


def insert_row(database: sqlite3.Connection, row: Mapping[str, Any]) -> None:
    marks = ",".join("?" for _ in WINDOW_COLUMNS)
    database.execute(
        f"INSERT OR IGNORE INTO legacy_executable_price_windows ({','.join(WINDOW_COLUMNS)}) VALUES ({marks})",
        [row.get(column) for column in WINDOW_COLUMNS],
    )


def snapshot(database_path: Path, *, attempted: int, reused: bool) -> dict[str, Any]:
    database = sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True)
    states = dict(database.execute(
        "SELECT coverage_state,COUNT(*) FROM legacy_executable_price_windows WHERE contract_id=? GROUP BY coverage_state",
        (CONTRACT_ID,),
    ).fetchall())
    metrics = database.execute(
        """SELECT COUNT(*),COUNT(DISTINCT instrument),AVG(best_net_pips),
                  AVG(average_spread_pips),AVG(latency_ms)
             FROM legacy_executable_price_windows
            WHERE contract_id=? AND coverage_state='exact_window'""",
        (CONTRACT_ID,),
    ).fetchone()
    integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
    database.close()
    total = sum(int(value) for value in states.values())
    result = {
        "schema_version": 1, "generated_utc": utc_now(), "contract_id": CONTRACT_ID,
        "attempted_this_run": attempted, "stored_total": total, "coverage_states": states,
        "exact_window_count": int(metrics[0] or 0), "instrument_count": int(metrics[1] or 0),
        "average_hindsight_best_net_pips": float(metrics[2]) if metrics[2] is not None else None,
        "average_spread_pips": float(metrics[3]) if metrics[3] is not None else None,
        "average_request_latency_ms": float(metrics[4]) if metrics[4] is not None else None,
        "sqlite_integrity": integrity, "resumed_existing_contract": reused,
        "evidence_class": "outcome_selected_attribution_diagnostic_not_forecast_proof",
        "research_only": True, "execution_eligible": False, "supported_execution_decision": "no_trade",
    }
    result["snapshot_sha256"] = sha256_bytes(canonical_json(result).encode("utf-8"))
    return result


def render_report(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Legacy Spike/Blurb Executable-Price Reacquisition V1", "",
            f"- Generated: `{report['generated_utc']}`",
            f"- Contract: `{report['contract_id']}`",
            f"- Snapshot SHA-256: `{report['snapshot_sha256']}`",
            f"- Stored labels: **{report['stored_total']:,}**",
            f"- Exact executable windows: **{report['exact_window_count']:,}**",
            f"- Instruments: **{report['instrument_count']:,}**",
            f"- Coverage states: `{canonical_json(report['coverage_states'])}`",
            "",
            "These are outcome-selected diagnostic windows. The restored direction and magnitude may explain historical moves but cannot serve as prospective forecast evidence.",
            "",
            "Safety: research-only, execution-ineligible, practice read-only candle endpoint, supported decision `no_trade`.", "",
        ]
    )


def run(
    *, labels_path: Path = LEGACY_TAGS, database_path: Path = DATABASE,
    limit: int = 0, workers: int = MAX_WORKERS,
) -> dict[str, Any]:
    labels = load_labels(labels_path)
    builder_sha = file_sha256(Path(__file__).resolve())
    label_sha = file_sha256(labels_path)
    database = sqlite3.connect(database_path, timeout=120)
    database.execute("PRAGMA journal_mode=WAL")
    database.execute("PRAGMA synchronous=FULL")
    database.execute("PRAGMA foreign_keys=ON")
    ensure_schema(database)
    existing = database.execute(
        "SELECT builder_sha256,label_file_sha256 FROM legacy_price_reacquisition_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    reused = existing is not None
    if existing and tuple(existing) != (builder_sha, label_sha):
        database.close()
        raise RuntimeError("immutable_legacy_price_contract_collision")
    if not existing:
        contract = {
            "contract_id": CONTRACT_ID, "created_utc": utc_now(), "builder_sha256": builder_sha,
            "label_file_sha256": label_sha, "endpoint": "https://api-fxpractice.oanda.com/v3/instruments/*/candles",
            "contract_json": canonical_json({"price":"BAM","granularity":"M1","research_only":True,"order_endpoints":False}),
        }
        with database:
            database.execute(
                "INSERT INTO legacy_price_reacquisition_contracts VALUES (?,?,?,?,?,?)", tuple(contract.values())
            )
    done = {row[0] for row in database.execute(
        "SELECT move_id FROM legacy_executable_price_windows WHERE contract_id=?", (CONTRACT_ID,)
    )}
    pending = [label for label in labels if label["move_id"] not in done]
    if limit > 0:
        pending = pending[:limit]
    database.close()
    if not pending:
        return snapshot(database_path, attempted=0, reused=reused)
    client, meta = resolve_readonly_oanda_client()
    if meta.get("environment") != "practice" or "api-fxpractice.oanda.com" not in meta.get("base_url", ""):
        raise RuntimeError("practice_readonly_endpoint_required")
    attempted = 0
    with ThreadPoolExecutor(max_workers=max(1, min(int(workers), 8))) as executor:
        futures = {executor.submit(fetch_label, client, label): label for label in pending}
        database = sqlite3.connect(database_path, timeout=120)
        database.execute("PRAGMA foreign_keys=ON")
        for future in as_completed(futures):
            row = db_row(future.result())
            with database:
                insert_row(database, row)
            attempted += 1
            if attempted % 100 == 0:
                print(f"[legacy-price] {attempted}/{len(pending)}", flush=True)
        database.close()
    return snapshot(database_path, attempted=attempted, reused=reused)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--labels", type=Path, default=LEGACY_TAGS)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--output-json", type=Path, default=OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run(labels_path=args.labels, database_path=args.database, limit=args.limit, workers=args.workers)
    atomic_text(args.output_json, json.dumps(report, indent=2, sort_keys=True) + "\n")
    atomic_text(args.output_md, render_report(report))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
