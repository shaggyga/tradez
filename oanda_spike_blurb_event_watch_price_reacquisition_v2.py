#!/usr/bin/env python3
"""Reacquire all-68 executable M1 paths for exact verified event watches (V2).

Only OANDA's practice instrument-candle GET endpoint is used.  The resulting
payloads are immutable retrospective research evidence and cannot authorize,
promote, or execute anything.
"""

from __future__ import annotations

import argparse
import ast
import gzip
import hashlib
import json
import math
import os
import re
import sqlite3
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from oanda_spike_blurb_factor_reconstruction import CONFIG as RECONSTRUCTION_CONFIG, load_contract
from oanda_spike_blurb_verified_source_cases_v1 import (
    CONTRACT_ID as SOURCE_CASE_CONTRACT_ID,
    DATABASE,
    canonical_json,
    parse_utc,
    sha256_bytes,
    sha256_file,
    stable_id,
    utc_now,
)


CONTRACT_ID = "spike_blurb_event_watch_price_reacquisition_v2_20260820"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "event_watch_price_reacquisition_v2"
)
FREEZE_UTC = "2026-08-20T16:30:00+00:00"
SCHEMA_VERSION = 1
PRE_WATCH_MINUTES = 5
POST_EPISODE_MINUTES = 10
MAX_WORKERS = 4


class ReadonlyPracticeCandleClient:
    """Minimal client whose request surface is only practice instrument candles."""

    def __init__(self, token: str, timeout: float = 30.0):
        if not token:
            raise RuntimeError("practice_oanda_token_missing")
        self.token = token
        self.base_url = "https://api-fxpractice.oanda.com"
        self.timeout = timeout

    def request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        if method != "GET" or not path.startswith("/v3/instruments/") or not path.endswith("/candles"):
            raise RuntimeError("readonly_client_rejects_non_candle_request")
        params = kwargs.get("params") or {}
        url = self.base_url + path + ("?" + urlencode(params) if params else "")
        request = Request(url, headers={"Authorization": f"Bearer {self.token}"}, method="GET")
        started = time.time()
        try:
            with urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
                payload = json.loads(raw.decode("utf-8")) if raw else {}
                payload["_http_status"] = int(response.status)
                payload["_latency_ms"] = int((time.time() - started) * 1000)
                return payload
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            return {
                "_error": True, "_http_status": int(exc.code), "_error_text": raw[:1000],
                "_latency_ms": int((time.time() - started) * 1000),
            }
        except (URLError, OSError, ValueError, json.JSONDecodeError) as exc:
            return {
                "_error": True, "_exception": repr(exc),
                "_latency_ms": int((time.time() - started) * 1000),
            }


def resolve_readonly_oanda_client() -> tuple[ReadonlyPracticeCandleClient, dict[str, str]]:
    creds_path = Path(os.environ.get("OANDA_CREDS_PATH", str(Path(__file__).resolve().parent / "creds")))
    credentials: dict[str, Any] = {}
    if creds_path.is_file():
        for raw in creds_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            name = name.strip()
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                continue
            try:
                credentials[name] = ast.literal_eval(value.strip())
            except (ValueError, SyntaxError):
                credentials[name] = value.strip().strip('"').strip("'")
    token = next((
        str(credentials.get(name) or os.environ.get(name) or "")
        for name in ("OANDA_API_KEY", "OANDA_ACCESS_TOKEN", "OANDA_TOKEN", "OANDA_API_TOKEN")
        if credentials.get(name) or os.environ.get(name)
    ), "")
    return ReadonlyPracticeCandleClient(token), {
        "environment": "practice", "base_url": "https://api-fxpractice.oanda.com"
    }


def pip_size(instrument: str) -> float:
    return 0.01 if instrument.endswith("_JPY") or instrument in {"USD_THB", "USD_HUF", "EUR_HUF"} else 0.0001


def oanda_time(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_candles(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    output: dict[int, dict[str, Any]] = {}
    for candle in payload.get("candles") or []:
        if candle.get("complete") is False:
            continue
        try:
            timestamp = datetime.fromisoformat(str(candle["time"]).replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                raise ValueError("candle_clock_missing_timezone")
            epoch = int(timestamp.timestamp())
            bid = candle["bid"]
            ask = candle["ask"]
            mid = candle["mid"]
            row = {
                "epoch": epoch,
                "time": datetime.fromtimestamp(epoch, timezone.utc).isoformat(),
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
        if all(
            math.isfinite(float(value))
            for key, value in row.items()
            if key not in {"epoch", "time", "volume"}
        ):
            output[epoch] = row
    return [output[epoch] for epoch in sorted(output)]


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS verified_event_price_contracts (
          contract_id TEXT PRIMARY KEY,
          source_case_contract_id TEXT NOT NULL,
          contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL,
          source_snapshot_sha256 TEXT NOT NULL,
          universe_sha256 TEXT NOT NULL,
          endpoint TEXT NOT NULL,
          frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS verified_event_price_windows (
          window_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          case_id TEXT NOT NULL,
          instrument TEXT NOT NULL,
          requested_start_utc TEXT NOT NULL,
          requested_end_utc TEXT NOT NULL,
          coverage_state TEXT NOT NULL,
          candle_count INTEGER NOT NULL,
          first_utc TEXT,
          last_utc TEXT,
          average_spread_pips REAL,
          maximum_spread_pips REAL,
          minimum_mid REAL,
          maximum_mid REAL,
          http_status INTEGER NOT NULL,
          latency_ms INTEGER NOT NULL,
          error_text TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL,
          payload_gzip BLOB NOT NULL,
          evidence_class TEXT NOT NULL,
          outcome_selected INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,case_id,instrument),
          FOREIGN KEY(contract_id) REFERENCES verified_event_price_contracts(contract_id)
        );
        CREATE INDEX IF NOT EXISTS verified_event_price_windows_state
          ON verified_event_price_windows(contract_id,case_id,coverage_state,instrument);
        CREATE TRIGGER IF NOT EXISTS verified_event_price_contracts_no_update
          BEFORE UPDATE ON verified_event_price_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_event_price_contracts_no_delete
          BEFORE DELETE ON verified_event_price_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_event_price_windows_no_update
          BEFORE UPDATE ON verified_event_price_windows BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_event_price_windows_no_delete
          BEFORE DELETE ON verified_event_price_windows BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def iso_utc(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def source_snapshot(connection: sqlite3.Connection) -> tuple[list[dict[str, Any]], str]:
    cases = []
    for raw in connection.execute(
        """
        SELECT case_id,response_watch_eligible_from_utc,case_json,row_sha256
        FROM verified_external_source_cases
        WHERE contract_id=? AND response_watch_eligible=1
        ORDER BY case_id
        """,
        (SOURCE_CASE_CONTRACT_ID,),
    ):
        case_id, watch_utc, case_json, row_hash = raw
        end_row = connection.execute(
            """
            SELECT max(representative_end_utc),count(*)
            FROM verified_external_source_episode_links
            WHERE contract_id=? AND case_id=?
            """,
            (SOURCE_CASE_CONTRACT_ID, case_id),
        ).fetchone()
        if end_row is None or end_row[0] is None or int(end_row[1]) <= 0:
            raise RuntimeError(f"verified_case_episode_links_missing:{case_id}")
        watch = parse_utc(watch_utc, "watch_utc")
        episode_end = parse_utc(end_row[0], "episode_end")
        assert watch is not None and episode_end is not None
        start = watch - timedelta(minutes=PRE_WATCH_MINUTES)
        end = episode_end + timedelta(minutes=POST_EPISODE_MINUTES)
        if end <= start:
            raise RuntimeError(f"invalid_event_window:{case_id}")
        cases.append({
            "case_id": str(case_id),
            "watch_utc": watch.isoformat(),
            "start_epoch": int(start.timestamp()),
            "end_epoch": int(end.timestamp()),
            "start_utc": start.isoformat(),
            "end_utc": end.isoformat(),
            "source_case_sha256": str(row_hash),
            "source_case_json_sha256": sha256_bytes(str(case_json).encode()),
            "linked_episode_count": int(end_row[1]),
        })
    if len(cases) != 2:
        raise RuntimeError(f"unexpected_exact_watch_case_count:{len(cases)}")
    payload = canonical_json(cases)
    return cases, sha256_bytes(payload.encode())


def request_jobs(cases: list[dict[str, Any]], instruments: list[str]) -> list[dict[str, Any]]:
    if len(instruments) != 68 or len(set(instruments)) != 68:
        raise ValueError("exact_68_instrument_universe_required")
    return [
        {**case, "instrument": instrument}
        for case in cases
        for instrument in instruments
    ]


def summarize(job: Mapping[str, Any], candles: list[dict[str, Any]]) -> dict[str, Any]:
    if not candles:
        return {
            "coverage_state": "no_candles", "candle_count": 0, "first_utc": None,
            "last_utc": None, "average_spread_pips": None, "maximum_spread_pips": None,
            "minimum_mid": None, "maximum_mid": None,
        }
    pip = pip_size(str(job["instrument"]))
    spreads = [(float(row["ask_c"]) - float(row["bid_c"])) / pip for row in candles]
    mids = [float(row["mid_c"]) for row in candles]
    expected_last = int(job["end_epoch"]) - 60
    first_lag = int(candles[0]["epoch"]) - int(job["start_epoch"])
    last_lag = expected_last - int(candles[-1]["epoch"])
    coverage = "exact_window" if abs(first_lag) <= 60 and abs(last_lag) <= 60 else "partial_window"
    return {
        "coverage_state": coverage,
        "candle_count": len(candles),
        "first_utc": candles[0]["time"],
        "last_utc": candles[-1]["time"],
        "average_spread_pips": sum(spreads) / len(spreads),
        "maximum_spread_pips": max(spreads),
        "minimum_mid": min(mids),
        "maximum_mid": max(mids),
    }


def fetch_job(client: Any, job: Mapping[str, Any], retries: int = 2) -> dict[str, Any]:
    params = {
        "price": "BAM",
        "granularity": "M1",
        "from": oanda_time(int(job["start_epoch"])),
        "to": oanda_time(int(job["end_epoch"])),
    }
    payload: Mapping[str, Any] = {}
    for attempt in range(retries + 1):
        payload = client.request("GET", f"/v3/instruments/{job['instrument']}/candles", params=params)
        if not payload.get("_error"):
            break
        if attempt < retries:
            time.sleep(0.5 * (attempt + 1))
    candles = normalize_candles(payload)
    normalized = canonical_json({
        "case_id": job["case_id"], "instrument": job["instrument"], "candles": candles
    }).encode()
    result = {
        **job,
        **summarize(job, candles),
        "http_status": int(payload.get("_http_status") or 0),
        "latency_ms": int(payload.get("_latency_ms") or 0),
        "error_text": str(payload.get("_error_text") or payload.get("_exception") or "")[:1000],
        "payload_sha256": sha256_bytes(normalized),
        "payload_gzip": gzip.compress(normalized, compresslevel=6),
    }
    if payload.get("_error"):
        result["coverage_state"] = "request_error"
    return result


WINDOW_COLUMNS = [
    "window_id", "contract_id", "case_id", "instrument", "requested_start_utc", "requested_end_utc",
    "coverage_state", "candle_count", "first_utc", "last_utc", "average_spread_pips",
    "maximum_spread_pips", "minimum_mid", "maximum_mid", "http_status", "latency_ms", "error_text",
    "payload_sha256", "payload_gzip", "evidence_class", "outcome_selected", "forecast_proof_eligible",
    "research_only", "execution_eligible",
]


def row_for(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "window_id": stable_id("verified_event_price", CONTRACT_ID, result["case_id"], result["instrument"]),
        "contract_id": CONTRACT_ID,
        "case_id": result["case_id"],
        "instrument": result["instrument"],
        "requested_start_utc": result["start_utc"],
        "requested_end_utc": result["end_utc"],
        "coverage_state": result["coverage_state"],
        "candle_count": int(result["candle_count"]),
        "first_utc": result["first_utc"],
        "last_utc": result["last_utc"],
        "average_spread_pips": result["average_spread_pips"],
        "maximum_spread_pips": result["maximum_spread_pips"],
        "minimum_mid": result["minimum_mid"],
        "maximum_mid": result["maximum_mid"],
        "http_status": int(result["http_status"]),
        "latency_ms": int(result["latency_ms"]),
        "error_text": result["error_text"],
        "payload_sha256": result["payload_sha256"],
        "payload_gzip": result["payload_gzip"],
        "evidence_class": "retrospective_event_watch_reconstruction_not_forecast_proof",
        "outcome_selected": 0,
        "forecast_proof_eligible": 0,
        "research_only": 1,
        "execution_eligible": 0,
    }


def insert_row(connection: sqlite3.Connection, row: Mapping[str, Any]) -> None:
    marks = ",".join("?" for _ in WINDOW_COLUMNS)
    connection.execute(
        f"INSERT OR IGNORE INTO verified_event_price_windows ({','.join(WINDOW_COLUMNS)}) VALUES ({marks})",
        [row[column] for column in WINDOW_COLUMNS],
    )
    existing = connection.execute(
        "SELECT payload_sha256 FROM verified_event_price_windows WHERE window_id=?", (row["window_id"],)
    ).fetchone()
    if existing is None or existing[0] != row["payload_sha256"]:
        raise RuntimeError(f"immutable_price_window_conflict:{row['window_id']}")


def snapshot(database: Path, attempted: int, reused: bool) -> dict[str, Any]:
    connection = sqlite3.connect(f"file:{database.resolve().as_posix()}?mode=ro", uri=True)
    states = dict(connection.execute(
        "SELECT coverage_state,count(*) FROM verified_event_price_windows WHERE contract_id=? GROUP BY coverage_state",
        (CONTRACT_ID,),
    ).fetchall())
    metrics = connection.execute(
        """
        SELECT count(*),count(DISTINCT case_id),count(DISTINCT instrument),avg(candle_count),
               avg(average_spread_pips),max(maximum_spread_pips),avg(latency_ms),
               coalesce(sum(execution_eligible),0),coalesce(sum(forecast_proof_eligible),0)
        FROM verified_event_price_windows WHERE contract_id=?
        """,
        (CONTRACT_ID,),
    ).fetchone()
    case_rows = [dict(zip(
        ["case_id", "window_count", "exact_count", "average_spread_pips", "maximum_spread_pips"], row
    )) for row in connection.execute(
        """
        SELECT case_id,count(*),sum(coverage_state='exact_window'),avg(average_spread_pips),
               max(maximum_spread_pips)
        FROM verified_event_price_windows WHERE contract_id=? GROUP BY case_id ORDER BY case_id
        """, (CONTRACT_ID,)
    )]
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    connection.close()
    result = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "source_case_contract_id": SOURCE_CASE_CONTRACT_ID,
        "generated_utc": utc_now(),
        "attempted_this_run": attempted,
        "stored_window_count": int(metrics[0] or 0),
        "case_count": int(metrics[1] or 0),
        "instrument_count": int(metrics[2] or 0),
        "coverage_states": states,
        "average_candle_count": float(metrics[3]) if metrics[3] is not None else None,
        "average_spread_pips": float(metrics[4]) if metrics[4] is not None else None,
        "maximum_spread_pips": float(metrics[5]) if metrics[5] is not None else None,
        "average_request_latency_ms": float(metrics[6]) if metrics[6] is not None else None,
        "execution_eligible_count": int(metrics[7] or 0),
        "forecast_proof_eligible_count": int(metrics[8] or 0),
        "case_coverage": case_rows,
        "resumed_existing_contract": reused,
        "sqlite_integrity": integrity,
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    stable = dict(result)
    stable.pop("generated_utc")
    stable.pop("attempted_this_run")
    stable.pop("resumed_existing_contract")
    result["snapshot_sha256"] = sha256_bytes(canonical_json(stable).encode())
    return result


def run(
    database: Path = DATABASE,
    report_root: Path = REPORT_ROOT,
    workers: int = MAX_WORKERS,
) -> dict[str, Any]:
    reconstruction = load_contract(RECONSTRUCTION_CONFIG)
    instruments = sorted(str(value) for value in reconstruction["expected_instruments"])
    universe_hash = sha256_bytes(canonical_json(instruments).encode())
    connection = sqlite3.connect(database, timeout=120)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    cases, source_hash = source_snapshot(connection)
    jobs = request_jobs(cases, instruments)
    builder_hash = sha256_file(Path(__file__).resolve())
    contract = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "source_case_contract_id": SOURCE_CASE_CONTRACT_ID,
        "builder_sha256": builder_hash,
        "source_snapshot_sha256": source_hash,
        "universe_sha256": universe_hash,
        "instrument_count": len(instruments),
        "case_count": len(cases),
        "window_count": len(jobs),
        "pre_watch_minutes": PRE_WATCH_MINUTES,
        "post_episode_minutes": POST_EPISODE_MINUTES,
        "endpoint": "https://api-fxpractice.oanda.com/v3/instruments/*/candles",
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
    }
    contract_json = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM verified_event_price_contracts WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()
    reused = existing is not None
    if existing is not None and existing[0] != contract_json:
        raise RuntimeError("immutable_event_price_contract_conflict")
    if existing is None:
        connection.execute(
            "INSERT INTO verified_event_price_contracts VALUES (?,?,?,?,?,?,?,?)",
            (
                CONTRACT_ID, SOURCE_CASE_CONTRACT_ID, contract_json, builder_hash, source_hash,
                universe_hash, contract["endpoint"], FREEZE_UTC,
            ),
        )
        connection.commit()
    existing_keys = {
        (str(row[0]), str(row[1])) for row in connection.execute(
            "SELECT case_id,instrument FROM verified_event_price_windows WHERE contract_id=?", (CONTRACT_ID,)
        )
    }
    pending = [job for job in jobs if (job["case_id"], job["instrument"]) not in existing_keys]
    if pending:
        client, meta = resolve_readonly_oanda_client()
        if meta.get("environment") != "practice" or "api-fxpractice.oanda.com" not in meta.get("base_url", ""):
            raise RuntimeError("practice_readonly_endpoint_required")
        with ThreadPoolExecutor(max_workers=max(1, min(int(workers), MAX_WORKERS))) as executor:
            futures = {executor.submit(fetch_job, client, job): job for job in pending}
            for future in as_completed(futures):
                result = future.result()
                insert_row(connection, row_for(result))
                connection.commit()
    connection.close()
    report = snapshot(database, len(pending), reused)
    if report["stored_window_count"] != 136 or report["case_count"] != 2 or report["instrument_count"] != 68:
        raise RuntimeError("event_watch_price_coverage_incomplete")
    if report["execution_eligible_count"] or report["forecast_proof_eligible_count"]:
        raise RuntimeError("event_watch_price_safety_violation")
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "EVENT_WATCH_PRICE_REACQUISITION_V2.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = [
        "# Event-watch executable-price reacquisition V2",
        "",
        f"- Exact verified event watches: **{report['case_count']}**.",
        f"- Instruments per watch: **{report['instrument_count']}**.",
        f"- Immutable all-68 windows: **{report['stored_window_count']}**.",
        f"- Coverage states: `{canonical_json(report['coverage_states'])}`.",
        f"- Mean spread: **{report['average_spread_pips']:.3f} pips**.",
        "- Evidence is retrospective event-watch reconstruction, not forecast proof.",
        "- Execution decision: **no_trade**.",
        "",
        "## Case coverage",
        "",
        "| Case | Windows | Exact | Average spread | Maximum spread |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in report["case_coverage"]:
        lines.append(
            f"| {row['case_id']} | {row['window_count']} | {row['exact_count']} | "
            f"{row['average_spread_pips']:.3f} | {row['maximum_spread_pips']:.3f} |"
        )
    lines.append("")
    (report_root / "EVENT_WATCH_PRICE_REACQUISITION_V2.md").write_text("\n".join(lines), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()
    print(json.dumps(run(args.database, args.report_root, args.workers), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
