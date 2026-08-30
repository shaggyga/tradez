"""Prospective all-68, both-side executable movement census (research only).

One immutable bid/ask frame is admitted per scheduled minute.  The database
stores the quote frame once; LONG/SHORT terminal paths are reconstructed from
entry and exit frames so 136 redundant arm rows are never materialized.
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
from zoneinfo import ZoneInfo

UTC = dt.timezone.utc
NY = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "executable_move_census_v1.json"
QUOTES = ROOT / "data" / "oanda_training_manager" / "state" / "practice_007_market_quotes_v1.json"
DATABASE = ROOT / "data" / "oanda_training_manager" / "state" / "executable_move_census_v1.sqlite"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "state" / "executable_move_census_latest_v1.json"
HEARTBEAT = ROOT / "data" / "oanda_training_manager" / "state" / "executable_move_census_heartbeat_v1.json"
SCHEMA = "executable_move_census_v1"
_RECONCILE_START: dict[str, dt.datetime] = {}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def parse_utc(value: Any) -> dt.datetime | None:
    try:
        result = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def market_open(value: dt.datetime) -> bool:
    local = value.astimezone(NY)
    weekday = local.weekday()
    minute = local.hour * 60 + local.minute
    if weekday == 4 and minute >= 16 * 60 + 59 or weekday == 5:
        return False
    if weekday == 6 and minute < 17 * 60 + 5:
        return False
    return not (16 * 60 + 59 <= minute < 17 * 60 + 5)


def instrument_market_open(instrument: str, value: dt.datetime, cfg: Mapping[str, Any]) -> bool:
    if not market_open(value):
        return False
    if instrument in set(cfg.get("special_hours_fail_closed_instruments") or []):
        local = value.astimezone(NY)
        minute = local.hour * 60 + local.minute
        if local.weekday() == 6 and minute < 17 * 60 + 10:
            return False
        if 16 * 60 + 55 <= minute < 17 * 60 + 10:
            return False
    return True


def load_config(path: Path = CONFIG) -> tuple[dict[str, Any], bytes]:
    raw = path.read_bytes()
    cfg = json.loads(raw)
    instruments = cfg.get("instruments") or {}
    if len(instruments) != 68 or list(instruments) != sorted(instruments):
        raise ValueError("frozen universe must contain exactly 68 sorted instruments")
    if tuple(cfg.get("horizons_min") or ()) != (1, 5, 10, 15, 30, 60):
        raise ValueError("terminal horizon contract drift")
    if cfg.get("research_only") is not True or any(cfg.get(key) for key in ("can_trade", "can_authorize", "can_promote")):
        raise ValueError("research boundary drift")
    return cfg, raw


def source_code_sha() -> str:
    return sha(Path(__file__).read_bytes())


def open_db(path: Path, cfg: Mapping[str, Any], config_raw: bytes) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=10.0)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA busy_timeout=10000")
    db.executescript("""
      CREATE TABLE IF NOT EXISTS cohort_manifest(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), cohort_id TEXT NOT NULL,
        contract_id TEXT NOT NULL, activation_utc TEXT NOT NULL, universe_json TEXT NOT NULL,
        universe_sha256 TEXT NOT NULL, pip_map_json TEXT NOT NULL, pip_map_sha256 TEXT NOT NULL,
        horizons_json TEXT NOT NULL, slippage_pips REAL NOT NULL, slippage_semantics TEXT NOT NULL,
        cadence_sec INTEGER NOT NULL, timing_contract_json TEXT NOT NULL,
        market_calendar_policy TEXT NOT NULL, capture_contract_id TEXT NOT NULL,
        capture_cohort_id TEXT NOT NULL, source_schema_version TEXT NOT NULL,
        source_producer TEXT NOT NULL, source_code_sha256 TEXT NOT NULL,
        quote_transport_sha256 TEXT NOT NULL, config_sha256 TEXT NOT NULL,
        producer_sha256 TEXT NOT NULL,
        research_only INTEGER NOT NULL CHECK(research_only=1), can_trade INTEGER NOT NULL CHECK(can_trade=0),
        can_authorize INTEGER NOT NULL CHECK(can_authorize=0), can_promote INTEGER NOT NULL CHECK(can_promote=0)
      );
      CREATE TABLE IF NOT EXISTS frames(
        frame_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL, scheduled_utc TEXT NOT NULL,
        read_started_utc TEXT NOT NULL, captured_utc TEXT NOT NULL, precommit_utc TEXT NOT NULL,
        market_open INTEGER NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL,
        expected_count INTEGER NOT NULL CHECK(expected_count=68), valid_count INTEGER NOT NULL,
        invalid_count INTEGER NOT NULL, raw_payload BLOB NOT NULL, raw_payload_sha256 TEXT NOT NULL,
        capture_contract_id TEXT NOT NULL, capture_cohort_id TEXT NOT NULL,
        source_schema_version TEXT NOT NULL, source_producer TEXT NOT NULL,
        UNIQUE(cohort_id,scheduled_utc)
      );
      CREATE TABLE IF NOT EXISTS frame_quotes(
        frame_id TEXT NOT NULL REFERENCES frames(frame_id), instrument TEXT NOT NULL,
        universe_index INTEGER NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL,
        bid REAL, ask REAL, pip REAL NOT NULL, quote_utc TEXT, quote_age_sec REAL,
        component_bytes BLOB NOT NULL, component_sha256 TEXT NOT NULL,
        capture_contract_id TEXT NOT NULL, capture_cohort_id TEXT NOT NULL,
        PRIMARY KEY(frame_id,instrument), UNIQUE(frame_id,universe_index)
      );
      CREATE TABLE IF NOT EXISTS frame_commit_receipts(
        frame_id TEXT PRIMARY KEY REFERENCES frames(frame_id), committed_utc TEXT NOT NULL,
        within_first_horizon INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS window_evaluations(
        target_scheduled_utc TEXT NOT NULL, declared_window_min INTEGER NOT NULL,
        entry_frame_id TEXT REFERENCES frames(frame_id), exit_frame_id TEXT REFERENCES frames(frame_id), evaluated_utc TEXT NOT NULL,
        state TEXT NOT NULL, expected_side_count INTEGER NOT NULL CHECK(expected_side_count=136),
        valid_count INTEGER NOT NULL, cleared_count INTEGER NOT NULL, path_cleared_count INTEGER NOT NULL,
        invalid_count INTEGER NOT NULL, pending_count INTEGER NOT NULL,
        terminal_rows_sha256 TEXT NOT NULL, path_summary_rows_sha256 TEXT NOT NULL,
        terminal_clear_ids_sha256 TEXT NOT NULL, path_clear_ids_sha256 TEXT NOT NULL,
        PRIMARY KEY(target_scheduled_utc,declared_window_min)
      );
      CREATE TABLE IF NOT EXISTS factor_episode_summaries(
        episode_utc TEXT NOT NULL, factor_currency TEXT NOT NULL, factor_sign INTEGER NOT NULL,
        event_kind TEXT NOT NULL, finalized_utc TEXT NOT NULL, member_count INTEGER NOT NULL,
        member_ids_sha256 TEXT NOT NULL, representative_json TEXT NOT NULL,
        PRIMARY KEY(episode_utc,factor_currency,factor_sign,event_kind)
      );
      CREATE TABLE IF NOT EXISTS factor_episode_finalizations(
        episode_utc TEXT PRIMARY KEY, finalized_utc TEXT NOT NULL,
        expected_window_count INTEGER NOT NULL, evaluated_window_count INTEGER NOT NULL,
        summary_count INTEGER NOT NULL, evaluation_keys_sha256 TEXT NOT NULL,
        terminal_member_count INTEGER NOT NULL, terminal_member_ids_sha256 TEXT NOT NULL,
        path_member_count INTEGER NOT NULL, path_member_ids_sha256 TEXT NOT NULL
      );
      CREATE TRIGGER IF NOT EXISTS manifest_no_update BEFORE UPDATE ON cohort_manifest BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS manifest_no_delete BEFORE DELETE ON cohort_manifest BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS frames_no_update BEFORE UPDATE ON frames BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS frames_no_delete BEFORE DELETE ON frames BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS quotes_no_update BEFORE UPDATE ON frame_quotes BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS quotes_no_delete BEFORE DELETE ON frame_quotes BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS receipts_no_update BEFORE UPDATE ON frame_commit_receipts BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS receipts_no_delete BEFORE DELETE ON frame_commit_receipts BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS evaluations_no_update BEFORE UPDATE ON window_evaluations BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS evaluations_no_delete BEFORE DELETE ON window_evaluations BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS factor_summaries_no_update BEFORE UPDATE ON factor_episode_summaries BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS factor_summaries_no_delete BEFORE DELETE ON factor_episode_summaries BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS factor_finalizations_no_update BEFORE UPDATE ON factor_episode_finalizations BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE TRIGGER IF NOT EXISTS factor_finalizations_no_delete BEFORE DELETE ON factor_episode_finalizations BEGIN SELECT RAISE(ABORT,'append_only'); END;
      CREATE INDEX IF NOT EXISTS frames_schedule_idx ON frames(scheduled_utc);
      CREATE INDEX IF NOT EXISTS quotes_instrument_idx ON frame_quotes(instrument,frame_id);
    """)
    instruments = cfg["instruments"]
    universe = list(instruments)
    expected = (
        str(cfg["cohort_id"]), SCHEMA, str(cfg["activation_utc"]),
        canonical(universe).decode(), sha(canonical(universe)), canonical(instruments).decode(), sha(canonical(instruments)),
        canonical(cfg["horizons_min"]).decode(), float(cfg["slippage_pips"]), str(cfg["slippage_semantics"]),
        int(cfg["frame_cadence_sec"]), canonical({k: cfg[k] for k in ("maximum_quote_age_sec","maximum_snapshot_generated_age_sec","maximum_future_skew_sec","maximum_capture_delay_sec","maximum_target_offset_sec")}).decode(),
        str(cfg["market_calendar_policy"]), str(cfg["capture_contract_id"]), str(cfg["capture_cohort_id"]),
        str(cfg["source_schema_version"]), str(cfg["source_producer"]),
        sha((ROOT / "oanda_practice_top_signal_executor.py").read_bytes()), sha((ROOT / "oanda_quote_transport.py").read_bytes()), sha(config_raw), source_code_sha(), 1, 0, 0, 0,
    )
    row = db.execute("SELECT * FROM cohort_manifest WHERE singleton=1").fetchone()
    if row is None:
        db.execute("INSERT INTO cohort_manifest VALUES (1," + ",".join("?" for _ in expected) + ")", expected)
        db.commit()
    elif tuple(row)[1:] != expected:
        db.close()
        raise RuntimeError("cohort manifest identity drift; rotate cohort/database")
    return db


def frame_id(cohort: str, scheduled: str) -> str:
    return sha(f"{cohort}|{scheduled}".encode())


def build_frame(cfg: Mapping[str, Any], raw: bytes, read_started: dt.datetime, captured: dt.datetime, precommit: dt.datetime) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = json.loads(raw)
    cadence = int(cfg["frame_cadence_sec"])
    scheduled_epoch = int(captured.timestamp() // cadence) * cadence
    scheduled = dt.datetime.fromtimestamp(scheduled_epoch, UTC)
    activation = parse_utc(cfg["activation_utc"])
    if activation is None or scheduled < activation:
        raise ValueError("no_backfill_activation_gate")
    capture_delay = (captured - scheduled).total_seconds()
    if capture_delay < 0 or capture_delay > float(cfg["maximum_capture_delay_sec"]):
        raise ValueError("capture_outside_scheduled_minute")
    quotes = payload.get("quotes") if isinstance(payload.get("quotes"), dict) else {}
    payload_generated=parse_utc(payload.get("generated_utc"))
    source_ok = str(payload.get("schema_version")) == str(cfg["source_schema_version"]) and str(payload.get("producer")) == str(cfg["source_producer"])
    header_ok = payload_generated is not None and -float(cfg["maximum_future_skew_sec"]) <= (captured-payload_generated).total_seconds() <= float(cfg["maximum_snapshot_generated_age_sec"]) and int(payload.get("quote_count") or -1)==len(quotes)
    unexpected_keys = set(quotes)-set(cfg["instruments"])
    rows: list[dict[str, Any]] = []
    valid = 0
    for index, (instrument, frozen_pip) in enumerate(cfg["instruments"].items()):
        component = quotes.get(instrument)
        component = component if isinstance(component, dict) else {}
        component_bytes = canonical(component)
        bid, ask, observed, observed_pip = finite(component.get("bid")), finite(component.get("ask")), parse_utc(component.get("time")), finite(component.get("pip"))
        state, reason, age = "valid", "", None
        if not instrument_market_open(instrument, scheduled, cfg):
            state, reason = "excluded_closed", "market_closed_or_special_hours_fail_closed"
        elif not source_ok:
            state, reason = "invalid", "source_identity_mismatch"
        elif not component:
            state, reason = "missing", "missing_instrument"
        elif not header_ok or unexpected_keys:
            state, reason = "invalid", "snapshot_header_or_universe_mismatch"
        elif bid is None or ask is None or bid <= 0 or ask <= bid:
            state, reason = "invalid", "invalid_bid_ask"
        elif observed_pip is None or abs(observed_pip - float(frozen_pip)) > 1e-12:
            state, reason = "invalid", "pip_contract_mismatch"
        elif observed is None:
            state, reason = "invalid", "invalid_quote_clock"
        else:
            age = (captured - observed).total_seconds()
            if age < -float(cfg["maximum_future_skew_sec"]):
                state, reason = "invalid", "future_quote"
            elif age > float(cfg["maximum_quote_age_sec"]):
                state, reason = "stale", "stale_quote"
            elif abs((observed-scheduled).total_seconds()) > float(cfg["maximum_target_offset_sec"]):
                state, reason = "invalid", "quote_target_offset"
        if state == "valid": valid += 1
        rows.append({"instrument":instrument,"universe_index":index,"state":state,"reason":reason,"bid":bid if state=="valid" else None,"ask":ask if state=="valid" else None,"pip":float(frozen_pip),"quote_utc":iso(observed) if observed else None,"quote_age_sec":age,"component_bytes":component_bytes,"component_sha256":sha(component_bytes)})
    scheduled_text = iso(scheduled)
    frame = {"frame_id":frame_id(str(cfg["cohort_id"]),scheduled_text),"cohort_id":str(cfg["cohort_id"]),"scheduled_utc":scheduled_text,"read_started_utc":iso(read_started),"captured_utc":iso(captured),"precommit_utc":iso(precommit),"market_open":int(market_open(scheduled)),"state":"valid" if valid==68 else ("excluded_closed" if not market_open(scheduled) else "partial_invalid"),"reason":"" if valid==68 else ("market_closed" if not market_open(scheduled) else "one_or_more_quote_rows_invalid"),"expected_count":68,"valid_count":valid,"invalid_count":68-valid,"raw_payload":raw,"raw_payload_sha256":sha(raw),"capture_contract_id":str(cfg["capture_contract_id"]),"capture_cohort_id":str(cfg["capture_cohort_id"]),"source_schema_version":str(cfg["source_schema_version"]),"source_producer":str(cfg["source_producer"])}
    return frame, rows


def insert_frame(db: sqlite3.Connection, frame: dict[str, Any], rows: list[Mapping[str, Any]], cfg: Mapping[str, Any], *, precommit: dt.datetime | None = None) -> bool:
    if len(rows) != 68:
        raise ValueError("Cartesian frame must contain 68 rows")
    db.execute("BEGIN IMMEDIATE")
    try:
        actual_precommit = (precommit or dt.datetime.now(UTC)).astimezone(UTC)
        scheduled = parse_utc(frame["scheduled_utc"])
        captured = parse_utc(frame["captured_utc"])
        read_started = parse_utc(frame["read_started_utc"])
        if not scheduled or not captured or not read_started or not (read_started <= captured <= actual_precommit):
            raise ValueError("noncausal_capture_clocks")
        if actual_precommit >= scheduled + dt.timedelta(seconds=55):
            raise ValueError("frame_not_committed_before_first_horizon")
        frame["precommit_utc"] = iso(actual_precommit)
        if db.execute("SELECT 1 FROM frames WHERE frame_id=?", (frame["frame_id"],)).fetchone():
            receipt=db.execute("SELECT 1 FROM frame_commit_receipts WHERE frame_id=?",(frame["frame_id"],)).fetchone()
            if receipt is None:
                db.execute("INSERT INTO frame_commit_receipts VALUES (?,?,0)",(frame["frame_id"],iso(actual_precommit))); db.commit()
            else: db.rollback()
            return False
        db.execute("INSERT INTO frames VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tuple(frame.values()))
        db.executemany("INSERT INTO frame_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [(
            frame["frame_id"], row["instrument"], row["universe_index"], row["state"], row["reason"], row["bid"], row["ask"], row["pip"], row["quote_utc"], row["quote_age_sec"], row["component_bytes"], row["component_sha256"], cfg["capture_contract_id"], cfg["capture_cohort_id"]
        ) for row in rows])
        count = db.execute("SELECT COUNT(*) FROM frame_quotes WHERE frame_id=?", (frame["frame_id"],)).fetchone()[0]
        if count != 68: raise RuntimeError("incomplete frame")
        db.commit()
        committed=(precommit or dt.datetime.now(UTC)).astimezone(UTC)
        within=int(committed < scheduled+dt.timedelta(seconds=60))
        db.execute("INSERT INTO frame_commit_receipts VALUES (?,?,?)",(frame["frame_id"],iso(committed),within)); db.commit()
        return True
    except Exception:
        db.rollback(); raise


def _quotes(db: sqlite3.Connection, fid: str) -> dict[str, sqlite3.Row]:
    return {row["instrument"]: row for row in db.execute("SELECT * FROM frame_quotes WHERE frame_id=? ORDER BY universe_index", (fid,))}


def path_row(entry: sqlite3.Row, exit: sqlite3.Row | None, eq: sqlite3.Row, xq: sqlite3.Row | None, side: str, horizon: int, slip: float, maximum_target_offset_sec: float=55.0) -> dict[str, Any]:
    base = {"instrument":eq["instrument"],"side":side,"horizon_min":horizon,"entry_frame_id":entry["frame_id"],"exit_frame_id":exit["frame_id"] if exit else None,"entry_scheduled_utc":entry["scheduled_utc"],"exit_scheduled_utc":exit["scheduled_utc"] if exit else None,"entry_quote_utc":eq["quote_utc"],"exit_quote_utc":xq["quote_utc"] if xq else None,"entry_bid":eq["bid"],"entry_ask":eq["ask"],"exit_bid":xq["bid"] if xq else None,"exit_ask":xq["ask"] if xq else None,"pip":eq["pip"],"slippage_pips":slip,"slippage_semantics":"total_round_trip_subtracted_once","raw_executable_pips":None,"net_pips":None,"net_bps":None,"state":"pending","reason":"exit_frame_pending"}
    if exit is None: return base
    if eq["state"] != "valid" or xq is None or xq["state"] != "valid":
        base.update(state="invalid",reason=f"entry_{eq['state']}" if eq["state"] != "valid" else f"exit_{xq['state'] if xq else 'missing'}"); return base
    if abs(float(eq["pip"])-float(xq["pip"])) > 1e-12:
        base.update(state="invalid",reason="pip_mismatch"); return base
    entry_target=parse_utc(entry["scheduled_utc"]); entry_quote=parse_utc(eq["quote_utc"])
    if entry_target is None or entry_quote is None or abs((entry_quote-entry_target).total_seconds()) > maximum_target_offset_sec:
        base.update(state="invalid",reason="entry_quote_target_offset"); return base
    target=parse_utc(exit["scheduled_utc"]); exit_quote=parse_utc(xq["quote_utc"])
    if target is None or exit_quote is None or abs((exit_quote-target).total_seconds()) > maximum_target_offset_sec:
        base.update(state="invalid",reason="exit_quote_target_offset"); return base
    pip=float(eq["pip"])
    raw=(float(xq["bid"])-float(eq["ask"]))/pip if side=="long" else (float(eq["bid"])-float(xq["ask"]))/pip
    net=raw-slip
    entry_mid=(float(eq["bid"])+float(eq["ask"]))/2
    signed_price=(float(xq["bid"])-float(eq["ask"])) if side=="long" else (float(eq["bid"])-float(xq["ask"]))
    base.update(raw_executable_pips=round(raw,6),net_pips=round(net,6),net_bps=round(10000*signed_price/entry_mid - slip*pip/entry_mid*10000,6),state="cleared" if net>0 else "not_cleared",reason="")
    return base


def build_latest(db: sqlite3.Connection, cfg: Mapping[str, Any], generated: dt.datetime) -> dict[str, Any]:
    frames = list(db.execute("SELECT f.* FROM frames f JOIN frame_commit_receipts r ON r.frame_id=f.frame_id AND r.within_first_horizon=1 ORDER BY f.scheduled_utc"))
    by_time = {row["scheduled_utc"]: row for row in frames}
    newest = frames[-1] if frames else None
    blocks=[]; top=[]
    for horizon in cfg["horizons_min"]:
        entry = None; exit = None
        terminal=generated.replace(second=0,microsecond=0)
        entry=by_time.get(iso(terminal-dt.timedelta(minutes=int(horizon))))
        exit=by_time.get(iso(terminal))
        eqs=_quotes(db,entry["frame_id"]) if entry else {}; xqs=_quotes(db,exit["frame_id"]) if exit else {}
        rows=[]
        for instrument in cfg["instruments"]:
            eq=eqs.get(instrument)
            for side in ("long","short"):
                if eq is None:
                    rows.append({"instrument":instrument,"side":side,"horizon_min":horizon,"state":"invalid","reason":"no_entry_frame","net_pips":None,"net_bps":None})
                else: rows.append(path_row(entry,exit,eq,xqs.get(instrument),side,int(horizon),float(cfg["slippage_pips"]),float(cfg["maximum_target_offset_sec"])))
        path_frames=[]
        if entry is not None:
            entry_time=parse_utc(entry["scheduled_utc"])
            for offset in range(1,int(horizon)+1):
                stamp=iso(entry_time+dt.timedelta(minutes=offset)); path_frames.append((offset,by_time.get(stamp)))
        path_quote_cache={fr["frame_id"]:_quotes(db,fr["frame_id"]) for _,fr in path_frames if fr is not None}
        path_clear_rows=[]
        for row in rows:
            eq=eqs.get(row["instrument"]); points=[]
            if eq is not None and eq["state"]=="valid":
                for offset,fr in path_frames:
                    if fr is None: continue
                    xq=path_quote_cache[fr["frame_id"]].get(row["instrument"])
                    sample=path_row(entry,fr,eq,xq,row["side"],offset,float(cfg["slippage_pips"]),float(cfg["maximum_target_offset_sec"]))
                    if sample["net_pips"] is not None: points.append({"minute":offset,"frame_id":fr["frame_id"],"scheduled_utc":fr["scheduled_utc"],"net_pips":sample["net_pips"],"net_bps":sample["net_bps"]})
            row["path_state"]="complete" if len(points)==int(horizon) else "incomplete"
            row["path_points"]=points
            row["first_clear_min"]=next((p["minute"] for p in points if p["net_pips"]>0),None)
            if points:
                maximum=max(points,key=lambda p:(p["net_pips"],-p["minute"])); minimum=min(points,key=lambda p:(p["net_pips"],p["minute"]))
                row.update(path_max_net_pips=maximum["net_pips"],path_max_net_bps=maximum["net_bps"],path_max_frame_id=maximum["frame_id"],path_max_scheduled_utc=maximum["scheduled_utc"],path_min_net_pips=minimum["net_pips"],path_cleared=maximum["net_pips"]>0)
                if row["path_cleared"]:
                    clone=dict(row); clone.update(event_kind="path_peak",horizon_min=maximum["minute"],exit_frame_id=maximum["frame_id"],exit_scheduled_utc=maximum["scheduled_utc"],net_pips=maximum["net_pips"],net_bps=maximum["net_bps"]); path_clear_rows.append(clone)
            else: row.update(path_max_net_pips=None,path_max_net_bps=None,path_max_frame_id=None,path_max_scheduled_utc=None,path_min_net_pips=None,path_cleared=False)
        clear=[row for row in rows if row["state"]=="cleared"]
        top.extend(clear)
        blocks.append({"horizon_min":horizon,"entry_frame_id":entry["frame_id"] if entry else None,"exit_frame_id":exit["frame_id"] if exit else None,"expected_side_count":136,"row_count":len(rows),"valid_count":sum(r["state"] in ("cleared","not_cleared") for r in rows),"cleared_count":len(clear),"path_cleared_count":sum(r.get("path_cleared") is True for r in rows),"invalid_count":sum(r["state"]=="invalid" for r in rows),"pending_count":sum(r["state"]=="pending" for r in rows),"rows":rows,"_path_clear_rows":path_clear_rows})
    top.sort(key=lambda r:(-float(r["net_bps"]),-float(r["net_pips"]),r["instrument"],r["side"],r["horizon_min"]))
    activation=parse_utc(cfg["activation_utc"]); expected=[]
    cursor=activation.replace(second=0,microsecond=0) if activation else generated
    end=generated.replace(second=0,microsecond=0)
    while cursor <= end:
        if market_open(cursor): expected.append(iso(cursor))
        cursor += dt.timedelta(minutes=1)
    observed={row["scheduled_utc"] for row in frames}
    missing=[value for value in expected if value not in observed]
    all_path_clears=[row for block in blocks for row in block.pop("_path_clear_rows",[])]
    result={"schema_version":"executable_move_census_latest_v1","generated_utc":iso(generated),"cohort_id":cfg["cohort_id"],"research_only":True,"can_trade":False,"can_authorize":False,"can_promote":False,"definition":"A move exists only when a later executable exit produces net pips > 0 after one frozen round-trip slippage deduction; spread is already embedded in bid/ask endpoints.","measurement_scope":"terminal fixed-window net is primary; descriptive hindsight path fields report first clear, maximum favorable executable net, and minimum executable net from predeclared causal minute frames","instrument_count":68,"side_count":136,"horizons_min":cfg["horizons_min"],"slippage_pips":cfg["slippage_pips"],"frame_count":len(frames),"latest_frame_utc":newest["scheduled_utc"] if newest else None,"schedule_census":{"expected_open_frames":len(expected),"observed_frames":sum(x in observed for x in expected),"missing_open_frames":len(missing),"recent_missing_scheduled_utc":missing[-120:]},"horizons":blocks,"top_cleared_paths":top[:50],"_all_cleared_paths":top,"_all_path_clears":all_path_clears}
    return result


def window_digests(block: Mapping[str, Any], cfg: Mapping[str, Any]) -> tuple[str,str,str,str]:
    terminal=[]; path=[]; terminal_ids=[]; path_ids=[]
    for row in block.get("rows") or []:
        identity={"instrument":row["instrument"],"side":row["side"],"declared_window_min":block["horizon_min"],"entry_frame_id":row.get("entry_frame_id"),"exit_frame_id":row.get("exit_frame_id")}
        terminal.append({**identity,"state":row.get("state"),"reason":row.get("reason"),"net_pips":row.get("net_pips"),"net_bps":row.get("net_bps")})
        path.append({**identity,"path_state":row.get("path_state"),"first_clear_min":row.get("first_clear_min"),"path_max_net_pips":row.get("path_max_net_pips"),"path_max_net_bps":row.get("path_max_net_bps"),"path_max_frame_id":row.get("path_max_frame_id"),"path_min_net_pips":row.get("path_min_net_pips"),"path_cleared":row.get("path_cleared")})
        if row.get("state")=="cleared": terminal_ids.append(sha(canonical({"kind":"terminal",**identity})))
        if row.get("path_cleared"): path_ids.append(sha(canonical({"kind":"path_peak","instrument":row["instrument"],"side":row["side"],"entry_frame_id":row.get("entry_frame_id"),"exit_frame_id":row.get("path_max_frame_id")})))
    return sha(canonical(terminal)),sha(canonical(path)),sha(canonical(sorted(terminal_ids))),sha(canonical(sorted(set(path_ids))))


def evaluated_block(block: Mapping[str, Any]) -> dict[str, Any]:
    result={**block,"rows":[dict(row) for row in block.get("rows") or []]}
    if result.get("exit_frame_id") is None:
        for row in result["rows"]: row.update(state="invalid",reason="scheduled_exit_frame_missing",net_pips=None,net_bps=None)
        result.update(valid_count=0,cleared_count=0,path_cleared_count=sum(row.get("path_cleared") is True for row in result["rows"]),invalid_count=136,pending_count=0)
    return result


def reconcile_window_evaluations(db: sqlite3.Connection, cfg: Mapping[str, Any], generated: dt.datetime) -> None:
    existing={(str(row[0]),int(row[1])) for row in db.execute("SELECT target_scheduled_utc,declared_window_min FROM window_evaluations")}
    activation=parse_utc(cfg["activation_utc"]); cutoff=generated.replace(second=0,microsecond=0)-dt.timedelta(minutes=1)
    if activation is None: return
    database_key=str(db.execute("PRAGMA database_list").fetchone()[2]); target_cursor=_RECONCILE_START.get(database_key,activation.replace(second=0,microsecond=0))
    if target_cursor > cutoff: return
    required=[]
    while target_cursor <= cutoff:
        for horizon in cfg["horizons_min"]:
            entry=target_cursor-dt.timedelta(minutes=int(horizon))
            if entry >= activation and market_open(entry) and (iso(target_cursor),int(horizon)) not in existing: required.append((target_cursor,int(horizon)))
        target_cursor += dt.timedelta(minutes=1)
    view_cache={}
    for target,horizon in required:
        target_text=iso(target)
        view=view_cache.get(target_text)
        if view is None: view=build_latest(db,cfg,target); view_cache[target_text]=view
        block=evaluated_block(next(row for row in view["horizons"] if int(row["horizon_min"])==horizon))
        terminal_sha,path_sha,terminal_clear_sha,path_clear_sha=window_digests(block,cfg)
        state="evaluated" if block["entry_frame_id"] and block["exit_frame_id"] else "terminal_invalid_missing_frame"
        db.execute("BEGIN IMMEDIATE")
        try:
            db.execute("INSERT INTO window_evaluations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(target_text,horizon,block["entry_frame_id"],block["exit_frame_id"],iso(generated),state,136,block["valid_count"],block["cleared_count"],block["path_cleared_count"],block["invalid_count"],block["pending_count"],terminal_sha,path_sha,terminal_clear_sha,path_clear_sha)); db.commit()
        except Exception: db.rollback(); raise
    _RECONCILE_START[database_key]=cutoff+dt.timedelta(minutes=1)


def finalize_factor_episodes(db: sqlite3.Connection, cfg: Mapping[str, Any], generated: dt.datetime) -> None:
    activation=parse_utc(cfg["activation_utc"])
    if activation is None: return
    last=db.execute("SELECT MAX(episode_utc) FROM factor_episode_finalizations").fetchone()[0]
    episode=(parse_utc(last)+dt.timedelta(minutes=15)) if last else dt.datetime.fromtimestamp(int(activation.timestamp()//900)*900,UTC)
    existing={row[0] for row in db.execute("SELECT episode_utc FROM factor_episode_finalizations")}
    while episode+dt.timedelta(minutes=75) <= generated:
        episode_text=iso(episode)
        if episode_text in existing: episode+=dt.timedelta(minutes=15); continue
        expected=[]
        for minute in range(15):
            entry=episode+dt.timedelta(minutes=minute)
            if entry < activation or not market_open(entry): continue
            for horizon in cfg["horizons_min"]: expected.append((iso(entry+dt.timedelta(minutes=int(horizon))),int(horizon)))
        available={(row[0],int(row[1])) for row in db.execute("SELECT target_scheduled_utc,declared_window_min FROM window_evaluations WHERE target_scheduled_utc BETWEEN ? AND ?",(iso(episode),iso(episode+dt.timedelta(minutes=75))))}
        if not set(expected).issubset(available): break
        groups: dict[tuple[str,int,str],dict[str,Any]]={}; cache={}
        for target,horizon in expected:
            view=cache.get(target)
            if view is None: view=build_latest(db,cfg,parse_utc(target)); cache[target]=view
            block=evaluated_block(next(row for row in view["horizons"] if int(row["horizon_min"])==horizon))
            for row in block["rows"]:
                candidates=[]
                if row.get("state")=="cleared": candidates.append(("terminal",row.get("exit_frame_id"),horizon,row.get("net_pips"),row.get("net_bps")))
                if row.get("path_cleared"):
                    peak_offset=next((point.get("minute") for point in row.get("path_points") or [] if point.get("frame_id")==row.get("path_max_frame_id")),None)
                    candidates.append(("path_peak",row.get("path_max_frame_id"),peak_offset,row.get("path_max_net_pips"),row.get("path_max_net_bps")))
                base,quote=row["instrument"].split("_",1); base_sign=1 if row["side"]=="long" else -1
                for kind,exit_id,offset,net_pips,net_bps in candidates:
                    member_id=sha(canonical({"kind":kind,"entry":row.get("entry_frame_id"),"exit":exit_id,"instrument":row["instrument"],"side":row["side"]}))
                    rep={"member_id":member_id,"instrument":row["instrument"],"side":row["side"],"declared_window_min":horizon,"actual_exit_offset_min":offset,"entry_frame_id":row.get("entry_frame_id"),"exit_frame_id":exit_id,"net_pips":net_pips,"net_bps":net_bps}
                    for currency,sign in ((base,base_sign),(quote,-base_sign)):
                        group=groups.setdefault((currency,sign,kind),{"ids":set(),"rep":rep}); group["ids"].add(member_id)
                        if (rep["net_bps"],rep["net_pips"],rep["member_id"])>(group["rep"]["net_bps"],group["rep"]["net_pips"],group["rep"]["member_id"]): group["rep"]=rep
        db.execute("BEGIN IMMEDIATE")
        try:
            terminal_ids=sorted({member for (currency,sign,kind),group in groups.items() if kind=="terminal" for member in group["ids"]})
            path_ids=sorted({member for (currency,sign,kind),group in groups.items() if kind=="path_peak" for member in group["ids"]})
            for (currency,sign,kind),group in groups.items():
                ids=sorted(group["ids"]); db.execute("INSERT INTO factor_episode_summaries VALUES (?,?,?,?,?,?,?,?)",(episode_text,currency,sign,kind,iso(generated),len(ids),sha(canonical(ids)),canonical(group["rep"]).decode()))
            evaluated_keys=set(expected)&available
            db.execute("INSERT INTO factor_episode_finalizations VALUES (?,?,?,?,?,?,?,?,?,?)",(episode_text,iso(generated),len(expected),len(evaluated_keys),len(groups),sha(canonical(sorted(evaluated_keys))),len(terminal_ids),sha(canonical(terminal_ids)),len(path_ids),sha(canonical(path_ids))))
            db.commit()
        except Exception: db.rollback(); raise
        episode+=dt.timedelta(minutes=15)


def populate_review_queue(db: sqlite3.Connection, payload: dict[str, Any], cfg: Mapping[str, Any]) -> None:
    totals=db.execute("SELECT COALESCE(SUM(cleared_count),0),COALESCE(SUM(path_cleared_count),0),COUNT(*) FROM window_evaluations").fetchone()
    summaries=[]
    for row in db.execute("SELECT * FROM factor_episode_summaries ORDER BY episode_utc DESC"):
        rep=json.loads(row["representative_json"]); summaries.append({"case_id":sha(f"{cfg['cohort_id']}|{row['episode_utc']}|{row['factor_currency']}|{row['factor_sign']}|{row['event_kind']}".encode()),"episode_utc":row["episode_utc"],"factor_currency":row["factor_currency"],"factor_sign":row["factor_sign"],"event_kind":row["event_kind"],"member_count":row["member_count"],"member_ids_sha256":row["member_ids_sha256"],**rep})
    summaries.sort(key=lambda r:(-float(r["net_bps"]),-float(r["net_pips"]),r["case_id"]))
    payload["review_queue"]={"total_terminal_cleared_arms":int(totals[0]),"total_path_cleared_arms":int(totals[1]),"total_cleared_arm_observations":int(totals[0]+totals[1]),"evaluated_windows":int(totals[2]),"distinct_factor_episode_cases":len(summaries),"storage_contract":"all 136 arms reconstructible from immutable frames and per-window terminal/path/clear digests; compact factor summaries finalize after episode end plus 60 minutes","dedupe_definition":"entry-time 15-minute episode x signed currency factor x event_kind","representatives":summaries[:50]}


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True,exist_ok=True); tmp=path.with_suffix(path.suffix+f".{os.getpid()}.tmp")
    tmp.write_bytes(canonical(payload))
    try:
        for attempt in range(5):
            try:
                os.replace(tmp,path); return
            except PermissionError:
                if attempt == 4: raise
                time.sleep(0.05 * (attempt + 1))
    finally:
        try: tmp.unlink(missing_ok=True)
        except OSError: pass


def run_once(*, config_path: Path=CONFIG, quotes_path: Path=QUOTES, database_path: Path=DATABASE, output_path: Path=OUTPUT, now: dt.datetime|None=None) -> dict[str, Any]:
    cfg,config_raw=load_config(config_path)
    read_started=(now or dt.datetime.now(UTC)).astimezone(UTC)
    activation=parse_utc(cfg["activation_utc"])
    if activation is None or read_started < activation:
        payload={"schema_version":"executable_move_census_latest_v1","generated_utc":iso(read_started),"cohort_id":cfg["cohort_id"],"status":"collecting_pre_activation","activation_utc":cfg["activation_utc"],"instrument_count":68,"side_count":136,"horizons_min":cfg["horizons_min"],"research_only":True,"can_trade":False,"can_authorize":False,"can_promote":False,"frame_count":0,"horizons":[],"top_cleared_paths":[],"review_queue":{}}
        payload["content_sha256_excluding_this_field"]=sha(canonical(payload)); write_json_atomic(output_path,payload); return payload
    raw=quotes_path.read_bytes()
    captured=(now or dt.datetime.now(UTC)).astimezone(UTC)
    db=open_db(database_path,cfg,config_raw)
    try:
        frame,rows=build_frame(cfg,raw,read_started,captured,captured)
        inserted=insert_frame(db,frame,rows,cfg,precommit=now)
        generated=(now or dt.datetime.now(UTC)).astimezone(UTC)
        reconcile_window_evaluations(db,cfg,generated)
        finalize_factor_episodes(db,cfg,generated)
        payload=build_latest(db,cfg,generated); payload["frame_inserted"]=inserted
        populate_review_queue(db,payload,cfg)
        payload.pop("_all_cleared_paths",None)
        payload.pop("_all_path_clears",None)
        payload["content_sha256_excluding_this_field"]=sha(canonical(payload))
        write_json_atomic(output_path,payload); return payload
    finally: db.close()


def main() -> int:
    parser=argparse.ArgumentParser(); parser.add_argument("--config",type=Path,default=CONFIG); parser.add_argument("--quotes",type=Path,default=QUOTES); parser.add_argument("--database",type=Path,default=DATABASE); parser.add_argument("--output",type=Path,default=OUTPUT); parser.add_argument("--heartbeat",type=Path,default=HEARTBEAT); parser.add_argument("--interval-sec",type=float,default=30.0); parser.add_argument("--duration-sec",type=float,default=0.0)
    args=parser.parse_args(); started=time.monotonic()
    while True:
        try:
            payload=run_once(config_path=args.config,quotes_path=args.quotes,database_path=args.database,output_path=args.output)
            heartbeat={"schema_version":"executable_move_census_heartbeat_v1","generated_utc":iso(dt.datetime.now(UTC)),"status":payload.get("status") or "ok","frame_count":payload.get("frame_count",0),"latest_frame_utc":payload.get("latest_frame_utc"),"error":"","research_only":True}
        except Exception as exc:
            heartbeat={"schema_version":"executable_move_census_heartbeat_v1","generated_utc":iso(dt.datetime.now(UTC)),"status":"degraded","error":f"{type(exc).__name__}: {exc}","research_only":True}
        write_json_atomic(args.heartbeat,heartbeat)
        if args.duration_sec <= 0 or time.monotonic()-started >= args.duration_sec: break
        time.sleep(max(1.0,args.interval_sec))
    return 0


if __name__ == "__main__": raise SystemExit(main())
