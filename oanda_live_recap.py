#!/usr/bin/env python3
"""Daily live-account recap and score-demotion helpers.

This module is intentionally broker-read-only.  It parses the account ledgers
already written by the live managers, optionally uses OANDA candle history for a
best-effort stop audit, and writes account-local recap files plus dashboard
state.  Failures are swallowed by callers so recap generation cannot stop a
trading loop.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import math
import os
import re
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


NY_TZ = ZoneInfo("America/New_York")
UTC = dt.timezone.utc

RECAP_STATE_KEY = "live_daily_recap"
DASHBOARD_STATE_KEY = "live_dashboard"
DEMOTION_STATE_KEY = "live_model_score_demotions"
LEARNING_STATE_KEY = "live_learning_recommendations"
LOSS_REVIEW_STATE_KEY = "live_loss_review_prompt"
CRISIS_STATE_KEY = "live_crisis_mode"


def iso_utc() -> str:
    return dt.datetime.now(UTC).isoformat()


def ny_date() -> str:
    return dt.datetime.now(NY_TZ).date().isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def parse_dt(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def row_ny_date(row: dict[str, Any]) -> str:
    time_ny = str(row.get("time_ny") or "").strip()
    if len(time_ny) >= 10 and time_ny[:4].isdigit():
        return time_ny[:10]
    parsed = parse_dt(row.get("time_utc"))
    if parsed:
        return parsed.astimezone(NY_TZ).date().isoformat()
    time_utc = str(row.get("time_utc") or "").strip()
    if len(time_utc) >= 10 and time_utc[:4].isdigit():
        return time_utc[:10]
    return ""


def csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def json_cell(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        obj = json.loads(str(value))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str), encoding="utf-8")


def normalize_instrument(value: Any) -> str:
    raw = str(value or "").strip().upper()
    raw = raw.replace("/", "_").replace("-", "_").replace(" ", "_")
    raw = re.sub(r"_+", "_", raw).strip("_")
    if "_" not in raw and re.fullmatch(r"[A-Z]{6}", raw):
        return f"{raw[:3]}_{raw[3:]}"
    return raw


def split_instrument(instrument: Any) -> tuple[str, str]:
    inst = normalize_instrument(instrument)
    if "_" in inst:
        left, right = inst.split("_", 1)
        return left, right
    return inst[:3], inst[3:]


def direction_from_units(units: Any) -> str:
    value = safe_float(units, 0.0)
    if value > 0:
        return "LONG"
    if value < 0:
        return "SHORT"
    return ""


def opposite_direction(direction: str) -> str:
    direction = str(direction or "").upper().strip()
    if direction == "LONG":
        return "SHORT"
    if direction == "SHORT":
        return "LONG"
    return ""


def currency_direction_keys(instrument: Any, direction: str) -> list[str]:
    base, quote = split_instrument(instrument)
    direction = str(direction or "").upper().strip()
    if not base or not quote:
        return []
    if direction == "LONG":
        return [f"{base}_LONG", f"{quote}_SHORT"]
    if direction == "SHORT":
        return [f"{base}_SHORT", f"{quote}_LONG"]
    return []


def pip_size_for_instrument(instrument: Any, manager: Any | None = None) -> float:
    inst = normalize_instrument(instrument)
    try:
        meta = getattr(manager, "instrument_meta", {}).get(inst) if manager is not None else None
        pip = safe_float(getattr(meta, "pip_size", 0.0), 0.0)
        if pip > 0:
            return pip
    except Exception:
        pass
    _, quote = split_instrument(inst)
    if quote in {"JPY", "HUF"}:
        return 0.01
    if quote in {"SEK", "NOK", "DKK", "CZK", "MXN", "TRY", "ZAR", "HKD", "CNH", "SGD"}:
        return 0.0001
    return 0.0001


def score_bucket(score: float | None) -> str:
    if score is None or not math.isfinite(score):
        return "unscored"
    if score < 70:
        return "lt70"
    if score < 80:
        return "70_79"
    if score < 90:
        return "80_89"
    if score < 95:
        return "90_94"
    return "95_plus"


def score_from_text(text: Any) -> float | None:
    value = str(text or "")
    patterns = [
        r"(?:ev_score|scout_ev_score|score|outlook_confidence|confidence)\s*[=:]\s*([0-9]+(?:\.[0-9]+)?)",
        r"\b([0-9]+(?:\.[0-9]+)?)\s*(?:score|confidence)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, value, flags=re.IGNORECASE)
        if match:
            score = safe_float(match.group(1), math.nan)
            if math.isfinite(score):
                return score
    return None


def score_from_action(action: dict[str, Any] | None, fallback_reason: Any = "") -> float | None:
    action = action or {}
    for key in (
        "scout_ev_score",
        "_scout_ev_score",
        "ev_score",
        "score",
        "_score",
    ):
        if key in action:
            score = safe_float(action.get(key), math.nan)
            if math.isfinite(score) and score > 0:
                return score
    text_score = score_from_text(action.get("reason") or action.get("why_now") or fallback_reason)
    if text_score is not None:
        return text_score
    for key in ("outlook_confidence", "confidence"):
        if key in action:
            score = safe_float(action.get(key), math.nan)
            if math.isfinite(score) and score > 0:
                return score
    return score_from_text(fallback_reason)


def order_demote_reject_reason(order: dict[str, Any], state: dict[str, Any] | None = None) -> str:
    state = state or {}
    demotions = state.get(DEMOTION_STATE_KEY)
    if not isinstance(demotions, dict) or not demotions.get("active"):
        return ""
    if demotions.get("date_ny") != ny_date():
        return ""
    if demotions.get("enforcement_default") is False and not demotions.get("enforcement_enabled"):
        return ""
    score = score_from_action(order)
    bucket = score_bucket(score)
    blocked = demotions.get("demoted_buckets") or {}
    if bucket not in blocked:
        return ""
    detail = blocked.get(bucket) if isinstance(blocked.get(bucket), dict) else {}
    score_text = "unscored" if score is None else f"{score:.1f}"
    return (
        f"live model score demotion blocked score bucket {bucket} "
        f"(score={score_text}; loss_rate={safe_float(detail.get('loss_rate'), 0.0):.0%}; "
        f"net_pl={safe_float(detail.get('realized_pl'), 0.0):.4f})"
    )


def theme_from_action(action: dict[str, Any], reason: str = "") -> str:
    for key in ("_movement_theme", "movement_theme", "theme", "_theme"):
        value = str(action.get(key) or "").strip().upper()
        if value:
            return value
    campaign = str(action.get("_campaign_key") or "").strip()
    if ":" in campaign:
        return campaign.split(":", 1)[0].strip().upper()
    match = re.search(r"event scout\s+([A-Z0-9_]+)", reason, flags=re.IGNORECASE)
    if match:
        return match.group(1).upper()
    if action.get("_event_scout"):
        return "EVENT_SCOUT"
    return "GPT_OR_MANUAL"


def model_source_from_action(action: dict[str, Any], reason: str = "") -> str:
    text = (str(reason or "") + " " + json.dumps(action, default=str)[:2000]).lower()
    if action.get("_synthetic_strength_cross") or "synthetic_strength_cross" in text:
        return "synthetic_strength_cross"
    if "ensemble" in text:
        return "ensemble_fallback"
    if "exhaustion" in text:
        return "exhaustion_hybrid"
    if "volatile_lane" in text:
        return "volatile_lane"
    if "broad_regime" in text or action.get("_scout_regime_lock_label"):
        return "broad_regime"
    if action.get("_event_scout"):
        return "event_scout"
    if action.get("outlook_confidence") or action.get("portfolio_mode") or action.get("why_now"):
        return "gpt_decision"
    return "unknown"


def stop_type_from_close(row: dict[str, Any], raw: dict[str, Any]) -> str:
    reason = str(row.get("reason") or raw.get("reason") or "").upper()
    typ = str(raw.get("type") or row.get("event") or "").upper()
    text = f"{reason} {typ}"
    if "TRAILING_STOP" in text:
        return "trailing_stop"
    if "STOP_LOSS" in text:
        return "stop_loss"
    if "TAKE_PROFIT" in text:
        return "take_profit"
    if "MARGIN" in text or "CLOSEOUT" in text:
        return "margin_or_closeout"
    if "MARKET_ORDER" in text or "CLIENT_ORDER" in text:
        return "manager_close"
    return reason.lower() or typ.lower() or "unknown"


def trade_open_index(account_dir: Path, date_ny: str | None = None) -> dict[str, dict[str, Any]]:
    opens: dict[str, dict[str, Any]] = {}
    for row in csv_rows(account_dir / "order_result_ledger.csv"):
        status = str(row.get("status") or "").lower().strip()
        source = str(row.get("source") or "").lower().strip()
        if status and not status.startswith("accepted"):
            continue
        if source and source not in {"open", "scale_in", "event_scout"}:
            continue
        raw = json_cell(row.get("raw_json"))
        action = raw.get("action") if isinstance(raw.get("action"), dict) else {}
        result = raw.get("result") if isinstance(raw.get("result"), dict) else {}
        fill = result.get("orderFillTransaction") if isinstance(result.get("orderFillTransaction"), dict) else {}
        create = result.get("orderCreateTransaction") if isinstance(result.get("orderCreateTransaction"), dict) else {}
        trade_id = str(row.get("trade_id") or (fill.get("tradeOpened") or {}).get("tradeID") or "").strip()
        if not trade_id:
            continue
        inst = normalize_instrument(row.get("instrument") or action.get("instrument") or fill.get("instrument"))
        direction = str(row.get("direction") or action.get("direction") or "").upper().strip()
        if direction not in {"LONG", "SHORT"}:
            direction = direction_from_units(row.get("units") or fill.get("units"))
        reason = str(row.get("reason") or action.get("reason") or "")
        score = score_from_action(action, reason)
        open_time_utc = str(row.get("time_utc") or fill.get("time") or create.get("time") or "")
        opens[trade_id] = {
            "trade_id": trade_id,
            "instrument": inst,
            "direction": direction,
            "time_utc": open_time_utc,
            "time_ny": row.get("time_ny", ""),
            "source": source or "open",
            "event_type": source or "open",
            "reason": reason,
            "action": action,
            "theme": theme_from_action(action, reason),
            "model_source": model_source_from_action(action, reason),
            "movement_key": action.get("_movement_key", ""),
            "score": score,
            "score_bucket": score_bucket(score),
            "risk_pct": safe_float(row.get("risk_pct") or action.get("risk_pct"), 0.0),
            "entry_price": safe_float(row.get("price") or fill.get("price"), 0.0),
            "stop_loss": safe_float(action.get("stop_loss"), 0.0),
            "take_profit": safe_float(action.get("take_profit") or action.get("tp1"), 0.0),
            "trailing_stop_pips": safe_float(action.get("trailing_stop_pips"), 0.0),
        }

    for row in csv_rows(account_dir / "trade_lifecycle_ledger.csv"):
        if str(row.get("reason") or "").upper().strip() != "MARKET_ORDER":
            continue
        trade_id = str(row.get("trade_id") or "").strip()
        if not trade_id:
            continue
        opens.setdefault(
            trade_id,
            {
                "trade_id": trade_id,
                "instrument": normalize_instrument(row.get("instrument")),
                "direction": str(row.get("direction") or "").upper().strip(),
                "time_utc": row.get("time_utc", ""),
                "time_ny": row.get("time_ny", ""),
                "source": "market_order",
                "event_type": "market_order",
                "reason": row.get("reason", ""),
                "action": {},
                "theme": "UNKNOWN",
                "model_source": "unknown",
                "movement_key": "",
                "score": None,
                "score_bucket": "unscored",
                "risk_pct": 0.0,
                "entry_price": safe_float(row.get("price"), 0.0),
                "stop_loss": 0.0,
                "take_profit": 0.0,
                "trailing_stop_pips": 0.0,
            },
        )
    if date_ny:
        return {k: v for k, v in opens.items() if row_ny_date(v) <= date_ny}
    return opens


def closed_trade_records(account_dir: Path, date_ny: str, manager: Any | None = None) -> list[dict[str, Any]]:
    opens = trade_open_index(account_dir, date_ny)
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in csv_rows(account_dir / "trade_lifecycle_ledger.csv"):
        if row_ny_date(row) != date_ny:
            continue
        reason = str(row.get("reason") or "").upper().strip()
        if reason == "MARKET_ORDER":
            continue
        raw = json_cell(row.get("raw_json"))
        closed_items = raw.get("tradesClosed")
        if not isinstance(closed_items, list) or not closed_items:
            closed_items = [
                {
                    "tradeID": row.get("trade_id", ""),
                    "realizedPL": row.get("pl", ""),
                    "units": row.get("units", ""),
                }
            ]
        tx_id = str(row.get("transaction_id") or raw.get("id") or "").strip()
        for idx, closed in enumerate(closed_items):
            if not isinstance(closed, dict):
                continue
            trade_id = str(closed.get("tradeID") or row.get("trade_id") or "").strip()
            key = (tx_id or str(row.get("time_utc") or ""), trade_id or str(idx))
            if key in seen:
                continue
            seen.add(key)
            pl = safe_float(closed.get("realizedPL", row.get("pl")), 0.0)
            if abs(pl) <= 1e-9:
                continue
            open_info = opens.get(trade_id, {})
            inst = normalize_instrument(open_info.get("instrument") or row.get("instrument"))
            direction = str(open_info.get("direction") or "").upper().strip()
            if direction not in {"LONG", "SHORT"}:
                direction = opposite_direction(str(row.get("direction") or ""))
            close_price = safe_float(row.get("price") or raw.get("price"), 0.0)
            entry = safe_float(open_info.get("entry_price"), 0.0)
            pip = pip_size_for_instrument(inst, manager)
            actual_pips = 0.0
            if entry > 0 and close_price > 0 and pip > 0 and direction in {"LONG", "SHORT"}:
                actual_pips = (close_price - entry) / pip if direction == "LONG" else (entry - close_price) / pip
            score = open_info.get("score")
            if score is not None:
                score = safe_float(score, math.nan)
                if not math.isfinite(score):
                    score = None
            records.append(
                {
                    "trade_id": trade_id,
                    "instrument": inst,
                    "direction": direction,
                    "realized_pl": round(pl, 6),
                    "actual_pips": round(actual_pips, 3),
                    "entry_price": entry,
                    "close_price": close_price,
                    "open_time_utc": open_info.get("time_utc", ""),
                    "open_time_ny": open_info.get("time_ny", ""),
                    "close_time_utc": row.get("time_utc", ""),
                    "close_time_ny": row.get("time_ny", ""),
                    "close_reason": reason,
                    "stop_type": stop_type_from_close(row, raw),
                    "event_type": open_info.get("event_type") or open_info.get("source") or "unknown",
                    "theme": open_info.get("theme") or "UNKNOWN",
                    "model_source": open_info.get("model_source") or "unknown",
                    "movement_key": open_info.get("movement_key", ""),
                    "score": score,
                    "score_bucket": open_info.get("score_bucket") or score_bucket(score),
                    "risk_pct": round(safe_float(open_info.get("risk_pct"), 0.0), 6),
                    "stop_loss": open_info.get("stop_loss", 0.0),
                    "take_profit": open_info.get("take_profit", 0.0),
                    "trailing_stop_pips": open_info.get("trailing_stop_pips", 0.0),
                    "currency_keys": currency_direction_keys(inst, direction),
                    "reason": open_info.get("reason", ""),
                }
            )
    records.sort(key=lambda rec: str(rec.get("close_time_utc") or rec.get("close_time_ny") or ""))
    return records


def summarize_group(records: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for rec in records:
        value = str(rec.get(key) or "unknown")
        item = out.setdefault(value, {"closed": 0, "wins": 0, "losses": 0, "realized_pl": 0.0})
        pl = safe_float(rec.get("realized_pl"), 0.0)
        item["closed"] += 1
        item["realized_pl"] = round(safe_float(item.get("realized_pl"), 0.0) + pl, 6)
        if pl > 0:
            item["wins"] += 1
        elif pl < 0:
            item["losses"] += 1
    for item in out.values():
        closed = max(1, safe_int(item.get("closed"), 0))
        item["loss_rate"] = round(safe_int(item.get("losses"), 0) / closed, 6)
        item["avg_pl"] = round(safe_float(item.get("realized_pl"), 0.0) / closed, 6)
    return dict(sorted(out.items(), key=lambda kv: safe_float(kv[1].get("realized_pl"), 0.0)))


def reentry_summary(records: list[dict[str, Any]], opens: dict[str, dict[str, Any]]) -> dict[str, Any]:
    open_items = list(opens.values())
    per_loss: list[dict[str, Any]] = []
    for rec in records:
        if safe_float(rec.get("realized_pl"), 0.0) >= 0:
            continue
        close_time = str(rec.get("close_time_utc") or "")
        inst = normalize_instrument(rec.get("instrument"))
        direction = str(rec.get("direction") or "").upper()
        keys = set(rec.get("currency_keys") or [])
        same_pair_same_dir = []
        same_pair_opposite = []
        same_currency = []
        for opened in open_items:
            if str(opened.get("time_utc") or "") <= close_time:
                continue
            opened_inst = normalize_instrument(opened.get("instrument"))
            opened_dir = str(opened.get("direction") or "").upper()
            opened_keys = set(currency_direction_keys(opened_inst, opened_dir))
            item = {
                "trade_id": opened.get("trade_id", ""),
                "instrument": opened_inst,
                "direction": opened_dir,
                "time_utc": opened.get("time_utc", ""),
                "theme": opened.get("theme", ""),
                "score_bucket": opened.get("score_bucket", "unscored"),
            }
            if opened_inst == inst and opened_dir == direction:
                same_pair_same_dir.append(item)
            elif opened_inst == inst and opened_dir == opposite_direction(direction):
                same_pair_opposite.append(item)
            elif keys.intersection(opened_keys):
                same_currency.append(item)
        if same_pair_same_dir or same_pair_opposite or same_currency:
            per_loss.append(
                {
                    "closed_trade_id": rec.get("trade_id"),
                    "instrument": inst,
                    "direction": direction,
                    "closed_time_utc": close_time,
                    "same_pair_same_direction": same_pair_same_dir[:5],
                    "same_pair_opposite": same_pair_opposite[:5],
                    "same_currency_direction": same_currency[:8],
                }
            )
    return {
        "losses_with_reentry": len(per_loss),
        "details": per_loss[-20:],
    }


def fetch_candles(oanda_client: Any, instrument: str, start_utc: str, end_utc: str) -> list[dict[str, Any]]:
    if not oanda_client or not hasattr(oanda_client, "request"):
        return []
    start = parse_dt(start_utc)
    end = parse_dt(end_utc)
    if not start or not end or end <= start:
        return []
    max_span = dt.timedelta(hours=30)
    if end - start > max_span:
        start = end - max_span
    try:
        resp = oanda_client.request(
            "GET",
            f"/v3/instruments/{normalize_instrument(instrument)}/candles",
            params={
                "price": "M",
                "granularity": "M1",
                "from": start.isoformat().replace("+00:00", "Z"),
                "to": end.isoformat().replace("+00:00", "Z"),
            },
        )
        candles = resp.get("candles", []) if isinstance(resp, dict) else []
        return [c for c in candles if isinstance(c, dict) and c.get("complete", True)]
    except Exception:
        return []


def candle_mid_ohlc(candle: dict[str, Any]) -> tuple[float, float, float, float] | None:
    mid = candle.get("mid") if isinstance(candle.get("mid"), dict) else {}
    if not mid:
        return None
    o = safe_float(mid.get("o"), math.nan)
    h = safe_float(mid.get("h"), math.nan)
    l = safe_float(mid.get("l"), math.nan)
    c = safe_float(mid.get("c"), math.nan)
    if not all(math.isfinite(x) for x in (o, h, l, c)):
        return None
    return o, h, l, c


def simulate_trailing_pips(
    entry: float,
    close_price: float,
    direction: str,
    pip_size: float,
    candles: list[dict[str, Any]],
    trail_pips: float,
) -> float | None:
    if entry <= 0 or close_price <= 0 or pip_size <= 0 or trail_pips <= 0 or not candles:
        return None
    direction = direction.upper()
    distance = trail_pips * pip_size
    if direction == "LONG":
        stop = entry - distance
        for candle in candles:
            ohlc = candle_mid_ohlc(candle)
            if not ohlc:
                continue
            _, high, low, _ = ohlc
            stop = max(stop, high - distance)
            if low <= stop:
                return (stop - entry) / pip_size
        return (close_price - entry) / pip_size
    if direction == "SHORT":
        stop = entry + distance
        for candle in candles:
            ohlc = candle_mid_ohlc(candle)
            if not ohlc:
                continue
            _, high, low, _ = ohlc
            stop = min(stop, low + distance)
            if high >= stop:
                return (entry - stop) / pip_size
        return (entry - close_price) / pip_size
    return None


def stop_audit(records: list[dict[str, Any]], manager: Any | None = None) -> dict[str, Any]:
    candidates = [10.0, 15.0, 20.0, 30.0, 40.0, 50.0]
    oanda_client = getattr(manager, "oanda", None) if manager is not None else None
    rows: list[dict[str, Any]] = []
    improved_counts = {str(int(x)): 0 for x in candidates}
    audited = 0
    for rec in records:
        if safe_float(rec.get("realized_pl"), 0.0) >= 0:
            continue
        inst = normalize_instrument(rec.get("instrument"))
        direction = str(rec.get("direction") or "").upper()
        entry = safe_float(rec.get("entry_price"), 0.0)
        close = safe_float(rec.get("close_price"), 0.0)
        pip = pip_size_for_instrument(inst, manager)
        actual_pips = safe_float(rec.get("actual_pips"), 0.0)
        candles = fetch_candles(oanda_client, inst, str(rec.get("open_time_utc") or ""), str(rec.get("close_time_utc") or ""))
        row = {
            "trade_id": rec.get("trade_id"),
            "instrument": inst,
            "direction": direction,
            "actual_pips": round(actual_pips, 3),
            "actual_trailing_stop_pips": safe_float(rec.get("trailing_stop_pips"), 0.0),
            "status": "audited" if candles else "insufficient_candles",
            "candidate_results": {},
        }
        if candles:
            audited += 1
            for cand in candidates:
                simulated = simulate_trailing_pips(entry, close, direction, pip, candles, cand)
                if simulated is None:
                    continue
                improvement = simulated - actual_pips
                helped = improvement > 1.0
                if helped:
                    improved_counts[str(int(cand))] += 1
                row["candidate_results"][str(int(cand))] = {
                    "simulated_pips": round(simulated, 3),
                    "improvement_pips": round(improvement, 3),
                    "helped": helped,
                }
        rows.append(row)
    best = ""
    if audited:
        best = max(improved_counts, key=lambda k: improved_counts[k])
    return {
        "audited_losing_trades": audited,
        "losing_trades": len([r for r in records if safe_float(r.get("realized_pl"), 0.0) < 0]),
        "candidate_trailing_stop_pips": candidates,
        "helped_counts_by_trailing_stop_pips": improved_counts,
        "most_helpful_candidate_pips": best,
        "records": rows[-30:],
    }


def build_demotions(
    calibration: dict[str, dict[str, Any]],
    *,
    date_ny: str,
    min_closed: int = 2,
    max_loss_rate: float = 0.67,
) -> dict[str, Any]:
    demoted: dict[str, Any] = {}
    for bucket, stats in calibration.items():
        if bucket == "unscored":
            continue
        closed = safe_int(stats.get("closed"), 0)
        loss_rate = safe_float(stats.get("loss_rate"), 0.0)
        realized = safe_float(stats.get("realized_pl"), 0.0)
        if closed >= min_closed and loss_rate >= max_loss_rate and realized < 0:
            demoted[bucket] = {
                "closed": closed,
                "losses": safe_int(stats.get("losses"), 0),
                "loss_rate": round(loss_rate, 6),
                "realized_pl": round(realized, 6),
                "action": "report_only_recommend_shadow_or_manual_block",
            }
    return {
        "active": bool(demoted),
        "date_ny": date_ny,
        "generated_utc": iso_utc(),
        "min_closed_trades": min_closed,
        "max_loss_rate": max_loss_rate,
        "enforcement_default": False,
        "demoted_buckets": demoted,
    }


def latest_monitor_metrics(account_dir: Path, date_ny: str) -> dict[str, Any]:
    candidates = []
    for name in ("monitor.csv", "account_monitor.csv"):
        candidates.extend(csv_rows(account_dir / name))
    today = [row for row in candidates if row_ny_date(row) == date_ny]
    if not today:
        return {}
    row = today[-1]
    return {
        "nav": safe_float(row.get("nav"), 0.0),
        "balance": safe_float(row.get("balance"), 0.0),
        "margin_used_pct": safe_float(row.get("margin_used_pct"), 0.0),
    }


def record_close_date(record: dict[str, Any]) -> str:
    return row_ny_date({"time_ny": record.get("close_time_ny"), "time_utc": record.get("close_time_utc")})


def closed_trade_dates(account_dir: Path) -> list[str]:
    dates = {
        row_ny_date(row)
        for row in csv_rows(account_dir / "trade_lifecycle_ledger.csv")
        if row_ny_date(row)
        and str(row.get("reason") or "").upper().strip() != "MARKET_ORDER"
        and abs(safe_float(row.get("pl"), 0.0)) > 1e-9
    }
    return sorted(dates)


def closed_trade_history(
    account_dir: Path,
    manager: Any | None = None,
    *,
    lookback_days: int = 60,
) -> list[dict[str, Any]]:
    cutoff = dt.datetime.now(NY_TZ).date() - dt.timedelta(days=max(1, lookback_days))
    out: list[dict[str, Any]] = []
    for date in closed_trade_dates(account_dir):
        try:
            if dt.date.fromisoformat(date) < cutoff:
                continue
        except Exception:
            continue
        out.extend(closed_trade_records(account_dir, date, manager))
    out.sort(key=lambda rec: str(rec.get("close_time_utc") or rec.get("close_time_ny") or ""))
    return out


def basic_trade_stats(records: list[dict[str, Any]]) -> dict[str, Any]:
    closed = len(records)
    wins = len([r for r in records if safe_float(r.get("realized_pl"), 0.0) > 0])
    losses = len([r for r in records if safe_float(r.get("realized_pl"), 0.0) < 0])
    realized = round(sum(safe_float(r.get("realized_pl"), 0.0) for r in records), 6)
    dates = sorted({record_close_date(r) for r in records if record_close_date(r)})
    return {
        "closed": closed,
        "wins": wins,
        "losses": losses,
        "realized_pl": realized,
        "loss_rate": round(losses / closed, 6) if closed else 0.0,
        "avg_pl": round(realized / closed, 6) if closed else 0.0,
        "distinct_dates": len(dates),
        "dates": dates,
    }


def scored_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for rec in records:
        score = rec.get("score")
        if score is None:
            continue
        value = safe_float(score, math.nan)
        if math.isfinite(value):
            row = dict(rec)
            row["score"] = value
            out.append(row)
    return out


def score_threshold_backtests(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = [60.0, 70.0, 75.0, 80.0, 85.0, 88.0, 90.0, 92.0, 95.0]
    scored = scored_records(records)
    baseline_pl = round(sum(safe_float(r.get("realized_pl"), 0.0) for r in scored), 6)
    results = []
    for threshold in candidates:
        kept = [r for r in scored if safe_float(r.get("score"), 0.0) >= threshold]
        blocked = [r for r in scored if safe_float(r.get("score"), 0.0) < threshold]
        kept_pl = round(sum(safe_float(r.get("realized_pl"), 0.0) for r in kept), 6)
        blocked_pl = round(sum(safe_float(r.get("realized_pl"), 0.0) for r in blocked), 6)
        improvement = round(kept_pl - baseline_pl, 6)
        result = {
            "threshold": threshold,
            "baseline_closed": len(scored),
            "kept_closed": len(kept),
            "blocked_closed": len(blocked),
            "baseline_pl": baseline_pl,
            "projected_pl_if_blocked": kept_pl,
            "blocked_realized_pl": blocked_pl,
            "improvement_vs_actual": improvement,
            "kept_loss_rate": basic_trade_stats(kept)["loss_rate"] if kept else 0.0,
            "blocked_loss_rate": basic_trade_stats(blocked)["loss_rate"] if blocked else 0.0,
            "helped": improvement > 0 and bool(blocked),
        }
        results.append(result)
    return results


def walk_forward_threshold_backtest(records: list[dict[str, Any]]) -> dict[str, Any]:
    scored = scored_records(records)
    dates = sorted({record_close_date(r) for r in scored if record_close_date(r)})
    runs: list[dict[str, Any]] = []
    for idx, test_date in enumerate(dates):
        train_dates = set(dates[:idx])
        if len(train_dates) < 2:
            continue
        train = [r for r in scored if record_close_date(r) in train_dates]
        test = [r for r in scored if record_close_date(r) == test_date]
        if len(train) < 8 or not test:
            continue
        tests = score_threshold_backtests(train)
        usable = [
            t
            for t in tests
            if t["helped"]
            and safe_int(t.get("kept_closed"), 0) >= 2
            and safe_int(t.get("blocked_closed"), 0) >= 1
        ]
        if not usable:
            continue
        chosen = max(
            usable,
            key=lambda t: (
                safe_float(t.get("improvement_vs_actual"), 0.0),
                safe_float(t.get("projected_pl_if_blocked"), 0.0),
            ),
        )
        threshold = safe_float(chosen.get("threshold"), 0.0)
        baseline = round(sum(safe_float(r.get("realized_pl"), 0.0) for r in test), 6)
        kept = [r for r in test if safe_float(r.get("score"), 0.0) >= threshold]
        projected = round(sum(safe_float(r.get("realized_pl"), 0.0) for r in kept), 6)
        runs.append(
            {
                "test_date": test_date,
                "chosen_threshold": threshold,
                "test_closed": len(test),
                "kept_closed": len(kept),
                "blocked_closed": len(test) - len(kept),
                "actual_pl": baseline,
                "projected_pl": projected,
                "improvement": round(projected - baseline, 6),
            }
        )
    actual = round(sum(safe_float(r.get("actual_pl"), 0.0) for r in runs), 6)
    projected = round(sum(safe_float(r.get("projected_pl"), 0.0) for r in runs), 6)
    improvement = round(projected - actual, 6)
    return {
        "eligible": bool(runs),
        "runs": runs[-20:],
        "run_count": len(runs),
        "actual_pl": actual,
        "projected_pl": projected,
        "improvement": improvement,
        "passed": bool(runs) and improvement > 0,
    }


def group_quarantine_candidates(
    records: list[dict[str, Any]],
    key: str,
    *,
    min_group_trades: int,
    min_group_dates: int,
    max_loss_rate: float,
) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for rec in records:
        value = str(rec.get(key) or "unknown")
        if not value or value == "unknown":
            continue
        groups.setdefault(value, []).append(rec)
    candidates: list[dict[str, Any]] = []
    for value, rows in groups.items():
        stats = basic_trade_stats(rows)
        improvement = round(-safe_float(stats.get("realized_pl"), 0.0), 6)
        enough = (
            safe_int(stats.get("closed"), 0) >= min_group_trades
            and safe_int(stats.get("distinct_dates"), 0) >= min_group_dates
        )
        bad = safe_float(stats.get("loss_rate"), 0.0) >= max_loss_rate and safe_float(stats.get("realized_pl"), 0.0) < 0
        if not bad:
            continue
        candidates.append(
            {
                "dimension": key,
                "value": value,
                "stats": stats,
                "historical_block_improvement": improvement,
                "status": "shadow_ready" if enough else "needs_more_history",
                "reason": (
                    "negative group with enough backtest sample"
                    if enough
                    else "negative group but not enough closed-trade dates/sample for shadow"
                ),
            }
        )
    candidates.sort(
        key=lambda c: (
            c.get("status") == "shadow_ready",
            safe_float(c.get("historical_block_improvement"), 0.0),
            safe_int((c.get("stats") or {}).get("closed"), 0),
        ),
        reverse=True,
    )
    return candidates


def stop_policy_learning_from_recap(recap: dict[str, Any] | None) -> dict[str, Any]:
    stop = (recap or {}).get("tighter_stop_audit") or {}
    audited = safe_int(stop.get("audited_losing_trades"), 0)
    helped = stop.get("helped_counts_by_trailing_stop_pips") or {}
    best = str(stop.get("most_helpful_candidate_pips") or "")
    best_helped = safe_int(helped.get(best), 0) if best else 0
    ratio = round(best_helped / audited, 6) if audited else 0.0
    status = "shadow_ready" if audited >= 10 and ratio >= 0.6 else "needs_more_audited_losses"
    return {
        "status": status,
        "audited_losing_trades": audited,
        "candidate_trailing_stop_pips": stop.get("candidate_trailing_stop_pips") or [],
        "helped_counts_by_trailing_stop_pips": helped,
        "recommended_shadow_trailing_stop_pips": safe_float(best, 0.0) if best else 0.0,
        "help_ratio": ratio,
        "reason": (
            "enough audited losing trades for shadow stop-policy test"
            if status == "shadow_ready"
            else "collect more audited losing trades before shadowing stop changes"
        ),
    }


def build_live_learning_recommendations(
    manager: Any,
    *,
    recap: dict[str, Any] | None = None,
    lookback_days: int = 60,
) -> dict[str, Any]:
    cfg = manager.cfg
    account_dir = Path(cfg.data_dir)
    records = closed_trade_history(account_dir, manager, lookback_days=lookback_days)
    scored = scored_records(records)
    gates = {
        "lookback_days": lookback_days,
        "min_closed_trades_for_shadow": 20,
        "min_distinct_dates_for_shadow": 3,
        "min_group_trades_for_shadow": 5,
        "max_group_loss_rate_for_shadow": 0.67,
        "promotion_requires": "manual review or separate shadow/live promotion process after backtest",
    }
    history_stats = basic_trade_stats(records)
    scored_stats = basic_trade_stats(scored)
    enough_history = (
        safe_int(history_stats.get("closed"), 0) >= gates["min_closed_trades_for_shadow"]
        and safe_int(history_stats.get("distinct_dates"), 0) >= gates["min_distinct_dates_for_shadow"]
    )
    threshold_tests = score_threshold_backtests(records)
    best_threshold = None
    helped_thresholds = [
        test
        for test in threshold_tests
        if test.get("helped")
        and safe_int(test.get("kept_closed"), 0) >= 2
        and safe_int(test.get("blocked_closed"), 0) >= 1
    ]
    if helped_thresholds:
        best_threshold = max(
            helped_thresholds,
            key=lambda t: (
                safe_float(t.get("improvement_vs_actual"), 0.0),
                safe_float(t.get("projected_pl_if_blocked"), 0.0),
            ),
        )
    walk_forward = walk_forward_threshold_backtest(records)
    group_dimensions = ["score_bucket", "theme", "model_source", "event_type", "instrument"]
    group_candidates = {
        dim: group_quarantine_candidates(
            records,
            dim,
            min_group_trades=gates["min_group_trades_for_shadow"],
            min_group_dates=gates["min_distinct_dates_for_shadow"],
            max_loss_rate=gates["max_group_loss_rate_for_shadow"],
        )
        for dim in group_dimensions
    }
    shadow_candidates = []
    if enough_history and walk_forward.get("passed") and best_threshold:
        shadow_candidates.append(
            {
                "type": "score_threshold",
                "action": "shadow_block_scores_below_threshold",
                "threshold": best_threshold.get("threshold"),
                "backtest": best_threshold,
                "walk_forward": {
                    "run_count": walk_forward.get("run_count"),
                    "improvement": walk_forward.get("improvement"),
                    "projected_pl": walk_forward.get("projected_pl"),
                    "actual_pl": walk_forward.get("actual_pl"),
                },
            }
        )
    for dim, candidates in group_candidates.items():
        for candidate in candidates:
            if enough_history and candidate.get("status") == "shadow_ready":
                shadow_candidates.append(
                    {
                        "type": "group_quarantine",
                        "action": "shadow_block_group",
                        "dimension": dim,
                        "value": candidate.get("value"),
                        "stats": candidate.get("stats"),
                        "historical_block_improvement": candidate.get("historical_block_improvement"),
                    }
                )
    stop_policy = stop_policy_learning_from_recap(recap)
    if enough_history and stop_policy.get("status") == "shadow_ready":
        shadow_candidates.append(
            {
                "type": "stop_policy",
                "action": "shadow_tighter_trailing_stop",
                "trailing_stop_pips": stop_policy.get("recommended_shadow_trailing_stop_pips"),
                "audit": stop_policy,
            }
        )

    recommendations: list[str] = []
    if not records:
        recommendations.append("No closed-trade history yet; collect labels before learning thresholds.")
    elif not enough_history:
        recommendations.append(
            "Closed-trade history is not large enough for shadow promotion; keep recommendations in report-only mode."
        )
    if best_threshold and safe_float(best_threshold.get("improvement_vs_actual"), 0.0) > 0:
        recommendations.append(
            f"Backtest candidate: shadow a minimum score threshold of {best_threshold.get('threshold')} "
            f"only after history gates pass."
        )
    if stop_policy.get("recommended_shadow_trailing_stop_pips"):
        recommendations.append(
            "Backtest candidate: shadow trailing stop cap "
            f"{stop_policy.get('recommended_shadow_trailing_stop_pips')} pips after stop-audit sample grows."
        )
    for dim, candidates in group_candidates.items():
        if candidates:
            top = candidates[0]
            recommendations.append(
                f"Watch {dim}={top.get('value')} for quarantine; status={top.get('status')} "
                f"pl={(top.get('stats') or {}).get('realized_pl')}."
            )

    return {
        "generated_utc": iso_utc(),
        "date_ny": ny_date(),
        "account_lane": getattr(cfg, "account_lane", ""),
        "account_display_name": getattr(cfg, "account_display_name", ""),
        "mode": "shadow_ready" if shadow_candidates else "recommendation_only",
        "account_independence": {
            "independent_accounts": True,
            "cross_account_exposure_cap_default": False,
            "note": "Recommendations and thresholds are account-local unless explicitly overridden.",
        },
        "gates": gates,
        "history_stats": history_stats,
        "scored_history_stats": scored_stats,
        "enough_history_for_shadow": enough_history,
        "score_threshold_backtests": threshold_tests,
        "best_score_threshold_candidate": best_threshold or {},
        "walk_forward_threshold_backtest": walk_forward,
        "group_quarantine_candidates": group_candidates,
        "stop_policy_candidate": stop_policy,
        "shadow_candidates": shadow_candidates,
        "recommendations": recommendations,
        "promotion_policy": {
            "auto_apply_live": False,
            "auto_loosen_live": False,
            "shadow_before_live": True,
            "manual_review_required_for_live_promotion": True,
        },
    }


def render_learning_markdown(learning: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"# Live Learning Recommendations - {learning.get('account_lane')} - {learning.get('date_ny')}")
    lines.append("")
    lines.append(f"Generated UTC: {learning.get('generated_utc')}")
    lines.append(f"Mode: {learning.get('mode')}")
    stats = learning.get("history_stats") or {}
    lines.append(
        f"History: closed={stats.get('closed')} dates={stats.get('distinct_dates')} "
        f"pl={stats.get('realized_pl')} loss_rate={stats.get('loss_rate')}"
    )
    lines.append(f"Enough history for shadow: {learning.get('enough_history_for_shadow')}")
    lines.append("")
    lines.append("## Recommendations")
    recs = learning.get("recommendations") or []
    if recs:
        for item in recs[:20]:
            lines.append(f"- {item}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Shadow Candidates")
    candidates = learning.get("shadow_candidates") or []
    if candidates:
        for item in candidates[:20]:
            lines.append(f"- {item.get('type')}: {item.get('action')} {item}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Best Score Threshold")
    lines.append(json.dumps(learning.get("best_score_threshold_candidate") or {}, sort_keys=True))
    lines.append("")
    lines.append("## Stop Policy")
    lines.append(json.dumps(learning.get("stop_policy_candidate") or {}, sort_keys=True))
    return "\n".join(lines) + "\n"


def build_loss_review_prompt(recap: dict[str, Any], learning: dict[str, Any]) -> str:
    lane = recap.get("account_lane") or learning.get("account_lane") or "unknown"
    return (
        "You are the model-improvement advisor for an independent live OANDA account.\n"
        "\n"
        "Account policy:\n"
        "- Do not recommend a generic failsafe as the primary fix.\n"
        "- Treat losses as evidence that the model, threshold, feature set, or trade-selection logic needs improvement.\n"
        "- Recommend only changes that can be backtested before shadow mode.\n"
        "- Keep recommendations account-local; do not rely on cross-account exposure caps.\n"
        "- Distinguish immediate live risk management from model research. For live changes, prefer report-only or shadow first.\n"
        "\n"
        "Required output JSON shape:\n"
        "{\n"
        '  "root_cause_hypotheses": [\n'
        '    {"hypothesis": "...", "evidence": ["..."], "confidence": 0.0, "backtest": "..."}\n'
        "  ],\n"
        '  "model_changes_to_test": [\n'
        '    {"change": "...", "expected_effect": "...", "required_backtest": "...", "shadow_gate": "..."}\n'
        "  ],\n"
        '  "threshold_changes_to_backtest": [\n'
        '    {"threshold": "...", "reason": "...", "do_not_apply_live_until": "..."}\n'
        "  ],\n"
        '  "feature_or_label_fixes": [\n'
        '    {"fix": "...", "why": "...", "validation": "..."}\n'
        "  ],\n"
        '  "trade_selection_notes": [\n'
        '    {"note": "...", "affected_pairs_or_themes": ["..."]}\n'
        "  ],\n"
        '  "do_not_change": ["..."]\n'
        "}\n"
        "\n"
        f"Account lane: {lane}\n"
        "\n"
        "Daily recap JSON:\n"
        "```json\n"
        f"{json.dumps(recap, indent=2, sort_keys=True, default=str)[:50000]}\n"
        "```\n"
        "\n"
        "Learning/backtest recommendation JSON:\n"
        "```json\n"
        f"{json.dumps(learning, indent=2, sort_keys=True, default=str)[:50000]}\n"
        "```\n"
    )


def crisis_review_triggers(recap: dict[str, Any], learning: dict[str, Any]) -> list[str]:
    totals = recap.get("totals") or {}
    history = learning.get("history_stats") or {}
    dashboard = recap.get("dashboard") or {}
    stop = recap.get("tighter_stop_audit") or {}
    demotions = recap.get("model_score_demotions") or {}
    triggers: list[str] = []
    realized = safe_float(totals.get("realized_pl"), 0.0)
    losses = safe_int(totals.get("losses"), 0)
    closed = safe_int(totals.get("closed"), 0)
    loss_rate = safe_float(totals.get("loss_rate"), 0.0)
    if realized < 0:
        triggers.append(f"today_realized_pl_negative={realized:.6f}")
    if losses > 0:
        triggers.append(f"today_losing_closes={losses}/{closed}")
    if loss_rate >= 0.5 and losses > 0:
        triggers.append(f"today_loss_rate_high={loss_rate:.0%}")
    if safe_float(history.get("realized_pl"), 0.0) < 0:
        triggers.append(f"recent_history_realized_pl_negative={safe_float(history.get('realized_pl'), 0.0):.6f}")
    if dashboard.get("bad_day"):
        triggers.append("daily_dashboard_bad_day=true")
    if demotions.get("active"):
        buckets = ",".join(sorted((demotions.get("demoted_buckets") or {}).keys()))
        triggers.append(f"report_only_score_demotions={buckets or 'active'}")
    if safe_int(stop.get("audited_losing_trades"), 0) > 0 and stop.get("most_helpful_candidate_pips"):
        triggers.append(f"stop_audit_candidate={stop.get('most_helpful_candidate_pips')}p")
    return triggers


def build_crisis_alert_prompt(
    recap: dict[str, Any],
    learning: dict[str, Any],
    loss_review: dict[str, Any] | None = None,
) -> str:
    lane = recap.get("account_lane") or learning.get("account_lane") or "unknown"
    date = recap.get("date_ny") or learning.get("date_ny") or ny_date()
    triggers = crisis_review_triggers(recap, learning)
    lines = [
        f"# Live Crisis Review - {lane} - {date}",
        "",
        "Mode: prompt_only_diagnostic",
        "Blocks trading: false",
        "",
        "This alert replaces the idea of a generic live failsafe. It is not a shutdown signal.",
        "Use it to diagnose whether the model, threshold, feature set, prompt, or trade-selection logic is wrong.",
        "",
        "Required crisis review questions:",
        "1. What thesis was invalidated by the losing trades?",
        "2. Was the entry model wrong, late, overfit, under-threshold, or missing a flip/regime feature?",
        "3. Which exact feature, threshold, prompt rule, or candidate filter should be backtested?",
        "4. What evidence would justify shadow mode before any live change?",
        "5. What should the next GPT scan avoid unless fresh evidence has changed?",
        "6. Did the model have positive expectancy, or did loss size overwhelm the win rate?",
        "7. What expected_R, invalidation distance, and flip/no-trade condition should similar future trades require?",
        "",
        "Triggers:",
    ]
    if triggers:
        lines.extend(f"- {trigger}" for trigger in triggers)
    else:
        lines.append("- none")
    if loss_review and loss_review.get("latest_prompt"):
        lines.extend(["", f"Advisor loss-review prompt: {loss_review.get('latest_prompt')}"])
    lines.extend(
        [
            "",
            "Daily recap JSON:",
            "```json",
            json.dumps(recap, indent=2, sort_keys=True, default=str)[:40000],
            "```",
            "",
            "Learning/backtest recommendation JSON:",
            "```json",
            json.dumps(learning, indent=2, sort_keys=True, default=str)[:40000],
            "```",
        ]
    )
    return "\n".join(lines) + "\n"


def write_crisis_mode_alert(
    manager: Any,
    state: dict[str, Any],
    *,
    recap: dict[str, Any],
    learning: dict[str, Any],
    loss_review: dict[str, Any] | None = None,
) -> dict[str, Any]:
    account_dir = Path(manager.cfg.data_dir)
    date = ny_date()
    triggers = crisis_review_triggers(recap, learning)
    if not triggers:
        summary = {
            "active": False,
            "date_ny": date,
            "updated_utc": iso_utc(),
            "mode": "normal",
            "blocks_trading": False,
            "action": "none",
            "triggers": [],
            "clear_reason": "no current loss-review triggers",
        }
        state[CRISIS_STATE_KEY] = summary
        return summary

    out_dir = account_dir / "crisis_alerts"
    prompt_path = out_dir / f"crisis_alert_{date}.md"
    prompt = build_crisis_alert_prompt(recap, learning, loss_review=loss_review)
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(prompt, encoding="utf-8")
    latest_path = account_dir / "latest_crisis_alert.md"
    latest_path.write_text(prompt, encoding="utf-8")
    summary = {
        "active": True,
        "date_ny": date,
        "updated_utc": iso_utc(),
        "mode": "prompt_only_crisis_review",
        "blocks_trading": False,
        "action": "alert_and_inject_diagnostic_prompt",
        "triggers": triggers,
        "latest_prompt": str(latest_path),
        "prompt_path": str(prompt_path),
        "loss_review_prompt": (loss_review or {}).get("latest_prompt", ""),
        "account_lane": getattr(manager.cfg, "account_lane", ""),
        "note": "Crisis mode is prompt/alert only. It does not block new trades.",
    }
    state[CRISIS_STATE_KEY] = summary
    try:
        logger = getattr(manager, "full_logger", None)
        if logger is not None:
            logger.log_event(
                "live_crisis_mode",
                status="active",
                severity="WARN",
                reason="prompt-only crisis review generated",
                raw=summary,
            )
    except Exception:
        pass
    return summary


def current_crisis_mode(state: dict[str, Any] | None) -> dict[str, Any]:
    state = state or {}
    crisis = state.get(CRISIS_STATE_KEY)
    if not isinstance(crisis, dict):
        return {
            "active": False,
            "mode": "normal",
            "blocks_trading": False,
            "triggers": [],
        }
    if crisis.get("date_ny") != ny_date():
        return {
            **crisis,
            "active": False,
            "mode": "normal",
            "blocks_trading": False,
            "clear_reason": "stale NY trading date",
        }
    crisis["blocks_trading"] = False
    return crisis


def write_loss_review_prompt(
    manager: Any,
    state: dict[str, Any],
    *,
    recap: dict[str, Any],
    learning: dict[str, Any],
) -> dict[str, Any]:
    totals = recap.get("totals") or {}
    history = learning.get("history_stats") or {}
    should_write = (
        safe_float(totals.get("realized_pl"), 0.0) < 0
        or safe_int(totals.get("losses"), 0) > 0
        or safe_float(history.get("realized_pl"), 0.0) < 0
    )
    if not should_write:
        return {}
    account_dir = Path(manager.cfg.data_dir)
    date = ny_date()
    out_dir = account_dir / "advisor_loss_reviews"
    prompt_path = out_dir / f"loss_review_prompt_{date}.md"
    prompt = build_loss_review_prompt(recap, learning)
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(prompt, encoding="utf-8")
    latest_path = account_dir / "latest_loss_review_prompt.md"
    latest_path.write_text(prompt, encoding="utf-8")
    summary = {
        "date_ny": date,
        "generated_utc": iso_utc(),
        "mode": "advisor_prompt_ready",
        "latest_prompt": str(latest_path),
        "prompt_path": str(prompt_path),
        "account_lane": getattr(manager.cfg, "account_lane", ""),
        "note": "Prompt is generated for diagnosis/backtest proposals; it is not a live trading block.",
    }
    state[LOSS_REVIEW_STATE_KEY] = summary
    try:
        logger = getattr(manager, "full_logger", None)
        if logger is not None:
            logger.log_event(
                "loss_review_prompt",
                status="generated",
                severity="INFO",
                reason="advisor loss review prompt generated",
                raw=summary,
            )
    except Exception:
        pass
    return summary


def maybe_write_live_learning(
    manager: Any,
    state: dict[str, Any],
    *,
    recap: dict[str, Any] | None = None,
    force: bool = False,
    min_interval_seconds: int = 3600,
) -> dict[str, Any]:
    account_dir = Path(manager.cfg.data_dir)
    date = ny_date()
    existing = state.get(LEARNING_STATE_KEY) if isinstance(state.get(LEARNING_STATE_KEY), dict) else {}
    existing_dt = parse_dt(existing.get("generated_utc"))
    if not force and existing.get("date_ny") == date and existing_dt:
        age = (dt.datetime.now(UTC) - existing_dt).total_seconds()
        if age < min_interval_seconds and existing.get("latest_json"):
            review = state.get(LOSS_REVIEW_STATE_KEY) if isinstance(state.get(LOSS_REVIEW_STATE_KEY), dict) else {}
            review_ready = (
                review.get("date_ny") == date
                and review.get("latest_prompt")
                and Path(str(review.get("latest_prompt"))).exists()
            )
            if not review_ready:
                latest_recap = recap if isinstance(recap, dict) else read_json(account_dir / "latest_daily_recap.json", {})
                latest_learning = read_json(Path(str(existing.get("latest_json"))), {})
                if isinstance(latest_recap, dict) and isinstance(latest_learning, dict):
                    loss_review = write_loss_review_prompt(manager, state, recap=latest_recap, learning=latest_learning)
                    write_crisis_mode_alert(
                        manager,
                        state,
                        recap=latest_recap,
                        learning=latest_learning,
                        loss_review=loss_review or state.get(LOSS_REVIEW_STATE_KEY),
                    )
            return existing

    learning = build_live_learning_recommendations(manager, recap=recap)
    out_dir = account_dir / "learning_recommendations"
    json_path = out_dir / f"learning_recommendations_{date}.json"
    md_path = out_dir / f"learning_recommendations_{date}.md"
    write_json(json_path, learning)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_learning_markdown(learning), encoding="utf-8")
    write_json(account_dir / "latest_learning_recommendations.json", learning)
    (account_dir / "latest_learning_recommendations.md").write_text(
        render_learning_markdown(learning),
        encoding="utf-8",
    )
    summary = {
        "date_ny": date,
        "generated_utc": learning.get("generated_utc"),
        "mode": learning.get("mode"),
        "enough_history_for_shadow": learning.get("enough_history_for_shadow"),
        "history_closed": (learning.get("history_stats") or {}).get("closed", 0),
        "history_distinct_dates": (learning.get("history_stats") or {}).get("distinct_dates", 0),
        "shadow_candidate_count": len(learning.get("shadow_candidates") or []),
        "latest_json": str(json_path),
        "latest_markdown": str(md_path),
    }
    state[LEARNING_STATE_KEY] = summary
    loss_review = write_loss_review_prompt(manager, state, recap=recap or {}, learning=learning)
    write_crisis_mode_alert(
        manager,
        state,
        recap=recap or {},
        learning=learning,
        loss_review=loss_review or state.get(LOSS_REVIEW_STATE_KEY),
    )
    try:
        logger = getattr(manager, "full_logger", None)
        if logger is not None:
            logger.log_event(
                "live_learning_recommendations",
                status=learning.get("mode") or "generated",
                severity="INFO",
                reason="live learning recommendations generated",
                raw={
                    "learning_path": str(json_path),
                    "summary": summary,
                    "recommendations": learning.get("recommendations", []),
                    "shadow_candidates": learning.get("shadow_candidates", []),
                },
            )
    except Exception:
        pass
    return summary


def render_markdown(recap: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"# Live Daily Recap - {recap.get('account_lane')} - {recap.get('date_ny')}")
    lines.append("")
    lines.append(f"Generated UTC: {recap.get('generated_utc')}")
    lines.append(f"Mode: {recap.get('dashboard', {}).get('mode', 'unknown')}")
    lines.append(f"Realized P/L: {recap.get('totals', {}).get('realized_pl')}")
    lines.append(f"Closed trades: {recap.get('totals', {}).get('closed')} wins={recap.get('totals', {}).get('wins')} losses={recap.get('totals', {}).get('losses')}")
    triggers = recap.get("dashboard", {}).get("failsafe_triggers") or []
    if triggers:
        lines.append("Failsafe: " + "; ".join(str(x) for x in triggers))
    lines.append("")
    lines.append("## Worst Buckets")
    for section in ("losses_by_pair", "losses_by_theme", "calibration_by_score_bucket", "losses_by_stop_type"):
        lines.append(f"### {section}")
        items = recap.get(section) or {}
        if not items:
            lines.append("- none")
            continue
        for key, stats in list(items.items())[:10]:
            lines.append(
                f"- {key}: closed={stats.get('closed')} losses={stats.get('losses')} "
                f"pl={stats.get('realized_pl')} loss_rate={stats.get('loss_rate')}"
            )
    lines.append("")
    demotions = recap.get("model_score_demotions", {})
    lines.append("## Score Demotions")
    if demotions.get("active"):
        for bucket, stats in (demotions.get("demoted_buckets") or {}).items():
            lines.append(f"- {bucket}: {stats.get('action')} pl={stats.get('realized_pl')} loss_rate={stats.get('loss_rate')}")
    else:
        lines.append("- none")
    lines.append("")
    stop = recap.get("tighter_stop_audit", {})
    lines.append("## Tighter Stop Audit")
    lines.append(f"- audited losing trades: {stop.get('audited_losing_trades', 0)} / {stop.get('losing_trades', 0)}")
    lines.append(f"- most helpful candidate pips: {stop.get('most_helpful_candidate_pips') or 'n/a'}")
    lines.append(f"- helped counts: {stop.get('helped_counts_by_trailing_stop_pips') or {}}")
    lines.append("")
    reentry = recap.get("reentry_behavior", {})
    lines.append("## Re-entry Behavior")
    lines.append(f"- losses with later re-entry evidence: {reentry.get('losses_with_reentry', 0)}")
    return "\n".join(lines) + "\n"


def build_recap(manager: Any, *, context: str = "", include_stop_audit: bool = True) -> dict[str, Any]:
    cfg = manager.cfg
    account_dir = Path(cfg.data_dir)
    date = ny_date()
    state = manager.load_state()
    records = closed_trade_records(account_dir, date, manager)
    opens = trade_open_index(account_dir, date)
    realized = round(sum(safe_float(rec.get("realized_pl"), 0.0) for rec in records), 6)
    wins = len([rec for rec in records if safe_float(rec.get("realized_pl"), 0.0) > 0])
    losses = len([rec for rec in records if safe_float(rec.get("realized_pl"), 0.0) < 0])
    loss_records = [rec for rec in records if safe_float(rec.get("realized_pl"), 0.0) < 0]

    failsafe = state.get("live_failsafe") if isinstance(state.get("live_failsafe"), dict) else {}
    calibration = summarize_group(records, "score_bucket")
    demotions = build_demotions(calibration, date_ny=date)
    lane = str(getattr(cfg, "account_lane", "") or "")
    score_demotion_enabled = False
    if lane == "primary_challenger":
        score_demotion_enabled = (
            str(os.environ.get("FOREX_PRIMARY_LIVE_SCORE_DEMOTION_ENABLED", "")).strip().lower()
            in {"1", "true", "yes", "y", "on"}
        )
    elif lane == "tech":
        score_demotion_enabled = (
            str(os.environ.get("FOREX_TECH_LIVE_SCORE_DEMOTION_ENABLED", "")).strip().lower()
            in {"1", "true", "yes", "y", "on"}
        )
    if score_demotion_enabled:
        demotions["enforcement_enabled"] = bool(demotions.get("active"))
    demotions_enforced = bool(demotions.get("active") and demotions.get("enforcement_enabled"))
    dashboard = {
        "date_ny": date,
        "updated_utc": iso_utc(),
        "mode": "fade_only" if failsafe.get("active") else "normal",
        "no_new_trades_after_failsafe": bool(failsafe.get("active")),
        "bad_day": realized < 0 or bool(failsafe.get("active")),
        "failsafe_active": bool(failsafe.get("active")),
        "failsafe_triggers": list(failsafe.get("triggers") or []),
        "model_score_demotions_active": bool(demotions.get("active")),
        "model_score_demotions_enforced": demotions_enforced,
        "demoted_score_buckets": sorted((demotions.get("demoted_buckets") or {}).keys()),
    }
    recap = {
        "generated_utc": iso_utc(),
        "date_ny": date,
        "account_lane": getattr(cfg, "account_lane", ""),
        "account_display_name": getattr(cfg, "account_display_name", ""),
        "context": context,
        "dashboard": dashboard,
        "account_metrics": latest_monitor_metrics(account_dir, date),
        "totals": {
            "closed": len(records),
            "wins": wins,
            "losses": losses,
            "realized_pl": realized,
            "loss_rate": round(losses / len(records), 6) if records else 0.0,
        },
        "losses_by_pair": summarize_group(loss_records, "instrument"),
        "losses_by_theme": summarize_group(loss_records, "theme"),
        "losses_by_event_type": summarize_group(loss_records, "event_type"),
        "losses_by_stop_type": summarize_group(loss_records, "stop_type"),
        "calibration_by_score_bucket": calibration,
        "model_score_demotions": demotions,
        "reentry_behavior": reentry_summary(records, opens),
        "recent_closed_trades": records[-25:],
        "tighter_stop_audit": stop_audit(records, manager) if include_stop_audit and (realized < 0 or failsafe.get("active")) else {},
    }
    return recap


def maybe_write_live_recap(
    manager: Any,
    *,
    context: str = "",
    force: bool = False,
    min_interval_seconds: int = 900,
) -> dict[str, Any]:
    cfg = manager.cfg
    account_dir = Path(cfg.data_dir)
    state = manager.load_state()
    date = ny_date()
    last = state.get(RECAP_STATE_KEY) if isinstance(state.get(RECAP_STATE_KEY), dict) else {}
    last_dt = parse_dt(last.get("generated_utc"))
    if not force and last.get("date_ny") == date and last_dt:
        age = (dt.datetime.now(UTC) - last_dt).total_seconds()
        failsafe = state.get("live_failsafe") if isinstance(state.get("live_failsafe"), dict) else {}
        current_failsafe_active = bool(failsafe.get("active"))
        dashboard = state.get(DASHBOARD_STATE_KEY) if isinstance(state.get(DASHBOARD_STATE_KEY), dict) else {}
        dashboard_stale = (
            dashboard.get("date_ny") == date
            and (
                bool(dashboard.get("failsafe_active")) != current_failsafe_active
                or bool(dashboard.get("no_new_trades_after_failsafe")) != current_failsafe_active
                or (dashboard.get("mode") == "fade_only") != current_failsafe_active
                or "model_score_demotions_enforced" not in dashboard
            )
        )
        demotions = state.get(DEMOTION_STATE_KEY) if isinstance(state.get(DEMOTION_STATE_KEY), dict) else {}
        stale_actions = [
            stats
            for stats in (demotions.get("demoted_buckets") or {}).values()
            if isinstance(stats, dict) and stats.get("action") == "block_new_entries_today"
        ]
        demotions_stale = (
            demotions.get("date_ny") == date
            and (
                demotions.get("enforcement_default") is not False
                or bool(stale_actions)
            )
        )
        if age < min_interval_seconds and not dashboard_stale and not demotions_stale:
            learning = state.get(LEARNING_STATE_KEY) if isinstance(state.get(LEARNING_STATE_KEY), dict) else {}
            learning_dt = parse_dt(learning.get("generated_utc"))
            learning_age = (dt.datetime.now(UTC) - learning_dt).total_seconds() if learning_dt else 1e12
            review = state.get(LOSS_REVIEW_STATE_KEY) if isinstance(state.get(LOSS_REVIEW_STATE_KEY), dict) else {}
            review_ready = (
                review.get("date_ny") == date
                and review.get("latest_prompt")
                and Path(str(review.get("latest_prompt"))).exists()
            )
            latest_recap = read_json(account_dir / "latest_daily_recap.json", {})
            if learning.get("date_ny") == date and learning_age < 3600 and learning.get("latest_json") and review_ready:
                latest_learning = read_json(Path(str(learning.get("latest_json"))), {})
                if isinstance(latest_recap, dict) and isinstance(latest_learning, dict):
                    write_crisis_mode_alert(
                        manager,
                        state,
                        recap=latest_recap,
                        learning=latest_learning,
                        loss_review=review,
                    )
                    manager.save_state(state)
                return last
            if learning.get("date_ny") == date and learning_age < 3600 and learning.get("latest_json"):
                latest_learning = read_json(Path(str(learning.get("latest_json"))), {})
                if isinstance(latest_recap, dict) and isinstance(latest_learning, dict):
                    loss_review = write_loss_review_prompt(manager, state, recap=latest_recap, learning=latest_learning)
                    write_crisis_mode_alert(
                        manager,
                        state,
                        recap=latest_recap,
                        learning=latest_learning,
                        loss_review=loss_review or state.get(LOSS_REVIEW_STATE_KEY),
                    )
            else:
                maybe_write_live_learning(
                    manager,
                    state,
                    recap=latest_recap if isinstance(latest_recap, dict) else None,
                )
            manager.save_state(state)
            return last

    recap = build_recap(manager, context=context, include_stop_audit=True)
    if not force and not recap.get("dashboard", {}).get("bad_day"):
        state[DASHBOARD_STATE_KEY] = recap["dashboard"]
        state[RECAP_STATE_KEY] = {
            "date_ny": date,
            "generated_utc": recap["generated_utc"],
            "bad_day": False,
            "latest_json": "",
            "latest_markdown": "",
        }
        state[DEMOTION_STATE_KEY] = recap["model_score_demotions"]
        maybe_write_live_learning(manager, state, recap=recap)
        manager.save_state(state)
        return state[RECAP_STATE_KEY]

    recap_dir = account_dir / "daily_recaps"
    json_path = recap_dir / f"daily_recap_{date}.json"
    md_path = recap_dir / f"daily_recap_{date}.md"
    write_json(json_path, recap)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_markdown(recap), encoding="utf-8")
    write_json(account_dir / "latest_daily_recap.json", recap)
    (account_dir / "latest_daily_recap.md").write_text(render_markdown(recap), encoding="utf-8")

    state[DASHBOARD_STATE_KEY] = {
        **recap["dashboard"],
        "latest_recap_json": str(json_path),
        "latest_recap_markdown": str(md_path),
    }
    state[DEMOTION_STATE_KEY] = recap["model_score_demotions"]
    state[RECAP_STATE_KEY] = {
        "date_ny": date,
        "generated_utc": recap["generated_utc"],
        "bad_day": bool(recap.get("dashboard", {}).get("bad_day")),
        "latest_json": str(json_path),
        "latest_markdown": str(md_path),
        "realized_pl": recap.get("totals", {}).get("realized_pl"),
        "closed": recap.get("totals", {}).get("closed"),
        "losses": recap.get("totals", {}).get("losses"),
    }
    maybe_write_live_learning(manager, state, recap=recap, force=force)
    manager.save_state(state)

    try:
        logger = getattr(manager, "full_logger", None)
        if logger is not None:
            logger.log_event(
                "daily_loss_recap",
                status="generated",
                severity="INFO",
                reason=f"live daily recap generated context={context}",
                raw={
                    "recap_path": str(json_path),
                    "dashboard": recap.get("dashboard", {}),
                    "totals": recap.get("totals", {}),
                    "model_score_demotions": recap.get("model_score_demotions", {}),
                },
            )
    except Exception:
        pass

    return state[RECAP_STATE_KEY]
