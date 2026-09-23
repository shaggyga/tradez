#!/usr/bin/env python3
"""ARIMA canary manifest validator / executor scaffold.

This file gives the stack an explicit ARIMA canary execution contract.  It can
validate the pending ARIMA canary manifest, convert actionable ARIMA signals
into technical-manager-compatible order candidates, and submit them only when
an explicit active ARIMA canary manifest plus a CLI execution flag are present.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np


EXECUTOR_SUPPORTS_ARIMA_PENDING_MANIFEST = True
EXECUTOR_SUPPORTS_ARIMA_ORDER_ADAPTER = True
EXECUTOR_SUPPORTS_ARIMA_BROKER_EXECUTION = True

PROJECT_ROOT = Path(__file__).resolve().parent
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
PROMOTIONS_ROOT = TRAINING_ROOT / "promotions"
REPORT_ROOT = TRAINING_ROOT / "arima_canary_executor"
PENDING_MANIFEST_PATH = PROMOTIONS_ROOT / "arima_canary_candidate_pending.json"
ACTIVE_MANIFEST_PATH = PROMOTIONS_ROOT / "arima_canary_candidate.json"
EXECUTOR_REPORT_PATH = REPORT_ROOT / "latest_arima_canary_executor_report.json"
MAX_ARIMA_CANARY_ORDERS_PER_RUN = 1


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_safe(value: Any) -> Any:
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, Path):
        return str(value)
    return value


def read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=json_safe), encoding="utf-8")
    os.replace(tmp, path)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        number = float(value)
        if not np.isfinite(number):
            return default
        return number
    except Exception:
        return default


def normalize_direction(action: Any) -> str:
    text = str(action or "").strip().lower()
    if text == "long":
        return "LONG"
    if text == "short":
        return "SHORT"
    return ""


def pip_size(pair: str) -> float:
    return 0.01 if str(pair).upper().endswith("_JPY") else 0.0001


def pair_stop_profile(pair: str) -> Dict[str, float]:
    quote = str(pair).upper().split("_")[-1] if "_" in str(pair) else ""
    if quote in {"HUF", "ZAR", "TRY", "MXN", "CNH"}:
        return {
            "min_stop_pips": 20.0,
            "max_stop_pips": 220.0,
            "take_profit_r": 1.75,
            "trailing_r": 0.85,
        }
    if quote in {"NOK", "SEK", "DKK", "HKD", "SGD", "PLN", "CZK"}:
        return {
            "min_stop_pips": 10.0,
            "max_stop_pips": 120.0,
            "take_profit_r": 1.65,
            "trailing_r": 0.80,
        }
    if quote == "JPY":
        return {
            "min_stop_pips": 8.0,
            "max_stop_pips": 80.0,
            "take_profit_r": 1.55,
            "trailing_r": 0.75,
        }
    return {
        "min_stop_pips": 5.0,
        "max_stop_pips": 60.0,
        "take_profit_r": 1.50,
        "trailing_r": 0.75,
    }


def clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def build_stop_take_profit(signal: Dict[str, Any], direction: str) -> Dict[str, Any]:
    pair = str(signal.get("pair") or "").strip()
    entry = safe_float(signal.get("latest_close"), 0.0)
    if entry <= 0:
        raise ValueError(f"latest_close unavailable for {pair}")
    predicted_pips = abs(safe_float(signal.get("predicted_pips"), 0.0))
    threshold_pips = abs(safe_float(signal.get("threshold_pips"), 0.0))
    shadow_mean = abs(safe_float(signal.get("shadow_test_mean_net_pips"), 0.0))
    profile = pair_stop_profile(pair)
    raw_stop = max(
        profile["min_stop_pips"],
        threshold_pips * 2.5,
        predicted_pips * 0.35,
        shadow_mean * 0.45,
    )
    stop_pips = clamp(raw_stop, profile["min_stop_pips"], profile["max_stop_pips"])
    target_pips = max(
        stop_pips * profile["take_profit_r"],
        min(predicted_pips, profile["max_stop_pips"] * 1.50),
    )
    psize = pip_size(pair)
    if direction == "LONG":
        stop_loss = entry - stop_pips * psize
        take_profit = entry + target_pips * psize
    elif direction == "SHORT":
        stop_loss = entry + stop_pips * psize
        take_profit = entry - target_pips * psize
    else:
        raise ValueError(f"invalid direction {direction!r}")
    trailing_stop_pips = clamp(
        stop_pips * profile["trailing_r"],
        profile["min_stop_pips"],
        profile["max_stop_pips"],
    )
    return {
        "entry_reference_price": entry,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "stop_pips": stop_pips,
        "take_profit_pips": target_pips,
        "trailing_stop_pips": trailing_stop_pips,
        "pip_size": psize,
        "stop_profile": profile,
    }


def load_source_manifest(prefer_active: bool = False) -> tuple[Path, Dict[str, Any]]:
    if prefer_active and ACTIVE_MANIFEST_PATH.exists():
        active = read_json(ACTIVE_MANIFEST_PATH, {})
        if isinstance(active, dict) and active:
            return ACTIVE_MANIFEST_PATH, active
    pending = read_json(PENDING_MANIFEST_PATH, {})
    return PENDING_MANIFEST_PATH, pending if isinstance(pending, dict) else {}


def order_candidates(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    risk_policy = manifest.get("risk_policy") if isinstance(manifest.get("risk_policy"), dict) else {}
    risk_pct = min(
        0.05,
        safe_float(risk_policy.get("max_total_new_risk_pct_per_scan"), 0.10),
    )
    out: List[Dict[str, Any]] = []
    for signal in manifest.get("signals", []) or []:
        if not isinstance(signal, dict):
            continue
        direction = normalize_direction(signal.get("action"))
        if not direction:
            continue
        pair = str(signal.get("pair") or "").strip()
        if not pair:
            continue
        stops = build_stop_take_profit(signal, direction)
        out.append({
            "instrument": pair,
            "direction": direction,
            "risk_pct": risk_pct,
            "stop_loss": stops["stop_loss"],
            "take_profit": stops["take_profit"],
            "trailing_stop_pips": stops["trailing_stop_pips"],
            "source": "arima_canary_pending",
            "horizon_minutes": signal.get("horizon_minutes", ""),
            "predicted_pips": signal.get("predicted_pips", ""),
            "threshold_pips": signal.get("threshold_pips", ""),
            "arima_model": signal.get("arima_model", ""),
            "latest_bar_utc": signal.get("latest_bar_utc", ""),
            "entry_reference_price": stops["entry_reference_price"],
            "stop_pips": stops["stop_pips"],
            "take_profit_pips": stops["take_profit_pips"],
            "pip_size": stops["pip_size"],
            "stop_profile": stops["stop_profile"],
            "reason": (
                f"ARIMA canary {pair} {direction} "
                f"pred={safe_float(signal.get('predicted_pips'), 0.0):.2f}p "
                f"thr={safe_float(signal.get('threshold_pips'), 0.0):.2f}p "
                f"stop={stops['stop_pips']:.1f}p tp={stops['take_profit_pips']:.1f}p"
            ),
        })
    return out


def build_arima_canary_bot(*, allow_broker_execution: bool):
    import oanda_technical_account_manager_auto as tech

    class ArimaCanaryBrokerManager(tech.ForexManager):
        def should_execute(self) -> bool:
            return bool(self.cfg.execute_trades and allow_broker_execution)

        def production_model_allows_new_entries(self):
            return True, "ARIMA canary active manifest gate handled by executor"

    base = tech.BotConfig.load()
    account_id = tech.cfg_str(
        "OANDA_ACCOUNT_ID_ARIMA_CANARY",
        "OANDA_ACCOUNT_ID_DUM1",
        "OANDA_ACCOUNT_ID_DUM",
        default="101-001-37981792-004",
    )
    cfg = replace(
        base,
        oanda_account_id=account_id,
        account_lane="canary",
        account_display_name="OANDA_ACCOUNT_ID_ARIMA_CANARY/OANDA_ACCOUNT_ID_DUM1",
        execute_trades=allow_broker_execution,
        auto_execute_research_actions=True,
        scan_on_launch=False,
        calls_per_trading_day=0,
        call_times_ny=[],
        friday_call_times_ny=[],
        event_trigger_research_enabled=False,
        max_open_trades=1,
        max_new_trades_per_scan=1,
        max_total_new_risk_pct_per_scan=0.10,
        target_margin_used_pct=5.0,
        max_margin_used_pct=12.0,
        min_risk_pct_per_trade=0.01,
    )
    cfg = tech.with_data_dir(
        cfg,
        base.data_dir / "account_arima_canary_candidate",
    )
    bot = ArimaCanaryBrokerManager(cfg)
    bot.validate_config()
    tech.ensure_bot_ready(bot, reconcile=False)
    return bot, tech


def submit_order_candidates(
    candidates: List[Dict[str, Any]],
    *,
    allow_broker_execution: bool,
    max_orders: int,
) -> List[Dict[str, Any]]:
    if not candidates:
        return []
    bot, tech = build_arima_canary_bot(
        allow_broker_execution=allow_broker_execution,
    )
    instruments = sorted({
        *(str(row.get("instrument") or "") for row in candidates),
        *bot.instruments,
    })
    instruments = [inst for inst in instruments if inst]
    prices = bot.oanda.get_prices(instruments)
    results: List[Dict[str, Any]] = []
    for order in candidates[: max(1, int(max_orders))]:
        order_payload = {
            **order,
            "action": "OPEN",
            "_arima_canary": True,
            "_event_scout": False,
        }
        status = bot.open_trade_from_order(order_payload, prices)
        results.append({
            "instrument": tech.normalize_instrument(order_payload.get("instrument", "")),
            "direction": str(order_payload.get("direction") or ""),
            "status": status,
            "risk_pct": order_payload.get("risk_pct", ""),
            "stop_loss": order_payload.get("stop_loss", ""),
            "take_profit": order_payload.get("take_profit", ""),
            "trailing_stop_pips": order_payload.get("trailing_stop_pips", ""),
            "reason": order_payload.get("reason", ""),
        })
    return results


def evaluate_once(
    *,
    prefer_active: bool,
    allow_broker_execution: bool,
    max_orders: int,
) -> Dict[str, Any]:
    source_path, manifest = load_source_manifest(prefer_active=prefer_active)
    if not manifest:
        return {
            "generated_utc": utc_iso(),
            "available": False,
            "source_manifest_path": str(source_path),
            "reason": "no ARIMA canary manifest available",
            "execution_enabled": False,
        }
    candidates = order_candidates(manifest)
    manifest_execution_enabled = bool(manifest.get("execution_enabled", False))
    manifest_ready = bool(manifest.get("canary_assignment_ready", False))
    active_stage = str(manifest.get("stage", "")).lower() == "arima_canary"
    broker_execution_available = bool(
        EXECUTOR_SUPPORTS_ARIMA_BROKER_EXECUTION
        and allow_broker_execution
        and manifest_execution_enabled
        and manifest_ready
        and active_stage
        and source_path == ACTIVE_MANIFEST_PATH
    )
    blockers = []
    if not manifest_ready:
        blockers.append("manifest_not_canary_ready")
    if not active_stage or source_path != ACTIVE_MANIFEST_PATH:
        blockers.append("requires_explicit_active_arima_canary_manifest")
    if not manifest_execution_enabled:
        blockers.append("manifest_execution_enabled_false")
    if not EXECUTOR_SUPPORTS_ARIMA_ORDER_ADAPTER:
        blockers.append("requires_pair_specific_stop_risk_order_adapter")
    if not EXECUTOR_SUPPORTS_ARIMA_BROKER_EXECUTION:
        blockers.append("requires_broker_execution_enablement")
    if not allow_broker_execution:
        blockers.append("allow_broker_execution_false")
    if not candidates:
        blockers.append("no_order_candidates")
    execution_results: List[Dict[str, Any]] = []
    execution_error = ""
    if broker_execution_available and candidates:
        try:
            execution_results = submit_order_candidates(
                candidates,
                allow_broker_execution=allow_broker_execution,
                max_orders=max_orders,
            )
        except Exception as exc:
            execution_error = str(exc)
            blockers.append("broker_execution_error")
    return {
        "generated_utc": utc_iso(),
        "available": True,
        "source_manifest_path": str(source_path),
        "stage": manifest.get("stage", ""),
        "pending_manifest_supported": EXECUTOR_SUPPORTS_ARIMA_PENDING_MANIFEST,
        "order_adapter_supported": EXECUTOR_SUPPORTS_ARIMA_ORDER_ADAPTER,
        "broker_execution_supported": EXECUTOR_SUPPORTS_ARIMA_BROKER_EXECUTION,
        "broker_execution_available": broker_execution_available,
        "execution_enabled": broker_execution_available,
        "manifest_execution_enabled": manifest_execution_enabled,
        "manifest_canary_ready": manifest_ready,
        "max_orders": max_orders,
        "order_candidate_count": len(candidates),
        "order_candidates": candidates,
        "execution_results": execution_results,
        "execution_error": execution_error,
        "blockers": blockers,
        "reason": (
            "ARIMA canary manifest validated. Broker execution is intentionally "
            "gated behind an active ARIMA canary manifest and explicit CLI flag."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefer-active", action="store_true", help="Prefer arima_canary_candidate.json over pending.")
    parser.add_argument("--allow-broker-execution", action="store_true", help="Allow broker submission only if the active ARIMA canary manifest is ready and execution_enabled=true.")
    parser.add_argument("--max-orders", type=int, default=MAX_ARIMA_CANARY_ORDERS_PER_RUN)
    parser.add_argument("--report", default=str(EXECUTOR_REPORT_PATH))
    args = parser.parse_args()
    report = evaluate_once(
        prefer_active=args.prefer_active,
        allow_broker_execution=args.allow_broker_execution,
        max_orders=args.max_orders,
    )
    atomic_write_json(Path(args.report), report)
    print(json.dumps({
        "time_utc": utc_iso(),
        "available": report.get("available", False),
        "order_candidate_count": report.get("order_candidate_count", 0),
        "broker_execution_available": report.get("broker_execution_available", False),
        "execution_results": report.get("execution_results", []),
        "blockers": report.get("blockers", []),
        "report": str(Path(args.report)),
    }, default=json_safe), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
