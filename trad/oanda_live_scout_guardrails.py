#!/usr/bin/env python3
"""Live-only guardrails for OANDA technical scout account wrappers."""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any

import oanda_live_recap as live_recap
import oanda_technical_account_manager_auto as tech


LIVE_FAILSAFE_STATE_KEY = "live_failsafe"

LIVE_ACCOUNT_STATE_ROOTS = {
    "gpt_live": tech.SCRIPT_DIR / "data" / "forex_gpt_manager" / "account_gpt_prod_live",
    "tech": tech.SCRIPT_DIR / "data" / "technical_scout_manager" / "account_live_tech_broad_regime_scout",
    "primary_challenger": tech.SCRIPT_DIR / "data" / "technical_scout_manager" / "account_live_primary_challenger_scout",
}


def _raw_setting(name: str) -> Any:
    value = os.environ.get(name)
    if value is not None and str(value).strip() != "":
        return value
    value = tech.CREDS.get(name)
    if value is not None and str(value).strip() != "":
        return value
    return None


def _setting_bool(names: tuple[str, ...], default: bool) -> bool:
    for name in names:
        value = _raw_setting(name)
        if value is None or str(value).strip() == "":
            continue
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _setting_float(names: tuple[str, ...], default: float) -> float:
    for name in names:
        value = _raw_setting(name)
        if value is None or str(value).strip() == "":
            continue
        try:
            return float(str(value).strip())
        except Exception:
            continue
    return default


def _setting_int(names: tuple[str, ...], default: int) -> int:
    for name in names:
        value = _raw_setting(name)
        if value is None or str(value).strip() == "":
            continue
        try:
            return int(float(str(value).strip()))
        except Exception:
            continue
    return default


def _setting_csv(names: tuple[str, ...], default: tuple[str, ...]) -> list[str]:
    for name in names:
        value = _raw_setting(name)
        if value is None or str(value).strip() == "":
            continue
        out = []
        for item in str(value).replace(";", ",").split(","):
            cleaned = item.strip().upper()
            if cleaned:
                out.append(cleaned)
        if out:
            return out
    return list(default)


def _csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def _row_ny_date(row: dict[str, Any]) -> str:
    time_ny = str(row.get("time_ny") or "").strip()
    if len(time_ny) >= 10 and time_ny[:4].isdigit():
        return time_ny[:10]
    time_utc = str(row.get("time_utc") or "").strip()
    if len(time_utc) >= 10 and time_utc[:4].isdigit():
        return time_utc[:10]
    return ""


def _json_cell(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        obj = json.loads(str(value))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _direction_from_units(units: Any) -> str:
    try:
        value = float(units)
    except Exception:
        return ""
    if value > 0:
        return "LONG"
    if value < 0:
        return "SHORT"
    return ""


def currency_direction_keys(instrument: str, direction: str) -> list[str]:
    inst = tech.normalize_instrument(instrument)
    if not inst or "_" not in inst:
        return []
    base, quote = tech.split_instrument(inst)
    direction = str(direction or "").upper().strip()
    if direction == "LONG":
        return [f"{base}_LONG", f"{quote}_SHORT"]
    if direction == "SHORT":
        return [f"{base}_SHORT", f"{quote}_LONG"]
    return []


def _opposite_direction(direction: str) -> str:
    direction = str(direction or "").upper().strip()
    if direction == "LONG":
        return "SHORT"
    if direction == "SHORT":
        return "LONG"
    return ""


def _read_state(root: Path) -> dict[str, Any]:
    path = root / "state.json"
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


class LiveScoutGuardMixin:
    """Mixin for live technical/scout wrappers.

    It does not change signal generation. It only blocks or fades live account
    exposure after account-level loss/cooldown rules trigger.
    """

    live_guardrail_profile = "scout"

    def live_guardrail_prefixes(self) -> tuple[str, ...]:
        lane = str(getattr(self.cfg, "account_lane", "") or "").upper()
        if lane == "PRIMARY_CHALLENGER":
            return ("FOREX_PRIMARY_LIVE", "FOREX_LIVE_PRIMARY", "FOREX_SCOUT_LIVE")
        if lane == "TECH":
            return ("FOREX_TECH_LIVE", "FOREX_LIVE_TECH", "FOREX_SCOUT_LIVE")
        return ("FOREX_SCOUT_LIVE",)

    def live_default_thresholds(self) -> dict[str, Any]:
        lane = str(getattr(self.cfg, "account_lane", "") or "").lower()
        if lane == "primary_challenger":
            return {
                "daily_loss_pct": 1.25,
                "daily_loss_usd": 0.0,
                "nav_drawdown_pct": 2.0,
                "losing_closes": 4,
                "pair_loss_limit": 1,
                "currency_loss_limit": 3,
                "cross_account_currency_cap": 4,
            }
        return {
            "daily_loss_pct": 2.0,
            "daily_loss_usd": 0.0,
            "nav_drawdown_pct": 3.0,
            "losing_closes": 3,
            "pair_loss_limit": 1,
            "currency_loss_limit": 3,
            "cross_account_currency_cap": 6,
        }

    def live_setting_bool(self, suffix: str, default: bool) -> bool:
        names = tuple(f"{prefix}_{suffix}" for prefix in self.live_guardrail_prefixes())
        names += (f"FOREX_LIVE_SCOUT_{suffix}",)
        return _setting_bool(names, default)

    def live_setting_float(self, suffix: str, default: float) -> float:
        names = tuple(f"{prefix}_{suffix}" for prefix in self.live_guardrail_prefixes())
        names += (f"FOREX_LIVE_SCOUT_{suffix}",)
        return _setting_float(names, default)

    def live_setting_int(self, suffix: str, default: int) -> int:
        names = tuple(f"{prefix}_{suffix}" for prefix in self.live_guardrail_prefixes())
        names += (f"FOREX_LIVE_SCOUT_{suffix}",)
        return _setting_int(names, default)

    def live_setting_csv(self, suffix: str, default: tuple[str, ...]) -> list[str]:
        names = tuple(f"{prefix}_{suffix}" for prefix in self.live_guardrail_prefixes())
        names += (f"FOREX_LIVE_SCOUT_{suffix}",)
        return _setting_csv(names, default)

    def live_opened_trade_index(self) -> dict[str, dict[str, Any]]:
        opens: dict[str, dict[str, Any]] = {}
        for row in _csv_rows(self.cfg.data_dir / "order_result_ledger.csv"):
            trade_id = str(row.get("trade_id") or "").strip()
            if not trade_id:
                continue
            status = str(row.get("status") or "").lower().strip()
            if status and not status.startswith("accepted"):
                continue
            inst = tech.normalize_instrument(row.get("instrument", ""))
            direction = str(row.get("direction") or "").upper().strip()
            if inst and direction in {"LONG", "SHORT"}:
                opens[trade_id] = {
                    "trade_id": trade_id,
                    "instrument": inst,
                    "direction": direction,
                    "time_utc": row.get("time_utc", ""),
                    "time_ny": row.get("time_ny", ""),
                    "reason": row.get("reason", ""),
                }

        for row in _csv_rows(self.cfg.data_dir / "trade_lifecycle_ledger.csv"):
            if str(row.get("reason") or "").upper().strip() != "MARKET_ORDER":
                continue
            trade_id = str(row.get("trade_id") or "").strip()
            if not trade_id:
                continue
            inst = tech.normalize_instrument(row.get("instrument", ""))
            direction = str(row.get("direction") or "").upper().strip()
            if inst and direction in {"LONG", "SHORT"}:
                opens.setdefault(
                    trade_id,
                    {
                        "trade_id": trade_id,
                        "instrument": inst,
                        "direction": direction,
                        "time_utc": row.get("time_utc", ""),
                        "time_ny": row.get("time_ny", ""),
                        "reason": row.get("reason", ""),
                    },
                )
        return opens

    def live_losing_close_records(self, today_ny: str | None = None) -> list[dict[str, Any]]:
        today_ny = today_ny or tech.ny_now().date().isoformat()
        opens = self.live_opened_trade_index()
        records: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for row in _csv_rows(self.cfg.data_dir / "trade_lifecycle_ledger.csv"):
            if _row_ny_date(row) != today_ny:
                continue
            reason = str(row.get("reason") or "").upper().strip()
            if reason == "MARKET_ORDER":
                continue
            raw = _json_cell(row.get("raw_json"))
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
                close_key = (tx_id or str(row.get("time_utc") or ""), trade_id or str(idx))
                if close_key in seen:
                    continue
                seen.add(close_key)
                pl = tech.safe_float(closed.get("realizedPL", row.get("pl")), 0.0)
                if pl >= 0:
                    continue
                open_info = opens.get(trade_id, {})
                inst = tech.normalize_instrument(open_info.get("instrument") or row.get("instrument", ""))
                direction = str(open_info.get("direction") or "").upper().strip()
                if direction not in {"LONG", "SHORT"}:
                    direction = _opposite_direction(str(row.get("direction") or ""))
                if not inst or direction not in {"LONG", "SHORT"}:
                    continue
                records.append(
                    {
                        "trade_id": trade_id,
                        "instrument": inst,
                        "direction": direction,
                        "pl": round(pl, 6),
                        "close_reason": reason,
                        "close_time_utc": row.get("time_utc", ""),
                        "close_time_ny": row.get("time_ny", ""),
                        "open_time_ny": open_info.get("time_ny", ""),
                        "currency_keys": currency_direction_keys(inst, direction),
                    }
                )
        records.sort(key=lambda rec: str(rec.get("close_time_utc") or rec.get("close_time_ny") or ""))
        return records

    def live_failsafe_metrics(self, account: dict[str, Any] | None = None) -> dict[str, Any]:
        today_ny = tech.ny_now().date().isoformat()
        close_rows: list[dict[str, Any]] = []
        for row in _csv_rows(self.cfg.data_dir / "trade_lifecycle_ledger.csv"):
            if _row_ny_date(row) != today_ny:
                continue
            reason = str(row.get("reason") or "").upper().strip()
            pl = tech.safe_float(row.get("pl"), 0.0)
            if reason == "MARKET_ORDER" or abs(pl) <= 1e-9:
                continue
            close_rows.append(row)
        close_rows.sort(key=lambda row: str(row.get("time_utc") or row.get("time_ny") or ""))

        daily_realized_pl = sum(tech.safe_float(row.get("pl"), 0.0) for row in close_rows)
        consecutive_losing_closes = 0
        for row in reversed(close_rows):
            if tech.safe_float(row.get("pl"), 0.0) < 0:
                consecutive_losing_closes += 1
            else:
                break

        monitor_rows = [row for row in _csv_rows(self.cfg.monitor_csv) if _row_ny_date(row) == today_ny]
        navs = [
            tech.safe_float(row.get("nav"), 0.0)
            for row in monitor_rows
            if tech.safe_float(row.get("nav"), 0.0) > 0
        ]
        account_summary = tech.account_summary_for_prompt(account or {}) if account else {}
        current_nav = tech.safe_float(account_summary.get("nav"), 0.0)
        if current_nav > 0:
            navs.append(current_nav)
        first_nav = navs[0] if navs else current_nav
        peak_nav = max(navs) if navs else current_nav
        if current_nav <= 0 and navs:
            current_nav = navs[-1]
        realized_loss_pct = max(0.0, -daily_realized_pl) / first_nav * 100.0 if first_nav > 0 else 0.0
        nav_drawdown_pct = max(0.0, peak_nav - current_nav) / peak_nav * 100.0 if peak_nav > 0 and current_nav > 0 else 0.0
        return {
            "date_ny": today_ny,
            "daily_realized_pl": round(daily_realized_pl, 6),
            "daily_realized_loss_pct": round(realized_loss_pct, 6),
            "consecutive_losing_closes": consecutive_losing_closes,
            "closed_trade_count_today": len(close_rows),
            "first_nav_today": round(first_nav, 6),
            "peak_nav_today": round(peak_nav, 6),
            "current_nav": round(current_nav, 6),
            "nav_drawdown_pct": round(nav_drawdown_pct, 6),
        }

    def evaluate_live_scout_failsafe(
        self,
        account: dict[str, Any] | None = None,
        *,
        persist: bool = True,
    ) -> dict[str, Any]:
        today_ny = tech.ny_now().date().isoformat()
        state = self.load_state()
        existing = state.get(LIVE_FAILSAFE_STATE_KEY)
        if not isinstance(existing, dict):
            existing = {}

        if existing.get("active") and existing.get("date_ny") != today_ny:
            existing = {
                **existing,
                "active": False,
                "cleared_utc": tech.iso_utc(),
                "clear_reason": "new NY trading date",
            }
            state[LIVE_FAILSAFE_STATE_KEY] = existing
            if persist:
                self.save_state(state)

        metrics = self.live_failsafe_metrics(account)
        if not self.live_setting_bool("FAILSAFE_ENABLED", False):
            status = {
                "active": False,
                "enabled": False,
                "date_ny": today_ny,
                "triggered_utc": "",
                "triggers": [],
                "metrics": metrics,
                "action": "report_only",
                "clear_reason": "live scout failsafe enforcement disabled by default",
            }
            if persist:
                state[LIVE_FAILSAFE_STATE_KEY] = status
                self.save_state(state)
            return status

        if existing.get("active") and existing.get("date_ny") == today_ny:
            status = {
                **existing,
                "enabled": True,
                "metrics": metrics,
                "triggers": list(existing.get("triggers") or []),
            }
            if persist:
                state[LIVE_FAILSAFE_STATE_KEY] = status
                self.save_state(state)
            return status

        defaults = self.live_default_thresholds()
        max_realized_loss_pct = self.live_setting_float("FAILSAFE_MAX_DAILY_REALIZED_LOSS_PCT", defaults["daily_loss_pct"])
        max_realized_loss_usd = self.live_setting_float("FAILSAFE_MAX_DAILY_REALIZED_LOSS_USD", defaults["daily_loss_usd"])
        max_nav_drawdown_pct = self.live_setting_float("FAILSAFE_MAX_INTRADAY_NAV_DRAWDOWN_PCT", defaults["nav_drawdown_pct"])
        max_losing_closes = self.live_setting_int("FAILSAFE_MAX_CONSECUTIVE_LOSING_CLOSES", defaults["losing_closes"])

        triggers: list[str] = []
        if max_realized_loss_pct > 0 and metrics["daily_realized_loss_pct"] >= max_realized_loss_pct:
            triggers.append(f"daily realized loss {metrics['daily_realized_loss_pct']:.2f}% >= {max_realized_loss_pct:.2f}%")
        if max_realized_loss_usd > 0 and metrics["daily_realized_pl"] <= -abs(max_realized_loss_usd):
            triggers.append(f"daily realized P/L {metrics['daily_realized_pl']:.2f} <= -{abs(max_realized_loss_usd):.2f}")
        if max_nav_drawdown_pct > 0 and metrics["nav_drawdown_pct"] >= max_nav_drawdown_pct:
            triggers.append(f"intraday NAV drawdown {metrics['nav_drawdown_pct']:.2f}% >= {max_nav_drawdown_pct:.2f}%")
        if max_losing_closes > 0 and metrics["consecutive_losing_closes"] >= max_losing_closes:
            triggers.append(f"consecutive losing closes {metrics['consecutive_losing_closes']} >= {max_losing_closes}")

        status = {
            "active": bool(triggers),
            "enabled": True,
            "date_ny": today_ny,
            "triggered_utc": tech.iso_utc() if triggers else "",
            "triggers": triggers,
            "metrics": metrics,
            "action": _raw_setting("FOREX_LIVE_SCOUT_FAILSAFE_ACTION") or "fade",
        }
        if persist:
            state[LIVE_FAILSAFE_STATE_KEY] = status
            self.save_state(state)
        return status

    def activate_live_scout_failsafe_mode(self, status: dict[str, Any]) -> None:
        if not status.get("active"):
            return
        action = str(status.get("action") or "fade").strip().lower()
        self.cfg.shutdown_mode = "close_now" if action == "close_now" else "fade"
        self.cfg.shutdown_block_new_trades = True
        self.cfg.shutdown_skip_research = True

    def log_live_scout_failsafe(self, status: dict[str, Any], context: str) -> None:
        triggers = "; ".join(str(x) for x in status.get("triggers") or [])
        metrics = status.get("metrics") or {}
        tech.log(
            f"{self.lane_prefix()}Live scout failsafe active during {context}: "
            f"{triggers or 'previous trigger still active'}; "
            f"realized_pl={metrics.get('daily_realized_pl')} "
            f"loss_pct={metrics.get('daily_realized_loss_pct')} "
            f"nav_dd={metrics.get('nav_drawdown_pct')}"
        )

    def live_cooldown_status(self) -> dict[str, Any]:
        today_ny = tech.ny_now().date().isoformat()
        defaults = self.live_default_thresholds()
        pair_limit = self.live_setting_int("COOLDOWN_PAIR_MAX_LOSSES", defaults["pair_loss_limit"])
        currency_limit = self.live_setting_int("COOLDOWN_CURRENCY_MAX_LOSSES", defaults["currency_loss_limit"])
        focus = set(
            self.live_setting_csv(
                "COOLDOWN_FOCUS_CURRENCIES",
                ("USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"),
            )
        )
        records = self.live_losing_close_records(today_ny)
        by_pair: dict[str, list[dict[str, Any]]] = {}
        by_currency: dict[str, list[dict[str, Any]]] = {}
        for rec in records:
            pair_key = f"{rec['instrument']}:{rec['direction']}"
            by_pair.setdefault(pair_key, []).append(rec)
            for ckey in rec.get("currency_keys") or []:
                currency = str(ckey).split("_", 1)[0]
                if focus and currency not in focus:
                    continue
                by_currency.setdefault(str(ckey), []).append(rec)

        blocked_pairs = {
            key: {
                "loss_count": len(items),
                "realized_pl": round(sum(tech.safe_float(x.get("pl"), 0.0) for x in items), 6),
                "recent_losses": items[-5:],
            }
            for key, items in by_pair.items()
            if pair_limit > 0 and len(items) >= pair_limit
        }
        blocked_currency = {
            key: {
                "loss_count": len(items),
                "realized_pl": round(sum(tech.safe_float(x.get("pl"), 0.0) for x in items), 6),
                "recent_losses": items[-5:],
            }
            for key, items in by_currency.items()
            if currency_limit > 0 and len(items) >= currency_limit
        }
        return {
            "active": bool(blocked_pairs or blocked_currency),
            "date_ny": today_ny,
            "pair_loss_limit": pair_limit,
            "currency_loss_limit": currency_limit,
            "blocked_pair_directions": blocked_pairs,
            "blocked_currency_keys": blocked_currency,
            "recent_losing_closes": records[-10:],
        }

    def live_cooldown_reject_reason(self, order: dict[str, Any]) -> str:
        if not self.live_setting_bool("COOLDOWN_ENABLED", False):
            return ""
        inst = tech.normalize_instrument(order.get("instrument", ""))
        direction = str(order.get("direction") or "").upper().strip()
        if not inst or direction not in {"LONG", "SHORT"}:
            return ""
        status = self.live_cooldown_status()
        pair_key = f"{inst}:{direction}"
        pair_hits = status.get("blocked_pair_directions") or {}
        if pair_key in pair_hits:
            count = pair_hits[pair_key].get("loss_count")
            return f"live scout cooldown blocked {pair_key} after {count} losing close(s) today"
        blocked_currency = status.get("blocked_currency_keys") or {}
        keys = [key for key in currency_direction_keys(inst, direction) if key in blocked_currency]
        if keys:
            return "live scout cooldown blocked same currency direction after losses today: " + ", ".join(sorted(keys))
        return ""

    def shared_live_currency_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for root in LIVE_ACCOUNT_STATE_ROOTS.values():
            state = _read_state(root)
            for trade in state.get("last_known_open_trades") or []:
                if not isinstance(trade, dict):
                    continue
                inst = tech.normalize_instrument(trade.get("instrument", ""))
                direction = str(trade.get("direction") or "").upper().strip()
                if direction not in {"LONG", "SHORT"}:
                    direction = _direction_from_units(trade.get("current_units", trade.get("currentUnits", "")))
                for key in currency_direction_keys(inst, direction):
                    counts[key] = counts.get(key, 0) + 1
        return counts

    def live_cross_account_reject_reason(self, order: dict[str, Any]) -> str:
        if not self.live_setting_bool("CROSS_ACCOUNT_EXPOSURE_ENABLED", False):
            return ""
        defaults = self.live_default_thresholds()
        cap = self.live_setting_int("CROSS_ACCOUNT_MAX_SAME_CURRENCY_DIRECTION", defaults["cross_account_currency_cap"])
        if cap <= 0:
            return ""
        focus = set(
            self.live_setting_csv(
                "CROSS_ACCOUNT_FOCUS_CURRENCIES",
                ("USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"),
            )
        )
        inst = tech.normalize_instrument(order.get("instrument", ""))
        direction = str(order.get("direction") or "").upper().strip()
        if not inst or direction not in {"LONG", "SHORT"}:
            return ""
        counts = self.shared_live_currency_counts()
        hits = []
        for key in currency_direction_keys(inst, direction):
            currency = key.split("_", 1)[0]
            if focus and currency not in focus:
                continue
            current = counts.get(key, 0)
            if current >= cap:
                hits.append(f"{key}={current}/{cap}")
        if hits:
            return "cross-account live exposure cap blocks same currency direction: " + ", ".join(sorted(hits))
        return ""

    def maybe_write_live_scout_recap(self, context: str, status: dict[str, Any] | None = None) -> None:
        try:
            if status is None:
                status = self.evaluate_live_scout_failsafe(persist=True)
            force = False
            if status and status.get("active"):
                state = self.load_state()
                last = state.get(live_recap.RECAP_STATE_KEY)
                today_ny = tech.ny_now().date().isoformat()
                force = not (
                    isinstance(last, dict)
                    and last.get("date_ny") == today_ny
                    and last.get("bad_day")
                )
            live_recap.maybe_write_live_recap(self, context=context, force=force)
        except Exception as exc:
            self.log_error("live scout recap", exc)

    def live_score_demotion_reject_reason(self, order: dict[str, Any]) -> str:
        if not self.live_setting_bool("SCORE_DEMOTION_ENABLED", False):
            return ""
        try:
            return live_recap.order_demote_reject_reason(order, self.load_state())
        except Exception:
            return ""

    def live_order_group_value(self, order: dict[str, Any], dimension: str) -> str:
        """Return the comparable live-learning group value for a pending order."""
        dim = str(dimension or "").strip().lower()
        reason = str(order.get("reason") or "")
        audit = order.get("_scout_audit") if isinstance(order.get("_scout_audit"), dict) else {}
        if dim == "score_bucket":
            score = tech.safe_float(
                audit.get("scout_ev_score", order.get("scout_ev_score", order.get("score"))),
                float("nan"),
            )
            if score != score:
                # Last-resort parse from the text carried through the order.
                import re

                match = re.search(r"(?:ev_score|score)=([0-9]+(?:\.[0-9]+)?)", reason)
                score = tech.safe_float(match.group(1), float("nan")) if match else float("nan")
            return live_recap.score_bucket(score) if score == score else ""
        if dim == "theme":
            return live_recap.theme_from_action(order, reason)
        if dim == "model_source":
            return live_recap.model_source_from_action(order, reason)
        if dim == "event_type":
            return "event_scout" if order.get("_event_scout") else "open"
        if dim == "instrument":
            return tech.normalize_instrument(order.get("instrument", ""))
        return ""

    def live_group_quarantine_reject_reason(self, order: dict[str, Any]) -> str:
        """Block fresh live entries from groups already promoted to shadow quarantine.

        The learning report is generated account-locally from closed live trades.
        This guard only acts when explicitly enabled by the live wrapper.  It is
        deliberately narrower than the report: event_type is ignored unless the
        account opts in, so a bad scout day does not accidentally shut off every
        possible scout family forever.
        """
        if not self.live_setting_bool("GROUP_QUARANTINE_ENABLED", False):
            return ""
        if not order.get("_event_scout"):
            return ""
        path = Path(self.cfg.data_dir) / "latest_learning_recommendations.json"
        if not path.exists():
            return ""
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return ""
        candidates = data.get("shadow_candidates") or []
        if not isinstance(candidates, list):
            return ""
        allowed_dims = {
            item.strip().lower()
            for item in self.live_setting_csv(
                "GROUP_QUARANTINE_DIMENSIONS",
                ("score_bucket", "model_source", "theme", "instrument"),
            )
            if item.strip()
        }
        allow_event_type = self.live_setting_bool("GROUP_QUARANTINE_ALLOW_EVENT_TYPE", False)
        min_trades = self.live_setting_int("GROUP_QUARANTINE_MIN_TRADES", 5)
        min_dates = self.live_setting_int("GROUP_QUARANTINE_MIN_DATES", 3)
        min_improvement = self.live_setting_float("GROUP_QUARANTINE_MIN_IMPROVEMENT_USD", 0.0)
        for candidate in candidates:
            if not isinstance(candidate, dict) or candidate.get("type") != "group_quarantine":
                continue
            dim = str(candidate.get("dimension") or "").strip().lower()
            if dim == "event_type" and not allow_event_type:
                continue
            if dim not in allowed_dims:
                continue
            stats = candidate.get("stats") if isinstance(candidate.get("stats"), dict) else {}
            if tech.safe_int(stats.get("closed"), 0) < min_trades:
                continue
            if tech.safe_int(stats.get("distinct_dates"), 0) < min_dates:
                continue
            improvement = tech.safe_float(candidate.get("historical_block_improvement"), 0.0)
            if improvement < min_improvement:
                continue
            wanted = str(candidate.get("value") or "").strip().upper()
            actual = self.live_order_group_value(order, dim).strip().upper()
            if wanted and actual == wanted:
                return (
                    "live group quarantine blocked "
                    f"{dim}={actual} after {tech.safe_int(stats.get('closed'), 0)} closes, "
                    f"loss_rate={tech.safe_float(stats.get('loss_rate'), 0.0):.0%}, "
                    f"realized_pl={tech.safe_float(stats.get('realized_pl'), 0.0):+.4f}"
                )
        return ""

    def open_trade_from_order(self, order: dict[str, Any], prices: dict[str, dict[str, Any]]) -> str:
        inst = tech.normalize_instrument(order.get("instrument", ""))
        direction = str(order.get("direction") or "").upper().strip()
        if direction == "BUY":
            direction = "LONG"
        if direction == "SELL":
            direction = "SHORT"
        order["instrument"] = inst
        order["direction"] = direction
        log_type = "event_scout" if order.get("_event_scout") else "open"

        status = self.evaluate_live_scout_failsafe(persist=True)
        if status.get("active"):
            self.activate_live_scout_failsafe_mode(status)
            self.log_action(order, log_type, "skipped", reject_reason="live scout failsafe active; new exposure blocked")
            return "skipped"

        demotion_reject = self.live_score_demotion_reject_reason(order)
        if demotion_reject:
            self.log_action(order, log_type, "skipped", reject_reason=demotion_reject)
            return "skipped"

        group_quarantine_reject = self.live_group_quarantine_reject_reason(order)
        if group_quarantine_reject:
            self.log_action(order, log_type, "skipped", reject_reason=group_quarantine_reject)
            return "skipped"

        cooldown_reject = self.live_cooldown_reject_reason(order)
        if cooldown_reject:
            self.log_action(order, log_type, "skipped", reject_reason=cooldown_reject)
            return "skipped"

        exposure_reject = self.live_cross_account_reject_reason(order)
        if exposure_reject:
            self.log_action(order, log_type, "skipped", reject_reason=exposure_reject)
            return "skipped"

        return super().open_trade_from_order(order, prices)

    def run_event_scan(self, reason: str = "interval") -> None:
        try:
            account = self.oanda.get_account_summary()
            open_trades = self.oanda.get_open_trades()
            status = self.evaluate_live_scout_failsafe(account=account, persist=True)
            if status.get("active"):
                self.activate_live_scout_failsafe_mode(status)
                self.log_live_scout_failsafe(status, f"event scan {reason}")
                if open_trades:
                    self.run_shutdown_fade_pass(
                        reason=f"live_scout_failsafe_replaced_event_scan:{reason}",
                        account=account,
                        open_trades=open_trades,
                        force=True,
                    )
                state = self.load_state()
                state["last_event_scan_utc"] = tech.iso_utc()
                self.save_state(state)
                self.maybe_write_live_scout_recap(f"event_scan:{reason}", status)
                return
        except Exception as exc:
            self.log_error("live scout failsafe pre-event scan", exc)
        result = super().run_event_scan(reason=reason)
        self.maybe_write_live_scout_recap(f"event_scan:{reason}")
        return result

    def run_local_monitor(self) -> None:
        try:
            account = self.oanda.get_account_summary()
            status = self.evaluate_live_scout_failsafe(account=account, persist=True)
            if status.get("active"):
                self.activate_live_scout_failsafe_mode(status)
        except Exception as exc:
            self.log_error("live scout failsafe pre-monitor", exc)

        super().run_local_monitor()

        try:
            account = self.oanda.get_account_summary()
            open_trades = self.oanda.get_open_trades()
            status = self.evaluate_live_scout_failsafe(account=account, persist=True)
            if status.get("active"):
                self.activate_live_scout_failsafe_mode(status)
                self.log_live_scout_failsafe(status, "local monitor")
                if open_trades:
                    self.run_shutdown_fade_pass(
                        reason="live_scout_failsafe_monitor",
                        account=account,
                        open_trades=open_trades,
                        force=True,
                    )
            self.maybe_write_live_scout_recap("local_monitor", status)
        except Exception as exc:
            self.log_error("live scout failsafe monitor pass", exc)
