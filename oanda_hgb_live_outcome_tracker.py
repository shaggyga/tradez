#!/usr/bin/env python3
"""Track causal, spread-adjusted live outcomes for the guarded HGB practice lane."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from oanda_live_account_readonly_status import cfg_value, list_practice_accounts, read_creds
except ImportError:
    from trad.oanda_live_account_readonly_status import cfg_value, list_practice_accounts, read_creds


UTC = timezone.utc
ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "primary_forecast_rotation_demo_019.json"
DEFAULT_OUTPUT = ROOT / "data" / "oanda_training_manager" / "state" / "hgb_live_outcomes_v1.json"
PRACTICE_URL = "https://api-fxpractice.oanda.com"
R_LEVELS = (0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0)


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(path)


def timeframe_label(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return {
        "30min": "M30", "30m": "M30", "m30": "M30",
        "1h": "H1", "h1": "H1", "60min": "H1",
        "4h": "H4", "h4": "H4", "240min": "H4",
        "1min": "M1", "m1": "M1",
    }.get(normalized, normalized.upper())


def forecast_key(row: dict[str, str]) -> str:
    identity = "|".join(
        str(row.get(field) or "")
        for field in (
            "event", "instrument", "direction", "horizon_minutes",
            "source_stream", "model_feature_timeframe", "model_feature_time_utc",
        )
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]


def inferred_pip_size(row: dict[str, str]) -> float:
    bid = safe_float(row.get("bid"))
    ask = safe_float(row.get("ask"))
    spread = safe_float(row.get("spread_pips"))
    if ask > bid > 0 and spread > 0:
        inferred = (ask - bid) / spread
        if 1e-6 <= inferred <= 0.1:
            return inferred
    quote = str(row.get("instrument") or "").split("_")[-1]
    return 0.01 if quote in {"JPY", "HUF"} else 0.0001


def load_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def resolve_account(creds_path: Path, suffix: str) -> tuple[str, str]:
    text = read_creds(creds_path)
    api_key = cfg_value(text, "OANDA_API_KEY", "OANDA_API_TOKEN")
    accounts = list_practice_accounts(text)
    account_id = next(
        (str(row.get("account_id") or "") for row in accounts if str(row.get("account_id") or "").endswith(f"-{suffix}")),
        "",
    )
    if not api_key or not account_id:
        raise RuntimeError(f"Unable to resolve practice account suffix {suffix}")
    return api_key, account_id


class Tracker:
    def __init__(self, config_path: Path, output_path: Path, creds_path: Path, source_fresh_minutes: float) -> None:
        self.config_path = config_path
        self.output_path = output_path
        self.creds_path = creds_path
        self.source_fresh_minutes = source_fresh_minutes
        self.config = load_payload(config_path)
        self.source = Path(str(self.config.get("data_dir") or "")) / "latest_forecasts.csv"
        self.suffix = str(self.config.get("practice_account_suffix") or "019").zfill(3)
        api_key, self.account_id = resolve_account(creds_path, self.suffix)
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"})
        payload = load_payload(output_path)
        self.pending: dict[str, dict[str, Any]] = {
            str(row.get("forecast_key")): row
            for row in payload.get("pending") or []
            if isinstance(row, dict) and row.get("forecast_key")
        }
        self.outcomes: list[dict[str, Any]] = [row for row in payload.get("outcomes") or [] if isinstance(row, dict)]
        self.seen = {str(row.get("forecast_key")) for row in self.outcomes}
        self.seen.update(self.pending)
        self.errors: list[dict[str, str]] = list(payload.get("recent_errors") or [])[-20:]

    def ingest(self) -> int:
        if not self.source.is_file():
            return 0
        added = 0
        with self.source.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                generated = parse_time(row.get("generated_utc"))
                if generated is None or utc_now() - generated > timedelta(minutes=self.source_fresh_minutes):
                    continue
                key = forecast_key(row)
                if key in self.seen:
                    continue
                horizon = max(1, int(safe_float(row.get("horizon_minutes"), 120)))
                accepted = not str(row.get("reject_reason") or "").strip()
                item = {
                    "forecast_key": key,
                    "forecast_id": row.get("forecast_id"),
                    "generated_utc": generated.isoformat(),
                    "target_utc": (generated + timedelta(minutes=horizon)).isoformat(),
                    "instrument": row.get("instrument"),
                    "direction": str(row.get("direction") or "").lower(),
                    "model_family": "hgb_reversal",
                    "model": row.get("event"),
                    "timeframe": timeframe_label(row.get("model_feature_timeframe")),
                    "horizon_minutes": horizon,
                    "source_stream": row.get("source_stream"),
                    "model_feature_time_utc": row.get("model_feature_time_utc"),
                    "probability": safe_float(row.get("probability")),
                    "rank_score": safe_float(row.get("rank_score")),
                    "edge_pips": safe_float(row.get("edge_pips")),
                    "risk_pips": safe_float(row.get("risk_pips")),
                    "stop_pips": safe_float(row.get("stop_pips")),
                    "take_profit_pips": safe_float(row.get("take_profit_pips")),
                    "trailing_stop_pips": safe_float(row.get("trailing_stop_pips")),
                    "expected_r_multiple": safe_float(row.get("expected_r_multiple")),
                    "model_threshold": safe_float(row.get("model_threshold")),
                    "model_expected_move_atr": safe_float(row.get("model_expected_move_atr")),
                    "model_raw_rank_score": safe_float(row.get("model_raw_rank_score")),
                    "model_score_percentile": safe_float(row.get("model_score_percentile")),
                    "model_atr240_pips": safe_float(row.get("model_atr240_pips")),
                    "model_momentum_30_atr": safe_float(row.get("model_momentum_30_atr")),
                    "pair_threshold_rank_score": safe_float(row.get("pair_threshold_rank_score")),
                    "m1_hold_prob": safe_float(row.get("m1_hold_prob")),
                    "m1_quick_prob": safe_float(row.get("m1_quick_prob")),
                    "m1_overlay_threshold": safe_float(row.get("m1_overlay_threshold")),
                    "m1_overlay_status": row.get("m1_overlay_status") or "",
                    "accepted": accepted,
                    "reject_reason": row.get("reject_reason") or "",
                    "entry_bid": safe_float(row.get("bid")),
                    "entry_ask": safe_float(row.get("ask")),
                    "entry_mid": safe_float(row.get("mid")),
                    "entry_spread_pips": safe_float(row.get("spread_pips")),
                    "pip_size": inferred_pip_size(row),
                    "path_samples": 0,
                    "max_favorable_net_pips": None,
                    "max_adverse_net_pips": None,
                    "max_favorable_inverse_pips": None,
                    "max_adverse_inverse_pips": None,
                    "r_hit_utc": {},
                }
                self.pending[key] = item
                self.seen.add(key)
                added += 1
        return added

    def prices(self, instruments: list[str]) -> dict[str, dict[str, Any]]:
        if not instruments:
            return {}
        response = self.session.get(
            f"{PRACTICE_URL}/v3/accounts/{self.account_id}/pricing",
            params={"instruments": ",".join(sorted(set(instruments)))},
            timeout=20,
        )
        if response.status_code != 200:
            raise RuntimeError(f"pricing HTTP {response.status_code}: {response.text[:200]}")
        result = {}
        for raw in response.json().get("prices") or []:
            bid = safe_float(raw.get("closeoutBid"))
            ask = safe_float(raw.get("closeoutAsk"))
            if not bid and raw.get("bids"):
                bid = safe_float(raw["bids"][0].get("price"))
            if not ask and raw.get("asks"):
                ask = safe_float(raw["asks"][0].get("price"))
            if bid > 0 and ask > 0:
                result[str(raw.get("instrument") or "")] = {
                    "bid": bid, "ask": ask, "mid": (bid + ask) / 2.0,
                    "time": raw.get("time") or utc_now().isoformat(),
                    "status": raw.get("status"),
                }
        return result

    @staticmethod
    def net_pips(row: dict[str, Any], quote: dict[str, Any], *, inverse: bool = False) -> float:
        pip = max(1e-9, safe_float(row.get("pip_size"), 0.0001))
        direction = str(row.get("direction") or "").lower()
        if inverse:
            direction = "short" if direction == "long" else "long"
        if direction == "long":
            return (safe_float(quote.get("bid")) - safe_float(row.get("entry_ask"))) / pip
        return (safe_float(row.get("entry_bid")) - safe_float(quote.get("ask"))) / pip

    def update_paths(self) -> dict[str, dict[str, Any]]:
        if not self.pending:
            return {}
        quotes = self.prices([str(row.get("instrument") or "") for row in self.pending.values()])
        for row in self.pending.values():
            quote = quotes.get(str(row.get("instrument") or ""))
            if not quote:
                continue
            current = self.net_pips(row, quote)
            inverse = self.net_pips(row, quote, inverse=True)
            row["path_samples"] = int(safe_float(row.get("path_samples"))) + 1
            for maximum_key, minimum_key, value in (
                ("max_favorable_net_pips", "max_adverse_net_pips", current),
                ("max_favorable_inverse_pips", "max_adverse_inverse_pips", inverse),
            ):
                prior_max = row.get(maximum_key)
                prior_min = row.get(minimum_key)
                row[maximum_key] = round(value if prior_max is None else max(safe_float(prior_max), value), 6)
                row[minimum_key] = round(value if prior_min is None else min(safe_float(prior_min), value), 6)
            risk = safe_float(row.get("risk_pips"))
            if risk <= 0:
                continue
            hits = row.get("r_hit_utc") if isinstance(row.get("r_hit_utc"), dict) else {}
            observed_utc = str(quote.get("time") or utc_now().isoformat())
            for level in R_LEVELS:
                label = f"{level:g}"
                if current >= risk * level:
                    hits.setdefault(f"tp_{label}", observed_utc)
                if current <= -risk * level:
                    hits.setdefault(f"sl_{label}", observed_utc)
            row["r_hit_utc"] = hits
        return quotes

    def mature(self, quotes: dict[str, dict[str, Any]] | None = None) -> int:
        now = utc_now()
        due = [row for row in self.pending.values() if (parse_time(row.get("target_utc")) or now) <= now]
        if not due:
            return 0
        quotes = quotes or self.prices([str(row.get("instrument") or "") for row in due])
        matured = 0
        for row in due:
            quote = quotes.get(str(row.get("instrument") or ""))
            if not quote:
                continue
            pip = max(1e-9, safe_float(row.get("pip_size"), 0.0001))
            direction = str(row.get("direction") or "").lower()
            if direction == "long":
                net_pips = (quote["bid"] - safe_float(row.get("entry_ask"))) / pip
                signed_mid_pips = (quote["mid"] - safe_float(row.get("entry_mid"))) / pip
            else:
                net_pips = (safe_float(row.get("entry_bid")) - quote["ask"]) / pip
                signed_mid_pips = (safe_float(row.get("entry_mid")) - quote["mid"]) / pip
            outcome = {
                **row,
                "outcome_utc": str(quote["time"]),
                "outcome_delay_sec": round(max(0.0, (now - (parse_time(row.get("target_utc")) or now)).total_seconds()), 3),
                "exit_bid": quote["bid"],
                "exit_ask": quote["ask"],
                "exit_mid": quote["mid"],
                "exit_spread_pips": round((quote["ask"] - quote["bid"]) / pip, 6),
                "actual_signed_move_pips": round(signed_mid_pips, 6),
                "net_pips": round(net_pips, 6),
                "inverse_net_pips": round(self.net_pips(row, quote, inverse=True), 6),
                "shadow_win": bool(net_pips > 0),
                "win": bool(net_pips > 0) if row.get("accepted") else None,
            }
            self.outcomes.append(outcome)
            self.pending.pop(str(row["forecast_key"]), None)
            matured += 1
        self.outcomes = self.outcomes[-50000:]
        return matured

    def save(self, *, added: int, matured: int) -> None:
        accepted_outcomes = [row for row in self.outcomes if row.get("accepted") and row.get("win") is not None]
        atomic_json(
            self.output_path,
            {
                "schema_version": 2,
                "time": utc_now().isoformat(),
                "source": str(self.source),
                "account_id": self.account_id,
                "pending_count": len(self.pending),
                "outcome_count": len(self.outcomes),
                "accepted_outcome_count": len(accepted_outcomes),
                "accepted_win_rate": (
                    sum(1 for row in accepted_outcomes if row.get("win")) / len(accepted_outcomes)
                    if accepted_outcomes else None
                ),
                "last_cycle": {"added": added, "matured": matured},
                "pending": sorted(self.pending.values(), key=lambda row: str(row.get("target_utc"))),
                "outcomes": self.outcomes,
                "recent_errors": self.errors[-20:],
            },
        )

    def cycle(self) -> tuple[int, int]:
        added = self.ingest()
        quotes = self.update_paths()
        matured = self.mature(quotes)
        self.save(added=added, matured=matured)
        return added, matured


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--creds", type=Path, default=ROOT / "creds")
    parser.add_argument("--interval-sec", type=float, default=10.0)
    parser.add_argument("--source-fresh-minutes", type=float, default=10.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    tracker = Tracker(args.config, args.output, args.creds, max(1.0, args.source_fresh_minutes))
    while True:
        try:
            added, matured = tracker.cycle()
            print(json.dumps({"time": utc_now().isoformat(), "added": added, "matured": matured, "pending": len(tracker.pending), "outcomes": len(tracker.outcomes)}), flush=True)
        except Exception as exc:
            tracker.errors.append({"time": utc_now().isoformat(), "error": f"{type(exc).__name__}: {exc}"})
            tracker.save(added=0, matured=0)
            print(json.dumps(tracker.errors[-1]), flush=True)
        if args.once:
            return 0
        time.sleep(max(5.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
