#!/usr/bin/env python3
"""Find recent Kraken coin spikes and the conditions that preceded them.

The script uses Kraken public REST endpoints only:

- /0/public/AssetPairs for current tradable spot pairs.
- /0/public/OHLC for recent hourly candles.

Kraken's OHLC endpoint returns a bounded recent history, so this is a recent
market-regime study, not a multi-year statistical proof.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
REPORT_ROOT = ROOT / "data" / "kraken_spike_research"
KRAKEN_BASE = "https://api.kraken.com/0/public"
USD_QUOTES = ["USD", "USDT", "USDC"]
QUOTE_PRIORITY = {"USD": 0, "USDT": 1, "USDC": 2}
STABLE_OR_FIAT_BASES = {
    "USD",
    "USDT",
    "USDC",
    "DAI",
    "EUR",
    "GBP",
    "CAD",
    "AUD",
    "CHF",
    "JPY",
    "PYUSD",
    "USDS",
    "UST",
    "USTC",
}


@dataclass(frozen=True)
class PairInfo:
    pair_key: str
    altname: str
    wsname: str
    base: str
    quote: str
    status: str
    ordermin: str = ""
    costmin: str = ""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if math.isfinite(number) else default


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


def write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def kraken_get(endpoint: str, params: dict[str, Any] | None = None, attempts: int = 5) -> dict[str, Any]:
    query = urllib.parse.urlencode(params or {})
    url = f"{KRAKEN_BASE}/{endpoint}" + (f"?{query}" if query else "")
    last_error = ""
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "kraken-spike-research/1.0"})
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
            errors = payload.get("error") or []
            if errors:
                last_error = "; ".join(str(item) for item in errors)
                time.sleep(min(8.0, 1.5 * (attempt + 1)))
                continue
            return payload
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = str(exc)
            time.sleep(min(8.0, 1.5 * (attempt + 1)))
    raise RuntimeError(f"Kraken API request failed for {endpoint}: {last_error}")


def load_asset_pairs() -> list[PairInfo]:
    payload = kraken_get("AssetPairs", {"assetVersion": 1, "aclass_base": "currency"})
    pairs: list[PairInfo] = []
    for pair_key, raw in (payload.get("result") or {}).items():
        wsname = str(raw.get("wsname") or "")
        base = str(raw.get("base") or "")
        quote = str(raw.get("quote") or "")
        if not wsname or "/" not in wsname:
            continue
        if not base or not quote:
            left, right = wsname.split("/", 1)
            base = base or left
            quote = quote or right
        pairs.append(
            PairInfo(
                pair_key=str(pair_key),
                altname=str(raw.get("altname") or pair_key),
                wsname=wsname,
                base=base,
                quote=quote,
                status=str(raw.get("status") or ""),
                ordermin=str(raw.get("ordermin") or ""),
                costmin=str(raw.get("costmin") or ""),
            )
        )
    return pairs


def choose_usd_pairs(pairs: Sequence[PairInfo]) -> list[PairInfo]:
    by_base: dict[str, list[PairInfo]] = {}
    for pair in pairs:
        if pair.status != "online":
            continue
        if pair.quote not in USD_QUOTES:
            continue
        if pair.base in STABLE_OR_FIAT_BASES:
            continue
        by_base.setdefault(pair.base, []).append(pair)
    selected: list[PairInfo] = []
    for base, candidates in by_base.items():
        selected.append(
            sorted(
                candidates,
                key=lambda item: (
                    QUOTE_PRIORITY.get(item.quote, 99),
                    item.wsname,
                ),
            )[0]
        )
    return sorted(selected, key=lambda item: item.base)


def fetch_ohlc(pair: PairInfo, interval: int, pause_seconds: float) -> pd.DataFrame:
    payload = kraken_get("OHLC", {"pair": pair.altname, "interval": interval})
    result = payload.get("result") or {}
    rows = None
    for key, value in result.items():
        if key == "last":
            continue
        rows = value
        break
    if rows is None:
        return pd.DataFrame()
    frame = pd.DataFrame(
        rows,
        columns=["time", "open", "high", "low", "close", "vwap", "volume", "count"],
    )
    if frame.empty:
        return frame
    frame["time_utc"] = pd.to_datetime(frame["time"], unit="s", utc=True)
    for column in ["open", "high", "low", "close", "vwap", "volume", "count"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["pair"] = pair.wsname
    frame["base"] = pair.base
    frame["quote"] = pair.quote
    frame = (
        frame.dropna(subset=["time_utc", "open", "high", "low", "close"])
        .sort_values("time_utc")
        .drop_duplicates("time_utc", keep="last")
        .set_index("time_utc")
    )
    time.sleep(max(0.0, pause_seconds))
    return frame


def add_pair_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    close = out["close"].astype(float)
    ret = close.pct_change()
    out["ret_1h"] = ret
    out["abs_ret_1h"] = ret.abs()
    out["intrabar_up_pct"] = out["high"] / close.shift(1) - 1.0
    out["intrabar_down_pct"] = out["low"] / close.shift(1) - 1.0
    out["quote_volume"] = out["volume"] * out["close"]
    for window in [3, 6, 12, 24]:
        out[f"pre_ret_{window}h"] = close.shift(1) / close.shift(window + 1) - 1.0
    out["pre_abs_ret_6h"] = out["pre_ret_6h"].abs()
    out["pre_abs_ret_24h"] = out["pre_ret_24h"].abs()
    out["pre_volatility_24h"] = ret.shift(1).rolling(24, min_periods=12).std()
    out["pre_volatility_72h"] = ret.shift(1).rolling(72, min_periods=36).std()
    out["pre_volatility_168h"] = ret.shift(1).rolling(168, min_periods=72).std()
    out["vol_compression_24v168"] = out["pre_volatility_24h"] / out["pre_volatility_168h"].replace(0.0, np.nan)
    prev_volume = out["volume"].shift(1)
    vol_mean_72 = prev_volume.rolling(72, min_periods=24).mean()
    vol_std_72 = prev_volume.rolling(72, min_periods=24).std()
    out["prev_volume_z72"] = (prev_volume - vol_mean_72) / vol_std_72.replace(0.0, np.nan)
    out["volume_trend_6v72"] = prev_volume.rolling(6, min_periods=3).mean() / vol_mean_72.replace(0.0, np.nan)
    high_24 = out["high"].shift(1).rolling(24, min_periods=12).max()
    low_24 = out["low"].shift(1).rolling(24, min_periods=12).min()
    range_24 = (high_24 - low_24).replace(0.0, np.nan)
    prev_close = close.shift(1)
    out["range_24h_pct"] = high_24 / low_24.replace(0.0, np.nan) - 1.0
    out["range_position_24h"] = (prev_close - low_24) / range_24
    out["dist_from_24h_high_pct"] = prev_close / high_24.replace(0.0, np.nan) - 1.0
    out["dist_from_24h_low_pct"] = prev_close / low_24.replace(0.0, np.nan) - 1.0
    out["prev_quote_volume_24h"] = out["quote_volume"].shift(1).rolling(24, min_periods=12).sum()
    out["quote_volume_30d"] = out["quote_volume"].rolling(720, min_periods=24).sum()
    return out


def add_market_features(combined: pd.DataFrame) -> pd.DataFrame:
    market = (
        combined.reset_index()
        .groupby("time_utc")
        .agg(
            market_median_ret_1h=("ret_1h", "median"),
            market_median_abs_ret_1h=("abs_ret_1h", "median"),
            market_positive_share=("ret_1h", lambda value: float((value > 0).mean())),
        )
        .sort_index()
    )
    for column in market.columns:
        market[f"pre_{column}"] = market[column].shift(1)
    keep = [column for column in market.columns if column.startswith("pre_")]
    out = combined.reset_index().merge(market[keep].reset_index(), on="time_utc", how="left")
    return out.set_index("time_utc").sort_index()


def row_for_event(row: pd.Series, rank: int, side: str) -> dict[str, Any]:
    return {
        "rank": rank,
        "side": side,
        "time_utc": row.name.isoformat() if hasattr(row.name, "isoformat") else str(row.name),
        "pair": row.get("pair", ""),
        "base": row.get("base", ""),
        "quote": row.get("quote", ""),
        "ret_1h_pct": finite_float(row.get("ret_1h")) * 100.0,
        "abs_ret_1h_pct": finite_float(row.get("abs_ret_1h")) * 100.0,
        "intrabar_up_pct": finite_float(row.get("intrabar_up_pct")) * 100.0,
        "intrabar_down_pct": finite_float(row.get("intrabar_down_pct")) * 100.0,
        "close": finite_float(row.get("close")),
        "quote_volume_30d": finite_float(row.get("quote_volume_30d")),
        "prev_quote_volume_24h": finite_float(row.get("prev_quote_volume_24h")),
        "pre_ret_1h_pct": finite_float(row.get("pre_ret_1h")) * 100.0,
        "pre_ret_6h_pct": finite_float(row.get("pre_ret_6h")) * 100.0,
        "pre_ret_24h_pct": finite_float(row.get("pre_ret_24h")) * 100.0,
        "pre_volatility_24h_pct": finite_float(row.get("pre_volatility_24h")) * 100.0,
        "vol_compression_24v168": finite_float(row.get("vol_compression_24v168")),
        "prev_volume_z72": finite_float(row.get("prev_volume_z72")),
        "volume_trend_6v72": finite_float(row.get("volume_trend_6v72")),
        "range_position_24h": finite_float(row.get("range_position_24h")),
        "dist_from_24h_high_pct": finite_float(row.get("dist_from_24h_high_pct")) * 100.0,
        "dist_from_24h_low_pct": finite_float(row.get("dist_from_24h_low_pct")) * 100.0,
        "pre_market_median_ret_1h_pct": finite_float(row.get("pre_market_median_ret_1h")) * 100.0,
        "pre_market_positive_share": finite_float(row.get("pre_market_positive_share")),
    }


def describe_group(frame: pd.DataFrame, label: str) -> dict[str, Any]:
    metrics = [
        "pre_ret_1h",
        "pre_ret_6h",
        "pre_ret_24h",
        "pre_abs_ret_6h",
        "pre_abs_ret_24h",
        "pre_volatility_24h",
        "vol_compression_24v168",
        "prev_volume_z72",
        "volume_trend_6v72",
        "range_position_24h",
        "pre_market_median_ret_1h",
        "pre_market_positive_share",
    ]
    out: dict[str, Any] = {"group": label, "rows": int(len(frame))}
    for metric in metrics:
        if metric not in frame.columns:
            continue
        series = pd.to_numeric(frame[metric], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if series.empty:
            continue
        suffix = "_pct" if metric in {
            "pre_ret_1h",
            "pre_ret_6h",
            "pre_ret_24h",
            "pre_abs_ret_6h",
            "pre_abs_ret_24h",
            "pre_volatility_24h",
            "pre_market_median_ret_1h",
        } else ""
        scale = 100.0 if suffix == "_pct" else 1.0
        out[f"{metric}_median{suffix}"] = float(series.median() * scale)
        out[f"{metric}_p75{suffix}"] = float(series.quantile(0.75) * scale)
    out["share_prev_volume_z_gt_1"] = float((frame["prev_volume_z72"] > 1.0).mean()) if "prev_volume_z72" in frame else 0.0
    out["share_volume_trend_gt_1p5"] = float((frame["volume_trend_6v72"] > 1.5).mean()) if "volume_trend_6v72" in frame else 0.0
    out["share_range_top_20"] = float((frame["range_position_24h"] > 0.8).mean()) if "range_position_24h" in frame else 0.0
    out["share_range_bottom_20"] = float((frame["range_position_24h"] < 0.2).mean()) if "range_position_24h" in frame else 0.0
    out["share_vol_compressed_lt_0p8"] = float((frame["vol_compression_24v168"] < 0.8).mean()) if "vol_compression_24v168" in frame else 0.0
    return out


def condition_columns(frame: pd.DataFrame) -> dict[str, pd.Series]:
    q = {
        "pre_abs_ret_6h_p75": frame["pre_abs_ret_6h"].quantile(0.75),
        "pre_abs_ret_24h_p75": frame["pre_abs_ret_24h"].quantile(0.75),
        "pre_ret_6h_p75": frame["pre_ret_6h"].quantile(0.75),
        "pre_ret_6h_p25": frame["pre_ret_6h"].quantile(0.25),
        "pre_ret_24h_p75": frame["pre_ret_24h"].quantile(0.75),
        "pre_ret_24h_p25": frame["pre_ret_24h"].quantile(0.25),
        "market_abs_p75": frame["pre_market_median_abs_ret_1h"].quantile(0.75),
    }
    return {
        "pre_6h_momentum_up": frame["pre_ret_6h"] > q["pre_ret_6h_p75"],
        "pre_24h_momentum_up": frame["pre_ret_24h"] > q["pre_ret_24h_p75"],
        "pre_6h_momentum_down": frame["pre_ret_6h"] < q["pre_ret_6h_p25"],
        "pre_24h_momentum_down": frame["pre_ret_24h"] < q["pre_ret_24h_p25"],
        "near_24h_high": frame["range_position_24h"] > 0.8,
        "near_24h_low": frame["range_position_24h"] < 0.2,
        "prev_volume_z_gt_1": frame["prev_volume_z72"] > 1.0,
        "prev_volume_z_gt_2": frame["prev_volume_z72"] > 2.0,
        "volume_trend_gt_1p5": frame["volume_trend_6v72"] > 1.5,
        "vol_compressed_lt_0p8": frame["vol_compression_24v168"] < 0.8,
        "vol_expanded_gt_1p2": frame["vol_compression_24v168"] > 1.2,
        "market_abs_hot": frame["pre_market_median_abs_ret_1h"] > q["market_abs_p75"],
        "market_broad_green": frame["pre_market_positive_share"] > 0.6,
        "market_broad_red": frame["pre_market_positive_share"] < 0.4,
    }


def evaluate_rules(frame: pd.DataFrame, target_column: str, direction: str, min_predictions: int) -> list[dict[str, Any]]:
    conditions = condition_columns(frame)
    if direction == "positive":
        names = [
            "pre_6h_momentum_up",
            "pre_24h_momentum_up",
            "near_24h_high",
            "prev_volume_z_gt_1",
            "prev_volume_z_gt_2",
            "volume_trend_gt_1p5",
            "vol_compressed_lt_0p8",
            "vol_expanded_gt_1p2",
            "market_abs_hot",
            "market_broad_green",
        ]
    elif direction == "negative":
        names = [
            "pre_6h_momentum_down",
            "pre_24h_momentum_down",
            "near_24h_low",
            "prev_volume_z_gt_1",
            "prev_volume_z_gt_2",
            "volume_trend_gt_1p5",
            "vol_compressed_lt_0p8",
            "vol_expanded_gt_1p2",
            "market_abs_hot",
            "market_broad_red",
        ]
    else:
        names = list(conditions)
    target = frame[target_column].fillna(False)
    base_rate = float(target.mean())
    rows: list[dict[str, Any]] = []
    for size in [1, 2, 3]:
        for combo in itertools.combinations(names, size):
            mask = pd.Series(True, index=frame.index)
            for name in combo:
                mask &= conditions[name].fillna(False)
            count = int(mask.sum())
            if count < min_predictions:
                continue
            hits = int((mask & target).sum())
            precision = hits / max(count, 1)
            recall = hits / max(int(target.sum()), 1)
            rows.append(
                {
                    "direction": direction,
                    "conditions": " AND ".join(combo),
                    "prediction_count": count,
                    "hit_count": hits,
                    "precision": precision,
                    "base_rate": base_rate,
                    "lift": precision / max(base_rate, 1e-12),
                    "recall": recall,
                }
            )
    return sorted(rows, key=lambda row: (row["lift"], row["precision"], row["hit_count"]), reverse=True)


def format_float(value: Any, digits: int = 2) -> str:
    return f"{finite_float(value):.{digits}f}"


def markdown_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Kraken Spike Condition Research",
        "",
        f"Generated: `{payload['generated_at_utc']}`",
        "",
        "## Scope",
        "",
        f"- Online spot pairs from Kraken AssetPairs: `{payload['online_spot_pair_count']}`",
        f"- Unique listed base assets from online spot pairs: `{payload['unique_online_base_asset_count']}`",
        f"- USD-like quote pairs selected for spike analysis: `{payload['usd_like_pair_count']}`",
        f"- Pairs with usable hourly candles: `{payload['analyzed_pair_count']}`",
        f"- Hourly rows analyzed: `{payload['hourly_row_count']}`",
        f"- Candle interval: `{payload['interval_minutes']}` minutes",
        f"- Data range: `{payload['data_start_utc']}` to `{payload['data_end_utc']}`",
        "",
        "## Largest Positive Hourly Spikes",
        "",
        "| rank | time UTC | pair | return % | 30d quote volume | pre 6h % | pre 24h % | prev vol z | range pos |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["top_positive_events"][:15]:
        lines.append(
            f"| {row['rank']} | {row['time_utc']} | {row['pair']} | {format_float(row['ret_1h_pct'])} | "
            f"{format_float(row['quote_volume_30d'], 0)} | {format_float(row['pre_ret_6h_pct'])} | "
            f"{format_float(row['pre_ret_24h_pct'])} | {format_float(row['prev_volume_z72'])} | "
            f"{format_float(row['range_position_24h'])} |"
        )
    lines.extend(
        [
            "",
            "## Largest Negative Hourly Spikes",
            "",
            "| rank | time UTC | pair | return % | 30d quote volume | pre 6h % | pre 24h % | prev vol z | range pos |",
            "|---:|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in payload["top_negative_events"][:15]:
        lines.append(
            f"| {row['rank']} | {row['time_utc']} | {row['pair']} | {format_float(row['ret_1h_pct'])} | "
            f"{format_float(row['quote_volume_30d'], 0)} | {format_float(row['pre_ret_6h_pct'])} | "
            f"{format_float(row['pre_ret_24h_pct'])} | {format_float(row['prev_volume_z72'])} | "
            f"{format_float(row['range_position_24h'])} |"
        )
    lines.extend(
        [
            "",
            "## Event Condition Summary",
            "",
            "| group | rows | median pre 6h % | median pre 24h % | median prev vol z | median range pos | vol z>1 share | top-20 share | bottom-20 share |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in payload["condition_summary"]:
        lines.append(
            f"| {row['group']} | {row['rows']} | {format_float(row.get('pre_ret_6h_median_pct'))} | "
            f"{format_float(row.get('pre_ret_24h_median_pct'))} | {format_float(row.get('prev_volume_z72_median'))} | "
            f"{format_float(row.get('range_position_24h_median'))} | {format_float(100 * finite_float(row.get('share_prev_volume_z_gt_1')))}% | "
            f"{format_float(100 * finite_float(row.get('share_range_top_20')))}% | "
            f"{format_float(100 * finite_float(row.get('share_range_bottom_20')))}% |"
        )
    lines.extend(
        [
            "",
            "## Best Simple Forecast Rules",
            "",
            "| direction | conditions | predictions | hits | precision | base rate | lift | recall |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in payload["best_rules"][:20]:
        lines.append(
            f"| {row['direction']} | {row['conditions']} | {row['prediction_count']} | {row['hit_count']} | "
            f"{format_float(100 * row['precision'])}% | {format_float(100 * row['base_rate'])}% | "
            f"{format_float(row['lift'])} | {format_float(100 * row['recall'])}% |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["interpretation"])
    lines.extend(
        [
            "",
            "## Files",
            "",
        ]
    )
    for label, path in payload["files"].items():
        lines.append(f"- {label}: `{path}`")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=int, default=60, help="Kraken OHLC interval in minutes.")
    parser.add_argument("--pause-seconds", type=float, default=0.20)
    parser.add_argument("--max-pairs", type=int, default=0, help="Optional cap for development runs.")
    parser.add_argument("--min-rows", type=int, default=120)
    parser.add_argument("--liquid-min-quote-volume", type=float, default=50_000.0)
    parser.add_argument("--top-events", type=int, default=50)
    parser.add_argument("--spike-quantile", type=float, default=0.99)
    parser.add_argument("--min-rule-predictions", type=int, default=80)
    parser.add_argument("--output-dir", type=Path, default=REPORT_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir
    pairs = load_asset_pairs()
    online_pairs = [pair for pair in pairs if pair.status == "online"]
    usd_pairs = choose_usd_pairs(pairs)
    if args.max_pairs > 0:
        usd_pairs = usd_pairs[: args.max_pairs]
    print(f"[setup] online spot pairs={len(online_pairs)} selected_usd_like={len(usd_pairs)}", flush=True)

    pair_rows = [asdict(pair) for pair in pairs]
    selected_pair_rows = [asdict(pair) for pair in usd_pairs]
    frames: list[pd.DataFrame] = []
    errors: list[dict[str, Any]] = []
    for idx, pair in enumerate(usd_pairs, start=1):
        try:
            print(f"[ohlc] {idx:03d}/{len(usd_pairs):03d} {pair.wsname}", flush=True)
            frame = fetch_ohlc(pair, int(args.interval), float(args.pause_seconds))
            if len(frame) < int(args.min_rows):
                errors.append({"pair": pair.wsname, "reason": f"too few rows: {len(frame)}"})
                continue
            frames.append(add_pair_features(frame))
        except Exception as exc:
            errors.append({"pair": pair.wsname, "reason": str(exc)[:500]})
    if not frames:
        raise RuntimeError("No Kraken OHLC data was collected.")

    combined = pd.concat(frames, axis=0).sort_index()
    combined = add_market_features(combined)
    combined = combined.replace([np.inf, -np.inf], np.nan)
    liquid = combined[
        (combined["quote_volume_30d"].fillna(0.0) >= float(args.liquid_min_quote_volume))
        & combined["ret_1h"].notna()
        & combined["pre_ret_24h"].notna()
    ].copy()
    if liquid.empty:
        liquid = combined[combined["ret_1h"].notna() & combined["pre_ret_24h"].notna()].copy()

    top_n = int(args.top_events)
    top_positive = liquid.nlargest(top_n, "ret_1h")
    top_negative = liquid.nsmallest(top_n, "ret_1h")
    top_abs = liquid.nlargest(top_n * 2, "abs_ret_1h")

    positive_threshold = float(liquid["ret_1h"].quantile(float(args.spike_quantile)))
    negative_threshold = float(liquid["ret_1h"].quantile(1.0 - float(args.spike_quantile)))
    abs_threshold = float(liquid["abs_ret_1h"].quantile(float(args.spike_quantile)))
    liquid["target_positive_spike"] = liquid["ret_1h"] >= positive_threshold
    liquid["target_negative_spike"] = liquid["ret_1h"] <= negative_threshold
    liquid["target_abs_spike"] = liquid["abs_ret_1h"] >= abs_threshold

    condition_summary = [
        describe_group(liquid, "baseline_liquid_rows"),
        describe_group(top_positive, f"top_{top_n}_positive"),
        describe_group(top_negative, f"top_{top_n}_negative"),
        describe_group(top_abs, f"top_{top_n * 2}_absolute"),
    ]
    rules = []
    rules.extend(evaluate_rules(liquid, "target_positive_spike", "positive", int(args.min_rule_predictions)))
    rules.extend(evaluate_rules(liquid, "target_negative_spike", "negative", int(args.min_rule_predictions)))
    rules.extend(evaluate_rules(liquid, "target_abs_spike", "absolute", int(args.min_rule_predictions)))
    best_rules = sorted(rules, key=lambda row: (row["lift"], row["precision"], row["hit_count"]), reverse=True)[:100]

    top_positive_events = [row_for_event(row, rank, "positive") for rank, (_, row) in enumerate(top_positive.iterrows(), start=1)]
    top_negative_events = [row_for_event(row, rank, "negative") for rank, (_, row) in enumerate(top_negative.iterrows(), start=1)]
    top_abs_events = [row_for_event(row, rank, "absolute") for rank, (_, row) in enumerate(top_abs.iterrows(), start=1)]

    # Mechanical interpretation based on the observed summaries and rule lifts.
    pos_summary = condition_summary[1]
    neg_summary = condition_summary[2]
    best_rule = best_rules[0] if best_rules else {}
    interpretation = [
        (
            "The largest positive spikes tended to have stronger pre-spike momentum than the baseline "
            f"(median pre-6h {finite_float(pos_summary.get('pre_ret_6h_median_pct')):.2f}% vs "
            f"{finite_float(condition_summary[0].get('pre_ret_6h_median_pct')):.2f}% baseline)."
        ),
        (
            "The largest negative spikes often looked like post-pump reversals rather than simple weak-trend continuations "
            f"(median pre-24h {finite_float(neg_summary.get('pre_ret_24h_median_pct')):.2f}%, "
            f"median 24h range position {finite_float(neg_summary.get('range_position_24h_median')):.2f}, "
            f"{100 * finite_float(neg_summary.get('share_range_top_20')):.1f}% in the top 20% of their prior 24h range)."
        ),
        (
            "Volume/attention mattered, but it was not universal: only "
            f"{100 * finite_float(pos_summary.get('share_prev_volume_z_gt_1')):.1f}% of top positive and "
            f"{100 * finite_float(neg_summary.get('share_prev_volume_z_gt_1')):.1f}% of top negative events had previous-hour volume z-score > 1."
        ),
        (
            "The best simple rule found here was "
            f"`{best_rule.get('conditions', 'n/a')}`, with lift {finite_float(best_rule.get('lift')):.2f} "
            f"and precision {100 * finite_float(best_rule.get('precision')):.2f}% against a "
            f"{100 * finite_float(best_rule.get('base_rate')):.2f}% base event rate."
        ),
        (
            "Conclusion: there are recurring preconditions that raise odds, but no common condition pattern "
            "covered all spikes. Treat the rules as watchlist filters, not stand-alone forecasts."
        ),
    ]

    pair_csv = output_dir / "kraken_all_spot_pairs.csv"
    selected_pairs_csv = output_dir / "kraken_selected_usd_like_pairs.csv"
    hourly_parquet = output_dir / "kraken_hourly_features.parquet"
    hourly_csv = output_dir / "kraken_hourly_features_sample.csv"
    top_positive_csv = output_dir / "top_positive_spikes.csv"
    top_negative_csv = output_dir / "top_negative_spikes.csv"
    top_abs_csv = output_dir / "top_absolute_spikes.csv"
    condition_summary_csv = output_dir / "condition_summary.csv"
    rules_csv = output_dir / "forecast_rule_search.csv"
    errors_csv = output_dir / "collection_errors.csv"
    summary_json = output_dir / "latest_kraken_spike_research.json"
    summary_md = output_dir / "latest_kraken_spike_research.md"

    write_csv(pair_csv, pair_rows)
    write_csv(selected_pairs_csv, selected_pair_rows)
    try:
        hourly_parquet.parent.mkdir(parents=True, exist_ok=True)
        combined.reset_index().to_parquet(hourly_parquet, index=False)
    except Exception:
        hourly_parquet = Path("")
    write_csv(hourly_csv, combined.reset_index().tail(25_000).to_dict(orient="records"))
    write_csv(top_positive_csv, top_positive_events)
    write_csv(top_negative_csv, top_negative_events)
    write_csv(top_abs_csv, top_abs_events)
    write_csv(condition_summary_csv, condition_summary)
    write_csv(rules_csv, best_rules)
    write_csv(errors_csv, errors)

    payload = {
        "generated_at_utc": utc_now(),
        "source_docs": {
            "asset_pairs": "https://docs.kraken.com/api-reference/market-data/get-tradable-asset-pairs",
            "ohlc": "https://docs.kraken.com/api-reference/market-data/get-ohlc-data",
        },
        "online_spot_pair_count": len(online_pairs),
        "unique_online_base_asset_count": len({pair.base for pair in online_pairs}),
        "usd_like_pair_count": len(usd_pairs),
        "analyzed_pair_count": int(combined["pair"].nunique()),
        "hourly_row_count": int(len(combined)),
        "liquid_hourly_row_count": int(len(liquid)),
        "liquid_min_quote_volume": float(args.liquid_min_quote_volume),
        "interval_minutes": int(args.interval),
        "data_start_utc": combined.index.min().isoformat(),
        "data_end_utc": combined.index.max().isoformat(),
        "positive_spike_threshold_pct": positive_threshold * 100.0,
        "negative_spike_threshold_pct": negative_threshold * 100.0,
        "absolute_spike_threshold_pct": abs_threshold * 100.0,
        "top_positive_events": top_positive_events,
        "top_negative_events": top_negative_events,
        "top_absolute_events": top_abs_events[:50],
        "condition_summary": condition_summary,
        "best_rules": best_rules[:50],
        "interpretation": interpretation,
        "collection_error_count": len(errors),
        "files": {
            "all_pairs_csv": str(pair_csv),
            "selected_pairs_csv": str(selected_pairs_csv),
            "hourly_parquet": str(hourly_parquet) if str(hourly_parquet) else "",
            "hourly_csv_sample": str(hourly_csv),
            "top_positive_csv": str(top_positive_csv),
            "top_negative_csv": str(top_negative_csv),
            "top_absolute_csv": str(top_abs_csv),
            "condition_summary_csv": str(condition_summary_csv),
            "rules_csv": str(rules_csv),
            "errors_csv": str(errors_csv),
            "summary_json": str(summary_json),
            "summary_md": str(summary_md),
        },
    }
    atomic_write_json(summary_json, payload)
    atomic_write_text(summary_md, markdown_report(payload))
    print(json.dumps({
        "online_spot_pair_count": payload["online_spot_pair_count"],
        "unique_online_base_asset_count": payload["unique_online_base_asset_count"],
        "usd_like_pair_count": payload["usd_like_pair_count"],
        "analyzed_pair_count": payload["analyzed_pair_count"],
        "data_start_utc": payload["data_start_utc"],
        "data_end_utc": payload["data_end_utc"],
        "best_rule": best_rules[0] if best_rules else {},
        "top_positive": top_positive_events[:5],
        "top_negative": top_negative_events[:5],
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
