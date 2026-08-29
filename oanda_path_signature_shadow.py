#!/usr/bin/env python3
"""Research-only path-signature audit on causal OANDA bid/ask candles.

The study compares a small price/volatility baseline with the same baseline plus
level-two signatures of time, cumulative return, and cumulative variation.  It
has no order adapter and only performs authenticated GET requests.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import requests
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

try:
    from oanda_practice_eurusd_micro_scalper import BASE_URL
    from oanda_practice_pair_rotation_scalper import read_credentials
except ModuleNotFoundError:  # Package imports used by tests.
    from trad.oanda_practice_eurusd_micro_scalper import BASE_URL
    from trad.oanda_practice_pair_rotation_scalper import read_credentials


CAN_PLACE_ORDERS = False
RESEARCH_ONLY = True
WINDOWS = (16, 32, 64)
HORIZONS = (5, 15, 30, 60)


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def level_two_signature(path: np.ndarray) -> np.ndarray:
    """Return the level-one and level-two signature of a piecewise-linear path."""
    if path.ndim != 2 or len(path) < 2:
        raise ValueError("path must have shape (n>=2, channels)")
    deltas = np.diff(path, axis=0)
    first = np.sum(deltas, axis=0)
    prefix = np.cumsum(deltas, axis=0) - deltas
    second = np.einsum("ni,nj->ij", prefix, deltas)
    second += 0.5 * np.einsum("ni,nj->ij", deltas, deltas)
    return np.concatenate((first, second.reshape(-1)))


def path_features(mid: np.ndarray, pip: float) -> tuple[np.ndarray, np.ndarray]:
    """Build transparent baseline and additive signature features."""
    returns = np.diff(mid) / max(pip, 1e-12)
    baseline: list[float] = []
    signatures: list[float] = []
    for window in WINDOWS:
        chunk = returns[-window:]
        scale = max(float(np.std(chunk)), 0.25)
        cumulative = np.concatenate(([0.0], np.cumsum(chunk) / scale))
        variation = np.concatenate(([0.0], np.cumsum(np.abs(chunk))))
        variation /= max(float(variation[-1]), 1e-9)
        clock = np.linspace(0.0, 1.0, len(cumulative))
        path = np.column_stack((clock, cumulative, variation))
        total = float(np.sum(chunk))
        absolute = float(np.sum(np.abs(chunk)))
        baseline.extend(
            (
                total,
                float(chunk[-1]),
                float(np.mean(np.abs(chunk))),
                float(np.std(chunk)),
                abs(total) / max(absolute, 1e-9),
                float(np.max(np.cumsum(chunk)) - np.min(np.cumsum(chunk))),
            )
        )
        signatures.extend(level_two_signature(path).tolist())
    return np.asarray(baseline), np.asarray(signatures)


def parse_candles(payload: dict[str, Any]) -> dict[str, np.ndarray]:
    epochs: list[float] = []
    bids: list[float] = []
    asks: list[float] = []
    for candle in payload.get("candles") or []:
        if not candle.get("complete"):
            continue
        try:
            epoch = datetime.fromisoformat(
                str(candle["time"]).replace("Z", "+00:00")
            ).timestamp()
            bid = float(candle["bid"]["c"])
            ask = float(candle["ask"]["c"])
        except (KeyError, TypeError, ValueError):
            continue
        if ask < bid:
            continue
        epochs.append(epoch)
        bids.append(bid)
        asks.append(ask)
    bid_array = np.asarray(bids, dtype=float)
    ask_array = np.asarray(asks, dtype=float)
    return {
        "epoch": np.asarray(epochs, dtype=float),
        "bid": bid_array,
        "ask": ask_array,
        "mid": (bid_array + ask_array) / 2.0,
    }


def fetch_candles(
    token: str, instrument: str, count: int, timeout_sec: float = 30.0
) -> tuple[str, dict[str, np.ndarray]]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Datetime-Format": "RFC3339",
    }
    last_error = ""
    for attempt in range(3):
        try:
            response = requests.get(
                f"{BASE_URL}/v3/instruments/{instrument}/candles",
                headers=headers,
                params={"granularity": "M1", "count": count, "price": "BA"},
                timeout=timeout_sec,
            )
            if response.status_code < 400:
                return instrument, parse_candles(response.json())
            last_error = f"HTTP {response.status_code}"
        except requests.RequestException as exc:
            last_error = type(exc).__name__
        time.sleep(0.5 * (2**attempt))
    raise RuntimeError(f"{instrument}: {last_error or 'request failed'}")


def pip_size(instrument: str) -> float:
    quote = instrument.split("_")[-1]
    return 0.01 if quote == "JPY" else 0.0001


def build_rows(
    instrument: str, candles: dict[str, np.ndarray], horizon: int
) -> dict[str, list[Any]]:
    n = len(candles["mid"])
    train_boundary = int(n * 0.60)
    dev_boundary = int(n * 0.80)
    pip = pip_size(instrument)
    rows: dict[str, list[Any]] = {
        "baseline": [],
        "signature": [],
        "target": [],
        "long_net": [],
        "short_net": [],
        "spread": [],
        "pair": [],
        "split": [],
    }
    start = max(WINDOWS)
    for index in range(start, n - horizon, horizon):
        target_index = index + horizon
        elapsed = candles["epoch"][target_index] - candles["epoch"][index]
        if elapsed > horizon * 60.0 + 120.0:
            continue
        if target_index < train_boundary:
            split = "train"
        elif index >= train_boundary and target_index < dev_boundary:
            split = "dev"
        elif index >= dev_boundary:
            split = "test"
        else:
            continue
        baseline, signature = path_features(
            candles["mid"][index - max(WINDOWS) : index + 1], pip
        )
        spread = (candles["ask"][index] - candles["bid"][index]) / pip
        target = (candles["mid"][target_index] - candles["mid"][index]) / pip
        long_net = (candles["bid"][target_index] - candles["ask"][index]) / pip
        short_net = (candles["bid"][index] - candles["ask"][target_index]) / pip
        rows["baseline"].append(np.concatenate((baseline, [spread])))
        rows["signature"].append(signature)
        rows["target"].append(target)
        rows["long_net"].append(long_net)
        rows["short_net"].append(short_net)
        rows["spread"].append(spread)
        rows["pair"].append(instrument)
        rows["split"].append(split)
    return rows


def cluster_interval(values: np.ndarray, pairs: np.ndarray) -> tuple[float, float]:
    unique = sorted(set(pairs.tolist()))
    if len(unique) < 2 or not len(values):
        return float("nan"), float("nan")
    grouped = {pair: values[pairs == pair] for pair in unique}
    rng = random.Random(20260804)
    samples: list[float] = []
    for _ in range(500):
        draw = [rng.choice(unique) for _ in unique]
        joined = np.concatenate([grouped[pair] for pair in draw])
        samples.append(float(np.mean(joined)))
    return tuple(float(x) for x in np.quantile(samples, [0.05, 0.95]))


def metrics(
    predictions: np.ndarray,
    target: np.ndarray,
    long_net: np.ndarray,
    short_net: np.ndarray,
    spreads: np.ndarray,
    pairs: np.ndarray,
    threshold: float,
    *,
    bootstrap: bool = True,
) -> dict[str, Any]:
    take = np.abs(predictions) >= np.maximum(threshold, 1.5 * spreads)
    direction = np.where(predictions >= 0.0, 1.0, -1.0)
    realised = np.where(direction > 0.0, long_net, short_net)[take]
    selected_pairs = pairs[take]
    signed_target = direction[take] * target[take]
    lo, hi = (
        cluster_interval(realised, selected_pairs)
        if bootstrap
        else (float("nan"), float("nan"))
    )
    trimmed = np.sort(realised)[:-1] if len(realised) > 1 else realised
    return {
        "trades": int(len(realised)),
        "coverage": round(float(np.mean(take)), 6),
        "direction_accuracy": round(float(np.mean(signed_target > 0.0)), 6)
        if len(realised)
        else None,
        "win_rate": round(float(np.mean(realised > 0.0)), 6) if len(realised) else None,
        "average_net_pips": round(float(np.mean(realised)), 6) if len(realised) else None,
        "median_net_pips": round(float(np.median(realised)), 6) if len(realised) else None,
        "total_net_pips": round(float(np.sum(realised)), 6),
        "average_without_largest": round(float(np.mean(trimmed)), 6)
        if len(trimmed)
        else None,
        "pair_cluster_ci90": [round(lo, 6), round(hi, 6)]
        if math.isfinite(lo)
        else None,
    }


def choose_threshold(
    predictions: np.ndarray,
    target: np.ndarray,
    long_net: np.ndarray,
    short_net: np.ndarray,
    spreads: np.ndarray,
    pairs: np.ndarray,
) -> float:
    choices = [0.0] + [float(x) for x in np.quantile(np.abs(predictions), [0.5, 0.7, 0.8, 0.9, 0.95])]
    best = (float("-inf"), float("inf"))
    for threshold in sorted(set(choices)):
        result = metrics(
            predictions,
            target,
            long_net,
            short_net,
            spreads,
            pairs,
            threshold,
            bootstrap=False,
        )
        score = finite(result.get("average_net_pips"), float("-inf"))
        if int(result["trades"]) < 30:
            score = float("-inf")
        candidate = (score, -threshold)
        if candidate > best:
            best = candidate
    return -best[1] if math.isfinite(best[0]) else float("inf")


def fit_arm(
    features: np.ndarray,
    target: np.ndarray,
    long_net: np.ndarray,
    short_net: np.ndarray,
    spreads: np.ndarray,
    pairs: np.ndarray,
    splits: np.ndarray,
) -> dict[str, Any]:
    train = splits == "train"
    dev = splits == "dev"
    test = splits == "test"
    scaler = StandardScaler().fit(features[train])
    model = Ridge(alpha=10.0).fit(scaler.transform(features[train]), target[train])
    dev_predictions = model.predict(scaler.transform(features[dev]))
    threshold = choose_threshold(
        dev_predictions,
        target[dev],
        long_net[dev],
        short_net[dev],
        spreads[dev],
        pairs[dev],
    )
    test_predictions = model.predict(scaler.transform(features[test]))
    residual = test_predictions - target[test]
    return {
        "ridge_alpha": 10.0,
        "selected_dev_absolute_prediction_threshold_pips": round(threshold, 6),
        "test_rmse_pips": round(float(np.sqrt(np.mean(residual**2))), 6),
        "test_mae_pips": round(float(np.mean(np.abs(residual))), 6),
        "test_all_direction_accuracy": round(
            float(np.mean(np.sign(test_predictions) * target[test] > 0.0)), 6
        ),
        "test_trading": metrics(
            test_predictions,
            target[test],
            long_net[test],
            short_net[test],
            spreads[test],
            pairs[test],
            threshold,
        ),
    }


def concatenate(rows: list[dict[str, list[Any]]], key: str) -> np.ndarray:
    values = [value for row in rows for value in row[key]]
    if key in {"baseline", "signature"}:
        return np.asarray(values, dtype=float)
    return np.asarray(values)


def study(candle_sets: dict[str, dict[str, np.ndarray]]) -> dict[str, Any]:
    horizons: dict[str, Any] = {}
    for horizon in HORIZONS:
        rows = [build_rows(pair, candles, horizon) for pair, candles in candle_sets.items()]
        baseline = concatenate(rows, "baseline")
        signatures = concatenate(rows, "signature")
        target = concatenate(rows, "target").astype(float)
        long_net = concatenate(rows, "long_net").astype(float)
        short_net = concatenate(rows, "short_net").astype(float)
        spreads = concatenate(rows, "spread").astype(float)
        pairs = concatenate(rows, "pair")
        splits = concatenate(rows, "split")
        if not len(target) or min(np.sum(splits == part) for part in ("train", "dev", "test")) < 30:
            horizons[f"M{horizon}"] = {"status": "insufficient_rows"}
            continue
        baseline_result = fit_arm(
            baseline, target, long_net, short_net, spreads, pairs, splits
        )
        augmented_result = fit_arm(
            np.column_stack((baseline, signatures)),
            target,
            long_net,
            short_net,
            spreads,
            pairs,
            splits,
        )
        base_net = finite(baseline_result["test_trading"].get("average_net_pips"), float("nan"))
        aug_net = finite(augmented_result["test_trading"].get("average_net_pips"), float("nan"))
        liquid = spreads <= 3.0
        liquid_result: dict[str, Any] | None = None
        if min(np.sum(liquid & (splits == part)) for part in ("train", "dev", "test")) >= 30:
            liquid_base = fit_arm(
                baseline[liquid],
                target[liquid],
                long_net[liquid],
                short_net[liquid],
                spreads[liquid],
                pairs[liquid],
                splits[liquid],
            )
            liquid_augmented = fit_arm(
                np.column_stack((baseline, signatures))[liquid],
                target[liquid],
                long_net[liquid],
                short_net[liquid],
                spreads[liquid],
                pairs[liquid],
                splits[liquid],
            )
            liquid_base_net = finite(
                liquid_base["test_trading"].get("average_net_pips"), float("nan")
            )
            liquid_augmented_net = finite(
                liquid_augmented["test_trading"].get("average_net_pips"), float("nan")
            )
            liquid_result = {
                "contract": "entry spread <= 3.0 pips in every split",
                "rows": int(np.sum(liquid)),
                "baseline": liquid_base,
                "baseline_plus_level2_signature": liquid_augmented,
                "signature_incremental_average_net_pips": round(
                    liquid_augmented_net - liquid_base_net, 6
                )
                if math.isfinite(liquid_base_net) and math.isfinite(liquid_augmented_net)
                else None,
                "signature_incremental_rmse_pips": round(
                    liquid_base["test_rmse_pips"]
                    - liquid_augmented["test_rmse_pips"],
                    6,
                ),
            }
        horizons[f"M{horizon}"] = {
            "status": "tested_shadow_only",
            "rows": int(len(target)),
            "split_rows": {part: int(np.sum(splits == part)) for part in ("train", "dev", "test")},
            "pair_count": int(len(set(pairs.tolist()))),
            "baseline": baseline_result,
            "baseline_plus_level2_signature": augmented_result,
            "signature_incremental_average_net_pips": round(aug_net - base_net, 6)
            if math.isfinite(base_net) and math.isfinite(aug_net)
            else None,
            "signature_incremental_rmse_pips": round(
                baseline_result["test_rmse_pips"] - augmented_result["test_rmse_pips"], 6
            ),
            "liquid_spread_bucket": liquid_result,
        }
    return horizons


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Path-signature FX shadow audit",
        "",
        f"Generated: {report['generated_utc']}",
        "",
        "Research-only. No order or execution adapter exists.",
        "",
        "| Horizon | Baseline avg | Signature avg | Increment | Signature CI90 | Decision |",
        "|---|---:|---:|---:|---|---|",
    ]
    for label, row in report["horizons"].items():
        if row.get("status") != "tested_shadow_only":
            lines.append(f"| {label} | n/a | n/a | n/a | n/a | insufficient data |")
            continue
        display = row.get("liquid_spread_bucket") or row
        base = display["baseline"]["test_trading"]
        aug = display["baseline_plus_level2_signature"]["test_trading"]
        ci = aug.get("pair_cluster_ci90")
        ci_text = f"[{ci[0]:.3f}, {ci[1]:.3f}]" if ci else "n/a"
        decision = "retain for more shadow evidence" if ci and ci[0] > 0 else "reject promotion"
        lines.append(
            f"| {label} | {base.get('average_net_pips')} | {aug.get('average_net_pips')} | "
            f"{display.get('signature_incremental_average_net_pips')} | {ci_text} | {decision} |"
        )
    lines.extend(
        [
            "",
            "The table reports the fixed <=3-pip liquid bucket when sufficient. Thresholds are selected only on the middle chronological block. The final block is untouched during selection; outcomes use executable bid/ask closes and require predicted gross movement of at least 1.5x entry spread.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parent
    parser.add_argument("--creds", type=Path, default=root / "creds")
    parser.add_argument("--account-key", default="OANDA_ACCOUNT_ID_DUM4")
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument(
        "--quotes",
        type=Path,
        default=root / "data/oanda_training_manager/state/practice_007_market_quotes_v1.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "data/oanda_training_manager/reports/unique_predictor_watch_20260804/path_signature_shadow_v1.json",
    )
    args = parser.parse_args()
    token, _ = read_credentials(args.creds, args.account_key, "")
    quote_payload = json.loads(args.quotes.read_text(encoding="utf-8"))
    quote_rows = quote_payload.get("quotes") or quote_payload.get("prices") or []
    if isinstance(quote_rows, dict):
        instruments = sorted(str(name) for name in quote_rows if str(name))
    else:
        instruments = sorted(
            {
                str(row.get("instrument") or "")
                for row in quote_rows
                if isinstance(row, dict) and str(row.get("instrument") or "")
            }
        )
    candle_sets: dict[str, dict[str, np.ndarray]] = {}
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(fetch_candles, token, instrument, args.count): instrument
            for instrument in instruments
        }
        for future in as_completed(futures):
            instrument = futures[future]
            try:
                name, candles = future.result()
                if len(candles["mid"]) >= 500:
                    candle_sets[name] = candles
            except Exception as exc:  # Error strings intentionally omit credentials.
                errors.append(f"{instrument}: {type(exc).__name__}: {exc}")
    report = {
        "schema_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "research_only": True,
        "can_place_orders": False,
        "data_contract": {
            "source": "OANDA practice GET M1 candles with bid and ask components",
            "requested_instruments": len(instruments),
            "usable_instruments": len(candle_sets),
            "candle_count_per_instrument": args.count,
            "chronological_split": "oldest 60% train / next 20% threshold selection / newest 20% untouched test",
            "purging": "nonoverlapping horizon-stride labels; split-crossing labels excluded",
            "costs": "entry ask-to-exit bid for longs; entry bid-to-exit ask for shorts",
        },
        "errors": errors,
        "horizons": study(candle_sets),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    args.output.with_suffix(".md").write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "usable_instruments": len(candle_sets), "errors": len(errors)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
