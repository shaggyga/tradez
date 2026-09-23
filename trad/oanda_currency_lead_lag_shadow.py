#!/usr/bin/env python3
"""Research-only audit of lagged currency-return networks on OANDA M1 data.

The experiment asks a narrow question: do lagged returns from *other*
currencies improve an own-pair autoregressive baseline out of sample and after
executable bid/ask costs?  It contains no broker order adapter.
"""

from __future__ import annotations

import argparse
import json
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

try:
    from oanda_path_signature_shadow import (
        choose_threshold,
        fetch_candles,
        metrics,
        pip_size,
        read_credentials,
    )
except ModuleNotFoundError:  # Package imports used by tests.
    from trad.oanda_path_signature_shadow import (
        choose_threshold,
        fetch_candles,
        metrics,
        pip_size,
        read_credentials,
    )


RESEARCH_ONLY = True
CAN_PLACE_ORDERS = False
HORIZONS = (5, 15, 30, 60)
LAGS = (1, 2, 3, 5, 10)


def currencies_for(instruments: list[str]) -> list[str]:
    return sorted({currency for pair in instruments for currency in pair.split("_")})


def latent_currency_returns(
    candle_sets: dict[str, dict[str, np.ndarray]],
    *,
    max_input_spread_pips: float = 3.0,
) -> tuple[list[str], dict[int, np.ndarray]]:
    """Solve r(base)-r(quote)=r(pair) at each completed minute.

    Only contemporaneously known closes and liquid observations enter the
    solve.  The zero-sum row identifies the otherwise relative currency level.
    """
    currencies = currencies_for(list(candle_sets))
    currency_index = {currency: i for i, currency in enumerate(currencies)}
    observations: dict[int, list[tuple[str, float]]] = {}
    for pair, candles in candle_sets.items():
        epochs = candles["epoch"].astype(int)
        mids = candles["mid"]
        spreads = (candles["ask"] - candles["bid"]) / pip_size(pair)
        for index in range(1, len(epochs)):
            if epochs[index] - epochs[index - 1] > 90:
                continue
            if spreads[index] > max_input_spread_pips:
                continue
            value = float(math.log(mids[index] / mids[index - 1]) * 10_000.0)
            observations.setdefault(int(epochs[index]), []).append((pair, value))

    result: dict[int, np.ndarray] = {}
    width = len(currencies)
    for epoch, rows in observations.items():
        active = {currency for pair, _ in rows for currency in pair.split("_")}
        if len(rows) < 6 or len(active) < 4:
            continue
        matrix = np.zeros((len(rows) + 1, width), dtype=float)
        target = np.zeros(len(rows) + 1, dtype=float)
        for row_index, (pair, value) in enumerate(rows):
            base, quote = pair.split("_")
            matrix[row_index, currency_index[base]] = 1.0
            matrix[row_index, currency_index[quote]] = -1.0
            target[row_index] = value
        matrix[-1, :] = 1.0 / max(width, 1)
        solved, *_ = np.linalg.lstsq(matrix, target, rcond=None)
        result[epoch] = solved
    return currencies, result


def own_lag_features(mid: np.ndarray, index: int, pip: float) -> np.ndarray:
    returns = np.diff(mid[index - max(LAGS) - 20 : index + 1]) / pip
    lagged = [float(returns[-lag]) for lag in LAGS]
    recent = returns[-20:]
    return np.asarray(
        lagged
        + [
            float(np.mean(recent[-5:])),
            float(np.mean(recent[-10:])),
            float(np.std(recent[-10:])),
            float(np.std(recent)),
        ],
        dtype=float,
    )


def network_features(
    epoch: int,
    currencies: list[str],
    latent: dict[int, np.ndarray],
) -> np.ndarray | None:
    vectors: list[np.ndarray] = []
    for lag in LAGS:
        # Match own_lag_features: lag 1 is the completed one-minute return
        # ending at the decision timestamp, never a future observation.
        value = latent.get(epoch - (lag - 1) * 60)
        if value is None or len(value) != len(currencies):
            return None
        vectors.append(value)
    return np.concatenate(vectors)


def build_pair_rows(
    pair: str,
    candles: dict[str, np.ndarray],
    currencies: list[str],
    latent: dict[int, np.ndarray],
    horizon: int,
) -> dict[str, np.ndarray]:
    n = len(candles["mid"])
    train_boundary = int(n * 0.60)
    dev_boundary = int(n * 0.80)
    pip = pip_size(pair)
    rows: dict[str, list[Any]] = {
        "baseline": [],
        "network": [],
        "target": [],
        "long_net": [],
        "short_net": [],
        "spread": [],
        "split": [],
    }
    start = max(LAGS) + 20
    for index in range(start, n - horizon, horizon):
        target_index = index + horizon
        if candles["epoch"][target_index] - candles["epoch"][index] > horizon * 60 + 120:
            continue
        if target_index < train_boundary:
            split = "train"
        elif index >= train_boundary and target_index < dev_boundary:
            split = "dev"
        elif index >= dev_boundary:
            split = "test"
        else:
            continue
        epoch = int(candles["epoch"][index])
        network = network_features(epoch, currencies, latent)
        if network is None:
            continue
        spread = float((candles["ask"][index] - candles["bid"][index]) / pip)
        if spread > 3.0:
            continue
        own = own_lag_features(candles["mid"], index, pip)
        target = float((candles["mid"][target_index] - candles["mid"][index]) / pip)
        rows["baseline"].append(np.concatenate((own, [spread])))
        rows["network"].append(network)
        rows["target"].append(target)
        rows["long_net"].append(
            float((candles["bid"][target_index] - candles["ask"][index]) / pip)
        )
        rows["short_net"].append(
            float((candles["bid"][index] - candles["ask"][target_index]) / pip)
        )
        rows["spread"].append(spread)
        rows["split"].append(split)
    return {key: np.asarray(value) for key, value in rows.items()}


def fitted_predictions(
    pair_rows: dict[str, dict[str, np.ndarray]], *, augmented: bool
) -> dict[str, np.ndarray]:
    combined: dict[str, list[np.ndarray]] = {
        "dev_prediction": [], "dev_target": [], "dev_long": [], "dev_short": [],
        "dev_spread": [], "dev_pair": [], "test_prediction": [], "test_target": [],
        "test_long": [], "test_short": [], "test_spread": [], "test_pair": [],
    }
    for pair, rows in pair_rows.items():
        split = rows["split"]
        masks = {name: split == name for name in ("train", "dev", "test")}
        if min(int(np.sum(mask)) for mask in masks.values()) < 20:
            continue
        features = rows["baseline"].astype(float)
        if augmented:
            features = np.column_stack((features, rows["network"].astype(float)))
        scaler = StandardScaler().fit(features[masks["train"]])
        model = Ridge(alpha=10.0).fit(
            scaler.transform(features[masks["train"]]),
            rows["target"][masks["train"]].astype(float),
        )
        for split_name in ("dev", "test"):
            mask = masks[split_name]
            prefix = split_name
            combined[f"{prefix}_prediction"].append(
                model.predict(scaler.transform(features[mask]))
            )
            combined[f"{prefix}_target"].append(rows["target"][mask].astype(float))
            combined[f"{prefix}_long"].append(rows["long_net"][mask].astype(float))
            combined[f"{prefix}_short"].append(rows["short_net"][mask].astype(float))
            combined[f"{prefix}_spread"].append(rows["spread"][mask].astype(float))
            combined[f"{prefix}_pair"].append(np.repeat(pair, int(np.sum(mask))))
    return {
        key: np.concatenate(value) if value else np.asarray([])
        for key, value in combined.items()
    }


def score_prediction_set(values: dict[str, np.ndarray]) -> dict[str, Any]:
    if len(values["test_prediction"]) < 30 or len(values["dev_prediction"]) < 30:
        return {"status": "insufficient_rows"}
    threshold = choose_threshold(
        values["dev_prediction"], values["dev_target"], values["dev_long"],
        values["dev_short"], values["dev_spread"], values["dev_pair"],
    )
    prediction = values["test_prediction"]
    target = values["test_target"]
    return {
        "status": "tested_shadow_only",
        "dev_rows": int(len(values["dev_prediction"])),
        "test_rows": int(len(prediction)),
        "selected_dev_absolute_prediction_threshold_pips": round(float(threshold), 6),
        "test_rmse_pips": round(float(np.sqrt(np.mean((prediction - target) ** 2))), 6),
        "test_all_direction_accuracy": round(float(np.mean(np.sign(prediction) * target > 0)), 6),
        "test_trading": metrics(
            prediction, target, values["test_long"], values["test_short"],
            values["test_spread"], values["test_pair"], threshold,
        ),
    }


def study(candle_sets: dict[str, dict[str, np.ndarray]]) -> dict[str, Any]:
    currencies, latent = latent_currency_returns(candle_sets)
    results: dict[str, Any] = {}
    for horizon in HORIZONS:
        pair_rows = {
            pair: build_pair_rows(pair, candles, currencies, latent, horizon)
            for pair, candles in candle_sets.items()
        }
        baseline = score_prediction_set(fitted_predictions(pair_rows, augmented=False))
        network = score_prediction_set(fitted_predictions(pair_rows, augmented=True))
        baseline_net = (baseline.get("test_trading") or {}).get("average_net_pips")
        network_net = (network.get("test_trading") or {}).get("average_net_pips")
        pairs_modeled = sum(
                min(int(np.sum(rows["split"] == part)) for part in ("train", "dev", "test")) >= 20
                for rows in pair_rows.values()
            )
        results[f"M{horizon}"] = {
            "status": "tested_shadow_only" if pairs_modeled else "insufficient_rows",
            "pairs_modeled": pairs_modeled,
            "own_lag_baseline": baseline,
            "baseline_plus_currency_network": network,
            "incremental_average_net_pips": round(float(network_net - baseline_net), 6)
            if baseline_net is not None and network_net is not None else None,
            "incremental_rmse_pips": round(
                float(baseline.get("test_rmse_pips", math.nan) - network.get("test_rmse_pips", math.nan)), 6
            ) if baseline.get("test_rmse_pips") is not None and network.get("test_rmse_pips") is not None else None,
        }
    return {
        "currencies": currencies,
        "synchronized_latent_minutes": len(latent),
        "horizons": results,
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Currency lead-lag network shadow audit", "",
        f"Generated: {report['generated_utc']}", "",
        "Research-only. No execution or order adapter exists.", "",
        "| Horizon | Own-lag avg | Network avg | Increment | Network CI90 | Decision |",
        "|---|---:|---:|---:|---|---|",
    ]
    for label, row in report["study"]["horizons"].items():
        base = row["own_lag_baseline"].get("test_trading") or {}
        network = row["baseline_plus_currency_network"].get("test_trading") or {}
        ci = network.get("pair_cluster_ci90")
        ci_text = f"[{ci[0]:.3f}, {ci[1]:.3f}]" if ci else "n/a"
        decision = "retain shadow" if ci and ci[0] > 0 and (row.get("incremental_average_net_pips") or 0) > 0 else "reject promotion"
        lines.append(
            f"| {label} | {base.get('average_net_pips')} | {network.get('average_net_pips')} | "
            f"{row.get('incremental_average_net_pips')} | {ci_text} | {decision} |"
        )
    lines.extend(["", "The middle chronological block selects one global magnitude threshold. The newest block is untouched. Every reported trade is in the <=3-pip entry-spread bucket, pays executable bid/ask costs, and must predict at least 1.5x its entry spread."])
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parent
    parser.add_argument("--creds", type=Path, default=root / "creds")
    parser.add_argument("--account-key", default="OANDA_ACCOUNT_ID_DUM4")
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--quotes", type=Path, default=root / "data/oanda_training_manager/state/practice_007_market_quotes_v1.json")
    parser.add_argument("--output", type=Path, default=root / "data/oanda_training_manager/reports/unique_predictor_watch_20260804/currency_lead_lag_network_v1.json")
    args = parser.parse_args()
    token, _ = read_credentials(args.creds, args.account_key, "")
    quote_payload = json.loads(args.quotes.read_text(encoding="utf-8"))
    quote_rows = quote_payload.get("quotes") or quote_payload.get("prices") or []
    instruments = sorted(quote_rows) if isinstance(quote_rows, dict) else sorted({str(row.get("instrument") or "") for row in quote_rows if isinstance(row, dict)})
    candle_sets: dict[str, dict[str, np.ndarray]] = {}
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fetch_candles, token, pair, args.count): pair for pair in instruments}
        for future in as_completed(futures):
            pair = futures[future]
            try:
                name, candles = future.result()
                if len(candles["mid"]) >= 500:
                    candle_sets[name] = candles
            except Exception as exc:
                errors.append(f"{pair}: {type(exc).__name__}: {exc}")
    report = {
        "schema_version": 1, "generated_utc": datetime.now(timezone.utc).isoformat(),
        "research_only": True, "can_place_orders": False,
        "evidence": "Basnarkov et al. (2019), arXiv:1906.10388; the paper finds most one-minute FX lead-lags absent and some statistically significant relationships, so this audit requires economic out-of-sample evidence rather than significance alone.",
        "data_contract": {
            "source": "OANDA practice GET M1 bid/ask candles", "usable_instruments": len(candle_sets),
            "chronology": "oldest 60% train / next 20% global threshold / newest 20% untouched",
            "network": "minute-by-minute least-squares latent currency returns using only <=3-pip inputs; lags 1,2,3,5,10",
            "comparison": "pair-specific own-lag Ridge versus same baseline plus lagged currency network",
            "costs": "actual entry/exit bid-ask; test entries <=3 pips; predicted magnitude >=1.5x spread",
        },
        "errors": errors, "study": study(candle_sets),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    args.output.with_suffix(".md").write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "usable_instruments": len(candle_sets), "errors": len(errors)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
