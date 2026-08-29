#!/usr/bin/env python3
"""Compare causal news state with price-only cost-clearance baselines.

All source values use source-governance ``effective_from`` knowledge time.
Source-only, price-only, combined, and timestamp-permuted-placebo arms share
the same chronological splits and executable bid/ask labels.  The archive is
already inspected, so this is incremental-value discovery, never proof.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import brier_score_loss, roc_auc_score

import oanda_cost_clearance_cross_sectional_research as price
import oanda_movement_news_episode_research as news


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "news_price_incremental_value_research_v1.json"
SOURCE = ROOT / "data" / "oanda_training_manager" / "candles"
SOURCE_EVENTS = ROOT / "data" / "oanda_training_manager" / "state" / "source_governance_v1.sqlite"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "news_price_incremental_value" / "NEWS_PRICE_INCREMENTAL_VALUE_20260809.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "news_price_incremental_value" / "NEWS_PRICE_INCREMENTAL_VALUE_20260809.md"

SOURCE_FEATURES = [
    "source_rows_1h", "source_rows_6h", "independent_stories_1h", "independent_stories_6h",
    "official_rows_6h", "directional_rows_6h", "policy_rows_6h", "macro_rows_6h",
    "media_rows_1h", "source_acceleration_1h", "seconds_since_latest_source",
]
PRICE_FEATURES = list(price.FEATURES)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def source_feature_row(
    source_index: Mapping[str, Any], base: str, quote: str, epoch: int,
) -> dict[str, float]:
    currencies = [base, quote]
    rows6 = news.relevant_source_events(source_index, currencies, epoch - 6 * 3600, epoch, True)
    rows1 = [row for row in rows6 if int(row["effective_epoch"]) >= epoch - 3600]
    prior1 = [row for row in rows6 if epoch - 7200 <= int(row["effective_epoch"]) < epoch - 3600]
    types = [str(row["event_type"]).lower() for row in rows6]
    populations = [str(row["source_population"]).lower() for row in rows6]
    latest = max((int(row["effective_epoch"]) for row in rows6), default=epoch - 21600)
    return {
        "source_rows_1h": float(len(rows1)),
        "source_rows_6h": float(len(rows6)),
        "independent_stories_1h": float(len({row["story_cluster_id"] or row["source_event_id"] for row in rows1})),
        "independent_stories_6h": float(len({row["story_cluster_id"] or row["source_event_id"] for row in rows6})),
        "official_rows_6h": float(sum(value.startswith("official") for value in populations)),
        "directional_rows_6h": float(sum(news._directional(row) for row in rows6)),
        "policy_rows_6h": float(sum("policy" in value or "central_bank" in value or "speech" in value for value in types)),
        "macro_rows_6h": float(sum(any(token in value for token in ("inflation", "employment", "gdp", "pmi", "macro")) for value in types)),
        "media_rows_1h": float(sum("media" in str(row["source_population"]).lower() or "public_market" in str(row["source_population"]).lower() for row in rows1)),
        "source_acceleration_1h": float(len(rows1) - len(prior1)),
        "seconds_since_latest_source": float(min(21600, max(0, epoch - latest))),
    }


def attach_source_features(frame: pd.DataFrame, source_index: Mapping[str, Any]) -> pd.DataFrame:
    rows = [
        source_feature_row(source_index, str(row.base), str(row.quote), int(row.epoch))
        for row in frame[["base", "quote", "epoch"]].itertuples(index=False)
    ]
    return pd.concat([frame.reset_index(drop=True), pd.DataFrame(rows)], axis=1)


def parameters(config: Mapping[str, Any]) -> dict[str, Any]:
    raw = config.get("model") or {}
    return {
        "learning_rate": float(raw.get("learning_rate", 0.05)),
        "max_iter": int(raw.get("max_iter", 80)),
        "max_leaf_nodes": int(raw.get("max_leaf_nodes", 15)),
        "min_samples_leaf": int(raw.get("min_samples_leaf", 40)),
        "l2_regularization": float(raw.get("l2_regularization", 1.0)),
        "random_state": int(raw.get("random_state", 20260809)),
    }


def score_arm(train: pd.DataFrame, valid: pd.DataFrame, features: list[str], config: Mapping[str, Any]) -> dict[str, Any]:
    model = HistGradientBoostingClassifier(**parameters(config)).fit(train[features], train["cost_clear"])
    probability = model.predict_proba(valid[features])[:, 1]
    return {
        "feature_count": len(features),
        "roc_auc": float(roc_auc_score(valid["cost_clear"], probability)) if valid["cost_clear"].nunique() > 1 else None,
        "brier": float(brier_score_loss(valid["cost_clear"], probability)),
        "mean_probability": float(np.mean(probability)),
        "proof_eligible": False,
    }


def permute_source(frame: pd.DataFrame, seed: int) -> pd.DataFrame:
    result = frame.copy()
    rng = np.random.default_rng(seed)
    permutation = rng.permutation(len(result))
    result.loc[:, SOURCE_FEATURES] = result[SOURCE_FEATURES].to_numpy()[permutation]
    return result


def fit_horizon(frame: pd.DataFrame, horizon: int, config: Mapping[str, Any]) -> dict[str, Any]:
    frame = frame[
        frame["epoch"].mod(horizon * 60).eq(0)
        & frame["spread_pips"].le(float(config.get("maximum_spread_pips", 2.0)))
    ].replace([np.inf, -np.inf], np.nan).dropna(subset=PRICE_FEATURES + SOURCE_FEATURES + ["cost_clear"])
    epochs = np.asarray(sorted(frame["epoch"].unique()))
    split1 = epochs[max(1, int(len(epochs) * float(config.get("train_fraction", 0.5)))) - 1]
    split2 = epochs[max(2, int(len(epochs) * (float(config.get("train_fraction", 0.5)) + float(config.get("calibration_fraction", 0.25))))) - 1]
    purge = horizon * 60
    train = frame[frame["epoch"] < split1 - purge].copy()
    calibration = frame[(frame["epoch"] >= split1) & (frame["epoch"] < split2 - purge)].copy()
    holdout = frame[frame["epoch"] >= split2].copy()
    # Calibration is retained as a separate untouched threshold-selection block;
    # model fitting is deliberately limited to the training segment here.
    placebo_train = permute_source(train, 20260809 + horizon)
    placebo_holdout = permute_source(holdout, 20260819 + horizon)
    arms = {
        "source_only": score_arm(train, holdout, SOURCE_FEATURES, config),
        "price_only": score_arm(train, holdout, PRICE_FEATURES, config),
        "price_plus_source": score_arm(train, holdout, PRICE_FEATURES + SOURCE_FEATURES, config),
        "price_plus_permuted_source": score_arm(placebo_train, placebo_holdout, PRICE_FEATURES + SOURCE_FEATURES, config),
    }
    combined = arms["price_plus_source"]["roc_auc"]
    price_auc = arms["price_only"]["roc_auc"]
    placebo_auc = arms["price_plus_permuted_source"]["roc_auc"]
    return {
        "horizon_min": horizon,
        "rows": {"train": len(train), "calibration": len(calibration), "holdout": len(holdout)},
        "epochs": {"train": int(train["epoch"].nunique()), "calibration": int(calibration["epoch"].nunique()), "holdout": int(holdout["epoch"].nunique())},
        "holdout_days": int(pd.to_datetime(holdout["epoch"], unit="s", utc=True).dt.date.nunique()),
        "holdout_clear_rate": float(holdout["cost_clear"].mean()),
        "arms": arms,
        "combined_minus_price_auc": None if combined is None or price_auc is None else float(combined - price_auc),
        "combined_minus_placebo_auc": None if combined is None or placebo_auc is None else float(combined - placebo_auc),
        "incremental_source_candidate": False,
        "incremental_source_candidate_reasons": [
            "archive_already_inspected", "no_multiple_testing_adjustment", "no_untouched_confirmation",
            "prediction_metric_does_not_establish_after_cost_allocator_value",
        ],
        "proof_eligible": False,
    }


def run(
    config_path: Path = CONFIG, source_dir: Path = SOURCE, source_events: Path = SOURCE_EVENTS,
    output: Path = OUTPUT, report: Path = REPORT,
) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    paths = sorted(source_dir.glob("*_M1.csv"))
    frames = [price.load_instrument(path) for path in paths]
    panel = price.add_cross_sectional_features(pd.concat(frames, ignore_index=True))
    panel = panel[panel["pair_coverage"] >= int(config.get("minimum_pairs_per_timestamp", 8))]
    slippage = float(config.get("modeled_slippage_pips", 0.25))
    panel["entry_cost_pips"] = panel["spread_pips"] + slippage
    panel["momentum_cost_ratio"] = (
        0.65 * panel["return_5m_pips"].abs() + 0.35 * panel["return_15m_pips"].abs()
    ) / panel["entry_cost_pips"].replace(0, np.nan)
    panel["movement_rank"] = panel.groupby("epoch")["momentum_cost_ratio"].rank(pct=True, ascending=True)
    source_index, source_highwater = news.load_source_index(source_events)
    source_start_epoch = min(
        int(value["epochs"][0]) for value in source_index.values()
        if isinstance(value, Mapping) and value.get("epochs")
    )
    panel = panel[panel["epoch"] >= source_start_epoch]
    results = []
    for horizon in config.get("horizons_min") or []:
        labeled = pd.concat([
            price.attach_outcomes(group.sort_values("epoch").reset_index(drop=True), int(horizon), slippage)
            for _, group in panel.groupby("instrument")
        ], ignore_index=True)
        aligned = labeled[
            labeled["epoch"].mod(int(horizon) * 60).eq(0)
            & labeled["spread_pips"].le(float(config.get("maximum_spread_pips", 2.0)))
        ].copy()
        mapped = attach_source_features(aligned, source_index)
        results.append(fit_horizon(mapped, int(horizon), config))
    generated = dt.datetime.now(dt.timezone.utc).isoformat()
    payload = {
        "schema_version": 1, "generated_utc": generated, "research_id": config["research_id"],
        "research_only": True, "execution_eligible": False, "can_place_orders": False,
        "evidence_class": "causal_knowledge_time_archive_incremental_value_discovery",
        "instrument_count": len(paths), "source_event_highwater_utc": source_highwater,
        "source_event_coverage_start_utc": dt.datetime.fromtimestamp(source_start_epoch, dt.timezone.utc).isoformat(),
        "source_features": SOURCE_FEATURES, "price_features": PRICE_FEATURES,
        "config_sha256": price.digest(config_path), "source_code_sha256": price.digest(Path(__file__)),
        "results": results,
    }
    atomic_text(output, json.dumps(payload, indent=2, sort_keys=True))
    lines = [
        "# News/price incremental-value research", "",
        "Causal source knowledge is compared with price/cost features and a timestamp-permuted placebo. Archive discovery only.", "",
        "| Horizon | Holdout rows / days | Clear rate | Source AUC | Price AUC | Combined AUC | Permuted AUC | Combined - price | Combined - placebo |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for result in results:
        arms = result["arms"]
        lines.append(
            f"| {result['horizon_min']}m | {result['rows']['holdout']:,} / {result['holdout_days']} | {result['holdout_clear_rate']:.1%} | "
            f"{arms['source_only']['roc_auc']:.3f} | {arms['price_only']['roc_auc']:.3f} | "
            f"{arms['price_plus_source']['roc_auc']:.3f} | {arms['price_plus_permuted_source']['roc_auc']:.3f} | "
            f"{result['combined_minus_price_auc']:+.3f} | {result['combined_minus_placebo_auc']:+.3f} |"
        )
    lines += [
        "", "No arm can promote from this archive. A source is useful only if incremental prediction later becomes incremental after-cost allocator value in an untouched cohort.", "",
    ]
    atomic_text(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--source-dir", type=Path, default=SOURCE)
    parser.add_argument("--source-events", type=Path, default=SOURCE_EVENTS)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    run(args.config, args.source_dir, args.source_events, args.output, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
