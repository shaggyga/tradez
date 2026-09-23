"""Second-stage continuation/exhaustion research for movement-gated spikes."""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .gate import VAULT_CAUSAL_FEATURES
from .pipeline import (
    _fit_and_apply_gate,
    _hash_payload,
    _write_csv,
    _write_json,
    _write_parquet,
    build_nested_splits,
    load_config,
    load_prepared_dataset,
)


CONTINUATION_CONTRACT_VERSION = "spike_continuation_exhaustion_v1"
CONTINUATION_CLASSES = ("continuation", "reversal", "no_trade")
SECOND_STAGE_FEATURES = (
    *VAULT_CAUSAL_FEATURES,
    "movement_score_rule",
    "movement_score",
    "movement_threshold",
    "atr_pips",
    "spread_pips",
    "expected_net_edge_pips",
)


@dataclass(frozen=True)
class ContinuationModel:
    estimator: Any
    feature_columns: tuple[str, ...]
    fill_values: dict[str, float]
    classes: tuple[str, ...]
    train_rows: int
    model_kind: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _profit_factor(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").fillna(0.0)
    wins = numeric[numeric > 0].sum()
    losses = numeric[numeric < 0].sum()
    if abs(float(losses)) > 0:
        return float(wins / abs(losses))
    return float("inf") if float(wins) > 0 else 0.0


def add_continuation_labels(
    frame: pd.DataFrame,
    *,
    minimum_net_pips: float = 0.0,
    advantage_pips: float = 0.0,
) -> pd.DataFrame:
    """Label gated rows as continuation, reversal, or no-trade.

    Continuation is defined relative to the causal trend proxy available at
    decision time.  The forward net-pip fields are retrospective labels only.
    """

    required = {
        "momentum_15_atr",
        "momentum_60_atr",
        "forward_long_net_pips",
        "forward_short_net_pips",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"continuation labels missing required columns: {missing}")

    output = frame.copy()
    momentum = pd.to_numeric(output["momentum_60_atr"], errors="coerce")
    fallback = pd.to_numeric(output["momentum_15_atr"], errors="coerce")
    momentum = momentum.where(momentum.ne(0) & momentum.notna(), fallback).fillna(0.0)
    output["trend_proxy_side"] = np.where(momentum >= 0.0, "long", "short")

    long_net = pd.to_numeric(output["forward_long_net_pips"], errors="coerce").fillna(0.0)
    short_net = pd.to_numeric(output["forward_short_net_pips"], errors="coerce").fillna(0.0)
    trend_is_long = output["trend_proxy_side"].eq("long")
    output["continuation_net_pips"] = np.where(trend_is_long, long_net, short_net)
    output["reversal_net_pips"] = np.where(trend_is_long, short_net, long_net)
    output["best_direction_net_pips"] = np.maximum(long_net, short_net)
    output["oracle_best_side"] = np.select(
        [long_net > short_net, short_net > long_net],
        ["long", "short"],
        default="flat",
    )

    min_net = float(minimum_net_pips)
    advantage = float(advantage_pips)
    continuation_ok = (
        output["continuation_net_pips"].ge(min_net)
        & output["continuation_net_pips"].ge(output["reversal_net_pips"] + advantage)
    )
    reversal_ok = (
        output["reversal_net_pips"].ge(min_net)
        & output["reversal_net_pips"].gt(output["continuation_net_pips"] + advantage)
    )
    output["continuation_label"] = np.select(
        [continuation_ok, reversal_ok],
        ["continuation", "reversal"],
        default="no_trade",
    )
    output["continuation_label_is_hindsight"] = True
    return output


def _prepare_features(frame: pd.DataFrame, fill_values: Mapping[str, float]) -> pd.DataFrame:
    data = frame[list(fill_values)].apply(pd.to_numeric, errors="coerce")
    return data.replace([np.inf, -np.inf], np.nan).fillna(fill_values)


def fit_continuation_model(
    train: pd.DataFrame,
    *,
    maximum_train_rows: int = 50000,
    random_state: int = 20260713,
    max_iter: int = 80,
) -> ContinuationModel:
    columns = tuple(column for column in SECOND_STAGE_FEATURES if column in train.columns)
    if not columns:
        raise ValueError("no continuation feature columns are available")
    ready = train.dropna(subset=["continuation_label"]).copy()
    if ready.empty:
        raise ValueError("cannot fit continuation model on empty training set")
    if len(ready) > int(maximum_train_rows):
        ready = ready.sample(
            n=int(maximum_train_rows),
            random_state=int(random_state),
            replace=False,
        ).sort_values(["timestamp", "instrument"], kind="stable")

    fill_values = {
        column: float(pd.to_numeric(ready[column], errors="coerce").median())
        for column in columns
    }
    fill_values = {key: (0.0 if math.isnan(value) else value) for key, value in fill_values.items()}
    x_train = _prepare_features(ready, fill_values)
    y_train = ready["continuation_label"].astype(str)

    if y_train.nunique() < 2 or len(ready) < 50:
        from sklearn.dummy import DummyClassifier

        estimator = DummyClassifier(strategy="most_frequent")
        model_kind = "dummy_most_frequent"
    else:
        from sklearn.ensemble import HistGradientBoostingClassifier

        estimator = HistGradientBoostingClassifier(
            max_iter=int(max_iter),
            learning_rate=0.05,
            l2_regularization=0.05,
            random_state=int(random_state),
        )
        model_kind = "hist_gradient_boosting_classifier"
    estimator.fit(x_train, y_train)
    return ContinuationModel(
        estimator=estimator,
        feature_columns=columns,
        fill_values=fill_values,
        classes=tuple(str(value) for value in estimator.classes_),
        train_rows=int(len(ready)),
        model_kind=model_kind,
    )


def score_continuation_model(frame: pd.DataFrame, model: ContinuationModel) -> pd.DataFrame:
    output = frame.copy()
    x_score = _prepare_features(output, model.fill_values)
    predicted = model.estimator.predict(x_score)
    output["continuation_prediction"] = predicted.astype(str)
    if hasattr(model.estimator, "predict_proba"):
        proba = model.estimator.predict_proba(x_score)
        for index, klass in enumerate(model.estimator.classes_):
            output[f"prob_{klass}"] = proba[:, index]
        output["prediction_confidence"] = proba.max(axis=1)
    else:
        output["prediction_confidence"] = 1.0

    output["model_trade_side"] = np.select(
        [
            output["continuation_prediction"].eq("continuation")
            & output["trend_proxy_side"].eq("long"),
            output["continuation_prediction"].eq("continuation")
            & output["trend_proxy_side"].eq("short"),
            output["continuation_prediction"].eq("reversal")
            & output["trend_proxy_side"].eq("long"),
            output["continuation_prediction"].eq("reversal")
            & output["trend_proxy_side"].eq("short"),
        ],
        ["long", "short", "short", "long"],
        default="flat",
    )
    output["model_net_pips"] = np.select(
        [
            output["continuation_prediction"].eq("continuation"),
            output["continuation_prediction"].eq("reversal"),
        ],
        [output["continuation_net_pips"], output["reversal_net_pips"]],
        default=0.0,
    )
    output["model_takes_trade"] = output["model_trade_side"].ne("flat")
    return output


def continuation_metrics(frame: pd.DataFrame, *, fold: str) -> dict[str, Any]:
    model_net = pd.to_numeric(frame["model_net_pips"], errors="coerce").fillna(0.0)
    trend_net = pd.to_numeric(frame["continuation_net_pips"], errors="coerce").fillna(0.0)
    oracle_net = pd.to_numeric(frame["best_direction_net_pips"], errors="coerce").clip(lower=0.0)
    traded = frame.loc[frame["model_takes_trade"]]
    return {
        "outer_fold": fold,
        "gated_rows": int(len(frame)),
        "model_trades": int(frame["model_takes_trade"].sum()),
        "model_trade_fraction": float(frame["model_takes_trade"].mean()) if len(frame) else 0.0,
        "model_net_pips": float(model_net.sum()),
        "model_mean_pips_per_gated_row": float(model_net.mean()) if len(model_net) else 0.0,
        "model_mean_pips_per_trade": (
            float(pd.to_numeric(traded["model_net_pips"], errors="coerce").mean())
            if len(traded)
            else 0.0
        ),
        "model_win_rate": (
            float(pd.to_numeric(traded["model_net_pips"], errors="coerce").gt(0).mean())
            if len(traded)
            else 0.0
        ),
        "model_profit_factor": _profit_factor(traded["model_net_pips"]) if len(traded) else 0.0,
        "trend_baseline_net_pips": float(trend_net.sum()),
        "trend_baseline_mean_pips": float(trend_net.mean()) if len(trend_net) else 0.0,
        "trend_baseline_profit_factor": _profit_factor(trend_net),
        "oracle_best_direction_net_pips": float(oracle_net.sum()),
        "oracle_best_direction_mean_pips": float(oracle_net.mean()) if len(oracle_net) else 0.0,
        "actual_continuation_fraction": float(frame["continuation_label"].eq("continuation").mean())
        if len(frame)
        else 0.0,
        "actual_reversal_fraction": float(frame["continuation_label"].eq("reversal").mean())
        if len(frame)
        else 0.0,
        "actual_no_trade_fraction": float(frame["continuation_label"].eq("no_trade").mean())
        if len(frame)
        else 0.0,
        "prediction_accuracy": float(
            frame["continuation_prediction"].eq(frame["continuation_label"]).mean()
        )
        if len(frame)
        else 0.0,
    }


def confidence_sweep(frame: pd.DataFrame, thresholds: Sequence[float] | None = None) -> pd.DataFrame:
    thresholds = thresholds or (0.0, 0.5, 0.6, 0.7, 0.8, 0.9)
    rows: list[dict[str, Any]] = []
    if frame.empty:
        return pd.DataFrame()
    for threshold in thresholds:
        subset = frame.loc[pd.to_numeric(frame["prediction_confidence"], errors="coerce").ge(threshold)]
        traded = subset.loc[subset["model_takes_trade"]]
        net = pd.to_numeric(traded["model_net_pips"], errors="coerce").fillna(0.0)
        rows.append(
            {
                "min_prediction_confidence": float(threshold),
                "rows": int(len(subset)),
                "trades": int(len(traded)),
                "net_pips": float(net.sum()),
                "mean_pips_per_trade": float(net.mean()) if len(net) else 0.0,
                "profit_factor": _profit_factor(net) if len(net) else 0.0,
                "win_rate": float(net.gt(0).mean()) if len(net) else 0.0,
            }
        )
    return pd.DataFrame(rows)


def _selected_gate_for_outer(output_root: Path, outer_fold: str) -> dict[str, Any]:
    path = output_root / "checkpoints" / "fit" / outer_fold / "outer_results.json"
    rows = json.loads(path.read_text(encoding="utf-8"))
    if not rows:
        raise ValueError(f"no selected gate rows found in {path}")
    return dict(rows[0]["selected_gate"])


def _artifact_fingerprint(config: Mapping[str, Any], dataset_manifest: Mapping[str, Any]) -> str:
    return _hash_payload(
        {
            "contract": CONTINUATION_CONTRACT_VERSION,
            "dataset_fingerprint": dataset_manifest.get("dataset_fingerprint"),
            "config_hash": _hash_payload(config),
        }
    )


def _report(
    metrics: pd.DataFrame,
    manifest: Mapping[str, Any],
    sweep: pd.DataFrame | None = None,
) -> str:
    if metrics.empty:
        total = {
            "gated_rows": 0,
            "model_trades": 0,
            "model_net_pips": 0.0,
            "trend_baseline_net_pips": 0.0,
            "oracle_best_direction_net_pips": 0.0,
        }
        metric_text = "(no evaluable walk-forward folds)"
    else:
        total = {
            "gated_rows": int(metrics["gated_rows"].sum()),
            "model_trades": int(metrics["model_trades"].sum()),
            "model_net_pips": float(metrics["model_net_pips"].sum()),
            "trend_baseline_net_pips": float(metrics["trend_baseline_net_pips"].sum()),
            "oracle_best_direction_net_pips": float(metrics["oracle_best_direction_net_pips"].sum()),
        }
        metric_text = metrics.to_string(index=False)
    lines = [
        "# Spike Continuation/Exhaustion Research",
        "",
        "Research-only retrospective diagnostic. Labels use future net pips and are not live signals.",
        "",
        f"- Contract: `{CONTINUATION_CONTRACT_VERSION}`",
        f"- Mode: `{manifest.get('mode')}`",
        f"- Dataset fingerprint: `{manifest.get('dataset_fingerprint')}`",
        f"- Gated evaluation rows: {total['gated_rows']:,}",
        f"- Model trades: {total['model_trades']:,}",
        f"- Model net pips: {total['model_net_pips']:.1f}",
        f"- Trend-follow baseline net pips: {total['trend_baseline_net_pips']:.1f}",
        f"- Oracle best-direction upper bound net pips: {total['oracle_best_direction_net_pips']:.1f}",
        "",
        "## Outer Fold Metrics",
        "",
        "```text",
        metric_text,
        "```",
        "",
        "## Confidence Sweep",
        "",
        "```text",
        "(not computed)" if sweep is None or sweep.empty else sweep.to_string(index=False),
        "```",
        "",
        "## Interpretation",
        "",
        "This tests whether the frozen movement gate contains enough causal information to choose continuation, reversal, or no-trade after a spike signal. It is still retrospective and should be shadow-logged prospectively before any account use.",
    ]
    return "\n".join(lines) + "\n"


def _merge_persisted_outer_opportunities(frame: pd.DataFrame, output_root: Path) -> pd.DataFrame:
    path = output_root / "oco" / "outer_test_outcomes.parquet"
    if not path.exists():
        raise FileNotFoundError(f"persisted outer OCO opportunities not found: {path}")
    outer = pd.read_parquet(path)
    outer_columns = [
        "outer_fold",
        "decision_id",
        "movement_score",
        "movement_threshold",
        "research_label_timestamp",
        "decision_timestamp",
        "triggered",
        "is_executable_candidate",
        "realized_pips",
        "gross_pips",
        "mfe_pips",
        "mae_pips",
        "exit_reason",
        "oco_config_id",
    ]
    outer_columns = [column for column in outer_columns if column in outer.columns]
    source_columns = [
        "decision_id",
        "timestamp",
        "instrument",
        *VAULT_CAUSAL_FEATURES,
        "movement_score_rule",
        "atr_pips",
        "spread_pips",
        "expected_net_edge_pips",
        "forward_signed_pips",
        "forward_abs_pips",
        "forward_long_net_pips",
        "forward_short_net_pips",
        "is_significant",
        "event_cluster_id",
        "currency_theme_cluster_id",
    ]
    source_columns = [column for column in source_columns if column in frame.columns]
    return outer[outer_columns].merge(
        frame[source_columns],
        on="decision_id",
        how="inner",
        validate="one_to_one",
    ).sort_values(["timestamp", "instrument"], kind="stable", ignore_index=True)


def _run_persisted_outer_continuation_research(
    frame: pd.DataFrame,
    config: Mapping[str, Any],
    paths_output_root: Path,
    output_root: Path,
    dataset_manifest: Mapping[str, Any],
    *,
    minimum_net_pips: float,
    advantage_pips: float,
    maximum_train_rows: int,
    max_iter: int,
) -> dict[str, Any]:
    opportunities = add_continuation_labels(
        _merge_persisted_outer_opportunities(frame, paths_output_root),
        minimum_net_pips=minimum_net_pips,
        advantage_pips=advantage_pips,
    )
    fold_names = sorted(opportunities["outer_fold"].dropna().astype(str).unique())
    predictions: list[pd.DataFrame] = []
    metrics: list[dict[str, Any]] = []
    model_ledgers: list[dict[str, Any]] = []
    for index, fold_name in enumerate(fold_names):
        if index == 0:
            continue
        train = opportunities.loc[opportunities["outer_fold"].isin(fold_names[:index])].copy()
        test = opportunities.loc[opportunities["outer_fold"].eq(fold_name)].copy()
        if train.empty or test.empty:
            continue
        model = fit_continuation_model(
            train,
            maximum_train_rows=maximum_train_rows,
            random_state=int(config["movement_gate"]["random_seed"]) + index,
            max_iter=max_iter,
        )
        scored = score_continuation_model(test, model)
        predictions.append(scored)
        metrics.append(continuation_metrics(scored, fold=fold_name))
        model_ledgers.append(
            {
                "outer_fold": fold_name,
                "train_outer_folds": fold_names[:index],
                "continuation_model_kind": model.model_kind,
                "continuation_model_classes": list(model.classes),
                "continuation_train_rows": model.train_rows,
                "continuation_feature_columns": list(model.feature_columns),
            }
        )

    prediction_frame = pd.concat(predictions, ignore_index=True) if predictions else pd.DataFrame()
    metric_frame = pd.DataFrame(metrics)
    sweep_frame = confidence_sweep(prediction_frame)
    manifest = {
        "contract": CONTINUATION_CONTRACT_VERSION,
        "mode": "persisted_outer_gate_walk_forward",
        "generated_utc": _utc_now(),
        "research_only": True,
        "api_or_live_state_touched": False,
        "dataset_fingerprint": dataset_manifest.get("dataset_fingerprint"),
        "artifact_fingerprint": _artifact_fingerprint(config, dataset_manifest),
        "persisted_outer_opportunity_rows": int(len(opportunities)),
        "evaluated_outer_folds": [row["outer_fold"] for row in metrics],
        "minimum_net_pips": float(minimum_net_pips),
        "advantage_pips": float(advantage_pips),
        "maximum_train_rows": int(maximum_train_rows),
        "max_iter": int(max_iter),
        "outer_models": model_ledgers,
        "warnings": [
            "This default mode reuses persisted out-of-sample movement-gate opportunities.",
            "The first outer opportunity fold is training-only for the second-stage model.",
            "Continuation labels are retrospective outcome labels.",
            "This is not live validation; prospective shadow logging is still required.",
        ],
    }
    _write_parquet(opportunities, output_root / "continuation_dataset.parquet")
    _write_parquet(prediction_frame, output_root / "outer_predictions.parquet")
    _write_csv(metric_frame, output_root / "outer_metrics.csv")
    _write_csv(sweep_frame, output_root / "confidence_sweep.csv")
    _write_json(output_root / "manifest.json", manifest)
    (output_root / "CONTINUATION_REPORT.md").write_text(
        _report(metric_frame, manifest, sweep_frame),
        encoding="utf-8",
    )
    return {"manifest": manifest, "metrics": metric_frame, "output_root": output_root}


def run_continuation_research(
    config_path: str | Path = "trad/config/spike_account_space.json",
    *,
    output_subdir: str = "continuation",
    minimum_net_pips: float = 0.0,
    advantage_pips: float = 0.0,
    maximum_train_rows: int = 50000,
    max_iter: int = 80,
    full_recompute_gate: bool = False,
) -> dict[str, Any]:
    config, paths = load_config(config_path)
    frame = load_prepared_dataset(paths)
    dataset_manifest_path = paths.output_root / "manifests" / "dataset_manifest.json"
    dataset_manifest = json.loads(dataset_manifest_path.read_text(encoding="utf-8"))
    output_root = paths.output_root / output_subdir
    output_root.mkdir(parents=True, exist_ok=True)
    if not bool(full_recompute_gate):
        return _run_persisted_outer_continuation_research(
            frame,
            config,
            paths.output_root,
            output_root,
            dataset_manifest,
            minimum_net_pips=minimum_net_pips,
            advantage_pips=advantage_pips,
            maximum_train_rows=maximum_train_rows,
            max_iter=max_iter,
        )

    folds = build_nested_splits(frame, config)
    all_rows: list[pd.DataFrame] = []
    predictions: list[pd.DataFrame] = []
    metrics: list[dict[str, Any]] = []
    model_ledgers: list[dict[str, Any]] = []
    for nested in folds:
        outer_name = nested.outer.name
        gate = _selected_gate_for_outer(paths.output_root, outer_name)
        train = nested.outer.take_train(frame)
        test = nested.outer.take_test(frame)
        train_scored, _ = _fit_and_apply_gate(
            train,
            train,
            model_kind=str(gate["model_kind"]),
            quantile=float(gate["quantile"]),
            top_n=int(gate["top_n"]),
            gate_config=config["movement_gate"],
        )
        test_scored, gate_manifest = _fit_and_apply_gate(
            train,
            test,
            model_kind=str(gate["model_kind"]),
            quantile=float(gate["quantile"]),
            top_n=int(gate["top_n"]),
            gate_config=config["movement_gate"],
        )
        train_gated = add_continuation_labels(
            train_scored.loc[train_scored["movement_gate_passed"]],
            minimum_net_pips=minimum_net_pips,
            advantage_pips=advantage_pips,
        )
        test_gated = add_continuation_labels(
            test_scored.loc[test_scored["movement_gate_passed"]],
            minimum_net_pips=minimum_net_pips,
            advantage_pips=advantage_pips,
        )
        train_gated["outer_fold"] = outer_name
        train_gated["split_role"] = "outer_train_gated"
        test_gated["outer_fold"] = outer_name
        test_gated["split_role"] = "outer_test_gated"
        model = fit_continuation_model(
            train_gated,
            maximum_train_rows=maximum_train_rows,
            random_state=int(config["movement_gate"]["random_seed"]),
            max_iter=max_iter,
        )
        scored = score_continuation_model(test_gated, model)
        metrics.append(continuation_metrics(scored, fold=outer_name))
        model_ledgers.append(
            {
                "outer_fold": outer_name,
                "selected_gate": gate,
                "gate_fit_manifest": gate_manifest,
                "continuation_model_kind": model.model_kind,
                "continuation_model_classes": list(model.classes),
                "continuation_train_rows": model.train_rows,
                "continuation_feature_columns": list(model.feature_columns),
            }
        )
        all_rows.extend([train_gated, test_gated])
        predictions.append(scored)

    dataset = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    prediction_frame = pd.concat(predictions, ignore_index=True) if predictions else pd.DataFrame()
    metric_frame = pd.DataFrame(metrics)
    sweep_frame = confidence_sweep(prediction_frame)
    manifest = {
        "contract": CONTINUATION_CONTRACT_VERSION,
        "mode": "full_gate_recompute",
        "generated_utc": _utc_now(),
        "research_only": True,
        "api_or_live_state_touched": False,
        "dataset_fingerprint": dataset_manifest.get("dataset_fingerprint"),
        "artifact_fingerprint": _artifact_fingerprint(config, dataset_manifest),
        "minimum_net_pips": float(minimum_net_pips),
        "advantage_pips": float(advantage_pips),
        "maximum_train_rows": int(maximum_train_rows),
        "max_iter": int(max_iter),
        "outer_models": model_ledgers,
        "warnings": [
            "Continuation labels are retrospective outcome labels.",
            "Event clusters remain hindsight metadata and are not model features.",
            "This is not live validation; prospective shadow logging is still required.",
        ],
    }
    _write_parquet(dataset, output_root / "continuation_dataset.parquet")
    _write_parquet(prediction_frame, output_root / "outer_predictions.parquet")
    _write_csv(metric_frame, output_root / "outer_metrics.csv")
    _write_csv(sweep_frame, output_root / "confidence_sweep.csv")
    _write_json(output_root / "manifest.json", manifest)
    (output_root / "CONTINUATION_REPORT.md").write_text(
        _report(metric_frame, manifest, sweep_frame),
        encoding="utf-8",
    )
    return {
        "manifest": manifest,
        "metrics": metric_frame,
        "output_root": output_root,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run second-stage spike continuation/exhaustion research."
    )
    parser.add_argument("--config", default="trad/config/spike_account_space.json")
    parser.add_argument("--output-subdir", default="continuation")
    parser.add_argument("--minimum-net-pips", type=float, default=0.0)
    parser.add_argument("--advantage-pips", type=float, default=0.0)
    parser.add_argument("--maximum-train-rows", type=int, default=50000)
    parser.add_argument("--max-iter", type=int, default=80)
    parser.add_argument(
        "--full-recompute-gate",
        action="store_true",
        help="Recompute full outer gate selections instead of reusing persisted outer opportunities.",
    )
    args = parser.parse_args(argv)
    result = run_continuation_research(
        args.config,
        output_subdir=args.output_subdir,
        minimum_net_pips=args.minimum_net_pips,
        advantage_pips=args.advantage_pips,
        maximum_train_rows=args.maximum_train_rows,
        max_iter=args.max_iter,
        full_recompute_gate=args.full_recompute_gate,
    )
    print(result["metrics"].to_string(index=False))
    print(f"wrote {result['output_root']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
