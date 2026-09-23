"""Resumable, offline movement-gated OCO and account-space research pipeline."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import platform
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .assumptions import attach_timestamped_economics, build_project_metadata
from .dataset import build_forward_labels, iter_normalized_bars, normalize_instrument
from .events import assign_event_clusters
from .gate import (
    VAULT_CAUSAL_FEATURES,
    apply_cross_sectional_top_n,
    audit_vault_columns,
    fit_movement_classifier,
    iter_vault_features,
    score_movement_classifier,
)
from .oco import evaluate_oco_candidates
from .portfolio import no_trade_baseline, replay_account
from .schemas import AccountConfig, BufferConfig, ExitConfig, OCOConfig, ObjectiveConfig
from .search import objective_for_summary, search_account_space, stable_fold_objective
from .splits import NestedPurgedFold, make_nested_purged_splits
from .thresholds import apply_train_only_thresholds, fit_train_only_thresholds


DATASET_CONTRACT_VERSION = "spike_account_space_dataset_v2"
FIT_CONTRACT_VERSION = "spike_account_space_fit_v2"


@dataclass(frozen=True)
class ResearchPaths:
    project_root: Path
    trad_root: Path
    bar_cache_root: Path
    vault_feature_root: Path
    instrument_inventory: Path
    output_root: Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_default(value: Any) -> Any:
    if isinstance(value, (Path, pd.Timestamp, pd.Timedelta, datetime)):
        return str(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    raise TypeError(type(value).__name__)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=_json_default),
        encoding="utf-8",
    )
    temporary.replace(path)


def _write_parquet(
    frame: pd.DataFrame,
    path: Path,
    *,
    compression: str = "zstd",
) -> None:
    """Atomically replace a parquet artifact."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp.parquet")
    frame.to_parquet(temporary, compression=compression, index=False)
    temporary.replace(path)


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    """Atomically replace a CSV artifact."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temporary, index=False)
    temporary.replace(path)


def _write_optional_parquet(frame: pd.DataFrame, path: Path) -> None:
    """Write non-empty output or remove a stale artifact from an older run."""

    if frame.empty:
        path.unlink(missing_ok=True)
        return
    _write_parquet(frame, path)


def _hash_payload(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=_json_default).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _source_signature(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _positions_digest(values: Sequence[int] | np.ndarray) -> str:
    array = np.asarray(values, dtype=np.int64)
    return hashlib.sha256(array.tobytes(order="C")).hexdigest()


def _fold_contract_payload(nested: NestedPurgedFold) -> dict[str, Any]:
    def one(fold: Any) -> dict[str, Any]:
        return {
            "name": fold.name,
            "train_positions_sha256": _positions_digest(fold.train_positions),
            "test_positions_sha256": _positions_digest(fold.test_positions),
            "omitted_positions_sha256": _positions_digest(fold.omitted_positions),
            "test_start": fold.test_start,
            "test_end_exclusive": fold.test_end_exclusive,
            "purge": fold.purge,
            "embargo": fold.embargo,
        }

    return {
        "outer": one(nested.outer),
        "inner": [one(fold) for fold in nested.inner],
    }


def load_config(path: str | Path) -> tuple[dict[str, Any], ResearchPaths]:
    config_path = Path(path).resolve()
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if not bool(config.get("research_only")) or bool(config.get("allow_live_or_practice_api")):
        raise ValueError("this pipeline requires research_only=true and API access disabled")
    trad_root = config_path.parents[1]
    project_root = trad_root.parent
    raw = config["paths"]

    def resolve(value: str) -> Path:
        candidate = Path(value)
        return candidate.resolve() if candidate.is_absolute() else (trad_root / candidate).resolve()

    paths = ResearchPaths(
        project_root=project_root,
        trad_root=trad_root,
        bar_cache_root=resolve(raw["bar_cache_root"]),
        vault_feature_root=resolve(raw["vault_feature_root"]),
        instrument_inventory=resolve(raw["instrument_inventory"]),
        output_root=resolve(raw["output_root"]),
    )
    return config, paths


def _ensure_output_layout(root: Path) -> None:
    for name in (
        "manifests",
        "checkpoints/pairs",
        "features",
        "labels",
        "clusters",
        "folds",
        "oco",
        "account",
        "reports",
        "models",
    ):
        (root / name).mkdir(parents=True, exist_ok=True)


def discover_instruments(
    paths: ResearchPaths, requested: Sequence[str] | None = None
) -> tuple[list[str], pd.DataFrame]:
    inventory = pd.read_csv(paths.instrument_inventory)
    inventory["instrument"] = inventory["instrument"].map(normalize_instrument)
    usable = inventory.loc[inventory["status"].eq("usable"), "instrument"].tolist()
    bar_names = {path.stem.upper() for path in paths.bar_cache_root.glob("*.parquet")}
    feature_names = {path.stem.upper() for path in paths.vault_feature_root.glob("*.parquet")}
    valid = sorted(set(usable).intersection(bar_names, feature_names))
    if requested:
        wanted = {normalize_instrument(value) for value in requested}
        missing = sorted(wanted.difference(valid))
        if missing:
            raise FileNotFoundError(f"requested instruments unavailable: {missing}")
        valid = sorted(wanted)
    audit = inventory.copy()
    audit["has_bar_cache"] = audit["instrument"].isin(bar_names)
    audit["has_vault_features"] = audit["instrument"].isin(feature_names)
    audit["selected_for_account_space"] = audit["instrument"].isin(valid)
    audit["account_space_skip_reason"] = np.select(
        [
            ~audit["status"].eq("usable"),
            ~audit["has_bar_cache"],
            ~audit["has_vault_features"],
            ~audit["selected_for_account_space"],
        ],
        [
            audit.get("skip_reason", "source_inventory_not_usable"),
            "missing_regular_bar_cache",
            "missing_vault_feature_cache",
            "not_in_requested_subset",
        ],
        default="",
    )
    return valid, audit


def _pair_checkpoint_fingerprint(
    instrument: str,
    config: Mapping[str, Any],
    bar_path: Path,
    feature_path: Path,
) -> tuple[str, dict[str, Any]]:
    payload = {
        "dataset_contract_version": DATASET_CONTRACT_VERSION,
        "schema_version": config["schema_version"],
        "instrument": instrument,
        "data": config["data"],
        "bar_source": _source_signature(bar_path),
        "feature_source": _source_signature(feature_path),
        "causal_features": list(VAULT_CAUSAL_FEATURES),
        "canonical_label_builder": "exact_timestamp_join",
    }
    return _hash_payload(payload), payload


def _prepare_pair(
    instrument: str,
    config: Mapping[str, Any],
    paths: ResearchPaths,
    *,
    force: bool,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    bar_path = paths.bar_cache_root / f"{instrument}.parquet"
    feature_path = paths.vault_feature_root / f"{instrument}.parquet"
    checkpoint = paths.output_root / "checkpoints" / "pairs" / f"{instrument}.parquet"
    manifest_path = checkpoint.with_suffix(".json")
    fingerprint, inputs = _pair_checkpoint_fingerprint(
        instrument, config, bar_path, feature_path
    )
    if not force and checkpoint.exists() and manifest_path.exists():
        prior = json.loads(manifest_path.read_text(encoding="utf-8"))
        if prior.get("fingerprint") == fingerprint:
            return pd.read_parquet(checkpoint), prior

    data_config = config["data"]
    feature = dict(
        iter_vault_features(
            paths.vault_feature_root,
            instruments=[instrument],
            start=data_config.get("start_utc"),
            end=data_config.get("end_utc"),
            decision_offset_minutes=int(
                data_config["vault_left_label_decision_offset_minutes"]
            ),
            decision_stride_minutes=int(data_config["decision_stride_minutes"]),
            expected_horizon_minutes=int(data_config["horizon_minutes"]),
            assumed_round_trip_slippage_pips=float(
                data_config["assumed_round_trip_slippage_pips"]
            ),
        )
    )[instrument]
    if feature.empty:
        raise ValueError(f"no feature rows in configured range for {instrument}")
    start = feature["timestamp"].min()
    horizon = int(data_config["horizon_minutes"])
    end = feature["timestamp"].max() + pd.Timedelta(minutes=horizon)
    bars = dict(
        iter_normalized_bars(
            paths.bar_cache_root,
            instruments=[instrument],
            start=start,
            end=end,
            require_complete_grid=True,
            drop_empty_bars=False,
        )
    )[instrument]
    labels = build_forward_labels(
        bars,
        horizon_minutes=horizon,
        minimum_observation_fraction=float(
            data_config["minimum_forward_observation_fraction"]
        ),
        drop_invalid=False,
        require_complete_grid=True,
    )
    merged = feature.merge(
        labels,
        on=["timestamp", "instrument"],
        how="left",
        validate="one_to_one",
        suffixes=("", "_label"),
    )
    merged["feature_ready"] = merged["feature_ready"].fillna(False).astype(bool)
    merged["label_is_valid"] = merged["label_is_valid"].fillna(False).astype(bool)
    merged["research_row_valid"] = merged["feature_ready"] & merged["label_is_valid"]
    merged = merged.sort_values("timestamp", kind="stable").reset_index(drop=True)
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    temporary = checkpoint.with_suffix(".tmp.parquet")
    merged.to_parquet(temporary, compression="zstd", index=False)
    temporary.replace(checkpoint)
    manifest = {
        "instrument": instrument,
        "fingerprint": fingerprint,
        "inputs": inputs,
        "generated_utc": _utc_now(),
        "row_count": int(len(merged)),
        "valid_row_count": int(merged["research_row_valid"].sum()),
        "start_utc": merged["timestamp"].min(),
        "end_utc": merged["timestamp"].max(),
        "exact_label_end_max_utc": merged["outcome_timestamp"].max(),
        "feature_source_left_label_shift_minutes": int(
            data_config["vault_left_label_decision_offset_minutes"]
        ),
        "hindsight_columns_are_labels_only": True,
    }
    _write_json(manifest_path, manifest)
    return merged, manifest


def _causal_theme(instrument: str) -> str:
    currencies = set(str(instrument).split("_", 1))
    for currency in ("USD", "JPY", "EUR", "GBP", "CHF", "AUD", "CAD", "NZD"):
        if currency in currencies:
            return f"instrument_composition:{currency}"
    return f"instrument_composition:{str(instrument).split('_', 1)[0]}"


def _assign_labels_and_clusters(
    frame: pd.DataFrame, config: Mapping[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    valid = frame.loc[frame["research_row_valid"]].copy()
    unique_time = pd.DatetimeIndex(valid["timestamp"].unique()).sort_values()
    fraction = float(config["event_definition"]["fixed_train_fraction"])
    cutoff_position = min(len(unique_time) - 1, max(1, int(len(unique_time) * fraction)))
    cutoff = pd.Timestamp(unique_time[cutoff_position])
    threshold_model = fit_train_only_thresholds(
        valid.loc[valid["timestamp"] < cutoff],
        value_col="forward_abs_pips",
        group_col="instrument",
        quantile=float(config["event_definition"]["instrument_quantile"]),
        minimum_group_samples=int(
            config["event_definition"]["minimum_instrument_samples"]
        ),
        valid_col="label_is_valid",
        timestamp_col="timestamp",
    )
    output = apply_train_only_thresholds(
        frame,
        threshold_model,
        threshold_col="event_magnitude_threshold_pips",
        output_col="is_significant",
    )
    candidates = output.loc[output["is_significant"]].copy()
    clustered = assign_event_clusters(
        candidates,
        candidate_col="is_significant",
        score_col="forward_abs_pips",
        within_pair_start_tolerance=config["event_definition"][
            "within_pair_start_tolerance"
        ],
        cross_pair_start_tolerance=config["event_definition"][
            "cross_pair_start_tolerance"
        ],
        minimum_interval_overlap_fraction=float(
            config["event_definition"]["minimum_interval_overlap_fraction"]
        ),
    )
    cluster_columns = [
        "decision_id",
        "overlap_group_id",
        "event_cluster_id",
        "event_anchor_timestamp",
        "event_pair_count",
        "event_candidate_count",
        "is_cross_pair_shock",
        "event_cluster_is_hindsight_metadata",
    ]
    output = output.drop(
        columns=[column for column in cluster_columns[1:] if column in output],
        errors="ignore",
    ).merge(clustered[cluster_columns], on="decision_id", how="left", validate="one_to_one")
    output["event_cluster_is_hindsight_metadata"] = True
    output["currency_theme_cluster_id"] = output["instrument"].map(_causal_theme)
    output["currency_theme_is_causal"] = True
    manifest = {
        "event_threshold_fit_scope": "initial_chronological_train_only",
        "event_threshold_train_cutoff_utc": cutoff,
        "event_threshold_model": asdict(threshold_model),
        "significant_candidate_count": int(output["is_significant"].sum()),
        "event_cluster_count": int(output["event_cluster_id"].nunique(dropna=True)),
        "cross_pair_cluster_count": int(
            output.loc[output["is_cross_pair_shock"].fillna(False), "event_cluster_id"].nunique()
        ),
        "event_cluster_prohibited_as_model_feature": True,
    }
    return output, manifest


def _fold_summary(folds: Sequence[NestedPurgedFold]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for nested in folds:
        outer = nested.outer
        rows.append(
            {
                "level": "outer",
                "parent": "",
                "fold": outer.name,
                "train_rows": len(outer.train_positions),
                "test_rows": len(outer.test_positions),
                "omitted_rows": len(outer.omitted_positions),
                "test_start": outer.test_start,
                "test_end_exclusive": outer.test_end_exclusive,
                "purge": outer.purge,
                "embargo": outer.embargo,
            }
        )
        for inner in nested.inner:
            rows.append(
                {
                    "level": "inner",
                    "parent": outer.name,
                    "fold": inner.name,
                    "train_rows": len(inner.train_positions),
                    "test_rows": len(inner.test_positions),
                    "omitted_rows": len(inner.omitted_positions),
                    "test_start": inner.test_start,
                    "test_end_exclusive": inner.test_end_exclusive,
                    "purge": inner.purge,
                    "embargo": inner.embargo,
                }
            )
    return pd.DataFrame(rows)


def build_nested_splits(frame: pd.DataFrame, config: Mapping[str, Any]) -> tuple[NestedPurgedFold, ...]:
    validation = config["validation"]
    return make_nested_purged_splits(
        frame,
        n_outer_splits=int(validation["outer_splits"]),
        n_inner_splits=int(validation["inner_splits"]),
        outer_initial_train_fraction=float(validation["outer_initial_train_fraction"]),
        inner_initial_train_fraction=float(validation["inner_initial_train_fraction"]),
        purge=validation["purge"],
        embargo=validation["embargo"],
        time_col="timestamp",
        label_end_col="outcome_timestamp",
        group_col="event_cluster_id",
        decision_id_col="decision_id",
    )


def prepare_research_dataset(
    config: Mapping[str, Any],
    paths: ResearchPaths,
    *,
    instruments: Sequence[str] | None = None,
    force: bool = False,
) -> tuple[pd.DataFrame, tuple[NestedPurgedFold, ...], dict[str, Any]]:
    _ensure_output_layout(paths.output_root)
    selected, inventory = discover_instruments(paths, instruments)
    _write_csv(inventory, paths.output_root / "instrument_inventory.csv")
    frames: list[pd.DataFrame] = []
    pair_manifests: list[dict[str, Any]] = []
    for index, instrument in enumerate(selected, start=1):
        print(f"prepare {index}/{len(selected)} {instrument}", flush=True)
        pair, manifest = _prepare_pair(instrument, config, paths, force=force)
        frames.append(pair)
        pair_manifests.append(manifest)
    combined = pd.concat(frames, ignore_index=True).sort_values(
        ["timestamp", "instrument"], kind="stable", ignore_index=True
    )
    combined, cluster_manifest = _assign_labels_and_clusters(combined, config)
    valid = combined.loc[combined["research_row_valid"]].reset_index(drop=True)
    folds = build_nested_splits(valid, config)

    feature_columns = [
        "decision_id",
        "timestamp",
        "instrument",
        *VAULT_CAUSAL_FEATURES,
        "movement_score_rule",
        "atr_pips",
        "spread_pips",
        "expected_net_edge_pips",
        "feature_ready",
        "vault_source_timestamp",
        "vault_decision_offset_minutes",
        "feature_schema",
        "features_are_causal",
    ]
    label_columns = [
        "decision_id",
        "timestamp",
        "outcome_timestamp",
        "instrument",
        "forward_signed_pips",
        "forward_abs_pips",
        "forward_signed_return",
        "forward_abs_return",
        "forward_long_net_pips",
        "forward_short_net_pips",
        "forward_observation_fraction",
        "label_is_valid",
        "label_quality_flags",
        "event_magnitude_threshold_pips",
        "is_significant",
        "overlap_group_id",
        "event_cluster_id",
        "event_anchor_timestamp",
        "event_pair_count",
        "event_candidate_count",
        "is_cross_pair_shock",
        "event_cluster_is_hindsight_metadata",
        "currency_theme_cluster_id",
        "currency_theme_is_causal",
    ]
    feature_path = paths.output_root / "features" / "decision_features.parquet"
    label_path = paths.output_root / "labels" / "exact_120m_labels.parquet"
    cluster_path = paths.output_root / "clusters" / "event_cluster_candidates.parquet"
    fold_path = paths.output_root / "folds" / "fold_summary.csv"
    _write_parquet(valid[feature_columns], feature_path)
    _write_parquet(valid[label_columns], label_path)
    candidates = valid.loc[valid["is_significant"], label_columns]
    _write_parquet(candidates, cluster_path)
    fold_table = _fold_summary(folds)
    _write_csv(fold_table, fold_path)

    # Cluster extraction happens before the final feature/economics readiness
    # filter.  Preserve both scopes, but make the headline counts describe the
    # rows actually persisted for fitting.
    cluster_manifest = dict(cluster_manifest)
    cluster_manifest["pre_research_filter_significant_candidate_count"] = int(
        cluster_manifest["significant_candidate_count"]
    )
    cluster_manifest["pre_research_filter_event_cluster_count"] = int(
        cluster_manifest["event_cluster_count"]
    )
    cluster_manifest["pre_research_filter_cross_pair_cluster_count"] = int(
        cluster_manifest["cross_pair_cluster_count"]
    )
    cluster_manifest["significant_candidate_count"] = int(len(candidates))
    cluster_manifest["event_cluster_count"] = int(
        candidates["event_cluster_id"].nunique(dropna=True)
    )
    cluster_manifest["cross_pair_cluster_count"] = int(
        candidates.loc[
            candidates["is_cross_pair_shock"].fillna(False), "event_cluster_id"
        ].nunique(dropna=True)
    )
    cluster_manifest["headline_count_scope"] = "persisted_research_rows"

    import pyarrow.parquet as pq

    first_schema = pq.ParquetFile(paths.vault_feature_root / f"{selected[0]}.parquet").schema.names
    _write_csv(
        audit_vault_columns(first_schema),
        paths.output_root / "features" / "vault_feature_audit.csv",
    )
    data_end = pd.Timestamp(valid["timestamp"].max())
    config_hash = _hash_payload(config)
    dataset_fingerprint = _hash_payload(
        {
            "dataset_contract_version": DATASET_CONTRACT_VERSION,
            "schema_version": config["schema_version"],
            "config_hash": config_hash,
            "instruments": selected,
            "pair_checkpoint_fingerprints": [
                item.get("fingerprint", "") for item in pair_manifests
            ],
            "decision_row_count": len(valid),
            "data_start_utc": valid["timestamp"].min(),
            "data_end_utc": data_end,
            "cluster_counts": {
                key: cluster_manifest[key]
                for key in (
                    "significant_candidate_count",
                    "event_cluster_count",
                    "cross_pair_cluster_count",
                )
            },
            "folds": [_fold_contract_payload(fold) for fold in folds],
        }
    )
    manifest = {
        "dataset_contract_version": DATASET_CONTRACT_VERSION,
        "dataset_fingerprint": dataset_fingerprint,
        "schema_version": config["schema_version"],
        "config_hash": config_hash,
        "generated_utc": _utc_now(),
        "research_only": True,
        "api_or_live_state_touched": False,
        "instrument_count": len(selected),
        "instruments": selected,
        "decision_row_count": len(valid),
        "data_start_utc": valid["timestamp"].min(),
        "data_end_utc": data_end,
        "source_data_age_days_at_run": (
            pd.Timestamp.now(tz="UTC") - data_end
        ).total_seconds()
        / 86400.0,
        "fresh_holdout_available": False,
        "forward_shadow_start_utc": data_end + pd.Timedelta(minutes=5),
        "validation_interpretation": "retrospective_nested_diagnostic_not_live_validation",
        "canonical_label_contract": "exact UTC timestamp join; no row-offset horizon",
        "vault_feature_contract": "explicit causal allow-list; left-labelled source shifted to knowable time",
        "event_cluster_contract": "hindsight metadata for grouping/splits only",
        "pair_checkpoints": pair_manifests,
        "clusters": cluster_manifest,
        "folds": fold_table.to_dict("records"),
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "pandas": pd.__version__,
            "numpy": np.__version__,
        },
        "artifact_signatures": {
            "decision_features": _source_signature(feature_path),
            "exact_labels": _source_signature(label_path),
            "event_clusters": _source_signature(cluster_path),
            "fold_summary": _source_signature(fold_path),
        },
        "warnings": [
            "No fresh untouched holdout remains after this retrospective audit.",
            "Most historical spreads are estimates; outputs are not deployment evidence.",
            "Financing is unavailable locally and remains an explicit zero/stress assumption.",
            "Margin rates are research assumptions unless a captured account instrument snapshot is supplied.",
        ],
    }
    _write_json(paths.output_root / "manifests" / "dataset_manifest.json", manifest)
    _write_json(
        paths.output_root / "manifests" / "fit_state.json",
        {
            "status": "STALE_PENDING_REFIT",
            "reason": "prepared dataset was regenerated",
            "dataset_fingerprint": dataset_fingerprint,
            "updated_utc": _utc_now(),
            "research_only": True,
        },
    )
    return valid, folds, manifest


def load_prepared_dataset(paths: ResearchPaths) -> pd.DataFrame:
    features = pd.read_parquet(paths.output_root / "features" / "decision_features.parquet")
    labels = pd.read_parquet(paths.output_root / "labels" / "exact_120m_labels.parquet")
    return features.merge(
        labels.drop(columns=["timestamp", "instrument"], errors="ignore"),
        on="decision_id",
        how="inner",
        validate="one_to_one",
    ).sort_values(["timestamp", "instrument"], kind="stable", ignore_index=True)


def _fit_gate_scores(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    model_kind: str,
    gate_config: Mapping[str, Any],
) -> tuple[pd.Series, pd.Series, dict[str, Any]]:
    if model_kind == "rule":
        train_score = pd.Series(train["movement_score_rule"].to_numpy(), index=train.index)
        test_score = pd.Series(test["movement_score_rule"].to_numpy(), index=test.index)
        model_manifest: dict[str, Any] = {
            "model_kind": "deterministic_rule",
            "causal_feature_count": len(VAULT_CAUSAL_FEATURES),
        }
    elif model_kind == "hist_gradient_boosting":
        model = fit_movement_classifier(
            train,
            label_col="is_significant",
            id_col="decision_id",
            maximum_train_rows=int(gate_config["maximum_classifier_train_rows"]),
            random_state=int(gate_config["random_seed"]),
            max_iter=int(gate_config["classifier_max_iter"]),
        )
        train_score = score_movement_classifier(train, model)
        test_score = score_movement_classifier(test, model)
        model_manifest = {
            "model_kind": model.model_kind,
            "train_fingerprint": model.train_fingerprint,
            "train_rows": model.train_rows,
            "positive_rows": model.positive_rows,
            "feature_columns": list(model.feature_columns),
        }
    else:
        raise ValueError(f"unknown movement gate model: {model_kind}")
    return train_score, test_score, model_manifest


def _apply_gate_scores(
    train: pd.DataFrame,
    test: pd.DataFrame,
    train_score: pd.Series,
    test_score: pd.Series,
    *,
    model_kind: str,
    quantile: float,
    top_n: int,
    gate_config: Mapping[str, Any],
    model_manifest: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    scored_train = train[["decision_id", "timestamp", "instrument"]].copy()
    scored_train["movement_score"] = train_score.to_numpy()
    threshold = fit_train_only_thresholds(
        scored_train,
        value_col="movement_score",
        group_col="instrument",
        quantile=float(quantile),
        minimum_group_samples=int(gate_config["minimum_group_samples"]),
        valid_col=None,
        timestamp_col="timestamp",
    )
    scored = test.copy()
    scored["movement_score"] = test_score.to_numpy()
    scored = apply_train_only_thresholds(
        scored,
        threshold,
        threshold_col="movement_threshold",
        output_col="instrument_gate_passed",
    )
    scored = apply_cross_sectional_top_n(
        scored,
        score_col="movement_score",
        threshold_col="movement_threshold",
        top_n=int(top_n),
    )
    scored["gate_model_kind"] = model_kind
    scored["gate_quantile"] = float(quantile)
    manifest = {
        **dict(model_manifest),
        "score_quantile": float(quantile),
        "cross_sectional_top_n": int(top_n),
        "threshold_model": asdict(threshold),
    }
    return scored, manifest


def _fit_and_apply_gate(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    model_kind: str,
    quantile: float,
    top_n: int,
    gate_config: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    train_score, test_score, model_manifest = _fit_gate_scores(
        train, test, model_kind=model_kind, gate_config=gate_config
    )
    return _apply_gate_scores(
        train,
        test,
        train_score,
        test_score,
        model_kind=model_kind,
        quantile=quantile,
        top_n=top_n,
        gate_config=gate_config,
        model_manifest=model_manifest,
    )


def _gate_metric(scored: pd.DataFrame) -> dict[str, Any]:
    selected = scored.loc[scored["movement_gate_passed"]]
    baseline = float(scored["forward_abs_pips"].mean())
    chosen = float(selected["forward_abs_pips"].mean()) if len(selected) else 0.0
    lift = chosen / baseline if baseline > 0 else 0.0
    precision = float(selected["is_significant"].mean()) if len(selected) else 0.0
    coverage = len(selected) / max(len(scored), 1)
    # Selection is intentionally based on magnitude quality, not oracle direction.
    score = math.log(max(lift, 1e-12)) + 0.25 * precision
    if len(selected) < 50:
        score -= (50 - len(selected)) / 50.0
    return {
        "selected_rows": len(selected),
        "coverage": coverage,
        "mean_forward_abs_pips": chosen,
        "baseline_mean_forward_abs_pips": baseline,
        "movement_lift": lift,
        "significant_precision": precision,
        "gate_objective": score,
    }


def _stable_trial_choice(rows: pd.DataFrame, id_columns: Sequence[str], score_column: str) -> pd.Series:
    grouped = (
        rows.groupby(list(id_columns), sort=True, dropna=False)[score_column]
        .agg(["median", "min", "count"])
        .reset_index()
    )
    grouped["stable_score"] = grouped["median"] - 0.5 * (
        grouped["median"] - grouped["min"]
    ).abs()
    return grouped.sort_values(
        ["stable_score", *id_columns], ascending=[False, *([True] * len(id_columns))], kind="stable"
    ).iloc[0]


def tune_gate(
    frame: pd.DataFrame,
    inner_folds: Sequence[Any],
    config: Mapping[str, Any],
) -> tuple[dict[str, Any], pd.DataFrame]:
    gate = config["movement_gate"]
    rows: list[dict[str, Any]] = []
    for fold in inner_folds:
        train, test = fold.take_train(frame), fold.take_test(frame)
        for model_kind in gate["model_kinds"]:
            train_score, test_score, model_manifest = _fit_gate_scores(
                train,
                test,
                model_kind=str(model_kind),
                gate_config=gate,
            )
            for quantile, top_n in itertools.product(
                gate["score_quantiles"], gate["cross_sectional_top_n"]
            ):
                scored, _ = _apply_gate_scores(
                    train,
                    test,
                    train_score,
                    test_score,
                    model_kind=str(model_kind),
                    quantile=float(quantile),
                    top_n=int(top_n),
                    gate_config=gate,
                    model_manifest=model_manifest,
                )
                rows.append(
                    {
                        "fold": fold.name,
                        "model_kind": model_kind,
                        "quantile": float(quantile),
                        "top_n": int(top_n),
                        **_gate_metric(scored),
                    }
                )
    trials = pd.DataFrame(rows)
    winner = _stable_trial_choice(
        trials, ["model_kind", "quantile", "top_n"], "gate_objective"
    )
    selected = {
        "model_kind": str(winner["model_kind"]),
        "quantile": float(winner["quantile"]),
        "top_n": int(winner["top_n"]),
        "stable_gate_objective": float(winner["stable_score"]),
    }
    return selected, trials


def _deterministic_grid(
    axes: Mapping[str, Sequence[Any]], maximum: int, seed: int
) -> list[dict[str, Any]]:
    keys = list(axes)
    grid = [dict(zip(keys, values)) for values in itertools.product(*(axes[key] for key in keys))]
    if len(grid) <= int(maximum):
        return grid
    generator = np.random.default_rng(int(seed))
    positions = np.sort(generator.choice(len(grid), size=int(maximum), replace=False))
    return [grid[int(position)] for position in positions]


def make_oco_configs(config: Mapping[str, Any], *, maximum_trials: int | None = None) -> list[OCOConfig]:
    search = config["oco_search"]
    maximum = int(maximum_trials or search["maximum_trials"])
    axes = {
        "range": search["range_lookback_minutes"],
        "timeout": search["trigger_timeout_minutes"],
        "latency": search["cancel_latency_minutes"],
        "spread_buffer": search["buffer_spread_multiple"],
        "atr_buffer": search["buffer_atr_multiple"],
        "stop_atr": search["stop_loss_atr_multiple"],
        "target_atr": search["take_profit_atr_multiple"],
        "hold": search["max_hold_minutes"],
    }
    rows = _deterministic_grid(axes, maximum, int(config["movement_gate"]["random_seed"]))
    return [
        OCOConfig(
            range_lookback_minutes=int(row["range"]),
            trigger_timeout_minutes=int(row["timeout"]),
            cancel_latency_minutes=int(row["latency"]),
            entry_slippage_pips=float(search["entry_slippage_pips"]),
            exit_slippage_pips=float(search["exit_slippage_pips"]),
            same_bar_entry_policy=str(search["same_bar_entry_policy"]),
            buffer=BufferConfig(
                spread_multiple=float(row["spread_buffer"]),
                atr_multiple=float(row["atr_buffer"]),
                combine="max",
            ),
            exit=ExitConfig(
                stop_loss_pips=None,
                take_profit_pips=None,
                stop_loss_atr_multiple=float(row["stop_atr"]),
                take_profit_atr_multiple=float(row["target_atr"]),
                fixed_atr_combine="atr",
                max_hold_minutes=int(row["hold"]),
            ),
        )
        for row in rows
    ]


def make_account_configs(
    config: Mapping[str, Any], *, maximum_trials: int | None = None
) -> list[AccountConfig]:
    search = config["account_search"]
    economics = config["account_economics"]
    maximum = int(maximum_trials or search["maximum_trials"])
    balances = [float(value) for value in economics["starting_balances"]]
    axes = {
        "risk": search["risk_per_trade_fraction"],
        "total_risk": search["max_total_open_risk_fraction"],
        "theme_risk": search["max_theme_open_risk_fraction"],
        "margin": search["max_margin_used_fraction"],
        "positions": search["max_concurrent_positions"],
        "theme_positions": search["max_positions_per_theme"],
        "new_positions": search["max_new_positions_per_timestamp"],
        "daily_loss": search["daily_loss_stop_fraction"],
        "drawdown": search["drawdown_halt_fraction"],
        "mco_buffer": search["margin_closeout_buffer"],
    }
    per_balance = max(1, int(math.ceil(maximum / max(len(balances), 1))))
    rows: list[dict[str, Any]] = []
    for balance_index, balance in enumerate(balances):
        sampled = _deterministic_grid(
            axes,
            per_balance,
            int(config["movement_gate"]["random_seed"]) + 1 + balance_index,
        )
        rows.extend({"starting_balance": balance, **row} for row in sampled)
    if len(rows) > max(maximum, len(balances)):
        rows = rows[: max(maximum, len(balances))]
    return [
        AccountConfig(
            starting_balance=float(row["starting_balance"]),
            account_currency=str(economics["account_currency"]),
            risk_per_trade_fraction=float(row["risk"]),
            max_total_open_risk_fraction=float(row["total_risk"]),
            max_theme_open_risk_fraction=float(row["theme_risk"]),
            max_margin_used_fraction=float(row["margin"]),
            max_concurrent_positions=int(row["positions"]),
            max_positions_per_theme=int(row["theme_positions"]),
            max_new_positions_per_timestamp=int(row["new_positions"]),
            daily_loss_stop_fraction=float(row["daily_loss"]),
            drawdown_halt_fraction=float(row["drawdown"]),
            margin_closeout_buffer=float(row["mco_buffer"]),
            selection_score_column="expected_net_edge_pips",
        )
        for row in rows
    ]


def _load_fold_bars(
    paths: ResearchPaths,
    decisions: pd.DataFrame,
    oco_configs: Sequence[OCOConfig],
) -> pd.DataFrame:
    if decisions.empty:
        return pd.DataFrame()
    before = max(config.range_lookback_minutes for config in oco_configs)
    after = max(
        config.trigger_timeout_minutes + config.exit.max_hold_minutes
        for config in oco_configs
    )
    start = decisions["timestamp"].min() - pd.Timedelta(minutes=before)
    end = decisions["timestamp"].max() + pd.Timedelta(minutes=after)
    instruments = set(decisions["instrument"].astype(str).unique())
    # Account P/L, stop risk, and margin require an exact-timestamp conversion
    # quote even when that conversion pair was not itself selected by the gate.
    available = {path.stem.upper() for path in paths.bar_cache_root.glob("*.parquet")}
    currencies = {
        currency
        for instrument in instruments
        for currency in str(instrument).split("_", 1)
    }
    for currency in currencies:
        if currency == "USD":
            continue
        direct, inverse = f"{currency}_USD", f"USD_{currency}"
        if direct in available:
            instruments.add(direct)
        elif inverse in available:
            instruments.add(inverse)
    instruments = sorted(instruments)
    frames = [
        frame.loc[frame["bar_observed"]].dropna(
            subset=[
                "bid_open",
                "bid_high",
                "bid_low",
                "bid_close",
                "ask_open",
                "ask_high",
                "ask_low",
                "ask_close",
            ]
        )
        for _, frame in iter_normalized_bars(
            paths.bar_cache_root,
            instruments=instruments,
            start=start,
            end=end,
            require_complete_grid=True,
            drop_empty_bars=False,
        )
    ]
    return pd.concat(frames, ignore_index=True)


def _oco_metric(outcomes: pd.DataFrame) -> dict[str, Any]:
    executable = outcomes.loc[outcomes["is_executable_candidate"]].copy()
    if executable.empty:
        return {
            "trades": 0,
            "net_pips": 0.0,
            "mean_r": 0.0,
            "p05_r": 0.0,
            "profit_factor_pips": 0.0,
            "double_trigger_fraction": 0.0,
            "ambiguous_fraction": 0.0,
            "event_cluster_profit_concentration": 0.0,
            "oco_objective": -10.0,
        }
    pnl = pd.to_numeric(executable["realized_pips"], errors="coerce").fillna(0.0)
    risk = pd.to_numeric(executable["risk_pips_per_unit"], errors="coerce").replace(0, np.nan)
    r = (pnl / risk).replace([np.inf, -np.inf], np.nan).dropna()
    wins, losses = pnl[pnl > 0], pnl[pnl < 0]
    profit_factor = (
        float(wins.sum() / abs(losses.sum())) if abs(losses.sum()) > 0 else 0.0
    )
    mean_r = float(r.mean()) if len(r) else 0.0
    tail = float(r.quantile(0.05)) if len(r) else -1.0
    double = float(executable["double_trigger"].mean())
    ambiguous = float(
        (executable["same_bar_entry_ambiguous"] | executable["same_bar_exit_ambiguous"]).mean()
    )
    cluster = executable["event_cluster_id"].astype("string")
    cluster = cluster.where(cluster.notna(), "decision:" + executable["decision_id"].astype(str))
    cluster_pnl = pnl.groupby(cluster).sum()
    positive = cluster_pnl.clip(lower=0.0)
    concentration = float(positive.max() / positive.sum()) if positive.sum() > 0 else 1.0
    objective = mean_r + 0.10 * min(tail, 0.0) - 0.25 * double - 0.10 * ambiguous - 0.15 * concentration
    if len(executable) < 20:
        objective -= (20 - len(executable)) / 20.0
    return {
        "trades": len(executable),
        "net_pips": float(pnl.sum()),
        "mean_r": mean_r,
        "p05_r": tail,
        "profit_factor_pips": profit_factor,
        "double_trigger_fraction": double,
        "ambiguous_fraction": ambiguous,
        "event_cluster_profit_concentration": concentration,
        "oco_objective": objective,
    }


def _decision_schema(
    scored: pd.DataFrame, *, execution_delay_minutes: int = 0
) -> pd.DataFrame:
    selected = scored.loc[scored["movement_gate_passed"]].copy()
    keep = [
        "decision_id",
        "timestamp",
        "instrument",
        "movement_score",
        "movement_threshold",
        "atr_pips",
        "spread_pips",
        "expected_net_edge_pips",
        "currency_theme_cluster_id",
        "event_cluster_id",
        "is_significant",
        "forward_abs_pips",
    ]
    output = selected[keep].reset_index(drop=True)
    output["research_label_timestamp"] = output["timestamp"]
    output["timestamp"] = output["timestamp"] + pd.Timedelta(
        minutes=int(execution_delay_minutes)
    )
    output["execution_knowledge_delay_minutes"] = int(execution_delay_minutes)
    return output


def tune_oco(
    frame: pd.DataFrame,
    inner_folds: Sequence[Any],
    gate_choice: Mapping[str, Any],
    oco_configs: Sequence[OCOConfig],
    config: Mapping[str, Any],
    paths: ResearchPaths,
) -> tuple[OCOConfig, pd.DataFrame, dict[str, pd.DataFrame]]:
    rows: list[dict[str, Any]] = []
    outcomes_by_fold_config: dict[str, pd.DataFrame] = {}
    for fold in inner_folds:
        scored, _ = _fit_and_apply_gate(
            fold.take_train(frame),
            fold.take_test(frame),
            model_kind=str(gate_choice["model_kind"]),
            quantile=float(gate_choice["quantile"]),
            top_n=int(gate_choice["top_n"]),
            gate_config=config["movement_gate"],
        )
        decisions = _decision_schema(
            scored,
            execution_delay_minutes=int(
                config["data"].get("execution_knowledge_delay_minutes", 0)
            ),
        )
        bars = _load_fold_bars(paths, decisions, oco_configs)
        for trial, oco_config in enumerate(oco_configs):
            outcome = evaluate_oco_candidates(decisions, bars, oco_config)
            key = f"{fold.name}|{trial}"
            outcomes_by_fold_config[key] = outcome
            rows.append(
                {
                    "fold": fold.name,
                    "oco_trial": trial,
                    "oco_config": json.dumps(asdict(oco_config), sort_keys=True),
                    **_oco_metric(outcome),
                }
            )
    trials = pd.DataFrame(rows)
    winner = _stable_trial_choice(trials, ["oco_trial"], "oco_objective")
    return oco_configs[int(winner["oco_trial"])], trials, outcomes_by_fold_config


def _objective_config(config: Mapping[str, Any]) -> ObjectiveConfig:
    return ObjectiveConfig(**config["objective"])


def run_nested_research(
    frame: pd.DataFrame,
    folds: Sequence[NestedPurgedFold],
    config: Mapping[str, Any],
    paths: ResearchPaths,
    *,
    maximum_oco_trials: int | None = None,
    maximum_account_trials: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    _ensure_output_layout(paths.output_root)
    oco_configs = make_oco_configs(config, maximum_trials=maximum_oco_trials)
    account_configs = make_account_configs(config, maximum_trials=maximum_account_trials)
    metadata, metadata_ledger = build_project_metadata(
        sorted(frame["instrument"].unique()),
        default_margin_rate=float(
            config["account_economics"]["default_margin_rate_assumption"]
        ),
        jpy_chf_margin_rate=float(
            config["account_economics"]["jpy_chf_margin_rate_assumption"]
        ),
    )
    metadata_ledger.to_csv(paths.output_root / "account" / "instrument_economics.csv", index=False)
    all_gate_trials: list[pd.DataFrame] = []
    all_oco_trials: list[pd.DataFrame] = []
    all_account_trials: list[pd.DataFrame] = []
    outer_summaries: list[dict[str, Any]] = []
    outer_trades: list[pd.DataFrame] = []
    outer_oco_outcomes: list[pd.DataFrame] = []
    fit_checkpoint_root = paths.output_root / "checkpoints" / "fit"
    fit_checkpoint_root.mkdir(parents=True, exist_ok=True)
    dataset_manifest_path = paths.output_root / "manifests" / "dataset_manifest.json"
    if not dataset_manifest_path.exists():
        raise FileNotFoundError("prepared dataset manifest is required before fitting")
    dataset_manifest = json.loads(dataset_manifest_path.read_text(encoding="utf-8"))
    dataset_fingerprint = dataset_manifest.get("dataset_fingerprint") or _file_sha256(
        dataset_manifest_path
    )
    fit_fingerprint_base = {
        "fit_contract_version": FIT_CONTRACT_VERSION,
        "dataset_contract_version": dataset_manifest.get("dataset_contract_version"),
        "dataset_fingerprint": dataset_fingerprint,
        "schema_version": config["schema_version"],
        "config_hash": _hash_payload(config),
        "maximum_oco_trials": len(oco_configs),
        "maximum_account_trials": len(account_configs),
        "oco_grid_sha256": _hash_payload([asdict(item) for item in oco_configs]),
        "account_grid_sha256": _hash_payload([asdict(item) for item in account_configs]),
        "decision_rows": len(frame),
        "decision_min": frame["timestamp"].min(),
        "decision_max": frame["timestamp"].max(),
        "features_source": _source_signature(
            paths.output_root / "features" / "decision_features.parquet"
        ),
        "labels_source": _source_signature(
            paths.output_root / "labels" / "exact_120m_labels.parquet"
        ),
    }

    for nested in folds:
        checkpoint_dir = fit_checkpoint_root / nested.outer.name
        checkpoint_manifest_path = checkpoint_dir / "manifest.json"
        checkpoint_inputs = {
            **fit_fingerprint_base,
            "outer_fold": nested.outer.name,
            "fold_contract": _fold_contract_payload(nested),
        }
        checkpoint_fingerprint = _hash_payload(checkpoint_inputs)
        required_checkpoint_files = [
            checkpoint_dir / "gate_trials.parquet",
            checkpoint_dir / "oco_trials.parquet",
            checkpoint_dir / "account_trials.parquet",
            checkpoint_dir / "outer_results.json",
            checkpoint_dir / "outer_oco_outcomes.parquet",
        ]
        if not force and checkpoint_manifest_path.exists():
            prior = json.loads(checkpoint_manifest_path.read_text(encoding="utf-8"))
            if prior.get("fingerprint") == checkpoint_fingerprint and all(
                path.exists() for path in required_checkpoint_files
            ):
                print(f"fit {nested.outer.name}: reuse completed checkpoint", flush=True)
                all_gate_trials.append(pd.read_parquet(checkpoint_dir / "gate_trials.parquet"))
                all_oco_trials.append(pd.read_parquet(checkpoint_dir / "oco_trials.parquet"))
                all_account_trials.append(pd.read_parquet(checkpoint_dir / "account_trials.parquet"))
                outer_summaries.extend(
                    json.loads((checkpoint_dir / "outer_results.json").read_text(encoding="utf-8"))
                )
                outer_oco_outcomes.append(
                    pd.read_parquet(checkpoint_dir / "outer_oco_outcomes.parquet")
                )
                trades_path = checkpoint_dir / "outer_trades.parquet"
                if trades_path.exists():
                    outer_trades.append(pd.read_parquet(trades_path))
                continue
        print(f"fit {nested.outer.name}: tune causal gate", flush=True)
        gate_choice, gate_trials = tune_gate(frame, nested.inner, config)
        gate_trials["outer_fold"] = nested.outer.name
        all_gate_trials.append(gate_trials)
        print(f"fit {nested.outer.name}: tune OCO after gate freeze", flush=True)
        oco_choice, oco_trials, cached = tune_oco(
            frame, nested.inner, gate_choice, oco_configs, config, paths
        )
        oco_trials["outer_fold"] = nested.outer.name
        all_oco_trials.append(oco_trials)
        chosen_trial = next(
            index for index, candidate in enumerate(oco_configs) if candidate == oco_choice
        )

        inner_account_frames: list[pd.DataFrame] = []
        for inner in nested.inner:
            outcome = cached[f"{inner.name}|{chosen_trial}"].copy()
            bars = _load_fold_bars(
                paths,
                _decision_schema(
                    _fit_and_apply_gate(
                        inner.take_train(frame),
                        inner.take_test(frame),
                        model_kind=str(gate_choice["model_kind"]),
                        quantile=float(gate_choice["quantile"]),
                        top_n=int(gate_choice["top_n"]),
                        gate_config=config["movement_gate"],
                    )[0],
                    execution_delay_minutes=int(
                        config["data"].get("execution_knowledge_delay_minutes", 0)
                    ),
                ),
                [oco_choice],
            )
            outcome = attach_timestamped_economics(outcome, bars, metadata)
            outcome["validation_fold"] = inner.name
            inner_account_frames.append(outcome)
        account_input = pd.concat(inner_account_frames, ignore_index=True)
        account_search = search_account_space(
            account_input,
            account_configs,
            _objective_config(config),
            fold_column="validation_fold",
        )
        leaderboard = account_search.leaderboard.copy()
        leaderboard["outer_fold"] = nested.outer.name
        all_account_trials.append(leaderboard)

        # The execution policy is frozen before account fitting.  Evaluate the
        # outer path once, then replay that identical trade opportunity stream
        # for each starting-balance/account scenario.
        scored_test, gate_manifest = _fit_and_apply_gate(
            nested.outer.take_train(frame),
            nested.outer.take_test(frame),
            model_kind=str(gate_choice["model_kind"]),
            quantile=float(gate_choice["quantile"]),
            top_n=int(gate_choice["top_n"]),
            gate_config=config["movement_gate"],
        )
        outer_gate_metrics = _gate_metric(scored_test)
        decisions = _decision_schema(
            scored_test,
            execution_delay_minutes=int(
                config["data"].get("execution_knowledge_delay_minutes", 0)
            ),
        )
        bars = _load_fold_bars(paths, decisions, [oco_choice])
        outer_outcome = evaluate_oco_candidates(decisions, bars, oco_choice)
        outer_outcome = attach_timestamped_economics(outer_outcome, bars, metadata)
        outer_oco_metrics = _oco_metric(outer_outcome)
        outer_oco_config_id = "oco_" + _hash_payload(asdict(oco_choice))[:12]
        outer_outcome_artifact = outer_outcome.copy()
        outer_outcome_artifact["outer_fold"] = nested.outer.name
        outer_outcome_artifact["oco_config_id"] = outer_oco_config_id
        outer_oco_outcomes.append(outer_outcome_artifact)

        # Select independently for each requested starting balance.  The
        # no-trade baseline remains eligible and wins whenever stable objective
        # is not positive.
        for starting_balance in config["account_economics"]["starting_balances"]:
            # Match using the exact serialized config stored in the leaderboard.
            no_trade_rows = leaderboard.loc[leaderboard["strategy"].eq("no_trade")].copy()
            active_rows = leaderboard.loc[leaderboard["strategy"].ne("no_trade")].copy()
            active_rows = active_rows.loc[
                active_rows["account_config"].map(
                    lambda value: float(value.get("starting_balance", np.nan))
                    == float(starting_balance)
                )
            ]
            active_rows = active_rows.sort_values(
                ["stable_objective", "account_config_id"],
                ascending=[False, True],
                kind="stable",
            )
            best_rejected_active = active_rows.iloc[0] if len(active_rows) else None
            subset = pd.concat([no_trade_rows, active_rows], ignore_index=True).sort_values(
                ["stable_objective", "account_config_id"],
                ascending=[False, True],
                kind="stable",
            )
            account_winner = subset.iloc[0]
            selected_account: AccountConfig | None = None
            if account_winner["strategy"] != "no_trade" and float(account_winner["stable_objective"]) > 0:
                payload = account_winner["account_config"]
                selected_account = AccountConfig(**payload)

            if selected_account is None:
                replay = no_trade_baseline(float(starting_balance))
            else:
                replay = replay_account(outer_outcome, selected_account)
            summary = {
                **replay.summary,
                "outer_fold": nested.outer.name,
                "starting_balance_scenario": float(starting_balance),
                "selected_gate": gate_choice,
                "gate_fit_manifest": gate_manifest,
                "selected_oco": asdict(oco_choice),
                "selected_oco_config_id": outer_oco_config_id,
                "selected_account": asdict(selected_account) if selected_account else None,
                "inner_account_stable_objective": float(account_winner["stable_objective"]),
                "best_rejected_active_account_id": (
                    str(best_rejected_active["account_config_id"])
                    if best_rejected_active is not None
                    else None
                ),
                "best_rejected_active_account": (
                    best_rejected_active["account_config"]
                    if best_rejected_active is not None
                    else None
                ),
                "best_rejected_active_stable_objective": (
                    float(best_rejected_active["stable_objective"])
                    if best_rejected_active is not None
                    else None
                ),
                "outer_gate_metrics": outer_gate_metrics,
                "outer_oco_metrics": outer_oco_metrics,
            }
            summary["outer_objective"] = objective_for_summary(
                replay.summary, _objective_config(config)
            )
            outer_summaries.append(summary)
            if not replay.trades.empty:
                trade = replay.trades.copy()
                trade["outer_fold"] = nested.outer.name
                trade["starting_balance_scenario"] = float(starting_balance)
                outer_trades.append(trade)

        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        _write_parquet(gate_trials, checkpoint_dir / "gate_trials.parquet")
        _write_parquet(oco_trials, checkpoint_dir / "oco_trials.parquet")
        _write_parquet(leaderboard, checkpoint_dir / "account_trials.parquet")
        current_summaries = [
            row for row in outer_summaries if row.get("outer_fold") == nested.outer.name
        ]
        _write_json(checkpoint_dir / "outer_results.json", current_summaries)
        current_trades = [
            trade
            for trade in outer_trades
            if not trade.empty and str(trade["outer_fold"].iloc[0]) == nested.outer.name
        ]
        current_trade_frame = (
            pd.concat(current_trades, ignore_index=True) if current_trades else pd.DataFrame()
        )
        _write_optional_parquet(current_trade_frame, checkpoint_dir / "outer_trades.parquet")
        # Persist the outer opportunity stream even when it has zero rows so a
        # completed checkpoint has an unambiguous, reusable evidence artifact.
        _write_parquet(
            outer_outcome_artifact,
            checkpoint_dir / "outer_oco_outcomes.parquet",
        )
        _write_json(
            checkpoint_manifest_path,
            {
                "fingerprint": checkpoint_fingerprint,
                "fingerprint_inputs": checkpoint_inputs,
                "fit_contract_version": FIT_CONTRACT_VERSION,
                "completed_utc": _utc_now(),
                "outer_fold": nested.outer.name,
                "research_only": True,
                "gate_trial_rows": len(gate_trials),
                "oco_trial_rows": len(oco_trials),
                "account_trial_rows": len(leaderboard),
                "scenario_rows": len(current_summaries),
                "outer_oco_outcome_rows": len(outer_outcome_artifact),
            },
        )

    gate_table = pd.concat(all_gate_trials, ignore_index=True)
    oco_table = pd.concat(all_oco_trials, ignore_index=True)
    account_table = pd.concat(all_account_trials, ignore_index=True)
    outer_table = pd.DataFrame(outer_summaries)
    _write_parquet(gate_table, paths.output_root / "models" / "gate_trials.parquet")
    _write_parquet(oco_table, paths.output_root / "oco" / "oco_trials.parquet")
    _write_parquet(account_table, paths.output_root / "account" / "account_trials.parquet")
    _write_json(
        paths.output_root / "reports" / "outer_fold_results.json", outer_summaries
    )
    aggregate_trades = (
        pd.concat(outer_trades, ignore_index=True) if outer_trades else pd.DataFrame()
    )
    _write_optional_parquet(
        aggregate_trades, paths.output_root / "account" / "outer_test_trades.parquet"
    )
    aggregate_oco_outcomes = (
        pd.concat(outer_oco_outcomes, ignore_index=True)
        if outer_oco_outcomes
        else pd.DataFrame()
    )
    _write_optional_parquet(
        aggregate_oco_outcomes,
        paths.output_root / "oco" / "outer_test_outcomes.parquet",
    )

    scenario_rows: list[dict[str, Any]] = []
    for balance, part in outer_table.groupby("starting_balance_scenario", sort=True):
        scores = part["outer_objective"].astype(float).tolist()
        stable = stable_fold_objective(scores, _objective_config(config))
        scenario_rows.append(
            {
                "starting_balance": float(balance),
                "outer_fold_count": len(part),
                "stable_outer_objective": stable,
                "positive_outer_fold_fraction": float((part["return_fraction"] > 0).mean()),
                "median_return_fraction": float(part["return_fraction"].median()),
                "worst_return_fraction": float(part["return_fraction"].min()),
                "median_max_drawdown_fraction": float(part["max_drawdown_fraction"].median()),
                "margin_closeout_risk_events": int(part["margin_closeout_risk_events"].sum()),
                "opened_trades": int(part["opened_trades"].sum()),
                "beats_no_trade": bool(stable > 0),
            }
        )
    scenarios = pd.DataFrame(scenario_rows)
    _write_csv(scenarios, paths.output_root / "reports" / "account_scenarios.csv")
    result = {
        "fit_contract_version": FIT_CONTRACT_VERSION,
        "dataset_fingerprint": dataset_fingerprint,
        "fit_fingerprint_base": fit_fingerprint_base,
        "generated_utc": _utc_now(),
        "research_only": True,
        "fresh_holdout_available": False,
        "outer_fold_scenarios": scenarios.to_dict("records"),
        "trial_counts": {
            "gate": len(gate_table),
            "oco": len(oco_table),
            "account": len(account_table),
        },
        "promotion_state": "NO_LIVE_PROMOTION_RETROSPECTIVE_ONLY",
        "forward_shadow_start_utc": pd.Timestamp(frame["timestamp"].max())
        + pd.Timedelta(minutes=5),
        "warnings": [
            "Nested outer results estimate retrospective research stability only.",
            "A new chronological shadow period is mandatory before any deployment decision.",
            "Financing remains zero because timestamped account financing schedules are unavailable.",
            "Historical spread and bid/ask OHLC include estimated/reconstructed values.",
        ],
    }
    _write_json(paths.output_root / "manifests" / "fit_manifest.json", result)
    _write_json(
        paths.output_root / "manifests" / "fit_state.json",
        {
            "status": "CURRENT_RETROSPECTIVE_RESEARCH_ONLY",
            "dataset_fingerprint": dataset_fingerprint,
            "fit_contract_version": FIT_CONTRACT_VERSION,
            "updated_utc": _utc_now(),
            "research_only": True,
        },
    )
    return result


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=str(Path(__file__).resolve().parents[1] / "config" / "spike_account_space.json"),
    )
    parser.add_argument("--stage", choices=("prepare", "fit", "all"), default="all")
    parser.add_argument("--instrument", action="append", dest="instruments")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-oco-trials", type=int)
    parser.add_argument("--max-account-trials", type=int)
    parser.add_argument(
        "--bounded-fit",
        action="store_true",
        help="Run a reproducible first-pass gate/search budget over the full universe.",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Use six liquid pairs and cap searches; outputs remain clearly marked smoke.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    config, paths = load_config(args.config)
    if args.smoke:
        paths = ResearchPaths(
            **{**asdict(paths), "output_root": paths.output_root.parent / "spike_account_space_smoke"}
        )
        if not args.instruments:
            args.instruments = ["EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD", "USD_CHF"]
        config = json.loads(json.dumps(config))
        config["validation"].update(
            {"outer_splits": 2, "inner_splits": 2, "outer_initial_train_fraction": 0.45}
        )
        config["movement_gate"].update(
            {
                "score_quantiles": [0.95, 0.99],
                "cross_sectional_top_n": [1, 3],
                "maximum_classifier_train_rows": 100000,
                "classifier_max_iter": 50,
            }
        )
        args.max_oco_trials = args.max_oco_trials or 4
        args.max_account_trials = args.max_account_trials or 12
    elif args.bounded_fit:
        config = json.loads(json.dumps(config))
        config["movement_gate"].update(
            {
                "score_quantiles": [0.95, 0.99],
                "cross_sectional_top_n": [1, 3],
                "maximum_classifier_train_rows": 200000,
                "classifier_max_iter": 60,
            }
        )
        args.max_oco_trials = args.max_oco_trials or 8
        args.max_account_trials = args.max_account_trials or 32
    _ensure_output_layout(paths.output_root)
    if args.stage in {"fit", "all"}:
        _write_json(
            paths.output_root / "manifests" / "effective_fit_config.json",
            {
                "generated_utc": _utc_now(),
                "bounded_fit": bool(args.bounded_fit),
                "smoke_run": bool(args.smoke),
                "maximum_oco_trials": args.max_oco_trials,
                "maximum_account_trials": args.max_account_trials,
                "effective_config": config,
            },
        )
    if args.stage in {"prepare", "all"}:
        frame, folds, manifest = prepare_research_dataset(
            config,
            paths,
            instruments=args.instruments,
            force=args.force,
        )
        if args.smoke:
            manifest["smoke_run"] = True
            _write_json(paths.output_root / "manifests" / "dataset_manifest.json", manifest)
    else:
        frame = load_prepared_dataset(paths)
        folds = build_nested_splits(frame, config)
    if args.stage in {"fit", "all"}:
        result = run_nested_research(
            frame,
            folds,
            config,
            paths,
            maximum_oco_trials=args.max_oco_trials,
            maximum_account_trials=args.max_account_trials,
            force=args.force,
        )
        if args.smoke:
            result["smoke_run"] = True
            _write_json(paths.output_root / "manifests" / "fit_manifest.json", result)
    print(f"research outputs: {paths.output_root}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
