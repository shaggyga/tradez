from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import write_json


GROUP_COLS = ["timestamp", "pair", "side", "policy_name"]
PREDICTION_COLS = [
    "predicted_ev_account",
    "predicted_ev_pips",
    "predicted_mfe_pips",
    "predicted_mae_pips",
    "predicted_tp_before_sl_prob",
    "predicted_stop_first_prob",
]
SURFACE_LABEL_COLS = [
    "actual_policy_pnl_1k",
    "actual_policy_net_pips",
    "actual_endpoint_pips",
    "actual_horizon_mfe_pips",
    "actual_horizon_mae_pips",
    "actual_tp_before_sl",
    "actual_stop_first",
    "actual_timeout",
]


def _read_table(path: Path, max_rows: int | None = None, columns: list[str] | None = None) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        try:
            df = pd.read_parquet(path, columns=columns)
        except Exception:
            df = pd.read_parquet(path)
            if columns is not None:
                df = df[[c for c in columns if c in df.columns]]
        return df.head(max_rows).copy() if max_rows else df
    return pd.read_csv(path, nrows=max_rows, usecols=lambda c: columns is None or c in columns)


def _normalize_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if "decision_time_utc" in df.columns:
        df["decision_time_utc"] = pd.to_datetime(df["decision_time_utc"], utc=True)
    return df


def _variance_rows(df: pd.DataFrame, value_cols: list[str]) -> pd.DataFrame:
    rows = []
    present = [c for c in value_cols if c in df.columns]
    if not present:
        return pd.DataFrame()
    for col in present:
        stats = df.groupby(GROUP_COLS)[col].agg(["nunique", "var", "std"]).reset_index()
        stats["value_column"] = col
        rows.append(stats.rename(columns={"nunique": "horizon_unique_values", "var": "horizon_variance", "std": "horizon_std"}))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _summary_from_variance(var_df: pd.DataFrame) -> dict[str, Any]:
    out = {}
    if var_df.empty:
        return out
    for col, g in var_df.groupby("value_column"):
        identical = g["horizon_unique_values"].fillna(0) <= 1
        out[col] = {
            "groups": int(len(g)),
            "percent_identical_across_horizons": float(identical.mean() * 100.0),
            "mean_horizon_variance": float(g["horizon_variance"].fillna(0).mean()),
            "std_horizon_variance": float(g["horizon_variance"].fillna(0).std(ddof=0)),
        }
    return out


def _corr_by_horizon(df: pd.DataFrame, value_cols: list[str]) -> dict[str, Any]:
    out = {}
    for col in [c for c in value_cols if c in df.columns]:
        pivot = df.pivot_table(index=GROUP_COLS, columns="horizon_min", values=col, aggfunc="first")
        corr = pivot.corr()
        out[col] = {
            str(k): {str(kk): (None if pd.isna(vv) else float(vv)) for kk, vv in v.items()}
            for k, v in corr.to_dict().items()
        }
    return out


def _example_rows(df: pd.DataFrame, label_var: pd.DataFrame, pred_var: pd.DataFrame, output_path: Path) -> None:
    examples = []
    candidates = []
    for kind, var_df in [("labels", label_var), ("predictions", pred_var)]:
        if var_df.empty:
            continue
        grouped = var_df.groupby(GROUP_COLS)["horizon_unique_values"].max().reset_index()
        identical = grouped[grouped["horizon_unique_values"] <= 1].head(5)
        different = grouped[grouped["horizon_unique_values"] > 1].head(5)
        candidates.extend([(kind, "all_horizons_identical", r) for _, r in identical.iterrows()])
        candidates.extend([(kind, "horizons_differ", r) for _, r in different.iterrows()])
    keep_cols = GROUP_COLS + ["horizon_min"] + [c for c in SURFACE_LABEL_COLS + PREDICTION_COLS if c in df.columns]
    for kind, status, row in candidates:
        mask = pd.Series(True, index=df.index)
        for col in GROUP_COLS:
            mask &= df[col] == row[col]
        part = df.loc[mask, keep_cols].sort_values("horizon_min").copy()
        part.insert(0, "audit_kind", kind)
        part.insert(1, "audit_status", status)
        examples.append(part)
    if examples:
        pd.concat(examples, ignore_index=True).to_csv(output_path, index=False)
    else:
        pd.DataFrame(columns=["audit_kind", "audit_status"] + keep_cols).to_csv(output_path, index=False)


def write_horizon_integrity_report(
    surface_path: Path,
    output_dir: Path,
    dataset_path: Path | None = None,
    max_rows: int | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    cols = list(dict.fromkeys(GROUP_COLS + ["horizon_min"] + PREDICTION_COLS + SURFACE_LABEL_COLS))
    surface = _normalize_timestamp(_read_table(surface_path, max_rows=max_rows, columns=cols))
    prediction_surface = surface.copy()
    if "timestamp" not in surface.columns and "decision_time_utc" in surface.columns:
        surface = surface.rename(columns={"decision_time_utc": "timestamp", "instrument": "pair"})
    label_cols = [c for c in SURFACE_LABEL_COLS if c in surface.columns]
    if not label_cols and dataset_path is not None:
        # Dataset-level fallback for old runs: synthesize long label rows from policy/horizon columns.
        ds = _normalize_timestamp(_read_table(dataset_path, max_rows=max_rows))
        records = []
        old_policies = [c.removeprefix("policy_").removesuffix("_net_account_pnl_1k_units") for c in ds.columns if c.startswith("policy_") and c.endswith("_net_account_pnl_1k_units")]
        horizons = sorted({int(c.split("_")[-3].removesuffix("m")) for c in ds.columns if c.startswith("net_account_pnl_") and c.endswith("_1k_units")})
        for policy in old_policies:
            for h in horizons:
                part = pd.DataFrame({
                    "timestamp": ds["decision_time_utc"],
                    "pair": ds["instrument"],
                    "side": ds["side"],
                    "policy_name": policy,
                    "horizon_min": h,
                    "actual_policy_pnl_1k": ds[f"policy_{policy}_net_account_pnl_1k_units"],
                    "actual_policy_net_pips": ds.get(f"policy_{policy}_net_pips"),
                    "actual_endpoint_pips": ds.get(f"endpoint_{h}m_pips"),
                    "actual_horizon_mfe_pips": ds.get(f"mfe_{h}m_pips"),
                    "actual_horizon_mae_pips": ds.get(f"mae_{h}m_pips"),
                    "actual_tp_before_sl": ds.get(f"policy_{policy}_tp_before_sl"),
                    "actual_stop_first": ds.get(f"policy_{policy}_sl_before_tp"),
                })
                records.append(part)
        labels = pd.concat(records, ignore_index=True) if records else pd.DataFrame()
        surface = labels
        label_cols = [c for c in SURFACE_LABEL_COLS if c in surface.columns]
    pred_cols = [c for c in PREDICTION_COLS if c in prediction_surface.columns]
    label_var = _variance_rows(surface, label_cols)
    pred_var = _variance_rows(prediction_surface, pred_cols)
    label_var.to_csv(output_dir / "horizon_label_variance.csv", index=False)
    pred_var.to_csv(output_dir / "horizon_prediction_variance.csv", index=False)
    example_source = surface
    if not prediction_surface.empty and all(c in prediction_surface.columns for c in GROUP_COLS + ["horizon_min"]):
        example_source = pd.concat([surface, prediction_surface], ignore_index=True, sort=False)
    _example_rows(example_source, label_var, pred_var, output_dir / "horizon_example_rows.csv")
    label_summary = _summary_from_variance(label_var)
    pred_summary = _summary_from_variance(pred_var)
    report = {
        "surface_path": str(surface_path),
        "dataset_path": str(dataset_path) if dataset_path else None,
        "rows_audited": int(len(surface)),
        "groups_audited": int(surface[GROUP_COLS].drop_duplicates().shape[0]) if all(c in surface.columns for c in GROUP_COLS) else 0,
        "label_summary": label_summary,
        "prediction_summary": pred_summary,
        "label_correlation_by_horizon": _corr_by_horizon(surface, label_cols),
        "prediction_correlation_by_horizon": _corr_by_horizon(prediction_surface, pred_cols),
        "label_integrity_failed": any(v["percent_identical_across_horizons"] > 80.0 for v in label_summary.values()),
        "prediction_integrity_failed": any(v["percent_identical_across_horizons"] > 80.0 for v in pred_summary.values()),
    }
    write_json(output_dir / "horizon_integrity_report.json", report)
    return output_dir / "horizon_integrity_report.json"


def write_top_ev_failure_diagnostics(surface_path: Path, output_dir: Path, max_rows: int | None = None) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    cols = list(dict.fromkeys(GROUP_COLS + ["horizon_min", "session", "spread_pips", "spread_to_atr"] + PREDICTION_COLS + SURFACE_LABEL_COLS))
    df = _read_table(surface_path, max_rows=max_rows, columns=cols)
    if df.empty or "actual_policy_pnl_1k" not in df.columns:
        report = {"rows": int(len(df)), "available": False, "reason": "surface lacks realized policy P/L columns"}
        write_json(output_dir / "top_ev_failure_diagnostics.json", report)
        pd.DataFrame().to_csv(output_dir / "top_ev_failure_examples.csv", index=False)
        return output_dir / "top_ev_failure_diagnostics.json"
    top = df.sort_values("predicted_ev_account", ascending=False).groupby("timestamp", as_index=False).head(5).copy()
    opp = df[["timestamp", "pair", "side", "horizon_min", "policy_name", "actual_policy_pnl_1k", "actual_horizon_mfe_pips"]].copy()
    opp["side"] = np.where(opp["side"] == "long", "short", "long")
    opp = opp.rename(columns={"actual_policy_pnl_1k": "opposite_actual_policy_pnl_1k", "actual_horizon_mfe_pips": "opposite_actual_mfe_pips"})
    top = top.merge(opp, on=["timestamp", "pair", "side", "horizon_min", "policy_name"], how="left")
    top["direction_wrong"] = (top["actual_policy_pnl_1k"] <= 0) & (top["opposite_actual_policy_pnl_1k"].fillna(0) > 0)
    top["both_sides_worked"] = (top["actual_policy_pnl_1k"] > 0) & (top["opposite_actual_policy_pnl_1k"].fillna(0) > 0)
    top["neither_side_worked"] = (top["actual_policy_pnl_1k"] <= 0) & (top["opposite_actual_policy_pnl_1k"].fillna(0) <= 0)
    top["high_mfe_but_bad_exit"] = (top["actual_horizon_mfe_pips"].fillna(0) > 0) & (top["actual_policy_pnl_1k"] <= 0)
    top.to_csv(output_dir / "top_ev_failure_examples.csv", index=False)
    report = {
        "available": True,
        "top_rows": int(len(top)),
        "actual_pnl_mean": float(top["actual_policy_pnl_1k"].mean()),
        "predicted_ev_mean": float(top["predicted_ev_account"].mean()),
        "prediction_overestimate_mean": float((top["predicted_ev_account"] - top["actual_policy_pnl_1k"]).mean()),
        "direction_wrong_rate": float(top["direction_wrong"].mean()),
        "both_sides_worked_rate": float(top["both_sides_worked"].mean()),
        "neither_side_worked_rate": float(top["neither_side_worked"].mean()),
        "high_mfe_but_bad_exit_rate": float(top["high_mfe_but_bad_exit"].mean()),
        "by_pair": top.groupby("pair")["actual_policy_pnl_1k"].agg(["count", "sum", "mean"]).reset_index().to_dict("records"),
        "by_horizon": top.groupby("horizon_min")["actual_policy_pnl_1k"].agg(["count", "sum", "mean"]).reset_index().to_dict("records"),
        "by_policy": top.groupby("policy_name")["actual_policy_pnl_1k"].agg(["count", "sum", "mean"]).reset_index().to_dict("records"),
    }
    if "spread_to_atr" in top.columns:
        report["spread_to_atr_mean"] = float(top["spread_to_atr"].mean())
    write_json(output_dir / "top_ev_failure_diagnostics.json", report)
    return output_dir / "top_ev_failure_diagnostics.json"
