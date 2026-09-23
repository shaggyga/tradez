#!/usr/bin/env python3
"""Frozen model-specialization review for the OANDA trainer.

This is intentionally reporting-only. It does not call OANDA, change live
account managers, promote models, or restart the trainer.

Outputs:
* model class summaries
* best-per-segment leaders
* spike-lead trade-simulation proxy from experiment threshold summaries
* ensemble routing candidates by role/segment
* a compact PNG visual and a ZIP archive of the frozen review
"""

from __future__ import annotations

import csv
import json
import math
import os
import re
import shutil
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

try:
    from PIL import Image, ImageDraw, ImageFont
except Exception as exc:  # pragma: no cover
    raise SystemExit("Missing dependency: pillow/PIL") from exc


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(os.environ.get("TRAD_PROJECT_ROOT", str(DEFAULT_PROJECT_ROOT)))
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
RESEARCH_ROOT = TRAINING_ROOT / "continuous_research"
EXPERIMENTS_ROOT = RESEARCH_ROOT / "experiments"
REPORTS_ROOT = TRAINING_ROOT / "reports"
PROMOTIONS_ROOT = TRAINING_ROOT / "promotions"
STATE_ROOT = TRAINING_ROOT / "state"
MODEL_LIFECYCLE_ROOT = TRAINING_ROOT / "model_lifecycle"
LEDGER_PATH = RESEARCH_ROOT / "experiment_ledger.csv"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_stamp() -> str:
    return utc_now().strftime("%Y%m%d_%H%M%S")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in ("", None):
            return default
        return int(float(value))
    except Exception:
        return default


def read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


def write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    os.replace(tmp, path)


def load_ledger() -> pd.DataFrame:
    if not LEDGER_PATH.exists():
        raise SystemExit(f"missing experiment ledger: {LEDGER_PATH}")
    df = pd.read_csv(LEDGER_PATH)
    df["time_utc"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
    numeric_cols = [
        "mean_auc",
        "minimum_week_auc",
        "selected_threshold",
        "trades",
        "positive_weeks",
        "mean_net_pips",
        "median_week_mean_net_pips",
        "median_profit_factor",
        "bootstrap_lower_mean_net_pips",
        "top_pair_trade_share",
        "score",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["time_utc"]).sort_values("time_utc").copy()
    df["gate_passed_bool"] = df.get("gate_passed", "").astype(str).str.lower().isin(
        {"true", "1", "yes", "y"}
    )
    df["successful_bool"] = df.get("status", "").astype(str).str.lower().eq("successful")
    df["estimated_total_net_pips"] = df["mean_net_pips"].fillna(0) * df["trades"].fillna(0)
    df["target_family"] = df.apply(infer_target_family_row, axis=1)
    df["quality_bucket"] = df.apply(infer_quality_bucket, axis=1)
    return df


def infer_target_family(text: str) -> str:
    s = (text or "").lower()
    if "major_event_lead" in s:
        return "spike_lead"
    if "major_event" in s:
        return "major_event"
    if "reversal_curve" in s or "reversal" in s:
        return "reversal"
    if "short_curve" in s or "short_" in s or "profitable_short" in s:
        return "short_bias"
    if "long_curve" in s or "long_" in s or "profitable_long" in s:
        return "long_bias"
    if "continuation_curve" in s or "continuation" in s:
        return "continuation"
    if "profitable_any" in s:
        return "opportunity"
    if "would_profit" in s:
        return "base_trade_quality"
    return "other"


def infer_target_family_row(row: pd.Series) -> str:
    text = " ".join(
        str(row.get(col, "") or "")
        for col in ["target", "outcome", "direction_target", "dataset_kind"]
    )
    return infer_target_family(text)


def infer_quality_bucket(row: pd.Series) -> str:
    auc = safe_float(row.get("mean_auc"), 0.0)
    mean_net = safe_float(row.get("mean_net_pips"), 0.0)
    trades = safe_float(row.get("trades"), 0.0)
    pf = safe_float(row.get("median_profit_factor"), 0.0)
    gate = bool(row.get("gate_passed_bool"))
    if gate and mean_net > 0 and trades >= 75 and pf >= 1.2:
        return "production_candidate"
    if auc >= 0.70 and mean_net <= 0:
        return "high_auc_bad_economics"
    if mean_net > 0 and trades >= 25 and pf >= 1.0:
        return "economic_edge"
    if auc >= 0.58:
        return "predictive_only"
    return "rejected_or_noise"


def summary_record(group: pd.DataFrame, fields: Dict[str, Any]) -> Dict[str, Any]:
    def safe_idxmax(column: str) -> Any:
        series = pd.to_numeric(group[column], errors="coerce")
        if series.notna().any():
            return series.idxmax()
        return group.index[0]

    best_pip_idx = safe_idxmax("estimated_total_net_pips")
    best_auc_idx = safe_idxmax("mean_auc")
    best_score_idx = safe_idxmax("score")
    best_pip = group.loc[best_pip_idx]
    best_auc = group.loc[best_auc_idx]
    best_score = group.loc[best_score_idx]
    return {
        **fields,
        "experiments": int(len(group)),
        "successful": int(group["successful_bool"].sum()),
        "gate_passed": int(group["gate_passed_bool"].sum()),
        "median_mean_net_pips": round(safe_float(group["mean_net_pips"].median()), 6),
        "best_mean_net_pips": round(safe_float(group["mean_net_pips"].max()), 6),
        "median_estimated_total_net_pips": round(
            safe_float(group["estimated_total_net_pips"].median()), 4
        ),
        "best_estimated_total_net_pips": round(
            safe_float(group["estimated_total_net_pips"].max()), 4
        ),
        "median_auc": round(safe_float(group["mean_auc"].median()), 6),
        "best_auc": round(safe_float(group["mean_auc"].max()), 6),
        "median_score": round(safe_float(group["score"].median()), 4),
        "best_score": round(safe_float(group["score"].max()), 4),
        "best_pip_experiment_id": str(best_pip.get("experiment_id", "")),
        "best_pip_model_type": str(best_pip.get("model_type", "")),
        "best_pip_target": str(best_pip.get("target", "")),
        "best_auc_experiment_id": str(best_auc.get("experiment_id", "")),
        "best_auc_model_type": str(best_auc.get("model_type", "")),
        "best_auc_target": str(best_auc.get("target", "")),
        "best_score_experiment_id": str(best_score.get("experiment_id", "")),
    }


def build_class_summaries(df: pd.DataFrame, outdir: Path) -> Dict[str, Any]:
    outputs: Dict[str, Any] = {}
    group_specs = {
        "model_class_summary": ["model_type"],
        "target_family_summary": ["target_family"],
        "model_target_class_summary": ["model_type", "target_family"],
        "subset_target_class_summary": ["instrument_subset", "target_family"],
        "dataset_model_class_summary": ["dataset_kind", "model_type"],
        "quality_bucket_summary": ["quality_bucket"],
    }
    fields_common = [
        "experiments",
        "successful",
        "gate_passed",
        "median_mean_net_pips",
        "best_mean_net_pips",
        "median_estimated_total_net_pips",
        "best_estimated_total_net_pips",
        "median_auc",
        "best_auc",
        "median_score",
        "best_score",
        "best_pip_experiment_id",
        "best_pip_model_type",
        "best_pip_target",
        "best_auc_experiment_id",
        "best_auc_model_type",
        "best_auc_target",
        "best_score_experiment_id",
    ]
    for name, keys in group_specs.items():
        rows: List[Dict[str, Any]] = []
        for key, group in df.groupby(keys, dropna=False):
            if not isinstance(key, tuple):
                key = (key,)
            fields = {col: str(value) for col, value in zip(keys, key)}
            rows.append(summary_record(group, fields))
        rows.sort(
            key=lambda row: (
                safe_float(row["best_estimated_total_net_pips"]),
                safe_float(row["best_auc"]),
                safe_float(row["gate_passed"]),
            ),
            reverse=True,
        )
        path = outdir / f"{name}.csv"
        write_csv(path, rows, keys + fields_common)
        outputs[name] = str(path)
    return outputs


def experiment_paths(limit: Optional[int] = None) -> List[Path]:
    paths = list(EXPERIMENTS_ROOT.glob("*.json"))
    paths.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return paths[:limit] if limit else paths


def experiment_base(payload: Dict[str, Any], path: Path) -> Dict[str, Any]:
    spec = payload.get("spec") if isinstance(payload.get("spec"), dict) else {}
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    gate = result.get("gate") if isinstance(result.get("gate"), dict) else {}
    return {
        "experiment_id": payload.get("experiment_id", ""),
        "detail_path": str(path),
        "status": payload.get("status", ""),
        "source": spec.get("source", ""),
        "research_role": spec.get("research_role", ""),
        "evaluation_stage": spec.get("evaluation_stage", result.get("evaluation_stage", "")),
        "dataset_kind": spec.get("dataset_kind", ""),
        "model_type": spec.get("model_type", ""),
        "target": spec.get("target", ""),
        "target_family": infer_target_family(
            " ".join(str(spec.get(k, "") or "") for k in ["target", "outcome", "direction_target"])
        ),
        "outcome": spec.get("outcome", ""),
        "execution_policy": spec.get("execution_policy", ""),
        "feature_set": spec.get("feature_set", ""),
        "instrument_subset": spec.get("instrument_subset", result.get("instrument_subset", "")),
        "score": round(safe_float(result.get("score")), 4),
        "mean_auc": round(safe_float(result.get("mean_auc")), 6),
        "mean_average_precision": round(safe_float(result.get("mean_average_precision")), 6),
        "mean_average_precision_lift": round(
            safe_float(result.get("mean_average_precision_lift")), 6
        ),
        "selected_threshold": result.get("selected_threshold", ""),
        "gate_passed": bool(gate.get("passed", result.get("gate_passed", False))),
    }


def leader_score(mean_net: float, trades: float, profit_factor: float, auc: float) -> float:
    return (
        max(mean_net, -2.0)
        * math.log1p(max(trades, 0.0))
        * max(min(profit_factor, 12.0), 0.0)
        * (0.75 + max(auc, 0.0))
    )


def build_segment_leaders(outdir: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    all_rows: List[Dict[str, Any]] = []
    for path in experiment_paths():
        payload = read_json(path, {})
        if not isinstance(payload, dict):
            continue
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        scorecards = result.get("scorecards")
        if not isinstance(scorecards, dict):
            continue
        base = experiment_base(payload, path)
        for scorecard, entries in scorecards.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                trades = safe_float(entry.get("trades"), 0.0)
                mean_net = safe_float(entry.get("mean_net"), 0.0)
                pf = safe_float(entry.get("profit_factor"), 0.0)
                row = {
                    **base,
                    "scorecard": scorecard,
                    "segment": entry.get("segment", ""),
                    "trades": round(trades, 2),
                    "win_rate": round(safe_float(entry.get("win_rate")), 6),
                    "mean_net": round(mean_net, 8),
                    "total_net": round(safe_float(entry.get("total_net")), 8),
                    "profit_factor": round(pf, 6),
                    "leader_score": round(leader_score(mean_net, trades, pf, safe_float(base["mean_auc"])), 8),
                }
                all_rows.append(row)
    all_rows.sort(
        key=lambda r: (
            bool(r.get("gate_passed")),
            safe_float(r.get("leader_score")),
            safe_float(r.get("trades")),
        ),
        reverse=True,
    )
    fields = [
        "scorecard",
        "segment",
        "leader_score",
        "trades",
        "win_rate",
        "mean_net",
        "total_net",
        "profit_factor",
        "gate_passed",
        "experiment_id",
        "status",
        "source",
        "research_role",
        "evaluation_stage",
        "dataset_kind",
        "model_type",
        "target_family",
        "target",
        "outcome",
        "execution_policy",
        "feature_set",
        "instrument_subset",
        "score",
        "mean_auc",
        "mean_average_precision",
        "mean_average_precision_lift",
        "selected_threshold",
        "detail_path",
    ]
    write_csv(outdir / "all_segment_scorecard_rows.csv", all_rows, fields)

    best_rows: List[Dict[str, Any]] = []
    seen: Dict[Tuple[str, str], int] = {}
    for row in all_rows:
        key = (str(row.get("scorecard", "")), str(row.get("segment", "")))
        count = seen.get(key, 0)
        if count >= 5:
            continue
        # Avoid extremely tiny sample rows as segment leaders unless no one else exists.
        if safe_float(row.get("trades")) < 10 and count > 0:
            continue
        best_rows.append({**row, "rank_in_segment": count + 1})
        seen[key] = count + 1
    write_csv(outdir / "best_models_per_segment_top5.csv", best_rows, ["rank_in_segment"] + fields)
    return {
        "all_segment_rows": len(all_rows),
        "best_segment_rows": len(best_rows),
        "all_segment_scorecard_rows_csv": str(outdir / "all_segment_scorecard_rows.csv"),
        "best_models_per_segment_top5_csv": str(outdir / "best_models_per_segment_top5.csv"),
    }, best_rows


def is_spike_lead_spec(base: Dict[str, Any]) -> bool:
    text = " ".join(
        str(base.get(key, "") or "")
        for key in ["target", "target_family", "outcome", "research_role", "source"]
    ).lower()
    return "major_event_lead" in text or "spike_lead" in text or "exhaustion" in text


def build_spike_lead_trade_proxy(outdir: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    for path in experiment_paths():
        payload = read_json(path, {})
        if not isinstance(payload, dict):
            continue
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        base = experiment_base(payload, path)
        if not is_spike_lead_spec(base):
            continue
        threshold_summary = result.get("threshold_summary")
        if not isinstance(threshold_summary, list):
            threshold_summary = []
        total_trades = sum(safe_float(item.get("trades")) for item in threshold_summary if isinstance(item, dict))
        total_net = sum(safe_float(item.get("total_net_pips")) for item in threshold_summary if isinstance(item, dict))
        positive_weeks = sum(
            1
            for item in threshold_summary
            if isinstance(item, dict) and safe_float(item.get("total_net_pips")) > 0
        )
        pfs = [
            safe_float(item.get("profit_factor"))
            for item in threshold_summary
            if isinstance(item, dict) and safe_float(item.get("profit_factor")) > 0
        ]
        win_rates = [
            safe_float(item.get("win_rate"))
            for item in threshold_summary
            if isinstance(item, dict)
        ]
        mean_net = total_net / total_trades if total_trades else 0.0
        avg_pf = sum(pfs) / len(pfs) if pfs else 0.0
        proxy_score = (
            mean_net
            * math.log1p(max(total_trades, 0.0))
            * max(min(avg_pf, 10.0), 0.0)
            * (0.75 + safe_float(base.get("mean_auc")))
            * (1.0 + positive_weeks / max(len(threshold_summary), 1))
        )
        rows.append(
            {
                **base,
                "weeks": len(threshold_summary),
                "positive_weeks": positive_weeks,
                "total_selected_trades": round(total_trades, 2),
                "proxy_total_net_pips": round(total_net, 6),
                "proxy_mean_net_pips": round(mean_net, 8),
                "median_profit_factor": round(pd.Series(pfs).median() if pfs else 0.0, 6),
                "mean_win_rate": round(sum(win_rates) / len(win_rates) if win_rates else 0.0, 6),
                "proxy_score": round(proxy_score, 8),
                "calibration_note": "threshold_summary proxy; not live P&L",
            }
        )
    rows.sort(
        key=lambda r: (
            safe_float(r.get("proxy_score")),
            safe_float(r.get("proxy_total_net_pips")),
            safe_float(r.get("mean_auc")),
        ),
        reverse=True,
    )
    fields = [
        "proxy_score",
        "proxy_total_net_pips",
        "proxy_mean_net_pips",
        "total_selected_trades",
        "weeks",
        "positive_weeks",
        "median_profit_factor",
        "mean_win_rate",
        "mean_auc",
        "mean_average_precision_lift",
        "gate_passed",
        "experiment_id",
        "status",
        "source",
        "research_role",
        "evaluation_stage",
        "dataset_kind",
        "model_type",
        "target_family",
        "target",
        "outcome",
        "execution_policy",
        "feature_set",
        "instrument_subset",
        "score",
        "selected_threshold",
        "calibration_note",
        "detail_path",
    ]
    write_csv(outdir / "spike_lead_trade_sim_proxy.csv", rows, fields)
    return {
        "spike_lead_rows": len(rows),
        "spike_lead_trade_sim_proxy_csv": str(outdir / "spike_lead_trade_sim_proxy.csv"),
        "top": rows[:10],
    }, rows


def role_for_row(row: Dict[str, Any]) -> str:
    family = str(row.get("target_family", ""))
    text = " ".join(str(row.get(k, "") or "") for k in ["target", "outcome", "execution_policy"]).lower()
    if family == "spike_lead":
        return "spike_probability"
    if "direction" in text or family in {"long_bias", "short_bias", "reversal"}:
        return "direction_or_reversal"
    if "curve" in text or "trailing" in text:
        return "exit_curve"
    if family == "opportunity":
        return "opportunity_cost_filter"
    if family == "continuation":
        return "continuation_filter"
    return "supporting_model"


def build_ensemble_routing_candidates(outdir: Path, segment_rows: List[Dict[str, Any]], spike_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    candidates: List[Dict[str, Any]] = []
    for row in segment_rows:
        role = role_for_row(row)
        if safe_float(row.get("trades")) < 10:
            continue
        if safe_float(row.get("mean_net")) <= 0:
            continue
        candidates.append(
            {
                **row,
                "ensemble_role": role,
                "routing_reason": "best positive segment scorecard row",
            }
        )
    for row in spike_rows[:80]:
        if safe_float(row.get("proxy_total_net_pips")) <= 0 and safe_float(row.get("mean_auc")) < 0.7:
            continue
        candidates.append(
            {
                **row,
                "scorecard": "spike_lead_proxy",
                "segment": row.get("instrument_subset", ""),
                "leader_score": row.get("proxy_score", 0.0),
                "ensemble_role": "spike_probability",
                "routing_reason": "lead-time spike candidate from threshold_summary proxy",
            }
        )

    # Keep top few per role/scorecard/segment to avoid an unreadable ensemble menu.
    candidates.sort(
        key=lambda r: (
            str(r.get("ensemble_role", "")),
            str(r.get("scorecard", "")),
            str(r.get("segment", "")),
            safe_float(r.get("leader_score", r.get("proxy_score", 0.0))),
        ),
        reverse=True,
    )
    kept: List[Dict[str, Any]] = []
    seen: Dict[Tuple[str, str, str], int] = {}
    for row in candidates:
        key = (
            str(row.get("ensemble_role", "")),
            str(row.get("scorecard", "")),
            str(row.get("segment", "")),
        )
        if seen.get(key, 0) >= 3:
            continue
        kept.append(row)
        seen[key] = seen.get(key, 0) + 1
    kept.sort(
        key=lambda r: safe_float(r.get("leader_score", r.get("proxy_score", 0.0))),
        reverse=True,
    )
    fields = [
        "ensemble_role",
        "routing_reason",
        "scorecard",
        "segment",
        "leader_score",
        "trades",
        "win_rate",
        "mean_net",
        "total_net",
        "profit_factor",
        "proxy_total_net_pips",
        "proxy_mean_net_pips",
        "total_selected_trades",
        "mean_auc",
        "gate_passed",
        "experiment_id",
        "status",
        "source",
        "research_role",
        "evaluation_stage",
        "dataset_kind",
        "model_type",
        "target_family",
        "target",
        "outcome",
        "execution_policy",
        "feature_set",
        "instrument_subset",
        "score",
        "selected_threshold",
        "detail_path",
    ]
    write_csv(outdir / "ensemble_routing_candidates.csv", kept, fields)
    role_counts = {}
    for row in kept:
        role_counts[row["ensemble_role"]] = role_counts.get(row["ensemble_role"], 0) + 1
    return {
        "ensemble_candidate_rows": len(kept),
        "role_counts": role_counts,
        "ensemble_routing_candidates_csv": str(outdir / "ensemble_routing_candidates.csv"),
        "top": kept[:20],
    }


def font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    candidates = [
        r"C:\Windows\Fonts\arialbd.ttf" if bold else r"C:\Windows\Fonts\arial.ttf",
        r"C:\Windows\Fonts\segoeuib.ttf" if bold else r"C:\Windows\Fonts\segoeui.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size=size)
        except Exception:
            pass
    return ImageFont.load_default()


def draw_bar_chart(
    draw: ImageDraw.ImageDraw,
    box: Tuple[int, int, int, int],
    rows: List[Tuple[str, float, str]],
    title: str,
    color: Tuple[int, int, int],
) -> None:
    x0, y0, x1, y1 = box
    f_title = font(18, True)
    f_small = font(12)
    draw.text((x0, y0 - 28), title, fill=(20, 30, 40), font=f_title)
    if not rows:
        draw.text((x0, y0 + 10), "no rows", fill=(100, 100, 100), font=f_small)
        return
    max_val = max(abs(v) for _, v, _ in rows) or 1.0
    bar_h = max(14, int((y1 - y0) / max(len(rows), 1)) - 5)
    for idx, (label, value, sub) in enumerate(rows):
        y = y0 + idx * (bar_h + 5)
        w = int((abs(value) / max_val) * (x1 - x0 - 280))
        draw.rectangle([x0, y, x0 + w, y + bar_h], fill=color)
        draw.text((x0 + w + 8, y - 1), f"{value:,.2f}", fill=(30, 30, 30), font=f_small)
        draw.text((x1 - 250, y - 1), label[:35], fill=(30, 30, 30), font=f_small)
        if sub:
            draw.text((x1 - 250, y + 13), sub[:42], fill=(90, 100, 110), font=f_small)


def build_specialization_plot(outdir: Path, df: pd.DataFrame, segment_rows: List[Dict[str, Any]], spike_rows: List[Dict[str, Any]]) -> str:
    width, height = 1600, 1120
    img = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(img)
    f_title = font(30, True)
    f_sub = font(15)
    draw.text((36, 28), "Model Specialization Review", fill=(25, 25, 25), font=f_title)
    draw.text(
        (38, 66),
        f"Frozen at {utc_now().isoformat()} | rows={len(df):,} | gate-passed={int(df['gate_passed_bool'].sum()):,}",
        fill=(90, 100, 115),
        font=f_sub,
    )
    model_rows = []
    for model, g in df.groupby("model_type"):
        model_rows.append(
            (
                str(model),
                safe_float(g["estimated_total_net_pips"].max()),
                f"best AUC={safe_float(g['mean_auc'].max()):.3f}; gates={int(g['gate_passed_bool'].sum())}",
            )
        )
    model_rows.sort(key=lambda x: x[1], reverse=True)
    target_rows = []
    for target, g in df.groupby("target_family"):
        target_rows.append(
            (
                str(target),
                safe_float(g["estimated_total_net_pips"].max()),
                f"best AUC={safe_float(g['mean_auc'].max()):.3f}; n={len(g)}",
            )
        )
    target_rows.sort(key=lambda x: x[1], reverse=True)
    segment_top = [
        (
            f"{r.get('scorecard')}:{r.get('segment')}",
            safe_float(r.get("leader_score")),
            f"{r.get('model_type')} / {r.get('target')}",
        )
        for r in segment_rows[:12]
    ]
    spike_top = [
        (
            str(r.get("experiment_id", ""))[-14:],
            safe_float(r.get("proxy_total_net_pips")),
            f"{r.get('model_type')} / {r.get('target')} / AUC={safe_float(r.get('mean_auc')):.3f}",
        )
        for r in spike_rows[:12]
    ]
    draw.rounded_rectangle([30, 105, 1570, 1085], radius=18, fill=(248, 250, 252), outline=(226, 232, 240))
    draw_bar_chart(draw, (70, 155, 760, 405), model_rows[:8], "Best pip capture by model class", (8, 81, 156))
    draw_bar_chart(draw, (840, 155, 1530, 405), target_rows[:8], "Best pip capture by target family", (44, 162, 95))
    draw_bar_chart(draw, (70, 500, 760, 1015), segment_top, "Best segment specialists", (117, 107, 177))
    draw_bar_chart(draw, (840, 500, 1530, 1015), spike_top, "Spike-lead trade-sim proxy", (242, 142, 43))
    path = outdir / "model_specialization_review.png"
    img.save(path)
    return str(path)


def archive_review(outdir: Path, outputs: Dict[str, Any]) -> str:
    archive_dir = outdir / "frozen_inputs"
    archive_dir.mkdir(parents=True, exist_ok=True)
    files_to_copy = [
        LEDGER_PATH,
        RESEARCH_ROOT / "research_state.json",
        STATE_ROOT / "research_only_state.json",
        MODEL_LIFECYCLE_ROOT / "research_queue.jsonl",
        PROMOTIONS_ROOT / "technical_production.json",
        PROMOTIONS_ROOT / "research_leader.json",
        PROMOTIONS_ROOT / "shadow_candidate.json",
        PROMOTIONS_ROOT / "ensemble_shadow_candidate.json",
        PROMOTIONS_ROOT / "arima_pair_challenger_shadow.json",
        REPORTS_ROOT / "latest_model_metrics.json",
        REPORTS_ROOT / "latest_trainer_reporting_extensions.json",
        REPORTS_ROOT / "model_improvements_over_time.png",
        REPORTS_ROOT / "model_improvements_over_time_points.csv",
    ]
    copied: List[str] = []
    for src in files_to_copy:
        try:
            if src.exists():
                dst = archive_dir / src.name
                shutil.copy2(src, dst)
                copied.append(str(dst))
        except Exception:
            pass
    write_json(archive_dir / "frozen_archive_manifest.json", {"copied_files": copied, "outputs": outputs})
    zip_path = outdir.with_suffix(".zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in outdir.rglob("*"):
            if path.is_file():
                zf.write(path, path.relative_to(outdir.parent))
    return str(zip_path)


def main() -> int:
    stamp = utc_stamp()
    outdir = REPORTS_ROOT / f"model_specialization_review_{stamp}"
    outdir.mkdir(parents=True, exist_ok=True)
    df = load_ledger()
    df.to_csv(outdir / "experiment_ledger_enriched.csv", index=False)
    outputs = build_class_summaries(df, outdir)
    segment_summary, segment_rows = build_segment_leaders(outdir)
    spike_summary, spike_rows = build_spike_lead_trade_proxy(outdir)
    ensemble_summary = build_ensemble_routing_candidates(outdir, segment_rows, spike_rows)
    plot_path = build_specialization_plot(outdir, df, segment_rows, spike_rows)
    outputs.update(segment_summary)
    outputs.update(
        {
            "experiment_ledger_enriched_csv": str(outdir / "experiment_ledger_enriched.csv"),
            "specialization_plot_png": plot_path,
        }
    )
    outputs.update(spike_summary)
    outputs.update(ensemble_summary)
    summary = {
        "generated_utc": utc_now().isoformat(),
        "execution": "reporting_only_trainer_stopped_snapshot",
        "project_root": str(PROJECT_ROOT),
        "output_dir": str(outdir),
        "ledger_rows": int(len(df)),
        "experiment_json_count": len(experiment_paths()),
        "gate_passed_rows": int(df["gate_passed_bool"].sum()),
        "successful_rows": int(df["successful_bool"].sum()),
        "best_overall_pip_capture": summary_record(df, {"scope": "all"}),
        "outputs": outputs,
    }
    archive_path = archive_review(outdir, outputs)
    summary["archive_zip"] = archive_path
    write_json(outdir / "model_specialization_review_summary.json", summary)
    write_json(REPORTS_ROOT / "latest_model_specialization_review.json", summary)
    # Copy top-level convenience files.
    for name in [
        "model_class_summary.csv",
        "target_family_summary.csv",
        "model_target_class_summary.csv",
        "subset_target_class_summary.csv",
        "best_models_per_segment_top5.csv",
        "spike_lead_trade_sim_proxy.csv",
        "ensemble_routing_candidates.csv",
        "model_specialization_review.png",
    ]:
        src = outdir / name
        if src.exists():
            shutil.copy2(src, REPORTS_ROOT / f"latest_{name}")
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
