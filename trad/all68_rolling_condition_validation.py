"""Rolling-origin validation for non-USD exhaustion/reversal conditions.

Each holdout week derives its feature cutoffs and pair-specific spike threshold
from data strictly before that week.  The report separates:

1. Event classification: whether the condition concentrates future q99 moves.
2. Trade-style triggers: first threshold crossing, one signal per horizon,
   evaluated as a cost-adjusted SHORT using the stored bid/ask estimates.

The second test prevents a condition that merely describes extreme candles
from being promoted into the technical account manager without entry evidence.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from all68_weekly_missed_move_study import FEATURE_ROOT, pair_group


REPORT_ROOT = Path("data") / "all68_weekly_move_study" / "rolling_validation"
HORIZONS = (30, 60, 120)
FEATURES = (
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_30_atr",
    "strength_gap_15",
)


@dataclass(frozen=True)
class Rule:
    name: str
    left: str
    right: str


RULES = (
    Rule("m5_m30_upper", "momentum_5_atr", "momentum_30_atr"),
    Rule("m15_m30_upper", "momentum_15_atr", "momentum_30_atr"),
    Rule("m5_strength15_upper", "momentum_5_atr", "strength_gap_15"),
    Rule("m15_strength15_upper", "momentum_15_atr", "strength_gap_15"),
    Rule("m30_strength15_upper", "momentum_30_atr", "strength_gap_15"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--first-week", default="2026-05-04")
    parser.add_argument("--last-week", default="2026-06-15")
    parser.add_argument("--training-weeks", type=int, default=0)
    parser.add_argument("--feature-quantile", type=float, default=0.90)
    parser.add_argument("--output-label", default="expanding")
    return parser.parse_args()


def week_starts(first: str, last: str) -> list[pd.Timestamp]:
    first_ts = pd.Timestamp(first, tz="UTC")
    last_ts = pd.Timestamp(last, tz="UTC")
    return list(pd.date_range(first_ts, last_ts, freq="7D"))


def load_frames(horizon: int) -> dict[str, pd.DataFrame]:
    columns = [
        *FEATURES,
        f"future_move_pips_{horizon}",
        f"future_short_net_pips_{horizon}",
    ]
    frames: dict[str, pd.DataFrame] = {}
    for path in sorted(FEATURE_ROOT.glob("*.parquet")):
        if pair_group(path.stem) == "usd_related":
            continue
        frame = pd.read_parquet(path, columns=columns)
        frame.index = pd.to_datetime(frame.index, utc=True)
        frames[path.stem] = frame.replace([np.inf, -np.inf], np.nan)
    return frames


def training_slice(
    frame: pd.DataFrame,
    week_start: pd.Timestamp,
    training_weeks: int,
) -> pd.DataFrame:
    start = None
    if training_weeks > 0:
        start = week_start - pd.Timedelta(weeks=training_weeks)
    mask = frame.index < week_start
    if start is not None:
        mask &= frame.index >= start
    return frame.loc[mask]


def independent_first_triggers(
    sample: pd.DataFrame,
    mask: pd.Series,
    horizon: int,
) -> pd.DataFrame:
    """Use the first false->true crossing, then enforce a full-horizon cooldown."""
    edge = mask & ~mask.shift(1, fill_value=False)
    candidates = sample.loc[edge].sort_index()
    if candidates.empty:
        return candidates
    selected: list[Any] = []
    next_allowed: pd.Timestamp | None = None
    for timestamp in candidates.index:
        if next_allowed is None or timestamp >= next_allowed:
            selected.append(timestamp)
            next_allowed = timestamp + pd.Timedelta(minutes=horizon)
    return candidates.loc[selected]


def independent_spikes(
    sample: pd.DataFrame,
    mask: pd.Series,
    horizon: int,
    target: str,
) -> pd.DataFrame:
    spikes = sample.loc[mask & sample["spike"]].copy()
    if spikes.empty:
        return spikes
    spikes["abs_target"] = spikes[target].abs()
    selected: list[pd.Series] = []
    cluster: list[pd.Series] = []
    prior: pd.Timestamp | None = None
    for timestamp, row in spikes.sort_index().iterrows():
        if prior is None or timestamp - prior <= pd.Timedelta(minutes=horizon):
            cluster.append(row)
        else:
            selected.append(max(cluster, key=lambda item: item["abs_target"]))
            cluster = [row]
        prior = timestamp
    if cluster:
        selected.append(max(cluster, key=lambda item: item["abs_target"]))
    return pd.DataFrame(selected)


def validate_week(
    frames: dict[str, pd.DataFrame],
    horizon: int,
    week_start: pd.Timestamp,
    training_weeks: int,
    feature_quantile: float,
) -> list[dict[str, Any]]:
    week_end = week_start + pd.Timedelta(days=5)
    train_feature_parts = []
    thresholds: dict[str, float] = {}
    samples: dict[str, pd.DataFrame] = {}
    target = f"future_move_pips_{horizon}"
    short_net = f"future_short_net_pips_{horizon}"

    for pair, frame in frames.items():
        train = training_slice(frame, week_start, training_weeks)
        if len(train) < 1_000:
            continue
        threshold = float(train[target].abs().quantile(0.99))
        if not np.isfinite(threshold) or threshold <= 0:
            continue
        train_feature_parts.append(train.loc[:, FEATURES])
        week = frame.loc[
            (frame.index >= week_start) & (frame.index < week_end),
            [*FEATURES, target, short_net],
        ].dropna()
        if week.empty:
            continue
        week = week.copy()
        week["instrument"] = pair
        week["threshold"] = threshold
        week["spike"] = week[target].abs() >= threshold
        samples[pair] = week
        thresholds[pair] = threshold

    if not train_feature_parts or not samples:
        return []
    train_features = pd.concat(train_feature_parts, ignore_index=True)
    bounds = {
        feature: float(train_features[feature].quantile(feature_quantile))
        for feature in FEATURES
    }
    base_rows = sum(len(sample) for sample in samples.values())
    base_spikes = sum(int(sample["spike"].sum()) for sample in samples.values())
    base_rate = base_spikes / max(base_rows, 1)

    results: list[dict[str, Any]] = []
    for rule in RULES:
        condition_rows = 0
        condition_spikes = 0
        spike_frames = []
        trigger_frames = []
        for pair, sample in samples.items():
            mask = sample[rule.left].ge(bounds[rule.left]) & sample[
                rule.right
            ].ge(bounds[rule.right])
            condition_rows += int(mask.sum())
            condition_spikes += int((mask & sample["spike"]).sum())
            spikes = independent_spikes(sample, mask, horizon, target)
            if not spikes.empty:
                spikes = spikes.copy()
                spikes["instrument"] = pair
                spike_frames.append(spikes)
            triggers = independent_first_triggers(sample, mask, horizon)
            if not triggers.empty:
                triggers = triggers.copy()
                triggers["instrument"] = pair
                triggers["short_net_threshold_units"] = (
                    triggers[short_net] / thresholds[pair]
                )
                trigger_frames.append(triggers)

        spikes = (
            pd.concat(spike_frames, axis=0)
            if spike_frames
            else pd.DataFrame()
        )
        triggers = (
            pd.concat(trigger_frames, axis=0)
            if trigger_frames
            else pd.DataFrame()
        )
        condition_rate = condition_spikes / max(condition_rows, 1)
        results.append(
            {
                "week_start": week_start,
                "week_end": week_end,
                "horizon_minutes": horizon,
                "rule": rule.name,
                "left_feature": rule.left,
                "right_feature": rule.right,
                "left_bound": bounds[rule.left],
                "right_bound": bounds[rule.right],
                "base_rows": base_rows,
                "base_spikes": base_spikes,
                "base_spike_rate": base_rate,
                "condition_rows": condition_rows,
                "condition_spikes": condition_spikes,
                "condition_spike_rate": condition_rate,
                "lift": condition_rate / max(base_rate, 1e-12),
                "independent_spikes": len(spikes),
                "spike_pairs": (
                    int(spikes["instrument"].nunique())
                    if not spikes.empty
                    else 0
                ),
                "spike_short_direction_accuracy": (
                    float((spikes[target] < 0).mean())
                    if not spikes.empty
                    else np.nan
                ),
                "trigger_count": len(triggers),
                "trigger_pairs": (
                    int(triggers["instrument"].nunique())
                    if not triggers.empty
                    else 0
                ),
                "trigger_short_win_rate": (
                    float((triggers[short_net] > 0).mean())
                    if not triggers.empty
                    else np.nan
                ),
                "trigger_short_mean_threshold_units": (
                    float(triggers["short_net_threshold_units"].mean())
                    if not triggers.empty
                    else np.nan
                ),
                "trigger_short_median_threshold_units": (
                    float(triggers["short_net_threshold_units"].median())
                    if not triggers.empty
                    else np.nan
                ),
                "trigger_short_total_threshold_units": (
                    float(triggers["short_net_threshold_units"].sum())
                    if not triggers.empty
                    else 0.0
                ),
            }
        )
    return results


def aggregate(weekly: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (horizon, rule), group in weekly.groupby(
        ["horizon_minutes", "rule"], sort=False
    ):
        pooled_spike_rate = group["condition_spikes"].sum() / max(
            group["condition_rows"].sum(), 1
        )
        pooled_base_rate = group["base_spikes"].sum() / max(
            group["base_rows"].sum(), 1
        )
        weighted_direction = np.average(
            group["spike_short_direction_accuracy"].fillna(0.0),
            weights=group["independent_spikes"].clip(lower=0),
        )
        trigger_count = int(group["trigger_count"].sum())
        weighted_win_rate = np.average(
            group["trigger_short_win_rate"].fillna(0.0),
            weights=group["trigger_count"].clip(lower=0),
        )
        total_units = float(group["trigger_short_total_threshold_units"].sum())
        rows.append(
            {
                "horizon_minutes": horizon,
                "rule": rule,
                "weeks": len(group),
                "weeks_lift_above_1": int((group["lift"] > 1.0).sum()),
                "weeks_direction_above_55pct": int(
                    (group["spike_short_direction_accuracy"] >= 0.55).sum()
                ),
                "weeks_positive_trigger_expectancy": int(
                    (group["trigger_short_mean_threshold_units"] > 0).sum()
                ),
                "median_lift": float(group["lift"].median()),
                "minimum_lift": float(group["lift"].min()),
                "pooled_lift": pooled_spike_rate / max(pooled_base_rate, 1e-12),
                "independent_spikes": int(group["independent_spikes"].sum()),
                "spike_short_direction_accuracy": float(weighted_direction),
                "trigger_count": trigger_count,
                "trigger_short_win_rate": float(weighted_win_rate),
                "trigger_short_mean_threshold_units": (
                    total_units / max(trigger_count, 1)
                ),
                "trigger_short_total_threshold_units": total_units,
                "median_left_bound": float(group["left_bound"].median()),
                "median_right_bound": float(group["right_bound"].median()),
            }
        )
    output = pd.DataFrame(rows)
    output["stable_for_shadow"] = (
        (output["weeks"] >= 6)
        & (output["weeks_lift_above_1"] >= output["weeks"] - 2)
        & (output["median_lift"] >= 1.15)
        & (output["independent_spikes"] >= 30)
        & (output["spike_short_direction_accuracy"] >= 0.60)
    )
    output["stable_for_execution"] = (
        output["stable_for_shadow"]
        & (
            output["weeks_positive_trigger_expectancy"]
            >= output["weeks"] - 2
        )
        & (output["trigger_count"] >= 100)
        & (output["trigger_short_win_rate"] >= 0.52)
        & (output["trigger_short_mean_threshold_units"] > 0)
    )
    return output.sort_values(
        [
            "stable_for_execution",
            "stable_for_shadow",
            "trigger_short_mean_threshold_units",
            "pooled_lift",
        ],
        ascending=[False, False, False, False],
    )


def render_report(
    weekly: pd.DataFrame,
    summary: pd.DataFrame,
    args: argparse.Namespace,
) -> str:
    lines = [
        "# All-68 Rolling Weekly Condition Validation",
        "",
        (
            f"Holdouts: `{weekly.week_start.min().isoformat()}` through "
            f"`{weekly.week_end.max().isoformat()}`"
        ),
        "",
        "All feature cutoffs and pair-specific q99 move thresholds use only data before each holdout week.",
        "",
        "## Aggregate",
        "",
        "| Horizon | Rule | Lift weeks | Median/pooled lift | Spikes | Short direction | Triggers | Win rate | Mean q99 units | Shadow | Execute |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for _, row in summary.iterrows():
        lines.append(
            f"| {int(row.horizon_minutes)}m | {row.rule} | "
            f"{int(row.weeks_lift_above_1)}/{int(row.weeks)} | "
            f"{row.median_lift:.2f}/{row.pooled_lift:.2f} | "
            f"{int(row.independent_spikes)} | "
            f"{row.spike_short_direction_accuracy:.1%} | "
            f"{int(row.trigger_count)} | {row.trigger_short_win_rate:.1%} | "
            f"{row.trigger_short_mean_threshold_units:.4f} | "
            f"{bool(row.stable_for_shadow)} | "
            f"{bool(row.stable_for_execution)} |"
        )
    lines += [
        "",
        "## Weekly Detail",
        "",
        "| Week | Horizon | Rule | Lift | Spikes | Short direction | Triggers | Win rate | Mean q99 units |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in weekly.sort_values(
        ["week_start", "horizon_minutes", "rule"]
    ).iterrows():
        lines.append(
            f"| {row.week_start.date()} | {int(row.horizon_minutes)}m | "
            f"{row.rule} | {row.lift:.2f} | "
            f"{int(row.independent_spikes)} | "
            f"{row.spike_short_direction_accuracy:.1%} | "
            f"{int(row.trigger_count)} | "
            f"{row.trigger_short_win_rate:.1%} | "
            f"{row.trigger_short_mean_threshold_units:.4f} |"
        )
    lines += [
        "",
        "## Promotion Policy",
        "",
        "- Shadow requires stable spike concentration and reversal direction across weeks.",
        "- Execution additionally requires positive cost-adjusted first-trigger expectancy across most weeks.",
        "- A rule that passes shadow but not execution is logged only and cannot create an order.",
        f"- Feature quantile: `{args.feature_quantile:.2f}`.",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    weeks = week_starts(args.first_week, args.last_week)
    all_rows: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        print(f"[load] horizon={horizon}", flush=True)
        frames = load_frames(horizon)
        for number, week_start in enumerate(weeks, 1):
            print(
                f"[validate] horizon={horizon} week={number}/{len(weeks)} "
                f"{week_start.date()}",
                flush=True,
            )
            all_rows.extend(
                validate_week(
                    frames,
                    horizon,
                    week_start,
                    args.training_weeks,
                    args.feature_quantile,
                )
            )
    weekly = pd.DataFrame(all_rows)
    summary = aggregate(weekly)
    report_root = REPORT_ROOT / args.output_label
    report_root.mkdir(parents=True, exist_ok=True)
    weekly.to_csv(report_root / "weekly_results.csv", index=False)
    summary.to_csv(report_root / "aggregate_results.csv", index=False)
    report = render_report(weekly, summary, args)
    (report_root / "report.md").write_text(report, encoding="utf-8")
    payload = {
        "first_week": args.first_week,
        "last_week": args.last_week,
        "weeks": len(weeks),
        "training_weeks": args.training_weeks,
        "feature_quantile": args.feature_quantile,
        "stable_for_shadow": summary.loc[
            summary["stable_for_shadow"],
            ["horizon_minutes", "rule"],
        ].to_dict("records"),
        "stable_for_execution": summary.loc[
            summary["stable_for_execution"],
            ["horizon_minutes", "rule"],
        ].to_dict("records"),
    }
    (report_root / "summary.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
    print(json.dumps(payload, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
