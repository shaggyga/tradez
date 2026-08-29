#!/usr/bin/env python3
"""Value-weighted subset report for missed spike-scout replays.

This is a research/reporting utility.  It reads detail CSVs produced by
``oanda_spike_missed_move_backtest.py`` and ranks subsets of missed events by:

- observed missed value,
- favorable excursion/oracle value,
- executable fixed/trailing exit proxies,
- reason/category/theme/pair concentration.

The purpose is to avoid promoting aggregate replay results.  If the aggregate
miss population is negative, only stable positive subsets should drive the next
training queue or live-gate discussion.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parent
REPORT_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_REPORT = REPORT_ROOT / "latest_missed_spike_subset_report.json"
DEFAULT_CSV = REPORT_ROOT / "latest_missed_spike_subset_report.csv"

EXOTIC_OR_VOL_QUOTES = {
    "TRY",
    "ZAR",
    "MXN",
    "CZK",
    "HUF",
    "PLN",
    "NOK",
    "SEK",
    "HKD",
    "THB",
    "CNH",
}
MAJOR_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"}

GROUP_SPECS: dict[str, tuple[str, ...]] = {
    "delay": ("delay_minutes",),
    "instrument": ("instrument",),
    "direction": ("direction",),
    "theme": ("theme",),
    "miss_category": ("miss_category",),
    "reason_bucket": ("reason_bucket",),
    "quote": ("quote",),
    "pair_family": ("pair_family",),
    "instrument_direction": ("instrument", "direction"),
    "instrument_theme": ("instrument", "theme"),
    "instrument_reason_bucket": ("instrument", "reason_bucket"),
    "theme_direction": ("theme", "direction"),
    "category_theme": ("miss_category", "theme"),
    "quote_theme": ("quote", "theme"),
    "delay_instrument_direction": ("delay_minutes", "instrument", "direction"),
}


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def positive_rate(values: Iterable[float]) -> float:
    values = list(values)
    if not values:
        return 0.0
    return sum(1 for value in values if value > 0) / len(values)


def mean(values: Iterable[float]) -> float:
    values = list(values)
    if not values:
        return 0.0
    return sum(values) / len(values)


def parse_delay_minutes(path: Path) -> str:
    match = re.search(r"_delay(\d+)m", path.stem)
    return match.group(1) if match else ""


def split_pair(instrument: str) -> tuple[str, str]:
    parts = str(instrument or "").upper().split("_")
    if len(parts) != 2:
        return "", ""
    return parts[0], parts[1]


def pair_family(instrument: str) -> str:
    base, quote = split_pair(instrument)
    if not base or not quote:
        return "unknown"
    if quote in EXOTIC_OR_VOL_QUOTES or base in EXOTIC_OR_VOL_QUOTES:
        return "volatile_or_exotic"
    if base == "USD" or quote == "USD":
        return "usd_major_cross"
    if "JPY" in {base, quote}:
        return "jpy_cross"
    if base in MAJOR_CURRENCIES and quote in MAJOR_CURRENCIES:
        return "major_cross"
    return "other"


def reason_bucket(row: dict[str, Any]) -> str:
    text = " ".join(
        str(row.get(key) or "").lower()
        for key in ("miss_reason", "row_reason", "row_status", "miss_category")
    )
    if "production model rejected" in text or "segment gate rejected" in text:
        return "model_rejected"
    if "approved no signals" in text:
        return "approved_no_signal"
    if "too old" in text or "late" in text or "continuation age" in text:
        return "stale_or_late"
    if "move/spread" in text or "spread" in text or "slippage" in text or "bounds" in text:
        return "spread_or_bounds"
    if "campaign" in text or "repeat" in text:
        return "campaign_repeat"
    if "risk" in text or "margin" in text:
        return "risk_or_margin"
    category = str(row.get("miss_category") or "").strip()
    return category or "other"


def exit_metric_columns(fieldnames: list[str]) -> list[str]:
    metrics = []
    for name in fieldnames:
        if not name.startswith("fresh_") or not name.endswith("_usd"):
            continue
        if "_final_" in name or "_trail_" in name:
            metrics.append(name)
    return sorted(metrics)


def normalize_row(row: dict[str, Any], delay_minutes: str) -> dict[str, Any]:
    instrument = str(row.get("instrument") or "").strip().upper()
    base, quote = split_pair(instrument)
    out = dict(row)
    out["instrument"] = instrument
    out["direction"] = str(row.get("direction") or "").strip().upper()
    out["theme"] = str(row.get("theme") or "").strip() or "unknown"
    out["miss_category"] = str(row.get("miss_category") or "").strip() or "unknown"
    out["delay_minutes"] = delay_minutes
    out["base"] = base
    out["quote"] = quote
    out["pair_family"] = pair_family(instrument)
    out["reason_bucket"] = reason_bucket(row)
    return out


def summarize_group(
    group_name: str,
    group_fields: tuple[str, ...],
    group_key: tuple[str, ...],
    rows: list[dict[str, Any]],
    exit_metrics: list[str],
) -> dict[str, Any]:
    observed_abs_usd = [safe_float(row.get("observed_abs_usd")) for row in rows]
    observed_net_usd = [safe_float(row.get("observed_net_usd")) for row in rows]
    observed_abs_pips = [safe_float(row.get("observed_abs_pips")) for row in rows]
    cost_adjusted_pips = [safe_float(row.get("cost_adjusted_net_pips")) for row in rows]
    ratios = [safe_float(row.get("move_to_spread_ratio"), float("nan")) for row in rows]
    ratios = [value for value in ratios if math.isfinite(value)]
    ages = [safe_float(row.get("event_age_minutes"), float("nan")) for row in rows]
    ages = [value for value in ages if math.isfinite(value)]

    mfe60 = [safe_float(row.get("fresh_mfe_60m_usd")) for row in rows]
    mfe120 = [safe_float(row.get("fresh_mfe_120m_usd")) for row in rows]
    final30 = [safe_float(row.get("fresh_final_30m_usd")) for row in rows]
    final60 = [safe_float(row.get("fresh_final_60m_usd")) for row in rows]
    final120 = [safe_float(row.get("fresh_final_120m_usd")) for row in rows]

    best_exit_metric = ""
    best_exit_sum = float("-inf")
    best_exit_values: list[float] = []
    for metric in exit_metrics:
        values = [safe_float(row.get(metric)) for row in rows]
        metric_sum = sum(values)
        if metric_sum > best_exit_sum:
            best_exit_sum = metric_sum
            best_exit_metric = metric
            best_exit_values = values
    if best_exit_sum == float("-inf"):
        best_exit_sum = 0.0

    observed_abs_sum = sum(observed_abs_usd)
    return {
        "group": group_name,
        "key": "|".join(group_key),
        "fields": ",".join(group_fields),
        "rows": len(rows),
        "observed_abs_usd_sum": observed_abs_sum,
        "observed_net_usd_sum": sum(observed_net_usd),
        "observed_abs_pips_sum": sum(observed_abs_pips),
        "cost_adjusted_net_pips_sum": sum(cost_adjusted_pips),
        "move_to_spread_ratio_mean": mean(ratios),
        "event_age_minutes_mean": mean(ages),
        "fresh_mfe_60m_usd_sum": sum(mfe60),
        "fresh_mfe_60m_pos_rate": positive_rate(mfe60),
        "fresh_mfe_120m_usd_sum": sum(mfe120),
        "fresh_mfe_120m_pos_rate": positive_rate(mfe120),
        "fresh_final_30m_usd_sum": sum(final30),
        "fresh_final_30m_pos_rate": positive_rate(final30),
        "fresh_final_60m_usd_sum": sum(final60),
        "fresh_final_60m_pos_rate": positive_rate(final60),
        "fresh_final_120m_usd_sum": sum(final120),
        "fresh_final_120m_pos_rate": positive_rate(final120),
        "best_exit_metric": best_exit_metric,
        "best_exit_usd_sum": best_exit_sum,
        "best_exit_usd_mean": mean(best_exit_values),
        "best_exit_pos_rate": positive_rate(best_exit_values),
        "best_exit_to_observed_abs_ratio": (
            best_exit_sum / observed_abs_sum if abs(observed_abs_sum) > 1e-12 else 0.0
        ),
        "mfe120_to_observed_abs_ratio": (
            sum(mfe120) / observed_abs_sum if abs(observed_abs_sum) > 1e-12 else 0.0
        ),
    }


def top_instruments(rows: list[dict[str, Any]], limit: int = 5) -> str:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row.get("instrument") or "")] += 1
    return ",".join(
        f"{instrument}:{count}"
        for instrument, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
        if instrument
    )


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    inputs = [path for path in args.inputs if path.exists() and path.stat().st_size > 0]
    if not inputs:
        # Prefer tight-trail partials; fall back to standard delay partials.
        inputs = sorted(REPORT_ROOT.glob("latest_spike_missed_tight_trail_compare_delay*m.csv"))
        if not inputs:
            inputs = sorted(REPORT_ROOT.glob("latest_spike_missed_delay_compare_delay*m.csv"))

    all_rows: list[dict[str, Any]] = []
    all_exit_metrics: set[str] = set()
    source_files: list[str] = []
    for path in inputs:
        delay = parse_delay_minutes(path)
        source_files.append(str(path))
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            all_exit_metrics.update(exit_metric_columns(fieldnames))
            for row in reader:
                if str(row.get("fresh_available") or "").strip().lower() not in {"true", "1", "yes"}:
                    continue
                all_rows.append(normalize_row(row, delay))

    exit_metrics = sorted(all_exit_metrics)
    grouped: dict[tuple[str, tuple[str, ...]], list[dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        for group_name, fields in GROUP_SPECS.items():
            key = tuple(str(row.get(field) or "unknown") for field in fields)
            grouped[(group_name, key)].append(row)

    group_rows: list[dict[str, Any]] = []
    for (group_name, key), rows in grouped.items():
        if len(rows) < args.min_rows:
            continue
        fields = GROUP_SPECS[group_name]
        summary = summarize_group(group_name, fields, key, rows, exit_metrics)
        summary["top_instruments"] = top_instruments(rows)
        group_rows.append(summary)

    group_rows.sort(
        key=lambda row: (
            safe_float(row.get("best_exit_usd_sum")),
            safe_float(row.get("fresh_mfe_120m_usd_sum")),
            safe_float(row.get("observed_abs_usd_sum")),
        ),
        reverse=True,
    )

    positive_subsets = [
        row
        for row in group_rows
        if safe_float(row.get("best_exit_usd_sum")) > 0
        and safe_float(row.get("best_exit_pos_rate")) >= args.min_positive_rate
    ]
    delay_specific_positive_subsets = [
        row
        for row in positive_subsets
        if "delay_minutes" in str(row.get("fields") or "").split(",")
    ]
    oracle_subsets = sorted(
        group_rows,
        key=lambda row: safe_float(row.get("fresh_mfe_120m_usd_sum")),
        reverse=True,
    )
    value_subsets = sorted(
        group_rows,
        key=lambda row: safe_float(row.get("observed_abs_usd_sum")),
        reverse=True,
    )
    worst_exit_subsets = sorted(group_rows, key=lambda row: safe_float(row.get("best_exit_usd_sum")))
    unique_event_keys = {
        "|".join(
            [
                str(row.get("event_key") or ""),
                str(row.get("event_start_utc") or ""),
                str(row.get("event_end_utc") or ""),
                str(row.get("instrument") or ""),
                str(row.get("direction") or ""),
            ]
        )
        for row in all_rows
    }

    report = {
        "generated_utc": utc_iso(),
        "source_files": source_files,
        "row_count": len(all_rows),
        "unique_event_count": len(unique_event_keys),
        "delay_minutes": sorted({row.get("delay_minutes", "") for row in all_rows if row.get("delay_minutes")}),
        "min_rows": args.min_rows,
        "min_positive_rate": args.min_positive_rate,
        "exit_metric_count": len(exit_metrics),
        "group_count": len(group_rows),
        "global": summarize_group("all", tuple(), ("all",), all_rows, exit_metrics) if all_rows else {},
        "top_delay_specific_positive_subsets": delay_specific_positive_subsets[: args.top],
        "top_positive_subsets": positive_subsets[: args.top],
        "top_oracle_mfe_subsets": oracle_subsets[: args.top],
        "top_observed_value_subsets": value_subsets[: args.top],
        "worst_best_exit_subsets": worst_exit_subsets[: args.top],
        "grouped_csv": str(args.csv_report),
        "caveat": (
            "Subset rankings are exploratory and can overfit.  Treat positive subsets as training "
            "queue candidates, not live promotion evidence, until validated on held-out days/weeks. "
            "When multiple delay files are supplied, row_count is scenario rows and can include the "
            "same event under several alternate entry delays; use delay-specific subsets for action."
        ),
    }

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    args.csv_report.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "group",
        "key",
        "fields",
        "rows",
        "top_instruments",
        "observed_abs_usd_sum",
        "observed_net_usd_sum",
        "observed_abs_pips_sum",
        "cost_adjusted_net_pips_sum",
        "move_to_spread_ratio_mean",
        "event_age_minutes_mean",
        "fresh_mfe_60m_usd_sum",
        "fresh_mfe_60m_pos_rate",
        "fresh_mfe_120m_usd_sum",
        "fresh_mfe_120m_pos_rate",
        "fresh_final_30m_usd_sum",
        "fresh_final_30m_pos_rate",
        "fresh_final_60m_usd_sum",
        "fresh_final_60m_pos_rate",
        "fresh_final_120m_usd_sum",
        "fresh_final_120m_pos_rate",
        "best_exit_metric",
        "best_exit_usd_sum",
        "best_exit_usd_mean",
        "best_exit_pos_rate",
        "best_exit_to_observed_abs_ratio",
        "mfe120_to_observed_abs_ratio",
    ]
    with args.csv_report.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in group_rows:
            writer.writerow(row)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="*", type=Path)
    parser.add_argument("--min-rows", type=int, default=10)
    parser.add_argument("--min-positive-rate", type=float, default=0.45)
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--csv-report", type=Path, default=DEFAULT_CSV)
    args = parser.parse_args()
    report = build_report(args)
    print(
        json.dumps(
            {
                "generated_utc": report["generated_utc"],
                "row_count": report["row_count"],
                "unique_event_count": report["unique_event_count"],
                "group_count": report["group_count"],
                "delay_specific_positive_subset_count": len(report["top_delay_specific_positive_subsets"]),
                "positive_subset_count": len(report["top_positive_subsets"]),
                "source_files": report["source_files"],
                "report": str(args.report),
                "csv_report": str(args.csv_report),
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
