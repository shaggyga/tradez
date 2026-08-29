#!/usr/bin/env python3
"""Rank missed spike-scout replay subsets by capture gap.

This is a read-only research/reporting utility.  It reads detail CSVs produced
by ``oanda_spike_missed_move_backtest.py`` and answers a narrower question than
the aggregate replay:

    "Which subsets had real favorable excursion, and how much did fixed/trailing
    exits actually capture versus leave on the table?"

The report deliberately treats "best exit" as diagnostic/oracle-style evidence.
It is not a promotion artifact and it does not touch broker state.
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
DEFAULT_JSON = REPORT_ROOT / "latest_missed_spike_capture_gap_report.json"
DEFAULT_CSV = REPORT_ROOT / "latest_missed_spike_capture_gap_report.csv"
DEFAULT_MD = REPORT_ROOT / "latest_missed_spike_capture_gap_report.md"

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
    "pair_family": ("pair_family",),
    "quote": ("quote",),
    "instrument_direction": ("instrument", "direction"),
    "instrument_theme": ("instrument", "theme"),
    "theme_direction": ("theme", "direction"),
    "delay_instrument_direction": ("delay_minutes", "instrument", "direction"),
    "delay_theme_direction": ("delay_minutes", "theme", "direction"),
    "delay_family_direction": ("delay_minutes", "pair_family", "direction"),
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
    return sum(1 for value in values if value > 0.0) / len(values)


def split_pair(instrument: str) -> tuple[str, str]:
    parts = str(instrument or "").upper().replace("/", "_").split("_")
    if len(parts) != 2:
        return "", ""
    return parts[0], parts[1]


def pair_family(instrument: str) -> str:
    base, quote = split_pair(instrument)
    if not base or not quote:
        return "unknown"
    if base in EXOTIC_OR_VOL_QUOTES or quote in EXOTIC_OR_VOL_QUOTES:
        return "volatile_or_exotic"
    if base == "USD" or quote == "USD":
        return "usd_major_cross"
    if "JPY" in {base, quote}:
        return "jpy_cross"
    if base in MAJOR_CURRENCIES and quote in MAJOR_CURRENCIES:
        return "major_cross"
    return "other_cross"


def reason_bucket(row: dict[str, Any]) -> str:
    text = " ".join(
        str(row.get(key) or "").lower()
        for key in ("miss_category", "miss_reason", "row_reason", "row_status")
    )
    if "margin" in text:
        return "margin_cap"
    if "spread" in text or "ratio" in text:
        return "spread_or_ratio"
    if "stale" in text or "old" in text or "late" in text:
        return "stale_or_late"
    if "risk" in text:
        return "risk_gate"
    if "campaign" in text or "churn" in text:
        return "campaign_or_churn"
    if "opposite" in text:
        return "existing_opposite"
    return "other"


def parse_delay_minutes(path: Path) -> str:
    match = re.search(r"_delay(\d+)m", path.stem)
    if match:
        return match.group(1)
    return ""


def candidate_input_files(patterns: list[str]) -> list[Path]:
    files: list[Path] = []
    for pattern in patterns:
        path = Path(pattern)
        if not path.is_absolute():
            path = REPORT_ROOT / pattern
        matches = sorted(path.parent.glob(path.name))
        files.extend(match for match in matches if match.exists() and match.is_file())
    # Avoid the aggregate comparison CSV; it has one row per delay, not one row per event.
    files = [
        path
        for path in files
        if not path.name.endswith("_delay_compare.csv")
        and "latest_spike_missed_delay_compare.csv" not in path.name
    ]
    unique: dict[str, Path] = {str(path.resolve()).lower(): path for path in files}
    return sorted(unique.values(), key=lambda path: path.stat().st_mtime, reverse=True)


def read_rows(paths: list[Path], limit_files: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths[: max(1, limit_files)]:
        delay = parse_delay_minutes(path)
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw in reader:
                row = dict(raw)
                row["source_file"] = str(path)
                row["delay_minutes"] = delay or str(row.get("delay_minutes") or "base")
                instrument = str(row.get("instrument") or "").upper().replace("/", "_")
                row["instrument"] = instrument
                base, quote = split_pair(instrument)
                row["base"] = base
                row["quote"] = quote
                row["pair_family"] = pair_family(instrument)
                row["direction"] = str(row.get("direction") or "").upper()
                row["theme"] = str(row.get("theme") or "unknown")
                row["miss_category"] = str(row.get("miss_category") or "unknown")
                row["reason_bucket"] = reason_bucket(row)
                rows.append(enrich_metrics(row))
    return rows


def numeric_columns(row: dict[str, Any], *, prefix: str, suffix: str) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for key, value in row.items():
        if key.startswith(prefix) and key.endswith(suffix):
            out.append((key, safe_float(value, float("nan"))))
    return [(key, value) for key, value in out if math.isfinite(value)]


def enrich_metrics(row: dict[str, Any]) -> dict[str, Any]:
    final_exits = numeric_columns(row, prefix="fresh_final_", suffix="_usd")
    trail_exits = numeric_columns(row, prefix="fresh_trail_", suffix="_usd")
    all_exits = final_exits + trail_exits
    best_exit_key, best_exit_usd = max(all_exits, key=lambda item: item[1], default=("", 0.0))
    best_trail_key, best_trail_usd = max(trail_exits, key=lambda item: item[1], default=("", 0.0))
    mfe60 = safe_float(row.get("fresh_mfe_60m_usd"), 0.0)
    mfe120 = safe_float(row.get("fresh_mfe_120m_usd"), 0.0)
    final60 = safe_float(row.get("fresh_final_60m_usd"), 0.0)
    observed = abs(safe_float(row.get("observed_abs_usd"), 0.0))
    row["observed_abs_usd_num"] = observed
    row["fresh_final_60m_usd_num"] = final60
    row["fresh_mfe_60m_usd_num"] = mfe60
    row["fresh_mfe_120m_usd_num"] = mfe120
    row["best_exit_usd"] = best_exit_usd
    row["best_exit_key"] = best_exit_key
    row["best_trail_usd"] = best_trail_usd
    row["best_trail_key"] = best_trail_key
    row["capture_gap_60m_usd"] = mfe60 - best_exit_usd
    row["final_gap_60m_usd"] = mfe60 - final60
    row["best_exit_to_mfe60"] = best_exit_usd / mfe60 if abs(mfe60) > 1e-12 else 0.0
    row["best_exit_to_observed"] = best_exit_usd / observed if abs(observed) > 1e-12 else 0.0
    return row


def group_key(row: dict[str, Any], fields: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(str(row.get(field) or "") for field in fields)


def summarize_values(values: list[float]) -> dict[str, float]:
    if not values:
        return {"sum": 0.0, "mean": 0.0, "positive_rate": 0.0}
    return {
        "sum": round(sum(values), 6),
        "mean": round(sum(values) / len(values), 6),
        "positive_rate": round(positive_rate(values), 4),
    }


def summarize_group(group: str, key: tuple[str, ...], rows: list[dict[str, Any]]) -> dict[str, Any]:
    instruments = sorted({str(row.get("instrument") or "") for row in rows if row.get("instrument")})
    best_exit_keys: defaultdict[str, int] = defaultdict(int)
    for row in rows:
        if row.get("best_exit_key"):
            best_exit_keys[str(row.get("best_exit_key"))] += 1
    observed = [safe_float(row.get("observed_abs_usd_num")) for row in rows]
    final60 = [safe_float(row.get("fresh_final_60m_usd_num")) for row in rows]
    mfe60 = [safe_float(row.get("fresh_mfe_60m_usd_num")) for row in rows]
    mfe120 = [safe_float(row.get("fresh_mfe_120m_usd_num")) for row in rows]
    best_exit = [safe_float(row.get("best_exit_usd")) for row in rows]
    best_trail = [safe_float(row.get("best_trail_usd")) for row in rows]
    gap60 = [safe_float(row.get("capture_gap_60m_usd")) for row in rows]
    final_gap60 = [safe_float(row.get("final_gap_60m_usd")) for row in rows]
    observed_sum = sum(observed)
    mfe60_sum = sum(mfe60)
    best_exit_sum = sum(best_exit)
    return {
        "group": group,
        "key": "|".join(key),
        "count": len(rows),
        "instrument_count": len(instruments),
        "sample_instruments": ",".join(instruments[:8]),
        "observed_abs_usd_sum": round(observed_sum, 6),
        "fresh_final_60m_usd_sum": round(sum(final60), 6),
        "fresh_final_60m_pos_rate": round(positive_rate(final60), 4),
        "fresh_mfe_60m_usd_sum": round(mfe60_sum, 6),
        "fresh_mfe_60m_pos_rate": round(positive_rate(mfe60), 4),
        "fresh_mfe_120m_usd_sum": round(sum(mfe120), 6),
        "best_exit_usd_sum": round(best_exit_sum, 6),
        "best_exit_usd_mean": round(best_exit_sum / max(1, len(rows)), 6),
        "best_exit_pos_rate": round(positive_rate(best_exit), 4),
        "best_trail_usd_sum": round(sum(best_trail), 6),
        "capture_gap_60m_usd_sum": round(sum(gap60), 6),
        "fixed_exit_gap_60m_usd_sum": round(sum(final_gap60), 6),
        "best_exit_to_mfe60_ratio": round(best_exit_sum / mfe60_sum, 6) if abs(mfe60_sum) > 1e-12 else 0.0,
        "best_exit_to_observed_ratio": round(best_exit_sum / observed_sum, 6) if abs(observed_sum) > 1e-12 else 0.0,
        "top_exit_key": max(best_exit_keys.items(), key=lambda item: item[1])[0] if best_exit_keys else "",
        "top_exit_key_count": max(best_exit_keys.values()) if best_exit_keys else 0,
        "observed_abs_usd": summarize_values(observed),
        "best_exit_usd": summarize_values(best_exit),
        "fresh_mfe_60m_usd": summarize_values(mfe60),
    }


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    patterns = args.input_glob or ["latest_spike_missed_delay_compare_delay*m.csv"]
    files = candidate_input_files(patterns)
    if not files:
        files = candidate_input_files(["latest_spike_missed_move_backtest_trades.csv"])
    rows = read_rows(files, args.limit_files)
    grouped_rows: list[dict[str, Any]] = []
    for group, fields in GROUP_SPECS.items():
        buckets: defaultdict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            key = group_key(row, fields)
            if any(part == "" for part in key):
                continue
            buckets[key].append(row)
        for key, bucket in buckets.items():
            if len(bucket) >= args.min_group_count:
                grouped_rows.append(summarize_group(group, key, bucket))

    top_positive = sorted(
        [
            row
            for row in grouped_rows
            if row["count"] >= args.min_group_count
            and row["best_exit_usd_sum"] > 0
            and row["best_exit_pos_rate"] >= args.min_positive_rate
        ],
        key=lambda row: (
            row["best_exit_usd_sum"],
            row["best_exit_pos_rate"],
            row["fresh_mfe_60m_usd_sum"],
        ),
        reverse=True,
    )
    top_gap = sorted(
        grouped_rows,
        key=lambda row: (
            row["capture_gap_60m_usd_sum"],
            row["fresh_mfe_60m_usd_sum"],
            row["observed_abs_usd_sum"],
        ),
        reverse=True,
    )
    top_observed = sorted(
        grouped_rows,
        key=lambda row: (row["observed_abs_usd_sum"], row["fresh_mfe_60m_usd_sum"]),
        reverse=True,
    )
    top_retention = sorted(
        [
            row
            for row in grouped_rows
            if row["count"] >= args.min_group_count
            and row["fresh_mfe_60m_usd_sum"] > 0
        ],
        key=lambda row: (row["best_exit_to_mfe60_ratio"], row["best_exit_usd_sum"]),
        reverse=True,
    )
    total_observed = sum(safe_float(row.get("observed_abs_usd_num")) for row in rows)
    total_mfe60 = sum(safe_float(row.get("fresh_mfe_60m_usd_num")) for row in rows)
    total_final60 = sum(safe_float(row.get("fresh_final_60m_usd_num")) for row in rows)
    total_best_exit = sum(safe_float(row.get("best_exit_usd")) for row in rows)
    report = {
        "generated_utc": utc_iso(),
        "execution": "reporting_only_no_broker_or_promotion_changes",
        "input_files": [str(path) for path in files[: args.limit_files]],
        "row_count": len(rows),
        "group_count": len(grouped_rows),
        "min_group_count": args.min_group_count,
        "min_positive_rate": args.min_positive_rate,
        "aggregate": {
            "observed_abs_usd_sum": round(total_observed, 6),
            "fresh_mfe_60m_usd_sum": round(total_mfe60, 6),
            "fresh_final_60m_usd_sum": round(total_final60, 6),
            "best_exit_usd_sum": round(total_best_exit, 6),
            "capture_gap_60m_usd_sum": round(total_mfe60 - total_best_exit, 6),
            "fixed_exit_gap_60m_usd_sum": round(total_mfe60 - total_final60, 6),
            "best_exit_to_mfe60_ratio": round(total_best_exit / total_mfe60, 6)
            if abs(total_mfe60) > 1e-12
            else 0.0,
            "best_exit_to_observed_ratio": round(total_best_exit / total_observed, 6)
            if abs(total_observed) > 1e-12
            else 0.0,
        },
        "top_positive_capture_groups": top_positive[: args.top],
        "top_capture_gap_groups": top_gap[: args.top],
        "top_observed_value_groups": top_observed[: args.top],
        "top_retention_groups": top_retention[: args.top],
        "output_csv": str(args.csv),
        "output_markdown": str(args.markdown),
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    write_csv(args.csv, grouped_rows)
    write_markdown(args.markdown, report)
    return report


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "group",
        "key",
        "count",
        "instrument_count",
        "sample_instruments",
        "observed_abs_usd_sum",
        "fresh_mfe_60m_usd_sum",
        "fresh_final_60m_usd_sum",
        "best_exit_usd_sum",
        "best_exit_pos_rate",
        "best_trail_usd_sum",
        "capture_gap_60m_usd_sum",
        "fixed_exit_gap_60m_usd_sum",
        "best_exit_to_mfe60_ratio",
        "best_exit_to_observed_ratio",
        "top_exit_key",
        "top_exit_key_count",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(rows, key=lambda item: item.get("best_exit_usd_sum", 0), reverse=True):
            writer.writerow(row)


def row_line(row: dict[str, Any]) -> str:
    return (
        f"- {row.get('group')} `{row.get('key')}`: "
        f"n={row.get('count')} best=${row.get('best_exit_usd_sum'):.3f} "
        f"pos={row.get('best_exit_pos_rate'):.0%} "
        f"mfe60=${row.get('fresh_mfe_60m_usd_sum'):.3f} "
        f"gap=${row.get('capture_gap_60m_usd_sum'):.3f} "
        f"exit={row.get('top_exit_key')}"
    )


def write_markdown(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    aggregate = report.get("aggregate") if isinstance(report.get("aggregate"), dict) else {}
    lines = [
        "# Missed spike capture-gap report",
        "",
        f"- Generated UTC: {report.get('generated_utc')}",
        f"- Execution: {report.get('execution')}",
        f"- Rows: {report.get('row_count')} | groups: {report.get('group_count')}",
        (
            "- Aggregate: "
            f"observed=${aggregate.get('observed_abs_usd_sum', 0):.3f}, "
            f"mfe60=${aggregate.get('fresh_mfe_60m_usd_sum', 0):.3f}, "
            f"final60=${aggregate.get('fresh_final_60m_usd_sum', 0):.3f}, "
            f"bestExit=${aggregate.get('best_exit_usd_sum', 0):.3f}, "
            f"gap=${aggregate.get('capture_gap_60m_usd_sum', 0):.3f}"
        ),
        "",
        "## Top positive capture groups",
        "",
    ]
    for row in report.get("top_positive_capture_groups", [])[:10]:
        if isinstance(row, dict):
            lines.append(row_line(row))
    lines.extend(["", "## Largest capture gaps", ""])
    for row in report.get("top_capture_gap_groups", [])[:10]:
        if isinstance(row, dict):
            lines.append(row_line(row))
    lines.extend(["", "## Top observed-value groups", ""])
    for row in report.get("top_observed_value_groups", [])[:10]:
        if isinstance(row, dict):
            lines.append(row_line(row))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-glob",
        action="append",
        default=[],
        help="Report-root-relative or absolute glob for event-level replay detail CSVs.",
    )
    parser.add_argument("--limit-files", type=int, default=12)
    parser.add_argument("--min-group-count", type=int, default=5)
    parser.add_argument("--min-positive-rate", type=float, default=0.55)
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--report", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MD)
    args = parser.parse_args()
    report = build_report(args)
    print(
        json.dumps(
            {
                "status": "completed",
                "generated_utc": report.get("generated_utc"),
                "row_count": report.get("row_count"),
                "group_count": report.get("group_count"),
                "top_positive": report.get("top_positive_capture_groups", [])[:3],
                "report": str(args.report),
                "csv": str(args.csv),
                "markdown": str(args.markdown),
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
