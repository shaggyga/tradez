#!/usr/bin/env python3
"""Report free official source depth for every priced currency.

This is a transport inventory, not a direction model. A non-empty category
means an official source can be observed; it does not claim a parsed value,
surprise, edge, promotion, or execution permission.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "official_currency_source_depth_v1.json"
CENTRAL_BANKS = ROOT / "config" / "official_central_bank_source_map_v1.json"
NEWS = ROOT / "config" / "news_sources_v1.json"
RATES = ROOT / "config" / "official_daily_rate_context_v1.json"
REPORT_DIR = ROOT / "data" / "oanda_training_manager" / "reports" / "official_currency_source_depth"
JSON_REPORT = REPORT_DIR / "OFFICIAL_CURRENCY_SOURCE_DEPTH_CURRENT.json"
MD_REPORT = REPORT_DIR / "OFFICIAL_CURRENCY_SOURCE_DEPTH_CURRENT.md"


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"expected object: {path}")
    return dict(value)


def build_report() -> dict[str, Any]:
    config = read_json(CONFIG)
    central = read_json(CENTRAL_BANKS)
    news = read_json(NEWS)
    rates = read_json(RATES)
    categories = [str(value) for value in config.get("categories") or []]
    expected = [str(value) for value in central.get("frozen_currency_universe") or []]
    news_sources = {
        str(row.get("source_id")): dict(row)
        for row in news.get("sources") or []
        if isinstance(row, Mapping) and row.get("source_id")
    }
    rate_sources = {
        str(row.get("source_id")): dict(row)
        for row in rates.get("sources") or []
        if isinstance(row, Mapping) and row.get("source_id")
    }
    news_ids = set(news_sources)
    rate_ids = set(rate_sources)
    known_ids = news_ids | rate_ids
    central_rows = {
        str(row.get("currency")): dict(row)
        for row in central.get("currencies") or []
        if isinstance(row, Mapping)
    }
    depth_rows = {
        str(row.get("currency")): dict(row)
        for row in config.get("currencies") or []
        if isinstance(row, Mapping)
    }
    if sorted(depth_rows) != sorted(expected):
        raise ValueError("currency_depth_universe_mismatch")

    rows: list[dict[str, Any]] = []
    unknown: dict[str, list[str]] = {}
    for currency in expected:
        depth = depth_rows[currency]
        policy = central_rows.get(currency) or {}
        policy_ids = list(policy.get("release_source_ids") or [])
        category_sources: dict[str, list[str]] = {}
        missing: list[str] = []
        for category in categories:
            ids = [str(value) for value in depth.get(category) or []]
            category_sources[category] = ids
            bad = sorted(set(ids) - known_ids)
            if bad:
                unknown[f"{currency}:{category}"] = bad
            for source_id in ids:
                if source_id in news_sources:
                    source = news_sources[source_id]
                    source_currencies = {str(value) for value in source.get("currencies") or []}
                    if (
                        source.get("verified") is not True
                        or source.get("direct") is not True
                        or source.get("enabled") is False
                        or currency not in source_currencies
                    ):
                        unknown.setdefault(f"{currency}:{category}:invalid_news_source", []).append(source_id)
                elif source_id in rate_sources:
                    if str(rate_sources[source_id].get("currency")) != currency:
                        unknown.setdefault(f"{currency}:{category}:invalid_rate_source", []).append(source_id)
            if not ids:
                missing.append(category)
        rows.append(
            {
                "currency": currency,
                "policy_source_ids": policy_ids,
                "category_source_ids": category_sources,
                "covered_category_count": len(categories) - len(missing),
                "required_category_count": len(categories),
                "missing_categories": missing,
                "complete_transport_depth": bool(policy_ids and not missing),
            }
        )
    if unknown:
        raise ValueError(f"unknown_source_ids:{json.dumps(unknown, sort_keys=True)}")
    category_counts = {
        category: sum(bool(row["category_source_ids"][category]) for row in rows)
        for category in categories
    }
    return {
        "schema_version": "official_currency_source_depth_report_v1",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "currency_strength_weight": 0.0,
        "currency_count": len(rows),
        "complete_transport_depth_count": sum(row["complete_transport_depth"] for row in rows),
        "category_currency_counts": category_counts,
        "currencies": rows,
        "limitations": [
            "transport coverage is not structured numeric coverage",
            "official actuals are not causal pre-release consensus",
            "coverage cannot confirm direction, edge, promotion, authorization, or execution",
        ],
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Official Currency Source Depth",
        "",
        f"Generated: `{report['generated_utc']}`",
        "",
        "Research-only transport inventory; no trading permission or weight.",
        "",
        f"- Currencies: **{report['currency_count']}**",
        f"- Complete policy + seven-category transport depth: **{report['complete_transport_depth_count']}/{report['currency_count']}**",
        "- Category coverage: " + ", ".join(
            f"{name} {count}/{report['currency_count']}"
            for name, count in report["category_currency_counts"].items()
        ),
        "",
        "| Currency | Covered | Missing official source categories |",
        "|---|---:|---|",
    ]
    for row in report["currencies"]:
        missing = ", ".join(row["missing_categories"]) or "none"
        lines.append(
            f"| {row['currency']} | {row['covered_category_count']}/{row['required_category_count']} | {missing} |"
        )
    lines.extend(
        [
            "",
            "A listed source proves transport only. Structured values, knowledge-time correctness, surprises and predictive value require separate validation.",
            "",
        ]
    )
    return "\n".join(lines)


def write_report() -> dict[str, Any]:
    report = build_report()
    atomic_write_text(JSON_REPORT, json.dumps(report, indent=2) + "\n")
    atomic_write_text(MD_REPORT, render_markdown(report))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        report = build_report() if args.no_write else write_report()
        print(json.dumps({
            "currency_count": report["currency_count"],
            "complete_transport_depth_count": report["complete_transport_depth_count"],
            "category_currency_counts": report["category_currency_counts"],
        }, indent=2))
        if args.interval_sec <= 0.0 or args.duration_sec <= 0.0:
            break
        if time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(1.0, min(args.interval_sec, args.duration_sec)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
