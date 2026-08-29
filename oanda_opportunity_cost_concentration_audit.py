#!/usr/bin/env python3
"""Split prospective opportunity outcomes by cost, session, horizon, and pair.

This audit diagnoses whether losses come from direction, insufficient movement,
or execution cost.  Forecast rows remain correlated and research-only; this
module cannot promote, authorize, or trade.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

import oanda_local_news_sentiment as news


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DATABASE = DATA / "state" / "executable_opportunity_prospective_v1.sqlite"
REPORT_ROOT = DATA / "reports" / "executable_opportunity_ranking"
DEFAULT_JSON = REPORT_ROOT / "OPPORTUNITY_COST_CONCENTRATION_CURRENT.json"
DEFAULT_MD = REPORT_ROOT / "OPPORTUNITY_COST_CONCENTRATION_CURRENT.md"


def cost_bucket(cost: float) -> str:
    if cost <= 1.5:
        return "cost_le_1_5"
    if cost <= 2.5:
        return "cost_1_5_to_2_5"
    if cost <= 5.0:
        return "cost_2_5_to_5"
    return "cost_gt_5"


def session(issued_at_utc: str) -> str:
    try:
        hour = dt.datetime.fromisoformat(issued_at_utc.replace("Z", "+00:00")).hour
    except (TypeError, ValueError):
        return "unknown"
    if hour < 7:
        return "asia_utc_00_07"
    if hour < 12:
        return "london_utc_07_12"
    if hour < 16:
        return "overlap_utc_12_16"
    if hour < 21:
        return "new_york_late_utc_16_21"
    return "rollover_utc_21_24"


def summarize(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    values = list(rows)
    n = len(values)
    net = [float(row["net"]) for row in values]
    costs = [float(row["cost"]) for row in values]
    gross = [float(row["gross_signed"]) for row in values]
    gains = sum(max(value, 0.0) for value in net)
    losses = -sum(min(value, 0.0) for value in net)
    return {
        "n": n,
        "average_gross_signed_pips": sum(gross) / n if n else None,
        "average_modeled_cost_pips": sum(costs) / n if n else None,
        "average_net_pips": sum(net) / n if n else None,
        "win_rate_after_cost": sum(value > 0 for value in net) / n if n else None,
        "direction_accuracy": sum(int(row["direction_correct"]) for row in values) / n if n else None,
        "magnitude_cost_clear_rate": sum(int(row["movement_cleared_cost"]) for row in values) / n if n else None,
        "profit_factor": gains / losses if losses else None,
        "average_net_spread_stress_25pct": sum(
            value - 0.25 * cost for value, cost in zip(net, costs)
        ) / n if n else None,
        "average_net_spread_stress_50pct": sum(
            value - 0.50 * cost for value, cost in zip(net, costs)
        ) / n if n else None,
        "average_net_spread_stress_100pct": sum(
            value - cost for value, cost in zip(net, costs)
        ) / n if n else None,
        "proof_eligible": False,
    }


def grouped(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in rows:
        buckets[str(item[key])].append(item)
    return [
        {key: name, **summarize(items)}
        for name, items in sorted(buckets.items())
    ]


def load_rows(database: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(
        f"file:{database.resolve().as_posix()}?mode=ro", uri=True, timeout=60
    )
    connection.execute("PRAGMA query_only=ON")
    try:
        raw = connection.execute(
            """
            SELECT f.instrument,f.horizon_sec,f.issued_at_utc,
                   o.modeled_cost_pips,o.predicted_side_net_pips,
                   o.direction_correct,o.movement_cleared_cost
            FROM forecasts f JOIN outcomes o USING(forecast_id)
            """
        ).fetchall()
    finally:
        connection.close()
    return [
        {
            "instrument": str(instrument),
            "horizon_sec": int(horizon),
            "session": session(str(issued)),
            "cost_bucket": cost_bucket(float(cost)),
            "cost": float(cost),
            "net": float(net),
            "gross_signed": float(net) + float(cost),
            "direction_correct": int(direction_correct),
            "movement_cleared_cost": int(movement_clear),
        }
        for instrument, horizon, issued, cost, net, direction_correct, movement_clear in raw
    ]


def render_markdown(payload: Mapping[str, Any]) -> str:
    def fmt(value: Any, pattern: str = ".3f") -> str:
        return "n/a" if value is None else format(float(value), pattern)

    overall = payload["overall"]
    lines = [
        "# Opportunity Cost + Concentration Audit",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Research-only. Forecast rows are correlated and are not independent proof observations.",
        "",
        f"- Matured rows: **{overall['n']:,}**",
        f"- Average signed movement before modeled cost: **{fmt(overall['average_gross_signed_pips'])} pips**",
        f"- Average modeled cost: **{fmt(overall['average_modeled_cost_pips'])} pips**",
        f"- Average result after modeled cost: **{fmt(overall['average_net_pips'])} pips**",
        f"- Direction accuracy: **{fmt(overall['direction_accuracy'], '.1%')}**",
        f"- After-cost win rate: **{fmt(overall['win_rate_after_cost'], '.1%')}**",
        f"- Average under +50% cost stress: **{fmt(overall['average_net_spread_stress_50pct'])} pips**",
        "",
        "## Cost buckets",
        "",
        "| Cost bucket | N | Gross signed | Cost | Net | After-cost win | Direction | Magnitude clears |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in payload["by_cost_bucket"]:
        lines.append(
            f"| {item['cost_bucket']} | {item['n']:,} | {fmt(item['average_gross_signed_pips'])} | "
            f"{fmt(item['average_modeled_cost_pips'])} | {fmt(item['average_net_pips'])} | "
            f"{fmt(item['win_rate_after_cost'], '.1%')} | {fmt(item['direction_accuracy'], '.1%')} | "
            f"{fmt(item['magnitude_cost_clear_rate'], '.1%')} |"
        )
    lines.extend(
        [
            "",
            "## Horizon",
            "",
            "| Horizon | N | Gross signed | Cost | Net | After-cost win | Direction |",
            "|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for item in payload["by_horizon"]:
        lines.append(
            f"| {item['horizon_sec']} | {item['n']:,} | {fmt(item['average_gross_signed_pips'])} | "
            f"{fmt(item['average_modeled_cost_pips'])} | {fmt(item['average_net_pips'])} | "
            f"{fmt(item['win_rate_after_cost'], '.1%')} | {fmt(item['direction_accuracy'], '.1%')} |"
        )
    lines.extend(
        [
            "",
            "## Concentration",
            "",
            f"- Best pair by average net: **{payload['concentration']['best_pair']}** ({fmt(payload['concentration']['best_pair_average_net_pips'])} pips, N={payload['concentration']['best_pair_n']:,}).",
            f"- Worst pair by average net: **{payload['concentration']['worst_pair']}** ({fmt(payload['concentration']['worst_pair_average_net_pips'])} pips, N={payload['concentration']['worst_pair_n']:,}).",
            f"- Share of all positive row-level pips from the best profit-contributing pair: **{fmt(payload['concentration']['best_positive_contribution_share'], '.1%')}**.",
            "",
            "No cost bucket or concentration slice can promote from this diagnostic. Execution decision remains `no_trade`.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    database: Path = DATABASE,
    output_json: Path = DEFAULT_JSON,
    output_md: Path = DEFAULT_MD,
) -> dict[str, Any]:
    rows = load_rows(database)
    by_instrument = grouped(rows, "instrument")
    ranked = sorted(
        by_instrument,
        key=lambda item: (
            float(item["average_net_pips"] if item["average_net_pips"] is not None else -1e99),
            item["instrument"],
        ),
        reverse=True,
    )
    positive_by_pair: dict[str, float] = defaultdict(float)
    for item in rows:
        positive_by_pair[item["instrument"]] += max(float(item["net"]), 0.0)
    total_positive = sum(positive_by_pair.values())
    top_positive = max(positive_by_pair.values(), default=0.0)
    payload = {
        "schema_version": 1,
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "execution_decision": "no_trade",
        "overall": summarize(rows),
        "by_cost_bucket": grouped(rows, "cost_bucket"),
        "by_horizon": grouped(rows, "horizon_sec"),
        "by_session": grouped(rows, "session"),
        "by_instrument": by_instrument,
        "concentration": {
            "best_pair": ranked[0]["instrument"] if ranked else None,
            "best_pair_n": ranked[0]["n"] if ranked else 0,
            "best_pair_average_net_pips": ranked[0]["average_net_pips"] if ranked else None,
            "worst_pair": ranked[-1]["instrument"] if ranked else None,
            "worst_pair_n": ranked[-1]["n"] if ranked else 0,
            "worst_pair_average_net_pips": ranked[-1]["average_net_pips"] if ranked else None,
            "best_positive_contribution_share": top_positive / total_positive if total_positive else None,
        },
        "limitations": [
            "forecast rows share market timestamps and currency factors",
            "cost stresses are deterministic diagnostics, not new outcomes",
            "best-pair ranking is post-selected",
            "no untouched confirmation is performed here",
        ],
    }
    news.atomic_write_json(output_json, payload)
    news.atomic_write_text(output_md, render_markdown(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_MD)
    args = parser.parse_args()
    payload = run(args.database, args.output_json, args.output_md)
    print(json.dumps({"generated_utc": payload["generated_utc"], "overall": payload["overall"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
