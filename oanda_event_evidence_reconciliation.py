#!/usr/bin/env python3
"""Reconcile event, news, technical, inverse, and no-trade evidence arms.

The inputs come from different frozen evidence windows.  This report keeps
them side by side and never pools their rows or treats an oracle magnitude arm
as a tradable directional strategy.  It is read-only and cannot execute.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Mapping

import oanda_local_news_sentiment as news


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
REPORTS = DATA / "reports"
DEFAULT_DIRECT_REPLAY = REPORTS / "direct_source_response" / "DIRECT_SOURCE_HISTORICAL_REPLAY_20260808.json"
DEFAULT_DIRECT_RULES = REPORTS / "direct_source_response" / "DIRECT_SOURCE_SIMPLE_RULES_20260808.json"
DEFAULT_NEWS_BACKTEST = REPORTS / "news_feed_backtest" / "news_first_key_pairs_replay_20260811_v4_causal_final.json"
DEFAULT_TECHNICAL_AUDIT = REPORTS / "executable_opportunity_ranking" / "OPPORTUNITY_DIRECTION_POLICY_AUDIT_20260814.json"
DEFAULT_OPPORTUNITY = DATA / "state" / "executable_opportunity_prospective_v1.json"
DEFAULT_WATCHLIST = DATA / "state" / "news_technical_watchlist_v1.json"
OUTPUT_ROOT = REPORTS / "event_evidence_reconciliation"
DEFAULT_JSON = OUTPUT_ROOT / "EVENT_EVIDENCE_RECONCILIATION_CURRENT.json"
DEFAULT_MD = OUTPUT_ROOT / "EVENT_EVIDENCE_RECONCILIATION_CURRENT.md"


def read_json(path: Path) -> dict[str, Any]:
    return news.load_json(path, {})


def row(
    arm: str,
    evidence_class: str,
    horizon_sec: int | None,
    n: int,
    average_net_pips: float | None,
    win_rate: float | None,
    direction_accuracy: float | None,
    cost_clear_rate: float | None,
    note: str,
) -> dict[str, Any]:
    return {
        "arm": arm,
        "evidence_class": evidence_class,
        "horizon_sec": horizon_sec,
        "n": int(n),
        "average_net_pips": average_net_pips,
        "win_rate": win_rate,
        "direction_accuracy": direction_accuracy,
        "cost_clear_rate": cost_clear_rate,
        "note": note,
        "proof_eligible": False,
        "execution_eligible": False,
    }


def reconcile(
    direct_replay: Mapping[str, Any],
    direct_rules: Mapping[str, Any],
    news_backtest: Mapping[str, Any],
    technical_audit: Mapping[str, Any],
    opportunity: Mapping[str, Any],
    watchlist: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for summary in direct_replay.get("matched_control_comparison") or []:
        horizon = int(summary.get("horizon_sec") or 0)
        rows.append(
            row(
                "official_event_magnitude_oracle",
                "historical_discovery_matched_control",
                horizon,
                int(summary.get("event_n") or 0),
                summary.get("event_mean_oracle_best_after_cost_pips"),
                None,
                None,
                summary.get("event_cost_clear_fraction"),
                "oracle side; opportunity existence only, not a forecast",
            )
        )
        rows.append(
            row(
                "matched_non_event_magnitude_oracle",
                "historical_discovery_matched_control",
                horizon,
                int(summary.get("control_n") or 0),
                summary.get("control_mean_oracle_best_after_cost_pips"),
                None,
                None,
                summary.get("control_cost_clear_fraction"),
                "matched non-event oracle control",
            )
        )
    for horizon, summary in (direct_rules.get("by_horizon") or {}).items():
        direction = summary.get("direction_rule") or {}
        flipped = summary.get("flipped_negative_control") or {}
        rows.append(
            row(
                "source_native_direction_rule",
                "same_window_historical_discovery",
                int(horizon),
                int(direction.get("n") or 0),
                direction.get("average_net_pips"),
                direction.get("win_rate"),
                None,
                None,
                "source-native actual/change rule; previous is often not consensus",
            )
        )
        rows.append(
            row(
                "source_native_direction_flipped",
                "same_window_historical_negative_control",
                int(horizon),
                int(flipped.get("n") or 0),
                flipped.get("average_net_pips"),
                flipped.get("win_rate"),
                None,
                None,
                "exact inverse of source-native direction",
            )
        )
    results = news_backtest.get("results") or {}
    for arm, key in (
        ("news_semantic_direction", "cleaned_current"),
        ("news_semantic_direction_flipped", "cleaned_inverse_direction_research"),
    ):
        for horizon_min, summary in (
            (results.get(key) or {}).get("by_horizon_minutes") or {}
        ).items():
            rows.append(
                row(
                    arm,
                    "historical_reclassified_news_discovery",
                    int(horizon_min) * 60,
                    int(summary.get("scored_calls") or 0),
                    summary.get("average_executable_pips"),
                    (
                        int(summary.get("profitable_calls") or 0)
                        / int(summary.get("scored_calls") or 1)
                        if int(summary.get("scored_calls") or 0)
                        else None
                    ),
                    summary.get("direction_accuracy"),
                    None,
                    "already-inspected news window; not causal prospective proof",
                )
            )
    for horizon in technical_audit.get("results") or []:
        policies = horizon.get("policies") or []
        if not policies:
            continue
        best = max(
            policies,
            key=lambda item: float(
                (item.get("strict_top_one") or {}).get("average_net_pips")
                if (item.get("strict_top_one") or {}).get("average_net_pips") is not None
                else float("-inf")
            ),
        )
        metrics = best.get("strict_top_one") or {}
        rows.append(
            row(
                "technical_archive_best_of_16",
                "already_inspected_archive_discovery",
                int(horizon.get("horizon_sec") or 0),
                int(metrics.get("n") or 0),
                metrics.get("average_net_pips"),
                metrics.get("win_rate"),
                metrics.get("direction_accuracy"),
                None,
                f"post-selection best policy: {best.get('policy')}; multiplicity not adjusted",
            )
        )
    totals = opportunity.get("totals") or {}
    rows.append(
        row(
            "price_magnitude_plus_direction_prospective",
            "prospective_shadow_correlated_forecasts",
            None,
            int(totals.get("matured") or 0),
            totals.get("average_predicted_side_net_pips"),
            None,
            totals.get("direction_accuracy"),
            totals.get("cost_clear_rate"),
            f"{int(totals.get('magnitude_gate_passes') or 0)} magnitude passes; "
            f"{int(totals.get('directional_trade_gate_passes') or 0)} directional trade passes",
        )
    )
    matured_watchlist = int(watchlist.get("matured_entries") or 0)
    rows.append(
        row(
            "news_plus_technical_confirmed_prospective",
            "prospective_shadow",
            None,
            matured_watchlist,
            None,
            None,
            None,
            None,
            f"cohort {watchlist.get('cohort_id')}; weekend currently has no entries",
        )
    )
    rows.append(
        row(
            "no_trade",
            "policy_control",
            None,
            0,
            0.0,
            None,
            None,
            None,
            "zero turnover and zero spread cost",
        )
    )
    return rows


def render_markdown(payload: Mapping[str, Any]) -> str:
    def fmt(value: Any, pattern: str = ".3f") -> str:
        return "n/a" if value is None else format(float(value), pattern)

    lines = [
        "# Event Evidence Reconciliation",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Read-only reconciliation. Rows from incompatible evidence windows are displayed side by side and are never pooled.",
        "",
        "| Arm | Evidence class | Horizon | N | Avg net | Win | Direction | Cost clear | Interpretation |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for item in payload["arms"]:
        horizon = "mixed" if item["horizon_sec"] is None else str(item["horizon_sec"])
        lines.append(
            f"| {item['arm']} | {item['evidence_class']} | {horizon} | {item['n']} | "
            f"{fmt(item['average_net_pips'])} | {fmt(item['win_rate'], '.1%')} | "
            f"{fmt(item['direction_accuracy'], '.1%')} | {fmt(item['cost_clear_rate'], '.1%')} | "
            f"{item['note']} |"
        )
    lines.extend(
        [
            "",
            "## Decision",
            "",
            "- Events increase short-horizon movement opportunity in the historical discovery sample, especially around 5-15 minutes, but the profitable magnitude statistic uses the best direction after the fact.",
            "- Semantic news direction and the current price-direction model are negative after costs. Inversion is a diagnostic, not a promotion path.",
            "- The existing prospective news-plus-technical cohort is the correct untouched test, but it has no matured weekend entries yet.",
            "- `no_trade` remains the only supported operational arm.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    direct_replay_path: Path = DEFAULT_DIRECT_REPLAY,
    direct_rules_path: Path = DEFAULT_DIRECT_RULES,
    news_backtest_path: Path = DEFAULT_NEWS_BACKTEST,
    technical_audit_path: Path = DEFAULT_TECHNICAL_AUDIT,
    opportunity_path: Path = DEFAULT_OPPORTUNITY,
    watchlist_path: Path = DEFAULT_WATCHLIST,
    output_json: Path = DEFAULT_JSON,
    output_md: Path = DEFAULT_MD,
) -> dict[str, Any]:
    arms = reconcile(
        read_json(direct_replay_path),
        read_json(direct_rules_path),
        read_json(news_backtest_path),
        read_json(technical_audit_path),
        read_json(opportunity_path),
        read_json(watchlist_path),
    )
    payload = {
        "schema_version": 1,
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "execution_decision": "no_trade",
        "arms": arms,
        "limitations": [
            "incompatible evidence windows are not pooled",
            "historical arms were already inspected",
            "oracle magnitude is not tradable direction",
            "correlated forecasts are not independent episodes",
            "no arm can promote from this reconciliation",
        ],
    }
    news.atomic_write_json(output_json, payload)
    news.atomic_write_text(output_md, render_markdown(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--direct-replay", type=Path, default=DEFAULT_DIRECT_REPLAY)
    parser.add_argument("--direct-rules", type=Path, default=DEFAULT_DIRECT_RULES)
    parser.add_argument("--news-backtest", type=Path, default=DEFAULT_NEWS_BACKTEST)
    parser.add_argument("--technical-audit", type=Path, default=DEFAULT_TECHNICAL_AUDIT)
    parser.add_argument("--opportunity", type=Path, default=DEFAULT_OPPORTUNITY)
    parser.add_argument("--watchlist", type=Path, default=DEFAULT_WATCHLIST)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_MD)
    args = parser.parse_args()
    payload = run(
        args.direct_replay,
        args.direct_rules,
        args.news_backtest,
        args.technical_audit,
        args.opportunity,
        args.watchlist,
        args.output_json,
        args.output_md,
    )
    print(json.dumps({"generated_utc": payload["generated_utc"], "arm_count": len(payload["arms"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
