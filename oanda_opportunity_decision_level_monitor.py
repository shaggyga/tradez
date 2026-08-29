#!/usr/bin/env python3
"""Collapse opportunity rows into causal top-one and disjoint-basket decisions.

This is a read-only research diagnostic.  It never changes the immutable
forecast/outcome ledger, model cohort, collector cohort, lifecycle state, or
execution policy.  Pair rows from one timestamp are correlated observations;
the report therefore keeps raw-pair counts separate from decisions selected
using only forecast-time scores.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state" / "executable_opportunity_prospective_v1.json"
DB = ROOT / "data" / "oanda_training_manager" / "state" / "executable_opportunity_prospective_v1.sqlite"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "executable_opportunity_ranking" / "OPPORTUNITY_DECISION_LEVEL_CURRENT.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "executable_opportunity_ranking" / "OPPORTUNITY_DECISION_LEVEL_CURRENT.md"
DIRECTION_ABLATION_COHORT_ID = "opportunity_top_one_direction_ablation_v1_20260814"
DIRECTION_ABLATION_START_UTC = "2026-08-14T15:55:00+00:00"


def atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{Path.cwd().stat().st_mtime_ns}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def currencies(instrument: str) -> tuple[str, str]:
    parts = str(instrument).upper().split("_")
    if len(parts) != 2 or any(len(part) != 3 for part in parts):
        raise ValueError(f"invalid FX instrument: {instrument!r}")
    return parts[0], parts[1]


def forecast_order(row: Mapping[str, Any]) -> tuple[float, float, float, str]:
    cost = max(float(row["modeled_entry_cost_pips"] or 0.0), 1e-12)
    ratio = float(row["predicted_magnitude_pips"] or 0.0) / cost
    return (
        -float(row["predicted_clear_probability"] or 0.0),
        -ratio,
        cost,
        str(row["instrument"]),
    )


def select_top_one(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    ordered = sorted(rows, key=forecast_order)
    return ordered[:1]


def select_disjoint_three(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    selected: list[Mapping[str, Any]] = []
    used: set[str] = set()
    for row in sorted(rows, key=forecast_order):
        legs = set(currencies(str(row["instrument"])))
        if legs & used:
            continue
        selected.append(row)
        used.update(legs)
        if len(selected) == 3:
            return selected
    return []


def metrics(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    values = list(rows)
    matured = [row for row in values if row.get("outcome_present")]
    def average(key: str) -> float | None:
        return None if not matured else sum(float(row[key]) for row in matured) / len(matured)
    return {
        "selected_rows": len(values),
        "matured_rows": len(matured),
        "cost_clearing_rows": sum(int(row["movement_cleared_cost"]) for row in matured),
        "cost_clear_rate": average("movement_cleared_cost"),
        "direction_accuracy": average("direction_correct"),
        "average_predicted_side_net_pips": average("predicted_side_net_pips"),
    }


def sign(value: Any) -> int:
    number = float(value or 0.0)
    return 1 if number > 0 else -1 if number < 0 else 0


def direction_ablation_metrics(
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Score frozen side rules on the causally selected top-one opportunity.

    Pair selection is unchanged and occurs before outcomes.  Only forecasts at
    or after the declared cohort start are included, so the live miss that
    motivated this diagnostic cannot become evidence for the new rule set.
    """

    variants: dict[str, list[tuple[int, Mapping[str, Any]]]] = defaultdict(list)
    selected = [
        row for row in rows
        if str(row.get("issued_at_utc") or "") >= DIRECTION_ABLATION_START_UTC
    ]
    for row in selected:
        try:
            features = json.loads(str(row.get("feature_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            features = {}
        model = sign(row.get("predicted_direction"))
        continuation_5m = sign(features.get("return_5m_pips"))
        continuation_15m = sign(features.get("return_15m_pips"))
        continuation_consensus = (
            continuation_5m
            if continuation_5m == continuation_15m
            else 0
        )
        for name, direction in (
            ("frozen_model", model),
            ("inverse_model", -model),
            ("continuation_5m", continuation_5m),
            ("continuation_15m", continuation_15m),
            ("continuation_5m_15m_consensus", continuation_consensus),
        ):
            variants[name].append((direction, row))
    result: dict[str, dict[str, Any]] = {}
    for name, values in variants.items():
        matured = [
            (direction, row) for direction, row in values
            if row.get("outcome_present") and direction
        ]
        signed_nets = [
            direction * float(row.get("signed_move_pips") or 0.0)
            - float(row.get("modeled_entry_cost_pips") or 0.0)
            for direction, row in matured
        ]
        result[name] = {
            "selected_rows": len(values),
            "matured_nonabstaining_rows": len(matured),
            "abstained_rows": sum(int(direction == 0) for direction, _ in values),
            "direction_accuracy": (
                None
                if not matured
                else sum(
                    int(direction * float(row.get("signed_move_pips") or 0.0) > 0)
                    for direction, row in matured
                ) / len(matured)
            ),
            "average_after_cost_pips": (
                None if not signed_nets else sum(signed_nets) / len(signed_nets)
            ),
        }
    return result


def build(state_path: Path = STATE, db_path: Path = DB) -> dict[str, Any]:
    state = json.loads(state_path.read_text(encoding="utf-8"))
    cohort_id = str(state.get("collection_cohort", {}).get("cohort_id") or state.get("cohort_id") or "")
    if not cohort_id:
        raise RuntimeError("live opportunity state has no exact collector cohort")
    uri = f"file:{db_path.resolve().as_posix()}?mode=ro"
    db = sqlite3.connect(uri, uri=True)
    db.row_factory = sqlite3.Row
    rows = [dict(row) for row in db.execute(
        """SELECT f.entry_epoch,f.issued_at_utc,f.horizon_sec,f.instrument,
                  f.predicted_clear_probability,f.predicted_magnitude_pips,
                  f.modeled_entry_cost_pips,f.predicted_direction,
                  f.feature_json,
                  CASE WHEN o.forecast_id IS NULL THEN 0 ELSE 1 END AS outcome_present,
                  COALESCE(o.movement_cleared_cost,0) AS movement_cleared_cost,
                  COALESCE(o.direction_correct,0) AS direction_correct,
                  COALESCE(o.predicted_side_net_pips,0.0) AS predicted_side_net_pips,
                  COALESCE(o.signed_move_pips,0.0) AS signed_move_pips
             FROM forecasts f LEFT JOIN outcomes o ON o.forecast_id=f.forecast_id
            WHERE f.cohort_id=?""",
        (cohort_id,),
    )]
    db.close()
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["entry_epoch"]), int(row["horizon_sec"]))].append(row)
    top_one: list[Mapping[str, Any]] = []
    basket: list[Mapping[str, Any]] = []
    basket_decisions = 0
    for group in grouped.values():
        top_one.extend(select_top_one(group))
        chosen = select_disjoint_three(group)
        if chosen:
            basket.extend(chosen)
            basket_decisions += 1
    horizons: dict[str, Any] = {}
    for horizon in sorted({int(row["horizon_sec"]) for row in rows}):
        raw = [row for row in rows if int(row["horizon_sec"]) == horizon]
        top = [row for row in top_one if int(row["horizon_sec"]) == horizon]
        three = [row for row in basket if int(row["horizon_sec"]) == horizon]
        horizons[str(horizon)] = {
            "raw_pair_rows": metrics(raw),
            "top_one": metrics(top),
            "exactly_three_currency_disjoint": metrics(three),
            "decision_epochs": len({int(row["entry_epoch"]) for row in raw}),
            "three_leg_decisions": len(three) // 3,
        }
    return {
        "schema_version": 1,
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "collector_cohort_id": cohort_id,
        "research_only": True,
        "execution_eligible": False,
        "can_promote": False,
        "can_place_orders": False,
        "selection_contract": {
            "causal": True,
            "selection_score": "predicted_clear_probability then predicted_magnitude/cost then cost then instrument",
            "top_one": "one pair per entry epoch and horizon",
            "basket": "exactly three greedily ranked pairs with six distinct currencies; absent if unavailable",
            "independence_warning": "decision epochs are not independent market episodes",
        },
        "direction_ablation": {
            "cohort_id": DIRECTION_ABLATION_COHORT_ID,
            "start_utc": DIRECTION_ABLATION_START_UTC,
            "research_only": True,
            "can_promote": False,
            "can_place_orders": False,
            "selection": "same frozen forecast-time top-one pair",
            "variants": direction_ablation_metrics(top_one),
            "by_horizon": {
                str(horizon): direction_ablation_metrics(
                    row for row in top_one if int(row["horizon_sec"]) == horizon
                )
                for horizon in sorted(
                    {int(row["horizon_sec"]) for row in top_one}
                )
            },
        },
        "raw_pair_rows": len(rows),
        "decision_epochs": len(grouped),
        "top_one": metrics(top_one),
        "exactly_three_currency_disjoint": {**metrics(basket), "decisions": basket_decisions},
        "horizons": horizons,
    }


def render(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Opportunity Decision-Level Monitor", "",
        f"Generated: `{payload['generated_utc']}`", "",
        f"- Exact collector cohort: `{payload['collector_cohort_id']}`",
        f"- Raw pair rows: **{payload['raw_pair_rows']}**",
        f"- Decision epochs: **{payload['decision_epochs']}**",
        "- Execution: **research-only; no_trade**", "",
        "| Horizon | Pair rows mature | Top-one mature | Top-one avg net | Top-one direction | 3-leg decisions | Basket avg net |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for horizon, item in payload["horizons"].items():
        raw, top, basket = item["raw_pair_rows"], item["top_one"], item["exactly_three_currency_disjoint"]
        net = top["average_predicted_side_net_pips"]
        acc = top["direction_accuracy"]
        bnet = basket["average_predicted_side_net_pips"]
        lines.append(
            f"| {horizon}s | {raw['matured_rows']} | {top['matured_rows']} | "
            f"{('n/a' if net is None else f'{net:.3f}')} | {('n/a' if acc is None else f'{acc:.1%}')} | "
            f"{item['three_leg_decisions']} | {('n/a' if bnet is None else f'{bnet:.3f}')} |"
        )
    lines += [
        "", "Pair rows from one timestamp are correlated. Top-one and the disjoint basket are selected only from forecast-time scores, but repeated timestamps still are not independent market episodes.", "",
    ]
    ablation = payload["direction_ablation"]
    lines += [
        "## Frozen prospective direction ablation", "",
        f"Cohort: `{ablation['cohort_id']}`; starts `{ablation['start_utc']}`.",
        "The selected pair is identical across arms; only the forecast-time side rule changes. No arm can promote or execute.", "",
        "| Side rule | Mature non-abstain | Direction | Avg after cost |",
        "|---|---:|---:|---:|",
    ]
    for name, item in ablation["variants"].items():
        accuracy = item["direction_accuracy"]
        average = item["average_after_cost_pips"]
        lines.append(
            f"| {name} | {item['matured_nonabstaining_rows']} | "
            f"{('n/a' if accuracy is None else f'{accuracy:.1%}')} | "
            f"{('n/a' if average is None else f'{average:.3f}')} |"
        )
    lines += ["", "### By horizon", ""]
    for horizon, variants in ablation.get("by_horizon", {}).items():
        lines += [
            f"#### {horizon}s",
            "",
            "| Side rule | Mature non-abstain | Direction | Avg after cost |",
            "|---|---:|---:|---:|",
        ]
        for name, item in variants.items():
            accuracy = item["direction_accuracy"]
            average = item["average_after_cost_pips"]
            lines.append(
                f"| {name} | {item['matured_nonabstaining_rows']} | "
                f"{('n/a' if accuracy is None else f'{accuracy:.1%}')} | "
                f"{('n/a' if average is None else f'{average:.3f}')} |"
            )
        lines.append("")
    lines.append("")
    return "\n".join(lines)


def run_once(
    state: Path = STATE,
    database: Path = DB,
    output: Path = OUTPUT,
    report: Path = REPORT,
) -> dict[str, Any]:
    """Refresh the passive decision-level diagnostic and its artifacts."""

    payload = build(state, database)
    atomic(output, json.dumps(payload, indent=2, sort_keys=True))
    atomic(report, render(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=STATE)
    parser.add_argument("--database", type=Path, default=DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    payload = run_once(args.state, args.database, args.output, args.report)
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
