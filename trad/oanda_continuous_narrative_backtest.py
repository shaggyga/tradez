#!/usr/bin/env python3
"""Historical discovery test for the continuous currency narrative meter.

Compares each frozen narrative formula with technical-only, conjunction, and
conflict arms on executable OANDA bid/ask paths.  It is read-only with respect
to market/news inputs and has no broker, authorization, or promotion imports.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import statistics
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from oanda_continuous_narrative_meter import (
    DATA, DEFAULT_DB as DEFAULT_METER, METER_CONTRACT_ID, MODEL_REGISTRY,
    finite, parse_time,
)
from oanda_news_currency_response_diagnostic import (
    DEFAULT_CANDLES, price_candidates,
)


DEFAULT_OUTPUT = DATA / "reports" / "continuous_narrative" / "CONTINUOUS_NARRATIVE_BACKTEST_V10.json"
DEFAULT_REPORT = DATA / "reports" / "continuous_narrative" / "CONTINUOUS_NARRATIVE_BACKTEST_V10.md"
DEFAULT_RESULTS = DATA / "state" / "continuous_narrative_model_results_v10.sqlite"
HORIZONS = (5, 15, 60, 240, 1440)
MIN_ABS_SCORE = 0.05
BACKTEST_CONTRACT_ID = "continuous_narrative_historical_discovery_v10_equivalence_20260824"


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def load_meter(path: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT clock_utc,currency,model_scores_json,attention_level,
                   attention_acceleration,new_story_count,active_story_count,
                   source_family_count,agreement,novelty,trusted_story_count,
                   forward_timely_story_count,story_ids_json,evidence_class
            FROM currency_meter WHERE meter_contract_id=?
            ORDER BY clock_utc,currency
            """,
            (METER_CONTRACT_ID,),
        ).fetchall()
        output = []
        for raw in rows:
            row = dict(raw)
            row["clock_utc"] = parse_time(row["clock_utc"])
            row["model_scores"] = json.loads(row.pop("model_scores_json"))
            row["story_ids"] = json.loads(row.pop("story_ids_json"))
            if row["clock_utc"] is not None:
                output.append(row)
        return output
    finally:
        connection.close()


def evidence_quality_slice(row: Mapping[str, Any]) -> str:
    """Assign one mutually exclusive, outcome-independent evidence slice."""

    trusted = int(row.get("trusted_story_count") or 0) > 0
    forward = int(row.get("forward_timely_story_count") or 0) > 0
    multi_source = int(row.get("source_family_count") or 0) >= 2
    if trusted and forward and multi_source:
        return "trusted_forward_multi_source"
    if trusted and forward:
        return "trusted_forward_single_source"
    if trusted:
        return "trusted_not_forward"
    if forward and multi_source:
        return "forward_multi_source_untrusted"
    if forward:
        return "forward_single_source_untrusted"
    return "secondary_or_stale"


def model_stream_equivalence(
    decisions: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Fingerprint complete decision streams so aliases do not add breadth."""

    by_model: defaultdict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for row in decisions:
        stamp = row.get("decision_utc")
        by_model[str(row.get("model_id") or "")].append((
            str(row.get("currency") or ""),
            stamp.isoformat() if isinstance(stamp, dt.datetime) else str(stamp or ""),
            str(row.get("sampling_cohort") or ""),
            int(row.get("direction") or 0),
            round(finite(row.get("score")), 12),
        ))
    fingerprints: dict[str, str] = {}
    groups: defaultdict[str, list[str]] = defaultdict(list)
    for model_id, rows in sorted(by_model.items()):
        digest = hashlib.sha256(
            json.dumps(sorted(rows), separators=(",", ":")).encode()
        ).hexdigest()
        fingerprints[model_id] = digest
        groups[digest].append(model_id)
    result_groups = []
    model_to_group: dict[str, str] = {}
    for digest, models in sorted(groups.items()):
        group_id = f"stream_{digest[:16]}"
        models = sorted(models)
        result_groups.append({
            "equivalence_id": group_id,
            "fingerprint_sha256": digest,
            "models": models,
            "model_count": len(models),
            "canonical_model_id": models[0],
        })
        for model_id in models:
            model_to_group[model_id] = group_id
    return {
        "named_model_count": len(by_model),
        "independent_exact_stream_count": len(result_groups),
        "duplicate_stream_groups": [
            row for row in result_groups if row["model_count"] > 1
        ],
        "groups": result_groups,
        "model_to_equivalence_id": model_to_group,
        "rule": "exact_full_decision_stream_including_clock_currency_cohort_side_and_score",
    }


def select_decisions(rows: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Use causal story updates and predeclared hourly state samples.

    The dense meter is preserved in full, but evaluating every overlapping
    five-minute point as a new trade would manufacture sample size.
    """

    decisions: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    prior_sign: dict[tuple[str, str], int] = {}
    for row in rows:
        if int(row.get("active_story_count") or 0) <= 0:
            continue
        stamp = row["clock_utc"]
        for model_id, raw_score in row["model_scores"].items():
            score = finite(raw_score)
            sign = 1 if score > 0 else -1 if score < 0 else 0
            key = (str(row["currency"]), str(model_id))
            event_update = int(row.get("new_story_count") or 0) > 0
            sign_change = sign != 0 and prior_sign.get(key, 0) not in (0, sign)
            hourly_state = stamp.minute == 0
            prior_sign[key] = sign
            if abs(score) < MIN_ABS_SCORE:
                counts["below_score_threshold"] += 1
                continue
            if not (event_update or sign_change or hourly_state):
                counts["overlapping_clock_not_sampled"] += 1
                continue
            cohort = "event_update" if event_update or sign_change else "hourly_state"
            stories = tuple(sorted(str(value) for value in row.get("story_ids") or ()))
            episode_id = "narrative_episode_" + hashlib.sha256(
                ("|".join(stories) or f"{row['currency']}|{stamp:%Y-%m-%dT%H}").encode()
            ).hexdigest()[:24]
            decisions.append({
                "decision_id": "narrative_decision_" + hashlib.sha256(
                    f"{model_id}|{row['currency']}|{stamp.isoformat()}|{cohort}".encode()
                ).hexdigest()[:24],
                "base_clock_id": "narrative_clock_" + hashlib.sha256(
                    f"{row['currency']}|{stamp.isoformat()}|{cohort}".encode()
                ).hexdigest()[:24],
                "model_id": model_id,
                "currency": row["currency"],
                "decision_utc": stamp,
                "score": score,
                "direction": sign,
                "sampling_cohort": cohort,
                "episode_id": episode_id,
                "story_count": len(stories),
                "attention_level": finite(row.get("attention_level")),
                "attention_acceleration": finite(row.get("attention_acceleration")),
                "source_family_count": int(row.get("source_family_count") or 0),
                "agreement": finite(row.get("agreement")),
                "novelty": finite(row.get("novelty")),
                "trusted_story_count": int(row.get("trusted_story_count") or 0),
                "forward_timely_story_count": int(row.get("forward_timely_story_count") or 0),
                "evidence_class": row.get("evidence_class"),
                "quality_slice": evidence_quality_slice(row),
            })
            counts[cohort] += 1
            counts[f"quality:{decisions[-1]['quality_slice']}"] += 1
    counts["decisions"] = len(decisions)
    counts["models"] = len({row["model_id"] for row in decisions})
    counts["currencies"] = len({row["currency"] for row in decisions})
    return decisions, dict(counts)


def build_price_clocks(decisions: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for row in decisions:
        unique.setdefault(str(row["base_clock_id"]), {
            "clock_id": row["base_clock_id"],
            "currency": row["currency"],
            "decision_utc": row["decision_utc"],
            "direction": 1,
        })
    return list(unique.values())


def evaluate(
    decisions: Iterable[Mapping[str, Any]],
    candidates: Mapping[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    for decision in decisions:
        choices = candidates.get(str(decision["base_clock_id"]), [])
        if not choices:
            continue
        choice = min(choices, key=lambda row: (row["spread_bps"], row["instrument"]))
        technical_direction = (
            1 if choice["prior_currency_bps"] > 0
            else -1 if choice["prior_currency_bps"] < 0 else 0
        )
        base_multiplier = int(choice["base_multiplier"])
        news_direction = int(decision["direction"])
        for horizon, pair_results in choice["horizon_pair_net_bps"].items():
            realized_currency_mid = float(pair_results["pair_mid_bps"]) * base_multiplier
            arms: dict[str, int] = {"sentiment_only": news_direction}
            if technical_direction:
                arms["technical_only_same_clocks"] = technical_direction
                if technical_direction == news_direction:
                    arms["sentiment_technical_confirmed"] = news_direction
                    if abs(float(choice["prior_currency_bps"])) >= float(choice["spread_bps"]):
                        arms["sentiment_breakout_confirmed"] = news_direction
                else:
                    arms["sentiment_technical_conflicted"] = news_direction
            for arm, currency_direction in arms.items():
                pair_direction = currency_direction * base_multiplier
                outcomes.append({
                    "decision_id": decision["decision_id"],
                    "model_id": decision["model_id"],
                    "sampling_cohort": decision["sampling_cohort"],
                    "episode_id": decision["episode_id"],
                    "currency": decision["currency"],
                    "decision_utc": decision["decision_utc"].isoformat(),
                    "instrument": choice["instrument"],
                    "horizon_minutes": int(horizon),
                    "arm": arm,
                    "direction": currency_direction,
                    "score": decision["score"],
                    "net_bps": float(pair_results[pair_direction]),
                    "realized_currency_mid_bps": realized_currency_mid,
                    "spread_bps": float(choice["spread_bps"]),
                    "prior_currency_bps": float(choice["prior_currency_bps"]),
                    "story_count": decision["story_count"],
                    "source_family_count": decision["source_family_count"],
                    "agreement": decision["agreement"],
                    "attention_acceleration": decision["attention_acceleration"],
                    "evidence_class": decision["evidence_class"],
                    "quality_slice": decision["quality_slice"],
                    "model_equivalence_id": str(
                        decision.get("model_equivalence_id") or ""
                    ),
                    "trusted_story_count": decision["trusted_story_count"],
                    "forward_timely_story_count": decision["forward_timely_story_count"],
                })
    return outcomes


def assign_splits(outcomes: list[dict[str, Any]]) -> None:
    days = sorted({row["decision_utc"][:10] for row in outcomes})
    if not days:
        return
    train_end = max(1, math.floor(len(days) * 0.60))
    validation_end = max(train_end + 1, math.floor(len(days) * 0.80))
    validation_end = min(validation_end, len(days))
    train = set(days[:train_end])
    validation = set(days[train_end:validation_end])
    for row in outcomes:
        day = row["decision_utc"][:10]
        row["split"] = "train" if day in train else "validation" if day in validation else "test"


def summarize(outcomes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: defaultdict[tuple[str, str, str, str, int, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in outcomes:
        groups[(
            str(row["model_id"]), str(row["sampling_cohort"]),
            str(row.get("quality_slice") or "unclassified"), str(row["arm"]),
            int(row["horizon_minutes"]), str(row["split"]),
        )].append(row)
    output = []
    for key, rows in sorted(groups.items()):
        model, cohort, quality_slice, arm, horizon, split = key
        days: defaultdict[str, list[float]] = defaultdict(list)
        episodes = Counter()
        currencies = Counter()
        for row in rows:
            days[str(row["decision_utc"])[:10]].append(float(row["net_bps"]))
            episodes[str(row["episode_id"])] += 1
            currencies[str(row["currency"])] += 1
        best_day = max(days, key=lambda value: statistics.fmean(days[value]))
        ex_best = [
            float(row["net_bps"]) for row in rows
            if str(row["decision_utc"])[:10] != best_day
        ]
        nets = [float(row["net_bps"]) for row in rows]
        output.append({
            "model_id": model,
            "sampling_cohort": cohort,
            "quality_slice": quality_slice,
            "model_equivalence_id": str(rows[0].get("model_equivalence_id") or ""),
            "arm": arm,
            "horizon_minutes": horizon,
            "split": split,
            "raw_n": len(rows),
            "effective_episode_n": len(episodes),
            "market_days": len(days),
            "currency_count": len(currencies),
            "mean_net_bps": statistics.fmean(nets),
            "median_net_bps": statistics.median(nets),
            "after_cost_win_rate": statistics.fmean(value > 0 for value in nets),
            "direction_accuracy": statistics.fmean(
                int(row["direction"]) * float(row["realized_currency_mid_bps"]) > 0
                for row in rows
            ),
            "mean_signed_gross_bps": statistics.fmean(
                int(row["direction"]) * float(row["realized_currency_mid_bps"])
                for row in rows
            ),
            "mean_spread_bps": statistics.fmean(float(row["spread_bps"]) for row in rows),
            "mean_without_best_day_bps": statistics.fmean(ex_best) if ex_best else None,
            "best_day_fraction": len(days[best_day]) / len(rows),
            "best_currency_fraction": max(currencies.values()) / len(rows),
            "confirmation_eligible": False,
            "evidence_class": "retrospective_classifier_adaptive_discovery",
        })
    return output


def persist_results(
    path: Path, run_id: str, payload: Mapping[str, Any],
    outcomes: Iterable[Mapping[str, Any]], summaries: Iterable[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=60.0)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs(
                run_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL,
                generated_utc TEXT NOT NULL, specification_json TEXT NOT NULL,
                research_only INTEGER NOT NULL CHECK(research_only=1),
                execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0)
            );
            CREATE TABLE IF NOT EXISTS outcomes(
                run_id TEXT NOT NULL, decision_id TEXT NOT NULL, model_id TEXT NOT NULL,
                sampling_cohort TEXT NOT NULL, episode_id TEXT NOT NULL, currency TEXT NOT NULL,
                decision_utc TEXT NOT NULL, instrument TEXT NOT NULL, horizon_minutes INTEGER NOT NULL,
                arm TEXT NOT NULL, split TEXT NOT NULL, direction INTEGER NOT NULL, score REAL NOT NULL,
                net_bps REAL NOT NULL, realized_currency_mid_bps REAL NOT NULL, spread_bps REAL NOT NULL,
                prior_currency_bps REAL NOT NULL, evidence_class TEXT NOT NULL,
                quality_slice TEXT NOT NULL, model_equivalence_id TEXT NOT NULL,
                PRIMARY KEY(run_id,decision_id,horizon_minutes,arm)
            );
            CREATE TABLE IF NOT EXISTS summaries(
                run_id TEXT NOT NULL, model_id TEXT NOT NULL, sampling_cohort TEXT NOT NULL,
                quality_slice TEXT NOT NULL, arm TEXT NOT NULL,
                horizon_minutes INTEGER NOT NULL, split TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY(run_id,model_id,sampling_cohort,quality_slice,arm,horizon_minutes,split)
            );
            """
        )
        with connection:
            connection.execute(
                "INSERT OR REPLACE INTO backtest_runs VALUES(?,?,?,?,1,0)",
                (run_id, BACKTEST_CONTRACT_ID, payload["generated_utc"], json.dumps(payload["specification"], sort_keys=True)),
            )
            connection.execute("DELETE FROM outcomes WHERE run_id=?", (run_id,))
            connection.execute("DELETE FROM summaries WHERE run_id=?", (run_id,))
            connection.executemany(
                """INSERT INTO outcomes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [(
                    run_id, row["decision_id"], row["model_id"], row["sampling_cohort"],
                    row["episode_id"], row["currency"], row["decision_utc"], row["instrument"],
                    row["horizon_minutes"], row["arm"], row["split"], row["direction"],
                    row["score"], row["net_bps"], row["realized_currency_mid_bps"],
                    row["spread_bps"], row["prior_currency_bps"], row["evidence_class"],
                    row["quality_slice"],
                    row["model_equivalence_id"],
                ) for row in outcomes],
            )
            connection.executemany(
                "INSERT INTO summaries VALUES(?,?,?,?,?,?,?,?)",
                [(
                    run_id, row["model_id"], row["sampling_cohort"],
                    row["quality_slice"], row["arm"],
                    row["horizon_minutes"], row["split"], json.dumps(row, sort_keys=True),
                ) for row in summaries],
            )
    finally:
        connection.close()


def render(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Continuous narrative meter historical discovery", "",
        f"Generated: `{payload['generated_utc']}`", "",
        "Research-only retrospective discovery. No row may authorize or place an order.", "",
        f"- Frozen meter models: **{len(payload['models'])}**",
        f"- Selected narrative decisions: **{payload['selection']['decisions']:,}**",
        f"- Price-scored outcomes: **{payload['outcome_rows']:,}**", "",
        f"- Named models / independent exact streams: **{payload['model_equivalence']['named_model_count']} / {payload['model_equivalence']['independent_exact_stream_count']}**",
        "",
        "| Model | Exact stream | Sample | Evidence quality | Arm | Horizon | Split | Raw N | Eff. episodes | Dir. hit | Win | Mean net bps | Ex-best-day |",
        "|---|---|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["summaries"]:
        ex_best = row["mean_without_best_day_bps"]
        lines.append(
            f"| {row['model_id']} | {row['model_equivalence_id']} | {row['sampling_cohort']} | {row['quality_slice']} | {row['arm']} | "
            f"{row['horizon_minutes']}m | {row['split']} | {row['raw_n']} | "
            f"{row['effective_episode_n']} | {row['direction_accuracy']:.1%} | "
            f"{row['after_cost_win_rate']:.1%} | {row['mean_net_bps']:.3f} | "
            f"{'n/a' if ex_best is None else f'{ex_best:.3f}'} |"
        )
    lines += ["", "Results are tracked by immutable formula ID. The test split is diagnostic because the upstream classifier was improved using the same historical era; final proof still requires a later frozen cohort.", ""]
    return "\n".join(lines)


def run(
    *, meter: Path = DEFAULT_METER, candles: Path = DEFAULT_CANDLES,
    output: Path = DEFAULT_OUTPUT, report: Path = DEFAULT_REPORT,
    results: Path = DEFAULT_RESULTS,
) -> dict[str, Any]:
    decisions, selection = select_decisions(load_meter(meter))
    equivalence = model_stream_equivalence(decisions)
    for decision in decisions:
        decision["model_equivalence_id"] = equivalence[
            "model_to_equivalence_id"
        ][str(decision["model_id"])]
    selection["independent_exact_model_streams"] = int(
        equivalence["independent_exact_stream_count"]
    )
    price_clocks = build_price_clocks(decisions)
    candidates = price_candidates(price_clocks, candles, horizons=HORIZONS)
    outcomes = evaluate(decisions, candidates)
    assign_splits(outcomes)
    summaries = summarize(outcomes)
    generated = dt.datetime.now(dt.timezone.utc).isoformat()
    specification = {
        "backtest_contract_id": BACKTEST_CONTRACT_ID,
        "meter_contract_id": METER_CONTRACT_ID,
        "models": MODEL_REGISTRY,
        "horizons_minutes": HORIZONS,
        "minimum_absolute_score": MIN_ABS_SCORE,
        "sampling": "story_update_or_sign_change_plus_predeclared_hourly_state",
        "evidence_quality_partition": (
            "mutually_exclusive_trusted_forward_multisource_single_source_"
            "forward_untrusted_or_secondary"
        ),
        "model_equivalence_rule": equivalence["rule"],
        "chronological_split": "60_20_20_by_UTC_market_day",
        "pair_selection": "lowest_entry_spread_pair_containing_currency",
        "costs": "executable_OANDA_bid_ask_endpoints",
    }
    run_id = "continuous_narrative_run_" + hashlib.sha256(
        json.dumps(specification, sort_keys=True, default=list).encode()
        + str(max((row["decision_utc"] for row in decisions), default="none")).encode()
    ).hexdigest()[:24]
    payload = {
        "schema_version": "continuous_narrative_backtest_v5_model_equivalence",
        "run_id": run_id,
        "generated_utc": generated,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "orders_placed": 0,
        "selection": selection,
        "price_clock_count": len(price_clocks),
        "price_scored_clock_count": len(candidates),
        "outcome_rows": len(outcomes),
        "models": MODEL_REGISTRY,
        "model_equivalence": equivalence,
        "specification": specification,
        "summaries": summaries,
        "supported_action": "freeze_promising_formula_for_new_untouched_shadow_cohort_or_reject",
    }
    persist_results(results, run_id, payload, outcomes, summaries)
    atomic_text(output, json.dumps(payload, indent=2, sort_keys=True))
    atomic_text(report, render(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--meter", type=Path, default=DEFAULT_METER)
    parser.add_argument("--candles", type=Path, default=DEFAULT_CANDLES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    args = parser.parse_args()
    payload = run(**vars(args))
    print(json.dumps({
        "run_id": payload["run_id"], "selection": payload["selection"],
        "price_scored_clock_count": payload["price_scored_clock_count"],
        "outcome_rows": payload["outcome_rows"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
