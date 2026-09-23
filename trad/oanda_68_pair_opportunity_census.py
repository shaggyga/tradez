#!/usr/bin/env python3
"""Count spread-clearing FX windows and evaluate a frozen 68-pair combiner.

All calculations are research-only.  The opportunity census uses aligned,
non-overlapping windows and reports hindsight magnitude separately from the
causal directional combiner.  The combiner selects either one pair or exactly
three currency-disjoint pairs from the complete 68-instrument universe.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from oanda_instrument_pips import fallback_pip_size

ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
DATABASE = STATE / "practice_007_quote_intensity_shadow_v1.sqlite"
CONFIG = ROOT / "config" / "sixty_eight_pair_combiner_v1.json"
QUOTE_SNAPSHOT = STATE / "practice_007_market_quotes_v1.json"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "sixty_eight_pair_combiner" / "SIXTY_EIGHT_PAIR_OPPORTUNITY_CENSUS_20260808.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "sixty_eight_pair_combiner" / "SIXTY_EIGHT_PAIR_OPPORTUNITY_CENSUS_20260808.md"
_PIP_SIZES: dict[str, float] = {}


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def pip_size(instrument: str) -> float:
    return _PIP_SIZES.get(instrument, fallback_pip_size(instrument))


def load_pip_sizes(path: Path = QUOTE_SNAPSHOT) -> dict[str, float]:
    quotes = read_json(path).get("quotes") or {}
    result = {}
    for instrument, quote in quotes.items():
        value = finite((quote or {}).get("pip")) if isinstance(quote, Mapping) else None
        if value is not None and value > 0:
            result[str(instrument)] = value
    return result


def currency_legs(instrument: str) -> frozenset[str]:
    return frozenset(instrument.split("_"))


def sign(value: float) -> int:
    return 1 if value > 0 else -1 if value < 0 else 0


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def load_minutes(path: Path) -> tuple[dict[str, dict[int, dict[str, float]]], list[str]]:
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=30)
    rows = db.execute(
        """SELECT minute_epoch,instrument,last_mid,average_spread_pips,updates,
                  imbalance_5s,imbalance_30s,imbalance_120s
           FROM quote_intensity_minutes_v1 ORDER BY minute_epoch,instrument"""
    )
    by_pair: dict[str, dict[int, dict[str, float]]] = defaultdict(dict)
    instruments: set[str] = set()
    for epoch, instrument, mid, spread, updates, imb5, imb30, imb120 in rows:
        instrument = str(instrument)
        by_pair[instrument][int(epoch)] = {
            "mid": float(mid), "spread": float(spread), "updates": float(updates),
            "imbalance_5s": float(imb5), "imbalance_30s": float(imb30),
            "imbalance_120s": float(imb120),
        }
        instruments.add(instrument)
    db.close()
    return dict(by_pair), sorted(instruments)


def modeled_window(instrument: str, start: Mapping[str, float], end: Mapping[str, float], slippage: float) -> dict[str, float | bool]:
    move = (float(end["mid"]) - float(start["mid"])) / pip_size(instrument)
    cost = (float(start["spread"]) + float(end["spread"])) / 2.0 + slippage
    oracle = abs(move) - cost
    return {"signed_move_pips": move, "absolute_move_pips": abs(move),
            "modeled_cost_pips": cost, "oracle_after_cost_pips": oracle,
            "movement_cleared_cost": oracle > 0}


def modeled_path_window(
    instrument: str,
    epoch: int,
    timeline: Mapping[int, Mapping[str, float]],
    horizon: int,
    slippage: float,
) -> dict[str, float | int | bool | None]:
    """Measure the best executable endpoint observed inside a future window.

    This is an explicitly hindsight-only opportunity diagnostic.  It answers
    whether a move existed at any minute inside the horizon; it is never used
    as a direction label or as an input to the causal combiner.
    """
    start = timeline.get(epoch)
    if start is None:
        return {
            "path_cleared_cost": False,
            "best_path_after_cost_pips": None,
            "first_path_clear_sec": None,
        }
    best: float | None = None
    first_clear: int | None = None
    for offset in range(60, horizon + 1, 60):
        end = timeline.get(epoch + offset)
        if end is None:
            continue
        value = float(
            modeled_window(instrument, start, end, slippage)[
                "oracle_after_cost_pips"
            ]
        )
        best = value if best is None else max(best, value)
        if first_clear is None and value > 0.0:
            first_clear = offset
    return {
        "path_cleared_cost": bool(best is not None and best > 0.0),
        "best_path_after_cost_pips": best,
        "first_path_clear_sec": first_clear,
    }


def independent_positive_count(rows: Iterable[Mapping[str, Any]]) -> int:
    used: set[str] = set()
    count = 0
    for row in sorted(rows, key=lambda item: float(item["oracle_after_cost_pips"]), reverse=True):
        legs = currency_legs(str(row["instrument"]))
        if used.isdisjoint(legs):
            used.update(legs)
            count += 1
    return count


def opportunity_census(by_pair: Mapping[str, Mapping[int, Mapping[str, float]]], config: Mapping[str, Any]) -> dict[str, Any]:
    slippage = float(config.get("modeled_slippage_pips") or 0.0)
    result: dict[str, Any] = {}
    for horizon in config.get("horizons_sec") or []:
        horizon = int(horizon)
        grouped: dict[str, dict[int, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        rolling_grouped: dict[str, dict[int, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        for instrument, timeline in by_pair.items():
            for epoch, start in timeline.items():
                end = timeline.get(epoch + horizon)
                if end is None:
                    continue
                row = {
                    "instrument": instrument,
                    **modeled_window(instrument, start, end, slippage),
                    **modeled_path_window(
                        instrument, epoch, timeline, horizon, slippage
                    ),
                }
                day = dt.datetime.fromtimestamp(epoch, dt.timezone.utc).date().isoformat()
                rolling_grouped[day][epoch].append(row)
                if epoch % horizon == 0:
                    grouped[day][epoch].append(row)

        def summarize(
            source: Mapping[str, Mapping[int, list[dict[str, Any]]]],
            expected_timestamps: int,
        ) -> dict[str, Any]:
            days = []
            for day, timestamps in sorted(source.items()):
                all_rows = [row for rows in timestamps.values() for row in rows]
                positives = [row for row in all_rows if row["movement_cleared_cost"]]
                path_positives = [row for row in all_rows if row["path_cleared_cost"]]
                positive_times = [rows for rows in timestamps.values() if any(row["movement_cleared_cost"] for row in rows)]
                path_positive_times = [rows for rows in timestamps.values() if any(row["path_cleared_cost"] for row in rows)]
                independent = sum(independent_positive_count(row for row in rows if row["movement_cleared_cost"]) for rows in timestamps.values())
                path_latencies = [
                    int(row["first_path_clear_sec"])
                    for row in path_positives
                    if row.get("first_path_clear_sec") is not None
                ]
                days.append({
                    "utc_day": day, "pair_windows": len(all_rows), "timestamp_windows": len(timestamps),
                    "spread_clearing_pair_windows": len(positives),
                    "spread_clearing_pair_rate": len(positives) / len(all_rows) if all_rows else None,
                    "timestamps_with_any_clear": len(positive_times),
                    "any_clear_timestamp_rate": len(positive_times) / len(timestamps) if timestamps else None,
                    "mean_clearing_pairs_per_timestamp": len(positives) / len(timestamps) if timestamps else None,
                    "currency_disjoint_oracle_count": independent,
                    "mean_currency_disjoint_per_timestamp": independent / len(timestamps) if timestamps else None,
                    "path_clearing_pair_windows": len(path_positives),
                    "path_clearing_pair_rate": len(path_positives) / len(all_rows) if all_rows else None,
                    "timestamps_with_any_path_clear": len(path_positive_times),
                    "any_path_clear_timestamp_rate": len(path_positive_times) / len(timestamps) if timestamps else None,
                    "median_first_path_clear_sec": (
                        sorted(path_latencies)[len(path_latencies) // 2]
                        if path_latencies else None
                    ),
                    "mean_oracle_after_cost_pips": sum(float(row["oracle_after_cost_pips"]) for row in all_rows) / len(all_rows) if all_rows else None,
                })
            full = [day for day in days if day["timestamp_windows"] >= int(0.75 * expected_timestamps)]
            pair_windows = sum(day["pair_windows"] for day in full)
            timestamp_windows = sum(day["timestamp_windows"] for day in full)
            return {
                "days": days,
                "full_day_count": len(full),
                "full_day_average_spread_clearing_pair_windows": sum(day["spread_clearing_pair_windows"] for day in full) / len(full) if full else None,
                "full_day_pair_window_clear_rate": sum(day["spread_clearing_pair_windows"] for day in full) / pair_windows if pair_windows else None,
                "full_day_average_timestamps_with_any_clear": sum(day["timestamps_with_any_clear"] for day in full) / len(full) if full else None,
                "full_day_any_clear_timestamp_rate": sum(day["timestamps_with_any_clear"] for day in full) / timestamp_windows if timestamp_windows else None,
                "full_day_average_currency_disjoint_oracle_count": sum(day["currency_disjoint_oracle_count"] for day in full) / len(full) if full else None,
                "full_day_average_path_clearing_pair_windows": sum(day["path_clearing_pair_windows"] for day in full) / len(full) if full else None,
                "full_day_path_pair_window_clear_rate": sum(day["path_clearing_pair_windows"] for day in full) / pair_windows if pair_windows else None,
                "full_day_average_timestamps_with_any_path_clear": sum(day["timestamps_with_any_path_clear"] for day in full) / len(full) if full else None,
                "full_day_any_path_clear_timestamp_rate": sum(day["timestamps_with_any_path_clear"] for day in full) / timestamp_windows if timestamp_windows else None,
            }

        aligned = summarize(grouped, 86400 // horizon)
        rolling = summarize(rolling_grouped, 86400 // 60)
        result[str(horizon)] = {
            **aligned,
            "sampling": "aligned_nonoverlapping",
            "rolling_every_minute": rolling,
        }
    return result


def candidate_at(instrument: str, epoch: int, timeline: Mapping[int, Mapping[str, float]], config: Mapping[str, Any]) -> dict[str, Any] | None:
    current = timeline.get(epoch); past5 = timeline.get(epoch - 300); past15 = timeline.get(epoch - 900)
    if current is None or past5 is None or past15 is None:
        return None
    spread = float(current["spread"]); maximum = float(config.get("maximum_entry_spread_pips") or 5.0)
    if spread <= 0 or spread > maximum:
        return None
    pip = pip_size(instrument)
    move5 = (float(current["mid"]) - float(past5["mid"])) / pip
    move15 = (float(current["mid"]) - float(past15["mid"])) / pip
    imbalance = float(current["imbalance_30s"])
    direction = sign(move5)
    if direction == 0 or direction != sign(move15) or direction != sign(imbalance):
        return None
    if abs(imbalance) < float(config.get("minimum_imbalance_absolute") or 0.05):
        return None
    modeled_cost = spread + float(config.get("modeled_slippage_pips") or 0.0)
    magnitude_proxy = 0.65 * abs(move5) + 0.35 * abs(move15)
    if magnitude_proxy < float(config.get("minimum_move_to_cost_ratio") or 1.5) * modeled_cost:
        return None
    preferred = float(config.get("preferred_entry_spread_pips") or 3.0)
    score = (magnitude_proxy / modeled_cost) * (1.0 + abs(imbalance))
    if spread > preferred:
        score *= preferred / spread
    return {"instrument": instrument, "epoch": epoch, "direction": direction,
            "score": score, "move5_pips": move5, "move15_pips": move15,
            "imbalance_30s": imbalance, "entry_spread_pips": spread}


def select_disjoint(candidates: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []; used: set[str] = set()
    for candidate in sorted(candidates, key=lambda row: (-float(row["score"]), str(row["instrument"]))):
        legs = currency_legs(str(candidate["instrument"]))
        if used.isdisjoint(legs):
            selected.append(candidate); used.update(legs)
            if len(selected) == count:
                break
    return selected if len(selected) == count else []


def metric(rows: list[dict[str, Any]], value_key: str = "net_pips") -> dict[str, Any]:
    values = [float(row[value_key]) for row in rows]
    gross = sum(max(value, 0) for value in values); loss = -sum(min(value, 0) for value in values)
    days = {row["utc_day"] for row in rows}
    best=max(values) if values else None
    return {"n": len(values), "days": len(days), "decisions_per_observed_day": len(values) / len(days) if days else None,
            "wins": sum(value > 0 for value in values), "win_rate": sum(value > 0 for value in values) / len(values) if values else None,
            "average_net_pips": sum(values) / len(values) if values else None, "total_net_pips": sum(values),
            "profit_factor": gross / loss if loss else None, "minimum_net_pips": min(values) if values else None,
            "maximum_net_pips": best,
            "average_without_best_result_pips": (sum(values)-best)/(len(values)-1) if len(values)>1 else None,
            "proof_eligible": False}


def combiner_backtest(by_pair: Mapping[str, Mapping[int, Mapping[str, float]]], config: Mapping[str, Any]) -> dict[str, Any]:
    slippage = float(config.get("modeled_slippage_pips") or 0.0)
    output: dict[str, Any] = {}
    epochs = sorted({epoch for timeline in by_pair.values() for epoch in timeline})
    for horizon in config.get("combiner_horizons_sec") or []:
        horizon = int(horizon); top_rows=[]; top_flip=[]; basket_rows=[]; basket_flip=[]; oracle=[]
        for epoch in epochs:
            if epoch % horizon:
                continue
            candidates=[]; oracle_candidates=[]
            for instrument,timeline in by_pair.items():
                current=timeline.get(epoch); end=timeline.get(epoch+horizon)
                if current is None or end is None:
                    continue
                window=modeled_window(instrument,current,end,slippage)
                if float(current["spread"]) <= float(config.get("maximum_entry_spread_pips") or 5.0):
                    oracle_candidates.append({"instrument":instrument,**window})
                candidate=candidate_at(instrument,epoch,timeline,config)
                if candidate is not None:
                    candidate={**candidate,**window}; candidates.append(candidate)
            if not oracle_candidates:
                continue
            day=dt.datetime.fromtimestamp(epoch,dt.timezone.utc).date().isoformat()
            best=max(oracle_candidates,key=lambda row:float(row["oracle_after_cost_pips"]))
            oracle.append({"utc_day":day,"net_pips":best["oracle_after_cost_pips"],"instrument":best["instrument"]})
            if candidates:
                top=max(candidates,key=lambda row:(float(row["score"]),-float(row["entry_spread_pips"])))
                cost=float(top["modeled_cost_pips"]); move=float(top["signed_move_pips"]); direction=int(top["direction"])
                net=direction*move-cost; flipped=-direction*move-cost
                common={"utc_day":day,"epoch":epoch,"instrument":top["instrument"],"score":top["score"]}
                top_rows.append({**common,"net_pips":net});top_flip.append({**common,"net_pips":flipped})
            basket=select_disjoint(candidates,3)
            if basket:
                nets=[];flips=[]
                for row in basket:
                    cost=float(row["modeled_cost_pips"]);move=float(row["signed_move_pips"]);direction=int(row["direction"])
                    nets.append(direction*move-cost);flips.append(-direction*move-cost)
                common={"utc_day":day,"epoch":epoch,"instruments":[row["instrument"] for row in basket]}
                basket_rows.append({**common,"net_pips":sum(nets)/3.0,"basket_total_net_pips":sum(nets)})
                basket_flip.append({**common,"net_pips":sum(flips)/3.0,"basket_total_net_pips":sum(flips)})
        midpoint = epochs[len(epochs)//2] if epochs else 0
        def split(rows: list[dict[str,Any]]) -> dict[str,Any]:
            return {"all":metric(rows),"early":metric([row for row in rows if int(row.get("epoch") or 0)<midpoint]),
                    "late":metric([row for row in rows if int(row.get("epoch") or 0)>=midpoint])}
        output[str(horizon)]={"strict_top_one":split(top_rows),"strict_top_one_flipped_control":split(top_flip),
                              "exactly_three_currency_disjoint":split(basket_rows),
                              "exactly_three_currency_disjoint_flipped_control":split(basket_flip),
                              "oracle_upper_bound":metric(oracle),"no_trade_net_pips":0.0,
                              "split_epoch":midpoint}
    return output


def run(database: Path=DATABASE, config_path: Path=CONFIG, output: Path=OUTPUT, report: Path=REPORT, quote_snapshot: Path=QUOTE_SNAPSHOT) -> dict[str,Any]:
    global _PIP_SIZES
    config=read_json(config_path);_PIP_SIZES=load_pip_sizes(quote_snapshot);by_pair,instruments=load_minutes(database)
    census=opportunity_census(by_pair,config);combiner=combiner_backtest(by_pair,config)
    first=min((epoch for timeline in by_pair.values() for epoch in timeline),default=None)
    last=max((epoch for timeline in by_pair.values() for epoch in timeline),default=None)
    stat=database.stat();fingerprint=f"{database.resolve()}|{stat.st_size}|{stat.st_mtime_ns}"
    payload={"schema_version":1,"cohort_id":config.get("cohort_id"),"research_only":True,"execution_eligible":False,
             "proof_eligible":False,"source_database":str(database),
             "source_file_metadata_fingerprint_sha256":hashlib.sha256(fingerprint.encode()).hexdigest(),
             "instrument_count":len(instruments),"instruments":instruments,
             "venue_pip_metadata_count":len(_PIP_SIZES),
             "first_minute_utc":dt.datetime.fromtimestamp(first,dt.timezone.utc).isoformat() if first else None,
             "last_minute_utc":dt.datetime.fromtimestamp(last,dt.timezone.utc).isoformat() if last else None,
             "opportunity_definition":"absolute midpoint move minus mean entry/exit spread minus modeled slippage; hindsight magnitude only",
             "census":census,"combiner":combiner,
             "limitations":["short three-calendar-day archive","midpoint plus modeled rather than exact bid/ask execution",
                            "aligned windows only","rules are historical diagnostics and lack untouched prospective confirmation"]}
    atomic(output,json.dumps(payload,indent=2,sort_keys=True))
    lines=["# 68-Pair Spread-Clearance Census and Combiner","","Research-only; no execution or promotion path.","",
           f"- Instruments: **{len(instruments)}**",f"- Archive: **{payload['first_minute_utc']} to {payload['last_minute_utc']}**","",
           "## Non-overlapping opportunity census","",
           "| Horizon | Full days | Aligned pair clear | Rolling pair clear | Rolling any-clear | Rolling path clear | Path any-clear | Disjoint oracle/day |",
           "|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for horizon,summary in census.items():
        def fmt(value:Any)->str:return "n/a" if value is None else f"{float(value):.1f}"
        pairrate="n/a" if summary["full_day_pair_window_clear_rate"] is None else f"{summary['full_day_pair_window_clear_rate']:.1%}"
        rolling=summary["rolling_every_minute"]
        rolling_pair="n/a" if rolling["full_day_pair_window_clear_rate"] is None else f"{rolling['full_day_pair_window_clear_rate']:.1%}"
        rolling_any="n/a" if rolling["full_day_any_clear_timestamp_rate"] is None else f"{rolling['full_day_any_clear_timestamp_rate']:.1%}"
        path_pair="n/a" if rolling["full_day_path_pair_window_clear_rate"] is None else f"{rolling['full_day_path_pair_window_clear_rate']:.1%}"
        path_any="n/a" if rolling["full_day_any_path_clear_timestamp_rate"] is None else f"{rolling['full_day_any_path_clear_timestamp_rate']:.1%}"
        lines.append(f"| {horizon}s | {summary['full_day_count']} | {pairrate} | {rolling_pair} | {rolling_any} | {path_pair} | {path_any} | {fmt(summary['full_day_average_currency_disjoint_oracle_count'])} |")
    lines += ["","Counts are opportunity availability, not forecasts. Currency-disjoint counts are hindsight upper bounds.","",
              "## Frozen cross-sectional combiner","",
              "| Horizon | Arm | N | Decisions/day | Win | Avg net pips | PF | Avg without best | Late N | Late avg |",
              "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for horizon,arms in combiner.items():
        for name in ["strict_top_one","exactly_three_currency_disjoint","strict_top_one_flipped_control","exactly_three_currency_disjoint_flipped_control"]:
            overall=arms[name]["all"];late=arms[name]["late"]
            win="n/a" if overall["win_rate"] is None else f"{overall['win_rate']:.1%}";avg="n/a" if overall["average_net_pips"] is None else f"{overall['average_net_pips']:.3f}"
            pf="n/a" if overall["profit_factor"] is None else f"{overall['profit_factor']:.3f}";lateavg="n/a" if late["average_net_pips"] is None else f"{late['average_net_pips']:.3f}"
            nobest="n/a" if overall["average_without_best_result_pips"] is None else f"{overall['average_without_best_result_pips']:.3f}"
            lines.append(f"| {horizon}s | {name} | {overall['n']} | {overall['decisions_per_observed_day'] or 0:.1f} | {win} | {avg} | {pf} | {nobest} | {late['n']} | {lateavg} |")
    lines += ["","The oracle row is deliberately excluded from the strategy table because it chooses pair and direction after observing the future.",""]
    atomic(report,"\n".join(lines));return payload


def main()->int:
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--database",type=Path,default=DATABASE);parser.add_argument("--config",type=Path,default=CONFIG);parser.add_argument("--quotes",type=Path,default=QUOTE_SNAPSHOT);parser.add_argument("--output",type=Path,default=OUTPUT);parser.add_argument("--report",type=Path,default=REPORT);args=parser.parse_args();run(args.database,args.config,args.output,args.report,args.quotes);return 0


if __name__=="__main__":raise SystemExit(main())
