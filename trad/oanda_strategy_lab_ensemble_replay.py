#!/usr/bin/env python3
"""Build account-independent ensemble outcomes from a strategy-lab JSONL log.

The source lab has already captured executable bid/ask entries and future
outcomes for every raw setup. Ensemble decisions use only same-cycle setup
events. Outcome rows are attached afterward, so replay cannot use future data
to decide whether an ensemble fired.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PROFILE_PRIORITY = {"strict": 4, "balanced": 3, "fast": 2, "loose": 1}
FAMILY_CLUSTERS = {
    "momentum": "trend",
    "pullback": "trend",
    "ema_trend_cross": "trend",
    "higher_timeframe_alignment": "trend",
    "rsi_trend_continuation": "trend",
    "linear_regression_trend": "trend",
    "donchian_breakout": "breakout",
    "volatility_squeeze_breakout": "breakout",
    "range_expansion": "breakout",
    "volume_impulse": "breakout",
    "macd_rsi_reversal": "reversal",
    "bollinger_reversion": "reversal",
    "relative_value_reversion": "reversal",
    "stochastic_reversal": "reversal",
    "candlestick_reversal": "reversal",
    "atr_mean_reversion": "reversal",
    "currency_strength": "cross_sectional",
    "cross_sectional_pair_rank": "cross_sectional",
    "supervised_return_rank": "model",
    "regime_switching": "adaptive",
}


@dataclass(frozen=True)
class EnsembleSpec:
    name: str
    include_near_threshold: bool
    vote_level: str
    min_voters: int
    min_clusters: int
    min_agreement: float


DEFAULT_ENSEMBLES = (
    EnsembleSpec("accepted_unanimous_2", False, "family", 2, 1, 1.0),
    EnsembleSpec("accepted_majority_3", False, "family", 3, 2, 2.0 / 3.0),
    EnsembleSpec("near_unanimous_3", True, "family", 3, 1, 1.0),
    EnsembleSpec("diversified_majority_3", True, "cluster", 3, 3, 2.0 / 3.0),
    EnsembleSpec("broad_majority_5", True, "family", 5, 2, 0.70),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def cycle_id(row: dict[str, Any]) -> str:
    event_id = str(row.get("id") or "")
    lane_id = str(row.get("lane_id") or "")
    instrument = str(row.get("instrument") or "")
    suffix = f"-{lane_id}-{instrument}"
    if event_id.endswith(suffix):
        return event_id[: -len(suffix)]
    return str(row.get("signal_candle_time") or row.get("time") or "")


def compact_setup(row: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "event",
        "id",
        "lane_id",
        "family",
        "profile",
        "instrument",
        "direction",
        "time",
        "signal_candle_time",
        "entry_time",
        "entry_bid",
        "entry_ask",
        "miss_class",
    )
    return {key: row.get(key) for key in keys if key in row}


def collect_source_row(
    row: dict[str, Any],
    start: dict[str, Any],
    groups: dict[tuple[str, str], list[dict[str, Any]]],
    outcomes: dict[tuple[str, int], float],
    eligible_ids: set[str],
) -> None:
    event = row.get("event")
    if event == "lab_start":
        start.clear()
        start.update(row)
        return
    if event == "shadow_signal" or (
        event == "shadow_miss" and row.get("miss_class") == "near_threshold"
    ):
        compact = compact_setup(row)
        event_id = str(compact.get("id") or "")
        instrument = str(compact.get("instrument") or "")
        if event_id and instrument:
            eligible_ids.add(event_id)
            groups[(cycle_id(compact), instrument)].append(compact)
        return
    if event == "shadow_outcome":
        event_id = str(row.get("id") or "")
        horizon = int(safe_float(row.get("horizon_sec")))
        if event_id in eligible_ids and horizon > 0:
            outcomes[(event_id, horizon)] = safe_float(row.get("theoretical_pips"))


def setup_is_eligible(row: dict[str, Any], spec: EnsembleSpec) -> bool:
    event = row.get("event")
    if event == "shadow_signal":
        return True
    return bool(
        spec.include_near_threshold
        and event == "shadow_miss"
        and row.get("miss_class") == "near_threshold"
    )


def majority_direction(votes: dict[str, str]) -> tuple[str | None, float, int, int]:
    counts = Counter(votes.values())
    long_count = counts["buy"]
    short_count = counts["sell"]
    total = long_count + short_count
    if total <= 0 or long_count == short_count:
        return None, 0.0, long_count, short_count
    direction = "buy" if long_count > short_count else "sell"
    agreement = max(long_count, short_count) / total
    return direction, agreement, long_count, short_count


def collapse_family_votes(rows: Iterable[dict[str, Any]]) -> dict[str, str]:
    directions: dict[str, Counter[str]] = defaultdict(Counter)
    observed_profiles: set[tuple[str, str, str]] = set()
    for row in rows:
        family = str(row.get("family") or "")
        profile = str(row.get("profile") or "")
        direction = str(row.get("direction") or "")
        if not family or direction not in {"buy", "sell"}:
            continue
        profile_key = (family, profile, direction)
        if profile_key in observed_profiles:
            continue
        observed_profiles.add(profile_key)
        directions[family][direction] += 1

    votes: dict[str, str] = {}
    for family, counts in directions.items():
        if counts["buy"] > counts["sell"]:
            votes[family] = "buy"
        elif counts["sell"] > counts["buy"]:
            votes[family] = "sell"
    return votes


def collapse_cluster_votes(family_votes: dict[str, str]) -> dict[str, str]:
    cluster_directions: dict[str, Counter[str]] = defaultdict(Counter)
    for family, direction in family_votes.items():
        cluster = FAMILY_CLUSTERS.get(family, "other")
        cluster_directions[cluster][direction] += 1
    votes: dict[str, str] = {}
    for cluster, counts in cluster_directions.items():
        if counts["buy"] > counts["sell"]:
            votes[cluster] = "buy"
        elif counts["sell"] > counts["buy"]:
            votes[cluster] = "sell"
    return votes


def candidate_for_group(
    spec: EnsembleSpec,
    group_key: tuple[str, str],
    rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    eligible = [row for row in rows if setup_is_eligible(row, spec)]
    family_votes = collapse_family_votes(eligible)
    cluster_votes = collapse_cluster_votes(family_votes)
    active_votes = cluster_votes if spec.vote_level == "cluster" else family_votes
    direction, agreement, long_count, short_count = majority_direction(active_votes)
    if direction is None:
        return None
    if len(active_votes) < spec.min_voters or len(cluster_votes) < spec.min_clusters:
        return None
    if agreement + 1e-12 < spec.min_agreement:
        return None

    supporting_rows = [
        row
        for row in eligible
        if family_votes.get(str(row.get("family") or "")) == direction
    ]
    if not supporting_rows:
        return None
    supporting_rows.sort(
        key=lambda row: (
            row.get("event") == "shadow_signal",
            PROFILE_PRIORITY.get(str(row.get("profile") or ""), 0),
            str(row.get("lane_id") or ""),
        ),
        reverse=True,
    )
    representative = supporting_rows[0]
    cycle, instrument = group_key
    accepted_families = {
        str(row.get("family") or "")
        for row in supporting_rows
        if row.get("event") == "shadow_signal"
    }
    near_families = {
        str(row.get("family") or "")
        for row in supporting_rows
        if row.get("event") == "shadow_miss"
    }
    return {
        "id": f"{cycle}-{spec.name}-{instrument}",
        "ensemble": spec.name,
        "cycle_id": cycle,
        "instrument": instrument,
        "direction": direction,
        "decision_time": max(str(row.get("time") or "") for row in eligible),
        "signal_candle_time": str(representative.get("signal_candle_time") or ""),
        "entry_time": str(representative.get("entry_time") or ""),
        "entry_bid": safe_float(representative.get("entry_bid")),
        "entry_ask": safe_float(representative.get("entry_ask")),
        "agreement": round(agreement, 6),
        "vote_level": spec.vote_level,
        "voter_count": len(active_votes),
        "family_count": len(family_votes),
        "cluster_count": len(cluster_votes),
        "long_votes": long_count,
        "short_votes": short_count,
        "family_votes": dict(sorted(family_votes.items())),
        "cluster_votes": dict(sorted(cluster_votes.items())),
        "accepted_family_count": len(accepted_families),
        "near_family_count": len(near_families),
        "supporting_ids": sorted(
            {
                str(row.get("id") or "")
                for row in supporting_rows
                if str(row.get("direction") or "") == direction
            }
        ),
    }


def parse_source(
    path: Path,
) -> tuple[dict[str, Any], dict[tuple[str, str], list[dict[str, Any]]], dict[tuple[str, int], float]]:
    start: dict[str, Any] = {}
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    outcomes: dict[tuple[str, int], float] = {}
    eligible_ids: set[str] = set()
    with path.open("r", encoding="utf-8-sig") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                collect_source_row(row, start, groups, outcomes, eligible_ids)
    return start, groups, outcomes


class SourceAccumulator:
    """Incrementally tail one append-only lab log without re-reading old rows."""

    def __init__(self, path: Path):
        self.path = path
        self.offset = 0
        self.remainder = b""
        self.start: dict[str, Any] = {}
        self.groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        self.outcomes: dict[tuple[str, int], float] = {}
        self.eligible_ids: set[str] = set()

    def reset(self) -> None:
        self.offset = 0
        self.remainder = b""
        self.start.clear()
        self.groups.clear()
        self.outcomes.clear()
        self.eligible_ids.clear()

    def read_available(self) -> int:
        try:
            current_size = self.path.stat().st_size
        except OSError:
            return 0
        if current_size < self.offset:
            self.reset()
        try:
            with self.path.open("rb") as handle:
                handle.seek(self.offset)
                chunk = handle.read()
                self.offset = handle.tell()
        except OSError:
            return 0
        if not chunk:
            return 0

        payload = self.remainder + chunk
        parts = payload.split(b"\n")
        self.remainder = parts.pop()
        rows_read = 0
        for raw_line in parts:
            if not raw_line:
                continue
            try:
                row = json.loads(raw_line.decode("utf-8-sig"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(row, dict):
                collect_source_row(
                    row,
                    self.start,
                    self.groups,
                    self.outcomes,
                    self.eligible_ids,
                )
                rows_read += 1
        return rows_read


def attach_outcomes(
    candidates: list[dict[str, Any]],
    outcomes: dict[tuple[str, int], float],
    horizons: Iterable[int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        for horizon in horizons:
            values = [
                outcomes[(event_id, horizon)]
                for event_id in candidate["supporting_ids"]
                if (event_id, horizon) in outcomes
            ]
            if not values:
                continue
            rows.append(
                {
                    "event": "ensemble_outcome",
                    "ensemble_id": candidate["id"],
                    "ensemble": candidate["ensemble"],
                    "instrument": candidate["instrument"],
                    "direction": candidate["direction"],
                    "horizon_sec": horizon,
                    "theoretical_pips": round(statistics.median(values), 3),
                    "constituent_outcome_count": len(values),
                    "constituent_dispersion_pips": round(max(values) - min(values), 6),
                }
            )
    return rows


def sample_stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "n": 0,
            "avg": 0.0,
            "median": 0.0,
            "win_rate": 0.0,
            "best": 0.0,
            "worst": 0.0,
            "total_pips": 0.0,
        }
    return {
        "n": len(values),
        "avg": round(statistics.fmean(values), 3),
        "median": round(statistics.median(values), 3),
        "win_rate": round(100.0 * sum(value > 0.0 for value in values) / len(values), 1),
        "best": round(max(values), 3),
        "worst": round(min(values), 3),
        "total_pips": round(sum(values), 3),
    }


def build_report_from_state(
    source: Path,
    start: dict[str, Any],
    groups: dict[tuple[str, str], list[dict[str, Any]]],
    source_outcomes: dict[tuple[str, int], float],
    specs: Iterable[EnsembleSpec] = DEFAULT_ENSEMBLES,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    specs = tuple(specs)
    candidates: list[dict[str, Any]] = []
    for spec in specs:
        for group_key, rows in groups.items():
            candidate = candidate_for_group(spec, group_key, rows)
            if candidate is not None:
                candidates.append(candidate)
    horizons = [int(value) for value in start.get("outcome_horizons") or [60, 180, 300]]
    outcome_rows = attach_outcomes(candidates, source_outcomes, horizons)

    outcomes_by_ensemble: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in outcome_rows:
        outcomes_by_ensemble[row["ensemble"]][row["horizon_sec"]].append(row["theoretical_pips"])
    signal_counts = Counter(candidate["ensemble"] for candidate in candidates)
    summaries = []
    for spec in specs:
        summaries.append(
            {
                "ensemble": spec.name,
                "signals": signal_counts[spec.name],
                "horizons": {
                    str(horizon): sample_stats(outcomes_by_ensemble[spec.name][horizon])
                    for horizon in horizons
                },
            }
        )
    report = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "evaluation_mode": "same_cycle_event_replay",
        "account_required": False,
        "places_orders": False,
        "decision_uses_outcomes": False,
        "source_log": str(source.resolve()),
        "source_run_label": str(start.get("run_label") or ""),
        "source_mode": str(start.get("mode") or ""),
        "source_account_suffix": start.get("account_suffix"),
        "source_lane_count": int(start.get("lane_count") or 0),
        "source_instrument_count": int(start.get("instrument_count") or 0),
        "source_setup_groups": len(groups),
        "ensemble_specs": [asdict(spec) for spec in specs],
        "summaries": summaries,
        "candidate_count": len(candidates),
        "outcome_count": len(outcome_rows),
        "method_note": (
            "Each family contributes at most one same-cycle vote. Four parameter profiles "
            "cannot multiply a family's influence. Cost-aware outcomes are attached only "
            "after candidates have been selected."
        ),
    }
    events = [
        {"event": "ensemble_signal", **candidate}
        for candidate in sorted(candidates, key=lambda row: (row["decision_time"], row["ensemble"], row["instrument"]))
    ]
    events.extend(outcome_rows)
    return report, events


def build_report(
    source: Path,
    specs: Iterable[EnsembleSpec] = DEFAULT_ENSEMBLES,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    start, groups, source_outcomes = parse_source(source)
    return build_report_from_state(source, start, groups, source_outcomes, specs)


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def atomic_write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    temporary.replace(path)


def output_paths(source: Path, output: Path | None, events_output: Path | None) -> tuple[Path, Path]:
    derived_stem = f"strategy_ensemble_{source.stem}"
    summary = output or source.with_name(derived_stem + ".json")
    events = events_output or source.with_name(derived_stem + "_events.jsonl")
    return summary, events


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Strategy-lab JSONL source")
    parser.add_argument("--output", type=Path, default=None, help="Ensemble summary JSON")
    parser.add_argument("--events-output", type=Path, default=None, help="Ensemble event JSONL")
    parser.add_argument("--watch-sec", type=float, default=0.0, help="Refresh interval; zero runs once")
    parser.add_argument("--duration-sec", type=float, default=0.0, help="Optional watch duration; zero is unlimited")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    if args.watch_sec < 0.0 or args.duration_sec < 0.0:
        raise SystemExit("watch and duration values cannot be negative")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.source.is_file():
        raise SystemExit(f"Source log does not exist: {args.source}")
    summary_path, events_path = output_paths(args.source, args.output, args.events_output)
    stop_at = time.monotonic() + args.duration_sec if args.duration_sec > 0.0 else math.inf
    accumulator = SourceAccumulator(args.source) if args.watch_sec > 0.0 else None
    while True:
        if accumulator is None:
            report, events = build_report(args.source)
        else:
            accumulator.read_available()
            report, events = build_report_from_state(
                args.source,
                accumulator.start,
                accumulator.groups,
                accumulator.outcomes,
            )
        atomic_write_json(summary_path, report)
        atomic_write_jsonl(events_path, events)
        if not args.quiet:
            print(
                json.dumps(
                    {
                        "generated_utc": report["generated_utc"],
                        "source": str(args.source),
                        "summary": str(summary_path),
                        "events": str(events_path),
                        "candidates": report["candidate_count"],
                        "outcomes": report["outcome_count"],
                        "ensembles": report["summaries"],
                    },
                    indent=2,
                ),
                flush=True,
            )
        if args.watch_sec <= 0.0 or time.monotonic() >= stop_at:
            break
        time.sleep(min(args.watch_sec, max(0.0, stop_at - time.monotonic())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
