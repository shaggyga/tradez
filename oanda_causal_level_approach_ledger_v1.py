#!/usr/bin/env python3
"""Research-only causal M5 level-approach ledger.

The module freezes levels and approach features before outcomes exist.  It
never selects a trade side and has no broker, authorization, promotion, or
watchlist surface.  Historical CSV replay is diagnostic and cannot create
prospective evidence.  A future live pre-outcome collector must use a new,
separately reviewed contract and cohort.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parent
DEFAULT_CANDLE_ROOT = ROOT / "data" / "oanda_training_manager" / "candles"
DEFAULT_DATABASE = (
    ROOT / "data" / "oanda_training_manager" / "research" /
    "causal_level_approach_ledger_v1.sqlite"
)
SCHEMA_VERSION = "causal_level_approach_ledger_v1"
CONTRACT_ID = "causal_level_approach_ledger_v1_frozen_20260827b"
COHORT_ID = "causal_level_approach_ledger_v1_historical_diagnostic_20260827b"
HORIZONS_MIN = (5, 15, 30, 60, 120, 240)
PRIMARY_HORIZON_MIN = 60
SECONDARY_HORIZON_MIN = 240
PIVOT_LEFT = 2
PIVOT_RIGHT = 2
ATR_WINDOW = 14
EQUILIBRIUM_WINDOW = 20
LEVEL_LOOKBACK_BARS = 288
CLUSTER_ATR = 0.15
APPROACH_ZONE_ATR = 0.25
REARM_LEAVE_ATR = 0.75
BREAK_ATR = 0.10
REJECT_AWAY_ATR = 0.50
NEW_YORK = ZoneInfo("America/New_York")
WEEKLY_MIN_COVERAGE_RATIO = 0.97
WEEKLY_MAX_OPEN_DELAY = timedelta(minutes=15)
WEEKLY_MAX_INTERNAL_GAP = timedelta(minutes=60)
WEEKLY_BOUNDARY_RULE = (
    "fx_week_sun1700_ny_first_bar_by_1715_friday_close_exact_"
    "coverage_gte_0.97_max_gap_lte_60m"
)

POLICY = {
    "research_only": True,
    "execution_eligible": False,
    "can_place_orders": False,
    "can_authorize": False,
    "can_promote": False,
    "broker_access": False,
    "watchlist_mutation": False,
    "selected_pair_or_side": False,
    "historical_replay_can_create_prospective_evidence": False,
    "prospective_capture_supported": False,
}


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: Any) -> datetime:
    text = str(value).strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def digest(*values: Any) -> str:
    raw = "\x1f".join(str(value) for value in values).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def pip_size(instrument: str) -> float:
    return 0.01 if instrument.upper().endswith("_JPY") else 0.0001


@dataclass(frozen=True)
class Bar:
    timestamp: datetime
    bid_open: float
    bid_high: float
    bid_low: float
    bid_close: float
    ask_open: float
    ask_high: float
    ask_low: float
    ask_close: float

    @property
    def known_at(self) -> datetime:
        return self.timestamp + timedelta(minutes=5)

    @property
    def mid_open(self) -> float:
        return (self.bid_open + self.ask_open) / 2.0

    @property
    def mid_high(self) -> float:
        return (self.bid_high + self.ask_high) / 2.0

    @property
    def mid_low(self) -> float:
        return (self.bid_low + self.ask_low) / 2.0

    @property
    def mid_close(self) -> float:
        return (self.bid_close + self.ask_close) / 2.0


@dataclass(frozen=True)
class Level:
    level_id: str
    price: float
    kind: str
    first_pivot_index: int
    last_pivot_index: int
    confirmed_at_index: int
    touches: int
    source: str = "confirmed_pivot_cluster"
    name: str = "cluster"
    weekly_coverage_ratio: float | None = None
    weekly_missing_bars: int | None = None
    weekly_max_gap_minutes: float | None = None
    weekly_boundary_rule: str | None = None


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def atr_at(bars: Sequence[Bar], decision_index: int) -> float:
    start = max(1, decision_index - ATR_WINDOW + 1)
    values = []
    for index in range(start, decision_index + 1):
        previous = bars[index - 1].mid_close
        values.append(max(
            bars[index].mid_high - bars[index].mid_low,
            abs(bars[index].mid_high - previous),
            abs(bars[index].mid_low - previous),
        ))
    return _mean(values)


def _is_contiguous(bars: Sequence[Bar], start: int, end: int) -> bool:
    if start < 0 or end >= len(bars) or start > end:
        return False
    return all(
        bars[index].timestamp - bars[index - 1].timestamp == timedelta(minutes=5)
        for index in range(start + 1, end + 1)
    )


def _spread_price(bar: Bar) -> float:
    return max(0.0, bar.ask_close - bar.bid_close)


def executable_scale(
    instrument: str, bars: Sequence[Bar], index: int, atr: float
) -> float:
    trailing = [_spread_price(row) for row in bars[max(0, index - 11):index + 1]]
    trailing_spread = max(trailing, default=0.0)
    return max(pip_size(instrument), 1.5 * trailing_spread, BREAK_ATR * atr)


def confirmed_pivots(
    bars: Sequence[Bar], decision_index: int
) -> list[tuple[int, str, float, int]]:
    """Return pivots whose right-hand confirmation bars are already complete."""

    maximum = decision_index - PIVOT_RIGHT
    minimum = max(PIVOT_LEFT, decision_index - LEVEL_LOOKBACK_BARS)
    output: list[tuple[int, str, float, int]] = []
    for index in range(minimum, maximum + 1):
        high = bars[index].mid_high
        low = bars[index].mid_low
        if all(high > bars[index - offset].mid_high for offset in range(1, PIVOT_LEFT + 1)) and all(
            high >= bars[index + offset].mid_high for offset in range(1, PIVOT_RIGHT + 1)
        ):
            output.append((index, "resistance", high, index + PIVOT_RIGHT))
        if all(low < bars[index - offset].mid_low for offset in range(1, PIVOT_LEFT + 1)) and all(
            low <= bars[index + offset].mid_low for offset in range(1, PIVOT_RIGHT + 1)
        ):
            output.append((index, "support", low, index + PIVOT_RIGHT))
    return output


def build_levels(bars: Sequence[Bar], decision_index: int, atr: float) -> list[Level]:
    tolerance = max(atr * CLUSTER_ATR, 1e-12)
    pivots = sorted(confirmed_pivots(bars, decision_index), key=lambda row: (row[1], row[2], row[0]))
    clusters: list[list[tuple[int, str, float, int]]] = []
    for pivot in pivots:
        matching = next(
            (
                cluster for cluster in clusters
                if cluster[0][1] == pivot[1]
                and abs(pivot[2] - _mean([row[2] for row in cluster])) <= tolerance
            ),
            None,
        )
        if matching is None:
            clusters.append([pivot])
        else:
            matching.append(pivot)
    levels = []
    for cluster in clusters:
        price = _mean([row[2] for row in cluster])
        first = min(row[0] for row in cluster)
        last = max(row[0] for row in cluster)
        confirmed = max(row[3] for row in cluster)
        kind = cluster[0][1]
        # Identity is anchored to the originating confirmed pivot, not the
        # mutable cluster centroid. Later causal touches can refine price and
        # touch count without resetting rearm/rejection history.
        anchor = min(cluster, key=lambda row: row[0])
        levels.append(Level(
            level_id="level_" + digest(kind, round(anchor[2], 10), bars[anchor[0]].timestamp)[:24],
            price=price,
            kind=kind,
            first_pivot_index=first,
            last_pivot_index=last,
            confirmed_at_index=confirmed,
            touches=len(cluster),
        ))
    return sorted(levels, key=lambda level: (level.price, -level.touches, level.level_id))


def _last_completed_fx_week(decision_utc: datetime) -> tuple[datetime, datetime]:
    local = decision_utc.astimezone(NEW_YORK)
    friday = local.date() - timedelta(days=(local.weekday() - 4) % 7)
    boundary = datetime.combine(friday, datetime.min.time(), tzinfo=NEW_YORK).replace(hour=17)
    if local < boundary:
        boundary -= timedelta(days=7)
    start = boundary - timedelta(days=5)
    return start.astimezone(timezone.utc), boundary.astimezone(timezone.utc)


def prior_completed_week_pivot_levels(
    instrument: str,
    bars: Sequence[Bar],
    decision_index: int,
) -> list[Level]:
    """Traditional P/R1/R2/S1/S2 from the last completed FX week only."""

    decision_clock = bars[decision_index].known_at
    week_start, week_end = _last_completed_fx_week(decision_clock)
    times = [row.timestamp for row in bars]
    first_index = bisect.bisect_left(times, week_start, 0, decision_index + 1)
    last_open = week_end - timedelta(minutes=5)
    last_index = bisect.bisect_right(times, last_open, first_index, decision_index + 1) - 1
    expected_count = 5 * 24 * 12
    actual_count = max(0, last_index - first_index + 1)
    first_delay = (
        bars[first_index].timestamp - week_start
        if first_index <= last_index and first_index < len(bars) else timedelta.max
    )
    gaps = [
        bars[index].timestamp - bars[index - 1].timestamp
        for index in range(first_index + 1, last_index + 1)
    ] if first_index <= last_index else []
    max_gap = max(gaps, default=timedelta(minutes=5))
    valid_grid = all(
        gap >= timedelta(minutes=5)
        and gap.total_seconds() % timedelta(minutes=5).total_seconds() == 0
        for gap in gaps
    )
    coverage_ratio = actual_count / expected_count
    missing_bars = max(0, expected_count - actual_count)
    if (
        first_index > last_index
        or first_index >= len(bars)
        or first_delay < timedelta(0)
        or first_delay > WEEKLY_MAX_OPEN_DELAY
        or bars[last_index].timestamp != last_open
        or coverage_ratio < WEEKLY_MIN_COVERAGE_RATIO
        or max_gap > WEEKLY_MAX_INTERNAL_GAP
        or not valid_grid
    ):
        return []
    completed = list(enumerate(bars[first_index:last_index + 1], start=first_index))
    high = max(row.mid_high for _, row in completed)
    low = min(row.mid_low for _, row in completed)
    close = completed[-1][1].mid_close
    pivot = (high + low + close) / 3.0
    values = {
        "P": pivot,
        "R1": 2.0 * pivot - low,
        "R2": pivot + (high - low),
        "S1": 2.0 * pivot - high,
        "S2": pivot - (high - low),
    }
    current = bars[decision_index].mid_close
    levels = []
    for name, price in values.items():
        kind = "resistance" if price >= current else "support"
        levels.append(Level(
            level_id="weekly_level_" + digest(instrument.upper(), iso(week_end), name, CONTRACT_ID)[:24],
            price=price,
            kind=kind,
            first_pivot_index=completed[0][0],
            last_pivot_index=completed[-1][0],
            confirmed_at_index=completed[-1][0],
            touches=1,
            source="prior_completed_week_traditional_pivot",
            name=name,
            weekly_coverage_ratio=coverage_ratio,
            weekly_missing_bars=missing_bars,
            weekly_max_gap_minutes=max_gap.total_seconds() / 60.0,
            weekly_boundary_rule=WEEKLY_BOUNDARY_RULE,
        ))
    return sorted(levels, key=lambda level: (level.price, level.name))


def select_approached_level(
    instrument: str, bars: Sequence[Bar], decision_index: int, levels: Sequence[Level], atr: float
) -> Level | None:
    now = bars[decision_index].mid_close
    previous = bars[decision_index - 1].mid_close
    candidates = []
    zone = max(APPROACH_ZONE_ATR * atr, executable_scale(instrument, bars, decision_index, atr))
    for level in levels:
        prior_distance = abs(previous - level.price)
        distance = abs(now - level.price)
        correct_side = now <= level.price if level.kind == "resistance" else now >= level.price
        if correct_side and distance <= zone and distance < prior_distance:
            candidates.append((distance, -level.touches, level.level_id, level))
    return min(candidates, default=(None, None, None, None))[3]


def _session(at: datetime) -> str:
    hour = at.hour
    if 0 <= hour < 7:
        return "asia"
    if 7 <= hour < 12:
        return "london"
    if 12 <= hour < 16:
        return "overlap"
    if 16 <= hour < 21:
        return "new_york"
    return "rollover"


def frozen_features(
    instrument: str,
    bars: Sequence[Bar],
    decision_index: int,
    level: Level,
    prior_rejections: int,
) -> dict[str, Any]:
    atr = atr_at(bars, decision_index)
    pip = pip_size(instrument)
    close = bars[decision_index].mid_close
    direction = 1.0 if level.kind == "resistance" else -1.0
    distance = abs(level.price - close)
    velocities = {}
    for lookback in (1, 3, 5):
        old_distance = abs(level.price - bars[decision_index - lookback].mid_close)
        velocities[f"approach_velocity_{lookback}_pips_per_bar"] = (old_distance - distance) / lookback / pip
    equilibrium = _mean([bar.mid_close for bar in bars[max(0, decision_index - EQUILIBRIUM_WINDOW + 1):decision_index + 1]])
    impulse = direction * (close - bars[decision_index - 5].mid_close)
    spread_pips = (bars[decision_index].ask_close - bars[decision_index].bid_close) / pip
    return {
        "feature_cutoff_utc": iso(bars[decision_index].known_at),
        "level_id": level.level_id,
        "level_kind": level.kind,
        "level_source": level.source,
        "level_name": level.name,
        "level_price": level.price,
        "level_known_at_utc": iso(bars[level.confirmed_at_index].known_at),
        "level_age_bars": decision_index - level.confirmed_at_index,
        "level_touches": level.touches,
        "weekly_coverage_ratio": level.weekly_coverage_ratio,
        "weekly_missing_bars": level.weekly_missing_bars,
        "weekly_max_gap_minutes": level.weekly_max_gap_minutes,
        "weekly_boundary_rule": level.weekly_boundary_rule,
        "prior_causal_rejections": prior_rejections,
        "distance_pips": distance / pip,
        "distance_atr": distance / atr,
        "distance_spread_multiple": distance / max(spread_pips * pip, 1e-12),
        **velocities,
        "approach_acceleration_pips_per_bar2": velocities["approach_velocity_1_pips_per_bar"] - velocities["approach_velocity_3_pips_per_bar"],
        "impulse_5_atr": impulse / atr,
        "equilibrium_gap_atr": direction * (close - equilibrium) / atr,
        "stretch_5_atr": abs(close - bars[decision_index - 5].mid_close) / atr,
        "atr_pips": atr / pip,
        "spread_pips": spread_pips,
        "session": _session(bars[decision_index].known_at),
    }


def _path_metrics(
    bars: Sequence[Bar], entry_index: int, end_index: int, entry_bid: float,
    entry_ask: float, pip: float, long_side: bool,
) -> dict[str, Any]:
    results = []
    for bar in bars[entry_index:end_index + 1]:
        if long_side:
            terminal = (bar.bid_close - entry_ask) / pip
            best = (bar.bid_high - entry_ask) / pip
            worst = (bar.bid_low - entry_ask) / pip
        else:
            terminal = (entry_bid - bar.ask_close) / pip
            best = (entry_bid - bar.ask_low) / pip
            worst = (entry_bid - bar.ask_high) / pip
        results.append((bar.known_at, terminal, best, worst))
    clear_index = next((index for index, (_, _, best, _) in enumerate(results) if best > 0.0), None)
    clear_end = None if clear_index is None else results[clear_index][0]
    clear_start = None if clear_end is None else clear_end - timedelta(minutes=5)
    return {
        "terminal_pips": results[-1][1],
        "mfe_pips": max(row[2] for row in results),
        "mae_pips": min(row[3] for row in results),
        "cost_clear_time_observation": "m5_interval_censored",
        "cost_clear_interval_start_utc": None if clear_start is None else iso(clear_start),
        "cost_clear_interval_end_utc": None if clear_end is None else iso(clear_end),
    }


def mature_outcome(
    instrument: str, bars: Sequence[Bar], entry_index: int, level: Level,
    atr: float, horizon_min: int,
) -> dict[str, Any]:
    steps = horizon_min // 5
    end = entry_index + steps - 1
    if entry_index >= len(bars) or end >= len(bars) or not _is_contiguous(bars, entry_index, end):
        raise ValueError("outcome_not_mature")
    entry = bars[entry_index]
    future = bars[entry_index:end + 1]
    if future[-1].known_at != entry.timestamp + timedelta(minutes=horizon_min):
        raise ValueError("outcome_not_mature")
    scale = executable_scale(instrument, bars, entry_index, atr)
    break_buffer = scale
    reject_distance = max(scale, REJECT_AWAY_ATR * atr)
    first_passage = None
    first_index = None
    for offset, bar in enumerate(future):
        if level.kind == "resistance":
            hit_break = bar.mid_high >= level.price + break_buffer
            hit_reject = bar.mid_low <= level.price - reject_distance
        else:
            hit_break = bar.mid_low <= level.price - break_buffer
            hit_reject = bar.mid_high >= level.price + reject_distance
        if hit_break and hit_reject:
            first_passage, first_index = "ambiguous_intrabar", offset
            break
        if hit_reject:
            first_passage, first_index = "reject-away-before-break", offset
            break
        if hit_break:
            first_passage, first_index = "break", offset
            break
    if first_passage == "break":
        after = future[int(first_index):]
        reclaimed = any(
            bar.mid_close < level.price if level.kind == "resistance" else bar.mid_close > level.price
            for bar in after
        )
        label = "false-break-reclaim" if reclaimed else "penetrate-and-hold"
    else:
        label = first_passage or "neither"
    pip = pip_size(instrument)
    return {
        "horizon_min": horizon_min,
        "label": label,
        "market_response_label": label,
        "first_passage_bar_offset": first_index,
        "barrier_scale_price": scale,
        "break_barrier_price": level.price + scale if level.kind == "resistance" else level.price - scale,
        "reject_barrier_price": level.price - reject_distance if level.kind == "resistance" else level.price + reject_distance,
        "entry_utc": iso(entry.timestamp),
        "maturity_utc": iso(future[-1].known_at),
        "entry_bid": entry.bid_open,
        "entry_ask": entry.ask_open,
        "long_path": _path_metrics(future, 0, len(future) - 1, entry.bid_open, entry.ask_open, pip, True),
        "short_path": _path_metrics(future, 0, len(future) - 1, entry.bid_open, entry.ask_open, pip, False),
        "selected_side": None,
    }


def scan_instrument(instrument: str, bars: Sequence[Bar]) -> list[dict[str, Any]]:
    episodes: list[dict[str, Any]] = []
    armed: dict[str, bool] = {}
    rejection_counts: dict[str, int] = {}
    pending_rejections: list[tuple[int, str]] = []
    weekly_cache: dict[datetime, list[Level]] = {}
    start = max(ATR_WINDOW, EQUILIBRIUM_WINDOW, 5) + PIVOT_LEFT + PIVOT_RIGHT
    for index in range(start, len(bars) - max(HORIZONS_MIN) // 5 - 1):
        matured = [row for row in pending_rejections if row[0] <= index]
        pending_rejections = [row for row in pending_rejections if row[0] > index]
        for _, level_id in matured:
            rejection_counts[level_id] = rejection_counts.get(level_id, 0) + 1
        atr = atr_at(bars, index)
        if atr <= 0.0:
            continue
        _, week_end = _last_completed_fx_week(bars[index].known_at)
        if week_end not in weekly_cache:
            weekly_cache[week_end] = prior_completed_week_pivot_levels(instrument, bars, index)
        levels = build_levels(bars, index, atr) + weekly_cache[week_end]
        active_ids = {level.level_id for level in levels}
        for level in levels:
            distance = abs(bars[index].mid_close - level.price)
            if level.level_id not in armed:
                armed[level.level_id] = True
            elif not armed[level.level_id] and distance >= max(
                REARM_LEAVE_ATR * atr, executable_scale(instrument, bars, index, atr)
            ):
                armed[level.level_id] = True
        level = select_approached_level(instrument, bars, index, levels, atr)
        if level is None or not armed.get(level.level_id, True):
            continue
        features = frozen_features(
            instrument, bars, index, level, rejection_counts.get(level.level_id, 0)
        )
        if bars[index + 1].timestamp != bars[index].timestamp + timedelta(minutes=5):
            continue
        try:
            outcomes = [
                mature_outcome(instrument, bars, index + 1, level, atr, horizon)
                for horizon in HORIZONS_MIN
            ]
        except ValueError:
            continue
        if outcomes[0]["label"] == "reject-away-before-break":
            pending_rejections.append((index + 1 + HORIZONS_MIN[0] // 5, level.level_id))
        observed = bars[index].known_at
        evidence_class = "historical_diagnostic"
        episode_id = "approach_" + digest(instrument, level.level_id, iso(observed), CONTRACT_ID)[:28]
        episodes.append({
            "episode_id": episode_id,
            "instrument": instrument,
            "decision_utc": iso(observed),
            "entry_utc": outcomes[0]["entry_utc"],
            "evidence_class": evidence_class,
            "prospective_proof_eligible": evidence_class == "prospective_v1",
            "features": features,
            "outcomes": outcomes,
            "research_only": True,
            "execution_eligible": False,
            "selected_pair_or_side": None,
        })
        armed[level.level_id] = False
        # Drop arm states only for levels no longer in the causal lookback.
        armed = {key: value for key, value in armed.items() if key in active_ids}
    return episodes


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS level_approach_episode (
          episode_id TEXT PRIMARY KEY,instrument TEXT NOT NULL,decision_utc TEXT NOT NULL,
          entry_utc TEXT NOT NULL,evidence_class TEXT NOT NULL,
          prospective_proof_eligible INTEGER NOT NULL CHECK(prospective_proof_eligible IN (0,1)),
          feature_payload_json TEXT NOT NULL,research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          selected_side TEXT CHECK(selected_side IS NULL),contract_id TEXT NOT NULL,cohort_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS level_approach_outcome (
          outcome_id TEXT PRIMARY KEY,episode_id TEXT NOT NULL,horizon_min INTEGER NOT NULL,
          label TEXT NOT NULL CHECK(label IN ('reject-away-before-break','penetrate-and-hold','false-break-reclaim','ambiguous_intrabar','neither')),
          entry_utc TEXT NOT NULL,maturity_utc TEXT NOT NULL,outcome_payload_json TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          contract_id TEXT NOT NULL,cohort_id TEXT NOT NULL,UNIQUE(episode_id,horizon_min),
          FOREIGN KEY(episode_id) REFERENCES level_approach_episode(episode_id)
        );
        CREATE TRIGGER IF NOT EXISTS episode_no_update BEFORE UPDATE ON level_approach_episode BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS episode_no_delete BEFORE DELETE ON level_approach_episode BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS outcome_no_update BEFORE UPDATE ON level_approach_outcome BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS outcome_no_delete BEFORE DELETE ON level_approach_outcome BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


def insert_episodes(connection: sqlite3.Connection, episodes: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"episodes": 0, "outcomes": 0}
    for episode in episodes:
        before = connection.total_changes
        connection.execute(
            "INSERT OR IGNORE INTO level_approach_episode VALUES (?,?,?,?,?,?,?,1,0,NULL,?,?)",
            (episode["episode_id"], episode["instrument"], episode["decision_utc"], episode["entry_utc"],
             episode["evidence_class"], int(bool(episode["prospective_proof_eligible"])),
             json.dumps(episode["features"], sort_keys=True), CONTRACT_ID, COHORT_ID),
        )
        counts["episodes"] += connection.total_changes - before
        for outcome in episode["outcomes"]:
            outcome_id = "outcome_" + digest(episode["episode_id"], outcome["horizon_min"], CONTRACT_ID)[:28]
            before = connection.total_changes
            connection.execute(
                "INSERT OR IGNORE INTO level_approach_outcome VALUES (?,?,?,?,?,?,?,1,0,?,?)",
                (outcome_id, episode["episode_id"], outcome["horizon_min"], outcome["label"],
                 outcome["entry_utc"], outcome["maturity_utc"], json.dumps(outcome, sort_keys=True), CONTRACT_ID, COHORT_ID),
            )
            counts["outcomes"] += connection.total_changes - before
    connection.commit()
    return counts


def load_csv(path: Path) -> list[Bar]:
    rows = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for raw in csv.DictReader(handle):
            try:
                rows.append(Bar(
                    timestamp=parse_time(raw.get("datetime") or raw.get("time")),
                    **{name: float(raw[name]) for name in (
                        "bid_open", "bid_high", "bid_low", "bid_close",
                        "ask_open", "ask_high", "ask_low", "ask_close",
                    )},
                ))
            except (KeyError, TypeError, ValueError):
                continue
    return sorted(rows, key=lambda bar: bar.timestamp)


def aggregate_completed_m1_to_m5(rows: Sequence[Bar]) -> list[Bar]:
    buckets: dict[datetime, list[Bar]] = {}
    for row in rows:
        minute = row.timestamp.minute - row.timestamp.minute % 5
        start = row.timestamp.replace(minute=minute, second=0, microsecond=0)
        buckets.setdefault(start, []).append(row)
    output = []
    for start, values in sorted(buckets.items()):
        values = sorted(values, key=lambda row: row.timestamp)
        expected = [start + timedelta(minutes=offset) for offset in range(5)]
        if [row.timestamp for row in values] != expected:
            continue
        output.append(Bar(
            timestamp=start,
            bid_open=values[0].bid_open,
            bid_high=max(row.bid_high for row in values),
            bid_low=min(row.bid_low for row in values),
            bid_close=values[-1].bid_close,
            ask_open=values[0].ask_open,
            ask_high=max(row.ask_high for row in values),
            ask_low=min(row.ask_low for row in values),
            ask_close=values[-1].ask_close,
        ))
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--input-granularity", choices=("M1", "M5"), default="M5")
    parser.add_argument("--max-bars", type=int, default=0)
    args = parser.parse_args()
    bars = load_csv(args.csv)
    if args.input_granularity == "M1":
        bars = aggregate_completed_m1_to_m5(bars)
    if args.max_bars > 0:
        bars = bars[: args.max_bars]
    episodes = scan_instrument(args.instrument.upper(), bars)
    connection = open_database(args.database)
    try:
        inserted = insert_episodes(connection, episodes)
        integrity = connection.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        connection.close()
    print(json.dumps({"instrument": args.instrument.upper(), "bars": len(bars), "episodes": len(episodes), "inserted": inserted, "integrity": integrity, "primary_horizon_min": PRIMARY_HORIZON_MIN, "secondary_horizon_min": SECONDARY_HORIZON_MIN, "diagnostic_horizons_min": [value for value in HORIZONS_MIN if value not in {PRIMARY_HORIZON_MIN, SECONDARY_HORIZON_MIN}], "policy": POLICY}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
