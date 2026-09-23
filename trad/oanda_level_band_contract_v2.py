#!/usr/bin/env python3
"""Pure causal support/resistance band contract.

This module contains geometry only.  It has no filesystem, database, broker,
signal-feed, authorization, promotion, or execution surface.  Every band is
made only from bars known by the supplied cutoff.  The live prospective
observer binds exact band versions and frozen barriers before any outcome bar
exists.
"""

from __future__ import annotations

import hashlib
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

try:
    from oanda_instrument_pips import fallback_pip_size
except ModuleNotFoundError:  # Package imports used by tests.
    from trad.oanda_instrument_pips import fallback_pip_size
from zoneinfo import ZoneInfo


SCHEMA_VERSION = "level_band_contract_v2"
CONTRACT_ID = "level_band_contract_v2_frozen_20260827b"
PIVOT_LEFT = 2
PIVOT_RIGHT = 2
ATR_WINDOW = 14
LEVEL_LOOKBACK_BARS = 288
EQUILIBRIUM_WINDOW = 20
ANCHOR_ATR_TOLERANCE = 0.15
BAND_PADDING_ATR = 0.05
APPROACH_ZONE_ATR = 0.25
REARM_ATR = 0.75
BREAK_ATR = 0.10
REJECT_ATR = 0.50
NEW_YORK = ZoneInfo("America/New_York")
SESSION_CONTRACT_ID = "fx_sessions_new_york_dst_v1_20260827"
WEEKLY_BOUNDARY_CONTRACT_ID = (
    "fx_week_sun1700_ny_to_fri1700_ny_m5_coverage_gte_0.97_v2"
)


def _digest(*parts: Any) -> str:
    raw = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def pip_size(instrument: str) -> float:
    """Return the audited 68-pair fallback when venue metadata is absent."""

    return fallback_pip_size(instrument)


def clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


@dataclass(frozen=True)
class MarketBar:
    """A complete BAM M1 or M5 bar with native midpoint geometry."""

    timestamp: datetime
    minutes: int
    mid_open: float
    mid_high: float
    mid_low: float
    mid_close: float
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
        return self.timestamp + timedelta(minutes=self.minutes)


@dataclass(frozen=True)
class PivotAnchor:
    anchor_id: str
    origin_kind: str
    price: float
    pivot_index: int
    confirmed_index: int
    pivot_utc: str
    known_at_utc: str
    anchor_atr: float
    anchor_spread: float
    tolerance: float
    padding: float


@dataclass(frozen=True)
class BandVersion:
    band_id: str
    band_version_id: str
    source: str
    name: str
    origin_kind: str
    lower: float
    center: float
    upper: float
    first_anchor_utc: str
    last_anchor_utc: str
    known_at_utc: str
    anchor_count: int
    distinct_source_count: int
    member_anchor_ids: tuple[str, ...]
    weekly_coverage_ratio: float | None = None


def validate_bar(bar: MarketBar) -> None:
    numeric = (
        bar.mid_open, bar.mid_high, bar.mid_low, bar.mid_close,
        bar.bid_open, bar.bid_high, bar.bid_low, bar.bid_close,
        bar.ask_open, bar.ask_high, bar.ask_low, bar.ask_close,
    )
    if bar.timestamp.tzinfo is None or bar.minutes <= 0:
        raise ValueError("invalid_bar_clock")
    if not all(math.isfinite(value) and value > 0.0 for value in numeric):
        raise ValueError("invalid_bar_number")
    if not (
        bar.mid_low <= min(bar.mid_open, bar.mid_close) <= bar.mid_high
        and bar.mid_low <= max(bar.mid_open, bar.mid_close) <= bar.mid_high
        and bar.bid_low <= min(bar.bid_open, bar.bid_close) <= bar.bid_high
        and bar.bid_low <= max(bar.bid_open, bar.bid_close) <= bar.bid_high
        and bar.ask_low <= min(bar.ask_open, bar.ask_close) <= bar.ask_high
        and bar.ask_low <= max(bar.ask_open, bar.ask_close) <= bar.ask_high
    ):
        raise ValueError("invalid_bar_ohlc")
    if any(
        ask + 1e-12 < bid
        for bid, ask in (
            (bar.bid_open, bar.ask_open),
            (bar.bid_high, bar.ask_high),
            (bar.bid_low, bar.ask_low),
            (bar.bid_close, bar.ask_close),
        )
    ):
        raise ValueError("crossed_bar_sides")


def validate_ordered_bars(
    bars: Sequence[MarketBar], *, minutes: int | None = None
) -> None:
    previous: datetime | None = None
    for bar in bars:
        validate_bar(bar)
        if minutes is not None and bar.minutes != minutes:
            raise ValueError("bar_granularity_mismatch")
        if previous is not None and bar.timestamp <= previous:
            raise ValueError("duplicate_or_unsorted_bars")
        previous = bar.timestamp


def aggregate_complete_m1_to_m5(rows: Sequence[MarketBar]) -> list[MarketBar]:
    validate_ordered_bars(rows, minutes=1)
    buckets: dict[datetime, list[MarketBar]] = {}
    for row in rows:
        start = row.timestamp.replace(
            minute=row.timestamp.minute - row.timestamp.minute % 5,
            second=0,
            microsecond=0,
        )
        buckets.setdefault(start, []).append(row)
    output: list[MarketBar] = []
    for start, values in sorted(buckets.items()):
        expected = [start + timedelta(minutes=offset) for offset in range(5)]
        if [row.timestamp for row in values] != expected:
            continue
        output.append(MarketBar(
            timestamp=start,
            minutes=5,
            mid_open=values[0].mid_open,
            mid_high=max(row.mid_high for row in values),
            mid_low=min(row.mid_low for row in values),
            mid_close=values[-1].mid_close,
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


def _mean(values: Sequence[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def atr_at(bars: Sequence[MarketBar], index: int) -> float:
    if index < 1 or index >= len(bars):
        return 0.0
    values: list[float] = []
    for cursor in range(max(1, index - ATR_WINDOW + 1), index + 1):
        previous = bars[cursor - 1].mid_close
        values.append(max(
            bars[cursor].mid_high - bars[cursor].mid_low,
            abs(bars[cursor].mid_high - previous),
            abs(bars[cursor].mid_low - previous),
        ))
    return _mean(values)


def spread_at(bar: MarketBar) -> float:
    return max(0.0, bar.ask_close - bar.bid_close)


def _anchor_scale(
    instrument: str, bars: Sequence[MarketBar], confirmation_index: int
) -> tuple[float, float, float, float]:
    atr = atr_at(bars, confirmation_index)
    spreads = [
        spread_at(row)
        for row in bars[max(0, confirmation_index - 11):confirmation_index + 1]
    ]
    median_spread = statistics.median(spreads) if spreads else 0.0
    pip = pip_size(instrument)
    tolerance = max(ANCHOR_ATR_TOLERANCE * atr, 1.5 * median_spread, pip)
    padding = max(BAND_PADDING_ATR * atr, 0.5 * tolerance, 0.5 * pip)
    return atr, median_spread, tolerance, padding


def confirmed_pivot_anchors(
    instrument: str,
    bars: Sequence[MarketBar],
    decision_index: int,
) -> list[PivotAnchor]:
    """Return only pivots with complete right-hand confirmation bars."""

    maximum = decision_index - PIVOT_RIGHT
    minimum = max(PIVOT_LEFT, decision_index - LEVEL_LOOKBACK_BARS)
    anchors: list[PivotAnchor] = []
    for index in range(minimum, maximum + 1):
        high = bars[index].mid_high
        low = bars[index].mid_low
        kinds: list[tuple[str, float]] = []
        if all(
            high > bars[index - offset].mid_high
            for offset in range(1, PIVOT_LEFT + 1)
        ) and all(
            high >= bars[index + offset].mid_high
            for offset in range(1, PIVOT_RIGHT + 1)
        ):
            kinds.append(("pivot_high", high))
        if all(
            low < bars[index - offset].mid_low
            for offset in range(1, PIVOT_LEFT + 1)
        ) and all(
            low <= bars[index + offset].mid_low
            for offset in range(1, PIVOT_RIGHT + 1)
        ):
            kinds.append(("pivot_low", low))
        confirmed_index = index + PIVOT_RIGHT
        anchor_atr, median_spread, tolerance, padding = _anchor_scale(
            instrument, bars, confirmed_index
        )
        if anchor_atr <= 0.0:
            continue
        for kind, price in kinds:
            anchor_id = "anchor_" + _digest(
                instrument, kind, iso(bars[index].timestamp), round(price, 10), CONTRACT_ID
            )[:24]
            anchors.append(PivotAnchor(
                anchor_id=anchor_id,
                origin_kind=kind,
                price=price,
                pivot_index=index,
                confirmed_index=confirmed_index,
                pivot_utc=iso(bars[index].timestamp),
                known_at_utc=iso(bars[confirmed_index].known_at),
                anchor_atr=anchor_atr,
                anchor_spread=median_spread,
                tolerance=tolerance,
                padding=padding,
            ))
    return sorted(anchors, key=lambda row: (row.confirmed_index, row.anchor_id))


def build_pivot_band_versions(anchors: Sequence[PivotAnchor]) -> list[BandVersion]:
    """Incrementally cluster anchors with each anchor's frozen tolerance.

    The result is restart-deterministic and does not use the decision-time ATR.
    Adding a newly confirmed anchor can create a new immutable band version,
    but cannot rewrite a version referenced by an earlier observation.
    """

    clusters: list[list[PivotAnchor]] = []
    for anchor in sorted(anchors, key=lambda row: (row.confirmed_index, row.anchor_id)):
        matches: list[tuple[float, int]] = []
        for cluster_index, cluster in enumerate(clusters):
            if cluster[0].origin_kind != anchor.origin_kind:
                continue
            # The first causal anchor is the permanent cluster reference.
            # Comparing to a moving centroid would allow a chain of nearby
            # anchors to drift into one enormous retrospective band.
            reference = cluster[0]
            distance = abs(anchor.price - reference.price)
            if distance <= max(anchor.tolerance, reference.tolerance):
                matches.append((distance, cluster_index))
        if matches:
            clusters[min(matches)[1]].append(anchor)
        else:
            clusters.append([anchor])

    output: list[BandVersion] = []
    for cluster in clusters:
        ordered = sorted(cluster, key=lambda row: (row.confirmed_index, row.anchor_id))
        anchor = ordered[0]
        member_ids = tuple(row.anchor_id for row in ordered)
        padding = max(row.padding for row in ordered)
        lower = min(row.price for row in ordered) - padding
        upper = max(row.price for row in ordered) + padding
        center = _mean([row.price for row in ordered])
        band_id = "band_" + _digest(anchor.anchor_id, CONTRACT_ID)[:24]
        version_id = "bandv_" + _digest(
            band_id, *member_ids, round(lower, 10), round(center, 10),
            round(upper, 10), CONTRACT_ID
        )[:28]
        output.append(BandVersion(
            band_id=band_id,
            band_version_id=version_id,
            source="confirmed_pivot_cluster",
            name="swing_cluster",
            origin_kind=anchor.origin_kind,
            lower=lower,
            center=center,
            upper=upper,
            first_anchor_utc=ordered[0].pivot_utc,
            last_anchor_utc=ordered[-1].pivot_utc,
            known_at_utc=max(row.known_at_utc for row in ordered),
            anchor_count=len(ordered),
            distinct_source_count=1,
            member_anchor_ids=member_ids,
        ))
    return sorted(output, key=lambda row: (row.center, row.band_version_id))


def last_completed_fx_week(decision_utc: datetime) -> tuple[datetime, datetime]:
    local = decision_utc.astimezone(NEW_YORK)
    friday = local.date() - timedelta(days=(local.weekday() - 4) % 7)
    boundary = datetime.combine(
        friday, datetime.min.time(), tzinfo=NEW_YORK
    ).replace(hour=17)
    if local < boundary:
        boundary -= timedelta(days=7)
    start = boundary - timedelta(days=5)
    return start.astimezone(timezone.utc), boundary.astimezone(timezone.utc)


def prior_completed_week_bands(
    instrument: str,
    bars: Sequence[MarketBar],
    decision_utc: datetime,
) -> list[BandVersion]:
    """Traditional P/R1/R2/S1/S2 from the last completed FX week."""

    start, end = last_completed_fx_week(decision_utc)
    rows = [row for row in bars if start <= row.timestamp < end]
    expected = 5 * 24 * 12
    if not rows:
        return []
    actual_times = [row.timestamp for row in rows]
    coverage = len(rows) / expected
    gaps = [
        actual_times[index] - actual_times[index - 1]
        for index in range(1, len(actual_times))
    ]
    if (
        coverage < 0.97
        or rows[0].timestamp - start > timedelta(minutes=15)
        or rows[-1].timestamp != end - timedelta(minutes=5)
        or max(gaps, default=timedelta(minutes=5)) > timedelta(minutes=60)
    ):
        return []
    high = max(row.mid_high for row in rows)
    low = min(row.mid_low for row in rows)
    close = rows[-1].mid_close
    pivot = (high + low + close) / 3.0
    values = {
        "P": pivot,
        "R1": 2.0 * pivot - low,
        "R2": pivot + high - low,
        "S1": 2.0 * pivot - high,
        "S2": pivot - high + low,
    }
    anchor_index = bars.index(rows[-1])
    anchor_atr, median_spread, _, padding = _anchor_scale(
        instrument, bars, anchor_index
    )
    if anchor_atr <= 0.0:
        return []
    output: list[BandVersion] = []
    for name, price in values.items():
        band_id = "weekly_" + _digest(
            instrument, iso(end), name, WEEKLY_BOUNDARY_CONTRACT_ID
        )[:24]
        lower, upper = price - padding, price + padding
        version_id = "bandv_" + _digest(
            band_id, round(lower, 10), round(price, 10), round(upper, 10),
            CONTRACT_ID
        )[:28]
        output.append(BandVersion(
            band_id=band_id,
            band_version_id=version_id,
            source="prior_completed_week_traditional_pivot",
            name=name,
            origin_kind="weekly_pivot",
            lower=lower,
            center=price,
            upper=upper,
            first_anchor_utc=iso(start),
            last_anchor_utc=iso(end - timedelta(minutes=5)),
            known_at_utc=iso(end),
            anchor_count=1,
            distinct_source_count=1,
            member_anchor_ids=(band_id,),
            weekly_coverage_ratio=coverage,
        ))
    return sorted(output, key=lambda row: (row.center, row.name))


def distance_to_band(price: float, band: BandVersion) -> float:
    if price < band.lower:
        return band.lower - price
    if price > band.upper:
        return price - band.upper
    return 0.0


def physical_approach_side(
    price: float, previous_price: float, band: BandVersion
) -> int | None:
    """Return +1 for approach from below and -1 for approach from above."""

    if price < band.lower:
        return 1
    if price > band.upper:
        return -1
    if previous_price <= band.lower:
        return 1
    if previous_price >= band.upper:
        return -1
    return None


def role_for_side(side: int) -> str:
    if side == 1:
        return "resistance"
    if side == -1:
        return "support"
    raise ValueError("invalid_approach_side")


def session_at(value: datetime) -> str:
    """DST-aware New York session labels frozen under SESSION_CONTRACT_ID."""

    local = value.astimezone(NEW_YORK)
    hour = local.hour + local.minute / 60.0
    if 17.0 <= hour or hour < 2.0:
        return "asia_open"
    if hour < 3.0:
        return "asia_late"
    if hour < 8.0:
        return "london"
    if hour < 12.0:
        return "london_new_york_overlap"
    if hour < 16.0:
        return "new_york"
    return "rollover"


def frozen_approach_descriptors(
    instrument: str,
    m1_bars: Sequence[MarketBar],
    m5_bars: Sequence[MarketBar],
    band: BandVersion,
    approach_side: int,
    decision_mid: float,
    decision_bid: float,
    decision_ask: float,
    decision_utc: datetime,
    venue_pip: float | None = None,
) -> dict[str, Any]:
    """Build transparent physical descriptors; no probability or edge score."""

    if approach_side not in {-1, 1}:
        raise ValueError("invalid_approach_side")
    if decision_bid <= 0.0 or decision_ask < decision_bid:
        raise ValueError("invalid_decision_quote")
    if len(m1_bars) < 13 or len(m5_bars) < max(ATR_WINDOW + 1, EQUILIBRIUM_WINDOW):
        raise ValueError("insufficient_descriptor_history")
    recent = list(m1_bars[-13:])
    if any(
        recent[index].timestamp - recent[index - 1].timestamp != timedelta(minutes=1)
        for index in range(1, len(recent))
    ):
        raise ValueError("noncontiguous_descriptor_history")
    atr = atr_at(m5_bars, len(m5_bars) - 1)
    if atr <= 0.0:
        raise ValueError("invalid_atr")
    pip = pip_size(instrument)
    if venue_pip is not None:
        if not math.isfinite(venue_pip) or venue_pip <= 0.0:
            raise ValueError("invalid_venue_pip")
        pip = venue_pip
    spread = decision_ask - decision_bid
    spread_pips = spread / pip
    current_distance = distance_to_band(decision_mid, band)
    distances = [distance_to_band(row.mid_close, band) for row in recent]
    velocities: dict[int, float] = {}
    for lookback in (1, 3, 5, 12):
        old_distance = distances[-1 - lookback]
        velocities[lookback] = (old_distance - current_distance) / lookback / pip
    deltas = [
        recent[index].mid_close - recent[index - 1].mid_close
        for index in range(len(recent) - 5, len(recent))
    ]
    path_length = sum(abs(value) for value in deltas)
    displacement = abs(recent[-1].mid_close - recent[-6].mid_close)
    efficiency = displacement / path_length if path_length > 0.0 else 0.0
    monotonicity = sum(
        1 for value in deltas if value * approach_side > 0.0
    ) / len(deltas)
    equilibrium = _mean([row.mid_close for row in m5_bars[-EQUILIBRIUM_WINDOW:]])
    impulse = approach_side * (decision_mid - recent[-6].mid_close) / atr
    stretch = abs(decision_mid - equilibrium) / atr
    zone = max(APPROACH_ZONE_ATR * atr, 1.5 * spread, pip)
    rearm = max(REARM_ATR * atr, 3.0 * spread, pip)
    break_distance = max(BREAK_ATR * atr, 1.5 * spread, pip)
    reject_distance = max(REJECT_ATR * atr, 2.0 * spread, 2.0 * pip)
    ttc = (
        current_distance / max(velocities[3] * pip, 1e-12)
        if velocities[3] > 0.0 else 60.0
    )
    return {
        "descriptor_contract_id": CONTRACT_ID,
        "session_contract_id": SESSION_CONTRACT_ID,
        "decision_utc": iso(decision_utc),
        "instrument": instrument,
        "pip": pip,
        "band_id": band.band_id,
        "band_version_id": band.band_version_id,
        "band_source": band.source,
        "band_name": band.name,
        "band_origin_kind": band.origin_kind,
        "band_lower": band.lower,
        "band_center": band.center,
        "band_upper": band.upper,
        "band_width_pips": (band.upper - band.lower) / pip,
        "band_width_atr": (band.upper - band.lower) / atr,
        "band_width_spread_multiple": (band.upper - band.lower) / max(spread, 1e-12),
        "band_anchor_count": band.anchor_count,
        "band_distinct_source_count": band.distinct_source_count,
        "band_known_at_utc": band.known_at_utc,
        "approach_side": approach_side,
        "physical_role": role_for_side(approach_side),
        "distance_to_band_pips": current_distance / pip,
        "distance_to_band_atr": current_distance / atr,
        "approach_zone_pips": zone / pip,
        "rearm_distance_pips": rearm / pip,
        "approach_velocity_1_pips_per_min": velocities[1],
        "approach_velocity_3_pips_per_min": velocities[3],
        "approach_velocity_5_pips_per_min": velocities[5],
        "approach_velocity_12_pips_per_min": velocities[12],
        "approach_acceleration_pips_per_min2": velocities[1] - velocities[3],
        "approach_deceleration_pips_per_min2": velocities[1] - velocities[5],
        "path_efficiency_5": clip(efficiency, 0.0, 1.0),
        "approach_monotonicity_5": clip(monotonicity, 0.0, 1.0),
        "estimated_time_to_contact_min": clip(ttc, 0.0, 60.0),
        "impulse_toward_band_atr": impulse,
        "equilibrium_stretch_atr": stretch,
        "atr_m5_pips": atr / pip,
        "spread_pips": spread_pips,
        "break_distance_pips": break_distance / pip,
        "reject_distance_pips": reject_distance / pip,
        "frozen_break_price": (
            band.upper + break_distance if approach_side == 1
            else band.lower - break_distance
        ),
        "frozen_reject_price": (
            band.lower - reject_distance if approach_side == 1
            else band.upper + reject_distance
        ),
        "session": session_at(decision_utc),
        "response_probability_state": "absent_no_frozen_model",
        "empirical_cost_clearance_state": "unknown_collecting_prospective_outcomes",
        "selected_side": None,
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
    }


def choose_physical_band(
    bands: Iterable[BandVersion],
    current_mid: float,
    previous_mid: float,
) -> tuple[BandVersion, int] | None:
    """Deterministically choose one representative among overlapping zones."""

    candidates: list[tuple[float, int, str, BandVersion, int]] = []
    for band in bands:
        side = physical_approach_side(current_mid, previous_mid, band)
        if side is None:
            continue
        candidates.append((
            distance_to_band(current_mid, band),
            -band.anchor_count,
            band.band_version_id,
            band,
            side,
        ))
    if not candidates:
        return None
    _, _, _, band, side = min(candidates)
    return band, side


__all__ = [
    "APPROACH_ZONE_ATR", "ATR_WINDOW", "BandVersion", "CONTRACT_ID",
    "MarketBar", "PivotAnchor", "SCHEMA_VERSION", "aggregate_complete_m1_to_m5",
    "atr_at", "build_pivot_band_versions", "choose_physical_band",
    "confirmed_pivot_anchors", "distance_to_band", "frozen_approach_descriptors",
    "iso", "last_completed_fx_week", "physical_approach_side", "pip_size",
    "prior_completed_week_bands", "role_for_side", "validate_ordered_bars",
]
