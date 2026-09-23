"""Small, pure contracts for the first all-68 offline replay.

The module deliberately has no file, network, model, or broker dependency.
It establishes event ordering and the forecast-tape boundaries that a later
reader and model adapter must obey.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence


MINUTE = 60


@dataclass(frozen=True)
class BarEvent:
    bar_end_epoch: int
    instrument: str
    close: float


@dataclass(frozen=True)
class Forecast:
    forecast_id: str
    instrument: str
    origin_epoch: int
    horizon_sec: int
    target_epoch: int
    feature_cutoff_epoch: int
    training_cutoff_epoch: int
    label_maturity_epoch: int
    fit_completed_epoch: int
    available_epoch: int
    model_id: str
    direction: int
    status: str
    reason: str


@dataclass(frozen=True)
class LedgerRow:
    arm: str
    forecast_id: str
    action: str
    capital_before: float
    capital_after: float
    reason: str


def _instrument(value: str) -> str:
    normalized = str(value).upper().replace("/", "_")
    if not normalized or "_" not in normalized:
        raise ValueError("instrument_format_required")
    return normalized


def ordered_events(events: Iterable[BarEvent]) -> tuple[BarEvent, ...]:
    """Return one stable global clock; no duplicate pair/minute is accepted."""
    normalized: list[BarEvent] = []
    seen: set[tuple[int, str]] = set()
    for event in events:
        if event.bar_end_epoch % MINUTE:
            raise ValueError("bar_end_must_be_utc_minute")
        item = BarEvent(int(event.bar_end_epoch), _instrument(event.instrument), float(event.close))
        key = (item.bar_end_epoch, item.instrument)
        if key in seen:
            raise ValueError("duplicate_pair_minute")
        seen.add(key)
        normalized.append(item)
    return tuple(sorted(normalized, key=lambda item: (item.bar_end_epoch, item.instrument)))


def global_minute_support(events: Iterable[BarEvent], instruments: Sequence[str]) -> tuple[tuple[int, Mapping[str, str]], ...]:
    """Report every requested instrument for every observed minute; never fill it."""
    universe = tuple(sorted({_instrument(item) for item in instruments}))
    if not universe:
        raise ValueError("instrument_universe_required")
    ordered = ordered_events(events)
    by_time: dict[int, set[str]] = {}
    for event in ordered:
        by_time.setdefault(event.bar_end_epoch, set()).add(event.instrument)
    return tuple(
        (timestamp, {pair: "supported" if pair in present else "missing_support" for pair in universe})
        for timestamp, present in sorted(by_time.items())
    )


def issue_no_change_tape(
    events: Iterable[BarEvent],
    instruments: Sequence[str],
    horizons_sec: Sequence[int],
    *,
    training_cutoff_epoch: int,
    fit_completed_epoch: int,
) -> tuple[Forecast, ...]:
    """Issue a policy-independent neutral-control tape with explicit blockers."""
    horizons = tuple(sorted({int(value) for value in horizons_sec}))
    if horizons != (300, 900, 1800, 3600, 7200, 14400, 28800, 86400):
        raise ValueError("required_horizon_set_mismatch")
    support = global_minute_support(events, instruments)
    forecasts: list[Forecast] = []
    for origin_epoch, minute in support:
        for instrument, state in sorted(minute.items()):
            for horizon in horizons:
                feature_cutoff = origin_epoch
                label_maturity = origin_epoch + horizon
                # The target maturity belongs to the later outcome.  Waiting
                # for it before issuing would leak the future into the tape.
                available = max(feature_cutoff, int(training_cutoff_epoch), int(fit_completed_epoch))
                status = "eligible" if state == "supported" else "blocked"
                reason = "" if status == "eligible" else "missing_support"
                forecasts.append(Forecast(
                    forecast_id=f"{instrument}:{origin_epoch}:{horizon}:no_change_v1",
                    instrument=instrument,
                    origin_epoch=origin_epoch,
                    horizon_sec=horizon,
                    target_epoch=label_maturity,
                    feature_cutoff_epoch=feature_cutoff,
                    training_cutoff_epoch=int(training_cutoff_epoch),
                    label_maturity_epoch=label_maturity,
                    fit_completed_epoch=int(fit_completed_epoch),
                    available_epoch=available,
                    model_id="no_change_control_v1",
                    direction=0,
                    status=status,
                    reason=reason,
                ))
    return tuple(forecasts)


def no_trade_ledger(tape: Iterable[Forecast], arm: str, initial_capital: float) -> tuple[LedgerRow, ...]:
    """Independent arm ledger for the neutral control; it cannot mutate another arm."""
    if initial_capital < 0:
        raise ValueError("nonnegative_initial_capital_required")
    capital = float(initial_capital)
    rows: list[LedgerRow] = []
    for forecast in tape:
        action = "no_trade"
        reason = forecast.reason or "neutral_control"
        rows.append(LedgerRow(str(arm), forecast.forecast_id, action, capital, capital, reason))
    return tuple(rows)
