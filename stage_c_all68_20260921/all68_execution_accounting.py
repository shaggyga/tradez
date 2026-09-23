"""Pure accounting contracts for a later offline policy replay."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Quote:
    epoch: int
    bid: float
    ask: float


def validate_quote(quote: Quote) -> Quote:
    if quote.bid <= 0 or quote.ask <= 0 or quote.ask < quote.bid:
        raise ValueError("invalid_or_crossed_quote")
    return quote


def fill_after(quotes: Iterable[Quote], scheduled_epoch: int, max_delay_sec: int) -> Quote:
    """Use the first executable quote at/after the scheduled fill time."""
    if max_delay_sec < 0:
        raise ValueError("nonnegative_fill_delay_required")
    candidates = sorted((validate_quote(item) for item in quotes), key=lambda item: item.epoch)
    for quote in candidates:
        if scheduled_epoch <= quote.epoch <= scheduled_epoch + max_delay_sec:
            return quote
    raise ValueError("executable_quote_missing_within_fill_delay")


def quote_pnl(direction: int, units: float, entry: Quote, exit: Quote) -> float:
    """Return P&L in the pair's quote currency, with the spread charged once."""
    validate_quote(entry)
    validate_quote(exit)
    if direction not in (-1, 1) or units <= 0:
        raise ValueError("direction_and_positive_units_required")
    return units * ((exit.bid - entry.ask) if direction == 1 else (entry.bid - exit.ask))


def convert_quote_pnl(amount: float, quote_currency: str, account_currency: str, direct_quote: Quote | None) -> float:
    """Convert quote-currency P&L using executable sides of QUOTE_ACCOUNT.

    A positive quote-currency amount is sold at bid; a negative liability is
    bought at ask. Equal currencies do not need a conversion quote.
    """
    if quote_currency == account_currency:
        return amount
    if direct_quote is None:
        raise ValueError("account_conversion_path_missing")
    quote = validate_quote(direct_quote)
    return amount * (quote.bid if amount >= 0 else quote.ask)


def financing_charge(crossed_charge_boundaries: int, charge_per_unit: float, units: float) -> float:
    if crossed_charge_boundaries < 0 or units < 0:
        raise ValueError("nonnegative_financing_inputs_required")
    return crossed_charge_boundaries * charge_per_unit * units
