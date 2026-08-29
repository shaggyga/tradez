"""Canonical signed-currency exposure identities and overlap semantics.

This module is deliberately pure and isolated from execution, lifecycle, and
broker code.  It gives research and evidence consumers one strict vocabulary
for the two currency exposures represented by an FX pair direction.

Unknown directions never default to ``short``.  Malformed input raises a
specific contract error so callers must explicitly abstain or reject it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Sequence


LONG = "long"
SHORT = "short"

INDEPENDENT = "independent"
SAME_SIDE_OVERLAP = "same_side_overlap"
OPPOSITE_SIDE_CONFLICT = "opposite_side_conflict"

_DIRECTION_ALIASES = {
    "buy": LONG,
    "long": LONG,
    "sell": SHORT,
    "short": SHORT,
}
_INSTRUMENT_PATTERN = re.compile(r"^[A-Z]{3}_[A-Z]{3}$")
_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")


class ExposureContractError(ValueError):
    """Base error for invalid signed-exposure contract values."""


class InvalidDirectionError(ExposureContractError):
    """Raised when a direction is not an explicit buy/long/sell/short."""


class InvalidInstrumentError(ExposureContractError):
    """Raised when an instrument is not a valid ``AAA_BBB`` currency pair."""


class InvalidFactorIdError(ExposureContractError):
    """Raised when a factor ID is not canonical."""


def normalize_direction(value: Any) -> str:
    """Return canonical ``long`` or ``short``; fail closed otherwise."""

    if not isinstance(value, str):
        raise InvalidDirectionError(
            "direction must be one of buy, long, sell, or short"
        )
    normalized = value.strip().lower()
    try:
        return _DIRECTION_ALIASES[normalized]
    except KeyError as exc:
        raise InvalidDirectionError(
            f"unsupported direction {value!r}; refusing to infer a side"
        ) from exc


def opposite_side(value: Any) -> str:
    return SHORT if normalize_direction(value) == LONG else LONG


def split_currency_pair(value: Any) -> tuple[str, str]:
    """Normalize case/outer whitespace while retaining strict pair syntax."""

    if not isinstance(value, str):
        raise InvalidInstrumentError("instrument must be an AAA_BBB string")
    instrument = value.strip().upper()
    if not _INSTRUMENT_PATTERN.fullmatch(instrument):
        raise InvalidInstrumentError(
            f"invalid currency pair {value!r}; expected exact AAA_BBB syntax"
        )
    base, quote = instrument.split("_")
    if base == quote:
        raise InvalidInstrumentError("base and quote currencies must differ")
    return base, quote


def _normalize_currency(value: Any) -> str:
    if not isinstance(value, str):
        raise InvalidFactorIdError("currency must be a three-letter string")
    currency = value.strip().upper()
    if not _CURRENCY_PATTERN.fullmatch(currency):
        raise InvalidFactorIdError(f"invalid currency code {value!r}")
    return currency


@dataclass(frozen=True, order=True)
class CurrencyExposure:
    currency: str
    side: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "currency", _normalize_currency(self.currency))
        object.__setattr__(self, "side", normalize_direction(self.side))

    @property
    def sign(self) -> int:
        return 1 if self.side == LONG else -1

    @property
    def factor_id(self) -> str:
        return f"currency:{self.currency}:{self.side}"

    def payload(self) -> dict[str, Any]:
        return {
            "currency": self.currency,
            "side": self.side,
            "sign": self.sign,
            "factor_id": self.factor_id,
        }


@dataclass(frozen=True, order=True)
class ExposureConflict:
    currency: str
    left_factor_id: str
    right_factor_id: str

    def payload(self) -> dict[str, str]:
        return {
            "currency": self.currency,
            "left_factor_id": self.left_factor_id,
            "right_factor_id": self.right_factor_id,
        }


@dataclass(frozen=True)
class ExposureComparison:
    relation: str
    same_side_factor_ids: tuple[str, ...]
    opposite_side_conflicts: tuple[ExposureConflict, ...]
    shared_currencies: tuple[str, ...]

    @property
    def has_conflict(self) -> bool:
        return bool(self.opposite_side_conflicts)

    @property
    def is_independent(self) -> bool:
        return not self.shared_currencies

    @property
    def can_count_as_independent_confirmation(self) -> bool:
        return self.is_independent and not self.has_conflict

    def payload(self) -> dict[str, Any]:
        return {
            "relation": self.relation,
            "same_side_factor_ids": list(self.same_side_factor_ids),
            "opposite_side_conflicts": [
                conflict.payload() for conflict in self.opposite_side_conflicts
            ],
            "shared_currencies": list(self.shared_currencies),
            "has_conflict": self.has_conflict,
            "is_independent": self.is_independent,
            "can_count_as_independent_confirmation": (
                self.can_count_as_independent_confirmation
            ),
        }


def pair_currency_exposures(
    instrument: Any, direction: Any
) -> tuple[CurrencyExposure, CurrencyExposure]:
    """Map a pair direction to ordered base and quote currency exposures."""

    base, quote = split_currency_pair(instrument)
    base_side = normalize_direction(direction)
    return (
        CurrencyExposure(base, base_side),
        CurrencyExposure(quote, opposite_side(base_side)),
    )


def canonical_factor_ids(instrument: Any, direction: Any) -> tuple[str, str]:
    return tuple(
        exposure.factor_id
        for exposure in pair_currency_exposures(instrument, direction)
    )


def parse_factor_id(value: Any) -> CurrencyExposure:
    if not isinstance(value, str):
        raise InvalidFactorIdError(
            "factor ID must use currency:AAA:long|short syntax"
        )
    parts = value.split(":")
    if len(parts) != 3 or parts[0] != "currency":
        raise InvalidFactorIdError(
            f"invalid factor ID {value!r}; expected currency:AAA:long|short"
        )
    currency, side = parts[1], parts[2]
    exposure = CurrencyExposure(currency, side)
    if value != exposure.factor_id:
        raise InvalidFactorIdError(
            f"factor ID {value!r} is not in canonical form {exposure.factor_id!r}"
        )
    return exposure


def _by_currency(
    exposures: Iterable[CurrencyExposure], *, label: str
) -> dict[str, CurrencyExposure]:
    result: dict[str, CurrencyExposure] = {}
    for exposure in exposures:
        if not isinstance(exposure, CurrencyExposure):
            raise ExposureContractError(
                f"{label} exposure set contains a non-CurrencyExposure value"
            )
        prior = result.get(exposure.currency)
        if prior is not None and prior.side != exposure.side:
            raise ExposureContractError(
                f"{label} exposure set is internally conflicted for "
                f"{exposure.currency}"
            )
        result[exposure.currency] = exposure
    return result


def compare_exposure_sets(
    left: Sequence[CurrencyExposure], right: Sequence[CurrencyExposure]
) -> ExposureComparison:
    """Classify same-side correlation and opposite-side currency conflict.

    Opposite-side conflict takes precedence if a basket comparison contains
    both same-side and opposite-side overlaps.  Either kind of shared currency
    prevents the rows from counting as independent confirmation.
    """

    left_by_currency = _by_currency(left, label="left")
    right_by_currency = _by_currency(right, label="right")
    shared = tuple(sorted(set(left_by_currency) & set(right_by_currency)))
    same_side: list[str] = []
    conflicts: list[ExposureConflict] = []
    for currency in shared:
        left_exposure = left_by_currency[currency]
        right_exposure = right_by_currency[currency]
        if left_exposure.side == right_exposure.side:
            same_side.append(left_exposure.factor_id)
        else:
            conflicts.append(
                ExposureConflict(
                    currency=currency,
                    left_factor_id=left_exposure.factor_id,
                    right_factor_id=right_exposure.factor_id,
                )
            )
    if conflicts:
        relation = OPPOSITE_SIDE_CONFLICT
    elif same_side:
        relation = SAME_SIDE_OVERLAP
    else:
        relation = INDEPENDENT
    return ExposureComparison(
        relation=relation,
        same_side_factor_ids=tuple(sorted(same_side)),
        opposite_side_conflicts=tuple(conflicts),
        shared_currencies=shared,
    )


def compare_pair_exposures(
    left_instrument: Any,
    left_direction: Any,
    right_instrument: Any,
    right_direction: Any,
) -> ExposureComparison:
    return compare_exposure_sets(
        pair_currency_exposures(left_instrument, left_direction),
        pair_currency_exposures(right_instrument, right_direction),
    )


def compare_factor_ids(
    left: Iterable[Any], right: Iterable[Any]
) -> ExposureComparison:
    return compare_exposure_sets(
        tuple(parse_factor_id(value) for value in left),
        tuple(parse_factor_id(value) for value in right),
    )


__all__ = [
    "INDEPENDENT",
    "LONG",
    "OPPOSITE_SIDE_CONFLICT",
    "SAME_SIDE_OVERLAP",
    "SHORT",
    "CurrencyExposure",
    "ExposureComparison",
    "ExposureConflict",
    "ExposureContractError",
    "InvalidDirectionError",
    "InvalidFactorIdError",
    "InvalidInstrumentError",
    "canonical_factor_ids",
    "compare_exposure_sets",
    "compare_factor_ids",
    "compare_pair_exposures",
    "normalize_direction",
    "opposite_side",
    "pair_currency_exposures",
    "parse_factor_id",
    "split_currency_pair",
]
