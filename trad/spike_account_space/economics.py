"""Offline OANDA-style FX execution and account economics primitives.

This module deliberately has no broker client, network access, pandas dependency, or
dependency on the live account manager.  Callers provide instrument metadata and
quotes captured at the timestamp being simulated.

Conventions
-----------
* Currency amounts and prices are floats; order units are always integers.
* Risk fractions are decimal fractions (``0.01`` means one percent).
* ``margin_closeout_percent`` follows the v20 API ratio convention: ``1.0`` means
  100%, not one percent.
* Executable conversion liquidates positive currency at bid and covers a negative
  currency liability at ask.  Missing conversion paths raise; they never silently
  become a 1:1 rate.
* Bid/ask fills already include spread.  Commission, financing, and broker currency
  conversion charges are supplied as explicit hooks because they depend on account
  division, tier, timestamp, and holding period.
"""

from __future__ import annotations

import math
import re
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence


class EconomicsError(ValueError):
    """Base class for invalid or unavailable economics inputs."""


class MissingMetadataError(EconomicsError):
    """Required account-specific instrument metadata was not supplied."""


class InvalidQuoteError(EconomicsError):
    """A quote is missing, non-finite, non-positive, or crossed."""


class ConversionUnavailableError(EconomicsError):
    """No quoted currency path can convert an amount to account currency."""


_INSTRUMENT_RE = re.compile(r"^[A-Z]{3}_[A-Z]{3}$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


def _float(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise EconomicsError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise EconomicsError(f"{field} must be finite")
    return result


def _optional_float(value: Any, field: str) -> Optional[float]:
    if value is None or value == "":
        return None
    return _float(value, field)


def normalize_currency(currency: Any) -> str:
    result = str(currency or "").strip().upper()
    if not _CURRENCY_RE.fullmatch(result):
        raise EconomicsError(f"invalid ISO-style currency code: {currency!r}")
    return result


def normalize_instrument(instrument: Any) -> str:
    result = str(instrument or "").strip().upper()
    result = result.replace("/", "_").replace("-", "_").replace(" ", "_")
    result = re.sub(r"_+", "_", result).strip("_")
    if "_" not in result and re.fullmatch(r"[A-Z]{6}", result):
        result = f"{result[:3]}_{result[3:]}"
    if not _INSTRUMENT_RE.fullmatch(result):
        raise EconomicsError(f"invalid FX instrument: {instrument!r}")
    return result


def split_instrument(instrument: Any) -> tuple[str, str]:
    normalized = normalize_instrument(instrument)
    base, quote = normalized.split("_", 1)
    return base, quote


@dataclass(frozen=True)
class InstrumentMeta:
    """Account-specific OANDA instrument economics metadata.

    ``pip_location`` is intentionally mandatory.  Pip size is never inferred from
    the quote currency: for example, an OANDA ``HKD_JPY`` snapshot may specify
    ``pipLocation=-4`` even though most JPY-quoted instruments specify ``-2``.
    """

    name: str
    pip_location: int
    margin_rate: float
    display_precision: int = 5
    trade_units_precision: int = 0
    minimum_trade_size: float = 1.0
    maximum_order_units: Optional[float] = None
    instrument_type: str = "CURRENCY"

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", normalize_instrument(self.name))
        if isinstance(self.pip_location, bool) or not isinstance(self.pip_location, int):
            raise MissingMetadataError("pip_location must be an explicit integer")
        if self.pip_location < -12 or self.pip_location > 3:
            raise MissingMetadataError("pip_location is outside a plausible range")
        margin_rate = _float(self.margin_rate, "margin_rate")
        if not 0.0 < margin_rate <= 1.0:
            raise MissingMetadataError("margin_rate must be in (0, 1]")
        object.__setattr__(self, "margin_rate", margin_rate)
        if self.display_precision < 0 or self.trade_units_precision < 0:
            raise MissingMetadataError("price and unit precision cannot be negative")
        minimum = _float(self.minimum_trade_size, "minimum_trade_size")
        if minimum <= 0:
            raise MissingMetadataError("minimum_trade_size must be positive")
        object.__setattr__(self, "minimum_trade_size", minimum)
        maximum = _optional_float(self.maximum_order_units, "maximum_order_units")
        if maximum is not None and maximum <= 0:
            maximum = None
        object.__setattr__(self, "maximum_order_units", maximum)

    @property
    def base_currency(self) -> str:
        return split_instrument(self.name)[0]

    @property
    def quote_currency(self) -> str:
        return split_instrument(self.name)[1]

    @property
    def pip_size(self) -> float:
        return 10.0 ** self.pip_location

    @property
    def minimum_integer_units(self) -> int:
        return max(1, math.ceil(self.minimum_trade_size))

    @property
    def maximum_integer_order_units(self) -> Optional[int]:
        if self.maximum_order_units is None:
            return None
        return math.floor(self.maximum_order_units)

    @classmethod
    def from_oanda(cls, raw: Mapping[str, Any]) -> "InstrumentMeta":
        """Parse a previously captured v20 instrument object.

        The method is offline.  Most importantly, it refuses to invent a default
        ``pipLocation`` or ``marginRate`` when either field is absent.
        """

        if "pipLocation" not in raw and "pip_location" not in raw:
            raise MissingMetadataError("instrument metadata is missing pipLocation")
        if "marginRate" not in raw and "margin_rate" not in raw:
            raise MissingMetadataError("instrument metadata is missing marginRate")
        name = raw.get("name", raw.get("instrument"))
        if not name:
            raise MissingMetadataError("instrument metadata is missing name")
        try:
            pip_location = int(raw.get("pipLocation", raw.get("pip_location")))
        except (TypeError, ValueError) as exc:
            raise MissingMetadataError("pipLocation must be an integer") from exc
        return cls(
            name=str(name),
            pip_location=pip_location,
            margin_rate=_float(raw.get("marginRate", raw.get("margin_rate")), "marginRate"),
            display_precision=int(raw.get("displayPrecision", raw.get("display_precision", 5))),
            trade_units_precision=int(
                raw.get("tradeUnitsPrecision", raw.get("trade_units_precision", 0))
            ),
            minimum_trade_size=_float(
                raw.get("minimumTradeSize", raw.get("minimum_trade_size", 1)),
                "minimumTradeSize",
            ),
            maximum_order_units=_optional_float(
                raw.get("maximumOrderUnits", raw.get("maximum_order_units")),
                "maximumOrderUnits",
            ),
            instrument_type=str(raw.get("type", raw.get("instrument_type", "CURRENCY"))),
        )


def build_instrument_metadata(
    rows: Iterable[Mapping[str, Any]], *, currency_only: bool = True
) -> dict[str, InstrumentMeta]:
    """Build a strict metadata map from captured OANDA instrument rows."""

    result: dict[str, InstrumentMeta] = {}
    for raw in rows:
        instrument_type = str(raw.get("type", raw.get("instrument_type", "CURRENCY"))).upper()
        if currency_only and instrument_type != "CURRENCY":
            continue
        meta = InstrumentMeta.from_oanda(raw)
        if meta.name in result:
            raise MissingMetadataError(f"duplicate instrument metadata for {meta.name}")
        result[meta.name] = meta
    return result


@dataclass(frozen=True)
class MarketQuote:
    """Top-of-book executable quote."""

    bid: float
    ask: float

    def __post_init__(self) -> None:
        bid = _float(self.bid, "bid")
        ask = _float(self.ask, "ask")
        if bid <= 0 or ask <= 0:
            raise InvalidQuoteError("bid and ask must be positive")
        if ask < bid:
            raise InvalidQuoteError(f"crossed quote: bid={bid}, ask={ask}")
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "MarketQuote":
        """Accept compact rows or a captured OANDA pricing object."""

        bids = raw.get("bids")
        asks = raw.get("asks")
        bid: Any = None
        ask: Any = None
        if isinstance(bids, Sequence) and not isinstance(bids, (str, bytes)) and bids:
            first_bid = bids[0]
            bid = first_bid.get("price") if isinstance(first_bid, Mapping) else first_bid
        if isinstance(asks, Sequence) and not isinstance(asks, (str, bytes)) and asks:
            first_ask = asks[0]
            ask = first_ask.get("price") if isinstance(first_ask, Mapping) else first_ask
        for key in ("bid", "closeoutBid", "bid_c", "bid_close"):
            if bid is None and raw.get(key) is not None:
                bid = raw[key]
        for key in ("ask", "closeoutAsk", "ask_c", "ask_close"):
            if ask is None and raw.get(key) is not None:
                ask = raw[key]
        if bid is None or ask is None:
            raise InvalidQuoteError("quote mapping must contain executable bid and ask")
        return cls(bid=_float(bid, "bid"), ask=_float(ask, "ask"))


QuoteLike = MarketQuote | Mapping[str, Any]


def coerce_quote(quote: QuoteLike) -> MarketQuote:
    if isinstance(quote, MarketQuote):
        return quote
    if isinstance(quote, Mapping):
        return MarketQuote.from_mapping(quote)
    raise InvalidQuoteError(f"unsupported quote type: {type(quote).__name__}")


@dataclass(frozen=True)
class ConversionResult:
    source_currency: str
    target_currency: str
    source_amount: float
    target_amount: float
    rate: float
    currency_path: tuple[str, ...]
    instrument_path: tuple[str, ...]
    pricing: str


def _conversion_edges(
    quotes: Mapping[str, QuoteLike], *, amount_is_liability: bool, pricing: str
) -> dict[str, list[tuple[str, float, str]]]:
    edges: dict[str, list[tuple[str, float, str]]] = defaultdict(list)
    for raw_instrument in sorted(quotes, key=lambda value: normalize_instrument(value)):
        instrument = normalize_instrument(raw_instrument)
        base, quote_currency = split_instrument(instrument)
        quote = coerce_quote(quotes[raw_instrument])
        if pricing == "mid":
            base_to_quote = quote.mid
            quote_to_base = 1.0 / quote.mid
        elif amount_is_liability:
            # Covering a BASE liability costs ASK quote; covering a QUOTE
            # liability costs 1/BID base.
            base_to_quote = quote.ask
            quote_to_base = 1.0 / quote.bid
        else:
            # Liquidating a positive BASE asset receives BID quote; liquidating a
            # positive QUOTE asset buys BASE at ASK.
            base_to_quote = quote.bid
            quote_to_base = 1.0 / quote.ask
        edges[base].append((quote_currency, base_to_quote, instrument))
        edges[quote_currency].append((base, quote_to_base, instrument))
    return edges


def resolve_conversion(
    amount: float,
    source_currency: str,
    target_currency: str,
    quotes: Mapping[str, QuoteLike],
    *,
    pricing: str = "executable",
    max_hops: int = 3,
) -> ConversionResult:
    """Resolve and price a currency conversion using supplied top-of-book quotes.

    Positive executable amounts are liquidated at bid/ask.  Negative amounts are
    liabilities and therefore use the opposite, conservative side.  Breadth-first
    routing prefers the fewest quoted conversions.  No path means an exception,
    never a 1.0 fallback.
    """

    source = normalize_currency(source_currency)
    target = normalize_currency(target_currency)
    amount_value = _float(amount, "amount")
    pricing_value = str(pricing).strip().lower()
    if pricing_value not in {"executable", "mid"}:
        raise EconomicsError("pricing must be 'executable' or 'mid'")
    if max_hops < 1:
        raise EconomicsError("max_hops must be at least one")
    if source == target:
        return ConversionResult(
            source_currency=source,
            target_currency=target,
            source_amount=amount_value,
            target_amount=amount_value,
            rate=1.0,
            currency_path=(source,),
            instrument_path=(),
            pricing=pricing_value,
        )

    edges = _conversion_edges(
        quotes, amount_is_liability=amount_value < 0.0, pricing=pricing_value
    )
    queue: deque[tuple[str, float, tuple[str, ...], tuple[str, ...]]] = deque(
        [(source, 1.0, (source,), ())]
    )
    visited = {source}
    while queue:
        current, accumulated_rate, currencies, instruments = queue.popleft()
        if len(instruments) >= max_hops:
            continue
        for destination, edge_rate, instrument in edges.get(current, ()):
            if destination in visited:
                continue
            rate = accumulated_rate * edge_rate
            currency_path = currencies + (destination,)
            instrument_path = instruments + (instrument,)
            if destination == target:
                return ConversionResult(
                    source_currency=source,
                    target_currency=target,
                    source_amount=amount_value,
                    target_amount=amount_value * rate,
                    rate=rate,
                    currency_path=currency_path,
                    instrument_path=instrument_path,
                    pricing=pricing_value,
                )
            visited.add(destination)
            queue.append((destination, rate, currency_path, instrument_path))
    raise ConversionUnavailableError(
        f"no {source}->{target} conversion path within {max_hops} quote(s)"
    )


def convert_currency(
    amount: float,
    source_currency: str,
    target_currency: str,
    quotes: Mapping[str, QuoteLike],
    *,
    pricing: str = "executable",
    max_hops: int = 3,
) -> float:
    """Return only the converted amount; see :func:`resolve_conversion`."""

    return resolve_conversion(
        amount,
        source_currency,
        target_currency,
        quotes,
        pricing=pricing,
        max_hops=max_hops,
    ).target_amount


def _side_sign(side: str) -> int:
    value = str(side).strip().upper()
    if value in {"LONG", "BUY", "UP"}:
        return 1
    if value in {"SHORT", "SELL", "DOWN"}:
        return -1
    raise EconomicsError(f"invalid trade side: {side!r}")


def market_fill(
    quote: QuoteLike, order_units: int, *, adverse_slippage_price: float = 0.0
) -> float:
    """Fill a signed market order at native bid/ask plus adverse slippage."""

    if isinstance(order_units, bool) or not isinstance(order_units, int) or order_units == 0:
        raise EconomicsError("order_units must be a non-zero integer")
    slippage = _float(adverse_slippage_price, "adverse_slippage_price")
    if slippage < 0:
        raise EconomicsError("adverse_slippage_price cannot be negative")
    book = coerce_quote(quote)
    return book.ask + slippage if order_units > 0 else book.bid - slippage


def entry_fill(
    quote: QuoteLike, side: str, *, adverse_slippage_price: float = 0.0
) -> float:
    return market_fill(
        quote, _side_sign(side), adverse_slippage_price=adverse_slippage_price
    )


def exit_fill(
    quote: QuoteLike, side: str, *, adverse_slippage_price: float = 0.0
) -> float:
    return market_fill(
        quote, -_side_sign(side), adverse_slippage_price=adverse_slippage_price
    )


def spread_pips(quote: QuoteLike, meta: InstrumentMeta) -> float:
    return coerce_quote(quote).spread / meta.pip_size


def gross_pnl_quote(units: int, entry_price: float, exit_price: float) -> float:
    """P/L in quote currency for signed base-currency units."""

    if isinstance(units, bool) or not isinstance(units, int) or units == 0:
        raise EconomicsError("units must be a non-zero signed integer")
    entry = _float(entry_price, "entry_price")
    exit_value = _float(exit_price, "exit_price")
    if entry <= 0 or exit_value <= 0:
        raise EconomicsError("entry and exit prices must be positive")
    return units * (exit_value - entry)


def pip_value_in_account(
    meta: InstrumentMeta,
    account_currency: str,
    quotes: Mapping[str, QuoteLike],
    *,
    units: int = 1,
    liability: bool = False,
    pricing: str = "executable",
) -> float:
    """Absolute account-currency value of one pip for ``units``.

    Use ``liability=True`` for stop-risk sizing so a loss is converted using the
    conservative side of the conversion market.
    """

    if isinstance(units, bool) or not isinstance(units, int) or units <= 0:
        raise EconomicsError("units must be a positive integer")
    amount = meta.pip_size * units * (-1.0 if liability else 1.0)
    converted = resolve_conversion(
        amount, meta.quote_currency, account_currency, quotes, pricing=pricing
    ).target_amount
    return abs(converted)


def position_notional_in_account(
    meta: InstrumentMeta,
    units: int,
    account_currency: str,
    quotes: Mapping[str, QuoteLike],
    *,
    pricing: str = "mid",
) -> float:
    """Absolute base notional expressed in account currency."""

    if isinstance(units, bool) or not isinstance(units, int):
        raise EconomicsError("units must be an integer")
    converted = resolve_conversion(
        float(abs(units)), meta.base_currency, account_currency, quotes, pricing=pricing
    ).target_amount
    return abs(converted)


def margin_required(
    meta: InstrumentMeta,
    units: int,
    account_currency: str,
    quotes: Mapping[str, QuoteLike],
    *,
    pricing: str = "mid",
) -> float:
    """Research estimate of initial margin from current converted base notional."""

    return position_notional_in_account(
        meta, units, account_currency, quotes, pricing=pricing
    ) * meta.margin_rate


def margin_closeout_percent(
    margin_closeout_nav: float,
    margin_closeout_margin_used: float,
    *,
    maintenance_factor: float = 0.5,
) -> float:
    """Approximate the OANDA v20 MCO ratio (``1.0`` is closeout).

    The default 50% maintenance factor models v20 FX accounts.  A caller should
    override it when replaying a different account division/product.  Broker-
    supplied ``marginCloseoutNAV`` and ``marginCloseoutMarginUsed`` should be used
    when available; locally reconstructed values remain an approximation.
    """

    nav = _float(margin_closeout_nav, "margin_closeout_nav")
    used = _float(margin_closeout_margin_used, "margin_closeout_margin_used")
    factor = _float(maintenance_factor, "maintenance_factor")
    if used < 0 or factor <= 0:
        raise EconomicsError("margin used cannot be negative and maintenance factor must be positive")
    if used == 0:
        return 0.0
    if nav <= 0:
        return math.inf
    return factor * used / nav


@dataclass(frozen=True)
class MarginState:
    nav: float
    margin_used: float
    margin_available: float
    margin_used_fraction: float
    margin_closeout_percent: float
    warning_threshold: float
    closeout_threshold: float
    in_margin_warning: bool
    in_margin_closeout: bool


def calculate_margin_state(
    nav: float,
    margin_used: float,
    *,
    maintenance_factor: float = 0.5,
    warning_threshold: float = 0.5,
    closeout_threshold: float = 1.0,
) -> MarginState:
    """Calculate replay margin availability, utilization, and MCO flags."""

    nav_value = _float(nav, "nav")
    used = _float(margin_used, "margin_used")
    warning = _float(warning_threshold, "warning_threshold")
    closeout = _float(closeout_threshold, "closeout_threshold")
    if used < 0 or warning < 0 or closeout <= 0 or warning > closeout:
        raise EconomicsError("invalid margin state thresholds")
    used_fraction = used / nav_value if nav_value > 0 else (math.inf if used > 0 else 0.0)
    mco = margin_closeout_percent(
        nav_value, used, maintenance_factor=maintenance_factor
    )
    return MarginState(
        nav=nav_value,
        margin_used=used,
        margin_available=nav_value - used,
        margin_used_fraction=used_fraction,
        margin_closeout_percent=mco,
        warning_threshold=warning,
        closeout_threshold=closeout,
        in_margin_warning=mco >= warning,
        in_margin_closeout=mco >= closeout,
    )


@dataclass(frozen=True)
class AccountState:
    account_currency: str
    balance: float
    nav: float
    margin_used: float = 0.0
    margin_available: Optional[float] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_currency", normalize_currency(self.account_currency))
        balance = _float(self.balance, "balance")
        nav = _float(self.nav, "nav")
        margin_used = _float(self.margin_used, "margin_used")
        if nav <= 0 or margin_used < 0:
            raise EconomicsError("NAV must be positive and margin_used cannot be negative")
        available = _optional_float(self.margin_available, "margin_available")
        object.__setattr__(self, "balance", balance)
        object.__setattr__(self, "nav", nav)
        object.__setattr__(self, "margin_used", margin_used)
        object.__setattr__(self, "margin_available", available)

    @property
    def effective_margin_available(self) -> float:
        reconstructed = self.nav - self.margin_used
        if self.margin_available is None:
            return max(0.0, reconstructed)
        return max(0.0, min(reconstructed, self.margin_available))

    @classmethod
    def from_oanda(cls, raw: Mapping[str, Any]) -> "AccountState":
        """Parse a captured account summary without making any API request."""

        currency = raw.get("currency", raw.get("accountCurrency", raw.get("account_currency")))
        if not currency:
            raise MissingMetadataError("account summary is missing account currency")
        if "NAV" not in raw and "nav" not in raw:
            raise MissingMetadataError("account summary is missing NAV")
        nav = raw.get("NAV", raw.get("nav"))
        balance = raw.get("balance", nav)
        return cls(
            account_currency=str(currency),
            balance=_float(balance, "balance"),
            nav=_float(nav, "NAV"),
            margin_used=_float(raw.get("marginUsed", raw.get("margin_used", 0)), "marginUsed"),
            margin_available=_optional_float(
                raw.get("marginAvailable", raw.get("margin_available")), "marginAvailable"
            ),
        )


@dataclass(frozen=True)
class PositionSizing:
    instrument: str
    side: str
    units: int
    absolute_units: int
    risk_budget_account: float
    risk_per_unit_account: float
    expected_stop_loss_account: float
    margin_per_unit_account: float
    margin_required_account: float
    margin_budget_account: float
    limiting_factor: str
    rejection_reason: str = ""

    @property
    def accepted(self) -> bool:
        return self.units != 0 and not self.rejection_reason


def _floor_units(value: float) -> int:
    if value <= 0 or not math.isfinite(value):
        return 0
    # Decimal prices commonly leave a mathematically integral ratio a handful of
    # ULPs below its integer value (for example 100 / (1.1002 - 1.0902)).
    # Snap only at machine-noise distance; otherwise retain conservative flooring.
    nearest = round(value)
    if math.isclose(value, nearest, rel_tol=1e-12, abs_tol=1e-12):
        return int(nearest)
    return math.floor(value)


def size_position(
    meta: InstrumentMeta,
    account: AccountState,
    quotes: Mapping[str, QuoteLike],
    *,
    side: str,
    entry_price: float,
    stop_price: float,
    risk_fraction: Optional[float] = None,
    risk_amount_account: Optional[float] = None,
    max_margin_fraction: float = 1.0,
    max_mco_fraction: Optional[float] = None,
    maintenance_factor: float = 0.5,
) -> PositionSizing:
    """Risk- and margin-cap a position, returning signed integer OANDA units.

    Exactly one of ``risk_fraction`` and ``risk_amount_account`` is required.  The
    function never rounds up to a broker minimum: doing so could exceed the stated
    risk budget.  Missing quote/account conversion data raises explicitly.
    """

    sign = _side_sign(side)
    side_name = "LONG" if sign > 0 else "SHORT"
    entry = _float(entry_price, "entry_price")
    stop = _float(stop_price, "stop_price")
    if entry <= 0 or stop <= 0 or entry == stop:
        raise EconomicsError("entry and stop prices must be positive and different")
    if sign > 0 and stop >= entry:
        raise EconomicsError("a long stop must be below entry")
    if sign < 0 and stop <= entry:
        raise EconomicsError("a short stop must be above entry")
    if (risk_fraction is None) == (risk_amount_account is None):
        raise EconomicsError("provide exactly one risk budget")
    if risk_fraction is not None:
        fraction = _float(risk_fraction, "risk_fraction")
        if not 0 < fraction <= 1:
            raise EconomicsError("risk_fraction must be in (0, 1]")
        risk_budget = account.nav * fraction
    else:
        risk_budget = _float(risk_amount_account, "risk_amount_account")
        if risk_budget <= 0:
            raise EconomicsError("risk_amount_account must be positive")

    margin_fraction = _float(max_margin_fraction, "max_margin_fraction")
    maintenance = _float(maintenance_factor, "maintenance_factor")
    if not 0 < margin_fraction <= 1 or maintenance <= 0:
        raise EconomicsError("invalid margin cap or maintenance factor")

    stop_distance_quote = abs(entry - stop)
    converted_loss = resolve_conversion(
        -stop_distance_quote,
        meta.quote_currency,
        account.account_currency,
        quotes,
        pricing="executable",
    ).target_amount
    risk_per_unit = abs(converted_loss)
    if risk_per_unit <= 0:
        raise EconomicsError("risk per unit is zero")

    margin_per_unit = margin_required(
        meta, 1, account.account_currency, quotes, pricing="mid"
    )
    if margin_per_unit <= 0:
        raise EconomicsError("margin per unit is zero")

    cap_budget = max(0.0, account.nav * margin_fraction - account.margin_used)
    margin_budget = min(account.effective_margin_available, cap_budget)
    if max_mco_fraction is not None:
        mco_cap = _float(max_mco_fraction, "max_mco_fraction")
        if not 0 < mco_cap < 1:
            raise EconomicsError("max_mco_fraction must be in (0, 1)")
        total_margin_at_cap = account.nav * mco_cap / maintenance
        margin_budget = min(margin_budget, max(0.0, total_margin_at_cap - account.margin_used))

    constraints: list[tuple[str, float]] = [
        ("risk", risk_budget / risk_per_unit),
        ("margin", margin_budget / margin_per_unit),
    ]
    if meta.maximum_integer_order_units is not None:
        constraints.append(("maximum_order_units", float(meta.maximum_integer_order_units)))
    limiting_factor, raw_units = min(constraints, key=lambda item: item[1])
    absolute_units = _floor_units(raw_units)
    minimum_units = meta.minimum_integer_units
    if absolute_units < minimum_units:
        return PositionSizing(
            instrument=meta.name,
            side=side_name,
            units=0,
            absolute_units=0,
            risk_budget_account=risk_budget,
            risk_per_unit_account=risk_per_unit,
            expected_stop_loss_account=0.0,
            margin_per_unit_account=margin_per_unit,
            margin_required_account=0.0,
            margin_budget_account=margin_budget,
            limiting_factor=limiting_factor,
            rejection_reason=(
                f"sized units {absolute_units} below integer broker minimum {minimum_units}"
            ),
        )

    signed_units = sign * absolute_units
    return PositionSizing(
        instrument=meta.name,
        side=side_name,
        units=signed_units,
        absolute_units=absolute_units,
        risk_budget_account=risk_budget,
        risk_per_unit_account=risk_per_unit,
        expected_stop_loss_account=absolute_units * risk_per_unit,
        margin_per_unit_account=margin_per_unit,
        margin_required_account=absolute_units * margin_per_unit,
        margin_budget_account=margin_budget,
        limiting_factor=limiting_factor,
    )


@dataclass(frozen=True)
class TradeCostContext:
    meta: InstrumentMeta
    account_currency: str
    units: int
    entry_fill: float
    exit_fill: float
    entry_quote: MarketQuote
    exit_quote: MarketQuote
    gross_pnl_quote: float
    gross_pnl_account: float
    entry_notional_account: float
    holding_days: float


ChargeHook = Callable[[TradeCostContext], float]


def _zero_charge(_: TradeCostContext) -> float:
    return 0.0


@dataclass(frozen=True)
class CostHooks:
    """Explicit account-currency cost/cashflow hooks.

    Commission and conversion hooks return non-negative charges.  Financing
    returns a signed cashflow: negative for a debit, positive for a credit.
    """

    commission: ChargeHook = _zero_charge
    conversion_charge: ChargeHook = _zero_charge
    financing: ChargeHook = _zero_charge


@dataclass(frozen=True)
class RoundTripResult:
    instrument: str
    units: int
    entry_fill: float
    exit_fill: float
    ideal_mid_pnl_quote: float
    spread_cost_quote: float
    slippage_cost_quote: float
    gross_pnl_quote: float
    gross_pnl_account: float
    commission_account: float
    conversion_charge_account: float
    financing_account: float
    net_pnl_account: float


def _hook_value(value: Any, name: str, *, nonnegative: bool) -> float:
    result = _float(value, name)
    if nonnegative and result < 0:
        raise EconomicsError(f"{name} hook must return a non-negative charge")
    return result


def round_trip_pnl(
    meta: InstrumentMeta,
    units: int,
    entry_quote: QuoteLike,
    exit_quote: QuoteLike,
    account_currency: str,
    conversion_quotes: Mapping[str, QuoteLike],
    *,
    entry_slippage_pips: float = 0.0,
    exit_slippage_pips: float = 0.0,
    holding_days: float = 0.0,
    cost_hooks: Optional[CostHooks] = None,
) -> RoundTripResult:
    """Mark an integer-unit FX round trip using executable bid/ask fills."""

    if isinstance(units, bool) or not isinstance(units, int) or units == 0:
        raise EconomicsError("units must be a non-zero signed integer")
    entry_book = coerce_quote(entry_quote)
    exit_book = coerce_quote(exit_quote)
    entry_slippage = _float(entry_slippage_pips, "entry_slippage_pips")
    exit_slippage = _float(exit_slippage_pips, "exit_slippage_pips")
    days = _float(holding_days, "holding_days")
    if entry_slippage < 0 or exit_slippage < 0 or days < 0:
        raise EconomicsError("slippage and holding_days cannot be negative")

    entry_slippage_price = entry_slippage * meta.pip_size
    exit_slippage_price = exit_slippage * meta.pip_size
    entry_price = market_fill(
        entry_book, units, adverse_slippage_price=entry_slippage_price
    )
    exit_price = market_fill(
        exit_book, -units, adverse_slippage_price=exit_slippage_price
    )
    no_slip_entry = market_fill(entry_book, units)
    no_slip_exit = market_fill(exit_book, -units)
    ideal_mid = units * (exit_book.mid - entry_book.mid)
    no_slip_pnl = units * (no_slip_exit - no_slip_entry)
    pnl_quote = gross_pnl_quote(units, entry_price, exit_price)
    # The traded instrument's exit quote is itself valid conversion evidence.  A
    # caller therefore need not duplicate it in ``conversion_quotes``.
    effective_conversion_quotes = dict(conversion_quotes)
    effective_conversion_quotes.setdefault(meta.name, exit_book)
    pnl_account = resolve_conversion(
        pnl_quote,
        meta.quote_currency,
        account_currency,
        effective_conversion_quotes,
        pricing="executable",
    ).target_amount
    entry_notional = position_notional_in_account(
        meta, units, account_currency, effective_conversion_quotes, pricing="mid"
    )
    context = TradeCostContext(
        meta=meta,
        account_currency=normalize_currency(account_currency),
        units=units,
        entry_fill=entry_price,
        exit_fill=exit_price,
        entry_quote=entry_book,
        exit_quote=exit_book,
        gross_pnl_quote=pnl_quote,
        gross_pnl_account=pnl_account,
        entry_notional_account=entry_notional,
        holding_days=days,
    )
    hooks = cost_hooks or CostHooks()
    commission = _hook_value(hooks.commission(context), "commission", nonnegative=True)
    conversion_charge = _hook_value(
        hooks.conversion_charge(context), "conversion_charge", nonnegative=True
    )
    financing = _hook_value(hooks.financing(context), "financing", nonnegative=False)
    return RoundTripResult(
        instrument=meta.name,
        units=units,
        entry_fill=entry_price,
        exit_fill=exit_price,
        ideal_mid_pnl_quote=ideal_mid,
        spread_cost_quote=max(0.0, ideal_mid - no_slip_pnl),
        slippage_cost_quote=max(0.0, no_slip_pnl - pnl_quote),
        gross_pnl_quote=pnl_quote,
        gross_pnl_account=pnl_account,
        commission_account=commission,
        conversion_charge_account=conversion_charge,
        financing_account=financing,
        net_pnl_account=pnl_account - commission - conversion_charge + financing,
    )


def commission_per_million(
    account_currency_charge_per_million_per_side: float,
) -> ChargeHook:
    """Create a simple two-sided commission hook from account notional."""

    rate = _float(
        account_currency_charge_per_million_per_side,
        "account_currency_charge_per_million_per_side",
    )
    if rate < 0:
        raise EconomicsError("commission rate cannot be negative")

    def charge(context: TradeCostContext) -> float:
        return 2.0 * context.entry_notional_account * rate / 1_000_000.0

    return charge


def pnl_conversion_charge_bps(basis_points: float) -> ChargeHook:
    """Create a hook charging basis points on converted absolute P/L.

    This is a scenario hook, not a universal OANDA fee formula.  It applies only
    when the instrument quote currency differs from account currency.
    """

    bps = _float(basis_points, "basis_points")
    if bps < 0:
        raise EconomicsError("basis_points cannot be negative")

    def charge(context: TradeCostContext) -> float:
        if context.meta.quote_currency == context.account_currency:
            return 0.0
        return abs(context.gross_pnl_account) * bps / 10_000.0

    return charge


__all__ = [
    "AccountState",
    "ConversionResult",
    "ConversionUnavailableError",
    "CostHooks",
    "EconomicsError",
    "InstrumentMeta",
    "InvalidQuoteError",
    "MarginState",
    "MarketQuote",
    "MissingMetadataError",
    "PositionSizing",
    "RoundTripResult",
    "TradeCostContext",
    "build_instrument_metadata",
    "calculate_margin_state",
    "coerce_quote",
    "commission_per_million",
    "convert_currency",
    "entry_fill",
    "exit_fill",
    "gross_pnl_quote",
    "margin_closeout_percent",
    "margin_required",
    "market_fill",
    "normalize_currency",
    "normalize_instrument",
    "pip_value_in_account",
    "pnl_conversion_charge_bps",
    "position_notional_in_account",
    "resolve_conversion",
    "round_trip_pnl",
    "size_position",
    "split_instrument",
    "spread_pips",
]
