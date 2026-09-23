"""Project-local metadata assumptions and timestamped USD economics adapters.

Instrument margin rates are account/division specific.  The default builder is
therefore explicitly labelled an assumption set; a captured OANDA instrument
snapshot can replace it without changing replay code.  Currency conversion is
never assumed: every accepted replay outcome must have an executable local
quote path at its entry and exit timestamps.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .dataset import PIP_LOCATION_MINUS2, normalize_instrument
from .economics import (
    ConversionUnavailableError,
    InstrumentMeta,
    MarketQuote,
    margin_required,
    pip_value_in_account,
    resolve_conversion,
)


DEFAULT_MARGIN_BY_CURRENCY = {
    "TRY": 0.25,
    "CNH": 0.10,
    "MXN": 0.10,
    "ZAR": 0.07,
    "HKD": 0.05,
    "SGD": 0.05,
    "CZK": 0.05,
    "PLN": 0.05,
    "HUF": 0.05,
    "NOK": 0.05,
    "SEK": 0.05,
    "DKK": 0.05,
    "THB": 0.10,
}


def _assumed_margin_rate(
    instrument: str,
    *,
    default_margin_rate: float,
    jpy_chf_margin_rate: float,
    margin_by_currency: Mapping[str, float],
) -> tuple[float, str]:
    base, quote = instrument.split("_", 1)
    for currency in (base, quote):
        if currency in margin_by_currency:
            return float(margin_by_currency[currency]), f"currency_assumption:{currency}"
    if "JPY" in {base, quote} or "CHF" in {base, quote}:
        return float(jpy_chf_margin_rate), "jpy_chf_assumption"
    return float(default_margin_rate), "default_major_assumption"


def build_project_metadata(
    instruments: Sequence[str],
    *,
    explicit_oanda_rows: Sequence[Mapping[str, Any]] | None = None,
    margin_rate_overrides: Mapping[str, float] | None = None,
    default_margin_rate: float = 0.02,
    jpy_chf_margin_rate: float = 0.0333,
    margin_by_currency: Mapping[str, float] = DEFAULT_MARGIN_BY_CURRENCY,
) -> tuple[dict[str, InstrumentMeta], pd.DataFrame]:
    """Build strict metadata plus a row-level source/assumption ledger."""

    normalized = sorted({normalize_instrument(value) for value in instruments})
    captured: dict[str, InstrumentMeta] = {}
    if explicit_oanda_rows:
        for raw in explicit_oanda_rows:
            meta = InstrumentMeta.from_oanda(raw)
            captured[meta.name] = meta
    overrides = {
        normalize_instrument(key): float(value)
        for key, value in (margin_rate_overrides or {}).items()
    }
    metadata: dict[str, InstrumentMeta] = {}
    ledger: list[dict[str, Any]] = []
    for instrument in normalized:
        if instrument in captured:
            meta = captured[instrument]
            source = "captured_oanda_instrument_snapshot"
            margin_source = source
            pip_source = source
            account_specific = True
        else:
            pip_location = -2 if instrument in PIP_LOCATION_MINUS2 else -4
            if instrument in overrides:
                margin_rate = overrides[instrument]
                margin_source = "config_instrument_override"
            else:
                margin_rate, margin_source = _assumed_margin_rate(
                    instrument,
                    default_margin_rate=float(default_margin_rate),
                    jpy_chf_margin_rate=float(jpy_chf_margin_rate),
                    margin_by_currency=margin_by_currency,
                )
            meta = InstrumentMeta(
                name=instrument,
                pip_location=pip_location,
                margin_rate=margin_rate,
            )
            source = "project_research_assumption"
            pip_source = "validated_significant_move_inventory"
            account_specific = False
        metadata[instrument] = meta
        ledger.append(
            {
                **asdict(meta),
                "pip_size": meta.pip_size,
                "metadata_source": source,
                "pip_location_source": pip_source,
                "margin_rate_source": margin_source,
                "is_account_specific_snapshot": account_specific,
                "requires_refresh_before_shadow_or_live": not account_specific,
            }
        )
    return metadata, pd.DataFrame(ledger)


def _quotes_at_required_times(
    bars: pd.DataFrame, required: set[pd.Timestamp]
) -> dict[pd.Timestamp, dict[str, MarketQuote]]:
    needed = {"timestamp", "instrument", "bid_close", "ask_close"}
    missing = sorted(needed.difference(bars.columns))
    if missing:
        raise ValueError(f"bars missing quote columns: {missing}")
    source = bars.loc[pd.to_datetime(bars["timestamp"], utc=True).isin(required)].copy()
    source["timestamp"] = pd.to_datetime(source["timestamp"], utc=True, errors="raise")
    source["bid_close"] = pd.to_numeric(source["bid_close"], errors="coerce")
    source["ask_close"] = pd.to_numeric(source["ask_close"], errors="coerce")
    source = source.loc[
        source["bid_close"].gt(0)
        & source["ask_close"].ge(source["bid_close"])
        & source["instrument"].notna()
    ]
    result: dict[pd.Timestamp, dict[str, MarketQuote]] = {}
    for timestamp, part in source.groupby("timestamp", sort=False, observed=True):
        result[pd.Timestamp(timestamp)] = {
            normalize_instrument(instrument): MarketQuote(float(bid), float(ask))
            for instrument, bid, ask in part[
                ["instrument", "bid_close", "ask_close"]
            ].itertuples(index=False, name=None)
        }
    return result


def _converted_leg_pnl_per_unit(
    meta: InstrumentMeta,
    realized_pips: float,
    quotes: Mapping[str, MarketQuote],
    account_currency: str,
) -> float:
    quote_amount = float(realized_pips) * meta.pip_size
    return float(
        resolve_conversion(
            quote_amount,
            meta.quote_currency,
            account_currency,
            quotes,
            pricing="executable",
        ).target_amount
    )


def attach_timestamped_economics(
    outcomes: pd.DataFrame,
    bars: pd.DataFrame,
    metadata: Mapping[str, InstrumentMeta],
    *,
    account_currency: str = "USD",
) -> pd.DataFrame:
    """Attach per-unit risk, realized P/L, and margin using exact local quotes.

    Missing entry/exit conversions invalidate the outcome and preserve its
    reason.  There is deliberately no stale quote, static FX, or 1:1 fallback.
    Double fills are costed as two legs when sibling fields are present.
    """

    required = {
        "instrument",
        "entry_timestamp",
        "exit_timestamp",
        "realized_pips",
        "risk_pips_per_unit",
        "filled_legs",
    }
    missing = sorted(required.difference(outcomes.columns))
    if missing:
        raise ValueError(f"OCO outcomes missing economics columns: {missing}")
    output = outcomes.copy()
    for column in ("entry_timestamp", "exit_timestamp", "sibling_exit_timestamp"):
        if column in output:
            output[column] = pd.to_datetime(output[column], utc=True, errors="coerce")
    timestamps: set[pd.Timestamp] = set()
    for column in ("entry_timestamp", "exit_timestamp", "sibling_exit_timestamp"):
        if column in output:
            timestamps.update(pd.Timestamp(value) for value in output[column].dropna().unique())
    quotes_by_time = _quotes_at_required_times(bars, timestamps)

    attached: list[dict[str, Any]] = []
    for row in output.to_dict("records"):
        instrument = normalize_instrument(row["instrument"])
        entry_time = row.get("entry_timestamp")
        exit_time = row.get("exit_timestamp")
        base = {
            "economics_valid": False,
            "economics_rejection_reason": "",
            "risk_per_pip_account_per_unit": np.nan,
            "risk_account_per_unit": np.nan,
            "pnl_per_pip_account_per_unit": np.nan,
            "realized_pnl_account_per_unit": np.nan,
            "margin_per_unit_account": np.nan,
            "metadata_is_account_specific": False,
            "conversion_is_exact_timestamp": False,
        }
        if not bool(row.get("triggered", False)):
            base["economics_rejection_reason"] = "not_triggered"
            attached.append(base)
            continue
        meta = metadata.get(instrument)
        if meta is None:
            base["economics_rejection_reason"] = "missing_instrument_metadata"
            attached.append(base)
            continue
        entry_quotes = quotes_by_time.get(pd.Timestamp(entry_time), {}) if pd.notna(entry_time) else {}
        exit_quotes = quotes_by_time.get(pd.Timestamp(exit_time), {}) if pd.notna(exit_time) else {}
        try:
            risk_pip_value = pip_value_in_account(
                meta,
                account_currency,
                entry_quotes,
                units=1,
                liability=True,
                pricing="executable",
            )
            legs = max(1, int(row.get("filled_legs", 1)))
            margin_per_unit = margin_required(
                meta, 1, account_currency, entry_quotes, pricing="mid"
            )
            primary_pips = float(row.get("primary_realized_pips", row["realized_pips"]))
            primary_pnl = _converted_leg_pnl_per_unit(
                meta, primary_pips, exit_quotes, account_currency
            )
            sibling_pips = float(row.get("sibling_realized_pips", 0.0) or 0.0)
            sibling_pnl = 0.0
            if legs > 1:
                sibling_time = row.get("sibling_exit_timestamp", exit_time)
                sibling_quotes = (
                    quotes_by_time.get(pd.Timestamp(sibling_time), {})
                    if pd.notna(sibling_time)
                    else exit_quotes
                )
                sibling_pnl = _converted_leg_pnl_per_unit(
                    meta, sibling_pips, sibling_quotes, account_currency
                )
            realized_account = primary_pnl + sibling_pnl
            total_pips = float(row["realized_pips"])
            base.update(
                {
                    "economics_valid": True,
                    "risk_per_pip_account_per_unit": float(risk_pip_value),
                    "risk_account_per_unit": float(risk_pip_value)
                    * float(row["risk_pips_per_unit"]),
                    "pnl_per_pip_account_per_unit": (
                        float(realized_account / total_pips)
                        if total_pips != 0.0
                        else 0.0
                    ),
                    "realized_pnl_account_per_unit": float(realized_account),
                    "margin_per_unit_account": float(margin_per_unit),
                    "conversion_is_exact_timestamp": True,
                }
            )
        except (ConversionUnavailableError, ValueError) as exc:
            base["economics_rejection_reason"] = (
                f"{type(exc).__name__}:{str(exc)}"
            )
        attached.append(base)
    economics = pd.DataFrame(attached, index=output.index)
    return pd.concat([output, economics], axis=1)
