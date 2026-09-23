# Offline FX economics interface

`economics.py` contains the broker-economics boundary for spike account-space
research. It makes no API calls and does not import a live account manager.

## Required captured inputs

For every instrument, pass an account-specific metadata snapshot to
`InstrumentMeta.from_oanda(...)`:

- `name`
- `pipLocation` (mandatory; never inferred from JPY)
- `marginRate` (mandatory)
- optional `displayPrecision`, `tradeUnitsPrecision`, `minimumTradeSize`,
  `maximumOrderUnits`, and `type`

This handles exceptions such as `HKD_JPY` solely from its explicit
`pipLocation`. Preserve the raw snapshot timestamp and account division in the
calling run manifest because margin rates and the supported universe can differ
by account.

Quotes are passed as `MarketQuote`, flat `{bid, ask}` / `{bid_c, ask_c}` rows, or
captured OANDA pricing dictionaries with `bids` and `asks`. No midpoint-only row
is considered executable.

## Primary API

- Metadata: `InstrumentMeta`, `build_instrument_metadata`
- Quotes and fills: `MarketQuote`, `market_fill`, `entry_fill`, `exit_fill`,
  `spread_pips`
- Conversion: `resolve_conversion`, `convert_currency`
- Value and margin: `pip_value_in_account`, `position_notional_in_account`,
  `margin_required`, `margin_closeout_percent`, `calculate_margin_state`
- Sizing: `AccountState`, `size_position`, `PositionSizing`
- Round trips and costs: `round_trip_pnl`, `CostHooks`,
  `commission_per_million`, `pnl_conversion_charge_bps`

Import these directly from `trad.spike_account_space.economics`; the shared
package `__init__.py` is maintained by the subsystem integrator.

## Important conventions

- Trade and order units are signed integers.
- `risk_fraction=0.0025` means 0.25% of NAV.
- The sizing function never rounds up to minimum size when that would breach the
  stated budget.
- Executable conversion sells positive assets at bid and covers negative
  liabilities at ask, including deterministic routes of up to three quote legs.
- Missing conversion raises `ConversionUnavailableError`. Only same-currency
  conversion has a 1.0 rate.
- `margin_closeout_percent` is the OANDA v20 ratio convention: `1.0` means 100%
  and closeout, not 1%. Its default maintenance factor is 0.5 and is caller-
  configurable for other account products/divisions.
- Bid/ask spread and configured slippage are included by `round_trip_pnl`.
  Commission and conversion hooks return non-negative account-currency charges;
  financing returns a signed cashflow (negative debit, positive credit).
- Local margin reconstruction is research-grade. Prefer broker-captured
  `marginCloseoutNAV`, `marginCloseoutMarginUsed`, and `marginCloseoutPercent`
  for reconciliation whenever available.

## Minimal example

```python
from trad.spike_account_space.economics import (
    AccountState,
    InstrumentMeta,
    MarketQuote,
    size_position,
)

instrument = InstrumentMeta.from_oanda({
    "name": "EUR_USD",
    "type": "CURRENCY",
    "pipLocation": -4,
    "marginRate": "0.02",
    "minimumTradeSize": "1",
})
quotes = {"EUR_USD": MarketQuote(1.0998, 1.1002)}
account = AccountState("USD", balance=10_000, nav=10_000)

sizing = size_position(
    instrument,
    account,
    quotes,
    side="LONG",
    entry_price=1.1002,
    stop_price=1.0902,
    risk_fraction=0.0025,
    max_margin_fraction=0.20,
)
```

Run the focused tests with:

```powershell
python -m unittest trad.spike_account_space.test_economics -v
```
