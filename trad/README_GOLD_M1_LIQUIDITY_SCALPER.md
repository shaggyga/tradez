# Gold M1 Liquidity/FVG Scalper

`gold_m1_liquidity_scalper.py` is a research and signal engine for a mechanical
gold scalping model. It does not place live orders.

## What It Implements

- Optional 15m/1h EMA bias filter.
- M1 sweep of recent highs or lows.
- Displacement candle in the intended direction.
- Three-candle fair value gap.
- Limit entry on retrace into the FVG.
- Stop beyond the sweep.
- Target opposing liquidity, with fixed-R fallback.
- Fixed fractional sizing using MGC-style contract math.

Default contract math is for Micro Gold futures:

- `point_value = 10.0`, meaning a full `$1.00` gold move is `$10` per contract.
- Round-turn cost defaults to `$3.00` commission plus `0.20` gold points of slippage.

## Backtest

```powershell
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\gold_m1_liquidity_scalper.py backtest `
  --csv .\path\to\MGC_M1.csv `
  --initial-equity 10000 `
  --risk-pct 0.5 `
  --output-dir .\trad\data\gold_m1_liquidity_scalper\reports\my_run
```

The CSV must contain M1 candles with time/open/high/low/close columns. Common
aliases like `timestamp`, `datetime`, `o`, `h`, `l`, and `c` are accepted.

Outputs:

- `summary.json`
- `config.json`
- `signals.csv`
- `trades.csv`

## Download Free Research Data

Recent spot-style XAUUSD minute candles can be pulled from Dukascopy:

```powershell
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\download_dukascopy_gold_m1.py `
  --symbol XAUUSD `
  --side BID `
  --start 2026-06-24 `
  --end 2026-06-30
```

Recent COMEX gold futures proxy candles can be pulled from Yahoo Finance:

```powershell
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\download_yahoo_gold_futures_m1.py `
  --symbol GC=F `
  --interval 1m `
  --range 5d
```

Current workspace data files:

- `trad\data\gold_m1_liquidity_scalper\candles\XAUUSD_BID_M1_2026-06-24_2026-06-30.csv`
- `trad\data\gold_m1_liquidity_scalper\candles\GC_F_YAHOO_M1_5D.csv`

## Latest Signal

```powershell
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\gold_m1_liquidity_scalper.py signal `
  --csv .\path\to\MGC_M1.csv
```

This prints JSON showing whether the latest detected setup is active, expired,
or absent.

## Practical Notes

Start with backtest and signal mode. Full automation should only be added behind
paper-trading and kill-switch controls after the CSV model survives realistic
slippage, commission, news windows, and out-of-sample testing.
