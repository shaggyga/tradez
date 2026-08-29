# S1 Layer Of The Unified Forecast Matrix

## Purpose

`oanda_second_forecast_runner.py` and `oanda_second_forecast.py` remove the M1
candle boundary from fast forecast generation. Their three S1 ridge profile lanes
belong to the same timeframe x family x variant x pair x outcome-horizon matrix
as the 156 candle lanes. They run in separate operating-system processes only to
isolate latency. The supervisor starts a `hot` role with research
persistence disabled and a `tracker` role for sampled forecasts/outcomes. Slow
SQLite or promotion writes can delay the tracker but cannot pause account
forecasts. OANDA stream events are retained in memory and normalized to one
causal snapshot per second. This changes forecast latency, not the evidence
standard for placing a practice order.

The account route is practice account `-007` only. No live-money route is added.

## Current Fit

The 2026-07-16 production snapshot covers all 68 available pairs and all 12
configured horizons, for 816 pair-horizon models. Of those, 799 are
pair-specific and account-eligible; 17 are labeled research-only proxies. The
proxies cover all 12 TRY/JPY horizons plus five sparse USD/TRY horizons.

## Data Flow

1. `MultiPriceStream` receives OANDA pricing events, normally up to four updates
   per instrument per second.
2. The stream retains executable bid/ask plus update version, depth imbalance,
   top/total liquidity, level counts, and microprice.
3. `SecondFeatureEngine` creates an in-memory one-second grid. Gaps up to five
   seconds are forward-filled and marked with zero activity; longer gaps reset
   the pair window.
4. After 60 causal seconds, compact ridge snapshots forecast signed movement at
   15s, 30s, M1, M3, M5, M10, M15, M30, H1, H2, H3, and H4. Features cover
   5/10/30/60-second movement, acceleration, volatility, range, spread, quote
   activity, session, and live depth diagnostics.
5. Candidate selection is called before SQLite forecast/outcome persistence.
6. Sampled forecasts mature against executable bid/ask prices in
   `second_forecast_live_v1.sqlite` for misses and what-if analysis.

## Account Safety

- 15/30-second outcomes are shadow-only under the current execution-horizon list.
- M1/M3/M5/M10/M15/M30/H1/H2/H3/H4 signals can become account candidates, but
  `LanePromotionModel` still requires persistent forward samples, chronological
  holdout blocks, positive executable-net confidence, pair breadth, and session
  breadth.
- Every available S5 pair remains represented in the model snapshot. Sparse
  pair/horizon combinations receive labeled pooled proxies for research
  coverage only. Proxy models have `account_eligible=false`; they cannot place
  practice orders until target-pair evidence supports a pair-specific fit.
- Historical fit metrics are diagnostics. They cannot bypass the live gate.
- The fast and candle selectors share `practice_007_order.lock`. Once locked,
  the winner rechecks broker open trades before submitting an order.
- The initial 30-day S5 sweep fitted 213 models on 59 pairs and found zero
  cost-positive confidence-qualified holdouts. It is not trading evidence.

## Runtime Files

| Purpose | Path under `trad/data/oanda_training_manager` |
|---|---|
| Compact model snapshot | `state/second_ridge_models_v1.json` |
| Live sampled ledger | `state/second_forecast_live_v1.sqlite` |
| S1 path/exit-fit ledger | `state/second_forecast_exit_fit_v1.sqlite` |
| S1 immediate promotion ledger | `state/second_forecast_promotion_v1.sqlite` |
| Live latency/state summary | `state/second_forecast_live_v1.json` |
| Hot-path latency/state summary | `state/second_forecast_hot_v1.json` |
| Fit report | `reports/second_ridge_fit_v1.json` |
| Raw S1 quote ledger | `state/micro_pattern_quotes_v1.sqlite3` |
| S5 training files | `candles_s5_bam/*_S5.parquet` |

## Manual Fit

```powershell
D:\forex\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe `
  D:\forex\trad\oanda_second_forecast_fit.py `
  --source D:\forex\trad\data\oanda_training_manager\candles_s5_bam `
  --pairs all --horizons 15,30,60,180,300,600,900,1800,3600,7200,10800,14400 --sample-step 4 `
  --output D:\forex\trad\data\oanda_training_manager\state\second_ridge_models_v1.json `
  --report D:\forex\trad\data\oanda_training_manager\reports\second_ridge_fit_v1.json
```

The fitter adaptively retries sparse pairs at denser S5 sampling before using a
research-only proxy. The supervisor repeats the fit every six hours. The
strategy checks for a new model snapshot every 60 seconds and swaps it in memory
without a restart. Live inference still runs each second at every horizon;
H2-H4 research outcomes are sampled at a lower cadence to control ledger growth.

S1 path/exit fitting uses its own SQLite ledger so short-horizon maturity bursts
cannot contend with the much larger candle-lane exit ledger. Promotion evidence
is first committed to an S1 ledger, then merged by `oanda_lane_promotion_fit.py`
into the shared `lane_promotion_v1.sqlite` namespace before the unified gate is
fitted.

## Latency Checks

Inspect `second_forecast_hot_v1.json` for account-path `latency_ms_latest`,
`latency_ms_p50`, and `latency_ms_p95`. The main JSONL log adds
`forecast_to_submit_ms` and `order_roundtrip_ms` if a signal ever clears the
promotion gate. Research persistence occurs after the candidate callback, so
the SQLite ledger is absent from the hot process and excluded from
forecast-to-submit latency. Use `second_forecast_live_v1.json` for tracker
counts and matured outcomes.

`forex_model_vault_sync.py` includes the compact model snapshot, this runbook,
and the fit report in both the portable checkpoint ZIP and
`artifacts/second_forecast` under each configured vault destination. Raw
forecast ledgers and market data remain excluded.
