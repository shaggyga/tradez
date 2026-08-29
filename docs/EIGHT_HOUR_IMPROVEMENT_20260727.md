# Eight-Hour Live Improvement Run - 2026-07-27

## Scope

This run improves measurement, data integrity, and prospective validation around
the always-on OANDA practice and shadow system. It does not claim profitability
and does not authorize a new route to account `-007`.

The canonical runtime is `D:\forex`; the compact credential-free audit copy is
stored in the OneDrive vault. D: has a known physical bad-sector history.
Recent zero-error windows are operational observations, not proof that the disk
is repaired.

## Runtime Changes

- Compacted unified signal-feed payloads and tuned SQLite access. A measured
  1,589-candidate publish improved from about 9.16 seconds to 2.99 seconds and
  later cycles commonly completed below 0.5 seconds.
- Added a dedicated five-second prospective microstructure collector. It
  samples all 68 priced OANDA FX instruments, stores research outcomes locally,
  and cannot publish account-eligible signals.
- Replaced linear pending-outcome scans with a maturity heap.
- Kept hot account evaluation, the shared tracker, and dense microstructure
  collection as separate latency roles.
- Added causal leave-one-pair-out cross-pair fields. A pair's own current move
  cannot leak into its peer-strength inputs.
- Expanded the prospective feature vector from 49 to 69 fields.
- Forced the existing `origin_epoch` SQLite index for bounded panel reads. The
  enriched 919 MB database scan fell from an open-ended full matured-row scan to
  a 25-second bounded build.
- Added calibration-quantile thresholds for rare spread-clearing targets.
  Fixed 0.50-0.90 thresholds were invalid when calibrated event probabilities
  had a 1-10% base rate.
- Corrected profit factor for an all-winning subset from `0` to unbounded.
- Added a nonblocking singleton lock and startup-stage heartbeat to the modern
  model-gap live worker.
- Reordered each model-gap cycle to forecast and publish before maturing the
  historical outcome backlog.

## Prospective Feature Contract

The collector stores:

- midpoint returns, volatility, range, and price efficiency;
- bid/ask spread level, changes, and short-window spread volatility;
- top and total bid/ask liquidity, depth totals, level counts, top-depth
  imbalance, top-liquidity share, and level imbalance;
- microprice displacement and depth changes;
- quote flow over 1, 5, and 30 seconds;
- quote activity, activity acceleration, and tick imbalance;
- leave-one-pair-out base/quote currency return and quote-flow strengths,
  cross-sectional ranks, and residuals.

OANDA liquidity buckets describe liquidity available through the account's
pricing stream. They are not a centralized foreign-exchange order book.

## Validation Added

`oanda_second_microstructure_panel.py` materializes only matured prospective
outcomes. It can enforce enriched-feature presence and a minimum origin epoch.

`oanda_shared_panel_model_benchmark.py` now supports S1 input labels, preserves
all-missing optional columns across folds, reports feature coverage, and
evaluates either direction or best executable side.

`oanda_live_signal_gate_audit.py`:

1. consolidates duplicate model predictions to one pair/horizon/origin;
2. selects a cost-aware threshold only on the oldest 70% of outcomes;
3. evaluates the selected threshold on an untouched newest 30%;
4. reports time-block stability and a lower 95% estimate;
5. never writes account authorization.

## Evidence So Far

The first liquid-16 prospective panel contained 23,631 unique events and 47,262
rows across 15-900 second outcomes.

Direction benchmark:

| Model | Mean fold AUC | Holdout AUC | Holdout all-action net pips | Selected |
|---|---:|---:|---:|---:|
| Logistic | 0.4131 | 0.4236 | -2.5969 | 0 |
| Histogram gradient boosting | 0.4920 | 0.4498 | -2.5429 | 0 |

The final chronological all-signal gate audit selected a liquid-pair,
confidence-at-least-0.52, horizon-at-most-four-hours, spread-at-most-1.5-pip
candidate on the fit segment. It still lost:

| Segment | N | Direction | Win rate | Avg net pips | Positive blocks | Lower 95% |
|---|---:|---:|---:|---:|---:|---:|
| Oldest 70% fit | 356 | 56.18% | 18.54% | -1.0188 | 3/11 | -1.3053 |
| Newest 30% holdout | 169 | 46.15% | 17.16% | -1.4686 | 0/8 | -1.7051 |

The correct outcome is `no_stable_positive_gate`. Raw direction accuracy does
not overcome entry/exit spread and signal magnitude error.

The completed 69-feature liquid-16 panel contains 30,256 unique events and
60,512 long/short rows from 10:18:45 through 11:22:00 UTC. All required
activity, spread-dynamics, depth, and causal cross-pair fields have 100%
coverage. It has zero duplicate conflicts and zero invalid-cost rows.

Enriched logistic direction result:

- mean purged-fold AUC: 0.5253;
- untouched holdout AUC: 0.5632;
- holdout all-action net: -2.0845 pips;
- selected trades: 0.

The same features ranked spread-clearing movement much better: profitability
mean-fold AUC was 0.8170 and holdout AUC was 0.8422. This did not identify side;
no calibration threshold produced positive combined economics.

`oanda_two_stage_microstructure_benchmark.py` therefore separates:

1. an event-level movement head with side labels removed;
2. a LONG-versus-SHORT direction head;
3. a calibration-only movement threshold.

The two-stage result made the bottleneck explicit:

| Metric | Result |
|---|---:|
| Mean movement AUC | 0.8504 |
| Holdout movement AUC | 0.8637 |
| Mean direction AUC | 0.4759 |
| Full-history holdout direction AUC | 0.5697 |
| Calibration-selected trailing-25% direction AUC | 0.6095 |
| Adapted holdout side accuracy | 62.83% |
| Adapted holdout all-action net | -1.8926 pips |
| Calibration-selected trades | 0 |

Recent-window direction adaptation improved side prediction, but the combined
strategy still did not pay spread. The movement and adapted-direction artifacts
remain shadow-only and are not registered as account-eligible contributors.

## Live Model-Gap Latency Fix

The maintenance restart exposed duplicate slow-start worker trees and a stale
publication bottleneck. Before the fix, one cycle:

- spent 99.36 seconds maturing historical outcomes;
- spent 83.06 seconds generating 1,016 live forecasts;
- crossed the 180-second freshness limit;
- rejected all 1,016 forecasts as stale.

The worker now has a nonblocking file lock, writes
`starting:loading_models`, `starting:opening_signal_feed`,
`starting:syncing_registry`, and `starting:opening_ledger` states, and publishes
new inference before maturity work.

The post-fix supervised cycle loaded logistic, HGB, CatBoost, and NGBoost and:

- generated 1,052 forecasts in 104.99 seconds;
- accepted 1,052 and rejected zero;
- matured the backlog afterward in 63.45 seconds;
- retained `account_execution_authorized=false`.

This restores modern-model research contributions without relaxing freshness or
account gates. Startup and inference are still too slow for a subminute live
model, so these artifacts remain contextual shadow inputs.

The follow-up query audit found that maturity left-joined `outcomes` even though
`predictions` transactionally retains only pending rows. Removing both redundant
joins lets the existing `target_epoch` index drive due/expired scans. The
supervisor maturity batch was also reduced from 20,000 to 3,000. Whole-ledger
summary refreshes now run every fifteen minutes instead of every five, smoothing
D: write load without changing forecast publication or promotion gates.

Warm post-query-fix cycle:

- 1,040 forecasts accepted, zero rejected;
- inference: 16.22 seconds;
- maturity: 36.75 seconds;
- total cycle: 54.53 seconds, down from 187.06 seconds.

Publication still occurs before the 36.75-second maintenance phase.

## End-Of-Window Check

The final chronological gate rerun remained `no_stable_positive_gate`.
Its holdout contained 133 actions across six time blocks:

- direction accuracy: 52.63%;
- executable win rate: 12.78%;
- average executable net: -1.711 pips;
- positive holdout blocks: 0 of 6.

The account gate was therefore not relaxed. `practice_007` remained flat with a
42.5483 balance/NAV, no trades, and no pending orders.

At the check, the dedicated 68-instrument microstructure worker reported
321,164 sampled forecasts, 304,504 matured outcomes, 16,660 pending outcomes,
zero errors, and 148.22 ms p95 runtime latency. The restarted modern-model
worker had published 2,104 forecasts with zero freshness rejections under the
3,000-row maturity and 900-second summary settings.

The final process audit also found that terminated Python launcher children
could remain briefly visible through `Win32_Process`. The supervisor treated
those exited records as the oldest active worker and repeatedly restarted the
legacy micro-pattern collector during startup. Process discovery now confirms
each CIM record with `Get-Process` and excludes exited records before freshness
and startup-grace decisions. The collector also has an explicit fifteen-minute
startup grace because reloading its 9.7-million-update state can exceed the
three-minute output freshness window.

## Commands

Build a matured enriched liquid-pair panel while the microstructure collector is
briefly paused or from a stable database copy:

```powershell
& "D:\forex\trad\..venv\Scripts\python.exe" `
  "D:\forex\trad\oanda_second_microstructure_panel.py" `
  --database "D:\forex\trad\data\oanda_training_manager\state\second_forecast_microstructure_v1.sqlite" `
  --output "D:\forex\trad\data\oanda_training_manager\training_sets\prospective_second_microstructure\second_microstructure_liquid16_enriched_latest.parquet" `
  --report "D:\forex\trad\data\oanda_training_manager\reports\modern_model_gap\second_microstructure_liquid16_enriched_panel_latest.json" `
  --instruments "AUD_JPY,AUD_USD,CAD_JPY,CHF_JPY,EUR_CHF,EUR_GBP,EUR_JPY,EUR_USD,GBP_CHF,GBP_JPY,GBP_USD,NZD_JPY,NZD_USD,USD_CAD,USD_CHF,USD_JPY" `
  --minimum-origin-epoch 1785147515 `
  --require-feature activity_5 `
  --require-feature cross_quote_flow_strength_5
```

Run the chronological account-independent gate audit:

```powershell
& "D:\forex\trad\..venv\Scripts\python.exe" `
  "D:\forex\trad\oanda_live_signal_gate_audit.py"
```

Run the two-stage shadow benchmark:

```powershell
& "D:\forex\trad\..venv\Scripts\python.exe" `
  "D:\forex\trad\oanda_two_stage_microstructure_benchmark.py" `
  --panels "D:\forex\trad\data\oanda_training_manager\training_sets\prospective_second_microstructure\second_microstructure_liquid16_enriched_latest.parquet"
```

## Account And Storage Contract

- `-007` remains practice-only and consumes only signals with explicit,
  registered account eligibility.
- Prospective microstructure artifacts are research-only.
- No threshold is promoted from a full-sample or same-window sweep.
- Long-running reads must not block the active SQLite writer.
- Raw rolling data stays on D and can be recollected. Source, configuration,
  model metadata, hashes, reports, and summaries belong in the vault.
