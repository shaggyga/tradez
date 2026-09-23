# Rolling technical model comparison — September 15, 2026

The matched historical comparison is complete: **32 learned models and 20 controls across all 68 pairs**, retaining every original assessment minute. None of the tested thresholded nonoverlap policies was positive in both assessment periods, even at quoted spread alone. Isolated positive cases remain worth preserving, particularly compact Ridge at one hour; a dependable trading edge is not established.

This continues the [completed historical data package](FOREX_TRAINING_WINDOWS_20260915.md). Current source, results and acceptance receipts belong to this dated experiment. Live feature/model inputs and trading settings are unchanged.

## Question and material difference

Do the shared rolling OHLC/activity/spread inputs and correctly timed peer inputs improve expected-return forecasts and after-cost decisions compared with simple controls? Does a compact representation retain or improve useful information, and does nonlinear boosting add useful conditional structure over additive Ridge?

Boosting, Ridge, peer comparisons and conditional direction/magnitude/cost heads already exist in the project. The earlier results are mapped in the preceding data guide. This run changes the input contract, current-clock alignment, missingness handling and fixed-target executable-cost proxies. It does not restart the model history or present those model families as new discoveries.

## Fixed comparison

| Component | Declared choice |
|---|---|
| Population | 68 pairs; original minute clocks from the accepted July 13–September 7 dataset |
| Fit period | July 13 inclusive–August 24 exclusive; supervised terminal candle END strictly before cutoff |
| Validation | August 24–31 |
| Later development comparison | August 31–September 7; already examined dates, not untouched confirmation |
| Horizons | 5, 15, 30 and 60 minutes, same original terminal clocks |
| Input groups | Compact 38 local, compact 38 plus 12 peers, full 216 local, full 228 |
| Learned models | Ridge alpha 1000; HGB 120 iterations, 15 leaves, minimum 200 samples/leaf, L2=10, learning rate0.05, 128 bins, fixed seed, no early stopping |
| Supervised training rows | Real TRAIN origins on original UTC 15-minute clocks, then horizon-specific mature endpoint-label eligibility |
| Assessment rows | Every original validation/later-test row; no future-validity filter on issuance |
| Controls | No change/no trade; pair TRAIN mean and retained directional prevalence; exact past-horizon momentum/reversal; aligned conditional-OLS ARIMA(1,1,0) |
| Entry diagnostic | Forecast sign; threshold abs(expected bps) > current spread bps +1, fixed before fitting |
| Costs | Observed chosen-side bid/ask endpoint net, with 0/1/2 bps additional round-trip cost; decisions unchanged between scenarios |
| Delay check | Same forecast, original decision and terminal clock; enter at exact next-minute close; normalize delayed net by delayed-entry midpoint |

The 32 learned fits and 20 horizon-specific controls are descriptive development comparisons, not independent discoveries or a parameter search. Neither assessment block tunes transformations, thresholds, stopping or model parameters. Small positive cases remain visible; their value depends on costs, support, stability and concentration rather than a 50% direction threshold.

## Inputs and missingness

The compact 38 list is fixed in `oanda_rolling_model_design_v1.py`. It spans all nine local feature families with at most 200 bars of support and retains none of the 42 previously flagged near-redundant pairs together. This is not proof of statistical independence. The twelve peer columns deliberately include four base-minus-quote differences, which supply precomputed tree inputs and change Ridge's effective regularization; they are not twelve independent information sources.

Per-pair means and variances use all full-TRAIN feature rows before clock sampling or supervised-label selection. A pair/field requires at least 20 finite values and actual nonzero range/variance. Unsupported pair/fields remain missing in every split. Floating-point overflow fails rather than silently becoming missing. Standardized values use float32 for bounded learning storage; the accepted raw features remain unchanged float64.

Ridge receives standardized values with zero imputation, one explicit missing indicator per selected field and a 68-column pair one-hot context. HGB receives native missing values and an explicitly categorical pair ID. Pair identity is the only deliberately added causal metadata context; future labels, outcome states, split identity and diagnostic values do not enter the feature matrix. Compare feature groups within each learner before attributing differences between learners purely to interactions.

Preparation retained 1,094,794 rows: 183,540 original-clock training samples and all 911,254 assessment origins. Training-only scaling uses the full 2,734,524 training-origin population. Model-specific supervised counts differ by horizon and are recorded with exact pair/clock and target hashes; every learned group at the same horizon uses the same fit population.

## Quote and ARIMA reconstruction

The auxiliary quote panel re-read the exact accepted source ranges and matched source identities before attaching current bid/ask/midpoint and current spread. Future outcome net costs cannot supply the origin-known spread gate. The same panel preserves delayed-entry labels separately from current metadata and model inputs.

Consume `data/rolling_model_quote_panel_20260915_v2`. Its first version admitted a small pre-start context into ARIMA parameter fitting. The incomplete first run was stopped, explicitly invalidated and retained with its original source and manifest. Version 2 restricts fit inputs to the exact declared training period while retaining pre-start context for causal prediction calculations. It covers all 3,645,778 origins; all 68 ARIMA fits and all quote/receipt hashes verified. Existing endpoint parity checks covered 71,487,101 numerical cells.

ARIMA here means no-drift conditional ordinary least squares on adjacent log-price increments, with stationarity clipping recorded per pair, followed by the analytic AR(1) sum at each requested horizon. Its fit uses contiguous M1 TRAIN triplets, not the sampled supervised horizon labels used by Ridge/HGB and the pair-mean control. It uses the current known increment, preserves gaps and suppresses pre-fit forecasts. This is a freshly aligned comparator, not an exact recreation of older statsmodels maximum-likelihood or multi-order ARIMA results. Its sign is a per-pair momentum/reversal transformation of the current increment; phi chiefly shapes decay and magnitude.

## Scoring interpretation

All original assessment rows receive learned predictions. For matched model/control comparisons, score rows where the current ARIMA input, exact past-horizon midpoint and trained pair prior are available. This common coverage mask is based on origin-known information and affects scoring only. Separate own-coverage error scores show what the comparator restriction excludes.

Future endpoint validity, strict path support and split maturity create three separately reported scoring cohorts: full endpoints, shared complete paths and added endpoint-only cases. None changes the original decisions. Fixed-horizon per-pair holding reservations are assigned before future masks, so a missing or excluded outcome cannot release a slot. Reservations reset independently at each assessment split; this is not a continuous account simulation. Across pairs, common currency exposures remain correlated.

Reported net sums describe equal unit-notional endpoint proxies, not dollars, compounding, account profit or broker fills. The delay check preserves the original gate and terminal clock. Actual spread, extra costs, exact-flat prices, withheld controls, absent outcomes, day/pair concentration and leave-best-group-out results remain visible. Positive descriptive means alone do not establish deployable profit.

## Recreation

Use the existing `timeseries312` Python runtime from the project directory. Output directories must be new.

```powershell
python -B tools/prepare_rolling_model_comparison_v1.py --base data/rolling_technical_training_20260915_v1 --overlay data/rolling_technical_endpoints_20260915_v1 --output data/rolling_model_inputs_20260915_v1
python -B tools/build_rolling_comparison_quotes_v1.py --base data/rolling_technical_training_20260915_v1 --endpoints data/rolling_technical_endpoints_20260915_v1 --output data/rolling_model_quote_panel_20260915_v2 --workers 3
python -B tools/run_rolling_model_comparison_v1.py --prepared data/rolling_model_inputs_20260915_v1 --quotes data/rolling_model_quote_panel_20260915_v2 --output data/rolling_model_comparison_20260915_v1
```

The runner keeps source/input bindings, training population hashes, fixed recipes, saved estimators, every assessment prediction, per-cell metrics and bounded output checks. Estimator reload compares the first 8,192 predictions exactly and an additional representative from every pair within a recorded numerical tolerance. Prediction Parquet readback verifies the complete population and finite float64 bits. The focused [test receipt](validation/rolling_model_comparison_20260915/TESTS.json) records 55 tests plus 19 subtests passed.

## Completed results and useful leads

All 64 learned model/period combinations had worse mean absolute error than no change on the matched full-endpoint cohort. Three later H1 boosting combinations improved RMSE, so not every error statistic deteriorated. Called nonflat direction accuracy ranged from 49.40–52.39% in validation and 49.43–51.50% later. **25/32 validation and 28/32 later assessments exceeded 50% direction accuracy.** A directional percentage alone does not establish trade value.

At quoted bid/ask plus 1 bp, 3 of the 64 learned model/period combinations had positive mean nonoverlapping endpoint returns; 61 were negative. None was positive in both periods. All eight quoted-spread-positive cases are retained below. Figures are mean bps per scored decision, not account returns.

| Model and horizon | Positive period | Scored decisions | Quoted spread only | Plus 1 bp | Plus 2 bps | Other period, plus 1 bp |
|---|---|---:|---:|---:|---:|---:|
| Ridge, compact 38, 5m | Validation | 2 | +7.682 | +6.682 | +5.682 | −6.414 |
| Ridge, compact 50, 5m | Validation | 5 | +4.486 | +3.486 | +2.486 | −8.804 |
| Ridge, compact 38, 15m | Validation | 12 | +0.227 | −0.773 | −1.773 | −4.092 |
| Ridge, local 216, 60m | Validation | 221 | +0.352 | −0.648 | −1.648 | −3.657 |
| Ridge, combined 228, 60m | Validation | 230 | +0.471 | −0.529 | −1.529 | −3.186 |
| Boosting, compact 50, 30m | Later development | 760 | +0.891 | −0.109 | −1.109 | −2.973 |
| Boosting, combined 228, 30m | Later development | 783 | +0.026 | −0.974 | −1.974 | −3.564 |
| Ridge, compact 38, 60m | Later development | 46 | +2.984 | +1.984 | +0.984 | −1.297 |

**Compact Ridge H1 is a research lead with strong concentration.** Its 46 scored decisions span 35 pairs and four UTC days. Delayed entry stays positive at +2.713 bps with the same 46 scored outcomes. However, September 4 supplies more than the entire net gain: removing that day gives −6.744 bps; removing USD/JPY gives −0.225 bps. Only one of the four days has positive aggregate net. Earlier validation is negative across 26 scored decisions. Retain the saved model and events for testing on different periods; do not select its successful date or pairs as a newly validated rule.

**Compact-plus-peer boosting at 30m is cost-sensitive and unstable.** The later period gives +0.891 bps at quoted spread alone and −0.109 with the extra 1 bp. Delayed entry gives +0.096 bps, but covers 757 rather than 760 decisions. Removing the best day or pair gives −0.747 or −0.430 bps at the primary cost. The earlier period is clearly negative. The small positive delay result remains visible without being treated as confirmation.

The two positive 5m validation cases have only two and five decisions; both become negative under delayed entry and lose later. The other quoted-only positives do not survive the extra 1 bp. Shared strict-path results contain the same eight quoted-positive cases. In the additional endpoint-only cohort, later compact-38 boosting H1 gives +1.418 bps at the primary cost over eight decisions, and local-216 boosting H1 gives +0.328 at quoted cost but −0.672 with 1 bp extra over 14 decisions. No cell is quoted-cost positive in both periods in any cohort. These future path cohorts cannot define an entry filter.

Peer additions improved MAE in only 4 of 32 matched learner/horizon/period comparisons: 0/16 for Ridge and 4/16 for boosting. They improved policy mean in 17/32, but those policies can choose different trades. Increasing local feature width improved MAE in 3/16 comparisons; increasing combined width improved it in 4/16. The full feature space is therefore not a demonstrated upgrade. This is a fixed representation comparison, not a causal importance ranking or exhaustive interaction search.

Conditional-OLS ARIMA improves MAE over no change by approximately 0.00025–0.00055 bps, but makes zero spread-plus-1-bp threshold decisions. An absent policy mean is not a trading loss. Exact past-horizon momentum and reversal remain negative after primary costs in both periods. Neither tiny ARIMA error improvements nor a reversal hit rate above 50% establishes a useful entry rule.

The [complete tables](validation/rolling_model_comparison_20260915/summary/README.md) show all 104 model/control/period rows and 64 feature-arm contrasts. The [JSON summary](validation/rolling_model_comparison_20260915/summary/COMPARISON_SUMMARY.json) retains all three cohorts, costs, delay coverage, own-coverage errors, concentration and positive-period flags. No low-support or losing cell is hidden.

## Acceptance and retained assets

The run completed in 1,011.641 seconds and wrote 368,818,397 bytes, including all original forecast rows. Horizon-eligible supervised counts are 179,211 / 178,315 / 177,737 / 176,566 for 5/15/30/60m. Identical fit population and target hashes apply to every learned arm at the same horizon. The [normalizer receipt](validation/rolling_model_comparison_20260915/NORMALIZERS.json) and compact NPZ preserve all 68 × 228 training parameters and support flags for inference without refitting.

Core tests passed 55 tests plus 19 subtests; the independent auditor and summary add 7 and 10 passing tests. The [independent audit](validation/rolling_model_comparison_20260915/INDEPENDENT_AUDIT.json) verified all 52 forecast artifacts and 47,385,208 original pair/clock key rows, fit selections, decision hashes, cohort counts and mean costs, including 14,277 scalar cost cases. It recreated all 20 controls, replayed each of the 32 models on the first 8,192 predictions exactly, and checked a later-period representative from every pair. All 272 pair-prior parameter records were independently matched. Existing endpoint receipts stayed unchanged. This does not independently refit each model, replay every learned prediction or recompute every concentration statistic.

The [acceptance receipt](validation/rolling_model_comparison_20260915/ACCEPTANCE.json) binds code, input metadata, saved models, forecasts, metrics, normalizers, tests and audit evidence. The dated vault supplement retains estimators, compact evidence and source recreation dependencies. Raw historical prices and forecast Parquet remain in the project. Its [publication receipt](validation/rolling_model_comparison_20260915/VAULT_PUBLICATION.json) records copy/hash and ZIP readback checks. Local OneDrive publication does not verify cloud synchronization.

Additional reproduction commands use new output paths:

```powershell
python -B tools/audit_rolling_model_comparison_v1.py --comparison data/rolling_model_comparison_20260915_v1 --output docs/validation/rolling_model_comparison_20260915/INDEPENDENT_AUDIT.json
python -B tools/summarize_rolling_model_comparison_v1.py --comparison data/rolling_model_comparison_20260915_v1 --output docs/validation/rolling_model_comparison_20260915/summary
```

## Next research step

Reuse the existing direction, movement and cost specialist implementations on this accepted rolling input contract, using training-only, time-ordered, horizon-purged out-of-fold specialist predictions for any combining model. This tests a different objective from unconditional expected return and should not repeat an unqualified wider-feature sweep. Keep compact Ridge H1, compact-plus-peer boosting 30m, no change and the aligned controls as explicit comparators. Diagnose retained positive events and their failures, then assess fixed candidates on additional separate periods and new forward observations. Do not turn successful dates, pairs or future path cohorts into retrospectively validated gates.

Feature-family contribution, archive expansion, specialist combination and independent confirmation remain open. News enrichment, curve/position management and older operational gaps stay in the [pending queue](../FOREX_PENDING_IMPROVEMENTS.md). This experiment completes the matched-comparison step, not the entire revamp.
