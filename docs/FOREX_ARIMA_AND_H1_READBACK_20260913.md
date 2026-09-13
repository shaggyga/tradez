# ARIMA and H1 results: retained-record clarification

No audited setup has established dependable profit after costs. That does not mean every historical result was negative or that no H1 model predicted direction above chance. This is a readback of saved reports, not a new fit, metric replay, account query or observation of live performance.

## The additional ARIMA records

The July 11 mandatory-validation programme has three retained full-run generations: `mandatory_model_validation_tier1_202407_202607_20260711`, `_v2` and `_v3` under `fresh_m1_intrahour/reports`. Each contains 87 registered results across 15 pairs. These include shared forecasts, gates, policies, controllers and diagnostics; they are not 87 independent learned models. The generations reuse the same periods and are not independent holdouts. Their final diagnostic period was already available and inspected.

The H1 target in these reports is the next hourly bar's signed return. The corrected v2/v3 records agree on these examples:

| Registered result | Direction, 47,580 forecast rows | Selected trade mean after costs | Reported simulated P/L | Validation |
|---|---:|---:|---:|---|
| ARIMA / H1_ARIMA | 49.95% | -1.7974 pips | -$413.12 over 3,172 trades | Failed |
| SARIMAX | 50.02% | -1.8136 pips | -$388.71 over 3,172 trades | Failed |
| ARIMA(1,2,1) | 50.42% | -1.7371 pips | -$396.15 over 3,172 trades | Failed |
| Trend Regime Classifier | 51.00% | -2.0698 pips | -$484.89 over 3,172 trades | Failed |

ARIMA's reported RMSE is 11.943157 pips versus 11.940490 for predicting no change. Thus this ARIMA result also loses narrowly to the no-change error baseline. These simulated dollar figures use the report's sizing and are not the user's approximately $50 practice account.

There are positive report rows, but none qualifies. `ARIMA_PRE` is before costs: +$95.87 becomes -$413.12 in the corresponding costed ARIMA result. `Edge-Gated ARIMA` has +$3.89 over only 22 selected trades; the randomized selector has +$2.06 over 13. Both fail validation profitability, minimum final sample and top-bucket profitability checks. All 87 rows in v3 report failed validation; the summary reports zero canonical models and zero research leads.

The first full generation reports 131 nonconverged fits. The v2/v3 summaries report zero nonconverged fits and 186 converged optimizer-tracked fits out of 231 total fits. V2 contains 85 failed and two invalidated results (`M15_ATR_032` and `ATR_032`, invalid implementation surfaces); v3 contains 87 failed results. The v1/v3 programme-level `PASS` means completion, not successful financial validation. This supports distinguishing the corrected generations from their predecessor; it does not turn failed financial validation into success.

The separate July 10 short-horizon baseline report did skip nine statsmodels ARIMA/SARIMAX variants because the library was unavailable. It tried custom AR-return and simpler models at 1–30-minute horizons. That failure belongs to that run; it does not describe the later completed H1 programme. The July 12 `ARIMA50` comparison is different again: gradient boosting using 50 autoregressive features, compared with wider feature sets. Some wider configurations beat that feature baseline without qualifying for deployment.

## Were there useful H1 direction results?

- September 7 price-only Ridge: 58/93 correct, or 62.4%, in an initial small sample. Its mean executable result was still -10.308 bps; MAE was 3.007 bps versus 2.498 for no change.
- The saved matched comparison of 2,779 joint and price-only H1 decisions reports 52.07% versus 50.20% direction, but -2.9693 versus -3.1702 bps net. The joint arm improved the comparison without becoming profitable.
- An older H60 cross-currency-factor discovery reports +0.308173 bps selected net and 50.83% direction. Its weak statistic, concentration and period instability prevent treating it as a qualified profitable setup.
- The latest repaired wide-feature study compares against compact gradient boosting, not ARIMA. Direction improved in two of six development cells. Five of six candidate costed selected means were negative; the only positive cell became negative with delayed execution or higher costs.

These populations, targets and executable-cost recipes differ. They cannot establish a current head-to-head ARIMA-versus-live-model ranking. Some H1 forecasts did better than chance, but the evidence has not established a sufficiently large, stable advantage after execution costs.

## Measurement repair and reuse

The old joint and opening price-control wrappers translated a model's completed-minute forecast onto a later live quote and moved its H1 target. The new joint native-target path preserves the original minute close and exact target. That defect is not evidence that it caused every old loss, and it does not invalidate separately aligned archive studies. The independent old opening controls retain an explicit translated-quote label until a separate native migration is reviewed. The new joint path's matched price-only and neutral-news comparisons use the same original target.

Keep the three July 11 generations, their convergence differences and shared-model lineage in the reuse register. A missing July 10 dependency is not a reason to repeat the completed July 11 programme. These records do not justify promoting a model or inventing a new result.

Exact six-file provenance and compact saved results are retained in `revamp_8h_20260912/runtime/collector_parse_recovery_001/ARIMA_MULTIRUN_READBACK_002.json` (SHA256 `10c367957eb803559e1cf6250212cac88a908a810d70c24b9af9938a39cbd2ce`). The earlier broader comparison sources are retained beside it in `BASELINE_H1_EVIDENCE_001.json`. This note supplements the first clarification with the recovered July 11 records; it leaves prior evidence unchanged.
