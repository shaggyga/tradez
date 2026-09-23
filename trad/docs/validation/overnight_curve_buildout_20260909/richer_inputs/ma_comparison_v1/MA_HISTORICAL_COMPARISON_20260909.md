# Corrected MA versus compact technical features — 2026-09-09

**The richer features did not improve magnitude error in this declared test.** MA643 and combined667 had higher test MAE than compact24 and zero change in all nine pair/horizon cells. Compact24 was also slightly worse than zero change in all nine. This result supports retaining the simpler baseline for further comparisons; it does not prove that these feature families can never help under a different separately declared design. No follow-up tuning or test rerun was performed.

The single retrospective run completed 2026-09-09T06:05:19.969625+00:00–2026-09-09T06:06:05.549183+00:00 in 45.58s. It used the immutable July9–September9 archived M1 captures. All nine cells met the frozen minimum sizes. Each of27 learned arms selected alpha1000, the largest predeclared candidate, using validation MAE before final-test scoring. Selection on the upper grid boundary is recorded; the grid was not expanded after seeing test results.

The three arms share origins, targets, time cutoffs and purges. MA has643 raw columns, compact24 has17 constant training columns (seven varying columns on this contiguous subset), and combined667 has17 constants. A raw feature count is not a count of independent information. All scalers and coefficients were fit only to the initial training period; no old weights, imputation, calibration or probability model was used.

| Pair / horizon min | Train / validation / test | Zero MAE | Momentum MAE | Compact MAE | MA MAE | Combined MAE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| EUR_USD/15 | 5631 / 1917 / 1728 | 1.8526 | 2.1428 | 1.8567 | 1.9317 | 1.9293 |
| EUR_USD/30 | 5544 / 1887 / 1698 | 2.6078 | 3.3988 | 2.6086 | 2.7801 | 2.7855 |
| EUR_USD/60 | 5370 / 1827 / 1638 | 3.5857 | 5.6286 | 3.6190 | 3.9319 | 3.9737 |
| GBP_USD/15 | 5981 / 2006 / 1838 | 2.1136 | 2.4394 | 2.1216 | 2.1780 | 2.1792 |
| GBP_USD/30 | 5903 / 1979 / 1811 | 2.9850 | 3.8233 | 2.9919 | 3.0615 | 3.0618 |
| GBP_USD/60 | 5747 / 1925 / 1757 | 4.1256 | 6.2850 | 4.1655 | 4.3179 | 4.3167 |
| USD_JPY/15 | 6001 / 2112 / 1872 | 4.0174 | 4.5857 | 4.0859 | 4.1584 | 4.1630 |
| USD_JPY/30 | 5923 / 2085 / 1845 | 5.6716 | 7.1731 | 5.7910 | 5.9672 | 5.9845 |
| USD_JPY/60 | 5767 / 2031 / 1791 | 8.1518 | 11.8640 | 8.2175 | 8.5649 | 8.5863 |

All MAEs are basis points relative to the reference archived midpoint close. Forecast targets are exact15/30/60-minute future closes in the same contiguous session. The zero baseline has no directional prediction or position.

| Pair / horizon min | Compact direction | MA direction | Combined direction | Compact cost bps | MA cost bps | Combined cost bps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| EUR_USD/15 | 837/1685 | 845/1685 | 848/1685 | -1.4003 (1728) | -1.3794 (1728) | -1.3843 (1728) |
| EUR_USD/30 | 816/1669 | 804/1669 | 798/1669 | -1.4774 (1698) | -1.6768 (1698) | -1.6824 (1698) |
| EUR_USD/60 | 725/1615 | 757/1615 | 752/1615 | -1.8109 (1638) | -1.7641 (1638) | -1.6892 (1638) |
| GBP_USD/15 | 854/1798 | 911/1798 | 915/1798 | -1.4555 (1838) | -1.3281 (1838) | -1.3219 (1838) |
| GBP_USD/30 | 843/1776 | 908/1776 | 909/1776 | -1.3341 (1811) | -1.4300 (1811) | -1.3838 (1811) |
| GBP_USD/60 | 794/1741 | 852/1741 | 840/1741 | -1.8195 (1757) | -1.5506 (1757) | -1.6130 (1757) |
| USD_JPY/15 | 923/1849 | 938/1849 | 949/1849 | -0.9652 (1872) | -0.8323 (1872) | -0.8403 (1872) |
| USD_JPY/30 | 866/1828 | 908/1828 | 911/1828 | -1.0019 (1845) | -0.7616 (1845) | -0.7159 (1845) |
| USD_JPY/60 | 845/1783 | 931/1783 | 940/1783 | -1.3694 (1791) | +0.0238 (1791) | +0.2077 (1791) |

Direction gives correct/non-neutral predictions with nonzero outcomes. Cost gives mean original-side bid/ask endpoint net bps with its separate denominator in parentheses. It is a hypothetical candle-close diagnostic, not a fill, tradeable target, financing/slippage-adjusted profit or dollar return.

Most learned-arm cost cells are negative. USD/JPY60m has MA+0.0238bps and combined+0.2077bps, while the same-horizon momentum diagnostic is+0.5555bps; neither richer arm improves MAE there. These cells were not selected as trading candidates.

All test cells cover **nine UTCdates**, below the frozen ten-date threshold. Therefore every date-block confidence interval is unavailable. The complete per-date MAE, paired directional and cost differences remain retained, with no across-currency pooling. Nearby origins, horizons and shared-currency pairs are dependent; thousands of rows are not thousands of independent trials. There is no Brier score because no probability model was fit.

| Pair | Archived rows | Segments / qualifying | Warmed rows | Five-minute origins | Validation cutoff UTC | Test cutoff UTC |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| EUR_USD | 61910 | 299 / 50 | 47168 | 9423 | 2026-08-15T16:57:00+00:00 | 2026-08-27T23:24:00+00:00 |
| GBP_USD | 62026 | 180 / 44 | 49878 | 9957 | 2026-08-15T17:00:00+00:00 | 2026-08-27T23:25:00+00:00 |
| USD_JPY | 61976 | 207 / 44 | 50669 | 10117 | 2026-08-15T17:11:00+00:00 | 2026-08-27T23:31:00+00:00 |

The complete machine report retains warm-up/grid/target-gap and split-purge counts per horizon. Every adjacent missing minute resets the session and EMA initialization; no weekend or provider gap is filled. Qualifying sessions require204 actual completed rows. Existing archived midpoint and bid/ask values were observed only at the new capture clock; original ingestion conventions and historical first availability remain unknown. This is retrospective feature/model engineering, not a reconstruction of prospectively issued predictions.

Validation:24 focused tests passed, including exact prefix invariance, timestamp/identity rejection, shared splits and strict purges, independent standardized Ridge arithmetic, missing-cost separation, fixed date resampling and flush/fsync before final-test evaluation. The independent source review found no remaining blocker. Initial21-case and intermediate23-case passes remain preserved.

The pre-evaluation design, durable selected coefficients/scalers, full per-origin final predictions, per-date comparisons, dependency versions and exact input/source hashes are retained outside the live project. All orders, promotion, account/proof and execution authority remain false. No running study, source registration, dashboard, broker process or vault was changed.
