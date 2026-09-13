# Strict M1 path-risk development — 2026-09-09

The single frozen comparison completed in 300.98 seconds. It uses the same immutable three-pair July–September history whose final period was already inspected in the MA experiment. Every result here is retrospective development under a new protocol, including the partition named `test`; no fresh confirmation, predictive edge or promotion is claimed.

The protocol was frozen before scores. Both fixed methods use the same label-valid positive past-volatility training support: unconditional empirical quantiles, and empirical quantiles of the target divided by past 60-minute RMS volatility scaled to the original horizon. Neither method uses validation for refitting or selection. Zero past volatility is withheld explicitly without a floor. All fits were durably written before validation/test scoring.

Origins use 204 real warm rows and the same UTC five-minute grid, strict sessions and chronological 60/20/20 cutoffs as the earlier MA comparison. Targets are exact 15/30/60-minute completed closes. Future paths exclude the origin bar’s earlier high/low and require every subsequent real minute. Midpoint ingestion and original historical arrival remain unproven. Real bid/ask absence withholds the affected path/cost label; there is no spread inference or filling.

| Pair | Horizon m | Common N | Absolute movement pinball empirical / scaled | Coverage empirical / scaled | Width bps empirical / scaled |
| --- | ---: | ---: | ---: | ---: | ---: |
| EUR_USD | 15 | 1728 | 0.4246 / 0.3948 | 81.2% / 82.7% | 4.270 / 4.037 |
| EUR_USD | 30 | 1698 | 0.6070 / 0.5661 | 80.6% / 82.2% | 6.051 / 5.830 |
| EUR_USD | 60 | 1638 | 0.8831 / 0.8303 | 78.1% / 80.1% | 8.878 / 8.414 |
| GBP_USD | 15 | 1838 | 0.4812 / 0.4423 | 79.3% / 78.9% | 4.960 / 4.449 |
| GBP_USD | 30 | 1811 | 0.6822 / 0.6377 | 81.6% / 80.2% | 7.071 / 6.477 |
| GBP_USD | 60 | 1757 | 0.9626 / 0.9229 | 81.3% / 80.0% | 10.549 / 9.252 |
| USD_JPY | 15 | 1872 | 1.1065 / 0.8881 | 72.2% / 80.3% | 5.326 / 8.206 |
| USD_JPY | 30 | 1845 | 1.5585 / 1.3288 | 72.3% / 76.9% | 7.901 / 12.068 |
| USD_JPY | 60 | 1791 | 2.2104 / 2.0509 | 72.6% / 76.6% | 11.567 / 18.141 |

Quantiles are 10/50/90%; lower pinball loss is better. Nominal central coverage is 80%, while the table gives actual development coverage. No calibrated-coverage guarantee follows from the nominal levels.

| Label | Evaluated pair/horizon cells | Scaled lower pinball cells |
| --- | ---: | ---: |
| signed_terminal_bps | 9 | 9 |
| absolute_terminal_bps | 9 | 9 |
| mid_future_range_bps | 9 | 9 |
| realized_path_variation_bps | 9 | 9 |
| long_close_MFE_bps | 9 | 9 |
| long_close_MAE_bps | 9 | 3 |
| short_close_MFE_bps | 9 | 9 |
| short_close_MAE_bps | 9 | 4 |
| long_envelope_MFE_bps | 9 | 9 |
| long_envelope_MAE_bps | 9 | 3 |
| short_envelope_MFE_bps | 9 | 9 |
| short_envelope_MAE_bps | 9 | 4 |

The test-named development cells contain UTC-date counts [9]. The minimum ten-date rule leaves 108 cells without a descriptive interval. Overlapping horizons, repeated origins and correlated pairs are not independent trials; the cell counts above are not significance evidence.

Both long and short close-path excursions use only observed closes plus the initial liquidation spread. Intrabar envelopes additionally use future bid/ask high/low and remain separately labeled. An envelope does not identify barrier order, an executable best price or a fill. Their full side/horizon coverage, upper-tail misses and pinball losses are retained in JSON.

The either-side endpoint-positive event is a separate direction-neutral opportunity label. Only its training-prevalence probability baseline is scored; no direction is inferred from the event. Bid/ask diagnostics contain no slippage, commissions, financing, USD account sizing or broker execution.

No method, threshold, manager rule or horizon was selected from these scores. Any later candidate using this development work requires a separately frozen protocol and future prospective confirmation. Existing paper management, the live joint model and the frozen pilot remain unchanged.
