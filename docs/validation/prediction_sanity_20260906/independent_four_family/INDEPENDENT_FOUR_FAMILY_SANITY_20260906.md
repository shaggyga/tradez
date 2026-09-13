# Independent sanity check of saved EURUSD four-family predictions

The previously reported direction percentages are arithmetically correct. Independent recomputation starts from each retained `forecast_json` and its outcome quotes, verifies all 1,628 payload hashes, and uses exact decimal price arithmetic. It does not import the prior scorer or access a runtime database. Every compared prior metric agrees within 1e-9; decimal and floating-point arithmetic assign the same direction label to all 368 paired outcomes. Stored theoretical quote returns also match.

| Model | Correct direction | Accuracy | Brier, lower is better | Mean archived quote-net pips |
|---|---:|---:|---:|---:|
| Graph | 156/368 | 42.39% | 0.36150 | -1.930 |
| Tabular | 175/368 | 47.55% | 0.27383 | -2.328 |
| State space | 176/368 | 47.83% | 0.38676 | -1.759 |
| Ridge | 163/368 | 44.29% | 0.37835 | -1.673 |

These are 368 matched archived EURUSD endpoints from six UTC days, with 172 up moves, 195 down moves and one flat. A constant sell direction would describe 195/368 correct (52.99%); constant 50% probability has Brier 0.25. The zero-move magnitude baseline has MAE 3.934 pips. All four models have worse Brier and magnitude errors than these simple baselines on this subset, and their stored quote-net averages are negative. These comparisons do not authorize selecting a constant direction or reversing the signals.

“None accurate” is too broad. Each model correctly predicted 156–176 of the 368 outcomes. The supportable statement is that none of these four showed a useful advantage on this particular archived comparison. The results do not establish that every project model fails, or that each algorithm will underperform across all periods.

All four models supplied 407 original forecasts; no family is missing from this selected forecast population. Missing outcome rows number 21 for graph, 21 for tabular, 21 for state space and 22 for ridge. Matching excludes 22 reference epochs with at least one missing outcome and 17 with different endpoint clocks or quotes, leaving 368. Missing models elsewhere in the project require the separate broader inventory.

Direction and probability are distinct outputs for the tabular model: its expected-return direction differs from the majority probability direction on 74 of these paired forecasts. The reported 47.55% preserves its emitted direction. Thresholding probability at 0.5 gives 161/368, or 43.75%, so changing that interpretation does not rescue this result. The other three have no such disagreements. There are no exact probability ties or zero expected-return forecasts. The one flat is a direction miss and a non-up Brier label; removing it leaves all four accuracies below 50%.

The archive cannot establish executable prediction performance. All reference quotes predate forecast recording, with median lag 84.63 seconds and maximum 10,695.93 seconds across the full 1,628 forecasts. The paired set mixes 353 canonical and 15 legacy endpoint epochs. The retained clocks and shifted targets are reviewed separately in `independent_timing_diagnostic.json`. Overlapping forecasts and six days of data do not supply 368 independent trials.

The models are not all moving together. Graph and ridge agree in direction on 269/368 (73.10%) and have probability correlation 0.617. Other direction agreements range from 40.22% to 48.10%; their probability correlations range from -0.212 to 0.083. At least one model is correct on 337/368 epochs, but knowing afterward which one was correct is not an available ensemble rule. The pairwise statistics are descriptive and no ensemble was fitted or promoted.

Evidence is preserved in `independent_four_family_results.json`, `independently_paired_endpoints.json`, `independently_excluded_references.json`, and the two standalone scripts in this directory. Production source, configuration, databases and runtime were not changed.
