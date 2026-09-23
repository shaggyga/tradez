# Joint probability bins: retained original outcomes

The original joint-v3 probabilities have Brier **0.252119**, compared with **0.250000** for the fixed 0.5 baseline, on the same **2,182** completed outcomes. This is a descriptive check of uncalibrated probabilities, with no fitted calibration or threshold selection.

The source is the original September 9 assessment: per-ledger transactions observed 08:33:45–08:35:29 UTC. The new summary was computed later from its immutable result files; it is not a new current-state assessment. All 68 registered pairs remain represented, including pairs with no completed outcomes.

| Original p(up) bin | Count | Mean p(up) | Observed up fraction | Mean Brier | Brier minus 0.25 |
|---|---:|---:|---:|---:|---:|
| [0, 0.1) | 0 | — | — | — | — |
| [0.1, 0.2) | 0 | — | — | — | — |
| [0.2, 0.3) | 0 | — | — | — | — |
| [0.3, 0.4) | 9 | 0.382551 | 0.555556 | 0.280069 | 0.030069 |
| [0.4, 0.5) | 1019 | 0.473605 | 0.483808 | 0.250579 | 0.000579 |
| [0.5, 0.6) | 1139 | 0.527028 | 0.464442 | 0.253146 | 0.003146 |
| [0.6, 0.7) | 15 | 0.626299 | 0.533333 | 0.261949 | 0.011949 |
| [0.7, 0.8) | 0 | — | — | — | — |
| [0.8, 0.9) | 0 | — | — | — | — |
| [0.9, 1] | 0 | — | — | — | — |

The overall mean probability was **0.502166** and the observed up fraction was **0.474335**: 1,035 up, 1,134 down and 13 exactly flat outcomes. The original scorer defines the binary event as a strictly positive exact reference-to-target midpoint move; flat counts as not-up (zero). No forecast was abstained in this actual sample, but the implementation includes abstentions and tests their probability scores. Exact p=0, p=0.5 and p=1 are supported without double binning; none occurred here. Empty means remain unavailable rather than zero.

The 2,182 Brier scores, fair-coin scores, pair counts and aggregate mean reconcile exactly to the retained assessment. Original wrapper, report, 68 result hashes and all 24 source/registry bindings were checked before and after reading. No raw input exports, news, account records, database, broker calls, model fitting or economic rescoring were used. Exact retained quote endpoints were read only to verify the binary event. The JSON contains all 680 pair/bin cells and the original disposition counts.

H1 targets overlap, instruments share currencies and inputs, and the per-database observations have distinct clocks. Counts are not independent sample sizes. These bins do not establish a calibration repair, a reliable confidence score, a profitable threshold or a promotion decision. Matched price-only and neutral-news ablations have no probability outputs and are not assigned manufactured probabilities.

Validation: 68 focused tests passed, including direct parity with the pinned pure original scorer for exact flats/tiny positive and negative moves, all bin boundaries, empty 68-pair assessments, duplicate-event rejection, source tampering and caller Decimal precision. The final run passed without warnings and includes invalid/backward completion-clock refusal plus an equal-clock boundary. The earlier 62-case run, exact pre-guard source/test bytes and its actual summary remain preserved. Independent source review is retained separately.

Reproduction for a later completed assessment uses the same helper with explicit `--assessment`, `--wrapper-sha`, `--report-sha` and a fresh `--output-name`. It never invokes the original assessor or reads live ledgers. The helper restricts CLI assessment roots to the original external reassessment directory and creates new outputs exclusively.

Source report SHA: `cd435169e303d1b867787f58a35a6ebbc54932e4358e5c1541a6493597ee88e3`.

Original wrapper SHA: `d7ea2c102e8612c5dea9b2755e1019ccb42ceee6792ee016e0e008165a3dc285`.

This summary JSON SHA: `997ace70fa6be301cba202f83725e98055c67991c945e2f479fab8d8f9faaad8`.

A narrow independent-review finding added finite, ordered actual analysis start/completion clocks. The new actual summary (`actual_calibration_002`) differs from the retained first summary only in the helper source identity and new actual start/end clocks; every probability, bin, denominator, score and source-evidence field is exactly unchanged. The two summaries are repeated views of the same original 2,182 outcomes and must not be added together.
