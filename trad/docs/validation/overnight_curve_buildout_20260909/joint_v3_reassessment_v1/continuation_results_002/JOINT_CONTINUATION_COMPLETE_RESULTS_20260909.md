# Joint study: resumed assessment at12:26–12:28UTC

The unchanged read-only assessor passed all68registered ledgers. Original transactions span **2026-09-09T12:26:42.221272+00:00 to 2026-09-09T12:28:55.118953+00:00**. The cohort now has **2,907completed outcomes**, up from2,182 in the08:33–08:35assessment. The earlier canceled12:15plan remains preserved; this run followed the user’s12:25resumption.

All **2182** earlier completed rows are present; **0** have any changed serialized scored-row value. There are **725** newly completed outcomes. These are cumulative snapshots, not independent samples.

| Original model/baseline | Completed N | Direction hits / directional N | Direction | Positive after spread / directional N | MAE bps / N | Brier / N | Mean net bps per completed |
|---|---:|---:|---:|---:|---:|---:|---:|
| ridge_price_news_v1 | 2907 | 1381/2907 | 47.51% | 642/2907 | 4.9419/2907 | 0.253048/2907 | -4.6798 |
| fair_coin | 2907 | 0/0 | — | 0/0 | —/0 | 0.250000/2907 | 0.0000 |
| zero_move | 2907 | 0/0 | — | 0/0 | 4.7308/2907 | —/0 | 0.0000 |
| no_trade | 2907 | 0/0 | — | 0/0 | —/0 | —/0 | 0.0000 |
| rolling_class_rate | 2907 | 757/1386 | 54.62% | 508/1386 | —/0 | 0.254409/2907 | -0.9667 |
| matched_price_only | 2907 | 1409/2907 | 48.47% | 645/2907 | 4.8518/2907 | —/0 | -4.8120 |
| neutral_news_ablation | 2907 | 1480/2907 | 50.91% | 688/2907 | 4.9224/2907 | —/0 | -4.3541 |

Compared with the earlier snapshot, joint direction moves48.53%→47.51%, MAE4.0317→4.9419bps, mean net−4.7681→−4.6798bps and Brier0.252119→0.253048. The slightly smaller mean trading loss is not evidence of an improved model: the source/model is unchanged and the cumulative evaluation window contains additional outcomes.

On the same2,907decisions, joint minus matched-price-only mean net is+0.132234bps while MAE is0.090047bps worse. Joint minus neutral-news mean net is−0.325725bps and MAE is0.019449bps worse. These retained pre-issue diagnostics are not independent price-v2 forecasts or causal identification of news value.

All68pairs and ten fixed probability bins remain in the companion JSON. There are1,375up,1,518down and14exact-flat outcomes; flat retains the original label0. Mean issued probability is0.501538 and observed up fraction0.472996. Brier0.253048 is worse than fixed0.25. The0.5–0.6bin has1,489rows, mean probability0.526970 and observed up fraction0.453324. No probability calibration was fitted.

There are195original exclusions:103missing later entries and92missing original target quotes. Unscored original completions, reconciliation errors and due-without-outcome-or-exclusion are allzero. The original report retains all seven baseline/model summaries and per-pair references.

Dependence remains explicit:66pairs with scores,62reference15-minute buckets,16UTC hours,2UTC dates and857greedy nonoverlapping H1windows within pairs. None establishes independentN; currency factors and inputs remain shared. Missing/unexpired outcomes stay separate. Net bps are original retained endpoint diagnostics, not actual broker fills, account returns, financing or leverage performance.

No source/model/registry/runtime changes, new forecasts, fitting or broker requests occurred. The accepted68-test calibration implementation was reused without changes; no tests were rerun for this additional application. Raw logical exports and full per-pair scored-row files remain machine-local.

Original report SHA: `119164bcf43eadbb841b821a9479a51c5305b43a8121477a3016c47d2750c77e`.
Calibration SHA: `01242a311feadb300ecfd1d60aae32891a6e82ed65693610b00cdd1660d9d4ab`.
Compact result SHA: `9e5860c41cce9c37daa94a55ee8b87927a823c52b0429cf11fc30efb43376c07`.
