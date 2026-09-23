# Earlier rolling periods and H1 feature-family contribution — September 15, 2026

This continues the [completed specialist comparison](FOREX_ROLLING_SPECIALISTS_20260915.md). That study found improved terminal-movement and execution-cost estimates, but no mean-return policy positive in both assessment weeks. The next question is whether those component gains recur in earlier periods, and which technical families contribute when the others remain present.

## Fixed scope before fitting

Recorded September 15 at 22:27 UTC, while the historical datasets were building and before the new model fits. Windows were selected by chronology and archive coverage, without reading their returns to select dates. These dates have appeared in older research: this is development replication, not untouched confirmation.

| Window | All origins, UTC, half-open | TRAIN | Validation | Later development |
|---|---|---|---|---|
| A | March 23–May 18 | March 23–May 4 | May 4–11 | May 11–18 |
| B | May 18–July 13 | May 18–June 29 | June 29–July 6 | July 6–13 |

All 68 instruments have archived rows in all eight weeks of both windows. That does not establish continuous histories or complete numeric features. Coverage preflight counted 3,668,725 original A rows and 3,656,968 B rows. Sparse periods, unsupported fields and absent future endpoints remain explicit; there is no gap filling. The exact coverage receipt is `validation/rolling_period_replication_20260915/COVERAGE.json`, SHA256 `fe9616607613725144294da725ba5888c64b957f69a6a4669e4bcd35a4f89b0b`.

The existing source-bound 216-local-plus-12-peer rolling dataset builder, endpoint labels, quote panel and 228-column preparation are reused unchanged. The specialist input adapter V2 derives its five prefix cutoffs from each window, preserving V1 numerical preparation. All original TRAIN minutes supply prefix normalization before UTC15-minute sampling; all assessment minutes receive forecasts before future label masks. Targets must end strictly before their training or OOF cutoff.

Each window repeats compact38 and compact50 at 30m and 60m. Four chronological OOF weeks follow an initial two-week training prefix. Five mean estimates are retained: direct signed-return HGB, raw conditional mixture, OOF-calibrated mixture, direct-only Ridge recalibration and direct-plus-context HGB. The richer specialist Ridge/context combiners were weaker in the preceding study; they are preserved there rather than refitted here. There are 20 six-head bundles, eight meta fits and four calibrators per window. No recipe, threshold or model is chosen from these new assessment results.

The same windows also refit the original compact38/compact50 Ridge and HGB recipes at both horizons, plus five controls per horizon: no change, pair TRAIN mean, conditional-OLS ARIMA(1,1,0), exact past-horizon momentum and its reversal. These original comparators do not learn from the two current quote-wing inputs added to specialists. Saved models trained on later dates are never applied to earlier history as controls.

## H1 family contribution

The full compact50 H1 fit and forecasts are reused exactly as the anchor. Seven additional final six-head bundles mask the following technical slots during both training and prediction:

| Variant | Technical fields retained |
|---|---:|
| Full compact50 | 50 |
| Remove peer currency factors | 38 |
| Remove returns and path shape | 40 |
| Remove trend and oscillators | 42 |
| Remove candle range and volatility | 41 |
| Remove activity and spread history | 43 |
| Remove calendar | 46 |
| Pair identity and current costs only | 0 |

All variants keep two known asymmetric entry costs and categorical pair identity, using the same 53-slot matrix. Removed technical values and their missingness become uniformly NaN. This removes their information, but is not a claim of bit-for-bit equivalence to deleting columns. Correlated remaining fields can carry similar information, so differences measure conditional contribution rather than causal or independent importance.

Family results cover direct signed return, raw mixture, positive probability, conditional magnitudes, expected absolute terminal return and separate exit costs. Family experiments do not include 30m, calibration or contextual meta models. In particular, a dropped field cannot re-enter through a meta model's market context. Family-induced changes to selected trade sets are distinct from forecast-error comparisons on identical original rows.

## Evaluation and completion

Reuse the unchanged primary current-spread-plus-1bp gate and separately reported predicted-side-cost-plus-1bp gate. Evaluate executable bid/ask terminal return with zero, one and two extra round-trip bps, and exact next-minute entry with unchanged decisions and terminal clocks. Fixed per-pair horizon reservations prevent overlapping decisions within a pair; currency overlap across pairs remains dependent exposure. Report both weeks, all three descriptive endpoint/path cohorts, sample size, decision clocks, active days, pair/currency concentration and strongest-day removal. Future cohorts are diagnostic, never entry filters. No 50% accuracy threshold filters leads.

Component comparisons use TRAIN mean and median references and side-correct current quote-wing persistence on identical scoreable origins. Expected absolute movement means absolute terminal return, not MFE, MAE, barrier sequence or time to peak. Prediction errors and endpoint policy means are not broker fills or account returns.

Completion requires both full fixed grids, intact source/input hashes, complete forecast-key and gate-digest checks, chronological fit/OOF and prefix-normalizer verification, saved-model replays, cost arithmetic checks, and a two-window summary. Partial runs are not interpreted as completed evidence. A 32 GiB drive reserve is enforced. Live collection, trading, news and position management settings are outside this experiment.

## Results

Both studies and their independent audits are complete. They cover **1,827,574 original assessment origins** across the two separate historical windows. Each completed window retains 27 six-head bundles (162 head estimators), eight meta models, four calibrators, eight original-recipe learned comparators and ten controls. The family anchor reuses the full H1 model; it is not an additional independent experiment. The complete summary contains 160 replication rows, 128 family rows and 72 comparator rows, each preserving three descriptive cohorts. These counts are overlapping evaluations, not independent statistical samples.

**No full-endpoint specialist or family policy was positive in both assessment weeks at quoted executable costs plus 1 bp, under either gate.** None was positive in all four new weeks even at quoted costs alone. This does not mean every period lost, nothing predicts movement, or no sub-50% win-rate strategy can have value. All positive cases remain recorded with their unsuccessful periods, support, cost sensitivity and concentration.

### Useful components and remaining weakness

| Component | Separate-window evidence | Meaning |
|---|---|---|
| Expected absolute terminal move | RMSE beats the TRAIN pair mean in all eight context/week cells in A and all eight in B. MAE beats TRAIN medians in 0/8 A and 6/8 B. | Movement-size information recurs, but improvement depends on the loss measure. This is not path, MFE/MAE or exit-management evidence. |
| Long/short exit costs | Beat TRAIN references and current quote-wing persistence in all 16 full-endpoint cells per window (48/48 including the diagnostic cohorts). | Retain these specialists as useful cost estimates. Accurate costs alone do not identify profitable trades. |
| Calibrated positive probability | Brier/log-loss beats TRAIN pair priors in 6/8 A and 8/8 B cells. Raw probabilities win 6/8 and 4/8 respectively. | Calibration helps in some periods, especially later B; probability quality is distinct from expected executable return. |
| Signed mean return | MAE/RMSE beats no-change in 6/40 and 4/40 A cells, and 1/40 and 8/40 B cells; the same counts beat contemporaneous ARIMA. | Conditional signed-return estimation and selection remain the main weakness. Direction exceeds 50% in 32/40 A and 34/40 B cells, so a 50% threshold would be misleading. |

The mean-error and direction counts use one copy of each context/variant/week, rather than treating the two gates as independent evidence. ARIMA is often inactive under the gate; inactivity is not a losing trade. In every active primary full-context cell (36/36 in each window), the magnitude of selected-return error exceeds the magnitude of exit-cost error. These are absolute mean errors, not a claim that every return forecast is optimistic. This points toward return selection as the larger model error in these evaluations; it is not proof of causality or a forecast of future trading results.

### Positive results retained for research

All numbers below are average bps per scored nonoverlapping decision, not account returns. The +1 and +2 columns deduct extra round-trip costs in addition to executable bid/ask endpoints.

| Fixed setup and assessment week | Quoted / +1 / +2 bps | Support and checks | Other periods |
|---|---|---|---|
| H1 compact50 raw mixture with trend/oscillator inputs removed, July 6–13 | +3.5126 / +2.5126 / +1.5126 | 72 decisions, 36 clocks, five days, 37 pairs; one-minute delayed +1 mean +1.7970; remove best day +0.6554; remove best pair +1.6739 | +1 means in the other three weeks: -4.6949, -3.7883, -4.8837. Retain as an isolated lead, not a selected trading model. |
| Full H1 compact50 raw mixture, July 6–13 | +1.7033 / +0.7033 / -0.2967 | 67 decisions; delayed +1 mean -0.3305; remove best day -0.7475 | Does not establish recurring profitability. |
| H1 compact38 direct-only Ridge recalibration, May 4–11 and May 11–18 | Quoted +1.5337 / +0.3050 across the two weeks; +1 gives +0.5337 / -0.6950 | 52/8 decisions; both delayed +1 results negative | The only new specialist both-week quoted-positive case loses that property with 1 bp extra costs. |
| Original compact50 Ridge 30m comparator, May 4–11 | +3.2845 / +2.2845 / +1.2845 | 73 decisions; 36.99% wins; delayed +1 mean +1.3447; remove best day -7.5244 | Following three weekly +1 means -9.7430, -3.588, -5.266. This explicitly preserves a profitable period below 50% wins. |

Tiny positive cases in future-defined additional-endpoint cohorts, including two/three and four/four decision cases, are also retained in the summary. Their future membership cannot be used as entry eligibility. The older August 24–September 7 reference remains separate and is not pooled with these dates or called new confirmation.

### What the feature families contribute

- Removing activity/spread history worsens signed-mean MAE in all eight mean-arm/week comparisons and expected-absolute MAE/RMSE in all four weeks.
- Removing trend/oscillator inputs worsens expected-absolute MAE/RMSE in all four weeks, despite the isolated profitable policy result above. Do not universally discard this family on that result.
- Removing calendar inputs worsens both exit-cost heads in all four weeks; their MAE deteriorates by about 0.19–0.24 bp.
- Other signed-return family effects are mixed. Per-policy mean changes can reflect different selected decisions and are not paired causal effects.
- Pair identity/current costs alone produces no scored primary-gate trades. The secondary direct arm produces five/three A decisions and zero B decisions; its raw mixture is inactive throughout. It remains a diagnostic baseline.

These findings support retaining separate movement and execution-cost specialists and testing better conditional signed-return selection next. Any further conditional/regime search must use TRAIN/OOF data and be evaluated on separately declared later periods; these examined dates cannot be relabeled untouched confirmation. Do not refit this same grid under a new name. News, position management, full-archive expansion, live feature completeness and the older operating queue remain separate unfinished work.

### Verification and final artifacts

Both `_v2` results report complete, both continuation processes exited zero, and both independent recovery audits passed. Each audit rechecked all 156 reused predecessor artifacts, replayed the 27 six-head bundles and eight meta models, checked the learned comparators/controls, and validated all 63 forecast files. Source/input hashes, prefix normalization, chronological OOF labels, removed-feature tree splits, forecast keys, gates and executable-cost arithmetic were checked. **172 distinct research tests passed**; subtests and separate cleanup tests are not counted again.

- [Audit A](validation/rolling_period_replication_20260915/INDEPENDENT_AUDIT_A.json), SHA256 `831871061be7993d824d2642f37468af96fc92e4b74b036063745e0fafc48170`.
- [Audit B](validation/rolling_period_replication_20260915/INDEPENDENT_AUDIT_B.json), SHA256 `7e0b509333d514a96f52befa4acb36218ba393827fb22557edfed4f7f93d88e2`.
- [Complete readable tables](validation/rolling_period_replication_20260915/summary/SUMMARY.md) and [machine-readable summary](validation/rolling_period_replication_20260915/summary/SUMMARY.json), JSON SHA256 `bb6e8f258913a626254f1d39371f8a3fa8bd244620c5e442fbe1577dd12c4bb3`.
- Final results: `data/rolling_period_replication_20260915_a_v2/RESULTS.json`, SHA256 `a5b77461b3f277ec55b28ee03fe287ec3b39b39e68e29a6dfc62b5e53ab4eb46`; B `_b_v2`, SHA256 `3e22e4025a9f4edd5720024aeba57e6642d9db9655fbb1ba964b2d2ed4afc3eb`.

The earlier descriptive observation file retains its accurate audit-pending status at creation. The final independent interpretation binds both passed audits, and `FINAL_RESEARCH_DOC_UPDATE.json` records the accepted conclusions. Models were not promoted, and live collection/trading settings were not changed.


## Recreation and retained dependencies

Run from the `trad` directory. The exact executed input-building arguments, UTC boundaries, source inventories, configuration hashes and output names are retained separately in `validation/rolling_period_replication_20260915/configs/period_a.json` and `period_b.json`. The input order is rolling technical partitions, endpoint labels, executable quote panel, normalized model inputs, then V2 specialist inputs. Use fresh output directories when recreating a completed run; these scripts deliberately refuse to overwrite accepted evidence. Changing paths requires corresponding configuration changes, not changes to the frozen numerical recipes.

The model commands used for the two windows are:

```powershell
python -B tools/run_rolling_specialists_v2.py --inputs data/rolling_specialist_inputs_20260915_period_a_v2 --comparison data/rolling_model_comparison_20260915_v1 --output data/rolling_period_replication_20260915_a_v1 --threads 2
python -B tools/run_rolling_specialists_v2.py --inputs data/rolling_specialist_inputs_20260915_period_b_v2 --comparison data/rolling_model_comparison_20260915_v1 --output data/rolling_period_replication_20260915_b_v1 --threads 2
```

`RUNTIME.json` records the actual Python executable and dependency versions. `PRE_FIT_SELECTION.json`, the immutable `PRE_FIT_SCOPE.md`, launch receipts and completed input-build receipts distinguish the design fixed before fitting from the later execution evidence. Exported `normalizers_A/NORMALIZERS.npz` and `normalizers_B/NORMALIZERS.npz` preserve the five TRAIN-prefix statistics for all 68 pairs and 50 selected fields. Their JSON records bind array order, support, cutoffs and hashes.

For the completed recovered `_v2` runs, independently check with `tools/audit_rolling_family_recovery_v1.py --comparison <run-directory> --output <new-audit.json>`. This wrapper performs the frozen full audit plus predecessor/compatibility checks and the corrected original-column tree mapping. The original `_v1` commands above are retained as executed history; on the recorded library version, follow them with the pinned family-only continuation, rather than treating their intentional missing-column failure as completion. After both runs complete, use `tools/summarize_rolling_period_replication_v1.py` with both `--comparison` arguments and `--old-reference data/rolling_specialists_20260915_v1`. A final vault package must bind the exact results, passed audits, summary, models, normalizers, source closure and test receipts; a launch receipt alone is not completion evidence.

The vault is a compact recreation checkpoint, not a duplicate of every private or large data file. Original M1 archives, prepared matrices, OOF rows and forecast rows remain local, with their paths and hashes recorded. In particular, the selected historical Parquet source values have canonical decoded-value hashes; a physical-file hash for every original archive is not claimed. Restoring the scripts without their recorded raw dependencies is insufficient to recreate the studies. Old stdout logs may be losslessly compressed under the separate cleanup checkpoint; they are not inputs to these model fits.

### Recorded compatibility repair

Both original `_v1` runs completed their 20 full-context variants and contemporaneous controls, then failed on the first removed-family fit. The installed scikit-learn 1.9.0 binning function did not handle a numeric column with zero effective observations. This matters because the fixed family-removal design deliberately supplies all-NaN columns. The failed manifests and execution receipts are preserved, and no partial results were used to change the experiment.

The continuation uses `tools/resume_rolling_family_replication_v1.py` and the scoped `tools/rolling_empty_binning_compat_v1.py`. It copies and verifies all 156 retained artifacts per window into fresh `_v2` outputs, then runs only the seven pending family fits. The helper returns an empty threshold array for empty effective columns and delegates every nonempty case unchanged. Original sources, installed library files, NaN masks, recipes and full-context model bytes stay unchanged. The helper is not needed to load and predict with the saved models.

Actual continuation commands and frozen hashes are in `validation/rolling_period_replication_20260915/Run-FamilyRecovery.ps1`, `RECOVERY_RUN_A.json` and `RECOVERY_RUN_B.json`. The repair design and exact failed predecessors are recorded in `RECOVERY_PLAN.md` and `RECOVERY_PLAN_001.json`. Twenty compatibility tests and ten continuation tests passed, including a real six-head masked fit, ordinary-input bit parity and saved-model replay without the shim.

The new independent recovery audit also maps internal tree feature indices back through the estimator's categorical-first preprocessing before checking removed original columns. The original auditor and its evidence remain preserved. This coordinate correction changes neither model predictions nor the removed-feature test's requirement: no removed original column may supply a tree split.
