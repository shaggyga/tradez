# Rolling specialist comparison — September 15, 2026

This continues the [completed rolling model comparison](FOREX_ROLLING_MODEL_COMPARISON_20260915.md). The new question is whether predictions from separate direction, movement-size and execution-cost models contain useful information when combined using strictly earlier training evidence. The run is isolated research; it does not change live models, input selection, trading settings or old results.

## Reuse and material difference

The project already has the relevant specialist mathematics in `../direction_decision_20260911/src/signed_cost_models_v1.py`. That study fitted probability, positive/nonpositive magnitude, direct-return and asymmetric exit-cost models. Its 24 learned policies had 17 negative means, four small or unstable positives and three inactive cases. Probability calibration improved, but selected signed returns remained optimistic. For one combined 30m example, return optimism contributed +5.977 bps while exit-cost error contributed −0.039 bps. Cost prediction or probability calibration alone is therefore not assumed to solve the problem.

The exact retained calibration and decision helpers are reused with a source hash. The old feature whitelist, imputation/scaling, date splits and timestamp weighting are incompatible with the accepted rolling contract, so a separate adapter fits the existing targets on the new inputs. This is not an unchanged rerun of the old specialist study.

The new component is a combining model trained on **chronological out-of-fold (OOF) predictions**. Each OOF prediction comes from a model and normalization statistics fitted before that prediction's week. Old full-TRAIN models and normalization are never presented as historical OOF predictions.

## Fixed comparison

| Component | Choice |
|---|---|
| Input/horizon contexts | Compact 38 local or compact 50 including peers; 30m and 60m |
| Reason for scope | Targeted follow-up to the retained H1 and 30m development leads; no new untouched-test claim |
| Historical population | Accepted July 13–September 7 archive slice, all 68 pairs |
| TRAIN | July 13 inclusive–August 24 exclusive |
| Assessment | August 24–31 and August 31–September 7, reported separately |
| Fit origins | Original UTC 15-minute TRAIN samples with mature endpoint targets |
| Assessment issuance | Every original assessment minute, before any future validity mask |
| Base heads | Positive-return probability; direct signed return; positive-return conditional mean; nonpositive magnitude including flats; separate long/short exit-cost means |
| Base recipe | Fixed HGB: 120 iterations, 15 leaves, minimum 200 samples/leaf, learning rate 0.05, L2 10, 128 bins, fixed seed, no early stopping |
| Base inputs | Prefix-normalized compact fields, two known asymmetric entry costs, categorical pair ID |
| Weights | Equal sampled rows; conditional branches retain the same underlying measure |
| Primary policy | Unchanged current observed spread plus 1 bp gate and per-pair fixed-horizon nonoverlap |
| Secondary policy | Separately named expected side-specific entry-plus-exit-cost gate, same 1 bp margin |
| Evaluation costs | Actual chosen-side bid/ask endpoint net minus 0, 1 or 2 extra round-trip bps |
| Delay | Exact next-minute entry, unchanged forecast, decision, reservation and terminal clock |

The seven mean estimates are direct return, raw conditional mixture, calibrated conditional mixture, direct-only Ridge linear recalibration, specialist Ridge, direct-plus-context HGB and specialist-plus-context HGB. This gives 28 mean variants; both gates are reported for every variant without selecting a winner from the two assessment weeks.

The direct-only Ridge control sees only the direct head and pair context. Its coefficient is unconstrained: it can reverse the forecast as well as reduce its magnitude, so it is not necessarily a shrinkage-only control. The direct-context HGB sees the direct head, current entry costs and eight causal market-state inputs. Their richer counterparts add the other specialist outputs. These controls distinguish specialist contribution from linear recalibration and a second pass over market context. Policy means can come from different decisions even when forecast error comparisons use identical origins.

## Chronology and normalization

| Prefix model fit cutoff, UTC | OOF prediction interval |
|---|---|
| July 27 00:00 | July 27–August 3 |
| August 3 00:00 | August 3–10 |
| August 10 00:00 | August 10–17 |
| August 17 00:00 | August 17–24 |
| August 24 00:00 | Final assessment models, using full TRAIN |

A fitting target's candle END must be strictly before its prefix cutoff. OOF meta-training labels must also end strictly before their respective OOF block end. Original OOF rows remain issued even when their future target is absent or excluded. The 602-minute backward feature context needs no extra embargo: it contains known past information. Forward targets are what require purging.

The new input package reads the accepted float64 core/peer columns again, without recalculating candles or indicators. It does not invert the old float32 standardized arrays, which would lose precision and preserve future-trained missingness. For each pair, all raw prefix observations determine that prefix's means, scales and support flags before sampling or label selection. The final normalization matches every retained old prepared value and missing mask exactly.

Completed preparation checked all 68 pairs and 544 raw partitions: 3,645,778 raw origins, retaining 183,540 TRAIN samples and 911,254 assessment origins. It took 115.938 seconds and wrote 304,790,578 bytes. There are 122,418 issued OOF origins; 118,131 at 30m and 116,943 at 60m have targets eligible for meta fitting. Earlier per-pair support can be small; pooled models do not make these independent per-pair validations.

Meta inputs contain six raw head predictions, the derived signed and absolute mean, known entry costs and eight causal context values. Context normalization uses the same earlier prefix as its head predictions. Meta Ridge scaling uses all issued OOF inputs before target filtering. HGB uses native missing values and categorical pair identity, with a fixed smaller 8-leaf/minimum-500-row recipe. No assessment statistic sets a transformation, gate or parameter.

The Platt calibrator uses raw OOF probabilities and mature TRAIN OOF labels. It supplies a separate calibrated-mixture assessment arm. Its calibrated same-row probabilities are **not** meta-training inputs. Raw, calibrated and TRAIN-prior probability scores are recorded separately.

## Movement and cost arithmetic

With p = P(R > 0), U = E[R | R > 0], and D = E[−R | R ≤ 0], the signed mixture is `p*U - (1-p)*D`; expected absolute movement is `p*U + (1-p)*D`. Flat returns belong to the nonpositive branch. These are conditional means, not median magnitudes multiplied by a direction score.

Here movement means the absolute **terminal return**. It does not forecast the largest intervening move, MFE/MAE, barrier order or time to peak. The derived mixture and absolute-mean columns are also not independent information sources. Path forecasts and management remain separate questions.

The midpoint candle need not equal the bid/ask center. Consequently, one symmetric cost estimate cannot simply be subtracted from midpoint return on both sides. The retained labels use separate costs:

```text
entry_long  = (ask_0 - mid_0) / mid_0 * 10000
entry_short = (mid_0 - bid_0) / mid_0 * 10000
exit_long   = R - entry_long - observed_long_net
exit_short  = -R - entry_short - observed_short_net
```

Actual sampled TRAIN quotes all had midpoint inside bid/ask and positive implied exit costs. Only negligible negative floating-point residues within 1e-7 bps may be rounded to zero; materially negative labels fail. Future costs remain targets, never entry inputs. Negative predicted magnitudes/costs are clamped to zero with explicit counts, following the retained specialist semantics.

The secondary gate uses current known entry cost plus predicted future exit cost. Actual outcome costs cannot alter entry decisions. Selected diagnostics separate signed-return optimism from exit-cost optimism and verify their sum against the expected-versus-realized net difference. Unique decision clocks, days and pairs remain visible so one event across related pairs does not masquerade as many independent successes.

## Verification and recreation

The [test receipt](validation/rolling_specialists_20260915/TESTS.json) records 73 passing tests for fold preparation, specialist models, scoring and orchestration. They cover strict chronology, future-input mutations, inherited source hashes, unchanged primary scoring, asymmetric costs, raw-only meta inputs, calibration separation, serialized model replay and artifact readback.

The implementation retains all prefix/final head models, meta models, calibration parameters, prefix normalizers, original OOF predictions and every assessment forecast. Completing the numerical run also requires checking the earlier recorded artifact hashes, not merely hashing whatever files exist at the end. Independent replay and result-summary receipts are added after actual completion.

Use the existing `C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe` from `trad`. Output paths below identify this run; recreation requires new output directories.

```powershell
python -B tools/prepare_rolling_specialists_v1.py --base data/rolling_technical_training_20260915_v1 --prepared data/rolling_model_inputs_20260915_v1 --quotes data/rolling_model_quote_panel_20260915_v2 --output data/rolling_specialist_inputs_20260915_v1
python -B tools/run_rolling_specialists_v1.py --inputs data/rolling_specialist_inputs_20260915_v1 --comparison data/rolling_model_comparison_20260915_v1 --output data/rolling_specialists_20260915_v1
```

These are previously examined development periods and dependent currency exposures. The study reports endpoint proxies, not broker fills, portfolio returns or account profit. News, position management, additional historical periods and forward confirmation remain separate work. A launched run is not a completed result; measured conclusions are appended only after completion and review.

## Interpretation boundaries

This experiment tests the contribution of specialist outputs with two compact input sets and fixed learners. It does not exhaust all conditional feature relationships, establish that the full feature archive is useless, or reproduce every older model. The comparison holds the chronological preparation and evaluation contract fixed so its particular question has a traceable answer.

A positive selected mean is retained even when small or limited to one period. Its decision clocks, currency concentration, delayed entry, added costs and unsuccessful periods determine what it supports. Related pairs at the same time do not supply independent confirmations. A lower direction-probability error also need not produce a profitable thresholded decision.

The provisional first-context review was requested after that context completed, while the fixed remaining fits continued. It is a descriptive concentration and coefficient inspection; no learned recipe or gate was changed in response. The separate component-baseline supplement was requested after the run began to compare magnitude and cost errors with simple TRAIN-only statistics. It is explicitly posthoc reporting, not a retroactively preregistered result or an extra model-selection stage.

## Completed numerical results

The run completed in 2,268.485 seconds (37 minutes 48 seconds): 20 six-head bundles containing 120 estimators, 16 combining models, four calibrators, two horizon prior records and all 28 forecast variants. Every variant retains all 911,254 original assessment origins. The sealed [RESULTS](validation/rolling_specialists_20260915/RESULTS.json) SHA256 is `43008a70624bec3ce6d25003c4f3dcb486b678019d01da99a7256bdc07233f1d`.

For **each gate**, the 56 full-endpoint variant-period results contain four positive means, 47 negative means and five inactive cases after quoted bid/ask costs plus 1 bp. No full-endpoint policy is positive in both development periods, including at quoted costs alone. All three path cohorts remain in the detailed report; membership in a future-valid path cohort cannot select an entry.

The positive cases under the original observed-spread gate are below. Values are mean net bps per scored, fixed-horizon decision, not account returns. Validation is August 24–31; later development is August 31–September 7.

| Compact38 variant | Validation: n / net +1 bp | Later: n / net +1 bp | Positive period with +2 bps | Positive period with 1m delay, +1 bp | Remove its strongest day |
|---|---:|---:|---:|---:|---:|
| 30m direct-only Ridge recalibration | 0 / inactive | 25 / +5.021 | +4.021 | +3.087 | −1.640 |
| 30m specialist Ridge | 9 / +0.823 | 38 / −9.574 | −0.177 | +1.488 | −2.168 |
| 60m calibrated mixture | 233 / −4.664 | 489 / +0.579 | −0.421 | +0.466 | −1.251 |
| 60m direct plus context HGB | 150 / −6.777 | 407 / +0.401 | −0.599 | −0.154 | −0.605 |

Delayed entry preserves original decisions but may change which outcomes can be scored. The detailed report retains both counts; the two means are not automatically a same-trade treatment effect.

The largest positive mean comes from a **learned reversal**, not the full specialist combination. Its direct-head coefficient is −0.1770415 in raw-return units, with small pair offsets. The 25 decisions span only 11 clocks, 16 pairs and three days. Seventeen involve JPY; those contribute 87.18% of the net sum. Removing September 4 turns the original and delayed results negative. The [dated case review](validation/rolling_specialists_20260915/COMPACT38_30M_CASE_REVIEW.json) preserves the exact saved coefficients, concentration, delay and unsuccessful comparison.

The secondary expected-cost gate retains the same four positive variant-period cases, but changes decisions and means. Its 30m direct-only later result is +2.767 bps over 28 decisions, falling to +0.730 with delay. Its calibrated 60m later result is +0.478 bps over 481 decisions (+0.387 delayed). It does not establish a stable improvement over the existing observed-spread gate.

### What the components contributed

Calibrated positive-return probabilities improve on the TRAIN pair probability in seven of eight context-period comparisons. They improve on raw probabilities in all four later-week contexts, while modestly worsening the four validation-week scores. For example, compact38 H1 later Brier score falls from raw 0.250457 to calibrated 0.249833 versus prior 0.249891. This is a small measured probability improvement; it is not an after-cost trading acceptance.

Mean-return errors are mixed. Only two of 56 variant-period comparisons improve MAE over predicting no change, and 11 improve RMSE; all these improvements occur in the later week. Compact38 H1 specialist-context HGB improves MAE by 0.012464 bps and RMSE by 0.019461 bps, yet its selected decisions lose 1.349 bps on average after costs. Compact50 H1 direct-only Ridge improves MAE by only 0.000454 bps and makes no thresholded decisions. Error improvement and profitable selection are different questions.

Relative to the new direct-return head, raw and calibrated mixtures improve RMSE in all eight context-period cells; raw mixture also improves MAE in seven and calibrated mixture in six. That is useful evidence for the conditional decomposition. However, specialist Ridge loses both MAE and RMSE to direct-only linear recalibration in all eight cells. Keep the simpler controls rather than assuming the largest combining model is best.

Adding every specialist output to the same context learner has inconsistent effects. At H1, compact38's direct-context later policy earns +0.401 bps while specialist-context loses −1.349; with peers they lose −0.430 and −1.297 respectively. At 30m with peers, the richer context learner improves the later mean from −1.506 to −0.909, but worsens validation from −5.831 to −8.754. This fixed comparison supplies no repeatable after-cost benefit from the larger combining layer.

Selected signed-return error remains much larger than exit-cost error. In compact38 30m specialist Ridge's losing later period, signed-return optimism is +10.663 bps versus exit-cost optimism −0.0158 bps. Compact38 H1 specialist-context gives +2.337 versus +0.0147 bps. These are measured decompositions on selected decisions; better spread estimation alone does not explain or repair those losses.

The retained prior comparison remains relevant: its compact38 Ridge H1 later lead was +1.984 bps over 46 decisions and negative in validation. The new study does not replace it merely because another cell has a larger best-week mean. Detailed summaries carry 18 prior controls/model cells over both periods without refitting them.

## Completed independent checks

The [independent audit](validation/rolling_specialists_20260915/INDEPENDENT_AUDIT.json) passed in 98.219 seconds. It verified all 138 artifacts and 138 saved references, 29,160,128 assessment key rows across 28 mean-forecast and four six-head forecast files, and 489,672 OOF rows across the four contexts. All 20 six-head bundles, 16 combining models and four calibrators replayed. Both gates passed 16,339 scalar cost cases; all 68 endpoint receipts and accepted source hashes remained unchanged. The first context's provisional-case hash still matches the completed context exactly.

There are 100 distinct passing tests: 73 for preparation/models/scoring/orchestration, nine for the independent auditor, ten for the summary and eight for the posthoc component check. [Auditor commands and hashes](validation/rolling_specialists_20260915/INDEPENDENT_AUDIT_RUN.json) and [summary/component test receipt](validation/rolling_specialists_20260915/SUMMARY_AND_COMPONENT_TESTS_FINAL.json) retain actual outputs. These checks verify numerical consistency and recreation, not profitable-model acceptance. The auditor replayed saved models on bounded rows and did not refit every estimator, regenerate raw candles or independently recompute every concentration ranking.

The [complete summary](validation/rolling_specialists_20260915/summary/SUMMARY.md) and [machine-readable table](validation/rolling_specialists_20260915/summary/SUMMARY.json) retain 112 variant/gate/period rows, 240 within-context contrasts, all path cohorts, separate probability scores and 36 prior-comparator period rows. The summarizer verified 80 files/manifests. Its direct-only control is explicitly named linear recalibration because a negative coefficient is allowed.

Recreate the independent reporting with new output destinations:

```powershell
python -B tools/audit_rolling_specialists_v1.py --comparison data/rolling_specialists_20260915_v1 --output <new-audit-json>
python -B tools/summarize_rolling_specialists_v1.py --comparison data/rolling_specialists_20260915_v1 --output <new-summary-directory>
python -B tools/analyze_rolling_specialist_component_baselines_v1.py --comparison data/rolling_specialists_20260915_v1 --output <new-component-directory>
```

These commands consume the retained accepted inputs. An output directory or file that already exists is not silently overwritten.

## Component value checked against simple baselines

The separate [posthoc component comparison](validation/rolling_specialists_20260915/component_baselines/POSTHOC_COMPONENT_BASELINES.md) completed 120 same-row comparisons across four contexts, two periods, three cohorts and five targets. It verified 281 files. Per-pair reference means/medians use exactly the final sampled and purged TRAIN population, with at least 20 finite labels. No assessment row is dropped for insufficient baseline support in these results.

**Movement information is present in this slice.** Expected absolute terminal return improves RMSE over the TRAIN pair mean in all eight full-endpoint context-period comparisons, by **6.60–8.26%** (0.217–0.420 bps). Both conditional magnitude heads also improve RMSE in all eight comparisons. These mean-target learners beat the TRAIN median on MAE in the four later-week comparisons, but lose on MAE in the four validation-week comparisons. This is narrower than saying every movement metric is better, and it does not establish peak-move or barrier prediction.

**Exit-cost estimation is also useful as a component.** Both long and short exit heads improve MAE against the TRAIN median and RMSE against the TRAIN mean in all eight full-endpoint comparisons. RMSE gains against the mean are approximately 53.6–58.6%. The report separately compares current quote-wing persistence: long exits use the current bid-side cost (entry-short wing), and short exits use the ask-side cost (entry-long wing). Midpoint asymmetry is preserved.

These findings support retaining the movement and cost specialists. They do not show that the combined entry policy captures this information profitably. Its much larger selected signed-return errors remain the main measured obstacle in the examples above.

The detailed report also preserves a tiny positive subgroup rather than hiding it: compact50 H1 specialist-context HGB with the secondary gate is positive at quoted costs in both **additional-endpoint-only** periods, on four validation decisions and one later decision. The later observation earns just +0.051 bps at quoted cost and loses −0.949 bps with the extra 1 bp. This cohort is defined using future path availability, cannot be an entry filter, and does not overturn the full-endpoint policy result.

## Checkpoint and next work

This closes the bounded specialist-combination experiment. It advances the existing feature/research queue; it does not close the whole project audit, all historical coverage gaps or the operating queue. Preserve the measured movement/cost value, calibrated probabilities and limited positive cases, together with their failures. Do not repeat the same seven-arm fit on these same dates and call it new evidence.

The next research work is separate historical rolling periods and feature-family contribution comparisons, retaining these exact controls and cost/latency definitions. Directional selection needs improvement and confirmation across different conditions. News and position-management reconstruction remain deferred as requested. No live model, manager, entry permission or broker setting changed in this step.

At the recorded 20:51:30 UTC collector observation, all 68 pairs were retained, 63 were current and 16 had the complete 216-field numeric set. Missing history still affects rolling feature completeness. The [retained snapshot](validation/rolling_specialists_20260915/COLLECTOR_OBSERVATION.json) concerns only this separate collector, not whole-project health. Collection continued; about 50.5 GiB remained free on C.

The [acceptance receipt](validation/rolling_specialists_20260915/ACCEPTANCE.json) binds the completed inputs, sources, models, tests and evidence. The compact vault supplement is `C:\Users\zmoor\OneDrive\thevault\projects\forex\ROLLING_SPECIALISTS_20260915`; its [publication receipt](validation/rolling_specialists_20260915/VAULT_PUBLICATION.json) records the final manifest, ZIP readback and source checks. It retains all saved model states and prefix normalizers, plus exact new sources and earlier-package dependency references. Full refit/evaluation still requires the local accepted raw/prepared/quote datasets or their exact reconstruction; these large row matrices are not duplicated into the supplement. Local copy verification does not establish cloud synchronization.
