# Joint forecast eligibility and horizon review — 7 September 2026

The live joint model already fits price and news together, but it predicts **H1 only**. Four pairs are being withheld partly because of a discretionary 15-minute training grid. Real, causally reconstructable 3-minute rows would pass their unchanged count checks. That finding justifies a separately tested research revision; it does not validate a learned news effect or justify weakening evidence clocks.

This review changed no project source, registration, ledger, process or order setting. It fitted no models and ran no new scorer. Price/news captures and stored scorecards were retained outside the project. Findings refer to the observation clocks below, not an indefinitely current state.

## What is actually combined

`oanda_joint_price_news_models_v1.py` fits 24 technical features, eight point-in-time news features and two price-rate × news-balance interactions in one ridge model. Its label is the real same-session price difference at exactly reference + 3,600 seconds. Retained matched price-only and same-fit neutral-news ablations provide comparisons on identical decisions; they do not establish causal news impact.

Context includes currency-attributed article balance, volume, signed fraction and age. Vetted directional features are separate. A joint fit with contextual articles is not evidence that a forward directional news relationship has been learned. The displayed probability is deliberately shrunk and uncalibrated; it is not historical accuracy.

## Which restrictions can be reconsidered

| Restriction | Assessment |
| --- | --- |
| Actual publication/first-seen/mapping/consumer clocks; original event expiry; exact source hashes; no future training labels | Evidence integrity. Preserve these and never make a late article appear earlier or renew its expiry on reread. |
| Exact H1 endpoint, same real session and explicit missingness | Preserve the real-time meaning. A different horizon needs its own exact label; filling missing prices is not a substitute. |
| Training starts only where epoch modulo 900 equals zero | Discretionary sampling. Demonstrably discards usable data for the four pairs below. |
| Minimum 48 labels, 12 nonzero-context rows, eight context patterns | Chosen model-eligibility policy, not a causal law or proof of stability. The latter two are not counts of independent stories. |
| Current eight real returns over at least 900 seconds; price age 900 seconds; news age 300 seconds | Chosen sparsity/freshness tolerances with an integrity purpose. Any new values require an explicit new contract and stale/missing-data tests. |
| An unseen nonconstant vetted-news query dimension blocks the joint model | Protects against claiming a learned effect without examples. A future model could explicitly omit unsupported dimensions while retaining a validated submodel, with scope disclosed; it must not invent an effect. |
| Ridge alpha 50, effective count n/4, shrinkage prior 20, probability shrinkage 0.5 | Modelling assumptions. They must be evaluated separately from source integrity and trading authorization. |
| 48-hour/5,000-record news history and 256 frame bound | Bounded workload/model scope. A denser grid also needs a bounded frame-capacity design; removing the cap is not the fix. |

No publication rule requires the price and news directions to agree, or the expected move to exceed the spread. Research orders and promotion remain disabled by separate controls. Small expected moves can therefore be valid research forecasts yet economically unattractive after spread.

## Reconstructed anchor counts

The frozen adapter's `_frame(news, t+60)` was replayed for 1,488 distinct eligible timestamps from retained original-member evidence. Each included row had a real exact H1 endpoint, the same session, valid technical inputs, and historical news coverage. The observation ran **23:29:36–23:30:16 UTC**. No fit, threshold search or outcome-based subset selection was performed.

Each cell below is **mature rows / nonzero-context rows / distinct context patterns**. Existing minima are 48 / 12 / 8.

| Pair | Existing 15-minute grid | Reconstructed 3-minute grid | Vetted training rows, 3-minute grid |
| --- | --- | --- | --- |
| EUR_DKK | 39 / 19 / 20 | 149 / 61 / 62 | 0 |
| GBP_HKD | 92 / 10 / 11 | 445 / 41 / 42 | 0 |
| GBP_PLN | 94 / 7 / 8 | 471 / 33 / 34 | 0 |
| GBP_SGD | 86 / 5 / 6 | 421 / 23 / 24 | 0 |

All four pass the unchanged count checks on the denser grid. This does **not** mean all four would pass every current-publication requirement. It also does not make 149 overlapping H1 rows into 149 independent observations. The current `n/4` heuristic assumes the nominal 15-minute/H1 relationship and cannot be carried unchanged to 3-minute rows. Use actual timestamp/label overlap and blocked evaluation; cross-pair observations can remain dependent too.

The eight-pattern check includes the continuous age feature: one story ageing can produce many distinct patterns. Twelve context rows may also repeat one story. Report unique claims, independent publishers and time blocks explicitly. **All four have zero vetted directional-news training rows under every inspected grid**, so lowering the grid cannot demonstrate that forward-news effects have been learned. Earlier price-only history without reconstructable news was excluded, not backfilled with today's articles.

## First retained H1 outcomes

At **23:33:10 UTC**, the retained scheduler-v1 study had 200 lifetime forecasts, 19 original targets due, 18 retained outcomes and 18 stored scored decisions. Direction was correct on **10/18 (55.6%)**; **1/18 (5.6%)** was positive after spread. Mean net was **−5.6129 bps per scored decision**. The separate fair-scheduler v2 study had 217 lifetime forecasts and **zero targets due**; its first retained target was 23:45:40 UTC.

The one due target without a retained outcome is kept outside the scored denominator; this snapshot does not assign its cause. Scorecards and ledgers have separate observation clocks. Net bps describe the scorer's executable bid/ask result before financing and additional slippage, not account dollar P/L. Eighteen overlapping, cross-pair observations are too few to establish accuracy, calibration or added news value. The paired comparator decomposition is retained separately on exactly these same 18 decisions, without any retrospective refitting.

## Smallest sensible next step

The immediate UI improvement is to expose already valid price-only forecasts where the joint model is waiting, explicitly saying that news is not included and retaining the joint blocker. This preserves an honest live view without inventing a combined forecast.

For the next registered model revision, investigate the demonstrated H1 anchor-grid bottleneck first. Preserve original news availability and exact labels; specify timestamp-overlap adjustment, event-diversity reporting and bounded frame handling before freezing a new contract. Compare the full new cohort with matched price-only and neutral-news ablations on chronological held-out blocks. Do not simply reduce the 48-row minimum or interpret more overlapping rows as independent evidence.

Existing `oanda_multihorizon_panel_model.py` provides direct M5/M15/M30/H1/H2 fits, timestamp-aligned horizon purges, train-defined normalization, selection-only parameter choices and horizon-block reporting. `oanda_shared_timeframe_horizon_panel.py` provides side-specific bid/ask label/schema machinery. These are useful patterns, not proof of a prospectively registered causal-news model: their historical snapshot/label provenance and news availability would require independent validation before reuse. A true curve needs separately trained and scored horizon labels with each horizon's own valid denominator. Scaling the H1 estimate into a line must remain labelled an illustration, not trained short-horizon predictions.

No new curve, model or registration was activated by this review.

Evidence: `ANCHOR_ELIGIBILITY_AUDIT_20260907.json`, the retained price/news captures, `CURRENT_JOINT_H1_OUTCOME_OBSERVATION_20260907.json`, `RETAINED_H1_PAIRED_DECOMPOSITION_20260907.json`, and `JOINT_HORIZON_REVIEW_VALIDATION_20260907.json` in this directory. Source locations and hashes are bound in the validation receipt.
