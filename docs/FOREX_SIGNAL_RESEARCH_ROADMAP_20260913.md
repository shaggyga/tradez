# Signal research: evidence, reuse and next decisions

Checkpoint date: September 13, 2026. This is a research roadmap and record of decisions. It does not activate services, change a model, or claim new performance.

The objective is a useful forecast of direction, magnitude and the path ahead, with evidence that it improves entry or position management after executable costs. The audit must identify what has already been tried and what a new experiment would add. Operational correctness is necessary, but is not evidence of predictive skill.

## What the evidence does and does not say

It does **not** establish that news and technical indicators have no relationship with prices. Same-time technical relationships can be mechanical because the features are computed from prices. Some models have detected larger moves or improved a directional score. What remains unestablished is a stable advantage in forecasting future direction and enough movement to cover costs.

| Existing work | Recorded result | Research decision |
|---|---|---|
| Completed H1 ARIMA/SARIMAX programme | Corrected ARIMA: 49.95% direction over 47,580 forecasts and -1.7974 net pips per selected trade. The three retained generations reuse periods. | Completed baseline work. Preserve convergence and validation differences; do not rerun because an earlier, separate run lacked a library. |
| HGB opportunity and direction studies | Movement/opportunity AUC around 0.6822 in development; direction AUC around 0.5215. AUC is a ranking measure, not percent of forecasts correct. | Evidence of stronger movement detection than direction in those studies. It does not identify a profitable long/short policy. |
| Matched news-plus-price H1 comparison | One retained sample: 52.07% direction versus 50.20% for price alone; both negative after costs. Later joint samples also failed. | A descriptive improvement worth retaining, not proof that news provides a durable incremental edge. |
| Wide technical, peer and interaction features | Hundreds of features, boosting, lagged errors and multi-condition rules have already been tried. Wider inputs have often failed compact or no-change controls. | Inspect the exact fitted columns and target before proposing more features. A catalogue entry does not establish that the current learner consumes it. |
| Curves and position management | Multi-horizon curves, risk estimates and paper management comparisons already exist; a dependable management advantage remains unestablished. | Reuse the existing implementations and episodes. Do not label a new H1 wrapper as the first curve, or assume that better direction automatically improves exits. |

These rows have different targets, periods, populations and cost conventions; they are not a current head-to-head model ranking. Positive historical slices remain in the records, with their failed sample-size, stability or validation qualifications.

Primary records: [ARIMA/H1 clarification](FOREX_ARIMA_AND_H1_READBACK_20260913.md), [model reuse and performance register](FOREX_MODEL_REUSE_REGISTER_20260911.md), [archive comparison](FOREX_ARCHIVE_DIRECTION_COMPARISON_20260912.md), [direction factorial comparison](FOREX_DIRECTION_RESEARCH_20260911.md), [signed-cost follow-up](FOREX_DIRECTION_DECISION_20260912.md), and [existing curve/management buildout](FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md).

## Ordered work

**Observation workstream clarified September 13:** the user also wants the largest changes in feature values mapped to currency/pair moves, with before/during/after snapshots. This view does not require a qualified forecast. Reuse and repair the existing move rankings and feature archive; retain feature spikes with no price move so noise can be assessed. [Source review, concrete gaps and implementation record](FOREX_FEATURE_MOVE_MAPPING_REVIEW_20260913.md). [Official coverage and BoJ episode review](FOREX_OFFICIAL_SOURCE_COVERAGE_REVIEW_20260913.md). These observations support, rather than replace, the predictive comparisons below.

1. **Close the evidence map for the next experiment.** Link each selected predecessor to its actual input columns, trained artifact, target, periods, costs, results and recreation method. Mark missing, invalidated, failed, descriptive-positive and untested separately. The broader every-model audit still has unresolved lineage/recreation coverage; inventory counts are not a claim that every variant was independently rescored. Do not rerun an already resolved comparison without a stated material difference.
2. **Finish the measurement prerequisites.** The new joint path preserves its original minute and exact H1 target; older standalone controls still need their separate native-target migration. Establish fresh input availability and live recovery when startup is possible. Keep original first-observation times, revisions, missingness and refused forecasts. CFTC and FRED/ALFRED remain separate adapters until their bridges are completed. These are implementation gaps, not an explanation for all historical losses.
3. **Test whether additional information improves direction.** Reuse the existing Ridge/HGB and technical/peer/news factorial work. A concrete successor would compare technical-only, technical-plus-peer, technical-plus-news and their interaction on identical decision rows, using fresh correctly timed news and the repaired target convention. Reuse the existing official-release/blurb and currency-strength mappings. Measure stronger-versus-weaker currency pairing against the same price-only and no-trade controls. The changed information and timing must be explicit; another algorithm name is not sufficient novelty.
4. **Test the curve's value for management separately.** Reuse existing horizon chains and hold/exit/reduce/rotation controls with compatible origins, targets and fresh updates. Compare against fixed hold and no-trade on matched opportunities. Report directional quality, magnitude error, entry economics and management economics separately, including missed and withheld decisions.

The first new research run is not selected or executed by this checkpoint. Its protocol must identify which unresolved question it can answer, name the closest predecessor, and justify its data and sample support before fitting.

## Required record for every experiment

- Prior family/variant/run and links; what materially changes and what is reused.
- Source/data/artifact hashes, ordered fitted columns, original availability times and transformations.
- Pair universe, forecast origin/target/horizon, gap rules, eligible/withheld/pending denominators.
- Chronological training and assessment periods, overlap controls and whether any assessment dates were previously inspected.
- Fixed decision rule and executable bid/ask costs, delayed-entry and higher-cost sensitivity.
- Direction accuracy, calibration, magnitude error and net outcomes with coverage and day/currency concentration.
- Recreation command/environment, result, limitation, and decision: retain as control, investigate a named gap, stop the variant, or seek fresh confirmation.

Already inspected periods are development evidence. A small positive slice or threshold chosen after seeing results cannot become fresh confirmation by renaming it. Failure is a useful recorded outcome; neither clean execution nor more features guarantees an edge.

## Current operational boundary

The [operational checkpoint](FOREX_OPERATIONAL_CHECKPOINT_20260913.md) records installed offline repairs and remaining activation limits. No profitable setup is qualified. Dashboard work remains secondary to input quality, comparable evaluation and demonstrated signal value. The [pending improvements index](../FOREX_PENDING_IMPROVEMENTS.md) and [change register](FOREX_CHANGE_REGISTER_20260911.md) retain older work rather than replacing it with a new unconnected queue.
