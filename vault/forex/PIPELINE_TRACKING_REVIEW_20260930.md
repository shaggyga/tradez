# Tracking review — September 30

This is a scoped read-only review following the rolling-pipeline repair. These
are visibility follow-ups, not a newly implemented monitor or research experiment.
The underlying records should be reused rather than collected twice.

## Already recorded

- Per-role heartbeat/schema/source identity, restarts, reported errors and cooldowns.
- News snapshots, actual availability, parsed frame hashes, capture history and replay.
- Per-pair input readiness: mature training rows, nonzero news rows, context patterns,
  unavailable quotes and refusal reasons.
- Original forecast/input/target identities, publication, consumption, native H1
  observations, scored outcomes and exclusions.
- Price-only scorecards with forecast errors, Brier scores, baseline comparisons and
  spread-adjusted research results. A checked EUR/USD snapshot had 276/278 state-space
  decisions scored and 272/276 Ridge decisions scored; missing entry/target quotes
  were explicitly excluded. These are retained cohort totals, not a new experiment
  or a today-only performance claim.
- Joint native scorecards already include matched-price and neutral-news comparisons.
  V11 currently has no mature forecast result. Its executable/position result is
  explicitly unavailable; native close-to-close scores are not trade returns.

## Visibility follow-ups

1. A unified model-status view including inactive and historical models, not just
   the selected 17 runtime roles. Show selected version/cohort, last input, last
   forecast/outcome and why each model is disabled, warming, blocked or active.
   Currency rankers demonstrate this gap: V7 has zero ledger decisions/forecasts/
   outcomes; V8 collection is disabled; Practice006 was intentionally retired.
2. Warmup and useful-work progress over time. Surface mature-row/context counts,
   their last increase, remaining thresholds and last successful capture/forecast.
   Preserve the distinction between a healthy heartbeat and useful work. An ETA
   cannot assume new context patterns or quote coverage will arrive on schedule.
3. A consolidated latency view using existing source/observation/parse/feature/
   issuance clocks. Show delay distributions and stale/missing inputs by pair and
   feed; do not infer source-publication availability from later first capture.
4. Cohort- and date-filtered outcome coverage: issued, not yet mature, scored,
   excluded and unresolved, with reason and age. Reuse existing scorecards for
   errors, probability calibration diagnostics and appropriate matched controls.
   Keep native-price scores separate from executable cost-aware evaluations.

No claim is made that all historical monitoring modules were audited. Checked
current supervisor/inspector, selected V11 worker/summary/scorecards, dashboard
selection, original EUR/USD price scorecards and currency-rank ledger/configuration.
The existing scientific queue remains unchanged. Independent review is separate.
