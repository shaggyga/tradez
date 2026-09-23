# Feature changes mapped to currency and pair movement

**Implementation follow-up:** [September 13 repairs and validation](FOREX_FEATURE_MOVE_REPAIRS_20260913.md) supersede the implementation-pending status below for snapshot capture, component clocks, the observation join and elapsed-window diagnostics. This original review remains dated evidence; older missing history and live activation are not retroactively repaired.

September 13, 2026. Source and saved-record review; no live market snapshot, new model fit, broker action or service activation.

## User objective clarified

The dashboard already displayed the largest market moves. The requested connection is **large changes in feature values mapped to currency/pair movement**, with an inspectable record of what the feature space looked like around each move. This is useful descriptive research without first qualifying a profitable model. The user also wants the authoritative news/release context retained across the 21-currency universe.

Support both directions of inspection: select a large feature change and see the associated pair movements, or select a large pair move and inspect the accompanying feature changes. Keep observations even when no model forecast or trade qualifies. This clarification supplements the [signal research roadmap](FOREX_SIGNAL_RESEARCH_ROADMAP_20260913.md); it does not replace the separate direction, cost and management evaluations.

## Existing pieces to reuse

| Existing component | Source-backed capability | Scope limit |
|---|---|---|
| `oanda_latest_moves.py` | 5/15/60-minute move rankings in comparable basis points, plus separate directional-leg and velocity views | Ranking price moves alone does not rank feature changes. The older `since_open_executable` ranking uses native net pips and is unsuitable as the cross-pair percentage ranking. |
| `oanda_practice_shadow_strategy_lab.py::write_live_model_feature_snapshot` | Per-pair scalar features, timeframe views, unified features, quote clocks and selected series | Receives actual populated caches; this is not proof that every feature in the design catalogue is collected. Generation time is not every constituent feature's observation time. |
| `oanda_model_gap_live_signal_worker.py::archive_feature_snapshot` | Compressed historical rows keyed by snapshot, pair and timeframe | Per-timeframe views take precedence over the top-level scalar row. Some current-only scalar enrichments are therefore omitted from history. Arrays, dictionaries and coverage/exclusion metadata are not archived by this function. |
| `oanda_move_first_news_case_audit.py::causal_m1_technical_state` | Reconstructs returns, the final admitted bar's opening spread, MA differences, range position, breakout and exhaustion around a move | A compact historical diagnostic, not the full feature space or an original live forecast. Timing and range naming defects below require a versioned correction before reuse. |
| Move/news snapshot V7R3 and live case capture/alignment | Retain moves, timed news context, technical direction and related currency episodes | Existing move-selected observations help explain cases; they omit the quiet/failed-alert denominator needed to assess a noise filter. |
| Feature dictionary and prior wide-family audits | Names, formulas, units, lookbacks, lineage and historical results | Catalogue counts differ from produced, archived and fitted columns. Preserve those distinctions. |

References: [rankings](../oanda_latest_moves.py:693), [snapshot writer](../oanda_practice_shadow_strategy_lab.py:7907), [archive writer](../oanda_model_gap_live_signal_worker.py:144), [technical reconstruction](../oanda_move_first_news_case_audit.py:492), [live alignment](../oanda_move_first_live_arm_alignment_v1.py:1), [dictionary](FOREX_FEATURE_DICTIONARY_CURRENT.md), [earlier feature/horizon audit](FOREX_EXISTING_FEATURE_HORIZON_AUDIT_20260908.md).

The project log already records this kind of observation: on August 28, a CAD-weakness episode was technically aligned, followed by a CAD reversal with technical continuation aligned across the ten top mover legs. Those are dated observations worth retaining. They are not current quotes or evidence that every technical alert worked. See [dated log](../FOREX_PROJECT_LOG.md:682). The reviewed `Add forex news feeds` chat also contains move-plus-technical/context summaries; chat narration is a discovery aid, not a substitute for the original timed records.

## Concrete audit findings

1. **Historical feature completeness is uneven.** The latest JSON and archival Parquet do not contain identical feature families. Do not claim to reconstruct the full original feature space from the archive until their schema coverage is reconciled. Retain top-level scalar additions, per-timeframe data, missingness and rejected-pair reasons explicitly. Nested forecast structures should be separately named and versioned rather than silently counted as indicator columns.
2. **Timeframe-specific clocks are necessary.** The old archive has generation, feature-origin and quote clocks, but unified rows can use the primary feature clock for mixed-timeframe fields. Its M1/M30/H1/H4 flattened values are alternate encodings of timeframe views, not extra independent indicators. The worker permits snapshot ages of at least 600 seconds; a recently generated snapshot is not sufficient evidence that every field is suitable for a five-minute change calculation. Show availability/age per feature group and refuse unsupported deltas without hiding the price row.
3. **The old technical reconstruction can mislabel elapsed windows.** An isolated synthetic check admitted 61 M1 records spaced two minutes apart. Fields named 5m/15m/60m then spanned 10/30/120 minutes. Consecutive-minute controls had the expected spans. The helper checks bar completion and count but uses row offsets for these returns. This establishes a helper defect, not that a particular historical result was affected. The current pair-local learner separately uses elapsed timestamps and must not be conflated with this helper.
4. **Its `atr14_pips` is average high-low range.** It omits gaps relative to the previous close. A synthetic case produced 2 pips under that label versus 11 pips for the simple mean of true range. Preserve the old field's provenance; a successor should name the existing statistic accurately or use an explicitly versioned true-range calculation.
5. **Wide-feature support has real limitations.** The earlier 795-field audit found five duplicated cross-currency windows, and the older macro/news columns were constants. The strict 60-minute compact archive comparison had 17/24 constant technical slots under its own support rules. These are specific historical findings, not claims that all current features are constant. Redundancy and unavailable lookbacks belong in the observation view and any noise-filter review.

Synthetic evidence: [diagnostic receipt](../../git_publication_20260913/LEGACY_MOVE_FEATURE_WINDOW_DIAGNOSTIC.json) and [standalone reproduction](../../git_publication_20260913/check_legacy_move_feature_windows.py). Only the named function was extracted with Python AST; no project imports, market data, model files or runtime ledgers were used. Reviewed source SHA256: `70b3c1f9004f326db10fdc120d12501131b389a63e228e9f4aed7ed240d9abda`. [Independent snapshot reuse review](../../git_publication_20260913/MOVE_FEATURE_SNAPSHOT_REUSE_REVIEW_20260913.md) records the archive, coverage and clock findings. These detailed audit receipts remain in the named sibling directory outside the Git source tree. No source repair was applied in this review.

## Next implementation, before another fit

Build the missing feature-change join using the existing producers and dictionaries. This is an observation/research work item, not a dashboard redesign or trading-rule change.

- **Market ranking:** preserve largest signed and absolute percentage changes for a selected 5/15/60-minute interval. Record exact endpoints, age and coverage. Basis points are comparable across pairs; 100 bps equals 1%. Show spread/executable movement separately without suppressing observations for lack of a forecast.
- **Feature-change ranking:** show old value, new value, raw change, units and how unusual the change is versus that feature's past changes. Use percentage change only where the feature's scale and denominator make it meaningful. RSI uses point changes; signed momentum, balances and near-zero values need a stable historical scale or percentile. Missing or constant history produces an explicit unavailable score, not a large percentage or fabricated zero.
- **Two-way mapping:** from a feature change, show concurrent and subsequent pair moves; from a large pair move, show the feature changes and grouped technical/peer/news state. Preserve base/quote orientation and shared currency exposure so one USD event is not presented as many independent confirmations.
- **Timing:** preserve release publication, first receipt, usable text/mapping time, price endpoints and each feature's availability. Mark earlier, contemporaneous and later changes explicitly. A retrospectively reconstructed snapshot stays labelled reconstructed.
- **Noise comparison:** retain large feature changes with no move, quiet periods and move reversals as well as the largest moves. Evaluate each feature family and existing combinations across comparable sessions. Showing only successful-looking episodes cannot measure false alarms. Compare repeated patterns with the existing technical/peer/news factorial work before adding new indicators.
- **Storage and lineage:** avoid another unbounded copy of history. Use deduplicated, versioned numeric snapshots with explicit family coverage, and retain the original feature schema/producer identity. Keep observations separate from a model's feature contributions and from its predicted return.

The first acceptance check is faithful recreation of a selected event's before/during/after values from retained snapshots, including missing fields and timings. Prediction quality remains a later, separate assessment on all eligible observations.

## Official releases and the recalled JPY case

The [21-currency source review](FOREX_OFFICIAL_SOURCE_COVERAGE_REVIEW_20260913.md) finds broad configured official coverage and existing issuer-aware sentiment rules. Configuration, successfully downloaded text, structured numeric parsing and a calibrated directional effect are different levels of coverage. The static readiness inventory recognizes 43 of 168 currency/category cells for structured numeric parsing, with documented allowlist limitations; that is not a claim that only 43 sources exist or that the other text is unparsed. RBNZ direct access remains explicitly blocked in saved configuration.

The approximate 4:00 release / 4:04 model receipt remains unidentified. The saved July 31, 2024 BoJ policy case and September 1, 2026 survey do not confirm that exact recollection. Keep the anecdote as a retrieval clue without assigning it a prediction win or discarding it as impossible.

## Status

Completed here: source/report review, reuse map, independently reviewed snapshot gaps, and bounded synthetic reproduction of two legacy diagnostic issues. Pending: versioned repairs, a complete feature-change history/join, a fresh live observation, and noise-filter evaluation. No new live status or profitable setup is claimed. The prior [operational checkpoint](FOREX_OPERATIONAL_CHECKPOINT_20260913.md) remains the source of activation status.
