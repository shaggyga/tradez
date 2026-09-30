# Forex operational status repair — September 7, 2026

The dashboard now distinguishes partial research operation, pair-specific news context, current directional news signals, and supplemental EUR/USD evaluation. The underlying study registrations, forecast targets, input requirements, source collectors, and order restrictions remain unchanged. The final combined 318-test suite and controlled runtime/desktop/mobile checks passed. These are engineering and display checks; predictive improvement remains unproven.

## News context and source health

The repeated “23 articles screened” was a global active-topic count shown on every pair row. It was not 23 pair-specific articles. The earlier dashboard also omitted neutral pair summaries and limited its ranked list to twelve non-neutral pairs, hiding available contextual headlines.

The read-only feed audit at 18:08:53–18:08:56 UTC found 19 active deduplicated topic clusters. All 68 published pair directions were neutral with zero eligible forward scores. The recorded primary withholding reasons were: ten topics lacking required independent corroboration, eight describing price moves that had already happened, and one discovered too late for its allowed reaction timing. Nine topics were also beyond their initial reaction horizon. Those overlapping age counts are not extra topics.

The source retained useful context: EUR/USD had 19 related topics, including 13 contextual directional topics; AUD/JPY had 15 and 15; USD/HUF had 16 and 10. None had an eligible forward topic at that observation. Generic textual tone, contextual currency direction, and a fresh forward signal are separate fields.

Those are dated audit observations, not a standing assertion that news must remain neutral. Later naturally eligible topics may produce forward signals under the unchanged producer rules. The final live view below records its own analytical cutoff and evidence quality.

The dashboard repair retains neutral summaries for all pairs, shows each pair's own related-topic and directional/context counts, and includes retained headline context. Global collection totals appear once. “Context only,” “No current news signal,” mixed or weak direction, a current long/short signal, and unavailable evidence are distinct. Analytical `as_of` age governs freshness; a recent output write cannot make an old topic a fresh catalyst.

The same audit found 192 configured sources: 176 healthy, three degraded, five disabled, five unsupported, one missing a credential, and two external adapters. The three enabled error sources were GDELT discovery (429 rate limit), HKMA press releases (timeout), and Sweden SCB CPI/CPIF (malformed or missing summary). Trading Economics lacked a credential. These transport/access issues remain separate repairs; this change exposes their status and does not claim to fix them. The official fast lane separately checked 42 configured sources, attempted six due sources, and reported no errors or new observations in that pass. Collector error health and recency-based source coverage use different definitions and retain their own clocks.

## EUR/USD minute gaps

At 18:13:38 UTC, the coherent EUR/USD local source ended at 18:10 and contained 55 of the latest 61 calendar-minute slots. Its consecutive suffix was 15 bars. Missing starts were 17:31, 17:34, 17:43, 17:47, 17:53, and 17:55 UTC (13:31, 13:34, 13:43, 13:47, 13:53, and 13:55 EDT).

A bounded read-only practice broker request returned 180 complete EUR/USD BAM candles through 18:12. It omitted all six same interior minutes. Normalization dropped no complete timestamp; all overlapping closes matched. The two newer broker candles were endpoint collection lag, distinct from missing interior candles. The captured local bytes and audited source hashes remained unchanged during the check.

Both EUR/USD study ledgers recorded `1<61` at 18:00, after a successful 17:30 capture with 1,024 consecutive retained rows ending at 17:24. This is a consecutive-window reset, not a statement that only one candle was collected all day. The rejected source bytes are not retained, so the failed attempt's exact reference minute is not independently reconstructed. The current source's one-row suffix at 17:54 is consistent with the recorded rejection.

All 957 retained recovery responses examined omitted their requested minute. The latest completed all-68 cycle reported 1,950 unresolved small gaps in its bounded 720-minute/at-most-five-minute-gap scope, zero recovered rows, and zero errors. This does not prove every gap is permanently absent. No recoverable EUR/USD candle was found, and no synthetic price, backfilled forecast, or historical availability claim was introduced. A model designed for sparse real minutes would require a separate registration and prospective comparison; the current 61-consecutive-bar contract is unchanged.

## Partial operation and supplemental scoring

The headline and Bot activity view describe the actual all-pair research state and split warmup/training, missing quote, building, and other unavailable reasons. The earlier EUR/USD/shared companion diagnostics remain explicitly named and available in detail; their counters are not presented as a global all-pair forecast total.

The original EUR/USD frozen scorer rejects repeated `reference_epoch` decisions. A separate read-only diagnostic module groups decisions only by exact finite reference timestamp and excludes every member of every duplicate group before examining outcomes. It keeps original quotes, order, clocks, cohorts, and H1 targets, then calls the unchanged scorer on the retained subset. It verifies the exact registered configuration, contract and nine source hashes, reads a bounded SQLite transaction, and retains distinct hashes for the full input, filtered input, and protocol.

The resulting metrics are **post hoc supplemental engineering diagnostics**. They are not a repair of the registered scorecard, a prospective performance claim, or promotion evidence. The original scorer failure and original scorecard state remain visible. All duplicate members are excluded; the diagnostic never picks a better-performing member. Samples overlap and are small. Hard read limits can make the diagnostic unavailable; the implementation does not silently substitute a rolling sample. Cached reports retain their original cutoff/generated clocks and may lag new observations by up to 60 seconds.

The first preserved adapter evidence at 18:20:56 UTC contained six original decisions, excluded both members of one repeated-reference group, retained four decisions, and scored three. Ridge had zero directional hits out of three and mean net return of approximately -4.0131 bps; state space had two hits out of three and approximately -1.1464 bps mean net return. The original ledger had five outcome rows, a different count that includes the excluded group. Both families were negative after costs in this tiny overlapping diagnostic sample. Later final metrics retain their own observation time and denominators below.

At the final API/ledger baseline recorded **18:33:11 UTC**, the pair study had 88 published and independently consumed forecast sets, 87 research entry-quote matches, 36 retained outcome rows, and three ledger exclusions. These are research records, not broker trades. Nineteen stored pair scorecards each had one paired scored decision at their own generation times (mostly around 18:18, with some around 18:30); they are not a re-score of all 36 outcome rows. Missing-target scorecard counts include pending original H1 targets and earlier observation cutoffs and must not all be described as failed outcomes.

The API baseline showed 16 forecasting pairs, 35 awaiting history/training, and 17 without fresh quotes, with zero pair-worker errors. News was explicitly stale at an analytical age of 302.938 seconds, despite a newer output timestamp. The later **18:35:13 UTC** desktop/mobile check showed 16 forecasting pairs, 43 warming and nine without fresh quotes. Its news evidence was 172 seconds old and EUR/USD was Short, with 18 related topics, one current directional topic and 13 topics carrying directional context. These changed observations demonstrate why every count and direction keeps its own clock; the earlier all-neutral audit is not a permanent assertion.

The final supplemental EUR/USD report was generated at **18:32:56 UTC**. All six original decisions now had outcome rows; excluding the two repeated-reference decisions left four retained and four scored decisions:

| Family | Direction correct | Positive after spread | Mean net return | Brier score |
| --- | ---: | ---: | ---: | ---: |
| State space | 3/4 (75%) | 3/4 (75%) | -0.8168 bps | 0.27697 |
| Ridge | 0/4 (0%) | 0/4 (0%) | -3.7195 bps | 0.52468 |

Both mean returns remained negative after spread. The fixed fair-coin probability baseline had Brier 0.25 on the same four decisions; the no-trade return baseline was zero. Four overlapping observations are insufficient to establish predictive quality, calibration, profitability or a model improvement. The original scorer still reported `duplicate_market_reference_epoch`; its original scorecard remained at 16:29:39 UTC and was visibly marked stale. The original companion's eight reported errors were retained, separately from the pair worker's zero errors.

## Verification and monitoring

The final combined suite passed **318 tests in 35.50 seconds**. The 187 dashboard/news focused tests and 43 diagnostic-adapter focused tests are supporting suites, not additional independent cases to add to 318. The preserved 311-case prefinal run used the source before the collapsed stale-news path was fixed and is not the final validation run. Independent dashboard and diagnostic reviews found no remaining blocking findings.

Final tested source hashes are:

- `oanda_practice_live_dashboard.py`: `e032972735ec8f204ade000a389b824a515db34722fda1a237adbe7c8b3cd3ad`
- `oanda_main_signal_dashboard.html`: `8544b5c242deb7ee68c18e4d4e73488e498250de885e18aabce32f0897156c65`
- `oanda_study_diagnostic_report_v1.py`: `52f148bebeacd24565775ba593ef349556651dfa75b60691ef8ac9ed6ef6164e`

The secondary/expanded news UI paths share the same validated evidence cutoff and freshness rules, so opening an older panel cannot expose a stale direction as a current signal. The earlier publication-generation consistency reader remains unchanged.

The controlled dashboard-only reload passed at **18:32:59 UTC**, replacing the dashboard pair with PIDs **27724/11604**. Supervisor **22400** and all thirteen other workers' 26 process identities were preserved. The current API retained the unchanged registrations and disabled order/promotion flags.

Actual browser checks passed at widths **1440 and 360**, with zero JavaScript page errors and no page overflow. They exercised EUR/USD filtering, scoped news labels, the separately named companion, original scorer failure, and the supplemental denominators/results. The retained screenshots show a flat account with no open positions or pending orders. Two initial verifier issues were corrected without changing product source or repeating the reload: a case-sensitive assertion did not recognize CSS-uppercase “ORDERS DISABLED,” and a later assertion blindly toggled a details panel closed when persisted state had already opened it on mobile. `LIVE_UI_FIRST_FAILURE.json` preserves the first observed UI text and empty page-error list; the second verifier issue is documented here rather than mislabeled as a product failure. The final passing browser receipt and screenshots bind the tested source version.

The user-requested monitoring already exists as the active thread automation `check-forex-bot-health`, running every 15 minutes. It checks the API, workers, feeds, archive, pair coverage, news, H1 diagnostics, and account positions, and stays quiet unless there is a meaningful change, failure, or action needed. It preserves frozen study sources and disabled orders. This record adds no second automation and does not claim that a future check has run. The initial runtime monitoring records are `data/oanda_training_manager/research_monitor/checks.jsonl` and `baseline_v1.json`; they are operational data and are excluded from the source-only export.

The canonical evidence directory is `docs/validation/operational_status_repair_20260907`. It contains the feed/minute read-only audit evidence, source snapshots, final tests, runtime observation, and UI evidence. The dated report/receipt add two mappings for 149 local canonical vault records.

The previous dashboard-consistency receipt remains SHA256 `b84890295124ae0dc6a0dae522f7c2aa51e0db5b55302287f9a54859ec966d59`; prior pair coverage remains `6953f2d7259be748847ec0dd6a34abd1b2f482dd8be7aa6913bc7d906ff4cfb5`; prior signals/live remains `3671297affd5addd4c2e4080c66d4d74382c24678a3430d7e4694814e5993792`. Earlier failed and incomplete consistency probes keep their original contents and conclusions. The preceding source archive `forex_worktree_source_885144f85e85de600e4470d8.zip` remains SHA256 `dbdcd79d73a761c7b8f0e1d591d483878c05eb973b2668e0913e7e5e06e3881e`.

Export verifies local OneDrive bytes, not cloud synchronization or full private-database recovery. Better presentation and supplemental scoring do not establish improved predictions or permission to trade.
