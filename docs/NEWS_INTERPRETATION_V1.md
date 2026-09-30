# News interpretation repair — 2026-09-30

User requested better interpretation of captured news. The new `headline_interpretation` field in `trad/oanda_local_news_sentiment.py` preserves structured claims independently of published currency scores. Its implementation is `trad/oanda_news_interpretation_v1.py` and each output carries its own version and normalized headline hash.

A Fed action belongs to USD; an ECB action belongs to EUR. Explicit euro relief/weakness is separately labelled retrospective. Rate actions describe a conditional rate-differential channel, not a measured FX effect. Inflation without consensus retains unknown surprise. Oil retains potentially conflicting inflation/growth/terms-of-trade mechanisms. Opinions and already-reported pair moves are distinct. Modal/negated/ambiguous cases abstain within the supported grammar.

This is a bounded English-headline parser, not an LLM or complete semantic model. It does not interpret full articles, prove causal effects, assign calibrated magnitude/probability, or change legacy currency scores, clocks, corroboration or forward eligibility. Existing move-attribution consumers still use legacy scores. The running collector was not restarted; this source checkpoint is not a live deployment.

## Evidence and reproduction

Local evidence: `evidence/news_interpretation_v1_20260930/`. Nine selected archived headlines were read from SQLite in read-only mode. `archive_mapping_final.json` contains original event IDs, saved payload hashes, original first-seen timestamps and newly computed interpretations. Its interpretation time is NOW, never backdated to first-seen. This is neither a whole-feed evaluation nor historical decision-time replay.

Replay the argv in `archive_command_final.json` with this machine's Python, a retrieved archive database, and a NEW output path. The report refuses overwrite and missing IDs. No external calls, archive writes, fits or price-data access are needed.

Verification: 520 pytest tests and 9 subtests passed before the final rebound-label correction; 12 focused tests and 9 subtests passed afterward. Nine headline fixtures had exactly equal pre-existing classifier fields versus the preserved baseline. The unittest-only run did not collect pytest functions and is not counted as full regression coverage.

## Readiness and next action

Implementation: bounded additive interpretation implemented. Testing: offline checks passed. Review: same-task only. Scientific/trading readiness: not established. Project preflight remains blocked by existing source/pointer/environment mismatches; this does not claim a research gate passed.

Next: a labelled held-out corpus assessment of actor, polarity, modality and evidence type, followed by explicit historical-mapping consumer integration. Preserve original history; do not promote new interpretations into old predictions. Legacy numerical score attribution requires its own reviewed migration. Vault packet: `NEWS_INTERPRETATION_V1_20260930`; scoped pointer `NEWS_INTERPRETATION_LATEST.json`. The forecasting queue remains separate.
