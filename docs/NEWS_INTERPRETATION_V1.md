# News interpretation repair — 2026-09-30

User requested better interpretation of captured news. Structured claims are available through `classify_article_with_interpretation` in `trad/oanda_news_interpretation_adapter_v1.py`. This opt-in adapter preserves every original classifier field and appends `headline_interpretation`. The original collector remains byte-identical to the qualified joint-producer dependency; direct collector callers do not receive the new field. Its implementation is `trad/oanda_news_interpretation_v1.py` and each output carries its own version and normalized headline hash.

A Fed action belongs to USD; an ECB action belongs to EUR. Explicit euro relief/weakness is separately labelled retrospective. Rate actions describe a conditional rate-differential channel, not a measured FX effect. Inflation without consensus retains unknown surprise. Oil retains potentially conflicting inflation/growth/terms-of-trade mechanisms. Opinions and already-reported pair moves are distinct. Modal/negated/ambiguous cases abstain within the supported grammar.

This is a bounded English-headline parser, not an LLM or complete semantic model. It does not interpret full articles, prove causal effects, assign calibrated magnitude/probability, or change legacy currency scores, clocks, corroboration or forward eligibility. Existing move-attribution consumers still use legacy scores. The running collector was not restarted; this source checkpoint is not a live deployment.

## Evidence and reproduction

Local evidence: `evidence/news_interpretation_v1_20260930/`. Nine selected archived headlines were read from SQLite in read-only mode. `archive_mapping_final.json` contains original event IDs, saved payload hashes, original first-seen timestamps and newly computed interpretations. Its interpretation time is NOW, never backdated to first-seen. This is neither a whole-feed evaluation nor historical decision-time replay.

Replay the argv in `archive_command_final.json` with this machine's Python, a retrieved archive database, and a NEW output path. The report refuses overwrite and missing IDs. No external calls, archive writes, fits or price-data access are needed.

Verification: 520 pytest tests and 9 subtests passed before the final rebound-label correction; 12 focused tests and 9 subtests passed afterward. Nine headline fixtures had exactly equal pre-existing classifier fields versus the preserved baseline. The unittest-only run did not collect pytest functions and is not counted as full regression coverage.

## Readiness and next action

Implementation: bounded additive interpretation implemented. Testing: offline checks passed. Review: same-task only. Scientific/trading readiness: not established. Project preflight remains blocked by existing source/pointer/environment mismatches; this does not claim a research gate passed.

Next: a labelled held-out corpus assessment of actor, polarity, modality and evidence type, followed by explicit historical-mapping consumer integration. Preserve original history; do not promote new interpretations into old predictions. Legacy numerical score attribution requires its own reviewed migration. Vault packet: `NEWS_INTERPRETATION_V1_20260930`; scoped pointer `NEWS_INTERPRETATION_LATEST.json`. The forecasting queue remains separate.

## Compatibility follow-up

The initial additive integration changed a source hash pinned by the joint producer. The September30 follow-up restored the exact pinned collector from Git, without editing registries or model identities, and moved opt-in integration to the adapter. New interpretations are computed now, not backdated to article first-seen. Existing offline archive reports continue to call the interpreter directly. No live collector was restarted. See Vault `DASHBOARD_PRODUCER_QUALIFICATION_20260930/REVIEW.md`.
