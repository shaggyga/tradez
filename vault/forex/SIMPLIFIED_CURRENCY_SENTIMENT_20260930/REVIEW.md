# Current currency news context

The typed news interpretation is now published by `trad/oanda_currency_news_context_v1.py` and consumed by the main dashboard. It replaces raw long/short news labels in the current pair table and news summary. Original collector scores, archives, guarded model inputs and model cohorts remain intact for reproducibility; this release does not silently change learned forecast features.

## Corrected interpretation

The worker reuses the existing headline interpreter and adds explicit policy guidance, timing and expectation changes. Williams's “no urgency” becomes `tightening_not_urgent`; Barr's further-hike guidance becomes `further_tightening`; cooling inflation tempering hike concerns becomes `tightening_expectations_easing`. These are distinct textual claims, not USD return scores.

Production, output and shipment quantities are separate from energy prices. Reported easing of a blockade is retained as a supply-constraint change without synthetic CAD/JPY/MXN/NOK directions. Negation and hypothetical wording remain uncertain. Explicit currency reactions remain retrospective. An unspecified “dollar” is not silently assigned to USD. The PCE example retains the reported dollar decline, AUD relative underperformance and explicit GBP rally separately.

This remains bounded English headline interpretation. Ambiguous subjects and unsupported formulations remain unresolved. Six previously audited examples are regression cases, not an independent accuracy sample or proof of predictive value.

## Flow and durable records

Existing collector SQLite (read-only) → typed parse-once cache → current context JSON → dashboard.

Runtime directory: `trad/data/oanda_training_manager/currency_news_context_v1/`:

- `context.sqlite`: exact interpreted text, parser/source identity, actual computation time, and separate input-provenance observations. Identical material text within the same parser version is reused; duplicate publisher records do not count as independent directional evidence.
- `current.json`: bounded current publication, collector freshness, original article clocks, processing duration, cache counters, source bindings and payload digest. Raw scores remain in this local audit output but are omitted from the corrected dashboard context response.
- `owner.sqlite`: exclusive local writer lock; a second worker cannot write concurrently.

Each 15-second poll selects at most 500 relevant source records published within 24 hours; selection truncation is explicit. Up to 100 distinct topics are exposed to the dashboard, which shows a smaller summary and pair-relevant claims. This is a bounded context view, not a complete news-coverage census. The separate cache stops at 128 MiB instead of deleting evidence silently. Source queries have a three-second bound; publications have a two-MiB bound and atomic replacement.

The actual interpretation computation time is never backdated to the source publication or first capture. Timeliness does not change the parse identity. Changed text/parser creates a new version; distinct source provenance is separately retained. Loaded-source changes are refused instead of relabeling old in-memory code with a new hash. A pinned predecessor prevents implicit interpreter replacement.

The native recovery profile explicitly enables the optional `currency_news_context_v1` role. Recovery still expires at `2026-10-07T08:14:50Z`; existing retry history and model workers are preserved. No new external feed, paid request, GPT call, broker action or trade is introduced. This worker only reads already captured news.

## Verification performed

- 126 staged tests and 13 subtests passed across semantics, runtime selection/refusal, recovery, rolling compatibility, predecessor interpretation and dashboard qualification.
- After substantive review additions, 27 final focused tests passed, including unspecified-dollar handling, future-clock refusal and loaded-source identity checks. These overlap the broader run and must not be added as unique test counts.
- The real CLI consumed 150 source rows in 0.262 seconds during the initial smoke check. The native publisher subsequently read 153 records and cached 123 distinct interpretations; a verified later poll took 0.104 seconds with zero new interpretations. These are observed samples, not an end-to-end feed latency guarantee.
- Live native supervisor PID5924 reported context worker PID25132 healthy and bound to the exact18-role profile. Actual native-store records for Williams, Barr, cooling inflation and production matched the repaired claims.
- Across two real publications, all100 shared displayed interpretation timestamps stayed unchanged. HTTP returned the corrected current context while research collection remained running and source-qualified.
- Served JavaScript passed syntax checks. A Node render check using actual HTTP data verified corrected rendering, escaped hostile headline text and a clean unavailable state. No browser visual inspection is claimed.

Review is substantive same-task review, not independent approval. Global historical preflight design-pointer/review-schema findings remain recorded; no new fitting or scientific acceptance is claimed.

## Resume and rollback

Evidence: `evidence/simplified_sentiment_current_20260930/WORK_LOG.jsonl`, `PENDING_CHANGES.md`, test transcripts, deployment before-images, source hashes, actual store/HTTP receipts and selected-case replay. Shared packet: `SIMPLIFIED_CURRENCY_SENTIMENT_20260930` in the live Forex Vault. Project logs and the current Vault pointer resolve the matching Git checkpoint.

Next operational work: reduce publication-to-capture delay by separating prompt official/FX polling from slow discovery after dependency analysis; integrate current-root move mapping with actual as-of technical availability. Forecast integration requires an explicit prospective feature/cohort contract and matched evaluation. Do not splice these newly computed text claims into old forecast history or relabel the old model as improved.

Rollback must stop the exact context worker, restore the previous helper/runtime/display bytes from the deployment before-images, validate that exact profile and restart the owned controllers/dashboard. Preserve the new context database and all original archives. Do not merely delete the new module while it remains selected in the runtime or dashboard source manifest.
