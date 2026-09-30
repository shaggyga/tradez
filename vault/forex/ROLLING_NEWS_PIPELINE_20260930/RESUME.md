# Rolling pipeline handoff

Source c3b530d66e4bf3c33740537cfdc7310163d5bf0b; scope reviewed by same task, not independently.

The retained replay stall has been replaced by bounded current-snapshot parsing.
The original 30-second capture, five-minute news freshness and numeric model gates
remain. Actual native-worker readback is NATIVE_FINAL.json. Do not infer model
accuracy or trading readiness from collection. New joint forecasts remain gated by
48 mature quarter-hour H1 rows,12 context rows and8 patterns; roughly13 hours of
uninterrupted collection is a minimum, not a promise. Missing quotes/support remain
visible. No old observations were imported.

Next operational item: joint_v11_mature_forecast_verification_v1. Read the canonical
runtime status and jointV11 heartbeat/summary/readiness. Verify rolling.sqlite
observations still advance, exact registry/source/activation bindings remain valid,
then inspect actual mature training counts, issued/target clocks and native outcomes.
Do not restart into a new cohort, refit old base models, lower gates or re-run the
completed capture repair merely because warmup has not finished.

Runtime: trad/config/operational_runtime_current_20260930.json; selected news config
rolling_news_io_v1_20260930.json; joint registry joint_price_news_rolling_v1_20260930.json.
Cohort root: trad/data/oanda_training_manager/operational_rolling_news_20260930/
joint_price_news_study_v11. Sibling rolling.sqlite/archive preserve current parsed
observations and audit objects. Recovery expiry remains2026-10-07T08:14:50Z.

Technical availabilityV2 binds reviewed resource-only runtime configs and original
feature sources. It preserves partial/missing data; originalV1 history remains.
RankerV7 ledger contains zero decisions/forecasts/outcomes; no eligible prospective
source rows in its retained inventory. V8 collection disabled; Practice006 retired.
No ranking worker was started. This is separate from offline currency projection.

Scientific queue unchanged: extra_trees_matched_development_comparison_v1 remains
source-only partial behind its own authorization/preflight requirements. Current
global research pointer-schema/coverage findings, old repairV1 defects and independent
review remain outside this operational repair. Other-chat EURUSD capture, accounts,
orders, paid APIs, GPT comparisons and D drive were untouched.

Full local work: evidence/joint_news_readiness_20260930. Git carries source and
versioned Vault knowledge, not production databases or credentials. Exact source
restore is SOURCE_RESTORE.json; it is not a claim that runtime artifacts were restored.
