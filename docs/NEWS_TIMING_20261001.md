# News refresh scheduling - October 1, 2026

The current reliability queue remains partial. This checkpoint isolates and
reproduces a scheduler defect; it does not claim a deployed repair.

The frozen v11 worker drains completed pair work at the end of `tick`, then
immediately schedules another pair. When completion happens during quote and
settlement work, an overdue shared-news refresh loses that scheduling opportunity.
The next tick sees the new pair in flight and defers news again. The actual tick
AST reproduces twenty consecutive pair dispatches with zero overdue news refreshes.

Calling `schedule_news()` between the final `finish_work()` and `schedule_work()`
corrects that reproduction. Five tests passed: original starvation, corrected
refresh priority, continued pair work when refresh is not due, preservation of
in-flight pair work, and no duplicate in-flight news capture. These are controlled
scheduler tests, not a live rollout or an evaluation of predictive accuracy.

Saved live diagnostics also show a fit dispatched with only 1.385 seconds before
the original news expiry. Actual-issue stale-news rejection must remain intact.
The observed counter is cumulative; it does not establish that every historical
error is a current collector outage. Duplicate market-reference refusals need
separate inspection of the exact ledger identity.

## Exact next action

Resume `live_news_input_reliability_v1`, finding NEWS-TIMING-01. Qualify deployment
of the minimal scheduler correction with explicit operational source lineage.
The worker hash is embedded in all 68 immutable ledger contracts. Merely editing
the worker and rehashing the registry fails activated-contract verification.
Preserve original contract bytes, activation records, models and forecast history;
do not silently relabel old cohorts or start replacement fits. Inspect the existing
operational migration mechanism before choosing a compatible successor boundary.

After deployment, verify news refresh completion and dispatch-to-issue timing over
multiple cycles. Then investigate any remaining near-expiry and duplicate-reference
attempts. Position-management qualification follows input reliability.

## Evidence and restore

Local: `evidence/news_timing_20261001/`. Shared packet:
`NEWS_TIMING_20261001` in the live Forex Vault. It contains original heartbeat
snapshots, source hash, findings, the proposed **not deployed** patch, test output,
work log and pending corrections. Run `python -B -m pytest
tests/test_joint_news_refresh_starvation.py -q` from the project root to reproduce.
The test reads the frozen worker source without importing or running models.

Same-task review only. Engineering diagnosis verified; runtime repair pending.
Forecast evidence unchanged; policy evidence unqualified; trading authorization
not granted. No service, model, ledger or other-task recorder was changed.
