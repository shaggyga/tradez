# News scheduler deployed - October 1, 2026

NEWS-TIMING-01 is repaired in the existing nontrading live worker. A separately
identified operational wrapper gives overdue shared-news refresh the scheduling
opportunity previously skipped after quote/settlement work. Original v11 source,
registry, all 68 ledger contracts, activations and previously saved forecast rows
remain unchanged. There is no replacement model cohort or historical relabeling.

The wrapper reuses the original numeric, causal, freshness and duplicate-reference
checks. Each new completed fit has an fsynced operational receipt linking its run,
scheduler/config hashes, original contract, forecast ID and payload hash. An
interrupted unrecorded gap stays unknown; subsequent processes do not claim it.
Operational populations must remain distinguishable in later analysis.

## Executed verification

- 78 deployment checks passed, including 12 scheduler/binding/receipt tests.
- The 12 scheduler tests also passed from a relocated 17-file source-only fixture
  copy. This is not a model or live-data restoration claim.
- All 68 original contract/activation records and pre-cutover forecast identities
  matched the captured baseline after deployment.
- At 14:38 UTC, three accepted news refreshes were observed, seven fit dispatches
  retained at least 208 seconds of original news validity, and six new forecast
  receipts matched their exact ledger rows. No failed completion was observed in
  that bounded window. The prior observed near-expiry dispatch had 1.385 seconds.
- Supervision reported `role_liveness_current`; the existing role's retry history
  and recovery expiry were retained. Counts were 61 forecast / 7 unavailable.

These observations establish the scoped deployment, not uninterrupted future
freshness, predictive superiority, full pair support or trading readiness.
The worker's normal authorized live computations continue; no replacement fits
or new research experiments were launched by this checkpoint.

## Resume and rollback

Resume `live_news_input_reliability_v1`: inspect the seven unavailable pair reasons
and any newly observed duplicate-reference or expiry refusals under the corrected
scheduler. Preserve unsupported cases and original strict gates. Do not repeat the
starvation diagnosis or refit models. Position-management qualification follows.

Local evidence: `evidence/news_scheduler_deploy_20261001`. Shared packet:
`NEWS_SCHEDULER_DEPLOY_20261001` in the live Vault. `BASELINE.json` binds all original
ledger identities; `VERIFICATION.json` and the captured scheduler receipt journal
bind live observations. Tests are `tests/test_joint_news_scheduler.py`,
`tests/test_joint_news_refresh_starvation.py`, and the operational recovery tests.

Rollback uses the exact owned-process procedure in `deploy.ps1`: stop only the
watchdog/supervisor and matching joint role, restore `contract.before` and
`profile.before`, validate, and restart through the qualified launcher. Keep all
ledger and receipt files. Never reset restart budgets. Other workers and the
other-task EUR/USD recorder are outside this cutover.

Same-task review, not independent review. Scoped engineering repair verified;
forecast scientific evidence unchanged; policy evidence unqualified; trading
authorization not granted.
