# Rolling price/news pipeline — September 30

Joint V11 consumes the validated current news snapshot, parses it once per refresh,
and retains compact sentiment frames in a rolling index. It no longer needs to
replay the growing transport archive before reading current news. This is the
user-authorized operational repair, not a claim of improved forecast accuracy.

## What changed

`rolling_news_io_v1.py` authenticates the existing repair-v2 snapshot and its
collector/clock evidence. It reuses the original guarded aggregation and eight
news features. The index serves the latest 48 hours and retains 72 hours; durable,
content-addressed compressed records remain outside that hot index for audit.
The 30-second capture limit and five-minute news freshness limit remain enforced.
Missing, expired or not-yet-observed sentiment is unavailable, not neutral sentiment.

`rolling_news_history_v1.py`, point V6 and inputs V7 supply causal histories to
the original numerical model through ledger V9 and worker V11. All 68 registered
pair contracts retain their original numeric parameters, maturity requirements,
target definitions and no-orders flags. Ledger calculation methods are unchanged.
The old transport stores and V9/V10 studies remain intact as historical evidence.

This is a new prospective cohort. No historical observations were imported, no
availability timestamps were backdated and no old forecasts were relabeled.
It needs at least 48 mature quarter-hour origins with H1 labels, 12 nonzero news
context rows and eight distinct context patterns. Continuous capture alone does
not guarantee those support gates. Even perfect collection needs roughly 13
hours to accumulate the time-based minimum from an empty cohort; context or
quote gaps can take longer. Zero forecasts during that warmup is expected.

## Local runtime and reproducibility

- Registry: `trad/config/joint_price_news_rolling_v1_20260930.json`.
- News routing: `trad/config/rolling_news_io_v1_20260930.json`.
- Cohort: `trad/data/oanda_training_manager/operational_rolling_news_20260930/joint_price_news_study_v11`.
- Hot index: the sibling `rolling.sqlite`; retained parsed/raw audit objects: `archive/`.
- Activation: `activation_receipt.json` under the cohort; each pair has its own ledger.
- Current heartbeat, summary and readiness records live under that same cohort.
- Canonical status/start/validate commands: [pipeline operations](PIPELINE_OPERATIONS.md).

The selected profile contains 17 roles. It removes the retired transport role and
replaces joint V10 with V11. Supervisors preserve the retired transport retry
history and the existing joint retry budget. Automatic cooldowns are not reset.
Other collectors, the other chat's EUR/USD recorder and account/order processes
are outside this cutover. Recovery still expires October 7 at 08:14:50 UTC.

Technical availability reader V2 also binds the exact September30 resource-only
configuration. Its predecessor was rejecting all observations as
`original_operations_runtime_binding_changed` despite the dataset updating.
All reader functions and imported numerical source hashes are unchanged. A real
read on September30 at 16:34 UTC verified 57 complete, seven partial, three
not-tradeable and one stale-quote pair, with no runtime-binding rejection.

On this machine the archive can reconstruct a retained capture using the exact
selected source/configuration and descriptor. For example, with `trad` on
`PYTHONPATH`, use `rolling_news_io_v1.create_session(config)` followed by
`rolling_news_io_v1.replay_capture(session, descriptor)`. Replay verifies the saved
snapshot, parsed frame and hashes; its opaque handle cannot authorize a fresh
forecast. Use production descriptors retained in worker input evidence, not
synthetic capacity fixtures. A new capture uses the current producer and clock.

Git carries the implementation, tests and configuration identities. Runtime data,
activation receipts and credentials are not a portable activated system. Another
machine must retrieve exact artifacts or explicitly qualify a new prospective
cohort with local paths; do not regenerate a model because data is missing locally.
Consult the live Vault first. Never call fresh activation on an existing study.

## Verification and limits

The local evidence directory is `evidence/joint_news_readiness_20260930` and the
compact Vault packet is `ROLLING_NEWS_PIPELINE_20260930`. Tests cover corruption,
staleness, actual producer parsing, opaque capture ownership, causal historical
coverage, replay refusal for fresh issuance, durable archive retention, native
training gates, exact runtime role selection and dashboard source/activation checks.
Additional checks compare native ledger calculations and all 68 registry contracts
against their preserved predecessors.

A synthetic 48-hour capacity fixture with 2,882 observations completed capture in
8.828 seconds and projected 192 origins in 0.046 seconds. This is capacity evidence,
not historical model or accuracy evidence. The initial real snapshot test took
3.156 seconds. Live deployment readbacks and final test counts are recorded in the
packet, separately from these fixtures. Review is same-task, not independent.

The September30 interpretation/timing successors remain separate offline work.
This repair does not establish incremental predictive value, authorize trading or
resolve the scientific queue's preflight findings.

## Currency ranking status

The Practice-006 currency-rank challenger was already retired in the project log.
Its retained heartbeat has an August 24 ranking and `session_loss_limit`; that is
historical state, not today's live performance. The source-conditioned V7 ranker
has a September 5 record with zero decisions and zero matured outcomes. Its V8
configuration disables collection. No matching currency-ranking worker was found
in the September30 process inspection. Neither ranker was restarted by this repair.
Those paths are distinct from the offline currency-projection research studies.

Final native verification at2026-09-30T16:54:37Z passed all11 deployment checks:
17 managed roles running, exact current controller/profile, fresh rolling records,
zero joint worker errors, valid technical reader and current V11 dashboard.
The dashboard admitted65 price-forecast pairs and zero joint forecasts.137 checks
passed in the exact restored source tree. Actual ten-record capture replay passed.
See the immutable Vault packet for counts and remaining prospective warmup.
