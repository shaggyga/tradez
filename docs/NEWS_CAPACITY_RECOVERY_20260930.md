# Joint news transport capacity recovery

This operational correction reuses the existing raw news archive, projection
objects, publication receipts, consumer receipts and unchanged Ridge model.
It does not establish forecasting accuracy or authorize orders.

The old transport stopped at 19,953 retained objects: the next batch exceeded
its 20,000-object and 192 MiB compressed limits. Restarting it could not advance
the cursor. The original publication and observation stores were backed up before
cutover; their profiles, rows, hashes and original availability clocks are retained.
Local evidence: `evidence/joint_news_capacity_20260930/ORIGINAL_PREFIX.json`.

## Explicit successor policy

`trad/revision_news_capacity_policy_v1.py` verifies the exact original module
bytes and applies only its enumerated process-local resource limits. The old
workers do not import it. Both successor transport V7 and IO V13 bind the policy
source in their complete source graphs and reject unexpected runtime values.

The immutable store profiles continue to identify the original serialization
and receipt implementations. New execution provenance additionally requires the
successor transport profile / IO capture source graph, which includes the capacity
policy. An old store profile alone does not identify this execution policy.
Historical captures remain historical; no old publication is relabeled as new.

| Bound | Successor |
|---|---:|
| Evidence objects | 65,536 |
| Compressed evidence | 768 MiB |
| Expanded evidence prefix | 8 GiB |
| Publication database | 1.5 GiB |
| Short raw read transaction | 5 seconds, at most 3 attempts |
| Nonauthorizing cold publication validation | 600 seconds |
| Bootstrap phase | 630 seconds |
| Current capture outer limit | 30 seconds, unchanged |
| News freshness | 300 seconds, unchanged |

Per-object limits, incremental expansion per step, corruption checks, original
clock ordering, failed-acknowledgment rules and forecast gates are unchanged.
The larger raw-read allowance addresses a measured 0.89-second read of the old
200 MiB store, already close to its former one-second limit under contention.
Successful backlogged scans use a two-second minimum scheduling interval;
ready scans and failures retain the sixty-second cadence/backoff. Cycles remain
serial. This is finite capacity, not a retention deletion or an infinite store.

## Runtime identities

- Transport config: `trad/config/revision_transport_capacity_v1_20260930.json`.
- Joint IO: `trad/config/revision_news_io_capacity_v1_20260930.json`.
- Joint registry: `trad/config/joint_price_news_capacity_v1_20260930.json`.
- Active study: `trad/data/oanda_training_manager/operational_news_capacity_20260930/bounded_capture_v2/joint_price_news_study_v10`.
- Runtime profile / launcher: see [pipeline operations](PIPELINE_OPERATIONS.md).

The first empty capacity candidate is retained in the parent directory, with
its original source/config bytes under local `attempt1_source`. It produced no
forecasts and is not the selected cohort. The bounded-capture revision has new
cohort identities and a new empty activation; no forecast rows were imported.

The OS recovery task owns the durable launcher. The recovery expiry remains
2026-10-07T08:14:50Z. The transport and joint successor inherit the predecessor
restart-budget keys; a version change does not erase failures or grant unlimited
restarts. Other collectors retain their original roles and data ownership.

## What must be verified

Check separately: fresh raw news; successful transport cursor advance; an actual
caught-up scan; successful fresh joint capture; model readiness; current issued
forecasts; dashboard readback. A fresh worker heartbeat alone proves none of the
later stages. Use the final receipt and live status, not this description, for
the measured completion status.

The unchanged joint model requires at least 48 mature quarter-hour training
origins, 12 nonzero-context rows and 8 distinct context patterns. A long collection
or transport gap can require new causal coverage before it can forecast. Catching
up old articles now cannot make them available at past decision times. These
requirements must not be lowered or neutral-filled to make the dashboard green.

Same-task verification is labeled separately from independent review. Original
source evolution, executed tests, exact prefix preservation and runtime readback
belong in the step evidence and its compact Vault packet.
