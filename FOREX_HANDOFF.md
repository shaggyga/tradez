# Forex handoff

The Vault is the shared brain; Git supplies the common source. Read the repository
[README](README.md), then use this machine's `thevault/projects/forex` path.
This page contains navigation, not a second copy of the current queue.

| Question | Authoritative Vault record |
|---|---|
| What must be finished before research? | `OPERATIONAL_READINESS_LATEST.json` and its `OPS_STATUS.json` |
| Which design/source is current? | `DESIGN_ALIGNMENT_LATEST.json` and its design, coverage, path-forward and source manifest |
| Which checkpoint was reviewed? | `CHECKPOINT_REVIEW_LATEST.json` and `REVIEW_QUEUE.json` |
| What is next? | `REVIEW_QUEUE.json`; finish required operational work and unresolved review findings first |
| Is somebody already working? | Active section of `CHAT_COORDINATION_BOARD.md` |
| Which Git commit/checkpoint is shared? | `SHARED_GIT_REMOTE_LATEST.json` |
| Does this model/run already exist? | `VAULT_FIRST_REUSE.md`, Git `artifacts/reuse_catalog.json`, and original run records |

Before implementation, read the active claim and run
[operational preflight](docs/OPERATIONAL_PREFLIGHT.md). Reconcile any blocked result;
do not bypass a missing dependency or replace an existing model. After preflight,
claim the exact scope on the Vault board and re-read it before edits or computation.
The synced board is advisory, not an atomic cross-machine lock. If ownership or shared
freshness is uncertain, defer duplicate computation and continue unrelated eligible work.

Use [artifact guidance](docs/ARTIFACT_REUSE.md) for byte retrieval. A recovery recipe
can refit; inspect its contract before treating restoration as saved-model retrieval.
The code/data/source/environment identities attached to a model remain binding.

During work keep `WORK_LOG.jsonl`, `PENDING_CHANGES.md`, raw command outputs and
baseline/after identities in the local step evidence directory. Follow the Vault
`CHECKPOINT_REVIEW.md`; publish a compact immutable packet and link it from
[project history](trad/FOREX_PROJECT_LOG.md). The
[legacy pending record](trad/FOREX_PENDING_IMPROVEMENTS.md) preserves context;
the live Vault queue sets order. [Directory map](docs/DIRECTORY_MAP.md) explains the layout.

Timed forecasting selection follows [forecasting continuation](docs/FORECASTING_CONTINUATION.md)
and the Vault FOREX_FORECASTING_CONTINUATION.md. A blocked experiment is local to its
dependents; continue supported siblings during the user's requested duration.

Timed launches and final-response checks use [run control](docs/TIMED_RUN_CONTROL.md).
The September 24 session is stopped by the user. Its residual checkpoint acceptance
was retracted; the current Vault review contains seven required corrections. Do not
restart the old session or count its idle gap as work.

For detailed research continuation and a two-hour launch prompt, use
[research handoff](docs/RESEARCH_HANDOFF.md), then the Vault
`FOREX_RESEARCH_PATH.md`. That path defines the next implementation acceptance gate
and progression toward indicator/layer comparisons and position rotation.

Read the full engineering design once, then relevant requirements and verified handoff.
Preserve original data, models, user edits and sealed packets. Passing tests, independent
review, forecast evidence, policy evidence and trading authorization are separate.
GPT/advisor comparisons, paid calls, broker/account/service actions and D-drive work
remain deferred. Operational readiness itself does not launch or authorize research.

Earlier notices and their exact bytes are retained in
[handoff history](docs/history/FOREX_HANDOFF_before_operations_20260923.md).
