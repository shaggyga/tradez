# Vault-first shared work and reuse

User decision,2026-09-23: the Vault is the shared brain and directs both local projects. The main objective is to avoid rebuilding existing models, duplicating experiments or redoing engineering. Git must represent the complete runnable project version. Each person runs locally from the version and artifacts identified by the Vault.

## Mandatory start before implementation or computation

1. Resolve DESIGN_ALIGNMENT_LATEST.json, CHECKPOINT_REVIEW_LATEST.json and REVIEW_QUEUE.json. Read the shared coordination board and current handoff. Check partial work first.
2. Search existing code, model definitions, run records, data manifests, outputs and approved recipes. Start with [the existing-work discovery index](VAULT_FIRST_REUSE_20260923_141547/REUSE_DISCOVERY_INDEX.md), its [machine-readable records](VAULT_FIRST_REUSE_20260923_141547/REUSE_DISCOVERY_INDEX.json), and [existing model/run registries and saved artifacts](VAULT_FIRST_REUSE_20260923_141547/EXISTING_REUSE_NAVIGATION.md). This snapshot covers48 inherited approvals at MACRO_METER_20260923_042731; it is not a census of every saved model. A stale index or no search match is not proof that work is new.
3. Compare exact identities: objective/target, source and implementation, input content/vintages, features/transforms, splits, model/configuration, seeds, environment and parent experiment. Reuse the existing run fingerprint when present. A filename, machine path or different owner does not make a new experiment.
4. Completed matching work: retrieve the existing fitted model/output and verify hashes. Missing local files are a retrieval/synchronization problem. In-progress work: respect its owner and coordinate independent scope. Partial work: resume only the same identity and with ownership reconciled. Failed work: inspect its evidence before retrying. Materially changed work: record the change and link the predecessor before claiming a new identity.
5. Claim eligible new work on CHAT_COORDINATION_BOARD.md before edits/runs. If shared freshness or ownership is uncertain, leave the duplicate run blocked and continue unrelated eligible work. The synced board is advisory coordination, not an atomic distributed lock.

## Retrieval, reproduction and new fitting are different

A checkpoint may contain only source/inputs and a replay procedure that retrains models. It must not be labelled a saved trained model or invoked merely because another machine lacks outputs. Inspect archive contents and restore behavior first. Copy existing verified fitted artifacts and associated results whenever available. If those artifacts were never preserved, record the missing-artifact gap; do not silently refit.

An explicitly authorized reproducibility check may rerun computation. Label it replication_of the original run, explain why it is needed and keep it separate from new research evidence. Ordinary deployment/loading uses the existing fitted model. Run success, independent reproduction, review acceptance and trading authorization remain separate.

## Responsibilities

Vault owns orientation: design, queue, ownership, model/run/artifact references, decisions, evidence and exact next actions. Git carries the complete runnable source version: code, model definitions/configuration, tests, documentation, environment locks and artifact manifests, including the current engineering tree. Large data/fitted artifacts may use separate immutable storage identified from the Vault; a complete checkout/setup must retrieve the exact required versions. Credentials, machine settings and active databases stay local.

Local workers record owner, machine/task, code/input/model identities, run ID, state, last update, blockers and output hashes/locations. Publish compact results and artifact references back to the Vault; project logs link to the same record. Share definitions and existing results before creating replacements. A future automated registry must distinguish saved fitted artifacts, results-only evidence, reproducible recipes and unavailable artifacts, and provide one authoritative owner for each running identity.

## Current operational implementation

Git covers the whole Forex workspace. Resolve the actual published version and bundle
through SHARED_GIT_REMOTE_LATEST.json; resolve required checks and gaps through
OPERATIONAL_READINESS_LATEST.json. Git `artifacts/reuse_catalog.json` and
`docs/ARTIFACT_REUSE.md` record the bounded saved-artifact inventory and exact retrieval
methods. The registry distinguishes preserved weights/parameters from results, recipes
and missing/unshared files. Do not infer a missing model from a file extension alone.

The read-only preflight checks the declared local source, documents, environment,
ownership and requested artifacts. Local verification does not certify another
machine or OneDrive freshness. The shared board remains advisory; concurrent or
ambiguous ownership must be reconciled before potentially duplicate computation.

Catalog coverage and cross-machine qualification are explicit, not assumed exhaustive.
The scientific queue and original experiment identities remain authoritative.
