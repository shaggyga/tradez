# Engineering for lower-model operation

The user explicitly requested that routine running and maintenance be designed for a cheaper Codex model. This is a permanent acceptance requirement for future Forex work packages, not a change to forecast model definitions or a request for GPT/advisor comparisons.

## Division of responsibility

Python owns numerical computation, frozen recipes, input validation, run identity, accounting, checkpoint verification, result metrics and acceptance predicates. The operator model invokes an approved recipe, reads the structured result, follows its next action and records the evidence. It does not decide whether a failed gate should count as passed.

Routine operator scope: inspect current verified status; run a registered offline recipe; resume the same identity after interruption; verify completed artifacts; export/restore a checkpoint; record actual outcomes and exact next action. Repeated run requests must verify an already complete matching result or resume an incomplete matching run, not silently create another experiment.

Maintenance scope: existing deterministic diagnostics and documented recovery actions. A source, dependency, schema or scientific-configuration change leaves the certified operating envelope. Such a change requires meaningful tests and a newly reviewed recipe before routine operation resumes. The cheaper model may collect the failure report and reproduction; it must not edit hashes, loosen tolerances, suppress failures or upgrade dependencies to manufacture a pass.

Engineering review scope: new research hypothesis, model/feature/target choices, changes to accounting or causality, unexplained discrepancies, unsupported inputs and promotion of research evidence. There is no automatic model switch or hidden API call. Escalation means returning a concrete evidence bundle and the unresolved decision to the user or subsequent engineering task.

## Required interface for every future computational package

1. Preflight binds exact code, dependency versions, numerical-model configuration, input inventory and data availability cutoffs. Paths may be machine-local; economic/scientific identity may not depend on the username.
2. A registered recipe fixes its dependencies, supported input tier, resource bounds, reproducibility expectations and acceptance tests. Arbitrary commands or natural-language experiment settings cannot be injected into an operating recipe.
3. Status is machine readable: ready, running, resumable, completed_verified, blocked or failed/review_required; report stable reason codes, evidence paths and one exact next action. Completed means all required artifacts and checks verified.
4. One writer owns each run. Resume checks identities and hashes before loading state; failures preserve evidence. No automatic fresh run to bypass a damaged or conflicting run.
5. The inspector reads original predictions/decisions and separates later outcomes. Reports retain all instruments and explicit unavailable cases, all attempts, and four readiness fields.
6. Recovery is tested from an unrelated folder using packaged source, fixture inputs and locked dependencies. A copied folder or successful import is insufficient.
7. Tests cover correct operation plus corruption, changed configuration, missing inputs, interrupted execution, false completion and unsupported requests. For numerical paths, reference comparison checks event-level outputs and independent invariants.
8. Each completed queue item adds its approved recipe, short runbook, failure table and exact resume instructions to the Vault. Preserve predecessor evidence and publish the latest pointer only after readback.

## Current certification boundary

The first operator handles the synthetic event-accounting workflow. It is a reusable pattern for later campaign recipes, not a claim that the full all-68 fitted research campaign already exists. The accounting engine includes partial fills, financing, capacity, independent arm state, resting-order events and a batched close kernel. Their passing checks are engineering evidence only.

Historical executable quotes, financing/margin provenance, causal fitted-consumer readiness, policy decision integration and populated technical/macro inputs still gate the complete campaign. The forthcoming HOLD/EXIT/REPLACE integration must expose this same operating contract before being called complete.

No lower-model capability comparison was run. A short supervised trial of the user's selected cheaper model against known run/resume/refusal cases should precede calling that specific model validated. Numerical replay tests validate the software independently of which model launches it. Advisor/GPT market comparisons remain deferred.

Official guidance supports setting a measurable accuracy target and then testing cheaper models against it: https://developers.openai.com/api/docs/guides/model-selection . This source informs model selection, not Forex economic acceptance.
