Current design-order decision: WP6 retained text layer is ready for review at [MACRO_TEXT_LAYER_20260922_135019/REVIEW.md](MACRO_TEXT_LAYER_20260922_135019/REVIEW.md). Read [MACRO_TEXT_LAYER_20260922_135019/FULL_DESIGN_QUEUE.md](MACRO_TEXT_LAYER_20260922_135019/FULL_DESIGN_QUEUE.md). Preserve the older warm partial; it does not block independent offline WP6 work. New recipe is not independently reviewed.

**Current operations index:** [OPERATIONS_CATALOG_20260922_125442/operations/OPERATIONS.md](OPERATIONS_CATALOG_20260922_125442/operations/OPERATIONS.md). Current source and historical-only recipes are distinguished; use the pinned recipe and its original recovery instructions.

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

Frozen offline recipes now cover the original bounded accounting/policy chain, causal technical and rich inputs, matched ridge/HGB families, remaining-target preparation, original-record inspection, populated rolling registries, paired/blocked dependence diagnostics, retained official context, nuisance/interaction controls, sampled-close path reconciliation, whole-origin capacity characterization, modeled-clock residual calibration and its outcome-separated inspector, and isolated remaining-target joint readiness. The current operations catalog distinguishes pinned source-compatible recipes from historical-only approvals and links exact recovery archives and companions. It does not execute commands or replace runtime preflight.

Current limitations are substantive: the shared whole-curve path met two seconds on only8/20 original measured origins; strict24h/2day/5day contiguous sampled paths have no support in the retained slices; calibration uses modeled issuance and assumed label clocks; remaining-target readiness covers an isolated8-fit workload and all native packets remain unissued. Qualified session calendars, causal macro surprise/vintages, historical execution/financing provenance, wider target/model families and protected confirmation remain open. Independent review is unperformed. Same-host relocation does not certify another machine, cloud sync or a particular cheaper model.

No lower-model capability comparison was run. A short supervised trial of the user's selected cheaper model against known run/resume/refusal cases should precede calling that specific model validated. Numerical replay tests validate the software independently of which model launches it. Advisor/GPT market comparisons remain deferred.

Official guidance supports setting a measurable accuracy target and then testing cheaper models against it: https://developers.openai.com/api/docs/guides/model-selection . This source informs model selection, not Forex economic acceptance.
