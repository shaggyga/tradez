# Frozen accounting operator

Scope: the synthetic reference/optimized accounting fixture only. No market model fitting, historical performance certification, API calls, broker actions or live service maintenance. Future campaign recipes must be added through engineering review. The numerical forecast models are unaffected by changing the Codex operator model.

An engineering handoff publishes OPERATOR_RECIPE.json and its external SHA-256. Do not regenerate this approval during ordinary operation. Verify the Vault manifest/pointer and obtain the recipe hash from its reviewed handoff. A changed source, recipe, predecessor or dependency requires escalation, not automatic resealing.

Use the isolated locked Python interpreter. On another machine, restore the verified checkpoint first; source/ and trad/ resolve under that restore root. Use a runs folder outside the Vault.

```text
python -I -B <source>/forex_operator_v2.py status --recipe <OPERATOR_RECIPE.json> --recipe-sha256 <approved-sha256> --runs-dir <runs> --trad-root <trad>
```

The same command accepts run, resume or verify in place of status. Run/resume only execute two fixed synthetic recipes, with one writer per run. Completed matching runs are verified without republishing. Run identity includes exact fixture inputs and scientific configuration. Partial status does not certify lock ownership or corruption-free recovery; the resume command performs those checks and refuses conflicts.

| Status | Operator action |
|---|---|
| ready | Invoke run with exactly the same arguments. |
| resumable | Invoke resume with exactly the same arguments. Never delete locks or alter run IDs. |
| completed_verified | Record the result and stop this recipe. Do not repeat the experiment. |
| review_required / nonzero exit / malformed output | Preserve stdout, stderr, paths, recipe hash and invocation. Escalate the concrete exception; do not modify scientific code, hashes, gates or dependencies. |

Routine maintenance means these diagnostics, verified resume and documented export/restore. Repairing code, changing economics, selecting models/thresholds, modifying schemas or accepting ambiguous evidence remains engineering work. No automatic model switching is performed. This interface is not an operating-system security sandbox; an operator with filesystem privileges must still follow the scope.

Every successful result keeps engineering_ready false, forecast evidence absent, policy evidence synthetic only and demo authorization not granted. This fixture cannot promote the full project. A supervised trial is still needed before claiming any specific cheaper model has been validated for this workflow.

## Review repairs: frozen v3 approval
The reviewed import closure includes the forex_system initializer and explicit absence of other parent initializers and shadow modules. Drift is refused before import. Export and restore execute status, run and verify against the actual packaged frozen approval. Routine operators must never regenerate approval to bypass drift. Cancellation requests before activation preserve execution eligibility until acknowledgment.
