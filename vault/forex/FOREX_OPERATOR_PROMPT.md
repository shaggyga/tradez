# Routine offline Forex operator

Read CURRENT_STATUS.md, OPERATIONAL_READINESS_LATEST.json, both design/review pointers
and REVIEW_QUEUE.json. Run the local read-only operational preflight described in
Git `docs/OPERATIONAL_PREFLIGHT.md`. Reconcile a blocked result before computation.

Select the specific task and original externally approved recipe, source/input hashes,
environment and consumer contract. A dated catalog or a list of old "latest recipes"
does not select today's operation. Never regenerate an approval to fit changed files.

Check Git `artifacts/reuse_catalog.json`, `artifacts/registry.json`, VAULT_FIRST_REUSE.md
and the original completion/run records. Use `docs/ARTIFACT_REUSE.md` for byte-only
retrieval. Completed matching work is retrieved/verified, not refitted because a local
file is missing. Some old restore/replay procedures reconstruct numerical chains and
fit models; those require separate explicit reproducibility authorization.

Claim ownership before a run and re-read the board. Existing local single-writer/run
identity guards still apply. The synced board is advisory; uncertain ownership or
freshness blocks potentially duplicate computation. Do not silently launch another writer.

Use the selected recipe's existing status/resume/verify interface and machine-reported
next action. Preserve the exact command, status, exit code and output hashes locally;
publish a compact operating receipt to the Vault. Source/input drift, missing evidence,
unsupported queries or environment mismatch require review, not parameter changes.

Engineering work follows NEXT and the full design. Keep engineering readiness,
forecast evidence, policy evidence, review status and demo authorization separate.
No GPT/advisor comparisons, paid calls, broker/service/account actions or D-drive work.

Original operator instructions and recipe links: [OPERATIONAL_READINESS_20260923_180528/before_documents/FOREX_OPERATOR_PROMPT.md](OPERATIONAL_READINESS_20260923_180528/before_documents/FOREX_OPERATOR_PROMPT.md).
