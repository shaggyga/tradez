# Forex design alignment — September 21, 2026

Read [PATH_FORWARD.md](PATH_FORWARD.md) first. It states the design goal, the actual current stage, the next coding batch, acceptance gates and the disposition of all 37 historical pending items.

The exact governing design is bundled in `specification/FOREX_CODEX_ENGINEERING_DESIGN.md`. Its SHA-256 is `61d9713034f6f058d9f59ff11c0dd05b2a7e36583dffb2cd6d6300c64b8dc374`.

- [Design coverage and reuse](DESIGN_COVERAGE.md)
- [Causality audit](CAUSALITY_AUDIT.md)
- [Operations and recovery audit](OPERATIONS_AUDIT.md)
- `RUN_STATUS.json`: the four separate readiness states and exact next work.
- `previous_documents/`: original bytes of the current-status documents changed by this reconciliation, with an inventory of hashes.
- `MANIFEST.json`: hashes of this documentation package's payloads. `PACKAGE_READBACK.json` records local copy/readback verification.

This package supersedes earlier next-step instructions that skip the unresolved integrity gates. Dated model results remain preserved as diagnostic evidence with the new limitations attached. GPT/advisor comparisons remain deferred.

This is an audit/documentation checkpoint, not a runnable Stage C recovery package or a full project backup. Use the separately scoped `RECOVERY_CHECKPOINT_20260921` for its declared source/input recovery. Do not start services, execute archived scripts, or assume cloud synchronization merely because files are present locally.
