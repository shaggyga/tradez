# Current Forex integrity checkpoint and handoff

Read [current status](RUN_STATUS.json), [ordered queue](PATH_FORWARD.md),
[batch 04](../ALL68_INTEGRITY_BATCH_04_20260921.md) and the [exact design](specification/FOREX_CODEX_ENGINEERING_DESIGN.md).
The original full R/WP/TST mapping is retained under `audit/`; its historical
assessment is supplemented by batch 04, not reclassified as newly executed tests.

## Checkpoint scope and machine bootstrap

`checkpoint/forex_integrity_20260921_210502.zip` is the runnable issuance/recovery
fixture. Verify its external SHA-256 before extracting any bootstrap files:
`6a7dcd2b8b02c98bbbe79f23b6fc59f0a36136ed09cca7ed6df679e2e9cc0167`. Use Python 3.12.10 with numpy 2.5.1, pyarrow 25.0.0,
and tzdata 2026.3, exactly as recorded inside `DEPENDENCIES.json` and
`requirements.lock`. Provisioning a new machine is separate from this run;
no global environment was changed here.

On another machine, hash-check the ZIP with Get-FileHash, extract the trusted
matching ZIP into a new empty bootstrap folder, and run its
`source/portable_checkpoint_v2.py` with the provisioned interpreter:

```text
python -I -B <bootstrap>/source/portable_checkpoint_v2.py restore --package <zip> --sha256 6a7dcd2b8b02c98bbbe79f23b6fc59f0a36136ed09cca7ed6df679e2e9cc0167 --destination <new-empty-work-folder>
```

The tool checks all member names/hashes, exact dependencies and a fresh runner
subprocess. Accept only `RESTORE_RECEIPT.json` with VERIFIED and replay parity.
It was tested in `C:\Users\zmoor\Documents\forex_recovery_verification_20260921_210502`
and again using the restored tool into `forex_recovery_bootstrap_20260921_210502`.
Neither replay imports the original Stage C source or reads the historical ZIP.

`source_snapshot/` contains the broader Stage C source/test handoff. It is not
the runtime ZIP allowlist. The new reference adapter can run using the two-file
`predecessor_source/trad` subset included here; its unrelated-folder report
matched byte for byte. Some older feature/macro audits still require the separately
recovered `trad` tree. To replay the accounting fixture, use a new output path:

```text
python -I -B <addendum>/source_snapshot/reference_accounting_adapter_v2.py --trad-root <addendum>/predecessor_source/trad --output <new-output.json>
```

This is synthetic accounting only, with no historical execution, financing,
margin or performance certification. The previous project recovery checkpoint at
`../RECOVERY_CHECKPOINT_20260921/` retains historical source and bulk data within
its own manifest; it does not by itself contain this Stage C successor. Current
runtime code remains isolated in the original workspace's
`stage_c_alignment_integrity_v2`; there was no deployment into `trad`.

## Commands on the original machine

```powershell
& 'C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2\verify_alignment_integrity.ps1'
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -I -B 'C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2\all68_neutral_runner_v2.py' --run-id all68-neutral-repair-20260921-210502 --resume --report-only
```

The second command verifies a completed run and returns without rereading the
archive. New source/config/input identities require a new run ID; never force
old identities to match or overwrite old outputs. Actual process-death resume
was verified in synthetic child processes at multiple publication boundaries.

## Next work

Step D / WP2-WP8: extend the reviewed synthetic reference-accounting adapter through remaining event-level parity, partial fills/closes, financing, margin/reservations and currency exposure, with isolated policy state and explicit input-tier blockers.

Continue through `FOREX_NEXT_PROMPT.md` in the Vault. Resolve equivalent machine
roots explicitly; original-machine absolute paths in evidence are provenance,
not portable identity fields. Readiness remains false for the full design;
GPT/advisor comparisons remain deferred. All 37 pending IDs remain mapped in
PATH_FORWARD.md. Local byte readback does not establish OneDrive cloud sync.
