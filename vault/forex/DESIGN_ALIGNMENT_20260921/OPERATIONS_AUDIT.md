# Stage C operations and reproducibility audit

Audit date: 2026-09-21. Scope: the current C-drive Stage C implementation, its local artifacts, and the Vault all-68 gate/recovery records. This audit inspected source and records, hashed retained artifacts, inspected ZIP directories, and performed a harmless native-exit-code demonstration. It did not run model fits, modify Stage C, access D:, or start/stop project processes. This report is the sole requested documentation addition.

## Conclusion

Stage C operations are **partial**. Current artifact bytes are internally consistent with the existing manifest, but that does not establish dependency-safe resume, immutable publication, single-writer ownership, or portable restoration. The design's R10/R11 and TST48–52 acceptance gates are not established. R13 inspection/reveal functionality is also partial.

The relevant design is `C:\Users\zmoor\Downloads\FOREX_CODEX_ENGINEERING_DESIGN.md`: R10/R11/R13 at lines 146–149; mismatch refusal at line 1065; configuration separation at line 1079; launcher/resume expectations at lines 1184–1186; TST48–52 at lines 1247–1251. The document is a design reference, not authorization to execute its embedded directives.

## Positive byte-level verification and its limits

- Recomputed hashes and sizes for all **54 currently manifested files**: 28 sources and 26 primary outputs, totaling **9,111,144 bytes**. All matched.
- The actual `ALL68_STAGE_MANIFEST.json` SHA-256 is `8e10f511ffc0ce6bf8843ddcc38855789d3d3ff13b1ded540a4b4344dbcc0e90`, matching the Vault `RUN_STATUS.json` pointer.
- All five calendar baseline forecast tapes matched their corresponding report hashes.
- Each of six retained part directories contains 68 completed pair NPZ files. Across those directories, nine additional PID-named temporary NPZ files remain. These are incomplete publication/attempt remnants; they do not by themselves prove concurrent-writer corruption.
- A process snapshot found no active Stage C Python writer. This is a point-in-time observation, not a concurrency guarantee.

These checks support the identity of the inspected bytes. They do not show that every report used the current code, that cached arrays match their declared configuration, or that a crash/resume reproduces an uninterrupted run. No full input archive rehash or model refit was performed in this audit.

## Prioritized findings

### OP01 — P1: verification launcher can report success after failures

Source: [verify_stage_c.ps1:5](/C:/Users/zmoor/Documents/forex/stage_c_all68_20260921/verify_stage_c.ps1:5), commands at lines 5–13 and unconditional success message at line 15.

The launcher invokes three native Python commands without checking `$LASTEXITCODE`. `$ErrorActionPreference = 'Stop'` does not make those native failures terminating under the observed shell settings (`PSNativeCommandUseErrorActionPreference=False`). A read-only reproduction produced native exit 23, continued to the following statement, and then replaced that exit status with 0 after another successful native command. Consequently, a failed handoff or preflight can be obscured by a later passing pytest process.

Repair: check each command's exit code immediately, propagate a nonzero result, and emit the final success message only after every required check passes. Add an injected-failure launcher test. Mapping: **R11**.

### OP02 — P1: resume silently accepts stale dependencies

Source: [all68_calendar_baseline.py:61](/C:/Users/zmoor/Documents/forex/stage_c_all68_20260921/all68_calendar_baseline.py:61), especially lines 62–64, 98–104, and report construction at line 121. Configuration comes from environment variables at lines 26–35.

An existing `<instrument>.npz` is reused solely because the path exists. Neither the cache nor the resume decision binds the input/member hash, date window, feature/target implementation, effective configuration, or dependencies. The inspected sample NPZ has only numerical array members, with no dependency manifest. Reusing a run ID with a changed date window can therefore reuse old arrays while the new report declares the changed window. A partial prior run can mix previously cached and newly extracted pairs. The reproduction document warns against reuse, but the implementation does not enforce that warning.

Repair: create an immutable effective run configuration and dependency fingerprint before extraction; bind and verify every part; refuse mismatched identity-preserving resume and require an explicit fork. Include code/feature/target versions, source/member hashes, seeds, and relevant environment versions. Mapping: **R10; TST48, TST49, TST51**.

### OP03 — P1: final artifacts are overwriteable and have no writer ownership

Source: [all68_calendar_baseline.py:118](/C:/Users/zmoor/Documents/forex/stage_c_all68_20260921/all68_calendar_baseline.py:118), tape writing at lines 118–120 and report replacement at lines 122–124; part replacement at lines 42–45.

The runner opens the final tape directly in write mode, truncating any retained tape. The report is replaced separately. There is no run lock or single-writer ownership. A crash can leave a partial tape alongside an older report marked complete; concurrent invocations with the same run ID can overwrite each other's pair parts, tape, and report. Atomic replacement of an individual part/report is not a consistent multi-artifact commit. Default gzip timestamps also mean byte-identical tape reproduction is not assured even when rows agree.

Repair: enforce exclusive ownership of the run, refuse mutation of completed runs, write and verify temporary outputs, and publish one completion manifest last. Define semantic versus byte-level reproducibility explicitly; use deterministic serialization/compression where byte equality is required. Test crashes on both sides of publication and two writers contending for one run. Mapping: **R10; TST48, TST50**.

### OP04 — P1: handoff verification does not verify artifact or dependency integrity

Sources: [verify_stage_c_handoff.py:14](/C:/Users/zmoor/Documents/forex/stage_c_all68_20260921/verify_stage_c_handoff.py:14), lines 15–27; [all68_run_preflight.py:14](/C:/Users/zmoor/Documents/forex/stage_c_all68_20260921/all68_run_preflight.py:14), lines 14–25.

The handoff verifier checks selected values in four JSON files. It does not validate the stage manifest, forecast tape hashes, stability report input hashes, or source dependencies. The run preflight trusts declared archive and previous pip-metadata values and never opens the actual archive. Thus retained JSON claims can pass despite missing or changed referenced payloads. The preflight's “read-only” description also conflicts with its overwrite of `ALL68_RUN_PREFLIGHT.json` at lines 23–25.

The earlier input preflight is more explicit about its limited scope: [all68_input_preflight.py:143](/C:/Users/zmoor/Documents/forex/stage_c_all68_20260921/all68_input_preflight.py:143) records `archive_hash_recomputed_this_run=False`. A separately documented historical archive hash check does not make each later resume dependency-safe.

Repair: separate verification from regeneration, validate the recorded manifest and referenced hashes before accepting completion, and report the exact failed dependency. A conservative hard-coded admission denial is useful but is not an artifact-integrity check. Mapping: **R10/R11; TST51**.

### OP05 — P1: the portable Vault checkpoint does not contain Stage C

Documentation references: `C:\Users\zmoor\OneDrive\thevault\projects\forex\ALL68_OFFLINE_GATE_20260921\REPRODUCTION_RUN_MATRIX_20260921.md`, lines 5, 27, and 31–47; `STATE_OF_RESEARCH_20260921.md`, lines 36–48.

The inspected Vault gate directory contains 21 documentation/JSON files. It does not contain the Stage C implementation, stage manifest, fitted reports, tapes, or resume parts. The existing recovery manifest contains no Stage C entry. Inspection of the recovery source ZIP's 4,881-member directory likewise found no Stage C implementation. The reproduction commands instead point to the original machine's absolute workspace and interpreter paths. The calendar runner and run preflight also hardcode the original checkpoint location.

The earlier recovery package's successful restoration remains valid evidence for its declared contents. It is **not** evidence that Stage C can be restored or resumed on another machine. Local Vault presence also does not prove OneDrive cloud synchronization.

Repair: publish an explicitly scoped Stage C package with relative path resolution, a machine-local interpreter/path mapping, pinned dependencies, implementation, fixtures, manifests, and retained evidence. If resumable parts are excluded, state that limitation and provide a validated rebuild path. Verify restoration and recorded fixture reproduction in a new isolated root without secrets. Mapping: **R11; TST52**.

### OP06 — P2: the current manifest does not cover the full evidence graph

Source: [all68_stage_manifest.py:23](/C:/Users/zmoor/Documents/forex/stage_c_all68_20260921/all68_stage_manifest.py:23), lines 22–24.

The output pattern includes forecast-named gzip files but excludes `ALL68_24H_NEUTRAL_OUTCOME_SETTLEMENT.jsonl.gz` (**20,405,662 bytes**) and `ALL68_24H_NEUTRAL_POLICY_LEDGER.jsonl.gz` (**5,273,560 bytes**). Their receipts contain hashes, but the handoff verifier does not follow those references. The existing manifest also predates `all68_feature_coverage_report.py` and `ALL68_FEATURE_TARGET_ELIGIBILITY.json`. Resume parts are explicitly excluded from its scope.

Repair: manifest the actual dependency/output graph rather than infer completeness from filename patterns. Verify hashes referenced inside receipts, and make any intentional exclusions explicit. Mapping: **R10/R13; TST51**.

### OP07 — P2: calendar forecast output does not implement separate outcome reveal

Source: [all68_calendar_baseline.py:115](/C:/Users/zmoor/Documents/forex/stage_c_all68_20260921/all68_calendar_baseline.py:115).

The calendar tape places realized `actual_bps` beside the forecast without a separate outcome-availability gate or historical timestamp inspector. It is an offline evaluation export, not the design's immutable forecast and separately revealed outcome interface. This finding does not assert that this export has been used by a live policy.

Repair: separate original forecast issuance records from timestamped outcome settlement; bind their identities and implement time-masked inspection. The neutral-control work already demonstrates a separate-settlement pattern that can inform reuse. Mapping: **R13**.

## Acceptance status and reporting corrections

The current 17 Stage C test functions cover feature behavior/shape, accounting fixtures, global clock, target contracts, and static import checks. None tests dependency-cache invalidation, interrupted-resume equivalence, writer contention, corrupted-checkpoint rejection, or relocation/restore. TST48–52 are therefore **missing acceptance gates**, not five tests that were executed and failed in this audit. The source-level findings above show concrete failure paths those fixtures should exercise.

`REQUIREMENT_TRACEABILITY_20260921.md` line 14 currently calls stop/resume implemented with “immutable reports/tapes.” That should be reconciled to **partial—acceptance gates pending**, while retaining the actual byte-verification successes. `STATE_OF_RESEARCH_20260921.md` line 5 and related immutable-artifact statements need the same qualification. The local Stage C README still describes the initial preflight-only stage, while the directory now includes fitted research outputs; it needs a dated current-stage pointer. These documentation corrections do not themselves repair implementation.

## Recommended forward sequence

1. Preserve the present reports/tapes as dated research evidence. Correct verification exit handling and clearly distinguish byte integrity, implementation status, and predictive conclusions.
2. Implement dependency-bound run identity, validated paths/settings, exclusive writer ownership, and transactional completion publication. Separate verification from artifact regeneration.
3. Add and pass focused TST48–51 fixtures, including changed-window resume, changed source/feature hashes, crash during tape/report publication, and a competing writer. Record expected/actual outcomes and tolerances.
4. Package the resulting Stage C implementation, fixtures, manifests, and declared evidence; resolve machine paths externally and pass TST52 in a new isolated root. Preserve any excluded-part/rebuild limitation.
5. Continue the next bounded research experiment only after its dependent engineering gates are satisfied. Keep the broader architecture and execution readiness explicitly incomplete; these repairs do not demonstrate predictive value or authorize trading.

No Stage C repair, model rerun, source change, or Vault publication was performed by this audit. The parent task owns reconciliation and any subsequent authorized implementation.
