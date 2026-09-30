# Numeric join publication handoff

This addendum completes `NUMERIC_JOIN_REVIEW_REPAIRS_20260924_004443` without
changing that sealed packet. PUB-01 and PUB-02 are resolved: all 460 package files,
375 source files, 15 current Vault documents and 22 current project documents were
independently verified; three duplicate queue entries are preserved as history in
one canonical step. The exact next item is `whole_curve_warm_start_capacity_followup_v2`.

## Evidence path mapping

`WORK_LOG.jsonl`, `PENDING_CHANGES.md` and `SESSION_STATE.json` are preserved snapshots
of local step evidence at publication time, not current mutable status. Their local
relative `independent_join_review/` prefix maps to packaged `independent_review/`.
Local `join_runs/numeric-evidence-join-20260914-20260921/COMPLETION_MANIFEST.json`
maps to packaged `evidence/RUN_COMPLETION_MANIFEST.json`; the meter report is in
`evidence/meter_report.json`. Local browser profiles and full browser HTML were
excluded; compact actual-browser and invariant receipts/scripts are packaged.
`PENDING_CHANGES.md` PUB statuses describe the pre-publication snapshot and are
closed by this independent publication addendum. The independent source review is
`independent_review/RESOLUTION_REVIEW.json`; first findings remain in REVIEW_RESULT.json.

## Exact restore and verify

From the project root, with an empty destination outside the Vault:

```powershell
$taskPy = 'C:/Users/zmoor/AppData/Local/CodexRuntimes/timeseries312/Scripts/python.exe'
& $taskPy -X utf8 -I -B stage_c_alignment_integrity_v2/macro_numeric_evidence_join_checkpoint_v2.py restore --package 'C:/Users/zmoor/OneDrive/thevault/projects/forex/NUMERIC_JOIN_REVIEW_REPAIRS_20260924_004443/checkpoint/forex_numeric_join.zip' --sha256 a05a7810ee6077362b74964af40012e1d9571f6d919c5f0bc129391cdcc9d95c --destination 'C:/Users/zmoor/Documents/forex/checkpoint_restores/numeric_join_review_fresh' --run-tests
```

Use a new empty destination if that directory already exists. No fitting or broker
operation occurs. Expected: five identical payloads; 16 portable tests pass and three
tests requiring external original evidence skip. For a new machine map roots and use
the same locked environment. Obtain source from Git `c14630874f8a75c637ecac31ca120e84e65cebd7`
or verified bundle in `NUMERIC_JOIN_GIT_20260924_004443` before invoking the source tool.

## Rollback / investigation

Never overwrite the current working tree or reaccept a defective predecessor.
Create a separate checkout of baseline `4daa6c6b0a4e6e6f0c1e5f6a4ecc508216376fd1`
for comparison; the exact patch is packaged as CHANGES.patch. Prior queue and pointer
bytes are retained in baseline/. A rollback of live pointers requires an explicit
recorded review disposition because the prior join has known NJR-01 through NJR-04
defects. Preserve new evidence and use status/verify for the pinned successor.
