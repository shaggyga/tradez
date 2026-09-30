# Forex portable research checkpoint — September 21, 2026

Start here on another machine. This checkpoint preserves a reviewed source worktree, explicit source supplements, both audited 68-pair historical inputs, audit records, selected primary research evidence and environment metadata. The original active project is left intact. This is an inactive offline recovery package; no launcher or account connection is part of restoration.

## Read and verify

1. Read [the new-machine handoff](NEW_MACHINE_HANDOFF.md), [scope and exclusions](SCOPE_AND_LIMITS.md) and [this chat's decisions](CHAT_HANDOFF_20260921.md).
2. Use [MANIFEST.json](MANIFEST.json) as the exact file and archive-member inventory. Keep this entire directory together.
3. With Python 3.12 or newer and this directory as the current directory, verify the package:

   `python -I -B restore_checkpoint.py --verify-only`

4. Restore to a new, absent directory whose parent already exists:

   `python -I -B restore_checkpoint.py --destination C:\ForexRecovered`

The verifier uses only Python's standard library. It checks safe paths, duplicate/case collisions, archive hashes, every member and extracted bytes. It will refuse an existing destination. It does not run restored source, install packages, reconstruct Git history or start services. A failed extraction can leave a partial destination; retain it for diagnosis and choose a different fresh directory.

## Restored layout

| Path beneath the destination | Contents |
| --- | --- |
| `trad/` | Reviewed current source/config/tests/docs, plus explicitly selected ignored intrahour sources |
| `direction_decision_20260911/src/` | Exact sibling signed-cost helper required by rolling research |
| `inputs/m1_reacquired_20260725/` | 68 long-history Parquet files; 50,321,016 rows |
| `inputs/native_candles/` | 68 native CSV files; 4,652,361 rows, retained with known spread defect |
| `checkpoint_records/` | Handoff, audit, source/data identities, environment metadata, primary evidence and restore receipt |

No data is installed over an active project. The long and native sources remain distinct. The native data has 26,824 incorrect stored spread values in four pairs during August 9–14; see the [source trace](audit_20260921/deep_audit_02/NATIVE_SPREAD_SOURCE_TRACE.md). Preserving those bytes is deliberate; any correction must be a documented derived view.

## Optional offline checks

After a separate compatible environment is available, run the included `verification/run_portable_contract_checks.py` with `--project` set to the restored `trad` directory and `--output` set to a new absent scratch directory. This copies only the selected test dependencies and runs eleven synthetic contracts with network, subprocess and model-deserialization restrictions. It does not fit models or evaluate market performance. See [environment metadata](environment/ENVIRONMENT.json); it is an observed version inventory, not a wheel bundle or proof that a fresh environment installation succeeds.

## Next project work

The first repair is the legacy instrument-pip calculation in an isolated copy. Then implement the [bounded all-68 offline comparison](audit_20260921/deep_audit_02/NEXT_OFFLINE_SLICE.md) through at least 24 hours. That implementation was not started by this cleanup. Read current user instructions before beginning later research, account or paid work.

Historical helpers and reports retain old absolute paths as provenance. Use the manifest, portable restore tool and evidence index for new-machine paths. Do not blindly run an old audit script, historical continuation prompt or supervisor recovery command. D-drive evidence remains deferred after disk errors.
