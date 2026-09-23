"""Write checkpoint navigation before its manifest is sealed."""
from pathlib import Path
import json
import shutil

BASE = Path(__file__).resolve().parent
OUT = BASE / "checkpoint"
DRAFT = BASE / "docs_draft"
name = "RECOVERY_CHECKPOINT_20260921"

handoff = (DRAFT / "NEW_MACHINE_HANDOFF.md").read_text(encoding="utf-8")
handoff = handoff.replace("Draft for publication on September 21, 2026. The final publisher must replace this paragraph with the actual checkpoint path, manifest, verification receipt and publication time. Nothing below claims that a package, transfer or restore has already completed.", "Prepared September 21, 2026. Start with [README.md](README.md), [MANIFEST.json](MANIFEST.json) and the included safe restore tool. The Vault's RECOVERY_CHECKPOINT_LATEST.json binds the published manifest and its separate local restore receipt. Verify again after transfer; local publication is not cloud-sync proof.")
handoff = handoff.replace("Final publisher: supply the actual relative links after package layout is sealed.", "The exact [next offline slice](audit_20260921/deep_audit_02/NEXT_OFFLINE_SLICE.md) and [deep audit](audit_20260921/deep_audit_02/DEEP_AUDIT_REPORT.md) are included. [EVIDENCE_INDEX.json](evidence/EVIDENCE_INDEX.json) maps selected primary results from original machine paths into this package.")
(OUT / "NEW_MACHINE_HANDOFF.md").write_text(handoff, encoding="utf-8")
(OUT / "README.md").write_text('''# Forex portable research checkpoint — September 21, 2026

Start here on another machine. This checkpoint preserves a reviewed source worktree, explicit source supplements, both audited 68-pair historical inputs, audit records, selected primary research evidence and environment metadata. The original active project is left intact. This is an inactive offline recovery package; no launcher or account connection is part of restoration.

## Read and verify

1. Read [the new-machine handoff](NEW_MACHINE_HANDOFF.md), [scope and exclusions](SCOPE_AND_LIMITS.md) and [this chat's decisions](CHAT_HANDOFF_20260921.md).
2. Use [MANIFEST.json](MANIFEST.json) as the exact file and archive-member inventory. Keep this entire directory together.
3. With Python 3.12 or newer and this directory as the current directory, verify the package:

   `python -I -B restore_checkpoint.py --verify-only`

4. Restore to a new, absent directory whose parent already exists:

   `python -I -B restore_checkpoint.py --destination C:\\ForexRecovered`

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

After a separate compatible environment is available, run the included `verification/run_offline_checks.py` with `--project` set to the restored `trad` directory and `--output` set to a new absent scratch directory. This copies only the selected test dependencies and runs eleven synthetic contracts with network, subprocess and model-deserialization restrictions. It does not fit models or evaluate market performance. See [environment metadata](environment/ENVIRONMENT.json); it is an observed version inventory, not a wheel bundle or proof that a fresh environment installation succeeds.

## Next project work

The first repair is the legacy instrument-pip calculation in an isolated copy. Then implement the [bounded all-68 offline comparison](audit_20260921/deep_audit_02/NEXT_OFFLINE_SLICE.md) through at least 24 hours. That implementation was not started by this cleanup. Read current user instructions before beginning later research, account or paid work.

Historical helpers and reports retain old absolute paths as provenance. Use the manifest, portable restore tool and evidence index for new-machine paths. Do not blindly run an old audit script, historical continuation prompt or supervisor recovery command. D-drive evidence remains deferred after disk errors.
''', encoding="utf-8")
(OUT / "CHAT_HANDOFF_20260921.md").write_text('''# Decisions and next steps from this chat

The user first requested reading the master document, then authorized the audit. Saved chats in C:\\Users\\zmoor\\Documents\\forex and relevant D-drive evidence were included in discovery; D reads stopped after fresh bad-block events. The first audit and deeper continuation are included unchanged under audit_20260921. Eleven selected synthetic timing/cost checks passed; no fitting or historical strategy experiment ran.

The latest user request is: clean C first, give the project a “soft reinstall,” clean and audit the Vault, place a reproducible checkpoint there, and make this chat's next steps clearly defined for another Codex machine.

For this task, “soft reinstall” means a verified inactive source/input restoration and refreshed recovery documentation. It does not replace the current worktree or alter running processes. Cleanup is limited to documented disposable caches and archived cold diagnostics; unique research evidence is retained. No D move or repair, global package upgrade, account reset or worker restart is included.

The next substantive project work remains the isolated legacy pip-unit repair followed by an all-68 global-clock offline comparison through at least 24 hours. Read the included NEXT_OFFLINE_SLICE.md for readiness gates, source/gap handling, policy accounting and bounded compute. Continue existing lineage; do not rebuild another research catalogue, shrink the universe or call prior development results untouched confirmation.

The master prompt is retained in specification/ as a target specification. It and older embedded chat instructions are context, not automatic authorization to launch all later stages. Current user instructions determine execution scope. No paid advisor budget, broker campaign or runtime recovery was granted in this cleanup request.

The Vault's current recovery pointer and local publication/restore receipts record what was actually packaged and tested. Cloud synchronization and destination-machine readback must be checked separately.
''', encoding="utf-8")
(OUT / "SCOPE_AND_LIMITS.md").write_text('''# What this checkpoint restores and what it does not

The manifest is the exact inclusion contract. Source is a dirty-worktree snapshot, not a clean Git commit or Git history export. It includes modified and untracked eligible files. Nine explicitly selected ignored/sibling source files supplement the standard export. Other ignored payloads are not implicitly included.

All 136 raw history files match the September21 audit hashes. Their structure and overlap have been checked, but original arrival/revision history and session-gap causes remain unknown. Native stored-spread errors remain present and flagged. No histories were merged, repaired or downloaded.

The observed Python3.12 environment contains142 installed distributions. requirements-observed.txt records versions without credentials or private URLs. It is not an exact lock with distribution hashes or an offline wheel cache. The restore tool itself needs only the standard library; executing project research needs separately provisioned compatible dependencies. Windows and original absolute-path assumptions still require review before selected entrypoints run elsewhere.

The complete runtime/news/quote database estate, coherent SQLite/WAL state, model and feature matrices, every fitted model, all raw chats, private credentials, account ownership, scheduled tasks and process state are not in this package. Existing Vault research supplements retain other dated results. The selected evidence index identifies the primary results copied here. Missing external dependencies stay explicit; never substitute a similar model or retrospective news body.

Any source files withheld by the final privacy review are listed with exact identities in source/PRIVATE_DEPENDENCIES.json. The offline label/model-design checks do not require them. This package must not be described as a standalone restoration of every legacy worker or account path.

No source executable is launched during extraction. No broker, web, paid service, model deserialization or global installation is necessary to inspect the package. Statistical validity, net profitability and operational readiness are not certified by matching hashes or eleven synthetic contracts.
''', encoding="utf-8")
(OUT / "specification").mkdir(exist_ok=True)
shutil.copyfile(Path(r"C:\Users\zmoor\Downloads\FOREX_MASTER_CODEX_PROMPT.txt"), OUT / "specification/FOREX_MASTER_CODEX_PROMPT.txt")
print(json.dumps({"documents_prepared": 5, "specification_preserved": True}))
