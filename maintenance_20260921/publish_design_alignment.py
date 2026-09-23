"""Publish a documentation-only alignment checkpoint; never launch research."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import shutil

WORK = Path(r'C:\Users\zmoor\Documents\forex')
VAULT = Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')
LOCAL = WORK / 'design_alignment_20260921'
DEST = VAULT / 'DESIGN_ALIGNMENT_20260921'

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def dump(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')

def text(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body.strip() + '\n', encoding='utf-8')

required = ['PATH_FORWARD.md', 'DESIGN_COVERAGE.md', 'CAUSALITY_AUDIT.md',
            'OPERATIONS_AUDIT.md', 'HISTORICAL_RECORDS_INDEX.md',
            'HISTORICAL_LINK_MAP.md', 'RUN_STATUS.json', 'README.md',
            'specification/FOREX_CODEX_ENGINEERING_DESIGN.md']
for rel in required:
    if not (LOCAL / rel).is_file():
        raise RuntimeError(f'Missing publication input: {rel}')
if DEST.exists():
    raise RuntimeError('Destination already exists; do not overwrite a completed package')

path_link = 'DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md'
coverage_link = 'DESIGN_ALIGNMENT_20260921/DESIGN_COVERAGE.md'
notice = f'''**Current direction — September 21 design alignment:** The engineering design is the user's goal. Read [the current path]({path_link}) and [acceptance/reuse map]({coverage_link}). Next: repair timing, target definitions, forecast/outcome separation and publication/recovery before another candidate run. All 37 historical pending IDs remain mapped. GPT/advisor comparisons remain deferred. The dated content below is predecessor evidence, not the current work order.'''
project_notice = '''**Current direction — September 21 design alignment:** Read [the governing design path](docs/FOREX_DESIGN_PATH_FORWARD_20260921.md) before acting on older entries. Pending changes now map to the full design and acceptance gates. Next: repair causality and reliable publication/recovery, then complete the integrated all-68 daily/multiday campaign. GPT/advisor comparisons remain deferred. Dated operational statements below are not fresh health checks.'''

docs = {}
docs[VAULT / 'README.md'] = '''# Forex — start here

The engineering design is the goal. [Current path forward](DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md) gives the actual stage, ordered implementation steps, acceptance gates and all 37 historical pending items.

1. Read the [exact design](DESIGN_ALIGNMENT_20260921/specification/FOREX_CODEX_ENGINEERING_DESIGN.md).
2. Read the [design coverage and reuse audit](DESIGN_ALIGNMENT_20260921/DESIGN_COVERAGE.md), [causality findings](DESIGN_ALIGNMENT_20260921/CAUSALITY_AUDIT.md) and [operations findings](DESIGN_ALIGNMENT_20260921/OPERATIONS_AUDIT.md).
3. Use the [current status](DESIGN_ALIGNMENT_20260921/RUN_STATUS.json) and [next coding batch](DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md#first-next-coding-batch).
4. Use the [Vault map](KNOWLEDGE_INDEX.md) for recovery, historical research and maintenance.

The current stage has substantial recovered data and reusable research, but unresolved timing, calendar, forecast-tape and resume defects. The next work is integrity repair, followed by the design's first complete all-68 daily/multiday campaign. Engineering readiness is false; Stage C reports are diagnostic evidence with the audit's limitations. GPT/advisor comparisons remain deferred.

The active source on the original machine is `C:\\Users\\zmoor\\Documents\\forex\\trad`; Stage C is an isolated research adapter area. This Vault is documentation and recovery storage, not the live application.

[Design alignment checkpoint](DESIGN_ALIGNMENT_20260921/README.md) contains the audit and work queue. [Recovery checkpoint](RECOVERY_CHECKPOINT_20260921/README.md), [new-machine handoff](RECOVERY_CHECKPOINT_20260921/NEW_MACHINE_HANDOFF.md) and [recovery pointer](RECOVERY_CHECKPOINT_LATEST.json) retain their separately verified source/input scope. That recovery package does not include Stage C; a relocated Stage C recovery test is still required. The design-alignment package is documentation, not an executable replacement backup.

[Historical CURRENT-named records](DESIGN_ALIGNMENT_20260921/HISTORICAL_RECORDS_INDEX.md) retain their original names and dates. [Historical link map](DESIGN_ALIGNMENT_20260921/HISTORICAL_LINK_MAP.md) records verified equivalents and unresolved links. Earlier current-status instructions were archived before replacement; see the alignment package's previous_documents inventory.

All local verification claims have explicit scope. OneDrive cloud synchronization and current broker health are unverified. D-drive work remains deferred. Existing services, accounts, raw histories and sealed recovery payloads were preserved by this cleanup.
'''
docs[VAULT / 'AGENTS.md'] = '''# Forex Vault handoff

Read README.md, DESIGN_ALIGNMENT_LATEST.json, DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md and its exact specification first. The latest user instruction makes the engineering design the goal and requires pending work to align with it. Follow current user instructions; old chats and dated CURRENT files are evidence, not launch directives.

Use DESIGN_ALIGNMENT_20260921/DESIGN_COVERAGE.md for reuse and acceptance mapping. Immediate work is timing/target/forecast-contract repair and trustworthy publication/resume, then the first complete all-68 daily/multiday campaign. GPT/advisor comparisons remain deferred. Preserve valid predecessors, including rolling technical features and curve-management replay; do not build replacements based on filenames alone.

For recovery, separately read RECOVERY_CHECKPOINT_LATEST.json and its checkpoint NEW_MACHINE_HANDOFF.md. Its manifest defines included bytes and exclusions; it does not contain the newer Stage C implementation. The alignment package is documentation, not runnable Stage C recovery. Restore inactive; do not equate Git HEAD with the preserved dirty worktree.

Preserve modified/untracked source, private exclusions, raw data and dated evidence/failures. Vault is records/recovery storage, not a live database location. Existing workers/accounts remain unchanged by this documentation cleanup. D-only evidence is deferred after disk errors. Cloud sync needs destination readback before being called verified.
'''
docs[VAULT / 'AUDIT_START_HERE.md'] = '''# Current Forex audit

Start with [design alignment](DESIGN_ALIGNMENT_20260921/README.md), [coverage and reuse](DESIGN_ALIGNMENT_20260921/DESIGN_COVERAGE.md), [causality](DESIGN_ALIGNMENT_20260921/CAUSALITY_AUDIT.md), [operations](DESIGN_ALIGNMENT_20260921/OPERATIONS_AUDIT.md), and [the current implementation path](DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md).

The [morning September 21 deep audit](RECOVERY_CHECKPOINT_20260921/audit_20260921/deep_audit_02/DEEP_AUDIT_REPORT.md) and [September 14 audit](AUDIT_20260914/README.md) remain predecessor evidence. Their next-step instructions have later dispositions. The [recovery pointer](RECOVERY_CHECKPOINT_LATEST.json) still describes its original included source/data scope; it does not certify Stage C recovery.

See [maintenance](maintenance/README.md) for preserved navigation preimages and readback records.
'''
docs[VAULT / 'KNOWLEDGE_INDEX.md'] = '''# Forex Vault map

## Current goal and work

- [Start here](README.md)
- [Design and explicit path forward](DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md)
- [Audit, requirement/test coverage and reuse](DESIGN_ALIGNMENT_20260921/DESIGN_COVERAGE.md)
- [Pending items](PENDING_IMPROVEMENTS_CURRENT.md), with all 37 mapped in the forward path
- [Current readiness](DESIGN_ALIGNMENT_20260921/RUN_STATUS.json)

## Recovery and source

- [September 21 recovery package](RECOVERY_CHECKPOINT_20260921/README.md) and [pointer](RECOVERY_CHECKPOINT_LATEST.json)
- [Source archive guide](source/README.md)
- [Isolated legacy pip repair](LEGACY_PIP_REPAIR_20260921/README.md)
- [Stage C diagnostic records and corrected status](ALL68_OFFLINE_GATE_20260921/README.md)

## Historical evidence

- [Research index](RESEARCH_INDEX.md) and [project history](PROJECT_HISTORY.md)
- [CURRENT-named historical record catalogue](DESIGN_ALIGNMENT_20260921/HISTORICAL_RECORDS_INDEX.md)
- [Historical link map](DESIGN_ALIGNMENT_20260921/HISTORICAL_LINK_MAP.md)
- [September 14 audit](AUDIT_20260914/README.md)

Dated experiment, operational and recovery directories retain their identities and contents. The word CURRENT in an old filename does not mean its account/worker/evidence status is current today.

## Maintenance and limitations

- [Maintenance history](maintenance/README.md)
- [This navigation cleanup](DESIGN_ALIGNMENT_20260921/VAULT_CLEANUP.md)

Earlier VAULT_FILE_INVENTORY.json and VAULT_READABILITY_REPORT.json remain dated records. They do not certify subsequent files or edits. This cleanup does not certify cloud sync, a full runtime backup, present account state or complete historical link repair.
'''
docs[VAULT / 'AUTHORITY_AND_REUSE_MAP_20260921.md'] = '''# Authority and reuse — current reconciliation

The user's governing goal is the full engineering design. Read [the design-aligned path](DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md) and the nine-column [current reuse map](DESIGN_ALIGNMENT_20260921/DESIGN_COVERAGE.md).

The former map is preserved byte-for-byte in DESIGN_ALIGNMENT_20260921/previous_documents/vault/AUTHORITY_AND_REUSE_MAP_20260921.md. Its claims of no fitted Stage C baseline and WP3 as the immediate unblocked next step are superseded: fitted diagnostics now exist, and timing/publication/recovery repairs are required.

Reuse the existing rolling technical and curve-management replay components after contract verification. Trace deterministic dependencies rather than treating a GPT-related filename as proof of a paid call. GPT/advisor comparisons themselves remain deferred. Existing runtime services and sealed recovery records are outside this documentation change.
'''
for name in ['PENDING_IMPROVEMENTS_CURRENT.md','RESEARCH_INDEX.md','PROJECT_HISTORY.md']:
    docs[VAULT / name] = notice + '\n\n---\n\n' + (VAULT / name).read_text(encoding='utf-8-sig')
for name in ['README.md','FOREX_PENDING_IMPROVEMENTS.md']:
    docs[WORK / 'trad' / name] = project_notice + '\n\n---\n\n' + (WORK / 'trad' / name).read_text(encoding='utf-8-sig')
docs[WORK / 'START_HERE.md'] = '''# Forex project and recovery

The active project is trad/. The user's goal is the full engineering design. Read [the current design-aligned path](design_alignment_20260921/PATH_FORWARD.md), [audit/reuse coverage](design_alignment_20260921/DESIGN_COVERAGE.md) and [readiness](design_alignment_20260921/RUN_STATUS.json).

The durable audit/design copy and current Vault navigation are in `C:\\Users\\zmoor\\OneDrive\\thevault\\projects\\forex\\README.md`. Existing current-status documents were archived before this reconciliation. GPT/advisor comparisons remain deferred. Do not start old restored scripts or alter live services based on historical launch instructions.
'''
docs[WORK / 'stage_c_all68_20260921' / 'README.md'] = '''# Stage C — research adapters and diagnostic evidence

Read [the governing design path](../design_alignment_20260921/PATH_FORWARD.md), [causality audit](../design_alignment_20260921/CAUSALITY_AUDIT.md) and [operations audit](../design_alignment_20260921/OPERATIONS_AUDIT.md) before running this directory.

This directory contains input audits, neutral forecast/settlement records, target coverage, fitted endpoint Ridge/HGB diagnostics and small fixtures. It has progressed beyond the original input-only README, but it is not engineering-ready.

Known defects include elapsed label maturity, future-dependent forecast population, realized outcomes embedded in fitted tapes, unresolved session-calendar semantics, implicit randomized HGB early stopping, unbound cached parts and nonexclusive/overwriteable publication. The verification launcher can mask native command failures; its output is not an acceptance certificate. Existing tapes and reports must remain as historical diagnostic evidence. Do not rerun scripts that overwrite them under the same IDs.

Next work is alignment_integrity_v2: repair and test those contracts, then a dependency-bound new run and relocated fixture restore. The old hard-coded admission/retirement outputs do not retire entire model families. GPT/advisor comparisons remain deferred. Source implementation is unchanged by this README correction.
'''
gate = VAULT / 'ALL68_OFFLINE_GATE_20260921'
docs[gate / 'README.md'] = '''# Stage C records — design reconciliation

The current authority is [the full design and forward path](../DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md). This directory preserves Stage C diagnostic receipts; it is not a portable implementation package.

The [causality audit](../DESIGN_ALIGNMENT_20260921/CAUSALITY_AUDIT.md) identifies a maturity-contract defect, future-dependent forecast eligibility, joined forecast/outcome exports and calendar/selection limitations. The [operations audit](../DESIGN_ALIGNMENT_20260921/OPERATIONS_AUDIT.md) identifies unbound resume parts, overwriteable publication, a launcher that can mask failure and incomplete checkpoint coverage. The existing 54-entry manifest matched its bytes; that does not establish those missing contracts.

Existing forecast metrics remain retrospective diagnostics on their stated populations. They are not policy-ready tapes or grounds for retiring Ridge/HGB families. Neutral tapes and separate outcomes remain useful predecessors. The original narrative and status were archived under ../DESIGN_ALIGNMENT_20260921/previous_documents/vault/ALL68_OFFLINE_GATE_20260921/.

Read [next implementation](NEXT_IMPLEMENTATION.md), [current status](RUN_STATUS.json) and [full requirement/test coverage](../DESIGN_ALIGNMENT_20260921/DESIGN_COVERAGE.md). GPT/advisor comparisons remain deferred.
'''
docs[gate / 'NEXT_IMPLEMENTATION.md'] = '''# Next implementation — integrity before new candidates

Follow [PATH_FORWARD.md](../DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md), especially the first next coding batch, alignment_integrity_v2. The previous instruction to create exactly one new non-GPT candidate is superseded.

1. Reproduce and repair maturity, forecast eligibility and forecast/outcome separation; version explicit target calendars and model readiness.
2. Repair native exit propagation, dependency-bound caches, exclusive publication and verified manifests.
3. Pass adversarial fixtures and relocated Stage C recovery before another large fit.
4. Complete the first integrated all-68 daily/multiday campaign, reusing reviewed rolling features, curve-management replay and historical baselines.

All 37 historical pending IDs and WP0–WP11 have a place in the current path. Preserve original v1 source/results; new semantics require a new run identity. Do not rely on the current launcher alone as proof of validity. GPT/advisor comparisons remain deferred.
'''
docs[gate / 'REQUIREMENT_TRACEABILITY_20260921.md'] = '''# Requirement traceability — current correction

Use the complete [R01–R15, WP0–WP11 and TST01–TST56 map](../DESIGN_ALIGNMENT_20260921/DESIGN_COVERAGE.md) and [forward path](../DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md).

The former table overstated causality, target calendars, inspectability and stop/resume. Those are partial, with specific defects or acceptance gates still open. Existing tests and matching file hashes do not close missing integration gates. All-68 input identity is useful evidence; fitted issuance coverage and independent executable policies require further work.

Engineering readiness is false. Forecast evidence is diagnostic with integrity caveats. The design campaign's portfolio policy evidence is unevaluated and demo authorization is not granted. GPT/advisor comparisons remain deferred. Earlier traceability bytes are preserved in the alignment package's previous_documents archive.
'''
docs[gate / 'STATE_OF_RESEARCH_20260921.md'] = '''# State of research — design alignment correction

Current status is [RUN_STATUS.json](RUN_STATUS.json). Read [the full audit and design path](../DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md). The prior state document is retained in the alignment package's previous_documents archive.

Stage C produced useful coverage, neutral-control records and fitted endpoint diagnostics. Its fitted paths need timing, forecast-population, outcome-separation, calendar and early-stopping corrections. Its run identity/publication/recovery is incomplete. These defects prevent using the saved fitted tapes as trustworthy policy input; they do not establish that every historical experiment failed or every saved prediction used future information.

Preserve both favorable and unfavorable results and their actual populations. Do not use hard-coded retired_negative_benchmark values to retire whole model families. Corrected cohort comparisons must have new identities and honest development/confirmation labels. The next work is integrity and recovery repair, then the first complete design campaign. Non-GPT macro/feature/reuse work can proceed independently. Advisor comparisons remain deferred.
'''
docs[VAULT / 'source' / 'README.md'] = '''# Source archive guide

Use [WORKTREE_SOURCE_LATEST.json](WORKTREE_SOURCE_LATEST.json) for the preserved dirty-worktree snapshot and the [September 21 recovery pointer](../RECOVERY_CHECKPOINT_LATEST.json) for its included package and verification scope. A committed baseline and the full worktree are different artifacts.

SOURCE_BASELINE_LATEST.json, forex_source_current.zip and older content-addressed archives retain their historical/compatibility roles. Do not delete an alias or archive merely because bytes appear duplicated; follow manifest references and exclusions first. Specialized packages such as typed_outcomes_v4_20260912_revision002 have their own manifests and receipts.

The newer isolated Stage C implementation is not included by the original recovery checkpoint. [Design alignment](../DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md) requires an explicit Stage C package and relocated recovery test. The alignment package itself contains documentation, not that runnable implementation. Restore inactive, preserve private exclusions, and never infer current runtime state from archived launchers.
'''
docs[VAULT / 'maintenance' / 'README.md'] = '''# Vault maintenance

[September 21 design alignment and navigation cleanup](../DESIGN_ALIGNMENT_20260921/VAULT_CLEANUP.md) is the latest documentation pass. Its previous_documents inventory preserves pre-update bytes, and PACKAGE_READBACK.json records the scoped local verification.

[Earlier September 21 cleanup, checkpoint and readback](vault_refresh_20260921/README.md) and [its navigation preimages](vault_refresh_20260921/PREIMAGES.json) remain dated evidence. [September 14 refresh](vault_refresh_20260914/README.md), September 15 cleanup and all older receipts retain their original scopes.

No sealed recovery contents or raw research evidence are removed by the new navigation pass. Its local verification does not certify cloud synchronization or later runtime health.
'''

changes = []
inventory = json.loads((LOCAL / 'previous_documents' / 'INVENTORY.json').read_text(encoding='utf-8-sig'))
existing_sources = {x['source'].lower() for x in inventory}
for path, body in docs.items():
    before = digest(path) if path.exists() else None
    if path.exists():
        root, label = (VAULT, 'vault') if path.is_relative_to(VAULT) else (WORK, 'workspace')
        archived = LOCAL / 'previous_documents' / label / path.relative_to(root)
        if archived.exists():
            if digest(archived) != before:
                raise RuntimeError(f'Document changed since archive: {path}')
        else:
            archived.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, archived)
        if str(path).lower() not in existing_sources:
            inventory.append({'source':str(path), 'archived_relative_path':archived.relative_to(LOCAL).as_posix(), 'sha256':before, 'bytes':path.stat().st_size})
    text(path, body)
    changes.append({'path':str(path), 'before_sha256':before, 'after_sha256':digest(path)})

status = json.loads((LOCAL / 'RUN_STATUS.json').read_text(encoding='utf-8'))
status['authority_path'] = '../DESIGN_ALIGNMENT_20260921/RUN_STATUS.json'
status['legacy_reports'] = 'preserved diagnostic evidence; see linked audits before use'
status['design_path'] = '../DESIGN_ALIGNMENT_20260921/specification/FOREX_CODEX_ENGINEERING_DESIGN.md'
status['path_forward'] = '../DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md'
status['test_requirement_map'] = '../DESIGN_ALIGNMENT_20260921/DESIGN_COVERAGE.md'
gate_status = gate / 'RUN_STATUS.json'
before_status = digest(gate_status)
if before_status != digest(LOCAL / 'previous_documents/vault/ALL68_OFFLINE_GATE_20260921/RUN_STATUS.json'):
    raise RuntimeError('Gate status changed since archive')
dump(gate_status, status)
changes.append({'path':str(gate_status), 'before_sha256':before_status, 'after_sha256':digest(gate_status)})
dump(LOCAL / 'previous_documents' / 'INVENTORY.json', inventory)
dump(LOCAL / 'DOCUMENT_CHANGES.json', changes)
text(LOCAL / 'VAULT_CLEANUP.md', '''# Forex Vault cleanup — scope and result

Authorized during the September 21 design-alignment audit. This pass cleans the Forex project's navigation and contradictory current instructions. It preserves historical files, sealed manifests, source ZIPs, raw histories and existing services/accounts.

The read-only starting inventory found 194 root files and 24 directories, including 156 names containing CURRENT. Hash comparison of 178 small root documents found no exact duplicate groups. Substantial historical status files are not treated as disposable clutter.

Changed: one design-first start page, current authority/reuse pointers, audit and knowledge indexes, current pending/research/history notices, Stage C status/next-step reconciliation, source-archive guidance and maintenance navigation. Added: historical record catalogue, historical-link map, exact design copy and audit/forward-path package. Current workspace and project docs now point to the same path.

All modified existing documents were copied and hash-checked before replacement. `previous_documents/INVENTORY.json` records their original paths and hashes; `DOCUMENT_CHANGES.json` records each replacement. Original historical payloads remain at their old paths, avoiding broken checkpoint references. No bulk deletion or disk-space reclamation is claimed.

The historical link map distinguishes verified counterparts from unresolved links. This is not a claim that every old source-relative link works. Earlier global Vault inventory/readability reports retain their dates and do not certify this pass. `PACKAGE_READBACK.json` provides a new scoped local verification; cloud synchronization is unverified.
''')

files = []
for path in sorted(LOCAL.rglob('*')):
    if path.is_file() and path.name not in {'MANIFEST.json','PACKAGE_READBACK.json'}:
        files.append({'path':path.relative_to(LOCAL).as_posix(), 'bytes':path.stat().st_size, 'sha256':digest(path)})
dump(LOCAL / 'MANIFEST.json', {'schema':'forex_documentation_checkpoint.v1', 'created_utc':datetime.now(timezone.utc).isoformat(), 'scope':'documentation only; not runnable Stage C recovery', 'files':files})
shutil.copytree(LOCAL, DEST)
for item in files:
    if digest(DEST / item['path']) != item['sha256']:
        raise RuntimeError(f'Published payload mismatch: {item["path"]}')
receipt = {'schema':'forex_documentation_readback.v1', 'verified_utc':datetime.now(timezone.utc).isoformat(), 'file_count':len(files), 'bytes':sum(x['bytes'] for x in files), 'manifest_sha256':digest(DEST / 'MANIFEST.json'), 'local_copy_payloads_verified':True, 'operational_stage_c_restore_verified':False, 'cloud_sync_verified':False}
dump(LOCAL / 'PACKAGE_READBACK.json', receipt)
dump(DEST / 'PACKAGE_READBACK.json', receipt)
dump(VAULT / 'DESIGN_ALIGNMENT_LATEST.json', {'schema':'forex_design_alignment_pointer.v1','package':'DESIGN_ALIGNMENT_20260921', 'path_forward':'DESIGN_ALIGNMENT_20260921/PATH_FORWARD.md', 'design':'DESIGN_ALIGNMENT_20260921/specification/FOREX_CODEX_ENGINEERING_DESIGN.md', 'manifest':'DESIGN_ALIGNMENT_20260921/MANIFEST.json','manifest_sha256':receipt['manifest_sha256'],'readback':'DESIGN_ALIGNMENT_20260921/PACKAGE_READBACK.json','readback_sha256':digest(DEST / 'PACKAGE_READBACK.json'),'scope':receipt['schema'],'cloud_sync_verified':False})
print(json.dumps({'documents_changed':len(changes),'archived_documents':len(inventory),'package_files':len(files),'vault_package':str(DEST)}))
