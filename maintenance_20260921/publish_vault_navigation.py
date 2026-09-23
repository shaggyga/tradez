"""Publish navigation only after the portable package has been restored and checked."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import shutil

BASE = Path(__file__).resolve().parent
VAULT = Path(r"C:\Users\zmoor\OneDrive\thevault")
PROJECT = VAULT / "projects/forex"
NAME = "RECOVERY_CHECKPOINT_20260921"
CP = PROJECT / NAME
MAINT = PROJECT / "maintenance/vault_refresh_20260921"
stamp = datetime.now(timezone.utc).isoformat()


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def read(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def js(path, obj):
    write(path, json.dumps(obj, indent=2) + "\n")


restore = read(BASE / "RESTORE_VERIFICATION.json")
contracts = read(BASE / "restored_contract_checks/CONTRACT_RESULTS.json")
assert restore["status"] == "verified" and restore["restored_files_sha256_verified"]
assert restore["manifest_sha256"] == sha(CP / "MANIFEST.json")
assert contracts["exit_code"] == 0 and contracts["source_and_copy_unchanged"]
assert "11 passed" in contracts["console"]
assert (MAINT / "PREIMAGES.json").is_file()
for src, dst in [(BASE / "RESTORE_VERIFICATION.json", "RESTORE_VERIFICATION.json"), (BASE / "restored_contract_checks/CONTRACT_RESULTS.json", "CONTRACT_RESULTS.json"), (BASE / "RESTORE_TOOL_TESTS.json", "RESTORE_TOOL_TESTS.json"), (BASE / "SHARED_SOURCE_EXPORT_RECEIPT.json", "SHARED_SOURCE_EXPORT_RECEIPT.json"), (BASE / "SOURCE_PRIVACY_REVIEW.json", "SOURCE_PRIVACY_REVIEW.json")]:
    shutil.copyfile(src, MAINT / dst)
shutil.copyfile(BASE / "RESTORE_ATTEMPT1.json", MAINT / "RESTORE_ATTEMPT1.json")
source_pointer = read(CP / "source/WORKTREE_SOURCE_LATEST.json")
for name in (source_pointer["archive"], source_pointer["manifest"], "PRIVATE_DEPENDENCIES.json"):
    target = PROJECT / "source" / name
    source = CP / "source" / name
    if target.exists():
        assert sha(target) == sha(source), "Never replace a different immutable source file"
    else:
        shutil.copyfile(source, target)
shutil.copyfile(CP / "source/WORKTREE_SOURCE_LATEST.json", PROJECT / "source/WORKTREE_SOURCE_LATEST.json")

pointer = {"schema": "forex_portable_recovery_pointer_v1", "published_utc": stamp, "checkpoint": NAME, "readme": NAME + "/README.md", "manifest": NAME + "/MANIFEST.json", "manifest_sha256": sha(CP / "MANIFEST.json"), "local_restore_receipt": "maintenance/vault_refresh_20260921/RESTORE_VERIFICATION.json", "local_restore_receipt_sha256": sha(MAINT / "RESTORE_VERIFICATION.json"), "source_snapshot_id": source_pointer["snapshot_id"], "scope": "Offline research source and audited input recovery with explicit privacy/runtime exclusions", "restored_locally": True, "synthetic_contracts_passed": 11, "cloud_sync_verified": False, "trading_or_worker_start_authorized": False}
js(PROJECT / "RECOVERY_CHECKPOINT_LATEST.json", pointer)
js(VAULT / "RECOVERY_CHECKPOINT_LATEST.json", {"schema": "vault_project_recovery_redirect_v1", "project": "forex", "pointer": "projects/forex/RECOVERY_CHECKPOINT_LATEST.json", "pointer_sha256": sha(PROJECT / "RECOVERY_CHECKPOINT_LATEST.json"), "readme": "projects/forex/README.md", "published_utc": stamp, "old_CHECKPOINT_LATEST_status": "August20 historical model-gap artifact, retained unchanged"})

project_doc = (BASE / "docs_draft/VAULT_PROJECT_README.md").read_text(encoding="utf-8")
project_doc = project_doc.replace("Proposed September 21, 2026 entrypoint. Publish only after the recovery checkpoint and its exact verification receipts exist; replace the draft checkpoint names/links below with the sealed paths.", "Updated September 21, 2026. The portable package passed local extraction/hash readback and eleven synthetic contracts. [Current recovery pointer](RECOVERY_CHECKPOINT_LATEST.json) binds the exact manifest and [restore receipt](maintenance/vault_refresh_20260921/RESTORE_VERIFICATION.json). Cloud synchronization is not verified.")
project_doc = project_doc.replace("The older `source/WORKTREE_SOURCE_LATEST.json` and committed-source baseline are separate export mechanisms; identify the new full recovery manifest explicitly.", "The source-only pointer `source/WORKTREE_SOURCE_LATEST.json` now names the same privacy-reviewed source snapshot included in the recovery package. The older committed-source baseline and root August20 CHECKPOINT_LATEST.json remain historical. Explicit private-source exclusions are in the recovery package; this is not a full legacy runtime restore.")
write(PROJECT / "README.md", project_doc)
root_doc = (BASE / "docs_draft/VAULT_ROOT_INDEX.md").read_text(encoding="utf-8")
root_doc = root_doc.replace("Proposed September 21, 2026 replacement navigation. Final publisher: replace the draft recovery link after sealing the checkpoint.", "Updated September 21, 2026. [Current portable recovery pointer](RECOVERY_CHECKPOINT_LATEST.json) · [Forex recovery package](projects/forex/RECOVERY_CHECKPOINT_20260921/README.md).")
write(VAULT / "INDEX.md", root_doc)
write(VAULT / "README.md", root_doc + "\n## Storage and audit scope\n\nThis Vault is the compact record/recovery layer. The September21 checkpoint intentionally includes one verified portable copy of the two audited history vintages so another machine can inspect offline research. Runtime databases, caches, raw diagnostic logs and credentials remain outside the Vault. Older sealed artifacts remain historical and were not rewritten. A fresh local byte inventory and navigation audit is linked from the Forex project; it does not certify every historical result or every old archive's privacy.\n")
old_index = read(MAINT / "before/index.json")
js(VAULT / "index.json", {"schema": "shared_vault_navigation_v2", "updated_utc": stamp, "forex": {"readme": "projects/forex/README.md", "recovery_pointer": "projects/forex/RECOVERY_CHECKPOINT_LATEST.json", "next_steps": "projects/forex/" + NAME + "/audit_20260921/deep_audit_02/NEXT_OFFLINE_SLICE.md"}, "project_commons": old_index.get("project_commons"), "projects_root": "projects", "cold_archive_map": "COLD_ARCHIVE_POINTERS_CURRENT.md", "historical_index_preimage": "projects/forex/maintenance/vault_refresh_20260921/before/index.json", "historical_root_checkpoint": "CHECKPOINT_LATEST.json", "historical_root_checkpoint_is_current": False})
write(PROJECT / "AUDIT_START_HERE.md", "# Current Forex audit\n\nStart with [the September21 deep audit](" + NAME + "/audit_20260921/deep_audit_02/DEEP_AUDIT_REPORT.md), [new-machine handoff](" + NAME + "/NEW_MACHINE_HANDOFF.md), and [current recovery pointer](RECOVERY_CHECKPOINT_LATEST.json). The initial audit is retained in the same package and its later findings are explicitly superseded by the deeper continuation.\n\n[September14 audit](AUDIT_20260914/README.md) and other dated records remain historical evidence. Current cleanup and Vault readback are in [maintenance](maintenance/vault_refresh_20260921/README.md).\n")
write(PROJECT / "RECREATION.md", "# Restore the current offline research checkpoint\n\nUpdated September21,2026. Use [the portable checkpoint](" + NAME + "/README.md) and its included restore_checkpoint.py, not the older forex_vault_import.py or bootstrap launcher. Verify every member and extract into a new absent directory. The restore tool never launches project code.\n\n[Current pointer](RECOVERY_CHECKPOINT_LATEST.json) binds the manifest and successful local restore receipt. Eleven synthetic contracts passed on restored source copies. Read [scope and exclusions](" + NAME + "/SCOPE_AND_LIMITS.md), including private legacy modules, historical runtime databases and separately provisioned dependencies. No standalone legacy-runtime or fresh-environment restoration is claimed.\n\n[Previous guide](maintenance/vault_refresh_20260921/before/projects/forex/RECREATION.md) is preserved. All older sealed source and research packages retain their own dates. OneDrive destination readback remains required.\n")
with (PROJECT / "RECREATION.md").open("a", encoding="utf-8") as f:
    f.write("\n## Windows destination length\n\nUse a short restore root, such as C:\\ForexRecovered. The first deeply nested maintenance-directory attempt hit the 260-character Windows path limit (LongPathsEnabled=0). It did not alter the source package. Retrying at C:\\ForexRestore_20260921 kept every archived path below216characters and passed. [Failed attempt record](maintenance/vault_refresh_20260921/RESTORE_ATTEMPT1.json) is retained. No system setting was changed.\n")
updates = {
    "PROJECT_HISTORY.md": "The exact July moments run and H1 two-hour bundle were recovered. All68 long and native histories were audited; a localized native spread-unit defect and legacy consumer issue were traced. Prior statements that these exact results were missing retain their historical date.",
    "RESEARCH_INDEX.md": "Recovered H1/moments and BOJ primary records are now portable under the checkpoint evidence index. The deep audit includes the six-family census, full source-history checks and eleven synthetic contracts. Movement/cost-survival component evidence is not portfolio-profit proof.",
    "PENDING_IMPROVEMENTS_CURRENT.md": "Priority next: repair the legacy instrument-pip calculation in an isolated copy, then implement the bounded all68 global-clock comparison through at least24hours. Preserve existing task IDs below; this update does not silently close unrelated items or launch StageB.",
    "PROJECT_LOG_CURRENT.md": "User requested C-drive cleanup, audited Vault, a reproducible checkpoint and a clear next-machine handoff. Current source, inputs and reports were preserved; an inactive restore and eleven synthetic contracts passed. Live workers/account configuration remain untouched. Cleanup outcomes are recorded separately in the maintenance receipt."
}
for filename, summary in updates.items():
    previous = (MAINT / "before/projects/forex" / filename).read_text(encoding="utf-8")
    write(PROJECT / filename, "# September21,2026 recovery update\n\n" + summary + "\n\n[New-machine handoff](" + NAME + "/NEW_MACHINE_HANDOFF.md) · [exact next slice](" + NAME + "/audit_20260921/deep_audit_02/NEXT_OFFLINE_SLICE.md) · [deep audit](" + NAME + "/audit_20260921/deep_audit_02/DEEP_AUDIT_REPORT.md).\n\n---\n\n" + previous)
write(PROJECT / "KNOWLEDGE_INDEX.md", "# Forex record map — September21,2026\n\n[Start here](README.md) · [current recovery pointer](RECOVERY_CHECKPOINT_LATEST.json) · [audit](AUDIT_START_HERE.md) · [history](PROJECT_HISTORY.md) · [research](RESEARCH_INDEX.md) · [pending work](PENDING_IMPROVEMENTS_CURRENT.md).\n\nThe current recovery package contains the exact handoff from this chat and original audits. Its MANIFEST.json uses package-relative paths. The complete fresh Vault inventory is VAULT_FILE_INVENTORY.json; its scope and exclusions are explicit. VAULT_READABILITY_REPORT.json binds the inventory and refreshed navigation. These are local byte/readability checks, not scientific or cloud-sync certification.\n\nThe [previous index](maintenance/vault_refresh_20260921/before/projects/forex/KNOWLEDGE_INDEX.md) and immutable dated packages remain available. Files named CURRENT within older packages retain their recorded dates. [Cold archive locations](../../COLD_ARCHIVE_POINTERS_CURRENT.md) remain external dependencies; D is excluded.\n")
write(PROJECT / "AGENTS.md", "# Forex Vault handoff\n\nRead README.md, RECOVERY_CHECKPOINT_LATEST.json and the checkpoint NEW_MACHINE_HANDOFF.md first. Follow the current user's scope; dated chats and old CURRENT reports are evidence, not new launch instructions.\n\nThe Vault is a records/recovery layer, not the live runtime. Restore inactive using the included safe tool. Preserve modified/untracked source, private exclusions, original raw data, dated evidence and failures. The checkpoint's relative manifest is authoritative for included bytes; do not treat Git HEAD as the full worktree.\n\nNext research work is described in RECOVERY_CHECKPOINT_20260921/audit_20260921/deep_audit_02/NEXT_OFFLINE_SLICE.md: isolated legacy pip repair, then an all68 chronological offline comparison through at least24hours. Cleanup did not start that implementation. Existing workers/accounts remain untouched. D-only evidence is deferred after fresh disk errors. Cloud synchronization is unverified until destination readback.\n")
write(PROJECT / "maintenance/README.md", "# Vault maintenance\n\n[September21 cleanup, checkpoint and readback](vault_refresh_20260921/README.md) is the current maintenance entrypoint. [Preserved prior navigation](vault_refresh_20260921/PREIMAGES.json) identifies exact pre-update bytes.\n\nThe [September14 refresh](vault_refresh_20260914/README.md), September15 cleanup and all older dated receipts remain retained. Their operational observations and validation scopes do not change.\n")
write(MAINT / "README.md", "# September21 Vault refresh and C-drive cleanup\n\nThe user requested a soft project reinstall, audited Vault, reproducible checkpoint and clear next-machine handoff. [Current checkpoint](../../" + NAME + "/README.md) was restored into a fresh inactive folder with every member hash checked; [eleven synthetic contracts](CONTRACT_RESULTS.json) passed using restored source. [Restore receipt](RESTORE_VERIFICATION.json) and [13 restore-tool safety tests](RESTORE_TOOL_TESTS.json) retain exact validation scope.\n\n[Navigation preimages](PREIMAGES.json) preserve all16 replaced existing documents/pointers/indexes. Dated historical payloads remain unchanged. The root August20 CHECKPOINT_LATEST.json remains historical; RECOVERY_CHECKPOINT_LATEST.json is the new explicit recovery entrypoint.\n\nPrivate legacy source/account-identifier records were withheld from the shared package and listed by identity. Original sources remain on C. Credentials, active database state, accounts and runtime processes were not transferred or changed. New-package privacy review does not certify every older archive. Cloud upload completion is not verified.\n\nCLEANUP_RESULTS.json records the exact reviewed cache/cold-log actions. Raw diagnostic archives stay outside OneDrive at the recorded C path. VAULT_PUBLICATION_RECEIPT.json binds final navigation/inventory/readability results after cleanup.\n")
write(BASE.parent / "START_HERE.md", "# Forex project and recovery\n\nThe active source remains trad/. Current audit, recovery checkpoint and next steps are in C:\\Users\\zmoor\\OneDrive\\thevault\\projects\\forex\\README.md. This folder's dated studies are retained evidence; maintenance_20260921 records the latest cleanup and restore verification. Do not run the restored inspection copy or old launch instructions automatically.\n")
js(BASE / "NAVIGATION_PUBLICATION.json", {"status": "published_pending_final_vault_readback", "published_utc": stamp, "checkpoint": str(CP), "manifest_sha256": sha(CP / "MANIFEST.json"), "source_snapshot_id": source_pointer["snapshot_id"], "current_pointer": str(PROJECT / "RECOVERY_CHECKPOINT_LATEST.json"), "cloud_sync_verified": False})
print(json.dumps({"status": "navigation_published", "checkpoint": str(CP)}))
