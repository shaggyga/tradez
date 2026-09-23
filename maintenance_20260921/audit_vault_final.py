"""Bounded local Vault byte/navigation audit. Run only after publication.

Reads no D paths, follows no name-surrogate reparse points, hydrates no cloud
placeholders, opens no archives, and never reads private credential paths.
Only the four explicitly generated catalogs/receipt below are written.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import html
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import stat
from urllib.parse import unquote, urlsplit

VAULT = Path(r"C:\Users\zmoor\OneDrive\thevault")
PROJECT = "projects/forex"
MAINT = PROJECT + "/maintenance/vault_refresh_20260921"
CP = PROJECT + "/RECOVERY_CHECKPOINT_20260921"
CAP = 8 * 1024**3
TEXT_CAP = 20 * 1024**2
GENERATED = {
    PROJECT + "/VAULT_FILE_INVENTORY.json",
    PROJECT + "/SHARED_PROJECT_STATE_CURRENT.json",
    PROJECT + "/VAULT_READABILITY_REPORT.json",
    MAINT + "/VAULT_PUBLICATION_RECEIPT.json",
}
STRICT_NAV = ["README.md", "INDEX.md", PROJECT + "/README.md",
              PROJECT + "/VAULT_AUDIT_SUMMARY_20260921.md",
              PROJECT + "/AUDIT_START_HERE.md", PROJECT + "/RECREATION.md",
              PROJECT + "/KNOWLEDGE_INDEX.md", PROJECT + "/maintenance/README.md",
              MAINT + "/README.md", CP + "/README.md", CP + "/NEW_MACHINE_HANDOFF.md"]
CURRENT = set(STRICT_NAV) | {"index.json", "RECOVERY_CHECKPOINT_LATEST.json"} | {
    PROJECT + "/" + name for name in ("AGENTS.md", "PROJECT_HISTORY.md", "RESEARCH_INDEX.md",
        "PENDING_IMPROVEMENTS_CURRENT.md", "PROJECT_LOG_CURRENT.md", "RECOVERY_CHECKPOINT_LATEST.json",
        "source/WORKTREE_SOURCE_LATEST.json", "source/PRIVATE_DEPENDENCIES.json")}
APPENDED_NAV = {PROJECT + "/" + name for name in ("PROJECT_HISTORY.md", "RESEARCH_INDEX.md",
                 "PENDING_IMPROVEMENTS_CURRENT.md", "PROJECT_LOG_CURRENT.md")}
BANNED_NAMES = {"creds", ".env", "credentials.json", "secrets.json", "auth.json", "token.json",
                "accounts_registry.json", ".netrc", ".npmrc", ".pypirc",
                "id_rsa", "id_dsa", "id_ecdsa", "id_ed25519"}
BANNED_DIRS = {"creds", "credentials", "secrets", "private_local_export"}
BANNED_STEMS = {"creds", "credentials", "secrets", "auth", "token", "accounts_registry"}
PRIVATE_SUFFIXES = {".env", ".ini", ".json", ".toml", ".yaml", ".yml"}
PLACEHOLDER_FLAGS = 0x1000 | 0x400000 | 0x40000  # Offline / RecallOnDataAccess / RecallOnOpen


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return (json.dumps(value, indent=2, ensure_ascii=True) + "\n").encode("utf-8")


def private_path(name):
    parts = PurePosixPath(name).parts
    lowered = [part.lower() for part in parts]
    filename = lowered[-1]
    if any(part in BANNED_DIRS for part in lowered[:-1]):
        return True
    if filename in BANNED_NAMES or filename.startswith((".env.", "creds.", "credentials.", "secrets.")):
        return True
    path = PurePosixPath(filename)
    return ((path.stem in BANNED_STEMS and path.suffix in PRIVATE_SUFFIXES)
            or path.suffix in {".pem", ".key", ".pfx", ".p12", ".kdbx", ".crt", ".cer"}
            or filename.endswith((".private.json", ".local.json", ".secrets.json")))


def unavailable(info):
    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_reparse_tag", 0) & 0x20000000:
        return "symlink_or_name_surrogate_reparse; not followed"
    if getattr(info, "st_file_attributes", 0) & PLACEHOLDER_FLAGS:
        return "offline_or_recall_cloud_placeholder; not hydrated"
    return None


def current_content(name):
    if name in CURRENT:
        return True
    if name.startswith(MAINT + "/"):
        return not name.startswith(MAINT + "/before/")
    if name.startswith(CP + "/"):
        return not name.startswith((CP + "/audit_20260921/", CP + "/evidence/"))
    return False


def enumerate_local():
    regular, directories, skipped, errors = [], {""}, [], []
    stack = [(VAULT, "")]
    while stack:
        folder, rel_folder = stack.pop()
        try:
            entries = sorted(os.scandir(folder), key=lambda item: item.name.casefold())
        except OSError as exc:
            errors.append({"path": rel_folder, "reason": "directory_metadata_unreadable", "error_type": type(exc).__name__})
            continue
        for entry in entries:
            name = (PurePosixPath(rel_folder) / entry.name).as_posix()
            if name in GENERATED:
                continue
            if private_path(name) or entry.name.lower() in BANNED_DIRS:
                skipped.append({"path": name, "reason": "private_credential_or_private_export_path; not read"})
                continue
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError as exc:
                errors.append({"path": name, "reason": "metadata_unreadable", "error_type": type(exc).__name__})
                continue
            reason = unavailable(info)
            if reason:
                skipped.append({"path": name, "reason": reason, "size_metadata": info.st_size})
            elif stat.S_ISDIR(info.st_mode):
                directories.add(name)
                stack.append((Path(entry.path), name))
            elif stat.S_ISREG(info.st_mode):
                regular.append((name, Path(entry.path), info))
            else:
                skipped.append({"path": name, "reason": "not_a_regular_file_or_directory"})
    return sorted(regular), directories, skipped, errors


def without_code(text):
    # Markdown code examples and inline-code paths are not navigation links.
    lines, fence = [], None
    for line in text.splitlines():
        marker = re.match(r"^\s{0,3}(`{3,}|~{3,})", line)
        if marker:
            value = marker.group(1)
            if fence is None:
                fence = value
            elif value[0] == fence[0] and len(value) >= len(fence):
                fence = None
            lines.append("")
        else:
            lines.append(line if fence is None else "")
    return re.sub(r"(`+)([^`]|(?!\1)`)*?\1", "", "\n".join(lines))


def markdown_links(text):
    text = without_code(text)
    definitions = {}
    for match in re.finditer(r"(?m)^\s{0,3}\[([^\]]+)\]:\s*(<[^>]+>|\S+)", text):
        definitions[match.group(1).strip().casefold()] = match.group(2).strip("<>")
    inline = re.compile(r"!?\[[^\]\n]*\]\(\s*(<[^>]+>|(?:\\.|[^\s)])+)(?:\s+[^)]+)?\s*\)")
    for match in inline.finditer(text):
        yield match.group(1).strip("<>")
    for match in re.finditer(r"!?\[([^\]\n]+)\]\[([^\]\n]*)\]", text):
        key = (match.group(2) or match.group(1)).strip().casefold()
        if key in definitions:
            yield definitions[key]


def classify_link(origin, target, available, directories, skipped):
    target = html.unescape(target)
    if target.startswith("#"):
        return "fragment_not_checked", None
    if PureWindowsPath(target).drive or target.startswith(("\\", "/")):
        return "absolute_machine_path_not_accessed", None
    parsed = urlsplit(target)
    if parsed.scheme:
        return "external_url_not_fetched", None
    path = unquote(parsed.path).replace("\\", "/")
    path = re.sub(r":\d+$", "", path)
    parts = list(PurePosixPath(origin).parent.parts)
    for part in path.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if not parts:
                return "relative_external_dependency_not_accessed", None
            parts.pop()
        else:
            parts.append(part)
    resolved = "/".join(parts)
    key = resolved.casefold()
    if key in available:
        return "local_file_hashed", available[key]
    if key in {name.casefold() for name in GENERATED}:
        return "generated_catalog_written_by_this_audit", resolved
    if key in directories:
        return "local_directory", directories[key]
    for skipped_name, reason in skipped.items():
        if key == skipped_name or key.startswith(skipped_name + "/"):
            return "target_not_read__" + reason.split(";")[0], resolved
    return "missing_internal_target", resolved


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--published", action="store_true", help="Explicitly confirm navigation/package publication is complete")
    parser.add_argument("--preflight", action="store_true", help="Read current navigation/JSON and local target metadata only; write nothing")
    args = parser.parse_args()
    if not (args.published or args.preflight):
        parser.error("Prepared only. Run with --published only after the root agent confirms publication is complete.")
    if VAULT.drive.upper() != "C:" or unavailable(VAULT.lstat()):
        raise RuntimeError("Vault must be a local C-drive directory without recall/link flags")
    stamp = datetime.now(timezone.utc).isoformat()
    regular, directories, skipped, errors = enumerate_local()
    if args.preflight:
        available = {name.casefold(): name for name, _, _ in regular}
        directory_map = {name.casefold(): name for name in directories}
        skipped_map = {row["path"].casefold(): row["reason"] for row in skipped}
        issues, checked, bytes_read = [], [], 0
        for name, path, info in regular:
            if not current_content(name) or path.suffix.lower() not in {".json", ".md"} or info.st_size > TEXT_CAP:
                continue
            if bytes_read + info.st_size > 100 * 1024**2:
                raise RuntimeError("Preflight text read bound exceeded")
            if unavailable(path.lstat()):
                issues.append({"path": name, "reason": "became_unavailable"})
                continue
            raw = path.read_bytes()
            bytes_read += len(raw)
            try:
                text = raw.decode("utf-8-sig")
                if path.suffix.lower() == ".json":
                    json.loads(text, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite")))
                checked.append(name)
            except (UnicodeError, ValueError) as exc:
                issues.append({"path": name, "reason": "parse_or_UTF8_error", "error_type": type(exc).__name__})
                continue
            if name in STRICT_NAV or name in APPENDED_NAV:
                if name in APPENDED_NAV:
                    text = re.split(r"\r?\n---\r?\n", text, maxsplit=1)[0]
                for target in markdown_links(text):
                    kind, resolved = classify_link(name, target, available, directory_map, skipped_map)
                    if kind == "missing_internal_target" or kind.startswith("target_not_read__"):
                        issues.append({"origin": name, "target": target, "classification": kind, "resolved": resolved})
        for name in STRICT_NAV:
            if name not in checked:
                issues.append({"path": name, "reason": "required_navigation_missing_or_unreadable"})
        print(json.dumps({"mode": "read_only_preflight", "files_checked": len(checked), "bytes_read": bytes_read,
                          "current_navigation_documents": len(STRICT_NAV), "issues": issues,
                          "enumeration_errors": errors, "writes": 0}, indent=2))
        return 0 if not (issues or errors) else 2
    files, texts, jsons, parse_issues, format_counts = [], {}, {}, [], Counter()
    total = 0
    for name, path, before in regular:
        if total + before.st_size > CAP:
            skipped.append({"path": name, "reason": "8_GiB_content_read_budget_exceeded", "size_metadata": before.st_size})
            errors.append({"path": name, "reason": "content_read_budget_exceeded"})
            continue
        h, raw_parts = hashlib.sha256(), []
        suffix = path.suffix.lower()
        parse_text = suffix in {".json", ".md"} and before.st_size <= TEXT_CAP
        try:
            # Recheck immediately before open, including attributes that can change after enumeration.
            immediate = path.lstat()
            if unavailable(immediate):
                skipped.append({"path": name, "reason": unavailable(immediate)})
                continue
            with path.open("rb") as handle:
                while block := handle.read(1024 * 1024):
                    h.update(block)
                    total += len(block)
                    if total > CAP:
                        raise RuntimeError("Cumulative content-read bound exceeded")
                    if parse_text:
                        raw_parts.append(block)
            after = path.lstat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                errors.append({"path": name, "reason": "changed_during_read"})
                continue
        except OSError as exc:
            errors.append({"path": name, "reason": "content_unreadable", "error_type": type(exc).__name__})
            continue
        files.append({"path": name, "size": before.st_size, "sha256": h.hexdigest(),
                      "modified_utc": datetime.fromtimestamp(before.st_mtime, timezone.utc).isoformat(),
                      "unchanged_during_read": True})
        if parse_text:
            try:
                text = b"".join(raw_parts).decode("utf-8-sig")
                if suffix == ".json":
                    def reject_nonfinite(_value):
                        raise ValueError("nonfinite JSON constant")
                    jsons[name] = json.loads(text, parse_constant=reject_nonfinite)
                else:
                    texts[name] = text
                format_counts[suffix] += 1
            except (UnicodeError, ValueError) as exc:
                # Error strings may echo file content, so retain only type and safe position fields.
                parse_issues.append({"path": name, "classification": "new_publication" if current_content(name) else "retained_historical_content",
                                     "error_type": type(exc).__name__, "line": getattr(exc, "lineno", None),
                                     "column": getattr(exc, "colno", None)})
        elif suffix in {".json", ".md"}:
            format_counts["text_over_20_MiB_not_parsed"] += 1
    by_name = {row["path"]: row for row in files}
    available = {name.casefold(): name for name in by_name}
    directory_map = {name.casefold(): name for name in directories}
    skipped_map = {row["path"].casefold(): row["reason"] for row in skipped}
    navigation_errors, references, historical_broken = [], [], []
    for name in STRICT_NAV:
        if name not in texts:
            navigation_errors.append({"origin": name, "reason": "required_current_navigation_not_readable_locally"})
    for origin, text in sorted(texts.items()):
        current_section = re.split(r"\r?\n---\r?\n", text, maxsplit=1)[0] if origin in APPENDED_NAV else None
        current_targets = set(markdown_links(current_section)) if current_section is not None else set()
        for target in markdown_links(text):
            kind, resolved = classify_link(origin, target, available, directory_map, skipped_map)
            is_current = origin in STRICT_NAV or (origin in APPENDED_NAV and target in current_targets)
            row = {"origin": origin, "target": target, "classification": kind, "resolved": resolved,
                   "scope": "current_navigation" if is_current else "historical_or_supporting_document"}
            references.append(row)
            if kind == "missing_internal_target" or kind.startswith("target_not_read__"):
                (navigation_errors if is_current else historical_broken).append(row)

    def require_json(name):
        value = jsons.get(name)
        if not isinstance(value, dict):
            errors.append({"path": name, "reason": "required_current_JSON_missing_or_invalid"})
            return {}
        return value

    def check_hash(name, expected, reason):
        row = by_name.get(name)
        if not row or row["sha256"] != expected:
            errors.append({"path": name, "reason": reason})
            return False
        return True

    preimage_name = MAINT + "/PREIMAGES.json"
    preimages = require_json(preimage_name)
    preserved = [row for row in preimages.get("files", []) if row.get("existed")]
    preservation_checks = [check_hash(row["preserved_path"], row["sha256"], "navigation_preimage_hash_mismatch") for row in preserved]
    if len(preserved) != 16:
        errors.append({"path": preimage_name, "reason": "expected_16_preserved_navigation_preimages"})
    prior_name = MAINT + "/before/projects/forex/SHARED_PROJECT_STATE_CURRENT.json"
    source_name = PROJECT + "/source/WORKTREE_SOURCE_LATEST.json"
    source = require_json(source_name)
    source_manifest_name = PROJECT + "/source/" + str(source.get("manifest", "missing"))
    source_manifest = require_json(source_manifest_name)
    source_archive_name = PROJECT + "/source/" + str(source.get("archive", "missing"))
    check_hash(source_manifest_name, source.get("manifest_sha256"), "current_source_manifest_hash_mismatch")
    check_hash(source_archive_name, source.get("archive_sha256"), "current_source_archive_hash_mismatch")
    if source_manifest.get("file_count") != 4881 or source_manifest.get("privacy_review", {}).get("total_withheld_paths") != 69:
        errors.append({"path": source_manifest_name, "reason": "expected_4881_source_members_and_69_explicit_privacy_exclusions"})
    CURRENT.update({source_manifest_name, source_archive_name})
    recovery_name = PROJECT + "/RECOVERY_CHECKPOINT_LATEST.json"
    recovery = require_json(recovery_name)
    root_recovery = require_json("RECOVERY_CHECKPOINT_LATEST.json")
    check_hash(recovery_name, root_recovery.get("pointer_sha256"), "root_recovery_redirect_hash_mismatch")
    checkpoint_manifest_name = CP + "/MANIFEST.json"
    checkpoint_manifest = require_json(checkpoint_manifest_name)
    check_hash(checkpoint_manifest_name, recovery.get("manifest_sha256"), "recovery_manifest_hash_mismatch")
    check_hash(checkpoint_manifest_name, "00d879d1ef0b13261b97f2f4f08da1acc5aa33755122057e29df2d5b72ae3e07",
               "checkpoint_manifest_differs_from_reviewed_final_identity")
    if len(checkpoint_manifest.get("files", [])) != 171:
        errors.append({"path": checkpoint_manifest_name, "reason": "expected_171_checkpoint_whole_file_records"})
    package_checks = []
    for row in checkpoint_manifest.get("files", []):
        name = CP + "/" + row["path"]
        okay = check_hash(name, row.get("sha256"), "checkpoint_loose_file_or_archive_hash_mismatch")
        if okay and by_name[name]["size"] != row.get("bytes"):
            errors.append({"path": name, "reason": "checkpoint_file_size_mismatch"})
            okay = False
        package_checks.append(okay)
    restore_name = MAINT + "/RESTORE_VERIFICATION.json"
    restore = require_json(restore_name)
    check_hash(restore_name, recovery.get("local_restore_receipt_sha256"), "local_restore_receipt_hash_mismatch")
    if restore.get("status") != "verified" or restore.get("manifest_sha256") != recovery.get("manifest_sha256"):
        errors.append({"path": restore_name, "reason": "restore_receipt_does_not_verify_current_manifest"})
    if source.get("snapshot_id") != checkpoint_manifest.get("source_snapshot_id") or source.get("snapshot_id") != recovery.get("source_snapshot_id"):
        errors.append({"path": source_name, "reason": "source_snapshot_lineage_mismatch"})
    cleanup = require_json(MAINT + "/CLEANUP_RESULTS.json")
    evidence = require_json(CP + "/evidence/EVIDENCE_INDEX.json")
    evidence_exclusions = len(evidence.get("privacy_excluded_primary_receipts", []))
    if evidence_exclusions != 2 or evidence.get("file_count") != 30:
        errors.append({"path": CP + "/evidence/EVIDENCE_INDEX.json", "reason": "expected_30_evidence_files_and_2_explicit_privacy_exclusions"})
    # Current canonical copies must be byte-identical to the packaged copies.
    for suffix in (source.get("manifest"), source.get("archive"), "WORKTREE_SOURCE_LATEST.json", "PRIVATE_DEPENDENCIES.json"):
        if suffix:
            package_row = by_name.get(CP + "/source/" + suffix)
            check_hash(PROJECT + "/source/" + suffix, package_row["sha256"] if package_row else None,
                       "canonical_source_differs_from_packaged_source")
    for name in CURRENT:
        if name not in by_name:
            errors.append({"path": name, "reason": "required_current_record_not_hashed_locally"})
    for row in parse_issues:
        row["classification"] = "new_publication" if current_content(row["path"]) else "retained_historical_content"
    new_parse = [row for row in parse_issues if row["classification"] == "new_publication"]
    historical_parse = [row for row in parse_issues if row["classification"] != "new_publication"]
    status = "pass_with_declared_historical_and_external_limits" if not (errors or navigation_errors or new_parse) else "failed_current_publication_readback"
    inventory = {"schema_version": "vault_root_file_inventory_v2", "generated_utc": stamp,
                 "path_base": "Vault root; every files[].path is relative to C:\\Users\\zmoor\\OneDrive\\thevault",
                 "scope": "SHA256 of locally readable regular files beneath the entire Vault. No archive/member inspection, cloud hydration or scientific replay.",
                 "excluded_generated_files": sorted(GENERATED), "file_count": len(files),
                 "payload_bytes": sum(row["size"] for row in files), "content_bytes_read": total,
                 "read_budget_bytes": CAP, "skipped": skipped, "read_errors": errors, "files": files}
    inv_raw = encoded(inventory)
    record_names = sorted(CURRENT | {checkpoint_manifest_name, preimage_name, restore_name,
        MAINT + "/CONTRACT_RESULTS.json", MAINT + "/RESTORE_TOOL_TESTS.json", MAINT + "/CLEANUP_RESULTS.json",
        MAINT + "/CURRENT_SOURCE_PRESERVATION.json", MAINT + "/RESTORE_SCRATCH_CLEANUP.json",
        CP + "/verification/LOOSE_RECORD_PRIVACY_REVIEW.json", CP + "/evidence/EVIDENCE_INDEX.json"})
    records = [{"name": name[len(PROJECT) + 1:] if name.startswith(PROJECT + "/") else "../../" + name,
                "vault_path": name, "source": "vault/" + name, "size": by_name[name]["size"],
                "sha256": by_name[name]["sha256"], "relationship": "current_navigation_or_exact_publication_receipt"}
               for name in record_names if name in by_name]
    state = {"schema_version": 2, "generated_utc": stamp, "record_count": len(records),
             "scope": "Actual final bytes of current navigation, recovery/source pointers and bounded verification receipts. Historical 208-record manifest is preserved unchanged, not silently carried forward as current.",
             "prior_manifest": "maintenance/vault_refresh_20260921/before/projects/forex/SHARED_PROJECT_STATE_CURRENT.json",
             "prior_manifest_sha256": by_name.get(prior_name, {}).get("sha256"), "inventory_sha256": sha(inv_raw),
             "source_pointer": "source/WORKTREE_SOURCE_LATEST.json", "source_snapshot_id": source.get("snapshot_id"),
             "recovery_pointer": "RECOVERY_CHECKPOINT_LATEST.json", "cloud_sync_verified": False, "records": records}
    state_raw = encoded(state)
    report = {"schema_version": "forex_vault_readability_v3", "generated_utc": stamp, "status": status,
              "scope": "Entire local Vault file hashing; UTF8 and strict JSON syntax up to 20 MiB; current Markdown relative links; publication hash bindings. Historical archive privacy, runtime and scientific replay are not certified.",
              "record_count": len(records), "inventory_file_count": len(files), "inventory_payload_bytes": inventory["payload_bytes"],
              "inventory_sha256": sha(inv_raw), "canonical_manifest_sha256": sha(state_raw),
              "knowledge_index_sha256": by_name.get(PROJECT + "/KNOWLEDGE_INDEX.md", {}).get("sha256"),
              "source_pointer_sha256": by_name.get(source_name, {}).get("sha256"),
              "source_snapshot_id": source.get("snapshot_id"), "source_archive_sha256": source.get("archive_sha256"),
              "source_members_in_bound_manifest": source_manifest.get("file_count"), "source_archive_members_reopened": False,
              "checkpoint_files_hash_checked": len(package_checks), "checkpoint_file_hashes_all_match": bool(package_checks) and all(package_checks),
              "navigation_documents": STRICT_NAV, "additional_current_sections": sorted(APPENDED_NAV),
              "navigation_errors": navigation_errors, "new_parse_issues": new_parse,
              "historical_parse_issues": historical_parse, "historical_broken_links": historical_broken,
              "read_errors": errors, "skipped": skipped, "readable_format_counts": dict(format_counts),
              "reference_classification_counts": dict(Counter(row["classification"] for row in references)),
              "preservation": {"preimages_verified": len(preservation_checks), "all_preimages_match": bool(preservation_checks) and all(preservation_checks),
                               "dated_historical_payloads_modified_by_this_audit": 0},
              "cloud_sync_verified": False,
              "limits": ["Local availability only; cloud synchronization and destination readback remain unverified.",
                         "Offline/recall placeholders and name-surrogate links were not followed or hydrated; exclusions are explicit.",
                         "Private credential paths were not read; all older archive privacy is outside this audit.",
                         "ZIP bytes were hashed, but archive members were not reopened here; the separate bound restore receipt supplies its prior verification scope.",
                         "Markdown check covers inline/reference file links outside code, not fragment existence, bare prose paths, or all Markdown extensions.",
                         "Historical missing links/parse defects remain preserved evidence; current-navigation success does not repair those records.",
                         "No D access, network, services, project execution, tests or model loading; no full environment/runtime recreation claim.",
                         "File stability was checked during each read, not as a filesystem-wide atomic snapshot."], "references": references}
    report_raw = encoded(report)
    generated_payloads = {PROJECT + "/VAULT_FILE_INVENTORY.json": inv_raw,
                          PROJECT + "/SHARED_PROJECT_STATE_CURRENT.json": state_raw,
                          PROJECT + "/VAULT_READABILITY_REPORT.json": report_raw}
    receipt = {"schema_version": "forex_vault_publication_receipt_v1", "generated_utc": stamp, "status": status,
               "path_base": "Vault root", "generated_catalogs": [{"path": name, "bytes": len(raw), "sha256": sha(raw)} for name, raw in generated_payloads.items()],
               "binding": "Catalogs and this receipt are excluded from inventory to avoid self-referential hashes. Receipt hashes all three catalogs; state binds inventory; readability binds both.",
               "checkpoint_manifest": {"path": checkpoint_manifest_name, "sha256": by_name.get(checkpoint_manifest_name, {}).get("sha256")},
               "source_pointer": {"path": source_name, "sha256": by_name.get(source_name, {}).get("sha256")},
               "preimage_receipt": {"path": preimage_name, "sha256": by_name.get(preimage_name, {}).get("sha256"), "verified_count": len(preservation_checks)},
               "inventory_files": len(files), "inventory_bytes": inventory["payload_bytes"],
               "current_navigation_errors": len(navigation_errors), "new_parse_issues": len(new_parse),
               "historical_parse_issues": len(historical_parse), "historical_broken_links": len(historical_broken),
               "skipped_count": len(skipped), "read_error_count": len(errors),
               "private_source_files_withheld": source_manifest.get("privacy_review", {}).get("total_withheld_paths"),
               "private_evidence_receipts_withheld": evidence_exclusions, "cloud_sync_verified": False,
               "scope_limits": report["limits"]}
    generated_payloads[MAINT + "/VAULT_PUBLICATION_RECEIPT.json"] = encoded(receipt)
    for name, raw in generated_payloads.items():
        target = VAULT / name
        # These are the only mutable outputs. Historical counterparts already have verified preimages.
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and unavailable(target.lstat()):
            raise RuntimeError("Refusing to write an unavailable/generated linked target")
        temporary = target.with_name(target.name + ".audit-pending")
        with temporary.open("xb") as handle:
            handle.write(raw)
        os.replace(temporary, target)
        if target.read_bytes() != raw:
            raise RuntimeError("Generated catalog readback mismatch")
    print(json.dumps({"status": status, "files_hashed": len(files), "payload_bytes": inventory["payload_bytes"],
                      "current_records": len(records), "navigation_errors": len(navigation_errors), "new_parse_issues": len(new_parse),
                      "historical_parse_issues": len(historical_parse), "historical_broken_links": len(historical_broken),
                      "skipped": len(skipped), "read_errors": len(errors), "receipt": str(VAULT / MAINT / "VAULT_PUBLICATION_RECEIPT.json")}, indent=2))
    return 0 if status.startswith("pass") else 2


if __name__ == "__main__":
    raise SystemExit(main())
