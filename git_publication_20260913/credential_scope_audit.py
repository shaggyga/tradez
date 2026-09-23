"""Bounded, redacted publication audit; uses the existing repository scanner."""
from __future__ import annotations

import argparse
import collections
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import stat
import subprocess
import sys
from datetime import datetime, timezone

sys.dont_write_bytecode = True
ROOT = Path(r"C:\Users\zmoor\Documents\forex\trad")
OUT = Path(r"C:\Users\zmoor\Documents\forex\git_publication_20260913")
SCANNER = ROOT / "tools" / "credential_audit.py"
MAX_FILE = 5 * 1024 * 1024
MAX_TOTAL = 256 * 1024 * 1024
TEXT_SUFFIXES = {".py", ".pyi", ".md", ".txt", ".json", ".ps1", ".cjs", ".js", ".ts", ".tsx", ".jsx", ".html", ".css", ".svg", ".xml", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".sh", ".bat", ".cmd"}
TEXT_NAMES = {".gitignore", ".gitattributes", "Dockerfile", "Makefile"}
RUNTIME_DIRS = {"data", "artifacts", "tmp", ".venv", "venv", "env", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", "pytest", "logs", "models"}
BROWSER_PROFILE_DIRS = {"edge-profile", "chrome-profile", "browser-profile", "user data"}
DATA_PAYLOAD_NAMES = {"normalized_forecasts.json", "selected_candidate_rows.json", "selected_rows.txt", "captured_week_candles.json", "strict_input_final.json", "independently_paired_endpoints.json"}


def git(*args: str) -> bytes:
    result = subprocess.run(["git", "-C", str(ROOT), *args], capture_output=True)
    if result.returncode:
        raise RuntimeError("Git metadata/object operation failed; raw output suppressed")
    return result.stdout


def check_local_chain(path: Path) -> os.stat_result:
    if path.drive.upper() != "C:":
        raise ValueError("non_c_path")
    current = Path(path.anchor)
    last = current.lstat()
    for part in path.parts[1:]:
        current /= part
        last = current.lstat()
        if stat.S_ISLNK(last.st_mode) or getattr(last, "st_file_attributes", 0) & 0x400:
            raise ValueError("reparse_path")
    return last


def setup_scanner():
    check_local_chain(SCANNER)
    check_local_chain(ROOT / ".git" / "objects")
    if (ROOT / ".git" / "objects" / "info" / "alternates").exists():
        raise ValueError("git_alternates_not_allowed")
    spec = importlib.util.spec_from_file_location("reviewed_credential_audit", SCANNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def path_exclusion(path: str, scanner) -> str | None:
    pure = PurePosixPath(path)
    if pure.is_absolute() or ".." in pure.parts or ":" in path or "\\" in path:
        return "unsafe_relative_path"
    if scanner.path_is_banned(path):
        return "banned_private_path"
    lower_parts = [part.lower() for part in pure.parts]
    name = lower_parts[-1]
    if any(part in BROWSER_PROFILE_DIRS for part in lower_parts[:-1]):
        return "private_browser_runtime"
    if path.startswith("docs/validation/") and name in DATA_PAYLOAD_NAMES:
        return "generated_market_data_payload"
    if name.endswith((".private.json", ".local.json", ".secrets.json")) or name.startswith("id_rsa"):
        return "private_filename"
    if any(part in RUNTIME_DIRS or part.startswith("owned_installed") for part in lower_parts[:-1]):
        return "runtime_directory"
    if pure.suffix.lower() not in TEXT_SUFFIXES and pure.name not in TEXT_NAMES:
        return "unsupported_non_source_format"
    return None


def inventory(scanner) -> dict:
    candidate_paths = sorted(set(part.decode("utf-8") for part in git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0") if part))
    current = []
    for path in candidate_paths:
        item = {"path": path, "scope": "candidate"}
        reason = path_exclusion(path, scanner)
        if not reason:
            try:
                metadata = check_local_chain(ROOT / path)
                item["size"] = metadata.st_size
                item["mtime_ns"] = metadata.st_mtime_ns
                if not stat.S_ISREG(metadata.st_mode):
                    reason = "non_regular_file"
                elif metadata.st_size > MAX_FILE:
                    reason = "individual_size_limit"
            except (OSError, ValueError) as exc:
                reason = str(exc) if isinstance(exc, ValueError) else "missing_or_unreadable_metadata"
        item["excluded_reason"] = reason
        current.append(item)

    revisions = git("rev-list", "--all").decode("ascii").splitlines()
    versions = {}
    for revision in revisions:
        records = git("ls-tree", "-r", "-l", "-z", revision).split(b"\0")
        for record in records:
            if not record:
                continue
            header, path_raw = record.split(b"\t", 1)
            mode, kind, oid, size_raw = header.split()
            path = path_raw.decode("utf-8")
            key = (path, oid.decode("ascii"))
            if key in versions:
                versions[key]["reachable_commit_count"] += 1
                continue
            item = {"scope": "history", "path": path, "blob_oid": key[1], "reachable_commit_count": 1, "representative_commit": revision}
            reason = path_exclusion(path, scanner)
            if kind != b"blob" or mode not in {b"100644", b"100755"}:
                reason = reason or "non_regular_git_entry"
            else:
                item["size"] = int(size_raw)
                if item["size"] > MAX_FILE:
                    reason = reason or "individual_size_limit"
            item["excluded_reason"] = reason
            versions[key] = item
    return {
        "schema_version": "bounded_credential_scope_v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "root": str(ROOT),
        "head": git("rev-parse", "HEAD").decode("ascii").strip(),
        "revisions": revisions,
        "individual_byte_limit": MAX_FILE,
        "total_payload_read_limit": MAX_TOTAL,
        "candidate": current,
        "history": sorted(versions.values(), key=lambda item: (item["path"], item["blob_oid"])),
    }


def inventory_summary(scope: dict) -> dict:
    result = {"reachable_commits": len(scope["revisions"])}
    for label in ("candidate", "history"):
        entries = scope[label]
        excluded = [item for item in entries if item["excluded_reason"]]
        result[label] = {
            "entry_count": len(entries),
            "approved_count": len(entries) - len(excluded),
            "approved_bytes_before_content_deduplication": sum(item.get("size", 0) for item in entries if not item["excluded_reason"]),
            "exclusion_counts": dict(collections.Counter(item["excluded_reason"] for item in excluded)),
            "excluded_entries": [{key: item[key] for key in ("path", "size", "excluded_reason") if key in item} for item in excluded],
            "extension_counts": dict(collections.Counter(PurePosixPath(item["path"]).suffix.lower() for item in entries)),
        }
    return result


def examine_payload(scanner, path: str, payload: bytes) -> dict:
    if b"\0" in payload:
        return {"excluded_reason": "binary_content_in_text_extension"}
    findings = []
    for rule, pattern in scanner.PATTERNS.items():
        for match in pattern.finditer(payload):
            material = match.group(1) if match.lastindex else match.group(0)
            if rule == "credential_assignment" and scanner.synthetic_fixture_value(path, material):
                continue
            if rule == "credential_assignment" and scanner.credential_assignment_is_reference(path, match):
                continue
            findings.append({"line": payload.count(b"\n", 0, match.start()) + 1, "rule": rule, "value_sha256_prefix": hashlib.sha256(material).hexdigest()[:16]})
    account_ids = [{"line": payload.count(b"\n", 0, match.start()) + 1, "rule": "private_account_id", "value_sha256_prefix": hashlib.sha256(match.group()).hexdigest()[:16]} for match in scanner.ACCOUNT_ID.finditer(payload)]
    return {"findings": findings, "account_ids": account_ids}


def scan(scope: dict, scanner) -> dict:
    if git("rev-parse", "HEAD").decode("ascii").strip() != scope["head"] or git("rev-list", "--all").decode("ascii").splitlines() != scope["revisions"]:
        raise ValueError("Git history changed after scope inventory")
    findings, accounts, exclusions = [], [], []
    cache = {}
    physical_bytes = 0
    scanned_counts = collections.Counter()
    deduplicated_counts = collections.Counter()
    current_digests = []
    for label in ("candidate", "history"):
        for item in scope[label]:
            path = item["path"]
            context = {key: item[key] for key in ("scope", "path", "blob_oid", "representative_commit", "reachable_commit_count") if key in item}
            if item["excluded_reason"]:
                exclusions.append({**context, "rule": item["excluded_reason"]})
                continue
            key = (path, item.get("blob_oid"))
            if label == "history" and key in cache:
                result = cache[key]
                deduplicated_counts[label] += 1
            else:
                if physical_bytes + item["size"] > MAX_TOTAL:
                    exclusions.append({**context, "rule": "total_read_limit"})
                    continue
                if label == "candidate":
                    try:
                        metadata = check_local_chain(ROOT / path)
                        if metadata.st_size != item["size"] or metadata.st_mtime_ns != item["mtime_ns"]:
                            exclusions.append({**context, "rule": "changed_after_inventory"})
                            continue
                        with (ROOT / path).open("rb") as stream:
                            payload = stream.read(MAX_FILE + 1)
                        if len(payload) != item["size"]:
                            exclusions.append({**context, "rule": "changed_during_read"})
                            continue
                    except (OSError, ValueError):
                        exclusions.append({**context, "rule": "unreadable_or_unsafe_candidate"})
                        continue
                    oid = hashlib.sha1(b"blob " + str(len(payload)).encode("ascii") + b"\0" + payload).hexdigest()
                    key = (path, oid)
                    current_digests.append({"path": path, "sha256": hashlib.sha256(payload).hexdigest(), "size": len(payload)})
                else:
                    payload = git("cat-file", "blob", item["blob_oid"])
                    if len(payload) != item["size"]:
                        raise RuntimeError("Git object size changed unexpectedly")
                physical_bytes += len(payload)
                result = examine_payload(scanner, path, payload)
                cache[key] = result
            if result.get("excluded_reason"):
                exclusions.append({**context, "rule": result["excluded_reason"]})
                continue
            scanned_counts[label] += 1
            findings.extend({**context, **finding} for finding in result["findings"])
            accounts.extend({**context, **finding} for finding in result["account_ids"])
    candidate_complete = not any(item["scope"] == "candidate" for item in exclusions)
    history_complete = not any(item["scope"] == "history" for item in exclusions)
    return {
        "schema_version": "bounded_redacted_credential_audit_v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "audited_head": scope["head"],
        "reachable_commits": len(scope["revisions"]),
        "scanner_sha256": hashlib.sha256(SCANNER.read_bytes()).hexdigest(),
        "rules": sorted(scanner.PATTERNS),
        "summary": {
            "scanned_entries": dict(scanned_counts),
            "deduplicated_payload_reads": dict(deduplicated_counts),
            "physical_payload_bytes_read": physical_bytes,
            "finding_count": len(findings),
            "finding_counts_by_scope": dict(collections.Counter(item["scope"] for item in findings)),
            "finding_counts_by_rule": dict(collections.Counter(item["rule"] for item in findings)),
            "private_account_id_occurrences": len(accounts),
            "private_account_id_unique_fingerprints": len({item["value_sha256_prefix"] for item in accounts}),
            "exclusion_count": len(exclusions),
            "exclusion_counts": dict(collections.Counter(item["rule"] for item in exclusions)),
            "candidate_complete": candidate_complete,
            "history_complete": history_complete,
            "passed_scanned_text": not findings,
            "complete_and_passed": candidate_complete and history_complete and not findings,
        },
        "findings": findings,
        "private_account_metadata": accounts,
        "exclusions": exclusions,
        "candidate_content_digests": current_digests,
        "limitations": ["Pattern-based scan cannot establish absence of every secret.", "Account identifiers are separately reported private metadata, not bearer credentials.", "History versions are unique path/blob pairs rather than repeated copies for every commit.", "Excluded payloads were not scanned; no complete clearance is asserted when exclusions remain."],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("inventory", "scan"))
    args = parser.parse_args()
    scanner = setup_scanner()
    check_local_chain(OUT)
    inventory_path = OUT / "credential_scope_inventory_final.json"
    if args.mode == "inventory":
        result = inventory(scanner)
        with inventory_path.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2)
        print(json.dumps(inventory_summary(result), indent=2))
    else:
        scope = json.loads(inventory_path.read_text(encoding="utf-8"))
        result = scan(scope, scanner)
        report_path = OUT / "credential_audit_report.json"
        with report_path.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2)
        print(json.dumps({"report_path": str(report_path), "summary": result["summary"]}, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"error_type": type(exc).__name__, "message": "Audit stopped; exception details suppressed to preserve redaction."}))
        raise SystemExit(1)
