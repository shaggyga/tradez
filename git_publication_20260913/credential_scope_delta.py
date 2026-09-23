"""Reconcile the audited Git scope after intentional source/ignore changes."""
from __future__ import annotations

import collections
import hashlib
import importlib.util
import json
from pathlib import Path
import stat
import sys
from datetime import datetime, timezone

sys.dont_write_bytecode = True
OUT = Path(r"C:\Users\zmoor\Documents\forex\git_publication_20260913")
spec = importlib.util.spec_from_file_location("scope_wrapper", OUT / "credential_scope_audit.py")
wrapper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wrapper)


def main() -> int:
    scanner = wrapper.setup_scanner()
    prior = json.loads((OUT / "credential_audit_report.json").read_text(encoding="utf-8"))
    inventory = json.loads((OUT / "credential_scope_inventory_final.json").read_text(encoding="utf-8"))
    if wrapper.git("rev-parse", "HEAD").decode("ascii").strip() != prior["audited_head"]:
        raise ValueError("history_changed")
    if wrapper.git("rev-list", "--all").decode("ascii").splitlines() != inventory["revisions"]:
        raise ValueError("reachable_history_changed")
    if hashlib.sha256(wrapper.SCANNER.read_bytes()).hexdigest() != prior["scanner_sha256"]:
        raise ValueError("scanner_changed")
    prior_metadata = {item["path"]: item for item in inventory["candidate"]}
    prior_digests = {item["path"]: item for item in prior["candidate_content_digests"]}
    prior_findings = collections.defaultdict(list)
    prior_accounts = collections.defaultdict(list)
    for finding in prior["findings"]:
        if finding["scope"] == "candidate":
            prior_findings[finding["path"]].append(finding)
    for finding in prior["private_account_metadata"]:
        if finding["scope"] == "candidate":
            prior_accounts[finding["path"]].append(finding)
    paths = sorted(set(part.decode("utf-8") for part in wrapper.git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0") if part))
    findings, accounts, exclusions, digests, scanned_paths, reused_paths = [], [], [], [], [], []
    fresh_metadata = {}
    delta_bytes = 0
    for path in paths:
        reason = wrapper.path_exclusion(path, scanner)
        if reason:
            exclusions.append({"path": path, "rule": reason})
            continue
        try:
            metadata = wrapper.check_local_chain(wrapper.ROOT / path)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("non_regular_file")
        except (OSError, ValueError):
            exclusions.append({"path": path, "rule": "unsafe_or_missing_current_path"})
            continue
        fresh_metadata[path] = (metadata.st_size, metadata.st_mtime_ns)
        old = prior_metadata.get(path)
        if path in prior_digests and old and old.get("size") == metadata.st_size and old.get("mtime_ns") == metadata.st_mtime_ns:
            reused_paths.append(path)
            digests.append(prior_digests[path])
            findings.extend(prior_findings[path])
            accounts.extend(prior_accounts[path])
            continue
        if metadata.st_size > wrapper.MAX_FILE or prior["summary"]["physical_payload_bytes_read"] + delta_bytes + metadata.st_size > wrapper.MAX_TOTAL:
            exclusions.append({"path": path, "rule": "delta_size_limit"})
            continue
        with (wrapper.ROOT / path).open("rb") as stream:
            payload = stream.read(wrapper.MAX_FILE + 1)
        delta_bytes += len(payload)
        if len(payload) != metadata.st_size:
            exclusions.append({"path": path, "rule": "changed_during_delta_read"})
            continue
        result = wrapper.examine_payload(scanner, path, payload)
        if result.get("excluded_reason"):
            exclusions.append({"path": path, "rule": result["excluded_reason"]})
            continue
        scanned_paths.append(path)
        digests.append({"path": path, "size": len(payload), "sha256": hashlib.sha256(payload).hexdigest()})
        findings.extend({"scope": "candidate", "path": path, **item} for item in result["findings"])
        accounts.extend({"scope": "candidate", "path": path, **item} for item in result["account_ids"])
    for path, expected in fresh_metadata.items():
        current = wrapper.check_local_chain(wrapper.ROOT / path)
        if (current.st_size, current.st_mtime_ns) != expected:
            exclusions.append({"path": path, "rule": "changed_during_delta_scan"})
    current_set = set(paths)
    prior_exclusions_removed = [item for item in prior["exclusions"] if item["scope"] == "candidate" and item["path"] not in current_set]
    remaining_history_findings = [item for item in prior["findings"] if item["scope"] == "history"]
    result = {
        "schema_version": "redacted_credential_audit_final_scope_v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "audited_head": prior["audited_head"],
        "scanner_sha256": prior["scanner_sha256"],
        "base_report": "credential_audit_report.json",
        "summary": {
            "current_candidate_paths": len(paths),
            "reused_unchanged_source_scans": len(reused_paths),
            "delta_source_scans": len(scanned_paths),
            "candidate_finding_count": len(findings),
            "history_finding_count": len(remaining_history_findings),
            "history_unique_path_blob_count": prior["summary"]["scanned_entries"]["history"],
            "reachable_commits": prior["reachable_commits"],
            "current_exclusions": len(exclusions),
            "initial_exclusions_removed_from_git_scope": len(prior_exclusions_removed),
            "delta_bytes_read": delta_bytes,
            "combined_payload_bytes_read": prior["summary"]["physical_payload_bytes_read"] + delta_bytes,
            "candidate_account_id_occurrences": len(accounts),
            "candidate_account_id_files": len({item["path"] for item in accounts}),
            "candidate_account_id_unique_fingerprints": len({item["value_sha256_prefix"] for item in accounts}),
            "candidate_complete": not exclusions,
            "history_complete": prior["summary"]["history_complete"],
            "passed_scanned_text": not findings and not remaining_history_findings,
            "complete_and_passed": not exclusions and prior["summary"]["history_complete"] and not findings and not remaining_history_findings,
        },
        "delta_scanned_paths": scanned_paths,
        "findings": findings,
        "private_account_metadata": accounts,
        "exclusions": exclusions,
        "initial_exclusions_now_outside_git_scope": prior_exclusions_removed,
        "approved_git_paths": sorted(item["path"] for item in digests),
        "candidate_content_digests": sorted(digests, key=lambda item: item["path"]),
        "limitations": ["Pattern-based scan does not establish absence of every secret.", "Private account identifiers remain in source/evidence; repository must remain private.", "Unchanged source scan results were reused after matching pathname, size and modification time to the original audited inventory.", "Ignored browser profiles, data payloads and PNGs were never read by this audit."],
    }
    report_path = OUT / "credential_audit_final_scope.json"
    with report_path.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps({"report_path": str(report_path), "summary": result["summary"], "delta_scanned_paths": scanned_paths}, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"error_type": type(exc).__name__, "message": "Delta audit stopped; exception details suppressed."}))
        raise SystemExit(1)
