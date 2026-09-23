"""Index and verify a Forex vault without reading the live project or running it.

Generated outputs live beside (not inside) the canonical record manifest to
avoid self-referential hashes. Rebuild after record and source publication.
This is a document/link audit, not model proof or a complete backup certificate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import posixpath
import re
import unicodedata
import zipfile
from collections import Counter
from pathlib import Path
from urllib.parse import unquote


INDEX = "KNOWLEDGE_INDEX.md"
REPORT = "VAULT_READABILITY_REPORT.json"
GENERATED = {INDEX, REPORT}
ENTRYPOINTS = {"README.md", "AUDIT_START_HERE.md", "SYSTEM_GUIDE.md", "RECREATION.md", "RESEARCH_INDEX.md", "ACTIVE_PIPELINE.md"}
REQUIRED_NAVIGATION = {"README.md", "SYSTEM_GUIDE.md", "FEATURE_DICTIONARY_CURRENT.md", "RECREATION.md"}
LINK = re.compile(r'(?<!!)\[[^\]\n]*\]\((<[^>\n]+>|[^)\n]+)\)')
INLINE = re.compile(r'(?<!`)`([^`\n]+)`(?!`)')
EXTENSION = re.compile(r'\.(?:py|ps1|cmd|json|md|txt|toml|yaml|yml|html|csv|sqlite|sqlite3|db|zip|joblib|pkl)(?::\d+)?(?:#[^ ]*)?$', re.I)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encoded(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def safe_relative(name: str) -> str:
    name = name.replace("\\", "/")
    if not name or name.startswith("/") or ":" in name or ".." in name.split("/"):
        raise ValueError("unsafe vault member name")
    return name


def read_local(root: Path, name: str) -> bytes:
    name = safe_relative(name)
    target = root / name
    for item in (target, *target.parents):
        if item == root.parent:
            break
        if item.is_symlink() or (item.exists() and getattr(item.lstat(), "st_reparse_tag", 0) & 0x20000000):
            raise ValueError("redirected vault path")
    if not target.resolve().is_relative_to(root.resolve()):
        raise ValueError("vault path escape")
    return target.read_bytes()


def checked_root(root: Path) -> Path:
    root = root.absolute()
    for item in (root, *root.parents):
        if item.is_symlink() or getattr(item.lstat(), "st_reparse_tag", 0) & 0x20000000:
            raise ValueError("redirected vault root")
    if not root.is_dir():
        raise ValueError("vault root is not a directory")
    return root.resolve()


def checked_output(root: Path, name: str) -> Path:
    name = safe_relative(name)
    if "/" in name:
        raise ValueError("generated record must be a direct vault child")
    target = root / name
    try:
        metadata = target.lstat()  # also sees dangling symlinks and reparse points
    except FileNotFoundError:
        return target
    if target.is_symlink() or getattr(metadata, "st_reparse_tag", 0) & 0x20000000 or not target.is_file():
        raise ValueError("redirected or nonregular generated output")
    return target


def safe_inventory_path(root: Path, target: Path) -> bool:
    """Metadata only: never follow a supplemental path outside the vault."""
    try:
        for item in (target, *target.parents):
            if item == root.parent:
                break
            if item.is_symlink() or getattr(item.lstat(), "st_reparse_tag", 0) & 0x20000000:
                return False
        return target.resolve().is_relative_to(root)
    except OSError:
        return False


def safe_inventory_file(root: Path, target: Path) -> bool:
    return safe_inventory_path(root, target) and target.is_file()


def without_fences(body: str) -> str:
    lines, fence = [], None
    for line in body.splitlines():
        marker = re.match(r'^ {0,3}(`{3,}|~{3,})', line)
        if fence is None:
            if marker:
                fence = marker[1]
            else:
                lines.append(line)
        elif marker and marker[1][0] == fence[0] and len(marker[1]) >= len(fence):
            fence = None
    return "\n".join(lines)


def anchors(body: str) -> set[str]:
    found, counts = set(), Counter()
    for line in without_fences(body).splitlines():
        match = re.match(r'^#{1,6}\s+(.+?)\s*#*$', line)
        if match:
            value = unicodedata.normalize("NFC", match[1].lower())
            value = ''.join(c for c in value if c.isalnum() or c in " _-").replace(" ", "-")
            count = counts[value]
            counts[value] += 1
            found.add(value if not count else f"{value}-{count}")
    return found


def resolve_reference(raw: str, origin: str, sources: dict, archive: set[str], local: set[str]) -> dict:
    target = unquote(raw.strip().strip("<>"))
    # Optional Markdown title; quoted title is not part of the location.
    target = re.sub(r'\s+["\'][^"\']*["\']$', '', target)
    target = target.replace("\\", "/")
    if re.match(r'^(?:https?://|mailto:)', target, re.I):
        return {"classification": "external_url_not_fetched", "resolved": target}
    path, _, fragment = target.partition("#")
    path = re.sub(r':\d+$', '', path)
    if not path:
        return {"classification": "vault_file", "resolved": origin, "fragment": fragment}
    canonical = "c:/users/zmoor/documents/forex/trad/"
    if path.lower().startswith(canonical):
        path = path[len(canonical):]
        candidates = ["trad/" + path]
    elif re.match(r'^(?:[a-z]:/|/|file:|\\\\)', path, re.I):
        return {"classification": "machine_local_external_not_read", "resolved": path}
    else:
        direct = posixpath.normpath(posixpath.join(posixpath.dirname(origin), path))
        if direct in GENERATED:
            return {"classification": "generated_index", "resolved": direct, "fragment": fragment}
        if direct in local:
            return {"classification": "vault_file", "resolved": direct, "fragment": fragment}
        original = sources.get(origin, "trad/" + origin)
        candidates = [posixpath.normpath(posixpath.join(posixpath.dirname(original), path)),
                      posixpath.normpath(path), posixpath.normpath("trad/" + path)]
    inverse = {v: k for k, v in sources.items()}
    for candidate in candidates:
        if candidate in inverse:
            return {"classification": "vault_alias", "resolved": inverse[candidate], "fragment": fragment}
        member = candidate.removeprefix("trad/")
        if member in archive:
            return {"classification": "source_archive_member", "resolved": member, "fragment": fragment}
    return {"classification": "not_in_vault_or_current_source_archive", "resolved": path, "fragment": fragment}


def document_info(name: str, payload: bytes) -> dict:
    decoded = payload.decode("utf-8-sig")
    info = {"name": name, "bytes": len(payload), "sha256": digest(payload), "format": Path(name).suffix.lower()}
    if name.lower().endswith(".json"):
        obj = json.loads(decoded)
        info["json_top_level"] = type(obj).__name__
        if isinstance(obj, dict):
            info["top_level_keys"] = list(obj)[:30]
            for key in ("title", "purpose", "scope", "description", "schema_version"):
                if isinstance(obj.get(key), str):
                    info["description"] = obj[key][:220]
                    break
            info["declared_clocks"] = {k: obj[k] for k in ("as_of_utc", "generated_utc", "created_utc", "updated_utc", "timestamp_utc", "recorded_utc") if isinstance(obj.get(k), str)}
    elif name.lower().endswith(".md"):
        headers = re.findall(r'^#{1,3}\s+(.+)$', without_fences(decoded), re.M)
        info["description"] = headers[0] if headers else "Markdown record"
        info["headings"] = headers[:14]
    info.setdefault("description", "Retained record; inspect its own scope and timestamp.")
    info["authority"] = "current_navigation_not_live_telemetry" if name in ENTRYPOINTS else "dated_evidence_or_contract_not_live_status"
    return info


def audit(root: Path) -> dict:
    root = checked_root(root)
    manifest_bytes = read_local(root, "SHARED_PROJECT_STATE_CURRENT.json")
    manifest = json.loads(manifest_bytes)
    pointer_bytes = read_local(root, "source/WORKTREE_SOURCE_LATEST.json")
    pointer = json.loads(pointer_bytes)
    archive_manifest_bytes = read_local(root, "source/" + safe_relative(pointer["manifest"]))
    archive_manifest = json.loads(archive_manifest_bytes)
    if digest(archive_manifest_bytes) != pointer["manifest_sha256"]:
        raise ValueError("source pointer/manifest hash mismatch")
    if archive_manifest["archive"] != pointer["archive"] or archive_manifest["archive_sha256"] != pointer["archive_sha256"]:
        raise ValueError("source pointer/manifest archive mismatch")
    archive_bytes = read_local(root, "source/" + safe_relative(pointer["archive"]))
    if digest(archive_bytes) != pointer["archive_sha256"]:
        raise ValueError("source archive hash mismatch")
    archive_rows = archive_manifest["files"]
    archive_names = [safe_relative(row["path"]) for row in archive_rows]
    if len(set(n.casefold() for n in archive_names)) != len(archive_names):
        raise ValueError("duplicate source members")
    archive_paths = set(archive_names)
    archive_by_path = {row["path"]: row for row in archive_rows}
    archive_by_basename = {}
    for name in archive_names:
        archive_by_basename.setdefault(posixpath.basename(name), []).append(name)
    import io
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as z:
        if len(z.namelist()) != len(archive_names) or set(z.namelist()) != archive_paths:
            raise ValueError("archive inventory mismatch")
        for row in archive_rows:
            data = z.read(row["path"])
            if len(data) != row["size"] or digest(data) != row["sha256"]:
                raise ValueError("archive member hash mismatch")
    rows = manifest["records"]
    if len(rows) != manifest["record_count"] or len({row["name"].casefold() for row in rows}) != len(rows):
        raise ValueError("record inventory count/duplicates")
    bodies, records, sources = {}, [], {}
    for row in rows:
        name = safe_relative(row["name"])
        if name in GENERATED:
            raise ValueError("generated index/report must stay outside the canonical manifest")
        payload = read_local(root, name)
        if len(payload) != row["size"] or digest(payload) != row["sha256"]:
            raise ValueError(f"record hash mismatch: {name}")
        info = document_info(name, payload)
        info["canonical_source"] = row["source"]
        member = row["source"].replace("\\", "/").removeprefix("trad/")
        source_row = archive_by_path.get(member)
        info["source_archive_relationship"] = ("not_in_source_archive" if source_row is None else
                                               "same_hash" if source_row["sha256"] == row["sha256"] else "different_hash")
        sources[name] = row["source"].replace("\\", "/")
        records.append(info)
        if name.lower().endswith(".md"):
            bodies[name] = payload.decode("utf-8-sig")
    # Local existence means vault-only; no linked external path is opened.
    local = set(sources) | {"SHARED_PROJECT_STATE_CURRENT.json", "source/WORKTREE_SOURCE_LATEST.json"}
    for directory_name in ("source", "maintenance"):
        directory = root / directory_name
        if safe_inventory_path(root, directory) and directory.is_dir():
            local |= {p.relative_to(root).as_posix() for p in directory.glob("*") if safe_inventory_file(root, p)}
    references = []
    for name, body in bodies.items():
        cleaned = without_fences(body)
        explicit = [(m.group(1), "markdown_link") for m in LINK.finditer(cleaned)]
        literals = [(m.group(1), "inline_path_reference") for m in INLINE.finditer(cleaned)
                    if EXTENSION.search(m.group(1)) and not any(c in m.group(1) for c in "\n{}=;")]
        seen = set()
        for target, kind in explicit + literals:
            if (target, kind) in seen:
                continue
            seen.add((target, kind))
            resolved = resolve_reference(target, name, sources, archive_paths, local)
            item = {"origin": name, "target": target, "kind": kind, **resolved}
            if resolved["classification"] in {"machine_local_external_not_read", "not_in_vault_or_current_source_archive"}:
                basename = posixpath.basename(resolved["resolved"])
                matches = archive_by_basename.get(basename, [])
                if matches:
                    item["same_name_archived_candidates_not_identity_verified"] = sorted(matches)
                locations = sorted(p for p in local if posixpath.basename(p) == basename)
                if locations:
                    item["same_name_vault_locations_not_identity_verified"] = locations
            if resolved.get("fragment") and resolved["resolved"] in bodies:
                item["fragment_found"] = resolved["fragment"] in anchors(bodies[resolved["resolved"]])
            references.append(item)
    navigation_errors = [r for r in references if r["origin"] in ENTRYPOINTS and r["kind"] == "markdown_link"
                         and (r["classification"] not in {"vault_file", "generated_index", "external_url_not_fetched"} or r.get("fragment_found") is False)]
    for missing in sorted(REQUIRED_NAVIGATION - set(sources)):
        navigation_errors.append({"origin": INDEX, "target": missing, "kind": "required_navigation_document",
                                  "classification": "missing_required_navigation_document"})
    source_mismatches = [r["name"] for r in records if r["source_archive_relationship"] == "different_hash"]
    status = ("navigation_repairs_required" if navigation_errors else "source_export_outdated" if source_mismatches else
              "pass_with_declared_external_dependencies")
    return {
        "schema_version": "forex_vault_readability_v1",
        "scope": "Vault-only UTF-8/JSON, record hashes, archive member hashes, navigation and reference inventory. No external paths read, network requests, source execution, or runtime inference.",
        "canonical_manifest_sha256": digest(manifest_bytes), "canonical_records_as_of": manifest.get("generated_utc"),
        "source_pointer_sha256": digest(pointer_bytes), "source_snapshot_id": pointer.get("snapshot_id"),
        "source_manifest_sha256": digest(archive_manifest_bytes), "source_archive_sha256": digest(archive_bytes),
        "source_archive": "source/" + pointer["archive"], "source_manifest": "source/" + pointer["manifest"],
        "record_count": len(records), "source_members_verified": len(archive_paths),
        "record_format_counts": dict(Counter(r["format"] for r in records)),
        "record_source_archive_relationship_counts": dict(Counter(r["source_archive_relationship"] for r in records)),
        "source_export_mismatches": source_mismatches,
        "reference_classification_counts": dict(Counter(r["classification"] for r in references)),
        "navigation_errors": navigation_errors,
        "status": status,
        "limits": ["CURRENT is an export alias, not a promise of a current observation or an active worker.",
                   "Source and formulas do not recreate excluded credentials, fitted weights, runtime ledgers or original first-seen clocks.",
                   "Historical documents remain verbatim; unresolved or source-layout links are indexed rather than rewritten as proof.",
                   "A literal path absent from both inventories is unresolved, not proof that its data does not exist elsewhere.",
                   "Same-name archived candidates are lookup suggestions only; identity with a referenced external artifact has not been verified.",
                   "Lightweight Markdown scan covers inline links and path-shaped code spans, not every prose citation or all embedded file formats.",
                   "No independent economic/model-performance claim is inferred from document availability."],
        "records": sorted(records, key=lambda r: r["name"]), "references": references,
    }


def cell(value: object) -> str:
    return str(value).replace("|", "&#124;").replace("\n", " ")


def render(report: dict) -> str:
    lines = ["# Forex vault knowledge index", "",
             "Start with [README](README.md), [system guide](SYSTEM_GUIDE.md),",
             "[feature dictionary](FEATURE_DICTIONARY_CURRENT.md) and [recreation](RECREATION.md).", "",
             "This index is generated from the published vault and source archive only. It does not",
             "open the live project. Every canonical record below decoded successfully and matched",
             "its published hash; JSON records parsed. The complete reference-resolution list is in",
             "[VAULT_READABILITY_REPORT.json](VAULT_READABILITY_REPORT.json).", "",
             "## What this proves", "",
             f"- Canonical records: {report['record_count']}; archived source members verified: {report['source_members_verified']}.",
             f"- Manifest observation: `{report['canonical_records_as_of']}`; source snapshot: `{report['source_snapshot_id']}`.",
             f"- Navigation result: `{report['status']}`.",
             "- Source/record correspondence: " + cell(report["record_source_archive_relationship_counts"]) + ".",
             "- It proves local readability, provenance and navigation—not a complete historical-data backup or profitable model.", "",
             "## Authority and missing information", ""]
    lines.extend("- " + value for value in report["limits"])
    lines.extend(["", "## All canonical records", "",
                  "Descriptions are extracted titles/schema labels, not newly inferred findings. A",
                  "dated report remains dated even when its filename contains CURRENT. Read its",
                  "scope, timestamp and limitation fields before citing its numbers.", "",
                  "| Record | Description | Canonical source / archive lookup |", "|---|---|---|"])
    for record in report["records"]:
        lines.append(f"| [{record['name']}]({record['name']}) | {cell(record['description'])} | `{record['canonical_source']}` |")
    lines.extend(["", "## Resolving references from older records", "",
                  "Many preserved reports use the original source-tree layout. Those links may not",
                  "open directly in a flat vault. Use the map below or search the JSON report by",
                  "`origin` and `target`. Extract source into a new inspection directory only; paths",
                  "inside its ZIP are relative to `trad`, without an extra `trad/` prefix.", "",
                  "| Original reference | Available here / disposition |", "|---|---|"])
    unique = {}
    for ref in report["references"]:
        if ref["classification"] in {"vault_file", "generated_index", "external_url_not_fetched"}:
            continue
        unique[(ref["target"], ref["classification"], ref["resolved"])] = ref
    for _, ref in sorted(unique.items()):
        kind, resolved = ref["classification"], ref["resolved"]
        if kind == "vault_alias":
            destination = f"[{resolved}]({resolved})"
        elif kind == "source_archive_member":
            destination = f"Source ZIP member `{cell(resolved)}`"
        else:
            destination = f"`{kind}` — `{cell(resolved)}`; not read or assumed available."
            suggestions = ref.get("same_name_vault_locations_not_identity_verified", [])
            if suggestions:
                destination += " Same-name local lookup (identity unverified): " + ", ".join(f"[{p}]({p})" for p in suggestions)
            candidates = ref.get("same_name_archived_candidates_not_identity_verified", [])
            if candidates:
                destination += " Same-name source lookup (identity unverified): " + ", ".join(f"`{cell(p)}`" for p in candidates)
        lines.append(f"| `{cell(ref['target'])}` | {destination} |")
    lines.extend(["", "## Refresh this index", "",
                  "After publishing canonical records and the latest worktree source archive, run",
                  "`python -B tools/audit_forex_vault_readability.py --build` from the inspected source.",
                  "Use `--check` to detect changed records, changed source pointers or stale generated",
                  "index bytes. This does not fetch sources or start the project.", ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault-project", type=Path, default=Path.home() / "OneDrive/thevault/projects/forex")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--build", action="store_true")
    mode.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = checked_root(args.vault_project)
    report = audit(root)
    outputs = {REPORT: encoded(report), INDEX: render(report).encode("utf-8")}
    targets = {name: checked_output(root, name) for name in outputs}
    for name, payload in outputs.items():
        if args.build:
            targets[name].write_bytes(payload)
        elif read_local(root, name) != payload:
            raise ValueError(f"generated vault index is stale: {name}")
    print(json.dumps({"mode": "built" if args.build else "checked", "status": report["status"],
                      "records": report["record_count"], "source_members": report["source_members_verified"],
                      "navigation_errors": len(report["navigation_errors"]),
                      "reference_counts": report["reference_classification_counts"]}))
    return 1 if report["status"] != "pass_with_declared_external_dependencies" else 0


if __name__ == "__main__":
    raise SystemExit(main())
