#!/usr/bin/env python3
"""Audit the compatibility-first Forex project reorientation.

This tool is read-only unless report paths are explicitly supplied. It does not
import trading managers, open broker connections, or mutate operational state.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping


UTC = dt.timezone.utc
DEFAULT_ROOT = Path(__file__).resolve().parent
DEFAULT_LAYOUT = DEFAULT_ROOT / "config" / "project_layout_v1.json"
SOURCE_SUFFIXES = {".py", ".ps1", ".html"}
OWNER_PRECEDENCE = (
    "tests",
    "execution",
    "governance",
    "evidence",
    "ingestion",
    "monitoring",
    "runtime",
    "contracts",
    "features",
    "research",
)


def load_layout(path: Path) -> dict[str, Any]:
    layout = json.loads(path.read_text(encoding="utf-8"))
    if layout.get("broker_environment") != "practice":
        raise ValueError("layout broker_environment must be practice")
    if layout.get("real_money_enabled") is not False:
        raise ValueError("layout real_money_enabled must be false")
    return layout


def root_source_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.iterdir()
        if path.is_file() and path.suffix.lower() in SOURCE_SUFFIXES
    )


def domain_matches(filename: str, domains: Iterable[Mapping[str, Any]]) -> list[str]:
    lowered = filename.lower()
    return [
        str(domain["name"])
        for domain in domains
        if any(str(token).lower() in lowered for token in domain.get("tokens", []))
    ]


def choose_owner(matches: Iterable[str]) -> str:
    values = set(matches)
    return next((name for name in OWNER_PRECEDENCE if name in values), "unowned")


def extract_supervisor_workers(path: Path) -> list[dict[str, str]]:
    text = path.read_text(encoding="utf-8-sig")
    workers: list[dict[str, str]] = []
    for segment in text.split("Start-ManagedProcess")[1:]:
        # A managed-process declaration ends before the next assignment block;
        # the first Name and Needle are its stable ownership identity.
        name = re.search(r'-Name\s+"([^"]+)"', segment)
        needle = re.search(r'-Needle\s+"([^"]+)"', segment)
        if name and needle:
            workers.append({"name": name.group(1), "needle": needle.group(1)})
    unique = {(row["name"], row["needle"]): row for row in workers}
    return [unique[key] for key in sorted(unique)]


def build_audit(root: Path, layout_path: Path) -> dict[str, Any]:
    layout = load_layout(layout_path)
    domains = list(layout.get("domains") or [])
    assignments = []
    for path in root_source_files(root):
        matches = domain_matches(path.name, domains)
        assignments.append(
            {
                "path": path.name,
                "owner": choose_owner(matches),
                "candidate_domains": matches,
                "ambiguous": len(matches) > 1,
            }
        )
    owner_counts = Counter(row["owner"] for row in assignments)
    supervisor = root / str(layout["current_runtime_entrypoint"])
    workers = extract_supervisor_workers(supervisor)
    critical_paths = {
        "layout": layout_path.exists(),
        "supervisor": supervisor.exists(),
        "credentials_ignored": "creds" in (root / ".gitignore").read_text(
            encoding="utf-8"
        ),
        "target_package": (root / "src" / "forex_system").exists(),
    }
    return {
        "schema_version": 1,
        "generated_utc": dt.datetime.now(UTC).isoformat(),
        "layout_id": layout.get("layout_id"),
        "migration_mode": layout.get("migration_mode"),
        "broker_environment": layout.get("broker_environment"),
        "real_money_enabled": layout.get("real_money_enabled"),
        "root_source_file_count": len(assignments),
        "owner_counts": dict(sorted(owner_counts.items())),
        "ambiguous_file_count": sum(row["ambiguous"] for row in assignments),
        "unowned_file_count": owner_counts.get("unowned", 0),
        "supervisor_worker_count": len(workers),
        "supervisor_workers": workers,
        "critical_paths": critical_paths,
        "status": "ok" if all(critical_paths.values()) else "blocked",
        "assignments": assignments,
        "policy": {
            "read_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
        },
    }


def markdown(audit: Mapping[str, Any]) -> str:
    lines = [
        "# Forex Structure Audit",
        "",
        f"Generated: `{audit['generated_utc']}`",
        "",
        f"- Status: **{audit['status']}**",
        f"- Root source files: **{audit['root_source_file_count']}**",
        f"- Supervised worker identities: **{audit['supervisor_worker_count']}**",
        f"- Ambiguous ownership: **{audit['ambiguous_file_count']}**",
        f"- Unowned files: **{audit['unowned_file_count']}**",
        f"- Broker environment: **{audit['broker_environment']}**",
        f"- Real-money enabled: **{audit['real_money_enabled']}**",
        "",
        "## Provisional Ownership",
        "",
        "| Domain | Root files |",
        "|---|---:|",
    ]
    lines.extend(
        f"| {name} | {count} |"
        for name, count in audit["owner_counts"].items()
    )
    lines.extend(
        [
            "",
            "## Migration Rule",
            "",
            "This inventory assigns ownership only. Live files remain at their old paths until",
            "a compatibility import, focused tests, supervisor parity, and a practice-only dry run pass.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--layout", type=Path, default=DEFAULT_LAYOUT)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    parser.add_argument("--stdout-json", action="store_true")
    args = parser.parse_args()
    audit = build_audit(args.root.resolve(), args.layout.resolve())
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    if args.markdown_output:
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(markdown(audit), encoding="utf-8")
    if args.stdout_json or not (args.json_output or args.markdown_output):
        print(json.dumps(audit, indent=2))
    return 0 if audit["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
