#!/usr/bin/env python3
"""Fail-closed validation for the compatibility-first module migration."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
LAYOUT = ROOT / "config" / "project_layout_v1.json"
REGISTRY = ROOT / "config" / "module_migration_registry_v1.json"
ALLOWED_STATES = {"inventory_only", "live_root_entrypoint", "extracted", "shim_verified", "retired"}


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate(root: Path = ROOT, layout_path: Path = LAYOUT, registry_path: Path = REGISTRY) -> dict[str, Any]:
    layout = load_json(layout_path)
    registry = load_json(registry_path)
    domains = {item["name"] for item in layout["domains"]}
    failures: list[str] = []
    if layout.get("broker_environment") != "practice":
        failures.append("broker_environment_not_practice")
    if layout.get("real_money_enabled") is not False:
        failures.append("real_money_enabled")
    seen: set[str] = set()
    for entry in registry.get("entries", []):
        module_id = str(entry.get("module_id") or "")
        if not module_id or module_id in seen:
            failures.append(f"duplicate_or_missing_module_id:{module_id}")
        seen.add(module_id)
        state = str(entry.get("state") or "")
        if state not in ALLOWED_STATES:
            failures.append(f"invalid_state:{module_id}:{state}")
        domain = str(entry.get("domain") or "")
        if domain not in domains:
            failures.append(f"unknown_domain:{module_id}:{domain}")
        current = root / str(entry.get("current_path") or "")
        target = root / str(entry.get("target_path") or "")
        if not current.is_file():
            failures.append(f"missing_current_path:{module_id}")
        if state in {"extracted", "shim_verified", "retired"} and not target.is_file():
            failures.append(f"missing_target_path:{module_id}")
        if bool(entry.get("live")) and state == "retired":
            failures.append(f"live_module_cannot_be_retired:{module_id}")
        forbidden = {"creds", "data", "artifacts", "tmp"}
        if current.parts and current.parts[0].lower() in forbidden:
            failures.append(f"forbidden_source_tree:{module_id}")
    return {
        "schema_version": 1,
        "registry_id": registry.get("registry_id"),
        "entry_count": len(registry.get("entries", [])),
        "live_root_entrypoint_count": sum(
            entry.get("state") == "live_root_entrypoint"
            for entry in registry.get("entries", [])
        ),
        "failures": failures,
        "status": "ok" if not failures else "blocked",
        "policy": {
            "read_only": True,
            "can_move_files": False,
            "can_restart_workers": False,
            "can_place_orders": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--layout", type=Path, default=LAYOUT)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    args = parser.parse_args()
    result = validate(args.root.resolve(), args.layout.resolve(), args.registry.resolve())
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
