#!/usr/bin/env python3
"""Create or verify exact package inventories for vault validation runtimes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "model_gap_runtime_profiles.json"
DEFAULT_LOCK = ROOT / "config" / "vault_runtime_lock.json"

INVENTORY_CODE = r'''
import importlib.metadata
import json
import platform
import sys

rows = []
for dist in importlib.metadata.distributions():
    name = dist.metadata.get("Name") or ""
    if name:
        rows.append({"name": name, "version": dist.version})
rows.sort(key=lambda row: row["name"].lower())
print(json.dumps({
    "python": platform.python_version(),
    "implementation": platform.python_implementation(),
    "platform": platform.platform(),
    "executable": sys.executable,
    "packages": rows,
}))
'''


class RuntimeLockError(RuntimeError):
    pass


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def expand_path(value: str) -> Path:
    runtime_root = os.environ.get(
        "FOREX_MODEL_RUNTIME_ROOT",
        str(ROOT.parent / "runtime" / "model_gap"),
    )
    value = value.replace("%FOREX_MODEL_RUNTIME_ROOT%", runtime_root)
    expanded = os.path.expandvars(value)
    if "%" in expanded:
        for key, item in os.environ.items():
            expanded = expanded.replace(f"%{key}%", item)
    return Path(expanded)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run_inventory(python: Path) -> dict[str, Any]:
    if not python.is_file():
        raise RuntimeLockError(f"runtime Python is absent: {python}")
    completed = subprocess.run(
        [str(python), "-c", INVENTORY_CODE],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeLockError(
            f"package inventory failed for {python}: {completed.stderr.strip()}"
        )
    inventory = json.loads(completed.stdout)
    check = subprocess.run(
        [str(python), "-m", "pip", "check"],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    inventory["pip_check"] = {
        "returncode": check.returncode,
        "output": (check.stdout + check.stderr).strip(),
        "status": "passed" if check.returncode == 0 else "failed",
    }
    inventory["package_count"] = len(inventory["packages"])
    return inventory


def build_lock(config_path: Path = DEFAULT_CONFIG) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    profiles: dict[str, Any] = {}
    for name, value in config["profiles"].items():
        python = expand_path(str(value["python"])).resolve()
        inventory = _run_inventory(python)
        profiles[name] = {
            **inventory,
            "python_template": value["python"],
            "bootstrap_requirements": list(value.get("bootstrap_requirements", [])),
            "account_wired": False,
        }
    source_pins: dict[str, Any] = {}
    for name, value in config.get("official_source_pins", {}).items():
        path = expand_path(str(value["path"])).resolve()
        commit_file = path / "PINNED_COMMIT.txt"
        observed = commit_file.read_text(encoding="utf-8").strip() if commit_file.is_file() else ""
        source_pins[name] = {
            "path_template": value["path"],
            "repository": value["repository"],
            "expected_commit": value["commit"],
            "observed_commit": observed,
            "pin_file_sha256": sha256_file(commit_file) if commit_file.is_file() else "",
            "status": "verified" if observed == value["commit"] else "mismatch",
        }
    failed_profiles = sum(
        value["pip_check"]["status"] != "passed" for value in profiles.values()
    )
    failed_pins = sum(value["status"] != "verified" for value in source_pins.values())
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "config": str(config_path.resolve()),
        "purpose": "exact environment inventory plus minimal bootstrap requirements",
        "execution_policy": "dependency_inventory_only_no_install_no_network_no_accounts",
        "profiles": profiles,
        "official_source_pins": source_pins,
        "summary": {
            "profiles": len(profiles),
            "pip_check_passed": len(profiles) - failed_profiles,
            "source_pins": len(source_pins),
            "source_pins_verified": len(source_pins) - failed_pins,
            "failed": failed_profiles + failed_pins,
            "account_wired": 0,
        },
    }


def verify_lock(lock: dict[str, Any], config_path: Path = DEFAULT_CONFIG) -> dict[str, Any]:
    observed = build_lock(config_path)
    mismatches: list[str] = []
    for name, expected_profile in lock.get("profiles", {}).items():
        actual_profile = observed.get("profiles", {}).get(name)
        if actual_profile is None:
            mismatches.append(f"missing profile: {name}")
            continue
        expected_packages = {
            canonical_name(row["name"]): str(row["version"])
            for row in expected_profile.get("packages", [])
        }
        actual_packages = {
            canonical_name(row["name"]): str(row["version"])
            for row in actual_profile.get("packages", [])
        }
        if expected_packages != actual_packages:
            missing = sorted(set(expected_packages) - set(actual_packages))
            extra = sorted(set(actual_packages) - set(expected_packages))
            changed = sorted(
                package
                for package in set(expected_packages) & set(actual_packages)
                if expected_packages[package] != actual_packages[package]
            )
            mismatches.append(
                f"{name}: missing={missing[:5]} extra={extra[:5]} changed={changed[:5]}"
            )
    for name, expected_pin in lock.get("official_source_pins", {}).items():
        actual_pin = observed.get("official_source_pins", {}).get(name, {})
        if actual_pin.get("observed_commit") != expected_pin.get("expected_commit"):
            mismatches.append(f"source pin mismatch: {name}")
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "status": "matched" if not mismatches else "mismatch",
        "mismatches": mismatches,
        "profiles_checked": len(lock.get("profiles", {})),
        "source_pins_checked": len(lock.get("official_source_pins", {})),
        "account_wired": False,
    }


def emit_requirements(lock: dict[str, Any], destination: Path, exact: bool) -> list[str]:
    destination.mkdir(parents=True, exist_ok=True)
    outputs: list[str] = []
    for name, profile in lock.get("profiles", {}).items():
        if exact:
            lines = sorted(
                f"{row['name']}=={row['version']}" for row in profile.get("packages", [])
            )
            suffix = "exact"
        else:
            lines = list(profile.get("bootstrap_requirements", []))
            suffix = "bootstrap"
        path = destination / f"requirements-{name}-{suffix}.txt"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        outputs.append(str(path))
    return outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--emit-requirements-dir", type=Path)
    parser.add_argument("--exact-requirements", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.verify:
            lock = json.loads(args.output.read_text(encoding="utf-8-sig"))
            report = verify_lock(lock, args.config)
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0 if report["status"] == "matched" else 2
        lock = build_lock(args.config)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(lock, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        outputs = (
            emit_requirements(lock, args.emit_requirements_dir, args.exact_requirements)
            if args.emit_requirements_dir
            else []
        )
        print(
            json.dumps(
                {"output": str(args.output), "summary": lock["summary"], "requirements": outputs},
                indent=2,
                sort_keys=True,
            )
        )
        return 0 if lock["summary"]["failed"] == 0 else 2
    except (OSError, json.JSONDecodeError, RuntimeLockError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
