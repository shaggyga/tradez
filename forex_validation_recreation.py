#!/usr/bin/env python3
"""Recreate vault validation evidence without starting Forex account processes."""

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


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "trad" / "config" / "vault_validation_recreation.json"
DEFAULT_PROFILE = "model_gap_h24_recovery"
ALLOWED_RERUN_SCRIPTS = {
    "trad/oanda_model_gap_runtime_profiles.py",
    "trad/oanda_model_gap_qualification.py",
    "trad/oanda_neuralforecast_family_adapters.py",
}
ALLOWED_CHECK_SCRIPTS = {"trad/forex_runtime_lock.py"}


class ValidationRecreationError(RuntimeError):
    pass


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_json_path(value: Any, dotted: str) -> Any:
    current = value
    for part in dotted.split("."):
        if part == "$len":
            current = len(current)
        elif isinstance(current, list) and part.isdigit():
            current = current[int(part)]
        elif isinstance(current, dict) and part in current:
            current = current[part]
        else:
            raise ValidationRecreationError(f"JSON assertion path does not exist: {dotted}")
    return current


def verify_assertions(payload: dict[str, Any], assertions: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for dotted, expected in assertions.items():
        observed = resolve_json_path(payload, dotted)
        passed = observed == expected
        rows.append(
            {
                "path": dotted,
                "expected": expected,
                "observed": observed,
                "passed": passed,
            }
        )
        if not passed:
            raise ValidationRecreationError(
                f"assertion failed at {dotted}: expected {expected!r}, observed {observed!r}"
            )
    return rows


def _rooted(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise ValidationRecreationError(f"configured path escapes project root: {relative}") from exc
    return path


def verify_evidence(root: Path, profile: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in profile.get("evidence", []):
        relative = str(item["path"])
        path = _rooted(root, relative)
        if not path.is_file():
            raise ValidationRecreationError(f"required evidence is absent: {relative}")
        observed_hash = sha256_file(path)
        expected_hash = str(item.get("sha256", "")).lower()
        if expected_hash and observed_hash != expected_hash:
            raise ValidationRecreationError(
                f"evidence hash mismatch for {relative}: "
                f"expected {expected_hash}, observed {observed_hash}"
            )
        row: dict[str, Any] = {
            "path": relative,
            "bytes": path.stat().st_size,
            "sha256": observed_hash,
            "assertions": [],
            "status": "verified",
        }
        if path.suffix.lower() == ".json" and item.get("assertions"):
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            row["assertions"] = verify_assertions(payload, item["assertions"])
        results.append(row)
    return results


def _run_command(
    command: list[str],
    *,
    root: Path,
    timeout_seconds: int,
    label: str,
) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "trad")
    env["PYTHONUTF8"] = "1"
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    completed = subprocess.run(
        command,
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    record = {
        "label": label,
        "command": command,
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
    }
    if completed.returncode != 0:
        raise ValidationRecreationError(
            f"{label} failed with return code {completed.returncode}: "
            f"{completed.stderr[-1000:] or completed.stdout[-1000:]}"
        )
    return record


def run_compile(root: Path, profile: dict[str, Any]) -> dict[str, Any]:
    files = [str(_rooted(root, item)) for item in profile.get("compile_files", [])]
    if not files:
        return {"status": "skipped", "files": 0}
    if profile.get("compile_isolate_files"):
        records = [
            _run_command(
                [sys.executable, "-m", "py_compile", path],
                root=root,
                timeout_seconds=int(profile.get("compile_timeout_seconds", 120)),
                label=f"py_compile:{Path(path).name}",
            )
            for path in files
        ]
        return {
            "status": "passed",
            "files": len(files),
            "process_isolation": "one py_compile process per configured file",
            "subprocesses": records,
        }
    record = _run_command(
        [sys.executable, "-m", "py_compile", *files],
        root=root,
        timeout_seconds=int(profile.get("compile_timeout_seconds", 120)),
        label="py_compile",
    )
    return {**record, "status": "passed", "files": len(files)}


def run_pytest(root: Path, profile: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    files = [str(_rooted(root, item)) for item in profile.get("pytest_files", [])]
    if not files:
        return {"status": "skipped", "files": 0, "passed": 0}
    minimum = int(profile.get("pytest_minimum_passed", 1))
    if profile.get("pytest_isolate_files"):
        records: list[dict[str, Any]] = []
        passed = 0
        for index, path in enumerate(files, start=1):
            junit = output_dir / f"pytest_validation_recreation_{index:03d}.xml"
            record = _run_command(
                [sys.executable, "-m", "pytest", "-q", f"--junitxml={junit}", path],
                root=root,
                timeout_seconds=int(profile.get("pytest_timeout_seconds", 600)),
                label=f"pytest:{Path(path).name}",
            )
            match = re.search(r"(\d+) passed", record["stdout_tail"])
            file_passed = int(match.group(1)) if match else 0
            passed += file_passed
            records.append(
                {
                    **record,
                    "file": str(Path(path).relative_to(root)),
                    "passed": file_passed,
                    "junit": str(junit),
                }
            )
        if passed < minimum:
            raise ValidationRecreationError(
                f"pytest passed count {passed} is below configured minimum {minimum}"
            )
        return {
            "status": "passed",
            "files": len(files),
            "passed": passed,
            "minimum_passed": minimum,
            "process_isolation": "one pytest process per configured file",
            "subprocesses": records,
        }
    junit = output_dir / "pytest_validation_recreation.xml"
    record = _run_command(
        [sys.executable, "-m", "pytest", "-q", f"--junitxml={junit}", *files],
        root=root,
        timeout_seconds=int(profile.get("pytest_timeout_seconds", 600)),
        label="pytest",
    )
    match = re.search(r"(\d+) passed", record["stdout_tail"])
    passed = int(match.group(1)) if match else 0
    if passed < minimum:
        raise ValidationRecreationError(
            f"pytest passed count {passed} is below configured minimum {minimum}"
        )
    return {
        **record,
        "status": "passed",
        "files": len(files),
        "passed": passed,
        "minimum_passed": minimum,
        "junit": str(junit),
    }


def run_full_reruns(
    root: Path,
    profile: dict[str, Any],
    output_dir: Path,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    substitutions = {
        "root": str(root),
        "output_dir": str(output_dir),
        "fixture": str(
            _rooted(root, str(profile["bounded_fixture"]["path"]))
        ),
    }
    for item in profile.get("full_reruns", []):
        script_relative = str(item["script"]).replace("\\", "/")
        if script_relative not in ALLOWED_RERUN_SCRIPTS:
            raise ValidationRecreationError(
                f"full rerun script is not allowlisted: {script_relative}"
            )
        output = output_dir / str(item["output"])
        local = {**substitutions, "output": str(output)}
        args = [str(value).format(**local) for value in item.get("args", [])]
        record = _run_command(
            [sys.executable, str(_rooted(root, script_relative)), *args],
            root=root,
            timeout_seconds=int(item.get("timeout_seconds", 600)),
            label=str(item["name"]),
        )
        if not output.is_file():
            raise ValidationRecreationError(
                f"{item['name']} did not write its configured output: {output}"
            )
        payload = json.loads(output.read_text(encoding="utf-8-sig"))
        assertion_rows = verify_assertions(payload, item.get("assertions", {}))
        records.append(
            {
                **record,
                "status": "passed",
                "output": str(output),
                "output_sha256": sha256_file(output),
                "assertions": assertion_rows,
            }
        )
    return records


def run_checks(root: Path, profile: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    substitutions = {"root": str(root)}
    for item in profile.get("checks", []):
        script_relative = str(item["script"]).replace("\\", "/")
        if script_relative not in ALLOWED_CHECK_SCRIPTS:
            raise ValidationRecreationError(
                f"validation check script is not allowlisted: {script_relative}"
            )
        args = [
            str(value).format(**substitutions)
            for value in item.get("args", [])
        ]
        record = _run_command(
            [sys.executable, str(_rooted(root, script_relative)), *args],
            root=root,
            timeout_seconds=int(item.get("timeout_seconds", 300)),
            label=str(item["name"]),
        )
        records.append({**record, "status": "passed"})
    return records


def run_recreation(
    root: Path,
    config: dict[str, Any],
    profile_name: str,
    mode: str,
    output_dir: Path,
) -> dict[str, Any]:
    profiles = config.get("profiles", {})
    if profile_name not in profiles:
        raise ValidationRecreationError(f"unknown validation profile: {profile_name}")
    profile = profiles[profile_name]
    output_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "project_root": str(root.resolve()),
        "profile": profile_name,
        "mode": mode,
        "execution_policy": "offline_shadow_validation_no_account_or_network_activity",
        "account_processes_started": 0,
        "evidence": verify_evidence(root, profile),
        "checks": [],
        "compile": {"status": "not_requested"},
        "pytest": {"status": "not_requested"},
        "full_reruns": [],
        "status": "passed",
    }
    if mode in {"smoke", "full"}:
        report["checks"] = run_checks(root, profile)
        report["compile"] = run_compile(root, profile)
        report["pytest"] = run_pytest(root, profile, output_dir)
    if mode == "full":
        report["full_reruns"] = run_full_reruns(root, profile, output_dir)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--mode", choices=("evidence", "smoke", "full"), default="evidence")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--report", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    config_path = args.config.resolve()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir
        else root
        / "trad"
        / "data"
        / "oanda_training_manager"
        / "model_space"
        / "validation_recreation"
        / stamp
    )
    report_path = args.report.resolve() if args.report else output_dir / "report.json"
    try:
        config = json.loads(config_path.read_text(encoding="utf-8-sig"))
        report = run_recreation(root, config, args.profile, args.mode, output_dir)
    except (OSError, json.JSONDecodeError, ValidationRecreationError, subprocess.TimeoutExpired) as exc:
        failure = {
            "schema_version": 1,
            "generated_utc": utc_iso(),
            "profile": args.profile,
            "mode": args.mode,
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "account_processes_started": 0,
        }
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(failure, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(failure, indent=2), file=sys.stderr)
        return 2
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
