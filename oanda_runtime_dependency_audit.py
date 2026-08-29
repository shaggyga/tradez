#!/usr/bin/env python3
"""Build a read-only keep/merge/repair/park map for supervised workers."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_SUPERVISOR = ROOT / "oanda_always_on_supervisor.ps1"
DEFAULT_OUTPUT = DATA / "state" / "runtime_dependency_audit_v1.json"
DEFAULT_REPORT = DATA / "reports" / "runtime_dependency_audit" / "RUNTIME_DEPENDENCY_AUDIT_CURRENT.md"


def read_json_lines(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def latest_supervisor_heartbeat(log_root: Path) -> dict[str, Any]:
    try:
        paths = sorted(
            log_root.glob("always_on_supervisor_*.jsonl"),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return {}
    for path in paths:
        rows = read_json_lines(path)
        for row in reversed(rows):
            if row.get("event") == "heartbeat" and isinstance(row.get("managed"), list):
                return {**row, "log_path": str(path.resolve())}
    return {}


def parse_supervisor_workers(text: str) -> dict[str, dict[str, Any]]:
    starts = list(re.finditer(r'\$managed\s*\+=\s*Start-ManagedProcess\s*`', text))
    workers: dict[str, dict[str, Any]] = {}
    for index, match in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(text)
        block = text[match.start() : end]
        name_match = re.search(r'-Name\s+"([^"]+)"', block)
        if not name_match:
            continue
        name = name_match.group(1)
        script_match = re.search(r'Join-Path\s+\$Trad\s+"([^"]+\.py)"', block)
        artifacts = sorted(
            set(
                re.findall(
                    r'"([^"\r\n]+\.(?:json|jsonl|sqlite|md|csv|parquet))"',
                    block,
                    flags=re.IGNORECASE,
                )
            )
        )
        workers[name] = {
            "name": name,
            "script": script_match.group(1) if script_match else "",
            "declared_artifacts": artifacts,
        }
    return workers


def classify_worker(name: str, runtime: dict[str, Any]) -> tuple[str, str]:
    freshness = runtime.get("freshness") or {}
    reason = str(freshness.get("reason") or "")
    if name == "practice_002_no_gpt_movement_ledger":
        return "retire_candidate", "prove no live/evidence consumer, then park"
    if name in {"major_move_gap_census", "shadow_archive_integrity", "verified_log_archiver"}:
        return "periodic_batch", "keep periodic; do not run continuously"
    if name in {
        "hgb_live_outcomes",
        "canonical_outcome_worker",
        "proof_shadow_predictors",
        "edge_evidence_worker",
        "evidence_operations_worker",
        "research_genealogy",
        "independent_evidence_verifier",
        "lane_promotion_fit",
        "strategy_exit_fit",
        "timeframe_matrix_calibration",
        "top_signal_position_ledger",
        "manager_decision_outcome_ledger",
        "canonical_signal_trials",
    }:
        return "frozen_outcome_maturation", "keep until retained forecasts/evidence obligations mature"
    if reason not in {"", "fresh", "not_checked", "disabled", "safe_core_only", "startup_stagger", "missing_credential", "startup_grace"}:
        return "broken_needs_repair", f"repair freshness failure: {reason}"
    if reason in {"disabled", "safe_core_only"}:
        return "legacy_reference", "leave disabled; review before archive"
    core = {
        "account_snapshot",
        "account_snapshot_007",
        "live_dashboard",
        "local_news_sentiment",
        "macro_surprise_ledger",
        "signal_feed_wal_maintenance",
        "practice_007_quote_stream",
        "clock_integrity_monitor",
        "source_governance",
        "project_integrity_audit",
        "storage_headroom_guard",
        "practice_007_quote_transport_crosscheck",
        "practice_007_signal_feed_availability",
        "practice_007_reentry_shadow",
        "practice_007_fast_executor",
        "strategy_lab",
    }
    if name in core:
        return "core_live", "keep"
    periodic_tokens = ("fit", "compactor", "maintenance", "audit", "census")
    if any(token in name for token in periodic_tokens):
        return "periodic_batch", "measure evidence velocity and reduce cadence if safe"
    return "prospective_collection", "keep research-only pending governed evidence"


def process_costs(pids: list[int]) -> dict[str, Any]:
    try:
        import psutil  # type: ignore
    except ImportError:
        return {"pid_count": len(pids), "rss_bytes": None, "cpu_sec": None}
    rss = 0
    cpu = 0.0
    observed = 0
    for pid in set(int(value) for value in pids):
        try:
            process = psutil.Process(pid)
            rss += int(process.memory_info().rss)
            times = process.cpu_times()
            cpu += float(times.user + times.system)
            observed += 1
        except (psutil.Error, OSError):
            continue
    return {"pid_count": observed, "rss_bytes": rss, "cpu_sec": round(cpu, 3)}


def source_consumers(root: Path, artifact_names: set[str]) -> dict[str, list[str]]:
    consumers = {name: [] for name in artifact_names}
    candidates = [
        *root.glob("*.py"),
        *root.glob("*.ps1"),
        *root.glob("*.html"),
        *root.glob("config/*.json"),
    ]
    for path in candidates:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for name in artifact_names:
            if name and name in text:
                consumers[name].append(path.name)
    return {name: sorted(set(rows)) for name, rows in consumers.items()}


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(path)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True))


def build(supervisor: Path = DEFAULT_SUPERVISOR) -> dict[str, Any]:
    text = supervisor.read_text(encoding="utf-8")
    declarations = parse_supervisor_workers(text)
    heartbeat = latest_supervisor_heartbeat(DATA / "logs")
    live = {
        str(row.get("name") or ""): row
        for row in heartbeat.get("managed") or []
        if isinstance(row, dict) and row.get("name")
    }
    artifact_names = {
        Path(value).name
        for row in declarations.values()
        for value in row["declared_artifacts"]
        if Path(value).suffix
    }
    consumers = source_consumers(ROOT, artifact_names)
    rows: list[dict[str, Any]] = []
    for name in sorted(set(declarations) | set(live)):
        declared = declarations.get(name) or {"script": "", "declared_artifacts": []}
        runtime = live.get(name) or {}
        category, action = classify_worker(name, runtime)
        freshness = runtime.get("freshness") or {}
        artifacts = declared["declared_artifacts"]
        artifact_consumers = {
            Path(item).name: [
                candidate
                for candidate in consumers.get(Path(item).name, [])
                if candidate not in {declared.get("script"), supervisor.name}
            ]
            for item in artifacts
        }
        rows.append(
            {
                "name": name,
                "script": declared.get("script") or "",
                "category": category,
                "proposed_action": action,
                "running": bool(runtime.get("running")),
                "pids": runtime.get("pids") or [],
                "resource_cost": process_costs(runtime.get("pids") or []),
                "freshness": freshness,
                "declared_artifacts": artifacts,
                "downstream_readers_by_artifact": artifact_consumers,
                "evidence_status": (
                    "immutable_or_maturation_dependency"
                    if category == "frozen_outcome_maturation"
                    else "prospective_research_only"
                    if category == "prospective_collection"
                    else "operational_or_not_applicable"
                ),
                "rollback": "restore prior supervisor configuration and restart hidden supervisor",
                "verification": [
                    "focused worker tests",
                    "supervisor parse",
                    "freshness heartbeat",
                    "downstream state/report integrity",
                    "Practice-007 remains fail-closed",
                ],
            }
        )
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["category"]] = counts.get(row["category"], 0) + 1
    return {
        "schema_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "read_only": True,
        "changes_authorized": False,
        "supervisor": str(supervisor.resolve()),
        "supervisor_heartbeat_log": heartbeat.get("log_path"),
        "worker_count": len(rows),
        "category_counts": counts,
        "workers": rows,
    }


def render(payload: dict[str, Any]) -> str:
    lines = [
        "# Runtime dependency audit",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "This is a read-only pre-change map. It authorizes no process stop, deletion, evidence merge, or cohort relabeling.",
        "",
        f"Workers: **{payload['worker_count']}**; categories: `{json.dumps(payload['category_counts'], sort_keys=True)}`.",
        "",
        "| Worker | Class | Running | RSS MiB | Output/readers | Proposed action |",
        "|---|---|---:|---:|---|---|",
    ]
    for row in payload["workers"]:
        rss = row["resource_cost"].get("rss_bytes")
        rss_mib = "n/a" if rss is None else f"{rss / 1024**2:.1f}"
        mappings = []
        for artifact, readers in row["downstream_readers_by_artifact"].items():
            mappings.append(f"{artifact}: {', '.join(readers[:4]) or 'no static reader found'}")
        lines.append(
            "| {name} | {category} | {running} | {rss} | {mapping} | {action} |".format(
                name=row["name"],
                category=row["category"],
                running="yes" if row["running"] else "no",
                rss=rss_mib,
                mapping="<br>".join(mappings) or "no literal artifact parsed",
                action=row["proposed_action"],
            )
        )
    lines.extend(
        [
            "",
            "## Decision rule",
            "",
            "Treat launcher and child-interpreter PIDs as one logical worker. Park only after all live consumers, pending maturities, immutable evidence obligations, dashboards, and rollback checks are cleared. Shared ingestion/schema work may be merged only in a new version; frozen evidence must remain separate.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--supervisor", type=Path, default=DEFAULT_SUPERVISOR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    payload = build(args.supervisor)
    atomic_json(args.output, payload)
    atomic_text(args.report, render(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
