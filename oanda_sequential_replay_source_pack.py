#!/usr/bin/env python3
"""Build deterministic exact-window, all-68 replay source packs.

The component is historical research infrastructure only.  It reads retained
practice bid/ask M1 files and has no broker, account, authorization, lifecycle,
signal-publication, or execution imports.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

from src.forex_system.research.sequential_replay_source_pack_v1 import (
    PackedSlice,
    build_packed_slice,
    canonical_json,
    parse_epoch,
    read_source,
    stable_hash,
    validate_config,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "sequential_replay_source_pack_v1.json"
CORE_PATH = ROOT / "src" / "forex_system" / "research" / "sequential_replay_source_pack_v1.py"
VERIFIER_PATH = ROOT / "oanda_sequential_replay_source_pack_verifier.py"


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            block = source.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".tmp-{os.getpid()}-{hashlib.sha256(str(path).encode('utf-8')).hexdigest()[:12]}"
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def atomic_text(path: Path, value: str) -> None:
    atomic_bytes(path, value.encode("utf-8"))


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def contained_path(root: Path, relative: str) -> Path:
    relative_path = Path(str(relative))
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError(f"path outside project root: {relative}")
    candidate = root / relative_path
    try:
        candidate.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"path outside project root: {relative}") from exc
    return candidate


def artifact_root(config: Mapping[str, Any], root: Path) -> Path:
    value = contained_path(root, str(config["storage"]["artifact_relative_root"]))
    if value == root.resolve():
        raise ValueError("artifact root cannot be project root")
    value.mkdir(parents=True, exist_ok=True)
    return value


def identity_manifest(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: row[key]
        for key in (
            "instrument",
            "session_key",
            "session_start_epoch",
            "session_end_epoch",
            "slice_first_epoch",
            "slice_last_epoch",
            "scheduled_clock_count",
            "scheduled_clock_sha256",
            "context_ready_clock_count",
            "execution_ready_clock_count",
            "feedback_ready_clock_count",
            "fully_ready_clock_count",
            "coverage_failure_count",
            "coverage_failures_sha256",
            "expected_minute_count",
            "present_minute_count",
            "missing_minute_count",
            "missing_minutes_sha256",
            "raw_bytes",
            "raw_sha256",
            "gzip_bytes",
            "gzip_sha256",
        )
    }


def _write_contract(root: Path, path: Path, label: str) -> dict[str, Any]:
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    destination = root / "contracts" / f"{digest}.{label}"
    if destination.exists():
        if destination.read_bytes() != payload:
            raise ValueError(f"contract archive conflict: {destination}")
    else:
        atomic_bytes(destination, payload)
    return {
        "label": label,
        "sha256": digest,
        "archive_relative_path": destination.relative_to(root).as_posix(),
    }


def render_report(state: Mapping[str, Any]) -> str:
    coverage = state["coverage"]
    lines = [
        "# Sequential replay exact-window source pack V1",
        "",
        "Status: generated historical training-source pack; independent verification required",
        "",
        f"- Pack: `{state['pack_id']}`",
        f"- Instruments: **{state['instrument_count']} / 68**",
        f"- Deterministic sessions: **{state['session_count']}**",
        f"- Scheduled global clocks: **{state['scheduled_global_clock_count']:,}**",
        f"- Pair contexts scheduled independently of coverage: **{state['scheduled_pair_context_count']:,}**",
        f"- Fully ready pair contexts: **{coverage['fully_ready_pair_context_count']:,}**",
        f"- Context failures: **{coverage['context_failure_count']:,}**",
        f"- Missing exact execution quotes: **{coverage['execution_failure_count']:,}**",
        f"- Missing exact feedback quotes: **{coverage['feedback_failure_count']:,}**",
        f"- Exact-window archives: **{state['slice_count']}**",
        f"- Raw/archive size: **{state['raw_archive_bytes'] / 1048576.0:.2f} / {state['gzip_archive_bytes'] / 1048576.0:.2f} MiB**",
        "",
        "The scheduled global clock is the repetition unit. Pair contexts, shared-currency expressions, missing-data rows, later replay attempts, and counterfactuals do not create independent market repetitions. These weekday blocks are structural coverage, not proven independent regimes.",
        "",
        "No missing candle or quote was forward-filled, backward-filled, or used to remove a scheduled clock. The downstream replay must fail closed on the recorded unavailable pair context.",
        "",
        "Practice 007, authorization, lifecycle promotion, and real-money routing are outside this component.",
    ]
    return "\n".join(lines) + "\n"


def build(config_path: Path = DEFAULT_CONFIG, *, root: Path = ROOT) -> dict[str, Any]:
    root = root.resolve()
    config_path = config_path.resolve()
    config = read_json(config_path)
    validate_config(config)
    output_root = artifact_root(config, root)
    source_root = contained_path(root, str(config["source"]["relative_root"]))
    if not source_root.is_dir():
        raise ValueError(f"source root missing: {source_root}")
    contracts = {
        "config": _write_contract(output_root, config_path, "config.json"),
        "runner": _write_contract(output_root, Path(__file__).resolve(), "runner.py"),
        "core": _write_contract(output_root, CORE_PATH, "core.py"),
        "verifier": _write_contract(output_root, VERIFIER_PATH, "verifier.py"),
    }
    source_config = config["source"]
    storage = config["storage"]
    schedule = config["schedule"]
    packed: list[PackedSlice] = []
    for instrument in source_config["expected_instruments"]:
        filename = str(source_config["filename_template"]).format(instrument=instrument)
        path = contained_path(source_root, filename)
        source = read_source(
            path,
            instrument=str(instrument),
            required_columns=source_config["required_columns"],
            maximum_bytes=int(source_config["maximum_source_bytes_per_instrument"]),
            reject_links=bool(source_config["reject_links"]),
        )
        for session in schedule["sessions"]:
            packed.append(
                build_packed_slice(
                    source,
                    session=session,
                    schedule=schedule,
                    compresslevel=int(storage["gzip_compresslevel"]),
                    maximum_archive_raw_bytes=int(storage["maximum_archive_raw_bytes"]),
                )
            )
            if len(packed[-1].gzip_bytes) > int(storage["maximum_archive_gzip_bytes"]):
                raise ValueError(
                    f"compressed archive exceeds limit: {instrument}:{session['session_key']}"
                )
    packed.sort(key=lambda row: (row.session_key, row.instrument))
    identities = [identity_manifest(row.manifest) for row in packed]
    material_contract = {
        "schema_version": 1,
        "experiment_key": config["experiment_key"],
        "config_sha256": contracts["config"]["sha256"],
        "runner_sha256": contracts["runner"]["sha256"],
        "core_sha256": contracts["core"]["sha256"],
        "verifier_sha256": contracts["verifier"]["sha256"],
        "safety": {
            key: config[key]
            for key in (
                "research_only", "execution_eligible", "proof_eligible",
                "can_promote", "can_place_orders", "can_authorize",
                "broker_access", "account_access", "supported_decision",
            )
        },
        "slice_identity_manifest": identities,
    }
    material_sha = stable_hash(material_contract)
    pack_id = "sequential_replay_source_pack_v1." + material_sha[:20]
    pack_root = output_root / "packs" / pack_id
    archive_rows: list[dict[str, Any]] = []
    for row in packed:
        # The full SHA-256 remains in the manifest and verifier.  A bounded
        # locator avoids legacy Win32 MAX_PATH failures in this deep artifact
        # hierarchy without weakening content verification.
        destination = pack_root / "sources" / f"{row.raw_sha256[:32]}.csv.gz"
        if destination.exists():
            if destination.read_bytes() != row.gzip_bytes:
                raise ValueError(f"archive conflict: {destination}")
        else:
            atomic_bytes(destination, row.gzip_bytes)
        manifest = dict(row.manifest)
        manifest["archive_relative_path"] = destination.relative_to(output_root).as_posix()
        archive_rows.append(manifest)
    sessions = list(schedule["sessions"])
    scheduled_global = sum(
        (parse_epoch(row["end_utc_exclusive"]) - parse_epoch(row["start_utc"]))
        // (int(schedule["decision_cadence_min"]) * 60)
        for row in sessions
    )
    coverage = {
        "fully_ready_pair_context_count": sum(int(row["fully_ready_clock_count"]) for row in archive_rows),
        "context_failure_count": sum(int(row["scheduled_clock_count"]) - int(row["context_ready_clock_count"]) for row in archive_rows),
        "execution_failure_count": sum(int(row["scheduled_clock_count"]) - int(row["execution_ready_clock_count"]) for row in archive_rows),
        "feedback_failure_count": sum(int(row["scheduled_clock_count"]) - int(row["feedback_ready_clock_count"]) for row in archive_rows),
        "missing_minute_count": sum(int(row["missing_minute_count"]) for row in archive_rows),
    }
    state = {
        "schema_version": 1,
        "pack_id": pack_id,
        "material_sha256": material_sha,
        "research_only": True,
        "execution_eligible": False,
        "proof_eligible": False,
        "can_promote": False,
        "can_place_orders": False,
        "can_authorize": False,
        "broker_access": False,
        "account_access": False,
        "supported_decision": "no_trade",
        "evidence_role": "historical_training_discovery",
        "selection_rule": schedule["selection_rule"],
        "instrument_count": len(source_config["expected_instruments"]),
        "session_count": len(sessions),
        "slice_count": len(archive_rows),
        "scheduled_global_clock_count": scheduled_global,
        "scheduled_pair_context_count": scheduled_global * len(source_config["expected_instruments"]),
        "market_repetition_count": scheduled_global,
        "independent_regime_count": None,
        "independent_regime_state": "unknown_structural_weekday_blocks_only",
        "raw_archive_bytes": sum(int(row["raw_bytes"]) for row in archive_rows),
        "gzip_archive_bytes": sum(int(row["gzip_bytes"]) for row in archive_rows),
        "coverage": coverage,
        "sessions": sessions,
        "contracts": contracts,
        "material_contract": material_contract,
        "source_manifest": archive_rows,
        "source_manifest_sha256": stable_hash(archive_rows),
    }
    manifest_path = pack_root / "manifest.json"
    if manifest_path.exists():
        existing = read_json(manifest_path)
        existing_identities = [
            identity_manifest(row) for row in existing.get("source_manifest") or []
        ]
        existing_identities.sort(key=lambda row: (row["session_key"], row["instrument"]))
        if (
            existing.get("material_sha256") != material_sha
            or existing.get("material_contract") != material_contract
            or existing_identities != identities
        ):
            raise ValueError(f"immutable pack conflict: {pack_id}")
        # Source files are append-only while a historical exact window is
        # stable. Capture-time whole-file diagnostics may therefore change
        # without creating a new pack; the already sealed exact-slice state is
        # authoritative when its material identities still match.
        state = existing
    else:
        atomic_json(manifest_path, state)
    atomic_json(output_root / str(storage["current_state_name"]), state)
    atomic_text(output_root / str(storage["current_report_name"]), render_report(state))
    return state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state = build(args.config)
    print(json.dumps({
        "pack_id": state["pack_id"],
        "instrument_count": state["instrument_count"],
        "session_count": state["session_count"],
        "scheduled_global_clock_count": state["scheduled_global_clock_count"],
        "scheduled_pair_context_count": state["scheduled_pair_context_count"],
        "coverage": state["coverage"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
