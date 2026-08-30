#!/usr/bin/env python3
"""Independent verifier for sequential replay exact-window source packs.

This file intentionally imports neither the producer nor its research core.
It reconstructs schedules, exact source slices, missing-data coverage, archive
identities, and the material pack identity from the retained candle files.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "sequential_replay_source_pack_v1.json"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_hash(*parts: Any) -> str:
    value = parts[0] if len(parts) == 1 else list(parts)
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            block = source.read(8 * 1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def parse_epoch(value: Any) -> int:
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        head, tail = text.split(".", 1)
        offset = ""
        if "+" in tail:
            fraction, zone = tail.split("+", 1)
            offset = "+" + zone
        elif "-" in tail:
            fraction, zone = tail.rsplit("-", 1)
            offset = "-" + zone
        else:
            fraction = tail
        text = head + "." + fraction[:6] + offset
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + f".{os.getpid()}.tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, path)


def contained(root: Path, relative: str) -> Path:
    relative_path = Path(str(relative))
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError("path_outside_root")
    path = root / relative_path
    path.resolve().relative_to(root.resolve())
    return path


def is_link(path: Path) -> bool:
    try:
        attributes = int(getattr(path.lstat(), "st_file_attributes", 0)) if path.exists() or path.is_symlink() else 0
        return path.is_symlink() or bool(os.path.islink(path)) or bool(attributes & 0x400)
    except OSError:
        return True


def schedule_clocks(start: int, end: int, cadence_min: int) -> tuple[int, ...]:
    width = int(cadence_min) * 60
    first = ((int(start) + width - 1) // width) * width
    return tuple(range(first, int(end), width))


def load_source(path: Path, expected_header: list[str], instrument: str, maximum_bytes: int) -> dict[str, Any]:
    if not path.is_file() or is_link(path):
        raise ValueError("missing_or_linked_source")
    if path.stat().st_size > int(maximum_bytes):
        raise ValueError("oversized_source")
    payload = path.read_bytes()
    lines = payload.splitlines()
    if not lines or next(csv.reader([lines[0].decode("utf-8")])) != expected_header:
        raise ValueError("header_mismatch")
    time_index = expected_header.index("time")
    instrument_index = expected_header.index("instrument")
    granularity_index = expected_header.index("granularity")
    rows: dict[int, bytes] = {}
    prior: int | None = None
    for raw in lines[1:]:
        if not raw.strip():
            continue
        values = next(csv.reader([raw.decode("utf-8")]))
        if len(values) != len(expected_header):
            raise ValueError("row_width_mismatch")
        if values[instrument_index] != instrument or values[granularity_index] != "M1":
            raise ValueError("row_identity_mismatch")
        epoch = parse_epoch(values[time_index])
        if prior is not None and epoch <= prior:
            raise ValueError("nonmonotone_source")
        prior = epoch
        rows[epoch] = raw + b"\n"
    if not rows:
        raise ValueError("empty_source")
    return {
        "header": lines[0] + b"\n",
        "rows": rows,
        "sha256": sha256_bytes(payload),
        "bytes": len(payload),
        "first": next(iter(rows)),
        "last": next(reversed(rows)),
    }


def read_gzip_bounded(
    path: Path,
    maximum_raw_bytes: int,
    maximum_gzip_bytes: int | None = None,
) -> bytes:
    if not path.is_file() or is_link(path):
        raise ValueError("missing_or_linked_archive")
    compressed_limit = int(maximum_gzip_bytes or maximum_raw_bytes)
    if path.stat().st_size <= 0 or path.stat().st_size > compressed_limit:
        raise ValueError("oversized_archive_gzip")
    with path.open("rb") as raw, gzip.GzipFile(fileobj=raw, mode="rb") as stream:
        value = stream.read(int(maximum_raw_bytes) + 1)
        if len(value) > int(maximum_raw_bytes):
            raise ValueError("oversized_archive_raw")
        if stream.read(1):
            raise ValueError("oversized_archive_tail")
    return value


IDENTITY_KEYS = (
    "instrument", "session_key", "session_start_epoch", "session_end_epoch",
    "slice_first_epoch", "slice_last_epoch", "scheduled_clock_count",
    "scheduled_clock_sha256", "context_ready_clock_count",
    "execution_ready_clock_count", "feedback_ready_clock_count",
    "fully_ready_clock_count", "coverage_failure_count",
    "coverage_failures_sha256", "expected_minute_count", "present_minute_count",
    "missing_minute_count", "missing_minutes_sha256", "raw_bytes", "raw_sha256",
    "gzip_bytes", "gzip_sha256",
)


def verify(config_path: Path = DEFAULT_CONFIG, *, root: Path = ROOT) -> dict[str, Any]:
    root = root.resolve()
    config = read_json(config_path)
    failures: list[str] = []
    if config.get("research_only") is not True or config.get("supported_decision") != "no_trade":
        failures.append("config_isolation")
    for key in ("execution_eligible", "proof_eligible", "can_promote", "can_place_orders", "can_authorize", "broker_access", "account_access"):
        if config.get(key) is not False:
            failures.append(f"config_{key}")
    output_root = contained(root, str(config["storage"]["artifact_relative_root"]))
    state_path = output_root / str(config["storage"]["current_state_name"])
    state = read_json(state_path)
    expected_safety = {
        key: config.get(key)
        for key in (
            "research_only", "execution_eligible", "proof_eligible",
            "can_promote", "can_place_orders", "can_authorize",
            "broker_access", "account_access", "supported_decision",
        )
    }
    for key, expected in expected_safety.items():
        if state.get(key) != expected:
            failures.append(f"state_safety_{key}")
    if state.get("evidence_role") != "historical_training_discovery":
        failures.append("state_evidence_role")
    pack_id = str(state.get("pack_id") or "")
    pack_manifest_path = output_root / "packs" / pack_id / "manifest.json"
    if not pack_manifest_path.is_file() or read_json(pack_manifest_path) != state:
        failures.append("current_pointer_or_pack_manifest_mismatch")
    contracts = state.get("contracts") or {}
    live_contracts = {
        "config": config_path,
        "runner": root / "oanda_sequential_replay_source_pack.py",
        "core": root / "src" / "forex_system" / "research" / "sequential_replay_source_pack_v1.py",
        "verifier": Path(__file__).resolve(),
    }
    for label, path in live_contracts.items():
        expected = str((contracts.get(label) or {}).get("sha256") or "")
        if not path.is_file() or file_sha256(path) != expected:
            failures.append(f"contract_{label}_mismatch")
        archive_rel = str((contracts.get(label) or {}).get("archive_relative_path") or "")
        try:
            archive = contained(output_root, archive_rel)
            if not archive.is_file() or is_link(archive) or file_sha256(archive) != expected:
                failures.append(f"contract_{label}_archive_mismatch")
        except Exception:
            failures.append(f"contract_{label}_archive_path")
    source_config = config["source"]
    schedule = config["schedule"]
    storage = config["storage"]
    sessions = {str(row["session_key"]): row for row in schedule["sessions"]}
    source_root = contained(root, str(source_config["relative_root"]))
    cached_sources: dict[str, dict[str, Any]] = {}
    rebuilt_identities: list[dict[str, Any]] = []
    rebuilt_manifest_rows: list[dict[str, Any]] = []
    expected_manifest_identities = {
        (session_key, str(instrument))
        for session_key in sessions
        for instrument in source_config["expected_instruments"]
    }
    seen_manifest_identities: set[tuple[str, str]] = set()
    seen_archive_paths: set[str] = set()
    active_pack_root = output_root / "packs" / pack_id
    for stored in state.get("source_manifest") or []:
        instrument = str(stored.get("instrument") or "")
        session_key = str(stored.get("session_key") or "")
        manifest_identity = (session_key, instrument)
        if manifest_identity in seen_manifest_identities:
            failures.append(f"duplicate_manifest_identity:{session_key}:{instrument}")
            continue
        seen_manifest_identities.add(manifest_identity)
        if instrument not in source_config["expected_instruments"] or session_key not in sessions:
            failures.append("unexpected_manifest_identity")
            continue
        try:
            if instrument not in cached_sources:
                filename = str(source_config["filename_template"]).format(instrument=instrument)
                cached_sources[instrument] = load_source(
                    contained(source_root, filename),
                    list(source_config["required_columns"]),
                    instrument,
                    int(source_config["maximum_source_bytes_per_instrument"]),
                )
            source = cached_sources[instrument]
            session = sessions[session_key]
            start = parse_epoch(session["start_utc"])
            end = parse_epoch(session["end_utc_exclusive"])
            lookback = int(schedule["feature_lookback_min"])
            execution_delay = int(schedule["execution_delay_min"])
            feedback_horizon = int(schedule["feedback_horizon_min"])
            first = start - (lookback + 1) * 60
            last = end + max(0, feedback_horizon - 5) * 60
            rows = source["rows"]
            present = tuple(epoch for epoch in rows if first <= epoch <= last)
            expected_raw = source["header"] + b"".join(rows[epoch] for epoch in present)
            archive_path = contained(output_root, str(stored["archive_relative_path"]))
            archive_path.resolve().relative_to(active_pack_root.resolve())
            archive_key = archive_path.resolve().as_posix().lower()
            if archive_key in seen_archive_paths:
                failures.append(f"duplicate_archive_path:{session_key}:{instrument}")
            seen_archive_paths.add(archive_key)
            if archive_path.suffixes[-2:] != [".csv", ".gz"]:
                failures.append(f"archive_extension:{session_key}:{instrument}")
            if archive_path.stat().st_size > int(storage["maximum_archive_gzip_bytes"]):
                raise ValueError("oversized_archive_gzip")
            compressed = archive_path.read_bytes()
            raw = read_gzip_bounded(
                archive_path,
                int(storage["maximum_archive_raw_bytes"]),
                int(storage["maximum_archive_gzip_bytes"]),
            )
            if raw != expected_raw:
                failures.append(f"archive_source_mismatch:{session_key}:{instrument}")
            clocks = schedule_clocks(start, end, int(schedule["decision_cadence_min"]))
            context_ready = execution_ready = feedback_ready = fully_ready = 0
            coverage_failures: list[dict[str, Any]] = []
            for clock in clocks:
                context_epochs = range(clock - (lookback + 1) * 60, clock, 60)
                missing_context = tuple(epoch for epoch in context_epochs if epoch not in rows)
                execution_epoch = clock + execution_delay * 60
                feedback_epoch = clock + feedback_horizon * 60
                has_context = not missing_context
                has_execution = execution_epoch in rows
                has_feedback = feedback_epoch in rows
                context_ready += int(has_context)
                execution_ready += int(has_execution)
                feedback_ready += int(has_feedback)
                fully_ready += int(has_context and has_execution and has_feedback)
                if not (has_context and has_execution and has_feedback):
                    coverage_failures.append({
                        "decision_epoch": clock,
                        "missing_context_count": len(missing_context),
                        "missing_context_sha256": stable_hash(missing_context),
                        "execution_quote_ready": has_execution,
                        "feedback_quote_ready": has_feedback,
                    })
            expected_minutes = tuple(range(first, last + 60, 60))
            missing_minutes = tuple(epoch for epoch in expected_minutes if epoch not in rows)
            rebuilt = dict(stored)
            rebuilt.update({
                "session_start_epoch": start,
                "session_end_epoch": end,
                "slice_first_epoch": first,
                "slice_last_epoch": last,
                "scheduled_clock_count": len(clocks),
                "scheduled_clock_sha256": stable_hash(clocks),
                "context_ready_clock_count": context_ready,
                "execution_ready_clock_count": execution_ready,
                "feedback_ready_clock_count": feedback_ready,
                "fully_ready_clock_count": fully_ready,
                "coverage_failure_count": len(coverage_failures),
                "coverage_failures_sha256": stable_hash(coverage_failures),
                "expected_minute_count": len(expected_minutes),
                "present_minute_count": len(present),
                "missing_minute_count": len(missing_minutes),
                "missing_minutes_sha256": stable_hash(missing_minutes),
                "raw_bytes": len(raw),
                "raw_sha256": sha256_bytes(raw),
                "gzip_bytes": len(compressed),
                "gzip_sha256": sha256_bytes(compressed),
                "coverage_failures": coverage_failures,
            })
            for key in tuple(rebuilt):
                if key not in stored:
                    rebuilt.pop(key, None)
            if rebuilt != stored:
                failures.append(f"manifest_rebuild_mismatch:{session_key}:{instrument}")
            rebuilt_manifest_rows.append(rebuilt)
            rebuilt_identities.append({key: rebuilt[key] for key in IDENTITY_KEYS})
        except Exception as exc:
            failures.append(f"slice_error:{session_key}:{instrument}:{type(exc).__name__}:{exc}")
    expected_count = len(source_config["expected_instruments"]) * len(sessions)
    if len(rebuilt_manifest_rows) != expected_count:
        failures.append("manifest_count")
    if seen_manifest_identities != expected_manifest_identities:
        failures.append("manifest_identity_cartesian_product")
    if len(seen_archive_paths) != expected_count:
        failures.append("archive_path_identity_count")
    if set(cached_sources) != set(source_config["expected_instruments"]):
        failures.append("source_instrument_cartesian_product")
    rebuilt_manifest_rows.sort(key=lambda row: (row["session_key"], row["instrument"]))
    rebuilt_identities.sort(key=lambda row: (row["session_key"], row["instrument"]))
    if stable_hash(rebuilt_manifest_rows) != state.get("source_manifest_sha256"):
        failures.append("source_manifest_root")
    material = {
        "schema_version": 1,
        "experiment_key": config["experiment_key"],
        "config_sha256": str((contracts.get("config") or {}).get("sha256") or ""),
        "runner_sha256": str((contracts.get("runner") or {}).get("sha256") or ""),
        "core_sha256": str((contracts.get("core") or {}).get("sha256") or ""),
        "verifier_sha256": str((contracts.get("verifier") or {}).get("sha256") or ""),
        "safety": expected_safety,
        "slice_identity_manifest": rebuilt_identities,
    }
    material_sha = stable_hash(material)
    if material != state.get("material_contract") or material_sha != state.get("material_sha256"):
        failures.append("material_contract")
    if pack_id != "sequential_replay_source_pack_v1." + material_sha[:20]:
        failures.append("pack_id")
    scheduled_global = sum(len(schedule_clocks(parse_epoch(row["start_utc"]), parse_epoch(row["end_utc_exclusive"]), int(schedule["decision_cadence_min"]))) for row in sessions.values())
    if int(state.get("scheduled_global_clock_count") or -1) != scheduled_global:
        failures.append("global_clock_count")
    if int(state.get("market_repetition_count") or -1) != scheduled_global:
        failures.append("market_repetition_inflation")
    if int(state.get("scheduled_pair_context_count") or -1) != scheduled_global * 68:
        failures.append("pair_context_count")
    rebuilt_coverage = {
        "fully_ready_pair_context_count": sum(int(row["fully_ready_clock_count"]) for row in rebuilt_manifest_rows),
        "context_failure_count": sum(int(row["scheduled_clock_count"]) - int(row["context_ready_clock_count"]) for row in rebuilt_manifest_rows),
        "execution_failure_count": sum(int(row["scheduled_clock_count"]) - int(row["execution_ready_clock_count"]) for row in rebuilt_manifest_rows),
        "feedback_failure_count": sum(int(row["scheduled_clock_count"]) - int(row["feedback_ready_clock_count"]) for row in rebuilt_manifest_rows),
        "missing_minute_count": sum(int(row["missing_minute_count"]) for row in rebuilt_manifest_rows),
    }
    if state.get("coverage") != rebuilt_coverage:
        failures.append("coverage_summary")
    expected_summary = {
        "instrument_count": len(source_config["expected_instruments"]),
        "session_count": len(sessions),
        "slice_count": expected_count,
        "raw_archive_bytes": sum(int(row["raw_bytes"]) for row in rebuilt_manifest_rows),
        "gzip_archive_bytes": sum(int(row["gzip_bytes"]) for row in rebuilt_manifest_rows),
    }
    for key, expected in expected_summary.items():
        if int(state.get(key) or -1) != int(expected):
            failures.append(f"summary_{key}")
    if state.get("independent_regime_count") is not None:
        failures.append("independent_regime_overclaim")
    receipt = {
        "schema_version": 1,
        "pack_id": pack_id,
        "verified": not failures,
        "failure_count": len(failures),
        "failures": failures,
        "reconstructed_instrument_count": len(cached_sources),
        "reconstructed_slice_count": len(rebuilt_manifest_rows),
        "reconstructed_global_clock_count": scheduled_global,
        "reconstructed_pair_context_count": scheduled_global * 68,
        "source_manifest_sha256": stable_hash(rebuilt_manifest_rows),
        "material_sha256": material_sha,
    }
    atomic_json(output_root / str(storage["current_verifier_name"]), receipt)
    return receipt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    return parser.parse_args()


def main() -> int:
    receipt = verify(parse_args().config)
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0 if receipt["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
