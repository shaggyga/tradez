#!/usr/bin/env python3
"""Build the immutable all-68 mistake-directed historical curriculum."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping

from src.forex_system.research.sequential_all68_mistake_curriculum_v1 import (
    POLICY,
    REPORT_CONTRACT_ID,
    build_report,
    file_sha256,
    json_text,
    read_json,
    render_markdown,
    semantic_sha256,
    stable_hash,
    validate_source_bundle,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "sequential_all68_mistake_curriculum_v1.json"
CORE = ROOT / "src" / "forex_system" / "research" / "sequential_all68_mistake_curriculum_v1.py"
VERIFIER = ROOT / "oanda_sequential_all68_mistake_curriculum_verifier.py"
ALLOWED_LEDGER_ROOT = ROOT / "data" / "oanda_training_manager" / "research_ledgers"


def atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def atomic_text(path: Path, value: str) -> None:
    atomic_bytes(path, value.encode("utf-8"))


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def immutable_text(path: Path, value: str) -> None:
    payload = value.encode("utf-8")
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError(f"immutable artifact conflict: {path}")
        return
    atomic_bytes(path, payload)


def safe_relative(base: Path, value: str, *, must_exist: bool = False) -> Path:
    relative = Path(str(value))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("unsafe relative path")
    path = (base / relative).resolve(strict=must_exist)
    if base.resolve() not in path.parents and path != base.resolve():
        raise ValueError("path escapes base")
    return path


def validate_config(config: Mapping[str, Any]) -> None:
    for key, expected in {**POLICY, "evidence_role": "historical_training_curriculum"}.items():
        if config.get(key) != expected:
            raise ValueError(f"unsafe curriculum config: {key}")
    if int(config.get("schema_version", 0)) != 1:
        raise ValueError("unsupported curriculum config schema")
    source = config.get("source")
    if not isinstance(source, dict):
        raise ValueError("missing exact source contract")
    required_hashes = (
        "required_state_sha256",
        "required_verifier_sha256",
        "required_state_semantic_sha256",
        "required_verifier_semantic_sha256",
        "required_source_pack_manifest_sha256",
        "required_source_pack_verifier_sha256",
        "required_dataset_roots_sha256",
    )
    if any(len(str(source.get(key) or "")) != 64 for key in required_hashes):
        raise ValueError("source hash binding is incomplete")
    if not str(source.get("required_cohort_id") or "").startswith("sequential_all68_portfolio_batch_replay_v1."):
        raise ValueError("wrong source family")
    if not str(source.get("required_source_pack_id") or "").startswith("sequential_replay_source_pack_v1."):
        raise ValueError("wrong source-pack family")
    classification = config.get("classification")
    if not isinstance(classification, dict):
        raise ValueError("missing classification contract")
    if float(classification.get("minimum_material_regret_pips", 0.0)) <= 0:
        raise ValueError("material-regret threshold must be positive")
    if float(classification.get("neutral_brier_baseline", -1.0)) != 0.25:
        raise ValueError("neutral Brier contract changed")
    expected = {
        "categories_are_nonexclusive": True,
        "primary_feedback_weight": 1,
        "depth_one_alternative_weight": 0,
        "review_weight": 0,
        "raw_rows_are_not_independent": True,
        "structural_clusters_are_not_independent_regimes": True,
        "depth_one_is_local_not_global_optimum": True,
    }
    for key, value in expected.items():
        if classification.get(key) != value:
            raise ValueError(f"unsafe classification contract: {key}")


def source_contract(config: Mapping[str, Any]) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Path]]:
    source = config["source"]
    source_root = safe_relative(ROOT, str(source["cohort_relative_root"]), must_exist=True)
    allowed = (ALLOWED_LEDGER_ROOT / "sequential_all68_portfolio_batch_replay_v1" / "cohorts").resolve()
    if allowed not in source_root.parents:
        raise ValueError("source cohort is outside the frozen all-68 ledger")
    paths = {
        "state": safe_relative(source_root, str(source["state_name"]), must_exist=True),
        "receipt": safe_relative(source_root, str(source["verifier_name"]), must_exist=True),
        "pack": safe_relative(source_root, str(source["source_pack_manifest_name"]), must_exist=True),
        "pack_receipt": safe_relative(source_root, str(source["source_pack_verifier_name"]), must_exist=True),
    }
    state = read_json(paths["state"])
    receipt = read_json(paths["receipt"])
    pack = read_json(paths["pack"])
    pack_receipt = read_json(paths["pack_receipt"])
    if file_sha256(paths["state"]) != source["required_state_sha256"]:
        raise ValueError("exact source state bytes changed")
    if file_sha256(paths["receipt"]) != source["required_verifier_sha256"]:
        raise ValueError("exact source verifier bytes changed")
    if semantic_sha256(state, "generated_utc") != source["required_state_semantic_sha256"]:
        raise ValueError("exact source semantic state changed")
    if semantic_sha256(receipt, "generated_utc") != source["required_verifier_semantic_sha256"]:
        raise ValueError("exact source semantic receipt changed")
    if file_sha256(paths["pack"]) != source["required_source_pack_manifest_sha256"]:
        raise ValueError("exact source-pack manifest hash changed")
    if file_sha256(paths["pack_receipt"]) != source["required_source_pack_verifier_sha256"]:
        raise ValueError("exact source-pack verifier hash changed")
    datasets = validate_source_bundle(source_root, state, receipt, pack, pack_receipt, source)
    return source_root, state, receipt, pack, pack_receipt, paths | {"config": DEFAULT_CONFIG}


def material_contract(
    *,
    config: Mapping[str, Any],
    config_path: Path,
    source_state: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    source = config["source"]
    return {
        "schema_version": 1,
        "contract_id": REPORT_CONTRACT_ID,
        "config_sha256": file_sha256(config_path),
        "config_semantic_sha256": stable_hash(config),
        "producer_code_sha256": file_sha256(Path(__file__).resolve()),
        "core_code_sha256": file_sha256(CORE),
        "verifier_code_sha256": file_sha256(VERIFIER),
        "source_cohort_id": source_state["cohort_id"],
        "source_state_sha256": file_sha256(paths["state"]),
        "source_verifier_sha256": file_sha256(paths["receipt"]),
        "source_state_semantic_sha256": semantic_sha256(source_state, "generated_utc"),
        "source_verifier_semantic_sha256": semantic_sha256(read_json(paths["receipt"]), "generated_utc"),
        "source_pack_id": source["required_source_pack_id"],
        "source_pack_manifest_sha256": file_sha256(paths["pack"]),
        "source_pack_verifier_receipt_sha256": file_sha256(paths["pack_receipt"]),
        "source_dataset_roots_sha256": stable_hash(source_state["datasets"]),
        "source_dataset_specs": source_state["datasets"],
        "classification": config["classification"],
        "safety": {**POLICY, "evidence_role": "historical_training_curriculum"},
    }


def run(
    *,
    config_path: Path = DEFAULT_CONFIG,
    output_dir: Path | None = None,
) -> tuple[dict[str, Any], Path, Path, Path]:
    config_path = config_path.resolve(strict=True)
    config = read_json(config_path)
    validate_config(config)
    source_root, state, receipt, pack, pack_receipt, paths = source_contract(config)
    datasets = validate_source_bundle(source_root, state, receipt, pack, pack_receipt, config["source"])
    material = material_contract(config=config, config_path=config_path, source_state=state, paths=paths)
    material_sha = stable_hash(material)
    cohort_id = str(config["experiment_key"]) + "." + material_sha[:20]
    source_binding = {
        "cohort_id": state["cohort_id"],
        "source_pack_id": config["source"]["required_source_pack_id"],
        "source_state_sha256": material["source_state_sha256"],
        "source_verifier_sha256": material["source_verifier_sha256"],
        "source_state_semantic_sha256": material["source_state_semantic_sha256"],
        "source_verifier_semantic_sha256": material["source_verifier_semantic_sha256"],
        "source_pack_manifest_sha256": material["source_pack_manifest_sha256"],
        "source_pack_verifier_receipt_sha256": material["source_pack_verifier_receipt_sha256"],
        "source_material_sha256": state["material_sha256"],
        "source_dataset_roots_sha256": material["source_dataset_roots_sha256"],
        "source_dataset_specs": state["datasets"],
    }
    report = build_report(
        source_state=state,
        source_binding=source_binding,
        material_contract=material,
        cohort_id=cohort_id,
        classification=config["classification"],
        datasets=datasets,
    )

    artifact_root = output_dir.resolve() if output_dir else safe_relative(ROOT, str(config["storage"]["artifact_relative_root"]))
    if output_dir is None and ALLOWED_LEDGER_ROOT.resolve() not in artifact_root.parents:
        raise ValueError("artifact root outside research ledgers")
    cohort_root = artifact_root / "cohorts" / cohort_id
    report_path = cohort_root / str(config["storage"]["report_json_name"])
    markdown_path = cohort_root / str(config["storage"]["report_markdown_name"])
    material_path = cohort_root / str(config["storage"]["material_contract_name"])
    receipt_path = cohort_root / str(config["storage"]["verifier_name"])
    immutable_text(report_path, json_text(report))
    immutable_text(markdown_path, render_markdown(report))
    immutable_text(material_path, json.dumps(material, indent=2, sort_keys=True) + "\n")

    completed = subprocess.run(
        [
            sys.executable,
            str(VERIFIER),
            "--config",
            str(config_path),
            "--report",
            str(report_path),
            "--material",
            str(material_path),
            "--output",
            str(receipt_path),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise ValueError("independent all-68 mistake verification failed: " + (completed.stdout + completed.stderr).strip())
    verifier_receipt = read_json(receipt_path)
    if verifier_receipt.get("verified") is not True or verifier_receipt.get("failures") != []:
        raise ValueError("independent all-68 mistake verification failed closed")

    atomic_text(artifact_root / str(config["storage"]["current_state_name"]), json_text(report))
    atomic_text(artifact_root / str(config["storage"]["current_report_name"]), render_markdown(report))
    atomic_json(artifact_root / str(config["storage"]["current_verifier_name"]), verifier_receipt)
    return report, report_path, markdown_path, receipt_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args()
    report, report_path, markdown_path, receipt_path = run(config_path=args.config)
    summary = report["summary"]
    print(
        f"{report['cohort_id']} {report['report_id']} "
        f"primary={summary['source_primary_feedback_count']} "
        f"reviews={summary['source_depth_one_review_count']} "
        f"mistake_clocks={summary['primary_clocks_with_any_mistake']} verified=true"
    )
    print(report_path)
    print(markdown_path)
    print(receipt_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
