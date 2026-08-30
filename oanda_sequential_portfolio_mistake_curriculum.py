#!/usr/bin/env python3
"""Build and independently verify the research-only mistake curriculum."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping

from src.forex_system.research.sequential_portfolio_mistake_curriculum_v1 import (
    POLICY,
    REPORT_CONTRACT_ID,
    build_report,
    json_text,
    open_verified_database,
    read_json,
    render_markdown,
    stable_hash,
    validate_verified_binding,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "sequential_portfolio_mistake_curriculum_v1.json"
VERIFIER_PATH = ROOT / "oanda_sequential_portfolio_mistake_curriculum_verifier.py"
LEDGER_ROOT = ROOT / "data" / "oanda_training_manager" / "research_ledgers" / "sequential_portfolio_replay_v1"
DEFAULT_STATE = LEDGER_ROOT / "sequential_portfolio_replay_v1.json"
DEFAULT_VERIFIER = LEDGER_ROOT / "sequential_portfolio_replay_verifier_v1.json"


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8", newline="\n")
    temporary.replace(path)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def immutable_text(path: Path, value: str) -> None:
    encoded = value.encode("utf-8")
    if path.exists():
        if path.read_bytes() != encoded:
            raise ValueError(f"immutable artifact conflict: {path}")
        return
    atomic_text(path, value)


def validate_config(config: Mapping[str, Any]) -> None:
    expected = {**POLICY, "evidence_role": "historical_training_curriculum"}
    for key, value in expected.items():
        if config.get(key) != value:
            raise ValueError(f"unsafe mistake-curriculum config: {key}")
    if int(config.get("schema_version", 0)) != 1:
        raise ValueError("unsupported mistake-curriculum config schema")
    classification = config.get("classification")
    if not isinstance(classification, dict):
        raise ValueError("missing classification contract")
    if float(classification.get("minimum_material_regret_pips", 0.0)) <= 0:
        raise ValueError("minimum material regret must be positive")
    if float(classification.get("neutral_brier_baseline", -1.0)) != 0.25:
        raise ValueError("neutral Brier baseline changed")
    required_true = (
        "categories_are_nonexclusive",
        "raw_observations_are_not_independent",
        "structural_clusters_are_not_independent_regimes",
    )
    if any(classification.get(key) is not True for key in required_true):
        raise ValueError("independence warning contract changed")
    if not str(config.get("experiment_key") or ""):
        raise ValueError("missing experiment key")


def resolve_relative(path_text: str) -> Path:
    relative = Path(path_text)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("unsafe source path")
    resolved = (ROOT / relative).resolve(strict=True)
    if ROOT.resolve() not in resolved.parents:
        raise ValueError("source path escapes project root")
    return resolved


def safe_artifact_root(config: Mapping[str, Any]) -> Path:
    relative = Path(str(config["storage"]["artifact_relative_root"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("unsafe artifact path")
    resolved = (ROOT / relative).resolve()
    allowed = (ROOT / "data" / "oanda_training_manager" / "research_ledgers").resolve()
    if allowed not in resolved.parents or resolved == allowed:
        raise ValueError("artifact path outside research ledgers")
    return resolved


def source_contract(
    config: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], Path, Path, Path]:
    source = config["source"]
    state_path = resolve_relative(str(source["state_relative_path"]))
    verifier_path = resolve_relative(str(source["verifier_relative_path"]))
    state = read_json(state_path)
    verifier = read_json(verifier_path)
    validate_verified_binding(state, verifier)
    if state.get("cohort_id") != source.get("required_cohort_id"):
        raise ValueError("wrong source cohort")
    if state.get("session_id") != source.get("required_session_id"):
        raise ValueError("wrong source session")
    if verifier.get("verified") is not bool(source.get("required_verified")):
        raise ValueError("source verifier state changed")
    if source.get("required_zero_failures") and verifier.get("failures") != []:
        raise ValueError("source verifier failures")
    if state.get("evidence_role") != source.get("required_evidence_role"):
        raise ValueError("source evidence role changed")
    if state.get("proof_eligible") is not source.get("required_proof_eligible"):
        raise ValueError("source proof eligibility changed")
    database = Path(str(state["database"])).resolve(strict=True)
    if LEDGER_ROOT.resolve() not in database.parents:
        raise ValueError("source database outside frozen replay ledger")
    return state, verifier, database, state_path, verifier_path


def build_material_contract(
    *,
    config: Mapping[str, Any],
    config_path: Path,
    state: Mapping[str, Any],
    database: Path,
    state_path: Path,
    verifier_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "contract_id": REPORT_CONTRACT_ID,
        "config_sha256": file_sha256(config_path),
        "config_semantic_sha256": stable_hash(config),
        "producer_code_sha256": file_sha256(Path(__file__).resolve()),
        "core_code_sha256": file_sha256(ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_mistake_curriculum_v1.py"),
        "verifier_code_sha256": file_sha256(VERIFIER_PATH),
        "source_state_sha256": file_sha256(state_path),
        "source_verifier_receipt_sha256": file_sha256(verifier_path),
        "source_database_sha256": file_sha256(database),
        "source_cohort_id": state["cohort_id"],
        "source_session_id": state["session_id"],
        "source_session_seal_id": state["session_seal_id"],
        "source_session_seal_roots_sha256": stable_hash(state["roots"]),
        "classification": config["classification"],
        "policy": {**POLICY, "evidence_role": "historical_training_curriculum"},
    }


def run(
    *,
    config_path: Path = DEFAULT_CONFIG,
    output_dir: Path | None = None,
) -> tuple[dict[str, Any], Path, Path]:
    config_path = config_path.resolve(strict=True)
    config = read_json(config_path)
    validate_config(config)
    state, verifier, database, state_path, verifier_path = source_contract(config)
    material = build_material_contract(
        config=config,
        config_path=config_path,
        state=state,
        database=database,
        state_path=state_path,
        verifier_path=verifier_path,
    )
    material_sha = stable_hash(material)
    cohort_id = str(config["experiment_key"]) + "." + material_sha[:20]
    source_binding: dict[str, Any] = {
        "cohort_id": state["cohort_id"],
        "session_id": state["session_id"],
        "session_seal_id": state["session_seal_id"],
        "session_seal_sha256": None,
        "as_of_seal_utc": None,
        "verified_roots": state["roots"],
        "source_state_sha256": material["source_state_sha256"],
        "source_verifier_receipt_sha256": material["source_verifier_receipt_sha256"],
        "source_database_sha256": material["source_database_sha256"],
    }
    connection = open_verified_database(database, state=state, allowed_root=LEDGER_ROOT)
    try:
        seal = connection.execute(
            "SELECT seal_sha256,created_utc FROM spr_session_seals WHERE seal_id=?",
            (state["session_seal_id"],),
        ).fetchone()
        if seal is None:
            raise ValueError("source session seal missing")
        source_binding["session_seal_sha256"] = str(seal["seal_sha256"])
        source_binding["as_of_seal_utc"] = str(seal["created_utc"])
        report = build_report(
            connection,
            state=state,
            curriculum_cohort_id=cohort_id,
            material_contract=material,
            material_contract_sha256=material_sha,
            source_binding=source_binding,
            classification_contract=config["classification"],
        )
    finally:
        connection.close()

    artifact_root = output_dir.resolve() if output_dir else safe_artifact_root(config)
    cohort_root = artifact_root / "cohorts" / cohort_id
    json_path = cohort_root / str(config["storage"]["report_json_name"])
    markdown_path = cohort_root / str(config["storage"]["report_markdown_name"])
    material_path = cohort_root / str(config["storage"]["material_contract_name"])
    receipt_path = cohort_root / str(config["storage"]["verifier_name"])
    immutable_text(json_path, json_text(report))
    immutable_text(markdown_path, render_markdown(report))
    immutable_text(material_path, json.dumps(material, indent=2, sort_keys=True) + "\n")

    completed = subprocess.run(
        [sys.executable, str(VERIFIER_PATH), "--config", str(config_path), "--report", str(json_path), "--output", str(receipt_path)],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise ValueError("independent mistake-curriculum verification failed: " + (completed.stdout + completed.stderr).strip())
    receipt = read_json(receipt_path)
    if receipt.get("verified") is not True or receipt.get("failures") != []:
        raise ValueError("independent mistake-curriculum verification failed closed")

    atomic_text(artifact_root / str(config["storage"]["current_state_name"]), json_text(report))
    atomic_text(artifact_root / str(config["storage"]["current_report_name"]), render_markdown(report))
    atomic_json(artifact_root / str(config["storage"]["current_verifier_name"]), receipt)
    return report, json_path, markdown_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args()
    report, json_path, markdown_path = run(config_path=args.config)
    print(f"{report['cohort_id']} {report['report_id']} observations={report['summary']['raw_observation_count']} clusters={report['summary']['structural_cluster_count']} verified=true")
    print(json_path)
    print(markdown_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
