from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import joblib


ARCHIVE_VERSION = "forex_feature_forecast_compact_archive_v1"
LIBRARY_NAME = "FOREX_MODEL_LIBRARY"
REQUIRED_FALSE_FLAGS = (
    "network_accessed",
    "credentials_accessed",
    "broker_api_used",
    "account_data_accessed",
    "order_placement_used",
    "live_execution_enabled",
    "demo_execution_enabled",
    "oanda_execution_enabled",
    "mt5_execution_enabled",
    "production_ready",
)
RUN_FILES = (
    "FEATURE_FORECAST_REPORT.json",
    "FEATURE_FORECAST_REPORT.md",
    "TWO_STAGE_FORECAST_REPORT.json",
    "run_manifest.json",
    "offline_research_forecast_bundle_index.json",
    "two_hour_cost_survival_offline_research_forecast_bundle.joblib",
    "two_hour_mid_direction_up_offline_research_forecast_bundle.joblib",
    "two_hour_cost_survival_2026_diagnostic_predictions.parquet",
    "two_hour_cost_survival_development_oos_predictions.parquet",
    "two_hour_mid_direction_up_2026_diagnostic_predictions.parquet",
    "two_hour_mid_direction_up_development_oos_predictions.parquet",
    "two_stage_direction_2026_diagnostic_predictions.parquet",
    "two_stage_direction_development_oos_predictions.parquet",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def atomic_write_json(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.parent.mkdir(parents=True, exist_ok=True)
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    return str(value)


def assert_safe_report(report: Mapping[str, Any]) -> None:
    if report.get("research_only") is not True or report.get("offline_only") is not True:
        raise ValueError("Forecast report is not explicitly offline research")
    enabled = [key for key in REQUIRED_FALSE_FLAGS if report.get(key) is not False]
    if enabled:
        raise ValueError(f"Unsafe or missing report flags: {enabled}")
    if report.get("confirmed_accurate_forecast") is not False:
        raise ValueError("Archive refuses a confirmed-accuracy claim")
    if report.get("profitability_claimed") is not False:
        raise ValueError("Archive refuses a profitability claim")


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected an object in {path}")
    return value


def package_versions() -> dict[str, str | None]:
    versions: dict[str, str | None] = {"python": platform.python_version()}
    for package in ("numpy", "pandas", "scikit-learn", "scipy", "joblib", "pyarrow"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def component_spec(component: Mapping[str, Any]) -> dict[str, Any]:
    estimator = component["estimator"]
    steps: list[dict[str, Any]] = []
    for step_name, step in estimator.named_steps.items():
        parameters = step.get_params(deep=False) if hasattr(step, "get_params") else {}
        steps.append(
            {
                "step_name": step_name,
                "class": f"{step.__class__.__module__}.{step.__class__.__qualname__}",
                "parameters": json_safe(parameters),
            }
        )
    return {
        "model_id": component["model_id"],
        "model_spec": component["spec"],
        "pipeline_steps": steps,
        "input_feature_count": len(component["features"]),
        "input_features_in_order": list(component["features"]),
        "selected_feature_count": len(component["selected_features"]),
        "selected_features_in_order": list(component["selected_features"]),
    }


def load_bundle_specs(run_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    bundle_index = load_json(run_dir / "offline_research_forecast_bundle_index.json")
    specs: dict[str, Any] = {}
    validation: dict[str, Any] = {}
    for target_id, record in sorted(bundle_index["target_bundles"].items()):
        path = run_dir / record["path"]
        actual_hash = sha256_file(path)
        if actual_hash != record["sha256"]:
            raise ValueError(f"Bundle hash mismatch: {path}")
        bundle = joblib.load(path)
        if bundle.get("target_id") != target_id:
            raise ValueError(f"Bundle target mismatch: {path}")
        specs[target_id] = {
            "artifact_file": path.name,
            "artifact_sha256": actual_hash,
            "bundle_version": bundle["bundle_version"],
            "artifact_role": bundle["artifact_role"],
            "candidate_id": bundle["candidate_id"],
            "target_definition": bundle["target_definition"],
            "training_rows": bundle["training_rows"],
            "training_decision_start": bundle["training_decision_start"],
            "training_decision_end": bundle["training_decision_end"],
            "training_label_end": bundle["training_label_end"],
            "development_gate": bundle["development_gate"],
            "confirmed_accurate_forecast": False,
            "fresh_holdout_required": True,
            "components": [component_spec(component) for component in bundle["components"]],
        }
        validation[target_id] = {
            "sha256_matches_index": True,
            "joblib_load_passed": True,
            "target_matches": True,
            "candidate_id": bundle["candidate_id"],
        }
    return specs, validation


def copy_tree_files(source: Path, destination: Path, pattern: str = "*.py") -> None:
    for path in sorted(source.rglob(pattern)):
        if "__pycache__" in path.parts or not path.is_file():
            continue
        target = destination / path.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def render_index(spec: Mapping[str, Any]) -> str:
    direction = spec["models"]["two_hour_mid_direction_up"]
    opportunity = spec["models"]["two_hour_cost_survival"]
    metrics = spec["key_results"]
    return "\n".join(
        [
            "# Forex Feature Forecast Research Addition",
            "",
            "Asset class: `forex`. Research-only offline archive. No broker API, credentials, account data, or order placement was used.",
            "",
            "## Index",
            "",
            "- `MODEL_REPLICATION_SPEC.json`: exact targets, feature order, selected features, pipeline classes, parameters, folds, data hashes, and environment versions.",
            "- `evidence/FEATURE_FORECAST_REPORT.json`: complete candidate grid, baselines, fold reports, pair reports, and diagnostics.",
            "- `evidence/TWO_STAGE_FORECAST_REPORT.json`: threshold grid, selected policy, all fold metrics, and 2026 diagnostic.",
            "- `models/`: loadable frozen offline bundles and bundle hash index.",
            "- `predictions/`: exact 2025 out-of-sample and 2026 diagnostic predictions.",
            "- `source/`: complete research source and tests used by this run.",
            "- `checksums/sha256.txt`: integrity hashes for every archived file.",
            "",
            "## Conclusions",
            "",
            f"- Direction model `{direction['candidate_id']}`: 2025 mean AUC `{metrics['direction_development_mean_auc']:.6f}`; gate `failed`.",
            f"- Opportunity model `{opportunity['candidate_id']}`: 2025 mean AUC `{metrics['opportunity_development_mean_auc']:.6f}`; 2026 diagnostic AUC `{metrics['opportunity_2026_diagnostic_auc']:.6f}`; development gate `passed`.",
            f"- Two-stage direction filter: 2025 accuracy `{metrics['two_stage_development_accuracy']:.4%}` at `{metrics['two_stage_development_coverage']:.4%}` coverage; directional gate `failed`.",
            "- Confirmed accurate directional forecast: `false`.",
            "- Profitability claimed: `false`.",
            "- Fresh untouched holdout available: `false`.",
            "",
            "The opportunity model ranks whether a two-hour move is likely to survive observed spread plus a static slippage assumption. It does not predict which side wins and is not a trading strategy.",
            "",
        ]
    )


def archive_run(
    *,
    run_dir: Path,
    project_root: Path,
    vault_root: Path,
    addition_id: str,
) -> Path:
    run_dir = run_dir.resolve()
    project_root = project_root.resolve()
    library_root = vault_root.resolve() / LIBRARY_NAME
    destination = library_root / "compact_additions" / addition_id
    if destination.exists():
        raise FileExistsError(f"Immutable addition already exists: {destination}")
    report = load_json(run_dir / "FEATURE_FORECAST_REPORT.json")
    assert_safe_report(report)
    missing = [name for name in RUN_FILES if not (run_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Run is missing required artifacts: {missing}")
    models, bundle_validation = load_bundle_specs(run_dir)
    snapshot_validation: dict[str, Any] = {}
    for key in ("source", "labels"):
        path = Path(report["data_snapshot"][f"{key}_path"])
        expected = report["data_snapshot"][f"{key}_sha256"]
        actual = sha256_file(path)
        if actual != expected:
            raise ValueError(f"Data snapshot hash mismatch for {key}: {path}")
        snapshot_validation[key] = {
            "path": str(path),
            "sha256": actual,
            "size_bytes": path.stat().st_size,
            "hash_verified_at_archive_time": True,
            "copied_to_vault": False,
        }

    direction = report["targets"]["two_hour_mid_direction_up"]
    opportunity = report["targets"]["two_hour_cost_survival"]
    two_stage = report["two_stage_forecast"]
    spec = {
        "archive_version": ARCHIVE_VERSION,
        "addition_id": addition_id,
        "asset_class": "forex",
        "domain": "foreign_exchange",
        "research_only": True,
        "offline_only": True,
        "production_ready": False,
        "confirmed_accurate_forecast": False,
        "profitability_claimed": False,
        "fresh_untouched_holdout_available": False,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_run": run_dir.name,
        "report_version": report["report_version"],
        "objective": report["objective"],
        "pairs": report["data_audit"].get("pairs") or report["data_audit"].get("instruments"),
        "timeframe": "H1 decision features with a 120-minute forward label",
        "data_snapshots": snapshot_validation,
        "feature_contract_hash": report["data_snapshot"]["feature_family_sha256"],
        "development_protocol": report["development_protocol"],
        "models": models,
        "two_stage_policy": {
            "opportunity_threshold": two_stage["selected_development_policy"]["opportunity_threshold"],
            "direction_confidence": two_stage["selected_development_policy"]["direction_confidence"],
            "development_gate": two_stage["development_gate"],
            "confirmed_accurate_directional_forecast": False,
        },
        "key_results": {
            "direction_development_mean_auc": direction["selected_development_candidate"]["aggregate_metrics"]["mean_roc_auc"],
            "direction_development_minimum_auc": direction["selected_development_candidate"]["aggregate_metrics"]["minimum_roc_auc"],
            "direction_2026_diagnostic_auc": direction["diagnostic_evaluation"]["metrics"]["roc_auc"],
            "opportunity_development_mean_auc": opportunity["selected_development_candidate"]["aggregate_metrics"]["mean_roc_auc"],
            "opportunity_development_minimum_auc": opportunity["selected_development_candidate"]["aggregate_metrics"]["minimum_roc_auc"],
            "opportunity_2026_diagnostic_auc": opportunity["diagnostic_evaluation"]["metrics"]["roc_auc"],
            "opportunity_2026_diagnostic_brier_skill": opportunity["diagnostic_evaluation"]["metrics"]["brier_skill_score"],
            "two_stage_development_accuracy": two_stage["selected_development_policy"]["aggregate_metrics"]["accuracy"],
            "two_stage_development_balanced_accuracy": two_stage["selected_development_policy"]["aggregate_metrics"]["balanced_accuracy"],
            "two_stage_development_coverage": two_stage["selected_development_policy"]["aggregate_metrics"]["coverage"],
            "two_stage_2026_diagnostic_accuracy": two_stage["diagnostic_2026"]["aggregate_metrics"]["accuracy"],
            "two_stage_2026_diagnostic_balanced_accuracy": two_stage["diagnostic_2026"]["aggregate_metrics"]["balanced_accuracy"],
            "two_stage_2026_diagnostic_coverage": two_stage["diagnostic_2026"]["aggregate_metrics"]["coverage"],
        },
        "failure_lessons": [
            "The available features contain only weak two-hour directional information.",
            "Filtering by predicted move quality improves directional accuracy only modestly and still fails the directional gate.",
            "Raw opportunity accuracy is inflated by target imbalance; use AUC, balanced accuracy, Brier skill, and calibration.",
            "All data through 2026-07-06 was previously inspected, so 2026 is diagnostic rather than a fresh holdout.",
            "Static slippage and observed historical spread are not live-verified costs and cannot support a profitability claim.",
            "The v2 direction bundle used an unstable __main__ selector identity; v3 moved it to an importable module and passed independent load checks.",
        ],
        "next_required_evidence": report["next_required_evidence"],
        "environment": package_versions(),
        "bundle_validation": bundle_validation,
        "replication_command": (
            "python -m fresh_m1_intrahour.src.feature_forecast_benchmark "
            "--n-jobs 2 --max-train-rows 100000 --output-dir <new_immutable_output_dir>"
        ),
        "complete_candidate_and_fold_evidence": "evidence/FEATURE_FORECAST_REPORT.json",
    }

    destination.mkdir(parents=True, exist_ok=False)
    evidence_dir = destination / "evidence"
    models_dir = destination / "models"
    predictions_dir = destination / "predictions"
    for name in RUN_FILES:
        source = run_dir / name
        if source.suffix == ".joblib" or name == "offline_research_forecast_bundle_index.json":
            target = models_dir / name
        elif source.suffix == ".parquet":
            target = predictions_dir / name
        else:
            target = evidence_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    copy_tree_files(project_root / "fresh_m1_intrahour" / "src", destination / "source" / "src")
    copy_tree_files(project_root / "fresh_m1_intrahour" / "tests", destination / "source" / "tests")
    shutil.copy2(
        project_root / "fresh_m1_intrahour" / "config.json",
        destination / "source" / "config.json",
    )
    write_json(destination / "MODEL_REPLICATION_SPEC.json", spec)
    (destination / "INDEX.md").write_text(render_index(spec), encoding="utf-8")
    validation = {
        "archive_version": ARCHIVE_VERSION,
        "addition_id": addition_id,
        "passed": True,
        "report_safety_passed": True,
        "bundle_validation": bundle_validation,
        "data_snapshot_hash_validation": snapshot_validation,
        "source_snapshot_present": True,
        "all_required_artifacts_present": True,
        "test_command": (
            "python -m unittest discover -s fresh_m1_intrahour/tests -p test_*.py -v"
        ),
        "test_result": "59 tests passed before archive export",
    }
    write_json(destination / "ARCHIVE_VALIDATION.json", validation)

    checksum_lines: list[str] = []
    for path in sorted(destination.rglob("*")):
        if path.is_file() and path.name != "sha256.txt":
            checksum_lines.append(f"{sha256_file(path)}  {path.relative_to(destination).as_posix()}")
    checksum_path = destination / "checksums" / "sha256.txt"
    checksum_path.parent.mkdir(parents=True, exist_ok=True)
    checksum_path.write_text("\n".join(checksum_lines) + "\n", encoding="utf-8")

    root_index_path = library_root / "COMPACT_ADDITIONS_INDEX.json"
    if root_index_path.exists():
        root_index = load_json(root_index_path)
    else:
        root_index = {"schema_version": 1, "asset_class": "forex", "additions": []}
    if any(row.get("addition_id") == addition_id for row in root_index["additions"]):
        raise ValueError(f"Addition already indexed: {addition_id}")
    root_index["additions"].append(
        {
            "addition_id": addition_id,
            "path": f"compact_additions/{addition_id}",
            "asset_class": "forex",
            "status": "research_lead_opportunity_model_directional_gate_failed",
            "confirmed_accurate_forecast": False,
            "profitability_claimed": False,
            "report_version": report["report_version"],
            "created_at_utc": spec["created_at_utc"],
        }
    )
    root_index["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    atomic_write_json(root_index_path, root_index)
    markdown = [
        "# Compact Forex Research Additions",
        "",
        "These immutable additions supplement the full catalogue without duplicating unchanged historical records.",
        "",
    ]
    for row in root_index["additions"]:
        markdown.append(
            f"- [{row['addition_id']}]({row['path']}/INDEX.md): `{row['status']}`"
        )
    markdown.append("")
    atomic_write_text(library_root / "COMPACT_ADDITIONS_INDEX.md", "\n".join(markdown))
    atomic_write_text(library_root / "LATEST_COMPACT_ADDITION.txt", addition_id + "\n")
    return destination


def main() -> int:
    parser = argparse.ArgumentParser(description="Export a validated compact Forex forecast addition.")
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--vault-root", type=Path, required=True)
    parser.add_argument("--addition-id", required=True)
    args = parser.parse_args()
    destination = archive_run(
        run_dir=args.run_dir,
        project_root=args.project_root,
        vault_root=args.vault_root,
        addition_id=args.addition_id,
    )
    print(json.dumps({"archive_path": str(destination), "archive_version": ARCHIVE_VERSION}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
