from __future__ import annotations

import importlib.util
import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "model_library_catalogue.py"
SPEC = importlib.util.spec_from_file_location("model_library_catalogue", MODULE_PATH)
assert SPEC and SPEC.loader
catalogue = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = catalogue
SPEC.loader.exec_module(catalogue)


class ModelLibraryCatalogueTests(unittest.TestCase):
    def test_nested_split_dates_are_catalogued(self) -> None:
        dates = catalogue.extract_dates(
            {
                "date_coverage": {
                    "validation": ["2025-11-01T00:00:00Z", "2026-01-01T00:00:00Z"],
                    "final_diagnostic": ["2026-01-01T00:00:00Z", "2026-07-07T00:00:00Z"],
                }
            }
        )
        self.assertEqual(len(dates), 4)
        self.assertIn("date_coverage.validation[0]", dates)
        self.assertIn("date_coverage.final_diagnostic[1]", dates)

    def test_later_duplicate_can_upgrade_reproducibility(self) -> None:
        base = {
            "model_name": "ARIMA",
            "variant": "arima_selected|H1",
            "final_pnl_usd": -10.0,
            "date_coverage": {"validation": ["2025-11-01", "2026-01-01"]},
        }
        improved = {
            **base,
            "reproducibility": {
                "command": "python run_pipeline.py mandatory-model-validation",
                "data_manifest_path": "reports/run/MANDATORY_MODEL_DATA_MANIFEST.json",
            },
        }
        existing = catalogue.extract_run(base, "reports/old.json", "/0", "artifact_" + "1" * 64)
        incoming = catalogue.extract_run(improved, "reports/new.json", "/0", "artifact_" + "2" * 64)
        assert existing and incoming
        self.assertEqual(existing["run_id"], incoming["run_id"])
        catalogue.merge_run(existing, incoming)
        self.assertEqual(
            existing["reproducibility"]["rerun_command"],
            "python run_pipeline.py mandatory-model-validation",
        )
        self.assertIn(
            "reports/run/MANDATORY_MODEL_DATA_MANIFEST.json",
            existing["reproducibility"]["declared_paths"],
        )
        self.assertTrue(existing["date_coverage"])

    def test_sensitive_values_are_redacted_without_losing_account_metrics(self) -> None:
        value = {
            "account_id": "secret-account",
            "token": "secret-token",
            "account_currency_p_l": 12.5,
            "nested": {"password": "secret-password"},
        }
        sanitized = catalogue.sanitize_json(value)
        self.assertEqual(sanitized["account_id"], "[REDACTED]")
        self.assertEqual(sanitized["token"], "[REDACTED]")
        self.assertEqual(sanitized["nested"]["password"], "[REDACTED]")
        self.assertEqual(sanitized["account_currency_p_l"], 12.5)
        self.assertEqual(catalogue.find_unredacted_sensitive_values(sanitized), [])

    def test_model_variant_and_application_are_stable(self) -> None:
        payload = {
            "model_family": "Extra Trees",
            "feature_family": "I_full",
            "label_family": "TP_before_SL_proxy",
            "horizon": "21m",
            "pair": "EUR_USD",
            "forecast_rmse_pips": 2.1,
            "direction_accuracy": 0.61,
            "pnl": 42.0,
            "trades": 30,
        }
        run_a = catalogue.extract_run(payload, "fresh_m1_intrahour/reports/example/report.json", "/candidate", "artifact_" + "a" * 64)
        run_b = catalogue.extract_run(payload, "fresh_m1_intrahour/reports/example/report.json", "/candidate", "artifact_" + "a" * 64)
        assert run_a and run_b
        self.assertEqual(run_a["run_id"], run_b["run_id"])
        self.assertEqual(run_a["variant_id"], run_b["variant_id"])
        self.assertEqual(run_a["application_id"], run_b["application_id"])
        self.assertEqual(run_a["family_id"], "extra_trees")
        self.assertEqual(run_a["asset_class"], "foreign_exchange")
        self.assertTrue(run_a["metrics"]["forecast"])
        self.assertTrue(run_a["metrics"]["trading"])

    def test_legacy_extreme_return_is_invalidated(self) -> None:
        payload = {
            "model_type": "random_forest",
            "strategy_id": "legacy_1",
            "return_pct": 1_000_000.0,
            "total_trades": 100,
        }
        run = catalogue.extract_run(payload, "data/oanda_training_manager/backtests/bad.json", "/", "artifact_" + "b" * 64)
        assert run
        self.assertEqual(run["validation_status"], "INVALIDATED")

    def test_run_id_does_not_become_family_but_report_context_can(self) -> None:
        payload = {"run_id": "sarima_full_tier1_20260710", "spec_id": "p2_q1", "status": "fit_complete"}
        run = catalogue.extract_run(
            payload,
            "fresh_m1_intrahour/reports/sarima_full/SARIMA_GRID_CHECKPOINT.json",
            "/specs/0",
            "artifact_" + "c" * 64,
        )
        assert run
        self.assertEqual(run["family_id"], "sarima")
        self.assertNotEqual(run["family_id"], "sarima_full_tier1_20260710")

    def test_report_container_is_not_mistaken_for_an_extra_model_run(self) -> None:
        container = {"results": [{"spec_id": "p1_q0", "validation_pnl": 4.0}]}
        self.assertFalse(
            catalogue.is_model_candidate(
                container,
                "fresh_m1_intrahour/reports/sarima_full/SARIMA_GRID_CHECKPOINT.json",
            )
        )
        self.assertTrue(
            catalogue.is_model_candidate(
                container["results"][0],
                "fresh_m1_intrahour/reports/sarima_full/SARIMA_GRID_CHECKPOINT.json",
            )
        )
        self.assertFalse(
            catalogue.is_model_candidate(
                {"pnl": 4.0, "trades": 12, "pair": "EUR_USD"},
                "fresh_m1_intrahour/reports/sarima_full/SARIMA_GRID_CHECKPOINT.json",
            )
        )

    def test_safety_gate_refuses_enabled_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            project = Path(temp)
            config = project / "fresh_m1_intrahour" / "config.json"
            config.parent.mkdir(parents=True)
            config.write_text(json.dumps({"execution": {"live_execution_enabled": True}}), encoding="utf-8")
            with self.assertRaises(RuntimeError):
                catalogue.assert_research_safety(project)

    def test_dependency_resolution_uses_artifact_paths(self) -> None:
        run = {
            "evidence": [{"artifact_id": "artifact_evidence"}],
            "date_coverage": {"start": "2026-01-01"},
            "reproducibility": {
                "declared_paths": ["data/example.csv"],
                "rerun_command": "python research.py",
            },
        }
        artifacts = [
            {"artifact_id": "artifact_dataset", "paths": ["data/example.csv"]},
            {"artifact_id": "artifact_evidence", "paths": ["reports/run.json"]},
        ]
        catalogue.resolve_run_dependencies([run], artifacts, Path("C:/project"))
        self.assertEqual(
            run["reproducibility"]["grade"],
            "B_reconstructable_not_revalidated",
        )
        self.assertIn("artifact_dataset", run["reproducibility"]["resolved_artifact_ids"])

    def test_full_evidence_snapshots_are_sharded(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            writer = catalogue.EvidenceSnapshotWriter(root, "full", shard_size=2)
            first = writer.add("artifact_a", {"model_type": "ridge", "rmse": 1.2})
            second = writer.add("artifact_b", {"model_type": "arima", "rmse": 1.1})
            hashes = writer.close()
            self.assertEqual(first["snapshot_format"], "gzip_jsonl_shard")
            self.assertEqual(first["sanitized_snapshot_path"], second["sanitized_snapshot_path"])
            self.assertIn(first["sanitized_snapshot_path"], hashes)
            with gzip.open(root / first["sanitized_snapshot_path"], "rt", encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle]
            self.assertEqual([row["artifact_id"] for row in rows], ["artifact_a", "artifact_b"])

    def test_bulk_runtime_events_are_collections_but_model_manifests_are_exact_evidence(self) -> None:
        movement = "data/forex/logging/raw_event_json/20260630_120000_movement_detected_abc123def456.json"
        manifest = "data/archive/partial_raw_event_json_from_migration_20260630_154455/20260630_120000_promoted_model_manifest_abc123def456.json"
        self.assertEqual(catalogue.bulk_event_scope(movement), "data/forex/logging")
        self.assertFalse(catalogue.bulk_model_evidence_hint(movement))
        self.assertTrue(catalogue.bulk_model_evidence_hint(manifest))

    def test_ensemble_members_are_first_class_variant_data(self) -> None:
        payload = {
            "model_type": "ensemble",
            "experiment_id": "ens-1",
            "members": ["m30_model", "h1_model", "h4_model"],
            "weights": [0.2, 0.3, 0.5],
            "pair": "EUR_USD",
            "forecast_rmse": 1.4,
        }
        run = catalogue.extract_run(payload, "data/reports/ensemble.json", "/", "artifact_" + "d" * 64)
        assert run
        self.assertTrue(run["ensemble"]["is_ensemble"])
        self.assertEqual(run["ensemble"]["member_count"], 3)
        self.assertEqual(run["specification"]["members"], payload["members"])
        self.assertEqual(run["specification"]["weights"], payload["weights"])

    def test_member_bearing_fixed_name_is_an_ensemble_variant(self) -> None:
        payload = {
            "model": "fixed_s001_s014_s061_week05",
            "members": ["s001", "s014", "s061"],
            "return_pct": 2.0,
        }
        run = catalogue.extract_run(payload, "data/reports/fixed.json", "/", "artifact_" + "f" * 64)
        assert run
        self.assertEqual(run["family_id"], "ensemble_meta")
        self.assertEqual(run["ensemble"]["member_count"], 3)

    def test_hgb_alias_and_estimator_source_mapping(self) -> None:
        payload = {"model_type": "archived_hgb_227_feature", "experiment_id": "hgb-1", "rmse": 1.0}
        run = catalogue.extract_run(payload, "reports/htf.json", "/", "artifact_" + "1" * 64)
        assert run
        self.assertEqual(run["family_id"], "hist_gradient_boosting")
        source = {
            "source_module_id": "src_hgb",
            "artifact_id": "artifact_source",
            "path": "research_hgb.py",
            "detected_estimators": ["HistGradientBoostingRegressor"],
            "public_functions": [],
            "classes": [],
            "ast_parse_error": None,
            "snapshot_path": "snapshots/source/research_hgb.py",
            "execution_source_excluded_from_snapshot": False,
        }
        families, _, _, _ = catalogue.build_entities([run], [source])
        self.assertEqual(families[0]["source_module_ids"], ["src_hgb"])

    def test_json_evidence_accepts_utf8_bom(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "report.json"
            source.write_bytes(b"\xef\xbb\xbf" + json.dumps({"model_type": "ridge", "rmse": 1.0}).encode("utf-8"))
            writer = catalogue.EvidenceSnapshotWriter(root / "build", "smoke")
            evidence, runs = catalogue.parse_json_evidence(
                source,
                "reports/report.json",
                "artifact_" + "e" * 64,
                writer,
                catalogue.Counter(),
            )
            writer.close()
            self.assertEqual(evidence["parse_status"], "ok")
            self.assertEqual(len(runs), 1)


if __name__ == "__main__":
    unittest.main()
