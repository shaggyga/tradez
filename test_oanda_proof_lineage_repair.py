import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from trad.oanda_proof_cohort_registry import ensure_cohorts
from trad.oanda_proof_lineage_repair import (
    GENERATION_ID,
    build_current_frozen_specifications,
    execute_lineage_repair,
)
from trad.oanda_proof_shadow_predictors import FAMILIES


def specifications(
    contract_name: str,
    start_utc: str,
    *,
    generation_id: str | None = None,
) -> list[dict]:
    rows = []
    for family in FAMILIES:
        row = {
            "family": family,
            "source_sha256": "sha256:frozen-source",
            "model_specification": {"model": contract_name},
            "hyperparameters": {"alpha": 1.0},
            "feature_schema_version": "sha256:frozen-feature-schema",
            "training_policy": {"mode": "causal_test"},
            "forecast_contract_version": "proof_forecast_contract_v2_cohort_bound",
            "cost_model_version": "cost_model_test_v1",
            "dimensions": {"horizon_sec": 3600},
            "deduplication_rules": {"episode": "signed_currency_factor"},
            "promotion_thresholds": {"minimum_net_edge_pips": 1.0},
            "initial_training_cutoff_utc": start_utc,
            "initial_training_dataset_sha256": "sha256:frozen-training-data",
            "pre_governance_forecast_count": 0,
            "cohort_start_utc": start_utc,
        }
        if generation_id is not None:
            row["cohort_generation_id"] = generation_id
        rows.append(row)
    return rows


def create_forecast_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE canonical_forecasts (
                family TEXT NOT NULL,
                entry_time TEXT NOT NULL,
                forecast_json TEXT NOT NULL
            )
            """
        )
        connection.commit()
    finally:
        connection.close()


def table_counts(path: Path) -> tuple[int, int]:
    connection = sqlite3.connect(path)
    try:
        cohorts = connection.execute("SELECT COUNT(*) FROM proof_cohorts").fetchone()[0]
        transitions = connection.execute(
            "SELECT COUNT(*) FROM proof_cohort_transitions"
        ).fetchone()[0]
        return int(cohorts), int(transitions)
    finally:
        connection.close()


def forecast_count(path: Path) -> int:
    connection = sqlite3.connect(path)
    try:
        return int(connection.execute("SELECT COUNT(*) FROM canonical_forecasts").fetchone()[0])
    finally:
        connection.close()


class ProofLineageRepairTests(unittest.TestCase):
    def test_spec_builder_uses_current_frozen_generation_without_forecasts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = root / "features.json"
            governance = root / "governance.json"
            forecasts = root / "forecasts.sqlite"
            create_forecast_database(forecasts)
            snapshot.write_text(
                json.dumps(
                    {
                        "generated_utc": "2026-08-29T12:00:00+00:00",
                        "instruments": {
                            "EUR_USD": {
                                "series": {
                                    "M1": [1.1 + index * 0.00001 for index in range(200)]
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            governance.write_text("{}", encoding="utf-8")

            before = forecast_count(forecasts)
            rows = build_current_frozen_specifications(
                snapshot_path=snapshot,
                governance_path=governance,
                forecast_database=forecasts,
                repair_started_utc="2026-08-29T12:05:00+00:00",
            )

            self.assertEqual({row["family"] for row in rows}, set(FAMILIES))
            self.assertTrue(
                all(row["cohort_generation_id"] == GENERATION_ID for row in rows)
            )
            self.assertTrue(
                all(row["cohort_start_utc"] == "2026-08-29T12:05:00+00:00" for row in rows)
            )
            self.assertEqual(forecast_count(forecasts), before)

    def test_spec_builder_can_open_registry_only_generation_on_weekend_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = root / "features.json"
            governance = root / "governance.json"
            forecasts = root / "forecasts.sqlite"
            create_forecast_database(forecasts)
            snapshot.write_text(
                json.dumps({"generated_utc": "2026-08-29T12:00:00+00:00", "instruments": {}}),
                encoding="utf-8",
            )
            governance.write_text("{}", encoding="utf-8")
            rows = build_current_frozen_specifications(
                snapshot_path=snapshot,
                governance_path=governance,
                forecast_database=forecasts,
                repair_started_utc="2026-08-29T12:05:00+00:00",
            )
            self.assertEqual(len(rows), len(FAMILIES))
            self.assertTrue(
                all(
                    row["initial_training_dataset_sha256"]
                    == "sha256:pending_first_prospective_forecast_after_lineage_repair"
                    for row in rows
                )
            )
            self.assertEqual(forecast_count(forecasts), 0)

    def test_dry_run_then_apply_is_one_time_safe_and_preserves_legacy_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "proof_registry.sqlite"
            registry_state = root / "proof_registry.json"
            forecasts = root / "forecasts.sqlite"
            audit_json = root / "lineage_repair.json"
            audit_markdown = root / "lineage_repair.md"
            create_forecast_database(forecasts)

            first = ensure_cohorts(
                registry,
                registry_state,
                specifications("current_frozen", "2026-08-06T00:00:00+00:00"),
            )
            second = ensure_cohorts(
                registry,
                registry_state,
                specifications("intervening", "2026-08-10T00:00:00+00:00"),
            )
            repair_specs = specifications(
                "current_frozen",
                "2026-08-29T12:05:00+00:00",
                generation_id=GENERATION_ID,
            )

            connection = sqlite3.connect(forecasts)
            try:
                for family in FAMILIES:
                    rows = [
                        (
                            family,
                            "2026-08-09T23:59:00+00:00",
                            json.dumps({"cohort_id": first[family]}),
                        ),
                        (
                            family,
                            "2026-08-10T00:01:00+00:00",
                            json.dumps({"cohort_id": first[family]}),
                        ),
                        (
                            family,
                            "2026-08-11T00:01:00+00:00",
                            json.dumps({"cohort_id": first[family]}),
                        ),
                        (
                            family,
                            "2026-08-10T00:02:00+00:00",
                            json.dumps({"cohort_id": second[family]}),
                        ),
                    ]
                    connection.executemany(
                        "INSERT INTO canonical_forecasts VALUES (?,?,?)", rows
                    )
                connection.commit()
            finally:
                connection.close()

            original_forecast_count = forecast_count(forecasts)
            original_registry_counts = table_counts(registry)
            planned = execute_lineage_repair(
                registry_database=registry,
                registry_state=registry_state,
                forecast_database=forecasts,
                audit_json=audit_json,
                audit_markdown=audit_markdown,
                specifications=repair_specs,
                repair_started_utc="2026-08-29T12:05:00+00:00",
                apply=False,
            )

            self.assertEqual(planned["command_status"], "planned_not_applied")
            self.assertEqual(planned["plan"]["excluded_forecast_count"], 8)
            self.assertEqual(planned["plan"]["forecasts_produced"], 0)
            self.assertFalse(planned["plan"]["forecast_ledger_mutated"])
            self.assertEqual(table_counts(registry), original_registry_counts)
            self.assertEqual(forecast_count(forecasts), original_forecast_count)
            self.assertFalse(audit_json.exists())
            self.assertFalse(audit_markdown.exists())

            with self.assertRaisesRegex(RuntimeError, "confirmation-token"):
                execute_lineage_repair(
                    registry_database=registry,
                    registry_state=registry_state,
                    forecast_database=forecasts,
                    audit_json=audit_json,
                    audit_markdown=audit_markdown,
                    specifications=repair_specs,
                    repair_started_utc="2026-08-29T12:05:00+00:00",
                    apply=True,
                    confirmation_token="wrong",
                )
            self.assertEqual(table_counts(registry), original_registry_counts)
            self.assertEqual(forecast_count(forecasts), original_forecast_count)

            applied = execute_lineage_repair(
                registry_database=registry,
                registry_state=registry_state,
                forecast_database=forecasts,
                audit_json=audit_json,
                audit_markdown=audit_markdown,
                specifications=repair_specs,
                repair_started_utc="2026-08-29T12:05:00+00:00",
                apply=True,
                confirmation_token=GENERATION_ID,
            )

            self.assertEqual(applied["command_status"], "applied")
            self.assertEqual(table_counts(registry), (12, 12))
            self.assertEqual(forecast_count(forecasts), original_forecast_count)
            self.assertEqual(applied["audit"]["excluded_forecast_count"], 8)
            self.assertEqual(applied["audit"]["forecasts_produced"], 0)
            self.assertEqual(applied["audit"]["forecast_writes_by_command"], 0)
            self.assertFalse(applied["audit"]["forecast_ledger_mutated"])
            self.assertTrue(applied["audit"]["quarantine_policy"]["legacy_rows_preserved"])
            state = json.loads(registry_state.read_text(encoding="utf-8"))
            active_ids = set(state["active_cohorts"].values())
            active_contracts = [
                row for row in state["cohorts"] if row["cohort_id"] in active_ids
            ]
            self.assertEqual(len(active_contracts), len(FAMILIES))
            self.assertTrue(
                all(row["cohort_generation_id"] == GENERATION_ID for row in active_contracts)
            )
            self.assertIn("excluded", audit_markdown.read_text(encoding="utf-8").lower())

            audit_before = audit_json.read_bytes()
            markdown_before = audit_markdown.read_bytes()
            audit_hash_before = hashlib.sha256(audit_before).hexdigest()
            repeated = execute_lineage_repair(
                registry_database=registry,
                registry_state=registry_state,
                forecast_database=forecasts,
                audit_json=audit_json,
                audit_markdown=audit_markdown,
                specifications=repair_specs,
                repair_started_utc="2026-08-29T12:05:00+00:00",
                apply=True,
                confirmation_token=GENERATION_ID,
            )

            self.assertEqual(repeated["command_status"], "already_applied")
            self.assertFalse(repeated["writes_performed"])
            self.assertEqual(table_counts(registry), (12, 12))
            self.assertEqual(forecast_count(forecasts), original_forecast_count)
            self.assertEqual(hashlib.sha256(audit_json.read_bytes()).hexdigest(), audit_hash_before)
            self.assertEqual(audit_json.read_bytes(), audit_before)
            self.assertEqual(audit_markdown.read_bytes(), markdown_before)


if __name__ == "__main__":
    unittest.main()
