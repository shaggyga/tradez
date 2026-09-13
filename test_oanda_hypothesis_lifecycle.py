import tempfile
import unittest
import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import trad.oanda_hypothesis_lifecycle as lifecycle_module

from trad.oanda_hypothesis_lifecycle import (
    current_state,
    initialize_database,
    ingest_current_cells,
    insert_hypothesis,
    register_reconsideration,
    retire,
    transition,
)


CONFIG = {
    "lifecycle": {
        "permitted_reconsideration_material_changes": ["new_information_source"],
        "insufficient_reconsideration_changes": ["rename_only"],
    }
}


class HypothesisLifecycleTests(unittest.TestCase):
    def test_run_lifecycle_finishes_read_only_velocity_before_database_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            config.write_text(
                json.dumps({"phase": "test", "lifecycle": {}}),
                encoding="utf-8",
            )
            events = []

            def velocity(**_kwargs):
                events.append("velocity")
                return []

            def bootstrap(*_args, **_kwargs):
                events.append("bootstrap")
                return {"status": "already_initialized"}

            def ingest(*_args, **_kwargs):
                events.append("ingest")
                return {"status": "ok"}

            def summary(*_args, **_kwargs):
                events.append("summary")
                return {
                    "hypothesis_count": 0,
                    "states": {},
                    "permanent_futility_retirement_count": 0,
                    "accepted_reconsideration_count": 0,
                    "rejected_reconsideration_count": 0,
                }

            def synchronize(_database_path):
                events.append("sync")
                return {"ok": True, "status": "synchronized"}

            with (
                patch.object(lifecycle_module, "proof_cohort_velocity", velocity),
                patch.object(lifecycle_module, "bootstrap_fixed_snapshot", bootstrap),
                patch.object(lifecycle_module, "ingest_current_cells", ingest),
                patch.object(lifecycle_module, "lifecycle_summary", summary),
            ):
                payload = lifecycle_module.run_lifecycle(
                    database_path=root / "lifecycle.sqlite",
                    state_path=root / "state.json",
                    config_path=config,
                    edge_database=root / "edge.sqlite",
                    source_database=root / "outcomes.sqlite",
                    cohort_state_path=root / "cohort.json",
                    post_ingest_sync=synchronize,
                )

            self.assertEqual(
                events,
                ["velocity", "bootstrap", "ingest", "sync", "summary"],
            )
            self.assertEqual(payload["proof_cohort_evidence_velocity"], [])
            self.assertEqual(payload["genealogy_sync"]["status"], "synchronized")

    def test_run_lifecycle_fails_before_publication_when_post_ingest_sync_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            state = root / "state.json"
            config.write_text(
                json.dumps({"phase": "test", "lifecycle": {}}),
                encoding="utf-8",
            )
            with (
                patch.object(lifecycle_module, "proof_cohort_velocity", lambda **_kwargs: []),
                patch.object(
                    lifecycle_module,
                    "bootstrap_fixed_snapshot",
                    lambda *_args, **_kwargs: {"status": "already_initialized"},
                ),
                patch.object(
                    lifecycle_module,
                    "ingest_current_cells",
                    lambda *_args, **_kwargs: {"status": "ok"},
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "synchronization failed"):
                    lifecycle_module.run_lifecycle(
                        database_path=root / "lifecycle.sqlite",
                        state_path=state,
                        config_path=config,
                        edge_database=root / "edge.sqlite",
                        source_database=root / "outcomes.sqlite",
                        cohort_state_path=root / "cohort.json",
                        post_ingest_sync=lambda _path: {
                            "ok": False,
                            "status": "count_mismatch",
                        },
                    )
            self.assertFalse(state.exists())

    def test_current_cell_ingest_emits_bounded_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "lifecycle.sqlite")
            edge = root / "edge.sqlite"
            source = sqlite3.connect(edge)
            source.executescript(
                """
                CREATE TABLE current_evidence_state(
                    singleton INTEGER PRIMARY KEY, run_id TEXT,
                    generated_utc TEXT, source_highwater_row_id INTEGER,
                    cell_count INTEGER
                );
                CREATE TABLE current_cell_evidence(
                    cell_id TEXT PRIMARY KEY, evidence_json TEXT
                );
                """
            )
            cell = {
                "cell_id": "family|EUR_USD|3600|london|liquid",
                "family": "family",
                "instrument": "EUR_USD",
                "horizon_sec": 3600,
                "session": "london",
                "liquidity_bucket": "liquid",
                "graduation_stage": "A_contract_valid",
                "time_uniform_upper_bound_pips": 1.0,
                "minimum_economic_edge_pips": 0.5,
                "effective_n": 1,
            }
            source.execute(
                "INSERT INTO current_evidence_state VALUES (1,?,?,?,?)",
                ("run-1", "2026-08-24T19:00:00+00:00", 1, 1),
            )
            source.execute(
                "INSERT INTO current_cell_evidence VALUES (?,?)",
                (cell["cell_id"], json.dumps(cell, sort_keys=True)),
            )
            source.commit()
            source.close()
            events = []
            try:
                result = ingest_current_cells(
                    connection,
                    edge_database=edge,
                    config={"lifecycle": {}},
                    progress_callback=lambda step, details: events.append(
                        (step, dict(details))
                    ),
                )
            finally:
                connection.close()
            self.assertEqual(result["cell_count"], 1)
            self.assertEqual(events[0][0], "ingesting_current_cells")
            self.assertEqual(events[0][1]["processed_cells"], 0)
            self.assertEqual(events[-1][0], "current_cell_ingest_complete")
            self.assertEqual(events[-1][1]["processed_cells"], 1)

    def test_futility_retirement_is_permanent(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "lifecycle.sqlite")
            cell = {
                "cell_id": "family|EUR_USD|3600|london|liquid",
                "family": "family", "instrument": "EUR_USD", "horizon_sec": 3600,
                "session_bucket": "london", "liquidity_bucket": "liquid",
            }
            hypothesis = insert_hypothesis(
                connection, cell, evidence_contract_id="contract", observed_utc="2026-08-06T00:00:00+00:00"
            )
            self.assertTrue(
                retire(
                    connection, hypothesis_id=hypothesis,
                    observed_utc="2026-08-06T00:00:00+00:00", source_run_id="run",
                    method="time_uniform_upper_bound_below_minimum_economic_edge",
                    minimum_edge=0.5, upper_bound=0.1, evidence={},
                )
            )
            self.assertEqual(current_state(connection, hypothesis), "futility_rejected")
            self.assertFalse(
                transition(
                    connection, hypothesis_id=hypothesis, next_state="continue_collecting",
                    observed_utc="2026-08-07T00:00:00+00:00", source_run_id="later", evidence={},
                )
            )
            self.assertEqual(current_state(connection, hypothesis), "futility_rejected")
            connection.close()

    def test_reconsideration_requires_materially_new_hypothesis(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "lifecycle.sqlite")
            cell = {
                "cell_id": "family|EUR_USD|3600|london|liquid",
                "family": "family", "instrument": "EUR_USD", "horizon_sec": 3600,
                "session_bucket": "london", "liquidity_bucket": "liquid",
            }
            hypothesis = insert_hypothesis(
                connection, cell, evidence_contract_id="contract", observed_utc="2026-08-06T00:00:00+00:00"
            )
            retire(
                connection, hypothesis_id=hypothesis,
                observed_utc="2026-08-06T00:00:00+00:00", source_run_id="run",
                method="fixed", minimum_edge=0.5, upper_bound=0.1, evidence={},
            )
            renamed = register_reconsideration(
                connection, retired_hypothesis_id=hypothesis,
                proposed_hypothesis_id="renamed", material_change_class="rename_only", config=CONFIG,
            )
            new_source = register_reconsideration(
                connection, retired_hypothesis_id=hypothesis,
                proposed_hypothesis_id="new-source", material_change_class="new_information_source", config=CONFIG,
            )
            self.assertFalse(renamed["accepted"])
            self.assertTrue(new_source["accepted"])
            connection.close()


if __name__ == "__main__":
    unittest.main()
