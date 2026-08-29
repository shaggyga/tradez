import sqlite3
import tempfile
import unittest
from pathlib import Path

from trad.oanda_model_predictor_auto_promotion import (
    PredictorPromotionStore,
    PredictorPromotionThresholds,
    build_state,
    verify_state_checksum,
)


def create_source(path: Path, *, blocks: int, rows_per_block: int, net_pips: float) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE predictions (
            candidate_id TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            artifact_sha256 TEXT NOT NULL,
            PRIMARY KEY(candidate_id, horizon_sec)
        );
        CREATE TABLE outcomes (
            candidate_id TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            model_id TEXT NOT NULL,
            instrument TEXT NOT NULL,
            input_timeframe TEXT NOT NULL,
            generated_epoch REAL NOT NULL,
            executable_net_pips REAL NOT NULL,
            executable_profitable INTEGER NOT NULL,
            direction_correct INTEGER NOT NULL,
            brier REAL NOT NULL,
            entry_spread_pips REAL NOT NULL,
            exit_spread_pips REAL NOT NULL,
            status TEXT NOT NULL,
            PRIMARY KEY(candidate_id, horizon_sec)
        );
        """
    )
    predictions = []
    outcomes = []
    base = 1_800_000_000.0
    for block in range(blocks):
        for sample in range(rows_per_block):
            candidate = f"candidate-{block}-{sample}"
            generated = base + block * 3600.0 + sample
            predictions.append((candidate, 900, "artifact-hash"))
            outcomes.append(
                (
                    candidate,
                    900,
                    "catboost",
                    "EUR_USD",
                    "M1",
                    generated,
                    net_pips,
                    int(net_pips > 0.0),
                    int(net_pips > 0.0),
                    0.20,
                    0.50,
                    0.50,
                    "matured",
                )
            )
    connection.executemany("INSERT INTO predictions VALUES (?, ?, ?)", predictions)
    connection.executemany(
        "INSERT INTO outcomes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        outcomes,
    )
    connection.commit()
    connection.close()


class ModelPredictorAutoPromotionTests(unittest.TestCase):
    def test_positive_independent_blocks_promote_exact_cell(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.sqlite"
            create_source(source, blocks=12, rows_per_block=10, net_pips=1.0)
            store = PredictorPromotionStore(source, root / "promotion.sqlite")
            ingest = store.ingest(chunk_rows=40, max_chunks=10)
            state = build_state(store, PredictorPromotionThresholds(), ingest)
            store.close()

        self.assertEqual(ingest["status"], "caught_up")
        self.assertEqual(state["eligible_cell_count"], 1)
        self.assertEqual(state["eligible_cells"][0]["instrument"], "EUR_USD")
        self.assertEqual(state["eligible_cells"][0]["horizon_sec"], 900)
        self.assertTrue(state["eligible_cells"][0]["eligible"])
        self.assertTrue(verify_state_checksum(state))
        self.assertFalse(state["real_account_authorized"])

    def test_dense_repetitions_in_one_block_do_not_promote(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.sqlite"
            create_source(source, blocks=1, rows_per_block=120, net_pips=2.0)
            store = PredictorPromotionStore(source, root / "promotion.sqlite")
            ingest = store.ingest(chunk_rows=200, max_chunks=1)
            state = build_state(store, PredictorPromotionThresholds(), ingest)
            store.close()

        self.assertEqual(state["eligible_cell_count"], 0)
        evidence = state["top_evidence"][0]
        self.assertEqual(evidence["sample_count"], 120)
        self.assertEqual(evidence["independent_blocks"], 1)
        self.assertIn("minimum_independent_blocks", evidence["blocked_by"])

    def test_positive_but_short_live_history_gets_practice_only_provisional_tier(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.sqlite"
            create_source(source, blocks=8, rows_per_block=30, net_pips=1.0)
            store = PredictorPromotionStore(source, root / "promotion.sqlite")
            ingest = store.ingest(chunk_rows=500, max_chunks=2)
            state = build_state(store, PredictorPromotionThresholds(), ingest)
            store.close()

        self.assertEqual(state["eligible_cell_count"], 1)
        self.assertEqual(state["strict_eligible_cell_count"], 0)
        self.assertEqual(state["provisional_eligible_cell_count"], 1)
        evidence = state["eligible_cells"][0]
        self.assertTrue(evidence["provisional_eligible"])
        self.assertEqual(
            evidence["promotion_tier"],
            "practice_provisional_temporal_breadth",
        )
        self.assertFalse(state["real_account_authorized"])


if __name__ == "__main__":
    unittest.main()
