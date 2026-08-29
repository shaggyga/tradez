import json
import sqlite3
import tempfile
import unittest
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from trad.oanda_multihorizon_panel_model import (
    design_matrix,
    fit_horizon_model,
    load_panel_dataset,
    project_currency_factor_moves,
    purged_partition_indices,
    run_audit,
)


def build_panel_database(path: Path, timestamps: int = 240) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE snapshots (
            snapshot_id TEXT PRIMARY KEY,
            instrument TEXT,
            origin_time TEXT,
            features_zlib BLOB
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE outcomes (
            row_id INTEGER PRIMARY KEY,
            snapshot_id TEXT,
            horizon_sec INTEGER,
            signed_move_pips REAL,
            long_net_pips REAL,
            short_net_pips REAL,
            UNIQUE(snapshot_id, horizon_sec)
        )
        """
    )
    connection.execute("CREATE INDEX idx_combo_horizon ON outcomes(horizon_sec, row_id)")
    pairs = ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_JPY")
    start = datetime(2026, 1, 5, tzinfo=timezone.utc)
    row_id = 0
    for minute in range(timestamps):
        origin = start + timedelta(minutes=minute)
        for pair_index, instrument in enumerate(pairs):
            row_id += 1
            # Stable, learnable relation plus a small cross-currency component.
            momentum = 1.0 if (minute + pair_index) % 6 < 3 else -1.0
            cross_strength = ((minute % 9) - 4) / 4.0
            signal = 1.8 * momentum + 0.7 * cross_strength
            long_net = signal - 0.15
            short_net = -signal - 0.15
            features = {
                "return_m5_atr": momentum,
                "cross_strength_m1": cross_strength,
                "atr_m1_pips": 1.0 + (minute % 5) * 0.1,
                "live_spread_atr": 0.15 / (1.0 + (minute % 5) * 0.1),
                "session_london": float(7 <= origin.hour < 16),
                "constant_feature": 1.0,
            }
            snapshot_id = f"s-{row_id}"
            connection.execute(
                "INSERT INTO snapshots VALUES (?, ?, ?, ?)",
                (
                    snapshot_id,
                    instrument,
                    origin.isoformat().replace("+00:00", "Z"),
                    zlib.compress(json.dumps(features).encode("utf-8")),
                ),
            )
            connection.execute(
                "INSERT INTO outcomes VALUES (?, ?, 300, ?, ?, ?)",
                (row_id, snapshot_id, signal, long_net, short_net),
            )
    connection.commit()
    connection.close()


class MultiHorizonPanelModelTests(unittest.TestCase):
    def test_currency_factor_projection_enforces_cross_pair_consistency(self):
        moves = project_currency_factor_moves(
            np.asarray(["EUR_USD", "USD_JPY", "EUR_JPY"], dtype=object),
            np.asarray([1000.0, 1000.0, 1000.0]),
            np.asarray([1.0, 1.0, 10.0]),
        )
        self.assertAlmostEqual(moves[2], moves[0] + moves[1], places=6)
        self.assertLess(np.sum((moves - np.asarray([1.0, 1.0, 10.0])) ** 2), 30.0)

    def test_partition_is_timestamp_aligned_and_purges_full_horizon(self):
        epochs = np.repeat(np.arange(100, dtype=float) * 60.0, 3)
        train, selection, holdout, audit = purged_partition_indices(epochs, 300)
        self.assertLess(epochs[train].max() + 300, epochs[selection].min() + 1e-9)
        self.assertLess(epochs[selection].max() + 300, epochs[holdout].min() + 1e-9)
        self.assertGreater(audit["purged_rows"], 0)
        self.assertEqual(len(set(epochs[train]).intersection(epochs[selection])), 0)

    def test_direct_panel_model_is_profitable_on_untouched_synthetic_holdout(self):
        with tempfile.TemporaryDirectory() as folder:
            database = Path(folder) / "panel.sqlite"
            build_panel_database(database)
            dataset = load_panel_dataset(database, 300, max_rows=5_000)
            model = fit_horizon_model(dataset, 300)
            matrix, names, audit = design_matrix(
                dataset,
                purged_partition_indices(dataset.epochs, 300)[0],
            )
        self.assertEqual(dataset.rows, 960)
        self.assertEqual(matrix.shape[0], 960)
        self.assertIn("currency_exposure_USD", names)
        self.assertIn("cyclical_time_context", audit["feature_domains"])
        self.assertGreater(model["untouched_holdout"]["average_net_pips"], 1.0)
        self.assertGreater(model["untouched_holdout"]["win_rate"], 0.90)
        self.assertEqual(
            model["untouched_holdout_by_cost_bucket"]["liquid_le_3_pips"][
                "population_rows"
            ],
            model["partition"]["holdout_rows"],
        )
        self.assertLess(
            model["inverse_direction_holdout_diagnostic"]["average_net_pips"],
            0.0,
        )
        self.assertIn("never selected", model["inverse_direction_policy"])
        self.assertTrue(model["research_only"])
        self.assertTrue(model["shadow_only"])
        self.assertFalse(model["account_eligible"])
        self.assertFalse(model["execution_adapter"])
        self.assertIn(
            model["shadow_decision"],
            ("forward_observe_only", "no_trade_failed_purged_selection"),
        )

    def test_audit_writes_json_and_markdown_without_execution_path(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            database = root / "panel.sqlite"
            output_json = root / "audit.json"
            output_markdown = root / "audit.md"
            build_panel_database(database)
            payload = run_audit(
                database,
                output_json,
                output_markdown,
                horizons=("M5",),
                max_rows=5_000,
            )
            saved = json.loads(output_json.read_text(encoding="utf-8"))
            report = output_markdown.read_text(encoding="utf-8")
        self.assertEqual(sorted(payload["horizons"]), ["M5"])
        self.assertFalse(saved["account_eligible"])
        self.assertIn("research-only", report)
        self.assertIn("M5", report)


if __name__ == "__main__":
    unittest.main()
