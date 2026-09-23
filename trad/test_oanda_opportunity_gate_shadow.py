import json
import sqlite3
import tempfile
import unittest
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from trad.oanda_multihorizon_panel_model import PanelDataset
from trad.oanda_opportunity_gate_shadow import opportunity_target, run_audit


def build_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE snapshots(
            snapshot_id TEXT PRIMARY KEY,
            instrument TEXT,
            origin_time TEXT,
            features_zlib BLOB
        );
        CREATE TABLE outcomes(
            row_id INTEGER PRIMARY KEY,
            snapshot_id TEXT,
            horizon_sec INTEGER,
            signed_move_pips REAL,
            long_net_pips REAL,
            short_net_pips REAL
        );
        CREATE INDEX idx_combo_horizon ON outcomes(horizon_sec, row_id);
        """
    )
    start = datetime(2026, 1, 5, tzinfo=timezone.utc)
    row_id = 0
    for minute in range(360):
        for pair_index, pair in enumerate(("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD")):
            high_opportunity = (minute + pair_index) % 5 < 2
            momentum = 1.0 if (minute + pair_index) % 4 < 2 else -1.0
            magnitude = 2.0 if high_opportunity else 0.1
            move = momentum * magnitude
            features = {
                "return_m5_atr": momentum,
                "atr_m1_pips": 1.0,
                "live_spread_atr": 0.5,
                "volume_ratio_12": 3.0 if high_opportunity else 0.5,
            }
            snapshot_id = f"{minute}-{pair}"
            connection.execute(
                "INSERT INTO snapshots VALUES(?, ?, ?, ?)",
                (
                    snapshot_id,
                    pair,
                    (start + timedelta(minutes=minute)).isoformat(),
                    zlib.compress(json.dumps(features).encode("utf-8")),
                ),
            )
            row_id += 1
            connection.execute(
                "INSERT INTO outcomes VALUES(?, ?, 300, ?, ?, ?)",
                (row_id, snapshot_id, move, move - 0.5, -move - 0.5),
            )
    connection.commit()
    connection.close()


class OpportunityGateShadowTests(unittest.TestCase):
    def test_target_marks_only_cost_beating_rows(self):
        dataset = PanelDataset(
            feature_names=[],
            raw_features=np.empty((2, 0)),
            instruments=np.asarray(["EUR_USD", "EUR_USD"], dtype=object),
            epochs=np.asarray([1.0, 2.0]),
            row_ids=np.asarray([1, 2]),
            long_net=np.asarray([1.0, -0.2]),
            short_net=np.asarray([-2.0, -0.3]),
            schema_missing_rate=0.0,
        )
        np.testing.assert_array_equal(opportunity_target(dataset), np.asarray([1, 0]))

    def test_audit_learns_opportunity_without_execution_path(self):
        with tempfile.TemporaryDirectory() as folder:
            database = Path(folder) / "audit.sqlite"
            output = Path(folder) / "opportunity.json"
            build_database(database)
            payload = run_audit(database, output, horizons=("M5",), max_rows=10_000)
            saved = json.loads(output.read_text(encoding="utf-8"))
        report = payload["horizons"]["M5"]
        self.assertGreater(
            report["holdout_opportunity_probability"]["roc_auc"],
            report["holdout_baseline_probability"]["roc_auc"],
        )
        self.assertFalse(saved["account_eligible"])
        self.assertFalse(saved["execution_adapter"])
        self.assertFalse(saved["holdout_used_for_selection"])
        self.assertIn("liquid_le_3_pips", report["untouched_holdout_by_cost_bucket"])


if __name__ == "__main__":
    unittest.main()
