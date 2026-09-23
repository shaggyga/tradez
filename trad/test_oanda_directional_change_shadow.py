import json
import sqlite3
import tempfile
import unittest
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from trad.oanda_directional_change_shadow import (
    DirectionalChangeDataset,
    arm_directions,
    directional_change_states,
    run_audit,
)


def synthetic_dataset(last_forward: float = 1.0) -> DirectionalChangeDataset:
    rows = 80
    moves = np.ones(rows, dtype=float)
    moves[-1] = last_forward
    return DirectionalChangeDataset(
        instruments=np.asarray(["EUR_USD"] * rows, dtype=object),
        epochs=np.arange(rows, dtype=float) * 60.0,
        forward_m1_pips=moves,
        long_net=np.ones(rows, dtype=float),
        short_net=-np.ones(rows, dtype=float),
        spread_pips=np.full(rows, 1.0, dtype=float),
    )


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
    for minute in range(240):
        for pair in ("EUR_USD", "GBP_USD"):
            snapshot_id = f"{minute}-{pair}"
            move = 1.0 if (minute // 20) % 2 == 0 else -1.0
            features = {
                "atr_m1_pips": 2.0,
                "live_spread_atr": 0.5,
            }
            connection.execute(
                "INSERT INTO snapshots VALUES(?, ?, ?, ?)",
                (
                    snapshot_id,
                    pair,
                    (start + timedelta(minutes=minute)).isoformat(),
                    zlib.compress(json.dumps(features).encode("utf-8")),
                ),
            )
            for horizon, scale in ((60, 1.0), (300, 4.0)):
                row_id += 1
                gross = move * scale
                connection.execute(
                    "INSERT INTO outcomes VALUES(?, ?, ?, ?, ?, ?)",
                    (row_id, snapshot_id, horizon, gross, gross - 1.0, -gross - 1.0),
                )
    connection.commit()
    connection.close()


class DirectionalChangeShadowTests(unittest.TestCase):
    def test_current_forward_move_cannot_change_current_state(self):
        base = directional_change_states(synthetic_dataset(1.0))
        shocked = directional_change_states(synthetic_dataset(-1000.0))
        self.assertEqual(base["mode"][-1], shocked["mode"][-1])
        self.assertEqual(
            base["event_age_minutes"][-1], shocked["event_age_minutes"][-1]
        )
        self.assertTrue(base["warm"][-1])

    def test_preregistered_arms_are_opposite_where_active(self):
        states = directional_change_states(synthetic_dataset())
        arms = arm_directions(states)
        active = arms["dc_regime_continuation"] != 0
        self.assertTrue(np.any(active))
        np.testing.assert_array_equal(
            arms["dc_regime_reversal"][active],
            -arms["dc_regime_continuation"][active],
        )

    def test_audit_is_shadow_only_and_writes_cost_buckets(self):
        with tempfile.TemporaryDirectory() as folder:
            database = Path(folder) / "audit.sqlite"
            output = Path(folder) / "dc.json"
            build_database(database)
            payload = run_audit(database, output, horizons=("M5",), max_rows=10_000)
            saved = json.loads(output.read_text(encoding="utf-8"))
        self.assertIn("M5", payload["horizons"])
        self.assertFalse(saved["account_eligible"])
        self.assertFalse(saved["execution_adapter"])
        self.assertFalse(saved["holdout_used_for_selection"])
        self.assertIn(
            "liquid_le_3_pips",
            saved["horizons"]["M5"]["selected_arm_holdout_by_cost_bucket"],
        )


if __name__ == "__main__":
    unittest.main()
