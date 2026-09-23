import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

import oanda_movement_news_episode_research as research


class MovementNewsEpisodeTests(unittest.TestCase):
    def test_future_source_is_not_pre_entry(self):
        index = {
            "EUR": [
                {"source_event_id": "a", "effective_epoch": 900, "effective_from_utc": "1970-01-01T00:15:00+00:00", "valid_until_epoch": None, "superseded_epoch": None},
                {"source_event_id": "b", "effective_epoch": 1100, "effective_from_utc": "1970-01-01T00:18:20+00:00", "valid_until_epoch": None, "superseded_epoch": None},
            ]
        }
        rows = research.relevant_source_events(index, ["EUR"], 800, 1000, True)
        self.assertEqual([row["source_event_id"] for row in rows], ["a"])

    def test_superseded_source_is_not_entry_valid(self):
        index = {"EUR": [{"source_event_id": "a", "effective_epoch": 900, "effective_from_utc": "x", "valid_until_epoch": None, "superseded_epoch": 950}]}
        self.assertEqual(research.relevant_source_events(index, ["EUR"], 800, 1000, True), [])

    def test_nonoverlap_prefers_larger_episode(self):
        candidates = pd.DataFrame({"epoch": [0, 60, 600], "best_after_cost_pips": [5.0, 10.0, 4.0]})
        result = research.nonoverlapping_local_extremes(candidates, 5)
        self.assertEqual(list(result["epoch"]), [60, 600])

    def test_episode_table_is_immutable(self):
        with tempfile.TemporaryDirectory() as folder:
            db = research.open_database(Path(folder) / "episodes.sqlite")
            columns = [row[1] for row in db.execute("PRAGMA table_info(movement_episodes)")]
            values = {column: 0 for column in columns}
            for column in columns:
                if column.endswith("_id") or column.endswith("_utc") or column in {"instrument", "base_currency", "quote_currency", "selected_side", "signed_currency_factor", "mapping_json"}:
                    values[column] = column
            db.execute(f"INSERT INTO movement_episodes ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})", [values[column] for column in columns])
            with self.assertRaises(sqlite3.DatabaseError):
                db.execute("UPDATE movement_episodes SET instrument='X'")
            db.close()


if __name__ == "__main__":
    unittest.main()
