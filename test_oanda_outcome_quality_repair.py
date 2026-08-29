import sqlite3
import tempfile
import unittest
from pathlib import Path

from trad.oanda_outcome_quality_repair import (
    audit_combination,
    audit_shadow,
    delete_matching_outcomes,
    quarantine_shadow,
)


START = "2026-07-17T20:48:00+00:00"


class OutcomeQualityRepairTests(unittest.TestCase):
    def test_shadow_audit_quarantine_and_downstream_delete(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "shadow.sqlite"
            connection = sqlite3.connect(source)
            connection.executescript(
                """
                CREATE TABLE outcomes (
                    row_id INTEGER PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    horizon_sec INTEGER NOT NULL,
                    observed_utc TEXT NOT NULL,
                    entry_time TEXT NOT NULL,
                    exit_time TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    family TEXT NOT NULL
                );
                CREATE INDEX shadow_outcomes_time ON outcomes(observed_utc);
                """
            )
            connection.executemany(
                "INSERT INTO outcomes VALUES (?, ?, 60, ?, ?, ?, 'signal', 'kalman')",
                [
                    (
                        1,
                        "invalid",
                        "2026-07-17T20:49:40+00:00",
                        "2026-07-17T20:48:00+00:00",
                        "2026-07-17T20:48:20+00:00",
                    ),
                    (
                        2,
                        "valid",
                        "2026-07-17T20:49:10+00:00",
                        "2026-07-17T20:48:00+00:00",
                        "2026-07-17T20:49:00+00:00",
                    ),
                ],
            )
            connection.commit()
            connection.close()

            self.assertEqual(audit_shadow(source, START, 30.0)["count"], 1)
            quarantine = root / "quarantine.sqlite"
            count = quarantine_shadow(
                source,
                quarantine,
                START,
                30.0,
                "2026-07-17T21:00:00+00:00",
            )

            downstream = root / "downstream.sqlite"
            connection = sqlite3.connect(downstream)
            connection.executescript(
                """
                CREATE TABLE outcomes (
                    row_id INTEGER PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    horizon_sec INTEGER NOT NULL,
                    UNIQUE(event_id, horizon_sec)
                );
                INSERT INTO outcomes VALUES (1, 'invalid', 60);
                INSERT INTO outcomes VALUES (2, 'valid', 60);
                """
            )
            connection.commit()
            connection.close()
            deleted = delete_matching_outcomes(downstream, quarantine)

            connection = sqlite3.connect(source)
            source_ids = [row[0] for row in connection.execute("SELECT event_id FROM outcomes")]
            connection.close()
            connection = sqlite3.connect(downstream)
            downstream_ids = [row[0] for row in connection.execute("SELECT event_id FROM outcomes")]
            connection.close()

        self.assertEqual(count, 1)
        self.assertEqual(deleted, 1)
        self.assertEqual(source_ids, ["valid"])
        self.assertEqual(downstream_ids, ["valid"])

    def test_combination_audit_uses_snapshot_target_time(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "combination.sqlite"
            connection = sqlite3.connect(path)
            connection.executescript(
                """
                CREATE TABLE snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    created_utc TEXT NOT NULL
                );
                CREATE TABLE outcomes (
                    row_id INTEGER PRIMARY KEY,
                    snapshot_id TEXT NOT NULL,
                    horizon_sec INTEGER NOT NULL,
                    outcome_time TEXT NOT NULL,
                    UNIQUE(snapshot_id, horizon_sec)
                );
                INSERT INTO snapshots VALUES (
                    '20260717T204800-example',
                    '2026-07-17T20:48:00+00:00'
                );
                INSERT INTO outcomes VALUES (
                    1,
                    '20260717T204800-example',
                    60,
                    '2026-07-17T20:48:20+00:00'
                );
                """
            )
            connection.commit()
            connection.close()

            count = audit_combination(path, START, 30.0)

        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
