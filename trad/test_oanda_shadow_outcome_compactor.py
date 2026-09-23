import sqlite3
import tempfile
import unittest
from pathlib import Path

from trad.oanda_shadow_outcome_compactor import (
    ShadowOutcomeCompactor,
    pa,
    pq,
)
from trad.oanda_shadow_outcome_store import ShadowOutcomeStore


@unittest.skipIf(pa is None or pq is None, "pyarrow is not installed")
class ShadowOutcomeCompactorTests(unittest.TestCase):
    def test_rolls_up_archives_then_deletes_old_detail(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "strategy_shadow_outcomes_v1.sqlite"
            store = ShadowOutcomeStore(source, batch_size=64, flush_sec=100)
            for index in range(4):
                store.observe(
                    {
                        "id": f"old-{index}",
                        "horizon_sec": 300,
                        "lane_id": "lane.fast",
                        "family": "momentum",
                        "profile": "fast",
                        "model_id": "lane.fast",
                        "input_timeframe": "M1",
                        "training_timeframe": "live",
                        "kind": "signal",
                        "instrument": "EUR_USD",
                        "direction": "buy",
                        "theoretical_pips": 1.0 if index % 2 else -0.5,
                        "entry_bid": 1.1,
                        "entry_ask": 1.1001,
                        "exit_bid": 1.1002,
                        "exit_ask": 1.1003,
                        "pip": 0.0001,
                    }
                )
            store.close()
            connection = sqlite3.connect(source)
            connection.execute(
                "UPDATE outcomes SET observed_utc = '2020-01-01T00:00:00+00:00'"
            )
            connection.commit()
            connection.close()

            compactor = ShadowOutcomeCompactor(
                [source],
                root / "rollups.sqlite",
                root / "archive",
                root / "state.json",
                retention_days=30,
                batch_size=100,
            )
            state = compactor.run_once()
            compactor.close()

            connection = sqlite3.connect(source)
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0],
                0,
            )
            connection.close()
            self.assertEqual(state["sources"][0]["archived_rows"], 4)
            self.assertEqual(state["sources"][0]["validated_archive_parts"], 1)
            self.assertEqual(state["sources"][0]["manifest_count"], 1)
            self.assertIn("reclaimable_bytes", state["sources"][0]["storage"])
            self.assertGreater(state["rollup_rows"], 0)
            self.assertEqual(
                len(list((root / "archive").rglob("*.parquet"))),
                1,
            )
            self.assertEqual(
                len(list((root / "archive").rglob("*.manifest.json"))),
                1,
            )

    def test_archive_defers_delete_when_live_writer_holds_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "strategy_shadow_outcomes_v1.sqlite"
            store = ShadowOutcomeStore(source, batch_size=64, flush_sec=100)
            store.observe(
                {
                    "id": "old-locked",
                    "horizon_sec": 300,
                    "lane_id": "lane.fast",
                    "family": "momentum",
                    "profile": "fast",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "theoretical_pips": 1.0,
                    "entry_bid": 1.1,
                    "entry_ask": 1.1001,
                    "exit_bid": 1.1002,
                    "exit_ask": 1.1003,
                    "pip": 0.0001,
                }
            )
            store.close()
            setup = sqlite3.connect(source)
            setup.execute(
                "UPDATE outcomes SET observed_utc = '2020-01-01T00:00:00+00:00'"
            )
            setup.commit()
            blocker = sqlite3.connect(source)
            blocker.execute("BEGIN IMMEDIATE")

            compactor = ShadowOutcomeCompactor(
                [source],
                root / "rollups.sqlite",
                root / "archive",
                root / "state.json",
                retention_days=30,
                batch_size=100,
                source_busy_timeout_ms=100,
                archive_pause_sec=0.0,
            )
            deferred = compactor.archive_source(
                source,
                "2021-01-01T00:00:00+00:00",
            )

            self.assertEqual(
                deferred["status"],
                "archive_deferred_database_busy",
            )
            self.assertEqual(
                setup.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0],
                1,
            )

            blocker.rollback()
            blocker.close()
            completed = compactor.archive_source(
                source,
                "2021-01-01T00:00:00+00:00",
            )
            compactor.close()
            setup.close()
            self.assertEqual(completed["archived_rows"], 1)
            self.assertEqual(completed["remaining_rows"], 0)

    def test_invalid_existing_archive_never_deletes_source_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "strategy_shadow_outcomes_v1.sqlite"
            store = ShadowOutcomeStore(source, batch_size=64, flush_sec=100)
            store.observe(
                {
                    "id": "old-invalid-archive",
                    "horizon_sec": 300,
                    "lane_id": "lane.fast",
                    "family": "momentum",
                    "profile": "fast",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "theoretical_pips": 1.0,
                    "entry_bid": 1.1,
                    "entry_ask": 1.1001,
                    "exit_bid": 1.1002,
                    "exit_ask": 1.1003,
                    "pip": 0.0001,
                }
            )
            store.close()
            connection = sqlite3.connect(source)
            connection.execute(
                "UPDATE outcomes SET observed_utc = '2020-01-01T00:00:00+00:00'"
            )
            row_id = int(connection.execute("SELECT row_id FROM outcomes").fetchone()[0])
            connection.commit()
            connection.close()
            partition = root / "archive" / source.stem / "2020-01-01"
            partition.mkdir(parents=True)
            destination = partition / f"rows_{row_id:012d}_{row_id:012d}.parquet"
            destination.write_bytes(b"not a parquet file")

            compactor = ShadowOutcomeCompactor(
                [source],
                root / "rollups.sqlite",
                root / "archive",
                root / "state.json",
                retention_days=30,
                batch_size=100,
            )
            result = compactor.archive_source(
                source,
                "2021-01-01T00:00:00+00:00",
            )
            compactor.close()
            check = sqlite3.connect(source)
            remaining = int(check.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0])
            check.close()

            self.assertEqual(result["status"], "archive_validation_failed_no_delete")
            self.assertEqual(result["archived_rows"], 0)
            self.assertEqual(remaining, 1)

    def test_archive_delete_is_scoped_to_the_selected_observed_date(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "strategy_shadow_outcomes_v1.sqlite"
            store = ShadowOutcomeStore(source, batch_size=64, flush_sec=100)
            for index in range(3):
                store.observe({
                    "id": f"interleaved-{index}", "horizon_sec": 300,
                    "lane_id": "lane.fast", "family": "momentum",
                    "profile": "fast", "kind": "signal",
                    "instrument": "EUR_USD", "direction": "buy",
                    "theoretical_pips": 1.0, "entry_bid": 1.1,
                    "entry_ask": 1.1001, "exit_bid": 1.1002,
                    "exit_ask": 1.1003, "pip": 0.0001,
                })
            store.close()
            connection = sqlite3.connect(source)
            ids = [row[0] for row in connection.execute(
                "SELECT row_id FROM outcomes ORDER BY row_id"
            )]
            connection.execute(
                "UPDATE outcomes SET observed_utc='2020-01-01T00:00:00+00:00' "
                "WHERE row_id IN (?,?)", (ids[0], ids[2])
            )
            connection.execute(
                "UPDATE outcomes SET observed_utc='2020-01-02T00:00:00+00:00' "
                "WHERE row_id=?", (ids[1],)
            )
            connection.commit(); connection.close()
            compactor = ShadowOutcomeCompactor(
                [source], root / "rollups.sqlite", root / "archive",
                root / "state.json", retention_days=30, batch_size=100,
                max_archive_batches=1,
            )
            result = compactor.archive_source(
                source, "2021-01-01T00:00:00+00:00"
            )
            compactor.close()
            check = sqlite3.connect(source)
            remaining = check.execute(
                "SELECT row_id,observed_utc FROM outcomes ORDER BY row_id"
            ).fetchall()
            check.close()
            self.assertEqual(result["archived_rows"], 2)
            self.assertEqual(remaining, [(ids[1], "2020-01-02T00:00:00+00:00")])
            parquet = next((root / "archive").rglob("*.parquet"))
            self.assertEqual(pq.ParquetFile(parquet).metadata.num_rows, 2)


if __name__ == "__main__":
    unittest.main()
