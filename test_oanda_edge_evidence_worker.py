import json
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from trad.oanda_edge_evidence_worker import (
    ProgressHeartbeat, checkpoint_can_defer_append_only_growth,
    checkpoint_matches_semantic_inputs,
    checkpoint_outputs_match, file_snapshot, input_fingerprint,
    macro_state_sha256, output_integrity,
)


class EdgeEvidenceWorkerTests(unittest.TestCase):
    def test_output_integrity_prevents_unchanged_skip_after_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_json = root / "report.json"
            report_md = root / "report.md"
            report_json.write_text('{"run_id":"one"}', encoding="utf-8")
            report_md.write_text("original", encoding="utf-8")
            checkpoint = {
                "output_integrity": output_integrity(report_json, report_md)
            }
            self.assertTrue(
                checkpoint_outputs_match(checkpoint, report_json, report_md)
            )
            report_md.write_text("mutated", encoding="utf-8")
            self.assertFalse(
                checkpoint_outputs_match(checkpoint, report_json, report_md)
            )

    def test_legacy_checkpoint_without_output_hashes_forces_rebuild(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text("{}", encoding="utf-8")
            self.assertFalse(checkpoint_outputs_match({}, path))

    def test_macro_fingerprint_ignores_collector_progress_not_report_semantics(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "macro.json"
            path.write_text(json.dumps({
                "generated_utc": "first", "articles_inspected": 10,
                "status": "blocked", "release_count": 2,
            }), encoding="utf-8")
            first = macro_state_sha256(path)
            path.write_text(json.dumps({
                "generated_utc": "second", "articles_inspected": 100,
                "status": "blocked", "release_count": 2,
            }), encoding="utf-8")
            self.assertEqual(first, macro_state_sha256(path))
            path.write_text(json.dumps({
                "generated_utc": "third", "articles_inspected": 100,
                "status": "blocked", "release_count": 3,
            }), encoding="utf-8")
            self.assertNotEqual(first, macro_state_sha256(path))

    def test_input_fingerprint_is_stable_and_changes_with_database_wal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            news = root / "news.sqlite"
            registry = root / "registry.md"
            with sqlite3.connect(source) as connection:
                connection.execute("CREATE TABLE outcomes(row_id INTEGER PRIMARY KEY, value TEXT)")
                connection.execute("CREATE TABLE canonical_outcomes(row_id INTEGER PRIMARY KEY)")
                connection.execute("CREATE TABLE canonical_forecasts(value TEXT)")
                connection.execute("CREATE TABLE forecast_integrity_events(row_id INTEGER PRIMARY KEY)")
                connection.execute("INSERT INTO outcomes VALUES (1,'source')")
            connection.close()
            with sqlite3.connect(news) as connection:
                connection.execute("CREATE TABLE articles(first_seen_utc TEXT,last_seen_utc TEXT)")
                connection.execute("CREATE TABLE topic_events(last_known_utc TEXT)")
                connection.execute("INSERT INTO articles VALUES ('t1','t1')")
            connection.close()
            registry.write_text("registry", encoding="utf-8")
            first = input_fingerprint(
                source_database=source, news_database=news, registry=registry
            )
            second = input_fingerprint(
                source_database=source, news_database=news, registry=registry
            )
            self.assertEqual(
                first["fingerprint_sha256"], second["fingerprint_sha256"]
            )
            with sqlite3.connect(source) as connection:
                connection.execute("INSERT INTO outcomes VALUES (2,'new commit')")
            connection.close()
            third = input_fingerprint(
                source_database=source, news_database=news, registry=registry
            )
            self.assertNotEqual(
                first["fingerprint_sha256"], third["fingerprint_sha256"]
            )

    def test_input_fingerprint_ignores_physical_wal_churn(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            news = root / "news.sqlite"
            registry = root / "registry.md"
            with sqlite3.connect(source) as connection:
                connection.execute("CREATE TABLE outcomes(row_id INTEGER PRIMARY KEY)")
                connection.execute("CREATE TABLE canonical_outcomes(row_id INTEGER PRIMARY KEY)")
                connection.execute("CREATE TABLE canonical_forecasts(value TEXT)")
                connection.execute("CREATE TABLE forecast_integrity_events(row_id INTEGER PRIMARY KEY)")
            connection.close()
            with sqlite3.connect(news) as connection:
                connection.execute("CREATE TABLE articles(first_seen_utc TEXT,last_seen_utc TEXT)")
                connection.execute("CREATE TABLE topic_events(last_known_utc TEXT)")
            connection.close()
            registry.write_text("registry", encoding="utf-8")
            first = input_fingerprint(
                source_database=source, news_database=news, registry=registry
            )
            Path(str(source) + "-wal").write_bytes(b"physical churn only")
            second = input_fingerprint(
                source_database=source, news_database=news, registry=registry
            )
            self.assertEqual(
                first["fingerprint_sha256"], second["fingerprint_sha256"]
            )

    def test_raw_news_topic_growth_is_diagnostic_not_heavy_rebuild_input(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            news = root / "news.sqlite"
            registry = root / "registry.md"
            connection = sqlite3.connect(source)
            with connection:
                connection.execute("CREATE TABLE outcomes(row_id INTEGER PRIMARY KEY)")
                connection.execute("CREATE TABLE canonical_outcomes(row_id INTEGER PRIMARY KEY)")
                connection.execute("CREATE TABLE canonical_forecasts(value TEXT)")
                connection.execute("CREATE TABLE forecast_integrity_events(row_id INTEGER PRIMARY KEY)")
            connection.close()
            connection = sqlite3.connect(news)
            with connection:
                connection.execute("CREATE TABLE topic_events(last_known_utc TEXT)")
            connection.close()
            registry.write_text("registry", encoding="utf-8")
            first = input_fingerprint(
                source_database=source, news_database=news, registry=registry
            )
            connection = sqlite3.connect(news)
            with connection:
                connection.execute("INSERT INTO topic_events VALUES ('later')")
            connection.close()
            second = input_fingerprint(
                source_database=source, news_database=news, registry=registry
            )
            self.assertEqual(first["fingerprint_sha256"], second["fingerprint_sha256"])
            self.assertNotEqual(
                first["diagnostic_database_snapshots"],
                second["diagnostic_database_snapshots"],
            )

    def test_pre_split_checkpoint_can_migrate_when_only_news_or_macro_diagnostics_differ(self):
        current = {
            "database_snapshots": [{"role": "source", "highwaters": {"x": [1]}}],
            "content_sha256": {"report.py": "same"},
            "cohort_definition_sha256": {"cohort": "same"},
            "diagnostic_macro_state_sha256": "new",
        }
        checkpoint = {
            **current,
            "database_snapshots": [
                dict(
                    current["database_snapshots"][0],
                    highwaters={"x": [1]},
                ),
                {"role": "news", "highwaters": {"topic_events": [1, 1, 1]}},
                {"role": "macro", "highwaters": {"reactions": [10]}},
            ],
            "macro_state_sha256": "old",
            "content_sha256": {
                **current["content_sha256"],
                "worker.py": "operational-only",
            },
        }
        self.assertTrue(checkpoint_matches_semantic_inputs(checkpoint, current))
        checkpoint["database_snapshots"][0]["highwaters"] = {"x": [2]}
        self.assertFalse(checkpoint_matches_semantic_inputs(checkpoint, current))

    def test_append_only_growth_can_defer_only_inside_bounded_cadence(self):
        now = datetime.now(timezone.utc).isoformat()
        common = {
            "content_sha256": {"report.py": "same"},
            "cohort_definition_sha256": {"cohort": "same"},
        }
        checkpoint = {
            **common,
            "completed_utc": now,
            "database_snapshots": [
                {
                    "role": "source", "path": "source.sqlite", "exists": True,
                    "schema_sha256": "schema", "highwaters": {"outcomes": [1, 10]},
                },
                {"role": "candidate_cohorts", "highwaters": {"cohorts": [1]}},
            ],
        }
        current = {
            **common,
            "database_snapshots": [
                {
                    "role": "source", "path": "source.sqlite", "exists": True,
                    "schema_sha256": "schema", "highwaters": {"outcomes": [1, 20]},
                },
                {"role": "candidate_cohorts", "highwaters": {"cohorts": [1]}},
            ],
        }
        self.assertTrue(checkpoint_can_defer_append_only_growth(
            checkpoint, current, minimum_rebuild_interval_sec=21600.0
        ))
        current["content_sha256"] = {"report.py": "changed"}
        self.assertFalse(checkpoint_can_defer_append_only_growth(
            checkpoint, current, minimum_rebuild_interval_sec=21600.0
        ))

    def test_progress_heartbeat_is_fresh_during_long_build(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.sqlite"
            evidence = root / "evidence.sqlite"
            heartbeat_path = root / "heartbeat.json"
            source.write_bytes(b"source")
            evidence.write_bytes(b"evidence")
            heartbeat = ProgressHeartbeat(
                heartbeat_path,
                source_database=source,
                evidence_database=evidence,
                interval_sec=0.02,
            )
            heartbeat.start()
            heartbeat.update(
                "loading_rows",
                {"rows_scanned": 50_000, "source_highwater_row_id": 70_000},
            )
            time.sleep(0.06)
            payload = json.loads(heartbeat_path.read_text(encoding="utf-8"))
            heartbeat.stop()

            self.assertEqual(payload["schema_version"], 2)
            self.assertEqual(payload["phase"], "loading_rows")
            self.assertGreaterEqual(payload["progress_sequence"], 1)
            self.assertLess(payload["progress_age_sec"], 1.0)
            self.assertEqual(payload["details"]["rows_scanned"], 50_000)
            self.assertEqual(
                payload["details"]["source_database"]["snapshot_id"],
                file_snapshot(source)["snapshot_id"],
            )
            self.assertFalse(payload["can_place_orders"])
            self.assertFalse(payload["can_promote"])


if __name__ == "__main__":
    unittest.main()
