import ast
import datetime as dt
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import oanda_immutable_event_clock_worker as worker  # noqa: E402
from forex_system.ingestion import immutable_event_clock as clock  # noqa: E402


UTC = dt.timezone.utc


class ImmutableEventClockWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.events = self.root / "events.json"
        self.manifest = self.root / "manifest.json"
        self.integrity = self.root / "clock.json"
        self.database = self.root / "clock.sqlite"
        self.state = self.root / "state.json"
        self.failures = self.root / "failures.jsonl"
        self.observed = dt.datetime(2026, 8, 17, 5, 0, 10, tzinfo=UTC)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_v2_defaults_are_isolated_from_legacy_v1_artifacts(self) -> None:
        self.assertEqual("immutable_event_clock_ledger_v2", clock.SCHEMA_VERSION)
        self.assertEqual(
            "immutable_point_in_time_event_clock_v2_20260817", clock.CONTRACT_ID
        )
        self.assertEqual("immutable_event_clock_v2.sqlite", worker.DEFAULT_DATABASE.name)
        self.assertEqual("immutable_event_clock_worker_v2.json", worker.DEFAULT_STATE.name)
        self.assertEqual("immutable_event_clock_worker_v2", worker.SCHEMA_VERSION)

    def event(self) -> dict:
        return {
            "event_id": "denmark-release",
            "source_id": "denmark_statistics_release_calendar_exact_v1",
            "source_contract_id": (
                "denmark_statistics_release_calendar_exact_v2_paged_45d_20260817"
            ),
            "source_cohort_id": (
                "denmark_statistics_release_calendar_exact_v2_paged_45d_20260817"
            ),
            "material_content_sha256": "a" * 64,
            "scheduled_utc": "2026-08-20T06:00:00+00:00",
            "event_utc": "2026-08-20T06:00:00+00:00",
            "first_known_utc": "2026-08-17T04:54:11+00:00",
            "updated_utc": "2026-08-17T04:54:11+00:00",
            "headline": "Denmark official release",
            "category": "official_economic_release",
            "source_name": "Statistics Denmark",
            "source_url": "https://www.dst.dk/",
            "source_type": "live_news_watch",
            "source_verified": True,
            "currencies": ["DKK"],
            "direct_currencies": ["DKK"],
            "timing_precision": "minute",
            "clock_semantics": "domestic_official_statistical_release",
            "independent_domestic_event": True,
            "event_time_basis": "scheduled_release",
        }

    def write_source(
        self,
        *,
        generated="2026-08-17T05:00:00+00:00",
        declared_count=1,
        events=None,
    ) -> None:
        rows = [self.event()] if events is None else events
        self.events.write_text(json.dumps(rows), encoding="utf-8")
        events_sha256 = hashlib.sha256(self.events.read_bytes()).hexdigest()
        self.manifest.write_text(
            json.dumps(
                {
                    "manifest_schema_version": 2,
                    "pipeline_version": "all_pair_news_event_tags_v3",
                    "generated_utc": generated,
                    "event_count": declared_count,
                    "events_artifact_name": self.events.name,
                    "events_sha256": events_sha256,
                }
            ),
            encoding="utf-8",
        )
        # The producer writes events first and manifest last.
        manifest_time = self.events.stat().st_mtime + 0.01
        import os

        os.utime(self.manifest, (manifest_time, manifest_time))

    def write_integrity(self, generated="2026-08-17T05:00:09+00:00") -> None:
        self.integrity.write_text(
            json.dumps(
                {
                    "generated_utc": generated,
                    "status": "ok",
                    "timestamp_normalization_trusted": True,
                    "host_clock_synchronized": True,
                    "source_fresh": True,
                }
            ),
            encoding="utf-8",
        )

    def cycle(self, **overrides):
        values = {
            "events_path": self.events,
            "manifest_path": self.manifest,
            "clock_integrity_path": self.integrity,
            "database_path": self.database,
            "state_path": self.state,
            "failure_path": self.failures,
            "observed_utc": self.observed,
        }
        values.update(overrides)
        return worker.run_cycle(**values)

    def test_fresh_coherent_generation_is_atomically_captured(self) -> None:
        self.write_source()
        self.write_integrity()
        result = self.cycle()
        self.assertEqual(result["status"], "captured")
        self.assertTrue(result["clock_attested"])
        self.assertTrue(self.database.is_file())
        with closing(sqlite3.connect(self.database)) as connection:
            snapshots = connection.execute(
                "SELECT COUNT(*) FROM event_clock_snapshots"
            ).fetchone()[0]
            observations = connection.execute(
                "SELECT COUNT(*) FROM event_clock_observations"
            ).fetchone()[0]
        self.assertEqual((snapshots, observations), (1, 1))
        reconstructed = clock.reconstruct_as_of(self.database, self.observed)
        self.assertEqual(reconstructed["event_count"], 1)
        self.assertEqual(
            reconstructed["events"][0]["source_contract_first_observed_utc"],
            self.observed.isoformat(),
        )

    def test_same_generation_is_idle_and_does_not_inflate_observations(self) -> None:
        self.write_source()
        self.write_integrity()
        first = self.cycle()
        second = self.cycle(observed_utc=self.observed + dt.timedelta(seconds=5))
        self.assertEqual(first["status"], "captured")
        self.assertEqual(second["status"], "idle_already_captured_generation")
        with closing(sqlite3.connect(self.database)) as connection:
            observations = connection.execute(
                "SELECT COUNT(*) FROM event_clock_observations"
            ).fetchone()[0]
        self.assertEqual(observations, 1)

    def test_stale_source_fails_without_creating_database(self) -> None:
        self.write_source(generated="2026-08-17T04:58:00+00:00")
        self.write_integrity()
        result = self.cycle()
        self.assertEqual(result["status"], "source_generation_stale")
        self.assertFalse(result["database_mutated"])
        self.assertFalse(self.database.exists())
        self.assertTrue(self.state.is_file())
        self.assertEqual(len(self.failures.read_text(encoding="utf-8").splitlines()), 1)

    def test_stale_clock_attestation_fails_without_database(self) -> None:
        self.write_source()
        self.write_integrity(generated="2026-08-17T04:58:00+00:00")
        result = self.cycle()
        self.assertEqual(result["status"], "clock_attestation_not_fresh_trusted")
        self.assertFalse(self.database.exists())

    def test_missing_source_fails_without_database(self) -> None:
        self.write_integrity()
        result = self.cycle()
        self.assertEqual(result["status"], "source_artifact_missing_or_unreadable")
        self.assertFalse(self.database.exists())

    def test_incoherent_publication_fails_without_database(self) -> None:
        self.write_source(declared_count=0)
        self.write_integrity()
        result = self.cycle()
        self.assertEqual(result["status"], "source_publication_incoherent_or_racing")
        self.assertFalse(self.database.exists())

    def test_read_race_fails_closed(self) -> None:
        self.write_source()
        self.write_integrity()
        with mock.patch.object(
            worker,
            "read_stable_source",
            side_effect=clock.EventClockLedgerError("manifest_changed_during_read"),
        ):
            result = self.cycle()
        self.assertEqual(result["status"], "source_publication_incoherent_or_racing")
        self.assertFalse(self.database.exists())

    def test_capture_clock_is_taken_after_successful_source_read(self) -> None:
        self.write_source()
        self.write_integrity(generated="2026-08-17T05:00:19+00:00")
        source = clock.read_stable_source(self.events, self.manifest)
        cycle_started = dt.datetime(2026, 8, 17, 5, 0, 10, tzinfo=UTC)
        read_completed = dt.datetime(2026, 8, 17, 5, 0, 20, tzinfo=UTC)
        now_values = iter([cycle_started, read_completed])
        with mock.patch.object(worker, "read_stable_source", return_value=source):
            result = worker.run_cycle(
                events_path=self.events,
                manifest_path=self.manifest,
                clock_integrity_path=self.integrity,
                database_path=self.database,
                state_path=self.state,
                failure_path=self.failures,
                now_fn=lambda: next(now_values),
            )
        self.assertEqual(result["status"], "captured")
        self.assertEqual(result["cycle_started_utc"], cycle_started.isoformat())
        self.assertEqual(result["source_read_completed_utc"], read_completed.isoformat())
        reconstructed = clock.reconstruct_as_of(self.database, read_completed)
        self.assertEqual(reconstructed["snapshot_captured_utc"], read_completed.isoformat())

    def test_snapshot_and_observation_rollback_together(self) -> None:
        self.write_source()
        self.write_integrity()
        source = clock.read_stable_source(self.events, self.manifest)
        attestation = clock.build_clock_attestation(
            self.integrity, self.observed, maximum_age_sec=90
        )
        snapshot = clock.build_snapshot(source, self.observed, clock_attestation=attestation)
        with mock.patch.object(
            clock,
            "_insert_observation_row_atomic",
            side_effect=clock.EventClockLedgerError("injected_observation_failure"),
        ):
            with self.assertRaisesRegex(
                clock.EventClockLedgerError, "injected_observation_failure"
            ):
                clock.append_snapshot_and_observation_atomic(self.database, snapshot)
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM event_clock_snapshots"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM event_clock_observations"
                ).fetchone()[0],
                0,
            )

    def test_worker_has_static_research_only_import_boundary(self) -> None:
        source = (ROOT / "oanda_immutable_event_clock_worker.py").read_text(
            encoding="utf-8"
        )
        tree = ast.parse(source)
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
        joined = "\n".join(imported).lower()
        for forbidden in (
            "oanda_news_event_tagger",
            "requests",
            "execution",
            "authorization",
            "lifecycle",
            "promotion",
        ):
            self.assertNotIn(forbidden, joined)
        self.assertNotIn("create_order", source.lower())
        self.assertNotIn("close_trade", source.lower())
        self.assertIn('SUPPORTED_EXECUTION_DECISION = "no_trade"', source)


if __name__ == "__main__":
    unittest.main()
