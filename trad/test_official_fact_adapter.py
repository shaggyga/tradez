from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from forex_system.ingestion.official_fact_adapter import (  # noqa: E402
    OfficialFactAdapter,
    OfficialFactPaths,
)
from forex_system.ingestion import immutable_event_clock_v1_reader as v1_reader  # noqa: E402


MACRO_COLUMNS = """
    row_id INTEGER PRIMARY KEY,
    revision_id TEXT,
    release_key TEXT,
    source_event_id TEXT,
    recorded_utc TEXT,
    causal_known_utc TEXT,
    scheduled_utc TEXT,
    source_reported_update_utc TEXT,
    event_series_id TEXT,
    event_name TEXT,
    event_country TEXT,
    currencies_json TEXT,
    reference_period TEXT,
    reference_date TEXT,
    importance TEXT,
    unit TEXT,
    actual_value REAL,
    consensus_value REAL,
    previous_value REAL,
    revised_previous_value REAL,
    surprise_raw REAL,
    standardized_surprise REAL,
    known_before_recorded_timestamp INTEGER,
    source_id TEXT,
    source_name TEXT,
    source_url TEXT,
    source_verified INTEGER,
    source_direct INTEGER,
    payload_sha256 TEXT,
    payload_json TEXT
"""


class OfficialFactAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.root = Path(self._temporary.name)

    def tearDown(self) -> None:
        self._temporary.cleanup()

    def paths(self) -> OfficialFactPaths:
        return OfficialFactPaths(
            source_governance_db=self.root / "source_governance.sqlite",
            macro_surprise_db=self.root / "macro.sqlite",
            daily_rates_db=self.root / "rates.sqlite",
            internal_expectations_db=self.root / "expectations.sqlite",
            policy_baselines_json=self.root / "policy.json",
            event_preflight_json=self.root / "events.json",
            immutable_event_clock_db=self.root / "immutable_event_clock.sqlite",
            source_coverage_json=self.root / "coverage.json",
            clock_integrity_json=self.root / "clock.json",
            intraday_rates_config_json=self.root / "intraday_rates.json",
        )

    def append_clock_snapshot(
        self,
        *,
        generated_utc: str,
        captured_utc: str,
        events: list[dict[str, object]],
    ) -> None:
        database = self.paths().immutable_event_clock_db
        captured = dt.datetime.fromisoformat(
            captured_utc.replace("Z", "+00:00")
        ).astimezone(dt.timezone.utc).isoformat()
        generated = dt.datetime.fromisoformat(
            generated_utc.replace("Z", "+00:00")
        ).astimezone(dt.timezone.utc).isoformat()
        snapshot_id = "historical_v1_fixture_" + hashlib.sha256(
            captured.encode("utf-8")
        ).hexdigest()[:24]
        normalized_events: list[dict[str, object]] = []
        for raw in events:
            event = dict(raw)
            material = json.dumps(event, sort_keys=True, separators=(",", ":"))
            event["event_version_id"] = "fixture_event_version_" + hashlib.sha256(
                material.encode("utf-8")
            ).hexdigest()[:24]
            event["upstream_event_id"] = str(event.pop("event_id"))
            event["upstream_first_known_utc"] = str(event.pop("first_known_utc"))
            event["direction_policy"] = "abstain"
            event["execution_eligible"] = False
            normalized_events.append(event)
        snapshot = {
            "schema_version": v1_reader.SCHEMA_VERSION,
            "contract_id": v1_reader.CONTRACT_ID,
            "snapshot_id": snapshot_id,
            "captured_utc": captured,
            "source_generated_utc": generated,
            "source_pipeline_version": v1_reader.EXPECTED_SOURCE_PIPELINE_VERSION,
            "source_event_count": len(normalized_events),
            "source_scheduled_clock_count": len(normalized_events),
            "rejected_malformed_clock_count": 0,
            "events_sha256": "a" * 64,
            "manifest_sha256": "b" * 64,
            "events_artifact_name": "events_latest.json",
            "manifest_artifact_name": "events_latest.manifest.json",
            "semantic_clock_sha256": "c" * 64,
            "event_versions": normalized_events,
            "research_only": True,
            "execution_eligible": False,
            "supported_execution_decision": "no_trade",
        }
        with closing(sqlite3.connect(database)) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS event_clock_snapshots(
                  snapshot_id TEXT PRIMARY KEY,schema_version TEXT NOT NULL,
                  contract_id TEXT NOT NULL,captured_utc TEXT NOT NULL,
                  source_generated_utc TEXT NOT NULL,source_pipeline_version TEXT NOT NULL,
                  source_event_count INTEGER NOT NULL,
                  source_scheduled_clock_count INTEGER NOT NULL,
                  rejected_malformed_clock_count INTEGER NOT NULL,
                  events_sha256 TEXT NOT NULL,manifest_sha256 TEXT NOT NULL,
                  events_artifact_name TEXT NOT NULL,manifest_artifact_name TEXT NOT NULL,
                  snapshot_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS event_clock_versions(
                  event_version_id TEXT PRIMARY KEY,upstream_event_id TEXT NOT NULL,
                  scheduled_utc TEXT NOT NULL,event_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS snapshot_events(
                  snapshot_id TEXT NOT NULL,event_version_id TEXT NOT NULL,
                  ordinal INTEGER NOT NULL,effective_known_utc TEXT NOT NULL,
                  PRIMARY KEY(snapshot_id,event_version_id)
                );
                """
            )
            for ordinal, event in enumerate(normalized_events):
                canonical = json.dumps(event, sort_keys=True, separators=(",", ":"))
                connection.execute(
                    "INSERT OR IGNORE INTO event_clock_versions VALUES(?,?,?,?)",
                    (
                        event["event_version_id"],
                        event["upstream_event_id"],
                        event["scheduled_utc"],
                        canonical,
                    ),
                )
                known = dt.datetime.fromisoformat(
                    str(event["upstream_first_known_utc"]).replace("Z", "+00:00")
                ).astimezone(dt.timezone.utc)
                effective = max(
                    dt.datetime.fromisoformat(captured), known
                ).isoformat()
                connection.execute(
                    "INSERT INTO snapshot_events VALUES(?,?,?,?)",
                    (snapshot_id, event["event_version_id"], ordinal, effective),
                )
            connection.execute(
                "INSERT INTO event_clock_snapshots VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    snapshot_id,
                    v1_reader.SCHEMA_VERSION,
                    v1_reader.CONTRACT_ID,
                    captured,
                    generated,
                    v1_reader.EXPECTED_SOURCE_PIPELINE_VERSION,
                    len(normalized_events),
                    len(normalized_events),
                    0,
                    "a" * 64,
                    "b" * 64,
                    "events_latest.json",
                    "events_latest.manifest.json",
                    json.dumps(snapshot, sort_keys=True, separators=(",", ":")),
                ),
            )
            connection.commit()
        self._write_v1_retirement_fingerprint()

    def _write_v1_retirement_fingerprint(self) -> Path:
        database = self.paths().immutable_event_clock_db
        with closing(sqlite3.connect(database)) as connection:
            tables = [
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                ).fetchall()
            ]
            counts = {
                table: int(
                    connection.execute(
                        f'SELECT COUNT(*) FROM "{table}"'
                    ).fetchone()[0]
                )
                for table in tables
            }
            quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        fingerprint = self.root / "immutable_event_clock_v1_retirement.json"
        fingerprint.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "record_type": "immutable_event_clock_retirement_fingerprint",
                    "record_id": "official_fact_adapter_historical_fixture",
                    "status": "retired_terminal",
                    "clock_schema_version": v1_reader.SCHEMA_VERSION,
                    "clock_contract_id": v1_reader.CONTRACT_ID,
                    "frozen_at_utc": "2026-08-17T03:00:00+00:00",
                    "database": {
                        "relative_path": database.name,
                        "byte_length": database.stat().st_size,
                        "sha256": hashlib.sha256(database.read_bytes()).hexdigest(),
                        "quick_check": quick_check,
                        "row_counts": counts,
                        "preserve_bytes": True,
                    },
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return fingerprint

    def historical_v1_reader(self) -> v1_reader.ImmutableEventClockV1Reader:
        return v1_reader.ImmutableEventClockV1Reader(
            self.paths().immutable_event_clock_db,
            retirement_fingerprint_path=self.root
            / "immutable_event_clock_v1_retirement.json",
        )

    @staticmethod
    def clock_event(
        *,
        event_id: str = "aud-release",
        scheduled_utc: str = "2026-08-20T01:30:00Z",
        first_known_utc: str = "2026-08-16T09:00:00Z",
    ) -> dict[str, object]:
        return {
            "event_id": event_id,
            "event_utc": scheduled_utc,
            "scheduled_utc": scheduled_utc,
            "first_known_utc": first_known_utc,
            "updated_utc": first_known_utc,
            "headline": "Australia official economic release",
            "category": "economic_release",
            "source_name": "ABS",
            "source_url": "https://www.abs.gov.au/",
            "source_type": "live_news_watch",
            "source_verified": True,
            "currencies": ["AUD"],
            "direct_currencies": ["AUD"],
            "timing_precision": "minute",
            "event_time_basis": "scheduled_release",
        }

    def make_governance(self, quarantined: tuple[str, ...] = ()) -> None:
        with closing(sqlite3.connect(self.paths().source_governance_db)) as connection:
            connection.executescript(
                """
                CREATE TABLE source_events(source_event_id TEXT PRIMARY KEY);
                CREATE VIEW source_events_causal_v1 AS SELECT * FROM source_events;
                CREATE TABLE source_event_quarantines(source_event_id TEXT PRIMARY KEY);
                """
            )
            connection.executemany(
                "INSERT INTO source_event_quarantines VALUES(?)",
                [(value,) for value in quarantined],
            )
            connection.commit()

    def make_macro(self) -> None:
        with closing(sqlite3.connect(self.paths().macro_surprise_db)) as connection:
            connection.execute(f"CREATE TABLE macro_release_revisions({MACRO_COLUMNS})")
            connection.commit()

    def add_macro(
        self,
        row_id: int,
        revision_id: str,
        release_key: str,
        known_utc: str,
        actual: float,
        *,
        source_event_id: str | None = None,
        recorded_utc: str | None = None,
        consensus: float | None = None,
        consensus_causal: bool = False,
        surprise: float | None = None,
        standardized: float | None = None,
    ) -> None:
        published = known_utc.replace("10:00", "09:59").replace("09:30", "09:29")
        published = published.replace("09:00", "08:59").replace("09:45", "09:44")
        payload = json.dumps(
            {
                "source_native_published_utc": published,
                "first_seen_utc": known_utc,
                "source_contract_id": "official_fixture_v1",
                "source_cohort_id": "official_fixture.discovery",
            }
        )
        values = (
            row_id,
            revision_id,
            release_key,
            source_event_id or f"event-{row_id}",
            recorded_utc or known_utc,
            known_utc,
            "2026-08-17T08:30:00+00:00",
            published,
            "fixture_series",
            "Fixture official release",
            "Australia",
            '["AUD"]',
            "2026-07",
            "2026-07-31",
            "high",
            "percent",
            actual,
            consensus,
            0.5,
            None,
            surprise,
            standardized,
            int(consensus_causal),
            "fixture_official",
            "Fixture Official Publisher",
            "https://example.invalid/release",
            1,
            1,
            f"hash-{row_id}",
            payload,
        )
        with closing(sqlite3.connect(self.paths().macro_surprise_db)) as connection:
            placeholders = ",".join("?" for _ in values)
            connection.execute(
                f"INSERT INTO macro_release_revisions VALUES({placeholders})", values
            )
            connection.commit()

    def test_cutoff_selects_latest_known_revision_and_excludes_quarantine(self) -> None:
        self.make_governance(("quarantined-event",))
        self.make_macro()
        self.add_macro(1, "revision-one", "revisable", "2026-08-16T10:00:00+00:00", 1.0)
        self.add_macro(2, "revision-two", "revisable", "2026-08-16T12:00:00+00:00", 2.0)
        self.add_macro(
            3,
            "quarantined",
            "quarantined",
            "2026-08-16T09:45:00+00:00",
            9.0,
            source_event_id="quarantined-event",
        )
        self.add_macro(
            4,
            "late-backfill",
            "late-backfill",
            "2026-08-16T09:00:00+00:00",
            8.0,
            recorded_utc="2026-08-16T13:00:00+00:00",
        )

        snapshot = OfficialFactAdapter(self.paths()).as_of(
            "2026-08-16T11:00:00+00:00", currencies=("AUD",)
        )
        facts = [row for row in snapshot["facts"] if row["fact_type"] == "official_macro_actual"]

        self.assertEqual(["revision-one"], [row["fact_id"].split(":")[0] for row in facts])
        self.assertEqual(1.0, facts[0]["actual_value"])
        self.assertLessEqual(facts[0]["effective_from_utc"], snapshot["decision_cutoff_utc"])
        self.assertEqual(1, snapshot["quarantined_source_event_count"])
        with closing(sqlite3.connect(self.paths().macro_surprise_db)) as connection:
            self.assertEqual(
                4,
                connection.execute("SELECT COUNT(*) FROM macro_release_revisions").fetchone()[0],
            )

    def test_noncausal_consensus_and_surprise_are_never_promoted(self) -> None:
        self.make_governance()
        self.make_macro()
        self.add_macro(
            1,
            "noncausal",
            "noncausal",
            "2026-08-16T09:00:00+00:00",
            3.0,
            consensus=2.5,
            surprise=0.5,
            standardized=1.2,
        )
        self.add_macro(
            2,
            "causal",
            "causal",
            "2026-08-16T09:30:00+00:00",
            4.0,
            consensus=3.5,
            consensus_causal=True,
            surprise=0.5,
            standardized=0.8,
        )

        snapshot = OfficialFactAdapter(self.paths()).as_of(
            "2026-08-16T11:00:00Z", currencies=("AUD",)
        )
        by_revision = {
            row["fact_id"].split(":")[0]: row
            for row in snapshot["facts"]
            if row["fact_type"] == "official_macro_actual"
        }

        noncausal = by_revision["noncausal"]
        self.assertFalse(noncausal["consensus_causal"])
        self.assertIsNone(noncausal["consensus_value"])
        self.assertEqual(2.5, noncausal["noncausal_consensus_value"])
        self.assertIsNone(noncausal["surprise_raw"])
        self.assertIsNone(noncausal["standardized_surprise"])
        self.assertIn("noncausal_consensus_ignored", noncausal["degradation_reasons"])

        causal = by_revision["causal"]
        self.assertTrue(causal["consensus_causal"])
        self.assertEqual(3.5, causal["consensus_value"])
        self.assertEqual(0.5, causal["surprise_raw"])
        self.assertIsNone(causal["standardized_surprise"])
        self.assertEqual(0.8, causal["noncanonical_ledger_standardized_surprise"])
        self.assertIn(
            "stored_standardization_prior_not_causal_filtered",
            causal["degradation_reasons"],
        )
        self.assertEqual(1, snapshot["causal_consensus_count"])

    def test_raw_fact_records_do_not_inflate_distinct_macro_observation_count(self) -> None:
        self.make_governance()
        self.make_macro()
        self.add_macro(
            1,
            "source-a",
            "release-key-a",
            "2026-08-16T09:00:00+00:00",
            3.0,
        )
        self.add_macro(
            2,
            "source-b",
            "release-key-b",
            "2026-08-16T09:01:00+00:00",
            3.0,
        )

        snapshot = OfficialFactAdapter(self.paths()).as_of(
            "2026-08-16T11:00:00Z", currencies=("AUD",)
        )

        self.assertEqual(2, snapshot["macro_fact_record_count"])
        self.assertEqual(1, snapshot["distinct_macro_observation_count"])
        self.assertEqual(
            "normalized_provenance_records_not_independent_events",
            snapshot["fact_count_semantics"],
        )
        self.assertEqual(
            1,
            snapshot["currency_evidence"]["AUD"][
                "distinct_macro_observation_count"
            ],
        )

    def test_json_snapshots_and_policy_details_obey_knowledge_cutoff(self) -> None:
        self.make_governance()
        self.make_macro()
        self.paths().policy_baselines_json.write_text(
            json.dumps(
                {
                    "created_utc": "2026-08-16T08:00:00Z",
                    "contract_id": "policy_fixture_v1",
                    "baselines": [
                        {
                            "event_id": "known-policy",
                            "currency": "AUD",
                            "known_utc": "2026-08-16T09:00:00Z",
                            "detail_available_utc": "2026-08-16T09:05:00Z",
                            "published_utc": "2026-08-16T09:00:00Z",
                            "headline": "Known policy document",
                        },
                        {
                            "event_id": "future-detail",
                            "currency": "AUD",
                            "known_utc": "2026-08-16T09:00:00Z",
                            "detail_available_utc": "2026-08-16T12:00:00Z",
                            "published_utc": "2026-08-16T09:00:00Z",
                            "headline": "Not yet available in full",
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )
        self.paths().event_preflight_json.write_text(
            json.dumps(
                {
                    "generated_utc": "2026-08-16T11:30:00Z",
                    "events": [
                        {
                            "event_id": "future-event",
                            "currency": "AUD",
                            "scheduled_utc": "2026-08-17T01:00:00Z",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        snapshot = OfficialFactAdapter(self.paths()).as_of(
            "2026-08-16T11:00:00Z", currencies=("AUD",)
        )
        policies = [
            row for row in snapshot["facts"]
            if row["fact_type"] == "official_policy_document_context"
        ]

        self.assertEqual(["known-policy:AUD"], [row["fact_id"] for row in policies])
        self.assertEqual([], snapshot["upcoming_events"])
        self.assertIn(
            "event_preflight_not_known_at_cutoff",
            {row["code"] for row in snapshot["global_gaps"]},
        )

    def test_before_first_immutable_capture_does_not_backdate_current_view(self) -> None:
        event = self.clock_event()
        self.append_clock_snapshot(
            generated_utc="2026-08-16T12:00:00Z",
            captured_utc="2026-08-16T12:00:01Z",
            events=[event],
        )
        self.paths().event_preflight_json.write_text(
            json.dumps(
                {
                    "generated_utc": "2026-08-16T12:00:00Z",
                    "events": [
                        {
                            "event_id": "mutable-copy",
                            "currency": "AUD",
                            "scheduled_utc": "2026-08-20T01:30:00Z",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        snapshot = self.historical_v1_reader().as_of(
            dt.datetime.fromisoformat("2026-08-16T11:59:59+00:00")
        )

        self.assertEqual([], snapshot["events"])
        self.assertIsNone(snapshot["snapshot_id"])
        self.assertTrue(snapshot["historical_only"])
        self.assertFalse(snapshot["can_promote"])

    def test_exact_and_after_capture_prefer_immutable_snapshot(self) -> None:
        event = self.clock_event()
        self.append_clock_snapshot(
            generated_utc="2026-08-16T12:00:00Z",
            captured_utc="2026-08-16T12:00:01Z",
            events=[event],
        )
        self.paths().event_preflight_json.write_text(
            json.dumps(
                {
                    "generated_utc": "2026-08-16T11:00:00Z",
                    "events": [
                        {
                            "event_id": "contradictory-mutable-view",
                            "currency": "AUD",
                            "scheduled_utc": "2026-08-22T01:30:00Z",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        exact = self.historical_v1_reader().as_of(
            dt.datetime.fromisoformat("2026-08-16T12:00:01+00:00")
        )
        after = self.historical_v1_reader().as_of(
            dt.datetime.fromisoformat("2026-08-16T12:05:00+00:00")
        )

        for snapshot in (exact, after):
            self.assertTrue(snapshot["historical_only"])
            self.assertFalse(snapshot["can_promote"])
            self.assertEqual(
                ["aud-release"],
                [row["upstream_event_id"] for row in snapshot["events"]],
            )

    def test_immutable_revisions_and_removals_remain_cutoff_reconstructible(self) -> None:
        original = self.clock_event(scheduled_utc="2026-08-20T01:30:00Z")
        revised = self.clock_event(scheduled_utc="2026-08-20T02:00:00Z")
        self.append_clock_snapshot(
            generated_utc="2026-08-16T10:00:00Z",
            captured_utc="2026-08-16T10:00:01Z",
            events=[original],
        )
        self.append_clock_snapshot(
            generated_utc="2026-08-16T11:00:00Z",
            captured_utc="2026-08-16T11:00:01Z",
            events=[revised],
        )
        self.append_clock_snapshot(
            generated_utc="2026-08-16T12:00:00Z",
            captured_utc="2026-08-16T12:00:01Z",
            events=[],
        )

        reader = self.historical_v1_reader()
        first = reader.as_of(
            dt.datetime.fromisoformat("2026-08-16T10:30:00+00:00")
        )
        second = reader.as_of(
            dt.datetime.fromisoformat("2026-08-16T11:30:00+00:00")
        )
        removed = reader.as_of(
            dt.datetime.fromisoformat("2026-08-16T12:30:00+00:00")
        )

        self.assertEqual(
            "2026-08-20T01:30:00Z",
            first["events"][0]["scheduled_utc"],
        )
        self.assertEqual(
            "2026-08-20T02:00:00Z",
            second["events"][0]["scheduled_utc"],
        )
        self.assertEqual([], removed["events"])
        self.assertTrue(removed["historical_only"])

    def test_cutoff_safe_mutable_fallback_is_explicit_not_historical_proof(self) -> None:
        self.paths().event_preflight_json.write_text(
            json.dumps(
                {
                    "contract_id": "mutable_fixture_v1",
                    "generated_utc": "2026-08-16T10:00:00Z",
                    "events": [
                        {
                            "event_id": "fallback-event",
                            "currency": "AUD",
                            "scheduled_utc": "2026-08-20T01:30:00Z",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        snapshot = OfficialFactAdapter(self.paths()).as_of(
            "2026-08-16T11:00:00Z", currencies=("AUD",)
        )

        provenance = snapshot["event_clock_provenance"]
        self.assertEqual("mutable_current_cutoff_safe_fallback", provenance["state"])
        self.assertTrue(provenance["fallback_used"])
        self.assertFalse(provenance["historical_proof"])
        self.assertEqual("fallback-event", snapshot["upcoming_events"][0]["event_id"])
        self.assertIn(
            "mutable_event_preflight_fallback_used",
            {row["code"] for row in snapshot["global_gaps"]},
        )

    def test_missing_inputs_are_explicit_read_only_and_fail_closed(self) -> None:
        paths = self.paths()
        before = set(self.root.iterdir())
        snapshot = OfficialFactAdapter(paths).as_of(
            "2026-08-16T11:00:00Z", currencies=("AUD", "NZD")
        )
        after = set(self.root.iterdir())

        self.assertEqual(before, after)
        self.assertEqual("degraded", snapshot["status"])
        self.assertTrue(snapshot["research_only"])
        self.assertFalse(snapshot["execution_eligible"])
        self.assertFalse(snapshot["can_place_orders"])
        self.assertEqual("no_trade", snapshot["supported_execution_decision"])
        self.assertGreater(len(snapshot["global_gaps"]), 0)
        for row in snapshot["facts"]:
            self.assertNotIn("direction", row)
            self.assertEqual("abstain", row["direction_policy"])

    def test_snapshot_identity_is_portable_across_restored_paths(self) -> None:
        def restored_paths(base: Path) -> OfficialFactPaths:
            return OfficialFactPaths(
                source_governance_db=base / "source_governance.sqlite",
                macro_surprise_db=base / "macro.sqlite",
                daily_rates_db=base / "rates.sqlite",
                internal_expectations_db=base / "expectations.sqlite",
                policy_baselines_json=base / "policy.json",
                event_preflight_json=base / "events.json",
                immutable_event_clock_db=base / "immutable_event_clock.sqlite",
                source_coverage_json=base / "coverage.json",
                clock_integrity_json=base / "clock.json",
                intraday_rates_config_json=base / "intraday_rates.json",
            )

        for base in (self.root / "first", self.root / "second"):
            base.mkdir()
            (base / "clock.json").write_text(
                json.dumps(
                    {
                        "generated_utc": "2026-08-16T10:00:00Z",
                        "status": "ok",
                        "timestamp_normalization_trusted": True,
                        "host_clock_synchronized": True,
                        "source": str(base / "practice_heartbeat.json"),
                    }
                ),
                encoding="utf-8",
            )
            (base / "policy.json").write_text(
                json.dumps(
                    {
                        "created_utc": "2026-08-16T08:00:00Z",
                        "contract_id": "portable_policy_fixture",
                        "baselines": [
                            {
                                "event_id": "portable-policy",
                                "currency": "AUD",
                                "known_utc": "2026-08-16T09:00:00Z",
                                "detail_available_utc": "2026-08-16T09:05:00Z",
                                "published_utc": "2026-08-16T09:00:00Z",
                                "headline": "Portable policy document",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

        first = OfficialFactAdapter(restored_paths(self.root / "first")).as_of(
            "2026-08-16T11:00:00Z", currencies=("AUD", "NZD")
        )
        second = OfficialFactAdapter(restored_paths(self.root / "second")).as_of(
            "2026-08-16T11:00:00Z", currencies=("AUD", "NZD")
        )
        self.assertEqual(first["snapshot_id"], second["snapshot_id"])
        self.assertEqual(first["global_gaps"], second["global_gaps"])
        self.assertTrue(all("path" not in row for row in first["global_gaps"]))

    def test_naive_cutoff_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            OfficialFactAdapter(self.paths()).as_of("2026-08-16T11:00:00")

    def test_undated_connected_rate_config_cannot_become_evidence(self) -> None:
        self.paths().intraday_rates_config_json.write_text(
            json.dumps({"state": "ready", "currencies": {"AUD": {"instrument": "fixture"}}}),
            encoding="utf-8",
        )
        snapshot = OfficialFactAdapter(self.paths()).as_of(
            "2026-08-16T11:00:00Z", currencies=("AUD",)
        )

        self.assertFalse(snapshot["intraday_rates"]["connected"])
        self.assertIn(
            "intraday_rate_config_not_known_at_cutoff",
            {row["code"] for row in snapshot["global_gaps"]},
        )


if __name__ == "__main__":
    unittest.main()
