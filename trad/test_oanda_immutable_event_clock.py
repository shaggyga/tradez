from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from forex_system.ingestion.immutable_event_clock import (  # noqa: E402
    CONTRACT_ID,
    EventClockLedgerError,
    append_capture_observation,
    append_snapshot,
    append_snapshot_and_observation_atomic,
    build_clock_attestation,
    build_snapshot,
    collect_to_ledger,
    read_stable_source,
    reconstruct_as_of,
)


UTC = dt.timezone.utc


class ImmutableEventClockTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.events = self.root / "events_latest.json"
        self.manifest = self.root / "manifest.json"
        self.database = self.root / "clock.sqlite"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_source(
        self,
        events: list[dict[str, object]],
        generated: str,
        *,
        directory: Path | None = None,
    ) -> tuple[Path, Path]:
        directory = directory or self.root
        directory.mkdir(parents=True, exist_ok=True)
        events_path = directory / "events_latest.json"
        manifest_path = directory / "manifest.json"
        events_path.write_text(json.dumps(events), encoding="utf-8")
        events_sha256 = hashlib.sha256(events_path.read_bytes()).hexdigest()
        manifest_path.write_text(
            json.dumps(
                {
                    "manifest_schema_version": 2,
                    "pipeline_version": "all_pair_news_event_tags_v3",
                    "generated_utc": generated,
                    "event_count": len(events),
                    "events_artifact_name": events_path.name,
                    "events_sha256": events_sha256,
                }
            ),
            encoding="utf-8",
        )
        return events_path, manifest_path

    @staticmethod
    def event(
        event_id: str = "ecb-rate",
        scheduled: str = "2026-08-20T12:15:00+00:00",
        first_known: str = "2026-08-17T00:00:00+00:00",
    ) -> dict[str, object]:
        return {
            "event_id": event_id,
            "scheduled_utc": scheduled,
            "event_utc": scheduled,
            "first_known_utc": first_known,
            "updated_utc": first_known,
            "headline": "ECB policy decision",
            "category": "monetary_policy",
            "source_name": "ECB",
            "source_url": "https://www.ecb.europa.eu/",
            "source_type": "live_news_watch",
            "source_verified": True,
            "source_id": "ecb_official_policy_calendar",
            "source_contract_id": "ecb_official_policy_calendar_v1",
            "source_cohort_id": "ecb_official_policy_calendar_v1",
            "currencies": ["EUR"],
            "direct_currencies": ["EUR"],
            "timing_precision": "minute",
            "clock_semantics": "domestic_official_statistical_release",
            "independent_domestic_event": True,
            "material_content_sha256": "a" * 64,
            "event_time_basis": "scheduled_release",
        }

    @staticmethod
    def huf_v3_event() -> dict[str, object]:
        identity = (
            "hungary_ksh_headline_cpi_release_clock_exact_v3_"
            "verified_policy_bytes_20260817"
        )
        return {
            "event_id": "huf-cpi-clock",
            "scheduled_utc": "2026-09-08T06:30:00+00:00",
            "event_utc": "2026-09-08T06:30:00+00:00",
            "first_known_utc": "2026-08-17T07:00:00+00:00",
            "updated_utc": "2026-08-17T07:00:00+00:00",
            "headline": "Hungary headline consumer price inflation",
            "category": "inflation_context",
            "scope": "currency",
            "source_name": (
                "Hungarian Central Statistical Office headline CPI exact "
                "public-release clock"
            ),
            "source_url": "https://www.ksh.hu/prices?lang=en",
            "source_type": "live_news_watch",
            "source_verified": True,
            "source_id": "hungary_ksh_headline_cpi_release_clock_exact_v3",
            "source_contract_id": identity,
            "source_cohort_id": identity,
            "currencies": ["HUF"],
            "direct_currencies": ["HUF"],
            "direct_currencies_known": True,
            "timing_precision": "minute",
            "clock_semantics": "domestic_official_statistical_release",
            "independent_domestic_event": True,
            "linked_policy_factor": False,
            "material_content_sha256": "a" * 64,
            "release_time_rule_observed_sha256": (
                "617f513efcccfd07fa159b0fdf9fc9e0e3243095e20deb50d88e88495fbd4030"
            ),
            "release_time_rule_archive_name": (
                "ksh_dissemination_policy_2024_617f513efcccfd07.pdf"
            ),
            "release_rule_bytes_verified": True,
            "event_time_basis": "scheduled_release",
            "context_only": True,
            "research_only": True,
            "directional_research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "currency_bias": {},
            "pair_bias": {},
        }

    def test_cutoff_before_capture_cannot_see_current_mutable_source(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        before = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 0, 59, tzinfo=UTC)
        )
        after = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 1, 1, tzinfo=UTC)
        )
        self.assertIsNone(before["snapshot_id"])
        self.assertEqual(after["event_count"], 1)
        self.assertEqual(
            after["events"][0]["raw_event_availability_utc"],
            "2026-08-17T01:00:01+00:00",
        )
        self.assertEqual(after["events"][0]["ledger_effective_known_utc"], "")
        self.assertFalse(after["events"][0]["trusted_for_prospective_evidence"])

    def test_engineering_only_huf_v1_v2_block_whole_snapshot(self) -> None:
        identities = (
            (
                "hungary_ksh_release_calendar_exact_v1",
                "hungary_ksh_release_calendar_exact_v1_0830_policy_20260817",
            ),
            (
                "hungary_ksh_headline_cpi_release_clock_exact_v2",
                "hungary_ksh_headline_cpi_release_clock_exact_v2_content_addressed_20260817",
            ),
        )
        for source_id, identity in identities:
            with self.subTest(source_id=source_id):
                event = {
                    **self.event(),
                    "source_id": source_id,
                    "source_contract_id": identity,
                    "source_cohort_id": identity,
                }
                source = read_stable_source(*self.write_source(
                    [event], "2026-08-17T06:13:00+00:00"
                ))
                with self.assertRaisesRegex(
                    EventClockLedgerError, "engineering_only_source_quarantined"
                ):
                    build_snapshot(
                        source,
                        dt.datetime(2026, 8, 17, 6, 13, 1, tzinfo=UTC),
                    )

    def test_huf_v3_clock_requires_exact_semantics_and_no_direction(self) -> None:
        valid = self.huf_v3_event()
        source = read_stable_source(*self.write_source(
            [valid], "2026-08-17T07:00:00+00:00"
        ))
        snapshot = build_snapshot(
            source,
            dt.datetime(2026, 8, 17, 7, 0, 1, tzinfo=UTC),
        )
        self.assertEqual(snapshot["event_versions"][0]["direct_currencies"], ["HUF"])

        winter = {
            **valid,
            "event_id": "huf-cpi-clock-winter",
            "scheduled_utc": "2027-01-08T07:30:00+00:00",
            "event_utc": "2027-01-08T07:30:00+00:00",
        }
        winter_source = read_stable_source(*self.write_source(
            [winter],
            "2026-08-17T07:00:00+00:00",
            directory=self.root / "huf-winter-valid",
        ))
        winter_snapshot = build_snapshot(
            winter_source,
            dt.datetime(2026, 8, 17, 7, 0, 1, tzinfo=UTC),
        )
        self.assertEqual(
            winter_snapshot["event_versions"][0]["scheduled_utc"],
            "2027-01-08T07:30:00+00:00",
        )

        mutations = [
            {"release_time_rule_archive_name": "anything.pdf"},
            {"source_verified": "0"},
            {"source_type": "curated_seed"},
            {"source_url": "https://www.ksh.hu/industry"},
            {"currencies": ["USD"], "direct_currencies": ["USD"]},
            {"direct_currencies_known": False},
            {"timing_precision": "date_window"},
            {"clock_semantics": "domestic_official_policy_release"},
            {"independent_domestic_event": False},
            {"linked_policy_factor": True},
            {"event_time_basis": "published"},
            {"headline": "Impersonating release"},
            {"material_content_sha256": "not-a-hash"},
            {"context_only": False},
            {"directional_research_only": False},
            {"execution_eligible": True},
            {"currency_bias": {"HUF": "BULLISH"}},
            {"event_utc": "2026-09-08T06:31:00+00:00"},
        ]
        for index, mutation in enumerate(mutations):
            with self.subTest(mutation=mutation):
                row = {**valid, **mutation}
                events, manifest = self.write_source(
                    [row],
                    "2026-08-17T07:00:00+00:00",
                    directory=self.root / f"huf-mutation-{index}",
                )
                source = read_stable_source(events, manifest)
                with self.assertRaisesRegex(
                    EventClockLedgerError,
                    "huf_ksh_(verified_rule_provenance_missing|event_clock_mismatch|local_release_time_mismatch)",
                ):
                    build_snapshot(
                        source,
                        dt.datetime(2026, 8, 17, 7, 0, 1, tzinfo=UTC),
                    )

    def test_revisions_and_removals_reconstruct_from_snapshot_membership(self) -> None:
        original = self.event(scheduled="2026-08-20T12:15:00+00:00")
        self.write_source([original], "2026-08-17T01:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        revised = self.event(scheduled="2026-08-20T12:45:00+00:00")
        self.write_source([revised], "2026-08-17T02:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 2, 0, 1, tzinfo=UTC),
        )
        self.write_source([], "2026-08-17T03:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 3, 0, 1, tzinfo=UTC),
        )
        at_one = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 1, 30, tzinfo=UTC)
        )
        at_two = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 2, 30, tzinfo=UTC)
        )
        at_three = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 3, 30, tzinfo=UTC)
        )
        self.assertEqual(at_one["events"][0]["scheduled_utc"], original["scheduled_utc"])
        self.assertEqual(at_two["events"][0]["scheduled_utc"], revised["scheduled_utc"])
        self.assertEqual(at_three["event_count"], 0)

    def test_new_source_contract_cannot_be_reconstructed_before_first_snapshot(self) -> None:
        old = {
            **self.event(first_known="2026-08-17T00:00:00+00:00"),
            "source_id": "denmark_statistics_release_calendar_exact_v1",
            "source_contract_id": "denmark_exact_v1",
            "source_cohort_id": "denmark_exact_v1",
        }
        self.write_source([old], "2026-08-17T01:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        upgraded = {
            **old,
            "source_contract_id": "denmark_exact_v2_paged",
            "source_cohort_id": "denmark_exact_v2_paged",
        }
        self.write_source([upgraded], "2026-08-17T02:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 2, 0, 1, tzinfo=UTC),
        )
        before = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 1, 59, 59, tzinfo=UTC)
        )
        after = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 2, 0, 2, tzinfo=UTC)
        )
        self.assertEqual(before["events"][0]["source_contract_id"], "denmark_exact_v1")
        self.assertNotEqual(
            before["events"][0]["source_contract_id"], "denmark_exact_v2_paged"
        )
        event = after["events"][0]
        self.assertEqual(event["source_contract_id"], "denmark_exact_v2_paged")
        self.assertEqual(event["original_fact_known_utc"], "2026-08-17T00:00:00+00:00")
        self.assertEqual(
            event["source_contract_first_observed_utc"],
            "2026-08-17T02:00:01+00:00",
        )
        self.assertEqual(event["raw_event_availability_utc"], "2026-08-17T02:00:01+00:00")
        self.assertEqual(event["event_availability_utc"], "")
        self.assertEqual(
            event["ledger_effective_known_utc"], ""
        )

    def test_unchanged_event_retains_first_snapshot_clock_when_catalog_grows(self) -> None:
        first = self.event(first_known="2026-08-17T00:00:00+00:00")
        self.write_source([first], "2026-08-17T01:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        second = self.event("second", "2026-08-21T12:15:00+00:00")
        self.write_source([first, second], "2026-08-17T02:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 2, 0, 1, tzinfo=UTC),
        )
        reconstructed = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 2, 0, 2, tzinfo=UTC)
        )
        retained = next(
            row
            for row in reconstructed["events"]
            if row["upstream_event_id"] == first["event_id"]
        )
        self.assertEqual(
            retained["event_snapshot_first_observed_utc"],
            "2026-08-17T01:00:01+00:00",
        )
        self.assertEqual(
            retained["raw_event_availability_utc"], "2026-08-17T01:00:01+00:00"
        )
        self.assertEqual(retained["event_availability_utc"], "")

    def test_upstream_future_known_time_remains_invisible(self) -> None:
        future_known = self.event(first_known="2026-08-17T04:00:00+00:00")
        self.write_source([future_known], "2026-08-17T01:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        hidden = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 3, 0, tzinfo=UTC)
        )
        visible = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 4, 1, tzinfo=UTC)
        )
        self.assertEqual(hidden["event_count"], 0)
        self.assertEqual(visible["event_count"], 1)

    def test_database_tables_reject_update_and_delete(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        with closing(sqlite3.connect(self.database)) as connection:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append_only_ledger"):
                connection.execute(
                    "UPDATE event_clock_snapshots SET contract_id='changed'"
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append_only_ledger"):
                connection.execute("DELETE FROM event_clock_versions")
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append_only_ledger"):
                connection.execute("DELETE FROM event_clock_observations")

    def test_snapshot_identity_is_path_and_ingestion_order_independent(self) -> None:
        rows = [self.event("a"), self.event("b", "2026-08-21T12:15:00+00:00")]
        first_events, first_manifest = self.write_source(
            rows, "2026-08-17T01:00:00+00:00", directory=self.root / "one"
        )
        second_events, second_manifest = self.write_source(
            list(reversed(rows)),
            "2026-08-17T01:00:00+00:00",
            directory=self.root / "two",
        )
        first_source = read_stable_source(first_events, first_manifest)
        second_source = read_stable_source(second_events, second_manifest)
        captured = dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC)
        first_snapshot = build_snapshot(first_source, captured)
        second_snapshot = build_snapshot(second_source, captured)
        self.assertEqual(
            [row["event_version_id"] for row in first_snapshot["event_versions"]],
            [row["event_version_id"] for row in second_snapshot["event_versions"]],
        )
        # Source byte hashes intentionally differ, so snapshot IDs differ;
        # semantic event-version identities remain portable and order-stable.
        self.assertNotEqual(first_snapshot["snapshot_id"], second_snapshot["snapshot_id"])
        self.assertEqual(first_snapshot["contract_id"], CONTRACT_ID)

    def test_source_generated_after_capture_is_rejected(self) -> None:
        self.write_source([self.event()], "2026-08-17T02:00:00+00:00")
        source = read_stable_source(self.events, self.manifest)
        with self.assertRaisesRegex(EventClockLedgerError, "source_generated_after_capture"):
            build_snapshot(
                source, dt.datetime(2026, 8, 17, 1, 0, tzinfo=UTC)
            )

    def test_append_rejects_source_generated_after_capture(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:30+00:00")
        source = read_stable_source(self.events, self.manifest)
        snapshot = build_snapshot(
            source, dt.datetime(2026, 8, 17, 1, 0, 31, tzinfo=UTC)
        )
        backdated = {
            **snapshot,
            "captured_utc": "2026-08-17T01:00:00+00:00",
        }
        for writer in (append_snapshot, append_snapshot_and_observation_atomic):
            with self.subTest(writer=writer.__name__):
                with self.assertRaisesRegex(
                    EventClockLedgerError,
                    "snapshot_source_generated_after_capture",
                ):
                    writer(self.database, backdated)

    def test_observation_cannot_backdate_referenced_snapshot(self) -> None:
        self.write_source([self.event()], "2026-08-17T02:00:00+00:00")
        later_source = read_stable_source(self.events, self.manifest)
        later = build_snapshot(
            later_source, dt.datetime(2026, 8, 17, 2, 0, 1, tzinfo=UTC)
        )
        append_snapshot(self.database, later)

        self.write_source([self.event()], "2026-08-17T00:30:00+00:00")
        earlier_source = read_stable_source(self.events, self.manifest)
        earlier_attempt = build_snapshot(
            earlier_source, dt.datetime(2026, 8, 17, 1, 0, 0, tzinfo=UTC)
        )
        for writer in (
            lambda: append_capture_observation(
                self.database,
                snapshot_id=later["snapshot_id"],
                attempted_snapshot=earlier_attempt,
            ),
            lambda: append_snapshot_and_observation_atomic(
                self.database, earlier_attempt
            ),
        ):
            with self.subTest(writer=writer):
                with self.assertRaisesRegex(
                    EventClockLedgerError,
                    "observation_precedes_referenced_snapshot_capture",
                ):
                    writer()

    def test_reconstruction_rejects_observation_with_missing_snapshot(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        attestation = {
            "state": "unattested_not_provided",
            "attested": False,
            "artifact_name": None,
            "generated_utc": None,
            "age_at_capture_sec": None,
            "payload_sha256": None,
            "maximum_age_sec": 300.0,
        }
        material = {
            "snapshot_id": "missing_snapshot",
            "observed_utc": "2026-08-17T02:00:00+00:00",
            "source_generated_utc": "2026-08-17T01:59:59+00:00",
            "semantic_clock_sha256": "a" * 64,
            "events_sha256": "b" * 64,
            "manifest_sha256": "c" * 64,
            "clock_attestation": attestation,
        }
        canonical = json.dumps(material, sort_keys=True, separators=(",", ":"))
        observation_id = "event_clock_observation_" + hashlib.sha256(
            canonical.encode("utf-8")
        ).hexdigest()[:32]
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute("PRAGMA foreign_keys=OFF")
            connection.execute(
                """INSERT INTO event_clock_observations(
                       observation_id,snapshot_id,observed_utc,source_generated_utc,
                       semantic_clock_sha256,events_sha256,manifest_sha256,
                       attestation_state,attested,attestation_payload_sha256,
                       observation_json
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    observation_id,
                    "missing_snapshot",
                    material["observed_utc"],
                    material["source_generated_utc"],
                    material["semantic_clock_sha256"],
                    material["events_sha256"],
                    material["manifest_sha256"],
                    attestation["state"],
                    0,
                    None,
                    canonical,
                ),
            )
            connection.commit()
        with self.assertRaisesRegex(
            EventClockLedgerError, "observation_referenced_snapshot_missing"
        ):
            reconstruct_as_of(
                self.database, dt.datetime(2026, 8, 17, 2, 0, 1, tzinfo=UTC)
            )

    def test_manifest_must_bind_exact_events_bytes_and_pipeline(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        manifest = json.loads(self.manifest.read_text(encoding="utf-8"))
        manifest["events_sha256"] = "0" * 64
        self.manifest.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(
            EventClockLedgerError, "manifest_events_sha256_mismatch"
        ):
            read_stable_source(self.events, self.manifest)

        manifest["events_sha256"] = hashlib.sha256(self.events.read_bytes()).hexdigest()
        manifest["pipeline_version"] = "WRONG_PIPELINE"
        self.manifest.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(
            EventClockLedgerError, "source_pipeline_version_mismatch"
        ):
            read_stable_source(self.events, self.manifest)

    def test_build_and_append_reject_wrong_source_pipeline(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        source = read_stable_source(self.events, self.manifest)
        wrong_source = replace(source, source_pipeline_version="WRONG_PIPELINE")
        with self.assertRaisesRegex(
            EventClockLedgerError, "source_pipeline_version_mismatch"
        ):
            build_snapshot(
                wrong_source, dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC)
            )

        snapshot = build_snapshot(
            source, dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC)
        )
        wrong_snapshot = {**snapshot, "source_pipeline_version": "WRONG_PIPELINE"}
        with self.assertRaisesRegex(
            EventClockLedgerError, "snapshot_pipeline_version_mismatch"
        ):
            append_snapshot(self.database, wrong_snapshot)

    def test_reconstruction_rejects_wrong_pipeline_in_history(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        source = read_stable_source(self.events, self.manifest)
        first = build_snapshot(
            source, dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC)
        )
        second = build_snapshot(
            source, dt.datetime(2026, 8, 17, 2, 0, 1, tzinfo=UTC)
        )
        append_snapshot(self.database, first)
        append_snapshot(self.database, second)

        # Model a pre-fix ledger containing a semantically valid but
        # unsupported historical producer.  The latest selected snapshot is
        # still correct, so reconstruction must validate every historical
        # contributor rather than only the selected row.
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "DROP TRIGGER immutable_event_clock_snapshots_update"
            )
            payload = json.loads(
                connection.execute(
                    "SELECT snapshot_json FROM event_clock_snapshots WHERE snapshot_id=?",
                    (first["snapshot_id"],),
                ).fetchone()[0]
            )
            payload["source_pipeline_version"] = "WRONG_PIPELINE"
            connection.execute(
                """UPDATE event_clock_snapshots
                   SET source_pipeline_version=?, snapshot_json=?
                   WHERE snapshot_id=?""",
                (
                    "WRONG_PIPELINE",
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                    first["snapshot_id"],
                ),
            )
            connection.commit()

        with self.assertRaisesRegex(
            EventClockLedgerError,
            "stored_history_snapshot_pipeline_version_mismatch",
        ):
            reconstruct_as_of(
                self.database, dt.datetime(2026, 8, 17, 2, 0, 2, tzinfo=UTC)
            )

    def test_v2_writes_cannot_mutate_legacy_v1_database(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        source = read_stable_source(self.events, self.manifest)
        legacy = build_snapshot(
            source, dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC)
        )
        append_snapshot(self.database, legacy)
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "DROP TRIGGER immutable_event_clock_snapshots_update"
            )
            payload = json.loads(
                connection.execute(
                    "SELECT snapshot_json FROM event_clock_snapshots"
                ).fetchone()[0]
            )
            payload["schema_version"] = "immutable_event_clock_ledger_v1"
            payload["contract_id"] = "immutable_point_in_time_event_clock_v1_20260817"
            connection.execute(
                """UPDATE event_clock_snapshots
                   SET schema_version=?,contract_id=?,snapshot_json=?""",
                (
                    payload["schema_version"],
                    payload["contract_id"],
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                ),
            )
            connection.execute("DROP TABLE event_clock_ledger_identity")
            connection.commit()

        candidate = build_snapshot(
            source, dt.datetime(2026, 8, 17, 2, 0, 1, tzinfo=UTC)
        )
        before = hashlib.sha256(self.database.read_bytes()).hexdigest()
        for writer in (append_snapshot, append_snapshot_and_observation_atomic):
            with self.subTest(writer=writer.__name__):
                with self.assertRaisesRegex(
                    EventClockLedgerError, "existing_ledger_identity_mismatch"
                ):
                    writer(self.database, candidate)
                self.assertEqual(
                    before, hashlib.sha256(self.database.read_bytes()).hexdigest()
                )

    def test_forged_fresh_attestation_is_rejected_before_append(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        source = read_stable_source(self.events, self.manifest)
        forged = {
            "state": "fresh_trusted",
            "attested": True,
            "artifact_name": "forged.json",
            "generated_utc": "2026-08-17T01:00:00+00:00",
            "age_at_capture_sec": 1.0,
            "maximum_age_sec": 300.0,
            "payload_sha256": "f" * 64,
            "status": "not_ok",
            "timestamp_normalization_trusted": True,
            "host_clock_synchronized": True,
            "source_fresh": True,
        }
        with self.assertRaisesRegex(
            EventClockLedgerError,
            "snapshot_clock_attestation_trust_semantics_invalid",
        ):
            build_snapshot(
                source,
                dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
                clock_attestation=forged,
            )

        inflated_maximum = {
            **forged,
            "generated_utc": "1999-01-01T00:00:00+00:00",
            "age_at_capture_sec": 871779601.0,
            "maximum_age_sec": 1_000_000_000.0,
            "status": "ok",
        }
        with self.assertRaisesRegex(
            EventClockLedgerError,
            "snapshot_clock_attestation_age_semantics_invalid",
        ):
            build_snapshot(
                source,
                dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
                clock_attestation=inflated_maximum,
            )

    def test_clock_attestation_does_not_coerce_truthy_non_boolean_flags(self) -> None:
        integrity = self.root / "clock_integrity.json"
        integrity.write_text(
            json.dumps(
                {
                    "generated_utc": "2026-08-17T01:00:00+00:00",
                    "status": "ok",
                    "timestamp_normalization_trusted": "false",
                    "host_clock_synchronized": 1,
                    "source_fresh": "true",
                }
            ),
            encoding="utf-8",
        )

        attestation = build_clock_attestation(
            integrity,
            dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )

        self.assertEqual(attestation["state"], "unattested_not_trusted")
        self.assertIs(attestation["attested"], False)
        self.assertIs(attestation["timestamp_normalization_trusted"], False)
        self.assertIs(attestation["host_clock_synchronized"], False)
        self.assertIs(attestation["source_fresh"], False)

    def test_exact_source_semantics_survive_immutable_round_trip(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        event = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 1, 0, 2, tzinfo=UTC)
        )["events"][0]
        self.assertEqual(event["timing_precision"], "minute")
        self.assertEqual(
            event["clock_semantics"], "domestic_official_statistical_release"
        )
        self.assertIs(event["independent_domestic_event"], True)
        self.assertEqual(event["material_content_sha256"], "a" * 64)

    def test_malformed_purported_scheduled_clock_blocks_whole_capture(self) -> None:
        malformed = {**self.event(), "scheduled_utc": "not-a-clock"}
        self.write_source([self.event("valid"), malformed], "2026-08-17T01:00:00+00:00")
        with self.assertRaisesRegex(EventClockLedgerError, "malformed_scheduled_clock"):
            collect_to_ledger(
                events_path=self.events,
                manifest_path=self.manifest,
                database_path=self.database,
                captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
            )
        self.assertFalse(self.database.exists())

    def test_reconstruction_recomputes_event_content_address(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute("DROP TRIGGER immutable_event_clock_versions_update")
            raw = json.loads(
                connection.execute(
                    "SELECT event_json FROM event_clock_versions"
                ).fetchone()[0]
            )
            raw["headline"] = "tampered without changing the content address"
            connection.execute(
                "UPDATE event_clock_versions SET event_json=?",
                (json.dumps(raw, separators=(",", ":"), sort_keys=True),),
            )
            connection.commit()
        with self.assertRaisesRegex(
            EventClockLedgerError, "stored_event_(clock_semantic|version)_id_mismatch"
        ):
            reconstruct_as_of(
                self.database, dt.datetime(2026, 8, 17, 1, 0, 2, tzinfo=UTC)
            )

    def test_reconstruction_rejects_membership_closure_tamper(self) -> None:
        mutations = {
            "missing": (
                "DROP TRIGGER immutable_snapshot_events_delete",
                "DELETE FROM snapshot_events WHERE ordinal=0",
            ),
            "extra": (
                None,
                """INSERT INTO snapshot_events(
                       snapshot_id,event_version_id,ordinal,effective_known_utc
                   ) SELECT snapshot_id,'orphan_version',99,effective_known_utc
                     FROM snapshot_events LIMIT 1""",
            ),
            "reordered": (
                "DROP TRIGGER immutable_snapshot_events_update",
                "UPDATE snapshot_events SET ordinal=ordinal+7 WHERE ordinal=0",
            ),
            "backdated": (
                "DROP TRIGGER immutable_snapshot_events_update",
                """UPDATE snapshot_events
                   SET effective_known_utc='2026-08-16T00:00:00+00:00'
                   WHERE ordinal=0""",
            ),
        }
        for name, (trigger_sql, mutation_sql) in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events, manifest = self.write_source(
                    [self.event("a"), self.event("b", "2026-08-21T12:15:00+00:00")],
                    "2026-08-17T01:00:00+00:00",
                    directory=root,
                )
                database = root / "clock.sqlite"
                collect_to_ledger(
                    events_path=events,
                    manifest_path=manifest,
                    database_path=database,
                    captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
                )
                with closing(sqlite3.connect(database)) as connection:
                    if trigger_sql:
                        connection.execute(trigger_sql)
                    connection.execute(mutation_sql)
                    connection.commit()
                with self.assertRaisesRegex(
                    EventClockLedgerError, "stored_snapshot_membership_"
                ):
                    reconstruct_as_of(
                        database, dt.datetime(2026, 8, 17, 1, 0, 2, tzinfo=UTC)
                    )

    def test_v2_reader_rejects_legacy_v1_snapshot_rows(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute("DROP TRIGGER immutable_event_clock_snapshots_update")
            payload = json.loads(
                connection.execute(
                    "SELECT snapshot_json FROM event_clock_snapshots"
                ).fetchone()[0]
            )
            payload["schema_version"] = "immutable_event_clock_ledger_v1"
            payload["contract_id"] = "immutable_point_in_time_event_clock_v1_20260817"
            connection.execute(
                """UPDATE event_clock_snapshots
                   SET schema_version=?,contract_id=?,snapshot_json=?""",
                (
                    payload["schema_version"],
                    payload["contract_id"],
                    json.dumps(payload, separators=(",", ":"), sort_keys=True),
                ),
            )
            connection.commit()
        with self.assertRaisesRegex(
            EventClockLedgerError, "existing_ledger_identity_mismatch"
        ):
            reconstruct_as_of(
                self.database, dt.datetime(2026, 8, 17, 1, 0, 2, tzinfo=UTC)
            )

    def test_same_snapshot_is_idempotent(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        source = read_stable_source(self.events, self.manifest)
        snapshot = build_snapshot(
            source, dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC)
        )
        first = append_snapshot(self.database, snapshot)
        second = append_snapshot(self.database, snapshot)
        self.assertEqual(first["status"], "appended")
        self.assertEqual(second["status"], "already_present")

    def test_semantically_unchanged_calendar_does_not_grow_ledger(self) -> None:
        original = self.event()
        self.write_source([original], "2026-08-17T01:00:00+00:00")
        first = collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        refreshed = {**original, "updated_utc": "2026-08-17T02:00:00+00:00"}
        self.write_source([refreshed], "2026-08-17T02:00:00+00:00")
        second = collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 2, 0, 1, tzinfo=UTC),
        )
        with closing(sqlite3.connect(self.database)) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM event_clock_snapshots"
            ).fetchone()[0]
            observation_count = connection.execute(
                "SELECT COUNT(*) FROM event_clock_observations"
            ).fetchone()[0]
        reconstructed = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 2, 0, 2, tzinfo=UTC)
        )
        self.assertEqual("appended", first["status"])
        self.assertEqual("semantically_unchanged", second["status"])
        self.assertEqual(1, count)
        self.assertEqual(2, observation_count)
        self.assertEqual(second["clock_observation_id"], reconstructed["clock_observation_id"])
        self.assertEqual("2026-08-17T02:00:01+00:00", reconstructed["clock_observed_utc"])
        # A semantically unchanged reread keeps the original immutable
        # snapshot but exposes the latest source-publication hashes.  Both are
        # required for downstream dual-freshness lineage.
        self.assertNotEqual(
            reconstructed["semantic_snapshot_events_sha256"],
            reconstructed["clock_observation_events_sha256"],
        )
        self.assertEqual(
            64, len(reconstructed["clock_observation_semantic_clock_sha256"])
        )

    def test_fresh_clock_attestation_can_supersede_unattested_same_calendar(self) -> None:
        self.write_source([self.event()], "2026-08-17T01:00:00+00:00")
        collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 0, 1, tzinfo=UTC),
        )
        clock = self.root / "clock_integrity_v1.json"
        clock.write_text(
            json.dumps(
                {
                    "generated_utc": "2026-08-17T01:04:59+00:00",
                    "status": "ok",
                    "timestamp_normalization_trusted": True,
                    "host_clock_synchronized": True,
                    "source_fresh": True,
                }
            ),
            encoding="utf-8",
        )
        second = collect_to_ledger(
            events_path=self.events,
            manifest_path=self.manifest,
            database_path=self.database,
            captured_utc=dt.datetime(2026, 8, 17, 1, 5, 0, tzinfo=UTC),
            clock_integrity_path=clock,
        )
        reconstructed = reconstruct_as_of(
            self.database, dt.datetime(2026, 8, 17, 1, 5, 1, tzinfo=UTC)
        )
        self.assertEqual("appended", second["status"])
        self.assertTrue(second["clock_attested"])
        self.assertEqual(
            "fresh_trusted", reconstructed["clock_attestation"]["state"]
        )
        self.assertTrue(reconstructed["clock_attestation"]["attested"])
        event = reconstructed["events"][0]
        self.assertEqual(
            "2026-08-17T01:00:01+00:00",
            event["event_snapshot_first_observed_utc"],
        )
        self.assertEqual(
            "2026-08-17T01:05:00+00:00",
            event["event_snapshot_first_trusted_observed_utc"],
        )
        self.assertEqual(
            "2026-08-17T01:05:00+00:00", event["event_availability_utc"]
        )
        self.assertTrue(event["trusted_for_prospective_evidence"])


if __name__ == "__main__":
    unittest.main()
