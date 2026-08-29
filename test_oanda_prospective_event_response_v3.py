from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from forex_system.ingestion.prospective_event_response_v3 import (  # noqa: E402
    ProspectiveEventResponseV3Error,
    append_cycle,
    build_event_plans,
    build_samples,
    collection_workset,
    insert_records,
    load_contract,
    mature_outcome_rows,
    open_ledger,
    quote_snapshot_candidates,
    read_evidence,
    register_frozen_cohort,
    verify_ledger,
)
from forex_system.ingestion.immutable_event_clock import collect_to_ledger  # noqa: E402
from oanda_prospective_event_response_v3 import (  # noqa: E402
    SingleWriterLock,
    _coverage,
    record_active_preflight_failure,
    run_once,
)


UTC = dt.timezone.utc
CONTRACT_PATH = ROOT / "config" / "prospective_event_response_capture_v3.json"
DEPENDENCY_PATH = ROOT / "config" / "currency_policy_dependency_registry_v1.json"


def stamp(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def rehash(row: dict) -> dict:
    output = dict(row)
    material = {
        key: value
        for key, value in output.items()
        if key not in {"payload_json", "payload_sha256"}
    }
    output["payload_json"] = json.dumps(
        material, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    output["payload_sha256"] = hashlib.sha256(
        output["payload_json"].encode("utf-8")
    ).hexdigest()
    return output


class ProspectiveEventResponseV3Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)
        self.contract = copy.deepcopy(self.contract)
        self.contract["cohort_id"] = "test_v3_cohort"
        self.contract["cohort_start_utc"] = "2026-08-17T04:00:00+00:00"
        self.dependencies = json.loads(DEPENDENCY_PATH.read_text(encoding="utf-8"))
        self.planned = stamp("2026-08-17T04:00:00Z")
        self.scheduled = stamp("2026-08-17T05:00:00Z")

    def event(self, **overrides):
        event = {
            "event_version_id": "clock_version_one",
            "upstream_event_id": "official-fomc-release",
            "scheduled_utc": self.scheduled.isoformat(),
            "ledger_effective_known_utc": "2026-08-17T04:00:00+00:00",
            "original_fact_known_utc": "2026-08-17T03:45:00+00:00",
            "event_snapshot_first_observed_utc": "2026-08-17T04:00:00+00:00",
            "event_snapshot_first_trusted_observed_utc": "2026-08-17T04:00:00+00:00",
            "source_id": "fomc_policy_calendar",
            "source_contract_id": "fomc_policy_calendar_v1",
            "source_cohort_id": "fomc_policy_calendar_v1",
            "source_contract_first_observed_utc": "2026-08-17T04:00:00+00:00",
            "source_contract_first_trusted_observed_utc": "2026-08-17T04:00:00+00:00",
            "event_availability_utc": "2026-08-17T04:00:00+00:00",
            "material_content_sha256": "f" * 64,
            "timing_precision": "minute",
            "clock_semantics": "domestic_official_policy_release",
            "independent_domestic_event": True,
            "linked_policy_factor": False,
            "headline": "FOMC monetary policy decision",
            "category": "monetary_policy",
            "source_name": "Federal Reserve",
            "source_url": "https://federalreserve.gov/example",
            "source_verified": True,
            "direct_currencies": ["USD"],
            "currencies": ["USD"],
            "event_time_basis": "scheduled_release",
            "schedule_window_end_utc": "",
        }
        event.update(overrides)
        return event

    def test_contract_freezes_append_only_event_admission_semantics(self):
        self.assertFalse(self.contract["event_catalog_frozen_at_cohort_start"])
        self.assertEqual(
            self.contract["event_instance_admission"],
            "append_only_from_each_new_immutable_semantic_snapshot",
        )
        self.assertTrue(self.contract["admit_previously_unseen_future_exact_events"])
        self.assertTrue(self.contract["new_event_requires_pre_release_observation"])
        self.assertTrue(self.contract["never_reclassify_or_backfill_prior_event_instances"])

        invalid = copy.deepcopy(self.contract)
        invalid["event_catalog_frozen_at_cohort_start"] = True
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bad_contract.json"
            path.write_text(json.dumps(invalid), encoding="utf-8")
            with self.assertRaisesRegex(
                ProspectiveEventResponseV3Error,
                "event_catalog_must_remain_append_admissible",
            ):
                load_contract(path)

    def test_huf_statistics_clock_is_distinct_from_policy_clock_gap(self):
        event = self.event(
            source_id="hungary_ksh_headline_cpi_release_clock_exact_v3",
            source_contract_id=(
                "hungary_ksh_headline_cpi_release_clock_exact_v3_"
                "verified_policy_bytes_20260817"
            ),
            clock_semantics="domestic_official_statistical_release",
            direct_currencies=["HUF"],
            currencies=["HUF"],
        )
        coverage = _coverage(
            contract=self.contract,
            plans=[{
                "instrument": "EUR_HUF",
                "direct_currencies_json": '["HUF"]',
                "linked_policy_dependencies_json": "[]",
            }],
            clock_snapshot={"events": [event]},
        )
        self.assertTrue(coverage["huf_exact_statistics_clock_present"])
        self.assertFalse(coverage["huf_exact_policy_clock_present"])
        self.assertTrue(coverage["huf_policy_exact_clock_gap_preserved"])
        self.assertNotIn("huf_exact_clock_gap_preserved", coverage)

    def test_huf_date_window_policy_semantics_do_not_claim_exact_clock(self):
        event = self.event(
            source_id="mnb_policy_calendar_date_window",
            source_contract_id="mnb_policy_calendar_date_window_v1",
            clock_semantics="domestic_official_policy_release",
            timing_precision="date_window",
            event_time_basis="scheduled_release",
            independent_domestic_event=True,
            linked_policy_factor=False,
            direct_currencies=["HUF"],
            currencies=["HUF"],
        )
        coverage = _coverage(
            contract=self.contract,
            plans=[{
                "instrument": "EUR_HUF",
                "direct_currencies_json": '["HUF"]',
                "linked_policy_dependencies_json": "[]",
            }],
            clock_snapshot={"events": [event]},
        )
        self.assertFalse(coverage["huf_exact_policy_clock_present"])
        self.assertTrue(coverage["huf_policy_exact_clock_gap_preserved"])

    def clock(
        self,
        *,
        at: str,
        events=None,
        source_generated: str | None = None,
        attestation_generated: str | None = None,
    ):
        observed = stamp(at)
        source = stamp(source_generated) if source_generated else observed - dt.timedelta(seconds=5)
        attested = (
            stamp(attestation_generated)
            if attestation_generated
            else observed - dt.timedelta(seconds=2)
        )
        return {
            "contract_id": "immutable_point_in_time_event_clock_v2_20260817",
            "source_pipeline_version": "all_pair_news_event_tags_v3",
            "snapshot_id": "clock_snapshot_one",
            "snapshot_captured_utc": observed.isoformat(),
            "clock_observation_id": "clock_observation_" + at.replace(":", ""),
            "clock_observed_utc": observed.isoformat(),
            "clock_observation_source_generated_utc": source.isoformat(),
            "semantic_snapshot_events_sha256": "a" * 64,
            "semantic_snapshot_manifest_sha256": "b" * 64,
            "clock_observation_events_sha256": "c" * 64,
            "clock_observation_manifest_sha256": "d" * 64,
            "clock_attestation": {
                "state": "fresh_trusted",
                "attested": True,
                "artifact_name": "clock_integrity_v1.json",
                "generated_utc": attested.isoformat(),
                "age_at_capture_sec": (observed - attested).total_seconds(),
                "payload_sha256": "e" * 64,
            },
            "events": [self.event()] if events is None else events,
        }

    def build(self):
        return build_event_plans(
            self.clock(at="2026-08-17T03:59:55Z"),
            contract=self.contract,
            instruments=["EUR_USD", "EUR_HKD", "AUD_NZD"],
            policy_dependency_registry=self.dependencies,
            planned_utc=self.planned,
        )

    def plan(self, instrument: str, offset: int):
        return next(
            row
            for row in self.build()["plans"]
            if row["instrument"] == instrument and row["target_offset_sec"] == offset
        )

    def quote(
        self,
        instrument="EUR_USD",
        quote_time="2026-08-17T05:00:00.500000Z",
        collector="2026-08-17T05:00:01Z",
        bid=1.1000,
        ask=1.1002,
    ):
        collector_time = stamp(collector)
        attestation_generated = collector_time - dt.timedelta(seconds=1)
        payload = {
            "schema_version": 2,
            "producer": "practice_007_fast_executor_price_stream",
            "coverage": {"execution_requires_independent_freshness_check": True},
            "generated_utc": collector_time.isoformat(),
            "quotes": {
                instrument: {
                    "bid": bid,
                    "ask": ask,
                    "pip": 0.0001,
                    "source": "stream",
                    "time": stamp(quote_time).isoformat(),
                }
            },
        }
        return quote_snapshot_candidates(
            payload,
            contract=self.contract,
            artifact_sha256="q" * 64,
            collector_observed_utc=collector_time,
            market_clock_attestation={
                "state": "fresh_trusted",
                "attested": True,
                "artifact_name": "clock_integrity_v1.json",
                "generated_utc": attestation_generated.isoformat(),
                "age_at_capture_sec": 1.0,
                "payload_sha256": "m" * 64,
            },
        )

    def capture(self, plan, observations, at, existing=(), events=None):
        return build_samples(
            [plan],
            observations,
            existing_samples=list(existing),
            current_clock_snapshot=self.clock(at=at, events=events),
            contract=self.contract,
            collector_observed_utc=stamp(at),
        )

    def test_exact_official_source_semantics_fail_closed(self):
        cases = (
            {"timing_precision": ""},
            {"clock_semantics": "linked_policy_dependency"},
            {"independent_domestic_event": False},
            {"material_content_sha256": ""},
        )
        for override in cases:
            with self.subTest(override=override):
                built = build_event_plans(
                    self.clock(
                        at="2026-08-17T03:59:55Z",
                        events=[self.event(**override)],
                    ),
                    contract=self.contract,
                    instruments=["EUR_USD", "EUR_HKD", "AUD_NZD"],
                    policy_dependency_registry=self.dependencies,
                    planned_utc=self.planned,
                )
                self.assertEqual(built["plan_count"], 0)
                self.assertTrue(
                    any(
                        row["reason"] == "exact_official_event_semantics_missing"
                        for row in built["attempts"]
                    )
                )

    def test_event_source_pipeline_must_match_frozen_producer(self):
        snapshot = self.clock(at="2026-08-17T03:59:55Z")
        snapshot["source_pipeline_version"] = "WRONG_PIPELINE"
        with self.assertRaisesRegex(
            ProspectiveEventResponseV3Error,
            "event_source_pipeline_version_mismatch",
        ):
            build_event_plans(
                snapshot,
                contract=self.contract,
                instruments=["EUR_USD"],
                policy_dependency_registry=self.dependencies,
                planned_utc=self.planned,
            )

    def test_whole_universe_relationships_and_shared_hkd_factor(self):
        built = self.build()
        self.assertEqual(built["plan_count"], 3 * 11)
        self.assertEqual(
            built["relationship_counts"],
            {"direct_leg": 11, "linked_policy_dependency": 11, "unaffected_control": 11},
        )
        direct = self.plan("EUR_USD", 0)
        linked = self.plan("EUR_HKD", 0)
        control = self.plan("AUD_NZD", 0)
        self.assertEqual(direct["relationship"], "direct_leg")
        self.assertEqual(linked["relationship"], "linked_policy_dependency")
        self.assertEqual(control["relationship"], "unaffected_control")
        self.assertEqual(direct["underlying_factor_id"], linked["underlying_factor_id"])
        self.assertEqual(linked["independent_confirmation_eligible"], 0)
        self.assertEqual(linked["direction_assigned"], 0)
        self.assertEqual(
            direct["event_availability_utc"], "2026-08-17T04:00:00+00:00"
        )
        self.assertEqual(direct["original_fact_known_utc"], "2026-08-17T03:45:00+00:00")
        self.assertEqual(direct["source_contract_id"], "fomc_policy_calendar_v1")
        self.assertTrue(
            all(
                row["source_pipeline_version"] == "all_pair_news_event_tags_v3"
                for row in built["plans"]
            )
        )

    def test_event_availability_must_equal_maximum_causal_lineage_clock(self):
        malformed = self.event(
            source_contract_first_observed_utc="2026-08-17T04:00:01+00:00",
            source_contract_first_trusted_observed_utc="2026-08-17T04:00:01+00:00",
            event_availability_utc="2026-08-17T04:00:00+00:00",
            ledger_effective_known_utc="2026-08-17T04:00:00+00:00",
        )
        planned = stamp("2026-08-17T04:00:02Z")
        with self.assertRaisesRegex(
            ProspectiveEventResponseV3Error,
            "event_availability_not_maximum_causal_clock",
        ):
            build_event_plans(
                self.clock(at="2026-08-17T04:00:02Z", events=[malformed]),
                contract=self.contract,
                instruments=["EUR_USD"],
                policy_dependency_registry=self.dependencies,
                planned_utc=planned,
            )

    def test_event_known_clock_must_match_availability_clock(self):
        malformed = self.event(
            ledger_effective_known_utc="2026-08-17T03:59:59+00:00"
        )
        with self.assertRaisesRegex(
            ProspectiveEventResponseV3Error,
            "event_known_clock_does_not_match_event_availability",
        ):
            build_event_plans(
                self.clock(at="2026-08-17T04:00:00Z", events=[malformed]),
                contract=self.contract,
                instruments=["EUR_USD"],
                policy_dependency_registry=self.dependencies,
                planned_utc=self.planned,
            )

    def test_missing_source_contract_lineage_fails_closed_nonterminal(self):
        missing = self.event(source_contract_first_observed_utc="")
        built = build_event_plans(
            self.clock(at="2026-08-17T04:00:00Z", events=[missing]),
            contract=self.contract,
            instruments=["EUR_USD"],
            policy_dependency_registry=self.dependencies,
            planned_utc=self.planned,
        )
        self.assertEqual(built["plan_count"], 0)
        attempts = [
            row
            for row in built["attempts"]
            if row["reason"] == "source_contract_lineage_missing"
        ]
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["terminal"], 0)

    def test_dual_clock_freshness_rejects_fresh_reread_of_stale_source(self):
        with self.assertRaisesRegex(
            ProspectiveEventResponseV3Error,
            "clock_observation_source_generated_utc_stale",
        ):
            build_event_plans(
                self.clock(
                    at="2026-08-17T04:00:00Z",
                    source_generated="2026-08-17T03:55:00Z",
                ),
                contract=self.contract,
                instruments=["EUR_USD"],
                policy_dependency_registry=self.dependencies,
                planned_utc=self.planned,
            )

    def test_target_capture_rejects_stale_upstream_event_source(self):
        with self.assertRaisesRegex(
            ProspectiveEventResponseV3Error,
            "clock_observation_source_generated_utc_stale",
        ):
            build_samples(
                [self.plan("EUR_USD", 0)],
                self.quote(),
                existing_samples=[],
                current_clock_snapshot=self.clock(
                    at="2026-08-17T05:00:01Z",
                    source_generated="2026-08-17T04:55:00Z",
                ),
                contract=self.contract,
                collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
            )
        with self.assertRaisesRegex(
            ProspectiveEventResponseV3Error,
            "clock_attestation_generated_utc_stale",
        ):
            build_event_plans(
                self.clock(
                    at="2026-08-17T04:00:00Z",
                    attestation_generated="2026-08-17T03:55:00Z",
                ),
                contract=self.contract,
                instruments=["EUR_USD"],
                policy_dependency_registry=self.dependencies,
                planned_utc=self.planned,
            )

    def test_prebaseline_revision_terminal_but_postbaseline_revision_allowed(self):
        baseline_plan = self.plan("EUR_USD", 0)
        revised = [self.event(event_version_id="clock_version_two")]
        rejected = self.capture(
            baseline_plan,
            self.quote(),
            "2026-08-17T05:00:01Z",
            events=revised,
        )
        self.assertEqual(rejected["sample_count"], 0)
        self.assertEqual(
            rejected["terminal_exclusions"][0]["reason"],
            "event_clock_revised_or_removed_before_baseline",
        )

        accepted_baseline = self.capture(
            baseline_plan,
            self.quote(),
            "2026-08-17T05:00:01Z",
        )["samples"][0]
        endpoint_plan = self.plan("EUR_USD", 60)
        endpoint_quotes = self.quote(
            quote_time="2026-08-17T05:01:00.500000Z",
            collector="2026-08-17T05:01:01Z",
            bid=1.1005,
            ask=1.1007,
        )
        accepted_endpoint = self.capture(
            endpoint_plan,
            endpoint_quotes,
            "2026-08-17T05:01:01Z",
            existing=[accepted_baseline],
            events=[],
        )
        self.assertEqual(accepted_endpoint["sample_count"], 1)
        self.assertEqual(
            accepted_endpoint["samples"][0]["event_lineage_state"],
            "postbaseline_frozen_event_lineage",
        )

    def test_recurring_upstream_series_does_not_shadow_exact_event_version(self):
        baseline_plan = self.plan("EUR_USD", 0)
        current_exact = self.event()
        later_release = self.event(
            event_version_id="clock_version_later_release",
            scheduled_utc="2026-08-17T06:00:00+00:00",
        )
        result = self.capture(
            baseline_plan,
            self.quote(),
            "2026-08-17T05:00:01Z",
            events=[current_exact, later_release],
        )
        self.assertEqual(result["sample_count"], 1)
        self.assertEqual(result["terminal_exclusions"], [])
        self.assertEqual(
            result["samples"][0]["event_version_id"], "clock_version_one"
        )

    def test_endpoint_without_baseline_is_nonterminal_attempt(self):
        result = self.capture(
            self.plan("EUR_USD", 60),
            self.quote(
                quote_time="2026-08-17T05:01:00Z",
                collector="2026-08-17T05:01:01Z",
            ),
            "2026-08-17T05:01:01Z",
        )
        self.assertEqual(result["sample_count"], 0)
        self.assertEqual(result["terminal_exclusions"], [])
        self.assertEqual(result["attempts"][0]["terminal"], 0)
        self.assertEqual(result["attempts"][0]["reason"], "endpoint_waiting_for_valid_baseline")

    def test_exact_horizon_requires_both_target_distances_and_ordered_clocks(self):
        baseline_plan = self.plan("EUR_USD", 0)
        endpoint_plan = self.plan("EUR_USD", 60)
        common = {
            "contract_id": self.contract["contract_id"],
            "cohort_id": self.contract["cohort_id"],
            "event_instance_id": baseline_plan["event_instance_id"],
            "instrument": "EUR_USD",
            "pip": 0.0001,
        }
        baseline = {
            **common,
            "sample_id": "baseline",
            "plan_id": baseline_plan["plan_id"],
            "target_offset_sec": 0,
            "target_distance_sec": 40.0,
            "bid": 1.1,
            "ask": 1.1002,
            "quote_time_utc": "2026-08-17T05:00:40+00:00",
            "observation_known_utc": "2026-08-17T05:00:41+00:00",
        }
        endpoint = {
            **common,
            "sample_id": "endpoint",
            "plan_id": endpoint_plan["plan_id"],
            "target_offset_sec": 60,
            "target_distance_sec": 40.0,
            "bid": 1.1005,
            "ask": 1.1007,
            "quote_time_utc": "2026-08-17T05:01:40+00:00",
            "observation_known_utc": "2026-08-17T05:01:41+00:00",
        }
        outcome = mature_outcome_rows(
            [baseline_plan, endpoint_plan],
            [baseline, endpoint],
            contract=self.contract,
            recorded_utc=stamp("2026-08-17T05:01:42Z"),
        )[0]
        self.assertEqual(outcome["actual_observation_duration_sec"], 60.0)
        self.assertEqual(outcome["exact_horizon_observation"], 0)
        self.assertEqual(outcome["proof_evaluation_eligible"], 0)
        self.assertIn(
            "baseline_target_distance_exceeds_tolerance",
            json.loads(outcome["exactness_reasons_json"]),
        )

        premature = mature_outcome_rows(
            [baseline_plan, endpoint_plan],
            [
                {**baseline, "target_distance_sec": 0.0},
                {**endpoint, "target_distance_sec": 0.0},
            ],
            contract=self.contract,
            recorded_utc=stamp("2026-08-17T05:01:40Z"),
        )[0]
        self.assertEqual(premature["maturation_recorded_after_outcome_known"], 0)
        self.assertEqual(premature["proof_evaluation_eligible"], 0)
        self.assertIn(
            "maturation_recorded_before_outcome_known",
            json.loads(premature["exactness_reasons_json"]),
        )

    def _registered_connection(self, directory: str):
        connection = open_ledger(Path(directory) / "v3.sqlite")
        connection.execute("BEGIN IMMEDIATE")
        register_frozen_cohort(
            connection,
            contract=self.contract,
            static_fingerprint={"source": "test", "hash": "a" * 64},
            cohort_start_source_lineage={
                "clock": "test",
                "source_pipeline_version": "all_pair_news_event_tags_v3",
            },
        )
        return connection

    def test_source_pipeline_is_persisted_in_frozen_cohort(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = self._registered_connection(directory)
            try:
                row = connection.execute(
                    "SELECT source_pipeline_version FROM event_response_cohort_manifests_v3"
                ).fetchone()
                self.assertEqual("all_pair_news_event_tags_v3", row[0])
            finally:
                connection.rollback()
                connection.close()

    def test_full_record_hash_duplicate_collision_and_constraints(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = self._registered_connection(directory)
            plan = self.plan("EUR_USD", 0)
            self.assertEqual(append_cycle(connection, plans=[plan])["plans_inserted"], 1)
            self.assertEqual(append_cycle(connection, plans=[plan])["plans_inserted"], 0)
            changed = rehash({**plan, "headline": "different evidence"})
            with self.assertRaisesRegex(
                ProspectiveEventResponseV3Error, "primary_key_content_collision"
            ):
                append_cycle(connection, plans=[changed])
            invalid = rehash({**plan, "plan_id": "bad-flags", "research_only": 0})
            with self.assertRaisesRegex(
                ProspectiveEventResponseV3Error, "constraint_collision"
            ):
                append_cycle(connection, plans=[invalid])
            connection.rollback()
            connection.close()

    def test_parent_lineage_and_terminal_sample_conflict_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = self._registered_connection(directory)
            plan = self.plan("EUR_USD", 0)
            append_cycle(connection, plans=[plan])
            sample = self.capture(
                plan, self.quote(), "2026-08-17T05:00:01Z"
            )["samples"][0]
            bad = rehash({**sample, "instrument": "USD_JPY"})
            with self.assertRaisesRegex(
                ProspectiveEventResponseV3Error, "sample_to_plan_lineage_mismatch"
            ):
                append_cycle(connection, samples=[bad])
            expired = collection_workset(
                [plan], [], [], contract=self.contract,
                collector_observed_utc=stamp("2026-08-17T05:00:46Z")
            )["expired_exclusions"][0]
            append_cycle(connection, terminal_exclusions=[expired])
            with self.assertRaisesRegex(
                ProspectiveEventResponseV3Error, "sample_conflicts_with_terminal_exclusion"
            ):
                append_cycle(connection, samples=[sample])
            self.assertEqual(verify_ledger(connection)["status"], "ok")
            connection.rollback()
            connection.close()

    def test_complete_plan_sample_outcome_round_trip_and_foreign_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = self._registered_connection(directory)
            baseline_plan = self.plan("EUR_USD", 0)
            endpoint_plan = self.plan("EUR_USD", 60)
            append_cycle(connection, plans=[baseline_plan, endpoint_plan])
            baseline = self.capture(
                baseline_plan, self.quote(), "2026-08-17T05:00:01Z"
            )["samples"][0]
            append_cycle(connection, samples=[baseline])
            endpoint = self.capture(
                endpoint_plan,
                self.quote(
                    quote_time="2026-08-17T05:01:00.500000Z",
                    collector="2026-08-17T05:01:01Z",
                    bid=1.1005,
                    ask=1.1007,
                ),
                "2026-08-17T05:01:01Z",
                existing=[baseline],
            )["samples"][0]
            append_cycle(connection, samples=[endpoint])
            outcome = mature_outcome_rows(
                [baseline_plan, endpoint_plan],
                [baseline, endpoint],
                contract=self.contract,
                recorded_utc=stamp("2026-08-17T05:01:02Z"),
            )[0]
            append_cycle(connection, outcomes=[outcome])
            self.assertEqual(outcome["proof_evaluation_eligible"], 1)
            self.assertEqual(verify_ledger(connection)["status"], "ok")
            plans, samples, exclusions = read_evidence(connection)
            self.assertEqual((len(plans), len(samples), len(exclusions)), (2, 2, 0))
            connection.rollback()
            connection.close()

    def test_core_and_worker_are_isolated_from_execution_and_supervisor(self):
        core = (
            ROOT / "src" / "forex_system" / "ingestion" / "prospective_event_response_v3.py"
        ).read_text(encoding="utf-8")
        worker = (ROOT / "oanda_prospective_event_response_worker_v3.py").read_text(
            encoding="utf-8"
        )
        forbidden_imports = (
            "oandapy",
            "requests.post",
            "create_order",
            "authorization",
            "lifecycle",
            "promotion",
        )
        import_lines = "\n".join(
            line
            for line in core.lower().splitlines()
            if line.startswith("import ") or line.startswith("from ")
        )
        for token in forbidden_imports:
            self.assertNotIn(token, import_lines)
        self.assertNotIn("supervisor", "\n".join(
            line for line in worker.lower().splitlines() if line.startswith("import ") or line.startswith("from ")
        ))

    def test_single_writer_lock_fails_closed_and_releases(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "writer.lock"
            with SingleWriterLock(path):
                self.assertTrue(path.exists())
                with self.assertRaisesRegex(RuntimeError, "writer_lock_exists"):
                    with SingleWriterLock(path):
                        pass
            self.assertFalse(path.exists())
            with SingleWriterLock(path):
                self.assertTrue(path.exists())

    def test_active_clock_failure_is_append_only_nonterminal_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = self._registered_connection(directory)
            plan = self.plan("EUR_USD", 0)
            append_cycle(connection, plans=[plan])
            connection.commit()
            connection.close()
            result = record_active_preflight_failure(
                ledger_path=root / "v3.sqlite",
                lock_path=root / "writer.lock",
                contract=self.contract,
                observed_utc=stamp("2026-08-17T05:00:01Z"),
                reason="active_target_clock_preflight_failed_closed",
                error="clock_observation_source_generated_utc_stale",
            )
            self.assertEqual(result["attempts_inserted"], 1)
            checked = open_ledger(root / "v3.sqlite")
            attempt = checked.execute(
                "SELECT * FROM event_response_attempt_diagnostics_v3"
            ).fetchone()
            terminal_count = checked.execute(
                "SELECT COUNT(*) FROM event_response_terminal_exclusions_v3"
            ).fetchone()[0]
            checked.close()
            self.assertEqual(attempt["source_ref"], plan["plan_id"])
            self.assertEqual(attempt["phase"], "active_target_preflight")
            self.assertEqual(attempt["terminal"], 0)
            self.assertEqual(terminal_count, 0)

    def test_full_record_verifier_detects_database_tamper(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = self._registered_connection(directory)
            plan = self.plan("EUR_USD", 0)
            append_cycle(connection, plans=[plan])
            connection.commit()
            connection.close()
            raw = sqlite3.connect(Path(directory) / "v3.sqlite")
            raw.execute("DROP TRIGGER event_response_plans_v3_no_update")
            raw.execute(
                "UPDATE event_response_plans_v3 SET headline='tampered' WHERE plan_id=?",
                (plan["plan_id"],),
            )
            raw.commit()
            raw.close()
            checked = open_ledger(Path(directory) / "v3.sqlite")
            with self.assertRaisesRegex(
                ProspectiveEventResponseV3Error, "payload_json_mismatch"
            ):
                read_evidence(checked)
            checked.close()

    def test_collector_creates_zero_sample_future_cohort_end_to_end(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            now = stamp("2026-08-17T04:00:00Z")
            contract = copy.deepcopy(self.contract)
            contract["cohort_id"] = "integration_v3_cohort"
            contract["cohort_start_utc"] = "2026-08-17T03:59:59+00:00"
            contract_path = root / "contract.json"
            contract_path.write_text(json.dumps(contract), encoding="utf-8")
            events_path = root / "events_latest.json"
            manifest_path = root / "manifest.json"
            event = {
                "event_id": "ecb-future",
                "source_id": "ecb_policy_calendar",
                "source_contract_id": "ecb_policy_calendar_v1",
                "source_cohort_id": "ecb_policy_calendar_v1",
                "material_content_sha256": "f" * 64,
                "scheduled_utc": "2026-08-17T05:00:00+00:00",
                "event_utc": "2026-08-17T05:00:00+00:00",
                "first_known_utc": "2026-08-17T03:45:00+00:00",
                "updated_utc": "2026-08-17T03:45:00+00:00",
                "headline": "ECB policy decision",
                "category": "monetary_policy",
                "source_name": "ECB",
                "source_url": "https://www.ecb.europa.eu/",
                "source_type": "live_news_watch",
                "source_verified": True,
                "currencies": ["EUR"],
                "direct_currencies": ["EUR"],
                "timing_precision": "minute",
                "clock_semantics": "domestic_official_policy_release",
                "independent_domestic_event": True,
                "event_time_basis": "scheduled_release",
            }
            events_path.write_text(json.dumps([event]), encoding="utf-8")
            events_sha256 = hashlib.sha256(events_path.read_bytes()).hexdigest()
            manifest_path.write_text(
                json.dumps(
                    {
                        "manifest_schema_version": 2,
                        "pipeline_version": "all_pair_news_event_tags_v3",
                        "generated_utc": "2026-08-17T03:59:59+00:00",
                        "event_count": 1,
                        "events_artifact_name": events_path.name,
                        "events_sha256": events_sha256,
                    }
                ),
                encoding="utf-8",
            )
            integrity = root / "clock_integrity_v1.json"
            integrity.write_text(
                json.dumps(
                    {
                        "generated_utc": "2026-08-17T03:59:59+00:00",
                        "status": "ok",
                        "timestamp_normalization_trusted": True,
                        "host_clock_synchronized": True,
                        "source_fresh": True,
                    }
                ),
                encoding="utf-8",
            )
            clock_db = root / "clock.sqlite"
            collect_to_ledger(
                events_path=events_path,
                manifest_path=manifest_path,
                database_path=clock_db,
                captured_utc=now,
                clock_integrity_path=integrity,
                maximum_clock_attestation_age_sec=90,
            )
            quotes = root / "quotes.json"
            quotes.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "producer": "practice_007_fast_executor_price_stream",
                        "generated_utc": now.isoformat(),
                        "coverage": {
                            "execution_requires_independent_freshness_check": True
                        },
                        "quotes": {
                            "EUR_USD": {
                                "bid": 1.1,
                                "ask": 1.1002,
                                "pip": 0.0001,
                                "source": "stream",
                                "time": now.isoformat(),
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = run_once(
                contract_path=contract_path,
                event_clock_db=clock_db,
                quote_snapshot_path=quotes,
                clock_integrity_path=integrity,
                ledger_path=root / "response.sqlite",
                lock_path=root / "writer.lock",
                state_path=root / "state.json",
                report_path=root / "report.md",
                observed_utc=now,
            )
            counts = payload["ledger"]["table_counts"]
            self.assertEqual(counts["event_response_plans_v3"], 68 * 11)
            self.assertEqual(counts["event_response_samples_v3"], 0)
            self.assertEqual(counts["event_response_outcomes_v3"], 0)
            self.assertEqual(payload["coverage"]["whole_universe_coverage"], "21/21")
            self.assertEqual(payload["ledger"]["verification"]["status"], "ok")
            self.assertTrue((root / "state.json").is_file())
            self.assertTrue((root / "report.md").is_file())

            # A later immutable semantic snapshot may append a previously unseen
            # future exact event.  It must not rewrite/rebind plans admitted from
            # the earlier snapshot.
            ledger_path = root / "response.sqlite"
            before = sqlite3.connect(ledger_path)
            original_rows = dict(
                before.execute(
                    "SELECT plan_id,payload_sha256 FROM event_response_plans_v3"
                ).fetchall()
            )
            before.close()
            later = now + dt.timedelta(seconds=10)
            second_event = dict(event)
            second_event.update(
                {
                    "event_id": "boj-future",
                    "scheduled_utc": "2026-08-17T06:00:00+00:00",
                    "event_utc": "2026-08-17T06:00:00+00:00",
                    "first_known_utc": "2026-08-17T03:50:00+00:00",
                    "updated_utc": "2026-08-17T03:50:00+00:00",
                    "headline": "BOJ policy decision",
                    "source_name": "Bank of Japan",
                    "source_url": "https://www.boj.or.jp/",
                    "currencies": ["JPY"],
                    "direct_currencies": ["JPY"],
                }
            )
            events_path.write_text(json.dumps([event, second_event]), encoding="utf-8")
            later_events_sha256 = hashlib.sha256(events_path.read_bytes()).hexdigest()
            manifest_path.write_text(
                json.dumps(
                    {
                        "manifest_schema_version": 2,
                        "pipeline_version": "all_pair_news_event_tags_v3",
                        "generated_utc": later.isoformat(),
                        "event_count": 2,
                        "events_artifact_name": events_path.name,
                        "events_sha256": later_events_sha256,
                    }
                ),
                encoding="utf-8",
            )
            integrity.write_text(
                json.dumps(
                    {
                        "generated_utc": later.isoformat(),
                        "status": "ok",
                        "timestamp_normalization_trusted": True,
                        "host_clock_synchronized": True,
                        "source_fresh": True,
                    }
                ),
                encoding="utf-8",
            )
            collect_to_ledger(
                events_path=events_path,
                manifest_path=manifest_path,
                database_path=clock_db,
                captured_utc=later,
                clock_integrity_path=integrity,
                maximum_clock_attestation_age_sec=90,
            )
            quote_payload = json.loads(quotes.read_text(encoding="utf-8"))
            quote_payload["generated_utc"] = later.isoformat()
            quote_payload["quotes"]["EUR_USD"]["time"] = later.isoformat()
            quotes.write_text(json.dumps(quote_payload), encoding="utf-8")
            appended = run_once(
                contract_path=contract_path,
                event_clock_db=clock_db,
                quote_snapshot_path=quotes,
                clock_integrity_path=integrity,
                ledger_path=ledger_path,
                lock_path=root / "writer.lock",
                state_path=root / "state.json",
                report_path=root / "report.md",
                observed_utc=later,
            )
            self.assertEqual(
                appended["ledger"]["table_counts"]["event_response_plans_v3"],
                2 * 68 * 11,
            )
            after = sqlite3.connect(ledger_path)
            after_rows = dict(
                after.execute(
                    "SELECT plan_id,payload_sha256 FROM event_response_plans_v3"
                ).fetchall()
            )
            after.close()
            self.assertEqual(
                {plan_id: after_rows[plan_id] for plan_id in original_rows},
                original_rows,
            )

            # A fresh re-observation of the unchanged semantic event catalog at
            # the target must use the bounded active path and capture the
            # executable baseline without rescanning/replanning the full ledger.
            target_now = stamp("2026-08-17T05:00:00Z")
            manifest_path.write_text(
                json.dumps(
                    {
                        "manifest_schema_version": 2,
                        "pipeline_version": "all_pair_news_event_tags_v3",
                        "generated_utc": target_now.isoformat(),
                        "event_count": 2,
                        "events_artifact_name": events_path.name,
                        "events_sha256": later_events_sha256,
                    }
                ),
                encoding="utf-8",
            )
            integrity.write_text(
                json.dumps(
                    {
                        "generated_utc": target_now.isoformat(),
                        "status": "ok",
                        "timestamp_normalization_trusted": True,
                        "host_clock_synchronized": True,
                        "source_fresh": True,
                    }
                ),
                encoding="utf-8",
            )
            collect_to_ledger(
                events_path=events_path,
                manifest_path=manifest_path,
                database_path=clock_db,
                captured_utc=target_now,
                clock_integrity_path=integrity,
                maximum_clock_attestation_age_sec=90,
            )
            quote_payload = json.loads(quotes.read_text(encoding="utf-8"))
            quote_payload["generated_utc"] = target_now.isoformat()
            quote_payload["quotes"]["EUR_USD"]["time"] = target_now.isoformat()
            quotes.write_text(json.dumps(quote_payload), encoding="utf-8")
            active = run_once(
                contract_path=contract_path,
                event_clock_db=clock_db,
                quote_snapshot_path=quotes,
                clock_integrity_path=integrity,
                ledger_path=ledger_path,
                lock_path=root / "writer.lock",
                state_path=root / "state.json",
                report_path=root / "report.md",
                observed_utc=target_now,
                cycle_mode="active_target_window",
                cadence_sec=1.0,
            )
            self.assertTrue(active["bounded_active_scope"])
            self.assertEqual(active["cycle"]["samples_inserted"], 1)
            self.assertEqual(
                active["ledger"]["table_counts"]["event_response_samples_v3"], 1
            )


if __name__ == "__main__":
    unittest.main()
