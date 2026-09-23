from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from forex_system.ingestion.prospective_event_response import (  # noqa: E402
    ProspectiveEventResponseError,
    ProspectiveEventResponseV2RetiredError,
    build_event_plans,
    build_samples,
    collection_workset,
    completed_candle_candidates,
    load_contract,
    mature_outcome_rows,
    open_ledger,
    quote_snapshot_candidates,
)


UTC = dt.timezone.utc
CONTRACT_PATH = ROOT / "config" / "prospective_event_response_capture_v2.json"


def stamp(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


class ProspectiveEventResponseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)
        self.planned = stamp("2026-08-17T04:00:00Z")
        self.scheduled = "2026-08-17T05:00:00+00:00"

    def event(self, **overrides):
        event = {
            "event_version_id": "clock_version_one",
            "upstream_event_id": "official-us-release",
            "scheduled_utc": self.scheduled,
            "ledger_effective_known_utc": "2026-08-17T03:46:00+00:00",
            "headline": "Official United States release",
            "category": "inflation_release",
            "source_name": "Official agency",
            "source_url": "https://example.gov/release",
            "source_verified": True,
            "direct_currencies": ["USD"],
            "currencies": ["USD"],
            "event_time_basis": "scheduled_release",
            "schedule_window_end_utc": "",
        }
        event.update(overrides)
        return event

    def clock(
        self,
        events=None,
        *,
        attested=True,
        captured="2026-08-17T04:59:00+00:00",
    ):
        return {
            "contract_id": "immutable_point_in_time_event_clock_v1_20260817",
            "snapshot_id": "clock_snapshot_one",
            "snapshot_captured_utc": captured,
            "clock_observation_id": "clock_observation_one",
            "clock_observed_utc": captured,
            "clock_attestation": {
                "state": "fresh_trusted" if attested else "unattested_stale",
                "attested": attested,
                "payload_sha256": "c" * 64,
            },
            "events": [self.event()] if events is None else events,
        }

    def plans(self):
        return build_event_plans(
            self.clock(captured="2026-08-17T03:59:00+00:00"),
            contract=self.contract,
            instruments=["EUR_USD", "USD_JPY"],
            planned_utc=self.planned,
        )["plans"]

    def test_plan_population_is_frozen_before_event_and_ignores_future_move_fields(self):
        self.assertEqual(
            self.contract["target_offsets_sec"][:6], [0, 30, 60, 120, 300, 600]
        )
        first = build_event_plans(
            self.clock(
                [self.event(future_move_pips=500, profitable=True)],
                captured="2026-08-17T03:59:00+00:00",
            ),
            contract=self.contract,
            instruments=["EUR_USD", "USD_JPY"],
            planned_utc=self.planned,
        )
        second = build_event_plans(
            self.clock(
                [self.event(future_move_pips=-500, profitable=False)],
                captured="2026-08-17T03:59:00+00:00",
            ),
            contract=self.contract,
            instruments=["EUR_USD", "USD_JPY"],
            planned_utc=self.planned,
        )
        self.assertEqual(first["plan_count"], 22)
        self.assertEqual(
            [row["plan_id"] for row in first["plans"]],
            [row["plan_id"] for row in second["plans"]],
        )
        baseline = [
            row
            for row in first["plans"]
            if row["instrument"] == "EUR_USD" and row["target_offset_sec"] == 0
        ][0]
        self.assertEqual(baseline["currency_orientation"], -1)
        self.assertEqual(baseline["sample_role"], "baseline")
        self.assertEqual(baseline["clock_snapshot_id"], "clock_snapshot_one")
        self.assertEqual(baseline["clock_observation_id"], "clock_observation_one")
        self.assertEqual(baseline["clock_attestation_payload_sha256"], "c" * 64)
        self.assertEqual(baseline["schedule_lock_utc"], self.planned.isoformat())

    def test_unattested_clock_and_post_event_schedule_discovery_fail_closed(self):
        with self.assertRaisesRegex(ProspectiveEventResponseError, "not_fresh_trusted"):
            build_event_plans(
                self.clock(
                    attested=False, captured="2026-08-17T03:59:00+00:00"
                ),
                contract=self.contract,
                instruments=["EUR_USD"],
                planned_utc=self.planned,
            )
        late = build_event_plans(
            self.clock(
                [self.event(ledger_effective_known_utc="2026-08-17T05:00:01+00:00")],
                captured="2026-08-17T04:59:00+00:00",
            ),
            contract=self.contract,
            instruments=["EUR_USD"],
            planned_utc=stamp("2026-08-17T05:00:02Z"),
        )
        self.assertEqual(late["plan_count"], 0)
        self.assertIn(
            "insufficient_pre_release_schedule_lead",
            {row["reason"] for row in late["exclusions"]},
        )

    def test_actual_matching_currency_is_preserved_for_multi_currency_event(self):
        result = build_event_plans(
            self.clock(
                [self.event(direct_currencies=["CAD", "USD"], currencies=["CAD", "USD"])],
                captured="2026-08-17T03:59:00+00:00",
            ),
            contract=self.contract,
            instruments=["EUR_USD", "USD_CAD"],
            planned_utc=self.planned,
        )
        eur_usd = [row for row in result["plans"] if row["instrument"] == "EUR_USD"]
        self.assertEqual(len(eur_usd), len(self.contract["target_offsets_sec"]))
        self.assertEqual({row["direct_currency"] for row in eur_usd}, {"USD"})
        self.assertEqual({row["currency_orientation"] for row in eur_usd}, {-1})
        self.assertFalse(any(row["instrument"] == "USD_CAD" for row in result["plans"]))
        self.assertIn(
            "ambiguous_both_pair_legs_direct",
            {row["reason"] for row in result["exclusions"]},
        )

    def quote_candidates(self, *, collector="2026-08-17T05:00:01+00:00", quote_time=None):
        quote_time = quote_time or "2026-08-17T05:00:00.500000+00:00"
        payload = {
            "schema_version": 2,
            "producer": "practice_007_fast_executor_price_stream",
            "coverage": {"execution_requires_independent_freshness_check": True},
            "generated_utc": collector,
            "quotes": {
                "EUR_USD": {
                    "bid": 1.1000,
                    "ask": 1.1002,
                    "pip": 0.0001,
                    "source": "stream",
                    "time": quote_time,
                }
            },
        }
        return quote_snapshot_candidates(
            payload,
            contract=self.contract,
            artifact_sha256="q" * 64,
            collector_observed_utc=stamp(collector),
            clock_attestation={"state": "fresh_trusted", "attested": True},
        )

    def baseline_plan(self):
        return [
            row
            for row in self.plans()
            if row["instrument"] == "EUR_USD" and row["target_offset_sec"] == 0
        ][0]

    def test_valid_quote_retains_distinct_market_collection_and_known_clocks(self):
        result = build_samples(
            [self.baseline_plan()],
            self.quote_candidates(),
            current_clock_snapshot=self.clock(),
            contract=self.contract,
            collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
        )
        self.assertEqual(result["sample_count"], 1)
        sample = result["samples"][0]
        self.assertEqual(sample["quote_time_utc"], "2026-08-17T05:00:00.500000+00:00")
        self.assertEqual(sample["collector_observed_utc"], "2026-08-17T05:00:01+00:00")
        self.assertEqual(sample["observation_known_utc"], "2026-08-17T05:00:01+00:00")
        self.assertEqual(sample["validation_clock_observation_id"], "clock_observation_one")
        self.assertAlmostEqual(sample["spread_pips"], 2.0)

    def test_quote_envelope_producer_and_schema_are_frozen(self):
        payload = {
            "schema_version": 2,
            "producer": "different_producer",
            "coverage": {"execution_requires_independent_freshness_check": True},
            "generated_utc": "2026-08-17T05:00:01+00:00",
            "quotes": {},
        }
        with self.assertRaisesRegex(
            ProspectiveEventResponseError, "producer_contract_mismatch"
        ):
            quote_snapshot_candidates(
                payload,
                contract=self.contract,
                artifact_sha256="q" * 64,
                collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
                clock_attestation={"state": "fresh_trusted", "attested": True},
            )

    def test_stale_quote_revised_event_and_precohort_observation_fail_closed(self):
        stale = build_samples(
            [self.baseline_plan()],
            self.quote_candidates(quote_time="2026-08-17T04:59:00+00:00"),
            current_clock_snapshot=self.clock(),
            contract=self.contract,
            collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
        )
        self.assertEqual(stale["sample_count"], 0)
        self.assertIn(stale["exclusions"][0]["reason"], {
            "stale_quote_at_collection",
            "market_time_outside_predeclared_target_window",
        })
        revised = self.clock([self.event(event_version_id="clock_version_two")])
        result = build_samples(
            [self.baseline_plan()],
            self.quote_candidates(),
            current_clock_snapshot=revised,
            contract=self.contract,
            collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
        )
        self.assertEqual(result["sample_count"], 0)
        self.assertEqual(
            result["exclusions"][0]["reason"],
            "event_clock_revised_or_removed_before_target",
        )
        with self.assertRaisesRegex(
            ProspectiveEventResponseError, "current_event_clock_snapshot_stale"
        ):
            build_samples(
                [self.baseline_plan()],
                self.quote_candidates(),
                current_clock_snapshot=self.clock(
                    captured="2026-08-17T04:40:00+00:00"
                ),
                contract=self.contract,
                collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
            )

    def test_completed_candle_requires_immutable_prospective_lineage(self):
        plan = self.baseline_plan()
        candidate = {
            "instrument": "EUR_USD",
            "source_type": "immutable_completed_bid_ask_candle",
            "source_record_id": "candle-one",
            "source_payload_sha256": "d" * 64,
            "bid": 1.1,
            "ask": 1.1002,
            "pip": 0.0001,
            "quote_time_utc": "2026-08-17T05:00:00+00:00",
            "source_generated_utc": "2026-08-17T05:00:01+00:00",
            "collector_observed_utc": "2026-08-17T05:00:01+00:00",
            "observation_known_utc": "2026-08-17T05:00:01+00:00",
            "clock_attestation_state": "fresh_trusted",
            "clock_attested": True,
            "immutable_prospective_lineage": False,
        }
        rejected = build_samples(
            [plan], [candidate], current_clock_snapshot=self.clock(),
            contract=self.contract, collector_observed_utc=stamp("2026-08-17T05:00:01Z")
        )
        self.assertEqual(rejected["sample_count"], 0)
        candidate["immutable_prospective_lineage"] = True
        accepted = build_samples(
            [plan], [candidate], current_clock_snapshot=self.clock(),
            contract=self.contract, collector_observed_utc=stamp("2026-08-17T05:00:01Z")
        )
        self.assertEqual(accepted["sample_count"], 1)

        normalized = completed_candle_candidates(
            [
                {
                    "immutable_record_id": "candle-two",
                    "immutable_prospective_lineage": True,
                    "instrument": "EUR_USD",
                    "close_bid": 1.1,
                    "close_ask": 1.1002,
                    "pip": 0.0001,
                    "completed_utc": "2026-08-17T05:00:00+00:00",
                    "first_observed_utc": "2026-08-17T05:00:01+00:00",
                    "source_generated_utc": "2026-08-17T05:00:01+00:00",
                }
            ],
            artifact_sha256="e" * 64,
            collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
            clock_attestation={"state": "fresh_trusted", "attested": True},
        )
        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0]["source_record_id"], "candle-two")

    def test_maturity_uses_executable_both_side_endpoint_without_best_side(self):
        plans = self.plans()
        entry_plan = [
            row for row in plans
            if row["instrument"] == "EUR_USD" and row["target_offset_sec"] == 0
        ][0]
        exit_plan = [
            row for row in plans
            if row["instrument"] == "EUR_USD" and row["target_offset_sec"] == 60
        ][0]
        common = {
            "contract_id": self.contract["contract_id"],
            "cohort_id": self.contract["cohort_id"],
            "event_instance_id": entry_plan["event_instance_id"],
            "instrument": "EUR_USD",
            "pip": 0.0001,
        }
        entry = {
            **common,
            "sample_id": "entry",
            "plan_id": entry_plan["plan_id"],
            "bid": 1.1000,
            "ask": 1.1002,
            "quote_time_utc": "2026-08-17T05:00:00+00:00",
            "observation_known_utc": "2026-08-17T05:00:01+00:00",
        }
        exit_sample = {
            **common,
            "sample_id": "exit",
            "plan_id": exit_plan["plan_id"],
            "bid": 1.1005,
            "ask": 1.1007,
            "quote_time_utc": "2026-08-17T05:01:00+00:00",
            "observation_known_utc": "2026-08-17T05:01:01+00:00",
        }
        rows = mature_outcome_rows(
            [entry_plan, exit_plan], [entry, exit_sample],
            contract=self.contract, recorded_utc=stamp("2026-08-17T05:01:02Z")
        )
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["long_after_spread_pips"], 3.0)
        self.assertAlmostEqual(rows[0]["short_after_spread_pips"], -7.0)
        self.assertFalse(rows[0]["direction_selected"])
        self.assertIsNone(rows[0]["best_side_metric"])
        self.assertTrue(rows[0]["exact_horizon_observation"])

    def test_collection_window_does_not_expire_plan_early(self):
        plan = self.baseline_plan()
        active = collection_workset(
            [plan], [], contract=self.contract,
            collector_observed_utc=stamp("2026-08-17T05:00:20Z")
        )
        self.assertEqual(active["active_plan_count"], 1)
        self.assertEqual(active["expired_plan_count"], 0)
        expired = collection_workset(
            [plan], [], contract=self.contract,
            collector_observed_utc=stamp("2026-08-17T05:00:46Z")
        )
        self.assertEqual(expired["active_plan_count"], 0)
        self.assertEqual(expired["expired_plan_count"], 1)

    def test_retired_v2_writer_fails_after_restarts_without_touching_ledger(self):
        ledger = (
            ROOT
            / "data"
            / "oanda_training_manager"
            / "research_ledgers"
            / "prospective_event_response_v2.sqlite"
        )
        before = (
            ledger.stat().st_size,
            ledger.stat().st_mtime_ns,
            hashlib.sha256(ledger.read_bytes()).hexdigest(),
        )
        with self.assertRaisesRegex(
            ProspectiveEventResponseV2RetiredError,
            "clock_dependency_superseded_before_any_sample",
        ):
            open_ledger(ROOT / "must_not_be_created.sqlite")
        self.assertFalse((ROOT / "must_not_be_created.sqlite").exists())

        code = """
from forex_system.ingestion.prospective_event_response import open_ledger
try:
    open_ledger('must_not_be_created_after_restart.sqlite')
except Exception as exc:
    print(type(exc).__name__, str(exc))
    raise SystemExit(0)
raise SystemExit(3)
"""
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(ROOT / "src")
        for _ in range(2):
            completed = subprocess.run(
                [sys.executable, "-c", code],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("ProspectiveEventResponseV2RetiredError", completed.stdout)
        self.assertFalse((ROOT / "must_not_be_created_after_restart.sqlite").exists())
        after = (
            ledger.stat().st_size,
            ledger.stat().st_mtime_ns,
            hashlib.sha256(ledger.read_bytes()).hexdigest(),
        )
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
