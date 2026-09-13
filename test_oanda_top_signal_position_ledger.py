import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import trad.oanda_top_signal_position_ledger as ledger

from trad.oanda_top_signal_position_ledger import (
    aggregate_cost_rows,
    aggregate_factor_episode_rows,
    aggregate_rows,
    horizon_label,
    mature_positions,
    open_database,
    open_positions,
    recent_rows,
    top_by_horizon,
)


class TopSignalPositionLedgerTests(unittest.TestCase):
    @staticmethod
    def _diagnostic_candidate(signal_id="cache", horizon_sec=60):
        return {
            "signal_id": signal_id,
            "instrument": "EUR_USD",
            "direction": "buy",
            "side": "Long",
            "horizon_sec": horizon_sec,
            "horizon_label": horizon_label(horizon_sec),
            "family": "trend",
            "lane_id": "trend.cache",
            "input_timeframe": "M1",
            "policy_state": "diagnostic_shadow",
            "signal_eligible": False,
            "validated": False,
            "direction_conflict": False,
            "blocked_by": ["unvalidated"],
            "signal_confidence": 0.5,
            "projected_net_pips": 0.0,
            "projected_net_pips_per_hour": 0.0,
            "gross_to_spread": 0.0,
        }

    def test_quote_snapshot_requires_a_fresh_timestamp(self):
        quotes = {"EUR_USD": {"bid": 1.1, "ask": 1.1002, "pip": 0.0001}}
        now = datetime(2026, 7, 31, 17, 0, tzinfo=timezone.utc).timestamp()
        fresh = {
            "generated_utc": "2026-07-31T16:59:30+00:00",
            "quotes": quotes,
        }
        stale = {
            "generated_utc": "2026-07-31T16:55:00+00:00",
            "quotes": quotes,
        }

        self.assertEqual(ledger.quote_map(fresh, now_epoch=now), quotes)
        self.assertEqual(ledger.quote_map(stale, now_epoch=now), {})
        self.assertEqual(ledger.quote_map({"quotes": quotes}, now_epoch=now), {})

    def test_atomic_state_publish_retries_transient_windows_reader_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            original_replace = Path.replace
            attempts = []

            def flaky_replace(source, target):
                attempts.append(target)
                if len(attempts) < 3:
                    raise PermissionError("temporary reader lock")
                return original_replace(source, target)

            with mock.patch.object(Path, "replace", new=flaky_replace), mock.patch.object(
                ledger.time, "sleep"
            ):
                ledger.write_json_atomic(path, {"status": "ok"})

            self.assertEqual(len(attempts), 3)
            self.assertEqual(ledger.load_json(path), {"status": "ok"})

    def test_d1_conflicted_projection_is_diagnostic_short(self):
        snapshot = {"top_signals": [{
            "id": "one", "instrument": "USD_CAD", "direction": "sell",
            "execution_validation": {
                "validated": False,
                "negative_historical_warmup": True,
                "promotion_rejected": True,
            },
            "direction_conflict": True,
            "horizon_breakdown": [{
                "horizon_sec": 86400, "direction": "sell",
                "direction_state": "neutral", "signal_confidence": 0.5,
                "projected_net_pips": 71.943,
                "projected_net_pips_per_hour": 2.998,
                "gross_to_spread": 20.0, "family_count": 17,
                "signal_eligible": False,
                "signal_blocked_by": ["unvalidated_signal"],
            }],
        }]}

        rows = top_by_horizon(snapshot)

        self.assertEqual(horizon_label(86400), "D1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["side"], "Short")
        self.assertEqual(
            rows[0]["measurement_version"],
            ledger.LEGACY_MEASUREMENT_VERSION,
        )
        self.assertEqual(rows[0]["policy_state"], "diagnostic_shadow")

    def test_blocked_placeholder_direction_uses_raw_consensus_for_what_if(self):
        snapshot = {"top_signals": [{
            "id": "raw-one", "instrument": "GBP_JPY", "direction": "buy",
            "execution_validation": {
                "validated": False,
                "negative_historical_warmup": True,
                "promotion_rejected": True,
            },
            "direction_conflict": True,
            "horizon_breakdown": [{
                "horizon_sec": 3600, "direction": "buy",
                "direction_state": "neutral", "signal_confidence": 0.5,
                "projected_net_pips": 0.0,
                "projected_net_pips_per_hour": 0.0,
                "raw_probability_up": 0.46,
                "raw_ensemble_signed_net_pips": -3.694,
                "raw_ensemble_aligned_weight_pct": 72.0,
                "gross_to_spread": 2.5,
                "all_contributor_gross_to_spread": 2.5,
                "directional_gross_to_spread": 1.25,
                "family_count": 4,
                "signal_eligible": False,
                "signal_blocked_by": [
                    "ensemble_no_eligible_structural_support"
                ],
            }],
        }]}

        rows = top_by_horizon(snapshot)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["direction"], "sell")
        self.assertEqual(rows[0]["side"], "Short")
        self.assertEqual(rows[0]["direction_source"], "raw_all_signal_consensus")
        self.assertAlmostEqual(rows[0]["signal_confidence"], 0.54)
        self.assertAlmostEqual(rows[0]["projected_net_pips"], 3.694)
        self.assertAlmostEqual(rows[0]["directional_gross_to_spread"], 1.25)
        self.assertEqual(
            rows[0]["measurement_version"],
            ledger.PREVIOUS_MEASUREMENT_VERSION,
        )
        self.assertFalse(rows[0]["strict_lineage_available"])
        self.assertTrue(rows[0]["negative_historical_warmup"])
        self.assertTrue(rows[0]["promotion_rejected"])
        self.assertFalse(rows[0]["execution_validation"]["validated"])
        self.assertEqual(
            rows[0]["policy_state"],
            "conflicted_aggressive_shadow",
        )

    def test_execution_eligible_direction_conflict_enters_aggressive_shadow(self):
        snapshot = {"top_signals": [{
            "id": "eligible-conflict", "instrument": "USD_JPY",
            "direction": "buy",
            "execution_validation": {"validated": True},
            "direction_conflict": True,
            "horizon_breakdown": [{
                "aggregate_signal_id": "aggregate_signal_14400",
                "aggregate_signal_lineage_contract_id": (
                    ledger.AGGREGATE_SIGNAL_LINEAGE_CONTRACT_ID
                ),
                "horizon_sec": 14400, "direction": "buy",
                "signal_confidence": 0.57,
                "projected_net_pips": 3.2,
                "projected_net_pips_per_hour": 0.8,
                "raw_probability_up": 0.57,
                "raw_ensemble_signed_net_pips": 3.2,
                "raw_ensemble_aligned_weight_pct": 80.0,
                "gross_to_spread": 2.1,
                "spread_pips": 1.8,
                "family_count": 7,
                "signal_eligible": True,
                "signal_blocked_by": [],
                "direction_conflict_contract_id": "conflict-v1",
                "direction_conflict_contributor_count": 1,
                "direction_conflict_account_eligible_count": 0,
                "direction_conflict_execution_component_count": 0,
                "direction_conflict_shadow_only_count": 1,
                "direction_conflict_only_shadow_or_account_ineligible": True,
                "direction_conflict_contributors": [{
                    "family": "inverse_correlation_veto",
                    "direction": "sell",
                    "account_eligible": False,
                }],
            }],
        }]}

        rows = top_by_horizon(snapshot)

        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["signal_eligible"])
        self.assertTrue(rows[0]["direction_conflict"])
        self.assertEqual(rows[0]["signal_id"], "aggregate_signal_14400")
        self.assertEqual(rows[0]["source_signal_id"], "eligible-conflict")
        self.assertEqual(
            rows[0]["signal_lineage_contract_id"],
            ledger.AGGREGATE_SIGNAL_LINEAGE_CONTRACT_ID,
        )
        self.assertEqual(rows[0]["measurement_version"], ledger.MEASUREMENT_VERSION)
        self.assertTrue(rows[0]["strict_lineage_available"])
        self.assertEqual(rows[0]["direction_conflict_contract_id"], "conflict-v1")
        self.assertTrue(
            rows[0]["direction_conflict_details"][
                "only_shadow_or_account_ineligible"
            ]
        )
        self.assertEqual(
            rows[0]["direction_conflict_details"]["contributors"][0]["family"],
            "inverse_correlation_veto",
        )
        self.assertEqual(
            rows[0]["policy_state"],
            "conflicted_aggressive_shadow",
        )

    def test_measurement_cohorts_can_open_same_horizon_without_lineage_collision(self):
        previous = self._diagnostic_candidate("previous", 60)
        previous["measurement_version"] = ledger.PREVIOUS_MEASUREMENT_VERSION
        current = self._diagnostic_candidate("current", 60)
        current["measurement_version"] = ledger.MEASUREMENT_VERSION
        quotes = {
            "EUR_USD": {
                "bid": 1.1000,
                "ask": 1.1002,
                "pip": 0.0001,
                "quote_epoch": 1_000.0,
            }
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            first = open_positions(
                connection,
                [previous],
                quotes,
                "2026-09-01T17:00:00+00:00",
                1_000.0,
            )
            second = open_positions(
                connection,
                [current],
                quotes,
                "2026-09-01T17:00:00+00:00",
                1_000.0,
            )
            rows = connection.execute(
                "SELECT cohort_key,measurement_version FROM positions "
                "ORDER BY measurement_version"
            ).fetchall()
            connection.close()

        self.assertEqual(first, 1)
        self.assertEqual(second, 1)
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(version in key for key, version in rows))

    def test_same_measurement_horizon_opens_only_once_per_cycle(self):
        first = self._diagnostic_candidate("first", 60)
        first["measurement_version"] = ledger.MEASUREMENT_VERSION
        second = self._diagnostic_candidate("second", 60)
        second["measurement_version"] = ledger.MEASUREMENT_VERSION
        second["instrument"] = "GBP_USD"
        quotes = {
            "EUR_USD": {
                "bid": 1.1000,
                "ask": 1.1002,
                "pip": 0.0001,
                "quote_epoch": 1_000.0,
            },
            "GBP_USD": {
                "bid": 1.3000,
                "ask": 1.3002,
                "pip": 0.0001,
                "quote_epoch": 1_000.0,
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            opened = open_positions(
                connection,
                [first, second],
                quotes,
                "2026-09-01T17:00:00+00:00",
                1_000.0,
            )
            rows = connection.execute(
                "SELECT instrument FROM positions"
            ).fetchall()
            connection.close()

        self.assertEqual(opened, 1)
        self.assertEqual(len(rows), 1)

    def test_long_outcome_uses_executable_bid_and_ask(self):
        candidate = {
            "signal_id": "three", "instrument": "EUR_USD",
            "direction": "buy", "side": "Long", "horizon_sec": 60,
            "horizon_label": "M1", "family": "momentum",
            "lane_id": "momentum.fast", "input_timeframe": "M1",
            "policy_state": "aggressive_shadow", "signal_eligible": False,
            "validated": True, "direction_conflict": False,
            "blocked_by": ["promotion_incomplete"],
            "signal_confidence": 0.58, "projected_net_pips": 1.2,
            "projected_net_pips_per_hour": 72.0, "gross_to_spread": 1.5,
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            opened = open_positions(
                connection, [candidate],
                {"EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}},
                "2026-07-31T12:00:00+00:00", 1_000.0,
            )
            closed = mature_positions(
                connection,
                {"EUR_USD": {"bid": 1.1005, "ask": 1.1007, "pip": 0.0001, "quote_epoch": 1_060.0}},
                1_061.0,
            )
            row = connection.execute(
                "SELECT status, net_pips, win FROM positions"
            ).fetchone()
            connection.close()

        self.assertEqual((opened, closed), (1, 1))
        self.assertEqual(row[0], "matured")
        self.assertAlmostEqual(row[1], 3.0, places=6)
        self.assertEqual(row[2], 1)

    def test_cost_buckets_separate_direction_from_after_cost_result(self):
        candidate = {
            "signal_id": "cost-one", "instrument": "EUR_USD",
            "direction": "buy", "side": "Long", "horizon_sec": 60,
            "horizon_label": "M1", "family": "momentum",
            "lane_id": "momentum.fast", "input_timeframe": "M1",
            "policy_state": "aggressive_shadow", "signal_eligible": False,
            "validated": True, "direction_conflict": False,
            "blocked_by": ["promotion_incomplete"],
            "signal_confidence": 0.58, "projected_net_pips": 1.2,
            "projected_net_pips_per_hour": 72.0, "gross_to_spread": 1.5,
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            open_positions(
                connection, [candidate],
                {"EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}},
                "2026-07-31T12:00:00+00:00", 1_000.0,
            )
            # Midpoint moves one pip in the forecast direction, but the
            # executable exit remains one pip underwater after spread.
            mature_positions(
                connection,
                {"EUR_USD": {"bid": 1.1001, "ask": 1.1003, "pip": 0.0001, "quote_epoch": 1_060.0}},
                1_061.0,
            )
            by_horizon = aggregate_rows(connection)
            by_cost = aggregate_cost_rows(connection)
            by_entry = aggregate_cost_rows(
                connection,
                bucket_basis="entry_spread",
            )
            connection.close()

        self.assertEqual(by_horizon[0]["direction_hits"], 1)
        self.assertEqual(by_horizon[0]["wins"], 0)
        self.assertEqual(by_cost[0]["cost_bucket"], "liquid_le_2")
        self.assertEqual(by_cost[0]["bucket_basis"], "realized_cost_drag")
        self.assertAlmostEqual(
            by_cost[0]["avg_realized_cost_drag_pips"], 2.0
        )
        self.assertEqual(by_cost[0]["direction_accuracy_pct"], 100.0)
        self.assertEqual(by_cost[0]["after_cost_win_rate_pct"], 0.0)
        self.assertEqual(by_entry[0]["bucket_basis"], "entry_spread")

    def test_realized_cost_bucket_catches_wide_exit_spread(self):
        candidate = {
            "signal_id": "wide-exit", "instrument": "USD_JPY",
            "direction": "buy", "side": "Long", "horizon_sec": 900,
            "horizon_label": "M15", "family": "trend",
            "lane_id": "trend.loose", "input_timeframe": "multi",
            "policy_state": "conflicted_aggressive_shadow",
            "signal_eligible": False, "validated": False,
            "direction_conflict": True,
            "blocked_by": ["unvalidated"],
            "signal_confidence": 0.52, "projected_net_pips": 0.3,
            "projected_net_pips_per_hour": 1.2, "gross_to_spread": 0.0,
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            open_positions(
                connection, [candidate],
                {"USD_JPY": {"bid": 157.173, "ask": 157.189, "pip": 0.01}},
                "2026-08-03T20:51:56+00:00", 1_000.0,
            )
            mature_positions(
                connection,
                {"USD_JPY": {"bid": 157.161, "ask": 157.241, "pip": 0.01, "quote_epoch": 1_900.0}},
                1_901.0,
            )
            realized = aggregate_cost_rows(connection)
            entry = aggregate_cost_rows(
                connection,
                bucket_basis="entry_spread",
            )
            recent = recent_rows(connection, 1)[0]
            connection.close()

        self.assertEqual(realized[0]["cost_bucket"], "elevated_3_to_5")
        self.assertAlmostEqual(
            realized[0]["avg_realized_cost_drag_pips"], 4.8
        )
        self.assertEqual(entry[0]["cost_bucket"], "liquid_le_2")
        self.assertEqual(recent["entry_liquidity_bucket"], "liquid_le_2")
        self.assertEqual(recent["realized_cost_bucket"], "elevated_3_to_5")
        self.assertAlmostEqual(recent["realized_cost_drag_pips"], 4.8)

    def test_factor_episode_summary_collapses_correlated_jpy_propagation(self):
        def candidate(signal_id, instrument, direction="buy"):
            return {
                "signal_id": signal_id, "instrument": instrument,
                "direction": direction,
                "side": "Long" if direction == "buy" else "Short",
                "horizon_sec": 60, "horizon_label": "M1",
                "family": "trend", "lane_id": "trend.loose",
                "input_timeframe": "multi",
                "policy_state": "conflicted_aggressive_shadow",
                "signal_eligible": False, "validated": False,
                "direction_conflict": True, "blocked_by": ["conflict"],
                "signal_confidence": 0.55, "projected_net_pips": 2.0,
                "projected_net_pips_per_hour": 120.0,
                "gross_to_spread": 2.0,
            }

        quotes = {
            "USD_JPY": {"bid": 157.10, "ask": 157.11, "pip": 0.01},
            "AUD_JPY": {"bid": 103.10, "ask": 103.11, "pip": 0.01},
            "EUR_USD": {"bid": 1.1000, "ask": 1.1001, "pip": 0.0001},
        }
        exits = {
            "USD_JPY": {"bid": 157.14, "ask": 157.15, "pip": 0.01},
            "AUD_JPY": {"bid": 103.14, "ask": 103.15, "pip": 0.01},
            "EUR_USD": {"bid": 1.1004, "ask": 1.1005, "pip": 0.0001},
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            for opened, row in [
                (1_000.0, candidate("jpy-1", "USD_JPY")),
                (1_100.0, candidate("jpy-2", "AUD_JPY")),
                (1_200.0, candidate("usd-opposite", "EUR_USD")),
                (2_101.0, candidate("jpy-later", "USD_JPY")),
            ]:
                open_positions(
                    connection, [row], quotes,
                    "2026-08-03T20:00:00+00:00", opened,
                )
                timed_exits = {
                    instrument: {**quote, "quote_epoch": opened + 60.0}
                    for instrument, quote in exits.items()
                }
                mature_positions(connection, timed_exits, opened + 61.0)
            summary = aggregate_factor_episode_rows(connection)
            connection.close()

        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["position_count"], 4)
        # The first two rows share JPY-short and collapse. EUR/USD is the
        # opposite USD factor, while the later JPY row is outside 15 minutes.
        self.assertEqual(summary[0]["episode_count"], 3)
        self.assertEqual(summary[0]["max_positions_per_episode"], 2)

    def test_late_target_quote_is_quarantined_and_excluded(self):
        candidate = {
            "signal_id": "late", "instrument": "EUR_USD",
            "direction": "buy", "side": "Long", "horizon_sec": 60,
            "horizon_label": "M1", "family": "trend", "lane_id": "trend",
            "input_timeframe": "M1", "policy_state": "diagnostic_shadow",
            "signal_eligible": False, "validated": False,
            "direction_conflict": False, "blocked_by": ["unvalidated"],
            "signal_confidence": 0.5, "projected_net_pips": 0.0,
            "projected_net_pips_per_hour": 0.0, "gross_to_spread": 0.0,
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            open_positions(
                connection, [candidate],
                {"EUR_USD": {"bid": 1.1, "ask": 1.1002, "pip": .0001, "quote_epoch": 1_000.0}},
                "2026-08-10T00:00:00+00:00", 999.0,
            )
            self.assertEqual(mature_positions(
                connection,
                {"EUR_USD": {"bid": 1.101, "ask": 1.1012, "pip": .0001, "quote_epoch": 1_600.0}},
                1_600.0,
            ), 0)
            row = connection.execute(
                "SELECT status,maturity_valid,target_quote_distance_sec,maturity_reason FROM positions"
            ).fetchone()
            self.assertEqual(row[0], "quarantined_maturity")
            self.assertEqual(row[1], 0)
            self.assertEqual(row[2], 540.0)
            self.assertEqual(row[3], "outcome_quote_after_target_limit")
            self.assertEqual(aggregate_rows(connection), [])
            connection.close()

    def test_entry_and_target_use_broker_quote_timestamp(self):
        candidate = {
            "signal_id": "clock", "instrument": "EUR_USD",
            "direction": "buy", "side": "Long", "horizon_sec": 60,
            "horizon_label": "M1", "family": "trend", "lane_id": "trend",
            "input_timeframe": "M1", "policy_state": "diagnostic_shadow",
            "signal_eligible": False, "validated": False,
            "direction_conflict": False, "blocked_by": ["unvalidated"],
            "signal_confidence": 0.5, "projected_net_pips": 0.0,
            "projected_net_pips_per_hour": 0.0, "gross_to_spread": 0.0,
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            open_positions(
                connection, [candidate],
                {"EUR_USD": {"bid": 1.1, "ask": 1.1002, "pip": .0001, "quote_epoch": 2_000.0}},
                "2026-08-10T00:00:00+00:00", 1_941.0,
            )
            row = connection.execute(
                "SELECT opened_epoch,target_epoch,measurement_version FROM positions"
            ).fetchone()
            self.assertEqual(row[0], 2_000.0)
            self.assertEqual(row[1], 2_060.0)
            self.assertEqual(row[2], ledger.MEASUREMENT_VERSION)
            connection.close()

    def test_demonstrably_late_legacy_maturity_is_quarantined(self):
        candidate = {
            "signal_id": "legacy-late", "instrument": "EUR_USD",
            "direction": "buy", "side": "Long", "horizon_sec": 60,
            "horizon_label": "M1", "family": "trend", "lane_id": "trend",
            "input_timeframe": "M1", "policy_state": "diagnostic_shadow",
            "signal_eligible": False, "validated": False,
            "direction_conflict": False, "blocked_by": ["unvalidated"],
            "signal_confidence": 0.5, "projected_net_pips": 0.0,
            "projected_net_pips_per_hour": 0.0, "gross_to_spread": 0.0,
            "measurement_version": ledger.PREVIOUS_MEASUREMENT_VERSION,
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            open_positions(
                connection, [candidate],
                {"EUR_USD": {"bid": 1.1, "ask": 1.1002, "pip": .0001}},
                "2026-08-10T00:00:00+00:00", 1_000.0,
            )
            connection.execute(
                """
                UPDATE positions SET status='matured',closed_epoch=1588,
                    maturity_valid=NULL,outcome_quote_epoch=NULL,
                    maturity_reason=NULL
                """
            )
            connection.commit()
            self.assertEqual(ledger.quarantine_legacy_maturities(connection), 1)
            row = connection.execute(
                "SELECT status,maturity_valid,maturity_reason FROM positions"
            ).fetchone()
            self.assertEqual(row[0], "quarantined_maturity")
            self.assertEqual(row[1], 0)
            self.assertEqual(
                row[2],
                "legacy_processing_delay_gt_limit_quote_time_unrecorded",
            )
            connection.close()

    def test_cached_publish_is_output_equivalent_and_skips_stable_rescan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = open_database(root / "ledger.sqlite")
            candidate = self._diagnostic_candidate()
            open_positions(
                connection,
                [candidate],
                {
                    "EUR_USD": {
                        "bid": 1.1000,
                        "ask": 1.1002,
                        "pip": 0.0001,
                        "quote_epoch": 1_000.0,
                    }
                },
                "2026-08-28T00:00:00+00:00",
                1_000.0,
            )
            mature_positions(
                connection,
                {
                    "EUR_USD": {
                        "bid": 1.1005,
                        "ask": 1.1007,
                        "pip": 0.0001,
                        "quote_epoch": 1_060.0,
                    }
                },
                1_061.0,
            )
            cycle = {"time": "fixed", "opened": 0, "matured": 0}
            baseline_path = root / "baseline.json"
            first_cached_path = root / "first_cached.json"
            second_cached_path = root / "second_cached.json"
            cache = {}

            with mock.patch.object(
                ledger,
                "utc_now",
                return_value="2026-08-28T00:00:00+00:00",
            ):
                ledger.publish_state(connection, baseline_path, cycle)
                with mock.patch.object(
                    ledger,
                    "mature_summary_payload",
                    wraps=ledger.mature_summary_payload,
                ) as summary_builder:
                    ledger.publish_state(
                        connection,
                        first_cached_path,
                        cycle,
                        summary_cache=cache,
                        metadata_dirty=False,
                        mature_dirty=False,
                    )
                    ledger.publish_state(
                        connection,
                        second_cached_path,
                        cycle,
                        summary_cache=cache,
                        metadata_dirty=False,
                        mature_dirty=False,
                    )

            baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
            first_cached = json.loads(
                first_cached_path.read_text(encoding="utf-8")
            )
            second_cached = json.loads(
                second_cached_path.read_text(encoding="utf-8")
            )
            self.assertEqual(summary_builder.call_count, 1)
            self.assertEqual(first_cached, baseline)
            self.assertEqual(second_cached, baseline)
            connection.close()

    def test_consecutive_unchanged_cycles_build_mature_summaries_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = open_database(root / "ledger.sqlite")
            cache = {}
            traced_sql = []
            snapshot = {
                "updated_at": "2026-08-28T00:00:00+00:00",
                "top_signals": [],
            }
            quotes = {
                "generated_utc": "2026-08-28T00:00:00+00:00",
                "quotes": {},
            }
            with mock.patch.object(
                ledger,
                "load_json",
                return_value=snapshot,
            ), mock.patch.object(
                ledger,
                "load_quote_snapshot",
                return_value=quotes,
            ), mock.patch.object(
                ledger.time,
                "time",
                return_value=1_777_593_600.0,
            ), mock.patch.object(
                ledger,
                "utc_now",
                return_value="2026-08-28T00:00:00+00:00",
            ), mock.patch.object(
                ledger,
                "mature_summary_payload",
                wraps=ledger.mature_summary_payload,
            ) as summary_builder, mock.patch.object(
                ledger,
                "position_metadata_payload",
                wraps=ledger.position_metadata_payload,
            ) as metadata_builder:
                first = ledger.run_cycle(
                    connection,
                    root / "signals.json",
                    root / "quotes.json",
                    root / "state.json",
                    summary_cache=cache,
                )
                connection.set_trace_callback(traced_sql.append)
                second = ledger.run_cycle(
                    connection,
                    root / "signals.json",
                    root / "quotes.json",
                    root / "state.json",
                    summary_cache=cache,
                )
                connection.set_trace_callback(None)

            self.assertEqual(summary_builder.call_count, 1)
            self.assertEqual(metadata_builder.call_count, 1)
            self.assertEqual(first["matured"], 0)
            self.assertEqual(second["matured"], 0)
            self.assertEqual(first["reclassified"], 0)
            self.assertEqual(second["reclassified"], 0)
            second_cycle_sql = "\n".join(traced_sql).lower()
            self.assertNotIn("avg(net_pips)", second_cycle_sql)
            self.assertNotIn("avg(gross_pips)", second_cycle_sql)
            self.assertNotIn("order by policy_state, opened_epoch", second_cycle_sql)
            self.assertNotIn(
                "select status, count(*) from positions group by status",
                second_cycle_sql,
            )
            self.assertNotIn(
                "select measurement_version, count(*) from positions",
                second_cycle_sql,
            )
            self.assertNotIn(
                "when status='quarantined_maturity'",
                second_cycle_sql,
            )
            connection.close()

    def test_repeated_opens_refresh_counts_without_rebuilding_mature_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = open_database(root / "ledger.sqlite")
            cache = {}
            cycle = {"time": "fixed", "opened": 0, "matured": 0}
            cached_path = root / "cached.json"
            baseline_path = root / "baseline.json"
            quotes = {
                "EUR_USD": {
                    "bid": 1.1000,
                    "ask": 1.1002,
                    "pip": 0.0001,
                    "quote_epoch": 1_000.0,
                }
            }

            with mock.patch.object(
                ledger,
                "utc_now",
                return_value="2026-08-28T00:00:00+00:00",
            ), mock.patch.object(
                ledger,
                "mature_summary_payload",
                wraps=ledger.mature_summary_payload,
            ) as summary_builder:
                ledger.publish_state(
                    connection,
                    cached_path,
                    cycle,
                    summary_cache=cache,
                    metadata_dirty=False,
                    mature_dirty=False,
                )
                for index, horizon in enumerate((60, 120), start=1):
                    opened = open_positions(
                        connection,
                        [
                            self._diagnostic_candidate(
                                signal_id=f"open-{index}",
                                horizon_sec=horizon,
                            )
                        ],
                        quotes,
                        "2026-08-28T00:00:00+00:00",
                        1_000.0,
                    )
                    self.assertEqual(opened, 1)
                    ledger.publish_state(
                        connection,
                        cached_path,
                        cycle,
                        summary_cache=cache,
                        metadata_dirty=True,
                        mature_dirty=False,
                    )
                self.assertEqual(summary_builder.call_count, 1)

            with mock.patch.object(
                ledger,
                "utc_now",
                return_value="2026-08-28T00:00:00+00:00",
            ):
                ledger.publish_state(connection, baseline_path, cycle)

            cached = json.loads(cached_path.read_text(encoding="utf-8"))
            baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
            self.assertEqual(cached, baseline)
            self.assertEqual(cached["counts"]["open"], 2)
            self.assertEqual(cached["counts"]["matured"], 0)
            self.assertEqual(cached["by_horizon"], [])
            self.assertEqual(len(cached["positions"]), 2)
            connection.close()

    def test_position_revision_ignores_open_marks_and_tracks_state_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            initial_metadata, initial_mature = ledger.position_revisions(
                connection
            )
            candidate = self._diagnostic_candidate()
            open_positions(
                connection,
                [candidate],
                {
                    "EUR_USD": {
                        "bid": 1.1000,
                        "ask": 1.1002,
                        "pip": 0.0001,
                        "quote_epoch": 1_000.0,
                    }
                },
                "2026-08-28T00:00:00+00:00",
                1_000.0,
            )
            opened_metadata, opened_mature = ledger.position_revisions(
                connection
            )
            self.assertGreater(opened_metadata, initial_metadata)
            self.assertEqual(opened_mature, initial_mature)

            mature_positions(
                connection,
                {
                    "EUR_USD": {
                        "bid": 1.1004,
                        "ask": 1.1006,
                        "pip": 0.0001,
                        "quote_epoch": 1_030.0,
                    }
                },
                1_031.0,
            )
            self.assertEqual(
                ledger.position_revisions(connection),
                (opened_metadata, opened_mature),
            )

            mature_positions(
                connection,
                {
                    "EUR_USD": {
                        "bid": 1.1005,
                        "ask": 1.1007,
                        "pip": 0.0001,
                        "quote_epoch": 1_060.0,
                    }
                },
                1_061.0,
            )
            matured_metadata, matured_revision = ledger.position_revisions(
                connection
            )
            self.assertGreater(matured_metadata, opened_metadata)
            self.assertGreater(matured_revision, opened_mature)

            connection.execute(
                "UPDATE positions SET policy_state='executable'"
            )
            connection.commit()
            before_repair_metadata, before_repair_mature = (
                ledger.position_revisions(connection)
            )
            self.assertEqual(ledger.repair_policy_labels(connection), 1)
            after_repair_metadata, after_repair_mature = (
                ledger.position_revisions(connection)
            )
            self.assertEqual(after_repair_metadata, before_repair_metadata)
            self.assertGreater(after_repair_mature, before_repair_mature)
            connection.close()

    def test_policy_repair_queries_use_bounded_predicate_index(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            plans = []
            plans.extend(
                connection.execute(
                    """
                    EXPLAIN QUERY PLAN
                    UPDATE positions SET policy_state='diagnostic_shadow'
                    WHERE policy_state='executable'
                      AND (signal_eligible=0 OR validated=0
                           OR direction_conflict=1 OR blocked_by_json <> '[]')
                    """
                ).fetchall()
            )
            plans.extend(
                connection.execute(
                    """
                    EXPLAIN QUERY PLAN
                    UPDATE positions
                    SET policy_state='conflicted_aggressive_shadow'
                    WHERE policy_state='aggressive_shadow'
                      AND direction_conflict=1 AND measurement_version=?
                    """,
                    (ledger.MEASUREMENT_VERSION,),
                ).fetchall()
            )
            details = " ".join(str(row[3]) for row in plans)
            self.assertEqual(details.count("position_policy_repair"), 2)
            self.assertNotIn("SCAN positions", details)
            connection.close()

    def test_cached_publish_invalidates_after_external_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "ledger.sqlite"
            connection = open_database(database)
            candidate = self._diagnostic_candidate()
            open_positions(
                connection,
                [candidate],
                {
                    "EUR_USD": {
                        "bid": 1.1000,
                        "ask": 1.1002,
                        "pip": 0.0001,
                        "quote_epoch": 1_000.0,
                    }
                },
                "2026-08-28T00:00:00+00:00",
                1_000.0,
            )
            mature_positions(
                connection,
                {
                    "EUR_USD": {
                        "bid": 1.1005,
                        "ask": 1.1007,
                        "pip": 0.0001,
                        "quote_epoch": 1_060.0,
                    }
                },
                1_061.0,
            )
            cache = {}
            first_path = root / "first.json"
            second_path = root / "second.json"
            cycle = {"time": "fixed"}
            ledger.publish_state(
                connection,
                first_path,
                cycle,
                summary_cache=cache,
                metadata_dirty=False,
                mature_dirty=False,
            )
            first = json.loads(first_path.read_text(encoding="utf-8"))

            external = open_database(database)
            external.execute(
                "UPDATE positions SET net_pips=net_pips+1.0 "
                "WHERE status='matured'"
            )
            external.commit()
            external.close()

            with mock.patch.object(
                ledger,
                "mature_summary_payload",
                wraps=ledger.mature_summary_payload,
            ) as summary_builder:
                ledger.publish_state(
                    connection,
                    second_path,
                    cycle,
                    summary_cache=cache,
                    metadata_dirty=False,
                    mature_dirty=False,
                )
            second = json.loads(second_path.read_text(encoding="utf-8"))
            self.assertEqual(summary_builder.call_count, 1)
            self.assertNotEqual(
                first["by_horizon"][0]["avg_net_pips"],
                second["by_horizon"][0]["avg_net_pips"],
            )
            connection.close()

    def test_unrelated_external_commit_does_not_rescan_position_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "ledger.sqlite"
            connection = open_database(database)
            cache = {}
            cycle = {"time": "fixed"}
            ledger.publish_state(
                connection,
                root / "first.json",
                cycle,
                summary_cache=cache,
                metadata_dirty=False,
                mature_dirty=False,
            )

            external = sqlite3.connect(database)
            external.execute("CREATE TABLE unrelated(value INTEGER)")
            external.execute("INSERT INTO unrelated VALUES(1)")
            external.commit()
            external.close()

            with mock.patch.object(
                ledger,
                "mature_summary_payload",
                wraps=ledger.mature_summary_payload,
            ) as summary_builder, mock.patch.object(
                ledger,
                "position_metadata_payload",
                wraps=ledger.position_metadata_payload,
            ) as metadata_builder:
                ledger.publish_state(
                    connection,
                    root / "second.json",
                    cycle,
                    summary_cache=cache,
                    metadata_dirty=False,
                    mature_dirty=False,
                )
            self.assertEqual(summary_builder.call_count, 0)
            self.assertEqual(metadata_builder.call_count, 0)
            connection.close()

    def test_recent_position_query_uses_bounded_expression_index(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = open_database(Path(directory) / "ledger.sqlite")
            plan = connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT * FROM positions
                ORDER BY CASE status WHEN 'open' THEN 0 ELSE 1 END,
                         COALESCE(closed_epoch, target_epoch) DESC
                LIMIT 80
                """
            ).fetchall()
            details = " ".join(str(row[3]) for row in plan)
            self.assertIn("position_recent_status_epoch", details)
            self.assertNotIn("USE TEMP B-TREE FOR ORDER BY", details)
            connection.close()


if __name__ == "__main__":
    unittest.main()
