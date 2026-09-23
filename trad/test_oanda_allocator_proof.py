import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from trad.oanda_allocator_proof import (
    AllocatorLineageError,
    allocator_summary,
    compact_candidate,
    comparator_choices,
    ensure_allocator_cohort,
    initialize_database,
    mature_candidates,
    record_decision,
    replay_allocator_active_cohort,
    run_allocator_cycle,
)


def current_account_payload():
    return {
        "aggregate": {
            "snapshot_state": "current",
            "account_values_current": True,
            "positions_current": True,
        },
        "accounts": [{
            "ok": True,
            "account_id": "101-001-37981792-007",
            "account_values_current": True,
            "positions_current": True,
            "trades": [],
        }],
    }


def unavailable_account_payload():
    return {
        "aggregate": {
            "snapshot_state": "unavailable",
            "account_values_current": False,
            "positions_current": False,
        },
        "accounts": [{
            "ok": False,
            "account_id": "101-001-37981792-007",
            "account_values_current": False,
            "positions_current": False,
            "trades": None,
        }],
    }


def config(edge=0.5):
    return {
        "phase": "test",
        "allocator": {
            "policy_id": "allocator_test", "decision_cadence_sec": 300,
            "minimum_incremental_edge_pips": edge, "modeled_slippage_pips": 0.25,
            "maximum_outcome_delay_sec": 15, "sequential_alpha": 0.01,
            "sequential_clip_bound_pips": 250.0,
            "strongest_constituent_family": "seed",
            "selection": {}, "comparators": [], "sampling": {"inclusion_probability": 1.0},
        },
    }


def candidate(candidate_id, net, spread, family="seed"):
    return {
        "candidate_id": candidate_id, "instrument": "EUR_USD", "direction": "buy",
        "family": family, "policy_eligible": True, "cost_adjusted_rank_pips": net,
        "projected_net_pips": net, "raw_predicted_return_pips": net + spread,
        "signal_confidence": 0.6, "spread_pips": spread,
    }


class AllocatorProofTests(unittest.TestCase):
    def test_material_contract_change_creates_new_cohort(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "allocator.sqlite")
            first = ensure_allocator_cohort(connection, config(0.5), start_utc="2026-08-06T00:00:00+00:00")
            repeated = ensure_allocator_cohort(connection, config(0.5), start_utc="2026-08-07T00:00:00+00:00")
            changed = ensure_allocator_cohort(connection, config(1.0), start_utc="2026-08-07T00:00:00+00:00")
            self.assertEqual(first["cohort_id"], repeated["cohort_id"])
            self.assertNotEqual(first["cohort_id"], changed["cohort_id"])
            connection.close()

    def test_superseded_contract_cannot_be_reactivated(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "allocator.sqlite")
            first = ensure_allocator_cohort(
                connection, config(0.5), start_utc="2026-08-06T00:00:00+00:00"
            )
            changed = ensure_allocator_cohort(
                connection, config(1.0), start_utc="2026-08-07T00:00:00+00:00"
            )
            replayed = replay_allocator_active_cohort(
                connection, policy_id="allocator_test", phase="discovery"
            )
            self.assertEqual(replayed["cohort_id"], changed["cohort_id"])
            with self.assertRaises(AllocatorLineageError):
                ensure_allocator_cohort(
                    connection,
                    config(0.5),
                    start_utc="2026-08-08T00:00:00+00:00",
                )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM allocator_cohorts").fetchone()[0],
                2,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM allocator_cohort_transitions"
                ).fetchone()[0],
                2,
            )
            self.assertNotEqual(first["cohort_id"], changed["cohort_id"])
            connection.close()

    def test_transition_replay_fails_closed_on_broken_chain(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "allocator.sqlite")
            first = ensure_allocator_cohort(
                connection, config(), start_utc="2026-08-06T00:00:00+00:00"
            )
            connection.execute(
                "INSERT INTO allocator_cohort_transitions VALUES (?,?,?,?,?,?)",
                (
                    "broken_transition",
                    "allocator_test",
                    "missing_previous_cohort",
                    first["cohort_id"],
                    "2026-08-07T00:00:00+00:00",
                    "test_broken_chain",
                ),
            )
            connection.commit()
            with self.assertRaises(AllocatorLineageError):
                replay_allocator_active_cohort(
                    connection, policy_id="allocator_test", phase="discovery"
                )
            connection.close()

    def test_comparators_are_deterministic_and_include_no_trade(self):
        candidates = [candidate("a", 2.0, 2.0), candidate("b", 1.0, 1.0)]
        first = comparator_choices(candidates, decision_id="d1", config=config(), account_payload=current_account_payload())
        second = comparator_choices(candidates, decision_id="d1", config=config(), account_payload=current_account_payload())
        self.assertEqual(first, second)
        self.assertIsNone(first["no_trade"]["candidate_id"])
        self.assertEqual(first["frozen_policy"]["candidate_id"], "a")
        self.assertEqual(first["lowest_cost_eligible"]["candidate_id"], "b")

    def test_comparators_fail_closed_when_account_state_is_unavailable(self):
        candidates = [candidate("a", 2.0, 2.0)]
        choices = comparator_choices(
            candidates,
            decision_id="d-unavailable",
            config=config(),
            account_payload=unavailable_account_payload(),
        )
        self.assertEqual(choices["no_trade"]["reason"], "fixed_baseline")
        for name, choice in choices.items():
            if name == "no_trade":
                continue
            self.assertIsNone(choice["candidate_id"])
            self.assertEqual(choice["action"], "no_trade")
            self.assertEqual(choice["reason"], "account_state_unavailable")

    def test_candidate_is_ineligible_when_account_state_is_unavailable(self):
        raw = {
            "id": "raw-a",
            "feed_candidate_id": "a",
            "instrument": "EUR_USD",
            "direction": "buy",
            "family": "seed",
            "instant_projected_net_pips": 2.0,
            "projected_gross_movement_pips": 4.0,
            "execution_exit_horizon_sec": 60,
        }
        result = compact_candidate(
            raw,
            quotes={"EUR_USD": {"bid": 1.1, "ask": 1.1002, "pip": 0.0001, "spread_pips": 2.0}},
            account_payload=unavailable_account_payload(),
            executor_payload={},
            config=config(),
            decision_epoch=1000.0,
        )
        self.assertFalse(result["policy_eligible"])
        self.assertIn("account_state_unavailable", result["policy_filter_reasons"])

    def test_maturation_uses_executable_bid_ask(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "allocator.sqlite")
            cohort = ensure_allocator_cohort(connection, config(), start_utc="2026-08-06T00:00:00+00:00")
            connection.execute(
                "INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("d", cohort["cohort_id"], 1, 1000.0, "2026-08-06T00:00:00+00:00", 1, 1000.0, "sha", 1, 1, "c", None, "{}"),
            )
            connection.execute(
                "INSERT INTO decision_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("d", "c", "EUR_USD", "buy", "seed", 60, 1060.0, 1, 1.1000, 1.1002, 0.0001, 2.0, 5.0, 3.0, "{}"),
            )
            connection.execute("INSERT INTO comparator_selections VALUES (?,?,?,?)", ("d", "frozen_policy", "c", "{}"))
            connection.commit()
            quotes = root / "quotes.sqlite"
            qc = sqlite3.connect(quotes)
            qc.execute("CREATE TABLE quote_snapshots_v2(sequence INTEGER,published_epoch REAL,payload_json TEXT)")
            qc.execute(
                "INSERT INTO quote_snapshots_v2 VALUES (2,1061.0,?)",
                (json.dumps({"quotes": {"EUR_USD": {"bid": 1.1005, "ask": 1.1007, "pip": 0.0001}}}),),
            )
            qc.commit(); qc.close()
            result = mature_candidates(connection, quote_database=quotes, config=config(), now_epoch=1070.0)
            self.assertEqual(result["pending_matured"], 1)
            net = connection.execute("SELECT executable_net_pips FROM candidate_outcomes").fetchone()[0]
            self.assertAlmostEqual(net, 3.0)
            summary = allocator_summary(connection, cohort=cohort, config=config())
            self.assertFalse(summary["confirmation"]["practice_canary_auto_route"])
            connection.close()

    def test_empty_eligible_timestamp_does_not_create_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "allocator.sqlite")
            cohort = ensure_allocator_cohort(connection, config(), start_utc="2026-08-06T00:00:00+00:00")
            connection.execute(
                "INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("empty", cohort["cohort_id"], 1, 1000.0, "2026-08-06T00:00:00+00:00", 1, 1000.0, "sha", 4, 0, None, "rejected", "{}"),
            )
            connection.commit()
            summary = allocator_summary(connection, cohort=cohort, config=config())
            self.assertEqual(summary["arms"]["frozen_policy"]["matured_decisions"], 0)
            self.assertEqual(summary["fully_matured_decision_count"], 0)
            connection.close()

    def test_ineligible_universe_does_not_insert_decision_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "allocator.sqlite")
            runtime_config = config()
            runtime_config["allocator"]["selection"] = {
                "require_account_eligible": True
            }
            cohort = ensure_allocator_cohort(
                connection,
                runtime_config,
                start_utc="2026-08-06T00:00:00+00:00",
            )
            signals = root / "signals.sqlite"
            with sqlite3.connect(signals) as signal_connection:
                signal_connection.execute(
                    "CREATE TABLE candidates(candidate_id TEXT,published_epoch REAL,expires_epoch REAL,source TEXT,payload_json TEXT)"
                )
                signal_connection.execute(
                    "INSERT INTO candidates VALUES (?,?,?,?,?)",
                    (
                        "candidate-a",
                        990.0,
                        2000.0,
                        "test",
                        json.dumps(
                            {
                                "id": "candidate-a",
                                "instrument": "EUR_USD",
                                "direction": "buy",
                                "family": "seed",
                                "account_eligible": False,
                                "instant_projected_net_pips": 2.0,
                                "execution_exit_horizon_sec": 60,
                            }
                        ),
                    ),
                )
            signal_connection.close()
            quotes = root / "quotes.sqlite"
            with sqlite3.connect(quotes) as quote_connection:
                quote_connection.execute(
                    "CREATE TABLE quote_snapshots_v2(sequence INTEGER,published_epoch REAL,payload_json TEXT)"
                )
                quote_connection.execute(
                    "INSERT INTO quote_snapshots_v2 VALUES (1,999.0,?)",
                    (
                        json.dumps(
                            {
                                "quotes": {
                                    "EUR_USD": {
                                        "bid": 1.1,
                                        "ask": 1.1002,
                                        "pip": 0.0001,
                                    }
                                }
                            }
                        ),
                    ),
                )
            quote_connection.close()
            account = root / "account.json"
            executor = root / "executor.json"
            account.write_text(json.dumps(current_account_payload()), encoding="utf-8")
            executor.write_text("{}", encoding="utf-8")

            result = record_decision(
                connection,
                cohort=cohort,
                config=runtime_config,
                signal_database=signals,
                quote_database=quotes,
                account_path=account,
                executor_path=executor,
                now_epoch=1000.0,
            )

            self.assertEqual(result["status"], "paused_no_eligible_universe")
            self.assertFalse(result["decision_persisted"])
            self.assertEqual(result["complete_prefilter_candidate_count"], 1)
            self.assertEqual(result["policy_eligible_candidate_count"], 0)
            self.assertEqual(result["filter_reason_counts"], {"account_ineligible": 1})
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM decisions").fetchone()[0], 0)
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM decision_candidates").fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM comparator_selections").fetchone()[0],
                0,
            )
            connection.close()

    def test_paused_cycle_still_matures_pending_outcomes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "allocator.sqlite"
            runtime_config = config()
            connection = initialize_database(database)
            cohort = ensure_allocator_cohort(
                connection,
                runtime_config,
                start_utc="2026-08-06T00:00:00+00:00",
            )
            connection.execute(
                "INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "pending-decision",
                    cohort["cohort_id"],
                    1,
                    1000.0,
                    "2026-08-06T00:00:00+00:00",
                    1,
                    1000.0,
                    "sha",
                    1,
                    1,
                    "pending-candidate",
                    None,
                    "{}",
                ),
            )
            connection.execute(
                "INSERT INTO decision_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "pending-decision",
                    "pending-candidate",
                    "EUR_USD",
                    "buy",
                    "seed",
                    60,
                    1060.0,
                    1,
                    1.1000,
                    1.1002,
                    0.0001,
                    2.0,
                    5.0,
                    3.0,
                    "{}",
                ),
            )
            connection.execute(
                "INSERT INTO comparator_selections VALUES (?,?,?,?)",
                (
                    "pending-decision",
                    "frozen_policy",
                    "pending-candidate",
                    "{}",
                ),
            )
            connection.commit()
            connection.close()

            signals = root / "signals.sqlite"
            with sqlite3.connect(signals) as signal_connection:
                signal_connection.execute(
                    "CREATE TABLE candidates(candidate_id TEXT,published_epoch REAL,expires_epoch REAL,source TEXT,payload_json TEXT)"
                )
            signal_connection.close()
            quotes = root / "quotes.sqlite"
            with sqlite3.connect(quotes) as quote_connection:
                quote_connection.execute(
                    "CREATE TABLE quote_snapshots_v2(sequence INTEGER,published_epoch REAL,payload_json TEXT)"
                )
                quote_connection.execute(
                    "INSERT INTO quote_snapshots_v2 VALUES (2,1061.0,?)",
                    (
                        json.dumps(
                            {
                                "quotes": {
                                    "EUR_USD": {
                                        "bid": 1.1005,
                                        "ask": 1.1007,
                                        "pip": 0.0001,
                                    }
                                }
                            }
                        ),
                    ),
                )
            quote_connection.close()
            config_path = root / "config.json"
            account = root / "account.json"
            executor = root / "executor.json"
            config_path.write_text(json.dumps(runtime_config), encoding="utf-8")
            account.write_text(json.dumps(current_account_payload()), encoding="utf-8")
            executor.write_text("{}", encoding="utf-8")

            result = run_allocator_cycle(
                database_path=database,
                state_path=root / "state.json",
                config_path=config_path,
                signal_database=signals,
                quote_database=quotes,
                account_path=account,
                executor_path=executor,
                now_epoch=1070.0,
            )

            self.assertEqual(result["collection_state"], "paused_no_eligible_universe")
            self.assertEqual(result["last_decision"]["status"], "paused_no_eligible_universe")
            self.assertEqual(result["maturation"]["pending_matured"], 1)
            with sqlite3.connect(database) as verification:
                self.assertEqual(verification.execute("SELECT COUNT(*) FROM decisions").fetchone()[0], 1)
                self.assertEqual(
                    verification.execute("SELECT COUNT(*) FROM candidate_outcomes").fetchone()[0],
                    1,
                )
            verification.close()


if __name__ == "__main__":
    unittest.main()
