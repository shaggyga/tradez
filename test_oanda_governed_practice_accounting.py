import json
import tempfile
import unittest
from pathlib import Path

from trad.oanda_governed_practice_accounting import (
    accounting_summary,
    classify_unclassified_snapshots,
    classify_execution,
    initialize_database,
    record_account_snapshot,
    routeability_sentinel,
)


CONFIG = {
    "allocator": {"modeled_slippage_pips": 0.25},
    "practice_accounting": {"legacy_bucket": "legacy_pre_governance"},
}


def current_account_payload():
    return {
        "time": "2026-08-06T12:00:00+00:00",
        "environment": "practice",
        "aggregate": {
            "snapshot_state": "current",
            "account_values_current": True,
            "positions_current": True,
            "orders_current": True,
        },
        "accounts": [{
            "ok": True,
            "env": "practice",
            "account_id": "101-001-37981792-007",
            "balance": "41.6042",
            "NAV": "41.6042",
            "pl": "-8.3430",
            "marginUsed": "0",
            "openTradeCount": 0,
            "pendingOrderCount": 0,
            "trades": [],
            "account_values_current": True,
            "positions_current": True,
            "orders_current": True,
        }],
    }


def unavailable_account_payload():
    return {
        "time": "2026-08-06T12:05:00+00:00",
        "environment": "practice",
        "aggregate": {
            "snapshot_state": "unavailable",
            "account_values_current": False,
            "positions_current": False,
            "orders_current": False,
        },
        "accounts": [{
            "ok": False,
            "env": "practice",
            "account_id": "101-001-37981792-007",
            "status_code": 503,
            "error": "maintenance",
            "balance": None,
            "NAV": None,
            "pl": None,
            "marginUsed": None,
            "openTradeCount": None,
            "pendingOrderCount": None,
            "trades": None,
            "account_values_current": False,
            "positions_current": False,
            "orders_current": False,
        }],
    }


class GovernedPracticeAccountingTests(unittest.TestCase):
    def test_governed_trade_without_exact_ids_fails_closed(self):
        source = {
            "client_id": "x", "candidate_id": "c", "submitted_epoch": 1.0,
            "status": "filled", "trade_id": "t",
            "payload": {"governed_canary": True, "proof_cohort_id": "proof-only"},
        }
        row = classify_execution(source, {}, CONFIG)
        self.assertFalse(row["attribution_valid"])
        self.assertEqual(row["accounting_bucket"], "unattributed_governed_blocked")

    def test_sentinel_builds_protected_practice_order_without_submission(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "accounting.sqlite")
            account = current_account_payload()
            result = routeability_sentinel(
                connection, account, CONFIG, observed_utc="2026-08-06T12:00:00+00:00"
            )
            self.assertTrue(result["passed"])
            self.assertFalse(result["submission_attempted"])
            self.assertTrue(result["checks"]["broker_stop_present"])
            self.assertTrue(result["checks"]["price_bound_present"])
            connection.close()

    def test_unavailable_snapshot_is_not_recorded_as_financial_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "accounting.sqlite")
            result = record_account_snapshot(connection, unavailable_account_payload())
            self.assertFalse(result["recorded"])
            self.assertEqual(result["state"], "unavailable")
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM account_snapshots").fetchone()[0], 0
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM accounting_integrity_events "
                    "WHERE event_type='account_snapshot_unavailable'"
                ).fetchone()[0],
                1,
            )
            summary = accounting_summary(
                connection, unavailable_account_payload(), root / "missing-allocator.sqlite"
            )["account_operational_continuity"]
            self.assertFalse(summary["account_state_current"])
            self.assertIsNone(summary["balance"])
            self.assertIsNone(summary["open_trade_count"])
            self.assertIsNone(summary["account_flat"])
            connection.close()

    def test_existing_failure_rows_are_quarantined_without_rewriting_history(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "accounting.sqlite"
            connection = initialize_database(database)
            # Simulate rows written by the pre-fix code before the insert guard existed.
            connection.execute("DROP TRIGGER account_snapshots_reject_unavailable")
            invalid = unavailable_account_payload()
            # This reproduces the legacy bug: an unavailable payload persisted as zeros.
            connection.execute(
                "INSERT INTO account_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "bad", invalid["time"], "-007", "", 0.0, 0.0, 0.0,
                    0.0, 0, 0, json.dumps(invalid),
                ),
            )
            valid = current_account_payload()
            connection.execute(
                "INSERT INTO account_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "good", valid["time"], "-007", "1", 41.6042, 41.6042,
                    -8.343, 0.0, 0, 0, json.dumps(valid),
                ),
            )
            connection.commit()
            connection.close()
            connection = initialize_database(database)
            result = classify_unclassified_snapshots(connection)
            self.assertEqual(result, {"classified": 2, "valid": 1, "quarantined": 1})
            states = dict(
                connection.execute(
                    "SELECT snapshot_id,is_valid FROM account_snapshot_validity"
                ).fetchall()
            )
            self.assertEqual(states, {"bad": 0, "good": 1})
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM account_snapshots").fetchone()[0], 2
            )
            self.assertEqual(
                connection.execute(
                    "SELECT balance FROM account_snapshots WHERE snapshot_id='bad'"
                ).fetchone()[0],
                0.0,
            )
            connection.close()

    def test_existing_daily_pass_fails_closed_while_account_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "accounting.sqlite")
            first = routeability_sentinel(
                connection,
                current_account_payload(),
                CONFIG,
                observed_utc="2026-08-06T12:00:00+00:00",
            )
            self.assertTrue(first["passed"])
            unavailable = routeability_sentinel(
                connection,
                unavailable_account_payload(),
                CONFIG,
                observed_utc="2026-08-06T12:05:00+00:00",
            )
            self.assertFalse(unavailable["passed"])
            self.assertTrue(unavailable["stored_passed"])
            self.assertEqual(unavailable["reason"], "account_state_unavailable")
            self.assertFalse(unavailable["checks"]["account_state_current"])
            connection.close()


if __name__ == "__main__":
    unittest.main()
