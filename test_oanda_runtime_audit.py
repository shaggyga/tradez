import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from trad import oanda_runtime_audit as audit


class RuntimeAuditTests(unittest.TestCase):
    def test_read_json_retries_transient_file_lock(self):
        path = mock.Mock(spec=Path)
        path.read_text.side_effect = [PermissionError("locked"), '{"ok": true}']

        payload = audit.read_json(path, attempts=2, retry_delay_sec=0.0)

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(path.read_text.call_count, 2)

    def test_runtime_snapshot_marks_failed_account_read_unhealthy(self):
        account = {
            "ok": False,
            "balance": 0.0,
        }
        feed = {"error": "", "executions": 0}
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            audit, "state_health", return_value={}
        ), mock.patch.object(
            audit, "dashboard_health", return_value={"ok": True}
        ), mock.patch.object(
            audit, "account_summary", return_value=account
        ), mock.patch.object(
            audit, "signal_feed_summary", return_value=feed
        ), mock.patch.object(
            audit, "combination_summary", return_value={}
        ), mock.patch.object(
            audit, "storage_summary", return_value={}
        ):
            payload = audit.runtime_snapshot(
                Path(temporary),
                Path(temporary) / "vault.zip",
                "http://127.0.0.1:8765/",
                900.0,
            )

        self.assertFalse(payload["overall_ok"])
        self.assertIs(payload["account_007"], account)

    def test_account_summary_preserves_unavailable_values_as_unknown(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = root / "state"
            state.mkdir()
            (state / "account_007_dashboard_v1.json").write_text(
                json.dumps(
                    {
                        "environment": "practice",
                        "time": "2026-08-28T23:00:00+00:00",
                        "accounts": [
                            {
                                "account_id": "101-001-37981792-007",
                                "env": "practice",
                                "ok": False,
                                "account_values_current": False,
                                "positions_current": False,
                                "orders_current": False,
                                "status_code": 503,
                                "error": "maintenance",
                            }
                        ],
                        "aggregate": {
                            "snapshot_state": "unavailable",
                            "account_values_current": False,
                            "positions_current": False,
                            "orders_current": False,
                        },
                    }
                ),
                encoding="utf-8",
            )

            summary = audit.account_summary(root)

        self.assertFalse(summary["ok"])
        self.assertFalse(summary["account_current"])
        self.assertEqual(summary["snapshot_state"], "unavailable")
        self.assertIsNone(summary["balance"])
        self.assertIsNone(summary["nav"])
        self.assertIsNone(summary["open_trade_count"])
        self.assertIsNone(summary["pending_order_count"])
        self.assertIsNone(summary["trades"])

    def test_runtime_snapshot_marks_signal_feed_error_unhealthy(self):
        account = {"ok": True, "balance": 42.5}
        feed = {"error": "locked", "executions": 0}
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            audit, "state_health", return_value={}
        ), mock.patch.object(
            audit, "dashboard_health", return_value={"ok": True}
        ), mock.patch.object(
            audit, "account_summary", return_value=account
        ), mock.patch.object(
            audit, "signal_feed_summary", return_value=feed
        ), mock.patch.object(
            audit, "combination_summary", return_value={}
        ), mock.patch.object(
            audit, "storage_summary", return_value={}
        ):
            payload = audit.runtime_snapshot(
                Path(temporary),
                Path(temporary) / "vault.zip",
                "http://127.0.0.1:8765/",
                900.0,
            )

        self.assertFalse(payload["overall_ok"])


if __name__ == "__main__":
    unittest.main()
