from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timezone

import oanda_currency_rank_model as model
import oanda_practice_currency_rank_challenger as challenger


UTC = timezone.utc


def snapshot() -> dict:
    now = datetime.now(UTC).isoformat()
    strengths = {
        horizon: {currency: 0.0 for currency in model.EXPECTED_CURRENCIES}
        for horizon in ("5", "15", "60")
    }
    for horizon, multiplier in (("5", 1.0), ("15", 1.2), ("60", 0.8)):
        strengths[horizon].update(
            {"USD": 4.0 * multiplier, "EUR": 2.5 * multiplier,
             "JPY": -2.5 * multiplier, "GBP": -4.0 * multiplier}
        )
    pairs = {}
    for instrument in model.EXPECTED_INSTRUMENTS:
        pairs[instrument] = {
            "instrument": instrument,
            "mid": 1.0,
            "spread_bps": 1.0,
            "windows": {
                window: {"return_bps": 0.0, "return_pips": 0.0}
                for window in ("5", "15", "60")
            },
        }
    # USD strong versus GBP means sell GBP_USD. EUR strong versus JPY means buy EUR_JPY.
    pairs["GBP_USD"]["windows"]["5"].update(return_bps=-4.0, return_pips=-4.0)
    pairs["GBP_USD"]["windows"]["15"].update(return_bps=-7.0, return_pips=-7.0)
    pairs["EUR_JPY"]["windows"]["5"].update(return_bps=3.0, return_pips=3.0)
    pairs["EUR_JPY"]["windows"]["15"].update(return_bps=4.0, return_pips=4.0)
    return {
        "schema_version": model.EXPECTED_SCHEMA,
        "status": "ready",
        "fresh": True,
        "generated_utc": now,
        "instrument_count": 68,
        "horizons": {
            horizon: {
                "currency_count": 21,
                "currency_strength_bps": values,
                "observation_count": 68,
            }
            for horizon, values in strengths.items()
        },
        "pair_moves": pairs,
    }


class CurrencyRankModelTests(unittest.TestCase):
    def test_ranks_all_21_and_selects_disjoint_opposite_trends(self) -> None:
        result = model.rank_snapshot(snapshot())
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["currency_count"], 21)
        self.assertEqual(result["instrument_count"], 68)
        selected = result["selected_pairs"]
        self.assertEqual([row["instrument"] for row in selected], ["GBP_USD", "EUR_JPY"])
        self.assertEqual(selected[0]["direction"], "sell")
        self.assertEqual(selected[1]["direction"], "buy")
        used = []
        for row in selected:
            used.extend((row["strong_currency"], row["weak_currency"]))
        self.assertEqual(len(used), len(set(used)))

    def test_reserved_factor_removes_whole_currency_expression(self) -> None:
        result = model.rank_snapshot(snapshot(), reserved_currencies={"USD"})
        self.assertNotIn("GBP_USD", [row["instrument"] for row in result["selected_pairs"]])
        self.assertGreater(result["rejection_counts"]["currency_factor_already_reserved"], 0)

    def test_missing_currency_fails_closed(self) -> None:
        payload = snapshot()
        del payload["horizons"]["5"]["currency_strength_bps"]["ZAR"]
        result = model.rank_snapshot(payload)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["selected_pairs"], [])
        self.assertIn("currency_universe_mismatch_h5", result["rejection_counts"])

    def test_pair_must_clear_stressed_cost(self) -> None:
        payload = snapshot()
        payload["pair_moves"]["GBP_USD"]["spread_bps"] = 10.0
        result = model.rank_snapshot(payload)
        self.assertNotIn("GBP_USD", [row["instrument"] for row in result["selected_pairs"]])
        self.assertIn("GBP_USD", [row["instrument"] for row in result["rank_theses"]])

    def test_entry_cost_failure_does_not_invalidate_factor_thesis(self) -> None:
        payload = snapshot()
        payload["pair_moves"]["GBP_USD"]["spread_bps"] = 10.0
        result = model.rank_snapshot(payload)
        state, detail = challenger.rank_thesis_state(result, "USD", "GBP")
        self.assertEqual(state, "valid")
        self.assertGreater(detail["score_gap"], 0.0)
        reversed_result = dict(result)
        reversed_result["currency_ranks"] = [
            dict(row, score=-row["score"], strength_5m_bps=-row["strength_5m_bps"], strength_15m_bps=-row["strength_15m_bps"])
            for row in result["currency_ranks"]
        ]
        self.assertEqual(
            challenger.rank_thesis_state(reversed_result, "USD", "GBP")[0],
            "reversed",
        )

    def test_final_order_hook_binds_account_model_and_universe(self) -> None:
        executor = object.__new__(challenger.CurrencyRankPracticeExecutor)
        executor.account_id = "101-001-test-006"
        candidate = {
            "model_id": model.MODEL_ID,
            "lane_id": "rank006.USD-GBP",
            "rank_source_ready": True,
            "rank_currency_count": 21,
            "rank_instrument_count": 68,
            "instrument": "GBP_USD",
            "strong_currency": "USD",
            "weak_currency": "GBP",
            "direction": "sell",
        }
        self.assertEqual(executor.final_submission_blocker(candidate, 100, {}), "")
        forged = dict(candidate, rank_currency_count=20)
        self.assertEqual(
            executor.final_submission_blocker(forged, 100, {}),
            "currency_rank_currency_universe_invalid",
        )

    def test_thesis_reentry_resets_only_after_disappearance(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.execute(
            "CREATE TABLE theses(thesis_id TEXT PRIMARY KEY,active INTEGER NOT NULL,"
            "active_since_utc TEXT NOT NULL,last_seen_utc TEXT NOT NULL,attempted INTEGER NOT NULL,"
            "attempted_utc TEXT NOT NULL DEFAULT '',last_status TEXT NOT NULL DEFAULT '',payload_json TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE attempts(attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,thesis_id TEXT NOT NULL,"
            "attempted_utc TEXT NOT NULL,utc_day TEXT NOT NULL,instrument TEXT NOT NULL,direction TEXT NOT NULL,"
            "status TEXT NOT NULL,fill_count INTEGER NOT NULL,payload_json TEXT NOT NULL)"
        )
        row = {"thesis_id": "x", "instrument": "GBP_USD", "direction": "sell"}
        challenger.synchronize_theses(connection, [row])
        challenger.record_attempt(connection, row, status="not_filled", filled=False)
        self.assertTrue(challenger.thesis_attempted(connection, "x"))
        challenger.synchronize_theses(connection, [row])
        self.assertTrue(challenger.thesis_attempted(connection, "x"))
        challenger.synchronize_theses(connection, [])
        challenger.synchronize_theses(connection, [row])
        self.assertFalse(challenger.thesis_attempted(connection, "x"))


if __name__ == "__main__":
    unittest.main()
