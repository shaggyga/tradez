from __future__ import annotations

import tempfile
import sqlite3
import unittest
from pathlib import Path

from fresh_m1_intrahour.src.practice_007_forecast_outcome_monitor import (
    build_summary,
    connect_database,
    executable_net_pips,
    mature_forecasts,
    projected_gross_pips,
    read_json_cached,
    signed_mid_move_pips,
    stable_generation,
)
from fresh_m1_intrahour.src.practice_007_monitor_final_analysis import (
    greedy_nonoverlap,
    largest_observed_moves,
)


class Practice007ForecastOutcomeMonitorTests(unittest.TestCase):
    def test_buy_outcome_uses_entry_ask_and_exit_bid(self) -> None:
        result = executable_net_pips(
            "buy", 1.1000, 1.1002, 1.1005, 1.1007, 0.0001
        )
        self.assertAlmostEqual(result, 3.0)

    def test_sell_outcome_uses_entry_bid_and_exit_ask(self) -> None:
        result = executable_net_pips(
            "sell", 1.1000, 1.1002, 1.0995, 1.0997, 0.0001
        )
        self.assertAlmostEqual(result, 3.0)

    def test_signed_mid_move_is_direction_relative(self) -> None:
        buy = signed_mid_move_pips(
            "buy", 1.1000, 1.1002, 1.1005, 1.1007, 0.0001
        )
        sell = signed_mid_move_pips(
            "sell", 1.1000, 1.1002, 1.1005, 1.1007, 0.0001
        )
        self.assertAlmostEqual(buy, 5.0)
        self.assertAlmostEqual(sell, -5.0)

    def test_stable_generation_requires_raw_and_consolidated_coverage(self) -> None:
        self.assertTrue(
            stable_generation(
                {
                    "raw_candidate_count": 500,
                    "consolidated_signal_count": 32,
                },
                500,
                32,
            )
        )
        self.assertFalse(
            stable_generation(
                {
                    "raw_candidate_count": 499,
                    "consolidated_signal_count": 32,
                },
                500,
                32,
            )
        )
        self.assertFalse(
            stable_generation(
                {
                    "raw_candidate_count": 500,
                    "consolidated_signal_count": 31,
                },
                500,
                32,
            )
        )

    def test_projected_gross_is_recovered_from_ratio_and_spread(self) -> None:
        self.assertAlmostEqual(
            projected_gross_pips(None, 1.5, 2.0),
            3.0,
        )
        self.assertAlmostEqual(
            projected_gross_pips(4.0, 1.5, 2.0),
            4.0,
        )
        self.assertIsNone(projected_gross_pips(None, None, 2.0))

    def test_snapshot_cache_reloads_only_after_file_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "snapshot.json"
            cache = {}
            path.write_text('{"value": 1}', encoding="utf-8")
            first = read_json_cached(path, cache)
            second = read_json_cached(path, cache)
            self.assertIs(first, second)

            path.write_text('{"value": 22}', encoding="utf-8")
            third = read_json_cached(path, cache)

        self.assertEqual(third["value"], 22)
        self.assertIsNot(first, third)

    def test_largest_move_attaches_preceding_forecast(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE quote_samples (
                quote_epoch REAL,
                instrument TEXT,
                bid REAL,
                ask REAL,
                pip REAL
            );
            CREATE TABLE forecasts (
                captured_epoch REAL,
                instrument TEXT,
                horizon_sec INTEGER,
                direction TEXT,
                signal_confidence REAL,
                projected_gross_movement_pips REAL,
                projected_net_pips REAL,
                signal_eligible INTEGER,
                blockers_json TEXT,
                best_family TEXT,
                best_model_id TEXT,
                qualified_signal_count INTEGER,
                stable_generation INTEGER
            );
            """
        )
        connection.executemany(
            "INSERT INTO quote_samples VALUES (?, ?, ?, ?, ?)",
            [
                (1000.0, "EUR_USD", 1.0000, 1.0002, 0.0001),
                (1300.0, "EUR_USD", 1.0010, 1.0012, 0.0001),
            ],
        )
        connection.execute(
            "INSERT INTO forecasts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                990.0,
                "EUR_USD",
                300,
                "buy",
                0.60,
                10.0,
                8.0,
                0,
                '["unvalidated_signal"]',
                "momentum",
                "momentum.fast",
                0,
                1,
            ),
        )

        result = largest_observed_moves(
            connection,
            1000.0,
            1300.0,
            horizons_sec=(300,),
        )
        connection.close()

        self.assertEqual(result["observations"], 1)
        self.assertEqual(result["direction_caught"], 1)
        self.assertEqual(result["post_spread_caught"], 1)
        self.assertFalse(result["moves"][0]["signal_eligible"])

    def test_nonoverlap_is_enforced_per_pair_and_horizon(self) -> None:
        rows = [
            {
                "instrument": "EUR_USD",
                "horizon_sec": 1800,
                "captured_epoch": 0.0,
                "due_epoch": 1800.0,
            },
            {
                "instrument": "EUR_USD",
                "horizon_sec": 1800,
                "captured_epoch": 900.0,
                "due_epoch": 2700.0,
            },
            {
                "instrument": "EUR_USD",
                "horizon_sec": 1800,
                "captured_epoch": 1800.0,
                "due_epoch": 3600.0,
            },
            {
                "instrument": "GBP_USD",
                "horizon_sec": 1800,
                "captured_epoch": 900.0,
                "due_epoch": 2700.0,
            },
        ]
        selected = greedy_nonoverlap(rows)
        self.assertEqual(
            [(row["instrument"], row["captured_epoch"]) for row in selected],
            [
                ("EUR_USD", 0.0),
                ("GBP_USD", 900.0),
                ("EUR_USD", 1800.0),
            ],
        )

    def test_preferred_summary_joins_the_exact_capture_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect_database(Path(temporary) / "monitor.sqlite")
            connection.execute(
                """
                INSERT INTO monitor_samples (
                    captured_epoch, captured_utc, raw_candidate_count,
                    consolidated_signal_count, qualified_signal_count,
                    stable_generation
                ) VALUES (1000, 'capture-1000', 500, 32, 0, 1)
                """
            )
            for captured_epoch, row_id in (
                (1000.0, "signal-1000"),
                (1015.0, "signal-1015"),
            ):
                connection.execute(
                    """
                    INSERT INTO signal_rows (
                        signal_row_id, captured_epoch, signal_snapshot_utc,
                        feature_snapshot_utc, instrument, signal_rank,
                        direction, preferred_horizon_sec, direction_conflict,
                        signal_eligible, blockers_json, selected,
                        stable_generation
                    ) VALUES (?, ?, 'same-signal', 'same-feature', 'EUR_USD',
                              1, 'buy', 60, 0, 0, '[]', 0, 1)
                    """,
                    (row_id, captured_epoch),
                )
            connection.execute(
                """
                INSERT INTO forecasts (
                    observation_id, captured_epoch, captured_utc,
                    signal_snapshot_utc, feature_snapshot_utc,
                    entry_quote_epoch, quote_age_sec, due_epoch, instrument,
                    direction, horizon_sec, pip, entry_bid, entry_ask,
                    entry_spread_pips, signal_eligible, direction_conflict,
                    blockers_json, raw_candidate_count,
                    qualified_signal_count, stable_generation, matured_epoch,
                    signed_mid_move_pips, executable_net_pips,
                    direction_correct, net_positive
                ) VALUES (
                    'forecast-1000', 1000, 'capture-1000', 'same-signal',
                    'same-feature', 1000, 0, 1060, 'EUR_USD', 'buy', 60,
                    0.0001, 1.1000, 1.1002, 2.0, 0, 0, '[]', 500, 0, 1,
                    1060, 3.0, 1.0, 1, 1
                )
                """
            )
            connection.commit()

            summary = build_summary(connection)
            connection.close()

        self.assertEqual(summary["counts"]["stable_preferred_forecasts"], 1)
        self.assertEqual(
            summary["counts"]["stable_rank_one_preferred_forecasts"],
            1,
        )

    def test_maturation_batches_quotes_and_censors_unobservable_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect_database(Path(temporary) / "monitor.sqlite")
            connection.executemany(
                """
                INSERT INTO quote_samples (
                    quote_epoch, captured_epoch, instrument, quote_utc,
                    bid, ask, pip
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        1000.0,
                        1000.0,
                        "EUR_USD",
                        "entry",
                        1.1000,
                        1.1002,
                        0.0001,
                    ),
                    (
                        1060.0,
                        1060.0,
                        "EUR_USD",
                        "exit",
                        1.1005,
                        1.1007,
                        0.0001,
                    ),
                ],
            )
            required = """
                observation_id, captured_epoch, captured_utc,
                entry_quote_epoch, quote_age_sec, due_epoch, instrument,
                direction, horizon_sec, pip, entry_bid, entry_ask,
                entry_spread_pips, signal_eligible, direction_conflict,
                blockers_json, raw_candidate_count,
                qualified_signal_count, stable_generation
            """
            connection.execute(
                f"""
                INSERT INTO forecasts ({required})
                VALUES (
                    'mature', 1000, 'capture', 1000, 0, 1060,
                    'EUR_USD', 'buy', 60, 0.0001, 1.1000, 1.1002,
                    2.0, 0, 0, '[]', 500, 0, 1
                )
                """
            )
            connection.execute(
                f"""
                INSERT INTO forecasts ({required})
                VALUES (
                    'censor', 1000, 'capture', 1000, 0, 1070,
                    'GBP_USD', 'buy', 60, 0.0001, 1.2500, 1.2502,
                    2.0, 0, 0, '[]', 500, 0, 1
                )
                """
            )
            connection.commit()

            matured = mature_forecasts(connection, 1300.0)
            mature_row = connection.execute(
                """
                SELECT executable_net_pips, censored_epoch
                FROM forecasts WHERE observation_id = 'mature'
                """
            ).fetchone()
            censor_row = connection.execute(
                """
                SELECT matured_epoch, censor_reason
                FROM forecasts WHERE observation_id = 'censor'
                """
            ).fetchone()
            connection.close()

        self.assertEqual(matured, 1)
        self.assertAlmostEqual(mature_row["executable_net_pips"], 3.0)
        self.assertIsNone(mature_row["censored_epoch"])
        self.assertIsNone(censor_row["matured_epoch"])
        self.assertEqual(
            censor_row["censor_reason"],
            "no_exit_quote_within_delay",
        )


if __name__ == "__main__":
    unittest.main()
