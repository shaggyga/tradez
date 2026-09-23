import json
import math
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

try:
    from oanda_forex_gpt_advisor_all_pairs_v5_24_movement_ledger import (
        LocalDecisionEngine,
    )
    from oanda_one_hour_shadow_signal import (
        CONTRIBUTORS,
        build_current_market_quote_snapshot,
        build_market_quote_snapshot,
        generate_one_hour_forecasts,
        parse_args,
        read_json_if_changed,
        register_contributors,
        run,
        summarize_shadow_family_calibration,
    )
    from oanda_model_gap_live_signal_worker import LiveForecastLedger
    from oanda_signal_contribution_feed import SignalContributionFeed
except ModuleNotFoundError:
    from trad.oanda_forex_gpt_advisor_all_pairs_v5_24_movement_ledger import (
        LocalDecisionEngine,
    )
    from trad.oanda_one_hour_shadow_signal import (
        CONTRIBUTORS,
        build_current_market_quote_snapshot,
        build_market_quote_snapshot,
        generate_one_hour_forecasts,
        parse_args,
        read_json_if_changed,
        register_contributors,
        run,
        summarize_shadow_family_calibration,
    )
    from trad.oanda_model_gap_live_signal_worker import LiveForecastLedger
    from trad.oanda_signal_contribution_feed import SignalContributionFeed


def price_series(count: int, step: float, cycle: float) -> list[float]:
    return [
        1.1000 + step * index + cycle * math.sin(index / 9.0)
        for index in range(count)
    ]


def snapshot() -> dict:
    now = time.time()
    return {
        "snapshot_id": "unit-one-hour",
        "generated_epoch": now,
        "generated_utc": "2026-07-28T16:00:00+00:00",
        "instrument_count": 1,
        "instruments": {
            "EUR_USD": {
                "feature_origin_utc": "2026-07-28T15:59:00+00:00",
                "quote": {"bid": 1.1050, "ask": 1.1051},
                "features": {
                    "pip": 0.0001,
                    "m1_atr14_pips": 1.4,
                    "live_spread_pips": 1.0,
                    "cross_strength_r1": 0.8,
                    "cross_strength_r3": 1.2,
                    "relative_residual_r3": 0.6,
                    "cross_breadth_r3": 0.7,
                    "cross_sample_count": 8,
                    "pair_rank_r3": 0.8,
                    "volume_ratio_12": 1.4,
                },
                "series": {
                    "M1": price_series(512, 0.000002, 0.00008),
                    "M5": price_series(500, 0.000004, 0.00012),
                    "M15": price_series(166, 0.000010, 0.00016),
                    "M30": price_series(82, 0.000020, 0.00018),
                    "H1": price_series(500, 0.000030, 0.00024),
                },
            }
        },
    }


class OneHourShadowSignalTests(unittest.TestCase):
    def test_market_quote_snapshot_keeps_only_fresh_executable_quotes(self):
        payload = {
            "producer": "unit-stream",
            "quotes": {
                "EUR_USD": {
                    "bid": 1.1000,
                    "ask": 1.1002,
                    "time": "1970-01-01T00:16:35+00:00",
                },
                "GBP_USD": {
                    "bid": 1.2000,
                    "ask": 1.2002,
                    "time": "1970-01-01T00:15:00+00:00",
                },
            },
        }
        result = build_market_quote_snapshot(
            payload,
            now=1000.0,
            max_quote_age_sec=30.0,
        )

        self.assertEqual(result["source"], "unit-stream")
        self.assertEqual(result["generated_epoch"], 995.0)
        self.assertEqual(set(result["instruments"]), {"EUR_USD"})

    def test_current_quote_snapshot_does_not_reuse_old_cycle_clock(self):
        payload = {
            "producer": "unit-stream",
            "quotes": {
                "EUR_USD": {
                    "bid": 1.1000,
                    "ask": 1.1002,
                    "time": "1970-01-01T00:18:40+00:00",
                }
            },
        }

        result = build_current_market_quote_snapshot(
            payload,
            max_quote_age_sec=30.0,
            clock=lambda: 1130.0,
        )

        self.assertEqual(result["generated_epoch"], 1120.0)
        self.assertEqual(set(result["instruments"]), {"EUR_USD"})

    def test_market_quote_snapshot_normalizes_common_broker_clock_skew(self):
        payload = {
            "producer": "unit-stream",
            "generated_utc": "1970-01-01T00:16:40+00:00",
            "quotes": {
                "EUR_USD": {
                    "bid": 1.1000,
                    "ask": 1.1002,
                    "time": "1970-01-01T00:17:40+00:00",
                },
                "GBP_USD": {
                    "bid": 1.2000,
                    "ask": 1.2002,
                    "time": "1970-01-01T00:17:39+00:00",
                },
            },
        }

        result = build_market_quote_snapshot(
            payload,
            now=1001.0,
            max_quote_age_sec=30.0,
        )

        self.assertEqual(set(result["instruments"]), {"EUR_USD", "GBP_USD"})
        self.assertAlmostEqual(result["transport_clock_offset_sec"], 59.5)
        self.assertAlmostEqual(result["generated_epoch"], 999.5)

    def test_feature_snapshot_is_reused_until_file_changes(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "snapshot.json"
            path.write_text('{"snapshot_id": "one"}', encoding="utf-8")
            first, mtime_ns, changed = read_json_if_changed(path, {}, None)
            second, same_mtime_ns, changed_again = read_json_if_changed(
                path,
                first,
                mtime_ns,
            )

        self.assertTrue(changed)
        self.assertFalse(changed_again)
        self.assertIs(second, first)
        self.assertEqual(same_mtime_ns, mtime_ns)

    def test_maintenance_defaults_are_slower_than_snapshot_polling(self):
        args = parse_args([])

        self.assertEqual(args.interval_sec, 2.0)
        self.assertEqual(args.state_interval_sec, 30.0)
        self.assertEqual(args.family_audit_interval_sec, 900.0)
        self.assertGreater(args.state_interval_sec, args.interval_sec)

    def test_mature_only_finishes_pending_without_publishing_new_forecasts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            feature_path = root / "features.json"
            quotes_path = root / "quotes.json"
            feed_path = root / "shared-feed.sqlite"
            normalizer_path = root / "normalizer.sqlite"
            ledger_path = root / "ledger.sqlite"
            state_path = root / "state.json"
            lock_path = root / "worker.lock"

            old_snapshot = snapshot()
            old_snapshot["generated_epoch"] = time.time() - 3605.0
            old_snapshot["snapshot_id"] = "mature-only-old"
            feature_path.write_text(json.dumps(old_snapshot), encoding="utf-8")
            now = time.time()
            quotes_path.write_text(
                json.dumps(
                    {
                        "producer": "unit-test",
                        "quotes": {
                            "EUR_USD": {
                                "bid": 1.1060,
                                "ask": 1.1061,
                                "time": time.strftime(
                                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)
                                ),
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            normalizer = SignalContributionFeed(normalizer_path)
            raw = generate_one_hour_forecasts(old_snapshot)
            normalized = [
                normalizer.normalize_forecast(
                    row,
                    "unit-one-hour",
                    now=old_snapshot["generated_epoch"],
                    max_age_sec=7200.0,
                )
                for row in raw
            ]
            normalizer.close()
            ledger = LiveForecastLedger(ledger_path)
            inserted = ledger.register(
                normalized,
                old_snapshot["snapshot_id"],
                minimum_target_epoch=0.0,
            )
            self.assertEqual(inserted, len(CONTRIBUTORS))
            ledger.close()

            args = parse_args(
                [
                    "--feature-snapshot", str(feature_path),
                    "--market-quotes", str(quotes_path),
                    "--signal-feed", str(feed_path),
                    "--ledger", str(ledger_path),
                    "--state", str(state_path),
                    "--process-lock", str(lock_path),
                    "--max-quote-age-sec", "120",
                    "--max-outcome-delay-sec", "7200",
                    "--mature-only",
                    "--once",
                ]
            )
            self.assertEqual(run(args), 0)
            state = json.loads(state_path.read_text(encoding="utf-8"))
            finished = LiveForecastLedger(ledger_path)
            pending = finished.summary()["pending"]
            matured = finished.connection.execute(
                "SELECT COUNT(*) FROM outcomes WHERE status='matured'"
            ).fetchone()[0]
            finished.close()

        self.assertFalse(feed_path.exists())
        self.assertEqual(state["runtime_mode"], "mature_only")
        self.assertEqual(state["status"], "drained_retired")
        self.assertFalse(state["contract"]["future_forecast_production"])
        self.assertEqual(state["session"]["published_forecasts"], 0)
        self.assertEqual(pending, 0)
        self.assertEqual(matured, len(CONTRIBUTORS))

    def test_family_audit_is_nonoverlapping_factor_aware_and_order_incapable(self):
        connection = sqlite3.connect(":memory:")
        connection.execute(
            """
            CREATE TABLE outcomes(
                candidate_id TEXT, family TEXT, instrument TEXT,
                horizon_sec INTEGER, generated_epoch REAL, observed_epoch REAL,
                direction TEXT, direction_correct INTEGER,
                executable_net_pips REAL, signed_mid_move_pips REAL,
                entry_spread_pips REAL, predicted_magnitude_pips REAL,
                snapshot_id TEXT, status TEXT
            )
            """
        )
        families = [row["family"] for row in CONTRIBUTORS]
        rows = []
        for event, instrument in enumerate(("EUR_USD", "GBP_USD")):
            generated = 1000.0 + 3600.0 * event
            for index, family in enumerate(families):
                unique = index == 0
                direction = "buy" if unique else "sell"
                rows.append(
                    (
                        f"{event}-{family}", family, instrument, 3600,
                        generated, generated + 3600, direction, int(unique),
                        6.0 if unique else -8.0, 8.0, 2.0, 4.0,
                        f"snapshot-{event}", "matured",
                    )
                )
                # Same family/pair/hour must not inflate the scorecard.
                rows.append(
                    (
                        f"{event}-{family}-repeat", family, instrument, 3600,
                        generated + 5, generated + 3605, direction, int(unique),
                        6.0 if unique else -8.0, 8.0, 2.0, 4.0,
                        f"snapshot-{event}-repeat", "matured",
                    )
                )
        connection.executemany(
            "INSERT INTO outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
        )

        report = summarize_shadow_family_calibration(
            connection,
            now_epoch=1000.0 + 7200.0,
            window_days=1,
        )
        connection.close()

        self.assertFalse(report["promotion_eligible"])
        self.assertFalse(report["execution_eligible"])
        self.assertEqual(report["holdout_candidates"], [])
        cells = {row["family"]: row for row in report["family_cells"]}
        self.assertEqual(cells[families[0]]["non_overlapping_observations"], 2)
        minority = {
            row["family"]: row for row in report["unique_minority_cells"]
        }
        self.assertEqual(minority[families[0]]["independent_factor_episodes"], 2)
        self.assertEqual(minority[families[0]]["after_cost_win_rate"], 1.0)

    def test_generates_all_distinct_one_hour_families(self):
        rows = generate_one_hour_forecasts(snapshot())

        self.assertEqual(len(rows), len(CONTRIBUTORS))
        self.assertEqual(
            {row["model_id"] for row in rows},
            {row["contributor_id"] for row in CONTRIBUTORS},
        )
        for row in rows:
            self.assertEqual(row["input_timeframe"], "MULTI")
            self.assertFalse(row["account_eligible"])
            self.assertEqual(set(row["forecast_curve"]), {"3600"})
            point = row["forecast_curve"]["3600"]
            self.assertTrue(0.0 < point["probability_up"] < 1.0)
            self.assertTrue(math.isfinite(point["predicted_signed_pips"]))
            self.assertGreater(point["predicted_magnitude_pips"], 0.0)
            self.assertFalse(point["account_eligible"])

    def test_feed_keeps_new_families_research_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            feed = SignalContributionFeed(Path(temporary) / "feed.sqlite")
            register_contributors(feed)
            result = feed.publish_forecasts(
                generate_one_hour_forecasts(snapshot()),
                "unit-one-hour",
                600.0,
                max_age_sec=600.0,
            )
            recent = feed.recent(limit=20)
            policies = {
                contributor_id: account_eligible
                for contributor_id, account_eligible in feed.connection.execute(
                    """
                    SELECT contributor_id, account_eligible
                    FROM contributor_registry
                    WHERE source_kind = 'one_hour_shadow'
                    """
                )
            }
            feed.close()

        self.assertEqual(result["accepted"], len(CONTRIBUTORS))
        self.assertEqual(result["rejected"], 0)
        self.assertEqual(len(recent), len(CONTRIBUTORS))
        self.assertTrue(all(row["research_only"] for row in recent))
        self.assertEqual(policies, {
            row["contributor_id"]: 0 for row in CONTRIBUTORS
        })

    def test_local_decision_engine_never_creates_orders(self):
        engine = LocalDecisionEngine(object())
        decision = engine.create_decision(
            {
                "as_of_utc": "2026-07-28T16:00:00Z",
                "market_snapshots": [
                    {
                        "instrument": "EUR_USD",
                        "news_context": {"active_event_count": 2},
                    }
                ],
                "open_trades": [
                    {
                        "trade_id": "42",
                        "instrument": "EUR_USD",
                        "direction": "SHORT",
                    }
                ],
            }
        )

        self.assertEqual(decision["decision_engine"], "local_only")
        self.assertEqual(decision["orders_to_execute"], [])
        self.assertEqual(decision["new_trade_candidates"], [])
        self.assertEqual(len(decision["open_position_actions"]), 1)
        self.assertEqual(decision["open_position_actions"][0]["action"], "HOLD")


if __name__ == "__main__":
    unittest.main()
