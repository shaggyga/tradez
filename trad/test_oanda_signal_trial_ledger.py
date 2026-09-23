import tempfile
import unittest
from pathlib import Path

from trad import oanda_signal_trial_ledger as trial


def candidate(
    instrument="EUR_USD",
    direction="buy",
    horizon=300,
    gross_to_spread=2.5,
    confidence=0.56,
):
    return {
        "signal_id": f"{instrument}-{direction}-{horizon}",
        "instrument": instrument,
        "direction": direction,
        "horizon_sec": horizon,
        "signal_confidence": confidence,
        "projected_net_pips": 2.0,
        "gross_to_spread": gross_to_spread,
    }


def news_for(pair, direction="BULLISH", event_id="event-one", eligible=True):
    return {
        "pairs": {
            pair: {
                "directional_state": direction,
                "events": [{
                    "event_id": event_id,
                    "age_minutes": 5,
                    "severity": 75,
                    "execution_eligible": eligible,
                    "reports_prior_market_move": False,
                    "expected_pair_direction": direction,
                }],
            }
        }
    }


class SignalTrialLedgerTests(unittest.TestCase):
    def test_opportunity_gate_is_explicitly_spread_normalized(self):
        self.assertTrue(trial.candidate_is_opportunity(candidate()))
        self.assertFalse(
            trial.candidate_is_opportunity(candidate(gross_to_spread=1.99))
        )

    def test_h2_direction_pair_preserves_source_and_adds_inverse(self):
        row = candidate("EUR_USD", "buy", horizon=7200)
        selected = trial.arm_candidates(
            "opportunity_price_top1_direction_pair_h2", [row], {}
        )

        self.assertEqual(len(selected), 2)
        self.assertEqual([item["direction"] for item in selected], ["buy", "sell"])
        self.assertEqual(selected[1]["source_direction"], "buy")
        self.assertEqual(selected[1]["source_arm"], "opportunity_price_top1")
        self.assertEqual(selected[1]["signal_id"], row["signal_id"])
        self.assertIn(
            "currency:USD:long", selected[1]["correlation_factor_ids"]
        )
        self.assertEqual(
            trial.arm_candidates(
                "opportunity_price_top1_direction_pair_h2",
                [candidate("EUR_USD", "buy", horizon=3600)],
                {},
            ),
            [],
        )

    def test_strict_h2_pair_requires_validated_directional_cost_evidence(self):
        row = candidate("EUR_USD", "buy", horizon=7200)
        row.update(
            {
                "directional_gross_to_spread": 2.25,
                "signal_eligible": True,
                "validated": True,
                "direction_conflict": False,
                "blocked_by": [],
                "policy_state": "executable",
                "execution_validation": {
                    "validated": True,
                    "negative_historical_warmup": False,
                    "promotion_rejected": False,
                },
            }
        )

        selected = trial.arm_candidates(
            "h2_strict_validated_direction_pair", [row], {}
        )

        self.assertEqual([item["direction"] for item in selected], ["buy", "sell"])
        self.assertEqual(selected[1]["source_arm"], "h2_strict_validated")
        self.assertEqual(
            trial.arm_candidates(
                "h2_strict_validated_direction_pair",
                [dict(row, directional_gross_to_spread=None)],
                {},
            ),
            [],
        )
        rejected = dict(row)
        rejected["execution_validation"] = {
            "validated": True,
            "promotion_rejected": True,
        }
        self.assertEqual(
            trial.arm_candidates(
                "h2_strict_validated_direction_pair", [rejected], {}
            ),
            [],
        )

    def test_cohort_persists_horizon_lineage_and_news_snapshot(self):
        row = candidate("EUR_USD", "buy", horizon=7200)
        row.update(
            {
                "family": "h2_breakout",
                "lane_id": "h2_breakout.fast",
                "input_timeframe": "M15",
                "direction_source": "raw_all_signal_consensus",
                "policy_state": "diagnostic_shadow",
                "signal_eligible": False,
                "validated": False,
                "direction_conflict": True,
                "blocked_by": ["unvalidated_signal"],
                "execution_validation": {
                    "validated": False,
                    "promotion_rejected": True,
                },
            }
        )
        news = news_for("EUR_USD")
        news["pairs"]["EUR_USD"]["events"][0].update(
            {
                "headline": "ECB policy headline",
                "first_known_utc": "2026-08-04T12:00:01+00:00",
                "published_utc": "2026-08-04T12:00:00+00:00",
            }
        )
        member = trial.arm_candidates("opportunity_price_top1", [row], news)[0]
        quotes = {"EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}}
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_price_top1",
                    7200,
                    [member],
                    quotes,
                    1_000.0,
                )
            )
            stored = connection.execute(
                """
                SELECT strategy_family, lane_id, input_timeframe,
                       direction_source, policy_state, direction_conflict,
                       blocked_by_json, execution_validation_json,
                       news_context_json
                FROM positions
                """
            ).fetchone()
            connection.close()

        self.assertEqual(stored[0], "h2_breakout")
        self.assertEqual(stored[1], "h2_breakout.fast")
        self.assertEqual(stored[2], "M15")
        self.assertEqual(stored[3], "raw_all_signal_consensus")
        self.assertEqual(stored[4], "diagnostic_shadow")
        self.assertEqual(stored[5], 1)
        self.assertIn("unvalidated_signal", stored[6])
        self.assertIn("promotion_rejected", stored[7])
        self.assertIn("ECB policy headline", stored[8])

    def test_h2_inverse_historical_summary_pays_opposite_bid_ask_cost(self):
        row = candidate("EUR_USD", "buy", horizon=7200)
        entry = {"EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}}
        exit_quotes = {
            "EUR_USD": {"bid": 1.0990, "ask": 1.0992, "pip": 0.0001}
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_price_top1",
                    7200,
                    [row],
                    entry,
                    1_000.0,
                )
            )
            self.assertEqual(
                trial.mature_cohorts(connection, exit_quotes, 8_201.0), 1
            )
            summary = trial.summarized_h2_inverse_shadow(connection)
            connection.close()

        population = next(
            item
            for item in summary["retrospective"]
            if item["population"] == "opportunity_price_top1"
        )
        self.assertAlmostEqual(population["original"]["total_net_pips"], -12.0)
        self.assertAlmostEqual(population["inverse"]["total_net_pips"], 8.0)
        self.assertFalse(summary["can_change_execution"])

    def test_prospective_inverse_arm_matures_as_a_normal_no_order_cohort(self):
        source = candidate("USD_JPY", "sell", horizon=7200)
        members = trial.arm_candidates(
            "h2_persistence_direction_pair", [source], {}
        )
        entry = {"USD_JPY": {"bid": 150.00, "ask": 150.02, "pip": 0.01}}
        exit_quotes = {
            "USD_JPY": {"bid": 150.10, "ask": 150.12, "pip": 0.01}
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertEqual(
                [member["direction"] for member in members], ["sell", "buy"]
            )
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "h2_persistence_direction_pair",
                    7200,
                    members,
                    entry,
                    1_000.0,
                )
            )
            self.assertEqual(
                trial.mature_cohorts(connection, exit_quotes, 8_201.0), 1
            )
            summary = trial.summarized_h2_inverse_shadow(connection)
            connection.close()

        arm = next(
            item
            for item in summary["prospective_direction_pairs"]
            if item["arm"] == "h2_persistence_direction_pair"
        )
        self.assertEqual(arm["matched_pairs"], 1)
        self.assertAlmostEqual(arm["original"]["total_net_pips"], -12.0)
        self.assertAlmostEqual(arm["inverse"]["total_net_pips"], 8.0)

    def test_conflicted_aggressive_h1_direction_pair_is_prospective_and_matched(self):
        source = candidate("USD_JPY", "sell", horizon=3600)
        source.update(
            {
                "policy_state": "conflicted_aggressive_shadow",
                "direction_conflict": True,
            }
        )
        members = trial.arm_candidates(
            "conflicted_aggressive_direction_pair", [source], {}
        )
        entry = {"USD_JPY": {"bid": 150.00, "ask": 150.02, "pip": 0.01}}
        exit_quotes = {
            "USD_JPY": {"bid": 150.10, "ask": 150.12, "pip": 0.01}
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertEqual(
                [member["direction"] for member in members], ["sell", "buy"]
            )
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "conflicted_aggressive_direction_pair",
                    3600,
                    members,
                    entry,
                    1_000.0,
                )
            )
            self.assertEqual(
                trial.mature_cohorts(connection, exit_quotes, 4_601.0), 1
            )
            summary = trial.summarized_h2_inverse_shadow(connection)
            connection.close()

        arm = next(
            item
            for item in summary["prospective_direction_pairs"]
            if item["arm"] == "conflicted_aggressive_direction_pair"
        )
        self.assertEqual(arm["matched_pairs"], 1)
        self.assertAlmostEqual(arm["original"]["total_net_pips"], -12.0)
        self.assertAlmostEqual(arm["inverse"]["total_net_pips"], 8.0)
        horizon_segment = next(
            item
            for item in arm["segments"]["horizon_label"]
            if item["value"] == "H1"
        )
        self.assertEqual(horizon_segment["original"]["decisions"], 1)

    def test_all_outcome_refractory_blocks_immediate_repeat_after_win(self):
        first = candidate("EUR_USD", "buy", horizon=300)
        first["projected_net_pips"] = 1.0
        quotes = {"EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}}
        winning_exit = {
            "EUR_USD": {"bid": 1.1010, "ask": 1.1012, "pip": 0.0001}
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_price_top1",
                    300,
                    [first],
                    quotes,
                    1_000.0,
                )
            )
            self.assertEqual(
                trial.mature_cohorts(connection, winning_exit, 1_301.0), 1
            )
            selected = trial.all_outcome_refractory_candidates(
                connection,
                [first],
                {},
                winning_exit,
                300,
                1_301.0,
            )
            connection.close()

        self.assertEqual(selected, [])

    def test_cross_horizon_summary_counts_one_signal_thesis(self):
        m15 = candidate("EUR_CAD", "sell", horizon=900)
        h2 = candidate("EUR_CAD", "sell", horizon=7200)
        h2["signal_id"] = m15["signal_id"]
        m15["family"] = h2["family"] = "trend_break"
        quotes = {"EUR_CAD": {"bid": 1.5000, "ask": 1.5002, "pip": 0.0001}}
        exits = {"EUR_CAD": {"bid": 1.5010, "ask": 1.5012, "pip": 0.0001}}
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection, "opportunity_price_top1", 900, [m15], quotes, 1_000.0
                )
            )
            self.assertTrue(
                trial.open_cohort(
                    connection, "h2_persistence", 7200, [h2], quotes, 1_000.0
                )
            )
            self.assertEqual(trial.mature_cohorts(connection, exits, 8_201.0), 2)
            summary = trial.summarized_cross_horizon_theses(connection)
            connection.close()

        self.assertEqual(summary["unique_horizon_endpoints"], 2)
        self.assertEqual(summary["unique_cross_horizon_theses"], 1)
        self.assertEqual(summary["cross_horizon_endpoints_collapsed"], 1)
        self.assertEqual(summary["theses"][0]["endpoint_count"], 2)

    def test_intrahour_cost_capture_requires_liquid_after_cost_movement(self):
        row = candidate(
            "EUR_USD",
            "buy",
            horizon=900,
            gross_to_spread=1.75,
            confidence=0.55,
        )
        row["projected_net_pips"] = 0.25
        liquid = {
            "EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}
        }
        wide = {
            "EUR_USD": {"bid": 1.1000, "ask": 1.1004, "pip": 0.0001}
        }

        self.assertTrue(
            trial.candidate_is_intrahour_cost_capture(row, liquid)
        )
        self.assertFalse(
            trial.candidate_is_intrahour_cost_capture(row, wide)
        )
        low_movement = dict(row, gross_to_spread=0.19)
        self.assertFalse(
            trial.candidate_is_intrahour_cost_capture(low_movement, liquid)
        )

    def test_calibrated_intrahour_arm_bounds_projected_magnitude(self):
        quotes = {
            "EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}
        }
        row = candidate(
            "EUR_USD",
            "buy",
            horizon=900,
            gross_to_spread=2.0,
            confidence=0.56,
        )
        row["projected_net_pips"] = 1.5
        self.assertTrue(
            trial.candidate_is_intrahour_calibrated_magnitude(row, quotes)
        )
        self.assertFalse(
            trial.candidate_is_intrahour_calibrated_magnitude(
                dict(row, projected_net_pips=0.99), quotes
            )
        )
        self.assertFalse(
            trial.candidate_is_intrahour_calibrated_magnitude(
                dict(row, projected_net_pips=3.01), quotes
            )
        )
        selected = trial.arm_candidates(
            "intrahour_magnitude_1_3_top1", [row], {}, quotes
        )
        self.assertEqual([item["instrument"] for item in selected], ["EUR_USD"])

    def test_intrahour_executor_parity_requires_final_validation_and_no_conflict(self):
        quotes = {
            "EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}
        }
        row = candidate(
            "EUR_USD",
            "buy",
            horizon=900,
            gross_to_spread=2.0,
            confidence=0.56,
        )
        row.update(
            {
                "signal_eligible": True,
                "direction_conflict": False,
                "policy_state": "eligible",
                "execution_validation": {"validated": True},
            }
        )
        self.assertTrue(
            trial.candidate_is_intrahour_executor_parity(row, quotes)
        )
        self.assertFalse(
            trial.candidate_is_intrahour_executor_parity(
                dict(row, direction_conflict=True), quotes
            )
        )
        blocked = dict(row, execution_validation={"validated": False})
        self.assertEqual(
            trial.arm_candidates(
                "intrahour_executor_parity_top1", [blocked], {}, quotes
            ),
            [],
        )
        selected = trial.arm_candidates(
            "intrahour_executor_parity_top1", [row], {}, quotes
        )
        self.assertEqual([item["instrument"] for item in selected], ["EUR_USD"])

    def test_news_alignment_and_event_arms_are_separate(self):
        row = candidate()
        news = news_for("EUR_USD")
        self.assertEqual(
            len(trial.arm_candidates("opportunity_news_aligned", [row], news)),
            1,
        )
        self.assertEqual(
            len(trial.arm_candidates("opportunity_event_regime", [row], news)),
            1,
        )
        bearish = news_for("EUR_USD", direction="BEARISH")
        self.assertEqual(
            trial.arm_candidates("opportunity_news_aligned", [row], bearish),
            [],
        )

    def test_verified_event_arm_excludes_severity_only_discovery_headline(self):
        row = candidate()
        unverified = news_for("EUR_USD", eligible=False)
        self.assertEqual(
            len(trial.arm_candidates("opportunity_event_regime", [row], unverified)),
            1,
        )
        self.assertEqual(
            trial.arm_candidates(
                "opportunity_verified_event_regime", [row], unverified
            ),
            [],
        )

        unverified["pairs"]["EUR_USD"]["events"][0]["source_verified"] = True
        selected = trial.arm_candidates(
            "opportunity_verified_event_regime", [row], unverified
        )
        self.assertEqual([item["instrument"] for item in selected], ["EUR_USD"])

    def test_verified_news_event_requires_directional_alignment(self):
        row = candidate()
        neutral = news_for("EUR_USD", direction="UNKNOWN", eligible=True)
        self.assertEqual(
            trial.arm_candidates(
                "opportunity_verified_news_event", [row], neutral
            ),
            [],
        )

        aligned = news_for("EUR_USD", direction="BULLISH", eligible=True)
        selected = trial.arm_candidates(
            "opportunity_verified_news_event", [row], aligned
        )
        self.assertEqual([item["instrument"] for item in selected], ["EUR_USD"])

        unverified = news_for("EUR_USD", direction="BULLISH", eligible=False)
        self.assertEqual(
            trial.arm_candidates(
                "opportunity_verified_news_event", [row], unverified
            ),
            [],
        )

    def test_context_only_event_does_not_activate_news_or_event_arms(self):
        row = candidate()
        context = news_for("EUR_USD", direction="BULLISH", eligible=True)
        context["pairs"]["EUR_USD"]["events"][0]["context_only"] = True

        self.assertEqual(trial.news_direction(context["pairs"]["EUR_USD"]), "neutral")
        for arm in (
            "opportunity_news_aligned",
            "opportunity_event_regime",
            "opportunity_verified_event_regime",
            "opportunity_verified_news_event",
            "opportunity_news_event",
        ):
            self.assertEqual(trial.arm_candidates(arm, [row], context), [])

    def test_top_three_removes_shared_directional_currency_exposure(self):
        rows = [
            candidate("EUR_USD", "buy"),
            candidate("GBP_USD", "buy"),
            candidate("AUD_JPY", "buy"),
            candidate("CAD_CHF", "sell"),
        ]
        selected = trial.independent_selection(rows, {}, 3)
        self.assertEqual(
            [row["instrument"] for row in selected],
            ["EUR_USD", "AUD_JPY", "CAD_CHF"],
        )

    def test_jpy_pair_propagation_is_one_explicit_factor(self):
        rows = [
            candidate("EUR_JPY", "buy"),
            candidate("USD_JPY", "buy"),
            candidate("GBP_CHF", "buy"),
        ]
        selected = trial.independent_selection(rows, {}, 3)

        self.assertEqual(
            [row["instrument"] for row in selected],
            ["EUR_JPY", "GBP_CHF"],
        )
        self.assertEqual(
            selected[0]["jpy_factor_ids"],
            ["currency:JPY:short"],
        )

    def test_conflicted_consensus_has_separate_research_only_arm(self):
        row = candidate(
            "GBP_JPY",
            "sell",
            gross_to_spread=1.6,
            confidence=0.56,
        )
        row.update(
            {
                "direction_conflict": True,
                "policy_state": "conflicted_aggressive_shadow",
            }
        )

        selected = trial.arm_candidates(
            "conflicted_aggressive_top1",
            [row],
            {},
        )

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["instrument"], "GBP_JPY")

    def test_reentry_guarded_arm_preserves_raw_but_skips_recent_loss_retry(self):
        usd_jpy = candidate("USD_JPY", "buy", horizon=7200)
        eur_usd = candidate("EUR_USD", "buy", horizon=7200)
        entry = {
            "USD_JPY": {"bid": 150.00, "ask": 150.02, "pip": 0.01},
            "EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001},
        }
        losing_exit = {
            "USD_JPY": {"bid": 149.88, "ask": 149.90, "pip": 0.01},
            "EUR_USD": entry["EUR_USD"],
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_price_top1",
                    7200,
                    [usd_jpy],
                    entry,
                    1_000.0,
                )
            )
            self.assertEqual(
                trial.mature_cohorts(connection, losing_exit, 8_201.0),
                1,
            )

            raw = trial.arm_candidates(
                "opportunity_price_top1", [usd_jpy, eur_usd], {}
            )
            guarded = trial.reentry_guarded_opportunity_candidates(
                connection, [usd_jpy, eur_usd], {}, 8_206.0
            )
            after_cooldown = trial.reentry_guarded_opportunity_candidates(
                connection, [usd_jpy, eur_usd], {}, 10_002.0
            )
            connection.close()

        self.assertEqual(raw[0]["instrument"], "USD_JPY")
        self.assertEqual(guarded[0]["instrument"], "EUR_USD")
        self.assertEqual(after_cooldown[0]["instrument"], "USD_JPY")

    def test_reentry_guard_sees_loss_matured_in_same_cycle(self):
        prior = candidate("CAD_SGD", "sell", horizon=900)
        opposite = candidate("CAD_SGD", "buy", horizon=900)
        entry = {
            "CAD_SGD": {"bid": 0.9500, "ask": 0.9502, "pip": 0.0001},
        }
        losing_exit = {
            "CAD_SGD": {"bid": 0.9507, "ask": 0.9509, "pip": 0.0001},
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_price_top1_reentry_guarded",
                    900,
                    [prior],
                    entry,
                    1_000.0,
                )
            )
            self.assertEqual(
                trial.mature_cohorts(connection, losing_exit, 1_901.0),
                1,
            )

            # run_cycle matures and selects with one shared now_epoch. The
            # just-recorded loss must block even when its closed_epoch equals
            # the selection timestamp, and the instrument cooldown is
            # direction-agnostic.
            guarded = trial.reentry_guarded_opportunity_candidates(
                connection,
                [opposite],
                {},
                1_901.0,
            )
            connection.close()

        self.assertEqual(guarded, [])

    def test_overlap_guard_skips_distinct_signal_until_open_thesis_matures(self):
        prior = candidate("USD_JPY", "buy", horizon=3600)
        opposite = candidate("USD_JPY", "sell", horizon=3600)
        alternate = candidate("EUR_USD", "buy", horizon=3600)
        entry = {
            "USD_JPY": {"bid": 150.00, "ask": 150.02, "pip": 0.01},
            "EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001},
        }
        exit_quotes = {
            "USD_JPY": {"bid": 150.05, "ask": 150.07, "pip": 0.01},
            "EUR_USD": entry["EUR_USD"],
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_price_top1",
                    3600,
                    [prior],
                    entry,
                    1_000.0,
                )
            )

            raw = trial.arm_candidates(
                "opportunity_price_top1", [opposite, alternate], {}
            )
            guarded = trial.overlap_guarded_opportunity_candidates(
                connection, [opposite, alternate], {}, 1_900.0
            )
            self.assertEqual(
                trial.mature_cohorts(connection, exit_quotes, 4_601.0),
                1,
            )
            after_maturity = trial.overlap_guarded_opportunity_candidates(
                connection, [opposite, alternate], {}, 4_602.0
            )
            connection.close()

        self.assertEqual(raw[0]["instrument"], "USD_JPY")
        self.assertEqual(guarded[0]["instrument"], "EUR_USD")
        self.assertEqual(after_maturity[0]["instrument"], "USD_JPY")

    def test_factor_overlap_guard_skips_shared_signed_jpy_factor(self):
        prior = candidate("USD_JPY", "buy", horizon=3600)
        same_jpy_factor = candidate("EUR_JPY", "buy", horizon=3600)
        independent = candidate("EUR_GBP", "buy", horizon=3600)
        entry = {
            "USD_JPY": {"bid": 150.00, "ask": 150.02, "pip": 0.01},
        }
        exit_quotes = {
            "USD_JPY": {"bid": 150.05, "ask": 150.07, "pip": 0.01},
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_price_top1",
                    3600,
                    [prior],
                    entry,
                    1_000.0,
                )
            )

            guarded = trial.factor_overlap_guarded_opportunity_candidates(
                connection, [same_jpy_factor, independent], {}, 1_900.0
            )
            self.assertEqual(
                trial.mature_cohorts(connection, exit_quotes, 4_601.0),
                1,
            )
            after_maturity = trial.factor_overlap_guarded_opportunity_candidates(
                connection, [same_jpy_factor, independent], {}, 4_602.0
            )
            connection.close()

        self.assertEqual(guarded[0]["instrument"], "EUR_GBP")
        self.assertEqual(after_maturity[0]["instrument"], "EUR_JPY")

    def test_top_three_removes_shared_event_id(self):
        rows = [candidate("EUR_USD"), candidate("AUD_JPY"), candidate("CAD_CHF")]
        news = {
            "pairs": {
                "EUR_USD": news_for("EUR_USD", event_id="macro-one")["pairs"]["EUR_USD"],
                "AUD_JPY": news_for("AUD_JPY", event_id="macro-one")["pairs"]["AUD_JPY"],
                "CAD_CHF": news_for("CAD_CHF", event_id="macro-two")["pairs"]["CAD_CHF"],
            }
        }
        selected = trial.independent_selection(rows, news, 3)
        self.assertEqual(
            [row["instrument"] for row in selected],
            ["EUR_USD", "CAD_CHF"],
        )

    def test_top_three_ignores_shared_retrospective_context_event(self):
        rows = [candidate("EUR_USD"), candidate("AUD_JPY"), candidate("CAD_CHF")]
        news = {
            "pairs": {
                pair: {
                    "events": [{
                        "event_id": "already-reported-market-move",
                        "age_minutes": 5,
                        "severity": 75,
                        "execution_eligible": False,
                        "reports_prior_market_move": True,
                    }]
                }
                for pair in ("EUR_USD", "AUD_JPY", "CAD_CHF")
            }
        }

        selected = trial.independent_selection(rows, news, 3)

        self.assertEqual(
            [row["instrument"] for row in selected],
            ["EUR_USD", "AUD_JPY", "CAD_CHF"],
        )
        self.assertEqual(selected[0]["event_ids"], ["already-reported-market-move"])
        self.assertEqual(selected[0]["independence_event_ids"], [])

    def test_top_three_readiness_explains_candidate_scarcity(self):
        ranked = {
            300: [candidate("EUR_USD"), candidate("AUD_JPY")],
            900: [
                candidate("EUR_USD", horizon=900),
                candidate("AUD_JPY", horizon=900),
                candidate("CAD_CHF", horizon=900),
            ],
        }

        result = trial.top3_readiness_by_horizon(ranked, {})

        m5 = next(row for row in result if row["horizon_sec"] == 300)
        m15 = next(row for row in result if row["horizon_sec"] == 900)
        self.assertEqual(m5["blocked_by"], ["fewer_than_three_opportunities"])
        self.assertTrue(m15["exactly_three_ready"])
        self.assertEqual(m15["fully_independent_capacity"], 3)

    def test_top_three_arm_waits_for_three_independent_members(self):
        rows = [candidate("EUR_USD"), candidate("GBP_USD")]
        self.assertEqual(
            trial.arm_candidates("opportunity_price_top3", rows, {}),
            [],
        )

    def test_risk_equal_cohort_uses_executable_bid_ask_outcomes(self):
        rows = [candidate("EUR_USD", "buy"), candidate("AUD_JPY", "sell")]
        quotes = {
            "EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001},
            "AUD_JPY": {"bid": 100.00, "ask": 100.02, "pip": 0.01},
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_price_top3",
                    300,
                    rows,
                    quotes,
                    1_000.0,
                )
            )
            exit_quotes = {
                "EUR_USD": {"bid": 1.1006, "ask": 1.1008, "pip": 0.0001},
                "AUD_JPY": {"bid": 99.92, "ask": 99.94, "pip": 0.01},
            }
            self.assertEqual(trial.mature_cohorts(connection, exit_quotes, 1_301.0), 1)
            weights = [
                row[0] for row in connection.execute(
                    "SELECT weight FROM positions ORDER BY rank_in_cohort"
                )
            ]
            result = connection.execute(
                "SELECT weighted_net_pips, weighted_return_pct, win FROM cohorts"
            ).fetchone()
            connection.close()
        self.assertEqual(weights, [0.5, 0.5])
        self.assertAlmostEqual(result[0], 5.0, places=6)
        self.assertGreater(result[1], 0.0)
        self.assertEqual(result[2], 1)

    def test_only_one_open_cohort_per_arm_and_horizon(self):
        row = candidate()
        quotes = {"EUR_USD": {"bid": 1.1, "ask": 1.1002, "pip": 0.0001}}
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            first = trial.open_cohort(
                connection, "opportunity_price_top1", 300, [row], quotes, 1_000.0
            )
            second = trial.open_cohort(
                connection, "opportunity_price_top1", 300, [row], quotes, 1_010.0
            )
            connection.close()
        self.assertTrue(first)
        self.assertFalse(second)

    def test_confirmed_reversal_requires_recent_same_family_adverse_signal(self):
        prior = candidate("EUR_USD", "sell", horizon=3600)
        prior["signal_id"] = "20260804T010000000000-breakout_retest.loose-EUR_USD"
        reversal = candidate("EUR_USD", "buy", horizon=3600)
        reversal["signal_id"] = "20260804T010500000000-breakout_retest.loose-EUR_USD"
        entry_quotes = {
            "EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}
        }
        adverse_quotes = {
            "EUR_USD": {"bid": 1.1004, "ask": 1.1006, "pip": 0.0001}
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "intrahour_cost_capture_top1",
                    3600,
                    [prior],
                    entry_quotes,
                    1_000.0,
                )
            )
            selected = trial.confirmed_reversal_candidates(
                connection,
                [reversal],
                {},
                adverse_quotes,
                3600,
                1_300.0,
            )
            same_direction = trial.confirmed_reversal_candidates(
                connection,
                [prior],
                {},
                adverse_quotes,
                3600,
                1_300.0,
            )
            expired = trial.confirmed_reversal_candidates(
                connection,
                [reversal],
                {},
                adverse_quotes,
                3600,
                1_901.0,
            )
            connection.close()

        self.assertEqual([row["instrument"] for row in selected], ["EUR_USD"])
        self.assertEqual(same_direction, [])
        self.assertEqual(expired, [])

    def test_confirmed_reversal_rejects_other_family_and_small_adverse_move(self):
        prior = candidate("USD_CHF", "sell", horizon=900)
        prior["signal_id"] = "20260804T010000000000-momentum.strict-USD_CHF"
        other_family = candidate("USD_CHF", "buy", horizon=900)
        other_family["signal_id"] = (
            "20260804T010500000000-breakout_retest.loose-USD_CHF"
        )
        same_family = candidate("USD_CHF", "buy", horizon=900)
        same_family["signal_id"] = "20260804T010500000000-momentum.strict-USD_CHF"
        entry_quotes = {
            "USD_CHF": {"bid": 0.8000, "ask": 0.8002, "pip": 0.0001}
        }
        barely_adverse_quotes = {
            "USD_CHF": {"bid": 0.8000, "ask": 0.8001, "pip": 0.0001}
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "intrahour_cost_capture_top1",
                    900,
                    [prior],
                    entry_quotes,
                    1_000.0,
                )
            )
            different = trial.confirmed_reversal_candidates(
                connection,
                [other_family],
                {},
                {"USD_CHF": {"bid": 0.8004, "ask": 0.8006, "pip": 0.0001}},
                900,
                1_300.0,
            )
            too_small = trial.confirmed_reversal_candidates(
                connection,
                [same_family],
                {},
                barely_adverse_quotes,
                900,
                1_300.0,
            )
            connection.close()

        self.assertEqual(different, [])
        self.assertEqual(too_small, [])

    def test_cross_family_reversal_is_separate_large_flip_trial(self):
        prior = candidate("SGD_CHF", "sell", horizon=1800)
        prior["signal_id"] = "20260804T010000000000-currency_strength.fast-SGD_CHF"
        reversal = candidate("SGD_CHF", "buy", horizon=1800)
        reversal["signal_id"] = "20260804T012500000000-momentum.balanced-SGD_CHF"
        reversal["projected_net_pips"] = 2.5
        low_magnitude = dict(reversal, projected_net_pips=1.9)
        entry_quotes = {
            "SGD_CHF": {"bid": 0.6302, "ask": 0.6304, "pip": 0.0001}
        }
        adverse_quotes = {
            "SGD_CHF": {"bid": 0.6308, "ask": 0.6310, "pip": 0.0001}
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "intrahour_cost_capture_top1",
                    1800,
                    [prior],
                    entry_quotes,
                    1_000.0,
                )
            )
            selected = trial.cross_family_confirmed_reversal_candidates(
                connection,
                [reversal],
                {},
                adverse_quotes,
                1800,
                2_500.0,
            )
            too_small = trial.cross_family_confirmed_reversal_candidates(
                connection,
                [low_magnitude],
                {},
                adverse_quotes,
                1800,
                2_500.0,
            )
            expired = trial.cross_family_confirmed_reversal_candidates(
                connection,
                [reversal],
                {},
                adverse_quotes,
                1800,
                2_801.0,
            )
            connection.close()

        self.assertEqual([row["instrument"] for row in selected], ["SGD_CHF"])
        self.assertEqual(too_small, [])
        self.assertEqual(expired, [])

    def test_underlying_decision_summary_collapses_identical_cross_arm_cohorts(self):
        row = candidate("USD_CHF", "buy", horizon=3600)
        quotes = {"USD_CHF": {"bid": 0.8000, "ask": 0.8002, "pip": 0.0001}}
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            for arm in (
                "opportunity_price_top1",
                "opportunity_event_regime",
                "intrahour_cost_capture_top1",
            ):
                self.assertTrue(
                    trial.open_cohort(
                        connection, arm, 3600, [row], quotes, 1_000.0
                    )
                )
            summary = trial.summarized_underlying_decisions(connection)
            connection.close()

        self.assertEqual(summary["arm_cohorts"], 3)
        self.assertEqual(summary["unique_underlying_decisions"], 1)
        self.assertEqual(summary["cross_arm_duplicates_excluded"], 2)
        self.assertEqual(summary["status_counts"], {"open": 1})
        self.assertEqual(summary["open_groups"][0]["arm_cohort_count"], 3)

    def test_factor_episode_summary_collapses_nearby_jpy_propagation(self):
        quotes = {
            "AUD_JPY": {"bid": 100.00, "ask": 100.02, "pip": 0.01},
            "USD_JPY": {"bid": 150.00, "ask": 150.02, "pip": 0.01},
            "EUR_JPY": {"bid": 160.00, "ask": 160.02, "pip": 0.01},
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_price_top1",
                    3600,
                    [candidate("AUD_JPY", "buy", horizon=3600)],
                    quotes,
                    1_000.0,
                )
            )
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_event_regime",
                    3600,
                    [candidate("USD_JPY", "buy", horizon=3600)],
                    quotes,
                    1_090.0,
                )
            )
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "intrahour_cost_capture_top1",
                    3600,
                    [candidate("EUR_JPY", "sell", horizon=3600)],
                    quotes,
                    1_100.0,
                )
            )
            exit_quotes = {
                "AUD_JPY": {"bid": 100.10, "ask": 100.12, "pip": 0.01},
                "USD_JPY": {"bid": 150.10, "ask": 150.12, "pip": 0.01},
                "EUR_JPY": {"bid": 159.90, "ask": 159.92, "pip": 0.01},
            }
            self.assertEqual(
                trial.mature_cohorts(connection, exit_quotes, 4_701.0),
                3,
            )
            summary = trial.summarized_factor_episodes(connection)
            connection.close()

        self.assertEqual(summary["underlying_decisions"], 3)
        self.assertEqual(summary["independent_factor_episodes"], 2)
        self.assertEqual(summary["factor_repeats_collapsed"], 1)
        self.assertEqual(summary["status_counts"], {"matured": 2})
        self.assertEqual(summary["results"][0]["underlying_decisions"], 3)
        self.assertEqual(summary["results"][0]["independent_factor_episodes"], 2)

    def test_pair_flip_diagnostic_collapses_repeats_and_splits_magnitude(self):
        quotes = {
            "EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}
        }
        prior = candidate("EUR_USD", "sell", horizon=3600)
        low_one = candidate("EUR_USD", "buy", horizon=3600)
        low_one["projected_net_pips"] = 1.5
        low_two = dict(low_one, signal_id="EUR_USD-buy-low-two")
        high = candidate("EUR_USD", "buy", horizon=3600)
        high["projected_net_pips"] = 2.5
        high["signal_id"] = "EUR_USD-buy-high"

        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            for arm, row, observed in (
                ("opportunity_price_top1", prior, 1_000.0),
                ("opportunity_event_regime", low_one, 1_100.0),
                ("intrahour_cost_capture_top1", low_two, 1_150.0),
                ("intrahour_magnitude_1_3_top1", high, 1_200.0),
            ):
                self.assertTrue(
                    trial.open_cohort(
                        connection, arm, 3600, [row], quotes, observed
                    )
                )
            exits = {
                "EUR_USD": {"bid": 1.1005, "ask": 1.1007, "pip": 0.0001}
            }
            self.assertEqual(trial.mature_cohorts(connection, exits, 4_801.0), 4)
            summary = trial.summarized_pair_direction_flips(
                connection, discovery_epoch=10_000.0
            )
            connection.close()

        self.assertFalse(summary["can_change_execution"])
        results = {row["magnitude_band"]: row for row in summary["results"]}
        self.assertEqual(results["projected_net_lt_2"]["exact_flips"], 2)
        self.assertEqual(results["projected_net_lt_2"]["factor_episodes"], 1)
        self.assertEqual(results["projected_net_lt_2"]["repeats_collapsed"], 1)
        self.assertEqual(results["projected_net_ge_2"]["exact_flips"], 1)
        self.assertEqual(results["projected_net_ge_2"]["factor_episodes"], 1)

    def test_family_horizon_summary_removes_cross_arm_duplicates(self):
        row = candidate("USD_CHF", "buy", horizon=3600)
        row["signal_id"] = "20260804T010000000000-micro_channel_break.fast-USD_CHF"
        quotes = {"USD_CHF": {"bid": 0.8000, "ask": 0.8002, "pip": 0.0001}}
        exit_quotes = {
            "USD_CHF": {"bid": 0.8012, "ask": 0.8014, "pip": 0.0001}
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            for arm in (
                "opportunity_price_top1",
                "opportunity_event_regime",
                "intrahour_cost_capture_top1",
            ):
                self.assertTrue(
                    trial.open_cohort(connection, arm, 3600, [row], quotes, 1_000.0)
                )
            self.assertEqual(
                trial.mature_cohorts(connection, exit_quotes, 4_601.0), 3
            )
            summary = trial.summarized_family_horizons(connection)
            connection.close()

        self.assertEqual(summary["raw_matured_arm_positions"], 3)
        self.assertEqual(summary["matured_exact_positions"], 1)
        self.assertEqual(summary["cross_arm_position_duplicates_excluded"], 2)
        self.assertEqual(summary["strategy_family_count"], 1)
        self.assertEqual(summary["results"][0]["strategy_family"], "micro_channel_break")
        self.assertEqual(
            summary["results"][0]["strategy_variants"],
            ["micro_channel_break.fast"],
        )
        self.assertEqual(summary["results"][0]["wins"], 1)
        self.assertEqual(summary["results"][0]["independent_factor_episodes"], 1)
        self.assertAlmostEqual(summary["results"][0]["total_net_pips"], 10.0)
        self.assertAlmostEqual(summary["results"][0]["best_net_pips"], 10.0)
        self.assertAlmostEqual(summary["results"][0]["worst_net_pips"], 10.0)
        self.assertIsNone(summary["results"][0]["profit_factor"])
        self.assertAlmostEqual(
            summary["results"][0]["factor_episode_total_net_pips"], 10.0
        )

    def test_family_horizon_summary_collapses_same_family_jpy_factor(self):
        aud = candidate("AUD_JPY", "buy", horizon=3600)
        aud["signal_id"] = "20260804T010000000000-micro_channel_break.fast-AUD_JPY"
        usd = candidate("USD_JPY", "buy", horizon=3600)
        usd["signal_id"] = "20260804T010130000000-micro_channel_break.fast-USD_JPY"
        quotes = {
            "AUD_JPY": {"bid": 100.00, "ask": 100.02, "pip": 0.01},
            "USD_JPY": {"bid": 150.00, "ask": 150.02, "pip": 0.01},
        }
        exits = {
            "AUD_JPY": {"bid": 100.10, "ask": 100.12, "pip": 0.01},
            "USD_JPY": {"bid": 150.10, "ask": 150.12, "pip": 0.01},
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection, "opportunity_price_top1", 3600, [aud], quotes, 1_000.0
                )
            )
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_event_regime",
                    3600,
                    [usd],
                    quotes,
                    1_090.0,
                )
            )
            self.assertEqual(trial.mature_cohorts(connection, exits, 4_701.0), 2)
            summary = trial.summarized_family_horizons(connection)
            connection.close()

        result = summary["results"][0]
        self.assertEqual(result["matured_exact_positions"], 2)
        self.assertEqual(result["independent_factor_episodes"], 1)
        self.assertEqual(result["factor_repeats_collapsed"], 1)
        self.assertEqual(result["factor_episode_wins"], 1)

    def test_family_horizon_watchlist_requires_independent_positive_payoff(self):
        summary = {
            "results": [
                {
                    "horizon_sec": 3600,
                    "horizon_label": "H1",
                    "strategy_family": "micro_channel_break",
                    "independent_factor_episodes": 7,
                    "factor_episode_win_rate_pct": 71.429,
                    "avg_factor_episode_net_pips": 1.94,
                    "factor_episode_total_net_pips": 13.6,
                    "factor_episode_profit_factor": 2.56,
                    "factor_episode_best_net_pips": 6.8,
                    "factor_episode_worst_net_pips": -4.6,
                },
                {
                    "horizon_sec": 7200,
                    "horizon_label": "H2",
                    "strategy_family": "tail_loss_reversion",
                    "independent_factor_episodes": 8,
                    "factor_episode_win_rate_pct": 75.0,
                    "avg_factor_episode_net_pips": -3.0,
                    "factor_episode_total_net_pips": -24.0,
                    "factor_episode_profit_factor": 0.4,
                    "factor_episode_best_net_pips": 8.0,
                    "factor_episode_worst_net_pips": -40.0,
                },
                {
                    "horizon_sec": 1800,
                    "horizon_label": "M30",
                    "strategy_family": "too_few",
                    "independent_factor_episodes": 4,
                    "factor_episode_win_rate_pct": 75.0,
                    "avg_factor_episode_net_pips": 3.0,
                    "factor_episode_total_net_pips": 12.0,
                    "factor_episode_profit_factor": 3.0,
                    "factor_episode_best_net_pips": 8.0,
                    "factor_episode_worst_net_pips": -2.0,
                },
            ]
        }

        result = trial.summarized_family_horizon_watchlist(summary)

        self.assertEqual(result["status"], "shadow_only_no_execution_hook")
        self.assertEqual(result["candidate_count"], 1)
        self.assertEqual(
            result["candidates"][0]["strategy_family"], "micro_channel_break"
        )
        self.assertEqual(result["candidates"][0]["evidence_tier"], "early_watch")
        self.assertFalse(result["candidates"][0]["execution_eligible"])

    def test_horizon_readiness_requires_independent_after_cost_payoff(self):
        summary = {
            "results": [
                {
                    "horizon_sec": 3600,
                    "horizon_label": "H1",
                    "independent_factor_episodes": 35,
                    "win_rate_pct": 57.143,
                    "avg_episode_net_pips": 0.8,
                    "episode_total_net_pips": 28.0,
                    "episode_profit_factor": 1.4,
                },
                {
                    "horizon_sec": 7200,
                    "horizon_label": "H2",
                    "independent_factor_episodes": 42,
                    "win_rate_pct": 59.524,
                    "avg_episode_net_pips": -3.4,
                    "episode_total_net_pips": -142.8,
                    "episode_profit_factor": 0.7,
                },
                {
                    "horizon_sec": 900,
                    "horizon_label": "M15",
                    "independent_factor_episodes": 12,
                    "win_rate_pct": 66.667,
                    "avg_episode_net_pips": 1.2,
                    "episode_total_net_pips": 14.4,
                    "episode_profit_factor": 1.8,
                },
            ]
        }

        result = trial.summarized_horizon_readiness(summary)
        by_horizon = {row["horizon_label"]: row for row in result["results"]}

        self.assertFalse(result["can_change_execution"])
        self.assertEqual(result["ready_horizon_count"], 1)
        self.assertTrue(by_horizon["H1"]["shadow_ready_for_frozen_holdout"])
        self.assertEqual(
            by_horizon["H2"]["disposition"],
            "abstain_negative_after_cost_expectancy",
        )
        self.assertEqual(
            by_horizon["M15"]["disposition"],
            "abstain_insufficient_independent_evidence",
        )

    def test_arm_summary_exposes_payoff_and_tail_asymmetry(self):
        row = candidate("EUR_USD", "buy", horizon=300)
        entry = {"EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}}
        exits = (
            {"EUR_USD": {"bid": 1.1012, "ask": 1.1014, "pip": 0.0001}},
            {"EUR_USD": {"bid": 1.0998, "ask": 1.1000, "pip": 0.0001}},
            {"EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}},
        )
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            for index, exit_quotes in enumerate(exits):
                member = dict(row, signal_id=f"tail-test-{index}-EUR_USD")
                opened = 1_000.0 + index * 400.0
                self.assertTrue(
                    trial.open_cohort(
                        connection,
                        "opportunity_price_top1",
                        300,
                        [member],
                        entry,
                        opened,
                    )
                )
                self.assertEqual(
                    trial.mature_cohorts(connection, exit_quotes, opened + 301.0),
                    1,
                )
            summary = trial.summarized_results(connection)[0]
            connection.close()

        self.assertAlmostEqual(summary["total_weighted_net_pips"], 4.0)
        self.assertAlmostEqual(summary["median_weighted_net_pips"], -2.0)
        self.assertAlmostEqual(summary["best_weighted_net_pips"], 10.0)
        self.assertAlmostEqual(summary["worst_weighted_net_pips"], -4.0)
        self.assertAlmostEqual(summary["profit_factor"], 10.0 / 6.0, places=5)

    def test_h2_stop_shadow_tracks_path_without_changing_maturity(self):
        row = candidate("USD_CHF", "buy", horizon=7200)
        entry = {"USD_CHF": {"bid": 0.8000, "ask": 0.8002, "pip": 0.0001}}
        adverse = {"USD_CHF": {"bid": 0.7977, "ask": 0.7979, "pip": 0.0001}}
        exit_quotes = {"USD_CHF": {"bid": 0.8012, "ask": 0.8014, "pip": 0.0001}}
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection, "h2_persistence", 7200, [row], entry, 1_000.0
                )
            )
            self.assertEqual(trial.update_open_excursions(connection, adverse), 1)
            self.assertEqual(
                trial.mature_cohorts(connection, exit_quotes, 8_201.0),
                1,
            )
            actual = connection.execute(
                "SELECT weighted_net_pips FROM cohorts"
            ).fetchone()[0]
            summary = trial.summarized_h2_stop_counterfactuals(connection)
            connection.close()

        self.assertAlmostEqual(actual, 10.0, places=6)
        self.assertEqual(summary["matured_underlying_decisions"], 1)
        self.assertEqual(summary["arms"][0]["stop_hits"], 1)
        self.assertEqual(summary["arms"][0]["total_net_pips"], -20.0)
        self.assertEqual(summary["arms"][1]["stop_hits"], 0)
        self.assertAlmostEqual(summary["arms"][1]["total_net_pips"], 10.0)
        self.assertEqual(summary["matured_factor_episodes"], 1)
        self.assertEqual(
            summary["factor_episode_arms"][0]["total_net_pips"],
            -20.0,
        )

    def test_spread_scaled_take_profit_shadow_captures_giveback(self):
        row = candidate("GBP_JPY", "buy", horizon=1800)
        entry = {"GBP_JPY": {"bid": 200.00, "ask": 200.03, "pip": 0.01}}
        favorable = {
            "GBP_JPY": {"bid": 200.10, "ask": 200.13, "pip": 0.01}
        }
        flat_exit = {
            "GBP_JPY": {"bid": 200.03, "ask": 200.06, "pip": 0.01}
        }
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "opportunity_price_top1",
                    1800,
                    [row],
                    entry,
                    1_000.0,
                )
            )
            self.assertEqual(trial.update_open_excursions(connection, favorable), 1)
            self.assertEqual(
                trial.mature_cohorts(connection, flat_exit, 2_801.0),
                1,
            )
            actual = connection.execute(
                "SELECT weighted_net_pips FROM cohorts"
            ).fetchone()[0]
            summary = trial.summarized_spread_scaled_take_profit_counterfactuals(
                connection
            )
            connection.close()

        self.assertAlmostEqual(actual, 0.0, places=6)
        horizon = summary["horizons"][0]
        self.assertEqual(horizon["horizon_label"], "M30")
        self.assertEqual(horizon["matured_underlying_decisions"], 1)
        self.assertEqual(horizon["take_profit_arms"][0]["take_profit_hits"], 1)
        self.assertAlmostEqual(
            horizon["take_profit_arms"][0]["total_net_pips"], 4.5, places=6
        )
        self.assertEqual(horizon["take_profit_arms"][1]["take_profit_hits"], 1)
        self.assertAlmostEqual(
            horizon["take_profit_arms"][1]["total_net_pips"], 6.0, places=6
        )

    def test_competing_risk_barriers_preserve_first_hit_order(self):
        row = candidate("EUR_USD", "buy", horizon=300)
        entry = {"EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001}}
        favorable = {
            "EUR_USD": {"bid": 1.1006, "ask": 1.1008, "pip": 0.0001}
        }
        adverse = {
            "EUR_USD": {"bid": 1.0996, "ask": 1.0998, "pip": 0.0001}
        }
        flat = {"EUR_USD": {"bid": 1.1002, "ask": 1.1004, "pip": 0.0001}}
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            first = dict(row, signal_id="target-first")
            self.assertTrue(
                trial.open_cohort(
                    connection, "opportunity_price_top1", 300, [first], entry, 1_000.0
                )
            )
            trial.update_open_excursions(connection, favorable, now_epoch=1_010.0)
            trial.update_open_excursions(connection, adverse, now_epoch=1_020.0)
            trial.mature_cohorts(connection, flat, 1_301.0)

            second = dict(row, signal_id="stop-first")
            self.assertTrue(
                trial.open_cohort(
                    connection, "opportunity_price_top1", 300, [second], entry, 1_400.0
                )
            )
            trial.update_open_excursions(connection, adverse, now_epoch=1_410.0)
            trial.update_open_excursions(connection, favorable, now_epoch=1_420.0)
            trial.mature_cohorts(connection, flat, 1_701.0)
            summary = trial.summarized_competing_risk_barriers(connection)
            connection.close()

        arm = next(
            value
            for value in summary["arms"]
            if value["barrier_id"] == "cost_1_5x_symmetric"
        )
        self.assertEqual(arm["matured_underlying_decisions"], 2)
        self.assertEqual(arm["target_first"], 1)
        self.assertEqual(arm["stop_first"], 1)
        self.assertEqual(arm["neither_before_horizon"], 0)
        self.assertEqual(arm["median_first_event_sec"], 10.0)
        self.assertFalse(summary["can_change_execution"])

    def test_competing_risk_barriers_collapse_nearby_same_signal_across_arms(self):
        row = candidate("GBP_JPY", "buy", horizon=900)
        row = dict(row, signal_id="same-signal")
        entry = {"GBP_JPY": {"bid": 200.00, "ask": 200.03, "pip": 0.01}}
        adverse = {"GBP_JPY": {"bid": 199.95, "ask": 199.98, "pip": 0.01}}
        flat = {"GBP_JPY": {"bid": 200.00, "ask": 200.03, "pip": 0.01}}
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection, "conflicted_aggressive_top1", 900, [row], entry, 1_000.0
                )
            )
            self.assertTrue(
                trial.open_cohort(
                    connection, "intrahour_cost_capture_top1", 900, [row], entry, 1_122.0
                )
            )
            trial.update_open_excursions(connection, flat, now_epoch=1_130.0)
            trial.update_open_excursions(connection, adverse, now_epoch=1_140.0)
            trial.mature_cohorts(connection, flat, 2_101.0)
            summary = trial.summarized_competing_risk_barriers(connection)
            connection.close()

        arm = next(
            value
            for value in summary["arms"]
            if value["barrier_id"] == "cost_1_5x_symmetric"
        )
        self.assertEqual(arm["matured_underlying_decisions"], 1)
        self.assertEqual(arm["stop_first"], 1)
        self.assertEqual(summary["nearby_same_thesis_duplicates_excluded"], 3)
        self.assertEqual(summary["same_signal_duplicates_excluded"], 3)
        self.assertEqual(summary["nearby_fallback_duplicates_excluded"], 0)
        self.assertEqual(summary["same_thesis_window_sec"], 60.0)

    def test_competing_risk_barriers_report_first_decision_per_factor_episode(self):
        first = dict(
            candidate("SGD_CHF", "buy", horizon=900),
            signal_id="first-signal",
        )
        second = dict(first, signal_id="second-signal")
        entry = {"SGD_CHF": {"bid": 0.6100, "ask": 0.6102, "pip": 0.0001}}
        adverse = {"SGD_CHF": {"bid": 0.6096, "ask": 0.6098, "pip": 0.0001}}
        flat = {"SGD_CHF": {"bid": 0.6100, "ask": 0.6102, "pip": 0.0001}}
        with tempfile.TemporaryDirectory() as directory:
            connection = trial.open_database(Path(directory) / "trial.sqlite")
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "conflicted_aggressive_top1",
                    900,
                    [first],
                    entry,
                    1_000.0,
                )
            )
            self.assertTrue(
                trial.open_cohort(
                    connection,
                    "intrahour_cost_capture_top1",
                    900,
                    [second],
                    entry,
                    1_120.0,
                )
            )
            trial.update_open_excursions(connection, flat, now_epoch=1_125.0)
            trial.update_open_excursions(connection, adverse, now_epoch=1_130.0)
            trial.mature_cohorts(connection, flat, 2_101.0)
            summary = trial.summarized_competing_risk_barriers(connection)
            connection.close()

        decision_arm = next(
            value
            for value in summary["arms"]
            if value["barrier_id"] == "cost_1_5x_symmetric"
        )
        episode_arm = next(
            value
            for value in summary["factor_episode_arms"]
            if value["barrier_id"] == "cost_1_5x_symmetric"
        )
        self.assertEqual(decision_arm["matured_underlying_decisions"], 2)
        self.assertEqual(episode_arm["matured_factor_episodes"], 1)
        self.assertEqual(episode_arm["underlying_decisions"], 2)
        self.assertEqual(episode_arm["factor_repeats_collapsed"], 1)
        self.assertEqual(episode_arm["stop_first"], 1)
        self.assertEqual(summary["factor_episode_window_sec"], 900.0)


if __name__ == "__main__":
    unittest.main()
