import json
import datetime as dt
import unittest
from pathlib import Path

import oanda_move_first_news_case_audit as audit


def event(payload, event_id="e1", cluster=""):
    return {
        "source_event_id": event_id,
        "source_id": "test",
        "source_population": "media_aggregator",
        "event_type": "test",
        "story_cluster_id": cluster,
        "effective_from_utc": "2026-08-11T00:00:00+00:00",
        "effective_epoch": 100,
        "payload_json": json.dumps({"raw_payload": payload}),
    }


def test_periodic_worker_skips_redundant_fresh_initial_refresh(tmp_path: Path):
    moves = tmp_path / "moves.csv"
    output = tmp_path / "audit.json"
    moves.write_text("move_id\n", encoding="utf-8")
    output.write_text("{}", encoding="utf-8")
    base = 1_800_000_000.0
    import os

    os.utime(moves, (base - 30.0, base - 30.0))
    os.utime(output, (base - 10.0, base - 10.0))
    assert audit.initial_refresh_delay_sec(
        output, moves, 3600.0, now_epoch=base
    ) == 3590.0
    os.utime(moves, (base, base))
    assert audit.initial_refresh_delay_sec(
        output, moves, 3600.0, now_epoch=base
    ) == 0.0


class MoveFirstNewsAuditTests(unittest.TestCase):
    def test_causal_m1_state_uses_only_fully_completed_bars(self):
        start = dt.datetime(2026, 8, 26, 0, 0, tzinfo=dt.timezone.utc)
        candles = []
        for minute in range(71):
            price = 1.1000 + minute * 0.0001
            candles.append(
                {
                    "timestamp": start + dt.timedelta(minutes=minute),
                    "bid_open": price - 0.00005,
                    "ask_open": price + 0.00005,
                    "bid_close": price - 0.00005,
                    "ask_close": price + 0.00005,
                    "bid_high": price + 0.00005,
                    "ask_high": price + 0.00015,
                    "bid_low": price - 0.00015,
                    "ask_low": price - 0.00005,
                }
            )
        # This bar opened before the decision but is not complete until 01:11;
        # make it extreme so any accidental inclusion is obvious.
        candles[-1].update(
            bid_open=1.5000, ask_open=1.5001, bid_high=1.5002,
            ask_high=1.5003, bid_low=1.4998, ask_low=1.4999,
        )
        decision = start + dt.timedelta(minutes=70, seconds=30)

        state = audit.causal_m1_technical_state(
            candles, decision_time=decision, pip_size=0.0001
        )

        self.assertEqual(state["state"], "available")
        self.assertEqual(state["last_bar_open_utc"], "2026-08-26T01:09:00+00:00")
        self.assertEqual(state["observed_utc"], "2026-08-26T01:10:00+00:00")
        self.assertEqual(state["observation_age_seconds"], 30.0)
        self.assertAlmostEqual(state["return_5m_pips"], 5.0)
        self.assertLess(state["spread_pips"], 1.01)
        self.assertTrue(state["research_only"])
        self.assertFalse(state["execution_eligible"])
        self.assertFalse(state["is_model_forecast"])

    def test_causal_m1_state_fails_closed_without_history(self):
        state = audit.causal_m1_technical_state(
            [],
            decision_time=dt.datetime(2026, 8, 26, tzinfo=dt.timezone.utc),
            pip_size=0.0001,
        )
        self.assertEqual(state["state"], "insufficient_completed_m1_history")

    def test_source_index_bounds_cover_policy_lookback_and_move_end(self):
        rows = [
            {
                "factor_representative": "true",
                "selected_side_label": "long",
                "start_utc": "2026-08-20T12:00:00+00:00",
                "end_utc": "2026-08-20T13:00:00+00:00",
            },
            {
                "factor_representative": "true",
                "selected_side_label": "short",
                "start_utc": "2026-08-22T10:00:00+00:00",
                "end_utc": "2026-08-22T10:15:00+00:00",
            },
            {
                # Nonrepresentative rows cannot expand the evidence query.
                "factor_representative": "false",
                "selected_side_label": "long",
                "start_utc": "2020-01-01T00:00:00+00:00",
                "end_utc": "2030-01-01T00:00:00+00:00",
            },
        ]

        minimum, maximum = audit.source_index_bounds(rows)

        self.assertEqual(
            minimum,
            int(
                dt.datetime(
                    2026, 8, 19, 12, 0, tzinfo=dt.timezone.utc
                ).timestamp()
            ),
        )
        self.assertEqual(
            maximum,
            int(
                dt.datetime(
                    2026, 8, 22, 10, 15, tzinfo=dt.timezone.utc
                ).timestamp()
            ),
        )

    def test_pair_score_uses_base_minus_quote(self):
        item = event({"currency_scores": {"EUR": 0.5, "USD": -0.2}})
        score, state = audit.pair_score(item, "EUR", "USD")
        self.assertAlmostEqual(score, 0.7)
        self.assertEqual(state, "directional")

    def test_quote_bullish_means_pair_short(self):
        item = event({"directional_bias": {"JPY": "BULLISH"}})
        score, _ = audit.pair_score(item, "USD", "JPY")
        self.assertLess(score, 0)

    def test_retrospective_story_cannot_vote(self):
        item = event(
            {
                "event_temporality": "retrospective_market_report",
                "directional_bias": {"JPY": "BULLISH"},
            }
        )
        score, state = audit.pair_score(item, "USD", "JPY")
        self.assertIsNone(score)
        self.assertEqual(state, "retrospective")

    def test_context_only_story_cannot_vote(self):
        item = event(
            {"context_only": True, "directional_bias": {"USD": "BULLISH"}}
        )
        score, state = audit.pair_score(item, "USD", "JPY")
        self.assertIsNone(score)
        self.assertEqual(state, "context_only")

    def test_repeated_headline_receives_one_vote(self):
        payload = {
            "headline": "Same wire story",
            "directional_bias": {"USD": "BULLISH"},
            "directional_confidence": 0.7,
        }
        result = audit.directional_vote(
            [event(payload, "e1"), event(payload, "e2")], "USD", "JPY", 200
        )
        self.assertEqual(result["independent_directional_stories"], 1)
        self.assertEqual(result["side"], 1)

    def test_move_class_is_independent_of_technical_data(self):
        primary = {"side": 1, "independent_directional_stories": 1}
        during = {"side": 0, "independent_directional_stories": 0}
        self.assertEqual(
            audit.case_class(primary, during, 1), "pre_move_news_correct"
        )

    def test_strict_forward_excludes_known_delayed_story(self):
        item = event(
            {
                "headline": "Delayed",
                "directional_bias": {"USD": "BULLISH"},
                "forward_signal_timely": False,
            }
        )
        result = audit.directional_vote(
            [item], "USD", "JPY", 200, strict_forward=True
        )
        self.assertEqual(result["side"], 0)
        self.assertEqual(result["exclusions"]["not_forward_timely"], 1)

    def test_strict_forward_excludes_research_only_direction(self):
        item = event(
            {
                "headline": "Observed currency move recap",
                "directional_bias": {"USD": "BEARISH"},
                "forward_signal_timely": True,
                "directional_publish_eligible": False,
            }
        )
        result = audit.directional_vote(
            [item], "USD", "JPY", 200, strict_forward=True
        )
        self.assertEqual(result["side"], 0)
        self.assertEqual(
            result["exclusions"]["not_directional_publish_eligible"], 1
        )

    def test_policy_state_excludes_non_policy_official_document(self):
        official_context = event(
            {
                "directional_bias": {"JPY": "BULLISH"},
                "official_policy_release": False,
            }
        )
        official_context["source_population"] = "official_policy_publisher"

        result = audit.directional_vote(
            [official_context],
            "USD",
            "JPY",
            200,
            official_only=True,
            policy_only=True,
        )

        self.assertEqual(result["side"], 0)
        self.assertEqual(
            result["exclusions"]["not_official_policy_release"], 1
        )

    def test_policy_state_applies_fixed_age_decay(self):
        old = event(
            {
                "headline": "Old hawkish release",
                "directional_bias": {"JPY": "BULLISH"},
                "directional_confidence": 0.95,
                "official_policy_release": True,
            },
            event_id="old",
        )
        old["source_population"] = "official_policy_publisher"
        old["effective_epoch"] = 100
        new = event(
            {
                "headline": "New dovish release",
                "directional_bias": {"JPY": "BEARISH"},
                "directional_confidence": 0.6,
                "official_policy_release": True,
            },
            event_id="new",
        )
        new["source_population"] = "official_policy_publisher"
        new["effective_epoch"] = 1_090

        result = audit.directional_vote(
            [old, new],
            "USD",
            "JPY",
            1_100,
            official_only=True,
            policy_only=True,
            age_decay_half_life_minutes=6.0,
        )

        self.assertEqual(result["side"], 1)

    def test_prior_price_reaction_uses_only_completed_predecision_prices(self):
        candles = [
            {
                "timestamp": dt.datetime(2026, 8, 11, 12, 0, tzinfo=dt.timezone.utc),
                "bid_open": 1.1000,
                "ask_open": 1.1002,
            },
            {
                "timestamp": dt.datetime(2026, 8, 11, 12, 4, tzinfo=dt.timezone.utc),
                "bid_open": 1.1008,
                "ask_open": 1.1010,
            },
            {
                # This large post-decision reversal must not be inspected.
                "timestamp": dt.datetime(2026, 8, 11, 12, 6, tzinfo=dt.timezone.utc),
                "bid_open": 1.0900,
                "ask_open": 1.0902,
            },
        ]
        result = audit.prior_price_reaction(
            candles,
            event_time=dt.datetime(2026, 8, 11, 12, 0, tzinfo=dt.timezone.utc),
            decision_time=dt.datetime(2026, 8, 11, 12, 5, tzinfo=dt.timezone.utc),
            predicted_side=1,
            pip_size=0.0001,
            modeled_cost_pips=2.0,
        )
        self.assertEqual(result["state"], "aligned_move_already_underway")
        self.assertAlmostEqual(result["signed_prior_reaction_pips"], 8.0)
        self.assertEqual(
            result["last_completed_price_utc"],
            "2026-08-11T12:04:00+00:00",
        )

    def test_technical_table_keeps_news_and_technical_roles_separate(self):
        rows = [
            {
                "pre30_side": "long",
                "pre30_correct": True,
                "technical_available": True,
                "technical_confirmed": False,
            },
            {
                "pre30_side": "conflicted_or_none",
                "pre30_correct": False,
                "technical_available": True,
                "technical_confirmed": True,
            },
        ]
        table = {
            (row["news_state"], row["technical_state"]): row["count"]
            for row in audit.technical_incremental_table(rows)
        }
        self.assertEqual(table[("news_correct", "technical_wrong")], 1)
        self.assertEqual(
            table[("no_directional_news", "technical_correct")], 1
        )

    def test_top_story_outcomes_use_one_strongest_story_per_move(self):
        rows = [
            {
                "_top_pre": [
                    {"source_population": "official_policy_publisher"},
                    {"source_population": "media_aggregator"},
                ],
                "pre30_correct": True,
                "liquid_major": True,
                "prior_reaction_state": "fresh_or_muted_reaction",
                "endpoint_after_cost_pips": 8.0,
            },
            {
                "_top_pre": [
                    {"source_population": "official_policy_publisher"}
                ],
                "pre30_correct": False,
                "liquid_major": False,
                "prior_reaction_state": "aligned_move_already_underway",
                "endpoint_after_cost_pips": 4.0,
            },
        ]

        result = audit.top_story_outcomes(rows, "source_population")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["moves"], 2)
        self.assertEqual(result[0]["correct"], 1)
        self.assertEqual(result[0]["fresh_or_muted"], 1)
        self.assertEqual(result[0]["average_selected_move_net_room_pips"], 6.0)

    def test_wilson_lower_bound_prevents_small_sample_overstatement(self):
        self.assertLess(audit.wilson_lower_bound(6, 8), 0.5)
        self.assertLess(audit.wilson_lower_bound(5, 7), 0.5)
        self.assertIsNone(audit.wilson_lower_bound(0, 0))


if __name__ == "__main__":
    unittest.main()
