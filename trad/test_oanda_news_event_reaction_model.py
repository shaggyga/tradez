import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from trad.oanda_news_event_reaction_model import (
    build_episode_rows,
    fit_horizon,
    surprise_coverage,
)


class NewsEventReactionModelTests(unittest.TestCase):
    def test_pair_fanout_and_nearby_topics_collapse_to_one_episode(self):
        calls = []
        for topic, timestamp in (("topic-a", 1000.0), ("topic-b", 1100.0)):
            for pair in ("EUR_USD", "GBP_USD"):
                calls.append(
                    {
                        "topic_id": topic,
                        "horizon_minutes": 15,
                        "signal_epoch": timestamp,
                        "category": "monetary_policy",
                        "pair": pair,
                        "follow_net_pips": 2.0,
                        "fade_net_pips": -4.0,
                    }
                )

        episodes = build_episode_rows(calls, cluster_minutes=30)

        self.assertEqual(len(episodes), 1)
        self.assertEqual(episodes[0]["pair_legs"], 2)
        self.assertEqual(episodes[0]["cluster_policy"], "first_known_topic_only")

    def test_small_news_history_returns_explicit_no_trade(self):
        rows = [
            {
                "signal_epoch": float(index),
                "category": "inflation_release",
                "follow_net_pips": 3.0,
                "fade_net_pips": -5.0,
            }
            for index in range(20)
        ]

        model = fit_horizon(rows, minimum_episodes=60)

        self.assertEqual(model["status"], "insufficient_independent_news_episodes")
        self.assertEqual(model["shadow_decision"], "no_trade_insufficient_news_history")
        self.assertFalse(model["account_eligible"])

    def test_stable_category_policy_is_selected_without_touching_execution(self):
        rows = [
            {
                "signal_epoch": float(index),
                "category": "inflation_release",
                "follow_net_pips": 3.0 + (index % 3) * 0.1,
                "fade_net_pips": -5.0,
            }
            for index in range(100)
        ]

        model = fit_horizon(
            rows,
            minimum_episodes=60,
            minimum_category_episodes=8,
        )

        self.assertEqual(model["status"], "historical_candidate")
        self.assertEqual(model["shadow_decision"], "forward_observe_only")
        self.assertEqual(
            model["selected_policy"]["actions"]["inflation_release"],
            "follow",
        )
        self.assertFalse(model["account_eligible"])

    def test_surprise_coverage_requires_actual_and_consensus(self):
        with tempfile.TemporaryDirectory() as folder:
            database = Path(folder) / "news.sqlite"
            connection = sqlite3.connect(database)
            connection.execute(
                "CREATE TABLE topic_events (topic_id TEXT PRIMARY KEY, payload_json TEXT)"
            )
            connection.execute(
                "INSERT INTO topic_events VALUES ('complete', ?)",
                (json.dumps({"actual_value": 3.1, "consensus_value": 3.0}),),
            )
            connection.execute(
                "INSERT INTO topic_events VALUES ('direction-only', ?)",
                (json.dumps({"topic_action": "hawkish"}),),
            )
            connection.commit()
            connection.close()

            result = surprise_coverage(database)

        self.assertEqual(result["topics"], 2)
        self.assertEqual(result["actual_and_consensus_topics"], 1)
        self.assertEqual(result["actual_and_consensus_coverage"], 0.5)


if __name__ == "__main__":
    unittest.main()
