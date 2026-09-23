import unittest

import oanda_news_blurb_direction_attribution as attribution


CONFIG = {
    "maximum_event_age_minutes": 360,
    "minimum_absolute_pair_score": 0.12,
    "minimum_weighted_pair_score": 0.0,
    "minimum_train_episodes_per_response_cell": 2,
    "minimum_train_direction_accuracy": 0.6,
    "chronological_train_fraction": 0.6,
    "source_grade_weights": {"official_deterministic": 1.0, "unknown": 1.0},
}


def episode(identifier: str, when: str, signed_move: float, horizon: int = 15) -> dict:
    return {
        "episode_id": identifier,
        "entry_utc": when,
        "instrument": "EUR_USD",
        "base_currency": "EUR",
        "quote_currency": "USD",
        "horizon_min": horizon,
        "market_episode_id": f"market-{identifier}",
        "signed_currency_factor": "EUR+|USD-" if signed_move > 0 else "EUR-|USD+",
        "entry_bid": 1.1000,
        "entry_ask": 1.1002,
        "exit_bid": 1.1010 if signed_move > 0 else 1.0990,
        "exit_ask": 1.1012 if signed_move > 0 else 1.0992,
        "entry_spread_pips": 2.0,
        "signed_mid_move_pips": signed_move,
        "gross_magnitude_pips": abs(signed_move),
    }


def source_row(ep: dict, score: float, *, event_id: str = "event", story: str = "story", category: str = "policy") -> dict:
    return {
        "episode_id": ep["episode_id"],
        "source_event_id": event_id,
        "relation": "pre_entry_causal",
        "causal_entry_eligible": 1,
        "effective_from_utc": ep["entry_utc"],
        "entry_utc": ep["entry_utc"],
        "base_currency": ep["base_currency"],
        "quote_currency": ep["quote_currency"],
        "horizon_min": ep["horizon_min"],
        "story_cluster_id": story,
        "source_population": "official_policy_publisher",
        "event_type": category,
        "payload_json": {
            "raw_payload": {
                "currency_scores": {"EUR": score},
                "directional_confidence": 1.0,
                "source_quality": 1.0,
                "directional_source_grade": "official_deterministic",
                "source_verified": True,
                "source_direct": True,
                "estimated_reaction_horizon_minutes": ep["horizon_min"],
                "category": category,
                "headline": event_id,
            }
        },
    }


class NewsBlurbDirectionAttributionTests(unittest.TestCase):
    def test_pair_direction_is_base_minus_quote(self):
        self.assertAlmostEqual(attribution.pair_score({"EUR": 0.7, "USD": -0.2}, "EUR", "USD"), 0.9)
        self.assertAlmostEqual(attribution.pair_score({"EUR": -0.3, "USD": 0.5}, "EUR", "USD"), -0.8)

    def test_future_and_ex_post_rows_are_rejected(self):
        ep = episode("one", "2026-08-19T12:00:00+00:00", 5)
        row = source_row(ep, 0.7)
        row["relation"] = "in_window_ex_post"
        self.assertIsNone(attribution.normalize_source_row(row, CONFIG, use_research_scores=False))
        row["relation"] = "pre_entry_causal"
        row["effective_from_utc"] = "2026-08-19T12:01:00+00:00"
        self.assertIsNone(attribution.normalize_source_row(row, CONFIG, use_research_scores=False))

    def test_reports_prior_market_move_are_rejected(self):
        ep = episode("one", "2026-08-19T12:00:00+00:00", 5)
        row = source_row(ep, 0.7)
        row["payload_json"]["raw_payload"]["reports_prior_market_move"] = True
        self.assertIsNone(attribution.normalize_source_row(row, CONFIG, use_research_scores=False))

    def test_story_deduplication_prefers_verified_direct_source(self):
        weak = {"story_key": "same", "source_event_id": "a", "source_verified": False, "source_direct": False, "weighted_pair_score": 0.9}
        strong = {"story_key": "same", "source_event_id": "b", "source_verified": True, "source_direct": True, "weighted_pair_score": 0.3}
        rows = attribution.deduplicate_stories([weak, strong])
        self.assertEqual([row["source_event_id"] for row in rows], ["b"])

    def test_executable_outcome_uses_bid_ask_not_midpoint(self):
        ep = episode("one", "2026-08-19T12:00:00+00:00", 10)
        self.assertAlmostEqual(attribution.executable_net_pips(ep, 1), 8.0)
        self.assertAlmostEqual(attribution.executable_net_pips(ep, -1), -12.0)

    def test_research_scores_are_separate_from_published_scores(self):
        payload = {"raw_payload": {"currency_scores": {}, "research_currency_scores": {"JPY": -0.6}}}
        published = attribution.extract_event_view(payload, use_research_scores=False)
        research = attribution.extract_event_view(payload, use_research_scores=True)
        self.assertEqual(published["scores"], {})
        self.assertEqual(research["scores"], {"JPY": -0.6})
        self.assertTrue(research["research_scores_used"])

    def test_chronological_calibration_can_fade_persistently_wrong_class(self):
        episodes = [
            episode("a", "2026-08-19T10:00:00+00:00", -5),
            episode("b", "2026-08-19T11:00:00+00:00", -5),
            episode("c", "2026-08-19T12:00:00+00:00", -5),
            episode("d", "2026-08-19T13:00:00+00:00", -5),
            episode("e", "2026-08-19T14:00:00+00:00", -5),
        ]
        rows = []
        for ep in episodes:
            normalized = attribution.normalize_source_row(
                source_row(ep, 0.8, event_id=f"event-{ep['episode_id']}", story=f"story-{ep['episode_id']}", category="wrong_way"),
                CONFIG,
                use_research_scores=False,
            )
            self.assertIsNotNone(normalized)
            rows.append(normalized)
        results, diagnostic = attribution.response_calibrated_holdout(rows, {ep["episode_id"]: ep for ep in episodes}, CONFIG)
        self.assertEqual(diagnostic["cells"]["wrong_way|H15"], "fade")
        self.assertEqual(diagnostic["cell_diagnostics"]["wrong_way|H15"]["independent_train_factors"], 3)
        self.assertTrue(results)
        self.assertTrue(all(row["direction_hit"] for row in results))

    def test_build_is_inert(self):
        ep = episode("one", "2026-08-19T12:00:00+00:00", 5)
        result = attribution.build([ep], [source_row(ep, 0.8)], CONFIG)
        self.assertTrue(result["research_only"])
        self.assertFalse(result["execution_eligible"])
        self.assertFalse(result["can_place_orders"])

    def test_metrics_deduplicate_currency_factor_episode_by_lowest_spread(self):
        rows = [
            {"market_episode_id": "same", "signed_currency_factor": "EUR+|USD-", "instrument": "EUR_USD", "entry_spread_pips": 1.0, "score": 0.4, "direction_hit": True, "predicted_after_cost_pips": 3.0, "flipped_after_cost_pips": -5.0},
            {"market_episode_id": "same", "signed_currency_factor": "EUR+|USD-", "instrument": "EUR_ZAR", "entry_spread_pips": 40.0, "score": 0.9, "direction_hit": False, "predicted_after_cost_pips": -100.0, "flipped_after_cost_pips": 50.0},
        ]
        result = attribution.metrics(rows)
        self.assertEqual(result["raw_episode_rows"], 2)
        self.assertEqual(result["episodes"], 1)
        self.assertEqual(result["direction_accuracy"], 1.0)
        self.assertEqual(result["mean_after_cost_pips"], 3.0)


if __name__ == "__main__":
    unittest.main()
