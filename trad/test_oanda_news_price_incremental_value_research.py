import unittest

import pandas as pd

import oanda_news_price_incremental_value_research as research


class NewsPriceIncrementalValueTests(unittest.TestCase):
    def test_source_feature_row_excludes_future(self):
        base = {
            "source_event_id": "past", "source_id": "official", "source_population": "official_policy_publisher",
            "event_type": "policy", "story_cluster_id": "s1", "effective_from_utc": "x",
            "effective_epoch": 900, "valid_until_epoch": None, "superseded_epoch": None, "payload_json": "{}",
        }
        future = {**base, "source_event_id": "future", "story_cluster_id": "s2", "effective_epoch": 1100}
        features = research.source_feature_row({"EUR": [base, future]}, "EUR", "USD", 1000)
        self.assertEqual(features["source_rows_6h"], 1.0)
        self.assertEqual(features["official_rows_6h"], 1.0)

    def test_permutation_preserves_source_marginals(self):
        frame = pd.DataFrame({name: [1.0, 2.0, 3.0] for name in research.SOURCE_FEATURES})
        result = research.permute_source(frame, 1)
        for name in research.SOURCE_FEATURES:
            self.assertEqual(sorted(result[name]), [1.0, 2.0, 3.0])


if __name__ == "__main__":
    unittest.main()
