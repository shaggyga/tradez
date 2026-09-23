"""Retained peer alignment checks against isolated synthetic SQLite stores."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import zlib

import numpy as np

import oanda_rolling_technical_alignment_v2 as alignment
from oanda_rolling_technical_store_v1 import TechnicalStore
from test_oanda_rolling_technical_store_v1 import BASE, synthetic_rows

PAIRS = ("EUR_USD", "EUR_GBP", "EUR_JPY", "GBP_USD", "AUD_USD")
NAMES = tuple(f"m1__return_{h}_bps" for h in (1, 5, 15, 60))
UNIVERSE = tuple("""AUD_CAD AUD_CHF AUD_HKD AUD_JPY AUD_NZD AUD_SGD AUD_USD
CAD_CHF CAD_HKD CAD_JPY CAD_SGD CHF_HKD CHF_JPY CHF_ZAR EUR_AUD EUR_CAD EUR_CHF
EUR_CZK EUR_DKK EUR_GBP EUR_HKD EUR_HUF EUR_JPY EUR_NOK EUR_NZD EUR_PLN EUR_SEK
EUR_SGD EUR_TRY EUR_USD EUR_ZAR GBP_AUD GBP_CAD GBP_CHF GBP_HKD GBP_JPY GBP_NZD
GBP_PLN GBP_SGD GBP_USD GBP_ZAR HKD_JPY NZD_CAD NZD_CHF NZD_HKD NZD_JPY NZD_SGD
NZD_USD SGD_CHF SGD_JPY TRY_JPY USD_CAD USD_CHF USD_CNH USD_CZK USD_DKK USD_HKD
USD_HUF USD_JPY USD_MXN USD_NOK USD_PLN USD_SEK USD_SGD USD_THB USD_TRY USD_ZAR
ZAR_JPY""".split())
STRUCTURAL = ["EUR_CZK", "EUR_DKK", "EUR_HUF", "EUR_NOK", "EUR_SEK", "USD_CNH",
              "USD_CZK", "USD_DKK", "USD_HUF", "USD_MXN", "USD_NOK", "USD_SEK", "USD_THB"]


class RetainedPeerAlignmentTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = TechnicalStore(Path(self.directory.name) / "synthetic.sqlite",
                                    {"feature_names": list(NAMES), "pairs": dict.fromkeys(UNIVERSE, .0001)})

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def append(self, pair, offsets=(0, 1, 2), *, missing=None, published=None):
        data = synthetic_rows([100] * len(offsets), offsets=offsets)
        features = {name: np.asarray(offsets, dtype=float) + index * 10 for index, name in enumerate(NAMES)}
        if missing is not None:
            features[missing][:] = np.nan
        observed = float(data["time"][-1] + 65)
        self.store.ingest(pair, data, features, {"observed_epoch": observed, "pair": pair},
                          published_epoch=observed + 1 if published is None else published)
        return self.store.latest(pair)

    def select(self, latest, *, now=BASE + 190, age=180, minutes=8):
        return alignment.build_aligned_panels(self.store, latest, now_epoch=now,
                maximum_bar_age_seconds=age, max_candidate_minutes=minutes)

    def test_minute_behind_pairs_use_retained_exact_context_without_replacing_latest(self):
        latest = {pair: self.append(pair, (0, 1, 2) if pair == "EUR_USD" else (0, 1)) for pair in PAIRS}
        original = copy.deepcopy(latest)
        writes_before = self.store.connection.total_changes
        result = self.select(latest)
        reference = result["by_pair"]["EUR_USD"]
        self.assertEqual(reference["target_bar_start_epoch"], BASE + 60)
        self.assertEqual(reference["alignment_lag_seconds"], 60)
        self.assertEqual(reference["status"], "available")
        self.assertEqual(reference["latest_pair_local_bar_start_epoch"], BASE + 120)
        self.assertEqual(result["candidates"][0]["target_bar_start_epoch"], BASE + 120)
        selected = next(c for c in result["candidates"] if c["target_bar_start_epoch"] == BASE + 60)
        self.assertEqual(selected["panel"]["accepted_clock_pair_count"], 5)
        self.assertEqual(selected["source_observations"]["EUR_USD"]["values"][NAMES[0]], 1)
        self.assertEqual(latest["EUR_USD"]["values"][NAMES[0]], 2)
        self.assertEqual(latest, original)
        self.assertEqual(self.store.connection.total_changes, writes_before)

    def test_newest_fully_supported_minute_wins_and_actual_publication_is_preserved(self):
        latest = {pair: self.append(pair) for pair in PAIRS}
        result = self.select(latest)
        reference = result["by_pair"]["EUR_USD"]
        self.assertEqual(reference["target_bar_start_epoch"], BASE + 120)
        self.assertEqual(reference["observation"]["published_epoch"], BASE + 186)
        self.assertGreater(reference["observation"]["published_epoch"], BASE + 180)
        self.assertEqual(result["candidates"][0]["source_max_published_epoch"], BASE + 186)
        self.assertNotIn("published_epoch", result["candidates"][0])

    def test_exact_hole_has_no_asof_substitution_even_with_adjacent_rows(self):
        latest = {pair: self.append(pair, (0, 2) if pair == "EUR_USD" else (0, 1)) for pair in PAIRS}
        result = self.select(latest, age=90)
        old = next(c for c in result["candidates"] if c["target_bar_start_epoch"] == BASE + 60)
        self.assertNotIn("EUR_USD", old["source_observations"])
        self.assertEqual(old["panel"]["by_pair"]["EUR_USD"]["status"], "unavailable")
        self.assertEqual(result["by_pair"]["EUR_USD"]["target_bar_start_epoch"], BASE + 120)
        self.assertEqual(result["by_pair"]["EUR_USD"]["finite_feature_count"], 0)

    def test_fresh_boundary_is_inclusive_and_age_recheck_expires_older_context(self):
        latest = {pair: self.append(pair, (0, 1, 2) if pair == "EUR_USD" else (0, 1)) for pair in PAIRS}
        at_boundary = self.select(latest, now=BASE + 210, age=90)
        self.assertEqual(at_boundary["by_pair"]["EUR_USD"]["target_bar_start_epoch"], BASE + 60)
        expired = self.select(latest, now=BASE + 210.001, age=90)
        self.assertEqual(expired["by_pair"]["EUR_USD"]["target_bar_start_epoch"], BASE + 120)
        self.assertEqual(expired["by_pair"]["EUR_USD"]["status"], "unavailable")
        self.assertEqual(expired["by_pair"]["GBP_USD"]["target_bar_start_epoch"], None)

    def test_missing_horizon_stays_none_while_real_zero_remains_available(self):
        latest = {pair: self.append(pair, (0,), missing=NAMES[1] if pair == "EUR_GBP" else None) for pair in PAIRS}
        result = self.select(latest, now=BASE + 70)
        peer = result["candidates"][0]["panel"]["by_pair"]["EUR_USD"]
        self.assertEqual(peer["values"]["peer__diff_1_bps"], 0.0)
        self.assertIsNone(peer["values"]["peer__base_mean_5_bps"])
        self.assertIsNone(peer["values"]["peer__diff_5_bps"])
        self.assertEqual(peer["values"]["peer__quote_mean_5_bps"], -10.0)
        self.assertEqual(peer["support"]["5"]["base"]["missing_horizon_peers"], ["EUR_GBP"])

    def test_all_68_pairs_preserved_with_13_explicit_structural_limits(self):
        latest = {pair: self.append(pair, (0,)) for pair in UNIVERSE}
        result = self.select(latest, now=BASE + 70)
        self.assertEqual(len(result["by_pair"]), 68)
        self.assertEqual(result["structural_peer_limited_pairs"], STRUCTURAL)
        self.assertEqual(sum(row["status"] == "available" for row in result["by_pair"].values()), 55)
        self.assertEqual(sum(row["status"] == "partial" for row in result["by_pair"].values()), 13)
        self.assertEqual(result["by_pair"]["USD_CNH"]["quote_peer_pair_count"], 0)
        self.assertEqual(result["by_pair"]["EUR_CZK"]["quote_peer_pair_count"], 1)

    def test_stale_try_block_does_not_delay_other_pairs_or_reduce_structural_universe(self):
        latest = {pair: self.append(pair, (-60,) if "TRY" in pair else (0, 1, 2)) for pair in UNIVERSE}
        result = self.select(latest)
        self.assertEqual(result["candidates"][0]["panel"]["accepted_clock_pair_count"], 65)
        self.assertEqual(result["by_pair"]["EUR_USD"]["target_bar_start_epoch"], BASE + 120)
        self.assertEqual(result["by_pair"]["EUR_USD"]["status"], "available")
        for pair in ("EUR_TRY", "USD_TRY", "TRY_JPY"):
            self.assertIsNone(result["by_pair"][pair]["target_bar_start_epoch"])
            self.assertFalse(result["by_pair"][pair]["structural_peer_limited"])
        self.assertEqual(result["structural_peer_limited_pairs"], STRUCTURAL)
        self.assertTrue(all(t >= BASE for t in result["candidate_target_epochs"]))

    def test_source_unavailable_flag_excludes_retained_rows_and_future_latest_is_hidden(self):
        latest = {pair: self.append(pair) for pair in PAIRS}
        latest["EUR_GBP"] = None
        latest["GBP_USD"]["published_epoch"] = BASE + 191
        result = self.select(latest)
        for candidate in result["candidates"]:
            self.assertNotIn("EUR_GBP", candidate["source_observations"])
            self.assertNotIn("GBP_USD", candidate["source_observations"])
        self.assertEqual(result["by_pair"]["EUR_USD"]["status"], "unavailable")

    def test_future_retained_row_is_never_used_above_pair_local_clock(self):
        self.append("EUR_USD", (0, 1))
        local = self.store.latest("EUR_USD")
        self.append("EUR_USD", (0, 1, 2))
        latest = {pair: self.append(pair) for pair in PAIRS if pair != "EUR_USD"}
        latest["EUR_USD"] = local
        result = self.select(latest)
        self.assertNotIn("EUR_USD", result["candidates"][0]["source_observations"])
        self.assertEqual(result["by_pair"]["EUR_USD"]["target_bar_start_epoch"], BASE + 60)

    def test_unpublished_future_publication_and_incomplete_rows_are_rejected(self):
        latest = {pair: self.append(pair) for pair in PAIRS}
        for pair, published in (("EUR_GBP", 0), ("GBP_USD", BASE + 191), ("EUR_JPY", BASE + 179)):
            self.store.connection.execute("UPDATE observations SET published=? WHERE pair=? AND t=?", (published, pair, BASE + 120))
        result = self.select(latest)
        rejected = {row["pair"] for row in result["rejected_retained_sources"]}
        self.assertEqual(rejected, {"EUR_GBP", "GBP_USD", "EUR_JPY"})
        self.assertEqual(result["candidates"][0]["panel"]["accepted_clock_pair_count"], 2)
        self.assertEqual(result["by_pair"]["EUR_USD"]["target_bar_start_epoch"], BASE + 60)

    def test_original_read_after_publication_and_revised_pairs_are_rejected(self):
        latest = {pair: self.append(pair) for pair in PAIRS}
        self.store.connection.execute("UPDATE bars SET first_observed=? WHERE pair=?", (BASE + 187, "EUR_GBP"))
        self.store.connection.execute("INSERT INTO revisions VALUES(?,?,?,?,?,?)", ("GBP_USD", BASE, "old", "new", BASE + 190, "fixture"))
        result = self.select(latest)
        reasons = {row["pair"]: row["reason"] for row in result["rejected_retained_sources"]}
        self.assertIn("invalid_original_observation_clock", reasons["EUR_GBP"])
        self.assertEqual(reasons["GBP_USD"], "unresolved_original_input_revision")
        self.assertTrue(all("GBP_USD" not in c["source_observations"] for c in result["candidates"]))

    def test_corrupt_hash_schema_and_compression_bomb_fail_closed(self):
        latest = {pair: self.append(pair) for pair in PAIRS}
        self.store.connection.execute("UPDATE observations SET feature_hash='bad' WHERE pair=?", ("EUR_GBP",))
        self.store.connection.execute("UPDATE observations SET feature_count=99 WHERE pair=?", ("GBP_USD",))
        bomb = zlib.compress(b" " * (alignment.MAX_BLOB_BYTES + 1))
        self.store.connection.execute("UPDATE observations SET values_blob=? WHERE pair=?", (bomb, "EUR_JPY"))
        result = self.select(latest)
        self.assertEqual({row["pair"] for row in result["rejected_retained_sources"]}, {"EUR_GBP", "GBP_USD", "EUR_JPY"})
        self.assertTrue(all(c["panel"]["accepted_clock_pair_count"] == 2 for c in result["candidates"]))

    def test_nonfinite_or_huge_json_numbers_do_not_become_missing_zero(self):
        latest = {pair: self.append(pair) for pair in PAIRS}
        for pair, value in (("EUR_GBP", float("nan")), ("GBP_USD", 10 ** 500)):
            raw = json.dumps([value, 1, 2, 3]).encode()
            self.store.connection.execute("UPDATE observations SET values_blob=?,feature_hash=? WHERE pair=?",
                                          (zlib.compress(raw), hashlib.sha256(raw).hexdigest(), pair))
        result = self.select(latest)
        self.assertEqual({row["pair"] for row in result["rejected_retained_sources"]}, {"EUR_GBP", "GBP_USD"})

    def test_explicit_bounds_and_readonly_indexed_sql(self):
        latest = {pair: self.append(pair, tuple(range(30))) for pair in PAIRS}
        queries = []
        self.store.connection.set_trace_callback(queries.append)
        result = self.select(latest, now=BASE + 1810, age=1800, minutes=2)
        self.store.connection.set_trace_callback(None)
        self.assertEqual(len(result["candidate_target_epochs"]), 2)
        self.assertEqual(result["retained_rows_read"], len(PAIRS) * 2)
        self.assertTrue(all(query.lstrip().startswith("SELECT") for query in queries))
        with self.assertRaisesRegex(ValueError, "bounded_candidate"):
            self.select(latest, minutes=17)
        with self.assertRaisesRegex(ValueError, "bounded_freshness"):
            self.select(latest, age=3601)
        with self.assertRaisesRegex(ValueError, "target_outside"):
            alignment.read_exact_observations(self.store, PAIRS, [BASE + 180], now_epoch=BASE + 190, maximum_bar_age_seconds=180)
        with self.assertRaisesRegex(ValueError, "unique_exact"):
            alignment.read_exact_observations(self.store, PAIRS, [BASE + 1], now_epoch=BASE + 190, maximum_bar_age_seconds=180)

    def test_empty_unavailable_universe_has_no_candidates(self):
        result = self.select(dict.fromkeys(UNIVERSE))
        self.assertEqual(result["candidates"], [])
        self.assertEqual(result["retained_rows_read"], 0)
        self.assertEqual(len(result["by_pair"]), 68)


if __name__ == "__main__":
    unittest.main()
