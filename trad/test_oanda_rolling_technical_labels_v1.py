"""Independent historical-label checks; synthetic arrays/temp databases only."""
from __future__ import annotations

from collections import OrderedDict
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np

from oanda_rolling_technical_labels_v1 import (
    compute_outcomes, label_registry, DEFAULT_HORIZONS, NUMERIC_OUTCOMES,
)
from oanda_rolling_technical_store_v1 import TechnicalStore


BASE = 1_700_000_040


def rows(prices, offsets=None):
    close = np.asarray(prices, dtype=float)
    offset = np.arange(len(close)) if offsets is None else np.asarray(offsets)
    return {
        "time": BASE + offset.astype(np.int64) * 60,
        "open": close.copy(), "high": close + .05, "low": close - .05,
        "close": close, "bid_close": close - .1, "ask_close": close + .1,
        "volume": np.zeros(len(close)),
    }


def values(output, horizon, field):
    return output[f"label__{horizon}m__{field}"]


def scalar_oracle(data, origin, horizon):
    """Intentionally slow exact-clock reference, independent of rolling joins."""
    ts = data["time"]
    by_time = {int(t): i for i, t in enumerate(ts)}
    target = int(ts[origin]) + 60 * horizon
    if target > ts[-1]:
        return "pending_right_edge", np.nan, None
    positions = [by_time.get(t) for t in range(int(ts[origin]), target + 1, 60)]
    missing = positions.count(None)
    if positions[-1] is None:
        return "missing_target", missing, None
    if missing:
        return "gap_in_path", missing, None
    start, end = positions[0], positions[-1]
    denominator = data["close"][start]
    future = positions[1:]
    long_path = [(data["bid_close"][p] - data["ask_close"][start]) / denominator * 10000 for p in future]
    short_path = [(data["bid_close"][start] - data["ask_close"][p]) / denominator * 10000 for p in future]
    ret = (data["close"][end] / denominator - 1) * 10000
    return "available", 0, {
        "return_bps": ret, "absolute_return_bps": abs(ret), "direction": np.sign(ret),
        "long_net_bps": long_path[-1], "short_net_bps": short_path[-1],
        "max_favorable_long_close_bps": max(long_path),
        "max_adverse_long_close_bps": min(long_path),
        "max_favorable_short_close_bps": max(short_path),
        "max_adverse_short_close_bps": min(short_path),
    }


class TechnicalHistoricalLabelTests(unittest.TestCase):
    def test_up_move_costs_and_maturity_clock(self):
        out = compute_outcomes(rows([100, 101, 102, 103, 104, 105]), (5,))
        self.assertEqual(values(out, 5, "state")[0], "available")
        self.assertAlmostEqual(values(out, 5, "return_bps")[0], 500)
        self.assertAlmostEqual(values(out, 5, "long_net_bps")[0], 480)
        self.assertAlmostEqual(values(out, 5, "short_net_bps")[0], -520)
        self.assertEqual(values(out, 5, "direction")[0], 1)
        self.assertEqual(values(out, 5, "target_bar_start_epoch")[0], BASE + 300)
        self.assertEqual(values(out, 5, "target_bar_end_epoch")[0], BASE + 360)
        self.assertEqual(values(out, 5, "assumed_available_epoch")[0], BASE + 360)
        self.assertEqual(values(out, 5, "path_expected_bars")[0], 6)

    def test_down_move_close_excursions(self):
        out = compute_outcomes(rows([100, 102, 99, 101, 98, 95]), (5,))
        expected = {
            "direction": -1, "return_bps": -500, "absolute_return_bps": 500,
            "long_net_bps": -520, "short_net_bps": 480,
            "max_favorable_long_close_bps": 180, "max_adverse_long_close_bps": -520,
            "max_favorable_short_close_bps": 480, "max_adverse_short_close_bps": -220,
        }
        for field, value in expected.items():
            self.assertAlmostEqual(values(out, 5, field)[0], value)

    def test_quiet_zero_rows_retained_with_spread_loss(self):
        out = compute_outcomes(rows([100] * 10), (5,))
        self.assertTrue(all(len(array) == 10 for array in out.values()))
        self.assertEqual(list(values(out, 5, "state")), ["available"] * 5 + ["pending_right_edge"] * 5)
        np.testing.assert_array_equal(values(out, 5, "direction")[:5], 0)
        np.testing.assert_array_equal(values(out, 5, "return_bps")[:5], 0)
        np.testing.assert_allclose(values(out, 5, "long_net_bps")[:5], -20)
        np.testing.assert_allclose(values(out, 5, "short_net_bps")[:5], -20)

    def test_all_four_states_and_precise_missing_counts(self):
        offsets = [0, 1, 2, 3, 4, 5, 7, 8, 10, 11, 12]
        out = compute_outcomes(rows([100] * len(offsets), offsets), (5,))
        state = values(out, 5, "state")
        self.assertEqual(state[0], "available")
        self.assertEqual(state[1], "missing_target")
        self.assertEqual(state[2], "gap_in_path")
        self.assertEqual(state[-1], "pending_right_edge")
        self.assertEqual(values(out, 5, "path_missing_bars")[0], 0)
        self.assertEqual(values(out, 5, "path_missing_bars")[1], 1)
        self.assertEqual(values(out, 5, "path_missing_bars")[4], 2)
        self.assertTrue(np.isnan(values(out, 5, "path_missing_bars")[-1]))
        invalid = state != "available"
        for field in NUMERIC_OUTCOMES:
            self.assertTrue(np.isnan(values(out, 5, field)[invalid]).all(), field)
        for field in ("midpoint_valid", "bidask_endpoint_valid", "bidask_excursion_valid"):
            self.assertFalse(values(out, 5, field)[invalid].any())

    def test_endpoint_after_weekend_not_substituted_for_missing_minute(self):
        data = rows([100, 100, 103, 104], [0, 1, 3000, 3001])
        out = compute_outcomes(data, (5,))
        self.assertEqual(values(out, 5, "state")[0], "missing_target")
        self.assertEqual(values(out, 5, "path_missing_bars")[0], 4)
        self.assertTrue(np.isnan(values(out, 5, "return_bps")[0]))
        self.assertEqual(values(out, 5, "state")[-1], "pending_right_edge")

    def test_closed_query_missing_tail_is_not_pending_right_edge(self):
        data = rows([100, 101, 102], [0, 1, 3])
        default = compute_outcomes(data, (5,))
        self.assertTrue((values(default, 5, "state") == "pending_right_edge").all())
        out = compute_outcomes(data, (5,), coverage_end_epoch=BASE + 8 * 60)
        self.assertEqual(list(values(out, 5, "state")),
                         ["missing_target", "missing_target", "pending_right_edge"])
        np.testing.assert_array_equal(values(out, 5, "path_missing_bars")[:2], [3, 4])
        self.assertTrue(np.isnan(values(out, 5, "path_missing_bars")[-1]))
        self.assertEqual(len(values(out, 5, "state")), len(data["time"]))
        for field in NUMERIC_OUTCOMES:
            self.assertTrue(np.isnan(values(out, 5, field)).all())
        weekend = compute_outcomes(data, (5,), coverage_end_epoch=BASE + 3000 * 60)
        self.assertTrue((values(weekend, 5, "state") == "missing_target").all())

    def test_explicit_default_boundary_matches_implicit_and_empty_input_valid(self):
        data = rows(np.arange(100, 110))
        default = compute_outcomes(data)
        explicit = compute_outcomes(data, coverage_end_epoch=int(data["time"][-1] + 60))
        for field in default:
            np.testing.assert_array_equal(default[field], explicit[field])
        empty = compute_outcomes(rows([]), coverage_end_epoch=BASE)
        self.assertTrue(all(len(array) == 0 for array in empty.values()))

    def test_bad_coverage_boundaries_are_refused(self):
        data = rows([100, 101, 102])
        invalid = (True, np.nan, np.inf, -60, BASE + 181, str(BASE + 180),
                   BASE + 120, 253402300860)
        for boundary in invalid:
            with self.subTest(boundary=boundary), self.assertRaises(ValueError):
                compute_outcomes(data, coverage_end_epoch=boundary)

    def test_excursions_exclude_origin_and_do_not_clip_at_zero(self):
        out = compute_outcomes(rows([100, 99, 98, 97]), (3,))
        self.assertAlmostEqual(values(out, 3, "max_favorable_long_close_bps")[0], -120)
        self.assertAlmostEqual(values(out, 3, "max_adverse_short_close_bps")[0], 80)

    def test_unused_intraminute_highs_lows_volume_never_change_labels(self):
        data = rows([100, 102, 99, 101, 98, 95])
        expected = compute_outcomes(data, (5,))
        data["high"][:] = 1000000
        data["low"][:] = .0001
        data["volume"][:] = 100000000
        actual = compute_outcomes(data, (5,))
        for name in expected:
            np.testing.assert_array_equal(actual[name], expected[name])

    def test_absent_bidask_preserves_midpoint_only(self):
        data = rows([100, 101, 102, 103, 104, 105])
        del data["bid_close"], data["ask_close"]
        out = compute_outcomes(data, (5,))
        self.assertEqual(values(out, 5, "state")[0], "available")
        self.assertTrue(values(out, 5, "midpoint_valid")[0])
        self.assertFalse(values(out, 5, "bidask_endpoint_valid")[0])
        self.assertFalse(values(out, 5, "bidask_excursion_valid")[0])
        self.assertAlmostEqual(values(out, 5, "return_bps")[0], 500)
        self.assertTrue(np.isnan(values(out, 5, "long_net_bps")[0]))

    def test_missing_intermediate_quote_invalidates_excursions_only(self):
        data = rows([100, 101, 102, 103, 104, 105])
        data["bid_close"][3] = np.nan
        out = compute_outcomes(data, (5,))
        self.assertTrue(values(out, 5, "midpoint_valid")[0])
        self.assertTrue(values(out, 5, "bidask_endpoint_valid")[0])
        self.assertFalse(values(out, 5, "bidask_excursion_valid")[0])
        self.assertAlmostEqual(values(out, 5, "long_net_bps")[0], 480)
        for field in NUMERIC_OUTCOMES:
            if "close_bps" in field:
                self.assertTrue(np.isnan(values(out, 5, field)[0]))

    def test_crossed_or_nonpositive_quotes_are_unavailable_and_input_unchanged(self):
        for bad in ("crossed", "zero", "infinite"):
            data = rows([100, 101, 102, 103, 104, 105])
            if bad == "crossed":
                data["bid_close"][0] = 101
            else:
                data["ask_close"][0] = 0 if bad == "zero" else np.inf
            before = {name: array.copy() for name, array in data.items()}
            out = compute_outcomes(data, (5,))
            self.assertTrue(values(out, 5, "midpoint_valid")[0])
            self.assertFalse(values(out, 5, "bidask_endpoint_valid")[0])
            self.assertTrue(np.isnan(values(out, 5, "long_net_bps")[0]))
            for name in before:
                np.testing.assert_array_equal(data[name], before[name])

    def test_truncation_only_changes_unmatured_suffix_and_labels_are_future_information(self):
        data = rows(np.linspace(100, 200, 90))
        first = {name: array[:40] for name, array in data.items()}
        before = compute_outcomes(first, (5, 15))
        after = compute_outcomes(data, (5, 15))
        for h in (5, 15):
            matured = 40 - h
            for field in NUMERIC_OUTCOMES:
                np.testing.assert_array_equal(values(before, h, field)[:matured], values(after, h, field)[:matured])
            self.assertEqual(values(before, h, "state")[matured], "pending_right_edge")
            self.assertEqual(values(after, h, "state")[matured], "available")

    def test_random_sparse_clock_against_independent_scalar_oracle(self):
        rng = np.random.default_rng(907)
        offsets = np.arange(500)[rng.random(500) > .10]
        data = rows(100 + np.cumsum(rng.normal(0, .05, len(offsets))), offsets)
        spread = rng.uniform(.002, .02, len(offsets))
        data["bid_close"] = data["close"] - spread
        data["ask_close"] = data["close"] + spread * 1.3
        horizons = (1, 2, 5, 15, 30, 60)
        out = compute_outcomes(data, horizons)
        for h in horizons:
            for origin in range(len(offsets)):
                state, missing, expected = scalar_oracle(data, origin, h)
                self.assertEqual(values(out, h, "state")[origin], state)
                if np.isnan(missing):
                    self.assertTrue(np.isnan(values(out, h, "path_missing_bars")[origin]))
                else:
                    self.assertEqual(values(out, h, "path_missing_bars")[origin], missing)
                for field in NUMERIC_OUTCOMES:
                    actual = values(out, h, field)[origin]
                    if expected is None:
                        self.assertTrue(np.isnan(actual))
                    else:
                        self.assertAlmostEqual(actual, expected[field], places=10)

    def test_available_and_invalid_labels_match_live_store_definitions(self):
        offsets = np.arange(150)
        offsets = offsets[offsets != 32]
        data = rows(100 + np.sin(offsets / 3), offsets)
        out = compute_outcomes(data)
        with tempfile.TemporaryDirectory() as directory:
            contract = {"feature_names": ["quiet"], "pairs": {"EUR_USD": .0001}}
            store = TechnicalStore(Path(directory) / "synthetic.sqlite", contract)
            try:
                observed = float(data["time"][-1] + 65)
                store.ingest("EUR_USD", data, {"quiet": np.zeros(len(offsets))},
                             {"observed_epoch": observed}, published_epoch=observed + 1)
                store.settle("EUR_USD", now_epoch=observed + 2)
                records = list(store.connection.execute("SELECT t,horizon,state,body FROM outcomes"))
            finally:
                store.close()
        self.assertGreater(len(records), 100)
        for h in DEFAULT_HORIZONS:
            self.assertTrue(any(record["horizon"] == h and record["state"] == "available" for record in records))
        indexes = {int(t): i for i, t in enumerate(data["time"])}
        for record in records:
            i, h = indexes[record["t"]], record["horizon"]
            body = json.loads(record["body"])
            self.assertEqual(values(out, h, "state")[i], record["state"])
            self.assertEqual(values(out, h, "path_missing_bars")[i], body["path_missing_bars"])
            self.assertEqual(values(out, h, "target_bar_end_epoch")[i], body["target_bar_end_epoch"])
            for field in NUMERIC_OUTCOMES:
                if field in body:
                    self.assertAlmostEqual(values(out, h, field)[i], body[field], places=10)
                else:
                    self.assertTrue(np.isnan(values(out, h, field)[i]))

    def test_overflow_is_null_and_not_marked_numerically_valid(self):
        data = rows([1e-300, 1e300])
        data["bid_close"] = data["close"].copy()
        data["ask_close"] = data["close"].copy()
        out = compute_outcomes(data, (1,))
        self.assertEqual(values(out, 1, "state")[0], "available")
        for field in ("midpoint_valid", "bidask_endpoint_valid", "bidask_excursion_valid"):
            self.assertFalse(values(out, 1, field)[0])
        for field in NUMERIC_OUTCOMES:
            self.assertTrue(np.isnan(values(out, 1, field)[0]))

    def test_empty_short_and_extreme_epoch_inputs(self):
        for size in (0, 1, 5):
            out = compute_outcomes(rows([100] * size))
            self.assertTrue(all(array.shape == (size,) for array in out.values()))
            for h in DEFAULT_HORIZONS:
                self.assertTrue((values(out, h, "state") == "pending_right_edge").all())
        data = rows([100, 101])
        for base in (0, 253402300680):
            data["time"] = base + np.arange(2) * 60
            out = compute_outcomes(data, (1,))
            self.assertEqual(values(out, 1, "state")[0], "available")
            self.assertAlmostEqual(values(out, 1, "return_bps")[0], 100)

    def test_registry_exact_order_scope_and_non_predictor_marking(self):
        horizons = (60, 5, 1)
        out = compute_outcomes(rows([100]), horizons)
        registry = label_registry(horizons)
        self.assertIsInstance(out, OrderedDict)
        self.assertEqual(list(out), [entry["name"] for entry in registry])
        self.assertEqual(len(out), len(set(out)))
        json.dumps(registry, allow_nan=False)
        for entry in registry:
            self.assertFalse(entry["model_input"])
            self.assertTrue(entry["future_information"])
            self.assertIn(entry["target_clock_column"], out)
            self.assertIn(entry["availability_assumption_column"], out)
            self.assertIn("assumption", entry["availability_basis"])
            self.assertIn("no_fills_or_slippage", entry["outcome_scope"])

    def test_invalid_shapes_times_and_horizons_refused_without_filtering(self):
        base = rows([100, 101, 102])
        bad_clocks = ([BASE, BASE, BASE + 60], [BASE + 60, BASE, BASE + 120],
                      [BASE, BASE + 61, BASE + 120], [BASE, np.nan, BASE + 120],
                      [-60, 0, 60], [[BASE], [BASE + 60], [BASE + 120]],
                      [str(BASE), str(BASE + 60), str(BASE + 120)])
        for clock in bad_clocks:
            with self.subTest(clock=clock), self.assertRaises(ValueError):
                compute_outcomes(dict(base, time=np.asarray(clock)), (5,))
        for field in ("close", "bid_close", "ask_close"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                compute_outcomes(dict(base, **{field: [100]}), (5,))
        for horizon in ((), (0,), (-1,), (1, 1), (True,), (1.0,), (525601,), None):
            with self.subTest(horizon=horizon), self.assertRaises(ValueError):
                compute_outcomes(base, horizon)


if __name__ == "__main__":
    unittest.main()
