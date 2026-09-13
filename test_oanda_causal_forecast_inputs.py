import base64
from copy import deepcopy
import csv
import hashlib
import math
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

try:
    from trad import oanda_causal_forecast_inputs as subject
except ModuleNotFoundError:
    import oanda_causal_forecast_inputs as subject

START = 1788732000.0  # Whole-minute synthetic clock, independent of local time.
FIELDS = ["time", "datetime", "instrument", "granularity", "close"]


def line(pair, epoch, index):
    from datetime import datetime, timezone
    timestamp = datetime.fromtimestamp(epoch, timezone.utc).isoformat()
    pip = .01 if pair.endswith("_JPY") else .0001
    base = 150 if pair.endswith("_JPY") else 1.1
    phase = subject.PAIRS.index(pair) * .4
    return [timestamp, timestamp, pair, "M1", str(base + pip * (3 * math.sin(index / 17 + phase) + math.sin(index / 3)))]


def write_fixture(root, count=512, *, omit=None, extra=None):
    for pair in subject.PAIRS:
        if pair == omit:
            continue
        rows = [line(pair, START + index * 60, index) for index in range(count)]
        if extra:
            rows = extra(pair, rows)
        with (root / f"{pair}_M1.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(FIELDS)
            writer.writerows(rows)


class _FixtureBase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def capture(self, count=512, **kwargs):
        write_fixture(self.root, count, **kwargs)
        return subject.capture_inputs(self.root, clock=lambda: START + count * 60 + 1)


    def compute(self, captured):
        return subject.compute_predictions(captured, clock=lambda: captured.get("first_observed_epoch", 0) + 10)


class CaptureTests(_FixtureBase):
    def test_common_timestamp_window_and_conservative_clocks(self):
        captured = self.capture()
        self.assertEqual(captured["status"], "ready")
        self.assertEqual(captured["common_start_epochs"], [START + index * 60 for index in range(512)])
        self.assertEqual(captured["max_bar_close_epoch"], START + 512 * 60)
        self.assertEqual(captured["training_labels_available_max_epoch"], START + 512 * 60 + 1)
        self.assertEqual(captured["feature_cutoff_epoch"], captured["max_bar_close_epoch"])
        self.assertFalse(captured["can_place_orders"])
        self.assertEqual(set(captured["series"]), set(subject.PAIRS))

    def test_captured_bytes_roundtrip_and_hashes(self):
        captured = self.capture()
        for pair, source in captured["sources"].items():
            header = base64.b64decode(source["header_base64"])
            tail = base64.b64decode(source["tail_base64"])
            raw = (self.root / f"{pair}_M1.csv").read_bytes()
            self.assertEqual(header + tail, raw)
            self.assertEqual(hashlib.sha256(raw).hexdigest(), source["captured_bytes_sha256"])
            self.assertEqual(raw[source["tail_byte_offset"]:], tail)
        self.assertEqual(subject._verified_series(captured), captured["series"])

    def test_missing_pair_abstains_entire_comparison(self):
        captured = self.capture(omit="NZD_USD")
        self.assertEqual(captured["status"], "abstain")
        self.assertEqual(captured["series"], {})
        self.assertTrue(any(reason.startswith("NZD_USD:") for reason in captured["reasons"]))

    def test_minimum_pooled_training_requirement(self):
        captured = self.capture(335)
        self.assertEqual(captured["status"], "ready")
        self.assertEqual(captured["pooled_training_rows"], 301)
        captured = self.capture(334)
        self.assertEqual(captured["status"], "abstain")
        self.assertIn("contiguous_common_warmup", " ".join(captured["reasons"]))

    def test_extra_tail_rows_do_not_expand_model_window(self):
        captured = self.capture(1800)
        self.assertEqual(captured["status"], "ready")
        self.assertEqual(len(captured["series"]["EUR_USD"]), 512)
        self.assertEqual(captured["common_start_epochs"][0], START + 1288 * 60)
        source = captured["sources"]["EUR_USD"]
        tail = base64.b64decode(source["tail_base64"])
        self.assertEqual(len(tail.splitlines()), 1024)
        raw = (self.root / "EUR_USD_M1.csv").read_bytes()
        self.assertEqual(raw[source["tail_byte_offset"]:], tail)

    def test_bounded_byte_read_still_has_exact_offsets(self):
        write_fixture(self.root, 700)
        with patch.object(subject, "MAX_TAIL_BYTES", 70000):
            captured = subject.capture_inputs(self.root, clock=lambda: START + 700 * 60 + 1)
        self.assertEqual(captured["status"], "ready")
        source = captured["sources"]["EUR_USD"]
        self.assertLessEqual(source["tail_byte_length"], 70000)
        raw = (self.root / "EUR_USD_M1.csv").read_bytes()
        self.assertEqual(raw[source["tail_byte_offset"]:], base64.b64decode(source["tail_base64"]))

    def test_alignment_uses_actual_times_for_asynchronous_pair_refresh(self):
        def extra(pair, rows):
            return rows[:-5] if pair == "USD_CHF" else rows
        captured = self.capture(extra=extra)
        self.assertEqual(captured["status"], "ready")
        self.assertEqual(len(captured["common_start_epochs"]), 507)
        self.assertEqual(captured["common_start_epochs"][-1], START + 506 * 60)
        self.assertEqual(captured["series"]["EUR_USD"][-1], float(line("EUR_USD", START + 506 * 60, 506)[4]))

    def test_missing_bar_cannot_be_compressed_into_one_hour(self):
        def extra(pair, rows):
            return rows[:400] + rows[401:] if pair == "GBP_USD" else rows
        captured = self.capture(extra=extra)
        self.assertEqual(captured["status"], "abstain")
        self.assertIn("contiguous_common_warmup:111", " ".join(captured["reasons"]))

    def test_older_gap_is_excluded_by_contiguous_suffix(self):
        def extra(pair, rows):
            return rows[:50] + rows[51:] if pair == "GBP_USD" else rows
        captured = self.capture(extra=extra)
        self.assertEqual(captured["status"], "ready")
        self.assertEqual(len(captured["common_start_epochs"]), 461)

    def test_weekend_gap_requires_fresh_contiguous_warmup(self):
        def extra(pair, rows):
            return rows[:400] + [line(pair, START + (index + 2880) * 60, index) for index in range(400, 512)]
        write_fixture(self.root, extra=extra)
        captured = subject.capture_inputs(self.root, clock=lambda: START + (512 + 2880) * 60 + 1)
        self.assertEqual(captured["status"], "abstain")
        self.assertIn("contiguous_common_warmup:112", " ".join(captured["reasons"]))

    def test_stale_common_bar_abstains_even_if_read_now(self):
        write_fixture(self.root)
        captured = subject.capture_inputs(self.root, clock=lambda: START + 512 * 60 + 901)
        self.assertEqual(captured["status"], "abstain")
        self.assertIn("stale_common_bar_window", " ".join(captured["reasons"]))

    def test_bar_close_after_observation_is_not_mature(self):
        write_fixture(self.root)
        captured = subject.capture_inputs(self.root, clock=lambda: START + 512 * 60 - 1)
        self.assertEqual(captured["status"], "abstain")
        self.assertIn("bar_not_complete_at_observation", " ".join(captured["reasons"]))

    def test_duplicate_or_out_of_order_is_rejected_before_alignment(self):
        for mode in ("duplicate", "reverse"):
            with self.subTest(mode=mode):
                def extra(pair, rows):
                    if pair != "AUD_USD":
                        return rows
                    return rows + [rows[-1]] if mode == "duplicate" else list(reversed(rows))
                captured = self.capture(extra=extra)
                self.assertEqual(captured["status"], "abstain")
                self.assertIn("duplicate_or_unordered_bar", " ".join(captured["reasons"]))

    def test_invalid_numeric_not_silently_dropped(self):
        for invalid in ("nan", "inf", "-1", "0", "bad"):
            with self.subTest(invalid=invalid):
                def extra(pair, rows):
                    if pair == "EUR_USD":
                        rows[0][4] = invalid
                    return rows
                captured = self.capture(extra=extra)
                self.assertEqual(captured["status"], "abstain")

    def test_timestamp_and_identity_validation(self):
        changes = ((0, "2026-09-06T00:00:01+00:00", "bar_start_not_minute_aligned"),
                   (0, "2026-09-06T00:00:00", "bar_timezone_required"),
                   (0, "2026-09-06T00:00:00+00:00", "conflicting_bar_timestamps"),
                   (2, "EUR_JPY", "wrong_bar_instrument"),
                   (3, "M5", "wrong_bar_granularity"))
        for column, value, reason in changes:
            with self.subTest(reason=reason):
                def extra(pair, rows):
                    if pair == "EUR_USD":
                        rows[0][column] = value
                    return rows
                captured = self.capture(extra=extra)
                self.assertEqual(captured["status"], "abstain")
                self.assertIn(reason, " ".join(captured["reasons"]))

    def test_partial_append_is_rejected(self):
        write_fixture(self.root)
        with (self.root / "EUR_USD_M1.csv").open("ab") as handle:
            handle.write(b"2026-09-06T")
        captured = subject.capture_inputs(self.root, clock=lambda: START + 512 * 60 + 1)
        self.assertEqual(captured["status"], "abstain")
        self.assertIn("empty_or_partial_csv_tail", " ".join(captured["reasons"]))

    def test_clock_regression_is_rejected(self):
        write_fixture(self.root)
        clocks = iter([START + 512 * 60 + 3] + [START + 512 * 60 + 1] * 8)
        captured = subject.capture_inputs(self.root, clock=lambda: next(clocks))
        self.assertEqual(captured["status"], "abstain")
        self.assertIn("observation_clock_moved_backwards", " ".join(captured["reasons"]))

    def test_invalid_clock_abstains(self):
        for clock in (True, float("nan"), float("inf"), "time"):
            with self.subTest(clock=clock):
                captured = subject.capture_inputs(self.root, clock=lambda: clock)
                self.assertEqual(captured["status"], "abstain")

    def test_forecast_capture_or_raw_bytes_tampering_rejected(self):
        captured = self.capture()
        bad = deepcopy(captured)
        bad["series"]["EUR_USD"][-1] = 10
        with self.assertRaisesRegex(ValueError, "input_capture_hash_mismatch"):
            subject._verified_series(bad)
        bad.pop("source_capture_sha256")
        subject._seal(bad)
        with self.assertRaisesRegex(ValueError, "derived_model_window_mismatch"):
            subject._verified_series(bad)
        bad = deepcopy(captured)
        bad["sources"]["EUR_USD"]["tail_base64"] = base64.b64encode(b"corrupted\n").decode()
        bad.pop("source_capture_sha256")
        subject._seal(bad)
        with self.assertRaisesRegex(ValueError, "captured_source_hash_mismatch"):
            subject._verified_series(bad)

    def test_changed_source_during_read_rejected(self):
        write_fixture(self.root)
        original = subject._signature
        calls = 0
        def signatures(stat):
            nonlocal calls
            calls += 1
            signature = original(stat)
            return signature + (calls,)
        with patch.object(subject, "_signature", side_effect=signatures):
            captured = subject.capture_inputs(self.root, clock=lambda: START + 512 * 60 + 1)
        self.assertEqual(captured["status"], "abstain")
        self.assertIn("source_changed_during_read", " ".join(captured["reasons"]))

    def test_derived_maturity_availability_and_count_cannot_be_rebound(self):
        captured = self.capture()
        for field in ("max_bar_close_epoch", "training_label_maturity_max_epoch", "feature_cutoff_epoch",
                      "features_available_epoch", "training_labels_available_max_epoch", "pooled_training_rows"):
            with self.subTest(field=field):
                altered = deepcopy(captured)
                altered[field] += 1
                altered.pop("source_capture_sha256")
                subject._seal(altered)
                with self.assertRaises(ValueError):
                    subject._verified_series(altered)

    def test_bar_must_be_complete_at_its_own_source_read(self):
        write_fixture(self.root)
        # A later global clock must not retroactively make an incomplete bar
        # in the first pair mature at that pair's earlier observation.
        clocks = iter([START + 512 * 60 - 2, START + 512 * 60 - 1]
                      + [START + 512 * 60 + 1] * 7)
        captured = subject.capture_inputs(self.root, clock=lambda: next(clocks))
        self.assertEqual(captured["status"], "abstain")
        self.assertIn("bar_not_complete_at_observation", " ".join(captured["reasons"]))

    def test_clock_regression_between_pairs_is_rejected(self):
        write_fixture(self.root)
        clocks = iter([START + 512 * 60 + 1, START + 512 * 60 + 3]
                      + [START + 512 * 60 + 2] * 6 + [START + 512 * 60 + 4])
        captured = subject.capture_inputs(self.root, clock=lambda: next(clocks))
        self.assertEqual(captured["status"], "abstain")
        self.assertIn("observation_clock_moved_backwards", " ".join(captured["reasons"]))


class NumericalTests(_FixtureBase):
    def stub(self, *, missing=False, expected=-1.25, probability=.8):
        def prediction(series, pips):
            self.assertEqual(set(series), set(subject.PAIRS))
            self.assertEqual(pips["USD_JPY"], .01)
            self.assertEqual(pips["EUR_USD"], .0001)
            return {} if missing else {"EUR_USD": (expected, probability, {"observations": len(series["EUR_USD"])})}
        return SimpleNamespace(ridge_predictions=prediction, pooled_tabular_predictions=prediction,
            graph_predictions=prediction, state_space_predictions=prediction)

    def test_four_outputs_preserve_expected_side_independently_of_probability(self):
        captured = self.capture()
        with patch.object(subject, "_numerical_module", return_value=self.stub()):
            computed = self.compute(captured)
        self.assertEqual(computed["status"], "ready")
        self.assertEqual(computed["source_capture_sha256"], captured["source_capture_sha256"])
        self.assertEqual(computed["computed_epoch"], captured["first_observed_epoch"] + 10)
        self.assertEqual(set(computed["predictions"]), set(subject.FAMILIES))
        for prediction in computed["predictions"].values():
            self.assertEqual(prediction["side"], -1)
            self.assertEqual(prediction["probability_up"], .8)

    def test_any_missing_family_abstains_whole_batch(self):
        captured = self.capture()
        model = self.stub()
        model.pooled_tabular_predictions = lambda *_: {}
        with patch.object(subject, "_numerical_module", return_value=model):
            computed = self.compute(captured)
        self.assertEqual(computed["status"], "abstain")
        self.assertEqual(computed["predictions"], {})

    def test_invalid_model_numbers_abstain(self):
        captured = self.capture()
        for expected, probability in ((float("nan"), .5), (0, float("inf")), (0, -1), (0, 2)):
            with self.subTest(expected=expected, probability=probability):
                with patch.object(subject, "_numerical_module", return_value=self.stub(expected=expected, probability=probability)):
                    computed = self.compute(captured)
                self.assertEqual(computed["status"], "abstain")
                self.assertEqual(computed["predictions"], {})

    def test_source_and_dependency_pins(self):
        captured = self.capture()
        with patch.object(subject, "NUMERICAL_SOURCE_SHA256", "0" * 64):
            computed = self.compute(captured)
            self.assertEqual(computed["status"], "abstain")
        with patch.object(subject, "dependency_versions", return_value={"python": "different"}):
            computed = self.compute(captured)
            self.assertEqual(computed["status"], "abstain")
        with patch.object(subject, "NUMERICAL_SOURCE_SHA256", "0" * 64):
            with self.assertRaisesRegex(ValueError, "numerical_source_binding_changed"):
                subject._numerical_module()

    def test_warmup_does_not_load_numerical_module(self):
        captured = self.capture(50)
        with patch.object(subject, "_numerical_module", side_effect=AssertionError("must not load")):
            computed = self.compute(captured)
        self.assertEqual(computed["status"], "abstain")

    def test_numerical_error_is_an_abstention_not_partial_predictions(self):
        captured = self.capture()
        model = self.stub()
        model.graph_predictions = lambda *_: (_ for _ in ()).throw(ArithmeticError("fixture failure"))
        with patch.object(subject, "_numerical_module", return_value=model):
            computed = self.compute(captured)
        self.assertEqual(computed["status"], "abstain")
        self.assertEqual(computed["predictions"], {})

    def test_computation_clock_sampled_after_all_four_models(self):
        captured = self.capture()
        calls = []
        model = self.stub()
        for family, name in zip(subject.FAMILIES, ("ridge_predictions", "pooled_tabular_predictions",
                                                 "graph_predictions", "state_space_predictions")):
            original = getattr(model, name)
            def recording(*args, original=original, family=family):
                calls.append(family)
                return original(*args)
            setattr(model, name, recording)
        def clock():
            self.assertEqual(calls, list(subject.FAMILIES))
            return captured["first_observed_epoch"] + 20
        with patch.object(subject, "_numerical_module", return_value=model):
            result = subject.compute_predictions(captured, clock=clock)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["computed_epoch"], captured["first_observed_epoch"] + 20)

    def test_computation_clock_cannot_precede_capture(self):
        captured = self.capture()
        for clock_value in (captured["first_observed_epoch"] - 1, float("nan"), True):
            with self.subTest(clock_value=clock_value):
                with patch.object(subject, "_numerical_module", return_value=self.stub()):
                    result = subject.compute_predictions(captured, clock=lambda: clock_value)
                self.assertEqual(result["status"], "abstain")
                self.assertEqual(result["predictions"], {})
                self.assertNotIn("computed_epoch", result)

    def test_actual_numerical_models_on_timestamped_fixture(self):
        captured = self.capture()
        from threadpoolctl import threadpool_limits
        with threadpool_limits(limits=1):
            computed = self.compute(captured)
        self.assertEqual(computed["status"], "ready", computed["reasons"])
        self.assertEqual(set(computed["predictions"]), set(subject.FAMILIES))
        for prediction in computed["predictions"].values():
            self.assertTrue(math.isfinite(prediction["expected_signed_pips"]))
            self.assertTrue(0 <= prediction["probability_up"] <= 1)


if __name__ == "__main__":
    unittest.main()
