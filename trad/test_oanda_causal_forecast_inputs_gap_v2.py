"""Fixture-only causal/source-bound checks for the separate gap-aware adapter."""
import base64
from copy import deepcopy
import csv
import datetime as dt
import hashlib
import io
import math
from pathlib import Path
import sys
import types

import pytest

import oanda_causal_forecast_inputs_gap_v2 as inputs


FRIDAY = dt.datetime(2026, 9, 4, 10, tzinfo=dt.timezone.utc)
SUNDAY = dt.datetime(2026, 9, 6, 21, tzinfo=dt.timezone.utc)


def archives(root, *, old_n=512, new_n=61, holes=None):
    holes = holes or {}
    for index, pair in enumerate(inputs.PAIRS):
        pip = .01 if pair.endswith("_JPY") else .0001
        base = 145. if pair.endswith("_JPY") else 1.1 + .1 * index
        with (root / f"{pair}_M1.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["datetime", "instrument", "granularity", "close"])
            for start, count in ((FRIDAY, old_n), (SUNDAY, new_n)):
                for minute in range(count):
                    epoch = int((start + dt.timedelta(minutes=minute)).timestamp())
                    if epoch in holes.get(pair, set()):
                        continue
                    price = base + pip * (5 * math.sin(minute / 8. + index) + .01 * minute)
                    writer.writerow([dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat(), pair, "M1", repr(price)])
    # Last current candle has completed by this real observation clock.
    return (SUNDAY + dt.timedelta(minutes=new_n, seconds=2)).timestamp()


def sealed(capture):
    capture.pop("source_capture_sha256", None)
    return inputs._seal(capture)


@pytest.fixture
def ready_capture(tmp_path):
    observed = archives(tmp_path)
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    return capture


def test_real_capture_friday_training_and_61_current_closes_compute_all_four(ready_capture):
    capture = ready_capture
    assert capture["required_current_common_bars"] == capture["current_common_bars"] == 61
    assert capture["retained_real_rows_by_pair"] == {pair: 573 for pair in inputs.PAIRS}
    assert capture["reference_start_epoch"] == (SUNDAY + dt.timedelta(minutes=60)).timestamp()
    assert capture["max_bar_close_epoch"] == (SUNDAY + dt.timedelta(minutes=61)).timestamp()
    assert capture["max_bar_close_epoch"] < capture["first_observed_epoch"]
    assert capture["training_labels_available_max_epoch"] == capture["first_observed_epoch"]
    inputs.validate_capture(capture)
    completed = capture["first_observed_epoch"] + 20
    result = inputs.compute_predictions(capture, clock=lambda: completed)
    assert result["status"] == "ready", result["reasons"]
    assert result["computed_epoch"] == completed
    assert result["source_capture_sha256"] == capture["source_capture_sha256"]
    assert set(result["predictions"]) == set(inputs.FAMILIES)
    for prediction in result["predictions"].values():
        diagnostics = prediction["diagnostics"]
        maturity = diagnostics["training_label_maturity_max_epoch"]
        assert maturity is None or maturity < SUNDAY.timestamp()
        assert diagnostics["feature_cutoff_epoch"] <= capture["max_bar_close_epoch"]
        assert math.isfinite(prediction["expected_signed_pips"])
        assert 0 <= prediction["probability_up"] <= 1


def test_earlier_hole_does_not_require_entire_retained_archive_to_be_contiguous(tmp_path):
    holes = {"NZD_USD": {int((FRIDAY + dt.timedelta(minutes=170)).timestamp()),
                         int((SUNDAY + dt.timedelta(minutes=15)).timestamp())}}
    observed = archives(tmp_path, new_n=90, holes=holes)
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert capture["current_common_bars"] == 74
    assert capture["retained_real_rows_by_pair"]["NZD_USD"] == 600
    rows = inputs._verified_rows(capture)
    assert all(epoch not in rows["NZD_USD"] for epoch in holes["NZD_USD"])


@pytest.mark.parametrize("pair", inputs.PAIRS)
def test_hole_in_current_feature_window_causes_explicit_warmup_abstention(tmp_path, pair):
    missing = int((SUNDAY + dt.timedelta(minutes=30)).timestamp())
    observed = archives(tmp_path, holes={pair: {missing}})
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "abstain"
    assert any("current_common_warmup:30<61" in reason for reason in capture["reasons"])


@pytest.mark.parametrize("field", ["series", "current_common_start_epochs", "current_common_bars",
    "required_current_common_bars", "retained_real_rows_by_pair", "reference_start_epoch",
    "max_bar_close_epoch", "common_bar_age_sec", "training_label_maturity_max_epoch",
    "training_labels_available_max_epoch", "feature_cutoff_epoch", "features_available_epoch"])
def test_resealed_derived_field_tamper_is_rejected(ready_capture, field):
    forged = deepcopy(ready_capture)
    if field == "series": forged[field]["EUR_USD"][0][1] += .01
    elif field == "current_common_start_epochs": forged[field][0] -= 60
    elif field == "retained_real_rows_by_pair": forged[field]["EUR_USD"] -= 1
    else: forged[field] += 1
    with pytest.raises(ValueError, match="derived_input_binding_mismatch"):
        inputs.validate_capture(sealed(forged))


def test_source_bytes_must_match_retained_hashes_even_if_capture_resealed(ready_capture):
    forged = deepcopy(ready_capture)
    forged["sources"]["EUR_USD"]["tail_base64"] = base64.b64encode(b"different source\n").decode()
    with pytest.raises(ValueError, match="captured_source_hash_mismatch"):
        inputs.validate_capture(sealed(forged))


def test_capture_hash_rejects_undeclared_mutation(ready_capture):
    forged = deepcopy(ready_capture)
    forged["current_common_bars"] += 1
    with pytest.raises(ValueError, match="capture_hash_mismatch"):
        inputs.validate_capture(forged)


def test_candle_close_maturity_is_checked_at_each_actual_source_observation(tmp_path):
    observed = archives(tmp_path)
    immature = observed - 3  # The last candle closes two seconds before observed.
    capture = inputs.capture_inputs(tmp_path, clock=lambda: immature)
    assert capture["status"] == "abstain"
    assert any("bar_not_complete_at_observation" in reason for reason in capture["reasons"])


def test_exact_close_maturity_is_accepted_without_backdating_availability(tmp_path):
    observed = archives(tmp_path) - 2
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert capture["max_bar_close_epoch"] == capture["features_available_epoch"] == observed
    assert all(source["first_observed_epoch"] == observed for source in capture["sources"].values())


def test_stale_reference_is_not_labeled_ready(tmp_path):
    observed = archives(tmp_path) + 900
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "abstain"
    assert any("stale_or_future_common_reference" in reason for reason in capture["reasons"])


def test_capture_clock_rollback_is_explicit(tmp_path):
    observed = archives(tmp_path)
    clocks = iter([observed + 20, observed + 19] + [observed + 30] * 8)
    capture = inputs.capture_inputs(tmp_path, clock=lambda: next(clocks))
    assert capture["status"] == "abstain"
    assert any("observation_clock_moved_backwards" in reason for reason in capture["reasons"])


def test_resealed_source_observation_after_final_capture_is_rejected(ready_capture):
    forged = deepcopy(ready_capture)
    forged["sources"]["EUR_USD"]["first_observed_epoch"] = forged["first_observed_epoch"] + 1
    with pytest.raises(ValueError, match="source_observed_after_capture"):
        inputs.validate_capture(sealed(forged))


def test_resealed_source_observation_order_rollback_is_rejected(ready_capture):
    forged = deepcopy(ready_capture)
    # Every close is mature and every source precedes aggregate capture, but
    # this sequence could not pass the capture-time monotonic-clock check.
    forged["sources"]["EUR_USD"]["first_observed_epoch"] -= .1
    forged["sources"]["GBP_USD"]["first_observed_epoch"] -= .2
    with pytest.raises(ValueError, match="observation_clock"):
        inputs.validate_capture(sealed(forged))


@pytest.mark.parametrize("field,value", [("output_instrument", "USD_JPY"),
    ("availability_semantics", "known_at_historical_candle_time"),
    ("research_only", False), ("account_eligible", True), ("can_place_orders", True)])
def test_resealed_fixed_identity_and_availability_claims_are_rejected(ready_capture, field, value):
    forged = deepcopy(ready_capture)
    forged[field] = value
    with pytest.raises(ValueError):
        inputs.validate_capture(sealed(forged))


def test_cached_module_cannot_replace_sha_bound_numerical_code(monkeypatch):
    fake = types.SimpleNamespace(predict_all=lambda *args: {"forged": "cached code"})
    monkeypatch.setitem(sys.modules, inputs.NUMERICAL_MODULE, fake)
    loaded = inputs._numerical_module()
    assert loaded is not fake
    assert getattr(loaded, "MODEL_VERSION", "").startswith("timestamp_gap_aware_four_family_models")


def test_computation_cannot_backdate_before_source_capture(ready_capture):
    result = inputs.compute_predictions(ready_capture, clock=lambda: ready_capture["first_observed_epoch"] - 1)
    assert result["status"] == "abstain" and result["predictions"] == {}
    assert any("computation_clock_moved_backwards" in reason for reason in result["reasons"])


def test_source_capture_is_bounded_to_last_1024_real_rows_per_pair(tmp_path):
    observed = archives(tmp_path, old_n=1100)
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert capture["retained_real_rows_by_pair"] == {pair: 1024 for pair in inputs.PAIRS}
    assert all(source["tail_byte_length"] <= 1024 * 1024 for source in capture["sources"].values())


@pytest.mark.parametrize("fraction", ["000000001", "000000000000000001", "000001", "1"])
def test_nonzero_fractional_start_never_rounds_down_into_an_exact_minute(tmp_path, fraction):
    observed = archives(tmp_path)
    path = tmp_path / "EUR_USD_M1.csv"
    raw = path.read_text()
    raw = raw.replace(FRIDAY.isoformat(), FRIDAY.isoformat().replace("+00:00", "." + fraction + "+00:00"), 1)
    path.write_text(raw, newline="")
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "abstain"
    assert any("fractional_bar_start_not_minute_aligned" in reason for reason in capture["reasons"])


def test_zero_nanoseconds_and_equivalent_nonzero_timezone_offset_are_supported(tmp_path):
    observed = archives(tmp_path)
    path = tmp_path / "EUR_USD_M1.csv"
    raw = path.read_text()
    raw = raw.replace(FRIDAY.isoformat(), "2026-09-04T12:00:00.000000000+02:00", 1)
    path.write_text(raw, newline="")
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert capture["series"]["EUR_USD"][0][0] == FRIDAY.timestamp()


def test_all_timestamp_columns_are_checked_before_shared_parser_precision_loss(ready_capture):
    forged = deepcopy(ready_capture)
    source = forged["sources"]["EUR_USD"]
    old_header = base64.b64decode(source["header_base64"]).decode()
    old_tail = base64.b64decode(source["tail_base64"]).decode()
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    parsed = csv.reader(old_tail.splitlines())
    for index, row in enumerate(parsed):
        extra_time = row[0].replace("+00:00", ".000000001+00:00") if index == 0 else row[0]
        writer.writerow(row + [extra_time])
    header = (old_header.strip() + ",time\n").encode()
    tail = stream.getvalue().encode()
    source.update(header_base64=base64.b64encode(header).decode(), tail_base64=base64.b64encode(tail).decode(),
        header_sha256=hashlib.sha256(header).hexdigest(), tail_sha256=hashlib.sha256(tail).hexdigest(),
        captured_bytes_sha256=hashlib.sha256(header + tail).hexdigest(), tail_byte_length=len(tail))
    with pytest.raises(ValueError, match="fractional_bar_start_not_minute_aligned"):
        inputs.validate_capture(sealed(forged))
