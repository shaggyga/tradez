"""Temporary archives only; no broker, database or worker operations."""
import base64
from copy import deepcopy
import csv
import datetime as dt
import hashlib
import math
import sys
import types

import pytest

import oanda_causal_forecast_inputs_eurusd_v1 as inputs


FRIDAY = dt.datetime(2026, 9, 4, 10, tzinfo=dt.timezone.utc)
SUNDAY = dt.datetime(2026, 9, 6, 21, tzinfo=dt.timezone.utc)


def archive(root, *, old_n=512, new_n=61, missing=()):
    path = root / "EUR_USD_M1.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["datetime", "instrument", "granularity", "close"])
        for start, count in ((FRIDAY, old_n), (SUNDAY, new_n)):
            for minute in range(count):
                stamp = start + dt.timedelta(minutes=minute)
                if stamp.timestamp() in missing:
                    continue
                price = 1.1 + .0001 * (5 * math.sin(minute / 8.) + .01 * minute)
                writer.writerow([stamp.isoformat(), "EUR_USD", "M1", repr(price)])
    return (SUNDAY + dt.timedelta(minutes=new_n, seconds=2)).timestamp()


def reseal(capture):
    capture.pop("source_capture_sha256", None)
    return inputs._seal(capture)


def replace_source_bytes(source, header, tail):
    source.update(header_base64=base64.b64encode(header).decode(), tail_base64=base64.b64encode(tail).decode(),
        header_sha256=hashlib.sha256(header).hexdigest(), tail_sha256=hashlib.sha256(tail).hexdigest(),
        captured_bytes_sha256=hashlib.sha256(header + tail).hexdigest(),
        tail_byte_offset=len(header), tail_byte_length=len(tail), file_size_bytes=len(header) + len(tail))


@pytest.fixture
def ready_capture(tmp_path):
    observed = archive(tmp_path)
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    return capture


def test_only_eurusd_file_is_read_when_all_peer_archives_are_missing(tmp_path, monkeypatch):
    observed = archive(tmp_path)
    actual = inputs._read_tail
    reads = []
    def guarded_read(path):
        assert path.name == "EUR_USD_M1.csv"
        reads.append(path.name)
        return actual(path)
    monkeypatch.setattr(inputs, "_read_tail", guarded_read)
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert reads == ["EUR_USD_M1.csv"]
    assert capture["pairs"] == ["EUR_USD"]
    assert set(capture["sources"]) == set(capture["series"]) == {"EUR_USD"}
    assert capture["retained_real_rows_by_pair"] == {"EUR_USD": 573}
    assert capture["current_common_bars"] == capture["required_current_common_bars"] == 61


def test_bad_or_gapped_peer_archives_have_no_effect(tmp_path):
    observed = archive(tmp_path)
    baseline = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    for pair in ("GBP_USD", "AUD_USD", "NZD_USD", "USD_JPY", "USD_CHF", "USD_CAD"):
        (tmp_path / f"{pair}_M1.csv").write_text("malformed,gapped,unavailable peer data")
    assert inputs.capture_inputs(tmp_path, clock=lambda: observed) == baseline


def test_real_capture_computes_two_local_families_from_friday_and_61_current_closes(ready_capture):
    capture = ready_capture
    inputs.validate_capture(capture)
    completed = capture["first_observed_epoch"] + 1
    result = inputs.compute_predictions(capture, clock=lambda: completed)
    assert result["status"] == "ready", result["reasons"]
    assert set(result["predictions"]) == {"ridge_return_repaired", "probabilistic_state_space"}
    assert result["computed_epoch"] == completed
    assert result["source_capture_sha256"] == capture["source_capture_sha256"]
    assert capture["max_bar_close_epoch"] < capture["first_observed_epoch"] < completed
    for row in result["predictions"].values():
        assert math.isfinite(row["expected_signed_pips"]) and 0 <= row["probability_up"] <= 1
        maturity = row["diagnostics"]["training_label_maturity_max_epoch"]
        assert maturity is None or maturity < SUNDAY.timestamp()
        assert row["diagnostics"]["feature_cutoff_epoch"] <= capture["max_bar_close_epoch"]


def test_earlier_eurusd_gap_preserves_valid_segments_and_current_window(tmp_path):
    holes = {(FRIDAY + dt.timedelta(minutes=170)).timestamp(), (SUNDAY + dt.timedelta(minutes=15)).timestamp()}
    observed = archive(tmp_path, new_n=90, missing=holes)
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert capture["current_common_bars"] == 74
    rows = inputs._verified_rows(capture)
    assert len(rows) == 600 and not holes.intersection(rows)


def test_latest_eurusd_gap_still_abstains(tmp_path):
    observed = archive(tmp_path, missing={(SUNDAY + dt.timedelta(minutes=30)).timestamp()})
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "abstain"
    assert any("current_common_warmup:30<61" in reason for reason in capture["reasons"])


@pytest.mark.parametrize("field", ["series", "current_common_start_epochs", "current_common_bars",
    "required_current_common_bars", "retained_real_rows_by_pair", "reference_start_epoch",
    "max_bar_close_epoch", "common_bar_age_sec", "training_label_maturity_max_epoch",
    "training_labels_available_max_epoch", "feature_cutoff_epoch", "features_available_epoch"])
def test_derived_field_tampering_still_fails_after_resealing(ready_capture, field):
    forged = deepcopy(ready_capture)
    if field == "series": forged[field]["EUR_USD"][0][1] += .01
    elif field == "current_common_start_epochs": forged[field][0] -= 60
    elif field == "retained_real_rows_by_pair": forged[field]["EUR_USD"] -= 1
    else: forged[field] += 1
    with pytest.raises(ValueError, match="derived_input_binding_mismatch"):
        inputs.validate_capture(reseal(forged))


@pytest.mark.parametrize("field,value", [("output_instrument", "USD_JPY"), ("pairs", ["EUR_USD", "NZD_USD"]),
    ("availability_semantics", "historically_available"), ("training_policy", "compress_gaps"),
    ("research_only", False), ("account_eligible", True), ("can_place_orders", True), ("reasons", ["bad"])])
def test_fixed_identity_and_availability_are_validated(ready_capture, field, value):
    forged = deepcopy(ready_capture)
    forged[field] = value
    with pytest.raises(ValueError):
        inputs.validate_capture(reseal(forged))


def test_capture_hash_and_source_hashes_are_both_enforced(ready_capture):
    forged = deepcopy(ready_capture)
    forged["series"]["EUR_USD"][0][1] += .01
    with pytest.raises(ValueError, match="input_capture_hash_mismatch"):
        inputs.validate_capture(forged)
    forged = deepcopy(ready_capture)
    source = forged["sources"]["EUR_USD"]
    tail = base64.b64decode(source["tail_base64"]).replace(b"1.1", b"2.2", 1)
    source["tail_base64"] = base64.b64encode(tail).decode()
    with pytest.raises(ValueError, match="captured_source_hash_mismatch"):
        inputs.validate_capture(reseal(forged))


@pytest.mark.parametrize("field", ["file_size_bytes", "tail_byte_offset", "tail_byte_length"])
def test_resealed_byte_geometry_lies_are_rejected(ready_capture, field):
    forged = deepcopy(ready_capture)
    forged["sources"]["EUR_USD"][field] += 1
    with pytest.raises(ValueError, match="invalid_captured_byte_geometry"):
        inputs.validate_capture(reseal(forged))


def test_real_capture_retains_at_most_1024_rows(tmp_path):
    observed = archive(tmp_path, old_n=1100)
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert capture["retained_real_rows_by_pair"] == {"EUR_USD": 1024}
    inputs.validate_capture(capture)


@pytest.mark.parametrize("extra", [-3, -2, 898, 899])
def test_latest_bar_maturity_and_freshness_boundaries(tmp_path, extra):
    observed = archive(tmp_path) + extra
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    expected_ready = extra in (-2, 898)
    assert (capture["status"] == "ready") is expected_ready, capture
    if expected_ready:
        assert capture["features_available_epoch"] == observed


@pytest.mark.parametrize("fraction", ["000000001", "00000000000001", "000001", "1"])
def test_nonzero_fractional_timestamp_cannot_round_to_a_minute(tmp_path, fraction):
    observed = archive(tmp_path)
    path = tmp_path / "EUR_USD_M1.csv"
    raw = path.read_text().replace(FRIDAY.isoformat(), FRIDAY.isoformat().replace("+00:00", "." + fraction + "+00:00"), 1)
    path.write_text(raw, newline="")
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "abstain"
    assert any("fractional_bar_start_not_minute_aligned" in reason for reason in capture["reasons"])


def test_zero_fraction_and_equivalent_timezone_are_allowed(tmp_path):
    observed = archive(tmp_path)
    path = tmp_path / "EUR_USD_M1.csv"
    raw = path.read_text().replace(FRIDAY.isoformat(), "2026-09-04T12:00:00.000000000+02:00", 1)
    path.write_text(raw, newline="")
    capture = inputs.capture_inputs(tmp_path, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert capture["series"]["EUR_USD"][0][0] == FRIDAY.timestamp()


@pytest.mark.parametrize("clocks", [(2, 1, 3), (0, 2, 1)])
def test_capture_clock_rollback_abstains(tmp_path, clocks):
    observed = archive(tmp_path)
    times = iter(observed + offset for offset in clocks)
    capture = inputs.capture_inputs(tmp_path, clock=lambda: next(times))
    assert capture["status"] == "abstain"
    assert any("observation_clock_moved_backwards" in reason for reason in capture["reasons"])


def test_source_observation_after_capture_and_immature_source_observation_rejected(ready_capture):
    forged = deepcopy(ready_capture)
    forged["sources"]["EUR_USD"]["first_observed_epoch"] = forged["first_observed_epoch"] + 1
    with pytest.raises(ValueError, match="source_observed_after_capture"):
        inputs.validate_capture(reseal(forged))
    forged = deepcopy(ready_capture)
    forged["sources"]["EUR_USD"]["first_observed_epoch"] = forged["max_bar_close_epoch"] - 1
    with pytest.raises(ValueError, match="bar_not_complete_at_observation"):
        inputs.validate_capture(reseal(forged))


def test_missing_or_changed_eurusd_source_is_explicit(tmp_path, monkeypatch):
    capture = inputs.capture_inputs(tmp_path)
    assert capture["status"] == "abstain" and "source_unreadable" in capture["reasons"][0]
    def changed(path):
        raise ValueError("source_changed_during_read")
    monkeypatch.setattr(inputs, "_read_tail", changed)
    capture = inputs.capture_inputs(tmp_path)
    assert capture["status"] == "abstain" and "source_changed_during_read" in capture["reasons"][0]


def test_cached_module_cannot_replace_registered_source(monkeypatch):
    fake = types.SimpleNamespace(predict_all=lambda *args: {})
    monkeypatch.setitem(sys.modules, inputs.NUMERICAL_MODULE, fake)
    assert inputs._numerical_module() is not fake


def test_computation_cannot_backdate_before_capture(ready_capture):
    result = inputs.compute_predictions(ready_capture, clock=lambda: ready_capture["first_observed_epoch"] - 1)
    assert result["status"] == "abstain" and result["predictions"] == {}
    assert any("computation_clock_moved_backwards" in reason for reason in result["reasons"])


def test_one_unavailable_local_model_abstains_whole_batch(ready_capture, monkeypatch):
    fake = types.SimpleNamespace(predict_all=lambda *args: {"probabilistic_state_space": (1., .6, {})})
    monkeypatch.setattr(inputs, "_numerical_module", lambda: fake)
    result = inputs.compute_predictions(ready_capture, clock=lambda: ready_capture["first_observed_epoch"] + 1)
    assert result["status"] == "abstain" and result["predictions"] == {}
    assert any("family_prediction_unavailable:ridge_return_repaired" in reason for reason in result["reasons"])
