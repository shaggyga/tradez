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

import oanda_causal_forecast_inputs_pair_v1 as inputs


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
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: observed)
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
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert reads == ["EUR_USD_M1.csv"]
    assert capture["pairs"] == ["EUR_USD"]
    assert set(capture["sources"]) == set(capture["series"]) == {"EUR_USD"}
    assert capture["retained_real_rows_by_pair"] == {"EUR_USD": 573}
    assert capture["current_common_bars"] == capture["required_current_common_bars"] == 61


def test_bad_or_gapped_peer_archives_have_no_effect(tmp_path):
    observed = archive(tmp_path)
    baseline = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: observed)
    for pair in ("GBP_USD", "AUD_USD", "NZD_USD", "USD_JPY", "USD_CHF", "USD_CAD"):
        (tmp_path / f"{pair}_M1.csv").write_text("malformed,gapped,unavailable peer data")
    assert inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: observed) == baseline


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
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert capture["current_common_bars"] == 74
    rows = inputs._verified_rows(capture)
    assert len(rows) == 600 and not holes.intersection(rows)


def test_latest_eurusd_gap_still_abstains(tmp_path):
    observed = archive(tmp_path, missing={(SUNDAY + dt.timedelta(minutes=30)).timestamp()})
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: observed)
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
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert capture["retained_real_rows_by_pair"] == {"EUR_USD": 1024}
    inputs.validate_capture(capture)


@pytest.mark.parametrize("extra", [-3, -2, 898, 899])
def test_latest_bar_maturity_and_freshness_boundaries(tmp_path, extra):
    observed = archive(tmp_path) + extra
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: observed)
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
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: observed)
    assert capture["status"] == "abstain"
    assert any("fractional_bar_start_not_minute_aligned" in reason for reason in capture["reasons"])


def test_zero_fraction_and_equivalent_timezone_are_allowed(tmp_path):
    observed = archive(tmp_path)
    path = tmp_path / "EUR_USD_M1.csv"
    raw = path.read_text().replace(FRIDAY.isoformat(), "2026-09-04T12:00:00.000000000+02:00", 1)
    path.write_text(raw, newline="")
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    assert capture["series"]["EUR_USD"][0][0] == FRIDAY.timestamp()


@pytest.mark.parametrize("clocks", [(2, 1, 3), (0, 2, 1)])
def test_capture_clock_rollback_abstains(tmp_path, clocks):
    observed = archive(tmp_path)
    times = iter(observed + offset for offset in clocks)
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: next(times))
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
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001)
    assert capture["status"] == "abstain" and "source_unreadable" in capture["reasons"][0]
    def changed(path):
        raise ValueError("source_changed_during_read")
    monkeypatch.setattr(inputs, "_read_tail", changed)
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001)
    assert capture["status"] == "abstain" and "source_changed_during_read" in capture["reasons"][0]


def test_cached_module_cannot_replace_registered_source(monkeypatch):
    fake = types.SimpleNamespace(predict_all=lambda *args, **kwargs: {})
    monkeypatch.setitem(sys.modules, inputs.NUMERICAL_MODULE, fake)
    assert inputs._numerical_module() is not fake


def test_computation_cannot_backdate_before_capture(ready_capture):
    result = inputs.compute_predictions(ready_capture, clock=lambda: ready_capture["first_observed_epoch"] - 1)
    assert result["status"] == "abstain" and result["predictions"] == {}
    assert any("computation_clock_moved_backwards" in reason for reason in result["reasons"])


def test_one_unavailable_local_model_abstains_whole_batch(ready_capture, monkeypatch):
    fake = types.SimpleNamespace(predict_all=lambda *args, **kwargs: {"probabilistic_state_space": (1., .6, {})})
    monkeypatch.setattr(inputs, "_numerical_module", lambda: fake)
    result = inputs.compute_predictions(ready_capture, clock=lambda: ready_capture["first_observed_epoch"] + 1)
    assert result["status"] == "abstain" and result["predictions"] == {}
    assert any("family_prediction_unavailable:ridge_return_repaired" in reason for reason in result["reasons"])


@pytest.mark.parametrize("pair,pip", [("USD_JPY", .01), ("HKD_JPY", .0001), ("EUR_HUF", .01), ("USD_THB", .01)])
def test_non_eur_capture_binds_real_pair_and_explicit_pip(tmp_path, pair, pip):
    observed = archive(tmp_path)
    eur = tmp_path / "EUR_USD_M1.csv"
    (tmp_path / f"{pair}_M1.csv").write_text(eur.read_text().replace("EUR_USD", pair), newline="")
    capture = inputs.capture_inputs(tmp_path, pair, pip_size=pip, clock=lambda: observed)
    assert capture["status"] == "ready", capture["reasons"]
    inputs.validate_capture(capture, instrument=pair, pip_size=pip)
    assert capture["pairs"] == [pair] and capture["pip_size"] == pip
    result = inputs.compute_predictions(capture, clock=lambda: observed + 1)
    assert result["status"] == "ready", result["reasons"]
    assert result["output_instrument"] == pair and result["pip_size"] == pip


def test_resealed_pip_metadata_must_still_match_registered_contract(ready_capture):
    forged = deepcopy(ready_capture)
    forged["pip_size"] = .01
    with pytest.raises(ValueError, match="registered_pip_size"):
        inputs.validate_capture(reseal(forged), instrument="EUR_USD", pip_size=.0001)
    with pytest.raises(ValueError, match="registered_instrument"):
        inputs.validate_capture(ready_capture, instrument="USD_JPY", pip_size=.0001)


def test_coverage_probe_reports_each_own_pair_gap_and_training_sufficiency(tmp_path):
    observed = archive(tmp_path)
    eur = tmp_path / "EUR_USD_M1.csv"
    raw = eur.read_text().replace("EUR_USD", "USD_JPY")
    remove = (SUNDAY + dt.timedelta(minutes=30)).isoformat()
    raw = "\n".join(line for line in raw.splitlines() if not line.startswith(remove)) + "\n"
    (tmp_path / "USD_JPY_M1.csv").write_text(raw, newline="")
    report = inputs.coverage_probe(tmp_path, ["EUR_USD", "USD_JPY", "AUD_USD"],
        pip_sizes={"EUR_USD": .0001, "USD_JPY": .01, "AUD_USD": .0001}, clock=lambda: observed)
    assert report["ready_pair_count"] == 1 and report["pair_count"] == 3
    by_pair = {row["instrument"]: row for row in report["rows"]}
    assert by_pair["EUR_USD"]["current_pair_bars"] == 61
    assert by_pair["EUR_USD"]["mature_ridge_training_rows"] == 131
    assert by_pair["USD_JPY"]["current_pair_bars"] == 30
    assert "current_common_warmup:30<61" in by_pair["USD_JPY"]["reasons"][0]
    assert "source_unreadable" in by_pair["AUD_USD"]["reasons"][0]
    assert report["can_place_orders"] is report["can_promote"] is False


def test_coverage_probe_does_not_fit_or_create_files(tmp_path, monkeypatch):
    observed = archive(tmp_path, old_n=0)
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    def forbidden():
        raise AssertionError("coverage probe must not load numerical source or fit")
    monkeypatch.setattr(inputs, "_numerical_module", forbidden)
    report = inputs.coverage_probe(tmp_path, ["EUR_USD"], pip_sizes={"EUR_USD": .0001}, clock=lambda: observed)
    assert report["ready_pair_count"] == 0
    assert report["rows"][0]["mature_ridge_training_rows"] == 0
    assert report["rows"][0]["reasons"] == ["mature_ridge_training_rows:0<24"]
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


def test_coverage_probe_requires_metadata_without_guessing_currency_suffix(tmp_path):
    with pytest.raises(ValueError, match="pip_size"):
        inputs.coverage_probe(tmp_path, ["HKD_JPY"], pip_sizes={})
    with pytest.raises(ValueError, match="duplicate_pair"):
        inputs.coverage_probe(tmp_path, ["EUR_USD", "EUR_USD"], pip_sizes={"EUR_USD": .0001})


def test_coverage_probe_rejects_69_pairs_before_any_source_read(tmp_path, monkeypatch):
    pairs = [f"ZZZ_A{chr(65 + i // 26)}{chr(65 + i % 26)}" for i in range(69)]
    def forbidden(path):
        raise AssertionError("input universe must be bounded before reading")
    monkeypatch.setattr(inputs, "_read_tail", forbidden)
    with pytest.raises(ValueError, match="maximum_68_pairs"):
        inputs.coverage_probe(tmp_path, pairs, pip_sizes=dict.fromkeys(pairs, .0001))


@pytest.mark.parametrize("edit,reason", [
    ("duplicate", "duplicate_or_unordered_bar"),
    ("wrong_pair", "wrong_bar_instrument"),
    ("naive_time", "bar_timezone_required"),
    ("partial", "empty_or_partial_csv_tail"),
])
def test_invalid_archive_identity_order_or_completion_fails_closed(tmp_path, edit, reason):
    observed = archive(tmp_path)
    path = tmp_path / "EUR_USD_M1.csv"
    raw = path.read_text()
    lines = raw.splitlines(keepends=True)
    if edit == "duplicate": raw = "".join(lines[:2] + [lines[1]] + lines[2:])
    elif edit == "wrong_pair": raw = raw.replace("EUR_USD", "USD_JPY", 1)
    elif edit == "naive_time": raw = raw.replace(FRIDAY.isoformat(), FRIDAY.replace(tzinfo=None).isoformat(), 1)
    else: raw = raw.rstrip("\n")
    path.write_text(raw, newline="")
    capture = inputs.capture_inputs(tmp_path, "EUR_USD", pip_size=.0001, clock=lambda: observed)
    assert capture["status"] == "abstain"
    assert reason in capture["reasons"][0]


def test_source_freeze_hash_mismatch_blocks_computation(ready_capture, monkeypatch):
    monkeypatch.setattr(inputs, "NUMERICAL_SOURCE_SHA256", "0" * 64)
    with pytest.raises(ValueError, match="numerical_source_binding_changed"):
        inputs._numerical_module()
    result = inputs.compute_predictions(ready_capture)
    assert result["status"] == "abstain" and result["predictions"] == {}
