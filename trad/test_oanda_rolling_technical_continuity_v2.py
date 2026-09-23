"""Boundary, missingness and evidence tests independent of running collectors."""
from datetime import datetime, timezone
import copy
import hashlib
import json

import numpy as np
import pytest

import oanda_rolling_technical_continuity_v2 as continuity
import oanda_rolling_technical_features_v1 as kernel

BASE = 1700000040
PAIR = "EUR_USD"


def candles(count=650):
    x = np.arange(count, dtype=float)
    close = 1.1+.00001*np.sin(x/3)+.000002*x
    return dict(time=BASE+np.arange(count, dtype=np.int64)*60,
        open=close.copy(), high=close+.00005, low=close-.00005, close=close,
        bid_close=close-.00004, ask_close=close+.00006, volume=5+x%9)


def iso(at):
    return datetime.fromtimestamp(at, timezone.utc).isoformat()


def receipt(at, *, present=False):
    candle = dict(time=iso(at if present else at-60), complete=True, volume=3,
        mid=dict(o="1.1", h="1.2", l="1.0", c="1.1"),
        bid=dict(o="1.09", h="1.19", l="0.99", c="1.09"),
        ask=dict(o="1.11", h="1.21", l="1.01", c="1.11"))
    response = dict(_http_status=200, instrument=PAIR, granularity="M1", candles=[candle])
    return dict(schema_version="all68_m1_gap_recovery_v1", instrument=PAIR,
        granularity="M1", price="BAM", candle_epoch=at, count=10,
        requested_utc=iso(at+120), response_observed_utc=iso(at+121),
        end_time_utc=iso(at+60), response=response,
        response_sha256=hashlib.sha256(json.dumps(response, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode()).hexdigest())


def rehash(value):
    value["response_sha256"] = hashlib.sha256(json.dumps(value["response"],
        sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return value


def test_boundary_has_no_clock_rounding_forward():
    assert continuity.latest_complete_boundary(BASE+59.999) == BASE
    assert continuity.latest_complete_boundary(BASE+60) == BASE+60
    result = continuity.summarize_continuity([BASE], PAIR, BASE+60)
    assert result["latest_completed_boundary_present"] is True
    with pytest.raises(ValueError, match="completed_source_rows"):
        continuity.summarize_continuity([BASE], PAIR, BASE+59.999)


def test_elapsed_gaps_suffix_and_trailing_unobserved_are_distinct():
    result = continuity.summarize_continuity([BASE, BASE+60, BASE+180, BASE+240], PAIR, BASE+420)
    assert result["consecutive_suffix_rows"] == 2
    assert result["consecutive_suffix_start_epoch"] == BASE+180
    assert result["trailing_unobserved_minutes"] == 2
    assert [(r["location"], r["minutes"]) for r in result["missing_intervals"]] == [
        ("interior_gap", 1), ("trailing_unobserved", 2)]
    assert result["missing_minutes_by_kind"] == {"missing_unknown": 3}


def test_tail_scope_never_invents_leading_history_or_full_suffix():
    result = continuity.summarize_continuity([BASE, BASE+60], PAIR, BASE+120, range_truncated=True)
    assert result["missing_minutes_in_scope"] == 0
    assert result["suffix_is_lower_bound"] is True
    empty = continuity.summarize_continuity([], PAIR, BASE)
    assert empty["missing_minutes_in_scope"] == 0
    assert empty["trailing_unobserved_minutes"] is None
    assert empty["latest_completed_boundary_present"] is False


def test_huge_stale_span_is_bounded_and_reported_without_filling():
    result = continuity.summarize_continuity([BASE], PAIR, BASE+1000000*60,
                                            max_span_minutes=12)
    assert result["diagnostic_span_clipped"] is True
    assert result["missing_minutes_in_scope"] == 12
    assert result["trailing_unobserved_minutes"] == 999999
    assert len(result["missing_intervals"]) == 1
    assert result["synthetic_rows"] == 0


@pytest.mark.parametrize("times", [[BASE, BASE], [BASE+60, BASE], [BASE+.5], [True], [[BASE]], [float("nan")]])
def test_invalid_source_clocks_fail_closed(times):
    with pytest.raises(ValueError):
        continuity.summarize_continuity(times, PAIR, BASE+600)


def test_bounds_fail_before_unlimited_diagnostics():
    with pytest.raises(ValueError, match="bounded_source_rows"):
        continuity.summarize_continuity([BASE]*4097, PAIR, BASE+600)
    with pytest.raises(ValueError, match="bounded_span_minutes"):
        continuity.summarize_continuity([], PAIR, BASE, max_span_minutes=10081)
    with pytest.raises(ValueError, match="bounded_gap_evidence"):
        continuity.summarize_continuity([], PAIR, BASE, gap_evidence=[{}]*513)


def test_gap_support_is_distinct_from_short_initial_history():
    data = candles()
    data = {k: np.delete(v, 600) for k, v in data.items()}
    values = kernel.compute_features(data, PAIR, .0001)
    result = continuity.classify_feature_support(data, values, range_truncated=True)
    assert result["features"]["m1__return_5_pips"]["category"] == "available"
    ema = result["features"]["m1__finite_ema_gap_200_pips"]
    assert ema["category"] == "missing_elapsed_support"
    assert ema["missing_elapsed_minutes_in_observed_scope"] == 1
    short = {k: v[-10:] for k, v in data.items()}
    short_values = kernel.latest_features(short, PAIR, .0001)
    short_result = continuity.classify_feature_support(short, short_values, range_truncated=True)
    assert short_result["features"]["m1__finite_ema_gap_200_pips"]["category"] == "insufficient_bounded_history"


def test_undefined_statistics_do_not_become_source_gaps():
    data = candles()
    for key in ("open", "high", "low", "close", "bid_close", "ask_close"):
        data[key][:] = 1.1
    data["volume"][:] = 0
    result = continuity.classify_feature_support(data, kernel.latest_features(data, PAIR, .0001))
    assert result["features"]["m1__body_to_range"]["category"] == "undefined_or_numerical"
    assert result["features"]["m1__tick_activity_ratio_120"]["category"] == "undefined_or_numerical"
    assert result["features"]["m1__rsi_14"]["category"] == "available"
    assert result["counts"].get("missing_elapsed_support", 0) == 0


def test_absent_optional_source_is_not_undefined_statistic_or_price_gap():
    data = candles()
    data.pop("volume")
    result = continuity.classify_feature_support(data, kernel.latest_features(data, PAIR, .0001))
    assert result["features"]["m1__tick_activity"]["category"] == "source_input_unavailable"
    assert result["features"]["m1__return_1_pips"]["category"] == "available"
    assert result["features"]["m1__utc_hour_sin"]["category"] == "available"


def test_no_source_or_registry_mismatch_is_hidden():
    data = candles(0)
    result = continuity.classify_feature_support(data, kernel.latest_features(data, PAIR, .0001))
    assert result["counts"] == {"no_completed_observation": 216}
    with pytest.raises(ValueError, match="exact_registered_features"):
        continuity.classify_feature_support(data, {})


def test_successful_omission_and_real_retained_observation_are_separate():
    at = BASE+120
    omitted = continuity.gap_evidence_from_receipt(receipt(at), PAIR, at, at+200,
                                                  evidence_ref="omitted.json")
    actual = continuity.gap_evidence_from_receipt(receipt(at, present=True), PAIR, at, at+200,
                                                 evidence_ref="actual.json")
    assert omitted["kind"] == "broker_omitted_requested_minute"
    assert actual["kind"] == "archive_observation_available"
    assert len(actual["row_value_sha256"]) == 64
    result = continuity.summarize_continuity([at-60, at+60], PAIR, at+200,
                                             gap_evidence=[omitted])
    assert result["missing_intervals"][0]["kind"] == "broker_omitted_requested_minute"
    assert result["consecutive_suffix_rows"] == 1


@pytest.mark.parametrize("change", ["hash", "future", "wrong_pair", "http", "duplicate", "outside", "incomplete", "geometry"])
def test_bad_or_unavailable_gap_evidence_is_refused(change):
    at = BASE+120
    value = receipt(at, present=True)
    if change == "hash":
        value["response_sha256"] = "0"*64
    elif change == "future":
        value["response_observed_utc"] = iso(at+300)
    elif change == "wrong_pair":
        value["response"]["instrument"] = "GBP_USD"
    elif change == "http":
        value["response"]["_http_status"] = 429
    elif change == "duplicate":
        value["response"]["candles"] *= 2
    elif change == "outside":
        value["response"]["candles"][0]["time"] = iso(at+60)
    elif change == "incomplete":
        value["response"]["candles"][0]["complete"] = False
    elif change == "geometry":
        value["response"]["candles"][0]["mid"]["h"] = "0.9"
    if change != "hash":
        rehash(value)
    with pytest.raises(ValueError):
        continuity.gap_evidence_from_receipt(value, PAIR, at, at+200, evidence_ref="bad.json")


def test_closure_requires_explicit_evidence_and_never_bridges_support():
    at = BASE+120
    evidence = dict(pair=PAIR, start_epoch=at, end_epoch=at,
        observed_epoch=at+200, kind="market_closed", evidence_ref="calendar-proof.json",
        evidence_sha256="a"*64)
    result = continuity.summarize_continuity([at-60, at+60], PAIR, at+200, gap_evidence=[evidence])
    assert result["missing_intervals"][0]["kind"] == "market_closed"
    assert result["consecutive_suffix_rows"] == 1
    evidence["pair"] = "GBP_USD"
    with pytest.raises(ValueError, match="pair_bound"):
        continuity.summarize_continuity([at-60, at+60], PAIR, at+200, gap_evidence=[evidence])


def test_named_receipt_loader_bounds_reports_and_preserves_files(tmp_path):
    at = BASE+120
    directory = tmp_path/".gap_recovery_v1"/PAIR
    directory.mkdir(parents=True)
    path = directory/f"{at}.attempt1.observed.json"
    raw = json.dumps(receipt(at)).encode()
    path.write_bytes(raw)
    gaps = [dict(start_epoch=at-60, end_epoch=at)]
    result = continuity.load_gap_evidence(tmp_path, PAIR, gaps, at+200, max_minutes=1)
    assert result["missing_minutes_not_examined"] == 1
    assert result["selected_minutes"] == 1
    assert len(result["attempts"]) == 2
    assert result["evidence"][0]["evidence_sha256"] == hashlib.sha256(raw).hexdigest()
    assert path.read_bytes() == raw
    limited = continuity.load_gap_evidence(tmp_path, PAIR, gaps, at+200, max_total_bytes=1)
    assert limited["bytes_read"] == 0
    assert limited["evidence"] == []
    assert any(a["status"] == "receipt_exceeds_remaining_byte_bound" for a in limited["attempts"])


@pytest.mark.parametrize("payload", [[], {"schema_version": "wrong"}, None])
def test_malformed_receipt_is_explicitly_unusable(tmp_path, payload):
    at = BASE+120
    directory = tmp_path/".gap_recovery_v1"/PAIR
    directory.mkdir(parents=True)
    path = directory/f"{at}.attempt1.observed.json"
    path.write_text(json.dumps(payload))
    result = continuity.load_gap_evidence(tmp_path, PAIR,
        [dict(start_epoch=at, end_epoch=at)], at+200)
    assert result["evidence"] == []
    assert result["attempts"][0]["status"] == "invalid_or_unreadable"
    assert result["complete_within_requested_scope"] is False


def test_diagnostics_do_not_mutate_data_features_receipts():
    data = candles()
    features = kernel.latest_features(data, PAIR, .0001)
    before = {k: v.copy() for k, v in data.items()}
    before_features = dict(features)
    source_receipt = dict(instrument=PAIR, retained_rows=len(data["time"]), range_truncated=True)
    original_receipt = copy.deepcopy(source_receipt)
    result = continuity.diagnose_pair(data, features, PAIR, int(data["time"][-1])+61,
                                      source_receipt=source_receipt)
    assert result["feature_support"]["feature_count"] == 216
    assert result["continuity"]["latest_completed_boundary_present"] is True
    for key in data:
        np.testing.assert_array_equal(data[key], before[key])
    assert features == before_features
    assert source_receipt == original_receipt
