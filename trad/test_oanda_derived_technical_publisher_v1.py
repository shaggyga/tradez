import json
from pathlib import Path

import oanda_derived_technical_publisher_v1 as publisher


def pairs68():
    return json.loads(publisher.DEFAULT_OBSERVATION_CONFIG.read_text(encoding="utf-8"))["pairs"]


def config():
    return {
        "schema": publisher.SCHEMA,
        "research_only": True,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "model_training_performed": False,
        "timeframe": "M5",
        "max_consecutive_fill": 3,
        "maximum_anchor_age_seconds": 900,
        "maximum_actual_lag_seconds": 900,
        "source_bindings": {
            "oanda_derived_technical_features_v1.py": publisher.sha(publisher.ROOT / "oanda_derived_technical_features_v1.py"),
            "oanda_feature_candle_inputs_v2.py": publisher.sha(publisher.ROOT / "oanda_feature_candle_inputs_v2.py"),
        },
        "pairs": pairs68(),
        "candle_root": str(publisher.ROOT / "data/oanda_training_manager/native_feature_candles_v1"),
        "output_path": str(publisher.DEFAULT_OUTPUT),
    }


def rows(pair, count=610, missing_tail=0):
    base = 1789344000
    seed = sum(ord(ch) for ch in pair)
    pip = pairs68()[pair]
    price = 1.0 + (seed % 200) / 1000
    out = []
    for index in range(count - missing_tail):
        drift = (index % 17 - 8) * pip * 0.03
        close = price + index * pip * 0.01 + drift
        opening = close - pip * 0.2
        high = max(opening, close) + pip * 0.5
        low = min(opening, close) - pip * 0.5
        out.append({
            "time": publisher.utc(base + index * 300),
            "complete": True,
            "volume": float(100 + index % 11),
            "mid": {"o": opening, "h": high, "l": low, "c": close},
            "bid": {"o": opening - pip * 0.1, "h": high - pip * 0.1, "l": low - pip * 0.1, "c": close - pip * 0.1},
            "ask": {"o": opening + pip * 0.1, "h": high + pip * 0.1, "l": low + pip * 0.1, "c": close + pip * 0.1},
        })
    return out


def all_histories(**overrides):
    return {pair: rows(pair, missing_tail=overrides.get(pair, 0)) for pair in pairs68()}


def test_default_config_is_bound_to_current_sources():
    loaded, reference = publisher.read_config()
    assert loaded["schema"] == publisher.SCHEMA
    assert len(loaded["pairs"]) == 68
    assert reference["sha256"] == loaded["observation_config_sha256"]


def test_all68_m5_derived_publish_has_shared_anchor_and_no_trading_authority():
    histories = all_histories()
    last_end = publisher.candles.stamp(histories["EUR_USD"][-1]["time"]).timestamp() + 300
    report = publisher.build_report(config(), histories, {}, {}, now=last_end + 30)
    assert report["schema_version"] == publisher.SCHEMA
    assert report["coverage"]["registered_pairs"] == 68
    assert report["coverage"]["histories_read"] == 68
    assert report["coverage"]["current_pairs"] == 68
    assert report["coverage"]["status_counts"] == {"observed_current": 68}
    assert report["coverage"]["minimal_feature_count"] == 9
    assert report["coverage"]["minimal_feature_finite_pairs"] == 68
    assert report["coverage"]["minimal_feature_current_pairs"] == 68
    assert report["coverage"]["movement_feature_count"] == 2
    assert report["coverage"]["movement_feature_current_pairs"] == 68
    assert report["analysis_anchor_epoch"] == last_end
    assert report["metadata"]["can_place_orders"] is False
    assert report["metadata"]["original_model_weight_compatibility"] is False
    eur = report["pairs"]["EUR_USD"]
    assert eur["feature_count"] == 216
    assert eur["finite_count"] >= 215
    assert eur["observed"] is True
    assert eur["imputed"] is False
    assert eur["minimal_feature_ready"] is True
    assert eur["movement_feature_ready"] is True
    assert eur["values"]["m5_derived__return_15_bps"] is not None


def test_short_missing_tail_is_published_as_derived_current_with_masks():
    histories = all_histories(EUR_USD=2)
    complete = rows("GBP_USD")
    last_end = publisher.candles.stamp(complete[-1]["time"]).timestamp() + 300
    report = publisher.build_report(config(), histories, {}, {}, now=last_end + 30)
    eur = report["pairs"]["EUR_USD"]
    assert eur["status"] == "derived_current"
    assert eur["observed"] is False
    assert eur["imputed"] is True
    assert eur["consecutive_imputed_bars"] == 2
    assert eur["trailing_unfilled_bars"] == 0
    assert eur["minimal_feature_ready"] is True
    assert eur["movement_feature_ready"] is True
    assert eur["quality_counts"]["partly_imputed_window"] > 0


def test_long_missing_tail_remains_an_explicit_gap():
    histories = all_histories(EUR_USD=6)
    complete = rows("GBP_USD")
    last_end = publisher.candles.stamp(complete[-1]["time"]).timestamp() + 300
    report = publisher.build_report(config(), histories, {}, {}, now=last_end + 30)
    eur = report["pairs"]["EUR_USD"]
    assert eur["status"] == "stale_gap"
    assert eur["trailing_unfilled_bars"] == 3
    assert eur["consecutive_imputed_bars"] == 3
    assert eur["minimal_feature_ready"] is True
    assert eur["movement_feature_ready"] is True
    assert eur["bar_end_epoch"] < report["analysis_anchor_epoch"]


def test_internal_short_gap_keeps_prior_history_and_marks_imputed_support():
    histories = all_histories()
    missing = rows("EUR_DKK")
    histories["EUR_DKK"] = missing[:-2] + missing[-1:]
    complete = rows("GBP_USD")
    last_end = publisher.candles.stamp(complete[-1]["time"]).timestamp() + 300
    report = publisher.build_report(config(), histories, {}, {}, now=last_end + 30)
    eur = report["pairs"]["EUR_DKK"]
    assert eur["status"] == "observed_current"
    assert eur["minimal_feature_ready"] is True
    assert eur["movement_feature_ready"] is True
    assert eur["values"]["m5_derived__return_1_pips"] is not None
    assert eur["quality_counts"]["partly_imputed_window"] > 0


def test_source_reader_retains_bounded_tail_across_internal_gap(tmp_path):
    pair = "EUR_DKK"
    sample = rows(pair, count=12)
    sample = sample[:9] + sample[10:]
    path = tmp_path / f"{pair}_M5.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("time,datetime,instrument,granularity,complete,open,high,low,close,bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close,volume\n")
        for row in sample:
            values = [row["time"], row["time"], pair, "M5", "true",
                      row["mid"]["o"], row["mid"]["h"], row["mid"]["l"], row["mid"]["c"],
                      row["bid"]["o"], row["bid"]["h"], row["bid"]["l"], row["bid"]["c"],
                      row["ask"]["o"], row["ask"]["h"], row["ask"]["l"], row["ask"]["c"], row["volume"]]
            handle.write(",".join(map(str, values)) + "\n")
    observed = publisher.utc(publisher.candles.stamp(sample[-1]["time"]).timestamp() + 330)
    read, receipt = publisher.read_tail_with_internal_gaps(path, pair, "M5", observed_utc=observed)
    assert len(read) == 11
    assert receipt["retained_rows"] == 11
    assert receipt["contiguous_suffix_rows"] == 2

