import datetime as dt

from oanda_spike_blurb_legacy_price_reacquisition import (
    load_labels,
    normalize_candles,
    pip_size,
    summarize_path,
)


def _candle(epoch, bid_o, bid_h, bid_l, bid_c, ask_o, ask_h, ask_l, ask_c):
    mid = lambda a, b: f"{(a+b)/2:.5f}"
    return {
        "time": dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat(), "complete": True,
        "volume": 3,
        "bid": {"o":str(bid_o),"h":str(bid_h),"l":str(bid_l),"c":str(bid_c)},
        "ask": {"o":str(ask_o),"h":str(ask_h),"l":str(ask_l),"c":str(ask_c)},
        "mid": {"o":mid(bid_o,ask_o),"h":mid(bid_h,ask_h),"l":mid(bid_l,ask_l),"c":mid(bid_c,ask_c)},
    }


def test_normalization_deduplicates_and_sorts():
    payload = {"candles":[_candle(120,1,1.1,.9,1.05,1.01,1.11,.91,1.06), _candle(60,1,1.1,.9,1.05,1.01,1.11,.91,1.06), _candle(120,1,1.1,.9,1.05,1.01,1.11,.91,1.06)]}
    rows = normalize_candles(payload)
    assert [row["epoch"] for row in rows] == [60,120]


def test_executable_path_math_includes_spread():
    rows = normalize_candles({"candles":[
        _candle(60,1.0000,1.0020,.9990,1.0010,1.0002,1.0022,.9992,1.0012),
        _candle(120,1.0010,1.0030,1.0000,1.0020,1.0012,1.0032,1.0002,1.0022),
    ]})
    result = summarize_path({"instrument":"EUR_USD","start_epoch":60,"end_epoch":180}, rows)
    assert result["coverage_state"] == "exact_window"
    assert round(result["long_net_pips"], 6) == 18.0
    assert round(result["short_net_pips"], 6) == -22.0
    assert result["best_direction"] == "long"
    assert round(result["long_mfe_pips"], 6) == 28.0
    assert round(result["short_mfe_pips"], 6) == 8.0


def test_empty_path_and_pip_conventions():
    assert summarize_path({"instrument":"EUR_USD"}, [])["coverage_state"] == "no_candles"
    assert pip_size("USD_JPY") == 0.01
    assert pip_size("EUR_USD") == 0.0001
