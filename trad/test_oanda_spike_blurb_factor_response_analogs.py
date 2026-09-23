import numpy as np

from oanda_spike_blurb_factor_response_analogs import (
    analog_key,
    attach_prequential_mapping,
    measure_currency_response,
)
from oanda_spike_blurb_response_entry_replay import PairSeries


def _pair(instrument, mids):
    base, quote = instrument.split("_")
    mid = np.asarray(mids, dtype=float)
    epochs = np.arange(1_800_000_000, 1_800_000_000 + len(mid)*60, 60, dtype=np.int64)
    return PairSeries(instrument,base,quote,.0001,epochs,mid,mid,mid-.00005,mid,mid,mid+.00005,mid,"a"*64)


def test_currency_response_orients_base_and_quote_legs():
    market = {
        "EUR_USD": _pair("EUR_USD", [1.0,1.01]),
        "EUR_GBP": _pair("EUR_GBP", [1.0,1.02]),
        "USD_EUR": _pair("USD_EUR", [1.0,.99]),
    }
    response = measure_currency_response(market,"EUR",1_800_000_000,1_800_000_060)
    assert response is not None
    assert response["currency_response_sign"] == 1
    assert response["response_breadth"] == 1.0


def _row(fid, baseline, target, factor, response):
    return {"factor_id":fid,"analog_key":"same","horizon_min":5,"baseline_epoch":baseline,
            "target_epoch":target,"signed_factor_score":factor,"currency_response_sign":response}


def test_prequential_mapping_uses_only_prior_matured_analogs():
    rows = [
        _row("a",100,200,1,1), _row("b",210,260,1,1), _row("c",270,320,1,1),
        _row("d",330,380,1,1), _row("overlap",340,500,1,-1),
    ]
    mapped = {row["factor_id"]:row for row in attach_prequential_mapping(rows)}
    assert mapped["d"]["prior_matured_analog_n"] == 3
    assert mapped["d"]["prequential_prediction_sign"] == 1
    assert mapped["d"]["prequential_correct"] == 1
    assert mapped["overlap"]["prior_matured_analog_n"] == 3


def test_analog_key_preserves_source_factor_and_relevance():
    assert analog_key({"source_id":"boj","factor_type":"policy_rate_change","relevance_state":"direct_action"}) == "boj|policy_rate_change|direct_action"
