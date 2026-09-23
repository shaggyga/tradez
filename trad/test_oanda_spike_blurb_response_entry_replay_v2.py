import numpy as np

from oanda_spike_blurb_response_entry_replay import PairSeries
from oanda_spike_blurb_response_entry_replay_v2 import (
    currency_snapshot_v2,
    normalize_fresh_watch_rows,
)


def _watch(factor_id: str, currency: str, known: int, published: int, relevance: str):
    return {
        "factor_id": factor_id,
        "currency": currency,
        "known_utc": str(known),
        "published_utc": str(published),
        "relevance_state": relevance,
        "source_id": "official_source",
    }


def test_stale_publications_are_barred_and_same_currency_batches_collapse():
    base = 1_800_000_000
    fresh, exclusions = normalize_fresh_watch_rows(
        [
            _watch("stale", "JPY", base, base - 3600, "direct_action"),
            _watch("weaker", "JPY", base + 10, base, "official_context"),
            _watch("stronger", "JPY", base + 20, base + 5, "direct_action"),
            _watch("usd", "USD", base + 20, base + 5, "direct_action"),
        ]
    )
    assert {row["factor_id"] for row in fresh} == {"stronger", "usd"}
    assert exclusions["stale_at_first_observation"] == 1
    assert exclusions["same_currency_source_batch_duplicate"] == 1


def _pair(instrument: str, spread: float) -> PairSeries:
    base, quote = instrument.split("_")
    epochs = np.arange(1_800_000_000, 1_800_000_000 + 5 * 60, 60, dtype=np.int64)
    bid = np.array([1.0, 1.001, 1.002, 1.003, 1.004])
    ask = bid + spread
    return PairSeries(
        instrument=instrument, base_currency=base, quote_currency=quote, pip=0.0001,
        epochs=epochs, bid_high=bid, bid_low=bid, bid_close=bid,
        ask_high=ask, ask_low=ask, ask_close=ask, mid=(bid+ask)/2,
        price_file_sha256="a"*64,
    )


def test_pair_selection_refuses_all_confirming_legs_above_five_pips():
    market = {
        "EUR_USD": _pair("EUR_USD", 0.0006),
        "EUR_GBP": _pair("EUR_GBP", 0.0007),
        "EUR_JPY": _pair("EUR_JPY", 0.0008),
    }
    assert currency_snapshot_v2(market, "EUR", 1_800_000_000, 1_800_000_120) is None


def test_pair_selection_keeps_low_cost_confirming_leg():
    market = {
        "EUR_USD": _pair("EUR_USD", 0.0001),
        "EUR_GBP": _pair("EUR_GBP", 0.0007),
        "EUR_JPY": _pair("EUR_JPY", 0.0008),
    }
    snapshot = currency_snapshot_v2(market, "EUR", 1_800_000_000, 1_800_000_120)
    assert snapshot is not None
    assert snapshot["selected_instrument"] == "EUR_USD"
    assert snapshot["selected_spread_pips"] <= 5.0
