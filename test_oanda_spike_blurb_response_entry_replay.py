import numpy as np

from oanda_spike_blurb_response_entry_replay import (
    PairSeries,
    currency_snapshot,
    detect_arm,
    outcome_rows,
)


def _pair(instrument: str, bids: list[float], spread: float) -> PairSeries:
    base, quote = instrument.split("_")
    epochs = np.arange(1_800_000_000, 1_800_000_000 + len(bids) * 60, 60, dtype=np.int64)
    bid = np.array(bids, dtype=float)
    ask = bid + spread
    return PairSeries(
        instrument=instrument,
        base_currency=base,
        quote_currency=quote,
        pip=0.0001,
        epochs=epochs,
        bid_high=bid + 0.0001,
        bid_low=bid - 0.0001,
        bid_close=bid,
        ask_high=ask + 0.0001,
        ask_low=ask - 0.0001,
        ask_close=ask,
        mid=(bid + ask) / 2,
        price_file_sha256="a" * 64,
    )


def _market():
    rising = [1.0000 + index * 0.0002 for index in range(25)]
    return {
        "EUR_USD": _pair("EUR_USD", rising, 0.0001),
        "EUR_GBP": _pair("EUR_GBP", [0.8500 + index * 0.00015 for index in range(25)], 0.0002),
        "EUR_JPY": _pair("EUR_JPY", [1.2000 + index * 0.00018 for index in range(25)], 0.00015),
    }


def test_currency_snapshot_gets_direction_from_synchronized_legs_and_selects_lowest_cost():
    market = _market()
    baseline = 1_800_000_000
    snapshot = currency_snapshot(market, "EUR", baseline, baseline + 3 * 60)
    assert snapshot is not None
    assert snapshot["currency_direction"] == "stronger"
    assert snapshot["breadth"] == 1.0
    assert snapshot["selected_instrument"] == "EUR_USD"
    assert snapshot["selected_pair_direction"] == "long"


def test_detection_clock_is_after_source_clock_and_does_not_use_source_direction():
    market = _market()
    detection = detect_arm(
        market=market,
        currency="EUR",
        known_epoch=1_800_000_000,
        arm_id="test_arm",
        arm={
            "start_min": 1, "end_min": 5, "minimum_strength_bps": 1.0,
            "persistence_min": 1, "technical_confirmation": False,
        },
    )
    assert detection is not None
    assert detection["detection_epoch"] > 1_800_000_000
    assert detection["selected_pair_direction"] == "long"


def test_outcome_uses_entry_ask_and_exit_bid_for_long():
    market = _market()
    detection = detect_arm(
        market=market,
        currency="EUR",
        known_epoch=1_800_000_000,
        arm_id="test_arm",
        arm={
            "start_min": 1, "end_min": 5, "minimum_strength_bps": 1.0,
            "persistence_min": 1, "technical_confirmation": False,
        },
    )
    rows = outcome_rows(detection_id="detection_1", detection=detection, market=market)
    row = next(value for value in rows if value["horizon_min"] == 5)
    pair = market[detection["selected_instrument"]]
    exit_index = pair.exact_index(detection["detection_epoch"] + 5 * 60)
    expected = (pair.bid_close[exit_index] - detection["entry_ask"]) / pair.pip - 0.25
    assert abs(row["after_cost_pips"] - expected) < 1e-9
    assert row["outcome_selected"] == 0
    assert row["forecast_proof_eligible"] == 0
