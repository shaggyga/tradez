from __future__ import annotations

from pathlib import Path

import pytest

import oanda_spike_blurb_rbnz_response_confirmation_v2 as module


def candle(epoch: int, bid: float, ask: float) -> dict[str, float | int]:
    return {
        "epoch": epoch,
        "bid_h": bid + 0.0001,
        "bid_l": bid - 0.0001,
        "bid_c": bid,
        "ask_h": ask + 0.0001,
        "ask_l": ask - 0.0001,
        "ask_c": ask,
    }


def test_fixed_horizon_net_survives_one_missing_interior_bar() -> None:
    start = 1_700_000_000
    detection = {
        "detection_epoch": start,
        "selected_instrument": "NZD_USD",
        "selected_pair_direction": "long",
        "entry_bid": 1.0000,
        "entry_ask": 1.0002,
    }
    candles = {
        start + minute * 60: candle(start + minute * 60, 1.0001 + minute * 0.0001, 1.0003 + minute * 0.0001)
        for minute in range(module.v1.HOLD_MINUTES)
        if minute != 7
    }
    result = module.fixed_horizon_outcome(detection, candles)
    assert result["path_expected_bars"] == 15
    assert result["path_observed_bars"] == 14
    assert result["path_complete"] == 0
    assert result["path_quality"] == "partial_interior_bars_mfe_mae_partial"
    assert result["net_after_cost_pips"] > 0


def test_declared_exit_candle_remains_mandatory() -> None:
    start = 1_700_000_000
    detection = {
        "detection_epoch": start,
        "selected_instrument": "NZD_USD",
        "selected_pair_direction": "short",
        "entry_bid": 1.0000,
        "entry_ask": 1.0002,
    }
    candles = {
        start + minute * 60: candle(start + minute * 60, 1.0000, 1.0002)
        for minute in range(module.v1.HOLD_MINUTES - 1)
    }
    with pytest.raises(RuntimeError, match="declared_exit_candle_missing"):
        module.fixed_horizon_outcome(detection, candles)


def test_v2_changes_only_outcome_gap_policy_and_remains_inert() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "candidate_identity_preserved" in source
    assert "interior_bar_gaps_do_not_change_trade_selection_or_net_return" in source
    assert "/orders" not in source
    assert "/trades" not in source
    assert "api-fxtrade.oanda.com" not in source
    assert '"supported_execution_decision": "no_trade"' in source
