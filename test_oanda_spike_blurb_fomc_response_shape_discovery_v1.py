from __future__ import annotations

import ast
from pathlib import Path

import oanda_spike_blurb_fomc_response_shape_discovery_v1 as module


def test_grid_is_exact_and_bounded_by_acquired_path() -> None:
    assert len(module.ARMS) == 3
    assert module.TRADE_MODES == ("follow", "fade")
    assert module.HOLDS == (3, 5, 10, 15)
    assert len(module.ARMS) * len(module.TRADE_MODES) * len(module.HOLDS) == 24
    for _, arm in module.ARMS:
        assert int(arm["end_min"]) + max(module.HOLDS) - 1 <= 19


def test_holm_adjust_is_monotone_and_order_preserving() -> None:
    raw = [0.04, 0.001, 0.02, 0.5]
    adjusted = module.holm_adjust(raw)
    assert adjusted[1] <= adjusted[2] <= adjusted[0] <= adjusted[3]
    assert all(value >= source for value, source in zip(adjusted, raw))
    assert adjusted[1] == 0.004


def test_flip_detection_changes_only_trade_side() -> None:
    original = {"selected_pair_direction": "long", "selected_instrument": "EUR_USD", "entry_bid": 1.0}
    changed = module.flip_detection(original)
    assert changed["selected_pair_direction"] == "short"
    assert changed["selected_instrument"] == original["selected_instrument"]
    assert original["selected_pair_direction"] == "long"


def test_executable_horizon_math_charges_spread_and_slippage() -> None:
    detection = {
        "detection_epoch": 100, "selected_instrument": "EUR_USD",
        "selected_pair_direction": "long", "entry_bid": 1.0000, "entry_ask": 1.0002,
    }
    candles = {
        100: {"bid_c": 1.0000, "ask_c": 1.0002, "bid_h": 1.0003, "bid_l": 0.9999, "ask_h": 1.0005, "ask_l": 1.0001},
        160: {"bid_c": 1.0004, "ask_c": 1.0006, "bid_h": 1.0005, "bid_l": 1.0000, "ask_h": 1.0007, "ask_l": 1.0002},
        220: {"bid_c": 1.0007, "ask_c": 1.0009, "bid_h": 1.0008, "bid_l": 1.0003, "ask_h": 1.0010, "ask_l": 1.0005},
    }
    result = module.horizon_outcome(detection, candles, 3)
    assert round(result["net_after_cost_pips"], 8) == 4.75
    assert result["cost_cleared"] == 1


def test_module_is_research_only() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        alias.name for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom)) for alias in node.names
    }
    assert "requests" not in imports
    assert "subprocess" not in imports
    assert "api-fxtrade.oanda.com" not in source
    assert '"POST"' not in source
    assert "authorization_id" not in source
