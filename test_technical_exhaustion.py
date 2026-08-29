from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

from oanda_technical_account_manager_auto import (
    ForexManager,
    technical_exhaustion_features_from_rows,
    technical_exhaustion_rule_matches,
)


def test_exhaustion_features_use_closed_five_minute_bars() -> None:
    start = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
    rows = []
    price = 1.1000
    for minute in range(300):
        open_price = price
        price += 0.0001
        rows.append(
            {
                "time": (start + dt.timedelta(minutes=minute)).isoformat(),
                "complete": True,
                "mid_o": open_price,
                "mid_h": price + 0.00002,
                "mid_l": open_price - 0.00002,
                "mid_c": price,
            }
        )

    features = technical_exhaustion_features_from_rows(rows, 0.0001)

    assert features is not None
    assert features["momentum_5_atr"] > 0.8
    assert features["momentum_15_atr"] > 1.35
    assert features["momentum_30_atr"] > 1.88
    assert features["atr240_pips"] > 0
    assert "momentum_60_atr" in features
    assert "acceleration_15_atr" in features
    assert "compression_30" in features
    assert "range_position_240_centered" in features
    assert "spread_ratio_60" in features


def test_exhaustion_rules_are_upper_tail_and_short_only_candidates() -> None:
    features = {
        "momentum_5_atr": 0.9,
        "momentum_15_atr": 1.5,
        "momentum_30_atr": 2.0,
    }

    rules = technical_exhaustion_rule_matches(features, 0.0006)

    assert set(rules) == {
        "m30_strength15_upper",
        "m15_strength15_upper",
        "m5_strength15_upper",
        "m15_m30_upper",
        "m5_m30_upper",
    }
    assert technical_exhaustion_rule_matches(
        {
            "momentum_5_atr": -0.9,
            "momentum_15_atr": -1.5,
            "momentum_30_atr": -2.0,
        },
        -0.0006,
    ) == []


def test_shadow_monitor_excludes_usd_and_never_builds_order_actions() -> None:
    manager = ForexManager.__new__(ForexManager)
    strong = {
        "momentum_5_atr": 0.9,
        "momentum_15_atr": 1.5,
        "momentum_30_atr": 2.0,
        "pair_log_return_15": 0.001,
        "atr240_pips": 8.0,
        "start_utc": "2026-06-01T11:30:00+00:00",
        "end_utc": "2026-06-01T12:00:00+00:00",
    }

    signals = manager.technical_exhaustion_shadow_signals(
        {
            "EUR_GBP": {"instrument": "EUR_GBP", **strong},
            "EUR_USD": {"instrument": "EUR_USD", **strong},
        }
    )

    assert [signal["instrument"] for signal in signals] == ["EUR_GBP"]
    assert signals[0]["direction"] == "SHORT"
    assert signals[0]["status"] == "shadow_only"
    assert "action" not in signals[0]
    assert "risk_pct" not in signals[0]


def test_technical_new_entries_require_active_production_manifest() -> None:
    manager = ForexManager.__new__(ForexManager)
    manager.cfg = SimpleNamespace(account_lane="tech")
    manager._promoted_model_manifest = {}
    manager._promoted_model_bundle = {}
    manager.sync_promoted_model_manifest = lambda: {}

    allowed, reason = manager.production_model_allows_new_entries()

    assert not allowed
    assert "technical_production.json" in reason
