from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

import gold_m1_liquidity_scalper as gold


def synthetic_bullish_frame() -> pd.DataFrame:
    start = datetime(2026, 1, 5, 13, 0, tzinfo=timezone.utc)
    rows = []
    for idx in range(70):
        ts = start + timedelta(minutes=idx)
        base = 101.4 + idx * 0.002
        rows.append(
            {
                "time_utc": ts,
                "open": base,
                "high": 103.5 if idx == 10 else base + 0.20,
                "low": base - 0.20,
                "close": base + 0.05,
                "volume": 1.0,
                "spread_points": 0.0,
            }
        )

    # Liquidity sweep below the prior range, then close back inside.
    rows[40].update({"open": 101.20, "high": 101.40, "low": 100.00, "close": 101.35})
    # Middle candle.
    rows[41].update({"open": 101.10, "high": 101.30, "low": 101.00, "close": 101.20})
    # Displacement candle that creates bullish FVG: high[40] < low[42].
    rows[42].update({"open": 101.20, "high": 102.30, "low": 101.60, "close": 102.20})
    # Retrace fills entry.
    rows[43].update({"open": 102.00, "high": 102.10, "low": 101.45, "close": 101.80})
    # Continue to target.
    rows[44].update({"open": 101.80, "high": 102.50, "low": 101.70, "close": 102.30})
    rows[45].update({"open": 102.30, "high": 103.80, "low": 102.20, "close": 103.60})

    frame = pd.DataFrame(rows).set_index("time_utc")
    return frame


def test_calculate_contracts_rejects_too_small_account() -> None:
    cfg = gold.StrategyConfig(
        risk_pct=0.5,
        point_value=10.0,
        commission_round_turn=0.0,
        slippage_points_round_turn=0.0,
    )
    signal = gold.Signal(
        index=0,
        time_utc="2026-01-01T00:00:00+00:00",
        direction="LONG",
        htf_bias="BULLISH",
        sweep_time_utc="2026-01-01T00:00:00+00:00",
        sweep_level=1998.0,
        displacement_close=2001.0,
        fvg_low=2000.0,
        fvg_high=2001.0,
        entry=2000.0,
        stop=1998.0,
        target=2004.0,
        risk_points=2.0,
        reward_points=4.0,
        rr=2.0,
        expires_index=1,
        expires_utc="2026-01-01T00:01:00+00:00",
    )

    contracts, risk_dollars = gold.calculate_contracts(2_000.0, signal, cfg)

    assert contracts == 0
    assert risk_dollars == 0.0


def test_calculate_contracts_accepts_realistic_micro_gold_size() -> None:
    cfg = gold.StrategyConfig(
        risk_pct=0.5,
        point_value=10.0,
        commission_round_turn=0.0,
        slippage_points_round_turn=0.0,
    )
    signal = gold.Signal(
        index=0,
        time_utc="2026-01-01T00:00:00+00:00",
        direction="LONG",
        htf_bias="BULLISH",
        sweep_time_utc="2026-01-01T00:00:00+00:00",
        sweep_level=1995.0,
        displacement_close=2001.0,
        fvg_low=1999.0,
        fvg_high=2001.0,
        entry=2000.0,
        stop=1995.0,
        target=2010.0,
        risk_points=5.0,
        reward_points=10.0,
        rr=2.0,
        expires_index=1,
        expires_utc="2026-01-01T00:01:00+00:00",
    )

    contracts, risk_dollars = gold.calculate_contracts(10_000.0, signal, cfg)

    assert contracts == 1
    assert risk_dollars == 50.0


def test_generate_signals_detects_bullish_sweep_fvg() -> None:
    cfg = gold.StrategyConfig(
        require_htf_bias=False,
        displacement_atr_mult=0.25,
        fvg_min_points=0.10,
        min_rr=1.0,
        min_stop_points=0.50,
        max_stop_points=4.0,
        commission_round_turn=0.0,
        slippage_points_round_turn=0.0,
    )
    frame = gold.prepare_frame(synthetic_bullish_frame(), cfg)

    signals = gold.generate_signals(frame, cfg)

    assert signals
    signal = signals[0]
    assert signal.direction == "LONG"
    assert signal.entry == 101.5
    assert signal.stop == 99.8
    assert signal.rr >= 1.0


def test_backtest_fills_and_hits_target() -> None:
    cfg = gold.StrategyConfig(
        require_htf_bias=False,
        displacement_atr_mult=0.25,
        fvg_min_points=0.10,
        min_rr=1.0,
        min_stop_points=0.50,
        max_stop_points=4.0,
        risk_pct=0.5,
        point_value=10.0,
        commission_round_turn=0.0,
        slippage_points_round_turn=0.0,
    )

    summary, trades, signals = gold.backtest(synthetic_bullish_frame(), cfg, initial_equity=10_000.0)

    assert len(signals) >= 1
    assert len(trades) == 1
    assert trades[0].exit_reason == "TARGET"
    assert trades[0].pnl > 0
    assert summary["ending_equity"] > 10_000.0
