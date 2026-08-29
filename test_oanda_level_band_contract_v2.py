from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from oanda_level_band_contract_v2 import (
    BandVersion,
    MarketBar,
    PivotAnchor,
    aggregate_complete_m1_to_m5,
    build_pivot_band_versions,
    confirmed_pivot_anchors,
    frozen_approach_descriptors,
    physical_approach_side,
    pip_size,
)


BASE = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)


def bar(
    offset: int,
    close: float,
    *,
    minutes: int = 1,
    high: float | None = None,
    low: float | None = None,
    spread: float = 0.0002,
) -> MarketBar:
    high = close + 0.0003 if high is None else high
    low = close - 0.0003 if low is None else low
    half = spread / 2.0
    return MarketBar(
        timestamp=BASE + timedelta(minutes=offset * minutes),
        minutes=minutes,
        mid_open=close,
        mid_high=high,
        mid_low=low,
        mid_close=close,
        bid_open=close - half,
        bid_high=high - half,
        bid_low=low - half,
        bid_close=close - half,
        ask_open=close + half,
        ask_high=high + half,
        ask_low=low + half,
        ask_close=close + half,
    )


def anchor(index: int, price: float, tolerance: float = 1.0) -> PivotAnchor:
    return PivotAnchor(
        anchor_id=f"a{index}",
        origin_kind="pivot_high",
        price=price,
        pivot_index=index,
        confirmed_index=index + 2,
        pivot_utc=(BASE + timedelta(minutes=index * 5)).isoformat(),
        known_at_utc=(BASE + timedelta(minutes=(index + 3) * 5)).isoformat(),
        anchor_atr=2.0,
        anchor_spread=0.1,
        tolerance=tolerance,
        padding=0.2,
    )


def band(lower: float = 1.1010, upper: float = 1.1014) -> BandVersion:
    return BandVersion(
        band_id="band1",
        band_version_id="bandv1",
        source="confirmed_pivot_cluster",
        name="swing_cluster",
        origin_kind="pivot_high",
        lower=lower,
        center=(lower + upper) / 2.0,
        upper=upper,
        first_anchor_utc=BASE.isoformat(),
        last_anchor_utc=BASE.isoformat(),
        known_at_utc=BASE.isoformat(),
        anchor_count=2,
        distinct_source_count=1,
        member_anchor_ids=("a", "b"),
    )


def test_native_midpoint_extrema_are_not_reconstructed_from_sides() -> None:
    rows = [bar(index, 1.1000 + index * 0.0001) for index in range(5)]
    special = rows[2]
    rows[2] = MarketBar(
        **{
            **special.__dict__,
            "mid_high": 1.1050,
            "bid_high": 1.1030,
            "ask_high": 1.1040,
        }
    )
    result = aggregate_complete_m1_to_m5(rows)
    assert len(result) == 1
    assert result[0].mid_high == pytest.approx(1.1050)
    assert (result[0].bid_high + result[0].ask_high) / 2.0 != pytest.approx(1.1050)


def test_right_hand_pivot_is_excluded_until_confirmation_bar_is_complete() -> None:
    values = [1.0, 1.1, 1.2, 1.3, 2.0, 1.4, 1.35, 1.3, 1.2, 1.1]
    rows = [
        bar(index, value, minutes=5, high=value + 0.01, low=value - 0.01, spread=0.002)
        for index, value in enumerate(values)
    ]
    assert not any(row.pivot_index == 4 for row in confirmed_pivot_anchors("USD_JPY", rows, 5))
    assert any(row.pivot_index == 4 for row in confirmed_pivot_anchors("USD_JPY", rows, 6))


def test_incremental_cluster_cannot_chain_drift_away_from_first_anchor() -> None:
    versions = build_pivot_band_versions([
        anchor(0, 100.0), anchor(1, 100.9), anchor(2, 101.8)
    ])
    assert len(versions) == 2
    assert sorted(row.anchor_count for row in versions) == [1, 2]
    first = next(row for row in versions if row.anchor_count == 2)
    assert first.lower == pytest.approx(99.8)
    assert first.upper == pytest.approx(101.1)


def test_new_anchor_creates_new_version_without_changing_stable_band_id() -> None:
    first = build_pivot_band_versions([anchor(0, 100.0)])[0]
    second = build_pivot_band_versions([anchor(0, 100.0), anchor(1, 100.5)])[0]
    assert first.band_id == second.band_id
    assert first.band_version_id != second.band_version_id


def test_audited_pip_map_handles_nonstandard_huf_and_hkd_jpy() -> None:
    assert pip_size("USD_HUF") == pytest.approx(0.01)
    assert pip_size("USD_JPY") == pytest.approx(0.01)
    assert pip_size("HKD_JPY") == pytest.approx(0.0001)
    assert pip_size("EUR_USD") == pytest.approx(0.0001)


def test_physical_role_flips_without_relabeling_band_identity() -> None:
    level = band()
    assert physical_approach_side(1.1008, 1.1007, level) == 1
    assert physical_approach_side(1.1016, 1.1017, level) == -1
    assert level.band_id == "band1"


def test_descriptors_are_transparent_and_not_probabilities() -> None:
    m1 = [bar(index, 1.0980 + index * 0.0001) for index in range(13)]
    m5 = [
        bar(index, 1.0950 + index * 0.00015, minutes=5, spread=0.0002)
        for index in range(30)
    ]
    quote_mid = 1.1000
    result = frozen_approach_descriptors(
        "EUR_USD", m1, m5, band(), 1,
        quote_mid, quote_mid - 0.0001, quote_mid + 0.0001,
        BASE + timedelta(minutes=31 * 5),
    )
    assert result["approach_velocity_3_pips_per_min"] > 0
    assert result["frozen_break_price"] > result["band_upper"]
    assert result["frozen_reject_price"] < result["band_lower"]
    assert result["response_probability_state"] == "absent_no_frozen_model"
    assert result["empirical_cost_clearance_state"].startswith("unknown")
    assert result["selected_side"] is None
    assert result["research_only"] is True
    assert result["execution_eligible"] is False


def test_descriptors_prefer_exact_venue_pip_metadata() -> None:
    m1 = [bar(index, 312.0 + index * 0.01, spread=0.04) for index in range(13)]
    m5 = [
        bar(index, 311.0 + index * 0.02, minutes=5, spread=0.04)
        for index in range(30)
    ]
    level = band(313.0, 313.2)
    result = frozen_approach_descriptors(
        "USD_HUF", m1, m5, level, 1,
        312.8, 312.78, 312.82, BASE + timedelta(minutes=31 * 5),
        venue_pip=0.01,
    )
    assert result["pip"] == pytest.approx(0.01)
    assert result["spread_pips"] == pytest.approx(4.0)


def test_noncontiguous_recent_m1_window_fails_closed() -> None:
    m1 = [bar(index, 1.0980 + index * 0.0001) for index in range(13)]
    broken = list(m1)
    broken[-1] = MarketBar(**{
        **broken[-1].__dict__,
        "timestamp": broken[-1].timestamp + timedelta(minutes=1),
    })
    m5 = [bar(index, 1.0950 + index * 0.0001, minutes=5) for index in range(30)]
    with pytest.raises(ValueError, match="noncontiguous_descriptor_history"):
        frozen_approach_descriptors(
            "EUR_USD", broken, m5, band(), 1,
            1.1000, 1.0999, 1.1001, BASE + timedelta(hours=3),
        )
