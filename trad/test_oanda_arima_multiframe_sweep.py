import pytest

from oanda_arima_multiframe_sweep import selected_sweeps


def test_selected_sweeps_supports_targeted_h1_h4_run() -> None:
    rows = selected_sweeps("H1,H4")

    assert [row["label"] for row in rows] == ["H1", "H4"]
    assert rows[0]["horizons"].endswith("720,1440")
    assert rows[1]["horizons"].endswith("720,1440")


def test_selected_sweeps_rejects_unknown_timeframe() -> None:
    with pytest.raises(ValueError, match="unsupported ARIMA"):
        selected_sweeps("H24")
