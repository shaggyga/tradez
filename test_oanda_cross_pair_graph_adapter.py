from __future__ import annotations

import numpy as np
import pandas as pd

import oanda_cross_pair_graph_adapter as graph


def _returns() -> pd.DataFrame:
    times = pd.date_range("2026-01-01", periods=40, freq="min", tz="UTC")
    rows = []
    for index, instrument in enumerate(("EUR_USD", "GBP_USD", "USD_JPY", "AUD_CAD")):
        values = np.sin(np.arange(40) / 4 + index * 0.2)
        rows.extend(
            {"timestamp_utc": time, "instrument": instrument, "return_pips": value}
            for time, value in zip(times, values)
        )
    return pd.DataFrame(rows)


def test_graph_is_strictly_lagged_and_future_invariant() -> None:
    frame = _returns()
    cutoff = pd.Timestamp("2026-01-01T00:30:00Z")
    first = graph.build_lagged_graph(frame, cutoff, min_observations=12)
    changed = frame.copy()
    changed.loc[changed["timestamp_utc"] >= cutoff, "return_pips"] = 99999.0
    second = graph.build_lagged_graph(changed, cutoff, min_observations=12)
    assert first == second
    assert pd.Timestamp(first.source_max_utc) < cutoff
    assert graph.adjacency_matrix(first).shape == (4, 4)


def test_currency_exposure_sign_is_directional() -> None:
    assert graph.currency_exposure("EUR_USD", "EUR") == 1
    assert graph.currency_exposure("EUR_USD", "USD") == -1
    assert graph.currency_exposure("EUR_USD", "JPY") == 0
