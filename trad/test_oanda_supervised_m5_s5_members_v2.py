import numpy as np
import pandas as pd
import pytest
import oanda_strategy_lab_historical_backtest as history


@pytest.mark.parametrize('mutation', ['nonfinite_mid_close','crossed_bid_ask_close'])
def test_invalid_interior_s5_quote_member_does_not_become_complete_m5(tmp_path, monkeypatch, mutation):
    index = pd.date_range('2026-01-05T00:00:00Z', periods=120, freq='5s')
    mid = 1.1 + np.arange(120) * .000001
    frame = pd.DataFrame({'dt':index,'volume':10.})
    for side, delta in [('mid',0),('bid',-.00005),('ask',.00005)]:
        frame[side+'_open'] = mid+delta
        frame[side+'_close'] = mid+delta
        frame[side+'_high'] = mid+delta+.0001
        frame[side+'_low'] = mid+delta-.0001
    # Interior row is neither minute-open nor minute-close: aggregation can hide it.
    if mutation == 'nonfinite_mid_close':
        frame.loc[2,'mid_close'] = np.inf
    else:
        frame.loc[2,'bid_close'] = frame.loc[2,'ask_close']+.00001
    (tmp_path/'EUR_USD_S5.parquet').touch()
    monkeypatch.setattr(pd,'read_parquet',lambda path:frame)
    try:
        result = history.load_pair_history_s5(tmp_path,'EUR_USD')
    except ValueError:
        return
    assert pd.Timestamp('2026-01-05T00:00:00Z') not in result.m5_times, 'Invalid S5 quote was counted as a complete member'
