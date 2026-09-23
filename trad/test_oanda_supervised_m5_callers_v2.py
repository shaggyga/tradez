import numpy as np
import pandas as pd
import pytest
import oanda_strategy_lab_historical_backtest as history
import oanda_practice_shadow_strategy_lab as lab
from test_oanda_supervised_m5_contract_v2 import m1_frame, candles


def s5_frame(rows=720):
    index = pd.date_range("2026-01-05T00:00:00Z", periods=rows, freq="5s")
    mid = 1.1 + np.arange(rows) * .000001
    frame = pd.DataFrame({"dt": index, "volume": 10, "complete": True})
    for side, delta in (("mid", 0), ("bid", -.00005), ("ask", .00005)):
        for field, bound in (("open", 0), ("high", .0001), ("low", -.0001), ("close", 0)):
            frame[side + "_" + field] = mid + delta + bound
    return frame


def load_synthetic(tmp_path, monkeypatch, frame):
    (tmp_path / "EUR_USD_S5.parquet").touch()
    monkeypatch.setattr(pd, "read_parquet", lambda path: frame.copy())
    return history.load_pair_history_s5(tmp_path, "EUR_USD")


def test_full_s5_grid_remains_complete_and_bound_to_source_counts(tmp_path, monkeypatch):
    result = load_synthetic(tmp_path, monkeypatch, s5_frame())
    assert len(result.m1_times) == 60 and len(result.m5_times) == 12
    quality = result.frame.attrs["input_quality"]["source_s5_quality"]
    assert quality["required_exact_s5_members_per_m1"] == 12
    assert quality["partial_m1_buckets_excluded"] == 0


@pytest.mark.parametrize("reason", ["missing", "incomplete", "nan_quote"])
def test_one_unusable_s5_member_excludes_its_m1_and_m5(tmp_path, monkeypatch, reason):
    frame = s5_frame()
    if reason == "missing":
        frame = frame.drop(24)
    elif reason == "incomplete":
        frame.loc[24, "complete"] = False
    else:
        frame.loc[24, "bid_close"] = np.nan
    result = load_synthetic(tmp_path, monkeypatch, frame)
    assert len(result.m1_times) == 59
    assert len(result.m5_times) == 11
    assert pd.Timestamp("2026-01-05T00:02:00Z") not in result.m1_times
    assert pd.Timestamp("2026-01-05T00:00:00Z") not in result.m5_times


def test_csv_loader_preserves_explicit_completion_column(tmp_path):
    frame = m1_frame(60)
    frame["complete"] = True
    frame.loc[frame.index[2], "complete"] = False
    frame.rename_axis("datetime").to_csv(tmp_path / "EUR_USD_M1.csv")
    result = history.load_pair_history(tmp_path, "EUR_USD")
    assert len(result.m1_times) == 59 and len(result.m5_times) == 11
    assert result.frame.attrs["input_quality"]["explicit_incomplete_m1_rows_excluded"] == 1


def test_cached_rejections_keep_stale_pairs_withheld_and_clear_old_fields(monkeypatch):
    lab.SUPERVISED_MODEL_CACHE.clear()
    monkeypatch.setattr(lab, "_fit_ridge", lambda x, y: None)
    monkeypatch.setattr(lab, "_predict_ridge", lambda model, x: np.zeros((len(x), 2)))
    features = {f"PAIR_{index}": {"pip": .0001} for index in range(20)}
    data = {key: {"M5": candles()} for key in features}
    try:
        lab.augment_supervised_return_features(features, data)
        data["PAIR_0"]["M5"] = data["PAIR_0"]["M5"][:-1]
        lab.augment_supervised_return_features(features, data)
        expected = dict(features["PAIR_0"])
        lab.augment_supervised_return_features(features, data)
        assert features["PAIR_0"] == expected
        assert not features["PAIR_0"]["supervised_ready"]
        assert not features["PAIR_0"]["supervised_model_decision_time"]
        assert features["PAIR_0"]["supervised_withheld_reason"] == "stale_m5_context_for_shared_fit"
        assert sum(item["supervised_ready"] for item in features.values()) == 19
    finally:
        lab.SUPERVISED_MODEL_CACHE.clear()
