"""Independent evaluator regressions using actual imported modules."""
import json
import math
import numpy as np
import pandas as pd
import pytest
import oanda_profit_factor_contract_v2 as pf
import oanda_model_gap_market_validation as market


def frame(values):
    size = len(values)
    return pd.DataFrame({"exposure": np.ones(size), "realized_net_pips": values,
        "market_mid_move_pips": np.ones(size), "predicted_direction": np.ones(size)})


@pytest.mark.parametrize("values,status,factor", [([2., 3.], "no_losses", None),
    ([2., -4.], "finite", .5), ([-2.], "finite", 0.), ([0.], "no_gross_returns", 0.),
    ([], "no_gross_returns", 0.)])
def test_actual_metric_handles_zero_denominators_without_nonfinite_json(values, status, factor):
    metric = market.execution_metrics(frame(values))
    assert metric["profit_factor_status"] == status
    assert metric["profit_factor"] == factor
    assert json.loads(json.dumps(metric, allow_nan=False)) == metric
    assert metric["sum_net_pips"] == pytest.approx(metric["gross_win_pips"] - metric["gross_loss_pips"])


def test_pooled_factor_uses_pips_not_weighted_cell_ratios():
    metric = pf.pooled_profit_factor([pf.factor_fields(10, 1), pf.factor_fields(1, 5)])
    assert metric["profit_factor"] == pytest.approx(11 / 6)
    assert metric["profit_factor"] != pytest.approx(5.1)


@pytest.mark.parametrize("status,value", [("finite", 2), ("no_losses", None),
    ("no_gross_returns", 0), ("unavailable_missing_cell_components", 2), ("anything", 2)])
def test_current_contract_cannot_fall_back_to_legacy_without_components(status, value):
    row = {"profit_factor_contract": pf.CONTRACT, "profit_factor_status": status, "profit_factor": value}
    with pytest.raises(ValueError, match="missing_profit_factor_components"):
        pf.profit_factor_at_least(row, 1.05)


def test_unavailable_pooled_factor_status_survives_repeated_consumers():
    metric = pf.pooled_profit_factor([{"profit_factor": 2.0}])
    assert pf.normalize_profit_factor(pf.normalize_profit_factor(metric)) == metric
    assert not pf.profit_factor_at_least(metric, 1.05)


def test_legacy_finite_factor_is_preserved_but_never_reconstructed():
    metric = pf.normalize_profit_factor({"profit_factor": 2.})
    assert metric["profit_factor_status"] == "legacy_finite"
    assert "gross_win_pips" not in metric
    assert pf.profit_factor_at_least(metric, 1.05)


def test_nan_cannot_become_no_losses():
    with pytest.raises(ValueError, match="nonfinite_execution_metric_input"):
        market.execution_metrics(frame([math.nan]))


def test_inconsistent_sufficient_statistics_are_rejected():
    metric = pf.factor_fields(10., 5.)
    metric["profit_factor"] = 3.
    with pytest.raises(ValueError, match="profit_factor_value_mismatch"):
        pf.profit_factor_at_least(metric, 1.05)
