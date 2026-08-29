import pandas as pd

import oanda_executable_opportunity_direction_audit as audit


def row(**changes):
    values = {
        "return_1m_pips": 1.0,
        "return_5m_pips": 2.0,
        "return_15m_pips": 3.0,
        "return_30m_pips": 4.0,
        "currency_factor_5m_pips": 1.5,
        "pair_residual_5m_pips": -0.5,
        "imbalance_30s": 0.2,
        "imbalance_120s": -0.1,
    }
    values.update(changes)
    return pd.Series(values)


def test_fixed_policy_signs_are_distinct():
    sample = row()
    assert audit.POLICIES["continuation_5m"](sample) == 1
    assert audit.POLICIES["reversion_5m"](sample) == -1
    assert audit.POLICIES["pair_residual_continuation"](sample) == -1
    assert audit.POLICIES["quote_imbalance_30s"](sample) == 1


def test_top_one_is_ranked_by_clearance_then_cost_ratio():
    frame = pd.DataFrame(
        [
            {"instrument": "EUR_USD", "predicted_clear_probability": 0.7, "predicted_magnitude_cost_ratio": 4.0},
            {"instrument": "USD_JPY", "predicted_clear_probability": 0.8, "predicted_magnitude_cost_ratio": 2.0},
        ]
    )
    assert audit.selected_rows(frame, 1).iloc[0]["instrument"] == "USD_JPY"


def test_evaluation_charges_cost_and_counts_direction():
    frame = pd.DataFrame(
        [
            {
                "epoch": 60,
                "instrument": "EUR_USD",
                "predicted_clear_probability": 0.8,
                "predicted_magnitude_cost_ratio": 2.0,
                "future_move_pips": 5.0,
                "actual_cost_pips": 1.5,
                "return_5m_pips": 1.0,
            }
        ]
    )
    result = audit.evaluate_policy(frame, lambda row: audit.sign(row["return_5m_pips"]), 1)
    assert result["n"] == 1
    assert result["direction_accuracy"] == 1.0
    assert result["average_net_pips"] == 3.5


def test_three_leg_selection_requires_currency_disjoint_pairs():
    frame = pd.DataFrame(
        [
            {"instrument": "EUR_USD", "predicted_clear_probability": 0.9, "predicted_magnitude_cost_ratio": 3.0},
            {"instrument": "GBP_USD", "predicted_clear_probability": 0.8, "predicted_magnitude_cost_ratio": 3.0},
            {"instrument": "AUD_JPY", "predicted_clear_probability": 0.7, "predicted_magnitude_cost_ratio": 3.0},
            {"instrument": "CAD_CHF", "predicted_clear_probability": 0.6, "predicted_magnitude_cost_ratio": 3.0},
        ]
    )
    selected = audit.selected_rows(frame, 3)
    assert set(selected["instrument"]) == {"EUR_USD", "AUD_JPY", "CAD_CHF"}
