import pandas as pd

import oanda_executable_opportunity_feature_contract as contract
import oanda_executable_opportunity_ranking as ranking


def synthetic() -> dict:
    rows = {}
    for epoch in range(0, 2701, 60):
        rows[epoch] = {
            "mid": 1.0 + epoch / 10_000_000,
            "spread": 1.0,
            "updates": 10.0 + epoch / 60,
            "imbalance_5s": 0.1,
            "imbalance_30s": 0.2,
            "imbalance_120s": 0.3,
        }
    return {"EUR_USD": rows, "GBP_USD": {key: {**value, "mid": value["mid"] + 0.2} for key, value in rows.items()}}


def test_feature_contract_reproduces_frozen_training_features() -> None:
    data = synthetic()
    expected = ranking.build_frame(data, 300, 0.25, 5.0)
    actual = contract.labeled_frame(data, 300, 0.25, 5.0, aligned_only=True)
    pd.testing.assert_frame_equal(
        expected[ranking.FEATURES].reset_index(drop=True),
        actual[ranking.FEATURES].reset_index(drop=True),
        check_dtype=False,
    )


def test_unaligned_contract_contains_more_rows() -> None:
    data = synthetic()
    aligned = contract.labeled_frame(data, 300, 0.25, 5.0, aligned_only=True)
    unaligned = contract.labeled_frame(data, 300, 0.25, 5.0, aligned_only=False)
    assert len(unaligned) > len(aligned)
