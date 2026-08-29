import oanda_opportunity_cost_concentration_audit as audit


def test_cost_bucket_boundaries():
    assert audit.cost_bucket(1.5) == "cost_le_1_5"
    assert audit.cost_bucket(2.0) == "cost_1_5_to_2_5"
    assert audit.cost_bucket(4.0) == "cost_2_5_to_5"
    assert audit.cost_bucket(6.0) == "cost_gt_5"


def test_summary_separates_gross_direction_from_cost():
    summary = audit.summarize(
        [
            {"net": 1.0, "cost": 2.0, "gross_signed": 3.0, "direction_correct": 1, "movement_cleared_cost": 1},
            {"net": -3.0, "cost": 2.0, "gross_signed": -1.0, "direction_correct": 0, "movement_cleared_cost": 1},
        ]
    )

    assert summary["average_gross_signed_pips"] == 1.0
    assert summary["average_modeled_cost_pips"] == 2.0
    assert summary["average_net_pips"] == -1.0
    assert summary["average_net_spread_stress_50pct"] == -2.0
    assert summary["win_rate_after_cost"] == 0.5
