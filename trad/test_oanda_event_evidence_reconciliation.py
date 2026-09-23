import oanda_event_evidence_reconciliation as audit


def test_reconcile_keeps_oracle_directional_and_no_trade_separate():
    rows = audit.reconcile(
        {
            "matched_control_comparison": [
                {
                    "horizon_sec": 300,
                    "event_n": 3,
                    "event_mean_oracle_best_after_cost_pips": 2.0,
                    "event_cost_clear_fraction": 2 / 3,
                    "control_n": 3,
                    "control_mean_oracle_best_after_cost_pips": 0.5,
                    "control_cost_clear_fraction": 1 / 3,
                }
            ]
        },
        {
            "by_horizon": {
                "300": {
                    "direction_rule": {"n": 2, "average_net_pips": -1.0, "win_rate": 0.5},
                    "flipped_negative_control": {"n": 2, "average_net_pips": -0.5, "win_rate": 0.5},
                }
            }
        },
        {},
        {},
        {"totals": {"matured": 4, "average_predicted_side_net_pips": -2.0}},
        {"cohort_id": "watch", "matured_entries": 0},
    )
    by_arm = {item["arm"]: item for item in rows}

    assert by_arm["official_event_magnitude_oracle"]["average_net_pips"] == 2.0
    assert "not a forecast" in by_arm["official_event_magnitude_oracle"]["note"]
    assert by_arm["source_native_direction_rule"]["average_net_pips"] == -1.0
    assert by_arm["no_trade"]["average_net_pips"] == 0.0
    assert all(item["execution_eligible"] is False for item in rows)
