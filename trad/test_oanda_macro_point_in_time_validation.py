import oanda_macro_point_in_time_validation as validation


def test_negative_point_estimate_fails_before_other_checks():
    assert validation.robustness_state(
        {
            "raw_n": 20,
            "average_net_pips": -1.0,
            "factor_lcb_normal_95_pips": 2.0,
            "discovery_bh_q_value": 0.01,
        }
    ) == "failed_negative_point_estimate"


def test_positive_candidate_still_requires_lower_bound_and_multiplicity():
    assert validation.robustness_state(
        {
            "raw_n": 20,
            "average_net_pips": 2.0,
            "factor_lcb_normal_95_pips": -1.0,
            "discovery_bh_q_value": 0.01,
        }
    ) == "failed_nonpositive_factor_lower_bound"
    assert validation.robustness_state(
        {
            "raw_n": 20,
            "average_net_pips": 2.0,
            "factor_lcb_normal_95_pips": 1.0,
            "discovery_bh_q_value": 0.2,
        }
    ) == "failed_multiplicity_control"
