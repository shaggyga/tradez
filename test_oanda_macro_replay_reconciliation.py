import json

import oanda_macro_replay_reconciliation as reconciliation


def payload(value, lcb, *, pair="EUR_USD"):
    return {
        "summaries": [
            {
                **reconciliation.FIXED,
                "rule": {
                    "raw_n": 10,
                    "factor_episode_n": 5,
                    "win_rate": 0.5,
                    "average_net_pips": value,
                    "factor_mean_pips": value,
                    "factor_lcb_normal_95_pips": lcb,
                    "discovery_bh_q_value": 0.9,
                },
            }
        ],
        "details": [
            {
                "pair": pair,
                "reference_date": "2026-01-01",
                "signal_rule": reconciliation.FIXED["signal_rule"],
                "horizon_hours": reconciliation.FIXED["horizon_hours"],
                "liquidity_bucket": reconciliation.FIXED["liquidity_bucket"],
                "signal_value": 0.3,
                "predicted_side": "long",
                "entry_utc": "2026-02-01T00:00:00Z",
                "rule_after_cost_pips": value,
            }
        ],
    }


def test_reconciliation_fails_when_point_in_time_sign_reverses(tmp_path):
    current = tmp_path / "current.json"
    point = tmp_path / "point.json"
    output = tmp_path / "output.json"
    report = tmp_path / "report.md"
    current.write_text(json.dumps(payload(10.0, 2.0)))
    point.write_text(json.dumps(payload(-1.0, -5.0)))
    result = reconciliation.run(current, point, output, report)
    assert result["raw_average_sign_reversed"] is True
    assert result["robustness_state"] == "failed_point_in_time_robustness"
    assert result["execution_eligible"] is False
    assert result["membership_decomposition"]["overlap_n"] == 1
    assert result["membership_decomposition"][
        "overlap_side_agreement_count"
    ] == 1
