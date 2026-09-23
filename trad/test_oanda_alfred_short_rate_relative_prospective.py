import json

import oanda_alfred_short_rate_relative_prospective as worker


def test_locked_short_rate_candidate_is_separate_and_fail_closed():
    config = json.loads(worker.CONFIG.read_text(encoding="utf-8"))
    assert config["change_direction_policy"] == "higher_is_stronger"
    assert config["horizon_sec"] == 86400
    assert config["maximum_spread_pips"] == 3.0
    assert config["minimum_absolute_change_differential_pct_points"] == 0.25
    assert config["contract"]["initial_current_view_excluded"] is True
    assert config["contract"]["cannot_promote_or_authorize"] is True
    assert config["execution_eligible"] is False
    assert worker.DATABASE.name == "alfred_short_rate_relative_prospective_v1.sqlite"
