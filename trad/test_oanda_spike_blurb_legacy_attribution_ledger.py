from oanda_spike_blurb_legacy_attribution_ledger import causal_use_state, case_row, expected_side, truth


def test_legacy_causal_states_are_not_collapsed():
    assert causal_use_state("PRE_MOVE")=="candidate_pre_entry_legacy_unverified"
    assert causal_use_state("FIRST_WAVE_CONFIRMATION")=="early_move_confirmation_not_entry"
    assert causal_use_state("POST_HOC_EXPLANATION")=="ex_post_attribution_only"
    assert causal_use_state("STALE_CONTEXT")=="stale_context_only"


def test_boolean_and_side_parsing_is_explicit():
    assert truth("True")==1 and truth("False")==0
    assert expected_side("LONG")=="long" and expected_side("UNKNOWN") is None


def test_case_direction_alignment_remains_descriptive():
    tag={"move_id":"m","instrument":"EUR_USD","start_utc":"s","end_utc":"e","news_match_status":"matched","news_tag_count":"1",
         "primary_causal_relation":"PRE_MOVE","primary_expected_pair_direction":"LONG","primary_predictive_eligible":"True","event_links_json":"[]"}
    price={"coverage_state":"exact_window","candle_count":90,"best_direction":"long","best_net_pips":5.0,"average_spread_pips":1.0}
    row=case_row(tag,price)
    assert row["expected_side_matches_restored"]==1
    assert row["forecast_proof_eligible"]==0 and row["execution_eligible"]==0
