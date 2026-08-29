import oanda_direct_source_simple_rules as rules


def config():
    return {"series":{"retail_sales":{"polarity":1,"meaning":"higher supports"}},"excluded_series":{},"preferred_horizons_sec":[300],"maximum_entry_spread_pips":3}


def test_rule_uses_change_proxy_and_can_abstain():
    assert rules.source_rule({"event_series_id":"retail_sales","actual_value":2,"previous_value":1,"consensus_value":None},config())["direction"]=="strengthen"
    assert rules.source_rule({"event_series_id":"unknown","actual_value":2,"previous_value":1},config())["direction"]=="abstain"


def test_conflicted_umich_release_is_explicitly_excluded_from_simple_direction():
    configured = rules.read_json(rules.CONFIG)
    result = rules.source_rule(
        {
            "event_series_id": "umich_consumer_sentiment_preliminary",
            "actual_value": 51.0,
            "previous_value": 55.2,
            "consensus_value": None,
        },
        configured,
    )
    assert result["direction"] == "abstain"
    assert "opposing USD/rates channels" in result["reason"]


def test_lowest_spread_pair_is_only_independent_decision():
    base={"release_key":"r","event_series_id":"retail_sales","event_name":"Retail","source_time_utc":"x","horizon_sec":300,
          "actual_value":0,"previous_value":1,"consensus_value":None,"currency_return_pips":-3,
          "strengthening_after_cost_pips":-4,"weakening_after_cost_pips":2}
    replay={"details":[{**base,"instrument":"USD_JPY","entry_spread_pips":2},{**base,"instrument":"EUR_USD","entry_spread_pips":1,"weakening_after_cost_pips":3}]}
    result=rules.evaluate(replay,config())
    assert len(result["evaluated"])==1
    assert result["evaluated"][0]["instrument"]=="EUR_USD"
    assert result["evaluated"][0]["rule_after_cost_pips"]==3


def test_rule_uses_revised_previous_before_original_previous():
    event={"event_series_id":"retail_sales","actual_value":1.0,
           "previous_value":0.5,"revised_previous_value":1.5}
    result=rules.source_rule(event,config())
    assert result["direction"]=="weaken"
    assert result["raw_delta"]==-0.5


def test_same_currency_clock_bundle_is_one_decision_and_conflicts_abstain():
    base={"currency":"USD","source_time_utc":"2026-08-06T14:00:00Z","horizon_sec":300,
          "event_name":"Release","instrument":"EUR_USD","entry_spread_pips":1,
          "currency_return_pips":3,"absolute_move_pips":3,"movement_cleared_cost":True,
          "best_after_cost_pips":2,"strengthening_after_cost_pips":2,
          "weakening_after_cost_pips":-4,"consensus_value":None}
    replay={"details":[
        {**base,"release_key":"a","event_series_id":"retail_sales","actual_value":2,"previous_value":1},
        {**base,"release_key":"b","event_series_id":"retail_sales","actual_value":0,"previous_value":1},
    ]}
    result=rules.evaluate(replay,config())
    assert len(result["magnitude_observations"])==1
    assert len(result["evaluated"])==0
    assert result["abstentions"][0]["reason"]=="same_clock_rule_conflict"
