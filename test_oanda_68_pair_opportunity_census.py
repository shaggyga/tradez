import oanda_68_pair_opportunity_census as census


def config():
    return {"modeled_slippage_pips":0.25,"maximum_entry_spread_pips":5.0,
            "preferred_entry_spread_pips":3.0,"minimum_imbalance_absolute":0.05,
            "minimum_move_to_cost_ratio":1.5,"horizons_sec":[300],"combiner_horizons_sec":[300]}


def point(mid,spread=1.0,imbalance=0.5):
    return {"mid":mid,"spread":spread,"updates":10.0,"imbalance_5s":imbalance,
            "imbalance_30s":imbalance,"imbalance_120s":imbalance}


def test_modeled_window_uses_both_endpoint_spreads_and_slippage():
    result=census.modeled_window("EUR_USD",point(1.0,1.0),point(1.0003,2.0),0.25)
    assert round(result["absolute_move_pips"],6)==3.0
    assert result["modeled_cost_pips"]==1.75
    assert round(result["oracle_after_cost_pips"],6)==1.25
    assert result["movement_cleared_cost"] is True


def test_venue_pip_metadata_covers_non_jpy_two_decimal_pairs(tmp_path):
    path=tmp_path/"quotes.json"
    path.write_text('{"quotes":{"USD_THB":{"pip":0.01},"EUR_USD":{"pip":0.0001}}}')
    values=census.load_pip_sizes(path)
    assert values=={"USD_THB":0.01,"EUR_USD":0.0001}


def test_candidate_is_causal_and_requires_three_way_direction_agreement():
    timeline={0:point(1.0),600:point(1.0001),900:point(1.0003,imbalance=0.5)}
    candidate=census.candidate_at("EUR_USD",900,timeline,config())
    assert candidate is not None and candidate["direction"]==1
    timeline[900]=point(1.0003,imbalance=-0.5)
    assert census.candidate_at("EUR_USD",900,timeline,config()) is None


def test_exactly_three_selection_rejects_shared_currency_factors():
    rows=[{"instrument":"EUR_USD","score":9},{"instrument":"GBP_USD","score":8},
          {"instrument":"AUD_JPY","score":7},{"instrument":"CAD_CHF","score":6}]
    selected=census.select_disjoint(rows,3)
    assert [row["instrument"] for row in selected]==["EUR_USD","AUD_JPY","CAD_CHF"]


def test_opportunity_census_uses_aligned_nonoverlapping_windows():
    by_pair={"EUR_USD":{0:point(1.0),300:point(1.0003),600:point(1.00031)}}
    result=census.opportunity_census(by_pair,config())["300"]["days"][0]
    assert result["pair_windows"]==2
    assert result["spread_clearing_pair_windows"]==1


def test_rolling_census_and_path_capture_in_window_move():
    by_pair={
        "EUR_USD":{
            0:point(1.0),
            60:point(1.0004),
            120:point(1.0),
            300:point(1.0),
            360:point(1.0004),
        }
    }
    result=census.opportunity_census(by_pair,config())["300"]
    rolling=result["rolling_every_minute"]["days"][0]
    assert rolling["pair_windows"]==2
    assert rolling["spread_clearing_pair_windows"]==0
    assert rolling["path_clearing_pair_windows"]==2
    assert rolling["median_first_path_clear_sec"]==60


def test_ten_minute_horizon_is_part_of_canonical_config():
    payload=census.read_json(census.CONFIG)
    assert 600 in payload["horizons_sec"]
    assert 600 in payload["combiner_horizons_sec"]
