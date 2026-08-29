import oanda_executable_opportunity_ranking as ranking
import oanda_68_pair_opportunity_census as source


def point(mid,spread=1.0,imbalance=.25):
    return {"mid":mid,"spread":spread,"updates":10.0,"imbalance_5s":imbalance,"imbalance_30s":imbalance,"imbalance_120s":imbalance}


def test_features_do_not_change_when_only_future_price_changes():
    source._PIP_SIZES={"EUR_USD":.0001}
    timeline={0:point(1.0),900:point(1.0001),1500:point(1.0002),1740:point(1.0003),1800:point(1.0004),2100:point(1.0005)}
    first=ranking.build_frame({"EUR_USD":timeline},300,.25,5.0)
    timeline[2100]=point(1.0020)
    second=ranking.build_frame({"EUR_USD":timeline},300,.25,5.0)
    assert first[ranking.FEATURES].iloc[0].tolist()==second[ranking.FEATURES].iloc[0].tolist()
    assert first["future_move_pips"].iloc[0]!=second["future_move_pips"].iloc[0]


def test_disjoint_allocator_never_reuses_currency():
    import pandas as pd
    frame=pd.DataFrame([{"instrument":"EUR_USD","predicted_ev_pips":4},{"instrument":"GBP_USD","predicted_ev_pips":3},
                        {"instrument":"AUD_JPY","predicted_ev_pips":2},{"instrument":"CAD_CHF","predicted_ev_pips":1}])
    selected=ranking.select_disjoint(frame,3)
    assert selected["instrument"].tolist()==["EUR_USD","AUD_JPY","CAD_CHF"]


def test_metrics_exposes_single_result_concentration():
    result=ranking.metrics([-1,-1,5])
    assert result["average_net_pips"]==1
    assert result["average_without_best_pips"]==-1
