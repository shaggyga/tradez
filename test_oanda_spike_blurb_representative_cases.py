from oanda_spike_blurb_representative_cases import horizon_class, overlap_ratio, select_pair_cases


def _row(i,start,horizon,side,score=10):
    return {"candidate_id":str(i),"entry_epoch":start,"exit_epoch":start+horizon*60,
            "horizon_min":horizon,"selected_side":side,"after_cost_pips":score,
            "path_efficiency":.8,"gross_cost_multiple":3,"selection_tier":"q99"}


def test_horizon_classes_cover_contract_boundaries():
    assert horizon_class(15)=="micro_1_15m"; assert horizon_class(30)=="intrahour_30_60m"
    assert horizon_class(120)=="session_2_6h"; assert horizon_class(720)=="intraday_12_24h"
    assert horizon_class(2880)=="multiday_2_30d"


def test_overlap_uses_shorter_interval():
    assert overlap_ratio(_row(1,0,10,"long"),_row(2,300,10,"short")) == .5
    assert overlap_ratio(_row(1,0,10,"long"),_row(2,1000,10,"short")) == 0


def test_selection_forces_both_directions_and_horizon_diversity():
    rows=[]
    horizons=[5,30,120,720,2880]
    for i,h in enumerate(horizons): rows.append(_row(i,i*300000,h,"long",20-i))
    rows.append(_row(20,2_000_000,30,"short",1))
    selected=select_pair_cases(rows,limit=6)
    assert len({row["candidate_id"] for row in selected}) == len(selected)
    assert {row["selected_side"] for row in selected}=={"long","short"}
    assert {row["horizon_class"] for row in selected}=={horizon_class(h) for h in horizons}
