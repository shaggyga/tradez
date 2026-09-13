from decimal import getcontext
import pytest
import derive_verified_paper_plot_data_v1 as d


def mapping():
    return {'mapping_sha256':'m','source_header':{'read_started_epoch':11,'read_completed_epoch':12},'quotes':{'GBP_USD':{
        'quote_id':'q','market_epoch':10,'available_epoch':12,'bid':'1.23456','ask':'1.23459','tradeable':True,
        'raw_snapshot_sha256':'r','capture_receipt_sha256':'c'}}}


def test_quote_midpoint_preserves_exact_half_tick_and_original_two_clocks():
    before=getcontext().prec;getcontext().prec=3
    try:row=d.point(mapping())
    finally:getcontext().prec=before
    assert row['midpoint']=='1.234575' and row['market_epoch']==10 and row['available_epoch']==12


def test_missing_quote_remains_missing_not_zero():
    assert d.point(None) is None
    assert d.point({'quotes':{}}) is None


@pytest.mark.parametrize('field,value',[('bid','2'),('tradeable',False),('market_epoch',13)])
def test_invalid_quote_never_becomes_plot_point(field,value):
    source=mapping();source['quotes']['GBP_USD'][field]=value
    with pytest.raises(ValueError):d.point(source)


def test_flat_is_actual_inventory_zero_without_inventing_price():
    value=d.inventory({'position':None,'realized_usd':'-1.23'})
    assert value=={'side':0,'base_units':0,'position_open':False,'realized_usd':'-1.23'}


def test_short_inventory_and_actual_cash_retained():
    assert d.inventory({'position':{'side':-1,'base_units':1842},'realized_usd':'0'})['side']==-1


def test_unverified_source_and_traversal_rejected_before_read():
    reader=d.Reader({'verified_files':[]})
    with pytest.raises(ValueError,match='plot_unverified_source'):reader.get('episodes/episode_01/plan.json')
    with pytest.raises(ValueError,match='plot_relative_path'):reader.get('episodes/episode_01/../../raw.json')
