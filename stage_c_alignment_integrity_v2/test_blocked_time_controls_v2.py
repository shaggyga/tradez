import copy,sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parent))
from blocked_time_controls_v2 import compare

def records(days=3):
    a=[];b=[];out=[]
    for day in range(days):
        for hour in range(4):
            origin=day*86400+hour*21600+60
            for pair,j in [('A_B',0),('C_D',1)]:
                value=10*day+hour+j;rid=f'{pair}:{origin}'
                f={'target_id':'t','instrument':pair,'decision_epoch':origin,'available_epoch':origin+2,'prediction':value}
                a.append({'record_id':rid,'procedure':'frozen','forecast':f});b.append({'record_id':rid,'procedure':'frozen','forecast':{**f,'prediction':0}})
                out.append({'record_id':rid,'target_id':'t','value':value,'available_epoch':origin+900,'label_end_epoch':origin+900})
    return a,b,out
def call(a,b,o):return compare(a,b,o,asof=10**9,universe=['A_B','C_D'])
def test_all_day_shifts_preserve_vector_and_destroy_perfect_association():
    s,v=call(*records());assert s['balanced_rows']==24 and len(v)==3 and s['p_value'] is None
    assert v[0]['metrics']['candidate_mse_bps2']==0
    assert [r['metrics']['candidate_mse_bps2'] for r in v[1:]]==[200,200]
    assert all(r['metrics']['control_mse_bps2']==v[0]['metrics']['control_mse_bps2'] for r in v)
    for r in v:
        mapping=r['outcome_origin_mapping'];assert sorted(x[0] for x in mapping)==sorted(x[1] for x in mapping)
        assert all(x%86400==y%86400 for x,y in mapping)
def test_missing_one_pair_balances_entire_panel_without_zero_fill():
    a,b,o=records();o[0]['value']=None;s,v=call(a,b,o)
    assert s['balanced_instruments']==['C_D'] and s['excluded_instruments']==['A_B'] and s['balanced_rows']==12
    assert s['outcome_exclusions']=={'missing_endpoint':1} and s['balance_excluded_mature_rows']==11
    assert all(r['rows']==12 for r in v)
def test_partial_day_excluded_and_last_full_day_can_be_unmature():
    a,b,o=records();cut=2*86400+18*3600+60
    a=[r for r in a if r['forecast']['decision_epoch']!=cut];b=[r for r in b if r['forecast']['decision_epoch']!=cut]
    s,v=call(a,b,o);assert s['day_count']==2 and len(s['excluded_utc_days'])==1 and len(v)==2
def test_no_future_outcome_admission():
    a,b,o=records()
    for row in o:
        if row['label_end_epoch']>=86400:row['available_epoch']=10**9+1
    r=call(a,b,o);assert r[0]['day_count']==1
    assert r[1][0]['status']=='insufficient_balanced_day_support' and r[1][0]['metrics'] is None
@pytest.mark.parametrize('kind',['forecast_duplicate','outcome_duplicate','support_mismatch','clock_mismatch','backdated_label','nonfinite'])
def test_invalid_inputs_refused(kind):
    a,b,o=records()
    if kind=='forecast_duplicate':a.append(copy.deepcopy(a[0]))
    elif kind=='outcome_duplicate':o.append(copy.deepcopy(o[0]))
    elif kind=='support_mismatch':b.pop()
    elif kind=='clock_mismatch':b[0]['forecast']['available_epoch']+=1
    elif kind=='backdated_label':o[0]['available_epoch']-=1
    else:o[0]['value']=float('nan')
    with pytest.raises(ValueError):call(a,b,o)
