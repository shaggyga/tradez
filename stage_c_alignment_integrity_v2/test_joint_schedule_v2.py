import copy
import pytest
from joint_readiness_schedule_v2 import schedule,select_fit,GROUPS,HORIZONS,visible_forecast

def fixture():
    fits=[{'group':g,'horizon_minutes':h,'fit_cutoff':t,'fit_id':f'{g}:{h}:{t}','maximum_outcome_available_epoch':t} for t in (1000,173800) for g in GROUPS for h in HORIZONS]
    origins=[1060,22660,173860,195460]
    return fits,origins,[1000,173800]

def test_hand_calculated_single_worker_schedule():
    s=schedule(*fixture());first=[x for x in s['fit_tasks'] if x['fit_cutoff']==1000]
    assert [(x['start_epoch'],x['end_epoch']) for x in first[:3]]==[(1000,1030),(1030,1060),(1172,1202)]
    assert first[-1]['joint_ready_epoch']==1952
    allrows=sorted(s['fit_tasks']+s['prediction_tasks'],key=lambda x:x['start_epoch'])
    assert all(a['end_epoch']<=b['start_epoch'] for a,b in zip(allrows,allrows[1:]))

def test_first_use_missing_then_later_ready():
    s=schedule(*fixture())
    assert select_fit(s,'legacy26',15,'frozen',1029) is None
    assert select_fit(s,'legacy26',15,'frozen',1030)['fit_cutoff']==1000
    assert select_fit(s,'full228_cost2',7200,'adaptive',1060) is None
    assert select_fit(s,'full228_cost2',7200,'adaptive',1952)['fit_cutoff']==1000

def test_adaptive_previous_ready_fallback_and_frozen_are_distinct():
    s=schedule(*fixture())
    assert select_fit(s,'full228_cost2',7200,'adaptive',173860)['fit_cutoff']==1000
    assert select_fit(s,'legacy26',15,'adaptive',173860)['fit_cutoff']==173800
    assert select_fit(s,'full228_cost2',7200,'adaptive',174752)['fit_cutoff']==173800
    assert select_fit(s,'full228_cost2',7200,'frozen',174752)['fit_cutoff']==1000

def test_future_artifact_identity_does_not_change_policy_or_earlier_selection():
    f,o,c=fixture();a=schedule(f,o,c)
    for row in f:
        if row['fit_cutoff']==173800:row['fit_id']='changed-future-'+row['fit_id']
    b=schedule(f,o,c)
    assert a['policy_sha256']==b['policy_sha256'] and a['schedule_id']!=b['schedule_id']
    assert select_fit(a,'full228_cost2',7200,'adaptive',22660)==select_fit(b,'full228_cost2',7200,'adaptive',22660)

@pytest.mark.parametrize('mutation',['duplicate','missing','unmatured','overlapping_origins'])
def test_invalid_schedule_refused(mutation):
    f,o,c=fixture()
    if mutation=='duplicate':f.append(copy.deepcopy(f[0]))
    elif mutation=='missing':f.pop()
    elif mutation=='unmatured':f[0]['maximum_outcome_available_epoch']=1001
    else:o=[1060,1061]
    with pytest.raises(ValueError):schedule(f,o,c)

def test_publication_and_native_conditioning_age_are_separate_gates():
    f={'schema_version':'forecast.v2','forecast_id':'id','instrument':'USD_JPY','decision_epoch':1000,'available_epoch':1010,'model_id':'m','model_ready_epoch':999,'training_view_fingerprint':'t','target_id':'z','prediction':1.,'coverage_reason':'eligible'}
    assert visible_forecast({'forecast':f},1009)['status']=='not_yet_available'
    assert visible_forecast({'forecast':f},1010)['status']=='available'
    assert visible_forecast({'forecast':f},1010,maximum_conditioning_age_seconds=2)['status']=='stale_conditioning'
