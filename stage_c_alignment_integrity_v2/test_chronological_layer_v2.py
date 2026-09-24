import copy
from types import SimpleNamespace
import pytest
from contracts import fingerprint,TrainingView
from test_magnitude_layer_v2 import fixture,current_row
from magnitude_layer_v2 import fit_snapshot
from chronological_layer_v2 import combine,frozen_snapshot,frame,assessment,verify_population
from later_surface_layer_v2 import layer_settings


def setup():
    rows,labels,settings=fixture();origin=500000
    current=[current_row(r,origin) for r in rows[:20]]
    obs=[{'record_id':r['record_id'],'instrument':r['instrument'],'origin_epoch':origin,'available_epoch':origin,'features':[1.]*26} for r in current]
    parent={'layer_contract':{**settings,'frozen_cutoff':400000}}
    c={'parent_surface_contract':parent,'horizons_minutes':[15],'new_origins':[origin],'universe':sorted(r['instrument'] for r in current),
       'variants':['raw_unrestricted']+[v+'_'+m for m in ('frozen','expanding') for v in ('raw_matched','signed_only','magnitude_interaction')],
       'assessment_asof':origin+1000,'cohorts':[{'name':'cohort','target':origin+900,'policy_origins':[origin]}]}
    scope=['legacy26','technical_endpoint_midpoint_elapsed_15m','frozen']
    frozen=fit_snapshot(rows,labels,400000,scope,15,layer_settings(parent))
    for r in current:labels[r['record_id'],scope[1]]={'record_id':r['record_id'],'target_id':scope[1],'label_end_epoch':origin+900,'available_epoch':origin+900,'value':0.}
    return rows+current,labels,obs,frozen,c


def test_original_frozen_snapshot_reused_without_regression(monkeypatch):
    rows,labels,obs,frozen,c=setup()
    monkeypatch.setattr('chronological_layer_v2.fit_snapshot',lambda *a:pytest.fail('existing snapshot must not refit'))
    actual,reused=frozen_snapshot({'snapshots':{'frozen':frozen}},rows,labels,15,c)
    assert reused and actual is frozen


def test_changed_mature_membership_refuses_original_snapshot():
    rows,labels,obs,frozen,c=setup();labels[rows[0]['record_id'],rows[0]['target_id']]['value']+=1
    with pytest.raises(ValueError,match='membership'):frozen_snapshot({'snapshots':{'frozen':frozen}},rows,labels,15,c)


def test_future_labels_and_current_values_cannot_change_training():
    rows,labels,obs,frozen,c=setup();before=frame(15,500000,rows,obs,labels,frozen,c)
    for r in rows:
        if r['decision_epoch']>=500000:labels[r['record_id'],r['target_id']]['value']=float('nan')
    after=frame(15,500000,rows,obs,labels,frozen,c)
    assert after==before


def test_missing_base_keeps_all_arms_unavailable():
    rows,labels,obs,frozen,c=setup();rows=[r for r in rows if r['record_id']!=obs[0]['record_id']]
    result=frame(15,500000,rows,obs,labels,frozen,c)
    assert len(result['coverage'])==280 and len(result['predictions'])==266
    assert sum(r['reason']=='base_unavailable' for r in result['coverage'])==14


def test_layer_controls_match_and_diagnostic_clock_is_not_native():
    rows,labels,obs,frozen,c=setup();result=frame(15,500000,rows,obs,labels,frozen,c)
    assert len(result['predictions'])==280
    for pair in c['universe']:
        for base in ('ridge','recovered_hgb'):
            part={r['variant']:r for r in result['predictions'] if r['instrument']==pair and r['base_method']==base}
            for mode in ('frozen','expanding'):
                values=[part[v+'_'+mode] for v in ('raw_matched','signed_only','magnitude_interaction')]
                assert len({v['eligibility_snapshot_id'] for v in values})==1
                assert all(v['available_epoch']==500031 and not v['native_policy_admitted'] and v['production_available_epoch'] is None for v in values)
                assert values[0]['prediction_bps']==part['raw_unrestricted']['prediction_bps']


def test_assessment_null_maturity_and_missingness():
    rows,labels,obs,frozen,c=setup();f=frame(15,500000,rows,obs,labels,frozen,c)
    labels[obs[0]['record_id'],'technical_endpoint_midpoint_elapsed_15m']['value']=None
    result=assessment([f],labels,c)
    assert all(s['mature_rows']==19 for s in result['scores'])
    assert all(s['mature_rows']==0 and s['mae_bps'] is None for s in assessment([f],labels,{**c,'assessment_asof':500899})['scores'])
    assert all(p['mature_rows']==19 for p in result['paired_differences'])


def test_wrong_endpoint_or_backdated_label_refused():
    rows,labels,obs,frozen,c=setup();f=frame(15,500000,rows,obs,labels,frozen,c)
    labels[obs[0]['record_id'],'technical_endpoint_midpoint_elapsed_15m']['available_epoch']=500899
    with pytest.raises(ValueError,match='maturity'):assessment([f],labels,c)


def test_score_population_mismatch_refuses():
    rows,labels,obs,frozen,c=setup();f=frame(15,500000,rows,obs,labels,frozen,c)
    index=next(i for i,r in enumerate(f['predictions']) if r['variant']=='signed_only_expanding');f['predictions'].pop(index)
    with pytest.raises(ValueError,match='matched_score_support'):assessment([f],labels,c)


def test_full_original_new_coverage_and_duplicates():
    c={'original_origins':[1],'new_origins':[2],'universe':['A']}
    def chunk(t):return {'horizon_minutes':15,'joined_rows':[{'record_id':'A:'+str(t),'decision_epoch':t}],
        'coverage':[{'record_id':'A:'+str(t),'base_methods':['ridge','recovered_hgb']}]}
    a,b=chunk(1),chunk(2);assert len(combine(a,b,15,c))==2
    b['coverage']*=2
    with pytest.raises(ValueError,match='coverage'):combine(a,b,15,c)


def test_population_change_refuses_before_saved_model_loading(monkeypatch):
    monkeypatch.setattr('chronological_layer_v2.validate_fit',lambda *a:None)
    from matched_campaign_models_v2 import population,FEATURES
    view=TrainingView(0,100,100,100,1000);target={'target_id':'test','horizon_seconds':10}
    obs=[{'record_id':'A:10','instrument':'A','origin_epoch':10,'available_epoch':10,'features':[1.]*26}]
    outcomes=[{'record_id':'A:10','target_id':'test','available_epoch':20,'label_end_epoch':20,'value':2.}]
    rows=population(obs,outcomes,target,view)
    meta={'fit_id':'x','training_view':view.identity(),'fit_cutoff':100,'target':target,'training_population_sha256':fingerprint(rows),'training_rows':1,'feature_schema_sha256':fingerprint(FEATURES)}
    assert verify_population(meta,b'',obs,outcomes)['training_rows']==1
    obs[0]['features'][0]+=1
    with pytest.raises(ValueError,match='population'):verify_population(meta,b'',obs,outcomes)
