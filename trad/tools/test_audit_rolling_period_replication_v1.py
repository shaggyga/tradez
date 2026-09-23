from datetime import datetime,timezone
import copy,json
from pathlib import Path

import numpy as np
import pytest

from tools import audit_rolling_period_replication_v1 as audit
from tools import analyze_rolling_specialist_component_baselines_v1 as component_source
from tools import prepare_rolling_specialists_v2 as preparation


def test_period_validation_uses_this_windows_boundaries():
    start=int(datetime(2026,3,23,tzinfo=timezone.utc).timestamp());week=7*86400
    b=dict(start=start,train_end=start+6*week,validation_end=start+7*week,end=start+8*week)
    pairs={str(i):{} for i in range(68)};cuts=[start+i*week for i in (2,3,4,5,6)]
    im={'schema':preparation.SCHEMA,'boundaries':b,'pairs':pairs,'fold_cutoffs_epoch':cuts}
    pm={'boundaries':b,'pairs':pairs};qm=copy.deepcopy(pm);result={'boundaries':b,'fold_cutoffs_epoch':cuts}
    audit.validate_period(im,pm,qm,result)
    changed=copy.deepcopy(result);changed['fold_cutoffs_epoch'][0]+=60
    with pytest.raises(ValueError,match='period_derived'):audit.validate_period(im,pm,qm,changed)
    qm['boundaries']['start']+=60
    with pytest.raises(ValueError,match='same_period'):audit.validate_period(im,pm,qm,result)


def test_representatives_handle_missing_pair_weeks_without_dropping_rows():
    ids=np.array([0,0,1,1,2]);eligible=np.array([False,True,False,False,True])
    assert audit.representatives_for(ids).tolist()==[0,2,4]
    assert audit.representatives_for(ids,eligible).tolist()==[1,4]
    assert audit.representatives_for(ids,np.zeros(len(ids),bool)).dtype==np.int64


def model_data():
    names=audit.family_design.feature_names();n=6
    params={name:np.empty((5,68,50),dtype=bool if name=='supported' else np.int64 if name=='count' else float)
            for name in ('count','mean','scale','supported')}
    params['count'].fill(200);params['mean'].fill(0);params['scale'].fill(1);params['supported'].fill(True)
    return {'raw_x':np.arange(n*50,dtype=float).reshape(n,50),'normalizers':params,'feature_names':names,
            'pair_id':np.arange(n,dtype=np.int16),'entry_long':np.arange(n,dtype=float)*.01+.1,
            'entry_short':np.arange(n,dtype=float)*.03+.2}


@pytest.mark.parametrize('group',audit.family_design.GROUPS)
def test_independent_family_masks_remove_only_declared_slots(group):
    data=model_data();design=audit.family_design.group_manifest(data['feature_names']);rows=np.arange(6)
    got=audit.family_rows(data,rows,group,design)
    before=audit.base_rows(data,rows,'compact50',4)
    expected=audit.family_design.apply_group_mask(before,group,names=data['feature_names'])
    assert audit.exact_arrays(got,expected)
    assert audit.exact_arrays(got[:,50:],before[:,50:])
    assert np.isnan(got[:,design['groups'][group]['removed_indices']]).all()
    assert audit.exact_arrays(data['raw_x'],np.arange(300,dtype=float).reshape(6,50))


def test_family_index_or_context_metadata_tampering_is_rejected():
    data=model_data();design=audit.family_design.group_manifest(data['feature_names'])
    design['groups']['drop_peers']['removed_indices'].append(50)
    with pytest.raises(ValueError,match='exact_canonical'):audit.family_rows(data,np.arange(6),'drop_peers',design)


def test_saved_tree_cannot_have_used_a_removed_family_column():
    from types import SimpleNamespace
    nodes=np.array([(3,0),(0,1),(0,1)],dtype=[('feature_idx','u4'),('is_leaf','u1')])
    class Estimator:
        is_categorical_=np.array([False]*52+[True])
        _predictors=[[SimpleNamespace(nodes=nodes)]]
        def get_params(self):return dict(audit.contract.BASE_RECIPE)
    estimator=Estimator()
    audit.verify_family_tree_features(estimator,[7,8])
    with pytest.raises(ValueError,match='cannot_split_removed'):audit.verify_family_tree_features(estimator,[3,7])
    # Leaf placeholder feature indices do not imply that a field was used.
    audit.verify_family_tree_features(estimator,[0])


def component_data():
    # Explicit original clocks: sampled TRAIN then the two assessment weeks.
    start=int(datetime(2026,3,23,tzinfo=timezone.utc).timestamp());week=7*86400
    boundaries=dict(start=start,train_end=start+6*week,validation_end=start+7*week,end=start+8*week)
    t=np.r_[start+np.arange(100)*900,boundaries['train_end']+np.arange(35)*60,
            boundaries['validation_end']+np.arange(35)*60].astype(np.int64)
    times=np.tile(t,2);split=np.tile(np.r_[np.zeros(100),np.ones(35),np.full(35,2)].astype(np.int8),2)
    n=len(times);ids=np.repeat(np.arange(2,dtype=np.int16),len(t));y=np.sin(np.arange(n)*.6)*5
    y[np.arange(n)%17==0]=0.
    data={'time':times,'split':split,'pair_id':ids,'pair_names':['EUR_USD','USD_JPY'],
          'entry_long':np.full(n,.2),'entry_short':np.full(n,.5),'boundaries':boundaries,
          'cutoffs':np.array([start+i*week for i in (2,3,4,5,6)])}
    for h in (30,60):
        valid=np.arange(n)%13!=0
        data.update({f'y_{h}':y,f'long_{h}':y-.2-.4,f'short_{h}':-y-.5-.7,
            f'valid_{h}':valid,f'strict_{h}':valid&(np.arange(n)%5!=0),
            f'eligible_{h}':times+(h+1)*60<np.where(split==0,boundaries['train_end'],np.where(split==1,boundaries['validation_end'],boundaries['end'])),
            f'arima_{h}':np.ones(n),f'momentum_{h}':np.ones(n)})
    return data


def test_component_train_reference_reconstruction_detects_wrong_wing_or_population():
    data=component_data();record=component_source.train_baselines(data,60)
    audit.verify_component_prior(data,60,record)
    changed=copy.deepcopy(record);changed['parameters']['long_exit_cost']['EUR_USD']['mean_bps']+=.01
    with pytest.raises(ValueError,match='parameters_recreated'):audit.verify_component_prior(data,60,changed)
    changed=copy.deepcopy(record);changed['training_selection']['rows']+=1
    with pytest.raises(ValueError,match='exact_supervised'):audit.verify_component_prior(data,60,changed)


def component_fixture(tmp_path,data,group='drop_peers'):
    assessment=np.flatnonzero(data['split']>0);n=len(assessment)
    heads={'probability':np.full(n,.55),'direct':np.sin(np.arange(n)),
        'positive':np.full(n,3.),'nonpositive':np.full(n,2.8),'long_exit':np.full(n,.42),'short_exit':np.full(n,.68)}
    baseline=component_source.train_baselines(data,60)
    payload={'schema':'rolling_replication_component_baselines_v1','horizon_minutes':60,'group':group,
        'baseline_parameters_sha256':baseline['parameters_sha256'],
        'rows':component_source.comparison_rows(data,assessment,heads,60,group,baseline)}
    path=tmp_path/'components.json';path.write_text(json.dumps(payload))
    return assessment,heads,baseline,payload,{'path':path.name,'sha256':audit.sha(path)}


def test_all30_component_scores_masks_and_cost_persistence_are_independent(tmp_path):
    data=component_data();assessment,heads,baseline,payload,rec=component_fixture(tmp_path,data)
    assert audit.verify_component_scores(tmp_path,rec,data,assessment,heads,60,'drop_peers',baseline)==30
    next(r for r in payload['rows'] if r['component']=='long_exit_cost')['current_quote_wing_persistence']['current_input']='entry_long'
    (tmp_path/'components.json').write_text(json.dumps(payload));rec['sha256']=audit.sha(tmp_path/'components.json')
    with pytest.raises(ValueError,match='opposite_entry_wing'):
        audit.verify_component_scores(tmp_path,rec,data,assessment,heads,60,'drop_peers',baseline)


def test_changed_component_metric_or_key_hash_is_not_accepted(tmp_path):
    data=component_data();assessment,heads,baseline,payload,rec=component_fixture(tmp_path,data)
    payload['rows'][0]['model']['rmse_bps']+=.01
    (tmp_path/'components.json').write_text(json.dumps(payload));rec['sha256']=audit.sha(tmp_path/'components.json')
    with pytest.raises(ValueError,match='numeric_metric'):audit.verify_component_scores(tmp_path,rec,data,assessment,heads,60,'drop_peers',baseline)
    payload['rows'][0]['matched_pair_clock_sha256']='bad'
    (tmp_path/'components.json').write_text(json.dumps(payload));rec['sha256']=audit.sha(tmp_path/'components.json')
    with pytest.raises(ValueError,match='key_and_target'):audit.verify_component_scores(tmp_path,rec,data,assessment,heads,60,'drop_peers',baseline)


def test_component_prior_never_uses_later_outcomes():
    data=component_data();before=component_source.train_baselines(data,30)
    changed=copy.deepcopy(data);later=changed['split']>0
    for key in ('y_30','long_30'):changed[key][later]+=100
    changed['short_30'][later]-=100
    audit.verify_component_prior(changed,30,before)


def test_complete_grid_requires_exact_reference_reuse():
    base={'final_model':{'path':'model','sha256':'x'},'head_forecasts':{'path':'heads','sha256':'y'},
          'component_baseline_scores':{'path':'components','sha256':'z'},'variants':{'direct':{},'mixture_raw':{}}}
    result={'schema':audit.RESULT_SCHEMA,'status':'complete','contexts':{f'{g}_{h}m':copy.deepcopy(base) for g in audit.GROUPS for h in audit.HORIZONS},
        'family_contexts':{g:{} for g in audit.family_design.GROUPS},'comparators':{},'completed_base_bundles':20,'completed_variants':20,
        'completed_family_refits':7,'completed_family_mean_variants':16,'completed_comparator_fits':8,
        'component_priors':{'30':{},'60':{}},'pair_priors':{'30':{},'60':{}},'can_place_orders':False,'models_promoted':0}
    result['family_contexts']['full_compact50']={'reused_context':'compact50_60m','model':base['final_model'],
        'head_forecasts':base['head_forecasts'],'component_baseline_scores':base['component_baseline_scores'],'variants':base['variants']}
    for h in audit.HORIZONS:
        for group in audit.GROUPS:
            for learner in ('ridge','hgb'):result['comparators'][f'comparator_{group}_{learner}_{h}m']={}
        for name in audit.previous_audit.CONTROL_NAMES:result['comparators'][f'comparator_{name}_{h}m']={}
    audit.validate_grid(result)
    changed=copy.deepcopy(result);changed['family_contexts']['full_compact50']['model']['sha256']='new-fit'
    with pytest.raises(ValueError,match='reuse_exact'):audit.validate_grid(changed)


def test_dynamic_prefix_loader_reads_raw_origins_before_training_sample(tmp_path,monkeypatch):
    import pyarrow as pa
    from tools.test_prepare_rolling_specialists_v2 import example
    e=example('2026-05-18');payload=preparation.prepare_pair_values(**e);names=audit.family_design.feature_names()
    data={'pair_names':['EUR_USD'],'feature_names':names,'time':payload['time'],'split':payload['split'],
        'pair_id':np.zeros(len(payload['time']),np.int16),'raw_x':payload['raw_x'],
        'normalizers':{n:payload['normalizer_'+n][:,None,:] for n in ('count','mean','scale','supported')},
        'cutoffs':np.array(preparation.derive_schedule(e['boundaries'])['fit_cutoffs_epoch'])}
    manifest={'pairs':{'EUR_USD':{'origin_rows':len(e['raw_times'])}},'partitions':[{}]}
    (tmp_path/'DATASET.json').write_text(json.dumps(manifest))
    im={'base_root':str(tmp_path),'base_sha256':audit.sha(tmp_path/'DATASET.json'),'boundaries':e['boundaries'],
        'pairs':{'EUR_USD':{'prefix_original_rows_by_cutoff':[56,84,112,140,168]}}}
    columns={'instrument':['EUR_USD']*len(e['raw_times']),'bar_start_epoch':e['raw_times'],'bar_end_epoch':e['raw_times']+60}
    columns.update({n:e['raw_x'][:,i] for i,n in enumerate(names)});table=pa.table(columns)
    monkeypatch.setattr(audit,'iter_partitions',lambda *args,**kwargs:iter([('EUR_USD',table)]))
    report=audit.verify_prefix_inputs(data,im)
    assert report['normalizer_sets']==5 and report['pairs']['EUR_USD']['raw_rows']==224
    data['normalizers']['count'][0,0,0]-=1
    with pytest.raises(ValueError,match='statistics_exact'):audit.verify_prefix_inputs(data,im)
