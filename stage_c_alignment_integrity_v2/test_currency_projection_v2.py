from copy import deepcopy
import hashlib,math,shutil
from pathlib import Path
import pytest
from contracts import fingerprint
from currency_projection_v2 import BASES,VARIANTS,load_solver,project_frame,assess

def solver_root():
    source=Path(__file__).resolve().parent
    if (source.parent/'solver/features/currency_state_engine.py').is_file():return source.parent/'solver'
    for parent in source.parents:
        root=parent/'trad/src/forex_system'
        if (root/'features/currency_state_engine.py').is_file():return root
    raise AssertionError('frozen solver fixture missing')

def pins(root):return {n:{'bytes':(root/n).stat().st_size,'sha256':hashlib.sha256((root/n).read_bytes()).hexdigest()} for n in ('features/currency_state_engine.py','contracts/currency_state.py')}

@pytest.fixture
def solver():
    root=solver_root();return load_solver(root,pins(root))

def fixture():
    pairs=['AUD_EUR','AUD_GBP','AUD_USD','EUR_GBP','EUR_USD','GBP_USD']
    c={'origins':[100],'horizons_minutes':[360],'universe':pairs,'assessment_asof':30000,'projection_slot_seconds':1,'solver_source_hashes':{},
       'solver_policy':{'minimum_observations':6,'minimum_component_currencies':4,'return_clip_minimum_bps':1000000.,'return_clip_median_multiple':1.,'measurement_uncertainty_floor_bps':.05}}
    rows=[];coverage=[]
    for base in BASES:
        for i,pair in enumerate(pairs):
            p={'instrument':pair,'record_id':pair+':100','base_method':base,'variant':'raw_unrestricted','origin_epoch':100,'decision_epoch':100,
                'horizon_minutes':360,'target_epoch':21700,'target_id':'technical_endpoint_midpoint_elapsed_360m','available_epoch':102,
                'prediction_bps':[1.,4.,2.,-3.,2.,7.][i],'model_id':'model-'+base,'signed_fit_id':'fit-'+base}
            p['forecast_id']=fingerprint(p);rows.append(p);coverage.append({'instrument':pair,'base_method':base,'variant':'raw_unrestricted','reason':'eligible'})
    frame={'origin_epoch':100,'horizon_minutes':360,'reserved_ready_epoch':116,'predictions':rows,'coverage':coverage}
    labels=[{'record_id':p+':100','target_id':'technical_endpoint_midpoint_elapsed_360m','label_end_epoch':21700,'available_epoch':21701,'value':2.} for p in pairs]
    return frame,c,labels

def test_preserved_solver_projects_zero_sum_and_triangle_without_changing_direct(solver):
    f,c,_=fixture();out=project_frame(f,c,solver)
    assert len(out['coverage'])==36 and len(out['predictions'])==36
    for d in out['solver_diagnostics']:assert abs(sum(d['forecast_factors_log_bps'].values()))<1e-9 and d['matrix_rank']==4
    by={(p['base_method'],p['variant'],p['instrument']):p for p in out['predictions']}
    for p in f['predictions']:assert by[p['base_method'],'direct',p['instrument']]['prediction_bps']==p['prediction_bps']
    for base in BASES:
        log=lambda pair:math.log1p(by[base,'currency_projection',pair]['prediction_bps']/10000)*10000
        assert abs(log('AUD_EUR')+log('EUR_USD')-log('AUD_USD'))<1e-9
        for pair in c['universe']:
            a=math.log1p(by[base,'direct',pair]['prediction_bps']/10000)*10000;b=log(pair)
            half=math.log1p(by[base,'half_residual',pair]['prediction_bps']/10000)*10000
            assert abs(half-(a+b)/2)<1e-9

@pytest.mark.parametrize('fault',['identity','origin','target','target_id','future','nonfinite','invalid_return','duplicate','coverage'])
def test_changed_identity_clock_target_and_inventory_refuse(solver,fault):
    f,c,_=fixture();p=f['predictions'][0]
    if fault=='identity':p['prediction_bps']=3.
    elif fault=='origin':p['origin_epoch']=99
    elif fault=='target':p['target_epoch']=21701
    elif fault=='target_id':p['target_id']='wrong'
    elif fault=='future':p['available_epoch']=117
    elif fault=='nonfinite':p['prediction_bps']=float('inf')
    elif fault=='invalid_return':p['prediction_bps']=-10000.
    elif fault=='duplicate':f['predictions'].append(deepcopy(p))
    else:f['coverage'].pop()
    if fault not in ('identity','nonfinite'):p['forecast_id']=fingerprint({k:v for k,v in p.items() if k!='forecast_id'})
    with pytest.raises((ValueError,OverflowError)):project_frame(f,c,solver)

def test_missing_base_does_not_create_synthetic_forecast(solver):
    f,c,_=fixture();f['predictions']=[]
    for row in f['coverage']:row['reason']='base_unavailable'
    out=project_frame(f,c,solver)
    assert not out['predictions'] and len(out['coverage'])==36
    assert all(x['reason']=='base_unavailable' for x in out['coverage'])

def test_disconnected_or_undersupported_graph_keeps_direct_only(solver):
    f,c,_=fixture();keep={'AUD_EUR','GBP_USD'}
    f['predictions']=[p for p in f['predictions'] if p['instrument'] in keep]
    for x in f['coverage']:
        if x['instrument'] not in keep:x['reason']='base_unavailable'
    out=project_frame(f,c,solver)
    assert len(out['predictions'])==4 and all(p['variant']=='direct' for p in out['predictions'])
    assert sum(x['reason']=='projection_component_unavailable' for x in out['coverage'])==8

def test_future_and_null_labels_are_explicit_and_never_change_projection(solver):
    f,c,labels=fixture();out=project_frame(f,c,solver);before=fingerprint(out)
    c['assessment_asof']=21700;a=assess([out],labels,c);assert a['outcome_status_counts']=={'not_mature':36} and not a['scores']
    c['assessment_asof']=30000
    for x in labels:x['value']=None
    a=assess([out],labels,c);assert a['outcome_status_counts']=={'unavailable':36} and not a['scores']
    assert fingerprint(out)==before and fingerprint(project_frame(f,c,solver))==before

@pytest.mark.parametrize('fault',['duplicate','end','backdate','nonfinite'])
def test_outcome_binding_and_maturity_refuse(solver,fault):
    f,c,labels=fixture();out=project_frame(f,c,solver)
    if fault=='duplicate':labels.append(deepcopy(labels[0]))
    if fault=='end':labels[0]['label_end_epoch']=21699
    if fault=='backdate':labels[0]['available_epoch']=21699
    if fault=='nonfinite':labels[0]['value']=float('nan')
    with pytest.raises(ValueError):assess([out],labels,c)

def test_matched_scores_use_exact_same_records(solver):
    f,c,labels=fixture();r=assess([project_frame(f,c,solver)],labels,c)
    assert len(r['matched_comparisons'])==4
    for x in r['matched_comparisons']:assert x['direct']['support_sha256']==x['projected']['support_sha256'] and x['direct']['mature_rows']==6

def test_changed_preserved_solver_refuses_before_import(tmp_path):
    original=solver_root();expected=pins(original)
    for n in expected:
        (tmp_path/n).parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(original/n,tmp_path/n)
    with (tmp_path/'features/currency_state_engine.py').open('a') as f:f.write('\n#changed\n')
    with pytest.raises(ValueError,match='before_import'):load_solver(tmp_path,expected)
