import copy,datetime as dt,os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_numeric_state_v2 as core
import macro_numeric_state_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_NUMERIC_STATE_INPUTS',str(ROOT/'evidence/timed_20260922_194458/macro_numeric_state/inputs')))
RECIPE=ROOT/'MACRO_NUMERIC_STATE_OPERATOR_RECIPE.json'

@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_NUMERIC_STATE_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('numeric-state')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

def fixture(value=3.3,vid='v1',known=1000,numeric=1100,event='e1',content=None):
    fields={'source_id':'source','event_series_id':'series','actual_value':value,'previous_value':3.1,'unit':'year_percent_change','reference_period':'2026M08','numeric_extraction_contract_id':'fixture'}
    content=content or core.fingerprint(fields)
    b={'version_id':vid,'event_id':event,'cache_key':'identical-text-cache','content_sha256':content,'currencies':['EUR'],'source_id':'source',
       'kind':'retained_parsed_detail','listing_bootstrap':False,'published_time_inferred':False,'policy_document_class':'decision',
       'observations':[{'observation_id':vid+'o','sequence':1,'known_epoch':known,'available_epoch':known,'published_epoch':900,'clock_status':'valid_attested_observation'}]}
    row={'version_id':vid,'content_sha256':content,'fields':fields}
    clock={'version_id':vid,'observation_id':vid+'o','known_epoch':known,'clock_status':'valid_attested_observation',
           'numeric_causal_known_utc':dt.datetime.fromtimestamp(numeric,dt.timezone.utc).isoformat() if numeric is not None else None}
    return b,row,clock

def snapshot(rows,cutoff=1500):
    bs=[r[0] for r in rows];numeric,material,clocks=core.prepare(bs,[r[1] for r in rows],[r[2] for r in rows])
    return core.snapshot(bs,numeric,material,clocks,cutoff,['EUR'])

def test_numeric_clock_delays_content_without_old_value_fallback():
    old=fixture();new=fixture(3.5,'new',1200,1800)
    event=snapshot([old,new])[0][0]
    assert event['selected_numeric_version_ids']==['new'] and event['status']=='numeric_clock_not_ready'
    assert event['numeric_cells']==[] and event['numeric_cache_key'] is None and not event['fallback_to_older_numeric_version']
    after=snapshot([old,new],1800)[0][0];assert after['numeric_cells'][0]['values']['actual']=='3.5'

def test_late_readiness_of_old_version_does_not_replace_newer_source_version():
    old=fixture(3.3,'old',1000,2000);new=fixture(3.5,'new',1200,1300)
    event=snapshot([old,new],2100)[0][0]
    assert event['selected_numeric_version_ids']==['new'] and event['numeric_cells'][0]['values']['actual']=='3.5'

def test_future_revision_cannot_change_earlier_numeric_state():
    old=fixture();future=fixture(99,'future',2000,2100)
    assert snapshot([old,future])==snapshot([old])

def test_numeric_material_conflict_cannot_hide_behind_identical_text():
    a,b=fixture(),fixture(3.4,'v2');x=snapshot([a,b]);assert x==snapshot([b,a])
    assert x[0][0]['status']=='ambiguous_numeric_material' and not x[0][0]['numeric_cells']

def test_missing_numeric_clock_abstains():
    a=fixture(numeric=None);e=snapshot([a])[0][0]
    assert e['status']=='missing_or_invalid_numeric_clock' and not e['numeric_cells']

@pytest.mark.parametrize('field,value',[('known_epoch',2000),('available_epoch',2000),('clock_status','unproven_observation')])
def test_original_source_receipt_gates_remain(field,value):
    a=fixture();a[0]['observations'][0][field]=value
    if field in ('known_epoch','clock_status'):a[2][field]=value
    assert not snapshot([a])[0]

def test_future_envelope_and_current_conflicting_numeric_clock():
    a=fixture();b,row,c=a
    later={**b['observations'][0],'observation_id':'later','sequence':2,'known_epoch':2000,'available_epoch':2000}
    later_clock={**c,'observation_id':'later','known_epoch':2000,'numeric_causal_known_utc':'1970-01-01T00:40:00Z'}
    before=snapshot([a]);b=copy.deepcopy(b);b['observations'].append(later)
    ns,ms,cs=core.prepare([b],[row],[c,later_clock]);assert core.snapshot([b],ns,ms,cs,1500,['EUR'])==before
    b['observations'][-1].update(known_epoch=1000,available_epoch=1000);later_clock['known_epoch']=1000
    ns,ms,cs=core.prepare([b],[row],[c,later_clock]);e=core.snapshot([b],ns,ms,cs,1500,['EUR'])[0][0]
    assert e['status']=='ambiguous_same_clock_numeric_readiness'

def test_components_do_not_inherit_parent_period_or_merge_priors():
    fields={'actual_value':196000,'previous_value':206000,'revised_previous_value':206000,'unit':'claims','reference_period':'2026-09-12',
        'release_components':[{'component_id':'insured','actual_value':1730000,'previous_value':1769000,'unrevised_previous_value':1774000,'revision_raw':-5000,'unit':'claims'}],
        'source_native_components':{'insured':{'reference_period':'September 5'}}}
    a,c=core.cells_for(fields)
    assert a['reference']['period']=='2026-09-12' and c['reference']['period'] is None and c['reference']['raw']=='September 5'
    assert c['values']['reported_previous']=='1769000' and c['values']['reported_unrevised_previous']=='1774000'
    assert c['values']['reported_revision']=='-5000' and c['numeric_surprise'] is None and not c['original_prior_vintage_verified']

def test_duplicate_component_identity_rejected_and_zero_is_present():
    assert core.cells_for({'actual_value':0})[0]['values']['actual']=='0'
    with pytest.raises(ValueError,match='numeric_component_identity_invalid'):
        core.cells_for({'release_components':[{'component_id':'x'},{'component_id':'x'}]})

def test_actual_population_and_shared_currency_pair_identity(completed):
    report=op.read(completed/'numeric_state_report.json');assert report['versions']==2696 and report['numeric_versions']==31
    assert report['pair_rows']==544 and report['currency_rows']==168 and report['components_with_missing_or_unresolved_reference']==9
    assert report['numeric_surprises_computed']==report['forecast_features_admitted']==0
    states={}
    for snap in op.read(completed/'numeric_currency_states.json'):
        for s in snap['states']:
            digest=s.pop('state_sha256');assert core.fingerprint(s)==digest;states[digest]=s
            assert s['cross_series_numeric_aggregate'] is None and not s['forecast_admission']
    for p in op.read(completed/'numeric_pair_views.json'):
        a,b=states[p['base_state_sha256']],states[p['quote_state_sha256']]
        assert p['pair']==a['currency']+'_'+b['currency'] and p['cutoff_epoch']==a['cutoff_epoch']==b['cutoff_epoch']
        assert p['base_minus_quote_numeric_value'] is None and not p['forecast_admission']
    for snap in op.read(completed/'numeric_source_asof.json'):
        for e in snap['events']:
            if e['status']!='retained_numeric_observation_ready':assert not e['numeric_cells'] and e['numeric_cache_key'] is None
            assert not e['original_extraction_ready_independently_proven']

def test_pin_projection_mismatch_and_crash_resume(tmp_path,completed):
    b,r,c=fixture();c['known_epoch']+=1
    with pytest.raises(ValueError,match='numeric_observation_projection_mismatch'):core.prepare([b],[r],[c])
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};blobs['numeric_projection.json']+=b' '
    with pytest.raises(ValueError,match='numeric_state_input_pin_mismatch'):core.build(blobs)
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_numeric_state_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','2'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91
    result=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert result['status']=='completed_verified'
    assert all(op.sha(Path(result['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
