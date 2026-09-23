import copy,json,os,shutil,subprocess,sys
from decimal import Decimal,localcontext
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_unit_binding_v2 as core
import macro_unit_binding_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_UNIT_BINDING_INPUTS',str(ROOT/'evidence/next_unit_binding_20260922_204402/unit_binding/inputs')))
RECIPE=ROOT/'MACRO_UNIT_BINDING_OPERATOR_RECIPE.json'

def blobs():return {n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS}
@pytest.fixture(scope='module')
def native():return core.native_tools(blobs())
@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_UNIT_BINDING_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('unit-binding')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

def fields(series):return next(json.loads(v['content_json']) for v in op.read(INPUTS/'target_versions.json') if json.loads(v['content_json'])['event_series_id']==series)
def levels():return [{'year':'2026','period':p,'value':v} for p,v in [('M08','110'),('M07','100'),('M06','90')]]
CFG={'series_id':'TEST','value_transform':'percent_change_1'}

def test_real_cohort_and_qualified_subjects(completed):
    r=op.read(completed/'unit_binding_report.json');assert (r['versions'],r['sources'],r['scoped_evidence_cells'],r['business_series_subject_mismatch_versions'])==(9,3,8,2)
    assert (r['cutoffs'],r['currency_rows'],r['pair_rows'],r['reconstructed_cell_event_cutoff_rows'])==(8,168,544,19)
    rows=op.read(completed/'unit_source_bindings.json');business=[r for r in rows if r['original_event_series_id']=='business_sales']
    assert len(business)==2
    for r in business:
        assert r['original_actual']==0.8 and r['original_event_series_id']=='business_sales'
        cells={c['component_id']:c for c in r['adapter']['cells']}
        assert cells['census_business_inventories_monthly_change']['actual']=='0.8'
        assert cells['census_business_sales_monthly_change']['actual']=='0.3'
        assert r['adapter']['native_reproduction']['native_actual']=='+0.8'
    assert all(not c['forecast_admission'] and not c['raw_response_binding_verified'] for r in rows for c in r['adapter']['cells'])

def test_housing_annual_rate_level_not_annual_change(native):
    r=core.census_binding(fields('housing_starts'),native);c=r['cells'][0]
    assert c['actual']=='-2.6' and c['reference']=={'month':'2026-08','comparison_month':'2026-07','comparison':'month_over_month'}
    assert c['level_annualized'] and not c['comparison_annualized'] and c['reported_revised_prior_level']=='1309000'

@pytest.mark.parametrize('mutation,reason',[
    ('direction','retail_subject_value_or_period_mismatch'),('footer','retained_native_footer_mismatch'),
    ('duplicate','missing_or_ambiguous_subject_sentence'),('comparison','missing_or_ambiguous_subject_sentence'),
    ('duplicate_footer','ambiguous_current_reference_footer'),('reference','retained_native_footer_mismatch')])
def test_census_conflicting_evidence_abstains(native,mutation,reason):
    f=fields('retail_sales');t=f['summary']
    if mutation=='direction':f['summary']=t.replace('up 1.2','down 1.2')
    elif mutation=='footer':f['summary']=t.replace('August 2026: +1.2','August 2026: +9.2')
    elif mutation=='duplicate':f['summary']=t.split('August 2026:')[0]+t
    elif mutation=='comparison':f['summary']=t.replace('previous month','previous year')
    elif mutation=='duplicate_footer':f['summary']=t+' August 2026: +1.2 % Change'
    elif mutation=='reference':f['reference_period']='July 2026'
    with pytest.raises(ValueError,match=reason):core.census_binding(f,native)

def test_business_shared_period_cannot_cross_unrelated_sentence(native):
    f=fields('business_sales');f['summary']=f['summary'].replace('U.S. total business sales','A separate historical example follows. U.S. total business sales')
    with pytest.raises(ValueError,match='business_subject_sentences_not_adjacent'):core.census_binding(f,native)

def test_business_footer_must_explicitly_bind_inventories(native):
    f=fields('business_sales');f['summary']=f['summary'].replace('Change in Inventories','Change in Sales')
    with pytest.raises(ValueError,match='inventory_footer_subject_required'):core.census_binding(f,native)

def test_business_negative_sales_keeps_separate_inventory(native):
    f=fields('business_sales');f['summary']=f['summary'].replace('up 0.3','down 0.3')
    cells=core.census_binding(f,native)['cells'];assert [c['actual'] for c in cells]==['0.8','-0.3']

@pytest.mark.parametrize('field,value',[('source_verified',False),('source_direct',False),('headline','Unrelated release'),('source_id','unknown')])
def test_source_and_title_scope_cannot_be_bypassed(native,field,value):
    f=fields('retail_sales');f[field]=value
    with pytest.raises(ValueError,match='scope_rejected|title_series_mismatch'):core.census_binding(f,native)

def test_no_inferred_bls_or_statcan_qualification(completed):
    rows=op.read(completed/'unit_source_bindings.json')
    for r in rows:
        if r['source_id']=='bls_major_timeseries_batch_v1':
            a=r['adapter'];assert a['status']=='raw_bls_levels_unavailable' and not a['cells'] and not a['current_configuration_is_historical_proof']
        if r['source_id']=='statcan_daily_releases':
            a=r['adapter'];assert a['native_actual']=='0.3' and a['comparison']=='unresolved' and not a['cells']

def test_original_bls_calculation_reproduces_gap_and_duplicate_defects(native):
    for rows in [[levels()[0],{**levels()[1],'period':'M06'},{**levels()[2],'period':'M05'}],
                 [levels()[0],{**levels()[1],'period':'M08'},levels()[2]]]:
        original=native.original_bls_calculation(copy.deepcopy(rows),CFG);assert original['actual']==pytest.approx(10)
        with pytest.raises(ValueError,match='nonadjacent_bls_months|duplicate_bls_month'):core.guarded_bls(rows,CFG,'TEST','TEST')

def test_bls_fixed_precision_reference_invariant_and_annual_average(native):
    rows=levels();rows.append({'year':'2026','period':'M13','value':'99999'})
    with localcontext() as ctx:
        ctx.prec=5;a=core.guarded_bls(rows,CFG,'TEST','TEST')
    with localcontext() as ctx:
        ctx.prec=70;b=core.guarded_bls(rows,CFG,'TEST','TEST')
    assert a==b and Decimal(a['actual'])==10 and a['reference_month']=='2026-08'
    assert Decimal(a['reported_previous_change'])==pytest.approx(Decimal(100)/Decimal(9))
    assert native.original_bls_calculation(rows,CFG)['actual']==pytest.approx(float(a['actual']))

def test_bls_year_rollover_and_future_unrelated_observation():
    rows=[{'year':y,'period':p,'value':v} for y,p,v in [('2026','M01','110'),('2025','M12','100'),('2025','M11','90')]]
    assert core.guarded_bls(rows,CFG,'TEST','TEST')['previous_month']=='2025-12'
    with pytest.raises(ValueError,match='nonadjacent_bls_months'):core.guarded_bls(rows+[{'year':'2026','period':'M03','value':'120'}],CFG,'TEST','TEST')

@pytest.mark.parametrize('value',['0','-1','NaN','inf','1,000','100junk',True,None,'9'*65])
def test_bls_invalid_denominator_never_falls_back(value):
    rows=levels();rows[1]['value']=value
    with pytest.raises(ValueError,match='invalid_numeric_value|nonpositive_bls_index_level'):core.guarded_bls(rows,CFG,'TEST','TEST')

def test_bls_identity_transform_and_insufficient_levels():
    with pytest.raises(ValueError,match='bls_series_identity_mismatch'):core.guarded_bls(levels(),CFG,'TEST','OTHER')
    with pytest.raises(ValueError,match='unsupported_bls_transform'):core.guarded_bls(levels(),{**CFG,'value_transform':'level'},'TEST','TEST')
    with pytest.raises(ValueError,match='three_month_levels_required'):core.guarded_bls(levels()[:2],CFG,'TEST','TEST')

def test_exact_spans_and_no_hidden_semantic_relabeling(completed):
    for r in op.read(completed/'unit_source_bindings.json'):
        a=r['adapter']
        for c in a['cells']:
            h=c.pop('evidence_sha256');assert core.fingerprint(c)==h
            for s in c['evidence']:assert a['normalized_text'][s['start']:s['end']]==s['quote']
        if a['cells']:assert a['original_numeric_fields_modified'] is False

def test_actual_source_asof_preserves_original_events_and_no_activation(completed):
    original=op.read(INPUTS/'numeric_source_asof.json');output=op.read(completed/'unit_source_asof.json')
    for old,new in zip(original,output):
        assert [e['original_numeric_event'] for e in new['events']]==old['events']
        for e in new['events']:
            assert not e['runtime_feature_cells'] and e['adapter_available_epoch'] is None and not e['forecast_admission']
            if e['reconstructed_component_cells']:assert e['original_numeric_event']['status']=='retained_numeric_observation_ready'

def test_consumer_newer_missing_or_unready_version_never_reuses_old_cell(completed):
    index=core.evidence_index(op.read(completed/'unit_source_bindings.json'));material={m['numeric_cache_key']:m for m in op.read(INPUTS/'numeric_material_cache.json')}
    event=next(e['original_numeric_event'] for s in op.read(completed/'unit_source_asof.json') for e in s['events'] if e['reconstructed_component_cells'])
    event['status']='numeric_clock_not_ready';assert not core.join_event(event,index,material,2e9)['reconstructed_component_cells']
    event['status']='retained_numeric_observation_ready';event['selected_numeric_version_ids']=['new_unknown_version'];assert not core.join_event(event,index,material,2e9)['reconstructed_component_cells']

def test_all68_shared_states_and_no_currency_arithmetic(completed):
    index={}
    for snap in op.read(completed/'unit_currency_states.json'):
        for s in snap['states']:
            h=s.pop('state_sha256');assert h==core.fingerprint(s);index[h]=s
    for p in op.read(completed/'unit_pair_views.json'):
        a,b=index[p['base_state_sha256']],index[p['quote_state_sha256']]
        assert p['pair']==a['currency']+'_'+b['currency'] and a['cutoff_epoch']==b['cutoff_epoch']==p['cutoff_epoch']
        assert p['base_minus_quote_numeric_value'] is None and not p['shared_events_are_independent_votes']

def test_input_recipe_scope_and_false_completion_refused(tmp_path,completed):
    bad=blobs();bad['target_versions.json']+=b' '
    with pytest.raises(ValueError,match='unit_binding_input_pin_mismatch'):core.build(bad)
    runs=tmp_path/'runs';assert op.operate('verify',RECIPE,op.sha(RECIPE),INPUTS,runs)['status']=='review_required'
    assert op.operate('unsupported',RECIPE,op.sha(RECIPE),INPUTS,runs)['status']=='review_required'
    assert op.operate('run',RECIPE,'0'*64,INPUTS,runs)['status']=='review_required'
    copied=tmp_path/'inputs';shutil.copytree(INPUTS,copied);(copied/'target_versions.json').write_text('[]')
    assert op.operate('run',RECIPE,op.sha(RECIPE),copied,runs)['status']=='review_required'
    poisoned=tmp_path/'poisoned';shutil.copytree(completed,poisoned/op.read(RECIPE)['run_id']);(poisoned/op.read(RECIPE)['run_id']/'unit_binding_report.json').write_text('{}')
    assert op.operate('verify',RECIPE,op.sha(RECIPE),INPUTS,poisoned)['status']=='review_required'

def test_crash_resume_exact_outputs(tmp_path,completed):
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_unit_binding_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','3'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91;r=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified'
    assert all(op.sha(Path(r['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
