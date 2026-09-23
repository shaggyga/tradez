import copy,hashlib,json,os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_detail_representation_v2 as core
import macro_detail_representation_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_DETAIL_REPRESENTATION_INPUTS',str(ROOT/'evidence/timed_20260922_194458/macro_detail_repair/inputs')))
RECIPE=ROOT/'MACRO_DETAIL_REPRESENTATION_OPERATOR_RECIPE.json'

@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_DETAIL_REPRESENTATION_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('detail')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

def fixture(action='hold',vid='v1',available=1000,detail=True,event='e1',text=None):
    text=text if text is not None else 'Synthetic '+str(action);key=core.fingerprint(text);digest=hashlib.sha256(text.encode()).hexdigest()
    t={'cache_key':key,'text_sha256':digest,'retained_text':text}
    s={'cache_key':key,'text_sha256':digest,'action_candidate':action,'status':'synthetic_candidate','action_evidence':[],'claim_evidence':[],
       'issuer_identity_verified':False,'policy_fact_admission':False,'forecast_admission':False}
    b={'version_id':vid,'event_id':event,'cache_key':key,'content_sha256':vid+'content','currencies':['EUR'],'source_id':'source',
       'kind':'retained_parsed_detail' if detail else 'headline_or_listing_only','listing_bootstrap':False,'published_time_inferred':False,'policy_document_class':'decision',
       'observations':[{'observation_id':vid+'o','sequence':1,'known_epoch':available,'available_epoch':available,'published_epoch':900,'clock_status':'valid_attested_observation'}]}
    m={'version_id':vid,'content_sha256':b['content_sha256'],'detail_enriched':detail,'verified_retraction_evidence':None}
    return b,t,s,m

def snapshot(rows,cutoff=1500):
    return core.detail_snapshot([x[0] for x in rows],{x[1]['cache_key']:x[1] for x in rows},{x[2]['cache_key']:x[2] for x in rows},
        {x[3]['version_id']:x[3] for x in rows},cutoff,['EUR'])

def test_later_listing_preserves_detail_but_retains_raw_and_uncertainty():
    first=fixture();listing=fixture(None,'listing',1200,False,text='Short listing')
    reps,states=snapshot([first,listing]);r=reps[0];s=states[0]
    assert r['latest_raw_version_ids']==['listing'] and r['detail_version_ids']==['v1']
    assert r['detail_is_separate_from_latest_raw'] and r['replacement_semantics']=='unresolved_later_raw_representation'
    assert s['linguistic_action_candidate']=='hold' and not s['policy_fact_admission'] and not s['forecast_admission']
    assert s['separate_raw_detail_event_ids']==['e1'] and not r['publisher_correction_or_retraction_absence_proven']

def test_later_detail_never_backdates_into_listing_only_history():
    listing=fixture(None,'listing',1000,False);detail=fixture('cut','detail',2000)
    r,s=snapshot([listing,detail],1999);assert not r[0]['detail_version_ids'] and s[0]['linguistic_action_candidate'] is None
    assert snapshot([listing,detail],2000)[1][0]['linguistic_action_candidate']=='cut'

def test_newer_true_detail_supersedes_even_when_shorter_or_empty():
    old=fixture('hold',text='Long detail '*100);new=fixture('cut','new',1200,text='Short')
    r,s=snapshot([old,new]);assert r[0]['detail_version_ids']==['new'] and s[0]['linguistic_action_candidate']=='cut'
    empty=fixture(None,'empty',1300,text='');r,s=snapshot([old,new,empty])
    assert r[0]['detail_version_ids']==['empty'] and r[0]['detail_context']['exclusion']=='missing_text'
    assert s[0]['linguistic_action_candidate'] is None

def test_future_translation_and_envelope_do_not_change_earlier_detail_view():
    old=fixture();later=fixture('cut','translation',2000);before=snapshot([old])
    old=copy.deepcopy(old);old[0]['observations'].append({**old[0]['observations'][0],'known_epoch':2000,'available_epoch':2000,'published_epoch':1950,'sequence':2})
    assert snapshot([old,later])==before

@pytest.mark.parametrize('field,value',[('known_epoch',2000),('available_epoch',2000),('clock_status','unproven_observation')])
def test_attested_both_clock_gate(field,value):
    row=fixture();row[0]['observations'][0][field]=value
    r,s=snapshot([row]);assert not r and s[0]['visible_events']==0

def test_conflicting_same_clock_details_abstain_order_independently():
    a,b=fixture(),fixture('cut','v2');x=snapshot([a,b]);assert x==snapshot([b,a])
    assert x[0][0]['detail_context']['version_status']=='ambiguous_same_clock_material'
    assert x[1][0]['linguistic_action_candidate'] is None and x[1][0]['categorical_transitions']==[]

def test_listing_at_same_clock_does_not_override_detail_and_raw_ambiguity_survives():
    a,b=fixture(),fixture(None,'listing',1000,False);r,s=snapshot([a,b])
    assert r[0]['raw_version_status']=='ambiguous_same_clock_material' and r[0]['detail_version_ids']==['v1']
    assert s[0]['linguistic_action_candidate']=='hold' and not s[0]['forecast_admission']

@pytest.mark.parametrize('change,reason',[({'kind':'calendar_or_schedule_listing'},'calendar_or_schedule_listing'),
    ({'listing_bootstrap':True},'listing_bootstrap'),({'published_time_inferred':True},'inferred_publication_clock')])
def test_detail_flag_never_bypasses_context_screens(change,reason):
    row=fixture();row[0].update(change);r,s=snapshot([row]);assert r[0]['detail_context']['exclusion']==reason
    assert s[0]['eligible_events']==0 and s[0]['linguistic_action_candidate'] is None

def test_publication_and_staleness_gates_survive():
    row=fixture();row[0]['observations'][0]['published_epoch']=1600
    assert snapshot([row])[0][0]['detail_context']['exclusion']=='future_publication_clock'
    row[0]['observations'][0]['published_epoch']=-4000000
    assert snapshot([row])[0][0]['detail_context']['exclusion']=='stale_context'

def test_duplicate_text_shares_count_and_input_not_mutated():
    a=fixture();b=fixture(vid='v2',event='e2');before=copy.deepcopy([a,b]);r,s=snapshot([a,b])
    assert [a,b]==before and s[0]['unique_texts']==1 and s[0]['eligible_events']==2
    assert s[0]['candidate_action_counts_by_unique_text']=={'hold':1}

def test_retraction_assertion_cannot_be_ignored_or_implicitly_trusted():
    b,t,s,m=fixture();m['verified_retraction_evidence']={'status':'verified'}
    with pytest.raises(ValueError,match='unsupported_retraction_evidence_requires_review'):core.validate_metadata([b],[m])
    m['verified_retraction_evidence']=None;m['content_sha256']='drift'
    with pytest.raises(ValueError,match='representation_metadata_content_mismatch'):core.validate_metadata([b],[m])

def test_full_population_shared_pairs_and_recovered_action_evidence(completed):
    report=op.read(completed/'detail_representation_report.json');assert report['versions']==2696 and report['unique_texts']==545
    assert report['detail_enriched_versions']==220 and report['detail_flags_by_context_kind']=={'calendar_or_schedule_listing':18,'retained_parsed_detail':202}
    assert report['pair_rows']==544 and report['currency_rows']==168
    assert report['currency_action_candidate_rows']>report['prior_raw_selector_action_rows']==0
    assert report['forecast_features_admitted']==report['base_models_fitted']==0
    states={}
    for snap in op.read(completed/'detail_currency_states.json'):
        for s in snap['states']:
            digest=s.pop('state_sha256');assert core.fingerprint(s)==digest;states[digest]=s
            assert not s['forecast_admission'] and s['historical_extraction_ready_epoch'] is None
    for p in op.read(completed/'detail_pair_views.json'):
        a,b=states[p['base_state_sha256']],states[p['quote_state_sha256']]
        assert p['pair']==a['currency']+'_'+b['currency'] and p['cutoff_epoch']==a['cutoff_epoch']==b['cutoff_epoch']
        assert p['directional_difference'] is None and not p['forecast_admission']
    sources={r['source_id'] for s in states.values() if s['linguistic_action_candidate'] for r in s['evidence_references'] if r['action_candidate']}
    assert {'boe_news','tcmb_press'}<=sources

def test_external_pin_and_corrupted_input_refused(tmp_path):
    assert op.operate('run',RECIPE,'0'*64,INPUTS,tmp_path/'bad')['status']=='review_required'
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};blobs['source_ledger.py']+=b' '
    with pytest.raises(ValueError,match='detail_repair_input_pin_mismatch'):core.build(blobs)

def test_process_death_resume_all_outputs(tmp_path,completed):
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_detail_representation_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','3'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91
    result=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert result['status']=='completed_verified'
    assert all(op.sha(Path(result['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)

def test_live_writer_is_not_taken_over(tmp_path):
    from publication import RunPublisher
    recipe=op.read(RECIPE);writer=RunPublisher(tmp_path,recipe['run_id'],op.identity_for(recipe));writer.acquire()
    try:
        assert op.operate('run',RECIPE,op.sha(RECIPE),INPUTS,tmp_path)['status']=='review_required'
        assert op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,tmp_path)['status']=='review_required'
    finally:writer.release()

def test_completed_payload_corruption_is_not_false_success(tmp_path,completed):
    import shutil
    target=tmp_path/op.read(RECIPE)['run_id'];shutil.copytree(completed,target)
    (target/'detail_representation_report.json').write_text('{}',encoding='utf-8')
    result=op.operate('verify',RECIPE,op.sha(RECIPE),INPUTS,tmp_path)
    assert result['status']=='review_required'
