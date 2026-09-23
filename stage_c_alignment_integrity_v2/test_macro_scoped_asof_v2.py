import copy,hashlib,json,os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_scoped_asof_v2 as core
import macro_scoped_asof_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_SCOPED_ASOF_INPUTS',str(ROOT/'evidence/timed_20260922_154551/macro_scoped_asof/inputs')))
RECIPE=ROOT/'MACRO_SCOPED_ASOF_OPERATOR_RECIPE.json'

@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_SCOPED_ASOF_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('asof')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

def fixture(action='hold',vid='v1',event='e1',available=1000,published=900):
    text='Synthetic '+str(action);key=core.fingerprint(text);sha=hashlib.sha256(text.encode()).hexdigest()
    t={'cache_key':key,'text_sha256':sha,'retained_text':text}
    r={'cache_key':key,'text_sha256':sha,'action_candidate':action,'status':'synthetic_candidate','action_evidence':[],'claim_evidence':[],
       'issuer_identity_verified':False,'policy_fact_admission':False,'forecast_admission':False}
    b={'version_id':vid,'event_id':event,'cache_key':key,'content_sha256':'synthetic','currencies':['EUR'],'source_id':'source',
       'kind':'retained_parsed_detail','listing_bootstrap':False,'published_time_inferred':False,'policy_document_class':'decision',
       'observations':[{'observation_id':vid+'o','sequence':1,'known_epoch':available,'available_epoch':available,'published_epoch':published,'clock_status':'valid_attested_observation'}]}
    return b,t,r

def state(bs,ts,rs,cutoff=1100):return core.snapshot(bs,{t['cache_key']:t for t in ts},{r['cache_key']:r for r in rs},cutoff,['EUR'])[1][0]

def test_future_revision_and_envelope_cannot_change_earlier_state():
    b,t,r=fixture();later,lt,lr=fixture('cut','v2',available=2000)
    before=state([b],[t],[r]);b['observations'].append({**b['observations'][0],'observation_id':'later','known_epoch':2000,'available_epoch':2000,'published_epoch':1900})
    assert state([b,later],[t,lt],[r,lr])==before
    assert state([b,later],[t,lt],[r,lr],2100)['linguistic_action_candidate']=='cut'

@pytest.mark.parametrize('field,value',[('known_epoch',1200),('available_epoch',1200),('clock_status','unproven_observation')])
def test_both_receipt_clocks_and_attestation_gate(field,value):
    b,t,r=fixture();b['observations'][0][field]=value
    assert state([b],[t],[r])['visible_events']==0

def test_same_clock_material_ambiguity_blocks_candidate_and_transition():
    a,t,r=fixture();b,bt,br=fixture('cut','v2')
    x=state([a,b],[t,bt],[r,br]);assert x==state([b,a],[t,bt],[r,br])
    assert x['linguistic_action_candidate'] is None and x['categorical_transitions']==[] and x['ambiguous_version_ids']==['v1','v2']

def test_same_clock_publication_conflict_does_not_choose_sequence():
    b,t,r=fixture();b['observations'].append({**b['observations'][0],'sequence':2,'published_epoch':850})
    x=state([b],[t],[r]);assert x['exclusions']=={'unresolved_publication_clock':1}

@pytest.mark.parametrize('change,reason',[
 ({'kind':'headline_or_listing_only'},'headline_or_listing_only'),
 ({'listing_bootstrap':True},'listing_bootstrap'),
 ({'published_time_inferred':True},'inferred_publication_clock'),
])
def test_context_screens_keep_coverage(change,reason):
    b,t,r=fixture();b.update(change);x=state([b],[t],[r])
    assert x['visible_events']==1 and x['eligible_events']==0 and x['exclusions']=={reason:1}

@pytest.mark.parametrize('published,reason',[(1200,'future_publication_clock'),(-4000000,'stale_context'),(None,'unresolved_publication_clock')])
def test_publication_time_screens(published,reason):
    b,t,r=fixture(published=published);assert state([b],[t],[r])['exclusions']=={reason:1}

def test_duplicate_text_counts_once_but_distinct_event_provenance_survives():
    b,t,r=fixture();dup=copy.deepcopy(b);dup.update(version_id='v2',event_id='e2')
    x=state([b,dup],[t],[r]);assert x['eligible_events']==2 and x['unique_texts']==1
    assert x['candidate_action_counts_by_unique_text']=={'hold':1} and len(x['evidence_references'])==2

def test_only_unique_ordered_same_source_class_has_categorical_transition():
    a,t,r=fixture('cut','old','old',900,800);b,bt,br=fixture('hold','new','new',1000,950)
    x=state([a,b],[t,bt],[r,br]);assert x['categorical_transitions'][0]['categorical_action_transition']==['cut','hold']
    assert x['linguistic_action_candidate'] is None and x['stance_change'] is None
    dup=copy.deepcopy(a);dup.update(version_id='tie',event_id='tie')
    assert state([a,b,dup],[t,bt],[r,br])['categorical_transitions'][0]['categorical_action_transition'] is None
    b['source_id']='different';assert all(t['categorical_action_transition'] is None for t in state([a,b],[t,bt],[r,br])['categorical_transitions'])

def test_span_identity_and_admission_tampering_rejected():
    b,t,r=fixture();core.validate_caches([t],[r],[b])
    r['action_evidence']=[{'start':0,'end':3,'context_start':0,'context_end':len(t['retained_text']),'quote':'bad'}]
    with pytest.raises(ValueError,match='scoped_evidence_identity_mismatch'):core.validate_caches([t],[r],[b])
    r['action_evidence']=[];r['forecast_admission']=True
    with pytest.raises(ValueError,match='premature_semantic_admission'):core.validate_caches([t],[r],[b])

def test_real_population_and_pair_state_identity(completed):
    report=op.read(completed/'scoped_asof_report.json');assert report['versions']==2696 and report['unique_texts']==545
    assert report['cutoffs']==8 and report['pair_rows']==544 and report['policy_facts_admitted']==report['forecast_features_admitted']==0
    states={}
    for snap in op.read(completed/'scoped_currency_states.json'):
        for s in snap['states']:
            digest=s.pop('state_sha256');assert core.fingerprint(s)==digest;states[digest]=s
            assert s['policy_stance'] is None and s['stance_change'] is None and not s['forecast_admission']
    for p in op.read(completed/'scoped_pair_views.json'):
        a,b=states[p['base_state_sha256']],states[p['quote_state_sha256']]
        assert p['pair']==a['currency']+'_'+b['currency'] and p['cutoff_epoch']==a['cutoff_epoch']==b['cutoff_epoch']
        assert p['directional_difference'] is None and not p['forecast_admission'] and not p['can_place_orders']

def test_crash_resume_exact_outputs_and_predecessor_pin(tmp_path,completed):
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};blobs['scoped_text_cache.json']+=b' '
    with pytest.raises(ValueError,match='scoped_predecessor_pin_mismatch'):core.build(blobs)
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_scoped_asof_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','4'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91
    result=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert result['status']=='completed_verified'
    assert all(op.sha(Path(result['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
