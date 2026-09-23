import copy,json,os,shutil,subprocess,sys,re
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_meter_v2 as core
import macro_meter_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_METER_INPUTS',str(ROOT/'evidence/four_next_20260923_040615/meter/inputs')))
RECIPE=ROOT/'MACRO_METER_OPERATOR_RECIPE.json'
@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_METER_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('meter')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])
def parent():return {'currency':'USD','cutoff_epoch':100,'state_sha256':'parent','raw_visible_events':3,'exclusions':{},'linguistic_action_candidate':None,'categorical_transitions':[]}
def doc(event,text,concepts):return {'event_id':event,'text_sha256':text,'concepts':concepts,'document_reference_sha256':event+'-'+text,'version_ids':[event],'source_id':'source','age_seconds':50}
def test_exact_documents_and_spans_roundtrip(completed):
    rows=op.read(completed/'meter_document_evidence.json');assert rows
    for d in rows:
        assert core.fingerprint({k:v for k,v in d.items() if k!='document_reference_sha256'})==d['document_reference_sha256']
        for e in d['evidence']:assert d['retained_text'][e['start']:e['end']]==e['quote']
        assert not d['forecast_admission'] and d['historical_extraction_ready_epoch'] is None
def test_duplicate_text_and_repeated_phrase_do_not_multiply_votes():
    ds=[doc('a','text1',['action:hold']),doc('b','text1',['action:hold']),doc('c','text2',[])]
    s=core.meter_state(parent(),ds,{'source','missing'});assert (s['document_version_count'],s['unique_event_count'],s['unique_text_count'],s['repeated_text_event_count'])==(3,3,2,1)
    assert s['concepts'][0]['normalized_presence']==0.5 and s['concepts'][0]['unique_text_count']==1
    assert len(s['concepts'][0]['document_references'])==2 and s['configured_sources_without_eligible_context']==['missing']
    with pytest.raises(ValueError,match='conflicting_extraction'):core.meter_state(parent(),[ds[0],doc('b','text1',['action:hike'])],set())
def test_missing_unmatched_and_neutral_language_differ():
    assert core.meter_state(parent(),[],set())['coverage_status']=='no_usable_source'
    assert core.meter_state(parent(),[doc('a','text',[])],set())['coverage_status']=='usable_source_no_supported_concept'
    hold=core.meter_state(parent(),[doc('a','text',['action:hold'])],set());assert hold['coverage_status']=='supported_hold_language' and hold['policy_stance'] is None
    assert core.meter_state(parent(),[doc('a','text',['action:hike','action:hold'])],set())['coverage_status']=='supported_concept_language'
def test_duplicate_events_refused():
    d=doc('a','text',[])
    with pytest.raises(ValueError,match='duplicate_event'):core.meter_state(parent(),[d,d],set())
def source_case():
    text='The bank held rates. The bank held rates.';key='key'
    e={'action':'hold','assertion_status':'asserted_linguistic_candidate','start':9,'end':19,'quote':'held rates','context_start':0,'context_end':20,'guard_reasons':[]}
    s={'action_evidence':[e,{**e,'start':30,'end':40,'context_start':21,'context_end':41}],'claim_evidence':[],'action_candidate':'hold','forecast_admission':False,'policy_fact_admission':False,'issuer_identity_verified':False,'text_sha256':core.hashlib.sha256(text.encode()).hexdigest(),'language_scope':'synthetic'}
    t={'retained_text':text,'text_sha256':s['text_sha256']};v={'cache_key':key,'event_id':'event','source_id':'source','observations':[{'clock_status':'valid_attested_observation','available_epoch':5}]}
    r={'cache_key':key,'text_sha256':s['text_sha256'],'scoped_evidence_sha256':core.fingerprint(s),'published_epoch':4,'version_ids':['version'],'event_id':'event','source_id':'source'}
    return r,{key:t},{key:s},{'version':v}
def test_within_document_boilerplate_count_cap():
    args=source_case();d=core.document(*args,10,lambda x:x);assert d['concepts']==['action:hold'] and len(d['evidence'])==2
def test_guarded_phrase_does_not_count():
    r,t,s,v=source_case()
    for e in s['key']['action_evidence']:e['guard_reasons']=['negated'];e['assertion_status']='rejected_or_unresolved'
    s['key']['action_candidate']=None;r['scoped_evidence_sha256']=core.fingerprint(s['key']);d=core.document(r,t,s,v,10,lambda x:x);assert d['concepts']==[]
@pytest.mark.parametrize('field,value',[('source_id','wrong'),('event_id','wrong'),('published_epoch',11),('version_ids',['missing']),('scoped_evidence_sha256','wrong'),('text_sha256','wrong')])
def test_source_document_time_and_evidence_identity_refused(field,value):
    r,t,s,v=source_case();r[field]=value
    with pytest.raises(ValueError):core.document(r,t,s,v,10,lambda x:x)
def test_future_version_and_wrong_span_refused():
    r,t,s,v=source_case();v['version']['observations'][0]['available_epoch']=11
    with pytest.raises(ValueError,match='not_available'):core.document(r,t,s,v,10,lambda x:x)
    r,t,s,v=source_case();s['key']['action_evidence'][0]['quote']='false';r['scoped_evidence_sha256']=core.fingerprint(s['key'])
    with pytest.raises(ValueError,match='quote_mismatch'):core.document(r,t,s,v,10,lambda x:x)
def test_shared_all68_pair_references_and_normalization(completed):
    states=op.read(completed/'meter_currency_states.json');pairs=op.read(completed/'meter_pair_views.json');docs={d['document_reference_sha256']:d for d in op.read(completed/'meter_document_evidence.json')}
    assert len(states)==168 and len(pairs)==544;index={s['state_sha256']:s for s in states}
    for s in states:
        assert core.fingerprint({k:v for k,v in s.items() if k!='state_sha256'})==s['state_sha256']
        assert all(k in docs for k in s['document_references'])
        for c in s['concepts']:assert 0<c['normalized_presence']<=1 and c['normalization_denominator']==s['unique_text_count']
        assert s['policy_stance'] is s['stance_change'] is s['uncertainty_probability'] is None
    for p in pairs:
        a,b=index[p['base_state_sha256']],index[p['quote_state_sha256']];assert p['pair']==a['currency']+'_'+b['currency'] and p['cutoff_epoch']==a['cutoff_epoch']==b['cutoff_epoch']
        assert p['directional_difference'] is None and not p['shared_documents_are_independent_votes']
def test_html_escapes_untrusted_documents_and_roundtrips(completed):
    payload='</script><img src=x onerror=alert(1)> & secrets';html=core.render_html([], [{'retained_text':payload}])
    assert payload not in html and '\\u003c/script\\u003e' in html
    data=json.loads(re.search(r'<script id="meter-data" type="application/json">(.*?)</script>',html,re.S)[1]);assert data['documents'][0]['retained_text']==payload
    page=(completed/'meter_view.html').read_text(encoding='utf-8');assert 'innerHTML' not in page and 'fetch(' not in page and 'src=' not in page
    embedded=json.loads(re.search(r'<script id="meter-data" type="application/json">(.*?)</script>',page,re.S)[1])
    assert embedded['states']==op.read(completed/'meter_currency_states.json') and embedded['documents']==op.read(completed/'meter_document_evidence.json')
def test_pin_false_completion_and_corruption_refused(tmp_path,completed):
    b={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};b['detail_currency_states.json']+=b' '
    with pytest.raises(ValueError,match='meter_input_pin_mismatch'):core.build(b)
    assert op.operate('verify',RECIPE,op.sha(RECIPE),INPUTS,tmp_path/'runs')['status']=='review_required'
    assert op.operate('other',RECIPE,op.sha(RECIPE),INPUTS,tmp_path/'runs')['status']=='review_required'
    assert op.operate('run',RECIPE,'0'*64,INPUTS,tmp_path/'runs')['status']=='review_required'
    poisoned=tmp_path/'bad';shutil.copytree(completed,poisoned/op.read(RECIPE)['run_id']);(poisoned/op.read(RECIPE)['run_id']/'meter_view.html').write_text('bad')
    assert op.operate('verify',RECIPE,op.sha(RECIPE),INPUTS,poisoned)['status']=='review_required'
def test_crash_resume_outputs(tmp_path,completed):
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_meter_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','2'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91;r=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified'
    assert all(op.sha(Path(r['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
