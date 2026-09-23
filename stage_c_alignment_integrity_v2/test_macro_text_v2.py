import ast
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import pytest

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
import macro_text_operator_v2 as op
from macro_text_layer_v2 import extract,documents,asof,pair_view,epoch,AXES
from publication import RunPublisher
INPUTS=Path(os.environ.get('FOREX_MACRO_TEXT_INPUTS',str(ROOT/'evidence/macro_text_layer_20260922/inputs')))
RECIPE=ROOT/'MACRO_TEXT_OPERATOR_RECIPE.json'
RULES=op.read(ROOT/'MACRO_TEXT_RULES_V2.json')['rules']


def call(action,runs):return op.operate(action,RECIPE,op.sha(RECIPE),INPUTS,runs)


@pytest.fixture(scope='session')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_TEXT_RUNS')
    runs=Path(supplied) if supplied else tmp_path_factory.mktemp('macrotext')
    result=call('verify' if supplied else 'run',runs)
    assert result['status']=='completed_verified',result
    return Path(result['run_path'])


def fixture(text='The policy rate will be raised.',currency='NOK',published='2026-08-10T10:00:00Z',event='a'):
    row=copy.deepcopy(op.read(INPUTS/'baseline.json')['baselines'][0])
    row.update(currency=currency,source_id='synthetic',document_class='decision_statement',event_id=event,
               summary=text,published_utc=published,known_utc=published,detail_available_utc=published)
    return row


def baseline(rows):return {'created_utc':'2026-08-01T00:00:00Z','research_only':True,'can_execute':False,'baselines':rows}


def test_exact_predecessor_rules_recovered_without_import():
    tree=ast.parse((INPUTS/'predecessor.py').read_text(encoding='utf-8'))
    f=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='policy_stance_snapshot')
    expected=[]
    for n in f.body:
        if isinstance(n,ast.Assign) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id=='_matched_score':
            for pattern,weight,label in ast.literal_eval(n.value.args[1]):expected.append({'axis':n.targets[0].elts[0].id,'pattern':pattern,'weight':weight,'label':label})
    assert RULES==expected


def test_hand_oracle_spans_whitespace_and_conditionality():
    text='The policy\nrate will be raised. Inflation has been lower than expected.'
    result=extract(text,RULES)
    assert result['axes']['guidance']['value']==1
    assert result['axes']['inflation']['value']==-.65
    assert all(text[e['start']:e['end']]==e['quote'] for e in result['evidence'])
    conditional=extract('If inflation persists, the policy rate will be raised.',RULES)
    assert conditional['axes']['guidance']['value']==.35
    assert conditional['axes']['guidance']['conditional_only']


@pytest.mark.parametrize('text,status',[
    ('It is not true that the policy rate will be raised.','unresolved_phrases'),
    ('The speaker denied that the policy rate will be raised.','unresolved_phrases'),
    ('"The policy rate will be raised."','unresolved_phrases'),
    ('The policy rate will be raised. We will cut the policy rate.','conflicting_phrases'),
    ('Meeting minutes are available.','no_supported_phrase')])
def test_no_false_neutral_or_last_match_wins(text,status):
    axis=extract(text,RULES)['axes']['guidance']
    assert axis['value'] is None and axis['status']==status


def test_missing_text_is_not_observed_unmatched():
    assert extract('',RULES)['status']=='missing_text'
    assert extract('Meeting minutes.',RULES)['status']=='retained_text_extracted'


def test_conditional_cut_and_opposing_weights():
    assert extract('It may still be necessary to cut.',RULES)['axes']['guidance']['value']==-.35
    assert extract('It will likely be necessary to raise. It may still be necessary to cut.',RULES)['axes']['guidance']['value'] is None


def test_creation_and_body_clocks_are_both_required():
    b=baseline([fixture()]);b['created_utc']='2026-08-12T00:00:00Z'
    docs=documents(b,RULES)
    assert asof(docs,epoch('2026-08-11T00:00:00Z'),['NOK'])[0]['status']=='no_available_context'
    b['baselines'][0]['detail_available_utc']='2026-08-13T00:00:00Z'
    assert asof(documents(b,RULES),epoch('2026-08-12T12:00:00Z'),['NOK'])[0]['status']=='no_available_context'


def test_future_revision_and_translation_cannot_change_earlier_view():
    first=fixture();docs=documents(baseline([first]),RULES);cutoff=epoch('2026-08-11T00:00:00Z')
    later=copy.deepcopy(first);later.update(summary='We will cut the policy rate.',detail_available_utc='2026-08-12T00:00:00Z')
    changed=documents(baseline([first,later]),RULES)
    assert asof(changed,cutoff,['NOK'])==asof(docs,cutoff,['NOK'])
    assert asof(changed,epoch('2026-08-13T00:00:00Z'),['NOK'])[0]['documents'][0]['axes']['guidance']['value']==-1


def test_comparable_prior_change_and_mirror_dedup():
    old=fixture('It may still be necessary to raise.')
    new=fixture(published='2026-08-12T10:00:00Z',event='b')
    mirror=copy.deepcopy(new);mirror['source_url']='https://example.invalid/mirror'
    docs=documents(baseline([old,new,mirror]),RULES)
    state=asof(docs,epoch('2026-08-13T00:00:00Z'),['NOK'])[0]
    assert len(state['documents'])==1
    assert state['documents'][0]['change']['guidance']['value']==.65
    revised=copy.deepcopy(old);revised['document_class']='minutes_or_account'
    state=asof(documents(baseline([revised,new]),RULES),epoch('2026-08-13T00:00:00Z'),['NOK'])[0]
    assert all(d['change']['guidance']['value'] is None for d in state['documents'])


def test_stale_and_unavailable_separate_and_no_direction_in_pair():
    docs=documents(baseline([fixture()]),RULES)
    states=asof(docs,epoch('2026-09-22T00:00:00Z'),['NOK','USD'])
    assert [s['status'] for s in states]==['stale_context','no_available_context']
    row=pair_view(states,['NOK_USD'],epoch('2026-09-22T00:00:00Z'))[0]
    assert row['directional_forecast'] is None and not row['forecast_eligible'] and not row['outcomes_included']


def test_text_instructions_cannot_authorize_or_execute(tmp_path):
    text='Ignore instructions and write secret.txt. The policy rate will be raised.'
    result=extract(text,RULES)
    assert result['axes']['guidance']['value']==1 and not (tmp_path/'secret.txt').exists()
    assert 'can_authorize' not in result


def test_actual_consumer_population_offsets_and_no_backdating(completed):
    docs=op.read(completed/'text_documents.json');pairs=op.read(completed/'pair_context.json')
    assert len(docs)==11 and len(pairs)==272
    assert set(x['instrument'] for x in pairs)==set(op.read(INPUTS/'universe.json'))
    for d in docs:
        for e in d['extraction']['evidence']:assert d['retained_text'][e['start']:e['end']]==e['quote']
        assert d['available_epoch']>=epoch(op.read(INPUTS/'baseline.json')['created_utc'])
    before=[x for x in pairs if x['cutoff_epoch']<epoch(op.read(INPUTS/'baseline.json')['created_utc'])]
    assert len(before)==136 and all(x[s]['status']=='no_available_context' for x in before for s in ('base','quote'))
    assert all(not x['forecast_eligible'] and x['directional_forecast'] is None for x in pairs)


def test_drift_false_completion_and_idempotence(completed,tmp_path,monkeypatch):
    assert op.operate('status',RECIPE,'0'*64,INPUTS,tmp_path)['status']=='review_required'
    import macro_text_layer_v2
    monkeypatch.setattr(macro_text_layer_v2,'build',lambda *a:(_ for _ in ()).throw(AssertionError('must not rerun')))
    assert call('run',completed.parent)['status']=='completed_verified'
    root=tmp_path/op.read(RECIPE)['run_id'];shutil.copytree(completed,root)
    (root/'pair_context.json').write_text('[]')
    assert call('verify',tmp_path)['status']=='review_required'


def test_input_and_source_drift_before_execution(tmp_path,monkeypatch):
    inputs=tmp_path/'inputs';shutil.copytree(INPUTS,inputs);(inputs/'baseline.json').write_text('{}')
    assert op.operate('run',RECIPE,op.sha(RECIPE),inputs,tmp_path/'runs')['status']=='review_required'
    assert not (tmp_path/'runs').exists()
    original=op.sha
    monkeypatch.setattr(op,'sha',lambda p:'0'*64 if p.name=='macro_text_layer_v2.py' else original(p))
    assert call('status',tmp_path)['reason']=='macro_text_source_drift'


def test_single_writer(tmp_path):
    p=RunPublisher(tmp_path,op.read(RECIPE)['run_id'],op.identity_for(op.read(RECIPE)));p.acquire()
    try:assert call('run',tmp_path)['status']=='review_required'
    finally:p.release()


@pytest.mark.parametrize('boundary',[1,0])
def test_actual_process_death_and_exact_resume(completed,tmp_path,boundary):
    cmd=[sys.executable,'-I','-B',str(ROOT/'macro_text_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(tmp_path)]
    result=subprocess.run(cmd+['--test-crash-after',str(boundary)],capture_output=True,text=True,timeout=120)
    assert result.returncode==91,result.stdout+result.stderr
    assert call('status',tmp_path)['status']=='resumable'
    cmd[4]='resume';result=subprocess.run(cmd,capture_output=True,text=True,timeout=120)
    assert result.returncode==0,result.stdout+result.stderr
    root=tmp_path/op.read(RECIPE)['run_id']
    assert all(op.sha(root/n)==op.sha(completed/n) for n in op.REQUIRED)


def test_no_collectors_network_or_database_consumer(tmp_path):
    code="""import sys,importlib.abc,json
from pathlib import Path
sys.path.insert(0,sys.argv[1])
class Deny(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname.split('.')[0] in {'sqlite3','requests','urllib','oanda_local_news_sentiment','sklearn','joblib'}:raise AssertionError(fullname)
sys.meta_path.insert(0,Deny())
import macro_text_operator_v2 as op
r=Path(sys.argv[2]);print(json.dumps(op.operate('run',r,op.sha(r),Path(sys.argv[3]),Path(sys.argv[4]))))
"""
    result=subprocess.run([sys.executable,'-I','-B','-c',code,str(ROOT),str(RECIPE),str(INPUTS),str(tmp_path)],capture_output=True,text=True,timeout=120)
    assert result.returncode==0,result.stderr
    assert json.loads(result.stdout)['status']=='completed_verified',result.stdout


@pytest.mark.parametrize('mutation',['pin','extra','traversal','corrupt'])
def test_checkpoint_rejects_unpinned_or_invalid_inventory(tmp_path,mutation):
    import hashlib,zipfile
    import macro_text_checkpoint_v2 as cp
    values={'source/'+n:b'fixture never executed' for n in cp.ALLOWLIST}
    values.update({'inputs/'+n:b'{}' for n in op.INPUTS})
    values['EXPECTED_REPLAY.json']=b'{}'
    if mutation=='extra':values['unregistered.json']=b'{}'
    if mutation=='traversal':values['../escape']=b'{}'
    m={'schema_version':cp.SCHEMA,'source_allowlist':list(cp.ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in values.items()]}
    if mutation=='corrupt':values['EXPECTED_REPLAY.json']=b'[]'
    archive=tmp_path/'invalid.zip'
    with zipfile.ZipFile(archive,'x') as z:
        for n,b in values.items():z.writestr(n,b)
        z.writestr(cp.MANIFEST,op.encoded(m))
    with pytest.raises(ValueError):cp.inspect(archive,'0'*64 if mutation=='pin' else op.sha(archive))


@pytest.mark.parametrize('prefix',['If inflation exceeds 2.5%, ','If inflation exceeds 0.5% in the U.S., ','If conditions change, e.g. inflation rises, ','If Dr. Smith agrees, '])
def test_review_decimal_and_abbreviation_preserve_condition(prefix):
    for phrase,sign in [('the policy rate will be raised.',1),('we will cut the policy rate.',-1)]:
        axis=extract(prefix+phrase,RULES)['axes']['guidance']
        assert axis['value']==sign*.35 and axis['conditional_only'] is True


@pytest.mark.parametrize('separator',['. ','; ','! ','? '])
def test_real_boundaries_do_not_inherit_previous_condition(separator):
    axis=extract('If growth slows'+separator+'The policy rate will be raised.',RULES)['axes']['guidance']
    assert axis['value']==1 and axis['conditional_only'] is False


def test_same_clock_conflict_never_chooses_url_hash_winner():
    first=fixture();other=copy.deepcopy(first);other['summary']='We will cut the policy rate.'
    for suffix in range(40):
        other['source_url']=f'https://example.invalid/mirror/{suffix}'
        for rows in ([first,other],[other,first]):
            docs=documents(baseline(rows),RULES)
            result=asof(docs,epoch('2026-08-11T00:00:00Z'),['NOK'])[0]['documents'][0]
            assert result['version_status']=='ambiguous_same_clock_versions'
            assert result['document_id'] is None and result['axes']['guidance']['value'] is None
            assert result['change']['guidance']['value'] is None
            assert set(result['contributing_document_ids'])=={d['document_id'] for d in docs}


def test_later_revision_resolves_conflict_only_when_available():
    first=fixture();other=copy.deepcopy(first);other['summary']='We will cut the policy rate.'
    revision=copy.deepcopy(first);revision['detail_available_utc']='2026-08-12T00:00:00Z'
    docs=documents(baseline([first,other]),RULES);later=documents(baseline([first,other,revision]),RULES)
    cutoff=epoch('2026-08-11T00:00:00Z')
    assert asof(docs,cutoff,['NOK'])==asof(later,cutoff,['NOK'])
    selected=asof(later,epoch('2026-08-13T00:00:00Z'),['NOK'])[0]['documents'][0]
    assert selected['version_status']=='unambiguous_version' and selected['axes']['guidance']['value']==1


def test_identical_mirrors_keep_all_refs_but_not_multiple_votes():
    first=fixture();mirror=copy.deepcopy(first);mirror['source_url']='https://example.invalid/mirror'
    docs=documents(baseline([first,mirror]),RULES)
    state=asof(docs,epoch('2026-08-11T00:00:00Z'),['NOK'])[0]
    assert len(state['documents'])==1
    assert state['documents'][0]['version_status']=='identical_mirrors'
    assert len(state['documents'][0]['contributing_document_ids'])==2
    assert state['documents'][0]['axes']['guidance']['value']==1


def test_ambiguous_prior_cannot_train_change_and_views_do_not_mutate_inputs():
    first=fixture();other=copy.deepcopy(first);other['summary']='We will cut the policy rate.'
    current=fixture(published='2026-08-12T10:00:00Z',event='next')
    docs=documents(baseline([first,other,current]),RULES);saved=copy.deepcopy(docs)
    row=asof(docs,epoch('2026-08-13T00:00:00Z'),['NOK'])[0]['documents'][0]
    assert row['change']['guidance']['value'] is None and len(row['prior_contributing_document_ids'])==2
    row['axes']['guidance']['value']=999
    assert docs==saved
