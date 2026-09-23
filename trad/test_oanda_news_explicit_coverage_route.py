"""Owned complete-cycle path fixtures for optional coverage report routing."""
import ast
import copy
import datetime as dt
import hashlib
import json
from pathlib import Path
import sys
import types
import pytest
import oanda_local_news_sentiment as news

HERE=Path(__file__).resolve().parent
SOURCE=Path(news.__file__)
TREE=ast.parse(SOURCE.read_bytes())
HELPER=Path(__file__)
# Retained owned-fixture definitions from independent review e54a13fd...
HELPER_SOURCE="def extract(names, env):\n    nodes=[copy.deepcopy(n) for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]\n    assert {n.name for n in nodes}==set(names)\n    mod=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0)]+nodes,type_ignores=[])\n    exec(compile(ast.fix_missing_locations(mod),str(SOURCE),'exec'),env)\n    return env\n\nclass AggregationReached(Exception): pass\n\nclass ProviderRefused(Exception): pass\n\nclass Fixture:\n    def __init__(self, fail_at=(), clock=SELECTED):\n        self.calls=[];self.writes=[];self.fetches=0;self.upserts=0;self.fail_at=set(fail_at);self.clock=clock\n        self.env={'dt':dt,'UTC':dt.timezone.utc,'Path':Path,'Any':object,'Mapping':dict,'hashlib':hashlib,'json':json,\n            '__file__':str(SOURCE),'DEFAULT_CONFIG':HERE/'config.json','DEFAULT_OUTPUT_ROOT':HERE/'output',\n            'DEFAULT_LEDGER':HERE/'ledger.csv','DEFAULT_EVENT_ROOT':HERE/'events',\n            'SCHEMA_VERSION':'fixture','COLLECTOR_CONTRACT_ID':'contract','COLLECTOR_COHORT_ID':'cohort',\n            'OBSERVATION_TIME_CONTRACT_ID':'clock-contract','CLASSIFICATION_VERSION':'classifier',\n            'utc_now':lambda:NOW,'iso_utc':lambda value=NOW:value.isoformat(),\n            'emit_collector_progress':lambda *a,**k:None,'apply_source_config_lineage':lambda x:x,\n            'atomic_write_json':lambda p,v:self.writes.append((p,copy.deepcopy(v))),\n            'load_json':lambda p,d: {'sources':[{'source_id':'one','kind':'fixture'}]} if p.name=='config.json' else {},\n            'safe_float':lambda x,d:float(x) if x is not None else d,\n            'process_database':lambda p:types.SimpleNamespace(commit=lambda:None,rollback=lambda:None),\n            'close_process_database':lambda *a:None,'collection_order':lambda x,now:x,\n            'migrate_derived_source_state':lambda source,prior,now:prior,\n            'provider_parse_retry_after':lambda *a:None,'source_runtime_status':lambda s:'enabled',\n            'due_for_poll':lambda *a:True,'clean_text':lambda x:str(x or ''),\n            'classify_article':lambda row,first_seen:dict(row,event_id='one',relevant=True),\n            'retain_classified_discovery_article':lambda a:True,\n            'partition_articles_by_retention':lambda x,before:(x,[]),\n            'load_canonical_articles_by_event_ids':lambda *a:[{'event_id':'one','relevant':True}],\n            'classification_floor':lambda a:None,\n            'cluster_incremental_topics_with_history':lambda *a,**k:[],\n            'upsert_topic_events':lambda *a:(0,0),'prune_database':lambda *a,**k:0,\n            'load_relevant_articles':lambda *a,**k:[],'load_context_articles':lambda *a,**k:[],\n            'cluster_articles':lambda *a,**k:[],'cluster_context_articles':lambda *a,**k:[],\n            'build_persistent_policy_state':lambda *a,**k:{},'reconcile_topic_history':lambda *a,**k:0,\n            'write_ledger':lambda *a:False,'event_tagger':types.SimpleNamespace(discover_instruments=lambda:['EUR_USD']),\n            'prospective_clock_attestation':lambda a:a.get('trusted') is True,\n            'normalized_observation_time':self.observe,'fetch_source':self.fetch,\n            'refresh_recent_topic_contract':self.refresh,'upsert_articles':self.upsert,\n            'classification_publication_as_of':self.publication,'reclassify_stored_articles':self.reclassify,\n            'build_pair_scores':self.finish}\n        extract({'collection_observation_time','refresh_pair_aggregation_clock','run_cycle'},self.env)\n    def observe(self, stamp, **kwargs):\n        expected={} if self.clock is None else {'clock_integrity_path':self.clock}\n        assert kwargs==expected, ('clock_route_changed_or_fallback',kwargs,expected)\n        self.calls.append((stamp,dict(kwargs)))\n        return stamp,{'trusted':len(self.calls) not in self.fail_at,'source':'selected_fixture'}\n    def provider(self, fn):\n        stamp, provenance=fn()\n        assert provenance['collector_contract_id']=='contract'\n        assert provenance['collector_cohort_id']=='cohort'\n        assert provenance['observation_time_contract_id']=='clock-contract'\n        if not provenance['observation_clock_trusted']: raise ProviderRefused()\n        return stamp\n    def refresh(self,*a,classification_clock_provider,**k):\n        self.provider(classification_clock_provider)\n        return {'topic_inserted':0,'topic_updated':0,'reclassified':0}\n    def fetch(self,*a,**k):\n        self.fetches+=1;return [{'title':'fixture'}],{'last_status':200}\n    def upsert(self,*a,classification_clock_provider,observation_receipt,**k):\n        self.provider(classification_clock_provider);self.upserts+=1\n        observation_receipt.update(source_observations_refused=0,classification_observations_refused=0)\n        return (1,0)\n    def publication(self,provider,*a,**k): return self.provider(provider)\n    def reclassify(self,*a,classification_clock_provider,**k):\n        self.provider(classification_clock_provider);return 0\n    def finish(self,*a,**k): raise AggregationReached()\n    def run(self): return self.env['run_cycle'](clock_integrity_path=self.clock,refresh_event_catalog=False)"
NOW=dt.datetime(2026,9,13,2,tzinfo=dt.timezone.utc)
environment={'ast':ast,'copy':copy,'dt':dt,'hashlib':hashlib,'json':json,'Path':Path,'types':types,
             'SOURCE':SOURCE,'tree':TREE,'HERE':HERE,'NOW':NOW,'SELECTED':HERE/'unused'}
definitions={'extract','Fixture','AggregationReached','ProviderRefused'}
nodes=[n for n in ast.parse(HELPER_SOURCE).body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in definitions]
assert {n.name for n in nodes}==definitions
exec(compile(ast.fix_missing_locations(ast.Module(body=nodes,type_ignores=[])),str(HELPER),'exec'),environment)

def fixture(tmp_path,monkeypatch):
    f=environment['Fixture'](clock=tmp_path/'selected-clock.json')
    report={'contract_complete':False,'fully_healthy':False,'currency_summary':{'count':21},'fixture_content':'unaltered'}
    legacy=tmp_path/'legacy-coverage'
    official=types.ModuleType('oanda_official_central_bank_coverage')
    official.DEFAULT_MAP=tmp_path/'mapping.json';official.DEFAULT_LINKS=tmp_path/'links.json'
    official.DEFAULT_JSON=legacy/'EXACT_LEGACY_NAME.json';official.DEFAULT_MD=legacy/'EXACT_LEGACY_NAME.md'
    official.build_report=lambda **kwargs:copy.deepcopy(report)
    official.render_markdown=lambda value:json.dumps(value,sort_keys=True)+'\n'
    broad=types.ModuleType('oanda_news_source_coverage');broad.build_coverage=lambda **kwargs:{}
    monkeypatch.setitem(sys.modules,'oanda_official_central_bank_coverage',official)
    monkeypatch.setitem(sys.modules,'oanda_news_source_coverage',broad)
    writes=[];database_paths=[];ledger_paths=[];checkpoints=[]
    def store(kind,path,value):
        assert Path(path).absolute().is_relative_to(tmp_path.absolute()),'write_outside_owned_fixture'
        writes.append((kind,path,copy.deepcopy(value)))
    refresh=f.env['refresh_recent_topic_contract']
    f.env['refresh_recent_topic_contract']=lambda *a,**k:{**refresh(*a,**k),'stale_topics':0,'topic_removed':0}
    original_database=f.env['process_database']
    def database(path):database_paths.append(path);return original_database(path)
    f.env.update({'process_database':database,'atomic_write_json':lambda path,value:store('json',path,value),
                  'atomic_write_text':lambda path,value:store('text',path,value),
                  'build_pair_scores':lambda *a,**k:{'pairs':{}},'build_current_news_snapshot':lambda *a,**k:{'topics':[]},
                  'MAX_CURRENT_SNAPSHOT_BYTES':1024*1024,'summarize_source_health':lambda *a:{},
                  'bounded_wal_checkpoint':lambda path:checkpoints.append(path) or {'fixture':True},
                  'write_ledger':lambda path,*a:ledger_paths.append(path) or False})
    for node in ast.walk(TREE):
        if isinstance(node,ast.Name) and node.id.startswith('ISSUER_BOUND_POLICY_COMMUNICATION_'):
            f.env.setdefault(node.id,'fixture')
    kwargs={'config_path':tmp_path/'config.json','output_root':tmp_path/'news','ledger_path':tmp_path/'ledger.csv',
            'event_root':tmp_path/'events','state_path':tmp_path/'state'/'collector.json',
            'clock_integrity_path':f.clock,'refresh_event_catalog':False}
    return f,kwargs,official,report,writes,database_paths,ledger_paths,checkpoints

@pytest.mark.parametrize('mode',['omitted','none','explicit'])
def test_full_cycle_routes_both_coverage_writes_and_truthful_result(tmp_path,monkeypatch,mode):
    f,kwargs,official,report,writes,databases,ledgers,checkpoints=fixture(tmp_path,monkeypatch)
    if mode=='none':kwargs['coverage_root']=None
    if mode=='explicit':kwargs['coverage_root']=tmp_path/'new-coverage'
    result=f.env['run_cycle'](**kwargs)
    target=(tmp_path/'new-coverage') if mode=='explicit' else official.DEFAULT_JSON.parent
    actual={path:(kind,value) for kind,path,value in writes}
    assert actual[target/official.DEFAULT_JSON.name]==('json',report)
    assert actual[target/official.DEFAULT_MD.name]==('text',official.render_markdown(report))
    assert result['paths']['official_central_bank_coverage']==str(target/official.DEFAULT_JSON.name)
    assert result['status']=='ok'
    assert sum(path.name==official.DEFAULT_JSON.name for kind,path,value in writes)==1
    assert sum(path.name==official.DEFAULT_MD.name for kind,path,value in writes)==1
    if mode=='explicit':assert not any(path.is_relative_to(official.DEFAULT_JSON.parent) for kind,path,value in writes)
    assert databases==[kwargs['output_root']/'local_news_sentiment_v1.sqlite']
    assert checkpoints==databases
    assert ledgers==[kwargs['ledger_path']]
    assert any(path==kwargs['state_path'] for kind,path,value in writes)
    assert all(path.is_relative_to(kwargs['output_root']) or path==kwargs['state_path'] or path.parent==target for kind,path,value in writes)
    assert len(f.calls)==10 and all(k=={'clock_integrity_path':f.clock} for stamp,k in f.calls)

def test_coverage_output_failure_propagates_without_default_fallback(tmp_path,monkeypatch):
    f,kwargs,official,report,writes,*rest=fixture(tmp_path,monkeypatch)
    selected=tmp_path/'cannot-write';kwargs['coverage_root']=selected
    old_write=f.env['atomic_write_json'];attempts=[]
    def fail(path,value):
        if path.name==official.DEFAULT_JSON.name:
            attempts.append(path);raise PermissionError('owned_fixture_selected_destination_refused')
        return old_write(path,value)
    f.env['atomic_write_json']=fail
    with pytest.raises(PermissionError,match='owned_fixture_selected_destination_refused'):f.env['run_cycle'](**kwargs)
    assert attempts==[selected/official.DEFAULT_JSON.name]
    assert not any(path.is_relative_to(official.DEFAULT_JSON.parent) for kind,path,value in writes)

@pytest.mark.parametrize('mode',['omitted','explicit'])
def test_coverage_cli_parses_optional_path(monkeypatch,tmp_path,mode):
    argv=['collector','--once']
    if mode=='explicit':argv+=['--coverage-root',str(tmp_path/'coverage')]
    monkeypatch.setattr(sys,'argv',argv)
    assert news.parse_args().coverage_root==(tmp_path/'coverage' if mode=='explicit' else None)

@pytest.mark.parametrize('mode',['missing_attribute','none','explicit'])
def test_main_forwards_coverage_root_without_starting_threads(monkeypatch,tmp_path,mode):
    args=types.SimpleNamespace(config=tmp_path/'config',output_root=tmp_path/'news',ledger=tmp_path/'ledger',
                              event_root=tmp_path/'events',state=None,clock_integrity_state=tmp_path/'clock',
                              no_refresh_event_catalog=True,force=False,once=True)
    expected=tmp_path/'coverage' if mode=='explicit' else None
    if mode!='missing_attribute':args.coverage_root=expected
    calls=[]
    env={'time':types.SimpleNamespace(monotonic=lambda:0),'threading':types.SimpleNamespace(
        Event=lambda:types.SimpleNamespace(set=lambda:None),Thread=lambda **kw:types.SimpleNamespace(start=lambda:None,join=lambda:None)),
        'lower_research_process_priority':lambda:None,'parse_args':lambda:args,'utc_now':lambda:NOW,
        'CollectorCycleProgress':lambda now:types.SimpleNamespace(update=lambda *a:None),'run_collector_cycle_heartbeat':None,
        'run_cycle':lambda **kw:calls.append(kw) or {},'iso_utc':lambda:NOW.isoformat(),
        'atomic_write_json':lambda *a:None,'publish_collector_cycle_heartbeat':lambda **kw:None,'json':json,'print':lambda *a,**k:None}
    environment['extract']({'main'},env)
    assert env['main']()==0
    assert len(calls)==1 and calls[0]['coverage_root']==expected
    assert calls[0]['clock_integrity_path']==args.clock_integrity_state
