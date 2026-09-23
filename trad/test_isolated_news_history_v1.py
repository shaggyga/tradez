"""Actual existing mapper to new owned registry, then actual joint history reader."""
from pathlib import Path
import copy,datetime as dt,hashlib,json,sqlite3
import pytest
import oanda_isolated_news_history_v1 as history
import oanda_causal_forecast_inputs_joint_news_v3 as joint
import oanda_news_causal_aggregation_guard_v2 as guard
from test_oanda_causal_forecast_inputs_joint_news_v1 import member

NOW=1789272000.0
def dump(path,value):path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value),encoding='utf-8')
def fixture(tmp_path):
    root=tmp_path/'fresh';paths=history.paths_for(root)
    config=tmp_path/'sources.json';dump(config,{'sources':[{'source_id':'source_a','name':'Source A','kind':'rss','currencies':['EUR'],'source_role':'aggregator_discovery'}]})
    digest=hashlib.sha256(config.read_bytes()).hexdigest()
    dump(paths['clock_path'],{'schema_version':1,'status':'ok','generated_utc':joint._iso(NOW-1),
        'timestamp_normalization_trusted':True,'host_clock_synchronized':True,'source_fresh':True,
        'broker_clock_lead_sec':0.2,'broker_clock_sample_count':128,'source_age_sec':1})
    paths['news_database'].parent.mkdir(parents=True)
    with sqlite3.connect(paths['news_database']) as db:
        db.execute('''CREATE TABLE articles(event_id TEXT PRIMARY KEY,source_id TEXT,source_name TEXT,source_kind TEXT,
            source_quality REAL,source_verified INTEGER,published_utc TEXT,first_seen_utc TEXT,last_seen_utc TEXT,
            headline TEXT,summary TEXT,category TEXT,scope TEXT,currencies_json TEXT,generic_sentiment_score REAL,
            directional_confidence REAL,severity REAL,movement_potential TEXT,duplicate_count INTEGER,payload_json TEXT)''')
    append(paths['news_database'],1)
    return root,config,digest,paths
def append(path,index,*,source='source_a',trusted=True,seen=None,classification=None):
    t=NOW-80+index if seen is None else seen
    payload=member(t,index,version=guard.CLASSIFICATION_VERSION,score=.4)
    payload.update(source_id=source,source_name='Source A',observation_clock_trusted=trusted,
        collector_contract_id=history.governance.LOCAL_NEWS_COLLECTOR_CONTRACT_ID,
        collector_cohort_id=history.governance.LOCAL_NEWS_COLLECTOR_COHORT_ID,
        observation_time_contract_id=history.governance.OBSERVATION_TIME_CONTRACT_ID,
        classification_observation_contract='article_classification_observation_v1_20260912',
        classification_clock_status='valid_attested_classification',classification_first_known_utc=joint._iso(t+5),
        classification_available_utc=joint._iso(t+5 if classification is None else classification),
        source_evidence_contract='retained_story_body_age_v1_20260912',source_evidence_content_sha256='a'*64,
        source_evidence_available_utc=joint._iso(t))
    row=[payload['event_id'],source,'Source A','rss',.7,0,payload['published_utc'],payload['first_seen_utc'],joint._iso(t+6),
        payload['headline'],'','economic_activity','currencies','["EUR"]',0.,.5,.5,'MEDIUM',0,json.dumps(payload)]
    with sqlite3.connect(path) as db:db.execute('INSERT INTO articles VALUES('+','.join('?'*20)+')',row)
    return payload
def run(root,config,digest,now=NOW):
    return history.run_once(data_root=root,config_path=config,config_sha256=digest,clock=lambda:now,
        mapper=lambda **kwargs:history.fast.run(**kwargs,now=dt.datetime.fromtimestamp(now,dt.timezone.utc)))

def test_new_source_contracts_mapper_and_joint_history_reader_form_actual_loop(tmp_path):
    root,config,digest,paths=fixture(tmp_path);news_before=paths['news_database'].read_bytes()
    result=run(root,config,digest)
    assert result['status']=='history_transport_observed' and result['new_mappings']==result['total_mappings']==1
    assert result['unknown_sources']==0 and result['joint_training_ready'] is False
    rows,start,proof=joint._history(paths['database_path'],NOW+1,guard)
    assert len(rows)==1 and proof['complete'] is True
    assert rows[0]['mapping_visible_epoch']==NOW and rows[0]['member']['currency_scores']=={'EUR':.4}
    assert rows[0]['member']['source_evidence_available_utc']==joint._iso(NOW-79)
    assert rows[0]['member']['classification_available_utc']==joint._iso(NOW-74)
    assert paths['news_database'].read_bytes()==news_before
    again=run(root,config,digest)
    assert again['new_mappings']==0 and again['total_mappings']==1
    with sqlite3.connect(paths['database_path']) as db:
        assert db.execute('SELECT COUNT(*) FROM source_contracts').fetchone()[0]==1
        assert db.execute('SELECT COUNT(*) FROM source_events').fetchone()[0]==1

def test_later_committed_article_matures_into_same_new_history(tmp_path):
    root,config,digest,paths=fixture(tmp_path);run(root,config,digest)
    append(paths['news_database'],2)
    result=run(root,config,digest,NOW+2)
    rows,_,_=joint._history(paths['database_path'],NOW+3,guard)
    assert len(rows)==2 and result['new_mappings']==1 and result['total_mappings']==2
    assert [row['mapping_visible_epoch'] for row in rows]==[NOW,NOW+2]

def test_untrusted_rows_are_retained_as_transport_rejections_not_features(tmp_path):
    root,config,digest,paths=fixture(tmp_path);append(paths['news_database'],2,trusted=False)
    result=run(root,config,digest)
    assert result['untrusted_rows']==1 and result['total_mappings']==1
    rows,_,_=joint._history(paths['database_path'],NOW+1,guard);assert len(rows)==1

@pytest.mark.parametrize('change',[{'host_clock_synchronized':False},{'timestamp_normalization_trusted':False},{'source_age_sec':999}])
def test_clock_refusal_precedes_any_output_database_or_state_write(tmp_path,change):
    root,config,digest,paths=fixture(tmp_path)
    clock=json.loads(paths['clock_path'].read_text());clock.update(change);dump(paths['clock_path'],clock)
    with pytest.raises(ValueError):run(root,config,digest)
    assert not paths['database_path'].exists() and not paths['state_path'].exists() and not paths['profile_state_path'].exists()

def test_missing_news_input_precedes_registry_creation(tmp_path):
    root=tmp_path/'new';config=tmp_path/'config.json';dump(config,{'sources':[{'source_id':'source_a'}]})
    paths=history.paths_for(root);dump(paths['clock_path'],{'schema_version':1,'status':'ok','generated_utc':joint._iso(NOW-1),'timestamp_normalization_trusted':True,'host_clock_synchronized':True,'source_fresh':True,'broker_clock_lead_sec':.2,'broker_clock_sample_count':128,'source_age_sec':1})
    with pytest.raises(ValueError,match='isolated_news_input_missing'):run(root,config,hashlib.sha256(config.read_bytes()).hexdigest())
    assert not paths['database_path'].exists()

def test_old_or_partial_governance_registry_refused_before_schema_changes(tmp_path):
    root,config,digest,paths=fixture(tmp_path)
    with sqlite3.connect(paths['database_path']) as db:db.execute('CREATE TABLE old_evidence(value TEXT)');db.execute("INSERT INTO old_evidence VALUES('preserved')")
    before=paths['database_path'].read_bytes()
    with pytest.raises(ValueError,match='unrelated_or_partial'):run(root,config,digest)
    assert paths['database_path'].read_bytes()==before

def test_changed_pinned_config_refuses_before_mapping(tmp_path):
    root,config,digest,paths=fixture(tmp_path);run(root,config,digest);append(paths['news_database'],2)
    before=paths['database_path'].read_bytes();config.write_text('{}')
    with pytest.raises(ValueError,match='config_hash_mismatch'):run(root,config,digest)
    assert paths['database_path'].read_bytes()==before

def test_changed_exact_policy_refuses_existing_registry(tmp_path,monkeypatch):
    root,config,digest,paths=fixture(tmp_path);run(root,config,digest)
    original=history.source_bindings
    monkeypatch.setattr(history,'source_bindings',lambda:{**original(),'test_changed_source.py':'a'*64})
    with pytest.raises(ValueError,match='immutable_history_profile_mismatch'):run(root,config,digest)

def test_new_derived_availability_kept_beyond_mapping_clock(tmp_path):
    root,config,digest,paths=fixture(tmp_path);append(paths['news_database'],2,classification=NOW+30)
    run(root,config,digest);rows,_,_=joint._history(paths['database_path'],NOW+1,guard)
    future=next(row for row in rows if row['member']['event_id']=='joint_fixture_2')
    assert joint._derived_known(future['member'])==NOW+30
    result=joint._frame({'news_capture_sha256':'f'*64,'history':[future]},NOW+1)
    assert result['members']==[]

def test_profile_marker_immutable(tmp_path):
    root,config,digest,paths=fixture(tmp_path);run(root,config,digest)
    with sqlite3.connect(paths['database_path']) as db:
        with pytest.raises(sqlite3.IntegrityError,match='immutable_history_profile'):db.execute('UPDATE isolated_news_history_profile_v1 SET activated_epoch=1')

def test_future_first_seen_not_advanced_by_existing_mapper(tmp_path):
    root,config,digest,paths=fixture(tmp_path);append(paths['news_database'],2,seen=NOW+60)
    result=run(root,config,digest)
    assert result['total_mappings']==1
    with sqlite3.connect(paths['database_path']) as db:assert db.execute('SELECT input_rowid FROM news_fast_lane_batches_v3 ORDER BY batch_seq DESC LIMIT 1').fetchone()[0]==1
