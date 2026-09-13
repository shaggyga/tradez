"""New source/derived availability invariants; actual modules, synthetic inputs."""
import copy, datetime as dt, hashlib, json, sqlite3
from pathlib import Path
import pytest
import oanda_local_news_sentiment_repair_v2 as repair
import oanda_entry_news_feature_join_v2 as join
import oanda_news_causal_aggregation_guard_v2 as guard
import oanda_news_causal_aggregation_guard_v1 as oldguard
import oanda_news_classification_observation_v1 as classification
import oanda_news_topic_identity_reconciliation_v2 as reconcile
from test_oanda_news_causal_aggregation_guard_v2 import row,topic,START
from test_oanda_local_news_sentiment_repair_v2 import make_repaired_fixture,NOW,reseal
from test_oanda_news_topic_identity_reconciliation_v2 import sealed

def at(minutes):return START+dt.timedelta(minutes=minutes)
def member(derived=15,story=1,**changes):
    value=row(verified=True)
    value.update(classification_version=guard.CLASSIFICATION_VERSION,
        classification_observation_contract=classification.CONTRACT,
        classification_clock_status='valid_attested_classification',
        classification_first_known_utc=at(derived).isoformat(),classification_available_utc=at(derived).isoformat(),
        source_evidence_contract='retained_story_body_age_v1_20260912',
        source_evidence_content_sha256='a'*64,source_evidence_available_utc=at(story).isoformat(),
        causal_known_utc=at(derived).isoformat(),estimated_reaction_horizon_minutes=20)
    value.update(changes);return value
def envelope(value,minute=16):
    cutoff=at(minute).timestamp()
    topics=[guard.guard_topic(topic(),[value],as_of=at(minute))]
    return dict(complete=True,guard_version=guard.GUARD_VERSION,classification_version=guard.CLASSIFICATION_VERSION,
        source_bindings=repair.source_bindings(),raw_source_sha256='b'*64,as_of_epoch=cutoff,
        source_visible_epoch=cutoff,consumer_observed_epoch=cutoff,topics=topics,topics_sha256=join.digest(topics))
def joined(value,minute=16):
    return join.join_entry_news_features(instrument='EUR_USD',cutoff_epoch=at(minute).timestamp(),
        current_news=envelope(value,minute),expected_source_bindings=repair.source_bindings(),clock=lambda:at(minute).timestamp())

def test_new_closure_binds_all_actual_dependencies_and_distinct_versions():
    pins=repair.source_bindings()
    assert set(pins)==join.SOURCE_FILES and len(pins)==10
    for name,digest in pins.items():
        assert hashlib.sha256((Path(repair.__file__).parent/name).read_bytes()).hexdigest()==digest
    assert repair.SCHEMA.endswith('v2_20260912_source_derived_clocks')
    assert join.SCHEMA.endswith('v2_20260912_source_derived_clocks')

def test_context_story_age_does_not_refresh_when_classification_computed_later():
    result=joined(member(),16)
    assert result['current_news']['status']=='available'
    assert len(result['current_news']['vetted'])==1
    assert result['news_features']['context_mean_age_hours']['value']==pytest.approx(15/60)
    assert result['news_features']['vetted_remaining_hours']['value']==pytest.approx(5/60)
    proof=result['current_news']['context'][0]
    assert proof['original_known_epoch']==at(1).timestamp()
    assert proof['derived_eligibility_epoch']==at(15).timestamp()

def test_before_derivation_has_no_context_or_forward_signal():
    current=joined(member(),14)['current_news']
    assert not current['context'] and not current['vetted']
    assert any(r['reason']=='member_not_available_at_cutoff' for r in current['rejections'])

def test_expired_story_cannot_gain_forward_life_from_later_classifier():
    current=joined(member(derived=25),26)['current_news']
    assert len(current['context'])==1 and not current['vetted']

def test_very_old_story_stays_outside_context_after_fresh_derivation():
    current=joined(member(derived=65),66)['current_news']
    assert not current['context'] and not current['vetted']
    assert any(r['reason']=='outside_context_window' for r in current['rejections'])

@pytest.mark.parametrize('key',['classification_first_known_utc','classification_available_utc','source_evidence_contract','source_evidence_available_utc'])
def test_missing_clock_contract_never_becomes_neutral_measurement(key):
    value=member();value.pop(key)
    current=joined(value)['current_news']
    assert not current['context'] and not current['vetted']

@pytest.mark.parametrize('key,value',[('classification_available_utc','naive'),('classification_available_utc',at(14).isoformat()),('source_evidence_available_utc',at(0).isoformat()),('classification_clock_status','unproven')])
def test_malformed_or_regressed_clock_refuses(key,value):
    current=joined(member(**{key:value}))['current_news']
    assert not current['context'] and not current['vetted']

def test_reconciliation_retains_every_new_member_field_exactly():
    a=member();b=member(derived=17,event_id='other',source_id='other',publisher_url='https://other.example',headline='Central bank announces policy rate increase after scheduled committee decision')
    topics=[sealed(v,as_of=at(18)) for v in (a,b)]
    before={m['event_id']:m for t in topics for m in t['causal_aggregation_guard']['members']}
    result=reconcile.reconcile_topic_identities(topics,as_of=at(18))
    after={m['event_id']:m for t in result for m in t['causal_aggregation_guard']['members']}
    assert before==after
    assert result[0]['direction_available_utc']==at(17).isoformat()
    assert result[0]['direction_expires_utc']==at(21).isoformat()

def test_legacy_topic_cannot_be_relabelled_by_v2_outer_envelope():
    payload=envelope(member());old=oldguard.guard_topic(topic(),[row(verified=True)],as_of=at(16))
    payload['topics']=[old];payload['topics_sha256']=join.digest(payload['topics'])
    result=join.join_entry_news_features(instrument='EUR_USD',cutoff_epoch=at(16).timestamp(),current_news=payload,
        expected_source_bindings=repair.source_bindings(),clock=lambda:at(16).timestamp())
    assert result['current_news']['status']=='unavailable'
    assert result['current_news']['reason']=='mixed_or_legacy_guard_cohort'

@pytest.mark.parametrize('field',['source_id','source_kind'])
def test_retained_table_payload_source_disagreement_is_explicit_refusal(tmp_path,field):
    value=make_repaired_fixture(tmp_path)['snapshot'];evidence=value['source_evidence']
    evidence['rows'][0][field]='different_query'
    evidence['rows_sha256']=repair.digest(evidence['rows']);reseal(value)
    with pytest.raises(ValueError,match='news_source_identity_mismatch:'+field):repair.validate_repaired_snapshot(value)

def test_v2_capture_retains_exact_table_source_identity(tmp_path):
    f=make_repaired_fixture(tmp_path);r=f['snapshot']['source_evidence']['rows'][0]
    assert r['source_id']==f['article']['source_id'] and r['source_kind']==f['article']['source_kind']

def test_new_publication_does_not_touch_old_snapshot_or_heartbeat(tmp_path):
    f=make_repaired_fixture(tmp_path);old=f['data_root']/'local_news_sentiment_repair_v1';old.mkdir()
    for name in ('current_news_v1.json','heartbeat_v1.json'):(old/name).write_bytes(b'old retained bytes')
    repair.run_cycle(f['data_root'],clock=lambda:NOW)
    for p in old.iterdir():assert p.read_bytes()==b'old retained bytes'
    assert (f['data_root']/'local_news_sentiment_repair_v2/current_news_v2.json').is_file()

def test_actual_classifier_and_ledger_row_reaches_complete_v2_capture(tmp_path):
    f=make_repaired_fixture(tmp_path/'clock_fixture')
    root=tmp_path/'actual_pipeline';newsroot=root/'local_news_sentiment';newsroot.mkdir(parents=True)
    (root/'state').mkdir()
    for relative in ('local_news_sentiment/collector_heartbeat_v1.json','state/clock_integrity_v1.json'):
        (root/relative).write_bytes((f['data_root']/relative).read_bytes())
    clock=dt.datetime.fromtimestamp(NOW-50,dt.timezone.utc)
    source=dt.datetime.fromtimestamp(NOW-110,dt.timezone.utc)
    raw=dict(source_id='fixture_feed',published_utc=repair.iso(NOW-120),headline='Federal Reserve raises interest rate',
        summary='Inflation remains above forecast')
    raw.update(title=raw['headline'],url='https://www.federalreserve.gov/newsevents/pressreleases/monetary20260908a.htm',
        source_currencies=['USD'],source_verified=True,source_direct=True,source_quality=1.,source_kind='rss',source_name='Federal Reserve')
    article=repair.collector.classify_article(raw,first_seen=source)
    provenance=dict(collector_contract_id=repair.collector.COLLECTOR_CONTRACT_ID,
        collector_cohort_id=repair.collector.COLLECTOR_COHORT_ID,observation_time_contract_id=repair.collector.OBSERVATION_TIME_CONTRACT_ID,
        observation_clock_trusted=True,observation_clock_source='synthetic_attested')
    article.update(provenance,source_contract_id='fixture_source_contract',source_cohort_id='fixture_source_cohort')
    database=repair.collector.open_database(newsroot/'local_news_sentiment_v1.sqlite')
    try:
        added,_=repair.collector.upsert_articles(database,[article],source,classification_clock_provider=lambda:(clock,provenance))
        assert added==1
        stored=json.loads(database.execute('SELECT payload_json FROM articles').fetchone()[0])
    finally:database.close()
    snapshot=repair.capture_repaired_snapshot(root,clock=lambda:NOW)
    assert snapshot['source_evidence']['row_count']==1
    core=repair.validate_repaired_snapshot(snapshot)
    assert core['status']=='current'
    members=[m for t in core['topics'] for m in t['causal_aggregation_guard']['members']]
    assert len(members)==1
    assert members[0]['classification_available_utc']==clock.isoformat()
    assert members[0]['source_evidence_available_utc']==stored['source_evidence_available_utc']
    assert members[0]['first_seen_utc']==stored['first_seen_utc']

@pytest.mark.parametrize('name',['oanda_news_source_observation_ledger_v1.py','oanda_news_classification_observation_v1.py','oanda_news_causal_aggregation_guard_v2.py'])
def test_missing_new_closure_member_is_not_accepted(name):
    payload=envelope(member());payload['source_bindings'].pop(name)
    result=join.join_entry_news_features(instrument='EUR_USD',cutoff_epoch=at(16).timestamp(),current_news=payload,
        expected_source_bindings=repair.source_bindings(),clock=lambda:at(16).timestamp())
    assert result['current_news']['status']=='unavailable'
