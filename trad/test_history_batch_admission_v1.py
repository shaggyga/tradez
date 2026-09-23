"""Genuine mapper commits remain excluded until exact postproof admission."""
import datetime as dt,json,sqlite3
import pytest
import oanda_isolated_news_history_v1 as history
import oanda_causal_forecast_inputs_joint_news_v3 as joint
import oanda_news_causal_aggregation_guard_v2 as guard
from test_isolated_news_history_v1 import fixture,run,append,NOW,dump

def map_run(root,config,digest,times):
    ticks=iter(times)
    return history.run_once(data_root=root,config_path=config,config_sha256=digest,clock=lambda:next(ticks),
        mapper=lambda **kwargs:history.fast.run(**kwargs,now=dt.datetime.fromtimestamp(times[0],dt.timezone.utc)))
def refresh(paths,now):
    state=json.loads(paths['clock_path'].read_text());state['generated_utc']=joint._iso(now-1);dump(paths['clock_path'],state)
def counts(paths):
    with sqlite3.connect(paths['database_path']) as con:
        return [con.execute('SELECT COUNT(*) FROM '+table).fetchone()[0] for table in
            ('news_fast_lane_mappings_v3','isolated_news_history_admissions_v1','isolated_news_history_admission_visibility_v1')]

@pytest.mark.parametrize('times,expected', [([NOW,NOW+91],[1,0,0]),([NOW,NOW+1,NOW+2,NOW+91],[1,1,0])])
def test_failed_clock_after_mapping_or_receipt_cannot_reach_reader(tmp_path,times,expected):
    root,config,digest,paths=fixture(tmp_path)
    with pytest.raises(ValueError,match='stale_or_future'):map_run(root,config,digest,times)
    assert counts(paths)==expected
    with pytest.raises(ValueError,match='postclock_admitted'):joint._history(paths['database_path'],NOW+100,guard)
    # Fresh retry with no input never rehabilitates old failed batch.
    refresh(paths,NOW+101);result=run(root,config,digest,NOW+101)
    assert result['batch_admission']['status']=='no_new_batch_no_prior_readmission'
    assert counts(paths)==expected
    with pytest.raises(ValueError,match='postclock_admitted'):joint._history(paths['database_path'],NOW+102,guard)
    # A new successful batch only admits its own newly mapped article.
    append(paths['news_database'],2,seen=NOW+102);result=run(root,config,digest,NOW+110)
    rows,start,diagnostics=joint._history(paths['database_path'],NOW+111,guard)
    assert len(rows)==1 and rows[0]['batch_seq']==2 and rows[0]['member']['event_id']=='joint_fixture_2'
    assert start==NOW+110 and diagnostics['excluded_unadmitted_batches']==1

def test_admission_visibility_is_after_independent_durable_receipt_read(tmp_path):
    root,config,digest,paths=fixture(tmp_path)
    result=map_run(root,config,digest,[NOW,NOW+1,NOW+2,NOW+3])
    assert result['batch_admission']['admitted_epoch']==NOW+2
    assert result['batch_admission']['admitted_available_epoch']==NOW+3
    rows,start,_=joint._history(paths['database_path'],NOW+4,guard)
    assert start==NOW+3 and joint._epoch(rows[0]['member']['observed_available_utc'])==NOW+3
    news={'news_capture_sha256':'b'*64,'history':rows}
    assert joint._frame(news,NOW+2.99)['members']==[]
    assert len(joint._frame(news,NOW+3)['members'])==1

def test_non_ok_mapper_commits_no_admission(tmp_path):
    root,config,digest,paths=fixture(tmp_path)
    def mapper(**kwargs):
        result=history.fast.run(**kwargs,now=dt.datetime.fromtimestamp(NOW,dt.timezone.utc));result['status']='retryable_database_error';return result
    result=history.run_once(data_root=root,config_path=config,config_sha256=digest,clock=lambda:NOW,mapper=mapper)
    assert counts(paths)==[1,0,0] and result['status']=='history_transport_requires_review'

@pytest.mark.parametrize('table,column,value',[
    ('news_fast_lane_mappings_v3','input_first_seen_utc',joint._iso(NOW-10)),
    ('news_fast_lane_batches_v3','input_identity','modified-input'),
    ('isolated_news_history_admissions_v1','receipt_sha256','a'*64),
    ('isolated_news_history_admission_visibility_v1','ack_sha256','b'*64)])
def test_reader_rejects_mapping_batch_receipt_or_ack_tampering(tmp_path,table,column,value):
    root,config,digest,paths=fixture(tmp_path);run(root,config,digest)
    with sqlite3.connect(paths['database_path']) as con:
        con.execute('DROP TRIGGER '+table+'_no_update');con.execute('UPDATE '+table+' SET '+column+'=?',(value,))
    with pytest.raises(ValueError,match='admission'):joint._history(paths['database_path'],NOW+1,guard)

def test_no_admission_tables_is_not_legacy_fallback(tmp_path):
    root,config,digest,paths=fixture(tmp_path);run(root,config,digest)
    with sqlite3.connect(paths['database_path']) as con:con.execute('DROP TABLE isolated_news_history_admission_visibility_v1')
    with pytest.raises(sqlite3.Error):joint._history(paths['database_path'],NOW+1,guard)

def test_new_training_policy_and_wrapper_binding_exclude_previous_capture_contract():
    assert 'exact post-clock-admitted mapping batches' in joint.TRAINING_POLICY
    assert 'oanda_isolated_news_history_v1.py' in joint.REQUIRED_SOURCE_FILES
    assert 'oanda_isolated_news_history_v1.py' in history.worker.REQUIRED_SOURCE_BINDINGS
    assert len(joint.CURRENT_SOURCE_FILES)==10
