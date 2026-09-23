import datetime as dt,json,sqlite3
import pytest
import oanda_isolated_news_history_v1 as history
from test_isolated_news_history_v1 import fixture,run,append,NOW,dump

def committed(paths):
    with sqlite3.connect(paths['database_path']) as db:return db.execute('SELECT COUNT(*) FROM news_fast_lane_mappings_v3').fetchone()[0]

def test_slow_mapping_preserves_rows_but_cannot_publish_success(tmp_path):
    root,config,digest,paths=fixture(tmp_path);times=iter([NOW,NOW+91])
    with pytest.raises(ValueError,match='news_clock_state_stale_or_future'):
        history.run_once(data_root=root,config_path=config,config_sha256=digest,clock=lambda:next(times),
            mapper=lambda **kwargs:history.fast.run(**kwargs,now=dt.datetime.fromtimestamp(NOW,dt.timezone.utc)))
    assert committed(paths)==1 and not paths['profile_state_path'].exists()
    assert json.loads(paths['state_path'].read_text())['mapping_visible_utc']==dt.datetime.fromtimestamp(NOW,dt.timezone.utc).isoformat()

def test_new_discontinuity_after_mapping_refuses_success_without_retiming_rows(tmp_path):
    root,config,digest,paths=fixture(tmp_path);times=iter([NOW,NOW+2])
    def mapper(**kwargs):
        result=history.fast.run(**kwargs,now=dt.datetime.fromtimestamp(NOW,dt.timezone.utc))
        value=json.loads(paths['clock_path'].read_text());value['clock_discontinuity_active']=True;dump(paths['clock_path'],value)
        return result
    with pytest.raises(ValueError,match='news_clock_discontinuity_active'):
        history.run_once(data_root=root,config_path=config,config_sha256=digest,clock=lambda:next(times),mapper=mapper)
    assert committed(paths)==1 and not paths['profile_state_path'].exists()

def test_backward_completion_clock_retains_commits_and_refuses_success(tmp_path):
    root,config,digest,paths=fixture(tmp_path);times=iter([NOW,NOW-1])
    with pytest.raises(ValueError,match='history_completion_clock_moved_backwards'):
        history.run_once(data_root=root,config_path=config,config_sha256=digest,clock=lambda:next(times),
            mapper=lambda **kwargs:history.fast.run(**kwargs,now=dt.datetime.fromtimestamp(NOW,dt.timezone.utc)))
    assert committed(paths)==1 and not paths['profile_state_path'].exists()

def test_fresh_completion_preserves_both_clock_observations(tmp_path):
    root,config,digest,paths=fixture(tmp_path);times=iter([NOW,NOW+2,NOW+3,NOW+4])
    result=history.run_once(data_root=root,config_path=config,config_sha256=digest,clock=lambda:next(times),
        mapper=lambda **kwargs:history.fast.run(**kwargs,now=dt.datetime.fromtimestamp(NOW,dt.timezone.utc)))
    assert result['observed_epoch']==NOW and result['completed_epoch']==NOW+2
    assert result['original_clock_rechecked_at_completion']['clock_state_sha256']==result['clock_proof']['clock_state_sha256']
    assert result['original_clock_rechecked_at_completion']['observed_epoch']==NOW+2

def test_partial_rejections_not_reported_as_full_transport_success(tmp_path):
    root,config,digest,paths=fixture(tmp_path);append(paths['news_database'],2,trusted=False)
    result=run(root,config,digest)
    assert result['status']=='history_transport_partial' and result['untrusted_rows']==1

def test_rejected_only_transport_never_claims_useful_history(tmp_path):
    root,config,digest,paths=fixture(tmp_path)
    with sqlite3.connect(paths['news_database']) as db:db.execute('DELETE FROM articles')
    append(paths['news_database'],2,trusted=False)
    result=run(root,config,digest)
    assert result['status']=='history_transport_rejected_only' and result['total_mappings']==0

def test_empty_input_transport_is_distinct_from_mapped_history(tmp_path):
    root,config,digest,paths=fixture(tmp_path)
    with sqlite3.connect(paths['news_database']) as db:db.execute('DELETE FROM articles')
    result=run(root,config,digest)
    assert result['status']=='history_transport_observed_empty' and result['total_mappings']==0
