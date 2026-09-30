"""Run separately from historical generation suites (explicit process policy)."""
import ast
import hashlib
import inspect
from pathlib import Path
import sqlite3

import pytest
import revision_news_capacity_policy_v1 as policy
import revision_news_io_v13 as io
import revision_transport_v7 as transport
import revision_news_io_v12 as previous
import test_revision_news_resumable_v3 as retained
from test_projection_revision_consumer_v2 import owned, fixture, mutate_preserving_schema


@pytest.fixture(autouse=True)
def successor_callers(monkeypatch):
    # Reuse the original real-store test scenarios through the new actual owners.
    monkeypatch.setattr(retained, 'io', io)
    monkeypatch.setattr(retained, 'transport', transport)


@pytest.mark.parametrize('scenario', [
    'test_resumable_transport_and_joint_preserve_original_features_and_clocks',
    'test_resumable_io_refuses_mixed_profile_and_cache',
])
def test_real_store_successor_paths(owned, tmp_path, scenario):
    getattr(retained, scenario)(owned, tmp_path)


@pytest.mark.parametrize('failure', ['changed_prefix', 'invalid_suffix', 'source_graph'])
def test_corrupt_or_changed_history_refused(owned, tmp_path, monkeypatch, failure):
    config=retained.configurations(owned,tmp_path)
    session=io.create_session(config)
    try:
        io.bootstrap_inputs(session,clock=lambda:fixture.epoch(3))
        if failure=='changed_prefix':
            mutate_preserving_schema(config['observation_path'],'acknowledgments',
                "UPDATE acknowledgments SET body=body || ' ' WHERE rowid=(SELECT MIN(rowid) FROM acknowledgments)")
            reason='verified_consumer_inventory_changed'
        elif failure=='invalid_suffix':
            with sqlite3.connect(config['observation_path']) as con:
                con.execute('INSERT INTO observations VALUES(?,?,?,?)',(3,'bad','0'*64,'{}'))
                con.execute('INSERT INTO acknowledgments VALUES(?,?,?)',('bad','0'*64,'{}'))
            reason='consumer_stored_digest_invalid'
        else:
            def changed():raise ValueError('io_bound_module_source_changed')
            monkeypatch.setattr(io,'source_graph',changed)
            reason='io_bound_module_source_changed'
        with pytest.raises(ValueError,match=reason):
            io.bootstrap_inputs(session,clock=lambda:fixture.epoch(3.1))
        assert io._session(session)['usable'] is False
    finally:io.close_session(session)


def test_capacity_crosses_old_count_and_still_has_a_hard_bound(monkeypatch):
    compact=io.consumer.compact
    publisher=io.publisher
    with sqlite3.connect(':memory:') as con:
        con.executescript(publisher.SQL)
        frames={}
        for i in range(20001):
            frame=compact.value_frame({'ordinal':i})
            frames[frame[1]]=frame
        with monkeypatch.context() as m:
            m.setattr(compact,'MAX_OBJECTS',20000)
            with pytest.raises(ValueError,match='cas_store_work_bound'):
                publisher.insert_frames(con,frames,0)
        publisher.insert_frames(con,frames,0)
        assert con.execute('SELECT count(*) FROM evidence_objects').fetchone()[0]==20001
        # Same consumer primitive must reject the new boundary, not go unbounded.
        with monkeypatch.context() as m:
            m.setattr(compact,'MAX_OBJECTS',20001)
            new=compact.value_frame({'ordinal':20002})
            with pytest.raises(ValueError,match='cas_store_work_bound'):
                publisher.insert_frames(con,{new[1]:new},0)
        # Reused content is idempotent and the packed content hashes stay exact.
        publisher.insert_frames(con,frames,0)
        for size,key,body in frames.values():
            saved=con.execute('SELECT expanded_bytes,packed_sha,payload FROM evidence_objects WHERE object_sha=?',(key,)).fetchone()
            assert saved==(size,hashlib.sha256(body).hexdigest(),body)


def test_policy_is_explicit_idempotent_and_rejects_mutation(monkeypatch):
    before=policy.source_binding()
    assert policy.install()==before
    assert io.source_graph()['revision_news_capacity_policy_v1.py']==before['revision_news_capacity_policy_v1.py']
    assert transport._graph(transport._owners())['revision_news_capacity_policy_v1.py']==before['revision_news_capacity_policy_v1.py']
    with monkeypatch.context() as m:
        m.setattr(io.consumer.compact,'MAX_OBJECTS',65537)
        with pytest.raises(ValueError,match='capacity_runtime_policy_changed'):
            io.source_graph()
        with pytest.raises(ValueError,match='capacity_unexpected_runtime_limit'):
            policy.install()


def test_freshness_readback_and_replay_are_unchanged():
    assert io.MAX_SECONDS==previous.MAX_SECONDS==30
    assert io.consumer.MAX_SCAN_AGE_SEC==300
    assert io.publisher.MAX_CAPTURE_SECONDS==5.0
    assert io.publisher.MAX_COLD_READ_SECONDS==600
    assert transport.incremental.MAX_STEP_SECONDS==20
    for name in ('capture_shared','_validate_capture_value','_validate_health_files','current_pair_features','replay_capture'):
        assert ast.dump(ast.parse(inspect.getsource(getattr(io,name))))==ast.dump(ast.parse(inspect.getsource(getattr(previous,name))))
    assert io.TRANSPORT_SOURCES['revision_transport_v7.py']==hashlib.sha256(Path(transport.__file__).read_bytes()).hexdigest()


@pytest.mark.parametrize('status,seconds',[('backlogged',2),('failed',60),('ready',60),('starting',60),('stopped',60)])
def test_fast_catchup_never_accelerates_failure_retries(status,seconds):
    assert transport.next_interval({'status':status})==seconds
