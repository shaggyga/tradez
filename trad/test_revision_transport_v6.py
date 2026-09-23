"""Owned fixtures and virtual clocks; no live configuration/process changes."""
import ast
from concurrent.futures import Future
import hashlib
import json
from pathlib import Path

import pytest

import revision_transport_v5 as previous
import revision_transport_v6 as transport
from test_projection_revision_consumer_v2 import owned, fixture
from test_revision_news_incremental_v2 import configurations


def new_runner(owned,tmp_path,duration=600):
    config = configurations(owned,tmp_path)
    value = json.loads(Path(config['transport_config_path']).read_bytes())
    value.update(schema_version=transport.CONFIG,duration_sec=duration)
    path = tmp_path/'transport-v6.json'
    path.write_bytes(transport._owners().encode(value))
    return transport.open_runner(str(path),hashlib.sha256(path.read_bytes()).hexdigest())


def progress(count,complete=False):
    return dict(status='prefix_verified_requires_fresh_read' if complete else 'bootstrapping',
        validated_observations=count,captured_observations=3,step_expanded_bytes=1024,
        historical_expanded_bytes=count*1024,current_context_available=False)


def pending(phase='validate_prefix'):
    return dict(status='bootstrap_pending',next_phase=phase,current_health_proven=False,**transport.INERT)


def ready():
    return dict(status='cache_prepared_fresh_cycle_required',publication_count=3,observation_count=3,
        current_health_proven=False,original_availability_unchanged=True,**transport.INERT)


class Clock:
    def __init__(self):self.now=0.;self.scheduled=[]
    def __call__(self):return self.now
    def wait(self,seconds):
        self.now+=seconds
        for at,future,result,report,shared in self.scheduled:
            if at<=self.now and not future.done():
                if report is not None:shared['last']=report
                if isinstance(result,Exception):future.set_exception(result)
                else:future.set_result(result)


class FakeReader:
    def __init__(self,*args,**kwargs):self.close_count=0
    def close(self):self.close_count+=1


def planned(monkeypatch,clock,operations):
    starts=[];futures=[]
    def start(owner,original,reader,shared,phase):
        assert all(f.done() for f in futures), 'overlapping bootstrap operations'
        index=len(starts);duration,result,report=operations[index]
        starts.append((reader,phase,clock()));future=Future();futures.append(future)
        clock.scheduled.append((clock()+duration,future,result,report,shared))
        return future
    monkeypatch.setattr(transport.incremental,'IncrementalReader',FakeReader)
    monkeypatch.setattr(transport,'_start_bootstrap',start)
    return starts,futures


def test_prefix_over_330_seconds_resumes_one_reader_then_requires_fresh_cycle(owned,tmp_path,monkeypatch):
    runner=new_runner(owned,tmp_path);clock=Clock()
    starts,futures=planned(monkeypatch,clock,[(200,pending(),progress(1)),
        (200,pending('fresh_read'),progress(3,True)),(2,ready(),None)])
    state=transport._state(runner)
    try:
        first=transport.prepare_runner(runner,monotonic=clock,wait=clock.wait)
        assert first['status']=='starting' and not state.bootstrap_complete
        retained=futures[0]
        while not state.bootstrap_complete:
            report=transport.prepare_runner(runner,monotonic=clock,wait=clock.wait)
            assert report['status']=='starting'
            assert report['last_success'] is None and report['successful_cycles']==0
            assert report['failure_generation']==0
            if clock()<200:assert state.bootstrap_future is retained
        assert clock()>400
        assert [x[1] for x in starts]==['validate_prefix','validate_prefix','fresh_read']
        assert len({id(x[0]) for x in starts})==1
        assert state.current['phase']=='cold_bootstrap_ready_fresh_cycle_required'
        assert state.current['report_clock_trusted'] is False
        transport._validate_status(state.owner,state.current,state.profile,state.profile_sha)
    finally:transport.close_runner(runner)


def test_run_loop_continues_pending_but_keeps_original_process_deadline(owned,tmp_path,monkeypatch):
    runner=new_runner(owned,tmp_path,duration=120);clock=Clock()
    starts,futures=planned(monkeypatch,clock,[(200,pending(),progress(1))])
    state=transport._state(runner)
    result=transport.run_loop(runner,monotonic=clock,wait=clock.wait)
    assert result['cycle_count']==0 and result['elapsed_seconds']==120
    assert result['last_status']['status']=='stopped'
    assert len(starts)==1 and state.bootstrap_abandoned
    reader=state.validation_reader
    transport.close_runner(runner)
    assert reader.close_count==0 and state.locked
    # The owner lease survives an abandoned read, preventing an overlapping
    # new owner until that operation ends (or the whole process exits).
    clock.wait(80)
    assert reader.close_count==1 and not state.locked


def test_run_loop_reaches_fresh_cycle_after_aggregate_330_seconds(owned,tmp_path,monkeypatch):
    runner=new_runner(owned,tmp_path);clock=Clock()
    starts,_=planned(monkeypatch,clock,[(200,pending('fresh_read'),progress(3,True)),
        (140,ready(),None)])
    cycle_calls=[]
    def cycle(runner,**kwargs):
        state=transport._state(runner)
        assert state.bootstrap_complete and state.bootstrap_future is None
        cycle_calls.append(clock())
        return {'status':'stopped'}
    monkeypatch.setattr(transport,'run_cycle',cycle)
    try:
        result=transport.run_loop(runner,monotonic=clock,wait=clock.wait)
        assert len(cycle_calls)==1 and 340<=cycle_calls[0]<600
        assert len(starts)==2 and result['last_status']['failure_generation']==0
    finally:transport.close_runner(runner)


def test_three_ordinary_failures_reprepare_same_reader_without_resetting_deadline(owned,tmp_path,monkeypatch):
    runner=new_runner(owned,tmp_path,duration=150);clock=Clock()
    starts,_=planned(monkeypatch,clock,[(0,pending('fresh_read'),progress(3,True)),(0,ready(),None)]*3)
    cycles=[]
    def failed_cycle(runner,**kwargs):
        state=transport._state(runner);cycles.append(clock())
        state.failure('fixture_ordinary_capture_timeout','source_scan',fixture.epoch(3),trusted=False)
        return state.report('failed','source_scan',fixture.epoch(3),trusted=False)
    monkeypatch.setattr(transport,'_run_cycle',failed_cycle)
    try:
        result=transport.run_loop(runner,monotonic=clock,wait=clock.wait)
        state=transport._state(runner)
        assert result['cycle_count']==len(cycles)==3
        assert result['elapsed_seconds']==150 and result['cooperative_duration_sec']==150
        assert len(starts)==6 and len({id(x[0]) for x in starts})==1
        assert state.bootstrap_terminal is False and state.generation==3
    finally:transport.close_runner(runner)


def test_integrity_failure_is_terminal_and_never_restarted_as_pending(owned,tmp_path,monkeypatch):
    runner=new_runner(owned,tmp_path);clock=Clock()
    starts,_=planned(monkeypatch,clock,[(2,ValueError('fixture_receipt_hash_changed'),None)])
    try:
        result=transport.run_loop(runner,monotonic=clock,wait=clock.wait)
        state=transport._state(runner)
        assert result['last_status']['status']=='failed' and state.bootstrap_terminal
        generation=state.generation
        again=transport.prepare_runner(runner,monotonic=clock,wait=clock.wait)
        assert again['status']=='failed' and state.generation==generation==1
        assert len(starts)==1
    finally:transport.close_runner(runner)


def test_single_operation_watchdog_still_refuses_over_330_seconds(owned,tmp_path,monkeypatch):
    runner=new_runner(owned,tmp_path);clock=Clock()
    starts,_=planned(monkeypatch,clock,[(400,pending(),progress(1))])
    state=transport._state(runner)
    result=transport.run_loop(runner,monotonic=clock,wait=clock.wait)
    assert result['last_status']['status']=='failed'
    assert 'transport_bootstrap_step_duration_bound' in state.current['last_failure']['reason']
    assert len(starts)==1 and state.bootstrap_abandoned
    transport.close_runner(runner);assert state.locked
    clock.wait(400-clock());assert not state.locked


def test_source_generation_change_during_pending_is_terminal(owned,tmp_path,monkeypatch):
    runner=new_runner(owned,tmp_path);clock=Clock()
    starts,_=planned(monkeypatch,clock,[(50,pending(),progress(1))])
    state=transport._state(runner)
    first=transport.prepare_runner(runner,monotonic=clock,wait=clock.wait)
    assert first['status']=='starting'
    def changed(state):raise ValueError('fixture_source_generation_changed')
    monkeypatch.setattr(transport,'_check_sources',changed)
    failed=transport.prepare_runner(runner,monotonic=clock,wait=clock.wait)
    assert failed['status']=='failed' and state.bootstrap_terminal and len(starts)==1
    transport.close_runner(runner);clock.wait(50-clock())


def test_late_suffix_requires_more_validation_before_fresh_read():
    calls=[];shared={}
    class Reader:
        def bootstrap_step(self):calls.append('step');return progress(3,True)
        def read_observations(self):calls.append('read');raise transport.incremental.BootstrapPending(progress(2))
    reader=Reader()
    first=transport._prepare_existing_inputs(None,{},reader,shared,'validate_prefix')
    assert first['next_phase']=='fresh_read'
    second=transport._prepare_existing_inputs(None,{},reader,shared,'fresh_read')
    assert second['next_phase']=='validate_prefix' and shared['last']['current_context_available'] is False
    assert calls==['step','read']


def test_generation_restart_revalidates_raw_cache_not_saved_progress(owned,tmp_path):
    first=new_runner(owned,tmp_path)
    state=transport._state(first)
    # These are descriptive progress fields, not a semantic attestation.
    state.bootstrap_progress['last']=progress(3,True)
    state.report('starting','cold_bootstrap_pending',fixture.epoch(3),trusted=False)
    path=state.config_proof['path'];digest=state.config_proof['sha256']
    transport.close_runner(first)
    second=transport.open_runner(path,digest)
    try:
        current=transport._state(second)
        assert current.validation_reader is None and current.bootstrap_future is None
        assert current.bootstrap_complete is False and current.bootstrap_phase=='validate_prefix'
        assert current.bootstrap_progress=={}
    finally:transport.close_runner(second)


def test_real_owned_prefix_fresh_read_then_original_cycle(owned,tmp_path):
    runner=new_runner(owned,tmp_path)
    try:
        for _ in range(20):
            prepared=transport.prepare_runner(runner)
            assert prepared['status']=='starting',prepared
            if transport._state(runner).bootstrap_complete:break
        assert transport._state(runner).bootstrap_complete
        result=transport.run_cycle(runner,clock_provider=fixture.proof_clock(2.5))
        assert result['status']=='ready',result
        # The fixture advances its verified clock by .01 for each real call.
        timing=result['last_success']
        assert fixture.epoch(2.5)<timing['scan_started_epoch']<=timing['scan_completed_epoch']
        assert timing['scan_completed_epoch']<=timing['consumer_observed_epoch']<=timing['consumer_ack_epoch']<fixture.epoch(2.51)
        assert result['successful_cycles']==1
    finally:transport.close_runner(runner)


def test_live_cycle_health_and_frozen_bounds_unchanged():
    def functions(path):
        return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(Path(path).read_text()).body
                if isinstance(n,ast.FunctionDef)}
    old,new=functions(previous.__file__),functions(transport.__file__)
    for name in ('_run_cycle','_fixed_clock','read_transport_health','require_current_transport_health','_validate_status'):
        assert old[name]==new[name],name
    assert transport.MAX_CYCLE_SECONDS==previous.MAX_CYCLE_SECONDS==150
    assert transport.MAX_BOOTSTRAP_SECONDS==previous.MAX_BOOTSTRAP_SECONDS==330
    assert transport.incremental.MAX_STEP_BYTES==64*1024*1024
    assert transport.incremental.MAX_STEP_SECONDS==20
    assert transport.incremental.MAX_STEP_RECEIPTS==64
