"""Finite transport with resumable, nonauthorizing in-process preparation.

Only fixed accepted source owners capture, publish and observe revision evidence.
This runner records orchestration health separately from collector health. The collector-only base
does not consume that health; separately bound transport-aware I/O does.
"""
from pathlib import Path, PureWindowsPath
from concurrent.futures import Future
import argparse
import hashlib
import importlib
import json
import math
import os
import re
import signal
import stat
import sys
import threading
import time
import uuid
import weakref
import projection_revision_consumer_v2 as incremental
import revision_news_capacity_policy_v1 as capacity_policy
capacity_policy.install()

CONFIG = 'revision_transport_resumable_config_capacity_v4_20260930'
PROFILE = 'revision_transport_resumable_profile_capacity_v4_20260930'
STATUS = 'revision_transport_resumable_status_capacity_v4_20260930'
FAILURE = 'revision_transport_resumable_failure_capacity_v4_20260930'
CONFIG_KEYS = {'schema_version','news_io_config_path','news_io_config_sha256','state_root','interval_sec','duration_sec','reader_profile_path','reader_profile_sha256','reader_cache_path'}
MAX_STATUS = 128*1024
MAX_FAILURE = 16*1024
MAX_FILES = 8192
MAX_STATE_BYTES = 64*1024**2
MAX_CYCLE_SECONDS = 150
MAX_BOOTSTRAP_SECONDS = 630
BOOTSTRAP_SLICE_SECONDS = 5
BOOTSTRAP_HEARTBEAT_SECONDS = 5
MAX_DURATION = 172800
INTERVAL = 60
HEALTH = 'revision_transport_resumable_health_read_capacity_v4_20260930'
STATUSES = ('starting','running','ready','backlogged','failed','stopped')
IO_SHA = 'b33240f730d261875f7a23c3223e192cd6a7a7b130bcdd666749bfadf13ac327'
INERT = {'research_only':True,'can_place_orders':False,'can_promote':False,'can_authorize':False,
         'account_eligible':False,'proof_eligible':False,'joint_model_consumption_proven':False}


def need(value, reason):
    if not value: raise ValueError(reason)


def lexical(value):
    need(type(value) is str, 'transport_explicit_path_required')
    pure=PureWindowsPath(value)
    need(pure.is_absolute() and pure.drive.lower()=='c:' and '..' not in pure.parts and
         all(':' not in part for part in pure.parts[1:]), 'transport_plain_c_path_required')
    return Path(value)


def plain(value, *, directory=False, missing=False):
    path=lexical(os.fspath(value))
    for item in (*reversed(path.parents),path):
        try: info=item.lstat()
        except FileNotFoundError:
            need(missing and item==path, 'transport_existing_ancestors_required'); return path
        need(not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_file_attributes',0)&1024,'transport_reparse_refused')
        need(stat.S_ISDIR(info.st_mode) if item!=path or directory else stat.S_ISREG(info.st_mode),'transport_path_kind_required')
    return path


def _owners():
    kit=plain(str(Path(__file__).absolute().parent),directory=True)
    fixed=kit/'revision_news_io_base_v1.py'
    path=plain(str(fixed))
    with path.open('rb') as stream:
        before=os.fstat(stream.fileno());need(before.st_size<=2*1024**2,'transport_owner_size_bound')
        raw=stream.read(2*1024**2+1);after=os.fstat(stream.fileno())
    current=plain(str(path)).stat()
    signature=lambda info:(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns)
    need(len(raw)<=2*1024**2 and signature(before)==signature(after)==signature(current),'transport_owner_changed_during_read')
    need(hashlib.sha256(raw).hexdigest()==IO_SHA,'fixed_transport_io_owner_required')
    loaded=sys.modules.get('revision_news_io_base_v1')
    need(loaded is None or Path(getattr(loaded,'__file__','')).absolute()==fixed,'fixed_transport_owner_path_required')
    if str(kit) not in sys.path:sys.path.insert(0,str(kit))
    owner=importlib.import_module('revision_news_io_base_v1')
    need(Path(owner.__file__).absolute()==fixed,'fixed_transport_owner_path_required')
    need(owner.source_graph()[fixed.name]==IO_SHA,'fixed_transport_owner_source_required')
    return owner


def _int(value, reason, minimum=0, maximum=(1<<63)-1):
    need(type(value) is int and minimum<=value<=maximum,reason); return value


def _epoch(value):
    need(type(value) in (int,float) and math.isfinite(value) and value>0,'transport_finite_epoch_required')
    return float(value)


def _configuration(owner, path, expected_sha):
    lexical(path); need(type(expected_sha) is str and re.fullmatch('[0-9a-f]{64}',expected_sha),'transport_config_hash_required')
    config,proof=owner.read_json(path,MAX_STATUS)
    need(proof['sha256']==expected_sha and set(config)==CONFIG_KEYS and config['schema_version']==CONFIG,'transport_exact_configuration_required')
    for key in ('news_io_config_path','state_root'): lexical(config[key])
    need(type(config['news_io_config_sha256']) is str and re.fullmatch('[0-9a-f]{64}',config['news_io_config_sha256']), 'transport_io_configuration_hash_required')
    need(type(config['interval_sec']) is int and config['interval_sec']==INTERVAL,'transport_fixed60second_cadence_required')
    _int(config['duration_sec'],'transport_finite_duration_required',1,MAX_DURATION)
    original,io_proof=owner.read_json(config['news_io_config_path'],MAX_STATUS)
    need(io_proof['sha256']==config['news_io_config_sha256'] and set(original)==owner.CONFIG_KEYS and original['schema_version']==owner.CONFIG,'transport_exact_io_configuration_required')
    for key in ('cohort_id','consumer_id'):
        need(type(original[key]) is str and re.fullmatch('[A-Za-z0-9_-]{1,96}',original[key]),'transport_explicit_stream_identity_required')
    identity=original['input_identity']
    need(type(identity) is dict and set(identity)=={'path','device','inode'},'transport_exact_source_identity_required')
    lexical(identity['path'])
    for key in ('device','inode'): _int(identity[key],'transport_exact_source_identity_integer')
    pathnames=[identity['path']]
    for key in ('publication_path','observation_path','clock_path','heartbeat_path','latest_path','archive_root'):
        lexical(original[key]); pathnames.append(original[key])
    need(len({name.lower() for name in pathnames})==len(pathnames),'transport_distinct_io_paths_required')
    root=plain(config['state_root'],directory=True)
    need(all(not Path(name).is_relative_to(root) for name in pathnames+[path,config['news_io_config_path']]),'transport_state_separate_from_input_paths')
    need(Path(original['heartbeat_path']).name=='collector_heartbeat_v1.json' and
         Path(original['latest_path']).name=='collector_latest_v1.json','transport_fixed_collector_health_filenames')
    owner.publisher.policy_current(original['policy'])
    need(owner.encode(original['policy']['source_bindings'])==owner.encode(owner.adapter.BOUND_POLICY_SOURCES),'transport_exact_source_policy_required')
    for key in ('reader_profile_path','reader_cache_path'): lexical(config[key])
    reader_profile,reader_proof=owner.read_json(config['reader_profile_path'],MAX_STATUS)
    need(reader_proof['sha256']==config['reader_profile_sha256'],'transport_reader_profile_hash_required')
    expected=incremental.operations_profile_for(original['publication_path'],original['observation_path'],
        cohort_id=original['cohort_id'],consumer_id=original['consumer_id'],
        expected_policy=original['policy'],input_identity=original['input_identity'])
    need(owner.encode(reader_profile)==owner.encode(expected),'transport_reader_profile_mismatch')
    cache=plain(config['reader_cache_path'],missing=True)
    need(not cache.is_relative_to(root) and str(cache).lower() not in {name.lower() for name in pathnames},
         'transport_reader_cache_separate_from_inputs_and_state')
    return owner.own(config), owner.own(original), proof, io_proof


def _graph(owner):
    result={**owner.source_graph(), **capacity_policy.source_binding()}; _,proof=owner.read_exact(__file__,2*1024**2)
    _,reader_proof=owner.read_exact(incremental.__file__,2*1024**2)
    return {**result,Path(__file__).name:proof['sha256'],Path(incremental.__file__).name:reader_proof['sha256']}


def _profile_value(owner,config,original,proof,io_proof,graph):
    return {'schema_version':PROFILE,'config_path':proof['path'],'config_sha256':proof['sha256'],
        'news_io_config_path':io_proof['path'],'news_io_config_sha256':io_proof['sha256'],
        'source_bindings':graph,'source_graph_sha256':owner.digest(graph),
        'cohort_id':original['cohort_id'],'consumer_id':original['consumer_id'],
        'input_identity_sha256':owner.digest(original['input_identity']),'policy_sha256':owner.digest(original['policy']),
        'interval_sec':INTERVAL,'duration_sec':config['duration_sec'],'state_root':config['state_root'],
        'reader_profile_sha256':config['reader_profile_sha256'],'reader_cache_path':config['reader_cache_path'],**INERT}


class StatusPersistenceError(RuntimeError): pass
class StopRequested(RuntimeError): pass
class Runner:
    __slots__=('__weakref__',)
    def __new__(cls,*args,**kwargs): raise ValueError('transport_runner_factory_required')
    def __setattr__(self,name,value): raise AttributeError('opaque_transport_runner')


def _registry():
    registry=weakref.WeakKeyDictionary()
    def make(value):
        result=object.__new__(Runner);registry[result]=value;return result
    def get(value):
        need(type(value) is Runner and value in registry,'registered_transport_runner_required')
        state=registry[value]; need(not state.closed,'transport_runner_closed'); return state
    return make,get


_make,_state=_registry()


class _State:
    def __init__(self,owner,config,original,proof,io_proof):
        self.owner=owner;self.config=config;self.original=original;self.config_proof=proof;self.io_proof=io_proof
        self.root=plain(config['state_root'],directory=True);self.root_identity=self._root_identity()
        self.closed=False;self.stream=None;self.locked=False;self.cycle_lock=threading.Lock()
        self.run_id=uuid.uuid4().hex;self.graph=_graph(owner)
        self.profile=_profile_value(owner,config,original,proof,io_proof,self.graph)
        self.profile_sha=owner.digest(self.profile)
        self.current=None;self.generation=0;self.failure_sha=None;self.profile_owned=False
        self.bootstrap_complete=False;self.bootstrap_abandoned=False
        self.validation_reader=None;self.bootstrap_progress={}
        self.bootstrap_future=None;self.bootstrap_step_started=None
        self.bootstrap_phase='validate_prefix';self.bootstrap_terminal=False
        try:
            self._inventory();self._lock();self._recover()
        except Exception as exc:
            if self.profile_owned and isinstance(exc,StatusPersistenceError):
                try:self.failure('startup_status_write_failed:'+str(exc),'startup',time.time(),trusted=False)
                except Exception:pass
            self._unlock();raise

    def _root_identity(self):
        info=plain(str(self.root),directory=True).stat();return info.st_dev,info.st_ino

    def _root_check(self): need(self._root_identity()==self.root_identity,'transport_state_root_replaced')

    def _inventory(self):
        self._root_check();count=total=0;failure=[]
        with os.scandir(self.root) as entries:
            for entry in entries:
                count+=1;need(count<=MAX_FILES,'transport_state_file_bound')
                name=entry.name
                need(name in ('owner.lock','profile.json','status.json') or re.fullmatch(r'failure-[0-9]{8}\.json',name)
                     or re.fullmatch(r'status-[0-9a-f]{32}\.pending',name),'transport_unknown_state_file')
                info=entry.stat(follow_symlinks=False)
                need(stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_file_attributes',0)&1024,'transport_state_leaf_kind')
                total+=info.st_size;need(total<=MAX_STATE_BYTES,'transport_state_byte_bound')
                if name.startswith('failure-'):failure.append(name)
        self._root_check();return count,total,sorted(failure)

    def _lock(self):
        import msvcrt
        path=plain(str(self.root/'owner.lock'),missing=True)
        try:self.stream=path.open('x+b')
        except FileExistsError:self.stream=plain(str(path)).open('r+b')
        self.stream.seek(0);msvcrt.locking(self.stream.fileno(),msvcrt.LK_NBLCK,1);self.locked=True
        self.stream.seek(0);value=self.stream.read(2)
        need(value in (b'',b'1'),'transport_owner_lock_format')
        if not value:self.stream.seek(0);self.stream.write(b'1');self.stream.flush();os.fsync(self.stream.fileno())
        self._root_check()

    def _unlock(self):
        if self.stream is not None:
            try:
                import msvcrt
                if self.locked:self.stream.seek(0);msvcrt.locking(self.stream.fileno(),msvcrt.LK_UNLCK,1)
            except OSError:pass
            self.stream.close();self.stream=None;self.locked=False

    def _write_new(self,path,value,limit):
        raw=self.owner.encode(value,limit);count,total,_=self._inventory()
        need(count<MAX_FILES and total+len(raw)<=MAX_STATE_BYTES,'transport_state_capacity_exhausted')
        path=plain(str(path),missing=True);self._root_check()
        with path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
        saved,proof=self.owner.read_exact(path,limit)
        need(saved==raw,'transport_durable_file_readback_failed');self._root_check()
        return proof['sha256']

    def _json(self,path,limit):
        value,proof=self.owner.read_json(path,limit)
        need(self.owner.encode(value,limit)==proof['raw_utf8'].encode(),'transport_canonical_state_required')
        return value,proof['sha256']

    def _recover(self):
        _,_,files=self._inventory();path=self.root/'profile.json';fresh=not plain(str(path),missing=True).exists()
        if fresh:
            need(not files and not (self.root/'status.json').exists(),'transport_orphan_state_without_profile')
            self._write_new(path,self.profile,MAX_STATUS)
        else:
            profile,_=self._json(path,MAX_STATUS);need(self.owner.encode(profile)==self.owner.encode(self.profile),'transport_profile_or_source_changed')
        self.profile_owned=True
        prior=None
        for sequence,name in enumerate(files,1):
            need(name==f'failure-{sequence:08d}.json','transport_failure_prefix_gap')
            value,digest=self._json(self.root/name,MAX_FAILURE)
            need(value['schema_version']==FAILURE and value['profile_sha256']==self.profile_sha and
                 type(value['failure_generation']) is int and value['failure_generation']==sequence and
                 type(value['previous_generation']) is int and value['previous_generation']==sequence-1 and
                 value['previous_failure_sha256']==prior,'transport_failure_chain_invalid')
            prior=digest
        self.generation=len(files);self.failure_sha=prior
        status=plain(str(self.root/'status.json'),missing=True)
        if status.exists():
            self.current,_=self._json(status,MAX_STATUS)
            _validate_status(self.owner,self.current,self.profile,self.profile_sha)
            value=_int(self.current['failure_generation'],'transport_status_failure_integer')
            need(value<=self.generation,'transport_status_ahead_of_durable_failure')
            if value==self.generation:need(self.current['latest_failure_sha256']==self.failure_sha,'transport_status_failure_binding')
            for key in ('attempt_count','successful_cycles','status_sequence'):_int(self.current[key],'transport_status_counter_invalid')
            need(self.current['status'] in STATUSES,'transport_status_kind_invalid')
        interrupted=(self.current is not None and self.current['status'] in ('starting','running')) or (not fresh and self.current is None)
        if interrupted:self.failure('interrupted_previous_run','startup',time.time(),trusted=False)
        self.report('starting','startup',time.time(),trusted=False)

    def report(self,status,phase,now,*,trusted,update=None):
        try:
            now=_epoch(now);old=self.current or {};value={
                'schema_version':STATUS,'profile_sha256':self.profile_sha,'config_sha256':self.config_proof['sha256'],
                'news_io_config_sha256':self.io_proof['sha256'],'source_graph_sha256':self.profile['source_graph_sha256'],
                'run_id':self.run_id,'status_sequence':old.get('status_sequence',0)+1,
                'attempt_count':old.get('attempt_count',0),'successful_cycles':old.get('successful_cycles',0),
                'failure_generation':self.generation,'latest_failure_sha256':self.failure_sha,
                'status':status,'phase':phase,'report_epoch':now,'report_clock_trusted':trusted,'progress_epoch':now,
                'cycle_started_epoch':old.get('cycle_started_epoch'),'last_success_epoch':old.get('last_success_epoch'),
                'last_consumer_observed_epoch':old.get('last_consumer_observed_epoch'),'last_consumer_ack_epoch':old.get('last_consumer_ack_epoch'),
                'last_success':old.get('last_success'),'last_failure':old.get('last_failure'),
                'bootstrap_progress':self.owner.own(self.bootstrap_progress.get('last')), 
                'transport_current_status_is_not_original_scan_time':True,'io8_consumes_transport_health':False,**INERT}
            if update:value.update(update)
            raw=self.owner.encode(value,MAX_STATUS);temporary=self.root/('status-'+uuid.uuid4().hex+'.pending')
            self._write_new(temporary,value,MAX_STATUS)
            destination=plain(str(self.root/'status.json'),missing=True);self._root_check()
            os.replace(temporary,destination)
            saved,proof=self.owner.read_exact(destination,MAX_STATUS)
            need(saved==raw,'transport_status_readback_failed');self._root_check();self.current=value
            return self.owner.own(value,MAX_STATUS)
        except Exception as exc:raise StatusPersistenceError(type(exc).__name__+':'+str(exc)[:300]) from exc

    def failure(self,reason,phase,now,*,trusted):
        sequence=self.generation+1
        need(sequence<=99999999,'transport_failure_name_bound')
        value={'schema_version':FAILURE,'profile_sha256':self.profile_sha,'failure_generation':sequence,
            'previous_generation':self.generation,'previous_failure_sha256':self.failure_sha,'run_id':self.run_id,
            'attempt_count':0 if self.current is None else self.current['attempt_count'],'phase':phase,
            'observed_epoch':_epoch(now),'observation_clock_trusted':trusted,'reason':str(reason)[:800],**INERT}
        digest=self._write_new(self.root/f'failure-{sequence:08d}.json',value,MAX_FAILURE)
        self.generation=sequence;self.failure_sha=digest
        if self.current is not None:self.current={**self.current,'last_failure':{'failure_sha256':digest,'reason':value['reason'],'phase':phase,'observed_epoch':value['observed_epoch']}}
        return digest


def open_runner(config_path,config_sha256):
    """Validate exact configs/owners, acquire one state owner and recover failures."""
    lexical(config_path);owner=_owners()
    config,original,proof,io_proof=_configuration(owner,config_path,config_sha256)
    return _make(_State(owner,config,original,proof,io_proof))


def runner_status(runner):
    state=_state(runner);return state.owner.own(state.current,MAX_STATUS)


def _fixed_clock(state):
    value,_=state.owner.read_json(state.original['clock_path'],MAX_STATUS)
    now=_epoch(time.time());state.owner.repair.validate_clock_state(value,now)
    return now,value


def _check_sources(state):
    config,original,proof,io_proof=_configuration(state.owner,state.config_proof['path'],state.config_proof['sha256'])
    need(config==state.config and original==state.original and proof['sha256']==state.config_proof['sha256'] and
         io_proof['sha256']==state.io_proof['sha256'] and _graph(state.owner)==state.graph,'transport_sources_or_configuration_changed')


def _prepare_existing_inputs(owner, original, validator, progress, phase):
    """Exactly one bounded operation; no prepared/current handle escapes."""
    if phase=='validate_prefix':
        result=validator.bootstrap_step()
    else:
        need(phase=='fresh_read','transport_bootstrap_phase_required')
        try:context=validator.read_observations()
        except incremental.BootstrapPending as exc:result=exc.progress
        else:
            metadata=owner.consumer.prepared_context_metadata(context)
            return {'status':'cache_prepared_fresh_cycle_required','publication_count':metadata['publication_count'],
                'observation_count':metadata['observation_count'],'current_health_proven':False,
                'original_availability_unchanged':True,**INERT}
    if result['status'] in ('bootstrapping','prefix_verified_requires_fresh_read'):
        progress['last']={key:result[key] for key in ('status','validated_observations','captured_observations',
            'step_expanded_bytes','historical_expanded_bytes','current_context_available')}
        need(result['current_context_available'] is False,'transport_bootstrap_step_not_authority')
        return {'status':'bootstrap_pending','next_phase':
            'fresh_read' if result['status']=='prefix_verified_requires_fresh_read' else 'validate_prefix',
            'current_health_proven':False,**INERT}
    raise ValueError('transport_bootstrap_step_status_required')


def _start_bootstrap(owner, original, validator, progress, phase):
    future=Future()
    def work():
        try:future.set_result(_prepare_existing_inputs(owner,original,validator,progress,phase))
        except BaseException as exc:future.set_exception(exc)
    threading.Thread(target=work,name='revision-transport-incremental-read',daemon=True).start()
    return future


def prepare_runner(runner, *, stop_requested=None, monotonic=time.monotonic, wait=time.sleep):
    """Keep startup liveness current without claiming a completed fresh scan.

    One Future and the same authenticated in-memory reader survive five-second
    slices. Each operation keeps the frozen publisher 300-second and reader
    30-second bounds; 330 seconds is a single-operation watchdog, never an
    aggregate-history limit. The process deadline remains owned by run_loop.
    """
    state=_state(runner);stop=stop_requested or (lambda:False)
    need(state.cycle_lock.acquire(blocking=False),'transport_cycle_already_running')
    started=monotonic()
    try:
        if state.bootstrap_terminal:return state.owner.own(state.current,MAX_STATUS)
        need(not state.bootstrap_abandoned,'transport_bootstrap_process_restart_required')
        if state.bootstrap_complete:return state.owner.own(state.current,MAX_STATUS)
        if stop():raise StopRequested('transport_stop_requested')
        _check_sources(state)
        state.report('starting','cold_bootstrap',time.time(),trusted=False)
        if state.validation_reader is None:
            config=state.config;original=state.original
            profile,_=state.owner.read_json(config['reader_profile_path'],MAX_STATUS)
            state.validation_reader=incremental.IncrementalReader(original['publication_path'],original['observation_path'],
                cohort_id=original['cohort_id'],consumer_id=original['consumer_id'],
                expected_policy=original['policy'],input_identity=original['input_identity'],
                cache_path=config['reader_cache_path'],operations_profile=profile)
        while True:
            if stop():raise StopRequested('transport_stop_requested')
            now=monotonic()
            if state.bootstrap_future is None:
                state.bootstrap_step_started=now
                state.bootstrap_future=_start_bootstrap(state.owner,state.owner.own(state.original),
                    state.validation_reader,state.bootstrap_progress,state.bootstrap_phase)
            need(now-state.bootstrap_step_started<=MAX_BOOTSTRAP_SECONDS,'transport_bootstrap_step_duration_bound')
            if state.bootstrap_future.done():
                result=state.bootstrap_future.result()
                state.bootstrap_future=None;state.bootstrap_step_started=None
                need(type(result) is dict and result.get('current_health_proven') is False and
                     all(result.get(key) is value for key,value in INERT.items()),'transport_bootstrap_non_authorizing_result_required')
                _check_sources(state)
                if result.get('status')=='cache_prepared_fresh_cycle_required':
                    need(state.bootstrap_phase=='fresh_read' and result.get('original_availability_unchanged') is True,
                         'transport_separate_fresh_bootstrap_read_required')
                    if stop():raise StopRequested('transport_stop_requested')
                    state.bootstrap_complete=True
                    return state.report('starting','cold_bootstrap_ready_fresh_cycle_required',time.time(),trusted=False)
                need(result.get('status')=='bootstrap_pending' and result.get('next_phase') in ('validate_prefix','fresh_read'),
                     'transport_bootstrap_pending_required')
                state.bootstrap_phase=result['next_phase']
            if monotonic()-started>=BOOTSTRAP_SLICE_SECONDS:
                return state.report('starting','cold_bootstrap_pending',time.time(),trusted=False)
            wait(min(1.,max(0.,BOOTSTRAP_SLICE_SECONDS-(monotonic()-started))))
    except StopRequested:
        state.bootstrap_terminal=True
        state.bootstrap_abandoned=state.bootstrap_future is not None and not state.bootstrap_future.done()
        return state.report('stopped','cold_bootstrap',time.time(),trusted=False)
    except Exception as exc:
        state.bootstrap_complete=False
        state.bootstrap_terminal=True
        state.bootstrap_abandoned=state.bootstrap_future is not None and not state.bootstrap_future.done()
        try:
            state.failure(type(exc).__name__+':'+str(exc)[:500],'cold_bootstrap',time.time(),trusted=False)
            status=state.report('failed','cold_bootstrap',time.time(),trusted=False)
        except Exception as persist:
            raise StatusPersistenceError('transport_bootstrap_failure_persistence_unavailable') from persist
        if isinstance(exc,StatusPersistenceError):raise
        return status
    finally:state.cycle_lock.release()


def _run_cycle(runner, *, clock_provider=None, stop_requested=None, fault_hook=None):
    """One capture/publish/observe cycle; hooks are trusted owned-test seams only."""
    state=_state(runner);owner=state.owner;provider=clock_provider or (lambda:_fixed_clock(state))
    stop=stop_requested or (lambda:False);started=time.monotonic();phase='start';previous=None
    def sample():
        nonlocal previous
        now,proof=provider();now=_epoch(now);owner.repair.validate_clock_state(proof,now)
        need(previous is None or now>=previous,'transport_clock_regressed');previous=now
        return now,proof
    def boundary(name):
        nonlocal phase
        phase=name
        if stop():raise StopRequested('transport_stop_requested')
        need(time.monotonic()-started<=MAX_CYCLE_SECONDS,'transport_cycle_duration_bound')
        now,_=sample();state.report('running',name,now,trusted=True)
    def hook(namespace):
        return None if fault_hook is None else lambda name:fault_hook(namespace+':'+name)
    try:
        if stop():raise StopRequested('transport_stop_requested')
        _check_sources(state);now,_=sample()
        state.report('running','start',now,trusted=True,update={'attempt_count':state.current['attempt_count']+1,'cycle_started_epoch':now})
        original=state.original;policy=original['policy'];identity=original['input_identity']
        boundary('publication_head')
        store=owner.publisher.path_for(original['publication_path'],missing=True)
        checkpoint=None
        if store.exists():
            head=owner.publisher.read_published(store,cohort_id=original['cohort_id'],expected_policy=policy,input_identity=identity)
            # This is a fully validated opaque compact publication. The head
            # retains its original committed cursor; no expanded history clone.
            checkpoint=head['head']['checkpoint']
        boundary('source_scan')
        snapshot=owner.reader.read_projection_snapshot(identity['path'],checkpoint=checkpoint,
            expected_sources=policy['sources'],expected_provenance=policy['provenance'],clock_provider=sample)
        need(owner.encode(snapshot['input_identity'])==owner.encode(identity) and owner.encode(snapshot['policy'])==owner.encode(policy),
             'transport_exact_captured_source_required')
        boundary('publication')
        publication=owner.publisher.publish_snapshot(original['publication_path'],snapshot,cohort_id=original['cohort_id'],
            clock_provider=sample,fault_hook=hook('publisher'))
        boundary('consumer_observation')
        observed=owner.consumer.observe_published(original['publication_path'],original['observation_path'],
            cohort_id=original['cohort_id'],consumer_id=original['consumer_id'],expected_policy=policy,input_identity=identity,
            completed_scan=snapshot,clock_provider=sample,fault_hook=hook('consumer'))
        boundary('completion');_check_sources(state);completed,_=sample()
        need(time.monotonic()-started<=MAX_CYCLE_SECONDS,'transport_cycle_duration_bound')
        receipt=observed['observation'];ack=observed['acknowledgment'];head=receipt['capture']['head']
        initial_empty=(head['sequence']==0 and snapshot['previous_checkpoint'] is None and
                       snapshot['after_seq']==snapshot['through_seq']==snapshot['committed_high_watermark']==0)
        caught=(snapshot['more_pending'] is False and snapshot['through_seq']==snapshot['committed_high_watermark'] and
                (owner.publisher.exact(snapshot['next_checkpoint'],head['checkpoint']) or initial_empty))
        summary={'run_id':state.run_id,'snapshot_sha256':snapshot['snapshot_sha256'],'scan_started_epoch':snapshot['read_started_epoch'],
            'scan_completed_epoch':snapshot['read_completed_epoch'],'source_high_watermark':snapshot['committed_high_watermark'],
            'scan_through_seq':snapshot['through_seq'],'more_pending':snapshot['more_pending'],'caught_up':caught,
            'published_sequence':head['sequence'],'publication_status':publication['status'],
            'consumer_receipt_sha256':observed['receipt_sha256'],'consumer_ack_sha256':observed['ack_sha256'],
            'consumer_observed_epoch':observed['consumer_observed_epoch'],'consumer_ack_epoch':ack['receipt_observed_epoch'],
            'new_source_entries':snapshot['row_count'],'cycle_seconds':time.monotonic()-started}
        result=state.report('ready' if caught else 'backlogged','complete',completed,trusted=True,update={
            'successful_cycles':state.current['successful_cycles']+1,'last_success_epoch':completed,
            'last_consumer_observed_epoch':observed['consumer_observed_epoch'],'last_consumer_ack_epoch':ack['receipt_observed_epoch'],
            'last_success':summary})
        # Status has now been durably read back. A later overrun/source/clock
        # failure remains a new recorded failure, never a returned success.
        sample();_check_sources(state)
        need(time.monotonic()-started<=MAX_CYCLE_SECONDS,'transport_cycle_duration_bound_after_status')
        return result
    except StopRequested:
        return state.report('stopped',phase,time.time(),trusted=False)
    except Exception as exc:
        reason=type(exc).__name__+':'+str(exc)[:500]
        try:
            state.failure(reason,phase,time.time(),trusted=False)
            result=state.report('failed',phase,time.time(),trusted=False)
        except Exception as persist:
            raise StatusPersistenceError('transport_failure_persistence_unavailable:'+type(persist).__name__+':'+str(persist)[:300]) from exc
        if isinstance(exc,StatusPersistenceError):raise StatusPersistenceError('transport_status_write_failed_run_stopped') from exc
        return result


def run_cycle(runner, *, clock_provider=None, stop_requested=None, fault_hook=None):
    state=_state(runner)
    if not state.bootstrap_complete:
        prepared=prepare_runner(runner,stop_requested=stop_requested)
        if not state.bootstrap_complete:return prepared
    need(state.cycle_lock.acquire(blocking=False),'transport_cycle_already_running')
    try:
        result=_run_cycle(runner,clock_provider=clock_provider,stop_requested=stop_requested,fault_hook=fault_hook)
        if result['status']=='failed':
            state.bootstrap_complete=False;state.bootstrap_phase='validate_prefix'
        return result
    finally:state.cycle_lock.release()


def close_runner(runner):
    state=_state(runner)
    need(state.cycle_lock.acquire(blocking=False),'transport_cycle_already_running')
    try:
        if state.current is not None and state.current['status']!='failed':state.report('stopped','closed',time.time(),trusted=False)
        return state.owner.own(state.current,MAX_STATUS)
    finally:
        state.closed=True;state.bootstrap_terminal=True
        def release(_=None):
            try:
                if state.validation_reader is not None:state.validation_reader.close()
            finally:state._unlock()
        future=state.bootstrap_future
        try:
            if future is not None and not future.done():
                # A stopped daemon cannot be cancelled safely during an exact
                # read. Keep the owner lease until it ends or the process exits;
                # never close its SQLite connection or permit a second owner.
                state.bootstrap_abandoned=True
                future.add_done_callback(release)
            else:release()
        finally:state.cycle_lock.release()


def next_interval(result):
    # A successful backlog scan may catch up without a minute of idle time.
    # Failures and ready scans retain the original 60-second backoff/cadence.
    return 2 if result.get('status')=='backlogged' else INTERVAL


def run_loop(runner, *, stop_requested=None, clock_provider=None, monotonic=time.monotonic, wait=time.sleep):
    """Serial finite scheduling. Wait callbacks are explicit trusted test seams."""
    state=_state(runner);stop=stop_requested or (lambda:False)
    start=monotonic();deadline=start+state.config['duration_sec'];next_attempt=start;cycles=0
    def stopping():return bool(stop()) or monotonic()>=deadline
    while not stopping():
        now=monotonic()
        if now<next_attempt:
            wait(min(1.0,next_attempt-now,max(0.,deadline-now)));continue
        if not state.bootstrap_complete:
            prepared=prepare_runner(runner,stop_requested=stopping,monotonic=monotonic,wait=wait)
            if not state.bootstrap_complete:
                if prepared['status'] in ('failed','stopped'):break
                continue
        began=monotonic();result=run_cycle(runner,clock_provider=clock_provider,stop_requested=stopping)
        if result['status']=='stopped':break
        cycles+=1;next_attempt=max(began+next_interval(result),monotonic())
        if monotonic()==deadline:break
    return {'cycle_count':cycles,'elapsed_seconds':monotonic()-start,'cooperative_duration_sec':state.config['duration_sec'],
            'last_status':runner_status(runner),'external_process_supervision':False,**INERT}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config',required=True);parser.add_argument('--config-sha256',required=True)
    args=parser.parse_args(argv)
    need(sys.dont_write_bytecode is True and type(sys.pycache_prefix) is str,'transport_cold_no_bytecode_required')
    plain(sys.pycache_prefix,missing=True,directory=True)
    need(not Path(sys.pycache_prefix).exists(),'transport_unused_bytecode_prefix_required')
    stopping=[False]
    def stop_signal(*args):stopping[0]=True
    signal.signal(signal.SIGINT,stop_signal);signal.signal(signal.SIGTERM,stop_signal)
    runner=open_runner(args.config,args.config_sha256)
    try:
        result=run_loop(runner,stop_requested=lambda:stopping[0])
    except BaseException:
        close_runner(runner);raise
    failed=result['last_status']['status']=='failed'
    result['last_status']=close_runner(runner)
    print(json.dumps(result));return 1 if failed else 0


def _validate_status(owner,value,profile,profile_sha):
    required={'schema_version','profile_sha256','config_sha256','news_io_config_sha256','source_graph_sha256',
        'run_id','status_sequence','attempt_count','successful_cycles','failure_generation','latest_failure_sha256',
        'status','phase','report_epoch','report_clock_trusted','progress_epoch','cycle_started_epoch',
        'last_success_epoch','last_consumer_observed_epoch','last_consumer_ack_epoch','last_success','last_failure',
        'transport_current_status_is_not_original_scan_time','io8_consumes_transport_health','bootstrap_progress',*INERT}
    need(type(value) is dict and set(value)==required and value['schema_version']==STATUS and
         value['profile_sha256']==profile_sha and all(value[key]==profile[key] for key in
         ('config_sha256','news_io_config_sha256','source_graph_sha256')),'transport_exact_status_identity')
    need(type(value['run_id']) is str and re.fullmatch('[0-9a-f]{32}',value['run_id']) and
         type(value['phase']) is str and 1<=len(value['phase'])<=64 and value['status'] in STATUSES,
         'transport_status_kind_invalid')
    for key in ('attempt_count','successful_cycles','failure_generation','status_sequence'):_int(value[key],'transport_status_counter_invalid')
    need(value['status_sequence']>0 and value['successful_cycles']<=value['attempt_count'],'transport_status_counter_order')
    need(type(value['report_clock_trusted']) is bool and value['transport_current_status_is_not_original_scan_time'] is True and
         value['io8_consumes_transport_health'] is False and all(value[key] is item for key,item in INERT.items()),'transport_status_scope_invalid')
    _epoch(value['report_epoch']);_epoch(value['progress_epoch'])
    need(value['progress_epoch']<=value['report_epoch'],'transport_status_progress_order')
    for key in ('cycle_started_epoch','last_success_epoch','last_consumer_observed_epoch','last_consumer_ack_epoch'):
        if value[key] is not None:_epoch(value[key])
    head=value['latest_failure_sha256']
    need((value['failure_generation']==0 and head is None) or
         (value['failure_generation']>0 and type(head) is str and re.fullmatch('[0-9a-f]{64}',head)),'transport_status_failure_hash')
    progress=value['bootstrap_progress']
    if progress is not None:
        need(type(progress) is dict and set(progress)=={'status','validated_observations','captured_observations',
             'step_expanded_bytes','historical_expanded_bytes','current_context_available'}
             and progress['status'] in ('bootstrapping','prefix_verified_requires_fresh_read')
             and progress['current_context_available'] is False,'transport_bootstrap_progress_scope')
        for key in ('validated_observations','captured_observations','step_expanded_bytes','historical_expanded_bytes'):
            _int(progress[key],'transport_bootstrap_progress_integer')
        need(progress['validated_observations']<=progress['captured_observations']<=incremental.legacy.MAX_OBSERVATIONS
             and progress['step_expanded_bytes']<=incremental.MAX_STEP_BYTES,'transport_bootstrap_progress_bound')
    summary=value['last_success']
    if summary is None:
        need(value['successful_cycles']==0 and all(value[k] is None for k in
            ('last_success_epoch','last_consumer_observed_epoch','last_consumer_ack_epoch')),'transport_unproved_success_clock')
    else:
        keys={'run_id','snapshot_sha256','scan_started_epoch','scan_completed_epoch','source_high_watermark','scan_through_seq',
            'more_pending','caught_up','published_sequence','publication_status','consumer_receipt_sha256','consumer_ack_sha256',
            'consumer_observed_epoch','consumer_ack_epoch','new_source_entries','cycle_seconds'}
        need(type(summary) is dict and set(summary)==keys and value['successful_cycles']>0,'transport_exact_success_summary')
        need(type(summary['run_id']) is str and re.fullmatch('[0-9a-f]{32}',summary['run_id']),'transport_success_run_identity')
        for key in ('snapshot_sha256','consumer_receipt_sha256','consumer_ack_sha256'):
            need(type(summary[key]) is str and re.fullmatch('[0-9a-f]{64}',summary[key]),'transport_success_hash')
        for key in ('source_high_watermark','scan_through_seq','published_sequence','new_source_entries'):_int(summary[key],'transport_success_count')
        need(type(summary['more_pending']) is bool and type(summary['caught_up']) is bool and
             summary['scan_through_seq']<=summary['source_high_watermark'] and
             summary['more_pending']==(summary['scan_through_seq']<summary['source_high_watermark']), 'transport_success_scan_accounting')
        need(not summary['caught_up'] or (summary['more_pending'] is False and
             summary['scan_through_seq']==summary['source_high_watermark']),'transport_caught_up_with_backlog')
        times=[_epoch(summary[k]) for k in ('scan_started_epoch','scan_completed_epoch','consumer_observed_epoch','consumer_ack_epoch')]
        need(times==sorted(times) and times[-1]<=_epoch(value['last_success_epoch']) and
             value['last_consumer_observed_epoch']==summary['consumer_observed_epoch'] and
             value['last_consumer_ack_epoch']==summary['consumer_ack_epoch'],'transport_success_original_time_order')
        need(type(summary['cycle_seconds']) in (int,float) and math.isfinite(summary['cycle_seconds']) and summary['cycle_seconds']>=0,
             'transport_success_duration')
    if value['last_failure'] is not None:
        failure=value['last_failure'];need(type(failure) is dict and set(failure)=={'failure_sha256','reason','phase','observed_epoch'},'transport_failure_summary_shape')
        need(type(failure['failure_sha256']) is str and re.fullmatch('[0-9a-f]{64}',failure['failure_sha256']) and
             type(failure['reason']) is str and len(failure['reason'])<=800 and type(failure['phase']) is str and len(failure['phase'])<=64,'transport_failure_summary_types')
        _epoch(failure['observed_epoch'])


def read_transport_health(config_path,config_sha256,*,clock=time.time):
    """Read current exact status plus the durable failure prefix, never a lease.

    This does not acquire the writer lock, create files, acknowledge scans or
    change consumer/story timestamps. New events after this read remain unseen.
    """
    lexical(config_path);began=time.monotonic();read_started=_epoch(clock());owner=_owners()
    config,original,proof,io_proof=_configuration(owner,config_path,config_sha256)
    graph=_graph(owner);expected=_profile_value(owner,config,original,proof,io_proof,graph)
    root=plain(config['state_root'],directory=True)
    # Only call the read-only inventory/JSON helpers; no _State constructor,
    # writer lock, initialization, recovery or status mutation occurs here.
    view=object.__new__(_State);view.owner=owner;view.root=root;view.root_identity=view._root_identity()
    _,_,names=view._inventory();profile,profile_sha=view._json(root/'profile.json',MAX_STATUS)
    need(owner.encode(profile)==owner.encode(expected),'transport_health_profile_or_sources_changed')
    previous=None
    for sequence,name in enumerate(names,1):
        need(time.monotonic()-began<=30,'transport_health_read_duration_bound')
        need(name==f'failure-{sequence:08d}.json','transport_failure_prefix_gap')
        record,digest=view._json(root/name,MAX_FAILURE)
        required={'schema_version','profile_sha256','failure_generation','previous_generation','previous_failure_sha256',
            'run_id','attempt_count','phase','observed_epoch','observation_clock_trusted','reason',*INERT}
        need(set(record)==required and record['schema_version']==FAILURE and record['profile_sha256']==profile_sha and
             type(record['failure_generation']) is int and record['failure_generation']==sequence and
             type(record['previous_generation']) is int and record['previous_generation']==sequence-1 and
             record['previous_failure_sha256']==previous,'transport_health_failure_chain_invalid')
        need(type(record['run_id']) is str and re.fullmatch('[0-9a-f]{32}',record['run_id']) and type(record['observation_clock_trusted']) is bool and
             type(record['reason']) is str and len(record['reason'])<=800 and type(record['phase']) is str and 1<=len(record['phase'])<=64 and
             all(record[k] is v for k,v in INERT.items()),'transport_health_failure_types')
        _int(record['attempt_count'],'transport_health_attempt_count');_epoch(record['observed_epoch']);previous=digest
    status,status_sha=view._json(root/'status.json',MAX_STATUS);_validate_status(owner,status,profile,profile_sha)
    need(status['failure_generation']<=len(names),'transport_health_status_ahead_of_failures')
    coherent=status['failure_generation']==len(names)
    if coherent:need(status['latest_failure_sha256']==previous,'transport_health_failure_head_binding')
    _,end_proof=owner.read_exact(config_path,MAX_STATUS);_,end_io=owner.read_exact(config['news_io_config_path'],MAX_STATUS)
    need(end_proof['sha256']==proof['sha256'] and end_io['sha256']==io_proof['sha256'] and _graph(owner)==graph,'transport_health_sources_changed_during_read')
    view._root_check();completed=_epoch(clock())
    need(read_started<=completed and time.monotonic()-began<=30,'transport_health_read_clock_or_duration')
    return {'schema_version':HEALTH,'config_sha256':proof['sha256'],'news_io_config_sha256':io_proof['sha256'],
        'profile_sha256':profile_sha,'source_graph_sha256':profile['source_graph_sha256'],'source_bindings':graph,
        'state_root':str(root),'profile':profile,'status':status,'status_sha256':status_sha,
        'failure_generation':len(names),'latest_failure_sha256':previous,'status_matches_durable_failure_head':coherent,
        'effective_status':status['status'] if coherent else 'failed','read_started_epoch':read_started,'read_completed_epoch':completed,
        'new_failure_awareness_is_observation_based':True,'consumer_observation_performed':False,**INERT}


def require_current_transport_health(health,at):
    """Current liveness only; original scan freshness remains in the consumer."""
    at=_epoch(at);need(health['schema_version']==HEALTH and health['status_matches_durable_failure_head'] is True,
                        'transport_unreported_durable_failure')
    need(_epoch(health['read_started_epoch'])<=_epoch(health['read_completed_epoch'])<=at,
         'transport_health_observation_after_decision')
    status=health['status'];need(health['effective_status'] in ('ready','running'),'transport_not_ready:'+str(health['effective_status']))
    need(status['report_clock_trusted'] is True and status['report_epoch']<=at and 0<=at-status['report_epoch']<=90 and
         status['progress_epoch']<=at and 0<=at-status['progress_epoch']<=180,'transport_status_stale_or_future')
    last=status['last_success']
    need(last is not None and last['run_id']==status['run_id'] and last['caught_up'] is True,
         'transport_no_current_run_caught_up_success')
    need(last['consumer_ack_epoch']<=status['last_success_epoch']<=status['report_epoch']<=at,
         'transport_current_status_time_order')
    return health


if __name__=='__main__':raise SystemExit(main())
