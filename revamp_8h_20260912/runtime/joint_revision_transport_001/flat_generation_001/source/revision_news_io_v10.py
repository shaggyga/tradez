"""Staged fixed-store capture primitives; no worker, fit, migration or activation.

Readback authentication belongs to capture_shared. Reconstruction belongs to
replay_capture and cannot authorize current use. Clock callbacks are trusted
in-process clocks; the separately read aligned-clock proof remains mandatory.
"""
import copy
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import re
import stat
import sys
import threading
import time
import uuid
import weakref

import revision_joint_features_v1 as adapter
import revision_transport_v4 as transport
consumer=adapter.consumer
publisher=consumer.publisher
reader=publisher.reader
repair=reader.repair
collector=repair.collector

SCHEMA='revision_news_fixed_io_capture_v2_20260913'
CONFIG='revision_news_fixed_io_transport_config_v2_20260913'
DESCRIPTOR='revision_news_fixed_io_descriptor_v2_20260913'
HEALTH='revision_news_current_collector_transport_observation_v2_20260913'
TRANSPORT_SOURCES={'oanda_causal_forecast_inputs.py': 'f12dbce89a36356c63d6d633c652ad3c31cd2d09828d367c7512108b1e4a7281', 'oanda_causal_forecast_inputs_gap_v2.py': '539b3eed7fe4df48b4034b41bc82101c6276d88e3bb7e06e9931d3ce02234edd', 'oanda_causal_forecast_inputs_joint_news_v3.py': 'd40cf670520e09ad18527324608e60c4ace40afa6a27ca37b2fc9d80f846a4fd', 'oanda_causal_forecast_inputs_pair_v2.py': '8e58491a49fca967e6d8d2a797cc77f142053576c92a724a2e7336d855fb4899', 'oanda_isolated_news_history_v1.py': '9bc87f2072189344fdd6196e16bdad0a09bd7ea5b3c3a8bd42e20a82fedd91ab', 'oanda_joint_price_news_models_v1.py': 'eb153acb966dc04ad950a0bbfcc730e78a9a0da1d8a24e91d641f473f5cfbc23', 'oanda_local_news_sentiment.py': '44e66d85f82b52d6dc82e2e277bad31d2ee8c16304bec9c6a083a8b17eeb6c50', 'oanda_local_news_sentiment_repair_v2.py': '542e287e44330d2e83ef2ec1a9b363bc716cf5581cf8b05bce87de84c7fa4124', 'oanda_news_causal_aggregation_guard_v1.py': '2a7a05204dee5191cb3f53d3d5a8082b7e725842e6af65aa6dd1ae7f6292dcf9', 'oanda_news_causal_aggregation_guard_v2.py': 'c62a26721694f6e98e65e25ddb476d175a01fef0320f353fe92234e46d273aaf', 'oanda_news_classification_contract.py': 'f792528f398fab3e11be10f68edefd9b40fac33edd86d525534bc557943d0eae', 'oanda_news_classification_observation_v1.py': 'fd50ede0e2f5c769963e1e564a7283e0a1b105010498020bb69d894a82ffe293', 'oanda_news_collector_contract.py': '41c30599a953d0fd967bc68c5f178d46fc21dd2382ac9fdca78bc3c64ac9f68f', 'oanda_news_event_tagger.py': '3657872fa84a6f081903307df7209350fed4f713d866735b9387ce375a5e4f3f', 'oanda_news_source_observation_ledger_v1.py': '0e570e853645e67707cc22b8923a70c7860d939483bcbf5fd70c1dd45c2ad284', 'oanda_news_topic_identity_reconciliation_v2.py': 'cd239119826a33f406d4241bf43e09ee2446016eb7238cb775a11d7c88825d23', 'oanda_pair_local_models_v2.py': '294c63bf0ed873395c4850a8722a2622eaa2bc563ceb7ecf9886764c63d8bf24', 'oanda_source_governance.py': 'c4b7793dcef1b9176079a959019f8b69059fbd6644fddf1e8718f924d3a13950', 'oanda_source_governance_news_fast_lane.py': 'bdd6a2542b3dbddbdf86f7148af4dcb38d0b8591b3f3fcdb843d9eed29898f03', 'projection_revision_admission_v1.py': '27e2cfafc2b0925a8f107ef890e2121a425518467d5636ab01af66205fb61563', 'projection_revision_consumer_v1.py': '3fc28130a764e06e16552e719a59d58dc7e6cc7799a6ca5719c272dfe4c6091c', 'projection_revision_reader_v1.py': 'e8c2b927aa6f9524ddbf8e71bd7844ca7eae44a24f54095f76057f54803650f9', 'revision_joint_features_v1.py': '47841a62fb5bfc7a927cc10c1d7d9bacc8d060b8e7abcadb0420c85d8a904aba', 'revision_news_io_base_v1.py': '36bf4ec867b5a1816ff1793733747de1b95ced221ec1a7d6acd2bc14b964d8de', 'revision_transport_v4.py': 'f8d4b88c4e45505682db5a3d384969059e327c14345d15f6fec7eda07b1bcea3'}
RECIPE='revision_news_packed_rows32_readback_recipe_v2_20260913'
MAX_STATE=128*1024
MAX_LATEST=2*1024*1024
MAX_OBJECT=18*1024*1024
MAX_READBACK=96*1024*1024
MAX_FILES=32768
MAX_ARCHIVE=1024*1024*1024
MAX_SECONDS=30
BLOCK=32
CONFIG_KEYS={'schema_version','cohort_id','consumer_id','publication_path','observation_path',
    'clock_path','heartbeat_path','latest_path','archive_root','policy','input_identity','transport_config_path','transport_config_sha256'}
INERT=dict(adapter.INERT,execution_eligible=False)
_BOUND_MODULES=(
    (adapter,'47841a62fb5bfc7a927cc10c1d7d9bacc8d060b8e7abcadb0420c85d8a904aba'),
    (consumer,'3fc28130a764e06e16552e719a59d58dc7e6cc7799a6ca5719c272dfe4c6091c'),
    (publisher,'27e2cfafc2b0925a8f107ef890e2121a425518467d5636ab01af66205fb61563'),
    (reader,'e8c2b927aa6f9524ddbf8e71bd7844ca7eae44a24f54095f76057f54803650f9'),
    (adapter.original,'d40cf670520e09ad18527324608e60c4ace40afa6a27ca37b2fc9d80f846a4fd'))
need=publisher.need


def encode(value,maximum=MAX_OBJECT):
    result=bytearray()
    for chunk in json.JSONEncoder(sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).iterencode(value):
        raw=chunk.encode();need(len(result)+len(raw)<=maximum,'io_encoded_byte_bound');result.extend(raw)
    return bytes(result)


def digest(value,maximum=MAX_OBJECT):return hashlib.sha256(encode(value,maximum)).hexdigest()


def own(value,maximum=MAX_OBJECT):return json.loads(encode(value,maximum))


def path_for(value,*,directory=False,missing=False):
    text=os.fspath(value);pure=PureWindowsPath(text)
    need(pure.is_absolute() and pure.drive.lower()=='c:' and '..' not in pure.parts and
         all(':' not in part for part in pure.parts[1:]),'plain_absolute_c_path_required')
    path=Path(text)
    for item in (*reversed(path.parents),path):
        try:info=item.lstat()
        except FileNotFoundError:
            need(missing and item==path,'existing_ancestors_required');return path
        need(not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_file_attributes',0)&1024,'io_reparse_path_refused')
        need(stat.S_ISDIR(info.st_mode) if item!=path or directory else stat.S_ISREG(info.st_mode),'io_path_kind_required')
    return path


def identity(path):
    info=path_for(path).stat();return [info.st_dev,info.st_ino]


def read_exact(path,limit):
    path=path_for(path)
    with path.open('rb') as stream:
        before=os.fstat(stream.fileno());need(before.st_size<=limit,'io_source_byte_bound')
        raw=stream.read(limit+1);after=os.fstat(stream.fileno())
    current=path_for(path).stat()
    signature=lambda info:(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns)
    need(len(raw)<=limit and signature(before)==signature(after)==signature(current),'io_file_changed_during_read')
    return raw,{'path':str(path),'identity':[before.st_dev,before.st_ino],'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}


def strict_health_json(raw):
    """Closed health metadata: duplicate keys and nonfinite numbers are invalid."""
    def pairs(items):
        result={}
        for key,value in items:
            need(key not in result,'io_duplicate_health_json_key');result[key]=value
        return result
    def constant(value):
        raise ValueError('io_nonfinite_health_json_number')
    def floating(value):
        result=float(value);need(math_finite(result),'io_nonfinite_health_json_number');return result
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=constant,parse_float=floating)


def read_json(path,limit):
    raw,proof=read_exact(path,limit);value=strict_health_json(raw)
    need(type(value) is dict,'io_json_object_required')
    return value,{**proof,'raw_utf8':raw.decode('utf-8'),'value':value}


def source_graph():
    kit=path_for(Path(__file__).absolute().parent,directory=True)
    values={}
    for module,expected in _BOUND_MODULES:
        need(Path(module.__file__).absolute()==kit/Path(module.__file__).name,'io_fixed_sibling_module_required')
        raw,proof=read_exact(module.__file__,2*1024*1024)
        need(proof['sha256']==expected,'io_bound_module_source_changed');values[Path(module.__file__).name]=proof['sha256']
    root=kit
    for name,expected in adapter.BOUND_POLICY_SOURCES.items():
        path=Path(reader.__file__) if name=='projection_revision_reader_v1.py' else root/name
        _,proof=read_exact(path,2*1024*1024)
        need(proof['sha256']==expected,'io_bound_policy_source_changed');values[name]=expected
    # Include the unchanged pair-feature/numerical source closures. No imports
    # of numerical functions, fit artifacts, configuration or market data.
    for name in adapter.original.REQUIRED_SOURCE_FILES:
        _,proof=read_exact(root/name,2*1024*1024);values[name]=proof['sha256']
    need(values.get('oanda_joint_price_news_models_v1.py')==adapter.BOUND_GENERATION['numeric_model'],
         'io_numerical_source_changed')
    for name,module_path in (('revision_transport_v4.py',transport.__file__),('revision_news_io_base_v1.py',str(Path(__file__).with_name('revision_news_io_base_v1.py')))):
        _,proof=read_exact(module_path,2*1024*1024);need(proof['sha256']==TRANSPORT_SOURCES[name],'io_transport_owner_source_changed');values[name]=proof['sha256']
    need(all(values.get(name)==expected for name,expected in TRANSPORT_SOURCES.items()),'io_transport_source_closure_changed')
    _,proof=read_exact(__file__,2*1024*1024);values[Path(__file__).name]=proof['sha256']
    for name in values:
        module=sys.modules.get(name[:-3])
        need(module is None or Path(getattr(module,'__file__','')).absolute()==kit/name,'io_mixed_source_kit_module_refused')
    return values


class Session:
    __slots__=('__weakref__',)
    def __new__(cls,*args,**kwargs):raise ValueError('io_session_factory_required')
    def __setattr__(self,name,value):raise AttributeError('opaque_io_session')


class Capture:
    __slots__=('__weakref__',)
    def __new__(cls,*args,**kwargs):raise ValueError('io_capture_factory_required')
    def __setattr__(self,name,value):raise AttributeError('opaque_io_capture')


def registry(cls):
    values=weakref.WeakKeyDictionary()
    def make(value):
        key=object.__new__(cls);values[key]=value;return key
    def get(key):
        need(type(key) is cls and key in values,'registered_private_io_object_required');return values[key]
    return make,get


_make_session,_session=registry(Session)
_make_capture,_capture=registry(Capture)


def create_session(config):
    config=own(config)
    need(set(config)==CONFIG_KEYS and config['schema_version']==CONFIG,'exact_io_configuration_required')
    for key in ('cohort_id','consumer_id'):
        need(type(config[key]) is str and re.fullmatch('[A-Za-z0-9_-]{1,96}',config[key]),'explicit_io_identity_required')
    source_identity=config['input_identity']
    need(type(source_identity) is dict and set(source_identity)=={'path','device','inode'} and
         type(source_identity['path']) is str and all(type(source_identity[k]) is int and source_identity[k]>=0 for k in ('device','inode')),
         'exact_configured_source_identity_required')
    pure=PureWindowsPath(source_identity['path'])
    need(pure.is_absolute() and pure.drive.lower()=='c:' and '..' not in pure.parts and
         all(':' not in part for part in pure.parts[1:]),'plain_configured_source_c_path_required')
    # No source-database filesystem probe/read here: identity is authenticated
    # by the exact retained source scan and publisher/consumer profiles.
    transport.lexical(config['transport_config_path'])
    need(type(config['transport_config_sha256']) is str and re.fullmatch('[0-9a-f]{64}',config['transport_config_sha256']),'io_transport_config_hash_required')
    _transport_base(config)
    paths=[]
    for key in ('publication_path','observation_path','clock_path','heartbeat_path','latest_path'):
        path=path_for(config[key],missing=True);need(str(path)==config[key],'canonical_explicit_path_required');paths.append(str(path).lower())
    need(len(paths)==len(set(paths)),'distinct_io_paths_required')
    need(Path(config['heartbeat_path']).name=='collector_heartbeat_v1.json' and Path(config['latest_path']).name=='collector_latest_v1.json','fixed_health_filenames_required')
    root=path_for(config['archive_root'],directory=True)
    need(all(not Path(p).is_relative_to(root) for p in paths),'archive_must_differ_from_input_paths')
    publisher.policy_current(config['policy'])
    need(encode(config['policy']['source_bindings'])==encode(adapter.BOUND_POLICY_SOURCES),'exact_adapter_source_policy_required')
    return _make_session({'config':encode(config),'graph':source_graph(),'failure_serial':0,'usable':False,
        'last_failure':None,'transport_failure_generation':None,'lock':threading.RLock()})


def session_status(session):
    state=_session(session)
    with state['lock']:return own({k:state[k] for k in ('failure_serial','usable','last_failure')})


def archive_capacity(session):
    """Explicit guarded inventory snapshot, never an automatic hot-path scan.

    The caller chooses when to request diagnostics. Existing session_status,
    capture and per-issued health operations do not call this function.
    This is single-owner metadata accounting, not a capacity reservation,
    content-integrity proof, health observation or 48-hour runtime guarantee.
    """
    state=_session(session);config=json.loads(state['config'])
    with state['lock']:
        try:
            wall=time.monotonic()
            need(source_graph()==state['graph'],'io_capacity_source_graph_changed')
            archive=Archive(config['archive_root'])
            used_files=len(archive.files);used_bytes=archive.total
            need(source_graph()==state['graph'],'io_capacity_source_changed_during_inventory')
            elapsed=time.monotonic()-wall
            need(elapsed<=MAX_SECONDS,'io_capacity_inventory_duration_bound')
            return {'schema_version':'revision_news_archive_capacity_snapshot_v1_20260913',
                    'archive_root':str(archive.root),'config_sha256':digest(config),
                    'source_graph_sha256':digest(state['graph']),
                    'used_files':used_files,'remaining_files':MAX_FILES-used_files,'file_limit':MAX_FILES,
                    'used_bytes':used_bytes,'remaining_bytes':MAX_ARCHIVE-used_bytes,'byte_limit':MAX_ARCHIVE,
                    'object_byte_limit':MAX_OBJECT,'reconstruction_byte_limit':MAX_READBACK,
                    'cooperative_operation_seconds':MAX_SECONDS,'inventory_elapsed_seconds':elapsed,
                    'scope':'exact_guarded_single_owner_inventory_snapshot_not_reservation',
                    'orphan_and_failed_write_files_included':True,'content_integrity_checked':False,
                    'cross_process_reservation':False,'failure_capacity_reserved':False,
                    'forty_eight_hour_operation_guaranteed':False,'fresh_health_proven':False,**INERT}
        except Exception as exc:
            _failure(state,config,'archive_capacity',exc);raise


def _tick(clock,previous=None):
    now=adapter.epoch(clock())
    need(previous is None or now>=previous,'io_clock_went_backwards');return now


class HealthFailure(ValueError):
    def __init__(self,reason,evidence):
        super().__init__(reason);self.evidence=evidence


def _validate_health_files(evidence,config,at):
    need(type(evidence) is dict and evidence.get('schema_version')==HEALTH,'io_health_evidence_schema')
    started=adapter.epoch(evidence['read_started_epoch']);observed=adapter.epoch(evidence['observed_epoch'])
    need(started<=observed<=at and observed-started<=MAX_SECONDS,'io_health_read_duration_bound')
    need(set(evidence['files'])=={'clock','heartbeat','latest'},'io_complete_health_files_required')
    for key,field,limit in (('clock','clock_path',MAX_STATE),('heartbeat','heartbeat_path',MAX_STATE),('latest','latest_path',MAX_LATEST)):
        item=evidence['files'][key]
        need(set(item)=={'path','identity','bytes','sha256','raw_utf8','value'} and item['path']==config[field],
             'io_health_file_path_binding')
        need(type(item['identity']) is list and len(item['identity'])==2 and all(type(v) is int for v in item['identity']),
             'io_health_file_identity_type')
        need(type(item['raw_utf8']) is str and type(item['bytes']) is int,'io_health_file_byte_types')
        raw=item['raw_utf8'].encode('utf-8')
        need(len(raw)==item['bytes']<=limit and hashlib.sha256(raw).hexdigest()==item['sha256'] and
             encode(strict_health_json(raw))==encode(item['value']),'io_health_file_exact_bytes')
    integrity=evidence['files']['clock']['value'];heartbeat=evidence['files']['heartbeat']['value'];latest=evidence['files']['latest']['value']
    repair.validate_clock_state(integrity,started);repair.validate_clock_state(integrity,at)
    for field in ('generated_utc','last_progress_utc','cycle_started_utc'):
        need(type(heartbeat.get(field)) is str,'io_heartbeat_utc_text_required')
    repair.validate_collector_observation(heartbeat,at)
    for key,expected in (('collector_contract_id',collector.COLLECTOR_CONTRACT_ID),('collector_cohort_id',collector.COLLECTOR_COHORT_ID)):
        need(heartbeat.get(key)==expected,'io_heartbeat_collector_identity_mismatch')
    need(type(latest.get('generated_utc')) is str,'io_latest_utc_text_required')
    report=repair.epoch(latest['generated_utc']);need(math_finite(report) and report<=observed,'io_latest_report_future_or_invalid')
    status=latest.get('status')
    need(status not in {'error','blocked_clock_integrity','blocked_clock_integrity_at_completion'},'io_known_current_collector_failure:'+str(status))
    need(status=='ok' and latest.get('schema_version')==collector.SCHEMA_VERSION,'io_latest_status_or_schema_unsupported')
    for key,expected in (('collector_contract_id',collector.COLLECTOR_CONTRACT_ID),('collector_cohort_id',collector.COLLECTOR_COHORT_ID)):
        need(latest.get(key)==expected,'io_latest_collector_identity_mismatch')
    policy=latest.get('policy')
    need(type(policy) is dict and policy.get('research_only') is True and policy.get('execution_eligible') is False,'io_latest_research_policy_required')
    _validate_transport_evidence(evidence['transport'],config,at)
    need(evidence['transport_sha256']==digest(evidence['transport']) and started<=evidence['transport']['read_started_epoch']<=evidence['transport']['read_completed_epoch']<=observed,'io_transport_health_read_order')
    return report


def _health(config,clock,state=None):
    started=_tick(clock);evidence={'schema_version':HEALTH,'read_started_epoch':started,'files':{},**INERT}
    try:
        integrity,proof=read_json(config['clock_path'],MAX_STATE);evidence['files']['clock']=proof
        heartbeat,proof=read_json(config['heartbeat_path'],MAX_STATE);evidence['files']['heartbeat']=proof
        latest,proof=read_json(config['latest_path'],MAX_LATEST);evidence['files']['latest']=proof
        evidence['transport']=transport.read_transport_health(config['transport_config_path'],config['transport_config_sha256'],clock=clock)
        evidence['transport_sha256']=digest(evidence['transport'])
        _observe_transport_generation(state,evidence)
        observed=_tick(clock,started);evidence['observed_epoch']=observed
        report=_validate_health_files(evidence,config,observed)
        status=latest['status'];evidence.update(latest_status=status,latest_generated_epoch=report,latest_report_age_sec=observed-report)
        evidence.update(status='observed_active_no_known_whole_collector_failure',all_sources_fresh_or_healthy_proven=False,
            scope='current_only; liveness is not upstream fetch completeness; completed report age is retained without a new maximum')
        return evidence
    except Exception as exc:
        evidence.update(status='unavailable',error=type(exc).__name__+':'+str(exc)[:500])
        raise HealthFailure(str(exc),evidence) from exc


def math_finite(value):return type(value) in (int,float) and value==value and value not in (float('inf'),-float('inf'))


class _DecodeBudget:
    """Conservative encoded-work budget, not a guarantee about Python heap size."""
    def __init__(self):
        self.used=0
        # Fixed reconstructed dictionary framing; recipe/reference bytes are
        # additionally charged although not all appear in the final readback.
        self.reserve(256)
    def reserve(self,size):
        need(type(size) is int and size>=0 and self.used+size<=MAX_READBACK,'io_reconstruction_encoded_work_bound')
        self.used+=size


class Archive:
    """Owned content-addressed objects and bounded32-row sequence chunks."""
    def __init__(self,root):
        self.root=path_for(root,directory=True);self.files={};self.total=0
        root_identity=self.root.stat()
        with os.scandir(self.root) as entries:
            for entry in entries:
                need(re.fullmatch('[0-9a-f]{64}\\.json',entry.name) is not None,'unexpected_capture_archive_member')
                need(len(self.files)<MAX_FILES,'capture_archive_file_bound')
                # Only flat leaf metadata is read here. Root/ancestors are
                # checked before and after the bounded inventory; each leaf
                # receives its own non-following type/reparse check.
                info=entry.stat(follow_symlinks=False)
                need(stat.S_ISREG(info.st_mode) and not getattr(info,'st_file_attributes',0)&1024,
                     'io_reparse_path_refused')
                size=info.st_size
                need(size<=MAX_OBJECT,'capture_archive_object_bound');self.total+=size
                need(self.total<=MAX_ARCHIVE,'capture_archive_total_byte_bound');self.files[entry.name]=size
        end=path_for(self.root,directory=True).stat()
        need((root_identity.st_dev,root_identity.st_ino)==(end.st_dev,end.st_ino),'capture_archive_root_replaced')
    def put(self,value):
        raw=encode(value);key=hashlib.sha256(raw).hexdigest();name=key+'.json';path=self.root/name
        if name in self.files:
            old,_=read_exact(path,MAX_OBJECT);need(old==raw,'immutable_capture_object_collision');return key
        need(len(self.files)<MAX_FILES and self.total+len(raw)<=MAX_ARCHIVE,'capture_archive_capacity_exhausted')
        path_for(path,missing=True)
        # Single fixed session serializes this store; an existing object is
        # compared exactly. Partial writes remain rejected, never rehabilitated.
        try:
            with path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
        except FileExistsError:
            old,_=read_exact(path,MAX_OBJECT);need(old==raw,'immutable_capture_object_collision')
        old,_=read_exact(path,MAX_OBJECT);need(old==raw,'capture_object_readback_failed')
        self.files[name]=len(raw);self.total+=len(raw);return key
    def get(self,key,*,budget=None):
        need(type(key) is str and re.fullmatch('[0-9a-f]{64}',key),'capture_object_hash_required')
        raw,_=read_exact(self.root/(key+'.json'),MAX_OBJECT)
        need(hashlib.sha256(raw).hexdigest()==key,'capture_object_hash_mismatch')
        if budget is not None:budget.reserve(len(raw))
        value=json.loads(raw)
        need(encode(value)==raw,'canonical_capture_object_required');return value
    def sequence(self,rows):
        need(type(rows) is list and len(rows)<=4096,'capture_sequence_bound')
        # Each immutable object contains at most32 rows. Full blocks remain
        # stable when an observation is appended; only the last partial block
        # is rewritten as a new content hash. No row/clock/value is changed.
        return {'encoding':'canonical_rows32_v1','count':len(rows),
                'blocks':[self.put(rows[i:i+BLOCK]) for i in range(0,len(rows),BLOCK)]}
    def unsequence(self,recipe,*,budget=None):
        need(type(recipe) is dict and set(recipe)=={'encoding','count','blocks'} and
             recipe['encoding']=='canonical_rows32_v1' and type(recipe['count']) is int and
             0<=recipe['count']<=4096,'capture_sequence_recipe_invalid')
        need(type(recipe['blocks']) is list and len(recipe['blocks'])==(recipe['count']+BLOCK-1)//BLOCK,'capture_block_count_invalid')
        result=[]
        for index,key in enumerate(recipe['blocks']):
            rows=self.get(key,budget=budget)
            need(type(rows) is list and len(rows)==min(BLOCK,recipe['count']-index*BLOCK),'capture_block_size_invalid')
            result.extend(rows)
        return result


def _store_bundle(archive,bundle):
    publication=bundle['publication_integrity']
    root={key:value for key,value in bundle.items() if key not in ('publication_integrity','observations','scan_objects')}
    header={key:value for key,value in publication.items() if key not in ('batches','publication_heads')}
    return archive.put({'schema_version':RECIPE,'root':archive.put(root),'publication':archive.put(header),
        'batches':archive.sequence(publication['batches']),'heads':archive.sequence(publication['publication_heads']),
        'observations':archive.sequence(bundle['observations']),
        'scan_objects':archive.sequence([[key,value] for key,value in sorted(bundle['scan_objects'].items())])})


def _load_bundle(archive,key):
    budget=_DecodeBudget()
    recipe=archive.get(key,budget=budget)
    need(type(recipe) is dict and set(recipe)=={'schema_version','root','publication','batches','heads','observations','scan_objects'} and recipe['schema_version']==RECIPE,'capture_bundle_recipe_required')
    root=archive.get(recipe['root'],budget=budget);pub=archive.get(recipe['publication'],budget=budget)
    need(type(root) is dict and not set(root)&{'publication_integrity','observations','scan_objects'} and type(pub) is dict and not set(pub)&{'batches','publication_heads'},'capture_header_fields_invalid')
    pairs=archive.unsequence(recipe['scan_objects'],budget=budget);objects={}
    for pair in pairs:
        need(type(pair) is list and len(pair)==2 and type(pair[0]) is str and pair[0] not in objects,'capture_scan_objects_invalid');objects[pair[0]]=pair[1]
    result={**root,'publication_integrity':{**pub,'batches':archive.unsequence(recipe['batches'],budget=budget),'publication_heads':archive.unsequence(recipe['heads'],budget=budget)},
        'observations':archive.unsequence(recipe['observations'],budget=budget),'scan_objects':objects}
    encode(result,MAX_READBACK);return result


def _validate_capture_value(value,config,archive):
    fields={'schema_version','config_sha256','source_graph','bundle_recipe_sha256','readback_sha256','readback_bytes',
        'context_metadata','store_identities','health_before_sha256','health_after_sha256','read_started_epoch',
        'read_completed_epoch','first_observed_epoch','actual_database_capture_performed','historical_collector_health_proven',
        'semantic_complete_validations','capture_scope','transport_failure_generation',*INERT}
    need(type(value) is dict and set(value)==fields and value['schema_version']==SCHEMA and value['config_sha256']==digest(config),
         'io_exact_capture_value_required')
    need(all(value[k] is v for k,v in INERT.items()) and value['actual_database_capture_performed'] is True and
         value['historical_collector_health_proven'] is False and type(value['semantic_complete_validations']) is int and value['semantic_complete_validations']==2,
         'io_capture_claim_types_required')
    need(type(value['readback_bytes']) is int and 0<=value['readback_bytes']<=MAX_READBACK,'io_capture_readback_size_type')
    need(value['capture_scope']=='fixed_actual_committed_store_reads_then_immutable_byte_readback','io_capture_scope_required')
    begun,read,observed=(adapter.epoch(value[k]) for k in ('read_started_epoch','read_completed_epoch','first_observed_epoch'))
    need(begun<=read<=observed and observed-begun<=MAX_SECONDS,'io_capture_clock_binding')
    need(type(value['store_identities']) is dict and set(value['store_identities'])=={'publication_path','observation_path'} and
         all(type(v) is list and len(v)==2 and all(type(x) is int for x in v) for v in value['store_identities'].values()),'io_store_identity_type')
    for key in ('bundle_recipe_sha256','readback_sha256','health_before_sha256','health_after_sha256'):
        need(type(value[key]) is str and re.fullmatch('[0-9a-f]{64}',value[key]),'io_capture_hash_required')
    before=archive.get(value['health_before_sha256']);after=archive.get(value['health_after_sha256'])
    _validate_health_files(before,config,observed);_validate_health_files(after,config,observed)
    need(type(value['transport_failure_generation']) is int and value['transport_failure_generation']==before['transport']['failure_generation']==after['transport']['failure_generation'],'io_capture_transport_generation_binding')
    need(begun<=before['read_started_epoch']<=before['observed_epoch']<=read<=after['read_started_epoch']<=after['observed_epoch']<=observed,
         'io_capture_health_order')


def _failure(state,config,operation,exc):
    state['failure_serial']+=1;state['usable']=False
    value={'schema_version':SCHEMA,'status':'failed','operation':operation,'failure_serial':state['failure_serial'],
        'error':type(exc).__name__+':'+str(exc)[:500],'health_evidence':getattr(exc,'evidence',None),**INERT}
    try:value['retained_failure_sha256']=Archive(config['archive_root']).put(value)
    except Exception as storage:value['failure_retention_error']=type(storage).__name__+':'+str(storage)[:300]
    state['last_failure']=own(value)


def _original_receipts_by_read(bundle,read_completed_epoch):
    """Original committed observation/ack clocks cannot follow our actual read.

    The fixed consumer has already validated these retained receipt fields.
    This is a cross-clock bound, not another complete semantic validation or
    a restriction on an article's future release/embargo timestamps.
    """
    cutoff=adapter.epoch(read_completed_epoch)
    for item in bundle['observations']:
        observed=adapter.epoch(item['observation']['consumer_observed_epoch'])
        acknowledged=adapter.epoch(item['acknowledgment']['receipt_observed_epoch'])
        need(observed<=acknowledged<=cutoff,'io_original_consumer_receipt_after_actual_read')


def capture_shared(session,*,clock=time.time):
    state=_session(session);config=json.loads(state['config'])
    with state['lock']:
        try:
            begun=_tick(clock);wall=time.monotonic();graph=source_graph()
            need(graph==state['graph'],'io_source_graph_changed')
            before=_health(config,clock,state)
            identities={key:identity(config[key]) for key in ('publication_path','observation_path')}
            # Fixed actual I/O call. No external bundle or validator argument.
            bundle=consumer.read_observations(config['publication_path'],config['observation_path'],
                cohort_id=config['cohort_id'],consumer_id=config['consumer_id'],expected_policy=config['policy'],input_identity=config['input_identity'])
            read_completed=_tick(clock,before['observed_epoch'])
            _original_receipts_by_read(bundle,read_completed)
            prepared=adapter.prepare_revision_context(bundle);meta=adapter.revision_context_metadata(prepared)
            need(meta['context_sha256']==digest(bundle,MAX_READBACK),'io_exact_readback_hash_required')
            after=_health(config,clock,state)
            need(after['read_started_epoch']>=read_completed,'io_post_health_clock_order')
            need(identities=={key:identity(config[key]) for key in identities},'io_store_replaced_during_capture')
            need(source_graph()==graph,'io_source_changed_during_capture')
            archive=Archive(config['archive_root']);recipe=_store_bundle(archive,bundle)
            # Durable reconstruction is a byte check, not a third semantic
            # validation. Both semantic validations already occurred above.
            rebuilt=_load_bundle(archive,recipe)
            need(digest(rebuilt,MAX_READBACK)==meta['context_sha256'],'io_durable_bundle_readback_mismatch')
            completed=_tick(clock,after['observed_epoch'])
            repair.validate_clock_state(before['files']['clock']['value'],completed)
            repair.validate_clock_state(after['files']['clock']['value'],completed)
            repair.validate_collector_observation(after['files']['heartbeat']['value'],completed)
            need(completed-begun<=MAX_SECONDS and time.monotonic()-wall<=MAX_SECONDS,'io_capture_duration_bound')
            value={'schema_version':SCHEMA,'config_sha256':digest(config),'source_graph':graph,'bundle_recipe_sha256':recipe,
                'readback_sha256':meta['context_sha256'],'readback_bytes':meta['readback_bytes'],'context_metadata':meta,
                'store_identities':identities,'health_before_sha256':archive.put(before),'health_after_sha256':archive.put(after),
                'read_started_epoch':begun,'read_completed_epoch':read_completed,'first_observed_epoch':completed,
                'actual_database_capture_performed':True,'historical_collector_health_proven':False,
                'semantic_complete_validations':2,'capture_scope':'fixed_actual_committed_store_reads_then_immutable_byte_readback',
                'transport_failure_generation':state['transport_failure_generation'],**INERT}
            _validate_capture_value(value,config,archive)
            key=archive.put(value);need(archive.get(key)==value,'io_capture_descriptor_readback_failed')
            need(source_graph()==graph,'io_source_changed_before_return')
            returned=_tick(clock,completed)
            _validate_health_files(before,config,returned);_validate_health_files(after,config,returned)
            need(returned-begun<=MAX_SECONDS and time.monotonic()-wall<=MAX_SECONDS,'io_capture_final_duration_bound')
            descriptor={'schema_version':DESCRIPTOR,'capture_sha256':key,'capture_path':str(archive.root/(key+'.json'))}
            state['usable']=True
            return _make_capture((prepared,encode(value),encode(descriptor),session,state['failure_serial'],'actual_capture'))
        except Exception as exc:
            _failure(state,config,'capture_shared',exc);raise


def capture_metadata(capture):
    prepared,body,descriptor,session,serial,scope=_capture(capture)
    return {'descriptor':json.loads(descriptor),'capture':json.loads(body),'scope':scope,
        'current_use_requires_session_health_guard':True,'replay_is_not_fresh_capture':scope=='immutable_replay'}


def replay_capture(session,descriptor):
    state=_session(session);config=json.loads(state['config']);descriptor=own(descriptor)
    need(set(descriptor)=={'schema_version','capture_sha256','capture_path'} and descriptor['schema_version']==DESCRIPTOR,'exact_io_descriptor_required')
    archive=Archive(config['archive_root']);key=descriptor['capture_sha256']
    need(str(archive.root/(str(key)+'.json'))==descriptor['capture_path'],'io_descriptor_path_binding')
    value=archive.get(key)
    _validate_capture_value(value,config,archive)
    need(value['schema_version']==SCHEMA and value['config_sha256']==digest(config) and value['source_graph']==source_graph()==state['graph'],'io_replay_source_or_config_mismatch')
    bundle=_load_bundle(archive,value['bundle_recipe_sha256'])
    need(digest(bundle,MAX_READBACK)==value['readback_sha256'] and len(encode(bundle,MAX_READBACK))==value['readback_bytes'],'io_replay_readback_binding')
    _original_receipts_by_read(bundle,value['read_completed_epoch'])
    prepared=adapter.prepare_revision_context(bundle)
    need(adapter.revision_context_metadata(prepared)==value['context_metadata'],'io_replay_context_binding')
    return _make_capture((prepared,encode(value),encode(descriptor),None,None,'immutable_replay'))


def historical_features(capture,requests):
    return adapter.features_for_origin_requests(_capture(capture)[0],requests)


def _eligible(session,capture):
    state=_session(session);parts=_capture(capture)
    need(parts[3] is session and parts[5]=='actual_capture' and parts[4]==state['failure_serial'] and state['usable'],'io_capture_invalidated_or_replay_only')
    return state,parts


def current_pair_features(session,capture,instrument,decision_epoch):
    state=_session(session)
    with state['lock']:
        _,parts=_eligible(session,capture);value=json.loads(parts[1]);decision=adapter.epoch(decision_epoch)
        need(decision>=value['first_observed_epoch'],'io_decision_before_capture')
        return adapter.pair_features_as_of(parts[0],instrument,decision)


def reobserve_before_issue(session,capture,*,clock=time.time):
    """Current health only. Caller retains original fit/input/expiry guards."""
    state=_session(session);config=json.loads(state['config'])
    with state['lock']:
        # A stale, replayed or foreign handle is not a newly attempted I/O failure.
        _,parts=_eligible(session,capture)
        try:
            wall=time.monotonic()
            need(source_graph()==state['graph'],'io_source_graph_changed')
            evidence=_health(config,clock,state)
            need(evidence['observed_epoch']>=json.loads(parts[1])['first_observed_epoch'],'io_issue_before_capture')
            proof={'schema_version':SCHEMA,'operation':'current_health_before_issue','capture_sha256':json.loads(parts[2])['capture_sha256'],
                'health':evidence,'source_graph':state['graph'],'news_features_replaced':False,'news_clock_refreshed':False,
                'orders_or_forecast_issue_authorized':False,**INERT}
            archive=Archive(config['archive_root']);key=archive.put(proof);need(archive.get(key)==proof,'io_issue_health_readback_failed')
            readback_observed=_tick(clock,evidence['observed_epoch'])
            _validate_health_files(evidence,config,readback_observed)
            need(readback_observed-evidence['read_started_epoch']<=MAX_SECONDS and time.monotonic()-wall<=MAX_SECONDS,'io_issue_health_duration_bound')
            need(source_graph()==state['graph'],'io_source_changed_during_issue_health')
            return {'health_proof_sha256':key,'observed_epoch':evidence['observed_epoch'],'readback_observed_epoch':readback_observed,
                'scope':'current_health_only_retained_input_expiry_guards_still_required',**INERT}
        except Exception as exc:
            _failure(state,config,'reobserve_before_issue',exc);raise


def scheduling_signature(session,*,clock=time.time):
    """Data/control identity hint, never health or feature authorization."""
    state=_session(session);config=json.loads(state['config'])
    with state['lock']:
        try:
            need(source_graph()==state['graph'],'io_source_graph_changed')
            health=_health(config,clock,state);parts={key:health['files'][key]['sha256'] for key in ('heartbeat','latest','clock')}
            parts['transport_status_sha256']=health['transport']['status_sha256'];parts['transport_failure_generation']=health['transport']['failure_generation']
            for key in ('publication_path','observation_path'):
                path=path_for(config[key]);info=path.stat();parts[key]=[info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns]
            return {'signature_sha256':digest(parts),'observed_epoch':health['observed_epoch'],'scope':'scheduling_hint_only',**INERT}
        except Exception as exc:
            _failure(state,config,'scheduling_signature',exc);raise


def _transport_base(config):
    owner=transport._owners()
    transport_config,base,_,_=transport._configuration(owner,config['transport_config_path'],config['transport_config_sha256'])
    expected={k:v for k,v in config.items() if k not in ('transport_config_path','transport_config_sha256')}
    expected['schema_version']=owner.CONFIG
    need(encode(expected)==encode(base),'io_transport_base_configuration_mismatch')
    return transport_config


def _validate_transport_evidence(value,config,at):
    fields={'schema_version','config_sha256','news_io_config_sha256','profile_sha256','source_graph_sha256','source_bindings',
        'state_root','profile','status','status_sha256','failure_generation','latest_failure_sha256','status_matches_durable_failure_head',
        'effective_status','read_started_epoch','read_completed_epoch','new_failure_awareness_is_observation_based','consumer_observation_performed',*transport.INERT}
    need(type(value) is dict and set(value)==fields and value['schema_version']==transport.HEALTH and
         value['config_sha256']==config['transport_config_sha256'] and encode(value['source_bindings'])==encode(TRANSPORT_SOURCES),
         'io_exact_transport_health_source_binding')
    profile=value['profile']
    need(digest(profile)==value['profile_sha256'] and profile['config_path']==config['transport_config_path'] and
         profile['config_sha256']==config['transport_config_sha256'] and profile['source_graph_sha256']==digest(TRANSPORT_SOURCES) and
         encode(profile['source_bindings'])==encode(TRANSPORT_SOURCES) and profile['input_identity_sha256']==digest(config['input_identity']) and
         profile['policy_sha256']==digest(config['policy']) and profile['cohort_id']==config['cohort_id'] and profile['consumer_id']==config['consumer_id'] and
         value['state_root']==profile['state_root'] and value['news_io_config_sha256']==profile['news_io_config_sha256'] and
         value['source_graph_sha256']==profile['source_graph_sha256'],'io_transport_profile_binding')
    transport._validate_status(publisher,value['status'],profile,value['profile_sha256'])
    need(digest(value['status'])==value['status_sha256'] and type(value['failure_generation']) is int and value['failure_generation']>=0 and
         value['failure_generation']==value['status']['failure_generation'] and value['latest_failure_sha256']==value['status']['latest_failure_sha256'] and
         value['effective_status']==value['status']['status'] and value['new_failure_awareness_is_observation_based'] is True and
         value['consumer_observation_performed'] is False and all(value[k] is v for k,v in transport.INERT.items()),'io_transport_status_binding')
    transport.require_current_transport_health(value,at)


def _observe_transport_generation(state,evidence):
    if state is None:return
    value=evidence['transport'];generation=value['failure_generation']
    need(type(generation) is int and generation>=0,'io_transport_failure_generation_type')
    previous=state['transport_failure_generation']
    if previous is None:state['transport_failure_generation']=generation;return
    need(generation>=previous,'io_transport_failure_generation_regressed')
    if generation>previous:
        state['transport_failure_generation']=generation
        raise HealthFailure('io_transport_failure_generation_advanced',evidence)
