"""Staged fixed-store capture primitives; no worker, fit, migration or activation.

Readback authentication belongs to capture_shared. Reconstruction belongs to
replay_capture and cannot authorize current use. Clock callbacks are trusted
in-process clocks; the separately read aligned-clock proof remains mandatory.
"""
import copy
import base64
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import re
import stat
import sys
import threading
import tempfile
import time
import uuid
import weakref

import revision_joint_features_v1 as adapter
import revision_transport_v7 as transport
capacity_policy=transport.capacity_policy
import projection_revision_consumer_v2 as incremental
consumer=adapter.consumer
publisher=consumer.publisher
reader=publisher.reader
repair=reader.repair
collector=repair.collector

SCHEMA='revision_news_resumable_io_capture_capacity_v4_20260930'
CONFIG='revision_news_resumable_io_transport_config_capacity_v4_20260930'
DESCRIPTOR='revision_news_resumable_io_descriptor_capacity_v4_20260930'
HEALTH='revision_news_resumable_collector_transport_observation_capacity_v4_20260930'
TRANSPORT_SOURCES = {'compact_projection_store_v1.py': 'c13b3a0f73949be8155e1e626462607207cad99eabaf7b90db964487111c67b0', 'revision_joint_features_v1.py': '077a32c270a9f776cfc7e2c7604af1bcd671e1a530e5b6638d46f1a9c2f52c7d', 'projection_revision_consumer_v1.py': '5c885ae837394398a2dba3dd0b8050305ac17cb95cc5e05578042ab2551ec485', 'projection_revision_admission_v1.py': '28e4738317c3166deb656f9935e356dc8314110d056e0a471fe8eb96b766da3d', 'projection_revision_reader_v1.py': 'e8c2b927aa6f9524ddbf8e71bd7844ca7eae44a24f54095f76057f54803650f9', 'oanda_causal_forecast_inputs_joint_news_v3.py': 'd40cf670520e09ad18527324608e60c4ace40afa6a27ca37b2fc9d80f846a4fd', 'oanda_local_news_sentiment.py': '44e66d85f82b52d6dc82e2e277bad31d2ee8c16304bec9c6a083a8b17eeb6c50', 'oanda_local_news_sentiment_repair_v2.py': '542e287e44330d2e83ef2ec1a9b363bc716cf5581cf8b05bce87de84c7fa4124', 'oanda_news_causal_aggregation_guard_v1.py': '2a7a05204dee5191cb3f53d3d5a8082b7e725842e6af65aa6dd1ae7f6292dcf9', 'oanda_news_causal_aggregation_guard_v2.py': 'c62a26721694f6e98e65e25ddb476d175a01fef0320f353fe92234e46d273aaf', 'oanda_news_classification_contract.py': 'f792528f398fab3e11be10f68edefd9b40fac33edd86d525534bc557943d0eae', 'oanda_news_classification_observation_v1.py': 'fd50ede0e2f5c769963e1e564a7283e0a1b105010498020bb69d894a82ffe293', 'oanda_news_collector_contract.py': '41c30599a953d0fd967bc68c5f178d46fc21dd2382ac9fdca78bc3c64ac9f68f', 'oanda_news_event_tagger.py': '3657872fa84a6f081903307df7209350fed4f713d866735b9387ce375a5e4f3f', 'oanda_news_source_observation_ledger_v1.py': '0e570e853645e67707cc22b8923a70c7860d939483bcbf5fd70c1dd45c2ad284', 'oanda_news_topic_identity_reconciliation_v2.py': 'cd239119826a33f406d4241bf43e09ee2446016eb7238cb775a11d7c88825d23', 'oanda_source_governance_news_fast_lane.py': 'bdd6a2542b3dbddbdf86f7148af4dcb38d0b8591b3f3fcdb843d9eed29898f03', 'oanda_causal_forecast_inputs.py': 'f12dbce89a36356c63d6d633c652ad3c31cd2d09828d367c7512108b1e4a7281', 'oanda_pair_local_models_v2.py': '294c63bf0ed873395c4850a8722a2622eaa2bc563ceb7ecf9886764c63d8bf24', 'oanda_source_governance.py': 'c4b7793dcef1b9176079a959019f8b69059fbd6644fddf1e8718f924d3a13950', 'oanda_joint_price_news_models_v1.py': 'eb153acb966dc04ad950a0bbfcc730e78a9a0da1d8a24e91d641f473f5cfbc23', 'oanda_isolated_news_history_v1.py': '9bc87f2072189344fdd6196e16bdad0a09bd7ea5b3c3a8bd42e20a82fedd91ab', 'oanda_causal_forecast_inputs_pair_v2.py': '8e58491a49fca967e6d8d2a797cc77f142053576c92a724a2e7336d855fb4899', 'oanda_causal_forecast_inputs_gap_v2.py': '539b3eed7fe4df48b4034b41bc82101c6276d88e3bb7e06e9931d3ce02234edd', 'revision_news_io_base_v1.py': 'b33240f730d261875f7a23c3223e192cd6a7a7b130bcdd666749bfadf13ac327', 'revision_news_capacity_policy_v1.py': '1eb2262e5b3694fc23238ad10cc25c2095730a4bc15278cbbcf565894023118b', 'revision_transport_v7.py': '11cb3969f7eb05fa85ca475eb3a79a6f21fdf3ace60807706f34f06f5d1fbfd4', 'projection_revision_consumer_v2.py': '6936a4e24a06e3c21c468004ac5478ba4a882c3928943fd09e9dc9da15bc7fe6'}
RECIPE='revision_news_compact_cas_recipe_v1_20260914'
MAX_STATE=128*1024
MAX_LATEST=2*1024*1024
MAX_OBJECT=18*1024*1024
MAX_READBACK=96*1024*1024
MAX_FILES=131072
MAX_ARCHIVE=4*1024*1024*1024
MAX_SECONDS=30
MAX_BOOTSTRAP_STAGE_SECONDS=630
MAX_BOOTSTRAP_SECONDS=2*MAX_BOOTSTRAP_STAGE_SECONDS
BLOCK=32
CATALOG_LEAF=256
PACK_BYTES=8*1024*1024
MAX_SPOOL=1024*1024*1024
STREAMED_RECIPE='revision_news_packed_cas_archive_v2_20260914'
CONFIG_KEYS={'schema_version','cohort_id','consumer_id','publication_path','observation_path',
    'clock_path','heartbeat_path','latest_path','archive_root','policy','input_identity','transport_config_path','transport_config_sha256','reader_profile_path','reader_profile_sha256','reader_cache_path'}
INERT=dict(adapter.INERT,execution_eligible=False)
_BOUND_MODULES=(
    (consumer.compact,'c13b3a0f73949be8155e1e626462607207cad99eabaf7b90db964487111c67b0'),
    (adapter,'077a32c270a9f776cfc7e2c7604af1bcd671e1a530e5b6638d46f1a9c2f52c7d'),
    (consumer,'5c885ae837394398a2dba3dd0b8050305ac17cb95cc5e05578042ab2551ec485'),
    (publisher,'28e4738317c3166deb656f9935e356dc8314110d056e0a471fe8eb96b766da3d'),
    (reader,'e8c2b927aa6f9524ddbf8e71bd7844ca7eae44a24f54095f76057f54803650f9'),
    (adapter.original,'d40cf670520e09ad18527324608e60c4ace40afa6a27ca37b2fc9d80f846a4fd'),
)
need=publisher.need


def encode(value,maximum=MAX_OBJECT):
    result=bytearray()
    for chunk in json.JSONEncoder(sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).iterencode(value):
        raw=chunk.encode();need(len(result)+len(raw)<=maximum,'io_encoded_byte_bound');result.extend(raw)
    return bytes(result)


def digest(value,maximum=MAX_OBJECT):return hashlib.sha256(encode(value,maximum)).hexdigest()


def own(value,maximum=MAX_OBJECT):return json.loads(encode(value,maximum))


def _compact_encoded_size(value):
    """Exact bound for the closed ASCII compact-object schema, before encoding."""
    need(type(value) is dict and set(value)=={'kind','object_sha','expanded_bytes','packed_sha','zlib_base64'},
         'compact_fast_object_shape')
    kind=value['kind'];key=value['object_sha'];packed=value['packed_sha'];body=value['zlib_base64'];size=value['expanded_bytes']
    need(type(kind) is str and kind in ('publication','scan') and type(key) is str
         and re.fullmatch('[0-9a-f]{64}',key) and type(packed) is str and re.fullmatch('[0-9a-f]{64}',packed)
         and type(size) is int and 0<=size<=MAX_OBJECT and type(body) is str
         and len(body)<=MAX_OBJECT and re.fullmatch('[A-Za-z0-9+/]*={0,2}',body) is not None,
         'compact_fast_object_fields')
    return len('{"expanded_bytes":,"kind":"","object_sha":"","packed_sha":"","zlib_base64":""}')+len(str(size))+len(kind)+128+len(body)


def _fast_canonical(value,size):
    need(type(size) is int and 0<=size<=MAX_OBJECT,'io_encoded_byte_bound')
    raw=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')
    need(len(raw)==size,'compact_fast_exact_size_mismatch');return raw


def _canonical_under_authenticated_bound(value,upper,maximum):
    # Only callers that reconstruct from canonical, hash-verified typed nodes
    # may supply this bound. Repeated references are charged repeatedly.
    need(type(upper) is int and 0<=upper<=maximum,'io_reconstruction_encoded_work_bound')
    raw=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')
    need(len(raw)<=upper,'compact_reconstruction_upper_bound_mismatch');return raw


def _archive_encode(value):
    """C encoding only for fixed, strictly prebounded shapes; generic path unchanged."""
    if type(value) is dict and value.get('encoding')=='exact_compressed_objects32_v2':
        need(set(value)=={'encoding','objects'} and type(value['objects']) is list
             and 0<len(value['objects'])<=BLOCK,'compact_fast_block_shape')
        sizes=[_compact_encoded_size(item) for item in value['objects']]
        size=len('{"encoding":"exact_compressed_objects32_v2","objects":[]}')+sum(sizes)+len(sizes)-1
        return _fast_canonical(value,size)
    if type(value) is dict and value.get('encoding')=='compact_objects_leaf_v2':
        need(set(value)=={'encoding','rows'} and type(value['rows']) is list
             and len(value['rows'])<=CATALOG_LEAF,'compact_fast_catalog_shape')
        sizes=[]
        for row in value['rows']:
            need(type(row) is list and len(row)==7 and type(row[0]) is str and row[0] in ('publication','scan')
                 and all(type(row[i]) is str and re.fullmatch('[0-9a-f]{64}',row[i]) for i in (1,3,5))
                 and type(row[2]) is int and 0<=row[2]<=MAX_OBJECT
                 and type(row[4]) is int and 0<row[4]<=MAX_OBJECT
                 and type(row[6]) is int and 0<=row[6]<BLOCK,'compact_fast_catalog_row')
            sizes.append(len(row[0])+208+sum(len(str(row[i])) for i in (2,4,6)))
        size=len('{"encoding":"compact_objects_leaf_v2","rows":[]}')+sum(sizes)+max(0,len(sizes)-1)
        return _fast_canonical(value,size)
    return encode(value)


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
    values=dict(capacity_policy.source_binding())
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
    for name,module_path in (('revision_transport_v7.py',transport.__file__),('projection_revision_consumer_v2.py',incremental.__file__),('revision_news_io_base_v1.py',str(Path(__file__).with_name('revision_news_io_base_v1.py')))):
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
    reader_profile,reader_proof=read_json(config['reader_profile_path'],MAX_STATE)
    need(reader_proof['sha256']==config['reader_profile_sha256'],'io_reader_profile_hash_required')
    validation_reader=incremental.IncrementalReader(config['publication_path'],config['observation_path'],
        cohort_id=config['cohort_id'],consumer_id=config['consumer_id'],expected_policy=config['policy'],
        input_identity=config['input_identity'],cache_path=config['reader_cache_path'],operations_profile=reader_profile)
    return _make_session({'config':encode(config),'graph':source_graph(),'failure_serial':0,'usable':False,'archive_layout':(),
        'last_failure':None,'transport_failure_generation':None,'lock':threading.RLock(),
        'validation_reader':validation_reader,'bootstrap_progress':None,'bootstrap_validated_context':None})


def session_status(session):
    state=_session(session)
    with state['lock']:return own({k:state[k] for k in ('failure_serial','usable','last_failure','bootstrap_progress')})


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
        return self._put(value,defer_readback=False)
    def _put(self,value,*,defer_readback):
        raw=_archive_encode(value);key=hashlib.sha256(raw).hexdigest();name=key+'.json';path=self.root/name
        if name in self.files:
            if defer_readback:
                need(self.files[name]==len(raw),'immutable_capture_object_size_collision');return key
            old,_=read_exact(path,MAX_OBJECT);need(old==raw,'immutable_capture_object_collision');return key
        need(len(self.files)<MAX_FILES and self.total+len(raw)<=MAX_ARCHIVE,'capture_archive_capacity_exhausted')
        path_for(path,missing=True)
        # Single fixed session serializes this store; an existing object is
        # compared exactly. Partial writes remain rejected, never rehabilitated.
        try:
            with path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
        except FileExistsError:
            if not defer_readback:
                old,_=read_exact(path,MAX_OBJECT);need(old==raw,'immutable_capture_object_collision')
        if not defer_readback:
            old,_=read_exact(path,MAX_OBJECT);need(old==raw,'capture_object_readback_failed')
        self.files[name]=len(raw);self.total+=len(raw);return key
    def get(self,key,*,budget=None):
        need(type(key) is str and re.fullmatch('[0-9a-f]{64}',key),'capture_object_hash_required')
        raw,proof=read_exact(self.root/(key+'.json'),MAX_OBJECT)
        need(proof['sha256']==key,'capture_object_hash_mismatch')
        if budget is not None:budget.reserve(len(raw))
        value=json.loads(raw)
        need(_archive_encode(value)==raw,'canonical_capture_object_required');return value
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
    """Archive compact JSON only; exact expanded evidence stays compressed.

    Small subtrees share ordinary immutable CAS leaves. Large maps retain each
    value separately, so appending evidence changes only small reference pages.
    List pages contain32 original values where bounded, never the raw history.
    """
    visits=0
    def put(value,depth=0):
        nonlocal visits
        visits+=1;need(visits<=MAX_FILES and depth<=32,'compact_recipe_tree_bound')
        try:encode(value,256*1024)
        except ValueError as exc:
            if str(exc)!='io_encoded_byte_bound':raise
        else:return archive.put({'kind':'value','value':value})
        if type(value) is dict:
            need(all(type(key) is str for key in value),'compact_recipe_string_keys')
            rows=[[key,put(item,depth+1)] for key,item in sorted(value.items())]
            return archive.put({'kind':'mapping','rows':put(rows,depth+1)})
        need(type(value) is list,'compact_recipe_leaf_bound')
        if len(value)>BLOCK:
            return archive.put({'kind':'pages','count':len(value),
                'pages':[put(value[i:i+BLOCK],depth+1) for i in range(0,len(value),BLOCK)]})
        return archive.put({'kind':'items','items':[put(item,depth+1) for item in value]})
    need(type(bundle) is dict,'compact_export_recipe_required')
    return archive.put({'schema_version':RECIPE,'compact_root':put(bundle),
        'digest_scope':'compact_manifest_not_expanded_legacy_readback'})


def _load_bundle(archive,key,*,with_bound=False):
    budget=_DecodeBudget()
    recipe=archive.get(key,budget=budget)
    need(type(recipe) is dict and set(recipe)=={'schema_version','compact_root','digest_scope'}
         and recipe['schema_version']==RECIPE and recipe['digest_scope']=='compact_manifest_not_expanded_legacy_readback',
         'capture_compact_recipe_required')
    visits=0
    def get(key,depth=0):
        nonlocal visits
        visits+=1;need(visits<=MAX_FILES and depth<=32,'compact_recipe_tree_bound')
        node=archive.get(key,budget=budget)
        need(type(node) is dict and type(node.get('kind')) is str,'compact_recipe_node_required')
        kind=node['kind']
        if kind=='value':
            need(set(node)=={'kind','value'},'compact_recipe_value_fields')
            encode(node['value'],256*1024);return node['value']
        if kind=='mapping':
            need(set(node)=={'kind','rows'},'compact_recipe_mapping_fields')
            rows=get(node['rows'],depth+1);need(type(rows) is list,'compact_recipe_mapping_rows')
            result={};previous=None
            for row in rows:
                need(type(row) is list and len(row)==2 and type(row[0]) is str
                     and (previous is None or previous<row[0]),'compact_recipe_mapping_order')
                previous=row[0];result[row[0]]=get(row[1],depth+1)
            return result
        if kind=='items':
            need(set(node)=={'kind','items'} and type(node['items']) is list and len(node['items'])<=BLOCK,
                 'compact_recipe_item_fields')
            return [get(item,depth+1) for item in node['items']]
        need(kind=='pages' and set(node)=={'kind','count','pages'} and type(node['count']) is int
             and 0<node['count']<=MAX_FILES and type(node['pages']) is list
             and len(node['pages'])==(node['count']+BLOCK-1)//BLOCK,'compact_recipe_page_fields')
        result=[]
        for index,page in enumerate(node['pages']):
            rows=get(page,depth+1)
            need(type(rows) is list and len(rows)==min(BLOCK,node['count']-index*BLOCK),'compact_recipe_page_count')
            result.extend(rows)
        return result
    result=get(recipe['compact_root']);need(type(result) is dict,'compact_export_recipe_required')
    _canonical_under_authenticated_bound(result,budget.used,MAX_READBACK)
    return (result,budget.used) if with_bound else result


def _object_expectations(manifest):
    """Small original content identities; no compressed or expanded article body."""
    need(type(manifest) is dict and type(manifest.get('publication')) is dict,
         'compact_manifest_required')
    result=[];seen=set()
    for kind,rows in (('publication',manifest['publication'].get('evidence_objects')),
                      ('scan',manifest.get('scan_objects'))):
        need(type(rows) is list and len(rows)<=MAX_FILES,'compact_manifest_object_count')
        for row in rows:
            need(type(row) is list and len(row)==4,'compact_manifest_object_fields')
            key,size,packed_sha,packed_bytes=row
            need(type(key) is str and re.fullmatch('[0-9a-f]{64}',key) and
                 type(packed_sha) is str and re.fullmatch('[0-9a-f]{64}',packed_sha) and
                 type(size) is int and 0<=size<=MAX_OBJECT and
                 type(packed_bytes) is int and 0<packed_bytes<=MAX_OBJECT and
                 (kind,key) not in seen,'compact_manifest_object_identity')
            seen.add((kind,key));result.append((kind,key,size,packed_sha,packed_bytes))
    need(len(result)<=MAX_FILES,'compact_manifest_total_object_count')
    return result


def _verify_compact_object(value,expected):
    kind,key,size,packed_sha,packed_bytes=expected
    need(type(value) is dict and set(value)=={'kind','object_sha','expanded_bytes','packed_sha','zlib_base64'}
         and value['kind']==kind and value['object_sha']==key and value['expanded_bytes']==size
         and type(value['expanded_bytes']) is int and value['packed_sha']==packed_sha
         and type(value['zlib_base64']) is str,'compact_archive_object_binding')
    need(len(value['zlib_base64'])==4*((packed_bytes+2)//3),'compact_archive_base64_size')
    packed=base64.b64decode(value['zlib_base64'],validate=True)
    need(len(packed)==packed_bytes and hashlib.sha256(packed).hexdigest()==packed_sha
         and base64.b64encode(packed).decode()==value['zlib_base64'],'compact_archive_compressed_byte_binding')
    # The semantic consumer validates bounded decompression and original raw
    # hashes. This archive verifies the exact compressed bytes already read.
    return value


def _store_object_catalog(archive,rows,depth=0):
    """Hash-prefix leaves make small appends local without altering row order."""
    need(type(rows) is list and len(rows)<=MAX_FILES and 0<=depth<=64,'compact_catalog_bound')
    if len(rows)<=CATALOG_LEAF:
        return archive.put({'encoding':'compact_objects_leaf_v2','rows':rows})
    need(depth<64,'compact_catalog_hash_collision')
    groups={}
    for row in rows:groups.setdefault(row[1][depth],[]).append(row)
    return archive.put({'encoding':'compact_objects_branch_v2','depth':depth,'count':len(rows),
        'children':[[prefix,_store_object_catalog(archive,group,depth+1)] for prefix,group in sorted(groups.items())]})


def _load_object_catalog(archive,key,kind,*,prefix='',budget=None):
    node=archive.get(key,budget=budget)
    need(type(node) is dict,'compact_catalog_node_required')
    if node.get('encoding')=='compact_objects_leaf_v2':
        need(set(node)=={'encoding','rows'} and type(node['rows']) is list and len(node['rows'])<=CATALOG_LEAF,
             'compact_catalog_leaf_fields')
        previous=None
        for row in node['rows']:
            need(type(row) is list and len(row)==7 and row[0]==kind and type(row[1]) is str
                 and re.fullmatch('[0-9a-f]{64}',row[1]) and row[1].startswith(prefix)
                 and (previous is None or previous<row[1]) and type(row[5]) is str
                 and re.fullmatch('[0-9a-f]{64}',row[5]) and type(row[6]) is int
                 and 0<=row[6]<BLOCK,'compact_catalog_row_order')
            previous=row[1]
        return node['rows']
    need(set(node)=={'encoding','depth','count','children'} and node['encoding']=='compact_objects_branch_v2'
         and type(node['depth']) is int and node['depth']==len(prefix)<64
         and type(node['count']) is int and CATALOG_LEAF<node['count']<=MAX_FILES
         and type(node['children']) is list and 1<=len(node['children'])<=16,'compact_catalog_branch_fields')
    result=[];previous=None
    for child in node['children']:
        need(type(child) is list and len(child)==2 and type(child[0]) is str and re.fullmatch('[0-9a-f]',child[0])
             and (previous is None or previous<child[0]),'compact_catalog_child_order')
        previous=child[0]
        result.extend(_load_object_catalog(archive,child[1],kind,prefix=prefix+child[0],budget=budget))
    need(len(result)==node['count'],'compact_catalog_count_mismatch')
    return result


class _ObjectSpool:
    """Bounded temporary compressed bytes; never a durable proof or authority.

    A single seekable file avoids retaining a second whole compressed universe
    in RAM. Every durable archive object is independently authenticated later.
    TemporaryFile owns deletion on close, including partial failures.
    """
    def __init__(self,archive):
        self.directory=path_for(archive.root.parent,directory=True)
        self.stream=None;self.index={};self.total=0
    def __enter__(self):
        self.stream=tempfile.TemporaryFile(mode='w+b',dir=self.directory,prefix='compact_pack_spool_')
        return self
    def __exit__(self,*_):
        self.stream.close();self.stream=None;self.index.clear()
    def put(self,key,value):
        need(key not in self.index and len(self.index)<MAX_FILES,'compact_spool_duplicate_or_count')
        raw=_fast_canonical(value,_compact_encoded_size(value))
        need(self.total+len(raw)<=MAX_SPOOL,'compact_spool_byte_bound')
        position=self.stream.tell();self.stream.write(raw)
        self.index[key]=(position,len(raw),hashlib.sha256(raw).digest());self.total+=len(raw)
    def get(self,key):
        need(key in self.index,'compact_spool_object_missing')
        position,size,expected=self.index[key];self.stream.seek(position);raw=self.stream.read(size)
        need(len(raw)==size and hashlib.sha256(raw).digest()==expected,'compact_spool_byte_mismatch')
        return json.loads(raw)


class _CaptureArchiveWriter:
    """Provisioning only; the caller must independently verify the entire graph.

    This wrapper is used solely by _store_capture_archive. It cannot create a
    current handle, and general Archive.put retains immediate readback.
    """
    def __init__(self,archive):self.archive=archive
    def put(self,value):return self.archive._put(value,defer_readback=True)


def _stable_object_order(manifest,expected):
    """Original receipt ordering makes complete32-object blocks append-stable."""
    allowed={(row[0],row[1]) for row in expected};seen=set();result=[]
    def add(kind,key):
        need(type(key) is str and (kind,key) in allowed,'compact_pack_receipt_object_unknown')
        if (kind,key) not in seen:seen.add((kind,key));result.append((kind,key))
    proofs=manifest['publication'].get('record_proofs')
    need(type(proofs) is list and len(proofs)<=MAX_FILES,'compact_pack_publication_list')
    for proof in proofs:
        need(type(proof) is dict and type(proof.get('attempt')) is dict
             and type(proof['attempt'].get('body')) is str,'compact_pack_original_attempt_required')
        body=json.loads(proof['attempt']['body']);pack=body.get('snapshot_pack')
        need(type(pack) is dict and type(pack.get('evidence_sha256')) is list,'compact_pack_original_refs_required')
        for key in pack['evidence_sha256']:add('publication',key)
    observations=manifest.get('consumer',{}).get('observations')
    need(type(observations) is list and len(observations)<=MAX_FILES,'compact_pack_observation_list')
    for item in observations:
        need(type(item) is dict and type(item.get('observation')) is dict,'compact_pack_original_observation_required')
        scan=item['observation'].get('completed_scan')
        need(type(scan) is dict and type(scan.get('evidence_refs')) is list,'compact_pack_original_scan_required')
        for ref in scan['evidence_refs']:
            need(type(ref) is dict and len(ref)==1,'compact_pack_scan_reference')
            if 'stored_evidence_sha256' in ref:add('scan',ref['stored_evidence_sha256'])
            else:need(set(ref)=={'publication_evidence_sha256'},'compact_pack_scan_reference')
    need(seen==allowed,'compact_pack_original_reference_set_incomplete')
    return result


def _expected_object_size(item):
    # _object_expectations has already authenticated the closed ASCII types.
    kind,key,size,packed_sha,packed_bytes=item
    return len('{"expanded_bytes":,"kind":"","object_sha":"","packed_sha":"","zlib_base64":""}')+len(str(size))+len(kind)+128+4*((packed_bytes+2)//3)


def _complete_layout_groups(layout):
    """Non-authorizing, bounded immutable metadata from an earlier full read.

    The final independent archive read still authenticates every reused byte.
    Only exact complete32-object groups can avoid repeat spool/provision work.
    """
    need(type(layout) is tuple and len(layout)<=MAX_FILES,'compact_layout_tuple_bound')
    references={'publication':[],'scan':[]};blocks={}
    for row in layout:
        need(type(row) is tuple and len(row)==7 and type(row[0]) is str and row[0] in references
             and type(row[5]) is str and re.fullmatch('[0-9a-f]{64}',row[5])
             and type(row[6]) is int and 0<=row[6]<BLOCK,'compact_layout_row_fields')
        references[row[0]].append(list(row[1:5]));slots=blocks.setdefault(row[5],{})
        need(row[6] not in slots,'compact_layout_duplicate_slot');slots[row[6]]=row
    _object_expectations({'publication':{'evidence_objects':references['publication']},'scan_objects':references['scan']})
    reusable={}
    for key,slots in blocks.items():
        need(set(slots)==set(range(len(slots))) and len(slots)<=BLOCK,'compact_layout_exact_slots')
        if len(slots)==BLOCK:
            group=tuple(tuple(slots[i][:5]) for i in range(BLOCK))
            need(group not in reusable,'compact_layout_duplicate_group');reusable[group]=key
    return reusable


def _store_capture_archive(archive,manifest,objects,*,prior_layout=()):
    expected=_object_expectations(manifest);iterator=iter(objects);references={'publication':[],'scan':[]}
    writer=_CaptureArchiveWriter(archive)
    by_key={(item[0],item[1]):item for item in expected};order=_stable_object_order(manifest,expected)
    for kind in references:
        keys=[item[1] for item in expected if item[0]==kind]
        need(all(keys[i-1]<keys[i] for i in range(1,len(keys))),'compact_manifest_object_order')
    groups=[];group=[];group_bytes=0
    for identity_key in order:
        item=by_key[identity_key];size=_expected_object_size(item)
        need(size<=MAX_OBJECT,'io_encoded_byte_bound')
        if group and (item[0]!=group[-1][0] or len(group)>=BLOCK or group_bytes+size>PACK_BYTES):
            groups.append(tuple(group));group=[];group_bytes=0
        group.append(item);group_bytes+=size
    if group:groups.append(tuple(group))
    reusable=_complete_layout_groups(prior_layout)
    changed={(item[0],item[1]) for group in groups if group not in reusable for item in group}
    with _ObjectSpool(archive) as spool:
        for item in expected:
            try:value=next(iterator)
            except StopIteration:raise ValueError('compact_archive_missing_object') from None
            # A layout hint never substitutes for the fresh object's proof.
            _verify_compact_object(value,item)
            if (item[0],item[1]) in changed:spool.put((item[0],item[1]),value)
        try:next(iterator)
        except StopIteration:pass
        else:raise ValueError('compact_archive_extra_object')
        for group in groups:
            key=reusable.get(group)
            if key is None:
                values=[spool.get((item[0],item[1])) for item in group]
                key=writer.put({'encoding':'exact_compressed_objects32_v2','objects':values})
            for slot,item in enumerate(group):references[item[0]].append([*item,key,slot])
    for rows in references.values():rows.sort(key=lambda row:row[1])
    core={key:value for key,value in manifest.items() if key not in ('publication','scan_objects')}
    core['publication']={key:value for key,value in manifest['publication'].items() if key!='evidence_objects'}
    raw_manifest=encode(manifest,MAX_READBACK)
    return writer.put({'schema_version':STREAMED_RECIPE,
        'manifest_core_root':_store_bundle(writer,core),
        'manifest_sha256':hashlib.sha256(raw_manifest).hexdigest(),'manifest_bytes':len(raw_manifest),
        'object_catalogs':{kind:_store_object_catalog(writer,rows) for kind,rows in references.items()},
        'object_count':len(expected)})


def _load_capture_archive(archive,key,*,with_layout=False):
    recipe=archive.get(key)
    need(type(recipe) is dict and set(recipe)=={'schema_version','manifest_core_root','manifest_sha256',
         'manifest_bytes','object_catalogs','object_count'} and
         recipe['schema_version']==STREAMED_RECIPE,
         'streamed_capture_recipe_required')
    core,core_bound=_load_bundle(archive,recipe['manifest_core_root'],with_bound=True)
    need(type(core) is dict and 'scan_objects' not in core and type(core.get('publication')) is dict
         and 'evidence_objects' not in core['publication'] and type(recipe['object_catalogs']) is dict
         and set(recipe['object_catalogs'])=={'publication','scan'},'compact_manifest_core_required')
    budget=_DecodeBudget()
    references={kind:_load_object_catalog(archive,recipe['object_catalogs'][kind],kind,budget=budget)
                for kind in ('publication','scan')}
    manifest={**core,'publication':{**core['publication'],'evidence_objects':[row[1:5] for row in references['publication']]},
              'scan_objects':[row[1:5] for row in references['scan']]}
    expected=_object_expectations(manifest)
    raw_manifest=_canonical_under_authenticated_bound(manifest,core_bound+budget.used+256,MAX_READBACK)
    need(type(recipe['manifest_bytes']) is int and recipe['manifest_bytes']==len(raw_manifest)
         and recipe['manifest_sha256']==hashlib.sha256(raw_manifest).hexdigest(),'compact_manifest_readback_binding')
    need(type(recipe['object_count']) is int and recipe['object_count']==len(expected),'compact_archive_reference_count')
    def objects():
        # Verify each immutable block once, then return the exact original
        # SHA-sorted object order through one bounded temporary file. A small
        # RAM block cache alone would reread randomly ordered blocks per object.
        blocks={}
        for kind in ('publication','scan'):
            for row in references[kind]:
                slots=blocks.setdefault(row[5],{})
                need(row[6] not in slots,'compact_pack_duplicate_slot');slots[row[6]]=row
        with _ObjectSpool(archive) as spool:
            for key,slots in blocks.items():
                block=archive.get(key)
                need(type(block) is dict and set(block)=={'encoding','objects'}
                     and block['encoding']=='exact_compressed_objects32_v2'
                     and type(block['objects']) is list and 0<len(block['objects'])<=BLOCK,
                     'compact_pack_block_fields')
                need(set(slots)==set(range(len(block['objects']))),'compact_pack_exact_slot_set')
                for slot,value in enumerate(block['objects']):
                    row=slots[slot];_verify_compact_object(value,tuple(row[:5]))
                    spool.put((row[0],row[1]),value)
            for item in expected:
                # The object was verified before spooling; get authenticates
                # exactly those original canonical bytes for this typed key.
                yield spool.get((item[0],item[1]))
    result=(manifest,objects())
    return (*result,tuple(tuple(row) for kind in ('publication','scan') for row in references[kind])) if with_layout else result


def _store_health(archive,value):
    """Retain exact read clocks while sharing unchanged collector file proofs."""
    need(type(value) is dict and type(value.get('files')) is dict,'compact_health_files_required')
    return archive.put({'schema_version':'revision_news_compact_health_recipe_v1_20260914',
        'core':{key:item for key,item in value.items() if key not in ('files','transport')},
        'files':{key:archive.put(item) for key,item in value['files'].items()},
        'transport':archive.put(value['transport']) if 'transport' in value else None,
        'original_health_sha256':digest(value)})


def _load_health(archive,key):
    recipe=archive.get(key)
    need(type(recipe) is dict and set(recipe)=={'schema_version','core','files','transport','original_health_sha256'}
         and recipe['schema_version']=='revision_news_compact_health_recipe_v1_20260914'
         and type(recipe['core']) is dict and not set(recipe['core'])&{'files','transport'}
         and type(recipe['files']) is dict and len(recipe['files'])<=16,'compact_health_recipe_required')
    value={**recipe['core'],'files':{name:archive.get(ref) for name,ref in recipe['files'].items()}}
    if recipe['transport'] is not None:value['transport']=archive.get(recipe['transport'])
    need(digest(value)==recipe['original_health_sha256'],'compact_health_reconstruction_mismatch')
    return value


def _store_issue_health(archive,value):
    return archive.put({'schema_version':'revision_news_compact_issue_health_v1_20260914',
        'core':{key:item for key,item in value.items() if key!='health'},
        'health':_store_health(archive,value['health']),'original_proof_sha256':digest(value)})


def _load_issue_health(archive,key):
    recipe=archive.get(key)
    need(type(recipe) is dict and set(recipe)=={'schema_version','core','health','original_proof_sha256'}
         and recipe['schema_version']=='revision_news_compact_issue_health_v1_20260914'
         and type(recipe['core']) is dict and 'health' not in recipe['core'],'compact_issue_health_recipe_required')
    value={**recipe['core'],'health':_load_health(archive,recipe['health'])}
    need(digest(value)==recipe['original_proof_sha256'],'compact_issue_health_reconstruction_mismatch')
    return value


def _validate_capture_value(value,config,archive):
    fields={'schema_version','config_sha256','source_graph','bundle_recipe_sha256','readback_sha256','readback_bytes',
        'context_metadata','store_identities','health_before_sha256','health_after_sha256','read_started_epoch',
        'read_completed_epoch','first_observed_epoch','actual_database_capture_performed','historical_collector_health_proven',
        'semantic_complete_validations','capture_scope','transport_failure_generation','readback_digest_scope',*INERT}
    need(type(value) is dict and set(value)==fields and value['schema_version']==SCHEMA and value['config_sha256']==digest(config),
         'io_exact_capture_value_required')
    need(all(value[k] is v for k,v in INERT.items()) and value['actual_database_capture_performed'] is True and
         value['historical_collector_health_proven'] is False and type(value['semantic_complete_validations']) is int and value['semantic_complete_validations']==1,
         'io_capture_claim_types_required')
    need(type(value['readback_bytes']) is int and 0<=value['readback_bytes']<=MAX_READBACK,'io_capture_readback_size_type')
    need(value['capture_scope']=='fixed_actual_compact_store_read_then_immutable_recipe_readback'
         and value['readback_digest_scope']=='compact_manifest_not_expanded_legacy_readback','io_capture_scope_required')
    begun,read,observed=(adapter.epoch(value[k]) for k in ('read_started_epoch','read_completed_epoch','first_observed_epoch'))
    need(begun<=read<=observed and observed-begun<=MAX_SECONDS,'io_capture_clock_binding')
    need(type(value['store_identities']) is dict and set(value['store_identities'])=={'publication_path','observation_path'} and
         all(type(v) is list and len(v)==2 and all(type(x) is int for x in v) for v in value['store_identities'].values()),'io_store_identity_type')
    for key in ('bundle_recipe_sha256','readback_sha256','health_before_sha256','health_after_sha256'):
        need(type(value[key]) is str and re.fullmatch('[0-9a-f]{64}',value[key]),'io_capture_hash_required')
    before=_load_health(archive,value['health_before_sha256']);after=_load_health(archive,value['health_after_sha256'])
    _validate_health_files(before,config,observed);_validate_health_files(after,config,observed)
    need(type(value['transport_failure_generation']) is int and value['transport_failure_generation']==before['transport']['failure_generation']==after['transport']['failure_generation'],'io_capture_transport_generation_binding')
    need(begun<=before['read_started_epoch']<=before['observed_epoch']<=read<=after['read_started_epoch']<=after['observed_epoch']<=observed,
         'io_capture_health_order')


def _failure(state,config,operation,exc):
    state['failure_serial']+=1;state['usable']=False;state['archive_layout']=()
    value={'schema_version':SCHEMA,'status':'failed','operation':operation,'failure_serial':state['failure_serial'],
        'error':type(exc).__name__+':'+str(exc)[:500],'health_evidence':getattr(exc,'evidence',None),**INERT}
    try:value['retained_failure_sha256']=Archive(config['archive_root']).put(value)
    except Exception as storage:value['failure_retention_error']=type(storage).__name__+':'+str(storage)[:300]
    state['last_failure']=own(value)


def _original_receipts_by_read(metadata,read_completed_epoch):
    """Original committed observation/ack clocks cannot follow our actual read.

    The fixed consumer has already validated these retained receipt fields.
    This is a cross-clock bound, not another complete semantic validation or
    a restriction on an article's future release/embargo timestamps.
    """
    cutoff=adapter.epoch(read_completed_epoch)
    acknowledged=metadata['consumer_receipt_max_epoch']
    need((acknowledged is None and metadata['observation_count']==0) or
         (type(acknowledged) in (int,float) and 0<acknowledged<=cutoff),
         'io_original_consumer_receipt_after_actual_read')


def bootstrap_inputs(session,*,clock=time.time):
    """Bounded validation and archive phases; a fresh capture is still required.

    No Capture handle or prepared context escapes this function. Original
    admissions and consumer receipts retain their times. The same reader keeps
    semantic progress across retries. Each phase has a separate startup work
    bound; the ordinary current-capture 30-second guard is unchanged.
    """
    state=_session(session);config=json.loads(state['config'])
    with state['lock']:
        state['usable']=False;state['failure_serial']+=1
        try:
            begun=_tick(clock);wall=time.monotonic();graph=source_graph()
            need(graph==state['graph'],'io_bootstrap_source_graph_changed')
            identities={key:identity(config[key]) for key in ('publication_path','observation_path')}
            resumed=state['bootstrap_validated_context'] is not None
            # Even on an archive retry, reread the complete original inventory
            # and any appended suffix before replacing this private checkpoint.
            # This context never substitutes for capture_shared's actual read.
            while True:
                need(time.monotonic()-wall<=MAX_BOOTSTRAP_STAGE_SECONDS,'io_bootstrap_validation_duration_bound')
                progress=state['validation_reader'].bootstrap_step()
                state['bootstrap_progress']=progress
                if progress['status']=='prefix_verified_requires_fresh_read':
                    try:consumed=state['validation_reader'].read_observations()
                    except incremental.BootstrapPending:continue
                    break
            prepared=adapter.prepare_revision_context(consumed);metadata=adapter.revision_context_metadata(prepared)
            validated=_tick(clock,begun);validation_seconds=time.monotonic()-wall
            _original_receipts_by_read(metadata,validated)
            need(validation_seconds<=MAX_BOOTSTRAP_STAGE_SECONDS and validated-begun<=MAX_BOOTSTRAP_STAGE_SECONDS,
                 'io_bootstrap_validation_duration_bound')
            need(graph==source_graph() and identities=={key:identity(config[key]) for key in identities},
                 'io_bootstrap_source_or_store_changed')
            state['bootstrap_validated_context']=consumed
            archive_started=time.monotonic();archive_begun=validated
            # Initial durable archive materialization is startup work. It
            # grants neither collector health nor a usable current handle.
            bundle=consumer.export_manifest(consumed);archive=Archive(config['archive_root'])
            recipe=_store_capture_archive(archive,bundle,consumer.iter_recipe_objects(consumed))
            rebuilt,objects,layout=_load_capture_archive(archive,recipe,with_layout=True)
            for _ in objects:pass
            need(rebuilt==bundle,'io_bootstrap_archive_manifest_mismatch')
            completed=_tick(clock,begun);_original_receipts_by_read(metadata,completed)
            need(graph==source_graph() and identities=={key:identity(config[key]) for key in identities},
                 'io_bootstrap_source_or_store_changed')
            archive_seconds=time.monotonic()-archive_started
            need(archive_seconds<=MAX_BOOTSTRAP_STAGE_SECONDS and completed-archive_begun<=MAX_BOOTSTRAP_STAGE_SECONDS,
                 'io_bootstrap_archive_duration_bound')
            elapsed=time.monotonic()-wall
            need(elapsed<=MAX_BOOTSTRAP_SECONDS and completed-begun<=MAX_BOOTSTRAP_SECONDS,
                 'io_bootstrap_duration_bound')
            state['archive_layout']=layout
            return {'schema_version':'revision_news_phase_bounded_bootstrap_v2_20260916',
                'status':'cache_prepared_fresh_capture_required','elapsed_sec':elapsed,
                'validation_phase_seconds':validation_seconds,'archive_phase_seconds':archive_seconds,
                'maximum_phase_seconds':MAX_BOOTSTRAP_STAGE_SECONDS,
                'maximum_total_seconds':MAX_BOOTSTRAP_SECONDS,'retained_prefix_before_retry':resumed,
                'read_started_epoch':begun,'read_completed_epoch':completed,
                'manifest_sha256':metadata['manifest_sha256'],'manifest_bytes':metadata['manifest_bytes'],
                'publication_count':metadata['publication_count'],'observation_count':metadata['observation_count'],
                'archive_prepared_and_byte_verified':True,'archive_recipe_sha256':recipe,
                'archive_bytes':archive.total,'archive_files':len(archive.files),
                'fresh_health_proven':False,'capture_handle_returned':False,'original_availability_unchanged':True,
                'scope':'actual_fixed_store_cache_preparation_not_current_capture_or_forecast_authority',**INERT}
        except Exception as exc:
            # Private prefix state can survive archive backpressure; a source
            # or semantic validation failure cannot retain this checkpoint.
            if str(exc)!='io_bootstrap_archive_duration_bound':state['bootstrap_validated_context']=None
            _failure(state,config,'bootstrap_inputs',exc);raise


def capture_shared(session,*,clock=time.time):
    state=_session(session);config=json.loads(state['config'])
    with state['lock']:
        try:
            begun=_tick(clock);wall=time.monotonic();graph=source_graph()
            need(graph==state['graph'],'io_source_graph_changed')
            before=_health(config,clock,state)
            identities={key:identity(config[key]) for key in ('publication_path','observation_path')}
            # Fixed actual I/O call. No external bundle or validator argument.
            consumed=state['validation_reader'].read_observations()
            read_completed=_tick(clock,before['observed_epoch'])
            prepared=adapter.prepare_revision_context(consumed);meta=adapter.revision_context_metadata(prepared)
            _original_receipts_by_read(meta,read_completed)
            bundle=consumer.export_manifest(consumed)
            original_raw=_canonical_under_authenticated_bound(bundle,meta['manifest_bytes'],MAX_READBACK)
            need(len(original_raw)==meta['manifest_bytes'] and meta['context_sha256']==hashlib.sha256(original_raw).hexdigest(),
                 'io_exact_readback_hash_required')
            after=_health(config,clock,state)
            need(after['read_started_epoch']>=read_completed,'io_post_health_clock_order')
            need(identities=={key:identity(config[key]) for key in identities},'io_store_replaced_during_capture')
            need(source_graph()==graph,'io_source_changed_during_capture')
            archive=Archive(config['archive_root']);recipe=_store_capture_archive(archive,bundle,consumer.iter_recipe_objects(consumed),prior_layout=state.get('archive_layout',()))
            # Compact recipe reconstruction is a byte check. The one complete
            # semantic validation happened at the fixed actual consumer read.
            rebuilt,objects,layout=_load_capture_archive(archive,recipe,with_layout=True)
            for _ in objects:pass  # Independent exact compressed-object readback, one object at a time.
            rebuilt_raw=_canonical_under_authenticated_bound(rebuilt,meta['manifest_bytes'],MAX_READBACK)
            need(len(rebuilt_raw)==meta['manifest_bytes'] and hashlib.sha256(rebuilt_raw).hexdigest()==meta['context_sha256'],
                 'io_durable_bundle_readback_mismatch')
            completed=_tick(clock,after['observed_epoch'])
            repair.validate_clock_state(before['files']['clock']['value'],completed)
            repair.validate_clock_state(after['files']['clock']['value'],completed)
            repair.validate_collector_observation(after['files']['heartbeat']['value'],completed)
            need(completed-begun<=MAX_SECONDS and time.monotonic()-wall<=MAX_SECONDS,'io_capture_duration_bound')
            value={'schema_version':SCHEMA,'config_sha256':digest(config),'source_graph':graph,'bundle_recipe_sha256':recipe,
                'readback_sha256':meta['context_sha256'],'readback_bytes':meta['readback_bytes'],'context_metadata':meta,
                'store_identities':identities,'health_before_sha256':_store_health(archive,before),'health_after_sha256':_store_health(archive,after),
                'read_started_epoch':begun,'read_completed_epoch':read_completed,'first_observed_epoch':completed,
                'actual_database_capture_performed':True,'historical_collector_health_proven':False,
                'semantic_complete_validations':1,'capture_scope':'fixed_actual_compact_store_read_then_immutable_recipe_readback',
                'readback_digest_scope':'compact_manifest_not_expanded_legacy_readback',
                'transport_failure_generation':state['transport_failure_generation'],**INERT}
            _validate_capture_value(value,config,archive)
            key=archive.put(value);need(archive.get(key)==value,'io_capture_descriptor_readback_failed')
            need(source_graph()==graph,'io_source_changed_before_return')
            returned=_tick(clock,completed)
            _validate_health_files(before,config,returned);_validate_health_files(after,config,returned)
            need(returned-begun<=MAX_SECONDS and time.monotonic()-wall<=MAX_SECONDS,'io_capture_final_duration_bound')
            descriptor={'schema_version':DESCRIPTOR,'capture_sha256':key,'capture_path':str(archive.root/(key+'.json'))}
            state['archive_layout']=layout;state['usable']=True
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
    bundle,objects=_load_capture_archive(archive,value['bundle_recipe_sha256'])
    need(digest(bundle,MAX_READBACK)==value['readback_sha256'] and len(encode(bundle,MAX_READBACK))==value['readback_bytes'],'io_replay_readback_binding')
    prepared=adapter.prepare_revision_context(incremental.restore_manifest(bundle,objects))
    _original_receipts_by_read(adapter.revision_context_metadata(prepared),value['read_completed_epoch'])
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
            archive=Archive(config['archive_root']);key=_store_issue_health(archive,proof);need(_load_issue_health(archive,key)==proof,'io_issue_health_readback_failed')
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
    expected={k:v for k,v in config.items() if k not in ('transport_config_path','transport_config_sha256','reader_profile_path','reader_profile_sha256','reader_cache_path')}
    expected['schema_version']=owner.CONFIG
    need(encode(expected)==encode(base),'io_transport_base_configuration_mismatch')
    need(all(config[key]==transport_config[key] for key in ('reader_profile_path','reader_profile_sha256')),
         'io_transport_shared_reader_profile_required')
    need(str(Path(config['reader_cache_path'])).lower()!=str(Path(transport_config['reader_cache_path'])).lower(),
         'separate_process_validation_caches_required')
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


def close_session(session):
    state=_session(session)
    with state['lock']:
        state['usable']=False
        state['validation_reader'].close()
