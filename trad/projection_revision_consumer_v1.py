"""Compact isolated consumer receipts and one-time complete-history validation.

New stores only. The private pure context validates supplied evidence; it does
not authenticate that an arbitrary in-process caller performed a database read.
"""
import copy
import hashlib
import json
from pathlib import Path
import os
import sqlite3
import time
import uuid
import bisect
import weakref
import base64
from collections.abc import Mapping
from collections import OrderedDict
import compact_projection_store_v1 as compact
from contextlib import contextmanager
import projection_revision_admission_v1 as publisher
reader=publisher.reader
SCHEMA='isolated_projection_consumer_cas_v4_20260914'
SCAN_SCHEMA='completed_projection_input_scan_cas_v4_20260914'
CONTEXT_SCHEMA='prepared_projection_consumer_cas_v4_20260914'
PREFIX_SCHEMA='ordered_admission_prefix_cas_v4_20260914'
MAX_SCAN_AGE_SEC=300
MAX_DOCUMENT=18*1024*1024
MAX_JSON=64*1024*1024
MAX_DATABASE=96*1024*1024
MAX_OBSERVATIONS=4096
MAX_SCAN_OBJECTS=4096
MAX_READBACK_BYTES=96*1024*1024
MAX_EXPANDED_SCAN_BYTES=512*1024*1024
MAX_CONTEXT_MEMBER_BYTES=160*1024*1024
TABLES={'consumer_profile':'singleton INTEGER PRIMARY KEY CHECK(singleton=1), body TEXT NOT NULL',
    'observations':'observation_sequence INTEGER PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE, observation_sha TEXT NOT NULL, body TEXT NOT NULL',
    'acknowledgments':'observation_id TEXT PRIMARY KEY, ack_sha TEXT NOT NULL, body TEXT NOT NULL',
    'scan_objects':'object_sha TEXT PRIMARY KEY, body TEXT NOT NULL'}
SQL='\n'.join('CREATE TABLE '+name+'('+columns+');\n'+
    '\n'.join('CREATE TRIGGER '+name+'_no_'+action.lower()+' BEFORE '+action+' ON '+name+
        " BEGIN SELECT RAISE(ABORT,'immutable_consumer_observation'); END;" for action in ('UPDATE','DELETE'))
    for name,columns in TABLES.items())
need=publisher.need


class IndexedEvidence(Mapping):
    def __init__(self, sequences, objects): self.sequences = dict(sequences); self.objects = objects
    def __getitem__(self, key): return self.sequences[key], self.objects[key]
    def __iter__(self): return iter(self.sequences)
    def __len__(self): return len(self.sequences)


def encode_frame(value):
    size, key, packed = value
    return raw({'schema_version':compact.SCHEMA, 'expanded_bytes':size, 'original_sha256':key,
                'zlib_base64':base64.b64encode(packed).decode('ascii')})


def decode_frame(body):
    value = publisher.checked_json(body)
    need(set(value) == {'schema_version','expanded_bytes','original_sha256','zlib_base64'}
         and value['schema_version'] == compact.SCHEMA, 'typed_scan_cas_required')
    packed = base64.b64decode(value['zlib_base64'], validate=True)
    result = (value['expanded_bytes'], value['original_sha256'], packed)
    compact.expand(result)
    return result


RECIPE_SCHEMA = 'compact_projection_consumer_recipe_v2_20260914'
_recipe_capture = weakref.WeakKeyDictionary()
_context_headers = weakref.WeakKeyDictionary()
_context_event_ids = weakref.WeakKeyDictionary()
_CAPTURE_METRICS = OrderedDict()
MANIFEST_SCHEMA = 'compact_projection_consumer_manifest_v2_20260914'


def _recipe_from_owned(owned):
    return {'schema_version':RECIPE_SCHEMA, 'publication':publisher.export_publication(owned['publication_integrity']),
        'consumer':{k:v for k,v in owned.items() if k not in ('publication_integrity','scan_objects')},
        'scan_objects':[[key, encode_frame(value).decode()] for key, value in sorted(compact._evidence(owned['scan_objects']).items())]}


def export_recipe(context):
    _prepared(context)
    publication, consumer_body, objects = _recipe_capture[context]
    return _recipe_from_owned({**json.loads(consumer_body), 'publication_integrity':publication, 'scan_objects':objects})


def _manifest_from_owned(owned):
    publication = owned['publication_integrity']
    frames = compact.evidence_frames(publication)
    return {'schema_version':MANIFEST_SCHEMA,
        'publication':{'schema_version':compact.SCHEMA,'profile':publication['profile'],'record_proofs':publication['record_proofs'],
            'evidence_objects':[[key,f[0],hashlib.sha256(f[2]).hexdigest(),len(f[2])] for key,f in sorted(frames.items())]},
        'consumer':{k:v for k,v in owned.items() if k not in ('publication_integrity','scan_objects')},
        'scan_objects':[[key,f[0],hashlib.sha256(f[2]).hexdigest(),len(f[2])] for key,f in sorted(compact._evidence(owned['scan_objects']).items())]}


def export_manifest(context):
    _prepared(context)
    publication, body, objects = _recipe_capture[context]
    return _manifest_from_owned({**json.loads(body),'publication_integrity':publication,'scan_objects':objects})


def iter_recipe_objects(context):
    _prepared(context); publication, _, objects = _recipe_capture[context]
    for kind, frames in (('publication', compact.evidence_frames(publication)),('scan', compact._evidence(objects))):
        for key,value in sorted(frames.items()):
            yield {'kind':kind,'object_sha':key,'expanded_bytes':value[0],
                   'packed_sha':hashlib.sha256(value[2]).hexdigest(),'zlib_base64':base64.b64encode(value[2]).decode('ascii')}


def restore_manifest(manifest, objects):
    owned, _, _ = bounded_owned(manifest)
    need(type(owned) is dict and set(owned) == {'schema_version','publication','consumer','scan_objects'}
         and owned['schema_version'] == MANIFEST_SCHEMA, 'typed_consumer_manifest_required')
    stream = iter(objects)
    publication = publisher.restore_publication_manifest(owned['publication'], stream)
    consumer = owned['consumer']
    need(type(consumer) is dict and not set(consumer)&{'publication_integrity','scan_objects'}, 'exact_consumer_manifest_required')
    need(type(owned['scan_objects']) is list and len(owned['scan_objects'])<=MAX_SCAN_OBJECTS,'scan_recipe_count_bound')
    frames = {}; previous = None
    for key,size,packed_sha,packed_size in owned['scan_objects']:
        need(type(key) is str and reader.re.fullmatch('[0-9a-f]{64}',key) and (previous is None or key>previous),'ordered_unique_scan_manifest_required')
        previous = key; item = next(stream, None)
        need(type(item) is dict and set(item)=={'kind','object_sha','expanded_bytes','packed_sha','zlib_base64'}
             and item['kind']=='scan' and item['object_sha']==key and item['expanded_bytes']==size
             and item['packed_sha']==packed_sha,'exact_ordered_scan_object_required')
        encoded=item['zlib_base64'];need(type(encoded) is str and len(encoded)<=(MAX_DOCUMENT*4//3+4),'scan_frame_bound')
        packed=base64.b64decode(encoded,validate=True)
        need(type(packed_size) is int and len(packed)==packed_size and hashlib.sha256(packed).hexdigest()==packed_sha,'scan_frame_digest_invalid')
        value=(size,key,packed);compact.expand(value);frames[key]=value
    need(next(stream,None) is None,'unexpected_extra_recipe_object')
    return _prepare_owned({**consumer,'publication_integrity':publication,'scan_objects':compact.owned_evidence(frames)})


def prepare_consumed_context(value):
    """Pass through an owned capture or revalidate a bounded archived recipe."""
    if type(value) is PreparedConsumer:
        _prepared(value); return value
    owned, _, _ = bounded_owned(value)
    need(type(owned) is dict and set(owned) == {'schema_version','publication','consumer','scan_objects'}
         and owned['schema_version'] == RECIPE_SCHEMA, 'typed_compact_consumer_recipe_required')
    publication = publisher.restore_publication(owned['publication'])
    consumer = owned['consumer']
    need(type(consumer) is dict and not set(consumer) & {'publication_integrity','scan_objects'}, 'exact_consumer_recipe_required')
    need(type(owned['scan_objects']) is list and len(owned['scan_objects']) <= MAX_SCAN_OBJECTS, 'scan_recipe_count_bound')
    frames = {}
    for key, body in owned['scan_objects']:
        value = decode_frame(body)
        need(key == value[1] and key not in frames, 'scan_recipe_duplicate_or_digest_invalid')
        frames[key] = value
    return _prepare_owned({**consumer, 'publication_integrity':publication, 'scan_objects':compact.owned_evidence(frames)})


def read_and_prepare(*args, **kwargs): return read_observations(*args, **kwargs)


def selected_timing(context, decision_epoch):
    """Public exact original timing, without private context layout coupling."""
    _,_,_,epochs,rows,_,_ = _prepared(context)
    decision = reader._number(decision_epoch); ordinal = bisect.bisect_right(epochs, decision) - 1
    if ordinal < 0:
        return {'index_row_ordinal':None,'consumer_observed_epoch':None,'observed_publication_prefix':0,
                'coverage':None,'coverage_usable':False}
    observed, prefix, body = rows[ordinal]; coverage = json.loads(body)
    usable = coverage['caught_up'] and 0 <= decision - coverage['read_started_epoch'] <= MAX_SCAN_AGE_SEC
    return {'index_row_ordinal':ordinal,'consumer_observed_epoch':observed,'observed_publication_prefix':prefix,
            'coverage':coverage,'coverage_usable':usable}


def resolve_evidence(context, evidence_sha256):
    """Explicit audit-only original evidence retrieval from the owned CAS."""
    _prepared(context); publication, _, objects = _recipe_capture[context]
    published = compact.evidence_store(publication)
    return published[evidence_sha256] if evidence_sha256 in published else objects[evidence_sha256]


def _materialize_member(context, body):
    descriptor=compact.value_expand(body)
    evidence=resolve_evidence(context,descriptor['evidence_sha256'])
    payload_json=evidence['projection']['payload_json'];payload=json.loads(payload_json)
    need(hashlib.sha256(payload_json.encode()).hexdigest()==descriptor['payload_original_json_sha256']
         and reader.sha(payload)==descriptor['payload_canonical_sha256'],'retained_member_payload_digest_invalid')
    member={k:v for k,v in descriptor.items() if k not in ('payload_canonical_sha256','member_sha256')}
    member['payload']=payload
    need(reader.sha(member)==descriptor['member_sha256'],'retained_member_digest_invalid')
    return member


def iter_selected_headers(context, decision_epoch):
    _,_,_,epochs,rows,first,_ = _prepared(context)
    decision=reader._number(decision_epoch);timing=selected_timing(context,decision)
    if not timing['coverage_usable']: return
    prefix=timing['observed_publication_prefix']
    for event,sequences,bodies in _context_headers[context]:
        at=bisect.bisect_right(sequences,prefix)-1
        if at<0:continue
        header=json.loads(bodies[at]);observed=first[sequences[at]]
        need(observed is not None and observed<=decision,'prepared_header_observation_required')
        header['consumer_observed_epoch']=observed
        header['effective_transport_available_epoch']=max(observed,header['admitted_available_epoch'])
        yield header


def resolve_selected_member(context, header, decision_epoch):
    _,_,_,_,_,first,versions = _prepared(context)
    need(type(header) is dict,'typed_selected_header_required')
    timing=selected_timing(context,decision_epoch);need(timing['coverage_usable'],'current_complete_coverage_required')
    ident=header.get('canonical_event_id');events=_context_event_ids[context]
    event_index=bisect.bisect_left(events,ident) if type(ident) is str else -1
    need(0<=event_index<len(events) and events[event_index]==ident,'selected_header_identity_unknown')
    _,sequences,bodies=versions[event_index];at=bisect.bisect_right(sequences,timing['observed_publication_prefix'])-1
    need(at>=0,'selected_header_not_yet_observed')
    expected=json.loads(_context_headers[context][event_index][2][at]);observed=first[sequences[at]]
    expected['consumer_observed_epoch']=observed
    expected['effective_transport_available_epoch']=max(observed,expected['admitted_available_epoch'])
    need(reader.encoded(expected)==reader.encoded(header),'exact_latest_selected_header_required')
    member=_materialize_member(context,bodies[at]);need(reader.sha(member)==header['member_sha256'],'selected_member_digest_invalid')
    member['consumer_observed_epoch']=observed;member['effective_transport_available_epoch']=expected['effective_transport_available_epoch']
    return member


def bounded_owned(value):
    """One bounded canonical copy; no caller-owned containers survive prepare."""
    total=bytearray()
    encoder=json.JSONEncoder(sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)
    for chunk in encoder.iterencode(value):
        part=chunk.encode();need(len(total)+len(part)<=MAX_READBACK_BYTES,'consumer_readback_byte_bound')
        total.extend(part)
    encoded=bytes(total)
    return json.loads(encoded),hashlib.sha256(encoded).hexdigest(),len(encoded)


def _publication_index(publication):
    """Verify every ordered envelope once before building temporary indexes."""
    batches=publication['batches'];heads=publication['publication_heads']
    need(type(publication) is compact.Published and len(batches)<=reader.MAX_BATCHES and
         type(heads) is list and len(heads)==len(batches),'complete_publication_heads_required')
    compact.manifest(publication)  # Opaque capture already completely validated.
    profile_sha=publisher.digest(publication['profile'])
    prefix=[sha([PREFIX_SCHEMA,profile_sha])];checkpoints={};evidence=publication['evidence_sequences']
    envelope_hashes=publication['envelope_sha256']
    need(len(envelope_hashes)==len(heads),'complete_verified_envelope_hashes_required')
    previous={'sequence':0,'publication_sha':None,'checkpoint':None}
    for seq,head in enumerate(heads,1):
        batch=json.loads(compact.batch_descriptor(publication,seq-1))
        need(type(head) is dict and set(head)=={'sequence','publication_sha','checkpoint'} and
             type(head['sequence']) is int and head['sequence']==seq and
             type(head['publication_sha']) is str and reader.re.fullmatch('[0-9a-f]{64}',head['publication_sha']) is not None and
             exact(head['checkpoint'],batch['snapshot']['next_checkpoint']), 'publication_head_binding_invalid')
        need(batch['admission']['batch_sequence']==seq,'original_publication_order_required')
        prefix.append(sha([PREFIX_SCHEMA,seq,prefix[-1],envelope_hashes[seq-1]]))
        key=sha(head['checkpoint']);need(key not in checkpoints,'duplicate_publication_checkpoint')
        checkpoints[key]=seq
        previous=head
    need(exact(publication['head'],previous),'publication_final_head_mismatch')
    return {'publication':publication,'profile_sha':profile_sha,'prefix':tuple(prefix),
            'heads':tuple(heads),'checkpoints':checkpoints,'evidence':IndexedEvidence(evidence, compact.evidence_store(publication))}


def _capture(index):
    publication=index['publication'];seq=publication['head']['sequence']
    return {'publication_profile_sha256':index['profile_sha'],'head':copy.deepcopy(publication['head']),
            'envelope_prefix_sha256':index['prefix'][seq]}


def _validate_capture(value,index):
    seq=value['head']['sequence'];reader._integer(seq,'consumer_prefix_sequence_integer',0,publisher.MAX_ATTEMPTS)
    publication=index['publication'];need(seq<=len(publication['batches']),'consumer_prefix_ahead_of_publisher')
    head=index['heads'][seq-1] if seq else {'sequence':0,'publication_sha':None,'checkpoint':None}
    expected={'publication_profile_sha256':index['profile_sha'],'head':head,'envelope_prefix_sha256':index['prefix'][seq]}
    need(exact(value,expected),'consumer_exact_prefix_binding_invalid')
    return seq


def capture(publication):return _capture(_publication_index(publication))


def validate_capture(value,publication):return _validate_capture(value,_publication_index(publication))


def _pack_scan(snapshot,index):
    need(type(snapshot) is dict,'completed_scan_dictionary_required')
    profile=index['publication']['profile']
    need(exact(snapshot['policy'],profile['policy']) and exact(snapshot['input_identity'],profile['input_identity']),
         'scan_source_stream_mismatch')
    core=copy.deepcopy({k:v for k,v in snapshot.items() if k not in ('policy','input_identity','entries')})
    references=[];objects={}
    for evidence in snapshot['entries']:
        key=reader.sha(evidence)
        if key in index['evidence']:
            need(exact(evidence,index['evidence'][key][1]),'publication_evidence_hash_conflict')
            references.append({'publication_evidence_sha256':key})
        else:
            objects[key]=copy.deepcopy(evidence);references.append({'stored_evidence_sha256':key})
    return {'schema_version':SCAN_SCHEMA,'snapshot_core':core,'evidence_refs':references},objects


def _unpack_scan(scan,index,objects,head_sequence):
    need(type(scan) is dict and set(scan)=={'schema_version','snapshot_core','evidence_refs'} and
         scan['schema_version']==SCAN_SCHEMA,'typed_compact_completed_scan_required')
    core=scan['snapshot_core'];references=scan['evidence_refs']
    need(type(core) is dict and not set(core)&{'policy','input_identity','entries'} and
         type(references) is list and len(references)<=reader.MAX_ROWS,'compact_scan_reference_bound')
    entries=[]
    for reference in references:
        need(type(reference) is dict and len(reference)==1,'exact_scan_reference_required')
        field,key=next(iter(reference.items()))
        need(type(key) is str and reader.re.fullmatch('[0-9a-f]{64}',key) is not None,'scan_object_sha_required')
        if field=='publication_evidence_sha256':
            need(key in index['evidence'] and index['evidence'][key][0]<=head_sequence,'scan_evidence_not_in_observed_publication')
            evidence=index['evidence'][key][1]
        else:
            need(field=='stored_evidence_sha256' and key in objects,'scan_object_missing')
            evidence=objects[key]
        need(reader.sha(evidence)==key,'scan_object_digest_invalid');entries.append(evidence)
    profile=index['publication']['profile']
    snapshot={**core,'policy':profile['policy'],'input_identity':profile['input_identity'],'entries':entries}
    need(len(reader.encoded(snapshot))<=MAX_DOCUMENT,'expanded_scan_document_bound')
    return snapshot


def _validate_scan(snapshot,index,head_sequence,observed,observed_state):
    """Same snapshot/clock/projection contracts, after one complete prefix check.

    The index supplies an already validated exact prior checkpoint, not a tail
    declared complete by the caller. No snapshot eligibility filter is applied.
    """
    publication=index['publication'];profile=publication['profile']
    need(exact(snapshot['policy'],profile['policy']) and exact(snapshot['input_identity'],profile['input_identity']),
         'scan_source_stream_mismatch')
    reader._validate_snapshot_shape(snapshot)
    need(snapshot['snapshot_sha256']==reader.sha({k:v for k,v in snapshot.items() if k!='snapshot_sha256'}) and
         snapshot['schema_version']==reader.SCHEMA and snapshot['complete_declared_prefix'] is True and
         snapshot['admitted'] is False,'snapshot_binding_invalid')
    previous=snapshot['previous_checkpoint'];before=0
    if previous is not None:
        before=index['checkpoints'].get(sha(previous),-1)
        need(0<before<=head_sequence and exact(previous,index['heads'][before-1]['checkpoint']),
             'scan_previous_checkpoint_not_in_observed_prefix')
    started=reader._number(snapshot['read_started_epoch']);completed=reader._number(snapshot['read_completed_epoch'])
    need(started<=completed<=observed and completed-started<=reader.MAX_SECONDS,'scan_not_completed_by_consumer')
    if before:need(publisher.admission_from_capture(publication,before)['admitted_available_epoch']<=observed,'scan_admission_clock_order')
    for state,at in ((snapshot['initial_clock_state'],started),(snapshot['initial_clock_state'],completed),
                     (snapshot['initial_clock_state'],observed),(snapshot['completion_clock_state'],completed),(observed_state,observed)):
        reader.repair.validate_clock_state(state,at)
    need(snapshot['row_count']==len(snapshot['entries'])<=reader.MAX_ROWS and
         snapshot['evidence_bytes']==sum(len(reader.encoded(e)) for e in snapshot['entries'])<=reader.MAX_BYTES,
         'snapshot_size_mismatch')
    prior=snapshot['after_seq']
    for evidence in snapshot['entries']:
        sequence=evidence['projection']['projection_seq']
        need(type(sequence) is int and prior<sequence<=snapshot['through_seq'],'duplicate_or_unordered_projection_sequence')
        reader.validate_projection(evidence,snapshot['policy'],started);prior=sequence
    need(prior==snapshot['through_seq'],'through_sequence_mismatch')
    head=index['heads'][head_sequence-1] if head_sequence else {'sequence':0,'publication_sha':None,'checkpoint':None}
    initial_empty=(head_sequence==0 and previous is None and snapshot['after_seq']==snapshot['through_seq']==snapshot['committed_high_watermark']==0)
    caught=(snapshot['more_pending'] is False and snapshot['through_seq']==snapshot['committed_high_watermark'] and
            (exact(snapshot['next_checkpoint'],head['checkpoint']) or initial_empty))
    return {'caught_up':caught,'read_started_epoch':started,'read_completed_epoch':completed,
            'more_pending':snapshot['more_pending'],'source_high_watermark':snapshot['committed_high_watermark'],
            'scan_through_seq':snapshot['through_seq']}


def _validate_observation(receipt,ack,profile,index,objects):
    need(receipt['schema_version']==ack['schema_version']==SCHEMA and receipt['observation_id']==ack['observation_id'] and
         receipt['profile_sha256']==ack['profile_sha256']==sha(profile) and ack['observation_sha256']==sha(receipt),
         'consumer_observation_binding_invalid')
    seq=_validate_capture(receipt['capture'],index)
    reader._integer(receipt['observation_sequence'],'consumer_sequence_integer',1,MAX_OBSERVATIONS)
    started=reader._number(receipt['read_started_epoch']);observed=reader._number(receipt['consumer_observed_epoch'])
    durable=reader._number(ack['receipt_observed_epoch'])
    need(started<=observed<=durable and observed-started<=publisher.MAX_SECONDS,'consumer_observation_clock_order')
    for state,at in ((receipt['initial_clock_state'],started),(receipt['initial_clock_state'],durable),
                    (receipt['completion_clock_state'],observed),(receipt['completion_clock_state'],durable),(ack['clock_state'],durable)):
        reader.repair.validate_clock_state(state,at)
    if seq:need(publisher.admission_from_capture(index['publication'],seq)['admitted_available_epoch']<=observed,
                'consumer_observation_precedes_publication_knowledge')
    snapshot=_unpack_scan(receipt['completed_scan'],index,objects,seq)
    coverage=_validate_scan(snapshot,index,seq,observed,receipt['completion_clock_state'])
    reader.repair.validate_clock_state(snapshot['initial_clock_state'],durable)
    return observed,coverage,len(reader.encoded(snapshot))


def schema_and_profile(con,profile,extra=0,new=False,new_objects=0):
    with sqlite3.connect(':memory:') as reference:
        reference.executescript(SQL)
        query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('table','trigger') ORDER BY type,name"
        need([tuple(r) for r in con.execute(query)]==reference.execute(query).fetchall(),'consumer_exact_schema_required')
    total=0
    for table in TABLES:
        count,size,maximum=con.execute('SELECT COUNT(*),COALESCE(SUM(length(CAST(body AS BLOB))),0),COALESCE(MAX(length(CAST(body AS BLOB))),0) FROM '+table).fetchone()
        limit=MAX_SCAN_OBJECTS if table=='scan_objects' else MAX_OBSERVATIONS
        need(count<=limit and maximum<=MAX_DOCUMENT,'consumer_row_document_bound')
        if table=='observations' and new:need(count<MAX_OBSERVATIONS,'consumer_observation_count_bound')
        if table=='scan_objects':need(count+new_objects<=MAX_SCAN_OBJECTS,'consumer_scan_object_count_bound')
        total+=size
    need(total+extra<=MAX_JSON,'consumer_total_json_bound')
    rows=con.execute('SELECT singleton,body FROM consumer_profile').fetchall()
    need(len(rows)==1 and rows[0][0]==1 and rows[0][1]==raw(profile).decode(),'consumer_exact_profile_required')


def _objects(con):
    result={}
    for row in con.execute('SELECT object_sha,body FROM scan_objects ORDER BY object_sha'):
        value = decode_frame(row['body'])
        need(value[1] == row['object_sha'], 'stored_scan_object_digest_invalid')
        compact.expand(value)
        result[row['object_sha']] = value
    return compact.owned_evidence(result)


def observe_published(publication_path,observation_path,*,cohort_id,consumer_id,expected_policy,input_identity,completed_scan,clock_provider,fault_hook=None):
    """New actual read; atomic compact receipt+objects, then independent acknowledgment."""
    path=publisher.path_for(observation_path,missing=True)
    need(str(path).lower()!=str(Path(publication_path)).lower() and str(path).lower()!=str(input_identity['path']).lower(),
         'separate_consumer_store_required')
    deadline=time.monotonic()+publisher.MAX_SECONDS;started,initial=clock(clock_provider)
    publication=publisher.read_published(publication_path,cohort_id=cohort_id,expected_policy=expected_policy,input_identity=input_identity)
    observed,completion=clock(clock_provider,started,initial)
    need(observed-started<=publisher.MAX_SECONDS,'consumer_read_duration_bound')
    index=_publication_index(publication);profile=profile_for(path,consumer_id,publication)
    source_snapshot=copy.deepcopy(completed_scan);raw(source_snapshot)
    _validate_scan(source_snapshot,index,publication['head']['sequence'],observed,completion)
    scan,objects=_pack_scan(source_snapshot,index);observed_capture=_capture(index)
    seq=observed_capture['head']['sequence']
    if seq:need(publisher.admission_from_capture(publication,seq)['admitted_available_epoch']<=observed,
                'consumer_observation_precedes_publication_knowledge')
    receipt={'schema_version':SCHEMA,'observation_id':uuid.uuid4().hex,'profile_sha256':sha(profile),
        'capture':observed_capture,'read_started_epoch':started,'consumer_observed_epoch':observed,
        'initial_clock_state':initial,'completion_clock_state':completion,'completed_scan':scan}
    initialize(path,profile,deadline);ident=publisher.identity(path)
    def hook(phase):
        if fault_hook is not None:fault_hook(phase)
    with connection(path,deadline,write=True,ident=ident) as con:
        schema_and_profile(con,profile)
        prior=con.execute('SELECT observation_sequence,body FROM observations ORDER BY observation_sequence DESC LIMIT 1').fetchone()
        sequence=prior['observation_sequence']+1 if prior else 1
        if prior:
            previous=publisher.checked_json(prior['body'])
            need(previous['consumer_observed_epoch']<=observed and previous['capture']['head']['sequence']<=seq,
                 'consumer_observation_order_regressed')
        receipt['observation_sequence']=sequence;receipt_raw=raw(receipt);receipt_sha=sha(receipt)
        new={}
        for key,evidence in objects.items():
            saved=con.execute('SELECT body FROM scan_objects WHERE object_sha=?',(key,)).fetchone()
            if saved:need(compact.expand(decode_frame(saved['body']))==raw(evidence),'stored_scan_object_conflict')
            else:new[key]=encode_frame(compact.value_frame(evidence))
        schema_and_profile(con,profile,len(receipt_raw)+sum(map(len,new.values())),True,len(new))
        for key,body in new.items():con.execute('INSERT INTO scan_objects VALUES(?,?)',(key,body.decode()))
        con.execute('INSERT INTO observations VALUES(?,?,?,?)',(sequence,receipt['observation_id'],receipt_sha,receipt_raw.decode()))
    hook('consumer_receipt_committed')
    with connection(path,deadline,ident=ident) as con:
        schema_and_profile(con,profile)
        saved=con.execute('SELECT observation_sha,body FROM observations WHERE observation_id=?',(receipt['observation_id'],)).fetchone()
        need(saved is not None and tuple(saved)==(receipt_sha,receipt_raw.decode()),'consumer_receipt_readback_failed')
        for key,evidence in objects.items():
            row=con.execute('SELECT body FROM scan_objects WHERE object_sha=?',(key,)).fetchone()
            need(row is not None and compact.expand(decode_frame(row['body']))==raw(evidence),'consumer_scan_object_readback_failed')
    hook('consumer_receipt_read_back')
    durable,durable_state=clock(clock_provider,observed,initial);reader.repair.validate_clock_state(completion,durable)
    ack={'schema_version':SCHEMA,'observation_id':receipt['observation_id'],'profile_sha256':sha(profile),
         'observation_sha256':receipt_sha,'receipt_observed_epoch':durable,'clock_state':durable_state}
    ack_raw=raw(ack);ack_sha=sha(ack)
    _validate_observation(receipt,ack,profile,index,objects);publisher.policy_current(expected_policy)
    with connection(path,deadline,write=True,ident=ident) as con:
        schema_and_profile(con,profile,len(ack_raw))
        con.execute('INSERT INTO acknowledgments VALUES(?,?,?)',(receipt['observation_id'],ack_sha,ack_raw.decode()))
    hook('consumer_ack_committed')
    with connection(path,deadline,ident=ident) as con:
        schema_and_profile(con,profile)
        saved=con.execute('SELECT ack_sha,body FROM acknowledgments WHERE observation_id=?',(receipt['observation_id'],)).fetchone()
        need(saved is not None and tuple(saved)==(ack_sha,ack_raw.decode()),'consumer_ack_readback_failed')
    publisher.policy_current(expected_policy)
    return {'status':'consumer_observation_durable_and_read_back','schema_version':SCHEMA,
            'observation':receipt,'acknowledgment':ack,'receipt_sha256':receipt_sha,'ack_sha256':ack_sha,
            'consumer_observed_epoch':observed,'joint_model_consumption_proven':False,**reader.INERT}


def _capture_observation_rows(path,profile,ident,capture_deadline):
    """Return owned primitive rows only after the actual read lock is released."""
    with connection(path,capture_deadline,ident=ident) as con:
        schema_and_profile(con,profile)
        packed_objects=[];packed_observations=[];retained=0
        queries=((packed_objects,'SELECT object_sha,body FROM scan_objects ORDER BY object_sha'),
                 (packed_observations,'SELECT o.observation_sequence,o.observation_id,o.observation_sha,o.body AS observation_body,a.ack_sha,a.body AS ack_body FROM observations o JOIN acknowledgments a USING(observation_id) ORDER BY o.observation_sequence'))
        for target,query in queries:
            for row in con.execute(query):
                need(time.monotonic()<=capture_deadline,'consumer_packed_capture_time_bound')
                values=tuple(row)
                for value in values:
                    need(type(value) in (str,int),'consumer_immutable_primitive_required')
                    retained+=len(value.encode('utf-8')) if type(value) is str else 8
                need(retained<=MAX_JSON,'consumer_packed_capture_byte_bound');target.append(values)
        packed_objects=tuple(packed_objects);packed_observations=tuple(packed_observations)
    return packed_objects,packed_observations,retained


def read_observations(publication_path,observation_path,*,cohort_id,consumer_id,expected_policy,input_identity):
    """One complete exact snapshot of acknowledged observations; never invent a poll."""
    path=publisher.path_for(observation_path)
    publication=publisher.read_published(publication_path,cohort_id=cohort_id,expected_policy=expected_policy,input_identity=input_identity)
    deadline=time.monotonic()+publisher.MAX_SECONDS
    profile=profile_for(path,consumer_id,publication);ident=publisher.identity(path);observations=[]
    began=time.monotonic()
    def capture(capture_deadline):return _capture_observation_rows(path,profile,ident,capture_deadline)
    (packed_objects,packed_observations,retained),capture_attempts=publisher._capture_with_retry(capture,deadline)
    capture_seconds=time.monotonic()-began
    transaction_seconds=capture_attempts['successful_transaction_seconds']
    frames={}
    for key,body in packed_objects:
        need(time.monotonic()<=deadline,'consumer_owned_preparation_time_bound')
        value=decode_frame(body);need(value[1]==key,'stored_scan_object_digest_invalid');frames[key]=value
    objects=compact.owned_evidence(frames)
    for sequence,oid,receipt_sha,receipt_body,ack_sha,ack_body in packed_observations:
        need(time.monotonic()<=deadline,'consumer_owned_preparation_time_bound')
        receipt=publisher.checked_json(receipt_body);ack=publisher.checked_json(ack_body)
        need(sha(receipt)==receipt_sha and sha(ack)==ack_sha and receipt['observation_id']==oid,
             'consumer_stored_digest_invalid')
        need(type(receipt['observation_sequence']) is int and receipt['observation_sequence']==sequence,'consumer_stored_sequence_invalid')
        observations.append({'observation':receipt,'acknowledgment':ack,'receipt_sha256':receipt_sha,'ack_sha256':ack_sha})
    result={'schema_version':SCHEMA,'profile':profile,'publication_integrity':publication,'observations':observations,
            'scan_objects':objects,'consumer_observation_readback_proven':True,'joint_model_consumption_proven':False,**reader.INERT}
    context = _prepare_owned(result)
    need(time.monotonic()<=deadline,'consumer_owned_preparation_time_bound')
    need(publisher.identity(path)==ident,'consumer_store_identity_changed')
    _CAPTURE_METRICS[str(path).casefold()]={'transaction_seconds':transaction_seconds,'packed_retained_bytes':retained,
        'observed_row_count':len(packed_observations),'scan_object_count':len(packed_objects),
        'preparation_seconds':time.monotonic()-began-capture_seconds,'capture_attempts':capture_attempts,
        'real_transaction_closed_before_semantic_validation':True}
    while len(_CAPTURE_METRICS)>publisher.MAX_CACHED_STORES:_CAPTURE_METRICS.popitem(last=False)
    publisher.policy_current(expected_policy)
    return context


def capture_metrics(observation_path):
    return copy.deepcopy(_CAPTURE_METRICS.get(str(publisher.path_for(observation_path)).casefold()))


_CONTEXT_KEY=object()


class PreparedConsumer:
    """Opaque identity only; immutable data lives in a private weak registry."""
    __slots__=('__weakref__',)
    def __new__(cls,*args,**kwargs):raise ValueError('prepared_consumer_factory_required')
    def __setattr__(self,name,value):raise AttributeError('immutable_prepared_consumer')


def _context_storage():
    registry=weakref.WeakKeyDictionary()
    def make(data):
        context=object.__new__(PreparedConsumer);registry[context]=data
        return context
    def get(context):
        need(type(context) is PreparedConsumer and context in registry,'exact_prepared_consumer_required')
        return registry[context]
    return make,get


_make_prepared,_prepared=_context_storage()


def prepared_context_metadata(context):return json.loads(_prepared(context)[1])


def _prepare_owned(owned):
    """Only fixed database capture or fully checked compact recipe reaches here."""
    manifest = _manifest_from_owned(owned)
    _, readback_sha, readback_bytes = bounded_owned(manifest)
    need(owned['schema_version']==SCHEMA and owned['consumer_observation_readback_proven'] is True,'typed_consumer_readback_required')
    publication=owned['publication_integrity'];profile=owned['profile'];entries=owned['observations'];objects=owned['scan_objects']
    need(exact(profile['publication_profile'],publication['profile']),'consumer_publication_profile_mismatch')
    need(profile['schema_version']==SCHEMA and profile['consumer_sha256']==hashlib.sha256(Path(__file__).read_bytes()).hexdigest() and
         profile['publisher_sha256']==publication['profile']['publisher_sha256']==hashlib.sha256(Path(publisher.__file__).read_bytes()).hexdigest(),
         'current_consumer_publisher_generation_required')
    need(type(profile['max_scan_age_sec']) is int and profile['max_scan_age_sec']==MAX_SCAN_AGE_SEC and
         profile['scan_age_basis']=='source_read_started_epoch','exact_coverage_policy_required')
    publisher.policy_current(publication['profile']['policy'])
    need(type(entries) is list and len(entries)<=MAX_OBSERVATIONS and type(objects) is compact.EvidenceStore and len(objects)<=MAX_SCAN_OBJECTS,
         'consumer_observation_list_bound')
    for key in objects:need(type(key) is str and reader.sha(objects[key])==key,'supplied_scan_object_digest_invalid')
    index=_publication_index(publication)
    first=[None]*(len(publication['batches'])+1);rows=[];seen={};seen_sequence={}
    prior_sequence=0;prior_epoch=None;prior_head=0;assigned=0;expanded=0
    for item in entries:reader._integer(item['observation']['observation_sequence'],'consumer_sequence_integer',1,MAX_OBSERVATIONS)
    for item in sorted(entries,key=lambda value:value['observation']['observation_sequence']):
        receipt=item['observation'];ack=item['acknowledgment']
        need(sha(receipt)==item['receipt_sha256'] and sha(ack)==item['ack_sha256'],'consumer_supplied_digest_invalid')
        observed,coverage,size=_validate_observation(receipt,ack,profile,index,objects)
        expanded+=size;need(expanded<=MAX_EXPANDED_SCAN_BYTES,'expanded_scan_work_bound')
        oid=receipt['observation_id'];sequence=receipt['observation_sequence'];head=receipt['capture']['head']['sequence']
        if oid in seen:need(exact(seen[oid],item),'consumer_duplicate_conflict');continue
        need(sequence not in seen_sequence,'consumer_duplicate_sequence_conflict');seen[oid]=item;seen_sequence[sequence]=oid
        need(sequence>prior_sequence and (prior_epoch is None or observed>=prior_epoch) and head>=prior_head,
             'consumer_supplied_observation_order')
        prior_sequence=sequence;prior_epoch=observed;prior_head=head
        while assigned<head:assigned+=1;first[assigned]=observed
        rows.append((observed,head,reader.encoded(coverage)))
    versions={}; member_bytes=0; compressed_member_bytes=0
    for event, sequence, body in compact.member_records(publication):
        member_bytes += body[0]; compressed_member_bytes += len(body[2])
        need(member_bytes <= compact.MAX_INDEX_EXPANDED and compressed_member_bytes <= compact.MAX_INDEX_COMPRESSED,
             'prepared_member_byte_bound')
        versions.setdefault(event, []).append((sequence, body))
    frozen=tuple((event,tuple(seq for seq,_ in values),tuple(body for _,body in values)) for event,values in sorted(versions.items()))
    metadata={'schema_version':CONTEXT_SCHEMA,'readback_sha256':readback_sha,'readback_bytes':readback_bytes,'consumer_profile':profile,
              'publication_profile':publication['profile'],'source_bindings':{
                  'consumer':profile['consumer_sha256'],'publisher':profile['publisher_sha256'],
                  'compact_store':hashlib.sha256(Path(compact.__file__).read_bytes()).hexdigest(),
                  'reader':hashlib.sha256(Path(reader.__file__).read_bytes()).hexdigest()},
              'validation_scope':'complete_supplied_history_validated_not_database_capture_authenticated',
              'observation_count':len(rows),'publication_count':len(publication['batches']),
              'expanded_scan_bytes_checked':expanded,'member_bytes':member_bytes,
              'compressed_member_bytes':compressed_member_bytes,
              'expanded_evidence_bytes_checked':compact.manifest(publication)['expanded_evidence_bytes_verified'],
              'manifest_bytes':readback_bytes, 'manifest_sha256':readback_sha,
              'readback_digest_scope':'compact_manifest_not_expanded_legacy_readback',
              'consumer_receipt_max_epoch':max((item['acknowledgment']['receipt_observed_epoch'] for item in entries),default=None),
              'evidence_encoding':compact.SCHEMA, 'semantic_complete_validations':1}
    context = _make_prepared((_CONTEXT_KEY,reader.encoded(metadata),profile['consumer_id'],
                           tuple(row[0] for row in rows),tuple(rows),tuple(first),frozen))
    _recipe_capture[context] = (publication, reader.encoded({k:v for k,v in owned.items() if k not in ('publication_integrity','scan_objects')}), objects)
    headers={}
    for event,sequence,body in compact.header_records(publication):headers.setdefault(event,[]).append((sequence,body))
    _context_headers[context]=tuple((event,tuple(seq for seq,_ in values),tuple(body for _,body in values)) for event,values in sorted(headers.items()))
    _context_event_ids[context]=tuple(v[0] for v in frozen)
    return context


def latest_consumed_from_context(context,decision_epoch):
    """Pure as-of selection after complete validation; no history parse or source IO."""
    _,_,consumer_id,epochs,rows,first,versions=_prepared(context);decision=reader._number(decision_epoch)
    at=bisect.bisect_right(epochs,decision)-1;coverage=None;prefix=0
    if at>=0:
        _,prefix,coverage_body=rows[at];coverage=json.loads(coverage_body)
    usable=coverage is not None and coverage['caught_up'] and 0<=decision-coverage['read_started_epoch']<=MAX_SCAN_AGE_SEC
    if not usable:
        return {'schema_version':SCHEMA,'decision_epoch':decision,'consumer_id':consumer_id,
                'coverage_usable':False,'status':'coverage_unavailable','coverage':coverage,'members':None,
                'canonical_event_count':None,'consumer_observation_required':True,'joint_model_consumption_proven':False,**reader.INERT}
    members=[]
    for event,sequences,bodies in versions:
        index=bisect.bisect_right(sequences,prefix)-1
        if index<0:continue
        member=_materialize_member(context,bodies[index]);observed=first[sequences[index]]
        need(observed is not None and observed<=decision,'prepared_member_observation_required')
        member['consumer_observed_epoch']=observed
        member['effective_transport_available_epoch']=max(observed,member['admitted_available_epoch'])
        members.append(member)
    return {'schema_version':SCHEMA,'decision_epoch':decision,'members':members,'canonical_event_count':len(members),
            'selection_before_relevance_score_and_headline_filters':True,'full_history_window_proven':False,
            'durable_storage_proven_by_this_pure_function':False,'consumer_id':consumer_id,
            'coverage_usable':True,'status':'observed_current_complete','coverage':coverage,
            'consumer_observation_required':True,'observed_publication_prefix':prefix,
            'original_source_story_classification_clocks_unchanged':True,'joint_model_consumption_proven':False,**reader.INERT}


def latest_consumed_as_of(readback,decision_epoch):
    return latest_consumed_from_context(prepare_consumed_context(readback),decision_epoch)

def raw(value):
    result=reader.encoded(value);need(len(result)<=MAX_DOCUMENT,'consumer_document_bound');return result


def sha(value):return hashlib.sha256(raw(value)).hexdigest()


def exact(a,b):return raw(a)==raw(b)


def clock(provider,previous=None,original=None):
    now,state=provider();now=reader._number(now);state=copy.deepcopy(state)
    if previous is not None:need(now>=previous,'consumer_clock_moved_backwards')
    reader.repair.validate_clock_state(state,now)
    if original is not None:reader.repair.validate_clock_state(original,now)
    return now,state


def profile_for(path,consumer_id,publication):
    need(type(consumer_id) is str and reader.re.fullmatch('[a-zA-Z0-9_-]{1,96}',consumer_id),'explicit_consumer_id_required')
    return {'schema_version':SCHEMA,'store_path':str(path),'consumer_id':consumer_id,
        'publication_profile':copy.deepcopy(publication['profile']),
        'publisher_sha256':hashlib.sha256(Path(publisher.__file__).read_bytes()).hexdigest(),
        'consumer_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'observation_semantics':'this_named_consumer_independently_read_committed_exact_prefix',
        'max_scan_age_sec':MAX_SCAN_AGE_SEC,'scan_age_basis':'source_read_started_epoch',
        'joint_model_consumption_proven':False,'old_history_imported':False,**reader.INERT}


@contextmanager
def connection(path,deadline,*,write=False,ident=None):
    need(path.stat().st_size<=MAX_DATABASE,'consumer_database_bound')
    with publisher.connection(path,deadline,write=write,expected_identity=ident) as con:
        con.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,MAX_DOCUMENT)
        if write:
            page=con.execute('PRAGMA page_size').fetchone()[0]
            con.execute('PRAGMA max_page_count='+str(MAX_DATABASE//page))
        yield con


def initialize(path,profile,deadline):
    if path.exists():return
    with path.open('xb') as file:file.flush();os.fsync(file.fileno())
    with sqlite3.connect(path,timeout=2) as con:
        con.execute('PRAGMA synchronous=FULL');con.executescript('BEGIN IMMEDIATE;'+SQL)
        con.execute('INSERT INTO consumer_profile VALUES(1,?)',(raw(profile).decode(),))
        need(time.monotonic()<=deadline,'consumer_operation_time_bound');con.commit()

