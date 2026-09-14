"""Bounded isolated projection admission; no collector/history mutation or CLI.

The clock provider and snapshot producer are trusted in-process dependencies.
This store proves durable byte bindings, not authentic external timestamps.
"""
import copy
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import re
import sqlite3
import stat
import time
import uuid
from contextlib import contextmanager
import projection_revision_reader_v1 as reader
import compact_projection_store_v1 as compact
import base64
from collections import OrderedDict
from types import MappingProxyType

SCHEMA = 'isolated_projection_admission_cas_v4_20260914'
MAX_DOCUMENT = 18 * 1024 * 1024
MAX_JSON_TOTAL = 256 * 1024 * 1024
MAX_DATABASE = 384 * 1024 * 1024
MAX_ATTEMPTS = 4096
MAX_SECONDS = 30
# A separately bounded non-authorizing startup/replay pass. The 12k prefix's
# measured cold work is dominated by exact JSON/hash validation; normal fresh
# reads remain MAX_SECONDS and each actual read lock remains short below.
MAX_COLD_READ_SECONDS = 300
MAX_CAPTURE_SECONDS = 1.0
MAX_CAPTURE_BYTES = 256 * 1024 * 1024
MAX_CAPTURE_ATTEMPTS = 3
MAX_CACHED_STORES = 2
_CAPTURE_CACHE = OrderedDict()
_CAPTURE_METRICS = OrderedDict()
TABLES = {
    'evidence_objects': 'object_sha TEXT PRIMARY KEY, expanded_bytes INTEGER NOT NULL, packed_sha TEXT NOT NULL, payload BLOB NOT NULL',
    'profile': 'singleton INTEGER PRIMARY KEY CHECK(singleton=1), body TEXT NOT NULL',
    'attempts': 'attempt_id TEXT PRIMARY KEY, snapshot_sha TEXT NOT NULL UNIQUE, receipt_sha TEXT NOT NULL, body TEXT NOT NULL',
    'acknowledgments': 'attempt_id TEXT PRIMARY KEY, ack_sha TEXT NOT NULL, body TEXT NOT NULL',
    'published': 'sequence INTEGER PRIMARY KEY, attempt_id TEXT NOT NULL UNIQUE, publication_sha TEXT NOT NULL, body TEXT NOT NULL',
}
SQL = '\n'.join('CREATE TABLE '+name+'('+columns+');\n'+
    '\n'.join('CREATE TRIGGER '+name+'_no_'+action.lower()+' BEFORE '+action+' ON '+name+
    " BEGIN SELECT RAISE(ABORT,'immutable_projection_admission'); END;" for action in ('UPDATE','DELETE'))
    for name, columns in TABLES.items())


def need(value, reason):
    if not value: raise ValueError(reason)


class CaptureReadTimeout(ValueError):
    """No current snapshot was accepted; an older verified cache is still inert."""


def _capture_with_retry(capture, deadline):
    """Retry only an expired short read after its connection has been closed.

    The callback owns one read-only transaction and returns immutable captured
    bytes. Partial snapshots and failed connections never survive into a retry.
    Representation, validation and the caller's outer deadline are unchanged.
    """
    attempts=[]
    for attempt in range(1,MAX_CAPTURE_ATTEMPTS+1):
        began=time.monotonic()
        if began>=deadline: raise CaptureReadTimeout('packed_capture_outer_time_bound')
        capture_deadline=min(deadline,began+MAX_CAPTURE_SECONDS)
        try:
            result=capture(capture_deadline)
        except Exception as error:
            elapsed=time.monotonic()-began
            timed_out=time.monotonic()>=capture_deadline
            reason=str(error)
            transient=timed_out and (
                isinstance(error,sqlite3.OperationalError) and reason.lower() in ('interrupted','database is locked','database is busy')
                or isinstance(error,ValueError) and reason in ('packed_capture_time_bound','consumer_packed_capture_time_bound','operation_time_bound','consumer_operation_time_bound'))
            if not transient: raise
            attempts.append({'attempt':attempt,'seconds':elapsed,'reason':reason})
            if attempt==MAX_CAPTURE_ATTEMPTS or time.monotonic()>=deadline:
                raise CaptureReadTimeout('packed_capture_retries_exhausted') from error
        else:
            return result,{'attempt_count':attempt,'retry_failures':attempts,
                           'successful_transaction_seconds':time.monotonic()-began,
                           'capture_seconds_total':sum(item['seconds'] for item in attempts)+time.monotonic()-began}
    raise AssertionError('bounded_capture_loop_unreachable')


def raw(value):
    result = reader.encoded(value)
    need(len(result) <= MAX_DOCUMENT, 'document_byte_bound')
    return result


def digest(value): return hashlib.sha256(raw(value)).hexdigest()


def exact(a, b): return raw(a) == raw(b)


def pack_receipt(receipt):
    snapshot = receipt['snapshot']
    core = {k:v for k,v in snapshot.items() if k not in ('entries','policy','input_identity')}
    frames = {}
    for evidence in snapshot['entries']:
        value = compact.value_frame(evidence); frames[value[1]] = value
    packed = {k:v for k,v in receipt.items() if k != 'snapshot'}
    packed['snapshot_pack'] = {'schema_version':compact.SCHEMA, 'core':core,
        'source_identity_sha256':reader.sha([snapshot['policy'],snapshot['input_identity']]),
        'evidence_sha256':[reader.sha(e) for e in snapshot['entries']]}
    return packed, frames


def insert_frames(con, frames, extra):
    new = []
    for key, value in frames.items():
        size, original, payload = value
        need(key == original, 'cas_key_mismatch')
        packed_sha = hashlib.sha256(payload).hexdigest()
        old = con.execute('SELECT expanded_bytes,packed_sha,payload FROM evidence_objects WHERE object_sha=?', (key,)).fetchone()
        if old is not None:
            need(tuple(old) == (size, packed_sha, payload), 'cas_immutable_conflict')
        else: new.append((key, size, packed_sha, payload))
    bounds(con, extra + sum(len(row[3]) for row in new))
    count, expanded = con.execute('SELECT COUNT(*),COALESCE(SUM(expanded_bytes),0) FROM evidence_objects').fetchone()
    need(count + len(new) <= compact.MAX_OBJECTS and expanded + sum(row[1] for row in new) <= compact.MAX_EXPANDED_PREFIX,
         'cas_store_work_bound')
    con.executemany('INSERT INTO evidence_objects VALUES(?,?,?,?)', new)


def unpack_receipt(con, packed, frames):
    need(type(packed) is dict and 'snapshot' not in packed and 'snapshot_pack' in packed, 'compact_receipt_required')
    pack = packed['snapshot_pack']
    need(type(pack) is dict and set(pack) == {'schema_version','core','evidence_sha256','source_identity_sha256'}
         and pack['schema_version'] == compact.SCHEMA and not set(pack['core']) & {'entries','policy','input_identity'}, 'typed_snapshot_pack_required')
    profile = checked_json(con.execute('SELECT body FROM profile WHERE singleton=1').fetchone()[0])
    need(pack['source_identity_sha256'] == reader.sha([profile['policy'],profile['input_identity']]), 'receipt_source_identity_invalid')
    refs = pack['evidence_sha256']; need(type(refs) is list and len(refs) <= reader.MAX_ROWS, 'snapshot_reference_bound')
    entries = []
    for key in refs:
        need(type(key) is str and re.fullmatch('[0-9a-f]{64}', key), 'cas_reference_digest_required')
        if key not in frames:
            row = con.execute('SELECT expanded_bytes,packed_sha,payload FROM evidence_objects WHERE object_sha=?', (key,)).fetchone()
            need(row is not None and hashlib.sha256(row['payload']).hexdigest() == row['packed_sha'], 'cas_stored_frame_missing_or_changed')
            frames[key] = (row['expanded_bytes'], key, bytes(row['payload']))
        entries.append(compact.value_expand(frames[key]))
    receipt = {k:v for k,v in packed.items() if k != 'snapshot_pack'}
    receipt['snapshot'] = {**pack['core'], 'entries':entries, 'policy':profile['policy'],'input_identity':profile['input_identity']}
    raw(receipt)  # Restore the original per-document bound, not its packed size.
    return receipt


def export_publication(publication):
    compact.manifest(publication)  # Require an actual owned validated capture.
    frames = compact.evidence_frames(publication)
    return {'schema_version':compact.SCHEMA, 'profile':publication['profile'],
        'record_proofs':publication['record_proofs'],
        'evidence_objects':[[key, f[0], hashlib.sha256(f[2]).hexdigest(), base64.b64encode(f[2]).decode('ascii')]
                            for key, f in sorted(frames.items())]}


def admission_from_capture(publication, sequence):
    reader._integer(sequence, 'original_publication_sequence_required', 1, MAX_ATTEMPTS)
    return json.loads(compact.batch_descriptor(publication, sequence-1))['admission']


def restore_publication(recipe):
    """Replay exact archived proofs in RAM; never imports/adopts a live store."""
    need(type(recipe) is dict and set(recipe) == {'schema_version','profile','record_proofs','evidence_objects'}
         and recipe['schema_version'] == compact.SCHEMA, 'typed_publication_recipe_required')
    manifest = {**recipe,'evidence_objects':[[key,size,packed_sha,len(base64.b64decode(encoded,validate=True))]
                                            for key,size,packed_sha,encoded in recipe['evidence_objects']]}
    def objects():
        for key,size,packed_sha,encoded in recipe['evidence_objects']:
            yield {'kind':'publication','object_sha':key,'expanded_bytes':size,'packed_sha':packed_sha,'zlib_base64':encoded}
    return restore_publication_manifest(manifest, objects())


def restore_publication_manifest(recipe, objects):
    need(type(recipe) is dict and set(recipe) == {'schema_version','profile','record_proofs','evidence_objects'}
         and recipe['schema_version'] == compact.SCHEMA, 'typed_publication_manifest_required')
    profile = recipe['profile']; policy_current(profile['policy'])
    expected = profile_for(Path(profile['store_path']), profile['cohort_id'], profile['policy'], profile['input_identity'])
    need(exact(profile, expected), 'archived_publisher_generation_mismatch')
    need(type(recipe['record_proofs']) is list and len(recipe['record_proofs']) <= MAX_ATTEMPTS
         and type(recipe['evidence_objects']) is list and len(recipe['evidence_objects']) <= compact.MAX_OBJECTS,
         'archived_prefix_count_bound')
    deadline = time.monotonic() + MAX_COLD_READ_SECONDS
    with sqlite3.connect(':memory:') as con:
        con.row_factory = sqlite3.Row; con.executescript(SQL)
        con.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_DOCUMENT)
        con.execute('INSERT INTO profile VALUES(1,?)', (raw(profile).decode(),))
        total = 0
        previous = None
        for key, size, packed_sha, packed_size in recipe['evidence_objects']:
            need(type(key) is str and re.fullmatch('[0-9a-f]{64}',key) and (previous is None or key>previous), 'ordered_unique_cas_manifest_required')
            previous = key
            item = next(objects, None)
            need(type(item) is dict and set(item) == {'kind','object_sha','expanded_bytes','packed_sha','zlib_base64'}
                 and item['kind']=='publication' and item['object_sha']==key and item['expanded_bytes']==size
                 and item['packed_sha']==packed_sha, 'exact_ordered_cas_object_required')
            encoded = item['zlib_base64']
            need(type(encoded) is str and len(encoded) <= (MAX_DOCUMENT * 4 // 3 + 4), 'cas_encoded_frame_bound')
            payload = base64.b64decode(encoded, validate=True); total += len(payload)
            need(type(size) is int and 0<=size<=MAX_DOCUMENT and type(packed_size) is int and len(payload)==packed_size
                 and total <= compact.MAX_COMPRESSED and hashlib.sha256(payload).hexdigest() == packed_sha, 'cas_recipe_byte_or_digest_bound')
            con.execute('INSERT INTO evidence_objects VALUES(?,?,?,?)', (key,size,packed_sha,payload))
        for record in recipe['record_proofs']:
            need(type(record) is dict and set(record) == {'attempt','ack','publication'}, 'typed_original_publication_proofs_required')
            for field, table in (('attempt','attempts'),('ack','acknowledgments'),('publication','published')):
                row = record[field]
                columns = [c[1] for c in con.execute('PRAGMA table_info(' + table + ')')]
                need(set(row) == set(columns), 'exact_original_record_columns_required')
                con.execute('INSERT INTO ' + table + ' VALUES(' + ','.join('?' for _ in columns) + ')', [row[k] for k in columns])
        result = _read(con, profile, deadline)
        need(len(compact.evidence_frames(result)) == len(recipe['evidence_objects']), 'unreferenced_publication_cas_refused')
        return result


def path_for(value, *, missing=False):
    text = os.fspath(value); pure = PureWindowsPath(text)
    need(pure.is_absolute() and pure.drive.lower() == 'c:' and '..' not in pure.parts
        and all(':' not in part for part in pure.parts[1:]), 'plain_absolute_c_path_required')
    path = Path(text)
    for item in (*reversed(path.parents), path):
        try: info = item.lstat()
        except FileNotFoundError:
            need(missing and item == path and path.parent.is_dir(), 'existing_parent_required')
            continue
        need(not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_file_attributes',0)&1024, 'reparse_path_refused')
    if path.exists(): need(path.is_file(), 'regular_store_required')
    for suffix in ('-wal','-shm','-journal'):
        side = path.with_name(path.name+suffix)
        try: info=side.lstat()
        except FileNotFoundError: continue
        need(not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_file_attributes',0)&1024, 'reparse_sidecar_refused')
    return path


def identity(path):
    info=path_for(path).stat()
    return (info.st_dev, info.st_ino)


def policy_current(policy):
    need(exact(policy, reader.policy_for(policy['sources'],policy['provenance'])), 'current_reader_source_policy_required')


def profile_for(path, cohort, policy, input_identity):
    need(type(cohort) is str and re.fullmatch('[a-zA-Z0-9_-]{1,96}',cohort), 'explicit_new_cohort_required')
    policy_current(policy)
    return {'schema_version':SCHEMA, 'store_path':str(path), 'cohort_id':cohort,
        'policy':copy.deepcopy(policy), 'input_identity':copy.deepcopy(input_identity),
        'publisher_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'compact_store_sha256':hashlib.sha256(Path(compact.__file__).read_bytes()).hexdigest(),
        'storage_contract':compact.SCHEMA,
        'old_history_imported':False, 'failed_attempt_readmission_allowed':False,
        'availability_basis':'independent_committed_receipt_and_acknowledgment_reads', **reader.INERT}


@contextmanager
def connection(path, deadline, *, write=False, expected_identity=None):
    path_for(path)
    if expected_identity is not None: need(identity(path)==expected_identity,'store_identity_changed')
    need(path.stat().st_size<=MAX_DATABASE,'database_byte_bound')
    con=sqlite3.connect(str(path) if write else path.as_uri()+'?mode=ro',uri=not write,timeout=2)
    try:
        con.row_factory=sqlite3.Row
        con.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,MAX_DOCUMENT)
        con.set_progress_handler(lambda: int(time.monotonic()>deadline),1000)
        if write:
            con.execute('PRAGMA synchronous=FULL')
            need(con.execute('PRAGMA journal_mode').fetchone()[0]=='delete','delete_journal_required')
            page=con.execute('PRAGMA page_size').fetchone()[0]
            con.execute('PRAGMA max_page_count='+str(MAX_DATABASE//page))
        else: con.execute('PRAGMA query_only=ON')
        con.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
        yield con
        need(time.monotonic()<=deadline,'operation_time_bound')
        if expected_identity is not None: need(identity(path)==expected_identity,'store_identity_changed')
        if write: con.commit()
    finally: con.close()


def schema_check(con):
    with sqlite3.connect(':memory:') as expected:
        expected.executescript(SQL)
        query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('table','trigger') ORDER BY type,name"
        need([tuple(r) for r in con.execute(query)]==expected.execute(query).fetchall(),'exact_store_schema_required')


def bounds(con, extra=0, *, new_attempt=False):
    total=0
    for table in TABLES:
        if table == 'evidence_objects':
            count, size, expanded, maximum = con.execute('SELECT COUNT(*),COALESCE(SUM(length(payload)),0),COALESCE(SUM(expanded_bytes),0),COALESCE(MAX(expanded_bytes),0) FROM evidence_objects').fetchone()
            need(count <= compact.MAX_OBJECTS and size <= compact.MAX_COMPRESSED
                 and expanded <= compact.MAX_EXPANDED_PREFIX and maximum <= MAX_DOCUMENT, 'cas_store_work_bound')
            total += size
            continue
        count, size, maximum=con.execute('SELECT COUNT(*),COALESCE(SUM(length(CAST(body AS BLOB))),0),COALESCE(MAX(length(CAST(body AS BLOB))),0) FROM '+table).fetchone()
        need(maximum<=MAX_DOCUMENT and count<=MAX_ATTEMPTS,'store_row_or_document_bound')
        if table=='attempts' and new_attempt: need(count<MAX_ATTEMPTS,'attempt_bound')
        total+=size
    need(total+extra<=MAX_JSON_TOTAL,'total_json_byte_bound')


def _capture_packed(con, expected_profile, deadline):
    """Copy immutable primitive row values while holding one bounded RO view.

    This is capture only, never a semantic-validation attestation. No caller
    can use these rows as a model context without the complete _read checks.
    """
    schema_check(con);bounds(con)
    profile=con.execute('SELECT singleton,body FROM profile').fetchall()
    need(len(profile)==1 and profile[0][0]==1 and exact(checked_json(profile[0][1]),expected_profile), 'capture_exact_profile_required')
    tables={};retained=0;count=0
    order={'profile':'singleton','attempts':'attempt_id','acknowledgments':'attempt_id',
           'published':'sequence','evidence_objects':'object_sha'}
    for table in TABLES:
        rows=[]
        for row in con.execute('SELECT * FROM '+table+' ORDER BY '+order[table]):
            need(time.monotonic()<=deadline,'packed_capture_time_bound')
            values=tuple(row)
            for value in values:
                need(type(value) in (str,bytes,int) or value is None,'immutable_sqlite_primitive_required')
                retained+=len(value.encode('utf-8')) if type(value) is str else len(value) if type(value) is bytes else 8
            need(retained<=MAX_CAPTURE_BYTES,'packed_capture_retained_byte_bound')
            rows.append(values);count+=1
        tables[table]=tuple(rows)
    need(time.monotonic()<=deadline,'packed_capture_time_bound')
    return MappingProxyType(tables),retained,count


def _read_owned_capture(tables, profile, deadline, base):
    """Full validation after the real database transaction has been closed."""
    with sqlite3.connect(':memory:') as con:
        con.row_factory=sqlite3.Row;con.executescript(SQL)
        con.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,MAX_DOCUMENT)
        con.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
        page=con.execute('PRAGMA page_size').fetchone()[0]
        con.execute('PRAGMA max_page_count='+str(MAX_DATABASE//page))
        for table in TABLES:
            need(time.monotonic()<=deadline,'owned_capture_restore_time_bound')
            width=len(con.execute('PRAGMA table_info('+table+')').fetchall())
            con.executemany('INSERT INTO '+table+' VALUES('+','.join('?' for _ in range(width))+')',tables[table])
        return _read(con,profile,deadline,base)


def capture_metrics(store_path):
    return copy.deepcopy(_CAPTURE_METRICS.get(str(path_for(store_path)).casefold()))


def checked_json(text):
    need(type(text) is str and len(text.encode())<=MAX_DOCUMENT,'retained_document_required')
    value=json.loads(text)
    need(type(value) is dict and raw(value).decode()==text,'canonical_document_required')
    return value


def head(con):
    row=con.execute('SELECT sequence,publication_sha,body FROM published ORDER BY sequence DESC LIMIT 1').fetchone()
    if row is None:return {'sequence':0,'publication_sha':None,'checkpoint':None}
    body=checked_json(row['body'])
    need(digest(body)==row['publication_sha'],'head_digest_mismatch')
    return {'sequence':row['sequence'],'publication_sha':row['publication_sha'],'checkpoint':body['next_checkpoint']}


def _admission(snapshot, sequence, admitted, available, clock_state):
    value={'schema_version':reader.ADMISSION,'status':'admitted',
        'availability_basis':'externally_supplied_durable_admission_readback',
        'snapshot_sha256':snapshot['snapshot_sha256'],'batch_sequence':sequence,
        'first_projection_seq':snapshot['entries'][0]['projection']['projection_seq'] if snapshot['entries'] else None,
        'through_seq':snapshot['through_seq'],'admitted_epoch':admitted,
        'admitted_available_epoch':available,'clock_state':copy.deepcopy(clock_state),
        'batch_id':reader.sha([reader.ADMISSION,reader.sha(snapshot['input_identity']),sequence,snapshot['snapshot_sha256']])}
    return {**value,'admission_sha256':reader.sha(value)}


def _clock(provider, snapshot, previous):
    now, state=provider(); now=reader._number(now); state=copy.deepcopy(state)
    need(now>=previous,'publisher_clock_moved_backwards')
    reader.repair.validate_clock_state(snapshot['initial_clock_state'],now)
    reader.repair.validate_clock_state(snapshot['completion_clock_state'],now)
    reader.repair.validate_clock_state(state,now)
    return now,state


def initialize(path, profile, deadline):
    # A partially initialized existing file is refused, never repaired/adopted.
    if path.exists():return
    with path.open('xb') as file:file.flush();os.fsync(file.fileno())
    with sqlite3.connect(path,timeout=2) as con:
        con.execute('PRAGMA synchronous=FULL')
        con.executescript('BEGIN IMMEDIATE;'+SQL)
        con.execute('INSERT INTO profile VALUES(1,?)',(raw(profile).decode(),))
        need(time.monotonic()<=deadline,'operation_time_bound')
        con.commit()


def _read(con, expected_profile, deadline=None, base=None):
    deadline = time.monotonic() + MAX_SECONDS if deadline is None else deadline
    validator = compact.StreamValidator(deadline)
    frames = {}; members = []; records = []; retained = []; envelope_hashes = []; evidence_sequences = {}
    retained_heads = (); retained_hashes = ()
    schema_check(con);bounds(con)
    rows=con.execute('SELECT singleton,body FROM profile').fetchall()
    need(len(rows)==1 and rows[0][0]==1,'exact_profile_required')
    profile=checked_json(rows[0][1])
    need(exact(profile,expected_profile),'store_profile_changed')
    if base is not None:
        compact.manifest(base)
        need(exact(base['profile'], profile), 'cached_profile_changed')
        frames = compact.evidence_frames(base)
        # A cached semantic result is usable only after rereading every retained
        # original record and compressed object from THIS SQLite snapshot.
        # Byte equality, identity and exact schema replace no cryptographic check.
        for key, value in frames.items():
            saved = con.execute('SELECT expanded_bytes,packed_sha,payload FROM evidence_objects WHERE object_sha=?',(key,)).fetchone()
            need(saved is not None and tuple(saved) == (value[0],hashlib.sha256(value[2]).hexdigest(),value[2]),
                 'verified_prefix_cas_changed')
        retained = base['record_proofs']
        retained_heads = base['publication_heads']; retained_hashes = base['envelope_sha256']
        members = list(compact.complete_member_records(base))
        evidence_sequences = base['evidence_sequences']
        validator = compact.StreamValidator.continue_after(base, deadline)
    batches=[]; publication_heads=[]; previous={'sequence':0,'publication_sha':None,'checkpoint':None}
    for row in con.execute('SELECT * FROM published ORDER BY sequence'):
        if row['sequence'] <= len(retained):
            record = retained[row['sequence']-1]
            attempt = con.execute('SELECT * FROM attempts WHERE attempt_id=?',(row['attempt_id'],)).fetchone()
            ack = con.execute('SELECT * FROM acknowledgments WHERE attempt_id=?',(row['attempt_id'],)).fetchone()
            need(attempt is not None and ack is not None and record ==
                 {'attempt':dict(attempt),'ack':dict(ack),'publication':dict(row)}, 'verified_prefix_record_changed')
            # The descriptor is immutable bytes from the already checked prefix.
            batches.append(compact.batch_descriptor(base, row['sequence']-1))
            envelope_hashes.append(retained_hashes[row['sequence']-1])
            previous = retained_heads[row['sequence']-1]
            publication_heads.append(previous); records.append(record)
            continue
        publication=checked_json(row['body']); seq=row['sequence']
        reader._integer(seq,'published_sequence_integer',1)
        need(seq==previous['sequence']+1 and publication['batch_sequence']==seq
            and type(publication['batch_sequence']) is int,'complete_published_prefix_required')
        need(digest(publication)==row['publication_sha'] and publication['schema_version']==SCHEMA
            and publication['attempt_id']==row['attempt_id'] and exact(publication['previous_head'],previous), 'publication_binding_invalid')
        attempt=con.execute('SELECT * FROM attempts WHERE attempt_id=?',(row['attempt_id'],)).fetchone()
        ackrow=con.execute('SELECT * FROM acknowledgments WHERE attempt_id=?',(row['attempt_id'],)).fetchone()
        need(attempt is not None and ackrow is not None,'published_attempt_and_ack_required')
        receipt=unpack_receipt(con, checked_json(attempt['body']), frames); ack=checked_json(ackrow['body'])
        need(digest(receipt)==attempt['receipt_sha']==publication['receipt_sha256'] and
            digest(ack)==ackrow['ack_sha']==publication['ack_sha256'],'receipt_or_ack_digest_invalid')
        need(receipt['attempt_id']==ack['attempt_id']==row['attempt_id'] and
            receipt['schema_version']==ack['schema_version']==SCHEMA and
            receipt['profile_sha256']==ack['profile_sha256']==digest(profile) and
            ack['receipt_sha256']==attempt['receipt_sha'] and exact(receipt['previous_head'],previous), 'receipt_or_ack_binding_invalid')
        snapshot=receipt['snapshot']
        need(snapshot['snapshot_sha256']==attempt['snapshot_sha'] and exact(snapshot['policy'],profile['policy'])
            and exact(snapshot['input_identity'],profile['input_identity']) and
            exact(snapshot['previous_checkpoint'],previous['checkpoint']) and
            exact(snapshot['next_checkpoint'],publication['next_checkpoint']),'exact_snapshot_stream_required')
        admitted=reader._number(receipt['admitted_epoch']); visible=reader._number(ack['receipt_observed_epoch'])
        available=reader._number(publication['ack_observed_epoch'])
        need(snapshot['read_completed_epoch']<=admitted<=visible<=available,'durable_clock_order_invalid')
        for state,at in ((receipt['clock_state'],admitted),(ack['clock_state'],visible),(publication['clock_state'],available)):
            reader.repair.validate_clock_state(state,at)
        for state in (snapshot['initial_clock_state'],snapshot['completion_clock_state'],receipt['clock_state']):
            reader.repair.validate_clock_state(state,available)
        envelope={'snapshot':snapshot,'admission':_admission(snapshot,seq,admitted,available,receipt['clock_state'])}
        need(exact(publication['admission'],envelope['admission']),'published_admission_binding_invalid')
        members.extend(validator.add(envelope))
        batches.append(compact.compact_batch(envelope))
        envelope_hashes.append(reader.sha(envelope))
        for evidence in snapshot['entries']:
            key = reader.sha(evidence)
            need(key not in evidence_sequences, 'duplicate_publication_evidence')
            evidence_sequences[key] = seq
        records.append({'attempt':dict(attempt), 'ack':dict(ackrow), 'publication':dict(row)})
        previous={'sequence':seq,'publication_sha':row['publication_sha'],'checkpoint':publication['next_checkpoint']}
        publication_heads.append(copy.deepcopy(previous))
    need(len(records) >= len(retained), 'verified_prefix_truncated')
    metadata = {'profile':profile,'head':previous,'publication_heads':publication_heads,'validation_state':validator.state(),
        'envelope_sha256':envelope_hashes,'evidence_sequences':evidence_sequences,
        'record_proofs':records,'durable_storage_readback_proven':True,'historical_completeness_proven':False,
        'batches_usage':'integrity_only_not_model_input','consumer_observation_required':True,
        'downstream_consumability_proven':False, **reader.INERT}
    result = compact._seal_validated_publication(metadata, batches, frames, members)
    need(time.monotonic() <= deadline, 'complete_prefix_validation_time_bound')
    return result


def read_published(store_path, *, cohort_id, expected_policy, input_identity):
    """Read the complete published prefix in one snapshot; orphans stay invisible."""
    path=path_for(store_path)
    profile=profile_for(path,cohort_id,expected_policy,input_identity); ident=identity(path)
    key=(str(path).casefold(),ident,digest(profile))
    base=_CAPTURE_CACHE.get(key)
    deadline=time.monotonic()+(MAX_SECONDS if base is not None else MAX_COLD_READ_SECONDS)
    try:
        began=time.monotonic()
        def capture(capture_deadline):
            with connection(path,capture_deadline,expected_identity=ident) as con:
                return _capture_packed(con,profile,capture_deadline)
        (captured,retained,count),capture_attempts=_capture_with_retry(capture,deadline)
        capture_seconds=time.monotonic()-began
        transaction_seconds=capture_attempts['successful_transaction_seconds']
        result=_read_owned_capture(captured,profile,deadline,base)
        need(identity(path)==ident,'store_identity_changed')
        metrics={'transaction_seconds':transaction_seconds,'packed_retained_bytes':retained,'row_count':count,
                 'validation_seconds':time.monotonic()-began-capture_seconds,'cold':base is None,
                 'capture_attempts':capture_attempts,
                 'real_transaction_closed_before_semantic_validation':True}
        _CAPTURE_METRICS[str(path).casefold()]=metrics
        while len(_CAPTURE_METRICS)>MAX_CACHED_STORES:_CAPTURE_METRICS.popitem(last=False)
    except CaptureReadTimeout:
        # This call returns no current handle. Retaining the old opaque prefix
        # avoids a cold rebuild after scheduler contention; the next read must
        # still compare every old exact row against a fresh actual snapshot.
        raise
    except BaseException:
        _CAPTURE_CACHE.pop(key,None)
        raise
    _CAPTURE_CACHE[key]=result;_CAPTURE_CACHE.move_to_end(key)
    while len(_CAPTURE_CACHE)>MAX_CACHED_STORES:_CAPTURE_CACHE.popitem(last=False)
    policy_current(expected_policy)
    return result


def publish_snapshot(store_path, snapshot, *, cohort_id, clock_provider, fault_hook=None):
    """Only this invocation can publish its fresh attempt; no orphan recovery API.

    fault_hook is an optional trusted test/observability hook, never persisted.
    A new capture is mandatory after a failed attempt (snapshot digest is unique).
    """
    snapshot=copy.deepcopy(snapshot);raw(snapshot)
    path=path_for(store_path,missing=True); deadline=time.monotonic()+MAX_SECONDS
    profile=profile_for(path,cohort_id,snapshot['policy'],snapshot['input_identity'])
    need(str(path).lower()!=str(snapshot['input_identity']['path']).lower(),'input_store_must_differ')
    now,state=_clock(clock_provider,snapshot,snapshot['read_completed_epoch'])
    # Validate all snapshot evidence before the first write. A synthetic singleton
    # can have a nonzero after cursor, so exact prefix validation follows below.
    reader._validate_snapshot_shape(snapshot)
    need(snapshot['snapshot_sha256']==reader.sha({k:v for k,v in snapshot.items() if k!='snapshot_sha256'}),'snapshot_digest_invalid')
    need(snapshot['schema_version']==reader.SCHEMA and snapshot['admitted'] is False
        and snapshot['complete_declared_prefix'] is True,'unadmitted_exact_snapshot_required')
    need(snapshot['row_count']==len(snapshot['entries']) and snapshot['evidence_bytes']==sum(len(reader.encoded(e)) for e in snapshot['entries']),'snapshot_counts_invalid')
    for evidence in snapshot['entries']:reader.validate_projection(evidence,snapshot['policy'],snapshot['read_started_epoch'])
    if not path.exists():
        need(snapshot['previous_checkpoint'] is None and snapshot['after_seq']==0,'new_store_initial_prefix_required')
        reader.latest_admitted_as_of([{'snapshot':snapshot,'admission':_admission(snapshot,1,now,now,state)}],now)
    initialize(path,profile,deadline); ident=identity(path)
    base=read_published(path,cohort_id=cohort_id,expected_policy=snapshot['policy'],input_identity=snapshot['input_identity'])
    need(exact(snapshot['previous_checkpoint'],base['head']['checkpoint']),'fresh_capture_from_published_cursor_required')
    seq=base['head']['sequence']+1
    if not snapshot['entries']:
        policy_current(snapshot['policy'])
        return {'status':'empty_capture_profile_read_back','checkpoint':base['head']['checkpoint'],
            'publication_sequence':base['head']['sequence'],'created_members':0,**reader.INERT}
    provisional={'snapshot':snapshot,'admission':_admission(snapshot,seq,now,now,state)}
    compact.validate_extension(base, provisional, deadline)
    attempt_id=uuid.uuid4().hex
    receipt={'schema_version':SCHEMA,'attempt_id':attempt_id,'profile_sha256':digest(profile),
        'previous_head':base['head'],'snapshot':snapshot,'admitted_epoch':now,'clock_state':state}
    receipt_sha=digest(receipt)
    packed_receipt, new_frames = pack_receipt(receipt)
    receipt_raw=raw(packed_receipt)
    def hook(phase):
        if fault_hook is not None:fault_hook(phase)
    with connection(path,deadline,write=True,expected_identity=ident) as con:
        schema_check(con);need(exact(head(con),base['head']),'concurrent_publication_conflict')
        insert_frames(con, new_frames, len(receipt_raw))
        bounds(con,len(receipt_raw),new_attempt=True)
        con.execute('INSERT INTO attempts VALUES(?,?,?,?)',(attempt_id,snapshot['snapshot_sha256'],receipt_sha,receipt_raw.decode()))
    hook('receipt_committed')
    with connection(path,deadline,expected_identity=ident) as con:
        saved=con.execute('SELECT snapshot_sha,receipt_sha,body FROM attempts WHERE attempt_id=?',(attempt_id,)).fetchone()
        need(saved is not None and tuple(saved)==(snapshot['snapshot_sha256'],receipt_sha,receipt_raw.decode()),'independent_receipt_readback_failed')
        need(digest(unpack_receipt(con, checked_json(saved['body']), {})) == receipt_sha, 'independent_cas_readback_failed')
    hook('receipt_read_back')
    visible, visible_state=_clock(clock_provider,snapshot,now)
    reader.repair.validate_clock_state(state,visible)
    ack={'schema_version':SCHEMA,'attempt_id':attempt_id,'profile_sha256':digest(profile),
        'receipt_sha256':receipt_sha,'receipt_observed_epoch':visible,'clock_state':visible_state}
    ack_raw=raw(ack);ack_sha=digest(ack)
    with connection(path,deadline,write=True,expected_identity=ident) as con:
        schema_check(con);bounds(con,len(ack_raw))
        con.execute('INSERT INTO acknowledgments VALUES(?,?,?)',(attempt_id,ack_sha,ack_raw.decode()))
    hook('ack_committed')
    with connection(path,deadline,expected_identity=ident) as con:
        saved=con.execute('SELECT ack_sha,body FROM acknowledgments WHERE attempt_id=?',(attempt_id,)).fetchone()
        need(saved is not None and tuple(saved)==(ack_sha,ack_raw.decode()),'independent_ack_readback_failed')
        saved=con.execute('SELECT receipt_sha,body FROM attempts WHERE attempt_id=?',(attempt_id,)).fetchone()
        need(saved is not None and tuple(saved)==(receipt_sha,receipt_raw.decode()),'receipt_changed_before_ack_readback')
    hook('ack_read_back')
    available,final_state=_clock(clock_provider,snapshot,visible)
    reader.repair.validate_clock_state(state,available)
    admission=_admission(snapshot,seq,now,available,state)
    compact.validate_extension(base, {'snapshot':snapshot,'admission':admission}, deadline)
    policy_current(snapshot['policy'])
    publication={'schema_version':SCHEMA,'batch_sequence':seq,'attempt_id':attempt_id,
        'previous_head':base['head'],'receipt_sha256':receipt_sha,'ack_sha256':ack_sha,
        'next_checkpoint':snapshot['next_checkpoint'],'ack_observed_epoch':available,
        'clock_state':final_state,'admission':admission}
    publication_raw=raw(publication);publication_sha=digest(publication)
    hook('before_publication')
    with connection(path,deadline,write=True,expected_identity=ident) as con:
        schema_check(con);need(exact(head(con),base['head']),'concurrent_publication_conflict')
        bounds(con,len(publication_raw))
        con.execute('INSERT INTO published VALUES(?,?,?,?)',(seq,attempt_id,publication_sha,publication_raw.decode()))
    hook('publication_committed')
    result=read_published(path,cohort_id=cohort_id,expected_policy=snapshot['policy'],input_identity=snapshot['input_identity'])
    need(time.monotonic()<=deadline,'operation_time_bound')
    need(result['head']['sequence']==seq and result['head']['publication_sha']==publication_sha,'published_cursor_readback_failed')
    policy_current(snapshot['policy'])
    key=(str(path).casefold(),ident,digest(profile))
    _CAPTURE_CACHE[key]=result;_CAPTURE_CACHE.move_to_end(key)
    while len(_CAPTURE_CACHE)>MAX_CACHED_STORES:_CAPTURE_CACHE.popitem(last=False)
    return {'status':'published_and_read_back','batch_sequence':seq,'checkpoint':snapshot['next_checkpoint'],
        'publication_sha256':publication_sha,'admitted_available_epoch':available,**reader.INERT}
