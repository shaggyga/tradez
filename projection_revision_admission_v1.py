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

SCHEMA = 'isolated_projection_admission_v2_20260913'
MAX_DOCUMENT = 18 * 1024 * 1024
MAX_JSON_TOTAL = 128 * 1024 * 1024
MAX_DATABASE = 192 * 1024 * 1024
MAX_ATTEMPTS = 4096
MAX_SECONDS = 30
TABLES = {
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


def raw(value):
    result = reader.encoded(value)
    need(len(result) <= MAX_DOCUMENT, 'document_byte_bound')
    return result


def digest(value): return hashlib.sha256(raw(value)).hexdigest()


def exact(a, b): return raw(a) == raw(b)


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
        count, size, maximum=con.execute('SELECT COUNT(*),COALESCE(SUM(length(CAST(body AS BLOB))),0),COALESCE(MAX(length(CAST(body AS BLOB))),0) FROM '+table).fetchone()
        need(maximum<=MAX_DOCUMENT and count<=MAX_ATTEMPTS,'store_row_or_document_bound')
        if table=='attempts' and new_attempt: need(count<MAX_ATTEMPTS,'attempt_bound')
        total+=size
    need(total+extra<=MAX_JSON_TOTAL,'total_json_byte_bound')


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


def _read(con, expected_profile):
    schema_check(con);bounds(con)
    rows=con.execute('SELECT singleton,body FROM profile').fetchall()
    need(len(rows)==1 and rows[0][0]==1,'exact_profile_required')
    profile=checked_json(rows[0][1])
    need(exact(profile,expected_profile),'store_profile_changed')
    batches=[]; publication_heads=[]; previous={'sequence':0,'publication_sha':None,'checkpoint':None}
    for row in con.execute('SELECT * FROM published ORDER BY sequence'):
        publication=checked_json(row['body']); seq=row['sequence']
        reader._integer(seq,'published_sequence_integer',1)
        need(seq==previous['sequence']+1 and publication['batch_sequence']==seq
            and type(publication['batch_sequence']) is int,'complete_published_prefix_required')
        need(digest(publication)==row['publication_sha'] and publication['schema_version']==SCHEMA
            and publication['attempt_id']==row['attempt_id'] and exact(publication['previous_head'],previous), 'publication_binding_invalid')
        attempt=con.execute('SELECT * FROM attempts WHERE attempt_id=?',(row['attempt_id'],)).fetchone()
        ackrow=con.execute('SELECT * FROM acknowledgments WHERE attempt_id=?',(row['attempt_id'],)).fetchone()
        need(attempt is not None and ackrow is not None,'published_attempt_and_ack_required')
        receipt=checked_json(attempt['body']); ack=checked_json(ackrow['body'])
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
        batches.append(envelope)
        previous={'sequence':seq,'publication_sha':row['publication_sha'],'checkpoint':publication['next_checkpoint']}
        publication_heads.append(copy.deepcopy(previous))
    reader.latest_admitted_as_of(batches,0.0) # Full stream integrity, no latest-only shortcut.
    return {'profile':profile,'batches':batches,'head':previous,'publication_heads':publication_heads}


def read_published(store_path, *, cohort_id, expected_policy, input_identity):
    """Read the complete published prefix in one snapshot; orphans stay invisible."""
    path=path_for(store_path); deadline=time.monotonic()+MAX_SECONDS
    profile=profile_for(path,cohort_id,expected_policy,input_identity); ident=identity(path)
    with connection(path,deadline,expected_identity=ident) as con:result=_read(con,profile)
    policy_current(expected_policy)
    return {**result,'durable_storage_readback_proven':True,'historical_completeness_proven':False,
        'batches_usage':'integrity_only_not_model_input','consumer_observation_required':True,
        'downstream_consumability_proven':False,**reader.INERT}


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
    with connection(path,deadline,expected_identity=ident) as con:base=_read(con,profile)
    need(exact(snapshot['previous_checkpoint'],base['head']['checkpoint']),'fresh_capture_from_published_cursor_required')
    seq=base['head']['sequence']+1
    if not snapshot['entries']:
        policy_current(snapshot['policy'])
        return {'status':'empty_capture_profile_read_back','checkpoint':base['head']['checkpoint'],
            'publication_sequence':base['head']['sequence'],'created_members':0,**reader.INERT}
    provisional={'snapshot':snapshot,'admission':_admission(snapshot,seq,now,now,state)}
    reader.latest_admitted_as_of(base['batches']+[provisional],now)
    attempt_id=uuid.uuid4().hex
    receipt={'schema_version':SCHEMA,'attempt_id':attempt_id,'profile_sha256':digest(profile),
        'previous_head':base['head'],'snapshot':snapshot,'admitted_epoch':now,'clock_state':state}
    receipt_raw=raw(receipt);receipt_sha=digest(receipt)
    def hook(phase):
        if fault_hook is not None:fault_hook(phase)
    with connection(path,deadline,write=True,expected_identity=ident) as con:
        schema_check(con);need(exact(head(con),base['head']),'concurrent_publication_conflict')
        bounds(con,len(receipt_raw),new_attempt=True)
        con.execute('INSERT INTO attempts VALUES(?,?,?,?)',(attempt_id,snapshot['snapshot_sha256'],receipt_sha,receipt_raw.decode()))
    hook('receipt_committed')
    with connection(path,deadline,expected_identity=ident) as con:
        saved=con.execute('SELECT snapshot_sha,receipt_sha,body FROM attempts WHERE attempt_id=?',(attempt_id,)).fetchone()
        need(saved is not None and tuple(saved)==(snapshot['snapshot_sha256'],receipt_sha,receipt_raw.decode()),'independent_receipt_readback_failed')
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
    reader.latest_admitted_as_of(base['batches']+[{'snapshot':snapshot,'admission':admission}],available)
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
    with connection(path,deadline,expected_identity=ident) as con:
        result=_read(con,profile)
        need(result['head']['sequence']==seq and result['head']['publication_sha']==publication_sha,'published_cursor_readback_failed')
    policy_current(snapshot['policy'])
    return {'status':'published_and_read_back','batch_sequence':seq,'checkpoint':snapshot['next_checkpoint'],
        'publication_sha256':publication_sha,'admitted_available_epoch':available,**reader.INERT}
