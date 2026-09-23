"""Explicit passive news-to-governed-history routing; no default activation.

Reuse the existing append-only source registry, configured contracts and bounded
committed-row fast lane. An old governance database is never imported or opened
for writing. Current guarded news and this historical training transport have
separate readiness. This is not a trading or forecasting process.
"""
from __future__ import annotations
import argparse
from contextlib import closing
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time

import oanda_source_governance as governance
import oanda_source_governance_news_fast_lane as fast
import oanda_local_news_sentiment_repair_v2 as repair
import oanda_joint_price_news_forecast_study_v5 as worker
import oanda_causal_forecast_inputs_joint_news_v3 as joint

PROFILE_ID='isolated_news_history_v1_20260913'
PROFILE_TABLE='isolated_news_history_profile_v1'
CONFIG_LIMIT=4*1024*1024
INERT={'research_only':True,'can_place_orders':False,'can_authorize':False,'can_promote':False,'account_eligible':False,'proof_eligible':False}

def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()

def paths_for(data_root):
    root=worker.plain_c_path(data_root)
    result={'data_root':root,'news_database':root/'local_news_sentiment/local_news_sentiment_v1.sqlite',
        'database_path':root/'state/source_governance_v1.sqlite',
        'state_path':root/'state/source_governance_news_fast_lane_v3.json',
        'profile_state_path':root/'state/isolated_news_history_v1.json',
        'clock_path':root/'state/clock_integrity_v1.json'}
    return {name:worker.plain_c_path(path) for name,path in result.items()}

def read_config(config_path,expected_sha256):
    path=worker.plain_c_path(config_path)
    if not isinstance(expected_sha256,str) or re.fullmatch('[0-9a-f]{64}',expected_sha256) is None:raise ValueError('literal_config_sha256_required')
    raw=worker.bounded_bytes(path,CONFIG_LIMIT)
    if sha(raw)!=expected_sha256:raise ValueError('history_config_hash_mismatch')
    value=json.loads(raw);sources=governance.configured_sources(value)
    if not 1<=len(sources)<=512:raise ValueError('bounded_history_source_registry_required')
    ids=[s['source_id'] for s in sources]
    if any(not isinstance(s,str) or not s or len(s)>256 for s in ids) or len(set(ids))!=len(ids):raise ValueError('unique_history_source_ids_required')
    return path,sources

def source_bindings():
    result={Path(__file__).name:sha(Path(__file__).read_bytes())}
    for module in (governance,fast,repair,worker,joint):
        path=worker.plain_c_path(module.__file__);result[path.name]=sha(path.read_bytes())
    result.update(repair.source_bindings())
    return result

def policy_for(paths,config_path,config_sha256,sources):
    contracts={row['source_id']:governance.contract_for(row) for row in sources}
    return {'schema_version':PROFILE_ID,'paths':{name:str(path) for name,path in paths.items()},
        'config_path':str(config_path),'config_sha256':config_sha256,'source_bindings':source_bindings(),
        'configured_contracts_sha256':sha(encoded(contracts)),'configured_source_ids':sorted(contracts),
        'history_contract_id':fast.CONTRACT_ID,'history_cohort_id':fast.COHORT_ID,
        'history_scope':'new isolated committed article rows; no old governance database imported',
        'transport_activation_scope':'existing v3 transport contract date is not this profile activation',
        'revision_scope':'initial committed article-row versions only; in-place later revisions require separate reconciliation',
        'history_admission_contract':joint.HISTORY_ADMISSION_SCHEMA,
        'failed_batch_readmission_allowed':False,'original_story_and_classification_clocks_preserved':True,'old_history_imported':False,**INERT}

def profile_readback(database,policy):
    """Read-only refusal before any existing registry is opened for writing."""
    database=worker.plain_c_path(database)
    with closing(sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=5)) as connection:
        connection.execute('PRAGMA query_only=ON')
        tables={row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if PROFILE_TABLE not in tables:raise ValueError('unrelated_or_partial_history_registry_refused')
        row=connection.execute(f'SELECT policy_sha256,policy_json,activated_epoch FROM {PROFILE_TABLE} WHERE id=1').fetchone()
        if row is None or row[:2]!=(sha(encoded(policy)),encoded(policy).decode()):raise ValueError('immutable_history_profile_mismatch')
        if type(row[2]) not in (int,float) or not 0<row[2]:raise ValueError('history_activation_clock_required')
        if not {'isolated_news_history_admissions_v1','isolated_news_history_admission_visibility_v1'}<=tables:raise ValueError('incomplete_history_admission_initialization')
        if sha(encoded(fast.latest_contracts(connection)))!=policy['configured_contracts_sha256']:raise ValueError('incomplete_history_contract_initialization')
        return float(row[2])

def initialize_if_absent(database,policy,sources,observed):
    database=worker.plain_c_path(database)
    if database.exists():return profile_readback(database,policy)
    database.parent.mkdir(parents=True,exist_ok=True)
    with database.open('xb'):pass
    # A crash/failure leaves a retained partial file that readback refuses.
    connection=governance.connect_registry(database)
    try:
        connection.executescript(f'''CREATE TABLE {PROFILE_TABLE}(
            id INTEGER PRIMARY KEY CHECK(id=1),policy_sha256 TEXT NOT NULL,policy_json TEXT NOT NULL,activated_epoch REAL NOT NULL);
            CREATE TRIGGER {PROFILE_TABLE}_no_update BEFORE UPDATE ON {PROFILE_TABLE} BEGIN SELECT RAISE(ABORT,'immutable_history_profile'); END;
            CREATE TRIGGER {PROFILE_TABLE}_no_delete BEFORE DELETE ON {PROFILE_TABLE} BEGIN SELECT RAISE(ABORT,'immutable_history_profile'); END;''')
        joint._ensure_history_admission_schema(connection)
        governance.insert_contracts(connection,sources)
        # Fail before the rowid mapper can skip sources due to missing contracts.
        found=fast.latest_contracts(connection)
        if set(found)!=set(policy['configured_source_ids']):raise ValueError('complete_initial_source_contracts_required')
        # Mark initialization complete only after all tables and contracts exist.
        connection.execute(f'INSERT INTO {PROFILE_TABLE} VALUES(1,?,?,?)',(sha(encoded(policy)),encoded(policy).decode(),observed))
        connection.commit()
    finally:connection.close()
    return profile_readback(database,policy)

def preflight_pending_sources(paths,policy,observed):
    """Bounded read-only source coverage check before mapper/writer admission.

    The configured collector uses this same fixed source universe. This check
    catches already-present unexpected sources without advancing their rowid;
    it is not a claim of an adversarially atomic cross-database snapshot.
    """
    checkpoint={}
    if paths['database_path'].exists():
        profile_readback(paths['database_path'],policy)
        with closing(sqlite3.connect(paths['database_path'].as_uri()+'?mode=ro',uri=True,timeout=5)) as connection:
            connection.execute('PRAGMA query_only=ON')
            tables={row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'news_fast_lane_batches_v3' in tables:checkpoint=fast._latest_batch(connection)
    batch=fast._read_input_batch(paths['news_database'],checkpoint,dt.datetime.fromtimestamp(observed,dt.timezone.utc))
    unexpected=sorted({str(row.get('source_id') or '') for row in batch['rows']}-set(policy['configured_source_ids']))
    if unexpected:raise ValueError('unconfigured_pending_news_sources_refused')
    return int(checkpoint.get('batch_seq') or 0)


def run_once(*,data_root,config_path,config_sha256,clock=time.time,mapper=None):
    paths=paths_for(data_root)
    config,sources=read_config(config_path,config_sha256)
    policy=policy_for(paths,config,config_sha256,sources)
    observed=worker.number(clock())
    clock_state=json.loads(worker.bounded_bytes(paths['clock_path'],128*1024))
    clock_proof=repair.validate_clock_state(clock_state,observed)
    if not paths['news_database'].is_file():raise ValueError('isolated_news_input_missing')
    previous_sequence=preflight_pending_sources(paths,policy,observed)
    # All writes below require the current attested clock and exact input policy.
    activated=initialize_if_absent(paths['database_path'],policy,sources,observed)
    if observed<activated:raise ValueError('history_clock_before_activation')
    result=(mapper or fast.run)(news_database=paths['news_database'],database_path=paths['database_path'],state_path=paths['state_path'])
    if read_config(config,config_sha256)[1]!=sources or policy_for(paths,config,config_sha256,sources)!=policy:
        raise ValueError('history_source_or_config_changed_during_mapping')
    profile_readback(paths['database_path'],policy)
    completed=worker.number(clock())
    if completed<observed:raise ValueError('history_completion_clock_moved_backwards')
    # The original proof must still be fresh; a new proof never rescues an
    # overlong mapping run. Also re-read current integrity to catch a newly
    # reported discontinuity or loss of clock alignment before success output.
    original_proof_at_completion=repair.validate_clock_state(clock_state,completed)
    latest_clock_state=json.loads(worker.bounded_bytes(paths['clock_path'],128*1024))
    completion_clock_proof=repair.validate_clock_state(latest_clock_state,completed)
    admission=admit_new_batch(paths,policy,result,previous_sequence,observed,completed,clock_state,latest_clock_state,clock)
    untrusted=result.get('rejected_untrusted_count',0)
    preactivation=result.get('rejected_preactivation_count',0)
    if result.get('status')!='ok' or result.get('rejected_unknown_source_count',0):status='history_transport_requires_review'
    elif untrusted or preactivation:status='history_transport_partial' if result.get('total_receipt_count',0) else 'history_transport_rejected_only'
    elif not result.get('total_receipt_count',0):status='history_transport_observed_empty'
    else:status='history_transport_observed'
    state={'schema_version':PROFILE_ID,'policy_sha256':sha(encoded(policy)),'activated_epoch':activated,
        'observed_epoch':observed,'completed_epoch':completed,'clock_proof':clock_proof,
        'original_clock_rechecked_at_completion':original_proof_at_completion,'completion_clock_proof':completion_clock_proof,
        'source_bindings':policy['source_bindings'],
        'input_news_database':str(paths['news_database']),'output_history_database':str(paths['database_path']),
        'mapping_status':result.get('status'),'batch_admission':admission,'new_mappings':result.get('new_receipt_count',0),
        'total_mappings':result.get('total_receipt_count',0),'unknown_sources':result.get('rejected_unknown_source_count',0),
        'untrusted_rows':untrusted,'preactivation_rows':preactivation,'mapping_visible_utc':result.get('mapping_visible_utc'),
        'joint_training_ready':False,'forecast_or_accuracy_claimed':False,'old_history_imported':False,
        'status':status,
        'revision_scope':policy['revision_scope'],**INERT}
    fast.atomic_json(paths['profile_state_path'],state)
    return state

def admit_new_batch(paths,policy,result,previous_sequence,observed,completed,original_state,completion_state,clock):
    """Admit only this call's exact new batch. Earlier failed commits stay excluded."""
    if type(previous_sequence) is not int or previous_sequence<0:raise ValueError('history_previous_sequence_integer_required')
    if result.get('status')=='ok':
        for key in ('committed_batch_seq','scan_cursor_rowid','next_scan_cursor_rowid','committed_input_high_watermark','input_row_count','total_receipt_count','new_receipt_count','rejected_unknown_source_count','rejected_untrusted_count','rejected_preactivation_count'):
            if type(result.get(key)) is not int or result[key]<0:raise ValueError('history_result_integer_required:'+key)
    sequence=result.get('committed_batch_seq')
    if result.get('status')!='ok' or result.get('rejected_unknown_source_count',0):
        return {'status':'not_admitted_mapper_not_ok'}
    if type(sequence) is not int or sequence<=previous_sequence:
        return {'status':'no_new_batch_no_prior_readmission'}
    if sequence!=previous_sequence+1:raise ValueError('history_exact_next_batch_required')
    database=worker.plain_c_path(paths['database_path'])
    with closing(sqlite3.connect(database,timeout=5)) as con:
        con.row_factory=sqlite3.Row;con.execute('BEGIN IMMEDIATE')
        capture=joint._history_batch_capture(con,sequence);batch=capture['batch'];visibility=capture['visibility']
        if (batch['input_identity']!=result.get('input_identity') or batch['input_rowid']!=result.get('next_scan_cursor_rowid')
            or batch['input_anchor_event_id']!=result.get('input_anchor_event_id') or batch['scan_started_utc']!=result.get('scan_started_utc')
            or visibility['mapping_visible_utc']!=result.get('mapping_visible_utc')
            or not observed<=joint._epoch(batch['scan_started_utc'])<=joint._epoch(visibility['mapping_visible_utc'])<=completed):
            raise ValueError('history_result_exact_batch_binding')
        # Sample after the bounded independent batch read. This marks admission,
        # preserving the mapper's earlier clocks, and must retain both proofs.
        admitted=worker.number(clock())
        if admitted<completed:raise ValueError('history_admission_clock_moved_backwards')
        repair.validate_clock_state(original_state,admitted)
        current_state=json.loads(worker.bounded_bytes(paths['clock_path'],128*1024))
        repair.validate_clock_state(current_state,admitted)
        receipt={'schema_version':joint.HISTORY_ADMISSION_SCHEMA,'policy_sha256':sha(encoded(policy)),
            'batch_seq':sequence,'previous_sequence':previous_sequence,'capture':capture,
            'observed_epoch':observed,'completed_epoch':completed,'admitted_epoch':admitted,
            'original_clock_state':original_state,'completion_clock_state':current_state}
        raw=joint._encoded(receipt)
        if len(raw)>joint.MAX_ADMISSION_RECEIPT_BYTES:raise ValueError('history_admission_receipt_byte_bound')
        receipt_sha=joint._digest(receipt)
        con.execute('INSERT INTO isolated_news_history_admissions_v1 VALUES(?,?,?,?)',(sequence,admitted,receipt_sha,raw.decode()))
        con.commit()
    # Independently observe the durable receipt before declaring its historical
    # availability. A later failure leaves it unacknowledged and unconsumable.
    with closing(sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=5)) as con:
        con.row_factory=sqlite3.Row;con.execute('PRAGMA query_only=ON');con.execute('BEGIN')
        saved=con.execute('SELECT receipt_sha256,receipt_json FROM isolated_news_history_admissions_v1 WHERE batch_seq=?',(sequence,)).fetchone()
        if saved is None or tuple(saved)!=(receipt_sha,raw.decode()) or joint._history_batch_capture(con,sequence)!=capture:
            raise ValueError('committed_history_admission_readback_mismatch')
    available=worker.number(clock())
    if available<admitted:raise ValueError('history_admission_visibility_clock_moved_backwards')
    repair.validate_clock_state(original_state,available)
    visible_clock=json.loads(worker.bounded_bytes(paths['clock_path'],128*1024))
    repair.validate_clock_state(visible_clock,available)
    ack={'schema_version':joint.HISTORY_ADMISSION_SCHEMA,'batch_seq':sequence,'receipt_sha256':receipt_sha,
        'admitted_available_epoch':available,'availability_basis':'independent_committed_admission_read','clock_state':visible_clock}
    with closing(sqlite3.connect(database,timeout=5)) as con:
        con.execute('INSERT INTO isolated_news_history_admission_visibility_v1 VALUES(?,?,?,?)',
            (sequence,available,joint._digest(ack),joint._encoded(ack).decode()));con.commit()
    # A successful JSON report requires independent committed acknowledgment readback.
    with closing(sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=5)) as con:
        con.row_factory=sqlite3.Row;con.execute('PRAGMA query_only=ON');con.execute('BEGIN')
        proof=joint._history_admission(con,sequence,available,joint._history_profile(con))
    return {'status':'exact_new_batch_admitted','batch_seq':sequence,**proof}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-root',type=Path,required=True)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--config-sha256',required=True)
    parser.add_argument('--interval-sec',type=float,default=30)
    parser.add_argument('--duration-sec',type=float,default=0)
    parser.add_argument('--once',action='store_true')
    args=parser.parse_args();started=time.monotonic()
    while True:
        try:result=run_once(data_root=args.data_root,config_path=args.config,config_sha256=args.config_sha256)
        except (OSError,ValueError,sqlite3.Error) as error:
            result={'schema_version':PROFILE_ID,'status':'blocked','error_type':type(error).__name__,'reason':str(error)[:240],**INERT}
        print(json.dumps(result,sort_keys=True),flush=True)
        if args.once or args.duration_sec>0 and time.monotonic()-started>=args.duration_sec:break
        time.sleep(max(10,float(args.interval_sec)))

if __name__=='__main__':main()
