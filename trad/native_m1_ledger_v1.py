"""Append-only exact-M1 outcome tables for the new native joint ledger.

The supplied owner is the source-bound new ledger, not a public raw-admission
API. Its clock-proof methods read/validate the configured actual clock source.
The worker must handle one pair's refusal without stopping other pair ledgers.
"""
from contextlib import closing
import hashlib
import json
import sqlite3
import zlib

import native_m1_outcome_v1 as outcome

SCHEMA='native_joint_m1_outcome_store_v1_20260913'
POLICY='native_joint_m1_outcome_policy_v1_20260913'
TABLES={
    'native_capacity':'seq INTEGER PRIMARY KEY, metadata_bytes INTEGER NOT NULL, source_bytes INTEGER NOT NULL, source_blobs INTEGER NOT NULL, observations INTEGER NOT NULL, score_attempts INTEGER NOT NULL, target_records INTEGER NOT NULL',
    'native_blobs':'id TEXT PRIMARY KEY, raw_bytes INTEGER NOT NULL, compressed_sha TEXT NOT NULL, payload BLOB NOT NULL',
    'native_observations':'seq INTEGER PRIMARY KEY, sha TEXT NOT NULL UNIQUE, body TEXT NOT NULL',
    'native_admissions':'seq INTEGER PRIMARY KEY, sha TEXT NOT NULL, body TEXT NOT NULL',
    'native_visibility':'seq INTEGER PRIMARY KEY, sha TEXT NOT NULL, body TEXT NOT NULL',
    'native_targets':'id TEXT PRIMARY KEY, seq INTEGER NOT NULL, sha TEXT NOT NULL, body TEXT NOT NULL',
    'native_processed':'seq INTEGER PRIMARY KEY, sha TEXT NOT NULL, body TEXT NOT NULL',
    'native_scores':'attempt TEXT PRIMARY KEY, id TEXT NOT NULL, seq INTEGER NOT NULL, sha TEXT NOT NULL, body TEXT NOT NULL',
    'native_score_visibility':'attempt TEXT PRIMARY KEY, id TEXT NOT NULL UNIQUE, sha TEXT NOT NULL, body TEXT NOT NULL',
    'native_revisions':'id TEXT NOT NULL, seq INTEGER NOT NULL, sha TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(id,seq)',
    'native_diagnostics':'id TEXT PRIMARY KEY, body TEXT NOT NULL'}
SQL='\n'.join('CREATE TABLE IF NOT EXISTS '+name+'('+columns+');\n'+
    '\n'.join('CREATE TRIGGER IF NOT EXISTS '+name+'_no_'+action.lower()+' BEFORE '+action+' ON '+name+
              " BEGIN SELECT RAISE(ABORT,'immutable_native_outcome'); END;" for action in ('UPDATE','DELETE'))
    for name,columns in TABLES.items())
need=outcome.need
encode=outcome.encode
digest=outcome.digest


def validate_policy(policy):
    need(type(policy) is dict and set(policy)=={'schema_version','maximum_retained_source_bytes',
        'maximum_source_observations','maximum_source_blobs','maximum_metadata_bytes','maximum_score_attempts',
        'maximum_target_records','target_selection','score_recipe'},'native_policy_shape')
    need(policy['schema_version']==POLICY and policy['target_selection']=='first_actual_admitted_exact_native_close'
         and policy['score_recipe']==outcome.SCORE_RECIPE,'native_fixed_outcome_recipe')
    for key,limit in (('maximum_retained_source_bytes',1024**3),('maximum_source_observations',65536),
        ('maximum_source_blobs',65536),('maximum_metadata_bytes',256*1024**2),
        ('maximum_score_attempts',65536),('maximum_target_records',65536)):
        need(type(policy[key]) is int and 1<=policy[key]<=limit,'native_explicit_resource_policy')
    return json.loads(encode(policy))


def _insert(owner,table,columns,values,body):
    """All successful metadata writes reserve their bytes before SQLite insertion.

    Page/WAL overhead is not an exact disk quota. The bounded diagnostic reserve
    is separate so a payload-cap refusal can still leave a visible reason.
    """
    raw=encode(body,256*1024)
    _reserve(owner,metadata_bytes=len(raw),observations=int(table=='native_observations'),
             score_attempts=int(table=='native_scores'),target_records=int(table in ('native_targets','native_revisions')))
    count=len(values)+1
    owner.db.execute('INSERT INTO '+table+'('+columns+',body) VALUES('+','.join('?'*count)+')',
                     (*values,raw.decode()))


CAPACITY_FIELDS=('metadata_bytes','source_bytes','source_blobs','observations','score_attempts','target_records')
def usage(owner):
    row=owner.db.execute('SELECT '+','.join(CAPACITY_FIELDS)+' FROM native_capacity ORDER BY seq DESC LIMIT 1').fetchone()
    values=dict(zip(CAPACITY_FIELDS,tuple(row) if row else (0,)*len(CAPACITY_FIELDS),strict=True))
    need(all(type(v) is int and v>=0 for v in values.values()),'native_capacity_integer_identity')
    return values


def _reserve(owner,**deltas):
    """Constant-size immutable cumulative journal, atomic with payload writes."""
    policy=validate_policy(owner.contract['native_outcome_policy']);current=usage(owner)
    for key,value in deltas.items():
        need(key in current and type(value) is int and value>=0,'native_capacity_delta')
        current[key]+=value
    limits={'metadata_bytes':'maximum_metadata_bytes','source_bytes':'maximum_retained_source_bytes',
        'source_blobs':'maximum_source_blobs','observations':'maximum_source_observations',
        'score_attempts':'maximum_score_attempts','target_records':'maximum_target_records'}
    for key,value in current.items():need(value<=policy[limits[key]],'native_capacity:'+key)
    owner.db.execute('INSERT INTO native_capacity('+','.join(CAPACITY_FIELDS)+') VALUES(?,?,?,?,?,?)',
        tuple(current[key] for key in CAPACITY_FIELDS))


def verify_existing(con):
    """One startup census, never a full-table sum per poll or insertion."""
    schema={row[0]:row[1] for row in con.execute("SELECT name,sql FROM sqlite_master WHERE type IN ('table','trigger')")}
    for name,columns in TABLES.items():
        need(schema.get(name)=='CREATE TABLE '+name+'('+columns+')','native_existing_table_contract')
        for action in ('UPDATE','DELETE'):
            trigger=name+'_no_'+action.lower()
            expected='CREATE TRIGGER '+trigger+' BEFORE '+action+' ON '+name+" BEGIN SELECT RAISE(ABORT,'immutable_native_outcome'); END"
            need(schema.get(trigger)==expected,'native_existing_immutability_contract')
    latest=con.execute('SELECT '+','.join(CAPACITY_FIELDS)+' FROM native_capacity ORDER BY seq DESC LIMIT 1').fetchone()
    expected=[sum(con.execute('SELECT COALESCE(SUM(length(CAST(body AS BLOB))),0) FROM '+name).fetchone()[0]
        for name in TABLES if name not in ('native_capacity','native_blobs','native_diagnostics')),
        con.execute('SELECT COALESCE(SUM(length(payload)),0) FROM native_blobs').fetchone()[0],
        con.execute('SELECT COUNT(*) FROM native_blobs').fetchone()[0],
        con.execute('SELECT COUNT(*) FROM native_observations').fetchone()[0],
        con.execute('SELECT COUNT(*) FROM native_scores').fetchone()[0],
        con.execute('SELECT (SELECT COUNT(*) FROM native_targets)+(SELECT COUNT(*) FROM native_revisions)').fetchone()[0]]
    need((list(latest) if latest else [0]*6)==expected,'native_capacity_startup_census')


def _read(owner,sql,args=()):
    path=outcome.plain_path(owner.path)
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as con:
        con.row_factory=sqlite3.Row;con.execute('PRAGMA query_only=ON')
        return list(con.execute(sql,args))


def _proof(owner,clock_path,previous=0,prior=()):
    proof=owner.get_native_clock_proof(clock_path)
    at=outcome.native.clock(proof['observed_epoch'],'native_clock_proof')
    need(at>=previous,'native_clock_proof_regression')
    owner.validate_native_clock_proof(proof,at)
    for item in prior:owner.validate_native_clock_proof(item,at)
    encode(proof,outcome.MAX_STATE)
    return proof


def _blob(owner,source,policy):
    if source is None:return None
    raw=encode(source);key=hashlib.sha256(raw).hexdigest()
    old=owner.db.execute('SELECT raw_bytes,compressed_sha,payload FROM native_blobs WHERE id=?',(key,)).fetchone()
    if old:
        need(_unpack(old)==raw,'native_existing_blob_conflict');return key
    compressed=zlib.compress(raw,6)
    _reserve(owner,source_bytes=len(compressed),source_blobs=1)
    owner.db.execute('INSERT INTO native_blobs VALUES(?,?,?,?)',
        (key,len(raw),hashlib.sha256(compressed).hexdigest(),compressed))
    return key


def _unpack(row):
    size=row['raw_bytes'] if isinstance(row,sqlite3.Row) else row[0]
    sha=row['compressed_sha'] if isinstance(row,sqlite3.Row) else row[1]
    compressed=row['payload'] if isinstance(row,sqlite3.Row) else row[2]
    need(type(size) is int and 0<size<=outcome.MAX_DOCUMENT and type(compressed) is bytes
         and len(compressed)<=outcome.MAX_DOCUMENT+1024,'native_blob_size_bound')
    need(hashlib.sha256(compressed).hexdigest()==sha,'native_compressed_blob_digest')
    decoder=zlib.decompressobj();raw=decoder.decompress(compressed,size+1)
    need(len(raw)==size and decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail,'native_bounded_blob_decode')
    return raw


def _retain(owner,capture,initial,policy):
    with owner.db:
        count=usage(owner)['observations']
        need(count<policy['maximum_source_observations'],'native_observation_capacity')
        blob=_blob(owner,capture.get('source'),policy)
        seq=owner.db.execute('SELECT COALESCE(MAX(seq),0)+1 FROM native_observations').fetchone()[0]
        meta={k:v for k,v in capture.items() if k not in ('source','row_evidence')}
        body={'schema_version':SCHEMA,'contract_sha256':owner.contract_hash,'sequence':seq,
            'source_blob_sha256':blob,'capture_metadata':meta,
            'row_evidence_sha256':None if 'row_evidence' not in capture else digest(capture['row_evidence']),
            'initial_clock_proof':initial,'observed_status':capture['status']}
        raw=encode(body,256*1024);sha=hashlib.sha256(raw).hexdigest()
        _insert(owner,'native_observations','seq,sha',(seq,sha),body)
    return seq,sha,body


def _capture_from_row(owner,row):
    body=json.loads(row['body']);need(digest(body)==row['sha'],'native_observation_digest')
    need(set(body)=={'schema_version','contract_sha256','sequence','source_blob_sha256','capture_metadata',
         'row_evidence_sha256','initial_clock_proof','observed_status'} and body['schema_version']==SCHEMA,
         'native_exact_observation_shape')
    need(type(body['sequence']) is int and body['sequence']==row['seq'] and
         body['contract_sha256']==owner.contract_hash,'native_observation_identity')
    key=body['source_blob_sha256'];source=None
    if key is not None:
        blobs=_read(owner,'SELECT raw_bytes,compressed_sha,payload FROM native_blobs WHERE id=?',(key,))
        need(len(blobs)==1,'native_source_blob_missing');raw=_unpack(blobs[0])
        need(hashlib.sha256(raw).hexdigest()==key,'native_source_blob_content_digest');source=json.loads(raw)
    capture={**body['capture_metadata'],'source':source}
    if body['observed_status']=='captured':
        rows=outcome.prices._source_rows(source,capture['read_completed_epoch'],capture['instrument'])
        evidence=outcome._row_evidence(source,rows)
        need(digest(evidence)==body['row_evidence_sha256'],'native_retained_row_evidence_digest')
        capture['row_evidence']=evidence;outcome.verify_capture(capture)
    else:
        need(capture.get('status')=='refused' and capture.get('capture_sha256')==
            digest({k:v for k,v in capture.items() if k!='capture_sha256'}),'native_refused_capture_digest')
    return capture,body


def diagnostic(owner,code,*,sequence=None):
    """Small separate visible failure record; no source blobs trimmed on failure."""
    body={'schema_version':SCHEMA,'contract_sha256':owner.contract_hash,
          'reason_code':str(code)[:256],'source_sequence':sequence,'can_place_orders':False}
    key=digest(body)
    with owner.db:
        if owner.db.execute('SELECT 1 FROM native_diagnostics WHERE id=?',(key,)).fetchone():return
        count=owner.db.execute('SELECT COUNT(*) FROM native_diagnostics').fetchone()[0]
        if count<256:owner.db.execute('INSERT INTO native_diagnostics VALUES(?,?)',(key,encode(body,2048).decode()))


def observe_source(owner,candle_path,*,clock_path,fault_hook=None):
    """Only a fresh actual read in this call may acquire a visibility receipt."""
    seq=None
    try:
        policy=validate_policy(owner.contract['native_outcome_policy'])
        initial=_proof(owner,clock_path)
        capture=outcome.capture_source(candle_path,owner.contract['instrument'],clock=owner.clock)
        seq,sha,body=_retain(owner,capture,initial,policy)
        if fault_hook:fault_hook('after_source_commit')
        rows=_read(owner,'SELECT seq,sha,body FROM native_observations WHERE seq=?',(seq,))
        need(len(rows)==1 and rows[0]['sha']==sha,'native_source_observation_readback')
        verified,_=_capture_from_row(owner,rows[0])
        if verified['status']!='captured':
            diagnostic(owner,verified['reason_code'],sequence=seq)
            return {'status':'refused','sequence':seq,'reason_code':verified['reason_code'],'native_targets_scored':0}
        need(initial['observed_epoch']<=verified['read_started_epoch'],'native_clock_regression_before_source_read')
        checked=_proof(owner,clock_path,verified['validated_epoch'],(initial,))
        receipt={'schema_version':SCHEMA,'sequence':seq,'observation_sha256':sha,
            'source_capture_sha256':verified['capture_sha256'],'observed_epoch':checked['observed_epoch'],
            'clock_proof':checked,'source_blob_sha256':body['source_blob_sha256']}
        receipt_sha=digest(receipt)
        with owner.db:_insert(owner,'native_admissions','seq,sha',(seq,receipt_sha),receipt)
        if fault_hook:fault_hook('after_admission_commit')
        committed=_read(owner,'SELECT sha,body FROM native_admissions WHERE seq=?',(seq,))
        need(len(committed)==1 and committed[0]['sha']==receipt_sha and
             committed[0]['body']==encode(receipt).decode(),'native_admission_independent_readback')
        visible=_proof(owner,clock_path,checked['observed_epoch'],(initial,checked))
        ack={'schema_version':SCHEMA,'sequence':seq,'admission_sha256':receipt_sha,
            'admitted_available_epoch':visible['observed_epoch'],'clock_proof':visible}
        with owner.db:_insert(owner,'native_visibility','seq,sha',(seq,digest(ack)),ack)
        if fault_hook:fault_hook('after_visibility_commit')
        return {'status':'admitted_exact_source','sequence':seq,'source_capture_sha256':verified['capture_sha256'],
                'available_epoch':visible['observed_epoch'],'native_targets_scored':0}
    except Exception as exc:
        code=type(exc).__name__+':'+str(exc)[:256]
        try:diagnostic(owner,code,sequence=seq)
        except Exception:pass
        return {'status':'refused','sequence':seq,'reason_code':code,'native_targets_scored':0}


def admitted_rows(owner, *, unprocessed=False):
    """Bounded compact metadata only; raw tails are decoded one at a time."""
    rows=_read(owner,'''SELECT o.seq,o.sha,o.body,a.sha admission_sha,a.body admission,
        v.sha visibility_sha,v.body visibility FROM native_observations o
        JOIN native_admissions a ON a.seq=o.seq JOIN native_visibility v ON v.seq=o.seq '''+
        ('WHERE NOT EXISTS(SELECT 1 FROM native_processed p WHERE p.seq=o.seq) ' if unprocessed else '')+'ORDER BY o.seq')
    need(len(rows)<=owner.contract['native_outcome_policy']['maximum_source_observations'],'native_readback_observation_bound')
    return rows


def verified_admission(owner,row):
    capture,body=_capture_from_row(owner,row);admission=json.loads(row['admission']);ack=json.loads(row['visibility'])
    need(set(admission)=={'schema_version','sequence','observation_sha256','source_capture_sha256',
         'observed_epoch','clock_proof','source_blob_sha256'} and
         set(ack)=={'schema_version','sequence','admission_sha256','admitted_available_epoch','clock_proof'},
         'native_exact_admission_shape')
    need(admission['schema_version']==ack['schema_version']==SCHEMA and type(admission['sequence']) is int
         and type(ack['sequence']) is int and admission['sequence']==ack['sequence']==row['seq'],
         'native_admitted_sequence_identity')
    need(digest(admission)==row['admission_sha'] and digest(ack)==row['visibility_sha'] and
         admission['observation_sha256']==row['sha'] and ack['admission_sha256']==row['admission_sha']
         and admission['source_capture_sha256']==capture['capture_sha256'] and
         admission['source_blob_sha256']==body['source_blob_sha256'],'native_complete_admission_binding')
    available=outcome.native.clock(ack['admitted_available_epoch'],'native_admission_available')
    checked=outcome.native.clock(admission['observed_epoch'],'native_admission_observed')
    need(capture['status']=='captured' and body['initial_clock_proof']['observed_epoch']<=capture['read_started_epoch']
         <=capture['validated_epoch']<=checked<=available,'native_admission_clock_order')
    for proof in (body['initial_clock_proof'],admission['clock_proof'],ack['clock_proof']):
        owner.validate_native_clock_proof(proof,available)
    need(admission['clock_proof']['observed_epoch']==checked and ack['clock_proof']['observed_epoch']==available,
         'native_admission_proof_time_binding')
    return {'sequence':row['seq'],'capture':capture,'available_epoch':available,
            'observation_sha256':row['sha'],'visibility_sha256':row['visibility_sha']}


def process_sources(owner,forecasts):
    """All issued forecasts, including not-yet-consumed ones, share one parse.

    A future issued forecast cannot have a mature target in an earlier source
    read: native admission requires issue < target close. Markers and selections
    commit together, so a failed batch retries without changing the first target.
    """
    for row in admitted_rows(owner,unprocessed=True):
        item=verified_admission(owner,row);capture=item['capture']
        rows=outcome.prices._source_rows(capture['source'],capture['read_completed_epoch'],capture['instrument'])
        with owner.db:
            for forecast in forecasts:
                identity=forecast['decision_id'];anchor=outcome.anchor_value(forecast['forecasts'][0]['native_anchor']).as_dict()
                selected=outcome._select_from_verified(anchor,capture,rows)
                if selected['status']!='exact_target':continue
                old=owner.db.execute('SELECT seq,sha,body FROM native_targets WHERE id=?',(identity,)).fetchone()
                if old:
                    body=json.loads(old['body']);need(digest(body)==old['sha'],'native_target_selection_digest')
                    if selected['target_mid_hex']==body['target']['target_mid_hex'] and selected['later_origin_equals_original'] is not False:
                        continue
                    revision={'schema_version':SCHEMA,'decision_id':identity,'source_sequence':item['sequence'],
                        'first_target_sha256':old['sha'],'later_target':selected,'original_score_replaced':False,
                        'original_origin_preserved':True,'source_visibility_sha256':item['visibility_sha256']}
                    if owner.db.execute('SELECT 1 FROM native_revisions WHERE id=? AND seq=?',(identity,item['sequence'])).fetchone():continue
                    count=usage(owner)['target_records']
                    need(count<owner.contract['native_outcome_policy']['maximum_target_records'],'native_target_record_capacity')
                    _insert(owner,'native_revisions','id,seq,sha',(identity,item['sequence'],digest(revision)),revision)
                else:
                    body={'schema_version':SCHEMA,'decision_id':identity,'forecast_sha256':digest(forecast),
                        'source_sequence':item['sequence'],'source_observation_sha256':item['observation_sha256'],
                        'source_visibility_sha256':item['visibility_sha256'],'source_available_epoch':item['available_epoch'],
                        'target':selected}
                    count=usage(owner)['target_records']
                    need(count<owner.contract['native_outcome_policy']['maximum_target_records'],'native_target_record_capacity')
                    _insert(owner,'native_targets','id,seq,sha',(identity,item['sequence'],digest(body)),body)
            processed={'schema_version':SCHEMA,'sequence':item['sequence'],'visibility_sha256':item['visibility_sha256'],
                'issued_forecast_set_sha256':digest([digest(f) for f in forecasts]),'issued_forecast_count':len(forecasts)}
            _insert(owner,'native_processed','seq,sha',(item['sequence'],digest(processed)),processed)
        del rows,capture,item


def settle(owner,forecasts,*,clock_path,fault_hook=None):
    """Only timely independently consumed publications are eligible to score.

    Source processing receives all issued forecasts separately. An interrupted
    score commit remains retained; a fresh score attempt never repairs its ack.
    """
    scored=0
    for forecast in forecasts:
        identity=forecast['decision_id'];anchor=forecast['forecasts'][0]['native_anchor']
        if _read(owner,'SELECT 1 FROM native_score_visibility WHERE id=?',(identity,)):continue
        targets=_read(owner,'SELECT seq,sha,body FROM native_targets WHERE id=?',(identity,))
        if not targets:continue
        target=json.loads(targets[0]['body'])
        need(digest(target)==targets[0]['sha'] and target['forecast_sha256']==digest(forecast),'native_target_forecast_binding')
        proof=_proof(owner,clock_path,target['source_available_epoch'])
        result=outcome.score_native(anchor,float.fromhex(target['target']['target_mid_hex']))
        with owner.db:
            attempts=usage(owner)['score_attempts']
            need(attempts<owner.contract['native_outcome_policy']['maximum_score_attempts'],'native_score_attempt_capacity')
            attempt=digest({'contract':owner.contract_hash,'decision_id':identity,'attempt_sequence':attempts+1})
            body={**target,'attempt_id':attempt,'target_selection_sha256':targets[0]['sha'],
                'scoring_observed_epoch':proof['observed_epoch'],'clock_proof':proof,'score':result}
            _insert(owner,'native_scores','attempt,id,seq,sha',(attempt,identity,target['source_sequence'],digest(body)),body)
        if fault_hook:fault_hook('after_score_commit')
        saved=_read(owner,'SELECT sha,body FROM native_scores WHERE attempt=?',(attempt,))
        need(len(saved)==1 and saved[0]['sha']==digest(body) and saved[0]['body']==encode(body).decode(),
             'native_score_independent_readback')
        observed=_proof(owner,clock_path,proof['observed_epoch'],(proof,))
        ack={'schema_version':SCHEMA,'decision_id':identity,'attempt_id':attempt,'score_sha256':digest(body),
            'outcome_available_epoch':observed['observed_epoch'],'clock_proof':observed}
        with owner.db:_insert(owner,'native_score_visibility','attempt,id,sha',(attempt,identity,digest(ack)),ack)
        scored+=1
    return scored


def outcome_statuses(owner,forecasts,*,as_of_epoch):
    """Complete forecast denominator; orphan score commits remain unknown."""
    now=outcome.native.clock(as_of_epoch,'native_status_as_of');records=[]
    rows=_read(owner,'''SELECT s.id,s.attempt,s.seq,s.sha,s.body,v.sha visibility_sha,v.body visibility
        FROM native_scores s JOIN native_score_visibility v ON v.attempt=s.attempt AND v.id=s.id''')
    scores={row['id']:row for row in rows}
    for forecast in forecasts:
        identity=forecast['decision_id'];anchor=outcome.anchor_value(forecast['forecasts'][0]['native_anchor']).as_dict()
        record={'decision_id':identity,'native_anchor_sha256':digest(anchor),'target_close_epoch':anchor['target_close_epoch']}
        row=scores.get(identity)
        if row:
            score=json.loads(row['body']);ack=json.loads(row['visibility'])
            need(digest(score)==row['sha'] and digest(ack)==row['visibility_sha'] and
                 ack['score_sha256']==row['sha'] and score['decision_id']==ack['decision_id']==identity and
                 score['forecast_sha256']==digest(forecast),'native_visible_score_identity')
            need(type(score['source_sequence']) is int and score['source_sequence']==row['seq'] and
                 score['attempt_id']==ack['attempt_id']==row['attempt'],'native_score_attempt_identity')
            available=outcome.native.clock(ack['outcome_available_epoch'],'native_outcome_available')
            need(score['scoring_observed_epoch']<=available,'native_score_availability_order')
            owner.validate_native_clock_proof(score['clock_proof'],available)
            owner.validate_native_clock_proof(ack['clock_proof'],available)
            need(score['clock_proof']['observed_epoch']==score['scoring_observed_epoch'] and
                 ack['clock_proof']['observed_epoch']==available,'native_score_proof_clock_binding')
            if available<=now:record.update(status='scored',outcome=score,visibility=ack)
            else:record.update(status='pending',reason_code='retained_score_not_available_at_as_of')
        elif now<anchor['target_close_epoch']:record.update(status='pending',reason_code='target_not_mature')
        else:record.update(status='unknown',reason_code='no_visible_admitted_exact_target_score')
        records.append(record)
    return records
