"""Read-only retained forecast/management boundary inspection; no forecasts or orders.

Capture original bytes and first-observer records, then replay the bounded
qualification with the existing tracking identity and management quote validator.
Never manufacture native curve receipts, decimal quote precision, position state,
conditional terminal forecasts or economic policy defaults.
"""
from pathlib import Path
import argparse
from collections import Counter
import hashlib
import json
import math
import sqlite3
import sys
import time
import zlib

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'trad'),str(ROOT/'stage_c_alignment_integrity_v2')]
import oanda_retained_forecast_tracking_v1 as tracking
from reference_accounting_adapter_v2 import load_reference
from publication import RunPublisher,effective_run_identity,verify_completed_run

SCHEMA='retained_management_readiness.v1'
REGISTRY='trad/config/retained_forecast_connection_20260930.json'
SOURCES=['tools/forex_retained_management_readiness.py','trad/oanda_retained_forecast_tracking_v1.py',
 'trad/oanda_current_news_mapping_v1.py','trad/oanda_news_technical_timing_v1.py',
 'trad/oanda_operational_dashboard_selection_v1.py','trad/oanda_curve_management_replay_v1.py',
 'trad/src/forex_system/research/sequential_portfolio_replay_v1.py','trad/src/forex_system/__init__.py',
 'stage_c_alignment_integrity_v2/reference_accounting_adapter_v2.py','stage_c_alignment_integrity_v2/publication.py',
 'stage_c_alignment_integrity_v2/contracts.py']
SOURCES+=['stage_c_alignment_integrity_v2/native_policy_input_v2.py',
 'trad/oanda_forecast_curve_contract_v1.py','trad/oanda_curve_management_adapter_v1.py']
MAX_BYTES=8*1024**2
FLAGS={'research_only':True,'can_place_orders':False,'can_promote':False,
       'account_access':False,'broker_access':False,'models_fitted':0,'manager_activation':False}

def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def sha(b):return hashlib.sha256(b).hexdigest()
def need(ok,reason):
    if not ok:raise ValueError(reason)
def finite(v):return type(v) in (int,float) and math.isfinite(v)
def read(path,limit=MAX_BYTES):
    need(path.is_file() and not path.is_symlink() and path.stat().st_size<=limit,'bounded_regular_file_required')
    return path.read_bytes()
def db_read(path):
    db=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
    db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON')
    deadline=time.monotonic()+20;db.set_progress_handler(lambda:int(time.monotonic()>deadline),10000)
    return db
def unpack(blob):
    need(len(blob)<=MAX_BYTES,'compressed_issue_bound')
    d=zlib.decompressobj();raw=d.decompress(blob,MAX_BYTES+1)
    need(len(raw)<=MAX_BYTES and d.eof and not d.unused_data,'issue_decompression_bound')
    return raw
def sources():return {n:sha(read(ROOT/n)) for n in SOURCES}

def terminal_options(original,entry,entries,current,now):
    """Potential same-family, different-horizon matches; never policy admission."""
    if original is None or entry.get('kind')!='legacy26_matched':return []
    result=[]
    for (name,pair),f in current.items():
        e=entries[name]
        if (pair==original['instrument'] and e.get('kind')==entry['kind'] and e.get('arm')==entry.get('arm')
            and e.get('feature_names')==entry.get('feature_names') and f['target_epoch']==original['target_epoch']
            and original['reference_epoch']<f['reference_epoch'] and original['input_hash']!=f['input_hash']
            and 0<=now-f['reference_epoch']<=180 and now<f['target_epoch']):result.append(name)
    return sorted(result)

def capture(output):
    need(not output.exists(),'capture_directory_must_be_new')
    started=time.time();regraw=read(ROOT/REGISTRY);reg=json.loads(regraw)
    need(len(reg['pairs'])==68 and len(set(reg['pairs']))==68,'exact_pair_population')
    need(1<=len(reg['connections'])<=48,'connection_population_bound')
    for n,h in reg['source_bindings'].items():
        p=(ROOT/n).resolve();need(p.is_relative_to(ROOT) and sha(read(p))==h,'current_source_binding:'+n)
    current_raw=read(ROOT/'trad/data/retained_connection_20260930/current.json');current=json.loads(current_raw)
    body={k:v for k,v in current.items() if k!='payload_sha256'}
    need(current['registry_sha256']==sha(regraw) and sha(encoded(body))==current['payload_sha256'],'current_publication_identity')
    need(0<=time.time()-current['generated_epoch']<=120,'fresh_capture_publication_required')
    t=db_read(ROOT/'trad/data/retained_connection_20260930/tracking.sqlite')
    try:
        t.execute('BEGIN')
        # One first-observed anchor per registry/connection/pair; not an account position.
        rows=t.execute('''SELECT * FROM (SELECT *,ROW_NUMBER() OVER
            (PARTITION BY connection,pair ORDER BY observed,id) AS rank
            FROM forecasts WHERE registry=?) WHERE rank=1 ORDER BY connection,pair''',(sha(regraw),)).fetchall()
        need(len(rows)<=48*68,'tracking_population_bound')
        anchors=[{k:r[k] for k in r.keys() if k!='rank'} for r in rows]
        for r in anchors:need(len(r['body'])<=65536 and (r['outcome'] is None or len(r['outcome'])<=65536),'tracking_record_bound')
    finally:t.close()
    identities={current['payload_sha256']}|{json.loads(r['body'])['publication_sha256'] for r in anchors}
    need(len(identities)<=32,'original_publication_count_bound')
    i=db_read(ROOT/'trad/data/retained_connection_20260930/issued.sqlite');issues={};total=0
    try:
        for identity in sorted(identities):
            row=i.execute('SELECT issued,payload_zlib FROM issues WHERE id=?',(identity,)).fetchone()
            need(row is not None,'original_issuance_missing')
            raw=unpack(row['payload_zlib']);v=json.loads(raw)
            need(sha(raw)==identity and v['generated_epoch']==row['issued'],'original_issuance_identity')
            issues[identity]=raw;total+=len(raw);need(total<=128*1024**2,'aggregate_issue_bound')
    finally:i.close()
    quotes_raw=read(ROOT/'trad/data/oanda_training_manager/state/practice_007_market_quotes_v1.json')
    completed=time.time();pins=sources();need(read(ROOT/REGISTRY)==regraw,'registry_changed_during_capture')
    output.mkdir(parents=True)
    files={'registry.json':regraw,'current.json':current_raw,'anchors.json':encoded(anchors),'quotes.json':quotes_raw,
           **{'issue_'+k+'.json':v for k,v in issues.items()}}
    for name,raw in files.items():(output/name).write_bytes(raw)
    manifest={'schema':SCHEMA,'capture_started_epoch':started,'capture_completed_epoch':completed,
      'registry_sha256':sha(regraw),'source_bindings':pins,'files':{k:{'sha256':sha(v),'bytes':len(v)} for k,v in files.items()},
      'scope':'actual current publication plus first observer anchors; anchor is NOT a position; original outcomes copied without rescoring',**FLAGS}
    raw=encoded(manifest);(output/'MANIFEST.json').write_bytes(raw)
    return {'status':'captured','manifest_sha256':sha(raw),'anchors':len(anchors),'original_publications':len(issues),'bytes':total}

def load_capture(path,expected):
    raw=read(path/'MANIFEST.json');need(sha(raw)==expected,'trusted_manifest_identity')
    m=json.loads(raw);need(m['schema']==SCHEMA and m['source_bindings']==sources(),'inspection_source_identity')
    need(m['registry_sha256']==m['files']['registry.json']['sha256'],'captured_registry_identity')
    need(all(m.get(k)==v for k,v in FLAGS.items()),'capture_authority')
    need(finite(m['capture_started_epoch']) and m['capture_started_epoch']<=m['capture_completed_epoch'],'capture_clocks')
    need(len(m['files'])<=36,'capture_inventory_bound');data={}
    for name,r in m['files'].items():
        need(Path(name).name==name and '/' not in name and '\\' not in name,'contained_input_name')
        raw=read(path/name);need(len(raw)==r['bytes'] and sha(raw)==r['sha256'],'input_hash:'+name);data[name]=json.loads(raw)
    need({p.name for p in path.iterdir()}==set(m['files'])|{'MANIFEST.json'},'unexpected_capture_member')
    return m,data

def inspect(m,data):
    reg=data['registry.json'];v=data['current.json'];now=m['capture_completed_epoch'];rid=m['registry_sha256']
    pairs=reg['pairs'];entries={e['id']:e for e in reg['connections']}
    need(len(pairs)==68 and len(set(pairs))==68 and 1<=len(entries)<=48 and len(entries)==len(reg['connections']),'exact_population')
    need(sha(encoded({k:x for k,x in v.items() if k!='payload_sha256'}))==v['payload_sha256'],'publication_seal')
    need(v['registry_sha256']==rid and v['connections']==reg['connections'],'publication_registry_binding')
    issues={n[6:-5]:x for n,x in data.items() if n.startswith('issue_')};issued={}
    for identity,x in issues.items():
        need(sha(encoded(x))==identity and x['registry_sha256']==rid,'issue_identity_or_registry')
        need(all(x.get(k)==value for k,value in {'research_only':True,'can_place_orders':False,'can_promote':False,'models_fitted':0}.items()),'issue_authority')
        byid={}
        for f in x['forecasts']:
            key=tracking.prediction_key(f);need(key not in byid,'duplicate_issued_forecast');byid[key]=f
        issued[identity]=byid
    need(v['payload_sha256'] in issues and issues[v['payload_sha256']]=={k:x for k,x in v.items() if k!='payload_sha256'},'current_issue_membership')
    anchors={}
    for a in data['anchors.json']:
        b=json.loads(a['body']);f=b['forecast'];key=(a['connection'],a['pair'])
        need(key not in anchors and key[0] in entries and key[1] in pairs,'anchor_population')
        need(tracking.prediction_key(f)==a['id'] and issued.get(b['publication_sha256'],{}).get(a['id'])==f,'anchor_original_issuance')
        need(a['registry']==rid==f['registry_sha256'] and a['connection']==f['connection'] and a['pair']==f['instrument']
             and a['reference']==f['reference_epoch'] and a['target']==f['target_epoch'] and a['horizon']==f['horizon_minutes'],'anchor_columns')
        need(a['observed']==b['first_observed_epoch'] and f['issued_epoch']<=b['publication_epoch']<=a['observed']<=now,'anchor_observation_clock')
        need(issues[b['publication_sha256']]['generated_epoch']==b['publication_epoch'],'anchor_publication_clock')
        need(a['state'] in ('pending','settled','unavailable'),'anchor_state')
        outcome=json.loads(a['outcome']) if a['outcome'] is not None else None
        need((a['state']=='pending' and outcome is None) or (isinstance(outcome,dict) and outcome['status']==a['state']),'anchor_outcome_state')
        if a['state']=='settled':need(now>=a['target'],'premature_settlement')
        anchors[key]=(a,f,outcome)
    coverage={(c['connection'],c['instrument']):c for c in v['coverage']}
    need(len(coverage)==len(v['coverage'])==len(entries)*68 and set(coverage)=={(e,p) for e in entries for p in pairs},'complete_coverage_required')
    current={}
    for f in v['forecasts']:
        k=f['connection'],f['instrument'];need(k in coverage and k not in current,'current_forecast_population')
        # Reuse the original observer's actual admission contract at issuance,
        # then evaluate freshness separately at this later inspection clock.
        need(finite(v['generated_epoch']) and f['issued_epoch']<=v['generated_epoch']<=now,'publication_causal_order')
        tracking.qualified_rows({'status':'current','generated_epoch':f['issued_epoch'],'registry_sha256':rid,'connections':reg['connections'],'forecasts':[f]},f['issued_epoch'])
        need(f['original_model_id']==entries[k[0]].get('original_model_id'),'original_model_identity')
        current[k]=f
    import native_policy_input_v2 as native
    try:native.admit_frame({'native_input_tier':v['schema']},{},trad_root=ROOT/'trad')
    except ValueError as ex:
        need(str(ex)=='native_synthetic_qualification_tier_required','unexpected_native_gate_failure');native_reason=str(ex)
    else:raise ValueError('retained_tier_unexpectedly_admitted')
    ref=load_reference(ROOT/'trad');rawquotes=data['quotes.json'].get('quotes',{});quote_status={}
    for pair in pairs:
        # Supply original fields as-is. Missing clocks/IDs and float endpoints
        # must fail the existing validator, not be silently fabricated/coerced.
        raw=rawquotes.get(pair);q={**raw,'instrument':pair} if isinstance(raw,dict) else raw
        try:ref.quote_at({pair:q},pair,now,60);reason='compatible'
        except (ValueError,TypeError,KeyError) as ex:reason=str(ex).split(':')[0]
        quote_status[pair]={'manager_quote_status':reason,'bid_representation':type(raw.get('bid')).__name__ if isinstance(raw,dict) else 'missing',
            'ask_representation':type(raw.get('ask')).__name__ if isinstance(raw,dict) else 'missing',
            'quote_id_present':isinstance(raw,dict) and bool(raw.get('quote_id')),
            'availability_clock_present':isinstance(raw,dict) and finite(raw.get('available_epoch'))}
    rows=[]
    for e,p in sorted(coverage):
        f=current.get((e,p));old=anchors.get((e,p));a,of,outcome=old if old else (None,None,None)
        fresh=bool(f and 0<=now-v['generated_epoch']<=120 and 0<=now-f['reference_epoch']<=180 and now<f['target_epoch'])
        row={'connection':e,'instrument':p,'horizon_minutes':entries[e]['horizon_minutes'],'coverage_status':coverage[e,p]['status'],
          'saved_forecast_present':f is not None,'fresh_at_inspection':fresh,'forecast_id':tracking.prediction_key(f) if f else None,
          'first_observed_forecast_id':a['id'] if a else None,'anchor_state':a['state'] if a else 'not_observed',
          'original_target_epoch':of['target_epoch'] if of else None,'current_target_epoch':f['target_epoch'] if f else None,
          'same_original_target':bool(f and of and f['target_epoch']==of['target_epoch']),
          'changed_input':bool(f and of and f['input_hash']!=of['input_hash']),
          'potential_same_family_terminal_matches':terminal_options(of,entries[e],entries,current,now),
          'outcome_sha256':sha(encoded(outcome)) if outcome else None,**quote_status[p],
          'native_curve_receipt_status':'not_provided_by_retained_schema','conditional_original_target_update':'not_qualified',
          'position_state_status':'not_observed_no_account_access','cost_and_conversion_policy':'not_bound',
          'management_eligible':False,'scope':'first-observed forecast anchor only; no inferred position, fill, cash or PnL',**FLAGS}
        row['row_id']=sha(encoded(row));rows.append(row)
    report={'schema':SCHEMA,'registry_sha256':rid,'population':len(rows),'connections':len(entries),'pairs':68,
      'saved_forecasts':sum(r['saved_forecast_present'] for r in rows),'fresh_forecasts':sum(r['fresh_at_inspection'] for r in rows),
      'first_observed_anchors':len(anchors),'anchor_states':dict(Counter(r['anchor_state'] for r in rows)),
      'same_target_changed_input':sum(r['same_original_target'] and r['changed_input'] for r in rows),
      'potential_cross_horizon_same_terminal_anchors':sum(bool(r['potential_same_family_terminal_matches']) for r in rows),
      'changed_target_from_anchor':sum(r['original_target_epoch'] is not None and r['current_target_epoch'] is not None and not r['same_original_target'] for r in rows),
      'quote_status_by_pair':dict(Counter(r['manager_quote_status'] for r in quote_status.values())),
      'management_eligible':0,'existing_native_policy_gate':native_reason,'forecast_evidence_status':'unchanged','policy_evidence_status':'not_qualified',
      'demo_authorization_status':'not_granted','scope':'Complete retained management input qualification; no new performance evaluation or management activation',**FLAGS}
    return rows,report

def run(path,expected,output,run_id,*,resume=False,max_new=None):
    m,data=load_capture(path,expected);rows,report=inspect(m,data)
    chunks=[rows[i:i+64] for i in range(0,len(rows),64)]
    required={f'rows_{i:04d}.json' for i in range(len(chunks))}|{'REPORT.json'}
    identity=effective_run_identity(contract={'schema':SCHEMA,'population':len(rows),'capture_manifest':expected,
      'rows_per_payload':64,'required_payloads':sorted(required)},dependency_hashes=m['source_bindings'])
    pub=RunPublisher(output,run_id,identity)
    if (pub.root/'COMPLETION_MANIFEST.json').exists():
        verify_completed_run(pub.root,identity);return {'status':'verified_completed','identity':identity['fingerprint']}
    pub.acquire(recover=resume);payloads=[];added=0;completed=False
    try:
        for index,chunk in enumerate(chunks):
            name=f'rows_{index:04d}.json';raw=encoded(chunk)
            if pub.read_verified_payload(name) is None:
                if max_new is not None and added>=max_new:return {'status':'checkpointed','identity':identity['fingerprint'],'next_row':index*64}
                added+=1
            payloads.append(pub.write_or_validate_payload(name,raw))
        payloads.append(pub.write_or_validate_payload('REPORT.json',encoded(report)))
        pub.complete(payloads,required);completed=True
    finally:
        if not completed:pub.release()
    return {'status':'completed','identity':identity['fingerprint'],**report}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='action',required=True)
    a=sub.add_parser('capture');a.add_argument('--output',type=Path,required=True)
    a=sub.add_parser('replay');a.add_argument('--input',type=Path,required=True);a.add_argument('--sha256',required=True)
    a.add_argument('--output',type=Path,required=True);a.add_argument('--run-id',default='readiness');a.add_argument('--resume',action='store_true');a.add_argument('--max-new',type=int)
    a=p.parse_args()
    if a.action=='capture':result=capture(a.output)
    else:
        need(a.max_new is None or a.max_new>0,'positive_chunk_size');result=run(a.input,a.sha256,a.output,a.run_id,resume=a.resume,max_new=a.max_new)
    print(json.dumps(result))
