"""One bounded read-only live diagnosis; writes only its external evidence files."""
from collections import Counter
from datetime import datetime, timezone
from hashlib import sha256
import json, sqlite3, time, urllib.request
from pathlib import Path

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
DATA=ROOT/'data/oanda_training_manager'
FAMILY='ridge_price_news_v1'
def digest(raw):return sha256(raw).hexdigest()
def iso(epoch):return datetime.fromtimestamp(epoch,timezone.utc).isoformat()
def file_value(path):
    raw=path.read_bytes()
    return json.loads(raw),{'path':str(path),'sha256':digest(raw),'bytes':len(raw),'read_finished_epoch':time.time()}
def registry_info(name):
    value,source=file_value(ROOT/'config'/name)
    checks={path: digest((ROOT/path).read_bytes())==expected for path,expected in value['source_bindings'].items()}
    return value,{'source':source,'binding_count':len(checks),'source_bindings_match':checks,'all_source_bindings_match':all(checks.values())}
def ledger_info(path):
    began=time.time()
    if not path.is_file():return {'path':str(path),'status':'missing','observed_epoch':began}
    db=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=5)
    db.row_factory=sqlite3.Row
    try:
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        counts={name:db.execute('SELECT COUNT(*) FROM '+name).fetchone()[0] for name in
                ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')}
        attempt=db.execute('SELECT * FROM attempts ORDER BY epoch DESC LIMIT 1').fetchone()
        quote=db.execute('SELECT payload FROM quotes ORDER BY rowid DESC LIMIT 1').fetchone()
        parsed=json.loads(quote[0]) if quote else {}
        return {'path':str(path),'status':'read_only_snapshot','started_epoch':began,'finished_epoch':time.time(),
                'counts':counts,'last_attempt':dict(attempt) if attempt else None,
                'last_retained_quote':{k:parsed.get(k) for k in ('instrument','market_epoch','available_epoch','bid','ask','tradeable','quote_id')} if quote else None}
    finally:db.rollback();db.close()
def compact_row(row):
    if not row:return None
    result={k:row.get(k) for k in ('instrument','status','reason','reason_label','observed_epoch')}
    result['families']={family:{k:slot.get(k) for k in ('status','reason','current_readiness','last_attempt','counts')}
                        for family,slot in row.get('families',{}).items()}
    result['active_forecasts']=[{k:arm.get(k) for k in ('family','decision_id','reference_epoch','issued_epoch','target_epoch',
                              'predicted_return_bps','probability_up','side','forecast_sha256')}
                                for arm in row.get('active_forecasts',[])]
    return result

began=time.time()
joint_registry,joint_binding=registry_info('joint_price_news_study_v2_20260907.json')
price_registry,price_binding=registry_info('pair_local_forecast_study_v2_20260907.json')
request_started=time.time()
with urllib.request.urlopen('http://127.0.0.1:8765/api/main',timeout=20) as response:raw=response.read(8*1024*1024+1)
if len(raw)>8*1024*1024:raise ValueError('bounded_api_size')
api_observed=time.time();api=json.loads(raw)
joint=api['joint_price_news_forecasts'];price=api['pair_local_forecasts_v2']
assert joint['study_version']=='joint_v2' and joint['status']=='current'
missing=[row['instrument'] for row in joint['rows'] if not row.get('active_forecasts')]
assert len(missing)<=68 and all(pair in joint_registry['pairs'] for pair in missing)
joint_rows={row['instrument']:row for row in joint['rows']};price_rows={row['instrument']:row for row in price['rows']}
direct,summary_source=file_value(DATA/'joint_price_news_study_v2/summary.json')
hb,heartbeat_source=file_value(DATA/'joint_price_news_study_v2/heartbeat.json')
quotes,quote_source=file_value(DATA/'state/practice_007_market_quotes_v1.json')
direct_rows={row['instrument']:row for row in direct['rows']}

log=DATA/'joint_price_news_study_v2/readiness.jsonl'
with log.open('rb') as handle:
    handle.seek(0,2);size=handle.tell();offset=max(0,size-4*1024*1024);handle.seek(offset);tail=handle.read(size-offset)
if offset:tail=tail.split(b'\n',1)[1]
latest={};numeric={}
for line in tail.splitlines():
    try:event=json.loads(line)
    except json.JSONDecodeError:continue
    pair=event.get('instrument')
    if pair not in missing:continue
    latest[pair]=event
    if 'mature_exact_h1_training_rows' in (event.get('family_readiness') or {}).get(FAMILY,{}):numeric[pair]=event

rows=[]
for pair in missing:
    jq=joint_rows[pair];slot=jq['families'][FAMILY];readiness=slot.get('current_readiness') or {}
    diag=readiness.get('diagnostics') or (numeric.get(pair,{}).get('family_readiness') or {}).get(FAMILY,{})
    structural=list(diag.get('reasons') or [])
    if 'market_closed_or_no_stream_quote' in jq.get('reason',''):category='no_current_tradeable_stream_quote'
    elif any('mature_joint_h1_training_rows' in x for x in structural):category='insufficient_joint_training_labels'
    elif any('context' in x for x in structural):category='insufficient_news_context_training_support'
    elif any('unseen_in_training' in x for x in structural):category='unseen_current_vetted_feature'
    else:category='current_readiness_or_cadence_requires_individual_review'
    quote=quotes.get('quotes',{}).get(pair,{})
    quote_projection={k:quote.get(k) for k in ('instrument','source','tradeable','time','bid','ask','pip')}
    rows.append({'instrument':pair,'classification':category,'structural_reasons_at_retained_input':structural,
      'joint_api':compact_row(jq),'price_only_api':compact_row(price_rows.get(pair)),
      'direct_summary_row':compact_row(direct_rows.get(pair)),
      'latest_readiness_event':latest.get(pair),'latest_retained_numeric_readiness_event':numeric.get(pair),
      'quote_snapshot_row':quote_projection,
      'joint_ledger':ledger_info(DATA/'joint_price_news_study_v2/pairs'/pair/FAMILY/'study.sqlite'),
      'price_ledgers':{family:ledger_info(DATA/'pair_local_forecast_study_v2/pairs'/pair/family/'study.sqlite')
                       for family in ('probabilistic_state_space','ridge_return_repaired')}})

_,joint_after=registry_info('joint_price_news_study_v2_20260907.json')
_,price_after=registry_info('pair_local_forecast_study_v2_20260907.json')
assert joint_after['source']['sha256']==joint_binding['source']['sha256'] and joint_after['all_source_bindings_match']
assert price_after['source']['sha256']==price_binding['source']['sha256'] and price_after['all_source_bindings_match']
assert joint_binding['all_source_bindings_match'] and price_binding['all_source_bindings_match']
finished=time.time()
result={'schema_version':'missing_joint_forecast_audit_v1_20260907','status':'completed_read_only_audit',
 'started_epoch':began,'finished_epoch':finished,'observed_utc':iso(api_observed),
 'api':{'url':'http://127.0.0.1:8765/api/main','request_started_epoch':request_started,'response_finished_epoch':api_observed,
        'raw_response_sha256':digest(raw),'raw_response_scope':'Observed in memory; only relevant forecast projection retained, account fields omitted.',
        'joint':{k:v for k,v in joint.items() if k!='rows'},'price_only':{k:v for k,v in price.items() if k!='rows'}},
 'universe_count':len(joint['rows']),'active_joint_pair_count':len(joint['rows'])-len(missing),'missing_pairs':missing,
 'categories':dict(Counter(row['classification'] for row in rows)),'rows':rows,
 'underlying_publication':{'summary_source':summary_source,'summary_generated_epoch':direct.get('generated_epoch'),
       'heartbeat_source':heartbeat_source,'heartbeat':hb,'quote_source':quote_source,
       'heartbeat_binds_captured_summary_bytes':hb.get('summary_sha256')==digest(json.dumps(direct,sort_keys=True,separators=(',',':'),allow_nan=False).encode())},
 'readiness_log_observation':{'path':str(log),'source_size_at_read':size,'tail_offset_before_partial_line_drop':offset,
       'retained_tail_bytes':len(tail),'retained_tail_sha256':digest(tail),'scope':'Latest matching events projected above; no full log copy.'},
 'registry_source_checks':{'joint_before':joint_binding,'joint_after':joint_after,'price_before':price_binding,'price_after':price_after},
 'limits':['API, publication files, readiness log and each ledger have separate retained observation clocks; no globally atomic snapshot claim.',
           'No fitting, new input capture, broker request, publication, backfill, source or runtime mutation was performed.',
           'Threshold counts are engineering criteria in this frozen model, not a proof that every smaller-sample estimate is mathematically impossible.',
           'Price-only predictions are legitimate distinct research estimates with uncalibrated uncertainty; they must not be relabeled as fitted joint news predictions.',
           'Existing H1 forecasts cannot be reconstructed into forecasts that were actually issued earlier at other horizons.',
           'No fresh tradeable stream quote means there is no verified current executable reference; it does not prove the currency has no market elsewhere.']}
stamp=datetime.fromtimestamp(api_observed,timezone.utc).strftime('%Y%m%dT%H%M%SZ')
target=OUT/f'MISSING_JOINT_FORECASTS_{stamp}.json'
with target.open('x',encoding='utf-8') as handle:json.dump(result,handle,indent=2,allow_nan=False);handle.write('\n')
lines=[f'# Missing combined forecasts — {iso(api_observed)}','',f"The current API shows {result['active_joint_pair_count']}/{result['universe_count']} combined H1 forecasts, with {len(missing)} missing pairs. This is a dated availability check, not an accuracy assessment.",'',
       '| Pair | Combined-model blocker | Price-only H1 arms active |','|---|---|---|']
for row in rows:
    p=row['instrument'];diag=row['joint_api']['families'][FAMILY]['current_readiness'].get('diagnostics') or (row.get('latest_retained_numeric_readiness_event') or {}).get('family_readiness',{}).get(FAMILY,{})
    reason='; '.join(row['structural_reasons_at_retained_input']) or row['joint_api']['reason']
    active=', '.join(arm['family'] for arm in (row.get('price_only_api') or {}).get('active_forecasts',[])) or 'None'
    lines.append(f'| {p} | {reason} | {active} |')
lines.extend(['','The three TRY pairs have no accepted current quote in these ledgers. The other four can already be displayed as price-only research estimates with their existing uncertainty, while their combined-model availability remains separate.',
 '','The joint model requires at least 48 mature exact-H1 training rows at 15-minute anchors, 12 rows with nonzero news context, and eight distinct context patterns. These are fixed readiness thresholds; they are not trade-profit filters. Changing them requires a new prospective model version and separate evaluation. Simply weakening a label cannot supply missing observed prices, original news availability, or past issued forecasts.',
 '','The four structural blockers are not resolved by a scheduler restart or an additional 61 consecutive minutes. They need qualifying mature data/context under this model, or a separately defined partial/price-only estimate. Cadence affects the next publication after readiness succeeds; no ready-but-unattempted missing pair is established by this observation.',
 '',f'Evidence: `{target.name}`; SHA-256 `{digest(target.read_bytes())}`. All 16 joint and nine price-v2 registered source bindings matched before and after the audit. Worker health and counts retain their own clocks in the JSON.'])
notes=OUT/f'MISSING_JOINT_FORECASTS_{stamp}.md'
with notes.open('x',encoding='utf-8') as handle:handle.write('\n'.join(lines)+'\n')
print(json.dumps({'json':str(target),'sha256':digest(target.read_bytes()),'notes':str(notes),
 'missing_pairs':missing,'categories':result['categories'],'rows':[{'pair':r['instrument'],'reason':r['structural_reasons_at_retained_input'] or r['joint_api']['reason'],
 'price_arms':len((r.get('price_only_api') or {}).get('active_forecasts',[])),'joint_counts':r['joint_ledger']['counts']} for r in rows]},indent=2))
