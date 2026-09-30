"""Typed, cached currency news context. No return forecast or execution signal.

The source archive is read-only. A separate prospective store retains exactly the
text interpreted and its actual computation time; expiry never reparses the text.
"""
from collections import Counter
import argparse
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time
import uuid

import oanda_news_interpretation_v1 as previous

PREDECESSOR_SHA256 = '7338b4dee04e179661f0c2c6327b44231d0ddbec4125660cc0045156a29a5fa0'
_LOADED_BINDINGS = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in (Path(__file__), Path(previous.__file__))}
if _LOADED_BINDINGS[Path(previous.__file__).name] != PREDECESSOR_SHA256:
    raise ValueError('qualified_interpretation_predecessor_changed')

SCHEMA = 'currency_news_context_v1_20260930'
LIMIT = 500
MAX_DB = 128 * 1024**2
FLAGS = dict(research_only=True, forecast_eligible=False, can_place_orders=False,
             can_promote=False, execution_eligible=False)


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(',', ':')).encode('utf8')


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def bindings():
    actual = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
              for p in (Path(__file__), Path(previous.__file__))}
    if actual != _LOADED_BINDINGS:
        raise ValueError('loaded_context_source_changed')
    return dict(_LOADED_BINDINGS)


def headline_text(headline, publisher=''):
    text = ' '.join(str(headline).split())
    if publisher and text.casefold().endswith((' - '+publisher).casefold()):
        text = text[:-len(publisher)-3].strip()
    return text


def interpret(text):
    """Signs here describe text/policy, never predicted currency returns."""
    result = previous.interpret_headline(text)
    claims = result['claims']
    def add(kind, value, evidence, currency=None):
        claims.append(dict(kind=kind, value=value, evidence=evidence,
                           currency=currency, direction=None, status='text_context'))
    # Clauses keep the subject of a second central bank from inheriting a sign.
    for clause in re.split(r'[;!?]|\s+(?:while|whereas|but)\s+', text, flags=re.I):
        banks = list(re.finditer(previous.BANK_PATTERN, clause, re.I))
        if len(banks) == 1:
            bank = banks[0]
            currency = previous.BANKS[bank.group().lower()]
            negative = bool(re.search(r'\b(?:denies?|denied|not|no|never|unlikely|may|might|could|if|rules? out|rejects?)\b', clause, re.I))
            timing = re.search(r'\b(?:no (?:need for )?urgency|no rush|not (?:in a )?hurry)\b.{0,65}\b(?:hik\w*|rais\w*|cut\w*|lower\w*)\b', clause, re.I)
            if timing:
                target = 'tightening' if re.search(r'\b(?:hik\w*|rais\w*)\b', timing[0], re.I) else 'easing'
                negated_timing = bool(re.search(r'\b(?:denies?|denied|not true|disputes?)\b', clause, re.I))
                add('policy_timing', 'unresolved' if negated_timing else target+'_not_urgent', clause, currency)
            guidance = re.search(r'\b(?:more|further|additional)\s+(?:(?:interest|policy)\s+)?(?:rate[ -])?(hikes?|increases?|cuts?|reductions?)\b', clause, re.I)
            if guidance:
                stance = 'further_easing' if re.match(r'cut|reduc', guidance[1], re.I) else 'further_tightening'
                add('policy_guidance', 'conditional_or_negated' if negative else stance, clause, currency)
            softening = re.search(r'\b(?:tempers?|eases?|reduces?|dampens?|cools?)\b.{0,55}\b(?:rate[ -])?hike\b.{0,20}\b(?:concerns?|bets|odds|expectations?)\b', clause, re.I)
            falling_odds = re.search(r'\b(?:rate[ -])?hike\s+(?:bets|odds|expectations?)\b.{0,25}\b(?:fall\w*|drop\w*|lower|declin\w*)\b', clause, re.I)
            if softening or falling_odds:
                add('policy_expectations', 'unresolved' if negative else 'tightening_expectations_easing', clause, currency)
        if re.search(r'\b(?:oil|gas|crude)\b', clause, re.I):
            supply = re.search(r'\b(?:production|output|shipments?|exports?|supply)\s+(?:\w+\s+){0,2}(rises?|rose|rising|increases?|increased|grows?|growing|falls?|fell|declines?|drops?)\b', clause, re.I)
            if supply:
                up = re.match(r'ris|rose|increas|grow', supply[1], re.I)
                modal = re.search(r'\b(?:not|no|never|may|might|could|if|expected|forecast)\b', clause, re.I)
                add('energy_supply_quantity', 'unresolved' if modal else 'increasing' if up else 'decreasing', clause)
            price = re.search(r'\b(?:oil|crude|gas)\s+prices?\s+(rise\w*|rose|rally|rallies|increase\w*|fall\w*|fell|drop\w*|declin\w*)\b', clause, re.I)
            if price:
                modal = re.search(r'\b(?:not|no|never|may|might|could|if|expected|forecast)\b', clause, re.I)
                add('energy_price_response', 'unresolved' if modal else 'increasing' if re.match(r'ris|rose|rall|increas', price[1], re.I) else 'decreasing', clause)
        if re.search(r'\blosing grip\b.{0,65}\bblockade\b', clause, re.I):
            modal = re.search(r'\b(?:not|denies?|may|might|could|if)\b', clause, re.I)
            add('supply_constraint', 'unresolved' if modal else 'reported_easing', clause)
    # Add only explicit currency responses absent from the predecessor. A bare
    # 'dollar' is ambiguous and remains unresolved rather than silently USD.
    more_names = {'new zealand dollar':'NZD','swiss franc':'CHF','south african rand':'ZAR',
                  'mexican peso':'MXN','norwegian krone':'NOK','sterling':'GBP'}
    pattern = r'\b('+ '|'.join(more_names)+ r')\s+(firms?|firmed|strengthens?|weakens?|extends?\s+(?:(?:its|BoE)\s+)?rally)\b'
    for m in re.finditer(pattern,text,re.I):
        currency=more_names[m[1].lower()]
        if any(c.get('kind')=='reported_currency_response' and c.get('currency')==currency for c in claims):continue
        modal=re.search(r'\b(?:may|might|could|not|if)\b',text[max(0,m.start()-12):m.end()],re.I)
        claims.append(dict(kind='reported_currency_response', evidence=m[0], currency=currency,
                           direction=None if modal else -1 if m[2].lower().startswith('weaken') else 1,
                           status='conditional_or_negated' if modal else 'retrospective'))
    # Retain an explicit reaction even when 'dollar' cannot be assigned to USD.
    # This does not turn contextual guesses about a publisher into currency IDs.
    bare=re.search(r'(?:^|[,;:]\s*)dollar\s+(slides?|falls?|rises?|gains?)\b',text,re.I)
    if bare:
        add('reported_unresolved_currency_response',
            'reported_dollar_decline_currency_unspecified' if re.match(r'slid|fall',bare[1],re.I) else 'reported_dollar_rise_currency_unspecified',bare[0])
    for m in re.finditer(r'\b(softer|cooling|hotter)\s+(?:core\s+)?(?:PCE|CPI|inflation)\b',text,re.I):
        add('inflation_measure_context','reported_cooling' if m[1].lower()!='hotter' else 'reported_firming',m[0])
    if re.search(r'\bAussie lags\b',text,re.I):
        add('relative_currency_response','reported_relative_underperformance',text,'AUD')
    return {**result,'version':SCHEMA,'claims':claims,
            'interpretation_status':'claims_extracted' if claims else 'unresolved',
            'scope':'headline_only; policy direction is not currency return direction',**FLAGS}


def epoch(value):
    parsed=dt.datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('timezone_required')
    return parsed.timestamp()


def iso(value):
    return dt.datetime.fromtimestamp(value,dt.timezone.utc).isoformat()


def source_rows(path, now):
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
    db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON')
    deadline=time.monotonic()+3
    db.set_progress_handler(lambda: int(time.monotonic()>deadline),10000)
    try:
        rows=[dict(r) for r in db.execute('''SELECT event_id,headline,source_name,source_id,
            source_url,published_utc,first_seen_utc,currency_scores_json FROM articles
            WHERE relevant=1 AND published_utc>=? AND first_seen_utc<=?
            ORDER BY first_seen_utc DESC LIMIT ?''',(iso(now-86400),iso(now),LIMIT+1))]
        return rows[:LIMIT],len(rows)>LIMIT
    finally:db.close()


def collect(db_path, output, collector_heartbeat, *, clock=time.time):
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    observed=clock();rows,truncated=source_rows(db_path,observed)
    graph=bindings(); parser_id=digest(graph)
    store=output/'context.sqlite'
    if sum(p.stat().st_size for p in output.glob('context.sqlite*'))>MAX_DB:raise ValueError('context_store_capacity')
    db=sqlite3.connect(store,timeout=2)
    try:
        db.execute('CREATE TABLE IF NOT EXISTS interpretations (id TEXT PRIMARY KEY, text TEXT, parser TEXT, computed REAL, body TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS observations (id TEXT PRIMARY KEY, interpretation TEXT, observed REAL, body TEXT)')
        items=[];seen={};parsed=0
        for row in rows:
            if len(row['headline'])>2000:continue
            try:
                published=epoch(row['published_utc']);first=epoch(row['first_seen_utc'])
                if not 0<published<=first<=observed:continue
            except (ValueError,TypeError):continue
            text=headline_text(row['headline'],row['source_name'])
            key=digest({'text':text.casefold(),'parser':parser_id})
            found=db.execute('SELECT computed,body FROM interpretations WHERE id=?',(key,)).fetchone()
            if found is None:
                body=interpret(text);computed=clock();parsed+=1
                db.execute('INSERT INTO interpretations VALUES (?,?,?,?,?)',(key,text,parser_id,computed,encoded(body).decode()))
            else:computed,raw=found;body=json.loads(raw)
            # Input provenance is distinct from semantic identity and unchanged
            # duplicates do not generate another interpretation or observation.
            record={**row,'interpretation_id':key,'parser_sha256':parser_id}
            identity=digest(record)
            db.execute('INSERT OR IGNORE INTO observations VALUES (?,?,?,?)',(identity,key,clock(),encoded(record).decode()))
            if key in seen:
                seen[key]['source_record_count']+=1
                continue
            item={'event_id':row['event_id'],'headline':text,'publisher':row['source_name'],
                  'published_utc':row['published_utc'],'first_seen_utc':row['first_seen_utc'],
                  'interpretation_id':key,'interpretation_computed_epoch':computed,
                  'available_epoch':max(computed,first),'source_record_count':1,
                  'age_seconds':round(observed-published,3),
                  'interpretation':body,'original_article_scores':json.loads(row['currency_scores_json'] or '{}')}
            seen[key]=item;items.append(item)
        db.commit()
        counts={table:db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0] for table in ('interpretations','observations')}
    finally:db.close()
    generated=clock()
    try:
        raw=Path(collector_heartbeat).read_bytes()
        if len(raw)>128*1024:raise ValueError('heartbeat_size')
        health=json.loads(raw);tick=epoch(health['generated_utc'])
        fresh=health.get('schema_version')=='local_fx_news_sentiment_v3' and 0<=generated-tick<=180 and health.get('status') not in ('error','failed','stopped')
    except (OSError,ValueError,KeyError,TypeError):fresh=False
    result={'schema_version':SCHEMA,'generated_epoch':generated,'generated_utc':iso(generated),
            'source_observed_epoch':observed,'source_database':str(Path(db_path).resolve()),
            'status':'current' if fresh else 'source_unavailable','collector_current':fresh,
            'source_bindings':graph,'parser_sha256':parser_id,'source_rows':len(rows),
            'selection':'latest500 relevant article records published within24h; first_seen<=read_time',
            'selection_truncated':truncated,'new_interpretations':parsed,'store_counts':counts,
            'topic_count':len(items),'topics':items[:100],'display_limit':100,
            'processing_seconds':round(generated-observed,3),
            'scope':'Current text interpretation, not a forecast or validated model feature',**FLAGS}
    result['payload_sha256']=digest(result)
    atomic(output/'current.json',result)
    return result


def atomic(path,value):
    raw=encoded(value)
    if len(raw)>2*1024**2:raise ValueError('context_publication_size')
    tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        tmp.write_bytes(raw)
        for attempt in range(5):
            try:tmp.replace(path);return
            except PermissionError:
                if attempt==4:raise
                time.sleep(.05)
    finally:tmp.unlink(missing_ok=True)


def read_current(path, *, now=None):
    now=time.time() if now is None else now
    try:
        path=Path(path)
        if path.stat().st_size>2*1024**2:raise ValueError('size')
        raw=path.read_bytes();value=json.loads(raw)
        if value.get('payload_sha256')!=digest({k:v for k,v in value.items() if k!='payload_sha256'}):raise ValueError('payload_changed')
        if (value.get('schema_version')!=SCHEMA or value.get('source_bindings')!=bindings()
            or any(value.get(k)!=v for k,v in FLAGS.items())
            or not 0<=now-value['generated_epoch']<=60
            or not 0<=value['generated_epoch']-value['source_observed_epoch']<=30
            or value.get('status')!='current' or value.get('collector_current') is not True):raise ValueError('identity_or_freshness')
        if not isinstance(value.get('topics'),list) or len(value['topics'])>100:raise ValueError('topics')
        for t in value['topics']:
            if (not isinstance(t.get('headline'),str) or len(t['headline'])>2000
                or not 0<t['interpretation_computed_epoch']<=t['available_epoch']<=value['generated_epoch']
                or t['interpretation'].get('version')!=SCHEMA
                or any(t['interpretation'].get(k)!=v for k,v in FLAGS.items())):raise ValueError('topic_identity')
        # Expose no raw long/short score as the corrected context.
        return {**value,'topics':[{k:v for k,v in t.items() if k!='original_article_scores'} for t in value['topics']]}
    except (OSError,ValueError,TypeError,KeyError):
        return {'schema_version':SCHEMA,'status':'unavailable','topics':[],**FLAGS}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--collector-heartbeat',type=Path,required=True)
    p.add_argument('--duration-sec',type=int,default=604800);p.add_argument('--once',action='store_true')
    args=p.parse_args()
    if not 1<=args.duration_sec<=604800:p.error('duration outside bounded range')
    args.output.mkdir(parents=True,exist_ok=True)
    # SQLite lock prevents competing writers, including across restarts.
    lock=sqlite3.connect(args.output/'owner.sqlite',timeout=0)
    lock.execute('CREATE TABLE IF NOT EXISTS owner (id INTEGER)');lock.commit()
    lock.execute('BEGIN EXCLUSIVE')
    deadline=time.monotonic()+args.duration_sec
    try:
        while time.monotonic()<deadline:
            try:collect(args.database,args.output,args.collector_heartbeat)
            except Exception as exc:
                atomic(args.output/'current.json',{'schema_version':SCHEMA,'generated_epoch':time.time(),'status':'error','error':type(exc).__name__+':'+str(exc)[:200],**FLAGS})
                if args.once:raise
            if args.once:break
            time.sleep(min(15,max(0,deadline-time.monotonic())))
    finally:lock.rollback();lock.close()
    return 0


if __name__=='__main__':raise SystemExit(main())
