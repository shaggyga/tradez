"""Bounded headline-only RSS lane beside the unchanged broad collector.

Separate observation store; these bytes are not legacy collector attestations,
full articles, trained features or a claim of an earlier observation.
"""
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import hashlib
import json
import re
from pathlib import Path
import sqlite3
import time
import urllib.error
import urllib.request
import oanda_local_news_sentiment as legacy

SCHEMA='fast_headline_observation_v1_20260930'
CONFIG_SHA='1a4ae5e79213eb1be1642162b2c649683d6511d9a9979920913bd8d7c2b7a016'
COLLECTOR_SHA='44e66d85f82b52d6dc82e2e277bad31d2ee8c16304bec9c6a083a8b17eeb6c50'
MAX_BYTES=2*1024**2
MAX_STORE=128*1024**2
CRYPTO=re.compile(r'\b(?:crypto(?:currency|currencies)?|bitcoin|ethereum|stablecoins?|blockchain|BTC|ETH|altcoins?)\b',re.I)

def iso(t): return dt.datetime.fromtimestamp(t,dt.timezone.utc).isoformat()
def dump(v): return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=True)
def digest(v): return hashlib.sha256(dump(v).encode()).hexdigest()

def source_config(path):
    raw=Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=CONFIG_SHA: raise ValueError('fast_feed_config_changed')
    if hashlib.sha256(Path(legacy.__file__).read_bytes()).hexdigest()!=COLLECTOR_SHA: raise ValueError('rss_parser_source_changed')
    value=json.loads(raw)
    assert value['research_only'] is True and value['can_place_orders'] is False
    rows=value['sources']
    if not 1<=len(rows)<=16 or len({r['source_id'] for r in rows})!=len(rows): raise ValueError('fast_feed_population')
    for row in rows:
        if row['kind']!='rss' or not row['url'].startswith('https://') or not 60<=row['poll_interval_sec']<=3600: raise ValueError('fast_feed_policy')
    return rows

def fetch(source,state,*,opener=urllib.request.urlopen,clock=time.time):
    headers={'User-Agent':'ForexResearchNewsCollector/1.0 (bounded headline context)'}
    for field,header in [('etag','If-None-Match'),('modified','If-Modified-Since')]:
        if state.get(field):headers[header]=state[field]
    started=clock()
    try:
        with opener(urllib.request.Request(source['url'],headers=headers),timeout=8) as response:
            raw=response.read(MAX_BYTES+1)
            if len(raw)>MAX_BYTES:raise ValueError('feed_byte_limit')
            # No detail-page fetch or Google publisher resolution on this lane.
            rows=legacy.parse_rss(raw,source)[:500]
            observed=clock()
            return dict(status='ok',started=started,observed=observed,rows=rows,
                        payload_sha256=hashlib.sha256(raw).hexdigest(),
                        etag=response.headers.get('ETag',''),modified=response.headers.get('Last-Modified',''))
    except urllib.error.HTTPError as exc:
        retry=exc.headers.get('Retry-After','') if exc.headers else ''
        try:retry=max(0,min(86400,float(retry)))
        except ValueError:
            parsed=legacy.parse_datetime(retry);retry=max(0,min(86400,parsed.timestamp()-clock())) if parsed else 0
        return dict(status='not_modified' if exc.code==304 else 'error',http_status=exc.code,
                    started=started,observed=clock(),retry_after_seconds=retry,rows=[],error=str(exc)[:200])
    except Exception as exc:
        return dict(status='error',started=started,observed=clock(),rows=[],error=type(exc).__name__+':'+str(exc)[:200])

class FastLane:
    def __init__(self,config,output,*,fetcher=fetch,clock=time.time,workers=4):
        self.sources=source_config(config);self.output=Path(output);self.output.mkdir(parents=True,exist_ok=True)
        self.clock=clock;self.fetcher=fetcher;self.pool=ThreadPoolExecutor(max_workers=workers,thread_name_prefix='headline-feed')
        self.workers=workers;self.pending={};self.db_path=self.output/'fast_headlines.sqlite'
        self.db=sqlite3.connect(self.db_path,timeout=2)
        self.db.execute('CREATE TABLE IF NOT EXISTS sources (id TEXT PRIMARY KEY, body TEXT)')
        self.db.execute('''CREATE TABLE IF NOT EXISTS articles (event_id TEXT PRIMARY KEY, headline TEXT,
            source_name TEXT, source_id TEXT, source_url TEXT, published_utc TEXT, first_seen_utc TEXT,
            currency_scores_json TEXT, relevant INTEGER, observation_contract TEXT, payload_sha256 TEXT)''')
        self.db.execute('CREATE INDEX IF NOT EXISTS articles_recent ON articles(first_seen_utc)')
        self.db.commit()
        self.states={sid:json.loads(body) for sid,body in self.db.execute('SELECT id,body FROM sources')}

    def ingest(self,source,result):
        sid=source['source_id'];prior=self.states.get(sid,{})
        observed=result['observed'];errors=0 if result['status'] in ('ok','not_modified') else prior.get('errors',0)+1
        wait=max(source['poll_interval_sec']*min(32,2**errors),result.get('retry_after_seconds',0))
        state={**prior,'status':result['status'],'last_attempt':result['started'],'completed':observed,
               'next_due':observed+min(86400,wait),'errors':errors,'last_error':result.get('error',''),
               'elapsed_seconds':observed-result['started'],'items':len(result['rows'])}
        if result['status']=='ok':state.update(etag=result.get('etag',''),modified=result.get('modified',''),last_success=observed)
        if result['status']=='not_modified':state['last_success']=observed
        inserted=0
        for row in result['rows']:
            pub=legacy.parse_datetime(row.get('published_utc'))
            if pub is None or not observed-86400<=pub.timestamp()<=observed:continue
            headline=str(row.get('headline') or row.get('title') or '');url=str(row.get('source_url') or row.get('url') or '')
            if not headline or len(headline)>2000:continue
            if CRYPTO.search(headline):continue
            # Headline changes create a new observation; a repeat cannot move first_seen.
            key=digest([SCHEMA,sid,url,headline,row.get('published_utc')])
            cur=self.db.execute('INSERT OR IGNORE INTO articles VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (key,headline,row.get('source_name') or source['name'],sid,url,pub.isoformat(),iso(observed),'{}',1,SCHEMA,result['payload_sha256']))
            inserted+=cur.rowcount
        state['new_records']=inserted;self.states[sid]=state
        self.db.execute('INSERT OR REPLACE INTO sources VALUES (?,?)',(sid,dump(state)));self.db.commit()

    def tick(self):
        if sum(p.stat().st_size for p in self.output.glob('fast_headlines.sqlite*'))>MAX_STORE: raise ValueError('fast_store_capacity')
        for sid,(source,future) in list(self.pending.items()):
            if future.done():
                self.ingest(source,future.result());del self.pending[sid]
        now=self.clock()
        due=sorted(self.sources,key=lambda s:self.states.get(s['source_id'],{}).get('next_due',0))
        for source in due:
            sid=source['source_id']
            if len(self.pending)>=self.workers:break
            if sid not in self.pending and now>=self.states.get(sid,{}).get('next_due',0):
                self.pending[sid]=(source,self.pool.submit(self.fetcher,source,dict(self.states.get(sid,{}))))
        return {'schema_version':SCHEMA,'generated_epoch':now,'sources':self.states,
                'in_flight':sorted(self.pending),'source_count':len(self.sources),
                'scope':'headline-only supplemental transport; original source clocks retained; no full-article/legacy-cohort claim',
                'config_sha256':CONFIG_SHA,'parser_source_sha256':COLLECTOR_SHA}

    def close(self):
        self.pool.shutdown(wait=False,cancel_futures=True);self.db.close()
