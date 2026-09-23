"""Read-only local feed audit; writes only this workspace evidence file."""
from __future__ import annotations
import collections
import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3
from contextlib import closing
import urllib.request

ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
DATA = ROOT / 'data/oanda_training_manager/local_news_sentiment'
OUT = Path(__file__).resolve().parent / 'FEED_AUDIT.json'
UTC = dt.timezone.utc
def now(): return dt.datetime.now(UTC)
def parse(v):
    if not v: return None
    try: return dt.datetime.fromisoformat(str(v).replace('Z', '+00:00'))
    except (ValueError, TypeError): return None
def age(v, clock):
    p = parse(v)
    return round((clock-p).total_seconds(), 3) if p and p.tzinfo else None
def counts(vals): return dict(sorted(collections.Counter(vals).items()))
def sign(v): return 'positive' if float(v or 0)>0 else 'negative' if float(v or 0)<0 else 'zero'
def kept(d, keys): return {k:d.get(k) for k in keys}

evidence = {'schema':'pair_news_read_only_audit_v1_20260907', 'started_utc':now().isoformat(),
    'project_root':str(ROOT), 'read_only_runtime':True, 'source_files':[],
    'limitations':['Runtime JSON files and database are separately timestamped snapshots, not one atomic multi-file generation.',
                   'Feed health means successful observation, not fresh original content, accurate interpretation, or proven trading value.',
                   'This audit uses local retained data and code; it does not verify publishers against the external web.']}
def read(name):
    p = DATA/name
    raw=p.read_bytes(); clock=now(); value=json.loads(raw.decode('utf-8-sig'))
    evidence['source_files'].append({'path':str(p),'sha256':hashlib.sha256(raw).hexdigest(),
                                   'read_utc':clock.isoformat(),'bytes':len(raw),
                                   'generated_utc':value.get('generated_utc')})
    return value, clock

collector, cc = read('collector_latest_v1.json')
heartbeat, hc = read('collector_heartbeat_v1.json')
state, sc = read('collector_state_v1.json')
pair, pc = read('pair_sentiment_latest.json')
coverage, covc = read('source_coverage_latest.json')
official, oc = read('official_release_fast_lane_latest_v4.json')
evidence['collector'] = kept(collector, ['generated_utc','status','cycle_in_progress','configured_sources',
 'attempted_sources','operational_sources','classified_items','fetched_items','inserted_items','duplicate_items',
 'retention_eligible_items','retention_skipped_items','active_articles','active_scored_pairs',
 'active_direct_scored_pairs','active_global_proxy_scored_pairs','context_retained_items','relevant_retained_items',
 'published_relevant_items','published_context_items','syndicated_items_collapsed','source_health','event_catalog','policy'])
evidence['collector']['age_seconds_at_read'] = age(collector.get('generated_utc'),cc)
evidence['heartbeat'] = kept(heartbeat,['generated_utc','heartbeat_utc','status','phase','phase_age_seconds',
 'progress_age_seconds','progress_sequence','cycle_in_progress','details'])
evidence['official_fast_lane'] = kept(official,['generated_utc','status','configured_release_source_count',
 'currency_count','completed_sources','attempted_sources','duplicate_observations','inserted_observations','counts'])
evidence['official_fast_lane']['error_count'] = len(official.get('errors') or [])
evidence['official_fast_lane']['age_seconds_at_read'] = age(official.get('generated_utc'),oc)

sources={}
for currency,row in coverage.get('currencies',{}).items():
    for s in row.get('sources',[]):
        sid=s['source_id']
        if sid not in sources:
            sources[sid]=kept(s,['source_id','name','kind','health_state','healthy','direct','operational',
                    'last_status','last_success_utc','recent_success_age_sec','runtime_status','source_role','retrieval_via'])
            sources[sid]['currency_coverage']=[]
        sources[sid]['currency_coverage'].append(currency)
for sid,s in state.get('sources',{}).items():
    target=sources.setdefault(sid,{'source_id':sid,'currency_coverage':[]})
    target.update(kept(s,['last_attempt_utc','last_success_utc','last_status','consecutive_errors','parsed_items',
                         'response_bytes','operational_status']))
    target['has_last_error']=bool(s.get('last_error'))
    target['has_last_detail_error']=bool(s.get('last_detail_error'))
    target['last_success_age_seconds_at_state_read']=age(s.get('last_success_utc'),sc)

pair_rows=pair.get('pairs',{})
if isinstance(pair_rows,list): pair_rows={r['instrument']:r for r in pair_rows}
evidence['pair_snapshot']={'generated_utc':pair.get('generated_utc'),
 'age_seconds_at_read':age(pair.get('generated_utc'),pc),
 'as_of_utc_distribution':counts(r.get('as_of_utc') or 'missing' for r in pair_rows.values()),
 'active_article_count_global':pair.get('active_article_count'),'instrument_count':len(pair_rows),
 'direction_counts':counts(r.get('direction') or 'missing' for r in pair_rows.values()),
 'evidence_quality_counts':counts(r.get('evidence_quality') or 'missing' for r in pair_rows.values()),
 'policy':pair.get('policy'),'pairs':{k:kept(v,['active_event_count','directional_event_count',
 'context_directional_event_count','direction','score','confidence','evidence_quality','as_of_utc'])
 for k,v in pair_rows.items()}}
asof=parse(next(iter(pair_rows.values()))['as_of_utc'])
dbpath=DATA/'local_news_sentiment_v1.sqlite'
dbclock=now()
with closing(sqlite3.connect(dbpath.as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
    db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
    evidence['database']={'path':str(dbpath),'transaction_started_utc':dbclock.isoformat(),
        'mode':'mode=ro + PRAGMA query_only + BEGIN',
        'article_rows':db.execute('SELECT count(*) FROM articles').fetchone()[0],
        'relevant_rows':db.execute('SELECT count(*) FROM articles WHERE relevant=1').fetchone()[0],
        'topic_rows':db.execute('SELECT count(*) FROM topic_events').fetchone()[0]}
    for sid,n,rel,pub,first,last in db.execute('SELECT source_id,count(*),sum(relevant),max(published_utc),max(first_seen_utc),max(last_seen_utc) FROM articles GROUP BY source_id'):
        sources.setdefault(sid,{'source_id':sid,'currency_coverage':[]}).update({'retained_article_rows':n,
            'retained_relevant_rows':rel,'latest_content_published_utc':pub,'latest_first_seen_utc':first,
            'latest_observed_utc':last,'latest_content_age_seconds_at_db_read':age(pub,dbclock)})
    windows={}
    recent=[]
    for hours in (1,6,24):
        since=(dbclock-dt.timedelta(hours=hours)).isoformat()
        rows=db.execute('SELECT source_id,count(*),sum(relevant) FROM articles WHERE published_utc>=? AND published_utc<=? GROUP BY source_id',(since,dbclock.isoformat())).fetchall()
        windows[str(hours)]={'by_source':{sid:{'published_article_rows':n,'relevant_rows':rel} for sid,n,rel in rows},
                            'published_article_rows':sum(r[1] for r in rows),'relevant_rows':sum(r[2] for r in rows)}
    since=(dbclock-dt.timedelta(hours=24)).isoformat()
    for payload,first,last in db.execute('SELECT payload_json,first_seen_utc,last_seen_utc FROM articles WHERE published_utc>=? AND published_utc<=?',(since,dbclock.isoformat())):
        item=json.loads(payload);item['first_seen_utc']=first;item['last_seen_utc']=last;recent.append(item)
    topics=[json.loads(row[0]) for row in db.execute('SELECT payload_json FROM topic_events')]
    db.rollback()
evidence['database']['transaction_finished_utc']=now().isoformat()
evidence['database']['publication_windows_hours']=windows
evidence['sources']=[sources[k] for k in sorted(sources)]
evidence['source_health_summary']={'covered_unique_source_health':counts(s.get('health_state') or 'not_in_coverage' for s in sources.values()),
 'problem_sources':[kept(s,['source_id','name','health_state','operational_status','last_status',
 'last_success_utc','consecutive_errors','has_last_error','has_last_detail_error']) for s in sources.values()
 if s.get('health_state')=='degraded' or s.get('operational_status') in ('credential_missing','disabled','unsupported')
 or (s.get('operational_status')=='enabled' and s.get('has_last_error'))]}

def distribution(rows):
    currency=collections.Counter();directional_currency=collections.Counter();research_currency=collections.Counter()
    for r in rows:
        currency.update(set(r.get('direct_currencies') or []))
        directional_currency.update(k for k,v in (r.get('currency_scores') or {}).items() if float(v or 0))
        research_currency.update(k for k,v in (r.get('research_currency_scores') or {}).items() if float(v or 0))
    return {'count':len(rows),'category':counts(r.get('category') or 'missing' for r in rows),
            'generic_sentiment':counts(sign(r.get('generic_sentiment_score')) for r in rows),
            'directional_publish_eligible':counts(str(r.get('directional_publish_eligible')) for r in rows),
            'forward_signal_timely':counts(str(r.get('forward_signal_timely')) for r in rows),
            'reports_prior_market_move':counts(str(r.get('reports_prior_market_move')) for r in rows),
            'context_reason':counts(r.get('context_reason') or 'none' for r in rows),
            'directional_source_grade':counts(r.get('directional_source_grade') or 'missing' for r in rows),
            'scope':counts(r.get('scope') or 'missing' for r in rows),
            'direct_currency_mentions':dict(sorted(currency.items())),
            'nonzero_currency_score_fields':dict(sorted(directional_currency.items())),
            'nonzero_research_currency_scores':dict(sorted(research_currency.items()))}
evidence['articles_published_last24h']=distribution(recent)
evidence['articles_published_last24h']['score_field_caution']='Raw article currency_scores are classifier candidates. They are not published pair directions: topic clustering can clear them for corroboration, lateness, prior-move and contextual guards.'
evidence['articles_published_last24h']['latest20']=[kept(r,['source_id','headline','published_utc','first_seen_utc',
 'category','direct_currencies','directional_publish_eligible','forward_signal_timely','reports_prior_market_move',
 'currency_scores','research_currency_scores','context_only','context_reason','exclusion_reason']) for r in sorted(recent,key=lambda r:r.get('published_utc') or '',reverse=True)[:20]]

active=[]
for r in topics:
    pub=parse(r.get('published_utc'));known=parse(r.get('causal_known_utc') or r.get('first_known_utc') or r.get('first_seen_utc'))
    if not pub or not known or known>asof:continue
    publication_age=(asof-pub).total_seconds()/60
    if publication_age>max(1,float(r.get('post_window_minutes') or 180)):continue
    causal_age=(asof-known).total_seconds()/60
    horizon=r.get('estimated_reaction_horizon_minutes')
    r['_publication_age_minutes']=round(publication_age,3);r['_causal_age_minutes']=round(causal_age,3)
    r['_reaction_window_expired']=causal_age>float(horizon) if horizon is not None else None
    active.append(r)
evidence['active_topic_reconstruction']={'as_of_utc':asof.isoformat(),'basis':'Read-only topic_events payloads filtered by causal_known <= as_of and publication age <= post_window_minutes, matching published scoring prefilter. Does not rerun classification.',
 'count':len(active),'matches_snapshot_global_count':len(active)==pair.get('active_article_count'),
 'distribution':distribution(active),
 'reaction_window_expired':counts(str(r['_reaction_window_expired']) for r in active),
 'topics':[kept(r,['topic_id','source_id','source_ids','headline','published_utc','first_seen_utc','causal_known_utc',
 'category','scope','direct_currencies','inferred_currencies','generic_sentiment_score','currency_scores','research_currency_scores',
 'context_only','context_reason','directional_source_grade','directional_candidate_source_count','directional_verified_source',
 'directional_publish_eligible','directional_uncertainty','forward_signal_timely','reports_prior_market_move',
 'non_catalyst_context','secondary_analysis_context','post_window_minutes','estimated_reaction_horizon_minutes',
 '_publication_age_minutes','_causal_age_minutes','_reaction_window_expired']) for r in active]}

api_start=now()
try:
    with urllib.request.urlopen('http://127.0.0.1:8765/api/main',timeout=20) as response:
        raw=response.read();api=json.loads(raw)
    news=api.get('news_sentiment') or {}
    evidence['dashboard_api']={'requested_utc':api_start.isoformat(),'received_utc':now().isoformat(),
        'http_status':200,'news':kept(news,['generated_utc','snapshot_age_sec','fresh','status','collector_status',
        'active_article_count','active_scored_pair_count','direction_counts','research_only','execution_eligible','matrix_weight','openai_calls']),
        'top_pairs_count':len(news.get('top_pairs') or [])}
except Exception as exc:
    evidence['dashboard_api']={'requested_utc':api_start.isoformat(),'error_type':type(exc).__name__}

for relative in ['oanda_main_signal_dashboard.html','oanda_practice_live_dashboard.py','oanda_local_news_sentiment.py',
                 'config/news_sources_v1.json','docs/LOCAL_NEWS_SENTIMENT.md']:
    p=ROOT/relative;raw=p.read_bytes()
    evidence['source_files'].append({'path':str(p),'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'read_utc':now().isoformat()})
evidence['code_findings']=[
 {'path':'oanda_main_signal_dashboard.html','line':218,'finding':'Every unranked pair is labeled using global active_scored_pair_count and global active_article_count; articles screened is not an independent per-pair screening count.'},
 {'path':'oanda_practice_live_dashboard.py','line':6431,'finding':'news_ranked drops NEUTRAL pair records; news_sentiment returns only top_pairs[:12], hiding neutral context and otherwise limiting pair summaries to 12.'},
 {'path':'oanda_local_news_sentiment.py','line':19078,'finding':'Forward pair score requires timely initial observation, no prior-move report, nonzero published currency differential, and causal age within reaction horizon. Context research scores remain separate.'},
 {'path':'oanda_local_news_sentiment.py','line':19303,'finding':'active_article_count is active clustered topic count after relevance window filtering, not all fetched or classified articles.'}]
evidence['finished_utc']=now().isoformat()
OUT.write_text(json.dumps(evidence,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
print(json.dumps({'output':str(OUT),'collector':evidence['collector'],'pair_direction_counts':evidence['pair_snapshot']['direction_counts'],
 'active_reconstruction_count':len(active),'active_reconstruction_matches':len(active)==pair.get('active_article_count'),
 'active_distribution':evidence['active_topic_reconstruction']['distribution'],
 'active_expired':evidence['active_topic_reconstruction']['reaction_window_expired'],
 'recent24h':evidence['articles_published_last24h']['count'],
 'publication_windows':{k:{x:y for x,y in v.items() if x!='by_source'} for k,v in windows.items()},
 'problems':evidence['source_health_summary']['problem_sources'], 'dashboard_api':evidence.get('dashboard_api')},indent=2))
