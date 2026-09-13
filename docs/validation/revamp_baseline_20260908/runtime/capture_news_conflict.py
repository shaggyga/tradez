"""Read-only two-topic reproduction using the frozen pure news guard; no live edits."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import hashlib,json,os,sys,time

ROOT=Path('C:/Users/zmoor/Documents/forex/trad')
OUT=Path(__file__).parent
sys.path.insert(0,str(ROOT))
import oanda_news_causal_aggregation_guard_v1 as guard

def digest(raw):return hashlib.sha256(raw).hexdigest()
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')
def retained(path,limit):
    with path.open('rb') as file:
        before=os.fstat(file.fileno());raw=file.read(limit+1);after=os.fstat(file.fileno())
    if len(raw)>limit:raise ValueError('size_limit')
    if (before.st_size,before.st_mtime_ns,before.st_ino)!=(after.st_size,after.st_mtime_ns,after.st_ino):raise ValueError('changed_during_read')
    return json.loads(raw),{'path':str(path),'bytes':len(raw),'sha256':digest(raw),'observed_epoch':time.time()}
def stamp(value):return datetime.fromisoformat(value.replace('Z','+00:00'))
def brief(topic):
    proof=topic.get('causal_admission_guard') or topic.get('causal_member_admission') or {}
    return {k:topic.get(k) for k in ('topic_id','event_id','headline','topic_signature','story_cluster_id','topic_article_count','article_event_ids','source_names','source_name','source_ids','published_utc','first_seen_utc','causal_known_utc','currency_scores','research_currency_scores','classification_version')}

def main():
    folder=ROOT/'data/oanda_training_manager/local_news_sentiment'
    first,fmeta=retained(folder/'joint_news_current_v1.json',8*1024*1024)
    topics,tmeta=retained(folder/'topics_latest.json',32*1024*1024)
    second,smeta=retained(folder/'joint_news_current_v1.json',8*1024*1024)
    if fmeta['sha256']!=smeta['sha256']:raise ValueError('snapshot_replaced_during_capture')
    if topics['generated_utc']!=first['as_of_utc']:raise ValueError('input_cutoff_mismatch')
    asof=stamp(first['as_of_utc']);by_id=defaultdict(list)
    for index,topic in enumerate(topics['topics']):
        published=stamp(topic['published_utc']);window=float(topic.get('post_window_minutes',0))
        if not 1<=window<=1440 or published>asof or asof>published+timedelta(minutes=window):continue
        view=guard.validate_guarded_topic(topic,as_of=asof)
        current=guard.guard_topic(topic,None,as_of=asof)
        by_id[topic['topic_id']].append((index,topic,view,current))
    conflicts=[]
    for ident,rows in by_id.items():
        if len({digest(canonical(row[3])) for row in rows})<2:continue
        a=rows[0];b=next(row for row in rows[1:] if canonical(row[3])!=canonical(a[3]))
        minimal=[a[1],b[1]]
        replay=guard.build_current_news_snapshot(minimal,as_of=asof)
        individual=[guard.build_current_news_snapshot([t],as_of=asof) for t in minimal]
        conflicts.append({'topic_id':ident,'source_indices':[a[0],b[0]],'topics':minimal,
            'individual_validation':[a[2],b[2]],'individual_snapshot_status':[r['status'] for r in individual],
            'individual_snapshot_news_state':[r['news_state'] for r in individual],
            'reguarded_topic_sha256':[digest(canonical(a[3])),digest(canonical(b[3]))],
            'reguarded_differing_keys':sorted(k for k in set(a[3])|set(b[3]) if a[3].get(k)!=b[3].get(k)),
            'brief_topics':[brief(t) for t in minimal],
            'pair_replay':{k:v for k,v in replay.items() if k!='topics'}})
    if not conflicts:raise ValueError('no_retained_active_conflicting_topics')
    result={'schema':'guarded_news_identity_conflict_reproduction_v1_20260908',
        'captured_utc':datetime.now(timezone.utc).isoformat(),'scope':'Only the conflicting two-topic input(s), copied unchanged from matching published topics/latest cutoff. Original topics file raw bytes are hash-observed provenance and are not retained; only these exact decoded topic records are retained. Pure frozen guard replay only; no collector, broker, model or runtime mutation.',
        'guarded_snapshot':first,'guarded_source_observations':[fmeta,smeta],
        'topic_source':{**tmeta,'generated_utc':topics['generated_utc'],'total_topic_count':topics.get('topic_count'),'retained_topic_count':len(topics['topics']),'active_unique_topic_ids_in_retained_tail':len(by_id)},
        'source_bindings':[{'name':name,'expected_sha256':expected,'actual_sha256':digest((ROOT/name).read_bytes())} for name,expected in first['source_bindings'].items()],
        'script_sha256':digest(Path(__file__).read_bytes()),'conflicts':conflicts}
    raw=(json.dumps(result,indent=2,ensure_ascii=False,allow_nan=False)+'\n').encode('utf-8')
    if len(raw)>1024*1024:raise ValueError('reproduction_size_limit')
    path=OUT/'NEWS_TOPIC_IDENTITY_CONFLICT_CASE_20260908.json'
    with path.open('xb') as file:file.write(raw)
    print(json.dumps({'path':str(path),'sha256':digest(raw),'bytes':len(raw),'as_of_utc':first['as_of_utc'],
        'conflicts':[{k:c[k] for k in ('topic_id','source_indices','individual_snapshot_status','individual_snapshot_news_state','reguarded_differing_keys','brief_topics','pair_replay')} for c in conflicts]},indent=2))

if __name__=='__main__':main()
