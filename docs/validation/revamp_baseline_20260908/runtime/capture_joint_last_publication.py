"""Bounded read-only publication metadata across the 68 registered joint-v2 ledgers."""
from pathlib import Path
from contextlib import closing
from datetime import datetime,timezone
import hashlib,json,re,sqlite3,time

ROOT=Path('C:/Users/zmoor/Documents/forex/trad');OUT=Path(__file__).parent
def sha(raw):return hashlib.sha256(raw).hexdigest()
def iso(value):return datetime.fromtimestamp(value,timezone.utc).isoformat() if value is not None else None
def main():
    config=ROOT/'config/joint_price_news_study_v2_20260907.json'
    raw=config.read_bytes();registry=json.loads(raw)
    pairs=registry['pairs'];assert len(pairs)==68 and all(re.fullmatch('[A-Z]{3}_[A-Z]{3}',p) for p in pairs)
    rows=[];started=time.time()
    for pair in sorted(pairs):
        path=ROOT/'data/oanda_training_manager/joint_price_news_study_v2/pairs'/pair/'ridge_price_news_v1/study.sqlite'
        entry={'instrument':pair,'path':str(path)}
        try:
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
                db.execute('PRAGMA query_only=ON');deadline=time.monotonic()+2
                db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
                row=db.execute('SELECT p.id,p.epoch,p.forecast_sha,f.reference,f.target,f.sha,c.epoch,c.forecast_sha FROM publication p JOIN forecasts f ON f.id=p.id LEFT JOIN consumption c ON c.id=p.id ORDER BY p.epoch DESC LIMIT 1').fetchone()
                count=db.execute('SELECT count(*) FROM publication').fetchone()[0]
                entry.update(publication_count=count,observed_epoch=time.time())
                if row:
                    entry.update(decision_id=row[0],publication_epoch=row[1],publication_utc=iso(row[1]),forecast_sha256=row[2],reference_epoch=row[3],target_epoch=row[4],target_utc=iso(row[4]),consumption_epoch=row[6],publication_and_forecast_sha_match=row[2]==row[5],consumption_sha_matches=row[7]==row[2])
        except (sqlite3.Error,OSError) as exc:entry['error']=type(exc).__name__+':'+str(exc)[:200]
        rows.append(entry)
    latest=max((r for r in rows if 'publication_epoch' in r),key=lambda r:r['publication_epoch'],default=None)
    record={'schema':'joint_v2_last_publication_readonly_v1_20260908','started_utc':iso(started),'finished_utc':iso(time.time()),
        'scope':'Read-only SQLite mode=ro, query_only, two-second per-query deadline; latest committed publication/consumption metadata only. Not model execution or full ledger integrity/scoring revalidation. Cross-ledger observations are sequential, not one global transaction.',
        'registry_path':str(config),'registry_sha256':sha(raw),'capture_script_sha256':sha(Path(__file__).read_bytes()),
        'rows':rows,'errors':[r for r in rows if 'error' in r],
        'publication_count_total':sum(r.get('publication_count',0) for r in rows),'latest_publication':latest,
        'all_latest_targets_elapsed_at_end':all(r.get('target_epoch',0)<time.time() for r in rows)}
    target=OUT/'JOINT_LAST_PUBLICATION_20260908.json';data=(json.dumps(record,indent=2)+'\n').encode('utf-8')
    with target.open('xb') as file:file.write(data)
    print(json.dumps({'path':str(target),'sha256':sha(data),'publication_count_total':record['publication_count_total'],'latest_publication':latest,'errors':record['errors']},indent=2))
if __name__=='__main__':main()
