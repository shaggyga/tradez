"""Bounded index-only maintenance of existing news stores; never rewrites articles."""
import argparse
from datetime import datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import time

INDEXES = {
    'idx_articles_relevant_published_event_v1':
        'CREATE INDEX idx_articles_relevant_published_event_v1 ON articles(relevant, published_utc, event_id)',
    'idx_topic_events_signature_published_v1':
        'CREATE INDEX idx_topic_events_signature_published_v1 ON topic_events(topic_signature, published_utc)',
}
QUERY = ('SELECT event_id, payload_json, first_seen_utc, last_seen_utc, duplicate_count '
         'FROM articles WHERE relevant = ? AND published_utc >= ? ORDER BY published_utc, event_id')


def query_identity(db, since, deadline=None):
    result = {}
    for relevant in (0, 1):
        digest = hashlib.sha256()
        count = 0
        for row in db.execute(QUERY, (relevant, since)):
            if deadline is not None and time.monotonic()>deadline:
                raise sqlite3.OperationalError('maintenance_time_bound')
            raw = json.dumps(row, ensure_ascii=False, separators=(',', ':')).encode()
            digest.update(len(raw).to_bytes(8, 'big')); digest.update(raw); count += 1
        result[str(relevant)] = {'rows': count, 'sha256': digest.hexdigest()}
    return result


def maintain(path, *, apply=False, since=None, maximum_seconds=30):
    path = Path(path).absolute()
    if not path.is_file() or any(p.is_symlink() or getattr(p, 'is_junction', lambda: False)() for p in (path,*path.parents)):
        raise ValueError('existing_plain_database_required')
    since = since or (datetime.now(timezone.utc)-timedelta(days=1)).isoformat()
    started = time.monotonic()
    db = sqlite3.connect(path.as_uri()+('?mode=rw' if apply else '?mode=ro'), uri=True, timeout=2)
    try:
        db.set_progress_handler(lambda: int(time.monotonic()-started > maximum_seconds), 1000)
        db.execute('BEGIN IMMEDIATE' if apply else 'BEGIN')
        existing = dict(db.execute("SELECT name,sql FROM sqlite_master WHERE type='index'"))
        for name, sql in INDEXES.items():
            if name in existing and existing[name] != sql:
                raise ValueError('existing_index_definition_mismatch:'+name)
        before_plan = list(db.execute('EXPLAIN QUERY PLAN '+QUERY, (1,since)))
        before = query_identity(db,since,started+maximum_seconds)
        count_before = db.execute('SELECT COUNT(*) FROM articles').fetchone()[0]
        created = []
        if apply:
            for name, sql in INDEXES.items():
                if name not in existing:
                    db.execute(sql); created.append(name)
        after = query_identity(db,since,started+maximum_seconds)
        after_plan = list(db.execute('EXPLAIN QUERY PLAN '+QUERY, (1,since)))
        if before != after or count_before != db.execute('SELECT COUNT(*) FROM articles').fetchone()[0]:
            raise ValueError('article_contents_or_order_changed')
        if apply and any('TEMP B-TREE' in row[-1] for row in after_plan):
            raise ValueError('publication_query_still_requires_sort')
        if time.monotonic()>started+maximum_seconds:
            raise sqlite3.OperationalError('maintenance_time_bound')
        db.commit()
        return {'schema':'forex.news_store_index_maintenance.v1','utc':datetime.now(timezone.utc).isoformat(),
                'database':str(path),'mode':'apply_indexes' if apply else 'read_only',
                'indexes_created':created,'article_count':count_before,'content_updates':0,
                'verified_window_start':since,'before':before,'after':after,
                'before_plan':before_plan,'after_plan':after_plan,'elapsed_seconds':time.monotonic()-started,
                'maximum_seconds':maximum_seconds,'status':'pass','models_fitted':0}
    except BaseException:
        db.rollback(); raise
    finally:
        db.close()


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',type=Path,required=True)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--receipt',type=Path,required=True)
    args=parser.parse_args()
    if args.receipt.exists(): raise ValueError('new_receipt_required')
    try:
        result=maintain(args.database,apply=args.apply)
    except Exception as exc:
        result={'status':'failed','utc':datetime.now(timezone.utc).isoformat(),
                'database':str(args.database),'error':type(exc).__name__+':'+str(exc),'models_fitted':0}
        with args.receipt.open('x',encoding='utf-8') as stream:
            json.dump(result,stream,indent=2);stream.write('\n')
        raise
    with args.receipt.open('x',encoding='utf-8') as stream:
        json.dump(result,stream,indent=2);stream.write('\n')
    print(json.dumps(result))
