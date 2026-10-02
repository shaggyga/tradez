"""Prospective observations of retained forecasts, per-target outcomes and news links.

No historical issuance import, fitting, account access or trading. Repeated
publications of identical model/input/origin predictions count once. Original
issued.sqlite remains the complete publication history.
"""
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
import shutil

import oanda_current_news_mapping_v1 as mapping
import oanda_news_technical_timing_v1 as timing

SCHEMA = 'retained_forecast_tracking_v1_20260930'
ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / 'data/retained_connection_20260930'
MAX_STORE = 2 * 1024**3
MAX_SEGMENTS = 8
SETTLEMENT_RESERVE = 512 * 1024**2
MIN_FREE = 2 * 1024**3
FLAGS = {'research_only': True, 'can_place_orders': False, 'can_promote': False}
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def number(value):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError('finite_numeric_value_required')
    return value


class TrackingConnection(sqlite3.Connection):
    writer_lock = None
    active_schema = 'main'
    segment_count = 1

    def close(self):
        try:
            super().close()
        finally:
            if self.writer_lock is not None:
                self.writer_lock.close()
                self.writer_lock = None


def segment_paths(path):
    """Deterministic names; gaps are corruption, not permission to forget history."""
    path = Path(path)
    successors = sorted(path.parent.glob(path.name + '.part*.sqlite'))
    expected = [path.with_name(path.name + '.part%03d.sqlite' % i)
                for i in range(1, len(successors) + 1)]
    if successors != expected or len(successors) >= MAX_SEGMENTS:
        raise ValueError('tracking_segment_inventory')
    paths = [path] + successors
    if any(p.is_symlink() or (not p.is_file() and (i or successors)) for i,p in enumerate(paths)):
        raise ValueError('tracking_segment_missing_or_redirected')
    return paths


def store_bytes(path):
    return sum(p.stat().st_size for p in (path, Path(str(path)+'-wal'), Path(str(path)+'-journal'))
               if p.exists())


def open_store(path, *, readonly=False):
    path = Path(path)
    lock = None
    db = None
    try:
        if not readonly:
            if shutil.disk_usage(path.parent).free < MIN_FREE:
                raise ValueError('tracking_free_space_reserve')
            # One writer chooses a successor; SQLite releases this lease after a crash.
            lock = sqlite3.connect(str(path)+'.writer-lock', timeout=2)
            lock.execute('BEGIN IMMEDIATE')
        paths = segment_paths(path)
        discovered_count = len(paths)
        if not readonly:
            if any(store_bytes(p) > MAX_STORE + SETTLEMENT_RESERVE for p in paths):
                raise ValueError('tracking_settlement_reserve_exhausted_no_evidence_deleted')
            if store_bytes(paths[-1]) >= MAX_STORE:
                if len(paths) >= MAX_SEGMENTS:
                    raise ValueError('tracking_segment_capacity_no_evidence_deleted')
                paths.append(path.with_name(path.name + '.part%03d.sqlite' % len(paths)))
        db = sqlite3.connect(path.resolve().as_uri() + ('?mode=ro' if readonly else '?mode=rwc'),
                             uri=True, timeout=2, factory=TrackingConnection)
        db.writer_lock = lock
        db.row_factory = sqlite3.Row
        has_meta=db.execute("SELECT 1 FROM sqlite_master WHERE name='tracking_segments'").fetchone()
        if has_meta:
            recorded=db.execute('SELECT count FROM tracking_segments WHERE id=1').fetchone()[0]
            if discovered_count<recorded or discovered_count>recorded+1:
                raise ValueError('tracking_segment_history_missing')
        schemas = ['main'] + ['s%d' % i for i in range(1,len(paths))]
        for schema, p in zip(schemas[1:], paths[1:]):
            db.execute('ATTACH DATABASE ? AS '+schema,
                       (p.resolve().as_uri()+ ('?mode=ro' if readonly else '?mode=rwc'),))
        if not readonly:
            for schema in schemas:
                db.executescript(_DDL.replace('forecasts',schema+'.forecasts')
                    .replace('news_links',schema+'.news_links')
                    .replace('pending_due',schema+'.pending_due').replace('score_groups',schema+'.score_groups')
                    .replace('ON '+schema+'.forecasts','ON forecasts'))
                ensure_totals(db, schema)
            with db:
                db.execute('CREATE TABLE IF NOT EXISTS tracking_segments(id INTEGER PRIMARY KEY CHECK(id=1),count INTEGER NOT NULL)')
                db.execute('INSERT OR REPLACE INTO tracking_segments VALUES(1,?)',(len(paths),))
        for table in ('forecasts','news_links'):
            db.execute('CREATE TEMP VIEW all_'+table+' AS '+ ' UNION ALL '.join(
                "SELECT '"+s+"' AS segment, * FROM "+s+'.'+table for s in schemas))
        db.active_schema = schemas[-1]
        db.segment_count = len(paths)
        if readonly:
            db.execute('PRAGMA query_only=ON')
        return db
    except BaseException:
        if db is not None:
            db.close()
        elif lock is not None:
            lock.close()
        raise


_DDL = '''
        CREATE TABLE IF NOT EXISTS forecasts (
          id TEXT PRIMARY KEY, registry TEXT NOT NULL, connection TEXT NOT NULL,
          pair TEXT NOT NULL, horizon INTEGER NOT NULL, reference REAL NOT NULL,
          target REAL NOT NULL, observed REAL NOT NULL, body TEXT NOT NULL,
          state TEXT NOT NULL DEFAULT 'pending', next_check REAL NOT NULL,
          outcome TEXT, error REAL, control_error REAL, direction INTEGER);
        CREATE INDEX IF NOT EXISTS pending_due ON forecasts(state,next_check);
        CREATE INDEX IF NOT EXISTS score_groups ON forecasts(registry,connection,horizon,state);
        CREATE TABLE IF NOT EXISTS news_links (
          id TEXT PRIMARY KEY, observed REAL NOT NULL, body TEXT NOT NULL);
    '''


TOTAL_COLUMNS = ('observed','settled','pending','unavailable','error_sum','error_n',
                 'control_sum','control_n','direction_sum','direction_n')


def ensure_totals(db, schema):
    """Incremental exact counts avoid rereading gigabytes on every news cycle."""
    if db.execute('SELECT 1 FROM '+schema+".sqlite_master WHERE name='tracking_totals'").fetchone():
        return
    def terms(alias):
        return ('1',alias+".state='settled'",alias+".state='pending'",alias+".state='unavailable'",
                'coalesce('+alias+'.error,0)',alias+'.error IS NOT NULL',
                'coalesce('+alias+'.control_error,0)',alias+'.control_error IS NOT NULL',
                'coalesce('+alias+'.direction,0)',alias+'.direction IS NOT NULL')
    columns=','.join(TOTAL_COLUMNS)
    add=','.join(c+'='+c+'+('+v+')' for c,v in zip(TOTAL_COLUMNS,terms('NEW')))
    subtract=','.join(c+'='+c+'-('+v+')' for c,v in zip(TOTAL_COLUMNS,terms('OLD')))
    match=lambda a:'registry='+a+'.registry AND connection='+a+'.connection AND horizon='+a+'.horizon'
    db.executescript('BEGIN IMMEDIATE; CREATE TABLE '+schema+'''.tracking_totals (
        registry TEXT,connection TEXT,horizon INTEGER,
        observed INTEGER,settled INTEGER,pending INTEGER,unavailable INTEGER,
        error_sum REAL,error_n INTEGER,control_sum REAL,control_n INTEGER,
        direction_sum REAL,direction_n INTEGER,PRIMARY KEY(registry,connection,horizon));
        INSERT INTO '''+schema+'''.tracking_totals SELECT registry,connection,horizon,
        count(*),sum(state='settled'),sum(state='pending'),sum(state='unavailable'),
        coalesce(sum(error),0),count(error),coalesce(sum(control_error),0),count(control_error),
        coalesce(sum(direction),0),count(direction) FROM '''+schema+'''.forecasts GROUP BY registry,connection,horizon;
        CREATE TRIGGER '''+schema+'''.tracking_totals_insert AFTER INSERT ON forecasts BEGIN
        INSERT INTO tracking_totals (registry,connection,horizon,'''+columns+''')
        VALUES(NEW.registry,NEW.connection,NEW.horizon,'''+','.join(terms('NEW'))+''')
        ON CONFLICT(registry,connection,horizon) DO UPDATE SET '''+add+'''; END;
        CREATE TRIGGER '''+schema+''' .tracking_totals_update AFTER UPDATE ON forecasts BEGIN
        UPDATE tracking_totals SET '''+subtract+' WHERE '+match('OLD')+''';
        INSERT INTO tracking_totals (registry,connection,horizon,'''+columns+''')
        VALUES(NEW.registry,NEW.connection,NEW.horizon,'''+','.join(terms('NEW'))+''')
        ON CONFLICT(registry,connection,horizon) DO UPDATE SET '''+add+'''; END; COMMIT;''')


def summary_groups(db):
    schemas=['main']+['s%d'%i for i in range(1,db.segment_count)]
    rows=db.execute('SELECT registry,connection,horizon,'+
        ','.join('sum('+c+') AS '+c for c in TOTAL_COLUMNS)+
        ' FROM ('+' UNION ALL '.join('SELECT * FROM '+s+'.tracking_totals' for s in schemas)+
        ') GROUP BY registry,connection,horizon ORDER BY registry,horizon,connection').fetchall()
    return [{**{k:r[k] for k in ('registry','connection','horizon','observed','settled','pending','unavailable','direction_n')},
             'mae_bps':r['error_sum']/r['error_n'] if r['error_n'] else None,
             'no_change_mae_bps':r['control_sum']/r['control_n'] if r['control_n'] else None,
             'direction_fraction':r['direction_sum']/r['direction_n'] if r['direction_n'] else None} for r in rows]


def prediction_key(forecast):
    # Issue times vary on repeated inference of the same saved input. Preserve
    # the first actual observer clock and original issue body, not a later retry.
    return digest({k: forecast[k] for k in (
        'registry_sha256', 'connection', 'instrument', 'horizon_minutes',
        'reference_epoch', 'target_epoch', 'feature_hash', 'input_hash',
        'panel_sha256', 'expected_return_bps', 'reference_mid')})


def qualified_rows(view, now):
    if view.get('status') != 'current':
        return []
    generated = number(view['generated_epoch'])
    if not 0 <= now - generated <= 120:
        raise ValueError('current_publication_required')
    entries = {e['id']: e for e in view['connections']}
    rows, seen = [], set()
    for f in view['forecasts']:
        ref, issued, target = [number(f[k]) for k in ('reference_epoch', 'issued_epoch', 'target_epoch')]
        h = f['horizon_minutes']
        if (type(h) is not int or h <= 0 or entries[f['connection']]['horizon_minutes'] != h
                or target != ref + h * 60 or not ref <= issued <= generated <= now < target
                or not 0 <= now-ref <= 180 or number(f['reference_mid']) <= 0
                or f['registry_sha256'] != view['registry_sha256']
                or any(f.get(k) != v for k, v in FLAGS.items())):
            raise ValueError('forecast_identity_or_clock')
        number(f['expected_return_bps'])
        key = prediction_key(f)
        if key in seen:
            raise ValueError('duplicate_prediction')
        seen.add(key)
        rows.append((key, f))
    return rows


def record(db, technical, view, context, now):
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != LOADED_SOURCE_SHA256:
        raise ValueError('loaded_tracking_source_changed')
    number(now)
    rows = qualified_rows(view, now)
    inserted = 0
    processed = 0
    settlement_started = time.monotonic()
    active = getattr(db, 'active_schema', 'main')
    with db:
        for key, f in rows:
            if db.execute('SELECT 1 FROM all_forecasts WHERE id=? LIMIT 1',(key,)).fetchone():
                continue
            before = db.total_changes
            db.execute('''INSERT OR IGNORE INTO '''+active+'''.forecasts
                (id,registry,connection,pair,horizon,reference,target,observed,body,next_check)
                VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (key, f['registry_sha256'], f['connection'], f['instrument'], f['horizon_minutes'],
                 f['reference_epoch'], f['target_epoch'], now,
                 encoded({'forecast': f, 'publication_epoch': view['generated_epoch'],
                          'publication_sha256': view['payload_sha256'], 'first_observed_epoch': now}),
                 f['target_epoch']))
            inserted += db.execute('SELECT changes()').fetchone()[0]
        # These link only forecasts observed at this decision; expired/backfilled
        # publications cannot gain a historical news association.
        if context.get('status') == 'current' and 0 <= now-context['generated_epoch'] <= 60:
            by_pair = {}
            for key, f in rows:
                by_pair.setdefault(f['instrument'], []).append(key)
            links = 0
            for topic in context.get('topics', [])[:100]:
                if not 0 < topic['available_epoch'] <= context['generated_epoch'] <= now:
                    continue
                currencies = {c.get('currency') for c in topic['interpretation']['claims'] if c.get('currency')}
                for pair, ids in sorted(by_pair.items()):
                    if not currencies.intersection(pair.split('_')):
                        continue
                    key = digest([topic['interpretation_id'], pair, view['registry_sha256']])
                    if links >= 256:
                        break
                    if db.execute('SELECT 1 FROM all_news_links WHERE id=? LIMIT 1',(key,)).fetchone():
                        continue
                    before = db.total_changes
                    db.execute('INSERT OR IGNORE INTO '+active+'.news_links VALUES (?,?,?)', (key, now, encoded({
                        'interpretation_id': topic['interpretation_id'], 'headline': topic['headline'],
                        'claims': topic['interpretation']['claims'], 'available_epoch': topic['available_epoch'],
                        'decision_epoch': now, 'instrument': pair, 'forecast_ids': ids,
                        'context_payload_sha256': context['payload_sha256'],
                        'scope': 'observed conjunction; typed context not claimed as a model input'})))
                    links += db.total_changes-before
        # Share the bounded batch between oldest backlog and newly due outcomes.
        # A long outage must not starve current monitoring while history catches up.
        recent=db.execute("SELECT * FROM all_forecasts WHERE state='pending' AND next_check<=? ORDER BY next_check DESC,id LIMIT 2048", (now,)).fetchall()
        oldest=db.execute("SELECT * FROM all_forecasts WHERE state='pending' AND next_check<=? ORDER BY next_check,id LIMIT 2048", (now,)).fetchall()
        due={}
        for a,b in zip(recent,oldest):
            due.setdefault(a['id'],a);due.setdefault(b['id'],b)
        for r in due.values():
            if time.monotonic()-settlement_started>5:
                break
            processed += 1
            f = json.loads(r['body'])['forecast']
            result = mapping.forecast_outcome(technical, r['pair'], {
                'target_epoch': r['target'], 'reference_mid': f['reference_mid'],
                'predicted_return_bps': f['expected_return_bps']}, now)
            state = result['status']
            if state == 'settled':
                result['scope'] = 'saved_reference_to_first_completed_M1_close_at_or_after_exact_elapsed_target; not fills or trading_PnL'
            elif state.startswith('pending') and now-r['target'] <= 86400:
                # Missing early targets cannot starve later eligible settlements.
                db.execute('UPDATE '+r['segment']+'.forecasts SET next_check=? WHERE id=?', (now+300, r['id']))
                continue
            else:
                result = {**result, 'status': 'unavailable', 'terminal_at_epoch': now}
                state = 'unavailable'
            db.execute('UPDATE '+r['segment']+'.forecasts SET state=?,outcome=?,error=?,control_error=?,direction=? WHERE id=?',
                (state, encoded(result), result.get('absolute_error_bps'), result.get('no_change_absolute_error_bps'),
                 result.get('direction_correct'), r['id']))
    groups = summary_groups(db)
    return {'schema': SCHEMA, 'generated_epoch': now, 'status': 'current',
            'source_sha256': LOADED_SOURCE_SHA256,
            'new_predictions': inserted, 'groups': groups,
            'outcomes_checked':processed,
            'news_links': db.execute('SELECT COUNT(*) FROM all_news_links').fetchone()[0],
            'storage': {'segments': getattr(db,'segment_count',1), 'maximum_segments':MAX_SEGMENTS,
                        'insert_limit_bytes':MAX_STORE, 'settlement_reserve_bytes':SETTLEMENT_RESERVE,
                        'history_deleted':False},
            'unit': 'unique registry/model/pair/reference/input/prediction; overlapping horizons are dependent',
            'scope': 'prospective observed sample, per-registry/horizon; no pooled skill or original producer settlement claim',
            **FLAGS}


def collect(context, *, output=OUTPUT, registry_path=None, clock=time.time):
    # Import failure stays inside the context worker's optional tracking guard.
    import oanda_retained_forecast_connection_v1 as retained
    view = retained.read_current(Path(output)/'current.json', registry_path or retained.REGISTRY, now=clock())
    cfg, reference = retained.availability.read_config(retained.availability.DEFAULT_CONFIG)
    retained.availability.verify_dependencies(cfg, reference, retained.availability.DEFAULT_OPERATIONS_CONFIG)
    technical = timing.readonly(Path(cfg['output_root'])/'technical.sqlite')
    deadline = time.monotonic()+8
    technical.set_progress_handler(lambda: int(time.monotonic()>deadline), 10000)
    db = open_store(Path(output)/'tracking.sqlite')
    db.set_progress_handler(lambda: int(time.monotonic()>deadline), 10000)
    try:
        value = record(db, technical, view, context, clock())
        value['payload_sha256'] = digest(value)
        retained.atomic(Path(output)/'tracking_current.json', value)
        return value
    finally:
        db.close()
        technical.close()


def read_current(path=OUTPUT/'tracking_current.json', *, now=None):
    now = time.time() if now is None else now
    try:
        path = Path(path)
        if path.stat().st_size > 4*1024**2:
            raise ValueError('summary_size')
        v = json.loads(path.read_bytes())
        if (v['schema'] != SCHEMA or v['status'] != 'current' or not 0 <= now-v['generated_epoch'] <= 180
                or v['source_sha256'] != LOADED_SOURCE_SHA256
                or LOADED_SOURCE_SHA256 != hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
                or v['payload_sha256'] != digest({k:x for k,x in v.items() if k != 'payload_sha256'})
                or any(v.get(k) != x for k,x in FLAGS.items())):
            raise ValueError('summary_identity_or_age')
        return v
    except (OSError, ValueError, KeyError, TypeError):
        return {'schema': SCHEMA, 'status': 'unavailable', 'groups': [], **FLAGS}
