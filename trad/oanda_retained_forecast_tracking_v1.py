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

import oanda_current_news_mapping_v1 as mapping
import oanda_news_technical_timing_v1 as timing

SCHEMA = 'retained_forecast_tracking_v1_20260930'
ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / 'data/retained_connection_20260930'
MAX_STORE = 2 * 1024**3
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


def open_store(path):
    path = Path(path)
    if sum(p.stat().st_size for p in path.parent.glob(path.name + '*')) > MAX_STORE:
        raise ValueError('tracking_capacity_no_evidence_deleted')
    db = sqlite3.connect(path, timeout=2)
    db.row_factory = sqlite3.Row
    db.executescript('''
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
    ''')
    return db


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
    with db:
        for key, f in rows:
            before = db.total_changes
            db.execute('''INSERT OR IGNORE INTO forecasts
                (id,registry,connection,pair,horizon,reference,target,observed,body,next_check)
                VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (key, f['registry_sha256'], f['connection'], f['instrument'], f['horizon_minutes'],
                 f['reference_epoch'], f['target_epoch'], now,
                 encoded({'forecast': f, 'publication_epoch': view['generated_epoch'],
                          'publication_sha256': view['payload_sha256'], 'first_observed_epoch': now}),
                 f['target_epoch']))
            inserted += db.total_changes - before
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
                    before = db.total_changes
                    db.execute('INSERT OR IGNORE INTO news_links VALUES (?,?,?)', (key, now, encoded({
                        'interpretation_id': topic['interpretation_id'], 'headline': topic['headline'],
                        'claims': topic['interpretation']['claims'], 'available_epoch': topic['available_epoch'],
                        'decision_epoch': now, 'instrument': pair, 'forecast_ids': ids,
                        'context_payload_sha256': context['payload_sha256'],
                        'scope': 'observed conjunction; typed context not claimed as a model input'})))
                    links += db.total_changes-before
        for r in db.execute("SELECT * FROM forecasts WHERE state='pending' AND next_check<=? ORDER BY next_check,id LIMIT 1024", (now,)).fetchall():
            f = json.loads(r['body'])['forecast']
            result = mapping.forecast_outcome(technical, r['pair'], {
                'target_epoch': r['target'], 'reference_mid': f['reference_mid'],
                'predicted_return_bps': f['expected_return_bps']}, now)
            state = result['status']
            if state == 'settled':
                result['scope'] = 'saved_reference_to_first_completed_M1_close_at_or_after_exact_elapsed_target; not fills or trading_PnL'
            elif state.startswith('pending') and now-r['target'] <= 86400:
                # Missing early targets cannot starve later eligible settlements.
                db.execute('UPDATE forecasts SET next_check=? WHERE id=?', (now+300, r['id']))
                continue
            else:
                result = {**result, 'status': 'unavailable', 'terminal_at_epoch': now}
                state = 'unavailable'
            db.execute('''UPDATE forecasts SET state=?,outcome=?,error=?,control_error=?,direction=? WHERE id=?''',
                (state, encoded(result), result.get('absolute_error_bps'), result.get('no_change_absolute_error_bps'),
                 result.get('direction_correct'), r['id']))
    groups = [dict(r) for r in db.execute('''SELECT registry,connection,horizon,
        count(*) AS observed, sum(state='settled') AS settled,
        sum(state='pending') AS pending, sum(state='unavailable') AS unavailable,
        avg(error) AS mae_bps, avg(control_error) AS no_change_mae_bps,
        avg(direction) AS direction_fraction, count(direction) AS direction_n
        FROM forecasts GROUP BY registry,connection,horizon ORDER BY registry,horizon,connection''')]
    return {'schema': SCHEMA, 'generated_epoch': now, 'status': 'current',
            'source_sha256': LOADED_SOURCE_SHA256,
            'new_predictions': inserted, 'groups': groups,
            'news_links': db.execute('SELECT COUNT(*) FROM news_links').fetchone()[0],
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
