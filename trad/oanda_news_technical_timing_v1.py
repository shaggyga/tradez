"""Read-only as-of join and availability-anchored candle outcome diagnostics.

No model predictions are manufactured. Entry is a later completed candle's
bid/ask proxy, not an executable quote or a claim of a historical live signal.
"""
import datetime as dt
import json
import math
import sqlite3
import zlib
from pathlib import Path


def decode(value):
    try:
        return json.loads(zlib.decompress(value))
    except (zlib.error, TypeError):
        return json.loads(value)


def revised(db, pair):
    exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='revisions'").fetchone()
    return bool(exists and db.execute('SELECT 1 FROM revisions WHERE pair=? LIMIT 1',(pair,)).fetchone())


def readonly(path):
    db = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    return db


def select_asof(db, pair, decision, maximum_age=180):
    if not math.isfinite(decision) or not math.isfinite(maximum_age) or maximum_age <= 0:
        raise ValueError('invalid_clock_or_age')
    if revised(db, pair):
        return {'status': 'unavailable', 'reason': 'unresolved_original_input_revision'}
    # Publication and actual read must both precede the decision. A historical
    # bar backfilled today cannot become yesterday's available observation.
    row = db.execute('''SELECT o.*,b.first_observed FROM observations o
        JOIN bars b ON o.pair=b.pair AND o.t=b.t
        WHERE o.pair=? AND o.published>0 AND o.published<=?
        AND b.first_observed<=o.published AND b.first_observed>=o.t+60
        AND o.t+60<=? AND o.t+60>=? ORDER BY o.t DESC LIMIT 1''',
        (pair, decision, decision, decision-maximum_age)).fetchone()
    if row is None:
        return {'status': 'unavailable', 'reason': 'no_fresh_published_technical_snapshot'}
    names = decode(db.execute("SELECT value FROM metadata WHERE key='contract'").fetchone()[0])['contract']['feature_names']
    return {'status': 'available', 'pair': pair, 'bar_start_epoch': row['t'],
            'bar_close_epoch': row['t']+60, 'feature_published_epoch': row['published'],
            'source_read_completed_epoch': row['first_observed'],
            'age_seconds': decision-row['t']-60, 'feature_hash': row['feature_hash'],
            'features': dict(zip(names, decode(row['values_blob'])))}


def join(db, pair, *, news_known, decision, maximum_age=180):
    if not all(math.isfinite(x) for x in (news_known, decision)) or news_known > decision:
        return {'status': 'unavailable', 'reason': 'news_not_known_at_decision'}
    row = select_asof(db, pair, decision, maximum_age)
    return {**row, 'news_available_epoch': news_known, 'decision_epoch': decision,
            'forecast_claimed': False}


def evaluate(db, pair, decision, horizons=(5,15,30,60), maximum_entry_wait=60):
    if not math.isfinite(decision) or not math.isfinite(maximum_entry_wait) or maximum_entry_wait < 0:
        raise ValueError('invalid_outcome_clock')
    if revised(db, pair):
        return {'status': 'unavailable', 'reason': 'unresolved_original_input_revision'}
    # Never use the pre-publication anchor that produced the feature vector.
    entry = db.execute('''SELECT t,body FROM bars WHERE pair=? AND t+60>=?
        AND t+60<=? ORDER BY t LIMIT 1''', (pair, decision, decision+maximum_entry_wait)).fetchone()
    if entry is None:
        return {'status': 'unavailable', 'reason': 'no_post_decision_entry_bar'}
    initial = decode(entry['body'])
    result = {'entry_bar_close_epoch': entry['t']+60,
              'entry_wait_seconds': entry['t']+60-decision, 'horizons': {},
              'scope': 'retrospective_bidask_close_proxy_not_fills;slippage_and_financing_unmodeled'}
    for horizon in horizons:
        if type(horizon) is not int or horizon <= 0:
            raise ValueError('positive_integer_horizon_required')
        rows = db.execute('SELECT t,body FROM bars WHERE pair=? AND t>=? AND t<=? ORDER BY t',
                          (pair, entry['t'], entry['t']+horizon*60)).fetchall()
        if [r['t'] for r in rows] != list(range(entry['t'],entry['t']+(horizon+1)*60,60)):
            result['horizons'][str(horizon)] = {'status': 'missing_path'}
            continue
        final = decode(rows[-1]['body'])
        result['horizons'][str(horizon)] = {'status': 'available',
            'return_bps': (final['close']-initial['close']) / initial['close']*10000,
            'long_net_bps': (final['bid_close']-initial['ask_close'])/initial['close']*10000,
            'short_net_bps': (initial['bid_close']-final['ask_close'])/initial['close']*10000}
    return result


def move_frequency(db, pair, *, pip, horizon=15, threshold_pips=10):
    """Fixed-clock endpoints; greedily keep non-overlapping threshold episodes.

    Count separate days and expose coverage; no daily extrapolation and no use
    of these hindsight-selected episodes as a predictive evaluation cohort.
    """
    if not math.isfinite(pip) or not math.isfinite(threshold_pips) or pip <= 0 or type(horizon) is not int or horizon <= 0 or threshold_pips <= 0:
        raise ValueError('positive_move_contract_required')
    if revised(db, pair):
        return {'status': 'unavailable', 'reason': 'unresolved_original_input_revision'}
    rows = {r['t']: decode(r['body'])['close'] for r in db.execute('SELECT t,body FROM bars WHERE pair=? ORDER BY t',(pair,))}
    daily, next_free = {}, -math.inf
    for t, price in rows.items():
        day = dt.datetime.fromtimestamp(t+60,dt.timezone.utc).date().isoformat()
        d = daily.setdefault(day, {'observed_bars': 0, 'eligible_windows': 0, 'nonoverlapping_moves': 0})
        d['observed_bars'] += 1
        if any(t+i*60 not in rows for i in range(horizon+1)):
            continue
        d['eligible_windows'] += 1
        if t >= next_free and abs(rows[t+horizon*60]-price)/pip >= threshold_pips:
            d['nonoverlapping_moves'] += 1
            next_free = t+horizon*60
    return {'pair': pair, 'horizon_minutes': horizon, 'threshold_pips': threshold_pips,
            'days_utc': daily, 'scope': 'partial_archived_coverage; endpoint_moves_not_intrawindow_excursions'}
