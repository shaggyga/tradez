"""Read-only current inputs for the preserved currency strength ranker.

Full21 and major8 are distinct, fixed universes. No fitting or order client.
Historical prices are observed minute-end mids, not executable bid/ask paths.
"""
from __future__ import annotations

import argparse
import bisect
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time

import oanda_currency_rank_model as ranker
import oanda_market_sentiment_ticker as surface
import oanda_exact_quote_receipts_v1 as receipts

ROOT = Path(__file__).resolve().parent
STATE = ROOT / 'data/oanda_training_manager/state'
DEFAULT_OUTPUT = STATE / 'live_currency_rank_v1.json'
SCHEMA = 'live_currency_rank_v1'
MAJORS = ('AUD', 'CAD', 'CHF', 'EUR', 'GBP', 'JPY', 'NZD', 'USD')
WINDOWS = (5, 15, 60)
DEPENDENCIES = {'oanda_currency_rank_model.py': '4be0cbcf49819c4295e39ae3881f2fd84c167bb3a56b9b67419423b60e78ef7a', 'oanda_market_sentiment_ticker.py': '6f5f6cc3d675a13ef4161aca415fde69c27c53ecff40139b725dbff8edfed158', 'oanda_exact_quote_receipts_v1.py': 'd5d48ea688562e0b6a5e655b4ccbc8c3d4a9fd0a1ffca1f08cddc371eebceb3b'}
SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
QUOTE_AGE = 60
ENDPOINT_LAG = 90


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def history_rows(path, now):
    """Bounded read-only query; no schema mutation or fabricated opening quote."""
    db = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=1)
    deadline = time.monotonic() + 5
    db.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
    try:
        db.execute('PRAGMA query_only=ON')
        return db.execute('SELECT instrument,last_event_epoch,last_mid FROM '
                          'quote_intensity_minutes_v1 WHERE minute_epoch>=? '
                          'AND minute_epoch<=? ORDER BY instrument,last_event_epoch',
                          (now - 65*60, now)).fetchall()
    finally:
        db.close()


def build_pairs(rows, mapped, now):
    history = {}
    for pair, epoch, mid in rows:
        if (pair in ranker.EXPECTED_INSTRUMENTS and
                isinstance(epoch, (float, int)) and math.isfinite(epoch) and
                isinstance(mid, (float, int)) and math.isfinite(mid) and mid > 0 and
                now - 65*60 <= epoch <= now):
            history.setdefault(pair, []).append((epoch, mid))
    for values in history.values():
        values.sort()
    pairs, refused = {}, {}
    for pair in ranker.EXPECTED_INSTRUMENTS:
        quote = mapped.get('quotes', {}).get(pair)
        if quote is None:
            refused[pair] = 'quote:' + mapped.get('refusals', {}).get(pair, 'missing')
            continue
        epoch = quote['market_epoch']
        if not 0 <= now - epoch <= QUOTE_AGE:
            refused[pair] = 'quote_stale_or_future'
            continue
        bid, ask = float(quote['bid']), float(quote['ask'])
        if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid < ask):
            refused[pair] = 'invalid_bid_ask'
            continue
        mid = (bid + ask) / 2
        values = history.get(pair, [])
        epochs = [v[0] for v in values]
        windows = {}
        for minutes in WINDOWS:
            cutoff = epoch - minutes*60
            index = bisect.bisect_right(epochs, cutoff) - 1
            if index < 0 or cutoff - values[index][0] > ENDPOINT_LAG:
                refused[pair] = 'historical_endpoint_unavailable_' + str(minutes)
                break
            start, old = values[index]
            windows[str(minutes)] = dict(return_bps=math.log(mid/old)*10000,
                return_pips=(mid-old)/surface.inferred_pip(pair), start_epoch=start,
                end_epoch=epoch, endpoint_lag_sec=cutoff-start,
                observed_minutes=(epoch-start)/60)
        if len(windows) == len(WINDOWS):
            pairs[pair] = dict(instrument=pair, latest_epoch=epoch, mid=mid,
                spread_bps=(ask-bid)/mid*10000, windows=windows,
                quote_id=quote['quote_id'])
    return pairs, refused


def cohort(pairs, currencies, now):
    excluded = tuple(c for c in ranker.EXPECTED_CURRENCIES if c not in currencies)
    expected = [p for p in ranker.EXPECTED_INSTRUMENTS
                if set(p.split('_')) <= set(currencies)]
    missing = [p for p in expected if p not in pairs]
    supported = {p: pairs[p] for p in expected if p in pairs}
    neighbours = {c: set() for c in currencies}
    for pair in supported:
        a, b = pair.split('_')
        neighbours[a].add(b)
        neighbours[b].add(a)
    seen, todo = set(), [currencies[0]]
    while todo:
        c = todo.pop()
        if c not in seen:
            seen.add(c)
            todo.extend(neighbours[c] - seen)
    minimum_degree = 3 if tuple(currencies) == MAJORS else 1
    enough = (len(seen) == len(currencies) and len(supported) >= math.ceil(.75*len(expected))
              and all(len(v) >= minimum_degree for v in neighbours.values()))
    if not enough:
        return dict(status='unavailable', currency_ranks=[], selected_pairs=[],
                    currencies=list(currencies), missing_pairs=missing,
                    expected_pairs=len(expected), supported_pairs=len(supported),
                    refusal='insufficient_connected_cross_support')
    # The original solver explicitly consumes observed edges, not fabricated
    # zero returns. Keep expected pair identities for the original ranker's
    # universe check; absent windows cannot pass its confirmation/cost gates.
    selected = {p: supported.get(p, dict(instrument=p, windows={}, spread_bps=None)) for p in expected}
    payload = dict(schema_version=ranker.EXPECTED_SCHEMA, status='ready', fresh=True,
        generated_utc=dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
        pair_moves=selected,
        horizons={str(m): surface.solve_currency_strength(supported, m) for m in WINDOWS})
    result = ranker.rank_snapshot(payload, now=dt.datetime.fromtimestamp(now, dt.timezone.utc),
                                 settings=ranker.RankSettings(excluded_currencies=excluded))
    return dict(result, currencies=list(currencies), missing_pairs=missing,
                support_edges=sorted(supported), minimum_degree=minimum_degree,
                coverage='partial' if missing else 'complete',
                expected_pairs=len(expected), supported_pairs=len(supported))


def calculate(rows, mapped, now):
    pairs, refused = build_pairs(rows, mapped, now)
    cohorts = {'full21': cohort(pairs, ranker.EXPECTED_CURRENCIES, now),
               'major8': cohort(pairs, MAJORS, now)}
    selected = next((k for k in ('full21', 'major8') if cohorts[k]['status'] == 'ready'), None)
    return dict(schema_version=SCHEMA, generated_epoch=now,
        generated_utc=dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
        status='ready' if selected else 'unavailable', selected_cohort=selected,
        model_id=ranker.MODEL_ID, source_sha256=SOURCE_SHA256, cohorts=cohorts, pair_refusals=refused,
        supported_pairs=len(pairs), expected_pairs=68, pairs=pairs,
        input_sha256=hashlib.sha256(canonical(dict(rows=rows, mapped=mapped))).hexdigest(),
        quote_snapshot_sha256=mapped.get('snapshot_sha256'),
        quote_max_age_sec=QUOTE_AGE, historical_endpoint_max_lag_sec=ENDPOINT_LAG,
        score_meaning='Relative observed strength; weighted 5m/15m/60m, not calibrated future return.',
        research_only=True, execution_eligible=False, can_place_orders=False)


def verify_sources():
    for name, expected in DEPENDENCIES.items():
        if hashlib.sha256((ROOT/name).read_bytes()).hexdigest() != expected:
            raise ValueError("rank_dependency_changed:"+name)
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != SOURCE_SHA256:
        raise ValueError("rank_worker_source_changed")


def collect(database, quote_path):
    verify_sources()
    rows = history_rows(database, time.time())
    snapshot = receipts.load_quote_snapshot(quote_path)
    now = time.time()
    mapped = receipts.map_receipts(snapshot, observed_epoch=now, decision_epoch=now,
        instruments=list(ranker.EXPECTED_INSTRUMENTS), maximum_age_sec=QUOTE_AGE)
    return calculate(rows, mapped, now)


def atomic_write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.' + str(os.getpid()) + '.tmp')
    tmp.write_bytes(canonical(value))
    try:
        for attempt in range(12):
            try:
                os.replace(tmp, path)
                return
            except PermissionError:
                if attempt == 11:
                    raise
                time.sleep(.1)
    finally:
        tmp.unlink(missing_ok=True)


def read_current(path=DEFAULT_OUTPUT, now=None):
    """Dashboard boundary: an old publication must never remain a live ranking."""
    now = time.time() if now is None else now
    try:
        verify_sources()
        data = json.loads(Path(path).read_bytes())
        if data.get('status') == 'ready' and data.get('source_sha256') != SOURCE_SHA256:
            raise ValueError('rank_publisher_source_changed')
        if data.get('schema_version') != SCHEMA or not 0 <= now-data['generated_epoch'] <= 45:
            raise ValueError('rank_publication_stale_or_invalid')
        if data.get('research_only') is not True or data.get('can_place_orders') is not False:
            raise ValueError('rank_publication_authority_invalid')
        # Each quote must also still meet the age limit at actual display time.
        for result in data.get('cohorts', {}).values():
            if result.get('status') == 'ready':
                expected = result['support_edges']
                if any(p not in data['pairs'] or not 0 <= now-data['pairs'][p]['latest_epoch'] <= QUOTE_AGE
                       for p in expected):
                    result.update(status='unavailable', currency_ranks=[], selected_pairs=[],
                                  display_refusal='quote_expired_since_publication')
        selected = next((k for k in ('full21', 'major8')
                         if data['cohorts'].get(k, {}).get('status') == 'ready'), None)
        data.update(selected_cohort=selected, status='ready' if selected else 'unavailable',
                    publication_age_sec=now-data['generated_epoch'])
        data.pop('pairs', None)
        return data
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return dict(schema_version=SCHEMA, status='unavailable', reason=str(exc),
                    cohorts={}, selected_cohort=None, research_only=True, can_place_orders=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=STATE/'practice_007_quote_intensity_shadow_v1.sqlite')
    parser.add_argument('--quotes', type=Path, default=STATE/'practice_007_exact_quote_receipts_v1.json')
    parser.add_argument('--output', type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument('--heartbeat', type=Path)
    parser.add_argument('--interval-sec', type=float, default=10)
    parser.add_argument('--duration-sec', type=float, default=0)
    args = parser.parse_args(argv)
    if not 5 <= args.interval_sec <= 30 or not 0 <= args.duration_sec <= 604800:
        parser.error('bounded interval 5..30 seconds and duration 0..604800 required')
    started = time.monotonic()
    while True:
        try:
            data = collect(args.database, args.quotes)
        except Exception as exc:
            data = dict(schema_version=SCHEMA, status='unavailable', generated_epoch=time.time(),
                generated_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
                reason=type(exc).__name__+':'+str(exc), cohorts={}, selected_cohort=None,
                research_only=True, execution_eligible=False, can_place_orders=False)
        data['pid'] = os.getpid()
        atomic_write(args.output, data)
        if args.heartbeat:
            atomic_write(args.heartbeat, dict(schema_version='live_currency_rank_worker_v1',
                status='running', pid=os.getpid(), updated_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                rank_status=data['status'], research_only=True, can_place_orders=False))
        if args.duration_sec == 0 or time.monotonic()-started >= args.duration_sec:
            return 0
        time.sleep(args.interval_sec)


if __name__ == '__main__':
    raise SystemExit(main())
