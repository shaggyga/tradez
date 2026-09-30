"""Original legacy26 inputs from authenticated, actually observed minute bars.

Reads the existing store only. No resampling, filling, fitting or acquisition.
The preceding gap must be present inside the bounded read so session age cannot
silently restart at a truncated tail. Missing windows retain original sentinels.
"""
from pathlib import Path
import hashlib
import importlib.util
import json
import math
import sqlite3
import time

import numpy as np
import pandas as pd
_spec = importlib.util.spec_from_file_location('retained_legacy26_original',
    Path(__file__).resolve().parents[1]/'stage_c_alignment_integrity_v2/retained_direction_features_v1.py')
original = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(original)

FEATURES = list(original.TECHNICAL_COLUMNS) + ['known_entry_long_cost_bps', 'known_entry_short_cost_bps']
MAX_ROWS = 10082


def encoded(x):
    return json.dumps(x, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def read_pair(db, pair, row, *, max_rows=MAX_ROWS):
    at, asof = row['bar_start_epoch'], row['captured_epoch']
    if type(at) is not int or at % 60 or not math.isfinite(asof) or asof < at+60:
        raise ValueError('legacy26_reference_clock')
    if db.execute('SELECT 1 FROM revisions WHERE pair=? LIMIT 1', (pair,)).fetchone():
        raise ValueError('legacy26_unresolved_revision')
    times = [r[0] for r in db.execute('SELECT t FROM bars WHERE pair=? AND t<=? ORDER BY t DESC LIMIT ?',
                                     (pair, at, max_rows))]
    if not times or times[0] != at:
        raise ValueError('legacy26_reference_missing')
    gaps = [i for i in range(1, len(times)) if times[i-1]-times[i] != 60]
    if not gaps:
        raise ValueError('legacy26_session_boundary_unproven')
    boundary = times[gaps[0]]  # Include the predecessor that establishes the gap.
    lower = min(boundary, at-3600)
    # Verify every consumed bar and receipt; the source is a single read snapshot.
    records = db.execute('''SELECT b.t,b.body,b.input_hash,b.receipt_id,b.first_observed,
        r.body AS receipt_body,r.pair AS receipt_pair,r.observed
        FROM bars b LEFT JOIN receipts r ON r.id=b.receipt_id
        WHERE b.pair=? AND b.t>=? AND b.t<=? ORDER BY b.t''', (pair, lower, at)).fetchall()
    if len(records) > max_rows or len(records) < 2:
        raise ValueError('legacy26_history_bound')
    candles, identities, receipt_cache = [], [], {}
    for r in records:
        raw, rr = bytes(r['body']), bytes(r['receipt_body']) if r['receipt_body'] is not None else b''
        if len(raw)>16384 or len(rr)>65536 or digest(raw)!=r['input_hash'] or digest(rr)!=r['receipt_id']:
            raise ValueError('legacy26_original_hash_mismatch')
        receipt = receipt_cache.setdefault(r['receipt_id'], json.loads(rr))
        observed = r['first_observed']
        if (r['t']%60 or not r['t']+60 <= observed <= asof or r['observed']!=observed
                or receipt.get('observed_epoch')!=observed or receipt.get('instrument')!=pair or r['receipt_pair']!=pair):
            raise ValueError('legacy26_receipt_clock_or_pair')
        b=json.loads(raw)
        if b.get('time')!=r['t'] or isinstance(b.get('close'),bool) or not isinstance(b.get('close'),(int,float)) or not math.isfinite(b['close']) or b['close']<=0:
            raise ValueError('legacy26_invalid_price')
        candles.append({'epoch':r['t'], 'mid':b['close']})
        identities.append([r['t'],r['input_hash'],r['receipt_id'],observed])
    if records[-1]['input_hash']!=row['input_hash'] or candles[-1]['mid']!=row['reference_mid']:
        raise ValueError('legacy26_reference_changed')
    values,_=original._technical(pd.DataFrame(candles))
    features=[float(v) for v in values.iloc[-1]] + [row['entry_long'],row['entry_short']]
    if len(features)!=26 or not np.isfinite(features).all() or min(features[-2:])<0:
        raise ValueError('legacy26_nonfinite_features')
    history=digest(encoded(identities))
    return {'status':'available','values':dict(zip(FEATURES,features)),
            'feature_hash':digest(encoded(features)), 'input_hash':history,
            'provenance':{'history_sha256':history,'rows':len(records),'first_bar_start':records[0]['t'],
                'reference_bar_start':at,'session_start':times[gaps[0]-1],
                'maximum_first_observed':max(r['first_observed'] for r in records),
                'available_asof':asof,'scope':'original legacy26 from receipt-qualified retained bars; no price fill'}}


def attach(rows, path, *, seconds=15):
    start=time.monotonic();db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=1)
    db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
    db.set_progress_handler(lambda:int(time.monotonic()-start>seconds),10000)
    try:
        for pair,row in rows.items():
            try:
                if time.monotonic()-start>seconds:raise ValueError('legacy26_read_budget')
                row['legacy26']=read_pair(db,pair,row)
            except (ValueError,KeyError,TypeError,sqlite3.Error) as exc:
                row['legacy26']={'status':'unavailable','reason':str(exc)[:160]}
    finally:
        db.rollback();db.close()
    return {'elapsed_seconds':time.monotonic()-start,'available':sum(r['legacy26']['status']=='available' for r in rows.values()),'read_only':True}
