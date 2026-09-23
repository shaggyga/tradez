"""Durable technical observations and separately matured endpoint outcomes.

This is an additive research dataset, never an order/forecast/account ledger.
Raw input sources are read-only. An input revision stops that pair's append
until explicitly reconciled; old feature observations are never overwritten.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
import zlib

import numpy as np

SCHEMA = 'rolling_technical_dataset_v1_20260915'
HORIZONS = (5, 15, 30, 60)
INPUT_FIELDS = ('time', 'open', 'high', 'low', 'close', 'bid_close', 'ask_close', 'volume')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def finite(value):
    value = float(value)
    return value if math.isfinite(value) else None


class RevisionDetected(ValueError):
    pass


class TechnicalStore:
    def __init__(self, path, contract, *, max_bytes=4*1024**3):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max_bytes
        self.connection = sqlite3.connect(self.path, timeout=10)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA journal_mode=WAL')
        self.connection.execute('PRAGMA synchronous=FULL')
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.executescript('''
          CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value BLOB NOT NULL);
          CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY, pair TEXT NOT NULL, observed REAL NOT NULL, body BLOB NOT NULL);
          CREATE TABLE IF NOT EXISTS bars(
            pair TEXT NOT NULL, t INTEGER NOT NULL, body BLOB NOT NULL,
            input_hash TEXT NOT NULL, source_row_hash TEXT NOT NULL, receipt_id TEXT NOT NULL,
            first_observed REAL NOT NULL, PRIMARY KEY(pair,t),
            FOREIGN KEY(receipt_id) REFERENCES receipts(id));
          CREATE TABLE IF NOT EXISTS observations(
            pair TEXT NOT NULL,t INTEGER NOT NULL,feature_hash TEXT NOT NULL,values_blob BLOB NOT NULL,
            feature_count INTEGER NOT NULL,available_count INTEGER NOT NULL,published REAL NOT NULL,
            PRIMARY KEY(pair,t),FOREIGN KEY(pair,t) REFERENCES bars(pair,t));
          CREATE TABLE IF NOT EXISTS outcomes(
            pair TEXT NOT NULL,t INTEGER NOT NULL,horizon INTEGER NOT NULL,target INTEGER NOT NULL,
            state TEXT NOT NULL,body BLOB NOT NULL,resolved REAL NOT NULL,
            PRIMARY KEY(pair,t,horizon),FOREIGN KEY(pair,t) REFERENCES observations(pair,t));
          CREATE TABLE IF NOT EXISTS revisions(
            pair TEXT NOT NULL,t INTEGER NOT NULL,old_hash TEXT NOT NULL,new_hash TEXT NOT NULL,
            detected REAL NOT NULL,receipt_id TEXT NOT NULL,
            PRIMARY KEY(pair,t,new_hash));
          CREATE TABLE IF NOT EXISTS panels(
            id TEXT PRIMARY KEY,t INTEGER NOT NULL,body BLOB NOT NULL,published REAL NOT NULL);
          CREATE INDEX IF NOT EXISTS receipts_pair ON receipts(pair,observed);
        ''')
        self.contract = contract
        raw = encoded({'schema': SCHEMA, 'contract': contract})
        stored = self.connection.execute('SELECT value FROM metadata WHERE key=?', ('contract',)).fetchone()
        if stored is not None and bytes(stored['value']) != raw:
            self.connection.close()
            raise ValueError('dataset_contract_changed_use_separate_dataset')
        if stored is None:
            with self.connection:
                self.connection.execute('INSERT INTO metadata VALUES(?,?)', ('contract', raw))
        self.names = tuple(contract['feature_names'])

    def close(self):
        self.connection.close()

    def size_bytes(self):
        return sum(p.stat().st_size for p in (self.path, Path(str(self.path)+'-wal'), Path(str(self.path)+'-shm')) if p.exists())

    def latest_time(self, pair):
        return self.connection.execute('SELECT MAX(t) FROM observations WHERE pair=? AND published>0', (pair,)).fetchone()[0]

    def ingest(self, pair, data, features, receipt, *, keep_from=None, published_epoch=None):
        if tuple(features) != self.names:
            raise ValueError('ordered_feature_schema_mismatch')
        count = len(data['time'])
        if any(len(data[k]) != count for k in INPUT_FIELDS) or any(len(a) != count for a in features.values()):
            raise ValueError('unaligned_feature_or_input_rows')
        # Conservative reservation includes SQLite/index/JSON overhead. A batch
        # is refused before mutation rather than overshooting the configured cap.
        reserve = count * (len(self.names)*32 + 2048) * 2
        if self.size_bytes() + reserve >= self.max_bytes:
            raise ValueError('dataset_storage_bound')
        observed = float(receipt['observed_epoch'])
        published = time.time() if published_epoch is None else float(published_epoch)
        if published < observed:
            raise ValueError('publication_before_read')
        rows = []
        source_hashes = receipt.get('row_hashes')
        if source_hashes is not None and len(source_hashes) != count:
            raise ValueError('source_hash_alignment')
        for i in range(count):
            t = int(data['time'][i])
            if t + 60 > observed:
                raise ValueError('incomplete_bar_at_observation')
            row = {k: int(data[k][i]) if k == 'time' else float(data[k][i]) for k in INPUT_FIELDS}
            raw = encoded(row)
            h = digest(raw)
            rows.append((t, raw, h, source_hashes[i] if source_hashes else h, i))
        saved_receipt = {k: v for k,v in receipt.items() if k not in (
            'row_hashes','row_value_hashes','completion_basis','available_epoch')}
        saved_receipt.update(row_count=count, source_rows_hash=digest(encoded(source_hashes)) if source_hashes else None)
        receipt_bytes = encoded(saved_receipt)
        receipt_id = digest(receipt_bytes)
        # Inspect every overlapping input, even warmup rows not emitted now.
        existing = {}
        if rows:
            existing = {r['t']: r['input_hash'] for r in self.connection.execute(
                'SELECT t,input_hash FROM bars WHERE pair=? AND t>=? AND t<=?', (pair,rows[0][0],rows[-1][0]))}
        changes = [(t,existing[t],h) for t,raw,h,source_h,i in rows if t in existing and existing[t] != h]
        present = {t for t,raw,h,source_h,i in rows}
        changes.extend((t,old,digest(b'consumed_bar_removed_from_source'))
                       for t,old in existing.items() if t not in present)
        if changes:
            with self.connection:
                self.connection.execute('INSERT OR IGNORE INTO receipts VALUES(?,?,?,?)', (receipt_id,pair,observed,receipt_bytes))
                self.connection.executemany('INSERT OR IGNORE INTO revisions VALUES(?,?,?,?,?,?)',
                    [(pair,t,old,new,published,receipt_id) for t,old,new in changes])
            raise RevisionDetected('original_input_revision_detected')
        if self.connection.execute('SELECT 1 FROM revisions WHERE pair=? LIMIT 1',(pair,)).fetchone():
            raise RevisionDetected('unresolved_original_input_revision')
        inserted = 0
        added_times=[]
        with self.connection:
            self.connection.execute('INSERT OR IGNORE INTO receipts VALUES(?,?,?,?)', (receipt_id,pair,observed,receipt_bytes))
            for t,raw,h,source_h,i in rows:
                self.connection.execute('INSERT OR IGNORE INTO bars VALUES(?,?,?,?,?,?,?)', (pair,t,raw,h,source_h,receipt_id,observed))
                if keep_from is not None and t < keep_from:
                    continue
                existing_obs=self.connection.execute('SELECT published,feature_hash FROM observations WHERE pair=? AND t=?',(pair,t)).fetchone()
                if existing_obs is not None and existing_obs['published']>0:
                    continue
                values = [finite(features[name][i]) for name in self.names]
                body = encoded(values)
                if existing_obs is not None:
                    if existing_obs['published']==0:
                        if existing_obs['feature_hash']!=digest(body):
                            raise ValueError('unpublished_observation_input_context_changed')
                        added_times.append(t)
                    continue
                self.connection.execute('INSERT INTO observations VALUES(?,?,?,?,?,?,?)',
                    (pair,t,digest(body),zlib.compress(body),len(values),sum(v is not None for v in values),0.))
                added_times.append(t)
                inserted += 1
        # Mark availability only after the values committed. A process crash
        # between these transactions leaves unpublished rows explicitly hidden.
        committed=time.time() if published_epoch is None else published
        if committed < observed or committed < published:
            raise ValueError('clock_moved_backward_before_publication')
        with self.connection:
            self.connection.executemany('UPDATE observations SET published=? WHERE pair=? AND t=? AND published=0',
                                        [(committed,pair,t) for t in added_times])
        return {'inserted_observations': inserted,'source_rows': count,'receipt_id':receipt_id}

    def settle(self, pair, *, now_epoch=None):
        if self.connection.execute('SELECT 1 FROM revisions WHERE pair=? LIMIT 1',(pair,)).fetchone():
            raise RevisionDetected('unresolved_original_input_revision')
        now = time.time() if now_epoch is None else float(now_epoch)
        latest = self.connection.execute('SELECT MAX(t) FROM bars WHERE pair=? AND first_observed<=?',(pair,now)).fetchone()[0]
        if latest is None:
            return 0
        earliest = self.connection.execute('SELECT MIN(t) FROM observations WHERE pair=?',(pair,)).fetchone()[0]
        if earliest is None:
            return 0
        pending = []
        for h in HORIZONS:
            pending.extend((r['t'],h) for r in self.connection.execute('''
                SELECT o.t FROM observations o LEFT JOIN outcomes y
                ON y.pair=o.pair AND y.t=o.t AND y.horizon=?
                WHERE o.pair=? AND o.published>0 AND o.published<=? AND o.t+?<=? AND y.t IS NULL ORDER BY o.t''',(h,pair,now,h*60, min(latest,int(now)-60))))
        if not pending:
            return 0
        lower = min(t for t,h in pending)
        upper = max(t+h*60 for t,h in pending)
        bars = {r['t']:(json.loads(r['body']),r['first_observed']) for r in self.connection.execute(
            'SELECT t,body,first_observed FROM bars WHERE pair=? AND t BETWEEN ? AND ?',(pair,lower,upper))}
        results=[]
        for t,h in sorted(pending):
            target=t+h*60
            path=[bars.get(at) for at in range(t,target+1,60)]
            if any(row is not None and row[1]>now for row in path):
                # A wall-clock rollback cannot make an outcome available
                # before the source observations it actually consumed.
                continue
            missing=sum(r is None for r in path)
            state='missing_target' if path[-1] is None else 'gap_in_path' if missing else 'available'
            body={'target_bar_start_epoch':target,'target_bar_end_epoch':target+60,
                  'horizon_minutes':h,'path_expected_bars':h+1,'path_missing_bars':missing,
                  'outcome_scope':'historical_bidask_close_endpoint_proxy_no_fills_or_slippage',
                  'availability_basis':'bar_end_assumption_for_retrospective_labels; actual first_observed retained separately'}
            if state=='available':
                start=path[0][0];end=path[-1][0]
                ret=(end['close']/start['close']-1)*10000
                future=[r[0] for r in path[1:]]
                body.update(direction=int(np.sign(ret)),return_bps=ret,absolute_return_bps=abs(ret),
                    long_net_bps=(end['bid_close']-start['ask_close'])/start['close']*10000,
                    short_net_bps=(start['bid_close']-end['ask_close'])/start['close']*10000,
                    max_favorable_long_close_bps=max((r['bid_close']-start['ask_close'])/start['close']*10000 for r in future),
                    max_adverse_long_close_bps=min((r['bid_close']-start['ask_close'])/start['close']*10000 for r in future),
                    max_favorable_short_close_bps=max((start['bid_close']-r['ask_close'])/start['close']*10000 for r in future),
                    max_adverse_short_close_bps=min((start['bid_close']-r['ask_close'])/start['close']*10000 for r in future),
                    outcome_inputs_observed_epoch=max(r[1] for r in path))
            results.append((pair,t,h,target,state,encoded(body),now))
        with self.connection:
            self.connection.executemany('INSERT INTO outcomes VALUES(?,?,?,?,?,?,?)',results)
        return len(results)

    def tail_inputs(self,pair,limit):
        rows=self.connection.execute('SELECT body FROM bars WHERE pair=? ORDER BY t DESC LIMIT ?',(pair,limit)).fetchall()
        records=[json.loads(r['body']) for r in reversed(rows)]
        return {key:np.asarray([r[key] for r in records],dtype=np.int64 if key=='time' else np.float64) for key in INPUT_FIELDS}

    def latest(self,pair):
        row=self.connection.execute('SELECT * FROM observations WHERE pair=? AND published>0 ORDER BY t DESC LIMIT 1',(pair,)).fetchone()
        if row is None:
            return None
        values=json.loads(zlib.decompress(row['values_blob']))
        return {'pair':pair,'bar_start_epoch':row['t'],'bar_end_epoch':row['t']+60,
                'published_epoch':row['published'],'available_features':row['available_count'],
                'feature_count':row['feature_count'],'feature_hash':row['feature_hash'],
                'values':dict(zip(self.names,values))}

    def publish_panel(self, panel, source_observations):
        """Preserve each exact-clock generation, including later peer arrivals.

        Sources bind to already committed feature observations. Availability
        is recorded after the panel body commits, never backdated to bar end.
        """
        target=int(panel['target_bar_start_epoch'])
        sources={pair:{k:row[k] for k in ('bar_start_epoch','feature_hash','published_epoch')}
                 for pair,row in sorted(source_observations.items()) if row is not None}
        for pair,row in sources.items():
            existing=self.connection.execute('SELECT feature_hash,published FROM observations WHERE pair=? AND t=?',
                                             (pair,row['bar_start_epoch'])).fetchone()
            if existing is None or existing['published']<=0 or existing['feature_hash']!=row['feature_hash'] or existing['published']!=row['published_epoch']:
                raise ValueError('panel_source_not_published_observation')
        body=encoded({'panel':panel,'source_observations':sources})
        identifier=digest(body)
        packed=zlib.compress(body)
        if self.size_bytes()+len(packed)*2+65536>=self.max_bytes:
            raise ValueError('dataset_storage_bound')
        with self.connection:
            self.connection.execute('INSERT OR IGNORE INTO panels VALUES(?,?,?,?)',(identifier,target,packed,0.))
        committed=time.time()
        if committed < target+60 or any(committed < row['published_epoch'] for row in sources.values()):
            raise ValueError('clock_moved_backward_before_panel_publication')
        with self.connection:
            self.connection.execute('UPDATE panels SET published=? WHERE id=? AND published=0',(committed,identifier))
        published=self.connection.execute('SELECT published FROM panels WHERE id=?',(identifier,)).fetchone()[0]
        return {'id':identifier,'published_epoch':published,'target_bar_start_epoch':target,
                'source_observations':sources,'panel':panel}

    def counts(self):
        return {name:self.connection.execute('SELECT COUNT(*) FROM '+name).fetchone()[0]
                for name in ('bars','observations','outcomes','revisions','receipts','panels')}
