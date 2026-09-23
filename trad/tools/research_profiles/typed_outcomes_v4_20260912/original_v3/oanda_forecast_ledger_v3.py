"""Staged research ledger with per-pair terminal quote clocks.

Generated source appends the existing pure register/summary/close methods.
Only explicitly versioned databases are accepted; legacy ledgers are never
migrated. This module has no worker loop, publication or broker interfaces.
"""
from __future__ import annotations
from collections import Counter
import math
from pathlib import Path
import sqlite3
import time
from typing import Any
try:
    from oanda_outcome_clock_v2 import CONTRACT, build_market_quote_snapshot, number, quote_for_maturity, validate_snapshot
except ModuleNotFoundError:
    from trad.oanda_outcome_clock_v2 import CONTRACT, build_market_quote_snapshot, number, quote_for_maturity, validate_snapshot


def finite(value, default=0.0):
    try:
        result=float(value)
    except (TypeError,ValueError):
        return default
    return result if math.isfinite(result) else default


def validate_schema(connection):
    """Validate exact v2 layout without modifying the candidate database."""
    reference=sqlite3.connect(':memory:')
    try:
        reference.executescript(SCHEMA_SQL)
        query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        if connection.execute(query).fetchall()!=reference.execute(query).fetchall():
            raise ValueError('legacy_or_unknown_ledger_schema_refused')
    finally:
        reference.close()


LEDGER_CONTRACT='per_pair_nullable_forecast_outputs_v3_20260912'


class LiveForecastLedgerV3:
    def __init__(self, path: Path, retention_days: int = 30):
        self.path=Path(path)
        if not self.path.name.endswith('_v3.sqlite'):
            raise ValueError('versioned_v3_ledger_filename_required')
        self.retention_days=max(1,int(retention_days))
        self.path.parent.mkdir(parents=True,exist_ok=True)
        try:
            # Exclusive creation closes the exists/connect race: if another
            # owner won, its database must go through the read-only probe.
            with self.path.open('xb'):
                pass
            existed=False
        except FileExistsError:
            existed=True
        if existed:
            # Immutable read prevents legacy-WAL sidecars being opened/created.
            # Initial v2 setup checkpoints its static contract into the main DB.
            probe=sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro&immutable=1',uri=True)
            try:
                row=probe.execute('SELECT contract FROM outcome_clock_contract WHERE singleton=1').fetchone()
                if row != (LEDGER_CONTRACT,):
                    raise ValueError('legacy_or_unknown_ledger_refused')
                validate_schema(probe)
            except sqlite3.DatabaseError as exc:
                raise ValueError('legacy_or_unknown_ledger_refused') from exc
            finally:
                probe.close()
        self.connection=sqlite3.connect(self.path,timeout=5)
        try:
            self.connection.execute('PRAGMA busy_timeout=5000')
            if not existed:
                if self.connection.execute('PRAGMA journal_mode=WAL').fetchone()[0].lower()!='wal':
                    raise ValueError('wal_required')
                self.connection.execute('PRAGMA synchronous=FULL')
                self.connection.executescript(SCHEMA_SQL)
                self.connection.execute('INSERT INTO outcome_clock_contract VALUES (1,?,0)',(LEDGER_CONTRACT,))
                self.connection.commit()
                self.connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            else:
                if self.connection.execute('PRAGMA journal_mode').fetchone()[0].lower()!='wal':
                    raise ValueError('existing_v2_wal_required')
                self.connection.execute('PRAGMA synchronous=FULL')
                row=self.connection.execute('SELECT contract,wall_highwater FROM outcome_clock_contract WHERE singleton=1').fetchone()
                if not row or row[0]!=LEDGER_CONTRACT:
                    raise ValueError('ledger_contract_changed')
            self.wall_highwater=number(self.connection.execute('SELECT wall_highwater FROM outcome_clock_contract WHERE singleton=1').fetchone()[0])
            if self.wall_highwater<0:raise ValueError('invalid_ledger_highwater')
            self.pending_predictions=int(self.connection.execute('SELECT COUNT(*) FROM predictions').fetchone()[0])
            self.last_register_stats={'candidates':0,'points':0,'expired_points_skipped':0,'inserted_points':0}
        except BaseException:
            self.connection.close()
            raise

    def mature(self,snapshot,max_delay_sec,batch_size=100000,*,censor_missing_quotes=True,now=None):
        """Use each pair's own quote and retain explicit terminal censor rows.

        The old censor_missing_quotes argument remains accepted, but a missing
        quote never censors early: every forecast receives its declared window.
        `now` is injectable only for deterministic local tests/caller clocks.
        """
        current=time.time() if now is None else number(now)
        current,_,_=validate_snapshot(snapshot,now=current)
        limit=number(max_delay_sec)
        if limit<0 or type(batch_size) is not int or not 1<=batch_size<=100000:
            raise ValueError('invalid_maturity_limits')
        if current<self.wall_highwater:
            raise ValueError('maturity_wall_clock_rollback')
        columns=[row[1] for row in self.connection.execute('PRAGMA table_info(predictions)')]
        due=self.connection.execute('SELECT rowid,* FROM predictions WHERE target_epoch<=? ORDER BY target_epoch,rowid LIMIT ?',
                                    (current,batch_size)).fetchall()
        output=[];processed=[];reasons=Counter();matured=censored=pending=0
        for values in due:
            row=dict(zip(columns,values[1:]))
            target=number(row['target_epoch'])
            if abs(target-(number(row['generated_epoch'])+int(row['horizon_sec'])))>1e-6:
                raise ValueError('prediction_target_contract_mismatch')
            selected=quote_for_maturity(snapshot,row['instrument'],target_epoch=target,max_delay_sec=limit,now=current)
            if selected['status']=='pending':
                pending+=1;reasons[selected['reason']]+=1
                continue
            okay=selected['status']=='matured'
            processed.append(int(values[0]))
            result={key:row[key] for key in ('candidate_id','horizon_sec','family','model_id','instrument','input_timeframe',
                'generated_epoch','target_epoch','direction','probability_up','predicted_signed_pips','predicted_magnitude_pips',
                'entry_bid','entry_ask','entry_mid','entry_spread_pips','pip','artifact_sha256','snapshot_id','entry_quote_epoch','entry_quote_time','source_capture_epoch',
                'forecast_observed_epoch','prediction_output_kind','probability_semantics','probability_calibration_status')}
            result.update({'observed_epoch':selected['quote_epoch'] if okay else current,
                'outcome_delay_sec':selected['outcome_delay_sec'],'quote_epoch':selected['quote_epoch'],
                'quote_time':selected['quote_time'],'received_epoch':selected['received_epoch'],
                'evaluated_epoch':current,'receipt_delay_sec':selected['receipt_delay_sec'],
                'evaluation_delay_sec':selected['evaluation_delay_sec'],'clock_contract':CONTRACT,
                'direction_metric_contract':'binary_up_else_down_v1',
                'status':'matured' if okay else 'censored_no_eligible_quote_before_deadline',
                'censor_reason':selected['reason']})
            outcome_fields=('actual_up','direction_correct','executable_profitable','executable_net_pips','signed_mid_move_pips',
                'signed_pip_error','signed_pip_abs_error','magnitude_pip_error','magnitude_pip_abs_error','brier','exit_spread_pips')
            if okay:
                bid,ask=selected['bid'],selected['ask'];pip=number(row['pip'])
                if pip<=0:raise ValueError('positive_pip_required')
                signed=(.5*(bid+ask)-number(row['entry_mid']))/pip
                net=(bid-number(row['entry_ask']))/pip if row['direction']=='buy' else (number(row['entry_bid'])-ask)/pip
                actual=int(signed>0);signed_error=None if row['predicted_signed_pips'] is None else number(row['predicted_signed_pips'])-signed
                magnitude_error=None if row['predicted_magnitude_pips'] is None else number(row['predicted_magnitude_pips'])-abs(signed)
                result.update(dict(zip(outcome_fields,(actual,int((row['direction']=='buy')==bool(actual)),int(net>0),net,signed,
                    signed_error,None if signed_error is None else abs(signed_error),magnitude_error,None if magnitude_error is None else abs(magnitude_error),(number(row['probability_up'])-actual)**2,(ask-bid)/pip))))
                matured+=1
            else:
                result.update({field:None for field in outcome_fields})
                censored+=1;reasons[selected['reason']]+=1
            output.append(result)
        with self.connection:
            self.connection.execute('BEGIN IMMEDIATE')
            stored_highwater=number(self.connection.execute('SELECT wall_highwater FROM outcome_clock_contract WHERE singleton=1').fetchone()[0])
            if current<stored_highwater:raise ValueError('maturity_wall_clock_rollback')
            for row in output:
                keys=list(row)
                self.connection.execute('INSERT INTO outcomes ('+','.join(keys)+') VALUES ('+','.join('?' for _ in keys)+')',
                                        [row[key] for key in keys])
            self._delete_prediction_rowids(processed)
            self.connection.execute('UPDATE outcome_clock_contract SET wall_highwater=? WHERE singleton=1',(current,))
        self.wall_highwater=current
        self.pending_predictions=int(self.connection.execute('SELECT COUNT(*) FROM predictions').fetchone()[0])
        return {'matured':matured,'censored':censored,'pending_due':pending,'reasons':dict(reasons),
                'clock_contract':CONTRACT,'evaluated_epoch':current}

    def prune(self,now):
        # Receipts remain immutable and recoverable. Deleting terminal rows
        # without tombstones would let recovery resurrect completed identities.
        raise ValueError('v3_retention_requires_terminal_identity_tombstones')
    def register(self,candidates,snapshot_id,*,minimum_target_epoch=None):
        if not isinstance(candidates,list) or len(candidates)>1000:raise ValueError('bounded_forecast_batch_required')
        rows=[];points=expired=0
        for candidate in candidates:
            generated=number(candidate['forecast_generated_epoch'])
            bid,ask,pip=number(candidate['bid']),number(candidate['ask']),number(candidate['pip'])
            observed=number(candidate['research_forecast_observed_epoch'])
            entry=candidate['research_entry_clock']
            entry_epoch=number(entry['entry_quote_epoch']);capture=number(entry['source_capture_epoch'])
            if not 0<entry_epoch<=capture<=observed or generated>observed or entry_epoch>generated:
                raise ValueError('forecast_entry_observation_order')
            if bid<=0 or ask<bid or pip<=0:raise ValueError('invalid_forecast_entry')
            metadata=candidate.get('producer_metadata') or {}
            for raw_horizon,point in candidate['forecast_curve'].items():
                horizon=number(raw_horizon)
                if not horizon.is_integer() or horizon<=0:raise ValueError('integer_horizon_required')
                target=generated+horizon;points+=1
                if target<=observed:raise ValueError('forecast_observed_after_target')
                if minimum_target_epoch is not None and target<=number(minimum_target_epoch):expired+=1;continue
                probability=number(point['probability_up'])
                if not 0<=probability<=1 or point['direction'] not in ('buy','sell'):raise ValueError('invalid_probability_direction')
                signed=None if point.get('predicted_signed_pips') is None else number(point['predicted_signed_pips'])
                magnitude=None if point.get('predicted_magnitude_pips') is None else number(point['predicted_magnitude_pips'])
                if magnitude is not None and magnitude<0:raise ValueError('negative_predicted_magnitude')
                row={'candidate_id':candidate['id'],'horizon_sec':int(horizon),'family':candidate['family'],
                     'model_id':candidate['model_id'],'instrument':candidate['instrument'],'input_timeframe':candidate['input_timeframe'],
                     'generated_epoch':generated,'target_epoch':target,'direction':point['direction'],'probability_up':probability,
                     'predicted_signed_pips':signed,'predicted_magnitude_pips':magnitude,
                     'entry_bid':bid,'entry_ask':ask,'entry_mid':.5*(bid+ask),'entry_spread_pips':(ask-bid)/pip,'pip':pip,
                     'artifact_sha256':str(metadata.get('artifact_sha256') or ''),'snapshot_id':snapshot_id,
                     'entry_quote_epoch':entry_epoch,'entry_quote_time':entry['entry_quote_time'],'source_capture_epoch':capture,
                     'forecast_observed_epoch':observed,'prediction_output_kind':point['prediction_output_kind'],
                     'probability_semantics':point['probability_semantics'],
                     'probability_calibration_status':'not_established_for_signed_direction'}
                rows.append(row)
        inserted=0
        highwater=max([self.wall_highwater]+[row['forecast_observed_epoch'] for row in rows])
        with self.connection:
            self.connection.execute('BEGIN IMMEDIATE')
            highwater=max(highwater,number(self.connection.execute('SELECT wall_highwater FROM outcome_clock_contract WHERE singleton=1').fetchone()[0]))
            for row in rows:
                columns=list(row);parameters=[row[key] for key in columns]
                existing=None
                for table in ('predictions','outcomes'):
                    existing=self.connection.execute('SELECT '+','.join(columns)+' FROM '+table+' WHERE candidate_id=? AND horizon_sec=?',
                                                     (row['candidate_id'],row['horizon_sec'])).fetchone()
                    if existing is not None:break
                if existing is not None:
                    if tuple(parameters)!=existing:raise ValueError('immutable_ledger_forecast_identity_collision')
                    continue
                self.connection.execute('INSERT INTO predictions ('+','.join(columns)+') VALUES ('+','.join('?' for _ in columns)+')',parameters)
                inserted+=1
            self.connection.execute('UPDATE outcome_clock_contract SET wall_highwater=? WHERE singleton=1',(highwater,))
        self.wall_highwater=highwater
        self.pending_predictions=int(self.connection.execute('SELECT COUNT(*) FROM predictions').fetchone()[0])
        self.last_register_stats={'candidates':len(candidates),'points':points,'expired_points_skipped':expired,'inserted_points':inserted}
        return inserted

    def _delete_prediction_rowids(
        self,
        rowids: list[int],
        *,
        chunk_size: int = 1000,
    ) -> None:
        for offset in range(0, len(rowids), max(1, int(chunk_size))):
            chunk = rowids[offset : offset + max(1, int(chunk_size))]
            placeholders = ",".join("?" for _ in chunk)
            self.connection.execute(
                f"DELETE FROM predictions WHERE rowid IN ({placeholders})",
                chunk,
            )

    def summary(self,window_days=7,*,now=None):
        current=time.time() if now is None else number(now)
        cursor=self.connection.execute('''SELECT family,model_id,artifact_sha256,input_timeframe,horizon_sec,prediction_output_kind,
                probability_semantics,probability_calibration_status,COUNT(*) n,
                AVG(direction_correct) direction_accuracy,AVG(executable_profitable) executable_win_rate,
                AVG(executable_net_pips) average_net_pips,SUM(executable_net_pips) sum_net_pips,
                AVG(brier) mapped_probability_brier,COUNT(signed_pip_error) signed_error_n,
                AVG(signed_pip_abs_error) signed_pip_mae,AVG(signed_pip_error) signed_pip_bias,
                COUNT(magnitude_pip_error) magnitude_error_n,AVG(magnitude_pip_abs_error) magnitude_pip_mae,
                AVG(magnitude_pip_error) magnitude_pip_bias FROM outcomes
                WHERE observed_epoch>=? AND status='matured'
                GROUP BY family,model_id,artifact_sha256,input_timeframe,horizon_sec,prediction_output_kind,
                         probability_semantics,probability_calibration_status''',
                (current-max(1,int(window_days))*86400,))
        names=[row[0] for row in cursor.description]
        cells=[dict(zip(names,row)) for row in cursor.fetchall()]
        for row in cells:
            row['signed_error_status']='available' if row['signed_error_n'] else 'unavailable_not_predicted'
            row['magnitude_error_status']='available' if row['magnitude_error_n'] else 'unavailable_not_predicted'
        return {'database':str(self.path.resolve()),'ledger_contract':LEDGER_CONTRACT,'clock_contract':CONTRACT,
                'pending':self.pending_predictions,'cells':cells,
                'direction_metric_contract':'binary_up_else_down_v1',
                'brier_semantics':'prospective score of supplied mapping; not source calibration evidence'}

    def close(self) -> None:
        self.connection.close()


SCHEMA_SQL = '\n            CREATE TABLE IF NOT EXISTS predictions (\n                candidate_id TEXT NOT NULL,\n                horizon_sec INTEGER NOT NULL,\n                family TEXT NOT NULL,\n                model_id TEXT NOT NULL,\n                instrument TEXT NOT NULL,\n                input_timeframe TEXT NOT NULL,\n                generated_epoch REAL NOT NULL,\n                target_epoch REAL NOT NULL,\n                direction TEXT NOT NULL,\n                probability_up REAL NOT NULL,\n                predicted_signed_pips REAL,\n                predicted_magnitude_pips REAL,\n                entry_bid REAL NOT NULL,\n                entry_ask REAL NOT NULL,\n                entry_mid REAL NOT NULL,\n                entry_spread_pips REAL NOT NULL,\n                pip REAL NOT NULL,\n                artifact_sha256 TEXT NOT NULL,\n                snapshot_id TEXT NOT NULL,\n                entry_quote_epoch REAL NOT NULL, entry_quote_time TEXT NOT NULL, source_capture_epoch REAL NOT NULL,\n                forecast_observed_epoch REAL NOT NULL, prediction_output_kind TEXT NOT NULL,\n                probability_semantics TEXT NOT NULL, probability_calibration_status TEXT NOT NULL,\n                PRIMARY KEY(candidate_id, horizon_sec)\n            );\n            CREATE INDEX IF NOT EXISTS model_gap_prediction_maturity\n                ON predictions(target_epoch, model_id, input_timeframe, horizon_sec);\n            CREATE TABLE IF NOT EXISTS outcomes (\n                candidate_id TEXT NOT NULL,\n                horizon_sec INTEGER NOT NULL,\n                family TEXT NOT NULL,\n                model_id TEXT NOT NULL,\n                instrument TEXT NOT NULL,\n                input_timeframe TEXT NOT NULL,\n                generated_epoch REAL NOT NULL,\n                target_epoch REAL NOT NULL,\n                observed_epoch REAL NOT NULL,\n                outcome_delay_sec REAL,\n                direction TEXT NOT NULL,\n                probability_up REAL NOT NULL,\n                predicted_signed_pips REAL,\n                predicted_magnitude_pips REAL,\n                actual_up INTEGER,\n                direction_correct INTEGER,\n                executable_profitable INTEGER,\n                executable_net_pips REAL,\n                signed_mid_move_pips REAL,\n                signed_pip_error REAL,\n                signed_pip_abs_error REAL,\n                magnitude_pip_error REAL,\n                magnitude_pip_abs_error REAL,\n                brier REAL,\n                entry_bid REAL NOT NULL,\n                entry_ask REAL NOT NULL,\n                entry_mid REAL NOT NULL,\n                entry_spread_pips REAL NOT NULL,\n                exit_spread_pips REAL,\n                pip REAL NOT NULL,\n                artifact_sha256 TEXT NOT NULL,\n                snapshot_id TEXT NOT NULL,\n                entry_quote_epoch REAL NOT NULL, entry_quote_time TEXT NOT NULL, source_capture_epoch REAL NOT NULL,\n                forecast_observed_epoch REAL NOT NULL, prediction_output_kind TEXT NOT NULL,\n                probability_semantics TEXT NOT NULL, probability_calibration_status TEXT NOT NULL,\n                status TEXT NOT NULL,\n                quote_epoch REAL, quote_time TEXT, received_epoch REAL, evaluated_epoch REAL NOT NULL,\n                receipt_delay_sec REAL, evaluation_delay_sec REAL NOT NULL,\n                clock_contract TEXT NOT NULL, direction_metric_contract TEXT NOT NULL, censor_reason TEXT,\n                PRIMARY KEY(candidate_id, horizon_sec)\n            );\n            CREATE INDEX IF NOT EXISTS model_gap_outcome_scope\n                ON outcomes(observed_epoch, model_id, input_timeframe, horizon_sec);\n            \nCREATE TABLE outcome_clock_contract(singleton INTEGER PRIMARY KEY CHECK(singleton=1),contract TEXT NOT NULL,wall_highwater REAL NOT NULL);\n'
