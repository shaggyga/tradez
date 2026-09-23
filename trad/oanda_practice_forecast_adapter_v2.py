"""Read-only native V7 publication adapter for a separately bounded practice trial.

No broker calls or research writes. Original native anchor, endpoint and model
probability are preserved; this adapter does not infer remaining probabilities.
"""
from __future__ import annotations

from contextlib import closing
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time

SCHEMA='practice_native_forecast_adapter_v2_20260913'
FAMILY='ridge_price_news_v1'
FLAGS=('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible')
SOURCE_KEYS={'registry_path','registry_sha256','study_path','activation_sha256'}


class AdapterError(ValueError):pass


def need(ok,reason):
    if not ok:raise AdapterError(reason)


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(value):return hashlib.sha256(encoded(value)).hexdigest()


def epoch(value):
    need(type(value) in (int,float) and math.isfinite(value) and value>0,'invalid_clock')
    return value


def plain(root,relative):
    need(type(relative) is str and len(relative)<=512,'source_path_shape')
    base=Path(root).absolute();candidate=Path(relative)
    need(not candidate.is_absolute() and '..' not in candidate.parts,'relative_source_path_required')
    path=base/candidate
    for cursor in (path,*path.parents):
        need(not cursor.is_symlink() and not cursor.is_junction(),'source_reparse_refused')
    need(path.resolve().is_relative_to(base.resolve()),'source_outside_project')
    return path


def raw_json(path,limit=1024*1024):
    with Path(path).open('rb') as handle:raw=handle.read(limit+1)
    need(0<len(raw)<=limit,'source_size_bound')
    def unique(items):
        out={}
        for k,v in items:
            need(k not in out,'duplicate_source_key');out[k]=v
        return out
    value=json.loads(raw,object_pairs_hook=unique,parse_constant=lambda _:(_ for _ in ()).throw(AdapterError('nonfinite_source')))
    need(type(value) is dict,'source_object_required')
    return raw,value


def validate_source_config(project_root,source):
    need(type(source) is dict and set(source)==SOURCE_KEYS,'native_source_config_shape')
    for key in ('registry_sha256','activation_sha256'):
        need(type(source[key]) is str and re.fullmatch('[a-f0-9]{64}',source[key]) is not None,'source_hash_required')
    registry_path=plain(project_root,source['registry_path']);study=plain(project_root,source['study_path'])
    need(study.name=='joint_price_news_study_v7','native_study_required')
    raw,registry=raw_json(registry_path)
    need(hashlib.sha256(raw).hexdigest()==source['registry_sha256'],'registry_bytes_changed')
    # Imports are read-only; the original worker validates its exact45-source
    # kit, native contracts, input closure and installed numeric dependencies.
    import oanda_joint_price_news_forecast_study_v7 as worker
    need(Path(worker.__file__).resolve()==Path(project_root).resolve()/'oanda_joint_price_news_forecast_study_v7.py','mixed_worker_source')
    need(worker.load_registry(registry_path)==registry,'registered_source_changed')
    receipt_path=plain(project_root,str(Path(source['study_path'])/'activation_receipt.json'))
    raw,activation=raw_json(receipt_path)
    need(hashlib.sha256(raw).hexdigest()==source['activation_sha256'],'activation_bytes_changed')
    need(activation.get('registry_sha256')==digest(registry),'activation_registry_mismatch')
    need(activation.get('schema_version')=='joint_price_news_v7_activation_receipt_20260913'
         and activation.get('status')=='activated_empty'
         and Path(activation.get('study_root','')).resolve()==study.resolve()
         and activation.get('registry_file_sha256')==source['registry_sha256']
         and activation.get('source_bindings')==registry['source_bindings']
         and activation.get('research_only') is True
         and all(activation.get(k) is False for k in FLAGS),'native_activation_identity')
    return registry,activation,study


def candidate_from_row(row,contract,*,activated_epoch,observed_epoch):
    """Pure translation after caller-verified original DB/source identity."""
    from oanda_fixed_forecast_evaluation_joint_news_v3 import forecast_errors
    value=json.loads(row['payload']);arm=value['forecasts'][0]
    need(digest(value)==row['sha'],'native_forecast_immutable_digest')
    need(row['id']==value['decision_id']==digest({'contract':digest(contract),'bucket':row['bucket']}),'native_decision_identity')
    need(row['attempt_id']==value['attempt_id'] and row['attempt_epoch']==value['attempt_epoch']
         and row['reference_id']==value['reference_quote_id'],'native_attempt_identity')
    published,consumed=epoch(row['published']),epoch(row['consumed']);observed=epoch(observed_epoch)
    need(row['publication_sha']==row['sha']==row['consumption_sha']
         and row['consumption_publication_sha']==digest({'epoch':published,'forecast_sha':row['sha']}),'native_receipt_identity')
    need(activated_epoch<row['attempt_epoch']<=arm['computation_started_epoch']
         and arm['issued_epoch']<=published<=consumed<=observed,'native_publication_clock_order')
    errors=forecast_errors({**arm,'committed_available_epoch':consumed},value,contract['evaluation_protocol'])
    need(not errors,'native_forecast_contract_invalid')
    need(value.get('research_only') is True and all(value.get(k) is False for k in FLAGS),'research_authority_changed')
    need(0<=observed-arm['issued_epoch']<=900,'original_forecast_stale')
    need(value['target_epoch']-observed>=60,'original_target_too_near_or_elapsed')
    anchor=arm['native_anchor'];side=arm['side']
    need(type(side) is int and side in (-1,1),'original_forecast_neutral')
    # The stored binary64 endpoint is authoritative. Recomputing via bps would
    # change its exact arithmetic and falsely relabel the original model.
    reference=format(Decimal(repr(float.fromhex(anchor['origin_mid_hex']))),'f')
    terminal=format(Decimal(repr(float.fromhex(anchor['expected_endpoint_mid_hex']))),'f')
    probability=format(Decimal(repr(float.fromhex(anchor['native_probability_up_hex']))),'f')
    need((Decimal(terminal)-Decimal(reference))*side>0,'original_direction_terminal_mismatch')
    return dict(instrument=value['instrument'],side=side,reference_epoch=anchor['origin_close_epoch'],
        reference_price=reference,original_target_epoch=anchor['target_close_epoch'],
        expected_terminal_price=terminal,probability_up=probability,issued_epoch=arm['issued_epoch'],
        available_epoch=consumed,publication_epoch=published,forecast_sha256=row['sha'],
        publication_receipt_sha256=digest({'epoch':published,'forecast_sha':row['sha']}),
        consumer_receipt_sha256=digest({'id':row['id'],'epoch':consumed,'forecast_sha':row['sha'],
            'publication_sha':row['consumption_publication_sha']}),
        native_anchor=anchor,native_anchor_sha256=arm['native_anchor_sha256'],
        native_probability_event=anchor['native_probability_event'],remaining_probability_available=False,
        adapter_observed_epoch=observed,source_authority={k:value.get(k) for k in ('research_only',*FLAGS)},
        experimental_practice_candidate=True,can_place_orders=False,can_authorize=False,
        interpretation='Original uncalibrated native H1 forecast; separate conservative practice policy required.')


def read_candidates(project_root,now_epoch,*,source_config,instruments=None,clock=time.time):
    started=epoch(clock());epoch(now_epoch)
    need(0<=started-now_epoch<=90,'adapter_request_clock')
    registry,activation,study=validate_source_config(project_root,source_config)
    activated=epoch(activation['activation_completed_epoch'])
    need(activated<=started,'future_activation')
    wanted=sorted(registry['pairs']) if instruments is None else sorted(instruments)
    need(1<=len(wanted)<=68 and len(set(wanted))==len(wanted) and all(p in registry['pairs'] for p in wanted),'pair_selection')
    signals=[];rejected=[];receipts=[];deadline=time.monotonic()+20
    for pair in wanted:
        try:
            need(time.monotonic()<=deadline,'adapter_read_budget')
            spec=registry['pairs'][pair]['families'][FAMILY];contract=spec['contract']
            path=plain(project_root,str(Path(source_config['study_path'])/'pairs'/pair/FAMILY/'study.sqlite'))
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
                db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
                saved=db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
                active=db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
                need(saved is not None and saved['sha']==spec['contract_sha256']==digest(contract)
                     and saved['payload']==encoded(contract).decode(),'activated_contract_mismatch')
                need(active is not None and active['contract_sha']==spec['contract_sha256']
                     and 0<epoch(active['epoch'])<=activated,'pair_activation_mismatch')
                row=db.execute('''SELECT f.id,f.bucket,f.attempt_id,f.payload,f.sha,a.epoch attempt_epoch,a.reference_id,
                    p.epoch published,p.forecast_sha publication_sha,c.epoch consumed,
                    c.forecast_sha consumption_sha,c.publication_sha consumption_publication_sha
                    FROM forecasts f JOIN attempts a ON a.id=f.attempt_id
                    LEFT JOIN publication p ON p.id=f.id LEFT JOIN consumption c ON c.id=f.id
                    ORDER BY f.bucket DESC LIMIT 1''').fetchone()
                need(row is not None,'no_published_native_forecast')
                need(len(row['payload'].encode())<=512*1024,'native_forecast_size')
                observed=epoch(clock())
                signals.append(candidate_from_row(row,contract,activated_epoch=active['epoch'],observed_epoch=observed))
                receipts.append({'instrument':pair,'row_sha256':digest(dict(row)),'observed_epoch':observed,
                    'query_only':True,'contract_sha256':spec['contract_sha256']})
        except (ValueError,KeyError,TypeError,sqlite3.Error,OSError) as exc:
            code=str(exc) if isinstance(exc,AdapterError) else 'native_source_unavailable'
            rejected.append({'instrument':pair,'reason':code})
    # Source/config changes during the read cannot leave eligible candidates.
    validate_source_config(project_root,source_config)
    completed=epoch(clock());need(started<=completed and completed-started<=30,'adapter_completion_budget')
    result=dict(schema_version=SCHEMA,started_epoch=started,completed_epoch=completed,
        source_config=dict(source_config),registry_sha256=digest(registry),signals=signals,rejected=rejected,
        receipts=receipts,research_only=True,can_place_orders=False,can_authorize=False,
        probability_scope='uncalibrated_native_original_target_no_remaining_probability')
    result['payload_sha256']=digest(result)
    return result
