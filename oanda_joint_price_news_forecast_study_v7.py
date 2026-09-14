"""Separate v7 fixed revision-news joint research cohort with immutable summary publication.

Reuses the retained weekend fair scheduler and unchanged numeric model.
Status coherence does not establish forecast or trading eligibility.

Each registered pair owns a separate immutable ledger and cadence. No broker
API, execution feed, lifecycle, promotion or order capability is imported.
"""
from __future__ import annotations
import argparse
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path, PureWindowsPath
import platform
import re
import sqlite3
import stat
import sys
import time

import oanda_immutable_summary_publication_v1 as summary_boundary

from oanda_causal_forecast_ledger_joint_news_v5 import CausalForecastLedger, digest, encoded, validate_contract, number, utc, _bound_inputs
import revision_worker_schedule_v1 as schedule
from oanda_fixed_forecast_evaluation_joint_news_v3 import evaluate, forecast_errors
from oanda_fixed_forecast_evaluation_joint_news_v1 import jsonable, loads_exact_prices
from oanda_exact_price_scoring import decimal_value, quote_midpoint

SCHEDULING_VERSION='shared_revision_history_completion_cadence_and_fit_handoff_v4_20260914'

ROOT=Path(__file__).resolve().parent
DEFAULT_CONFIG=ROOT/'config/joint_price_news_study_v7_20260913_revision.json'
STUDY=ROOT/'data/oanda_training_manager/joint_price_news_study_v7'
FAMILIES=frozenset({'ridge_price_news_v1'})
REGISTRY_SCHEMA='joint_price_news_registry_v7_20260913'
SUMMARY_SCHEMA='joint_price_news_forecast_summary_v7_20260913'
HEARTBEAT_SCHEMA='joint_price_news_forecast_heartbeat_v7_20260913'
COHORT_SCOPE='fixed_revision_news_publication_v7_20260913'
REQUIRED_SOURCE_BINDINGS=frozenset({
 'compact_projection_store_v1.py',
 'joint_native_anchor_v1.py',
 'native_m1_ledger_v1.py',
 'native_m1_outcome_v1.py',
 'oanda_causal_forecast_inputs.py',
 'oanda_causal_forecast_inputs_gap_v2.py',
 'oanda_causal_forecast_inputs_joint_news_v3.py',
 'oanda_causal_forecast_inputs_pair_v2.py',
 'oanda_causal_forecast_ledger_joint_news_v3.py',
 'oanda_causal_forecast_ledger_joint_news_v5.py',
 'oanda_causal_prediction_baselines.py',
 'oanda_exact_price_scoring.py',
 'oanda_fixed_forecast_evaluation_joint_news_v1.py',
 'oanda_fixed_forecast_evaluation_joint_news_v3.py',
 'oanda_immutable_summary_publication_v1.py',
 'oanda_isolated_news_history_v1.py',
 'oanda_joint_price_news_forecast_study_v5.py',
 'oanda_joint_price_news_forecast_study_v7.py',
 'oanda_joint_price_news_models_v1.py',
 'oanda_local_news_sentiment.py',
 'oanda_local_news_sentiment_repair_v2.py',
 'oanda_news_causal_aggregation_guard_v1.py',
 'oanda_news_causal_aggregation_guard_v2.py',
 'oanda_news_classification_contract.py',
 'oanda_news_classification_observation_v1.py',
 'oanda_news_collector_contract.py',
 'oanda_news_event_tagger.py',
 'oanda_news_source_coverage.py',
 'oanda_news_source_observation_ledger_v1.py',
 'oanda_news_topic_identity_reconciliation_v2.py',
 'oanda_official_central_bank_coverage.py',
 'oanda_official_release_fast_lane_contract.py',
 'oanda_pair_local_models_v2.py',
 'oanda_source_governance.py',
 'oanda_source_governance_news_fast_lane.py',
 'projection_revision_admission_v1.py',
 'projection_revision_consumer_v1.py',
 'projection_revision_reader_v1.py',
 'revision_joint_features_v1.py',
 'revision_joint_inputs_v3.py',
 'revision_joint_point_v2.py',
 'revision_news_io_base_v1.py',
 'revision_news_io_v10.py',
 'revision_transport_v4.py',
 'revision_worker_schedule_v1.py',
 'shared_revision_history_v3.py',
})

def bounded_bytes(path,limit):
    with Path(path).open('rb') as handle:raw=handle.read(limit+1)
    if len(raw)>limit:raise ValueError('source_size_limit')
    return raw

def read_summary_bytes(path):
    # A readback size refusal is a counted status-publication error.
    # Keep ordinary registry/source-reader validation unchanged.
    try:return bounded_bytes(path,summary_boundary.MAX_BYTES)
    except ValueError as exc:
        if str(exc)!='source_size_limit':raise
        raise summary_boundary.SummaryBoundaryError('summary_boundary_readback_size_limit') from None


def atomic_json(path,value):
    return atomic_bytes(path,encoded(value))


def atomic_bytes(path,raw):
    if type(raw) is not bytes:raise TypeError("immutable_publication_bytes_required")
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+f'.{os.getpid()}.{time.time_ns()}.tmp')
    owned=False
    try:
        with temporary.open('xb') as handle:
            owned=True;handle.write(raw);handle.flush();os.fsync(handle.fileno())
        for attempt in range(8):
            try:os.replace(temporary,path);break
            except OSError as exc:
                if (not isinstance(exc,PermissionError) and getattr(exc,'winerror',None) not in (5,32,33)) or attempt==7:raise
                time.sleep(min(.01*2**attempt,.5))
    finally:
        if owned:
            try:temporary.unlink(missing_ok=True)
            except OSError:pass

def epoch(value):
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('timezone_required')
    return parsed.timestamp()

def plain_c_path(value):
    """Validate public C paths without resolving or probing through reparse points."""
    lexical=PureWindowsPath(str(value))
    if lexical.drive.lower()!='c:' or not lexical.is_absolute() or '..' in lexical.parts:
        raise ValueError('absolute_c_runtime_path_required')
    if any(':' in part for part in lexical.parts[1:]):raise ValueError('alternate_stream_runtime_path_refused')
    path=Path(value)
    for part in reversed((path,*path.parents)):
        try:info=os.lstat(part)
        except FileNotFoundError:break
        if getattr(info,'st_file_attributes',0)&0x400 or stat.S_ISLNK(info.st_mode):
            raise ValueError('reparse_runtime_path_refused')
    return path


def verify_source_kit(bindings):
    """Bounded physical hash and loaded-owner closure; caller prehashes before import."""
    if type(bindings) is not dict or set(bindings)!=REQUIRED_SOURCE_BINDINGS:
        raise ValueError('complete_source_bindings_required')
    root=plain_c_path(ROOT)
    for name,expected in bindings.items():
        if type(expected) is not str or re.fullmatch('[0-9a-f]{64}',expected) is None:
            raise ValueError('source_binding_sha256_required')
        path=plain_c_path(root/name);before=path.stat()
        raw=bounded_bytes(path,2*1024*1024);after=plain_c_path(path).stat()
        signature=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns)
        if signature(before)!=signature(after) or hashlib.sha256(raw).hexdigest()!=expected:
            raise ValueError('source_binding_mismatch:'+name)
        module=sys.modules.get(name[:-3])
        if module is not None and Path(getattr(module,'__file__','')).absolute()!=path:
            raise ValueError('worker_mixed_source_kit_module_refused:'+name)
    actual=_bound_inputs()._bindings()
    if any(bindings.get(name)!=sha for name,sha in actual.items()):
        raise ValueError('worker_fixed_input_source_closure_required')


def load_registry(path):
    path=plain_c_path(path)
    registry=json.loads(bounded_bytes(path,1024*1024))
    io_sha=registry.get('news_io_config_sha256')
    if type(io_sha) is not str or re.fullmatch('[0-9a-f]{64}',io_sha) is None:raise ValueError('exact_news_io_config_hash_required')
    if registry.get('schema_version')!=REGISTRY_SCHEMA or registry.get('registry_id')!='joint_price_news_study_v7_20260913':
        raise ValueError('registry_identity')
    if registry.get('collection_enabled') is not True or registry.get('research_only') is not True:
        raise ValueError('research_collection_required')
    for flag in ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported'):
        if registry.get(flag) is not False:raise ValueError('inert_registry_required')
    if set(registry.get('source_bindings',{}))!=REQUIRED_SOURCE_BINDINGS:raise ValueError('complete_source_bindings_required')
    verify_source_kit(registry['source_bindings'])
    dependencies=registry.get('dependency_versions',{})
    if set(dependencies)!={'python','numpy','scikit-learn'}:raise ValueError('complete_dependency_bindings_required')
    for package,expected in dependencies.items():
        actual=platform.python_version() if package=='python' else importlib.metadata.version(package)
        if actual!=expected:raise ValueError('dependency_binding_mismatch:'+package)
    pairs=registry.get('pairs',{})
    if not isinstance(pairs,dict) or not 1<=len(pairs)<=68:raise ValueError('bounded_pair_registry_required')
    cohorts=[]
    for pair,item in pairs.items():
        if not re.fullmatch(r'[A-Z]{3}_[A-Z]{3}',pair):raise ValueError('pair_identity')
        if set(item.get('families',{})) != FAMILIES:raise ValueError('registered_joint_family_slot_required')
        for family, slot in item['families'].items():
            contract=slot['contract'];validate_contract(contract)
            identity=contract.get('cohorts',{}).get(family,'')
            prefix=f'joint_price_news_native_v1_20260913.{pair}.{family}.{COHORT_SCOPE}.'
            if not isinstance(identity,str) or not identity.startswith(prefix) or not re.fullmatch(r'[a-z0-9][a-z0-9_]{0,63}',identity[len(prefix):]):
                raise ValueError('new_v7_cohort_scope_required')
            if contract.get('contract_id')!=identity or contract['evaluation_protocol'].get('contract_id')!=identity+'.evaluation':
                raise ValueError('new_v7_contract_identity_required')
            if contract['instrument']!=pair or contract['family']!=family or decimal_value(item['pip_size'])!=decimal_value(contract['pip_size']):
                raise ValueError('registered_pair_family_or_pip_mismatch')
            if digest(contract)!=slot['contract_sha256']:raise ValueError('contract_hash_mismatch')
            if contract.get('source_bindings')!=registry['source_bindings'] or contract.get('dependency_versions')!=dependencies:
                raise ValueError('pair_source_or_dependency_mismatch')
            if contract['numeric_model_source_sha256']!=registry['source_bindings']['oanda_joint_price_news_models_v1.py']:
                raise ValueError('numeric_model_binding_mismatch')
            if contract['model_version']!='sha256:'+registry['source_bindings']['oanda_joint_price_news_models_v1.py']:
                raise ValueError('model_version_source_binding_mismatch')
            if contract['feature_version']!='sha256:'+registry['source_bindings']['revision_joint_inputs_v3.py']:
                raise ValueError('feature_version_source_binding_mismatch')
            cohorts.extend(contract['cohorts'].values())
    if len(cohorts)!=len(set(cohorts)):raise ValueError('cohort_reuse')
    return registry


def activate_fresh_study(config=DEFAULT_CONFIG,*,study=STUDY,clock=time.time):
    """Explicit deployment-only activation; never starts a worker or imports rows.

    Registration preparation is not activation. Each new ledger samples its
    actual clock here after registration/source verification. Existing or
    partially activated roots are never reopened by this helper.
    """
    resolved=plain_c_path(study)
    if resolved.name!='joint_price_news_study_v7' or resolved.exists():
        raise ValueError('wholly_absent_v7_study_root_required')
    registry=load_registry(config)
    started=number(clock());records=[]
    if started<=0:raise ValueError('positive_activation_clock_required')
    # Explicit directory creation is an atomic refusal of a competing activation.
    resolved.mkdir(parents=True,exist_ok=False)
    try:
        for pair,item in sorted(registry['pairs'].items()):
            for family,spec in sorted(item['families'].items()):
                path=plain_c_path(resolved/'pairs'/pair/family/'study.sqlite')
                if not path.is_relative_to(resolved) or path.exists():
                    raise ValueError('new_ledger_path_required')
                ledger=CausalForecastLedger(path,spec['contract'],clock=clock,activate=True)
                try:
                    counts=ledger.counts()
                    if any(counts.values()):raise ValueError('new_ledger_must_be_empty')
                    if ledger.activated_epoch<started:raise ValueError('activation_clock_before_deployment')
                    records.append({'instrument':pair,'family':family,'path':str(path),
                        'contract_sha256':ledger.contract_hash,'activated_epoch':ledger.activated_epoch,'counts':counts})
                finally:ledger.close()
        if digest(load_registry(config))!=digest(registry):raise ValueError('registration_changed_during_activation')
        completed=number(clock())
        if any(r['activated_epoch']>completed for r in records):raise ValueError('activation_clock_after_completion')
        receipt={'schema_version':'joint_price_news_v7_activation_receipt_20260913','status':'activated_empty',
            'registry_sha256':digest(registry),'registry_path':str(Path(config).resolve()),
            'registry_file_sha256':hashlib.sha256(Path(config).read_bytes()).hexdigest(),
            'source_bindings':registry['source_bindings'],'activation_started_epoch':started,
            'activation_completed_epoch':completed,'study_root':str(resolved),'ledgers':records,
            'historical_rows_imported':False,'worker_started':False,'research_only':True,
            **{flag:False for flag in INERT_FLAGS}}
        atomic_json(resolved/'activation_receipt.json',receipt)
        return receipt
    except BaseException as exc:
        atomic_json(resolved/'activation_failed.json',{'schema_version':'joint_price_news_v7_failed_activation_20260913',
            'status':'partial_activation_requires_explicit_review','registry_sha256':digest(registry),
            'completed_ledgers':records,'error':type(exc).__name__+':'+str(exc)[:500],
            'historical_rows_imported':False,'worker_started':False})
        raise


def verify_activated_study(registry,study,*,clock=time.time):
    """Read-only preflight before a worker lock/directory or writer is opened."""
    root=plain_c_path(study);observed=number(clock())
    for pair,item in registry['pairs'].items():
        for family,spec in item['families'].items():
            path=plain_c_path(root/'pairs'/pair/family/'study.sqlite')
            if not path.is_relative_to(root) or not path.is_file():
                raise ValueError('complete_prior_v7_activation_required')
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
                db.execute('PRAGMA query_only=ON')
                saved=db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
                activation=db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
                if saved!=(spec['contract_sha256'],encoded(spec['contract']).decode()):
                    raise ValueError('activated_contract_mismatch')
                if activation is None or activation[1]!=spec['contract_sha256'] or not 0<number(activation[0])<=observed:
                    raise ValueError('actual_v7_activation_required')
    return True

def read_snapshot(path,*,clock=time.time):
    raw=bounded_bytes(path,262144);observed=number(clock())
    snapshot=loads_exact_prices(raw)
    if (not isinstance(snapshot,dict) or snapshot.get('producer')!='practice_007_dedicated_quote_stream'
        or not isinstance(snapshot.get('quotes'),dict) or not isinstance(snapshot.get('coverage'),dict)
        or not isinstance(snapshot['coverage'].get('retained_last_known_instruments'),list)):
        raise ValueError('quote_snapshot_identity_or_shape')
    retained=snapshot['coverage']['retained_last_known_instruments']
    if len(snapshot['quotes'])>68 or len(retained)>68 or any(not isinstance(pair,str) for pair in retained):
        raise ValueError('quote_snapshot_coverage_shape')
    if not 0<=observed-epoch(snapshot['generated_utc'])<=60:raise ValueError('stale_or_future_quote_snapshot')
    return snapshot,observed,hashlib.sha256(raw).hexdigest()

def pair_quote(snapshot,observed,source_hash,pair,pip):
    observed=number(observed)
    quote=snapshot['quotes'].get(pair)
    if not isinstance(quote,dict):raise ValueError('no_pair_quote')
    if quote.get('instrument',pair)!=pair:raise ValueError('quote_pair_identity_mismatch')
    if pair in snapshot['coverage']['retained_last_known_instruments']:raise ValueError('retained_quote_not_current')
    if quote.get('source')!='stream' or quote.get('tradeable') is not True:raise ValueError('market_closed_or_no_stream_quote')
    market=epoch(quote['time'])
    if not 0<=observed-market<=60:raise ValueError('no_fresh_quote')
    if decimal_value(quote['pip'])!=decimal_value(pip):raise ValueError('quote_pip_metadata_mismatch')
    if not 0<decimal_value(quote['bid'])<=decimal_value(quote['ask']):raise ValueError('quote_price_order')
    return {'instrument':pair,'pip_size':pip,'market_epoch':market,'available_epoch':observed,
        'bid':str(decimal_value(quote['bid'])),'ask':str(decimal_value(quote['ask'])),'tradeable':True,
        'provider_time':quote['time'],'raw_provider_quote':jsonable(quote),
        'source_snapshot_sha256':source_hash,'producer':snapshot['producer']}


INERT_FLAGS=('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported')


def write_scorecard(ledger,destination):
    dataset,protocol=ledger.export_evaluation()
    report=jsonable(evaluate(dataset,protocol))
    report.update(collection_counts=dataset['collection_counts'],study_contract_sha256=ledger.contract_hash,
        interpretation='Native M1 origin and H1 target research cohort. Original native-event probabilities are uncalibrated; static remaining-price views have no rebased probability or execution authority.')
    atomic_json(destination,report)
    return report


def verified_publication(ledger):
    """The fixed native ledger verifies forecast/publication/consumer evidence."""
    return ledger.verified_native_publication()


def observe_and_score_native(ledger,candle_path,clock_path,destination):
    """One serialized native source observation and score task, independent of news."""
    observation=ledger.observe_native_source(candle_path,clock_path=clock_path)
    ledger.settle()
    # Freeze only this owner's bounded descriptive result, never raw candle rows.
    raw=encoded(observation)
    if len(raw)>8192:raise ValueError('native_observation_diagnostic_bound')
    return {'observation':json.loads(raw),'scorecard':write_scorecard(ledger,destination)}


def input_readiness(capture,family,now):
    source_clocks={'price_bar_close_epoch':(capture or {}).get('max_bar_close_epoch'),
                   'news_evidence_epoch':(capture or {}).get('news_evidence_epoch'),
                   'news_expires_epoch':(capture or {}).get('news_expires_epoch')}
    if not capture:return {'observed_epoch':None,'status':'unavailable','reason':'awaiting_pair_input_read','diagnostics':{},**source_clocks}
    if capture.get('first_observed_epoch') is None:
        return {'observed_epoch':None,'status':'unavailable','reason':'|'.join(capture.get('reasons',[])) or 'pair_input_read_failed','diagnostics':{},**source_clocks}
    observed=number(capture['first_observed_epoch'])
    diagnostics=capture.get('family_readiness',{}).get(family,{})
    reason='|'.join(diagnostics.get('reasons',[]) or capture.get('reasons',[]))
    status='ready' if capture.get('status')=='ready' and diagnostics.get('ready') is True else 'blocked'
    maturity=capture.get('max_bar_close_epoch')
    if maturity is None or not 0<=now-number(maturity)<=900:
        status='unavailable';reason='stale_or_future_pair_input'
    evidence=capture.get('news_evidence_epoch');expires=capture.get('news_expires_epoch')
    if evidence is None or expires is None or not 0<=now-number(evidence)<=300 or now>number(expires):
        status='unavailable';reason='stale_or_unavailable_news_input'
    return {'observed_epoch':observed,'status':status,'reason':reason,'diagnostics':diagnostics,**source_clocks}


SUMMARY_PROJECTION_SCHEMA='native_h1_status_projection_v1_20260913'
SUMMARY_DIAGNOSTIC_SCHEMA='bounded_status_diagnostic_v1_20260913'
SUMMARY_FAILURE_SCHEMA='joint_status_publication_failure_v1_20260913'


def _summary_require(ok,code):
    if not ok:raise summary_boundary.SummaryBoundaryError('summary_projection_'+code)


def _summary_scalar(value,kind,name):
    if kind=='number':_summary_require(type(value) in (int,float) and math.isfinite(value),name+'_number')
    elif kind=='text':_summary_require(type(value) is str and 0<len(value)<=512,name+'_text')
    elif kind=='hash':_summary_require(type(value) is str and re.fullmatch('[a-f0-9]{64}',value) is not None,name+'_hash')
    elif kind=='bool':_summary_require(type(value) is bool,name+'_bool')
    return value


def _summary_diagnostic(value,preview_chars=512):
    """Explicit display truncation; no forecast, row, denominator or clock is dropped."""
    if value is None:return None
    _summary_require(type(value) is str and len(value)<=16384,'bounded_diagnostic_text')
    raw=value.encode('utf-8')
    return {'schema_version':SUMMARY_DIAGNOSTIC_SCHEMA,'text':value[:preview_chars],
        'full_text_sha256':hashlib.sha256(raw).hexdigest(),'full_text_chars':len(value),
        'display_truncated':len(value)>preview_chars}


def _summary_readiness(value):
    _summary_require(type(value) is dict,'readiness_object')
    result={key:value[key] for key in ('observed_epoch','status','price_bar_close_epoch','news_evidence_epoch','news_expires_epoch')}
    result['projection_schema_version']='joint_readiness_status_projection_v1_20260913'
    for key in ('observed_epoch','price_bar_close_epoch','news_evidence_epoch','news_expires_epoch'):
        if result[key] is not None:_summary_scalar(result[key],'number',key)
    _summary_scalar(result['status'],'text','readiness_status')
    reason=_summary_diagnostic(value['reason'])
    result['reason']=reason['text'];result['reason_detail']=reason
    diagnostics=value['diagnostics'];_summary_require(type(diagnostics) is dict,'readiness_diagnostics')
    raw=encoded(diagnostics);_summary_require(len(raw)<=64*1024,'readiness_diagnostics_bound')
    keys=('ready','status','current_real_prices','current_real_returns','current_span_sec',
        'mature_exact_h1_training_rows','required_joint_training_rows','nonzero_news_context_training_rows',
        'distinct_news_context_patterns','vetted_news_training_rows','training_scope')
    compact={}
    for key in keys:
        if key not in diagnostics:continue
        value=diagnostics[key]
        _summary_scalar(value,'bool' if key=='ready' else 'text' if key in ('status','training_scope') else 'number',key)
        compact[key]=value
    reasons=diagnostics.get('reasons',[])
    _summary_require(type(reasons) is list and len(reasons)<=64,'readiness_reasons_bound')
    reason_details=[_summary_diagnostic(reason,192) for reason in reasons[:8]]
    compact['reasons']=[reason['text'] for reason in reason_details]
    compact['reason_details']=reason_details
    result.update(diagnostics=compact,full_diagnostics_sha256=hashlib.sha256(raw).hexdigest(),
        full_reason_count=len(reasons),reason_previews_omitted=max(0,len(reasons)-8),
        diagnostic_fields_omitted=sorted(set(diagnostics)-set(keys)-{'reasons'}))
    return result


def _summary_native_observation(value):
    if value is None:return None
    _summary_require(type(value) is dict,'native_observation_object')
    raw=encoded(value);_summary_require(len(raw)<=8192,'native_observation_bound')
    result={'schema_version':'native_observation_status_projection_v1_20260913',
        'full_observation_sha256':hashlib.sha256(raw).hexdigest(),'full_observation_bytes':len(raw)}
    for key in ('status','reason_code','reason'):
        if key in value:
            detail=_summary_diagnostic(value[key],256)
            result[key]=detail['text'];result[key+'_detail']=detail
    for key in ('sequence','native_targets_scored','observed_epoch','read_started_epoch','read_completed_epoch'):
        if key in value:
            item=value[key]
            if item is not None:_summary_scalar(item,'number',key)
            result[key]=item
    result['fields_omitted']=sorted(set(value)-{'status','reason_code','reason','sequence','native_targets_scored','observed_epoch','read_started_epoch','read_completed_epoch'})
    return result


def _summary_native_publication(value):
    if value is None:return None
    _summary_require(type(value) is dict and type(value.get('forecasts')) is list and len(value['forecasts'])==1,'single_native_arm_required')
    result={'projection_schema_version':SUMMARY_PROJECTION_SCHEMA,
        'full_evidence_location':'immutable_pair_family_ledger',
        'full_verified_publication_sha256':digest(value)}
    fields={
        'text':('decision_id','instrument','family','reference_quote_id','reference_quote_role'),
        'number':('horizon_sec','publication_epoch','reference_epoch','reference_available_epoch','issued_epoch','target_epoch','verified_epoch','consumption_epoch'),
        'hash':('forecast_sha256','publication_receipt_sha256','consumer_receipt_sha256','native_anchor_sha256'),
        'bool':('publication_verified','consumption_verified')}
    for kind,names in fields.items():
        for name in names:result[name]=_summary_scalar(value[name],kind,name)
    _summary_require(result['horizon_sec']==3600 and result['target_epoch']>result['reference_epoch'],'native_h1_target')
    original=value['forecasts'][0];_summary_require(type(original) is dict,'arm_object')
    arm={}
    fields={
        'text':('family','instrument','cohort_id','forecast_id','probability_event','reference_mid','pip_size','input_timeframe','model_version','feature_version','learning_data_role'),
        'number':('horizon_sec','reference_epoch','target_epoch','predicted_return_bps','probability_up','issued_epoch',
            'feature_cutoff_epoch','features_available_epoch','input_source_observed_epoch','computation_started_epoch','computation_completed_epoch',
            'news_available_epoch','news_evidence_epoch','news_expires_epoch','news_first_observed_epoch','news_generated_epoch',
            'training_label_maturity_max_epoch','training_labels_available_max_epoch','training_news_available_max_epoch','training_news_feature_cutoff_max_epoch'),
        'hash':('native_anchor_sha256','news_capture_sha256'),
        'bool':('research_only','can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible')}
    for kind,names in fields.items():
        for name in names:arm[name]=_summary_scalar(original[name],kind,name)
    _summary_require(type(original['side']) is int and original['side'] in (-1,0,1),'native_side_integer')
    arm['side']=original['side']
    _summary_require(arm['instrument']==result['instrument'] and arm['family']==result['family']
        and arm['reference_epoch']==result['reference_epoch'] and arm['target_epoch']==result['target_epoch']
        and arm['horizon_sec']==3600 and 0<=arm['probability_up']<=1,'arm_target_or_probability_identity')
    # The small native anchor has no fitted vectors; preserve its exact original values and semantics.
    anchor=original['native_anchor'];_summary_require(type(anchor) is dict,'native_anchor_object')
    anchor_raw=encoded(anchor);_summary_require(len(anchor_raw)<=4096 and digest(anchor)==arm['native_anchor_sha256'],'native_anchor_binding')
    arm['native_anchor']=json.loads(anchor_raw)
    revision=original['news_revision_evidence'];revision_raw=encoded(revision)
    _summary_require(type(revision) is dict and len(revision_raw)<=128*1024,'revision_evidence_bound')
    arm['news_revision_evidence_sha256']=hashlib.sha256(revision_raw).hexdigest()
    arm['news_revision_evidence_bytes']=len(revision_raw)
    diagnostics=original['diagnostics'];_summary_require(type(diagnostics) is dict,'forecast_diagnostics_object')
    diagnostics_raw=encoded(diagnostics);_summary_require(len(diagnostics_raw)<=128*1024,'forecast_diagnostics_bound')
    compact={'full_diagnostics_sha256':hashlib.sha256(diagnostics_raw).hexdigest(),
        'full_diagnostics_bytes':len(diagnostics_raw),'fitted_vectors_in_immutable_ledger':True}
    scalar_keys=('training_rows','feature_count','price_feature_count','matched_price_only_expected_pips',
        'neutral_news_ablation_expected_pips','news_ablation_difference_pips','ridge_alpha','probability_shrinkage')
    text_keys=('method','probability_scope','news_ablation_scope','training_scope')
    for key in scalar_keys+text_keys:
        if key in diagnostics:compact[key]=_summary_scalar(diagnostics[key],'text' if key in text_keys else 'number',key)
    for key in ('fitted_joint','matched_price_only_fitted'):
        if key in diagnostics:
            fitted=diagnostics[key];_summary_require(type(fitted) is dict,key+'_object')
            compact[key]={name:_summary_scalar(fitted[name],'number',name) for name in ('sigma_pips','residual_sigma_pips','expected_return_shrinkage','overlap_adjusted_training_count_heuristic') if name in fitted}
    origins=diagnostics.get('training_row_start_epochs_by_pair',{})
    _summary_require(type(origins) is dict and set(origins)<= {arm['instrument']},'training_origin_pair')
    values=origins.get(arm['instrument'],[])
    _summary_require(type(values) is list and len(values)<=256 and all(type(v) is int for v in values),'training_origin_metadata_bound')
    compact['training_origins']={'count':len(values),'first_epoch':min(values) if values else None,
        'last_epoch':max(values) if values else None,'ordered_original_sha256':digest(origins)}
    arm['diagnostics']=compact;result['forecasts']=[arm]
    _summary_require(len(encoded(result))<=8192,'native_projection_capacity')
    return result


def _status_failure_record(self,exc):
    self.errors+=1;self.heartbeat_errors+=1
    code=type(exc).__name__+':'+str(exc)[:512]
    self.last_error='status_publication:'+code
    previous=getattr(self,'status_publication_failure',None)
    sequence=(previous or {}).get('failure_count',0)+1
    record={'schema_version':SUMMARY_FAILURE_SCHEMA,'status':'failed','failure_count':sequence,
        'failed_epoch':number(self.clock()),'reason':code,'errors':self.errors,
        'heartbeat_publication_errors':self.heartbeat_errors,'last_verified_summary':None,
        'research_only':True,'can_place_orders':False,'successful_summary_claimed':False}
    published=getattr(self,'published_summary',None)
    if published is not None:
        record['last_verified_summary']={'sha256':published.snapshot.summary_sha256,
            'generated_epoch':published.snapshot.generated_epoch,'read_completed_epoch':published.read_completed_epoch,
            'original_clocks_retained':True}
    self.status_publication_failure=record
    try:
        raw=encoded(record);_summary_require(len(raw)<=8192,'failure_record_capacity')
        atomic_bytes(self.study/'summary_publication_status.json',raw)
        if read_summary_bytes(self.study/'summary_publication_status.json')!=raw:raise OSError('status_failure_readback_mismatch')
    except (OSError,summary_boundary.SummaryBoundaryError) as write_error:
        self.status_failure_write_error=type(write_error).__name__+':'+str(write_error)[:256]
        print(json.dumps({'status':'summary_failure_record_write_unconfirmed','failure_count':sequence,
            'reason':code,'write_error':self.status_failure_write_error}),file=sys.stderr)


def _status_recovery_record(self,published):
    previous=getattr(self,'status_publication_failure',None)
    if previous is None or previous.get('status')=='recovered':return
    record={**previous,'status':'recovered','recovered_epoch':published.read_completed_epoch,
        'recovered_summary_sha256':published.snapshot.summary_sha256,
        'recovered_summary_generated_epoch':published.snapshot.generated_epoch,
        'successful_summary_claimed':True}
    raw=encoded(record);_summary_require(len(raw)<=8192,'recovery_record_capacity')
    atomic_bytes(self.study/'summary_publication_status.json',raw)
    if read_summary_bytes(self.study/'summary_publication_status.json')!=raw:raise OSError('status_recovery_readback_mismatch')
    self.status_publication_failure=record
    self.status_failure_write_error=None

def build_summary(registry,states,now):
    rows=[]
    for pair,item in sorted(registry['pairs'].items()):
        state=states[pair];slots={}
        for family in sorted(FAMILIES):
            slot=state['families'][family];ledger=slot['ledger'];forecast=slot.get('publication')
            if forecast and forecast['target_epoch']<=now:forecast=None
            readiness=input_readiness(state.get('capture'),family,now)
            reason=state.get('capture_error') if not state.get('capture') else None
            reason=reason or readiness['reason'] or slot.get('reason') or 'awaiting_family_attempt'
            status='ready' if readiness['status']=='ready' else 'warming'
            if readiness['status']=='unavailable':status='unavailable'
            if state.get('quote_reason'):status='unavailable';reason=state['quote_reason']
            if slot.get('building'):status='building';reason='building_family_model'
            if forecast:status='forecast';reason='forecast'
            slots[family]={'status':status,'reason':reason[:4096],'observed_epoch':now,
                'activated_epoch':ledger.activated_epoch,'contract_sha256':ledger.contract_hash,
                'cohort_id':ledger.contract['cohorts'][family],'current_readiness':_summary_readiness(readiness),
                'last_attempt':slot.get('last_attempt'),'latest_forecast':_summary_native_publication(forecast),'counts':ledger.counts(),
                'native_outcome_status':slot.get('native_outcome_status','awaiting_native_outcome_observation'),
                'native_outcome_reason':_summary_diagnostic(slot.get('native_outcome_reason',''))['text'],
                'native_outcome_reason_detail':_summary_diagnostic(slot.get('native_outcome_reason','')),
                'native_observation':_summary_native_observation(slot.get('native_observation')),'last_native_attempt_epoch':slot.get('last_score'),
                'last_native_completed_epoch':slot.get('last_native_completed_epoch')}
        for value in slots.values():
            _summary_require(len(encoded(value))<=12*1024,'family_slot_capacity')
        rows.append({'instrument':pair,'pip_size':item['pip_size'],'families':slots})
    summary={'schema_version':SUMMARY_SCHEMA,'registry_sha256':digest(registry),'generated_epoch':now,
        'research_only':True,**{flag:False for flag in INERT_FLAGS},'rows':rows}
    summary['payload_sha256']=digest(summary)
    _summary_require(len(encoded(summary))<=1024*1024,'summary_capacity')
    return summary


def capture_once(candle_root,pair,pip,*,session,news_capture,history_share,clock=time.time):
    return _bound_inputs().capture_inputs(candle_root,pair,pip_size=pip,session=session,news_capture=news_capture,history_share=history_share,clock=clock)


def fit_capture(capture,family,*,session,news_capture,history_share,clock=time.time):
    return _bound_inputs().compute_predictions(capture,families=(family,),session=session,news_capture=news_capture,history_share=history_share,clock=clock)


def source_signature(candle_root,pair):
    # Price-only scheduling metadata; input capture still authenticates real bytes.
    if type(pair) is not str or re.fullmatch('[A-Z]{3}_[A-Z]{3}',pair) is None:raise ValueError('pair_identity')
    path=plain_c_path(Path(candle_root)/(pair+'_M1.csv'));info=path.stat()
    if not stat.S_ISREG(info.st_mode):raise ValueError('price_source_regular_file_required')
    return digest([str(path),info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns])


def capture_shared_owned(session,instruments,*,clock=time.time):
    inputs=_bound_inputs();io=inputs.news_io;state=io._session(session)
    with state['lock']:
        serial=state['failure_serial']
        try:
            handle=io.capture_shared(session,clock=clock)
            shared=inputs.history.prepare_universal_history_share(session,handle,instruments)
            _,parts=io._eligible(session,handle)
            inputs.history._live(shared,session,handle)
            return handle,shared,io.capture_metadata(handle)['descriptor']['capture_sha256'],parts[4]
        except Exception as exc:
            # Fixed IO failures already invalidate once; history failures must also
            # invalidate older handles before any other pair can use them.
            if state['failure_serial']==serial:
                io._failure(state,json.loads(state['config']),'shared_history_preparation',exc)
            raise


def retained_abstention(ledger,attempt_id,reason,*,capture=None,result=None):
    # Evidence serialization can itself refuse after a real durable attempt.
    # Preserve that refusal without inventing a second attempt or closing success.
    safe={'reason':str(reason)[:4096]}
    try:return ledger.record_abstention(attempt_id,safe,capture=capture,result=result)
    except (ValueError,TypeError,OverflowError) as exc:
        if str(exc) in ('successful_attempt_cannot_abstain','attempt_already_closed','attempt_not_reserved'):raise
        safe['evidence_retention_refusal']=type(exc).__name__+':'+str(exc)[:500]
        return ledger.record_abstention(attempt_id,safe)


class PairRunner:
    """Serial fitting, continuous quotes, independent successful family slots."""
    def __init__(self,registry,study,candle_root,quote_path,*,session,clock=time.time,monotonic=time.monotonic,activate=False):
        self.registry=registry;self.study=Path(study);self.candle_root=Path(candle_root);self.quote_path=Path(quote_path)
        self.news_session=session;self.news_io=_bound_inputs().news_io;self.monotonic=monotonic
        self.native_clock_path=plain_c_path(json.loads(self.news_io._session(session)['config'])['clock_path']);self.native_cursor=0
        self.news_schedule=schedule.SharedNewsSchedule();self.news_capture=None;self.history_share=None;self.news_future=None
        self.news_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='joint-revision-news-capture')
        self.news_bootstrap_future=None;self.news_bootstrap_complete=False
        self.news_bootstrap_next_monotonic=0.;self.news_bootstrap_report=None
        self.news_refresh_next_monotonic=0.
        self.clock=clock;self.states={};self.current={};self.queue=sorted(registry['pairs'])
        self.future=None;self.active=None;self.score_future=None;self.score_key=None
        self.pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='joint-price-news-fit')
        self.score_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='joint-price-news-score')
        self.last_poll=self.last_summary=self.last_heartbeat=0.0;self.summary=None;self.errors=0
        self.published_summary=None
        self.heartbeat_errors=0;self.last_error=''
        self.capture_cursor=0;self.fit_cursor=0;self.prefer_fit=True
        self.fresh_capture_pair=None;self.fresh_priority_allowed=True
        self.family_cursors={};self.scheduler_events=[];self.scheduler_diagnostic_errors=0
        self.scheduler_events_total=0;self.scheduler_events_evicted=0
        try:
            for pair,item in registry['pairs'].items():
                state={'families':{},'quote_reason':'awaiting_current_quote','capture':None,'news_capture':None,'history_share':None,'source_signature':None,'last_scan':0.0}
                self.states[pair]=state
                for family,registered in item['families'].items():
                    native_contract=registered['contract']
                    if (plain_c_path(native_contract['native_candle_path'])!=self.candle_root/(pair+'_M1.csv') or
                            plain_c_path(native_contract['native_clock_path'])!=self.native_clock_path):
                        raise ValueError('worker_native_source_paths_must_match_registered_inputs')
                    ledger=CausalForecastLedger(self.study/'pairs'/pair/family/'study.sqlite',native_contract,clock=clock,activate=activate)
                    slot={'ledger':ledger,'reason':'awaiting_family_attempt','building':False,'last_try':0.0,
                        'last_success_bucket':-1,'failed_basis':None,'last_score':0.0,'scored_outcomes':0,
                        'native_outcome_status':'awaiting_native_outcome_observation','native_outcome_reason':'',
                        'native_observation':None,'last_native_completed_epoch':None}
                    state['families'][family]=slot
                    row=ledger.db.execute('SELECT MAX(bucket) FROM forecasts').fetchone()
                    if row[0] is not None:slot['last_success_bucket']=row[0]
                    last=ledger.db.execute('SELECT id,bucket,epoch FROM attempts ORDER BY epoch DESC LIMIT 1').fetchone()
                    if last:slot['last_attempt']={'attempt_id':last['id'],'bucket':last['bucket'],'epoch':last['epoch'],'reason':'retained_attempt'}
                    ledger.settle();slot['publication']=verified_publication(ledger)
        except BaseException:
            self.close();raise

    def observe_news_failure(self):
        # A capture owns the same lock. Do not block quote polling on that read.
        state=self.news_io._session(self.news_session)
        if not state['lock'].acquire(blocking=False):return False
        try:
            self.news_schedule,wake=schedule.observe_failure_generation(self.news_schedule,state['failure_serial'])
            if wake:
                self.news_capture=None;self.history_share=None;self.fresh_capture_pair=None
                for item in self.states.values():
                    item['capture']=None;item['news_capture']=None;item['history_share']=None;item['source_signature']=None;item['last_scan']=0.
                    item['capture_error']='observed_news_failure_generation:'+str(state['failure_serial'])
                self.record_scheduler_event('news_failure_generation',None,failure_serial=state['failure_serial'])
            return True
        finally:state['lock'].release()

    def news_hint(self,handle):
        state=self.news_io._session(self.news_session)
        if not state['lock'].acquire(blocking=False):return None
        try:
            _,parts=self.news_io._eligible(self.news_session,handle)
            return self.news_io.capture_metadata(handle)['descriptor']['capture_sha256'],parts[4]
        finally:state['lock'].release()

    def finish_news(self):
        self.observe_news_failure()
        bootstrap=getattr(self,'news_bootstrap_future',None)
        if bootstrap is not None:
            if not bootstrap.done():return
            try:
                report=bootstrap.result()
                if (type(report) is not dict or report.get('status')!='cache_prepared_fresh_capture_required'
                    or report.get('fresh_health_proven') is not False
                    or report.get('capture_handle_returned') is not False
                    or report.get('original_availability_unchanged') is not True):
                    raise ValueError('explicit_non_authorizing_bootstrap_required')
                self.news_bootstrap_report=report;self.news_bootstrap_complete=True
                self.record_scheduler_event('news_bootstrap_completed_fresh_capture_required',None,
                    elapsed_sec=report.get('elapsed_sec'),manifest_sha256=report.get('manifest_sha256'))
            except Exception as exc:
                self.news_bootstrap_complete=False;self.news_bootstrap_report=None
                self.news_bootstrap_next_monotonic=self.monotonic()+60
                self.error('news_bootstrap',exc)
            self.news_bootstrap_future=None
            # Bootstrap invalidates old handles. A separate, ordinary
            # capture will establish current health and issue authority.
            self.news_capture=None;self.history_share=None
            self.observe_news_failure()
        if self.news_future is None or not self.news_future.done():return
        # A slow successful or failed read gets a complete quiet interval.
        # This is scheduling only; retained clocks/authority never move.
        self.news_refresh_next_monotonic=self.monotonic()+schedule.NEWS_CAPTURE_INTERVAL_SECONDS
        try:
            handle,shared,sha,serial=self.news_future.result()
            hint=self.news_hint(handle)
            if hint is None:return
            status=self.news_io.session_status(self.news_session)
            self.news_schedule,accepted,wake=schedule.complete_shared_capture(self.news_schedule,
                capture_sha256=sha,captured_failure_serial=serial,current_failure_serial=status['failure_serial'],current_usable=status['usable'])
            self.news_capture=handle if accepted else None
            self.history_share=shared if accepted else None
            self.record_scheduler_event('shared_news_capture_completed',None,capture_sha256=sha,accepted=accepted,failure_serial=status['failure_serial'])
        except Exception as exc:
            self.observe_news_failure()
            status=self.news_io.session_status(self.news_session)
            self.news_schedule,_=schedule.failed_shared_capture(self.news_schedule,current_failure_serial=status['failure_serial'])
            self.news_capture=None;self.history_share=None;self.error('shared_news',exc)
        self.news_future=None

    def schedule_news(self):
        # A current pair capture/fit owns its useful-work window. Never
        # queue a competing lock-holding news task behind that work.
        if getattr(self,'future',None) is not None:return
        if not getattr(self,'news_bootstrap_complete',False):
            now=self.monotonic()
            if getattr(self,'news_bootstrap_future',None) is not None or now<getattr(self,'news_bootstrap_next_monotonic',0.):return
            self.news_bootstrap_next_monotonic=now+60
            self.news_bootstrap_future=self.news_pool.submit(self.news_io.bootstrap_inputs,self.news_session,clock=self.clock)
            self.record_scheduler_event('news_bootstrap_dispatched',None)
            return
        if self.news_future is not None:return
        now=self.monotonic()
        if now<getattr(self,'news_refresh_next_monotonic',0.):return
        state,dispatch=schedule.dispatch_shared_capture(self.news_schedule,now)
        if not dispatch:return
        self.news_schedule=state
        self.news_refresh_next_monotonic=now+schedule.NEWS_CAPTURE_INTERVAL_SECONDS
        try:self.news_future=self.news_pool.submit(capture_shared_owned,self.news_session,tuple(self.queue),clock=self.clock)
        except Exception:
            self.news_refresh_next_monotonic=self.monotonic()+schedule.NEWS_CAPTURE_INTERVAL_SECONDS
            self.news_schedule,_=schedule.failed_shared_capture(self.news_schedule,current_failure_serial=self.news_schedule.failure_serial)
            raise
        self.record_scheduler_event('shared_news_capture_dispatched',None)

    def error(self,pair,exc):
        reason=type(exc).__name__+':'+str(exc);self.errors+=1;self.last_error=pair+':'+reason
        return reason

    def poll_quotes(self):
        self.current={}
        try:snapshot,observed,source=read_snapshot(self.quote_path,clock=self.clock)
        except Exception as exc:
            for state in self.states.values():state['quote_reason']=type(exc).__name__+':'+str(exc)
            return
        for pair,state in self.states.items():
            try:
                quote=pair_quote(snapshot,observed,source,pair,self.registry['pairs'][pair]['pip_size'])
                self.current[pair]={family:slot['ledger'].observe_quote(quote) for family,slot in state['families'].items()}
                state['quote_reason']=''
            except Exception as exc:state['quote_reason']=type(exc).__name__+':'+str(exc)

    def finish_work(self):
        if self.future is None or not self.future.done():return
        kind,pair,detail=self.active;state=self.states[pair]
        if kind=='capture':
            io_state=self.news_io._session(self.news_session)
            if not io_state['lock'].acquire(blocking=False):return
            io_state['lock'].release()
        work_observed=number(self.clock());issue_started=None;outcome='completed';error_code=None;guard_code=None
        try:
            value=self.future.result()
            if kind=='capture':
                signature,news_capture,history_share=detail
                # The main thread is the only news dispatcher; no new read can start here.
                if self.news_hint(news_capture) is None:raise ValueError('news_session_busy_at_pair_handoff')
                state['capture']=value;state['source_signature']=signature;state['news_capture']=news_capture;state['history_share']=history_share
                state['capture_received_epoch']=work_observed;self.fresh_capture_pair=pair
                observation={'instrument':pair,'observed_epoch':value.get('first_observed_epoch'),'event_observed_epoch':number(self.clock()),
                    'source_capture_sha256':value.get('source_capture_sha256'),'status':value.get('status'),
                    'reasons':value.get('reasons',[]),'family_readiness':value.get('family_readiness',{})}
                with (self.study/'readiness.jsonl').open('ab') as handle:handle.write(encoded(observation)+b'\n')
            else:
                family,attempt_id,capture,basis,news_capture,history_share=detail;slot=state['families'][family];ledger=slot['ledger']
                if value.get('status') in ('ready','partial') and family in value.get('predictions',{}):
                    issue_started=number(self.clock())
                    ledger.issue(attempt_id,capture,value,session=self.news_session,news_capture=news_capture,history_share=history_share);ledger.consume_publications()
                    slot['publication']=verified_publication(ledger);slot['last_success_bucket']=slot['last_attempt']['bucket']
                    slot['reason']='';slot['last_attempt']['reason']='published';slot['failed_basis']=None
                else:
                    outcome='abstained'
                    reason='|'.join(value.get('reasons',[])) or 'selected_family_unavailable'
                    retained_abstention(ledger,attempt_id,reason,capture=capture,result=value)
                    slot['reason']=reason;slot['last_attempt']['reason']=reason;slot['failed_basis']=basis
        except Exception as exc:
            outcome='failed';error_code=type(exc).__name__
            if type(exc) is ValueError and str(exc) in {
                    'news_stale_or_future_at_actual_issue','news_original_expiry_before_actual_issue',
                    'cached_input_expired_before_issue','computation_before_attempt_or_target_expired',
                    'construction_deadline_exceeded','input_or_learning_availability_order'}:
                guard_code=str(exc)
            reason=self.error(pair,exc)
            if kind=='capture':
                state['source_signature']=detail[0];state['capture']=None;state['news_capture']=None;state['history_share']=None;state['capture_error']=reason
                if getattr(self,'fresh_capture_pair',None)==pair:self.fresh_capture_pair=None
            else:
                family,attempt_id,capture,basis,news_capture,history_share=detail;slot=state['families'][family]
                slot['reason']=reason;slot['last_attempt']['reason']=reason;slot['failed_basis']=basis
                try:retained_abstention(slot['ledger'],attempt_id,reason,capture=capture)
                except Exception as diagnostic_exc:self.error(pair,diagnostic_exc)
        finally:
            captured=state.get('capture') if kind=='capture' else detail[2]
            self.record_scheduler_event('work_finished',pair,captured,kind=kind,
                completed_work_observed_epoch=work_observed,issue_call_started_epoch=issue_started,
                outcome=outcome,error_code=error_code,guard_code=guard_code,
                computation_completed_epoch=value.get('computed_epoch') if 'value' in locals() and isinstance(value,dict) else None)
            if kind=='fit':state['families'][detail[0]]['building']=False
            self.future=None;self.active=None

    def record_scheduler_event(self,event,pair,capture=None,**details):
        # No I/O on this critical path. The existing heartbeat publishes only the
        # newest 32 bounded records; original attempts remain durable in the ledger.
        self.scheduler_events_total=getattr(self,'scheduler_events_total',0)+1
        try:
            row={'schema_version':SCHEDULING_VERSION,'event':event,'instrument':pair,
                 'observed_epoch':number(self.clock()),**details}
            for key in ('source_capture_sha256','first_observed_epoch','max_bar_close_epoch',
                        'news_capture_sha256','news_evidence_epoch','news_generated_epoch',
                        'news_first_observed_epoch','news_available_epoch','news_expires_epoch'):
                if isinstance(capture,dict) and key in capture:row[key]=capture[key]
            raw=encoded(row)
            if len(raw)>4096:raise ValueError('scheduler_event_bound')
            # Canonical snapshot, not a mutable alias to a capture or caller data.
            events=getattr(self,'scheduler_events',[]);events.append(json.loads(raw))
            self.scheduler_events_evicted=getattr(self,'scheduler_events_evicted',0)+max(0,len(events)-32)
            self.scheduler_events=events[-32:]
        except Exception:
            self.scheduler_diagnostic_errors=getattr(self,'scheduler_diagnostic_errors',0)+1

    def schedule_work(self):
        if self.future is not None:return
        # Avoid a dispatch race before the news executor acquires its
        # session lock. Existing eligible handles are not renewed here.
        if self.news_future is not None or getattr(self,'news_bootstrap_future',None) is not None:return
        now=number(self.clock());bucket=int(now//900)
        phases=(self.schedule_fit,self.schedule_capture) if getattr(self,'prefer_fit',True) else (self.schedule_capture,self.schedule_fit)
        for phase in phases:
            if phase(now,bucket):return

    def schedule_capture(self,now,bucket):
        if self.news_capture is None or self.history_share is None:return False
        try:hint=self.news_hint(self.news_capture)
        except ValueError:self.observe_news_failure();return False
        if hint is None:return False
        sha,serial=hint
        for _ in range(len(self.queue)):
            index=getattr(self,'capture_cursor',0)
            pair=self.queue[index];self.capture_cursor=(index+1)%len(self.queue);state=self.states[pair]
            if now-state['last_scan']>=30:
                state['last_scan']=now
                try:signature=schedule.pair_capture_basis(source_signature(self.candle_root,pair),sha,serial)
                except (OSError,ValueError) as exc:
                    state['capture']=None;state['capture_error']=type(exc).__name__+':'+str(exc)[:300]
                    state['source_signature']=None;continue
                if signature!=state['source_signature']:
                    handle=self.news_capture;shared=self.history_share
                    self.active=('capture',pair,(signature,handle,shared))
                    self.future=self.pool.submit(capture_once,self.candle_root,pair,self.registry['pairs'][pair]['pip_size'],session=self.news_session,news_capture=handle,history_share=shared,clock=self.clock)
                    self.prefer_fit=True;self.record_scheduler_event('capture_dispatched',pair)
                    return True
        return False

    def schedule_fit(self,now,bucket):
        # At most one priority handoff between ordinary round-robin fits.
        # Priority never advances the ordinary cursor, so a single changing
        # pair cannot starve other cached ready pairs or their control families.
        preferred=getattr(self,'fresh_capture_pair',None);self.fresh_capture_pair=None
        start=getattr(self,'fit_cursor',0)
        regular=self.queue[start:]+self.queue[:start]
        priority=preferred if getattr(self,'fresh_priority_allowed',True) and preferred in regular and preferred!=regular[0] else None
        pairs=([priority] if priority is not None else [])+[p for p in regular if p!=priority]
        for pair in pairs:
            state=self.states[pair];capture=state.get('capture');news_capture=state.get('news_capture');history_share=state.get('history_share')
            if capture is None or news_capture is None or history_share is None:continue
            try:news_hint=self.news_hint(news_capture)
            except ValueError:self.observe_news_failure();continue
            if news_hint is None:continue
            families=sorted(state['families']);cursors=getattr(self,'family_cursors',{})
            cursor=cursors.get(pair,0)%len(families)
            for offset in range(len(families)):
                family=families[(cursor+offset)%len(families)];slot=state['families'][family]
                quote=self.current.get(pair,{}).get(family)
                if not quote or slot['last_success_bucket']>=bucket or now-slot['last_try']<30:continue
                if input_readiness(capture,family,now)['status']!='ready':continue
                if number(capture['first_observed_epoch'])>now:continue
                if not 0<=now-quote['market_epoch']<=60:continue
                basis=schedule.failed_fit_basis(capture['source_capture_sha256'],bucket,news_hint[1])
                if slot['failed_basis']==basis:continue
                slot['last_try']=now;attempt_id=None
                try:
                    attempt_id=slot['ledger'].begin_attempt(bucket,quote)
                    if attempt_id is None:slot['last_success_bucket']=bucket;continue
                    slot['last_attempt']={'epoch':number(self.clock()),'reason':'building','attempt_id':attempt_id,'bucket':bucket}
                    dispatch=number(self.clock())
                    if (input_readiness(capture,family,dispatch)['status']!='ready'
                            or number(capture['first_observed_epoch'])>dispatch
                            or not 0<=dispatch-quote['market_epoch']<=60):
                        raise ValueError('scheduler_inputs_unavailable_before_dispatch')
                    slot['building']=True
                    self.active=('fit',pair,(family,attempt_id,capture,basis,news_capture,history_share))
                    self.future=self.pool.submit(fit_capture,capture,family,session=self.news_session,news_capture=news_capture,history_share=history_share,clock=self.clock)
                    self.prefer_fit=False
                    cursors[pair]=(cursor+offset+1)%len(families);self.family_cursors=cursors
                    if pair==priority:self.fresh_priority_allowed=False
                    else:
                        self.fit_cursor=(self.queue.index(pair)+1)%len(self.queue)
                        self.fresh_priority_allowed=True
                    self.record_scheduler_event('fit_dispatched',pair,capture,family=family,
                        attempt_id=attempt_id,capture_received_epoch=state.get('capture_received_epoch'),
                        selected_epoch=now,dispatch_checked_epoch=dispatch,
                        queue_age_sec=dispatch-number(capture['first_observed_epoch']),
                        priority_handoff=pair==priority)
                    return True
                except Exception as exc:
                    reason=self.error(pair,exc);slot['reason']=reason;slot['building']=False
                    if attempt_id is not None:
                        if slot.get('last_attempt',{}).get('attempt_id')!=attempt_id:
                            # No invented wall clock if sampling failed after the durable claim.
                            slot['last_attempt']={'epoch':None,'attempt_id':attempt_id,'bucket':bucket}
                        slot['last_attempt']['reason']=reason;slot['failed_basis']=basis
                        try:retained_abstention(slot['ledger'],attempt_id,reason,capture=capture)
                        except Exception as diagnostic_exc:self.error(pair,diagnostic_exc)
                    self.future=None;self.active=None
                    self.record_scheduler_event('fit_dispatch_failed',pair,capture,family=family,
                        attempt_id=attempt_id,error_code=type(exc).__name__)
        return False

    def score(self):
        # Native outcome observation must continue when news/quotes are blocked.
        # Reuse the existing sole score executor, with fair per-pair scheduling.
        if self.score_future is not None and self.score_future.done():
            pair,family=self.score_key;slot=self.states[pair]['families'][family]
            try:
                value=self.score_future.result();observation=value['observation']
                slot['scored_outcomes']=value['scorecard']['collection_counts']['outcomes']
                slot['native_observation']=observation
                slot['native_outcome_status']=str(observation.get('status','native_observation_returned'))[:256]
                slot['native_outcome_reason']=str(observation.get('reason_code',observation.get('reason','')))[:4096]
                slot['last_native_completed_epoch']=number(self.clock())
            except Exception as exc:
                slot['native_outcome_status']='native_observation_or_score_failed'
                slot['native_outcome_reason']=self.error(pair,exc)[:4096]
            self.score_future=None;self.score_key=None
        if self.score_future is not None:return
        now=number(self.clock())
        for _ in range(len(self.queue)):
            index=getattr(self,'native_cursor',0);pair=self.queue[index]
            self.native_cursor=(index+1)%len(self.queue)
            state=self.states[pair]
            for family,slot in state['families'].items():
                if now-slot['last_score']<30:continue
                slot['last_score']=now;self.score_key=(pair,family)
                try:
                    self.score_future=self.score_pool.submit(observe_and_score_native,slot['ledger'],
                        self.candle_root/(pair+'_M1.csv'),self.native_clock_path,
                        self.study/'pairs'/pair/family/'scorecard.json')
                    return
                except Exception as exc:
                    self.score_future=None;self.score_key=None
                    slot['native_outcome_status']='native_observation_dispatch_failed'
                    slot['native_outcome_reason']=self.error(pair,exc)[:4096]


    def publish_status(self,*,force=False):
        now=number(self.clock())
        if force or now-self.last_summary>=15:
            for pair,state in self.states.items():
                for slot in state['families'].values():
                    try:slot['publication']=verified_publication(slot['ledger'])
                    except Exception as exc:slot['publication']=None;self.error(pair,exc)
            now=number(self.clock());summary=build_summary(self.registry,self.states,now)
            frozen=summary_boundary.freeze_summary(summary,expected_schema=SUMMARY_SCHEMA,
                expected_registry_sha256=digest(self.registry))
            atomic_bytes(self.study/'summary.json',frozen.raw)
            observed=read_summary_bytes(self.study/'summary.json')
            published=summary_boundary.verify_written_summary(frozen,observed,
                read_completed_epoch=number(self.clock()))
            # Advance only after actual exact-byte readback; no mutable state
            # alias can alter the acknowledged generation or its counts.
            self.published_summary=published
            self.summary=json.loads(frozen.raw)
            self.last_summary=published.read_completed_epoch
            _status_recovery_record(self,published)
        if getattr(self,'published_summary',None) is not None and (force or now-self.last_heartbeat>=5):
            fields=summary_boundary.heartbeat_fields(self.published_summary,
                generated_epoch=number(self.clock()))
            heartbeat={'schema_version':HEARTBEAT_SCHEMA,**fields,'research_only':True,
                **{flag:False for flag in INERT_FLAGS},'phase':'research_collection','supported_decision':'no_trade',
                'errors':self.errors,'last_error':self.last_error[:4096],'heartbeat_publication_errors':self.heartbeat_errors,
                'active_pair':self.active[1] if self.active else None,
                'scheduling_version':SCHEDULING_VERSION,
                'news_capture_interval_seconds':60,'news_capture_in_flight':self.news_future is not None,
                'news_refresh_cadence_basis':'completion_or_failure_plus_interval',
                'news_refresh_wait_seconds':max(0.,getattr(self,'news_refresh_next_monotonic',0.)-self.monotonic()),
                'news_refresh_waits_for_pair_work':self.future is not None,
                'news_bootstrap_phase':('complete_fresh_capture_still_required' if getattr(self,'news_bootstrap_complete',False)
                    else 'running' if getattr(self,'news_bootstrap_future',None) is not None else 'pending_or_retry'),
                'news_bootstrap_report':getattr(self,'news_bootstrap_report',None),
                'shared_history_prepared':self.history_share is not None,'shared_history_origin_limit':193,
                'observed_news_failure_serial':self.news_schedule.failure_serial,
                'retry_basis':'input_capture_sha256,cadence_bucket,news_failure_serial',
                'scheduler_diagnostic_errors':getattr(self,'scheduler_diagnostic_errors',0),
                'scheduler_events_total':getattr(self,'scheduler_events_total',0),
                'scheduler_events_evicted':getattr(self,'scheduler_events_evicted',0),
                'scheduler_events_retained':len(getattr(self,'scheduler_events',[])),
                'scheduler_events':list(getattr(self,'scheduler_events',[])),
                'scheduler_event_retention':'latest_32_in_memory_published_with_existing_heartbeat'}
            atomic_json(self.study/'heartbeat.json',heartbeat);self.last_heartbeat=now

    def tick(self):
        # Drain and hand off completed work before ancillary quote/settlement I/O.
        self.finish_news();self.finish_work()
        # A completed pair capture gets the existing guarded fair-fit
        # opportunity before an overdue refresh can monopolize the lock.
        # If no fit qualifies, news is free to proceed; no forced signal.
        if (getattr(self,'future',None) is None and self.news_future is None
                and getattr(self,'news_bootstrap_future',None) is None
                and getattr(self,'fresh_capture_pair',None) is not None):
            handoff_now=number(self.clock());self.schedule_fit(handoff_now,int(handoff_now//900))
        self.schedule_news();self.schedule_work()
        now=number(self.clock())
        if now-self.last_poll>=2:
            self.poll_quotes();self.last_poll=now
            for pair,state in self.states.items():
                for slot in state['families'].values():
                    try:slot['ledger'].settle()
                    except Exception as exc:self.error(pair,exc)
        self.finish_work();self.schedule_work();self.score()
        try:self.publish_status()
        except (OSError,summary_boundary.SummaryBoundaryError) as exc:
            _status_failure_record(self,exc)

    def close(self):
        self.news_pool.shutdown(wait=True,cancel_futures=True)
        self.pool.shutdown(wait=True,cancel_futures=True);self.score_pool.shutdown(wait=True,cancel_futures=True)
        for state in self.states.values():
            for slot in state['families'].values():slot['ledger'].close()


def resolve_runtime_paths(*,study=STUDY,candle_root=None,quote_path=None,news_io_config=None):
    """New cohort paths only; the exact I/O config owns publisher/observer/health roots."""
    study=plain_c_path(study)
    if study.name!='joint_price_news_study_v7':raise ValueError('distinct_v7_study_root_required')
    if news_io_config is None:raise ValueError('explicit_revision_news_io_config_required')
    root=study.parent
    return {'study':study,'candle_root':plain_c_path(candle_root or root/'candles'),
        'quote_path':plain_c_path(quote_path or root/'state/practice_007_market_quotes_v1.json'),
        'news_io_config':plain_c_path(news_io_config)}


def create_news_session(registry,path):
    raw=bounded_bytes(plain_c_path(path),1024**2)
    if hashlib.sha256(raw).hexdigest()!=registry.get('news_io_config_sha256'):raise ValueError('registry_news_io_configuration_binding')
    config=_bound_inputs().news_io.strict_health_json(raw)
    return _bound_inputs().news_io.create_session(config)

def run(config=DEFAULT_CONFIG,*,duration_sec=0,once=False,study=STUDY,candle_root=None,quote_path=None,news_io_config=None):
    paths=resolve_runtime_paths(study=study,candle_root=candle_root,quote_path=quote_path,news_io_config=news_io_config)
    registry=load_registry(config)
    verify_activated_study(registry,paths['study'])
    for variable in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','LOKY_MAX_CPU_COUNT'):
        os.environ[variable]='1'
    paths['study'].mkdir(parents=True,exist_ok=True)
    import msvcrt
    lock=(paths['study']/'worker.lock').open('a+b');lock.seek(0)
    if not lock.read(1):lock.write(b'0');lock.flush()
    lock.seek(0)
    try:msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
    except OSError:lock.close();raise RuntimeError('pair_worker_already_running')
    runner=None;begun=time.monotonic()
    try:
        session=create_news_session(registry,paths['news_io_config'])
        runner=PairRunner(registry,paths['study'],paths['candle_root'],paths['quote_path'],session=session,activate=False)
        if once:
            runner.poll_quotes();runner.publish_status(force=True);return
        while True:
            runner.tick()
            if duration_sec>0 and time.monotonic()-begun>=duration_sec:break
            time.sleep(.2)
    finally:
        if runner is not None:runner.close()
        lock.seek(0);msvcrt.locking(lock.fileno(),msvcrt.LK_UNLCK,1);lock.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=DEFAULT_CONFIG)
    parser.add_argument('--duration-sec',type=float,default=0)
    parser.add_argument('--once',action='store_true',help='Observe quotes and status once without numeric attempts')
    parser.add_argument('--study',type=Path,default=STUDY)
    parser.add_argument('--candle-root',type=Path)
    parser.add_argument('--quote-path',type=Path)
    parser.add_argument('--news-io-config',type=Path,required=True)
    args=parser.parse_args();run(args.config,duration_sec=args.duration_sec,once=args.once,study=args.study,
        candle_root=args.candle_root,quote_path=args.quote_path,news_io_config=args.news_io_config)


if __name__=='__main__':main()
