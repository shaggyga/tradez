"""Independent, order-incapable per-pair research forecasts and observations.

Each registered pair owns a separate immutable ledger and cadence. No broker
API, execution feed, lifecycle, promotion or order capability is imported.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path, PureWindowsPath
from contextlib import closing
import sqlite3
import stat
import platform
import re
import time

from oanda_causal_forecast_ledger_pair_v2 import CausalForecastLedger, digest, encoded, validate_contract, number, utc
from oanda_fixed_forecast_evaluation_pair_v2 import evaluate, jsonable, loads_exact_prices, forecast_errors
from oanda_exact_price_scoring import decimal_value, quote_midpoint

ROOT=Path(__file__).resolve().parent
DEFAULT_CONFIG=ROOT/'config/pair_local_forecast_study_v3_20260913.json'
STUDY=ROOT/'data/oanda_training_manager/market_open_20260913_v1/pair_local_forecast_study_v3'
COHORT_SCOPE='market_open_price_controls_v3_20260913'
FAMILIES=frozenset({'ridge_return_repaired','probabilistic_state_space'})
REGISTRY_SCHEMA='pair_local_forecast_registry_v3_20260913'
SUMMARY_SCHEMA='pair_local_forecast_summary_v3_20260913'
HEARTBEAT_SCHEMA='pair_local_forecast_heartbeat_v3_20260913'
SCHEDULING_VERSION='pair_capture_fit_fair_v3_20260913'
REQUIRED_SOURCE_BINDINGS=frozenset({
 'oanda_pair_local_forecast_study_v3.py','oanda_causal_forecast_ledger_pair_v2.py',
 'oanda_fixed_forecast_evaluation_pair_v2.py','oanda_pair_local_models_v2.py',
 'oanda_causal_forecast_inputs_pair_v2.py','oanda_causal_forecast_inputs.py',
 'oanda_causal_forecast_inputs_gap_v2.py','oanda_exact_price_scoring.py',
 'oanda_causal_prediction_baselines.py'})

def bounded_bytes(path,limit):
    with Path(path).open('rb') as handle:raw=handle.read(limit+1)
    if len(raw)>limit:raise ValueError('source_size_limit')
    return raw

def atomic_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+f'.{os.getpid()}.{time.time_ns()}.tmp')
    owned=False
    try:
        with temporary.open('xb') as handle:
            owned=True;handle.write(encoded(value));handle.flush();os.fsync(handle.fileno())
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

def load_registry(path):
    path=plain_c_path(path)
    registry=json.loads(bounded_bytes(path,1024*1024))
    if registry.get('schema_version')!=REGISTRY_SCHEMA or registry.get('registry_id')!='pair_local_forecast_study_v3_20260913':
        raise ValueError('registry_identity')
    if registry.get('collection_enabled') is not True or registry.get('research_only') is not True:
        raise ValueError('research_collection_required')
    for flag in ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported'):
        if registry.get(flag) is not False:raise ValueError('inert_registry_required')
    if set(registry.get('source_bindings',{}))!=REQUIRED_SOURCE_BINDINGS:raise ValueError('complete_source_bindings_required')
    for name,expected in registry['source_bindings'].items():
        if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=expected:raise ValueError('source_binding_mismatch:'+name)
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
        if set(item.get('families',{})) != FAMILIES:raise ValueError('two_independent_family_slots_required')
        for family, slot in item['families'].items():
            contract=slot['contract'];validate_contract(contract)
            identity=contract.get('cohorts',{}).get(family,'')
            prefix=f'causal_pair_family_v2_20260907.{pair}.{family}.{COHORT_SCOPE}.'
            if not isinstance(identity,str) or not identity.startswith(prefix) or not re.fullmatch(r'[a-z0-9][a-z0-9_]{0,63}',identity[len(prefix):]):
                raise ValueError('distinct_opening_control_cohort_required')
            if contract.get('contract_id')!=identity or contract['evaluation_protocol'].get('contract_id')!=identity+'.evaluation':
                raise ValueError('distinct_opening_control_contract_required')
            if contract['instrument']!=pair or contract['family']!=family or decimal_value(item['pip_size'])!=decimal_value(contract['pip_size']):
                raise ValueError('registered_pair_family_or_pip_mismatch')
            if digest(contract)!=slot['contract_sha256']:raise ValueError('contract_hash_mismatch')
            if contract.get('source_bindings')!=registry['source_bindings'] or contract.get('dependency_versions')!=dependencies:
                raise ValueError('pair_source_or_dependency_mismatch')
            if contract['numeric_model_source_sha256']!=registry['source_bindings']['oanda_pair_local_models_v2.py']:
                raise ValueError('numeric_model_binding_mismatch')
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
    if resolved.name!='pair_local_forecast_study_v3' or resolved.exists():
        raise ValueError('wholly_absent_v3_study_root_required')
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
        receipt={'schema_version':'pair_local_price_controls_v3_activation_receipt_20260913','status':'activated_empty',
            'registry_sha256':digest(registry),'registry_path':str(Path(config).resolve()),
            'registry_file_sha256':hashlib.sha256(Path(config).read_bytes()).hexdigest(),
            'source_bindings':registry['source_bindings'],'activation_started_epoch':started,
            'activation_completed_epoch':completed,'study_root':str(resolved),'ledgers':records,
            'historical_rows_imported':False,'worker_started':False,'research_only':True,
            **{flag:False for flag in INERT_FLAGS}}
        atomic_json(resolved/'activation_receipt.json',receipt)
        return receipt
    except BaseException as exc:
        atomic_json(resolved/'activation_failed.json',{'schema_version':'pair_local_price_controls_v3_failed_activation_20260913',
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
                raise ValueError('complete_prior_v3_activation_required')
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
                db.execute('PRAGMA query_only=ON')
                saved=db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
                activation=db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
                if saved!=(spec['contract_sha256'],encoded(spec['contract']).decode()):
                    raise ValueError('activated_contract_mismatch')
                if activation is None or activation[1]!=spec['contract_sha256'] or not 0<number(activation[0])<=observed:
                    raise ValueError('actual_v3_activation_required')
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
        interpretation='Independent family research cohort. Probabilities are uncalibrated; availability is not accuracy or execution authority.')
    atomic_json(destination,report)
    return report


def verified_publication(ledger):
    """Verify committed forecast, publication and consumer receipts independently."""
    rows=ledger._read('''SELECT f.id,f.sha,f.payload,p.epoch AS published,p.forecast_sha,
        c.epoch AS consumed,c.forecast_sha AS consumed_sha,c.publication_sha,a.id AS attempt_id,
        q.payload AS reference FROM forecasts f JOIN publication p ON p.id=f.id
        JOIN consumption c ON c.id=f.id JOIN attempts a ON a.id=f.attempt_id
        JOIN quotes q ON q.id=a.reference_id ORDER BY f.bucket DESC LIMIT 1''')
    if not rows:return None
    row=rows[0];payload=json.loads(row['payload']);quote=json.loads(row['reference'])
    contract=ledger.contract;published=number(row['published']);consumed=number(row['consumed']);now=number(ledger.clock())
    publication_receipt={'epoch':published,'forecast_sha':row['sha']}
    publication_hash=digest(publication_receipt)
    if (digest(payload)!=row['sha'] or row['sha']!=row['forecast_sha'] or row['sha']!=row['consumed_sha']
        or payload['decision_id']!=row['id'] or row['publication_sha']!=publication_hash):
        raise ValueError('summary_publication_or_consumer_hash_mismatch')
    pair=contract['instrument'];pip=decimal_value(contract['pip_size']);family=contract['family']
    if (payload.get('instrument')!=pair or quote.get('instrument')!=pair or payload.get('family')!=family
        or payload.get('attempt_id')!=row['attempt_id']
        or decimal_value(payload.get('pip_size'))!=pip or decimal_value(quote.get('pip_size'))!=pip
        or payload.get('reference_quote_id')!=quote.get('quote_id')):
        raise ValueError('summary_pair_reference_mismatch')
    available=number(quote['available_epoch']);market=number(quote['market_epoch'])
    target=number(payload['target_epoch']);arms=payload['forecasts']
    if (not ledger.activated_epoch<available<=published<=consumed<=now or not 0<=available-market<=60
        or target!=market+3600 or payload['reference_epoch']!=market or not consumed<target
        or len(arms)!=1 or arms[0]['family']!=family):
        raise ValueError('summary_publication_clock_or_family')
    arm=arms[0];issue=number(arm['issued_epoch']);p=number(arm['probability_up']);number(arm['predicted_return_bps'])
    protocol={**contract['evaluation_protocol'],'historical_start_utc':utc(ledger.activated_epoch)}
    errors=forecast_errors({**arm,'committed_available_epoch':consumed},payload,protocol)
    if (errors or arm.get('forecast_id')!=row['id']+':'+family
        or any(arm.get(flag) is not False for flag in ('can_place_orders','account_eligible','proof_eligible'))):
        raise ValueError('summary_full_forecast_contract_binding')
    if (arm['instrument']!=pair or decimal_value(arm['pip_size'])!=pip
        or arm['cohort_id']!=contract['cohorts'][family]
        or arm['reference_epoch']!=market or arm['target_epoch']!=target
        or decimal_value(arm['reference_mid'])!=quote_midpoint(quote)
        or not available<=issue<=published or not 0<=p<=1
        or type(arm['side']) is not int or arm['side'] not in (-1,0,1)):
        raise ValueError('summary_arm_binding')
    compact={key:arm[key] for key in ('family','cohort_id','instrument','pip_size','horizon_sec','reference_epoch',
        'reference_mid','issued_epoch','target_epoch','side','probability_up','predicted_return_bps')}
    return {'decision_id':row['id'],'instrument':pair,'family':family,'horizon_sec':3600,'publication_epoch':published,
        'reference_epoch':market,'reference_available_epoch':available,'issued_epoch':issue,'target_epoch':target,
        'forecast_sha256':row['sha'],'publication_receipt_sha256':publication_hash,
        'publication_verified':True,'publication_verified_epoch':now,'consumption_verified':True,'consumption_epoch':consumed,
        'consumer_receipt_sha256':digest({'epoch':consumed,'forecast_sha':row['sha'],'publication_sha':publication_hash}),
        'forecasts':[compact]}


def input_readiness(capture,family,now):
    if not capture:return {'observed_epoch':None,'status':'unavailable','reason':'awaiting_pair_input_read','diagnostics':{}}
    if capture.get('first_observed_epoch') is None:
        return {'observed_epoch':None,'status':'unavailable','reason':'|'.join(capture.get('reasons',[])) or 'pair_input_read_failed','diagnostics':{}}
    observed=number(capture['first_observed_epoch'])
    diagnostics=capture.get('family_readiness',{}).get(family,{})
    reason='|'.join(diagnostics.get('reasons',[]) or capture.get('reasons',[]))
    status='ready' if capture.get('status')=='ready' and diagnostics.get('ready') is True else 'blocked'
    maturity=capture.get('max_bar_close_epoch')
    if maturity is None or not 0<=now-number(maturity)<=900:
        status='unavailable';reason='stale_or_future_pair_input'
    return {'observed_epoch':observed,'status':status,'reason':reason,'diagnostics':diagnostics}


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
                'cohort_id':ledger.contract['cohorts'][family],'current_readiness':readiness,
                'last_attempt':slot.get('last_attempt'),'latest_forecast':forecast,'counts':ledger.counts()}
        rows.append({'instrument':pair,'pip_size':item['pip_size'],'families':slots})
    summary={'schema_version':SUMMARY_SCHEMA,'registry_sha256':digest(registry),'generated_epoch':now,
        'research_only':True,**{flag:False for flag in INERT_FLAGS},'rows':rows}
    summary['payload_sha256']=digest(summary)
    if len(encoded(summary))>1024*1024:raise ValueError('summary_size_limit')
    return summary


def capture_once(candle_root,pair,pip):
    from oanda_causal_forecast_inputs_pair_v2 import capture_inputs
    return capture_inputs(candle_root,pair,pip_size=pip)


def fit_capture(capture,family):
    from oanda_causal_forecast_inputs_pair_v2 import compute_predictions
    return compute_predictions(capture,families=(family,))


class PairRunner:
    """Serial fitting, continuous quotes, independent successful family slots."""
    def __init__(self,registry,study,candle_root,quote_path,*,clock=time.time,activate=False):
        self.registry=registry;self.study=plain_c_path(study);self.candle_root=plain_c_path(candle_root);self.quote_path=plain_c_path(quote_path)
        self.clock=clock;self.states={};self.current={};self.queue=sorted(registry['pairs']);self.cursor=0
        self.capture_cursor=0;self.fit_cursor=0;self.family_cursors={};self.prefer_fit=True
        self.future=None;self.active=None;self.score_future=None;self.score_key=None
        self.pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='pair-v3-fit')
        self.score_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='pair-v3-score')
        self.last_poll=self.last_summary=self.last_heartbeat=0.0;self.summary=None;self.errors=0
        self.heartbeat_errors=0;self.last_error=''
        try:
            for pair,item in registry['pairs'].items():
                state={'families':{},'quote_reason':'awaiting_current_quote','capture':None,'source_signature':None,'last_scan':0.0}
                self.states[pair]=state
                for family,registered in item['families'].items():
                    ledger=CausalForecastLedger(plain_c_path(self.study/'pairs'/pair/family/'study.sqlite'),registered['contract'],clock=clock,activate=activate)
                    slot={'ledger':ledger,'reason':'awaiting_family_attempt','building':False,'last_try':0.0,
                        'last_success_bucket':-1,'failed_basis':None,'last_score':0.0,'scored_outcomes':0}
                    state['families'][family]=slot
                    row=ledger.db.execute('SELECT MAX(bucket) FROM forecasts').fetchone()
                    if row[0] is not None:slot['last_success_bucket']=row[0]
                    last=ledger.db.execute('SELECT id,bucket,epoch FROM attempts ORDER BY epoch DESC LIMIT 1').fetchone()
                    if last:slot['last_attempt']={'attempt_id':last['id'],'bucket':last['bucket'],'epoch':last['epoch'],'reason':'retained_attempt'}
                    ledger.settle();slot['publication']=verified_publication(ledger)
        except BaseException:
            self.close();raise

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
        try:
            value=self.future.result()
            if kind=='capture':
                state['capture']=value;state['source_signature']=detail
                observation={'instrument':pair,'observed_epoch':value.get('first_observed_epoch'),'event_observed_epoch':number(self.clock()),
                    'source_capture_sha256':value.get('source_capture_sha256'),'status':value.get('status'),
                    'reasons':value.get('reasons',[]),'family_readiness':value.get('family_readiness',{})}
                with (self.study/'readiness.jsonl').open('ab') as handle:handle.write(encoded(observation)+b'\n')
            else:
                family,attempt_id,capture,basis=detail;slot=state['families'][family];ledger=slot['ledger']
                if value.get('status') in ('ready','partial') and family in value.get('predictions',{}):
                    ledger.issue(attempt_id,capture,value);ledger.consume_publications()
                    slot['publication']=verified_publication(ledger);slot['last_success_bucket']=slot['last_attempt']['bucket']
                    slot['reason']='';slot['last_attempt']['reason']='published';slot['failed_basis']=None
                else:
                    reason='|'.join(value.get('reasons',[])) or 'selected_family_unavailable'
                    ledger.record_abstention(attempt_id,{'reason':reason},capture=capture,result=value)
                    slot['reason']=reason;slot['last_attempt']['reason']=reason;slot['failed_basis']=basis
        except Exception as exc:
            reason=self.error(pair,exc)
            if kind=='capture':
                state['source_signature']=detail;state['capture']=None;state['capture_error']=reason
            else:
                family,attempt_id,capture,basis=detail;slot=state['families'][family]
                slot['reason']=reason;slot['last_attempt']['reason']=reason;slot['failed_basis']=basis
                try:slot['ledger'].record_abstention(attempt_id,{'reason':reason},capture=capture)
                except Exception as diagnostic_exc:self.error(pair,diagnostic_exc)
        finally:
            if kind=='fit':state['families'][detail[0]]['building']=False
            self.future=None;self.active=None

    def schedule_work(self):
        if self.future is not None:return
        now=number(self.clock());bucket=int(now//900)
        # Candle capture and fitting have independent fair cursors. A source
        # changing on every visit must not indefinitely displace a usable,
        # already observed capture. Alternate dispatched work, with fallback
        # when either class has no eligible work.
        phases=(self.schedule_fit,self.schedule_capture) if getattr(self,'prefer_fit',True) else (self.schedule_capture,self.schedule_fit)
        for phase in phases:
            if phase(now,bucket):return

    def schedule_capture(self,now,bucket):
        for _ in range(len(self.queue)):
            index=getattr(self,'capture_cursor',0)
            pair=self.queue[index];self.capture_cursor=(index+1)%len(self.queue);state=self.states[pair]
            if now-state['last_scan']>=30:
                state['last_scan']=now
                try:
                    stat=plain_c_path(self.candle_root/f'{pair}_M1.csv').stat();signature=(stat.st_size,stat.st_mtime_ns)
                except OSError:
                    state['capture']=None;continue
                if signature!=state['source_signature']:
                    self.active=('capture',pair,signature)
                    self.future=self.pool.submit(capture_once,self.candle_root,pair,self.registry['pairs'][pair]['pip_size'])
                    self.prefer_fit=True
                    return True
        return False

    def schedule_fit(self,now,bucket):
        start=getattr(self,'fit_cursor',0)
        for pair in self.queue[start:]+self.queue[:start]:
            state=self.states[pair]
            capture=state.get('capture')
            families=sorted(state['families']);cursors=getattr(self,'family_cursors',{})
            cursor=cursors.get(pair,0)%len(families)
            for offset in range(len(families)):
                family=families[(cursor+offset)%len(families)];slot=state['families'][family]
                quote=self.current.get(pair,{}).get(family)
                if not quote or slot['last_success_bucket']>=bucket or now-slot['last_try']<30:continue
                if input_readiness(capture,family,now)['status']!='ready':continue
                if number(capture['first_observed_epoch'])>now:continue
                if not 0<=now-quote['market_epoch']<=60:continue
                basis=(capture['source_capture_sha256'],quote['quote_id'],bucket)
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
                    self.active=('fit',pair,(family,attempt_id,capture,basis))
                    self.future=self.pool.submit(fit_capture,capture,family)
                    self.prefer_fit=False
                    cursors[pair]=(cursor+offset+1)%len(families);self.family_cursors=cursors
                    self.fit_cursor=(self.queue.index(pair)+1)%len(self.queue)
                    return True
                except Exception as exc:
                    reason=self.error(pair,exc);slot['reason']=reason;slot['building']=False
                    if attempt_id is not None:
                        slot['failed_basis']=basis
                        if slot.get('last_attempt',{}).get('attempt_id')!=attempt_id:
                            slot['last_attempt']={'epoch':None,'attempt_id':attempt_id,'bucket':bucket}
                        slot['last_attempt']['reason']=reason
                        try:slot['ledger'].record_abstention(attempt_id,{'reason':reason},capture=capture)
                        except Exception as diagnostic_exc:self.error(pair,diagnostic_exc)
                    self.future=None;self.active=None
        return False

    def score(self):
        if self.score_future is not None and self.score_future.done():
            pair,family=self.score_key;slot=self.states[pair]['families'][family]
            try:slot['scored_outcomes']=self.score_future.result()['collection_counts']['outcomes']
            except Exception as exc:self.error(pair,exc)
            self.score_future=None;self.score_key=None
        if self.score_future is not None:return
        now=number(self.clock())
        for pair,state in self.states.items():
            for family,slot in state['families'].items():
                if now-slot['last_score']<60:continue
                count=slot['ledger'].db.execute('SELECT COUNT(*) FROM outcomes').fetchone()[0]
                if count<=slot['scored_outcomes']:continue
                slot['last_score']=now;self.score_key=(pair,family)
                self.score_future=self.score_pool.submit(write_scorecard,slot['ledger'],self.study/'pairs'/pair/family/'scorecard.json')
                return

    def publish_status(self,*,force=False):
        now=number(self.clock())
        if force or now-self.last_summary>=15:
            for pair,state in self.states.items():
                for slot in state['families'].values():
                    try:slot['publication']=verified_publication(slot['ledger'])
                    except Exception as exc:slot['publication']=None;self.error(pair,exc)
            now=number(self.clock());summary=build_summary(self.registry,self.states,now)
            atomic_json(self.study/'summary.json',summary);self.summary=summary;self.last_summary=now
        if self.summary is not None and (force or now-self.last_heartbeat>=5):
            slots=[slot for row in self.summary['rows'] for slot in row['families'].values()]
            counts={status:sum(slot['status']==status for slot in slots) for status in ('forecast','warming','building','unavailable','ready')}
            heartbeat={'schema_version':HEARTBEAT_SCHEMA,'generated_epoch':number(self.clock()),
                'scheduling_version':SCHEDULING_VERSION,
                'registry_sha256':digest(self.registry),'summary_sha256':digest(self.summary),'research_only':True,
                **{flag:False for flag in INERT_FLAGS},'phase':'research_collection','supported_decision':'no_trade',
                'pair_count':len(self.states),'family_count':len(slots),'counts':counts,
                'pairs_with_forecast':sum(any(s['status']=='forecast' for s in row['families'].values()) for row in self.summary['rows']),
                'errors':self.errors,'last_error':self.last_error[:4096],'heartbeat_publication_errors':self.heartbeat_errors,
                'active_pair':self.active[1] if self.active else None}
            atomic_json(self.study/'heartbeat.json',heartbeat);self.last_heartbeat=now

    def tick(self):
        now=number(self.clock())
        if now-self.last_poll>=2:
            self.poll_quotes();self.last_poll=now
            for pair,state in self.states.items():
                for slot in state['families'].values():
                    try:slot['ledger'].settle()
                    except Exception as exc:self.error(pair,exc)
        self.finish_work();self.schedule_work();self.score()
        try:self.publish_status()
        except OSError as exc:
            self.errors+=1;self.heartbeat_errors+=1;self.last_error='status_publication:'+type(exc).__name__

    def close(self):
        self.pool.shutdown(wait=True,cancel_futures=True);self.score_pool.shutdown(wait=True,cancel_futures=True)
        for state in self.states.values():
            for slot in state['families'].values():slot['ledger'].close()


def resolve_runtime_paths(*,study=STUDY,candle_root=None,quote_path=None):
    study=plain_c_path(study)
    if study.name!='pair_local_forecast_study_v3':raise ValueError('distinct_v3_study_root_required')
    root=study.parent
    return {'study':study,'candle_root':plain_c_path(candle_root or root/'candles'),
            'quote_path':plain_c_path(quote_path or root/'state/practice_007_market_quotes_v1.json')}


def run(config=DEFAULT_CONFIG,*,duration_sec=0,once=False,study=STUDY,candle_root=None,quote_path=None):
    paths=resolve_runtime_paths(study=study,candle_root=candle_root,quote_path=quote_path)
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
        runner=PairRunner(registry,paths['study'],paths['candle_root'],paths['quote_path'],activate=False)
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
    args=parser.parse_args();run(args.config,duration_sec=args.duration_sec,once=args.once,study=args.study,
        candle_root=args.candle_root,quote_path=args.quote_path)


if __name__=='__main__':main()

