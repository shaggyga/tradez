"""Joint technical and news research forecasts with independently observed evidence.

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
from pathlib import Path
import platform
import re
import time

from oanda_causal_forecast_ledger_joint_news_v1 import CausalForecastLedger, digest, encoded, validate_contract, number, utc
from oanda_fixed_forecast_evaluation_joint_news_v1 import evaluate, jsonable, loads_exact_prices, forecast_errors
from oanda_exact_price_scoring import decimal_value, quote_midpoint

ROOT=Path(__file__).resolve().parent
DEFAULT_CONFIG=ROOT/'config/joint_price_news_study_v1_20260907.json'
STUDY=ROOT/'data/oanda_training_manager/joint_price_news_study_v1'
FAMILIES=frozenset({'ridge_price_news_v1'})
REGISTRY_SCHEMA='joint_price_news_registry_v1_20260907'
SUMMARY_SCHEMA='joint_price_news_forecast_summary_v1_20260907'
HEARTBEAT_SCHEMA='joint_price_news_forecast_heartbeat_v1_20260907'
REQUIRED_SOURCE_BINDINGS=frozenset({
 'oanda_joint_price_news_forecast_study_v1.py','oanda_causal_forecast_ledger_joint_news_v1.py',
 'oanda_fixed_forecast_evaluation_joint_news_v1.py','oanda_joint_price_news_models_v1.py',
 'oanda_causal_forecast_inputs_joint_news_v1.py','oanda_causal_forecast_inputs.py',
 'oanda_causal_forecast_inputs_gap_v2.py','oanda_exact_price_scoring.py',
 'oanda_causal_prediction_baselines.py','oanda_pair_local_models_v2.py',
 'oanda_causal_forecast_inputs_pair_v2.py','oanda_news_causal_aggregation_guard_v1.py',
 'oanda_news_classification_contract.py','oanda_local_news_sentiment.py',
 'oanda_source_governance.py','oanda_source_governance_news_fast_lane.py'})

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

def load_registry(path):
    registry=json.loads(bounded_bytes(path,1024*1024))
    if registry.get('schema_version')!=REGISTRY_SCHEMA or registry.get('registry_id')!='joint_price_news_study_v1_20260907':
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
        if set(item.get('families',{})) != FAMILIES:raise ValueError('registered_joint_family_slot_required')
        for family, slot in item['families'].items():
            contract=slot['contract'];validate_contract(contract)
            if contract['instrument']!=pair or contract['family']!=family or decimal_value(item['pip_size'])!=decimal_value(contract['pip_size']):
                raise ValueError('registered_pair_family_or_pip_mismatch')
            if digest(contract)!=slot['contract_sha256']:raise ValueError('contract_hash_mismatch')
            if contract.get('source_bindings')!=registry['source_bindings'] or contract.get('dependency_versions')!=dependencies:
                raise ValueError('pair_source_or_dependency_mismatch')
            if contract['numeric_model_source_sha256']!=registry['source_bindings']['oanda_joint_price_news_models_v1.py']:
                raise ValueError('numeric_model_binding_mismatch')
            cohorts.extend(contract['cohorts'].values())
    if len(cohorts)!=len(set(cohorts)):raise ValueError('cohort_reuse')
    return registry

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
    diagnostics=arm.get('diagnostics',{})
    compact['input_contributions']={key:diagnostics[key] for key in (
        'matched_price_only_expected_pips','neutral_news_ablation_expected_pips','news_ablation_difference_pips',
        'training_rows','nonzero_news_context_training_rows','vetted_news_training_rows',
        'news_feature_names','current_news_features','news_ablation_scope','training_scope','probability_scope') if key in diagnostics}
    for key in ('news_capture_sha256','news_evidence_epoch','news_generated_epoch','news_first_observed_epoch',
                'news_available_epoch','news_expires_epoch'):
        compact[key]=arm[key]
    return {'decision_id':row['id'],'instrument':pair,'family':family,'horizon_sec':3600,'publication_epoch':published,
        'reference_epoch':market,'reference_available_epoch':available,'issued_epoch':issue,'target_epoch':target,
        'forecast_sha256':row['sha'],'publication_receipt_sha256':publication_hash,
        'publication_verified':True,'publication_verified_epoch':now,'consumption_verified':True,'consumption_epoch':consumed,
        'consumer_receipt_sha256':digest({'epoch':consumed,'forecast_sha':row['sha'],'publication_sha':publication_hash}),
        'forecasts':[compact]}


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
    from oanda_causal_forecast_inputs_joint_news_v1 import capture_inputs
    return capture_inputs(candle_root,pair,pip_size=pip)


def fit_capture(capture,family):
    from oanda_causal_forecast_inputs_joint_news_v1 import compute_predictions
    return compute_predictions(capture,families=(family,))


def source_signature(candle_root,pair):
    from oanda_causal_forecast_inputs_joint_news_v1 import source_signature as combined_signature
    return combined_signature(candle_root,pair)


class PairRunner:
    """Serial fitting, continuous quotes, independent successful family slots."""
    def __init__(self,registry,study,candle_root,quote_path,*,clock=time.time,activate=False):
        self.registry=registry;self.study=Path(study);self.candle_root=Path(candle_root);self.quote_path=Path(quote_path)
        self.clock=clock;self.states={};self.current={};self.queue=sorted(registry['pairs']);self.cursor=0
        self.future=None;self.active=None;self.score_future=None;self.score_key=None
        self.pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='joint-price-news-fit')
        self.score_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='joint-price-news-score')
        self.last_poll=self.last_summary=self.last_heartbeat=0.0;self.summary=None;self.errors=0
        self.heartbeat_errors=0;self.last_error=''
        try:
            for pair,item in registry['pairs'].items():
                state={'families':{},'quote_reason':'awaiting_current_quote','capture':None,'source_signature':None,'last_scan':0.0}
                self.states[pair]=state
                for family,registered in item['families'].items():
                    ledger=CausalForecastLedger(self.study/'pairs'/pair/family/'study.sqlite',registered['contract'],clock=clock,activate=activate)
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
        for _ in range(len(self.queue)):
            pair=self.queue[self.cursor];self.cursor=(self.cursor+1)%len(self.queue);state=self.states[pair]
            if now-state['last_scan']>=30:
                state['last_scan']=now
                try:
                    signature=source_signature(self.candle_root,pair)
                except OSError:
                    state['capture']=None;continue
                if signature!=state['source_signature']:
                    self.active=('capture',pair,signature)
                    self.future=self.pool.submit(capture_once,self.candle_root,pair,self.registry['pairs'][pair]['pip_size'])
                    return
            capture=state.get('capture')
            for family,slot in state['families'].items():
                quote=self.current.get(pair,{}).get(family)
                if not quote or slot['last_success_bucket']>=bucket or now-slot['last_try']<30:continue
                if input_readiness(capture,family,now)['status']!='ready':continue
                if not 0<=now-quote['market_epoch']<=60:continue
                basis=(capture['source_capture_sha256'],quote['quote_id'],bucket)
                if slot['failed_basis']==basis:continue
                slot['last_try']=now
                try:
                    attempt_id=slot['ledger'].begin_attempt(bucket,quote)
                    if attempt_id is None:slot['last_success_bucket']=bucket;continue
                    slot['last_attempt']={'epoch':number(self.clock()),'reason':'building','attempt_id':attempt_id,'bucket':bucket}
                    slot['building']=True
                    self.active=('fit',pair,(family,attempt_id,capture,basis))
                    self.future=self.pool.submit(fit_capture,capture,family)
                    return
                except Exception as exc:slot['reason']=self.error(pair,exc)

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


def run(config=DEFAULT_CONFIG,*,duration_sec=0,once=False,activate=False):
    registry=load_registry(config)
    for variable in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','LOKY_MAX_CPU_COUNT'):
        os.environ[variable]='1'
    STUDY.mkdir(parents=True,exist_ok=True)
    import msvcrt
    lock=(STUDY/'worker.lock').open('a+b');lock.seek(0)
    if not lock.read(1):lock.write(b'0');lock.flush()
    lock.seek(0)
    try:msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
    except OSError:lock.close();raise RuntimeError('pair_worker_already_running')
    runner=None;begun=time.monotonic()
    try:
        runner=PairRunner(registry,STUDY,ROOT/'data/oanda_training_manager/candles',
            ROOT/'data/oanda_training_manager/state/practice_007_market_quotes_v1.json',activate=activate)
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
    parser.add_argument('--activate',action='store_true',help='Explicitly initialize only this new version of family ledgers')
    args=parser.parse_args();run(args.config,duration_sec=args.duration_sec,once=args.once,activate=args.activate)


if __name__=='__main__':main()

