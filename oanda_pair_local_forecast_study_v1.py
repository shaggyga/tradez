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
from pathlib import Path
import platform
import re
import time

from oanda_causal_forecast_ledger_pair_v1 import CausalForecastLedger, digest, encoded, validate_contract, number
from oanda_fixed_forecast_evaluation_pair_v1 import evaluate, jsonable, loads_exact_prices
from oanda_exact_price_scoring import decimal_value, quote_midpoint

ROOT=Path(__file__).resolve().parent
DEFAULT_CONFIG=ROOT/'config/pair_local_forecast_study_v1_20260907.json'
STUDY=ROOT/'data/oanda_training_manager/pair_local_forecast_study_v1'
FAMILIES=frozenset({'ridge_return_repaired','probabilistic_state_space'})
REGISTRY_SCHEMA='pair_local_forecast_registry_v1_20260907'
SUMMARY_SCHEMA='pair_local_forecast_summary_v1_20260907'
HEARTBEAT_SCHEMA='pair_local_forecast_heartbeat_v1_20260907'
REQUIRED_SOURCE_BINDINGS=frozenset({
 'oanda_pair_local_forecast_study_v1.py','oanda_causal_forecast_ledger_pair_v1.py',
 'oanda_fixed_forecast_evaluation_pair_v1.py','oanda_pair_local_models_v1.py',
 'oanda_causal_forecast_inputs_pair_v1.py','oanda_causal_forecast_inputs.py',
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

def load_registry(path):
    registry=json.loads(bounded_bytes(path,1024*1024))
    if registry.get('schema_version')!=REGISTRY_SCHEMA or registry.get('registry_id')!='pair_local_forecast_study_v1_20260907':
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
        contract=item['contract'];validate_contract(contract)
        if contract['instrument']!=pair or decimal_value(item['pip_size'])!=decimal_value(contract['pip_size']):
            raise ValueError('registered_pair_or_pip_mismatch')
        if digest(contract)!=item['contract_sha256']:raise ValueError('contract_hash_mismatch')
        if contract.get('source_bindings')!=registry['source_bindings'] or contract.get('dependency_versions')!=dependencies:
            raise ValueError('pair_source_or_dependency_mismatch')
        if contract['numeric_model_source_sha256']!=registry['source_bindings']['oanda_pair_local_models_v1.py']:
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

def fit_once(candle_root,pair,pip):
    from oanda_causal_forecast_inputs_pair_v1 import capture_inputs,compute_predictions
    capture=capture_inputs(candle_root,pair,pip_size=pip)
    result=compute_predictions(capture) if capture.get('status')=='ready' else {'status':'abstain','reasons':capture.get('reasons',[])}
    return capture,result

def write_scorecard(ledger,destination):
    dataset,protocol=ledger.export_evaluation()
    report=jsonable(evaluate(dataset,protocol))
    report.update(collection_counts=dataset['collection_counts'],study_contract_sha256=ledger.contract_hash,
        interpretation='Independent pair-local research cohort; model probabilities are uncalibrated estimates, not measured accuracy or execution authority.')
    atomic_json(destination,report)
    return report


def verified_publication(ledger):
    """Read only committed evidence through a separate read-only connection."""
    rows=ledger._read('''SELECT f.id,f.sha,f.payload,p.epoch AS published,p.forecast_sha,
        q.payload AS reference FROM forecasts f JOIN publication p ON p.id=f.id
        JOIN attempts a ON a.bucket=f.bucket JOIN quotes q ON q.id=a.reference_id
        ORDER BY f.bucket DESC LIMIT 1''')
    if not rows:return None
    row=rows[0];payload=json.loads(row['payload']);quote=json.loads(row['reference'])
    contract=ledger.contract;published=number(row['published']);now=number(ledger.clock())
    if digest(payload)!=row['sha'] or row['sha']!=row['forecast_sha'] or payload['decision_id']!=row['id']:
        raise ValueError('summary_publication_hash_mismatch')
    pair=contract['instrument'];pip=decimal_value(contract['pip_size'])
    if (payload.get('instrument')!=pair or quote.get('instrument')!=pair
        or decimal_value(payload.get('pip_size'))!=pip or decimal_value(quote.get('pip_size'))!=pip
        or payload.get('reference_quote_id')!=quote.get('quote_id')):
        raise ValueError('summary_pair_reference_mismatch')
    available=number(quote['available_epoch']);market=number(quote['market_epoch'])
    target=number(payload['target_epoch']);arms=payload['forecasts']
    if (not ledger.activated_epoch<available<=published<=now or not 0<=available-market<=60
        or target!=market+3600 or payload['reference_epoch']!=market or not published<target
        or len(arms)!=2 or {arm['family'] for arm in arms}!=FAMILIES):
        raise ValueError('summary_publication_clock_or_arms')
    compact=[]
    for arm in arms:
        issue=number(arm['issued_epoch']);p=number(arm['probability_up']);bps=number(arm['predicted_return_bps'])
        if (arm['instrument']!=pair or decimal_value(arm['pip_size'])!=pip
            or arm['cohort_id']!=contract['cohorts'][arm['family']]
            or arm['reference_epoch']!=market or arm['target_epoch']!=target
            or decimal_value(arm['reference_mid'])!=quote_midpoint(quote)
            or not available<=issue<=published or not 0<=p<=1
            or type(arm['side']) is not int or arm['side'] not in (-1,0,1)):
            raise ValueError('summary_arm_binding')
        compact.append({key:arm[key] for key in ('family','cohort_id','instrument','reference_epoch',
            'reference_mid','issued_epoch','target_epoch','side','probability_up','predicted_return_bps')})
    return {'decision_id':row['id'],'instrument':pair,'horizon_sec':3600,'publication_epoch':published,
        'reference_available_epoch':available,'target_epoch':target,'forecast_sha256':row['sha'],
        'publication_verified':True,'publication_verified_epoch':now,'forecasts':compact}


def bar_count(capture,reason):
    value=capture.get('current_common_bars')
    if type(value) is int and 0<=value<=1024:return value
    match=re.search(r'current_common_warmup:(\d+)<61',reason)
    return min(1024,int(match.group(1))) if match else None


def build_summary(registry,states,now):
    now=number(now);rows=[]
    for pair in sorted(registry['pairs']):
        state=states[pair];ledger=state['ledger'];forecast=state.get('publication')
        if forecast and forecast['target_epoch']<=now:forecast=None
        bars=state.get('current_common_bars');reason=state.get('reason') or 'awaiting_pair_attempt'
        status='warming' if type(bars) is int and bars<61 else 'unavailable'
        if state.get('building'):status='building';reason='building_pair_models'
        if state.get('quote_reason') and not forecast:status='unavailable';reason=state['quote_reason']
        if forecast:status='forecast';reason='forecast'
        rows.append({'instrument':pair,'contract_sha256':ledger.contract_hash,
            'activated_epoch':ledger.activated_epoch,'observed_epoch':state.get('observed_epoch',now),'status':status,'reason':reason[:4096],
            'current_common_bars':bars,'required_current_common_bars':61,
            'last_attempt_epoch':state.get('last_attempt_epoch'),
            'last_attempt_reason':state.get('reason','')[:4096],
            'latest_published_forecast':forecast})
    summary={'schema_version':SUMMARY_SCHEMA,'registry_sha256':digest(registry),'generated_epoch':now,
        'research_only':True,'can_place_orders':False,'can_promote':False,'rows':rows}
    summary['payload_sha256']=digest(summary)
    if len(encoded(summary))>512*1024:raise ValueError('summary_size_limit')
    return summary


class PairRunner:
    """One bounded fitter, independent pair cadence, continuous quote observation."""
    def __init__(self,registry,study,candle_root,quote_path,*,clock=time.time):
        self.registry=registry;self.study=Path(study);self.candle_root=Path(candle_root);self.quote_path=Path(quote_path)
        self.clock=clock;self.states={};self.current={};self.queue=sorted(registry['pairs']);self.cursor=0
        self.future=None;self.active=None;self.score_future=None;self.score_pair=None
        self.fit_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='pair-fit')
        self.score_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='pair-score')
        self.last_poll=self.last_summary=self.last_heartbeat=0.0;self.summary=None;self.errors=0
        self.heartbeat_errors=0;self.last_error=''
        try:
            for pair,item in registry['pairs'].items():
                ledger=CausalForecastLedger(self.study/'pairs'/pair/'study.sqlite',item['contract'],clock=clock)
                self.states[pair]={'ledger':ledger,'reason':'awaiting_pair_attempt','building':False,
                    'quote_reason':'awaiting_current_quote','last_bucket':-1,'last_score':0.0,'scored_outcomes':0}
                state=self.states[pair]
                row=ledger.db.execute('SELECT bucket,epoch FROM attempts ORDER BY bucket DESC LIMIT 1').fetchone()
                if row:state.update(last_bucket=row['bucket'],last_attempt_epoch=row['epoch'])
                old=ledger.db.execute('SELECT payload FROM diagnostics ORDER BY bucket DESC LIMIT 1').fetchone()
                if old:
                    diag=json.loads(old[0]);reason='|'.join(diag.get('reasons',[])) or diag.get('reason','')
                    state.update(reason=reason,current_common_bars=bar_count(diag,reason))
                ledger.settle();state['publication']=verified_publication(ledger);state['observed_epoch']=number(self.clock())
        except BaseException:
            self.close();raise

    def error(self,pair,exc):
        reason=type(exc).__name__+':'+str(exc);self.errors+=1;self.last_error=pair+':'+reason
        self.states[pair]['reason']=reason
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
                self.current[pair]=state['ledger'].observe_quote(quote);state['quote_reason']=''
            except Exception as exc:state['quote_reason']=type(exc).__name__+':'+str(exc)

    def finish_fit(self):
        if self.future is None or not self.future.done():return
        pair,bucket=self.active;state=self.states[pair];ledger=state['ledger']
        try:
            capture,result=self.future.result();reason='|'.join(result.get('reasons',capture.get('reasons',[])))
            state['current_common_bars']=bar_count(capture,reason)
            if capture.get('status')==result.get('status')=='ready':
                ledger.publish(bucket,capture,result);ledger.consume_publications()
                state['publication']=verified_publication(ledger);state['reason']=''
            else:
                state['reason']=reason or 'pair_models_unavailable'
                ledger.diagnostic(bucket,{'status':'abstain','reasons':[state['reason']],
                    'current_common_bars':state['current_common_bars']})
        except Exception as exc:
            reason=self.error(pair,exc)
            try:ledger.diagnostic(bucket,{'status':'failed_closed','reason':reason})
            except Exception as diagnostic_exc:self.error(pair,diagnostic_exc)
        finally:
            state['building']=False;self.future=None;self.active=None

    def schedule_fit(self):
        if self.future is not None:return
        now=number(self.clock());bucket=int(now//900)
        for _ in range(len(self.queue)):
            pair=self.queue[self.cursor];self.cursor=(self.cursor+1)%len(self.queue)
            state=self.states[pair];quote=self.current.get(pair)
            if quote is None or state['last_bucket']>=bucket:continue
            if not 0<=now-quote['market_epoch']<=60 or not 0<=now-quote['available_epoch']<=60:continue
            try:
                if not state['ledger'].reserve_attempt(bucket,quote):state['last_bucket']=bucket;continue
                state.update(last_bucket=bucket,last_attempt_epoch=number(self.clock()),building=True)
                self.active=(pair,bucket)
                self.future=self.fit_pool.submit(fit_once,self.candle_root,pair,self.registry['pairs'][pair]['pip_size'])
                return
            except Exception as exc:
                state['building']=False;self.active=None
                reason=self.error(pair,exc)
                if state['last_bucket']==bucket:
                    try:state['ledger'].diagnostic(bucket,{'status':'failed_closed','reason':reason})
                    except Exception as diagnostic_exc:self.error(pair,diagnostic_exc)

    def score(self):
        if self.score_future is not None and self.score_future.done():
            try:
                report=self.score_future.result()
                self.states[self.score_pair]['scored_outcomes']=report['collection_counts']['outcomes']
            except Exception as exc:self.error(self.score_pair,exc)
            self.score_future=None;self.score_pair=None
        if self.score_future is not None:return
        now=number(self.clock())
        for pair,state in self.states.items():
            if now-state['last_score']<900:continue
            count=state['ledger'].db.execute('SELECT COUNT(*) FROM outcomes').fetchone()[0]
            if count<=state['scored_outcomes']:continue
            state['last_score']=now;self.score_pair=pair
            self.score_future=self.score_pool.submit(write_scorecard,state['ledger'],self.study/'pairs'/pair/'scorecard.json')
            return

    def publish_status(self,*,force=False):
        now=number(self.clock())
        if force or now-self.last_summary>=15:
            for pair,state in self.states.items():
                try:state['publication']=verified_publication(state['ledger'])
                except Exception as exc:
                    state['publication']=None;self.error(pair,exc)
                state['observed_epoch']=number(self.clock())
            now=number(self.clock())
            summary=build_summary(self.registry,self.states,now)
            atomic_json(self.study/'summary.json',summary);self.summary=summary;self.last_summary=now
        if self.summary is not None and (force or now-self.last_heartbeat>=5):
            counts={status:sum(row['status']==status for row in self.summary['rows'])
                for status in ('forecast','warming','building','unavailable')}
            heartbeat={'schema_version':HEARTBEAT_SCHEMA,'generated_epoch':number(self.clock()),
                'registry_sha256':digest(self.registry),'summary_sha256':digest(self.summary),'research_only':True,
                'can_place_orders':False,'can_promote':False,'account_eligible':False,'proof_eligible':False,
                'phase':'research_collection','supported_decision':'no_trade','pair_count':len(self.states),
                'counts':counts,'errors':self.errors,'last_error':self.last_error[:4096],
                'heartbeat_publication_errors':self.heartbeat_errors,'active_pair':self.active[0] if self.active else None}
            atomic_json(self.study/'heartbeat.json',heartbeat);self.last_heartbeat=now

    def tick(self):
        now=number(self.clock())
        if now-self.last_poll>=2:
            self.poll_quotes();self.last_poll=now
            for pair,state in self.states.items():
                try:state['ledger'].settle()
                except Exception as exc:self.error(pair,exc)
        self.finish_fit();self.schedule_fit();self.score()
        try:self.publish_status()
        except OSError as exc:
            self.errors+=1;self.heartbeat_errors+=1;self.last_error='status_publication:'+type(exc).__name__

    def close(self):
        self.fit_pool.shutdown(wait=True,cancel_futures=True);self.score_pool.shutdown(wait=True,cancel_futures=True)
        for state in self.states.values():state['ledger'].close()


def run(config=DEFAULT_CONFIG,*,duration_sec=0,once=False):
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
            ROOT/'data/oanda_training_manager/state/practice_007_market_quotes_v1.json')
        if once:
            # A diagnostic cycle must not spend a cadence bucket on an
            # asynchronous fit whose result the process will never publish.
            runner.poll_quotes()
            for state in runner.states.values():state['ledger'].settle()
            runner.publish_status(force=True)
            return
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
    parser.add_argument('--once',action='store_true',help='Observe quotes and publish status once; do not reserve model attempts')
    args=parser.parse_args();run(args.config,duration_sec=args.duration_sec,once=args.once)


if __name__=='__main__':main()
