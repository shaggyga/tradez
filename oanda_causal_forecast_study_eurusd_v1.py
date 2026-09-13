"""Order-incapable two-family EUR/USD successor study using local observed inputs.

The main loop records quotes and target outcomes while one background fitter
works. It never writes legacy forecast, signal, lifecycle or order databases.
The immutable experiment is fixed before its first forecast; no outcome-based
tuning, selection, historical forecast import, or graduation is supported.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import importlib.metadata
import json
import os
import platform
from pathlib import Path
import time

from oanda_causal_forecast_ledger_eurusd_v1 import CausalForecastLedger, digest, encoded, number
from oanda_fixed_forecast_evaluation_eurusd_v1 import evaluate, jsonable, loads_exact_prices
from oanda_exact_price_scoring import decimal_value

ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT/'config/causal_forecast_study_eurusd_v1_20260907.json'
STUDY = ROOT/'data/oanda_training_manager/causal_forecast_study_eurusd_v1'
CAN_PLACE_ORDERS = False
REQUIRED_SOURCE_BINDINGS = frozenset({
    'oanda_causal_forecast_study_eurusd_v1.py','oanda_causal_forecast_ledger_eurusd_v1.py',
    'oanda_causal_forecast_inputs_eurusd_v1.py','oanda_eurusd_local_models_v1.py',
    'oanda_causal_forecast_inputs.py','oanda_causal_forecast_inputs_gap_v2.py','oanda_fixed_forecast_evaluation_eurusd_v1.py',
    'oanda_exact_price_scoring.py','oanda_causal_prediction_baselines.py'})


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name+f'.{os.getpid()}.{time.time_ns()}.tmp')
    owned_temporary = False
    try:
        with temporary.open('xb') as f:
            owned_temporary = True
            f.write(encoded(value)); f.flush(); os.fsync(f.fileno())
        # Windows readers/scanners can briefly deny replacement. Reuse the
        # exact same bytes and clocks; never regenerate evidence during retry.
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                break
            except OSError as exc:
                if (not isinstance(exc, PermissionError) and
                        getattr(exc, 'winerror', None) not in (5, 32, 33)) or attempt == 7:
                    raise
                time.sleep(min(0.01 * 2**attempt, 0.5))
    finally:
        if owned_temporary:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass  # Cleanup cannot reverse publication or mask its error.


def load_contract(path):
    contract = json.loads(Path(path).read_bytes())
    if contract.get('collection_enabled') is not True:
        raise ValueError('study_collection_disabled')
    for flag in ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported'):
        if contract.get(flag) is not False:
            raise ValueError('research_only_contract_required')
    if set(contract.get('source_bindings',{})) != REQUIRED_SOURCE_BINDINGS:
        raise ValueError('complete_source_bindings_required')
    if set(contract.get('dependency_versions',{})) != {'python','numpy','scikit-learn'}:
        raise ValueError('complete_dependency_bindings_required')
    for relative, expected in contract['source_bindings'].items():
        source = (ROOT/relative).resolve()
        if not source.is_relative_to(ROOT) or hashlib.sha256(source.read_bytes()).hexdigest() != expected:
            raise ValueError('study_source_binding_mismatch:'+relative)
    for package, expected in contract['dependency_versions'].items():
        actual = platform.python_version() if package == 'python' else importlib.metadata.version(package)
        if actual != expected:
            raise ValueError('study_dependency_version_mismatch:'+package)
    return contract


def read_quote(path, *, clock=time.time):
    """Independent consumer clock is taken after reading the published bytes."""
    raw = Path(path).read_bytes()
    observed = number(clock())
    try:
        snapshot = loads_exact_prices(raw)
    except (ValueError, UnicodeError) as exc:
        raise ValueError('quote_snapshot_invalid_json') from exc
    if (not isinstance(snapshot,dict) or not isinstance(snapshot.get('quotes'),dict) or
            not isinstance(snapshot['quotes'].get('EUR_USD'),dict) or
            not isinstance(snapshot.get('coverage'),dict) or
            not isinstance(snapshot['coverage'].get('retained_last_known_instruments'),list)):
        raise ValueError('quote_snapshot_invalid_shape')
    if snapshot.get('producer') != 'practice_007_dedicated_quote_stream':
        raise ValueError('unexpected_quote_producer')
    quote = snapshot['quotes']['EUR_USD']
    if quote.get('tradeable') is not True or quote.get('source') != 'stream':
        raise ValueError('market_closed_or_quote_not_tradeable')
    if not isinstance(quote.get('time'),str):
        raise ValueError('quote_timestamp_required')
    parsed = datetime.fromisoformat(quote['time'].replace('Z','+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('quote_timezone_missing')
    if 'EUR_USD' in snapshot.get('coverage',{}).get('retained_last_known_instruments',[]):
        raise ValueError('retained_quote_not_current')
    return {'instrument':'EUR_USD', 'market_epoch':parsed.timestamp(), 'available_epoch':observed,
            'bid':str(decimal_value(quote['bid'])), 'ask':str(decimal_value(quote['ask'])), 'tradeable':True,
            'provider_time':quote['time'], 'raw_provider_quote':jsonable(quote),
            'source_snapshot_sha256':hashlib.sha256(raw).hexdigest(), 'producer':snapshot['producer']}


def fit_once(candle_root):
    # Import only the pure input/model adapter, never legacy main/row builders.
    from oanda_causal_forecast_inputs_eurusd_v1 import capture_inputs, compute_predictions
    capture = capture_inputs(candle_root)
    if capture.get('status') != 'ready':
        return capture, {'status':'abstain','reasons':capture.get('reasons',[])}
    return capture, compute_predictions(capture)


def write_scorecard(ledger, destination):
    dataset, protocol = ledger.export_evaluation()
    report = jsonable(evaluate(dataset, protocol))
    report['collection_counts'] = dataset['collection_counts']
    report['study_contract_sha256'] = ledger.contract_hash
    report['interpretation'] = 'Fresh successor study only; no historical forecast imports. Scores are engineering diagnostics, never execution authority.'
    atomic_json(destination, report)
    return report


def run(config=DEFAULT_CONFIG, *, activate=False, duration_sec=0, once=False):
    contract = load_contract(config)
    # Keep these small, frozen models from spawning a large native thread pool.
    for variable in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','LOKY_MAX_CPU_COUNT'):
        os.environ[variable] = '1'
    STUDY.mkdir(parents=True, exist_ok=True)
    # OS-held lock rejects duplicate workers and automatically releases on crash.
    lock = (STUDY/'worker.lock').open('a+b')
    import msvcrt
    lock.seek(0)
    if not lock.read(1): lock.write(b'0'); lock.flush()
    lock.seek(0)
    try:
        msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        lock.close()
        raise RuntimeError('study_worker_already_running')
    try:
        ledger = CausalForecastLedger(STUDY/'study.sqlite', contract, activate=activate)
    except BaseException:
        lock.seek(0); msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1); lock.close()
        raise
    executor = None
    future = None
    score_future = None
    active_bucket = None
    started = time.monotonic()
    last_score = 0.0
    last_reason = ''
    errors = 0
    heartbeat_publication_errors = 0
    last_heartbeat_publication_error = ''
    try:
        executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='causal-study')
        while True:
            phase = 'waiting_for_tradeable_quote'
            current = None
            try:
                current = ledger.observe_quote(read_quote(ROOT/'data/oanda_training_manager/state/practice_007_market_quotes_v1.json'))
                phase = 'collecting'
                last_reason = ''
            except (ValueError, KeyError, OSError, json.JSONDecodeError) as exc:
                last_reason = str(exc)
            ledger.settle()
            if future is not None:
                phase = 'building_models_while_capturing_quotes'
                if future.done():
                    try:
                        capture, result = future.result()
                        if capture.get('status') == result.get('status') == 'ready':
                            if result['model_source_sha256'] != contract['numeric_model_source_sha256']:
                                raise ValueError('numeric_model_source_binding_mismatch')
                            ledger.publish(active_bucket, capture, result)
                            last_reason = ''
                        else:
                            last_reason = '|'.join(result.get('reasons',capture.get('reasons',[])))
                            ledger.diagnostic(active_bucket, {'status':'abstain','reasons':result.get('reasons',capture.get('reasons',[]))})
                    except Exception as exc:
                        errors += 1
                        last_reason = type(exc).__name__+': '+str(exc)
                        ledger.diagnostic(active_bucket, {'status':'failed_closed','reason':last_reason})
                    finally:
                        future = None
                        active_bucket = None
            if current is not None and future is None:
                bucket = int(time.time()//contract['cadence_sec'])
                try:
                    reserved = ledger.reserve_attempt(bucket, current)
                except ValueError as exc:
                    if str(exc) != 'stale_reference':
                        raise
                    reserved = False
                    last_reason = 'stale_reference'
                if reserved:
                    active_bucket = bucket
                    future = executor.submit(fit_once, ROOT/'data/oanda_training_manager/candles')
            if score_future is not None and score_future.done():
                try:
                    score_future.result()
                except Exception as exc:
                    errors += 1
                    last_reason = 'scorecard_failed:'+type(exc).__name__
                score_future = None
            if score_future is None and time.monotonic()-last_score >= 900:
                score_future = executor.submit(write_scorecard, ledger, STUDY/'scorecard.json')
                last_score = time.monotonic()
            heartbeat = {
                'schema_version':'causal_forecast_study_heartbeat_v1', 'generated_epoch':time.time(),
                'contract_id':contract['contract_id'], 'contract_sha256':ledger.contract_hash,
                'phase':phase, 'last_reason':last_reason, 'errors':errors,
                'counts':ledger.counts(), 'research_only':True, 'can_place_orders':False,
                'can_promote':False, 'account_eligible':False, 'proof_eligible':False,
                'active_bucket':active_bucket, 'activated_epoch':ledger.activated_epoch,
                'supported_decision':'no_trade',
                'heartbeat_publication_errors':heartbeat_publication_errors,
                'last_heartbeat_publication_error':last_heartbeat_publication_error}
            try:
                atomic_json(STUDY/'heartbeat.json', heartbeat)
            except OSError as exc:
                # Retain the last good heartbeat and keep observing quotes.
                # Persistent failure remains visible to supervisor freshness.
                errors += 1
                heartbeat_publication_errors += 1
                last_heartbeat_publication_error = type(exc).__name__+': '+str(exc)
            if once or (duration_sec > 0 and time.monotonic()-started >= duration_sec):
                break
            time.sleep(2)
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
        ledger.close()
        lock.seek(0); msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1); lock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=DEFAULT_CONFIG)
    parser.add_argument('--activate-new-study',action='store_true',help='Register this frozen contract at the actual current clock before its first forecast')
    parser.add_argument('--duration-sec',type=float,default=0)
    parser.add_argument('--once',action='store_true')
    args = parser.parse_args()
    run(args.config,activate=args.activate_new_study,duration_sec=args.duration_sec,once=args.once)


if __name__ == '__main__':
    main()

