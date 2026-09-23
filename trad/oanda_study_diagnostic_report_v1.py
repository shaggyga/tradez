"""Supplemental, outcome-blind EUR/USD engineering report; read-only runtime.

The frozen scorer rejects repeated decision reference epochs. This separate
report excludes every member of such groups and invokes that unchanged scorer.
It never repairs registered evidence, alters cohorts, or authorizes orders.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import importlib
import json
import math
from pathlib import Path
import sqlite3
import threading
import time

ROOT = Path(__file__).resolve().parent
CONFIG = 'config/causal_forecast_study_eurusd_v1_20260907.json'
CONFIG_SHA256 = '8c6fc84e36b7b46a872f9652210f8c5cc022013f7b8c39f07b1078e791bb318b'
CONTRACT_SHA256 = '515c81be37a8828da1fd89edcbee33083ac1f6cae1b2e13bdb5338961058afe0'
REQUIRED_SOURCES = frozenset({
    'oanda_causal_forecast_study_eurusd_v1.py', 'oanda_causal_forecast_ledger_eurusd_v1.py',
    'oanda_causal_forecast_inputs_eurusd_v1.py', 'oanda_eurusd_local_models_v1.py',
    'oanda_causal_forecast_inputs.py', 'oanda_causal_forecast_inputs_gap_v2.py',
    'oanda_fixed_forecast_evaluation_eurusd_v1.py', 'oanda_exact_price_scoring.py',
    'oanda_causal_prediction_baselines.py'})
SCHEMA = 'eurusd_supplemental_diagnostic_report_v1_20260907'
CACHE_SECONDS = 60
MAX_REPORT_AGE_SECONDS = 120
MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_DB_BYTES = 64 * 1024 * 1024
MAX_WAL_BYTES = 128 * 1024 * 1024
MAX_QUOTES = 8192
MAX_DECISIONS = 128
MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
MAX_REPORT_BYTES = 2 * 1024 * 1024
MAX_REVIEW_BYTES = 48 * 1024 * 1024
SQL_DEADLINE_SECONDS = 2
TABLES = ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')
_LOCK = threading.Lock()
_CACHE = {}

def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()

def _sha(raw):
    return hashlib.sha256(raw).hexdigest()

def _utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()

def _finite(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('invalid_finite_clock')
    return float(value)

def _read_bounded(path, limit=MAX_FILE_BYTES):
    if not path.is_file() or path.stat().st_size > limit:
        raise ValueError('missing_or_oversize_file')
    with path.open('rb') as handle:
        raw = handle.read(limit+1)
    if len(raw) > limit:
        raise ValueError('oversize_file')
    return raw

def _registered_contract():
    raw = _read_bounded(ROOT/CONFIG)
    if _sha(raw) != CONFIG_SHA256:
        raise ValueError('registered_config_hash_mismatch')
    contract = json.loads(raw)
    if _sha(_encoded(contract)) != CONTRACT_SHA256:
        raise ValueError('registered_contract_hash_mismatch')
    bindings = contract.get('source_bindings')
    if not isinstance(bindings, dict) or set(bindings) != REQUIRED_SOURCES:
        raise ValueError('registered_source_set_mismatch')
    for name, expected in bindings.items():
        path = (ROOT/name).resolve()
        if path.parent != ROOT or _sha(_read_bounded(path)) != expected:
            raise ValueError('registered_source_hash_mismatch')
    return contract

def _snapshot(data_root, contract):
    data_root = Path(data_root).resolve()
    study = (data_root/'causal_forecast_study_eurusd_v1').resolve()
    if not study.is_relative_to(data_root):
        raise ValueError('study_path_escape')
    path = (study/'study.sqlite').resolve()
    if path.parent != study or not path.is_file() or path.stat().st_size > MAX_DB_BYTES:
        raise ValueError('missing_oversize_or_escaped_database')
    for suffix, maximum in (('-wal', MAX_WAL_BYTES), ('-shm', MAX_FILE_BYTES)):
        sidecar = Path(str(path)+suffix)
        if sidecar.exists() and (sidecar.resolve().parent != study or sidecar.stat().st_size > maximum):
            raise ValueError('oversize_or_escaped_database_sidecar')
    deadline = time.monotonic()+SQL_DEADLINE_SECONDS
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=.2)) as db:
        db.row_factory = sqlite3.Row
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        saved = db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
        if saved is None or saved['sha'] != CONTRACT_SHA256 or saved['payload'].encode() != _encoded(contract):
            raise ValueError('ledger_contract_mismatch')
        active = db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
        if active is None or active['contract_sha'] != CONTRACT_SHA256:
            raise ValueError('ledger_activation_mismatch')
        activation = _finite(active['epoch'])
        counts = {table:db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in TABLES}
        if counts['quotes'] > MAX_QUOTES or counts['forecasts'] > MAX_DECISIONS:
            raise ValueError('diagnostic_row_limit')
        size = sum(db.execute(f'SELECT coalesce(sum(length(payload)),0) FROM {table}').fetchone()[0]
                   for table in ('quotes','forecasts'))
        if size > MAX_PAYLOAD_BYTES:
            raise ValueError('diagnostic_payload_limit')
        rows = db.execute('SELECT f.payload,f.sha,c.epoch FROM forecasts f LEFT JOIN consumption c ON c.id=f.id ORDER BY f.bucket').fetchall()
        quotes = [json.loads(r[0]) for r in db.execute('SELECT payload FROM quotes ORDER BY available,market,id')]
        highwater = _finite(db.execute('SELECT max(epoch) FROM clocks').fetchone()[0])
        cutoff = time.time()
        if not 0 < activation <= highwater <= cutoff:
            raise ValueError('activation_or_export_clock_order')
        db.rollback()
    decisions = []
    for row in rows:
        value = json.loads(row['payload'])
        if _sha(_encoded(value)) != row['sha']:
            raise ValueError('export_forecast_hash_mismatch')
        for forecast in value['forecasts']:
            forecast['committed_available_epoch'] = row['epoch']
        decisions.append(value)
    protocol = deepcopy(contract['evaluation_protocol'])
    protocol.update(historical_start_utc=_utc(activation), historical_end_utc=_utc(max(cutoff, activation+.000001)))
    dataset = {'schema_version':'fixed_forecast_evaluation_input_v1','observed_cutoff_epoch':cutoff,
               'decisions':decisions,'quotes':quotes,'collection_counts':counts}
    return dataset, protocol, study

def _exclude_duplicate_references(dataset):
    """No outcome, side, probability, return, quote or family value selects rows."""
    groups = defaultdict(list)
    identities = set()
    for decision in dataset.get('decisions', []):
        identity = decision.get('decision_id')
        if not isinstance(identity, str) or not identity or identity in identities:
            raise ValueError('missing_or_duplicate_decision_id')
        identities.add(identity)
        groups[_finite(decision.get('reference_epoch'))].append(identity)
    repeated = {ref:ids for ref,ids in groups.items() if len(ids)>1}
    excluded = {identity for ids in repeated.values() for identity in ids}
    filtered = deepcopy(dataset)
    filtered['decisions'] = [d for d in filtered['decisions'] if d['decision_id'] not in excluded]
    manifest = [{'reference_epoch':ref,'decision_ids':sorted(ids),'reason':'all_members_of_duplicate_reference_group'}
                for ref,ids in sorted(repeated.items())]
    return filtered, manifest

def _scorecard_state(study, now):
    path = study/'scorecard.json'
    value = {'exists':path.is_file(),'generated_utc':None,'age_sec':None,'stale':True}
    if not path.is_file():
        return value
    try:
        raw = _read_bounded(path)
        saved = json.loads(raw)
        stamp = datetime.fromisoformat(str(saved.get('generated_utc')).replace('Z','+00:00'))
        if stamp.tzinfo is None:
            raise ValueError('scorecard_naive_clock')
        age = now-stamp.timestamp()
        value.update(generated_utc=saved.get('generated_utc'), age_sec=round(age,3),
                     stale=not 0<=age<=300, sha256=_sha(raw), status=saved.get('status'),
                     collection_counts=saved.get('collection_counts'))
    except (OSError, ValueError, TypeError):
        value['error'] = 'original_scorecard_unreadable'
    return value

def _base():
    return {'schema_version':SCHEMA,'supplemental':True,'registered_scorecard':False,
            'research_only':True,'execution_eligible':False,'account_eligible':False,
            'proof_eligible':False,'can_place_orders':False,'can_promote':False,
            'scope_label':'Supplemental EUR/USD engineering diagnostic; duplicate reference groups excluded; not the registered scorecard.',
            'bounds':{'maximum_lifetime_quotes':MAX_QUOTES,'maximum_lifetime_decisions':MAX_DECISIONS,
                      'maximum_database_bytes':MAX_DB_BYTES,'maximum_wal_bytes':MAX_WAL_BYTES,
                      'maximum_selected_payload_bytes':MAX_PAYLOAD_BYTES,'maximum_api_report_bytes':MAX_REPORT_BYTES,
                      'maximum_review_evidence_bytes':MAX_REVIEW_BYTES,'sqlite_deadline_seconds':SQL_DEADLINE_SECONDS,
                      'cache_seconds':CACHE_SECONDS,'maximum_report_age_seconds':MAX_REPORT_AGE_SECONDS,
                      'policy':'Hard fail-closed lifetime ceilings; no rolling window, truncation or dropped history. Exceeding a bound makes the supplemental report unavailable.'}}

def _build(data_root, *, include_inputs=False):
    contract = _registered_contract()
    dataset, protocol, study = _snapshot(data_root, contract)
    filtered, manifest = _exclude_duplicate_references(dataset)
    # Only import the scorer after all nine registered source bytes validate.
    scorer = importlib.import_module('oanda_fixed_forecast_evaluation_eurusd_v1')
    if Path(scorer.__file__).resolve() != ROOT/'oanda_fixed_forecast_evaluation_eurusd_v1.py':
        raise ValueError('scorer_module_path_mismatch')
    original = {'status':'passed','error':None}
    try:
        raw_report = scorer.evaluate(dataset, protocol)
    except ValueError as exc:
        if str(exc) != 'duplicate_market_reference_epoch' or not manifest:
            raise ValueError('original_scorer_other_validation_failure') from exc
        original = {'status':'failed','error':'duplicate_market_reference_epoch'}
        raw_report = None
    report = scorer.jsonable(raw_report if not manifest else scorer.evaluate(filtered, protocol))
    _registered_contract()  # Fail closed if source bytes changed during the read/evaluation.
    now = time.time()
    result = _base()
    result.update(status='available', generated_utc=_utc(now), generated_epoch=now,
                  original_scorer=original, original_scorecard=_scorecard_state(study, now),
                  collection_counts=dataset['collection_counts'], original_input_sha256=_sha(_encoded(dataset)),
                  filtered_input_sha256=_sha(_encoded(filtered)), protocol_sha256=_sha(_encoded(protocol)),
                  original_decision_count=len(dataset['decisions']),retained_decision_count=len(filtered['decisions']),
                  excluded_decision_count=len(dataset['decisions'])-len(filtered['decisions']),
                  remaining_capacity={'quotes':MAX_QUOTES-dataset['collection_counts']['quotes'],
                                      'decisions':MAX_DECISIONS-dataset['collection_counts']['forecasts']},
                  capacity_state='near_limit' if (dataset['collection_counts']['quotes']>=.8*MAX_QUOTES
                                                  or dataset['collection_counts']['forecasts']>=.8*MAX_DECISIONS) else 'within_limits',
                  excluded_duplicate_reference_groups=manifest, report=report,
                  observed_cutoff_epoch=dataset['observed_cutoff_epoch'], study_contract_sha256=CONTRACT_SHA256,
                  registered_config_sha256=CONFIG_SHA256, registered_source_bindings=contract['source_bindings'],
                  selection_policy='Exclude every decision in every repeated finite reference_epoch group before examining outcomes. Keep original quotes, clocks, targets, cohorts and remaining order.',
                  limitations=['Post hoc supplemental engineering analysis; not prospective registered performance or promotion evidence.',
                    'Overlap and small sample size do not establish independent accuracy. Original registered scorecard and worker errors remain unchanged.',
                    'Original input hash includes all decisions and quotes; filtered hash separately binds the outcome-blind subset.',
                    'Cache can lag new observations by up to 60 seconds; original_scorecard describes its state at report construction.',
                    'Hard lifetime ceilings of 8192 quotes and 128 decisions eventually withhold this report. This revision does not paginate or silently score a recent subset.'])
    if len(_encoded(result)) > MAX_REPORT_BYTES:
        raise ValueError('diagnostic_report_size_limit')
    if include_inputs:
        result['original_input'] = dataset
        result['filtered_input'] = filtered
        result['evaluation_protocol'] = protocol
    return result

def _present(value, now):
    value = deepcopy(value)
    age = now-value['generated_epoch']
    value.update(report_age_sec=round(age,3), freshness='current' if 0<=age<=MAX_REPORT_AGE_SECONDS else 'stale')
    if not 0<=age<=MAX_REPORT_AGE_SECONDS:
        value.update(status='unavailable',error='diagnostic_clock_or_age_invalid',report=None)
    return value

def get_eurusd_diagnostic_report(data_root: Path) -> dict:
    """Bounded read-only report; cache both success and sanitized error for 60s."""
    key = str(Path(data_root).resolve())
    if not _LOCK.acquire(timeout=.05):
        result = _base()
        now = time.time()
        result.update(status='unavailable',generated_utc=_utc(now),generated_epoch=now,
                      error='diagnostic_build_in_progress',report=None)
        return _present(result,now)
    try:
        cached = _CACHE.get(key)
        now = time.time()
        if cached and 0<=time.monotonic()-cached[0]<CACHE_SECONDS and now>=cached[1]['generated_epoch']:
            return _present(cached[1],now)
        try:
            value = _build(Path(data_root))
        except Exception as exc:
            now = time.time()
            error = str(exc) if isinstance(exc,ValueError) and str(exc).replace('_','').isalnum() else type(exc).__name__
            value = _base()
            value.update(status='unavailable',generated_utc=_utc(now),generated_epoch=now,error=error,report=None)
        if len(_CACHE)>=4 and key not in _CACHE:
            _CACHE.pop(next(iter(_CACHE)))
        _CACHE[key]=(time.monotonic(),value)
        return _present(value,time.time())
    finally:
        _LOCK.release()

def write_review_evidence(data_root: Path, destination: Path):
    """Exclusive-create new review evidence outside project data/config trees."""
    destination = Path(destination).resolve()
    for forbidden in ((ROOT/'data').resolve(), (ROOT/'config').resolve(), Path(data_root).resolve()):
        if destination.is_relative_to(forbidden):
            raise ValueError('review_output_in_runtime_or_config')
    if destination.suffix.lower() != '.json' or not destination.parent.is_dir():
        raise ValueError('existing_review_directory_and_json_output_required')
    report = _build(Path(data_root),include_inputs=True)
    raw = _encoded(_present(report,time.time()))+b'\n'
    if len(raw)>MAX_REVIEW_BYTES:
        raise ValueError('diagnostic_review_size_limit')
    with destination.open('xb') as handle:
        handle.write(raw)
    return destination

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-root',type=Path,default=ROOT/'data/oanda_training_manager')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    print(write_review_evidence(args.data_root,args.output))
