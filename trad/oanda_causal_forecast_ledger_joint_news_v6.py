"""Separate native-origin joint ledger with immutable exact-M1 outcomes.

The original numerical delta/probability remain anchored to the completed price
input minute. Live quotes are retained as context, never as native targets.
All publication, consumption, source and score observations remain append-only.
"""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path, PureWindowsPath
import os, stat
import sqlite3
import time
import zlib
import threading
from functools import wraps
import native_m1_outcome_v1 as native_outcome
import native_m1_ledger_v1 as native_store
from oanda_exact_price_scoring import decimal_value, quote_midpoint

def canonical_price(value):
    text = format(value, 'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text

FAMILIES = ('ridge_price_news_v1',)
SCHEMA = 'causal_joint_price_news_ledger_v6_20260916_native_exact_m1'
BOUND_INPUT_SOURCE_SHA256 = '01c7c156e04b6832528a48e6ec39e8c0341b163afebf3193098bb34cd4b46f22'
NEWS_CLOCK_FIELDS = ('news_evidence_epoch','news_generated_epoch','news_first_observed_epoch',
                     'news_available_epoch','news_expires_epoch')
TRAINING_CLOCK_FIELDS = ('training_news_available_max_epoch',
                        'training_news_feature_cutoff_max_epoch',
                        'training_label_maturity_max_epoch')
COMPARISON_FIELDS = ('matched_price_only_expected_pips','neutral_news_ablation_expected_pips')


def _bound_inputs():
    if type(BOUND_INPUT_SOURCE_SHA256) is not str or len(BOUND_INPUT_SOURCE_SHA256)!=64:
        raise ValueError('reviewed_revision_input_source_binding_pending')
    import revision_joint_inputs_v4 as inputs
    _,proof=inputs.news_io.read_exact(inputs.__file__,1024**2)
    if proof['sha256']!=BOUND_INPUT_SOURCE_SHA256:
        raise ValueError('revision_input_source_changed')
    return inputs


def validate_input_capture(capture, *, instrument, pip_size, session, news_capture, history_share):
    return _bound_inputs().validate_capture(capture,instrument=instrument,pip_size=pip_size,session=session,news_capture=news_capture,history_share=history_share)


def _snapshot_issue_value(value):
    """Exact bounded canonical JSON ownership; no numeric coercion or cleanup."""
    if type(value) is not dict:raise ValueError('issue_capture_and_result_objects_required')
    # The reviewed input module bounds both capture and computed output at8MiB.
    raw=_bound_inputs().news_io.encode(value,8*1024**2)
    return json.loads(raw)


def compact_revision_evidence(capture,health_proof):
    descriptor=capture['news_capture_descriptor'];point=capture['current_news_point'];points=capture['training_news_points']
    if (type(descriptor) is not dict or set(descriptor)!={'schema_version','capture_sha256','capture_path'}
            or descriptor['schema_version']!=_bound_inputs().news_io.DESCRIPTOR
            or descriptor['capture_sha256']!=capture['news_capture_sha256']):
        raise ValueError('exact_shared_revision_capture_descriptor_required')
    for value in (capture['news_capture_sha256'],capture['news_context_sha256'],point['source_generation_sha256'],point['transport_timing_sha256'],health_proof['health_proof_sha256']):
        if type(value) is not str or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):raise ValueError('revision_evidence_sha256_required')
    if (type(points) is not list or len(points)>256 or point['decision_epoch']!=capture['feature_decision_epoch']
            or point['context_sha256']!=capture['news_context_sha256'] or point['coverage_usable'] is not True):
        raise ValueError('complete_current_revision_timing_required')
    if (health_proof['scope']!='current_health_only_retained_input_expiry_guards_still_required'
            or health_proof['research_only'] is not True
            or any(health_proof.get(k) is not False for k in ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','joint_model_consumption_proven','execution_eligible'))):
        raise ValueError('fixed_io_health_proof_scope_required')
    before,after=number(health_proof['observed_epoch']),number(health_proof['readback_observed_epoch'])
    if not 0<before<=after:raise ValueError('actual_health_proof_clock_order')
    evidence={'schema_version':'compact_joint_revision_forecast_evidence_v1_20260913',
        'news_capture_descriptor':descriptor,'news_context_sha256':capture['news_context_sha256'],
        'feature_decision_epoch':capture['feature_decision_epoch'],'current_news_point':point,
        'training_news_points_sha256':digest(points),'training_news_point_count':len(points),
        'source_bindings':capture['source_bindings'],'history_share_descriptor':capture['history_share_descriptor'],
        'before_issue_health_proof':health_proof,
        'original_news_expiry_epoch':capture['news_expires_epoch'],
        'fresh_health_replaced_news_features':False,'fresh_health_extended_news_expiry':False}
    frozen=encoded(evidence)
    if len(frozen)>128*1024:raise ValueError('compact_revision_forecast_evidence_bound')
    return json.loads(frozen)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('finite_numeric_clock_or_price_required')
    return float(value)


def utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def native_source_bindings():
    import oanda_fixed_forecast_evaluation_joint_news_v3 as evaluator
    values=native_outcome.source_bindings()
    for path in (Path(__file__),Path(native_store.__file__),Path(evaluator.__file__)):
        path=native_outcome.plain_path(path);before=path.stat()
        if before.st_size>2*1024**2:raise ValueError('native_source_file_bound')
        raw=path.read_bytes()
        if native_outcome.signature(before)!=native_outcome.signature(path.stat()):raise ValueError('native_source_changed')
        values[path.name]=hashlib.sha256(raw).hexdigest()
    return values


def validate_contract(contract):
    _bound_inputs()  # No database writer or activation before exact reviewed source binding.
    from oanda_fixed_forecast_evaluation_joint_news_v3 import validate_protocol, validate_pair_metadata, validate_cohorts
    if contract.get('schema_version') != SCHEMA or not contract.get('contract_id'):
        raise ValueError('research_contract_required')
    if contract.get('research_only') is not True or any(contract.get(k) is not False for k in
            ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported')):
        raise ValueError('inert_research_flags_required')
    instrument, pip = validate_pair_metadata(contract)
    if type(contract.get('horizon_sec')) is not int or contract['horizon_sec'] != 3600 or contract.get('input_timeframe') != 'M1':
        raise ValueError('fixed_pair_h1_required')
    for key, expected in {'cadence_sec':900,'maximum_build_sec':120,'maximum_input_age_sec':900,'maximum_news_age_sec':300,'quote_max_age_sec':60,
                         'maximum_entry_delay_sec':60,'maximum_target_quote_delay_sec':60}.items():
        if number(contract.get(key)) != expected:
            raise ValueError('fixed_contract_tolerance_required:'+key)
    validate_cohorts(contract.get('cohorts'), instrument, contract.get('family'))
    if contract.get('family') not in contract['cohorts']:
        raise ValueError('selected_family_required')
    if not isinstance(contract.get('dependency_versions'),dict) or not contract['dependency_versions']:
        raise ValueError('dependency_binding_required')
    model_hash = contract.get('numeric_model_source_sha256','')
    if len(model_hash) != 64 or any(c not in '0123456789abcdef' for c in model_hash):
        raise ValueError('numeric_source_sha256_required')
    if encoded(contract.get('native_source_bindings'))!=encoded(native_source_bindings()):
        raise ValueError('native_source_contract_binding_changed')
    native_store.validate_policy(contract.get('native_outcome_policy'))
    native_outcome.plain_path(contract.get('native_clock_path'),missing=True)
    candle=native_outcome.plain_path(contract.get('native_candle_path'),missing=True)
    if candle.name!=instrument+'_M1.csv':raise ValueError('native_registered_candle_path_required')
    protocol = contract['evaluation_protocol']
    validate_protocol(protocol)
    if decimal_value(protocol['pip_size']) != pip:
        raise ValueError('evaluation_collection_contract_mismatch:pip_size')
    for key in ('family','instrument','input_timeframe','horizon_sec','cohorts','model_version','feature_version','quote_max_age_sec',
                'maximum_entry_delay_sec','maximum_target_quote_delay_sec','maximum_input_age_sec','maximum_news_age_sec'):
        if protocol.get(key) != contract.get(key):
            raise ValueError('evaluation_collection_contract_mismatch:'+key)
    if protocol.get('native_outcome_policy') != contract['native_outcome_policy']:
        raise ValueError('native_outcome_policy_protocol_mismatch')


def plain_ledger_path(value):
    """Guard every C ancestor before any target lookup; never follow reparses."""
    lexical=PureWindowsPath(str(value))
    if lexical.drive.lower()!='c:' or not lexical.is_absolute() or '..' in lexical.parts:
        raise ValueError('absolute_c_ledger_path_required')
    if any(':' in part for part in lexical.parts[1:]):raise ValueError('ledger_alternate_stream_refused')
    path=Path(value)
    for part in reversed((path,*path.parents)):
        try:info=os.lstat(part)
        except FileNotFoundError:break
        if stat.S_ISLNK(info.st_mode) or getattr(info,'st_file_attributes',0)&0x400:
            raise ValueError('ledger_reparse_path_refused')
    return path


def serialized(method):
    @wraps(method)
    def call(self,*args,**kwargs):
        with self._lock:return method(self,*args,**kwargs)
    return call


class CausalForecastLedger:
    def __init__(self, path: Path, contract: dict, *, clock=time.time, activate=False):
        self._lock=threading.RLock()
        self.path = plain_ledger_path(path)
        validate_contract(contract)
        if not self.path.is_file() and not activate:
            raise ValueError('explicit_new_study_activation_required')
        if self.path.is_file() and self.path.stat().st_size:
            # Refuse a v1/unrelated DB before opening a writer or issuing DDL.
            with closing(sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True)) as preflight:
                tables = {row[0] for row in preflight.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if not {'contract','attempts','forecasts','activation',*native_store.TABLES} <= tables:
                    raise ValueError('refuse_existing_unrelated_database')
                native_store.verify_existing(preflight)
                saved = preflight.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
                if saved is None or saved[0] != digest(contract) or saved[1] != encoded(contract).decode():
                    raise ValueError('immutable_contract_mismatch')
                columns = {row[1] for row in preflight.execute('PRAGMA table_info(forecasts)')}
                attempt_columns = {row[1] for row in preflight.execute('PRAGMA table_info(attempts)')}
                if not {'attempt_id','reference'} <= columns or not {'id','sequence'} <= attempt_columns:
                    raise ValueError('refuse_existing_non_v2_database')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self._contract_encoded = encoded(contract)
        self.contract_hash = digest(contract)
        self.db = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        tables = {row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables and not {'contract','activation','forecasts','publication','consumption'} <= tables:
            self.close()
            raise ValueError('refuse_existing_unrelated_database')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS contract(id INTEGER PRIMARY KEY CHECK(id=1), sha TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS activation(id INTEGER PRIMARY KEY CHECK(id=1), epoch REAL NOT NULL, contract_sha TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS clocks(seq INTEGER PRIMARY KEY, epoch REAL NOT NULL, event TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS quotes(id TEXT PRIMARY KEY, market REAL NOT NULL, available REAL NOT NULL, payload TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS quotes_available ON quotes(available,market);
            CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY, bucket INTEGER NOT NULL, sequence INTEGER NOT NULL, epoch REAL NOT NULL, reference_id TEXT NOT NULL, UNIQUE(bucket,sequence));
            CREATE TABLE IF NOT EXISTS diagnostics(attempt_id TEXT PRIMARY KEY, bucket INTEGER NOT NULL, epoch REAL NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS inputs(id TEXT PRIMARY KEY, payload BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS forecasts(id TEXT PRIMARY KEY, bucket INTEGER UNIQUE NOT NULL, attempt_id TEXT UNIQUE NOT NULL, reference REAL UNIQUE NOT NULL, target REAL NOT NULL, sha TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS publication(id TEXT PRIMARY KEY, epoch REAL NOT NULL, forecast_sha TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS consumption(id TEXT PRIMARY KEY, epoch REAL NOT NULL, forecast_sha TEXT NOT NULL, publication_sha TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS entries(id TEXT PRIMARY KEY, quote_id TEXT NOT NULL, epoch REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS outcomes(id TEXT PRIMARY KEY, quote_id TEXT NOT NULL, epoch REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS exclusions(id TEXT PRIMARY KEY, reason TEXT NOT NULL, epoch REAL NOT NULL);
        ''')
        self.db.executescript(native_store.SQL)
        for table in ('contract', 'activation', 'clocks', 'quotes', 'attempts', 'diagnostics',
                      'inputs', 'forecasts', 'publication', 'consumption', 'entries', 'outcomes', 'exclusions'):
            for operation in ('UPDATE', 'DELETE'):
                self.db.execute(f'''CREATE TRIGGER IF NOT EXISTS immutable_{table}_{operation}
                    BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT,'immutable_evidence'); END''')
        existing = self.db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
        if existing is None:
            if not activate:
                self.close()
                raise ValueError('explicit_new_study_activation_required')
            with self.db:
                self.db.execute('INSERT INTO contract VALUES(1,?,?)', (self.contract_hash, encoded(contract).decode()))
        elif existing['sha'] != self.contract_hash or existing['payload'] != encoded(contract).decode():
            self.close()
            raise ValueError('immutable_contract_mismatch')
        self.last_clock = float(self.db.execute('SELECT COALESCE(MAX(epoch),0) FROM clocks').fetchone()[0])
        active = self.db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
        if active is None:
            if not activate:
                self.close()
                raise ValueError('activation_receipt_missing')
            with self.db:
                at = self._tick('activation_after_contract_commit')
                self.db.execute('INSERT INTO activation VALUES(1,?,?)', (at, self.contract_hash))
            self.activated_epoch = at
        else:
            if active['contract_sha'] != self.contract_hash:
                self.close()
                raise ValueError('activation_contract_mismatch')
            self.activated_epoch = float(active['epoch'])

    @property
    def contract(self):
        # Neither caller mutations nor mutations of a returned view alter the
        # parameters bound by the immutable registration receipt.
        return json.loads(self._contract_encoded)

    @serialized
    def close(self):
        self.db.close()

    def _now(self):
        value = number(self.clock())
        if value < self.last_clock:
            raise ValueError('wall_clock_regression')
        return value

    def _tick(self, event):
        value = self._now()
        self.db.execute('INSERT INTO clocks(epoch,event) VALUES(?,?)', (value, event))
        self.last_clock = value
        return value

    def _read(self, query, params=()):
        # A new connection cannot see the producer's uncommitted transaction.
        with closing(sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True)) as reader:
            reader.row_factory = sqlite3.Row
            reader.execute('PRAGMA query_only=ON')
            return list(reader.execute(query, params))

    @serialized
    def observe_quote(self, quote: dict):
        """Persist the successor consumer's first actual observation of a tick."""
        market, read_observed = number(quote['market_epoch']), number(quote['available_epoch'])
        bid, ask = decimal_value(quote['bid']), decimal_value(quote['ask'])
        if quote.get('instrument') != self.contract['instrument'] or quote.get('tradeable') is not True:
            raise ValueError('registered_pair_tradeable_quote_required')
        pip = decimal_value(quote.get('pip_size'), 'pip_size')
        if pip != decimal_value(self.contract['pip_size']):
            raise ValueError('quote_pip_contract_mismatch')
        observed = self._now()
        if not 0 < bid <= ask or not market <= read_observed <= observed:
            raise ValueError('quote_price_or_clock_order')
        if observed-market > self.contract['quote_max_age_sec'] or observed <= self.activated_epoch:
            raise ValueError('stale_or_preactivation_quote')
        identity = {'instrument':self.contract['instrument'],'pip_size':canonical_price(pip),'market_epoch':market,'bid':canonical_price(bid),'ask':canonical_price(ask),'tradeable':True}
        quote_id = digest(identity)
        old = self.db.execute('SELECT payload FROM quotes WHERE id=?', (quote_id,)).fetchone()
        if old is not None:
            return json.loads(old['payload'])
        with self.db:
            observed = self._tick('quote_observation')
            if observed-market > self.contract['quote_max_age_sec']:
                raise ValueError('quote_expired_before_observation_commit')
            payload = dict(quote, **identity)
            payload.update(quote_id=quote_id, available_epoch=observed, source_read_epoch=read_observed)
            self.db.execute('INSERT INTO quotes VALUES(?,?,?,?)', (quote_id, market, observed, encoded(payload).decode()))
        return payload

    @serialized
    def begin_attempt(self, bucket, reference):
        """Reserve a retry identity; a failed attempt never consumes the bucket."""
        if type(bucket) is not int or bucket < 0:
            raise ValueError('cadence_bucket_required')
        if self.db.execute('SELECT 1 FROM forecasts WHERE bucket=?', (bucket,)).fetchone():
            return None
        retained = self.db.execute('SELECT payload FROM quotes WHERE id=?', (reference['quote_id'],)).fetchone()
        if retained is None or json.loads(retained['payload']) != reference:
            raise ValueError('reference_not_observed')
        with self.db:
            at = self._tick('reserve_readiness_attempt')
            if bucket != int(at // self.contract['cadence_sec']):
                raise ValueError('attempt_outside_actual_cadence_bucket')
            if at-reference['market_epoch'] > self.contract['quote_max_age_sec'] or at-reference['available_epoch'] > self.contract['quote_max_age_sec']:
                raise ValueError('stale_reference')
            sequence = self.db.execute('SELECT coalesce(max(sequence),0)+1 FROM attempts WHERE bucket=?', (bucket,)).fetchone()[0]
            identity = digest({'contract':self.contract_hash,'bucket':bucket,'sequence':sequence})
            self.db.execute('INSERT INTO attempts VALUES(?,?,?,?,?)', (identity,bucket,sequence,at,reference['quote_id']))
        return identity

    def _store_capture(self, capture):
        raw = encoded(capture)
        if len(raw) > 24*1024*1024:
            raise ValueError('capture_evidence_size_limit')
        identity = digest(capture)
        self.db.execute('INSERT OR IGNORE INTO inputs VALUES(?,?)', (identity,zlib.compress(raw,6)))
        return identity

    @serialized
    def record_abstention(self, attempt_id, payload, *, capture=None, result=None):
        """Append reasons and optional observed raw evidence, without authorizing it.

        Captures from unavailable inputs are retained as evidence, not validated
        forecast inputs. issue() separately verifies all causal input bindings.
        """
        attempt = self.db.execute('SELECT * FROM attempts WHERE id=?', (attempt_id,)).fetchone()
        if attempt is None:
            raise ValueError('attempt_not_reserved')
        if self.db.execute('SELECT 1 FROM forecasts WHERE attempt_id=?', (attempt_id,)).fetchone():
            raise ValueError('successful_attempt_cannot_abstain')
        if self.db.execute('SELECT 1 FROM diagnostics WHERE attempt_id=?', (attempt_id,)).fetchone():
            raise ValueError('attempt_already_closed')
        value = dict(payload)
        if result is not None:
            value['retained_result'] = result
        if len(encoded(value)) > 1024*1024:
            raise ValueError('diagnostic_evidence_size_limit')
        with self.db:
            at = self._tick('attempt_abstained')
            value.update(attempt_id=attempt_id, family=self.contract['family'],
                         capture_is_forecast_authority=False)
            if capture is not None:
                value['input_capture_sha256'] = self._store_capture(capture)
            self.db.execute('INSERT INTO diagnostics VALUES(?,?,?,?)',
                            (attempt_id,attempt['bucket'],at,encoded(value).decode()))

    reserve_attempt = begin_attempt
    diagnostic = record_abstention

    @serialized
    def issue(self, attempt_id, capture, result, *, session, news_capture, history_share, after_commit=None):
        """Order observed news failure and forecast commit under the fixed lock.

        No fitting occurs here. Lock wait and health-read time remain inside the
        original actual-issue freshness/deadline checks. This orders observed
        failures; it cannot know a collector failure not yet observed by I/O.
        """
        io=_bound_inputs().news_io
        state=io._session(session)
        with state['lock']:
            # Snapshot under the same lock, before validation or any I/O wait.
            # The actual issue clock still charges this time to original expiry.
            capture=_snapshot_issue_value(capture)
            result=_snapshot_issue_value(result)
            io._eligible(session,news_capture)
            identity=self._issue_locked(attempt_id,capture,result,session=session,news_capture=news_capture,history_share=history_share)
        if after_commit is not None:after_commit()
        self.recover_publications()
        return identity

    def _issue_locked(self, attempt_id, capture, result, *, session, news_capture, history_share):
        """The hook is for crash tests; issuance is sampled here after fitting."""
        attempt = self.db.execute('SELECT * FROM attempts WHERE id=?', (attempt_id,)).fetchone()
        if attempt is None:
            raise ValueError('attempt_not_reserved')
        bucket = attempt['bucket']
        if self.db.execute('SELECT 1 FROM diagnostics WHERE attempt_id=?', (attempt_id,)).fetchone():
            raise ValueError('attempt_already_closed')
        if self.db.execute('SELECT 1 FROM forecasts WHERE bucket=?', (bucket,)).fetchone():
            raise ValueError('family_bucket_already_issued')
        if capture.get('status') != 'ready' or result.get('status') not in ('ready','partial'):
            raise ValueError('inputs_or_models_not_ready')
        for item in (capture,result):
            if item.get('research_only') is not True or any(item.get(key) is not False for key in
                    ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible')):
                raise ValueError('input_or_result_inert_flags_required')
        predictions = result['predictions']
        family = self.contract['family']
        if not isinstance(predictions,dict) or family not in predictions or not set(predictions) <= set(FAMILIES):
            raise ValueError('selected_local_family_required')
        readiness = capture.get('family_readiness',{}).get(family,{})
        if readiness.get('ready') is not True or readiness.get('status') != 'ready':
            raise ValueError('selected_family_input_not_ready')
        observed = number(capture['first_observed_epoch'])
        maturity = number(capture['max_bar_close_epoch'])
        validate_input_capture(capture, instrument=self.contract['instrument'], pip_size=self.contract['pip_size'], session=session, news_capture=news_capture, history_share=history_share)
        instrument, pip = self.contract['instrument'], decimal_value(self.contract['pip_size'])
        if capture.get('pairs') != [instrument]:
            raise ValueError('input_pair_contract_mismatch')
        for item in (capture, result):
            if item.get('output_instrument') != instrument or decimal_value(item.get('pip_size'), 'pip_size') != pip:
                raise ValueError('input_or_result_pair_pip_contract_mismatch')
        if (result.get('source_capture_sha256') != capture.get('source_capture_sha256') or
                result.get('model_source_sha256') != self.contract['numeric_model_source_sha256'] or
                capture.get('model_source_sha256') != self.contract['numeric_model_source_sha256'] or
                result.get('dependency_versions') != self.contract['dependency_versions'] or
                capture.get('dependency_versions') != self.contract['dependency_versions']):
            raise ValueError('result_input_or_model_binding_mismatch')
        started = number(result['computation_started_epoch'])
        completed = number(result['computed_epoch'])
        try:
            news = {key:number(capture[key]) for key in NEWS_CLOCK_FIELDS}
            if any(number(result[key]) != value for key,value in news.items()):
                raise ValueError('result_news_clock_binding_mismatch')
            news_hash = capture['news_capture_sha256']
            if (not isinstance(news_hash,str) or len(news_hash)!=64
                    or any(c not in '0123456789abcdef' for c in news_hash)
                    or result.get('news_capture_sha256') != news_hash):
                raise ValueError('result_news_capture_binding_mismatch')
        except (KeyError,TypeError) as exc:
            raise ValueError('news_clock_or_capture_binding_missing') from exc
        try:
            diagnostics = predictions[family]['diagnostics']
            training = {key:number(diagnostics[key]) for key in TRAINING_CLOCK_FIELDS}
            for key in COMPARISON_FIELDS:
                number(diagnostics[key])
        except (KeyError,TypeError) as exc:
            raise ValueError('training_news_clock_binding_missing') from exc
        if not (0 < training['training_news_available_max_epoch']
                <= training['training_news_feature_cutoff_max_epoch']
                <= training['training_label_maturity_max_epoch'] <= maturity):
            raise ValueError('training_news_and_label_clock_order')
        reference = json.loads(self.db.execute('SELECT payload FROM quotes WHERE id=?', (attempt['reference_id'],)).fetchone()[0])
        input_owner=_bound_inputs()
        native_rows,_=input_owner.price_inputs._verified_rows(capture['price_capture'])
        native_start=capture['reference_start_epoch']
        native_mid=native_rows[native_start]
        target = native_start + 3660
        if self.db.execute('SELECT 1 FROM forecasts WHERE reference=?',(native_start+60,)).fetchone():
            raise ValueError('market_reference_already_issued')
        # This call is fixed, current and inside the original session lock.
        health_proof=_bound_inputs().news_io.reobserve_before_issue(session,news_capture,clock=self.clock)
        revision_evidence=compact_revision_evidence(capture,health_proof)
        with self.db:
            issued = self._tick('selected_family_issued_after_computation')
            if number(health_proof['readback_observed_epoch'])>issued:
                raise ValueError('health_proof_observation_after_actual_issue')
            if not self.activated_epoch < observed <= started <= completed < issued or not maturity <= observed:
                raise ValueError('input_or_learning_availability_order')
            if issued-maturity > self.contract['maximum_input_age_sec']:
                raise ValueError('cached_input_expired_before_issue')
            if not (0 < news['news_evidence_epoch'] <= news['news_generated_epoch']
                    <= news['news_first_observed_epoch'] <= news['news_available_epoch'] <= observed):
                raise ValueError('news_source_consumer_availability_order')
            if not 0 <= issued-news['news_evidence_epoch'] <= self.contract['maximum_news_age_sec']:
                raise ValueError('news_stale_or_future_at_actual_issue')
            if issued > news['news_expires_epoch']:
                raise ValueError('news_original_expiry_before_actual_issue')
            if started < attempt['epoch'] or issued >= target:
                raise ValueError('computation_before_attempt_or_target_expired')
            if issued-reference['available_epoch'] > self.contract['maximum_build_sec']:
                raise ValueError('construction_deadline_exceeded')
            capture_hash = digest(capture)
            decision_id = digest({'contract': self.contract_hash, 'bucket': bucket})
            forecasts = []
            for family in (self.contract['family'],):
                prediction = predictions[family]
                p = number(prediction['probability_up'])
                expected = number(prediction['expected_signed_pips'])
                side = prediction['side']
                if not 0 <= p <= 1 or type(side) is not int or side not in (-1, 0, 1):
                    raise ValueError('invalid_model_output')
                mid = native_mid
                anchor=native_outcome.native.make_native_anchor(instrument=instrument,pip_size=float(pip),
                    origin_bar_start_epoch=native_start,origin_mid=native_mid,expected_signed_pips=expected,
                    probability_up=p,source_anchor_complete=True,
                    source_observed_epoch=number(capture['price_capture']['first_observed_epoch']),
                    feature_decision_epoch=number(capture['feature_decision_epoch']),
                    computation_started_epoch=started,computation_completed_epoch=completed,issued_epoch=issued,emitted_side=side)
                forecasts.append({
                    'forecast_id': decision_id+':'+family, 'family': family,
                    'cohort_id': self.contract['cohorts'][family],
                    'model_version': self.contract['model_version'],
                    'feature_version': self.contract['feature_version'],
                    'instrument': instrument, 'pip_size': canonical_price(pip), 'input_timeframe': 'M1', 'horizon_sec': 3600,
                    'issued_epoch': issued, 'reference_epoch': native_start+60,
                    'reference_mid': str(mid), 'target_epoch': target,
                    'native_anchor':anchor.as_dict(),'native_anchor_sha256':anchor.sha256,
                    'probability_event':'strict_native_target_close_above_original_origin_close',
                    'feature_cutoff_epoch': maturity, 'features_available_epoch': completed,
                    'input_source_observed_epoch': observed,
                    'computation_started_epoch': started, 'computation_completed_epoch': completed,
                    **news, 'news_capture_sha256':news_hash,
                    'news_revision_evidence':revision_evidence,
                    **training,
                    'training_labels_available_max_epoch': completed,
                    'learning_data_role': 'completed_exact_h1_labels_with_point_in_time_price_and_news_features',
                    'probability_up': p, 'predicted_return_bps': number(10000*expected*float(pip)/float(mid)),
                    'side': side, 'diagnostics': prediction.get('diagnostics', {}),
                    'research_only': True, 'account_eligible': False, 'proof_eligible': False,
                    'can_place_orders': False, 'can_promote': False, 'can_authorize': False})
            payload = {'decision_id': decision_id, 'attempt_id': attempt_id, 'attempt_epoch': attempt['epoch'], 'family': family, 'instrument': instrument, 'pip_size': canonical_price(pip), 'reference_epoch': native_start+60,
                'target_epoch': target, 'reference_quote_id': reference['quote_id'],
                'reference_quote_role':'current_context_not_native_model_anchor',
                'native_anchor_sha256':anchor.sha256,
                'forecasts': forecasts, 'input_capture_sha256': capture_hash,
                'news_capture_sha256': news_hash, 'news_revision_evidence':revision_evidence,
                'model_source_sha256': result['model_source_sha256'],
                'dependency_versions': result['dependency_versions'], 'research_only': True,
                'can_place_orders': False, 'can_promote': False, 'can_authorize': False,
                'account_eligible': False, 'proof_eligible': False}
            self._store_capture(capture)
            self.db.execute('INSERT INTO forecasts VALUES(?,?,?,?,?,?,?)',
                (decision_id,bucket,attempt_id,native_start+60,target,digest(payload),encoded(payload).decode()))
            # Charge serialization/compression and transaction work to the same
            # original deadlines immediately before the actual commit call.
            committing=self._tick('forecast_transaction_precommit_observed')
            if committing-maturity>self.contract['maximum_input_age_sec']:
                raise ValueError('cached_input_expired_before_commit')
            if (not 0<=committing-news['news_evidence_epoch']<=self.contract['maximum_news_age_sec']
                    or committing>news['news_expires_epoch']):
                raise ValueError('original_news_expired_before_commit')
            if committing>=target or committing-reference['available_epoch']>self.contract['maximum_build_sec']:
                raise ValueError('construction_or_target_deadline_before_commit')
        # The public issue wrapper releases the session lock only after this
        # transaction commits, then performs the unchanged publication receipt.
        return decision_id

    publish = issue

    @serialized
    def recover_publications(self):
        rows = self._read('SELECT f.id,f.sha,f.target,f.payload FROM forecasts f LEFT JOIN publication p ON p.id=f.id WHERE p.id IS NULL')
        for row in rows:
            if digest(json.loads(row['payload'])) != row['sha']:
                raise ValueError('forecast_commit_hash_mismatch')
            with self.db:
                at = self._tick('forecast_commit_independently_observed')
                self.db.execute('INSERT INTO publication VALUES(?,?,?)', (row['id'], at, row['sha']))

    @serialized
    def consume_publications(self):
        rows = self._read('''SELECT f.id,f.sha,f.payload,p.epoch,p.forecast_sha FROM forecasts f
            JOIN publication p ON p.id=f.id LEFT JOIN consumption c ON c.id=f.id WHERE c.id IS NULL''')
        for row in rows:
            if digest(json.loads(row['payload'])) != row['sha'] or row['forecast_sha'] != row['sha']:
                raise ValueError('publication_hash_mismatch')
            with self.db:
                observed = self._tick('consumer_observed_committed_forecast_and_receipt')
                if observed < row['epoch']:
                    raise ValueError('consumer_clock_before_publication')
                self.db.execute('INSERT INTO consumption VALUES(?,?,?,?)',
                    (row['id'], observed, row['sha'], digest({'epoch':row['epoch'], 'forecast_sha':row['sha']})))

    consume = consume_publications

    def get_native_clock_proof(self, clock_path):
        io=_bound_inputs().news_io
        path=native_outcome.plain_path(clock_path)
        if str(path).lower()!=str(native_outcome.plain_path(self.contract['native_clock_path'])).lower():
            raise ValueError('native_registered_clock_path_required')
        started=self._now()
        state,source=io.read_json(path,native_outcome.MAX_STATE)
        observed=self._now()
        if observed<started:raise ValueError('native_clock_read_regression')
        validation=io.repair.validate_clock_state(state,observed)
        proof={'schema_version':'native_exact_m1_clock_read_v1_20260913','observed_epoch':observed,
            'read_started_epoch':started,'source':source,'validation':validation}
        native_outcome.encode(proof,native_outcome.MAX_STATE)
        return proof

    def validate_native_clock_proof(self,proof,at):
        io=_bound_inputs().news_io
        if set(proof)!={'schema_version','observed_epoch','read_started_epoch','source','validation'} or proof['schema_version']!='native_exact_m1_clock_read_v1_20260913':
            raise ValueError('native_clock_read_proof_shape')
        observed=number(proof['observed_epoch']);started=number(proof['read_started_epoch'])
        if not 0<started<=observed<=number(at):raise ValueError('native_clock_proof_order')
        source=proof['source'];raw=source['raw_utf8'].encode('utf-8')
        if len(raw)>native_outcome.MAX_STATE or hashlib.sha256(raw).hexdigest()!=source['sha256']:
            raise ValueError('native_clock_retained_source_hash')
        if str(PureWindowsPath(source['path'])).lower()!=str(PureWindowsPath(self.contract['native_clock_path'])).lower():
            raise ValueError('native_clock_retained_path')
        state=io.strict_health_json(raw)
        if encoded(state)!=encoded(source['value']) or encoded(io.repair.validate_clock_state(state,observed))!=encoded(proof['validation']):
            raise ValueError('native_clock_retained_validation')
        io.repair.validate_clock_state(state,at)
        return proof

    def _native_publications(self):
        from oanda_fixed_forecast_evaluation_joint_news_v3 import forecast_errors
        rows=self._read('''SELECT f.payload,f.sha,p.epoch published,p.forecast_sha publication_sha,
            c.epoch consumed,c.forecast_sha consumption_sha,c.publication_sha consumption_publication_sha
            FROM forecasts f LEFT JOIN publication p ON p.id=f.id LEFT JOIN consumption c ON c.id=f.id ORDER BY f.bucket''')
        output=[]
        for row in rows:
            value=json.loads(row['payload'])
            if digest(value)!=row['sha']:raise ValueError('native_forecast_immutable_digest')
            errors=[]
            if row['published'] is None or row['consumed'] is None:
                errors=['forecast_not_independently_consumed']
            elif (row['publication_sha']!=row['sha'] or row['consumption_sha']!=row['sha'] or
                row['consumption_publication_sha']!=digest({'epoch':row['published'],'forecast_sha':row['sha']})):
                raise ValueError('native_publication_consumer_digest')
            else:
                arm={**value['forecasts'][0],'committed_available_epoch':row['consumed']}
                errors=forecast_errors(arm,value,self.contract['evaluation_protocol'])
                if not arm['issued_epoch']<=row['published']<=row['consumed']<value['target_epoch']:
                    errors.append('native_publication_after_target_or_before_issue')
            output.append({'forecast':value,'forecast_sha256':row['sha'],'publication_epoch':row['published'],
                'consumed_epoch':row['consumed'],'publication_errors':errors})
        return output

    @serialized
    def verified_native_publication(self):
        """Native summary plus independent publication/consumption receipts."""
        values=self._native_publications()
        value=next((v for v in reversed(values) if not v['publication_errors']),None)
        if value is None:return None
        decision=value['forecast'];arm=decision['forecasts'][0]
        pub={'epoch':value['publication_epoch'],'forecast_sha':value['forecast_sha256']}
        consumption={'id':decision['decision_id'],'epoch':value['consumed_epoch'],
            'forecast_sha':value['forecast_sha256'],'publication_sha':digest(pub)}
        return {'decision_id':decision['decision_id'],'instrument':decision['instrument'],'family':decision['family'],
            'horizon_sec':3600,'publication_epoch':value['publication_epoch'],'reference_epoch':decision['reference_epoch'],
            'reference_available_epoch':arm['input_source_observed_epoch'],'issued_epoch':arm['issued_epoch'],
            'target_epoch':decision['target_epoch'],'forecast_sha256':value['forecast_sha256'],
            'publication_receipt_sha256':digest(pub),'publication_verified':True,'verified_epoch':self._now(),
            'consumption_verified':True,'consumption_epoch':value['consumed_epoch'],
            'consumer_receipt_sha256':digest(consumption),'forecasts':decision['forecasts'],
            'native_anchor_sha256':decision['native_anchor_sha256'],
            'reference_quote_id':decision['reference_quote_id'],'reference_quote_role':decision['reference_quote_role']}

    @serialized
    def observe_native_source(self,candle_path,*,clock_path):
        path=native_outcome.plain_path(candle_path,missing=True)
        if str(path).lower()!=str(native_outcome.plain_path(self.contract['native_candle_path'],missing=True)).lower():
            raise ValueError('native_registered_candle_path_required')
        if not self.native_source_due():
            return {'status':'idle_no_unresolved_mature_target','sequence':None,'native_targets_scored':0}
        return native_store.observe_source(self,path,clock_path=clock_path)

    @serialized
    def native_source_due(self):
        now=self._now()
        # Publication diagnostics retain late/unknown rows, but they do not
        # cause perpetual source reads for forecasts that cannot be scored.
        for item in self._native_publications():
            forecast=item['forecast']
            if not item['publication_errors'] and forecast['target_epoch']<=now and not self.db.execute(
                'SELECT 1 FROM native_targets WHERE id=?',(forecast['decision_id'],)).fetchone():return True
        return False

    @serialized
    def settle(self):
        self.recover_publications();self.consume_publications()
        publications=self._native_publications()
        native_store.process_sources(self,[v['forecast'] for v in publications])
        scored=0;failures=[]
        for value in publications:
            if value['publication_errors']:continue
            try:
                scored+=native_store.settle(self,[value['forecast']],clock_path=self.contract['native_clock_path'])
            except (ValueError,KeyError,TypeError,OverflowError,OSError,sqlite3.Error) as exc:
                reason=type(exc).__name__+':'+str(exc)[:180]
                failures.append({'decision_id':value['forecast']['decision_id'],'reason_code':reason})
                native_store.diagnostic(self,reason+':'+value['forecast']['decision_id'])
        self.last_native_settlement_report={'status':'partial_refusal' if failures else 'settled',
            'native_targets_scored':scored,'failed_forecast_count':len(failures),'failures':failures[:16],
            'failure_details_truncated':len(failures)>16,'can_place_orders':False}
        return scored

    @serialized
    def counts(self):
        result={table:self.db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]
                for table in ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption')}
        result.update({table:self.db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0] for table in native_store.TABLES})
        result.update(entries=0,exclusions=0,outcomes=result['native_score_visibility'])
        return result

    @serialized
    def export_evaluation(self):
        read_started=self._now()
        publications=self._native_publications();cutoff=self._now()
        forecasts=[v['forecast'] for v in publications]
        statuses=native_store.outcome_statuses(self,forecasts,as_of_epoch=cutoff)
        # Independent exact-source verification streams one raw authority blob
        # per selected source; only compact verified score/status rows escape.
        selected={row['outcome']['source_sequence'] for row in statuses if row['status']=='scored'}
        source_rows={row['seq']:row for row in native_store.admitted_rows(self) if row['seq'] in selected}
        for seq in sorted(selected):
            if seq not in source_rows:raise ValueError('native_scored_source_admission_missing')
            item=native_store.verified_admission(self,source_rows[seq])
            rows=native_outcome.prices._source_rows(item['capture']['source'],item['capture']['read_completed_epoch'],self.contract['instrument'])
            for status,forecast in zip(statuses,forecasts,strict=True):
                if status['status']!='scored' or status['outcome']['source_sequence']!=seq:continue
                body=status['outcome'];anchor=forecast['forecasts'][0]['native_anchor']
                exact=native_outcome._select_from_verified(anchor,item['capture'],rows)
                expected=native_outcome.score_native(anchor,float.fromhex(exact['target_mid_hex']))
                if (encoded(exact)!=encoded(body['target']) or encoded(expected)!=encoded(body['score']) or
                    body['source_observation_sha256']!=item['observation_sha256'] or
                    body['source_visibility_sha256']!=item['visibility_sha256'] or
                    body['source_available_epoch']!=item['available_epoch']):
                    raise ValueError('native_outcome_exact_source_replay_mismatch')
            del rows,item
        read_completed=self._now()
        if not read_started<=cutoff<=read_completed:raise ValueError('native_export_read_clock_regression')
        protocol=dict(self.contract['evaluation_protocol'])
        protocol.update(historical_start_utc=utc(self.activated_epoch),historical_end_utc=utc(max(cutoff,self.activated_epoch+.000001)))
        return {'schema_version':'native_joint_forecast_evaluation_input_v1_20260913','observed_cutoff_epoch':cutoff,
            'publications':publications,'native_scores':statuses,'collection_counts':self.counts(),
            'source_replay_scope':'source_bound_ledger_independent_readback_exact_retained_m1_bytes',
            'external_publication_attestation_claimed':False,'quotes_used_as_native_targets':False,
            'export_read_started_epoch':read_started,'export_read_completed_epoch':read_completed,
            'availability_scope':'original_owner_observation_after_prior_commit_not_final_ack_row_visibility',
            'current_export_consumable_no_earlier_than_epoch':read_completed,
            'native_settlement_status':getattr(self,'last_native_settlement_report',None)},protocol
