"""Separate, append-only forecast/availability/entry ledger. No broker API.

Publication uses three boundaries: forecast commit, postcommit receipt, then
an independent consumer read. A later quote may fill an entry only after all
three. Existing prediction/outcome databases are neither imported nor opened.
"""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
import zlib
from oanda_exact_price_scoring import decimal_value, quote_midpoint

def canonical_price(value):
    text = format(value, 'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text

FAMILIES = ('probabilistic_state_space', 'ridge_return_repaired')
SCHEMA = 'causal_eurusd_two_family_ledger_v1_20260907'


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


def validate_contract(contract):
    from oanda_fixed_forecast_evaluation_eurusd_v1 import validate_protocol
    if contract.get('schema_version') != SCHEMA or not contract.get('contract_id'):
        raise ValueError('research_contract_required')
    if contract.get('research_only') is not True or any(contract.get(k) is not False for k in
            ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported')):
        raise ValueError('inert_research_flags_required')
    if contract.get('instrument') != 'EUR_USD' or contract.get('horizon_sec') != 3600 or contract.get('input_timeframe') != 'M1':
        raise ValueError('fixed_eurusd_h1_required')
    for key, expected in {'cadence_sec':900,'maximum_build_sec':120,'quote_max_age_sec':60,
                         'maximum_entry_delay_sec':60,'maximum_target_quote_delay_sec':60}.items():
        if number(contract.get(key)) != expected:
            raise ValueError('fixed_contract_tolerance_required:'+key)
    cohorts = contract.get('cohorts', {})
    if (set(cohorts) != set(FAMILIES) or len(set(cohorts.values())) != 2 or
            any(not isinstance(v,str) or not v.startswith('causal_eurusd_two_family_v1_20260907.') for v in cohorts.values())):
        raise ValueError('two_new_distinct_cohorts_required')
    if not isinstance(contract.get('dependency_versions'),dict) or not contract['dependency_versions']:
        raise ValueError('dependency_binding_required')
    model_hash = contract.get('numeric_model_source_sha256','')
    if len(model_hash) != 64 or any(c not in '0123456789abcdef' for c in model_hash):
        raise ValueError('numeric_source_sha256_required')
    protocol = contract['evaluation_protocol']
    validate_protocol(protocol)
    for key in ('instrument','input_timeframe','horizon_sec','cohorts','model_version','feature_version','quote_max_age_sec',
                'maximum_entry_delay_sec','maximum_target_quote_delay_sec'):
        if protocol.get(key) != contract.get(key):
            raise ValueError('evaluation_collection_contract_mismatch:'+key)
    if protocol['extra_cost_stress_bps'] != [0.0,0.5,1.0]:
        raise ValueError('fixed_cost_stresses_required')


class CausalForecastLedger:
    def __init__(self, path: Path, contract: dict, *, clock=time.time, activate=False):
        validate_contract(contract)
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self._contract_encoded = encoded(contract)
        self.contract_hash = digest(contract)
        self.db = sqlite3.connect(self.path, timeout=10)
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
            CREATE TABLE IF NOT EXISTS attempts(bucket INTEGER PRIMARY KEY, epoch REAL NOT NULL, reference_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS diagnostics(bucket INTEGER PRIMARY KEY, epoch REAL NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS inputs(id TEXT PRIMARY KEY, payload BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS forecasts(id TEXT PRIMARY KEY, bucket INTEGER UNIQUE NOT NULL, target REAL NOT NULL, sha TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS publication(id TEXT PRIMARY KEY, epoch REAL NOT NULL, forecast_sha TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS consumption(id TEXT PRIMARY KEY, epoch REAL NOT NULL, forecast_sha TEXT NOT NULL, publication_sha TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS entries(id TEXT PRIMARY KEY, quote_id TEXT NOT NULL, epoch REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS outcomes(id TEXT PRIMARY KEY, quote_id TEXT NOT NULL, epoch REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS exclusions(id TEXT PRIMARY KEY, reason TEXT NOT NULL, epoch REAL NOT NULL);
        ''')
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

    def observe_quote(self, quote: dict):
        """Persist the successor consumer's first actual observation of a tick."""
        market, read_observed = number(quote['market_epoch']), number(quote['available_epoch'])
        bid, ask = decimal_value(quote['bid']), decimal_value(quote['ask'])
        if quote.get('instrument') != 'EUR_USD' or quote.get('tradeable') is not True:
            raise ValueError('eurusd_tradeable_quote_required')
        observed = self._now()
        if not 0 < bid <= ask or not market <= read_observed <= observed:
            raise ValueError('quote_price_or_clock_order')
        if observed-market > self.contract['quote_max_age_sec'] or observed <= self.activated_epoch:
            raise ValueError('stale_or_preactivation_quote')
        identity = {'instrument':'EUR_USD','market_epoch':market,'bid':canonical_price(bid),'ask':canonical_price(ask),'tradeable':True}
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

    def reserve_attempt(self, bucket, reference):
        if type(bucket) is not int or bucket < 0:
            raise ValueError('cadence_bucket_required')
        existing = self.db.execute('SELECT 1 FROM attempts WHERE bucket=?', (bucket,)).fetchone()
        if existing:
            return False
        retained = self.db.execute('SELECT payload FROM quotes WHERE id=?', (reference['quote_id'],)).fetchone()
        if retained is None or json.loads(retained['payload']) != reference:
            raise ValueError('reference_not_observed')
        with self.db:
            at = self._tick('reserve_attempt')
            if bucket != int(at // self.contract['cadence_sec']):
                raise ValueError('attempt_outside_actual_cadence_bucket')
            if at-reference['available_epoch'] > self.contract['quote_max_age_sec']:
                raise ValueError('stale_reference')
            self.db.execute('INSERT INTO attempts VALUES(?,?,?)', (bucket, at, reference['quote_id']))
        return True

    def diagnostic(self, bucket, payload):
        with self.db:
            at = self._tick('attempt_diagnostic')
            self.db.execute('INSERT OR IGNORE INTO diagnostics VALUES(?,?,?)', (bucket, at, encoded(payload).decode()))

    def publish(self, bucket, capture, result, *, after_commit=None):
        """The hook is for crash tests; issuance is sampled here after fitting."""
        attempt = self.db.execute('SELECT * FROM attempts WHERE bucket=?', (bucket,)).fetchone()
        if attempt is None:
            raise ValueError('attempt_not_reserved')
        if capture.get('status') != 'ready' or result.get('status') != 'ready':
            raise ValueError('inputs_or_models_not_ready')
        predictions = result['predictions']
        if set(predictions) != set(FAMILIES):
            raise ValueError('both_local_families_required')
        observed = number(capture['first_observed_epoch'])
        maturity = number(capture['max_bar_close_epoch'])
        from oanda_causal_forecast_inputs_eurusd_v1 import validate_capture
        validate_capture(capture)
        if (result.get('source_capture_sha256') != capture.get('source_capture_sha256') or
                result.get('model_source_sha256') != self.contract['numeric_model_source_sha256'] or
                capture.get('model_source_sha256') != self.contract['numeric_model_source_sha256'] or
                result.get('dependency_versions') != self.contract['dependency_versions'] or
                capture.get('dependency_versions') != self.contract['dependency_versions']):
            raise ValueError('result_input_or_model_binding_mismatch')
        completed = number(result['computed_epoch'])
        reference = json.loads(self.db.execute('SELECT payload FROM quotes WHERE id=?', (attempt['reference_id'],)).fetchone()[0])
        target = number(reference['market_epoch']) + 3600
        with self.db:
            issued = self._tick('forecasts_issued_after_all_models')
            if not self.activated_epoch < observed <= completed < issued or not maturity <= observed:
                raise ValueError('input_or_learning_availability_order')
            if observed < attempt['epoch'] or issued >= target:
                raise ValueError('capture_before_attempt_or_target_expired')
            if issued-reference['available_epoch'] > self.contract['maximum_build_sec']:
                raise ValueError('construction_deadline_exceeded')
            capture_hash = digest(capture)
            decision_id = digest({'contract': self.contract_hash, 'bucket': bucket})
            forecasts = []
            for family in FAMILIES:
                prediction = predictions[family]
                p = number(prediction['probability_up'])
                expected = number(prediction['expected_signed_pips'])
                side = prediction['side']
                if not 0 <= p <= 1 or type(side) is not int or side not in (-1, 0, 1):
                    raise ValueError('invalid_model_output')
                mid = quote_midpoint(reference)
                forecasts.append({
                    'forecast_id': decision_id+':'+family, 'family': family,
                    'cohort_id': self.contract['cohorts'][family],
                    'model_version': self.contract['model_version'],
                    'feature_version': self.contract['feature_version'],
                    'instrument': 'EUR_USD', 'input_timeframe': 'M1', 'horizon_sec': 3600,
                    'issued_epoch': issued, 'reference_epoch': reference['market_epoch'],
                    'reference_mid': str(mid), 'target_epoch': target,
                    'feature_cutoff_epoch': maturity, 'features_available_epoch': completed,
                    'input_source_observed_epoch': observed,
                    'training_label_maturity_max_epoch': maturity,
                    'training_labels_available_max_epoch': completed,
                    'learning_data_role': 'observed_one_step_returns' if family == 'probabilistic_state_space' else 'completed_h1_training_labels',
                    'probability_up': p, 'predicted_return_bps': 10000*expected*.0001/float(mid),
                    'side': side, 'diagnostics': prediction.get('diagnostics', {}),
                    'account_eligible': False, 'proof_eligible': False, 'can_place_orders': False})
            payload = {'decision_id': decision_id, 'reference_epoch': reference['market_epoch'],
                'target_epoch': target, 'reference_quote_id': reference['quote_id'],
                'forecasts': forecasts, 'input_capture_sha256': capture_hash,
                'model_source_sha256': result['model_source_sha256'],
                'dependency_versions': result['dependency_versions']}
            raw_capture = encoded(capture)
            self.db.execute('INSERT OR IGNORE INTO inputs VALUES(?,?)', (capture_hash, zlib.compress(raw_capture, 6)))
            self.db.execute('INSERT INTO forecasts VALUES(?,?,?,?,?)', (decision_id, bucket, target, digest(payload), encoded(payload).decode()))
        if after_commit is not None:
            after_commit()
        # This clock certifies the preceding forecast commit, never a predicted
        # commit time. The consumer must also observe this second transaction.
        self.recover_publications()
        return decision_id

    def recover_publications(self):
        rows = self._read('SELECT f.id,f.sha,f.target FROM forecasts f LEFT JOIN publication p ON p.id=f.id WHERE p.id IS NULL')
        for row in rows:
            with self.db:
                at = self._tick('forecast_commit_independently_observed')
                self.db.execute('INSERT INTO publication VALUES(?,?,?)', (row['id'], at, row['sha']))

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

    def settle(self):
        self.recover_publications()
        self.consume_publications()
        rows = self.db.execute('''SELECT f.id,f.target,c.epoch AS consumed,e.quote_id AS entry,
            o.id AS outcome,x.id AS excluded FROM forecasts f JOIN consumption c ON c.id=f.id
            LEFT JOIN entries e ON e.id=f.id LEFT JOIN outcomes o ON o.id=f.id
            LEFT JOIN exclusions x ON x.id=f.id WHERE o.id IS NULL AND x.id IS NULL''').fetchall()
        for row in rows:
            now = self._now()
            with self.db:
                entry = row['entry']
                if entry is None:
                    quote = self.db.execute('''SELECT id FROM quotes WHERE market>? AND available>?
                        AND available<? AND available<=? ORDER BY available,market,id LIMIT 1''',
                        (row['consumed'],row['consumed'],row['target'],row['consumed']+self.contract['maximum_entry_delay_sec'])).fetchone()
                    if quote:
                        entry = quote['id']
                        at = self._tick('later_executable_entry_selected')
                        self.db.execute('INSERT INTO entries VALUES(?,?,?)', (row['id'], entry, at))
                    elif now > min(row['target'],row['consumed']+self.contract['maximum_entry_delay_sec']):
                        at = self._tick('entry_excluded')
                        self.db.execute('INSERT INTO exclusions VALUES(?,?,?)', (row['id'],'missing_later_entry_before_deadline',at))
                if entry is not None and now >= row['target']:
                    quote = self.db.execute('''SELECT id FROM quotes WHERE market>=? AND available<=?
                        ORDER BY available,market,id LIMIT 1''',
                        (row['target'],row['target']+self.contract['maximum_target_quote_delay_sec'])).fetchone()
                    if quote:
                        at = self._tick('original_target_outcome_selected')
                        self.db.execute('INSERT INTO outcomes VALUES(?,?,?)', (row['id'],quote['id'],at))
                    elif now > row['target']+self.contract['maximum_target_quote_delay_sec']:
                        at = self._tick('target_excluded')
                        self.db.execute('INSERT INTO exclusions VALUES(?,?,?)', (row['id'],'missing_quote_at_original_target',at))

    def counts(self):
        return {table:self.db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                for table in ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')}

    def export_evaluation(self):
        """Export actual clocks for the existing strict offline evaluator."""
        # Report building runs off the quote loop and uses its own read-only
        # snapshot. Never share the writer connection across Python threads.
        with closing(sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True)) as reader:
            reader.row_factory = sqlite3.Row
            reader.execute('PRAGMA query_only=ON')
            reader.execute('BEGIN')
            rows = reader.execute('''SELECT f.payload,f.sha,c.epoch FROM forecasts f
                LEFT JOIN consumption c ON c.id=f.id ORDER BY f.bucket''').fetchall()
            quotes = [json.loads(row[0]) for row in reader.execute('SELECT payload FROM quotes ORDER BY available,market,id')]
            counts = {table:reader.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                      for table in ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')}
            highwater = reader.execute('SELECT MAX(epoch) FROM clocks').fetchone()[0]
            cutoff = number(self.clock())
            if cutoff < highwater:
                raise ValueError('export_clock_regression')
            reader.rollback()
        decisions = []
        for row in rows:
            value = json.loads(row['payload'])
            if digest(value) != row['sha']:
                raise ValueError('export_forecast_hash_mismatch')
            for forecast in value['forecasts']:
                forecast['committed_available_epoch'] = row['epoch']
            decisions.append(value)
        protocol = dict(self.contract['evaluation_protocol'])
        protocol.update(historical_start_utc=utc(self.activated_epoch), historical_end_utc=utc(max(cutoff,self.activated_epoch+.000001)))
        return {'schema_version':'fixed_forecast_evaluation_input_v1', 'observed_cutoff_epoch':cutoff,
                'decisions':decisions, 'quotes':quotes, 'collection_counts':counts}, protocol

