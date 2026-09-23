"""Fixture-only worker checks; no broker, production database, worker or thread."""
import ast
from contextlib import ExitStack
from copy import deepcopy
import hashlib
import importlib
import json
from pathlib import Path
import sys
from unittest.mock import Mock, patch

import pytest

with patch.object(sys, 'path', [str(Path(__file__).resolve().parent), *sys.path]):
    study = importlib.import_module('oanda_causal_forecast_study_eurusd_v1')
    inputs = importlib.import_module('oanda_causal_forecast_inputs_eurusd_v1')


def snapshot():
    return {'producer': 'practice_007_dedicated_quote_stream',
            'generated_utc': '1999-01-01T00:00:00+00:00',
            'quotes': {'EUR_USD': {'time': '2026-09-06T21:05:10+00:00',
                       'bid': 1.1000, 'ask': 1.1002, 'tradeable': True, 'source': 'stream'}},
            'coverage': {'retained_last_known_instruments': []}}


def write_snapshot(tmp_path, payload):
    path = tmp_path / 'quotes.json'
    path.write_text(json.dumps(payload), encoding='utf-8')
    return path


def test_quote_uses_observed_after_read_clock_and_exact_source_hash(tmp_path):
    path = write_snapshot(tmp_path, snapshot())
    raw = path.read_bytes()
    events = []
    original = Path.read_bytes
    def read(p):
        events.append('read')
        return original(p)
    def clock():
        assert events == ['read']
        events.append('clock')
        return 1788728711.0
    with patch.object(Path, 'read_bytes', read):
        quote = study.read_quote(path, clock=clock)
    assert quote['available_epoch'] == 1788728711.0
    assert quote['source_snapshot_sha256'] == hashlib.sha256(raw).hexdigest()
    assert quote['producer'] == 'practice_007_dedicated_quote_stream'
    assert quote['market_epoch'] != quote['available_epoch']
    assert events == ['read', 'clock']


def test_numeric_json_prices_retain_decimal_digits_before_float_parsing(tmp_path):
    path=write_snapshot(tmp_path,snapshot())
    path.write_text(path.read_text().replace('1.1,','1.100000000000000001,').replace('1.1002,','1.100000000000000003,'))
    quote=study.read_quote(path,clock=lambda:1788728711.0)
    assert quote['bid']=='1.100000000000000001'
    assert quote['ask']=='1.100000000000000003'
    assert quote['raw_provider_quote']['bid']==quote['bid']
    json.dumps(quote,allow_nan=False)


@pytest.mark.parametrize('value', [False, None, 'true', 1])
def test_nonboolean_or_closed_tradeability_rejected(tmp_path, value):
    payload = snapshot()
    payload['quotes']['EUR_USD']['tradeable'] = value
    with pytest.raises(ValueError):
        study.read_quote(write_snapshot(tmp_path, payload), clock=lambda: 1788728711.0)


@pytest.mark.parametrize('change', ['producer', 'rest_source', 'retained', 'timezone'])
def test_quote_transport_provenance_rejected(tmp_path, change):
    payload = snapshot()
    if change == 'producer':
        payload['producer'] = 'old_executor'
    elif change == 'rest_source':
        payload['quotes']['EUR_USD']['source'] = 'rest_poll'
    elif change == 'retained':
        payload['coverage']['retained_last_known_instruments'] = ['EUR_USD']
    else:
        payload['quotes']['EUR_USD']['time'] = '2026-09-06T21:05:10'
    with pytest.raises(ValueError):
        study.read_quote(write_snapshot(tmp_path, payload), clock=lambda: 1788728711.0)


@pytest.mark.parametrize('malformation', ['null', 'list', 'quotes_null', 'quote_null', 'coverage_null', 'time_null'])
def test_malformed_quote_shapes_fail_as_value_error(tmp_path, malformation):
    payload = snapshot()
    if malformation == 'null': payload = None
    elif malformation == 'list': payload = []
    elif malformation == 'quotes_null': payload['quotes'] = None
    elif malformation == 'quote_null': payload['quotes']['EUR_USD'] = None
    elif malformation == 'coverage_null': payload['coverage'] = None
    elif malformation == 'time_null': payload['quotes']['EUR_USD']['time'] = None
    with pytest.raises(ValueError):
        study.read_quote(write_snapshot(tmp_path, payload), clock=lambda: 1788728711.0)


@pytest.mark.parametrize('field,value', [('bid', float('nan')), ('ask', float('inf')), ('bid', True), ('ask', 'bad')])
def test_non_numeric_and_nonfinite_quote_prices_rejected(tmp_path, field, value):
    payload = snapshot()
    payload['quotes']['EUR_USD'][field] = value
    with pytest.raises(ValueError):
        study.read_quote(write_snapshot(tmp_path, payload), clock=lambda: 1788728711.0)


def test_fit_once_only_calls_bound_input_model_adapter(tmp_path):
    captured = {'status': 'ready', 'source_capture_sha256': 'capture'}
    result = {'status': 'ready', 'source_capture_sha256': 'capture'}
    with patch.object(inputs, 'capture_inputs', return_value=captured) as capture:
        with patch.object(inputs, 'compute_predictions', return_value=result) as compute:
            assert study.fit_once(tmp_path) == (captured, result)
    capture.assert_called_once_with(tmp_path)
    compute.assert_called_once_with(captured)


def test_warmup_never_computes_predictions(tmp_path):
    captured = {'status': 'abstain', 'reasons': ['warmup']}
    with patch.object(inputs, 'capture_inputs', return_value=captured):
        with patch.object(inputs, 'compute_predictions', side_effect=AssertionError('must not compute')):
            captured_out, result = study.fit_once(tmp_path)
    assert captured_out is captured
    assert result == {'status': 'abstain', 'reasons': ['warmup']}


class LoopClock:
    def __init__(self, base=100): self.elapsed = 0; self.base = base
    def wall(self): return 1800000000.0 + self.elapsed
    def mono(self): return self.base + self.elapsed
    def sleep(self, duration): self.elapsed += duration


class FixtureLedger:
    def __init__(self):
        self.contract_hash = 'fixture-contract-hash'
        self.activated_epoch = 1799999999.0
        self.quotes = []
        self.attempts = set()
        self.settlements = 0
        self.publications = []
        self.diagnostics = []
        self.closed = False
    def observe_quote(self, quote):
        self.quotes.append(quote)
        return dict(quote, quote_id='fixture-' + str(len(self.quotes)))
    def settle(self): self.settlements += 1
    def reserve_attempt(self, bucket, reference):
        if bucket in self.attempts: return False
        self.attempts.add(bucket)
        return True
    def publish(self, bucket, capture, result): self.publications.append((bucket, capture, result))
    def diagnostic(self, bucket, payload): self.diagnostics.append((bucket, payload))
    def counts(self): return {'quotes':len(self.quotes), 'forecasts':len(self.publications)}
    def close(self): self.closed = True


class FixtureFuture:
    def __init__(self, result=None, delay=1000, error=None):
        self.payload = result
        self.delay = delay
        self.error = error
        self.polls = 0
    def done(self):
        self.polls += 1
        return self.polls >= self.delay
    def result(self):
        if self.error: raise self.error
        return self.payload


class FixtureExecutor:
    def __init__(self, future, score_future=None):
        self.future = future
        self.score_future = score_future or FixtureFuture()
        self.submitted = []
        self.stopped = False
    def submit(self, function, *args):
        self.submitted.append((function, args))
        return self.future if function is study.fit_once else self.score_future
    def shutdown(self, **kwargs): self.stopped = True; self.shutdown_options = kwargs


def run_fixture(tmp_path, *, quote_error=None, future=None, constructor_error=None, executor_error=None,
                score_future=None, monotonic_base=100, publish_error=None, once=False):
    clock = LoopClock(monotonic_base)
    ledger = FixtureLedger()
    future = future or FixtureFuture()
    executor = FixtureExecutor(future, score_future)
    contract = {'cadence_sec':900, 'contract_id':'fixture-study', 'numeric_model_source_sha256':'a'*64}
    writes = []
    def accepted_quote(path):
        if quote_error is not None: raise quote_error
        return {'instrument':'EUR_USD','market_epoch':clock.wall()-1,'available_epoch':clock.wall(),
                'bid':1.1,'ask':1.1002,'tradeable':True}
    with ExitStack() as stack:
        for name, value in (('ROOT', tmp_path), ('STUDY', tmp_path / 'study')):
            stack.enter_context(patch.object(study, name, value))
        stack.enter_context(patch.object(study, 'load_contract', return_value=contract))
        make_ledger = stack.enter_context(patch.object(study, 'CausalForecastLedger', return_value=ledger, side_effect=constructor_error))
        stack.enter_context(patch.object(study, 'ThreadPoolExecutor', return_value=executor, side_effect=executor_error))
        stack.enter_context(patch.object(study, 'read_quote', side_effect=accepted_quote))
        stack.enter_context(patch.object(study, 'write_scorecard', return_value={}))
        stack.enter_context(patch.object(study, 'atomic_json', side_effect=lambda p, value:writes.append((p, deepcopy(value)))))
        stack.enter_context(patch.object(study.time, 'time', clock.wall))
        stack.enter_context(patch.object(study.time, 'monotonic', clock.mono))
        stack.enter_context(patch.object(study.time, 'sleep', clock.sleep))
        if publish_error is not None:
            stack.enter_context(patch.object(ledger, 'publish', side_effect=publish_error))
        study.run(tmp_path/'config.json', activate=True, duration_sec=6, once=once)
    return ledger, executor, writes, make_ledger


def test_closed_weekend_keeps_waiting_without_reservation_or_models(tmp_path):
    ledger, executor, writes, _ = run_fixture(tmp_path, quote_error=ValueError('market_closed_or_quote_not_tradeable'))
    assert ledger.settlements == 4
    assert not ledger.quotes and not ledger.attempts and not ledger.publications
    assert not executor.submitted
    assert all(row['phase'] == 'waiting_for_tradeable_quote' for _, row in writes)
    assert all(row['supported_decision'] == 'no_trade' and row['can_place_orders'] is False for _, row in writes)
    assert ledger.closed and executor.stopped


def test_quote_and_settlement_loop_continues_while_fit_pending(tmp_path):
    future = FixtureFuture(delay=1000)
    ledger, executor, writes, _ = run_fixture(tmp_path, future=future)
    assert len(executor.submitted) == 1
    assert len(ledger.quotes) == 4 and ledger.settlements == 4
    assert future.polls == 3
    assert not ledger.publications
    assert all(row['phase'] == 'building_models_while_capturing_quotes' for _, row in writes[1:])


def test_quote_loop_keeps_collecting_during_background_fit_and_scorecard(tmp_path):
    future = FixtureFuture(delay=1000)
    score = FixtureFuture(delay=1000)
    ledger, executor, _, _ = run_fixture(tmp_path, future=future, score_future=score, monotonic_base=2000)
    assert len(executor.submitted) == 2
    assert len(ledger.quotes) == 4 and ledger.settlements == 4
    assert future.polls == score.polls == 3
    assert executor.shutdown_options == {'wait':True,'cancel_futures':True}


def test_background_scorecard_failure_is_reported_without_stopping_quotes(tmp_path):
    score = FixtureFuture(delay=1, error=ValueError('fixture export error'))
    ledger, _, writes, _ = run_fixture(tmp_path, score_future=score, monotonic_base=2000)
    assert len(ledger.quotes) == 4
    assert any(row['last_reason'].startswith('scorecard_failed') for _, row in writes)


def test_completed_fit_publishes_original_bound_objects_once(tmp_path):
    capture = {'status':'ready','source_capture_sha256':'fixture-capture'}
    result = {'status':'ready','model_source_sha256':'a'*64,'source_capture_sha256':'fixture-capture'}
    ledger, executor, _, _ = run_fixture(tmp_path, future=FixtureFuture((capture,result), delay=2))
    assert len(ledger.publications) == 1
    assert ledger.publications[0][1] is capture and ledger.publications[0][2] is result
    assert len(executor.submitted) == 1


@pytest.mark.parametrize('failure', ['exception','missing','numeric_binding','warmup'])
def test_model_failure_abstains_without_partial_publication(tmp_path, failure):
    capture = {'status':'ready','source_capture_sha256':'fixture-capture'}
    result = {'status':'ready','model_source_sha256':'a'*64}
    error = None
    if failure == 'exception': error = ValueError('fixture numerical failure')
    elif failure == 'missing': result = None
    elif failure == 'numeric_binding': result['model_source_sha256'] = 'b'*64
    else: capture = result = {'status':'abstain','reasons':['fixture warmup']}
    ledger, _, writes, _ = run_fixture(tmp_path, future=FixtureFuture((capture,result),delay=1,error=error))
    assert not ledger.publications
    assert len(ledger.diagnostics) == 1
    assert ledger.diagnostics[0][1]['status'] in ('abstain','failed_closed')
    assert any(row['last_reason'] for _, row in writes)


@pytest.mark.parametrize('reason', ['result_input_or_model_binding_mismatch', 'input_or_learning_availability_order',
                                  'construction_deadline_exceeded', 'numerical_source_binding_changed'])
def test_ledger_validation_failure_is_preserved_as_failed_closed(tmp_path, reason):
    capture = {'status':'ready','source_capture_sha256':'fixture-capture'}
    result = {'status':'ready','model_source_sha256':'a'*64}
    ledger, _, writes, _ = run_fixture(tmp_path, future=FixtureFuture((capture,result),delay=1),
                                       publish_error=ValueError(reason))
    assert not ledger.publications
    assert len(ledger.diagnostics) == 1
    assert ledger.diagnostics[0][1]['status'] == 'failed_closed'
    assert reason in ledger.diagnostics[0][1]['reason']


def test_duplicate_lock_refuses_before_ledger_construction(tmp_path):
    import msvcrt
    with patch.object(msvcrt, 'locking', side_effect=OSError('fixture lock held')):
        with patch.object(study, 'CausalForecastLedger') as construct:
            with pytest.raises(RuntimeError, match='already_running'):
                # Keep this separate from run_fixture's ledger constructor patch.
                with patch.object(study, 'STUDY', tmp_path/'study'):
                    with patch.object(study, 'load_contract', return_value={}):
                        study.run(tmp_path/'config.json', once=True)
            construct.assert_not_called()


def test_failed_ledger_constructor_releases_lock(tmp_path):
    import msvcrt
    original = msvcrt.locking
    events = []
    def locking(fd, mode, length):
        events.append(mode)
        return original(fd, mode, length)
    with patch.object(msvcrt, 'locking', side_effect=locking):
        with pytest.raises(ValueError, match='fixture contract failure'):
            run_fixture(tmp_path, constructor_error=ValueError('fixture contract failure'))
    assert events == [msvcrt.LK_NBLCK, msvcrt.LK_UNLCK]


def test_failed_executor_constructor_releases_lock_and_closes_ledger(tmp_path):
    import msvcrt
    original = msvcrt.locking
    events = []
    original_close = FixtureLedger.close
    def closed(instance):
        events.append('ledger_closed')
        original_close(instance)
    def locking(fd, mode, length):
        events.append(mode)
        return original(fd, mode, length)
    with patch.object(msvcrt, 'locking', side_effect=locking):
        with patch.object(FixtureLedger, 'close', closed):
            with pytest.raises(RuntimeError, match='fixture executor failure'):
                run_fixture(tmp_path, executor_error=RuntimeError('fixture executor failure'))
    assert events == [msvcrt.LK_NBLCK, 'ledger_closed', msvcrt.LK_UNLCK]


def fixture_contract(tmp_path):
    sources = {}
    for relative in study.REQUIRED_SOURCE_BINDINGS:
        raw = ('source fixture: '+relative).encode()
        (tmp_path/relative).write_bytes(raw)
        sources[relative] = hashlib.sha256(raw).hexdigest()
    contract = {'collection_enabled':True,
        'can_place_orders':False,'can_promote':False,'can_authorize':False,
        'account_eligible':False,'proof_eligible':False,'historical_rows_imported':False,
        'source_bindings':sources,
        'dependency_versions':{'python':study.platform.python_version(),'numpy':'numpy-fixture','scikit-learn':'sklearn-fixture'}}
    return contract


def load_fixture_contract(tmp_path, contract):
    path = tmp_path/'config.json'
    path.write_text(json.dumps(contract),encoding='utf-8')
    with patch.object(study,'ROOT',tmp_path):
        with patch.object(study.importlib.metadata,'version',side_effect={'numpy':'numpy-fixture','scikit-learn':'sklearn-fixture'}.__getitem__):
            return study.load_contract(path)


def test_full_source_and_dependency_bindings_including_python_runtime(tmp_path):
    contract = fixture_contract(tmp_path)
    assert load_fixture_contract(tmp_path,contract) == contract


@pytest.mark.parametrize('flag',['collection_enabled','can_place_orders','can_promote','can_authorize',
                               'account_eligible','proof_eligible','historical_rows_imported'])
def test_unsafe_or_disabled_contract_refused(tmp_path,flag):
    contract = fixture_contract(tmp_path)
    contract[flag] = not contract[flag]
    with pytest.raises(ValueError):
        load_fixture_contract(tmp_path,contract)


@pytest.mark.parametrize('change',['missing_source','missing_dependency','wrong_source_hash','source_changed',
                                 'dependency_changed','python_changed','extra_dependency','extra_source'])
def test_source_and_dependency_bindings_fail_closed(tmp_path,change):
    contract = fixture_contract(tmp_path)
    first = sorted(study.REQUIRED_SOURCE_BINDINGS)[0]
    if change == 'missing_source': del contract['source_bindings'][first]
    elif change == 'missing_dependency': del contract['dependency_versions']['numpy']
    elif change == 'wrong_source_hash': contract['source_bindings'][first] = 'f'*64
    elif change == 'source_changed': (tmp_path/first).write_bytes(b'altered source')
    elif change == 'dependency_changed': contract['dependency_versions']['numpy'] = 'altered'
    elif change == 'python_changed': contract['dependency_versions']['python'] = 'altered'
    elif change == 'extra_dependency': contract['dependency_versions']['unknown'] = 'altered'
    elif change == 'extra_source': contract['source_bindings']['unknown.py'] = 'f'*64
    with pytest.raises(ValueError):
        load_fixture_contract(tmp_path,contract)


def test_atomic_heartbeat_publication_retains_reviewable_payload(tmp_path):
    path = tmp_path/'heartbeat.json'
    expected = {'research_only':True,'can_place_orders':False,'supported_decision':'no_trade'}
    study.atomic_json(path, expected)
    assert json.loads(path.read_bytes()) == expected
    assert list(tmp_path.iterdir()) == [path]


def test_worker_import_surface_has_no_network_broker_or_process_launcher():
    tree = ast.parse(Path(study.__file__).read_text(encoding='utf-8'))
    names = {node.module for node in ast.walk(tree) if isinstance(node,ast.ImportFrom)}
    names |= {alias.name for node in ast.walk(tree) if isinstance(node,ast.Import) for alias in node.names}
    forbidden = ('requests','http','urllib','socket','subprocess','oanda_practice_shadow_strategy_lab',
                 'oanda_gpt_training_strategy_manager','oanda_shadow_outcome_store')
    assert not any(name and name.startswith(forbidden) for name in names)
    assert study.CAN_PLACE_ORDERS is False
