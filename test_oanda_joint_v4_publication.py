"""Whole-worker publication probes, actual atomic files and synthetic state only."""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import ast
import hashlib
import importlib.util
import json
import sys

import pytest

import oanda_joint_price_news_forecast_study_v4 as worker

FAMILY = 'ridge_price_news_v1'


@pytest.fixture(autouse=True)
def refuse_network(monkeypatch):
    import socket
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('no_network_expected'))


def runner_fixture(tmp_path, monkeypatch, module=worker):
    clock = [1000.0]
    ledger = SimpleNamespace(activated_epoch=900.0, contract_hash='a' * 64,
        contract={'cohorts': {FAMILY: 'synthetic_publication_only'}}, counts=lambda: {})
    attempt = {'epoch': 998.0, 'reason': 'building', 'attempt_id': 'synthetic_attempt', 'bucket': 1}
    capture = {'first_observed_epoch': 998.0, 'max_bar_close_epoch': 990.0,
        'news_evidence_epoch': 998.0, 'news_expires_epoch': 1298.0, 'status': 'ready',
        'family_readiness': {FAMILY: {'ready': True, 'reasons': []}}}
    slot = {'ledger': ledger, 'publication': None, 'building': True,
        'last_attempt': attempt, 'reason': 'building'}
    registry = {'pairs': {'EUR_USD': {'pip_size': 0.0001}}}
    runner = module.PairRunner.__new__(module.PairRunner)
    runner.registry = registry
    runner.states = {'EUR_USD': {'capture': capture, 'families': {FAMILY: slot}}}
    runner.study = tmp_path
    runner.clock = lambda: clock[0]
    runner.last_summary = runner.last_heartbeat = 0.0
    runner.summary = runner.published_summary = None
    runner.errors = runner.heartbeat_errors = 0
    runner.last_error = ''
    runner.active = None
    runner.error = lambda pair, error: pytest.fail('unexpected ledger verification error')
    monkeypatch.setattr(module, 'verified_publication', lambda ledger: None)
    return runner, clock, attempt, capture


def read_header(tmp_path):
    return json.loads((tmp_path / 'heartbeat.json').read_bytes())




@pytest.mark.parametrize('mutation', ['attempt', 'readiness', 'summary_alias', 'state_population'])
def test_header_always_describes_exact_published_generation(tmp_path, monkeypatch, mutation):
    runner, clock, attempt, capture = runner_fixture(tmp_path, monkeypatch)
    runner.publish_status(force=True)
    raw = (tmp_path / 'summary.json').read_bytes()
    first = read_header(tmp_path)
    if mutation == 'attempt':
        attempt['reason'] = 'published'
    elif mutation == 'readiness':
        capture['family_readiness'][FAMILY]['ready'] = False
    elif mutation == 'summary_alias':
        runner.summary['rows'][0]['families'][FAMILY]['status'] = 'forecast'
    else:
        runner.states['GBP_USD'] = deepcopy(runner.states['EUR_USD'])
    clock[0] = 1006.0
    runner.publish_status()
    header = read_header(tmp_path)
    assert header['summary_sha256'] == hashlib.sha256(raw).hexdigest()
    assert header['summary_sha256'] == first['summary_sha256']
    assert header['counts'] == first['counts']
    assert header['pair_count'] == first['pair_count'] == 1
    assert header['summary_generated_epoch'] == 1000.0
    assert header['generated_epoch'] == 1006.0
    assert (tmp_path / 'summary.json').read_bytes() == raw


@pytest.mark.parametrize('failure', ['write', 'read', 'mismatched', 'truncated', 'future_readback'])
def test_failed_write_or_readback_never_advances_acknowledged_generation(tmp_path, monkeypatch, failure):
    runner, clock, attempt, capture = runner_fixture(tmp_path, monkeypatch)
    runner.publish_status(force=True)
    old = runner.published_summary
    prior_header = (tmp_path / 'heartbeat.json').read_bytes()
    clock[0] = 1020.0
    actual_write = worker.atomic_bytes

    def writing(path, raw):
        if failure == 'write':
            raise OSError('synthetic_write_failure')
        actual_write(path, raw)
        if failure == 'future_readback':
            clock[0] = 1019.0

    def reading(path, limit):
        if failure == 'read':
            raise OSError('synthetic_read_failure')
        raw = Path(path).read_bytes()
        return b'{}' if failure == 'mismatched' else raw[:-1] if failure == 'truncated' else raw

    monkeypatch.setattr(worker, 'atomic_bytes', writing)
    monkeypatch.setattr(worker, 'bounded_bytes', reading)
    with pytest.raises((OSError, worker.summary_boundary.SummaryBoundaryError)):
        runner.publish_status(force=True)
    assert runner.published_summary is old
    assert runner.last_summary == 1000.0
    assert (tmp_path / 'heartbeat.json').read_bytes() == prior_header


def test_summary_sealed_before_writer_and_actual_completion_recorded(tmp_path, monkeypatch):
    runner, clock, attempt, capture = runner_fixture(tmp_path, monkeypatch)
    actual_write = worker.atomic_bytes

    def writing(path, raw):
        if Path(path).name == 'summary.json':
            attempt['reason'] = 'mutated_during_write'
            capture['family_readiness'][FAMILY]['ready'] = False
            clock[0] = 1003.0
        return actual_write(path, raw)

    monkeypatch.setattr(worker, 'atomic_bytes', writing)
    runner.publish_status(force=True)
    raw = (tmp_path / 'summary.json').read_bytes()
    retained = json.loads(raw)['rows'][0]['families'][FAMILY]
    header = read_header(tmp_path)
    assert retained['last_attempt']['reason'] == 'building'
    assert retained['current_readiness']['diagnostics']['ready'] is True
    assert header['summary_generated_epoch'] == 1000.0
    assert header['summary_read_completed_epoch'] == 1003.0
    assert header['generated_epoch'] == 1003.0
    assert header['summary_sha256'] == hashlib.sha256(raw).hexdigest()


def test_subsequent_success_adopts_new_generation_and_keeps_original_row_times(tmp_path, monkeypatch):
    runner, clock, attempt, capture = runner_fixture(tmp_path, monkeypatch)
    runner.publish_status(force=True)
    previous = runner.published_summary
    clock[0] = 1020.0
    attempt['reason'] = 'published'
    runner.publish_status()
    assert runner.published_summary != previous
    slot = json.loads(runner.published_summary.snapshot.raw)['rows'][0]['families'][FAMILY]
    assert slot['last_attempt']['epoch'] == 998.0
    assert slot['current_readiness']['news_evidence_epoch'] == 998.0
    assert read_header(tmp_path)['summary_generated_epoch'] == 1020.0


def test_tick_accounts_for_boundary_failure_without_losing_prior_publication(tmp_path, monkeypatch):
    runner, clock, attempt, capture = runner_fixture(tmp_path, monkeypatch)
    runner.publish_status(force=True)
    previous = runner.published_summary
    runner.last_poll = 1000.0
    for method in ['finish_work', 'schedule_work', 'score']:
        setattr(runner, method, lambda: None)
    def failing():
        raise worker.summary_boundary.SummaryBoundaryError('summary_boundary_readback_mismatch')
    runner.publish_status = failing
    runner.tick()
    assert runner.published_summary is previous
    assert runner.errors == runner.heartbeat_errors == 1
    assert runner.last_error == 'status_publication:SummaryBoundaryError'


def test_source_binding_and_default_locations_are_new_and_inactive():
    assert worker.REGISTRY_SCHEMA == 'joint_price_news_registry_v4_20260912'
    assert worker.COHORT_SCOPE == 'immutable_publication_v4_20260912'
    assert worker.STUDY.name == 'joint_price_news_study_v4'
    assert worker.DEFAULT_CONFIG.name == 'joint_price_news_study_v4_20260912.json'
    assert 'oanda_immutable_summary_publication_v1.py' in worker.REQUIRED_SOURCE_BINDINGS
    assert 'oanda_joint_price_news_forecast_study_v4.py' in worker.REQUIRED_SOURCE_BINDINGS
    assert 'oanda_joint_price_news_forecast_study_v3.py' not in worker.REQUIRED_SOURCE_BINDINGS


