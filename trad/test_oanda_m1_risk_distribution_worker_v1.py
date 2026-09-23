from copy import deepcopy
from pathlib import Path

import pytest

import oanda_m1_risk_distribution_worker_v1 as w
import oanda_m1_risk_distribution_contract_v1 as c
import test_oanda_m1_risk_distribution_contract_v1 as fixtures
import test_oanda_m1_mba_research_capture_v1 as captures


class Clock:
    def __init__(self, value): self.value = value
    def __call__(self):
        result = self.value
        self.value += .001
        return result


@pytest.fixture
def setup(monkeypatch, tmp_path):
    ref = fixtures.REFERENCE
    monkeypatch.setattr(captures, 'NOW', ref + 1.)
    raw, receipt, mapping = captures.make_capture(monkeypatch)
    fit_raw = c.canonical(fixtures.training_artifact())
    monkeypatch.setattr(c, 'TRAINING_ARTIFACT_SHA256', c.sha(fit_raw))
    fit = tmp_path / 'training.json'
    fit.write_bytes(fit_raw)
    source_bindings = {name: c.sha((w.ROOT / name).read_bytes()) for name in sorted(w.SOURCE_NAMES)}
    registry = dict(schema_version=w.SCHEMA, registry_id=w.REGISTRY_ID,
        cohort_id=c.COHORT_ID, created_epoch=ref - 100, first_reference_epoch=ref,
        last_reference_epoch=w.LAST_REFERENCE, collection_end_epoch=w.COLLECTION_END,
        capture_cadence_sec=60, capture_start_delay_sec=2,
        source_bindings=source_bindings,
        metadata={pair: captures.metadata(pair) for pair in c.PAIRS},
        fit_artifact_path=str(fit), fit_artifact_sha256=c.sha(fit_raw),
        output_root=str(w.ROOT / 'data/oanda_training_manager' / w.REGISTRY_ID),
        registered_methods=list(c.METHODS), registered_labels=list(c.labels.CONTINUOUS_LABELS),
        registered_horizons_minutes=list(c.labels.HORIZONS), forecast_policy={
            'all_methods_and_labels_required': True, 'maximum_entry_age_sec': 20,
            'minimum_same_session_rows': 204, 'original_targets_retimed': False,
            'refit_or_method_selection': False, 'risk_routing_to_management': False,
            'missing_prices_filled': False, 'late_issue_backfill': False}, **c.AUTHORITY)
    clock = Clock(ref + .5)
    calls = []
    def fake_capture(pair, **kwargs):
        calls.append((pair, kwargs['credential_path']))
        clock.value = ref + 2.
        return raw, deepcopy(receipt)
    monkeypatch.setattr(w.capture, 'capture_once', fake_capture)
    directory = tmp_path / 'cycle'
    directory.mkdir()
    return registry, fit_raw, raw, receipt, directory, clock, calls


def test_actual_io_chain_publishes_before_independent_read(setup):
    registry, fit_raw, raw, receipt, directory, clock, calls = setup
    result = w.collect_pair(registry, directory, 'EUR_USD', fixtures.REFERENCE, fit_raw, clock=clock)
    assert result['status'] == 'issued_published_consumed'
    assert result['nodes'] == 72 and len(calls) == 1
    pair = directory / 'eur_usd'
    issued, pub, consumed = [c.decode((pair / name).read_bytes())
                             for name in ('issued.json', 'publication.json', 'consumption.json')]
    observed = c.decode((pair / 'consumer_read_evidence.json').read_bytes())
    assert (result['artifacts']['issued']['write_readback_completed_epoch'] <= pub['publication_completed_epoch']
            <= result['artifacts']['publication']['write_readback_completed_epoch']
            <= consumed['read_started_epoch'] <= observed['issued']['read_started_epoch']
            <= observed['issued']['read_completed_epoch'] <= observed['publication']['read_started_epoch']
            <= observed['publication']['read_completed_epoch'] <= consumed['read_completed_epoch'])
    assert c.validate_chain(issued, pub, consumed, input_raw=raw, input_receipt=receipt,
        metadata=registry['metadata']['EUR_USD'], fit_raw=fit_raw,
        expected_source_bindings=registry['source_bindings']) == issued
    assert result['can_place_orders'] is False and result['positions_managed'] is False


def test_nonissue_minute_captures_without_distribution(setup):
    registry, fit, _, _, directory, clock, _ = setup
    registry['first_reference_epoch'] += 300
    result = w.collect_pair(registry, directory, 'EUR_USD', fixtures.REFERENCE, fit, clock=clock)
    assert result['status'] == 'captured_only'
    assert not (directory / 'eur_usd/issued.json').exists()
    assert (directory / 'eur_usd/source_raw.json').exists()


def test_changed_source_refuses_before_get(setup):
    registry, fit, _, _, directory, clock, calls = setup
    registry['source_bindings'][next(iter(registry['source_bindings']))] = '0' * 64
    result = w.collect_pair(registry, directory, 'EUR_USD', fixtures.REFERENCE, fit, clock=clock)
    assert result['reason_code'] == 'risk_worker_source_changed' and not calls


def test_no_get_near_collection_boundary(setup):
    registry, fit, _, _, directory, clock, calls = setup
    clock.value = w.COLLECTION_END - 10
    result = w.collect_pair(registry, directory, 'EUR_USD', w.COLLECTION_END - 60, fit, clock=clock)
    assert result['reason_code'] == 'risk_worker_capture_stop_boundary' and not calls


def test_slow_source_check_cannot_admit_get_after_stop_boundary(setup, monkeypatch):
    registry, fit, _, _, directory, clock, calls = setup
    def delayed(_): clock.value = w.COLLECTION_END - 10
    monkeypatch.setattr(w, 'source_check', delayed)
    result = w.collect_pair(registry, directory, 'EUR_USD', w.COLLECTION_END - 60, fit, clock=clock)
    assert result['reason_code'] == 'risk_worker_capture_stop_boundary' and not calls


def test_delayed_capture_initialization_rechecks_actual_request_start(setup, monkeypatch):
    registry, fit, _, _, directory, clock, _ = setup
    request_sent = []
    def delayed(pair, **kwargs):
        clock.value = w.COLLECTION_END - 10
        kwargs['clock']()
        request_sent.append(pair)
        pytest.fail('late request started')
    monkeypatch.setattr(w.capture, 'capture_once', delayed)
    result = w.collect_pair(registry, directory, 'EUR_USD', fixtures.REFERENCE, fit, clock=clock)
    assert result['reason_code'] == 'm1_worker_request_start_outside_window'
    assert not request_sent and result['capture_attempted'] is False


def test_capture_failure_keeps_raw_and_receipt_without_issue(setup, monkeypatch):
    registry, fit, raw, receipt, directory, clock, _ = setup
    receipt.update(status='failed', error_reason='provider_omission_fixture')
    monkeypatch.setattr(w.capture, 'capture_once', lambda *a, **k: (raw, receipt))
    result = w.collect_pair(registry, directory, 'EUR_USD', fixtures.REFERENCE, fit, clock=clock)
    assert result['status'] == 'withheld_or_partial'
    assert (directory / 'eur_usd/source_raw.json').read_bytes() == raw
    assert not (directory / 'eur_usd/issued.json').exists()


def test_empty_failed_capture_retains_explicit_zero_byte_identity(setup, monkeypatch):
    registry, fit, _, receipt, directory, clock, _ = setup
    receipt.update(status='failed', error_reason='connection_failure_fixture')
    monkeypatch.setattr(w.capture, 'capture_once', lambda *a, **k: (b'', receipt))
    result = w.collect_pair(registry, directory, 'EUR_USD', fixtures.REFERENCE, fit, clock=clock)
    assert result['empty_raw_response'] == {'bytes': 0, 'sha256': c.sha(b'')}
    assert (directory / 'eur_usd/capture_receipt.json').exists()


def test_late_capture_is_retained_but_no_forecast_is_backfilled(setup, monkeypatch):
    registry, fit, raw, receipt, directory, clock, _ = setup
    def late(*a, **k):
        clock.value = fixtures.REFERENCE + 21
        return raw, receipt
    monkeypatch.setattr(w.capture, 'capture_once', late)
    result = w.collect_pair(registry, directory, 'EUR_USD', fixtures.REFERENCE, fit, clock=clock)
    assert result['reason_code'] == 'risk_worker_original_issue_window_expired'
    assert (directory / 'eur_usd/source_raw.json').exists()
    assert not (directory / 'eur_usd/issued.json').exists()


def test_old_capture_clock_cannot_be_called_new_get(setup, monkeypatch):
    registry, fit, raw, receipt, directory, clock, _ = setup
    clock.value = fixtures.REFERENCE + 3
    def replay_old_capture(*a, **k):
        clock.value = fixtures.REFERENCE + 4
        return raw, receipt
    monkeypatch.setattr(w.capture, 'capture_once', replay_old_capture)
    result = w.collect_pair(registry, directory, 'EUR_USD', fixtures.REFERENCE, fit, clock=clock)
    assert result['reason_code'] == 'risk_worker_actual_capture_clock_order'


def test_failed_publication_readback_never_produces_consumer(setup, monkeypatch):
    registry, fit, _, _, directory, clock, _ = setup
    original = w.record
    def fail(directory, name, value, **kwargs):
        if name == 'publication.json': raise ValueError('fixture_publication_failed')
        return original(directory, name, value, **kwargs)
    monkeypatch.setattr(w, 'record', fail)
    result = w.collect_pair(registry, directory, 'EUR_USD', fixtures.REFERENCE, fit, clock=clock)
    assert result['reason_code'] == 'fixture_publication_failed'
    assert (directory / 'eur_usd/issued.json').exists()
    assert not (directory / 'eur_usd/consumption.json').exists()


def test_late_publication_is_preserved_as_partial_not_retimed(setup, monkeypatch):
    registry, fit, _, _, directory, clock, _ = setup
    original = w.record
    def delayed(directory, name, value, **kwargs):
        receipt = original(directory, name, value, **kwargs)
        if name == 'issued.json': clock.value = fixtures.REFERENCE + 21
        return receipt
    monkeypatch.setattr(w, 'record', delayed)
    result = w.collect_pair(registry, directory, 'EUR_USD', fixtures.REFERENCE, fit, clock=clock)
    assert result['status'] == 'withheld_or_partial' and result['phase'] == 'issue_computed'
    assert result['reason_code'] == 'risk_publication_clock_or_delay'
    assert not (directory / 'eur_usd/publication.json').exists()


def test_registry_load_checks_full_inventory(setup, tmp_path):
    registry, *_ = setup
    path = tmp_path / 'registry.json'
    raw = c.canonical(registry)
    path.write_bytes(raw)
    restored, digest = w.load_registry(path, c.sha(raw), clock=lambda: fixtures.REFERENCE)
    assert restored == registry and digest == c.sha(raw)


@pytest.mark.parametrize('kind', ['authority', 'missing_source', 'fit', 'wrong_output',
                                  'changed_grid', 'new_methods', 'new_labels', 'refit',
                                  'late_target', 'unknown_field', 'bool_cadence'])
def test_registry_cannot_change_fixed_research_scope(setup, tmp_path, kind):
    registry, *_ = setup
    if kind == 'authority': registry['can_place_orders'] = True
    if kind == 'missing_source': registry['source_bindings'].pop(next(iter(registry['source_bindings'])))
    if kind == 'fit': registry['fit_artifact_sha256'] = '0' * 64
    if kind == 'wrong_output': registry['output_root'] = str(tmp_path / 'elsewhere')
    if kind == 'changed_grid': registry['first_reference_epoch'] += 60
    if kind == 'new_methods': registry['registered_methods'] = ['chosen_method']
    if kind == 'new_labels': registry['registered_labels'] = ['chosen_label']
    if kind == 'refit': registry['forecast_policy']['refit_or_method_selection'] = True
    if kind == 'late_target': registry['last_reference_epoch'] += 300
    if kind == 'unknown_field': registry['unknown'] = True
    if kind == 'bool_cadence': registry['capture_cadence_sec'] = True
    path = tmp_path / 'registry.json'
    raw = c.canonical(registry)
    path.write_bytes(raw)
    with pytest.raises(ValueError): w.load_registry(path, c.sha(raw), clock=lambda: fixtures.REFERENCE)


def test_immutable_records_cannot_be_overwritten(setup):
    *_, directory, clock, _ = setup
    w.record(directory, 'first.json', {'a': 1}, clock=clock)
    with pytest.raises(ValueError, match='existing_record'):
        w.record(directory, 'first.json', {'a': 2}, clock=clock)
    assert (directory / 'first.json').read_bytes() == b'{"a":1}'


def test_worker_lock_refuses_second_owner_and_releases(setup):
    *_, directory, _, _ = setup
    with w.worker_lock(directory):
        with pytest.raises(OSError):
            with w.worker_lock(directory): pytest.fail('second owner entered')
    with w.worker_lock(directory): pass


def test_once_run_records_fit_read_and_preserved_source(setup, monkeypatch, tmp_path):
    registry, fit, *_ = setup
    registry['output_root'] = str(tmp_path / 'isolated_runtime')
    clock = Clock(fixtures.REFERENCE + 2.5)
    def collect(registry, directory, pair, cycle_epoch, fit_raw, **kwargs):
        assert fit_raw == fit
        return {'instrument': pair, 'status': 'captured_only'}
    monkeypatch.setattr(w, 'collect_pair', collect)
    w.run(registry, '0' * 64, clock=clock, once=True)
    sessions = list((Path(registry['output_root']) / 'sessions').iterdir())
    assert len(sessions) == 1
    start = c.decode((sessions[0] / 'started.json').read_bytes())
    stop = c.decode((sessions[0] / 'stopped.json').read_bytes())
    assert start['training_artifact_read']['bytes_sha256'] == c.sha(fit)
    assert (sessions[0] / 'training_artifact.json').read_bytes() == fit
    assert stop['reason'] == 'once_complete' and stop['completed_cycles'] == 1


def test_partial_publication_is_counted_separately_from_consumption(setup, monkeypatch, tmp_path):
    registry, *_ = setup
    registry['output_root'] = str(tmp_path / 'partial_runtime')
    clock = Clock(fixtures.REFERENCE + 2.5)
    def collect(registry, directory, pair, cycle_epoch, fit_raw, **kwargs):
        return {'instrument': pair, 'status': 'withheld_or_partial',
                'artifacts': {'issued': {'file': 'issued.json'}, 'publication': {'file': 'publication.json'}}}
    monkeypatch.setattr(w, 'collect_pair', collect)
    w.run(registry, '0' * 64, clock=clock, once=True)
    path = Path(registry['output_root']) / 'cycles' / str(fixtures.REFERENCE) / 'cycle_completed.json'
    cycle = c.decode(path.read_bytes())
    assert cycle['retained_issue_count'] == cycle['publication_count'] == 3
    assert cycle['consumption_count'] == cycle['complete_chain_count'] == 0


def test_skipped_capture_minutes_are_retained_without_backfill(setup, monkeypatch, tmp_path):
    registry, *_ = setup
    registry['output_root'] = str(tmp_path / 'missed_runtime')
    clock = Clock(fixtures.REFERENCE + 2.5)
    calls = []
    def collect(registry, directory, pair, cycle_epoch, fit_raw, **kwargs):
        calls.append(cycle_epoch)
        if len(calls) == 3: clock.value = fixtures.REFERENCE + 182.5
        if len(calls) == 6: clock.value = w.COLLECTION_END + 1
        return {'instrument': pair, 'status': 'captured_only'}
    monkeypatch.setattr(w, 'collect_pair', collect)
    w.run(registry, '0' * 64, clock=clock)
    path = Path(registry['output_root']) / 'cycles' / str(fixtures.REFERENCE + 180) / 'cycle_completed.json'
    second = c.decode(path.read_bytes())
    assert second['skipped_prior_capture_cycle_epochs'] == [fixtures.REFERENCE + 60, fixtures.REFERENCE + 120]
    assert calls == [fixtures.REFERENCE] * 3 + [fixtures.REFERENCE + 180] * 3
