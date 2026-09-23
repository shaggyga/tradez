"""The repaired study requires an explicit, inert, registry-bound selection."""
import copy
import hashlib
import json
import socket
import sqlite3

import pytest
import oanda_practice_live_dashboard as dashboard
import test_oanda_joint_price_news_dashboard_v1 as previous
import test_oanda_pair_forecast_dashboard_v2 as baseline

NOW = previous.NOW
FAMILY = previous.FAMILY


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    with dashboard._PAIR_SUMMARY_CACHE_LOCK:
        dashboard._PAIR_SUMMARY_CACHE.clear()

    def forbidden(*args, **kwargs):
        raise AssertionError('Dashboard must not open a ledger or a network connection')

    monkeypatch.setattr(sqlite3, 'connect', forbidden)
    monkeypatch.setattr(socket.socket, 'connect', forbidden)


@pytest.fixture
def publication(tmp_path):
    legacy = previous.publication.__wrapped__(tmp_path)
    root = legacy[0]
    project = root.parent.parent
    registry, summary, heartbeat = copy.deepcopy(legacy[1:4])
    raw = b'# registered joint display fixture\n'
    bindings = {name: hashlib.sha256(raw).hexdigest() for name in dashboard._JOINT_NEWS_V3_FORECAST_SOURCE_BINDINGS}
    for name in bindings:
        (project / name).write_bytes(raw)
    registry.update(schema_version='joint_price_news_registry_v3_20260908',
                    registry_id='joint_price_news_study_v3_20260908', source_bindings=bindings)
    for pair, entry in registry['pairs'].items():
        registered = entry['families'][FAMILY]
        contract = registered['contract']
        cohort = f'joint_price_news_v1_20260907.{pair}.{FAMILY}.news_repair_v3_20260908.prospective'
        contract.update(schema_version='causal_joint_price_news_ledger_v2_20260908',
                        source_bindings=bindings, cohorts={FAMILY: cohort})
        registered['contract_sha256'] = baseline.digest(contract)
        slot = next(row for row in summary['rows'] if row['instrument'] == pair)['families'][FAMILY]
        slot.update(contract_sha256=registered['contract_sha256'], cohort_id=cohort)
        if slot['latest_forecast']:
            for arm in slot['latest_forecast']['forecasts']:
                arm['cohort_id'] = cohort
    summary['schema_version'] = 'joint_price_news_forecast_summary_v3_20260908'
    heartbeat['schema_version'] = 'joint_price_news_forecast_heartbeat_v3_20260908'
    selector = {'schema_version': 'joint_forecast_primary_selection_v1_20260908',
                'selected': 'v3', 'activated_epoch': NOW - 400, 'research_only': True,
                'can_place_orders': False, 'can_promote': False, 'can_authorize': False}

    def save(*, pointer=True):
        summary['registry_sha256'] = heartbeat['registry_sha256'] = baseline.digest(registry)
        summary['payload_sha256'] = baseline.digest({k: v for k, v in summary.items() if k != 'payload_sha256'})
        heartbeat['summary_sha256'] = baseline.digest(summary)
        selector['registry_sha256'] = baseline.digest(registry)
        for path, value in (
            (project / 'config/joint_price_news_study_v3_20260908.json', registry),
            (root / 'joint_price_news_study_v3/summary.json', summary),
            (root / 'joint_price_news_study_v3/heartbeat.json', heartbeat),
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value), encoding='utf-8')
        if pointer:
            write_selector()

    def write_selector():
        (project / 'config/joint_forecast_primary_current.json').write_text(json.dumps(selector), encoding='utf-8')

    save()
    return root, registry, summary, heartbeat, selector, save, write_selector


def read(publication):
    return dashboard.summarize_joint_price_news_forecasts(publication[0], now_epoch=NOW)


def test_selected_v3_preserves_original_forecast_clocks_and_comparisons(publication):
    value = read(publication)
    assert value['status'] == 'current', value
    assert value['study_version'] == 'joint_v3'
    assert value['counts'] == {'forecast': 2, 'unavailable': 1}
    assert value['primary_selection']['selected'] == 'v3'
    arm = value['rows'][0]['active_forecasts'][0]
    assert arm['target_epoch'] == NOW + 3500
    assert arm['news_first_observed_epoch'] == NOW - 250
    assert arm['input_contributions']['news_ablation_difference_pips'] == .1
    assert '.news_repair_v3_20260908.' in arm['cohort_id']
    projected = dashboard.project_joint_collection_status({}, value, now_epoch=NOW)
    assert projected['running'] is True
    assert projected['selected_study'] == 'joint_price_news_v3'
    assert projected['model_attempts']['scope'] == 'joint_price_news_v3_ledgers_only'
    assert projected['forecasting']['pairs_with_forecast'] == 2


def test_no_pointer_keeps_old_study_primary_even_if_v3_files_exist(publication):
    (publication[0].parent.parent / 'config/joint_forecast_primary_current.json').unlink()
    value = read(publication)
    assert value['status'] == 'current'
    assert value['study_version'] == 'joint_v2'


@pytest.mark.parametrize('field, value', [
    ('schema_version', 'wrong'), ('selected', 'v2'), ('activated_epoch', NOW + 1),
    ('activated_epoch', True), ('activated_epoch', float('nan')),
    ('research_only', False), ('can_place_orders', True), ('can_promote', 'false'),
    ('can_authorize', None), ('registry_sha256', 'a' * 64),
])
def test_invalid_explicit_selection_withholds_instead_of_falling_back(publication, field, value):
    publication[4][field] = value
    publication[6]()
    actual = read(publication)
    assert actual['status'] == 'unavailable'
    assert actual['study_version'] == 'joint_v3'
    assert actual['reason'] == 'invalid_joint_primary_selection'
    assert actual['rows'] == []


def test_selected_missing_registry_does_not_fall_back_to_old_study(publication):
    (publication[0].parent.parent / 'config/joint_price_news_study_v3_20260908.json').unlink()
    assert read(publication)['reason'] == 'invalid_joint_primary_selection'


def test_repaired_producer_source_tamper_withholds_new_study(publication):
    path = publication[0].parent.parent / 'oanda_news_topic_identity_reconciliation_v1.py'
    path.write_text('changed', encoding='utf-8')
    actual = read(publication)
    assert actual['status'] == 'unavailable'
    assert actual['reason'] == 'pair_registry_source_binding_mismatch'


def test_old_ledger_schema_is_not_accepted_for_v3(publication):
    contract = next(iter(publication[1]['pairs'].values()))['families'][FAMILY]['contract']
    contract['schema_version'] = 'causal_joint_price_news_ledger_v1_20260907'
    publication[5]()
    assert read(publication)['status'] == 'unavailable'


def test_renderer_displays_v3_only_with_its_verified_selection(publication):
    joint = read(publication)
    data = {'joint_price_news_forecasts': joint,
            'market_overview': {'generated_epoch': NOW, 'rows': []}}
    html = baseline.render(data)['html']
    assert 'Combined: Up' in html
    assert 'not verified accuracy' in html
    joint.pop('primary_selection')
    assert 'Combined: Up' not in baseline.render(data)['html']
    projected = dashboard.project_joint_collection_status({}, joint, now_epoch=NOW)
    assert projected['running'] is False


def test_new_study_bound_source_set_contains_complete_producer_dependencies():
    from oanda_joint_price_news_forecast_study_v3 import REQUIRED_SOURCE_BINDINGS
    assert dashboard._JOINT_NEWS_V3_FORECAST_SOURCE_BINDINGS == REQUIRED_SOURCE_BINDINGS
