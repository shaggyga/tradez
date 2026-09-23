"""New operational import chain; unchanged numerical and native target rules."""
import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

import revision_joint_point_v2 as old_point
import revision_joint_point_v3 as point
import shared_revision_history_v3 as old_history
import shared_revision_history_v4 as history
import revision_joint_inputs_v3 as old_inputs
import revision_joint_inputs_v4 as inputs
import oanda_causal_forecast_ledger_joint_news_v6 as ledger
import oanda_joint_price_news_forecast_study_v8 as study
from test_projection_revision_consumer_v2 import owned, fixture
from test_revision_news_incremental_v2 import configurations

ROOT = Path(__file__).absolute().parent
CONFIG = ROOT/'config/joint_price_news_operational_v5_20260916.json'
OLD_CONFIG = ROOT/'config/joint_price_news_operational_v4_20260913.json'


def functions(name):
    tree = ast.parse((ROOT/name).read_text(encoding='utf-8'))
    return {node.name: ast.dump(node, include_attributes=False)
            for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))}


@pytest.mark.parametrize('previous,current,excluded', [
    ('revision_joint_point_v2.py','revision_joint_point_v3.py',()),
    ('shared_revision_history_v3.py','shared_revision_history_v4.py',()),
    ('revision_joint_inputs_v3.py','revision_joint_inputs_v4.py',()),
    ('oanda_causal_forecast_ledger_joint_news_v5.py','oanda_causal_forecast_ledger_joint_news_v6.py',('_bound_inputs',)),
    ('oanda_joint_price_news_forecast_study_v7.py','oanda_joint_price_news_forecast_study_v8.py',
     ('load_registry','activate_fresh_study','verify_activated_study','resolve_runtime_paths')),
])
def test_all_numerical_causal_and_settlement_function_bodies_unchanged(previous,current,excluded):
    old, new = functions(previous), functions(current)
    assert old.keys() == new.keys()
    for name in old.keys()-set(excluded):
        assert old[name] == new[name], name


def test_actual_registry_and_ledger_import_chain_resolve_new_io():
    registry = study.load_registry(CONFIG)
    assert len(registry['pairs']) == 68
    assert study.CausalForecastLedger is ledger.CausalForecastLedger
    assert study._bound_inputs() is inputs
    assert inputs.point_contract is point and inputs.history is history
    assert point.news_io is inputs.news_io is history.news_io
    assert inputs.news_io.__name__ == 'revision_news_io_v11'
    assert inputs.news_io.transport.__name__ == 'revision_transport_v5'
    assert inputs.news_io.incremental.__name__ == 'projection_revision_consumer_v2'
    assert set(inputs._bindings()) <= study.REQUIRED_SOURCE_BINDINGS
    assert registry['news_io_config_sha256'] == hashlib.sha256(
        (ROOT/'config/revision_news_io_operational_v6_20260916.json').read_bytes()).hexdigest()


def test_only_identity_and_source_contract_fields_change_for_all_pairs():
    previous = json.loads(OLD_CONFIG.read_bytes())
    current = json.loads(CONFIG.read_bytes())
    assert current['pairs'].keys() == previous['pairs'].keys()
    assert current['dependency_versions'] == previous['dependency_versions']
    identity_fields = {'schema_version','contract_id','cohorts','source_bindings','native_source_bindings','feature_version'}
    for pair in current['pairs']:
        old = deepcopy(previous['pairs'][pair]['families']['ridge_price_news_v1']['contract'])
        new = deepcopy(current['pairs'][pair]['families']['ridge_price_news_v1']['contract'])
        old_eval, new_eval = old.pop('evaluation_protocol'), new.pop('evaluation_protocol')
        for key in identity_fields:
            old.pop(key);new.pop(key)
        for key in ('contract_id','cohorts','feature_version'):
            old_eval.pop(key);new_eval.pop(key)
        assert old == new
        assert old_eval == new_eval
    assert inputs.BOUND_NUMERIC_SHA == old_inputs.BOUND_NUMERIC_SHA
    assert inputs._model() is old_inputs._model()
    assert inputs.TRAINING_POLICY == old_inputs.TRAINING_POLICY
    assert inputs.MAX_NEWS_AGE_SEC == 300 and inputs.MAX_PRICE_AGE_SEC == 900


def test_new_ledger_restarts_with_original_activation_and_refuses_old_contract(tmp_path):
    contract = json.loads(CONFIG.read_bytes())['pairs']['EUR_USD']['families']['ridge_price_news_v1']['contract']
    old = json.loads(OLD_CONFIG.read_bytes())['pairs']['EUR_USD']['families']['ridge_price_news_v1']['contract']
    refused = tmp_path/'old-contract.sqlite'
    with pytest.raises(ValueError, match='research_contract_required'):
        ledger.CausalForecastLedger(refused,old,clock=lambda:fixture.epoch(3),activate=True)
    assert not refused.exists()
    path = tmp_path/'new-cohort.sqlite'
    first = ledger.CausalForecastLedger(path,contract,clock=lambda:fixture.epoch(3),activate=True)
    activation = first.activated_epoch
    assert not any(first.counts().values())
    first.close()
    second = ledger.CausalForecastLedger(path,contract,clock=lambda:fixture.epoch(4))
    assert second.activated_epoch == activation
    assert second.contract_hash == study.digest(contract)
    assert second.settle() == 0
    second.close()


def test_new_capture_point_and_history_use_original_values_and_clocks(owned,tmp_path):
    io = inputs.news_io
    config = configurations(owned,tmp_path)
    runner = io.transport.open_runner(config['transport_config_path'],config['transport_config_sha256'])
    session = None
    try:
        assert io.transport.run_cycle(runner,clock_provider=fixture.proof_clock(2.5))['status'] == 'ready'
        fixture.health(config,2.6)
        session = io.create_session(config)
        io.bootstrap_inputs(session,clock=lambda:fixture.epoch(2.61))
        capture = io.capture_shared(session,clock=lambda:fixture.epoch(2.62))
        decision = fixture.epoch(2.63)
        value = io.current_pair_features(session,capture,'EUR_USD',decision)
        current = point._point(value,decision,value['context_sha256'])
        assert current == old_point._point(value,decision,value['context_sha256'])
        assert current['features'] == value['features']
        assert current['consumer_observed_epoch'] <= decision
        origins = history.universal_origins(fixture.epoch(2.62))[-2:]
        assert origins == old_history.universal_origins(fixture.epoch(2.62))[-2:]
        shared = history.prepare_history_share(session,capture,{'EUR_USD':origins})
        projected = history.project_pair_history(shared,session=session,news_capture=capture,instrument='EUR_USD',origins=origins)
        assert projected['origins'] == list(origins)
        assert projected['price_or_label_maturity_evaluated'] is False
        assert projected['fresh_health_proven'] is False
        for row in projected['records']:
            if row['status'] == 'unavailable':
                assert row['point']['features'] is None
            assert row['point']['decision_epoch'] == row['origin']+60
        replay = io.replay_capture(session,io.capture_metadata(capture)['descriptor'])
        with pytest.raises(ValueError,match='same_history_session_and_capture_required'):
            history.project_pair_history(shared,session=session,news_capture=replay,instrument='EUR_USD',origins=origins)
    finally:
        if session is not None:io.close_session(session)
        io.transport.close_runner(runner)
