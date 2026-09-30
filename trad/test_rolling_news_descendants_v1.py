import ast
import copy
import json
from pathlib import Path
import pytest
import oanda_causal_forecast_ledger_joint_news_v9 as ledger
import oanda_joint_price_news_forecast_study_v11 as study
import revision_joint_inputs_v7 as inputs

ROOT = Path(__file__).parent
CONFIG = ROOT/'config/joint_price_news_rolling_v1_20260930.json'
OLD = ROOT/'config/joint_price_news_capacity_v1_20260930.json'


def test_native_ledger_and_forecast_math_preserved():
    old = ast.parse((ROOT/'oanda_causal_forecast_ledger_joint_news_v8.py').read_text())
    new = ast.parse((ROOT/'oanda_causal_forecast_ledger_joint_news_v9.py').read_text())
    old_funcs = {n.name: ast.dump(n) for n in old.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
    new_funcs = {n.name: ast.dump(n) for n in new.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
    for name in old_funcs:
        if name != '_bound_inputs': assert new_funcs[name] == old_funcs[name], name
    assert inputs.BOUND_NUMERIC_SHA == 'eb153acb966dc04ad950a0bbfcc730e78a9a0da1d8a24e91d641f473f5cfbc23'


def test_all68_contracts_keep_original_gates_and_distinct_identity():
    old = json.loads(OLD.read_bytes()); new = study.load_registry(CONFIG)
    assert len(new['pairs']) == 68 and new['pairs'].keys() == old['pairs'].keys()
    assert old['dependency_versions'] == new['dependency_versions']
    fields = {'schema_version', 'contract_id', 'cohorts', 'source_bindings', 'native_source_bindings', 'feature_version'}
    for pair in new['pairs']:
        a = copy.deepcopy(old['pairs'][pair]['families']['ridge_price_news_v1']['contract'])
        b = copy.deepcopy(new['pairs'][pair]['families']['ridge_price_news_v1']['contract'])
        assert a['cohorts'] != b['cohorts']
        ae = a.pop('evaluation_protocol'); be = b.pop('evaluation_protocol')
        for k in fields: a.pop(k); b.pop(k)
        for k in ('contract_id', 'cohorts', 'feature_version'): ae.pop(k); be.pop(k)
        assert a == b and ae == be


def test_actual_new_ledger_restart_preserves_activation_and_refuses_old_contract(tmp_path):
    new = study.load_registry(CONFIG)['pairs']['EUR_USD']['families']['ridge_price_news_v1']['contract']
    old = json.loads(OLD.read_bytes())['pairs']['EUR_USD']['families']['ridge_price_news_v1']['contract']
    with pytest.raises(ValueError, match='research_contract_required'):
        ledger.CausalForecastLedger(tmp_path/'refused.sqlite', old, clock=lambda: 1790784000., activate=True)
    assert not (tmp_path/'refused.sqlite').exists()
    path = tmp_path/'rolling.sqlite'
    first = ledger.CausalForecastLedger(path, new, clock=lambda: 1790784000., activate=True)
    activated = first.activated_epoch
    assert not any(first.counts().values()); first.close()
    second = ledger.CausalForecastLedger(path, new, clock=lambda: 1790784001.)
    assert second.activated_epoch == activated
    assert second.contract_hash == study.digest(new)
    second.close()
