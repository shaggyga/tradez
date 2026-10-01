import hashlib
import json
import pytest
from test_oanda_operational_recovery_v6 import fixture, validate, invoke, HELPER, ROOT


def prepared(tmp_path):
    project, path, value = fixture(tmp_path)
    value.update(enable_rolling_news=True, enable_joint_forecasts=True)
    value['services'] = [r for r in value['services'] if r['name'] != 'revision_news_transport_v6']
    row = next(r for r in value['services'] if r['name'] == 'joint_price_news_isolation_status_v1')
    row.update(name='joint_price_news_study_v11', script='oanda_joint_price_news_forecast_study_v11.py',
               needle='oanda_joint_price_news_forecast_study_v11.py')
    body = b'# inert rolling fixture\n'; (project/row['script']).write_bytes(body)
    row['source_sha256'] = hashlib.sha256(body).hexdigest()
    path.write_text(json.dumps(value))
    return project, path, value


def test_explicit_rolling_selection_replaces_transport_and_preserves_restart_history(tmp_path):
    project, path, value = prepared(tmp_path)
    result = validate(tmp_path, project, path)
    assert result.returncode == 0, result.stderr
    assert len(value['services']) == 17
    result = invoke(tmp_path, f". '{HELPER}'\nGet-OperationalRestartBudgetKey joint_price_news_study_v11")
    assert result.returncode == 0 and result.stdout.strip() == 'joint_price_news_study_v8'


def test_scheduler_overlay_requires_explicit_selection_and_keeps_role(tmp_path):
    project, path, value = prepared(tmp_path)
    row = next(r for r in value['services'] if r['name'] == 'joint_price_news_study_v11')
    row.update(script='oanda_joint_news_scheduler_v1.py', needle='oanda_joint_news_scheduler_v1.py')
    body = b'# inert operational scheduler fixture\n'
    (project / row['script']).write_bytes(body)
    row['source_sha256'] = hashlib.sha256(body).hexdigest()
    path.write_text(json.dumps(value))
    assert validate(tmp_path, project, path).returncode != 0
    value['enable_joint_scheduler'] = True
    path.write_text(json.dumps(value))
    result = validate(tmp_path, project, path)
    assert result.returncode == 0, result.stderr
    value['enable_joint_scheduler'] = 'true'
    path.write_text(json.dumps(value))
    assert validate(tmp_path, project, path).returncode != 0


def test_progress_wrapper_requires_explicit_selection(tmp_path):
    project, path, value = prepared(tmp_path)
    row = next(r for r in value['services'] if r['name'] == 'default_news_collector_v1')
    row.update(script='oanda_news_collector_progress_v1.py', needle='oanda_news_collector_progress_v1.py')
    body = b'# inert collector progress fixture\n'
    (project / row['script']).write_bytes(body)
    row['source_sha256'] = hashlib.sha256(body).hexdigest()
    path.write_text(json.dumps(value))
    assert validate(tmp_path, project, path).returncode != 0
    value['enable_collector_progress'] = True
    path.write_text(json.dumps(value))
    result = validate(tmp_path, project, path)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('defect', ['string', 'unselected', 'missing_consumer', 'source'])
def test_bad_rolling_runtime_is_refused(tmp_path, defect):
    project, path, value = prepared(tmp_path)
    if defect == 'string': value['enable_rolling_news'] = 'true'
    if defect == 'unselected': value['enable_rolling_news'] = False
    if defect == 'missing_consumer': value['enable_joint_forecasts'] = False
    if defect == 'source': (project/'oanda_joint_price_news_forecast_study_v11.py').write_bytes(b'changed')
    path.write_text(json.dumps(value))
    assert validate(tmp_path, project, path).returncode != 0


def test_actual_loader_preserves_retired_transport_and_current_joint_circuit(tmp_path):
    source = (ROOT/'oanda_operational_supervisor_v6.ps1').read_text()
    loader = source[source.index('if (Test-Path -LiteralPath $RestartLedgerPath) {'):source.index('\nfunction Write-SupervisorEvent')]
    ledger = tmp_path/'restart.json'
    saved = {'revision_news_transport_v5': [990., 991., 992.],
             'joint_price_news_study_v8': [993., 994., 995.]}
    ledger.write_text(json.dumps(saved))
    code = f""". '{HELPER}'
$RestartLedgerPath='{ledger}';$opNames=@('joint_price_news_study_v11');$script:RestartLedger=@{{}}
{loader}
$budget=Get-OperationalRestartDecision -Attempts @($script:RestartLedger['joint_price_news_study_v8']) -NowEpoch 1000
@{{retained=$script:RestartLedger;allowed=$budget.allowed}}|ConvertTo-Json -Depth 8 -Compress
"""
    result = invoke(tmp_path, code)
    assert result.returncode == 0, result.stderr
    got = json.loads(result.stdout)
    assert got == {'retained': saved, 'allowed': False}
    ledger.write_text(json.dumps({**saved, 'unknown': [990.]}))
    assert invoke(tmp_path, code).returncode != 0


def test_exact_technical_capacity_successor_selection(tmp_path):
    project, path, value = prepared(tmp_path)
    value['enable_technical_capacity'] = True
    row = next(r for r in value['services'] if r['name'] == 'all68_technical_availability_v1')
    row.update(name='all68_technical_availability_v2', script='oanda_all68_technical_availability_v2.py',
               needle='oanda_all68_technical_availability_v2.py')
    body = b'# inert exact capacity reader fixture\n'; (project/row['script']).write_bytes(body)
    row['source_sha256'] = hashlib.sha256(body).hexdigest(); path.write_text(json.dumps(value))
    result = validate(tmp_path, project, path)
    assert result.returncode == 0, result.stderr
    result = invoke(tmp_path, f". '{HELPER}'\nGet-OperationalRestartBudgetKey all68_technical_availability_v2")
    assert result.stdout.strip() == 'all68_technical_availability_v1'
    value['enable_technical_capacity'] = 'true'; path.write_text(json.dumps(value))
    assert validate(tmp_path, project, path).returncode != 0
