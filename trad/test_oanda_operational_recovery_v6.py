"""Pure successor contract and simulated supervisor checks; no real services."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import re

import pytest

ROOT = Path(__file__).resolve().parent
HELPER = ROOT / 'oanda_operational_recovery_contract_v6.ps1'
PS = Path(os.environ.get('SystemRoot', 'C:/Windows')) / 'System32/WindowsPowerShell/v1.0/powershell.exe'
SCRIPTS = ['oanda_operational_recovery_contract_v6.ps1', 'oanda_operational_supervisor_v6.ps1',
           'oanda_supervisor_watchdog_v6.ps1', 'start_oanda_operational_research_v6.ps1',
           'start_oanda_supervisor_watchdog_v6.ps1', 'oanda_operational_log_relay_v2.py']
ROLES = dict(all68_m1_cadence_v3='oanda_all68_m1_cadence_v3.py',
             all68_technical_availability_v1='oanda_all68_technical_availability_v1.py',
             practice_quote_stream_v1='oanda_practice_quote_stream.py',
             clock_integrity_monitor_v1='oanda_clock_integrity_monitor.py',
             rolling_technical_operations_v2='oanda_rolling_technical_worker_v2.py',
             research_feature_forward_cached_v2='oanda_feature_forward_worker_v2.py',
             default_news_collector_v1='oanda_local_news_sentiment.py',
             revision_news_collector_v2='oanda_local_news_sentiment.py',
             local_news_sentiment_repair_v2='oanda_local_news_sentiment_repair_v2.py',
             revision_news_transport_v6='revision_transport_v6.py',
             joint_price_news_isolation_status_v1='oanda_joint_price_news_isolation_status_v1.py',
             pair_local_forecast_study_v3='oanda_pair_local_forecast_study_v3.py',
             retained_price_settlement_v1='oanda_retained_price_settlement_v1.py',
             native_feature_candles_v1='oanda_native_feature_candle_updater_v1.py',
             research_feature_observations_v2='oanda_research_feature_observation_worker_v2.py',
             research_feature_forward_v2='oanda_feature_forward_worker_v1.py',
             official_pair_horizon_v2='oanda_official_event_pair_horizon_capture_v2.py',
             all68_derived_technical_publisher_v1='oanda_derived_technical_publisher_v1.py')


@pytest.mark.parametrize('priority,expected', [(None, 'BelowNormal'), ('BelowNormal', 'BelowNormal'), ('Normal', 'Normal')])
def test_role_priority_is_explicit_bounded_and_defaults_below_normal(tmp_path, priority, expected):
    project, path, value = fixture(tmp_path)
    if priority is not None:
        value['services'][0]['priority_class'] = priority
    path.write_text(json.dumps(value), encoding='utf-8')
    assert validate(tmp_path, project, path).returncode == 0
    service = json.dumps(value['services'][0]).replace("'", "''")
    result = invoke(tmp_path, f". '{HELPER}'\n$s='{service}'|ConvertFrom-Json\nGet-OperationalRolePriority $s")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected


@pytest.mark.parametrize('priority', ['High', 'Realtime', 'normal', '', 1, True])
def test_invalid_or_elevated_role_priority_cannot_start(tmp_path, priority):
    project, path, value = fixture(tmp_path)
    value['services'][0]['priority_class'] = priority
    path.write_text(json.dumps(value), encoding='utf-8')
    result = validate(tmp_path, project, path)
    assert result.returncode != 0 and 'Normal or BelowNormal' in result.stderr


def test_v2_v5_owners_are_conflicts_and_successor_retry_budgets_keep_original_keys(tmp_path):
    code = f""". '{HELPER}'
$p='{ROOT / 'config/fixture.json'}'
$v2='powershell -File {ROOT / 'oanda_operational_supervisor_v2.ps1'} -SafeCoreOnly -ResearchCollectionOnly -OperationalProfilePath '+$p
$v5='powershell -File {ROOT / 'oanda_operational_supervisor_v5.ps1'} -SafeCoreOnly -ResearchCollectionOnly -OperationalProfilePath '+$p
$w2='powershell -File {ROOT / 'oanda_supervisor_watchdog_v2.ps1'}'
$w5='powershell -File {ROOT / 'oanda_supervisor_watchdog_v5.ps1'}'
@{{seen=(Test-OperationalOwnerCommand $v2 '{ROOT}' supervisor);v5_seen=(Test-OperationalOwnerCommand $v5 '{ROOT}' supervisor);adopted=(Test-OperationalSupervisorCommand $v2 $p);v5_adopted=(Test-OperationalSupervisorCommand $v5 $p);watchdog_seen=(Test-OperationalOwnerCommand $w2 '{ROOT}' watchdog);watchdog_v5_seen=(Test-OperationalOwnerCommand $w5 '{ROOT}' watchdog);transport_key=(Get-OperationalRestartBudgetKey revision_news_transport_v6);joint_key=(Get-OperationalRestartBudgetKey joint_price_news_study_v9);same_key=(Get-OperationalRestartBudgetKey rolling_technical_operations_v2)}}|ConvertTo-Json -Compress
"""
    result = invoke(tmp_path, code)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(seen=True, v5_seen=True, adopted=False, v5_adopted=False, watchdog_seen=True, watchdog_v5_seen=True,
        transport_key='revision_news_transport_v5', joint_key='joint_price_news_study_v8', same_key='rolling_technical_operations_v2')
    text = (ROOT / 'oanda_operational_supervisor_v6.ps1').read_text()
    assert "'operational_supervisor_restart_v2.json'" in text
    assert '$script:RestartLedger[$budgetKey]' in text
    watchdog = (ROOT / 'oanda_supervisor_watchdog_v6.ps1').read_text()
    assert "'operational_watchdog_restarts_v2.json'" in watchdog


def test_unmodified_pure_helpers_have_exact_v2_behavioral_source_parity():
    old = (ROOT / 'oanda_operational_recovery_contract_v2.ps1').read_text()
    new = HELPER.read_text()
    def functions(text):
        return {m.group(1): m.group(0).strip() for m in re.finditer(
            r'(?ms)^function ([\w-]+).*?(?=^function |\Z)', text)}
    oldf, newf = functions(old), functions(new)
    changed = {'Get-OperationalRecoveryServices', 'Read-OperationalRecoveryProfile',
               'Get-OperationalResearchSupervisorArguments', 'Test-OperationalSupervisorCommand',
               'Test-OperationalOwnerCommand', 'Get-OperationalInterpreterArguments',
               'Test-OperationalSupervisorIdentity'}
    for name in oldf.keys() - changed:
        assert newf[name] == oldf[name], name
    assert newf.keys() - oldf.keys() == {'Get-OperationalRolePriority', 'Get-OperationalRestartBudgetKey', 'Test-OperationalRelayCommand', 'Clear-OperationalOptionalNewsCredentials'}


@pytest.mark.parametrize('version', [1, 2])
def test_old_and_new_relays_are_excluded_even_before_actual_child_exists(tmp_path, version):
    source = (ROOT / 'oanda_operational_supervisor_v6.ps1').read_text()
    fn = source[source.index('function Get-MatchingPython {'):source.index('\nfunction Stop-MatchingPython {')]
    relay_path = ROOT / f'oanda_operational_log_relay_v{version}.py'
    child = ROOT / 'worker.py'
    code = f""". '{HELPER}'
{fn}
$Trad='{ROOT}';$Root='{ROOT.parent}'
$script:MatchingPythonProcessSnapshot=@([pscustomobject]@{{Name='python.exe';ProcessId=123;ParentProcessId=1;CommandLine='python -B "{relay_path}" --log-directory logs -- python -B "{child}" --duration-sec 604800'}})
function Get-Process {{ [pscustomobject]@{{HasExited=$false}} }}
@{{relay=(Test-OperationalRelayCommand $script:MatchingPythonProcessSnapshot[0].CommandLine $Trad);count=@(Get-MatchingPython worker.py).Count;worker=(Test-OperationalRelayCommand 'python -B "{child}" --duration-sec 604800' $Trad)}}|ConvertTo-Json -Compress
"""
    result = invoke(tmp_path, code)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(relay=True, count=0, worker=False)


def test_native_ps5_retained_role_keys_load_and_circuit_survives_actual_json_roundtrip(tmp_path):
    source = (ROOT / 'oanda_operational_supervisor_v6.ps1').read_text()
    loader = source[source.index('if (Test-Path -LiteralPath $RestartLedgerPath) {'):source.index('\nfunction Write-SupervisorEvent')]
    ledger = tmp_path / 'restart.json'
    original = {'revision_news_transport_v5': [990.25, 991.5], 'joint_price_news_study_v8': [992.0, 993.0]}
    ledger.write_text(json.dumps(original))
    code = f""". '{HELPER}'
$RestartLedgerPath='{ledger}'
$opNames=@('revision_news_transport_v6','joint_price_news_study_v9')
$script:RestartLedger=@{{}}
{loader}
$before=$script:RestartLedger|ConvertTo-Json -Compress
$key=Get-OperationalRestartBudgetKey revision_news_transport_v6
$budget=Get-OperationalRestartDecision -Attempts @($script:RestartLedger[$key]) -NowEpoch 1000
$script:RestartLedger[$key]=@($budget.attempts)+@(1000.0)
Write-OperationalAtomicJson $RestartLedgerPath $script:RestartLedger
$script:RestartLedger=@{{}}
{loader}
$after=Get-OperationalRestartDecision -Attempts @($script:RestartLedger[$key]) -NowEpoch 1001
@{{before=($before|ConvertFrom-Json);after=$script:RestartLedger;allowed=$after.allowed;reason=$after.reason;version=$PSVersionTable.PSVersion.Major}}|ConvertTo-Json -Depth 8 -Compress
"""
    result = invoke(tmp_path, code)
    assert result.returncode == 0, result.stderr
    got = json.loads(result.stdout)
    assert got['version'] == 5
    assert got['before'] == original
    assert got['after'] == {**original, 'revision_news_transport_v5': [990.25, 991.5, 1000.0]}
    assert got['allowed'] is False and got['reason'] == 'restart_circuit_open'


@pytest.mark.parametrize('role', ['joint_price_news_study_v9', 'joint_price_news_isolation_status_v1'])
def test_joint_selection_preserves_both_restart_histories(tmp_path, role):
    source = (ROOT / 'oanda_operational_supervisor_v6.ps1').read_text()
    loader = source[source.index('if (Test-Path -LiteralPath $RestartLedgerPath) {'):source.index('\nfunction Write-SupervisorEvent')]
    ledger = tmp_path / 'restart.json'
    original = {'joint_price_news_study_v8': [990., 991., 992.],
                'joint_price_news_isolation_status_v1': [993., 994., 995.]}
    ledger.write_text(json.dumps(original))
    code = f""". '{HELPER}'
$RestartLedgerPath='{ledger}';$opNames=@('{role}');$script:RestartLedger=@{{}}
{loader}
$key=Get-OperationalRestartBudgetKey '{role}'
$budget=Get-OperationalRestartDecision -Attempts @($script:RestartLedger[$key]) -NowEpoch 1000
@{{retained=$script:RestartLedger;allowed=$budget.allowed}}|ConvertTo-Json -Depth 8 -Compress
"""
    result = invoke(tmp_path, code)
    assert result.returncode == 0, result.stderr
    got = json.loads(result.stdout)
    assert got['retained'] == original
    assert got['allowed'] is False
    ledger.write_text(json.dumps({**original, 'unregistered_worker': [996.]}))
    result = invoke(tmp_path, code)
    assert result.returncode != 0 and 'Invalid bounded restart ledger' in result.stderr


@pytest.mark.parametrize('initial_count', [1, 2])
def test_native_ps5_watchdog_persistence_scalar_or_flat_array_then_next_cycle(tmp_path, initial_count):
    source = (ROOT / 'oanda_supervisor_watchdog_v6.ps1').read_text()
    recent_fn = source[source.index('function Get-RecentRestartCount {'):source.index('\nfunction Write-Incident')]
    start_fn = source[source.index('function Start-SafeCoreSupervisor {'):source.index('\ntry {', source.index('function Start-SafeCoreSupervisor {'))]
    ledger = tmp_path / 'watchdog.json'
    code = f""". '{HELPER}'
{recent_fn}
{start_fn}
$RestartLedger='{ledger}';$RestartWindowMinutes=30;$MaximumRestartsPerWindow=3
$now=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()/1000.0
$original=@(($now-20),($now-10))|Select-Object -First {initial_count}
Write-OperationalAtomicJson $RestartLedger @($original)
$before=Get-RecentRestartCount
$Root='{ROOT.parent}';$Trad='{ROOT}';$Launcher='{ROOT / 'start_oanda_operational_research_v6.ps1'}'
$OperationalProfilePath='';$OperationalBinding=$null;$RecoveryUntilUtc='2026-09-20T23:59:00Z';$Logs='{tmp_path}'
$script:starts=0
function Start-Process {{[CmdletBinding()]param($FilePath,$ArgumentList,$WorkingDirectory,$WindowStyle,$RedirectStandardOutput,$RedirectStandardError,[switch]$PassThru);$script:starts++;[pscustomobject]@{{Id=999}}}}
$null=Start-SafeCoreSupervisor
$after=Get-RecentRestartCount
$parsed=Get-Content -LiteralPath $RestartLedger -Raw|ConvertFrom-Json
@{{before=$before;after=$after;retained=@($parsed);original=@($original);starts=$script:starts;version=$PSVersionTable.PSVersion.Major}}|ConvertTo-Json -Depth 8 -Compress
"""
    result = invoke(tmp_path, code)
    assert result.returncode == 0, result.stderr
    got = json.loads(result.stdout)
    assert got['version'] == 5
    assert got['before'] == initial_count and got['after'] == initial_count + 1
    assert got['starts'] == 1
    assert got['retained'][:initial_count] == got['original']
    assert len(got['retained']) == initial_count + 1


def invoke(tmp_path, code, *args):
    script = tmp_path / 'pure.ps1'
    script.write_text("$ErrorActionPreference='Stop'\n" + code, encoding='utf-8')
    return subprocess.run([str(PS), '-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
                           '-File', str(script), *map(str, args)], capture_output=True, text=True, timeout=30)


def fixture(tmp_path):
    project = tmp_path / 'project with space' / 'trad'
    project.mkdir(parents=True)
    support = {}
    for name in SCRIPTS:
        raw = b'# inert fixture\n'
        (project / name).write_bytes(raw)
        support[name] = hashlib.sha256(raw).hexdigest()
    rows = []
    for name, script in ROLES.items():
        raw = f'# inert {script}\n'.encode()
        (project / script).write_bytes(raw)
        args = ['--duration-sec', '604800']
        needle = script
        if name == 'revision_news_collector_v2':
            needle += '*' + str(project / 'data/oanda_training_manager/market_open_20260913_v1/local_news_sentiment')
        if name == 'research_feature_forward_v2':
            directory = str(project / 'data/oanda_training_manager/forward')
            needle += '*' + directory
            args = ['--directory', directory]
        rows.append(dict(name=name, script=script, needle=needle, arguments=args,
                         source_sha256=hashlib.sha256(raw).hexdigest(), max_age_sec=120, startup_grace_sec=180,
                         heartbeat=str(project / f'data/{name}/status.json'), heartbeat_schema='fixture',
                         heartbeat_field='operations_schema' if name == 'rolling_technical_operations_v2' else 'schema_version',
                         interpreter_mode='cold_no_bytecode_unique_prefix' if name == 'revision_news_transport_v6' else 'no_bytecode'))
    value = dict(schema_version='forex_operational_runtime_v6_20260916', research_only=True, can_place_orders=False,
                 services=rows, support_sources=support, config_dependencies={}, recovery_until_utc='2026-09-20T23:59:00Z')
    path = project / 'config.json'
    path.write_text(json.dumps(value), encoding='utf-8')
    return project, path, value


def validate(tmp_path, project, path, suffix=''):
    code = f""". '{HELPER}'
$b=Read-OperationalRecoveryProfile -Trad '{project}' -ProfilePath '{path}' -RecoveryUntilUtc '2026-09-20T23:59:00Z' -Now ([DateTimeOffset]'2026-09-16T03:00:00Z') {suffix}
$b | ConvertTo-Json -Depth 8 -Compress
"""
    return invoke(tmp_path, code)


def test_exact_profile_has_eighteen_pinned_research_roles_and_external_rolling_data(tmp_path):
    project, path, _ = fixture(tmp_path)
    result = validate(tmp_path, project, path)
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert len(value['services']) == 18
    assert value['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert value['recovery_allowed'] is True


def test_v4_controllers_remain_conflicts_and_m1_retry_history_is_preserved(tmp_path):
    code = f""". '{HELPER}'
$p='{ROOT / 'config/fixture.json'}'
$old='powershell -File {ROOT / 'oanda_operational_supervisor_v4.ps1'} -SafeCoreOnly -ResearchCollectionOnly -OperationalProfilePath '+$p
$watch='powershell -File {ROOT / 'oanda_supervisor_watchdog_v4.ps1'}'
@{{seen=(Test-OperationalOwnerCommand $old '{ROOT}' supervisor);adopted=(Test-OperationalSupervisorCommand $old $p);watchdog_seen=(Test-OperationalOwnerCommand $watch '{ROOT}' watchdog);key=(Get-OperationalRestartBudgetKey all68_m1_cadence_v3)}}|ConvertTo-Json -Compress
"""
    result = invoke(tmp_path, code)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(seen=True, adopted=False,
        watchdog_seen=True, key='all68_m1_cadence_v2')


@pytest.mark.parametrize('change,expected', [
    ('orders', 'explicit research-only'), ('expiry', 'expiry differ'),
    ('source', 'worker source changed'), ('support', 'support source changed'),
    ('missing', 'eighteen distinct'), ('duplicate', 'eighteen distinct'),
    ('field', 'heartbeat identity field'), ('heartbeat', 'declared project data path'),
    ('age_boolean', 'Invalid bounded'), ('argument_newline', 'Invalid bounded'),
    ('interpreter', 'Exact operational interpreter'),
])
def test_invalid_contract_refused_without_any_launch(tmp_path, change, expected):
    project, path, value = fixture(tmp_path)
    if change == 'orders': value['can_place_orders'] = True
    if change == 'expiry': value['recovery_until_utc'] = '2026-09-21T00:00:00Z'
    if change == 'source': (project / value['services'][0]['script']).write_text('changed')
    if change == 'support': (project / SCRIPTS[0]).write_text('changed')
    if change == 'missing': value['services'].pop()
    if change == 'duplicate': value['services'][-1] = value['services'][0]
    if change == 'field': value['services'][0]['heartbeat_field'] = 'arbitrary'
    if change == 'heartbeat': value['services'][0]['heartbeat'] = str(tmp_path / 'outside.json')
    if change == 'age_boolean': value['services'][0]['max_age_sec'] = True
    if change == 'argument_newline': value['services'][0]['arguments'] = ['bad\nargument']
    if change == 'interpreter': value['services'][0]['interpreter_mode'] = 'arbitrary_python_options'
    path.write_text(json.dumps(value), encoding='utf-8')
    result = validate(tmp_path, project, path)
    assert result.returncode != 0
    assert expected in result.stderr


def test_expired_profile_only_allows_diagnostic_binding_not_recovery(tmp_path):
    project, path, _ = fixture(tmp_path)
    base = f". '{HELPER}'\nRead-OperationalRecoveryProfile -Trad '{project}' -ProfilePath '{path}' -RecoveryUntilUtc '2026-09-20T23:59:00Z' -Now ([DateTimeOffset]'2026-09-21T00:00:00Z')"
    denied = invoke(tmp_path, base)
    assert denied.returncode != 0 and 'expiry reached' in denied.stderr
    allowed = invoke(tmp_path, base + ' -AllowExpired | ConvertTo-Json -Depth 8 -Compress')
    assert allowed.returncode == 0, allowed.stderr
    assert json.loads(allowed.stdout)['recovery_allowed'] is False


def test_legacy_command_owner_recognized_but_not_adopted_as_v2(tmp_path):
    code = f""". '{HELPER}'
$p='{ROOT / 'config/fixture.json'}'
$legacy='powershell -Command & ''{ROOT / 'oanda_always_on_supervisor.ps1'}'' -SafeCoreOnly -ResearchCollectionOnly -OperationalProfilePath '+$p
$current='powershell -File {ROOT / 'oanda_operational_supervisor_v6.ps1'} -SafeCoreOnly -ResearchCollectionOnly -OperationalProfilePath '+$p
@{{legacy_seen=(Test-OperationalOwnerCommand $legacy '{ROOT}' supervisor);legacy_adopted=(Test-OperationalSupervisorCommand $legacy $p);current=(Test-OperationalSupervisorCommand $current $p);lookalike=(Test-OperationalOwnerCommand ($legacy.Replace('.ps1','.ps1.backup')) '{ROOT}' supervisor)}}|ConvertTo-Json -Compress
"""
    result = invoke(tmp_path, code)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(legacy_seen=True, legacy_adopted=False, current=True, lookalike=False)


def test_worker_adoption_requires_exact_arguments_and_no_extra_config(tmp_path):
    result = invoke(tmp_path, f""". '{HELPER}'
$script='C:\\project space\\trad\\worker.py'
$cmd='python -B "'+$script+'" --duration-sec 604800'
@{{same=(Test-OperationalWorkerArguments $cmd $script @('--duration-sec','604800'));different=(Test-OperationalWorkerArguments $cmd $script @('--duration-sec','30'));extra=(Test-OperationalWorkerArguments ($cmd+' --config other.json') $script @('--duration-sec','604800'))}}|ConvertTo-Json -Compress
""")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(same=True, different=False, extra=False)


def test_restart_circuit_survives_recent_attempts_and_clock_regression(tmp_path):
    result = invoke(tmp_path, f""". '{HELPER}'
@{{empty=(Get-OperationalRestartDecision @() 10000);three=(Get-OperationalRestartDecision @(9000,9500,9900) 10000);expired=(Get-OperationalRestartDecision @(1000,2000,9900) 10000);clock=(Get-OperationalRestartDecision @(10002) 10000)}}|ConvertTo-Json -Depth 5 -Compress
""")
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value['empty']['allowed'] and value['expired']['allowed']
    assert not value['three']['allowed'] and value['three']['reason'] == 'restart_circuit_open'
    assert value['clock']['reason'] == 'recovery_clock_regressed'


def test_rotated_event_budget_never_deletes_or_exceeds_bound(tmp_path):
    directory = tmp_path / 'logs'
    result = invoke(tmp_path, f""". '{HELPER}'
$results=@(1..12|ForEach-Object {{Write-OperationalBoundedEvent '{directory}' events @{{n=$_;text=('a'*60)}} 110 330}})
@{{results=$results;files=@(Get-ChildItem -LiteralPath '{directory}' -File|ForEach-Object {{@{{name=$_.Name;bytes=$_.Length}}}})}}|ConvertTo-Json -Depth 5 -Compress
""")
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert any(value['results']) and not all(value['results'])
    assert all(x['bytes'] <= 110 for x in value['files'])
    assert sum(x['bytes'] for x in value['files']) <= 330
    assert len(value['files']) >= 2


def test_heartbeat_pid_profile_and_observation_clock_bound(tmp_path):
    result = invoke(tmp_path, f""". '{HELPER}'
$h=@{{schema_version='operational_supervisor_v6_20260916';supervisor_pid=123;operational_profile_sha256='hash';generated_utc='2026-09-16T03:00:00Z'}}
$t=([DateTimeOffset]'2026-09-16T03:01:00Z').ToUnixTimeSeconds()
@{{good=(Test-OperationalSupervisorIdentity $h 123 hash $t);wrongpid=(Test-OperationalSupervisorIdentity $h 124 hash $t);wrongprofile=(Test-OperationalSupervisorIdentity $h 123 other $t);future=(Test-OperationalSupervisorIdentity $h 123 hash ($t-120));stale=(Test-OperationalSupervisorIdentity $h 123 hash ($t+300))}}|ConvertTo-Json -Compress
""")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(good=True, wrongpid=False, wrongprofile=False, future=False, stale=False)


def test_all_successors_parse_without_execution(tmp_path):
    names = ','.join("'" + str(ROOT / name) + "'" for name in SCRIPTS if name.endswith('.ps1'))
    result = invoke(tmp_path, f"""foreach($path in @({names})) {{
$tokens=$null;$errors=$null
$null=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tokens,[ref]$errors)
if($errors.Count){{throw ($errors|Out-String)}}
}}
'parsed'
""")
    assert result.returncode == 0, result.stderr
    assert 'parsed' in result.stdout


def test_watchdog_account_staleness_cannot_trigger_recovery():
    text = (ROOT / 'oanda_supervisor_watchdog_v6.ps1').read_text()
    assert 'account_aggregate_stale' not in text
    assert 'account_freshness_is_recovery_criterion = $false' in text
    assert 'Test-OperationalSupervisorIdentity' in text
    assert 'Local\\ForexSupervisorWatchdogV1_' in text  # Preserve single-owner mutex.


def test_profile_exclusive_supervisor_keeps_existing_mutex_and_no_legacy_role_body():
    text = (ROOT / 'oanda_operational_supervisor_v6.ps1').read_text()
    assert 'Global\\ForexOandaAlwaysOnSupervisorV1' in text
    assert '-Name "account_snapshot"' not in text
    assert 'conflicting_worker_arguments' in text
    assert 'Get-OperationalRestartDecision' in text
    assert 'source_sha256' not in text or 'Read-OperationalRecoveryProfile' in text


def simulated_owner(tmp_path, mode):
    project = tmp_path / 'project space'
    project.mkdir()
    worker = project / 'worker.py'
    worker.write_text('# inert')
    source = (ROOT / 'oanda_operational_supervisor_v6.ps1').read_text()
    function = source[source.index('function Start-ManagedProcess {'):source.index('\ntry {\n    Write-SupervisorEvent')]
    cmd = '"' + str(worker) + '" --duration-sec 604800'
    if mode == 'conflict':
        cmd += ' --config unreviewed.json'
    age = '-10' if mode == 'startup_grace' else '-600'
    existing = '$script:existing=@()' if mode == 'missing' else f"$script:existing=@([pscustomobject]@{{ProcessId=123;CommandLine='{cmd}';CreationDate=[DateTime]::Now.AddSeconds({age})}})"
    attempts = '@(($now-30),($now-20),($now-10))' if mode == 'circuit' else '@()'
    code = f""". '{HELPER}'
{function}
$OperationalProfile=@{{}};$opNames=@('fixture');$ResearchCollectionOnly=$true;$SafeCoreOnly=$true
$Root='{project}';$Trad='{project}';$Logs='{project}';$BoundedChildLogs='{project / 'bounded'}';$Python='{os.sys.executable}'
$RestartLedgerPath='{project / 'restart.json'}';$SupervisorStartedUtc=[DateTime]::UtcNow.AddSeconds(-600)
$script:RecoveryAllowed=${'false' if mode == 'expired' else 'true'}
$now=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()/1000.0
$script:RestartLedger=@{{fixture={attempts}}};$script:stops=0;$script:starts=0;$script:captured=@()
{existing}
function Get-MatchingPython {{ return $script:existing }}
function Test-FreshOutput {{ @{{fresh=${'true' if mode in ('healthy','conflict') else 'false'};reason='synthetic';path='fixture'}} }}
function Stop-MatchingPython {{ $script:stops++;$script:existing=@() }}
function Start-Sleep {{}}
function Write-SupervisorEvent {{}}
function Add-StartedProcessToMatchingSnapshot {{}}
function Start-Process {{
    [CmdletBinding()]param($FilePath,$ArgumentList,$WorkingDirectory,$WindowStyle,$RedirectStandardOutput,$RedirectStandardError,[switch]$PassThru)
    $script:starts++;$script:captured=@($ArgumentList)
    [pscustomobject]@{{Id=789;PriorityClass='Normal'}}
}}
$result=Start-ManagedProcess -Name fixture -Needle worker.py -Arguments @('{worker}','--duration-sec','604800') -InterpreterArguments @('-B') -Executable '{os.sys.executable}' -Freshness @{{MaxAgeSec=120;StartupGraceSec=180}}
@{{result=$result;starts=$script:starts;stops=$script:stops;captured=$script:captured;reserved=@($script:RestartLedger.fixture)}}|ConvertTo-Json -Depth 8 -Compress
"""
    result = invoke(tmp_path, code)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize('mode,reason', [('healthy', 'synthetic'), ('startup_grace', 'startup_grace'),
                                        ('conflict', 'conflicting_worker_arguments')])
def test_existing_exact_or_conflicting_child_never_blindly_restarted(tmp_path, mode, reason):
    value = simulated_owner(tmp_path, mode)
    assert value['starts'] == value['stops'] == 0
    assert value['result']['freshness']['reason'] == reason


@pytest.mark.parametrize('mode,reason', [('expired', 'recovery_expiry_reached'), ('circuit', 'restart_circuit_open')])
def test_expiry_and_persisted_circuit_preserve_stale_child(tmp_path, mode, reason):
    value = simulated_owner(tmp_path, mode)
    assert value['starts'] == value['stops'] == 0
    assert value['result']['freshness']['recovery_reason'] == reason


def test_missing_child_reserves_before_launch_through_quoted_bounded_relay(tmp_path):
    value = simulated_owner(tmp_path, 'missing')
    assert value['starts'] == 1 and value['stops'] == 0
    assert len(value['reserved']) == 1
    assert any('oanda_operational_log_relay_v2.py' in arg for arg in value['captured'])
    assert '--priority-class' in value['captured']
    assert any('worker.py' in arg and arg.startswith('"') for arg in value['captured'])
    assert (tmp_path / 'project space/restart.json').exists()


def test_relative_python_script_is_visible_as_conflict_not_adoption(tmp_path):
    result = invoke(tmp_path, f""". '{HELPER}'
@{{relative=(Test-OperationalRelativeWorkerCommand 'python -B oanda_rolling_technical_worker_v2.py --config config/a.json' 'oanda_rolling_technical_worker_v2.py');dot=(Test-OperationalRelativeWorkerCommand 'python .\\oanda_rolling_technical_worker_v2.py' 'oanda_rolling_technical_worker_v2.py');absolute=(Test-OperationalRelativeWorkerCommand 'python C:\\project\\oanda_rolling_technical_worker_v2.py' 'oanda_rolling_technical_worker_v2.py');other=(Test-OperationalRelativeWorkerCommand 'python -c print(x<2)' 'oanda_rolling_technical_worker_v2.py')}}|ConvertTo-Json -Compress
""")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(relative=True, dot=True, absolute=False, other=False)


def test_three_level_relay_virtualenv_actual_worker_is_one_leaf(tmp_path):
    source = (ROOT / 'oanda_operational_supervisor_v6.ps1').read_text()
    functions = source[source.index('function Get-MatchingPython {'):source.index('function Stop-MatchingPython {')]
    result = invoke(tmp_path, f""". '{HELPER}'
{functions}
$Root='C:\\project';$Trad='C:\\project\\trad'
function Get-Process {{param($Id,$ErrorAction);[pscustomobject]@{{HasExited=$false}}}}
$script:MatchingPythonProcessSnapshot=@(
    [pscustomobject]@{{Name='python.exe';ProcessId=101;ParentProcessId=99;CommandLine='python C:\\project\\trad\\oanda_operational_log_relay_v1.py -- python C:\\project\\trad\\worker.py --x 1'}},
    [pscustomobject]@{{Name='python.exe';ProcessId=202;ParentProcessId=101;CommandLine='python C:\\project\\trad\\worker.py --x 1'}},
    [pscustomobject]@{{Name='python.exe';ProcessId=303;ParentProcessId=202;CommandLine='python C:\\project\\trad\\worker.py --x 1'}},
    [pscustomobject]@{{Name='python.exe';ProcessId=404;ParentProcessId=98;CommandLine='python C:\\project\\trad\\worker.py --x 1'}}
)
@{{ids=@(Get-MatchingPython worker.py|ForEach-Object{{$_.ProcessId}})}}|ConvertTo-Json -Compress
""")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['ids'] == [303, 404]


@pytest.mark.parametrize('mode', ['normal', 'reused', 'stop_fails'])
def test_duplicate_cleanup_preserves_keeper_and_refuses_reused_pid(tmp_path, mode):
    reused = mode == 'reused'
    source = (ROOT / 'oanda_operational_supervisor_v6.ps1').read_text()
    functions = source[source.index('function Stop-MatchingPython {'):source.index('function Get-LatestMatchingFile {')]
    result = invoke(tmp_path, f"""{functions}
$old=[pscustomobject]@{{Name='python.exe';ProcessId=10;ParentProcessId=1;CreationDate=[DateTime]'2026-09-15T01:00:00';CommandLine='python C:\\project\\worker.py'}}
$keeper=[pscustomobject]@{{Name='python.exe';ProcessId=20;ParentProcessId=1;CreationDate=[DateTime]'2026-09-15T02:00:00';CommandLine='python C:\\project\\worker.py'}}
$replacement=[pscustomobject]@{{Name='python.exe';ProcessId=10;ParentProcessId=1;CreationDate=[DateTime]'2026-09-15T03:00:00';CommandLine='python C:\\project\\worker.py'}}
$script:current=@{{10=${'replacement' if reused else 'old'};20=$keeper}};$script:stopped=@()
function Get-CimInstance {{param($ClassName,$Filter,$ErrorAction);$script:current[[int]($Filter -replace '[^0-9]','')]}}
function Get-MatchingPython {{param($Needle);@($script:current.Values)}}
function Stop-Process {{param($Id,[switch]$Force,$ErrorAction);{'throw "synthetic stop failed";' if mode == 'stop_fails' else ''}$script:stopped+=$Id;$script:current.Remove([int]$Id)}}
function Write-SupervisorEvent {{}}
function Start-Sleep {{}}
$blocked=$false
try {{Stop-MatchingPython fixture worker.py @($old) duplicate}} catch {{$blocked=$true}}
@{{stopped=@($script:stopped);remaining=@($script:current.Keys|Sort-Object);blocked=$blocked}}|ConvertTo-Json -Compress
""")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == (dict(stopped=[], remaining=[10, 20], blocked=True) if mode != 'normal' else dict(stopped=[10], remaining=[20], blocked=False))


def test_watchdog_cim_failure_is_not_an_empty_inventory(tmp_path):
    source = (ROOT / 'oanda_supervisor_watchdog_v6.ps1').read_text()
    functions = source[source.index('function Get-Supervisors {'):source.index('function Get-RecentRestartCount {')]
    result = invoke(tmp_path, f""". '{HELPER}'
{functions}
$Supervisor='C:\\project\\oanda_operational_supervisor_v6.ps1';$Trad='C:\\project'
function Get-CimInstance {{param($ClassName,$ErrorAction);throw 'synthetic CIM unavailable'}}
$supervisorThrows=$false;$inventoryThrows=$false
try {{$null=Get-Supervisors}} catch {{$supervisorThrows=$true}}
try {{$null=Get-ProjectPythonInventory}} catch {{$inventoryThrows=$true}}
@{{supervisor=$supervisorThrows;inventory=$inventoryThrows}}|ConvertTo-Json -Compress
""")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(supervisor=True, inventory=True)
    assert "action='no_recovery_without_process_inventory'" in source


@pytest.mark.parametrize('same', [False, True])
def test_watchdog_stop_requires_same_current_owner_identity(tmp_path, same):
    result = invoke(tmp_path, f""". '{HELPER}'
$original=[pscustomobject]@{{Name='powershell.exe';ProcessId=10;CreationDate=[DateTime]'2026-09-15T01:00:00';CommandLine='powershell -File selected.ps1'}}
$script:current=[pscustomobject]@{{Name='powershell.exe';ProcessId=10;CreationDate=[DateTime]'2026-09-15T{'01' if same else '02'}:00:00';CommandLine='powershell -File selected.ps1'}}
$script:stopped=@()
function Get-CimInstance {{param($ClassName,$Filter,$ErrorAction);$script:current}}
function Stop-Process {{param($Id,[switch]$Force,$ErrorAction);$script:stopped+=$Id}}
$answer=Stop-OperationalVerifiedSupervisor $original
@{{answer=$answer;stopped=@($script:stopped)}}|ConvertTo-Json -Compress
""")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(answer=same, stopped=[10] if same else [])


def test_declared_cold_transport_flags_pass_actual_cli_guard_before_missing_config(tmp_path):
    result = invoke(tmp_path, f""". '{HELPER}'
$service=@{{name='revision_news_transport_v6';interpreter_mode='cold_no_bytecode_unique_prefix'}}
@{{first=@(Get-OperationalInterpreterArguments $service '{tmp_path}');second=@(Get-OperationalInterpreterArguments $service '{tmp_path}');plain=@(Get-OperationalInterpreterArguments @{{name='other';interpreter_mode='no_bytecode'}} '{tmp_path}')}}|ConvertTo-Json -Compress
""")
    assert result.returncode == 0, result.stderr
    values = json.loads(result.stdout)
    assert values['plain'] == ['-B']
    assert values['first'][:2] == ['-B', '-X']
    assert values['first'][2] != values['second'][2]
    prefix = Path(values['first'][2].split('=', 1)[1])
    assert not prefix.exists()
    # Missing input guarantees refusal before _State creation/network work.
    command = [os.sys.executable, *values['first'], str(ROOT / 'revision_transport_v6.py'),
               '--config', str(tmp_path / 'deliberately_missing_config.json'), '--config-sha256', '0' * 64]
    preflight = subprocess.run(command, capture_output=True, text=True, timeout=30)
    assert preflight.returncode != 0
    assert 'FileNotFoundError' in preflight.stderr, preflight.stderr
    assert 'transport_cold_no_bytecode_required' not in preflight.stderr
    assert 'transport_unused_bytecode_prefix_required' not in preflight.stderr
    assert not prefix.exists()


def test_same_script_roles_are_separated_only_by_declared_exact_arguments(tmp_path):
    result = invoke(tmp_path, f""". '{HELPER}'
$script='C:\\project\\news.py'
$default=[pscustomobject]@{{ProcessId=10;CommandLine='python C:\\project\\news.py --interval-sec 60 --duration-sec 604800'}}
$selected=[pscustomobject]@{{ProcessId=20;CommandLine='python C:\\project\\news.py --output-root C:\\project\\selected --interval-sec 60 --duration-sec 604800'}}
$unknown=[pscustomobject]@{{ProcessId=30;CommandLine='python C:\\project\\news.py --output-root C:\\project\\other --interval-sec 60 --duration-sec 604800'}}
$profile=@{{services=@(@{{name='default';script='news.py';arguments=@('--interval-sec','60','--duration-sec','604800')}},@{{name='selected';script='news.py';arguments=@('--output-root','C:\\project\\selected','--interval-sec','60','--duration-sec','604800')}})}}
$selection=Select-OperationalRoleCandidates @($default,$selected,$unknown) default $script @('--interval-sec','60','--duration-sec','604800') $profile
@{{owned=@($selection.owned|ForEach-Object{{$_.ProcessId}});other=@($selection.other_declared_roles|ForEach-Object{{$_.ProcessId}});conflicts=@($selection.conflicts|ForEach-Object{{$_.ProcessId}})}}|ConvertTo-Json -Compress
""")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(owned=[10], other=[20], conflicts=[30])
