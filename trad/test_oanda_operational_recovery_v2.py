"""Pure successor contract and simulated supervisor checks; no real services."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parent
HELPER = ROOT / 'oanda_operational_recovery_contract_v2.ps1'
PS = Path(os.environ.get('SystemRoot', 'C:/Windows')) / 'System32/WindowsPowerShell/v1.0/powershell.exe'
SCRIPTS = ['oanda_operational_recovery_contract_v2.ps1', 'oanda_operational_supervisor_v2.ps1',
           'oanda_supervisor_watchdog_v2.ps1', 'start_oanda_operational_research_v2.ps1',
           'start_oanda_supervisor_watchdog_v2.ps1', 'oanda_operational_log_relay_v1.py']
ROLES = dict(all68_m1_cadence_v2='oanda_all68_m1_cadence_v2.py',
             practice_quote_stream_v1='oanda_practice_quote_stream.py',
             clock_integrity_monitor_v1='oanda_clock_integrity_monitor.py',
             rolling_technical_operations_v2='oanda_rolling_technical_worker_v2.py',
             research_feature_forward_cached_v2='oanda_feature_forward_worker_v2.py',
             default_news_collector_v1='oanda_local_news_sentiment.py',
             revision_news_collector_v2='oanda_local_news_sentiment.py',
             local_news_sentiment_repair_v2='oanda_local_news_sentiment_repair_v2.py',
             revision_news_transport_v5='revision_transport_v5.py',
             joint_price_news_study_v8='oanda_joint_price_news_forecast_study_v8.py',
             pair_local_forecast_study_v3='oanda_pair_local_forecast_study_v3.py',
             retained_price_settlement_v1='oanda_retained_price_settlement_v1.py',
             native_feature_candles_v1='oanda_native_feature_candle_updater_v1.py',
             research_feature_observations_v2='oanda_research_feature_observation_worker_v2.py',
             research_feature_forward_v2='oanda_feature_forward_worker_v1.py',
             official_pair_horizon_v2='oanda_official_event_pair_horizon_capture_v2.py')


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
                         interpreter_mode='cold_no_bytecode_unique_prefix' if name == 'revision_news_transport_v5' else 'no_bytecode'))
    value = dict(schema_version='forex_operational_runtime_v2_20260916', research_only=True, can_place_orders=False,
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


def test_exact_profile_has_sixteen_pinned_research_roles_and_external_rolling_data(tmp_path):
    project, path, _ = fixture(tmp_path)
    result = validate(tmp_path, project, path)
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert len(value['services']) == 16
    assert value['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert value['recovery_allowed'] is True


@pytest.mark.parametrize('change,expected', [
    ('orders', 'explicit research-only'), ('expiry', 'expiry differ'),
    ('source', 'worker source changed'), ('support', 'support source changed'),
    ('missing', 'sixteen distinct'), ('duplicate', 'sixteen distinct'),
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
$current='powershell -File {ROOT / 'oanda_operational_supervisor_v2.ps1'} -SafeCoreOnly -ResearchCollectionOnly -OperationalProfilePath '+$p
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
$h=@{{schema_version='operational_supervisor_v2_20260916';supervisor_pid=123;operational_profile_sha256='hash';generated_utc='2026-09-16T03:00:00Z'}}
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
    text = (ROOT / 'oanda_supervisor_watchdog_v2.ps1').read_text()
    assert 'account_aggregate_stale' not in text
    assert 'account_freshness_is_recovery_criterion = $false' in text
    assert 'Test-OperationalSupervisorIdentity' in text
    assert 'Local\\ForexSupervisorWatchdogV1_' in text  # Preserve single-owner mutex.


def test_profile_exclusive_supervisor_keeps_existing_mutex_and_no_legacy_role_body():
    text = (ROOT / 'oanda_operational_supervisor_v2.ps1').read_text()
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
    source = (ROOT / 'oanda_operational_supervisor_v2.ps1').read_text()
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
    assert any('oanda_operational_log_relay_v1.py' in arg for arg in value['captured'])
    assert any('worker.py' in arg and arg.startswith('"') for arg in value['captured'])
    assert (tmp_path / 'project space/restart.json').exists()


def test_relative_python_script_is_visible_as_conflict_not_adoption(tmp_path):
    result = invoke(tmp_path, f""". '{HELPER}'
@{{relative=(Test-OperationalRelativeWorkerCommand 'python -B oanda_rolling_technical_worker_v2.py --config config/a.json' 'oanda_rolling_technical_worker_v2.py');dot=(Test-OperationalRelativeWorkerCommand 'python .\\oanda_rolling_technical_worker_v2.py' 'oanda_rolling_technical_worker_v2.py');absolute=(Test-OperationalRelativeWorkerCommand 'python C:\\project\\oanda_rolling_technical_worker_v2.py' 'oanda_rolling_technical_worker_v2.py');other=(Test-OperationalRelativeWorkerCommand 'python -c print(x<2)' 'oanda_rolling_technical_worker_v2.py')}}|ConvertTo-Json -Compress
""")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(relative=True, dot=True, absolute=False, other=False)


def test_three_level_relay_virtualenv_actual_worker_is_one_leaf(tmp_path):
    source = (ROOT / 'oanda_operational_supervisor_v2.ps1').read_text()
    functions = source[source.index('function Get-MatchingPython {'):source.index('function Stop-MatchingPython {')]
    result = invoke(tmp_path, f"""{functions}
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
    source = (ROOT / 'oanda_operational_supervisor_v2.ps1').read_text()
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
    source = (ROOT / 'oanda_supervisor_watchdog_v2.ps1').read_text()
    functions = source[source.index('function Get-Supervisors {'):source.index('function Get-RecentRestartCount {')]
    result = invoke(tmp_path, f""". '{HELPER}'
{functions}
$Supervisor='C:\\project\\oanda_operational_supervisor_v2.ps1';$Trad='C:\\project'
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
$service=@{{name='revision_news_transport_v5';interpreter_mode='cold_no_bytecode_unique_prefix'}}
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
    command = [os.sys.executable, *values['first'], str(ROOT / 'revision_transport_v5.py'),
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
