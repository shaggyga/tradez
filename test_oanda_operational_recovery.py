"""PowerShell parser and pure recovery-contract tests; never launch services."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest

ROOT=Path(__file__).parent
PS=Path(os.environ.get('SystemRoot','C:/Windows'))/'System32/WindowsPowerShell/v1.0/powershell.exe'
SERVICES={
    'revision_news_collector_v2':'oanda_local_news_sentiment.py',
    'local_news_sentiment_repair_v2':'oanda_local_news_sentiment_repair_v2.py',
    'revision_news_transport_v4':'revision_transport_v4.py',
    'joint_price_news_study_v7':'oanda_joint_price_news_forecast_study_v7.py',
    'pair_local_forecast_study_v3':'oanda_pair_local_forecast_study_v3.py',
    'retained_price_settlement_v1':'oanda_retained_price_settlement_v1.py',
    'native_feature_candles_v1':'oanda_native_feature_candle_updater_v1.py',
    'research_feature_observations_v2':'oanda_research_feature_observation_worker_v2.py',
    'research_feature_forward_v2':'oanda_feature_forward_worker_v1.py',
    'official_pair_horizon_v2':'oanda_official_event_pair_horizon_capture_v2.py'}


def invoke(tmp_path, code, *args):
    if not PS.exists(): pytest.skip('native Windows PowerShell required')
    runner=tmp_path/'pure_test.ps1'
    runner.write_text(code,encoding='utf-8')
    result=subprocess.run([str(PS),'-NoLogo','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass',
                           '-File',str(runner),*map(str,args)],capture_output=True,text=True,timeout=30)
    return result


def profile(tmp_path):
    project=tmp_path/'project space'/'trad'; project.mkdir(parents=True)
    rows=[]
    for name,script in SERVICES.items():
        raw=f'# fixture only {name}\n'.encode(); (project/script).write_bytes(raw)
        needle=script
        if name=='revision_news_collector_v2':
            needle += '*'+str(project/'data'/'oanda_training_manager'/'market_open_20260913_v1'/'local_news_sentiment')
        elif name=='research_feature_forward_v2':
            needle += '*'+str(project/'data'/'oanda_training_manager'/'operational_repair_20260913_v1'/'feature_forward_v3')
        rows.append(dict(name=name,script=script,source_sha256=hashlib.sha256(raw).hexdigest(),
                         arguments=['--quiet'],heartbeat=str(project/'data'/'oanda_training_manager'/f'{name}.json'),
                         heartbeat_schema='fixture',max_age_sec=120,startup_grace_sec=180,
                         needle=needle))
    value=dict(schema_version='forex_operational_runtime_v1_20260913',research_only=True,
               can_place_orders=False,services=rows)
    target=project/'profile.json';target.write_text(json.dumps(value),encoding='utf-8')
    return project,target,value


VALIDATE="""param($Helper,$Trad,$Profile)
$ErrorActionPreference='Stop'
. $Helper
$b=Read-OperationalRecoveryProfile -Trad $Trad -ProfilePath $Profile -RecoveryUntilUtc '2026-09-20T23:59:00Z' -Now ([DateTimeOffset]'2026-09-14T00:00:00Z')
$b | ConvertTo-Json -Depth 6 -Compress
"""


def test_exact_ten_worker_profile_validates_and_hashes_parsed_bytes(tmp_path):
    project,path,value=profile(tmp_path)
    result=invoke(tmp_path,VALIDATE,ROOT/'oanda_operational_recovery_contract.ps1',project,path)
    assert result.returncode==0,result.stderr
    binding=json.loads(result.stdout)
    assert binding['sha256']==hashlib.sha256(path.read_bytes()).hexdigest()
    assert set(binding['services'])==set(SERVICES)


@pytest.mark.parametrize('mode,reason',[
    ('orders','explicit research-only no-orders profile'),
    ('not_research','explicit research-only no-orders profile'),
    ('unknown_worker','Unregistered operational recovery service'),
    ('missing_worker','exactly ten distinct managed services'),
    ('duplicate_worker','exactly ten distinct managed services'),
    ('changed_source','Operational worker source changed'),
    ('escaping_heartbeat','declared project data path'),
    ('bad_arguments','Invalid bounded operational recovery parameters'),
    ('bad_age','Invalid bounded operational recovery parameters'),
    ('ambiguous_needle','Operational worker identity is not exact'),
])
def test_broken_or_broader_profile_is_refused_before_launch(tmp_path,mode,reason):
    project,path,value=profile(tmp_path)
    if mode=='orders': value['can_place_orders']=True
    if mode=='not_research': value['research_only']='true'
    if mode=='unknown_worker': value['services'][0]['name']='order_executor'
    if mode=='missing_worker': value['services'].pop()
    if mode=='duplicate_worker': value['services'][0]=value['services'][1]
    if mode=='changed_source': (project/value['services'][0]['script']).write_text('# changed')
    if mode=='escaping_heartbeat': value['services'][0]['heartbeat']=str(tmp_path/'outside.json')
    if mode=='bad_arguments': value['services'][0]['arguments']=['line\nbreak']
    if mode=='bad_age': value['services'][0]['max_age_sec']=True
    if mode=='ambiguous_needle': value['services'][0]['needle']='oanda_local_news_sentiment.py'
    path.write_text(json.dumps(value),encoding='utf-8')
    result=invoke(tmp_path,VALIDATE,ROOT/'oanda_operational_recovery_contract.ps1',project,path)
    assert result.returncode!=0
    assert reason in result.stderr


def test_expired_recovery_is_refused(tmp_path):
    project,path,_=profile(tmp_path)
    result=invoke(tmp_path,VALIDATE.replace("2026-09-14T00:00:00Z","2026-09-21T00:00:00Z"),
                  ROOT/'oanda_operational_recovery_contract.ps1',project,path)
    assert result.returncode!=0
    assert 'expiry reached' in result.stderr


def test_recovery_arguments_always_specify_safe_research_profile(tmp_path):
    project,path,_=profile(tmp_path)
    code="""param($Helper,$Root,$Profile)
. $Helper
$arguments=@(Get-OperationalResearchSupervisorArguments -Root $Root -ProfilePath $Profile)
@{arguments=$arguments;quoted=@($arguments|ForEach-Object {ConvertTo-OperationalProcessArgument $_})} | ConvertTo-Json -Compress
"""
    result=invoke(tmp_path,code,ROOT/'oanda_operational_recovery_contract.ps1',project.parent,path)
    assert result.returncode==0,result.stderr
    values=json.loads(result.stdout)
    assert '-SafeCoreOnly' in values['arguments']
    assert '-ResearchCollectionOnly' in values['arguments']
    assert values['arguments'][values['arguments'].index('-OperationalProfilePath')+1]==str(path)
    assert not any('start_oanda_safe_core.ps1' in arg for arg in values['arguments'])
    assert any(arg.startswith('"') and 'project space' in arg for arg in values['quoted'])


def test_profile_matching_refuses_legacy_and_other_profile(tmp_path):
    code="""param($Helper)
. $Helper
$p='C:\\project\\trad\\config\\profile.json'
$good='powershell -File C:\\project\\trad\\oanda_always_on_supervisor.ps1 -SafeCoreOnly -ResearchCollectionOnly -OperationalProfilePath '+$p
@{good=(Test-OperationalSupervisorCommand $good $p);legacy=(Test-OperationalSupervisorCommand 'powershell -File old.ps1 -SafeCoreOnly' $p);other=(Test-OperationalSupervisorCommand $good 'C:\\other.json')} | ConvertTo-Json -Compress
"""
    result=invoke(tmp_path,code,ROOT/'oanda_operational_recovery_contract.ps1')
    assert result.returncode==0,result.stderr
    assert json.loads(result.stdout)==dict(good=True,legacy=False,other=False)


def test_powershell_scripts_parse_without_execution(tmp_path):
    code="""param($Project)
$ErrorActionPreference='Stop'
foreach($name in @('oanda_operational_recovery_contract.ps1','start_oanda_operational_research.ps1','oanda_supervisor_watchdog.ps1','start_oanda_supervisor_watchdog.ps1')) {
    $tokens=$null; $errors=$null
    $null=[System.Management.Automation.Language.Parser]::ParseFile((Join-Path $Project $name),[ref]$tokens,[ref]$errors)
    if($errors.Count) { throw ($errors | Out-String) }
}
'parsed'
"""
    result=invoke(tmp_path,code,ROOT)
    assert result.returncode==0,result.stderr
    assert 'parsed' in result.stdout
