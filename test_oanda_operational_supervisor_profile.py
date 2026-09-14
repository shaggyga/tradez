"""Execute only the inert operational profile gate, never the supervisor loop."""
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parent
SCRIPTS = {
    'revision_news_collector_v2':'oanda_local_news_sentiment.py',
    'local_news_sentiment_repair_v2':'oanda_local_news_sentiment_repair_v2.py',
    'revision_news_transport_v4':'revision_transport_v4.py',
    'joint_price_news_study_v7':'oanda_joint_price_news_forecast_study_v7.py',
    'pair_local_forecast_study_v3':'oanda_pair_local_forecast_study_v3.py',
    'retained_price_settlement_v1':'oanda_retained_price_settlement_v1.py',
    'native_feature_candles_v1':'oanda_native_feature_candle_updater_v1.py',
    'research_feature_observations_v2':'oanda_research_feature_observation_worker_v2.py',
    'research_feature_forward_v2':'oanda_feature_forward_worker_v1.py',
    'official_pair_horizon_v2':'oanda_official_event_pair_horizon_capture_v2.py',
}


def check(tmp_path, change=None):
    data = tmp_path/'data'
    data.mkdir()
    services=[]
    for name, script in SCRIPTS.items():
        (tmp_path/script).write_bytes(b'# fixture source\n')
        services.append(dict(name=name, script=script, source_sha256=hashlib.sha256(b'# fixture source\n').hexdigest(),
            arguments=['--duration-sec','60'], heartbeat=str(data/(name+'.json')), heartbeat_schema=name,
            max_age_sec=90,startup_grace_sec=120,
            needle=(script+'*'+str(data/'market_open_20260913_v1/local_news_sentiment'))
                if name=='revision_news_collector_v2' else (script+'*'+str(data/'operational_repair_20260913_v1/feature_forward_v3'))
                if name=='research_feature_forward_v2' else script))
    profile=dict(schema_version='forex_operational_runtime_v1_20260913',research_only=True,can_place_orders=False,services=services)
    if change:change(profile)
    path=tmp_path/'profile.json';path.write_text(json.dumps(profile))
    source=(ROOT/'oanda_always_on_supervisor.ps1').read_text(encoding='utf-8-sig')
    block=source[source.index('$OperationalProfile = $null'):source.index('$SupervisorStartedUtc =')]
    assert 'Start-Process' not in block and 'Stop-Process' not in block
    harness=tmp_path/'gate.ps1'
    harness.write_text('param([string]$Trad,[string]$OperationalProfilePath)\n$ErrorActionPreference="Stop"\n'
        '$DataRoot=Join-Path $Trad "data"\n$ResearchCollectionOnly=$true\n$SafeCoreOnly=$true\n'
        '$DisabledNames=@()\n$ResearchCollectionNames=@("account_snapshot","pair_local_forecast_study_v2",'
        '"research_feature_forward_v1")\n'+block+'\n@{disabled=$DisabledNames;allowed=$ResearchCollectionNames}|ConvertTo-Json -Compress\n')
    return subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(harness),
        '-Trad',str(tmp_path),'-OperationalProfilePath',str(path)],capture_output=True,text=True,timeout=15)


def test_exact_profile_retires_old_workers_preserves_account_and_adds_all_successors(tmp_path):
    result=check(tmp_path)
    assert result.returncode==0,result.stderr
    value=json.loads(result.stdout)
    assert set(SCRIPTS)<=set(value['allowed'])
    assert 'account_snapshot' in value['allowed']
    assert 'pair_local_forecast_study_v2' in value['disabled']
    assert 'research_feature_forward_v1' not in value['allowed']


@pytest.mark.parametrize('change',[
    lambda p:p.update(can_place_orders=True),
    lambda p:p.update(can_place_orders='false'),
    lambda p:p['services'].pop(),
    lambda p:p['services'][0].update(source_sha256='0'*64),
    lambda p:p['services'][0].update(needle='*'),
    lambda p:p['services'][0].update(heartbeat='C:\\outside\\heartbeat.json'),
    lambda p:p['services'][0].update(name='order_executor'),
])
def test_unsafe_or_ambiguous_profile_refused(tmp_path,change):
    assert check(tmp_path,change).returncode!=0


def test_old_forward_process_match_cannot_capture_new_forward_cohort():
    source=(ROOT/'oanda_always_on_supervisor.ps1').read_text(encoding='utf-8-sig')
    old=source.split('-Name "research_feature_forward_v1"',1)[1].split('-Executable',1)[0]
    assert '-Needle "oanda_feature_forward_worker_v1.py*feature_forward_v2"' in old
