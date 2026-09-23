"""Exercise only the inert registration gate; never execute supervisor actions."""
from pathlib import Path
import json
import subprocess
import pytest

FLAGS=('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported')
VALID={'schema_version':'joint_price_news_registry_v1_20260907','collection_enabled':True,'research_only':True,**dict.fromkeys(FLAGS,False)}

def check(tmp_path,registry):
    source=Path(__file__).with_name('oanda_always_on_supervisor.ps1').read_text(encoding='utf-8-sig')
    gate='$JointPriceNewsConfig = '+source.split('$JointPriceNewsConfig = ',1)[1].split('# Retire superseded',1)[0]
    assert not any(word in gate for word in ('Start-Process','Stop-Process','New-Item','Mutex','OANDA_CREDS'))
    folder=tmp_path/'config';folder.mkdir()
    if registry is not None:(folder/'joint_price_news_study_v1_20260907.json').write_text(json.dumps(registry))
    script=tmp_path/'gate.ps1'
    script.write_text("param([string]$Trad)\n$ErrorActionPreference='Stop'\n$DisabledNames=@()\n"+gate+"\n@{enabled=$JointPriceNewsEnabled;disabled=@($DisabledNames)} | ConvertTo-Json -Compress")
    result=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(script),'-Trad',str(tmp_path)],capture_output=True,text=True,timeout=15)
    assert result.returncode==0,result.stderr
    return json.loads(result.stdout)

def test_registered_inert_joint_worker_is_allowed(tmp_path):
    result=check(tmp_path,VALID)
    assert result=={'enabled':True,'disabled':[]}

@pytest.mark.parametrize('flag',FLAGS)
@pytest.mark.parametrize('value',[True,'false',None,0])
def test_joint_authority_flags_require_exact_false(tmp_path,flag,value):
    result=check(tmp_path,dict(VALID,**{flag:value}))
    assert result['enabled'] is False and result['disabled']==['joint_price_news_study_v1']

@pytest.mark.parametrize('registry',[None,dict(VALID,schema_version='wrong'),dict(VALID,collection_enabled='true'),dict(VALID,research_only=False)])
def test_missing_or_invalid_joint_registry_is_disabled(tmp_path,registry):
    result=check(tmp_path,registry)
    assert result['enabled'] is False and result['disabled']==['joint_price_news_study_v1']


@pytest.mark.parametrize('name',['local_news_sentiment','official_release_fast_mapper','project_integrity_audit'])
def test_active_news_freshness_gate_matches_current_producer_version(name):
    import re
    import oanda_news_classification_contract as contract
    source=Path(__file__).with_name('oanda_always_on_supervisor.ps1').read_text(encoding='utf-8-sig')
    block=source.split('-Name "'+name+'"',1)[1].split('$managed += Start-ManagedProcess',1)[0]
    expected=re.search(r'ExpectedJsonValue = "([^"]+)"',block).group(1)
    assert expected==contract.NEWS_CLASSIFICATION_VERSION
