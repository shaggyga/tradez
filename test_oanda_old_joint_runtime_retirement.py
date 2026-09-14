"""Only the two fully settled old joint workers change operational scope."""
import hashlib
import json
from pathlib import Path
import re

import pytest

from trad import oanda_project_runtime_health as health
from trad.test_oanda_project_runtime_health import fixture, observe

ROOT=Path(__file__).resolve().parent
EVIDENCE=ROOT.parent/'overnight_curve_buildout_20260909/old_joint_retirement_preflight_v1'
RETIRED={'joint_price_news_study_v1','joint_price_news_study_v2'}


def block(text,name):
    return text.split('$'+name+' = @(',1)[1].split(')',1)[0]


def test_supervisor_preserves_original_joint_retirement_with_later_operational_profile():
    before=(EVIDENCE/'before_source/oanda_always_on_supervisor.ps1').read_text()
    after=(ROOT/'oanda_always_on_supervisor.ps1').read_text()
    quoted=lambda value:set(re.findall(r'"([a-z0-9_]+)"',value))
    assert quoted(block(after,'DisabledNames'))-quoted(block(before,'DisabledNames'))==RETIRED
    assert quoted(block(before,'DisabledNames'))<=quoted(block(after,'DisabledNames'))
    # Subsequent feature collectors and the opt-in operational profile have
    # their own contracts; this historical test still guards its two retirees.
    assert quoted(block(before,'ResearchCollectionNames'))<=quoted(block(after,'ResearchCollectionNames'))
    assert RETIRED <= quoted(block(after,'ResearchCollectionNames'))
    begin=after.index('    # September 9: original joint V1/V2 obligations')
    end=after.index('    # September 6 clock audit:',begin)
    assert quoted(after[begin:end]) == RETIRED


def test_health_only_two_expected_names_removed():
    before=(EVIDENCE/'before_source/oanda_project_runtime_health.py').read_text()
    namespace={};exec(compile(before,'retained_before_health','exec'),namespace)
    assert namespace['EXPECTED_RESEARCH_WORKERS']-health.EXPECTED_RESEARCH_WORKERS==RETIRED
    assert health.EXPECTED_RESEARCH_WORKERS<=namespace['EXPECTED_RESEARCH_WORKERS']
    assert len(health.EXPECTED_RESEARCH_WORKERS)==15
    assert namespace['CHECK_WORKERS']==health.CHECK_WORKERS


@pytest.mark.parametrize('mode',['inactive','running','missing'])
def test_old_joint_rows_never_upgrade_failures_or_infer_missing_retirement(tmp_path,mode):
    start,hb=fixture();start['research_collection_names']+=sorted(RETIRED)
    for idx,name in enumerate(sorted(RETIRED)):
        if mode!='missing':hb['managed'].append({'name':name,'running':mode=='running',
            'pids':[9000+idx] if mode=='running' else [],
            'freshness':{'fresh':True,'reason':'fresh' if mode=='running' else 'disabled','age_sec':0}})
    result=observe(tmp_path,start,hb)
    for name in RETIRED:
        if mode=='missing':assert name not in result['workers']
        else:
            assert result['workers'][name]['explicitly_inactive']==(mode=='inactive')
            if mode=='running':assert 'unexpected_active_worker:'+name in result['reasons']
    checks={'old_joint_retained_result':False}
    assert health.scope_integrity_checks(checks,result)['check_scopes']['old_joint_retained_result']['artifact_check_passed'] is False
    assert checks=={'old_joint_retained_result':False}


def test_separate_manifest_binds_all_original_terminal_obligations():
    value=json.loads((ROOT/'config/joint_study_runtime_retirement_v1_20260909.json').read_text())
    assert set(value['disabled_names'])==RETIRED
    assert value['runtime_actions_executed'] is False
    assert value['registered_ledgers']==136 and value['original_forecasts_terminal']==4050
    assert value['preserve_registered_sources_and_records'] is True
    source=ROOT/value['prior_retirement_history']['path']
    assert hashlib.sha256(source.read_bytes()).hexdigest()==value['prior_retirement_history']['sha256']
    evidence=Path(value['obligation_audit']['path'])
    raw=evidence.read_bytes();audit=json.loads(raw)
    assert hashlib.sha256(raw).hexdigest()==value['obligation_audit']['sha256']
    assert audit['status']=='no_retirement_obligation_blocker_observed'
    assert len(audit['ledgers'])==136 and all(not row['issues'] for row in audit['ledgers'])
    assert sum(s['row_counts']['forecasts'] for s in audit['studies'].values())==4050
    assert sum(s['row_counts']['outcomes']+s['row_counts']['exclusions'] for s in audit['studies'].values())==4050
