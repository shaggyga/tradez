"""Run only extracted side-effect-free gates, never the live supervisor body."""
from pathlib import Path
import copy
import hashlib
import json
import subprocess
import time

import pytest

SUPERVISOR = Path(__file__).parents[1] / 'trad/oanda_always_on_supervisor.ps1'
REGISTRY = 'pair_local_forecast_study_v2_20260907.json'
FLAGS = ('can_place_orders', 'can_promote', 'can_authorize', 'account_eligible',
         'proof_eligible', 'historical_rows_imported')
VALID = {'schema_version':'pair_local_forecast_registry_v2_20260907',
         'collection_enabled':True, 'research_only':True, **dict.fromkeys(FLAGS, False)}
LEGACY = ('causal_forecast_study_v1', 'eurusd_local_forecast_study')


def extract(source, begin, end):
    assert source.count(begin) == 1
    return source.split(begin, 1)[1].split(end, 1)[0]


def run_gates(tmp_path, registry=VALID, selection=None):
    # Do not evaluate the prefix that reads credentials, creates directories,
    # acquires the global mutex, or starts/stops real processes.
    source = SUPERVISOR.read_text(encoding='utf-8-sig')
    allowlist = '$ResearchCollectionNames = @(' + extract(source, '$ResearchCollectionNames = @(', '\n)') + '\n)'
    registration = '$CausalStudyConfig = ' + extract(source, '$CausalStudyConfig = ', '\n$SupervisorMutex = ')
    reason = '$disabledReason = if ' + extract(source, '$disabledReason = if ', '\n    if ($disabledReason) {')
    assert not any(token in registration for token in ('Start-Process', 'Stop-Process', 'New-Item', 'Mutex', 'OANDA_CREDS'))
    config = tmp_path/'trad/config'
    config.mkdir(parents=True)
    for name in ('causal_forecast_study_gap_v2_20260907.json', 'causal_forecast_study_eurusd_v1_20260907.json'):
        (config/name).write_text(json.dumps({'collection_enabled':True}), encoding='utf-8')
    old = dict(VALID, schema_version='pair_local_forecast_registry_v1_20260907')
    (config/'pair_local_forecast_study_v1_20260907.json').write_text(json.dumps(old), encoding='utf-8')
    if registry is not None:
        (config/REGISTRY).write_text(registry if isinstance(registry, str) else json.dumps(registry), encoding='utf-8')
    if selection is not None:
        chosen = copy.deepcopy(selection)
        if chosen.get('registry_sha256') == 'CURRENT':
            chosen['registry_sha256'] = hashlib.sha256((config/REGISTRY).read_bytes()).hexdigest()
        (config/'pair_forecast_primary_current.json').write_text(json.dumps(chosen), encoding='utf-8')
    script = tmp_path/'extracted_gate.ps1'
    script.write_text('''param([string]$Trad)
$ErrorActionPreference = 'Stop'
$DisabledNames = @()
$ResearchCollectionOnly = $true
$SafeCoreOnly = $false
$SafeCoreSkippedNames = @()
''' + allowlist + '\n' + registration + '\n' + '''$allowed = @{}
foreach ($Name in @('causal_forecast_study_v1', 'eurusd_local_forecast_study', 'pair_local_forecast_study_v1', 'pair_local_forecast_study_v2', 'unknown_order_executor', 'model_predictor_auto_promotion', 'account_snapshot')) {
''' + reason + '''
    $allowed[$Name] = ($disabledReason -eq '')
}
@{v2_enabled=$PairStudyV2Enabled; disabled=@($DisabledNames); allowed=$allowed;
  selection_epoch=$pairPrimaryEpoch; selection_epoch_valid=$pairPrimaryEpochValid;
  source_registry_hash=$pairV2RegistryHash; selected_registry_hash=$pairPrimaryValue.registry_sha256;
  gate_observed_epoch=([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()/1000.0);
  retained_errors=@($Error | ForEach-Object { $_.Exception.Message })} | ConvertTo-Json -Depth 10 -Compress
''', encoding='utf-8')
    result = subprocess.run(['powershell.exe','-NoLogo','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass',
                             '-File',str(script),'-Trad',str(config.parent)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    state = json.loads(result.stdout)
    assert state['allowed']['unknown_order_executor'] is False
    assert state['allowed']['model_predictor_auto_promotion'] is False
    assert state['allowed']['account_snapshot'] is True
    assert state['allowed']['pair_local_forecast_study_v1'] is True
    return state


def selected(**changes):
    return {'schema_version':'pair_forecast_primary_selection_v1_20260907', 'selected':'v2',
            'registry_sha256':'CURRENT', 'activated_epoch':time.time()-60, **changes}


@pytest.mark.parametrize('registry', [None, '{broken', dict(VALID, schema_version='wrong'),
    dict(VALID, collection_enabled=False), dict(VALID, collection_enabled='true'),
    dict(VALID, research_only=False), dict(VALID, research_only='true')])
def test_missing_or_invalid_registry_disables_only_new_study(tmp_path, registry):
    state = run_gates(tmp_path, registry=registry)
    assert state['v2_enabled'] is False
    assert state['allowed']['pair_local_forecast_study_v2'] is False
    assert all(state['allowed'][name] for name in LEGACY)


@pytest.mark.parametrize('flag', FLAGS)
@pytest.mark.parametrize('value', [True, 'false', None, 0])
def test_every_inert_flag_requires_literal_boolean_false(tmp_path, flag, value):
    state = run_gates(tmp_path, registry=dict(VALID, **{flag:value}))
    assert state['v2_enabled'] is False
    assert all(state['allowed'][name] for name in LEGACY)


@pytest.mark.parametrize('selection', [None, selected(registry_sha256='0'*64), selected(schema_version='wrong'),
    selected(selected='v1'), selected(activated_epoch=0), selected(activated_epoch='123'),
    selected(activated_epoch=True), selected(activated_epoch=time.time()+86400)])
def test_valid_registry_without_valid_selection_keeps_legacy_studies(tmp_path, selection):
    state = run_gates(tmp_path, selection=selection)
    assert state['v2_enabled'] is True
    assert state['allowed']['pair_local_forecast_study_v2'] is True
    assert all(state['allowed'][name] for name in LEGACY)


def test_selected_valid_v2_retires_exactly_two_superseded_workers(tmp_path):
    unselected = run_gates(tmp_path/'unselected')
    state = run_gates(tmp_path/'selected', selection=selected())
    assert state['v2_enabled'] is True
    assert state['allowed']['pair_local_forecast_study_v2'] is True
    # Other deliberately absent study configs retain their own disabled rows.
    # The valid pair-V2 selection adds exactly its two superseded workers.
    assert set(state['disabled'])-set(unselected['disabled']) == set(LEGACY), state
    assert set(unselected['disabled']) <= set(state['disabled'])
    assert all(not state['allowed'][name] for name in LEGACY)


def test_unsafe_registry_cannot_retire_legacy_even_with_matching_selector(tmp_path):
    state = run_gates(tmp_path, registry=dict(VALID, can_place_orders=True), selection=selected())
    assert state['v2_enabled'] is False
    assert all(state['allowed'][name] for name in LEGACY)
