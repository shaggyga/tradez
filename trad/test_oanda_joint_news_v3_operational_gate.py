"""Execute only the isolated inert v3 gate; never launch or stop a process."""
import json
from pathlib import Path
import subprocess

import pytest

SOURCE = Path(__file__).with_name('oanda_always_on_supervisor.ps1')
FLAGS = ('can_place_orders', 'can_promote', 'can_authorize', 'account_eligible', 'proof_eligible', 'historical_rows_imported')
VALID = {'schema_version': 'joint_price_news_registry_v3_20260908',
         'collection_enabled': True, 'research_only': True, **dict.fromkeys(FLAGS, False)}
DISABLED = ['local_news_sentiment_repair_v1', 'joint_price_news_study_v3']


def check(tmp_path, registry):
    source = SOURCE.read_text(encoding='utf-8-sig')
    block = '$JointPriceNewsV3Config = ' + source.split('$JointPriceNewsV3Config = ', 1)[1].split('$SupervisorMutex = ', 1)[0]
    assert not any(word in block for word in ('Start-Process', 'Stop-Process', 'New-Item', 'Mutex', 'OANDA_CREDS'))
    folder = tmp_path / 'config'
    folder.mkdir()
    if registry is not None:
        (folder / 'joint_price_news_study_v3_20260908.json').write_text(json.dumps(registry))
    script = tmp_path / 'gate.ps1'
    script.write_text("param([string]$Trad)\n$ErrorActionPreference='Stop'\n$DisabledNames=@()\n" + block +
                      "\n@{enabled=$JointPriceNewsV3Enabled;disabled=@($DisabledNames)}|ConvertTo-Json -Compress")
    result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
                             '-File', str(script), '-Trad', str(tmp_path)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_registered_inert_v3_allows_only_its_two_named_workers(tmp_path):
    assert check(tmp_path, VALID) == {'enabled': True, 'disabled': []}


@pytest.mark.parametrize('flag', FLAGS)
@pytest.mark.parametrize('bad', [True, 'false', None, 0])
def test_authority_flags_require_exact_false(tmp_path, flag, bad):
    assert check(tmp_path, {**VALID, flag: bad}) == {'enabled': False, 'disabled': DISABLED}


@pytest.mark.parametrize('registry', [None, [], dict(VALID, schema_version='joint_price_news_registry_v2_20260907'),
                                    dict(VALID, collection_enabled='true'), dict(VALID, research_only=False)])
def test_missing_wrong_or_unsafe_registration_disables_both_workers(tmp_path, registry):
    assert check(tmp_path, registry) == {'enabled': False, 'disabled': DISABLED}


def test_launches_are_unique_research_workers_without_automatic_activation():
    source = SOURCE.read_text(encoding='utf-8-sig')
    for name, module, schema in (
        ('local_news_sentiment_repair_v1', 'oanda_local_news_sentiment_repair_v1.py', 'repaired_joint_news_heartbeat_v1_20260908'),
        ('joint_price_news_study_v3', 'oanda_joint_price_news_forecast_study_v3.py', 'joint_price_news_forecast_heartbeat_v3_20260908'),
    ):
        assert source.count('-Name "' + name + '"') == 1
        block = source.split('-Name "' + name + '"', 1)[1].split('$managed += Start-ManagedProcess', 1)[0]
        assert module in block and schema in block
        assert '$CoreTimeseriesPython' in block and '"BelowNormal"' in block
        assert '--activate' not in block
        if name=='local_news_sentiment_repair_v1':
            assert '"--interval-sec", "60"' in block
            assert 'MaxAgeSec = 90' in block
