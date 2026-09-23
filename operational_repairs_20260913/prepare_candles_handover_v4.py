"""Stage the independently releasable candle, settlement and health repairs."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

AREA = Path(__file__).resolve().parent
ROOT = AREA.parent / 'trad'
TARGET = AREA / 'candles_handover_v4'
ENTRY = 'oanda_all68_m1_cadence_v2.py'
PROFILE = 'config/operational_runtime_v4_20260914_candles.json'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, indent=2, allow_nan=False).encode()


def once(text, before, after):
    if text.count(before) != 1:
        raise ValueError('unexpected_handover_patch_site')
    return text.replace(before, after, 1)


def prepare():
    manifest_raw = (AREA / 'm1_cadence_candidate_v2/SOURCE_MANIFEST.json').read_bytes()
    manifest = json.loads(manifest_raw)
    entry = (AREA / 'm1_cadence_candidate_v2' / ENTRY).read_bytes()
    if sha(entry) != manifest['candidate_files_sha256'][ENTRY]:
        raise ValueError('m1_candidate_changed')
    for name, expected in manifest['reused_production_sources_sha256'].items():
        if sha((ROOT / name).read_bytes()) != expected:
            raise ValueError('m1_reused_source_changed')
    source = (AREA / 'supervisor_health_candidate_v2/oanda_always_on_supervisor.ps1').read_text(encoding='utf-8-sig')
    begin = source.index('        $managed += Start-ManagedProcess `\n            -Name "all68_m1_forward_archive"')
    end = source.index('        # Isolated research-only support/resistance observer.', begin)
    block = '''        $managed += Start-ManagedProcess `
            -Name "all68_m1_forward_archive" `
            -Needle "oanda_all68_m1_cadence_v2.py" `
            -PriorityClass "BelowNormal" `
            -InterpreterArguments @("-B") `
            -Arguments @(
                (Join-Path $Trad "oanda_all68_m1_cadence_v2.py"),
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "all68_m1_cadence_heartbeat_v2.json")
                MaxAgeSec = 30
                StartupGraceSec = 120
                MaxProgressAgeSec = 180
                ProgressPhases = @("refreshing_forward", "background_gap_or_waiting")
                ExpectedJsonField = "worker"
                ExpectedJsonValue = "all68_m1_forward_archive"
            }
'''
    source = source[:begin] + block + source[end:]
    needle = '    $OperationalProfileSha256 = Get-OperationalSourceHash -LiteralPath $OperationalProfilePath\n'
    guard = ('    # The additional core cadence owner has an exact reviewed entrypoint.\n'
             '    if ((Get-OperationalSourceHash -LiteralPath (Join-Path $Trad "' + ENTRY + '")) -cne "' + sha(entry) + '") {\n'
             '        throw "Operational M1 cadence source changed."\n'
             '    }\n')
    source = once(source, needle, guard + needle)
    profile_raw = (ROOT / 'config/operational_runtime_v3_20260913.json').read_bytes()
    profile = json.loads(profile_raw)
    profile['prepared_utc'] = datetime.now(timezone.utc).isoformat()
    settlement = (AREA / 'settlement_idle_candidate_v2/oanda_retained_price_settlement_v1.py').read_bytes()
    for service in profile['services']:
        if service['name'] == 'retained_price_settlement_v1':
            service['source_sha256'] = sha(settlement)
    gate = (ROOT / 'test_oanda_operational_supervisor_profile.py').read_text(encoding='utf-8-sig')
    gate = once(gate, "    if change:change(profile)\n", "    (tmp_path/'" + ENTRY + "').write_bytes((ROOT/'" + ENTRY + "').read_bytes())\n    if change:change(profile)\n")
    files = {ENTRY: entry, 'oanda_always_on_supervisor.ps1': source.encode(),
             'oanda_retained_price_settlement_v1.py': settlement, PROFILE: encoded(profile),
             'test_oanda_operational_supervisor_profile.py': gate.encode(),
             'test_oanda_m1_cadence_v2.py': (AREA / 'm1_cadence_candidate_v2/test_m1_cadence_v2.py').read_bytes(),
             'test_oanda_supervisor_completion_health.py': (AREA / 'supervisor_health_candidate_v2/test_completion_health.py').read_bytes(),
             'test_oanda_retained_price_settlement_v1.py': (AREA / 'settlement_idle_candidate_v2/test_oanda_retained_price_settlement_v1.py').read_bytes()}
    TARGET.mkdir()
    prior = TARGET / 'retained_prior'
    prior.mkdir()
    for name, raw in files.items():
        path = TARGET / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        if (ROOT / name).is_file():
            old = prior / name
            old.parent.mkdir(parents=True, exist_ok=True)
            old.write_bytes((ROOT / name).read_bytes())
    (prior / 'operational_runtime_v3_20260913.json').write_bytes(profile_raw)
    (TARGET / 'M1_SOURCE_MANIFEST.json').write_bytes(manifest_raw)
    receipt = dict(status='prepared_not_deployed', profile=PROFILE,
                   files={name: sha(raw) for name, raw in files.items()},
                   original_files={name: sha((ROOT / name).read_bytes()) for name in files if (ROOT / name).is_file()},
                   original_profile_sha256=sha(profile_raw), native_generation_unchanged=True,
                   practice_configuration_unchanged=True, source_cohorts_unchanged=True)
    (TARGET / 'CANDLES_HANDOVER.json').write_bytes(encoded(receipt))
    print(json.dumps(receipt))


if __name__ == '__main__':
    prepare()
