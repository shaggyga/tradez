from pathlib import Path
import json
import re

ROOT = Path(__file__).resolve().parent.parent / 'trad'
path = ROOT / 'oanda_always_on_supervisor.ps1'
text = path.read_text(encoding='utf-8-sig')

def retired_block(name):
    script = 'oanda_' + name + '.py'
    return f'''        # Preserve the old ledger as diagnostics; future collection uses a new contract.
        $retiredPublicationWorker = @(Get-MatchingPython -Needle "{script}")
        if ($retiredPublicationWorker.Count -gt 0) {{
            Stop-MatchingPython `
                -Name "{name}_preserved" `
                -Needle "{script}" `
                -Processes $retiredPublicationWorker `
                -Reason "publication_availability_contract_20260905"
        }}
'''

retired = ['causal_source_factor_response_map_v7', 'causal_source_factor_response_map_v8',
           'source_conditioned_currency_rank_v6', 'source_conditioned_currency_rank_v7']
for name in retired:
    pattern = r'        \$managed \+= Start-ManagedProcess `\n            -Name "' + name + r'".*?\n            }\n'
    text, count = re.subn(pattern, lambda _: retired_block(name), text, count=1, flags=re.S)
    assert count == 1, name
text = text.replace('''        # V7 remains the frozen V152 baseline. V8 is a parallel exact-source
        # taxonomy overlay, not a rewrite or replacement of V7 evidence.
''', '''        # V7/V8 are frozen diagnostics after the publication-availability audit.
''')
text = text.replace('''        # V8 source forecasts and cannot route or authorize.
''', '''        # V9 source forecasts and cannot route or authorize.
''')
text = text.replace('''        # Keep V6/V7 as a frozen baseline while V7/V8 measures the narrow
        # market-structure taxonomy overlay as a separate comparison cohort.
''', '''        # V6/V7 rank ledgers remain frozen diagnostics with their original clocks.
''')

anchor = '        # Test whether already-known news direction becomes economically useful'
assert text.count(anchor) == 1
blocks = '''        # These manifests ship disabled. An enabled source must separately hold
        # its hash-bound future activation receipt; neither CLI creates one.
        $publicationSourceConfig = Read-JsonFileWithRetry -LiteralPath (Join-Path $Trad "config\\source_factor_response_v9.json")
        $publicationRankConfig = Read-JsonFileWithRetry -LiteralPath (Join-Path $Trad "config\\source_conditioned_currency_rank_v8.json")
        if ($publicationSourceConfig.collection_enabled -eq $true) {
            $managed += Start-ManagedProcess `
                -Name "causal_source_factor_response_map_v9" `
                -Needle "oanda_causal_source_factor_response_map_v9.py" `
                -PriorityClass "BelowNormal" `
                -Arguments @(
                    (Join-Path $Trad "oanda_causal_source_factor_response_map_v9.py"),
                    "--config", (Join-Path $Trad "config\\source_factor_response_v9.json"),
                    "--interval-sec", "5", "--duration-sec", "$ChildDurationSec"
                ) `
                -Freshness @{
                    LiteralPath = (Join-Path $DataRoot "local_news_sentiment\\causal_source_factor_response_map_latest_v9.json")
                    MaxAgeSec = 180
                    StartupGraceSec = 600
                    ExpectedJsonField = "contract_id"
                    ExpectedJsonValue = "causal_source_factor_response_map_v9_publication_availability_20260905"
                }
        }
        if (($publicationSourceConfig.collection_enabled -eq $true) -and ($publicationRankConfig.collection_enabled -eq $true)) {
            $managed += Start-ManagedProcess `
                -Name "source_conditioned_currency_rank_v8" `
                -Needle "oanda_source_conditioned_currency_rank_v8.py" `
                -StartupDelaySec 10 `
                -PriorityClass "BelowNormal" `
                -Arguments @(
                    (Join-Path $Trad "oanda_source_conditioned_currency_rank_v8.py"),
                    "--config", (Join-Path $Trad "config\\source_conditioned_currency_rank_v8.json"),
                    "--interval-sec", "5", "--duration-sec", "$ChildDurationSec"
                ) `
                -Freshness @{
                    LiteralPath = (Join-Path $State "source_conditioned_currency_rank_v8.json")
                    MaxAgeSec = 180
                    StartupGraceSec = 600
                    ExpectedJsonField = "contract_id"
                    ExpectedJsonValue = "source_conditioned_currency_rank_v8_observed_publication_entry_20260905"
                }
        }
'''
text = text.replace(anchor, blocks + anchor)
text = text.replace('source_governance_news_fast_lane_v2.json', 'source_governance_news_fast_lane_v3.json')
text = text.replace('news_source_governance_fast_lane_v2_first_seen_prospective_20260901',
                    'news_source_governance_fast_lane_v3_committed_visibility_20260905')
path.write_text(text, encoding='utf-8', newline='\n')

path = ROOT / 'config/shadow_runtime_retirements_v1.json'
policy = json.loads(path.read_text(encoding='utf-8'))
policy['updated_utc'] = '2026-09-05T20:00:00Z'
for name in retired:
    source = name.startswith('causal_')
    successor = 'causal_source_factor_response_map_v9' if source else 'source_conditioned_currency_rank_v8'
    directory = 'local_news_sentiment' if source else 'state'
    snapshot = name.replace('response_map_v', 'response_map_latest_v') if source else name
    policy['retired_collectors'].append({
        'name':name,'script':'oanda_'+name+'.py', 'state':'superseded_publication_clock_diagnostics_preserved',
        'runtime_action':'disabled',
        'reason':'The prior issue/entry clock can precede consumer-visible publication. Preserve this contract and ledger as diagnostics; no rows are imported or credited as repaired prospective proof.',
        'successor':{'script':'oanda_'+successor+'.py','collection_enabled':False,
                     'activation':'separate_future_hash_bound_receipt_required'},
        'preserved_artifacts':['oanda_'+name+'.py',f'data/oanda_training_manager/{directory}/{name}.sqlite',
                               f'data/oanda_training_manager/{directory}/{snapshot}.json']})
path.write_text(json.dumps(policy, indent=2)+'\n',encoding='utf-8')

path = ROOT / 'test_oanda_shadow_runtime_retirements.py'
text = path.read_text(encoding='utf-8')
needle = '                "causal_source_factor_response_map_v5",'
text = text.replace(needle, needle+'\n'+''.join(f'                "{name}",\n' for name in retired).rstrip())
path.write_text(text,encoding='utf-8')

for test, name, versions in [
    ('test_oanda_causal_source_factor_response_map_v8.py','causal_source_factor_response_map',[7,8]),
    ('test_oanda_source_conditioned_currency_rank_v7.py','source_conditioned_currency_rank',[6,7])]:
    path = ROOT/test
    text = path.read_text(encoding='utf-8')
    pattern = r'def test_supervisor_[^\n]+\n.*?(?=\n\ndef |\Z)'
    matches = list(re.finditer(pattern,text,re.S))
    assert len(matches)==1, test
    replacement = f'''def test_supervisor_preserves_retired_{name}_diagnostics():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(encoding="utf-8")
    for version in {versions!r}:
        name = f"{name}_v{{version}}"
        assert f'-Name "{{name}}"' not in supervisor
        assert f'-Name "{{name}}_preserved"' in supervisor
        assert f'-Needle "oanda_{{name}}.py"' in supervisor
'''
    text = text[:matches[0].start()] + replacement + text[matches[0].end():]
    path.write_text(text,encoding='utf-8')
print('Updated supervisor, retirement manifest and historical supervisor assertions; no process launched.')
