"""Freeze a verified collection restart receipt; retain older stopped records."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import shutil

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
def load(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
obs = load(OUT / 'observation_verified.json')
review = load(OUT / 'gate_review_result.json')
assert review['status'] == 'passed'
assert obs['dashboard_http_status'] == 200 and obs['supervisor_error_count'] == 0
assert len(obs['managed_running']) == 11
assert all(x['state'] == 'Disabled' for x in obs['forex_tasks'])
assert obs['quote_stream']['can_place_orders'] is False
assert obs['quote_stream']['stream']['connected'] is True
assert obs['account']['aggregate']['openTradeCount'] == 0
assert obs['account']['aggregate']['pendingOrderCount'] == 0
assert all(not load(ROOT / 'config' / n)['collection_enabled'] for n in [
    'source_factor_response_v9.json', 'source_conditioned_currency_rank_v8.json'])
evidence = ROOT / 'docs/validation/restart_20260906'
evidence.mkdir(parents=True, exist_ok=True)
names = ['observe.ps1', 'observation_verified.json', 'stopped_before_restart.json',
         'review_gate.ps1', 'gate_review_result.json', 'startup_regression_tests.xml',
         'runtime_review.json', 'integration_tests.xml']
for name in names:
    target = evidence / name
    with target.open('xb') as dest: dest.write((OUT / name).read_bytes())
when = obs['observed_utc']
report = f'''# Forex collection restart — September 6, 2026

Forex restarted at 12:16:44 UTC following the user's authorization to turn it
on for today's market opening. Verification timestamp: {when}.
This supersedes the earlier keep-stopped instruction for collection only.

One supervisor runs the new `-ResearchCollectionOnly` mode. Eleven workers
provide practice-account reads, localhost dashboard, quote streaming, news
collection/mapping, source-governance fastlane V3, M1 candle updates, clock
monitoring, storage monitoring and the existing integrity audit. The dashboard
responds at http://127.0.0.1:8765/ and the OANDA practice quote stream is connected.
The practice account has zero open trades and zero pending orders.

The allow-list excludes all execution, model production, calibration, outcome
production, promotion and lifecycle workers. An unknown future worker is also
blocked. Source V9 and rank V8 remain disabled. The existing broad safe-core
launcher includes practice execution, so it was not used. Both existing Forex
scheduled tasks remain disabled; no watchdog or new scheduled task was enabled.
The supervisor restarts its admitted children while this session is running.
Restart after a computer reboot requires the collection launcher again.

The dedicated quote stream now owns the canonical quote snapshot and records
its own producer identity. The previous JSON and SQLite/WAL/SHM were copied
and hashed before launch. Broker quote timestamps remain original; all 68
observed weekend quotes are nontradeable. Connection health does not establish
fresh executable market prices. The broader integrity audit remains an
unrelaxed diagnostic with deliberately absent inputs; this receipt is not a
passing full-project integrity or trading-readiness certificate. At the
verification timestamp its initial refresh was still running and the prior
saved integrity publication remained historical.

Fastlane V3 is alive and committing scan progress through retained input.
Its initial scans rejected preactivation rows and created no new prospective
receipts. Successful new-event acceptance remains unverified. The two open
prediction-clock findings and historical evaluation results are unchanged.

Validation: 20 offline startup/quote/watchdog and 21 vault-sync regression tests passed. An
independent isolated PowerShell AST/function review tested 107 blocked worker
names, the exact eleven-name allow-list, existing-disallowed-worker handling,
quote ownership, and the sole guarded process-launch path. Live process ancestry,
heartbeats, dashboard response and account reads were also checked. Windows
virtual-environment launcher/child pairs are not duplicate independent workers.

OANDA US lists the Sunday reopening at 17:05 New York time (21:05 UTC today),
except TRY pairs, and regular FX hours for September 6 and 7:
[regular hours](https://www.oanda.com/us-en/trading/hours-of-operation/),
[holiday hours](https://www.oanda.com/us-en/trading/holiday-trading-hours/).

Start command from the parent Forex directory:

```powershell
.\\trad\\start_oanda_research_collection.ps1
```

Read `FOREX_RESEARCH_RESTART_VALIDATION_20260906.json` for hashes and observations.
Prior dated audit/repair/evaluation receipts remain historical. The original
saved large integrity snapshot is retained in `../restart_20260906/before/`.
'''
report_path = ROOT / 'docs/FOREX_RESEARCH_RESTART_20260906.md'
with report_path.open('x', encoding='utf-8') as f: f.write(report)
runtime_note = '''Current runtime — September 6, 2026: Forex has restarted in collection-only
mode. Eleven data/dashboard/health workers run; execution and model/proof
production remain off. See [the restart record](docs/FOREX_RESEARCH_RESTART_20260906.md)
and `FOREX_RESEARCH_RESTART_VALIDATION_20260906.json`. Earlier stopped-state
observations below describe their recorded times and are now historical.

'''
for name in ['README.md', 'FOREX_AUDIT_START_HERE.md']:
    p = ROOT / name
    text = p.read_text(encoding='utf-8')
    head, rest = text.split('\n', 1)
    p.write_text(head + '\n\n' + runtime_note + rest.lstrip('\n'), encoding='utf-8')
p = ROOT / 'docs/AUDIT_STATE_CURRENT.md'
text = p.read_text(encoding='utf-8'); head, rest = text.split('\n', 1)
p.write_text(head + '\n\n' + runtime_note.replace('(docs/', '(') + rest.lstrip('\n'), encoding='utf-8')
p = ROOT / 'FOREX_PROJECT_LOG.md'
text = p.read_text(encoding='utf-8'); pos = text.index('\n## ')
entry = '''
## 2026-09-06 — authorized collection restart

The user authorized Forex to be on for today's opening. Added a closed
eleven-worker ResearchCollectionOnly allow-list and a dedicated hidden launcher.
The prior safe-core launcher still admits practice execution and was not used.
No executor, model/proof producer, calibration, lifecycle or promotion worker
was resumed. Source V9/rank V8 and the two existing Forex tasks stay disabled.
The dedicated read-only stream owns the canonical quote snapshot with its own
producer identity; pre-restart quote and integrity evidence is preserved.
Twenty regression tests and an independent 107-name exclusion check passed.
Live verification found the dashboard responding, quote stream connected,
fresh collection heartbeats and zero practice trades/orders. Full integrity
and new prospective event acceptance are not certified by this startup check.
See docs/FOREX_RESEARCH_RESTART_20260906.md and the corresponding JSON receipt.

'''
p.write_text(text[:pos] + '\n' + entry + text[pos:], encoding='utf-8')
bound = ['oanda_always_on_supervisor.ps1', 'start_oanda_research_collection.ps1',
         'forex_model_vault_sync.py', 'docs/FOREX_RESEARCH_RESTART_20260906.md']
bound += [f'docs/validation/restart_20260906/{name}' for name in names]
receipt = {
    'schema_version': 'forex_research_restart_validation_v1',
    'generated_utc': datetime.now(timezone.utc).isoformat(),
    'status': 'collection_running_execution_excluded',
    'authorization': 'User authorized Forex on for today; supersedes keep-stopped for research collection.',
    'started_utc': '2026-09-06T12:16:44Z', 'observed': obs,
    'full_integrity_pass_claimed': False, 'prediction_quality_improved_claimed': False,
    'initial_integrity_publication_pending_at_observation': True,
    'source_v9_enabled': False, 'rank_v8_enabled': False,
    'scheduled_tasks_enabled': False, 'new_automation_created': False,
    'startup_regression_tests_passed': 20, 'isolated_gate_review': review,
    'vault_sync_regression_tests_passed': 21,
    'prior_integrity_and_quote_backups': load(OUT/'before/manifest.json'),
    'prior_quote_sqlite_backups': load(OUT/'before/quote_sqlite_manifest.json'),
    'bindings': [{'path': n, 'sha256': sha(ROOT/n)} for n in bound],
    'market_hours': {'today': '2026-09-06', 'fx_except_try_reopens_new_york': '17:05',
        'reopens_utc': '21:05', 'holiday_exception': 'regular FX hours'},
    'limitations': ['Closed market: no fresh executable Sunday-open quotes verified.',
        'Fastlane scan progress is not new prospective event acceptance.',
        'Full-project integrity findings and missing inputs remain unresolved.']
}
with (ROOT/'FOREX_RESEARCH_RESTART_VALIDATION_20260906.json').open('x', encoding='utf-8') as f:
    json.dump(receipt, f, indent=2); f.write('\n')
print(json.dumps({'status': receipt['status'], 'workers': len(obs['managed_running']), 'receipt': sha(ROOT/'FOREX_RESEARCH_RESTART_VALIDATION_20260906.json')}))
