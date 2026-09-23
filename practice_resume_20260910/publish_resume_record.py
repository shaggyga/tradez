"""Publish dated recovery evidence without replacing earlier audit records."""
import datetime as dt
import hashlib
import json
import shutil
from pathlib import Path

BASE = Path(r'C:\Users\zmoor\Documents\forex')
ROOT = BASE / 'trad'
OUT = BASE / 'practice_resume_20260910'
RECOVERY = BASE / 'practice_recovery_20260910'
VAULT = Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')
PUB = OUT / 'publication'
PUB.mkdir(exist_ok=False)
BEFORE = PUB / 'before'
BEFORE.mkdir()
result = json.loads((OUT / 'RESUME_FINAL_VERIFIED_20260910.json').read_text(encoding='utf-8'))
stamp = dt.datetime.fromtimestamp(result['completed_epoch'], dt.timezone.utc).isoformat()

report = f'''# Practice007 resumed and Windows sign-in recovery — September 10, 2026

The user explicitly requested that trading be enabled. The original finite OANDA Practice007 trial and research services were restarted, and the broker confirmed new practice fills. Final independent GET-only observation completed **{stamp}**. This later observation supersedes the September 9 reboot/stopped status; it does not revise the earlier six-trade loss review.

At that observation, worker 11988 was managing an open **NZD/JPY short, 283 units**, broker trade 2493, entry 89.395. Broker-held stop 2494 was pending at 89.474. The account had one open trade and one pending protective order, balance **USD 41.1189**, marked NAV **USD 41.0233**. These are timestamped values, not a promise of subsequent position state or performance. The first resumed USD/CHF long was independently confirmed earlier; by this final observation the account balance had fallen another USD 0.1678 from the previous review's USD 41.2867.

The [final runtime and broker receipt](../../practice_resume_20260910/RESUME_FINAL_VERIFIED_20260910.json) retains the enabled config validation, separate account/trade/order observations, worker/watchdog/bootstrap status and native Windows task observation. Worker status was about four seconds old when printed; entry and session-loss halts were false. The two Python PIDs shown by Windows are the virtual-environment launcher and its actual worker, not two independent trading workers.

## Recovery installed

The current-user Windows task **Forex Practice Research Recovery 20260910** starts a hidden recovery guard 30 seconds after sign-in. It is enabled, uses limited interactive privileges, allows only one task instance and has no execution-time limit. Task Scheduler was used to start the installed action; the running guard observed one correct research supervisor, one practice watchdog and the existing Python worker without creating duplicates. Native task result 267009 (0x41301) means the task is currently running.

This is application sign-in recovery. No Codex scheduled monitoring or chat watch was created. A real reboot was not performed during validation, and services cannot be assumed to start before Windows sign-in. Earlier disabled broad/legacy trading tasks remain disabled.

The new guard validates the exact source files, original config bytes and finite deadline before recovery. It invokes the existing research-only launcher and original practice watchdog, leaves surviving workers alone, refuses conflicting supervisors, and preserves the ledger, baseline, stop requests and loss latches. It never restarts research after the cutoff; the original practice runner may still resume solely to finish required closes until it confirms flat. The guard exits on completed-flat status.

## Existing trial contract retained

The model, forecast adapter, execution policy, broker adapter, runner, research registrations and study authorities were not replaced. Research studies retain their research-only authority; the user-authorized separate Practice007 worker executes admitted forecasts. The actual route is OANDA practice, not a real-money account.

The original USD 41.6042 baseline remains intact. Limits remain one position, 0.5% NAV modeled stop risk, 20% NAV entry margin, eight claimed attempts per UTC day, and a persistent session-loss stop of the lesser of 10% original NAV or USD 5. Entries still require fresh eligible forecasts and cost/risk checks. The original H1 target and attached protective stop remain the position exit contract. The final cutoff remains **Friday September 11, 2026, 20:45 UTC / 4:45 p.m. Eastern**, with latest admitted target at 20:40 UTC. Resuming the process did not reset the account, loss history, limits or deadline.

## Validation and retained evidence

- **29 native Windows PowerShell checks passed** for the new bootstrap, including exact process selection, surviving-worker adoption, duplicate refusal, source/config binding, fixed cutoff, read-only modes and Windows file replacement during reads.
- Independent source review found no remaining blocker. It binds the exact reviewed source and tests to the 29-check receipt. The earlier 286 practice-component tests remain earlier evidence; no new model-performance validation is claimed.
- Actual validation includes current broker GETs and invoking the installed task action against already-running services. Missing-service/reboot decisions were tested in native fixtures; the active trading worker was not killed for a recovery test.
- [Task installation receipt](../../practice_resume_20260910/LOGON_RECOVERY_INSTALLED_20260910.json), [installed task XML](../../practice_resume_20260910/RECOVERY_TASK_INSTALLED.xml), [first action verification](../../practice_resume_20260910/RECOVERY_ACTION_VERIFIED_001.json).
- [Bootstrap implementation](../../practice_recovery_20260910/RECOVERY_BOOTSTRAP_IMPLEMENTATION_20260910.md), [native test receipt](../../practice_recovery_20260910/native_checks/NATIVE_CHECKS_20260910T150411882.json), [independent source review](../../practice_recovery_20260910/RECOVERY_BOOTSTRAP_INDEPENDENT_REVIEW_20260910.json).

Canonical bootstrap SHA-256: `c027f122be82631f0eae518f79c2bc470c5f0b505de1738eebd61d709fea0fd6`. Recovery manifest SHA-256: `5a1347797fc45427875dff1530242f3602e5db7a9e2cbe1ff5d5daf8fd4f6e46`. Original trial config raw SHA-256 remains `ae2901395ba117b2df7a6d13670d48cb3d4f10963bd3b983d6b3775be90b5af8`.

Prediction quality, realistic exit spreads, matched H1/stop/remaining-risk comparisons and currency-factor concentration remain pending research. Restoring operation does not resolve the earlier losses or establish profitability. The user's planned end-of-week review should reconcile later broker outcomes and refused attempts with retained original decisions.

This standalone publication preserves all earlier reports and source snapshots. Its own publication receipt inventories the added files and changed documentation; older shared vault maps/ZIPs retain their earlier cutoffs.
'''
doc = ROOT / 'docs/FOREX_PRACTICE_RESUME_20260910.md'
assert not doc.exists()
doc.write_text(report, encoding='utf-8')

copies = [
    OUT / 'RESUME_FINAL_VERIFIED_20260910.json',
    OUT / 'BROKER_RESUMED_TRIAL_VERIFIED_20260910.json',
    OUT / 'LOGON_RECOVERY_INSTALLED_20260910.json',
    OUT / 'RECOVERY_TASK_INSTALLED.xml',
    OUT / 'RECOVERY_TASK_FINAL_20260910.json',
    OUT / 'RECOVERY_ACTION_VERIFIED_001.json',
    RECOVERY / 'RECOVERY_BOOTSTRAP_IMPLEMENTATION_20260910.md',
    RECOVERY / 'RECOVERY_BOOTSTRAP_INDEPENDENT_REVIEW_20260910.json',
    RECOVERY / 'native_checks/NATIVE_CHECKS_20260910T150411882.json',
    ROOT / 'start_oanda_practice_recovery_v1.ps1',
    ROOT / 'test_start_oanda_practice_recovery_v1.ps1',
    ROOT / 'config/practice_recovery_launcher_20260910.json',
]
evidence = VAULT / 'PRACTICE_RESUME_20260910_EVIDENCE'
evidence.mkdir(exist_ok=False)
for source in copies:
    shutil.copy2(source, evidence / source.name)
vault_report = report.replace('../../practice_resume_20260910/', 'PRACTICE_RESUME_20260910_EVIDENCE/').replace(
    '../../practice_recovery_20260910/native_checks/', 'PRACTICE_RESUME_20260910_EVIDENCE/').replace(
    '../../practice_recovery_20260910/', 'PRACTICE_RESUME_20260910_EVIDENCE/')
vault_doc = VAULT / 'PRACTICE_RESUME_20260910.md'
assert not vault_doc.exists()
vault_doc.write_text(vault_report, encoding='utf-8')

changes = []
def prepend_after_title(path, insertion, backup_name):
    old = path.read_bytes()
    (BEFORE / backup_name).write_bytes(old)
    newline = b'\r\n' if b'\r\n' in old else b'\n'
    at = old.index(b'\n') + 1
    block = (insertion.strip() + '\n\n').encode('utf-8').replace(b'\n', newline)
    new = old[:at] + newline + block + old[at:]
    path.write_bytes(new)
    assert new[:at] + new[at + len(newline) + len(block):] == old
    changes.append({'path': str(path), 'before_sha256': hashlib.sha256(old).hexdigest(),
                    'after_sha256': hashlib.sha256(new).hexdigest(), 'operation': 'prepend dated update after title'})

prepend_after_title(ROOT / 'README.md', '**September 10 — practice trading resumed:** [Current resume record](docs/FOREX_PRACTICE_RESUME_20260910.md). Research and the original Practice007 worker are running again; fresh broker reads confirm a new position with its protective stop. Windows sign-in recovery is installed and its task action was verified; 29 native checks and independent source review passed. Existing risk limits, loss history and Friday September 11, 4:45 p.m. Eastern cutoff remain intact. Earlier stopped/flat status below is dated history.', 'project_README.md')
prepend_after_title(ROOT / 'FOREX_PENDING_IMPROVEMENTS.md', '## Current — September 10 practice resume\n\n[Resume and recovery record](docs/FOREX_PRACTICE_RESUME_20260910.md): the research supervisor and original Practice007 worker are running, with a fresh broker-confirmed NZD/JPY position and attached stop at the recorded cutoff. Missing sign-in recovery is resolved by a source-bound guard and current-user Windows logon task; 29 native checks and independent source review passed, and the installed action was exercised without duplicate workers. Pre-login startup was not added or reboot-tested.\n\nThe Friday cutoff, loss history and existing risk/forecast gates remain unchanged. Still pending: realistic exit-spread research, matched H1/stop/remaining-risk comparisons, currency-factor concentration, and the end-of-week broker/forecast review. The older missing-recovery statement below describes the September 9 observation.', 'FOREX_PENDING_IMPROVEMENTS.md')
prepend_after_title(VAULT / 'README.md', '**September 10 — practice trading resumed:** [Current resume and recovery](PRACTICE_RESUME_20260910.md) supersedes the September 9 stopped-status observation. Research and the original Practice007 trial are running again; fresh broker reads confirm an open position and attached stop. Current-user Windows sign-in recovery is installed and its action verified, with 29 native checks and independent source review. The original limits, loss history and Friday September 11, 4:45 p.m. Eastern cutoff remain. Added evidence is standalone; earlier source snapshots/maps retain their original cutoffs.', 'vault_README.md')
log = ROOT / 'FOREX_PROJECT_LOG.md'
old = log.read_bytes()
(BEFORE / 'FOREX_PROJECT_LOG.md').write_bytes(old)
entry = f'\n\n## {stamp} — user-requested Practice007 resume and sign-in recovery\n\nRestored the research supervisor and original finite practice watchdog/worker after explicit user instruction to enable trading. Independent practice GETs confirmed resumed USD/CHF execution and, at final observation, NZD/JPY short283 with broker-held stop; balance USD41.1189, marked NAV41.0233. These later losses/marks remain recorded, not reset. The original baseline, source/config, risk limits and Friday20:45UTC cutoff are unchanged.\n\nAdded source-bound hidden recovery guard and current-user Windows sign-in task, then invoked its installed action and verified existing services without duplication.29nativePowerShellchecks and independent source review passed; no actual reboot was performed. No Codex monitoring was scheduled. Canonical narrative: docs/FOREX_PRACTICE_RESUME_20260910.md; exact evidence and publication: C:/Users/zmoor/Documents/forex/practice_resume_20260910. Earlier reports/snapshots remain preserved; prediction/cost/management research gaps remain open.\n'
entry = entry.replace('Friday20:45UTC', 'Friday 20:45 UTC').replace('duplication.29nativePowerShellchecks', 'duplication. 29 native PowerShell checks')
log.write_bytes(old + entry.encode('utf-8'))
changes.append({'path': str(log), 'before_sha256': hashlib.sha256(old).hexdigest(),
                'after_sha256': hashlib.sha256(log.read_bytes()).hexdigest(), 'operation': 'append dated entry'})

files = [doc, vault_doc, *evidence.iterdir(), *copies, OUT / 'capture_final_resume.py', OUT / 'install_logon_recovery.ps1']
receipt = {'published_utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'changed_documents': changes,
           'files': [{'path': str(p), 'bytes': p.stat().st_size,
                      'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in files],
           'scope': 'Dated standalone recovery publication; historical source/maps/reports unchanged.'}
(PUB / 'RESUME_PUBLICATION_20260910.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
shutil.copy2(PUB / 'RESUME_PUBLICATION_20260910.json', evidence / 'RESUME_PUBLICATION_20260910.json')
print(json.dumps({'report': str(doc), 'vault_report': str(vault_doc), 'changed_documents': len(changes),
                  'inventoried_files': len(files), 'publication_receipt': str(PUB / 'RESUME_PUBLICATION_20260910.json')}, indent=2))
