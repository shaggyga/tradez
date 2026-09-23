"""One-shot process continuity and read-only stopped-companion SQLite backups."""
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import time

OUT = Path(__file__).resolve().parent
TRAD = OUT.parent/'trad'
DATA = TRAD/'data/oanda_training_manager'
BEFORE = OUT/'RUNTIME_BEFORE_RELOAD.json'
REPORT = OUT/'RUNTIME_CONTINUITY_AND_RETIRED_BACKUPS_20260907.json'
BACKUP = OUT/'retired_companions'
RETIRED = {'oanda_causal_forecast_study_gap_v2.py':'causal_forecast_study_gap_v2',
           'oanda_causal_forecast_study_eurusd_v1.py':'causal_forecast_study_eurusd_v1'}
SUPERVISOR = 'oanda_always_on_supervisor.ps1'
DASHBOARD = 'oanda_practice_live_dashboard.py'


def utc():
    return datetime.now(timezone.utc).isoformat()


def binding(path):
    raw = path.read_bytes()
    stat = path.stat()
    return {'path':str(path.resolve()),'sha256':hashlib.sha256(raw).hexdigest(),
            'bytes':len(raw),'mtime_ns':stat.st_mtime_ns}


def processes():
    ps = r'''$ErrorActionPreference='Stop'
Get-CimInstance Win32_Process | Where-Object {$_.Name -match '^(python.*|powershell)\.exe$'} | Select-Object ProcessId,ParentProcessId,Name,CreationDate,CommandLine | ConvertTo-Json -Depth 4 -Compress'''
    raw = subprocess.check_output(['powershell.exe','-NoProfile','-NonInteractive','-Command',ps],text=True,timeout=15)
    rows = json.loads(raw)
    kept = []
    for row in rows if isinstance(rows,list) else [rows]:
        cmd = row.get('CommandLine') or ''
        if str(TRAD).lower() not in cmd.lower():
            continue
        match = re.search(r'(?:^|[\\/"\s])(oanda_[a-zA-Z0-9_]+\.(?:py|ps1))(?=["\s]|$)',cmd)
        if not match:
            continue
        script = match[1]
        if script == SUPERVISOR and not re.search(r'\s-File\s',cmd,re.I):
            continue
        created = row['CreationDate']
        if isinstance(created,str) and created.startswith('/Date('):
            ms = int(re.search(r'/Date\((-?\d+)',created)[1])
            created = datetime.fromtimestamp(ms/1000,timezone.utc).isoformat()
        kept.append({key:row[key] for key in ('ProcessId','ParentProcessId')} |
                    {'CreationDate':created,'script':script})
    return {'observed_utc':utc(),'processes':sorted(kept,key=lambda r:r['ProcessId'])}


def identity(row):
    dt = datetime.fromisoformat(row['CreationDate'].replace('Z','+00:00'))
    return row['ProcessId'], int(dt.timestamp()*1000)


def grouped(snapshot):
    answer = {}
    for row in snapshot['processes']:
        answer.setdefault(row['script'],[]).append(row)
    return answer


def continuity(before, after):
    old,new = grouped(before),grouped(after)
    expected = set(old)-set(RETIRED)-{SUPERVISOR}
    assert set(new)-{SUPERVISOR} == expected, ('worker_set',sorted(new),sorted(expected))
    assert len(expected) == 13
    assert all(len(new[name])==2 for name in expected), 'launcher_child_count'
    kept = expected-{DASHBOARD}
    assert all({identity(r) for r in old[name]} == {identity(r) for r in new[name]} for name in kept), 'retained_process_identity_changed'
    assert not set(RETIRED)&set(new)
    assert len(new[SUPERVISOR])==1 and new[SUPERVISOR][0]['ProcessId']==45324
    assert not {identity(r) for r in old[DASHBOARD]}&{identity(r) for r in new[DASHBOARD]}
    assert not {identity(r) for r in old[SUPERVISOR]}&{identity(r) for r in new[SUPERVISOR]}
    return {'research_worker_count':len(expected),'python_launcher_and_child_count':sum(len(new[n]) for n in expected),
        'preserved_worker_count':len(kept),'preserved_python_pid_count':sum(len(new[n]) for n in kept),
        'preserved_workers':{name:[r['ProcessId'] for r in new[name]] for name in sorted(kept)},
        'retired_absent':sorted(RETIRED),'new_supervisor':new[SUPERVISOR], 'new_dashboard':new[DASHBOARD]}


def database_facts(connection):
    tables = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    assert all(re.fullmatch(r'[A-Za-z0-9_]+',name) for name in tables)
    counts = {name:connection.execute('SELECT COUNT(*) FROM "'+name+'"').fetchone()[0] for name in tables}
    sha = hashlib.sha256()
    for line in connection.iterdump():
        sha.update(line.encode()); sha.update(b'\n')
    return {'tables':counts,'logical_sql_dump_sha256':sha.hexdigest()}


assert not REPORT.exists() and not BACKUP.exists(), 'new_evidence_paths_required'
started = utc()
result = {'schema_version':'v2_runtime_continuity_retired_backup_audit_20260907','status':'running',
    'started_utc':started,'before_evidence':binding(BEFORE),'helper':binding(Path(__file__)),
    'backups':[],'errors':[],'broker_requests':0,'process_mutations':0,
    'source_database_connection_mode':'mode=ro; query_only=ON; BEGIN read snapshot',
    'writes_scope':'New evidence and backup files under this external workspace only.'}
try:
    before = json.loads(BEFORE.read_bytes())
    result['first_process_snapshot'] = processes()
    result['initial_continuity'] = continuity(before,result['first_process_snapshot'])
    BACKUP.mkdir()
    for script,stem in RETIRED.items():
        # Re-check immediately before opening either retired source database.
        check = processes()
        assert not set(RETIRED)&set(grouped(check)), 'retired_worker_reappeared_before_backup'
        source_root = DATA/stem
        destination = BACKUP/stem
        destination.mkdir()
        database = source_root/'study.sqlite'
        physical = [database,Path(str(database)+'-wal')]
        before_bytes = [binding(path) for path in physical if path.exists()]
        copied = []
        for path in (source_root/'heartbeat.json',source_root/'scorecard.json',TRAD/'config'/f'{stem}_20260907.json'):
            target = destination/path.name
            initial = binding(path)
            shutil.copy2(path,target)
            assert binding(target)['sha256'] == initial['sha256'] == binding(path)['sha256']
            copied.append({'source':initial,'copy':binding(target)})
        target = destination/'study.sqlite'
        target.touch(exist_ok=False)
        deadline = time.monotonic()+30
        def progress(status,remaining,total):
            if time.monotonic()>deadline:
                raise TimeoutError('bounded_sqlite_backup_deadline')
        with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=5)) as source:
            source.execute('PRAGMA query_only=ON')
            source.execute('BEGIN')
            source_facts = database_facts(source)
            with closing(sqlite3.connect(target)) as dest:
                source.backup(dest,pages=128,progress=progress,sleep=.01)
                assert dest.execute('PRAGMA quick_check').fetchone()[0]=='ok'
                backup_facts = database_facts(dest)
            source.rollback()
        assert source_facts == backup_facts, 'backup_logical_snapshot_mismatch'
        after_bytes = [binding(Path(item['path'])) for item in before_bytes]
        assert before_bytes == after_bytes, 'retired_source_database_or_wal_changed'
        result['backups'].append({'study':stem,'process_absence_observed_utc':check['observed_utc'],
            'source_database_and_wal_before':before_bytes,'source_database_and_wal_after':after_bytes,
            'sqlite_backup':binding(target),'source_facts':source_facts,'backup_facts':backup_facts,
            'integrity_check':'ok','copied_metadata':copied})
    result['last_process_snapshot'] = processes()
    result['final_continuity'] = continuity(before,result['last_process_snapshot'])
    result['registry_source_checks'] = []
    for name in ('pair_local_forecast_study_v1_20260907.json','pair_local_forecast_study_v2_20260907.json'):
        path = TRAD/'config'/name
        registry = json.loads(path.read_bytes())
        checks = [dict(binding(TRAD/source),expected_sha256=sha) for source,sha in registry['source_bindings'].items()]
        assert len(checks)==9 and all(c['sha256']==c['expected_sha256'] for c in checks)
        result['registry_source_checks'].append({'registry':binding(path),'bindings':checks})
    result['status']='passed'
except Exception as exc:
    result['status']='failed'
    result['errors'].append(type(exc).__name()+': '+str(exc))
finally:
    result['finished_utc']=utc()
    result['scope_limit']='A bounded post-reload continuity and stopped-study backup observation, not a claim of ongoing uptime or forecast performance. SQLite WAL shared-memory reader metadata is not forecast evidence; only source DB and WAL byte/mtime stability are asserted.'
    with REPORT.open('x',encoding='utf-8') as handle:
        json.dump(result,handle,indent=2,sort_keys=True,allow_nan=False);handle.write('\n')
print(json.dumps({'status':result['status'],'report':binding(REPORT),'errors':result['errors'],
                  'continuity':result.get('final_continuity',result.get('initial_continuity'))},indent=2))
raise SystemExit(0 if result['status']=='passed' else 1)
