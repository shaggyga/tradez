"""Read-only inventory of the fixed four proof cohorts; no project imports."""
from pathlib import Path
import datetime as dt
import hashlib
import json
import sqlite3
import time

ROOT = Path(__file__).resolve().parents[2] / 'trad'
OUT = Path(__file__).resolve().parent
STATE = ROOT / 'data/oanda_training_manager/state'
DB = STATE / 'strategy_shadow_outcomes_v1.sqlite'

def stat(path):
    if not path.exists(): return {'exists': False}
    s = path.stat()
    return {'exists': True, 'size': s.st_size, 'mtime_ns': s.st_mtime_ns}

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''): h.update(block)
    return h.hexdigest()

registry_path = STATE / 'proof_cohort_registry_v1.json'
registry = json.loads(registry_path.read_text(encoding='utf-8'))
cohorts = registry['active_cohorts']
names = [DB, Path(str(DB) + '-wal')]
before = {str(p.relative_to(ROOT)): stat(p) for p in names}
started = time.monotonic()
conn = sqlite3.connect(DB.as_uri() + '?mode=ro', uri=True, timeout=2)
conn.row_factory = sqlite3.Row
conn.execute('PRAGMA query_only=ON')
conn.execute('BEGIN')
deadline = time.monotonic() + 45
conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
schemas = [dict(r) for r in conn.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name IN ('canonical_forecasts','canonical_outcomes','forecast_integrity_events') ORDER BY type,name")]
groups = {}
selected = []
for family, cohort in sorted(cohorts.items()):
    params = (family, 'EUR_USD', '2026-08-29T12:36:36.550804+00:00')
    sql = "SELECT rowid AS forecast_rowid,* FROM canonical_forecasts INDEXED BY canonical_forecasts_scope WHERE family=? AND instrument=? AND entry_time>=? ORDER BY entry_time,event_id"
    plan = [dict(r) for r in conn.execute('EXPLAIN QUERY PLAN ' + sql, params)]
    raw = [dict(r) for r in conn.execute(sql, params)]
    rows = []
    for f in raw:
        payload = json.loads(f['forecast_json'])
        if payload.get('cohort_id') != cohort: continue
        f['payload_verified'] = hashlib.sha256(f['forecast_json'].encode()).hexdigest() == f['payload_sha256']
        f['outcome'] = next((dict(r) for r in conn.execute('SELECT * FROM canonical_outcomes WHERE event_id=? AND horizon_sec=3600', (f['event_id'],))), None)
        f['integrity_events'] = [dict(r) for r in conn.execute('SELECT * FROM forecast_integrity_events WHERE event_id=? AND horizon_sec=3600', (f['event_id'],))]
        rows.append(f)
    groups[family] = {'cohort_id': cohort, 'query_plan': plan, 'candidate_rows': len(raw), 'exact_cohort_rows': len(rows), 'matured_rows': sum(bool(r['outcome']) for r in rows), 'first_entry_time': rows[0]['entry_time'] if rows else None, 'last_entry_time': rows[-1]['entry_time'] if rows else None, 'sample': rows[0] if rows else None}
    selected.extend(rows)
conn.rollback()
conn.close()
after = {str(p.relative_to(ROOT)): stat(p) for p in names}
if before != after: raise RuntimeError('Production DB/WAL changed during bounded read')
report = {'generated_utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'boundary': {'connection': 'URI mode=ro; query_only; one read transaction', 'production_logical_writes': 0, 'database_main_or_wal_stat_changes': False, 'shm_caveat': 'SQLite may maintain ephemeral reader marks in existing SHM; no byte-identity claim is made for SHM.', 'workers_started': 0, 'broker_access': False}, 'fixed_selection': {'instrument':'EUR_USD','horizon_sec':3600,'family_basis':'all four active proof cohorts from saved registry, not selected from returns','cutoff_basis':'all locally retained rows in current stopped checkpoint after cohort activation'}, 'source_registry': {'path': str(registry_path.relative_to(ROOT)), 'sha256': digest(registry_path)}, 'database_before':before,'database_after':after,'schemas':schemas,'groups':groups,'read_seconds':time.monotonic()-started}
(OUT/'inventory.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n',encoding='utf-8')
(OUT/'selected_candidate_rows.json').write_text(json.dumps(selected,indent=2,sort_keys=True)+'\n',encoding='utf-8')
print(json.dumps({'groups':{k:{q:v[q] for q in ('exact_cohort_rows','matured_rows','first_entry_time','last_entry_time')} for k,v in groups.items()},'seconds':report['read_seconds'],'output':str(OUT/'inventory.json')},indent=2))
