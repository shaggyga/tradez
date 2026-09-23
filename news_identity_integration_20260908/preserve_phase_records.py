from pathlib import Path
from datetime import datetime, timezone
import hashlib,json
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent/'trad'
names=['forex_model_vault_sync.py','FOREX_PENDING_IMPROVEMENTS.md','FOREX_PROJECT_LOG.md',
       'FOREX_AUDIT_START_HERE.md','docs/VAULT_SYSTEM_GUIDE.md','FOREX_AUDIT_STATE_CURRENT.json',
       'FOREX_COMMONS_CURRENT.json','README.md']
rows=[]
for name in names:
    source=ROOT/name;target=HERE/'before_records'/name
    target.parent.mkdir(parents=True,exist_ok=True)
    raw=source.read_bytes()
    with target.open('xb') as f:f.write(raw)
    sha=hashlib.sha256(raw).hexdigest()
    assert hashlib.sha256(target.read_bytes()).hexdigest()==sha
    rows.append({'source':str(source),'copy':str(target),'sha256':sha,'bytes':len(raw)})
with (HERE/'BEFORE_RECORDS_MANIFEST_20260908.json').open('x',encoding='utf-8') as f:
    json.dump({'observed_utc':datetime.now(timezone.utc).isoformat(),'files':rows},f,indent=2)
print(json.dumps({'preserved_files':len(rows)}))
