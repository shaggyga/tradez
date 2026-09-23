"""Retain compact current health/storage evidence without account information."""
from pathlib import Path
import hashlib,json,time
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent/'trad'
paths={'integrity':ROOT/'data/oanda_training_manager/state/project_integrity_audit_v1.json',
       'storage':ROOT/'data/oanda_training_manager/state/storage_headroom_v1.json'}
result={'schema_version':'repaired_news_operational_scope_20260908','observed_epoch':time.time(),
        'read_only':True,'raw_account_payload_retained':False}
for name,path in paths.items():
    raw=path.read_bytes();value=json.loads(raw)
    keys=('status','generated_utc','publication_status','snapshot_fresh_at_publication','check_scopes',
          'active_shared_or_unknown_failures','inactive_component_failures','runtime_health',
          'database_inventory','can_place_orders','can_promote','research_only','automatic_evidence_deletion','automatic_vacuum')
    result[name]={'path':str(path),'raw_sha256':hashlib.sha256(raw).hexdigest(),
                  'observation':{key:value[key] for key in keys if key in value}}
assert result['storage']['observation']['database_inventory']['existing_count']==420
assert result['storage']['observation']['database_inventory']['complete_for_registered_scope'] is True
assert result['integrity']['observation']['runtime_health']['status']=='current'
assert result['integrity']['observation']['active_shared_or_unknown_failures']==[]
with (HERE/'OPERATIONAL_SCOPE_FINAL_20260908.json').open('x',encoding='utf-8') as f:
    json.dump(result,f,indent=2,allow_nan=False)
print(json.dumps({'managed_databases':420,'integrity_retained_failures':len(result['integrity']['observation']['inactive_component_failures']),
                  'active_shared_unknown_failures':0,'runtime_health':'current'}))
