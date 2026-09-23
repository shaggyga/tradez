from pathlib import Path
import json,zipfile,hashlib
from datetime import datetime,timezone
OUT=Path(__file__).parent;C=OUT.parent/'trad'
inventory=json.loads((OUT/'CURVE_SOURCE_ARCHIVE_INVENTORY_20260908.json').read_text(encoding='utf-8'))
archives=inventory['vault_source_archives']+inventory['local_archives']+inventory['vault_other_archives']
target='unified_intrahour_forecast_latest.joblib';matches=[]
for item in archives:
    try:
        with zipfile.ZipFile(item['path']) as z:
            for e in z.infolist():
                if e.filename.replace('\\','/').endswith('/'+target) or e.filename==target:
                    matches.append({'archive':item['path'],'member':e.filename,'bytes':e.file_size})
    except (OSError,zipfile.BadZipFile):pass
source=C/'oanda_unified_intrahour_shadow_producer.py';local=Path('C:/Users/zmoor/AppData/Local/ForexResearchData/unified_intrahour_v1')
validation=json.loads((local/'latest_validation.json').read_text(encoding='utf-8'))
record={'schema':'feature_curve_inventory_closure_v1_20260908','observed_utc':datetime.now(timezone.utc).isoformat(),
 'exact_model_name_searched':target,'archive_count':len(archives),'archive_matches':matches,
 'appdata_paths':[{ 'path':str(local/name),'exists':(local/name).is_file(),'bytes':(local/name).stat().st_size if (local/name).is_file() else None} for name in ['models/'+target,'latest_validation.json','latest_shadow_forecast_curves.json','build_manifest.json','matrix_manifest.json','unified_feature_registry.csv','unified_training_matrix.parquet']],
 'retained_validation':{k:validation.get(k) for k in ['feature_schema_version','status','verdict','forecast_validation_pass','after_cost_tradability_pass','deployment_eligible','deployment_performed','live_execution_enabled','artifact','artifact_sha256']},
 'inspected_source_sha256':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [source,C/'oanda_ma_feature_grid.py',C/'oanda_practice_shadow_strategy_lab.py',C/'oanda_signal_contribution_feed.py',C/'oanda_always_on_supervisor.ps1',C/'oanda_practice_live_dashboard.py',C/'oanda_main_signal_dashboard.html',C/'docs/MA_FEATURE_GRID.md',C/'tools/vault_worktree_snapshot.py',C/'.gitignore']},
 'scope':'Metadata only. The large training matrix and serialized model bytes were not read by this closure; sibling evaluation separately owns the fitted artifact hash/validation audit. Zero archive matches is limited to the explicitly inventoried vault/source, vault-root, local artifacts and quarantine checkpoint locations, not the entire computer.'}
(OUT/'CURVE_INVENTORY_CLOSURE_20260908.json').write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'archive_count':len(archives),'model_members':matches,'gates':record['retained_validation']},indent=2))
