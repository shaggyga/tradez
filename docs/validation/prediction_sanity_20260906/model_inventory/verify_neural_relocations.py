"""Verify known relocated neural checkpoint paths without deserialization."""
import hashlib
import json
from pathlib import Path

OUT=Path(__file__).resolve().parent
saved=json.loads((OUT/'model_inventory_evidence.json').read_text(encoding='utf-8'))
target=Path(r'D:\forex\trad\data\oanda_training_manager\models\modern_model_gap')
rows=[]
for report in saved['historical_reports_now']:
    for artifact in report.get('artifact_file_evidence',[]):
        if not artifact['present'] and Path(artifact['path']).suffix=='.pt':
            path=target/Path(artifact['path']).name
            row={'recorded_old_path':artifact['path'],'relocated_path':str(path),'present':path.is_file(),
                 'expected_sha256':artifact.get('recorded_sha256')}
            if path.is_file():
                row['bytes']=path.stat().st_size
                if row['bytes']<=12_000_000:
                    row['sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
                    row['matches_recorded_sha256']=row['sha256']==row['expected_sha256']
                else:
                    row['hash_verification']='not repeated: bounded metadata inventory'
            rows.append(row)
destination=OUT/'neural_relocation_evidence.json'
with destination.open('x',encoding='utf-8') as handle:
    json.dump({'deserialized_models':False,'checkpoints':rows},handle,indent=2,sort_keys=True)
    handle.write('\n')
print(json.dumps(rows))
