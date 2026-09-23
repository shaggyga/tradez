"""Preserve a dated sanity check and update current audit navigation."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import sys

OUT=Path(__file__).resolve().parent;ROOT=OUT.parent/'trad'
sys.dont_write_bytecode=True;sys.path.insert(0,str(ROOT))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def write_new(p,value):
    with p.open('x',encoding='utf-8') as f:json.dump(value,f,indent=2);f.write('\n')
now=datetime.now(timezone.utc).isoformat()
exact=load(OUT/'root_offline_counts.json')
assert (exact['n'],exact['decimal_direction_hits'],exact['decimal_flat_count'],exact['after_spread_wins'])==(8414,4148,296,716)
final=OUT/'broad_signal/broad_signal_final_verification.json'
assert sha(final)=='f99dd7ded33fbe708c2db314b8d5414406af635ac84c947da06b4c8b5bf60c60'
evidence=ROOT/'docs/validation/prediction_sanity_20260906'
evidence.mkdir(parents=True,exist_ok=False)
files=[]
for name in ('broad_signal','model_inventory','independent_four_family','comovement'):
    for p in (OUT/name).iterdir():
        if p.is_file() and p.suffix in ('.py','.json','.md','.svg'):files.append((p,Path(name)/p.name))
files.append((OUT/'broad_signal/selected_rows.jsonl',Path('broad_signal/selected_rows.txt')))
files.extend((OUT/name,Path(name)) for name in
             ('comovement_diagnostic.py','plot_comovement.py','recheck_broad_counts_offline.py','root_offline_counts.json'))
for p,relative in files:
    target=evidence/relative;target.parent.mkdir(parents=True,exist_ok=True)
    with target.open('xb') as f:f.write(p.read_bytes())
inventory_path=ROOT/'docs/FOREX_MODEL_INVENTORY_20260906.md'
with inventory_path.open('xb') as f:f.write((OUT/'model_inventory/MODEL_INVENTORY_REVIEW.md').read_bytes())
readme='''# Frozen sanity-check evidence

The authoritative broad-signal result is broad_signal/broad_signal_final_verification.json.
The preliminary raw_verification JSON is preserved only because it contains the original
extraction metadata and is an input to the finalizer; its suffix-based pip assumptions
and derived pip means were superseded. Read the final report's explicit correction.

selected_rows.txt retains the original JSONL bytes under an archive-supported extension.
For a portable read-only count check from the source root:

```powershell
python -B docs/validation/prediction_sanity_20260906/recheck_broad_counts_offline.py docs/validation/prediction_sanity_20260906/broad_signal/selected_rows.txt
```

Expected: 8414 rows, 4148 exact direction hits, 296 flats and 716 positive bid/ask outcomes.
Other acquisition scripts record their original local paths and output locations; do not
run them in the sealed evidence folder. Use disposable copies and explicitly adapt paths
when reproducing them. No production database is needed for the portable count check.
The original four-family raw extract is under ../fixed_evaluation_20260906.
'''
with (evidence/'README.md').open('x',encoding='utf-8') as f:f.write(readme)
from oanda_causal_forecast_study import load_contract,DEFAULT_CONFIG,STUDY
from oanda_causal_forecast_ledger import digest
contract=load_contract(DEFAULT_CONFIG)
heartbeat=load(STUDY/'heartbeat.json')
assert heartbeat['contract_sha256']==digest(contract)
assert heartbeat['can_place_orders'] is False and heartbeat['can_promote'] is False
write_new(evidence/'collection_observation.json',{'observed_utc':now,'heartbeat':heartbeat,
    'registered_source_bindings_match':True,'review_changed_study':False})
report=ROOT/'docs/FOREX_PREDICTION_SANITY_20260906.md'
plan=ROOT/'FOREX_COMOVEMENT_RESEARCH_PLAN_20260906.json'
binding_paths=[report,inventory_path,plan]+[p for p in evidence.rglob('*') if p.is_file()]
receipt={'schema_version':'forex_prediction_sanity_validation_v1','generated_utc':now,
    'scope':'Independent saved prediction arithmetic, live read-only indexed broad-ledger extraction, frozen comovement diagnostics and model inventory',
    'result':'Most historical arithmetic reproduced; eight floating-point-only direction hits corrected offline; useful predictive edge remains unproven',
    'exact_broad_counts':exact,
    'four_family_verified':{'forecasts':1628,'shared_references':407,'matched_endpoints':368,'all_payload_hashes_verified':True,
        'correct_counts':{'cross_pair_graph_transfer':156,'modern_tabular_probabilistic_repaired':175,'probabilistic_state_space':176,'ridge_return_repaired':163}},
    'source_bindings':[{'path':p.relative_to(ROOT).as_posix(),'sha256':sha(p)} for p in binding_paths],
    'prior_receipts_unchanged':{name:sha(ROOT/name) for name in ('FOREX_PREDICTION_QUALITY_20260906.json',
        'FOREX_FIXED_EVALUATION_RESULTS_20260906.json','FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json')},
    'active_contract_sha256':digest(contract),'active_contract_or_model_source_changed':False,
    'new_models_fitted':0,'new_models_activated':0,'orders_placed':0,'proof_eligible':False,
    'production_scoring_precision_fix':'pending separately versioned scorer; corrected historical diagnostic is available offline',
    'registered_model_source_bindings':contract['source_bindings']}
receipt_path=ROOT/'FOREX_PREDICTION_SANITY_VALIDATION_20260906.json';write_new(receipt_path,receipt)
backup=OUT/'before_current_records';backup.mkdir(exist_ok=False)
for name in ('README.md','FOREX_AUDIT_START_HERE.md','FOREX_PENDING_IMPROVEMENTS.md','FOREX_PROJECT_LOG.md',
             'docs/AUDIT_STATE_CURRENT.md','FOREX_ISSUE_REGISTER_CURRENT.json','forex_model_vault_sync.py'):
    target=backup/name;target.parent.mkdir(parents=True,exist_ok=True)
    with target.open('xb') as f:f.write((ROOT/name).read_bytes())
register_path=ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json';register=load(register_path)
issue={'issue_id':'FX-20260906-EXACT-MIDPOINT-SCORING','title':'Float midpoint noise counts unchanged prices as directional outcomes',
    'priority':'P2','owner_component':'prediction_scoring','status':'open',
    'acceptance_tests':['Define exact decimal or tick midpoint direction and flat semantics in a separately versioned scorer',
        'Reproduce stored and corrected historical denominators separately without changing forecasts',
        'Test equal midpoint with changed spreads, legitimate minimum moves, buy/sell signs and pair pip conventions',
        'Apply exact-price comparison before predictive acceptance; preserve registered source and study identities'],
    'current_mitigation':'Independent offline decimal result available:4148/8414 direction hits,296 flats,716 after-spread wins. Trading/proof remain disabled; registered study retains raw quotes for separately versioned rescoring.',
    'evidence_paths':[report.relative_to(ROOT).as_posix()],
    'validation_artifacts':[{'path':receipt_path.name,'sha256':sha(receipt_path)}],
    'status_history':[{'at_utc':now,'status':'open'}]}
assert issue['issue_id'] not in {i['issue_id'] for i in register['issues']}
register['issues'].insert(0,issue);register['generated_utc']=now
register_path.write_text(json.dumps(register,indent=2)+'\n',encoding='utf-8')
note=f'''Current sanity check ({now}): raw counts reproduce, with eight floating-point-only
direction hits corrected offline: **4,148/8,414 = 49.30%**; after-spread positives
remain **716/8,414 = 8.51%**. The four-model EUR/USD percentages reproduce exactly.
See [the prediction sanity check](docs/FOREX_PREDICTION_SANITY_20260906.md),
[model inventory](docs/FOREX_MODEL_INVENTORY_20260906.md) and
`FOREX_COMOVEMENT_RESEARCH_PLAN_20260906.json`. Scoring precision follow-up is open;
the research plan is inactive and the registered collecting study is unchanged.

Earlier dated observations below retain their original counts and scope.

'''
for name in ('README.md','FOREX_PENDING_IMPROVEMENTS.md','docs/AUDIT_STATE_CURRENT.md'):
    p=ROOT/name;head,rest=p.read_text(encoding='utf-8').split('\n',1)
    p.write_text(head+'\n\n'+(note.replace('(docs/','(') if name.startswith('docs/') else note)+rest.lstrip('\n'),encoding='utf-8')
p=ROOT/'FOREX_AUDIT_START_HERE.md';head,rest=p.read_text(encoding='utf-8').split('\n',1)
vault_note=f'''Latest sanity check ({now}): start with `PREDICTION_SANITY_CURRENT.md`,
`PREDICTION_SANITY_VALIDATION_CURRENT.json`, `MODEL_INVENTORY_CURRENT.md` and
`COMOVEMENT_RESEARCH_PLAN_CURRENT.json` in this vault. Source counterparts are
`docs/FOREX_PREDICTION_SANITY_20260906.md`, `FOREX_PREDICTION_SANITY_VALIDATION_20260906.json`,
`docs/FOREX_MODEL_INVENTORY_20260906.md` and `FOREX_COMOVEMENT_RESEARCH_PLAN_20260906.json`.
Corrected broad direction is 49.30%, after-spread positives remain8.51%, and all
four archived EUR/USD percentages reproduce. Joint-movement diagnostics and
actual D-drive model assets are inventoried. Exact-price scoring follow-up is
open. Collection and its registered source are unchanged; trading stays off.
Earlier dated observations and original counts follow as history.

'''
p.write_text(head+'\n\n'+vault_note+rest.lstrip('\n'),encoding='utf-8')
p=ROOT/'FOREX_PROJECT_LOG.md';text=p.read_text(encoding='utf-8');pos=text.index('\n## ')
entry=f'''\n## 2026-09-06 — independent prediction sanity check and co-movement inventory\n\nRecorded {now}. Indexed raw ledger extraction reproduced 8414rows/4156stored\ndirection hits/716positive spread outcomes; independent decimal prices corrected\neight rounding-only hits to4148(49.30%). Four-model368endpoint metrics and1628\npayload hashes reproduce. Independent co-movement verification matched98matrix\ncells and35lagcells; unanimous models were right9/40. D-drive research artifacts\nwere located and separated from the four current predictors. Added exact-price\nscoring issue and inactive co-movement research plan. Historical receipts and\nregistered collection source remain unchanged; no models fitted or orders placed.\nSee docs/FOREX_PREDICTION_SANITY_20260906.md and its validation receipt.\n\n'''
p.write_text(text[:pos]+entry+text[pos:],encoding='utf-8')
from oanda_issue_register_validator import validate_register
validation=validate_register(register_path,root=ROOT);assert validation['valid'],validation
write_new(OUT/'ISSUE_REGISTER_VALIDATION.json',validation)
print(json.dumps({'receipt_sha256':sha(receipt_path),'evidence_files':len(binding_paths),'register':validation}))
