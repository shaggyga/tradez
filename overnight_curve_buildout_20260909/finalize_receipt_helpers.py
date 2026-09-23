"""Bind tested candidate sources and retain one private local quote observation."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

OUT=Path(__file__).resolve().parent
PROJECT=OUT.parent/'trad'
FILES=('oanda_forecast_curve_contract_v1.py','oanda_forecast_curve_file_store_v1.py',
       'test_oanda_forecast_curve_file_store_v1.py','oanda_research_quote_receipt_v1.py',
       'test_oanda_research_quote_receipt_v1.py')
sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
before={name:sha(PROJECT/name) for name in FILES}
result=subprocess.run([sys.executable,'-B','-m','pytest','-q','-p','no:cacheprovider',
    'test_oanda_forecast_curve_file_store_v1.py','test_oanda_research_quote_receipt_v1.py',
    '--junitxml='+str(OUT/'receipt_helpers_bound_tests.xml')],cwd=PROJECT,capture_output=True,text=True,timeout=60)
after={name:sha(PROJECT/name) for name in FILES}
assert before==after,'Retain failed source-stability run; do not bind changing candidates'
assert result.returncode==0,result.stdout[-1000:]
suite=ET.parse(OUT/'receipt_helpers_bound_tests.xml').getroot()
assert not suite.findall('.//failure') and not suite.findall('.//error')

private=OUT/'quote_receipt_private_001'
private.mkdir(exist_ok=False)
(private/'PRIVATE_SOURCE_NOT_FOR_EXPORT.txt').write_text('Retained original quote snapshot. Do not export raw_source.json; compact receipt/map metadata is sufficient.\n')
sys.path.insert(0,str(PROJECT))
import oanda_research_quote_receipt_v1 as q
observation=q.capture_quote_snapshot()
mapped=q.map_quote_snapshot(observation['raw_bytes'],observation['receipt'],decision_epoch=time.time())
(private/'raw_source.json').write_bytes(observation['raw_bytes'])
(private/'capture_receipt.json').write_bytes(q._bytes(observation['receipt']))
(private/'normalized_selected_quotes.json').write_bytes(q._bytes(mapped))

sources=OUT/'receipt_helper_sources'
sources.mkdir(exist_ok=False)
for name in FILES:
    shutil.copyfile(PROJECT/name,sources/(name+'.txt'))
    assert sha(sources/(name+'.txt'))==before[name]
registry=PROJECT/'config/joint_price_news_study_v3_20260908.json'
bindings=json.loads(registry.read_text())['source_bindings']
assert {name:sha(PROJECT/name) for name in bindings}==bindings

report={'schema_version':'offline_research_receipt_helpers_validation_v1_20260909',
    'generated_utc':datetime.now(timezone.utc).isoformat(),'status':'passed_offline_candidates',
    'source_hashes_before':before,'source_hashes_after':after,'source_stable':True,
    'tests':{'cases':len(suite.findall('.//testcase')),'failures':0,'errors':0,
        'skipped':len(suite.findall('.//skipped')),'xml_path':str(OUT/'receipt_helpers_bound_tests.xml'),
        'xml_sha256':sha(OUT/'receipt_helpers_bound_tests.xml')},
    'prior_tests':[{'path':str(OUT/name),'sha256':sha(OUT/name)} for name in
        ('curve_file_store_initial_tests.xml','curve_file_store_candidate_tests.xml','quote_receipt_initial_tests.xml')],
    'initial_fixture_failure':'Initial file-store run had36 passing and1 actual-junction fixture failure due PowerShell script execution policy. Per-process fixture invocation corrected; actual junction test then passed. Production helper unchanged by that fixture correction.',
    'independent_review':{'file_store':'Parent read-only review reported no blocker within documented trusted-root boundary; no independent rerun claimed.',
        'quote_receipt':'Pending separate source review.'},
    'live_source_observation':{'capture_receipt':observation['receipt'],
        'source_header':mapped['source_header'],'source_header_sha256':mapped['source_header_sha256'],
        'mapping_sha256':mapped['mapping_sha256'],'decision_epoch':mapped['decision_epoch'],
        'accepted_pairs':list(mapped['quotes']),'refusals':mapped['refusals'],
        'private_original_bytes_path':str(private/'raw_source.json'),
        'private_original_bytes_sha256':sha(private/'raw_source.json'),
        'private_normalized_map_path':str(private/'normalized_selected_quotes.json'),
        'private_normalized_map_sha256':sha(private/'normalized_selected_quotes.json'),
        'scope':'One bounded local file observation only. No network GET, fills, runtime restart, source mutation or broker/account action.'},
    'registered_source_hashes_unchanged':True,
    'limitations':['Caller owns registered model/cohort/policy/source closure and actual decision gates.',
        'Consumption records factual observed bytes; target eligibility is separately checked by the adapter.',
        'No delete/overwrite or orphan repair. In-progress and partial immutable publications fail closed.',
        'File fsync and readback do not establish Windows directory power-loss durability.',
        'Trusted root required; path checks do not sandbox against concurrent administrator directory replacement.',
        'Quote decimal precision is only the precision retained in upstream JSON; original raw bytes and source timestamp remain evidence.',
        'No registered study or runtime adopted either helper in this task.']}
dest=OUT/'RECEIPT_HELPERS_IMPLEMENTATION_VALIDATION_20260909.json'
with dest.open('x',encoding='utf-8') as handle:
    json.dump(report,handle,indent=2,allow_nan=False)
    handle.write('\n')
print(json.dumps({'receipt_path':str(dest),'receipt_sha256':sha(dest),'tested_cases':report['tests']['cases'],
    'source_hashes':before,'accepted_pairs':list(mapped['quotes']),'refusals':mapped['refusals'],
    'source_generated_utc':mapped['source_header']['generated_utc'],
    'read_completed_epoch':observation['receipt']['read_completed_epoch'],'no_runtime_activation':True}))
