"""Bind the passed focused suite and reviewed source closure before activation."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

WORK = Path(__file__).resolve().parent
ROOT = WORK.parent/'trad'
sys.path.insert(0,str(ROOT))
import oanda_recovered_curve_pilot_v1 as pilot


def sha(raw): return hashlib.sha256(raw).hexdigest()


def main():
    xml_path=WORK/'preactivation_combined_tests_20260909.xml'
    suites=ET.fromstring(xml_path.read_bytes()).findall('testsuite')
    counts={key:sum(int(suite.attrib.get(key,0)) for suite in suites)
            for key in ('tests','failures','errors','skipped')}
    if counts != dict(tests=544,failures=0,errors=0,skipped=1):
        raise ValueError('unexpected_combined_test_result')
    old=json.loads((ROOT/'config/joint_price_news_study_v3_20260908.json').read_bytes())['source_bindings']
    old_checks={name:sha((ROOT/name).read_bytes())==digest for name,digest in old.items()}
    if len(old_checks)!=20 or not all(old_checks.values()):raise ValueError('prior_registered_sources_changed')
    names=set(pilot.SOURCE_FILES)|{'oanda_curve_management_replay_v1.py','oanda_news_capture_storage_v1.py'}
    names|={name for name in (
        'test_oanda_recovered_second_curve_v1.py','test_oanda_forecast_curve_contract_v1.py',
        'test_oanda_curve_management_adapter_v1.py','test_oanda_recovered_curve_bridge_v1.py',
        'test_oanda_forecast_curve_file_store_v1.py','test_oanda_research_quote_receipt_v1.py',
        'test_oanda_s5_mba_research_capture_v1.py','test_oanda_native_curve_outcomes_v1.py',
        'test_oanda_recovered_curve_pilot_v1.py','test_oanda_curve_management_replay_v1.py',
        'test_oanda_news_capture_storage_v1.py')}
    output=WORK/'preactivation_source'
    output.mkdir(exist_ok=False)
    records=[]
    for name in sorted(names):
        raw=(ROOT/name).read_bytes()
        with (output/name).open('xb') as handle:handle.write(raw)
        records.append(dict(path=name,bytes=len(raw),sha256=sha(raw)))
    value=dict(schema_version='overnight_curve_pipeline_preactivation_20260909',
        observed_utc=datetime.now(timezone.utc).isoformat(),status='engineering_checks_passed_not_activated',
        test_result={**counts,'passed':counts['tests']-counts['skipped'],
            'xml_path':str(xml_path),'xml_sha256':sha(xml_path.read_bytes()),
            'warnings':['two original NumPy datetime timezone representation warnings'],
            'skip':'Windows symlink creation privilege unavailable; actual NTFS junction test passed'},
        sources=records,prior_registered_source_checks=old_checks,
        pilot_source_bindings=pilot.source_bindings(),
        pending='Create immutable pilot registry; one actual bounded cycle; inspect outcomes and then continue collection to registered deadline.',
        predictive_quality_claim=False,orders_enabled=False,manager_activated=False,research_only=True)
    path=WORK/'PIPELINE_PREACTIVATION_ACCEPTANCE_20260909.json'
    with path.open('x',encoding='utf8') as handle:json.dump(value,handle,sort_keys=True,indent=2)
    print(json.dumps(dict(path=str(path),sha256=sha(path.read_bytes()),passed=543,skipped=1,
        sources=len(records),prior_registered_unchanged=len(old_checks))))


if __name__=='__main__':main()
