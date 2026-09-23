from pathlib import Path
import hashlib,json,xml.etree.ElementTree as ET
from datetime import datetime,timezone

root=Path(__file__).parent
project=root.parent/'trad'
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
names=['oanda_main_signal_dashboard.html','test_oanda_joint_price_news_dashboard_v1.py']
expected={'oanda_main_signal_dashboard.html':'3ffead742b158de5685b01fef2b25c3e68a556f365356828da9d90f0b91aced4',
          'test_oanda_joint_price_news_dashboard_v1.py':'01417e4cc9d0a9354e2fcaede84c7b9cc4f0c0d189d50f621410de509e791ca2'}
assert all(digest(project/name)==expected[name] for name in names)
suites=list(ET.parse(root/'forecast_visibility_tests.xml').getroot().iter('testsuite'))
assert sum(int(s.get('tests','0')) for s in suites)==240
assert not any(int(s.get('failures','0')) or int(s.get('errors','0')) for s in suites)
live=json.loads((root/'LIVE_FORECAST_VISIBILITY_VERIFICATION_20260907.json').read_text(encoding='utf-8'))
assert live['status']=='passed' and live['source_sha256']['oanda_main_signal_dashboard.html']==expected[names[0]]
assert digest(project/'oanda_practice_live_dashboard.py')=='a5dd6945cc448a6ab86fa5fab8d55aea6040bf197e5a50117fb1bbc218d2f9fe'
artifacts=['forecast_visibility_tests.xml','LIVE_FORECAST_VISIBILITY_VERIFICATION_20260907.json',
           'LIVE_FORECAST_VISIBILITY_DESKTOP_20260907.png','LIVE_FORECAST_VISIBILITY_PRICE_ONLY_20260907.png',
           'LIVE_FORECAST_VISIBILITY_MOBILE_20260907.png','HORIZON_SOURCE_API_INVENTORY_20260907.json',
           'HORIZON_AND_FORECAST_VISIBILITY_REVIEW_20260907.md']
record={'schema':'forecast_visibility_validation_receipt_v1_20260907','created_utc':datetime.now(timezone.utc).isoformat(),
        'validation_status':'passed','scope':'UI-only availability projection and bounded source/API horizon audit; no backend, model, registration, gate or runtime reload changes by this subtask.',
        'source_sha256':expected,'unchanged_backend_sha256':digest(project/'oanda_practice_live_dashboard.py'),
        'before_source_sha256':{name:digest(root/'before_source'/name) for name in names},
        'test_result':{'tests':240,'failures':0,'errors':0,'reported_duration_sec':21.54,
          'files':['test_oanda_joint_price_news_dashboard_v1.py','test_oanda_pair_forecast_dashboard_v2.py','test_oanda_news_pair_dashboard.py','test_oanda_collection_dashboard_status.py']},
        'live_observed_utc':live['initial']['observed_utc'],'live_coverage':live['initial']['coverage'],
        'artifact_sha256':{name:digest(root/name) for name in artifacts},
        'limits':['Dated UI availability is not measured prediction success or trading readiness.',
                  'Repeated joint reader generation-mismatch API responses are preserved separately; no cause inferred or binding guard weakened.',
                  'Independent source review is recorded separately; this receipt attests the owner-run focused tests and live observation only.']}
with (root/'FORECAST_VISIBILITY_VALIDATION_RECEIPT_20260907.json').open('x',encoding='utf-8') as handle:
    handle.write(json.dumps(record,indent=2)+'\n')
print(digest(root/'FORECAST_VISIBILITY_VALIDATION_RECEIPT_20260907.json'))
