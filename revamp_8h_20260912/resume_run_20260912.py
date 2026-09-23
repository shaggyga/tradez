"""Record the requested continuation without replacing the dated closeout."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json
ROOT=Path(__file__).resolve().parent
RICH=ROOT/'direction_richer_archive_003'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
freeze=RICH/'PROTOCOL_FREEZE_003.json'
assert sha(freeze)=='870a22150ab6124f37cc32480ab9577af8e3cdee1cd9214880a2135950167083'
checks=[]
for pair in ('AUD_CAD','AUD_CHF'):
    for suffix in ('FEATURES','DIAGNOSTICS'):
        name=f'{pair}_{suffix}.parquet'
        old=RICH/'prepared_001'/name;new=RICH/'prepared_002'/name
        assert sha(old)==sha(new)
        checks.append({'pair':pair,'kind':suffix,'sha256':sha(new),'same_as_stopped_attempt':True})
now=datetime.now(timezone.utc).isoformat()
record={'schema':'user_requested_continuation_v1','observed_utc':now,'request':'Continue work',
    'prior_stop_receipt_sha256':sha(RICH/'USER_STOPPED_001.json'),'freeze_sha256':sha(freeze),
    'new_preparation_output':str(RICH/'prepared_002'),'partial_recreation_checks':checks,
    'reason_for_new_directory':'Frozen preparation requires a new exclusive output; no same-contract resume implementation is declared.',
    'new_model_search_or_parameter_change':False,'real_richer_fits_before_restart':0,
    'tasks':['complete frozen richer preparation and six predeclared fits','finish matched saved-artifact verifier',
             'complete typed probability adapters and caller review','current source recovery profile'],
    'service_or_trading_activation':False,'prior_closeout_preserved':True}
with (ROOT/'RUN_RESUMED_001.json').open('x',encoding='utf-8') as f:json.dump(record,f,indent=2)
with (ROOT/'WORK_LOG.md').open('a',encoding='utf-8') as f:
    f.write('\n\n### '+now+' — resumed at user request\n\nThe user requested continued work after the closeout. Revision003 preparation restarted in prepared_002 under the same30-source freeze; no feature, population, target, cost, model or fold choice changed. The frozen routine requires a new exclusive output directory, so the first two pair computations repeat preparation only. Their feature and diagnostic Parquet byte hashes exactly match the stopped attempt. No richer fit had previously run. Prior stopped outputs and closeout checkpoint004 remain dated evidence. Parallel work resumes the003saved-artifact verifier, typed probability adapters and source-recovery profile. No service/trading activation or clock-start retry is part of this continuation.\n')
print(json.dumps({'resumed':True,'same_frozen_protocol':True,'prior_partial_parquets_reproduced':len(checks)}))
