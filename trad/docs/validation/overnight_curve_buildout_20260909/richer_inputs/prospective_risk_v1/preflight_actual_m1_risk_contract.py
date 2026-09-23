"""Reviewed one-grid, three-GET preflight. Default is plan-only; never publishes."""
from pathlib import Path
from datetime import datetime,timezone
import argparse,hashlib,json,math,os,sys,time

BASE=Path('C:/Users/zmoor/Documents/forex')
TRAD=BASE/'trad'
ALLOWED_OUTPUT=Path(__file__).resolve().parent
FIT=BASE/'overnight_curve_buildout_20260909/richer_inputs/path_risk_development_v1/actual_development_001/PRE_SCORE_TRAINING_BASELINES.json'
FIT_SHA='1d3d6f6dfd946f2c251abebce78d5624ec73c6da393ee0cd68173303a1851b10'
FIXED={
 'oanda_m1_mba_research_capture_v1.py':'6bb09acc2573443cf1d6c28bd5ecbea72e582096c616d5accc412ea9f50abe27',
 'oanda_m1_path_risk_labels_v1.py':'b998f6f3fbe144ae949749e03d0658f7b89676dc7015eaefa7e583e85f6afccd',
 'oanda_live_account_readonly_status.py':'2734dda34d532a094eff614d5fe05d01c7aa6a2ebd80ca2bb974f12d8c5d93af'}
PAIRS=('EUR_USD','GBP_USD','USD_JPY')
FLAGS=dict(research_only=True,orders_enabled=False,can_place_orders=False,can_promote=False,
 account_eligible=False,proof_eligible=False,manager_activation=False,canonical_publication=False)

def need(ok,reason):
    if not ok:raise ValueError(reason)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def utc(t):return datetime.fromtimestamp(t,timezone.utc).isoformat()
def binding(path):
    raw=path.read_bytes();return dict(path=str(path),bytes=len(raw),sha256=sha(raw))
def verify(sources):
    for name,value in sources.items():need(binding(TRAD/name)['sha256']==value,'preflight_source_changed:'+name)
def read(path,limit):
    started=time.time()
    with path.open('rb') as f:
        raw=f.read(limit+1)
    completed=time.time();need(len(raw)<=limit,'preflight_read_bound')
    return raw,dict(path=str(path),read_started_epoch=started,read_completed_epoch=completed,bytes=len(raw),sha256=sha(raw))
def write(path,raw):
    started=time.time()
    with path.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    return dict(path=str(path),write_started_epoch=started,write_completed_epoch=time.time(),sha256=sha(raw),bytes=len(raw))
def error_code(error):
    # No raw request, credential, response text or arbitrary exception repr.
    value=str(error)
    return value if type(error) is ValueError and len(value)<=160 and all(c.isalnum() or c in '_:./-' for c in value) else 'preflight_computation_or_storage_failed'

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-epoch',type=int,required=True)
    parser.add_argument('--expected-contract-sha',required=True)
    parser.add_argument('--credential-path',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--execute',action='store_true',help='Only use after parent source/contract review and explicit GET dispatch.')
    args=parser.parse_args(argv)
    ref=args.reference_epoch
    need(ref>0 and ref%300==60,'preflight_exact_reference_grid_required')
    need(len(args.expected_contract_sha)==64 and all(c in '0123456789abcdef' for c in args.expected_contract_sha),'preflight_contract_hash_required')
    output=Path(args.output).resolve()
    need(output.parent==ALLOWED_OUTPUT.resolve() and not output.exists(),'preflight_new_immediate_child_output_required')
    sources={**FIXED,'oanda_m1_risk_distribution_contract_v1.py':args.expected_contract_sha}
    verify(sources)
    fit_raw,fit_read=read(FIT,2*1024*1024);need(sha(fit_raw)==FIT_SHA,'preflight_training_artifact_changed')
    plan=dict(mode='execute' if args.execute else 'plan_only',reference_price_epoch=ref,reference_utc=utc(ref),
        issue_deadline_epoch=ref+20,pairs=list(PAIRS),maximum_GETs=3,maximum_quantile_nodes=216,
        methods_per_pair=2,labels_per_pair=12,horizons_minutes=[15,30,60],nodes_per_successful_pair=72,
        fit=fit_read,source_bindings=sources,script=binding(Path(__file__)),output=str(output),
        no_retry=True,no_backfill=True,no_publication=True,**FLAGS)
    if not args.execute:
        print(json.dumps(plan,indent=2));return
    now=time.time()
    need(-20<=ref-now<=60,'preflight_dispatch_only_within60_seconds_before_or20_after_reference')
    output.mkdir()
    write(output/'PREFLIGHT_PLAN.json',canonical(plan))
    fit_copy=write(output/'training_artifact.json',fit_raw)
    sys.path.insert(0,str(TRAD))
    import oanda_m1_mba_research_capture_v1 as capture
    import oanda_m1_risk_distribution_contract_v1 as contract
    need(capture.source_bindings()=={k:sources[k] for k in capture.source_bindings()},'preflight_loaded_capture_closure')
    verify(sources)
    # Bounded wait only: parent dispatches near the chosen grid, never minutes early.
    while time.time()<ref:
        time.sleep(min(.05,max(0,ref-time.time())))
    results=[]
    for pair in PAIRS:
        row=dict(instrument=pair,scheduled_reference_price_epoch=ref,status='not_attempted',GET_attempted=False,
                 result_scope='engineering_preflight_unpublished_not_eligible_prospective_outcome',artifacts={})
        pair_dir=output/pair;pair_dir.mkdir()
        try:
            verify(sources)
            if time.time()>ref+20:
                row.update(status='withheld',reason='preflight_issue_window_already_expired')
                continue
            md=dict(instrument=pair,base_currency=pair[:3],quote_currency=pair[4:],pip_size='0.01' if pair=='USD_JPY' else '0.0001')
            row['GET_attempted']=True
            raw,receipt=capture.capture_once(pair,credential_path=args.credential_path,metadata=md)
            row['capture_status']=receipt['status'];row['capture_clock']=dict(request_started_epoch=receipt['request_started_epoch'],
                read_completed_epoch=receipt['read_completed_epoch'],capture_completed_epoch=receipt['capture_completed_epoch'])
            row['artifacts']['raw']=write(pair_dir/'source_raw.json',raw)
            row['artifacts']['receipt']=write(pair_dir/'capture_receipt.json',canonical(receipt))
            if receipt['status']!='captured_not_issued':
                row.update(status='withheld',reason=receipt.get('error_reason','preflight_capture_failed'))
                continue
            consume_started=time.time()
            retained_raw,raw_read=read(pair_dir/'source_raw.json',capture.MAX_BYTES)
            retained_receipt_raw,receipt_read=read(pair_dir/'capture_receipt.json',2*1024*1024)
            consumed=time.time();retained_receipt=contract.decode(retained_receipt_raw)
            need(raw==retained_raw and canonical(receipt)==canonical(retained_receipt),'preflight_retained_source_mismatch')
            consumption=dict(raw_sha256=sha(retained_raw),receipt_sha256=contract.digest(retained_receipt),
                read_started_epoch=consume_started,read_completed_epoch=consumed)
            row['input_consumption']=consumption
            row['artifacts']['input_read_receipt']=write(pair_dir/'input_consumption.json',canonical(dict(
                consumption=consumption,raw=raw_read,receipt=receipt_read,training_artifact_read=fit_read,training_artifact_copy=fit_copy)))
            mapping=capture.map_verified_capture(retained_raw,retained_receipt,md)
            row['coverage']=mapping['coverage']
            row['artifacts']['CSV']=write(pair_dir/'complete_mba_M1.csv',mapping['canonical_csv_bytes'])
            row['artifacts']['mapping']=write(pair_dir/'source_mapping.json',canonical(capture.mapping_document(mapping)))
            need(time.time()<=ref+20,'preflight_deadline_before_computation')
            issue=contract.issue_distribution(retained_raw,retained_receipt,md,fit_raw,input_consumption=consumption,
                cohort_id=contract.COHORT_ID,scheduled_reference_price_epoch=ref,expected_source_bindings=sources)
            # Preserve the actual calculation before any independent replay; no publication is made.
            row['artifacts']['unpublished_candidate']=write(pair_dir/'unpublished_distribution_candidate.json',canonical(issue))
            row.update(status='contract_candidate_created_unpublished',nodes=len(issue['nodes']),
                issued_epoch=issue['issued_epoch'],reference_price_epoch=issue['reference_price_epoch'],
                issued_sha256=issue['issued_sha256'])
            contract.validate_issue(issue,input_raw=retained_raw,input_receipt=retained_receipt,metadata=md,
                fit_raw=fit_raw,expected_source_bindings=sources)
            row['independent_replay_passed']=True
        except Exception as error:
            row.update(status='withheld_or_validation_failed',reason=error_code(error),error_type=type(error).__name__)
        finally:
            row['observation_completed_epoch']=time.time();results.append(row)
            write(pair_dir/'PREFLIGHT_RESULT.json',canonical({**row,**FLAGS}))
    verify(sources)
    report=dict(schema_version='m1_risk_contract_real_preflight_v1_20260909',status='completed',
        completed_epoch=time.time(),reference_price_epoch=ref,source_bindings=sources,
        fit_sha256=FIT_SHA,script=binding(Path(__file__)),results=results,
        GET_attempts=sum(r['GET_attempted'] for r in results),
        successful_unpublished_pairs=sum(r['status']=='contract_candidate_created_unpublished' for r in results),
        retained_unpublished_nodes=sum(r.get('nodes',0) for r in results),
        actual_publications=0,actual_consumptions_of_forecasts=0,actual_orders=0,**FLAGS)
    result=write(output/'M1_RISK_CONTRACT_PREFLIGHT.json',canonical(report))
    print(json.dumps(result,indent=2))

if __name__=='__main__':main()
