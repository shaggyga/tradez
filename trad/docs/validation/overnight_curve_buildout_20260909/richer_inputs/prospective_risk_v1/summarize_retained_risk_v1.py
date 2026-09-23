"""Present all retained risk cells without rescoring, pooling or selecting a model."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time

BASE=Path(__file__).resolve().parent
AUTHORITY=('account_eligible','broker_requests','can_authorize','can_place_orders','can_promote',
           'execution_eligible','model_fits','proof_eligible','runtime_writes')
METHODS={'training_empirical','training_empirical_volatility_scaled'}
LABELS={'signed_terminal_bps','absolute_terminal_bps','mid_future_range_bps','realized_path_variation_bps',
        'long_close_MFE_bps','long_close_MAE_bps','short_close_MFE_bps','short_close_MAE_bps',
        'long_envelope_MFE_bps','long_envelope_MAE_bps','short_envelope_MFE_bps','short_envelope_MAE_bps'}


def sha(raw):return hashlib.sha256(raw).hexdigest()


def present(path,expected,output):
    path=Path(path).absolute();output=Path(output).absolute()
    if not path.is_relative_to(BASE) or path.stat().st_size>64*1024*1024:raise ValueError('bounded_retained_report_required')
    if output.parent!=BASE or output.exists():raise ValueError('fresh_direct_output_directory_required')
    raw=path.read_bytes()
    if sha(raw)!=expected:raise ValueError('original_report_identity_mismatch')
    value=json.loads(raw)
    if value.get('status')!='passed' or value.get('issues')!=[] or value.get('research_only') is not True:
        raise ValueError('successful_original_evaluation_required')
    if any(value.get(flag) is not False for flag in AUTHORITY):raise ValueError('authority_mismatch')
    aggregate=value['aggregate'];groups=aggregate['groups'];paired=aggregate['paired_methods']
    keys=[(r['instrument'],r['horizon_minutes'],r['method'],r['label']) for r in groups]
    wanted={(pair,horizon,method,label) for pair in ('EUR_USD','GBP_USD','USD_JPY')
            for horizon in (15,30,60) for method in METHODS for label in LABELS}
    if len(keys)!=len(set(keys)) or set(keys)!=wanted or len(paired)!=108:raise ValueError('complete_original_cell_inventory_required')
    counts={}
    for horizon in (15,30,60):
        rows=[row for row in groups if row['horizon_minutes']==horizon]
        status={}
        for row in rows:
            for name,count in row['status_counts'].items():status[name]=status.get(name,0)+count
        counts[str(horizon)]=dict(node_status_counts=status,
            scope='Method/label node counts; not independent origins, trades or trials.')
    summary=dict(schema_version='complete_retained_risk_results_v1_20260909',presented_epoch=time.time(),
        original_report=dict(path=str(path),sha256=expected,bytes=len(raw),
            verification_started_epoch=value['verification_started_epoch'],verification_completed_epoch=value['verification_completed_epoch']),
        presenter_sha256=sha(Path(__file__).read_bytes()),verified_chain_count=value['verified_chain_count'],
        registry_sha256=value['registry_sha256'],source_bindings=value['source_bindings'],
        issue_attempt_status_counts=value['attempt_status_counts'],scheduled_issue_status_counts=value['scheduled_issue_dispositions']['status_counts'],
        all_216_original_groups=groups,all_108_original_paired_method_cells=paired,node_dispositions_by_horizon=counts,
        independent_sample_size=aggregate['independent_sample_size'],original_aggregate_scope=aggregate['scope'],
        original_aggregate_sha256=aggregate['aggregate_sha256'],original_aggregate_as_of_epoch=aggregate['as_of_epoch'],
        original_report_unchanged=path.read_bytes()==raw,**{flag:False for flag in AUTHORITY},research_only=True,
        limits=['All cells retained, including pending and missing outcomes. No winner selection or new scoring.',
                'Reported 80% intervals are model quantiles; observed coverage on overlapping origins does not establish calibration.',
                'These full-horizon risk distributions have not been attached to a position manager or interpreted as conditional remaining risk.',
                'The full original report, per-origin evidence, price captures and train-only fitted baseline remain separate machine-local dependencies.'])
    output.mkdir()
    target=output/'COMPLETE_RETAINED_RISK_RESULTS_20260909.json'
    payload=(json.dumps(summary,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    with target.open('xb') as handle:handle.write(payload);handle.flush();os.fsync(handle.fileno())
    print(json.dumps(dict(path=str(target),sha256=sha(payload),groups=len(groups),paired_cells=len(paired),verified_chains=value['verified_chain_count'],node_dispositions=counts)))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--expected-sha256',required=True);parser.add_argument('--output-directory',type=Path,required=True)
    args=parser.parse_args();present(args.report,args.expected_sha256,args.output_directory)
