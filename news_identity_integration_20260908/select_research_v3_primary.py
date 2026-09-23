"""Select only a verified current v3 publication for the research dashboard."""
from pathlib import Path
import json, sys, time
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent/'trad'
sys.path.insert(0,str(ROOT))
import oanda_joint_price_news_forecast_study_v3 as worker
import oanda_practice_live_dashboard as dashboard

def main():
    pointer=ROOT/'config/joint_forecast_primary_current.json'
    if pointer.exists():raise ValueError('primary_selection_already_exists')
    registry=worker.load_registry(worker.DEFAULT_CONFIG)
    worker.verify_activated_study(registry,worker.STUDY)
    activation=json.loads((worker.STUDY/'activation_receipt.json').read_bytes())
    observations=[]
    for attempt in range(3):
        observed=dashboard._summarize_registered_family_forecasts(worker.STUDY.parent,joint=3)
        active=sum(bool(row.get('active_forecasts')) for row in observed.get('rows',[]))
        observations.append({k:v for k,v in observed.items() if k!='rows'})
        if (observed.get('status')=='current' and observed.get('registry_sha256')==worker.digest(registry) and active>=1):break
        if attempt<2:time.sleep(1)
    else:
        with (HERE/f'PRIMARY_PREFLIGHT_WITHHELD_{time.time_ns()}.json').open('xb') as f:f.write(worker.encoded(observations))
        raise ValueError('at_least_one_verified_current_v3_publication_required')
    selected={'schema_version':'joint_forecast_primary_selection_v1_20260908','selected':'v3',
              'activated_epoch':time.time(),'registry_sha256':worker.digest(registry),
              'study_activation_completed_epoch':activation['activation_completed_epoch'],
              'research_only':True,'can_place_orders':False,'can_promote':False,'can_authorize':False,
              'scope':'Primary dashboard selection only; new empty-ledger study activation was earlier and independently recorded.'}
    if selected['activated_epoch']<activation['activation_completed_epoch']:raise ValueError('selection_before_study_activation')
    worker.atomic_json(pointer,selected)
    confirmed=dashboard.summarize_joint_price_news_forecasts(worker.STUDY.parent)
    if confirmed.get('study_version')!='joint_v3' or confirmed.get('status')!='current':
        raise ValueError('selected_v3_projection_unavailable')
    record={'schema_version':'repaired_news_primary_selection_receipt_20260908',
            'selection':selected,'observed_epoch':time.time(),'projection_status':confirmed['status'],
            'counts':confirmed['counts'],'ledger_counts':confirmed.get('ledger_counts'),
            'verified_current_pairs_before_selection':active,'summary_sha256':confirmed['summary_sha256'],
            'preflight_observations':observations}
    with (HERE/'PRIMARY_SELECTION_RECEIPT_20260908.json').open('xb') as f:f.write(worker.encoded(record))
    print(json.dumps(record))

if __name__=='__main__':main()
