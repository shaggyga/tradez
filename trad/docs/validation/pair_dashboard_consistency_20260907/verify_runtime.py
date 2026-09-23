"""Read-only dashboard follow-up checks; never activate or start a worker."""
from __future__ import annotations
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from urllib.request import urlopen

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
sys.dont_write_bytecode=True
RECEIPTS={
 'FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json':'6953f2d7259be748847ec0dd6a34abd1b2f482dd8be7aa6913bc7d906ff4cfb5',
 'FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json':'3671297affd5addd4c2e4080c66d4d74382c24678a3430d7e4694814e5993792'}
REGISTRY_SHA='e0aa74fa5d20b7be983050625773834ea68d74ffc15b566ab5e03df9b3380d65'
DASHBOARD='oanda_practice_live_dashboard.py'
SUPERVISOR='oanda_always_on_supervisor.ps1'
WORKERS={DASHBOARD,'oanda_account_snapshot_writer.py','oanda_local_news_sentiment.py',
 'oanda_official_release_fast_lane.py','oanda_official_release_fast_mapper.py',
 'oanda_source_governance_news_fast_lane.py','oanda_practice_quote_stream.py',
 'oanda_clock_integrity_monitor.py','oanda_all68_m1_forward_updater.py',
 'oanda_project_integrity_audit.py','oanda_storage_headroom_guard.py',
 'oanda_causal_forecast_study_gap_v2.py','oanda_causal_forecast_study_eurusd_v1.py',
 'oanda_pair_local_forecast_study_v1.py'}


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def load(path):return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def processes():
    command=r'''
$projectPattern=[regex]::Escape('C:\Users\zmoor\Documents\forex\trad')
$rows=@(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -match '^(python|powershell|pwsh)\.exe$' -and $_.CommandLine -match $projectPattern -and
    $_.CommandLine -match '(?:oanda_[A-Za-z0-9_]+\.py|oanda_always_on_supervisor\.ps1)'
} | ForEach-Object {
    $scriptMatch=[regex]::Match($_.CommandLine,'(oanda_[A-Za-z0-9_]+\.(?:py|ps1))')
    [pscustomobject]@{script=$scriptMatch.Value;pid=[int]$_.ProcessId;parent_pid=[int]$_.ParentProcessId;
        created_utc=$_.CreationDate.ToUniversalTime().ToString('o');
        research_only=($_.CommandLine -match '\s-ResearchCollectionOnly(?:\s|$)');
        safe_core_only=($_.CommandLine -match '\s-SafeCoreOnly(?:\s|$)')}
})
ConvertTo-Json -InputObject $rows -Depth 3 -Compress
'''
    result=subprocess.run(['powershell.exe','-NoLogo','-NoProfile','-NonInteractive','-Command',command],
        capture_output=True,text=True,check=True,timeout=15)
    return json.loads(result.stdout)


def verify_processes(rows, baseline=None):
    supervisors=[p for p in rows if p['script']==SUPERVISOR]
    if (len(supervisors)!=1 or supervisors[0]['pid']!=22400 or not supervisors[0]['research_only']
            or not supervisors[0]['safe_core_only']):raise ValueError('supervisor_22400_identity_or_mode_changed')
    grouped={script:[row for row in rows if row['script']==script] for script in WORKERS}
    # The admitted news collector may run its short-lived event-tagger child.
    # It is not an additional supervised worker or a stable reload identity.
    extras=[row for row in rows if row['script'] not in WORKERS|{SUPERVISOR}]
    news_pids={row['pid'] for row in grouped['oanda_local_news_sentiment.py']}
    auxiliary_pids={row['pid'] for row in extras if row['script']=='oanda_news_event_tagger.py' and row['parent_pid'] in news_pids}
    if any(row['script']!='oanda_news_event_tagger.py' or row['parent_pid'] not in news_pids|auxiliary_pids for row in extras):
        raise ValueError('unexpected_project_worker')
    if any(len(value)!=2 for value in grouped.values()):raise ValueError('expected_14_launcher_child_pairs')
    dashboard=grouped[DASHBOARD]
    launcher=[row for row in dashboard if row['parent_pid']==22400]
    if len(launcher)!=1 or not any(row['parent_pid']==launcher[0]['pid'] for row in dashboard):
        raise ValueError('dashboard_not_owned_by_expected_supervisor')
    if baseline is not None:
        before=[row for row in baseline['processes'] if row['script'] in (WORKERS|{SUPERVISOR})-{DASHBOARD}]
        after=[row for row in rows if row['script'] in (WORKERS|{SUPERVISOR})-{DASHBOARD}]
        if sorted(before,key=lambda row:row['pid'])!=sorted(after,key=lambda row:row['pid']):
            raise ValueError('non_dashboard_process_identity_changed')
        old_pids={row['pid'] for row in baseline['processes'] if row['script']==DASHBOARD}
        if old_pids & {row['pid'] for row in dashboard}:raise ValueError('dashboard_not_reloaded')
    return {'running_worker_count':14,'preserved_other_worker_count':13,'preserved_other_process_count':26,
        'supervisor_pid':22400,'dashboard_pids':[row['pid'] for row in dashboard]}


def registration():
    sys.path.insert(0,str(ROOT))
    from oanda_pair_local_forecast_study_v1 import load_registry,digest,encoded
    registry=load_registry(ROOT/'config/pair_local_forecast_study_v1_20260907.json')
    if digest(registry)!=REGISTRY_SHA:raise ValueError('frozen_registry_changed')
    for name,expected in RECEIPTS.items():
        if sha(ROOT/name)!=expected:raise ValueError('prior_receipt_changed:'+name)
    if len(registry['pairs'])!=68:raise ValueError('all68_registry_required')
    activations=[]
    for pair,item in registry['pairs'].items():
        path=ROOT/'data/oanda_training_manager/pair_local_forecast_study_v1/pairs'/pair/'study.sqlite'
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
            db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
            active=db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
            retained=db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
            if (active is None or active[1]!=item['contract_sha256'] or retained is None
                    or retained!=(item['contract_sha256'],encoded(item['contract']).decode())):
                raise ValueError('immutable_pair_registration_changed:'+pair)
            activations.append({'instrument':pair,'activation_epoch':active[0],'contract_sha256':active[1]})
            db.rollback()
    return {'registry_sha256':REGISTRY_SHA,'source_bindings':registry['source_bindings'],
        'prior_receipts_unchanged':RECEIPTS,'activations':activations}


def api_observations(samples,interval):
    rows=[]
    for index in range(samples):
        with urlopen('http://127.0.0.1:8765/api/main',timeout=10) as response:api=json.load(response)
        observed=time.time();pair=api['pair_local_forecasts']
        row={'observed_epoch':observed,'status':pair.get('status'),'reason':pair.get('reason'),
            'generated_epoch':pair.get('generated_epoch'),'counts':pair.get('counts'),
            'registry_sha256':pair.get('registry_sha256'),'summary_sha256':pair.get('summary_sha256')}
        rows.append(row)
        if (pair.get('status')!='current' or pair.get('registry_sha256')!=REGISTRY_SHA
                or len(pair.get('rows',[]))!=68 or pair.get('can_place_orders') is not False
                or pair.get('can_promote') is not False):raise ValueError('pair_api_not_current:'+json.dumps(row))
        if index+1<samples:time.sleep(interval)
    return rows


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--baseline',type=Path)
    parser.add_argument('--preflight-only',action='store_true')
    parser.add_argument('--samples',type=int,default=1)
    parser.add_argument('--interval-sec',type=float,default=.5)
    args=parser.parse_args()
    if not 1<=args.samples<=30 or not 0<=args.interval_sec<=1:raise ValueError('bounded_observation_parameters_required')
    destination=args.output.resolve()
    if not destination.is_relative_to(OUT.resolve()):raise ValueError('evidence_output_path_required')
    baseline=load(args.baseline) if args.baseline else None
    registered=registration();current=processes();process_checks=verify_processes(current,baseline)
    observations=[] if args.preflight_only else api_observations(args.samples,args.interval_sec)
    report={'schema_version':'pair_dashboard_consistency_runtime_v1_20260907','status':'passed',
        'observed_utc':datetime.now(timezone.utc).isoformat(),'preflight_only':args.preflight_only,
        'processes':current,'process_checks':process_checks,'registration':registered,
        'dashboard_source_sha256':sha(ROOT/DASHBOARD),'api_observations':observations,
        'baseline':str(args.baseline) if args.baseline else None,
        'runtime_writes':0,'process_starts':0,'process_stops':0,'can_place_orders':False,
        'prediction_improvement_demonstrated':False}
    destination.parent.mkdir(parents=True,exist_ok=True)
    with destination.open('x',encoding='utf-8') as handle:json.dump(report,handle,indent=2);handle.write('\n')
    print(json.dumps({'status':'passed','receipt':str(destination),'samples':len(observations),**process_checks}))


if __name__=='__main__':main()
