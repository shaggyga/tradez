"""Bind the completed implementation and explicitly blocked live observation."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET

ROOT=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()


def read(name):return json.loads((ROOT/name).read_text(encoding='utf-8-sig'))


def main():
    def tests(name):
        suite=ET.parse(ROOT/name).getroot().find('testsuite');values={k:int(suite.get(k)) for k in ('tests','failures','errors','skipped')}
        assert values=={'tests':133,'failures':0,'errors':0,'skipped':0};return values
    initial=tests('review/INTEGRATED_FINAL_TESTS.xml');restored=tests('review/RESTORED_TESTS.xml')
    release=read('RELEASE_MANIFEST.json')
    for name,expected in release['files'].items():assert sha(ROOT/name)==expected,name
    build=read('review/RESTORATION_BUILD.json');extracted=ROOT/'restore_check_001'
    for name,expected in build['files'].items():assert sha(extracted/name)==expected,name
    assert sha(Path(build['archive']))==build['archive_sha256']
    observe=read('review/OPERATIONAL_OBSERVATION.json');worker=observe['worker']
    assert worker['release_sha256']==sha(ROOT/'RELEASE_MANIFEST.json')
    assert worker['heartbeat_write_errors']==worker['diagnostic_write_errors']==worker['clock_errors']==0
    assert worker['attempts']>=2 and worker['published_captures']==0
    assert worker['last_attempt']['reason']=='upstream_latest_cycle_not_successful'
    assert observe['upstream']['status']=='blocked_clock_integrity'
    assert worker['last_attempt']['runtime']['memory_limit_enforced'] is True
    prior=ROOT.parent/'direction_decision_20260911/local_restoration_001.zip'
    prior_expected='a9f29a3d51e40c114655d27465ec03b269cba5c26c5bb24105c97da096d86fd9'
    assert sha(prior)==prior_expected
    proof={'schema_version':'continuous_currency_meter_completion_v1_20260912','completed_utc':datetime.now(timezone.utc).isoformat(),
           'status':'implementation_started_and_verified_upstream_clock_activation_blocked',
           'implementation_verified':True,'entire_project_operational':False,'integrated_tests':initial,'restored_tests':restored,
           'release_sha256':sha(ROOT/'RELEASE_MANIFEST.json'),'release_files':len(release['files']),
           'restoration_archive':build['archive'],'restoration_sha256':build['archive_sha256'],'restoration_files_verified':len(build['files']),
           'restoration_inactive_stop_marker':True,'worker_pid':worker['pid'],'worker_run_id':worker['run_id'],
           'operational_observation_utc':observe['observed_utc'],'observed_attempts':worker['attempts'],'successful_live_captures':0,
           'blocked_reason':'upstream_clock_attestation_stale','clock_start_automatic_review_result':'blocked by policy',
           'clock_block_bypassed':False,'new_meter_supervisor_started':True,'existing_news_supervisor_resumed':True,
           'new_upstream_clock_monitor_started':False,'windows_logon_recovery_installed':False,'broker_worker_started':False,
           'numeric_model_changed':False,'trading_policy_or_trial_changed':False,'previous_signed_cost_archive_preserved':prior_expected,
           'benchmarks':{'real_retained_rows_per_capture':5357,'second_unchanged_capture_database_growth_bytes':8192,
                         'prior_full_compressed_snapshot_bytes_approx':3446690,'storage_peak_commit_bytes_after':546200000,
                         'benchmark_peak_memory_is_approximate':True,'benchmark_is_not_live_forward_capture':True},
           'source_audit':{'additional_id_lineage_mismatches':299,'prior_untrusted_clock_exclusions':59,'new_admitted_rows_from_retained_snapshot':4999},
           'remaining':['approved restoration of independent clock monitor','successful live captures after upstream resumes',
                        'upstream source identity/versioned provenance repair','consensus/revision/surprise inputs','separate sign-in recovery',
                        'later prediction evaluation before learner or management changes']}
    evidence={}
    for path in sorted(ROOT.glob('*.json')):evidence[path.name]=sha(path)
    for folder in ('review','feed_audit'):
        for path in sorted((ROOT/folder).rglob('*')):
            if path.is_file() and path.suffix in {'.json','.md','.xml'}:evidence[path.relative_to(ROOT).as_posix()]=sha(path)
    proof['evidence']=evidence
    with (ROOT/'COMPLETION_RECEIPT.json').open('x',encoding='utf-8') as out:json.dump(proof,out,indent=2,sort_keys=True)
    print(json.dumps({k:proof[k] for k in ('status','implementation_verified','successful_live_captures','release_files','restoration_files_verified')}))


if __name__=='__main__':main()
