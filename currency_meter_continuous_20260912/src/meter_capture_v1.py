"""Current-only passive currency capture; no broker or model fitting interfaces."""
from __future__ import annotations
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'vendor'))
import collect_current_meter as recipe
from sealed_meter_adapter import CURRENCIES, MODEL_INPUTS, METRICS, INACTIVE_MODEL, epoch, number
import meter_store_v1 as store
from meter_runtime_support_v1 import ClockGuard

CONTRACT='continuous_currency_meter_capture_v1_20260912'
COHORT='continuous_currency_meter_fresh_observations_v1_20260912'
FLAGS={'research_only':True,'execution_eligible':False,'forecast':False,'can_place_orders':False,'can_promote':False}


def utcnow():return datetime.now(timezone.utc)
def sha(raw):return hashlib.sha256(raw).hexdigest()


def verify_release(root=ROOT):
    manifest=json.loads((root/'RELEASE_MANIFEST.json').read_text(encoding='utf-8'))
    if manifest['schema_version']!='currency_meter_release_v1_20260912':raise ValueError('release_schema_changed')
    for name,expected in manifest['files'].items():
        path=(root/name).resolve()
        if not path.is_relative_to(root.resolve()) or sha(path.read_bytes())!=expected:
            raise ValueError('release_source_changed')
    return sha((root/'RELEASE_MANIFEST.json').read_bytes())


def load_profile(root=ROOT):
    profile=json.loads((root/'PROFILE.json').read_text(encoding='utf-8'))
    if profile['schema_version']!='continuous_currency_meter_profile_v1_20260912':raise ValueError('profile_schema_changed')
    if any(profile.get(key) is not val for key,val in FLAGS.items()):raise ValueError('profile_research_flags_changed')
    if profile['capture_contract']!=CONTRACT or profile['capture_cohort']!=COHORT:raise ValueError('profile_contract_changed')
    output=(root/profile['database']).resolve();runtime=(root/'runtime').resolve()
    if not output.is_relative_to(runtime):raise ValueError('output_outside_dedicated_runtime')
    if output.is_relative_to(Path(profile['source_project']).resolve()):raise ValueError('output_inside_source_project')
    return profile


def storage_limits(profile):return store.Limits(**profile['storage_limits'])


def observe_upstream(path,*,now,latest_path=None,maximum_age=120,maximum_progress_age=900):
    """Capture heartbeat bytes; a quiet news cycle is distinct from a dead feed."""
    started=utcnow()
    with Path(path).open('rb') as handle:raw=handle.read(64*1024+1)
    if len(raw)>64*1024:raise ValueError('upstream_heartbeat_oversized')
    value=json.loads(raw)
    if value.get('collector_contract_id')!=recipe.COLLECTOR or value.get('classification_version')!=recipe.CLASSIFIER:
        raise ValueError('upstream_contract_changed')
    policy=value.get('policy',{})
    if policy.get('research_only') is not True or policy.get('execution_eligible') is not False:
        raise ValueError('upstream_policy_changed')
    heartbeat=epoch(value['heartbeat_utc']);progress=epoch(value['last_progress_utc'])
    age=now-heartbeat;progress_age=now-progress
    if min(age,progress_age)<-2:raise ValueError('upstream_future_clock')
    if age>maximum_age:raise ValueError('upstream_heartbeat_stale')
    if progress_age>maximum_progress_age:raise ValueError('upstream_progress_stalled')
    if value.get('status') not in {'running_cycle','cycle_complete'}:raise ValueError('upstream_reports_failure')
    latest_path=latest_path or Path(path).with_name('collector_latest_v1.json')
    with Path(latest_path).open('rb') as handle:latest_raw=handle.read(128*1024+1)
    if len(latest_raw)>128*1024:raise ValueError('upstream_cycle_report_oversized')
    latest=json.loads(latest_raw)
    if latest.get('status')!='ok':raise ValueError('upstream_latest_cycle_not_successful')
    latest_age=now-epoch(latest['generated_utc'])
    if not -2<=latest_age<=1800:raise ValueError('upstream_completed_cycle_stale_or_future')
    ended=utcnow()
    return {'raw_base64':base64.b64encode(raw).decode(),'raw_sha256':sha(raw),
            'latest_cycle_base64':base64.b64encode(latest_raw).decode(),'latest_cycle_sha256':sha(latest_raw),
            'latest_successful_cycle_utc':latest['generated_utc'],
            'read_started_utc':started.isoformat(),'read_completed_utc':ended.isoformat(),
            'heartbeat_utc':value['heartbeat_utc'],'last_progress_utc':value['last_progress_utc'],
            'phase':value.get('phase'),'status':value['status'],'heartbeat_age_seconds':age,'progress_age_seconds':progress_age}


def observation_state(raw_rows,admitted,states):
    if not raw_rows:return 'source_empty'
    if not admitted:return 'all_source_rows_excluded'
    if any(abs(row['model_scores'][name])>0 for row in states for name in MODEL_INPUTS):return 'unvalidated_directional_channels_present'
    return 'observed_zero_channels'


def source_lineages(project):
    """Bind each source ID to its own configured lineage, not two separate sets."""
    data=json.loads((Path(project)/'config/news_sources_v1.json').read_text(encoding='utf-8'))
    excluded={'source_contract_id','source_cohort_id','source_contract_derived','source_lineage_version','source_config_sha256'}
    mapping={}
    for source in data['sources']:
        source_id=source['source_id']
        if source_id in mapping:raise ValueError('duplicate_configured_source_id')
        contract=source.get('source_contract_id')
        if not contract:
            material={key:value for key,value in source.items() if key not in excluded}
            digest=sha(json.dumps(material,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode())
            safe_id=re.sub(r'[^a-z0-9_.-]+','_',source_id.casefold()).strip('_') or 'unknown'
            contract='derived_source_config_lineage_v1:'+safe_id+':'+digest[:24]
        mapping[source_id]=(contract,source.get('source_cohort_id') or contract)
    return mapping


def admit_current_rows(rows,*,asof,mapping):
    admitted,report=recipe.admit_rows(rows,asof=asof,allowed_source_ids=set(mapping),allowed_lineages=set(mapping.values()))
    kept=[]
    for row in admitted:
        payload=json.loads(row['payload_json'])
        if mapping[row['source_id']]!=(payload['source_contract_id'],payload['source_cohort_id']):
            reason='source_id_lineage_mismatch'
            report['exclusions'].append({'event_id':row['event_id'],'source_id':row['source_id'],'reasons':[reason]})
            report['exclusion_reason_counts'][reason]=report['exclusion_reason_counts'].get(reason,0)+1
        else:kept.append(row)
    report['admitted_source_rows']=len(kept);report['excluded_source_rows']=len(rows)-len(kept)
    report['source_identity_policy']='exact_source_id_to_configured_contract_and_cohort'
    return kept,report


def compact_capture(database,capture_id,*,decision_utc):
    saved=store.read_capture(database,capture_id)
    feature=saved['feature'];receipt=saved['receipt']
    ready=epoch(receipt['observed_ready_utc']);decision=epoch(decision_utc)
    states=feature['current_states'];clock=epoch(states[0]['clock_utc'])
    if feature['schema_version']!=CONTRACT or feature['cohort_id']!=COHORT:raise ValueError('capture_contract_changed')
    if len(states)!=21 or {r['currency'] for r in states}!=set(CURRENCIES) or len({r['clock_utc'] for r in states})!=1:
        raise ValueError('incomplete_currency_set')
    if not clock<=epoch(feature['computation_started_utc'])<=epoch(feature['computation_completed_utc'])<=ready:
        raise ValueError('capture_semantic_clock_order')
    if clock%300 or int((epoch(feature['computation_started_utc'])-60)//300)*300!=clock:
        raise ValueError('capture_context_bucket_changed')
    if any(feature.get(k) is not v for k,v in FLAGS.items()):raise ValueError('capture_research_flags_changed')
    if not re.fullmatch('[a-f0-9]{64}',feature['source_bindings'].get('continuous_capture_release_sha256','')):
        raise ValueError('capture_release_binding_missing')
    for row in states:
        if set(row['model_scores'])!=set(MODEL_INPUTS)|{INACTIVE_MODEL}:raise ValueError('capture_channel_set_changed')
        if row['model_scores'][INACTIVE_MODEL]!=0:raise ValueError('inactive_analog_changed')
        if any(abs(number(v,k))>1 for k,v in row['model_scores'].items()):raise ValueError('capture_score_range')
        for key in METRICS:number(row[key],key)
    if ready>decision:reason='decision_before_actual_publication'
    elif decision-clock>300:reason='stale_original_context_clock'
    elif feature['observation_state'] in {'source_empty','all_source_rows_excluded'}:reason=feature['observation_state']
    else:reason='available'
    nonzero={name:sum(abs(number(row['model_scores'][name],name))>0 for row in states) for name in MODEL_INPUTS}
    return {'capture_id':capture_id,'observed_ready_utc':receipt['observed_ready_utc'],
            'context_clock_utc':states[0]['clock_utc'],'context_age_seconds':decision-clock,
            'available':reason=='available','reason':reason,'observation_state':feature['observation_state'],
            'source_rows':len(saved['raw']['original_rows']),'admitted_source_rows':feature['source_admission']['admitted_source_rows'],
            'excluded_source_rows':feature['source_admission']['excluded_source_rows'],
            'nonzero_currency_channels':nonzero,'currency_count':21,
            'upstream_heartbeat_utc':feature['upstream_observation']['heartbeat_utc'],
            'raw_sha256':receipt['raw_sha256'],'feature_sha256':receipt['feature_sha256'],**FLAGS}


def capture_once(*,profile,bucket_epoch,wall=utcnow,monotonic=time.monotonic,release_sha=None,dispatch_clock=None):
    database=(ROOT/profile['database']).resolve()
    project=Path(profile['source_project']);source=Path(profile['source_database'])
    limits=storage_limits(profile)
    release_sha=release_sha or verify_release()
    bindings=dict(recipe.PINS,continuous_capture_release_sha256=release_sha)
    clock_guard=ClockGuard(prior_highwater=dispatch_clock[0] if dispatch_clock else 0)
    if dispatch_clock:clock_guard.check(*dispatch_clock)
    def checked_wall():
        value=wall();clock_guard.check(value.timestamp(),monotonic());return value
    old=store.find_bucket_status(database,bucket_epoch,cohort_id=COHORT) if database.exists() else None
    if old:
        if not old['published']:store.recover_pending(database,old['capture_id'],limits=limits,clock=checked_wall)
        return {'status':'existing_bucket','capture':compact_capture(database,old['capture_id'],decision_utc=checked_wall().isoformat())}
    start=checked_wall();mono_start=monotonic()
    if not bucket_epoch+90<=start.timestamp()<=bucket_epoch+210:raise ValueError('outside_current_capture_window')
    module,ids,lineages=recipe.load_pinned_producer(project)
    mapping=source_lineages(project)
    upstream=observe_upstream(profile['upstream_heartbeat'],now=start.timestamp())
    read_start=checked_wall()
    if not start.timestamp()<=epoch(upstream['read_started_utc'])<=epoch(upstream['read_completed_utc'])<=read_start.timestamp():
        raise ValueError('upstream_observation_clock_order')
    rows,read_receipt=recipe.load_rows(source,started=read_start)
    admitted,admission=admit_current_rows(rows,asof=read_start,mapping=mapping)
    computation=checked_wall()
    if int((computation.timestamp()-60)//300)*300!=bucket_epoch:raise ValueError('bucket_changed_before_computation')
    states,selection=recipe.build_current(module,admitted,computed_asof=computation)
    ended=checked_wall();elapsed=monotonic()-mono_start
    if abs((ended-start).total_seconds()-elapsed)>2:raise ValueError('capture_wall_clock_discontinuity')
    if not start<=read_start<=datetime.fromisoformat(read_receipt['read_completed_utc'])<=computation<=ended:
        raise ValueError('capture_clock_order')
    if ended.timestamp()-bucket_epoch>290:raise ValueError('capture_too_old_before_commit')
    # Check again after computation; never stamp an old heartbeat as current.
    if ended.timestamp()-epoch(upstream['heartbeat_utc'])>120:raise ValueError('upstream_stale_before_publication')
    capture_id=start.strftime('%Y%m%dT%H%M%S%fZ')+'_'+uuid.uuid4().hex
    raw={'source_path':str(source.resolve()),'source_bindings':bindings,'read_receipt':read_receipt,'capture_started_utc':start.isoformat(),
         'original_rows':rows,'upstream_heartbeat':upstream}
    feature={'schema_version':CONTRACT,'cohort_id':COHORT,'capture_id':capture_id,
             'computation_started_utc':computation.isoformat(),'computation_completed_utc':ended.isoformat(),
             'state_availability':'requires_actual_postcommit_publication_receipt','history_outputs_published':0,
             'currency_count':21,'current_states':states,'source_admission':admission,'formula_selection':selection,
             'source_bindings':bindings,'collector_contract_id':recipe.COLLECTOR,'classification_version':recipe.CLASSIFIER,
             'upstream_observation':{k:v for k,v in upstream.items() if k not in {'raw_base64','latest_cycle_base64'}},
             'observation_state':observation_state(rows,admitted,states),
             'clock_claim':'original article versions retained; current state available only at new receipt',**FLAGS}
    # Store JSON-owned canonical values; original formula datetime precision is preserved.
    raw=json.loads(recipe.encoded(raw));feature=json.loads(recipe.encoded(feature))
    receipt=store.publish(database,raw,feature,capture_id,limits=limits,clock=checked_wall)
    return {'status':'published','capture':compact_capture(database,capture_id,decision_utc=checked_wall().isoformat()),
            'elapsed_seconds':monotonic()-mono_start,'storage':store.stats(database)}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--bucket-epoch',type=int,required=True)
    parser.add_argument('--dispatch-wall',type=float,required=True);parser.add_argument('--dispatch-monotonic',type=float,required=True)
    args=parser.parse_args()
    try:
        release=verify_release();result=capture_once(profile=load_profile(),bucket_epoch=args.bucket_epoch,release_sha=release,
                                                   dispatch_clock=(args.dispatch_wall,args.dispatch_monotonic))
    except Exception as exc:
        known=str(exc)
        # Never emit source bodies, SQL, paths, provider response text or arbitrary exceptions.
        code=known if known and len(known)<=96 and all(c.islower() or c.isdigit() or c in '_:' for c in known) else 'capture_exception_'+type(exc).__name__
        print(json.dumps({'status':'failed','reason':code,**FLAGS}),flush=True)
        return 1
    print(json.dumps(result,allow_nan=False),flush=True);return 0


if __name__=='__main__':raise SystemExit(main())
