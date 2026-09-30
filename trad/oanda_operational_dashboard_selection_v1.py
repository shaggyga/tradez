"""Bounded read-only projection of explicitly selected current research producers.

Producer summary and heartbeat seals are checked. This is not an independent
ledger rescore, fitted-model validation, accuracy report or execution authority.
"""
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import re
import time

SCHEMA = 'operational_dashboard_selection_v1_20260913'
POINTER = 'config/operational_dashboard_current.json'
SOURCE_FILES = frozenset({'oanda_operational_dashboard_selection_v1.py',
    'oanda_practice_live_dashboard.py','oanda_main_signal_dashboard.html',
    'oanda_feature_move_mapping_v1.py','oanda_feature_observations_v1.py'})
FLAGS = ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible')
KINDS = {
    'price':('pair_local_forecast_registry_v3_20260913','pair_local_forecast_summary_v3_20260913',
             'pair_local_forecast_heartbeat_v3_20260913','pair_local_price_controls_v3_activation_receipt_20260913',
             'pair_local_forecast_study_v3','oanda_pair_local_forecast_study_v3.py',3,
             frozenset({'probabilistic_state_space','ridge_return_repaired'})),
    'joint':('joint_price_news_registry_v7_20260913','joint_price_news_forecast_summary_v7_20260913',
             'joint_price_news_forecast_heartbeat_v7_20260913','joint_price_news_v7_activation_receipt_20260913',
             'joint_price_news_study_v7','oanda_joint_price_news_forecast_study_v7.py','joint_v7',
             frozenset({'ridge_price_news_v1'})),
}


JOINT_V9 = ('joint_price_news_registry_v9_20260916','joint_price_news_forecast_summary_v9_20260916',
    'joint_price_news_forecast_heartbeat_v9_20260916','joint_price_news_v9_activation_receipt_20260916',
    'joint_price_news_study_v9','oanda_joint_price_news_forecast_study_v9.py','joint_v9',
    frozenset({'ridge_price_news_v1'}))


JOINT_V10 = ('joint_price_news_registry_v10_20260930','joint_price_news_forecast_summary_v10_20260930',
    'joint_price_news_forecast_heartbeat_v10_20260930','joint_price_news_v10_activation_receipt_20260930',
    'joint_price_news_study_v10','oanda_joint_price_news_forecast_study_v10.py','joint_v10',
    frozenset({'ridge_price_news_v1'}))


def study_spec(kind, source):
    if kind=='joint':
        for spec in (JOINT_V9, JOINT_V10):
            if Path(source.get('study_path','')).name==spec[4]:
                return spec
    return KINDS[kind]


class SelectionError(ValueError): pass


def need(value, code):
    if not value: raise SelectionError(code)


def encoded(value): return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(value): return hashlib.sha256(encoded(value)).hexdigest()
def ishash(value): return type(value) is str and re.fullmatch('[a-f0-9]{64}',value) is not None


def number(value):
    need(type(value) in (int,float,str) and type(value) is not bool,'invalid_number')
    result=float(value);need(math.isfinite(result),'nonfinite_number');return result


def stamp(value):
    need(type(value) in (int,float),'invalid_clock_type')
    result=number(value);need(0<result<1e11,'invalid_clock');return result


def relative(root, value, *, data=False):
    need(type(value) is str and 0<len(value)<=512,'invalid_selected_path')
    part=Path(value);need(not part.is_absolute() and '..' not in part.parts,'relative_selected_path_required')
    root=Path(root).absolute();path=root/part
    for cursor in (path,*path.parents):
        need(not cursor.is_symlink() and not cursor.is_junction(),'selected_reparse_path')
    boundary=root/'data/oanda_training_manager' if data else root
    need(path.resolve().is_relative_to(boundary.resolve()),'selected_path_escape')
    return path


def read_raw(path, cap=1024*1024):
    with Path(path).open('rb') as handle:
        before=Path(path).stat();raw=handle.read(cap+1);after=Path(path).stat()
    need(0<len(raw)<=cap,'selected_source_size')
    need((before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns),
         'selected_source_changed')
    return raw


def read_json(path, cap=1024*1024):
    raw=read_raw(path,cap)
    def unique(items):
        out={}
        for key,value in items:
            need(key not in out,'duplicate_json_key');out[key]=value
        return out
    value=json.loads(raw,object_pairs_hook=unique,parse_constant=lambda _:need(False,'nonfinite_json'))
    need(type(value) is dict,'selected_object_required')
    return raw,value


def inert(value, flags=FLAGS):
    need(value.get('research_only') is True and all(value.get(key) is False for key in flags),'selected_authority_changed')


def read_selection(root, now):
    path=Path(root)/POINTER
    if not path.exists():return None
    raw,value=read_json(path,16384)
    need(value.get('schema_version')==SCHEMA,'selection_schema')
    inert(value,FLAGS[:3]);need(stamp(value['activated_epoch'])<=now,'selection_future')
    bindings=value.get('source_bindings')
    need(type(bindings) is dict and set(bindings)==SOURCE_FILES,'selection_source_inventory')
    for name,sha in bindings.items():
        need(ishash(sha) and hashlib.sha256(read_raw(relative(root,name),2*1024*1024)).hexdigest()==sha,'dashboard_source_changed')
    for kind,spec in KINDS.items():
        source=value.get(kind)
        need(type(source) is dict and set(source)=={'registry_path','registry_sha256','study_path','activation_sha256'},'study_selection_shape')
        spec=study_spec(kind,source)
        need(ishash(source['registry_sha256']) and ishash(source['activation_sha256']),'selection_hash')
        need(relative(root,source['study_path'],data=True).name==spec[4],'selected_study_version')
        relative(root,source['registry_path'])
    features=value.get('features')
    need(type(features) is dict and set(features)=={'archive_path'},'feature_selection_shape')
    need(relative(root,features['archive_path'],data=True).name=='feature_observations_v2','feature_archive_version')
    need(read_raw(path,16384)==raw,'selection_changed')
    return value


def project_forecast(publication, *, pair, family, contract, pip, now, generated, native):
    need(type(publication) is dict and publication.get('publication_verified') is True
         and publication.get('consumption_verified') is True,'unverified_publication')
    for key in ('forecast_sha256','publication_receipt_sha256','consumer_receipt_sha256'):
        need(ishash(publication.get(key)),'publication_hash')
    arms=publication.get('forecasts');need(type(arms) is list and len(arms)==1,'single_forecast_arm')
    arm=arms[0];need(type(arm) is dict,'forecast_arm_shape')
    ref,issued,published,consumed,target=(stamp(publication[k]) for k in
        ('reference_epoch','issued_epoch','publication_epoch','consumption_epoch','target_epoch'))
    need(ref<=issued<=published<=consumed<=generated<=now and target-ref==3600,'forecast_clock_order')
    need(publication.get('instrument')==pair and publication.get('family')==family
         and publication.get('horizon_sec')==3600,'publication_identity')
    need(arm.get('instrument')==pair and arm.get('family')==family
         and arm.get('cohort_id')==contract['cohorts'][family]
         and all(arm.get(k)==publication.get(k) for k in ('reference_epoch','issued_epoch','target_epoch','horizon_sec')),
         'forecast_contract_identity')
    probability=number(arm.get('probability_up'));mid=number(arm.get('reference_mid'))
    number(arm.get('predicted_return_bps'))
    need(0<=probability<=1 and mid>0 and number(arm.get('pip_size'))==number(pip)
         and type(arm.get('side')) is int and arm['side'] in (-1,0,1),'forecast_numeric_bounds')
    if native:
        inert(arm);anchor=arm.get('native_anchor')
        need(type(anchor) is dict and digest(anchor)==arm.get('native_anchor_sha256')==publication.get('native_anchor_sha256'),
             'native_anchor_binding')
        need(stamp(anchor['origin_close_epoch'])==ref and stamp(anchor['target_close_epoch'])==target
             and float.fromhex(anchor['origin_mid_hex'])==mid
             and float.fromhex(anchor['native_probability_up_hex'])==probability,'native_anchor_semantics')
    if target<=now:return None
    return {**arm,'publication_epoch':published,'consumption_epoch':consumed,
            'reference_available_epoch':publication.get('reference_available_epoch'),
            'forecast_sha256':publication['forecast_sha256'],
            'display_evidence_scope':'source-bound producer summary; not independent ledger rescore'}


def read_study(root, selection, kind, now, *, observed_clock=None):
    source=selection[kind];spec=study_spec(kind,source);study=relative(root,source['study_path'],data=True)
    registry_raw,registry=read_json(relative(root,source['registry_path']))
    need(hashlib.sha256(registry_raw).hexdigest()==source['registry_sha256'],'selected_registry_changed')
    need(registry.get('schema_version')==spec[0],'registry_schema');inert(registry)
    bindings=registry.get('source_bindings')
    need(type(bindings) is dict and spec[5] in bindings and 1<=len(bindings)<=100,'model_source_inventory')
    for name,sha in bindings.items():
        need(ishash(sha) and hashlib.sha256(read_raw(relative(root,name),2*1024*1024)).hexdigest()==sha,'model_source_changed')
    activation_raw,activation=read_json(study/'activation_receipt.json')
    need(hashlib.sha256(activation_raw).hexdigest()==source['activation_sha256'],'activation_changed')
    inert(activation)
    need(activation.get('schema_version')==spec[3] and activation.get('status')=='activated_empty'
         and activation.get('registry_file_sha256')==source['registry_sha256']
         and activation.get('registry_sha256')==digest(registry) and activation.get('source_bindings')==bindings
         and Path(activation.get('study_root','')).resolve()==study.resolve(),'activation_registry_identity')
    need(stamp(activation['activation_completed_epoch'])<=now,'activation_future')
    # Summaries and heartbeats are separate atomic publications; the price
    # heartbeat cadence can lag its summary by five seconds. Retry only these
    # generation races, for at most 12 reads / 5.5 seconds of waiting. Never use
    # a previous generation, wait out a future timestamp or renew a source clock.
    for attempt in range(12):
        try:
            raw,summary=read_json(study/'summary.json');_,heartbeat=read_json(study/'heartbeat.json')
            observed=stamp(observed_clock()) if observed_clock is not None else now
            need(stamp(summary['generated_epoch'])<=observed and stamp(heartbeat['generated_epoch'])<=observed,
                 'stale_or_future_summary')
            need(heartbeat.get('summary_sha256')==digest(summary),'summary_heartbeat_generation_mismatch')
            now=observed
            break
        except SelectionError as exc:
            if str(exc) not in {'selected_source_changed','summary_heartbeat_generation_mismatch'} or attempt==11:raise
            time.sleep(.5)
    need(summary.get('schema_version')==spec[1] and heartbeat.get('schema_version')==spec[2],'current_summary_schema')
    inert(summary);inert(heartbeat)
    need(heartbeat.get('status') not in {'failed','error','unavailable','stopped','partial_unavailable'}
         and heartbeat.get('phase') not in {'failed','cycle_failed','stopped'},'producer_reports_failure')
    need(summary.get('registry_sha256')==heartbeat.get('registry_sha256')==digest(registry),'summary_registry_mismatch')
    need(summary.get('payload_sha256')==digest({k:v for k,v in summary.items() if k!='payload_sha256'}),'summary_payload_seal')
    generated,reported=stamp(summary['generated_epoch']),stamp(heartbeat['generated_epoch'])
    need(generated<=reported<=now and now-generated<=90 and now-reported<=90,'stale_or_future_summary')
    if spec is JOINT_V9 or spec is JOINT_V10:
        need(heartbeat.get('summary_boundary_version')=='immutable_family_summary_publication_v1_20260909',
             'immutable_summary_boundary_version')
    if kind=='joint':
        need(heartbeat.get('summary_generated_epoch')==generated
             and generated<=stamp(heartbeat.get('summary_read_completed_epoch'))<=reported,'native_summary_readback_clock')
    pairs=registry.get('pairs');rows=summary.get('rows')
    need(type(pairs) is dict and 1<=len(pairs)<=68 and type(rows) is list and len(rows)==len(pairs),'summary_pair_coverage')
    seen=set();projected=[];totals=Counter();reasons=Counter();states=Counter()
    for row in rows:
        pair=row.get('instrument');need(pair in pairs and pair not in seen,'summary_pair_identity');seen.add(pair)
        need(number(row.get('pip_size'))==number(pairs[pair]['pip_size']),'pair_pip_identity')
        families=row.get('families');need(type(families) is dict and set(families)==spec[7],'family_coverage')
        active=[];slots={}
        for family,slot in families.items():
            contract=pairs[pair]['families'][family]['contract'];contract_sha=pairs[pair]['families'][family]['contract_sha256']
            need(contract_sha==digest(contract) and slot.get('contract_sha256')==contract_sha
                 and slot.get('cohort_id')==contract['cohorts'][family],'slot_contract_binding')
            observed=stamp(slot['observed_epoch']);need(observed<=generated and now-observed<=90,'stale_family_observation')
            need(0<stamp(slot['activated_epoch'])<=observed,'slot_activation_clock')
            state=slot.get('status');need(state in {'forecast','warming','building','ready','unavailable'},'slot_status')
            reason=slot.get('reason');need(type(reason) is str and len(reason)<=4096,'slot_reason')
            counts=slot.get('counts');need(type(counts) is dict and len(counts)<=64,'slot_counts')
            need(all(type(v) is int and 0<=v<=1e12 for v in counts.values()),'invalid_ledger_count')
            totals.update(counts);states[state]+=1;reasons[reason[:240]]+=1
            publication=slot.get('latest_forecast')
            expired=False
            if publication is not None:
                arm=project_forecast(publication,pair=pair,family=family,contract=contract,pip=row['pip_size'],
                                     now=now,generated=generated,native=kind=='joint')
                if arm is not None:active.append(arm)
                else:expired=True
            slots[family]={'status':state,'reason':reason[:512],
                'reason_label':'Original H1 target elapsed; awaiting next forecast' if expired else reason[:240],
                'observed_epoch':observed,'current_readiness':slot.get('current_readiness'),
                'last_attempt':slot.get('last_attempt'),'latest_published_forecast':publication}
        projected.append({'instrument':pair,'pip_size':row['pip_size'],'families':slots,
            'observed_epoch':generated,'active_forecasts':active,'status':'forecast' if active else
                'building' if any(v['status']=='building' for v in slots.values()) else 'unavailable'})
    return {'study_version':spec[6],'status':'current','generated_epoch':generated,'observed_epoch':now,
        'registry_sha256':digest(registry),'research_only':True,**{k:False for k in FLAGS},'rows':projected,
        'ledger_counts':dict(totals),'state_counts':dict(states),'reason_counts':dict(reasons),
        'forecast_pair_count':sum(bool(row['active_forecasts']) for row in projected),
        'worker_observation':{'age_sec':now-reported,**{k:heartbeat.get(k) for k in
            ('generated_epoch','errors','last_error','heartbeat_publication_errors','phase')}},
        'evidence_scope':'Verified selection/source/activation/producer-summary binding; no independent ledger rescore or accuracy claim.',
        'primary_selection':{'selected':spec[6].removeprefix('joint_') if kind=='joint' else 'v3','activated_epoch':selection['activated_epoch'],
            'registry_sha256':digest(registry)},'source_path':source['study_path']}


def unavailable(kind, reason):
    return {'study_version':KINDS[kind][6],'status':'unavailable','reason':reason,'reason_label':reason,
        'rows':[],'ledger_counts':{},'research_only':True,**{key:False for key in FLAGS}}


def project_collection_status(legacy, joint, *, now_epoch=None):
    """Selected study observation plus independently retained quote/account ages."""
    now=stamp(time.time() if now_epoch is None else now_epoch)
    worker=joint.get('worker_observation') or {}
    generated=worker.get('generated_epoch')
    recent=type(generated) in (int,float) and math.isfinite(generated) and 0<=now-generated<=90
    current=joint.get('study_version') in {'joint_v7','joint_v9','joint_v10'} and joint.get('status')=='current' and recent
    reasons=dict(joint.get('reason_counts') or {}) if current else {}
    study={'status':'current' if current else 'unavailable','current':bool(current),
        'observed_at':generated,'generated_epoch':generated,
        'age_sec':now-generated if type(generated) in (int,float) and math.isfinite(generated) else None,
        'max_age_sec':90,'phase':worker.get('phase'),
        'reason':joint.get('reason') if not current else None,'reason_counts':reasons,
        'last_error':worker.get('last_error'),
        'reported_cumulative_errors':worker.get('errors'),
        'reported_cumulative_heartbeat_publication_errors':worker.get('heartbeat_publication_errors'),
        'scope':'selected_'+str(joint.get('study_version','joint_unavailable'))+'_producer_observation_not_forecast_success'}
    observations={'study':study,**{key:dict((legacy.get('observations') or {}).get(key)
        or {'status':'unavailable','current':False}) for key in ('quote_stream','account')}}
    count=joint.get('forecast_pair_count',0) if current else None
    running=all(row.get('current') is True for row in observations.values())
    return {'schema_version':'operational_research_collection_display_v1_20260913',
        'selected_study':'native_'+str(joint.get('study_version','joint_unavailable')),'status':'running' if running else 'unavailable','running':running,
        'label':'Native joint producer summary current' if current else 'Native joint summary unavailable',
        'registered_study_trading_enabled':False,'research_only':True,'can_place_orders':False,'can_promote':False,
        'running_scope':'recent_selected_study_quote_and_account_observations_not_forecast_success',
        'forecasting':{'blocked':not count,'label':f'{count} pairs publishing native H1 forecasts' if current
            else 'Forecast readiness unavailable','observation_current':bool(current),'reason_counts':reasons},
        'model_attempts':{'status':'available' if current else 'unavailable','counts':joint.get('ledger_counts',{}) if current else {},
            'scope':'selected native V7 producer summary counts; not independent ledger rescore'},
        'observations':observations,'unavailable_observations':[key+':'+row['status']
            for key,row in observations.items() if not row.get('current')]}


def read_dashboard_sources(root, *, now_epoch=None):
    now=stamp(time.time() if now_epoch is None else now_epoch)
    try:selection=read_selection(root,now)
    except (OSError,ValueError,TypeError,KeyError) as exc:
        reason=str(exc) if isinstance(exc,SelectionError) else 'selected_source_unavailable'
        return {'selected':True,'status':'unavailable','reason':reason,'price':unavailable('price',reason),
            'joint':unavailable('joint',reason),'features':None}
    if selection is None:return None
    result={'selected':True,'status':'current','selection_sha256':digest(selection),'features':selection['features']}
    for kind in KINDS:
        try:result[kind]=read_study(root,selection,kind,now,
                                  observed_clock=time.time if now_epoch is None else None)
        except (OSError,ValueError,TypeError,KeyError,OverflowError) as exc:
            result[kind]=unavailable(kind,str(exc) if isinstance(exc,SelectionError) else 'selected_source_unavailable')
            result['status']='partial_unavailable'
    return result
