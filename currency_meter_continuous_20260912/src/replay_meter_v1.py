"""Offline, source-bound recreation of one retained continuous-meter capture."""
import argparse
import base64
from datetime import datetime
import hashlib
import json
from pathlib import Path

import meter_capture_v1 as capture
import meter_store_v1 as store


def verify_capture(database,capture_id,*,project,expected_release):
    saved=store.read_capture(database,capture_id);raw=saved['raw'];feature=saved['feature'];receipt=saved['receipt']
    expected_bindings=dict(capture.recipe.PINS,continuous_capture_release_sha256=expected_release)
    if raw['source_bindings']!=expected_bindings or feature['source_bindings']!=expected_bindings:raise ValueError('replay_source_binding_changed')
    upstream=raw['upstream_heartbeat']
    beat_bytes=base64.b64decode(upstream['raw_base64'],validate=True)
    latest_bytes=base64.b64decode(upstream['latest_cycle_base64'],validate=True)
    if capture.sha(beat_bytes)!=upstream['raw_sha256'] or capture.sha(latest_bytes)!=upstream['latest_cycle_sha256']:
        raise ValueError('replay_upstream_hash_changed')
    beat=json.loads(beat_bytes);latest=json.loads(latest_bytes)
    if beat['collector_contract_id']!=capture.recipe.COLLECTOR or beat['classification_version']!=capture.recipe.CLASSIFIER:
        raise ValueError('replay_upstream_contract_changed')
    if beat['status'] not in {'running_cycle','cycle_complete'} or latest['status']!='ok':raise ValueError('replay_upstream_not_successful')
    if beat['policy']['research_only'] is not True or beat['policy']['execution_eligible'] is not False:raise ValueError('replay_upstream_flags_changed')
    started=capture.epoch(raw['capture_started_utc'])
    age=started-capture.epoch(beat['heartbeat_utc']);progress=started-capture.epoch(beat['last_progress_utc'])
    if not -2<=age<=120 or not -2<=progress<=900 or not -2<=started-capture.epoch(latest['generated_utc'])<=1800:
        raise ValueError('replay_upstream_stale_or_future')
    if age!=upstream['heartbeat_age_seconds'] or progress!=upstream['progress_age_seconds']:
        raise ValueError('replay_upstream_age_changed')
    clocks=[raw['capture_started_utc'],upstream['read_started_utc'],upstream['read_completed_utc'],
            raw['read_receipt']['read_started_utc'],raw['read_receipt']['read_completed_utc'],
            feature['computation_started_utc'],feature['computation_completed_utc'],receipt['observed_ready_utc']]
    epochs=list(map(capture.epoch,clocks))
    if any(a>b for a,b in zip(epochs,epochs[1:])):raise ValueError('replay_clock_order')
    module,_,_=capture.recipe.load_pinned_producer(Path(project));mapping=capture.source_lineages(Path(project))
    admitted,admission=capture.admit_current_rows(raw['original_rows'],asof=datetime.fromisoformat(raw['read_receipt']['read_started_utc']),mapping=mapping)
    states,selection=capture.recipe.build_current(module,admitted,computed_asof=datetime.fromisoformat(feature['computation_started_utc']))
    recreated={'schema_version':capture.CONTRACT,'cohort_id':capture.COHORT,'capture_id':capture_id,
               'computation_started_utc':feature['computation_started_utc'],'computation_completed_utc':feature['computation_completed_utc'],
               'state_availability':'requires_actual_postcommit_publication_receipt','history_outputs_published':0,
               'currency_count':21,'current_states':states,'source_admission':admission,'formula_selection':selection,
               'source_bindings':expected_bindings,'collector_contract_id':capture.recipe.COLLECTOR,'classification_version':capture.recipe.CLASSIFIER,
               'upstream_observation':{k:v for k,v in upstream.items() if k not in {'raw_base64','latest_cycle_base64'}},
               'observation_state':capture.observation_state(raw['original_rows'],admitted,states),
               'clock_claim':'original article versions retained; current state available only at new receipt',**capture.FLAGS}
    exact=store.canonical_bytes(recreated)==store.canonical_bytes(feature)
    if not exact:raise ValueError('feature_does_not_recreate')
    return {'verified':True,'capture_id':capture_id,'raw_sha256':receipt['raw_sha256'],'feature_sha256':receipt['feature_sha256'],
            'exact_feature_bytes_recreated':True,'source_rows':len(raw['original_rows']),'admitted_rows':len(admitted),
            'excluded_rows':admission['excluded_source_rows'],'new_captures_created':0,'upstream_network_requests':0,
            'source_clocks_preserved':True,'source_id_bound_lineages':True,'can_place_orders':False,'forecast':False}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--database',type=Path,required=True)
    parser.add_argument('--capture-id',required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():raise FileExistsError('replay_output_exists')
    result=verify_capture(args.database,args.capture_id,project=capture.ROOT/'pinned_project',expected_release=capture.verify_release())
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('x',encoding='utf-8') as out:json.dump(result,out,indent=2,sort_keys=True)
    print(json.dumps(result))
