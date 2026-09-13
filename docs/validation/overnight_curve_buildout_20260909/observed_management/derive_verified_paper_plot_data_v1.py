"""Actual original observation points from three verified paper episodes; no fit."""
from decimal import Context,Decimal,localcontext
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

BASE=Path(__file__).resolve().parent
TRAD=BASE.parent.parent/'trad'
RUNTIME=TRAD/'data/oanda_training_manager/observed_curve_management_v1_20260909'
REPORT=BASE/'OBSERVED_THREE_EPISODE_FINAL_VERIFICATION_20260909.json'
REPORT_SHA='cdaac1862e6d2d2153e65d394ff5bffada25dd505b43d186197144cf1286e5ce'
REGISTRY=TRAD/'config/observed_curve_management_v1_20260909.json'
REGISTRY_SHA='60689101fb63a8bcce2465d19f627b9b2ce357d1ee271c65298f255663d991e1'
ARMS=('usd_curve_manager','curve_hold_no_rotation')
MAX_FILE_BYTES=2*1024*1024
MAX_TOTAL_BYTES=32*1024*1024


def need(ok,reason):
    if not ok:raise ValueError(reason)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def safe(path):
    for p in reversed([path,*path.parents]):
        s=p.lstat();need(not stat.S_ISLNK(s.st_mode) and not getattr(s,'st_file_attributes',0)&1024,'plot_reparse_path')


def read(path):
    safe(path);started=time.time()
    with path.open('rb') as f:
        before=os.fstat(f.fileno());raw=f.read(MAX_FILE_BYTES+1);after=os.fstat(f.fileno())
    current=path.stat();completed=time.time()
    ident=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns)
    need(started<=completed and len(raw)<=MAX_FILE_BYTES and ident(before)==ident(after)==ident(current),'plot_read_changed_or_bound')
    return raw,{'path':str(path),'sha256':sha(raw),'bytes':len(raw),'derived_read_started_epoch':started,'derived_read_completed_epoch':completed}


class Reader:
    def __init__(self,report):
        self.refs={r['relative_path']:r for r in report['verified_files']};self.evidence={};self.total=0
        need(len(self.refs)==len(report['verified_files'])<=4000,'plot_verified_reference_inventory')

    def get(self,relative):
        need(type(relative) is str and re.fullmatch(r'episodes/episode_0[123]/(?:[a-zA-Z0-9_]+/)*[a-zA-Z0-9_]+\.json',relative),'plot_relative_path')
        need(relative in self.refs,'plot_unverified_source')
        raw,meta=read(RUNTIME/relative);expected=self.refs[relative]
        need(meta['sha256']==expected['sha256'] and meta['bytes']==expected['bytes'],'plot_original_source_changed')
        self.total+=len(raw);need(self.total<=MAX_TOTAL_BYTES,'plot_total_byte_bound')
        self.evidence[relative]={**meta,'relative_path':relative,'original_verifier_read_epoch':expected['verified_read_epoch']}
        return json.loads(raw)

    def ref(self,relative):
        need(relative in self.evidence,'plot_source_not_read')
        return {'relative_path':relative,'sha256':self.evidence[relative]['sha256'],'bytes':self.evidence[relative]['bytes']}


def point(mapping):
    if mapping is None:return None
    q=mapping['quotes'].get('GBP_USD')
    if q is None:return None
    with localcontext(Context(prec=96)):
        bid,ask=Decimal(q['bid']),Decimal(q['ask']);need(0<bid<=ask,'plot_quote_order')
        mid=(bid+ask)/2
    need(q['tradeable'] is True and 0<q['market_epoch']<=q['available_epoch'],'plot_quote_clock_or_status')
    return {**{k:q[k] for k in ('quote_id','market_epoch','available_epoch','bid','ask','tradeable','raw_snapshot_sha256','capture_receipt_sha256')},
        'midpoint':str(mid),'source_read_started_epoch':mapping['source_header']['read_started_epoch'],
        'source_read_completed_epoch':mapping['source_header']['read_completed_epoch'],
        'mapping_sha256':mapping['mapping_sha256'],'observation_is_broker_fill':False}


def inventory(value):
    pos=value['position']
    if pos is None:return {'side':0,'base_units':0,'position_open':False,'realized_usd':value['realized_usd']}
    need(type(pos['side']) is int and pos['side'] in (-1,1) and type(pos['base_units']) is int and pos['base_units']>0,'plot_position_shape')
    return {'side':pos['side'],'base_units':pos['base_units'],'position_open':True,'realized_usd':value['realized_usd']}


def receipt_bind(reader,relative,quote):
    receipt=reader.get(relative)
    need(receipt['receipt_sha256']==quote['capture_receipt_sha256'],'plot_quote_receipt_semantic_hash')
    need(receipt['read_completed_epoch']==quote['available_epoch'] and receipt['raw_sha256']==quote['raw_snapshot_sha256'],'plot_quote_original_clock_hash')
    raw_relative=relative.replace('_receipt.json','_raw.json');reader.get(raw_relative)
    need(reader.ref(raw_relative)['sha256']==receipt['raw_sha256'],'plot_raw_quote_sha')
    return {'receipt':reader.ref(relative),'raw':reader.ref(raw_relative),'receipt_semantic_sha256':receipt['receipt_sha256']}


def selected_sources(reader,prefix,quote):
    if quote is None:return None
    candidates=[name for name in reader.refs if name.startswith(prefix+'/quotes/') and name.endswith('_receipt.json')]
    need(len(candidates)<=160,'plot_observation_inventory_bound')
    matched=[]
    for name in sorted(candidates):
        value=reader.get(name)
        if value['receipt_sha256']==quote['capture_receipt_sha256']:matched.append(name)
    need(len(matched)==1,'plot_selected_quote_receipt_identity')
    result=receipt_bind(reader,matched[0],quote)
    observation=matched[0].replace('_receipt.json','_observation.json');value=reader.get(observation)
    need(value['raw']['sha256']==result['raw']['sha256'] and value['receipt']['sha256']==result['receipt']['sha256'],'plot_observation_file_binding')
    result['observation']=reader.ref(observation);return result


def episode(reader,verified):
    name=verified['episode_id'];prefix='episodes/'+name
    config=reader.get(prefix+'/episode_config.json');anchor=reader.get(prefix+'/initial_curve_consumption.json')
    prepared=anchor['curve']['prepared_curve'];binding=config['curve_binding']
    nodes=[n for n in prepared['nodes'] if n['node_id']==binding['node_id']];need(len(nodes)==1,'plot_original_node')
    node=nodes[0]
    need(prepared['instrument']=='GBP_USD' and prepared['reference_epoch']==binding['reference_epoch']
        and node['original_target_epoch']==config['native_target_epoch']==verified['native_target_epoch']
        and node['expected_terminal_price']==binding['expected_terminal_price'],'plot_original_curve_identity')
    rows=[]
    for step in verified['steps']:
        here=prefix+'/steps/'+step['step_id']
        plan=reader.get(here+'/plan.json');settlement=reader.get(here+'/settlement.json');publication=reader.get(here+'/plan_publication.json')
        state=reader.get(here+'/state_after.json')
        need(plan['plan_sha256']==settlement['plan_sha256']==publication['plan_sha256'],'plot_plan_identity')
        need(publication['publication_sha256']==settlement['publication_sha256'],'plot_publication_identity')
        need(state['state_sha256']==step['state_sha256']==settlement['states']['state_sha256'],'plot_state_identity')
        need(plan['decision_epoch']==step['decision_epoch'] and plan['plan_created_epoch']==step['plan_created_epoch']
            and publication['publication_completed_epoch']==step['publication_completed_epoch']
            and settlement['settlement_computed_epoch']==step['settlement_computed_epoch']
            and settlement['selected_observation_epoch']==step['selected_observation_epoch'],'plot_original_clocks')
        decision=point(plan['decision_quote_mapping']);selected=point(settlement['selected_quote_mapping'])
        decision_source=None if decision is None else receipt_bind(reader,here+'/decision_quote_receipt.json',decision)
        execution_source=selected_sources(reader,here,selected)
        candidates=plan['curve_candidate_originals'];need(len(candidates)<=1,'plot_candidate_count')
        candidate=candidates[0] if candidates else None
        if candidate:
            need(candidate['curve_sha256']==binding['curve_sha256'] and candidate['original_target_epoch']==node['original_target_epoch']
                and candidate['expected_terminal_price']==node['expected_terminal_price'],'plot_static_terminal_changed')
        arm_rows={}
        for arm in ARMS:
            original=settlement['arms'][arm]
            need(original['decision']['action']==step['decisions'][arm] and original['state_after']['realized_usd']==step['independent_arithmetic'][arm]['realized_usd'],'plot_original_arm_state')
            arm_rows[arm]={'action':original['decision']['action'],'reason':original['decision'].get('reason'),
                'virtual_action_status':original['virtual_action']['status'],'before':inventory(original['state_before']),
                'after':inventory(original['state_after']),'position_state_known_epoch':state['known_epoch']}
        rows.append({'step_id':step['step_id'],'scheduled_epoch':step['scheduled_epoch'],'terminal':step['terminal'],
            'information_cutoff_epoch':plan['information_cutoff_epoch'],'plan_created_epoch':plan['plan_created_epoch'],
            'plan_publication_completed_epoch':publication['publication_completed_epoch'],'selected_observation_epoch':settlement['selected_observation_epoch'],
            'settlement_computed_epoch':settlement['settlement_computed_epoch'],'cadence_lateness_sec':step['cadence_lateness_sec'],
            'decision_quote':decision,'selected_quote':selected,'decision_quote_sources':decision_source,'selected_quote_sources':execution_source,
            'curve_candidate':None if candidate is None else {k:candidate[k] for k in ('candidate_sha256','side','expected_remaining_price_change','expected_remaining_move_pips','remaining_sec')},
            'curve_refusals':step['candidate_refusals']['curve'],'source_refusals':step['source_refusals'],'arms':arm_rows,
            'source_files':{n:reader.ref(here+'/'+n+'.json') for n in ('plan','plan_publication','settlement','state_after')}})
    return {'episode_id':name,'instrument':'GBP_USD','price_convention':binding['price_convention'],
        'reference_epoch':prepared['reference_epoch'],'reference_label_epoch':prepared['reference_label_epoch'],
        'reference_price':prepared['reference_price'],'issued_epoch':anchor['curve']['issued_epoch'],
        'original_publication_completed_epoch':anchor['publication']['publication_completed_epoch'],
        'original_consumption_observed_epoch':anchor['consumption']['observed_epoch'],
        'original_target_epoch':node['original_target_epoch'],'target_label_epoch':node['target_label_epoch'],
        'target_price_window_end_epoch':node['target_price_window_end_epoch'],'target_selection_policy':prepared['target_selection_policy'],
        'fixed_expected_terminal_price':node['expected_terminal_price'],'original_predicted_signed_pips':node['predicted_signed_pips'],
        'original_probability_up':node['probability_up'],'original_probability_scope':node['probability_scope'],
        'curve_sha256':binding['curve_sha256'],'node_sha256':node['node_sha256'],'model_sha256':prepared['model_sha256'],
        'config_source':reader.ref(prefix+'/episode_config.json'),'initial_curve_source':reader.ref(prefix+'/initial_curve_consumption.json'),
        'rows':rows,'missing_slots':verified['uncompleted_or_unobserved_slots'],'recorded_missed_slots':verified['missed_slots']}


def source_check():
    raw,_=read(REGISTRY);need(sha(raw)==REGISTRY_SHA,'plot_registry_changed');value=json.loads(raw)
    for name,want in value['source_bindings'].items():need(sha(read(TRAD/name)[0])==want,'plot_registered_source_changed')
    return value['source_bindings']


def run(output):
    need(output.parent==BASE and not output.exists(),'plot_fresh_external_output')
    own=Path(__file__).read_bytes();started=time.time();source=source_check();raw,report_meta=read(REPORT)
    need(sha(raw)==REPORT_SHA,'plot_original_verifier_changed');report=json.loads(raw)
    need(report['status']=='passed' and report['issues']==[] and report['source_bindings']==source,'plot_verified_scope')
    need([e['episode_id'] for e in report['episodes']]==['episode_01','episode_02','episode_03'] and all(e['completed_matched_endpoint_eligible'] for e in report['episodes']),'plot_all_original_episodes')
    reader=Reader(report);episodes=[episode(reader,e) for e in report['episodes']]
    need(source_check()==source and Path(__file__).read_bytes()==own and read(REPORT)[0]==raw,'plot_source_changed_during_read')
    value={'schema_version':'verified_three_episode_plot_dataset_v1_20260909','status':'derived_from_original_verified_observations',
        'derivation_started_epoch':started,'derivation_completed_epoch':time.time(),'original_verifier':report_meta,
        'original_verification_started_epoch':report['verification_started_epoch'],'original_verification_completed_epoch':report['verification_completed_epoch'],
        'helper_sha256':sha(own),'registry_sha256':REGISTRY_SHA,'source_bindings':source,'episodes':episodes,
        'source_file_evidence':list(reader.evidence.values()),'total_new_read_bytes':reader.total,
        'scope':['All three predeclared GBP_USD episodes; no selection by outcome. All178 original steps remain.','Quote midpoint is exact Decimal(bid+ask)/2, not an interpolated execution path.',
            'Decision-input observation and later selected virtual-execution observation have distinct original clocks. Repeated older market ticks remain repeated observations, not fabricated new ticks.',
            'The fixed terminal level is an original endpoint expectation, not a path forecast. Plot as a horizontal annotation without inventing intermediary expected prices.',
            'Positions/cash are recorded isolated virtual-arm states; state becomes known only at original settlement completion. No actual broker fills/account PnL or new counterfactual policy simulation.',
            'Missing quotes/candidates remain null with original refusals. Original probability stays in header with its uncalibrated original-event scope, never as a remaining-move probability.'],
        'interpolation':False,'new_forecasts':False,'counterfactual_guard_pnl':False,'GET':False,'runtime_writes':False}
    data=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    with output.open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
    print(json.dumps({'path':str(output),'sha256':sha(data),'episodes':len(episodes),'steps':sum(len(e['rows']) for e in episodes),'source_files':len(reader.evidence),'bytes':len(data)}))
    return value


if __name__=='__main__':run(BASE/'VERIFIED_THREE_EPISODE_PLOT_DATA_20260909.json')
