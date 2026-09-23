"""Read-only audit/scoring of retained pilot evidence; writes only a new report directory."""
from __future__ import annotations
import argparse
from collections import Counter,defaultdict
from copy import deepcopy
from decimal import Context,Decimal,localcontext
import hashlib,json,os,re,sys,time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]/'trad'
sys.dont_write_bytecode=True
sys.path.insert(0,str(ROOT))
import oanda_recovered_curve_pilot_v1 as pilot
import oanda_forecast_curve_contract_v1 as contract
import oanda_forecast_curve_file_store_v1 as files
import oanda_recovered_second_curve_v1 as recovered
import oanda_s5_mba_research_capture_v1 as candles
import oanda_native_curve_outcomes_v1 as outcomes
import oanda_research_quote_receipt_v1 as quotes
from oanda_recovered_curve_bridge_v1 import prepare_recovered_computation
from oanda_curve_management_adapter_v1 import candidate_for_target

SCHEMA='recovered_curve_pilot_offline_evaluation_v1_20260909'
MAX_CYCLES=600
MAX_CURVES=3600
MAX_READ_BYTES=1024**3
VARIANT_FILES=('attempt_started.json','model_input.json','computed_result.json','issue_stage.json',
    'publication_stage.json','issued_observation.json','decision_observation.json','attempt_completed.json')

def require(ok,reason):
    if not ok:raise contract.CurveContractError(reason)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def reason(exc):
    value=str(exc)
    return value if re.fullmatch(r'[a-z][a-z0-9_]{1,120}',value) else 'invalid_or_unavailable_evidence'

class Reader:
    """Each immutable file is independently observed; this is not a global filesystem transaction."""
    def __init__(self,root):
        self.root=Path(root).absolute();files._safe_components(self.root)
        self.manifest={};self.total=0
    def read(self,path):
        path=Path(path).absolute()
        require(self.root in path.parents,'evidence_path_escape')
        raw=files._read(path);info=path.stat();identity=files._identity(info)
        relative=path.relative_to(self.root).as_posix()
        prior=self.manifest.get(relative)
        if prior:require(prior['sha256']==sha(raw),'immutable_evidence_changed')
        else:
            self.total+=len(raw);require(self.total<=MAX_READ_BYTES,'evaluator_total_read_budget')
            self.manifest[relative]={'relative_path':relative,'sha256':sha(raw),'bytes':len(raw),
                'observed_epoch':time.time(),'identity':list(identity)}
        return raw
    def json(self,path):return files._decode(self.read(path))
    def optional(self,path):return self.json(path) if Path(path).exists() else None
    def reference(self,ref):
        require(isinstance(ref,dict) and set(ref)=={'relative_path','sha256','bytes'},'file_reference_schema')
        value=ref['relative_path']
        require(isinstance(value,str) and '\\' not in value and not Path(value).is_absolute()
            and all(part not in ('','.','..') for part in value.split('/')),'file_reference_path')
        raw=self.read(self.root/value)
        require(len(raw)==ref['bytes'] and sha(raw)==ref['sha256'],'file_reference_hash')
        return raw
    def unchanged(self):
        for item in self.manifest.values():
            path=self.root/item['relative_path'];files._safe_components(path,require_file=True)
            require(list(files._identity(path.stat()))==item['identity'],'immutable_evidence_changed_after_read')

def _authority(value):
    require(isinstance(value,dict) and all(value.get(k) is v for k,v in contract.AUTHORITY.items()),'record_authority')

def _source_capture(reader,directory,instrument,registry,clock):
    receipt=reader.json(directory/'capture_receipt.json')
    if receipt.get('status')!='captured_not_issued':
        return {'status':'failed_source_capture','reason_code':receipt.get('error_reason','source_capture_failed'),
                'instrument':instrument,'directory':directory.relative_to(reader.root).as_posix()},None,{}
    raw=reader.read(directory/'source_raw.json')
    mappings={}
    for convention in pilot.CONVENTIONS:
        mapped=candles.map_verified_capture(raw,receipt,registry['metadata'][instrument],convention)
        require(mapped['instrument']==instrument,'source_path_instrument_mismatch')
        require(max(mapped['first_observed_epoch'],receipt['capture_completed_epoch'])<=clock(),'future_source_observation')
        retained=reader.optional(directory/convention/'source_mapping.json')
        if retained is not None:require(retained==mapped,'retained_source_mapping_mismatch')
        mappings[convention]=mapped
    mapped=mappings[pilot.CONVENTIONS[0]];rows=mapped['complete_rows'];capture=None
    if rows:
        # This derived seal is created NOW. Original source arrival stays in
        # row/read clocks and drives the frozen wrapper's source selection.
        normalized=[{'bar_start_epoch':r['bar_label_epoch'],'available_epoch':r['available_epoch'],
            'complete':True,'mid_close':r['mid']['c'],'bid_close':r['bid']['c'],'ask_close':r['ask']['c']} for r in rows]
        att=mapped['source_attestation']
        capture=outcomes.capture_outcome_candles(normalized,instrument=instrument,
            source_sha256=mapped['source_sha256'],coverage_start_label_epoch=att['coverage_start_label_epoch'],
            coverage_end_label_epoch=att['coverage_end_label_epoch'],complete_range=True,
            read_started_epoch=att['read_started_epoch'],read_completed_epoch=att['read_completed_epoch'],
            source_attestation=att,clock=clock)
    return {'status':'verified','instrument':instrument,'directory':directory.relative_to(reader.root).as_posix(),
        'raw_source_sha256':mapped['source_sha256'],'receipt_sha256':receipt['receipt_sha256'],
        'original_read_completed_epoch':receipt['read_completed_epoch'],'coverage':mapped['coverage'],
        'derived_capture_sha256':capture['capture_sha256'] if capture else None,
        'derived_capture_first_observed_epoch':capture['first_observed_epoch'] if capture else None},capture,mappings

def _variant(reader,directory,instrument,convention,mapping,registry,model):
    records={name:reader.optional(directory/name) for name in VARIANT_FILES}
    for name,value in records.items():
        if value is None:continue
        if name in ('model_input.json','computed_result.json'):
            require(all(value.get(k) is v for k,v in recovered.INERT.items()),'recovered_record_authority')
        else:_authority(value)
        if name not in ('model_input.json','computed_result.json'):
            require(value.get('instrument',instrument)==instrument and value.get('price_convention',convention)==convention,
                'variant_record_identity')
        for key in ('model_input','computed_result','decision_observation'):
            if key in value:reader.reference(value[key])
    capture,result=records['model_input.json'],records['computed_result.json']
    proof=None
    if capture is not None and result is not None:
        require(mapping is not None,'model_input_without_verified_source')
        require(capture['rows']==mapping['last13_feature_rows'] and capture['source_sha256']==mapping['source_sha256']
            and capture['price_convention']==convention and capture['sampling_policy']==registry['sampling_policy']
            and capture['scope']=='current_research' and capture['instrument']==instrument
            and Decimal(str(capture['pip_size']))==Decimal(str(registry['metadata'][instrument]['pip_size'])),
            'model_input_raw_mapping_mismatch')
        start=records['attempt_started.json']
        if start is not None:
            require(mapping['first_observed_epoch']<=contract.epoch(start['started_epoch'])<=capture['first_observed_epoch'],
                'attempt_source_input_clock_order')
        prepared=prepare_recovered_computation(capture,result,model,source_bindings=registry['source_bindings'],
            forecast_cohort=registry['study_id']+'/'+instrument+'/'+convention,policy=registry['curve_policy'])
        proof={'prepared':prepared,'source_raw_sha256':mapping['source_sha256'],'directory':directory,
            'records':records,'instrument':instrument,'convention':convention}
    completed=records['attempt_completed.json']
    summary={'directory':directory.relative_to(reader.root).as_posix(),'instrument':instrument,'price_convention':convention,
        'retained_stages':[name for name,value in records.items() if value is not None],
        'attempt_status':completed.get('status') if completed else 'incomplete_or_not_attempted',
        'reason_code':completed.get('reason_code') if completed else None,
        'numerical_raw_mapping_verified':proof is not None,
        'logical_issue_claimed':any(v and v.get('logical_issue_created') is True for v in records.values()),
        'publication_claimed':any(v and v.get('publication_completed') is True for v in records.values()),
        'consumption_claimed':any(v and v.get('consumption_completed') is True for v in records.values())}
    return summary,proof

def _stage_chain(proof,curve,publication,consumptions,now):
    for name,value in proof['records'].items():
        if value is None or name in ('model_input.json','computed_result.json','decision_observation.json'):continue
        if 'curve_sha256' in value:
            require(value['curve_sha256']==curve['curve_sha256'] and value.get('curve_id')==curve['curve_id']
                and value.get('issued_epoch')==curve['issued_epoch']
                and value.get('reference_epoch')==curve['prepared_curve']['reference_epoch'],'stage_curve_identity_mismatch')
        if 'publication' in value:
            claimed=value['publication']
            require(publication is not None and claimed['publication']==publication
                and claimed['descriptor']==files._descriptor(curve,publication),'stage_publication_mismatch')
            observed=contract.epoch(claimed['receipt_persisted_observed_epoch'])
            require(publication['publication_completed_epoch']<=observed<=now,'stage_publication_persistence_clock')
        if 'consumption' in value:
            require(any(value['consumption']==item for item in consumptions),'stage_consumption_not_retained')

def _entry(reader,proof,curve,publication,known_consumptions,registry,now):
    record=proof['records']['decision_observation.json']
    if record is None:return None,{'status':'missing','reason_code':'no_retained_decision_observation'}
    try:
        _authority(record)
        require(contract.epoch(record['decision_epoch'])<=now,'future_decision_observation')
        require(record.get('curve_id')==curve['curve_id'] and record.get('curve_sha256')==curve['curve_sha256'],
            'decision_curve_identity')
        require(record.get('position_actions_performed') is False and record.get('broker_fills_performed') is False,
            'decision_execution_claim')
        consumption=record['consumption']
        require(any(consumption==value for value in known_consumptions),'decision_consumption_not_retained')
        raw=reader.reference(record['quote_source']);qreceipt=files._decode(reader.reference(record['quote_capture_receipt']))
        mapped=quotes.map_quote_snapshot(raw,qreceipt,decision_epoch=record['decision_epoch'],
            instruments=pilot.INSTRUMENTS,maximum_quote_age_sec=30)
        require(mapped==record['quote_mapping'] and mapped['metadata']==registry['metadata'],'decision_quote_mapping_mismatch')
        pair=curve['prepared_curve']['instrument'];quote=mapped['quotes'].get(pair)
        expected=[candidate_for_target(curve,publication,consumption,decision_epoch=record['decision_epoch'],
            target_epoch=node['original_target_epoch'],quote=quote,metadata=registry['metadata'][pair],
            expected_source_bindings=registry['source_bindings'],maximum_quote_age_sec=30,
            target_window_policy=registry['target_window_policy']) for node in curve['prepared_curve']['nodes']]
        require(expected==record['candidates'],'decision_candidate_replay_mismatch')
        entry=record.get('entry_observation')
        if entry is not None:
            require(quote is not None,'entry_without_observed_quote')
            rebuilt=outcomes.capture_entry_quote(quote,source_sha256=sha(raw),clock=lambda:entry['first_observed_epoch'])
            require(rebuilt==entry,'entry_raw_quote_mapping_mismatch')
        return entry,{'status':'verified','entry_present':entry is not None,
            'decision_epoch':record['decision_epoch'],'candidate_count':len(expected),
            'available_candidate_count':sum(c['status']=='available' for c in expected),
            'position_actions_performed':False,'broker_fills_performed':False}
    except (ValueError,KeyError,TypeError,OSError) as exc:
        return None,{'status':'withheld','reason_code':reason(exc)}

def _dispositions(report,curve,now):
    if report['status']!='evaluated':return [{'disposition':'withheld','reason_code':report.get('reason_code')}]
    result=[]
    for node in report['nodes']:
        for name,view in node['views'].items():
            why=view.get('reason_code')
            if view['status']=='scored':status='scored'
            elif why=='node_not_issued_as_forecast':status='withheld'
            elif now<node['original_target_epoch']:status='pending'
            elif why=='provider_reported_no_complete_target_bar':status='missing_provider_bar'
            else:status='missing_source_coverage'
            result.append({'node_id':node['node_id'],'horizon_sec':node['horizon_sec'],'view':name,
                'disposition':status,'reason_code':why,'source_domain_reasons':view.get('source_domain_reasons')})
    return result

def _matched(curves):
    groups=defaultdict(lambda:defaultdict(list))
    for item in curves:
        if item.get('status')!='scored_chain':continue
        prepared=item['curve']['prepared_curve']
        for node in item['report'].get('nodes',[]):
            key=(prepared['instrument'],prepared['reference_label_epoch'],node['horizon_sec'],
                prepared['model_sha256'],item['input_source_sha256'])
            groups[key][prepared['input_context']['price_convention']].append((item,node))
    matches=[]
    with localcontext(Context(prec=80)):
        for key,variants in sorted(groups.items()):
            base={'instrument':key[0],'reference_label_epoch':key[1],'horizon_sec':key[2],
                'model_sha256':key[3],'input_source_sha256':key[4]}
            if any(len(variants.get(c,[]))!=1 for c in pilot.CONVENTIONS):
                matches.append({**base,'status':'unmatched_or_ambiguous','variant_counts':{k:len(v) for k,v in variants.items()}});continue
            official,ba=(variants[c][0] for c in pilot.CONVENTIONS)
            for name in ('nominal_exact','retained_training_target'):
                left,right=official[1]['views'][name],ba[1]['views'][name]
                row={**base,'view':name,'official_curve_id':official[0]['curve']['curve_id'],
                    'ba_curve_id':ba[0]['curve']['curve_id'],'official_status':left['status'],'ba_status':right['status']}
                if left['status']!='scored' or right['status']!='scored':
                    matches.append({**row,'status':'matched_forecasts_outcome_unavailable',
                        'official_reason':left.get('reason_code'),'ba_reason':right.get('reason_code')});continue
                same=(left['actual_selected_price_epoch']==right['actual_selected_price_epoch']
                    and left['selected_source_capture']['raw_source_sha256']==right['selected_source_capture']['raw_source_sha256']
                    and left['selected_source_capture']['source_receipt_sha256']==right['selected_source_capture']['source_receipt_sha256'])
                if not same:
                    matches.append({**row,'status':'matched_origin_different_outcome_source'});continue
                cost_left,cost_right=left.get('executable',{}),right.get('executable',{})
                entry_left,entry_right=cost_left.get('entry_sha256'),cost_right.get('entry_sha256')
                entry_same=entry_left is not None and entry_left==entry_right
                cost_a,cost_b=cost_left.get('original_prediction_side_net_bps'),cost_right.get('original_prediction_side_net_bps')
                cost_comparable=entry_same and cost_a is not None and cost_b is not None
                matches.append({**row,'status':'matched_scored_same_source_endpoint',
                    'actual_selected_price_epoch':left['actual_selected_price_epoch'],
                    'outcome_raw_source_sha256':left['selected_source_capture']['raw_source_sha256'],
                    'official_mae_bps':left['absolute_error_bps'],'ba_mae_bps':right['absolute_error_bps'],
                    'official_minus_ba_absolute_error_bps':str(Decimal(left['absolute_error_bps'])-Decimal(right['absolute_error_bps'])),
                    'official_direction_correct':left['direction_correct'],'ba_direction_correct':right['direction_correct'],
                    'official_brier':left['brier'],'ba_brier':right['brier'],
                    'entry_matching_status':('same_exact_entry_receipt' if entry_same else
                        'different_entry_observation' if entry_left and entry_right else 'entry_unavailable'),
                    'official_entry_sha256':entry_left,'ba_entry_sha256':entry_right,
                    'paired_bid_ask_cost_denominator_eligible':cost_comparable,
                    'official_minus_ba_original_side_net_bps':str(Decimal(cost_a)-Decimal(cost_b)) if cost_comparable else None})
    return matches

def evaluate(registry_path,expected_registry_sha256,*,clock=time.time):
    started=contract.epoch(clock())
    registry,registry_sha=pilot.load_registry(Path(registry_path),expected_registry_sha256)
    runtime=Path(registry['output_root']);reader=Reader(runtime)
    model=recovered.load_model(registry['model_path'])
    captures=defaultdict(list);sources=[];variants=[];proofs=defaultdict(list);issues=[]
    cycle_root=runtime/'cycles';cycle_dirs=sorted(cycle_root.iterdir()) if cycle_root.exists() else []
    require(len(cycle_dirs)<=MAX_CYCLES,'evaluator_cycle_bound')
    for cycle in cycle_dirs:
        files._safe_components(cycle)
        require(re.fullmatch(r'[a-z0-9_]{1,64}',cycle.name),'cycle_directory_name')
        completed=reader.optional(cycle/'cycle_completed.json')
        if completed is not None:_authority(completed)
        for pair in pilot.INSTRUMENTS:
            directory=cycle/pair.lower()
            if not directory.exists():continue
            files._safe_components(directory)
            mappings={}
            try:
                record,capture,mappings=_source_capture(reader,directory,pair,registry,clock)
                sources.append(record)
                if capture:captures[pair].append(capture)
            except (ValueError,KeyError,TypeError,OSError) as exc:
                sources.append({'status':'withheld','instrument':pair,'directory':directory.relative_to(runtime).as_posix(),
                    'reason_code':reason(exc)})
            for convention in pilot.CONVENTIONS:
                variant=directory/convention
                if not variant.exists():continue
                files._safe_components(variant)
                try:
                    summary,proof=_variant(reader,variant,pair,convention,mappings.get(convention),registry,model)
                    variants.append(summary)
                    if proof:proofs[proof['prepared']['prepared_sha256']].append(proof)
                except (ValueError,KeyError,TypeError,OSError) as exc:
                    variants.append({'directory':variant.relative_to(runtime).as_posix(),'status':'withheld',
                        'instrument':pair,'price_convention':convention,'reason_code':reason(exc)})
    consumption_index=defaultdict(list)
    consumption_root=runtime/'published/consumptions'
    consumption_paths=sorted(consumption_root.glob('*.json')) if consumption_root.exists() else []
    require(len(consumption_paths)<=MAX_CURVES*2,'evaluator_consumption_bound')
    for path in consumption_paths:
        try:
            value=reader.json(path);_authority(value)
            require(path.stem==value['consumption_sha256'],'consumption_filename_hash')
            consumption_index[value['curve_sha256']].append(value)
        except (ValueError,KeyError,TypeError,OSError) as exc:
            issues.append({'path':path.relative_to(runtime).as_posix(),'reason_code':reason(exc),'stage':'consumption'})
    curve_root=runtime/'published/curves'
    curve_dirs=sorted(curve_root.iterdir()) if curve_root.exists() else []
    require(len(curve_dirs)<=MAX_CURVES,'evaluator_curve_bound')
    retained_curves=[];scored_reports=[]
    for directory in curve_dirs:
        item={'directory':directory.relative_to(runtime).as_posix(),'status':'withheld'}
        try:
            files._safe_components(directory)
            require(re.fullmatch(r'[a-f0-9]{64}',directory.name),'curve_directory_hash')
            curve=reader.json(directory/'curve.json');prepared=curve['prepared_curve']
            contract.validate_curve(curve,expected_source_bindings=registry['source_bindings'])
            require(curve['issued_epoch']<=clock(),'future_curve_issue')
            require(curve['curve_sha256']==directory.name,'curve_filename_hash')
            require(registry['created_epoch']<=curve['issued_epoch']<registry['issue_cutoff_epoch'],'curve_outside_registered_issue_window')
            require(prepared['model_sha256']==registry['model_sha256'] and prepared['policy']==registry['curve_policy'],
                'curve_registry_model_or_policy_mismatch')
            pair=prepared['instrument'];convention=prepared['input_context']['price_convention']
            require(pair in pilot.INSTRUMENTS and convention in pilot.CONVENTIONS,'curve_registry_inventory')
            require(prepared['forecast_cohort']==registry['study_id']+'/'+pair+'/'+convention,'curve_registry_cohort')
            candidates=proofs.get(prepared['prepared_sha256'],[])
            require(len(candidates)==1 and candidates[0]['prepared']==prepared,'curve_missing_or_ambiguous_raw_computation_proof')
            proof=candidates[0];item.update(curve=curve,input_source_sha256=proof['source_raw_sha256'],
                variant_directory=proof['directory'].relative_to(runtime).as_posix())
            publication=reader.optional(directory/'publication.json')
            if publication is None:
                item.update(status='issued_without_publication_receipt',reason_code='missing_publication_receipt');retained_curves.append(item);continue
            files._validate_publication(curve,publication,registry['source_bindings'])
            require(publication['publication_completed_epoch']<=clock(),'future_curve_publication')
            item['publication']=publication
            valid=[]
            for consumption in consumption_index.get(curve['curve_sha256'],[]):
                try:
                    contract.validate_consumption(curve,publication,consumption,expected_source_bindings=registry['source_bindings'])
                    require(consumption['available_epoch']<=clock(),'future_consumption')
                    valid.append(consumption)
                except (ValueError,KeyError,TypeError) as exc:
                    issues.append({'curve_sha256':curve['curve_sha256'],'stage':'consumption','reason_code':reason(exc)})
            if not valid:
                item.update(status='published_without_consumption_receipt',reason_code='missing_valid_consumption_receipt');retained_curves.append(item);continue
            _stage_chain(proof,curve,publication,valid,clock())
            valid.sort(key=lambda c:(c['available_epoch'],c['consumption_sha256']))
            consumption=valid[0];item['consumption']=consumption;item['retained_valid_consumption_count']=len(valid)
            entry,decision=_entry(reader,proof,curve,publication,valid,registry,clock());item['decision_validation']=decision
            pair_captures=captures[pair]
            lo=min(n['target_label_epoch'] for n in prepared['nodes'])
            hi=max(n['target_label_epoch'] for n in prepared['nodes'])+prepared['target_selection_policy']['maximum_delay_sec']
            selected=[c for c in pair_captures if c['coverage_start_label_epoch']<=hi and c['coverage_end_label_epoch']>=lo]
            if not selected and pair_captures:selected=[max(pair_captures,key=lambda c:c['read_completed_epoch'])]
            now=contract.epoch(clock())
            report=outcomes.score_curve_from_captures(curve,publication,consumption,selected,
                expected_source_bindings=registry['source_bindings'],metadata=registry['metadata'][pair],clock=lambda:now,entry_quote=entry)
            item.update(status='scored_chain',report=report,dispositions=_dispositions(report,curve,now))
            scored_reports.append(report)
        except (ValueError,KeyError,TypeError,OSError) as exc:
            item['reason_code']=reason(exc)
        retained_curves.append(item)
    reader.unchanged();pilot.verify_sources(registry)
    finished=contract.epoch(clock());matches=_matched(retained_curves)
    dispositions=Counter(d['disposition'] for item in retained_curves for d in item.get('dispositions',[]))
    summary=outcomes.aggregate_reports(scored_reports)
    assessment={'schema_version':SCHEMA,'status':'completed','started_epoch':started,'completed_epoch':finished,
        'registry_path':str(Path(registry_path).absolute()),'registry_sha256':registry_sha,'runtime_root':str(runtime),
        'source_bindings':deepcopy(registry['source_bindings']),'source_closure_unchanged':True,
        'evaluator_source_sha256':sha(Path(__file__).read_bytes()),
        'native_horizons_sec':list(recovered.HORIZONS),'cycle_directories_observed':len(cycle_dirs),
        'source_capture_status_counts':dict(Counter(x['status'] for x in sources)),
        'attempt_status_counts':dict(Counter(x.get('attempt_status',x.get('status','unspecified')) for x in variants)),
        'curve_chain_status_counts':dict(Counter(x['status'] for x in retained_curves)),
        'view_dispositions':dict(dispositions),'matched_comparison_status_counts':dict(Counter(x['status'] for x in matches)),
        'retained_file_count':len(reader.manifest),'retained_file_bytes':reader.total,'issues':issues,
        'horizon_summary':summary,'independent_sample_size':None,
        'limits':['Each file was independently observed; ongoing new cycles may appear after the bounded enumeration.',
            'Derived outcome seals are made at actual evaluation time; earliest source election uses original attested query read completion.',
            'Missing source coverage, provider omissions, pending targets and withheld publication/consumption evidence remain distinct.',
            'Original nominal and delayed-target views overlap; horizon/variant counts are not independent trials.',
            'Matched variants require same pair/reference/native horizon/model/input raw source, and exact matching outcome source/endpoint for paired errors.',
            'No management fills, positions, dollars, profitability or calibration are inferred from these descriptive forecast/cost diagnostics.'],
        **contract.AUTHORITY}
    return {'assessment':assessment,'source_captures':sources,'variant_attempts':variants,
        'curves':retained_curves,'matched_comparisons':matches,'manifest':list(reader.manifest.values())}

def _save(path,value):
    data=json.dumps(value,sort_keys=True,indent=2,allow_nan=False).encode()+b'\n'
    with path.open('xb') as handle:handle.write(data)
    return {'path':path.name,'sha256':sha(data),'bytes':len(data)}

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry',type=Path,required=True)
    parser.add_argument('--registry-sha256',required=True)
    parser.add_argument('--output-directory',type=Path,required=True)
    args=parser.parse_args(argv)
    out=args.output_directory.absolute();base=Path(__file__).resolve().parent
    require(out.parent==base and not out.exists(),'new_direct_evaluation_directory_required')
    files._safe_components(base);out.mkdir()
    try:
        value=evaluate(args.registry,args.registry_sha256)
        refs=[]
        for name in ('source_captures','variant_attempts','matched_comparisons','manifest'):
            refs.append(_save(out/(name+'.json'),value[name]))
        (out/'curves').mkdir()
        for index,curve in enumerate(value['curves']):
            ref=_save(out/'curves'/('curve_'+str(index).zfill(5)+'.json'),curve)
            ref['path']='curves/'+ref['path'];refs.append(ref)
        assessment={**value['assessment'],'evidence_files':refs}
        receipt=_save(out/'PROSPECTIVE_PILOT_EVALUATION.json',assessment)
        print(json.dumps({'status':assessment['status'],'assessment':str(out/receipt['path']),
            'sha256':receipt['sha256'],'curve_counts':assessment['curve_chain_status_counts'],
            'view_dispositions':assessment['view_dispositions']}))
        return 0
    except Exception as exc:
        receipt=_save(out/'EVALUATION_FAILED.json',{'status':'failed','observed_epoch':time.time(),
            'error_type':type(exc).__name__,'reason_code':reason(exc),'registry_sha256':args.registry_sha256,
            'evaluator_source_sha256':sha(Path(__file__).read_bytes()),**contract.AUTHORITY})
        print(json.dumps({'status':'failed','receipt':str(out/receipt['path']),'sha256':receipt['sha256']}))
        return 1

if __name__=='__main__':raise SystemExit(main())
