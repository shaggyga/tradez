"""As-of inspection of pinned original records; no fitting or outcome inference."""
import hashlib,json
from pathlib import Path
from contracts import fingerprint,validate_forecast
from prequential_residual_calibration_v2 import contract,apply_snapshot,visible_calibration
GROUPS=('legacy26','compact38_cost2','compact50_cost2','full228_cost2')
HORIZONS=(15,60,240,720,1440,2880,7200)
ORIGINS=tuple(range(1721606460,1722038400,21600))
QUERY_KEYS={'group','horizon_minutes','base_procedure','method','instrument','origin_epoch','calibration_mode','asof_epoch','reveal_outcome','outcome_asof_epoch'}

def validate_query(q,universe):
    if set(q)!=QUERY_KEYS:raise ValueError('exact_inspection_query_fields_required')
    if q['group'] not in GROUPS or type(q['horizon_minutes']) is not int or q['horizon_minutes'] not in HORIZONS:raise ValueError('registered_inspection_scope_required')
    if q['base_procedure'] not in ('frozen','adaptive') or q['method'] not in ('ridge','recovered_hgb') or q['calibration_mode'] not in contract()['modes']:raise ValueError('registered_inspection_procedure_required')
    if q['instrument'] not in universe or type(q['origin_epoch']) is not int or q['origin_epoch'] not in ORIGINS:raise ValueError('original_inspection_event_required')
    if type(q['asof_epoch']) is not int or q['asof_epoch']<q['origin_epoch']:raise ValueError('inspection_asof_at_or_after_origin_required')
    if type(q['reveal_outcome']) is not bool:raise ValueError('explicit_boolean_outcome_request_required')
    if q['reveal_outcome']:
        if type(q['outcome_asof_epoch']) is not int or q['outcome_asof_epoch']<q['origin_epoch']:raise ValueError('separate_outcome_asof_required')
    elif q['outcome_asof_epoch'] is not None:raise ValueError('outcome_asof_requires_explicit_reveal')

def unique(rows,label):
    if len(rows)!=1:raise ValueError('exact_original_'+label+'_required')
    return rows[0]

class OriginalRecordReader:
    """A bounded authenticated in-memory snapshot, cached only for this reader."""
    def __init__(self,paths,recipe):
        self.paths={k:Path(v) for k,v in paths.items()};self.recipe=recipe;self.cache={}

    def payload(self,alias,name):
        key=(alias,name)
        if key not in self.cache:
            expected=self.recipe['dependencies'][alias]['payloads']
            if Path(name).name!=name or name not in expected:raise ValueError('registered_inspector_payload_required')
            path=self.paths[alias]/name
            if path.is_symlink() or path.stat().st_size>8*1024*1024:raise ValueError('bounded_plain_inspector_payload_required')
            raw=path.read_bytes()
            if hashlib.sha256(raw).hexdigest()!=expected[name]:raise ValueError('inspector_consumed_bytes_changed')
            self.cache[key]=json.loads(raw)
        return self.cache[key]

    def inspect(self,q):
        validate_query(q,self.recipe['universe']);g=q['group'];h=q['horizon_minutes'];p=q['base_procedure'];method=q['method'];pair=q['instrument'];origin=q['origin_epoch'];mode=q['calibration_mode'];target=f'technical_endpoint_midpoint_elapsed_{h}m';rid=f'{pair}:{origin}';suffix=f'{g}_{h}_{p}.json'
        cov=unique([x for x in self.payload('calibration','coverage_'+suffix) if x['instrument']==pair and x['decision_epoch']==origin and x['method']==method and x['calibration_mode']==mode],'coverage')
        source=[x for x in self.payload('joint','forecasts_'+suffix) if x['record_id']==rid and x['method']==method]
        result={'query':dict(q),'status':None,'record':None,'snapshot':None,'base_forecast':None,'coverage':None,'outcome':None,'outcome_status':'not_requested','actual_publication_qualified':False,'production_calibrator_available_epoch':None,'base_models_fitted':0,'calibrators_fitted':0,'source_run_identities':{k:v['identity']['fingerprint'] for k,v in self.recipe['dependencies'].items()},'scope':'original_modeled_clock_development_records_only'}
        if cov['reason']!='eligible':
            if source:raise ValueError('unavailable_coverage_must_not_have_forecast')
            result.update(status=cov['calibration_status'],coverage=dict(cov))
        else:
            row=unique(source,'forecast');f=row['forecast'];validate_forecast(f)
            if row['group']!=g or row['procedure']!=p or f['target_id']!=target or f['instrument']!=pair or f['decision_epoch']!=origin:raise ValueError('original_forecast_scope_mismatch')
            data=self.payload('calibration','calibration_'+suffix)
            if (data['group'],data['target_id'],data['base_procedure'])!=(g,target,p):raise ValueError('original_calibration_chunk_scope_mismatch')
            record=unique([x for x in data['rows'] if x['base_forecast_id']==f['forecast_id'] and x['mode']==mode],'calibration_record')
            if record['record_id']!=rid or record['base_prediction_bps']!=f['prediction'] or record['base_forecast_available_epoch']!=f['available_epoch']:raise ValueError('original_calibration_forecast_mismatch')
            visible=visible_calibration(record,q['asof_epoch']);result['status']=visible['status']
            if visible['record'] is not None:
                snapshot=None
                if record['calibrator_id'] is not None:
                    snapshot=unique([x['snapshot'] for x in data['snapshots'] if x['mode']==mode and x['snapshot']['calibrator_id']==record['calibrator_id']],'snapshot')
                    if apply_snapshot(row,snapshot,mode)!=record:raise ValueError('original_calibration_application_mismatch')
                    support=snapshot['support']
                    if any(support[k] is not None and support[k]>snapshot['cutoff_epoch'] for k in ('maximum_forecast_available_epoch','maximum_label_available_epoch')):raise ValueError('snapshot_support_exceeds_cutoff')
                elif record['status']!='calibration_phase_not_started' or mode!='frozen_prefix' or origin>=contract()['frozen_cutoff']:raise ValueError('explicit_prefrozen_phase_required')
                result.update(record=dict(record),snapshot=snapshot,base_forecast=row,coverage=dict(cov))
        if q['reveal_outcome']:
            outcome=unique([x for x in self.payload('technical','pair_'+pair+'.json')['outcomes'] if x['record_id']==rid and x['target_id']==target],'outcome')
            if outcome['label_end_epoch']!=origin+h*60 or outcome['available_epoch']<outcome['label_end_epoch']:raise ValueError('original_outcome_clock_mismatch')
            if q['outcome_asof_epoch']<outcome['available_epoch']:result['outcome_status']='not_yet_mature'
            else:result.update(outcome=dict(outcome),outcome_status='observed' if outcome['value'] is not None else 'unavailable_original_label')
        # Return detached JSON: caller mutations cannot alter the cached snapshot.
        return json.loads(json.dumps(result,allow_nan=False))
