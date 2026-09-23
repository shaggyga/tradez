"""Original raw-bar causal extraction and deliberately future-reading positive control."""
import hashlib,io,json,math
import pandas as pd
from contracts import fingerprint
from rolling_registry_adapter_v2 import pair_features
from rolling_registry_operator_v2 import checked_bytes

def changed(left,right):
    if set(left)!=set(right):raise ValueError('feature_schema_changed')
    return sorted(k for k in left if left[k]!=right[k])

def audit_frame(pair,frame,pip,source_sha,origin):
    epoch=origin-60
    # Exactly the established603 completed bars plus two future bars; no labels/models.
    window=frame.loc[(frame.time>=epoch-602*60)&(frame.time<=origin+60)].copy()
    for name in window.columns:
        if name!='time':window[name]=window[name].astype('float64')
    past=window.loc[window.time<origin].copy();future=window.loc[window.time>=origin]
    def extract(data):return pair_features(pair,data,pip,source_sha,[origin])['observations'][0]
    clean=extract(window);prefix=extract(past);poison=window.copy()
    columns=[n for n in poison.columns if n!='time'];poison.loc[poison.time>=origin,columns]*=1.37
    altered=extract(poison)
    finite=sum(v is not None for v in clean['values'].values())
    prefix_differences=changed(clean['values'],prefix['values']);future_differences=changed(clean['values'],altered['values'])
    # Intentionally invalid: reads the first uncompleted minute's mid_close at origin.
    exact=window.loc[window.time==origin];bad=poison.loc[poison.time==origin]
    leaky_original=float(exact.close.iloc[0]) if len(exact)==1 else None
    leaky_poisoned=float(bad.close.iloc[0]) if len(bad)==1 else None
    leaky_available=leaky_original is not None and math.isfinite(leaky_original) and math.isfinite(leaky_poisoned)
    detected=leaky_available and leaky_original!=leaky_poisoned
    status='verified_causal_and_leak_detected' if clean['exact_reference_present'] and finite and not prefix_differences and not future_differences and detected else 'unavailable_support' if not clean['exact_reference_present'] or not finite or not leaky_available else 'audit_failed'
    return {'instrument':pair,'origin_epoch':origin,'status':status,'source_member_sha256':source_sha,'window_rows':len(window),'past_rows':len(past),'future_rows':len(future),
        'exact_reference_present':clean['exact_reference_present'],'finite_causal_features':finite,'missing_causal_features':len(clean['values'])-finite,
        'clean_values_sha256':fingerprint(clean['values']),'prefix_values_sha256':fingerprint(prefix['values']),'poisoned_values_sha256':fingerprint(altered['values']),
        'prefix_differences':prefix_differences,'future_differences':future_differences,
        'planted_leak':{'field':'mid_close_of_bar_starting_at_origin','available_at':origin+60,'illegally_read_at':origin,'original_value':leaky_original,'poisoned_value':leaky_poisoned,'detected':detected,'admitted_to_campaign':False},
        'clock_scope':'completed_bar_end_assumption_not_measured_arrival','models_fitted':0}

def build(root,manifest_sha,configuration):
    raw=(root/'INPUT_MANIFEST.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=manifest_sha:raise ValueError('control_consumed_raw_manifest_changed')
    manifest=json.loads(raw);rows=[]
    for member in manifest['members']:
        frame=pd.read_parquet(io.BytesIO(checked_bytes(root,member)))
        for origin in configuration['leak_origins']:
            rows.append(audit_frame(member['instrument'],frame,member['pip_size'],member['source_member_sha256'],origin))
    if any(r['status']=='audit_failed' for r in rows):raise ValueError('future_positive_control_or_causal_audit_failed')
    return rows
