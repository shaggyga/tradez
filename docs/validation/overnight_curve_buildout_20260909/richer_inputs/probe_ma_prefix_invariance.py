"""Check whether a selected historical MA feature row changes with later data.

No fitted artifact is loaded. Synthetic examples isolate feature causality;
they are not evidence of model accuracy or effect frequency in historical data.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
import numpy as np
import oanda_ma_feature_grid as ma


def main():
    cutoff=203
    prefix=np.ones(cutoff+1,dtype=np.float64)
    future=1+np.sin(np.arange(400,dtype=np.float64))*.002
    whole=np.r_[prefix,future]
    short,names=ma.build_ma_feature_matrix(prefix,[cutoff],.0001,'M1')
    longer,names2=ma.build_ma_feature_matrix(whole,[cutoff],.0001,'M1')
    assert names==names2
    changed=[dict(feature=name,at_prefix=float(a),with_future_rows=float(b),absolute_difference=float(abs(a-b)))
        for name,a,b in zip(names,short[0],longer[0]) if not np.isclose(a,b,atol=1e-7,rtol=1e-7,equal_nan=True)]
    vector=ma.build_ma_feature_vector(prefix,.0001,'M1')
    vector_diff=[dict(feature=name,batch=float(value),vector=vector.get(name)) for name,value in zip(names,short[0])
        if name not in vector or not np.isclose(value,vector[name],atol=1e-7,rtol=1e-7,equal_nan=True)]
    value=dict(schema_version='ma_prefix_causality_probe_20260909',
        observed_utc=datetime.now(timezone.utc).isoformat(),source_sha256=hashlib.sha256((ROOT/'oanda_ma_feature_grid.py').read_bytes()).hexdigest(),
        scope='synthetic_feature_causality_only_no_artifact_inference',
        same_selected_row_index=cutoff,original_prefix_rows=len(prefix),appended_future_rows=len(future),
        original_prefix_sha256=hashlib.sha256(prefix.tobytes()).hexdigest(),
        full_sequence_sha256=hashlib.sha256(whole.tobytes()).hexdigest(),
        feature_count=len(names),changed_feature_count=len(changed),changed_features=changed,
        same_prefix_batch_vector_difference_count=len(vector_diff),same_prefix_batch_vector_differences=vector_diff,
        source_findings=[dict(function='_rolling_scale',line=383,issue='fallback median includes later valid scales'),
                         dict(function='_cross_age',line=399,issue='no-previous-cross age uses entire supplied series length')],
        original_source_changed=False,model_loaded=False,forecasts_issued=False,research_only=True)
    path=OUT/'MA_PREFIX_CAUSALITY_PROBE_20260909.json'
    with path.open('x',encoding='utf8') as handle:json.dump(value,handle,indent=2,sort_keys=True,allow_nan=False)
    print(json.dumps(dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        feature_count=len(names),changed_feature_count=len(changed),
        changed_examples=changed[:3],same_prefix_batch_vector_difference_count=len(vector_diff))))


if __name__=='__main__':main()
