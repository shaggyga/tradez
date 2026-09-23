"""Quantify feature changes on an immutable real-history capture; no model fit."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT=Path(__file__).resolve().parent/'ma_history_audit'
sys.path.insert(0,str(ROOT))
import numpy as np
import pandas as pd
import oanda_ma_causal_features_v2 as causal
import oanda_ma_feature_grid as original


def changed(a,b):
    return ~np.isclose(a,b,rtol=1e-7,atol=1e-7,equal_nan=True)


def summarize(a,b,names):
    mask=changed(a,b)
    counts=mask.sum(axis=0)
    return dict(rows=int(len(a)),changed_rows=int(mask.any(axis=1).sum()),
        changed_feature_values=int(mask.sum()),distinct_changed_features=int((counts>0).sum()),
        maximum_changed_features_in_row=int(mask.sum(axis=1).max(initial=0)),
        field_counts={name:int(n) for name,n in zip(names,counts) if n})


def main():
    begun=time.time()
    manifest_path=OUT/'MA_HISTORY_CAPTURE_20260909.json'
    manifest=json.loads(manifest_path.read_text())
    original_sha=hashlib.sha256((ROOT/'oanda_ma_feature_grid.py').read_bytes()).hexdigest()
    assert original_sha==causal.ORIGINAL_SOURCE_SHA256
    results={}
    for pair,source in manifest['sources'].items():
        raw=Path(source['retained_path']).read_bytes()
        assert hashlib.sha256(raw).hexdigest()==source['retained_sha256']
        frame=pd.read_csv(source['retained_path'])
        prices=frame['close'].to_numpy(dtype=np.float64)
        pip=.01 if pair=='USD_JPY' else .0001
        segments=[s for s in source['contiguous_segments'] if s['row_count']>=204]
        totals=Counter()
        fields=Counter()
        details=[]
        prefix_checks=[]
        selected_review={s['start_index'] for s in sorted(segments,key=lambda s:s['row_count'],reverse=True)[:4]}
        for segment in segments:
            values=prices[segment['start_index']:segment['end_index']+1]
            positions=np.arange(203,len(values))
            old,names=original.build_ma_feature_matrix(values,positions,pip,'M1')
            revised,newnames=causal.build_feature_matrix(values,positions,pip,'M1')
            assert tuple(names)==tuple(newnames)
            detail=summarize(old,revised,names)
            fields.update(detail.pop('field_counts'))
            for key in ('rows','changed_rows','changed_feature_values'):
                totals[key]+=detail[key]
            totals['maximum_changed_features_in_row']=max(totals['maximum_changed_features_in_row'],detail['maximum_changed_features_in_row'])
            details.append({**segment,**detail})
            if segment['start_index'] in selected_review:
                for position in sorted(set((203,len(values)//2,len(values)-1))):
                    if position<203:
                        continue
                    old_prefix,_=original.build_ma_feature_matrix(values[:position+1],[position],pip,'M1')
                    new_prefix,_=causal.build_feature_matrix(values[:position+1],[position],pip,'M1')
                    row=position-203
                    prefix_checks.append(dict(segment_start_index=segment['start_index'],selected_position=position,
                        appended_future_rows=len(values)-position-1,
                        original_future_prefix_changed_fields=int(changed(old[[row]],old_prefix).sum()),
                        revised_future_prefix_changed_fields=int(changed(revised[[row]],new_prefix).sum()),
                        revised_vs_original_same_prefix_changed_fields=int(changed(new_prefix,old_prefix).sum())))
        positions=np.unique(np.r_[np.linspace(203,len(prices)-1,512,dtype=int),np.arange(len(prices)-720,len(prices))])
        old_full,names=original.build_ma_feature_matrix(prices,positions,pip,'M1')
        new_full,_=causal.build_feature_matrix(prices,positions,pip,'M1')
        full_summary=summarize(old_full,new_full,names)
        old_last,_=original.build_ma_feature_matrix(prices,[len(prices)-1],pip,'M1')
        new_last,_=causal.build_feature_matrix(prices,[len(prices)-1],pip,'M1')
        truncated,_=causal.build_feature_matrix(prices[-204:],[203],pip,'M1')
        old_vector=original.build_ma_feature_vector(prices,pip,'M1')
        vector=np.array([[old_vector[name] for name in names]])
        results[pair]=dict(source_sha256=source['retained_sha256'],source_rows=len(prices),
            contiguous_session_reset_comparison=dict(**dict(totals),distinct_changed_features=len(fields),
                field_counts=dict(sorted(fields.items())),segments=details),
            real_prefix_checks=prefix_checks,
            original_concatenated_archive_semantics_sample=full_summary,
            last_row=dict(original_batch_vs_revised_fields=int(changed(old_last,new_last).sum()),
                original_batch_vs_original_live_vector_fields=int(changed(old_last,vector).sum()),
                revised_all_history_vs_truncated204_fields=int(changed(new_last,truncated).sum())),
            prefix_invariance_all_verified=all(r['revised_future_prefix_changed_fields']==0 for r in prefix_checks))
        print(json.dumps(dict(pair=pair,warmed_rows=totals['rows'],changed_rows=totals['changed_rows'],
            feature_changes=totals['changed_feature_values'],distinct_fields=len(fields),
            old_prefix_changes=sum(r['original_future_prefix_changed_fields']>0 for r in prefix_checks),
            new_prefix_changes=sum(r['revised_future_prefix_changed_fields']>0 for r in prefix_checks),
            last_row=results[pair]['last_row'])),flush=True)
    result=dict(schema='ma_real_history_causality_audit_20260909',started_epoch=begun,completed_epoch=time.time(),
        observed_utc=datetime.now(timezone.utc).isoformat(),feature_count=643,timeframe='M1',
        capture_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        source_sha256={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
            ('oanda_ma_feature_grid.py','oanda_ma_causal_features_v2.py')},
        audit_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        comparison_tolerance={'rtol':1e-7,'atol':1e-7},
        methodology='All warmed rows in each exact consecutive-minute segment; each segment supplies its full history. Compare original full-segment features to revised causal features. Separately verify selected rows against their own prefixes; full-archive concatenation and latest204 truncation are explicitly separate diagnostics.',
        limitations=['Feature differences do not measure forecast accuracy or attribute prior losses.',
            'Session-reset comparisons define a new explicit gap policy; they are not a replay of old model training.',
            'Full-archive sample preserves the older row-based concatenation across missing minutes only as a labeled diagnostic.',
            'Historical ingestion clocks are not retained; all data was observed at actual capture time.',
            'Revised features need new fit/evaluation; retained weight compatibility is unproven.'],
        results=results,research_only=True,model_loaded=False,model_fitted=False,forecast_issued=False,
        can_place_orders=False,can_promote=False)
    assert hashlib.sha256((ROOT/'oanda_ma_feature_grid.py').read_bytes()).hexdigest()==original_sha
    path=OUT/'MA_REAL_HISTORY_CAUSALITY_AUDIT_20260909.json'
    with path.open('x',encoding='utf8') as handle:
        json.dump(result,handle,indent=2,sort_keys=True,allow_nan=False)
    print(json.dumps(dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),elapsed_sec=time.time()-begun)),flush=True)


if __name__=='__main__':
    main()
