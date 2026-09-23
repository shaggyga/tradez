import argparse
import json
from pathlib import Path
import numpy as np
import pyarrow as pa
import pytest
from test_oanda_rolling_model_design_v1 import names
from tools import prepare_rolling_model_comparison_v1 as prepare


def test_all68_clock_selection_no_future_filter_and_train_transform(tmp_path,monkeypatch):
    monkeypatch.setattr(prepare,'ROOT',tmp_path)
    for n in ['oanda_rolling_technical_dataset_v1.py','oanda_rolling_model_design_v1.py','tools/prepare_rolling_model_comparison_v1.py']:
        p=tmp_path/n;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('test-bound source')
    base=tmp_path/'base';overlay=tmp_path/'overlay';base.mkdir();overlay.mkdir();(tmp_path/'data').mkdir()
    pairs=[f'P{i:02d}_USD' for i in range(68)]
    manifest={'status':'complete','feature_names':names(),'pairs':{p:{'origin_rows':90} for p in pairs},'boundaries':{}}
    (base/'DATASET.json').write_text(json.dumps(manifest))
    (overlay/'ENDPOINT_DATASET.json').write_text(json.dumps({'status':'complete','base_manifest_sha256':prepare.file_sha(base/'DATASET.json'),'source_bindings':{}}))
    t=1800000000+np.arange(90)*60
    matrix=np.repeat(np.arange(90,dtype=float)[:,None],228,axis=1)
    matrix[60:]+=1e6
    cols={'bar_start_epoch':t,'origin_split':['train']*60+['validation']*15+['later_development_test']*15}
    cols.update({n:matrix[:,i] for i,n in enumerate(names())})
    for h in prepare.HORIZONS:
        for prefix in ['endpoint_label','label']:
            for f in ['return_bps','long_net_bps','short_net_bps']:
                cols[f'{prefix}__{h}m__{f}']=np.full(90,np.nan)
            for f in ['midpoint_valid','bidask_endpoint_valid','split_eligible']:
                cols[f'{prefix}__{h}m__{f}']=np.zeros(90,dtype=bool)
    table=pa.table(cols)
    monkeypatch.setattr(prepare,'iter_endpoint_partitions',lambda *a,**k:((p,table) for p in pairs))
    r=prepare.run(argparse.Namespace(base=base,overlay=overlay,output=tmp_path/'data'/'prepared'))
    expected=(t%900==0)&(np.arange(90)<60) | (np.arange(90)>=60)
    assert r['status']=='complete' and r['rows']==68*int(expected.sum())
    assert sum(p['validation_rows'] for p in r['pairs'].values())==68*15
    with np.load(tmp_path/'data'/'prepared'/r['pairs'][pairs[0]]['path']) as z:
        assert np.array_equal(z['time'],t[expected])
        assert np.all(z['normalizer_mean']==29.5)
        assert np.isnan(z['y_60']).all() and not z['valid_60'].any()
        assert np.isfinite(z['x']).all()
        assert z['x'][0,0]<0 and z['x'][-1,0]>10000
    with pytest.raises(ValueError,match='new_project'):prepare.run(argparse.Namespace(base=base,overlay=overlay,output=tmp_path/'data'/'prepared'))
