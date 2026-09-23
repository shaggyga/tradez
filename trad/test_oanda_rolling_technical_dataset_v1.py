import argparse
import json
from pathlib import Path
import numpy as np
import pytest
from oanda_rolling_technical_dataset_v1 import origin_splits,split_maturity,iter_partitions
from tools import build_rolling_technical_dataset_v1 as build


def test_strict_terminal_end_purge_and_original_split_membership():
    start=0;train_end=10000*60;val_end=20000*60;end=30000*60
    t=np.array([train_end-3660-60,train_end-3660,train_end-60,train_end,val_end,end-60])
    assert origin_splits(t,start,train_end,val_end,end).tolist()==['train','train','train','validation','later_development_test','later_development_test']
    assert split_maturity(t,60,start,train_end,val_end,end).tolist()==[True,False,False,True,True,False]
    with pytest.raises(ValueError,match='outside'):
        origin_splits(np.array([-60]),start,train_end,val_end,end)


def test_fractional_or_nonminute_boundary_rejected():
    with pytest.raises(ValueError,match='whole'):
        build.epoch('2026-07-13T00:00:00.9Z')
    with pytest.raises(ValueError,match='aligned'):
        build.epoch('2026-07-13T00:00:01Z')


def test_complete_68_pair_build_readback_and_feature_label_separation(tmp_path,monkeypatch):
    monkeypatch.setattr(build,'ROOT',tmp_path)
    for name in build.SOURCE_NAMES:
        path=tmp_path/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('fixture source pin '+name)
    manifest=tmp_path/'source.json';manifest.write_text('{}')
    currencies=('AUD','CAD','CHF','EUR','GBP','HKD','JPY','NZD','SGD','USD')
    pairs=[a+'_'+b for a in currencies for b in currencies if a!=b][:68]
    recipes={pair:{'manifest_sha256':build.file_sha(manifest)} for pair in pairs}
    monkeypatch.setattr(build.inputs,'discover_archive_sources',lambda path:recipes)
    metadata=tmp_path/'pip.json';metadata.write_text(json.dumps({'pairs':{p:{'pip_size':.0001} for p in pairs}}))
    def synthetic_range(pair,recipe,start,end,observed,**kw):
        t=np.arange(start,end,60,dtype=np.int64)
        close=100+np.arange(len(t))*.001
        data={'time':t,'open':close,'high':close+.003,'low':close-.003,'close':close,
              'bid_close':close-.0001,'ask_close':close+.0001,'volume':np.full(len(t),5.)}
        # Sparse future/late origins are retained, not selected by labels.
        keep=np.ones(len(t),dtype=bool);keep[-20:-18]=False
        return {k:a[keep] for k,a in data.items()},{'pair':pair,'observed_epoch':observed}
    monkeypatch.setattr(build.ranges,'read_range',synthetic_range)
    args=argparse.Namespace(manifest=manifest,pair_metadata=metadata,output=tmp_path/'data'/'result',
        start='2026-07-13T00:00:00Z',train_end='2026-07-13T00:10:00Z',validation_end='2026-07-13T00:20:00Z',
        end='2026-07-13T00:30:00Z',max_output_gib=1,minimum_free_gib=16)
    report=build.run(args)
    assert report['status']=='complete' and report['origin_rows']==68*30
    assert report['feature_count']==228 and report['readback']['all_shards_verified']
    assert len(report['partitions'])==68 and len(report['pairs'])==68
    assert all(r['core']['rows']==r['peers']['rows']==30 for r in report['partitions'])
    selected=list(iter_partitions(args.output,split='validation',feature_names=['m1__return_1_bps','peer__diff_15_bps']))
    assert sum(table.num_rows for _,table in selected)==68*10
    for _,table in selected:
        assert 'label__60m__state' in table.column_names
        assert table['origin_split'].to_pylist()==['validation']*10
        assert table['label__60m__split_eligible'].to_pylist()==[False]*10
    with pytest.raises(ValueError,match='registered'):
        list(iter_partitions(args.output,feature_names=['label__60m__return_bps']))
    summary=json.loads((args.output/'TRAIN_FEATURE_SUMMARY.json').read_text())
    assert all(s['finite_train_rows']+s['missing_train_rows']==680 for s in summary['features'].values())
    core=args.output/report['partitions'][0]['core']['path']
    with core.open('ab') as f:
        f.write(b'changed')
    with pytest.raises(ValueError,match='hash'):
        list(iter_partitions(args.output))
