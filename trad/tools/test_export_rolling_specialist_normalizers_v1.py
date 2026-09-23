from datetime import datetime,timezone
import json
from pathlib import Path

import numpy as np
import pytest

from tools import export_rolling_specialist_normalizers_v1 as export


def fixture(tmp_path,schema='rolling_specialist_fold_inputs_v2_20260915'):
    root=tmp_path/'inputs';root.mkdir();(root/'pairs').mkdir()
    names=list(export.COMPACT_LOCAL)+[r['name'] for r in export.panel_registry()]
    pairs=[f'AAA_B{chr(65+i//26)}{chr(65+i%26)}' for i in range(68)]
    start=int(datetime(2026,3,23,tzinfo=timezone.utc).timestamp());week=7*86400
    cuts=[start+i*week for i in (2,3,4,5,6)]
    records={}
    payload={
        'normalizer_count':np.tile(np.arange(1,6,dtype=np.int64)[:,None]*40,(1,50)),
        'normalizer_mean':np.arange(250,dtype=np.float64).reshape(5,50)*.001,
        'normalizer_scale':np.ones((5,50),dtype=np.float64),
        'normalizer_supported':np.ones((5,50),dtype=bool),
        # Accessing this member with allow_pickle=False would fail. The
        # exporter must never decode it or copy it into the compact artifact.
        'raw_x':np.array([{'never':'decode feature rows'}],dtype=object),
    }
    payload['normalizer_mean'][0,0]=-0.
    payload['normalizer_mean'][:,1]=np.nan
    payload['normalizer_scale'][:,1]=0.
    payload['normalizer_supported'][:,1]=False
    for pair in pairs:
        path=root/'pairs'/(pair+'.npz');np.savez_compressed(path,**payload)
        records[pair]={'path':'pairs/'+path.name,'sha256':export.sha(path),'bytes':path.stat().st_size,
            'supported_fields_by_cutoff':[49]*5,'prefix_original_rows_by_cutoff':[100,200,300,400,500]}
    manifest={'schema':schema,'status':'complete','pairs':records,'feature_names':names,
        'groups':{'compact38':names[:38],'compact50':names},'fold_cutoffs_epoch':cuts,'final_normalizer_index':4,
        'boundaries':dict(start=start,train_end=start+6*week,validation_end=start+7*week,end=start+8*week),
        'source_bindings':{},'base_sha256':'base','endpoint_manifest_sha256':'endpoint','prepared_sha256':'prepared','quote_sha256':'quotes',
        'fit_cutoff_dates_utc':[datetime.fromtimestamp(x,timezone.utc).strftime('%Y-%m-%d') for x in cuts],
        'folds':[],'normalizer_scope':'synthetic fixture','normalizer_contract':'count20 positive variance'}
    path=root/'SPECIALIST_INPUTS.json';path.write_text(json.dumps(manifest))
    return root,path,manifest,payload


@pytest.mark.parametrize('schema',sorted(export.INPUT_SCHEMAS))
def test_complete_v1_v2_exact_seven_array_export_without_decoding_raw_x(tmp_path,monkeypatch,schema):
    root,path,manifest,payload=fixture(tmp_path,schema)
    monkeypatch.setattr(export,'ROOT',tmp_path)
    output=tmp_path/'docs/validation/normalizers'
    report=export.export(root,output)
    assert report['status']=='complete' and report['source_manifest_sha256']==export.sha(path)
    assert report['schema']=='rolling_specialist_normalizer_extraction_v1_20260915'
    assert report['supported_pair_feature_cells_by_cutoff']==[49*68]*5
    assert report['readback']['source_normalizer_sets_verified']==340
    with np.load(output/'NORMALIZERS.npz',allow_pickle=False) as arrays:
        assert set(arrays.files)=={'count','mean','scale','supported','pair_names','feature_names','fold_cutoffs_epoch'}
        for name in export.ARRAY_DTYPES:
            assert arrays[name].shape==(5,68,50)
            for pid in range(68):
                assert export.array_record(arrays[name][:,pid])==export.array_record(payload['normalizer_'+name])
        assert np.signbit(arrays['mean'][0,0,0])
        assert arrays['pair_names'].tolist()==sorted(manifest['pairs'])
        assert arrays['feature_names'].tolist()==manifest['feature_names']
    assert report['raw_x_or_feature_rows_or_labels_included'] is False
    assert (output/'NORMALIZERS.npz').stat().st_size<1024*1024
    with pytest.raises(ValueError,match='new_docs'):export.export(root,output)


@pytest.mark.parametrize('kind',['incomplete','wrong_schema','pair_count','order','cutoff'])
def test_invalid_manifest_rejected_before_creating_output(tmp_path,monkeypatch,kind):
    root,path,m,payload=fixture(tmp_path);monkeypatch.setattr(export,'ROOT',tmp_path)
    if kind=='incomplete':m['status']='building'
    elif kind=='wrong_schema':m['schema']='other'
    elif kind=='pair_count':m['pairs'].pop(next(iter(m['pairs'])))
    elif kind=='order':m['feature_names']=m['feature_names'][::-1]
    elif kind=='cutoff':m['fold_cutoffs_epoch'][0]+=60
    path.write_text(json.dumps(m));output=tmp_path/'docs/validation/new'
    with pytest.raises(ValueError):export.export(root,output)
    assert not output.exists()


def test_changed_pair_bytes_rejected(tmp_path,monkeypatch):
    root,path,m,payload=fixture(tmp_path);monkeypatch.setattr(export,'ROOT',tmp_path)
    source=root/next(iter(m['pairs'].values()))['path']
    with source.open('ab') as handle:handle.write(b'changed')
    output=tmp_path/'docs/validation/new'
    with pytest.raises(ValueError,match='sha256'):export.export(root,output)
    assert not output.exists()


@pytest.mark.parametrize('kind',['dtype','shape','support_count'])
def test_rehashed_wrong_arrays_or_metadata_still_rejected(tmp_path,monkeypatch,kind):
    root,path,m,payload=fixture(tmp_path);monkeypatch.setattr(export,'ROOT',tmp_path)
    pair=next(iter(m['pairs']));rec=m['pairs'][pair];source=root/rec['path']
    if kind=='dtype':payload['normalizer_count']=payload['normalizer_count'].astype(np.int32)
    elif kind=='shape':payload['normalizer_mean']=payload['normalizer_mean'][:,:49]
    else:rec['supported_fields_by_cutoff'][0]-=1
    np.savez_compressed(source,**payload);rec['sha256']=export.sha(source);rec['bytes']=source.stat().st_size;path.write_text(json.dumps(m))
    with pytest.raises(ValueError):export.collect(path)


def test_after_read_mutation_is_detected_by_final_source_pass(tmp_path,monkeypatch):
    root,path,m,payload=fixture(tmp_path);original=export.verify_sources
    def changed(manifest_path,manifest_sha,records):
        first=Path(next(iter(records.values()))['path'])
        with first.open('ab') as handle:handle.write(b'late mutation')
        original(manifest_path,manifest_sha,records)
    monkeypatch.setattr(export,'verify_sources',changed)
    with pytest.raises(ValueError,match='sha256'):export.collect(path)


def test_source_bindings_and_output_scope_are_checked(tmp_path,monkeypatch):
    root,path,m,payload=fixture(tmp_path);monkeypatch.setattr(export,'ROOT',tmp_path)
    source=tmp_path/'source.py';source.write_text('retained source')
    m['source_bindings']={'source.py':export.sha(source)};path.write_text(json.dumps(m))
    export.collect(path)
    source.write_text('changed')
    with pytest.raises(ValueError,match='sha256'):export.collect(path)
    with pytest.raises(ValueError,match='new_docs'):export.export(root,tmp_path/'outside')
