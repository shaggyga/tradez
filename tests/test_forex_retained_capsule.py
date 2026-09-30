import json
from pathlib import Path
import sys
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import forex_retained_capsule as m

def capsule(tmp_path):
    root=tmp_path/'source';p=root/(m.PREFIX+'model.bin');p.parent.mkdir(parents=True);p.write_bytes(b'original_saved_weights')
    reg={'normalizers':{},'connections':[{'models':[{'path':m.PREFIX+'model.bin','sha256':m.sha(p.read_bytes())}]}]}
    path=root/m.REGISTRY;path.parent.mkdir(parents=True);path.write_text(json.dumps(reg))
    archive=tmp_path/'saved.zip';receipt=m.pack(root,archive)
    return archive,receipt

def test_relocated_restore_and_conflict_preservation(tmp_path):
    archive,receipt=capsule(tmp_path);target=tmp_path/'replica'
    v=m.restore(target,archive,receipt['sha256']);assert v['models_fitted']==v['models_loaded']==0
    p=target/(m.PREFIX+'model.bin');assert p.read_bytes()==b'original_saved_weights'
    m.restore(target,archive,receipt['sha256'])
    p.write_bytes(b'user_edit')
    with pytest.raises(ValueError,match='existing_different'):m.restore(target,archive,receipt['sha256'])
    assert p.read_bytes()==b'user_edit'

def test_bad_identity_and_paths_refused(tmp_path):
    archive,receipt=capsule(tmp_path)
    with pytest.raises(ValueError,match='capsule_identity'):m.restore(tmp_path/'replica',archive,'0'*64)
    for path in ('../escape','C:/escape','trad/data/retained_connection_20260930/capsule/../../escape'):
        with pytest.raises(ValueError):m.contained(tmp_path,path)


def test_inline_metadata_and_additional_saved_model_restore(tmp_path):
    root=tmp_path/'source';model=root/(m.MODEL_PREFIX+'saved.joblib');model.parent.mkdir(parents=True);model.write_bytes(b'saved-not-fitted')
    ref={'path':m.MODEL_PREFIX+'saved.joblib','sha256':m.sha(model.read_bytes())}
    meta={'model_sha256':ref['sha256'],'fit_cutoff':123};meta['fit_id']=m.sha(json.dumps(meta,sort_keys=True,separators=(',',':')).encode())
    reg={'normalizers':{},'connections':[{'kind':'legacy26_extra_trees','models':[ref],'fit_metadata':meta}]}
    p=root/m.REGISTRY;p.parent.mkdir(parents=True);p.write_text(json.dumps(reg))
    cap=tmp_path/'saved.zip';receipt=m.pack(root,cap);target=tmp_path/'restored'
    assert m.restore(target,cap,receipt['sha256'])['payloads']==2
    assert json.loads((target/m.REGISTRY).read_text())==reg
    assert (target/ref['path']).read_bytes()==model.read_bytes()
    reg['connections'][0]['fit_metadata']['fit_cutoff']=999
    with pytest.raises(ValueError,match='inline_fit_metadata_identity'):m.records(reg)


def test_conflicting_duplicate_references_refused():
    refs=[{'path':m.PREFIX+'x','sha256':h} for h in ['a'*64,'b'*64]]
    with pytest.raises(ValueError,match='conflicting_artifact_identity'):m.records({'normalizers':{},'connections':[{'models':refs}]})
