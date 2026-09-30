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
