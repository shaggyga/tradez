"""An archive-layout hint may save provisioning, never independent proof."""
from copy import deepcopy
import hashlib
import json

import pytest
import test_packed_archive_v5 as fixtures

io=fixtures.io


def verified(store,manifest,objects,prior=()):
    key=io._store_capture_archive(store,manifest,iter(objects),prior_layout=prior)
    rebuilt,stream,layout=io._load_capture_archive(store,key,with_layout=True)
    assert rebuilt==manifest and list(stream)==objects
    return key,layout


def test_identical_complete_groups_skip_provisioning_but_verify_every_fresh_object(tmp_path,monkeypatch):
    store=fixtures.archive(tmp_path);m,objects=fixtures.storage_fixture(64,32)
    original,layout=verified(store,m,objects);before=(len(store.files),store.total)
    spooled=[];checked=[];old_put=io._ObjectSpool.put;old_verify=io._verify_compact_object
    def put(self,key,value):spooled.append(key);return old_put(self,key,value)
    def verify(value,item):checked.append(item[:2]);return old_verify(value,item)
    monkeypatch.setattr(io._ObjectSpool,'put',put);monkeypatch.setattr(io,'_verify_compact_object',verify)
    assert io._store_capture_archive(store,m,iter(objects),prior_layout=layout)==original
    assert spooled==[] and len(checked)==len(objects)
    assert (len(store.files),store.total)==before
    # Independent readback still visits and authenticates every original object.
    assert list(io._load_capture_archive(store,original)[1])==objects
    assert len(spooled)==len(objects) and len(checked)==2*len(objects)


def test_growing_manifest_rewrites_partial_groups_and_keeps_complete_original_groups(tmp_path,monkeypatch):
    store=fixtures.archive(tmp_path);m,objects=fixtures.storage_fixture(65,35)
    original,layout=verified(store,m,objects);later,more=fixtures.storage_fixture(66,36)
    spooled=[];old=io._ObjectSpool.put
    def put(self,key,value):spooled.append(key);return old(self,key,value)
    monkeypatch.setattr(io._ObjectSpool,'put',put)
    new=io._store_capture_archive(store,later,iter(more),prior_layout=layout)
    assert len(spooled)==6  # two publication and four scan terminal members.
    rebuilt,stream=io._load_capture_archive(store,new);assert rebuilt==later and list(stream)==more
    assert list(io._load_capture_archive(store,original)[1])==objects
    assert io._store_capture_archive(store,later,iter(more))==new


def test_changed_original_order_does_not_reuse_same_members_in_different_slots(tmp_path):
    store=fixtures.archive(tmp_path);m,objects=fixtures.storage_fixture(64)
    _,layout=verified(store,m,objects);changed=deepcopy(m)
    body=json.loads(changed['publication']['record_proofs'][0]['attempt']['body'])
    body['snapshot_pack']['evidence_sha256'].reverse()
    changed['publication']['record_proofs'][0]['attempt']['body']=json.dumps(body)
    assert verified(store,changed,objects,layout)[0]==verified(store,changed,objects)[0]


def test_changed_compressed_bytes_cannot_hide_behind_unchanged_layout(tmp_path):
    store=fixtures.archive(tmp_path);m,objects=fixtures.storage_fixture(64)
    _,layout=verified(store,m,objects);changed=deepcopy(objects)
    changed[0]['zlib_base64']='A'+changed[0]['zlib_base64'][1:]
    with pytest.raises(ValueError,match='compact_archive_compressed_byte_binding'):
        io._store_capture_archive(store,m,iter(changed),prior_layout=layout)


def test_corrupt_reused_block_is_never_replaced_or_accepted_as_cache_miss(tmp_path):
    store=fixtures.archive(tmp_path);m,objects=fixtures.storage_fixture(64)
    original,layout=verified(store,m,objects);block=store.root/(layout[0][5]+'.json')
    corrupt=block.read_bytes().replace(b'zlib_base64',b'zlib_base65',1);block.write_bytes(corrupt)
    assert io._store_capture_archive(store,m,iter(objects),prior_layout=layout)==original
    assert block.read_bytes()==corrupt
    with pytest.raises(ValueError,match='capture_object_hash_mismatch'):
        list(io._load_capture_archive(store,original)[1])


@pytest.mark.parametrize('change',['mutable','duplicate','wrong_slot','bool_size'])
def test_layout_bound_types_and_duplicates_are_not_trusted(tmp_path,change):
    store=fixtures.archive(tmp_path);m,objects=fixtures.storage_fixture(64)
    _,layout=verified(store,m,objects)
    if change=='mutable':bad=list(layout)
    elif change=='duplicate':bad=layout+(layout[0],)
    else:
        row=list(layout[0]);row[6 if change=='wrong_slot' else 2]=99 if change=='wrong_slot' else True
        bad=(tuple(row),)+layout[1:]
    with pytest.raises(ValueError,match='compact_layout|compact_manifest_object_identity'):
        io._store_capture_archive(store,m,iter(objects),prior_layout=bad)


def test_current_failure_discards_layout_and_retains_unusable_session(tmp_path):
    store=fixtures.archive(tmp_path);m,objects=fixtures.storage_fixture(32)
    _,layout=verified(store,m,objects)
    state={'failure_serial':0,'usable':True,'archive_layout':layout}
    io._failure(state,{'archive_root':str(store.root)},'fixture_reused_block_failure',ValueError('corrupt'))
    assert state['archive_layout']==() and state['usable'] is False and state['failure_serial']==1
    assert state['last_failure']['error']=='ValueError:corrupt'


def test_metadata_size_grouping_is_exact_base64_padding_and_byte_boundary(tmp_path,monkeypatch):
    store=fixtures.archive(tmp_path);m,objects=fixtures.storage_fixture(75,2)
    expected=io._object_expectations(m);lookup={(v['kind'],v['object_sha']):v for v in objects}
    for item in expected:assert io._expected_object_size(item)==len(io.encode(lookup[item[:2]]))
    # Force small groups so the byte bound, rather than member count, splits.
    monkeypatch.setattr(io,'PACK_BYTES',1500)
    first,layout=verified(store,m,objects)
    assert verified(store,m,objects,layout)[0]==first
