"""Storage/proof counterexamples, never fabricated publication authority."""
import base64
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import zlib

import pytest

# Runner places all exact registered source owners beside these fixtures.
import revision_news_io_base_v1 as io


def storage_fixture(count,scan_count=0):
    objects=[];order={'publication':[],'scan':[]}
    for kind,n in [('publication',count),('scan',scan_count)]:
        for index in range(n):
            # Lexical JSON is deliberately not canonical. It must stay exact.
            raw=('{ "number" : 1.000, "text": "\\u65e5", "kind": "%s", "i": %d }'%(kind,index)).encode()
            packed=zlib.compress(raw);key=hashlib.sha256(raw).hexdigest();order[kind].append(key)
            objects.append({'kind':kind,'object_sha':key,'expanded_bytes':len(raw),
                'packed_sha':hashlib.sha256(packed).hexdigest(),'zlib_base64':base64.b64encode(packed).decode()})
    objects.sort(key=lambda value:(0 if value['kind']=='publication' else 1,value['object_sha']))
    rows={kind:[[v['object_sha'],v['expanded_bytes'],v['packed_sha'],len(base64.b64decode(v['zlib_base64']))]
                for v in objects if v['kind']==kind] for kind in order}
    manifest={'schema_version':'storage_test_not_admission','publication':{'profile':{'fixture':True},
        'record_proofs':[{'attempt':{'body':json.dumps({'snapshot_pack':{'evidence_sha256':order['publication']}})}}],
        'evidence_objects':rows['publication']},'consumer':{'observations':[{'observation':{'completed_scan':
        {'evidence_refs':[{'stored_evidence_sha256':key} for key in order['scan']]}}}]},'scan_objects':rows['scan']}
    return manifest,objects


def archive(tmp_path):
    root=tmp_path/'archive';root.mkdir();return io.Archive(root)


def refs(store,key):
    recipe=store.get(key)
    return recipe,{kind:io._load_object_catalog(store,key,kind) for kind,key in recipe['object_catalogs'].items()}


def test_packed_roundtrip_preserves_original_sha_order_exact_lexical_bytes_and_no_growth(tmp_path):
    store=archive(tmp_path);manifest,objects=storage_fixture(100,7)
    key=io._store_capture_archive(store,manifest,iter(objects));before=(len(store.files),store.total)
    rebuilt,stream=io._load_capture_archive(store,key);assert rebuilt==manifest and list(stream)==objects
    assert io._store_capture_archive(store,manifest,iter(objects))==key
    assert (len(store.files),store.total)==before
    assert not list(tmp_path.glob('compact_pack_spool_*'))


def test_complete_publication_and_scan_blocks_are_stable_across_append(tmp_path):
    store=archive(tmp_path);m,objects=storage_fixture(64,35)
    first=io._store_capture_archive(store,m,iter(objects));_,old=refs(store,first)
    later,more=storage_fixture(65,36);second=io._store_capture_archive(store,later,iter(more));_,new=refs(store,second)
    old_map={(r[0],r[1]):r[5:] for rows in old.values() for r in rows}
    new_map={(r[0],r[1]):r[5:] for rows in new.values() for r in rows}
    order=io._stable_object_order(m,io._object_expectations(m))
    for kind in ('publication','scan'):
        complete=[key for key in order if key[0]==kind][:64 if kind=='publication' else 32]
        assert all(old_map[key]==new_map[key] for key in complete)
    assert list(io._load_capture_archive(store,first)[1])==objects


def test_catalog_and_blocks_require_fewer_than_one_hundred_reads_for_thousand_objects(tmp_path,monkeypatch):
    store=archive(tmp_path);m,objects=storage_fixture(1000)
    key=io._store_capture_archive(store,m,iter(objects));calls=[];original=store.get
    def get(key,**kwargs):calls.append(key);return original(key,**kwargs)
    monkeypatch.setattr(store,'get',get)
    rebuilt,stream=io._load_capture_archive(store,key);assert rebuilt==m and list(stream)==objects
    assert len(calls)<100


def test_spool_bound_and_partial_generator_always_cleanup(tmp_path,monkeypatch):
    store=archive(tmp_path);m,objects=storage_fixture(35)
    with monkeypatch.context() as patch:
        patch.setattr(io,'MAX_SPOOL',10)
        with pytest.raises(ValueError,match='compact_spool_byte_bound'):
            io._store_capture_archive(store,m,iter(objects))
    assert not list(tmp_path.glob('compact_pack_spool_*')) and len(store.files)==0
    key=io._store_capture_archive(store,m,iter(objects));_,stream=io._load_capture_archive(store,key)
    next(stream);stream.close();assert not list(tmp_path.glob('compact_pack_spool_*'))


def test_original_receipt_missing_object_is_not_an_ordering_shortcut(tmp_path):
    store=archive(tmp_path);m,objects=storage_fixture(35)
    body=json.loads(m['publication']['record_proofs'][0]['attempt']['body'])
    body['snapshot_pack']['evidence_sha256'].pop()
    m['publication']['record_proofs'][0]['attempt']['body']=json.dumps(body)
    with pytest.raises(ValueError,match='compact_pack_original_reference_set_incomplete'):
        io._store_capture_archive(store,m,iter(objects))
    assert len(store.files)==0


def test_wrong_ordered_input_and_corrupted_durable_block_fail_closed(tmp_path):
    store=archive(tmp_path);m,objects=storage_fixture(35)
    wrong=deepcopy(objects);wrong[0],wrong[1]=wrong[1],wrong[0]
    with pytest.raises(ValueError,match='compact_archive_object_binding'):
        io._store_capture_archive(store,m,iter(wrong))
    key=io._store_capture_archive(store,m,iter(objects));_,rows=refs(store,key)
    block=store.root/(rows['publication'][0][5]+'.json');raw=block.read_bytes()
    block.write_bytes(raw.replace(b'zlib_base64',b'zlib_base65',1))
    with pytest.raises(ValueError,match='capture_object_hash_mismatch'):
        list(io._load_capture_archive(store,key)[1])
    assert not list(tmp_path.glob('compact_pack_spool_*'))


@pytest.mark.parametrize('mutation',['duplicate_slot','wrong_slot','extra_block_member'])
def test_catalog_slot_rebinding_and_unreferenced_block_member_fail(tmp_path,mutation):
    store=archive(tmp_path);m,objects=storage_fixture(20)
    key=io._store_capture_archive(store,m,iter(objects));recipe,rows=refs(store,key)
    changed=deepcopy(rows['publication'])
    if mutation=='duplicate_slot':changed[1][6]=changed[0][6]
    elif mutation=='wrong_slot':changed[0][6],changed[1][6]=changed[1][6],changed[0][6]
    else:
        value=store.get(changed[0][5]);value['objects'].append(deepcopy(value['objects'][0]));newblock=store.put(value)
        for row in changed:row[5]=newblock
    recipe['object_catalogs']['publication']=io._store_object_catalog(store,changed);altered=store.put(recipe)
    with pytest.raises(ValueError,match='compact_pack_duplicate_slot|compact_archive_object_binding|compact_pack_exact_slot_set'):
        list(io._load_capture_archive(store,altered)[1])
    assert not list(tmp_path.glob('compact_pack_spool_*'))


def test_capture_scoped_deferred_write_requires_complete_independent_byte_proof(tmp_path):
    store=archive(tmp_path);m,objects=storage_fixture(35)
    key=io._store_capture_archive(store,m,iter(objects));_,rows=refs(store,key)
    blockkey=rows['publication'][0][5];original=store.get(blockkey)
    path=store.root/(blockkey+'.json');raw=path.read_bytes()
    path.write_bytes(raw.replace(b'zlib_base64',b'zlib_base65',1))
    # General writes still check immediately. The capture provisioner may
    # return only a content key, never a handle; final readback must fail.
    with pytest.raises(ValueError,match='immutable_capture_object_collision'):store.put(original)
    assert io._store_capture_archive(store,m,iter(objects))==key
    with pytest.raises(ValueError,match='capture_object_hash_mismatch'):
        list(io._load_capture_archive(store,key)[1])


def test_spool_proof_reuse_keeps_one_complete_compressed_validation(tmp_path,monkeypatch):
    store=archive(tmp_path);m,objects=storage_fixture(35)
    key=io._store_capture_archive(store,m,iter(objects));calls=[];original=io._verify_compact_object
    def verify(value,expected):calls.append(expected[:2]);return original(value,expected)
    monkeypatch.setattr(io,'_verify_compact_object',verify)
    assert list(io._load_capture_archive(store,key)[1])==objects
    assert len(calls)==len(objects) and len(set(calls))==len(objects)


def test_repeated_recipe_aliases_are_charged_against_reconstruction_bound(tmp_path,monkeypatch):
    store=archive(tmp_path)
    child=store.put({'kind':'value','value':'x'*2000})
    parent=store.put({'kind':'items','items':[child]*20})
    rows=store.put({'kind':'value','value':[['rows',parent]]})
    root=store.put({'kind':'mapping','rows':rows})
    key=store.put({'schema_version':io.RECIPE,'compact_root':root,
        'digest_scope':'compact_manifest_not_expanded_legacy_readback'})
    with monkeypatch.context() as patch:
        patch.setattr(io,'MAX_READBACK',10000)
        with pytest.raises(ValueError,match='io_reconstruction_encoded_work_bound'):
            io._load_bundle(store,key)
    restored,bound=io._load_bundle(store,key,with_bound=True)
    assert restored=={'rows':['x'*2000]*20}
    assert len(io.encode(restored,io.MAX_READBACK))<=bound<=io.MAX_READBACK
