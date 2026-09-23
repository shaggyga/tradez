from datetime import datetime, timezone
from pathlib import Path
import argparse
import json

import numpy as np
import pytest

from tools import prepare_rolling_specialists_v2 as prep


def boundaries(date='2026-07-13'):
    start = int(datetime.fromisoformat(date).replace(tzinfo=timezone.utc).timestamp())
    return dict(start=start, train_end=start+6*prep.WEEK,
                validation_end=start+7*prep.WEEK, end=start+8*prep.WEEK)


def example(date='2026-07-13'):
    b = boundaries(date)
    times = b['start'] + np.repeat(np.arange(56, dtype=np.int64)*86400, 4) + np.tile([0, 60, 120, 900], 56)
    row = np.arange(len(times), dtype=np.float64)
    raw = np.column_stack([np.sin(row/(j+3)) + row*.001 for j in range(50)])
    raw[:, 1] = .1
    raw[times < b['start']+20*86400, 2] = np.nan
    raw[0, 3] = -0.
    keep = (times >= b['train_end']) | (times % 900 == 0)
    retained = times[keep]
    split = np.where(retained < b['train_end'], 0, np.where(retained < b['validation_end'], 1, 2)).astype(np.int8)
    old = prep.transform_inputs(raw[keep], prep.fit_normalizer(raw[times < b['train_end']]))
    mid = 1.1 + row*1e-6
    return dict(raw_times=times, raw_x=raw, retained_times=retained, retained_split=split,
                old_x=old, quote_times=times, mid=mid, bid=mid-.0001, ask=mid+.0002, boundaries=b)


def test_original_dates_recreate_v1_schedule_and_exact_arrays():
    e = example()
    schedule = prep.derive_schedule(e['boundaries'])
    assert schedule['fit_cutoffs_epoch'] == list(prep.legacy.FIT_CUTOFFS)
    old_args = {k:v for k,v in e.items() if k != 'boundaries'}
    before = prep.legacy.prepare_pair_values(**old_args)
    after = prep.prepare_pair_values(**e)
    assert set(before) == set(after)
    assert all(prep.exact_arrays(before[k],after[k]) for k in before)


def test_earlier_period_translates_clocks_without_changing_numerics():
    original, earlier = example(), example('2026-05-18')
    a, b = prep.prepare_pair_values(**original), prep.prepare_pair_values(**earlier)
    assert prep.derive_schedule(earlier['boundaries'])['fit_cutoff_dates_utc'] == [
        '2026-06-01','2026-06-08','2026-06-15','2026-06-22','2026-06-29']
    for name in a:
        if name != 'time': assert prep.exact_arrays(a[name],b[name]),name
    assert np.array_equal(a['time']-b['time'], np.full(len(a['time']),8*prep.WEEK))
    assert a['normalizer_count'].shape == (5,50)
    assert a['normalizer_count'][:,0].tolist() == [56,84,112,140,168]
    assert a['normalizer_supported'][:,1].tolist() == [False]*5
    assert not a['normalizer_supported'][0,2]


@pytest.mark.parametrize('kind', ['short_train','long_validation','short_final','unaligned','bool','extra','nonmonday'])
def test_invalid_period_contract_fails(kind):
    b = boundaries()
    if kind == 'short_train': b['train_end'] -= 60
    elif kind == 'long_validation': b['validation_end'] += prep.WEEK
    elif kind == 'short_final': b['end'] -= prep.WEEK
    elif kind == 'unaligned': b['start'] += 1
    elif kind == 'bool': b['start'] = True
    elif kind == 'extra': b['hidden_cutoff'] = b['start']
    elif kind == 'nonmonday': b = {k:v+86400 for k,v in b.items()}
    with pytest.raises(ValueError): prep.derive_schedule(b)


def test_future_changes_leave_all_earlier_prefix_statistics_unchanged():
    e = example('2026-05-18');before = prep.prepare_pair_values(**e)
    changed = {k:v.copy() if isinstance(v,np.ndarray) else dict(v) if isinstance(v,dict) else v for k,v in e.items()}
    cutoff = prep.derive_schedule(e['boundaries'])['fit_cutoffs_epoch'][1]
    changed['raw_x'][changed['raw_times'] >= cutoff,0] += 1000
    index = np.searchsorted(changed['raw_times'],changed['retained_times'])
    changed['old_x'] = prep.transform_inputs(changed['raw_x'][index],prep.fit_normalizer(changed['raw_x'][changed['raw_times']<e['boundaries']['train_end']]))
    after = prep.prepare_pair_values(**changed)
    for name in ('count','mean','scale','supported'):
        assert prep.exact_arrays(before['normalizer_'+name][:2],after['normalizer_'+name][:2])
    assert not prep.exact_arrays(before['normalizer_mean'][-1],after['normalizer_mean'][-1])


def test_origin_at_final_cutoff_belongs_to_assessment_and_not_normalizer():
    e=example();result=prep.prepare_pair_values(**e)
    at=np.flatnonzero(result['time']==e['boundaries']['train_end'])
    assert len(at)==1 and result['split'][at[0]]==1
    assert result['normalizer_count'][-1,0] == np.sum(e['raw_times']<e['boundaries']['train_end'])


@pytest.mark.parametrize('which', ['train','assessment'])
def test_no_dropping_original_rows_for_future_or_feature_availability(which):
    e=example();remove=np.flatnonzero(e['retained_split']==(0 if which=='train' else 1))[0]
    for name in ('retained_times','retained_split','old_x'):e[name]=np.delete(e[name],remove,axis=0)
    with pytest.raises(ValueError,match='all_original'):prep.prepare_pair_values(**e)


def test_wrong_split_and_context_outside_origin_window_fail():
    e=example();e['retained_split'][-1]=1
    with pytest.raises(ValueError,match='original_split'):prep.prepare_pair_values(**e)
    e=example();e['raw_times'][0]-=60
    with pytest.raises(ValueError,match='inside_declared_period'):prep.prepare_pair_values(**e)


def partition_fixture():
    pairs={f'PAIR_{i:02d}':{'origin_rows':2} for i in range(68)}
    records=[{'pair':p,'block':0,'core':{'rows':2},'peers':{'rows':2}} for p in pairs]
    return {'pairs':pairs,'partitions':records,'origin_rows':136}


def test_actual_partition_census_replaces_magic_544_without_dropping_pairs():
    m=partition_fixture();result=prep.validate_partitions(m)
    assert result=={'partitions':68,'possible_pair_weeks':544,'missing_pair_weeks':476}
    m['partitions'].pop()
    with pytest.raises(ValueError,match='population_identity'):prep.validate_partitions(m)


@pytest.mark.parametrize('which',['duplicate','outside','wrong_rows','total'])
def test_partition_identity_is_strict(which):
    m=partition_fixture()
    if which=='duplicate':m['partitions'].append(m['partitions'][0].copy())
    elif which=='outside':m['partitions'][0]['block']=8
    elif which=='wrong_rows':m['partitions'][0]['peers']['rows']=1
    else:m['origin_rows']+=1
    with pytest.raises(ValueError):prep.validate_partitions(m)


def test_generalization_adds_own_source_pin_without_replacing_v1():
    bindings=prep.merged_bindings({'source_bindings':{}})
    assert bindings['tools/prepare_rolling_specialists_v1.py']==prep.LEGACY_SHA256
    assert bindings['tools/prepare_rolling_specialists_v2.py']==prep.file_sha(prep.ROOT/'tools/prepare_rolling_specialists_v2.py')


def test_failed_68_pair_preflight_creates_nothing(tmp_path):
    folders={name:tmp_path/name for name in ('base','prepared','quotes')}
    for path in folders.values():path.mkdir()
    for name,filename in [('base','DATASET.json'),('prepared','PREPARED.json'),('quotes','QUOTE_PANEL.json')]:
        (folders[name]/filename).write_text(json.dumps({'status':'complete','pairs':{'EUR_USD':{}},'partitions':[]}))
    output=tmp_path/'output'
    with pytest.raises(ValueError,match='all68'):prep.run(argparse.Namespace(**folders,output=output))
    assert not output.exists()


def test_new_npz_readback_keeps_unsupported_masks_and_signed_zero(tmp_path):
    payload=prep.prepare_pair_values(**example());p=tmp_path/'EUR_USD.npz'
    result=prep.write_payload(p,payload)
    assert result['rows']==len(payload['time'])
    with np.load(p,allow_pickle=False) as restored:
        assert all(prep.exact_arrays(restored[k],payload[k]) for k in payload)
    with pytest.raises(ValueError,match='new_pair'):prep.write_payload(p,payload)


def test_complete_68_pair_earlier_window_orchestration(tmp_path,monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    e=example('2026-05-18');names=[f'm1__field_{i}' for i in range(38)]+[f'peer__field_{i}' for i in range(12)]
    roots={key:tmp_path/key for key in ('base','prepared','quotes')}
    for root in roots.values():root.mkdir()
    pairs=[f'PAIR_{i:02d}' for i in range(68)];parts=[];prepared_pairs={};quote_pairs={}
    for pair in pairs:
        pp=roots['prepared']/(pair+'.npz')
        np.savez_compressed(pp,time=e['retained_times'],split=e['retained_split'],x=e['old_x'])
        prepared_pairs[pair]={'path':pp.name,'sha256':prep.file_sha(pp),'rows':len(e['retained_times']),
                              'key_sha256':prep.key_hash(e['retained_times'])}
        qp=roots['quotes']/(pair+'.parquet')
        pq.write_table(pa.table({'instrument':[pair]*len(e['raw_times']),'bar_start_epoch':e['raw_times'],
            'quote__mid_close':e['mid'],'quote__bid_close':e['bid'],'quote__ask_close':e['ask']}),qp)
        receipt=roots['quotes']/(pair+'.json');receipt.write_text('{}')
        quote_pairs[pair]={'path':qp.name,'sha256':prep.file_sha(qp),'rows':len(e['raw_times']),
                          'key_sha256':prep.key_hash(e['raw_times']),'receipt_path':receipt.name,'receipt_sha256':prep.file_sha(receipt)}
        for block in range(8):parts.append({'pair':pair,'block':block,'core':{'rows':28},'peers':{'rows':28}})
    bm={'status':'complete','pairs':{p:{'origin_rows':len(e['raw_times'])} for p in pairs},'partitions':parts,
        'origin_rows':68*len(e['raw_times']),'feature_names':names,'boundaries':e['boundaries'],'source_bindings':{}}
    bp=roots['base']/'DATASET.json';bp.write_text(json.dumps(bm));base_sha=prep.file_sha(bp)
    pm={'status':'complete','pairs':prepared_pairs,'base_sha256':base_sha,'overlay_sha256':'same-overlay',
        'boundaries':e['boundaries'],'groups':{'compact38':names[:38],'compact50':names},'feature_names':names,
        'rows':68*len(e['retained_times']),'source_bindings':{}}
    (roots['prepared']/'PREPARED.json').write_text(json.dumps(pm))
    qm={'status':'complete','pairs':quote_pairs,'base_manifest_sha256':base_sha,'endpoint_manifest_sha256':'same-overlay',
        'boundaries':e['boundaries'],'source_bindings':{}}
    (roots['quotes']/'QUOTE_PANEL.json').write_text(json.dumps(qm))
    def tables(_root,*,feature_names):
        assert feature_names==names
        for pair in pairs:
            for block in range(8):
                sl=slice(block*28,(block+1)*28);t=e['raw_times'][sl]
                columns={'instrument':[pair]*28,'bar_start_epoch':t,'bar_end_epoch':t+60}
                columns.update({n:e['raw_x'][sl,i] for i,n in enumerate(names)})
                yield pair,pa.table(columns)
    monkeypatch.setattr(prep,'iter_partitions',tables)
    output=tmp_path/'new_output'
    monkeypatch.setattr(prep,'validate_destination',lambda path:Path(path))
    report=prep.run(argparse.Namespace(**roots,output=output))
    assert report['status']=='complete' and len(report['pairs'])==68
    assert report['rows']==pm['rows'] and report['raw_origin_rows']==bm['origin_rows']
    assert report['period_identity']=='20260518_20260713'
    assert report['schema']==prep.SCHEMA and report['feature_family_ablation_applied'] is False
    assert report['raw_partitions_verified']==544 and report['partition_coverage']['missing_pair_weeks']==0
    assert report['fold_cutoffs_epoch'][0]==e['boundaries']['start']+2*prep.WEEK
    for pair,record in report['pairs'].items():
        assert record['training_clock_rows']==84 and record['validation_rows']==28 and record['later_development_test_rows']==28
        assert record['prefix_original_rows_by_cutoff']==[56,84,112,140,168]
        assert prep.file_sha(output/record['path'])==record['sha256']
