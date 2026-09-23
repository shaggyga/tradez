import argparse
import json
from pathlib import Path

import numpy as np
import pytest

from tools import prepare_rolling_specialists_v1 as prep


def example():
    start = 1800000000
    times = start + np.arange(150, dtype=np.int64) * 60
    cutoffs = (start + 60 * 60, start + 90 * 60, start + 120 * 60)
    raw = np.column_stack((np.sin(np.arange(150) / 7), np.full(150, .1), np.arange(150, dtype=float)))
    raw[:50, 2] = np.nan
    keep = (times % 900 == 0) | (times >= cutoffs[-1])
    t = times[keep]
    split = np.where(t < cutoffs[-1], 0, 1).astype(np.int8)
    old = prep.transform_inputs(raw[keep], prep.fit_normalizer(raw[times < cutoffs[-1]]))
    mid = 1.1 + np.arange(150) * 1e-6
    return dict(raw_times=times, raw_x=raw, retained_times=t, retained_split=split, old_x=old,
                quote_times=times, mid=mid, bid=mid - .0001, ask=mid + .0002, cutoffs=cutoffs, start=start)


def test_prefix_uses_all_real_origins_and_final_transform_is_exact():
    e = example()
    p = prep.prepare_pair_values(**e)
    assert p['raw_x'].dtype == np.float64
    assert p['normalizer_count'].shape == (3, 3)
    assert p['normalizer_count'][:, 0].tolist() == [60, 90, 120]
    assert np.isnan(p['raw_x'][0, 2])
    assert p['normalizer_supported'][:, 1].tolist() == [False, False, False]
    assert p['normalizer_supported'][:, 2].tolist() == [False, True, True]
    params = {name: p['normalizer_' + name][-1] for name in ('count', 'mean', 'scale', 'supported')}
    assert prep.exact_arrays(prep.transform_inputs(p['raw_x'], params), e['old_x'])
    assert (p['known_entry_long_bps'] > p['known_entry_short_bps']).all()
    assert not any(name.startswith('label') or name.startswith('y_') for name in p)


def test_future_raw_mutation_does_not_change_earlier_normalizer():
    original = example()
    before = prep.prepare_pair_values(**original)
    changed = {key: value.copy() if isinstance(value, np.ndarray) else value for key, value in original.items()}
    changed['raw_x'][changed['raw_times'] >= changed['cutoffs'][0]] += 200
    keep = np.searchsorted(changed['raw_times'], changed['retained_times'])
    final = prep.fit_normalizer(changed['raw_x'][changed['raw_times'] < changed['cutoffs'][-1]])
    changed['old_x'] = prep.transform_inputs(changed['raw_x'][keep], final)
    after = prep.prepare_pair_values(**changed)
    for name in ('count', 'mean', 'scale', 'supported'):
        assert prep.exact_arrays(before['normalizer_' + name][0], after['normalizer_' + name][0])
    assert not prep.exact_arrays(before['normalizer_mean'][-1], after['normalizer_mean'][-1])


def test_wrong_original_keys_and_raw_dtype_are_rejected():
    e = example()
    e['quote_times'] = e['quote_times'][::-1]
    with pytest.raises(ValueError, match='ascending'):
        prep.prepare_pair_values(**e)
    e = example()
    e['retained_times'][-1] += 60
    with pytest.raises(ValueError, match='every_retained'):
        prep.prepare_pair_values(**e)
    e = example()
    e['raw_x'] = e['raw_x'].astype(np.float32)
    with pytest.raises(ValueError, match='raw_float64'):
        prep.prepare_pair_values(**e)


def test_accepted_final_transform_cannot_be_approximately_substituted():
    e = example()
    e['old_x'][0, 0] = np.nextafter(e['old_x'][0, 0], np.float32(np.inf))
    with pytest.raises(ValueError, match='final_transform'):
        prep.prepare_pair_values(**e)


def test_exact_npz_readback_preserves_signed_zero_nan_and_refuses_overwrite(tmp_path):
    payload = {'time': np.array([60, 120], dtype=np.int64), 'raw_x': np.array([[0., -0.], [np.nan, 1.]]),
               'split': np.array([0, 1], dtype=np.int8)}
    path = tmp_path / 'pair.npz'
    record = prep.write_payload(path, payload)
    assert record['rows'] == 2 and record['sha256'] == prep.file_sha(path)
    with np.load(path, allow_pickle=False) as a:
        assert prep.exact_arrays(a['raw_x'], payload['raw_x'])
    assert not prep.exact_arrays(np.array([0.]), np.array([-0.]))
    with pytest.raises(ValueError, match='new_pair'):
        prep.write_payload(path, payload)


def test_source_hash_guards_preserve_inherited_pins(tmp_path, monkeypatch):
    monkeypatch.setattr(prep, 'ROOT', tmp_path)
    (tmp_path / 'tools').mkdir()
    for name in ('tools/prepare_rolling_specialists_v1.py', 'oanda_rolling_model_design_v1.py', 'oanda_rolling_technical_dataset_v1.py'):
        (tmp_path / name).write_text('saved bytes', encoding='utf-8')
    name = 'oanda_rolling_model_design_v1.py'
    expected = prep.file_sha(tmp_path / name)
    bindings = prep.merged_bindings({'source_bindings': {name: expected}})
    assert bindings[name] == expected
    with pytest.raises(ValueError, match='incompatible_inherited'):
        prep.merged_bindings({'source_bindings': {name: expected}}, {'source_bindings': {name: 'different'}})
    (tmp_path / name).write_text('changed', encoding='utf-8')
    with pytest.raises(ValueError, match='inherited_source_changed'):
        prep.merged_bindings({'source_bindings': {name: expected}})


def test_destination_must_be_new_and_in_project_data_with_reserve(tmp_path, monkeypatch):
    monkeypatch.setattr(prep, 'ROOT', tmp_path)
    (tmp_path / 'data').mkdir()
    (tmp_path / 'data/existing').mkdir()
    with pytest.raises(ValueError, match='new_independent'):
        prep.validate_destination(tmp_path / 'data/existing')
    with pytest.raises(ValueError, match='new_independent'):
        prep.validate_destination(tmp_path / 'outside')
    class Usage:
        free = prep.MIN_FREE_BYTES + prep.MAX_BYTES - 1
    monkeypatch.setattr(prep.shutil, 'disk_usage', lambda _: Usage())
    with pytest.raises(ValueError, match='reserve'):
        prep.validate_destination(tmp_path / 'data/new')
    Usage.free += 1
    assert prep.validate_destination(tmp_path / 'data/new') == tmp_path / 'data/new'


def test_all68_population_required_before_output_creation(tmp_path):
    folders = [tmp_path / name for name in ('base', 'prepared', 'quotes')]
    for folder in folders:
        folder.mkdir()
    (folders[0] / 'DATASET.json').write_text(json.dumps({'status': 'complete', 'pairs': {'EUR_USD': {}}, 'partitions': []}))
    (folders[1] / 'PREPARED.json').write_text(json.dumps({'status': 'complete'}))
    (folders[2] / 'QUOTE_PANEL.json').write_text(json.dumps({'status': 'complete'}))
    output = tmp_path / 'not_created'
    args = argparse.Namespace(base=folders[0], prepared=folders[1], quotes=folders[2], output=output)
    with pytest.raises(ValueError, match='all68'):
        prep.run(args)
    assert not output.exists()


def test_current_entry_costs_never_use_terminal_or_negative_midpoint_adjustments():
    e = example()
    e['mid'][0] = e['ask'][0] + .1
    with pytest.raises(ValueError, match='inside_bidask'):
        prep.prepare_pair_values(**e)
