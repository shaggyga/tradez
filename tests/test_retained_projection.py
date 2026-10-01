import copy
import hashlib
import json
from pathlib import Path
import shutil
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'trad'))
import oanda_retained_projection_v1 as p
import oanda_retained_forecast_tracking_v1 as tracking


def setup(tmp_path):
    for n in ('stage_c_alignment_integrity_v2/contracts.py',
              'stage_c_alignment_integrity_v2/currency_projection_v2.py',
              'stage_c_alignment_integrity_v2/CURRENCY_PROJECTION_CONTRACT_V2.json',
              'trad/src/forex_system/contracts/currency_state.py',
              'trad/src/forex_system/features/currency_state_engine.py'):
        target = tmp_path/n; target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT/n, target)
    path = tmp_path/'stage_c_alignment_integrity_v2/CURRENCY_PROJECTION_CONTRACT_V2.json'
    c = json.loads(path.read_bytes())
    reg = {'pairs': c['universe'], 'connections': [], 'projection': {
        'original_contract_sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'parents': {}}}
    for h in (360, 1080):
        for arm in ('ridge', 'recovered_hgb'):
            name = arm+str(h)
            (tmp_path/name).write_text(json.dumps({'fit_id': name}), encoding='utf-8')
            e = {'id': name, 'kind': 'legacy26_matched', 'arm': arm, 'horizon_minutes': h,
                 'original_model_id': name, 'fit_metadata': {'path': name, 'sha256': 'fixture'}}
            reg['connections'].append(e)
            reg['projection']['parents'][name] = {k:e[k] for k in ('arm', 'original_model_id', 'fit_metadata')}
            reg['projection']['parents'][name]['fit_id'] = name
            for variant in p.VARIANTS:
                reg['connections'].append({'id': name+'__'+variant, 'kind': p.KIND, 'parent': name,
                    'variant': variant, 'horizon_minutes': h, 'original_model_id': name,
                    'models': [], 'selection_scope': 'synthetic'})
    forecasts = []
    for e in reg['connections']:
        if e['kind'] != 'legacy26_matched': continue
        for i, pair in enumerate(reg['pairs']):
            forecasts.append({'instrument': pair, 'connection': e['id'], 'horizon_minutes': e['horizon_minutes'],
                'reference_epoch': 1020, 'target_epoch': 1020+e['horizon_minutes']*60,
                'issued_epoch': 1021., 'original_model_id': e['original_model_id'],
                'expected_return_bps': (i-30)*.3, 'reference_mid': 1.1,
                'registry_sha256': 'fixture', 'feature_hash': 'feature'+pair, 'input_hash': 'input'+pair,
                'panel_sha256': None, 'can_place_orders': False, 'can_promote': False,
                'research_only': True, 'models_fitted': 0})
    return reg, forecasts


def run(tmp_path, mutate=None, clock=lambda:1022.):
    reg, forecasts = setup(tmp_path)
    if mutate: mutate(reg, forecasts)
    adapter = p.Projection(tmp_path, reg)
    original = copy.deepcopy(forecasts); coverage = []
    diag = adapter.append(forecasts, coverage, 'fixture', clock=clock)
    return reg, original, forecasts[len(original):], coverage, diag


def test_original_algebra_and_observed_clocks(tmp_path):
    reg, original, derived, slots, diag = run(tmp_path)
    assert len(original) == 272 and len(derived) == 544 and len(slots) == 544
    assert all(f['issued_epoch'] == 1022. for f in derived)
    assert all(f['projection']['modeled_available_epoch'] == 1022. for f in derived)
    assert all(not f['can_place_orders'] and not f['projection']['learned_residual'] for f in derived)
    assert all(s['status'] == 'eligible' for s in slots)
    import math
    by = {(f['connection'], f['instrument']): f['expected_return_bps'] for f in derived}
    for f in original:
        factor = math.log1p(by[f['connection']+'__currency_projection', f['instrument']]/10000)
        half = math.log1p(by[f['connection']+'__half_residual', f['instrument']]/10000)
        assert abs(half-(factor+math.log1p(f['expected_return_bps']/10000))/2) < 1e-12


@pytest.mark.parametrize('field,value', [('original_model_id','wrong'), ('target_epoch',1234),
    ('issued_epoch',2000), ('expected_return_bps',float('nan')), ('reference_mid',0),
    ('registry_sha256','wrong'), ('can_place_orders',True)])
def test_bad_parent_refuses_affected_horizon_only(tmp_path, field, value):
    _, original, derived, slots, diag = run(tmp_path, lambda r,f:f[0].update({field:value}))
    assert len(derived) == 272 and all(f['horizon_minutes']==1080 for f in derived)
    assert sum(s['status']=='projection_inference_unavailable' for s in slots)==272


def test_origin_mismatch_never_combines_minutes(tmp_path):
    def change(reg, fs):
        fs[0]['reference_epoch'] -= 60; fs[0]['target_epoch'] -= 60
    _, _, derived, slots, _ = run(tmp_path, change)
    assert len(derived)==542 and sum(s['status']=='parent_origin_mismatch' for s in slots)==2


def test_undersupported_graph_has_coverage_without_fabricated_values(tmp_path):
    def change(reg, fs): fs[:] = [f for f in fs if f['instrument'] in ('EUR_USD','GBP_JPY')]
    _, _, derived, slots, _ = run(tmp_path, change)
    assert not derived and len(slots)==544
    assert sum(s['status']=='projection_component_unavailable' for s in slots)==16


def test_empty_inputs_keep_all_slots(tmp_path):
    _, _, derived, slots, _ = run(tmp_path, lambda r,f:f.clear())
    assert not derived and len(slots)==544 and all(s['status']=='base_unavailable' for s in slots)


def test_expiry_during_projection_refuses_publication(tmp_path):
    times = iter([1022., 1201., 1201., 1201., 1201.])
    _, _, derived, slots, _ = run(tmp_path, clock=lambda:next(times))
    assert not derived and all(s['status']=='projection_inference_unavailable' for s in slots)


def test_identity_tracks_other_pair_inputs_but_not_repeated_issue_time(tmp_path):
    reg, fs = setup(tmp_path); adapter=p.Projection(tmp_path,reg)
    def derive(source):
        out=copy.deepcopy(source); adapter.append(out,[], 'fixture',clock=lambda:1023.)
        return out[len(source):]
    first=derive(fs)
    for f in fs:f['issued_epoch']+=.5
    repeated=derive(fs)
    assert [tracking.prediction_key(f) for f in first]==[tracking.prediction_key(f) for f in repeated]
    fs[0]['input_hash']='new authenticated history'
    changed=derive(fs)
    # Cross-pair provenance changes even where numerical parent values do not.
    assert tracking.prediction_key(first[1])!=tracking.prediction_key(changed[1])


@pytest.mark.parametrize('fault', ['parent','variant','policy','solver'])
def test_altered_dependencies_refused(tmp_path,fault):
    reg, _=setup(tmp_path)
    if fault=='parent':reg['connections'][0]['original_model_id']='other'
    elif fault=='variant':reg['connections'][1]['horizon_minutes']=1
    elif fault=='policy':
        target=tmp_path/'stage_c_alignment_integrity_v2/CURRENCY_PROJECTION_CONTRACT_V2.json'
        target.write_bytes(target.read_bytes()+b' ')
    else:
        target=tmp_path/'trad/src/forex_system/features/currency_state_engine.py'
        target.write_bytes(target.read_bytes()+b'\n')
    with pytest.raises(ValueError):p.Projection(tmp_path,reg)
