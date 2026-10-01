"""Observed-time adapter for the preserved fixed currency projection.

Uses original algebra, graph policy and units. No fitting, outcome inputs, data
acquisition or order actions. Fixed half-residual is not the learned residual.
"""
import hashlib
import importlib
import importlib.machinery
import importlib.util
import json
import math
from pathlib import Path
import sys
import time

KIND = 'retained_currency_projection'
VARIANTS = ('currency_projection', 'half_residual')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def load_original(root):
    """Isolate imports by source root, including relocated numerical replicas."""
    source = Path(root).resolve() / 'stage_c_alignment_integrity_v2'
    prefix = '_retained_projection_' + hashlib.sha256(str(source).encode()).hexdigest()[:16]
    if prefix not in sys.modules:
        spec = importlib.machinery.ModuleSpec(prefix, loader=None, is_package=True)
        module = importlib.util.module_from_spec(spec)
        module.__path__ = [str(source)]
        sys.modules[prefix] = module
    module = importlib.import_module(prefix + '.currency_projection_v2')
    if Path(module.__file__).resolve() != source / 'currency_projection_v2.py':
        raise ValueError('projection_source_origin')
    return module


def stable_parent(f):
    # Repeated issuance of unchanged inputs must not create another observation.
    return {k: f[k] for k in ('instrument', 'connection', 'horizon_minutes',
        'reference_epoch', 'target_epoch', 'original_model_id', 'feature_hash',
        'input_hash', 'panel_sha256', 'expected_return_bps', 'reference_mid')}


class Projection:
    def __init__(self, root, registry):
        self.root = Path(root)
        self.registry = registry
        config = registry['projection']
        path = self.root / 'stage_c_alignment_integrity_v2/CURRENCY_PROJECTION_CONTRACT_V2.json'
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != config['original_contract_sha256']:
            raise ValueError('projection_contract_identity')
        self.contract = json.loads(raw)
        if registry['pairs'] != self.contract['universe']:
            raise ValueError('projection_exact_universe')
        self.original = load_original(self.root)
        solver_root = self.root / 'trad/src/forex_system'
        pins = {n: {'sha256': h, 'bytes': (solver_root/n).stat().st_size}
                for n, h in self.contract['solver_source_hashes'].items()}
        self.solver = self.original.load_solver(solver_root, pins)
        entries = {e['id']: e for e in registry['connections']}
        self.parents = {}
        for parent_id, identity in config['parents'].items():
            e = entries[parent_id]
            if (e['kind'] != 'legacy26_matched' or e['horizon_minutes'] not in (360, 1080)
                    or e['original_model_id'] != identity['original_model_id']
                    or e['fit_metadata'] != identity['fit_metadata']
                    or e['arm'] != identity['arm']
                    or json.loads((self.root/e['fit_metadata']['path']).read_bytes())['fit_id'] != identity['fit_id']):
                raise ValueError('projection_exact_parent')
            self.parents[parent_id] = e
        expected = {(h, a) for h in (360, 1080) for a in self.original.BASES}
        if {(e['horizon_minutes'], e['arm']) for e in self.parents.values()} != expected or len(self.parents) != 4:
            raise ValueError('projection_parent_inventory')
        self.entries = [e for e in entries.values() if e['kind'] == KIND]
        if len(self.entries) != 8 or {(e['parent'], e['variant']) for e in self.entries} != {(p, v) for p in self.parents for v in VARIANTS}:
            raise ValueError('projection_variant_inventory')
        for e in self.entries:
            p = self.parents[e['parent']]
            if e['models'] or e['horizon_minutes'] != p['horizon_minutes'] or e['original_model_id'] != p['original_model_id']:
                raise ValueError('projection_variant_target')

    def append(self, forecasts, coverage, registry_sha256, *, clock=time.time):
        start = clock()
        result, slots, diagnostics = [], [], []
        selected = [f for f in forecasts if f['connection'] in self.parents]
        for h in (360, 1080):
            entries = [e for e in self.entries if e['horizon_minutes'] == h]
            parents = [f for f in selected if f['horizon_minutes'] == h]
            origin = max((f['reference_epoch'] for f in parents), default=None)
            lookup = {}; failure = None
            try:
                for f in parents:
                    e = self.parents[f['connection']]
                    key = (f['instrument'], e['arm'])
                    if (key in lookup or f['instrument'] not in self.registry['pairs']
                            or f['original_model_id'] != e['original_model_id']
                            or f['registry_sha256'] != registry_sha256
                            or type(f['reference_epoch']) is not int
                            or f['target_epoch'] != f['reference_epoch'] + h*60
                            or not f['reference_epoch'] < f['issued_epoch'] <= start < f['target_epoch']
                            or not 0 <= start-f['reference_epoch'] <= 180
                            or f['can_place_orders'] is not False or f['can_promote'] is not False
                            or f['models_fitted'] != 0 or f['research_only'] is not True
                            or not math.isfinite(f['expected_return_bps']) or f['expected_return_bps'] <= -10000
                            or not math.isfinite(f['reference_mid']) or f['reference_mid'] <= 0):
                        raise ValueError('projection_parent_identity_clock_or_value')
                    lookup[key] = f
                frame = {'origin_epoch': origin, 'horizon_minutes': h, 'reserved_ready_epoch': start,
                         'predictions': [], 'coverage': []}
                for pair in self.registry['pairs']:
                    for base in self.original.BASES:
                        f = lookup.get((pair, base))
                        eligible = f is not None and f['reference_epoch'] == origin
                        frame['coverage'].append({'instrument': pair, 'base_method': base,
                            'variant': 'raw_unrestricted', 'reason': 'eligible' if eligible else 'base_unavailable'})
                        if not eligible:
                            continue
                        p = self.parents[f['connection']]
                        row = {'instrument': pair, 'record_id': pair+':'+str(origin),
                            'base_method': base, 'variant': 'raw_unrestricted', 'origin_epoch': origin,
                            'decision_epoch': origin, 'horizon_minutes': h, 'target_epoch': f['target_epoch'],
                            'target_id': f'technical_endpoint_midpoint_elapsed_{h}m',
                            'available_epoch': f['issued_epoch'], 'prediction_bps': f['expected_return_bps'],
                            'model_id': f['original_model_id'], 'signed_fit_id': self.registry['projection']['parents'][p['id']]['fit_id'],
                            'observed_parent_sha256': digest(stable_parent(f))}
                        row['forecast_id'] = self.original.fingerprint(row)
                        frame['predictions'].append(row)
                contract = {**self.contract, 'origins': [origin]}
                projected = self.original.project_frame(frame, contract, self.solver) if origin is not None else None
                issued = clock()
                if issued < start or (origin is not None and issued-origin > 180):
                    raise ValueError('projection_publication_clock_or_expiry')
            except Exception as exc:
                failure = type(exc).__name__+':'+str(exc)[:160]
                projected = None
                issued = clock()
            values = {(p['instrument'], p['base_method'], p['variant']): p
                      for p in projected['predictions']} if projected else {}
            reasons = {(p['instrument'], p['method']): p['reason'] for p in projected['coverage']} if projected else {}
            if projected:
                diagnostics.append({'horizon_minutes': h, 'reference_epoch': origin,
                    'actual_completed_epoch': issued, 'original_modeled_clock_is_not_actual_publication': True,
                    'solver': projected['solver_diagnostics']})
            elif failure:
                diagnostics.append({'horizon_minutes': h, 'reason': failure})
            for e in entries:
                base = self.parents[e['parent']]['arm']
                aligned = [] if failure else [stable_parent(f) for (pair, b), f in sorted(lookup.items())
                           if b == base and f['reference_epoch'] == origin]
                inputs = digest(aligned)
                for pair in self.registry['pairs']:
                    f = lookup.get((pair, base)); p = values.get((pair, base, e['variant']))
                    reason = ('projection_inference_unavailable' if failure else
                              'parent_origin_mismatch' if f and f['reference_epoch'] != origin else
                              reasons.get((pair, base+'__'+e['variant']), 'base_unavailable'))
                    slot = {'instrument': pair, 'connection': e['id'], 'horizon_minutes': h,
                        'status': reason, 'input_support': {'finite': len(aligned), 'expected': 68,
                        'scope': 'exact-origin original forecast edges, fixed graph support required'}}
                    if failure: slot['reason'] = failure
                    slots.append(slot)
                    if p is None or reason != 'eligible':
                        continue
                    result.append({**f, 'connection': e['id'], 'issued_epoch': issued,
                        'expected_return_bps': p['prediction_bps'], 'selection_scope': e['selection_scope'],
                        'input_hash': inputs, 'panel_sha256': p['layer_definition_sha256'],
                        'input_support': slot['input_support'], 'projection': {
                            'variant': e['variant'], 'parent_connection': e['parent'],
                            'parent_forecast_sha256': digest(stable_parent(f)),
                            'parent_input_hash': f['input_hash'], 'constituent_input_sha256': inputs,
                            'source_frame_sha256': projected['source_frame_sha256'],
                            'modeled_available_epoch': p['available_epoch'],
                            'actual_completed_epoch': issued, 'learned_residual': False}})
        forecasts.extend(result); coverage.extend(slots)
        return diagnostics
