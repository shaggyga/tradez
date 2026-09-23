"""Fixed compact-family removals for separate historical base-head research.

Every matrix retains 50 technical slots followed by the two current known
entry costs and categorical pair ID. Removed technical slots become NaN;
columns are never shifted and the three context columns are never removed.
This is conditional contribution under one fixed representation/learner,
not causal importance or a claim that correlated families are independent.

Only final base heads and their direct/raw-mixture means belong to this grid.
The separate chronological OOF replication owns calibrated and contextual
meta arms. Keeping their old context inputs here would reintroduce removed
families and invalidate a whole-predictor removal claim.
"""
from __future__ import annotations

from functools import lru_cache
import hashlib
import json

import numpy as np

from oanda_rolling_model_design_v1 import COMPACT_LOCAL
from oanda_rolling_technical_features_v1 import feature_registry
from oanda_rolling_technical_panel_v1 import panel_registry


SCHEMA = 'rolling_family_design_v1_20260915'
TECHNICAL_WIDTH = 50
MATRIX_WIDTH = 53
PAIR_COUNT = 68
MEAN_ARMS = ('direct', 'mixture_raw')
CONTEXT_NAMES = ('known_entry_long_bps', 'known_entry_short_bps', 'pair_id')
GROUPS = (
    'full_compact50',
    'drop_peers',
    'drop_returns_path',
    'drop_trend_oscillator',
    'drop_range_volatility',
    'drop_activity_spread_history',
    'drop_calendar',
    'context_only',
)
_LOCAL_COUNTS = {
    'returns': 5, 'path': 5, 'ohlc': 3, 'volatility': 6,
    'oscillator': 3, 'trend': 5, 'activity': 4, 'spread': 3,
    'calendar': 4,
}
_REMOVALS = {
    'full_compact50': (),
    'drop_peers': ('currency_peer',),
    'drop_returns_path': ('returns', 'path'),
    'drop_trend_oscillator': ('trend', 'oscillator'),
    'drop_range_volatility': ('ohlc', 'volatility'),
    'drop_activity_spread_history': ('activity', 'spread'),
    'drop_calendar': ('calendar',),
    'context_only': tuple(_LOCAL_COUNTS) + ('currency_peer',),
}
_ACTIVE_COUNTS = dict(zip(GROUPS, (50, 38, 40, 42, 41, 43, 46, 0)))


@lru_cache(maxsize=1)
def _contract():
    local = feature_registry()
    peers = panel_registry()
    registry = local + peers
    by_name = {r['name']: r for r in registry}
    if len(by_name) != len(registry):
        raise ValueError('duplicate_registered_feature_name')
    names = tuple(COMPACT_LOCAL) + tuple(r['name'] for r in peers)
    if len(COMPACT_LOCAL) != 38 or len(peers) != 12 or len(set(names)) != 50:
        raise ValueError('exact_compact38_plus12_peer_contract_required')
    if not all(n in by_name for n in names):
        raise ValueError('canonical_feature_registry_binding_required')
    families = tuple(by_name[n]['family'] for n in names)
    counts = {f: families[:38].count(f) for f in set(families[:38])}
    if counts != _LOCAL_COUNTS or families[38:] != ('currency_peer',) * 12:
        raise ValueError('registered_compact_family_mapping_changed')
    # Canonical registry names only: aliases are lineage, not extra columns.
    lineage = tuple({
        'name': n,
        'family': by_name[n]['family'],
        'lookback_bars': int(by_name[n]['lookback_bars']),
        'aliases_not_added_as_inputs': list(by_name[n].get('aliases', ())),
    } for n in names)
    return names, families, lineage


def feature_names():
    """Fresh ordered list of the sealed compact38 plus twelve peer fields."""
    return list(_contract()[0])


def _checked_names(names):
    expected = _contract()[0]
    if names is None:
        return expected
    if isinstance(names, (str, bytes)):
        raise ValueError('exact_ordered_50_canonical_feature_names_required')
    try:
        actual = tuple(names)
    except TypeError as exc:
        raise ValueError('exact_ordered_50_canonical_feature_names_required') from exc
    if actual != expected:
        raise ValueError('exact_ordered_50_canonical_feature_names_required')
    return expected


def feature_mask(group, *, names=None):
    """True for retained technical slots; current costs/pair ID are separate."""
    _checked_names(names)
    if not isinstance(group, str) or group not in _REMOVALS:
        raise ValueError('undeclared_family_group')
    removed = _REMOVALS[group]
    keep = np.asarray([f not in removed for f in _contract()[1]], dtype=bool)
    if int(keep.sum()) != _ACTIVE_COUNTS[group]:
        raise ValueError('fixed_family_group_width_changed')
    return keep


def validate_mean_arm(arm):
    if arm not in MEAN_ARMS:
        raise ValueError('family_grid_is_base_direct_and_raw_mixture_only')
    return arm


def apply_group_mask(matrix53, group, *, names=None):
    """Return a copy, preserving original row order and fixed matrix width.

    Matrix input is already normalized using that historical TRAIN prefix.
    This function never fits statistics or consults outcomes/eligibility.
    Technical NaNs are retained. Infinite values and unavailable current
    costs/pair IDs fail rather than silently becoming usable context.
    """
    keep = feature_mask(group, names=names)
    x = np.asarray(matrix53)
    if x.ndim != 2 or x.shape[1] != MATRIX_WIDTH or x.dtype.kind != 'f':
        raise ValueError('floating_fixed_53_column_matrix_required')
    if np.any(np.isinf(x[:, :TECHNICAL_WIDTH])):
        raise ValueError('technical_infinity_refused')
    costs = x[:, TECHNICAL_WIDTH:TECHNICAL_WIDTH+2]
    ids = x[:, -1]
    if not np.isfinite(costs).all() or np.any(costs < 0):
        raise ValueError('finite_nonnegative_known_entry_costs_required')
    if (not np.isfinite(ids).all() or np.any(ids != np.floor(ids))
            or np.any(ids < 0) or np.any(ids >= PAIR_COUNT)):
        raise ValueError('fixed_68_pair_categorical_context_required')
    out = x.copy()
    out[:, np.flatnonzero(~keep)] = np.nan
    return out


def build_family_matrix(technical50, entry_long, entry_short, pair_id, group,
                        *, names=None):
    """Explicit input whitelist; future labels/metadata have no argument."""
    _checked_names(names)
    z = np.asarray(technical50)
    if z.ndim != 2 or z.shape[1] != TECHNICAL_WIDTH or z.dtype.kind != 'f':
        raise ValueError('floating_exact_50_technical_columns_required')
    values = [np.asarray(v) for v in (entry_long, entry_short, pair_id)]
    if any(v.shape != (len(z),) or v.dtype.kind not in 'iuf' for v in values):
        raise ValueError('same_origin_numeric_costs_and_pair_ids_required')
    # Match the retained specialist construction: float64 quote context is
    # not downcast merely because the normalized technical values are float32.
    return apply_group_mask(np.column_stack((z, *values)), group, names=names)


def group_manifest(names=None):
    """JSON-safe exact family and column mappings for the new run's pins."""
    ordered = _checked_names(names)
    families = _contract()[1]
    groups = {}
    for group in GROUPS:
        keep = feature_mask(group)
        groups[group] = {
            'kept_technical_fields': _ACTIVE_COUNTS[group],
            'removed_families': list(_REMOVALS[group]),
            'kept_names': [n for i, n in enumerate(ordered) if keep[i]],
            'removed_names': [n for i, n in enumerate(ordered) if not keep[i]],
            'kept_indices': np.flatnonzero(keep).tolist(),
            'removed_indices': np.flatnonzero(~keep).tolist(),
            'fixed_matrix_columns': MATRIX_WIDTH,
            'mask_value': 'NaN',
        }
    feature_family_map = dict(zip(ordered, families))
    digest = hashlib.sha256(json.dumps(feature_family_map, sort_keys=True,
                                      separators=(',', ':')).encode()).hexdigest()
    return {
        'schema': SCHEMA,
        'feature_names': list(ordered),
        'feature_family_map': feature_family_map,
        'feature_family_map_sha256': digest,
        'lineage': json.loads(json.dumps(_contract()[2])),
        'groups': groups,
        'context_columns': {name: TECHNICAL_WIDTH+i
                            for i, name in enumerate(CONTEXT_NAMES)},
        'context_preserved_for_every_group': True,
        'pair_categorical_index': MATRIX_WIDTH-1,
        'mean_arms': list(MEAN_ARMS),
        'head_scope': 'final base six-head refits; no calibrated or contextual meta arms',
        'mask_scope': 'same technical mask for every base head; no other technical context path',
        'normalization_scope': 'caller supplies TRAIN-prefix-only normalized values; no fitting here',
        'row_scope': 'original row order unchanged; no future-label or feature-completeness row filter',
        'cost_scope': 'known entry costs retained; outcome costs and decision gates are not modified',
        'drop_peers_scope': 'compact38 field values in original 53 slots; not the old 41-column estimator layout',
        'drop_spread_scope': 'remove registered spread features; current known quote costs remain',
        'dependency_limit': 'families can encode related information; removal is conditional on retained fields',
        'peer_limit': 'four difference fields are derived from base/quote means, not twelve independent sources',
        'aliases_added': 0,
        'can_place_orders': False,
    }
