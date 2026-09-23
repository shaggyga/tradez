"""Endpoint-only retrospective labels, separate from immutable strict paths.

Valid exact origin/target quotes suffice for fixed-horizon returns and spread
proxies. Missing intermediate candles do not establish continuous tradability.
Every input origin remains present; future validity is never an entry feature.
"""
from __future__ import annotations

from collections import OrderedDict
import json
from pathlib import Path

import numpy as np

import oanda_rolling_technical_labels_v1 as strict
from oanda_rolling_technical_dataset_v1 import file_sha, key_hash

SCHEMA = "rolling_technical_endpoint_overlay_v1_20260915"
DEFAULT_HORIZONS = (5, 15, 30, 60)
NUMERIC_FIELDS = ("return_bps", "absolute_return_bps", "direction", "long_net_bps", "short_net_bps")
FIELDS = ("target_bar_start_epoch", "target_bar_end_epoch", "assumed_available_epoch",
          "state", "target_present", "midpoint_valid", "bidask_endpoint_valid", *NUMERIC_FIELDS)


def compute_endpoint_outcomes(data, horizons=DEFAULT_HORIZONS, *, coverage_end_epoch=None):
    """Registry-ordered arrays for every supplied origin; never impute quotes.

    The caller's exclusive scanned coverage distinguishes absent targets from
    targets beyond the bounded read. Completion is a historical bar-end
    assumption, not evidence of original receipt, execution or publication.
    """
    horizons = strict._horizons(horizons)
    source = strict._normalize(data)
    times, close, bid, ask = (source[n] for n in ("time", "close", "bid_close", "ask_close"))
    n = len(times)
    if coverage_end_epoch is None:
        end = int(times[-1])+60 if n else 0
    else:
        if (isinstance(coverage_end_epoch, (bool, np.bool_)) or
                not isinstance(coverage_end_epoch, (int, float, np.integer, np.floating)) or
                not np.isfinite(coverage_end_epoch) or coverage_end_epoch < 0 or
                coverage_end_epoch > 253402300800 or coverage_end_epoch % 60):
            raise ValueError("aligned_finite_exclusive_coverage_end_epoch_required")
        end = int(coverage_end_epoch)
        if n and end < int(times[-1])+60:
            raise ValueError("coverage_end_precedes_last_completed_candle")
    result = OrderedDict()
    for h in horizons:
        target = times+h*60
        indexes = np.searchsorted(times, target)
        safe = np.minimum(indexes, max(0, n-1))
        present = (indexes < n) & (times[safe] == target)
        pending = target >= end
        state = np.full(n, "missing_target", dtype=object)
        state[pending] = "pending_right_edge"
        state[present] = "available"
        mid_valid = present & np.isfinite(close) & np.isfinite(close[safe])
        net_valid = present & np.isfinite(close) & np.isfinite(bid) & np.isfinite(ask) & np.isfinite(bid[safe]) & np.isfinite(ask[safe])
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            ret = (close[safe]/close-1.)*10000.
            long_net = (bid[safe]-ask)/close*10000.
            short_net = (bid-ask[safe])/close*10000.
        mid_valid &= np.isfinite(ret)
        net_valid &= np.isfinite(long_net) & np.isfinite(short_net)
        ret[~mid_valid] = np.nan
        long_net[~net_valid] = np.nan
        short_net[~net_valid] = np.nan
        values = {"target_bar_start_epoch": target, "target_bar_end_epoch": target+60,
            "assumed_available_epoch": target+60, "state": state, "target_present": present,
            "midpoint_valid": mid_valid, "bidask_endpoint_valid": net_valid,
            "return_bps": ret, "absolute_return_bps": np.abs(ret), "direction": np.sign(ret),
            "long_net_bps": long_net, "short_net_bps": short_net}
        for field in FIELDS:
            result[f"endpoint_label__{h}m__{field}"] = values[field]
    return result


def endpoint_registry(horizons=DEFAULT_HORIZONS, *, include_split_eligibility=False):
    formulas = {"target_bar_start_epoch": "origin_start+horizon_minutes*60",
        "target_bar_end_epoch": "target_start+60", "assumed_available_epoch": "target_bar_end; historical assumption only",
        "state": "exact target present: available; otherwise missing_target inside scanned range or pending_right_edge outside it",
        "target_present": "exact origin_start+horizon_minutes*60 exists",
        "midpoint_valid": "exact target and finite positive midpoint endpoints yield finite return",
        "bidask_endpoint_valid": "exact target plus noncrossed positive origin/target quotes yield finite net proxies",
        "return_bps": "10000*(mid_target/mid_origin-1)", "absolute_return_bps": "abs(return_bps)",
        "direction": "sign(return_bps); flat=0, missing=NaN",
        "long_net_bps": "10000*(bid_target-ask_origin)/mid_origin",
        "short_net_bps": "10000*(bid_origin-ask_target)/mid_origin"}
    result = []
    for h in strict._horizons(horizons):
        for field in FIELDS:
            result.append({"name": f"endpoint_label__{h}m__{field}", "horizon_minutes": h,
                "role": "label" if field in NUMERIC_FIELDS else "label_metadata",
                "model_input": False, "future_information": True, "formula": formulas[field],
                "intermediate_path_required": False,
                "scope": "fixed exact candle-close endpoint proxy; no fills, slippage, financing or continuous-tradability claim"})
    if include_split_eligibility:
        for h in strict._horizons(horizons):
            result.append({"name": f"endpoint_label__{h}m__split_eligible", "horizon_minutes": h,
                "role": "label_filter", "model_input": False, "future_information": True,
                "formula": "copied and checked against base: target_bar_end strictly before origin split boundary; numerical validity remains separate"})
    return result


def checked_path(root, relative, expected_sha=None):
    root = Path(root).resolve()
    path = (root/relative).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError("contained_existing_artifact_required")
    if expected_sha is not None and file_sha(path) != expected_sha:
        raise ValueError("artifact_hash_mismatch")
    return path


def iter_endpoint_partitions(base_root, overlay_root, *, split=None, feature_names=None,
                             endpoint_names=None):
    """Join exact sidecar keys; do not filter any rows by later label validity.

    This yields metadata, registered features, strict labels and selected
    endpoint labels. Fitting must explicitly select the BASE feature_names;
    the endpoint namespace is labels only and cannot be selected as inputs.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    from oanda_rolling_technical_dataset_v1 import iter_partitions, SPLITS
    base_root, overlay_root = Path(base_root).resolve(), Path(overlay_root).resolve()
    manifest_path = overlay_root/'ENDPOINT_DATASET.json'
    overlay_sha = file_sha(manifest_path)
    overlay = json.loads(manifest_path.read_bytes())
    if overlay.get('schema') != SCHEMA or overlay.get('status') != 'complete':
        raise ValueError('complete_endpoint_overlay_required')
    if file_sha(base_root/'DATASET.json') != overlay['base_manifest_sha256']:
        raise ValueError('base_manifest_hash_mismatch')
    base = json.loads((base_root/'DATASET.json').read_bytes())
    names = overlay['label_names'] if endpoint_names is None else list(endpoint_names)
    if (len(names) != len(set(names)) or not set(names) <= set(overlay['label_names']) or
            any(not n.startswith('endpoint_label__') for n in names)):
        raise ValueError('only_registered_endpoint_labels_allowed')
    if split is not None and split not in SPLITS:
        raise ValueError('unknown_development_split')
    by_key = {(r['pair'],r['block']):r for r in overlay['partitions']}
    expected = {(r['pair'],r['block']) for r in base['partitions']}
    if len(by_key) != len(overlay['partitions']) or set(by_key) != expected:
        raise ValueError('complete_one_to_one_overlay_partition_map_required')
    iterator = iter_partitions(base_root, feature_names=feature_names)
    for record, (pair, table) in zip(base['partitions'], iterator, strict=True):
        supplement = by_key[(record['pair'],record['block'])]
        if (pair != record['pair'] or supplement['base_core_sha256'] != record['core']['sha256'] or
                supplement['base_peer_sha256'] != record['peers']['sha256']):
            raise ValueError('base_partition_identity_mismatch')
        path = checked_path(overlay_root, supplement['path'], supplement['sha256'])
        endpoints = pq.ParquetFile(path).read(columns=['bar_start_epoch']+names)
        times = table['bar_start_epoch'].to_numpy()
        if (len(times) != supplement['rows'] or key_hash(times) != supplement['key_sha256'] or
                not np.array_equal(times, endpoints['bar_start_epoch'].to_numpy())):
            raise ValueError('exact_endpoint_origin_join_required')
        for name in names:
            table = table.append_column(name, endpoints[name])
        if split is not None:
            table = table.filter(pc.equal(table['origin_split'], split))
        if file_sha(manifest_path) != overlay_sha or file_sha(base_root/'DATASET.json') != overlay['base_manifest_sha256']:
            raise ValueError('dataset_manifest_changed_during_read')
        yield pair, table
