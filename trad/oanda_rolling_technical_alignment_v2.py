"""Bounded exact-minute retained peer alignment; no writes or V1 changes.

The worker supplies all universe keys and only currently eligible pair-local
observations. A None value excludes that pair from every panel in this call.
Retained rows are read from a TechnicalStore-compatible ``connection`` and
``names`` adapter. Candidate panels must be published by the caller before
their references become externally available; selection time is not panel
publication time. Newer pair-local features remain separate from an older
aligned peer observation.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
import math
from numbers import Real
from typing import Mapping
import zlib

import oanda_rolling_technical_panel_v1 as panel_kernel

SCHEMA = "rolling_m1_retained_peer_alignment_v2_20260915"
MAX_CANDIDATE_MINUTES = 16
MAX_FEATURES = 4096
MAX_BLOB_BYTES = 1024 * 1024
MAX_BAR_AGE_SECONDS = 3600


def _finite(value):
    try:
        return isinstance(value, Real) and not isinstance(value, bool) and math.isfinite(value)
    except (ValueError, OverflowError):
        return False


def _settings(now_epoch, maximum_bar_age_seconds):
    if not _finite(now_epoch) or not 60 <= now_epoch <= 253402300799:
        raise ValueError("finite_selection_clock_required")
    if not _finite(maximum_bar_age_seconds) or not 0 <= maximum_bar_age_seconds <= MAX_BAR_AGE_SECONDS:
        raise ValueError("bounded_freshness_seconds_required")
    return float(now_epoch), float(maximum_bar_age_seconds)


def _pairs(values):
    pairs = tuple(values)
    if any(not isinstance(p, str) or not panel_kernel.PAIR.fullmatch(p) or p[:3] == p[4:] for p in pairs):
        raise ValueError("valid_distinct_currency_pair_keys_required")
    if len(pairs) > panel_kernel.MAX_PAIRS or len(set(pairs)) != len(pairs):
        raise ValueError("bounded_unique_pair_universe_required")
    return tuple(sorted(pairs))


def _minute(value):
    return _finite(value) and 0 <= value <= 253402300799 and value % 60 == 0


def structural_peer_support(pairs):
    """Universe-level support, independent of freshness or return missingness."""
    pairs = _pairs(pairs)
    counts = Counter(currency for pair in pairs for currency in pair.split("_"))
    return {pair: {
        "structural_peer_limited": any(counts[currency] - 1 < panel_kernel.MIN_PEERS for currency in pair.split("_")),
        "minimum_peer_pairs_per_leg": panel_kernel.MIN_PEERS,
        "base_peer_pair_count": counts[pair[:3]] - 1,
        "quote_peer_pair_count": counts[pair[4:]] - 1,
    } for pair in pairs}


def _decode(row, names, now, maximum_age):
    t, published, observed = row["t"], row["published"], row["first_observed"]
    if not _minute(t):
        raise ValueError("invalid_original_minute")
    if not 0 <= now - (t + 60) <= maximum_age:
        raise ValueError("bar_outside_fresh_completed_window")
    if not _finite(published) or not t + 60 <= published <= now or published <= 0:
        raise ValueError("observation_not_published_by_selection")
    if not _finite(observed) or not t + 60 <= observed <= published:
        raise ValueError("invalid_original_observation_clock")
    if row["feature_count"] != len(names) or row["values_blob"] is None:
        raise ValueError("bounded_ordered_feature_schema_required")
    unpacker = zlib.decompressobj()
    raw = unpacker.decompress(row["values_blob"], MAX_BLOB_BYTES + 1)
    if len(raw) > MAX_BLOB_BYTES or not unpacker.eof or unpacker.unused_data:
        raise ValueError("bounded_complete_feature_blob_required")
    if hashlib.sha256(raw).hexdigest() != row["feature_hash"]:
        raise ValueError("retained_feature_hash_mismatch")
    values = json.loads(raw)
    if not isinstance(values, list) or len(values) != len(names):
        raise ValueError("ordered_feature_schema_mismatch")
    if any(value is not None and not _finite(value) for value in values):
        raise ValueError("nonfinite_retained_feature")
    available = sum(value is not None for value in values)
    if row["available_count"] != available:
        raise ValueError("retained_feature_availability_mismatch")
    return {"pair": row["pair"], "bar_start_epoch": int(t), "bar_end_epoch": int(t) + 60,
            "published_epoch": published, "first_observed_epoch": observed,
            "available_features": available, "feature_count": len(values),
            "feature_hash": row["feature_hash"], "values": dict(zip(names, values))}


def read_exact_observations(store, pairs, target_epochs, *, now_epoch, maximum_bar_age_seconds):
    """Read at most 256 * 16 retained rows using the (pair,t) primary index.

    SQL never chooses a nearest row. Blob size is bounded before fetching and
    during decompression. Published/read-completed clocks and hashes must be
    valid at the explicit selection clock. This method performs only SELECTs.
    Missing, unpublished, revised, malformed and future rows stay unavailable.
    """
    now, maximum_age = _settings(now_epoch, maximum_bar_age_seconds)
    pairs = _pairs(pairs)
    targets = tuple(target_epochs)
    if (len(targets) > MAX_CANDIDATE_MINUTES or any(not _minute(t) for t in targets)
            or len(set(targets)) != len(targets)):
        raise ValueError("bounded_unique_exact_target_minutes_required")
    targets = tuple(sorted(int(t) for t in targets))
    if any(not 0 <= now - (t + 60) <= maximum_age for t in targets):
        raise ValueError("target_outside_fresh_completed_window")
    names = tuple(store.names)
    if (not names or len(names) > MAX_FEATURES
            or any(not isinstance(name, str) or len(name) > 256 for name in names)
            or len(set(names)) != len(names)):
        raise ValueError("bounded_ordered_feature_schema_required")
    result = {t: {} for t in targets}
    rejected = []
    rows_read = 0
    if not targets:
        return {"by_epoch": result, "rejected_sources": rejected, "rows_read": rows_read}
    placeholders = ",".join("?" for _ in targets)
    for pair in pairs:
        if store.connection.execute("SELECT 1 FROM revisions WHERE pair=? LIMIT 1", (pair,)).fetchone():
            rejected.append({"pair": pair, "reason": "unresolved_original_input_revision"})
            continue
        rows = store.connection.execute(
            "SELECT o.pair,o.t,o.feature_hash,o.feature_count,o.available_count,o.published,"
            "CASE WHEN length(o.values_blob)<=? THEN o.values_blob END AS values_blob,b.first_observed "
            "FROM observations o JOIN bars b ON b.pair=o.pair AND b.t=o.t "
            "WHERE o.pair=? AND o.t IN (" + placeholders + ") ORDER BY o.t DESC LIMIT ?",
            (MAX_BLOB_BYTES, pair, *targets, len(targets)))
        for row in rows:
            rows_read += 1
            try:
                observation = _decode(row, names, now, maximum_age)
            except (ValueError, TypeError, zlib.error, UnicodeError) as exc:
                rejected.append({"pair": pair, "bar_start_epoch": row["t"],
                                 "reason": "invalid_retained_observation:" + str(exc)[:160]})
                continue
            result[observation["bar_start_epoch"]][pair] = observation
    return {"by_epoch": result, "rejected_sources": rejected, "rows_read": rows_read}


def build_aligned_panels(store, latest_observations: Mapping, *, now_epoch,
                         maximum_bar_age_seconds, max_candidate_minutes=8):
    """Select useful exact panels without replacing pair-local observations.

    Fresh minute candidates are newest first, capped at max_candidate_minutes.
    Each pair chooses its newest full panel, or highest finite feature count
    then newest clock if only partial panels exist. Missing features remain
    None. A pair can never use a panel newer than its eligible local row.
    ``by_pair`` references candidates by original target epoch; the caller adds
    the durable panel id and actual publication clock after publication.
    """
    now, maximum_age = _settings(now_epoch, maximum_bar_age_seconds)
    if not isinstance(latest_observations, Mapping):
        raise ValueError("explicit_latest_observation_mapping_required")
    if (isinstance(max_candidate_minutes, bool) or not isinstance(max_candidate_minutes, int)
            or not 1 <= max_candidate_minutes <= MAX_CANDIDATE_MINUTES):
        raise ValueError("bounded_candidate_minute_count_required")
    pairs = _pairs(latest_observations)
    structural = structural_peer_support(pairs)
    eligible, excluded = {}, []
    for pair in pairs:
        row = latest_observations[pair]
        reason = None
        if row is None:
            reason = "ineligible_latest_observation"
        elif not isinstance(row, Mapping) or row.get("pair") != pair or not _minute(row.get("bar_start_epoch")):
            reason = "invalid_latest_observation"
        elif not 0 <= now - (row["bar_start_epoch"] + 60) <= maximum_age:
            reason = "latest_observation_outside_fresh_completed_window"
        elif (not _finite(row.get("published_epoch"))
              or not row["bar_start_epoch"] + 60 <= row["published_epoch"] <= now):
            reason = "latest_observation_not_published_by_selection"
        if reason:
            excluded.append({"pair": pair, "reason": reason})
        else:
            eligible[pair] = row
    targets = []
    if eligible:
        newest = max(int(row["bar_start_epoch"]) for row in eligible.values())
        targets = [newest - 60 * i for i in range(max_candidate_minutes)
                   if newest - 60 * i >= 0 and 0 <= now - (newest - 60 * i + 60) <= maximum_age]
    retained = read_exact_observations(store, eligible, targets, now_epoch=now,
                                       maximum_bar_age_seconds=maximum_age)
    candidates = []
    for target in targets:
        sources = {pair: row for pair, row in retained["by_epoch"][target].items()
                   if target <= eligible[pair]["bar_start_epoch"]}
        if not sources:
            continue
        panel = panel_kernel.compute_panel({pair: sources.get(pair) for pair in pairs}, target)
        candidates.append({"target_bar_start_epoch": target, "panel": panel,
                           "source_observations": sources,
                           "source_max_published_epoch": max(row["published_epoch"] for row in sources.values())})
    by_pair = {}
    for pair in pairs:
        supported = [candidate for candidate in candidates if pair in candidate["source_observations"]]
        selection = max(supported, key=lambda c: (c["panel"]["by_pair"][pair]["finite_feature_count"],
                                                c["target_bar_start_epoch"]), default=None)
        reference = {**structural[pair], "status": "unavailable", "reason": "no_fresh_exact_retained_observation",
                     "target_bar_start_epoch": None, "observation": None,
                     "latest_pair_local_bar_start_epoch": eligible[pair]["bar_start_epoch"] if pair in eligible else None,
                     "alignment_lag_seconds": None, "finite_feature_count": 0}
        if selection is not None:
            observation = selection["source_observations"][pair]
            peer = selection["panel"]["by_pair"][pair]
            reference.update(status=peer["status"], reason=None if peer["status"] == "available" else
                             "structural_peer_limit" if structural[pair]["structural_peer_limited"] else
                             "insufficient_exact_peer_support_or_missing_horizon",
                             target_bar_start_epoch=selection["target_bar_start_epoch"],
                             observation={key: observation[key] for key in
                                          ("bar_start_epoch", "bar_end_epoch", "published_epoch", "feature_hash")},
                             alignment_lag_seconds=int(eligible[pair]["bar_start_epoch"] - selection["target_bar_start_epoch"]),
                             finite_feature_count=peer["finite_feature_count"],
                             peer_provenance_sha256=peer["provenance_sha256"])
        by_pair[pair] = reference
    return {"schema": SCHEMA, "selection_epoch": now, "maximum_bar_age_seconds": maximum_age,
            "candidate_minute_limit": max_candidate_minutes, "candidate_target_epochs": targets,
            "candidates": candidates, "by_pair": by_pair,
            "structural_peer_limited_pairs": [pair for pair in pairs if structural[pair]["structural_peer_limited"]],
            "excluded_latest_sources": excluded, "rejected_retained_sources": retained["rejected_sources"],
            "retained_rows_read": retained["rows_read"],
            "clock_policy": "exact original completed M1 minute; no as-of substitution or future rows",
            "publication_policy": "caller must retain actual panel publication; selection is not publication",
            "can_place_orders": False}
