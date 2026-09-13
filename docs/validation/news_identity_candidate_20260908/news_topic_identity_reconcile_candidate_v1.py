"""OFFLINE candidate: reconcile compatible current topic-ID variants.

The caller must put the preserved isolated project on sys.path so the ordinary
guard import below resolves to its frozen source. This module performs no data
I/O, changes no registrations, and is not integrated with the live producer.
All errors require the caller to withhold the current payload, not retry without
the offending topic. The independent frozen guard remains unchanged.
"""
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import math

import oanda_news_causal_aggregation_guard_v1 as guard

CANDIDATE_ID = "offline_topic_identity_reconciliation_candidate_v1_20260908"
MAX_INPUT_TOPICS = 10000
MAX_INPUT_BYTES = 32 * 1024 * 1024
MAX_COLLIDING_VARIANTS = 128
_EXPECTED_GUARD = "causal_news_member_admission_v1_20260907"
# These are provenance/display values which may differ for the same root claim.
# All remaining sealed original projected fields must agree exactly. In
# particular, a disagreement in direction, research score, scope, category,
# safety/classification flags, directness or currency attribution is not merged.
_RECONCILABLE_FIELDS = frozenset({
    "headline", "source_name", "source_url", "source_ids", "source_names",
    "distinct_source_count", "corroboration_count", "availability_lag_minutes",
    "directional_candidate_source_count", "forward_source_count", "forward_corroboration_count",
    "published_utc", "first_seen_utc", "causal_known_utc", "post_window_minutes",
    "direction_available_utc", "direction_expires_utc", "context_reason",
})
_MEMBER_CLOCKS = ("published_utc", "first_seen_utc", "causal_known_utc",
                  "detail_available_utc", "numeric_causal_known_utc",
                  "publication_clock_known_utc", "observed_available_utc")
_FULL_IDENTITY_KEYS = ("story_cluster_id", "topic_signature", "structured_event", "scheduled_utc")


def _encoded(value):
    try:
        return guard._canonical(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("reconcile_nonfinite_or_invalid_json") from exc


def _sha(value):
    return hashlib.sha256(_encoded(value)).hexdigest()


def _clock(value):
    try:
        return guard._clock(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("reconcile_invalid_original_clock") from exc


def _interval(topic):
    published = _clock(topic.get("published_utc"))
    window = topic.get("post_window_minutes")
    if type(window) not in (int, float) or not math.isfinite(window) or not 1 <= window <= 1440:
        raise ValueError("reconcile_invalid_original_window")
    return published, published + dt.timedelta(minutes=window)


def _strings(values, key):
    result = set()
    for value in values:
        items = value.get(key)
        if not isinstance(items, list) or any(not isinstance(item, str) for item in items):
            raise ValueError("reconcile_invalid_source_list")
        result.update(items)
    return sorted(result)


def _refresh(topic, as_of):
    """Refresh current admission, retaining the full producer identity fields."""
    result = copy.deepcopy(topic)
    result.update(guard.guard_topic(topic, None, as_of=as_of))
    guard.validate_guarded_topic(result, as_of=as_of)
    return result


def _merge(ident, topics, as_of):
    if len(topics) > MAX_COLLIDING_VARIANTS:
        raise ValueError("reconcile_variant_count_bound")
    # The old guard does not seal these full producer identity fields. They are
    # additional compatibility requirements on the retained input and their
    # complete input hashes are recorded; they are not newly authenticated by
    # successfully replaying the old guard.
    identities = []
    for topic in topics:
        for key, limit in (("story_cluster_id", 200), ("topic_signature", 1000)):
            value = topic.get(key)
            if not isinstance(value, str) or not 1 <= len(value.strip()) <= limit:
                raise ValueError("reconcile_missing_or_invalid_identity:" + key)
        if type(topic.get("structured_event")) is not bool:
            raise ValueError("reconcile_missing_or_invalid_identity:structured_event")
        schedule = topic.get("scheduled_utc")
        if topic["structured_event"]:
            if not isinstance(schedule, str) or not schedule:
                raise ValueError("reconcile_missing_or_invalid_identity:scheduled_utc")
            _clock(schedule)
        elif schedule not in (None, ""):
            raise ValueError("reconcile_ambiguous_nonstructured_schedule")
        identities.append({key: copy.deepcopy(topic.get(key)) for key in _FULL_IDENTITY_KEYS})
    if any(_encoded(identity) != _encoded(identities[0]) for identity in identities[1:]):
        raise ValueError("reconcile_incompatible_full_topic_identity")
    originals = [topic["causal_aggregation_guard"]["original_topic"] for topic in topics]
    for i, left in enumerate(originals):
        for right in originals[i + 1:]:
            if not guard.same_claim(left, right):
                raise ValueError("reconcile_incompatible_root_claim")
            for key in guard.PROJECTED_KEYS:
                if key not in _RECONCILABLE_FIELDS and _encoded(left.get(key)) != _encoded(right.get(key)):
                    raise ValueError("reconcile_incompatible_root_metadata:" + key)

    # Exact event-ID equality is not evidence of equal content. Reject conflicting
    # versions rather than choosing the earliest, latest or largest payload.
    members = {}
    duplicate_members = 0
    for topic in topics:
        topic_member_ids = set()
        for member in topic["causal_aggregation_guard"]["members"]:
            event_id = member.get("event_id")
            if not isinstance(event_id, str) or not 1 <= len(event_id) <= 200:
                raise ValueError("reconcile_invalid_member_identity")
            if event_id in topic_member_ids:
                # The original guard deliberately withholds this malformed
                # within-topic identity. Do not turn it into new support by
                # treating it as an ordinary duplicate across valid variants.
                raise ValueError("reconcile_duplicate_member_within_input_topic")
            topic_member_ids.add(event_id)
            if event_id in members:
                if _encoded(members[event_id]) != _encoded(member):
                    raise ValueError("reconcile_conflicting_member_identity")
                duplicate_members += 1
            else:
                members[event_id] = copy.deepcopy(member)
    if not 1 <= len(members) <= guard.MAX_MEMBERS:
        raise ValueError("reconcile_member_count_bound")
    union = [members[key] for key in sorted(members)]
    if len(_encoded(union)) > guard.MAX_EVIDENCE_BYTES:
        raise ValueError("reconcile_member_byte_bound")

    # The visible aggregate may describe the oldest retained publication, but
    # its current inclusion deadline is the intersection of original deadlines.
    intervals = [_interval(original) for original in originals]
    oldest_publication = min(start for start, _ in intervals)
    intersection_start = max(start for start, _ in intervals)
    expires = min(end for _, end in intervals)
    if not intersection_start <= as_of <= expires:
        raise ValueError("reconcile_no_current_window_intersection")

    first_seen = []
    available = []
    lags = []
    for member in union:
        published = _clock(member.get("published_utc"))
        seen = _clock(member.get("first_seen_utc"))
        clocks = [published, seen] + [_clock(member[key]) for key in _MEMBER_CLOCKS[2:] if member.get(key)]
        known = max(clocks)
        if known > as_of:
            raise ValueError("reconcile_member_not_yet_available")
        first_seen.append(seen)
        available.append(known)
        lags.append((known - published).total_seconds() / 60)

    # Deterministic representative provides display text only. It cannot choose
    # members, scores or support counts. Original inputs remain separately bound
    # by provenance returned below, and the original member views stay exact.
    original = copy.deepcopy(min(originals, key=lambda row: (_clock(row["published_utc"]), _encoded(row))))
    original.update(copy.deepcopy(identities[0]))
    original.update(
        topic_id=ident,
        published_utc=oldest_publication.isoformat(),
        first_seen_utc=min(first_seen).isoformat(),
        causal_known_utc=max(available).isoformat(),
        post_window_minutes=(expires - oldest_publication).total_seconds() / 60,
        source_ids=_strings(originals, "source_ids"),
        source_names=_strings(originals, "source_names"),
        # Support is recomputed from the full union by the guard. Do not sum
        # prior group counts or label the union's publishers as corroborators.
        distinct_source_count=0, corroboration_count=0,
        directional_candidate_source_count=0,
        forward_source_count=0, forward_corroboration_count=0,
        direction_available_utc=None, direction_expires_utc=None,
        availability_lag_minutes=max(lags),
        context_reason="reconciled_topic_requires_member_admission",
    )
    result = guard.guard_topic(original, union, as_of=as_of)
    guard.validate_guarded_topic(result, as_of=as_of)
    output_members = result["causal_aggregation_guard"]["members"]
    if {m["event_id"]: m for m in output_members} != members:
        raise ValueError("reconcile_member_preservation_failure")
    if len(_encoded(result)) > guard.MAX_EVIDENCE_BYTES:
        raise ValueError("reconcile_output_topic_byte_bound")
    provenance = {
        "topic_id": ident,
        "input_topic_sha256": sorted(_sha(topic) for topic in topics),
        "input_guard_sha256": sorted(topic["causal_aggregation_guard"]["sha256"] for topic in topics),
        "input_variants": len(topics), "retained_unique_members": len(union),
        "exact_duplicate_members_collapsed": duplicate_members,
        "original_window_intersection_start_utc": intersection_start.isoformat(),
        "original_window_intersection_end_utc": expires.isoformat(),
        "reconciled_at_utc": as_of.isoformat(),
        "all_member_payloads_preserved": True,
        "root_claims_compatible": True,
        "full_producer_identity": copy.deepcopy(identities[0]),
        "full_identity_scope": "Compared from complete retained producer inputs; covered by their input hashes, not by the old guard's projected-field seal.",
        "all_members_same_claim_asserted": False,
        "result_directional": bool(result["directional_publish_eligible"]),
        "result_guard_sha256": result["causal_aggregation_guard"]["sha256"],
        "result_topic_sha256": _sha(result),
    }
    return result, provenance


def reconcile_topic_identities_with_provenance(topics, *, as_of):
    """Return current reconciled topics and offline provenance, or raise.

    Current inclusion has the unchanged guard's inclusive original deadline.
    Expired/future publications are omitted; current singleton topics refresh
    their guarded admission while retaining full producer identity metadata.
    Every current topic is guard-validated before any reconciliation.
    No scores, member clocks or current schema/version are invented by this API.
    """
    if guard.GUARD_VERSION != _EXPECTED_GUARD:
        raise ValueError("reconcile_unexpected_guard_version")
    as_of = guard._asof(as_of)
    if not isinstance(topics, (list, tuple)) or len(topics) > MAX_INPUT_TOPICS:
        raise ValueError("reconcile_topic_input_bound")
    groups = {}
    total_bytes = 0
    omitted = 0
    for topic in topics:
        if not isinstance(topic, dict):
            raise ValueError("reconcile_topic_object_required")
        encoded = _encoded(topic)
        total_bytes += len(encoded)
        if total_bytes > MAX_INPUT_BYTES:
            raise ValueError("reconcile_input_byte_bound")
        start, end = _interval(topic)
        if not start <= as_of <= end:
            omitted += 1
            continue
        ident = topic.get("topic_id")
        if not isinstance(ident, str) or not 1 <= len(ident) <= 200:
            raise ValueError("reconcile_invalid_topic_identity")
        guard.validate_guarded_topic(topic, as_of=as_of)
        groups.setdefault(ident, []).append(copy.deepcopy(topic))
        if len(groups) > guard.MAX_CURRENT_TOPICS:
            raise ValueError("reconcile_current_topic_count_bound")

    results = []
    provenance = []
    for ident in sorted(groups):
        variants = groups[ident]
        if len(variants) == 1:
            results.append(_refresh(variants[0], as_of))
            continue
        # Exact duplicates need no new seal. Same semantic ID with distinct
        # evidence requires the narrow, explicit union path above.
        unique = {_encoded(topic): topic for topic in variants}
        if len(unique) == 1:
            results.append(_refresh(next(iter(unique.values())), as_of))
            continue
        merged, row = _merge(ident, list(unique.values()), as_of)
        results.append(merged)
        provenance.append(row)

    # A final unchanged guard pass independently checks unique IDs, replay,
    # current topic/member bounds and the accepted current-payload byte ceiling.
    verification = guard.build_current_news_snapshot(results, as_of=as_of)
    if verification["status"] != "current":
        raise ValueError("reconcile_final_guard_rejection:" + "|".join(verification.get("errors") or []))
    return {"offline_candidate": True, "candidate_id": CANDIDATE_ID,
            "topics": results, "reconciliations": provenance,
            "out_of_window_inputs_omitted": omitted}


def reconcile_topic_identities(topics, *, as_of):
    """List-only caller API; errors must withhold the complete current payload."""
    return reconcile_topic_identities_with_provenance(topics, as_of=as_of)["topics"]
