"""Pure, bounded news-feature eligibility diagnostic; no fit, prediction or I/O.

Caller-supplied source receipts must come from verified raw/SQLite mapping. The
checksums below preserve identity; they do not authenticate an asserted receipt.
This separate experiment excludes post-move explanations from entry features.
It does not change the frozen 34-input model or reinterpret its neutral defaults.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
import datetime as dt
import hashlib
import json
import math
import re
import time

import oanda_news_causal_aggregation_guard_v1 as guard
from oanda_news_classification_contract import NEWS_CLASSIFICATION_VERSION_V164

SCHEMA = "entry_news_feature_join_v1_20260909"
MAX_TOPICS = 128
MAX_MEMBERS = 4096
MAX_HISTORY = 4096
MAX_BYTES = 16 * 1024 * 1024
MAX_NEWS_AGE_SEC = 300
CONTEXT_WINDOW_SEC = 3600
NEWS_FEATURES = (
    "context_balance", "context_volume_log", "context_signed_fraction", "context_mean_age_hours",
    "vetted_balance", "vetted_volume_log", "vetted_conflict_fraction", "vetted_remaining_hours",
)
LEGACY_DEFAULTS = (0., 0., 0., 1., 0., 0., 0., 0.)
AUTHORITY = dict(research_only=True, feature_only=True, can_place_orders=False,
                 can_promote=False, can_authorize=False, forecast_proof_eligible=False)
SOURCE_FILES = frozenset((
    "oanda_local_news_sentiment_repair_v1.py", "oanda_news_topic_identity_reconciliation_v1.py",
    "oanda_local_news_sentiment.py", "oanda_news_causal_aggregation_guard_v1.py",
    "oanda_news_classification_contract.py", "oanda_news_event_tagger.py",
    "oanda_news_collector_contract.py",
))
CLASSIFICATIONS = frozenset((NEWS_CLASSIFICATION_VERSION_V164, guard.CLASSIFICATION_VERSION))


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def epoch(value):
    require(isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value > 0, "invalid_epoch")
    return float(value)


def utc(value):
    require(isinstance(value, str), "missing_original_clock")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("invalid_original_clock") from None
    require(parsed.tzinfo is not None, "unaware_original_clock")
    return epoch(parsed.timestamp())


def sha(value):
    require(isinstance(value, str) and re.fullmatch("[a-f0-9]{64}", value), "missing_source_hash")
    return value


def identity(value):
    require(isinstance(value, str) and 0 < len(value) <= 256, "missing_record_identity")
    return value


def _binding(value):
    require(isinstance(value, dict) and set(value) == SOURCE_FILES, "source_closure_incomplete")
    return {name: sha(item) for name, item in value.items()}


def _scores(value):
    require(isinstance(value, dict) and len(value) <= 32, "invalid_currency_scores")
    result = {}
    for key, score in value.items():
        require(isinstance(key, str) and re.fullmatch("[A-Z]{3}", key), "invalid_currency")
        require(isinstance(score, (int, float)) and not isinstance(score, bool)
                and math.isfinite(score) and -1 <= score <= 1, "invalid_currency_score")
        result[key] = float(score)
    return result


def _member(member, enclosing_observed, cutoff):
    event_id = identity(member.get("event_id"))
    published, first_seen = utc(member.get("published_utc")), utc(member.get("first_seen_utc"))
    known = max(published, first_seen, *(utc(member[key]) for key in (
        "causal_known_utc", "detail_available_utc", "numeric_causal_known_utc", "publication_clock_known_utc"
    ) if member.get(key) not in (None, "")))
    available = max(known, enclosing_observed,
                    utc(member["observed_available_utc"]) if member.get("observed_available_utc") else known)
    require(available <= cutoff, "member_not_available_at_cutoff")
    require(member.get("classification_version") in CLASSIFICATIONS, "member_classification_unrecognized")
    return dict(event_id=event_id, source_id=identity(member.get("source_id")),
        member_sha256=digest(member), source_version=member["classification_version"],
        original_event_epoch=published, original_first_seen_epoch=first_seen,
        original_known_epoch=known, observed_available_epoch=available,
        currency_scores=_scores(member.get("currency_scores")),
        reports_prior_market_move=member.get("reports_prior_market_move") is True,
        headline_identity=guard._headline(member), underlying_event_id=member.get("underlying_event_id"))


def _features(context, vetted, pair, cutoff, available):
    base, quote = pair.split("_")
    def relevant(rows):
        return [row for row in rows if base in row["currency_scores"] or quote in row["currency_scores"]]
    context, vetted = relevant(context), relevant(vetted)
    def value(row):
        return row["currency_scores"].get(base, 0.) - row["currency_scores"].get(quote, 0.)
    cv, vv = [value(row) for row in context], [value(row) for row in vetted]
    def average(values):
        return sum(values) / len(values) if values else None
    values = (
        average(cv), math.log1p(len(cv)) if available else None,
        average([(v > 0) - (v < 0) for v in cv]),
        average([(cutoff-row["original_known_epoch"])/3600 for row in context]),
        average(vv), math.log1p(len(vv)) if available else None,
        min(sum(v > 0 for v in vv), sum(v < 0 for v in vv))/len(vv) if vv else None,
        min(1., min((row["expires_epoch"]-cutoff)/3600 for row in vetted)) if vv else None,
    )
    result = {}
    for index, name in enumerate(NEWS_FEATURES):
        count = len(cv) if index < 4 else len(vv)
        val = values[index]
        result[name] = {"value": val, "status": ("unavailable" if not available else
            "measured" if count else "observed_empty_count" if index in (1, 5) else "undefined_empty_population"),
            "support_count": count if available else None,
            "legacy_default_not_a_measurement": LEGACY_DEFAULTS[index]}
    return result


def _current(envelope, cutoff, expected):
    """All-topic validity is separate from explicit per-member eligibility."""
    require(isinstance(envelope, dict), "current_news_missing")
    require(envelope.get("complete") is True, "current_news_population_incomplete")
    require(envelope.get("guard_version") == guard.GUARD_VERSION, "guard_version_mismatch")
    require(envelope.get("classification_version") == guard.CLASSIFICATION_VERSION,
            "snapshot_classification_mismatch")
    require(_binding(envelope.get("source_bindings")) == expected, "source_bindings_mismatch")
    sha(envelope.get("raw_source_sha256"))
    asof = epoch(envelope.get("as_of_epoch"))
    visible = epoch(envelope.get("source_visible_epoch"))
    observed = epoch(envelope.get("consumer_observed_epoch"))
    require(asof <= visible <= observed <= cutoff and cutoff-asof <= MAX_NEWS_AGE_SEC,
            "current_news_stale_future_or_unobserved")
    topics = envelope.get("topics")
    require(isinstance(topics, list) and len(topics) <= MAX_TOPICS, "topic_bound")
    require(envelope.get("topics_sha256") == digest(topics), "topic_population_hash_mismatch")
    members = defaultdict(list); validated = []; rejected = []; topic_identities = {}
    total = 0
    for topic in topics:
        checked = guard.validate_guarded_topic(topic, as_of=dt.datetime.fromtimestamp(cutoff, dt.timezone.utc))
        proof = topic["causal_aggregation_guard"]
        require(utc(proof["evaluated_utc"]) <= asof, "topic_after_source_cutoff")
        topic_id = identity(topic.get("topic_id"))
        topic_hash = digest(topic)
        require(topic_id not in topic_identities or topic_identities[topic_id] == topic_hash,
                "conflicting_topic_identity")
        topic_identities[topic_id] = topic_hash
        compact = proof["members"]
        total += len(compact); require(total <= MAX_MEMBERS, "member_bound")
        ids = []; member_invalid = False
        for member in compact:
            try:
                row = _member(member, observed, cutoff)
                members[row["event_id"]].append(row); ids.append(row["event_id"])
            except (ValueError, TypeError, KeyError) as exc:
                member_invalid = True
                rejected.append({"topic_id": topic_id, "event_id": member.get("event_id"),
                    "reason": str(exc) if isinstance(exc, ValueError) else "invalid_member"})
        validated.append((topic_id, checked, ids, len(ids) != len(set(ids)), member_invalid))
    ambiguous = {key for key, rows in members.items() if len({r["member_sha256"] for r in rows}) != 1}
    context = []; discovery = []; headlines = set(); duplicates = 0
    for event_id, rows in sorted(members.items(), key=lambda x: (x[1][0]["observed_available_epoch"], x[0])):
        if event_id in ambiguous:
            rejected.append({"event_id": event_id, "reason": "conflicting_member_identity"}); continue
        row = rows[0]; duplicates += len(rows)-1
        if row["reports_prior_market_move"]:
            discovery.append({**row, "reason": "post_move_explanation"}); continue
        if cutoff-row["original_known_epoch"] > CONTEXT_WINDOW_SEC:
            rejected.append({"event_id": event_id, "reason": "outside_context_window"}); continue
        if row["headline_identity"] in headlines:
            duplicates += 1; continue
        headlines.add(row["headline_identity"])
        context.append(row)
    vetted = []; seen_guards = set()
    for topic_id, checked, ids, internal_duplicate, member_invalid in validated:
        if checked["status"] != "directional":
            rejected.append({"topic_id": topic_id, "reason": checked["reason"] or "guard_context_only"}); continue
        if internal_duplicate or member_invalid or ambiguous.intersection(ids):
            rejected.append({"topic_id": topic_id, "reason": "duplicate_or_conflicting_member_support"}); continue
        if checked["guard_sha256"] in seen_guards:
            duplicates += 1; continue
        seen_guards.add(checked["guard_sha256"])
        expires = utc(checked["direction_expires_utc"])
        available = max(observed, utc(checked["direction_available_utc"]))
        require(available <= cutoff <= expires, "forward_clock_binding")
        vetted.append(dict(topic_id=topic_id, guard_sha256=checked["guard_sha256"],
            source_version=guard.CLASSIFICATION_VERSION, currency_scores=_scores(checked["currency_scores"]),
            observed_available_epoch=available, expires_epoch=expires,
            forward_source_count=checked["forward_source_count"], member_ids=ids))
    # Shared members across distinct directional proofs cannot become two votes.
    incidence = Counter(event_id for row in vetted for event_id in set(row["member_ids"]))
    overlap = {key for key, count in incidence.items() if count > 1}
    retained = []
    for row in vetted:
        if overlap.intersection(row["member_ids"]):
            rejected.append({"topic_id": row["topic_id"], "reason": "overlapping_forward_topic_support"})
        else:
            retained.append(row)
    forward_ids = {event_id for row in retained for event_id in row["member_ids"]}
    context_only = [row for row in context if row["event_id"] not in forward_ids]
    return dict(status="available", context=context, context_only=context_only,
        forward_context_overlap_member_count=len(context)-len(context_only), vetted=retained, discovery=discovery,
        rejections=rejected, identical_duplicates_collapsed=duplicates,
        source=dict(raw_source_sha256=envelope["raw_source_sha256"], as_of_epoch=asof,
                    source_visible_epoch=visible, consumer_observed_epoch=observed,
                    topics_sha256=envelope["topics_sha256"]), input_topic_count=len(topics))


def _history(records, cutoff, kind):
    require(isinstance(records, (list, tuple)) and len(records) <= MAX_HISTORY, "history_bound")
    accepted = []; discovery = []; rejected = []; groups = defaultdict(list)
    for row in records:
        require(isinstance(row, dict), "history_record_invalid")
        try:
            groups[identity(row.get("record_id"))].append(row)
        except ValueError as exc:
            rejected.append({"record_id": None, "reason": str(exc)})
    for record_id, variants in sorted(groups.items()):
        if len({digest(row) for row in variants}) != 1:
            rejected.append({"record_id": record_id, "reason": "conflicting_history_identity"}); continue
        row = variants[0]
        try:
            # A historical event timestamp cannot attest when this feature or
            # result was observed. Retrospective material stays discovery.
            sha(row.get("source_sha256")); identity(row.get("source_version"))
            require(row.get("source_visible_epoch") is not None and row.get("observed_available_epoch") is not None,
                    "source_visibility_unavailable")
            require(row.get("feature_available_epoch") is not None, "feature_availability_unavailable")
            event = epoch(row.get("original_event_epoch")); seen = epoch(row.get("original_first_seen_epoch"))
            visible = epoch(row.get("source_visible_epoch")); observed = epoch(row.get("observed_available_epoch"))
            derived = epoch(row.get("feature_available_epoch"))
            require(event <= observed and seen <= observed and visible <= observed
                    and max(observed, derived) <= cutoff, "history_not_available_at_cutoff")
            require(row.get("source_availability_basis") == "independent_committed_observation",
                    "independent_visibility_unproven")
            if row.get("selection_basis") != "source_first_universe" or row.get("reports_prior_market_move") is True:
                discovery.append({"record_id": record_id, "reason": "outcome_selected_or_unproven_universe",
                    "source_sha256": row["source_sha256"], "source_version": row["source_version"]}); continue
            event_id = identity(row.get("underlying_event_id"))
            currency = row.get("currency")
            require(isinstance(currency, str) and re.fullmatch("[A-Z]{3}", currency), "invalid_currency")
            retained = dict(record_id=record_id, underlying_event_id=event_id, currency=currency,
                factor_type=identity(row.get("factor_type")),
                source_sha256=row["source_sha256"], source_version=row["source_version"],
                original_event_epoch=event, source_visible_epoch=visible, observed_available_epoch=observed,
                feature_available_epoch=derived, record_sha256=digest(row))
            if kind == "memory":
                require(row.get("response_available_epoch") is not None, "response_observation_unavailable")
                start = epoch(row.get("response_start_epoch")); target = epoch(row.get("response_target_epoch"))
                response_available = epoch(row.get("response_available_epoch"))
                require(max(event, seen, visible, observed, derived) <= start < target <= response_available <= cutoff,
                        "response_not_causally_mature_at_cutoff")
                sha(row.get("response_source_sha256"))
                value = row.get("response_value_bps")
                require(isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value),
                        "response_measurement_missing")
                retained.update(response_start_epoch=start, response_target_epoch=target,
                    response_available_epoch=response_available, response_value_bps=float(value),
                    response_source_sha256=row["response_source_sha256"])
            else:
                value = row.get("signed_factor_score")
                require(row.get("numeric_measurement_state") == "measured" and isinstance(value, (int, float))
                        and not isinstance(value, bool) and math.isfinite(value), "factor_measurement_unproven")
                retained["signed_factor_score"] = float(value)
            accepted.append(retained)
        except ValueError as exc:
            rejected.append({"record_id": record_id, "reason": str(exc),
                "selection_basis": row.get("selection_basis"),
                "missing_clock_fields": [name for name in (
                    "original_event_epoch", "original_first_seen_epoch", "source_visible_epoch",
                    "observed_available_epoch", "feature_available_epoch",
                    *( ("response_start_epoch", "response_target_epoch", "response_available_epoch") if kind == "memory" else () )
                ) if row.get(name) is None]})
    return dict(eligible=accepted, discovery=discovery, rejections=rejected,
        input_record_count=len(records), distinct_eligible_events=len({r["underlying_event_id"] for r in accepted}),
        learned_orientation=None, orientation_status="not_fitted_by_this_diagnostic")


def join_entry_news_features(*, instrument, cutoff_epoch, current_news=None,
        factor_records=(), response_records=(), expected_source_bindings, clock=time.time):
    """Join caller-attested evidence at a decision cutoff without creating forecasts.

    `current_news` has the complete guarded topic list and its hash, exact seven
    producer bindings, as_of/source_visible/consumer_observed clocks. Historical
    records require independent source and feature visibility; response memory
    additionally requires pre-response feature availability and mature outcomes.
    An omitted history collection means not provided, never a complete empty DB.
    """
    require(isinstance(instrument, str) and re.fullmatch("[A-Z]{3}_[A-Z]{3}", instrument)
            and instrument[:3] != instrument[4:], "invalid_instrument")
    cutoff = epoch(cutoff_epoch); observed = epoch(clock())
    require(cutoff <= observed, "future_feature_cutoff")
    bindings = _binding(expected_source_bindings)
    inputs = dict(current_news=current_news, factors=factor_records, responses=response_records)
    require(len(canonical(inputs)) <= MAX_BYTES, "join_input_byte_bound")
    try:
        current = _current(current_news, cutoff, bindings)
    except (ValueError, TypeError, KeyError) as exc:
        current = dict(status="unavailable", reason=str(exc) if isinstance(exc, ValueError) else "current_evidence_invalid",
            context=[], context_only=[], forward_context_overlap_member_count=0, vetted=[], discovery=[], rejections=[])
    factors = _history(factor_records, cutoff, "factor")
    memory = _history(response_records, cutoff, "memory")
    for lane in (factors, memory):
        lane["population_scope"] = "caller_provided_records_only_not_complete_history"
        lane["status"] = "evaluated_sample" if lane["input_record_count"] else "not_provided"
        lane["pair_eligible"] = [r for r in lane["eligible"] if r["currency"] in instrument.split("_")]
    features = _features(current["context"], current["vetted"], instrument, cutoff, current["status"] == "available")
    result = dict(schema_version=SCHEMA, instrument=instrument, cutoff_epoch=cutoff,
        diagnostic_observed_epoch=observed, input_sha256=digest(inputs), source_bindings=bindings,
        current_news=current, entry_factors=factors, matured_response_memory=memory,
        news_features=features, numerical_forecast=None, learned_news_direction=None,
        lineage={"existing_model_total_features":34, "existing_price_features":24,
                 "existing_news_features":list(NEWS_FEATURES), "existing_interactions":2,
                 "new_join_is_not_frozen_model_input_replacement":True,
                 "post_move_context_excluded_by_new_policy":True},
        limitations=["Receipts are caller-attested; pure replay cannot authenticate external I/O.",
            "Empty complete current populations permit measured zero counts, not invented balances or ages.",
            "Context scores are classifier outputs, not calibrated forecasts or independently verified facts.",
            "The legacy broad-context feature population can overlap forward members; context_only explicitly excludes retained forward members.",
            "Matured response rows and distinct event IDs are not an independent effective sample size.",
            "No fitting, response-dependent selection, publication, orders or model activation occurs."], **AUTHORITY)
    result["diagnostic_sha256"] = digest(result)
    return deepcopy(result)
