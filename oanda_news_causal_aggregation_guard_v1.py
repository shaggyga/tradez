"""Versioned, pure admission guard for causal FX-news aggregation.

This module performs no I/O and cannot place orders. Classification candidates
remain research evidence. Admission requires each contributing publisher's own
clock, a current reaction window, and independent support for the same claim.
"""
from __future__ import annotations

from collections import Counter
import copy
import datetime as dt
import hashlib
import json
import math
import re
from urllib.parse import urlsplit

GUARD_VERSION = "causal_news_member_admission_v1_20260907"
CLASSIFICATION_VERSION = "local_fx_news_rules_20260907_v165_causal_member_admission"
MAX_MEMBERS = 128
MAX_EVIDENCE_BYTES = 256 * 1024
MAX_CURRENT_TOPICS = 128
MAX_CURRENT_SNAPSHOT_BYTES = 8 * 1024 * 1024
MEMBER_KEYS = (
    "event_id", "headline", "source_name", "publisher_url", "source_url", "source_id",
    "source_verified", "source_direct", "source_quality", "directional_confidence",
    "first_seen_utc", "causal_known_utc", "observed_available_utc", "published_utc", "detail_available_utc",
    "numeric_causal_known_utc", "publication_clock_known_utc", "currency_scores",
    "semantic_claims", "forward_signal_timely", "forward_timeliness_limit_minutes",
    "estimated_reaction_horizon_minutes", "reports_prior_market_move", "context_only",
    "source_listing_bootstrap", "directional_research_only", "detail_enrichment_research_only",
    "classification_version", "non_catalyst_context", "secondary_analysis_context",
    "direct_currencies", "inferred_currencies", "scope", "category",
)
PROJECTED_KEYS = (
    "topic_id", "headline", "source_name", "source_ids", "source_names", "source_url", "category",
    "source_verified", "source_direct", "direct_currencies", "inferred_currencies", "scope",
    "distinct_source_count", "corroboration_count", "availability_lag_minutes",
    "post_window_minutes",
    "currency_scores", "research_currency_scores", "directional_bias", "directional_publish_eligible",
    "directional_evidence", "directional_source_grade", "directional_candidate_source_count",
    "directional_verified_source", "forward_signal_timely", "context_only", "context_reason",
    "relevant", "causal_known_utc", "first_seen_utc", "published_utc", "direction_available_utc",
    "direction_expires_utc", "forward_source_count", "forward_corroboration_count",
    "classification_version", "execution_eligible", "can_place_orders", "research_only",
)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _clock(value):
    if not isinstance(value, str):
        raise ValueError("member_clock_missing")
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or not math.isfinite(parsed.timestamp()) or parsed.timestamp() <= 0:
        raise ValueError("member_clock_invalid")
    return parsed.astimezone(dt.timezone.utc)


def _asof(value):
    if not isinstance(value, dt.datetime) or value.tzinfo is None:
        raise ValueError("aware_as_of_required")
    return _clock(value.isoformat())


def _number(value, low, high):
    if type(value) not in (int, float) or not math.isfinite(value) or not low <= value <= high:
        raise ValueError("member_numeric_invalid")
    return float(value)


def _publisher(member):
    # Google is transport, not an independently reporting publisher.
    url = member.get("publisher_url")
    host = urlsplit(url).hostname if isinstance(url, str) else None
    if host and host.lower().removeprefix("www.") not in {"news.google.com", "google.com"}:
        return "domain:" + host.lower().removeprefix("www.")
    name = member.get("source_name")
    if isinstance(name, str) and 1 <= len(name.strip()) <= 200:
        return "publisher:" + " ".join(name.lower().split())
    raise ValueError("publisher_identity_missing")


def _headline(member):
    value = member.get("headline")
    if not isinstance(value, str) or not 1 <= len(value) <= 4000:
        raise ValueError("headline_invalid")
    publisher = member.get("source_name")
    if isinstance(publisher, str):
        value = re.sub(r"\s+[-|–—]\s*" + re.escape(publisher) + r"\s*$", "", value, flags=re.I)
    value = re.sub(r"^(?:(?:business|world|financial|market|latest|international)\s+news|news)\s*(?:\||:|-)\s*", "", value, flags=re.I)
    return " ".join(re.findall(r"[^\W_]+", value.casefold(), flags=re.UNICODE))


def same_claim(left, right):
    """Conservative corroboration, independent of broad category signatures."""
    a, b = _headline(left), _headline(right)
    # Exact syndicated headlines form one evidence group, never corroboration.
    if a == b:
        return True
    negations = {"not", "no", "never", "denies", "denied", "rejects", "rejected"}
    aw, bw = set(a.split()), set(b.split())
    if aw & negations != bw & negations:
        return False
    if set(re.findall(r"\d+(?:\.\d+)?", str(left.get("headline")))) != set(re.findall(r"\d+(?:\.\d+)?", str(right.get("headline")))):
        return False
    ac, bc = left.get("semantic_claims"), right.get("semantic_claims")
    if isinstance(ac, list) and isinstance(bc, list) and ac and bc:
        return bool({_canonical(item) for item in ac} & {_canonical(item) for item in bc})
    stop = {"a", "an", "the", "and", "or", "of", "to", "in", "on", "at", "for", "as", "by", "with", "from", "its", "says", "said", "news", "latest"}
    aw, bw = aw - stop, bw - stop
    return min(len(aw), len(bw)) >= 5 and len(aw & bw) / len(aw | bw) >= 0.8


def _member_view(member, as_of):
    row = {key: copy.deepcopy(member.get(key)) for key in MEMBER_KEYS}
    evidence = {"event_id": row.get("event_id"), "admitted": False, "reasons": []}
    try:
        if not isinstance(row["event_id"], str) or not 1 <= len(row["event_id"]) <= 200:
            raise ValueError("event_identity_missing")
        evidence["publisher_identity"] = _publisher(row)
        evidence["normalized_headline"] = _headline(row)
        published = _clock(row["published_utc"])
        first_seen = _clock(row["first_seen_utc"])
        event_known = max([published, first_seen] + [_clock(row[key]) for key in ("causal_known_utc", "detail_available_utc", "numeric_causal_known_utc", "publication_clock_known_utc") if row.get(key)])
        known = max(event_known, _clock(row["observed_available_utc"])) if row.get("observed_available_utc") else event_known
        limit = _number(row.get("forward_timeliness_limit_minutes"), 0, 30)
        horizon = _number(row.get("estimated_reaction_horizon_minutes"), 1, 1440)
        # The expiry derives from this member's original causal clock. A later
        # corroborator can never renew or extend it.
        expires = event_known + dt.timedelta(minutes=horizon)
        evidence.update(published_utc=published.isoformat(), first_seen_utc=first_seen.isoformat(), original_event_known_utc=event_known.isoformat(), available_utc=known.isoformat(), expires_utc=expires.isoformat(), arrival_lag_minutes=(known-published).total_seconds()/60)
        if known > as_of:
            evidence["reasons"].append("member_not_yet_available")
        if as_of > expires:
            evidence["reasons"].append("member_reaction_window_expired")
        if row.get("forward_signal_timely") is not True or known - published > dt.timedelta(minutes=limit):
            evidence["reasons"].append("member_arrived_late")
        for key in ("reports_prior_market_move", "context_only", "non_catalyst_context", "secondary_analysis_context", "source_listing_bootstrap", "directional_research_only", "detail_enrichment_research_only"):
            if row.get(key): evidence["reasons"].append(key)
        scores = row.get("currency_scores")
        if not isinstance(scores, dict) or not 1 <= len(scores) <= 32:
            raise ValueError("member_no_publishable_currency_scores")
        for currency, score in scores.items():
            if not isinstance(currency, str) or re.fullmatch(r"[A-Z]{3}", currency) is None:
                raise ValueError("member_currency_invalid")
            _number(score, -1, 1)
        if not any(scores.values()):
            raise ValueError("member_zero_currency_scores")
        _number(row.get("source_quality"), 0, 1)
        _number(row.get("directional_confidence"), 0, 1)
    except (ValueError, TypeError, OverflowError) as exc:
        evidence["reasons"].append(str(exc)[:160])
    evidence["admitted"] = not evidence["reasons"]
    return row, evidence


def _context(topic, reason):
    result = copy.deepcopy(topic)
    if result.get("currency_scores"):
        result["research_currency_scores"] = copy.deepcopy(result["currency_scores"])
    result.update(currency_scores={}, directional_bias={}, directional_publish_eligible=False,
                  directional_evidence=False, directional_source_grade="member_guard_context",
                  directional_candidate_source_count=0, directional_verified_source=False,
                  forward_signal_timely=False, context_only=True, context_reason=reason,
                  relevant=False, direction_available_utc=None, direction_expires_utc=None,
                  forward_source_count=0, forward_corroboration_count=0,
                  classification_version=CLASSIFICATION_VERSION, research_only=True,
                  execution_eligible=False, can_place_orders=False)
    result.pop("causal_aggregation_guard", None)
    return result


def guard_topic(topic, members, *, as_of):
    """Return a new guarded topic; preserve raw arrival clocks and candidates.

    ``members=None`` is for retained, already-clustered topics. Only topics with
    verifiable member evidence can retain a forward direction in that case.
    """
    as_of = _asof(as_of)
    if not isinstance(topic, dict): raise ValueError("topic_object_required")
    if members is None:
        try:
            validate_guarded_topic(topic, as_of=as_of)
            sealed = topic["causal_aggregation_guard"]
            return guard_topic(sealed["original_topic"], sealed["members"], as_of=as_of)
        except (ValueError, TypeError, KeyError):
            return _context(topic, "guarded_member_evidence_unavailable")
    original = copy.deepcopy(topic)
    original.pop("causal_aggregation_guard", None)
    result = _context(original, "member_guard_no_eligible_same_claim_support")
    if not isinstance(members, (list, tuple)) or not 1 <= len(members) <= MAX_MEMBERS:
        return _context(original, "member_evidence_bound_or_missing")
    try:
        evidence_bytes = len(_canonical(members))
    except (ValueError, TypeError):
        return _context(original, "member_evidence_not_finite_json")
    if evidence_bytes > MAX_EVIDENCE_BYTES:
        return _context(original, "member_evidence_byte_bound")
    compact, evidence = [], []
    for member in members:
        row, view = _member_view(member if isinstance(member, dict) else {}, as_of)
        compact.append(row); evidence.append(view)
    identities = Counter(row["event_id"] for row in compact if isinstance(row.get("event_id"),str))
    for view in evidence:
        if isinstance(view["event_id"],str) and identities[view["event_id"]] > 1:
            view["admitted"] = False; view["reasons"].append("duplicate_member_identity")
    eligible = [(row, view) for row, view in zip(compact, evidence) if view["admitted"]]
    if any(value > 1 for value in identities.values()):
        eligible = []
    # Collapse both repeated publishers and exact syndicated headlines. Do not
    # let a second feed or a second site inflate confidence or support counts.
    representatives = []
    parents = {view["publisher_identity"]:view["publisher_identity"] for _,view in eligible}
    def find(value):
        while parents[value] != value:
            value = parents[value]
        return value
    first_publisher_by_headline = {}
    for _,view in eligible:
        publisher,headline=view["publisher_identity"],view["normalized_headline"]
        prior=first_publisher_by_headline.setdefault(headline,publisher)
        left,right=find(prior),find(publisher)
        if left!=right: parents[max(left,right)]=min(left,right)
    groups = set()
    for row, view in sorted(eligible, key=lambda item: (item[1]["available_utc"], item[0]["event_id"])):
        group=find(view["publisher_identity"])
        if group in groups:
            view["admitted"] = False; view["reasons"].append("duplicate_publisher_or_syndicated_headline"); continue
        groups.add(group)
        representatives.append((row, view))
    coherent = bool(representatives) and all(same_claim(a[0], b[0]) for i, a in enumerate(representatives) for b in representatives[i+1:])
    verified = any(row.get("source_verified") is True for row, _ in representatives)
    enough = verified or len(representatives) >= 2
    if not representatives:
        result["context_reason"] = (original.get("context_reason") if not original.get("currency_scores") else None) or "member_no_eligible_contributors"
    elif not coherent:
        result["context_reason"] = "member_claims_not_coherent"
    elif not enough:
        result["context_reason"] = "member_independent_corroboration_missing"
    else:
        scores = {}; denominators = {}
        for row, _ in representatives:
            weight = max(0.1, float(row["source_quality"]))
            for currency, score in row["currency_scores"].items():
                scores[currency] = scores.get(currency, 0.0) + weight * float(score)
                denominators[currency] = denominators.get(currency, 0.0) + weight
        scores = {currency: round(value / denominators[currency], 6) for currency, value in sorted(scores.items())}
        available = max(_clock(view["available_utc"]) for _, view in representatives)
        expires = min(_clock(view["expires_utc"]) for _, view in representatives)
        representative = max(representatives,key=lambda item:(item[0].get("source_verified") is True,float(item[0]["source_quality"])))[0]
        direct = {currency for row,_ in representatives for currency in (row.get("direct_currencies") or []) if currency in scores}
        inferred = {currency for row,_ in representatives for currency in (row.get("inferred_currencies") or []) if currency in scores} - direct
        result.update(currency_scores=scores, directional_bias={c:("BULLISH" if v>0 else "BEARISH") for c,v in scores.items() if v},
                      directional_publish_eligible=bool(any(scores.values())), directional_evidence=bool(any(scores.values())),
                      directional_source_grade="member_guard_verified" if verified else "member_guard_corroborated_secondary",
                      directional_candidate_source_count=len(representatives), directional_verified_source=verified,
                      forward_signal_timely=True, context_only=False, context_reason="", relevant=True,
                      causal_known_utc=available.isoformat(), direction_available_utc=available.isoformat(),
                      direction_expires_utc=expires.isoformat(), forward_source_count=len(representatives),
                      forward_corroboration_count=max(0,len(representatives)-1),
                      source_verified=verified, source_direct=any(row.get("source_direct") is True for row,_ in representatives),
                      direct_currencies=sorted(direct), inferred_currencies=sorted(inferred),
                      source_ids=sorted({row["source_id"] for row,_ in representatives if isinstance(row.get("source_id"),str)}),
                      source_names=sorted({row["source_name"] for row,_ in representatives if isinstance(row.get("source_name"),str)}),
                      distinct_source_count=len(representatives), corroboration_count=max(0,len(representatives)-1),
                      availability_lag_minutes=max(view["arrival_lag_minutes"] for _,view in representatives))
        for key in ("headline","source_name","source_url","scope","category"):
            result[key]=copy.deepcopy(representative.get(key))
        if not any(scores.values()):
            result=_context(result,"member_directions_cancelled")
    # Store the original aggregate separately so the independent adapter can
    # rebuild admission, compare every public direction field and recheck age.
    original_compact={key:copy.deepcopy(original.get(key)) for key in PROJECTED_KEYS}
    guard = {"version":GUARD_VERSION, "evaluated_utc":as_of.isoformat(), "original_topic":original_compact,
             "members":compact, "member_evidence":evidence, "same_claim_support":coherent,
             "projected":{key:result.get(key) for key in PROJECTED_KEYS}}
    if len(_canonical(guard)) > MAX_EVIDENCE_BYTES:
        return _context(original, "guarded_evidence_byte_bound")
    guard["sha256"] = hashlib.sha256(_canonical(guard)).hexdigest()
    result["causal_aggregation_guard"] = guard
    return result


def validate_guarded_topic(topic, *, as_of):
    """Return current admission evidence or raise for missing/tampered input.

    The caller must additionally bind the containing snapshot's first-observed
    clock, producer source hashes and freshness. This seal is an integrity
    checksum, not external authentication or proof of news truth.
    """
    as_of = _asof(as_of)
    guard = topic.get("causal_aggregation_guard") if isinstance(topic, dict) else None
    if not isinstance(guard, dict) or guard.get("version") != GUARD_VERSION:
        raise ValueError("guarded_member_evidence_missing")
    if len(_canonical(guard)) > MAX_EVIDENCE_BYTES:
        raise ValueError("guarded_evidence_byte_bound")
    material = {key:value for key,value in guard.items() if key != "sha256"}
    if hashlib.sha256(_canonical(material)).hexdigest() != guard.get("sha256"):
        raise ValueError("guarded_evidence_hash_mismatch")
    evaluated = _clock(guard.get("evaluated_utc"))
    if evaluated > as_of:
        raise ValueError("guarded_evidence_future")
    rebuilt = guard_topic(guard["original_topic"], guard["members"], as_of=evaluated)
    if rebuilt.get("causal_aggregation_guard") != guard:
        raise ValueError("guarded_evidence_replay_mismatch")
    if any(topic.get(key) != guard["projected"].get(key) for key in PROJECTED_KEYS):
        raise ValueError("guarded_public_fields_mismatch")
    current = guard_topic(guard["original_topic"], guard["members"], as_of=as_of)
    return {"version":GUARD_VERSION, "status":"directional" if current.get("directional_publish_eligible") else "context",
            "currency_scores":current["currency_scores"], "reason":current.get("context_reason"),
            "direction_available_utc":current.get("direction_available_utc"),
            "direction_expires_utc":current.get("direction_expires_utc"),
            "forward_source_count":current.get("forward_source_count"), "guard_sha256":guard["sha256"],
            "evaluated_utc":guard["evaluated_utc"], "research_only":True, "execution_eligible":False}


def build_current_news_snapshot(topics, *, as_of):
    """Bounded current proofs, published once rather than repeated per pair.

    The producer adds its actual post-computation generated_utc and source
    hashes. A current topic missing its proof makes the whole snapshot
    unavailable, so absence of validated evidence cannot masquerade as neutral.
    """
    as_of=_asof(as_of)
    result={"schema_version":"joint_news_current_v1_20260907", "as_of_utc":as_of.isoformat(),
            "classification_version":CLASSIFICATION_VERSION,"guard_version":GUARD_VERSION,
            "status":"current","news_state":"no_current_evidence","topics":[],
            "topic_count":0,"directional_topic_count":0,"context_topic_count":0,
            "duplicate_topics_collapsed":0,"errors":[],"research_only":True,
            "execution_eligible":False,"can_place_orders":False,"can_promote":False,
            "limits":{"maximum_topics":MAX_CURRENT_TOPICS,"maximum_bytes":MAX_CURRENT_SNAPSHOT_BYTES}}
    seen={}
    if not isinstance(topics,(list,tuple)) or len(topics)>10000:
        result.update(status="unavailable",news_state="unavailable",errors=["topic_input_bound"])
        return result
    for topic in topics:
        try:
            published=_clock(topic.get("published_utc"))
            window=_number(topic.get("post_window_minutes"),1,1440)
            if published>as_of or (as_of-published).total_seconds()>window*60:
                continue
            ident=topic.get("topic_id")
            if not isinstance(ident,str) or not 1<=len(ident)<=200:
                raise ValueError("current_topic_identity_invalid")
            validate_guarded_topic(topic,as_of=as_of)
            current=guard_topic(topic,None,as_of=as_of)
            digest=hashlib.sha256(_canonical(current)).hexdigest()
            if ident in seen:
                if seen[ident]!=digest:raise ValueError("conflicting_current_topic_identity")
                result["duplicate_topics_collapsed"]+=1
                continue
            seen[ident]=digest
            result["topics"].append(current)
            if len(result["topics"])>MAX_CURRENT_TOPICS:
                raise ValueError("current_topic_count_bound")
        except (ValueError,TypeError,KeyError,AttributeError) as exc:
            result["errors"].append(str(exc)[:160])
            if len(result["errors"])>=32:break
    if result["errors"]:
        result.update(status="unavailable",news_state="unavailable",topics=[])
        return result
    result["topics"].sort(key=lambda item:item["topic_id"])
    result["topic_count"]=len(result["topics"])
    result["directional_topic_count"]=sum(bool(item["directional_publish_eligible"]) for item in result["topics"])
    result["context_topic_count"]=result["topic_count"]-result["directional_topic_count"]
    result["news_state"]="directional" if result["directional_topic_count"] else "context_only" if result["topic_count"] else "no_current_evidence"
    if len(_canonical(result))>MAX_CURRENT_SNAPSHOT_BYTES:
        result.update(status="unavailable",news_state="unavailable",topics=[],topic_count=0,directional_topic_count=0,context_topic_count=0,errors=["current_snapshot_byte_bound"])
    return result
