"""Compare frozen research arms on immutable, factor-deduplicated live movers.

This is an explanation/alignment diagnostic, not a predictor backtest.  The
input rows exist because an executable move already cleared its spread, so no
result from this module may promote, authorize, or place a trade.  Its purpose
is to expose abstention, opposition, and duplicate-decision behavior while the
separate prospective forecast ledgers continue collecting genuine proof.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_major_move_gap_census as census


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "move_first_live_arm_alignment_v1"
)
DEFAULT_CONFIG = ROOT / "config" / "move_first_live_arm_alignment_v1_20260901.json"
DEFAULT_SOURCE_DATABASE = STATE / "move_first_live_case_capture_v4_20260901.sqlite"
DEFAULT_JSON_REPORT = REPORT_ROOT / "MOVE_FIRST_LIVE_ARM_ALIGNMENT_CURRENT.json"
DEFAULT_MD_REPORT = REPORT_ROOT / "MOVE_FIRST_LIVE_ARM_ALIGNMENT_CURRENT.md"
CAUSAL_GAP_TAXONOMY_CONTRACT_ID = (
    "move_first_causal_gap_taxonomy_v4_narrative_family_concentration_exact_retained_clock_20260901"
)
NARRATIVE_FAMILY_CONTRACT_ID = (
    "retained_story_narrative_family_v1_conservative_time_bounded_headline_component_20260901"
)
FACTOR_SUPPORT_DIAGNOSTIC_CONTRACT_ID = (
    "move_first_factor_support_v1_positive_aligned_primary_and_unambiguous_20260901"
)
NARRATIVE_FAMILY_MAX_SEPARATION_SEC = 6 * 60 * 60
NARRATIVE_FAMILY_MIN_SHARED_TERMS = 3
NARRATIVE_FAMILY_MIN_OVERLAP_COEFFICIENT = 0.5

_HEADLINE_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "before",
        "by",
        "central",
        "currency",
        "economy",
        "for",
        "forex",
        "from",
        "in",
        "is",
        "it",
        "latest",
        "market",
        "markets",
        "moving",
        "near",
        "news",
        "of",
        "on",
        "or",
        "policy",
        "rate",
        "rates",
        "report",
        "said",
        "says",
        "the",
        "to",
        "today",
        "update",
        "what",
        "with",
    }
)
_HEADLINE_TERM_ALIASES = {
    "attacks": "attack",
    "hikes": "hike",
    "hitting": "hit",
    "hits": "hit",
    "projectiles": "projectile",
    "strikes": "hit",
    "striking": "hit",
    "struck": "hit",
    "tankers": "tanker",
    "warned": "warn",
    "warning": "warn",
    "warns": "warn",
    "yields": "yield",
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def load_config(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "schema_version",
        "audit_id",
        "source_cohort_id",
        "source_ledger_schema_version",
        "source_database_contract_id",
        "factor_episode_contract_id",
        "deduplication_rule",
        "minimum_absolute_pair_score",
        "pair_score_formula",
        "forward_shadow_arms",
        "label_derived_forward_shadow_arms",
        "narrative_score_arms",
    }
    missing = sorted(required - set(value))
    if missing:
        raise ValueError(f"config missing required fields: {missing}")
    if value["schema_version"] != "move_first_live_arm_alignment_config_v1":
        raise ValueError("unexpected alignment config schema")
    if value["pair_score_formula"] != "base_currency_score_minus_quote_currency_score":
        raise ValueError("pair score formula must remain base minus quote")
    if float(value["minimum_absolute_pair_score"]) < 0.0:
        raise ValueError("minimum pair-score magnitude must be nonnegative")
    if len(set(value["forward_shadow_arms"])) != len(value["forward_shadow_arms"]):
        raise ValueError("duplicate forward-shadow arm")
    label_derived = set(value["label_derived_forward_shadow_arms"])
    if not label_derived <= set(value["forward_shadow_arms"]):
        raise ValueError("label-derived arms must be declared forward-shadow arms")
    if label_derived != {
        "technical_continuation",
        "broad_context_plus_continuation",
    }:
        raise ValueError("realized-direction-derived arms must remain explicit controls")
    if len(set(value["narrative_score_arms"])) != len(value["narrative_score_arms"]):
        raise ValueError("duplicate narrative-score arm")
    required_false = (
        "predictive_backtest_eligible",
        "promotion_eligible",
        "execution_eligible",
        "can_authorize",
        "can_place_orders",
    )
    if value.get("selection_conditioned_on_realized_executable_move") is not True:
        raise ValueError("move-first selection conditioning must be explicit")
    if value.get("research_only") is not True:
        raise ValueError("alignment audit must remain research only")
    if any(value.get(key) is not False for key in required_false):
        raise ValueError("alignment audit must remain nonexecuting and nonpromotional")
    return value


def _open_read_only(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"source capture database missing: {path}")
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("BEGIN")
    return connection


def _manifest(connection: sqlite3.Connection) -> dict[str, str]:
    return {
        str(row["key"]): str(row["value"])
        for row in connection.execute("SELECT key, value FROM manifest")
    }


def _verify_source_manifest(
    observed: Mapping[str, str], config: Mapping[str, Any]
) -> None:
    expected = {
        "schema_version": str(config["source_ledger_schema_version"]),
        "cohort_id": str(config["source_cohort_id"]),
        "source_database_contract_id": str(config["source_database_contract_id"]),
        "factor_episode_contract_id": str(config["factor_episode_contract_id"]),
        "historical_rows_imported": "0",
        "execution_eligible": "false",
    }
    failures = [key for key, expected_value in expected.items() if observed.get(key) != expected_value]
    if failures:
        raise RuntimeError(f"source capture manifest mismatch: {failures}")


def _resolve_root(value: str, edges: Mapping[str, str]) -> str:
    current = value
    seen: set[str] = set()
    while current in edges:
        if current in seen:
            raise RuntimeError(f"factor root merge cycle at {current}")
        seen.add(current)
        current = edges[current]
    return current


def _direction(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return 1 if value > 0 else -1 if value < 0 else 0
    normalized = str(value or "").strip().lower()
    if normalized in {"up", "long", "buy", "+1", "positive"}:
        return 1
    if normalized in {"down", "short", "sell", "-1", "negative"}:
        return -1
    if normalized in {"", "neutral", "flat", "abstain", "0", "none"}:
        return 0
    raise ValueError(f"unsupported direction: {value!r}")


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result != result or result in {float("inf"), float("-inf")}:
        return None
    return result


def _factor_support_diagnostic(
    payload: Mapping[str, Any], primary_token: str
) -> dict[str, Any]:
    """Check whether the frozen primary factor actually supports the move.

    The upstream factor contract selects the better of the two directionally
    compatible currency tokens. A large margin between two negative scores
    can therefore look non-ambiguous even though neither currency-strength
    estimate agrees with the observed move. This diagnostic fails those
    cases closed without rewriting their immutable membership.
    """

    method = str(payload.get("factor_primary_method") or "")
    raw_scores = payload.get("factor_primary_scores_bps") or {}
    scores = {
        str(token): value
        for token, raw in raw_scores.items()
        if (value := _safe_float(raw)) is not None
    }
    primary = str(primary_token or payload.get("factor_primary_token") or "")
    upstream_ambiguous = payload.get("factor_primary_ambiguous") is True
    winner = None
    best_score = None
    primary_score = scores.get(primary)
    if scores:
        winner = sorted(scores, key=lambda token: (-scores[token], token))[0]
        best_score = scores[winner]

    if not method.startswith("causal_all68_currency_strength"):
        status = "noncausal_factor_assignment"
    elif len(scores) < 2:
        status = "missing_factor_scores"
    elif primary not in scores:
        status = "primary_missing_from_scores"
    elif primary != winner:
        status = "primary_not_best_scored"
    elif best_score is None or best_score <= 0.0:
        status = "no_move_aligned_positive_factor"
    elif upstream_ambiguous:
        status = "ambiguous_primary_margin"
    else:
        status = "supported"

    return {
        "contract_id": FACTOR_SUPPORT_DIAGNOSTIC_CONTRACT_ID,
        "status": status,
        "support_qualified": status == "supported",
        "factor_primary_token": primary,
        "best_scored_token": winner,
        "best_aligned_score_bps": best_score,
        "primary_score_bps": primary_score,
        "upstream_primary_ambiguous": upstream_ambiguous,
        "minimum_required_aligned_score_bps": 0.0,
        "membership_rewritten": False,
    }


def _load_verified_cases(
    connection: sqlite3.Connection,
    config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        raise RuntimeError(f"source capture integrity failed: {integrity}")
    _verify_source_manifest(_manifest(connection), config)

    memberships: dict[str, sqlite3.Row] = {}
    for row in connection.execute("SELECT * FROM factor_memberships"):
        source_case_id = str(row["source_case_id"])
        if str(row["membership_sha256"]) != sha256_text(str(row["membership_json"])):
            raise RuntimeError(f"membership hash mismatch: {source_case_id}")
        if str(row["factor_episode_contract_id"]) != str(config["factor_episode_contract_id"]):
            raise RuntimeError(f"factor contract mismatch: {source_case_id}")
        memberships[source_case_id] = row

    edges: dict[str, str] = {}
    merge_count = 0
    for row in connection.execute("SELECT * FROM factor_root_merges"):
        merge_count += 1
        if str(row["merge_sha256"]) != sha256_text(str(row["merge_json"])):
            raise RuntimeError(f"root-merge hash mismatch: {row['merge_id']}")
        source = str(row["from_root_id"])
        target = str(row["into_root_id"])
        if source in edges and edges[source] != target:
            raise RuntimeError(f"conflicting root merge: {source}")
        edges[source] = target

    cases: list[dict[str, Any]] = []
    for row in connection.execute(
        "SELECT * FROM cases ORDER BY first_recorded_utc, source_case_id"
    ):
        case_id = str(row["source_case_id"])
        if str(row["case_sha256"]) != sha256_text(str(row["case_json"])):
            raise RuntimeError(f"case hash mismatch: {case_id}")
        membership = memberships.get(case_id)
        if membership is None:
            raise RuntimeError(f"case missing factor membership: {case_id}")
        payload = json.loads(str(row["case_json"]))
        if str(payload.get("case_id") or "") != case_id:
            raise RuntimeError(f"case identity mismatch: {case_id}")
        if str(payload.get("instrument") or "") != str(row["instrument"]):
            raise RuntimeError(f"case instrument mismatch: {case_id}")
        if payload.get("execution_eligible") is not False:
            raise RuntimeError(f"source case is not explicitly nonexecuting: {case_id}")
        episode = str(membership["factor_episode_id"])
        cases.append(
            {
                "source_case_id": case_id,
                "first_recorded_utc": str(row["first_recorded_utc"]),
                "instrument": str(row["instrument"]),
                "factor_episode_id": episode,
                "resolved_factor_episode_id": _resolve_root(episode, edges),
                "factor_primary_token": str(membership["factor_primary_token"]),
                "payload": payload,
            }
        )
    if len(cases) != len(memberships):
        raise RuntimeError("case and factor-membership counts differ")
    for root in list(edges):
        _resolve_root(root, edges)
    return cases, merge_count


def _select_episode_cases(cases: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in cases:
        grouped.setdefault(str(row["resolved_factor_episode_id"]), []).append(row)
    selected: list[dict[str, Any]] = []
    for root, members in sorted(grouped.items()):
        ordered = sorted(
            members,
            key=lambda item: (str(item["first_recorded_utc"]), str(item["source_case_id"])),
        )
        chosen = dict(ordered[0])
        chosen["root_member_count"] = len(ordered)
        chosen["any_upstream_representative"] = any(
            member["payload"].get("factor_representative") is True for member in ordered
        )
        selected.append(chosen)
    selected.sort(key=lambda item: (item["first_recorded_utc"], item["source_case_id"]))
    return selected


def _arm_values(
    payload: Mapping[str, Any], config: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    frozen = payload.get("forward_shadow_arms") or {}
    for arm in config["forward_shadow_arms"]:
        raw = frozen.get(arm)
        output[f"forward_shadow::{arm}"] = {
            "direction": _direction(raw),
            "raw_value": raw,
        }

    narrative = payload.get("continuous_narrative_state") or {}
    base_state = narrative.get("base_currency_state") or {}
    quote_state = narrative.get("quote_currency_state") or {}
    base_scores = base_state.get("model_scores") or {}
    quote_scores = quote_state.get("model_scores") or {}
    threshold = float(config["minimum_absolute_pair_score"])
    state_available = narrative.get("state_available") is True
    for arm in config["narrative_score_arms"]:
        base_score = _safe_float(base_scores.get(arm)) if state_available else None
        quote_score = _safe_float(quote_scores.get(arm)) if state_available else None
        pair_score = (
            None if base_score is None or quote_score is None else base_score - quote_score
        )
        direction = 0
        if pair_score is not None and abs(pair_score) >= threshold:
            direction = 1 if pair_score > 0 else -1
        output[f"currency_score::{arm}"] = {
            "direction": direction,
            "raw_value": pair_score,
            "base_score": base_score,
            "quote_score": quote_score,
        }
    return output


def _round(value: float) -> float:
    return round(float(value), 9)


def _arm_metric(
    rows: Sequence[Mapping[str, Any]],
    *,
    clock_classification: str,
    interpretation: str,
) -> dict[str, Any]:
    """Build one deterministic move-conditioned arm summary.

    The legacy all-episode view and the factor-support-qualified subset share
    this exact calculation. The subset is therefore a transparent diagnostic,
    not a second scoring system.
    """

    signaled = [item for item in rows if int(item["direction"]) != 0]
    correct = [
        item
        for item in signaled
        if int(item["direction"]) == int(item["actual"])
    ]
    wrong = [
        item
        for item in signaled
        if int(item["direction"]) != int(item["actual"])
    ]
    aligned_room = sum(float(item["net_room_pips"]) for item in correct)
    opposed_room = sum(float(item["net_room_pips"]) for item in wrong)
    signature_payload = [
        [str(item["episode"]), int(item["direction"])] for item in rows
    ]
    signature = sha256_text(canonical_json(signature_payload))
    return {
        "clock_classification": clock_classification,
        "eligible_as_pre_move_directional_diagnostic": (
            clock_classification == "available_at_or_before_move_start"
        ),
        "effective_episode_count": len(rows),
        "signaled_episode_count": len(signaled),
        "abstained_episode_count": len(rows) - len(signaled),
        "correct_direction_count": len(correct),
        "wrong_direction_count": len(wrong),
        "directional_coverage_pct": (
            _round(100.0 * len(signaled) / len(rows)) if rows else None
        ),
        "directional_accuracy_pct": (
            _round(100.0 * len(correct) / len(signaled)) if signaled else None
        ),
        "aligned_observed_net_room_pips": _round(aligned_room),
        "opposed_observed_net_room_pips": _round(opposed_room),
        "signed_alignment_room_pips": _round(aligned_room - opposed_room),
        "decision_signature_sha256": signature,
        "interpretation": interpretation,
    }


def _utc_datetime(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _headline_terms(value: Any) -> frozenset[str]:
    """Return conservative lexical anchors for diagnostic story-family matching."""

    headline = str(value or "").casefold().rsplit(" - ", 1)[0]
    terms: set[str] = set()
    for raw_term in re.findall(r"[^\W_]+", headline, flags=re.UNICODE):
        term = _HEADLINE_TERM_ALIASES.get(raw_term, raw_term)
        if len(term) < 3 or term in _HEADLINE_STOPWORDS:
            continue
        terms.add(term)
    return frozenset(terms)


def _narrative_similarity_edge(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> bool:
    """Conservatively link likely syndication variants in retained data only."""

    left_cluster = str(left.get("story_cluster_key") or "")
    right_cluster = str(right.get("story_cluster_key") or "")
    if left_cluster and left_cluster == right_cluster:
        return True

    left_type = str(left.get("event_type") or "")
    right_type = str(right.get("event_type") or "")
    if not left_type or left_type != right_type:
        return False
    left_time = _utc_datetime(left.get("effective_from_utc"))
    right_time = _utc_datetime(right.get("effective_from_utc"))
    if left_time is None or right_time is None:
        return False
    separation = abs((left_time - right_time).total_seconds())
    if separation > NARRATIVE_FAMILY_MAX_SEPARATION_SEC:
        return False

    left_terms = _headline_terms(left.get("headline"))
    right_terms = _headline_terms(right.get("headline"))
    if not left_terms or not right_terms:
        return False
    shared = len(left_terms & right_terms)
    overlap = shared / min(len(left_terms), len(right_terms))
    return bool(
        shared >= NARRATIVE_FAMILY_MIN_SHARED_TERMS
        and overlap >= NARRATIVE_FAMILY_MIN_OVERLAP_COEFFICIENT
    )


def _assign_narrative_families(
    stories: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Build diagnostic-only connected components over immutable story records.

    Components never establish independent proof.  They only make concentration
    visible when an upstream story-cluster contract splits syndicated variants.
    """

    unique: dict[str, Mapping[str, Any]] = {}
    for story in stories:
        story_key = str(story.get("story_key") or "")
        if story_key:
            unique.setdefault(story_key, story)
    keys = sorted(unique)
    parent = {key: key for key in keys}

    def find(key: str) -> str:
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root == right_root:
            return
        if left_root < right_root:
            parent[right_root] = left_root
        else:
            parent[left_root] = right_root

    similarity_edges: set[tuple[str, str]] = set()
    for index, left_key in enumerate(keys):
        for right_key in keys[index + 1 :]:
            if _narrative_similarity_edge(unique[left_key], unique[right_key]):
                union(left_key, right_key)
                if (
                    str(unique[left_key].get("story_cluster_key") or "")
                    != str(unique[right_key].get("story_cluster_key") or "")
                ):
                    similarity_edges.add((left_key, right_key))

    components: dict[str, list[str]] = {}
    for key in keys:
        components.setdefault(find(key), []).append(key)

    assignments: dict[str, dict[str, Any]] = {}
    for members in components.values():
        ordered = sorted(
            members,
            key=lambda key: (
                str(unique[key].get("effective_from_utc") or "~"),
                key,
            ),
        )
        representative_key = ordered[0]
        family_key = "narrative_family:" + sha256_text(
            canonical_json([NARRATIVE_FAMILY_CONTRACT_ID, representative_key])
        )[:24]
        clusters = {
            str(unique[key].get("story_cluster_key") or "") for key in members
        }
        component_edges = sum(
            1
            for left, right in similarity_edges
            if left in members and right in members
        )
        method = (
            "time_bounded_headline_similarity_component"
            if component_edges
            else "upstream_story_cluster_or_singleton"
        )
        for key in members:
            assignments[key] = {
                "narrative_family_key": family_key,
                "narrative_family_contract_id": NARRATIVE_FAMILY_CONTRACT_ID,
                "narrative_family_method": method,
                "narrative_family_representative_story_key": representative_key,
                "narrative_family_component_story_count": len(members),
                "narrative_family_component_upstream_cluster_count": len(clusters),
                "narrative_family_similarity_edge_count": component_edges,
            }
    return assignments


def _causal_gap_diagnostic(
    payload: Mapping[str, Any],
    arms: Mapping[str, Mapping[str, Any]],
    *,
    actual: int,
    minimum_absolute_pair_score: float,
) -> dict[str, Any]:
    """Classify why the strict pre-move arm did or did not explain a mover.

    This uses only material already retained with the move-first case.  It does
    not acquire later articles and cannot turn the move-conditioned sample into
    a predictor backtest.
    """

    strict = int(
        (arms.get("forward_shadow::strict_forward_direction") or {}).get(
            "direction"
        )
        or 0
    )
    research = int(
        (arms.get("currency_score::research_semantic_v1") or {}).get(
            "direction"
        )
        or 0
    )
    broad = int(
        (arms.get("forward_shadow::broad_context_direction") or {}).get(
            "direction"
        )
        or 0
    )
    exclusions: dict[str, int] = {}
    raw_exclusions = payload.get("strict_forward_exclusions") or {}
    if isinstance(raw_exclusions, Mapping):
        for key, value in sorted(raw_exclusions.items()):
            try:
                count = int(value)
            except (TypeError, ValueError):
                continue
            if count > 0:
                exclusions[str(key)] = count

    move_start = _utc_datetime(payload.get("start_utc"))
    candidates_by_story_key: dict[str, dict[str, Any]] = {}
    stories = payload.get("recent_context_stories") or []
    if isinstance(stories, Sequence) and not isinstance(stories, (str, bytes)):
        for story in stories:
            if not isinstance(story, Mapping):
                continue
            if story.get("forward_signal_timely") is not True:
                continue
            if str(story.get("direction_disposition") or "") != "directional":
                continue
            score = _safe_float(story.get("pair_score"))
            if score is None or abs(score) < minimum_absolute_pair_score:
                continue
            effective = _utc_datetime(story.get("effective_from_utc"))
            if move_start is not None and (effective is None or effective > move_start):
                continue
            direction = 1 if score > 0 else -1
            source_event_id = str(story.get("source_event_id") or "")
            source_id = str(story.get("source_id") or "")
            story_cluster_id = str(story.get("story_cluster_id") or "")
            headline = str(story.get("headline") or "")
            effective_text = (
                effective.isoformat().replace("+00:00", "Z")
                if effective is not None
                else ""
            )
            if source_event_id:
                story_key = f"source_event:{source_event_id}"
                story_key_type = "source_event_id"
            else:
                story_key = "fallback:" + sha256_text(
                    canonical_json([source_id, effective_text, headline])
                )[:24]
                story_key_type = "source_time_headline_sha256"
            candidate = {
                "story_key": story_key,
                "story_key_type": story_key_type,
                "story_cluster_id": story_cluster_id,
                "story_cluster_key": (
                    f"story_cluster:{story_cluster_id}"
                    if story_cluster_id
                    else story_key
                ),
                "story_cluster_key_type": (
                    "story_cluster_id" if story_cluster_id else "story_key_fallback"
                ),
                "source_event_id": source_event_id,
                "source_id": source_id,
                "event_type": str(story.get("event_type") or ""),
                "headline": headline,
                "effective_from_utc": effective_text,
                "pair_score": score,
                "direction": direction,
                "aligned": direction == actual,
                "directional_publish_eligible": (
                    story.get("directional_publish_eligible") is True
                ),
            }
            previous = candidates_by_story_key.get(story_key)
            if previous is None or (
                candidate["effective_from_utc"],
                candidate["directional_publish_eligible"],
            ) > (
                previous["effective_from_utc"],
                previous["directional_publish_eligible"],
            ):
                candidates_by_story_key[story_key] = candidate
    candidates = list(candidates_by_story_key.values())
    candidates.sort(
        key=lambda item: (item["effective_from_utc"], item["source_event_id"])
    )
    aligned_story_count = sum(1 for item in candidates if item["aligned"])
    opposed_story_count = len(candidates) - aligned_story_count

    if strict:
        state = "strict_direction_aligned" if strict == actual else "strict_direction_opposed"
    elif candidates:
        if aligned_story_count and opposed_story_count:
            state = "strict_abstain_research_story_conflict"
        elif aligned_story_count:
            state = "strict_abstain_research_story_aligned"
        else:
            state = "strict_abstain_research_story_opposed"
    elif research:
        state = (
            "strict_abstain_research_semantic_aligned"
            if research == actual
            else "strict_abstain_research_semantic_opposed"
        )
    elif broad:
        state = (
            "strict_abstain_broad_context_aligned"
            if broad == actual
            else "strict_abstain_broad_context_opposed"
        )
    else:
        state = "strict_abstain_no_direction"

    return {
        "contract_id": CAUSAL_GAP_TAXONOMY_CONTRACT_ID,
        "state": state,
        "strict_direction": strict,
        "research_semantic_direction": research,
        "broad_context_direction": broad,
        "strict_forward_independent_story_count": int(
            payload.get("strict_forward_independent_story_count") or 0
        ),
        "strict_abstention_reasons": exclusions,
        "retained_forward_research_story_count": len(candidates),
        "retained_forward_aligned_story_count": aligned_story_count,
        "retained_forward_opposed_story_count": opposed_story_count,
        "retained_forward_research_stories": candidates,
        "nearest_retained_forward_research_story": (
            candidates[-1] if candidates else None
        ),
        "interpretation": (
            "move_conditioned_source_gap_diagnostic_not_prediction_or_trade_pnl"
        ),
    }


def build_audit(
    config_path: Path = DEFAULT_CONFIG,
    source_database: Path = DEFAULT_SOURCE_DATABASE,
) -> dict[str, Any]:
    config = load_config(config_path)
    connection = _open_read_only(source_database)
    try:
        cases, merge_count = _load_verified_cases(connection, config)
    finally:
        connection.close()
    selected = _select_episode_cases(cases)

    source_factor_support = [
        _factor_support_diagnostic(row["payload"], row["factor_primary_token"])
        for row in cases
    ]
    selected_factor_support = [
        _factor_support_diagnostic(row["payload"], row["factor_primary_token"])
        for row in selected
    ]
    source_factor_support_status_counts: dict[str, int] = {}
    selected_factor_support_status_counts: dict[str, int] = {}
    for diagnostic in source_factor_support:
        status = str(diagnostic["status"])
        source_factor_support_status_counts[status] = (
            source_factor_support_status_counts.get(status, 0) + 1
        )
    for diagnostic in selected_factor_support:
        status = str(diagnostic["status"])
        selected_factor_support_status_counts[status] = (
            selected_factor_support_status_counts.get(status, 0) + 1
        )
    unsupported_factor_examples = []
    for row, diagnostic in zip(cases, source_factor_support):
        if diagnostic["support_qualified"]:
            continue
        unsupported_factor_examples.append(
            {
                "source_case_id": row["source_case_id"],
                "first_recorded_utc": row["first_recorded_utc"],
                "instrument": row["instrument"],
                "move_direction": row["payload"].get("move_direction"),
                "executable_net_pips": row["payload"].get("executable_net_pips"),
                "factor_primary_token": row["factor_primary_token"],
                "status": diagnostic["status"],
                "best_scored_token": diagnostic["best_scored_token"],
                "best_aligned_score_bps": diagnostic["best_aligned_score_bps"],
                "primary_score_bps": diagnostic["primary_score_bps"],
            }
        )
        if len(unsupported_factor_examples) >= 20:
            break

    arm_names = [f"forward_shadow::{name}" for name in config["forward_shadow_arms"]]
    arm_names += [f"currency_score::{name}" for name in config["narrative_score_arms"]]
    observations: dict[str, list[dict[str, Any]]] = {name: [] for name in arm_names}
    episode_rows: list[dict[str, Any]] = []
    excluded_episodes = 0
    gap_state_counts: dict[str, int] = {}
    gap_reason_episode_counts: dict[str, int] = {}
    retained_forward_story_count = 0
    retained_forward_aligned_story_count = 0
    retained_forward_opposed_story_count = 0
    factor_episodes_with_retained_forward_story_count = 0
    factor_episodes_with_strict_independent_story_count = 0
    retained_story_episode_ids: dict[str, set[str]] = {}
    retained_story_aligned_links: dict[str, int] = {}
    retained_story_opposed_links: dict[str, int] = {}
    retained_story_metadata: dict[str, dict[str, Any]] = {}
    retained_source_ids: set[str] = set()
    retained_cluster_episode_ids: dict[str, set[str]] = {}
    retained_cluster_source_event_ids: dict[str, set[str]] = {}
    retained_cluster_aligned_links: dict[str, int] = {}
    retained_cluster_opposed_links: dict[str, int] = {}
    retained_cluster_metadata: dict[str, dict[str, Any]] = {}
    factor_episodes_with_multiple_retained_clusters_count = 0
    for row, factor_support in zip(selected, selected_factor_support):
        payload = row["payload"]
        actual = _direction(payload.get("move_direction"))
        if actual == 0:
            raise RuntimeError(f"captured mover lacks direction: {row['source_case_id']}")
        net_room = _safe_float(payload.get("executable_net_pips"))
        if net_room is None or net_room < 0.0:
            raise RuntimeError(f"captured mover lacks nonnegative net room: {row['source_case_id']}")
        eligible = payload.get("forward_shadow_eligible") is True
        if not eligible:
            excluded_episodes += 1
        arms = _arm_values(payload, config)
        compact_arms: dict[str, Any] = {}
        for arm in arm_names:
            value = arms[arm]
            direction = int(value["direction"]) if eligible else 0
            observation = {
                "episode": row["resolved_factor_episode_id"],
                "direction": direction,
                "actual": actual,
                "net_room_pips": net_room,
                "eligible": eligible,
                "factor_support_qualified": factor_support["support_qualified"],
                "raw_value": value.get("raw_value"),
            }
            observations[arm].append(observation)
            compact_arms[arm] = {
                "direction": direction,
                "raw_value": value.get("raw_value"),
            }
        causal_gap = _causal_gap_diagnostic(
            payload,
            arms,
            actual=actual,
            minimum_absolute_pair_score=float(
                config["minimum_absolute_pair_score"]
            ),
        )
        gap_state = str(causal_gap["state"])
        gap_state_counts[gap_state] = gap_state_counts.get(gap_state, 0) + 1
        for reason in causal_gap["strict_abstention_reasons"]:
            gap_reason_episode_counts[reason] = (
                gap_reason_episode_counts.get(reason, 0) + 1
            )
        retained_forward_story_count += int(
            causal_gap["retained_forward_research_story_count"]
        )
        retained_forward_aligned_story_count += int(
            causal_gap["retained_forward_aligned_story_count"]
        )
        retained_forward_opposed_story_count += int(
            causal_gap["retained_forward_opposed_story_count"]
        )
        episode_id = row["resolved_factor_episode_id"]
        retained_stories = causal_gap["retained_forward_research_stories"]
        if retained_stories:
            factor_episodes_with_retained_forward_story_count += 1
        if int(causal_gap["strict_forward_independent_story_count"]) > 0:
            factor_episodes_with_strict_independent_story_count += 1
        for story in retained_stories:
            story_key = str(story["story_key"])
            retained_story_episode_ids.setdefault(story_key, set()).add(episode_id)
            retained_story_aligned_links[story_key] = (
                retained_story_aligned_links.get(story_key, 0)
                + int(story["aligned"] is True)
            )
            retained_story_opposed_links[story_key] = (
                retained_story_opposed_links.get(story_key, 0)
                + int(story["aligned"] is not True)
            )
            source_id = str(story.get("source_id") or "")
            if source_id:
                retained_source_ids.add(source_id)
            retained_story_metadata.setdefault(
                story_key,
                {
                    "story_key": story_key,
                    "story_key_type": story["story_key_type"],
                    "source_event_id": story["source_event_id"],
                    "source_id": source_id,
                    "headline": story["headline"],
                    "effective_from_utc": story["effective_from_utc"],
                },
            )
            cluster_key = str(story["story_cluster_key"])
            retained_cluster_episode_ids.setdefault(cluster_key, set()).add(
                episode_id
            )
            source_event_id = str(story.get("source_event_id") or "")
            if source_event_id:
                retained_cluster_source_event_ids.setdefault(
                    cluster_key, set()
                ).add(source_event_id)
            retained_cluster_aligned_links[cluster_key] = (
                retained_cluster_aligned_links.get(cluster_key, 0)
                + int(story["aligned"] is True)
            )
            retained_cluster_opposed_links[cluster_key] = (
                retained_cluster_opposed_links.get(cluster_key, 0)
                + int(story["aligned"] is not True)
            )
            cluster_metadata = {
                "story_cluster_key": cluster_key,
                "story_cluster_key_type": story["story_cluster_key_type"],
                "story_cluster_id": story["story_cluster_id"],
                "representative_source_event_id": source_event_id,
                "representative_source_id": source_id,
                "representative_headline": story["headline"],
                "first_effective_from_utc": story["effective_from_utc"],
            }
            previous_cluster_metadata = retained_cluster_metadata.get(
                cluster_key
            )
            if (
                previous_cluster_metadata is None
                or cluster_metadata["first_effective_from_utc"]
                < previous_cluster_metadata["first_effective_from_utc"]
            ):
                retained_cluster_metadata[cluster_key] = cluster_metadata
        if len({story["story_cluster_key"] for story in retained_stories}) > 1:
            factor_episodes_with_multiple_retained_clusters_count += 1
        episode_rows.append(
            {
                "resolved_factor_episode_id": row["resolved_factor_episode_id"],
                "source_case_id": row["source_case_id"],
                "first_recorded_utc": row["first_recorded_utc"],
                "instrument": row["instrument"],
                "factor_primary_token": row["factor_primary_token"],
                "factor_support": factor_support,
                "factor_support_qualified": factor_support["support_qualified"],
                "root_member_count": row["root_member_count"],
                "any_upstream_representative": row["any_upstream_representative"],
                "actual_direction": actual,
                "executable_net_room_pips": net_room,
                "forward_shadow_eligible": eligible,
                "arms": compact_arms,
                "causal_gap": causal_gap,
            }
        )

    unique_retained_stories: dict[str, dict[str, Any]] = {}
    for row in episode_rows:
        for story in row["causal_gap"]["retained_forward_research_stories"]:
            unique_retained_stories.setdefault(str(story["story_key"]), story)
    narrative_assignments = _assign_narrative_families(
        list(unique_retained_stories.values())
    )
    retained_family_episode_ids: dict[str, set[str]] = {}
    retained_family_source_event_ids: dict[str, set[str]] = {}
    retained_family_source_ids: dict[str, set[str]] = {}
    retained_family_cluster_keys: dict[str, set[str]] = {}
    retained_family_aligned_links: dict[str, int] = {}
    retained_family_opposed_links: dict[str, int] = {}
    retained_family_metadata: dict[str, dict[str, Any]] = {}
    factor_episodes_with_multiple_retained_families_count = 0
    for row in episode_rows:
        episode_id = str(row["resolved_factor_episode_id"])
        row_family_keys: set[str] = set()
        for story in row["causal_gap"]["retained_forward_research_stories"]:
            assignment = narrative_assignments[str(story["story_key"])]
            story.update(assignment)
            family_key = str(assignment["narrative_family_key"])
            row_family_keys.add(family_key)
            retained_family_episode_ids.setdefault(family_key, set()).add(
                episode_id
            )
            source_event_id = str(story.get("source_event_id") or "")
            if source_event_id:
                retained_family_source_event_ids.setdefault(
                    family_key, set()
                ).add(source_event_id)
            source_id = str(story.get("source_id") or "")
            if source_id:
                retained_family_source_ids.setdefault(family_key, set()).add(
                    source_id
                )
            cluster_key = str(story.get("story_cluster_key") or "")
            if cluster_key:
                retained_family_cluster_keys.setdefault(family_key, set()).add(
                    cluster_key
                )
            retained_family_aligned_links[family_key] = (
                retained_family_aligned_links.get(family_key, 0)
                + int(story["aligned"] is True)
            )
            retained_family_opposed_links[family_key] = (
                retained_family_opposed_links.get(family_key, 0)
                + int(story["aligned"] is not True)
            )
            metadata = {
                "narrative_family_key": family_key,
                "narrative_family_contract_id": assignment[
                    "narrative_family_contract_id"
                ],
                "narrative_family_method": assignment[
                    "narrative_family_method"
                ],
                "representative_story_key": assignment[
                    "narrative_family_representative_story_key"
                ],
                "representative_source_event_id": source_event_id,
                "representative_source_id": source_id,
                "representative_headline": story["headline"],
                "first_effective_from_utc": story["effective_from_utc"],
                "similarity_edge_count": assignment[
                    "narrative_family_similarity_edge_count"
                ],
            }
            previous_metadata = retained_family_metadata.get(family_key)
            if (
                previous_metadata is None
                or (
                    metadata["first_effective_from_utc"],
                    metadata["representative_story_key"],
                )
                < (
                    previous_metadata["first_effective_from_utc"],
                    previous_metadata["representative_story_key"],
                )
            ):
                retained_family_metadata[family_key] = metadata
        if len(row_family_keys) > 1:
            factor_episodes_with_multiple_retained_families_count += 1

    metrics: dict[str, dict[str, Any]] = {}
    support_qualified_metrics: dict[str, dict[str, Any]] = {}
    signatures: dict[str, list[str]] = {}
    pre_move_signatures: dict[str, list[str]] = {}
    support_qualified_signatures: dict[str, list[str]] = {}
    support_qualified_pre_move_signatures: dict[str, list[str]] = {}
    label_derived_names = {
        f"forward_shadow::{name}"
        for name in config["label_derived_forward_shadow_arms"]
    }
    for arm in arm_names:
        rows = observations[arm]
        clock_classification = (
            "label_derived_after_move_detection"
            if arm in label_derived_names
            else "available_at_or_before_move_start"
        )
        metric = _arm_metric(
            rows,
            clock_classification=clock_classification,
            interpretation="move_conditioned_alignment_diagnostic_not_trade_pnl",
        )
        metrics[arm] = metric
        signature = str(metric["decision_signature_sha256"])
        signatures.setdefault(signature, []).append(arm)
        if clock_classification == "available_at_or_before_move_start":
            pre_move_signatures.setdefault(signature, []).append(arm)
        support_rows = [
            item for item in rows if item["factor_support_qualified"] is True
        ]
        support_metric = _arm_metric(
            support_rows,
            clock_classification=clock_classification,
            interpretation=(
                "factor_support_qualified_move_conditioned_alignment_"
                "diagnostic_not_trade_pnl"
            ),
        )
        support_qualified_metrics[arm] = support_metric
        support_signature = str(support_metric["decision_signature_sha256"])
        support_qualified_signatures.setdefault(support_signature, []).append(arm)
        if clock_classification == "available_at_or_before_move_start":
            support_qualified_pre_move_signatures.setdefault(
                support_signature, []
            ).append(arm)
    equivalence_groups = [
        {
            "decision_signature_sha256": signature,
            "arms": sorted(arms),
            "arm_count": len(arms),
        }
        for signature, arms in sorted(signatures.items())
    ]
    equivalence_groups.sort(key=lambda item: (-item["arm_count"], item["arms"]))
    pre_move_equivalence_groups = [
        {
            "decision_signature_sha256": signature,
            "arms": sorted(arms),
            "arm_count": len(arms),
        }
        for signature, arms in sorted(pre_move_signatures.items())
    ]
    pre_move_equivalence_groups.sort(
        key=lambda item: (-item["arm_count"], item["arms"])
    )
    support_qualified_equivalence_groups = [
        {
            "decision_signature_sha256": signature,
            "arms": sorted(arms),
            "arm_count": len(arms),
        }
        for signature, arms in sorted(support_qualified_signatures.items())
    ]
    support_qualified_equivalence_groups.sort(
        key=lambda item: (-item["arm_count"], item["arms"])
    )
    support_qualified_pre_move_equivalence_groups = [
        {
            "decision_signature_sha256": signature,
            "arms": sorted(arms),
            "arm_count": len(arms),
        }
        for signature, arms in sorted(
            support_qualified_pre_move_signatures.items()
        )
    ]
    support_qualified_pre_move_equivalence_groups.sort(
        key=lambda item: (-item["arm_count"], item["arms"])
    )
    retained_story_concentration = []
    for story_key, episode_ids in retained_story_episode_ids.items():
        retained_story_concentration.append(
            {
                **retained_story_metadata[story_key],
                "factor_episode_count": len(episode_ids),
                "aligned_factor_episode_link_count": (
                    retained_story_aligned_links.get(story_key, 0)
                ),
                "opposed_factor_episode_link_count": (
                    retained_story_opposed_links.get(story_key, 0)
                ),
            }
        )
    retained_story_concentration.sort(
        key=lambda item: (
            -item["factor_episode_count"],
            -item["aligned_factor_episode_link_count"],
            item["story_key"],
        )
    )
    dominant_story_episode_count = (
        retained_story_concentration[0]["factor_episode_count"]
        if retained_story_concentration
        else 0
    )
    retained_cluster_concentration = []
    for cluster_key, episode_ids in retained_cluster_episode_ids.items():
        retained_cluster_concentration.append(
            {
                **retained_cluster_metadata[cluster_key],
                "factor_episode_count": len(episode_ids),
                "source_event_count": len(
                    retained_cluster_source_event_ids.get(cluster_key, set())
                ),
                "aligned_factor_episode_link_count": (
                    retained_cluster_aligned_links.get(cluster_key, 0)
                ),
                "opposed_factor_episode_link_count": (
                    retained_cluster_opposed_links.get(cluster_key, 0)
                ),
            }
        )
    retained_cluster_concentration.sort(
        key=lambda item: (
            -item["factor_episode_count"],
            -item["source_event_count"],
            -item["aligned_factor_episode_link_count"],
            item["story_cluster_key"],
        )
    )
    dominant_cluster_episode_count = (
        retained_cluster_concentration[0]["factor_episode_count"]
        if retained_cluster_concentration
        else 0
    )
    retained_family_concentration = []
    for family_key, episode_ids in retained_family_episode_ids.items():
        retained_family_concentration.append(
            {
                **retained_family_metadata[family_key],
                "factor_episode_count": len(episode_ids),
                "source_event_count": len(
                    retained_family_source_event_ids.get(family_key, set())
                ),
                "source_count": len(
                    retained_family_source_ids.get(family_key, set())
                ),
                "upstream_story_cluster_count": len(
                    retained_family_cluster_keys.get(family_key, set())
                ),
                "aligned_factor_episode_link_count": (
                    retained_family_aligned_links.get(family_key, 0)
                ),
                "opposed_factor_episode_link_count": (
                    retained_family_opposed_links.get(family_key, 0)
                ),
            }
        )
    retained_family_concentration.sort(
        key=lambda item: (
            -item["factor_episode_count"],
            -item["source_event_count"],
            -item["upstream_story_cluster_count"],
            -item["aligned_factor_episode_link_count"],
            item["narrative_family_key"],
        )
    )
    dominant_family_episode_count = (
        retained_family_concentration[0]["factor_episode_count"]
        if retained_family_concentration
        else 0
    )

    return {
        "schema_version": "move_first_live_arm_alignment_report_v1",
        "generated_utc": utc_now(),
        "audit_id": config["audit_id"],
        "config_sha256": file_sha256(config_path),
        "source_cohort_id": config["source_cohort_id"],
        "source_database": str(source_database),
        "source_case_count": len(cases),
        "factor_root_merge_count": merge_count,
        "resolved_factor_episode_count": len(selected),
        "evaluable_factor_episode_count": len(selected) - excluded_episodes,
        "excluded_factor_episode_count": excluded_episodes,
        "deduplication_rule": config["deduplication_rule"],
        "minimum_absolute_pair_score": float(config["minimum_absolute_pair_score"]),
        "pair_score_formula": config["pair_score_formula"],
        "factor_support_audit": {
            "contract_id": FACTOR_SUPPORT_DIAGNOSTIC_CONTRACT_ID,
            "rule": (
                "causal primary must be the best scored token, have signed "
                "move-aligned strength strictly above zero, and not be "
                "upstream-ambiguous"
            ),
            "source_case_status_counts": dict(
                sorted(source_factor_support_status_counts.items())
            ),
            "selected_episode_status_counts": dict(
                sorted(selected_factor_support_status_counts.items())
            ),
            "support_qualified_source_case_count": sum(
                item["support_qualified"] for item in source_factor_support
            ),
            "support_qualified_selected_episode_count": sum(
                item["support_qualified"] for item in selected_factor_support
            ),
            "unsupported_source_case_count": sum(
                not item["support_qualified"] for item in source_factor_support
            ),
            "unsupported_selected_episode_count": sum(
                not item["support_qualified"] for item in selected_factor_support
            ),
            "unsupported_examples_first20": unsupported_factor_examples,
            "existing_membership_bytes_preserved": True,
            "changes_existing_arm_metrics": False,
            "promotion_eligible": False,
            "execution_eligible": False,
            "interpretation": (
                "support-qualified subset diagnostic; frozen factor memberships "
                "and legacy headline metrics remain unchanged"
            ),
        },
        "selection_conditioned_on_realized_executable_move": True,
        "predictive_backtest_eligible": False,
        "promotion_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
        "arm_count": len(arm_names),
        "pre_move_arm_count": len(arm_names) - len(label_derived_names),
        "label_derived_control_arm_count": len(label_derived_names),
        "unique_decision_vector_count": len(equivalence_groups),
        "pre_move_unique_decision_vector_count": len(pre_move_equivalence_groups),
        "decision_equivalence_groups": equivalence_groups,
        "pre_move_decision_equivalence_groups": pre_move_equivalence_groups,
        "arm_metrics": metrics,
        "support_qualified_factor_episode_count": sum(
            row["factor_support_qualified"] is True for row in episode_rows
        ),
        "support_qualified_unique_decision_vector_count": len(
            support_qualified_equivalence_groups
        ),
        "support_qualified_pre_move_unique_decision_vector_count": len(
            support_qualified_pre_move_equivalence_groups
        ),
        "support_qualified_decision_equivalence_groups": (
            support_qualified_equivalence_groups
        ),
        "support_qualified_pre_move_decision_equivalence_groups": (
            support_qualified_pre_move_equivalence_groups
        ),
        "support_qualified_arm_metrics": support_qualified_metrics,
        "causal_gap_taxonomy_contract_id": CAUSAL_GAP_TAXONOMY_CONTRACT_ID,
        "causal_gap_taxonomy": {
            "episode_count": len(episode_rows),
            "state_counts": dict(sorted(gap_state_counts.items())),
            "strict_abstention_reason_episode_counts": dict(
                sorted(gap_reason_episode_counts.items())
            ),
            "retained_forward_research_story_count": retained_forward_story_count,
            "retained_forward_aligned_story_count": (
                retained_forward_aligned_story_count
            ),
            "retained_forward_opposed_story_count": (
                retained_forward_opposed_story_count
            ),
            "factor_episodes_with_retained_forward_story_count": (
                factor_episodes_with_retained_forward_story_count
            ),
            "factor_episodes_with_strict_independent_story_count": (
                factor_episodes_with_strict_independent_story_count
            ),
            "unique_retained_forward_story_count": len(
                retained_story_episode_ids
            ),
            "unique_retained_forward_source_count": len(retained_source_ids),
            "unique_retained_forward_story_cluster_count": len(
                retained_cluster_episode_ids
            ),
            "factor_episodes_with_multiple_retained_story_clusters_count": (
                factor_episodes_with_multiple_retained_clusters_count
            ),
            "narrative_family_contract_id": NARRATIVE_FAMILY_CONTRACT_ID,
            "unique_retained_forward_narrative_family_count": len(
                retained_family_episode_ids
            ),
            "factor_episodes_with_multiple_retained_narrative_families_count": (
                factor_episodes_with_multiple_retained_families_count
            ),
            "dominant_retained_story_factor_episode_count": (
                dominant_story_episode_count
            ),
            "dominant_retained_story_factor_episode_pct": (
                _round(
                    100.0 * dominant_story_episode_count / len(episode_rows)
                )
                if episode_rows
                else None
            ),
            "retained_story_concentration_top20": (
                retained_story_concentration[:20]
            ),
            "dominant_retained_story_cluster_factor_episode_count": (
                dominant_cluster_episode_count
            ),
            "dominant_retained_story_cluster_factor_episode_pct": (
                _round(
                    100.0 * dominant_cluster_episode_count / len(episode_rows)
                )
                if episode_rows
                else None
            ),
            "retained_story_cluster_concentration_top20": (
                retained_cluster_concentration[:20]
            ),
            "dominant_retained_narrative_family_factor_episode_count": (
                dominant_family_episode_count
            ),
            "dominant_retained_narrative_family_factor_episode_pct": (
                _round(
                    100.0 * dominant_family_episode_count / len(episode_rows)
                )
                if episode_rows
                else None
            ),
            "retained_narrative_family_concentration_top20": (
                retained_family_concentration[:20]
            ),
            "interpretation": (
                "retained_pre_move_source_gap_and_syndication_concentration_diagnostic_on_move_conditioned_cases"
            ),
        },
        "episode_rows": episode_rows,
        "warning": (
            "Cases are selected because an executable move occurred. Alignment can diagnose "
            "mapping and abstention only; it is not predictive expectancy or promotion evidence."
        ),
    }


def render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Move-first live arm alignment",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "> This is a move-conditioned explanation diagnostic, not a predictor backtest. "
        "It cannot promote or place orders.",
        "",
        f"Raw cases: **{payload['source_case_count']}**; resolved factor episodes: "
        f"**{payload['resolved_factor_episode_count']}**; evaluable episodes: "
        f"**{payload['evaluable_factor_episode_count']}**.",
        "",
        "## Factor-support integrity",
        "",
        f"Support-qualified selected episodes: **{payload['factor_support_audit']['support_qualified_selected_episode_count']}**; "
        f"unsupported or unavailable: **{payload['factor_support_audit']['unsupported_selected_episode_count']}**.",
        "",
        "A factor is support-qualified only when its signed currency-strength score is "
        "strictly move-aligned, it is the best of the two candidate legs, and the "
        "upstream margin is not ambiguous. Frozen membership bytes are preserved; "
        "this layer does not alter the legacy arm metrics or any execution path.",
        "",
        "## Legacy all-episode alignment",
        "",
        "| Arm | Clock | Signals | Abstains | Correct | Wrong | Accuracy | Alignment room* |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for arm, metric in payload["arm_metrics"].items():
        accuracy = metric["directional_accuracy_pct"]
        accuracy_text = "n/a" if accuracy is None else f"{accuracy:.1f}%"
        lines.append(
            f"| `{arm}` | {metric['clock_classification']} | "
            f"{metric['signaled_episode_count']} | "
            f"{metric['abstained_episode_count']} | {metric['correct_direction_count']} | "
            f"{metric['wrong_direction_count']} | {accuracy_text} | "
            f"{metric['signed_alignment_room_pips']:+.2f} |"
        )
    lines += [
        "",
        "*Alignment room gives the observed move's after-spread room a positive sign "
        "when the arm agrees and a negative sign when it opposes. It is not simulated P/L.",
        "",
        "## Factor-support-qualified alignment",
        "",
        f"This subset contains **{payload['support_qualified_factor_episode_count']}** "
        "resolved episodes whose frozen primary factor has positive signed support.",
        "",
        "| Arm | Clock | Signals | Abstains | Correct | Wrong | Accuracy | Alignment room* |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for arm, metric in payload["support_qualified_arm_metrics"].items():
        accuracy = metric["directional_accuracy_pct"]
        accuracy_text = "n/a" if accuracy is None else f"{accuracy:.1f}%"
        lines.append(
            f"| `{arm}` | {metric['clock_classification']} | "
            f"{metric['signaled_episode_count']} | "
            f"{metric['abstained_episode_count']} | {metric['correct_direction_count']} | "
            f"{metric['wrong_direction_count']} | {accuracy_text} | "
            f"{metric['signed_alignment_room_pips']:+.2f} |"
        )
    lines += [
        "",
        "This is the same calculation on the support-qualified subset. It remains "
        "move-conditioned, nonpredictive, nonpromotional, and nonexecuting.",
        "",
        f"Pre-move decision-distinct vectors: **{payload['pre_move_unique_decision_vector_count']}** "
        f"from **{payload['pre_move_arm_count']}** pre-move named arms. "
        f"The other **{payload['label_derived_control_arm_count']}** arms are after-move controls.",
        "",
        "## Causal source-gap taxonomy",
        "",
        "| State | Episodes |",
        "|---|---:|",
    ]
    for state, count in payload["causal_gap_taxonomy"]["state_counts"].items():
        lines.append(f"| `{state}` | {count} |")
    lines += [
        "",
        f"Unique retained forward stories: **{payload['causal_gap_taxonomy']['unique_retained_forward_story_count']}** "
        f"in **{payload['causal_gap_taxonomy']['unique_retained_forward_story_cluster_count']}** clusters "
        f"and **{payload['causal_gap_taxonomy']['unique_retained_forward_narrative_family_count']}** "
        "conservative narrative families "
        f"across **{payload['causal_gap_taxonomy']['factor_episodes_with_retained_forward_story_count']}** "
        "factor episodes; episodes with strict independent corroboration: "
        f"**{payload['causal_gap_taxonomy']['factor_episodes_with_strict_independent_story_count']}**.",
        "",
        "This taxonomy uses only source material retained at or before each move start. "
        "The narrative-family layer conservatively groups time-adjacent headline variants "
        "for concentration diagnostics only; it never establishes independent proof. "
        "The taxonomy explains abstention and mapping failures; it is not a forecast score.",
        "",
        "Execution decision: **no_trade**.",
        "",
    ]
    return "\n".join(lines)


def run_once(
    config_path: Path = DEFAULT_CONFIG,
    source_database: Path = DEFAULT_SOURCE_DATABASE,
    json_report: Path = DEFAULT_JSON_REPORT,
    md_report: Path = DEFAULT_MD_REPORT,
) -> dict[str, Any]:
    payload = build_audit(config_path, source_database)
    census.atomic_text(json_report, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    census.atomic_text(md_report, render_markdown(payload))
    return payload


def verify(
    config_path: Path = DEFAULT_CONFIG,
    source_database: Path = DEFAULT_SOURCE_DATABASE,
    json_report: Path = DEFAULT_JSON_REPORT,
) -> dict[str, Any]:
    failures: list[str] = []
    if not json_report.exists():
        return {"verified": False, "failures": ["report_missing"]}
    observed = json.loads(json_report.read_text(encoding="utf-8"))
    rebuilt = build_audit(config_path, source_database)
    stable_keys = (
        "audit_id",
        "config_sha256",
        "source_cohort_id",
        "source_case_count",
        "factor_root_merge_count",
        "resolved_factor_episode_count",
        "evaluable_factor_episode_count",
        "excluded_factor_episode_count",
        "arm_count",
        "pre_move_arm_count",
        "label_derived_control_arm_count",
        "unique_decision_vector_count",
        "pre_move_unique_decision_vector_count",
        "decision_equivalence_groups",
        "pre_move_decision_equivalence_groups",
        "factor_support_audit",
        "arm_metrics",
        "support_qualified_factor_episode_count",
        "support_qualified_unique_decision_vector_count",
        "support_qualified_pre_move_unique_decision_vector_count",
        "support_qualified_decision_equivalence_groups",
        "support_qualified_pre_move_decision_equivalence_groups",
        "support_qualified_arm_metrics",
        "causal_gap_taxonomy_contract_id",
        "causal_gap_taxonomy",
        "episode_rows",
        "execution_decision",
    )
    for key in stable_keys:
        if observed.get(key) != rebuilt.get(key):
            failures.append(f"report_mismatch:{key}")
    required = {
        "selection_conditioned_on_realized_executable_move": True,
        "predictive_backtest_eligible": False,
        "promotion_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_place_orders": False,
        "execution_decision": "no_trade",
    }
    for key, expected in required.items():
        if observed.get(key) != expected:
            failures.append(f"unsafe_report_flag:{key}")
    return {
        "verified": not failures,
        "failures": failures,
        "audit_id": rebuilt["audit_id"],
        "source_case_count": rebuilt["source_case_count"],
        "resolved_factor_episode_count": rebuilt["resolved_factor_episode_count"],
        "checked_utc": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--source-database", type=Path, default=DEFAULT_SOURCE_DATABASE)
    parser.add_argument("--json-report", type=Path, default=DEFAULT_JSON_REPORT)
    parser.add_argument("--md-report", type=Path, default=DEFAULT_MD_REPORT)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress recurring report payloads on stdout; report files are unchanged.",
    )
    args = parser.parse_args()
    if args.verify_only:
        payload = verify(args.config, args.source_database, args.json_report)
        if not args.quiet:
            print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        return 0 if payload["verified"] else 1
    stop = time.monotonic() + max(0.0, float(args.duration_sec))
    while True:
        payload = run_once(
            args.config,
            args.source_database,
            args.json_report,
            args.md_report,
        )
        if not args.quiet:
            print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        if float(args.interval_sec) <= 0.0 or time.monotonic() >= stop:
            return 0
        time.sleep(min(float(args.interval_sec), max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
