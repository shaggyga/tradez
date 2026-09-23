"""Frozen structural contract for the seven-Wednesday policy expansion.

This module contains no broker, account, lifecycle, authorization, promotion,
or signal-feed surface. It only defines the deterministic historical-session
roles used by the research evaluator.
"""

from __future__ import annotations

from typing import Any, Mapping


EXPERIMENT_KEY = "sequential_all68_policy_expansion_v1"
PRIOR_DISCOVERY_SESSION = "20260826_wed_overlap_prior_discovery"
EXPECTED_SESSION_COUNT = 7
EXPECTED_GLOBAL_CLOCK_COUNT = 336


def ordered_session_keys(manifest: Mapping[str, Any]) -> list[str]:
    rows = list(manifest["sessions"])
    starts = [str(row["start_utc"]) for row in rows]
    sessions = [str(row["session_key"]) for row in rows]
    if starts != sorted(starts) or len(starts) != len(set(starts)):
        raise ValueError("source sessions are not uniquely time ordered")
    if len(sessions) != len(set(sessions)):
        raise ValueError("duplicate source session key")
    if len(sessions) != EXPECTED_SESSION_COUNT:
        raise ValueError("frozen expansion session count changed")
    return sessions


def strictly_prior_session_map(manifest: Mapping[str, Any]) -> dict[str, list[str]]:
    """Return every and only strictly earlier listed session per application."""
    sessions = ordered_session_keys(manifest)
    return {session: list(sessions[:index]) for index, session in enumerate(sessions)}


def validate_session_roles(manifest: Mapping[str, Any], roles: Mapping[str, Any]) -> None:
    sessions = ordered_session_keys(manifest)
    if roles.get("all_sessions_are_historical_training") is not True:
        raise ValueError("expansion sessions must remain historical training")
    if list(roles.get("prior_discovery_session_keys", ())) != [PRIOR_DISCOVERY_SESSION]:
        raise ValueError("prior-discovery session role changed")
    if sessions[-1] != PRIOR_DISCOVERY_SESSION:
        raise ValueError("prior-discovery session is not last")
    mapping = strictly_prior_session_map(manifest)
    if mapping[sessions[0]]:
        raise ValueError("first session must have no training")
    for index, session in enumerate(sessions):
        if mapping[session] != sessions[:index]:
            raise ValueError("same/future session entered training map")
