"""Hindsight-only overlap and cross-pair shock clustering.

Cluster identifiers are intended for deduplication, fold grouping, and event
analysis.  They are derived from forward outcomes and must never be supplied
to a causal trading feature matrix.
"""

from __future__ import annotations

import hashlib
from typing import Any

import numpy as np
import pandas as pd

from .dataset import _strict_utc_series


def _stable_id(prefix: str, values: list[str]) -> str:
    payload = "|".join(sorted(values)).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(payload).hexdigest()[:20]}"


def _overlap_fraction(
    left_start: pd.Timestamp,
    left_end: pd.Timestamp,
    right_start: pd.Timestamp,
    right_end: pd.Timestamp,
) -> float:
    overlap = min(left_end, right_end) - max(left_start, right_start)
    if overlap <= pd.Timedelta(0):
        return 0.0
    shortest = min(left_end - left_start, right_end - right_start)
    return float(overlap / shortest) if shortest > pd.Timedelta(0) else 0.0


def _empty_cluster_columns(output: pd.DataFrame) -> pd.DataFrame:
    output["is_event_candidate"] = False
    output["overlap_group_id"] = pd.Series(pd.NA, index=output.index, dtype="string")
    output["event_cluster_id"] = pd.Series(pd.NA, index=output.index, dtype="string")
    output["event_anchor_timestamp"] = pd.Series(pd.NaT, index=output.index, dtype="datetime64[ns, UTC]")
    output["event_pair_count"] = pd.Series(pd.NA, index=output.index, dtype="Int64")
    output["event_candidate_count"] = pd.Series(pd.NA, index=output.index, dtype="Int64")
    output["is_cross_pair_shock"] = False
    output["event_cluster_is_hindsight_metadata"] = True
    return output


def assign_event_clusters(
    decisions: pd.DataFrame,
    *,
    candidate_col: str = "is_significant",
    score_col: str = "forward_abs_pips",
    within_pair_start_tolerance: Any = "60min",
    cross_pair_start_tolerance: Any = "45min",
    minimum_interval_overlap_fraction: float = 0.25,
) -> pd.DataFrame:
    """Assign pair-overlap groups and contemporaneous macro shock clusters.

    First, overlapping candidate labels from the same instrument and direction
    are collapsed into a bounded-start overlap group.  The strongest row is its
    representative.  Representatives across instruments are then clustered
    when both their start times and their forward intervals align.

    The fixed anchor tolerance prevents a long sequence of five-minute starts
    from chaining into an unbounded multi-hour event.  Every candidate receives
    an ``event_cluster_id``; ``is_cross_pair_shock`` identifies clusters with at
    least two distinct instruments.
    """

    required = {
        "decision_id",
        "timestamp",
        "outcome_timestamp",
        "instrument",
        "forward_signed_pips",
        score_col,
        candidate_col,
    }
    missing = sorted(required.difference(decisions.columns))
    if missing:
        raise ValueError(f"decision frame missing clustering columns: {missing}")
    if not 0.0 <= float(minimum_interval_overlap_fraction) <= 1.0:
        raise ValueError("minimum_interval_overlap_fraction must be between zero and one")
    pair_tolerance = pd.Timedelta(within_pair_start_tolerance)
    event_tolerance = pd.Timedelta(cross_pair_start_tolerance)
    if pair_tolerance < pd.Timedelta(0) or event_tolerance < pd.Timedelta(0):
        raise ValueError("cluster start tolerances must be non-negative")

    output = _empty_cluster_columns(decisions.copy().reset_index(drop=True))
    if output.empty:
        return output
    output["timestamp"] = _strict_utc_series(output["timestamp"], name="timestamp")
    output["outcome_timestamp"] = _strict_utc_series(
        output["outcome_timestamp"], name="outcome_timestamp"
    )
    if output["decision_id"].duplicated().any():
        raise ValueError("decision_id must be unique before event clustering")
    invalid_interval = output["outcome_timestamp"] <= output["timestamp"]
    if bool(invalid_interval.any()):
        raise ValueError("every outcome_timestamp must be later than timestamp")

    candidates = output[candidate_col].fillna(False).astype(bool)
    if "label_is_valid" in output:
        candidates &= output["label_is_valid"].fillna(False).astype(bool)
    output["is_event_candidate"] = candidates
    candidate_positions = np.flatnonzero(candidates.to_numpy())
    if not len(candidate_positions):
        return output

    work = output.loc[candidate_positions].copy()
    work["_position"] = candidate_positions
    work["_score"] = pd.to_numeric(work[score_col], errors="coerce").fillna(-np.inf)
    signed = pd.to_numeric(work["forward_signed_pips"], errors="coerce")
    work["_direction"] = np.where(signed >= 0, "up", "down")

    pair_groups: list[dict[str, Any]] = []
    for (_, _), part in work.groupby(["instrument", "_direction"], sort=True, observed=True):
        part = part.sort_values(["timestamp", "decision_id"], kind="stable")
        current: dict[str, Any] | None = None
        columns = [
            "instrument",
            "_direction",
            "timestamp",
            "outcome_timestamp",
            "_position",
            "decision_id",
            "_score",
        ]
        for (
            row_instrument,
            row_direction,
            start,
            end,
            row_position,
            row_decision_id,
            row_score,
        ) in part[columns].itertuples(index=False, name=None):
            if current is not None:
                joins = (
                    start - current["anchor_start"] <= pair_tolerance
                    and _overlap_fraction(
                        current["anchor_start"], current["anchor_end"], start, end
                    )
                    >= minimum_interval_overlap_fraction
                )
            else:
                joins = False
            if not joins:
                if current is not None:
                    pair_groups.append(current)
                current = {
                    "instrument": row_instrument,
                    "direction": row_direction,
                    "anchor_start": start,
                    "anchor_end": end,
                    "rows": [],
                }
            assert current is not None
            current["rows"].append(
                {
                    "position": int(row_position),
                    "decision_id": str(row_decision_id),
                    "timestamp": start,
                    "outcome_timestamp": end,
                    "score": float(row_score),
                }
            )
        if current is not None:
            pair_groups.append(current)

    representatives: list[dict[str, Any]] = []
    for group in pair_groups:
        group_id = _stable_id(
            "overlap", [row["decision_id"] for row in group["rows"]]
        )
        for row in group["rows"]:
            output.at[row["position"], "overlap_group_id"] = group_id
        representative = sorted(
            group["rows"], key=lambda row: (-row["score"], row["timestamp"], row["decision_id"])
        )[0]
        representatives.append(
            {
                "overlap_group_id": group_id,
                "instrument": group["instrument"],
                "timestamp": representative["timestamp"],
                "outcome_timestamp": representative["outcome_timestamp"],
                "score": representative["score"],
                "member_positions": [row["position"] for row in group["rows"]],
            }
        )

    # Fixed-anchor temporal clustering of the de-duplicated pair representatives.
    event_groups: list[dict[str, Any]] = []
    for representative in sorted(
        representatives,
        key=lambda row: (row["timestamp"], row["instrument"], row["overlap_group_id"]),
    ):
        best: dict[str, Any] | None = None
        best_distance: pd.Timedelta | None = None
        for event in reversed(event_groups):
            distance = representative["timestamp"] - event["anchor_start"]
            if distance > event_tolerance:
                break
            if distance < pd.Timedelta(0):
                continue
            overlap = _overlap_fraction(
                event["anchor_start"],
                event["anchor_end"],
                representative["timestamp"],
                representative["outcome_timestamp"],
            )
            if overlap < minimum_interval_overlap_fraction:
                continue
            if best_distance is None or distance < best_distance:
                best = event
                best_distance = distance
        if best is None:
            event_groups.append(
                {
                    "anchor_start": representative["timestamp"],
                    "anchor_end": representative["outcome_timestamp"],
                    "members": [representative],
                }
            )
        else:
            best["members"].append(representative)

    for event in event_groups:
        overlap_ids = [member["overlap_group_id"] for member in event["members"]]
        event_id = _stable_id("event", overlap_ids)
        positions = [
            position
            for member in event["members"]
            for position in member["member_positions"]
        ]
        pair_count = len({member["instrument"] for member in event["members"]})
        output.loc[positions, "event_cluster_id"] = event_id
        output.loc[positions, "event_anchor_timestamp"] = event["anchor_start"]
        output.loc[positions, "event_pair_count"] = pair_count
        output.loc[positions, "event_candidate_count"] = len(positions)
        output.loc[positions, "is_cross_pair_shock"] = pair_count >= 2

    return output
