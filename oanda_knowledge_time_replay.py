#!/usr/bin/env python3
"""Reconstruct the externally sourced information known at a decision cutoff."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_DATABASE = ROOT / "data" / "oanda_training_manager" / "state" / "source_governance_v1.sqlite"
STORY_CLUSTER_CONTRACT_ID = "story_cluster_v2_event_lineage_or_headline_20260808"


def normalize_cutoff(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("cutoff must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat()


def replay(database: Path, cutoff_utc: str) -> dict[str, Any]:
    cutoff = normalize_cutoff(cutoff_utc)
    connection = sqlite3.connect(
        f"file:{database.resolve().as_posix()}?mode=ro", uri=True, timeout=20.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=20000")
    try:
        has_governed_clusters = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='source_story_cluster_assignments'"
        ).fetchone() is not None
        has_causal_view = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name='source_events_causal_v1'"
        ).fetchone() is not None
        source_event_relation = (
            "source_events_causal_v1" if has_causal_view else "source_events"
        )
        has_causal_supersession_view = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name='source_supersession_events_causal_v1'"
        ).fetchone() is not None
        supersession_relation = (
            "source_supersession_events_causal_v1"
            if has_causal_supersession_view
            else "source_supersession_events"
        )
        cluster_select = (
            ", assignment.story_cluster_id AS governed_story_cluster_id"
            if has_governed_clusters
            else ""
        )
        cluster_join = (
            " LEFT JOIN source_story_cluster_assignments AS assignment"
            " ON assignment.source_event_id=event.source_event_id"
            " AND assignment.clustering_contract_id=?"
            if has_governed_clusters
            else ""
        )
        parameters: tuple[Any, ...] = (
            (STORY_CLUSTER_CONTRACT_ID, cutoff, cutoff, cutoff, cutoff, cutoff)
            if has_governed_clusters
            else (cutoff, cutoff, cutoff, cutoff, cutoff)
        )
        rows = [
            dict(row)
            for row in connection.execute(
                f"""
                SELECT event.*{cluster_select}
                FROM {source_event_relation} AS event
                {cluster_join}
                WHERE event.decision_cutoff_utc<=?
                  AND event.effective_from_utc<=?
                  AND event.retrieved_at_utc<=?
                  AND (event.valid_until_utc IS NULL OR event.valid_until_utc>?)
                  AND NOT EXISTS (
                      SELECT 1 FROM {supersession_relation} AS supersession
                      WHERE supersession.prior_source_event_id=event.source_event_id
                        AND supersession.observed_utc<=?
                  )
                ORDER BY event.source_id,event.provider_event_id,event.event_version
                """,
                parameters,
            )
        ]
    finally:
        connection.close()
    for row in rows:
        governed = row.pop("governed_story_cluster_id", None)
        if governed:
            row["story_cluster_id"] = governed
    identity = [
        (
            row["source_event_id"], row["raw_payload_sha256"],
            row["source_contract_id"], row["source_cohort_id"],
        )
        for row in rows
    ]
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    sources = sorted({str(row["source_id"]) for row in rows})
    stories = sorted({str(row["story_cluster_id"]) for row in rows if row["story_cluster_id"]})
    return {
        "schema_version": 1,
        "decision_cutoff_utc": cutoff,
        "knowledge_contract": (
            "decision_cutoff/effective/retrieval must precede cutoff; supersession applies only "
            "after it was locally observed"
        ),
        "source_event_count": len(rows),
        "independent_story_cluster_count": len(stories),
        "story_cluster_contract_id": (
            STORY_CLUSTER_CONTRACT_ID if has_governed_clusters else "legacy_embedded"
        ),
        "source_count": len(sources),
        "source_ids": sources,
        "knowledge_snapshot_sha256": "sha256:" + digest,
        "events": rows,
        "research_only": True,
        "can_place_orders": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--cutoff-utc", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    payload = replay(args.database, args.cutoff_utc)
    encoded = json.dumps(payload, indent=2, sort_keys=True)
    if args.output is None:
        print(encoded)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(encoded, encoding="utf-8")
        temporary.replace(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["replay"]
