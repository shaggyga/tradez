#!/usr/bin/env python3
"""Reconcile unmatched research episodes against the frozen local source view.

Matches are candidates for later manual/structured attribution, never causal
assignments.  Publication time and first-seen time remain distinct so a
historical article backfilled after the move cannot masquerade as a forecast.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
EVIDENCE_DB = ROOT / "data/oanda_training_manager/state/spike_blurb_factor_reconstruction_v1.sqlite"
SOURCE_DB = ROOT / "data/oanda_training_manager/local_news_sentiment/local_news_sentiment_v1.sqlite"
REPORT_ROOT = ROOT / "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/existing_source_reconciliation_v1"
CONTRACT_ID = "spike_blurb_existing_source_reconciliation_v1_20260820"
UPSTREAM_CONTRACT_ID = "spike_blurb_unmatched_episode_queue_v2_20260820"
SCHEMA_VERSION = 1
PRE_WINDOW_MIN = 360
POST_WINDOW_MIN = 30
MAX_MATCHES_PER_EPISODE = 5


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_id(prefix: str, *parts: object) -> str:
    return f"{prefix}_{sha256_bytes('|'.join(map(str, parts)).encode())[:24]}"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_utc(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def parse_currency_list(value: Any) -> list[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, list):
        return sorted({str(item).upper() for item in parsed if str(item)})
    if isinstance(parsed, dict):
        return sorted({str(item).upper() for item in parsed if str(item)})
    return []


def normalize_headline(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def availability_state(published: datetime, first_seen: datetime, start: datetime) -> str:
    if published <= start and first_seen <= start:
        return "pre_entry_causal_available"
    if first_seen <= start + timedelta(minutes=POST_WINDOW_MIN):
        return "during_first_wave"
    if published <= start and first_seen > start + timedelta(minutes=POST_WINDOW_MIN):
        return "published_pre_entry_observed_late"
    return "future_or_ex_post"


def relevance_state(article_currencies: list[str], episode_currencies: set[str], factor_currency: str) -> str:
    overlap = episode_currencies.intersection(article_currencies)
    if not overlap:
        return "no_currency_overlap"
    if article_currencies == [factor_currency]:
        return "exact_factor_currency"
    if len(article_currencies) == 1:
        return "exact_single_episode_leg"
    if len(article_currencies) <= 3:
        return "narrow_multi_currency"
    return "broad_risk_mapping"


AVAILABILITY_RANK = {
    "pre_entry_causal_available": 0,
    "during_first_wave": 1,
    "published_pre_entry_observed_late": 2,
    "future_or_ex_post": 3,
}
RELEVANCE_RANK = {
    "exact_factor_currency": 0,
    "exact_single_episode_leg": 1,
    "narrow_multi_currency": 2,
    "broad_risk_mapping": 3,
    "no_currency_overlap": 4,
}


def candidate_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        AVAILABILITY_RANK[row["availability_state"]],
        RELEVANCE_RANK[row["relevance_state"]],
        -int(row["source_verified"]),
        -float(row["source_quality"]),
        abs(float(row["published_lead_minutes"])),
        row["headline_normalized"],
        row["event_id"],
    )


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS existing_source_reconciliation_contracts (
          contract_id TEXT PRIMARY KEY,
          contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL,
          source_snapshot_sha256 TEXT NOT NULL,
          created_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS unmatched_episode_source_candidates (
          match_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          episode_id TEXT NOT NULL,
          candidate_rank INTEGER NOT NULL,
          event_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          source_name TEXT,
          source_kind TEXT,
          source_quality REAL NOT NULL,
          source_verified INTEGER NOT NULL,
          published_utc TEXT NOT NULL,
          first_seen_utc TEXT NOT NULL,
          headline TEXT NOT NULL,
          headline_normalized TEXT NOT NULL,
          category TEXT,
          source_url TEXT,
          currencies_json TEXT NOT NULL,
          availability_state TEXT NOT NULL,
          relevance_state TEXT NOT NULL,
          published_lead_minutes REAL NOT NULL,
          availability_lead_minutes REAL NOT NULL,
          factor_currency TEXT NOT NULL,
          factor_sign INTEGER NOT NULL,
          source_candidate_only INTEGER NOT NULL,
          causal_driver_assigned INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          UNIQUE(contract_id, episode_id, candidate_rank),
          UNIQUE(contract_id, episode_id, event_id),
          FOREIGN KEY(contract_id) REFERENCES existing_source_reconciliation_contracts(contract_id)
        );
        CREATE INDEX IF NOT EXISTS unmatched_episode_candidates_state
          ON unmatched_episode_source_candidates(contract_id, availability_state, relevance_state);
        CREATE TRIGGER IF NOT EXISTS source_reconciliation_contracts_no_update
          BEFORE UPDATE ON existing_source_reconciliation_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS source_reconciliation_contracts_no_delete
          BEFORE DELETE ON existing_source_reconciliation_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS episode_source_candidates_no_update
          BEFORE UPDATE ON unmatched_episode_source_candidates BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS episode_source_candidates_no_delete
          BEFORE DELETE ON unmatched_episode_source_candidates BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def read_source_snapshot(source_db: Path) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
    connection = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    rows: list[dict[str, Any]] = []
    digest = hashlib.sha256()
    invalid_clock_count = 0
    for raw in connection.execute(
        """
        SELECT event_id,source_id,source_name,source_kind,source_quality,source_verified,
               published_utc,first_seen_utc,headline,category,source_url,currencies_json
        FROM articles ORDER BY event_id
        """
    ):
        row = dict(raw)
        digest.update(canonical_json(row).encode())
        published = parse_utc(row["published_utc"])
        first_seen = parse_utc(row["first_seen_utc"])
        if published is None or first_seen is None:
            invalid_clock_count += 1
            continue
        row["published_dt"] = published
        row["first_seen_dt"] = first_seen
        row["currencies"] = parse_currency_list(row["currencies_json"])
        row["headline_normalized"] = normalize_headline(str(row["headline"] or ""))
        rows.append(row)
    highwater = connection.execute(
        "SELECT count(*),max(first_seen_utc),max(published_utc) FROM articles"
    ).fetchone()
    connection.close()
    rows.sort(key=lambda row: (row["published_dt"], row["event_id"]))
    return rows, digest.hexdigest(), {
        "article_count": int(highwater[0]),
        "first_seen_highwater_utc": highwater[1],
        "published_highwater_utc": highwater[2],
        "invalid_clock_count": invalid_clock_count,
    }


def reconcile(
    evidence_db: Path = EVIDENCE_DB,
    source_db: Path = SOURCE_DB,
    report_root: Path = REPORT_ROOT,
) -> dict[str, Any]:
    articles, source_hash, source_state = read_source_snapshot(source_db)
    article_epochs = [row["published_dt"].timestamp() for row in articles]
    connection = sqlite3.connect(evidence_db)
    connection.row_factory = sqlite3.Row
    ensure_schema(connection)
    episodes = list(connection.execute(
        "SELECT * FROM unmatched_research_episodes WHERE contract_id=? ORDER BY priority_rank",
        (UPSTREAM_CONTRACT_ID,),
    ))
    if len(episodes) != 1646:
        raise RuntimeError(f"unexpected_episode_count:{len(episodes)}")

    all_matches: list[dict[str, Any]] = []
    episode_match_states: Counter[str] = Counter()
    for episode in episodes:
        start = parse_utc(episode["representative_start_utc"])
        assert start is not None
        currencies = set(json.loads(str(episode["currency_legs_json"])))
        factor_currency = str(episode["hindsight_factor_currency"])
        lower = bisect.bisect_left(article_epochs, (start - timedelta(minutes=PRE_WINDOW_MIN)).timestamp())
        upper = bisect.bisect_right(article_epochs, (start + timedelta(minutes=POST_WINDOW_MIN)).timestamp())
        candidates: list[dict[str, Any]] = []
        seen_story_keys: set[tuple[str, int]] = set()
        for article in articles[lower:upper]:
            overlap = currencies.intersection(article["currencies"])
            if not overlap:
                continue
            story_key = (
                article["headline_normalized"],
                int(article["published_dt"].timestamp() // 300),
            )
            if story_key in seen_story_keys:
                continue
            seen_story_keys.add(story_key)
            available = availability_state(article["published_dt"], article["first_seen_dt"], start)
            relevance = relevance_state(article["currencies"], currencies, factor_currency)
            candidates.append(
                {
                    "event_id": str(article["event_id"]),
                    "source_id": str(article["source_id"] or ""),
                    "source_name": str(article["source_name"] or ""),
                    "source_kind": str(article["source_kind"] or ""),
                    "source_quality": float(article["source_quality"] or 0.0),
                    "source_verified": int(article["source_verified"] or 0),
                    "published_utc": article["published_dt"].isoformat(),
                    "first_seen_utc": article["first_seen_dt"].isoformat(),
                    "headline": str(article["headline"] or ""),
                    "headline_normalized": article["headline_normalized"],
                    "category": str(article["category"] or ""),
                    "source_url": str(article["source_url"] or ""),
                    "currencies_json": canonical_json(article["currencies"]),
                    "availability_state": available,
                    "relevance_state": relevance,
                    "published_lead_minutes": (start - article["published_dt"]).total_seconds() / 60.0,
                    "availability_lead_minutes": (start - article["first_seen_dt"]).total_seconds() / 60.0,
                    "factor_currency": factor_currency,
                    "factor_sign": int(episode["hindsight_factor_sign"]),
                }
            )
        candidates.sort(key=candidate_sort_key)
        selected = candidates[:MAX_MATCHES_PER_EPISODE]
        if not selected:
            episode_match_states["no_local_candidate"] += 1
        elif any(row["availability_state"] == "pre_entry_causal_available" for row in selected):
            episode_match_states["has_pre_entry_candidate"] += 1
        elif any(row["availability_state"] == "during_first_wave" for row in selected):
            episode_match_states["has_first_wave_candidate"] += 1
        else:
            episode_match_states["late_or_ex_post_candidates_only"] += 1
        for rank, row in enumerate(selected, start=1):
            row["episode_id"] = str(episode["episode_id"])
            row["candidate_rank"] = rank
            row["match_id"] = stable_id("episode_source_candidate", CONTRACT_ID, episode["episode_id"], row["event_id"])
            all_matches.append(row)

    contract = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "upstream_contract_id": UPSTREAM_CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "source_snapshot_sha256": source_hash,
        "source_state": source_state,
        "published_window_minutes": [-PRE_WINDOW_MIN, POST_WINDOW_MIN],
        "maximum_matches_per_episode": MAX_MATCHES_PER_EPISODE,
        "candidate_policy": "currency_overlap_story_dedup_ranked_by_availability_relevance_verification_quality_proximity",
        "causal_assignment_policy": "none_candidates_only",
        "research_only": True,
        "execution_eligible": False,
    }
    encoded = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json,builder_sha256,source_snapshot_sha256 FROM existing_source_reconciliation_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is None:
        connection.execute(
            "INSERT INTO existing_source_reconciliation_contracts VALUES (?,?,?,?,?)",
            (CONTRACT_ID, encoded, contract["builder_sha256"], source_hash, utc_now()),
        )
    elif tuple(existing) != (encoded, contract["builder_sha256"], source_hash):
        raise RuntimeError("immutable_existing_source_reconciliation_collision")

    statement = "INSERT OR IGNORE INTO unmatched_episode_source_candidates VALUES (" + ",".join("?" for _ in range(28)) + ")"
    for row in all_matches:
        connection.execute(
            statement,
            (
                row["match_id"], CONTRACT_ID, row["episode_id"], row["candidate_rank"], row["event_id"],
                row["source_id"], row["source_name"], row["source_kind"], row["source_quality"],
                row["source_verified"], row["published_utc"], row["first_seen_utc"], row["headline"],
                row["headline_normalized"], row["category"], row["source_url"], row["currencies_json"],
                row["availability_state"], row["relevance_state"], row["published_lead_minutes"],
                row["availability_lead_minutes"], row["factor_currency"], row["factor_sign"],
                1, 0, 0, 1, 0,
            ),
        )
    connection.commit()
    stored = int(connection.execute(
        "SELECT count(*) FROM unmatched_episode_source_candidates WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()[0])
    availability_counts = dict(connection.execute(
        "SELECT availability_state,count(*) FROM unmatched_episode_source_candidates WHERE contract_id=? GROUP BY availability_state",
        (CONTRACT_ID,),
    ).fetchall())
    relevance_counts = dict(connection.execute(
        "SELECT relevance_state,count(*) FROM unmatched_episode_source_candidates WHERE contract_id=? GROUP BY relevance_state",
        (CONTRACT_ID,),
    ).fetchall())
    verified_count = int(connection.execute(
        "SELECT count(*) FROM unmatched_episode_source_candidates WHERE contract_id=? AND source_verified=1",
        (CONTRACT_ID,),
    ).fetchone()[0])
    integrity = str(connection.execute("pragma integrity_check").fetchone()[0])
    connection.close()

    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "generated_utc": utc_now(),
        "source_snapshot_sha256": source_hash,
        "source_state": source_state,
        "episode_count": len(episodes),
        "candidate_match_count": stored,
        "episode_match_states": dict(sorted(episode_match_states.items())),
        "availability_counts": availability_counts,
        "relevance_counts": relevance_counts,
        "verified_source_candidate_count": verified_count,
        "causal_driver_assignments": 0,
        "source_candidates_only": True,
        "forecast_proof_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
        "sqlite_integrity": integrity,
    }
    snapshot["snapshot_sha256"] = sha256_bytes(canonical_json(snapshot).encode())
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "EXISTING_SOURCE_RECONCILIATION_V1.json").write_text(
        json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8"
    )
    lines = [
        "# Existing-source reconciliation V1", "",
        f"- Episodes: **{len(episodes):,}**.",
        f"- Candidate matches retained: **{stored:,}**.",
        f"- Episode states: `{canonical_json(dict(sorted(episode_match_states.items())))}`.",
        f"- Availability states: `{canonical_json(availability_counts)}`.",
        f"- Verified-source candidates: **{verified_count:,}**.",
        "- Causal drivers assigned: **0**. Matches are research pointers only.",
        "- Historical publications first observed after the move remain late/ex-post.",
        "- Execution decision: **no_trade**.",
    ]
    (report_root / "EXISTING_SOURCE_RECONCILIATION_V1.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(snapshot, indent=2, sort_keys=True))
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-db", type=Path, default=EVIDENCE_DB)
    parser.add_argument("--source-db", type=Path, default=SOURCE_DB)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    args = parser.parse_args()
    reconcile(args.evidence_db, args.source_db, args.report_root)


if __name__ == "__main__":
    main()
