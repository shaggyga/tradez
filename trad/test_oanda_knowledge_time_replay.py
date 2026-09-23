from __future__ import annotations

import sqlite3

from trad.oanda_knowledge_time_replay import replay


def test_replay_excludes_late_revision_until_observed(tmp_path):
    database = tmp_path / "source.sqlite"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE source_events(
          source_event_id TEXT, provider_event_id TEXT, source_id TEXT,
          story_cluster_id TEXT, decision_cutoff_utc TEXT, effective_from_utc TEXT,
          retrieved_at_utc TEXT, valid_until_utc TEXT, raw_payload_sha256 TEXT,
          source_contract_id TEXT, source_cohort_id TEXT, event_version INTEGER
        );
        CREATE TABLE source_supersession_events(
          prior_source_event_id TEXT, observed_utc TEXT
        );
        """
    )
    base = ("provider-1", "source-1", "story-1")
    connection.execute(
        "INSERT INTO source_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        ("v1", *base, "2026-08-08T10:00:00+00:00", "2026-08-08T10:00:00+00:00", "2026-08-08T10:00:01+00:00", None, "hash1", "contract1", "cohort1", 1),
    )
    connection.execute(
        "INSERT INTO source_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        ("v2", *base, "2026-08-08T10:00:00+00:00", "2026-08-08T10:00:00+00:00", "2026-08-08T11:00:00+00:00", None, "hash2", "contract1", "cohort1", 2),
    )
    connection.execute(
        "INSERT INTO source_supersession_events VALUES (?,?)",
        ("v1", "2026-08-08T11:00:00+00:00"),
    )
    connection.commit(); connection.close()
    before = replay(database, "2026-08-08T10:30:00+00:00")
    after = replay(database, "2026-08-08T11:30:00+00:00")
    assert [row["source_event_id"] for row in before["events"]] == ["v1"]
    assert [row["source_event_id"] for row in after["events"]] == ["v2"]


def test_replay_prefers_governed_story_cluster_assignment(tmp_path):
    database = tmp_path / "source.sqlite"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE source_events(
          source_event_id TEXT, provider_event_id TEXT, source_id TEXT,
          story_cluster_id TEXT, decision_cutoff_utc TEXT, effective_from_utc TEXT,
          retrieved_at_utc TEXT, valid_until_utc TEXT, raw_payload_sha256 TEXT,
          source_contract_id TEXT, source_cohort_id TEXT, event_version INTEGER
        );
        CREATE TABLE source_supersession_events(prior_source_event_id TEXT, observed_utc TEXT);
        CREATE TABLE source_story_cluster_assignments(
          assignment_id TEXT,source_event_id TEXT,clustering_contract_id TEXT,
          story_cluster_id TEXT,assigned_utc TEXT,method TEXT,assignment_json TEXT
        );
        """
    )
    connection.execute(
        "INSERT INTO source_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        ("v1", "provider-1", "source-1", "bad_topic_signature", "2026-08-08T10:00:00+00:00", "2026-08-08T10:00:00+00:00", "2026-08-08T10:00:01+00:00", None, "hash1", "contract1", "cohort1", 1),
    )
    connection.execute(
        "INSERT INTO source_story_cluster_assignments VALUES (?,?,?,?,?,?,?)",
        ("a1", "v1", "story_cluster_v2_event_lineage_or_headline_20260808", "actual_story", "2026-08-08T12:00:00+00:00", "test", "{}"),
    )
    connection.commit(); connection.close()
    result = replay(database, "2026-08-08T11:00:00+00:00")
    assert result["independent_story_cluster_count"] == 1
    assert result["events"][0]["story_cluster_id"] == "actual_story"


def test_replay_excludes_quarantined_source_event(tmp_path):
    database = tmp_path / "source.sqlite"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE source_events(
          source_event_id TEXT, provider_event_id TEXT, source_id TEXT,
          story_cluster_id TEXT, decision_cutoff_utc TEXT, effective_from_utc TEXT,
          retrieved_at_utc TEXT, valid_until_utc TEXT, raw_payload_sha256 TEXT,
          source_contract_id TEXT, source_cohort_id TEXT, event_version INTEGER
        );
        CREATE TABLE source_supersession_events(
          prior_source_event_id TEXT, next_source_event_id TEXT, observed_utc TEXT
        );
        CREATE TABLE source_event_quarantines(source_event_id TEXT);
        CREATE VIEW source_events_causal_v1 AS
          SELECT event.* FROM source_events AS event
          WHERE NOT EXISTS (
            SELECT 1 FROM source_event_quarantines AS quarantine
            WHERE quarantine.source_event_id=event.source_event_id
          );
        CREATE VIEW source_supersession_events_causal_v1 AS
          SELECT supersession.* FROM source_supersession_events AS supersession
          WHERE NOT EXISTS (
            SELECT 1 FROM source_event_quarantines AS quarantine
            WHERE quarantine.source_event_id=supersession.prior_source_event_id
               OR quarantine.source_event_id=supersession.next_source_event_id
          );
        """
    )
    base = (
        "v0", "provider-1", "source-1", "story-1",
        "2026-08-08T10:00:00+00:00", "2026-08-08T10:00:00+00:00",
        "2026-08-08T10:00:01+00:00", None, "hash1", "contract1", "cohort1", 1,
    )
    quarantined = (
        "v1", "provider-1", "source-1", "story-1",
        "2026-08-08T10:30:00+00:00", "2026-08-08T10:30:00+00:00",
        "2026-08-08T10:30:01+00:00", None, "hash2", "contract1", "cohort1", 2,
    )
    connection.execute("INSERT INTO source_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", base)
    connection.execute("INSERT INTO source_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", quarantined)
    connection.execute("INSERT INTO source_event_quarantines VALUES (?)", ("v1",))
    connection.execute(
        "INSERT INTO source_supersession_events VALUES (?,?,?)",
        ("v0", "v1", "2026-08-08T10:30:01+00:00"),
    )
    connection.commit(); connection.close()

    result = replay(database, "2026-08-08T11:00:00+00:00")

    assert result["source_event_count"] == 1
    assert [row["source_event_id"] for row in result["events"]] == ["v0"]
