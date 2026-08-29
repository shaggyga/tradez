from __future__ import annotations

import json
import hashlib
import sqlite3
from pathlib import Path

from trad.oanda_source_governance import (
    LOCAL_NEWS_COLLECTOR_CONTRACT_ID,
    OBSERVATION_TIME_CONTRACT_ID,
    SOURCE_EVENT_COLUMNS,
    STORY_CLUSTER_CONTRACT_ID,
    _event_payload,
    article_events,
    backfill_article_substantive_identities,
    canonical_story_cluster,
    cftc_substantive_payload,
    connect_registry,
    contract_for,
    governance_progress_payload,
    insert_cftc_events,
    import_official_fast_lane_events,
    insert_source_event,
    insert_treasury_events,
    merge_collector_runtime,
    official_fast_lane_source_ids,
    run,
    source_card,
    source_reliability_statistics,
    stable_hash,
    substantive_article_payload,
    treasury_observations,
)
from trad.oanda_local_news_sentiment import source_config_lineage
from trad.oanda_official_release_fast_lane import open_database as open_fast_database
from trad.oanda_official_release_fast_lane_contract import (
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC,
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_ACTIVATED_UTC,
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_CONTRACT_ID,
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
    OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
)
from trad.oanda_project_integrity_audit import (
    source_governance_fast_lane_adapter_integrity,
)


def proof_payload(value: dict) -> dict:
    return {
        **value,
        "collector_contract_id": LOCAL_NEWS_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": LOCAL_NEWS_COLLECTOR_CONTRACT_ID,
        "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
        "observation_clock_trusted": True,
        "observation_clock_source": "test_clock",
    }


def test_fast_lane_source_ids_include_separate_statistical_class() -> None:
    mapping = {
        "currencies": [
            {
                "currency": "EUR",
                "release_source_ids": ["ecb_press"],
                "communication_source_ids": ["ecb_speeches"],
                "statistical_release_source_ids": [
                    "ecb_statistical_press_releases"
                ],
            },
            {
                "currency": "USD",
                "release_source_ids": ["fed_monetary_policy"],
                "communication_source_ids": ["fed_speeches"],
                "statistical_release_source_ids": [
                    "dol_eta_ui_claims_scheduled_direct_pdf_v1"
                ],
            },
        ]
    }

    assert official_fast_lane_source_ids(mapping) == {
        "ecb_press",
        "ecb_speeches",
        "ecb_statistical_press_releases",
        "fed_monetary_policy",
        "fed_speeches",
        "dol_eta_ui_claims_scheduled_direct_pdf_v1",
    }


def _fast_lane_fixture_source() -> tuple[dict, dict]:
    source = {
        "source_id": "boj_updates",
        "name": "Bank of Japan updates",
        "kind": "rss",
        "source_role": "primary_policy_release",
        "currencies": ["JPY"],
        "verified": True,
        "direct": True,
        "license_class": "public_official_research_use",
    }
    return source_config_lineage(source), contract_for(source)


def _insert_fast_lane_fixture(
    database: Path,
    *,
    first_seen_utc: str,
    published_utc: str,
    raw_overrides: dict | None = None,
    column_overrides: dict | None = None,
) -> str:
    lineage, _ = _fast_lane_fixture_source()
    raw = {
        "headline": "Summary of Opinions at the Monetary Policy Meeting",
        "summary": "The Bank should continue to raise the policy interest rate.",
        "source_url": "https://www.boj.or.jp/en/mopo/mpmsche_minu/opinion_2026.htm",
        "published_utc": published_utc,
        **(raw_overrides or {}),
    }
    payload_json = json.dumps(raw, sort_keys=True, separators=(",", ":"), default=str)
    material_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    item_key = hashlib.sha256(str(raw["source_url"]).encode("utf-8")).hexdigest()
    observation_id = hashlib.sha256(
        f"boj_updates|{item_key}|{material_sha256}".encode("utf-8")
    ).hexdigest()
    values = {
        "observation_id": observation_id,
        "source_id": "boj_updates",
        "source_contract_id": lineage["source_contract_id"],
        "source_cohort_id": lineage["source_cohort_id"],
        "item_key": item_key,
        "material_sha256": material_sha256,
        "first_seen_utc": first_seen_utc,
        "prospective_observation": 1,
        "listing_bootstrap": 0,
        "identity_preexisting": 0,
        "publisher_time_eligible": 1,
        "observation_clock_trusted": 1,
        "observation_clock_source": "normalized_broker_clock",
        "raw_payload_json": payload_json,
        "research_only": 1,
        "execution_eligible": 0,
        "can_authorize": 0,
        "collector_contract_id": OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
        "collector_cohort_id": OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
    }
    values.update(column_overrides or {})
    columns = tuple(values)
    with open_fast_database(database) as connection:
        connection.execute(
            f"INSERT INTO official_release_observation "
            f"({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
            tuple(values[column] for column in columns),
        )
        connection.commit()
    return observation_id


def test_source_governance_binds_active_collector_contract() -> None:
    from trad import oanda_local_news_sentiment as news

    assert LOCAL_NEWS_COLLECTOR_CONTRACT_ID == news.COLLECTOR_CONTRACT_ID


def test_build_progress_rebinds_current_collector_contract() -> None:
    progress = governance_progress_payload(
        {
            "news_collector_contract_id": "stale-contract",
            "news_collector_cohort_id": "stale-cohort",
        },
        phase="backfilling_substantive_replay_index",
        progress={"processed": 10, "total": 100},
        configured_news_source_count=102,
        configured_source_count=106,
        incremental_article_scan=True,
        article_scan_since_utc="2026-08-24T13:00:00Z",
    )
    assert progress["status"] == "building_governance"
    assert progress["news_collector_contract_id"] == (
        LOCAL_NEWS_COLLECTOR_CONTRACT_ID
    )
    assert progress["news_collector_cohort_id"] == (
        LOCAL_NEWS_COLLECTOR_CONTRACT_ID
    )
    assert progress["can_place_orders"] is False


def test_fast_lane_governance_adapter_rejects_audited_boj_preactivation_row(
    tmp_path: Path,
) -> None:
    fast_database = tmp_path / "fast.sqlite"
    _insert_fast_lane_fixture(
        fast_database,
        first_seen_utc="2026-08-27T03:57:07.220000+00:00",
        published_utc="2026-08-27T01:35:00+00:00",
    )
    lineage, contract = _fast_lane_fixture_source()
    registry = connect_registry(tmp_path / "registry.sqlite")
    result = import_official_fast_lane_events(
        registry,
        fast_lane_database=fast_database,
        allowed_source_ids={"boj_updates"},
        expected_source_lineages={"boj_updates": lineage},
        governance_contracts={"boj_updates": contract},
        observed_utc="2026-08-28T16:00:00+00:00",
    )
    assert result["rows_scanned"] == 1
    assert result["inserted_versions"] == 0
    assert result["rejections"] == {
        "preactivation_or_invalid_first_seen": 1
    }
    assert registry.execute("SELECT COUNT(*) FROM source_events").fetchone()[0] == 0
    assert registry.execute(
        "SELECT COUNT(*) FROM official_fast_lane_governance_imports"
    ).fetchone()[0] == 0
    registry.close()


def test_fast_lane_governance_adapter_filters_are_fail_closed(
    tmp_path: Path,
) -> None:
    lineage, contract = _fast_lane_fixture_source()
    cases = (
        ("not_prospective", {"prospective_observation": 0}, {}, "not_prospective"),
        ("bootstrap", {"listing_bootstrap": 1}, {}, "listing_bootstrap"),
        ("preexisting", {"identity_preexisting": 1}, {}, "identity_preexisting"),
        ("publisher_untrusted", {"publisher_time_eligible": 0}, {}, "publisher_clock_untrusted"),
        ("clock_untrusted", {"observation_clock_trusted": 0}, {}, "observation_clock_untrusted"),
        ("empty_contract", {"source_contract_id": ""}, {}, "empty_source_contract"),
        ("wrong_contract", {"source_contract_id": "wrong"}, {}, "source_contract_mismatch"),
        ("wrong_collector", {"collector_contract_id": "wrong"}, {}, "collector_contract_mismatch"),
        (
            "retained_prior_collector",
            {
                "collector_contract_id": OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
                "collector_cohort_id": OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
            },
            {},
            "retained_prior_collector_contract_diagnostic_only",
        ),
        (
            "old_publisher",
            {},
            {"published_utc": "2026-08-28T14:59:59+00:00"},
            "publisher_clock_outside_adapter_window",
        ),
    )
    for name, columns, raw, reason in cases:
        fast_database = tmp_path / f"{name}.sqlite"
        _insert_fast_lane_fixture(
            fast_database,
            first_seen_utc="2026-08-28T15:00:10+00:00",
            published_utc="2026-08-28T15:00:05+00:00",
            raw_overrides=raw,
            column_overrides=columns,
        )
        registry = connect_registry(tmp_path / f"registry-{name}.sqlite")
        result = import_official_fast_lane_events(
            registry,
            fast_lane_database=fast_database,
            allowed_source_ids={"boj_updates"},
            expected_source_lineages={"boj_updates": lineage},
            governance_contracts={"boj_updates": contract},
            observed_utc="2026-08-28T16:00:00+00:00",
        )
        assert result["inserted_versions"] == 0, name
        assert result["rejections"] == {reason: 1}, name
        registry.close()


def test_fast_lane_governance_adapter_versions_later_body_and_pdf_without_backdating(
    tmp_path: Path,
) -> None:
    fast_database = tmp_path / "fast.sqlite"
    body = "The Bank should continue to raise the policy interest rate."
    pdf = "Full text with policy discussion."
    observation_id = _insert_fast_lane_fixture(
        fast_database,
        first_seen_utc="2026-08-28T15:00:10+00:00",
        published_utc="2026-08-28T15:00:05+00:00",
        raw_overrides={
            "detail_enriched": True,
            "detail_available_utc": "2026-08-28T15:01:00+00:00",
            "detail_content_sha256": hashlib.sha256(body.encode()).hexdigest(),
            "detail_parser_contract_id": "official_html_text_v1",
            "summary": body,
            "detail_attachments": [
                {
                    "available_utc": "2026-08-28T15:02:00+00:00",
                    "content_sha256": hashlib.sha256(pdf.encode()).hexdigest(),
                    "content_bytes": len(pdf),
                    "text_characters": len(pdf),
                    "parser_contract_id": "official_pdf_text_v1",
                    "url": "https://www.boj.or.jp/en/mopo/mpmsche_minu/opinion.pdf",
                }
            ],
        },
    )
    lineage, contract = _fast_lane_fixture_source()
    registry_path = tmp_path / "registry.sqlite"
    registry = connect_registry(registry_path)
    arguments = {
        "fast_lane_database": fast_database,
        "allowed_source_ids": {"boj_updates"},
        "expected_source_lineages": {"boj_updates": lineage},
        "governance_contracts": {"boj_updates": contract},
        "observed_utc": "2026-08-28T16:00:00+00:00",
    }
    first = import_official_fast_lane_events(registry, **arguments)
    assert first["eligible_root_observations"] == 1
    assert first["inserted_versions"] == 3
    assert first["inserted_listing_versions"] == 1
    assert first["inserted_detail_versions"] == 1
    assert first["inserted_attachment_versions"] == 1
    receipts = registry.execute(
        "SELECT observation_id,material_kind,effective_from_utc,material_sha256,"
        "research_only,execution_eligible,can_authorize "
        "FROM official_fast_lane_governance_imports "
        "ORDER BY effective_from_utc"
    ).fetchall()
    assert [row[0] for row in receipts] == [observation_id] * 3
    assert [row[1] for row in receipts] == [
        "listing_observation", "official_body_detail", "official_pdf_attachment"
    ]
    assert [row[2] for row in receipts] == [
        "2026-08-28T15:00:10+00:00",
        "2026-08-28T15:01:00+00:00",
        "2026-08-28T15:02:00+00:00",
    ]
    assert len({row[3] for row in receipts}) == 3
    assert {(row[4], row[5], row[6]) for row in receipts} == {(1, 0, 0)}
    versions = registry.execute(
        "SELECT event_version,event_type,effective_from_utc FROM source_events "
        "ORDER BY event_version"
    ).fetchall()
    assert versions == [
        (1, "listing_observation", "2026-08-28T15:00:10+00:00"),
        (2, "official_body_detail", "2026-08-28T15:01:00+00:00"),
        (3, "official_pdf_attachment", "2026-08-28T15:02:00+00:00"),
    ]
    second = import_official_fast_lane_events(registry, **arguments)
    assert second["inserted_versions"] == 0
    assert second["duplicate_receipts"] == 3
    assert registry.execute("SELECT COUNT(*) FROM source_events").fetchone()[0] == 3
    registry.commit()
    for statement in (
        "UPDATE official_fast_lane_governance_imports SET source_id='x'",
        "DELETE FROM official_fast_lane_governance_imports",
    ):
        try:
            registry.execute(statement)
        except sqlite3.IntegrityError as error:
            assert "append_only" in str(error)
        else:
            raise AssertionError("fast-lane import receipt history was mutable")
    registry.close()
    adapter_state = {
        "status": "ok",
        "research_only": True,
        "can_place_orders": False,
        "real_money_routing": False,
        "official_fast_lane_governance_adapter_contract_id": (
            OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID
        ),
        "official_fast_lane_governance_adapter_activated_utc": (
            OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC.isoformat()
        ),
        "official_fast_lane_source_governance_adapter": {
            **first,
            "allowed_official_source_count": 24,
            "active_source_lineage_count": 24,
        },
    }
    integrity = source_governance_fast_lane_adapter_integrity(
        adapter_state, database_path=registry_path
    )
    assert integrity["ok"] is True
    assert integrity["receipt_count"] == 3
    assert integrity["preactivation_receipts"] == 0
    assert integrity["backdated_effective_receipts"] == 0


def test_fast_lane_integrity_retains_prior_adapter_receipts_by_exact_cohort(
    monkeypatch, tmp_path: Path
) -> None:
    from trad import oanda_source_governance as governance

    fast_database = tmp_path / "prior-fast.sqlite"
    _insert_fast_lane_fixture(
        fast_database,
        first_seen_utc="2026-08-27T06:00:10+00:00",
        published_utc="2026-08-27T06:00:05+00:00",
        column_overrides={
            "collector_contract_id": OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
            "collector_cohort_id": OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
        },
    )
    lineage, contract = _fast_lane_fixture_source()
    registry_path = tmp_path / "registry.sqlite"
    registry = connect_registry(registry_path)
    with monkeypatch.context() as prior:
        prior.setattr(
            governance,
            "OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID",
            OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_CONTRACT_ID,
        )
        prior.setattr(
            governance,
            "OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC",
            OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_ACTIVATED_UTC,
        )
        prior.setattr(
            governance,
            "OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID",
            OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
        )
        prior.setattr(
            governance,
            "OFFICIAL_RELEASE_FAST_LANE_COHORT_ID",
            OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
        )
        imported = governance.import_official_fast_lane_events(
            registry,
            fast_lane_database=fast_database,
            allowed_source_ids={"boj_updates"},
            expected_source_lineages={"boj_updates": lineage},
            governance_contracts={"boj_updates": contract},
            observed_utc="2026-08-27T07:00:00+00:00",
        )
    assert imported["inserted_versions"] == 1
    registry.commit()
    registry.close()

    activated = OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC.isoformat()
    state = {
        "status": "ok",
        "research_only": True,
        "can_place_orders": False,
        "real_money_routing": False,
        "official_fast_lane_governance_adapter_contract_id": (
            OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID
        ),
        "official_fast_lane_governance_adapter_activated_utc": activated,
        "official_fast_lane_source_governance_adapter": {
            "status": "ok",
            "adapter_contract_id": OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
            "adapter_activated_utc": activated,
            "upstream_collector_contract_id": OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
            "upstream_collector_cohort_id": OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
            "allowed_official_source_count": 42,
            "active_source_lineage_count": 42,
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "can_authorize": False,
        },
    }
    integrity = source_governance_fast_lane_adapter_integrity(
        state, database_path=registry_path
    )
    assert integrity["ok"] is True
    assert integrity["receipt_count"] == 1
    assert integrity["active_receipt_count"] == 0
    assert integrity["retained_prior_receipt_count"] == 1
    assert integrity["unknown_adapter_receipt_count"] == 0


def test_source_reliability_uses_last_card_per_source_day() -> None:
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE source_card_snapshots("
        "snapshot_id TEXT,observed_utc TEXT,source_id TEXT,"
        "source_contract_id TEXT,card_json TEXT)"
    )
    connection.execute(
        "CREATE TABLE source_events_causal_v1("
        "source_id TEXT,source_event_id TEXT,story_cluster_id TEXT,"
        "event_version INTEGER,latency_ms INTEGER)"
    )
    degraded = {
        "missingness_state": "degraded_or_missing",
        "last_error": "temporary",
        "causal_timestamp_quality": "clock_untrusted_or_not_currently_collecting",
        "outage_state": "error",
    }
    available = {
        "missingness_state": "available",
        "last_error": "",
        "causal_timestamp_quality": "first_seen_prospective_clock_bound",
        "outage_state": "ok",
    }
    configured_only = {
        "missingness_state": "degraded_or_missing",
        "last_error": "",
        "causal_timestamp_quality": "clock_untrusted_or_not_currently_collecting",
        "outage_state": "configured_not_observed",
    }
    connection.executemany(
        "INSERT INTO source_card_snapshots VALUES(?,?,?,?,?)",
        [
            ("pre", "2026-08-17T20:00:00Z", "official", "v1", json.dumps(configured_only)),
            ("a", "2026-08-18T10:00:00Z", "official", "v1", json.dumps(degraded)),
            ("b", "2026-08-18T20:00:00Z", "official", "v1", json.dumps(available)),
            ("c", "2026-08-19T20:00:00Z", "official", "v1", json.dumps(degraded)),
        ],
    )
    connection.executemany(
        "INSERT INTO source_events_causal_v1 VALUES(?,?,?,?,?)",
        [
            ("official", "event-1", "story-1", 1, 100),
            ("official", "event-2", "story-1", 2, 300),
            ("official", "event-3", "story-2", 1, None),
        ],
    )
    [result] = source_reliability_statistics(connection)
    assert result["registered_observation_days"] == 3
    assert result["observation_days"] == 2
    assert result["available_days"] == 1
    assert result["availability_day_rate"] == 0.5
    assert result["clock_trusted_days"] == 1
    assert result["causal_event_versions"] == 3
    assert result["independent_story_clusters"] == 2
    assert result["revision_versions"] == 1
    assert result["mean_latency_ms"] == 200.0
    assert result["sample_quality"] == "limited_sample_current_state"


def test_article_without_exact_clock_provenance_is_not_rebound() -> None:
    contract = contract_for(
        {
            "source_id": "official",
            "name": "Official",
            "kind": "rss",
            "source_role": "primary_statistical_release",
            "currencies": ["USD"],
        }
    )
    row = {
        "event_id": "old-row",
        "source_id": "official",
        "source_name": "Official",
        "source_kind": "rss",
        "source_quality": 1.0,
        "source_verified": 1,
        "published_utc": "2026-08-17T10:00:00+00:00",
        "first_seen_utc": "2026-08-17T10:00:01+00:00",
        "last_seen_utc": "2026-08-17T10:00:01+00:00",
        "headline": "Official release",
        "summary": "Facts",
        "category": "inflation",
        "scope": "currency",
        "currencies_json": '["USD"]',
        "generic_sentiment_score": 0.0,
        "directional_confidence": 0.0,
        "severity": 0.0,
        "movement_potential": "LOW",
        "duplicate_count": 0,
        "payload_json": json.dumps({"headline": "Official release"}),
    }
    assert _event_payload(row, contract, observed_utc=row["last_seen_utc"]) is None
    row["payload_json"] = json.dumps(proof_payload({"headline": "Official release"}))
    event = _event_payload(row, contract, observed_utc=row["last_seen_utc"])
    assert event is not None
    assert event["source_cohort_id"] == contract["source_cohort_id"]

    wrong = proof_payload({"headline": "Official release"})
    wrong["collector_contract_id"] = "mismatched-collector-contract"
    row["payload_json"] = json.dumps(wrong)
    assert _event_payload(row, contract, observed_utc=row["last_seen_utc"]) is None


def test_incremental_scan_reconsiders_verified_structured_rows_without_retimestamping(tmp_path: Path) -> None:
    database = tmp_path / "news.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """CREATE TABLE articles (
            event_id TEXT,source_id TEXT,source_name TEXT,source_kind TEXT,
            source_quality REAL,source_verified INTEGER,published_utc TEXT,
            first_seen_utc TEXT,last_seen_utc TEXT,headline TEXT,summary TEXT,
            category TEXT,scope TEXT,currencies_json TEXT,
            generic_sentiment_score REAL,directional_confidence REAL,
            severity REAL,movement_potential TEXT,duplicate_count INTEGER,
            payload_json TEXT
        )"""
    )
    common = (
        "official", "Official", "html", 1.0, 1,
        "2026-08-01T00:00:00Z", "2026-08-01T00:01:00Z",
        "2026-08-01T00:02:00Z", "summary", "macro_release", "currency",
        '["USD"]', 0.0, 0.0, 1.0, "HIGH", 0,
    )
    connection.execute(
        "INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("structured", *common[:7], common[7], "CPI rose 3.0%", *common[8:], json.dumps({"structured_event": True, "actual_value": 3.0})),
    )
    connection.execute(
        "INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("ordinary", *common[:7], common[7], "Ordinary old row", *common[8:], json.dumps({"structured_event": False})),
    )
    connection.commit()
    connection.close()

    rows = list(
        article_events(
            database,
            changed_after_utc="2026-08-16T00:00:00Z",
        )
    )

    assert [row["event_id"] for row in rows] == ["structured"]
    assert rows[0]["first_seen_utc"] == "2026-08-01T00:01:00Z"
    assert rows[0]["last_seen_utc"] == "2026-08-01T00:02:00Z"


def test_collector_runtime_adds_non_currency_sources_without_masking_http_failure() -> None:
    runtime = merge_collector_runtime(
        {},
        {
            "sources": [
                {"source_id": "alpha", "status": "not_due", "last_status": 200},
                {"source_id": "gdelt", "status": "not_due", "last_status": 429},
                {"source_id": "paid", "status": "credential_missing", "last_status": 401},
            ]
        },
    )

    assert runtime["alpha"]["operational"] is True
    assert runtime["alpha"]["healthy"] is True
    assert runtime["gdelt"]["operational"] is True
    assert runtime["gdelt"]["healthy"] is False
    assert runtime["paid"]["operational"] is False
    assert runtime["paid"]["healthy"] is False


def test_registry_indexes_source_events_by_effective_time(tmp_path: Path) -> None:
    connection = connect_registry(tmp_path / "registry.sqlite")
    indexes = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='index' AND tbl_name='source_events'"
        )
    }
    connection.close()

    assert "ix_source_events_effective" in indexes


def test_substantive_identity_backfill_is_resumable(tmp_path: Path) -> None:
    connection = connect_registry(tmp_path / "registry.sqlite")
    source = {
        "source_id": "official",
        "name": "Official",
        "kind": "rss",
        "source_role": "primary",
        "currencies": ["CAD"],
    }
    contract = contract_for(source)
    connection.execute(
        "INSERT INTO source_contracts VALUES (?,?,?,?,?,?)",
        (
            contract["source_contract_id"],
            "official",
            contract["source_cohort_id"],
            "now",
            "sha",
            "{}",
        ),
    )
    rows = []
    for number in range(3):
        observed = f"2026-08-15T12:00:0{number}+00:00"
        row = {
            "event_id": f"article-{number}",
            "source_id": "official",
            "source_name": "Official",
            "source_kind": "rss",
            "source_quality": 1.0,
            "source_verified": 1,
            "published_utc": observed,
            "first_seen_utc": observed,
            "last_seen_utc": observed,
            "headline": f"Release {number}",
            "summary": "",
            "category": "monetary_policy",
            "scope": "currency",
            "currencies_json": '["CAD"]',
            "generic_sentiment_score": 0.0,
            "directional_confidence": 0.0,
            "severity": 0.0,
            "movement_potential": "LOW",
            "duplicate_count": 0,
            "payload_json": json.dumps(
                proof_payload({"headline": f"Release {number}"})
            ),
        }
        rows.append(row)
        event = _event_payload(row, contract, observed_utc=observed)
        connection.execute(
            f"INSERT INTO source_events ({','.join(SOURCE_EVENT_COLUMNS)}) "
            f"VALUES ({','.join('?' for _ in SOURCE_EVENT_COLUMNS)})",
            tuple(event.get(column) for column in SOURCE_EVENT_COLUMNS),
        )
    connection.commit()
    progress: list[dict[str, int]] = []
    first = backfill_article_substantive_identities(
        connection,
        rows,
        {"official": contract},
        observed_utc="2026-08-15T13:00:00+00:00",
        batch_size=2,
        progress_callback=lambda value: progress.append(dict(value)),
    )
    assert first == {
        "processed": 3,
        "total": 3,
        "linked": 3,
        "already_indexed": 0,
    }
    assert [item["processed"] for item in progress] == [2, 3]
    second = backfill_article_substantive_identities(
        connection,
        rows,
        {"official": contract},
        observed_utc="2026-08-15T14:00:00+00:00",
        batch_size=2,
    )
    assert second["linked"] == 0
    assert second["already_indexed"] == 3
    assert connection.execute(
        "SELECT COUNT(*) FROM source_event_substantive_identities"
    ).fetchone()[0] == 3
    connection.close()


def test_enriched_official_detail_cannot_backdate_direction() -> None:
    row = {
        "event_id": "boj-summary",
        "source_name": "BOJ",
        "source_kind": "rss",
        "source_quality": 1.0,
        "source_verified": 1,
        "published_utc": "2026-08-09T23:50:00+00:00",
        "first_seen_utc": "2026-08-10T05:14:58+00:00",
        "last_seen_utc": "2026-08-10T14:17:04+00:00",
        "headline": "Summary of Opinions",
        "summary": "The Bank should continue raising the policy rate.",
        "category": "monetary_policy",
        "scope": "currency",
        "currencies_json": json.dumps(["JPY"]),
        "generic_sentiment_score": 0.0,
        "directional_confidence": 0.95,
        "severity": 90.0,
        "movement_potential": "HIGH",
        "duplicate_count": 0,
        "payload_json": json.dumps(
            proof_payload({
                "detail_enriched": True,
                "detail_available_utc": "2026-08-10T14:17:04+00:00",
                "currency_scores": {"JPY": 1.0},
            })
        ),
    }
    contract = {
        "source_id": "boj_updates",
        "information_population": "official_policy_publisher",
        "provider": "BOJ",
        "license_class": "public_first_party",
        "raw_payload_retention": "retain",
        "source_contract_id": "contract",
        "source_cohort_id": "cohort",
    }

    event = _event_payload(
        row,
        contract,
        observed_utc="2026-08-10T14:17:04+00:00",
    )

    assert event["first_seen_at_utc"] == "2026-08-10T05:14:58+00:00"
    assert event["effective_from_utc"] == "2026-08-10T14:17:04+00:00"
    assert event["decision_cutoff_utc"] == "2026-08-10T14:17:04+00:00"


def test_poll_and_classifier_metadata_do_not_manufacture_source_revision() -> None:
    base = {
        "headline": "Bank holds policy rate",
        "currency_scores": {"CAD": -0.2},
        "classification_version": "rules-v1",
        "collector_cohort_id": "collector-v1",
        "collector_contract_id": "collector-contract-v1",
        "forward_signal_timely": True,
        "last_seen_utc": "2026-08-15T12:00:00+00:00",
        "sentiment_schema_version": "sentiment-v1",
    }
    refreshed = dict(
        base,
        classification_version="rules-v2",
        collector_cohort_id="collector-v2",
        collector_contract_id="collector-contract-v2",
        forward_signal_timely=False,
        last_seen_utc="2026-08-15T13:00:00+00:00",
        sentiment_schema_version="sentiment-v2",
    )

    assert stable_hash(substantive_article_payload(base)) == stable_hash(
        substantive_article_payload(refreshed)
    )


def test_operational_lineage_and_availability_fields_do_not_version_source() -> None:
    base = {
        "headline": "Bank holds policy rate",
        "actual_value": 4.0,
        "availability_lag_minutes": 5.0,
        "causal_known_utc": "2026-08-16T09:00:00+00:00",
        "event_lineage_id": "lineage-a",
        "material_update_id": "update-a",
        "publication_hold_seconds": 60.0,
        "published_utc": "2026-08-16T08:59:00+00:00",
    }
    refreshed = dict(
        base,
        availability_lag_minutes=10.0,
        causal_known_utc="2026-08-16T10:00:00+00:00",
        event_lineage_id="lineage-b",
        material_update_id="update-b",
        publication_hold_seconds=3600.0,
        published_utc="",
    )

    assert stable_hash(substantive_article_payload(base)) == stable_hash(
        substantive_article_payload(refreshed)
    )


def test_nested_raw_collector_metadata_does_not_manufacture_revision() -> None:
    base = {
        "headline": "GDP increased by 0.4%",
        "summary": "Official quarterly release.",
        "raw_payload": {
            "headline": "GDP increased by 0.4%",
            "actual_value": 0.4,
            "classification_version": "rules-v1",
            "collector_contract_id": "collector-v1",
            "numeric_causal_known_utc": "2026-08-16T06:35:00+00:00",
            "retrieval_via": "official-feed",
            "source_direct": True,
            "source_id": "official-a",
            "source_role": "direct_publisher",
            "vendor_currencies": ["EUR", "USD"],
        },
    }
    refreshed = json.loads(json.dumps(base))
    refreshed["raw_payload"].update(
        {
            "classification_version": "rules-v2",
            "collector_contract_id": "collector-v2",
            "numeric_causal_known_utc": "2026-08-16T06:45:00+00:00",
            "retrieval_via": "fallback-search",
            "source_direct": False,
            "source_id": "discovery-a",
            "source_role": "aggregator_discovery",
            "vendor_currencies": [],
        }
    )

    assert stable_hash(substantive_article_payload(base)) == stable_hash(
        substantive_article_payload(refreshed)
    )

    changed = json.loads(json.dumps(refreshed))
    changed["raw_payload"]["actual_value"] = 0.5
    assert stable_hash(substantive_article_payload(base)) != stable_hash(
        substantive_article_payload(changed)
    )


def test_publisher_attribution_suffix_does_not_version_google_headline() -> None:
    first = {
        "headline": "Petrol Price in India | Check Latest Petrol Rates in India Today - Hindustan Times",
        "source_name": "Hindustan Times",
    }
    second = {
        "headline": "Petrol Price in India | Check Latest Petrol Rates in India Today - hindustantimes.com",
        "source_name": "hindustantimes.com",
    }

    assert stable_hash(substantive_article_payload(first)) == stable_hash(
        substantive_article_payload(second)
    )


def test_structured_numeric_enrichment_uses_numeric_knowledge_time() -> None:
    row = {
        "event_id": "eurostat-gdp",
        "source_name": "Eurostat",
        "source_kind": "official",
        "source_quality": 1.0,
        "source_verified": 1,
        "published_utc": "2026-07-30T09:00:00+00:00",
        "first_seen_utc": "2026-08-02T21:14:28+00:00",
        "last_seen_utc": "2026-08-16T06:35:00+00:00",
        "headline": "GDP up 0.4%",
        "summary": "Second-quarter GDP increased 0.4%.",
        "category": "growth",
        "scope": "currency",
        "currencies_json": json.dumps(["EUR"]),
        "generic_sentiment_score": 0.0,
        "directional_confidence": 0.0,
        "severity": 0.0,
        "movement_potential": "UNKNOWN",
        "duplicate_count": 0,
        "payload_json": json.dumps(
            proof_payload({
                "structured_event": True,
                "actual_value": 0.4,
                "numeric_causal_known_utc": "2026-08-16T06:35:00+00:00",
            })
        ),
    }
    contract = {
        "source_id": "eurostat_economy_finance",
        "information_population": "official_macro_publisher",
        "provider": "Eurostat",
        "license_class": "public_first_party",
        "raw_payload_retention": "retain",
        "source_contract_id": "contract",
        "source_cohort_id": "cohort",
    }

    event = _event_payload(row, contract, observed_utc="2026-08-16T06:35:00+00:00")

    assert event["first_seen_at_utc"] == "2026-08-02T21:14:28+00:00"
    assert event["effective_from_utc"] == "2026-08-16T06:35:00+00:00"
    assert event["decision_cutoff_utc"] == "2026-08-16T06:35:00+00:00"


def test_semantic_change_remains_a_real_source_event_revision() -> None:
    base = {
        "headline": "Bank holds policy rate",
        "currency_scores": {"CAD": -0.2},
        "classification_version": "rules-v1",
    }
    changed = dict(
        base,
        currency_scores={"CAD": 0.2},
        classification_version="rules-v2",
    )

    assert stable_hash(substantive_article_payload(base)) != stable_hash(
        substantive_article_payload(changed)
    )


def test_legacy_full_payload_hash_does_not_force_metadata_only_revision(
    tmp_path: Path,
) -> None:
    connection = connect_registry(tmp_path / "registry.sqlite")
    connection.execute(
        "INSERT INTO source_contracts VALUES (?,?,?,?,?,?)",
        ("contract", "official", "cohort", "now", "sha", "{}"),
    )
    contract = {
        "source_id": "official",
        "information_population": "official_macro_publisher",
        "provider": "Official",
        "license_class": "public",
        "raw_payload_retention": "retain",
        "source_contract_id": "contract",
        "source_cohort_id": "cohort",
    }
    row = {
        "event_id": "article-1",
        "source_name": "Official",
        "source_kind": "rss",
        "source_quality": 1.0,
        "source_verified": 1,
        "published_utc": "2026-08-15T12:00:00+00:00",
        "first_seen_utc": "2026-08-15T12:00:01+00:00",
        "last_seen_utc": "2026-08-15T12:00:01+00:00",
        "headline": "Bank holds policy rate",
        "summary": "",
        "category": "monetary_policy",
        "scope": "currency",
        "currencies_json": '["CAD"]',
        "generic_sentiment_score": -0.2,
        "directional_confidence": 0.6,
        "severity": 0.4,
        "movement_potential": "MEDIUM",
        "duplicate_count": 0,
        "payload_json": json.dumps(
            proof_payload({
                "headline": "Bank holds policy rate",
                "currency_scores": {"CAD": -0.2},
                "classification_version": "rules-v1",
                "last_seen_utc": "2026-08-15T12:00:01+00:00",
            })
        ),
    }
    legacy = _event_payload(row, contract, observed_utc=row["last_seen_utc"])
    legacy["raw_payload_sha256"] = stable_hash(json.loads(row["payload_json"]))
    assert insert_source_event(connection, legacy)

    refreshed = dict(row)
    refreshed["last_seen_utc"] = "2026-08-15T13:00:00+00:00"
    refreshed["payload_json"] = json.dumps(
        proof_payload({
            "headline": "Bank holds policy rate",
            "currency_scores": {"CAD": -0.2},
            "classification_version": "rules-v2",
            "last_seen_utc": "2026-08-15T13:00:00+00:00",
        })
    )
    current = _event_payload(
        refreshed,
        contract,
        observed_utc=refreshed["last_seen_utc"],
    )
    assert not insert_source_event(connection, current)
    assert connection.execute("SELECT COUNT(*) FROM source_events").fetchone()[0] == 1
    assert connection.execute(
        "SELECT COUNT(*) FROM source_event_substantive_identities"
    ).fetchone()[0] == 1
    changes_before = connection.total_changes
    assert not insert_source_event(connection, current)
    assert connection.total_changes == changes_before
    connection.close()


def _event(payload_hash: str, first_seen: str = "2026-08-08T12:00:01+00:00") -> dict:
    return {
        "source_event_id": f"event-{payload_hash}", "provider_event_id": "provider-1",
        "source_id": "official", "source_family": "rss",
        "source_population": "official_macro_publisher", "provider": "Official",
        "instrument": None, "base_currency": "USD", "quote_currency": None,
        "country": "US", "event_type": "inflation", "article_id": "provider-1",
        "story_cluster_id": "story-1", "event_id": "provider-1",
        "market_episode_id": None, "published_at_utc": "2026-08-08T12:00:00+00:00",
        "first_seen_at_utc": first_seen, "retrieved_at_utc": first_seen,
        "effective_from_utc": first_seen, "valid_until_utc": None,
        "revised_at_utc": None, "superseded_at_utc": None,
        "decision_cutoff_utc": first_seen, "source_clock_uncertainty_ms": None,
        "raw_value": 1.0, "normalized_value": 1.0, "coverage_count": 1,
        "source_dispersion": None, "quality_state": "verified", "latency_ms": 1000,
        "parser_version": "v1", "vendor_model_name": None,
        "vendor_model_version": None, "raw_payload_sha256": payload_hash,
        "license_class": "public", "retention_policy": "retain",
        "source_contract_id": "contract-1", "source_cohort_id": "cohort-1",
        "event_version": 1, "supersedes_source_event_id": None,
        "payload_json": json.dumps({"hash": payload_hash}),
    }


def test_source_event_versions_are_append_only(tmp_path: Path) -> None:
    connection = connect_registry(tmp_path / "registry.sqlite")
    connection.execute(
        "INSERT INTO source_contracts VALUES (?,?,?,?,?,?)",
        ("contract-1", "official", "cohort-1", "now", "sha", "{}"),
    )
    assert insert_source_event(connection, _event("a"))
    assert not insert_source_event(connection, _event("a"))
    assert insert_source_event(connection, _event("b", "2026-08-08T12:05:00+00:00"))
    # A previously observed payload may become current again when upstream
    # cohorts alternate.  Preserve the original immutable A and B versions,
    # but do not manufacture a third A version.
    assert not insert_source_event(
        connection, _event("a", "2026-08-08T12:10:00+00:00")
    )
    connection.commit()
    rows = connection.execute(
        "SELECT event_version,supersedes_source_event_id FROM source_events ORDER BY event_version"
    ).fetchall()
    assert rows == [(1, None), (2, "event-a")]
    assert connection.execute("SELECT COUNT(*) FROM source_supersession_events").fetchone()[0] == 1
    indexes = {
        row[1] for row in connection.execute(
            "PRAGMA index_list(source_supersession_events)"
        )
    }
    assert "ix_source_supersession_prior_observed" in indexes
    try:
        connection.execute("UPDATE source_events SET quality_state='bad'")
    except sqlite3.IntegrityError as exc:
        assert "append_only" in str(exc)
    else:
        raise AssertionError("source event update was not blocked")
    connection.close()


def test_consensus_capture_state_alone_does_not_version_article(
    tmp_path: Path,
) -> None:
    connection = connect_registry(tmp_path / "registry.sqlite")
    contract = {
        "source_id": "official",
        "source_family": "rss",
        "information_population": "official_macro_publisher",
        "provider": "Official",
        "raw_payload_retention": "retain",
        "license_class": "public",
        "source_contract_id": "contract-1",
        "source_cohort_id": "cohort-1",
    }
    row = {
        "event_id": "provider-1",
        "source_id": "official",
        "source_name": "Official",
        "source_kind": "rss",
        "source_quality": "primary",
        "source_verified": 1,
        "published_utc": "2026-08-08T12:00:00+00:00",
        "first_seen_utc": "2026-08-08T12:00:01+00:00",
        "last_seen_utc": "2026-08-08T12:00:01+00:00",
        "headline": "Official release",
        "summary": "Unchanged facts",
        "category": "inflation",
        "scope": "currency",
        "currencies_json": '["USD"]',
        "generic_sentiment_score": 0.0,
        "directional_confidence": 0.0,
        "severity": 0.0,
        "movement_potential": 0.0,
        "duplicate_count": 0,
        "payload_json": json.dumps(
            proof_payload(
                {"headline": "Official release", "consensus_capture_state": None}
            )
        ),
    }
    first = _event_payload(row, contract, observed_utc=row["last_seen_utc"])
    assert insert_source_event(connection, first)
    row["payload_json"] = json.dumps(
        proof_payload({
            "headline": "Official release",
            "consensus_capture_state": "",
            "detail_context_archive_only": True,
            "detail_archive_path": "local/archive/path.json",
            "numeric_extraction_contract_id": "numeric-v2",
            "post_window_minutes": 30,
            "relevance_window_minutes": 60,
            "schedule_window_end_utc": "2026-08-08T18:00:00Z",
            "semantic_claim_contract": "semantic-v2",
            "source_listing_bootstrap": True,
        })
    )
    second = _event_payload(row, contract, observed_utc=row["last_seen_utc"])
    assert first["raw_payload_sha256"] == second["raw_payload_sha256"]
    assert not insert_source_event(connection, second)
    assert connection.execute("SELECT COUNT(*) FROM source_events").fetchone()[0] == 1
    connection.close()


def test_source_contract_rollout_alone_does_not_version_article(
    tmp_path: Path,
) -> None:
    connection = connect_registry(tmp_path / "registry.sqlite")
    contract = {
        "source_id": "official",
        "source_family": "rss",
        "information_population": "official_macro_publisher",
        "provider": "Official",
        "raw_payload_retention": "retain",
        "license_class": "public",
        "source_contract_id": "registry-contract",
        "source_cohort_id": "registry-cohort",
    }
    row = {
        "event_id": "provider-1",
        "source_id": "official",
        "source_name": "Official",
        "source_kind": "rss",
        "source_quality": "primary",
        "source_verified": 1,
        "published_utc": "2026-08-08T12:00:00+00:00",
        "first_seen_utc": "2026-08-08T12:00:01+00:00",
        "last_seen_utc": "2026-08-08T12:00:01+00:00",
        "headline": "Official release",
        "summary": "Unchanged facts",
        "category": "inflation",
        "scope": "currency",
        "currencies_json": '["USD"]',
        "generic_sentiment_score": 0.0,
        "directional_confidence": 0.0,
        "severity": 0.0,
        "movement_potential": 0.0,
        "duplicate_count": 0,
        "payload_json": json.dumps(
            proof_payload({
                "headline": "Official release",
                "source_contract_id": "collector-v1",
                "source_cohort_id": "cohort-v1",
            })
        ),
    }
    first = _event_payload(row, contract, observed_utc=row["last_seen_utc"])
    assert insert_source_event(connection, first)
    row["payload_json"] = json.dumps(
        proof_payload({
            "headline": "Official release",
            "source_contract_id": "collector-v2",
            "source_cohort_id": "cohort-v2",
        })
    )
    second = _event_payload(row, contract, observed_utc=row["last_seen_utc"])
    assert first["raw_payload_sha256"] == second["raw_payload_sha256"]
    assert not insert_source_event(connection, second)
    assert connection.execute("SELECT COUNT(*) FROM source_events").fetchone()[0] == 1
    connection.close()


def test_run_builds_cards_and_causal_events(tmp_path: Path) -> None:
    config = tmp_path / "sources.json"
    config.write_text(json.dumps({"sources": [{
        "source_id": "official", "name": "Official", "kind": "rss",
        "currencies": ["USD"], "verified": True,
        "source_role": "primary_statistical_release",
    }]}), encoding="utf-8")
    coverage = tmp_path / "coverage.json"
    coverage.write_text(json.dumps({"currencies": {"USD": {"sources": [{
        "source_id": "official", "name": "Official", "kind": "rss",
        "source_role": "primary_statistical_release", "runtime_status": "enabled",
        "operational": True, "healthy": True, "last_error": "",
        }]}}}), encoding="utf-8")
    coverage.with_name("collector_latest_v1.json").write_text(
        json.dumps(
            {
                "observation_clock": {
                    "contract_id": OBSERVATION_TIME_CONTRACT_ID,
                    "trusted_for_prospective_evidence": True,
                },
                "sources": [
                    {
                        "source_id": "official",
                        "status": "ok",
                        "http_status": 200,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    news = tmp_path / "news.sqlite"
    connection = sqlite3.connect(news)
    connection.execute("""CREATE TABLE articles (
        event_id TEXT,source_id TEXT,source_name TEXT,source_kind TEXT,
        source_quality REAL,source_verified INTEGER,published_utc TEXT,
        first_seen_utc TEXT,last_seen_utc TEXT,headline TEXT,summary TEXT,
        category TEXT,scope TEXT,currencies_json TEXT,generic_sentiment_score REAL,
        directional_confidence REAL,severity REAL,movement_potential REAL,
        duplicate_count INTEGER,payload_json TEXT)""")
    connection.execute(
        "INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("article-1", "official", "Official", "rss", 1.0, 1,
         "2026-08-08T12:00:00+00:00", "2026-08-08T12:00:02+00:00",
         "2026-08-08T12:00:02+00:00", "Headline", "Summary", "inflation",
         "USD", '["USD"]', -0.2, 0.5, 0.3, 0.6, 4,
         json.dumps(proof_payload({"topic_signature": "inflation|USD"}))),
    )
    connection.commit(); connection.close()
    state = tmp_path / "state.json"
    result = run(
        config_path=config, coverage_path=coverage, news_database=news,
        cftc_path=tmp_path / "missing.json", database_path=tmp_path / "registry.sqlite",
        treasury_path=tmp_path / "missing_treasury.sqlite",
        state_path=state, report_path=tmp_path / "report.md",
    )
    assert result["configured_source_count"] == 1
    assert result["counts"]["events"] == 1
    assert result["counts"]["story_clusters"] == 1
    assert result["counts"]["story_cluster_assignments"] == 1
    assert result["story_cluster_contract_id"] == STORY_CLUSTER_CONTRACT_ID
    assert result["article_scan_mode"] == "full"
    assert result["source_cards"][0]["causal_timestamp_quality"] == (
        "first_seen_prospective_clock_bound"
    )
    assert json.loads(state.read_text(encoding="utf-8"))["can_place_orders"] is False

    repeated = run(
        config_path=config, coverage_path=coverage, news_database=news,
        cftc_path=tmp_path / "missing.json", database_path=tmp_path / "registry.sqlite",
        treasury_path=tmp_path / "missing_treasury.sqlite",
        state_path=state, report_path=tmp_path / "report.md",
    )
    assert repeated["article_scan_mode"] == "incremental"
    assert repeated["identity_backfill"]["total"] == 0
    assert repeated["inserted_article_versions"] == 0


def test_treasury_bootstrap_is_registered_but_remains_nonproof(tmp_path: Path) -> None:
    connection = connect_registry(tmp_path / "registry.sqlite")
    contract = {
        "source_id": "us_treasury_daily_yield_curve",
        "information_population": "official_rates_curve",
        "provider": "Treasury",
        "license_class": "public",
        "raw_payload_retention": "retain",
        "source_contract_id": "contract-rates",
        "source_cohort_id": "cohort-rates",
    }
    connection.execute(
        "INSERT INTO source_contracts VALUES (?,?,?,?,?,?)",
        ("contract-rates", "us_treasury_daily_yield_curve", "cohort-rates", "now", "sha", "{}"),
    )
    row = {
        "observation_id": "one", "cohort_id": "rates-cohort",
        "observed_utc": "2026-08-08T23:10:00+00:00",
        "yield_date": "2026-08-07", "feed_updated_utc": "2026-08-08T12:00:00Z",
        "two_year_pct": 3.5, "ten_year_pct": 4.2,
        "curve_2s10s_bps": 70.0, "observation_version": 1,
        "observation_kind": "bootstrap_current_view",
        "bootstrap_current_view": 1, "prospective_eligible": 0,
        "direction_policy": "abstain", "raw_archive_sha256": "raw",
        "source_contract_json": "{}",
    }
    assert insert_treasury_events(connection, [row], contract) == 1
    stored = connection.execute(
        "SELECT quality_state,source_cohort_id,payload_json FROM source_events"
    ).fetchone()
    assert stored[0] == "bootstrap_or_revision_nonproof"
    assert stored[1] == "cohort-rates"
    payload = json.loads(stored[2])
    assert payload["direction_policy"] == "abstain"
    assert payload["upstream_source_cohort_id"] == "rates-cohort"
    assert payload["canonical_source_binding"] == "canonical_contract_cohort_v2"
    connection.close()


def test_treasury_observations_uses_only_latest_cohort_and_latest_date_version(
    tmp_path: Path,
) -> None:
    database = tmp_path / "treasury.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """CREATE TABLE rate_observations (
            observation_id TEXT,cohort_id TEXT,observed_utc TEXT,yield_date TEXT,
            feed_updated_utc TEXT,two_year_pct REAL,ten_year_pct REAL,
            curve_2s10s_bps REAL,row_sha256 TEXT,raw_archive_sha256 TEXT,
            observation_version INTEGER,supersedes_observation_id TEXT,
            observation_kind TEXT,bootstrap_current_view INTEGER,
            prospective_eligible INTEGER,direction_policy TEXT,
            source_contract_json TEXT
        )"""
    )
    rows = [
        ("old", "cohort-old", "2026-08-08T12:00:00Z", "2026-08-07", 1),
        ("new-v1", "cohort-new", "2026-08-09T12:00:00Z", "2026-08-07", 1),
        ("new-v2", "cohort-new", "2026-08-10T12:00:00Z", "2026-08-07", 2),
        ("new-day", "cohort-new", "2026-08-10T12:00:00Z", "2026-08-10", 1),
    ]
    for observation_id, cohort_id, observed, yield_date, version in rows:
        connection.execute(
            "INSERT INTO rate_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                observation_id, cohort_id, observed, yield_date, observed,
                4.0 + version, 4.5, 50.0, observation_id, "archive", version,
                None, "bootstrap_current_view", 0, 0, "abstain", "{}",
            ),
        )
    connection.commit()
    connection.close()

    selected = treasury_observations(database)

    assert [row["observation_id"] for row in selected] == ["new-v2", "new-day"]
    assert {row["cohort_id"] for row in selected} == {"cohort-new"}


def test_runtime_adapter_and_credential_are_material_source_contract_fields():
    source = {
        "source_id": "fred_alfred_vintages", "name": "ALFRED",
        "kind": "vintage_backfill", "currencies": ["USD"],
        "runtime_supported": True,
        "runtime_adapter": "oanda_alfred_vintage_prospective.py",
        "credential_env": "FRED_API_KEY",
    }
    contract = contract_for(source)
    card = source_card(source, None, contract, observed_utc="2026-08-08T12:00:00Z")
    assert contract["runtime_supported"] is True
    assert contract["credential_environment"] == "FRED_API_KEY"
    assert card["runtime_adapter"] == "oanda_alfred_vintage_prospective.py"
    assert card["runtime_supported"] is True


def test_topic_signature_is_not_a_story_identity() -> None:
    row_a = {"headline": "Central bank holds rates", "summary": ""}
    row_b = {"headline": "Unrelated bank earnings", "summary": ""}
    payload = {"topic_signature": "market_news|global|general|neutral"}
    cluster_a, method_a = canonical_story_cluster(row_a, payload)
    cluster_b, method_b = canonical_story_cluster(row_b, payload)
    assert cluster_a != cluster_b
    assert method_a == method_b == "normalized_headline_summary"


def test_source_lineage_clusters_syndicated_updates() -> None:
    cluster, method = canonical_story_cluster(
        {"headline": "Changed headline", "summary": ""},
        {"event_lineage_id": "wire-story-123", "topic_signature": "policy|USD"},
    )
    assert cluster == "wire-story-123"
    assert method == "source_event_lineage"


def test_cftc_poll_time_does_not_manufacture_a_revision(tmp_path: Path) -> None:
    connection = connect_registry(tmp_path / "registry.sqlite")
    contract = {
        "source_id": "cftc_cot_positioning",
        "information_population": "institutional_futures_positioning",
        "provider": "CFTC",
        "license_class": "public",
        "raw_payload_retention": "retain",
        "source_contract_id": "contract-cftc",
        "source_cohort_id": "cohort-cftc",
    }
    connection.execute(
        "INSERT INTO source_contracts VALUES (?,?,?,?,?,?)",
        ("contract-cftc", "cftc_cot_positioning", "cohort-cftc", "now", "sha", "{}"),
    )
    value = {
        "currency": "EUR",
        "report_date": "2026-08-04T00:00:00.000",
        "first_seen_utc": "2026-08-07T19:31:00+00:00",
        "last_observed_utc": "2026-08-07T19:31:00+00:00",
        "leveraged_money_net_pct_oi": -0.1,
    }
    assert insert_cftc_events(
        connection,
        {"generated_utc": value["last_observed_utc"], "currencies": {"EUR": value}},
        contract,
    ) == 1
    later = dict(value, last_observed_utc="2026-08-08T01:31:00+00:00")
    assert insert_cftc_events(
        connection,
        {"generated_utc": later["last_observed_utc"], "currencies": {"EUR": later}},
        contract,
    ) == 0
    assert connection.execute(
        "SELECT COUNT(*) FROM source_events"
    ).fetchone()[0] == 1
    assert connection.execute(
        "SELECT COUNT(*) FROM source_supersession_events"
    ).fetchone()[0] == 0
    connection.close()


def test_cftc_position_change_is_a_real_revision(tmp_path: Path) -> None:
    connection = connect_registry(tmp_path / "registry.sqlite")
    contract = {
        "source_id": "cftc_cot_positioning",
        "information_population": "institutional_futures_positioning",
        "provider": "CFTC",
        "license_class": "public",
        "raw_payload_retention": "retain",
        "source_contract_id": "contract-cftc",
        "source_cohort_id": "cohort-cftc",
    }
    connection.execute(
        "INSERT INTO source_contracts VALUES (?,?,?,?,?,?)",
        ("contract-cftc", "cftc_cot_positioning", "cohort-cftc", "now", "sha", "{}"),
    )
    base = {
        "currency": "EUR",
        "report_date": "2026-08-04T00:00:00.000",
        "first_seen_utc": "2026-08-07T19:31:00+00:00",
        "last_observed_utc": "2026-08-07T19:31:00+00:00",
        "leveraged_money_net_pct_oi": -0.1,
    }
    revised = dict(
        base,
        last_observed_utc="2026-08-08T01:31:00+00:00",
        leveraged_money_net_pct_oi=-0.11,
    )
    assert stable_hash(cftc_substantive_payload(base)) != stable_hash(
        cftc_substantive_payload(revised)
    )
    assert insert_cftc_events(
        connection,
        {"generated_utc": base["last_observed_utc"], "currencies": {"EUR": base}},
        contract,
    ) == 1
    assert insert_cftc_events(
        connection,
        {"generated_utc": revised["last_observed_utc"], "currencies": {"EUR": revised}},
        contract,
    ) == 1
    versions = connection.execute(
        "SELECT event_version FROM source_events ORDER BY event_version"
    ).fetchall()
    assert versions == [(1,), (2,)]
    connection.close()
