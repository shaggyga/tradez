import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

import oanda_move_first_operational_mapping_alignment_v1 as alignment


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def write_config(path: Path, **overrides) -> None:
    payload = {
        "schema_version": "move_first_operational_mapping_alignment_config_v1",
        "cohort_id": "test_operational_mapping_cohort",
        "cohort_start_utc": "2026-09-01T14:00:00Z",
        "source_case_cohort_id": "test_source_case_cohort",
        "source_case_ledger_schema_version": "move_first_live_case_capture_ledger_v1",
        "source_case_database_contract_id": "test_source_case_contract",
        "factor_episode_contract_id": "test_factor_contract",
        "news_mapping_contract_id": alignment.NEWS_MAPPING_CONTRACT_ID,
        "official_mapping_contract_id": (
            alignment.OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID
        ),
        "operational_clock_rule": "max_source_effective_detail_available_and_mapping_receipt_available_utc",
        "lookback_minutes": 120,
        "deduplication_rule": "resolve_append_only_factor_root_merges_then_select_earliest_sealed_case_per_root",
        "selection_conditioned_on_realized_executable_move": True,
        "predictive_backtest_eligible": False,
        "promotion_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_place_orders": False,
    }
    payload.update(overrides)
    path.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )


def build_capture(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE manifest(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE cases(
            source_case_id TEXT PRIMARY KEY,first_recorded_utc TEXT NOT NULL,
            instrument TEXT NOT NULL,start_utc TEXT NOT NULL,end_utc TEXT NOT NULL,
            case_json TEXT NOT NULL,case_sha256 TEXT NOT NULL,inserted_utc TEXT NOT NULL
        );
        CREATE TABLE factor_memberships(
            source_case_id TEXT PRIMARY KEY,factor_episode_contract_id TEXT NOT NULL,
            factor_episode_id TEXT NOT NULL,factor_primary_token TEXT NOT NULL,
            registered_utc TEXT NOT NULL,membership_json TEXT NOT NULL,
            membership_sha256 TEXT NOT NULL,inserted_utc TEXT NOT NULL
        );
        CREATE TABLE factor_root_merges(
            merge_id TEXT PRIMARY KEY,factor_episode_contract_id TEXT NOT NULL,
            from_root_id TEXT NOT NULL,into_root_id TEXT NOT NULL,
            factor_primary_token TEXT NOT NULL,bridge_case_ids TEXT NOT NULL,
            detected_utc TEXT NOT NULL,merge_json TEXT NOT NULL,
            merge_sha256 TEXT NOT NULL,inserted_utc TEXT NOT NULL
        );
        """
    )
    manifest = {
        "schema_version": "move_first_live_case_capture_ledger_v1",
        "cohort_id": "test_source_case_cohort",
        "source_database_contract_id": "test_source_case_contract",
        "factor_episode_contract_id": "test_factor_contract",
        "historical_rows_imported": "0",
        "execution_eligible": "false",
    }
    connection.executemany("INSERT INTO manifest VALUES(?,?)", sorted(manifest.items()))
    connection.commit()
    return connection


def add_case(
    connection: sqlite3.Connection,
    *,
    case_id: str = "case_one",
    start: str = "2026-09-01T14:10:00Z",
    stories: list[dict] | None = None,
) -> None:
    payload = {
        "case_id": case_id,
        "instrument": "EUR_USD",
        "start_utc": start,
        "end_utc": "2026-09-01T14:15:00Z",
        "move_direction": "up",
        "move_bps": 8.0,
        "executable_net_pips": 5.0,
        "pre_move_event_count": len(stories or []),
        "recent_context_stories": stories or [],
        "forward_shadow_arms": {
            "strict_forward_direction": 1,
            "broad_context_direction": 1,
            "technical_continuation": 1,
        },
        "execution_eligible": False,
    }
    case_json = canonical(payload)
    recorded = "2026-09-01T14:16:00Z"
    connection.execute(
        "INSERT INTO cases VALUES(?,?,?,?,?,?,?,?)",
        (
            case_id,
            recorded,
            "EUR_USD",
            start,
            "2026-09-01T14:15:00Z",
            case_json,
            digest(case_json),
            recorded,
        ),
    )
    membership = canonical(
        {
            "source_case_id": case_id,
            "factor_episode_contract_id": "test_factor_contract",
            "factor_episode_id": f"episode_{case_id}",
            "factor_primary_token": "EUR+",
        }
    )
    connection.execute(
        "INSERT INTO factor_memberships VALUES(?,?,?,?,?,?,?,?)",
        (
            case_id,
            "test_factor_contract",
            f"episode_{case_id}",
            "EUR+",
            recorded,
            membership,
            digest(membership),
            recorded,
        ),
    )
    connection.commit()


def build_source(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE source_events(
            source_event_id TEXT PRIMARY KEY,source_id TEXT NOT NULL,
            source_population TEXT NOT NULL,event_type TEXT NOT NULL,
            story_cluster_id TEXT,effective_from_utc TEXT NOT NULL,
            valid_until_utc TEXT,superseded_at_utc TEXT,
            base_currency TEXT,quote_currency TEXT,payload_json TEXT NOT NULL,
            published_at_utc TEXT,first_seen_at_utc TEXT,retrieved_at_utc TEXT,
            revised_at_utc TEXT,event_version INTEGER,
            supersedes_source_event_id TEXT
        );
        CREATE TABLE source_event_quarantines(source_event_id TEXT PRIMARY KEY);
        CREATE VIEW source_events_causal_v1 AS
            SELECT event.* FROM source_events AS event
            WHERE NOT EXISTS(
                SELECT 1 FROM source_event_quarantines AS quarantine
                WHERE quarantine.source_event_id=event.source_event_id
            );
        CREATE TABLE news_fast_lane_import_receipts(
            receipt_id TEXT PRIMARY KEY,source_event_id TEXT NOT NULL,
            governance_available_utc TEXT NOT NULL,adapter_contract_id TEXT NOT NULL,
            research_only INTEGER NOT NULL,execution_eligible INTEGER NOT NULL,
            can_authorize INTEGER NOT NULL
        );
        CREATE TABLE official_fast_lane_governance_imports(
            receipt_id TEXT PRIMARY KEY,source_event_id TEXT NOT NULL,
            imported_utc TEXT NOT NULL,adapter_contract_id TEXT NOT NULL,
            research_only INTEGER NOT NULL,execution_eligible INTEGER NOT NULL,
            can_authorize INTEGER NOT NULL
        );
        """
    )
    connection.commit()
    return connection


def add_event(
    connection: sqlite3.Connection,
    event_id: str,
    *,
    headline: str,
    effective: str = "2026-09-01T14:04:00Z",
    mapping_available: str | None = None,
    kind: str = "news",
    contract: str | None = None,
) -> None:
    population = "official_policy" if kind == "official" else "media_aggregator"
    payload = canonical(
        {
            "headline": headline,
            "currency_scores": {"EUR": 1.0, "USD": -1.0},
            "directional_confidence": 0.8,
            "forward_signal_timely": True,
            "directional_publish_eligible": True,
            "official_policy_release": kind == "official",
        }
    )
    connection.execute(
        "INSERT INTO source_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            event_id,
            f"source_{event_id}",
            population,
            "monetary_policy",
            f"cluster_{event_id}",
            effective,
            None,
            None,
            "EUR",
            None,
            payload,
            effective,
            effective,
            effective,
            None,
            1,
            None,
        ),
    )
    if mapping_available is not None:
        if kind == "official":
            connection.execute(
                "INSERT INTO official_fast_lane_governance_imports VALUES(?,?,?,?,?,?,?)",
                (
                    f"receipt_{event_id}",
                    event_id,
                    mapping_available,
                    contract
                    or alignment.OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
                    1,
                    0,
                    0,
                ),
            )
        else:
            connection.execute(
                "INSERT INTO news_fast_lane_import_receipts VALUES(?,?,?,?,?,?,?)",
                (
                    f"receipt_{event_id}",
                    event_id,
                    mapping_available,
                    contract or alignment.NEWS_MAPPING_CONTRACT_ID,
                    1,
                    0,
                    0,
                ),
            )
    connection.commit()


def fixture_paths(tmp_path: Path):
    return {
        "config": tmp_path / "config.json",
        "capture": tmp_path / "capture.sqlite",
        "source": tmp_path / "source.sqlite",
        "ledger": tmp_path / "ledger.sqlite",
        "json": tmp_path / "report.json",
        "md": tmp_path / "report.md",
    }


def run_fixture(paths):
    return alignment.run_once(
        paths["config"],
        paths["capture"],
        paths["source"],
        paths["ledger"],
        paths["json"],
        paths["md"],
    )


def test_only_pre_move_receipt_backed_mapping_is_admitted(tmp_path: Path):
    paths = fixture_paths(tmp_path)
    write_config(paths["config"])
    capture = build_capture(paths["capture"])
    stories = [
        {"source_event_id": "early", "headline": "early"},
        {"source_event_id": "late", "headline": "late"},
        {"source_event_id": "legacy", "headline": "legacy"},
        {"source_event_id": "wrong", "headline": "wrong contract"},
    ]
    add_case(capture, stories=stories)
    capture.close()
    source = build_source(paths["source"])
    add_event(source, "early", headline="early", mapping_available="2026-09-01T14:05:00Z")
    add_event(source, "late", headline="late", mapping_available="2026-09-01T14:11:00Z")
    add_event(source, "legacy", headline="legacy")
    add_event(
        source,
        "wrong",
        headline="wrong contract",
        mapping_available="2026-09-01T14:05:00Z",
        contract="obsolete_contract",
    )
    source.close()

    result = run_fixture(paths)
    row = result["episode_rows"][0]

    assert row["operational_pre_move_event_count"] == 1
    assert row["directions"]["receipt_backed_strict"] == 1
    assert row["legacy_recent_story_mapping_status_counts"] == {
        "admitted": 1,
        "mapping_available_after_move_start": 1,
        "no_current_mapping_receipt": 2,
    }
    assert result["execution_eligible"] is False
    assert result["execution_decision"] == "no_trade"


def test_official_and_news_receipts_dedupe_identical_story(tmp_path: Path):
    paths = fixture_paths(tmp_path)
    write_config(paths["config"])
    capture = build_capture(paths["capture"])
    add_case(capture)
    capture.close()
    source = build_source(paths["source"])
    add_event(source, "news", headline="same policy statement", mapping_available="2026-09-01T14:05:00Z")
    add_event(
        source,
        "official",
        headline="same policy statement",
        mapping_available="2026-09-01T14:06:00Z",
        kind="official",
    )
    source.close()

    result = run_fixture(paths)
    row = result["episode_rows"][0]

    assert row["operational_pre_move_event_count"] == 2
    assert row["operational_independent_story_count"] == 1
    assert row["receipt_backed_strict_vote"]["independent_directional_stories"] == 1
    assert row["operational_mapping_kind_counts"] == {
        "general_news_fast_lane": 1,
        "official_release_fast_lane": 1,
    }


def test_v2_dedupes_cross_publisher_syndication_without_rewriting_raw_receipts(
    tmp_path: Path,
):
    paths = fixture_paths(tmp_path)
    write_config(
        paths["config"],
        contract_id="move_first_operational_mapping_alignment_v2_test",
        cohort_id="test_operational_mapping_v2_cohort",
        story_deduplication_rule=(
            alignment.PUBLISHER_SUFFIX_STORY_DEDUPLICATION_RULE
        ),
    )
    capture = build_capture(paths["capture"])
    add_case(capture)
    capture.close()
    source = build_source(paths["source"])
    add_event(
        source,
        "wire_a",
        headline="Blockade succeeds where sanctions failed as oil exports stall - Reuters",
        mapping_available="2026-09-01T14:05:00Z",
    )
    add_event(
        source,
        "wire_b",
        headline="Blockade succeeds where sanctions failed as oil exports stall - Business Day",
        mapping_available="2026-09-01T14:06:00Z",
    )
    source.close()

    result = run_fixture(paths)
    row = result["episode_rows"][0]

    assert result["contract_id"] == "move_first_operational_mapping_alignment_v2_test"
    assert result["story_deduplication_rule"] == (
        alignment.PUBLISHER_SUFFIX_STORY_DEDUPLICATION_RULE
    )
    assert row["operational_pre_move_event_count"] == 2
    assert len(row["operational_event_receipts"]) == 2
    assert row["operational_independent_story_count"] == 1
    assert row["operational_syndicated_duplicate_count"] == 1
    assert row["receipt_backed_broad_vote"]["independent_directional_stories"] == 1
    assert result["operational_syndicated_duplicate_count"] == 1


def test_v3_collapses_near_duplicate_narratives_and_decays_broad_vote(
    tmp_path: Path,
):
    paths = fixture_paths(tmp_path)
    write_config(
        paths["config"],
        contract_id="move_first_operational_mapping_alignment_v3_test",
        cohort_id="test_operational_mapping_v3_cohort",
        story_deduplication_rule=(
            alignment.NARRATIVE_FAMILY_STORY_DEDUPLICATION_RULE
        ),
        broad_age_decay_half_life_minutes=30,
    )
    capture = build_capture(paths["capture"])
    add_case(capture)
    capture.close()
    source = build_source(paths["source"])
    add_event(
        source,
        "tanker_a",
        headline=(
            "Two oil tankers reportedly struck as Strait of Hormuz "
            "hostilities resume - World Oil"
        ),
        mapping_available="2026-09-01T14:05:00Z",
    )
    add_event(
        source,
        "tanker_b",
        headline=(
            "Oil tanker struck by projectiles in Strait of Hormuz as "
            "hostilities resume - Reuters"
        ),
        mapping_available="2026-09-01T14:06:00Z",
    )
    add_event(
        source,
        "distinct",
        headline="Fed officials debate the inflation path ahead of the meeting",
        mapping_available="2026-09-01T14:07:00Z",
    )
    source.close()

    result = run_fixture(paths)
    row = result["episode_rows"][0]

    assert result["story_deduplication_rule"] == (
        alignment.NARRATIVE_FAMILY_STORY_DEDUPLICATION_RULE
    )
    assert result["broad_age_decay_half_life_minutes"] == 30.0
    assert row["operational_pre_move_event_count"] == 3
    assert len(row["operational_event_receipts"]) == 3
    assert row["operational_independent_story_count"] == 2
    assert row["operational_story_family_duplicate_count"] == 1
    assert row["receipt_backed_broad_vote"]["independent_directional_stories"] == 2
    assert all(
        0.0 < story["age_weight"] < 1.0
        for story in row["receipt_backed_broad_vote"]["stories"]
    )
    assert result["operational_story_family_duplicate_count"] == 1


def test_v3_requires_a_positive_broad_age_decay_half_life(tmp_path: Path):
    config = tmp_path / "config.json"
    write_config(
        config,
        story_deduplication_rule=(
            alignment.NARRATIVE_FAMILY_STORY_DEDUPLICATION_RULE
        ),
    )
    with pytest.raises(ValueError, match="requires broad age decay"):
        alignment.load_config(config)

    write_config(
        config,
        story_deduplication_rule=(
            alignment.NARRATIVE_FAMILY_STORY_DEDUPLICATION_RULE
        ),
        broad_age_decay_half_life_minutes=0,
    )
    with pytest.raises(ValueError, match="half-life must be positive"):
        alignment.load_config(config)


def test_later_receipt_cannot_rewrite_a_sealed_case(tmp_path: Path):
    paths = fixture_paths(tmp_path)
    write_config(paths["config"])
    capture = build_capture(paths["capture"])
    add_case(capture, stories=[{"source_event_id": "delayed", "headline": "delayed"}])
    capture.close()
    source = build_source(paths["source"])
    add_event(source, "delayed", headline="delayed")
    source.close()

    first = run_fixture(paths)
    ledger = sqlite3.connect(paths["ledger"])
    before = ledger.execute(
        "SELECT case_sha256,case_json FROM operational_cases"
    ).fetchone()
    ledger.close()

    source = sqlite3.connect(paths["source"])
    source.execute(
        "INSERT INTO news_fast_lane_import_receipts VALUES(?,?,?,?,?,?,?)",
        (
            "receipt_delayed",
            "delayed",
            "2026-09-01T14:05:00Z",
            alignment.NEWS_MAPPING_CONTRACT_ID,
            1,
            0,
            0,
        ),
    )
    source.commit()
    source.close()
    second = run_fixture(paths)
    ledger = sqlite3.connect(paths["ledger"])
    after = ledger.execute(
        "SELECT case_sha256,case_json FROM operational_cases"
    ).fetchone()
    ledger.close()

    assert first["episode_rows"][0]["operational_pre_move_event_count"] == 0
    assert second["episode_rows"][0]["operational_pre_move_event_count"] == 0
    assert before == after
    assert second["inserted_this_run"] == 0


def test_source_capture_is_read_only_and_report_rebuilds(tmp_path: Path):
    paths = fixture_paths(tmp_path)
    write_config(paths["config"])
    capture = build_capture(paths["capture"])
    add_case(capture)
    capture.close()
    source = build_source(paths["source"])
    add_event(source, "event", headline="policy", mapping_available="2026-09-01T14:05:00Z")
    source.close()
    before = hashlib.sha256(paths["capture"].read_bytes()).hexdigest()

    run_fixture(paths)
    after = hashlib.sha256(paths["capture"].read_bytes()).hexdigest()
    verified = alignment.verify(
        paths["config"], paths["capture"], paths["ledger"], paths["json"]
    )

    assert before == after
    assert verified["verified"] is True


def test_valid_source_case_mutation_after_sealing_is_rejected(tmp_path: Path):
    paths = fixture_paths(tmp_path)
    write_config(paths["config"])
    capture = build_capture(paths["capture"])
    add_case(capture)
    capture.close()
    source = build_source(paths["source"])
    source.close()
    run_fixture(paths)

    capture = sqlite3.connect(paths["capture"])
    row = capture.execute("SELECT case_json FROM cases WHERE source_case_id='case_one'").fetchone()
    payload = json.loads(row[0])
    payload["move_bps"] = 99.0
    case_json = canonical(payload)
    capture.execute(
        "UPDATE cases SET case_json=?,case_sha256=? WHERE source_case_id='case_one'",
        (case_json, digest(case_json)),
    )
    capture.commit()
    capture.close()

    with pytest.raises(RuntimeError, match="sealed source case mutated"):
        run_fixture(paths)


def test_pre_activation_case_is_never_backfilled(tmp_path: Path):
    paths = fixture_paths(tmp_path)
    write_config(paths["config"])
    capture = build_capture(paths["capture"])
    add_case(capture, start="2026-09-01T13:59:59Z")
    capture.close()
    source = build_source(paths["source"])
    source.close()

    result = run_fixture(paths)

    assert result["source_case_count"] == 0
    assert result["historical_rows_imported"] == 0
    assert result["episode_rows"] == []


def test_later_factor_merge_does_not_rewrite_a_published_report(tmp_path: Path):
    paths = fixture_paths(tmp_path)
    write_config(paths["config"])
    capture = build_capture(paths["capture"])
    add_case(capture, case_id="case_one", start="2026-09-01T14:10:00Z")
    add_case(capture, case_id="case_two", start="2026-09-01T14:20:00Z")
    capture.close()
    source = build_source(paths["source"])
    source.close()
    report = run_fixture(paths)
    assert report["resolved_factor_episode_count"] == 2

    capture = sqlite3.connect(paths["capture"])
    merge = canonical(
        {
            "merge_id": "late_merge",
            "from_root_id": "episode_case_two",
            "into_root_id": "episode_case_one",
        }
    )
    capture.execute(
        "INSERT INTO factor_root_merges VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            "late_merge",
            "test_factor_contract",
            "episode_case_two",
            "episode_case_one",
            "EUR+",
            "[]",
            "2026-09-01T15:00:00Z",
            merge,
            digest(merge),
            "2026-09-01T15:00:00Z",
        ),
    )
    capture.commit()
    capture.close()

    verified = alignment.verify(
        paths["config"], paths["capture"], paths["ledger"], paths["json"]
    )
    assert verified["verified"] is True


def test_subsecond_mapping_receipt_after_move_start_is_not_causal() -> None:
    move_start = alignment._parse_epoch("2026-09-02T11:25:00Z")
    receipt_time = alignment._parse_epoch("2026-09-02T11:25:00.757212Z")
    assert move_start is not None
    assert receipt_time is not None
    assert receipt_time - move_start == pytest.approx(0.757212)

    events = [
        {
            "source_event_id": "event-after-onset",
            "effective_epoch": receipt_time,
            "currencies": ["USD"],
            "valid_until_epoch": None,
            "superseded_epoch": None,
        }
    ]
    assert alignment._eligible_events(
        events,
        base="USD",
        quote="JPY",
        start_epoch=move_start,
        lookback_minutes=120,
    ) == []


def test_subsecond_operational_clock_contract_is_accepted(tmp_path: Path) -> None:
    config = tmp_path / "config.json"
    expected = (
        "max_source_effective_detail_available_and_mapping_receipt_"
        "available_utc_subsecond_precision"
    )
    write_config(config, operational_clock_rule=expected)
    assert alignment.load_config(config)["operational_clock_rule"] == expected
