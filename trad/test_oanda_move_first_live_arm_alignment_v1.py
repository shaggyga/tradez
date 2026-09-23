import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

import oanda_move_first_live_arm_alignment_v1 as audit


SOURCE_COHORT = "capture_test"
SOURCE_CONTRACT = "source_test"
FACTOR_CONTRACT = "factor_test"


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def write_config(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "move_first_live_arm_alignment_config_v1",
                "audit_id": "test_audit",
                "source_cohort_id": SOURCE_COHORT,
                "source_ledger_schema_version": "move_first_live_case_capture_ledger_v1",
                "source_database_contract_id": SOURCE_CONTRACT,
                "factor_episode_contract_id": FACTOR_CONTRACT,
                "deduplication_rule": "resolve_append_only_factor_root_merges_then_select_earliest_first_recorded_case_per_root",
                "minimum_absolute_pair_score": 0.05,
                "pair_score_formula": "base_currency_score_minus_quote_currency_score",
                "forward_shadow_arms": [
                    "strict_forward_direction",
                    "broad_context_direction",
                    "technical_continuation",
                    "broad_context_plus_continuation",
                ],
                "label_derived_forward_shadow_arms": [
                    "technical_continuation",
                    "broad_context_plus_continuation",
                ],
                "narrative_score_arms": ["research_semantic_v1", "secondary_directional_discovery_v1"],
                "selection_conditioned_on_realized_executable_move": True,
                "predictive_backtest_eligible": False,
                "promotion_eligible": False,
                "research_only": True,
                "execution_eligible": False,
                "can_authorize": False,
                "can_place_orders": False,
            }
        ),
        encoding="utf-8",
    )


def build_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE manifest(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE cases(
            source_case_id TEXT PRIMARY KEY, first_recorded_utc TEXT NOT NULL,
            instrument TEXT NOT NULL, start_utc TEXT NOT NULL, end_utc TEXT NOT NULL,
            case_json TEXT NOT NULL, case_sha256 TEXT NOT NULL, inserted_utc TEXT NOT NULL
        );
        CREATE TABLE factor_memberships(
            source_case_id TEXT PRIMARY KEY, factor_episode_contract_id TEXT NOT NULL,
            factor_episode_id TEXT NOT NULL, factor_primary_token TEXT NOT NULL,
            registered_utc TEXT NOT NULL, membership_json TEXT NOT NULL,
            membership_sha256 TEXT NOT NULL, inserted_utc TEXT NOT NULL
        );
        CREATE TABLE factor_root_merges(
            merge_id TEXT PRIMARY KEY, factor_episode_contract_id TEXT NOT NULL,
            from_root_id TEXT NOT NULL, into_root_id TEXT NOT NULL,
            factor_primary_token TEXT NOT NULL, bridge_case_ids TEXT NOT NULL,
            detected_utc TEXT NOT NULL, merge_json TEXT NOT NULL,
            merge_sha256 TEXT NOT NULL, inserted_utc TEXT NOT NULL
        );
        """
    )
    manifest = {
        "schema_version": "move_first_live_case_capture_ledger_v1",
        "cohort_id": SOURCE_COHORT,
        "source_database_contract_id": SOURCE_CONTRACT,
        "factor_episode_contract_id": FACTOR_CONTRACT,
        "historical_rows_imported": "0",
        "execution_eligible": "false",
    }
    connection.executemany("INSERT INTO manifest VALUES(?,?)", sorted(manifest.items()))
    connection.commit()
    return connection


def add_case(
    connection: sqlite3.Connection,
    case_id: str,
    recorded: str,
    episode: str,
    instrument: str,
    move: str,
    base_score: float | None,
    quote_score: float | None,
    *,
    broad: int,
    technical: int,
    strict: int = 0,
    net_room: float = 2.0,
    eligible: bool = True,
    start_utc: str | None = None,
    recent_context_stories: list[dict] | None = None,
    strict_forward_exclusions: dict[str, int] | None = None,
    strict_forward_independent_story_count: int = 0,
    factor_supported: bool = True,
    factor_ambiguous: bool = False,
) -> None:
    base, quote = instrument.split("_")
    base_factor_token = f"{base}{'+' if move == 'up' else '-'}"
    quote_factor_token = f"{quote}{'-' if move == 'up' else '+'}"
    factor_scores = (
        {base_factor_token: 1.0, quote_factor_token: 0.2}
        if factor_supported
        else {base_factor_token: -0.1, quote_factor_token: -0.3}
    )
    base_scores = {
        "research_semantic_v1": base_score,
        "secondary_directional_discovery_v1": base_score,
    }
    quote_scores = {
        "research_semantic_v1": quote_score,
        "secondary_directional_discovery_v1": quote_score,
    }
    payload = {
        "case_id": case_id,
        "instrument": instrument,
        "start_utc": start_utc or recorded,
        "move_direction": move,
        "executable_net_pips": net_room,
        "execution_eligible": False,
        "forward_shadow_eligible": eligible,
        "factor_primary_method": "causal_all68_currency_strength_fixed_onset_5m",
        "factor_primary_scores_bps": factor_scores,
        "factor_primary_ambiguous": factor_ambiguous,
        "forward_shadow_arms": {
            "strict_forward_direction": strict,
            "broad_context_direction": broad,
            "technical_continuation": technical,
        },
        "recent_context_stories": recent_context_stories or [],
        "strict_forward_exclusions": strict_forward_exclusions or {},
        "strict_forward_independent_story_count": (
            strict_forward_independent_story_count
        ),
        "continuous_narrative_state": {
            "state_available": True,
            "base": base,
            "quote": quote,
            "base_currency_state": {"model_scores": base_scores},
            "quote_currency_state": {"model_scores": quote_scores},
        },
    }
    case_json = canonical(payload)
    connection.execute(
        "INSERT INTO cases VALUES(?,?,?,?,?,?,?,?)",
        (case_id, recorded, instrument, recorded, recorded, case_json, digest(case_json), recorded),
    )
    membership = canonical(
        {
            "case_id": case_id,
            "factor_episode_contract_id": FACTOR_CONTRACT,
            "factor_episode_id": episode,
            "factor_primary_token": base_factor_token,
            "registered_utc": recorded,
        }
    )
    connection.execute(
        "INSERT INTO factor_memberships VALUES(?,?,?,?,?,?,?,?)",
        (
            case_id,
            FACTOR_CONTRACT,
            episode,
            base_factor_token,
            recorded,
            membership,
            digest(membership),
            recorded,
        ),
    )
    connection.commit()


def add_merge(connection: sqlite3.Connection, source: str, target: str) -> None:
    payload = canonical(
        {
            "merge_id": "merge",
            "factor_episode_contract_id": FACTOR_CONTRACT,
            "from_root_id": source,
            "into_root_id": target,
            "factor_primary_token": "USD",
            "bridge_case_ids": "[]",
            "detected_utc": "2026-09-01T00:03:00Z",
        }
    )
    connection.execute(
        "INSERT INTO factor_root_merges VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            "merge", FACTOR_CONTRACT, source, target, "USD", "[]",
            "2026-09-01T00:03:00Z", payload, digest(payload), "2026-09-01T00:03:00Z",
        ),
    )
    connection.commit()


def paths(tmp_path: Path):
    config = tmp_path / "config.json"
    database = tmp_path / "capture.sqlite"
    report = tmp_path / "report.json"
    markdown = tmp_path / "report.md"
    write_config(config)
    return config, database, report, markdown


def test_pair_score_is_base_minus_quote_and_episodes_are_deduplicated(tmp_path: Path):
    config, database, _, _ = paths(tmp_path)
    connection = build_database(database)
    add_case(connection, "one", "2026-09-01T00:01:00Z", "ep1", "EUR_USD", "up", 0.2, -0.1, broad=1, technical=1)
    add_case(connection, "duplicate", "2026-09-01T00:02:00Z", "ep1", "EUR_GBP", "down", -0.3, 0.1, broad=-1, technical=-1)
    add_case(connection, "two", "2026-09-01T00:03:00Z", "ep2", "GBP_JPY", "down", -0.2, 0.2, broad=-1, technical=1)
    connection.close()

    result = audit.build_audit(config, database)
    metric = result["arm_metrics"]["currency_score::research_semantic_v1"]

    assert result["source_case_count"] == 3
    assert result["resolved_factor_episode_count"] == 2
    assert metric["signaled_episode_count"] == 2
    assert metric["correct_direction_count"] == 2
    assert metric["directional_accuracy_pct"] == 100.0
    assert result["episode_rows"][0]["arms"]["currency_score::research_semantic_v1"]["raw_value"] == pytest.approx(0.3)


def test_identical_named_arms_are_reported_as_one_decision_vector(tmp_path: Path):
    config, database, _, _ = paths(tmp_path)
    connection = build_database(database)
    add_case(connection, "one", "2026-09-01T00:01:00Z", "ep1", "EUR_USD", "up", 0.2, -0.1, broad=1, technical=1)
    connection.close()

    result = audit.build_audit(config, database)
    groups = [set(group["arms"]) for group in result["decision_equivalence_groups"]]

    expected = {
        "currency_score::research_semantic_v1",
        "currency_score::secondary_directional_discovery_v1",
    }
    assert any(expected <= group for group in groups)
    assert result["unique_decision_vector_count"] < result["arm_count"]


def test_move_conditioning_can_never_become_predictive_or_executable(tmp_path: Path):
    config, database, report, markdown = paths(tmp_path)
    connection = build_database(database)
    add_case(connection, "one", "2026-09-01T00:01:00Z", "ep1", "EUR_USD", "up", 0.2, -0.1, broad=1, technical=1)
    connection.close()

    result = audit.run_once(config, database, report, markdown)

    assert result["selection_conditioned_on_realized_executable_move"] is True
    assert result["predictive_backtest_eligible"] is False
    assert result["promotion_eligible"] is False
    assert result["execution_eligible"] is False
    assert result["can_authorize"] is False
    assert result["can_place_orders"] is False
    assert result["execution_decision"] == "no_trade"
    assert audit.verify(config, database, report)["verified"] is True


def test_source_case_mutation_is_rejected(tmp_path: Path):
    config, database, _, _ = paths(tmp_path)
    connection = build_database(database)
    add_case(connection, "one", "2026-09-01T00:01:00Z", "ep1", "EUR_USD", "up", 0.2, -0.1, broad=1, technical=1)
    connection.execute("UPDATE cases SET case_json='{}' WHERE source_case_id='one'")
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="case hash mismatch"):
        audit.build_audit(config, database)


def test_root_merge_reduces_effective_episode_count(tmp_path: Path):
    config, database, _, _ = paths(tmp_path)
    connection = build_database(database)
    add_case(connection, "one", "2026-09-01T00:01:00Z", "ep1", "EUR_USD", "up", 0.2, -0.1, broad=1, technical=1)
    add_case(connection, "two", "2026-09-01T00:02:00Z", "ep2", "GBP_USD", "down", -0.2, 0.1, broad=-1, technical=-1)
    add_merge(connection, "ep2", "ep1")
    connection.close()

    result = audit.build_audit(config, database)

    assert result["source_case_count"] == 2
    assert result["factor_root_merge_count"] == 1
    assert result["resolved_factor_episode_count"] == 1
    assert result["episode_rows"][0]["source_case_id"] == "one"


def test_unavailable_or_subthreshold_score_abstains(tmp_path: Path):
    config, database, _, _ = paths(tmp_path)
    connection = build_database(database)
    add_case(connection, "one", "2026-09-01T00:01:00Z", "ep1", "EUR_USD", "up", 0.02, 0.0, broad=0, technical=0)
    connection.close()

    result = audit.build_audit(config, database)
    metric = result["arm_metrics"]["currency_score::research_semantic_v1"]

    assert metric["signaled_episode_count"] == 0
    assert metric["abstained_episode_count"] == 1
    assert metric["directional_accuracy_pct"] is None


def test_factor_support_fails_closed_when_both_aligned_scores_are_negative():
    payload = {
        "factor_primary_method": "causal_all68_currency_strength_fixed_onset_5m",
        "factor_primary_scores_bps": {"USD+": -0.6, "HUF-": -1.4},
        "factor_primary_ambiguous": False,
    }

    result = audit._factor_support_diagnostic(payload, "USD+")

    assert result["status"] == "no_move_aligned_positive_factor"
    assert result["support_qualified"] is False
    assert result["best_scored_token"] == "USD+"
    assert result["best_aligned_score_bps"] == pytest.approx(-0.6)
    assert result["membership_rewritten"] is False


def test_factor_support_requires_positive_best_primary_and_clear_margin():
    supported = audit._factor_support_diagnostic(
        {
            "factor_primary_method": "causal_all68_currency_strength_fixed_onset_5m",
            "factor_primary_scores_bps": {"EUR+": 1.2, "HUF-": 4.5},
            "factor_primary_ambiguous": False,
        },
        "HUF-",
    )
    ambiguous = audit._factor_support_diagnostic(
        {
            "factor_primary_method": "causal_all68_currency_strength_fixed_onset_5m",
            "factor_primary_scores_bps": {"EUR+": 1.2, "HUF-": 1.4},
            "factor_primary_ambiguous": True,
        },
        "HUF-",
    )

    assert supported["status"] == "supported"
    assert supported["support_qualified"] is True
    assert ambiguous["status"] == "ambiguous_primary_margin"
    assert ambiguous["support_qualified"] is False


def test_support_qualified_metrics_exclude_unsupported_factor_episode(
    tmp_path: Path,
):
    config, database, _, _ = paths(tmp_path)
    connection = build_database(database)
    add_case(
        connection,
        "supported",
        "2026-09-01T00:01:00Z",
        "ep1",
        "EUR_USD",
        "up",
        0.2,
        -0.1,
        broad=1,
        technical=1,
        factor_supported=True,
    )
    add_case(
        connection,
        "unsupported",
        "2026-09-01T00:02:00Z",
        "ep2",
        "GBP_JPY",
        "down",
        -0.2,
        0.1,
        broad=-1,
        technical=-1,
        factor_supported=False,
    )
    connection.close()

    result = audit.build_audit(config, database)
    legacy = result["arm_metrics"]["forward_shadow::broad_context_direction"]
    supported = result["support_qualified_arm_metrics"][
        "forward_shadow::broad_context_direction"
    ]

    assert result["resolved_factor_episode_count"] == 2
    assert result["support_qualified_factor_episode_count"] == 1
    assert result["factor_support_audit"][
        "support_qualified_selected_episode_count"
    ] == 1
    assert legacy["effective_episode_count"] == 2
    assert legacy["correct_direction_count"] == 2
    assert supported["effective_episode_count"] == 1
    assert supported["correct_direction_count"] == 1
    assert supported["wrong_direction_count"] == 0
    assert supported["directional_accuracy_pct"] == 100.0
    assert supported["interpretation"] == (
        "factor_support_qualified_move_conditioned_alignment_"
        "diagnostic_not_trade_pnl"
    )
    assert result["promotion_eligible"] is False
    assert result["execution_eligible"] is False


def test_causal_gap_preserves_aligned_forward_story_and_abstention_reason(
    tmp_path: Path,
):
    config, database, _, _ = paths(tmp_path)
    connection = build_database(database)
    add_case(
        connection,
        "one",
        "2026-09-01T00:01:00Z",
        "ep1",
        "USD_ZAR",
        "up",
        None,
        None,
        broad=0,
        technical=0,
        strict=0,
        recent_context_stories=[
            {
                "source_event_id": "event-before",
                "source_id": "research-aggregator",
                "headline": "Forward-timely systemic risk report",
                "effective_from_utc": "2026-09-01T00:00:30Z",
                "forward_signal_timely": True,
                "direction_disposition": "directional",
                "pair_score": 0.9,
                "directional_publish_eligible": False,
            },
            {
                "source_event_id": "event-after",
                "source_id": "later-source",
                "headline": "Not available at the move start",
                "effective_from_utc": "2026-09-01T00:01:30Z",
                "forward_signal_timely": True,
                "direction_disposition": "directional",
                "pair_score": -1.0,
                "directional_publish_eligible": True,
            },
        ],
        strict_forward_exclusions={"not_directional_publish_eligible": 3},
        strict_forward_independent_story_count=0,
    )
    connection.close()

    result = audit.build_audit(config, database)
    gap = result["episode_rows"][0]["causal_gap"]
    taxonomy = result["causal_gap_taxonomy"]

    assert result["causal_gap_taxonomy_contract_id"] == (
        audit.CAUSAL_GAP_TAXONOMY_CONTRACT_ID
    )
    assert gap["state"] == "strict_abstain_research_story_aligned"
    assert gap["retained_forward_research_story_count"] == 1
    assert gap["retained_forward_opposed_story_count"] == 0
    assert gap["nearest_retained_forward_research_story"]["source_event_id"] == (
        "event-before"
    )
    assert gap["strict_abstention_reasons"] == {
        "not_directional_publish_eligible": 3
    }
    assert taxonomy["state_counts"] == {
        "strict_abstain_research_story_aligned": 1
    }
    assert taxonomy["strict_abstention_reason_episode_counts"] == {
        "not_directional_publish_eligible": 1
    }
    assert result["execution_decision"] == "no_trade"


def test_causal_gap_reports_conflicting_forward_research_stories(tmp_path: Path):
    config, database, _, _ = paths(tmp_path)
    connection = build_database(database)
    add_case(
        connection,
        "one",
        "2026-09-01T00:02:00Z",
        "ep1",
        "EUR_USD",
        "up",
        None,
        None,
        broad=0,
        technical=0,
        recent_context_stories=[
            {
                "source_event_id": "aligned",
                "source_id": "source-a",
                "headline": "Aligned story",
                "effective_from_utc": "2026-09-01T00:00:00Z",
                "forward_signal_timely": True,
                "direction_disposition": "directional",
                "pair_score": 0.4,
                "directional_publish_eligible": False,
            },
            {
                "source_event_id": "opposed",
                "source_id": "source-b",
                "headline": "Opposed story",
                "effective_from_utc": "2026-09-01T00:01:00Z",
                "forward_signal_timely": True,
                "direction_disposition": "directional",
                "pair_score": -0.3,
                "directional_publish_eligible": False,
            },
        ],
    )
    connection.close()

    result = audit.build_audit(config, database)
    gap = result["episode_rows"][0]["causal_gap"]

    assert gap["state"] == "strict_abstain_research_story_conflict"
    assert gap["retained_forward_aligned_story_count"] == 1
    assert gap["retained_forward_opposed_story_count"] == 1
    assert gap["nearest_retained_forward_research_story"]["source_event_id"] == (
        "opposed"
    )
    assert result["causal_gap_taxonomy"]["retained_forward_research_story_count"] == 2
    assert result["execution_decision"] == "no_trade"


def test_story_concentration_deduplicates_one_event_across_factor_episodes(
    tmp_path: Path,
):
    config, database, _, _ = paths(tmp_path)
    connection = build_database(database)
    common_story = {
        "source_event_id": "shared-systemic-event",
        "story_cluster_id": "shared-systemic-cluster",
        "source_id": "one-aggregator",
        "headline": "One story mapped across several currencies",
        "effective_from_utc": "2026-09-01T00:00:30Z",
        "forward_signal_timely": True,
        "direction_disposition": "directional",
        "pair_score": 0.7,
        "directional_publish_eligible": False,
    }
    duplicate_version = {
        **common_story,
        "effective_from_utc": "2026-09-01T00:00:45Z",
        "pair_score": 0.8,
    }
    cluster_copy = {
        **common_story,
        "source_event_id": "shared-systemic-event-copy",
        "source_id": "another-aggregator",
    }
    add_case(
        connection,
        "one",
        "2026-09-01T00:01:00Z",
        "ep1",
        "EUR_USD",
        "up",
        None,
        None,
        broad=0,
        technical=0,
        recent_context_stories=[common_story, duplicate_version],
    )
    add_case(
        connection,
        "two",
        "2026-09-01T00:02:00Z",
        "ep2",
        "GBP_USD",
        "up",
        None,
        None,
        broad=0,
        technical=0,
        recent_context_stories=[cluster_copy],
    )
    connection.close()

    result = audit.build_audit(config, database)
    taxonomy = result["causal_gap_taxonomy"]

    assert result["episode_rows"][0]["causal_gap"][
        "retained_forward_research_story_count"
    ] == 1
    assert taxonomy["retained_forward_research_story_count"] == 2
    assert taxonomy["factor_episodes_with_retained_forward_story_count"] == 2
    assert taxonomy["unique_retained_forward_story_count"] == 2
    assert taxonomy["unique_retained_forward_source_count"] == 2
    assert taxonomy["unique_retained_forward_story_cluster_count"] == 1
    assert taxonomy["dominant_retained_story_factor_episode_count"] == 1
    assert taxonomy["dominant_retained_story_factor_episode_pct"] == 50.0
    assert len(taxonomy["retained_story_concentration_top20"]) == 2
    assert {
        item["story_key"]
        for item in taxonomy["retained_story_concentration_top20"]
    } == {
        "source_event:shared-systemic-event",
        "source_event:shared-systemic-event-copy",
    }
    assert taxonomy["dominant_retained_story_cluster_factor_episode_count"] == 2
    assert taxonomy["dominant_retained_story_cluster_factor_episode_pct"] == 100.0
    assert taxonomy["retained_story_cluster_concentration_top20"] == [
        {
            "story_cluster_key": "story_cluster:shared-systemic-cluster",
            "story_cluster_key_type": "story_cluster_id",
            "story_cluster_id": "shared-systemic-cluster",
            "representative_source_event_id": "shared-systemic-event-copy",
            "representative_source_id": "another-aggregator",
            "representative_headline": "One story mapped across several currencies",
            "first_effective_from_utc": "2026-09-01T00:00:30Z",
            "factor_episode_count": 2,
            "source_event_count": 2,
            "aligned_factor_episode_link_count": 2,
            "opposed_factor_episode_link_count": 0,
        }
    ]
    assert result["execution_decision"] == "no_trade"


def test_narrative_family_conservatively_groups_syndicated_headline_variants(
    tmp_path: Path,
):
    config, database, _, _ = paths(tmp_path)
    connection = build_database(database)
    headlines = [
        "Tanker hit by unknown projectiles in Strait of Hormuz - Outlet A",
        "Oil tanker hit by three unknown projectiles near Strait of Hormuz - Outlet B",
        "Tanker struck in Hormuz as escalation worries rise - Outlet C",
        "Tanker struck by three projectiles in Hormuz as Iran warns of attacks - Outlet D",
    ]
    for index, headline in enumerate(headlines):
        story = {
            "source_event_id": f"tanker-event-{index}",
            "story_cluster_id": f"upstream-cluster-{index}",
            "source_id": f"aggregator-{index}",
            "event_type": "risk_off_geopolitical_or_financial",
            "headline": headline,
            "effective_from_utc": f"2026-09-01T00:{index:02d}:00Z",
            "forward_signal_timely": True,
            "direction_disposition": "directional",
            "pair_score": 0.7,
            "directional_publish_eligible": False,
        }
        add_case(
            connection,
            f"case-{index}",
            f"2026-09-01T00:{index + 10:02d}:00Z",
            f"ep-{index}",
            "USD_ZAR",
            "up",
            None,
            None,
            broad=0,
            technical=0,
            recent_context_stories=[story],
        )
    connection.close()

    result = audit.build_audit(config, database)
    taxonomy = result["causal_gap_taxonomy"]

    assert taxonomy["unique_retained_forward_story_count"] == 4
    assert taxonomy["unique_retained_forward_story_cluster_count"] == 4
    assert taxonomy["unique_retained_forward_narrative_family_count"] == 1
    assert taxonomy["dominant_retained_narrative_family_factor_episode_count"] == 4
    assert taxonomy["dominant_retained_narrative_family_factor_episode_pct"] == 100.0
    family = taxonomy["retained_narrative_family_concentration_top20"][0]
    assert family["source_event_count"] == 4
    assert family["upstream_story_cluster_count"] == 4
    assert family["narrative_family_method"] == (
        "time_bounded_headline_similarity_component"
    )
    assert result["execution_decision"] == "no_trade"


def test_narrative_family_does_not_merge_different_event_type_or_distant_story(
    tmp_path: Path,
):
    config, database, _, _ = paths(tmp_path)
    connection = build_database(database)
    base_story = {
        "source_event_id": "event-a",
        "story_cluster_id": "cluster-a",
        "source_id": "source-a",
        "event_type": "risk_off_geopolitical_or_financial",
        "headline": "Tanker hit by unknown projectiles in Strait of Hormuz - A",
        "effective_from_utc": "2026-09-01T00:00:00Z",
        "forward_signal_timely": True,
        "direction_disposition": "directional",
        "pair_score": 0.7,
        "directional_publish_eligible": False,
    }
    different_type = {
        **base_story,
        "source_event_id": "event-b",
        "story_cluster_id": "cluster-b",
        "source_id": "source-b",
        "event_type": "commodity_shock",
    }
    distant_story = {
        **base_story,
        "source_event_id": "event-c",
        "story_cluster_id": "cluster-c",
        "source_id": "source-c",
        "effective_from_utc": "2026-09-01T07:00:01Z",
    }
    for index, story in enumerate((base_story, different_type, distant_story)):
        add_case(
            connection,
            f"case-{index}",
            "2026-09-01T08:00:00Z",
            f"ep-{index}",
            "USD_ZAR",
            "up",
            None,
            None,
            broad=0,
            technical=0,
            recent_context_stories=[story],
        )
    connection.close()

    result = audit.build_audit(config, database)

    assert result["causal_gap_taxonomy"][
        "unique_retained_forward_narrative_family_count"
    ] == 3
