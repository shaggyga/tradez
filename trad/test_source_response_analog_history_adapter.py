import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3

from src.forex_system.ingestion.source_response_analog_history_adapter import (
    HistoryAdapterPaths,
    SourceResponseAnalogHistoryAdapter,
)
from src.forex_system.research.source_response_analog_selector import (
    normalize_point_in_time_event,
)


ROOT = Path(__file__).resolve().parent
CONFIG = json.loads(
    (ROOT / "config" / "source_response_analog_history_adapter_v1.json").read_text(
        encoding="utf-8"
    )
)
SELECTOR = json.loads(
    (ROOT / "config" / "source_response_analog_selector_v1.json").read_text(
        encoding="utf-8"
    )
)


def _macro_database(path: Path) -> None:
    database = sqlite3.connect(path)
    database.executescript(
        """
        CREATE TABLE macro_release_revisions(
          row_id INTEGER PRIMARY KEY, revision_id TEXT, release_key TEXT,
          source_event_id TEXT, recorded_utc TEXT, causal_known_utc TEXT,
          scheduled_utc TEXT, event_series_id TEXT, event_name TEXT,
          event_country TEXT, currencies_json TEXT, importance TEXT, unit TEXT,
          actual_value REAL, consensus_value REAL, previous_value REAL,
          revised_previous_value REAL, standardized_surprise REAL,
          known_before_recorded_timestamp INTEGER, source_id TEXT,
          source_name TEXT, source_url TEXT, source_verified INTEGER,
          source_direct INTEGER, payload_sha256 TEXT, payload_json TEXT
        );
        CREATE TABLE macro_consensus_observations(
          observation_id TEXT PRIMARY KEY, release_key TEXT, event_series_id TEXT,
          scheduled_utc TEXT, captured_utc TEXT, source_timestamp_utc TEXT,
          consensus_value REAL, source_id TEXT, source_verified INTEGER,
          causal_valid INTEGER, payload_sha256 TEXT
        );
        CREATE TABLE macro_reaction_samples(
          sample_id TEXT PRIMARY KEY, release_key TEXT, instrument TEXT,
          target_offset_sec INTEGER, scheduled_utc TEXT, sampled_utc TEXT,
          quote_time_utc TEXT, bid REAL, ask REAL, mid REAL, pip REAL,
          distance_from_target_sec REAL, payload_sha256 TEXT
        );
        """
    )
    database.commit()
    database.close()


def _other_databases(root: Path) -> HistoryAdapterPaths:
    governance = root / "governance.sqlite"
    connection = sqlite3.connect(governance)
    connection.execute("CREATE TABLE source_event_quarantines(source_event_id TEXT)")
    connection.commit()
    connection.close()

    direct = root / "direct.sqlite"
    connection = sqlite3.connect(direct)
    connection.execute(
        """CREATE TABLE response_targets(
             target_id TEXT,status TEXT,outcome_quote_time TEXT,
             source_observation_id TEXT,horizon_sec INTEGER,instrument TEXT
           )"""
    )
    connection.commit()
    connection.close()

    rates = root / "rates.sqlite"
    connection = sqlite3.connect(rates)
    connection.execute(
        """CREATE TABLE daily_rate_observations(
             observation_id TEXT,currency TEXT,prospective_eligible INTEGER
           )"""
    )
    connection.commit()
    connection.close()
    movement = root / "movement.sqlite"
    connection = sqlite3.connect(movement)
    connection.executescript(
        """CREATE TABLE movement_episodes(episode_id TEXT);
           CREATE TABLE episode_source_links(
             episode_id TEXT,source_event_id TEXT,causal_entry_eligible INTEGER
           );"""
    )
    connection.commit()
    connection.close()
    return HistoryAdapterPaths(
        macro_surprise_db=root / "macro.sqlite",
        source_governance_db=governance,
        direct_source_response_db=direct,
        daily_rates_db=rates,
        movement_episode_db=movement,
    )


def _insert_release(
    path: Path,
    *,
    release_key: str = "release_1",
    revision_id: str = "revision_1",
    source_event_id: str = "source_1",
    scheduled: str = "2026-08-16T12:00:00Z",
    causal_known: str = "2026-08-16T12:00:05Z",
    recorded: str = "2026-08-16T12:00:06Z",
    consensus_value=None,
    standardized_surprise=None,
    payload=None,
) -> None:
    database = sqlite3.connect(path)
    database.execute(
        """INSERT INTO macro_release_revisions VALUES(
             NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
           )""",
        (
            revision_id,
            release_key,
            source_event_id,
            recorded,
            causal_known,
            scheduled,
            "official_cpi",
            "Official CPI release",
            "United States",
            '["USD"]',
            "high",
            "percent",
            3.2,
            consensus_value,
            3.0,
            None,
            standardized_surprise,
            0,
            "official_stats",
            "Official Statistics",
            "https://example.invalid/release",
            1,
            1,
            "release_hash",
            json.dumps(payload or {}, sort_keys=True),
        ),
    )
    database.commit()
    database.close()


def _insert_consensus(
    path: Path,
    *,
    release_key: str = "release_1",
    captured: str = "2026-08-16T11:00:00Z",
    source_time: str = "2026-08-16T10:59:00Z",
    causal_valid: int = 1,
) -> None:
    database = sqlite3.connect(path)
    database.execute(
        "INSERT INTO macro_consensus_observations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (
            "consensus_1",
            release_key,
            "official_cpi",
            "2026-08-16T12:00:00Z",
            captured,
            source_time,
            3.1,
            "licensed_calendar",
            1,
            causal_valid,
            "consensus_hash",
        ),
    )
    database.commit()
    database.close()


def _insert_reactions(path: Path, *, release_key: str = "release_1") -> None:
    database = sqlite3.connect(path)
    database.executemany(
        "INSERT INTO macro_reaction_samples VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                "baseline",
                release_key,
                "USD_JPY",
                0,
                "2026-08-16T12:00:00Z",
                "2026-08-16T12:00:02Z",
                "2026-08-16T12:00:01Z",
                100.00,
                100.02,
                100.01,
                0.01,
                2.0,
                "baseline_hash",
            ),
            (
                "outcome",
                release_key,
                "USD_JPY",
                60,
                "2026-08-16T12:00:00Z",
                "2026-08-16T12:01:02Z",
                "2026-08-16T12:01:01Z",
                100.10,
                100.12,
                100.11,
                0.01,
                2.0,
                "outcome_hash",
            ),
        ],
    )
    database.commit()
    database.close()


def _fixture(tmp_path: Path) -> tuple[SourceResponseAnalogHistoryAdapter, HistoryAdapterPaths]:
    _macro_database(tmp_path / "macro.sqlite")
    paths = _other_databases(tmp_path)
    adapter = SourceResponseAnalogHistoryAdapter(
        paths=paths,
        config=CONFIG,
        selector_contract=SELECTOR,
    )
    return adapter, paths


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_maps_only_exact_clocked_reaction_outcomes_and_is_read_only(tmp_path):
    adapter, paths = _fixture(tmp_path)
    _insert_release(paths.macro_surprise_db)
    _insert_consensus(paths.macro_surprise_db)
    _insert_reactions(paths.macro_surprise_db)
    hashes_before = {path: _sha256(path) for path in paths.__dict__.values()}

    snapshot = adapter.build_snapshot(decision_cutoff_utc="2026-08-16T12:02:00Z")

    assert {path: _sha256(path) for path in paths.__dict__.values()} == hashes_before
    assert snapshot["coverage"]["mapped_event_count"] == 1
    assert snapshot["coverage"]["mapped_outcome_count"] == 1
    event = snapshot["events"][0]
    outcome = event["outcomes"][0]
    assert event["market_consensus"] is True
    assert event["consensus_value"] == 3.1
    assert event["rates_repricing_bps"] is None
    assert "missing_numeric_intraday_rate_repricing" in event["degradation_reasons"]
    assert outcome["matured_utc"] == "2026-08-16T12:01:01.000000Z"
    assert outcome["outcome_known_utc"] == "2026-08-16T12:01:02.000000Z"
    assert outcome["currency_mid_return_pips"] == 10.0
    assert outcome["strengthening_after_cost_pips"] == 8.0
    assert outcome["direction_policy"] == "abstain"

    view = normalize_point_in_time_event(
        event,
        decision_cutoff_utc="2026-08-16T12:02:00Z",
        contract=SELECTOR,
    )
    assert view["consensus_state"] == "causal_market_consensus"
    assert view["standardized_surprise"] is None
    assert view["rates_repricing_bps"] is None


def test_noncausal_consensus_and_unclocked_standardization_are_withheld(tmp_path):
    adapter, paths = _fixture(tmp_path)
    _insert_release(
        paths.macro_surprise_db,
        consensus_value=3.1,
        standardized_surprise=1.4,
    )
    _insert_consensus(
        paths.macro_surprise_db,
        captured="2026-08-16T12:00:01Z",
        source_time="2026-08-16T12:00:01Z",
        causal_valid=0,
    )
    snapshot = adapter.build_snapshot(decision_cutoff_utc="2026-08-16T12:02:00Z")
    event = snapshot["events"][0]

    assert event["market_consensus"] is False
    assert event["consensus_value"] is None
    assert event["standardized_surprise"] is None
    assert "missing_causal_consensus_provenance" in event["degradation_reasons"]
    assert "noncausal_revision_consensus_withheld" in event["degradation_reasons"]
    assert "noncausal_standardization_withheld" in event["degradation_reasons"]
    assert snapshot["coverage"]["mapped_event_degradation_count_by_reason"][
        "missing_causal_consensus_provenance"
    ] == 1
    assert snapshot["coverage"]["excluded_count_by_reason"][
        "missing_causal_consensus_provenance"
    ] == 1


def test_consensus_release_identity_mismatch_is_withheld(tmp_path):
    adapter, paths = _fixture(tmp_path)
    _insert_release(paths.macro_surprise_db)
    _insert_consensus(paths.macro_surprise_db)
    connection = sqlite3.connect(paths.macro_surprise_db)
    connection.execute(
        "UPDATE macro_consensus_observations SET scheduled_utc=?",
        ("2026-08-16T13:00:00Z",),
    )
    connection.commit()
    connection.close()

    snapshot = adapter.build_snapshot(decision_cutoff_utc="2026-08-16T14:00:00Z")
    event = snapshot["events"][0]
    assert event["market_consensus"] is False
    assert event["consensus_value"] is None
    assert "causal_consensus_release_identity_mismatch" in event["degradation_reasons"]


def test_duplicate_reaction_offset_withholds_ambiguous_outcome(tmp_path):
    adapter, paths = _fixture(tmp_path)
    _insert_release(paths.macro_surprise_db)
    _insert_reactions(paths.macro_surprise_db)
    connection = sqlite3.connect(paths.macro_surprise_db)
    row = connection.execute(
        "SELECT * FROM macro_reaction_samples WHERE sample_id='outcome'"
    ).fetchone()
    duplicate = list(row)
    duplicate[0] = "outcome_duplicate"
    connection.execute(
        "INSERT INTO macro_reaction_samples VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        duplicate,
    )
    connection.commit()
    connection.close()

    snapshot = adapter.build_snapshot(decision_cutoff_utc="2026-08-16T12:02:00Z")
    assert snapshot["coverage"]["mapped_outcome_count"] == 0
    assert snapshot["coverage"]["excluded_count_by_reason"][
        "duplicate_reaction_offset_withheld"
    ] == 1


def test_direct_response_matured_rows_without_outcome_known_clock_are_withheld(tmp_path):
    adapter, paths = _fixture(tmp_path)
    connection = sqlite3.connect(paths.direct_source_response_db)
    connection.execute(
        "INSERT INTO response_targets VALUES(?,?,?,?,?,?)",
        ("target_1", "matured", "2026-08-16T12:01:00Z", "obs_1", 60, "USD_JPY"),
    )
    connection.commit()
    connection.close()

    snapshot = adapter.build_snapshot(decision_cutoff_utc="2026-08-16T12:02:00Z")
    direct = snapshot["input_states"]["direct_source_response"]
    assert direct["outcome_known_timestamp_column_present"] is False
    assert direct["mapped_outcome_count"] == 0
    assert direct["withheld_matured_outcome_count"] == 1
    assert snapshot["coverage"]["excluded_count_by_reason"][
        "missing_exact_outcome_known_timestamp"
    ] == 1


def test_future_outcome_is_not_visible_at_earlier_cutoff(tmp_path):
    adapter, paths = _fixture(tmp_path)
    _insert_release(paths.macro_surprise_db)
    _insert_reactions(paths.macro_surprise_db)

    snapshot = adapter.build_snapshot(decision_cutoff_utc="2026-08-16T12:00:30Z")
    assert snapshot["coverage"]["mapped_event_count"] == 1
    assert snapshot["coverage"]["mapped_outcome_count"] == 0


def test_broker_clock_ahead_of_collector_is_reconciled_conservatively(tmp_path):
    adapter, paths = _fixture(tmp_path)
    _insert_release(paths.macro_surprise_db)
    _insert_reactions(paths.macro_surprise_db)
    connection = sqlite3.connect(paths.macro_surprise_db)
    connection.execute(
        "UPDATE macro_reaction_samples SET sampled_utc=? WHERE sample_id='outcome'",
        ("2026-08-16T12:00:59Z",),
    )
    connection.commit()
    connection.close()

    snapshot = adapter.build_snapshot(decision_cutoff_utc="2026-08-16T12:02:00Z")
    outcome = snapshot["events"][0]["outcomes"][0]
    assert outcome["collector_observed_utc"] == "2026-08-16T12:00:59.000000Z"
    assert outcome["outcome_known_utc"] == "2026-08-16T12:01:01.000000Z"
    assert outcome["quote_clock_ahead_of_collector_clock"] is True


def test_quarantined_source_event_does_not_map(tmp_path):
    adapter, paths = _fixture(tmp_path)
    _insert_release(paths.macro_surprise_db, source_event_id="quarantined")
    connection = sqlite3.connect(paths.source_governance_db)
    connection.execute("INSERT INTO source_event_quarantines VALUES(?)", ("quarantined",))
    connection.commit()
    connection.close()

    snapshot = adapter.build_snapshot(decision_cutoff_utc="2026-08-16T12:02:00Z")
    assert snapshot["coverage"]["mapped_event_count"] == 0
    assert snapshot["coverage"]["excluded_count_by_reason"]["quarantined_source_event"] == 1


def test_daily_rates_are_reported_as_context_not_intraday_repricing(tmp_path):
    adapter, paths = _fixture(tmp_path)
    connection = sqlite3.connect(paths.daily_rates_db)
    connection.execute(
        "INSERT INTO daily_rate_observations VALUES(?,?,?)",
        ("rate_1", "USD", 1),
    )
    connection.commit()
    connection.close()

    snapshot = adapter.build_snapshot(decision_cutoff_utc="2026-08-16T12:02:00Z")
    rates = snapshot["input_states"]["daily_rates"]
    assert rates["row_count"] == 1
    assert rates["numeric_intraday_repricing_available"] is False
    assert snapshot["coverage"][
        "mapped_event_with_numeric_intraday_rate_repricing_count"
    ] == 0


def test_outcome_selected_movement_episodes_are_never_mapped(tmp_path):
    adapter, paths = _fixture(tmp_path)
    connection = sqlite3.connect(paths.movement_episode_db)
    connection.execute("INSERT INTO movement_episodes VALUES('episode_1')")
    connection.execute("INSERT INTO episode_source_links VALUES('episode_1','source_1',1)")
    connection.commit()
    connection.close()

    snapshot = adapter.build_snapshot(decision_cutoff_utc="2026-08-16T12:02:00Z")
    state = snapshot["input_states"]["movement_episodes"]
    assert state["movement_episode_count"] == 1
    assert state["causal_source_link_count"] == 1
    assert state["mapped_outcome_count"] == 0
    assert snapshot["coverage"]["excluded_count_by_reason"][
        "outcome_selected_movement_episode_not_admissible"
    ] == 1


def test_bounded_release_read_reports_truncation(tmp_path):
    adapter, paths = _fixture(tmp_path)
    _insert_release(paths.macro_surprise_db, release_key="a", revision_id="a")
    _insert_release(paths.macro_surprise_db, release_key="b", revision_id="b")
    config = deepcopy(CONFIG)
    config["maximum_release_rows"] = 1
    bounded = SourceResponseAnalogHistoryAdapter(
        paths=paths,
        config=config,
        selector_contract=SELECTOR,
    )

    snapshot = bounded.build_snapshot(decision_cutoff_utc="2026-08-16T12:02:00Z")
    assert snapshot["coverage"]["mapped_event_count"] == 1
    assert snapshot["input_states"]["macro_surprise"]["release_read_truncated"] is True


def test_safety_contract_and_import_boundary(tmp_path):
    adapter, _ = _fixture(tmp_path)
    snapshot = adapter.build_snapshot(decision_cutoff_utc="2026-08-16T12:02:00Z")
    assert snapshot["research_only"] is True
    assert snapshot["execution_eligible"] is False
    assert snapshot["can_place_orders"] is False
    assert snapshot["supported_execution_decision"] == "no_trade"
    assert snapshot["direction_policy"] == "abstain"

    path = ROOT / "src" / "forex_system" / "ingestion" / "source_response_analog_history_adapter.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    assert not any(
        token in name.lower()
        for name in imports
        for token in ("oanda", "broker", "execution", "lifecycle", "authorization", "supervisor")
    )
