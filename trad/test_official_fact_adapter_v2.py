from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from forex_system.features.currency_state_official_context_v2 import (  # noqa: E402
    CONTEXT_CONTRACT_ID,
    CONTEXT_V2_TOP_LEVEL_FIELDS,
    OFFICIAL_COMPONENT_CONTEXT_FIELDS,
    OFFICIAL_COMPONENT_FIELDS_BY_ID,
    OFFICIAL_CURRENCY_SUMMARY_FIELDS,
    attach_official_fact_context_v2,
    validate_currency_state_official_context_v2_snapshot,
)
from forex_system.contracts.currency_state import stable_hash  # noqa: E402
from forex_system.ingestion.immutable_event_clock import (  # noqa: E402
    CONTRACT_ID as CLOCK_V2_CONTRACT_ID,
    SCHEMA_VERSION as CLOCK_V2_SCHEMA_VERSION,
)
from forex_system.ingestion import immutable_event_clock_v1_reader as v1_reader  # noqa: E402
from forex_system.ingestion.official_fact_adapter_v2 import (  # noqa: E402
    ADAPTER_CONTRACT_ID,
    OFFICIAL_FACT_V2_TOP_LEVEL_FIELDS,
    OFFICIAL_UPCOMING_EVENT_FIELDS,
    OfficialFactAdapterV2,
    OfficialFactAdapterV2Error,
    OfficialFactPathsV2,
    validate_official_fact_v2_snapshot,
)
from test_oanda_currency_state_official_context import (  # noqa: E402
    base_snapshot,
    official_snapshot,
)


UTC = dt.timezone.utc
ADVERSARIAL_DIRECTION_EXECUTION_ALIASES = (
    "directional_score",
    "Directional-Score",
    "DIRECTIONAL_SCORE",
    "suggested_side",
    "suggested-side",
    "SuggestedSide",
    "trade_direction",
    "Trade-Direction",
    "trade_side",
    "position_side",
    "buy_sell",
    "buy-sell",
    "trade_action",
    "maximum_units",
    "maximum-units",
    "units",
    "notional",
)


def _canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _create_clock_fixture(
    path: Path,
    *,
    schema_version: str = v1_reader.SCHEMA_VERSION,
    contract_id: str = v1_reader.CONTRACT_ID,
) -> Path:
    captured = "2026-08-17T02:00:00+00:00"
    generated = "2026-08-17T01:59:00+00:00"
    event = {
        "event_version_id": "event_version_fixture",
        "upstream_event_id": "fixture-eur-clock",
        "scheduled_utc": "2026-08-20T12:15:00+00:00",
        "upstream_first_known_utc": "2026-08-17T01:00:00+00:00",
        "currencies": ["EUR"],
        "direct_currencies": ["EUR"],
        "category": "monetary_policy",
        "headline": "ECB policy decision",
        "direction_policy": "abstain",
        "execution_eligible": False,
        "can_promote": True,
        "authorization": {"nonce": "must-not-survive"},
        "nested": {
            "forecast_mean_bps": 99.0,
            "allocator_rank": 1,
            "execution": {"route": "practice"},
            "safe_context": "retained",
            "Directional-Score": 0.99,
            "suggested_side": "buy",
            "suggested-side": "buy",
            "trade_direction": "buy",
            "Trade-Direction": "buy",
            "trade_side": "buy",
            "position_side": "long",
            "buy_sell": "buy",
            "trade_action": "open",
            "maximum_units": 1000,
            "units": 1000,
            "notional": 100000,
        },
    }
    snapshot_id = "event_clock_snapshot_fixture"
    snapshot = {
        "schema_version": schema_version,
        "contract_id": contract_id,
        "snapshot_id": snapshot_id,
        "captured_utc": captured,
        "source_generated_utc": generated,
        "source_pipeline_version": v1_reader.EXPECTED_SOURCE_PIPELINE_VERSION,
        "source_event_count": 1,
        "source_scheduled_clock_count": 1,
        "rejected_malformed_clock_count": 0,
        "events_sha256": "a" * 64,
        "manifest_sha256": "b" * 64,
        "events_artifact_name": "events_latest.json",
        "manifest_artifact_name": "events_latest.manifest.json",
        "semantic_clock_sha256": "f" * 64,
        "event_versions": [event],
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE event_clock_snapshots(
              snapshot_id TEXT PRIMARY KEY,schema_version TEXT NOT NULL,
              contract_id TEXT NOT NULL,captured_utc TEXT NOT NULL,
              source_generated_utc TEXT NOT NULL,source_pipeline_version TEXT NOT NULL,
              source_event_count INTEGER NOT NULL,
              source_scheduled_clock_count INTEGER NOT NULL,
              rejected_malformed_clock_count INTEGER NOT NULL,
              events_sha256 TEXT NOT NULL,manifest_sha256 TEXT NOT NULL,
              events_artifact_name TEXT NOT NULL,manifest_artifact_name TEXT NOT NULL,
              snapshot_json TEXT NOT NULL
            );
            CREATE TABLE event_clock_versions(
              event_version_id TEXT PRIMARY KEY,upstream_event_id TEXT NOT NULL,
              scheduled_utc TEXT NOT NULL,event_json TEXT NOT NULL
            );
            CREATE TABLE snapshot_events(
              snapshot_id TEXT NOT NULL,event_version_id TEXT NOT NULL,
              ordinal INTEGER NOT NULL,effective_known_utc TEXT NOT NULL,
              PRIMARY KEY(snapshot_id,event_version_id)
            );
            """
        )
        connection.execute(
            "INSERT INTO event_clock_versions VALUES(?,?,?,?)",
            (
                event["event_version_id"],
                event["upstream_event_id"],
                event["scheduled_utc"],
                _canonical(event),
            ),
        )
        connection.execute(
            "INSERT INTO snapshot_events VALUES(?,?,?,?)",
            (snapshot_id, event["event_version_id"], 0, captured),
        )
        connection.execute(
            "INSERT INTO event_clock_snapshots VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                snapshot_id,
                schema_version,
                contract_id,
                captured,
                generated,
                v1_reader.EXPECTED_SOURCE_PIPELINE_VERSION,
                1,
                1,
                0,
                "a" * 64,
                "b" * 64,
                "events_latest.json",
                "events_latest.manifest.json",
                _canonical(snapshot),
            ),
        )
        connection.commit()
    finally:
        connection.close()
    return _write_retirement_fingerprint(path)


def _write_retirement_fingerprint(database: Path) -> Path:
    connection = sqlite3.connect(database)
    try:
        tables = [
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        ]
        counts = {
            table: int(
                connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            for table in tables
        }
        quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        connection.close()
    fingerprint = database.with_suffix(".retirement.json")
    fingerprint.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "record_type": "immutable_event_clock_retirement_fingerprint",
                "record_id": "unit_test_v1_retirement",
                "status": "retired_terminal",
                "clock_schema_version": v1_reader.SCHEMA_VERSION,
                "clock_contract_id": v1_reader.CONTRACT_ID,
                "frozen_at_utc": "2026-08-17T03:00:00+00:00",
                "database": {
                    "relative_path": database.name,
                    "byte_length": database.stat().st_size,
                    "sha256": hashlib.sha256(database.read_bytes()).hexdigest(),
                    "quick_check": quick_check,
                    "row_counts": counts,
                    "preserve_bytes": True,
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return fingerprint


def _mock_v2_snapshot(
    *,
    availability: str = "2026-08-17T00:59:30+00:00",
    observed: str = "2026-08-17T00:59:30+00:00",
) -> dict:
    return {
        "schema_version": CLOCK_V2_SCHEMA_VERSION,
        "contract_id": CLOCK_V2_CONTRACT_ID,
        "decision_cutoff_utc": "2026-08-17T01:00:00+00:00",
        "snapshot_id": "event_clock_snapshot_v2_fixture",
        "snapshot_captured_utc": "2026-08-17T00:59:25+00:00",
        "clock_observation_id": "event_clock_observation_v2_fixture",
        "clock_observed_utc": observed,
        "clock_observation_source_generated_utc": "2026-08-17T00:59:20+00:00",
        "source_generated_utc": "2026-08-17T00:59:20+00:00",
        "events_sha256": "c" * 64,
        "manifest_sha256": "d" * 64,
        "clock_attestation": {
            "state": "fresh_trusted",
            "attested": True,
            "generated_utc": "2026-08-17T00:59:25+00:00",
            "payload_sha256": "e" * 64,
        },
        "events": [
            {
                "upstream_event_id": "eur-clock-v2",
                "event_version_id": "eur-clock-v2-version",
                "scheduled_utc": "2026-08-20T12:15:00+00:00",
                "ledger_effective_known_utc": availability,
                "currencies": ["EUR"],
                "direct_currencies": ["EUR"],
                "category": "monetary_policy",
                "headline": "ECB policy decision",
                "timing_precision": "minute",
                "currency_bias": {"EUR": 1.0},
                "pair_bias": {"EUR_USD": "buy"},
                "direction": "buy",
                "execution_eligible": True,
            }
        ],
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }


def _v2_official_fixture(cutoff: str) -> dict:
    result = official_snapshot(cutoff)
    result["facts"][0].pop("direction", None)
    result.update(
        {
            "schema_version": 2,
            "adapter_contract_id": ADAPTER_CONTRACT_ID,
            "parent_adapter_contract_id": "official_fact_adapter_v1_20260817",
            "immutable_event_clock_schema_version": CLOCK_V2_SCHEMA_VERSION,
            "immutable_event_clock_contract_id": CLOCK_V2_CONTRACT_ID,
            "clock_dependency_policy": "strict_v2_only_no_mutable_fallback",
            "maximum_clock_observation_age_sec": 300.0,
            "can_promote": False,
            "can_authorize": False,
            "event_clock_provenance": {
                "state": "immutable_v2_fresh_attested_at_cutoff",
                "schema_version": CLOCK_V2_SCHEMA_VERSION,
                "contract_id": CLOCK_V2_CONTRACT_ID,
                "fallback_used": False,
                "clock_ready_for_cutoff": True,
                "complete_snapshot_at_cutoff": True,
            },
        }
    )
    _brand_v2_official(result)
    return result


def _brand_v2_official(result: dict) -> None:
    material = {key: value for key, value in result.items() if key != "snapshot_id"}
    encoded = json.dumps(
        material,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=str,
    )
    result["snapshot_id"] = "official_fact_v2_snapshot_" + hashlib.sha256(
        encoded.encode("utf-8")
    ).hexdigest()[:24]


def _brand_v2_context(result: dict) -> None:
    result["snapshot_id"] = "currency_state_official_v2_" + stable_hash(
        {
            "context_contract_id": CONTEXT_CONTRACT_ID,
            "fact_basis_eligibility_contract_id": result["component_context"][
                "fact_basis_eligibility_contract_id"
            ],
            "base_snapshot_id": result["base_currency_state_snapshot_id"],
            "official_fact_snapshot_id": result["official_fact_snapshot_id"],
            "official_fact_adapter_contract_id": ADAPTER_CONTRACT_ID,
            "immutable_event_clock_contract_id": CLOCK_V2_CONTRACT_ID,
            "component_context": result["component_context"],
        }
    )[:24]


def test_v1_reader_is_exact_read_only_and_does_not_backdate(tmp_path: Path) -> None:
    database = tmp_path / "clock_v1.sqlite"
    fingerprint = _create_clock_fixture(database)
    before_hash = hashlib.sha256(database.read_bytes()).hexdigest()
    identity = v1_reader.inspect_v1_identity(
        database, retirement_fingerprint_path=fingerprint
    )
    before = v1_reader.reconstruct_v1_as_of(
        database,
        dt.datetime(2026, 8, 17, 1, 59, tzinfo=UTC),
        retirement_fingerprint_path=fingerprint,
    )
    after = v1_reader.reconstruct_v1_as_of(
        database,
        dt.datetime(2026, 8, 17, 2, 1, tzinfo=UTC),
        retirement_fingerprint_path=fingerprint,
    )
    assert identity["schema_version"] == v1_reader.SCHEMA_VERSION
    assert identity["contract_id"] == v1_reader.CONTRACT_ID
    assert before["snapshot_id"] is None
    assert before["events"] == []
    assert after["event_count"] == 1
    assert after["events"][0]["event_availability_utc"] == "2026-08-17T02:00:00+00:00"
    assert after["events"][0]["direction_policy"] == "abstain"
    assert after["events"][0]["execution_eligible"] is False
    assert after["events"][0]["can_promote"] is False
    assert after["events"][0]["can_authorize"] is False
    serialized = json.dumps(after, sort_keys=True)
    assert "must-not-survive" not in serialized
    assert "forecast_mean_bps" not in serialized
    assert "allocator_rank" not in serialized
    assert '"route": "practice"' not in serialized
    assert "safe_context" not in serialized
    assert set(after["events"][0]) == v1_reader.HISTORICAL_EVENT_SAFE_OUTPUT_FIELDS
    for alias in (
        "Directional-Score",
        "suggested_side",
        "suggested-side",
        "trade_direction",
        "Trade-Direction",
        "trade_side",
        "position_side",
        "buy_sell",
        "trade_action",
        "maximum_units",
        "units",
        "notional",
    ):
        assert alias not in serialized
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before_hash


def test_v1_reader_rejects_v2_and_exposes_no_writer(tmp_path: Path) -> None:
    database = tmp_path / "clock_v2.sqlite"
    fingerprint = _create_clock_fixture(
        database,
        schema_version=CLOCK_V2_SCHEMA_VERSION,
        contract_id=CLOCK_V2_CONTRACT_ID,
    )
    with pytest.raises(v1_reader.ImmutableEventClockV1ReadError, match="identity_mismatch"):
        v1_reader.inspect_v1_identity(
            database, retirement_fingerprint_path=fingerprint
        )
    assert not any(
        token in name
        for name in v1_reader.__all__
        for token in ("append", "create", "initialize", "write", "collect")
    )
    reader = v1_reader.ImmutableEventClockV1Reader(
        database, retirement_fingerprint_path=fingerprint
    )
    for name in ("append", "write", "initialize", "create_schema"):
        assert not hasattr(reader, name)


def test_v1_reader_rejects_backdated_observation_reference(tmp_path: Path) -> None:
    database = tmp_path / "clock_v1.sqlite"
    _create_clock_fixture(database)
    material = {
        "snapshot_id": "event_clock_snapshot_fixture",
        "observed_utc": "2026-08-17T01:59:59+00:00",
        "source_generated_utc": "2026-08-17T01:59:00+00:00",
        "semantic_clock_sha256": "f" * 64,
        "events_sha256": "a" * 64,
        "manifest_sha256": "b" * 64,
        "clock_attestation": {
            "state": "unattested_fixture",
            "attested": False,
            "payload_sha256": "",
        },
    }
    observation_id = "event_clock_observation_" + hashlib.sha256(
        _canonical(material).encode("utf-8")
    ).hexdigest()[:32]
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            """CREATE TABLE event_clock_observations(
                   observation_id TEXT PRIMARY KEY,snapshot_id TEXT NOT NULL,
                   observed_utc TEXT NOT NULL,source_generated_utc TEXT NOT NULL,
                   semantic_clock_sha256 TEXT NOT NULL,events_sha256 TEXT NOT NULL,
                   manifest_sha256 TEXT NOT NULL,attestation_state TEXT NOT NULL,
                   attested INTEGER NOT NULL,attestation_payload_sha256 TEXT,
                   observation_json TEXT NOT NULL)"""
        )
        connection.execute(
            "INSERT INTO event_clock_observations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                observation_id,
                material["snapshot_id"],
                material["observed_utc"],
                material["source_generated_utc"],
                material["semantic_clock_sha256"],
                material["events_sha256"],
                material["manifest_sha256"],
                "unattested_fixture",
                0,
                "",
                _canonical(material),
            ),
        )
        connection.commit()
    finally:
        connection.close()
    fingerprint = _write_retirement_fingerprint(database)
    with pytest.raises(
        v1_reader.ImmutableEventClockV1ReadError,
        match="observation_precedes_referenced_snapshot_capture",
    ):
        v1_reader.reconstruct_v1_as_of(
            database,
            dt.datetime(2026, 8, 17, 3, 0, tzinfo=UTC),
            retirement_fingerprint_path=fingerprint,
        )


def test_v1_reader_requires_frozen_fingerprint_before_reconstruction(
    tmp_path: Path,
) -> None:
    database = tmp_path / "clock_v1.sqlite"
    fingerprint = _create_clock_fixture(database)
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "INSERT INTO event_clock_versions VALUES(?,?,?,?)",
            ("tamper", "tamper", "2026-08-21T00:00:00+00:00", "{}"),
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(
        v1_reader.ImmutableEventClockV1ReadError,
        match="retirement_(byte_length|sha256)_mismatch",
    ):
        v1_reader.reconstruct_v1_as_of(
            database,
            dt.datetime(2026, 8, 17, 3, 0, tzinfo=UTC),
            retirement_fingerprint_path=fingerprint,
        )


def test_canonical_v1_retirement_fingerprint_matches_preserved_ledger() -> None:
    database = (
        ROOT
        / "data"
        / "oanda_training_manager"
        / "research_ledgers"
        / "immutable_event_clock_v1.sqlite"
    )
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    identity = v1_reader.inspect_v1_identity(database)
    assert identity["snapshot_count"] == 3
    assert identity["schema_version"] == v1_reader.SCHEMA_VERSION
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_adapter_v2_rejects_a_v1_clock_instead_of_falling_back(tmp_path: Path) -> None:
    database = tmp_path / "clock.sqlite"
    _create_clock_fixture(database)
    paths = OfficialFactPathsV2(
        immutable_event_clock_db=database,
        source_governance_db=tmp_path / "missing-governance.sqlite",
        macro_surprise_db=tmp_path / "missing-macro.sqlite",
        daily_rates_db=tmp_path / "missing-rates.sqlite",
        internal_expectations_db=tmp_path / "missing-expectations.sqlite",
        policy_baselines_json=tmp_path / "missing-policy.json",
        event_preflight_json=tmp_path / "mutable-fallback-must-not-be-read.json",
        source_coverage_json=tmp_path / "missing-coverage.json",
        clock_integrity_json=tmp_path / "missing-integrity.json",
        intraday_rates_config_json=tmp_path / "missing-intraday.json",
    )
    with pytest.raises(OfficialFactAdapterV2Error, match="identity_or_integrity"):
        OfficialFactAdapterV2(paths=paths).as_of(
            "2026-08-17T02:01:00+00:00", currencies=("EUR",)
        )


def test_adapter_v2_strips_upstream_direction_and_remains_no_trade(tmp_path: Path) -> None:
    placeholder = tmp_path / "clock-v2-placeholder.sqlite"
    placeholder.write_bytes(b"not-used-because-reconstruction-is-patched")
    paths = OfficialFactPathsV2(
        immutable_event_clock_db=placeholder,
        source_governance_db=tmp_path / "missing-governance.sqlite",
        macro_surprise_db=tmp_path / "missing-macro.sqlite",
        daily_rates_db=tmp_path / "missing-rates.sqlite",
        internal_expectations_db=tmp_path / "missing-expectations.sqlite",
        policy_baselines_json=tmp_path / "missing-policy.json",
        event_preflight_json=tmp_path / "must-not-fallback.json",
        source_coverage_json=tmp_path / "missing-coverage.json",
        clock_integrity_json=tmp_path / "missing-integrity.json",
        intraday_rates_config_json=tmp_path / "missing-intraday.json",
    )
    with patch(
        "forex_system.ingestion.official_fact_adapter_v2.reconstruct_event_clock_as_of",
        return_value=_mock_v2_snapshot(),
    ):
        snapshot = OfficialFactAdapterV2(paths=paths).as_of(
            "2026-08-17T01:00:00+00:00", currencies=("EUR",)
        )
    assert snapshot["schema_version"] == 2
    assert snapshot["adapter_contract_id"] == ADAPTER_CONTRACT_ID
    assert snapshot["immutable_event_clock_contract_id"] == CLOCK_V2_CONTRACT_ID
    assert snapshot["event_clock_provenance"]["fallback_used"] is False
    assert snapshot["upcoming_event_count"] == 1
    assert set(snapshot) == OFFICIAL_FACT_V2_TOP_LEVEL_FIELDS
    assert set(snapshot["upcoming_events"][0]) == OFFICIAL_UPCOMING_EVENT_FIELDS
    serialized = json.dumps(snapshot, sort_keys=True)
    assert "currency_bias" not in serialized
    assert "pair_bias" not in serialized
    assert '"direction": "buy"' not in serialized
    assert snapshot["upcoming_events"][0]["direction_policy"] == "abstain"
    assert snapshot["execution_eligible"] is False
    assert snapshot["can_place_orders"] is False
    assert snapshot["supported_execution_decision"] == "no_trade"


def test_adapter_v2_rejects_event_availability_after_cutoff(tmp_path: Path) -> None:
    placeholder = tmp_path / "clock-v2-placeholder.sqlite"
    placeholder.write_bytes(b"fixture")
    paths = OfficialFactPathsV2(
        immutable_event_clock_db=placeholder,
        source_governance_db=tmp_path / "missing-governance.sqlite",
        macro_surprise_db=tmp_path / "missing-macro.sqlite",
        daily_rates_db=tmp_path / "missing-rates.sqlite",
        internal_expectations_db=tmp_path / "missing-expectations.sqlite",
        policy_baselines_json=tmp_path / "missing-policy.json",
        event_preflight_json=tmp_path / "must-not-fallback.json",
        source_coverage_json=tmp_path / "missing-coverage.json",
        clock_integrity_json=tmp_path / "missing-integrity.json",
        intraday_rates_config_json=tmp_path / "missing-intraday.json",
    )
    with patch(
        "forex_system.ingestion.official_fact_adapter_v2.reconstruct_event_clock_as_of",
        return_value=_mock_v2_snapshot(availability="2026-08-17T01:00:01+00:00"),
    ), pytest.raises(OfficialFactAdapterV2Error, match="not_known_at_cutoff"):
        OfficialFactAdapterV2(paths=paths).as_of(
            "2026-08-17T01:00:00+00:00", currencies=("EUR",)
        )


def test_adapter_v2_degrades_stale_clock_without_claiming_completeness(
    tmp_path: Path,
) -> None:
    placeholder = tmp_path / "clock-v2-placeholder.sqlite"
    placeholder.write_bytes(b"fixture")
    paths = OfficialFactPathsV2(
        immutable_event_clock_db=placeholder,
        source_governance_db=tmp_path / "missing-governance.sqlite",
        macro_surprise_db=tmp_path / "missing-macro.sqlite",
        daily_rates_db=tmp_path / "missing-rates.sqlite",
        internal_expectations_db=tmp_path / "missing-expectations.sqlite",
        policy_baselines_json=tmp_path / "missing-policy.json",
        event_preflight_json=tmp_path / "must-not-fallback.json",
        source_coverage_json=tmp_path / "missing-coverage.json",
        clock_integrity_json=tmp_path / "missing-integrity.json",
        intraday_rates_config_json=tmp_path / "missing-intraday.json",
    )
    stale = _mock_v2_snapshot(
        availability="2026-08-17T00:30:00+00:00",
        observed="2026-08-17T00:30:00+00:00",
    )
    stale["snapshot_captured_utc"] = "2026-08-17T00:29:55+00:00"
    stale["clock_observation_source_generated_utc"] = "2026-08-17T00:29:50+00:00"
    stale["source_generated_utc"] = "2026-08-17T00:29:50+00:00"
    stale["clock_attestation"]["generated_utc"] = "2026-08-17T00:29:55+00:00"
    with patch(
        "forex_system.ingestion.official_fact_adapter_v2.reconstruct_event_clock_as_of",
        return_value=stale,
    ):
        snapshot = OfficialFactAdapterV2(paths=paths).as_of(
            "2026-08-17T01:00:00+00:00", currencies=("EUR",)
        )
    provenance = snapshot["event_clock_provenance"]
    assert provenance["state"] == "immutable_v2_stale_at_cutoff"
    assert provenance["clock_ready_for_cutoff"] is False
    assert provenance["complete_snapshot_at_cutoff"] is False
    assert snapshot["upcoming_events"] == []
    assert snapshot["upcoming_event_count"] == 0
    assert snapshot["status"] == "degraded"


def test_currency_context_v2_rejects_v1_adapter_identity() -> None:
    contract, base = base_snapshot()
    official = official_snapshot(base["decision_cutoff_utc"])
    with pytest.raises(ValueError, match="adapter V2 schema"):
        attach_official_fact_context_v2(base, official, contract=contract)


def test_currency_context_v2_is_unscored_and_execution_ineligible() -> None:
    contract, base = base_snapshot()
    official = _v2_official_fixture(base["decision_cutoff_utc"])
    combined = attach_official_fact_context_v2(base, official, contract=contract)
    assert combined["component_context_contract_id"] == CONTEXT_CONTRACT_ID
    assert combined["official_fact_adapter_contract_id"] == ADAPTER_CONTRACT_ID
    assert combined["immutable_event_clock_contract_id"] == CLOCK_V2_CONTRACT_ID
    assert combined["snapshot_schema"] == "currency_state_official_context_snapshot_v2"
    assert combined["execution_eligible"] is False
    assert combined["can_place_orders"] is False
    assert combined["supported_execution_decision"] == "no_trade"
    assert set(combined) == CONTEXT_V2_TOP_LEVEL_FIELDS
    assert set(combined["component_context"]) == OFFICIAL_COMPONENT_CONTEXT_FIELDS
    for summary in combined["component_context"]["currency_summary"].values():
        assert set(summary) == OFFICIAL_CURRENCY_SUMMARY_FIELDS
    serialized = json.dumps(combined, sort_keys=True)
    assert "strengthen" not in serialized
    for horizon in combined["horizons"].values():
        for edge in horizon["pair_edges"].values():
            assert edge["forecast_mean_bps"] is None
            assert edge["expected_net_pips"] is None
            assert edge["allocator_rank"] is None
            assert edge["execution_eligible"] is False
        for currency in horizon["currencies"].values():
            for component in currency["components"]:
                component_id = component["component_id"]
                if component_id in OFFICIAL_COMPONENT_FIELDS_BY_ID:
                    assert set(component) == OFFICIAL_COMPONENT_FIELDS_BY_ID[component_id]


@pytest.mark.parametrize("surface", ["source_health", "global_gaps"])
def test_currency_context_v2_rejects_nested_branded_control_surfaces(
    surface: str,
) -> None:
    contract, base = base_snapshot()
    official = _v2_official_fixture(base["decision_cutoff_utc"])
    if surface == "source_health":
        official["currency_evidence"]["AUD"]["source_health"]["nested"] = {
            "authorization": {"nonce": "forbidden"}
        }
    else:
        official["global_gaps"].append(
            {"code": "nested-fixture", "forecast_mean_bps": 4.0}
        )
    _brand_v2_official(official)
    with pytest.raises(ValueError, match="integrity failed"):
        attach_official_fact_context_v2(base, official, contract=contract)


@pytest.mark.parametrize("alias", ADVERSARIAL_DIRECTION_EXECUTION_ALIASES)
def test_official_fact_v2_closed_schema_rejects_rehashed_deep_aliases(
    alias: str,
) -> None:
    _, base = base_snapshot()
    official = _v2_official_fixture(base["decision_cutoff_utc"])
    official["currency_evidence"]["AUD"]["source_health"][alias] = {
        "deep": [{"transport": {"value": "buy", "amount": 1000}}]
    }
    _brand_v2_official(official)
    with pytest.raises(OfficialFactAdapterV2Error, match="closed_schema_unknown_fields"):
        validate_official_fact_v2_snapshot(official)


@pytest.mark.parametrize("alias", ADVERSARIAL_DIRECTION_EXECUTION_ALIASES)
def test_context_v2_closed_schema_rejects_rehashed_deep_aliases(alias: str) -> None:
    contract, base = base_snapshot()
    official = _v2_official_fixture(base["decision_cutoff_utc"])
    combined = attach_official_fact_context_v2(base, official, contract=contract)
    combined["component_context"]["currency_summary"]["AUD"][alias] = {
        "deep": [{"transport": {"value": "buy", "amount": 1000}}]
    }
    _brand_v2_context(combined)
    with pytest.raises(ValueError, match="closed schema has unknown fields"):
        validate_currency_state_official_context_v2_snapshot(combined)


def test_retirement_record_preserves_zero_sample_v2_ledger_bytes() -> None:
    record = json.loads(
        (ROOT / "config" / "prospective_event_response_capture_v2_retirement.json").read_text(
            encoding="utf-8"
        )
    )
    assert record["status"] == "retired_terminal"
    assert record["retirement_reason"] == "clock_dependency_superseded_before_any_sample"
    frozen = record["frozen_ledger"]
    database = ROOT / frozen["relative_path"]
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    assert database.stat().st_size == frozen["byte_length"]
    assert before == frozen["sha256"]
    connection = sqlite3.connect(
        f"file:{database.resolve().as_posix()}?mode=ro", uri=True
    )
    try:
        assert connection.execute("SELECT COUNT(*) FROM event_response_samples").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM event_response_outcomes").fetchone()[0] == 0
    finally:
        connection.close()
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_frozen_v1_pins_and_retired_v6_dependency_mismatches_are_exact() -> None:
    expected_v1 = {
        "src/forex_system/ingestion/official_fact_adapter.py": "8b6a1605b95ea21dfdffde42a502eb7fa1b5f592d4c3dbc2af501e9cc1c5a6e4",
        "oanda_official_fact_adapter.py": "2527132426a000d837c308456e7d00c680945dd3bbc0f240ed9048a98e363555",
        "config/official_fact_adapter_contract_v1.json": "c6ab4e326e10dfdb7b150a18f6aea50ebdb06c96d5f999c1962e924b9d867583",
        "src/forex_system/features/currency_state_official_context.py": "beca86b5fec03a4cfe233680fd27560478ec5edaa3f23e2a0cd140fe52869418",
        "oanda_currency_state_official_context.py": "9706e80dd4db60ffe26f2fa7a45104efd6b82f305be519f2deb7609a315f674a",
    }
    for relative, expected in expected_v1.items():
        assert hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == expected

    manifest = json.loads(
        (ROOT / "config" / "currency_state_after_cost_counterfactual_v6_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    retirement = json.loads(
        (
            ROOT
            / "config"
            / "currency_state_after_cost_counterfactual_v6_retirement_20260824.json"
        ).read_text(encoding="utf-8")
    )
    observed_mismatches = {}
    for artifact in manifest["artifacts"].values():
        path = ROOT / artifact["relative_path"]
        observed = hashlib.sha256(path.read_bytes()).hexdigest()
        if observed != artifact["sha256"]:
            observed_mismatches[artifact["relative_path"]] = {
                "manifest_sha256": artifact["sha256"],
                "observed_sha256": observed,
            }
    assert retirement["status"] == "retired_terminal_zero_evidence"
    assert retirement["manifest"]["sha256"] == hashlib.sha256(
        (ROOT / retirement["manifest"]["relative_path"]).read_bytes()
    ).hexdigest()
    assert observed_mismatches == retirement["known_dependency_mismatches"]
    assert retirement["evidence_state_at_retirement"] == {
        "state_artifact_present": False,
        "proof_registry_rows": 0,
        "lifecycle_rows": 0,
        "genealogy_rows": 0,
        "execution_rows": 0,
    }
    assert retirement["disposition"]["can_restore_under_same_cohort_id"] is False
    assert retirement["execution_eligible"] is False


def test_v2_direct_dependency_manifests_are_complete_and_byte_exact() -> None:
    for relative in (
        "config/official_fact_adapter_v2_manifest.json",
        "config/currency_state_official_context_v2_manifest.json",
    ):
        manifest = json.loads((ROOT / relative).read_text(encoding="utf-8"))
        assert manifest["policy"][
            "all_direct_code_and_static_contract_dependencies_are_pinned"
        ] is True
        for artifact in manifest["artifacts"].values():
            path = ROOT / artifact["relative_path"]
            assert path.is_file()
            assert path.stat().st_size == artifact["byte_length"]
            assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact["sha256"]

    adapter_contract = json.loads(
        (ROOT / "config" / "official_fact_adapter_contract_v2.json").read_text(
            encoding="utf-8"
        )
    )
    context_contract = json.loads(
        (ROOT / "config" / "currency_state_official_context_contract_v2.json").read_text(
            encoding="utf-8"
        )
    )
    assert adapter_contract["adapter_contract_id"] == ADAPTER_CONTRACT_ID
    assert adapter_contract["immutable_event_clock_contract_id"] == CLOCK_V2_CONTRACT_ID
    assert context_contract["context_contract_id"] == CONTEXT_CONTRACT_ID
    assert context_contract["required_official_fact_adapter_contract_id"] == ADAPTER_CONTRACT_ID
    assert context_contract["required_immutable_event_clock_contract_id"] == CLOCK_V2_CONTRACT_ID
