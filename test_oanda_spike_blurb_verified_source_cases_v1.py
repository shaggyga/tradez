import copy
import json
import sqlite3

import pytest

import oanda_spike_blurb_verified_source_cases_v1 as verified


def frozen_config():
    return json.loads(verified.CONFIG.read_text(encoding="utf-8"))


def test_frozen_verified_source_config_is_valid_and_inert():
    config = verified.validate_config(frozen_config())
    assert len(config["cases"]) == 3
    assert config["research_only"] is True
    assert config["execution_eligible"] is False
    assert config["forecast_proof_eligible"] is False
    assert all(case["direction_policy"].startswith("abstain") for case in config["cases"])


def test_date_only_document_cannot_claim_exact_publication_clock():
    config = frozen_config()
    document = config["cases"][2]["documents"][0]
    document["published_at_utc"] = "2025-04-04T10:00:00+00:00"
    with pytest.raises(ValueError, match="date_only_document_cannot_have_exact_publication_clock"):
        verified.validate_config(config)


def test_secondary_source_cannot_be_causal_content():
    config = frozen_config()
    config["cases"][2]["documents"][1]["content_causal_at_event"] = True
    with pytest.raises(ValueError, match="secondary_document_cannot_be_causal_content"):
        verified.validate_config(config)


def test_watch_requires_exact_primary_clock_support():
    config = frozen_config()
    case = config["cases"][0]
    for document in case["documents"]:
        document["event_clock_utc"] = None
    with pytest.raises(ValueError, match="watch_clock_lacks_primary_support"):
        verified.validate_config(config)


def test_direction_assignment_cannot_replace_abstention():
    config = frozen_config()
    config["cases"][1]["direction_policy"] = "usd_negative"
    with pytest.raises(ValueError, match="direction_policy_must_abstain"):
        verified.validate_config(config)


def test_factor_requires_one_value_representation():
    config = frozen_config()
    factor = config["cases"][1]["factors"][0]
    factor["value_text"] = "also_set"
    with pytest.raises(ValueError, match="factor_requires_exactly_one_value_representation"):
        verified.validate_config(config)


def test_verified_source_tables_are_immutable(tmp_path):
    connection = sqlite3.connect(tmp_path / "test.sqlite")
    verified.ensure_schema(connection)
    connection.execute(
        "INSERT INTO verified_external_source_contracts VALUES (?,?,?,?,?,?,?)",
        ("c", "u", 1, "{}", "a" * 64, "b" * 64, "2026-01-01T00:00:00+00:00"),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("DELETE FROM verified_external_source_contracts WHERE contract_id='c'")
