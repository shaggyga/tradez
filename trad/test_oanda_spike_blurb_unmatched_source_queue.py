import json
import sqlite3
from pathlib import Path

import pytest

import oanda_spike_blurb_unmatched_source_queue as queue


def test_bucket_contracts():
    assert queue.liquidity_bucket(1.9) == "liquid_le_2p"
    assert queue.liquidity_bucket(2.1) == "moderate_2_to_5p"
    assert queue.liquidity_bucket(10.0) == "wide_5_to_15p"
    assert queue.liquidity_bucket(16.0) == "very_wide_gt_15p"
    assert queue.horizon_bucket(15) == "intrahour_le_15m"
    assert queue.horizon_bucket(60) == "intrahour_16_to_60m"


def test_authority_plan_never_assigns_causality(tmp_path: Path):
    central = tmp_path / "central.json"
    depth = tmp_path / "depth.json"
    central.write_text(json.dumps({"currencies": [
        {"currency": "EUR", "authority": "European Central Bank", "authority_id": "ecb", "policy_framework": "rates", "release_source_ids": ["ecb"], "communication_source_ids": [], "calendar_source_ids": [], "schedule_mode": "domestic"},
    ]}), encoding="utf-8")
    depth.write_text(json.dumps({"currencies": [{"currency": "EUR", "inflation": ["eurostat"]}]}), encoding="utf-8")
    sources = queue.load_currency_sources(central, depth)
    plan = queue.authority_plan("EUR", sources, "2026-01-01")
    assert plan["causality_state"] == "unresolved_do_not_force"
    assert plan["release_source_ids"] == ["ecb"]
    assert plan["official_numeric_source_families"]["inflation"] == ["eurostat"]


def test_immutable_tables_reject_updates(tmp_path: Path):
    database = tmp_path / "queue.sqlite"
    connection = sqlite3.connect(database)
    queue.ensure_schema(connection)
    connection.execute(
        "INSERT INTO unmatched_source_queue_contracts VALUES (?,?,?,?)",
        ("c", "{}", "a" * 64, "2026-01-01T00:00:00+00:00"),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("UPDATE unmatched_source_queue_contracts SET contract_json='x' WHERE contract_id='c'")
