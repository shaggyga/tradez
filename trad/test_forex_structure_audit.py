from pathlib import Path

import forex_structure_audit as structure


ROOT = Path(__file__).resolve().parent


def test_layout_is_practice_only_and_compatibility_first():
    layout = structure.load_layout(ROOT / "config" / "project_layout_v1.json")
    assert layout["broker_environment"] == "practice"
    assert layout["real_money_enabled"] is False
    assert layout["migration_mode"] == "compatibility_first"


def test_domain_assignment_distinguishes_execution_and_ingestion():
    layout = structure.load_layout(ROOT / "config" / "project_layout_v1.json")
    domains = layout["domains"]
    execution = structure.domain_matches(
        "oanda_practice_top_signal_executor.py", domains
    )
    ingestion = structure.domain_matches("oanda_local_news_sentiment.py", domains)
    assert structure.choose_owner(execution) == "execution"
    assert structure.choose_owner(ingestion) == "ingestion"


def test_supervisor_worker_inventory_contains_critical_workers():
    workers = structure.extract_supervisor_workers(ROOT / "oanda_always_on_supervisor.ps1")
    names = {row["name"] for row in workers}
    assert "local_news_sentiment" in names
    assert "practice_007_fast_executor" in names
    assert len(names) >= 40


def test_structure_audit_is_read_only_and_operationally_safe():
    audit = structure.build_audit(
        ROOT,
        ROOT / "config" / "project_layout_v1.json",
    )
    assert audit["status"] == "ok"
    assert audit["root_source_file_count"] >= 400
    assert audit["policy"] == {
        "read_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
    }
