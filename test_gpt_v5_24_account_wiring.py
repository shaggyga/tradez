from __future__ import annotations

import hashlib
import json
from pathlib import Path


TRAD_ROOT = Path(__file__).resolve().parent
SCRIPT_NAME = "oanda_forex_gpt_advisor_all_pairs_v5_24_movement_ledger.py"
ACCOUNT_ID = "101-001-37981792-002"


def test_gpt_v5_24_checkpoint_is_preserved_but_account_002_is_parked() -> None:
    registry_path = TRAD_ROOT / "config" / "accounts_registry.json"
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    account = registry["account_inventory"]["practice_accounts"]["002"]
    role = registry["roles"]["gpt_orig"]
    attachment = account["active_attachments"][0]

    assert account["account_id"] == ACCOUNT_ID
    assert account["status"] == "assigned_idle"
    assert attachment["script"] == SCRIPT_NAME
    assert attachment["process_status"] == "retired_flat_preserved_20260818"
    assert role["account_id"] == ACCOUNT_ID
    assert role["script"] == SCRIPT_NAME
    assert role["execution"] == "retired_flat_preserved"
    assert "last_deployed_source_sha256" in attachment

    supervisor = (TRAD_ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8-sig"
    )
    assert '-Name "practice_002_no_gpt_movement_ledger"' not in supervisor
    assert "Retired from always-on runtime on 2026-08-18" in supervisor


def test_gpt_v5_24_orders_have_checkpoint_specific_provenance() -> None:
    source = (TRAD_ROOT / SCRIPT_NAME).read_text(encoding="utf-8-sig")

    assert "gptfxv5_24_ledger_" in source
    assert "oanda_forex_gpt_advisor_v5_24_movement_ledger" in source
    assert "gptfxv5_17_" not in source
    assert '"comment": "oanda_forex_gpt_manager_v5_17"' not in source


def test_gpt_v5_24_includes_all_pair_news_context_and_movement_links() -> None:
    source = (TRAD_ROOT / SCRIPT_NAME).read_text(encoding="utf-8-sig")

    assert "import oanda_news_event_tagger as news_event_tagger" in source
    assert "news_event_tagger.ensure_catalog_current(" in source
    assert '"news_context": pair_news_context.get(' in source
    assert '"news_event_context": {' in source
    assert "news_event_tagger.append_live_movement_links(" in source
    assert "News tags are evidence only and never authorize an order." in source
