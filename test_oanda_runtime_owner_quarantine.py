import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
POLICY = ROOT / "config" / "runtime_owner_quarantine_v1.json"
SUPERVISOR = ROOT / "oanda_always_on_supervisor.ps1"


def test_quarantined_launchers_and_manual_mutators_are_not_supervised():
    payload = json.loads(POLICY.read_text(encoding="utf-8"))
    supervisor = SUPERVISOR.read_text(encoding="utf-8")

    assert payload["canonical_runtime_owner"] == SUPERVISOR.name
    assert payload["policy"]["practice_only"] is True
    assert payload["policy"]["real_money_enabled"] is False
    for name in [
        *payload["retired_launch_surfaces"],
        *payload["restricted_manual_mutation_tools"],
    ]:
        assert (ROOT / name).is_file()
        assert name not in supervisor
