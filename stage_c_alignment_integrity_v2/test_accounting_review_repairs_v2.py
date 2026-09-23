"""Regression coverage for checkpoint review B05-R1/R2/R3."""
from decimal import Decimal
import json
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile
import hashlib

import pytest

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from accounting_events_v2 import EventLedger, event_contract
from accounting_fastpath_v2 import OptimizedEventLedger
from accounting_event_fixtures_v2 import event
from reference_accounting_adapter_v2 import DEFAULT_TRAD, ABSENT_IMPORT_PATHS
from accounting_checkpoint_v2 import PREDECESSORS, SOURCE_ALLOWLIST, export_checkpoint, restore_checkpoint


def race(resting=False, early_ack=False):
    rows = [event("intent", 1000, 0, arm="hold", order_id="race", instrument="EUR_USD",
                  action="open", side=1, notional_usd="1100.2", target_epoch=5000,
                  **({"order_type": "stop", "trigger_price": "1.1001"} if resting else {})),
            event("cancel_request", 1030, 0, arm="hold", order_id="race")]
    if early_ack:
        rows.append(event("cancel_ack", 1040, 0, arm="hold", order_id="race"))
    rows.append(event("activate", 1060, 0, arm="hold", order_id="race"))
    if resting:
        rows.append(event("trigger", 1061, 0, arm="hold", order_id="race", trigger_evidence_id="quote"))
    rows.append(event("fill", 1062, 0, arm="hold", order_id="race", fill_id="partial", units=400,
                      filled_epoch=1062, fee_usd="0", execution_evidence_id="explicit-fill"))
    rows.append(event("cancel_ack", 1070, 0, arm="hold", order_id="race"))
    return rows


@pytest.mark.parametrize("engine", [EventLedger, OptimizedEventLedger])
@pytest.mark.parametrize("resting", [False, True])
@pytest.mark.parametrize("early_ack", [False, True])
def test_preactivation_cancel_requires_ack_to_stop_fill(engine, resting, early_ack):
    book = engine(event_contract())
    rows = book.replay(race(resting, early_ack))
    fill = rows[-2]
    if early_ack:
        assert fill["receipt"]["status"] == "rejected"
        assert not book.state["arms"]["hold"]["lots"]
    else:
        assert fill["receipt"]["status"] != "rejected"
        assert fill["arms"]["hold"]["used_margin_usd"] == Decimal("44.008")
        assert fill["arms"]["hold"]["reserved_margin_usd"] == Decimal("66.012")
        assert rows[-1]["arms"]["hold"]["used_margin_usd"] == Decimal("44.008")
    assert rows[-1]["arms"]["hold"]["reserved_margin_usd"] == 0


@pytest.mark.parametrize("engine", ["reference", "optimized"])
def test_cancel_activation_race_process_death_resume(engine, tmp_path):
    fixture = tmp_path / "input.json"
    fixture.write_text(json.dumps({"contract": event_contract(), "events": race(True)}))
    base = [sys.executable, "-I", "-B", str(ROOT / "accounting_event_runner_v2.py"),
            "--runs-dir", str(tmp_path), "--trad-root", str(DEFAULT_TRAD), "--engine", engine,
            "--input", str(fixture)]
    for name, extra, code in [("clean", [], 0), ("crash", ["--test-crash-after-event", "3"], 91),
                              ("crash", ["--resume"], 0)]:
        result = subprocess.run(base + ["--run-id", name] + extra, capture_output=True, text=True, timeout=30)
        assert result.returncode == code, result.stderr
    manifest = json.loads((tmp_path / "clean" / "COMPLETION_MANIFEST.json").read_text())
    for item in manifest["payloads"]:
        assert (tmp_path / "clean" / item["path"]).read_bytes() == (tmp_path / "crash" / item["path"]).read_bytes()


@pytest.mark.parametrize("path", ["src/__init__.py", "src/forex_system/__init__.py", "src/forex_system/research/__init__.py"])
def test_unreviewed_initializer_never_executes(path, tmp_path):
    trad = tmp_path / "trad"
    for name in PREDECESSORS:
        target = trad / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(DEFAULT_TRAD / name, target)
    marker = tmp_path / "executed"
    (trad / path).write_text("from pathlib import Path\nPath(" + repr(str(marker)) + ").touch()\n")
    code = "import sys; sys.path.insert(0,sys.argv[1]); from reference_accounting_adapter_v2 import load_reference; from pathlib import Path; load_reference(Path(sys.argv[2]))"
    result = subprocess.run([sys.executable, "-I", "-B", "-c", code, str(ROOT), str(trad)], capture_output=True, text=True)
    assert result.returncode != 0
    assert "unreviewed_reference_dependency_refused_before_import" in result.stderr
    assert not marker.exists()
    from publication import sha256_file
    result = subprocess.run([sys.executable, "-I", "-B", str(ROOT / "forex_operator_v2.py"), "status",
        "--recipe", str(ROOT / "OPERATOR_RECIPE.json"), "--recipe-sha256", sha256_file(ROOT / "OPERATOR_RECIPE.json"),
        "--trad-root", str(trad), "--runs-dir", str(tmp_path / "runs")], capture_output=True, text=True)
    assert result.returncode == 2
    assert json.loads(result.stdout)["status"] == "review_required"
    assert not marker.exists()


@pytest.mark.parametrize("cached", [False, True])
def test_foreign_namespace_search_root_cannot_execute(cached, tmp_path):
    foreign = tmp_path / "foreign"
    (foreign / "src").mkdir(parents=True)
    marker = tmp_path / "executed"
    (foreign / "src" / "__init__.py").write_text("from pathlib import Path\nPath(" + repr(str(marker)) + ").touch()\n")
    code = "import sys,types; from pathlib import Path; sys.path[:0]=[sys.argv[1],sys.argv[3]]; from reference_accounting_adapter_v2 import load_reference; "
    if cached:
        code += "m=types.ModuleType('src'); m.__path__=[str(Path(sys.argv[3])/'src')]; sys.modules['src']=m; "
    code += "load_reference(Path(sys.argv[2]))"
    result = subprocess.run([sys.executable, "-I", "-B", "-c", code, str(ROOT), str(DEFAULT_TRAD), str(foreign)], capture_output=True, text=True)
    assert result.returncode == (1 if cached else 0), result.stderr
    if cached:
        assert "cached_reference_package_from_unexpected_root" in result.stderr
    assert not marker.exists()


def test_restore_rejects_stale_approval_even_with_consistent_inventory(tmp_path):
    package = tmp_path / "valid.zip"
    export_checkpoint(package)
    with zipfile.ZipFile(package) as archive:
        members = {name: archive.read(name) for name in archive.namelist()}
    name = "source/accounting_event_fixtures_v2.py"
    members[name] += b"\n# stale packaged approval\n"
    manifest = json.loads(members["CHECKPOINT_MANIFEST.json"])
    for item in manifest["members"]:
        if item["path"] == name:
            item.update(bytes=len(members[name]), sha256=hashlib.sha256(members[name]).hexdigest())
    members["CHECKPOINT_MANIFEST.json"] = json.dumps(manifest).encode()
    altered = tmp_path / "stale.zip"
    with zipfile.ZipFile(altered, "x") as archive:
        for name, raw in members.items():
            archive.writestr(name, raw)
    with pytest.raises(ValueError, match="packaged_operator_acceptance_failed:status"):
        restore_checkpoint(altered, tmp_path / "refused", hashlib.sha256(altered.read_bytes()).hexdigest())
    assert not (tmp_path / "refused" / "RESTORE_RECEIPT.json").exists()


def test_export_rejects_stale_frozen_operator_approval(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    for name in SOURCE_ALLOWLIST:
        shutil.copyfile(ROOT / name, source / name)
    with (source / "accounting_event_fixtures_v2.py").open("a") as handle:
        handle.write("\n# Simulate code changed after recipe approval.\n")
    with pytest.raises(ValueError, match="packaged_operator_acceptance_failed:status"):
        export_checkpoint(tmp_path / "invalid.zip", source=source)
    assert not (tmp_path / "invalid.zip").exists()
