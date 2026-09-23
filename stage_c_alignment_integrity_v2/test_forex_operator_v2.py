import json
from pathlib import Path
import subprocess
import sys
import pytest

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from forex_operator_v2 import approved_recipe, operate, sha
from reference_accounting_adapter_v2 import DEFAULT_TRAD


@pytest.fixture
def recipe(tmp_path):
    path = tmp_path / "recipe.json"
    path.write_text(json.dumps(approved_recipe(DEFAULT_TRAD)))
    return path, sha(path)


def test_operator_runs_verifies_without_republishing_and_detects_corruption(recipe, tmp_path):
    path, digest = recipe
    runs = tmp_path / "runs"
    assert operate("status", path, digest, runs, DEFAULT_TRAD)["status"] == "ready"
    assert operate("run", path, digest, runs, DEFAULT_TRAD)["status"] == "completed_verified"
    before = {str(p): p.read_bytes() for p in runs.rglob("COMPLETION_MANIFEST.json")}
    assert operate("resume", path, digest, runs, DEFAULT_TRAD)["status"] == "completed_verified"
    assert before == {str(p): p.read_bytes() for p in runs.rglob("COMPLETION_MANIFEST.json")}
    (runs / "operator-accounting-reference" / "final_state.json").write_text("{}")
    assert operate("verify", path, digest, runs, DEFAULT_TRAD)["status"] == "review_required"


def test_operator_refuses_hash_and_source_contract_drift_before_run(recipe, tmp_path, monkeypatch):
    path, digest = recipe
    runs = tmp_path / "runs"
    assert operate("run", path, "0" * 64, runs, DEFAULT_TRAD)["status"] == "review_required"
    content = json.loads(path.read_text())
    content["sources"]["accounting_events_v2.py"] = "0" * 64
    path.write_text(json.dumps(content))
    assert operate("run", path, sha(path), runs, DEFAULT_TRAD)["status"] == "review_required"
    assert not runs.exists()


def test_operator_resume_actual_interruption_and_missing_completion(recipe, tmp_path):
    path, digest = recipe
    runs = tmp_path / "runs"
    assert operate("verify", path, digest, runs, DEFAULT_TRAD)["status"] == "review_required"
    crash = subprocess.run([sys.executable, "-I", "-B", str(ROOT / "accounting_event_runner_v2.py"),
        "--run-id", "operator-accounting-reference", "--runs-dir", str(runs), "--test-crash-after-event", "8"], capture_output=True)
    assert crash.returncode == 91
    assert operate("status", path, digest, runs, DEFAULT_TRAD)["status"] == "resumable"
    assert operate("resume", path, digest, runs, DEFAULT_TRAD)["status"] == "completed_verified"
