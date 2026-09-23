import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = ROOT / "oanda_joint_price_news_isolation_status_v1.py"


def test_joint_news_isolation_status_publishes_non_failing_fresh_heartbeat(tmp_path):
    source = tmp_path / "old" / "heartbeat.json"
    source.parent.mkdir()
    source.write_text(json.dumps({
        "schema_version": "joint_price_news_forecast_heartbeat_v9_20260916",
        "worker": "joint_price_news_forecast_study_v9",
        "status": "failed",
        "phase": "research_collection",
        "last_error": "news_bootstrap:CaptureReadTimeout:packed_capture_retries_exhausted",
        "errors": ["old failure"],
        "shared_history_prepared": False,
        "pairs_with_forecast": 0,
    }), encoding="utf-8")
    output = tmp_path / "status" / "heartbeat.json"
    result = subprocess.run([
        "python", str(PY), "--once", "--output", str(output), "--source-heartbeat", str(source),
        "--reason", "bounded_validation_capture_blocker",
    ], cwd=ROOT, text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, result.stderr
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "joint_price_news_isolation_status_v1_20260916"
    assert payload["worker"] == "joint_price_news_isolation_status_v1"
    assert payload["status"] == "isolated"
    assert payload["phase"] == "research_collection_isolated"
    assert payload["last_error"] == ""
    assert payload["errors"] == []
    assert payload["reported_failure"] is False
    assert payload["can_place_orders"] is False
    assert payload["can_promote"] is False
    assert payload["source_status"] == "failed"
    assert "packed_capture_retries_exhausted" in payload["source_last_error"]
