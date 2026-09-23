from __future__ import annotations

import ast
import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3

import oanda_executable_move_census_v2 as subject
import oanda_executable_move_census_v2d_verifier as d_verifier
import oanda_executable_move_census_v2e_verifier as verifier


UTC = dt.timezone.utc


def _freeze_capture_clock(monkeypatch, observed: dt.datetime) -> None:
    original = dt.datetime

    class FrozenDateTime(original):
        @classmethod
        def now(cls, tz=None):
            return observed if tz is not None else observed.replace(tzinfo=None)

    monkeypatch.setattr(subject.dt, "datetime", FrozenDateTime)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _quote_payload(config: dict, observed: dt.datetime) -> dict:
    quotes = {
        instrument: {
            "bid": 1.0,
            "ask": 1.0 + pip,
            "pip": pip,
            "source": "stream",
            "time": observed.isoformat().replace("+00:00", "Z"),
            "tradeable": True,
        }
        for instrument, pip in config["instruments"].items()
    }
    return {
        "schema_version": int(config["source_schema_version"]),
        "generated_utc": observed.isoformat(),
        "producer": "practice_007_fast_executor_price_stream",
        "quote_count": 68,
        "quotes": quotes,
        "research_only": True,
    }


def test_source_contract_mismatch_fails_before_activation_or_database_creation(
    tmp_path,
):
    config = json.loads(verifier.CONFIG_PATH.read_text(encoding="utf-8"))
    observed = verifier.ACTIVATION_UTC - dt.timedelta(minutes=1)
    payload = _quote_payload(config, observed)
    payload["schema_version"] = int(config["source_schema_version"]) + 1
    quotes = tmp_path / "quotes.json"
    quotes.write_text(json.dumps(payload), encoding="utf-8")
    database = tmp_path / "census.sqlite"
    try:
        subject.capture_once(
            config_path=verifier.CONFIG_PATH,
            quotes_path=quotes,
            database_path=database,
            now=observed,
        )
    except RuntimeError as exc:
        assert str(exc).startswith("source_contract_mismatch:")
    else:
        raise AssertionError("incompatible source contract was accepted")
    assert not database.exists()


def test_frozen_v2e_hashes_and_contract_are_literal():
    assert _sha(verifier.CONFIG_PATH) == verifier.FROZEN_CONFIG_SHA256
    assert _sha(Path(subject.__file__)) == verifier.FROZEN_PRODUCER_SHA256
    assert verifier.COHORT_ID == "all68_executable_move_census_v2_20260901e"
    assert verifier.CAPTURE_CONTRACT_ID.endswith("cold_work_separated")
    assert verifier.CAPTURE_COHORT_ID.endswith("20260901e")
    assert verifier.SOURCE_SCHEMA_VERSION == "3"
    assert "prior.main()" not in Path(verifier.__file__).read_text(encoding="utf-8")


def test_cohort_e_has_new_lineage_and_imports_no_cohort_d_rows():
    config = json.loads(verifier.CONFIG_PATH.read_text(encoding="utf-8"))
    assert config["parent_cohort_id"] == (
        "all68_executable_move_census_v2_20260901d"
    )
    assert config["parent_evidence_status"] == "permanently_invalid"
    assert config["historical_row_import_count"] == 0
    assert config["no_backfill"] is True


def test_cohort_d_source_schema_mismatch_is_preserved_and_e_corrects_it():
    d_config = json.loads(d_verifier.CONFIG_PATH.read_text(encoding="utf-8"))
    e_config = json.loads(verifier.CONFIG_PATH.read_text(encoding="utf-8"))
    assert d_config["source_schema_version"] == "2"
    assert e_config["source_schema_version"] == "3"
    assert e_config["parent_cohort_id"] == d_config["cohort_id"]


def test_capture_and_evaluator_have_disjoint_hot_and_cold_calls():
    tree = ast.parse(Path(subject.__file__).read_text(encoding="utf-8"))
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }

    def calls(name):
        return {
            node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
            for node in ast.walk(functions[name])
            if isinstance(node, ast.Call)
            and isinstance(node.func, (ast.Attribute, ast.Name))
        }

    capture_calls = calls("capture_once")
    evaluator_calls = calls("evaluate_once")
    assert "insert_frame" in capture_calls
    assert "reconcile_window_evaluations" not in capture_calls
    assert "finalize_factor_episodes" not in capture_calls
    assert "build_latest" not in capture_calls
    assert "read_bytes" not in evaluator_calls
    assert "insert_frame" not in evaluator_calls
    assert "reconcile_window_evaluations" in evaluator_calls
    assert "build_latest" in evaluator_calls


def test_capture_error_remains_visible_until_a_successful_commit():
    source = Path(subject.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    capture_loop = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "capture_loop"
    )
    waiting_heartbeat_calls = [
        node for node in ast.walk(capture_loop)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "heartbeat"
        and any(
            keyword.arg == "error"
            and isinstance(keyword.value, ast.Name)
            and keyword.value.id == "last_error"
            for keyword in node.keywords
        )
    ]
    assert waiting_heartbeat_calls
    assert "awaiting_absolute_minute_boundary_after_error" in source


def test_verifier_retries_only_the_bounded_latest_refresh_race():
    assert verifier.latest_refresh_race_only({
        "failures": [
            "evaluations:scheduled_key_coverage",
            "latest:frame_count:mismatch",
            "latest:latest_frame_utc:mismatch",
            "latest:review_queue:mismatch",
        ]
    }) is True
    assert verifier.latest_refresh_race_only({
        "failures": ["schedule:missing_open_minute:2026-09-01T02:30:00Z"]
    }) is False
    assert verifier.latest_refresh_race_only({"failures": []}) is False


def test_semantic_verifier_rejects_structurally_consistent_all_invalid_frames(
    tmp_path,
):
    database = tmp_path / "all_invalid.sqlite"
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "CREATE TABLE frames (market_open INTEGER, valid_count INTEGER)"
        )
        connection.execute(
            "CREATE TABLE frame_quotes (state TEXT, reason TEXT)"
        )
        connection.execute("INSERT INTO frames VALUES (1, 0)")
        connection.executemany(
            "INSERT INTO frame_quotes VALUES ('invalid', ?)",
            [("source_identity_mismatch",)] * 68,
        )
        connection.commit()
    finally:
        connection.close()
    result = verifier.semantic_validity_diagnostics(database)
    assert result == {
        "valid_quote_rows": 0,
        "invalid_quote_rows": 68,
        "source_identity_mismatch_rows": 68,
        "zero_valid_open_market_frames": 1,
    }


def test_one_frame_round_trip_verifies_without_backfill(tmp_path, monkeypatch):
    config = json.loads(verifier.CONFIG_PATH.read_text(encoding="utf-8"))
    observed = verifier.ACTIVATION_UTC + dt.timedelta(seconds=1)
    _freeze_capture_clock(monkeypatch, observed)
    quotes = tmp_path / "quotes.json"
    quotes.write_text(
        json.dumps(_quote_payload(config, observed), sort_keys=True),
        encoding="utf-8",
    )
    database = tmp_path / "census.sqlite"
    latest = tmp_path / "latest.json"
    captured = subject.capture_once(
        config_path=verifier.CONFIG_PATH,
        quotes_path=quotes,
        database_path=database,
        now=observed,
    )
    assert captured["frame_inserted"] is True
    assert captured["scheduled_utc"] == subject.base.iso(
        observed.replace(second=0, microsecond=0)
    )
    assert captured["commit_before_cold_work"] is True
    connection = sqlite3.connect(database)
    try:
        assert connection.execute(
            "SELECT valid_count, invalid_count FROM frames"
        ).fetchone() == (68, 0)
    finally:
        connection.close()

    payload = subject.evaluate_once(
        config_path=verifier.CONFIG_PATH,
        database_path=database,
        output_path=latest,
        now=observed + dt.timedelta(seconds=10),
    )
    assert payload["frame_count"] == 1
    assert payload["schedule_census"]["missing_open_frames"] == 0
    assert payload["frame_inserted"] is False

    result = verifier.verify(
        database_path=database,
        config_path=verifier.CONFIG_PATH,
        latest_path=latest,
        producer_path=Path(subject.__file__),
        now=observed + dt.timedelta(seconds=20),
    )
    assert result["verified"] is True
    assert result["failure_count"] == 0
    assert result["counts"]["frames"] == 1
    assert result["counts"]["frame_quotes"] == 68
    assert result["semantic_validity"] == {
        "valid_quote_rows": 68,
        "invalid_quote_rows": 0,
        "source_identity_mismatch_rows": 0,
        "zero_valid_open_market_frames": 0,
    }


def test_capture_database_is_append_only_and_idempotent(tmp_path, monkeypatch):
    config = json.loads(verifier.CONFIG_PATH.read_text(encoding="utf-8"))
    observed = verifier.ACTIVATION_UTC + dt.timedelta(seconds=1)
    _freeze_capture_clock(monkeypatch, observed)
    quotes = tmp_path / "quotes.json"
    quotes.write_text(json.dumps(_quote_payload(config, observed)), encoding="utf-8")
    database = tmp_path / "census.sqlite"
    first = subject.capture_once(
        config_path=verifier.CONFIG_PATH,
        quotes_path=quotes,
        database_path=database,
        now=observed,
    )
    second = subject.capture_once(
        config_path=verifier.CONFIG_PATH,
        quotes_path=quotes,
        database_path=database,
        now=observed,
    )
    assert first["frame_inserted"] is True
    assert second["frame_inserted"] is False
    connection = sqlite3.connect(database)
    try:
        assert connection.execute("SELECT COUNT(*) FROM frames").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM frame_quotes").fetchone()[0] == 68
    finally:
        connection.close()


def test_v2_has_no_order_or_authorization_surface():
    tree = ast.parse(Path(subject.__file__).read_text(encoding="utf-8"))
    imports = []
    calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
        elif isinstance(node, ast.Call):
            calls.append(
                node.func.attr if isinstance(node.func, ast.Attribute) else
                node.func.id if isinstance(node.func, ast.Name) else ""
            )
    forbidden = ("requests", "httpx", "oandapyv20", "broker", "executor")
    assert not any(any(word in name.lower() for word in forbidden) for name in imports)
    assert not {name.lower() for name in calls} & {
        "create_order", "place_order", "submit_order", "authorize", "promote"
    }
