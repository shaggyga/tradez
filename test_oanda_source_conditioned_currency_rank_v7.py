import datetime as dt
import json
import sqlite3

import oanda_causal_source_factor_response_map_v8 as source_v8
import oanda_source_conditioned_currency_rank_v7 as subject


UTC = dt.timezone.utc


def test_v7_rank_is_clean_v8_no_trade_contract():
    assert subject.REQUIRED_SOURCE_CONTRACT_ID == source_v8.CONTRACT_ID
    assert subject.PARENT_CONTRACT_ID.endswith(
        "v7_input_explicit_no_trade_20260901"
    )
    assert subject.INPUT_MODE.endswith("no_v1_v2_v3_v4_v5_v6_v7_fallback")
    assert subject.DEFAULT_MANIFEST.exists()
    assert subject.resolve_source_database() == source_v8.OUTPUT_DATABASE.resolve()


def test_v7_rank_rejects_foreign_source_contract(tmp_path):
    path = tmp_path / "foreign.sqlite"
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE source_factor_forecast (
              forecast_id TEXT PRIMARY KEY,
              factor_observation_id TEXT,
              canonical_event_id TEXT,
              currency TEXT,
              factor_key TEXT,
              horizon_min INTEGER,
              issued_utc TEXT,
              training_cutoff_utc TEXT,
              forecast_state TEXT,
              abstain_reason TEXT,
              backoff_level TEXT,
              backoff_key TEXT,
              raw_n INTEGER,
              effective_event_n INTEGER,
              probability_strengthening REAL,
              predicted_currency_factor_bps REAL,
              predicted_absolute_factor_bps REAL,
              training_latest_maturity_utc TEXT,
              forecast_payload_json TEXT,
              evidence_class TEXT,
              prospective_proof_eligible INTEGER,
              research_only INTEGER,
              execution_eligible INTEGER,
              can_authorize INTEGER,
              can_promote INTEGER,
              contract_id TEXT,
              cohort_id TEXT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO source_factor_forecast VALUES (
              'f1','o1','e1','JPY','topic:test',60,
              '2026-09-01T11:46:00+00:00','2026-09-01T11:46:00+00:00',
              'abstain','low_effective_n:1<8','none','',1,1,NULL,NULL,NULL,'',
              '{}','prospective_v8',1,1,0,0,0,'foreign_contract','foreign_cohort'
            )
            """
        )
        connection.commit()
    finally:
        connection.close()
    try:
        subject.source_forecast_inventory(
            path,
            cutoff_utc=dt.datetime(2026, 9, 1, 12, tzinfo=UTC),
        )
    except ValueError as exc:
        assert "contamination" in str(exc)
    else:
        raise AssertionError("foreign V8 contract was accepted")


def test_v7_rank_empty_v8_inventory_is_explicit(tmp_path):
    path = tmp_path / "v8.sqlite"
    connection = source_v8.open_output_database(path)
    connection.close()
    inventory = subject.source_forecast_inventory(
        path,
        cutoff_utc=dt.datetime(2026, 9, 1, 12, tzinfo=UTC),
    )
    assert inventory["status"] == "no_rows_at_cutoff"
    assert inventory["total_rows"] == 0
    assert inventory["rank_eligible_rows"] == 0
    assert inventory["abstain_rows"] == 0


def test_supervisor_preserves_retired_source_conditioned_currency_rank_diagnostics():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(encoding="utf-8")
    for version in [6, 7]:
        name = f"source_conditioned_currency_rank_v{version}"
        assert f'-Name "{name}"' not in supervisor
        assert f'-Name "{name}_preserved"' in supervisor
        assert f'-Needle "oanda_{name}.py"' in supervisor


def test_v7_never_publishes_inherited_intermediate_contract(
    tmp_path, monkeypatch
):
    source = tmp_path / "source.sqlite"
    source.touch()
    public_state = tmp_path / "rank_v7.json"
    public_report = tmp_path / "rank_v7.md"
    inherited_paths = {}

    def fake_v6_run_cycle(**kwargs):
        inherited_paths["state"] = kwargs["state_path"]
        inherited_paths["report"] = kwargs["report_path"]
        assert kwargs["state_path"] != public_state
        assert kwargs["report_path"] != public_report
        assert not public_state.exists()
        assert not public_report.exists()
        kwargs["state_path"].write_text(
            json.dumps({"contract_id": "intermediate_v6"}),
            encoding="utf-8",
        )
        kwargs["report_path"].write_text(
            "intermediate v6", encoding="utf-8"
        )
        return {
            "contract_id": subject.CONTRACT_ID,
            "source_database": str(source),
            "policy": {},
        }

    monkeypatch.setattr(subject.v6, "run_cycle", fake_v6_run_cycle)
    monkeypatch.setattr(
        subject, "_render_report_v7", lambda snapshot: "complete v7\n"
    )

    snapshot = subject.run_cycle(
        source_database=source,
        state_path=public_state,
        report_path=public_report,
    )

    assert snapshot["required_source_contract_id"] == (
        subject.REQUIRED_SOURCE_CONTRACT_ID
    )
    assert json.loads(public_state.read_text(encoding="utf-8"))[
        "required_source_contract_id"
    ] == subject.REQUIRED_SOURCE_CONTRACT_ID
    assert public_report.read_text(encoding="utf-8") == "complete v7\n"
    assert not inherited_paths["state"].exists()
    assert not inherited_paths["report"].exists()
