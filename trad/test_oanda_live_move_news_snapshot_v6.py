from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from trad import oanda_live_move_news_snapshot_v6 as v6
from trad import oanda_live_move_news_outcomes_v2 as outcomes_v2
from trad.oanda_continuous_narrative_meter_v12 import (
    METER_CONTRACT_ID,
    initialize_database,
)


def _model_scores(score: float) -> str:
    return json.dumps(
        {
            "published_semantic_v1": 0.0,
            "research_semantic_v1": 0.0,
            "secondary_directional_discovery_v1": 0.0,
            "recovered_blurb_analog_v1": 0.0,
            "linguistic_tone_v1": 0.0,
            "source_balanced_v1": 0.0,
            "narrative_acceleration_v1": score,
        },
        sort_keys=True,
    )


def _insert_currency(
    connection: sqlite3.Connection,
    *,
    clock: str,
    currency: str,
    score: float,
    created: str,
) -> None:
    connection.execute(
        """INSERT INTO currency_meter VALUES(
               ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
           )""",
        (
            METER_CONTRACT_ID,
            clock,
            currency,
            _model_scores(score),
            0.3,
            0.2,
            1,
            2,
            1,
            0.8,
            0.5,
            1,
            1,
            '["official"]',
            '["story"]',
            '["classifier"]',
            "trusted_clock_subset",
            created,
        ),
    )


def meter_fixture(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        initialize_database(connection)
        connection.execute(
            "INSERT INTO meter_contract_registry VALUES(?,?,?,?,?,?,?,?)",
            (
                METER_CONTRACT_ID,
                "2026-08-27T07:25:00+00:00",
                5,
                60,
                "clock_utc_is_closed_bucket_end; rows_seal_only_after_close_plus_grace",
                1,
                0,
                "2026-08-27T07:26:00+00:00",
            ),
        )
        for clock, sealed, aud_score in (
            (
                "2026-08-27T07:30:00+00:00",
                "2026-08-27T07:31:00+00:00",
                0.30,
            ),
            (
                "2026-08-27T07:35:00+00:00",
                "2026-08-27T07:36:00.265424+00:00",
                -0.40,
            ),
        ):
            _insert_currency(
                connection,
                clock=clock,
                currency="AUD",
                score=aud_score,
                created=sealed,
            )
            _insert_currency(
                connection,
                clock=clock,
                currency="USD",
                score=0.0,
                created=sealed,
            )
            connection.execute(
                "INSERT INTO bucket_seals VALUES(?,?,?,?,?,?,?,?)",
                (METER_CONTRACT_ID, clock, sealed, 60, 21, 1, 1, 0),
            )
        connection.commit()


def test_causal_pair_join_uses_latest_state_sealed_by_move_start(tmp_path: Path) -> None:
    database = tmp_path / "meter.sqlite"
    meter_fixture(database)
    with v6._read_only_database(database) as connection:
        state, provenance = v6.causal_pair_state(
            connection,
            "AUD_USD",
            move_start_epoch=int(
                datetime(2026, 8, 27, 7, 34, tzinfo=timezone.utc).timestamp()
            ),
        )
    assert state["direction"] == "LONG"
    assert state["score"] == pytest.approx(0.30)
    assert provenance["meter_clock_utc"] == "2026-08-27T07:30:00+00:00"
    assert provenance["meter_sealed_at_utc"] == "2026-08-27T07:31:00+00:00"
    assert provenance["partial_live_excluded"] is True


def test_bucket_close_alone_is_not_enough_until_seal_is_available(tmp_path: Path) -> None:
    database = tmp_path / "meter.sqlite"
    meter_fixture(database)
    with v6._read_only_database(database) as connection:
        before_seal, before_provenance = v6.causal_pair_state(
            connection,
            "AUD_USD",
            move_start_epoch=int(
                datetime(2026, 8, 27, 7, 35, 30, tzinfo=timezone.utc).timestamp()
            ),
        )
        after_seal, after_provenance = v6.causal_pair_state(
            connection,
            "AUD_USD",
            move_start_epoch=int(
                datetime(2026, 8, 27, 7, 36, 1, tzinfo=timezone.utc).timestamp()
            ),
        )
        exact_second, exact_provenance = v6.causal_pair_state(
            connection,
            "AUD_USD",
            move_start_epoch=int(
                datetime(2026, 8, 27, 7, 36, 0, tzinfo=timezone.utc).timestamp()
            ),
        )
    assert before_seal["direction"] == "LONG"
    assert before_provenance["meter_clock_utc"].endswith("07:30:00+00:00")
    assert after_seal["direction"] == "SHORT"
    assert after_provenance["meter_clock_utc"].endswith("07:35:00+00:00")
    assert exact_second["direction"] == "LONG"
    assert exact_provenance["meter_clock_utc"].endswith("07:30:00+00:00")


def test_no_causal_seal_is_explicitly_unavailable_not_partial_live(tmp_path: Path) -> None:
    database = tmp_path / "meter.sqlite"
    meter_fixture(database)
    with v6._read_only_database(database) as connection:
        state, provenance = v6.causal_pair_state(
            connection,
            "AUD_USD",
            move_start_epoch=int(
                datetime(2026, 8, 27, 7, 29, tzinfo=timezone.utc).timestamp()
            ),
        )
    assert state["state_available"] is False
    assert state["direction"] == "NEUTRAL"
    assert provenance["meter_clock_utc"] is None
    assert provenance["meter_state_kind"] == "unavailable"
    assert provenance["partial_live_excluded"] is True


def test_v6_case_history_is_separate_append_only_and_contract_bound(tmp_path: Path) -> None:
    history = tmp_path / "cases.sqlite"
    meter = tmp_path / "meter.sqlite"
    meter_fixture(meter)
    row = {
        "instrument": "AUD_USD",
        "start_utc": "2026-08-27T07:34:00+00:00",
        "end_utc": "2026-08-27T07:40:00+00:00",
        "move_direction": "up",
        "contract_id": v6.CONTRACT_ID,
    }
    result = v6.record_cases(
        history,
        [row],
        recorded_utc="2026-08-27T07:50:00+00:00",
        meter_database=meter,
    )
    assert result == {"inserted": 1, "total": 1}
    with sqlite3.connect(history) as connection:
        registry = connection.execute(
            "SELECT meter_contract_id,partial_live_excluded,research_only,execution_eligible "
            "FROM mover_case_contract_registry WHERE contract_id=?",
            (v6.CONTRACT_ID,),
        ).fetchone()
        assert registry == (METER_CONTRACT_ID, 1, 1, 0)
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute("DELETE FROM mover_cases")
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute("UPDATE mover_case_contract_registry SET research_only=0")


def test_v2_outcomes_bind_only_the_v6_case_contract(tmp_path: Path) -> None:
    history = tmp_path / "cases.sqlite"
    meter = tmp_path / "meter.sqlite"
    candle_root = tmp_path / "candles"
    output = tmp_path / "outcomes.json"
    report = tmp_path / "outcomes.md"
    candle_root.mkdir()
    meter_fixture(meter)
    v6.record_cases(
        history,
        [
            {
                "instrument": "AUD_USD",
                "start_utc": "2026-08-27T07:34:00+00:00",
                "end_utc": "2026-08-27T07:40:00+00:00",
                "move_direction": "up",
                "contract_id": v6.CONTRACT_ID,
                "entry_quote_fresh": False,
            }
        ],
        recorded_utc="2026-08-27T07:50:00+00:00",
        meter_database=meter,
    )
    result = outcomes_v2.run(
        database=history,
        candle_root=candle_root,
        output=output,
        report=report,
    )
    assert result["contract_id"] == outcomes_v2.CONTRACT_ID
    assert result["case_contract_id"] == v6.CONTRACT_ID
    assert result["case_count"] == 1
    assert result["retained_outcome_count"] == 0
    assert result["execution_eligible"] is False


def test_supervisor_and_consumers_bind_v7r3_without_reusing_v6r2_files() -> None:
    root = Path(v6.__file__).resolve().parent
    supervisor = (root / "oanda_always_on_supervisor.ps1").read_text(encoding="utf-8")
    dashboard = (root / "oanda_practice_live_dashboard.py").read_text(encoding="utf-8")
    dashboard_html = (root / "oanda_main_signal_dashboard.html").read_text(encoding="utf-8")
    integrity = (root / "oanda_project_integrity_audit.py").read_text(encoding="utf-8")
    assert '"oanda_live_move_news_snapshot_v7r3.py"' in supervisor
    assert '"--narrative-database"' in supervisor
    assert '"continuous_narrative_meter_v12.sqlite"' in supervisor
    assert '"live_move_news_snapshot_v7r3.json"' in supervisor
    assert '"oanda_live_move_news_outcomes_v4.py"' in supervisor
    assert '"live_move_news_outcomes_v4r3.json"' in supervisor
    assert '"oanda_live_move_persistent_news_context_v5.py"' in supervisor
    assert '"live_move_news_snapshot_v7r3.json"' in dashboard
    assert '"live_move_news_outcomes_v4r3.json"' in dashboard
    assert "narrative_meter_clock_utc" in dashboard
    assert "partial excluded" in dashboard_html
    assert '"live_move_news_snapshot_v7r3.json"' in integrity
    assert '"live_move_news_cases_v7r3.sqlite"' in integrity
    assert '"live_move_persistent_news_context_v5r3.json"' in integrity
    assert '"live_move_news_snapshot_v6r2.json"' not in dashboard
    assert '"live_move_news_outcomes_v2r2.json"' not in dashboard
    assert v6.DEFAULT_HISTORY.name != "live_move_news_cases_v1.sqlite"


def test_v6_has_no_broker_or_authorization_import_surface() -> None:
    source = Path(v6.__file__).read_text(encoding="utf-8").lower()
    assert "requests." not in source
    assert "oanda-api-v20" not in source
    assert "authorization" not in "\n".join(
        line for line in source.splitlines() if line.startswith("import ") or line.startswith("from ")
    )
