import datetime as dt
import json
from pathlib import Path
import shutil
import sqlite3
import sys

import pytest

import oanda_executable_move_census_v1_verifier as verifier


UTC = dt.timezone.utc


DDL = """
CREATE TABLE cohort_manifest(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), cohort_id TEXT NOT NULL,
 contract_id TEXT NOT NULL, activation_utc TEXT NOT NULL, universe_json TEXT NOT NULL,
 universe_sha256 TEXT NOT NULL, pip_map_json TEXT NOT NULL, pip_map_sha256 TEXT NOT NULL,
 horizons_json TEXT NOT NULL, slippage_pips REAL NOT NULL, slippage_semantics TEXT NOT NULL,
 cadence_sec INTEGER NOT NULL, timing_contract_json TEXT NOT NULL,
 market_calendar_policy TEXT NOT NULL, capture_contract_id TEXT NOT NULL,
 capture_cohort_id TEXT NOT NULL, source_schema_version TEXT NOT NULL,
 source_producer TEXT NOT NULL, source_code_sha256 TEXT NOT NULL,
 quote_transport_sha256 TEXT NOT NULL, config_sha256 TEXT NOT NULL,
 producer_sha256 TEXT NOT NULL,
 research_only INTEGER NOT NULL CHECK(research_only=1),
 can_trade INTEGER NOT NULL CHECK(can_trade=0),
 can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
 can_promote INTEGER NOT NULL CHECK(can_promote=0));
CREATE TABLE frames(
 frame_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL, scheduled_utc TEXT NOT NULL,
 read_started_utc TEXT NOT NULL, captured_utc TEXT NOT NULL, precommit_utc TEXT NOT NULL,
 market_open INTEGER NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL,
 expected_count INTEGER NOT NULL CHECK(expected_count=68), valid_count INTEGER NOT NULL,
 invalid_count INTEGER NOT NULL, raw_payload BLOB NOT NULL, raw_payload_sha256 TEXT NOT NULL,
 capture_contract_id TEXT NOT NULL, capture_cohort_id TEXT NOT NULL,
 source_schema_version TEXT NOT NULL, source_producer TEXT NOT NULL,
 UNIQUE(cohort_id,scheduled_utc));
CREATE TABLE frame_quotes(
 frame_id TEXT NOT NULL REFERENCES frames(frame_id), instrument TEXT NOT NULL,
 universe_index INTEGER NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL,
 bid REAL, ask REAL, pip REAL NOT NULL, quote_utc TEXT, quote_age_sec REAL,
 component_bytes BLOB NOT NULL, component_sha256 TEXT NOT NULL,
 capture_contract_id TEXT NOT NULL, capture_cohort_id TEXT NOT NULL,
 PRIMARY KEY(frame_id,instrument), UNIQUE(frame_id,universe_index));
CREATE TABLE frame_commit_receipts(
 frame_id TEXT PRIMARY KEY REFERENCES frames(frame_id), committed_utc TEXT NOT NULL,
 within_first_horizon INTEGER NOT NULL);
CREATE TABLE window_evaluations(
 target_scheduled_utc TEXT NOT NULL, declared_window_min INTEGER NOT NULL,
 entry_frame_id TEXT REFERENCES frames(frame_id), exit_frame_id TEXT REFERENCES frames(frame_id),
 evaluated_utc TEXT NOT NULL, state TEXT NOT NULL,
 expected_side_count INTEGER NOT NULL CHECK(expected_side_count=136),
 valid_count INTEGER NOT NULL, cleared_count INTEGER NOT NULL,
 path_cleared_count INTEGER NOT NULL, invalid_count INTEGER NOT NULL,
 pending_count INTEGER NOT NULL, terminal_rows_sha256 TEXT NOT NULL,
 path_summary_rows_sha256 TEXT NOT NULL, terminal_clear_ids_sha256 TEXT NOT NULL,
 path_clear_ids_sha256 TEXT NOT NULL,
 PRIMARY KEY(target_scheduled_utc,declared_window_min));
CREATE TABLE factor_episode_summaries(
 episode_utc TEXT NOT NULL, factor_currency TEXT NOT NULL, factor_sign INTEGER NOT NULL,
 event_kind TEXT NOT NULL, finalized_utc TEXT NOT NULL, member_count INTEGER NOT NULL,
 member_ids_sha256 TEXT NOT NULL, representative_json TEXT NOT NULL,
 PRIMARY KEY(episode_utc,factor_currency,factor_sign,event_kind));
CREATE TABLE factor_episode_finalizations(
 episode_utc TEXT PRIMARY KEY, finalized_utc TEXT NOT NULL,
 expected_window_count INTEGER NOT NULL, evaluated_window_count INTEGER NOT NULL,
 summary_count INTEGER NOT NULL, evaluation_keys_sha256 TEXT NOT NULL,
 terminal_member_count INTEGER NOT NULL, terminal_member_ids_sha256 TEXT NOT NULL,
 path_member_count INTEGER NOT NULL, path_member_ids_sha256 TEXT NOT NULL);
CREATE TRIGGER manifest_no_update BEFORE UPDATE ON cohort_manifest BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER manifest_no_delete BEFORE DELETE ON cohort_manifest BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER frames_no_update BEFORE UPDATE ON frames BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER frames_no_delete BEFORE DELETE ON frames BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER quotes_no_update BEFORE UPDATE ON frame_quotes BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER quotes_no_delete BEFORE DELETE ON frame_quotes BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER receipts_no_update BEFORE UPDATE ON frame_commit_receipts BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER receipts_no_delete BEFORE DELETE ON frame_commit_receipts BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER evaluations_no_update BEFORE UPDATE ON window_evaluations BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER evaluations_no_delete BEFORE DELETE ON window_evaluations BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER factor_summaries_no_update BEFORE UPDATE ON factor_episode_summaries BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER factor_summaries_no_delete BEFORE DELETE ON factor_episode_summaries BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER factor_finalizations_no_update BEFORE UPDATE ON factor_episode_finalizations BEGIN SELECT RAISE(ABORT,'append_only'); END;
CREATE TRIGGER factor_finalizations_no_delete BEFORE DELETE ON factor_episode_finalizations BEGIN SELECT RAISE(ABORT,'append_only'); END;
"""


def _manifest(connection: sqlite3.Connection) -> None:
    config_raw = verifier.CONFIG_PATH.read_bytes()
    universe = list(verifier.EXPECTED_INSTRUMENTS)
    pips = dict(verifier.EXPECTED_PIPS)
    timing = {
        "maximum_quote_age_sec": 30,
        "maximum_snapshot_generated_age_sec": 30,
        "maximum_future_skew_sec": 2,
        "maximum_capture_delay_sec": 55,
        "maximum_target_offset_sec": 55,
    }
    values = (
        1, verifier.COHORT_ID, verifier.SCHEMA, verifier.iso(verifier.ACTIVATION_UTC),
        verifier.canonical(universe).decode(), verifier.sha(verifier.canonical(universe)),
        verifier.canonical(pips).decode(), verifier.sha(verifier.canonical(pips)),
        verifier.canonical(list(verifier.HORIZONS_MIN)).decode(),
        verifier.SLIPPAGE_PIPS, verifier.SLIPPAGE_SEMANTICS,
        verifier.CADENCE_SEC, verifier.canonical(timing).decode(),
        verifier.MARKET_CALENDAR_POLICY, verifier.CAPTURE_CONTRACT_ID,
        verifier.CAPTURE_COHORT_ID, verifier.SOURCE_SCHEMA_VERSION,
        verifier.SOURCE_PRODUCER,
        verifier.sha(verifier.SOURCE_PRODUCER_PATH.read_bytes()),
        verifier.sha(verifier.QUOTE_TRANSPORT_PATH.read_bytes()),
        verifier.sha(config_raw), verifier.sha(verifier.PRODUCER_PATH.read_bytes()),
        1, 0, 0, 0,
    )
    connection.execute(
        "INSERT INTO cohort_manifest VALUES (" + ",".join("?" for _ in values) + ")",
        values,
    )


def _insert_frame(
    connection: sqlite3.Connection,
    scheduled: dt.datetime,
    step: float,
    *,
    missing: str | None = None,
) -> None:
    captured = scheduled + dt.timedelta(seconds=10)
    quotes = {}
    for instrument, pip in verifier.EXPECTED_PIPS.items():
        if instrument == missing:
            continue
        mid = 1.0 + step * pip
        quotes[instrument] = {
            "ask": mid + 0.5 * pip, "bid": mid - 0.5 * pip,
            "pip": pip, "source": "stream", "time": verifier.iso(captured),
        }
    payload = {
        "generated_utc": verifier.iso(captured), "producer": verifier.SOURCE_PRODUCER,
        "quote_count": len(quotes), "quotes": quotes, "schema_version": 2,
    }
    raw = verifier.canonical(payload)
    frame_id = verifier.sha(
        f"{verifier.COHORT_ID}|{verifier.iso(scheduled)}".encode()
    )
    rows = []
    valid = 0
    for index, instrument in enumerate(verifier.EXPECTED_INSTRUMENTS):
        component = quotes.get(instrument, {})
        expected = verifier._expected_quote(
            instrument, component, captured, scheduled, True, True
        )
        valid += int(expected["state"] == "valid")
        rows.append((
            frame_id, instrument, index, expected["state"], expected["reason"],
            expected["bid"], expected["ask"], expected["pip"],
            expected["quote_utc"], expected["quote_age_sec"],
            expected["component_bytes"], expected["component_sha256"],
            verifier.CAPTURE_CONTRACT_ID, verifier.CAPTURE_COHORT_ID,
        ))
    state = "valid" if valid == 68 else "partial_invalid"
    reason = "" if valid == 68 else "one_or_more_quote_rows_invalid"
    connection.execute(
        "INSERT INTO frames VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            frame_id, verifier.COHORT_ID, verifier.iso(scheduled),
            verifier.iso(captured), verifier.iso(captured),
            verifier.iso(captured + dt.timedelta(milliseconds=500)), 1, state, reason,
            68, valid, 68 - valid, raw, verifier.sha(raw),
            verifier.CAPTURE_CONTRACT_ID, verifier.CAPTURE_COHORT_ID,
            verifier.SOURCE_SCHEMA_VERSION, verifier.SOURCE_PRODUCER,
        ),
    )
    connection.executemany(
        "INSERT INTO frame_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
    )
    connection.execute(
        "INSERT INTO frame_commit_receipts VALUES (?,?,1)",
        (frame_id, verifier.iso(captured + dt.timedelta(seconds=1))),
    )


def _seed(
    tmp_path: Path,
    *,
    minutes: int = 3,
    steps: list[float] | None = None,
    missing_at: dict[int, str] | None = None,
    omit_minutes: set[int] | None = None,
) -> tuple[Path, Path, dt.datetime]:
    database = tmp_path / "census.sqlite"
    latest = tmp_path / "latest.json"
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(DDL)
    _manifest(connection)
    steps = steps or [2.0 * minute for minute in range(minutes)]
    missing_at = missing_at or {}
    omit_minutes = omit_minutes or set()
    for minute in range(minutes):
        if minute in omit_minutes:
            continue
        _insert_frame(
            connection,
            verifier.ACTIVATION_UTC + dt.timedelta(minutes=minute),
            steps[minute],
            missing=missing_at.get(minute),
        )
    connection.commit()
    frames = list(connection.execute(
        "SELECT f.* FROM frames f JOIN frame_commit_receipts r ON r.frame_id=f.frame_id "
        "AND r.within_first_horizon=1 ORDER BY f.scheduled_utc"
    ))
    quote_cache = {
        frame["frame_id"]: {
            row["instrument"]: row for row in connection.execute(
                "SELECT * FROM frame_quotes WHERE frame_id=? ORDER BY universe_index",
                (frame["frame_id"],),
            )
        }
        for frame in frames
    }
    generated = verifier.ACTIVATION_UTC + dt.timedelta(
        minutes=minutes - 1, seconds=10
    )
    cutoff = generated.replace(second=0, microsecond=0) - dt.timedelta(minutes=1)
    block_cache = {}
    for target_text, horizon in verifier._expected_evaluation_keys(cutoff):
        target = verifier.parse_utc(target_text)
        block = verifier._build_block(
            frames, quote_cache, target, horizon, terminal_missing_is_final=True
        )
        block_cache[(target_text, horizon)] = block
        digests = verifier._window_digests(block)
        connection.execute(
            "INSERT INTO window_evaluations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                target_text, horizon, block["entry_frame_id"], block["exit_frame_id"],
                verifier.iso(generated),
                "evaluated" if block["entry_frame_id"] and block["exit_frame_id"]
                else "terminal_invalid_missing_frame",
                136, block["valid_count"], block["cleared_count"],
                block["path_cleared_count"], block["invalid_count"],
                block["pending_count"], *digests,
            ),
        )
    episode = dt.datetime.fromtimestamp(
        int(verifier.ACTIVATION_UTC.timestamp() // 900) * 900, UTC
    )
    while episode + dt.timedelta(minutes=75) <= generated:
        expected, groups, terminal_ids, path_ids = verifier._factor_contract_for_episode(
            episode, block_cache
        )
        for (currency, sign, kind), group in groups.items():
            ids = sorted(group["ids"])
            connection.execute(
                "INSERT INTO factor_episode_summaries VALUES (?,?,?,?,?,?,?,?)",
                (
                    verifier.iso(episode), currency, sign, kind,
                    verifier.iso(generated), len(ids), verifier.sha(verifier.canonical(ids)),
                    verifier.canonical(group["representative"]).decode(),
                ),
            )
        connection.execute(
            "INSERT INTO factor_episode_finalizations VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                verifier.iso(episode), verifier.iso(generated), len(expected),
                len(expected), len(groups), verifier.sha(verifier.canonical(sorted(expected))),
                len(terminal_ids), verifier.sha(verifier.canonical(terminal_ids)),
                len(path_ids), verifier.sha(verifier.canonical(path_ids)),
            ),
        )
        episode += dt.timedelta(minutes=15)
    connection.commit()
    evaluations = [dict(row) for row in connection.execute("SELECT * FROM window_evaluations")]
    summaries = [dict(row) for row in connection.execute("SELECT * FROM factor_episode_summaries")]
    blocks, top = verifier._rebuild_blocks(frames, quote_cache, generated)
    expected_open = []
    cursor = verifier.ACTIVATION_UTC
    while cursor <= generated.replace(second=0, microsecond=0):
        if verifier.market_open(cursor):
            expected_open.append(verifier.iso(cursor))
        cursor += dt.timedelta(minutes=1)
    observed = {row["scheduled_utc"] for row in frames}
    missing = [stamp for stamp in expected_open if stamp not in observed]
    payload = {
        "schema_version": "executable_move_census_latest_v1",
        "generated_utc": verifier.iso(generated), "cohort_id": verifier.COHORT_ID,
        "research_only": True, "can_trade": False, "can_authorize": False,
        "can_promote": False,
        "definition": (
            "A move exists only when a later executable exit produces net pips > 0 "
            "after one frozen round-trip slippage deduction; spread is already "
            "embedded in bid/ask endpoints."
        ),
        "measurement_scope": (
            "terminal fixed-window net is primary; descriptive hindsight path fields "
            "report first clear, maximum favorable executable net, and minimum "
            "executable net from predeclared causal minute frames"
        ),
        "instrument_count": 68, "side_count": 136,
        "horizons_min": list(verifier.HORIZONS_MIN),
        "slippage_pips": verifier.SLIPPAGE_PIPS, "frame_count": len(frames),
        "latest_frame_utc": frames[-1]["scheduled_utc"] if frames else None,
        "schedule_census": {
            "expected_open_frames": len(expected_open),
            "observed_frames": sum(stamp in observed for stamp in expected_open),
            "missing_open_frames": len(missing),
            "recent_missing_scheduled_utc": missing[-120:],
        },
        "horizons": blocks, "top_cleared_paths": top[:50],
        "frame_inserted": True,
        "review_queue": verifier._review_queue(evaluations, summaries),
    }
    payload["content_sha256_excluding_this_field"] = verifier.sha(
        verifier.canonical(payload)
    )
    latest.write_bytes(verifier.canonical(payload))
    connection.close()
    return database, latest, generated


def _verify(database: Path, latest: Path, generated: dt.datetime, **overrides):
    return verifier.verify(
        database_path=database, latest_path=latest,
        now=generated + dt.timedelta(seconds=1), **overrides,
    )


def test_clean_full_68_by_2_fixture_verifies(tmp_path):
    database, latest, generated = _seed(tmp_path)
    result = _verify(database, latest, generated)
    assert result["verified"], result["failures"]
    assert result["counts"]["frames"] == 3
    assert result["counts"]["frame_quotes"] == 204
    assert result["counts"]["window_evaluations"] == 1
    payload = json.loads(latest.read_bytes())
    assert len(payload["horizons"]) == 6
    assert all(block["row_count"] == 136 for block in payload["horizons"])
    assert all(
        block["valid_count"] + block["invalid_count"] + block["pending_count"] == 136
        for block in payload["horizons"]
    )


def test_executable_math_uses_ask_to_bid_and_slippage_once(tmp_path):
    database, latest, generated = _seed(tmp_path)
    payload = json.loads(latest.read_bytes())
    block = next(row for row in payload["horizons"] if row["horizon_min"] == 1)
    long = next(row for row in block["rows"] if row["instrument"] == "AUD_CAD" and row["side"] == "long")
    short = next(row for row in block["rows"] if row["instrument"] == "AUD_CAD" and row["side"] == "short")
    assert long["raw_executable_pips"] == pytest.approx(1.0)
    assert long["net_pips"] == pytest.approx(0.75)
    assert short["raw_executable_pips"] == pytest.approx(-3.0)
    assert short["net_pips"] == pytest.approx(-3.25)
    assert _verify(database, latest, generated)["verified"]


def test_missing_source_pair_retains_two_invalid_arms_and_verifies(tmp_path):
    database, latest, generated = _seed(tmp_path, missing_at={1: "EUR_USD"})
    payload = json.loads(latest.read_bytes())
    block = payload["horizons"][0]
    rows = [row for row in block["rows"] if row["instrument"] == "EUR_USD"]
    assert len(rows) == 2 and {row["state"] for row in rows} == {"invalid"}
    result = _verify(database, latest, generated)
    assert result["verified"], result["failures"]


def test_wholly_missing_minute_is_136_invalid_and_gap_fails_closed(tmp_path):
    database, latest, generated = _seed(tmp_path, omit_minutes={1})
    connection = sqlite3.connect(database)
    row = connection.execute(
        "SELECT valid_count,invalid_count,pending_count,state FROM window_evaluations "
        "WHERE target_scheduled_utc=? AND declared_window_min=1",
        (verifier.iso(verifier.ACTIVATION_UTC + dt.timedelta(minutes=1)),),
    ).fetchone()
    connection.close()
    assert row == (0, 136, 0, "terminal_invalid_missing_frame")
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert any("missing_open_minute" in failure for failure in result["failures"])


def test_trailing_missing_open_minute_is_not_hidden_by_latest_frame(tmp_path):
    database, latest, generated = _seed(tmp_path, omit_minutes={2})
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert "latest:schedule_census:missing_open_frames" in result["failures"]


def test_noop_append_only_trigger_and_extra_table_are_rejected(tmp_path):
    database, latest, generated = _seed(tmp_path)
    connection = sqlite3.connect(database)
    connection.execute("DROP TRIGGER frames_no_update")
    connection.execute(
        "CREATE TRIGGER frames_no_update BEFORE UPDATE ON frames WHEN 0 "
        "BEGIN SELECT RAISE(ABORT,'append_only'); END"
    )
    connection.execute("CREATE TABLE orders(order_id TEXT PRIMARY KEY)")
    connection.commit(); connection.close()
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert "trigger:frames_no_update:missing_or_unsafe" in result["failures"]
    assert "schema:table_set_mismatch" in result["failures"]


def test_precommit_and_evaluation_future_clock_tamper_are_rejected(tmp_path):
    database, latest, generated = _seed(tmp_path)
    connection = sqlite3.connect(database)
    connection.execute("DROP TRIGGER frames_no_update")
    connection.execute("DROP TRIGGER receipts_no_update")
    connection.execute("DROP TRIGGER evaluations_no_update")
    scheduled = verifier.ACTIVATION_UTC
    connection.execute(
        "UPDATE frames SET precommit_utc=? WHERE scheduled_utc=?",
        (
            verifier.iso(scheduled + dt.timedelta(seconds=56)),
            verifier.iso(scheduled),
        ),
    )
    connection.execute(
        "UPDATE frame_commit_receipts SET committed_utc=? "
        "WHERE frame_id=(SELECT frame_id FROM frames WHERE scheduled_utc=?)",
        (
            verifier.iso(scheduled + dt.timedelta(seconds=57)),
            verifier.iso(scheduled),
        ),
    )
    connection.execute(
        "UPDATE window_evaluations SET evaluated_utc='2099-01-01T00:00:00Z'"
    )
    connection.commit(); connection.close()
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert any("noncausal_clocks" in failure for failure in result["failures"])
    assert any("evaluated_clock" in failure for failure in result["failures"])


def test_deleted_quote_row_and_removed_trigger_are_detected(tmp_path):
    database, latest, generated = _seed(tmp_path)
    connection = sqlite3.connect(database)
    connection.execute("DROP TRIGGER quotes_no_delete")
    connection.execute(
        "DELETE FROM frame_quotes WHERE rowid=(SELECT rowid FROM frame_quotes WHERE instrument='EUR_USD' LIMIT 1)"
    )
    connection.commit(); connection.close()
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert any("quotes_no_delete" in failure for failure in result["failures"])
    assert any("quote_count" in failure for failure in result["failures"])


def test_late_receipt_flag_tamper_is_detected(tmp_path):
    database, latest, generated = _seed(tmp_path)
    connection = sqlite3.connect(database)
    connection.execute("DROP TRIGGER receipts_no_update")
    connection.execute(
        "UPDATE frame_commit_receipts SET within_first_horizon=0 "
        "WHERE rowid=(SELECT rowid FROM frame_commit_receipts LIMIT 1)"
    )
    connection.commit(); connection.close()
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert any("commit_receipt_flag" in failure for failure in result["failures"])


def test_window_digest_and_count_tamper_are_detected(tmp_path):
    database, latest, generated = _seed(tmp_path)
    connection = sqlite3.connect(database)
    connection.execute("DROP TRIGGER evaluations_no_update")
    connection.execute(
        "UPDATE window_evaluations SET terminal_rows_sha256=?,valid_count=135",
        ("0" * 64,),
    )
    connection.commit(); connection.close()
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert any("terminal_rows_sha256" in failure for failure in result["failures"])
    assert any("count_partition" in failure or "valid_count" in failure for failure in result["failures"])


def test_latest_missing_side_and_rehashed_payload_still_fails(tmp_path):
    database, latest, generated = _seed(tmp_path)
    payload = json.loads(latest.read_bytes())
    payload["horizons"][0]["rows"].pop()
    payload.pop("content_sha256_excluding_this_field")
    payload["content_sha256_excluding_this_field"] = verifier.sha(verifier.canonical(payload))
    latest.write_bytes(verifier.canonical(payload))
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert any("horizons" in failure for failure in result["failures"])


def test_latest_velocity_or_trading_surface_is_rejected(tmp_path):
    database, latest, generated = _seed(tmp_path)
    payload = json.loads(latest.read_bytes())
    payload["velocity_bps_per_hour"] = 999999
    payload["can_trade"] = True
    payload.pop("content_sha256_excluding_this_field")
    payload["content_sha256_excluding_this_field"] = verifier.sha(verifier.canonical(payload))
    latest.write_bytes(verifier.canonical(payload))
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert "latest:top_level_contract" in result["failures"]
    assert "latest:can_trade:mismatch" in result["failures"]


def test_config_and_source_identity_drift_are_detected(tmp_path):
    database, latest, generated = _seed(tmp_path)
    config = tmp_path / "config.json"
    payload = json.loads(verifier.CONFIG_PATH.read_bytes())
    payload["slippage_pips"] = 9
    config.write_text(json.dumps(payload), encoding="utf-8")
    source = tmp_path / "source.py"
    source.write_bytes(verifier.SOURCE_PRODUCER_PATH.read_bytes() + b"\n# drift\n")
    quote_transport = tmp_path / "quote_transport.py"
    quote_transport.write_bytes(
        verifier.QUOTE_TRANSPORT_PATH.read_bytes() + b"\n# drift\n"
    )
    result = _verify(
        database, latest, generated, config_path=config,
        source_producer_path=source, quote_transport_path=quote_transport,
    )
    assert not result["verified"]
    assert "config:file_sha256:mismatch" in result["failures"]
    assert "source_producer:file_sha256:mismatch" in result["failures"]
    assert "quote_transport:file_sha256:mismatch" in result["failures"]


def test_producer_order_or_network_surface_is_detected(tmp_path):
    database, latest, generated = _seed(tmp_path)
    producer = tmp_path / "producer.py"
    producer.write_bytes(
        verifier.PRODUCER_PATH.read_bytes()
        + b"\nimport requests\ndef forbidden():\n    return requests.post('x')\n"
    )
    result = _verify(database, latest, generated, producer_path=producer)
    assert not result["verified"]
    assert any("forbidden_import" in failure for failure in result["failures"])
    assert any("forbidden_call:post" in failure for failure in result["failures"])


def test_terminal_loser_can_have_distinct_observed_path_clear(tmp_path):
    database, _, _ = _seed(
        tmp_path, minutes=6, steps=[0, 3, 0, 0, 0, 0]
    )
    connection = sqlite3.connect(database); connection.row_factory = sqlite3.Row
    frames = list(connection.execute(
        "SELECT f.* FROM frames f JOIN frame_commit_receipts r ON r.frame_id=f.frame_id "
        "AND r.within_first_horizon=1 ORDER BY f.scheduled_utc"
    ))
    cache = {frame["frame_id"]: {row["instrument"]: row for row in connection.execute(
        "SELECT * FROM frame_quotes WHERE frame_id=?", (frame["frame_id"],)
    )} for frame in frames}
    block = verifier._build_block(
        frames, cache, verifier.ACTIVATION_UTC + dt.timedelta(minutes=5), 5
    )
    row = next(item for item in block["rows"] if item["instrument"] == "AUD_CAD" and item["side"] == "long")
    connection.close()
    assert row["state"] == "not_cleared"
    assert row["path_cleared"] is True and row["first_clear_min"] == 1
    assert row["path_max_frame_id"] == row["path_points"][0]["frame_id"]


def test_finalized_factor_episode_and_empty_safe_receipt_verify(tmp_path):
    database, latest, generated = _seed(
        tmp_path, minutes=76, steps=[0.0] * 76
    )
    result = _verify(database, latest, generated)
    assert result["verified"], result["failures"]
    assert result["counts"]["factor_episode_finalizations"] == 1
    assert result["counts"]["factor_episode_summaries"] == 0
    connection = sqlite3.connect(database)
    connection.execute("DROP TRIGGER factor_finalizations_no_update")
    connection.execute(
        "UPDATE factor_episode_finalizations SET finalized_utc=?",
        (verifier.iso(verifier.ACTIVATION_UTC.replace(minute=0) + dt.timedelta(minutes=75)),),
    )
    connection.commit(); connection.close()
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert any("factor_finalization" in failure and ":clock" in failure for failure in result["failures"])


def test_factor_summary_representative_tamper_is_detected(tmp_path):
    database, latest, generated = _seed(tmp_path, minutes=76)
    connection = sqlite3.connect(database)
    connection.execute("DROP TRIGGER factor_summaries_no_update")
    connection.execute("DROP TRIGGER factor_finalizations_no_update")
    connection.execute(
        "UPDATE factor_episode_summaries SET member_ids_sha256=?,finalized_utc=? "
        "WHERE rowid=(SELECT rowid FROM factor_episode_summaries LIMIT 1)",
        (
            "f" * 64,
            verifier.iso(
                verifier.ACTIVATION_UTC.replace(minute=0)
                + dt.timedelta(minutes=75)
            ),
        ),
    )
    connection.execute(
        "UPDATE factor_episode_finalizations SET finalized_utc=?",
        (
            verifier.iso(
                verifier.ACTIVATION_UTC.replace(minute=0)
                + dt.timedelta(minutes=75)
            ),
        ),
    )
    connection.commit(); connection.close()
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert any("member_ids_sha256" in failure for failure in result["failures"])
    assert any("factor_finalization" in failure and ":clock" in failure for failure in result["failures"])
    assert any("factor_summary" in failure and ":clock" in failure for failure in result["failures"])


def test_atomic_replace_retries_and_preserves_cleanup(tmp_path, monkeypatch):
    target = tmp_path / "verifier.json"
    target.write_text('{"old":true}', encoding="utf-8")
    real_replace = verifier.os.replace
    calls = {"count": 0}

    def flaky(source, destination):
        calls["count"] += 1
        if calls["count"] < 3:
            raise PermissionError(5, "busy")
        return real_replace(source, destination)

    monkeypatch.setattr(verifier.os, "replace", flaky)
    monkeypatch.setattr(verifier.time, "sleep", lambda _: None)
    verifier.write_json_atomic(target, {"new": True})
    assert json.loads(target.read_text()) == {"new": True}
    assert calls["count"] == 3
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_replace_exhaustion_preserves_prior_target(tmp_path, monkeypatch):
    target = tmp_path / "verifier.json"
    target.write_text('{"old":true}', encoding="utf-8")
    monkeypatch.setattr(
        verifier.os, "replace",
        lambda source, destination: (_ for _ in ()).throw(PermissionError(5, "busy")),
    )
    monkeypatch.setattr(verifier.time, "sleep", lambda _: None)
    with pytest.raises(PermissionError):
        verifier.write_json_atomic(target, {"new": True})
    assert json.loads(target.read_text()) == {"old": True}
    assert not list(tmp_path.glob("*.tmp"))


def test_checkpoint_tamper_forces_empty_cache(tmp_path):
    checkpoint = tmp_path / "checkpoint.json"
    verifier.write_runtime_checkpoint(
        checkpoint,
        {
            "database_path": "x", "frame_count": 1,
            "frame_chain_sha256": "1" * 64,
            "last_generated_utc": verifier.iso(verifier.ACTIVATION_UTC),
            "evaluation_rows": {}, "factor_episodes": {},
        },
    )
    payload = json.loads(checkpoint.read_bytes())
    payload["frame_count"] = 99
    checkpoint.write_bytes(verifier.canonical(payload))
    assert verifier.load_runtime_checkpoint(checkpoint) == {}


def test_main_runs_bounded_supervised_loop(tmp_path, monkeypatch):
    output = tmp_path / "verifier.json"
    clock = iter((0.0, 0.0, 30.0, 60.0))
    calls = []

    def fake_verify(**kwargs):
        calls.append(kwargs)
        return {"verified": True, "iteration": len(calls)}

    monkeypatch.setattr(verifier, "verify", fake_verify)
    monkeypatch.setattr(verifier.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(verifier.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "verifier", "--output", str(output), "--interval-sec", "30",
            "--duration-sec", "60", "--checkpoint",
            str(tmp_path / "checkpoint.json"),
        ],
    )
    assert verifier.main() == 0
    assert len(calls) == 3
    assert json.loads(output.read_text()) == {"verified": True, "iteration": 3}


def test_runtime_cache_skips_verified_evidence_but_rechecks_changed_row(
    tmp_path, monkeypatch
):
    database, latest, generated = _seed(tmp_path)
    runtime_cache = {}
    assert _verify(
        database, latest, generated, runtime_cache=runtime_cache
    )["verified"]
    checkpoint = tmp_path / "checkpoint.json"
    verifier.write_runtime_checkpoint(checkpoint, runtime_cache)
    runtime_cache = verifier.load_runtime_checkpoint(checkpoint)
    assert runtime_cache["frame_count"] == 3
    real_build = verifier._build_block
    calls = {"count": 0}

    def counted(*args, **kwargs):
        calls["count"] += 1
        return real_build(*args, **kwargs)

    monkeypatch.setattr(verifier, "_build_block", counted)
    assert _verify(
        database, latest, generated, runtime_cache=runtime_cache
    )["verified"]
    # Only the six current dashboard horizons are rebuilt; the immutable
    # historical evidence window is served by its verified process prefix.
    assert calls["count"] == len(verifier.HORIZONS_MIN)

    connection = sqlite3.connect(database)
    connection.execute("DROP TRIGGER frames_no_update")
    connection.execute("DROP TRIGGER quotes_no_update")
    frame_id, raw = connection.execute(
        "SELECT frame_id,raw_payload FROM frames ORDER BY scheduled_utc LIMIT 1"
    ).fetchone()
    payload = json.loads(bytes(raw))
    component = dict(payload["quotes"]["AUD_CAD"])
    component["bid"] += 0.0001
    component["ask"] += 0.0001
    payload["quotes"]["AUD_CAD"] = component
    raw = verifier.canonical(payload)
    component_bytes = verifier.canonical(component)
    connection.execute(
        "UPDATE frames SET raw_payload=?,raw_payload_sha256=? WHERE frame_id=?",
        (raw, verifier.sha(raw), frame_id),
    )
    connection.execute(
        "UPDATE frame_quotes SET bid=?,ask=?,component_bytes=?,component_sha256=? "
        "WHERE frame_id=? AND instrument='AUD_CAD'",
        (
            component["bid"], component["ask"], component_bytes,
            verifier.sha(component_bytes), frame_id,
        ),
    )
    connection.execute(
        "CREATE TRIGGER frames_no_update BEFORE UPDATE ON frames "
        "BEGIN SELECT RAISE(ABORT,'append_only'); END"
    )
    connection.execute(
        "CREATE TRIGGER quotes_no_update BEFORE UPDATE ON frame_quotes "
        "BEGIN SELECT RAISE(ABORT,'append_only'); END"
    )
    connection.commit(); connection.close()
    result = _verify(database, latest, generated, runtime_cache=runtime_cache)
    assert not result["verified"]
    assert "runtime_cache:frame_prefix_drift" in result["failures"]

    connection = sqlite3.connect(database)
    connection.execute("DROP TRIGGER evaluations_no_update")
    connection.execute(
        "UPDATE window_evaluations SET terminal_rows_sha256=?",
        ("9" * 64,),
    )
    connection.execute(
        "CREATE TRIGGER evaluations_no_update BEFORE UPDATE ON window_evaluations "
        "BEGIN SELECT RAISE(ABORT,'append_only'); END"
    )
    connection.commit(); connection.close()
    calls["count"] = 0
    result = _verify(database, latest, generated, runtime_cache=runtime_cache)
    assert not result["verified"]
    assert any("terminal_rows_sha256" in failure for failure in result["failures"])
    assert calls["count"] > len(verifier.HORIZONS_MIN)


def test_malformed_durable_value_returns_failure_instead_of_crashing(tmp_path):
    database, latest, generated = _seed(tmp_path)
    connection = sqlite3.connect(database)
    connection.execute("DROP TRIGGER frames_no_update")
    connection.execute(
        "UPDATE frames SET raw_payload='not-bytes' "
        "WHERE rowid=(SELECT rowid FROM frames LIMIT 1)"
    )
    connection.commit(); connection.close()
    result = _verify(database, latest, generated)
    assert not result["verified"]
    assert any("database:contract:TypeError" in failure for failure in result["failures"])


def test_verifier_module_does_not_import_producer():
    source = verifier.Path(verifier.__file__).read_text(encoding="utf-8")
    assert "import oanda_executable_move_census_v1" not in source
    assert "from oanda_executable_move_census_v1" not in source
