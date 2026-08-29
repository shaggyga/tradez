from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import oanda_practice_top_signal_executor as fast
import oanda_independent_evidence_verifier as verifier_producer


def aggregate_candidate(**updates):
    candidate = {
        "id": "forecast-representative-1",
        "instrument": "AUD_USD",
        "direction": "buy",
        "execution_horizon_sec": 21600,
        "horizon_breakdown": [
            {
                "horizon_sec": 21600,
                "contributors": [
                    {
                        "family": "momentum",
                        "model_id": "momentum.fast",
                        "lane_id": "momentum.fast",
                        "input_timeframe": "M30",
                        "direction": "buy",
                        "signal_role": "structural",
                        "source_kind": "strategy_signal",
                        "feed_source": "strategy_lab",
                        "forecast_generated_epoch": 1234.5,
                    }
                ],
            }
        ],
    }
    candidate.update(updates)
    return candidate


def test_execution_signal_contract_is_deterministic_and_preserves_lineage():
    first = fast.bind_execution_signal_contract(aggregate_candidate())
    second = fast.bind_execution_signal_contract(aggregate_candidate())

    assert first["id"] == second["id"]
    assert first["id"].startswith("exec_signal_")
    assert first["representative_component_id"] == "forecast-representative-1"
    assert first["execution_signal_contract_schema"] == (
        fast.EXECUTION_SIGNAL_CONTRACT_SCHEMA
    )


def test_execution_signal_contract_changes_with_aggregate_direction_or_horizon():
    baseline = fast.bind_execution_signal_contract(aggregate_candidate())
    opposite = fast.bind_execution_signal_contract(
        aggregate_candidate(direction="sell")
    )
    other_horizon = fast.bind_execution_signal_contract(
        aggregate_candidate(execution_horizon_sec=14400)
    )

    assert opposite["id"] != baseline["id"]
    assert other_horizon["id"] != baseline["id"]


def test_execution_signal_contract_changes_with_contributor_generation():
    baseline = aggregate_candidate()
    changed = aggregate_candidate()
    changed["horizon_breakdown"][0]["contributors"][0][
        "forecast_generated_epoch"
    ] = 1235.0

    assert fast.bind_execution_signal_contract(changed)["id"] != (
        fast.bind_execution_signal_contract(baseline)["id"]
    )


def test_governed_executor_does_not_manage_isolated_news_challenger_trade():
    ordinary = {
        "clientExtensions": {
            "tag": "strategy_lab_top",
            "comment": "momentum.fast|h900",
        }
    }
    challenger = {
        "clientExtensions": {
            "tag": "strategy_lab_top",
            "comment": "news007.news_technical_confirmed.abc|h900",
        }
    }
    assert fast.GovernedPracticeExecutor.owns_trade(ordinary) is True
    assert fast.GovernedPracticeExecutor.owns_trade(challenger) is False


def write_canary_authorization(path, **updates):
    payload = {
        "schema_version": 2,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "environment": "practice",
        "account_suffix": "-007",
        "account_id": "101-001-37981792-007",
        "entry_authorized": False,
        "authorized_entries": [],
        "reason": "zero_confirmed_candidates",
    }
    payload.update(updates)
    if payload["entry_authorized"]:
        payload["signature_hmac_sha256"] = fast.canary_payload_signature(
            payload, "test-secret"
        )
    path.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def write_independent_verifier(path, *, completed=None):
    completed = completed or datetime.now(timezone.utc).isoformat()
    payload = {
        "schema_version": 2,
        "publication_contract": fast.INDEPENDENT_VERIFIER_PUBLICATION_CONTRACT,
        "generated_utc": completed,
        "completed_verification_utc": completed,
        "verification_pass_id": "test-pass-1",
        "verification_owner_id": "test-owner-1",
        "publication_generation": 1,
        "status": "match",
        "authorization_safe": True,
        "verified_confirmed_candidates": [
            {
                "governed_hypothesis_id": "hypothesis-1",
                "proof_cohort_id": "proof-1",
            }
        ],
    }
    state_bytes = json.dumps(payload).encode("utf-8")
    path.write_bytes(state_bytes)
    guard = fast.independent_verifier_guard_path(path)
    connection = sqlite3.connect(guard)
    connection.executescript(
        """
        CREATE TABLE verifier_publication_guard(
            singleton INTEGER PRIMARY KEY,
            generation INTEGER NOT NULL,
            pass_id TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            phase TEXT NOT NULL,
            status TEXT NOT NULL,
            authorization_safe INTEGER NOT NULL,
            started_utc TEXT NOT NULL,
            heartbeat_utc TEXT NOT NULL,
            lease_expires_epoch REAL NOT NULL,
            completed_utc TEXT,
            state_sha256 TEXT,
            failure_reason TEXT
        );
        """
    )
    connection.execute(
        "INSERT INTO verifier_publication_guard VALUES(1,1,?,?,?,?,1,?,?,?, ?,?,NULL)",
        (
            "test-pass-1",
            "test-owner-1",
            "completed",
            "match",
            completed,
            completed,
            time.time() + 60.0,
            completed,
            "sha256:" + fast.hashlib.sha256(state_bytes).hexdigest(),
        ),
    )
    connection.commit()
    connection.close()


def write_lifecycle_database(path, *, state="confirmed_candidate"):
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE hypotheses(
            hypothesis_id TEXT PRIMARY KEY,
            cohort_id TEXT
        );
        CREATE TABLE lifecycle_events(
            event_id TEXT PRIMARY KEY,
            hypothesis_id TEXT NOT NULL,
            next_state TEXT NOT NULL
        );
        """
    )
    connection.execute(
        "INSERT INTO hypotheses VALUES (?, ?)",
        ("hypothesis-1", "proof-1"),
    )
    connection.execute(
        "INSERT INTO lifecycle_events VALUES (?, ?, ?)",
        ("event-1", "hypothesis-1", state),
    )
    connection.commit()
    connection.close()


def test_governed_canary_authorizer_fails_closed_when_missing(tmp_path):
    authorizer = fast.GovernedCanaryAuthorizer(
        tmp_path / "missing.json",
        lifecycle_database=tmp_path / "lifecycle.sqlite",
    )

    decision = authorizer.authorize(
        {"id": "signal-1"},
        account_id="101-001-37981792-007",
    )

    assert decision["allowed"] is False
    assert decision["reason"] == "authorization_file_missing"


def test_independent_verifier_guard_invalidates_unchanged_old_match(tmp_path):
    verifier = tmp_path / "verifier.json"
    write_independent_verifier(verifier)
    authorizer = fast.GovernedCanaryAuthorizer(
        tmp_path / "authorization.json",
        lifecycle_database=tmp_path / "lifecycle.sqlite",
        independent_verifier_state=verifier,
    )
    assert authorizer._independent_verification("hypothesis-1", "proof-1") == (
        True,
        "independently_verified_confirmed_candidate",
    )
    guard = fast.independent_verifier_guard_path(verifier)
    connection = sqlite3.connect(guard)
    connection.execute(
        """
        UPDATE verifier_publication_guard
        SET generation=2,pass_id='new-pass',owner_id='new-owner',
            phase='verification_in_progress',status='verification_in_progress',
            authorization_safe=0,completed_utc=NULL,state_sha256=NULL
        WHERE singleton=1
        """
    )
    connection.commit()
    connection.close()
    assert authorizer._independent_verification("hypothesis-1", "proof-1") == (
        False,
        "independent_verifier_guard_not_safe",
    )


def test_independent_verifier_rejects_completion_ten_minutes_in_future(tmp_path):
    verifier = tmp_path / "verifier.json"
    future = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    write_independent_verifier(verifier, completed=future)
    authorizer = fast.GovernedCanaryAuthorizer(
        tmp_path / "authorization.json",
        lifecycle_database=tmp_path / "lifecycle.sqlite",
        independent_verifier_state=verifier,
        maximum_age_sec=900.0,
    )

    assert authorizer._independent_verification("hypothesis-1", "proof-1") == (
        False,
        "independent_verifier_stale",
    )


def test_independent_verifier_requires_guard_and_exact_state_hash(tmp_path):
    verifier = tmp_path / "verifier.json"
    write_independent_verifier(verifier)
    guard = fast.independent_verifier_guard_path(verifier)
    guard.unlink()
    authorizer = fast.GovernedCanaryAuthorizer(
        tmp_path / "authorization.json",
        lifecycle_database=tmp_path / "lifecycle.sqlite",
        independent_verifier_state=verifier,
    )
    allowed, reason = authorizer._independent_verification(
        "hypothesis-1", "proof-1"
    )
    assert allowed is False
    assert reason == "independent_verifier_guard_missing"

    write_independent_verifier(verifier)
    payload = json.loads(verifier.read_text(encoding="utf-8"))
    payload["generated_utc"] = datetime.now(timezone.utc).isoformat()
    verifier.write_text(json.dumps(payload), encoding="utf-8")
    allowed, reason = authorizer._independent_verification(
        "hypothesis-1", "proof-1"
    )
    assert allowed is False
    assert reason == "independent_verifier_guard_state_mismatch"


def test_windows_producer_to_current_executor_exact_byte_round_trip(tmp_path):
    state = tmp_path / "independent_evidence_verifier_v1.json"
    report = tmp_path / "INDEPENDENT_EVIDENCE_VERIFIER_CURRENT.md"
    guard = verifier_producer.verifier_guard_path(state)
    ownership = verifier_producer._claim_verification_pass(guard)
    completed = datetime.now(timezone.utc).isoformat()
    payload = {
        "schema_version": 2,
        "publication_contract": verifier_producer.PUBLICATION_CONTRACT,
        "generated_utc": completed,
        "completed_verification_utc": completed,
        "verification_pass_id": ownership["pass_id"],
        "verification_owner_id": ownership["owner_id"],
        "publication_generation": ownership["generation"],
        "status": "match",
        "authorization_safe": True,
        "can_place_orders": False,
        "can_promote": False,
        "verified_confirmed_candidates": [
            {
                "governed_hypothesis_id": "hypothesis-1",
                "proof_cohort_id": "proof-1",
            }
        ],
    }
    verifier_producer._publish_owned_final(
        state_path=state,
        report_path=report,
        guard_path=guard,
        ownership=ownership,
        payload=payload,
        report="exact-byte round trip",
    )
    state_bytes = state.read_bytes()
    expected_bytes = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    assert state_bytes == expected_bytes
    if os.name == "nt":
        assert b"\r\n" not in state_bytes
    connection = sqlite3.connect(guard)
    try:
        row = connection.execute(
            "SELECT state_sha256 FROM verifier_publication_guard WHERE singleton=1"
        ).fetchone()
    finally:
        connection.close()
    assert row[0] == "sha256:" + hashlib.sha256(state_bytes).hexdigest()

    authorizer = fast.GovernedCanaryAuthorizer(
        tmp_path / "authorization.json",
        lifecycle_database=tmp_path / "lifecycle.sqlite",
        independent_verifier_state=state,
        independent_verifier_guard=guard,
    )
    assert authorizer._independent_verification("hypothesis-1", "proof-1") == (
        True,
        "independently_verified_confirmed_candidate",
    )


def test_governed_canary_authorizer_blocks_qualified_but_unconfirmed(tmp_path):
    path = tmp_path / "authorization.json"
    write_canary_authorization(path)
    authorizer = fast.GovernedCanaryAuthorizer(
        path,
        lifecycle_database=tmp_path / "lifecycle.sqlite",
    )

    decision = authorizer.authorize(
        {
            "id": "legacy-qualified-signal",
            "signal_eligible": True,
            "proof_cohort_id": "",
            "allocator_cohort_id": "",
        },
        account_id="101-001-37981792-007",
    )

    assert decision["allowed"] is False
    assert decision["reason"] == "zero_confirmed_candidates"


def test_governed_canary_authorizer_requires_exact_versioned_match(tmp_path):
    path = tmp_path / "authorization.json"
    write_canary_authorization(
        path,
        entry_authorized=True,
        reason="explicit_canary_active",
        authorized_entries=[
            {
                "authorization_id": "canary-1",
                "one_time_nonce": "nonce-1",
                "account_id": "101-001-37981792-007",
                "signal_id": "signal-1",
                "proof_cohort_id": "proof-1",
                "governed_hypothesis_id": "hypothesis-1",
                "allocator_cohort_id": "allocator-1",
                "confirmed_candidate": True,
                "allowed_instrument": "EUR_USD",
                "allowed_direction": "buy",
                "maximum_units": 10,
                "maximum_notional": 1000.0,
                "maximum_total_exposure": 2000.0,
                "issued_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": datetime.fromtimestamp(time.time() + 60.0, timezone.utc).isoformat(),
            }
        ],
    )
    lifecycle = tmp_path / "lifecycle.sqlite"
    write_lifecycle_database(lifecycle)
    verifier = tmp_path / "verifier.json"
    write_independent_verifier(verifier)
    authorizer = fast.GovernedCanaryAuthorizer(
        path,
        lifecycle_database=lifecycle,
        independent_verifier_state=verifier,
        consumption_database=tmp_path / "consumptions.sqlite",
        hmac_key="test-secret",
    )

    wrong = authorizer.authorize(
        {
            "id": "signal-1",
            "proof_cohort_id": "proof-1",
            "governed_hypothesis_id": "hypothesis-1",
            "allocator_cohort_id": "allocator-wrong",
            "instrument": "EUR_USD",
            "direction": "buy",
        },
        account_id="101-001-37981792-007",
    )
    exact = authorizer.authorize(
        {
            "id": "signal-1",
            "proof_cohort_id": "proof-1",
            "governed_hypothesis_id": "hypothesis-1",
            "allocator_cohort_id": "allocator-1",
            "instrument": "EUR_USD",
            "direction": "buy",
        },
        account_id="101-001-37981792-007",
    )

    assert wrong["allowed"] is False
    assert exact["allowed"] is True
    assert exact["authorization_id"] == "canary-1"


def test_authorization_cannot_override_unconfirmed_lifecycle_state(tmp_path):
    path = tmp_path / "authorization.json"
    write_canary_authorization(
        path,
        entry_authorized=True,
        authorized_entries=[
            {
                "authorization_id": "forged-canary",
                "one_time_nonce": "nonce-forged",
                "account_id": "101-001-37981792-007",
                "signal_id": "signal-1",
                "proof_cohort_id": "proof-1",
                "governed_hypothesis_id": "hypothesis-1",
                "allocator_cohort_id": "allocator-1",
                "confirmed_candidate": True,
                "allowed_instrument": "EUR_USD",
                "allowed_direction": "buy",
                "maximum_units": 10,
                "maximum_notional": 1000.0,
                "maximum_total_exposure": 2000.0,
                "issued_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": datetime.fromtimestamp(time.time() + 60.0, timezone.utc).isoformat(),
            }
        ],
    )
    lifecycle = tmp_path / "lifecycle.sqlite"
    write_lifecycle_database(lifecycle, state="continue_collecting")
    verifier = tmp_path / "verifier.json"
    write_independent_verifier(verifier)
    authorizer = fast.GovernedCanaryAuthorizer(
        path,
        lifecycle_database=lifecycle,
        independent_verifier_state=verifier,
        consumption_database=tmp_path / "consumptions.sqlite",
        hmac_key="test-secret",
    )

    decision = authorizer.authorize(
        {
            "id": "signal-1",
            "proof_cohort_id": "proof-1",
            "governed_hypothesis_id": "hypothesis-1",
            "allocator_cohort_id": "allocator-1",
            "instrument": "EUR_USD",
            "direction": "buy",
        },
        account_id="101-001-37981792-007",
    )

    assert decision["allowed"] is False
    assert decision["reason"] == "governed_hypothesis_not_confirmed"


def test_governed_canary_rejects_tampered_hmac(tmp_path):
    path = tmp_path / "authorization.json"
    payload = write_canary_authorization(
        path,
        entry_authorized=True,
        authorized_entries=[],
    )
    payload["reason"] = "tampered_after_signing"
    path.write_text(json.dumps(payload), encoding="utf-8")
    lifecycle = tmp_path / "lifecycle.sqlite"
    write_lifecycle_database(lifecycle)
    verifier = tmp_path / "verifier.json"
    write_independent_verifier(verifier)
    authorizer = fast.GovernedCanaryAuthorizer(
        path,
        lifecycle_database=lifecycle,
        independent_verifier_state=verifier,
        consumption_database=tmp_path / "consumptions.sqlite",
        hmac_key="test-secret",
    )
    decision = authorizer.authorize({"id": "signal-1"}, account_id="101-001-37981792-007")
    assert decision["allowed"] is False
    assert decision["reason"] == "authorization_hmac_invalid"


def test_governed_canary_nonce_is_atomic_and_one_time(tmp_path):
    path = tmp_path / "authorization.json"
    write_canary_authorization(
        path,
        entry_authorized=True,
        authorized_entries=[
            {
                "authorization_id": "canary-once",
                "one_time_nonce": "nonce-once",
                "account_id": "101-001-37981792-007",
                "signal_id": "signal-1",
                "proof_cohort_id": "proof-1",
                "governed_hypothesis_id": "hypothesis-1",
                "allocator_cohort_id": "allocator-1",
                "confirmed_candidate": True,
                "allowed_instrument": "EUR_USD",
                "allowed_direction": "buy",
                "maximum_units": 10,
                "maximum_notional": 1000.0,
                "maximum_total_exposure": 2000.0,
                "issued_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": datetime.fromtimestamp(time.time() + 60.0, timezone.utc).isoformat(),
            }
        ],
    )
    lifecycle = tmp_path / "lifecycle.sqlite"
    write_lifecycle_database(lifecycle)
    verifier = tmp_path / "verifier.json"
    write_independent_verifier(verifier)
    consumption = tmp_path / "consumptions.sqlite"
    candidate = {
        "id": "signal-1",
        "proof_cohort_id": "proof-1",
        "governed_hypothesis_id": "hypothesis-1",
        "allocator_cohort_id": "allocator-1",
        "instrument": "EUR_USD",
        "direction": "buy",
    }
    first = fast.GovernedCanaryAuthorizer(
        path,
        lifecycle_database=lifecycle,
        independent_verifier_state=verifier,
        consumption_database=consumption,
        hmac_key="test-secret",
    ).authorize(candidate, account_id="101-001-37981792-007")
    second = fast.GovernedCanaryAuthorizer(
        path,
        lifecycle_database=lifecycle,
        independent_verifier_state=verifier,
        consumption_database=consumption,
        hmac_key="test-secret",
    ).authorize(candidate, account_id="101-001-37981792-007")
    assert first["allowed"] is True
    assert second["allowed"] is False
    assert second["reason"] == "authorization_nonce_already_consumed"


def test_final_submission_rechecks_expiry_quote_and_signed_file(tmp_path):
    path = tmp_path / "authorization.json"
    payload = write_canary_authorization(
        path,
        entry_authorized=True,
        authorized_entries=[
            {
                "authorization_id": "canary-final",
                "one_time_nonce": "nonce-final",
                "account_id": "101-001-37981792-007",
                "signal_id": "signal-1",
                "proof_cohort_id": "proof-1",
                "governed_hypothesis_id": "hypothesis-1",
                "allocator_cohort_id": "allocator-1",
                "confirmed_candidate": True,
                "allowed_instrument": "EUR_USD",
                "allowed_direction": "buy",
                "maximum_units": 10,
                "maximum_notional": 1000.0,
                "maximum_total_exposure": 2000.0,
                "issued_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": datetime.fromtimestamp(time.time() + 60.0, timezone.utc).isoformat(),
            }
        ],
    )
    lifecycle = tmp_path / "lifecycle.sqlite"
    write_lifecycle_database(lifecycle)
    verifier = tmp_path / "verifier.json"
    write_independent_verifier(verifier)
    authorizer = fast.GovernedCanaryAuthorizer(
        path,
        lifecycle_database=lifecycle,
        independent_verifier_state=verifier,
        consumption_database=tmp_path / "consumptions.sqlite",
        hmac_key="test-secret",
    )
    candidate = {
        "id": "signal-1",
        "proof_cohort_id": "proof-1",
        "governed_hypothesis_id": "hypothesis-1",
        "allocator_cohort_id": "allocator-1",
        "instrument": "EUR_USD",
        "direction": "buy",
        "entry_time": datetime.now(timezone.utc).isoformat(),
    }
    decision = authorizer.authorize(candidate, account_id="101-001-37981792-007")
    executor = object.__new__(fast.GovernedPracticeExecutor)
    executor.canary_authorizer = authorizer
    candidate["_governed_canary_authorization"] = decision
    assert executor.final_submission_blocker(candidate, 10, {}) == ""
    stale = dict(candidate)
    stale["entry_time"] = datetime.fromtimestamp(time.time() - 30.0, timezone.utc).isoformat()
    assert executor.final_submission_blocker(stale, 10, {}) == "governed_canary_quote_stale_before_submission"
    payload["reason"] = "revoked_or_tampered"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert executor.final_submission_blocker(candidate, 10, {}) == "governed_canary_final_hmac_invalid"


def test_governed_executor_denial_never_calls_broker_submission(monkeypatch):
    calls = []
    monkeypatch.setattr(
        fast.lab.PracticeExecutor,
        "submit_selected_locked",
        lambda self, candidate: calls.append(candidate),
    )
    executor = object.__new__(fast.GovernedPracticeExecutor)
    executor.account_id = "101-001-37981792-007"
    executor.canary_authorizer = SimpleNamespace(
        authorize=lambda candidate, account_id: {
            "allowed": False,
            "reason": "zero_confirmed_candidates",
            "candidate_id": candidate["id"],
            "proof_cohort_id": "",
            "governed_hypothesis_id": "",
            "allocator_cohort_id": "",
        }
    )
    skips = []
    executor.log_execution_skip = lambda **fields: skips.append(fields)

    executor.submit_selected_locked({"id": "legacy-qualified-signal"})

    assert calls == []
    assert skips[0]["reason"] == "governed_canary_not_authorized"


def test_governed_portfolio_limit_waits_for_attached_authorization():
    executor = object.__new__(fast.GovernedPracticeExecutor)
    executor.args = SimpleNamespace(
        execution_max_open_positions=1,
        execution_max_currency_direction_positions=3,
    )

    candidate = {"instrument": "EUR_USD", "direction": "buy"}
    assert executor.portfolio_blocker(candidate, []) == ""

    candidate["_governed_canary_authorization"] = {
        "allowed": True,
        "maximum_total_exposure": 0.0,
    }
    assert (
        executor.portfolio_blocker(candidate, [])
        == "governed_canary_total_exposure_limit"
    )


def test_usable_price_map_rejects_invalid_quotes(monkeypatch):
    monkeypatch.setattr(
        fast.lab,
        "outcome_quote_rejection_reason",
        lambda quote: "bad" if quote == "bad" else "",
    )
    assert fast.usable_price_map(
        {"EUR_USD": "good", "USD_JPY": "bad"}
    ) == {"EUR_USD": "good"}


def test_research_quote_payload_contains_only_usable_quotes(monkeypatch):
    good = SimpleNamespace(
        bid=1.1,
        ask=1.1002,
        time="2026-07-31T17:00:00Z",
        source="rest",
    )
    monkeypatch.setattr(
        fast.lab,
        "outcome_quote_rejection_reason",
        lambda quote: "bad" if quote == "bad" else "",
    )
    monkeypatch.setattr(
        fast.lab,
        "utc_now",
        lambda: "2026-07-31T17:00:01+00:00",
    )

    payload = fast.research_quote_payload(
        {"EUR_USD": good, "USD_JPY": "bad"},
        {"EUR_USD": 0.0001, "USD_JPY": 0.01},
        "101-001-37981792-007",
    )

    assert payload["producer"] == "practice_007_fast_executor"
    assert payload["quote_count"] == 1
    assert payload["quotes"]["EUR_USD"]["pip"] == 0.0001
    assert "USD_JPY" not in payload["quotes"]


def test_healthy_stream_owns_quote_snapshot_publication():
    stream = object()

    assert fast.main_loop_owns_quote_snapshot(stream, True) is False
    assert fast.main_loop_owns_quote_snapshot(stream, False) is True
    assert fast.main_loop_owns_quote_snapshot(None, False) is True


def test_active_quote_transport_reports_stream_owned_publisher():
    result = fast.active_quote_transport_stats(
        {
            "connected": True,
            "research_snapshot_transport": {
                "enabled": True,
                "thread_alive": True,
                "last_success_age_sec": 0.2,
            },
        },
        None,
    )

    assert result == {
        "enabled": True,
        "thread_alive": True,
        "last_success_age_sec": 0.2,
        "mode": "price_stream_async_snapshot",
    }


def test_active_quote_transport_reports_disabled_when_unconfigured():
    assert fast.active_quote_transport_stats({}, None) == {
        "enabled": False,
        "mode": "disabled",
    }


def test_qualified_candidate_preview_is_compact_and_marks_conflict_gate():
    rows = fast.qualified_candidate_preview(
        [
            {
                "id": "candidate-1",
                "instrument": "USD_JPY",
                "direction": "buy",
                "family": "momentum",
                "execution_horizon_sec": 7200,
                "signal_eligible": True,
                "direction_conflict": True,
                "secret": "not-heartbeat-telemetry",
            },
            {
                "id": "candidate-2",
                "instrument": "EUR_USD",
                "direction": "sell",
                "signal_eligible": True,
                "direction_conflict": False,
            },
        ]
    )

    assert rows[0]["execution_permitted_after_conflict_gate"] is False
    assert rows[1]["execution_permitted_after_conflict_gate"] is True
    assert "secret" not in rows[0]


def test_price_stream_accepts_an_executor_snapshot_producer():
    stream = fast.lab.MultiPriceStream(
        lambda: ("token", "account"),
        ["EUR_USD"],
        lambda *args, **kwargs: None,
        research_snapshot_producer=(
            "practice_007_fast_executor_price_stream"
        ),
        research_snapshot_min_instruments=2,
    )

    assert (
        stream.research_snapshot_producer
        == "practice_007_fast_executor_price_stream"
    )
    assert stream.research_snapshot_min_instruments == 2


def test_price_stream_reports_broker_clock_lead_without_changing_quotes():
    stream = fast.lab.MultiPriceStream(
        lambda: ("token", "account"),
        ["EUR_USD"],
        lambda *args, **kwargs: None,
    )
    stream._broker_clock_leads_sec.extend([62.0, 63.0, 64.0])

    stats = stream.stats()

    assert stats["broker_clock_lead_sec"] == 63.0
    assert stats["broker_clock_sample_count"] == 3
    assert stats["clock_sync_status"] == "local_clock_behind_broker"


def test_research_snapshot_retains_seed_without_making_it_executable(tmp_path):
    output = tmp_path / "canonical_quotes.json"
    seed = tmp_path / "independent_quotes.json"
    seed.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_utc": "2026-08-08T12:00:00+00:00",
                "producer": "independent",
                "quote_count": 2,
                "quotes": {
                    "EUR_USD": {"bid": 1.1, "ask": 1.1002, "time": "2026-08-07T20:59:00Z", "pip": 0.0001},
                    "TRY_JPY": {"bid": 3.3, "ask": 3.311, "time": "2026-08-07T14:59:55Z", "pip": 0.01},
                },
            }
        ),
        encoding="utf-8",
    )
    stream = fast.lab.MultiPriceStream(
        lambda: ("token", "account"),
        ["EUR_USD", "TRY_JPY"],
        lambda *args, **kwargs: None,
        research_snapshot_path=output,
        research_snapshot_seed_paths=[seed],
    )
    try:
        generation = stream.publish_research_snapshot(
            {
                "schema_version": 1,
                "generated_utc": "2026-08-08T13:00:00+00:00",
                "producer": "executor",
                "quote_count": 1,
                "quotes": {
                    "EUR_USD": {"bid": 1.101, "ask": 1.1012, "time": "2026-08-07T21:00:00Z", "pip": 0.0001}
                },
            }
        )
        deadline = time.time() + 3.0
        while time.time() < deadline and stream._research_snapshot_publisher.stats()["written_generation"] < generation:
            time.sleep(0.01)
        payload = fast.lab.load_quote_snapshot(output)
    finally:
        stream.stop()

    assert stream.snapshot() == {}
    assert payload["quote_count"] == 2
    assert payload["coverage"]["current_quote_count"] == 1
    assert payload["coverage"]["retained_last_known_instruments"] == ["TRY_JPY"]
    assert payload["coverage"]["retained_quotes_execution_eligible"] is False


def test_initial_stream_snapshot_republishes_when_coverage_increases(tmp_path):
    stream = fast.lab.MultiPriceStream(
        lambda: ("token", "account"),
        ["EUR_USD", "USD_JPY"],
        lambda *args, **kwargs: None,
        research_snapshot_path=tmp_path / "quotes.json",
    )
    try:
        stream._last_research_snapshot_monotonic = time.monotonic()
        stream._last_research_snapshot_quote_count = 1
        stream._quotes = {"EUR_USD": object(), "USD_JPY": object()}

        assert stream._research_snapshot_due(time.monotonic()) is True
    finally:
        stream.stop()


def test_price_stream_reconnect_starts_clean_current_generation(tmp_path):
    output = tmp_path / "quotes.json"
    stream = fast.lab.MultiPriceStream(
        lambda: ("token", "account"),
        ["EUR_USD", "USD_JPY"],
        lambda *args, **kwargs: None,
        research_snapshot_path=output,
    )
    try:
        first_generation, first_reconnect = stream._activate_connection(now=1.0)
        stream._quotes = {
            "EUR_USD": SimpleNamespace(
                bid=1.1,
                ask=1.1002,
                time="2026-08-28T04:00:00Z",
                source="stream",
            ),
            "USD_JPY": SimpleNamespace(
                bid=150.0,
                ask=150.02,
                time="2026-08-28T04:00:01Z",
                source="stream",
            ),
        }
        stream._metadata = {
            "EUR_USD": {"connection_generation": first_generation},
            "USD_JPY": {"connection_generation": first_generation},
        }

        second_generation, second_reconnect = stream._activate_connection(now=2.0)
        generation = stream._publish_connection_boundary(
            second_generation,
            is_reconnect=second_reconnect,
        )
        deadline = time.time() + 3.0
        while (
            time.time() < deadline
            and stream._research_snapshot_publisher.stats()["written_generation"]
            < generation
        ):
            time.sleep(0.01)
        payload = fast.lab.load_quote_snapshot(output)
    finally:
        stream.stop()

    assert first_generation == 1
    assert first_reconnect is False
    assert second_generation == 2
    assert second_reconnect is True
    assert stream.snapshot() == {}
    assert stream.snapshot_metadata() == {}
    assert stream.stats()["reconnects"] == 1
    assert payload["coverage"]["current_quote_count"] == 0
    assert payload["coverage"]["retained_last_known_instruments"] == [
        "EUR_USD",
        "USD_JPY",
    ]
    assert payload["coverage"]["retained_quotes_execution_eligible"] is False


def test_price_stream_startup_reclassifies_seed_as_retained(tmp_path):
    output = tmp_path / "quotes.json"
    output.write_text(
        json.dumps(
            {
                "generated_utc": "2026-08-28T03:59:00Z",
                "producer": "prior_process",
                "quotes": {
                    "EUR_USD": {
                        "bid": 1.1,
                        "ask": 1.1002,
                        "pip": 0.0001,
                        "time": "2026-08-28T03:58:59Z",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    stream = fast.lab.MultiPriceStream(
        lambda: ("token", "account"),
        ["EUR_USD"],
        lambda *args, **kwargs: None,
        research_snapshot_path=output,
    )
    try:
        connection_generation, is_reconnect = stream._activate_connection(now=1.0)
        generation = stream._publish_connection_boundary(
            connection_generation,
            is_reconnect=is_reconnect,
        )
        deadline = time.time() + 3.0
        while (
            time.time() < deadline
            and stream._research_snapshot_publisher.stats()["written_generation"]
            < generation
        ):
            time.sleep(0.01)
        payload = fast.lab.load_quote_snapshot(output)
    finally:
        stream.stop()

    assert is_reconnect is False
    assert payload["coverage"]["current_quote_count"] == 0
    assert payload["coverage"]["retained_last_known_instruments"] == ["EUR_USD"]


def test_cycle_error_is_published_to_heartbeat_immediately(monkeypatch, tmp_path):
    updates = []
    logged = []
    heartbeat = SimpleNamespace(update=lambda **fields: updates.append(fields))
    monkeypatch.setattr(fast.lab, "utc_now", lambda: "2026-08-03T00:29:00+00:00")
    monkeypatch.setattr(
        fast.lab,
        "log_line",
        lambda path, event, **fields: logged.append((path, event, fields)),
    )

    payload = fast.report_cycle_error(
        heartbeat,
        tmp_path / "executor.jsonl",
        3,
        RuntimeError("pricing unavailable"),
    )

    assert payload["kind"] == "RuntimeError"
    assert updates == [
        {
            "phase": "streaming",
            "session_errors": 3,
            "last_error": payload,
        }
    ]
    assert logged[0][1] == "fast_executor_cycle_error"


def test_slow_cycle_snapshot_survives_normal_cycle(monkeypatch):
    monkeypatch.setattr(
        fast.lab,
        "utc_now",
        lambda: "2026-08-03T04:32:26+00:00",
    )
    slow = fast.capture_slow_cycle(
        None,
        cycle_ms=136_547.892,
        cycle_timings={"selection_ms": 136_000.0},
        rank_timings={"feed_recent_sec": 130.0},
        priced_instruments=68,
        usable_priced_instruments=0,
    )
    retained = fast.capture_slow_cycle(
        slow,
        cycle_ms=392.514,
        cycle_timings={"selection_ms": 327.776},
        rank_timings={"feed_recent_sec": 0.2},
        priced_instruments=68,
        usable_priced_instruments=64,
    )

    assert retained is slow
    assert slow["cycle_ms"] == 136_547.892
    assert slow["rank_timings"]["feed_recent_sec"] == 130.0
    assert slow["usable_priced_instruments"] == 0


def test_build_promotion_loads_existing_state(monkeypatch):
    observed = {}

    class FakePromotion:
        def __init__(self, database, state, horizons, **kwargs):
            observed["database"] = database
            observed["state"] = state
            observed["horizons"] = horizons
            observed["kwargs"] = kwargs

        def load_state(self):
            observed["loaded"] = True

    monkeypatch.setattr(fast.lab, "LanePromotionModel", FakePromotion)
    monkeypatch.setattr(
        fast.lab,
        "PromotionThresholds",
        lambda **kwargs: kwargs,
    )
    args = SimpleNamespace(
        promotion_database="promotion.sqlite",
        promotion_state="promotion.json",
        execution_horizons=[60, 300],
        execution_min_samples=30,
        execution_min_average_pips=0.1,
        execution_min_median_pips=0.0,
        execution_min_win_rate=52.0,
        execution_min_lower_confidence_pips=0.0,
        execution_min_independent_blocks=12,
        execution_min_holdout_blocks=4,
        execution_min_pairs=3,
        execution_min_sessions=2,
        execution_min_segment_samples=10,
        promotion_refresh_sec=300.0,
    )
    result = fast.build_promotion(args)
    assert isinstance(result, FakePromotion)
    assert observed["loaded"] is True
    assert observed["horizons"] == [60, 300]


def test_fast_prefilter_keeps_full_evidence_for_seeded_instrument():
    candidates = [
        {
            "id": "seed",
            "instrument": "EUR_USD",
            "account_eligible": True,
            "research_only": False,
            "signal_role": "structural",
            "preconsensus_class": "accepted",
        },
        {
            "id": "timing",
            "instrument": "EUR_USD",
            "account_eligible": False,
            "research_only": True,
            "signal_role": "entry_exit_timing",
        },
        {
            "id": "unseeded",
            "instrument": "USD_JPY",
            "account_eligible": False,
            "research_only": True,
            "signal_role": "structural",
        },
    ]
    filtered = fast.lab.PracticeExecutor.execution_candidate_prefilter(
        candidates
    )
    assert [row["id"] for row in filtered] == ["seed", "timing"]


def test_fast_prefilter_fails_closed_without_structural_seed():
    assert (
        fast.lab.PracticeExecutor.execution_candidate_prefilter(
            [
                {
                    "instrument": "EUR_USD",
                    "account_eligible": False,
                    "research_only": True,
                    "signal_role": "entry_exit_timing",
                }
            ]
        )
        == []
    )


def test_fast_prefilter_excludes_partial_cycle_shadow_from_rank_surface():
    candidates = [
        {
            "id": "seed",
            "instrument": "EUR_USD",
            "account_eligible": True,
            "research_only": False,
            "signal_role": "structural",
            "preconsensus_class": "accepted",
        },
        {
            "id": "partial",
            "instrument": "EUR_USD",
            "account_eligible": False,
            "research_only": True,
            "signal_role": "structural",
            "source_kind": "partial_cycle_shadow",
        },
    ]
    filtered = fast.lab.PracticeExecutor.execution_candidate_prefilter(
        candidates
    )
    assert [row["id"] for row in filtered] == ["seed"]


def test_fast_prefilter_deduplicates_repeated_feed_generations():
    candidates = [
        {
            "id": "seed-old",
            "instrument": "EUR_USD",
            "direction": "buy",
            "lane_id": "momentum.fast",
            "family": "momentum",
            "input_timeframe": "M5",
            "signal_role": "structural",
            "account_eligible": True,
            "research_only": False,
            "preconsensus_class": "accepted",
            "feed_published_epoch": 100.0,
        },
        {
            "id": "seed-new",
            "instrument": "EUR_USD",
            "direction": "buy",
            "lane_id": "momentum.fast",
            "family": "momentum",
            "input_timeframe": "M5",
            "signal_role": "structural",
            "account_eligible": True,
            "research_only": False,
            "preconsensus_class": "accepted",
            "feed_published_epoch": 110.0,
        },
        {
            "id": "timing-old",
            "instrument": "EUR_USD",
            "direction": "sell",
            "lane_id": "micro.timing",
            "family": "microstructure",
            "input_timeframe": "M1",
            "signal_role": "entry_exit_timing",
            "account_eligible": False,
            "research_only": True,
            "feed_published_epoch": 100.0,
        },
        {
            "id": "timing-new",
            "instrument": "EUR_USD",
            "direction": "sell",
            "lane_id": "micro.timing",
            "family": "microstructure",
            "input_timeframe": "M1",
            "signal_role": "entry_exit_timing",
            "account_eligible": False,
            "research_only": True,
            "feed_published_epoch": 110.0,
        },
    ]

    filtered = fast.lab.PracticeExecutor.execution_candidate_prefilter(candidates)

    assert [row["id"] for row in filtered] == ["seed-new", "timing-new"]


def test_execution_feed_is_read_light_and_records_audit(tmp_path):
    path = tmp_path / "feed.sqlite"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE candidates(
            candidate_id TEXT PRIMARY KEY,
            published_epoch REAL NOT NULL,
            expires_epoch REAL NOT NULL,
            source TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE executions(
            client_id TEXT PRIMARY KEY,
            candidate_id TEXT NOT NULL,
            submitted_epoch REAL NOT NULL,
            status TEXT NOT NULL,
            trade_id TEXT,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE contributor_registry(
            contributor_id TEXT PRIMARY KEY,
            expected INTEGER NOT NULL,
            account_eligible INTEGER NOT NULL,
            last_seen_epoch REAL
        );
        """
    )
    now = time.time()
    connection.execute(
        "INSERT INTO candidates VALUES (?, ?, ?, ?, ?)",
        (
            "candidate",
            now,
            now + 60,
            "test",
            json.dumps({"id": "candidate", "instrument": "EUR_USD"}),
        ),
    )
    connection.execute(
        "INSERT INTO contributor_registry VALUES (?, ?, ?, ?)",
        ("test", 1, 1, now),
    )
    connection.commit()
    connection.close()

    feed = fast.ExecutionSignalFeed(path)
    try:
        assert feed.connection.execute(
            "PRAGMA wal_autocheckpoint"
        ).fetchone()[0] == 0
        assert feed.recent(10)[0]["id"] == "candidate"
        assert feed.coverage()["fresh_contributors"] == 1
        feed.record_execution(
            "client",
            {"id": "candidate"},
            "filled",
            "trade",
        )
        assert feed.seconds_since_last_fill() < 1.0
    finally:
        feed.close()
