from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess

import pytest

import oanda_official_event_paired_evaluator_v1 as subject


UTC = dt.timezone.utc
EVENT = dt.datetime(2026, 8, 30, 19, 10, tzinfo=UTC)
REAL_VALIDATE_DEPENDENCY_SOURCES = subject.validate_dependency_sources


def frozen_news_source_contract() -> tuple[bytes, dict[str, tuple[str, str]]]:
    raw = subprocess.check_output(
        [
            "git",
            "cat-file",
            "blob",
            "8e115984965e8e952048cf4bb323560e5c496f43:config/news_sources_v1.json",
        ],
        cwd=Path(__file__).resolve().parent,
        stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        timeout=15,
    )
    assert hashlib.sha256(raw).hexdigest() == subject.REQUIRED_NEWS_SOURCE_CONFIG_SHA256
    payload = json.loads(raw.decode("utf-8"))
    lineages = {
        str(row["source_id"]): subject._configured_source_lineage(row)
        for row in payload.get("sources") or []
        if isinstance(row, dict) and row.get("source_id")
    }
    assert lineages
    return raw, lineages


@pytest.fixture(autouse=True)
def bind_frozen_news_source_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    raw, lineages = frozen_news_source_contract()
    monkeypatch.setattr(
        subject,
        "validate_news_source_config",
        lambda path=subject.NEWS_SOURCE_CONFIG_PATH: (raw, lineages),
    )
    monkeypatch.setattr(
        subject,
        "validate_dependency_sources",
        lambda: (
            subject.REQUIRED_FAST_LANE_SOURCE_SHA256,
            subject.REQUIRED_HORIZON_CAPTURE_SOURCE_SHA256,
        ),
    )


def test_cohort_b_uses_isolated_paths_and_preserves_a_sentinel(tmp_path: Path):
    cohort_a = tmp_path / "official_event_paired_evaluator_v1.sqlite"
    cohort_a.write_bytes(b"immutable-cohort-a-zero-evidence-baseline")
    before = hashlib.sha256(cohort_a.read_bytes()).hexdigest()
    cohort_b = tmp_path / "official_event_paired_evaluator_v1_20260830b.sqlite"

    connection = subject.open_database(cohort_b)
    try:
        manifest = dict(
            connection.execute(
                "SELECT * FROM paired_event_cohort_manifest"
            ).fetchone()
        )
    finally:
        connection.close()

    assert subject.COHORT_ID == "official_event_paired_evaluator_v1_20260830b"
    assert subject.OUTPUT_DATABASE.name.endswith("_20260830b.sqlite")
    assert subject.STATE_PATH.name.endswith("_20260830b.json")
    assert subject.HEARTBEAT_PATH.name.endswith("_20260830b.json")
    assert manifest["cohort_id"] == subject.COHORT_ID
    assert hashlib.sha256(cohort_a.read_bytes()).hexdigest() == before


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


DEFAULT_SOURCE_PAYLOAD = {
    "source_id": "fed_speeches",
    "source_currencies": ["USD"],
    "currencies": ["USD", "JPY", "GBP"],
    "source_verified": True,
    "source_direct": True,
    "title": "Official policy statement",
}
DEFAULT_SOURCE_BYTES = canonical(DEFAULT_SOURCE_PAYLOAD).encode()
DEFAULT_MATERIAL_SHA256 = hashlib.sha256(DEFAULT_SOURCE_BYTES).hexdigest()
DEFAULT_OBSERVATION_ID = hashlib.sha256(
    f"fed_speeches|item-1|{DEFAULT_MATERIAL_SHA256}".encode()
).hexdigest()
FED_SOURCE_LINEAGE = (
    "derived_source_config_lineage_v1:fed_speeches:373e70aba73d9d006fc98b84"
)


def quote_universe(*, cheapest="EUR_USD"):
    output = {}
    for index, instrument in enumerate(subject.EXPECTED_INSTRUMENTS):
        pip = 0.01 if instrument.endswith("_JPY") else 0.0001
        bid = 1.0 + index * pip * 10
        spread = 0.8 if instrument == cheapest else 2.0
        output[instrument] = {
            "bid": bid,
            "ask": bid + spread * pip,
            "pip": pip,
            "quote_time_utc": subject.iso_utc(EVENT),
            "age_seconds": 2.0,
            "event_offset_seconds": 0.0,
            "source": "fixture_stream",
        }
    return output


def capture_row(*, observation_id=DEFAULT_OBSERVATION_ID, cheapest="EUR_USD", event=EVENT):
    payload = {
        "schema_version": "official_release_raw_quote_capture_v1",
        "capture_contract_id": subject.REQUIRED_ENTRY_CONTRACT_ID,
        "capture_cohort_id": subject.REQUIRED_ENTRY_COHORT_ID,
        "observation_id": observation_id,
        "event_first_known_utc": subject.iso_utc(event),
        "captured_utc": subject.iso_utc(event + dt.timedelta(seconds=2)),
        "capture_latency_seconds": 2.0,
        "capture_activation_eligible": True,
        "input_prospective_observation": True,
        "timing_quality": "prospective_exact_live_quote",
        "proof_quote_count": subject.EXPECTED_INSTRUMENT_COUNT,
        "instrument_universe_sha256": subject.EXPECTED_UNIVERSE_SHA256,
        "quotes": quote_universe(cheapest=cheapest),
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_promote": False,
    }
    capture_id = hashlib.sha256(
        f"{observation_id}|{subject.REQUIRED_ENTRY_CONTRACT_ID}|{subject.REQUIRED_ENTRY_COHORT_ID}".encode()
    ).hexdigest()
    return {
        "capture_id": capture_id,
        "observation_id": observation_id,
        "event_first_known_utc": subject.iso_utc(event),
        "captured_utc": subject.iso_utc(event + dt.timedelta(seconds=2)),
        "input_prospective_observation": 1,
        "capture_activation_eligible": 1,
        "timing_quality": "prospective_exact_live_quote",
        "observed_valid_quote_count": subject.EXPECTED_INSTRUMENT_COUNT,
        "proof_quote_count": subject.EXPECTED_INSTRUMENT_COUNT,
        "capture_payload_json": canonical(payload),
        "research_only": 1,
        "execution_eligible": 0,
        "can_authorize": 0,
        "can_promote": 0,
        "capture_contract_id": subject.REQUIRED_ENTRY_CONTRACT_ID,
        "capture_cohort_id": subject.REQUIRED_ENTRY_COHORT_ID,
        "capture_activated_utc": subject.iso_utc(subject.ACTIVATED_UTC),
    }


def source_row(*, observation_id=DEFAULT_OBSERVATION_ID, issuer="USD"):
    payload = {
        "source_id": "fed_speeches",
        "source_currencies": [issuer],
        "currencies": [issuer, "JPY", "GBP"],
        "source_verified": True,
        "source_direct": True,
        "title": "Official policy statement",
    }
    return {
        "observation_id": observation_id,
        "source_id": "fed_speeches",
        "source_contract_id": FED_SOURCE_LINEAGE,
        "source_cohort_id": FED_SOURCE_LINEAGE,
        "item_key": "item-1",
        "material_sha256": hashlib.sha256(canonical(payload).encode()).hexdigest(),
        "first_seen_utc": subject.iso_utc(EVENT),
        "prospective_observation": 1,
        "listing_bootstrap": 0,
        "identity_preexisting": 0,
        "publisher_time_eligible": 1,
        "observation_clock_trusted": 1,
        "observation_clock_source": "synchronized_host",
        "raw_payload_json": canonical(payload),
        "research_only": 1,
        "execution_eligible": 0,
        "can_authorize": 0,
        "collector_contract_id": subject.REQUIRED_RAW_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": subject.REQUIRED_RAW_COLLECTOR_COHORT_ID,
    }


def mapping_row(
    *, observation_id=DEFAULT_OBSERVATION_ID, issuer="USD", score=0.7,
    mapped_sec=10, classification_version=None,
):
    classification = classification_version or subject.REQUIRED_CLASSIFICATION_VERSION
    payload = {
        "fast_lane_observation_id": observation_id,
        "source_id": "fed_speeches",
        "source_currencies": [issuer],
        "currencies": [issuer, "JPY", "GBP"],
        "currency_scores": {issuer: score, "JPY": -1.0, "GBP": 1.0},
        "mentioned_currency_entities": ["JPY", "GBP"],
        "classification_version": classification,
    }
    return {
        "mapping_id": hashlib.sha256(
            (
                f"{observation_id}|{classification}|"
                f"{subject.REQUIRED_MAPPING_CONTRACT_ID}"
            ).encode()
        ).hexdigest(),
        "observation_id": observation_id,
        "source_id": "fed_speeches",
        "source_contract_id": FED_SOURCE_LINEAGE,
        "first_seen_utc": subject.iso_utc(EVENT),
        "input_prospective_observation": 1,
        "input_listing_bootstrap": 0,
        "input_publisher_time_eligible": 1,
        "classification_version": classification,
        "semantic_direction_available": int(score != 0),
        "prospective_semantic_candidate": int(score != 0),
        "publish_eligible_forward_candidate": 0,
        "forward_shadow_candidate": 0,
        "mapping_payload_json": canonical(payload),
        "mapped_utc": subject.iso_utc(EVENT + dt.timedelta(seconds=mapped_sec)),
        "research_only": 1,
        "execution_eligible": 0,
        "can_authorize": 0,
        "can_promote": 0,
        "mapper_contract_id": subject.REQUIRED_MAPPING_CONTRACT_ID,
        "mapper_cohort_id": subject.REQUIRED_MAPPING_COHORT_ID,
    }


def feature_snapshot(*, instrument="EUR_USD", rising=False, generated=EVENT - dt.timedelta(seconds=30)):
    starts = [EVENT - dt.timedelta(minutes=21 - index) for index in range(20)]
    closes = [1.10 + (index if rising else -index) * 0.0001 for index in range(20)]
    payload = {
        "schema_version": 1,
        "snapshot_id": "feature-t0",
        "generated_utc": subject.iso_utc(generated),
        "instrument_count": 1,
        "contract": {
            "feature_values_observable_at_generation": True,
            "historical_outcomes_included": False,
            "intrahour_forecast": {"completed_bars_only": True},
        },
        "instruments": {
            instrument: {
                "feature_origin_utc": subject.iso_utc(starts[-1]),
                "structural_series": {
                    "M1": {
                        "bar_start_times_utc": [subject.iso_utc(value) for value in starts],
                        "close": closes,
                    }
                },
            }
        },
    }
    return canonical(payload).encode(), payload


def valid_decision(*, rising=False, issued_sec=20, cheapest="EUR_USD", score=0.7):
    feature_bytes, feature = feature_snapshot(instrument=cheapest, rising=rising)
    return subject.build_decision(
        capture_row=capture_row(cheapest=cheapest),
        source_row=source_row(),
        mapping_row=mapping_row(score=score),
        feature_snapshot_bytes=feature_bytes,
        feature_snapshot=feature,
        issued_utc=EVENT + dt.timedelta(seconds=issued_sec),
        config_bytes=subject.CONFIG_PATH.read_bytes(),
    )


def test_frozen_config_and_safety_contract_are_exact():
    payload = subject.validate_frozen_config()
    assert payload["arms"] == list(subject.ARMS)
    assert payload["horizons_min"] == list(subject.HORIZONS_MIN)
    assert payload["round_trip_slippage_pips"] == list(subject.SLIPPAGE_STRESS_PIPS)
    assert payload["policy"]["supported_execution_decision"] == "no_trade"
    assert hashlib.sha256(subject.CONFIG_PATH.read_bytes()).hexdigest() == subject.FROZEN_CONFIG_FILE_SHA256


def test_lowest_entry_spread_pair_is_deterministic_and_outcome_blind():
    quotes = quote_universe(cheapest="EUR_USD")
    # Exact tie is resolved lexicographically, not by later outcomes.
    quotes["GBP_USD"]["ask"] = quotes["GBP_USD"]["bid"] + 0.8 * quotes["GBP_USD"]["pip"]
    instrument, row, reason = subject.select_lowest_spread_pair(quotes, "USD")
    assert (instrument, reason) == ("EUR_USD", "")
    assert row == quotes["EUR_USD"]


def test_issuer_binding_ignores_mentioned_foreign_currency_scores():
    decision, arms = valid_decision(rising=False)
    assert decision["decision_state"] == "sealed"
    assert decision["issuer_currency"] == "USD"
    assert decision["selected_instrument"] == "EUR_USD"
    # USD strengthens => EUR/USD sell. JPY/GBP scores never create legs.
    assert decision["official_pair_side"] == "sell"
    assert {row["arm_name"] for row in arms} == set(subject.ARMS)


def test_technical_can_confirm_or_veto_but_never_reverse_official():
    confirmed, arms = valid_decision(rising=False)
    assert confirmed["technical_pair_side"] == "sell"
    by_name = {row["arm_name"]: row for row in arms}
    assert by_name["official_plus_technical_confirmation"]["side"] == "sell"
    conflicted, conflict_arms = valid_decision(rising=True)
    assert conflicted["technical_pair_side"] == "buy"
    conflict = {row["arm_name"]: row for row in conflict_arms}[
        "official_plus_technical_confirmation"
    ]
    assert (conflict["action_state"], conflict["side"], conflict["reason"]) == (
        "abstain", "abstain", "technical_veto_direction_conflict"
    )


def test_noncausal_technical_snapshot_is_terminal_abstention():
    feature_bytes, feature = feature_snapshot(generated=EVENT + dt.timedelta(seconds=1))
    decision, arms = subject.build_decision(
        capture_row=capture_row(), source_row=source_row(), mapping_row=mapping_row(),
        feature_snapshot_bytes=feature_bytes, feature_snapshot=feature,
        issued_utc=EVENT + dt.timedelta(seconds=20),
    )
    assert decision["decision_state"] == "sealed"
    assert decision["technical_pair_side"] == ""
    assert decision["technical_reason"] == "technical_snapshot_postdates_event_t0"
    by_name = {row["arm_name"]: row for row in arms}
    assert by_name["price_only"]["action_state"] == "abstain"
    assert by_name["official_plus_technical_confirmation"]["action_state"] == "abstain"


def test_out_of_order_or_stale_completed_bars_terminally_abstain():
    feature_bytes, feature = feature_snapshot()
    times = feature["instruments"]["EUR_USD"]["structural_series"]["M1"][
        "bar_start_times_utc"
    ]
    times[-1], times[-2] = times[-2], times[-1]
    mutated = canonical(feature).encode()
    decision, _ = subject.build_decision(
        capture_row=capture_row(), source_row=source_row(), mapping_row=mapping_row(),
        feature_snapshot_bytes=mutated, feature_snapshot=feature,
        issued_utc=EVENT + dt.timedelta(seconds=20),
    )
    assert decision["technical_pair_side"] == ""
    assert decision["technical_reason"] == "technical_completed_bar_clocks_not_strictly_increasing"

    stale_bytes, stale = feature_snapshot(generated=EVENT)
    stale_times = stale["instruments"]["EUR_USD"]["structural_series"]["M1"][
        "bar_start_times_utc"
    ]
    stale["instruments"]["EUR_USD"]["structural_series"]["M1"][
        "bar_start_times_utc"
    ] = [
        subject.iso_utc(subject.parse_time(value) - dt.timedelta(minutes=10))
        for value in stale_times
    ]
    stale_bytes = canonical(stale).encode()
    stale_decision, _ = subject.build_decision(
        capture_row=capture_row(), source_row=source_row(), mapping_row=mapping_row(),
        feature_snapshot_bytes=stale_bytes, feature_snapshot=stale,
        issued_utc=EVENT + dt.timedelta(seconds=20),
    )
    assert stale_decision["technical_reason"] == "technical_last_completed_bar_stale_at_event_t0"


def test_late_decision_is_terminal_invalid_but_no_trade_remains_explicit():
    decision, arms = valid_decision(issued_sec=56)
    assert decision["decision_state"] == "invalid"
    assert "decision_not_sealed_before_first_horizon" in decision["invalid_reason"]
    by_name = {row["arm_name"]: row for row in arms}
    assert by_name["official_source_only"]["action_state"] == "invalid"
    assert by_name["no_trade"]["action_state"] == "abstain"


def test_entry_capture_latency_is_derived_and_cannot_be_self_certified():
    before = capture_row()
    before_payload = json.loads(before["capture_payload_json"])
    before["captured_utc"] = subject.iso_utc(EVENT - dt.timedelta(seconds=1))
    before_payload["captured_utc"] = before["captured_utc"]
    before_payload["capture_latency_seconds"] = -1.0
    before["capture_payload_json"] = canonical(before_payload)
    decision, _ = subject.build_decision(
        capture_row=before, source_row=source_row(), mapping_row=mapping_row(),
        feature_snapshot_bytes=b"", feature_snapshot={},
        issued_utc=EVENT + dt.timedelta(seconds=20),
    )
    assert decision["decision_state"] == "invalid"
    assert "entry_capture_latency_invalid" in decision["invalid_reason"]

    mismatched = capture_row()
    mismatched_payload = json.loads(mismatched["capture_payload_json"])
    mismatched_payload["capture_latency_seconds"] = 1.0
    mismatched["capture_payload_json"] = canonical(mismatched_payload)
    mismatch_decision, _ = subject.build_decision(
        capture_row=mismatched, source_row=source_row(), mapping_row=mapping_row(),
        feature_snapshot_bytes=b"", feature_snapshot={},
        issued_utc=EVENT + dt.timedelta(seconds=20),
    )
    assert "entry_capture_latency_invalid" in mismatch_decision["invalid_reason"]


def test_source_contract_and_cohort_are_frozen_together():
    mixed = source_row()
    mixed["source_cohort_id"] = "same-source-new-unregistered-cohort"
    decision, _ = subject.build_decision(
        capture_row=capture_row(), source_row=mixed, mapping_row=mapping_row(),
        feature_snapshot_bytes=b"", feature_snapshot={},
        issued_utc=EVENT + dt.timedelta(seconds=20),
    )
    assert decision["decision_state"] == "invalid"
    assert "raw_source_contract_or_cohort_mismatch" in decision["invalid_reason"]


def test_dependency_limit_drift_is_rejected_before_evaluation(
    monkeypatch: pytest.MonkeyPatch,
):
    current_fast = Path(subject.fast_lane.__file__).read_bytes()
    current_horizon = Path(subject.horizon_v1.__file__).read_bytes()
    real_sha256_bytes = subject.sha256_bytes

    def frozen_dependency_hashes(value: bytes) -> str:
        if value == current_fast:
            return subject.REQUIRED_FAST_LANE_SOURCE_SHA256
        if value == current_horizon:
            return subject.REQUIRED_HORIZON_CAPTURE_SOURCE_SHA256
        return real_sha256_bytes(value)

    monkeypatch.setattr(subject, "sha256_bytes", frozen_dependency_hashes)
    monkeypatch.setattr(
        subject.fast_lane, "QUOTE_CAPTURE_MAX_QUOTE_AGE_SECONDS", 31.0
    )
    with pytest.raises(ValueError, match="dependency timing limits changed"):
        REAL_VALIDATE_DEPENDENCY_SOURCES()


def test_decision_copies_all_input_material_and_seals_full_schedule():
    decision, _ = valid_decision()
    assert decision["decision_latency_sec"] == 20.0
    assert len(json.loads(decision["outcome_schedule_json"])) == 75
    for name in (
        "input_capture", "input_capture_row", "entry_component", "source_payload",
        "source_row", "authority_map", "mapping_payload", "mapping_row",
        "technical_material",
    ):
        raw = decision[f"{name}_bytes"]
        assert hashlib.sha256(raw).hexdigest() == decision[f"{name}_sha256"]
    assert "not_proof_of_order_submission" in decision["entry_economics_role"]


def test_signed_factor_deduplicates_by_issuer_not_selected_pair():
    one = subject._factor_id("EUR_USD", "sell", "USD")
    two = subject._factor_id("USD_JPY", "buy", "USD")
    assert one == two
    assert one
    assert subject._factor_id("USD_JPY", "sell", "USD") != one


def test_append_only_decision_and_arms_cannot_be_rewritten(tmp_path: Path):
    database = tmp_path / "paired.sqlite"
    connection = subject.open_database(database)
    try:
        decision, arms = valid_decision()
        assert subject.insert_decision(connection, decision, arms)
        assert not subject.insert_decision(connection, decision, arms)
        assert connection.execute("SELECT COUNT(*) FROM paired_event_arm").fetchone()[0] == 5
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("UPDATE paired_event_decision SET decision_state='invalid'")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM paired_event_arm")
    finally:
        connection.close()


def horizon_fixture(decision, *, horizon=1, exit_bid=None, exit_ask=None, exit_pip=None):
    pip = float(decision["entry_pip"])
    bid = float(decision["entry_bid"]) - 5 * pip if exit_bid is None else exit_bid
    ask = bid + 1.0 * pip if exit_ask is None else exit_ask
    target = EVENT + dt.timedelta(minutes=horizon)
    read_started = target + dt.timedelta(seconds=1)
    attempted = target + dt.timedelta(seconds=2)
    component = {
        "instrument": decision["selected_instrument"],
        "bid": bid,
        "ask": ask,
        "pip": pip if exit_pip is None else exit_pip,
        "quote_utc": subject.iso_utc(target),
        "quote_age_sec": 2.0,
        "target_offset_sec": 0.0,
        "spread_pips": 1.0,
        "source": "fixture_stream",
        "connection_generation": 1,
    }
    component_json = canonical(component)
    entry_capture = json.loads(decision["input_capture_bytes"].decode())
    all_components = {}
    for instrument, entry in entry_capture["quotes"].items():
        instrument_pip = float(entry["pip"])
        instrument_bid = float(entry["bid"])
        instrument_ask = float(entry["ask"])
        all_components[instrument] = {
            "instrument": instrument,
            "bid": instrument_bid,
            "ask": instrument_ask,
            "pip": instrument_pip,
            "quote_utc": subject.iso_utc(target),
            "quote_age_sec": 2.0,
            "target_offset_sec": 0.0,
            "spread_pips": (instrument_ask - instrument_bid) / instrument_pip,
            "source": "fixture_stream",
            "connection_generation": 1,
        }
    all_components[decision["selected_instrument"]] = component
    horizon_capture_id = "official_event_horizon_" + hashlib.sha256(
        f"{decision['input_capture_id']}|{horizon}|{subject.REQUIRED_HORIZON_CONTRACT_ID}|{subject.REQUIRED_HORIZON_COHORT_ID}".encode()
    ).hexdigest()[:32]
    component_hashes = [
        hashlib.sha256(canonical(all_components[instrument]).encode()).hexdigest()
        for instrument in sorted(all_components)
    ]
    component_root = hashlib.sha256("".join(component_hashes).encode()).hexdigest()
    snapshot_material = {"fixture": "causal-exact-live-quote"}
    snapshot_hash = hashlib.sha256(canonical(snapshot_material).encode()).hexdigest()
    payload = {
        "capture_id": horizon_capture_id,
        "input_capture_id": decision["input_capture_id"],
        "observation_id": decision["observation_id"],
        "event_first_known_utc": decision["event_first_known_utc"],
        "horizon_min": horizon,
        "target_utc": subject.iso_utc(target),
        "capture_read_started_utc": subject.iso_utc(read_started),
        "attempted_utc": subject.iso_utc(attempted),
        "attempt_delay_sec": 2.0,
        "timing_quality": "prospective_exact_live_quote",
        "proof_quote_count": subject.EXPECTED_INSTRUMENT_COUNT,
        "quotes": all_components,
        "connection_generation": 1,
        "quote_snapshot_generated_utc": subject.iso_utc(attempted),
        "quote_snapshot_material": snapshot_material,
        "quote_snapshot_sha256": snapshot_hash,
        "component_root_sha256": component_root,
        "expected_instrument_count": subject.EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": subject.EXPECTED_UNIVERSE_SHA256,
        "entry_capture_contract_id": subject.REQUIRED_ENTRY_CONTRACT_ID,
        "entry_capture_cohort_id": subject.REQUIRED_ENTRY_COHORT_ID,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "contract_id": subject.REQUIRED_HORIZON_CONTRACT_ID,
        "cohort_id": subject.REQUIRED_HORIZON_COHORT_ID,
    }
    payload_json = canonical(payload)
    capture = {
        "capture_id": horizon_capture_id,
        "input_capture_id": decision["input_capture_id"],
        "observation_id": decision["observation_id"],
        "event_first_known_utc": decision["event_first_known_utc"],
        "horizon_min": horizon,
        "target_utc": subject.iso_utc(target),
        "capture_read_started_utc": subject.iso_utc(read_started),
        "attempted_utc": subject.iso_utc(attempted),
        "attempt_delay_sec": 2.0,
        "timing_quality": "prospective_exact_live_quote",
        "proof_quote_count": subject.EXPECTED_INSTRUMENT_COUNT,
        "input_capture_payload_sha256": decision["input_capture_sha256"],
        "connection_generation": 1,
        "quote_snapshot_generated_utc": subject.iso_utc(attempted),
        "quote_snapshot_sha256": snapshot_hash,
        "component_root_sha256": component_root,
        "payload_json": payload_json,
        "payload_sha256": hashlib.sha256(payload_json.encode()).hexdigest(),
        "research_only": 1,
        "execution_eligible": 0,
        "can_place_orders": 0,
        "can_authorize": 0,
        "can_promote": 0,
        "contract_id": subject.REQUIRED_HORIZON_CONTRACT_ID,
        "cohort_id": subject.REQUIRED_HORIZON_COHORT_ID,
    }
    quote = {
        "quote_id": "official_event_horizon_quote_" + hashlib.sha256(
            (
                f"{capture['capture_id']}|{decision['selected_instrument']}|"
                f"{hashlib.sha256(component_json.encode()).hexdigest()}"
            ).encode()
        ).hexdigest()[:32],
        "capture_id": capture["capture_id"],
        "horizon_min": horizon,
        "instrument": decision["selected_instrument"],
        "bid": component["bid"],
        "ask": component["ask"],
        "pip": component["pip"],
        "quote_utc": component["quote_utc"],
        "quote_age_sec": component["quote_age_sec"],
        "target_offset_sec": component["target_offset_sec"],
        "spread_pips": component["spread_pips"],
        "source": component["source"],
        "connection_generation": component["connection_generation"],
        "payload_json": component_json,
        "payload_sha256": hashlib.sha256(component_json.encode()).hexdigest(),
        "research_only": 1,
        "execution_eligible": 0,
        "can_place_orders": 0,
        "can_authorize": 0,
        "can_promote": 0,
        "contract_id": subject.REQUIRED_HORIZON_CONTRACT_ID,
        "cohort_id": subject.REQUIRED_HORIZON_COHORT_ID,
    }
    return capture, quote


def test_exact_executable_side_math_and_slippage_cartesian():
    decision, arms = valid_decision(rising=False)
    capture, quote = horizon_fixture(decision)
    horizon_input = subject._horizon_input(decision, capture, quote)
    assert horizon_input["input_state"] == "valid"
    outcomes = subject._outcome_rows(decision, arms, horizon_input)
    assert len(outcomes) == 15
    official = [row for row in outcomes if row["arm_name"] == "official_source_only"]
    assert [row["slippage_stress_pips"] for row in official] == [0.0, 0.25, 0.5]
    assert official[0]["gross_executable_pips"] == pytest.approx(4.0)
    assert official[2]["net_after_cost_pips"] == pytest.approx(3.5)
    no_trade = [row for row in outcomes if row["arm_name"] == "no_trade"]
    assert all(row["net_after_cost_pips"] == 0.0 for row in no_trade)


def test_exit_pip_drift_is_invalid_not_silently_priced():
    decision, arms = valid_decision()
    capture, quote = horizon_fixture(decision, exit_pip=decision["entry_pip"] * 10)
    horizon_input = subject._horizon_input(decision, capture, quote)
    assert horizon_input["input_state"] == "invalid"
    assert "selected_exit_pip_mismatch" in horizon_input["invalid_reason"]
    outcomes = subject._outcome_rows(decision, arms, horizon_input)
    trade_rows = [row for row in outcomes if row["arm_name"] == "official_source_only"]
    assert all(row["outcome_state"] == "invalid" for row in trade_rows)


def test_horizon_header_component_or_hash_tamper_fails_closed():
    decision, _ = valid_decision()
    capture, quote = horizon_fixture(decision)
    quote["payload_sha256"] = "0" * 64
    horizon_input = subject._horizon_input(decision, capture, quote)
    assert horizon_input["input_state"] == "invalid"
    assert "selected_exit_component_hash_mismatch" in horizon_input["invalid_reason"]


def test_horizon_self_hashed_header_cannot_override_row_lineage():
    decision, _ = valid_decision()
    capture, quote = horizon_fixture(decision)
    capture["observation_id"] = "different-observation"
    horizon_input = subject._horizon_input(decision, capture, quote)
    assert horizon_input["input_state"] == "invalid"
    assert "horizon_row_decision_lineage_mismatch" in horizon_input["invalid_reason"]


def test_horizon_backfill_clock_forgery_fails_despite_historical_quote_time():
    decision, _ = valid_decision()
    capture, quote = horizon_fixture(decision)
    payload = json.loads(capture["payload_json"])
    late = EVENT + dt.timedelta(minutes=2)
    payload["capture_read_started_utc"] = subject.iso_utc(late)
    payload["attempted_utc"] = subject.iso_utc(late)
    payload["attempt_delay_sec"] = 60.0
    capture["capture_read_started_utc"] = subject.iso_utc(late)
    capture["attempted_utc"] = subject.iso_utc(late)
    capture["attempt_delay_sec"] = 60.0
    capture["payload_json"] = canonical(payload)
    capture["payload_sha256"] = hashlib.sha256(
        capture["payload_json"].encode()
    ).hexdigest()
    horizon_input = subject._horizon_input(decision, capture, quote)
    assert horizon_input["input_state"] == "invalid"
    assert "horizon_capture_attempt_clock_invalid" in horizon_input["invalid_reason"]


def test_five_horizons_append_exact_75_row_cartesian_once(tmp_path: Path):
    connection = subject.open_database(tmp_path / "paired.sqlite")
    try:
        decision, arms = valid_decision(rising=False)
        assert subject.insert_decision(connection, decision, arms)
        stored_decision = dict(
            connection.execute("SELECT * FROM paired_event_decision").fetchone()
        )
        stored_arms = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM paired_event_arm ORDER BY arm_name"
            ).fetchall()
        ]
        for horizon in subject.HORIZONS_MIN:
            capture, quote = horizon_fixture(stored_decision, horizon=horizon)
            horizon_input = subject._horizon_input(stored_decision, capture, quote)
            outcomes = subject._outcome_rows(
                stored_decision, stored_arms, horizon_input
            )
            assert len(outcomes) == 15
            assert subject.insert_horizon_and_outcomes(
                connection, horizon_input, outcomes
            )
            assert not subject.insert_horizon_and_outcomes(
                connection, horizon_input, outcomes
            )
        assert connection.execute(
            "SELECT COUNT(*) FROM paired_event_horizon_input"
        ).fetchone()[0] == 5
        assert connection.execute(
            "SELECT COUNT(*) FROM paired_event_outcome"
        ).fetchone()[0] == 75
        assert connection.execute(
            "SELECT COUNT(*) FROM paired_event_outcome "
            "WHERE arm_name='no_trade' AND net_after_cost_pips=0.0"
        ).fetchone()[0] == 15
    finally:
        connection.close()


def _create_table(connection, name, row):
    columns = []
    for key, value in row.items():
        sql_type = "REAL" if isinstance(value, float) else "INTEGER" if isinstance(value, int) else "TEXT"
        columns.append(f'"{key}" {sql_type}')
    connection.execute(f'CREATE TABLE "{name}" ({",".join(columns)})')
    connection.execute(
        f'INSERT INTO "{name}" ({",".join(chr(34)+key+chr(34) for key in row)}) VALUES ({",".join("?" for _ in row)})',
        tuple(row.values()),
    )
    connection.commit()


def _insert_row(connection, name, row):
    connection.execute(
        f'INSERT INTO "{name}" ({",".join(chr(34)+key+chr(34) for key in row)}) '
        f'VALUES ({",".join("?" for _ in row)})',
        tuple(row.values()),
    )
    connection.commit()


def test_maturity_worker_consumes_only_exact_horizon_rows(tmp_path: Path):
    output = subject.open_database(tmp_path / "output.sqlite")
    horizon_path = tmp_path / "horizon.sqlite"
    upstream = sqlite3.connect(horizon_path)
    try:
        decision, arms = valid_decision(rising=False)
        assert subject.insert_decision(output, decision, arms)
        capture, quote = horizon_fixture(decision)
        _create_table(upstream, "official_event_horizon_capture", capture)
        _create_table(upstream, "official_event_horizon_quote", quote)
        upstream.close()
        result = subject.mature_available_outcomes(
            output, horizon_database=horizon_path
        )
        assert result["inserted_horizons"] == 1
        assert result["inserted_outcomes"] == 15
        assert subject.mature_available_outcomes(
            output, horizon_database=horizon_path
        )["duplicates"] == 1
    finally:
        try:
            upstream.close()
        except Exception:
            pass
        output.close()


def test_missing_mapping_waits_then_becomes_explicit_terminal_invalid(tmp_path: Path):
    release_path = tmp_path / "release.sqlite"
    mapping_path = tmp_path / "mapping.sqlite"
    release = sqlite3.connect(release_path)
    _create_table(release, "official_release_quote_capture", capture_row())
    _create_table(release, "official_release_observation", source_row())
    release.close()
    mapping = sqlite3.connect(mapping_path)
    sample = mapping_row()
    _create_table(mapping, "official_release_mapping", sample)
    mapping.execute("DELETE FROM official_release_mapping")
    mapping.commit()
    mapping.close()
    output = subject.open_database(tmp_path / "output.sqlite")
    try:
        pending = subject.seal_due_decisions(
            output, release_database=release_path, mapping_database=mapping_path,
            feature_snapshot_path=tmp_path / "missing.json",
            now=EVENT + dt.timedelta(seconds=40),
        )
        assert pending["pending_mapping"] == 1
        assert subject.database_counts(output)["decisions"] == 0
        terminal = subject.seal_due_decisions(
            output, release_database=release_path, mapping_database=mapping_path,
            feature_snapshot_path=tmp_path / "missing.json",
            now=EVENT + dt.timedelta(seconds=56),
        )
        assert terminal["invalid"] == 1
        row = output.execute("SELECT * FROM paired_event_decision").fetchone()
        assert "mapping_copy_missing_by_seal_deadline" in row["invalid_reason"]
        assert output.execute("SELECT COUNT(*) FROM paired_event_arm").fetchone()[0] == 5
    finally:
        output.close()


def test_pre_activation_capture_is_not_backfilled(tmp_path: Path):
    release_path = tmp_path / "release.sqlite"
    mapping_path = tmp_path / "mapping.sqlite"
    old = EVENT - dt.timedelta(days=1)
    release = sqlite3.connect(release_path)
    _create_table(release, "official_release_quote_capture", capture_row(event=old))
    _create_table(release, "official_release_observation", source_row())
    release.close()
    mapping = sqlite3.connect(mapping_path)
    _create_table(mapping, "official_release_mapping", mapping_row())
    mapping.close()
    output = subject.open_database(tmp_path / "output.sqlite")
    try:
        result = subject.seal_due_decisions(
            output, release_database=release_path, mapping_database=mapping_path,
            feature_snapshot_path=tmp_path / "missing.json", now=EVENT,
        )
        assert result["eligible_captures"] == 0
        assert subject.database_counts(output)["decisions"] == 0
    finally:
        output.close()


def test_sealed_technical_abstention_cannot_attach_later(tmp_path: Path):
    connection = subject.open_database(tmp_path / "paired.sqlite")
    try:
        empty_decision, empty_arms = subject.build_decision(
            capture_row=capture_row(), source_row=source_row(), mapping_row=mapping_row(),
            feature_snapshot_bytes=b"", feature_snapshot={},
            issued_utc=EVENT + dt.timedelta(seconds=20),
        )
        assert subject.insert_decision(connection, empty_decision, empty_arms)
        later_decision, later_arms = valid_decision()
        assert later_decision["technical_pair_side"] == "sell"
        assert not subject.insert_decision(connection, later_decision, later_arms)
        stored = connection.execute(
            "SELECT action_state,reason FROM paired_event_arm WHERE arm_name='price_only'"
        ).fetchone()
        assert stored["action_state"] == "abstain"
        assert stored["reason"] == "technical_snapshot_missing"
    finally:
        connection.close()


def test_mapping_query_selects_only_frozen_classification_version(tmp_path: Path):
    release_path = tmp_path / "release.sqlite"
    mapping_path = tmp_path / "mapping.sqlite"
    release = sqlite3.connect(release_path)
    _create_table(release, "official_release_quote_capture", capture_row())
    _create_table(release, "official_release_observation", source_row())
    release.close()
    mapping = sqlite3.connect(mapping_path)
    wrong = mapping_row(
        mapped_sec=5,
        classification_version="local_fx_news_rules_20260830_v999_not_frozen",
    )
    required = mapping_row(mapped_sec=10)
    _create_table(mapping, "official_release_mapping", wrong)
    _insert_row(mapping, "official_release_mapping", required)
    mapping.close()
    output = subject.open_database(tmp_path / "output.sqlite")
    try:
        result = subject.seal_due_decisions(
            output,
            release_database=release_path,
            mapping_database=mapping_path,
            feature_snapshot_path=tmp_path / "missing.json",
            now=EVENT + dt.timedelta(seconds=20),
        )
        assert result["health_state"] == "healthy"
        assert result["sealed"] == 1
        stored = output.execute(
            "SELECT mapping_id,mapping_row_bytes FROM paired_event_decision"
        ).fetchone()
        assert stored["mapping_id"] == required["mapping_id"]
        copied = json.loads(bytes(stored["mapping_row_bytes"]).decode())
        assert copied["classification_version"] == subject.REQUIRED_CLASSIFICATION_VERSION
    finally:
        output.close()


def test_manifest_is_append_only_and_rejects_identity_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    database = tmp_path / "paired.sqlite"
    connection = subject.open_database(database)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE paired_event_cohort_manifest SET cohort_id='changed'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM paired_event_cohort_manifest")
    finally:
        connection.close()

    with monkeypatch.context() as scoped:
        scoped.setattr(subject, "script_sha256", lambda: "f" * 64)
        with pytest.raises(ValueError, match="manifest identity mismatch"):
            subject.open_database(database)

    altered_config = tmp_path / "altered_config.json"
    altered_config.write_bytes(subject.CONFIG_PATH.read_bytes() + b"\n")
    with monkeypatch.context() as scoped:
        scoped.setattr(subject, "CONFIG_PATH", altered_config)
        scoped.setattr(subject, "validate_frozen_config", lambda path=altered_config: {})
        with pytest.raises(ValueError, match="manifest identity mismatch"):
            subject.open_database(database)

    with monkeypatch.context() as scoped:
        original_bytes, original_payload = subject.validate_authority_map()
        scoped.setattr(
            subject,
            "validate_authority_map",
            lambda path=subject.AUTHORITY_MAP_PATH: (original_bytes + b" ", original_payload),
        )
        with pytest.raises(ValueError, match="manifest identity mismatch"):
            subject.open_database(database)


def test_insert_api_rejects_same_cohort_hash_contamination(tmp_path: Path):
    connection = subject.open_database(tmp_path / "paired.sqlite")
    try:
        decision, arms = valid_decision()
        contaminated = dict(decision)
        contaminated["producer_source_sha256"] = "b" * 64
        with pytest.raises(ValueError, match="decision cohort identity mismatch"):
            subject.insert_decision(connection, contaminated, arms)
        assert subject.database_counts(connection)["decisions"] == 0
    finally:
        connection.close()


def test_fresh_clock_after_write_lock_turns_slow_cycle_into_invalid(
    tmp_path: Path,
):
    release_path = tmp_path / "release.sqlite"
    mapping_path = tmp_path / "mapping.sqlite"
    release = sqlite3.connect(release_path)
    _create_table(release, "official_release_quote_capture", capture_row())
    _create_table(release, "official_release_observation", source_row())
    release.close()
    mapping = sqlite3.connect(mapping_path)
    _create_table(mapping, "official_release_mapping", mapping_row())
    mapping.close()
    output = subject.open_database(tmp_path / "output.sqlite")
    clocks = iter(
        [
            EVENT + dt.timedelta(seconds=54),  # causal reads start
            EVENT + dt.timedelta(seconds=56),  # sampled only after write lock
        ]
    )
    try:
        result = subject.seal_due_decisions(
            output,
            release_database=release_path,
            mapping_database=mapping_path,
            feature_snapshot_path=tmp_path / "missing.json",
            clock=lambda: next(clocks),
        )
        assert result["invalid"] == 1
        stored = output.execute(
            "SELECT decision_read_started_utc,decision_precommit_utc,issued_utc,"
            "invalid_reason FROM paired_event_decision"
        ).fetchone()
        assert subject.parse_time(stored["decision_read_started_utc"]) == EVENT + dt.timedelta(seconds=54)
        assert subject.parse_time(stored["decision_precommit_utc"]) == EVENT + dt.timedelta(seconds=56)
        assert stored["decision_precommit_utc"] == stored["issued_utc"]
        assert "decision_not_sealed_before_first_horizon" in stored["invalid_reason"]
    finally:
        output.close()


def test_missing_and_corrupt_inputs_publish_degraded_health(tmp_path: Path):
    corrupt = tmp_path / "corrupt.sqlite"
    corrupt.write_bytes(b"not-a-sqlite-database")
    missing_health = subject.inspect_sqlite_input(
        tmp_path / "missing.sqlite", subject.RELEASE_REQUIRED_SCHEMA
    )
    corrupt_health = subject.inspect_sqlite_input(
        corrupt, subject.RELEASE_REQUIRED_SCHEMA
    )
    assert missing_health["state"] == "unhealthy"
    assert "database_missing" in missing_health["errors"]
    assert corrupt_health["state"] == "unhealthy"
    assert any("sqlite_error" in item for item in corrupt_health["errors"])

    locked = tmp_path / "locked.sqlite"
    locker = sqlite3.connect(locked)
    locker.execute("PRAGMA journal_mode=DELETE")
    locker.execute("CREATE TABLE official_release_mapping (mapping_id TEXT)")
    locker.commit()
    locker.execute("BEGIN EXCLUSIVE")
    try:
        locked_health = subject.inspect_sqlite_input(
            locked, subject.MAPPING_REQUIRED_SCHEMA
        )
        assert locked_health["state"] == "unhealthy"
        assert any("database is locked" in item for item in locked_health["errors"])
    finally:
        locker.rollback()
        locker.close()

    payload = subject.run_cycle(
        output_database=tmp_path / "output.sqlite",
        release_database=tmp_path / "missing-release.sqlite",
        mapping_database=tmp_path / "missing-mapping.sqlite",
        horizon_database=tmp_path / "missing-horizon.sqlite",
        feature_snapshot_path=tmp_path / "missing-features.json",
        state_path=tmp_path / "state.json",
        heartbeat_path=tmp_path / "heartbeat.json",
        now=EVENT + dt.timedelta(seconds=20),
    )
    assert payload["status"] == "degraded"
    assert payload["decision_cycle"]["health_state"] == "degraded"
    assert payload["outcome_cycle"]["health_state"] == "degraded"
    assert json.loads((tmp_path / "heartbeat.json").read_text())["status"] == "degraded"


def test_atomic_json_publish_recovers_from_transient_windows_access_denied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "state.json"
    real_replace = subject.os.replace
    attempts = 0
    sleeps: list[float] = []

    def transient_replace(source: Path, destination: Path) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError(5, "Access is denied")
        real_replace(source, destination)

    monkeypatch.setattr(subject.os, "replace", transient_replace)
    monkeypatch.setattr(subject.time, "sleep", sleeps.append)

    subject.write_json_atomic(target, {"status": "ok", "attempt": 3})

    assert attempts == 3
    assert sleeps == [0.01, 0.02]
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "attempt": 3,
        "status": "ok",
    }
    assert list(tmp_path.glob(".state.json.*.tmp")) == []


def test_atomic_json_publish_raises_after_bounded_windows_failure_and_cleans_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "heartbeat.json"
    target.write_text('{"status":"previous"}\n', encoding="utf-8")
    attempts = 0
    sleeps: list[float] = []

    def permanently_denied(source: Path, destination: Path) -> None:
        del source, destination
        nonlocal attempts
        attempts += 1
        raise PermissionError(5, "Access is denied")

    monkeypatch.setattr(subject.os, "replace", permanently_denied)
    monkeypatch.setattr(subject.time, "sleep", sleeps.append)

    with pytest.raises(PermissionError, match="Access is denied"):
        subject.write_json_atomic(target, {"status": "new"})

    assert attempts == 8
    assert sleeps == [0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.5]
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "status": "previous"
    }
    assert list(tmp_path.glob(".heartbeat.json.*.tmp")) == []
