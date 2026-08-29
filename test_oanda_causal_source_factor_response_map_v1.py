from __future__ import annotations

import ast
import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_causal_source_factor_response_map_v1 as subject


UTC = timezone.utc


def _payload(
    *,
    currency: str = "JPY",
    headline: str = "Policy release",
    url: str = "https://central.example/releases/a",
    published: str = "2026-08-27T14:30:00+00:00",
    **updates,
) -> dict:
    payload = {
        "source_id": "central_feed",
        "source_name": "Central Example",
        "source_kind": "rss",
        "source_role": "primary_policy_release",
        "source_direct": True,
        "source_verified": True,
        "source_native_currency_bound": True,
        "source_currencies": [currency],
        "direct_currencies": [currency],
        "headline": headline,
        "source_url": url,
        "published_utc": published,
        "category": "monetary_policy",
        "topic_action": "hold",
        "topic_signature": f"monetary_policy|{currency}|hold",
        "transmission_mechanisms": ["rate_expectations"],
        "currency_scores": {},
        "research_currency_scores": {},
    }
    payload.update(updates)
    return payload


def _observation(
    mapping_id: str,
    first_seen: datetime,
    payload: dict,
    *,
    source_id: str = "central_feed",
) -> dict:
    return {
        "mapping_id": mapping_id,
        "observation_id": f"obs_{mapping_id}",
        "source_id": source_id,
        "source_contract_id": f"contract_{source_id}",
        "first_seen_utc": subject.iso(first_seen),
        "mapped_utc": subject.iso(first_seen + timedelta(seconds=4)),
        "classification_version": subject.REQUIRED_CLASSIFICATION_VERSION,
        "mapper_contract_id": subject.REQUIRED_MAPPER_CONTRACT,
        "mapper_cohort_id": "mapper_cohort",
        "material_sha256": f"sha_{mapping_id}",
        "observation_clock_source": "immutable_collector_first_seen",
        "collector_contract_id": "collector_contract",
        "collector_cohort_id": "collector_cohort",
        "raw_payload_hash": f"raw_{mapping_id}",
        "mapping_payload": payload,
        "currencies": list(payload["source_currencies"]),
    }


def _create_input_databases(mapping_path: Path, raw_path: Path) -> None:
    mapping = sqlite3.connect(mapping_path)
    mapping.execute(
        """
        CREATE TABLE official_release_mapping (
          mapping_id TEXT PRIMARY KEY, observation_id TEXT, source_id TEXT,
          source_contract_id TEXT, first_seen_utc TEXT,
          input_prospective_observation INTEGER, input_listing_bootstrap INTEGER,
          classification_version TEXT, mapping_payload_json TEXT, mapped_utc TEXT,
          mapper_contract_id TEXT, mapper_cohort_id TEXT
        )
        """
    )
    mapping.commit()
    mapping.close()
    raw = sqlite3.connect(raw_path)
    raw.execute(
        """
        CREATE TABLE official_release_observation (
          observation_id TEXT PRIMARY KEY, material_sha256 TEXT,
          first_seen_utc TEXT, prospective_observation INTEGER,
          listing_bootstrap INTEGER, observation_clock_trusted INTEGER,
          observation_clock_source TEXT, collector_contract_id TEXT,
          collector_cohort_id TEXT, raw_payload_json TEXT
        )
        """
    )
    raw.commit()
    raw.close()


def _insert_input(
    mapping_path: Path,
    raw_path: Path,
    *,
    mapping_id: str,
    observation_id: str,
    first_seen: datetime,
    payload: dict,
    classifier: str = subject.REQUIRED_CLASSIFICATION_VERSION,
    mapper_contract: str = subject.REQUIRED_MAPPER_CONTRACT,
    prospective: int = 1,
    bootstrap: int = 0,
    clock_trusted: int = 1,
) -> None:
    mapping = sqlite3.connect(mapping_path)
    mapping.execute(
        "INSERT INTO official_release_mapping VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            mapping_id,
            observation_id,
            payload.get("source_id", "central_feed"),
            "source_contract",
            subject.iso(first_seen),
            prospective,
            bootstrap,
            classifier,
            json.dumps(payload),
            subject.iso(first_seen + timedelta(seconds=5)),
            mapper_contract,
            "mapper_cohort",
        ),
    )
    mapping.commit()
    mapping.close()
    raw = sqlite3.connect(raw_path)
    raw.execute(
        "INSERT OR IGNORE INTO official_release_observation VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            observation_id,
            f"sha_{observation_id}",
            subject.iso(first_seen),
            prospective,
            bootstrap,
            clock_trusted,
            "immutable_first_seen",
            "collector_contract",
            "collector_cohort",
            json.dumps({"headline": payload["headline"]}),
        ),
    )
    raw.commit()
    raw.close()


def _event(
    event_id: str,
    at: datetime,
    *,
    currency: str = "USD",
    eligible: bool = False,
    authority: str = "central_example",
    factor_key: str = "category:monetary_policy",
    market_episode: str | None = None,
    factor_known: datetime | None = None,
) -> dict:
    evidence = "prospective_v1" if eligible else "preactivation_diagnostic"
    return {
        "canonical_event_id": event_id,
        "market_episode_id": market_episode or f"episode_{event_id}",
        "currency": currency,
        "first_known_utc": subject.iso(at),
        "evidence_class": evidence,
        "prospective_proof_eligible": eligible,
        "headline": event_id,
        "source_url": "",
        "authority": authority,
        "category": "monetary_policy",
        "event_series_id": "",
        "topic_signature": "",
        "source_role": "primary_policy_release",
        "source_facts": {"topic_action": "hold"},
        "transport_count": 0,
        "transport_observations": [],
        "factors": [
            {
                "factor_key": factor_key,
                "factor_type": factor_key.split(":", 1)[0],
                "factor_value": factor_key.split(":", 1)[1],
                "factor_known_utc": subject.iso(factor_known or at),
            }
        ],
    }


def _response(
    event_id: str,
    at: datetime,
    value: float,
    horizon: int = 5,
    currency: str = "USD",
) -> dict:
    return {
        "response_id": f"response_{event_id}_{horizon}",
        "canonical_event_id": event_id,
        "currency": currency,
        "horizon_min": horizon,
        "event_clock_utc": subject.iso(at),
        "maturity_utc": subject.iso(at + timedelta(minutes=horizon)),
        "maturity_state": "valid_canonical_ls_factor_and_bid_ask_paths",
        "currency_factor_bps": value,
        "absolute_currency_factor_bps": abs(value),
        "currency_rank": 1 if value > 0 else 4,
        "factor_currency_count": 4,
        "usable_pair_count": 6,
        "currency_pair_path_count": 3,
        "entry_method": "preactivation_m1_approximation",
        "response_timing_quality": "diagnostic_approximate_not_proof",
        "early_currency_factor_bps": value / 2,
        "response_shape": "immediate",
        "solver_status": "ok",
        "solver_condition_number": 2.0,
        "solver_weighted_observation_equivalent": 5.0,
        "solver_diagnostics": {"status": "ok", "observation_count": 6},
        "both_executable_paths": [],
        "selected_side": None,
    }


def _complete_panel(at: datetime) -> dict[str, list[subject.CandlePoint]]:
    pairs = ("EUR_USD", "GBP_USD", "USD_JPY", "EUR_GBP", "EUR_JPY", "GBP_JPY")
    panel = {}
    for index, instrument in enumerate(pairs):
        pip = subject.pip_size(instrument)
        middle = 150.0 if instrument.endswith("_JPY") else 1.10 + index * 0.01
        panel[instrument] = [
            subject.CandlePoint(
                at,
                middle - pip,
                middle + pip,
                middle - pip,
                middle + pip,
                middle + 3 * pip,
                middle - 3 * pip,
                middle + 3 * pip,
                middle - 3 * pip,
            ),
            subject.CandlePoint(
                at + timedelta(minutes=1),
                middle - pip,
                middle + pip,
                middle + 2 * pip,
                middle + 4 * pip,
                middle + 5 * pip,
                middle - 3 * pip,
                middle + 6 * pip,
                middle - 3 * pip,
            ),
        ]
    return panel


def _write_m1_fixture(
    path: Path,
    start: datetime,
    count: int,
    *,
    instrument: str = "EUR_USD",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "time",
        "datetime",
        "instrument",
        "granularity",
        "open",
        "high",
        "low",
        "close",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
        "spread_pips",
        "volume",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for index in range(count):
            stamp = start + timedelta(minutes=index)
            middle = 1.10 + index * 0.000001
            writer.writerow(
                {
                    "time": subject.iso(stamp),
                    "datetime": subject.iso(stamp),
                    "instrument": instrument,
                    "granularity": "M1",
                    "open": middle,
                    "high": middle + 0.0002,
                    "low": middle - 0.0002,
                    "close": middle + 0.0001,
                    "bid_open": middle - 0.00005,
                    "bid_high": middle + 0.00015,
                    "bid_low": middle - 0.00025,
                    "bid_close": middle + 0.00005,
                    "ask_open": middle + 0.00005,
                    "ask_high": middle + 0.00025,
                    "ask_low": middle - 0.00015,
                    "ask_close": middle + 0.00015,
                    "spread_pips": 1.0,
                    "volume": 10 + index,
                }
            )


def test_zero_score_verified_direct_input_is_retained_and_old_classifier_excluded(tmp_path):
    mapping_path = tmp_path / "mapping.sqlite"
    raw_path = tmp_path / "raw.sqlite"
    _create_input_databases(mapping_path, raw_path)
    clock = datetime(2026, 8, 27, 14, 40, tzinfo=UTC)
    payload = _payload()
    _insert_input(
        mapping_path,
        raw_path,
        mapping_id="current",
        observation_id="same_observation",
        first_seen=clock,
        payload=payload,
    )
    _insert_input(
        mapping_path,
        raw_path,
        mapping_id="old_classifier",
        observation_id="same_observation",
        first_seen=clock,
        payload=payload,
        classifier="old_classifier",
    )
    _insert_input(
        mapping_path,
        raw_path,
        mapping_id="bootstrap",
        observation_id="bootstrap_obs",
        first_seen=clock,
        payload=payload,
        bootstrap=1,
    )
    rows = subject.load_current_observations(mapping_path, raw_path)
    assert [row["mapping_id"] for row in rows] == ["current"]
    assert rows[0]["mapping_payload"]["currency_scores"] == {}


def test_parallel_transports_with_different_urls_collapse_but_are_preserved():
    at = datetime(2026, 8, 27, 14, 40, tzinfo=UTC)
    first = _observation(
        "a",
        at,
        _payload(url="https://central.example/feed/release-a", event_lineage_id="lineage_a"),
        source_id="transport_a",
    )
    second = _observation(
        "b",
        at + timedelta(seconds=40),
        _payload(url="https://mirror.other.example/press/other-url", event_lineage_id="lineage_b"),
        source_id="transport_b",
    )
    events = subject.canonicalize_observations([first, second])
    assert len(events) == 1
    assert events[0]["transport_count"] == 2
    assert len(events[0]["transport_observations"]) == 2
    factor_keys = [row["factor_key"] for row in events[0]["factors"]]
    assert len(factor_keys) == len(set(factor_keys))


def test_policy_decision_documents_share_episode_but_later_decisions_do_not():
    decision = datetime(2026, 9, 16, 18, 0, tzinfo=UTC)
    documents = (
        ("Federal Reserve issues FOMC statement", "rate_decision", 0),
        ("Summary of Economic Projections", "projections", 1),
        ("Implementation Note issued September 16, 2026", "implementation_note", 3),
        ("Transcript of the Chair's press conference", "press_conference_transcript", 35),
    )
    observations = []
    for index, (headline, document_type, delay_min) in enumerate(documents):
        published = decision + timedelta(minutes=delay_min)
        observations.append(
            _observation(
                f"decision_doc_{index}",
                published + timedelta(seconds=2),
                _payload(
                    currency="USD",
                    headline=headline,
                    url=f"https://official-{index}.example/document/{index}",
                    published=subject.iso(published),
                    scheduled_utc=subject.iso(decision),
                    policy_document_type=document_type,
                    event_family=document_type,
                    official_policy_release=True,
                ),
                source_id=f"official_transport_{index}",
            )
        )

    events = subject.canonicalize_observations(
        observations, activation_utc=decision - timedelta(hours=1)
    )
    assert len(events) == 4  # Documents remain separately inspectable events.
    assert len({event["canonical_event_id"] for event in events}) == 4
    assert len({event["market_episode_id"] for event in events}) == 1

    # Current fast-mapper content often lacks an exact scheduled clock. The
    # conservative publication-window fallback still collapses the documents.
    fallback_ids = {
        subject.market_episode_id(
            _payload(
                currency="USD",
                headline=headline,
                published=subject.iso(decision + timedelta(minutes=delay_min)),
                policy_document_type=document_type,
                event_family=document_type,
                official_policy_release=True,
            ),
            "USD",
            first_known_utc=decision + timedelta(minutes=delay_min, seconds=2),
        )
        for headline, document_type, delay_min in documents
    }
    assert len(fallback_ids) == 1

    later_decision = decision + timedelta(days=42)
    later_id = subject.market_episode_id(
        _payload(
            currency="USD",
            headline="Federal Reserve issues FOMC statement",
            published=subject.iso(later_decision),
            scheduled_utc=subject.iso(later_decision),
            policy_document_type="rate_decision",
            official_policy_release=True,
        ),
        "USD",
        first_known_utc=later_decision,
    )
    assert later_id != events[0]["market_episode_id"]

    second_scheduled_clock = decision + timedelta(hours=2)
    second_same_day_id = subject.market_episode_id(
        _payload(
            currency="USD",
            headline="Federal Reserve issues a separate policy decision",
            published=subject.iso(second_scheduled_clock),
            scheduled_utc=subject.iso(second_scheduled_clock),
            policy_document_type="rate_decision",
            official_policy_release=True,
        ),
        "USD",
        first_known_utc=second_scheduled_clock,
    )
    assert second_same_day_id != events[0]["market_episode_id"]

    # A different release family at the same clock remains a separate episode.
    inflation_id = subject.market_episode_id(
        _payload(
            currency="USD",
            headline="Consumer Price Index",
            published=subject.iso(decision),
            scheduled_utc=subject.iso(decision),
            category="inflation_release",
            event_name="Consumer Price Index",
            event_series_id="cpi",
            policy_document_type="",
            official_policy_release=False,
        ),
        "USD",
        first_known_utc=decision,
    )
    assert inflation_id != events[0]["market_episode_id"]

    # A genuine upstream episode ID remains shared across currency expressions.
    upstream_usd = _payload(currency="USD", economic_episode_id="fomc-2026-09-16")
    upstream_eur = _payload(currency="EUR", economic_episode_id="fomc-2026-09-16")
    assert subject.market_episode_id(upstream_usd, "USD") == subject.market_episode_id(
        upstream_eur, "EUR"
    )


def test_activation_boundary_and_later_numeric_clock_are_strict():
    before = subject.ACTIVATED_UTC - timedelta(seconds=1)
    after = subject.ACTIVATED_UTC + timedelta(seconds=1)
    numeric_known = after + timedelta(minutes=2)
    old = _observation("old", before, _payload(headline="old", published="2026-08-27T14:34:00Z"))
    new_payload = _payload(
        headline="new",
        published="2026-08-27T14:35:00Z",
        actual_value=4.0,
        previous_value=3.0,
        consensus_value=2.0,
        consensus_capture_state="post_release_retrieval",
        numeric_causal_known_utc=subject.iso(numeric_known),
    )
    new = _observation("new", after, new_payload)
    events = subject.canonicalize_observations([old, new])
    assert events[0]["evidence_class"] == "preactivation_diagnostic"
    assert events[1]["evidence_class"] == "prospective_v1"
    factors = {row["factor_key"]: row for row in events[1]["factors"]}
    assert factors["actual_vs_previous:up"]["factor_known_utc"] == subject.iso(numeric_known)
    assert not any(key.startswith("surprise:") for key in factors)
    assert events[1]["source_facts"]["actual_value"] == 4.0


def test_causal_pre_release_consensus_creates_surprise_factor():
    at = subject.ACTIVATED_UTC + timedelta(minutes=1)
    release = at + timedelta(minutes=4)
    payload = _payload(
        actual_value=5,
        previous_value=3,
        consensus_value=4,
        consensus_capture_state="causal_pre_release_snapshot",
        consensus_observed_at_utc=subject.iso(at),
        scheduled_utc=subject.iso(release),
        consensus_source_provenance="licensed_calendar_snapshot_hash",
    )
    factors = subject.structured_factors(payload, at)
    assert "surprise:up" in {row["factor_key"] for row in factors}
    payload["consensus_observed_at_utc"] = subject.iso(release + timedelta(seconds=1))
    factors = subject.structured_factors(payload, at)
    assert "surprise:up" not in {row["factor_key"] for row in factors}


def test_live_entry_snapshot_is_exact_only_inside_clock_and_quote_gate():
    at = subject.ACTIVATED_UTC + timedelta(minutes=1)
    event = _event("live", at, eligible=True, currency="USD")
    quotes = {}
    for index in range(60):
        instrument = f"A{index:02d}_USD"
        quotes[instrument] = {
            "bid": 1.0,
            "ask": 1.0002,
            "pip": 0.0001,
            "time": subject.iso(at + timedelta(seconds=5)),
        }
    valid = subject.build_live_entry_snapshot(
        event, {"quotes": quotes}, {"updated_at": subject.iso(at)}, at + timedelta(seconds=10)
    )
    assert valid["timing_quality"] == "prospective_exact_live_quote"
    assert valid["quote_count"] == 60
    missed = subject.build_live_entry_snapshot(
        event, {"quotes": quotes}, {}, at + timedelta(seconds=100)
    )
    assert missed["timing_quality"] == "prospective_clock_missed"
    assert missed["quotes"] == {}
    offset_89_quotes = {
        key: {**value, "time": subject.iso(at + timedelta(seconds=89))}
        for key, value in quotes.items()
    }
    offset_89 = subject.build_live_entry_snapshot(
        event,
        {"quotes": offset_89_quotes},
        {},
        at + timedelta(seconds=89),
    )
    assert offset_89["timing_quality"] != "prospective_exact_live_quote"


def _attached_pre_map_snapshot(
    at: datetime, *, captured_after_seconds: int
) -> dict:
    quotes = {
        f"A{index:02d}_USD": {
            "bid": 1.0,
            "ask": 1.0002,
            "pip": 0.0001,
            "quote_time_utc": subject.iso(at + timedelta(seconds=5)),
        }
        for index in range(60)
    }
    return {
        "capture_contract_id": subject.REQUIRED_PRE_MAP_QUOTE_CONTRACT,
        "observation_id": "obs_pre_map",
        "event_first_known_utc": subject.iso(at),
        "captured_utc": subject.iso(
            at + timedelta(seconds=captured_after_seconds)
        ),
        "capture_latency_seconds": float(captured_after_seconds),
        "timing_quality": (
            "prospective_exact_live_quote"
            if captured_after_seconds <= 15
            else "prospective_clock_missed"
        ),
        "invalid_reason": "",
        "quote_count": len(quotes),
        "quotes": quotes,
        "research_only": True,
        "execution_eligible": False,
    }


def test_pre_map_quote_capture_survives_t0_plus_20_semantic_mapping(tmp_path):
    at = subject.ACTIVATED_UTC + timedelta(minutes=2)
    event = _event("pre_map_early", at, eligible=True, currency="USD")
    event["transport_count"] = 1
    event["transport_observations"] = [
        {
            "mapping_payload": {
                "fast_lane_pre_map_quote_snapshot": _attached_pre_map_snapshot(
                    at, captured_after_seconds=10
                )
            }
        }
    ]
    connection = subject.open_output_database(tmp_path / "response.sqlite")
    try:
        subject.insert_events(connection, [event])
        # The semantic response-map cycle is deliberately later than the 15s
        # gate and its current quotes are therefore invalid for a new entry.
        inserted = subject.insert_live_entry_snapshots(
            connection,
            [event],
            {},
            {"updated_at": subject.iso(at + timedelta(seconds=20))},
            at + timedelta(seconds=20),
        )
        row = connection.execute(
            """
            SELECT captured_utc,capture_latency_seconds,timing_quality,
                   technical_snapshot_json,snapshot_payload_json
            FROM source_event_entry_snapshot
            WHERE canonical_event_id='pre_map_early'
            """
        ).fetchone()
    finally:
        connection.close()
    assert inserted == 1
    assert row is not None
    assert row[0] == subject.iso(at + timedelta(seconds=10))
    assert row[1] == 10.0
    assert row[2] == "prospective_exact_live_quote"
    technical = json.loads(row[3])
    payload = json.loads(row[4])
    assert technical["availability"] == "not_captured_at_pre_map_clock"
    assert payload["capture_origin"] == "fast_mapper_pre_semantic_quote"
    assert payload["quote_count"] == 60


def test_late_pre_map_and_late_semantic_capture_remain_invalid(tmp_path):
    at = subject.ACTIVATED_UTC + timedelta(minutes=3)
    event = _event("pre_map_late", at, eligible=True, currency="USD")
    event["transport_count"] = 1
    event["transport_observations"] = [
        {
            "mapping_payload": {
                "fast_lane_pre_map_quote_snapshot": _attached_pre_map_snapshot(
                    at, captured_after_seconds=20
                )
            }
        }
    ]
    connection = subject.open_output_database(tmp_path / "response.sqlite")
    try:
        subject.insert_events(connection, [event])
        subject.insert_live_entry_snapshots(
            connection,
            [event],
            {},
            {},
            at + timedelta(seconds=20),
        )
        quality, quote_count = connection.execute(
            """
            SELECT timing_quality,quote_count
            FROM source_event_entry_snapshot
            WHERE canonical_event_id='pre_map_late'
            """
        ).fetchone()
    finally:
        connection.close()
    assert quality == "prospective_clock_missed"
    assert quote_count == 0


def test_canonical_ls_response_retains_both_paths_and_intrabar_extrema():
    at = datetime(2026, 8, 27, 10, tzinfo=UTC)
    response = subject.compute_event_response(
        _event("response", at, currency="USD"), _complete_panel(at), 1
    )
    assert response is not None
    assert response["solver_status"] == "ok"
    assert response["usable_pair_count"] == 6
    assert response["selected_side"] is None
    assert response["response_shape"] in {"immediate", "null"}
    assert response["entry_method"] == "preactivation_m1_approximation"
    paths = response["both_executable_paths"]
    assert paths
    assert all("currency_strengthening" in row and "currency_weakening" in row for row in paths)
    assert all(row["extrema_sampling"] == "executable_m1_bid_ask_high_low" for row in paths)
    assert any(
        row["currency_strengthening"]["mfe_pips"]
        != row["currency_strengthening"]["terminal_executable_pips"]
        for row in paths
    )
    with pytest.raises(ValueError):
        subject.compute_event_response(_event("bad", at), _complete_panel(at), 2)


def test_completed_m1_clock_uses_bar_close_not_next_bar_close():
    at = datetime(2026, 8, 27, 10, tzinfo=UTC)
    rows = _complete_panel(at)["EUR_USD"]
    point = subject.close_point_at_or_after(rows, at + timedelta(minutes=1))
    assert point is rows[0]
    assert point.close_timestamp == at + timedelta(minutes=1)


def test_prospective_response_refuses_reconstructed_entry():
    at = subject.ACTIVATED_UTC + timedelta(minutes=1)
    event = _event("prospective", at, eligible=True)
    assert subject.compute_event_response(event, _complete_panel(at), 1) is None


def test_prequential_training_uses_only_previously_matured_unique_events(tmp_path):
    connection = subject.open_output_database(tmp_path / "ledger.sqlite")
    try:
        base = datetime(2026, 8, 20, tzinfo=UTC)
        past_events = [
            _event("past_a", base),
            _event("past_b", base + timedelta(minutes=10)),
            _event("future_maturity", base + timedelta(minutes=58)),
            _event("target", base + timedelta(hours=1)),
        ]
        subject.insert_events(connection, past_events)
        subject.insert_responses(
            connection,
            [
                _response("past_a", base, 2.0),
                _response("past_b", base + timedelta(minutes=10), 4.0),
                _response("future_maturity", base + timedelta(minutes=58), 20.0),
            ],
        )
        row = connection.execute(
            """
            SELECT sf.factor_observation_id,sf.canonical_event_id,sf.currency,
                   sf.factor_key,sf.factor_type,sf.factor_known_utc,
                   sf.evidence_class,sf.prospective_proof_eligible,e.authority,
                   ep.market_episode_id
            FROM source_factor_observation sf JOIN source_event_observation e
              ON e.canonical_event_id=sf.canonical_event_id
            JOIN source_event_episode ep
              ON ep.canonical_event_id=sf.canonical_event_id
            WHERE sf.canonical_event_id='target'
            """
        ).fetchone()
        factor = {
            "factor_observation_id": row[0],
            "canonical_event_id": row[1],
            "currency": row[2],
            "factor_key": row[3],
            "factor_type": row[4],
            "factor_known_utc": row[5],
            "evidence_class": row[6],
            "prospective_proof_eligible": bool(row[7]),
            "authority": row[8],
            "market_episode_id": row[9],
        }
        forecast = subject.build_prequential_forecast(
            connection, factor, 5, minimum_n=2
        )
        assert forecast["forecast_state"] == "forecast"
        assert forecast["effective_event_n"] == 2
        assert forecast["predicted_currency_factor_bps"] == pytest.approx(3.0)
        assert forecast["training_latest_maturity_utc"] <= factor["factor_known_utc"]
        abstain = subject.build_prequential_forecast(
            connection, factor, 5, minimum_n=3
        )
        assert abstain["forecast_state"] == "abstain"
        assert "low_effective_n" in abstain["abstain_reason"]
    finally:
        connection.close()


def test_late_known_factor_cannot_inherit_event_clock_response_or_proof(tmp_path):
    connection = subject.open_output_database(tmp_path / "ledger.sqlite")
    try:
        base = subject.ACTIVATED_UTC + timedelta(minutes=1)
        late_factor = _event(
            "late_factor",
            base,
            eligible=True,
            factor_known=base + timedelta(minutes=2),
        )
        target = _event("later_target", base + timedelta(hours=1))
        subject.insert_events(connection, [late_factor, target])
        subject.insert_responses(
            connection, [_response("late_factor", base, 25.0)]
        )
        proof_flag = connection.execute(
            "SELECT prospective_proof_eligible FROM source_factor_observation WHERE canonical_event_id='late_factor'"
        ).fetchone()[0]
        assert proof_flag == 0
        target_row = connection.execute(
            """
            SELECT sf.factor_observation_id,sf.canonical_event_id,sf.currency,
                   sf.factor_key,sf.factor_type,sf.factor_known_utc,
                   sf.evidence_class,sf.prospective_proof_eligible,e.authority,
                   ep.market_episode_id
            FROM source_factor_observation sf
            JOIN source_event_observation e USING(canonical_event_id)
            JOIN source_event_episode ep USING(canonical_event_id)
            WHERE sf.canonical_event_id='later_target'
            """
        ).fetchone()
        factor = {
            "factor_observation_id": target_row[0],
            "canonical_event_id": target_row[1],
            "currency": target_row[2],
            "factor_key": target_row[3],
            "factor_type": target_row[4],
            "factor_known_utc": target_row[5],
            "evidence_class": target_row[6],
            "prospective_proof_eligible": bool(target_row[7]),
            "authority": target_row[8],
            "market_episode_id": target_row[9],
        }
        forecast = subject.build_prequential_forecast(
            connection, factor, 5, minimum_n=1
        )
        assert forecast["forecast_state"] == "abstain"
        assert forecast["effective_event_n"] == 0
    finally:
        connection.close()


def test_effective_n_deduplicates_multicurrency_underlying_episode(tmp_path):
    connection = subject.open_output_database(tmp_path / "ledger.sqlite")
    try:
        base = datetime(2026, 8, 20, tzinfo=UTC)
        shared_episode = "shared_policy_release_episode"
        usd = _event(
            "usd_expression",
            base,
            currency="USD",
            market_episode=shared_episode,
        )
        eur = _event(
            "eur_expression",
            base + timedelta(seconds=2),
            currency="EUR",
            market_episode=shared_episode,
        )
        target = _event("episode_target", base + timedelta(hours=1))
        subject.insert_events(connection, [usd, eur, target])
        subject.insert_responses(
            connection,
            [
                _response("usd_expression", base, 2.0, currency="USD"),
                _response(
                    "eur_expression",
                    base + timedelta(seconds=2),
                    3.0,
                    currency="EUR",
                ),
            ],
        )
        target_row = connection.execute(
            """
            SELECT sf.factor_observation_id,sf.canonical_event_id,sf.currency,
                   sf.factor_key,sf.factor_type,sf.factor_known_utc,
                   sf.evidence_class,sf.prospective_proof_eligible,e.authority,
                   ep.market_episode_id
            FROM source_factor_observation sf
            JOIN source_event_observation e USING(canonical_event_id)
            JOIN source_event_episode ep USING(canonical_event_id)
            WHERE sf.canonical_event_id='episode_target'
            """
        ).fetchone()
        factor = {
            "factor_observation_id": target_row[0],
            "canonical_event_id": target_row[1],
            "currency": target_row[2],
            "factor_key": target_row[3],
            "factor_type": target_row[4],
            "factor_known_utc": target_row[5],
            "evidence_class": target_row[6],
            "prospective_proof_eligible": bool(target_row[7]),
            "authority": target_row[8],
            "market_episode_id": target_row[9],
        }
        forecast = subject.build_prequential_forecast(
            connection, factor, 5, minimum_n=2
        )
        assert forecast["forecast_state"] == "abstain"
        assert forecast["effective_event_n"] == 1
    finally:
        connection.close()


def test_append_only_and_idempotent_inserts(tmp_path):
    connection = subject.open_output_database(tmp_path / "ledger.sqlite")
    try:
        event = _event("immutable", datetime(2026, 8, 20, tzinfo=UTC))
        first = subject.insert_events(connection, [event])
        second = subject.insert_events(connection, [event])
        assert first["events"] == 1
        assert second == {
            "events": 0,
            "episodes": 0,
            "transports": 0,
            "factors": 0,
        }
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute(
                "UPDATE source_event_observation SET headline='changed' WHERE canonical_event_id='immutable'"
            )
    finally:
        connection.close()


def test_candle_tail_window_is_output_equivalent_to_full_scan(tmp_path):
    candle_root = tmp_path / "candles"
    path = candle_root / "EUR_USD_M1.csv"
    base = datetime(2026, 8, 1, tzinfo=UTC)
    _write_m1_fixture(path, base, 500)
    start = base + timedelta(minutes=487)
    end = base + timedelta(minutes=494)

    expected = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for raw in csv.DictReader(handle):
            stamp = subject.parse_time(raw.get("datetime") or raw.get("time"))
            if stamp is None or stamp < start:
                continue
            if stamp > end:
                break
            point = subject._candle_point_from_csv_row(raw)
            if point is not None:
                expected.append(point)

    panel = subject.load_candle_panel(candle_root, start, end)
    assert panel == {"EUR_USD": expected}
    assert [row.timestamp for row in panel["EUR_USD"]] == [
        base + timedelta(minutes=index) for index in range(487, 495)
    ]


def test_recent_candle_window_parses_only_tail_not_full_history(
    tmp_path, monkeypatch
):
    candle_root = tmp_path / "candles"
    path = candle_root / "EUR_USD_M1.csv"
    base = datetime(2026, 1, 1, tzinfo=UTC)
    row_count = 5_000
    _write_m1_fixture(path, base, row_count)
    start = base + timedelta(minutes=row_count - 8)
    end = base + timedelta(minutes=row_count - 3)

    original = subject.parse_time
    parse_calls = 0

    def counted_parse_time(value):
        nonlocal parse_calls
        parse_calls += 1
        return original(value)

    monkeypatch.setattr(subject, "parse_time", counted_parse_time)
    panel = subject.load_candle_panel(candle_root, start, end)
    assert len(panel["EUR_USD"]) == 6
    # Six included rows, two newer rows, and one older boundary row are enough.
    # A byte-zero scan would call the parser roughly 5,000 times.
    assert parse_calls <= 12


def test_zero_due_maturities_skips_candle_panel_io(tmp_path, monkeypatch):
    mapping_path = tmp_path / "mapping.sqlite"
    raw_path = tmp_path / "raw.sqlite"
    _create_input_databases(mapping_path, raw_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("candle panel must not be loaded")

    monkeypatch.setattr(subject, "load_candle_panel", forbidden)
    snapshot = subject.run_cycle(
        mapping_database=mapping_path,
        raw_database=raw_path,
        candle_root=tmp_path / "candles",
        output_database=tmp_path / "out.sqlite",
        snapshot_path=tmp_path / "out.json",
        report_path=tmp_path / "out.md",
        quote_path=tmp_path / "quotes.json",
        technical_path=tmp_path / "technical.json",
        observed_utc=subject.ACTIVATED_UTC,
    )
    assert snapshot["candle_panel_loaded"] is False
    assert snapshot["pending_maturities_processed"] == 0


def test_missed_prospective_entry_becomes_terminal_gap_without_panel_rescan(
    tmp_path, monkeypatch
):
    mapping_path = tmp_path / "mapping.sqlite"
    raw_path = tmp_path / "raw.sqlite"
    _create_input_databases(mapping_path, raw_path)
    event_time = subject.ACTIVATED_UTC + timedelta(minutes=1)
    _insert_input(
        mapping_path,
        raw_path,
        mapping_id="late",
        observation_id="late_obs",
        first_seen=event_time,
        payload=_payload(
            currency="USD",
            headline="late event",
            published=subject.iso(event_time),
        ),
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("invalid entry must terminal-gap without candle scan")

    monkeypatch.setattr(subject, "load_candle_panel", forbidden)
    kwargs = {
        "mapping_database": mapping_path,
        "raw_database": raw_path,
        "candle_root": tmp_path / "candles",
        "output_database": tmp_path / "out.sqlite",
        "snapshot_path": tmp_path / "out.json",
        "report_path": tmp_path / "out.md",
        "quote_path": tmp_path / "quotes.json",
        "technical_path": tmp_path / "technical.json",
        "observed_utc": event_time + timedelta(minutes=5),
    }
    first = subject.run_cycle(**kwargs)
    second = subject.run_cycle(**kwargs)
    assert first["inserted"]["terminal_gaps"] == 1
    assert second["inserted"]["terminal_gaps"] == 0
    assert first["candle_panel_loaded"] is False
    connection = sqlite3.connect(kwargs["output_database"])
    try:
        reason = connection.execute(
            "SELECT reason FROM source_response_terminal_gap"
        ).fetchone()[0]
    finally:
        connection.close()
    assert reason.startswith("invalid_exact_entry_snapshot:")


def test_no_broker_execution_or_promotion_imports_and_policy_is_inert():
    source = Path(subject.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
    assert not any(
        token in name.lower()
        for name in imports
        for token in ("broker", "execution", "authorization", "promotion", "lifecycle")
    )
    assert subject.POLICY["research_only"] is True
    assert subject.POLICY["execution_eligible"] is False
    assert subject.POLICY["can_authorize"] is False
    assert subject.POLICY["can_promote"] is False


def test_inputs_are_readonly_and_unchanged_files_skip_repeated_quick_check(
    tmp_path, monkeypatch
):
    mapping_path = tmp_path / "mapping.sqlite"
    raw_path = tmp_path / "raw.sqlite"
    _create_input_databases(mapping_path, raw_path)
    subject._INPUT_INTEGRITY_CACHE.clear()
    subject._INPUT_OBSERVATION_CACHE.clear()
    calls = 0
    original = subject._quick_check

    def counted(connection):
        nonlocal calls
        calls += 1
        return original(connection)

    monkeypatch.setattr(subject, "_quick_check", counted)
    subject.load_current_observations(mapping_path, raw_path)
    subject.load_current_observations(mapping_path, raw_path)
    assert calls == 2  # one startup check per unchanged input database
    readonly = subject._open_readonly(mapping_path)
    try:
        with pytest.raises(sqlite3.OperationalError):
            readonly.execute("CREATE TABLE forbidden_write(x INTEGER)")
    finally:
        readonly.close()


def test_unchanged_input_snapshot_skips_database_reopen_and_zero_wal_noise(
    tmp_path, monkeypatch
):
    mapping_path = tmp_path / "mapping.sqlite"
    raw_path = tmp_path / "raw.sqlite"
    _create_input_databases(mapping_path, raw_path)
    subject._INPUT_INTEGRITY_CACHE.clear()
    subject._INPUT_OBSERVATION_CACHE.clear()
    opens = 0
    original = subject._open_readonly

    def counted(path):
        nonlocal opens
        opens += 1
        return original(path)

    monkeypatch.setattr(subject, "_open_readonly", counted)
    first = subject.load_current_observations(mapping_path, raw_path)
    assert opens == 2
    # A read-only SQLite connection can create or retouch an empty WAL.  It
    # carries no committed page and must not trigger a 119 MB rescan.
    Path(str(mapping_path) + "-wal").write_bytes(b"")
    Path(str(raw_path) + "-wal").write_bytes(b"")
    second = subject.load_current_observations(mapping_path, raw_path)
    Path(str(mapping_path) + "-wal").touch()
    third = subject.load_current_observations(mapping_path, raw_path)
    assert first == second == third == []
    assert opens == 2


def test_input_snapshot_cache_invalidates_when_new_event_is_committed(tmp_path):
    mapping_path = tmp_path / "mapping.sqlite"
    raw_path = tmp_path / "raw.sqlite"
    _create_input_databases(mapping_path, raw_path)
    subject._INPUT_INTEGRITY_CACHE.clear()
    subject._INPUT_OBSERVATION_CACHE.clear()
    at = subject.ACTIVATED_UTC + timedelta(minutes=1)
    _insert_input(
        mapping_path,
        raw_path,
        mapping_id="first",
        observation_id="first_obs",
        first_seen=at,
        payload=_payload(published=subject.iso(at)),
    )
    first = subject.load_current_observations(mapping_path, raw_path)
    assert [row["mapping_id"] for row in first] == ["first"]

    _insert_input(
        mapping_path,
        raw_path,
        mapping_id="second",
        observation_id="second_obs",
        first_seen=at + timedelta(minutes=1),
        payload=_payload(
            headline="second",
            url="https://central.example/releases/second",
            published=subject.iso(at + timedelta(minutes=1)),
        ),
    )
    second = subject.load_current_observations(mapping_path, raw_path)
    assert [row["mapping_id"] for row in second] == ["first", "second"]
