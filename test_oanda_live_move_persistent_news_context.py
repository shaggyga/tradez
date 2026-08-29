import json
from pathlib import Path

import oanda_live_move_persistent_news_context as persistent


def _event(*, epoch=100, horizon=360, official=True):
    payload = {
        "headline": "Official policy shock",
        "directional_bias": {"USD": "BULLISH", "ZAR": "BEARISH"},
        "directional_confidence": 0.7,
        "relevance_window_minutes": horizon,
        "direct_currencies": ["USD"],
        "source_direct": official,
        "source_verified": official,
    }
    return {
        "source_event_id": "e1",
        "source_id": "treasury",
        "source_population": "official_policy_publisher" if official else "media",
        "event_type": "risk_off_geopolitical_or_financial",
        "story_cluster_id": "s1",
        "effective_from_utc": "1970-01-01T00:01:40+00:00",
        "effective_epoch": epoch,
        "valid_until_epoch": None,
        "superseded_epoch": None,
        "payload_json": json.dumps({"raw_payload": payload}),
    }


def test_declared_horizon_keeps_active_and_rejects_expired_event():
    event = _event(epoch=100, horizon=10)
    assert persistent.active_persistent_events([event], entry_epoch=699)
    assert not persistent.active_persistent_events([event], entry_epoch=701)


def test_persistent_context_is_separate_and_can_expose_wrong_mapping():
    event = _event(epoch=100, horizon=360)
    index = {
        "USD": {"events": [event], "epochs": [100]},
        "ZAR": {"events": [], "epochs": []},
    }
    result = persistent.enrich_case(
        {
            "instrument": "USD_ZAR",
            "move_direction": "down",
            "start_utc": "1970-01-01T05:00:00+00:00",
        },
        index,
    )
    assert result["persistent_active_event_count"] == 1
    assert result["persistent_independent_story_count"] == 1
    assert result["persistent_research_side"] == "long"
    assert result["persistent_research_alignment"] == "opposed"
    assert result["persistent_official_side"] == "long"
    assert result["persistent_official_alignment"] == "opposed"
    assert result["persistent_direct_authority_side"] == "long"
    assert result["persistent_direct_authority_alignment"] == "opposed"
    assert result["persistent_official_stories"][0]["headline"] == "Official policy shock"


def test_official_spillover_is_not_mislabeled_as_direct_pair_authority():
    event = _event(epoch=100, horizon=360)
    index = {
        "AUD": {"events": [event], "epochs": [100]},
        "CAD": {"events": [], "epochs": []},
    }
    result = persistent.enrich_case(
        {
            "instrument": "AUD_CAD",
            "move_direction": "up",
            "start_utc": "1970-01-01T05:00:00+00:00",
        },
        index,
    )
    assert result["persistent_official_side"] == "neutral"
    assert result["persistent_direct_authority_side"] == "neutral"


def test_worker_is_supervised_and_inert():
    supervisor = (Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1").read_text(encoding="utf-8")
    assert '-Name "live_move_persistent_news_context"' in supervisor
    block = supervisor.split('-Name "live_move_persistent_news_context"', 1)[1].split(
        '$managed += Start-ManagedProcess', 1
    )[0]
    assert "order" not in block.lower()
    assert "authorization" not in block.lower()


def test_contract_is_shadow_only():
    assert persistent.CONTRACT_ID == "live_move_persistent_news_context_v2_direct_authority_20260825"
