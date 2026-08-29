from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from forex_system.ingestion import sarb_mpc_decision_exact_v2 as sarb  # noqa: E402
from forex_system.ingestion.sarb_mpc_decision_exact_v2 import (  # noqa: E402
    CONTRACT_ID,
    JULY_DECISION_FACTS_SHA256,
    JULY_STATEMENT_URL,
    SCHEDULE_FACTS_SHA256,
    SCHEDULE_OBSERVATION_ID,
    SCHEDULE_OBSERVATION_MANIFEST_PATH,
    SCHEDULE_OBSERVATION_MANIFEST_SHA256,
    SCHEDULE_OBSERVED_UTC,
    SCHEDULE_OBSERVED_UTC_ORIGINAL_100NS,
    SCHEDULE_SOURCE_ARCHIVE_PATH,
    SCHEDULE_SOURCE_BYTES,
    SCHEDULE_SOURCE_SHA256,
    SCHEDULE_URL,
    SarbMpcDecisionSourceError,
    assert_johannesburg_utc_plus_two_no_dst,
    load_contract_manifest,
    load_schedule_observation_manifest,
    parse_official_decision,
    parse_official_schedule,
)


SCHEDULE_HTML = b"""
<html><body>
<div>2026 Announcements Date Presentation Viewing Status</div>
<div>2026/01/29 - 15:00 29 January 2026 press conference of the Monetary Policy Committee</div>
<div>2026/03/26 - 15:00 26 March 2026 press conference of the Monetary Policy Committee</div>
<div>2026/05/28 - 15:00 28 May 2026 press conference of the Monetary Policy Committee</div>
<div>2026/07/23 - 15:00 23 July 2026 press conference of the Monetary Policy Committee</div>
<div>2026/09/23 - 15:00 23 September 2026 press conference of the Monetary Policy Committee</div>
<div>2026/11/19 - 15:00 19 November 2026 press conference of the Monetary Policy Committee</div>
</body></html>
"""

JULY_STATEMENT_HTML = b"""
<html><head><title>Statement of the Monetary Policy Committee July 2026</title></head>
<body>
<p>Inflation is at 5.0% and the inflation target is 3%.</p>
<p>Against this backdrop, the committee decided to keep the policy rate unchanged, at 7%.</p>
<p>Four members preferred a hold, while two favoured an increase of 25 basis points.</p>
<p>The model shows inflation returning to 3% over time.</p>
</body></html>
"""


def _parse_schedule_fixture(payload: bytes = SCHEDULE_HTML) -> dict:
    # Unit parser fixtures exercise row ambiguity and chronology independently.
    # The production entry point itself is separately proved to reject any bytes
    # that do not match the frozen 100,939-byte official observation.
    with mock.patch.object(
        sarb,
        "_validate_frozen_schedule_payload",
        return_value=SCHEDULE_SOURCE_SHA256,
    ):
        return parse_official_schedule(
            payload,
            observed_utc=SCHEDULE_OBSERVED_UTC,
            source_url=SCHEDULE_URL,
        )


def _schedule() -> dict:
    return _parse_schedule_fixture()


def _event(date: str) -> dict:
    return next(row for row in _schedule()["events"] if row["event_date"] == date)


def test_schedule_exact_clocks_timezone_and_boundaries() -> None:
    parsed = _schedule()
    assert parsed["event_count"] == 3
    assert parsed["facts_sha256"] == SCHEDULE_FACTS_SHA256
    assert [row["scheduled_utc"] for row in parsed["events"]] == [
        "2026-07-23T13:00:00Z",
        "2026-09-23T13:00:00Z",
        "2026-11-19T13:00:00Z",
    ]
    july, september, november = parsed["events"]
    assert july["collection_class"] == "historical_availability_counterfactual_archive_only"
    assert july["prospective_schedule"] is False
    assert september["collection_class"] == "prospective_schedule_only"
    assert september["prospective_schedule"] is True
    assert november["prospective_schedule"] is True
    assert all(row["local_timezone"] == "Africa/Johannesburg" for row in parsed["events"])
    assert_johannesburg_utc_plus_two_no_dst(2026)


def test_schedule_clock_is_not_decision_availability_or_direction() -> None:
    for row in _schedule()["events"]:
        assert row["decision_causal_known_utc"] is None
        assert row["direction_policy"] == "abstain"
        assert row["currency_bias"] is None
        assert row["pair_bias"] is None
        assert row["consensus"] is None
        assert row["surprise"] is None
        assert row["same_time_global_event_confounding_required"] is True
        assert row["causal_attribution_status"] == "unresolved"
        assert row["confirmation_eligible"] is False
        assert row["proof_eligible"] is False
        assert row["execution_eligible"] is False


def test_july_decision_extracts_only_explicit_rate_and_vote_facts() -> None:
    record = parse_official_decision(
        JULY_STATEMENT_HTML,
        source_url=JULY_STATEMENT_URL,
        statement_first_seen_utc="2026-08-17T08:30:00Z",
    )
    assert record["action"] == "hold"
    assert record["policy_rate_pct"] == 7.0
    assert record["policy_rate_pct"] not in {3.0, 5.0}
    assert record["votes"] == (
        {"count": 4, "preference": "hold", "basis_points": 0},
        {"count": 2, "preference": "hike", "basis_points": 25},
    )
    assert record["vote_total_observed"] == 6
    assert record["decision_facts_sha256"] == JULY_DECISION_FACTS_SHA256
    assert record["collection_class"] == "historical_availability_counterfactual_archive_only"
    assert record["prospective_observation"] is False
    assert record["decision_causal_known_utc"] == "2026-08-17T08:30:00Z"
    assert record["event_id"] == _event("2026-07-23")["event_id"]
    assert record["independent_episode_key"] == _event("2026-07-23")["independent_episode_key"]
    assert record["schedule_and_statement_are_one_event"] is True


def test_source_bytes_are_hashed_but_full_text_is_never_returned() -> None:
    schedule = _schedule()
    assert schedule["source_sha256"] == SCHEDULE_SOURCE_SHA256
    assert schedule["source_bytes"] == SCHEDULE_SOURCE_BYTES
    record = parse_official_decision(
        JULY_STATEMENT_HTML,
        source_url=JULY_STATEMENT_URL,
        statement_first_seen_utc="2026-08-17T08:30:00Z",
    )
    assert record["statement_source_sha256"] == hashlib.sha256(JULY_STATEMENT_HTML).hexdigest()
    assert record["full_text_retained"] is False
    assert record["license_full_text_retention_verified"] is False
    for forbidden in ("raw_html", "raw_text", "full_text", "article_text"):
        assert forbidden not in schedule
        assert forbidden not in record
    assert "Against this backdrop" not in repr(record)


def test_causal_known_time_is_max_of_clock_and_first_seen() -> None:
    september_html = b"""
    <html><title>Statement of the Monetary Policy Committee September 2026</title>
    <p>The committee decided to increase the policy rate by 25 basis points to 7.25%.</p>
    <p>Three members preferred a hold, while three favoured an increase of 25 basis points.</p>
    </html>
    """
    with pytest.raises(SarbMpcDecisionSourceError, match="before_scheduled_event"):
        parse_official_decision(
            september_html,
            source_url=(
                "https://www.resbank.co.za/en/home/publications/publication-detail-pages/"
                "statements/monetary-policy-statements/2026/september"
            ),
            statement_first_seen_utc="2026-09-23T12:59:59Z",
        )

    after_clock = parse_official_decision(
        september_html,
        source_url=(
            "https://www.resbank.co.za/en/home/publications/publication-detail-pages/"
            "statements/monetary-policy-statements/2026/september"
        ),
        statement_first_seen_utc="2026-09-23T13:00:07Z",
    )
    assert after_clock["decision_causal_known_utc"] == "2026-09-23T13:00:07Z"
    assert after_clock["clock_floor_applied"] is False


def test_frozen_schedule_observation_time_cannot_be_relabelled() -> None:
    with mock.patch.object(
        sarb,
        "_validate_frozen_schedule_payload",
        return_value=SCHEDULE_SOURCE_SHA256,
    ):
        with pytest.raises(
            SarbMpcDecisionSourceError, match="observation_timestamp_mismatch"
        ):
            parse_official_schedule(
                SCHEDULE_HTML,
                observed_utc="2026-09-23T13:01:00Z",
                source_url=SCHEDULE_URL,
            )


@pytest.mark.parametrize(
    "bad_url",
    [
        "https://custom.resbank.co.za/SarbWebApi/WebIndicators/HomePageRates",
        "https://www.google.com/search?q=SARB+rate",
        "http://www.resbank.co.za/en/home/what-we-do/monetary-policy/MPC-announcement-webcasts",
        "https://www.resbank.co.za/en/home/what-we-do/monetary-policy/MPC-announcement-webcasts?cached=1",
    ],
)
def test_non_contract_schedule_and_api_search_urls_are_rejected(bad_url: str) -> None:
    with pytest.raises(SarbMpcDecisionSourceError):
        parse_official_schedule(
            SCHEDULE_HTML, observed_utc=SCHEDULE_OBSERVED_UTC, source_url=bad_url
        )


def test_homepage_api_date_cannot_be_relabelled_as_decision_time() -> None:
    api_payload = b'[{"SeriesCode":"MMRD002A","Date":"2026-08-14","Value":7.0}]'
    with pytest.raises(SarbMpcDecisionSourceError):
        parse_official_decision(
            api_payload,
            source_url="https://custom.resbank.co.za/SarbWebApi/WebIndicators/HomePageRates",
            statement_first_seen_utc="2026-08-14T12:00:00Z",
        )


def test_official_statement_path_must_match_frozen_event_month() -> None:
    with pytest.raises(SarbMpcDecisionSourceError, match="frozen_event_contract"):
        parse_official_decision(
            JULY_STATEMENT_HTML,
            source_url=(
                "https://www.resbank.co.za/en/home/publications/publication-detail-pages/"
                "statements/monetary-policy-statements/2026/august"
            ),
            statement_first_seen_utc="2026-08-17T08:30:00Z",
        )


def test_schedule_revision_missing_and_duplicate_rows_fail_closed() -> None:
    revised = SCHEDULE_HTML.replace(b"2026/09/23 - 15:00", b"2026/09/23 - 14:00")
    with pytest.raises(SarbMpcDecisionSourceError, match="time_revision_requires_new_cohort"):
        _parse_schedule_fixture(revised)

    missing = SCHEDULE_HTML.replace(
        b"<div>2026/11/19 - 15:00 19 November 2026 press conference of the Monetary Policy Committee</div>",
        b"",
    )
    with pytest.raises(SarbMpcDecisionSourceError, match="missing_target_row"):
        _parse_schedule_fixture(missing)

    duplicate = SCHEDULE_HTML.replace(b"</body>", SCHEDULE_HTML.splitlines()[-2] + b"</body>")
    with pytest.raises(SarbMpcDecisionSourceError, match="duplicate_target_row"):
        _parse_schedule_fixture(duplicate)


def test_display_date_mismatch_is_rejected() -> None:
    mismatch = SCHEDULE_HTML.replace(
        b"2026/09/23 - 15:00 23 September 2026",
        b"2026/09/23 - 15:00 24 September 2026",
    )
    with pytest.raises(SarbMpcDecisionSourceError, match="display_date_mismatch"):
        _parse_schedule_fixture(mismatch)


def test_decision_requires_explicit_action_and_vote_split() -> None:
    no_action = JULY_STATEMENT_HTML.replace(
        b"the committee decided to keep the policy rate unchanged, at 7%.",
        b"the current policy-rate indicator is 7%.",
    )
    with pytest.raises(SarbMpcDecisionSourceError, match="decision_sentence"):
        parse_official_decision(
            no_action,
            source_url=JULY_STATEMENT_URL,
            statement_first_seen_utc="2026-08-17T08:30:00Z",
        )
    no_vote = JULY_STATEMENT_HTML.replace(
        b"Four members preferred a hold, while two favoured an increase of 25 basis points.",
        b"Members discussed several options.",
    )
    with pytest.raises(SarbMpcDecisionSourceError, match="vote_split_missing"):
        parse_official_decision(
            no_vote,
            source_url=JULY_STATEMENT_URL,
            statement_first_seen_utc="2026-08-17T08:30:00Z",
        )


def test_historical_facts_change_requires_a_new_contract() -> None:
    changed = JULY_STATEMENT_HTML.replace(b"at 7%", b"at 8%")
    with pytest.raises(SarbMpcDecisionSourceError, match="facts_changed_requires_new_contract"):
        parse_official_decision(
            changed,
            source_url=JULY_STATEMENT_URL,
            statement_first_seen_utc="2026-08-17T08:30:00Z",
        )


def test_schedule_identity_prevents_manual_reclassification() -> None:
    event = _event("2026-07-23")
    event["scheduled_utc"] = "2026-08-14T00:00:00Z"
    with pytest.raises(SarbMpcDecisionSourceError, match="clock_mismatch"):
        sarb._validate_schedule_event(event)


@pytest.mark.parametrize(
    ("field", "bad_value", "error"),
    [
        ("event_id", "sarb_mpc_decision:2026-08-14", "event_id_mismatch"),
        ("independent_episode_key", "ZAR:pair:USD_ZAR:2026-07-23", "episode_key_mismatch"),
        (
            "schedule_source_url",
            "https://www.resbank.co.za/en/home",
            "identity_mismatch:schedule_source_url",
        ),
        (
            "schedule_source_sha256",
            "0" * 64,
            "identity_mismatch:schedule_source_sha256",
        ),
        ("schedule_binding_sha256", "0" * 64, "source_content_binding_mismatch"),
    ],
)
def test_supplied_schedule_identity_and_content_binding_cannot_be_overridden(
    field: str, bad_value: str, error: str
) -> None:
    event = _event("2026-07-23")
    event[field] = bad_value
    with pytest.raises(SarbMpcDecisionSourceError, match=error):
        sarb._validate_schedule_event(event)


@pytest.mark.parametrize(
    ("field", "bad_value", "error"),
    [
        ("schedule_captured_pre_event", "false", "not_bool"),
        ("schedule_captured_pre_event", 0, "not_bool"),
        ("prospective_schedule", "false", "not_bool"),
        ("prospective_schedule", 1, "not_bool"),
    ],
)
def test_non_boolean_schedule_flags_are_rejected(
    field: str, bad_value: object, error: str
) -> None:
    event = _event("2026-09-23")
    event[field] = bad_value
    with pytest.raises(SarbMpcDecisionSourceError, match=error):
        sarb._validate_schedule_event(event)


def test_schedule_flags_are_recomputed_from_typed_timestamp_and_strict_boundary() -> None:
    before = _event("2026-09-23")
    before["schedule_captured_pre_event"] = False
    with pytest.raises(SarbMpcDecisionSourceError, match="captured_pre_event_inconsistent"):
        sarb._validate_schedule_event(before)

    before = _event("2026-09-23")
    before["prospective_schedule"] = False
    with pytest.raises(SarbMpcDecisionSourceError, match="prospective_schedule_inconsistent"):
        sarb._validate_schedule_event(before)

    observed_relabel = _event("2026-09-23")
    observed_relabel["schedule_observed_utc"] = "2026-09-23T13:00:00Z"
    with pytest.raises(SarbMpcDecisionSourceError, match="schedule_observed_utc"):
        sarb._validate_schedule_event(observed_relabel)


def test_duplicate_or_conflicting_action_sentences_are_rejected() -> None:
    duplicate = JULY_STATEMENT_HTML.replace(
        b"</body>",
        b"<p>The committee decided to keep the policy rate unchanged, at 7%.</p></body>",
    )
    conflicting = JULY_STATEMENT_HTML.replace(
        b"</body>",
        b"<p>The committee decided to increase the policy rate by 25 basis points to 7.25%.</p></body>",
    )
    for payload in (duplicate, conflicting):
        with pytest.raises(SarbMpcDecisionSourceError, match="missing_or_ambiguous"):
            parse_official_decision(
                payload,
                source_url=JULY_STATEMENT_URL,
                statement_first_seen_utc="2026-08-17T08:30:00Z",
            )


def test_duplicate_and_three_way_vote_descriptions_are_rejected() -> None:
    duplicate = JULY_STATEMENT_HTML.replace(
        b"</body>",
        b"<p>Four members preferred a hold, while two favoured an increase of 25 basis points.</p></body>",
    )
    three_way = JULY_STATEMENT_HTML.replace(
        b"Four members preferred a hold, while two favoured an increase of 25 basis points.",
        b"Three members preferred a hold, while two favoured an increase of 25 basis points, and one favoured a decrease of 25 basis points.",
    )
    with pytest.raises(SarbMpcDecisionSourceError, match="missing_or_ambiguous"):
        parse_official_decision(
            duplicate,
            source_url=JULY_STATEMENT_URL,
            statement_first_seen_utc="2026-08-17T08:30:00Z",
        )
    with pytest.raises(SarbMpcDecisionSourceError, match="unsupported_or_complex"):
        parse_official_decision(
            three_way,
            source_url=JULY_STATEMENT_URL,
            statement_first_seen_utc="2026-08-17T08:30:00Z",
        )


@pytest.mark.parametrize(
    ("guard", "bad_value"),
    [
        ("consensus", 0.0),
        ("surprise", 0.0),
        ("direction", "buy"),
        ("currency_bias", {"ZAR": 1.0}),
        ("pair_bias", {"USD_ZAR": "sell"}),
        ("confirmation_eligible", True),
        ("promotion_eligible", True),
        ("proof_eligible", True),
        ("authorization_eligible", True),
        ("execution_eligible", True),
        ("execution_eligible", 0),
    ],
)
def test_manifest_deep_hard_guards_cannot_be_weakened(
    tmp_path: Path, guard: str, bad_value: object
) -> None:
    manifest = json.loads(
        (ROOT / "config" / "sarb_mpc_decision_exact_v2.json").read_text(encoding="utf-8")
    )
    manifest["hard_guards"][guard] = bad_value
    path = tmp_path / f"bad-{guard}.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(
        SarbMpcDecisionSourceError,
        match="contract_manifest_deep_typed_mismatch",
    ):
        load_contract_manifest(path)


def test_manifest_rejects_non_boolean_top_level_and_nested_full_text(tmp_path: Path) -> None:
    original = json.loads(
        (ROOT / "config" / "sarb_mpc_decision_exact_v2.json").read_text(encoding="utf-8")
    )
    wrong_type = copy.deepcopy(original)
    wrong_type["enabled"] = 0
    wrong_type_path = tmp_path / "wrong-type.json"
    wrong_type_path.write_text(json.dumps(wrong_type), encoding="utf-8")
    with pytest.raises(
        SarbMpcDecisionSourceError, match="contract_manifest_deep_typed_mismatch"
    ):
        load_contract_manifest(wrong_type_path)

    retained = copy.deepcopy(original)
    retained["evidence_policy"]["raw_text"] = "forbidden"
    retained_path = tmp_path / "retained.json"
    retained_path.write_text(json.dumps(retained), encoding="utf-8")
    with pytest.raises(
        SarbMpcDecisionSourceError, match="contract_manifest_deep_typed_mismatch"
    ):
        load_contract_manifest(retained_path)


def test_schedule_requires_exact_frozen_official_response_bytes() -> None:
    with pytest.raises(SarbMpcDecisionSourceError, match="byte_count_mismatch"):
        parse_official_schedule(
            SCHEDULE_HTML,
            observed_utc=SCHEDULE_OBSERVED_UTC,
            source_url=SCHEDULE_URL,
        )
    wrong_same_size = SCHEDULE_HTML.ljust(SCHEDULE_SOURCE_BYTES, b" ")
    assert len(wrong_same_size) == SCHEDULE_SOURCE_BYTES
    with pytest.raises(SarbMpcDecisionSourceError, match="sha256_mismatch"):
        parse_official_schedule(
            wrong_same_size,
            observed_utc=SCHEDULE_OBSERVED_UTC,
            source_url=SCHEDULE_URL,
        )


def test_schedule_observation_manifest_is_exact_safe_and_content_addressed() -> None:
    path = ROOT / SCHEDULE_OBSERVATION_MANIFEST_PATH
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == SCHEDULE_OBSERVATION_MANIFEST_SHA256
    observation = load_schedule_observation_manifest()
    assert observation["observation_id"] == SCHEDULE_OBSERVATION_ID
    assert observation["observed_utc"] == SCHEDULE_OBSERVED_UTC
    assert (
        observation["observed_utc_original_100ns"]
        == SCHEDULE_OBSERVED_UTC_ORIGINAL_100NS
    )
    assert observation["raw_response_bytes"] == SCHEDULE_SOURCE_BYTES
    assert observation["raw_response_sha256"] == SCHEDULE_SOURCE_SHA256
    assert observation["raw_payload_retained"] is True
    assert observation["raw_payload_archive_path"] == SCHEDULE_SOURCE_ARCHIVE_PATH
    archive = ROOT / SCHEDULE_SOURCE_ARCHIVE_PATH
    assert len(archive.read_bytes()) == SCHEDULE_SOURCE_BYTES
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == SCHEDULE_SOURCE_SHA256
    assert observation["direction"] is None
    assert observation["consensus"] is None
    assert observation["surprise"] is None
    assert observation["promotion_eligible"] is False
    assert observation["authorization_eligible"] is False
    assert observation["execution_eligible"] is False

    assert archive.stat().st_nlink == 1
    if os.name == "nt":
        assert archive.stat().st_file_attributes & 0x1


@pytest.mark.parametrize(
    "alias",
    [
        "2026-08-17T08:27:16.3040410+00:00",
        "2026-08-17T08:27:16.3040411+00:00",
        "2026-08-17T08:27:16.3040419+00:00",
        "2026-08-17T08:27:16.304041Z",
    ],
)
def test_schedule_observation_timestamp_aliases_are_rejected(alias: str) -> None:
    with pytest.raises(SarbMpcDecisionSourceError, match="observation_timestamp_mismatch"):
        parse_official_schedule(SCHEDULE_HTML, observed_utc=alias, source_url=SCHEDULE_URL)


def test_schedule_observation_timestamp_requires_exact_string_not_datetime() -> None:
    with pytest.raises(SarbMpcDecisionSourceError, match="observation_timestamp_mismatch"):
        parse_official_schedule(
            SCHEDULE_HTML,
            observed_utc=sarb._parse_utc(SCHEDULE_OBSERVED_UTC),
            source_url=SCHEDULE_URL,
        )


def test_decision_parser_has_no_caller_schedule_mapping_surface() -> None:
    event = _event("2026-09-23")
    mappings = [event, copy.deepcopy(event), json.loads(json.dumps(event))]
    for supplied in mappings:
        with pytest.raises(TypeError, match="schedule_event"):
            parse_official_decision(
                b"unused",
                source_url=(
                    "https://www.resbank.co.za/en/home/publications/publication-detail-pages/"
                    "statements/monetary-policy-statements/2026/september"
                ),
                statement_first_seen_utc="2026-09-23T13:00:01Z",
                schedule_event=supplied,  # type: ignore[call-arg]
            )


def test_decision_archive_derivation_survives_a_fresh_process_restart() -> None:
    direct = parse_official_decision(
        JULY_STATEMENT_HTML,
        source_url=JULY_STATEMENT_URL,
        statement_first_seen_utc="2026-08-17T08:30:00Z",
    )
    script = (
        "import json; "
        "from forex_system.ingestion.sarb_mpc_decision_exact_v2 import "
        "parse_official_decision; "
        f"record=parse_official_decision({JULY_STATEMENT_HTML!r}, "
        f"source_url={JULY_STATEMENT_URL!r}, "
        "statement_first_seen_utc='2026-08-17T08:30:00Z'); "
        "print(json.dumps({k: record[k] for k in "
        "('event_id','schedule_source_sha256','schedule_observed_utc',"
        "'decision_facts_sha256')}))"
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    restarted = json.loads(completed.stdout)
    assert restarted == {
        key: direct[key]
        for key in (
            "event_id",
            "schedule_source_sha256",
            "schedule_observed_utc",
            "decision_facts_sha256",
        )
    }


def test_statement_first_seen_must_follow_clock_and_schedule_observation() -> None:
    with pytest.raises(SarbMpcDecisionSourceError, match="before_schedule_observation"):
        parse_official_decision(
            JULY_STATEMENT_HTML,
            source_url=JULY_STATEMENT_URL,
            statement_first_seen_utc="2026-08-17T08:20:00Z",
        )


def test_duplicate_vote_preference_and_missing_move_size_are_rejected() -> None:
    url = (
        "https://www.resbank.co.za/en/home/publications/publication-detail-pages/"
        "statements/monetary-policy-statements/2026/september"
    )
    duplicate_preference = b"""
    <title>Statement of the Monetary Policy Committee September 2026</title>
    <p>The committee decided to keep the policy rate unchanged at 7%.</p>
    <p>Three members preferred a hold, while three favoured a hold.</p>
    """
    with pytest.raises(SarbMpcDecisionSourceError, match="duplicate_vote_preference"):
        parse_official_decision(
            duplicate_preference,
            source_url=url,
            statement_first_seen_utc="2026-09-23T13:00:01Z",
        )

    missing_move_size = b"""
    <title>Statement of the Monetary Policy Committee September 2026</title>
    <p>The committee decided to increase the policy rate to 7.25%.</p>
    <p>Three members preferred a hold, while three favoured an increase of 25 basis points.</p>
    """
    with pytest.raises(
        SarbMpcDecisionSourceError, match="missing_explicit_basis_points"
    ):
        parse_official_decision(
            missing_move_size,
            source_url=url,
            statement_first_seen_utc="2026-09-23T13:00:01Z",
        )


@pytest.mark.parametrize("field", ["side", "trade", "order", "units", "notional", "proof", "promote", "authorized"])
@pytest.mark.parametrize("branch", [None, "evidence_policy", "hard_guards", "separation_contract"])
def test_manifest_rejects_every_unlisted_restricted_key_at_any_depth(
    tmp_path: Path, branch: str | None, field: str
) -> None:
    manifest = json.loads(
        (ROOT / "config" / "sarb_mpc_decision_exact_v2.json").read_text(
            encoding="utf-8"
        )
    )
    target = manifest if branch is None else manifest[branch]
    target[field] = False
    path = tmp_path / f"injected-{branch or 'root'}-{field}.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(
        SarbMpcDecisionSourceError,
        match="contract_manifest_deep_typed_mismatch",
    ):
        load_contract_manifest(path)


@pytest.mark.parametrize(
    ("path_parts", "bad_value"),
    [
        (("currency",), "EUR"),
        (("timezone_contract",), "UTC+02:00"),
        (("schema_version",), True),
        (("schema_version",), 2.0),
        (("enabled",), 0),
        (("schedule_source_bytes",), True),
        (("schedule_source_bytes",), 100_939.0),
        (("direction_policy",), []),
        (("schedule_source_sha256",), {}),
        (("hard_guards", "execution_eligible"), 0),
        (("schedule_facts",), {}),
        (("schedule_facts", 0, "event_date"), ["2026-07-23"]),
        (("july_decision_facts", "policy_rate_pct"), 7),
        (("july_decision_facts", "votes", 0, "count"), True),
    ],
)
def test_manifest_pins_every_nested_value_and_scalar_type(
    tmp_path: Path, path_parts: tuple[object, ...], bad_value: object
) -> None:
    manifest = json.loads(
        (ROOT / "config" / "sarb_mpc_decision_exact_v2.json").read_text(
            encoding="utf-8"
        )
    )
    target: object = manifest
    for part in path_parts[:-1]:
        target = target[part]  # type: ignore[index]
    target[path_parts[-1]] = bad_value  # type: ignore[index]
    path = tmp_path / "typed-mismatch.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(
        SarbMpcDecisionSourceError, match="contract_manifest_deep_typed_mismatch"
    ):
        load_contract_manifest(path)


@pytest.mark.parametrize("nonfinite", [float("nan"), float("inf"), float("-inf")])
def test_manifest_rejects_json_nonfinite_constants(
    tmp_path: Path, nonfinite: float
) -> None:
    manifest = json.loads(
        (ROOT / "config" / "sarb_mpc_decision_exact_v2.json").read_text(
            encoding="utf-8"
        )
    )
    manifest["schema_version"] = nonfinite
    path = tmp_path / "nonfinite.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SarbMpcDecisionSourceError, match="nonfinite_number"):
        load_contract_manifest(path)


def test_manifest_rejects_overflow_number_and_duplicate_keys(tmp_path: Path) -> None:
    raw = (ROOT / "config" / "sarb_mpc_decision_exact_v2.json").read_bytes()
    overflow = tmp_path / "overflow.json"
    overflow.write_bytes(raw.replace(b'"schema_version": 2', b'"schema_version": 1e999', 1))
    with pytest.raises(SarbMpcDecisionSourceError, match="nonfinite_number"):
        load_contract_manifest(overflow)

    duplicate = tmp_path / "duplicate.json"
    duplicate.write_bytes(raw.replace(b"{", b'{"currency":"ZAR",', 1))
    with pytest.raises(SarbMpcDecisionSourceError, match="duplicate_key:currency"):
        load_contract_manifest(duplicate)


def test_pinned_file_rejects_tamper_hardlink_and_path_substitution(tmp_path: Path) -> None:
    expected = tmp_path / "expected.bin"
    expected.write_bytes(b"official")
    digest = hashlib.sha256(b"official").hexdigest()

    tampered = tmp_path / "tampered.bin"
    tampered.write_bytes(b"tampered")
    with pytest.raises(SarbMpcDecisionSourceError, match="sha256_mismatch"):
        sarb._read_exact_pinned_file(
            tampered,
            expected_path=tampered,
            expected_bytes=len(b"tampered"),
            expected_sha256=digest,
            require_read_only=False,
        )

    substitute = tmp_path / "substitute.bin"
    substitute.write_bytes(b"official")
    with pytest.raises(SarbMpcDecisionSourceError, match="path_substitution"):
        sarb._read_exact_pinned_file(
            substitute,
            expected_path=expected,
            expected_bytes=len(b"official"),
            expected_sha256=digest,
            require_read_only=False,
        )

    hardlink = tmp_path / "hardlink.bin"
    try:
        os.link(expected, hardlink)
    except OSError as exc:
        pytest.skip(f"hard links unavailable: {exc}")
    with pytest.raises(SarbMpcDecisionSourceError, match="hardlink_forbidden"):
        sarb._read_exact_pinned_file(
            expected,
            expected_path=expected,
            expected_bytes=len(b"official"),
            expected_sha256=digest,
            require_read_only=False,
        )


def test_pinned_file_detects_path_swap_after_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected = tmp_path / "expected.bin"
    expected.write_bytes(b"official")
    digest = hashlib.sha256(b"official").hexdigest()
    real_lstat = os.lstat
    calls = 0

    class ChangedStat:
        def __init__(self, original: os.stat_result) -> None:
            self._original = original
            self.st_ino = int(original.st_ino) + 1

        def __getattr__(self, name: str) -> object:
            return getattr(self._original, name)

    def swapped_lstat(path: object, *args: object, **kwargs: object) -> object:
        nonlocal calls
        value = real_lstat(path, *args, **kwargs)
        if Path(path) == expected:
            calls += 1
            if calls >= 2:
                return ChangedStat(value)
        return value

    monkeypatch.setattr(sarb.os, "lstat", swapped_lstat)
    with pytest.raises(SarbMpcDecisionSourceError, match="path_swapped_after_read"):
        sarb._read_exact_pinned_file(
            expected,
            expected_path=expected,
            expected_bytes=len(b"official"),
            expected_sha256=digest,
            require_read_only=False,
        )


def test_all_zar_pairs_share_one_independent_currency_factor_episode() -> None:
    event = _event("2026-07-23")
    assert event["currency_factor"] == "ZAR"
    assert event["independent_episode_key"] == "ZAR:sarb_mpc_decision:2026-07-23"
    assert "pair" not in event["independent_episode_key"].lower()
    assert "pair_count" not in event


def test_contract_manifest_is_disabled_shadow_only_and_content_addressed() -> None:
    manifest = load_contract_manifest(ROOT / "config" / "sarb_mpc_decision_exact_v2.json")
    assert manifest["source_contract_id"] == CONTRACT_ID
    assert manifest["schedule_facts_sha256"] == SCHEDULE_FACTS_SHA256
    assert manifest["july_decision_facts_sha256"] == JULY_DECISION_FACTS_SHA256
    assert manifest["registered_with_live_collector"] is False
    assert manifest["enabled"] is False
    assert manifest["runtime_supported"] is False
    assert manifest["full_text_retention_enabled"] is False
    assert manifest["hard_guards"]["same_time_global_event_confounding_required"] is True
    assert manifest["hard_guards"]["causal_attribution_status"] == "unresolved"
    assert manifest["separation_contract"]["rewrite_or_relabel_existing_snapshot"] is False
    assert manifest["separation_contract"]["touch_immutable_event_clock"] is False
