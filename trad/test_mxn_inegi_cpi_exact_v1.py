from __future__ import annotations

import ast
import copy
import hashlib
import inspect
import json
import os
import stat
import sys
from pathlib import Path
from unittest import mock

import pytest


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from forex_system.ingestion import mxn_inegi_cpi_exact_v1 as mxn  # noqa: E402
from forex_system.ingestion.mxn_inegi_cpi_exact_v1 import (  # noqa: E402
    COHORT_ID,
    CONTRACT_ID,
    EXPECTED_JULY_FACTS,
    MxnInegiCpiExactSourceError,
    OBSERVATION_MANIFEST_BYTES,
    OBSERVATION_MANIFEST_PATH,
    OBSERVATION_MANIFEST_SHA256,
    SOURCE_ID,
    load_contract_manifest,
    load_exact_inegi_cpi_snapshot,
    load_observation_manifest,
)


CONFIG = ROOT / "config" / "mxn_inegi_cpi_exact_v1.json"
BUILD_MANIFEST = ROOT / "config" / "mxn_inegi_cpi_exact_v1_manifest.json"
ARCHIVE_DIR = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "source_archives"
    / "mxn_inegi_cpi_exact_v1"
)


@pytest.fixture(scope="module")
def snapshot() -> dict:
    return load_exact_inegi_cpi_snapshot()


def _event(snapshot: dict, date: str) -> dict:
    return next(row for row in snapshot["calendar_events"] if row["event_date"] == date)


def _assert_no_trade(value: dict) -> None:
    assert value["research_only"] is True
    assert value["shadow_only"] is True
    assert value["registered_with_live_collector"] is False
    assert value["enabled"] is False
    assert value["runtime_supported"] is False
    for key in (
        "confirmation_eligible",
        "proof_eligible",
        "promotion_eligible",
        "authorization_eligible",
        "execution_eligible",
        "can_authorize",
        "can_place_orders",
    ):
        assert value[key] is False
    for key in ("consensus", "surprise", "direction", "currency_bias", "pair_bias"):
        assert value[key] is None
    assert value["supported_execution_decision"] == "no_trade"


def test_real_archive_snapshot_has_exact_two_clocks(snapshot: dict) -> None:
    assert snapshot["source_id"] == SOURCE_ID
    assert snapshot["source_contract_id"] == CONTRACT_ID
    assert snapshot["source_cohort_id"] == COHORT_ID
    assert len(snapshot["calendar_events"]) == 2
    july = _event(snapshot, "2026-08-07")
    september = _event(snapshot, "2026-09-09")
    assert july["local_time"] == september["local_time"] == "06:00"
    assert july["scheduled_utc"] == "2026-08-07T12:00:00Z"
    assert september["scheduled_utc"] == "2026-09-09T12:00:00Z"
    assert july["reference_period"] == "2026-07"
    assert september["reference_period"] == "2026-08"


def test_calendar_chronology_is_not_backfilled(snapshot: dict) -> None:
    july = _event(snapshot, "2026-08-07")
    september = _event(snapshot, "2026-09-09")
    assert july["calendar_captured_pre_event"] is False
    assert july["prospective_schedule"] is False
    assert july["collection_class"] == "historical_availability_counterfactual_archive_only"
    assert september["calendar_captured_pre_event"] is True
    assert september["prospective_schedule"] is True
    assert september["collection_class"] == "prospective_schedule_only"
    assert july["calendar_observed_utc"] == september["calendar_observed_utc"]
    assert july["calendar_xml_used"] is False
    assert july["calendar_xml_prior_snapshot_backfilled"] is False


def test_timezone_is_fixed_utc_minus_six_for_accepted_clocks() -> None:
    for event_date in ("2026-08-07", "2026-09-09"):
        assert mxn._iso_z(mxn._scheduled_utc(event_date)).endswith("T12:00:00Z")


def test_july_facts_are_exact_pdf_xml_corroborated(snapshot: dict) -> None:
    release = snapshot["historical_july_release"]
    assert release["facts"] == {key: str(value) for key, value in EXPECTED_JULY_FACTS.items()}
    assert release["pdf_xml_corroborated"] is True
    assert release["historical_availability_counterfactual"] is True
    assert release["prospective_observation"] is False
    assert release["collection_class"] == "historical_availability_counterfactual_archive_only"
    assert release["fact_bundle_known_utc"] == "2026-08-17T09:39:55.407843+00:00"
    assert release["calendar_xml_used"] is False
    assert release["calendar_xml_later_revision_quarantined"] is True


@pytest.mark.parametrize(
    ("artifact_id", "fact_name", "expected"),
    [
        ("headline_monthly_xml", "headline_monthly_pct", "0.03"),
        ("headline_annual_xml", "headline_annual_pct", "3.12"),
        ("core_monthly_xml", "core_monthly_pct", "0.23"),
        ("core_annual_xml", "core_annual_pct", "3.95"),
    ],
)
def test_each_xml_exactly_normalizes_declared_three_decimal_precision(
    artifact_id: str, fact_name: str, expected: str
) -> None:
    name, value = mxn._parse_xml_fact(artifact_id)
    assert name == fact_name
    assert str(value) == expected


def test_release_pdf_and_xml_are_independently_parsed() -> None:
    pdf = mxn._parse_release_pdf_facts()
    xml = dict(
        mxn._parse_xml_fact(artifact_id)
        for artifact_id in (
            "headline_annual_xml",
            "headline_monthly_xml",
            "core_annual_xml",
            "core_monthly_xml",
        )
    )
    assert pdf == xml == EXPECTED_JULY_FACTS


def test_all_output_levels_remain_no_trade(snapshot: dict) -> None:
    _assert_no_trade(snapshot)
    _assert_no_trade(snapshot["historical_july_release"])
    for event in snapshot["calendar_events"]:
        _assert_no_trade(event)
    rendered = json.dumps(snapshot, sort_keys=True)
    for forbidden in (
        "expected_net_pips",
        "forecast_mean_bps",
        "allocator_rank",
        "trade_side",
        "signal_direction",
        "raw_pdf",
        "raw_xml",
        "full_text",
    ):
        assert forbidden not in rendered


def test_all_mxn_pairs_are_one_currency_factor_episode(snapshot: dict) -> None:
    for event in snapshot["calendar_events"]:
        assert event["currency"] == "MXN"
        assert event["currency_factor"] == "MXN"
        assert event["independent_episode_key"] == f"MXN:inegi_cpi_release:{event['event_date']}"
        assert "pair" not in event["independent_episode_key"].lower()


def test_calendar_xml_revision_is_truthfully_quarantined(snapshot: dict) -> None:
    drift = snapshot["calendar_xml_later_revision"]
    assert drift == {
        "observed_utc": "2026-08-17T09:39:55.645228+00:00",
        "bytes": 13370,
        "sha256": "4af72939ac4754fa4016f0321f70562df5fcc4440cb3c70c6908fd524a017629",
        "semantic_valid": False,
        "causal_use": "forbidden_for_prior_clock_or_proof",
        "prior_expected_snapshot_retained": False,
        "prior_expected_snapshot_backfilled": False,
        "prior_expected_snapshot_relabelled": False,
    }


def test_observation_manifest_is_exact_closed_and_chronological() -> None:
    manifest = load_observation_manifest()
    assert manifest["calendar_authority"] == "calendar_pdf"
    assert manifest["optional_historical_xmls_retained"] is False
    assert manifest["calendar_xml_prior_expected_unretained"] == {
        "backfilled": False,
        "bytes": 12922,
        "causal_use": "forbidden",
        "relabelled": False,
        "sha256": "e30b070e3db019249bddd3e22e72de504b55466dd23699df873cc3e773d6af91",
    }
    assert {row["artifact_id"] for row in manifest["artifacts"]} == set(mxn._ARTIFACT_SPECS)


def test_manifest_and_all_retained_inputs_are_exact_read_only_regular_files() -> None:
    manifest_path = ROOT / OBSERVATION_MANIFEST_PATH
    assert manifest_path.stat().st_size == OBSERVATION_MANIFEST_BYTES
    assert hashlib.sha256(manifest_path.read_bytes()).hexdigest() == OBSERVATION_MANIFEST_SHA256
    for path in ARCHIVE_DIR.iterdir():
        if not path.is_file():
            continue
        value = os.lstat(path)
        assert stat.S_ISREG(value.st_mode)
        assert value.st_nlink == 1
        assert not path.is_symlink()
        if os.name == "nt":
            assert int(value.st_file_attributes) & int(stat.FILE_ATTRIBUTE_READONLY)


def test_pdf_runtime_reproduces_both_retained_text_artifacts() -> None:
    for artifact_id in ("calendar_pdf", "july_release_pdf"):
        spec = mxn._ARTIFACT_SPECS[artifact_id]
        text = mxn._read_and_verify_pdf_text(artifact_id).encode("utf-8")
        assert len(text) == spec["text_bytes"]
        assert hashlib.sha256(text).hexdigest() == spec["text_sha256"]


def test_snapshot_identity_covers_every_field() -> None:
    first = load_exact_inegi_cpi_snapshot()
    second = load_exact_inegi_cpi_snapshot()
    assert first == second
    claimed = first.pop("snapshot_id")
    assert claimed == "mxn_inegi_cpi_exact_v1_snapshot_" + mxn.canonical_sha256(first)[:24]


def test_public_snapshot_has_no_caller_payload_clock_or_mapping_surface() -> None:
    signature = inspect.signature(load_exact_inegi_cpi_snapshot)
    assert not signature.parameters


@pytest.mark.parametrize(
    "alias",
    [
        "2026-08-17T09:39:53.776879Z",
        "2026-08-17T03:39:53.776879-06:00",
        "2026-08-17 09:39:53.776879+00:00",
        "2026-08-17T09:39:53.7768790+00:00",
    ],
)
def test_timestamp_aliases_are_rejected(alias: str) -> None:
    with pytest.raises(MxnInegiCpiExactSourceError, match="exact_utc|string|canonical"):
        mxn._canonical_utc(alias, label="probe")


@pytest.mark.parametrize(
    "raw",
    [
        b'{"x":1,"x":2}',
        b'{"x":1,"\\u0078":2}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b'{"x":-Infinity}',
        b'{"x":1e999}',
    ],
)
def test_strict_json_rejects_duplicates_nonfinite_and_overflow(raw: bytes) -> None:
    with pytest.raises(MxnInegiCpiExactSourceError):
        mxn._strict_json(raw, label="probe")


@pytest.mark.parametrize(
    ("path_parts", "bad_value"),
    [
        (("schema_version",), True),
        (("schema_version",), 1.0),
        (("enabled",), 0),
        (("calendar_authority",), []),
        (("observation_manifest_bytes",), True),
        (("accepted_clocks", 0, "event_date"), ["2026-08-07"]),
        (("july_facts", "headline_monthly_pct"), 0.03),
        (("artifacts", "calendar_pdf", "semantic_valid"), 1),
        (("hard_guards", "execution_eligible"), 0),
        (("separation_contract", "touch_database"), 0),
    ],
)
def test_contract_manifest_rejects_every_nested_type_alias(
    tmp_path: Path, path_parts: tuple[object, ...], bad_value: object
) -> None:
    manifest = json.loads(CONFIG.read_text(encoding="utf-8"))
    target: object = manifest
    for part in path_parts[:-1]:
        target = target[part]  # type: ignore[index]
    target[path_parts[-1]] = bad_value  # type: ignore[index]
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(MxnInegiCpiExactSourceError, match="deep_typed_mismatch"):
        load_contract_manifest(path)


@pytest.mark.parametrize(
    ("branch", "field"),
    [
        (None, "trade_side"),
        ("hard_guards", "expected_net_pips"),
        ("separation_contract", "maximum_notional"),
        ("prior_calendar_xml", "proof_eligible"),
    ],
)
def test_contract_manifest_rejects_unlisted_fields_at_any_depth(
    tmp_path: Path, branch: str | None, field: str
) -> None:
    manifest = json.loads(CONFIG.read_text(encoding="utf-8"))
    target = manifest if branch is None else manifest[branch]
    target[field] = False
    path = tmp_path / "extra.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(MxnInegiCpiExactSourceError, match="deep_typed_mismatch"):
        load_contract_manifest(path)


def test_contract_json_duplicate_and_overflow_fail_before_semantic_validation(
    tmp_path: Path,
) -> None:
    raw = CONFIG.read_bytes()
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_bytes(raw.replace(b"{", b'{"currency":"MXN",', 1))
    with pytest.raises(MxnInegiCpiExactSourceError, match="duplicate_key"):
        load_contract_manifest(duplicate)
    overflow = tmp_path / "overflow.json"
    overflow.write_bytes(raw.replace(b'"schema_version": 1', b'"schema_version": 1e999', 1))
    with pytest.raises(MxnInegiCpiExactSourceError, match="nonfinite_number"):
        load_contract_manifest(overflow)


def test_contract_is_disabled_unregistered_and_deep_exact() -> None:
    manifest = load_contract_manifest(CONFIG)
    assert manifest["registered_with_live_collector"] is False
    assert manifest["enabled"] is False
    assert manifest["runtime_supported"] is False
    assert manifest["hard_guards"]["execution_eligible"] is False
    assert manifest["separation_contract"] == {
        "existing_banxico_source_untouched": True,
        "touch_live_news_config": False,
        "touch_live_collector": False,
        "touch_immutable_event_clock": False,
        "touch_database": False,
        "touch_executor_or_account": False,
        "write_process_state": False,
    }


def test_xml_fact_tamper_and_duplicate_observation_fail_closed() -> None:
    raw = mxn._read_artifact("headline_monthly_xml")
    tampered = raw.replace(b'CurrentValue="0.02999999999999999900"', b'CurrentValue="9.99999999999999999900"')
    with mock.patch.object(mxn, "_read_artifact", return_value=tampered):
        with pytest.raises(MxnInegiCpiExactSourceError, match="xml_fact_value_mismatch"):
            mxn._parse_xml_fact("headline_monthly_xml")
    duplicate = raw.replace(b"</SERIE>", raw[raw.find(b"<Obs"):raw.find(b"</SERIE>")] + b"</SERIE>")
    with mock.patch.object(mxn, "_read_artifact", return_value=duplicate):
        with pytest.raises(MxnInegiCpiExactSourceError, match="xml_series_contract_mismatch"):
            mxn._parse_xml_fact("headline_monthly_xml")


def test_pdf_fact_duplicate_and_mismatch_fail_closed() -> None:
    original = mxn._read_and_verify_pdf_text("july_release_pdf")
    duplicate = original + " aumentó 0.03 % respecto al mes anterior"
    with mock.patch.object(mxn, "_read_and_verify_pdf_text", return_value=duplicate):
        with pytest.raises(MxnInegiCpiExactSourceError, match="missing_or_ambiguous"):
            mxn._parse_release_pdf_facts()
    mismatch = original.replace("aumentó 0.03 % respecto", "aumentó 9.03 % respecto", 1)
    with mock.patch.object(mxn, "_read_and_verify_pdf_text", return_value=mismatch):
        with pytest.raises(MxnInegiCpiExactSourceError, match="fact_bundle_mismatch"):
            mxn._parse_release_pdf_facts()


def test_calendar_duplicate_clock_row_fails_closed() -> None:
    original = mxn._read_and_verify_pdf_text("calendar_pdf")
    duplicate = original.replace("07/agosto                         Julio", "07/agosto                         Julio\n07/agosto                         Julio", 1)
    with mock.patch.object(mxn, "_read_and_verify_pdf_text", return_value=duplicate):
        with pytest.raises(MxnInegiCpiExactSourceError, match="missing_or_ambiguous"):
            mxn._parse_calendar(load_observation_manifest())


def test_path_substitution_tamper_hardlink_and_symlink_fail_closed(tmp_path: Path) -> None:
    expected = tmp_path / "expected.bin"
    expected.write_bytes(b"official")
    digest = hashlib.sha256(b"official").hexdigest()
    substitute = tmp_path / "substitute.bin"
    substitute.write_bytes(b"official")
    with pytest.raises(MxnInegiCpiExactSourceError, match="path_substitution"):
        mxn._read_exact_pinned_file(
            substitute,
            expected_path=expected,
            expected_bytes=8,
            expected_sha256=digest,
            require_read_only=False,
        )
    with pytest.raises(MxnInegiCpiExactSourceError, match="sha256_mismatch"):
        mxn._read_exact_pinned_file(
            expected,
            expected_path=expected,
            expected_bytes=8,
            expected_sha256=hashlib.sha256(b"tampered").hexdigest(),
            require_read_only=False,
        )
    hardlink = tmp_path / "hardlink.bin"
    try:
        os.link(expected, hardlink)
    except OSError as exc:
        pytest.skip(f"hard links unavailable: {exc}")
    with pytest.raises(MxnInegiCpiExactSourceError, match="hardlink_forbidden"):
        mxn._read_exact_pinned_file(
            expected,
            expected_path=expected,
            expected_bytes=8,
            expected_sha256=digest,
            require_read_only=False,
        )


def test_symlink_component_branch_rejects_without_touching_archive() -> None:
    path = mxn._workspace_path(mxn.OBSERVATION_MANIFEST_PATH)
    with mock.patch.object(Path, "is_symlink", return_value=True):
        # The implementation relies on lstat/reparse attributes rather than
        # Path.is_symlink, so simulate that exact branch directly.
        real_lstat = os.lstat

        class SymlinkStat:
            def __init__(self, original: os.stat_result) -> None:
                self._original = original
                self.st_mode = stat.S_IFLNK | 0o777

            def __getattr__(self, name: str) -> object:
                return getattr(self._original, name)

        def fake_lstat(value: object, *args: object, **kwargs: object) -> object:
            result = real_lstat(value, *args, **kwargs)
            if Path(value) == path:
                return SymlinkStat(result)
            return result

        with mock.patch.object(mxn.os, "lstat", side_effect=fake_lstat):
            with pytest.raises(MxnInegiCpiExactSourceError, match="symlink_component"):
                mxn._assert_no_reparse_components(path)


def test_file_identity_race_after_read_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "official.bin"
    path.write_bytes(b"official")
    digest = hashlib.sha256(b"official").hexdigest()
    real_lstat = os.lstat
    calls = 0

    class ChangedStat:
        def __init__(self, original: os.stat_result) -> None:
            self._original = original
            self.st_ino = int(original.st_ino) + 1

        def __getattr__(self, name: str) -> object:
            return getattr(self._original, name)

    def fake_lstat(value: object, *args: object, **kwargs: object) -> object:
        nonlocal calls
        result = real_lstat(value, *args, **kwargs)
        if Path(value) == path:
            calls += 1
            if calls >= 3:
                return ChangedStat(result)
        return result

    with mock.patch.object(mxn.os, "lstat", side_effect=fake_lstat):
        with pytest.raises(MxnInegiCpiExactSourceError, match="path_swapped_after_read"):
            mxn._read_exact_pinned_file(
                path,
                expected_path=path,
                expected_bytes=8,
                expected_sha256=digest,
                require_read_only=False,
            )


def test_runtime_module_has_no_network_write_database_or_process_surface() -> None:
    module_path = ROOT / "src" / "forex_system" / "ingestion" / "mxn_inegi_cpi_exact_v1.py"
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imports = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports.update(
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    )
    assert imports.isdisjoint({"requests", "httpx", "urllib", "socket", "sqlite3", "subprocess"})
    source = module_path.read_text(encoding="utf-8")
    for token in ("write_text(", "write_bytes(", "os.replace(", "unlink(", "mkdir("):
        assert token not in source


def test_source_is_not_registered_or_imported_by_runtime() -> None:
    hits: list[Path] = []
    for path in (ROOT / "src").rglob("*.py"):
        if path.name == "mxn_inegi_cpi_exact_v1.py":
            continue
        if "mxn_inegi_cpi_exact_v1" in path.read_text(encoding="utf-8", errors="ignore"):
            hits.append(path)
    assert hits == []
    live_config = (ROOT / "config" / "news_sources_v1.json").read_text(
        encoding="utf-8", errors="ignore"
    )
    assert "mxn_inegi_cpi_exact_v1" not in live_config


def test_capture_utility_is_not_imported_and_refuses_recapture() -> None:
    path = ROOT / "tools" / "capture_mxn_inegi_cpi_exact_v1.py"
    source = path.read_text(encoding="utf-8")
    assert "capture_manifest_already_exists_refusing_recapture" in source
    assert "if __name__ == \"__main__\"" in source
    for runtime in (ROOT / "src").rglob("*.py"):
        assert "capture_mxn_inegi_cpi_exact_v1" not in runtime.read_text(
            encoding="utf-8", errors="ignore"
        )


def test_final_build_manifest_is_disabled_and_byte_exact() -> None:
    manifest = json.loads(BUILD_MANIFEST.read_text(encoding="utf-8"))
    assert manifest["state"] == {
        "enabled": False,
        "registered": False,
        "runtime_supported": False,
        "independent_review_state": "pending",
    }
    assert manifest["policy"]["research_only"] is True
    assert manifest["policy"]["execution_eligible"] is False
    assert manifest["policy"]["supported_execution_decision"] == "no_trade"
    for artifact in manifest["artifacts"].values():
        path = ROOT / artifact["relative_path"]
        assert path.is_file()
        assert path.stat().st_size == artifact["byte_length"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact["sha256"]
