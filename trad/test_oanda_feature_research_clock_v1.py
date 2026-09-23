"""Owned synthetic clock boundaries; no monitor import, network or real state."""

import ast
import copy
import datetime as dt
import email.utils
import hashlib
import json
import math
import os
from pathlib import Path
import types
from unittest import mock

import pytest

import oanda_feature_research_clock_v1 as gate


NOW = dt.datetime(2026, 9, 13, 12, tzinfo=dt.timezone.utc).timestamp()


def clock_state(**changes):
    result = {"schema_version": 1, "generated_utc": "2026-09-13T12:00:00+00:00",
              "status": "ok", "timestamp_normalization_trusted": True,
              "host_clock_synchronized": True, "clock_discontinuity_active": False,
              "clock_sources_consistent": True, "source_fresh": True,
              "source_age_sec": 0, "broker_clock_lead_sec": 0.2,
              "broker_clock_sample_count": 32, "external_https_clock": {}}
    result.update(changes)
    return result


def external(*, offset=0.0, probe_epoch=NOW, rtt=100, precision=1):
    return {"status": "ok", "offset_sec": offset, "round_trip_ms": rtt,
            "precision_sec": precision,
            "server_date": email.utils.format_datetime(
                dt.datetime.fromtimestamp(probe_epoch + offset, dt.timezone.utc), usegmt=True)}


def valid(state=None, *, now=NOW):
    return gate.validate_clock_state(clock_state() if state is None else state, now_epoch=now)


def test_valid_proof_exact_owned_hash_no_timestamp_normalization():
    state = clock_state()
    before = copy.deepcopy(state)
    result = valid(state)
    assert result["valid"] is True and result["reason"] == "clock_aligned"
    evidence = result["evidence"]
    assert evidence["applied_offset_sec"] == 0
    assert evidence["clock_state_sha256"] == hashlib.sha256(
        json.dumps(state, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                   allow_nan=False).encode()).hexdigest()
    assert result["can_place_orders"] is False and result["authorizes_activation"] is False
    state["broker_clock_lead_sec"] = 14400
    assert evidence["clock_state"] == before
    evidence["clock_state"]["status"] = "degraded"
    assert state["status"] == "ok"


@pytest.mark.parametrize("age,expected", [(0, True), (90, True), (90.000001, False), (-0.000001, False)])
def test_exact_state_age_bounds(age, expected):
    assert valid(now=NOW + age)["valid"] is expected


@pytest.mark.parametrize("changes", [
    {"status": "degraded"}, {"status": "blocked"},
    {"schema_version": True}, {"schema_version": 1.0},
    {"timestamp_normalization_trusted": 1}, {"host_clock_synchronized": False},
    {"clock_discontinuity_active": True}, {"clock_discontinuity_active": 0},
    {"clock_sources_consistent": False}, {"clock_sources_consistent": 1},
    {"broker_clock_sample_count": 31}, {"broker_clock_sample_count": 32.0},
    {"broker_clock_sample_count": True}, {"broker_clock_lead_sec": 2.000001},
    {"broker_clock_lead_sec": True}, {"source_age_sec": -0.001},
    {"source_age_sec": 90.000001}, {"source_fresh": 1},
    {"generated_utc": "2026-09-13T12:00:00"},
    {"generated_utc": "2026-09-13T13:00:00+01:00"},
    {"generated_utc": NOW}, {"external_https_clock": [1]},
])
def test_required_state_refusals(changes):
    assert valid(clock_state(**changes))["valid"] is False


@pytest.mark.parametrize("value", [True, None, "1", float("nan"), float("inf"), 10**1000])
def test_actual_boundary_requires_finite_number(value):
    assert valid(now=value)["reason"] == "clock_now_invalid"


def test_combined_reference_age_and_original_proof_cannot_be_renewed():
    state = clock_state(source_age_sec=20)
    assert valid(state, now=NOW + 70)["valid"] is True
    assert valid(state, now=NOW + 70.001)["valid"] is False
    old = valid()["evidence"]["clock_state"]
    new = clock_state(generated_utc="2026-09-13T12:02:00+00:00")
    assert valid(new, now=NOW + 120)["valid"] is True
    assert valid(old, now=NOW + 120)["valid"] is False


def test_existing_https_only_mitigated_path_remains_usable():
    state = clock_state(status="mitigated", source_fresh=False, source_age_sec=999,
                        broker_clock_sample_count=0, broker_clock_lead_sec=None,
                        external_https_clock=external())
    result = valid(state)
    assert result["valid"] is True
    assert result["evidence"]["aligned_source"] == "external_https"


@pytest.mark.parametrize("change", [
    {"probe_epoch": NOW - 91}, {"probe_epoch": NOW + 1},
    {"offset": 3}, {"rtt": 2000.01}, {"rtt": -1},
    {"precision": 1.01}, {"precision": -1}, {"precision": True},
])
def test_https_exact_independent_reference_requirements(change):
    state = clock_state(source_fresh=False, external_https_clock=external(**change))
    assert valid(state)["valid"] is False


def test_disagreement_even_when_one_source_aligned():
    state = clock_state(broker_clock_lead_sec=3, external_https_clock=external(offset=0))
    assert valid(state)["reason"] == "clock_sources_disagree"
    state["broker_clock_lead_sec"] = 2
    assert valid(state)["valid"] is True
    state["broker_clock_lead_sec"] = -2
    state["external_https_clock"] = external(offset=2)
    assert valid(state)["reason"] == "clock_sources_disagree"


def test_naive_external_date_cannot_establish_alignment():
    state = clock_state(source_fresh=False, external_https_clock=external())
    state["external_https_clock"]["server_date"] = "Sun, 13 Sep 2026 12:00:00"
    assert valid(state)["valid"] is False


def test_acceptance_equivalence_to_actual_source_extracted_original_contract():
    # Read only the canonical Python source and compile exactly four pure defs.
    # No original module/project dependencies/monitor are imported or executed.
    source = Path(__file__).with_name("oanda_local_news_sentiment_repair_v2.py")
    tree = ast.parse(source.read_text(encoding="utf-8"))
    names = {"epoch", "encoded", "digest", "validate_clock_state"}
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in nodes} == names
    namespace = dict(dt=dt, math=math, json=json, hashlib=hashlib, email=email,
                     CLOCK_CONTRACT=gate.UPSTREAM_CONTRACT)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), namespace)
    fixtures = [clock_state(), clock_state(status="mitigated"),
                clock_state(source_age_sec=20), clock_state(broker_clock_lead_sec=14400),
                clock_state(clock_discontinuity_active=True),
                clock_state(source_fresh=False, external_https_clock=external()),
                clock_state(broker_clock_lead_sec=3, external_https_clock=external())]
    for state in fixtures:
        for age in (0, 70, 90, 91, -1):
            try:
                expected = namespace["validate_clock_state"](state, NOW + age)
            except ValueError:
                expected = None
            actual = valid(state, now=NOW + age)
            assert actual["valid"] is (expected is not None)
            if expected is not None:
                for field in ("clock_state_sha256", "observed_epoch", "integrity_generated_epoch", "applied_offset_sec"):
                    assert actual["evidence"][field] == expected[field]


def test_missing_read_returns_visible_refusal(tmp_path):
    result = gate.read_verified_clock(tmp_path / "absent.json", now_epoch=NOW)
    assert result["valid"] is False and result["reason"] == "clock_read_failed"
    assert list(tmp_path.iterdir()) == []


def test_reader_samples_actual_boundary_after_read_and_reports_exact_file(tmp_path):
    path = tmp_path / "clock.json"
    raw = json.dumps(clock_state(), indent=2).encode()
    path.write_bytes(raw)
    original = gate._decoded
    decoded = []

    def decode(value):
        decoded.append(True)
        return original(value)

    def clock():
        assert decoded
        return NOW + 1

    with mock.patch.object(gate, "_decoded", side_effect=decode), mock.patch.object(gate.time, "time", side_effect=clock):
        result = gate.read_verified_clock(path)
    assert result["valid"] is True
    assert result["evidence"]["observed_epoch"] == NOW + 1
    assert result["evidence"]["source"]["file_sha256"] == hashlib.sha256(raw).hexdigest()
    assert path.read_bytes() == raw


@pytest.mark.parametrize("tail", [
    ',"status":"ok"', ',"external_https_clock":{"offset_sec":0,"offset_sec":1}',
    ',"external_https_clock":{"off\\u0073et_sec":0,"offset_sec":1}',
    ',"extra":NaN', ',"extra":Infinity', ',"extra":1e999',
])
def test_reader_rejects_ambiguous_and_nonfinite_json(tmp_path, tail):
    path = tmp_path / "clock.json"
    raw = json.dumps(clock_state())[:-1] + tail + "}"
    path.write_text(raw, encoding="utf-8")
    result = gate.read_verified_clock(path, now_epoch=NOW)
    assert result["valid"] is False
    assert path.read_text(encoding="utf-8") == raw


def test_file_byte_bound_refused_before_read(tmp_path):
    path = tmp_path / "clock.json"
    path.write_bytes(b"x" * (gate.MAX_STATE_BYTES + 1))
    result = gate.read_verified_clock(path, now_epoch=NOW)
    assert result["reason"] == "clock_state_byte_bound"


@pytest.mark.parametrize("path", ["D:/forbidden/clock.json", "relative.json", "C:relative.json",
                                  "C:/one/../clock.json", "C:/clock.json:stream", "//server/share/x"])
def test_bad_path_refused_before_any_filesystem_probe(path):
    with mock.patch.object(gate.Path, "lstat", side_effect=AssertionError("forbidden probe")):
        result = gate.read_verified_clock(path, now_epoch=NOW)
    assert result["reason"] == "clock_plain_c_path_required"


def test_reparse_ancestor_refused_before_open(tmp_path):
    path = tmp_path / "clock.json"
    path.write_text(json.dumps(clock_state()), encoding="utf-8")
    original = gate.Path.lstat

    def lstat(item):
        info = original(item)
        if item == tmp_path:
            return types.SimpleNamespace(st_mode=info.st_mode, st_file_attributes=1024)
        return info

    with mock.patch.object(gate.Path, "lstat", lstat), mock.patch.object(gate.Path, "open", side_effect=AssertionError("open refused")):
        assert gate.read_verified_clock(path, now_epoch=NOW)["reason"] == "clock_reparse_path_refused"


def test_changed_descriptor_identity_refused(tmp_path):
    path = tmp_path / "clock.json"
    path.write_text(json.dumps(clock_state()), encoding="utf-8")
    original = gate.os.fstat
    calls = []

    def changed(fd):
        info = original(fd)
        calls.append(1)
        if len(calls) == 2:
            return types.SimpleNamespace(st_dev=info.st_dev, st_ino=info.st_ino,
                                         st_size=info.st_size, st_mtime_ns=info.st_mtime_ns + 1)
        return info

    with mock.patch.object(gate.os, "fstat", side_effect=changed):
        assert gate.read_verified_clock(path, now_epoch=NOW)["reason"] == "clock_source_changed_during_read"


def test_nonobject_and_oversized_pure_states_refused():
    assert valid([])["reason"] == "clock_state_object_required"
    assert valid(clock_state(extra="x" * gate.MAX_STATE_BYTES))["reason"] == "clock_state_byte_bound"
    assert valid(clock_state(extra=float("nan")))["reason"] == "clock_state_json_invalid"


def test_module_has_no_project_import_or_process_network_startup():
    tree = ast.parse(Path(gate.__file__).read_text(encoding="utf-8"))
    imports = [node for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))]
    names = {alias.name.split(".")[0] for node in imports if isinstance(node, ast.Import) for alias in node.names}
    assert names == {"datetime", "email", "hashlib", "json", "math", "os", "stat", "time"}
    assert all(node.module in {"__future__", "pathlib"} for node in imports if isinstance(node, ast.ImportFrom))
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                   and (node.func.attr in {"mkdir", "unlink", "system", "Popen", "connect", "urlopen"}
                        or (node.func.attr == "replace" and isinstance(node.func.value, ast.Name)
                            and node.func.value.id == "os"))
                   for node in ast.walk(tree))
