from __future__ import annotations

import ast
import copy
import datetime as dt
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

try:
    import forex_system.ingestion.prospective_event_response_v6_r4 as response_v6_r4
    from forex_system.contracts.currency_state import stable_hash
    from forex_system.features.currency_state_official_context_v5 import attach_official_fact_context_v5
    from forex_system.ingestion.official_fact_adapter_v5 import OfficialFactAdapterV5
except ModuleNotFoundError:
    import src.forex_system.ingestion.prospective_event_response_v6_r4 as response_v6_r4
    from src.forex_system.contracts.currency_state import stable_hash
    from src.forex_system.features.currency_state_official_context_v5 import attach_official_fact_context_v5
    from src.forex_system.ingestion.official_fact_adapter_v5 import OfficialFactAdapterV5

from test_oanda_currency_state_official_context import base_snapshot


ROOT = Path(__file__).resolve().parent
UTC = dt.timezone.utc
FIXED_UPSTREAM_CUTOFF = "2026-08-17T08:00:00+00:00"


def _aligned_base() -> dict:
    _, base = base_snapshot()
    base["decision_cutoff_utc"] = FIXED_UPSTREAM_CUTOFF
    for horizon in base["horizons"].values():
        horizon["decision_cutoff_utc"] = FIXED_UPSTREAM_CUTOFF
    identity = {
        key: base[key]
        for key in (
            "contract_id", "contract_sha256", "decision_cutoff_utc",
            "completed_bar_cutoff_utc", "input_identity", "horizons",
        )
    }
    base["snapshot_id"] = "currency_state_snapshot_" + stable_hash(identity)[:24]
    return base


@pytest.fixture(scope="module")
def governed() -> dict:
    official = OfficialFactAdapterV5().as_of(FIXED_UPSTREAM_CUTOFF)
    base = _aligned_base()
    context = attach_official_fact_context_v5(base, official)
    return {"official": official, "base": base, "context": context}


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _event_and_instrument(governed: dict, *, require_jpy_quote: bool = False) -> tuple[dict, str]:
    for event in governed["official"]["upcoming_events"]:
        affected = [
            instrument
            for instrument in response_v6_r4.CANONICAL_INSTRUMENTS
            if event["currency"] in instrument.split("_")
            and (not require_jpy_quote or instrument.endswith("_JPY"))
        ]
        if affected:
            return event, affected[0]
    raise AssertionError("fixture has no suitable governed event")


def _observation(
    governed: dict,
    *,
    horizon: int = 300,
    require_jpy_quote: bool = False,
) -> tuple[dict, str]:
    event, instrument = _event_and_instrument(governed, require_jpy_quote=require_jpy_quote)
    scheduled = dt.datetime.fromisoformat(event["scheduled_utc"].replace("Z", "+00:00"))
    start_quote = scheduled + dt.timedelta(seconds=1)
    end_target = scheduled + dt.timedelta(seconds=horizon)
    end_quote = end_target + dt.timedelta(seconds=2)
    pip = response_v6_r4.CANONICAL_PIP_BY_INSTRUMENT[instrument]
    anchor = 100.0 if pip == 0.01 else 1.1
    response = {
        "event_id": event["event_id"],
        "event_version_id": event["event_version_id"],
        "currency": event["currency"],
        "instrument": instrument,
        "horizon_sec": horizon,
        "event_scheduled_utc": _iso(scheduled),
        "start_target_utc": _iso(scheduled),
        "end_target_utc": _iso(end_target),
        "start_quote_time_utc": _iso(start_quote),
        "end_quote_time_utc": _iso(end_quote),
        "start_known_utc": _iso(start_quote + dt.timedelta(seconds=1)),
        "end_known_utc": _iso(end_quote + dt.timedelta(seconds=1)),
        "start_bid": anchor,
        "start_ask": anchor + 2 * pip,
        "end_bid": anchor + 10 * pip,
        "end_ask": anchor + 12 * pip,
        "pip": pip,
        "start_source_payload_sha256": "a" * 64,
        "end_source_payload_sha256": "b" * 64,
        "start_source_record_id": "fixture-start-record",
        "end_source_record_id": "fixture-end-record",
    }
    capture = _iso(end_quote + dt.timedelta(seconds=2))
    return response, capture


def _build(
    governed: dict,
    observations: list[dict] | None = None,
    *,
    mode: str = "prospective_capture_test_fixture",
    capture: str | dt.datetime | None = None,
) -> dict:
    if observations is None:
        observation, default_capture = _observation(governed)
        observations = [observation]
    else:
        _, default_capture = _observation(governed)
    return response_v6_r4.build_test_snapshot(
        context_snapshot=governed["context"],
        base_snapshot=governed["base"],
        official_snapshot=governed["official"],
        executable_responses=observations,
        capture_cutoff_utc=capture if capture is not None else default_capture,
        mode=mode,
    )


def test_rejected_r1_r2_and_r3_bytes_remain_unchanged() -> None:
    expected = {
        "src/forex_system/ingestion/prospective_event_response_v6.py": "be25bce1c50dd7a34b83fdbf1a8fc335646a2cc31332f47ed87a019f4e32498c",
        "config/prospective_event_response_v6_contract.json": "d5aae12a41bb4fd7638b7b9c40a97bb086f5f98f7916d13368589d164af7527c",
        "config/prospective_event_response_v6_manifest.json": "0b2a315080520945d410bc8c2c403c75c4ad230c4998c31f8bd6c1f9499b9504",
        "test_prospective_event_response_v6.py": "c14f5af6c445d290299732475598dcd3e689af65d46794eb12772a616c00e09d",
        "src/forex_system/ingestion/prospective_event_response_v6_r2.py": "4587ff1b70fc99d4114aac9a1164b92a75c913dad9307863802dcba494bafeca",
        "config/prospective_event_response_v6_r2_contract.json": "18e022c4b01514a57c9a60dea3dfd6bbd95cd01f709c95cfb7799e11c29285ff",
        "config/prospective_event_response_v6_r2_manifest.json": "2cdffa4c5c7372894dc71a8d0e3ef1598de8492fec9a4f06940b926d18cc319b",
        "test_prospective_event_response_v6_r2.py": "d10571ffe771a335880233ada923be47b423b066b72658a28f88fce54cd878a3",
        "src/forex_system/ingestion/prospective_event_response_v6_r3.py": "9e709d569c17ed07fcd5348f5cb07fbd8f020846bd011c913993e2cc248fb8f1",
        "config/prospective_event_response_v6_r3_contract.json": "6598258b20a122b8e3ecb50400cb5698de55c262c255c737b0eb0d3ac6af6e06",
        "config/prospective_event_response_v6_r3_manifest.json": "58be11db773b3c53f2cdae27a260f6c5709c094d53e841bdde86f26c6d058e9d",
        "test_prospective_event_response_v6_r3.py": "5bb7a060a65a2b941033c22eeafdf20aeef613fb75e31373149042f69111a1dc",
    }
    for relative_path, sha256 in expected.items():
        assert hashlib.sha256((ROOT / relative_path).read_bytes()).hexdigest() == sha256


def test_exact_r3_dependency_closure_and_review_certificate() -> None:
    assert response_v6_r4.verify_dependency_closure() == {
        "contract_sha256": response_v6_r4.CONTRACT_SHA256,
        "review_certificate_sha256": response_v6_r4.REVIEW_CERTIFICATE_SHA256,
        "review_certificate_id": response_v6_r4.REVIEW_CERTIFICATE_ID,
        "pinned_r3_artifact_count": 23,
        "r3_artifact_list_sha256": response_v6_r4.R3_ARTIFACT_LIST_SHA256,
        "closure_verified": True,
    }


def test_successor_manifest_pins_own_and_transitive_closure_without_activation() -> None:
    assert response_v6_r4.verify_candidate_manifest() == {
        "manifest_id": "prospective_event_response_v6_r4_manifest_20260817",
        "artifact_count": 6,
        "transitive_upstream_artifact_count": 23,
        "candidate_closure_verified": True,
        "review_state": "pending_independent_review",
    }


def test_runtime_imports_no_r1_rejected_predecessor_or_broker() -> None:
    source = Path(response_v6_r4.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module)
    forbidden = (
        "prospective_event_response_v6", "prospective_event_response_v6_r2",
        "prospective_event_response_v6_r3",
        "official_fact_adapter_v3", "official_fact_adapter_v4",
        "currency_state_official_context_v3", "currency_state_official_context_v4",
        "oandapy", "requests", "httpx", "sqlite3",
    )
    assert all(not any(name.endswith(item) or name.startswith(item) for item in forbidden) for name in imports)
    assert "__main__" not in source


def test_fixed_cutoff_snapshot_is_deterministic_complete_and_noncanonical(governed: dict) -> None:
    first = _build(governed)
    second = _build(governed)
    assert first == second
    assert first["snapshot_id"].startswith("prospective_event_response_v6_r4_test_")
    assert first["currency_count"] == 21
    assert first["instrument_count"] == 68
    assert first["horizon_count"] == 5
    assert len(first["context_matrix"]) == 340
    assert first["diagnostic_response_count"] == 1
    assert first["proof_row_count"] == 0
    assert first["test_only"] is True
    assert first["canonical_output"] is False
    assert first["execution_eligible"] is False
    assert first["can_place_orders"] is False
    assert first["can_promote"] is False
    assert first["can_authorize"] is False
    assert first["supported_execution_decision"] == "no_trade"


def test_cross_process_snapshot_is_byte_deterministic() -> None:
    script = r'''
import hashlib, json
import test_prospective_event_response_v6_r4 as t
from forex_system.ingestion import prospective_event_response_v6_r4 as v
g=t.governed.__wrapped__(); observation,capture=t._observation(g)
snapshot=v.build_test_snapshot(
    context_snapshot=g["context"], base_snapshot=g["base"], official_snapshot=g["official"],
    executable_responses=[observation], capture_cutoff_utc=capture,
    mode="prospective_capture_test_fixture")
print(hashlib.sha256(json.dumps(snapshot,sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest())
'''
    env = {**dict(__import__("os").environ), "PYTHONPATH": str(ROOT / "src")}
    values = []
    for _ in range(2):
        completed = subprocess.run(
            [sys.executable, "-c", script], cwd=ROOT, env=env,
            capture_output=True, text=True, check=False,
        )
        assert completed.returncode == 0, completed.stderr
        values.append(completed.stdout.strip())
    assert values[0] == values[1]


def test_exact_v5_r3_snapshot_and_input_identities_are_bound(governed: dict) -> None:
    binding = _build(governed)["source_binding"]
    assert binding["official_fact_snapshot_id"] == governed["official"]["snapshot_id"]
    assert binding["currency_state_official_context_snapshot_id"] == governed["context"]["snapshot_id"]
    assert binding["base_currency_state_snapshot_id"] == governed["base"]["snapshot_id"]
    assert binding["canonical_pip_mapping_sha256"] == response_v6_r4.CANONICAL_PIP_MAPPING_SHA256


def test_no_upstream_prediction_can_become_forecast_ev_rank_or_basis(governed: dict) -> None:
    snapshot = _build(governed)
    rows = snapshot["plans"] + snapshot["response_diagnostics"] + snapshot["context_matrix"]
    for row in rows:
        for field in (
            "forecast_mean_pips", "forecast_mean_bps", "forecast_absolute_move_bps",
            "forecast_probability", "cost_clear_probability", "expected_value_pips",
            "allocator_rank", "direction_selected",
        ):
            if field in row:
                assert row[field] is None
        assert row["basis_eligible"] is False
        assert row["proof_eligible"] is False


def test_zero_quote_inputs_create_zero_response_or_proof_rows(governed: dict) -> None:
    snapshot = _build(governed, [])
    assert snapshot["valid_input_response_count"] == 0
    assert snapshot["diagnostic_response_count"] == 0
    assert snapshot["duplicate_valid_response_count"] == 0
    assert snapshot["response_diagnostics"] == []
    assert snapshot["proof_row_count"] == 0
    assert len(snapshot["context_matrix"]) == 340


@pytest.mark.parametrize("require_jpy_quote", [False, True])
def test_executable_chronology_bid_ask_and_canonical_pip_are_exact(governed: dict, require_jpy_quote: bool) -> None:
    observation, capture = _observation(governed, require_jpy_quote=require_jpy_quote)
    snapshot = _build(governed, [observation], capture=capture)
    row = snapshot["response_diagnostics"][0]
    assert row["pip"] == response_v6_r4.CANONICAL_PIP_BY_INSTRUMENT[row["instrument"]]
    assert row["start_alignment_sec"] == 1.0
    assert row["end_alignment_sec"] == 2.0
    assert row["actual_quote_duration_sec"] == 301.0
    assert row["start_spread_pips"] == pytest.approx(2.0)
    assert row["end_spread_pips"] == pytest.approx(2.0)
    assert row["gross_long_mid_pips"] == pytest.approx(10.0)
    assert row["gross_short_mid_pips"] == pytest.approx(-10.0)
    assert row["long_after_executable_spread_pips"] == pytest.approx(8.0)
    assert row["short_after_executable_spread_pips"] == pytest.approx(-12.0)


@pytest.mark.parametrize("wrong_pip", [0.001, 0.01, 0.1, 1.0])
def test_arbitrary_pip_scaling_is_rejected(governed: dict, wrong_pip: float) -> None:
    observation, capture = _observation(governed)
    canonical = response_v6_r4.CANONICAL_PIP_BY_INSTRUMENT[observation["instrument"]]
    if wrong_pip == canonical:
        wrong_pip *= 10
    observation["pip"] = wrong_pip
    with pytest.raises(response_v6_r4.ProspectiveEventResponseV6R4Error, match="pip_not_exact"):
        _build(governed, [observation], capture=capture)


def test_naive_datetime_capture_is_rejected_and_aware_offset_is_exact(governed: dict) -> None:
    observation, capture = _observation(governed)
    naive = dt.datetime.fromisoformat(capture.replace("Z", ""))
    with pytest.raises(response_v6_r4.ProspectiveEventResponseV6R4Error, match="naive"):
        _build(governed, [observation], capture=naive)
    aware = dt.datetime.fromisoformat(capture.replace("Z", "+00:00")).astimezone(
        dt.timezone(dt.timedelta(hours=-4))
    )
    snapshot = _build(governed, [observation], capture=aware)
    assert snapshot["capture_cutoff_utc"] == capture


def test_deterministic_earliest_valid_duplicate_wins_independent_of_input_order(governed: dict) -> None:
    first, capture = _observation(governed)
    later = copy.deepcopy(first)
    later["start_known_utc"] = _iso(
        dt.datetime.fromisoformat(later["start_known_utc"].replace("Z", "+00:00"))
        + dt.timedelta(seconds=2)
    )
    later["end_known_utc"] = _iso(
        dt.datetime.fromisoformat(later["end_known_utc"].replace("Z", "+00:00"))
        + dt.timedelta(seconds=2)
    )
    later["start_source_payload_sha256"] = "c" * 64
    later["end_source_payload_sha256"] = "d" * 64
    later["start_source_record_id"] = "later-start"
    later["end_source_record_id"] = "later-end"
    capture_later = _iso(
        dt.datetime.fromisoformat(capture.replace("Z", "+00:00")) + dt.timedelta(seconds=3)
    )
    forward = _build(governed, [later, first], capture=capture_later)
    reverse = _build(governed, [first, later], capture=capture_later)
    assert forward == reverse
    assert forward["valid_input_response_count"] == 2
    assert forward["diagnostic_response_count"] == 1
    assert forward["duplicate_valid_response_count"] == 1
    assert forward["response_diagnostics"][0]["start_source_record_id"] == "fixture-start-record"



def test_fractional_duplicate_order_uses_actual_instants_not_iso_lexical_order(governed: dict) -> None:
    earlier, capture = _observation(governed)
    later = copy.deepcopy(earlier)
    for field in ("start_known_utc", "end_known_utc"):
        later[field] = _iso(
            dt.datetime.fromisoformat(later[field].replace("Z", "+00:00"))
            + dt.timedelta(microseconds=100_000)
        )
    later["start_source_payload_sha256"] = "c" * 64
    later["end_source_payload_sha256"] = "d" * 64
    later["start_source_record_id"] = "fractional-later-start"
    later["end_source_record_id"] = "fractional-later-end"
    later_capture = _iso(
        dt.datetime.fromisoformat(capture.replace("Z", "+00:00"))
        + dt.timedelta(seconds=1)
    )
    forward = _build(governed, [later, earlier], capture=later_capture)
    reverse = _build(governed, [earlier, later], capture=later_capture)
    assert forward == reverse
    assert forward["response_diagnostics"][0]["start_source_record_id"] == "fixture-start-record"


def test_pip_view_is_read_only_and_datetime_subclass_cannot_run_callback(governed: dict) -> None:
    with pytest.raises(TypeError):
        response_v6_r4.CANONICAL_PIP_BY_INSTRUMENT["EUR_DKK"] = 1.0

    class TrapDatetime(dt.datetime):
        callback_called = False

        def utcoffset(self):
            type(self).callback_called = True
            raise AssertionError("datetime subclass callback must not execute")

    observation, _ = _observation(governed)
    trap = TrapDatetime(2026, 8, 20, 6, 5, 4, tzinfo=UTC)
    with pytest.raises(response_v6_r4.ProspectiveEventResponseV6R4Error, match="must_be_utc_string"):
        _build(governed, [observation], capture=trap)
    assert TrapDatetime.callback_called is False




def test_custom_timezone_and_identity_subclasses_cannot_run_callbacks(governed: dict) -> None:
    class TrapTimezone(dt.tzinfo):
        callback_count = 0

        def utcoffset(self, value):
            type(self).callback_count += 1
            raise AssertionError("custom timezone callback must not execute")

    observation, capture = _observation(governed)
    capture_value = dt.datetime.fromisoformat(capture.replace("Z", "+00:00"))
    trapped_capture = capture_value.replace(tzinfo=TrapTimezone())
    with pytest.raises(
        response_v6_r4.ProspectiveEventResponseV6R4Error,
        match="tzinfo_must_be_exact_builtin_timezone",
    ):
        _build(governed, [observation], capture=trapped_capture)
    assert TrapTimezone.callback_count == 0

    class TrapString(str):
        callback_count = 0

        def _trap(self):
            type(self).callback_count += 1
            raise AssertionError("identity string callback must not execute")

        def __hash__(self):
            self._trap()

        def __eq__(self, other):
            self._trap()

        def split(self, *args, **kwargs):
            self._trap()

        def endswith(self, *args, **kwargs):
            self._trap()

    for field in ("event_id", "event_version_id", "currency", "instrument"):
        attacked = copy.deepcopy(observation)
        attacked[field] = TrapString(attacked[field])
        with pytest.raises(
            response_v6_r4.ProspectiveEventResponseV6R4Error,
            match="must_be_exact_nonempty_string",
        ):
            _build(governed, [attacked], capture=capture)
    assert TrapString.callback_count == 0


def test_closed_schema_key_and_mode_subclasses_reject_before_callback(governed: dict) -> None:
    class TrapKey(str):
        armed = False
        callback_count = 0

        def __hash__(self):
            if type(self).armed:
                type(self).callback_count += 1
                raise AssertionError("closed schema key callback must not execute")
            return str.__hash__(self)

        def __eq__(self, other):
            if type(self).armed:
                type(self).callback_count += 1
                raise AssertionError("closed schema key callback must not execute")
            return str.__eq__(self, other)

    observation, capture = _observation(governed)
    value = observation.pop("event_id")
    key = TrapKey("event_id")
    observation[key] = value
    TrapKey.armed = True
    with pytest.raises(
        response_v6_r4.ProspectiveEventResponseV6R4Error,
        match="closed_schema_mismatch",
    ):
        _build(governed, [observation], capture=capture)
    assert TrapKey.callback_count == 0

    class TrapMode(str):
        callback_count = 0

        def __hash__(self):
            type(self).callback_count += 1
            raise AssertionError("mode callback must not execute")

    with pytest.raises(
        response_v6_r4.ProspectiveEventResponseV6R4Error,
        match="only_explicit",
    ):
        response_v6_r4.build_test_snapshot(
            context_snapshot=governed["context"],
            base_snapshot=governed["base"],
            official_snapshot=governed["official"],
            executable_responses=[],
            capture_cutoff_utc=FIXED_UPSTREAM_CUTOFF,
            mode=TrapMode("prospective_capture_test_fixture"),
        )
    assert TrapMode.callback_count == 0


def test_content_addressed_response_identity_changes_with_economics_or_provenance(governed: dict) -> None:
    first, capture = _observation(governed)
    changed = copy.deepcopy(first)
    pip = changed["pip"]
    changed["start_bid"] += pip
    changed["start_ask"] += pip
    changed["start_source_payload_sha256"] = "c" * 64
    rows = []
    for observation in (first, changed):
        rows.append(_build(governed, [observation], capture=capture)["response_diagnostics"][0])
    assert rows[0]["response_id"] != rows[1]["response_id"]
    assert rows[0]["response_content_sha256"] != rows[1]["response_content_sha256"]
    assert rows[0]["long_after_executable_spread_pips"] != rows[1]["long_after_executable_spread_pips"]


@pytest.mark.parametrize(
    ("field", "delta", "match"),
    [
        ("start_quote_time_utc", -1, "start_alignment"),
        ("end_quote_time_utc", 46, "end_alignment"),
        ("start_known_utc", 30, "start_knowledge_clock"),
        ("end_known_utc", 30, "end_knowledge_clock"),
    ],
)
def test_future_stale_alignment_and_clock_poison_fail_closed(governed: dict, field: str, delta: int, match: str) -> None:
    observation, capture = _observation(governed)
    if field == "start_quote_time_utc":
        base = dt.datetime.fromisoformat(observation["start_target_utc"].replace("Z", "+00:00"))
    elif field == "end_quote_time_utc":
        base = dt.datetime.fromisoformat(observation["end_target_utc"].replace("Z", "+00:00"))
    elif field == "start_known_utc":
        base = dt.datetime.fromisoformat(observation["start_quote_time_utc"].replace("Z", "+00:00"))
    else:
        base = dt.datetime.fromisoformat(observation["end_quote_time_utc"].replace("Z", "+00:00"))
    observation[field] = _iso(base + dt.timedelta(seconds=delta))
    with pytest.raises(response_v6_r4.ProspectiveEventResponseV6R4Error, match=match):
        _build(governed, [observation], capture=capture)


def test_replay_and_prospective_capture_are_separate_nonproof_partitions(governed: dict) -> None:
    prospective = _build(governed, mode="prospective_capture_test_fixture")
    replay = _build(governed, mode="replay_diagnostic_test_fixture")
    assert prospective["snapshot_id"] != replay["snapshot_id"]
    assert prospective["response_diagnostics"][0]["response_id"] != replay["response_diagnostics"][0]["response_id"]
    assert prospective["response_diagnostics"][0]["partition"] == "diagnostic_prospective_capture"
    assert replay["response_diagnostics"][0]["partition"] == "diagnostic_replay"
    assert prospective["proof_row_count"] == replay["proof_row_count"] == 0
    with pytest.raises(response_v6_r4.ProspectiveEventResponseV6R4Error, match="only_explicit"):
        response_v6_r4.build_test_snapshot(
            context_snapshot=governed["context"], base_snapshot=governed["base"],
            official_snapshot=governed["official"], executable_responses=[],
            capture_cutoff_utc=FIXED_UPSTREAM_CUTOFF, mode="prospective_proof",
        )


def test_reconstruction_rejects_any_snapshot_mutation(governed: dict) -> None:
    observation, capture = _observation(governed)
    snapshot = _build(governed, [observation], capture=capture)
    response_v6_r4.validate_test_snapshot(
        snapshot, context_snapshot=governed["context"], base_snapshot=governed["base"],
        official_snapshot=governed["official"], executable_responses=[observation],
        capture_cutoff_utc=capture, mode="prospective_capture_test_fixture",
    )
    tampered = copy.deepcopy(snapshot)
    tampered["context_matrix"][0]["expected_value_pips"] = 1.0
    with pytest.raises(response_v6_r4.ProspectiveEventResponseV6R4Error, match="snapshot_reconstruction"):
        response_v6_r4.validate_test_snapshot(
            tampered, context_snapshot=governed["context"], base_snapshot=governed["base"],
            official_snapshot=governed["official"], executable_responses=[observation],
            capture_cutoff_utc=capture, mode="prospective_capture_test_fixture",
        )


def test_contract_and_module_expose_no_live_worker_or_canonical_output_path() -> None:
    contract = json.loads(
        (ROOT / "config/prospective_event_response_v6_r4_contract.json").read_text(encoding="utf-8")
    )
    assert contract["state"]["enabled"] is False
    assert contract["state"]["registered"] is False
    assert contract["state"]["research_worker_enabled"] is False
    assert contract["state"]["collector_enabled"] is False
    assert contract["state"]["canonical_output_initialized"] is False
    assert contract["output_policy"]["canonical_output_forbidden"] is True
    source = Path(response_v6_r4.__file__).read_text(encoding="utf-8")
    for forbidden in ("sqlite3", "research_ledgers", "order_submission", "oandapy", "requests"):
        assert forbidden not in source
