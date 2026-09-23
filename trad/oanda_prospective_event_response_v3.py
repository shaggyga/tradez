#!/usr/bin/env python3
"""Run one atomic, research-only prospective event-response v3 cycle."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Mapping

try:
    from forex_system.ingestion.immutable_event_clock import (
        build_clock_attestation,
        reconstruct_as_of,
    )
    from forex_system.ingestion.prospective_event_response_v3 import (
        EXPECTED_EVENT_PIPELINE_VERSION,
        append_cycle,
        build_event_plans,
        build_samples,
        collection_workset,
        insert_records,
        ledger_summary,
        load_contract,
        mature_outcome_rows,
        open_ledger,
        quote_snapshot_candidates,
        read_active_capture_scope,
        read_evidence,
        register_frozen_cohort,
        transient_attempt_record,
        worker_cycle_record,
    )
except ModuleNotFoundError:
    from src.forex_system.ingestion.immutable_event_clock import (
        build_clock_attestation,
        reconstruct_as_of,
    )
    from src.forex_system.ingestion.prospective_event_response_v3 import (
        EXPECTED_EVENT_PIPELINE_VERSION,
        append_cycle,
        build_event_plans,
        build_samples,
        collection_workset,
        insert_records,
        ledger_summary,
        load_contract,
        mature_outcome_rows,
        open_ledger,
        quote_snapshot_candidates,
        read_active_capture_scope,
        read_evidence,
        register_frozen_cohort,
        transient_attempt_record,
        worker_cycle_record,
    )


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_CONTRACT = ROOT / "config" / "prospective_event_response_capture_v3.json"
DEFAULT_UNIVERSE = ROOT / "config" / "currency_state_engine_v2.json"
DEFAULT_POLICY_DEPENDENCIES = ROOT / "config" / "currency_policy_dependency_registry_v1.json"
DEFAULT_CLOCK_DB = DATA / "research_ledgers" / "immutable_event_clock_v2.sqlite"
DEFAULT_QUOTE_SNAPSHOT = DATA / "state" / "practice_007_market_quotes_v1.json"
DEFAULT_CLOCK_INTEGRITY = DATA / "state" / "clock_integrity_v1.json"
DEFAULT_LEDGER = DATA / "research_ledgers" / "prospective_event_response_v3.sqlite"
DEFAULT_LOCK = DATA / "research_ledgers" / "prospective_event_response_v3.writer.lock"
DEFAULT_STATE = DATA / "state" / "prospective_event_response_v3.json"
DEFAULT_REPORT = (
    DATA
    / "reports"
    / "prospective_event_response"
    / "PROSPECTIVE_EVENT_RESPONSE_CURRENT.md"
)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


class SingleWriterLock:
    """Fail-closed process lock; no stale lock is silently broken."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def __enter__(self) -> "SingleWriterLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.fd = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
        except FileExistsError as exc:
            raise RuntimeError(f"prospective_event_response_v3_writer_lock_exists:{self.path}") from exc
        payload = {
            "pid": os.getpid(),
            "created_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "research_only": True,
        }
        os.write(self.fd, (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"))
        os.fsync(self.fd)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


def record_active_preflight_failure(
    *,
    ledger_path: Path,
    lock_path: Path,
    contract: Mapping[str, Any],
    observed_utc: dt.datetime,
    reason: str,
    error: str,
) -> dict[str, Any]:
    """Append a nonterminal failure for every currently due unsampled plan.

    This is the minimal fail-closed path used when target-window sampling
    cannot even begin because a required clock/artifact is stale or absent.
    It never creates an uninitialized cohort and never terminally excludes an
    offset.
    """

    if not ledger_path.is_file():
        return {"status": "ledger_not_initialized", "attempts_inserted": 0}
    observed = observed_utc.astimezone(dt.timezone.utc)
    lateness = float(
        (contract.get("sampling") or {}).get("maximum_target_lateness_sec") or 0
    )
    with SingleWriterLock(lock_path):
        connection = open_ledger(ledger_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT p.plan_id,p.target_utc
                FROM event_response_plans_v3 p
                LEFT JOIN event_response_samples_v3 s ON s.plan_id=p.plan_id
                LEFT JOIN event_response_terminal_exclusions_v3 x ON x.plan_id=p.plan_id
                WHERE s.plan_id IS NULL AND x.plan_id IS NULL
                """
            ).fetchall()
            due: list[tuple[str, str]] = []
            for row in rows:
                target = dt.datetime.fromisoformat(
                    str(row["target_utc"]).replace("Z", "+00:00")
                ).astimezone(dt.timezone.utc)
                if target <= observed <= target + dt.timedelta(seconds=lateness):
                    due.append((str(row["plan_id"]), target.isoformat()))
            attempts = [
                transient_attempt_record(
                    reason,
                    source_ref=plan_id,
                    observed_utc=observed,
                    phase="active_target_preflight",
                    details={
                        "target_utc": target_utc,
                        "error": str(error)[:1000],
                        "terminal": False,
                    },
                )
                for plan_id, target_utc in due
            ]
            inserted = append_cycle(connection, attempts=attempts)[
                "attempts_inserted"
            ]
            connection.commit()
            return {
                "status": "transient_attempts_appended",
                "attempts_inserted": int(inserted),
                "due_plan_count": len(due),
            }
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def _file_hash(path: Path, *, required: bool = True) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError:
        if required:
            raise
        return ""


def _static_fingerprint(
    *,
    contract: Mapping[str, Any],
    contract_bytes: bytes,
    universe_bytes: bytes,
    normalized_universe_sha256: str,
    dependency_bytes: bytes,
) -> dict[str, Any]:
    paths = {
        "v3_core_module_sha256": ROOT / "src" / "forex_system" / "ingestion" / "prospective_event_response_v3.py",
        "v2_dependency_module_sha256": ROOT / "src" / "forex_system" / "ingestion" / "prospective_event_response.py",
        "collector_entrypoint_sha256": Path(__file__).resolve(),
        "adaptive_worker_sha256": ROOT / "oanda_prospective_event_response_worker_v3.py",
        "immutable_clock_normalizer_sha256": ROOT / "src" / "forex_system" / "ingestion" / "immutable_event_clock.py",
        "immutable_clock_cli_sha256": ROOT / "oanda_immutable_event_clock.py",
        "immutable_clock_worker_sha256": ROOT / "oanda_immutable_event_clock_worker.py",
        "event_producer_sha256": ROOT / "oanda_news_event_tagger.py",
        "event_seed_sha256": ROOT / "config" / "news_event_seed_v1.json",
        "quote_producer_sha256": ROOT / "oanda_practice_top_signal_executor.py",
        "quote_crosscheck_sha256": ROOT / "oanda_quote_transport_crosscheck.py",
    }
    hashes = {
        key: _file_hash(path, required=key != "quote_crosscheck_sha256")
        for key, path in paths.items()
    }
    event_contract = {
        "producer_implementation": "oanda_news_event_tagger.py",
        "expected_pipeline_version": contract["required_source_pipeline_version"],
        "required_event_fields": [
            "event_version_id",
            "upstream_event_id",
            "scheduled_utc",
            "ledger_effective_known_utc",
            "direct_currencies",
            "event_time_basis",
            "source_verified",
            "source_id",
            "source_contract_id",
            "source_cohort_id",
            "original_fact_known_utc",
            "event_snapshot_first_observed_utc",
            "event_snapshot_first_trusted_observed_utc",
            "source_contract_first_observed_utc",
            "source_contract_first_trusted_observed_utc",
            "event_availability_utc",
            "material_content_sha256",
            "timing_precision",
            "clock_semantics",
            "independent_domestic_event",
        ],
        "immutable_clock_contract_id": contract["immutable_event_clock_contract_id"],
    }
    quote_contract = dict(contract.get("market_observation_contract") or {})
    return {
        "contract_config_sha256": _sha256_bytes(contract_bytes),
        **hashes,
        "instrument_universe_normalized_sha256": normalized_universe_sha256,
        "instrument_universe_artifact_sha256": _sha256_bytes(universe_bytes),
        "policy_dependency_registry_sha256": _sha256_bytes(dependency_bytes),
        "policy_dependency_contract_id": contract[
            "linked_policy_dependency_contract_id"
        ],
        "event_producer_contract_sha256": _sha256_json(event_contract),
        "event_producer_contract": event_contract,
        "quote_producer_contract_sha256": _sha256_json(quote_contract),
        "quote_producer_contract": quote_contract,
        "target_offsets_sec": list(contract["target_offsets_sec"]),
        "worker_cadence": dict(contract["worker_cadence"]),
        "source_producer_ownership": contract["source_producer_ownership"],
        "worker_runs_catalog_sync": False,
        "worker_runs_immutable_clock_writer": False,
        "event_instance_admission": contract["event_instance_admission"],
        "event_catalog_frozen_at_cohort_start": False,
        "admit_previously_unseen_future_exact_events": True,
        "new_event_requires_pre_release_observation": True,
        "never_reclassify_or_backfill_prior_event_instances": True,
        "whole_executable_universe_per_event": True,
        "linked_policy_dependencies_assign_direction": False,
        "linked_policy_dependencies_are_not_independent_confirmation": True,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }


def _source_lineage(
    clock_snapshot: Mapping[str, Any], market_attestation: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "source_pipeline_version": clock_snapshot.get("source_pipeline_version"),
        "clock_snapshot_id": clock_snapshot.get("snapshot_id"),
        "semantic_snapshot_events_sha256": clock_snapshot.get(
            "semantic_snapshot_events_sha256"
        ),
        "semantic_snapshot_manifest_sha256": clock_snapshot.get(
            "semantic_snapshot_manifest_sha256"
        ),
        "clock_observation_id": clock_snapshot.get("clock_observation_id"),
        "clock_observed_utc": clock_snapshot.get("clock_observed_utc"),
        "clock_observation_source_generated_utc": clock_snapshot.get(
            "clock_observation_source_generated_utc"
        ),
        "clock_observation_events_sha256": clock_snapshot.get(
            "clock_observation_events_sha256"
        ),
        "clock_observation_manifest_sha256": clock_snapshot.get(
            "clock_observation_manifest_sha256"
        ),
        "clock_observation_semantic_clock_sha256": clock_snapshot.get(
            "clock_observation_semantic_clock_sha256"
        ),
        "event_clock_attestation": dict(clock_snapshot.get("clock_attestation") or {}),
        "market_clock_attestation": dict(market_attestation),
    }


def _coverage(
    *,
    contract: Mapping[str, Any],
    plans: list[Mapping[str, Any]],
    clock_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    expected = sorted({str(value) for value in contract.get("expected_currency_nodes") or []})
    represented = sorted(
        {
            currency
            for plan in plans
            for currency in str(plan.get("instrument") or "").split("_")
            if currency
        }
    )
    direct = sorted(
        {
            currency
            for plan in plans
            for currency in json.loads(str(plan.get("direct_currencies_json") or "[]"))
        }
    )
    linked = sorted(
        {
            currency
            for plan in plans
            for row in json.loads(
                str(plan.get("linked_policy_dependencies_json") or "[]")
            )
            for currency in [str(row.get("dependent_currency") or "")]
            if currency
        }
    )
    source_currencies = sorted(
        {
            str(currency).upper()
            for event in clock_snapshot.get("events") or []
            if isinstance(event, Mapping)
            for currency in event.get("direct_currencies") or []
        }
    )
    huf_exact_statistics_clock_present = any(
        isinstance(event, Mapping)
        and "HUF" in {str(value).upper() for value in event.get("direct_currencies") or []}
        and str(event.get("source_id") or "")
        == "hungary_ksh_headline_cpi_release_clock_exact_v3"
        and str(event.get("source_contract_id") or "")
        == "hungary_ksh_headline_cpi_release_clock_exact_v3_verified_policy_bytes_20260817"
        and str(event.get("clock_semantics") or "")
        == "domestic_official_statistical_release"
        and str(event.get("timing_precision") or "") == "minute"
        and str(event.get("event_time_basis") or "") == "scheduled_release"
        and event.get("independent_domestic_event") is True
        and event.get("linked_policy_factor") is False
        for event in clock_snapshot.get("events") or []
    )
    huf_exact_policy_clock_present = any(
        isinstance(event, Mapping)
        and "HUF" in {str(value).upper() for value in event.get("direct_currencies") or []}
        and str(event.get("clock_semantics") or "")
        == "domestic_official_policy_release"
        and str(event.get("timing_precision") or "") == "minute"
        and str(event.get("event_time_basis") or "") == "scheduled_release"
        and event.get("independent_domestic_event") is True
        and event.get("linked_policy_factor") is False
        for event in clock_snapshot.get("events") or []
    )
    gap_reasons = {}
    for currency in sorted(set(expected) - set(direct)):
        if currency in linked:
            gap_reasons[currency] = "linked_policy_dependency_not_independent_direct_clock"
        elif currency in {"DKK", "HKD"}:
            gap_reasons[currency] = "no_independent_exact_clock_in_current_catalog"
        elif currency in source_currencies:
            gap_reasons[currency] = "present_in_source_but_not_v3_exact_eligible"
        else:
            gap_reasons[currency] = "no_exact_direct_clock_in_current_catalog"
    return {
        "expected_currency_nodes": expected,
        "whole_universe_represented_currencies": represented,
        "whole_universe_coverage": f"{len(set(represented) & set(expected))}/{len(expected)}",
        "direct_exact_clock_currencies": direct,
        "direct_exact_clock_coverage": f"{len(set(direct) & set(expected))}/{len(expected)}",
        "linked_policy_dependency_currencies": linked,
        "direct_clock_gap_reasons": gap_reasons,
        "huf_exact_statistics_clock_present": huf_exact_statistics_clock_present,
        "huf_exact_policy_clock_present": huf_exact_policy_clock_present,
        "huf_policy_exact_clock_gap_preserved": not huf_exact_policy_clock_present,
    }


def _render_report(payload: Mapping[str, Any]) -> str:
    ledger = payload["ledger"]
    counts = ledger["table_counts"]
    coverage = payload["coverage"]
    lines = [
        "# Prospective official-event response capture v3",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        "Research-only whole-universe event-response evidence. This cohort cannot trade, promote, authorize, or assign event direction.",
        "",
        f"- Cohort: `{payload['cohort_id']}`",
        f"- Cohort start: `{payload['frozen_cohort']['cohort_start_utc']}`",
        f"- Static fingerprint: `{payload['frozen_cohort']['static_fingerprint_sha256']}`",
        f"- Supported execution decision: **{payload['supported_execution_decision']}**",
        f"- Frozen plans: **{counts['event_response_plans_v3']:,}**",
        f"- Captured samples: **{counts['event_response_samples_v3']:,}**",
        f"- Matured outcomes: **{counts['event_response_outcomes_v3']:,}**",
        f"- Exact proof-eligible outcomes: **{ledger['proof_evaluation_eligible_outcomes']:,}**",
        f"- Inexact diagnostic outcomes: **{ledger['inexact_diagnostic_outcomes']:,}**",
        f"- Terminal exclusions: **{counts['event_response_terminal_exclusions_v3']:,}**",
        f"- Nonterminal attempt diagnostics: **{counts['event_response_attempt_diagnostics_v3']:,}**",
        f"- Relationship rows: `{json.dumps(ledger['relationship_counts'], sort_keys=True)}`",
        f"- 21-node whole-universe representation: **{coverage['whole_universe_coverage']}**",
        f"- Independent exact direct clocks: **{coverage['direct_exact_clock_coverage']}**",
        f"- Linked policy nodes: `{', '.join(coverage['linked_policy_dependency_currencies']) or 'none'}`",
        f"- Exact-clock gaps: `{json.dumps(coverage['direct_clock_gap_reasons'], sort_keys=True)}`",
        f"- SQLite/full-record/FK verification: **{ledger['verification']['status']}**",
        "",
        "## Frozen safeguards",
        "",
        "- The event catalog is append-admissible, not frozen at cohort start: each fresh immutable semantic snapshot may add previously unseen future exact events observed causally before release.",
        "- Event instances already planned are never reclassified, rewritten, or backfilled; interim source rows do not define a permanent event universe.",
        "- Event availability is the maximum of original fact-known, immutable snapshot first-observed, and source-contract/cohort first-observed clocks; newly deployed parsers cannot be backdated.",
        "- Every eligible official event snapshots all 68 executable pairs at 11 predeclared offsets.",
        "- Rows are labeled direct_leg, linked_policy_dependency, or unaffected_control.",
        "- ECB→DKK and FOMC→HKD share the driver event factor and are never independent confirmation or assigned direction.",
        "- Exact event/schedule version is required through offset-zero baseline; later endpoints bind to that frozen baseline lineage.",
        "- Event-clock observation, upstream publication, and clock attestation must each be no more than 90 seconds old at planning and capture.",
        "- Proof eligibility requires both samples within five seconds of targets, valid ordered clocks, positive near-horizon duration, and outcome knowledge after both samples.",
        "- Temporary missing/stale reads are attempt diagnostics. Only expiry or prebaseline revision/removal is terminal.",
        "- IDs hash stable identity; payload hashes cover every persisted evidence field and are verified on insert/read.",
        "- One writer lock plus BEGIN IMMEDIATE makes plan/read/sample/outcome appends atomic.",
        "- The target worker is a pure consumer: supervisor-owned source/clock producers remain independent, and no heavy catalog synchronization can block 1 Hz sampling.",
        "- The adaptive worker is not supervisor-wired and defaults to one explicit cycle.",
        "",
    ]
    return "\n".join(lines)


def _snapshot_already_planned(ledger_path: Path, snapshot_id: str) -> bool:
    if not ledger_path.is_file():
        return False
    try:
        connection = sqlite3.connect(
            f"file:{ledger_path.resolve().as_posix()}?mode=ro", uri=True, timeout=5.0
        )
        row = connection.execute(
            "SELECT 1 FROM event_response_plans_v3 WHERE clock_snapshot_id=? LIMIT 1",
            (snapshot_id,),
        ).fetchone()
        connection.close()
        return row is not None
    except sqlite3.Error:
        return False


def _light_ledger_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    tables = (
        "event_response_cohort_manifests_v3",
        "event_response_plans_v3",
        "event_response_samples_v3",
        "event_response_outcomes_v3",
        "event_response_terminal_exclusions_v3",
        "event_response_attempt_diagnostics_v3",
        "event_response_worker_cycles_v3",
    )
    counts = {
        table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in tables
    }
    proof = int(
        connection.execute(
            "SELECT COUNT(*) FROM event_response_outcomes_v3 WHERE proof_evaluation_eligible=1"
        ).fetchone()[0]
    )
    inexact = counts["event_response_outcomes_v3"] - proof
    return {
        "schema_version": 3,
        "contract_id": "prospective_event_response_capture_v3_20260817",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
        "table_counts": counts,
        "proof_evaluation_eligible_outcomes": proof,
        "inexact_diagnostic_outcomes": inexact,
        "verification": {
            "status": "bounded_active_scope_verified_full_scan_deferred_to_idle_cycle",
            "violations": [],
        },
    }


def run_once(
    *,
    contract_path: Path = DEFAULT_CONTRACT,
    universe_path: Path = DEFAULT_UNIVERSE,
    dependency_path: Path = DEFAULT_POLICY_DEPENDENCIES,
    event_clock_db: Path = DEFAULT_CLOCK_DB,
    quote_snapshot_path: Path = DEFAULT_QUOTE_SNAPSHOT,
    clock_integrity_path: Path = DEFAULT_CLOCK_INTEGRITY,
    ledger_path: Path = DEFAULT_LEDGER,
    lock_path: Path = DEFAULT_LOCK,
    state_path: Path = DEFAULT_STATE,
    report_path: Path = DEFAULT_REPORT,
    observed_utc: dt.datetime | None = None,
    cycle_mode: str = "manual_single_cycle_not_supervisor_wired",
    cadence_sec: float = 0.0,
) -> dict[str, Any]:
    decision_cutoff = (observed_utc or dt.datetime.now(tz=dt.timezone.utc)).astimezone(
        dt.timezone.utc
    )
    started = decision_cutoff
    contract_bytes = contract_path.read_bytes()
    contract = load_contract(contract_path)
    universe_bytes = universe_path.read_bytes()
    universe = json.loads(universe_bytes.decode("utf-8-sig"))
    instruments = sorted({str(value).upper() for value in universe.get("instruments") or []})
    if len(instruments) != 68:
        raise RuntimeError(f"expected_68_instruments_found_{len(instruments)}")
    dependency_bytes = dependency_path.read_bytes()
    dependencies = json.loads(dependency_bytes.decode("utf-8-sig"))
    if str(dependencies.get("contract_id") or "") != str(
        contract.get("linked_policy_dependency_contract_id") or ""
    ):
        raise RuntimeError("linked_policy_dependency_contract_mismatch")
    clock_snapshot = reconstruct_as_of(event_clock_db, decision_cutoff)
    if not clock_snapshot.get("snapshot_id"):
        raise RuntimeError("no_immutable_event_clock_snapshot_at_cutoff")
    quote_bytes = quote_snapshot_path.read_bytes()
    quote_payload = json.loads(quote_bytes.decode("utf-8-sig"))
    collector_observed = (
        observed_utc or dt.datetime.now(tz=dt.timezone.utc)
    ).astimezone(dt.timezone.utc)
    if collector_observed < decision_cutoff:
        raise RuntimeError("collector_observed_before_decision_cutoff")
    market_attestation = build_clock_attestation(
        clock_integrity_path,
        collector_observed,
        maximum_age_sec=float(contract["maximum_clock_attestation_age_sec"]),
    )
    normalized_universe_sha256 = _sha256_json(instruments)
    snapshot_preplanned = _snapshot_already_planned(
        ledger_path, str(clock_snapshot.get("snapshot_id") or "")
    )
    requested_fast_active = cycle_mode == "active_target_window"
    planning_needed = not snapshot_preplanned and (
        not requested_fast_active or not ledger_path.is_file()
    )
    if planning_needed:
        planned = build_event_plans(
            clock_snapshot,
            contract=contract,
            instruments=instruments,
            policy_dependency_registry=dependencies,
            planned_utc=decision_cutoff,
        )
    else:
        planned = {
            "plans": [],
            "attempts": [],
            "instrument_universe_sha256": normalized_universe_sha256,
            "event_instance_count": 0,
            "relationship_counts": {},
        }
    use_fast_active_path = requested_fast_active and snapshot_preplanned
    observations = quote_snapshot_candidates(
        quote_payload,
        contract=contract,
        artifact_sha256=_sha256_bytes(quote_bytes),
        collector_observed_utc=collector_observed,
        market_clock_attestation=market_attestation,
    )
    static_fingerprint = _static_fingerprint(
        contract=contract,
        contract_bytes=contract_bytes,
        universe_bytes=universe_bytes,
        normalized_universe_sha256=normalized_universe_sha256,
        dependency_bytes=dependency_bytes,
    )
    source_lineage = _source_lineage(clock_snapshot, market_attestation)

    with SingleWriterLock(lock_path):
        connection = open_ledger(ledger_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            cohort = register_frozen_cohort(
                connection,
                contract=contract,
                static_fingerprint=static_fingerprint,
                cohort_start_source_lineage=source_lineage,
            )
            if use_fast_active_path:
                existing_plan_ids: set[str] = set()
            else:
                existing_plans, _, _ = read_evidence(connection)
                existing_plan_ids = {row["plan_id"] for row in existing_plans}
            new_plans = [row for row in planned["plans"] if row["plan_id"] not in existing_plan_ids]
            first = append_cycle(
                connection,
                plans=new_plans,
                attempts=planned["attempts"],
            )
            if use_fast_active_path:
                scope_plans, baseline_samples = read_active_capture_scope(
                    connection,
                    collector_observed_utc=collector_observed,
                    maximum_target_lateness_sec=float(
                        contract["sampling"]["maximum_target_lateness_sec"]
                    ),
                )
                baseline_plan_ids = {
                    str(row.get("plan_id") or "") for row in baseline_samples
                }
                active_plans = [
                    row
                    for row in scope_plans
                    if str(row.get("plan_id") or "") not in baseline_plan_ids
                    and dt.datetime.fromisoformat(
                        str(row["target_utc"]).replace("Z", "+00:00")
                    )
                    <= collector_observed
                    <= dt.datetime.fromisoformat(
                        str(row["target_utc"]).replace("Z", "+00:00")
                    )
                    + dt.timedelta(
                        seconds=float(
                            contract["sampling"]["maximum_target_lateness_sec"]
                        )
                    )
                ]
                workset = {
                    "active_plans": active_plans,
                    "expired_exclusions": [],
                    "active_plan_count": len(active_plans),
                    "expired_plan_count": 0,
                }
                all_samples = baseline_samples
            else:
                all_plans, all_samples, all_terminal = read_evidence(connection)
                workset = collection_workset(
                    all_plans,
                    all_samples,
                    all_terminal,
                    contract=contract,
                    collector_observed_utc=collector_observed,
                )
            captured = build_samples(
                workset["active_plans"],
                observations,
                existing_samples=all_samples,
                current_clock_snapshot=clock_snapshot,
                contract=contract,
                collector_observed_utc=collector_observed,
            )
            second = append_cycle(
                connection,
                samples=captured["samples"],
                terminal_exclusions=[
                    *workset["expired_exclusions"],
                    *captured["terminal_exclusions"],
                ],
                attempts=captured["attempts"],
            )
            if use_fast_active_path:
                outcomes = mature_outcome_rows(
                    scope_plans,
                    [*baseline_samples, *captured["samples"]],
                    contract=contract,
                    recorded_utc=collector_observed,
                )
            else:
                all_plans, all_samples, _ = read_evidence(connection)
                outcomes = mature_outcome_rows(
                    all_plans,
                    all_samples,
                    contract=contract,
                    recorded_utc=collector_observed,
                )
            third = append_cycle(connection, outcomes=outcomes)
            cycle_counts = {
                key: int(first.get(key, 0) + second.get(key, 0) + third.get(key, 0))
                for key in {
                    "plans_inserted",
                    "samples_inserted",
                    "outcomes_inserted",
                    "terminal_exclusions_inserted",
                    "attempts_inserted",
                }
            }
            completed = dt.datetime.now(tz=dt.timezone.utc)
            cycle_row = worker_cycle_record(
                cohort_id=str(contract["cohort_id"]),
                started_utc=started,
                completed_utc=completed,
                mode=cycle_mode,
                cadence_sec=cadence_sec,
                counts=cycle_counts,
            )
            insert_records(
                connection, "event_response_worker_cycles_v3", [cycle_row]
            )
            ledger = (
                _light_ledger_summary(connection)
                if use_fast_active_path
                else ledger_summary(connection)
            )
            if not use_fast_active_path and ledger["verification"]["status"] != "ok":
                raise RuntimeError("v3_ledger_verification_failed")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    if use_fast_active_path:
        return {
            "schema_version": 3,
            "contract_id": contract["contract_id"],
            "cohort_id": contract["cohort_id"],
            "generated_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "supported_execution_decision": "no_trade",
            "collection_mode": cycle_mode,
            "supervisor_wired": False,
            "bounded_active_scope": True,
            "cycle": {
                **cycle_counts,
                "active_plan_count": workset["active_plan_count"],
                "candidate_market_observation_count": len(observations),
            },
            "ledger": ledger,
        }

    # Reopen after commit so the emitted idle/manual state is itself a
    # complete read-verified view.
    connection = open_ledger(ledger_path)
    try:
        all_plans, _, _ = read_evidence(connection)
        ledger = ledger_summary(connection)
    finally:
        connection.close()
    coverage = _coverage(
        contract=contract,
        plans=all_plans,
        clock_snapshot=clock_snapshot,
    )
    payload = {
        "schema_version": 3,
        "contract_id": contract["contract_id"],
        "cohort_id": contract["cohort_id"],
        "generated_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
        "collection_mode": cycle_mode,
        "supervisor_wired": False,
        "frozen_cohort": {
            "status": cohort["status"],
            "cohort_id": cohort["cohort_id"],
            "cohort_start_utc": cohort["cohort_start_utc"],
            "research_generation": cohort["research_generation"],
            "static_fingerprint_sha256": cohort["static_fingerprint_sha256"],
            "parent_cohort_id": cohort["parent_cohort_id"],
            "parent_cohort_disposition": cohort["parent_cohort_disposition"],
        },
        "cycle": {
            **cycle_counts,
            "active_plan_count": workset["active_plan_count"],
            "expired_plan_count": workset["expired_plan_count"],
            "candidate_market_observation_count": len(observations),
            "event_instances_in_current_clock": planned["event_instance_count"],
            "relationship_counts_in_current_plan_build": planned[
                "relationship_counts"
            ],
        },
        "coverage": coverage,
        "ledger": ledger,
        "input_lineage": source_lineage,
        "static_fingerprint": static_fingerprint,
    }
    _atomic_text(state_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _atomic_text(report_path, _render_report(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--policy-dependencies", type=Path, default=DEFAULT_POLICY_DEPENDENCIES)
    parser.add_argument("--event-clock-db", type=Path, default=DEFAULT_CLOCK_DB)
    parser.add_argument("--quote-snapshot", type=Path, default=DEFAULT_QUOTE_SNAPSHOT)
    parser.add_argument("--clock-integrity", type=Path, default=DEFAULT_CLOCK_INTEGRITY)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    result = run_once(
        contract_path=args.contract,
        universe_path=args.universe,
        dependency_path=args.policy_dependencies,
        event_clock_db=args.event_clock_db,
        quote_snapshot_path=args.quote_snapshot,
        clock_integrity_path=args.clock_integrity,
        ledger_path=args.ledger,
        lock_path=args.lock,
        state_path=args.state,
        report_path=args.report,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
