#!/usr/bin/env python3
"""Independent verifier for Sequential Deliberate Replay V1.

This file deliberately does not import the replay producer or its core module.
It verifies the derived identity hierarchy against the already independently
verified SIM ledger.  It cannot promote, authorize, publish, or execute.
"""

from __future__ import annotations

import argparse
import ast
from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "sequential_deliberate_replay_v1.json"
CORE = ROOT / "src" / "forex_system" / "research" / "sequential_deliberate_replay_v1.py"
PRODUCER = ROOT / "oanda_sequential_deliberate_replay.py"
FORBIDDEN_BLIND_KEYS = {
    "instrument",
    "pair",
    "decision_epoch",
    "decision_utc",
    "datetime",
    "date",
    "future",
    "outcome",
    "realized",
    "pnl",
    "mfe",
    "mae",
}


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def stable_hash(*parts: Any) -> str:
    return sha256(canonical_json(parts).encode("utf-8")).hexdigest()


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _clean_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return round(value, 9)
    if isinstance(value, Mapping):
        return {str(key): _clean_scalar(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple, set, frozenset)):
        values = [_clean_scalar(item) for item in value]
        return sorted(values, key=canonical_json)
    return str(value)


FINGERPRINT_FIELDS = (
    "session_bucket",
    "session",
    "liquidity_bucket",
    "spread_pips",
    "volatility_bucket",
    "regime_bucket",
    "level_state",
    "support_distance_pips",
    "resistance_distance_pips",
    "trend_state",
    "source_state",
    "news_state",
    "rate_state",
    "currency_factor",
    "signal_signature",
    "recent_path",
    "candidate_count_bucket",
    "portfolio_state",
)


def independent_fingerprint(snapshot: Mapping[str, Any]) -> str:
    causal = {
        field: _clean_scalar(snapshot[field])
        for field in FINGERPRINT_FIELDS
        if field in snapshot
    }
    if not causal:
        raise ValueError("empty causal situation")
    return "situation_v1_" + stable_hash("sequential_deliberate_replay_v1", causal)[:28]


def _nested_keys(value: Any) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, Mapping):
        for key, child in value.items():
            keys.add(str(key).lower())
            keys.update(_nested_keys(child))
    elif isinstance(value, list):
        for child in value:
            keys.update(_nested_keys(child))
    return keys


def _pip_size(instrument: str) -> float:
    return 0.01 if instrument.upper().endswith("_JPY") else 0.0001


def _currencies(instrument: str) -> tuple[str, str]:
    parts = instrument.upper().split("_")
    if len(parts) != 2:
        raise ValueError("invalid instrument")
    return parts[0], parts[1]


def _currency_components(instruments: Sequence[str]) -> dict[str, str]:
    remaining = set(instruments)
    output: dict[str, str] = {}
    while remaining:
        seed = min(remaining)
        component = {seed}
        currencies = set(_currencies(seed))
        remaining.remove(seed)
        changed = True
        while changed:
            changed = False
            for instrument in sorted(remaining):
                if currencies.intersection(_currencies(instrument)):
                    component.add(instrument)
                    currencies.update(_currencies(instrument))
                    remaining.remove(instrument)
                    changed = True
        identifier = "factorcomponent_" + stable_hash(
            "unsigned_currency_graph_v1", sorted(currencies), sorted(component)
        )[:24]
        for instrument in component:
            output[instrument] = identifier
    return output


def _root(connection: sqlite3.Connection, table: str, hash_column: str, cohort_id: str) -> dict[str, Any]:
    hashes = [
        str(row[0])
        for row in connection.execute(
            f"SELECT {hash_column} FROM {table} WHERE cohort_id=? ORDER BY {hash_column}",
            (cohort_id,),
        )
    ]
    return {"count": len(hashes), "set_sha256": stable_hash(hashes)}


def _imports_forbidden_runtime() -> list[str]:
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    forbidden = (
        "oanda_sequential_deliberate_replay",
        "src.forex_system.research.sequential_deliberate_replay_v1",
        "oandapy",
        "requests",
    )
    return sorted(name for name in names if name.startswith(forbidden))


def _independent_mistake_census(
    source: sqlite3.Connection, source_cohort_id: str
) -> dict[str, int]:
    pair_clocks: set[tuple[str, str]] = set()
    global_clocks: set[str] = set()
    paths: set[tuple[Any, ...]] = set()
    raw = 0
    for row in source.execute(
        "SELECT sample_json FROM sim_mistake_samples WHERE cohort_id=?",
        (source_cohort_id,),
    ):
        raw += 1
        payload = json.loads(str(row["sample_json"]))
        timestamp = str(payload.get("decision_utc") or "")
        instrument = str(payload.get("instrument") or "")
        pair_clocks.add((instrument, timestamp))
        global_clocks.add(timestamp)
        paths.add((
            instrument,
            timestamp,
            int(payload.get("entry_delay_min") or 0),
            int(payload.get("horizon_min") or 0),
            str(payload.get("exit_policy_id") or ""),
            str(payload.get("side") or ""),
        ))
    return {
        "raw_samples": raw,
        "unique_pair_clocks": len(pair_clocks),
        "unique_portfolio_clocks": len(global_clocks),
        "unique_two_sided_paths": len(paths),
    }


def _append_only_checks(database_path: Path, tables: Sequence[str]) -> dict[str, Any]:
    connection = sqlite3.connect(database_path, timeout=30.0)
    result: dict[str, Any] = {}
    connection.execute("BEGIN")
    try:
        for table in tables:
            row = connection.execute(f"SELECT rowid FROM {table} LIMIT 1").fetchone()
            if row is None:
                trigger_names = {
                    str(value[0])
                    for value in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
                        (table,),
                    )
                }
                result[table] = {
                    "update_blocked": f"{table}_no_update" in trigger_names,
                    "delete_blocked": f"{table}_no_delete" in trigger_names,
                }
                continue
            update_blocked = delete_blocked = False
            try:
                connection.execute(f"UPDATE {table} SET rowid=rowid WHERE rowid=?", (int(row[0]),))
            except sqlite3.DatabaseError as exc:
                update_blocked = "append_only" in str(exc)
            try:
                connection.execute(f"DELETE FROM {table} WHERE rowid=?", (int(row[0]),))
            except sqlite3.DatabaseError as exc:
                delete_blocked = "append_only" in str(exc)
            result[table] = {"update_blocked": update_blocked, "delete_blocked": delete_blocked}
    finally:
        connection.rollback()
        connection.close()
    return result


def verify(config_path: Path = CONFIG) -> dict[str, Any]:
    config_path = config_path.resolve()
    config = json.loads(config_path.read_text(encoding="utf-8"))
    storage = config["storage"]
    artifact_root = ROOT / str(storage["artifact_relative_root"])
    database_path = artifact_root / str(storage["database_name"])
    state_path = artifact_root / str(storage["state_name"])
    output_path = artifact_root / str(storage["verifier_name"])
    state = json.loads(state_path.read_text(encoding="utf-8"))
    cohort_id = str(state["cohort_id"])
    source_database_path = Path(str(state["source_database"]))
    source_verifier_path = Path(str(state["source_verifier"]))
    source_verifier = json.loads(source_verifier_path.read_text(encoding="utf-8"))
    source_cohort_id = str(state["source_cohort_id"])
    failures: list[str] = []
    checks: dict[str, Any] = {}

    if source_verifier.get("verified") is not True or source_verifier.get("failures"):
        failures.append("source_not_independently_verified")
    if str(source_verifier.get("cohort_id") or "") != source_cohort_id:
        failures.append("source_cohort_mismatch")

    replay = readonly(database_path)
    source = readonly(source_database_path)
    integrity = str(replay.execute("PRAGMA integrity_check").fetchone()[0])
    foreign_keys = [tuple(row) for row in replay.execute("PRAGMA foreign_key_check")]
    checks["sqlite_integrity"] = integrity
    checks["foreign_key_check"] = foreign_keys
    if integrity != "ok":
        failures.append("sqlite_integrity_failed")
    if foreign_keys:
        failures.append("foreign_key_failed")

    cohort = replay.execute("SELECT * FROM replay_cohorts WHERE cohort_id=?", (cohort_id,)).fetchone()
    if cohort is None:
        failures.append("cohort_missing")
        replay.close()
        source.close()
        audit = {"verified": False, "failures": failures}
        atomic_json(output_path, audit)
        return audit
    code_checks = {
        "config_sha256": file_sha256(config_path) == str(cohort["config_sha256"]),
        "runner_sha256": file_sha256(PRODUCER) == str(cohort["runner_sha256"]),
        "core_sha256": file_sha256(CORE) == str(cohort["core_sha256"]),
        "source_database_sha256": file_sha256(source_database_path) == str(cohort["source_database_sha256"]),
        "source_verifier_sha256": file_sha256(source_verifier_path) == str(cohort["source_verifier_sha256"]),
    }
    checks["content_bindings"] = code_checks
    failures.extend(f"content_binding_failed:{key}" for key, ok in code_checks.items() if not ok)

    source_clocks = list(
        source.execute(
            "SELECT * FROM sim_decision_clocks WHERE cohort_id=? ORDER BY decision_epoch,instrument",
            (source_cohort_id,),
        )
    )
    by_epoch: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for row in source_clocks:
        by_epoch[int(row["decision_epoch"])].append(row)
    cases = {
        int(row["decision_epoch"]): row
        for row in replay.execute(
            "SELECT * FROM replay_portfolio_cases WHERE cohort_id=? ORDER BY decision_epoch",
            (cohort_id,),
        )
    }
    case_failures = 0
    factor_components = 0
    for epoch, rows in sorted(by_epoch.items()):
        actual = cases.get(epoch)
        if actual is None:
            case_failures += 1
            continue
        global_clock_id = "decisionclock_" + stable_hash(
            "oanda", epoch, sorted(str(row["row_sha256"]) for row in rows)
        )[:28]
        expected_case_id = "replaycase_" + stable_hash(cohort_id, global_clock_id)[:28]
        if str(actual["decision_clock_id"]) != global_clock_id or str(actual["case_id"]) != expected_case_id:
            case_failures += 1
        if int(actual["proof_eligible"]) != 0 or str(actual["evidence_role"]) != "historical_training_discovery":
            case_failures += 1
        case_identity = {
            key: actual[key]
            for key in actual.keys()
            if key not in {"case_id", "row_sha256"}
        }
        if stable_hash(case_identity) != str(actual["row_sha256"]):
            case_failures += 1
        blind = json.loads(str(actual["blind_context_json"]))
        if FORBIDDEN_BLIND_KEYS.intersection(_nested_keys(blind)):
            case_failures += 1
        expected_fingerprint = independent_fingerprint(blind)
        if expected_fingerprint != str(actual["situation_fingerprint_id"]):
            case_failures += 1
        components = _currency_components([str(row["instrument"]) for row in rows])
        factor_components += len(set(components.values()))
        members = list(
            replay.execute(
                "SELECT * FROM replay_pair_members WHERE cohort_id=? AND case_id=?",
                (cohort_id, str(actual["case_id"])),
            )
        )
        if len(members) != len(rows):
            case_failures += 1
        by_clock = {str(row["clock_id"]): row for row in rows}
        for member in members:
            source_clock = by_clock.get(str(member["source_clock_id"]))
            if source_clock is None:
                case_failures += 1
                continue
            if str(member["instrument"]) != str(source_clock["instrument"]):
                case_failures += 1
            if str(member["factor_component_id"]) != components[str(source_clock["instrument"])]:
                case_failures += 1
            member_identity = {
                key: member[key]
                for key in member.keys()
                if key not in {"member_id", "row_sha256"}
            }
            if stable_hash(member_identity) != str(member["row_sha256"]):
                case_failures += 1
            context = json.loads(str(member["causal_context_json"]))
            if int(context.get("knowledge_cutoff_epoch") or 0) != epoch:
                case_failures += 1
    if len(cases) != len(by_epoch):
        case_failures += abs(len(cases) - len(by_epoch))
    checks["independent_case_projection"] = {
        "expected_portfolio_clocks": len(by_epoch),
        "observed_portfolio_clocks": len(cases),
        "expected_chart_clocks": len(source_clocks),
        "expected_factor_components": factor_components,
        "failures": case_failures,
    }
    if case_failures:
        failures.append("case_projection_failed")

    observed_roots = {
        "portfolio_cases": _root(replay, "replay_portfolio_cases", "row_sha256", cohort_id),
        "pair_members": _root(replay, "replay_pair_members", "row_sha256", cohort_id),
        "case_diagnostics": _root(replay, "replay_case_diagnostics", "diagnostic_sha256", cohort_id),
    }
    checks["normalized_roots"] = observed_roots
    if observed_roots != state.get("normalized_roots"):
        failures.append("normalized_roots_mismatch")

    census = state.get("repetition_census") or {}
    physical_path_count = int(
        source.execute(
            "SELECT COUNT(*) FROM ("
            "SELECT DISTINCT clock_id,entry_delay_min,horizon_min,exit_policy_id "
            "FROM sim_outcome_parts WHERE cohort_id=?"
            ")",
            (source_cohort_id,),
        ).fetchone()[0]
    )
    exact_counts = {
        "source_virtual_variants": int(source.execute("SELECT COUNT(*) FROM sim_intent_map WHERE cohort_id=?", (source_cohort_id,)).fetchone()[0]),
        "source_eligible_variants": int(source.execute("SELECT COUNT(*) FROM sim_intent_map WHERE cohort_id=? AND eligible_for_diagnostics=1", (source_cohort_id,)).fetchone()[0]),
        "source_signal_observations": int(source.execute("SELECT COUNT(*) FROM sim_signal_observations WHERE cohort_id=?", (source_cohort_id,)).fetchone()[0]),
        "source_chart_decision_clocks": len(source_clocks),
        "source_two_sided_outcome_parts": int(source.execute("SELECT COUNT(*) FROM sim_outcome_parts WHERE cohort_id=?", (source_cohort_id,)).fetchone()[0]),
        "portfolio_decision_clocks": len(by_epoch),
        "factor_component_count": factor_components,
        "physical_path_projection_count": physical_path_count,
    }
    count_failures = {
        key: {"expected": value, "observed": census.get(key)}
        for key, value in exact_counts.items()
        if census.get(key) != value
    }
    checks["repetition_census"] = {"exact": exact_counts, "failures": count_failures}
    if count_failures:
        failures.append("repetition_census_mismatch")

    expected_mistakes = _independent_mistake_census(source, source_cohort_id)
    observed_mistakes = state.get("mistake_curriculum_census") or {}
    mistake_failures = {
        key: {"expected": value, "observed": observed_mistakes.get(key)}
        for key, value in expected_mistakes.items()
        if observed_mistakes.get(key) != value
    }
    checks["mistake_curriculum_census"] = {
        "exact": expected_mistakes,
        "failures": mistake_failures,
    }
    if mistake_failures:
        failures.append("mistake_curriculum_census_mismatch")

    diagnostic_failures = 0
    for row in replay.execute(
        "SELECT * FROM replay_case_diagnostics WHERE cohort_id=?", (cohort_id,)
    ):
        payload = json.loads(str(row["diagnostic_json"]))
        if stable_hash(payload) != str(row["diagnostic_sha256"]):
            diagnostic_failures += 1
        for key, value in payload.items():
            if key not in row.keys() or row[key] != value:
                diagnostic_failures += 1
                break
    checks["diagnostic_rows"] = {"failures": diagnostic_failures}
    if diagnostic_failures:
        failures.append("diagnostic_rows_failed")

    attempts = list(replay.execute("SELECT * FROM replay_attempts WHERE cohort_id=?", (cohort_id,)))
    exposures = list(replay.execute("SELECT * FROM replay_exposures WHERE cohort_id=?", (cohort_id,)))
    journal_failures = 0
    attempts_by_id = {str(row["attempt_id"]): row for row in attempts}
    for row in attempts:
        payload = json.loads(str(row["attempt_json"]))
        if stable_hash(payload) != str(row["attempt_sha256"]):
            journal_failures += 1
        if str(row["action"]) in {"enter", "rotate"} and any(
            payload.get(key) in (None, "")
            for key in (
                "pair", "side", "units", "confidence", "expected_move_pips",
                "horizon_min", "entry_condition", "invalidation", "rationale",
            )
        ):
            journal_failures += 1
    for row in exposures:
        attempt_id = str(row["attempt_id"] or "")
        if not attempt_id or attempt_id not in attempts_by_id:
            journal_failures += 1
            continue
        if str(attempts_by_id[attempt_id]["committed_utc"]) >= str(row["exposed_utc"]):
            journal_failures += 1
    checks["journal"] = {
        "attempt_count": len(attempts),
        "exposure_count": len(exposures),
        "failures": journal_failures,
    }
    if journal_failures:
        failures.append("journal_failed")

    append_tables = sorted(
        {
            str(row[0])
            for row in replay.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    )
    replay.close()
    source.close()
    append_only = _append_only_checks(database_path, append_tables)
    checks["append_only"] = append_only
    if any(not state for table in append_only.values() for state in table.values()):
        failures.append("append_only_failed")

    forbidden_imports = _imports_forbidden_runtime()
    checks["verifier_imports"] = {"forbidden": forbidden_imports}
    if forbidden_imports:
        failures.append("verifier_imports_producer_or_runtime")
    audit = {
        "schema_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "cohort_id": cohort_id,
        "source_cohort_id": source_cohort_id,
        "research_only": True,
        "execution_eligible": False,
        "can_promote": False,
        "can_place_orders": False,
        "can_authorize": False,
        "supported_decision": "no_trade",
        "verifier_imports_producer_or_core": bool(forbidden_imports),
        "verified": not failures,
        "failures": failures,
        "checks": checks,
    }
    atomic_json(output_path, audit)
    return audit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    return parser.parse_args()


def main() -> int:
    audit = verify(parse_args().config)
    print(json.dumps(audit, indent=2, sort_keys=True))
    return 0 if audit["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
