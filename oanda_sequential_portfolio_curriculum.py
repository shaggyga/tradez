"""Build the research-only sequential portfolio practice curriculum."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping

from src.forex_system.research.sequential_portfolio_curriculum_v1 import (
    Memory,
    POLICY,
    canonical_json,
    case_identity,
    choose_precommitted_action,
    exact_attempt_identity,
    memory_payload,
    mistake_class,
    option_identity,
    select_session_cases,
    stable_hash,
    update_memory,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "sequential_portfolio_curriculum_v1.json"
VERIFIER_PATH = ROOT / "oanda_sequential_portfolio_curriculum_verifier.py"
TABLES = {
    "spc_cohorts", "spc_cases", "spc_source_outcomes", "spc_sessions",
    "spc_assignments", "spc_attempts", "spc_feedback",
    "spc_memory_transitions", "spc_session_seals", "spc_snapshots",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8", newline="\n")
    temporary.replace(path)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def validate_config(config: Mapping[str, Any]) -> None:
    for key, expected in POLICY.items():
        if config.get(key) != expected:
            raise ValueError(f"unsafe curriculum config: {key}")
    curriculum = config["curriculum"]
    required_true = (
        "precommit_entire_session_before_any_feedback",
        "within_session_adaptation_forbidden",
    )
    required_false = (
        "counterfactuals_count_as_market_repetitions",
        "repeat_attempts_count_as_market_repetitions",
        "repeat_attempts_count_as_regime_repetitions",
    )
    if any(curriculum.get(key) is not True for key in required_true):
        raise ValueError("session precommit contract disabled")
    if any(curriculum.get(key) is not False for key in required_false):
        raise ValueError("repetition inflation contract disabled")
    if int(curriculum["session_count"]) <= 0 or int(curriculum["attempts_per_session"]) <= 0:
        raise ValueError("empty curriculum")


def safe_artifact_root(config: Mapping[str, Any]) -> Path:
    relative = Path(str(config["storage"]["artifact_relative_root"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("unsafe artifact path")
    path = (ROOT / relative).resolve()
    expected = (ROOT / "data" / "oanda_training_manager" / "research_ledgers").resolve()
    if expected not in path.parents or path == expected:
        raise ValueError("artifact path outside research ledger")
    return path


def output_connection(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS spc_cohorts (
          cohort_id TEXT PRIMARY KEY,created_utc TEXT NOT NULL,source_cohort_id TEXT NOT NULL,
          source_session_id TEXT NOT NULL,contract_sha256 TEXT NOT NULL,contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS spc_cases (
          case_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,source_clock_id TEXT NOT NULL UNIQUE,
          source_decision_id TEXT NOT NULL,source_sequence_no INTEGER NOT NULL,
          decision_epoch INTEGER NOT NULL,market_episode_id TEXT NOT NULL,
          causal_sha256 TEXT NOT NULL,case_sha256 TEXT NOT NULL,case_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,FOREIGN KEY(cohort_id) REFERENCES spc_cohorts(cohort_id)
        );
        CREATE TABLE IF NOT EXISTS spc_source_outcomes (
          source_outcome_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,case_id TEXT NOT NULL UNIQUE,
          source_feedback_id TEXT NOT NULL,source_feedback_sha256 TEXT NOT NULL,
          revealed_after_attempt_id TEXT NOT NULL,outcome_sha256 TEXT NOT NULL,
          outcome_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          FOREIGN KEY(case_id) REFERENCES spc_cases(case_id)
        );
        CREATE TABLE IF NOT EXISTS spc_sessions (
          practice_session_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,learner_id TEXT NOT NULL,
          session_index INTEGER NOT NULL,precommitted_utc TEXT NOT NULL,
          prior_feedback_count INTEGER NOT NULL,schedule_sha256 TEXT NOT NULL,
          schedule_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          UNIQUE(cohort_id,learner_id,session_index),FOREIGN KEY(cohort_id) REFERENCES spc_cohorts(cohort_id)
        );
        CREATE TABLE IF NOT EXISTS spc_assignments (
          assignment_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,practice_session_id TEXT NOT NULL,
          assignment_ordinal INTEGER NOT NULL,case_id TEXT NOT NULL,
          attempt_ordinal_for_case INTEGER NOT NULL,selection_weight REAL NOT NULL,
          selection_reason TEXT NOT NULL,counts_as_market_repetition INTEGER NOT NULL,
          counts_as_regime_repetition INTEGER NOT NULL,assignment_sha256 TEXT NOT NULL,
          assignment_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          UNIQUE(practice_session_id,assignment_ordinal),UNIQUE(practice_session_id,case_id),
          FOREIGN KEY(practice_session_id) REFERENCES spc_sessions(practice_session_id),
          FOREIGN KEY(case_id) REFERENCES spc_cases(case_id)
        );
        CREATE TABLE IF NOT EXISTS spc_attempts (
          attempt_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,practice_session_id TEXT NOT NULL,
          assignment_id TEXT NOT NULL UNIQUE,case_id TEXT NOT NULL,learner_id TEXT NOT NULL,
          attempt_ordinal_for_case INTEGER NOT NULL,committed_utc TEXT NOT NULL,
          committed_stage INTEGER NOT NULL,prior_feedback_count INTEGER NOT NULL,
          action_option_id TEXT NOT NULL,action_sha256 TEXT NOT NULL,action_json TEXT NOT NULL,
          evidence_role TEXT NOT NULL,proof_eligible INTEGER NOT NULL,row_sha256 TEXT NOT NULL,
          FOREIGN KEY(assignment_id) REFERENCES spc_assignments(assignment_id)
        );
        CREATE TABLE IF NOT EXISTS spc_feedback (
          feedback_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,practice_session_id TEXT NOT NULL,
          attempt_id TEXT NOT NULL UNIQUE,case_id TEXT NOT NULL,revealed_utc TEXT NOT NULL,
          revealed_stage INTEGER NOT NULL,source_outcome_id TEXT NOT NULL,
          chosen_equity_pips REAL NOT NULL,best_equity_pips REAL NOT NULL,regret_pips REAL NOT NULL,
          correct INTEGER NOT NULL,mistake_class TEXT NOT NULL,best_option_id TEXT NOT NULL,
          feedback_sha256 TEXT NOT NULL,feedback_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          FOREIGN KEY(attempt_id) REFERENCES spc_attempts(attempt_id),
          FOREIGN KEY(source_outcome_id) REFERENCES spc_source_outcomes(source_outcome_id)
        );
        CREATE TABLE IF NOT EXISTS spc_memory_transitions (
          transition_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,practice_session_id TEXT NOT NULL,
          attempt_id TEXT NOT NULL UNIQUE,case_id TEXT NOT NULL,before_sha256 TEXT NOT NULL,
          before_json TEXT NOT NULL,after_sha256 TEXT NOT NULL,after_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,FOREIGN KEY(attempt_id) REFERENCES spc_attempts(attempt_id)
        );
        CREATE TABLE IF NOT EXISTS spc_session_seals (
          seal_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,created_utc TEXT NOT NULL,
          seal_sha256 TEXT NOT NULL,seal_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS spc_snapshots (
          snapshot_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,generated_utc TEXT NOT NULL,
          statistics_sha256 TEXT NOT NULL,statistics_json TEXT NOT NULL
        );
        """
    )
    for table in sorted(TABLES):
        connection.execute(
            f"CREATE TRIGGER IF NOT EXISTS {table}_no_update BEFORE UPDATE ON {table} "
            f"BEGIN SELECT RAISE(ABORT,'append_only:{table}'); END"
        )
        connection.execute(
            f"CREATE TRIGGER IF NOT EXISTS {table}_no_delete BEFORE DELETE ON {table} "
            f"BEGIN SELECT RAISE(ABORT,'append_only:{table}'); END"
        )
    connection.commit()
    return connection


def insert_immutable(connection: sqlite3.Connection, table: str, values: Mapping[str, Any], key: str) -> None:
    existing = connection.execute(f"SELECT * FROM {table} WHERE {key}=?", (values[key],)).fetchone()
    if existing is not None:
        if any(existing[name] != value for name, value in values.items()):
            raise ValueError(f"immutable conflict: {table}:{values[key]}")
        return
    names = list(values)
    connection.execute(
        f"INSERT INTO {table} ({','.join(names)}) VALUES ({','.join('?' for _ in names)})",
        tuple(values[name] for name in names),
    )


def row_hash(payload: Mapping[str, Any]) -> str:
    return stable_hash(payload)


def source_contract(config: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], Path]:
    source = config["source"]
    state_path = (ROOT / source["state_relative_path"]).resolve()
    verifier_path = (ROOT / source["verifier_relative_path"]).resolve()
    state = read_json(state_path)
    receipt = read_json(verifier_path)
    if state.get("cohort_id") != source["required_cohort_id"]:
        raise ValueError("wrong source cohort")
    if state.get("session_id") != source["required_session_id"]:
        raise ValueError("wrong source session")
    if bool(receipt.get("verified")) is not bool(source["required_verified"]):
        raise ValueError("source verifier not valid")
    if source["required_zero_failures"] and receipt.get("failures") != []:
        raise ValueError("source verifier failures")
    database = Path(str(state["database"])).resolve()
    if not database.is_file():
        raise ValueError("source database missing")
    return state, receipt, database


def source_snapshot(path: Path) -> sqlite3.Connection:
    source = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    memory = sqlite3.connect(":memory:")
    source.backup(memory)
    source.close()
    memory.row_factory = sqlite3.Row
    return memory


def build_cases(source: sqlite3.Connection, source_state: Mapping[str, Any]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    session_id = str(source_state["session_id"])
    cases: list[dict[str, Any]] = []
    outcomes: dict[str, dict[str, Any]] = {}
    query = """
      SELECT c.*,d.decision_id,d.decision_json,d.state_before_json,d.decision_sha256,
             f.feedback_id,f.row_sha256 AS source_feedback_sha256,
             f.primary_terminal_equity_pips,f.physical_path_id,f.currency_resources_json
      FROM spr_clocks c JOIN spr_decisions d ON d.clock_id=c.clock_id
      JOIN spr_feedback f ON f.decision_id=d.decision_id
      WHERE c.session_id=? ORDER BY c.sequence_no
    """
    for row in source.execute(query, (session_id,)):
        primary = json.loads(row["decision_json"])
        options = [dict(primary, counts_as_market_repetition=0, source_kind="primary")]
        equity: dict[str, float] = {option_identity(primary): float(row["primary_terminal_equity_pips"])}
        for branch in source.execute(
            "SELECT * FROM spr_counterfactuals WHERE decision_id=? ORDER BY branch_label",
            (row["decision_id"],),
        ):
            action = json.loads(branch["action_json"])
            options.append(dict(action, counts_as_market_repetition=0, source_kind="counterfactual"))
            equity[option_identity(action)] = float(branch["terminal_equity_pips"])
        options.sort(key=lambda action: (str(action.get("branch_label")), option_identity(action)))
        case_id = case_identity(
            str(source_state["cohort_id"]), session_id, row["clock_id"], row["causal_sha256"], options
        )
        case_payload = {
            "case_id": case_id,
            "source_clock_id": row["clock_id"],
            "source_decision_id": row["decision_id"],
            "source_sequence_no": int(row["sequence_no"]),
            "decision_epoch": int(row["decision_epoch"]),
            "market_episode_id": row["market_episode_id"],
            "causal_sha256": row["causal_sha256"],
            "causal_snapshot": json.loads(row["causal_json"]),
            "state_before": json.loads(row["state_before_json"]),
            "options": options,
            "feedback_withheld": True,
            "proof_eligible": False,
        }
        best_value = max(equity.values())
        best_option_id = sorted(key for key, value in equity.items() if abs(value - best_value) <= 1e-12)[0]
        outcomes[case_id] = {
            "source_feedback_id": row["feedback_id"],
            "source_feedback_sha256": row["source_feedback_sha256"],
            "option_equity_pips": equity,
            "best_equity_pips": best_value,
            "best_option_id": best_option_id,
            "physical_path_id": row["physical_path_id"],
            "currency_resources": json.loads(row["currency_resources_json"]),
            "counts_as_market_repetition": 0,
        }
        cases.append(case_payload)
    if len(cases) != int(source_state["global_clock_count"]):
        raise ValueError("source case count mismatch")
    return cases, outcomes


def table_root(connection: sqlite3.Connection, table: str, cohort_id: str) -> dict[str, Any]:
    hashes = sorted(str(row[0]) for row in connection.execute(f"SELECT row_sha256 FROM {table} WHERE cohort_id=?", (cohort_id,)))
    return {"count": len(hashes), "set_sha256": stable_hash(hashes)}


def render_report(state: Mapping[str, Any]) -> str:
    mistakes = state["mistake_counts"]
    lines = [
        "# Sequential Portfolio Curriculum V1", "",
        "Status: generated historical training/discovery; independent verification required", "",
        f"- Cohort: `{state['cohort_id']}`",
        f"- Practice sessions: {state['practice_session_count']}",
        f"- Learner attempts: {state['attempt_count']}",
        f"- Distinct historical cases: {state['distinct_case_count']}",
        f"- Repeated attempts: {state['repeated_attempt_count']}",
        f"- Distinct market repetitions: {state['distinct_market_repetition_count']}",
        f"- Structural episode components encountered: {state['structural_episode_count']}",
        f"- Correct/best-action attempts: {state['correct_attempt_count']}",
        f"- Mean regret: {state['mean_regret_pips']:.4f} pips", "",
        "## Mistake curriculum", "",
    ]
    lines.extend(f"- {name}: {count}" for name, count in sorted(mistakes.items()))
    lines += ["", "Repeats and every counterfactual option have repetition weight zero. Structural episode components are not claimed as independent regimes.", ""]
    return "\n".join(lines)


def run(config_path: Path = DEFAULT_CONFIG) -> dict[str, Any]:
    config = read_json(config_path)
    validate_config(config)
    source_state, source_receipt, source_database = source_contract(config)
    config_sha = file_sha256(config_path)
    source_state_semantic = {key: value for key, value in source_state.items() if key != "generated_utc"}
    source_receipt_semantic = {key: value for key, value in source_receipt.items() if key != "generated_utc"}
    core_path = ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_curriculum_v1.py"
    deterministic_utc = str(source_state.get("generated_utc") or "")
    if not deterministic_utc:
        raise ValueError("source state lacks deterministic timestamp anchor")
    contract = {
        "config_sha256": config_sha,
        "producer_code_sha256": file_sha256(Path(__file__).resolve()),
        "core_code_sha256": file_sha256(core_path),
        "verifier_code_sha256": file_sha256(VERIFIER_PATH),
        "source_state_semantic_sha256": stable_hash(source_state_semantic),
        "source_verifier_semantic_sha256": stable_hash(source_receipt_semantic),
        "source_cohort_id": source_state["cohort_id"],
        "source_session_id": source_state["session_id"],
        "source_session_seal_id": source_state["session_seal_id"],
        "timestamp_contract": {
            "mode": "source_state_generated_utc",
            "value": deterministic_utc,
        },
        "curriculum": config["curriculum"],
        "policy": POLICY,
    }
    contract_sha = stable_hash(contract)
    cohort_id = config["experiment_key"] + "." + contract_sha[:20]
    artifact_root = safe_artifact_root(config)
    cohort_root = artifact_root / "cohorts" / cohort_id
    database = cohort_root / config["storage"]["database_name"]
    state_path = artifact_root / config["storage"]["state_name"]
    report_path = artifact_root / config["storage"]["report_name"]
    if state_path.is_file():
        existing = read_json(state_path)
        if existing.get("cohort_id") == cohort_id and Path(str(existing.get("database", ""))).is_file():
            if config_path.resolve() != DEFAULT_CONFIG.resolve():
                raise ValueError("existing custom curriculum requires independent verification")
            from oanda_sequential_portfolio_curriculum_verifier import verify as verify_existing

            receipt = verify_existing()
            if (
                receipt.get("verified") is True
                and receipt.get("failures") == []
                and receipt.get("cohort_id") == cohort_id
            ):
                return read_json(state_path)
            raise ValueError("existing curriculum failed independent verification")

    source = source_snapshot(source_database)
    try:
        cases, outcomes = build_cases(source, source_state)
    finally:
        source.close()
    connection = output_connection(database)
    if connection.execute("SELECT COUNT(*) FROM spc_cohorts").fetchone()[0]:
        connection.close()
        raise ValueError("unsealed or foreign curriculum database")
    created = deterministic_utc
    cohort_values = {
        "cohort_id": cohort_id, "created_utc": created,
        "source_cohort_id": source_state["cohort_id"], "source_session_id": source_state["session_id"],
        "contract_sha256": contract_sha, "contract_json": canonical_json(contract),
    }
    insert_immutable(connection, "spc_cohorts", cohort_values, "cohort_id")
    by_id = {str(row["case_id"]): row for row in cases}
    for case in cases:
        payload = {key: case[key] for key in case}
        values = {
            "case_id": case["case_id"], "cohort_id": cohort_id,
            "source_clock_id": case["source_clock_id"], "source_decision_id": case["source_decision_id"],
            "source_sequence_no": case["source_sequence_no"], "decision_epoch": case["decision_epoch"],
            "market_episode_id": case["market_episode_id"], "causal_sha256": case["causal_sha256"],
            "case_sha256": stable_hash(payload), "case_json": canonical_json(payload),
        }
        values["row_sha256"] = row_hash(values)
        insert_immutable(connection, "spc_cases", values, "case_id")
    connection.commit()

    curriculum = config["curriculum"]
    learner_id = str(curriculum["learner_id"])
    memory: dict[str, Memory] = {}
    total_feedback = 0
    for session_index in range(1, int(curriculum["session_count"]) + 1):
        scheduled = select_session_cases(
            cases, memory, session_index=session_index,
            attempts_per_session=int(curriculum["attempts_per_session"]),
            target_review_slots=int(curriculum["target_review_slots_after_first_session"]),
            seed=str(curriculum["seed"]), novelty_weight=float(curriculum["novelty_weight"]),
            due_error_weight=float(curriculum["due_error_weight"]),
            due_correct_weight=float(curriculum["due_correct_weight"]),
            overdue_weight_per_session=float(curriculum["overdue_weight_per_session"]),
        )
        if len(scheduled) != int(curriculum["attempts_per_session"]):
            raise ValueError("insufficient cases for precommitted practice session")
        practice_session_id = "spcsession_" + stable_hash(
            cohort_id, learner_id, session_index, [row.case_id for row in scheduled]
        )[:28]
        schedule_payload = {
            "practice_session_id": practice_session_id, "session_index": session_index,
            "learner_id": learner_id, "prior_feedback_count": total_feedback,
            "assignments": [row.__dict__ for row in scheduled],
            "feedback_withheld_until_all_actions_committed": True,
        }
        session_values = {
            "practice_session_id": practice_session_id, "cohort_id": cohort_id,
            "learner_id": learner_id, "session_index": session_index,
            "precommitted_utc": deterministic_utc, "prior_feedback_count": total_feedback,
            "schedule_sha256": stable_hash(schedule_payload), "schedule_json": canonical_json(schedule_payload),
        }
        session_values["row_sha256"] = row_hash(session_values)
        insert_immutable(connection, "spc_sessions", session_values, "practice_session_id")
        prepared: list[tuple[Any, str, str, Mapping[str, Any], Memory]] = []
        for ordinal, scheduled_case in enumerate(scheduled, start=1):
            case = by_id[scheduled_case.case_id]
            assignment_id = "spcassignment_" + stable_hash(
                practice_session_id, ordinal, scheduled_case.case_id, scheduled_case.attempt_ordinal_for_case
            )[:28]
            assignment_payload = {
                "assignment_id": assignment_id, "practice_session_id": practice_session_id,
                "assignment_ordinal": ordinal, **scheduled_case.__dict__,
                "counterfactual_options_count_as_repetitions": False,
            }
            assignment_values = {
                "assignment_id": assignment_id, "cohort_id": cohort_id,
                "practice_session_id": practice_session_id, "assignment_ordinal": ordinal,
                "case_id": scheduled_case.case_id,
                "attempt_ordinal_for_case": scheduled_case.attempt_ordinal_for_case,
                "selection_weight": scheduled_case.selection_weight,
                "selection_reason": scheduled_case.selection_reason,
                "counts_as_market_repetition": scheduled_case.counts_as_market_repetition,
                "counts_as_regime_repetition": scheduled_case.counts_as_regime_repetition,
                "assignment_sha256": stable_hash(assignment_payload),
                "assignment_json": canonical_json(assignment_payload),
            }
            assignment_values["row_sha256"] = row_hash(assignment_values)
            insert_immutable(connection, "spc_assignments", assignment_values, "assignment_id")
            prior = memory.get(scheduled_case.case_id, Memory())
            action = choose_precommitted_action(
                case, prior, learner_id=learner_id,
                practice_session_id=practice_session_id, assignment_id=assignment_id,
            )
            attempt_id = exact_attempt_identity(
                cohort_id, learner_id, practice_session_id, assignment_id,
                scheduled_case.case_id, scheduled_case.attempt_ordinal_for_case, action,
            )
            action_payload = dict(action)
            attempt_values = {
                "attempt_id": attempt_id, "cohort_id": cohort_id,
                "practice_session_id": practice_session_id, "assignment_id": assignment_id,
                "case_id": scheduled_case.case_id, "learner_id": learner_id,
                "attempt_ordinal_for_case": scheduled_case.attempt_ordinal_for_case,
                "committed_utc": deterministic_utc, "committed_stage": session_index * 1000 + ordinal,
                "prior_feedback_count": total_feedback, "action_option_id": option_identity(action),
                "action_sha256": stable_hash(action_payload), "action_json": canonical_json(action_payload),
                "evidence_role": "historical_training_discovery", "proof_eligible": 0,
            }
            attempt_values["row_sha256"] = row_hash(attempt_values)
            insert_immutable(connection, "spc_attempts", attempt_values, "attempt_id")
            prepared.append((scheduled_case, assignment_id, attempt_id, action, prior))
        # This commit is the learner boundary: every action in the session is
        # durable before any outcome row for the session is allowed to exist.
        connection.commit()

        for ordinal, (scheduled_case, assignment_id, attempt_id, action, prior) in enumerate(prepared, start=1):
            outcome = outcomes[scheduled_case.case_id]
            source_outcome_id = "spcsourceoutcome_" + stable_hash(cohort_id, scheduled_case.case_id, outcome)[:28]
            outcome_payload = dict(outcome)
            existing_outcome = connection.execute(
                "SELECT revealed_after_attempt_id FROM spc_source_outcomes WHERE source_outcome_id=?",
                (source_outcome_id,),
            ).fetchone()
            first_reveal_attempt_id = (
                str(existing_outcome["revealed_after_attempt_id"])
                if existing_outcome is not None else attempt_id
            )
            outcome_values = {
                "source_outcome_id": source_outcome_id, "cohort_id": cohort_id,
                "case_id": scheduled_case.case_id, "source_feedback_id": outcome["source_feedback_id"],
                "source_feedback_sha256": outcome["source_feedback_sha256"],
                "revealed_after_attempt_id": first_reveal_attempt_id,
                "outcome_sha256": stable_hash(outcome_payload),
                "outcome_json": canonical_json(outcome_payload),
            }
            outcome_values["row_sha256"] = row_hash(outcome_values)
            insert_immutable(connection, "spc_source_outcomes", outcome_values, "source_outcome_id")
            chosen_option_id = option_identity(action)
            chosen_equity = float(outcome["option_equity_pips"][chosen_option_id])
            best_equity = float(outcome["best_equity_pips"])
            regret = max(0.0, best_equity - chosen_equity)
            correct = regret <= float(curriculum["correct_regret_tolerance_pips"])
            best_action = next(
                row for row in by_id[scheduled_case.case_id]["options"]
                if option_identity(row) == outcome["best_option_id"]
            )
            category = mistake_class(action, best_action, by_id[scheduled_case.case_id]["state_before"])
            feedback_payload = {
                "attempt_id": attempt_id, "chosen_option_id": chosen_option_id,
                "chosen_equity_pips": chosen_equity, "best_option_id": outcome["best_option_id"],
                "best_equity_pips": best_equity, "regret_pips": regret,
                "correct": correct, "mistake_class": category,
            }
            feedback_id = "spcfeedback_" + stable_hash(cohort_id, attempt_id, feedback_payload)[:28]
            feedback_values = {
                "feedback_id": feedback_id, "cohort_id": cohort_id,
                "practice_session_id": practice_session_id, "attempt_id": attempt_id,
                "case_id": scheduled_case.case_id, "revealed_utc": deterministic_utc,
                "revealed_stage": session_index * 1000 + 500 + ordinal,
                "source_outcome_id": source_outcome_id, "chosen_equity_pips": chosen_equity,
                "best_equity_pips": best_equity, "regret_pips": regret,
                "correct": int(correct), "mistake_class": category,
                "best_option_id": outcome["best_option_id"],
                "feedback_sha256": stable_hash(feedback_payload), "feedback_json": canonical_json(feedback_payload),
            }
            feedback_values["row_sha256"] = row_hash(feedback_values)
            insert_immutable(connection, "spc_feedback", feedback_values, "feedback_id")
            after = update_memory(
                prior, session_index=session_index, regret_pips=regret, correct=correct,
                best_option_id=outcome["best_option_id"],
                incorrect_delay=int(curriculum["incorrect_review_delay_sessions"]),
                correct_base_interval=int(curriculum["correct_review_base_interval_sessions"]),
                maximum_interval=int(curriculum["maximum_review_interval_sessions"]),
            )
            before_payload, after_payload = memory_payload(prior), memory_payload(after)
            transition_id = "spctransition_" + stable_hash(cohort_id, attempt_id, before_payload, after_payload)[:28]
            transition_values = {
                "transition_id": transition_id, "cohort_id": cohort_id,
                "practice_session_id": practice_session_id, "attempt_id": attempt_id,
                "case_id": scheduled_case.case_id, "before_sha256": stable_hash(before_payload),
                "before_json": canonical_json(before_payload), "after_sha256": stable_hash(after_payload),
                "after_json": canonical_json(after_payload),
            }
            transition_values["row_sha256"] = row_hash(transition_values)
            insert_immutable(connection, "spc_memory_transitions", transition_values, "transition_id")
            memory[scheduled_case.case_id] = after
        connection.commit()
        total_feedback += len(prepared)

    roots = {
        name: table_root(connection, table, cohort_id)
        for name, table in {
            "cases": "spc_cases", "source_outcomes": "spc_source_outcomes",
            "sessions": "spc_sessions", "assignments": "spc_assignments",
            "attempts": "spc_attempts", "feedback": "spc_feedback",
            "memory_transitions": "spc_memory_transitions",
        }.items()
    }
    attempt_count = roots["attempts"]["count"]
    distinct_case_count = int(connection.execute(
        "SELECT COUNT(DISTINCT case_id) FROM spc_attempts WHERE cohort_id=?", (cohort_id,)
    ).fetchone()[0])
    repetition_count = int(connection.execute(
        "SELECT COALESCE(SUM(counts_as_market_repetition),0) FROM spc_assignments WHERE cohort_id=?", (cohort_id,)
    ).fetchone()[0])
    structural_count = int(connection.execute(
        "SELECT COALESCE(SUM(counts_as_regime_repetition),0) FROM spc_assignments WHERE cohort_id=?", (cohort_id,)
    ).fetchone()[0])
    correct_count = int(connection.execute(
        "SELECT COALESCE(SUM(correct),0) FROM spc_feedback WHERE cohort_id=?", (cohort_id,)
    ).fetchone()[0])
    mean_regret = float(connection.execute(
        "SELECT COALESCE(AVG(regret_pips),0) FROM spc_feedback WHERE cohort_id=?", (cohort_id,)
    ).fetchone()[0])
    mistake_counts = {
        str(row[0]): int(row[1]) for row in connection.execute(
            "SELECT mistake_class,COUNT(*) FROM spc_feedback WHERE cohort_id=? GROUP BY mistake_class",
            (cohort_id,),
        )
    }
    statistics = {
        "practice_session_count": int(curriculum["session_count"]), "attempt_count": attempt_count,
        "distinct_case_count": distinct_case_count, "repeated_attempt_count": attempt_count - distinct_case_count,
        "distinct_market_repetition_count": repetition_count, "structural_episode_count": structural_count,
        "independent_regime_count": None, "independent_regime_count_state": "unknown_inspected_historical_window",
        "correct_attempt_count": correct_count, "mean_regret_pips": mean_regret,
        "mistake_counts": mistake_counts, "roots": roots,
    }
    seal_payload = {"cohort_id": cohort_id, "contract_sha256": contract_sha, **statistics}
    seal_id = "spcseal_" + stable_hash(seal_payload)[:28]
    seal_values = {
        "seal_id": seal_id, "cohort_id": cohort_id, "created_utc": deterministic_utc,
        "seal_sha256": stable_hash(seal_payload), "seal_json": canonical_json(seal_payload),
    }
    insert_immutable(connection, "spc_session_seals", seal_values, "seal_id")
    snapshot_id = "spcsnapshot_" + stable_hash(cohort_id, statistics)[:28]
    snapshot_values = {
        "snapshot_id": snapshot_id, "cohort_id": cohort_id, "generated_utc": deterministic_utc,
        "statistics_sha256": stable_hash(statistics), "statistics_json": canonical_json(statistics),
    }
    insert_immutable(connection, "spc_snapshots", snapshot_values, "snapshot_id")
    connection.commit()
    integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    foreign_keys = list(connection.execute("PRAGMA foreign_key_check"))
    connection.close()
    if integrity != "ok" or foreign_keys:
        raise ValueError("curriculum database integrity failure")
    state = {
        "schema_version": 1, "generated_utc": deterministic_utc, "cohort_id": cohort_id,
        "source_cohort_id": source_state["cohort_id"], "source_session_id": source_state["session_id"],
        "source_session_seal_id": source_state["session_seal_id"], "contract_sha256": contract_sha,
        "session_seal_id": seal_id, "database": str(database),
        "research_only": True, "execution_eligible": False, "proof_eligible": False,
        "can_promote": False, "can_place_orders": False, "can_authorize": False,
        "broker_access": False, "account_access": False,
        "supported_decision": "no_trade", "evidence_role": "historical_training_discovery",
        "counterfactual_options_count_as_repetitions": False,
        "repeat_attempts_count_as_repetitions": False, **statistics,
        "limitations": [
            "historical learner practice is not market proof",
            "repeated attempts and alternatives cannot increase evidence counts",
            "structural episodes are not independent regimes",
            "deterministic baseline learner is a mechanics fixture, not a human-skill claim",
        ],
    }
    atomic_json(state_path, state)
    atomic_text(report_path, render_report(state))
    return state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    return parser.parse_args()


def main() -> int:
    state = run(parse_args().config)
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
