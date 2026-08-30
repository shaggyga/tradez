"""Independent verifier for the sequential portfolio curriculum sidecar.

It intentionally imports neither the producer nor the curriculum/replay cores.
"""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "sequential_portfolio_curriculum_v1.json"
ARTIFACT_ROOT = ROOT / "data" / "oanda_training_manager" / "research_ledgers" / "sequential_portfolio_curriculum_v1"
STATE = ARTIFACT_ROOT / "sequential_portfolio_curriculum_v1.json"
OUTPUT = ARTIFACT_ROOT / "sequential_portfolio_curriculum_verifier_v1.json"
APPEND_ONLY_TABLES = {
    "spc_cohorts", "spc_cases", "spc_source_outcomes", "spc_sessions",
    "spc_assignments", "spc_attempts", "spc_feedback", "spc_memory_transitions",
    "spc_session_seals", "spc_snapshots",
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def stable_hash(*parts: Any) -> str:
    return sha256(canonical_json(parts).encode("utf-8")).hexdigest()


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected object: {path}")
    return value


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def sqlite_snapshot(path: Path) -> sqlite3.Connection:
    source = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    memory = sqlite3.connect(":memory:")
    source.backup(memory)
    source.close()
    memory.row_factory = sqlite3.Row
    return memory


def option_id(action: Mapping[str, Any]) -> str:
    payload = {
        "action": action.get("action"), "instrument": action.get("instrument"),
        "side": action.get("side"), "units": int(action.get("units") or 0),
        "branch_label": action.get("branch_label"),
    }
    return "spcoption_" + stable_hash(payload)[:28]


def case_id(source_cohort: str, source_session: str, clock: str, causal_hash: str, options: Sequence[Mapping[str, Any]]) -> str:
    return "spccase_" + stable_hash(
        source_cohort, source_session, clock, causal_hash, sorted(option_id(row) for row in options)
    )[:28]


def memory_default() -> dict[str, Any]:
    return {
        "attempt_count": 0, "last_session_index": 0, "next_due_session": 1,
        "mastery_streak": 0, "last_regret_pips": 0.0, "last_correct": False,
        "last_best_option_id": None,
    }


def tiebreak(seed: str, session_index: int, case: str) -> str:
    return stable_hash(seed, int(session_index), case)


def select_cases(cases: Sequence[Mapping[str, Any]], memory: Mapping[str, Mapping[str, Any]], session_index: int, cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    by_id = {str(row["case_id"]): row for row in cases}
    unseen = [key for key in by_id if int(memory.get(key, memory_default())["attempt_count"]) == 0]
    due = [
        key for key in by_id
        if int(memory.get(key, memory_default())["attempt_count"]) > 0
        and int(memory[key]["next_due_session"]) <= session_index
    ]
    def review_key(key: str) -> tuple[float, str]:
        state = memory[key]
        base = float(cfg["due_correct_weight"] if state["last_correct"] else cfg["due_error_weight"])
        overdue = max(0, session_index - int(state["next_due_session"]))
        return (-(base + float(cfg["overdue_weight_per_session"]) * overdue), tiebreak(str(cfg["seed"]), session_index, key))
    due.sort(key=review_key)
    unseen.sort(key=lambda key: tiebreak(str(cfg["seed"]), session_index, key))
    target = 0 if session_index == 1 else min(int(cfg["target_review_slots_after_first_session"]), len(due))
    reviews = due[:target]
    remaining = int(cfg["attempts_per_session"]) - len(reviews)
    new = unseen[:remaining]
    remaining -= len(new)
    extra = [key for key in due if key not in reviews][:remaining]
    chosen = reviews + new + extra
    if len(chosen) < int(cfg["attempts_per_session"]):
        future = [key for key in by_id if int(memory.get(key, memory_default())["attempt_count"]) > 0 and key not in chosen]
        future.sort(key=lambda key: (int(memory[key]["next_due_session"]), tiebreak(str(cfg["seed"]), session_index, key)))
        chosen += future[: int(cfg["attempts_per_session"]) - len(chosen)]
    seen_episodes = {
        str(by_id[key]["market_episode_id"]) for key, state in memory.items()
        if int(state["attempt_count"]) > 0 and key in by_id
    }
    rows = []
    for key in chosen:
        state = memory.get(key, memory_default())
        new_case = int(state["attempt_count"]) == 0
        episode = str(by_id[key]["market_episode_id"])
        if new_case:
            weight, reason = float(cfg["novelty_weight"]), "novel_case"
        else:
            base = float(cfg["due_correct_weight"] if state["last_correct"] else cfg["due_error_weight"])
            overdue = max(0, session_index - int(state["next_due_session"]))
            weight = base + float(cfg["overdue_weight_per_session"]) * overdue
            reason = "due_correct_review" if state["last_correct"] else "due_error_review"
        rows.append({
            "case_id": key, "selection_weight": weight, "selection_reason": reason,
            "attempt_ordinal_for_case": int(state["attempt_count"]) + 1,
            "counts_as_market_repetition": 1 if new_case else 0,
            "counts_as_regime_repetition": 1 if new_case and episode not in seen_episodes else 0,
        })
        if new_case:
            seen_episodes.add(episode)
    return rows


def choose_action(case: Mapping[str, Any], memory: Mapping[str, Any], learner: str, session: str, assignment: str) -> Mapping[str, Any]:
    options = list(case["options"])
    remembered = memory.get("last_best_option_id")
    if remembered:
        found = next((row for row in options if option_id(row) == remembered), None)
        if found is not None:
            return found
    index = int(stable_hash(learner, session, assignment)[:16], 16) % len(options)
    return options[index]


def update_memory(before: Mapping[str, Any], session_index: int, regret: float, correct: bool, best_option: str, cfg: Mapping[str, Any]) -> dict[str, Any]:
    streak = int(before["mastery_streak"]) + 1 if correct else 0
    if correct:
        interval = min(int(cfg["maximum_review_interval_sessions"]), int(cfg["correct_review_base_interval_sessions"]) * (2 ** max(0, streak - 1)))
    else:
        interval = int(cfg["incorrect_review_delay_sessions"])
    return {
        "attempt_count": int(before["attempt_count"]) + 1,
        "last_session_index": session_index, "next_due_session": session_index + max(1, interval),
        "mastery_streak": streak, "last_regret_pips": float(regret),
        "last_correct": bool(correct), "last_best_option_id": best_option,
    }


def classify(chosen: Mapping[str, Any], best: Mapping[str, Any], state: Mapping[str, Any]) -> str:
    if option_id(chosen) == option_id(best): return "none"
    ca, ba = str(chosen.get("action")), str(best.get("action"))
    if ca == "wait" or ba == "wait": return "opportunity_selection"
    if state.get("position") is None and {ca, ba} & {"enter"}:
        if chosen.get("instrument") == best.get("instrument") and chosen.get("side") != best.get("side"): return "direction"
        return "entry"
    if "rotate" in {ca, ba}: return "rotation"
    if "exit" in {ca, ba}: return "exit"
    if "hold" in {ca, ba}: return "management"
    if chosen.get("side") != best.get("side"): return "direction"
    return "opportunity_selection"


def source_cases(source: sqlite3.Connection, state: Mapping[str, Any]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    cases, outcomes = [], {}
    query = """SELECT c.*,d.decision_id,d.decision_json,d.state_before_json,
      f.feedback_id,f.row_sha256 AS source_feedback_sha256,f.primary_terminal_equity_pips,
      f.physical_path_id,f.currency_resources_json
      FROM spr_clocks c JOIN spr_decisions d ON d.clock_id=c.clock_id
      JOIN spr_feedback f ON f.decision_id=d.decision_id
      WHERE c.session_id=? ORDER BY c.sequence_no"""
    for row in source.execute(query, (state["session_id"],)):
        primary = json.loads(row["decision_json"])
        options = [dict(primary, counts_as_market_repetition=0, source_kind="primary")]
        equities = {option_id(primary): float(row["primary_terminal_equity_pips"])}
        for branch in source.execute("SELECT * FROM spr_counterfactuals WHERE decision_id=? ORDER BY branch_label", (row["decision_id"],)):
            action = json.loads(branch["action_json"])
            options.append(dict(action, counts_as_market_repetition=0, source_kind="counterfactual"))
            equities[option_id(action)] = float(branch["terminal_equity_pips"])
        options.sort(key=lambda action: (str(action.get("branch_label")), option_id(action)))
        identity = case_id(state["cohort_id"], state["session_id"], row["clock_id"], row["causal_sha256"], options)
        payload = {
            "case_id": identity, "source_clock_id": row["clock_id"],
            "source_decision_id": row["decision_id"], "source_sequence_no": int(row["sequence_no"]),
            "decision_epoch": int(row["decision_epoch"]), "market_episode_id": row["market_episode_id"],
            "causal_sha256": row["causal_sha256"], "causal_snapshot": json.loads(row["causal_json"]),
            "state_before": json.loads(row["state_before_json"]), "options": options,
            "feedback_withheld": True, "proof_eligible": False,
        }
        best = max(equities.values())
        best_id = sorted(key for key, value in equities.items() if abs(value - best) <= 1e-12)[0]
        outcomes[identity] = {
            "source_feedback_id": row["feedback_id"], "source_feedback_sha256": row["source_feedback_sha256"],
            "option_equity_pips": equities, "best_equity_pips": best, "best_option_id": best_id,
            "physical_path_id": row["physical_path_id"],
            "currency_resources": json.loads(row["currency_resources_json"]),
            "counts_as_market_repetition": 0,
        }
        cases.append(payload)
    return cases, outcomes


def row_self_hash(row: sqlite3.Row) -> str:
    payload = {key: row[key] for key in row.keys() if key != "row_sha256"}
    return stable_hash(payload)


def table_root(connection: sqlite3.Connection, table: str, cohort: str) -> dict[str, Any]:
    hashes = sorted(str(row[0]) for row in connection.execute(f"SELECT row_sha256 FROM {table} WHERE cohort_id=?", (cohort,)))
    return {"count": len(hashes), "set_sha256": stable_hash(hashes)}


def forbidden_imports() -> list[str]:
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    blocked = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import): names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom): names = [node.module or ""]
        else: continue
        for name in names:
            if "sequential_portfolio_curriculum" in name or "sequential_portfolio_replay" in name:
                blocked.append(name)
    return sorted(set(blocked))


def verify() -> dict[str, Any]:
    failures: list[str] = []
    recomputed_statistics: dict[str, Any] = {}
    config, state = read_json(CONFIG), read_json(STATE)
    source_state = read_json(ROOT / config["source"]["state_relative_path"])
    source_receipt = read_json(ROOT / config["source"]["verifier_relative_path"])
    if not source_receipt.get("verified") or source_receipt.get("failures") != []: failures.append("source_verifier")
    if state.get("source_session_seal_id") != source_state.get("session_seal_id"): failures.append("source_seal")
    expected_safety = {
        "research_only": True, "execution_eligible": False,
        "proof_eligible": False, "can_promote": False,
        "can_place_orders": False, "can_authorize": False,
        "broker_access": False, "account_access": False,
        "supported_decision": "no_trade",
    }
    for key, expected in expected_safety.items():
        if state.get(key) != expected:
            failures.append("unsafe_state_" + key)
    if state.get("evidence_role") != "historical_training_discovery":
        failures.append("unsafe_state_evidence_role")
    if state.get("counterfactual_options_count_as_repetitions") is not False:
        failures.append("counterfactual_repetition_state")
    if state.get("repeat_attempts_count_as_repetitions") is not False:
        failures.append("repeat_repetition_state")
    semantic_state = {key: value for key, value in source_state.items() if key != "generated_utc"}
    semantic_receipt = {key: value for key, value in source_receipt.items() if key != "generated_utc"}
    producer_path = ROOT / "oanda_sequential_portfolio_curriculum.py"
    core_path = ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_curriculum_v1.py"
    contract = {
        "config_sha256": file_sha256(CONFIG),
        "producer_code_sha256": file_sha256(producer_path),
        "core_code_sha256": file_sha256(core_path),
        "verifier_code_sha256": file_sha256(Path(__file__).resolve()),
        "source_state_semantic_sha256": stable_hash(semantic_state),
        "source_verifier_semantic_sha256": stable_hash(semantic_receipt),
        "source_cohort_id": source_state["cohort_id"],
        "source_session_id": source_state["session_id"],
        "source_session_seal_id": source_state["session_seal_id"],
        "timestamp_contract": {
            "mode": "source_state_generated_utc",
            "value": str(source_state.get("generated_utc") or ""),
        },
        "curriculum": config["curriculum"],
        "policy": {
            **expected_safety,
        },
    }
    contract_sha = stable_hash(contract)
    expected_cohort = config["experiment_key"] + "." + contract_sha[:20]
    if state.get("cohort_id") != expected_cohort or state.get("contract_sha256") != contract_sha:
        failures.append("material_contract")
    blocked = forbidden_imports()
    if blocked: failures.append("verifier_import_isolation")
    source = sqlite_snapshot(Path(str(source_state["database"])))
    try: cases, outcomes = source_cases(source, source_state)
    finally: source.close()
    by_id = {row["case_id"]: row for row in cases}
    connection = sqlite_snapshot(Path(str(state["database"])))
    try:
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok": failures.append("sqlite_integrity")
        if list(connection.execute("PRAGMA foreign_key_check")): failures.append("foreign_keys")
        cohort_row = connection.execute("SELECT * FROM spc_cohorts WHERE cohort_id=?", (state["cohort_id"],)).fetchone()
        if cohort_row is None or cohort_row["contract_sha256"] != contract_sha or json.loads(cohort_row["contract_json"]) != contract:
            failures.append("cohort_contract")
        for table in APPEND_ONLY_TABLES:
            triggers = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (table,))}
            if not {f"{table}_no_update", f"{table}_no_delete"}.issubset(triggers): failures.append("append_only_" + table)
        for table in ("spc_cases", "spc_source_outcomes", "spc_sessions", "spc_assignments", "spc_attempts", "spc_feedback", "spc_memory_transitions"):
            for row in connection.execute(f"SELECT * FROM {table} WHERE cohort_id=?", (state["cohort_id"],)):
                if row_self_hash(row) != row["row_sha256"]: failures.append("row_hash_" + table)
        stored_cases = {row["case_id"]: row for row in connection.execute("SELECT * FROM spc_cases WHERE cohort_id=?", (state["cohort_id"],))}
        if set(stored_cases) != set(by_id): failures.append("case_set")
        for identity, case in by_id.items():
            row = stored_cases.get(identity)
            if row is None or json.loads(row["case_json"]) != case or row["case_sha256"] != stable_hash(case): failures.append("case_" + identity)
            if any(int(option.get("counts_as_market_repetition", -1)) != 0 for option in case["options"]): failures.append("counterfactual_rep_" + identity)
        curriculum = config["curriculum"]
        learner = str(curriculum["learner_id"])
        memory: dict[str, dict[str, Any]] = {}
        total_feedback = 0
        first_attempt: dict[str, str] = {}
        for index in range(1, int(curriculum["session_count"]) + 1):
            expected = select_cases(cases, memory, index, curriculum)
            session = connection.execute("SELECT * FROM spc_sessions WHERE cohort_id=? AND learner_id=? AND session_index=?", (state["cohort_id"], learner, index)).fetchone()
            if session is None: failures.append(f"session_{index}"); continue
            practice_session = session["practice_session_id"]
            assignments = list(connection.execute("SELECT * FROM spc_assignments WHERE practice_session_id=? ORDER BY assignment_ordinal", (practice_session,)))
            if len(assignments) != len(expected): failures.append(f"assignment_count_{index}")
            attempts = list(connection.execute("SELECT * FROM spc_attempts WHERE practice_session_id=? ORDER BY committed_stage", (practice_session,)))
            feedback_rows = list(connection.execute("SELECT * FROM spc_feedback WHERE practice_session_id=? ORDER BY revealed_stage", (practice_session,)))
            if attempts and feedback_rows and max(row["committed_stage"] for row in attempts) >= min(row["revealed_stage"] for row in feedback_rows): failures.append(f"precommit_boundary_{index}")
            if any(int(row["prior_feedback_count"]) != total_feedback for row in attempts): failures.append(f"within_session_adaptation_{index}")
            for ordinal, spec in enumerate(expected, start=1):
                assignment = assignments[ordinal - 1]
                for key in ("case_id", "attempt_ordinal_for_case", "selection_reason", "counts_as_market_repetition", "counts_as_regime_repetition"):
                    if assignment[key] != spec[key]: failures.append(f"schedule_{index}_{ordinal}_{key}")
                if abs(float(assignment["selection_weight"]) - float(spec["selection_weight"])) > 1e-12: failures.append(f"schedule_weight_{index}_{ordinal}")
                prior = memory.get(spec["case_id"], memory_default())
                action = choose_action(by_id[spec["case_id"]], prior, learner, practice_session, assignment["assignment_id"])
                attempt = next((row for row in attempts if row["assignment_id"] == assignment["assignment_id"]), None)
                if attempt is None: failures.append(f"attempt_{index}_{ordinal}"); continue
                expected_attempt = "spcattempt_" + stable_hash(
                    state["cohort_id"], learner, practice_session, assignment["assignment_id"], spec["case_id"],
                    spec["attempt_ordinal_for_case"], option_id(action), action,
                )[:28]
                if attempt["attempt_id"] != expected_attempt or json.loads(attempt["action_json"]) != action: failures.append(f"attempt_identity_{index}_{ordinal}")
                first_attempt.setdefault(spec["case_id"], attempt["attempt_id"])
                outcome = outcomes[spec["case_id"]]
                stored_outcome = connection.execute("SELECT * FROM spc_source_outcomes WHERE case_id=?", (spec["case_id"],)).fetchone()
                if stored_outcome is None or json.loads(stored_outcome["outcome_json"]) != outcome: failures.append(f"outcome_{index}_{ordinal}")
                if stored_outcome is not None and stored_outcome["revealed_after_attempt_id"] != first_attempt[spec["case_id"]]: failures.append(f"first_reveal_{index}_{ordinal}")
                chosen = float(outcome["option_equity_pips"][option_id(action)])
                best = float(outcome["best_equity_pips"])
                regret = max(0.0, best - chosen)
                correct = regret <= float(curriculum["correct_regret_tolerance_pips"])
                best_action = next(row for row in by_id[spec["case_id"]]["options"] if option_id(row) == outcome["best_option_id"])
                category = classify(action, best_action, by_id[spec["case_id"]]["state_before"])
                feedback = next((row for row in feedback_rows if row["attempt_id"] == attempt["attempt_id"]), None)
                if feedback is None or abs(float(feedback["regret_pips"]) - regret) > 1e-9 or int(feedback["correct"]) != int(correct) or feedback["mistake_class"] != category: failures.append(f"feedback_{index}_{ordinal}")
                after = update_memory(prior, index, regret, correct, outcome["best_option_id"], curriculum)
                transition = connection.execute("SELECT * FROM spc_memory_transitions WHERE attempt_id=?", (attempt["attempt_id"],)).fetchone()
                if transition is None or json.loads(transition["before_json"]) != prior or json.loads(transition["after_json"]) != after: failures.append(f"memory_{index}_{ordinal}")
                memory[spec["case_id"]] = after
            total_feedback += len(expected)
        assignments = list(connection.execute("SELECT * FROM spc_assignments WHERE cohort_id=?", (state["cohort_id"],)))
        attempt_rows = list(connection.execute("SELECT * FROM spc_attempts WHERE cohort_id=?", (state["cohort_id"],)))
        all_feedback_rows = list(connection.execute("SELECT * FROM spc_feedback WHERE cohort_id=?", (state["cohort_id"],)))
        distinct_cases = len({row["case_id"] for row in assignments})
        market_reps = sum(int(row["counts_as_market_repetition"]) for row in assignments)
        structural_reps = sum(int(row["counts_as_regime_repetition"]) for row in assignments)
        distinct_episodes = len({by_id[row["case_id"]]["market_episode_id"] for row in assignments})
        if market_reps != distinct_cases or market_reps != int(state["distinct_market_repetition_count"]): failures.append("market_repetition_count")
        if structural_reps != distinct_episodes or structural_reps != int(state["structural_episode_count"]): failures.append("regime_repetition_count")
        if any(int(row["counts_as_market_repetition"]) != (1 if int(row["attempt_ordinal_for_case"]) == 1 else 0) for row in assignments): failures.append("repeat_inflation")
        roots = {name: table_root(connection, table, state["cohort_id"]) for name, table in {
            "cases":"spc_cases", "source_outcomes":"spc_source_outcomes", "sessions":"spc_sessions",
            "assignments":"spc_assignments", "attempts":"spc_attempts", "feedback":"spc_feedback",
            "memory_transitions":"spc_memory_transitions",
        }.items()}
        if roots != state["roots"]: failures.append("roots")
        mistake_counts: dict[str, int] = {}
        for row in all_feedback_rows:
            key = str(row["mistake_class"])
            mistake_counts[key] = mistake_counts.get(key, 0) + 1
        recomputed_statistics = {
            "practice_session_count": int(connection.execute(
                "SELECT COUNT(*) FROM spc_sessions WHERE cohort_id=?",
                (state["cohort_id"],),
            ).fetchone()[0]),
            "attempt_count": len(attempt_rows),
            "distinct_case_count": distinct_cases,
            "repeated_attempt_count": len(attempt_rows) - distinct_cases,
            "distinct_market_repetition_count": market_reps,
            "structural_episode_count": structural_reps,
            "independent_regime_count": None,
            "independent_regime_count_state": "unknown_inspected_historical_window",
            "correct_attempt_count": sum(int(row["correct"]) for row in all_feedback_rows),
            "mean_regret_pips": (
                sum(float(row["regret_pips"]) for row in all_feedback_rows)
                / len(all_feedback_rows) if all_feedback_rows else 0.0
            ),
            "mistake_counts": dict(sorted(mistake_counts.items())),
            "roots": roots,
        }
        for key, expected in recomputed_statistics.items():
            actual = state.get(key)
            if key == "mean_regret_pips":
                if actual is None or abs(float(actual) - float(expected)) > 1e-12:
                    failures.append("state_stat_" + key)
            elif actual != expected:
                failures.append("state_stat_" + key)
        seal = connection.execute("SELECT * FROM spc_session_seals WHERE seal_id=?", (state["session_seal_id"],)).fetchone()
        expected_seal_payload = {
            "cohort_id": state["cohort_id"],
            "contract_sha256": contract_sha,
            **recomputed_statistics,
        }
        if (
            seal is None
            or json.loads(seal["seal_json"]) != expected_seal_payload
            or seal["seal_sha256"] != stable_hash(expected_seal_payload)
            or seal["seal_id"] != "spcseal_" + stable_hash(expected_seal_payload)[:28]
        ):
            failures.append("seal")
        snapshots = list(connection.execute(
            "SELECT * FROM spc_snapshots WHERE cohort_id=?", (state["cohort_id"],)
        ))
        expected_snapshot_id = "spcsnapshot_" + stable_hash(
            state["cohort_id"], recomputed_statistics
        )[:28]
        if (
            len(snapshots) != 1
            or snapshots[0]["snapshot_id"] != expected_snapshot_id
            or json.loads(snapshots[0]["statistics_json"]) != recomputed_statistics
            or snapshots[0]["statistics_sha256"] != stable_hash(recomputed_statistics)
        ):
            failures.append("snapshot")
        timestamp = str((contract.get("timestamp_contract") or {}).get("value") or "")
        if not timestamp or cohort_row is None or str(cohort_row["created_utc"]) != timestamp:
            failures.append("cohort_timestamp_contract")
        timestamp_checks = (
            ("spc_sessions", "precommitted_utc"),
            ("spc_attempts", "committed_utc"),
            ("spc_feedback", "revealed_utc"),
            ("spc_session_seals", "created_utc"),
            ("spc_snapshots", "generated_utc"),
        )
        for table, column in timestamp_checks:
            count = connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE cohort_id=? AND {column}<>?",
                (state["cohort_id"], timestamp),
            ).fetchone()[0]
            if int(count):
                failures.append("timestamp_contract_" + table)
    finally:
        connection.close()
    result = {
        "schema_version": 1, "generated_utc": datetime.now(timezone.utc).isoformat(),
        "verified": not failures, "failures": sorted(set(failures)),
        "cohort_id": state.get("cohort_id"), **expected_safety,
        "evidence_role": "historical_training_discovery",
        "checks": {
            "verifier_forbidden_imports": blocked,
            "precommitted_session_count": recomputed_statistics.get("practice_session_count"),
            "attempt_count": recomputed_statistics.get("attempt_count"),
            "distinct_case_count": recomputed_statistics.get("distinct_case_count"),
            "repeated_attempt_count": recomputed_statistics.get("repeated_attempt_count"),
            "distinct_market_repetition_count": recomputed_statistics.get("distinct_market_repetition_count"),
            "structural_episode_count": recomputed_statistics.get("structural_episode_count"),
            "correct_attempt_count": recomputed_statistics.get("correct_attempt_count"),
            "mean_regret_pips": recomputed_statistics.get("mean_regret_pips"),
            "mistake_counts": recomputed_statistics.get("mistake_counts"),
        },
    }
    atomic_json(OUTPUT, result)
    return result


def main() -> int:
    result = verify()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
