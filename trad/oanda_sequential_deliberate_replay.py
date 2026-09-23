#!/usr/bin/env python3
"""Build the training-only sequential deliberate-replay case bank.

The input is the independently verified counterfactual SIM gym.  This adapter
does not recompute or reinterpret its historical results.  It projects those
immutable diagnostics into an arm-independent practice hierarchy:

variant -> executable outcome part -> pair/chart clock -> portfolio clock.

No broker, account, signal-feed, promotion, authorization, or lifecycle path is
available here.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Mapping, Sequence

from src.forex_system.research.sequential_deliberate_replay_v1 import (
    POLICY,
    blind_case_alias,
    build_situation_fingerprint,
    canonical_json,
    stable_hash,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "sequential_deliberate_replay_v1.json"
CORE_PATH = ROOT / "src" / "forex_system" / "research" / "sequential_deliberate_replay_v1.py"

TABLES = {
    "replay_cohorts",
    "replay_portfolio_cases",
    "replay_pair_members",
    "replay_case_diagnostics",
    "replay_attempts",
    "replay_exposures",
    "replay_snapshots",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(path)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def readonly_connection(path: Path) -> sqlite3.Connection:
    uri = path.resolve().as_uri() + "?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def output_connection(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=60.0)
    connection.row_factory = sqlite3.Row
    existing = {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    unexpected = sorted(existing - TABLES)
    if unexpected:
        connection.close()
        raise ValueError(f"refusing non-replay database {path}: {unexpected}")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS replay_cohorts (
          cohort_id TEXT PRIMARY KEY,created_utc TEXT NOT NULL,source_cohort_id TEXT NOT NULL,
          source_database_sha256 TEXT NOT NULL,source_verifier_sha256 TEXT NOT NULL,
          config_sha256 TEXT NOT NULL,runner_sha256 TEXT NOT NULL,core_sha256 TEXT NOT NULL,
          contract_sha256 TEXT NOT NULL,contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS replay_portfolio_cases (
          case_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,source_cohort_id TEXT NOT NULL,
          decision_clock_id TEXT NOT NULL,decision_epoch INTEGER NOT NULL,
          knowledge_cutoff_epoch INTEGER NOT NULL,blind_alias TEXT NOT NULL,
          evidence_role TEXT NOT NULL,proof_eligible INTEGER NOT NULL,
          market_episode_id TEXT NOT NULL,situation_fingerprint_id TEXT NOT NULL,
          blind_context_sha256 TEXT NOT NULL,blind_context_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,UNIQUE(cohort_id,decision_clock_id),
          FOREIGN KEY(cohort_id) REFERENCES replay_cohorts(cohort_id)
        );
        CREATE TABLE IF NOT EXISTS replay_pair_members (
          member_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,case_id TEXT NOT NULL,
          source_clock_id TEXT NOT NULL,instrument TEXT NOT NULL,
          currency_resource_1 TEXT NOT NULL,currency_resource_2 TEXT NOT NULL,
          factor_component_id TEXT NOT NULL,signal_count INTEGER NOT NULL,
          source_intent_count INTEGER NOT NULL,eligible_intent_count INTEGER NOT NULL,
          two_sided_outcome_part_count INTEGER NOT NULL,physical_path_projection_count INTEGER NOT NULL,
          archetype_ids_json TEXT NOT NULL,causal_context_sha256 TEXT NOT NULL,
          causal_context_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          UNIQUE(cohort_id,source_clock_id),
          FOREIGN KEY(case_id) REFERENCES replay_portfolio_cases(case_id)
        );
        CREATE TABLE IF NOT EXISTS replay_case_diagnostics (
          diagnostic_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,case_id TEXT NOT NULL,
          chart_rep_count INTEGER NOT NULL,factor_component_count INTEGER NOT NULL,
          nested_variant_count INTEGER NOT NULL,eligible_variant_count INTEGER NOT NULL,
          two_sided_outcome_part_count INTEGER NOT NULL,physical_path_projection_count INTEGER NOT NULL,
          raw_mistake_sample_count INTEGER NOT NULL,deduplicated_mistake_lesson_count INTEGER NOT NULL,
          diagnostic_sha256 TEXT NOT NULL,diagnostic_json TEXT NOT NULL,
          FOREIGN KEY(case_id) REFERENCES replay_portfolio_cases(case_id)
        );
        CREATE TABLE IF NOT EXISTS replay_attempts (
          attempt_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          case_id TEXT NOT NULL,committed_utc TEXT NOT NULL,action TEXT NOT NULL,
          pair TEXT,side TEXT,units INTEGER NOT NULL,confidence REAL,
          expected_move_pips REAL,horizon_min INTEGER,entry_condition TEXT NOT NULL,
          invalidation TEXT NOT NULL,rationale TEXT NOT NULL,attempt_sha256 TEXT NOT NULL,
          attempt_json TEXT NOT NULL,UNIQUE(session_id,case_id),
          FOREIGN KEY(case_id) REFERENCES replay_portfolio_cases(case_id)
        );
        CREATE TABLE IF NOT EXISTS replay_exposures (
          exposure_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          case_id TEXT NOT NULL,attempt_id TEXT,exposed_utc TEXT NOT NULL,
          exposure_type TEXT NOT NULL,scope_id TEXT NOT NULL,exposure_sha256 TEXT NOT NULL,
          exposure_json TEXT NOT NULL,
          FOREIGN KEY(case_id) REFERENCES replay_portfolio_cases(case_id),
          FOREIGN KEY(attempt_id) REFERENCES replay_attempts(attempt_id)
        );
        CREATE TABLE IF NOT EXISTS replay_snapshots (
          snapshot_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,generated_utc TEXT NOT NULL,
          statistics_sha256 TEXT NOT NULL,statistics_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS replay_cohorts_no_update BEFORE UPDATE ON replay_cohorts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_cohorts_no_delete BEFORE DELETE ON replay_cohorts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_portfolio_cases_no_update BEFORE UPDATE ON replay_portfolio_cases BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_portfolio_cases_no_delete BEFORE DELETE ON replay_portfolio_cases BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_pair_members_no_update BEFORE UPDATE ON replay_pair_members BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_pair_members_no_delete BEFORE DELETE ON replay_pair_members BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_case_diagnostics_no_update BEFORE UPDATE ON replay_case_diagnostics BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_case_diagnostics_no_delete BEFORE DELETE ON replay_case_diagnostics BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_attempts_no_update BEFORE UPDATE ON replay_attempts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_attempts_no_delete BEFORE DELETE ON replay_attempts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_exposures_no_update BEFORE UPDATE ON replay_exposures BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_exposures_no_delete BEFORE DELETE ON replay_exposures BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_snapshots_no_update BEFORE UPDATE ON replay_snapshots BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS replay_snapshots_no_delete BEFORE DELETE ON replay_snapshots BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


def validate_config(config: Mapping[str, Any]) -> None:
    for key, expected in POLICY.items():
        if config.get(key) != expected:
            raise ValueError(f"research policy mismatch: {key}")
    source = config.get("source") or {}
    case = config.get("case_contract") or {}
    if source.get("historical_source_is_already_inspected") is not True:
        raise ValueError("historical SIM source must remain labeled inspected")
    if case.get("all_imported_cases_proof_eligible") is not False:
        raise ValueError("imported historical cases must be proof-ineligible")
    if case.get("counterfactuals_count_as_repetitions") is not False:
        raise ValueError("counterfactuals cannot count as repetitions")
    if case.get("repeat_attempts_count_as_new_market_repetitions") is not False:
        raise ValueError("repeat attempts cannot count as new market repetitions")
    if int(case.get("maximum_open_positions") or 0) != 1:
        raise ValueError("V1 requires exactly one-position portfolio state")


def _insert_immutable(
    connection: sqlite3.Connection,
    table: str,
    key_column: str,
    key: str,
    hash_column: str,
    expected_hash: str,
    columns: Sequence[str],
    values: Sequence[Any],
) -> bool:
    row = connection.execute(
        f"SELECT {hash_column} FROM {table} WHERE {key_column}=?", (key,)
    ).fetchone()
    if row:
        if str(row[0]) != expected_hash:
            raise ValueError(f"immutable conflict: {table}:{key}")
        return False
    connection.execute(
        f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
        tuple(values),
    )
    return True


def _liquidity_bucket(spread_pips: float) -> str:
    if spread_pips <= 2.0:
        return "liquid"
    if spread_pips <= 3.0:
        return "normal"
    if spread_pips <= 5.0:
        return "elevated"
    return "wide"


def _volatility_bucket(max_abs_feature_pips: float) -> str:
    if max_abs_feature_pips < 2.0:
        return "quiet"
    if max_abs_feature_pips < 6.0:
        return "normal"
    if max_abs_feature_pips < 12.0:
        return "elevated"
    return "shock"


def _candidate_count_bucket(count: int) -> str:
    if count <= 0:
        return "zero"
    if count <= 4:
        return "one_to_four"
    if count <= 12:
        return "five_to_twelve"
    return "thirteen_plus"


def _pip_size(instrument: str) -> float:
    return 0.01 if instrument.upper().endswith("_JPY") else 0.0001


def _currencies(instrument: str) -> tuple[str, str]:
    parts = instrument.upper().split("_")
    if len(parts) != 2:
        raise ValueError(f"invalid instrument: {instrument}")
    return parts[0], parts[1]


def _archetype(rule_id: str) -> str:
    lower = rule_id.lower()
    if "reversion" in lower:
        name = "mean_reversion"
    elif "sma" in lower or "moving_average" in lower:
        name = "moving_average_trend"
    elif "momentum" in lower:
        name = "momentum"
    else:
        name = "unknown_fail_closed"
    return "strategy_archetype:price_rule_archetypes_v1:" + name


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


def _row_root(connection: sqlite3.Connection, table: str, hash_column: str, cohort_id: str) -> dict[str, Any]:
    rows = [
        str(row[0])
        for row in connection.execute(
            f"SELECT {hash_column} FROM {table} WHERE cohort_id=? ORDER BY {hash_column}",
            (cohort_id,),
        )
    ]
    return {"count": len(rows), "set_sha256": stable_hash(rows)}


def _source_statistics(source: sqlite3.Connection, source_cohort_id: str) -> dict[str, int]:
    def count(table: str, condition: str = "", parameters: Sequence[Any] = ()) -> int:
        sql = f"SELECT COUNT(*) FROM {table} WHERE cohort_id=?"
        if condition:
            sql += " AND " + condition
        return int(source.execute(sql, (source_cohort_id, *parameters)).fetchone()[0])

    return {
        "source_virtual_variants": count("sim_intent_map"),
        "source_eligible_variants": count("sim_intent_map", "eligible_for_diagnostics=1"),
        "source_signal_observations": count("sim_signal_observations"),
        "source_chart_decision_clocks": count("sim_decision_clocks"),
        "source_two_sided_outcome_parts": count("sim_outcome_parts"),
        "source_raw_mistake_samples": count("sim_mistake_samples"),
    }


def _mistake_census(source: sqlite3.Connection, source_cohort_id: str) -> dict[str, Any]:
    pair_clocks: set[tuple[str, str]] = set()
    global_clocks: set[str] = set()
    paths: set[tuple[Any, ...]] = set()
    by_cluster: dict[str, dict[str, set[Any] | int]] = {}
    for row in source.execute(
        "SELECT cluster,sample_json FROM sim_mistake_samples WHERE cohort_id=?",
        (source_cohort_id,),
    ):
        payload = json.loads(str(row["sample_json"]))
        cluster = str(row["cluster"])
        timestamp = str(payload.get("decision_utc") or "")
        instrument = str(payload.get("instrument") or "")
        pair_clock = (instrument, timestamp)
        path = (
            instrument,
            timestamp,
            int(payload.get("entry_delay_min") or 0),
            int(payload.get("horizon_min") or 0),
            str(payload.get("exit_policy_id") or ""),
            str(payload.get("side") or ""),
        )
        pair_clocks.add(pair_clock)
        global_clocks.add(timestamp)
        paths.add(path)
        state = by_cluster.setdefault(
            cluster,
            {"raw": 0, "pair_clocks": set(), "global_clocks": set(), "paths": set()},
        )
        state["raw"] = int(state["raw"]) + 1
        state["pair_clocks"].add(pair_clock)  # type: ignore[union-attr]
        state["global_clocks"].add(timestamp)  # type: ignore[union-attr]
        state["paths"].add(path)  # type: ignore[union-attr]
    return {
        "raw_samples": sum(int(value["raw"]) for value in by_cluster.values()),
        "unique_pair_clocks": len(pair_clocks),
        "unique_portfolio_clocks": len(global_clocks),
        "unique_two_sided_paths": len(paths),
        "by_cluster": {
            cluster: {
                "raw_samples": int(value["raw"]),
                "unique_pair_clocks": len(value["pair_clocks"]),  # type: ignore[arg-type]
                "unique_portfolio_clocks": len(value["global_clocks"]),  # type: ignore[arg-type]
                "unique_two_sided_paths": len(value["paths"]),  # type: ignore[arg-type]
            }
            for cluster, value in sorted(by_cluster.items())
        },
    }


def _render_report(state: Mapping[str, Any]) -> str:
    counts = state["repetition_census"]
    mistakes = state["mistake_curriculum_census"]
    return "\n".join(
        [
            "# Sequential Deliberate Replay V1",
            "",
            f"Generated: {state['generated_utc']}",
            "",
            "Status: verified-source historical practice curriculum; training/discovery only",
            "",
            "## Honest repetition hierarchy",
            "",
            f"- Virtual order variants: **{counts['source_virtual_variants']:,}**",
            f"- Eligible virtual variants: **{counts['source_eligible_variants']:,}**",
            f"- Two-sided executable outcome parts: **{counts['source_two_sided_outcome_parts']:,}**",
            f"- Side-independent physical-path projections: **{counts['physical_path_projection_count']:,}**",
            f"- Pair/chart decision repetitions: **{counts['source_chart_decision_clocks']:,}**",
            f"- Portfolio-choice decision repetitions: **{counts['portfolio_decision_clocks']:,}**",
            f"- Within-clock unsigned-currency components: **{counts['factor_component_count']:,}**",
            "- Independent regime/episode evidence: **unknown; one inspected historical week**",
            "",
            "Variants and repeated attempts are nested beneath a decision clock. They never increase",
            "the count of distinct market repetitions or independent evidence.",
            "",
            "## Mistake-curriculum correction",
            "",
            f"The source stored {mistakes['raw_samples']:,} severity-ranked diagnostic examples,",
            f"but these reduce to **{mistakes['unique_pair_clocks']} pair clocks**,",
            f"**{mistakes['unique_portfolio_clocks']} portfolio clocks**, and",
            f"**{mistakes['unique_two_sided_paths']} two-sided paths**. They remain objective",
            "outcome diagnostics until a learner makes a precommitted attempt; they are not",
            "sixty independent mistakes.",
            "",
            "## Practice and proof firewall",
            "",
            "Every imported case is permanently `historical_training_discovery`. All source",
            "partitions were already inspected. The append-only journal accepts one primary",
            "`wait`, `enter`, `hold`, `exit`, or `rotate` action per session/case before any",
            "feedback exposure. Reviewed, revealed, overlapping, or factor-linked cases can never",
            "be relabeled as untouched confirmation.",
            "",
            "This component cannot trade, authorize, promote, publish signals, or access an account.",
            "",
        ]
    )


def run(config_path: Path = DEFAULT_CONFIG) -> dict[str, Any]:
    config_path = config_path.resolve()
    config = json.loads(config_path.read_text(encoding="utf-8"))
    validate_config(config)
    source_contract = config["source"]
    source_root = ROOT / str(source_contract["artifact_relative_root"])
    source_database = source_root / str(source_contract["database_name"])
    source_state_path = source_root / str(source_contract["state_name"])
    source_verifier_path = source_root / str(source_contract["verifier_name"])
    source_state = json.loads(source_state_path.read_text(encoding="utf-8"))
    verifier = json.loads(source_verifier_path.read_text(encoding="utf-8"))
    if source_contract.get("require_verified") and verifier.get("verified") is not True:
        raise ValueError("source SIM cohort is not independently verified")
    if source_contract.get("require_zero_failures") and list(verifier.get("failures") or []):
        raise ValueError("source verifier contains failures")
    source_cohort_id = str(verifier.get("cohort_id") or "")
    if source_cohort_id != str(source_state.get("cohort_id") or ""):
        raise ValueError("source state/verifier cohort mismatch")

    storage = config["storage"]
    artifact_root = ROOT / str(storage["artifact_relative_root"])
    database_path = artifact_root / str(storage["database_name"])
    state_path = artifact_root / str(storage["state_name"])
    report_path = artifact_root / str(storage["report_name"])
    artifact_root.mkdir(parents=True, exist_ok=True)

    source_db_sha = file_sha256(source_database)
    verifier_sha = file_sha256(source_verifier_path)
    config_sha = file_sha256(config_path)
    runner_sha = file_sha256(Path(__file__))
    core_sha = file_sha256(CORE_PATH)
    material = {
        "experiment_key": config["experiment_key"],
        "config": config,
        "source_cohort_id": source_cohort_id,
        "source_database_sha256": source_db_sha,
        "source_verifier_sha256": verifier_sha,
        "runner_sha256": runner_sha,
        "core_sha256": core_sha,
    }
    contract_sha = stable_hash(material)
    cohort_id = str(config["experiment_key"]) + "." + contract_sha[:24]
    generated = utc_now()

    source = readonly_connection(source_database)
    output = output_connection(database_path)
    existing = output.execute(
        "SELECT contract_sha256 FROM replay_cohorts WHERE cohort_id=?", (cohort_id,)
    ).fetchone()
    if existing is None:
        output.execute(
            "INSERT INTO replay_cohorts VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                cohort_id,
                generated,
                source_cohort_id,
                source_db_sha,
                verifier_sha,
                config_sha,
                runner_sha,
                core_sha,
                contract_sha,
                canonical_json(material),
            ),
        )
    elif str(existing[0]) != contract_sha:
        raise ValueError("replay cohort identity collision")

    clocks = list(
        source.execute(
            "SELECT * FROM sim_decision_clocks WHERE cohort_id=? ORDER BY decision_epoch,instrument",
            (source_cohort_id,),
        )
    )
    signals_by_clock: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in source.execute(
        "SELECT clock_id,rule_id,raw_side,score,feature_value_pips FROM sim_signal_observations "
        "WHERE cohort_id=? ORDER BY clock_id,rule_id",
        (source_cohort_id,),
    ):
        signals_by_clock[str(row["clock_id"])].append(dict(row))

    intent_counts: dict[str, tuple[int, int]] = {}
    for row in source.execute(
        "SELECT s.clock_id,COUNT(*) AS total,SUM(i.eligible_for_diagnostics) AS eligible "
        "FROM sim_intent_map i JOIN sim_signal_observations s ON s.signal_id=i.signal_id "
        "WHERE i.cohort_id=? GROUP BY s.clock_id",
        (source_cohort_id,),
    ):
        intent_counts[str(row["clock_id"])] = (int(row["total"]), int(row["eligible"] or 0))

    outcomes_by_clock: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in source.execute(
        "SELECT clock_id,side,entry_delay_min,horizon_min,exit_policy_id,status FROM sim_outcome_parts "
        "WHERE cohort_id=? ORDER BY clock_id,entry_delay_min,horizon_min,side",
        (source_cohort_id,),
    ):
        outcomes_by_clock[str(row["clock_id"])].append(dict(row))

    clocks_by_epoch: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for row in clocks:
        clocks_by_epoch[int(row["decision_epoch"])].append(row)

    case_hashes: list[str] = []
    member_hashes: list[str] = []
    diagnostic_hashes: list[str] = []
    factor_component_count = 0
    physical_path_projection_ids: set[str] = set()
    case_contract = config["case_contract"]
    episode_seconds = int(config["deduplication"]["market_episode_minutes"]) * 60
    for epoch, rows in sorted(clocks_by_epoch.items()):
        instruments = [str(row["instrument"]) for row in rows]
        factor_components = _currency_components(instruments)
        factor_component_count += len(set(factor_components.values()))
        global_clock_id = "decisionclock_" + stable_hash(
            "oanda", epoch, sorted(str(row["row_sha256"]) for row in rows)
        )[:28]
        case_id = "replaycase_" + stable_hash(cohort_id, global_clock_id)[:28]
        market_episode_id = f"market_episode_v1_{epoch // episode_seconds}"
        blind_alias = blind_case_alias(
            cohort_id,
            global_clock_id,
            str(case_contract["blind_alias_salt"]),
        )
        pair_views: list[dict[str, Any]] = []
        spreads: list[float] = []
        feature_values: list[float] = []
        all_signatures: list[dict[str, Any]] = []
        for ordinal, row in enumerate(rows, start=1):
            clock_id = str(row["clock_id"])
            instrument = str(row["instrument"])
            signals = signals_by_clock.get(clock_id, [])
            spread = (float(row["decision_ask_close"]) - float(row["decision_bid_close"])) / _pip_size(instrument)
            spreads.append(spread)
            feature_values.extend(abs(float(item["feature_value_pips"])) for item in signals)
            signature = [
                {
                    "rule": str(item["rule_id"]),
                    "side": int(item["raw_side"]),
                    "score_bucket": round(float(item["score"]), 1),
                    "feature_bucket_pips": round(float(item["feature_value_pips"]), 1),
                }
                for item in signals
            ]
            all_signatures.append({"market": ordinal, "signals": signature})
            pair_views.append(
                {
                    "clock": row,
                    "instrument": instrument,
                    "spread_pips": spread,
                    "signals": signals,
                    "signature": signature,
                }
            )
        directions = [int(item["raw_side"]) for view in pair_views for item in view["signals"]]
        if directions and all(value > 0 for value in directions):
            regime = "unanimous_up"
        elif directions and all(value < 0 for value in directions):
            regime = "unanimous_down"
        elif directions:
            regime = "direction_conflicted"
        else:
            regime = "no_signal"
        fingerprint_input = {
            "session_bucket": str(rows[0]["session_bucket"]),
            "liquidity_bucket": _liquidity_bucket(sum(spreads) / len(spreads)),
            "spread_pips": round(sum(spreads) / len(spreads), 1),
            "volatility_bucket": _volatility_bucket(max(feature_values or [0.0])),
            "regime_bucket": regime,
            "level_state": "unavailable_in_source",
            "source_state": "price_only",
            "rate_state": "unavailable_in_source",
            "signal_signature": all_signatures,
            "candidate_count_bucket": _candidate_count_bucket(len(directions)),
            "portfolio_state": "flat_case_bank",
        }
        fingerprint = build_situation_fingerprint(fingerprint_input)
        blind_context = {
            "blind_alias": blind_alias,
            "market_count": len(pair_views),
            **fingerprint_input,
        }
        blind_sha = stable_hash(blind_context)
        case_identity = {
            "cohort_id": cohort_id,
            "source_cohort_id": source_cohort_id,
            "decision_clock_id": global_clock_id,
            "decision_epoch": epoch,
            "knowledge_cutoff_epoch": epoch,
            "blind_alias": blind_alias,
            "evidence_role": str(case_contract["all_imported_cases_evidence_role"]),
            "proof_eligible": 0,
            "market_episode_id": market_episode_id,
            "situation_fingerprint_id": fingerprint,
            "blind_context_sha256": blind_sha,
            "blind_context_json": canonical_json(blind_context),
        }
        case_row_sha = stable_hash(case_identity)
        _insert_immutable(
            output,
            "replay_portfolio_cases",
            "case_id",
            case_id,
            "row_sha256",
            case_row_sha,
            ("case_id", *case_identity.keys(), "row_sha256"),
            (case_id, *case_identity.values(), case_row_sha),
        )
        case_hashes.append(case_row_sha)

        nested_total = eligible_total = outcome_total = path_total = 0
        for view in pair_views:
            row = view["clock"]
            clock_id = str(row["clock_id"])
            instrument = str(view["instrument"])
            base, quote = _currencies(instrument)
            intents, eligible = intent_counts.get(clock_id, (0, 0))
            outcomes = outcomes_by_clock.get(clock_id, [])
            paths = {
                "pricepath_projection_" + stable_hash(
                    "retrospective_path_projection_v1",
                    instrument,
                    epoch,
                    int(item["entry_delay_min"]),
                    int(item["horizon_min"]),
                    str(item["exit_policy_id"]),
                    source_db_sha,
                )[:28]
                for item in outcomes
            }
            physical_path_projection_ids.update(paths)
            archetypes = sorted({_archetype(str(item["rule_id"])) for item in view["signals"]})
            causal_context = {
                "session_bucket": str(row["session_bucket"]),
                "spread_pips": round(float(view["spread_pips"]), 9),
                "decision_bid_close": float(row["decision_bid_close"]),
                "decision_ask_close": float(row["decision_ask_close"]),
                "signal_signature": view["signature"],
                "source_candle_epoch": int(row["source_candle_epoch"]),
                "knowledge_cutoff_epoch": int(row["knowledge_cutoff_epoch"]),
            }
            context_sha = stable_hash(causal_context)
            member_identity = {
                "cohort_id": cohort_id,
                "case_id": case_id,
                "source_clock_id": clock_id,
                "instrument": instrument,
                "currency_resource_1": "currency:" + base,
                "currency_resource_2": "currency:" + quote,
                "factor_component_id": factor_components[instrument],
                "signal_count": len(view["signals"]),
                "source_intent_count": intents,
                "eligible_intent_count": eligible,
                "two_sided_outcome_part_count": len(outcomes),
                "physical_path_projection_count": len(paths),
                "archetype_ids_json": canonical_json(archetypes),
                "causal_context_sha256": context_sha,
                "causal_context_json": canonical_json(causal_context),
            }
            member_id = "replaymember_" + stable_hash(cohort_id, clock_id)[:28]
            member_sha = stable_hash(member_identity)
            _insert_immutable(
                output,
                "replay_pair_members",
                "member_id",
                member_id,
                "row_sha256",
                member_sha,
                ("member_id", *member_identity.keys(), "row_sha256"),
                (member_id, *member_identity.values(), member_sha),
            )
            member_hashes.append(member_sha)
            nested_total += intents
            eligible_total += eligible
            outcome_total += len(outcomes)
            path_total += len(paths)

        diagnostic = {
            "chart_rep_count": len(pair_views),
            "factor_component_count": len(set(factor_components.values())),
            "nested_variant_count": nested_total,
            "eligible_variant_count": eligible_total,
            "two_sided_outcome_part_count": outcome_total,
            "physical_path_projection_count": path_total,
            "raw_mistake_sample_count": 0,
            "deduplicated_mistake_lesson_count": 0,
        }
        diagnostic_sha = stable_hash(diagnostic)
        diagnostic_id = "replaydiagnostic_" + stable_hash(cohort_id, case_id)[:28]
        _insert_immutable(
            output,
            "replay_case_diagnostics",
            "diagnostic_id",
            diagnostic_id,
            "diagnostic_sha256",
            diagnostic_sha,
            (
                "diagnostic_id", "cohort_id", "case_id", *diagnostic.keys(),
                "diagnostic_sha256", "diagnostic_json",
            ),
            (
                diagnostic_id, cohort_id, case_id, *diagnostic.values(),
                diagnostic_sha, canonical_json(diagnostic),
            ),
        )
        diagnostic_hashes.append(diagnostic_sha)

    source_counts = _source_statistics(source, source_cohort_id)
    mistakes = _mistake_census(source, source_cohort_id)
    repetition_census = {
        **source_counts,
        "portfolio_decision_clocks": len(clocks_by_epoch),
        "factor_component_count": factor_component_count,
        "physical_path_projection_count": len(physical_path_projection_ids),
        "practice_attempt_count": int(
            output.execute("SELECT COUNT(*) FROM replay_attempts WHERE cohort_id=?", (cohort_id,)).fetchone()[0]
        ),
        "feedback_exposure_count": int(
            output.execute("SELECT COUNT(*) FROM replay_exposures WHERE cohort_id=?", (cohort_id,)).fetchone()[0]
        ),
        "independent_regime_episode_count": None,
        "independent_regime_episode_state": "unknown_one_inspected_historical_week",
    }
    roots = {
        "portfolio_cases": {"count": len(case_hashes), "set_sha256": stable_hash(sorted(case_hashes))},
        "pair_members": {"count": len(member_hashes), "set_sha256": stable_hash(sorted(member_hashes))},
        "case_diagnostics": {"count": len(diagnostic_hashes), "set_sha256": stable_hash(sorted(diagnostic_hashes))},
    }
    statistics = {
        "repetition_census": repetition_census,
        "mistake_curriculum_census": mistakes,
        "normalized_roots": roots,
    }
    statistics_sha = stable_hash(statistics)
    snapshot_id = "replaysnapshot_" + stable_hash(cohort_id, statistics_sha)[:28]
    snapshot = output.execute(
        "SELECT statistics_sha256 FROM replay_snapshots WHERE snapshot_id=?", (snapshot_id,)
    ).fetchone()
    if snapshot is None:
        output.execute(
            "INSERT INTO replay_snapshots VALUES (?,?,?,?,?)",
            (snapshot_id, cohort_id, generated, statistics_sha, canonical_json(statistics)),
        )
    elif str(snapshot[0]) != statistics_sha:
        raise ValueError("snapshot identity conflict")
    output.commit()
    state = {
        "schema_version": 1,
        "generated_utc": generated,
        "cohort_id": cohort_id,
        "source_cohort_id": source_cohort_id,
        **POLICY,
        "database": str(database_path),
        "source_database": str(source_database),
        "source_verifier": str(source_verifier_path),
        "material_contract_sha256": contract_sha,
        "snapshot_id": snapshot_id,
        "statistics_sha256": statistics_sha,
        **statistics,
        "limitations": [
            "all imported cases are already-inspected historical training/discovery data",
            "portfolio management, exit, and rotation skill are not measured by isolated endpoint arms",
            "factor components are structural within-clock groups, not independent regimes",
            "physical path IDs are explicit retrospective projections and cannot revise source evidence",
            "no attempt has occurred until a primary action is precommitted in the append-only journal",
        ],
    }
    atomic_json(state_path, state)
    atomic_text(report_path, _render_report(state))
    source.close()
    output.close()
    return state


def record_attempt(
    database_path: Path,
    *,
    cohort_id: str,
    session_id: str,
    case_id: str,
    action: str,
    pair: str | None,
    side: str | None,
    units: int,
    confidence: float | None,
    expected_move_pips: float | None,
    horizon_min: int | None,
    entry_condition: str,
    invalidation: str,
    rationale: str,
    committed_utc: str | None = None,
) -> str:
    """Append one pre-outcome primary action; feedback is a separate operation."""

    action = action.lower().strip()
    if action not in {"wait", "enter", "hold", "exit", "rotate"}:
        raise ValueError("unsupported action")
    if not rationale.strip():
        raise ValueError("every action requires a rationale")
    if action in {"enter", "rotate"}:
        required = {
            "pair": pair,
            "side": side,
            "units": units if units > 0 else None,
            "confidence": confidence,
            "expected_move_pips": expected_move_pips,
            "horizon_min": horizon_min if horizon_min and horizon_min > 0 else None,
            "entry_condition": entry_condition.strip(),
            "invalidation": invalidation.strip(),
        }
        missing = sorted(key for key, value in required.items() if value in (None, ""))
        if missing:
            raise ValueError(f"precommitment missing for {action}: {missing}")
        if str(side).lower() not in {"long", "short"}:
            raise ValueError("side must be long or short")
        if not 0.0 <= float(confidence) <= 1.0:
            raise ValueError("confidence must be in [0,1]")
    connection = output_connection(database_path)
    case = connection.execute(
        "SELECT proof_eligible FROM replay_portfolio_cases WHERE cohort_id=? AND case_id=?",
        (cohort_id, case_id),
    ).fetchone()
    if case is None:
        connection.close()
        raise ValueError("unknown replay case")
    committed = committed_utc or utc_now()
    payload = {
        "cohort_id": cohort_id,
        "session_id": session_id,
        "case_id": case_id,
        "committed_utc": committed,
        "action": action,
        "pair": pair.upper() if pair else None,
        "side": side.lower() if side else None,
        "units": int(units),
        "confidence": confidence,
        "expected_move_pips": expected_move_pips,
        "horizon_min": horizon_min,
        "entry_condition": entry_condition.strip(),
        "invalidation": invalidation.strip(),
        "rationale": rationale.strip(),
    }
    attempt_sha = stable_hash(payload)
    attempt_id = "replayattempt_" + stable_hash(session_id, case_id)[:28]
    _insert_immutable(
        connection,
        "replay_attempts",
        "attempt_id",
        attempt_id,
        "attempt_sha256",
        attempt_sha,
        (
            "attempt_id", *payload.keys(), "attempt_sha256", "attempt_json",
        ),
        (
            attempt_id, *payload.values(), attempt_sha, canonical_json(payload),
        ),
    )
    connection.commit()
    connection.close()
    return attempt_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print(json.dumps(run(args.config), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
