#!/usr/bin/env python3
"""Parser-corrected FOMC source-factor response discovery V2.

V1 remains immutable diagnostic evidence.  Its post-build event audit found
two bounded parser defects: activity matching could cross into a later labor
sentence, and the March 2025 balance-sheet-runoff dissent was counted without
an economic tilt.  V2 changes only those parsing rules (plus ``rose`` as the
document's equivalent of ``risen``), binds the exact V1 parent, and rebuilds a
separate immutable factor/link cohort.  No source, price, timing, inference,
or execution rule changes.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping

import oanda_spike_blurb_fomc_response_generalization_v1 as fomc
import oanda_spike_blurb_fomc_statement_factor_response_v1 as v1
import oanda_spike_blurb_fomc_statement_sources_v2 as source_v2
from oanda_spike_blurb_rbnz_schedule_cohort_v1 import (
    DATABASE,
    canonical_json,
    immutable_insert,
    sha256_bytes,
    sha256_file,
    stable_id,
    utc_now,
)


CONTRACT_ID = "spike_blurb_fomc_statement_factor_response_v2_20260820"
SCHEMA_VERSION = 1
FREEZE_UTC = "2026-08-20T19:15:00+00:00"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "fomc_statement_factor_response_v2"
)


def ordinal_state(text: Any) -> dict[str, int]:
    value = v1.policy_text(text).lower()
    if re.search(r"moderate pace|activity moderated", value):
        activity = 0
    elif "solid pace" in value:
        activity = 1
    elif "strong pace" in value:
        activity = 2
    elif re.search(r"growth of economic activity has slowed|economic activity has slowed", value):
        activity = -1
    else:
        activity = 0

    if re.search(r"downside risks to employment (?:have )?(?:risen|rose)", value):
        labor = -2
    elif re.search(r"job gains have remained low|labor market conditions have generally eased", value):
        labor = -1
    elif re.search(r"job gains have slowed|unemployment rate has (?:moved|edged) up", value):
        labor = -1
    elif "job gains have moderated" in value:
        labor = 0
    elif re.search(r"labor market conditions remain solid|job gains have kept pace", value):
        labor = 1
    elif re.search(r"job gains .*remain strong|job gains have remained strong", value):
        labor = 2
    else:
        labor = 0

    if "somewhat elevated" in value:
        inflation_pressure = 1
    elif re.search(r"inflation (?:remains|is) elevated|inflation has .* remains elevated", value):
        inflation_pressure = 2
    else:
        inflation_pressure = 0
    if re.search(r"lack of further progress|inflation has moved up|inflation moved up", value):
        inflation_progress = -1
    elif re.search(
        r"inflation has eased|made (?:further )?progress|(?:modest|some) further progress",
        value,
    ):
        inflation_progress = 1
    else:
        inflation_progress = 0

    if re.search(r"downside risks to employment (?:have )?(?:risen|rose)", value):
        risk = -2
    elif re.search(r"risks to both sides|roughly in balance", value):
        risk = 0
    elif re.search(r"moving into better balance|moved toward better balance", value):
        risk = 1
    elif "highly attentive to inflation risks" in value:
        risk = 2
    else:
        risk = 0
    if "additional policy firming" in value:
        guidance = 2
    elif "greater confidence that inflation is moving sustainably" in value:
        guidance = 1
    else:
        guidance = 0
    if re.search(r"initiate purchases|maintain an ample supply of reserves", value):
        balance_sheet_tightness = -2
    elif re.search(r"conclude the reduction|maintaining ample reserves", value):
        balance_sheet_tightness = 0
    elif "slow the pace of decline" in value:
        balance_sheet_tightness = 1
    elif "continue reducing its holdings" in value:
        balance_sheet_tightness = 2
    else:
        balance_sheet_tightness = 0
    return {
        "activity_strength": activity, "labor_tightness": labor,
        "inflation_pressure": inflation_pressure, "inflation_progress": inflation_progress,
        "risk_balance": risk, "guidance_tilt": guidance,
        "balance_sheet_tightness": balance_sheet_tightness,
    }


def dissent_state(text: Any, action_change_bps: int) -> dict[str, Any]:
    state = dict(v1.dissent_state(text, action_change_bps))
    value = v1.clean_statement(text)
    if re.search(
        r"preferred to continue the current pace of decline in securities holdings",
        value,
        flags=re.I,
    ):
        preferences = list(state["preferred_action_changes_bps"])
        preferences.append(action_change_bps + 1)
        state.update(
            {
                "dissent_hawkish": 1,
                "dissent_tilt": 0 if int(state["dissent_dovish"]) else 1,
                "preferred_action_changes_bps": preferences,
            }
        )
    return state


def statement_state(text: Any, *, event_date: str, action_change_bps: int) -> dict[str, Any]:
    return {
        **ordinal_state(text), **dissent_state(text, action_change_bps),
        "action_change_bps": action_change_bps,
        "sep_release_flag": int(event_date in v1.SEP_DATES),
    }


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS fomc_statement_factor_v2_contracts (
          contract_id TEXT PRIMARY KEY, parent_contract_id TEXT NOT NULL,
          source_contract_id TEXT NOT NULL, response_contract_id TEXT NOT NULL,
          contract_json TEXT NOT NULL, builder_sha256 TEXT NOT NULL,
          parent_builder_sha256 TEXT NOT NULL, input_snapshot_sha256 TEXT NOT NULL,
          frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fomc_statement_factor_v2_events (
          factor_event_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL,
          source_event_id TEXT NOT NULL, event_date TEXT NOT NULL,
          source_document_id TEXT NOT NULL, prior_document_id TEXT NOT NULL,
          event_clock_utc TEXT NOT NULL, source_url TEXT NOT NULL,
          state_json TEXT NOT NULL, prior_state_json TEXT NOT NULL,
          lexical_delta_json TEXT NOT NULL, row_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          causal_consensus_state TEXT NOT NULL, rate_repricing_state TEXT NOT NULL,
          retrospective_retrieval INTEGER NOT NULL, research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          UNIQUE(contract_id,event_date)
        );
        CREATE TABLE IF NOT EXISTS fomc_statement_factor_v2_observations (
          factor_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL,
          factor_event_id TEXT NOT NULL, source_event_id TEXT NOT NULL,
          event_date TEXT NOT NULL, factor_name TEXT NOT NULL, factor_role TEXT NOT NULL,
          current_value REAL NOT NULL, prior_value REAL, raw_delta REAL NOT NULL,
          signed_factor_score REAL NOT NULL, economic_mapping TEXT NOT NULL,
          direction_state TEXT NOT NULL, proof_state TEXT NOT NULL,
          source_document_sha256 TEXT NOT NULL, prior_document_sha256 TEXT NOT NULL,
          factor_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          UNIQUE(contract_id,event_date,factor_name)
        );
        CREATE TABLE IF NOT EXISTS fomc_statement_factor_v2_response_links (
          link_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, factor_id TEXT NOT NULL,
          source_event_id TEXT NOT NULL, outcome_id TEXT NOT NULL, control_slot INTEGER NOT NULL,
          arm_id TEXT NOT NULL, trade_mode TEXT NOT NULL, hold_minutes INTEGER NOT NULL,
          factor_present INTEGER NOT NULL, factor_sign INTEGER NOT NULL,
          detected_currency_sign INTEGER NOT NULL, alignment_state TEXT NOT NULL,
          selected_instrument TEXT, net_after_cost_pips REAL NOT NULL,
          mfe_pips REAL, mae_pips REAL, cost_cleared INTEGER NOT NULL,
          link_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          UNIQUE(contract_id,factor_id,outcome_id)
        );
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_v2_contract_no_update BEFORE UPDATE ON fomc_statement_factor_v2_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_v2_contract_no_delete BEFORE DELETE ON fomc_statement_factor_v2_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_v2_event_no_update BEFORE UPDATE ON fomc_statement_factor_v2_events BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_v2_event_no_delete BEFORE DELETE ON fomc_statement_factor_v2_events BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_v2_observation_no_update BEFORE UPDATE ON fomc_statement_factor_v2_observations BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_v2_observation_no_delete BEFORE DELETE ON fomc_statement_factor_v2_observations BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_v2_link_no_update BEFORE UPDATE ON fomc_statement_factor_v2_response_links BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_v2_link_no_delete BEFORE DELETE ON fomc_statement_factor_v2_response_links BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def input_snapshot(
    connection: sqlite3.Connection,
    sources: Iterable[Mapping[str, Any]],
    outcomes: Iterable[Mapping[str, Any]],
) -> str:
    parent = connection.execute(
        "SELECT contract_json,builder_sha256,input_snapshot_sha256 FROM fomc_statement_factor_contracts WHERE contract_id=?",
        (v1.CONTRACT_ID,),
    ).fetchone()
    if parent is None:
        raise RuntimeError("frozen_v1_factor_parent_missing")
    base = v1.input_snapshot(connection, sources, outcomes)
    return sha256_bytes(
        canonical_json(
            {
                "base_input_snapshot_sha256": base,
                "parent_contract": list(map(str, parent)),
                "parser_repairs": [
                    "activity_clause_does_not_cross_sentence_into_labor_clause",
                    "downside_employment_risk_accepts_rose_or_risen",
                    "balance_sheet_runoff_dissent_is_hawkish_tightness",
                ],
            }
        ).encode("utf-8")
    )


def freeze_contract(connection: sqlite3.Connection, snapshot_sha: str) -> dict[str, Any]:
    parent = connection.execute(
        "SELECT builder_sha256 FROM fomc_statement_factor_contracts WHERE contract_id=?",
        (v1.CONTRACT_ID,),
    ).fetchone()
    assert parent is not None
    contract = {
        "schema_version": SCHEMA_VERSION, "contract_id": CONTRACT_ID,
        "parent_contract_id": v1.CONTRACT_ID,
        "source_contract_id": source_v2.CONTRACT_ID,
        "response_contract_id": v1.shape.CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "parent_builder_sha256": str(parent[0]), "input_snapshot_sha256": snapshot_sha,
        "parser_repairs": {
            "activity_sentence_scope": "bounded_exact_phrase_precedence",
            "downside_risk_tense": ["risen", "rose"],
            "balance_sheet_dissent": "continued_faster_runoff_is_hawkish_tightness",
        },
        "all_other_factor_definitions_preserved": [list(row) for row in v1.FACTOR_DEFINITIONS],
        "inference_preserved": {
            "minimum_selected_events": v1.MINIMUM_SELECTED_EVENTS,
            "minimum_cost_clearance": v1.MINIMUM_COST_CLEARANCE,
            "randomization_draws": v1.RANDOMIZATION_DRAWS,
            "randomization_seed": v1.RANDOMIZATION_SEED,
            "multiplicity": "holm_all_tested_directional_factor_policy_arm_hold_cells",
        },
        "evidence_role": "corrected_adaptive_source_factor_discovery",
        "causal_consensus_state": "unavailable",
        "event_time_rate_repricing_state": "unavailable",
        "research_only": True, "execution_eligible": False,
        "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
        "frozen_utc": FREEZE_UTC,
    }
    encoded = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM fomc_statement_factor_v2_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is not None and str(existing[0]) != encoded:
        raise RuntimeError("immutable_fomc_statement_factor_v2_contract_conflict")
    if existing is None:
        connection.execute(
            "INSERT INTO fomc_statement_factor_v2_contracts VALUES(?,?,?,?,?,?,?,?,?)",
            (
                CONTRACT_ID, v1.CONTRACT_ID, source_v2.CONTRACT_ID, v1.shape.CONTRACT_ID,
                encoded, contract["builder_sha256"], str(parent[0]), snapshot_sha, FREEZE_UTC,
            ),
        )
    connection.commit()
    return contract


def build_factor_rows(sources: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    schedule = {str(row["event_date"]): row for row in fomc.validate_schedule()}
    events: list[dict[str, Any]] = []
    factors: list[dict[str, Any]] = []
    previous = sources[0]
    previous_state = statement_state(
        previous["statement_text"], event_date=previous["event_date"], action_change_bps=0
    )
    for current in sources[1:]:
        event = schedule[str(current["event_date"])]
        action_bps = int(round(float(event["official_target_change_percentage_points"]) * 100.0))
        current_state = statement_state(
            current["statement_text"], event_date=current["event_date"], action_change_bps=action_bps
        )
        lexical = v1.lexical_delta(current["statement_text"], previous["statement_text"])
        factor_event_id = stable_id("fomc_statement_factor_v2_event", CONTRACT_ID, current["event_date"])
        event_payload = {
            "factor_event_id": factor_event_id, "contract_id": CONTRACT_ID,
            "source_event_id": event["event_id"], "event_date": current["event_date"],
            "source_document_id": current["document_id"], "prior_document_id": previous["document_id"],
            "event_clock_utc": current["event_clock_utc"], "source_url": current["source_url"],
            "state_json": canonical_json(current_state), "prior_state_json": canonical_json(previous_state),
            "lexical_delta_json": canonical_json(lexical), "causal_consensus_state": "unavailable",
            "rate_repricing_state": "unavailable", "retrospective_retrieval": 1,
            "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
        }
        encoded = canonical_json(event_payload)
        event_payload.update({"row_json": encoded, "row_sha256": sha256_bytes(encoded.encode("utf-8"))})
        events.append(event_payload)
        for factor in v1.factor_values(current_state, previous_state, lexical):
            factor_id = stable_id(
                "fomc_statement_factor_v2", CONTRACT_ID, current["event_date"], factor["factor_name"]
            )
            payload = {
                "factor_id": factor_id, "contract_id": CONTRACT_ID,
                "factor_event_id": factor_event_id, "source_event_id": event["event_id"],
                "event_date": current["event_date"], **factor,
                "source_document_sha256": current["statement_text_sha256"],
                "prior_document_sha256": previous["statement_text_sha256"],
                "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
            }
            factor_json = canonical_json(payload)
            payload.update({"factor_json": factor_json, "row_sha256": sha256_bytes(factor_json.encode("utf-8"))})
            factors.append(payload)
        previous = current
        previous_state = current_state
    if len(events) != 21 or len(factors) != 21 * len(v1.FACTOR_DEFINITIONS):
        raise RuntimeError("v2_factor_population_mismatch")
    return events, factors


def store_factors(
    connection: sqlite3.Connection,
    events: Iterable[Mapping[str, Any]],
    factors: Iterable[Mapping[str, Any]],
) -> None:
    for event in events:
        immutable_insert(connection, "fomc_statement_factor_v2_events", event, "factor_event_id")
    for factor in factors:
        immutable_insert(connection, "fomc_statement_factor_v2_observations", factor, "factor_id")
    connection.commit()


def build_links(
    factors: Iterable[Mapping[str, Any]], outcomes: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    links = v1.build_links(factors, outcomes)
    output: list[dict[str, Any]] = []
    for row in links:
        payload = {
            **dict(row), "contract_id": CONTRACT_ID,
            "link_id": stable_id(
                "fomc_statement_factor_v2_response_link", CONTRACT_ID,
                row["factor_id"], row["outcome_id"],
            ),
        }
        payload.pop("link_json", None)
        payload.pop("row_sha256", None)
        encoded = canonical_json(payload)
        payload.update({"link_json": encoded, "row_sha256": sha256_bytes(encoded.encode("utf-8"))})
        output.append(payload)
    return output


def store_links(connection: sqlite3.Connection, links: Iterable[Mapping[str, Any]]) -> None:
    for link in links:
        immutable_insert(connection, "fomc_statement_factor_v2_response_links", link, "link_id")
    connection.commit()


def snapshot(
    connection: sqlite3.Connection, contract: Mapping[str, Any], cells: list[dict[str, Any]]
) -> dict[str, Any]:
    counts = {
        "factor_events": int(connection.execute("SELECT COUNT(*) FROM fomc_statement_factor_v2_events WHERE contract_id=?", (CONTRACT_ID,)).fetchone()[0]),
        "factor_observations": int(connection.execute("SELECT COUNT(*) FROM fomc_statement_factor_v2_observations WHERE contract_id=?", (CONTRACT_ID,)).fetchone()[0]),
        "response_links": int(connection.execute("SELECT COUNT(*) FROM fomc_statement_factor_v2_response_links WHERE contract_id=?", (CONTRACT_ID,)).fetchone()[0]),
    }
    result = {
        "schema_version": SCHEMA_VERSION, "generated_utc": utc_now(),
        "contract_id": CONTRACT_ID, "parent_contract_id": v1.CONTRACT_ID,
        "builder_sha256": contract["builder_sha256"],
        "input_snapshot_sha256": contract["input_snapshot_sha256"], **counts,
        "directional_factor_count": len(v1.DIRECTIONAL_FACTORS),
        "tested_cell_count": len(cells),
        "discovery_seed_count": sum(bool(row["discovery_seed"]) for row in cells),
        "best_cells": cells[:12], "causal_consensus_state": "unavailable",
        "event_time_rate_repricing_state": "unavailable",
        "sqlite_integrity": str(connection.execute("PRAGMA integrity_check").fetchone()[0]),
        "research_only": True, "execution_eligible": False,
        "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
    }
    result["snapshot_sha256"] = sha256_bytes(canonical_json(result).encode("utf-8"))
    return result


def write_report(result: Mapping[str, Any]) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    (REPORT_ROOT / "FOMC_STATEMENT_FACTOR_RESPONSE_V2.json").write_text(
        json.dumps(result, indent=2, sort_keys=True), encoding="utf-8"
    )
    lines = [
        "# FOMC statement factor-response discovery V2", "",
        f"Generated: `{result['generated_utc']}`", "",
        f"- Corrected factor events / observations: **{result['factor_events']} / {result['factor_observations']}**",
        f"- Immutable response links: **{result['response_links']:,}**",
        f"- Tested cells / discovery seeds: **{result['tested_cell_count']} / {result['discovery_seed_count']}**",
        f"- SQLite integrity: **{result['sqlite_integrity']}**",
        "- V1 preserved; V2 repairs only sentence scoping, rose/risen, and runoff-dissent tilt.",
        "- Consensus and event-time policy-path repricing remain unavailable.",
        "- Research-only adaptive discovery; no-trade and execution-ineligible.", "",
        "| Cell | N | Event net | Control | Increment | Clear | LOO min | Raw p | Holm p | Seed |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    lines.extend(
        f"| {row['cell_id']} | {row['event_n']} | {row['average_event_net_pips']:+.3f} | "
        f"{row['average_control_net_pips']:+.3f} | {row['average_incremental_net_pips']:+.3f} | "
        f"{row['cost_clearance_rate']:.1%} | {row['minimum_leave_one_out_event_net_pips']:+.3f} | "
        f"{row['raw_pvalue']:.6f} | {row['holm_pvalue']:.6f} | {row['discovery_seed']} |"
        for row in result["best_cells"]
    )
    (REPORT_ROOT / "FOMC_STATEMENT_FACTOR_RESPONSE_V2.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def build(*, database_path: Path = DATABASE) -> dict[str, Any]:
    connection = sqlite3.connect(database_path, timeout=120)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    sources = v1.load_source_documents(connection)
    outcomes = v1.load_shape_outcomes(connection)
    snapshot_sha = input_snapshot(connection, sources, outcomes)
    contract = freeze_contract(connection, snapshot_sha)
    events, factors = build_factor_rows(sources)
    store_factors(connection, events, factors)
    links = build_links(factors, outcomes)
    store_links(connection, links)
    cells = v1.discovery_cells(factors, outcomes)
    result = snapshot(connection, contract, cells)
    connection.close()
    write_report(result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE)
    args = parser.parse_args()
    result = build(database_path=args.database)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["CONTRACT_ID", "build", "dissent_state", "ensure_schema", "ordinal_state", "statement_state"]
