#!/usr/bin/env python3
"""Build source-proximate FOMC factors and immutable response analog links.

The factor vocabulary is frozen before this builder reads any response value.
It measures changes between consecutive exact official Federal Reserve
statements, then links every factor to the already-frozen 24-cell FOMC
response-shape ledger.  Directional analysis is explicitly discovery-only:

* source-confirmed responses may follow after the declared detector fires;
* source-conflicted responses are scored only in a separate fade paper arm;
* action levels are explanatory without causal pre-release expectations;
* no statement factor can promote, authorize, or place an order.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

import oanda_spike_blurb_fomc_response_generalization_v1 as fomc
import oanda_spike_blurb_fomc_response_shape_discovery_v1 as shape
import oanda_spike_blurb_fomc_statement_sources_v1 as source_v1
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


CONTRACT_ID = "spike_blurb_fomc_statement_factor_response_v1_20260820"
SCHEMA_VERSION = 1
FREEZE_UTC = "2026-08-20T19:00:00+00:00"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "fomc_statement_factor_response_v1"
)
MINIMUM_SELECTED_EVENTS = 5
MINIMUM_COST_CLEARANCE = 0.55
RANDOMIZATION_DRAWS = 100_000
RANDOMIZATION_SEED = 20_260_821
SEP_DATES = frozenset(
    {
        "2024-03-20", "2024-06-12", "2024-09-18", "2024-12-18",
        "2025-03-19", "2025-06-18", "2025-09-17", "2025-12-10",
        "2026-03-18", "2026-06-17",
    }
)

FACTOR_DEFINITIONS = (
    ("action_change_bps", "directional", "positive_is_hawkish", "current"),
    ("activity_strength_change", "directional", "positive_is_hawkish", "delta"),
    ("labor_tightness_change", "directional", "positive_is_hawkish", "delta"),
    ("inflation_pressure_change", "directional", "positive_is_hawkish", "delta"),
    ("inflation_progress_change", "directional", "positive_disinflation_is_dovish", "negative_delta"),
    ("risk_balance_change", "directional", "positive_is_hawkish", "delta"),
    ("guidance_tilt_change", "directional", "positive_is_hawkish", "delta"),
    ("balance_sheet_tightness_change", "directional", "positive_is_hawkish", "delta"),
    ("dissent_tilt", "directional", "positive_is_hawkish", "current"),
    ("dissent_count", "magnitude", "context_only", "current"),
    ("sep_release_flag", "context", "calendar_state_only", "current"),
    ("statement_novelty", "magnitude", "magnitude_only", "current"),
    ("added_token_fraction", "magnitude", "magnitude_only", "current"),
    ("removed_token_fraction", "magnitude", "magnitude_only", "current"),
)
DIRECTIONAL_FACTORS = frozenset(
    name for name, role, _meaning, _mapping in FACTOR_DEFINITIONS if role == "directional"
)


def finite(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def sign(value: float) -> int:
    return 1 if value > 0 else -1 if value < 0 else 0


def clean_statement(text: Any) -> str:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    value = re.sub(r"^Share\s+", "", value, flags=re.I)
    value = re.sub(
        r"^The Federal Open Market Committee approved the following statement "
        r"for release by a \d+\s*[\-–]\s*\d+ vote:\s*",
        "",
        value,
        flags=re.I,
    )
    return value.strip()


def policy_text(text: Any) -> str:
    value = clean_statement(text)
    vote = re.search(r"\bVoting (?:for|against)\b", value, flags=re.I)
    if vote:
        value = value[: vote.start()]
    return value.strip()


def word_tokens(text: str) -> list[str]:
    return re.findall(r"[a-z]+(?:'[a-z]+)?|\d+(?:/\d+)?", text.lower())


def lexical_delta(current: str, prior: str) -> dict[str, float]:
    current_tokens = word_tokens(policy_text(current))
    prior_tokens = word_tokens(policy_text(prior))
    current_set = set(current_tokens)
    prior_set = set(prior_tokens)
    union = current_set | prior_set
    overlap = current_set & prior_set
    novelty = 1.0 - (len(overlap) / len(union) if union else 1.0)
    return {
        "statement_novelty": novelty,
        "added_token_fraction": len(current_set - prior_set) / max(1, len(current_set)),
        "removed_token_fraction": len(prior_set - current_set) / max(1, len(prior_set)),
    }


def ordinal_state(text: Any) -> dict[str, int]:
    value = policy_text(text).lower()
    if re.search(r"growth .*slowed|economic activity .*slowed", value):
        activity = -1
    elif re.search(r"moderate pace|activity moderated", value):
        activity = 0
    elif re.search(r"solid pace", value):
        activity = 1
    elif re.search(r"strong pace", value):
        activity = 2
    else:
        activity = 0

    if re.search(r"downside risks to employment (?:have )?risen", value):
        labor = -2
    elif re.search(r"job gains have remained low|labor market conditions have generally eased", value):
        labor = -1
    elif re.search(r"job gains have slowed|unemployment rate has (?:moved|edged) up", value):
        labor = -1
    elif re.search(r"job gains have moderated", value):
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

    if re.search(r"downside risks to employment (?:have )?risen", value):
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
        "activity_strength": activity,
        "labor_tightness": labor,
        "inflation_pressure": inflation_pressure,
        "inflation_progress": inflation_progress,
        "risk_balance": risk,
        "guidance_tilt": guidance,
        "balance_sheet_tightness": balance_sheet_tightness,
    }


def preferred_changes(text: str) -> list[int]:
    against = re.search(r"\bVoting against\b(?P<body>.*)$", clean_statement(text), flags=re.I)
    body = against.group("body") if against else clean_statement(text)
    changes: list[int] = []
    for match in re.finditer(
        r"preferred to (?P<verb>raise|lower|maintain)(?P<tail>.{0,180}?)(?=(?:;\s*(?:and\s+)?)|\.$|$)",
        body,
        flags=re.I,
    ):
        verb = match.group("verb").lower()
        tail = match.group("tail").lower()
        if verb == "maintain":
            changes.append(0)
            continue
        amount = re.search(r"by\s+(?P<num>1/4|1/2)\s+percentage point", tail)
        magnitude = 50 if amount and amount.group("num") == "1/2" else 25
        changes.append(magnitude if verb == "raise" else -magnitude)
    if re.search(r"preferred no change", body, flags=re.I):
        changes.append(0)
    if re.search(r"did not support inclusion of an easing bias", body, flags=re.I):
        changes.append(1)
    return changes


def dissent_state(text: Any, action_change_bps: int) -> dict[str, Any]:
    raw_value = re.sub(r"\s+", " ", str(text or "")).strip()
    raw_value = re.sub(r"^Share\s+", "", raw_value, flags=re.I)
    value = clean_statement(text)
    header = re.search(
        r"approved .{0,120}? by a (\d+)\s*[\-–]\s*(\d+) vote",
        raw_value,
        flags=re.I,
    )
    against = re.search(r"\bVoting against\b(?P<body>.*)$", value, flags=re.I)
    if header:
        dissent_count = int(header.group(2))
    elif against:
        names = re.findall(r"\b[A-Z][a-z]+\s+[A-Z]\.\s+[A-Z][A-Za-z'-]+\b", against.group("body"))
        dissent_count = len(set(names))
    else:
        dissent_count = 0
    preferences = preferred_changes(value)
    hawkish = any(change > action_change_bps for change in preferences)
    dovish = any(change < action_change_bps for change in preferences)
    tilt = 0 if hawkish == dovish else 1 if hawkish else -1
    return {
        "dissent_count": dissent_count,
        "dissent_hawkish": int(hawkish),
        "dissent_dovish": int(dovish),
        "dissent_tilt": tilt,
        "preferred_action_changes_bps": preferences,
    }


def statement_state(
    text: Any, *, event_date: str, action_change_bps: int
) -> dict[str, Any]:
    return {
        **ordinal_state(text),
        **dissent_state(text, action_change_bps),
        "action_change_bps": action_change_bps,
        "sep_release_flag": int(event_date in SEP_DATES),
    }


def load_source_documents(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT r.event_date,r.document_id,r.source_generation,
               COALESCE(d.event_id,v.event_id),COALESCE(d.event_clock_utc,v.event_clock_utc),
               COALESCE(d.source_url,v.source_url),
               COALESCE(d.statement_text,v.statement_text),
               COALESCE(d.statement_text_sha256,v.statement_text_sha256),
               COALESCE(d.raw_payload_sha256,v.raw_payload_sha256)
        FROM fomc_statement_source_v2_resolutions r
        LEFT JOIN fomc_statement_source_documents d ON r.document_id=d.document_id
        LEFT JOIN fomc_statement_source_v2_repair_documents v ON r.document_id=v.document_id
        WHERE r.contract_id=? ORDER BY r.event_date
        """,
        (source_v2.CONTRACT_ID,),
    ).fetchall()
    output = [
        {
            "event_date": str(row[0]), "document_id": str(row[1]),
            "source_generation": str(row[2]), "event_id": str(row[3]),
            "event_clock_utc": str(row[4]), "source_url": str(row[5]),
            "statement_text": str(row[6]), "statement_text_sha256": str(row[7]),
            "raw_payload_sha256": str(row[8]),
        }
        for row in rows
    ]
    if len(output) != 22 or output[0]["event_date"] != source_v1.BASELINE_DATE:
        raise RuntimeError("exact_resolved_source_population_required")
    return output


def load_shape_outcomes(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT outcome_id,source_event_id,control_slot,arm_id,trade_mode,hold_minutes,
               detected,detection_utc,currency_direction,selected_instrument,
               selected_pair_direction,entry_spread_pips,net_after_cost_pips,mfe_pips,
               mae_pips,cost_cleared,analysis_eligible,row_sha256
        FROM fomc_shape_outcomes WHERE contract_id=?
        ORDER BY source_event_id,control_slot,arm_id,trade_mode,hold_minutes
        """,
        (shape.CONTRACT_ID,),
    ).fetchall()
    output = [
        {
            "outcome_id": str(row[0]), "source_event_id": str(row[1]),
            "control_slot": int(row[2]), "arm_id": str(row[3]),
            "trade_mode": str(row[4]), "hold_minutes": int(row[5]),
            "detected": int(row[6]), "detection_utc": row[7],
            "currency_direction": row[8], "selected_instrument": row[9],
            "selected_pair_direction": row[10], "entry_spread_pips": row[11],
            "net_after_cost_pips": float(row[12]), "mfe_pips": row[13],
            "mae_pips": row[14], "cost_cleared": int(row[15]),
            "analysis_eligible": int(row[16]), "row_sha256": str(row[17]),
        }
        for row in rows
    ]
    if len(output) != 1512:
        raise RuntimeError(f"exact_frozen_shape_outcome_population_required:{len(output)}")
    return output


def input_snapshot(
    connection: sqlite3.Connection,
    sources: Iterable[Mapping[str, Any]],
    outcomes: Iterable[Mapping[str, Any]],
) -> str:
    source_contract = connection.execute(
        "SELECT contract_json,builder_sha256,parent_snapshot_sha256 "
        "FROM fomc_statement_source_v2_contracts WHERE contract_id=?",
        (source_v2.CONTRACT_ID,),
    ).fetchone()
    shape_contract = connection.execute(
        "SELECT contract_json,builder_sha256,input_snapshot_sha256 "
        "FROM fomc_shape_contracts WHERE contract_id=?", (shape.CONTRACT_ID,)
    ).fetchone()
    if source_contract is None or shape_contract is None:
        raise RuntimeError("required_frozen_parent_contract_missing")
    material = {
        "source_contract": list(map(str, source_contract)),
        "shape_contract": list(map(str, shape_contract)),
        "source_identities": [
            [row["event_date"], row["document_id"], row["statement_text_sha256"], row["raw_payload_sha256"]]
            for row in sources
        ],
        "shape_identities": [
            [row["outcome_id"], row["row_sha256"]] for row in outcomes
        ],
    }
    return sha256_bytes(canonical_json(material).encode("utf-8"))


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS fomc_statement_factor_contracts (
          contract_id TEXT PRIMARY KEY, source_contract_id TEXT NOT NULL,
          response_contract_id TEXT NOT NULL, contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL, dependency_sha256_json TEXT NOT NULL,
          input_snapshot_sha256 TEXT NOT NULL, frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fomc_statement_factor_events (
          factor_event_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL,
          source_event_id TEXT NOT NULL, event_date TEXT NOT NULL,
          source_document_id TEXT NOT NULL, prior_document_id TEXT NOT NULL,
          event_clock_utc TEXT NOT NULL, source_url TEXT NOT NULL,
          state_json TEXT NOT NULL, prior_state_json TEXT NOT NULL,
          lexical_delta_json TEXT NOT NULL, row_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          causal_consensus_state TEXT NOT NULL, rate_repricing_state TEXT NOT NULL,
          retrospective_retrieval INTEGER NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          UNIQUE(contract_id,event_date)
        );
        CREATE TABLE IF NOT EXISTS fomc_statement_factor_observations (
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
        CREATE TABLE IF NOT EXISTS fomc_statement_factor_response_links (
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
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_contract_no_update BEFORE UPDATE ON fomc_statement_factor_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_contract_no_delete BEFORE DELETE ON fomc_statement_factor_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_event_no_update BEFORE UPDATE ON fomc_statement_factor_events BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_event_no_delete BEFORE DELETE ON fomc_statement_factor_events BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_observation_no_update BEFORE UPDATE ON fomc_statement_factor_observations BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_observation_no_delete BEFORE DELETE ON fomc_statement_factor_observations BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_link_no_update BEFORE UPDATE ON fomc_statement_factor_response_links BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_statement_factor_link_no_delete BEFORE DELETE ON fomc_statement_factor_response_links BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def freeze_contract(connection: sqlite3.Connection, snapshot_sha: str) -> dict[str, Any]:
    dependencies = {
        "source_v1": sha256_file(Path(source_v1.__file__).resolve()),
        "source_v2": sha256_file(Path(source_v2.__file__).resolve()),
        "shape": sha256_file(Path(shape.__file__).resolve()),
    }
    contract = {
        "schema_version": SCHEMA_VERSION, "contract_id": CONTRACT_ID,
        "source_contract_id": source_v2.CONTRACT_ID,
        "response_contract_id": shape.CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "dependency_sha256": dependencies, "input_snapshot_sha256": snapshot_sha,
        "factor_definitions": [list(row) for row in FACTOR_DEFINITIONS],
        "directional_discovery_policies": {
            "source_confirmed_follow": "factor_sign_equals_detected_currency_sign_then_follow",
            "source_conflicted_fade": "factor_sign_opposes_detected_currency_sign_then_fade_separate_aggressive_shadow_arm",
        },
        "response_cells": {
            "arms": [arm_id for arm_id, _arm in shape.ARMS],
            "holds_minutes": list(shape.HOLDS),
        },
        "inference": {
            "minimum_selected_events": MINIMUM_SELECTED_EVENTS,
            "randomization_draws": RANDOMIZATION_DRAWS,
            "randomization_seed": RANDOMIZATION_SEED,
            "multiplicity": "holm_all_tested_directional_factor_policy_arm_hold_cells",
            "required_cost_clearance": MINIMUM_COST_CLEARANCE,
        },
        "evidence_role": "adaptive_source_factor_discovery_after_price_only_fomc_null",
        "causal_consensus_state": "unavailable",
        "event_time_rate_repricing_state": "unavailable",
        "retrospective_official_text_not_untouched_confirmation": True,
        "research_only": True, "execution_eligible": False,
        "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
        "frozen_utc": FREEZE_UTC,
    }
    encoded = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM fomc_statement_factor_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is not None and str(existing[0]) != encoded:
        raise RuntimeError("immutable_fomc_statement_factor_contract_conflict")
    if existing is None:
        connection.execute(
            "INSERT INTO fomc_statement_factor_contracts VALUES(?,?,?,?,?,?,?,?)",
            (
                CONTRACT_ID, source_v2.CONTRACT_ID, shape.CONTRACT_ID, encoded,
                contract["builder_sha256"], canonical_json(dependencies), snapshot_sha, FREEZE_UTC,
            ),
        )
    connection.commit()
    return contract


def factor_values(
    current: Mapping[str, Any], prior: Mapping[str, Any], lexical: Mapping[str, float]
) -> list[dict[str, Any]]:
    raw = {
        "action_change_bps": (finite(current["action_change_bps"]), None, finite(current["action_change_bps"])),
        "activity_strength_change": (finite(current["activity_strength"]), finite(prior["activity_strength"]), finite(current["activity_strength"]) - finite(prior["activity_strength"])),
        "labor_tightness_change": (finite(current["labor_tightness"]), finite(prior["labor_tightness"]), finite(current["labor_tightness"]) - finite(prior["labor_tightness"])),
        "inflation_pressure_change": (finite(current["inflation_pressure"]), finite(prior["inflation_pressure"]), finite(current["inflation_pressure"]) - finite(prior["inflation_pressure"])),
        "inflation_progress_change": (finite(current["inflation_progress"]), finite(prior["inflation_progress"]), finite(current["inflation_progress"]) - finite(prior["inflation_progress"])),
        "risk_balance_change": (finite(current["risk_balance"]), finite(prior["risk_balance"]), finite(current["risk_balance"]) - finite(prior["risk_balance"])),
        "guidance_tilt_change": (finite(current["guidance_tilt"]), finite(prior["guidance_tilt"]), finite(current["guidance_tilt"]) - finite(prior["guidance_tilt"])),
        "balance_sheet_tightness_change": (finite(current["balance_sheet_tightness"]), finite(prior["balance_sheet_tightness"]), finite(current["balance_sheet_tightness"]) - finite(prior["balance_sheet_tightness"])),
        "dissent_tilt": (finite(current["dissent_tilt"]), None, finite(current["dissent_tilt"])),
        "dissent_count": (finite(current["dissent_count"]), None, finite(current["dissent_count"])),
        "sep_release_flag": (finite(current["sep_release_flag"]), None, finite(current["sep_release_flag"])),
        "statement_novelty": (finite(lexical["statement_novelty"]), None, finite(lexical["statement_novelty"])),
        "added_token_fraction": (finite(lexical["added_token_fraction"]), None, finite(lexical["added_token_fraction"])),
        "removed_token_fraction": (finite(lexical["removed_token_fraction"]), None, finite(lexical["removed_token_fraction"])),
    }
    definitions = {name: (role, meaning, mapping) for name, role, meaning, mapping in FACTOR_DEFINITIONS}
    output = []
    for name, (current_value, prior_value, delta) in raw.items():
        role, meaning, mapping = definitions[name]
        score = (
            current_value if mapping == "current" else -delta if mapping == "negative_delta" else delta
        )
        direction_state = (
            "economic_tilt_hawkish_positive" if role == "directional"
            else "magnitude_only" if role == "magnitude" else "context_only"
        )
        proof_state = (
            "explanatory_only_without_pre_release_expectation"
            if name == "action_change_bps"
            else "retrospective_official_text_discovery_only"
        )
        output.append(
            {
                "factor_name": name, "factor_role": role, "current_value": current_value,
                "prior_value": prior_value, "raw_delta": delta,
                "signed_factor_score": score if role == "directional" else 0.0,
                "economic_mapping": meaning, "direction_state": direction_state,
                "proof_state": proof_state,
            }
        )
    return output


def build_factor_rows(sources: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    schedule = {str(row["event_date"]): row for row in fomc.validate_schedule()}
    events: list[dict[str, Any]] = []
    factors: list[dict[str, Any]] = []
    previous = sources[0]
    previous_state = statement_state(previous["statement_text"], event_date=previous["event_date"], action_change_bps=0)
    for current in sources[1:]:
        event = schedule[str(current["event_date"])]
        action_bps = int(round(float(event["official_target_change_percentage_points"]) * 100.0))
        current_state = statement_state(
            current["statement_text"], event_date=current["event_date"], action_change_bps=action_bps
        )
        lexical = lexical_delta(current["statement_text"], previous["statement_text"])
        factor_event_id = stable_id("fomc_statement_factor_event", CONTRACT_ID, current["event_date"])
        event_payload = {
            "factor_event_id": factor_event_id, "contract_id": CONTRACT_ID,
            "source_event_id": event["event_id"], "event_date": current["event_date"],
            "source_document_id": current["document_id"], "prior_document_id": previous["document_id"],
            "event_clock_utc": current["event_clock_utc"], "source_url": current["source_url"],
            "state_json": canonical_json(current_state),
            "prior_state_json": canonical_json(previous_state),
            "lexical_delta_json": canonical_json(lexical),
            "causal_consensus_state": "unavailable", "rate_repricing_state": "unavailable",
            "retrospective_retrieval": 1, "research_only": 1,
            "execution_eligible": 0, "forecast_proof_eligible": 0,
        }
        encoded = canonical_json(event_payload)
        event_payload.update({"row_json": encoded, "row_sha256": sha256_bytes(encoded.encode("utf-8"))})
        events.append(event_payload)
        for factor in factor_values(current_state, previous_state, lexical):
            factor_id = stable_id(
                "fomc_statement_factor", CONTRACT_ID, current["event_date"], factor["factor_name"]
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
    if len(events) != 21 or len(factors) != 21 * len(FACTOR_DEFINITIONS):
        raise RuntimeError("factor_event_or_observation_count_mismatch")
    return events, factors


def store_factors(
    connection: sqlite3.Connection,
    events: Iterable[Mapping[str, Any]],
    factors: Iterable[Mapping[str, Any]],
) -> None:
    for event in events:
        immutable_insert(connection, "fomc_statement_factor_events", event, "factor_event_id")
    for factor in factors:
        immutable_insert(connection, "fomc_statement_factor_observations", factor, "factor_id")
    connection.commit()


def direction_sign(value: Any) -> int:
    return 1 if value == "stronger" else -1 if value == "weaker" else 0


def build_links(
    factors: Iterable[Mapping[str, Any]], outcomes: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    by_event: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for outcome in outcomes:
        by_event[str(outcome["source_event_id"])].append(outcome)
    links: list[dict[str, Any]] = []
    for factor in factors:
        factor_sign = sign(finite(factor["signed_factor_score"]))
        event_outcomes = by_event[str(factor["source_event_id"])]
        if len(event_outcomes) != 72:
            raise RuntimeError("exact_seventy_two_shape_outcomes_per_event_required")
        treatment_direction = {
            (str(row["arm_id"]), int(row["hold_minutes"])): direction_sign(row["currency_direction"])
            for row in event_outcomes if int(row["control_slot"]) == 0 and row["trade_mode"] == "follow"
        }
        for outcome in event_outcomes:
            key = (str(outcome["arm_id"]), int(outcome["hold_minutes"]))
            detected_sign = treatment_direction.get(key, 0)
            if int(outcome["control_slot"]) != 0:
                alignment = "matched_control_reference"
            elif not factor_sign or not detected_sign:
                alignment = "factor_or_detection_neutral"
            elif factor_sign == detected_sign:
                alignment = "source_confirmed_response"
            else:
                alignment = "source_conflicted_response"
            payload = {
                "link_id": stable_id("fomc_statement_factor_response_link", CONTRACT_ID, factor["factor_id"], outcome["outcome_id"]),
                "contract_id": CONTRACT_ID, "factor_id": factor["factor_id"],
                "source_event_id": factor["source_event_id"], "outcome_id": outcome["outcome_id"],
                "control_slot": int(outcome["control_slot"]), "arm_id": outcome["arm_id"],
                "trade_mode": outcome["trade_mode"], "hold_minutes": int(outcome["hold_minutes"]),
                "factor_present": int(int(outcome["control_slot"]) == 0),
                "factor_sign": factor_sign, "detected_currency_sign": detected_sign,
                "alignment_state": alignment, "selected_instrument": outcome["selected_instrument"],
                "net_after_cost_pips": float(outcome["net_after_cost_pips"]),
                "mfe_pips": outcome["mfe_pips"], "mae_pips": outcome["mae_pips"],
                "cost_cleared": int(outcome["cost_cleared"]), "research_only": 1,
                "execution_eligible": 0, "forecast_proof_eligible": 0,
            }
            encoded = canonical_json(payload)
            payload.update({"link_json": encoded, "row_sha256": sha256_bytes(encoded.encode("utf-8"))})
            links.append(payload)
    expected = 21 * len(FACTOR_DEFINITIONS) * 72
    if len(links) != expected:
        raise RuntimeError(f"factor_response_link_count_mismatch:{len(links)}:{expected}")
    return links


def store_links(connection: sqlite3.Connection, links: Iterable[Mapping[str, Any]]) -> None:
    for link in links:
        immutable_insert(connection, "fomc_statement_factor_response_links", link, "link_id")
    connection.commit()


def randomization_pvalue(triples: list[tuple[float, float, float]], seed: int) -> float:
    observed = sum(event - (before + after) / 2.0 for event, before, after in triples)
    choices = [
        (
            event - (before + after) / 2.0,
            before - (event + after) / 2.0,
            after - (event + before) / 2.0,
        )
        for event, before, after in triples
    ]
    generator = random.Random(seed)
    exceedances = 0
    for _ in range(RANDOMIZATION_DRAWS):
        value = sum(row[generator.randrange(3)] for row in choices)
        exceedances += int(value >= observed - 1e-12)
    return (exceedances + 1.0) / (RANDOMIZATION_DRAWS + 1.0)


def holm(rows: list[dict[str, Any]]) -> None:
    ordered = sorted(enumerate(rows), key=lambda item: (float(item[1]["raw_pvalue"]), str(item[1]["cell_id"])))
    running = 0.0
    count = len(ordered)
    for rank, (_index, row) in enumerate(ordered, start=1):
        running = max(running, min(1.0, (count - rank + 1) * float(row["raw_pvalue"])))
        row["holm_pvalue"] = running


def discovery_cells(
    factors: Iterable[Mapping[str, Any]], outcomes: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    factor_by_event: dict[tuple[str, str], Mapping[str, Any]] = {
        (str(row["factor_name"]), str(row["source_event_id"])): row
        for row in factors if str(row["factor_name"]) in DIRECTIONAL_FACTORS
    }
    outcome_map = {
        (
            str(row["source_event_id"]), int(row["control_slot"]), str(row["arm_id"]),
            str(row["trade_mode"]), int(row["hold_minutes"]),
        ): row
        for row in outcomes
    }
    event_ids = sorted({event_id for _factor, event_id in factor_by_event})
    rows: list[dict[str, Any]] = []
    policies = (
        ("source_confirmed_follow", "follow", 1),
        ("source_conflicted_fade", "fade", -1),
    )
    for factor_name in sorted(DIRECTIONAL_FACTORS):
        for policy_name, trade_mode, required_alignment in policies:
            for arm_id, _arm in shape.ARMS:
                for hold in shape.HOLDS:
                    triples: list[tuple[float, float, float]] = []
                    selected_ids: list[str] = []
                    event_cost_clear = 0
                    for event_id in event_ids:
                        factor = factor_by_event[(factor_name, event_id)]
                        factor_sign = sign(finite(factor["signed_factor_score"]))
                        treatment = outcome_map[(event_id, 0, arm_id, trade_mode, hold)]
                        detected_sign = direction_sign(treatment["currency_direction"])
                        if not factor_sign or not detected_sign or factor_sign * detected_sign != required_alignment:
                            continue
                        before = outcome_map[(event_id, -1, arm_id, trade_mode, hold)]
                        after = outcome_map[(event_id, 1, arm_id, trade_mode, hold)]
                        if not all(int(row["analysis_eligible"]) for row in (treatment, before, after)):
                            continue
                        triples.append(
                            (
                                float(treatment["net_after_cost_pips"]),
                                float(before["net_after_cost_pips"]),
                                float(after["net_after_cost_pips"]),
                            )
                        )
                        selected_ids.append(event_id)
                        event_cost_clear += int(treatment["cost_cleared"])
                    if len(triples) < MINIMUM_SELECTED_EVENTS:
                        continue
                    event_values = [row[0] for row in triples]
                    control_values = [(row[1] + row[2]) / 2.0 for row in triples]
                    increments = [event - control for event, control in zip(event_values, control_values)]
                    leave_one_out = [
                        sum(value for index, value in enumerate(event_values) if index != omitted) / (len(event_values) - 1)
                        for omitted in range(len(event_values))
                    ]
                    cell_id = f"{factor_name}:{policy_name}:{arm_id}:h{hold}"
                    seed = RANDOMIZATION_SEED + int(hashlib.sha256(cell_id.encode()).hexdigest()[:8], 16)
                    rows.append(
                        {
                            "cell_id": cell_id, "factor_name": factor_name,
                            "policy": policy_name, "arm_id": arm_id, "trade_mode": trade_mode,
                            "hold_minutes": int(hold), "event_n": len(triples),
                            "event_ids": selected_ids,
                            "average_event_net_pips": sum(event_values) / len(event_values),
                            "average_control_net_pips": sum(control_values) / len(control_values),
                            "average_incremental_net_pips": sum(increments) / len(increments),
                            "cost_clearance_rate": event_cost_clear / len(triples),
                            "minimum_leave_one_out_event_net_pips": min(leave_one_out),
                            "raw_pvalue": randomization_pvalue(triples, seed),
                        }
                    )
    holm(rows)
    for row in rows:
        gates = {
            "minimum_n": int(row["event_n"]) >= MINIMUM_SELECTED_EVENTS,
            "positive_event_net": float(row["average_event_net_pips"]) > 0.0,
            "positive_incremental_net": float(row["average_incremental_net_pips"]) > 0.0,
            "leave_one_out_positive": float(row["minimum_leave_one_out_event_net_pips"]) > 0.0,
            "cost_clearance": float(row["cost_clearance_rate"]) >= MINIMUM_COST_CLEARANCE,
            "holm_significance": float(row["holm_pvalue"]) <= 0.05,
        }
        row["gates"] = gates
        row["discovery_seed"] = all(gates.values())
    return sorted(rows, key=lambda row: (float(row["holm_pvalue"]), -float(row["average_incremental_net_pips"]), str(row["cell_id"])))


def snapshot(
    connection: sqlite3.Connection,
    contract: Mapping[str, Any],
    cells: list[dict[str, Any]],
) -> dict[str, Any]:
    counts = {
        "factor_events": int(connection.execute("SELECT COUNT(*) FROM fomc_statement_factor_events WHERE contract_id=?", (CONTRACT_ID,)).fetchone()[0]),
        "factor_observations": int(connection.execute("SELECT COUNT(*) FROM fomc_statement_factor_observations WHERE contract_id=?", (CONTRACT_ID,)).fetchone()[0]),
        "response_links": int(connection.execute("SELECT COUNT(*) FROM fomc_statement_factor_response_links WHERE contract_id=?", (CONTRACT_ID,)).fetchone()[0]),
    }
    result = {
        "schema_version": SCHEMA_VERSION, "generated_utc": utc_now(),
        "contract_id": CONTRACT_ID, "builder_sha256": contract["builder_sha256"],
        "source_contract_id": source_v2.CONTRACT_ID, "response_contract_id": shape.CONTRACT_ID,
        "input_snapshot_sha256": contract["input_snapshot_sha256"], **counts,
        "directional_factor_count": len(DIRECTIONAL_FACTORS),
        "tested_cell_count": len(cells),
        "discovery_seed_count": sum(bool(row["discovery_seed"]) for row in cells),
        "best_cells": cells[:12],
        "causal_consensus_state": "unavailable",
        "event_time_rate_repricing_state": "unavailable",
        "sqlite_integrity": str(connection.execute("PRAGMA integrity_check").fetchone()[0]),
        "research_only": True, "execution_eligible": False,
        "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
    }
    result["snapshot_sha256"] = sha256_bytes(canonical_json(result).encode("utf-8"))
    return result


def write_report(result: Mapping[str, Any]) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    (REPORT_ROOT / "FOMC_STATEMENT_FACTOR_RESPONSE_V1.json").write_text(
        json.dumps(result, indent=2, sort_keys=True), encoding="utf-8"
    )
    lines = [
        "# FOMC statement factor-response discovery V1", "",
        f"Generated: `{result['generated_utc']}`", "",
        f"- Factor events / observations: **{result['factor_events']} / {result['factor_observations']}**",
        f"- Immutable factor-response links: **{result['response_links']:,}**",
        f"- Multiplicity family / discovery seeds: **{result['tested_cell_count']} / {result['discovery_seed_count']}**",
        f"- SQLite integrity: **{result['sqlite_integrity']}**",
        "- Causal pre-release consensus: **unavailable**.",
        "- Event-time policy-path repricing: **unavailable**.",
        "- Status: adaptive retrospective discovery only; no-trade and execution-ineligible.", "",
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
    (REPORT_ROOT / "FOMC_STATEMENT_FACTOR_RESPONSE_V1.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def build(*, database_path: Path = DATABASE) -> dict[str, Any]:
    connection = sqlite3.connect(database_path, timeout=120)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    sources = load_source_documents(connection)
    outcomes = load_shape_outcomes(connection)
    snapshot_sha = input_snapshot(connection, sources, outcomes)
    contract = freeze_contract(connection, snapshot_sha)
    factor_events, factors = build_factor_rows(sources)
    store_factors(connection, factor_events, factors)
    links = build_links(factors, outcomes)
    store_links(connection, links)
    cells = discovery_cells(factors, outcomes)
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


__all__ = [
    "CONTRACT_ID", "DIRECTIONAL_FACTORS", "FACTOR_DEFINITIONS", "build",
    "clean_statement", "discovery_cells", "dissent_state", "ensure_schema",
    "factor_values", "lexical_delta", "ordinal_state", "policy_text",
    "preferred_changes", "statement_state",
]
