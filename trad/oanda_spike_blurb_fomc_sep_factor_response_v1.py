#!/usr/bin/env python3
"""Link exact official FOMC projection deltas to frozen response outcomes.

The factor vocabulary, response grid, gates, and multiplicity family are fixed
in this module.  It consumes only the immutable official SEP source cohort and
the existing immutable FOMC event/control response paths.  All results are
retrospective discovery, research-only, and execution-ineligible.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data/oanda_training_manager/state/spike_blurb_factor_reconstruction_v1.sqlite"
REPORT_ROOT = ROOT / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "fomc_sep_factor_response_v1"
)
CONTRACT_ID = "spike_blurb_fomc_sep_factor_response_v1_20260820"
SOURCE_CONTRACT_ID = "spike_blurb_fomc_sep_sources_v1_20260820"
RESPONSE_CONTRACT_ID = "spike_blurb_fomc_response_shape_discovery_v1_20260820"
FOMC_CONTRACT_ID = "spike_blurb_fomc_response_generalization_v1_20260820"
EXPECTED_SOURCE_BUILDER_SHA256 = "ca3e1a9bf1ac3c00d555ee8e5c691a8d9dda4d1eb69e81e6add03d8be39643a2"
EXPECTED_RESPONSE_BUILDER_SHA256 = "0f1ee9ba0c927ee0a83db95aa85e6ef4061b82140c7e4fe1a486a98c3b767a31"
SCHEMA_VERSION = 1
FREEZE_UTC = "2026-08-20T19:20:00+00:00"
MINIMUM_SELECTED_EVENTS = 5
MINIMUM_COST_CLEARANCE = 0.55
RANDOMIZATION_DRAWS = 100_000
RANDOMIZATION_SEED = 20_260_822
ARMS = ("response_at_1m", "response_at_3m_persist_2", "response_at_5m_persist_3")
HOLDS = (3, 5, 10, 15)


FACTOR_DEFINITIONS: tuple[tuple[str, str, str], ...] = (
    ("fed_funds_current_year_change_bps", "directional", "positive_is_hawkish"),
    ("fed_funds_next_year_change_bps", "directional", "positive_is_hawkish"),
    ("fed_funds_longer_run_change_bps", "directional", "positive_is_hawkish"),
    ("fed_funds_curve_slope_change_bps", "directional", "positive_is_hawkish"),
    ("pce_current_year_change_pp", "directional", "positive_is_hawkish"),
    ("core_pce_current_year_change_pp", "directional", "positive_is_hawkish"),
    ("unemployment_current_year_change_pp", "directional", "positive_is_dovish"),
    ("real_gdp_current_year_change_pp", "directional", "positive_is_hawkish"),
    ("policy_path_absolute_revision_bps", "magnitude", "magnitude_only"),
    ("macro_absolute_revision_pp", "magnitude", "magnitude_only"),
    ("projection_revision_count", "context", "context_only"),
)
DIRECTIONAL_FACTORS = frozenset(
    name for name, role, _mapping in FACTOR_DEFINITIONS if role == "directional"
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def stable_id(*parts: Any) -> str:
    return sha256_bytes("|".join(str(part) for part in parts).encode("utf-8"))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def sign(value: Any) -> int:
    number = finite(value)
    return 1 if number > 0 else -1 if number < 0 else 0


def direction_sign(value: Any) -> int:
    return 1 if value == "stronger" else -1 if value == "weaker" else 0


def immutable_insert(
    connection: sqlite3.Connection, table: str, row: Mapping[str, Any], primary_key: str
) -> None:
    existing = connection.execute(
        f"SELECT * FROM {table} WHERE {primary_key}=?", (row[primary_key],)
    ).fetchone()
    columns = list(row)
    if existing is not None:
        names = [item[1] for item in connection.execute(f"PRAGMA table_info({table})")]
        if dict(zip(names, existing)) != dict(row):
            raise RuntimeError(f"immutable_conflict:{table}:{row[primary_key]}")
        return
    connection.execute(
        f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
        tuple(row[column] for column in columns),
    )


def _delta(current: Mapping[str, float], prior: Mapping[str, float], horizon: str) -> float:
    if horizon not in current or horizon not in prior:
        raise RuntimeError(f"projection_horizon_missing:{horizon}")
    return round(finite(current[horizon]) - finite(prior[horizon]), 10)


def factor_values(projection: Mapping[str, Any], event_year: int) -> list[dict[str, Any]]:
    current_year = str(event_year)
    next_year = str(event_year + 1)
    funds = projection["federal_funds_rate"]
    pce = projection["pce_inflation"]
    core = projection["core_pce_inflation"]
    unemployment = projection["unemployment_rate"]
    gdp = projection["real_gdp_growth"]

    funds_current = _delta(funds["current"], funds["prior"], current_year) * 100.0
    funds_next = _delta(funds["current"], funds["prior"], next_year) * 100.0
    funds_long = _delta(funds["current"], funds["prior"], "longer_run") * 100.0
    slope_current = (finite(funds["current"][next_year]) - finite(funds["current"][current_year])) * 100.0
    slope_prior = (finite(funds["prior"][next_year]) - finite(funds["prior"][current_year])) * 100.0
    funds_slope = round(slope_current - slope_prior, 10)
    pce_delta = _delta(pce["current"], pce["prior"], current_year)
    core_delta = _delta(core["current"], core["prior"], current_year)
    unemployment_delta = _delta(unemployment["current"], unemployment["prior"], current_year)
    gdp_delta = _delta(gdp["current"], gdp["prior"], current_year)
    policy_abs = abs(funds_current) + abs(funds_next) + abs(funds_long)
    macro_abs = abs(pce_delta) + abs(core_delta) + abs(unemployment_delta) + abs(gdp_delta)
    raw = {
        "fed_funds_current_year_change_bps": (funds_current, funds["current"][current_year], funds["prior"][current_year]),
        "fed_funds_next_year_change_bps": (funds_next, funds["current"][next_year], funds["prior"][next_year]),
        "fed_funds_longer_run_change_bps": (funds_long, funds["current"]["longer_run"], funds["prior"]["longer_run"]),
        "fed_funds_curve_slope_change_bps": (funds_slope, slope_current, slope_prior),
        "pce_current_year_change_pp": (pce_delta, pce["current"][current_year], pce["prior"][current_year]),
        "core_pce_current_year_change_pp": (core_delta, core["current"][current_year], core["prior"][current_year]),
        "unemployment_current_year_change_pp": (unemployment_delta, unemployment["current"][current_year], unemployment["prior"][current_year]),
        "real_gdp_current_year_change_pp": (gdp_delta, gdp["current"][current_year], gdp["prior"][current_year]),
        "policy_path_absolute_revision_bps": (policy_abs, policy_abs, 0.0),
        "macro_absolute_revision_pp": (macro_abs, macro_abs, 0.0),
        "projection_revision_count": (
            float(sum(abs(value) > 1e-12 for value in (funds_current, funds_next, funds_long, funds_slope, pce_delta, core_delta, unemployment_delta, gdp_delta))),
            0.0,
            0.0,
        ),
    }
    definitions = {name: (role, mapping) for name, role, mapping in FACTOR_DEFINITIONS}
    output = []
    for name, (delta, current_value, prior_value) in raw.items():
        role, mapping = definitions[name]
        score = -delta if mapping == "positive_is_dovish" else delta
        output.append(
            {
                "factor_name": name,
                "factor_role": role,
                "current_value": finite(current_value),
                "prior_value": finite(prior_value),
                "raw_delta": finite(delta),
                "signed_factor_score": finite(score) if role == "directional" else 0.0,
                "economic_mapping": mapping,
                "direction_state": "policy_implication_hawkish_positive" if role == "directional" else "direction_abstain",
                "proof_state": "retrospective_official_projection_discovery_only",
            }
        )
    return output


def load_sources(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    contract = connection.execute(
        "SELECT builder_sha256 FROM fomc_sep_source_contracts_v1 WHERE contract_id=?",
        (SOURCE_CONTRACT_ID,),
    ).fetchone()
    if contract is None or str(contract[0]) != EXPECTED_SOURCE_BUILDER_SHA256:
        raise RuntimeError("exact_fomc_sep_source_parent_required")
    rows = connection.execute(
        "SELECT document_id,event_id,event_date,event_clock_utc,source_url,raw_payload_sha256,"
        "projection_json,row_sha256 FROM fomc_sep_source_documents_v1 "
        "WHERE contract_id=? AND document_role='analysis_event' ORDER BY event_date",
        (SOURCE_CONTRACT_ID,),
    ).fetchall()
    output = [
        {
            "document_id": str(row[0]), "sep_event_id": str(row[1]),
            "event_date": str(row[2]), "event_clock_utc": str(row[3]),
            "source_url": str(row[4]), "raw_payload_sha256": str(row[5]),
            "projection": json.loads(str(row[6])), "row_sha256": str(row[7]),
        }
        for row in rows
    ]
    if len(output) != 10:
        raise RuntimeError(f"exact_ten_sep_analysis_events_required:{len(output)}")
    return output


def load_event_map(connection: sqlite3.Connection) -> dict[str, str]:
    rows = connection.execute(
        "SELECT event_date,event_id FROM fomc_generalization_events WHERE contract_id=?",
        (FOMC_CONTRACT_ID,),
    ).fetchall()
    result = {str(row[0]): str(row[1]) for row in rows}
    if len(result) != 21:
        raise RuntimeError("exact_fomc_event_map_required")
    return result


def load_outcomes(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    contract = connection.execute(
        "SELECT builder_sha256 FROM fomc_shape_contracts WHERE contract_id=?",
        (RESPONSE_CONTRACT_ID,),
    ).fetchone()
    if contract is None or str(contract[0]) != EXPECTED_RESPONSE_BUILDER_SHA256:
        raise RuntimeError("exact_fomc_shape_parent_required")
    rows = connection.execute(
        "SELECT outcome_id,source_event_id,control_slot,arm_id,trade_mode,hold_minutes,"
        "detected,detection_utc,currency_direction,selected_instrument,selected_pair_direction,"
        "entry_spread_pips,net_after_cost_pips,mfe_pips,mae_pips,cost_cleared,analysis_eligible,row_sha256 "
        "FROM fomc_shape_outcomes WHERE contract_id=? ORDER BY source_event_id,control_slot,arm_id,trade_mode,hold_minutes",
        (RESPONSE_CONTRACT_ID,),
    ).fetchall()
    output = [
        {
            "outcome_id": str(row[0]), "source_event_id": str(row[1]),
            "control_slot": int(row[2]), "arm_id": str(row[3]), "trade_mode": str(row[4]),
            "hold_minutes": int(row[5]), "detected": int(row[6]), "detection_utc": row[7],
            "currency_direction": row[8], "selected_instrument": row[9],
            "selected_pair_direction": row[10], "entry_spread_pips": row[11],
            "net_after_cost_pips": float(row[12]), "mfe_pips": row[13], "mae_pips": row[14],
            "cost_cleared": int(row[15]), "analysis_eligible": int(row[16]), "row_sha256": str(row[17]),
        }
        for row in rows
    ]
    if len(output) != 1512:
        raise RuntimeError(f"exact_1512_fomc_shape_outcomes_required:{len(output)}")
    return output


def input_snapshot(connection: sqlite3.Connection, sources: Iterable[Mapping[str, Any]], outcomes: Iterable[Mapping[str, Any]]) -> str:
    source_contract = connection.execute(
        "SELECT contract_json,builder_sha256,source_population_sha256 FROM fomc_sep_source_contracts_v1 WHERE contract_id=?",
        (SOURCE_CONTRACT_ID,),
    ).fetchone()
    shape_contract = connection.execute(
        "SELECT contract_json,builder_sha256,input_snapshot_sha256 FROM fomc_shape_contracts WHERE contract_id=?",
        (RESPONSE_CONTRACT_ID,),
    ).fetchone()
    material = {
        "source_contract": list(map(str, source_contract or ())),
        "shape_contract": list(map(str, shape_contract or ())),
        "source_rows": [[row["event_date"], row["document_id"], row["raw_payload_sha256"], row["row_sha256"]] for row in sources],
        "outcome_rows": [[row["outcome_id"], row["row_sha256"]] for row in outcomes],
    }
    return sha256_bytes(canonical_json(material).encode("utf-8"))


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS fomc_sep_factor_contracts_v1 (
          contract_id TEXT PRIMARY KEY, source_contract_id TEXT NOT NULL,
          response_contract_id TEXT NOT NULL, contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL, input_snapshot_sha256 TEXT NOT NULL,
          frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fomc_sep_factor_events_v1 (
          factor_event_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL,
          source_event_id TEXT NOT NULL, sep_event_id TEXT NOT NULL,
          event_date TEXT NOT NULL, event_clock_utc TEXT NOT NULL,
          source_document_id TEXT NOT NULL, source_url TEXT NOT NULL,
          source_payload_sha256 TEXT NOT NULL, event_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL, research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          UNIQUE(contract_id,event_date)
        );
        CREATE TABLE IF NOT EXISTS fomc_sep_factor_observations_v1 (
          factor_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL,
          factor_event_id TEXT NOT NULL, source_event_id TEXT NOT NULL,
          event_date TEXT NOT NULL, factor_name TEXT NOT NULL, factor_role TEXT NOT NULL,
          current_value REAL NOT NULL, prior_value REAL NOT NULL, raw_delta REAL NOT NULL,
          signed_factor_score REAL NOT NULL, economic_mapping TEXT NOT NULL,
          direction_state TEXT NOT NULL, proof_state TEXT NOT NULL,
          factor_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          UNIQUE(contract_id,event_date,factor_name)
        );
        CREATE TABLE IF NOT EXISTS fomc_sep_factor_response_links_v1 (
          link_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, factor_id TEXT NOT NULL,
          source_event_id TEXT NOT NULL, outcome_id TEXT NOT NULL, control_slot INTEGER NOT NULL,
          arm_id TEXT NOT NULL, trade_mode TEXT NOT NULL, hold_minutes INTEGER NOT NULL,
          factor_sign INTEGER NOT NULL, detected_currency_sign INTEGER NOT NULL,
          alignment_state TEXT NOT NULL, selected_instrument TEXT,
          net_after_cost_pips REAL NOT NULL, mfe_pips REAL, mae_pips REAL,
          cost_cleared INTEGER NOT NULL, link_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          UNIQUE(contract_id,factor_id,outcome_id)
        );
        CREATE TRIGGER IF NOT EXISTS fomc_sep_factor_contract_v1_no_update BEFORE UPDATE ON fomc_sep_factor_contracts_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_factor_contract_v1_no_delete BEFORE DELETE ON fomc_sep_factor_contracts_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_factor_event_v1_no_update BEFORE UPDATE ON fomc_sep_factor_events_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_factor_event_v1_no_delete BEFORE DELETE ON fomc_sep_factor_events_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_factor_observation_v1_no_update BEFORE UPDATE ON fomc_sep_factor_observations_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_factor_observation_v1_no_delete BEFORE DELETE ON fomc_sep_factor_observations_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_factor_link_v1_no_update BEFORE UPDATE ON fomc_sep_factor_response_links_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_sep_factor_link_v1_no_delete BEFORE DELETE ON fomc_sep_factor_response_links_v1 BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def freeze_contract(connection: sqlite3.Connection, snapshot_sha: str) -> dict[str, Any]:
    contract = {
        "schema_version": SCHEMA_VERSION, "contract_id": CONTRACT_ID,
        "source_contract_id": SOURCE_CONTRACT_ID, "response_contract_id": RESPONSE_CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "input_snapshot_sha256": snapshot_sha,
        "expected_source_builder_sha256": EXPECTED_SOURCE_BUILDER_SHA256,
        "expected_response_builder_sha256": EXPECTED_RESPONSE_BUILDER_SHA256,
        "factor_definitions": [list(row) for row in FACTOR_DEFINITIONS],
        "directional_policies": {
            "source_confirmed_follow": "projection_factor_sign_equals_detected_USD_sign_then_follow",
            "source_conflicted_fade": "projection_factor_sign_opposes_detected_USD_sign_then_fade_aggressive_shadow_only",
        },
        "response_grid": {"arms": list(ARMS), "holds_minutes": list(HOLDS)},
        "inference": {
            "minimum_selected_events": MINIMUM_SELECTED_EVENTS,
            "minimum_cost_clearance": MINIMUM_COST_CLEARANCE,
            "randomization_draws": RANDOMIZATION_DRAWS,
            "randomization_seed": RANDOMIZATION_SEED,
            "multiplicity": "holm_all_tested_factor_policy_arm_hold_cells",
        },
        "causal_pre_release_consensus": "unavailable",
        "event_time_market_repricing": "unavailable",
        "evidence_role": "retrospective_source_projection_discovery",
        "research_only": True, "execution_eligible": False,
        "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
        "frozen_utc": FREEZE_UTC,
    }
    encoded = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM fomc_sep_factor_contracts_v1 WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()
    if existing is not None and str(existing[0]) != encoded:
        raise RuntimeError("immutable_fomc_sep_factor_contract_conflict")
    if existing is None:
        connection.execute(
            "INSERT INTO fomc_sep_factor_contracts_v1 VALUES(?,?,?,?,?,?,?)",
            (CONTRACT_ID, SOURCE_CONTRACT_ID, RESPONSE_CONTRACT_ID, encoded,
             contract["builder_sha256"], snapshot_sha, FREEZE_UTC),
        )
    connection.commit()
    return contract


def build_factor_rows(sources: Iterable[Mapping[str, Any]], event_map: Mapping[str, str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    events: list[dict[str, Any]] = []
    factors: list[dict[str, Any]] = []
    for source in sources:
        event_date = str(source["event_date"])
        source_event_id = event_map[event_date]
        factor_event_id = stable_id("fomc_sep_factor_event_v1", CONTRACT_ID, event_date)
        event_payload = {
            "factor_event_id": factor_event_id, "contract_id": CONTRACT_ID,
            "source_event_id": source_event_id, "sep_event_id": source["sep_event_id"],
            "event_date": event_date, "event_clock_utc": source["event_clock_utc"],
            "source_document_id": source["document_id"], "source_url": source["source_url"],
            "source_payload_sha256": source["raw_payload_sha256"],
            "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
        }
        encoded = canonical_json(event_payload)
        event_payload.update({"event_json": encoded, "row_sha256": sha256_bytes(encoded.encode("utf-8"))})
        events.append(event_payload)
        for factor in factor_values(source["projection"], int(event_date[:4])):
            factor_id = stable_id("fomc_sep_factor_v1", CONTRACT_ID, event_date, factor["factor_name"])
            payload = {
                "factor_id": factor_id, "contract_id": CONTRACT_ID,
                "factor_event_id": factor_event_id, "source_event_id": source_event_id,
                "event_date": event_date, **factor,
                "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
            }
            factor_json = canonical_json(payload)
            payload.update({"factor_json": factor_json, "row_sha256": sha256_bytes(factor_json.encode("utf-8"))})
            factors.append(payload)
    expected = 10 * len(FACTOR_DEFINITIONS)
    if len(events) != 10 or len(factors) != expected:
        raise RuntimeError(f"fomc_sep_factor_population_mismatch:{len(events)}:{len(factors)}:{expected}")
    return events, factors


def build_links(factors: Iterable[Mapping[str, Any]], outcomes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_event: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for outcome in outcomes:
        by_event[str(outcome["source_event_id"])].append(outcome)
    links: list[dict[str, Any]] = []
    for factor in factors:
        factor_sign = sign(factor["signed_factor_score"])
        event_outcomes = by_event[str(factor["source_event_id"])]
        if len(event_outcomes) != 72:
            raise RuntimeError("exact_seventy_two_shape_outcomes_per_sep_event_required")
        treatment_direction = {
            (str(row["arm_id"]), int(row["hold_minutes"])): direction_sign(row["currency_direction"])
            for row in event_outcomes if int(row["control_slot"]) == 0 and row["trade_mode"] == "follow"
        }
        for outcome in event_outcomes:
            detected_sign = treatment_direction[(str(outcome["arm_id"]), int(outcome["hold_minutes"]))]
            if int(outcome["control_slot"]) != 0:
                alignment = "matched_control_reference"
            elif not factor_sign or not detected_sign:
                alignment = "factor_or_detection_neutral"
            elif factor_sign == detected_sign:
                alignment = "source_confirmed_response"
            else:
                alignment = "source_conflicted_response"
            payload = {
                "link_id": stable_id("fomc_sep_factor_response_link_v1", CONTRACT_ID, factor["factor_id"], outcome["outcome_id"]),
                "contract_id": CONTRACT_ID, "factor_id": factor["factor_id"],
                "source_event_id": factor["source_event_id"], "outcome_id": outcome["outcome_id"],
                "control_slot": int(outcome["control_slot"]), "arm_id": outcome["arm_id"],
                "trade_mode": outcome["trade_mode"], "hold_minutes": int(outcome["hold_minutes"]),
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
    expected = 10 * len(FACTOR_DEFINITIONS) * 72
    if len(links) != expected:
        raise RuntimeError(f"fomc_sep_link_population_mismatch:{len(links)}:{expected}")
    return links


def randomization_pvalue(triples: list[tuple[float, float, float]], seed: int) -> float:
    observed = sum(event - (before + after) / 2.0 for event, before, after in triples)
    choices = [
        (event - (before + after) / 2.0, before - (event + after) / 2.0, after - (event + before) / 2.0)
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


def discovery_cells(factors: Iterable[Mapping[str, Any]], outcomes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    factor_by_event = {
        (str(row["factor_name"]), str(row["source_event_id"])): row
        for row in factors if str(row["factor_name"]) in DIRECTIONAL_FACTORS
    }
    outcome_map = {
        (str(row["source_event_id"]), int(row["control_slot"]), str(row["arm_id"]), str(row["trade_mode"]), int(row["hold_minutes"])): row
        for row in outcomes
    }
    event_ids = sorted({event_id for _factor, event_id in factor_by_event})
    rows: list[dict[str, Any]] = []
    policies = (("source_confirmed_follow", "follow", 1), ("source_conflicted_fade", "fade", -1))
    for factor_name in sorted(DIRECTIONAL_FACTORS):
        for policy_name, trade_mode, required_alignment in policies:
            for arm_id in ARMS:
                for hold in HOLDS:
                    triples: list[tuple[float, float, float]] = []
                    selected_ids: list[str] = []
                    cost_clear = 0
                    for event_id in event_ids:
                        factor = factor_by_event[(factor_name, event_id)]
                        factor_sign = sign(factor["signed_factor_score"])
                        treatment = outcome_map[(event_id, 0, arm_id, trade_mode, hold)]
                        detected_sign = direction_sign(treatment["currency_direction"])
                        if not factor_sign or not detected_sign or factor_sign * detected_sign != required_alignment:
                            continue
                        before = outcome_map[(event_id, -1, arm_id, trade_mode, hold)]
                        after = outcome_map[(event_id, 1, arm_id, trade_mode, hold)]
                        if not all(int(row["analysis_eligible"]) for row in (treatment, before, after)):
                            continue
                        triples.append((float(treatment["net_after_cost_pips"]), float(before["net_after_cost_pips"]), float(after["net_after_cost_pips"])))
                        selected_ids.append(event_id)
                        cost_clear += int(treatment["cost_cleared"])
                    if len(triples) < MINIMUM_SELECTED_EVENTS:
                        continue
                    event_values = [row[0] for row in triples]
                    controls = [(row[1] + row[2]) / 2.0 for row in triples]
                    increments = [event - control for event, control in zip(event_values, controls)]
                    leave_one_out = [
                        sum(value for index, value in enumerate(event_values) if index != omitted) / (len(event_values) - 1)
                        for omitted in range(len(event_values))
                    ]
                    cell_id = f"{factor_name}:{policy_name}:{arm_id}:h{hold}"
                    seed = RANDOMIZATION_SEED + int(hashlib.sha256(cell_id.encode()).hexdigest()[:8], 16)
                    rows.append(
                        {
                            "cell_id": cell_id, "factor_name": factor_name, "policy": policy_name,
                            "arm_id": arm_id, "trade_mode": trade_mode, "hold_minutes": hold,
                            "event_n": len(triples), "event_ids": selected_ids,
                            "average_event_net_pips": sum(event_values) / len(event_values),
                            "average_control_net_pips": sum(controls) / len(controls),
                            "average_incremental_net_pips": sum(increments) / len(increments),
                            "cost_clearance_rate": cost_clear / len(triples),
                            "minimum_leave_one_out_event_net_pips": min(leave_one_out),
                            "raw_pvalue": randomization_pvalue(triples, seed),
                        }
                    )
    holm(rows)
    for row in rows:
        gates = {
            "minimum_n": row["event_n"] >= MINIMUM_SELECTED_EVENTS,
            "positive_event_net": row["average_event_net_pips"] > 0.0,
            "positive_incremental_net": row["average_incremental_net_pips"] > 0.0,
            "leave_one_out_positive": row["minimum_leave_one_out_event_net_pips"] > 0.0,
            "cost_clearance": row["cost_clearance_rate"] >= MINIMUM_COST_CLEARANCE,
            "holm_significance": row["holm_pvalue"] <= 0.05,
        }
        row["gates"] = gates
        row["discovery_seed"] = all(gates.values())
    return sorted(rows, key=lambda row: (float(row["holm_pvalue"]), -float(row["average_incremental_net_pips"]), str(row["cell_id"])))


def write_report(result: Mapping[str, Any]) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    (REPORT_ROOT / "FOMC_SEP_FACTOR_RESPONSE_V1.json").write_text(json.dumps(dict(result), indent=2, sort_keys=True), encoding="utf-8")
    lines = [
        "# FOMC SEP factor-response discovery V1", "",
        f"- Events / factors / links: **{result['factor_events']} / {result['factor_observations']} / {result['response_links']:,}**",
        f"- Tested cells / seeds: **{result['tested_cell_count']} / {result['discovery_seed_count']}**",
        f"- SQLite integrity: **{result['sqlite_integrity']}**", "",
        "Official projection changes are retrospective discovery inputs. Pre-release consensus",
        "and event-time traded-rate repricing remain unavailable. No result can execute.", "",
        "| Cell | N | Event | Control | Increment | Clear | LOO | Raw p | Holm p | Seed |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    lines.extend(
        f"| {row['cell_id']} | {row['event_n']} | {row['average_event_net_pips']:+.3f} | {row['average_control_net_pips']:+.3f} | {row['average_incremental_net_pips']:+.3f} | {row['cost_clearance_rate']:.1%} | {row['minimum_leave_one_out_event_net_pips']:+.3f} | {row['raw_pvalue']:.6f} | {row['holm_pvalue']:.6f} | {row['discovery_seed']} |"
        for row in result["best_cells"]
    )
    (REPORT_ROOT / "FOMC_SEP_FACTOR_RESPONSE_V1.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def build(*, database_path: Path = DATABASE) -> dict[str, Any]:
    connection = sqlite3.connect(database_path, timeout=120.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    sources = load_sources(connection)
    event_map = load_event_map(connection)
    outcomes = load_outcomes(connection)
    snapshot_sha = input_snapshot(connection, sources, outcomes)
    contract = freeze_contract(connection, snapshot_sha)
    events, factors = build_factor_rows(sources, event_map)
    for row in events:
        immutable_insert(connection, "fomc_sep_factor_events_v1", row, "factor_event_id")
    for row in factors:
        immutable_insert(connection, "fomc_sep_factor_observations_v1", row, "factor_id")
    connection.commit()
    links = build_links(factors, outcomes)
    for row in links:
        immutable_insert(connection, "fomc_sep_factor_response_links_v1", row, "link_id")
    connection.commit()
    cells = discovery_cells(factors, outcomes)
    result = {
        "schema_version": SCHEMA_VERSION, "contract_id": CONTRACT_ID,
        "builder_sha256": contract["builder_sha256"], "input_snapshot_sha256": snapshot_sha,
        "factor_events": len(events), "factor_observations": len(factors),
        "response_links": len(links), "directional_factor_count": len(DIRECTIONAL_FACTORS),
        "tested_cell_count": len(cells), "discovery_seed_count": sum(bool(row["discovery_seed"]) for row in cells),
        "best_cells": cells[:12], "causal_pre_release_consensus": "unavailable",
        "event_time_market_repricing": "unavailable",
        "sqlite_integrity": str(connection.execute("PRAGMA integrity_check").fetchone()[0]),
        "research_only": True, "execution_eligible": False,
        "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
        "generated_utc": utc_now(),
    }
    result["snapshot_sha256"] = sha256_bytes(canonical_json(result).encode("utf-8"))
    connection.close()
    write_report(result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE)
    args = parser.parse_args()
    print(json.dumps(build(database_path=args.database), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
