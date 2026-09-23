#!/usr/bin/env python3
"""Independently verify the sequential portfolio replay from frozen M1 data.

This module intentionally imports neither the producer nor either replay core.
It rebuilds causal clocks, candidates, policy actions, exact bid/ask fills,
per-leg costs, portfolio state, counterfactual branches, feedback, row hashes,
roots, and the terminal seal using an independent implementation.
"""

from __future__ import annotations

import ast
import csv
from datetime import datetime, timezone
import gzip
from hashlib import sha256
import io
import json
import math
from pathlib import Path
import sqlite3
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "sequential_portfolio_replay_v1.json"
PRODUCER = ROOT / "oanda_sequential_portfolio_replay.py"
CORE = ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_replay_v1.py"
ARTIFACT_ROOT = ROOT / "data" / "oanda_training_manager" / "research_ledgers" / "sequential_portfolio_replay_v1"
STATE = ARTIFACT_ROOT / "sequential_portfolio_replay_v1.json"
OUTPUT = ARTIFACT_ROOT / "sequential_portfolio_replay_verifier_v1.json"

TABLES = {
    "spr_cohorts", "spr_sessions", "spr_pair_contexts", "spr_clocks",
    "spr_decisions", "spr_execution_outcomes", "spr_execution_legs",
    "spr_counterfactuals", "spr_feedback", "spr_administrative_events",
    "spr_session_seals", "spr_snapshots", "spr_integrity_events",
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


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


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def parse_epoch(value: Any) -> int:
    return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())


def pip_size(instrument: str) -> float:
    return 0.01 if str(instrument).endswith("_JPY") else 0.0001


def liquidity_bucket(spread: float) -> str:
    if spread <= 2.0:
        return "liquid"
    if spread <= 3.0:
        return "normal"
    if spread <= 5.0:
        return "elevated"
    return "wide"


def session_bucket(epoch: int) -> str:
    hour = (int(epoch) // 3600) % 24
    if hour < 7:
        return "asia"
    if hour < 12:
        return "london"
    if hour < 16:
        return "london_new_york_overlap"
    if hour < 21:
        return "new_york"
    return "rollover"


class Market:
    def __init__(self, instrument: str, source_sha256: str, rows: list[dict[str, float | int]]) -> None:
        self.instrument = instrument
        self.source_sha256 = source_sha256
        self.pip = pip_size(instrument)
        self.rows = rows
        self.indices = {int(row["epoch"]): index for index, row in enumerate(rows)}

    def row(self, epoch: int) -> dict[str, float | int] | None:
        index = self.indices.get(int(epoch))
        return self.rows[index] if index is not None else None

    def index(self, epoch: int) -> int | None:
        return self.indices.get(int(epoch))


def load_market(path: Path, manifest: Mapping[str, Any], maximum_bytes: int) -> Market:
    chunks: list[bytes] = []
    total = 0
    with gzip.open(path, "rb") as handle:
        while True:
            chunk = handle.read(min(1024 * 1024, maximum_bytes + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_bytes:
                raise ValueError("oversized source archive")
            chunks.append(chunk)
    raw = b"".join(chunks)
    if sha256(raw).hexdigest() != str(manifest["sha256"]):
        raise ValueError(f"source hash mismatch: {manifest['instrument']}")
    fields = [
        "bid_open", "bid_high", "bid_low", "bid_close",
        "ask_open", "ask_high", "ask_low", "ask_close",
    ]
    rows: list[dict[str, float | int]] = []
    with io.StringIO(raw.decode("utf-8"), newline="") as text:
        reader = csv.DictReader(text)
        for number, row in enumerate(reader, start=2):
            epoch = parse_epoch(row["time"])
            if (
                parse_epoch(row["datetime"]) != epoch
                or row["instrument"] != manifest["instrument"]
                or row["granularity"] != "M1"
            ):
                raise ValueError(f"source identity error at row {number}")
            values = {field: float(row[field]) for field in fields}
            if any(not math.isfinite(value) or value <= 0 for value in values.values()):
                raise ValueError(f"nonfinite source row {number}")
            if any(values[f"ask_{part}"] <= values[f"bid_{part}"] for part in ("open", "high", "low", "close")):
                raise ValueError(f"crossed source row {number}")
            rows.append({"epoch": epoch, **values})
    rows.sort(key=lambda row: int(row["epoch"]))
    if len({int(row["epoch"]) for row in rows}) != len(rows):
        raise ValueError("duplicate source epoch")
    return Market(str(manifest["instrument"]), str(manifest["sha256"]), rows)


def contiguous(market: Market, first: int, last: int) -> bool:
    return first >= 0 and last < len(market.rows) and last >= first and (
        int(market.rows[last]["epoch"]) - int(market.rows[first]["epoch"])
        == (last - first) * 60
    )


def source_slice(market: Market, decision_epoch: int, lookback: int) -> dict[str, Any]:
    last = market.index(decision_epoch - 60)
    if last is None:
        raise ValueError("missing causal row")
    first = last - int(lookback)
    if not contiguous(market, first, last):
        raise ValueError("incomplete causal slice")
    rows = []
    for index in range(first, last + 1):
        row = market.rows[index]
        rows.append([
            int(row["epoch"]), float(row["bid_open"]), float(row["bid_high"]),
            float(row["bid_low"]), float(row["bid_close"]), float(row["ask_open"]),
            float(row["ask_high"]), float(row["ask_low"]), float(row["ask_close"]),
        ])
    return {
        "instrument": market.instrument,
        "first_epoch": rows[0][0],
        "last_epoch": rows[-1][0],
        "row_count": len(rows),
        "source_sha256": market.source_sha256,
        "slice_sha256": stable_hash(rows),
        "last_bid_close": rows[-1][4],
        "last_ask_close": rows[-1][8],
        "spread_pips": (rows[-1][8] - rows[-1][4]) / market.pip,
        "quote_age_seconds": decision_epoch - rows[-1][0] - 60,
    }


def causal_clocks(markets: Mapping[str, Market], config: Mapping[str, Any]) -> list[int]:
    start = parse_epoch(config["session"]["start_utc"])
    end = parse_epoch(config["session"]["end_utc_exclusive"])
    cadence = int(config["session"]["decision_cadence_min"]) * 60
    first = ((start + cadence - 1) // cadence) * cadence
    output: list[int] = []
    for epoch in range(first, end, cadence):
        valid = True
        for market in markets.values():
            last = market.index(epoch - 60)
            if last is None or not contiguous(market, last - int(config["session"]["feature_lookback_min"]), last):
                valid = False
                break
        if valid:
            output.append(epoch)
    return output


def candidates(markets: Mapping[str, Market], epoch: int, config: Mapping[str, Any]) -> list[dict[str, Any]]:
    policy = config["frozen_policy"]
    output = []
    for instrument in sorted(markets):
        market = markets[instrument]
        index = market.index(epoch - 60)
        if index is None:
            continue
        row = market.rows[index]
        current = (float(row["bid_close"]) + float(row["ask_close"])) / 2.0
        spread = (float(row["ask_close"]) - float(row["bid_close"])) / market.pip
        if spread <= 0 or spread > float(config["source"]["maximum_entry_spread_pips"]):
            continue
        moves: list[float] = []
        slopes: list[float] = []
        valid = True
        for lookback in policy["lookbacks_min"]:
            prior = index - int(lookback)
            if not contiguous(market, prior, index):
                valid = False
                break
            prior_row = market.rows[prior]
            prior_mid = (float(prior_row["bid_close"]) + float(prior_row["ask_close"])) / 2.0
            move = (current - prior_mid) / market.pip
            moves.append(move)
            slopes.append(move / math.sqrt(float(lookback)))
        if not valid:
            continue
        weighted = sum(float(weight) * slope for weight, slope in zip(policy["slope_weights"], slopes))
        if abs(weighted) <= 1e-12:
            continue
        side = 1 if weighted > 0 else -1
        aligned = sum(1 for move in moves if (move > 0) == (side > 0))
        if aligned < int(policy["minimum_aligned_windows"]):
            continue
        score = abs(weighted) / max(0.01, spread + float(config["costs"]["round_trip_slippage_pips"]))
        if score < float(policy["minimum_score_cost_ratio"]):
            continue
        confidence = min(
            float(policy["confidence_ceiling"]),
            max(float(policy["confidence_floor"]), float(policy["confidence_floor"]) + (score - 1.0) * float(policy["confidence_scale"])),
        )
        expected = abs(weighted) * math.sqrt(5.0)
        snapshot = {
            "instrument": instrument,
            "decision_epoch": epoch,
            "source_sha256": market.source_sha256,
            "side": side,
            "score": round(score, 12),
            "moves_pips": [round(value, 12) for value in moves],
            "spread_pips": round(spread, 12),
        }
        output.append({
            "instrument": instrument, "side": side, "score": score,
            "confidence": confidence, "expected_move_pips": expected,
            "spread_pips": spread, "liquidity_bucket": liquidity_bucket(spread),
            "aligned_windows": aligned, "moves_pips": moves,
            "snapshot_id": "candidate_" + stable_hash(snapshot)[:28],
        })
    return sorted(output, key=lambda row: (-float(row["score"]), str(row["instrument"]), -int(row["side"])))


def decision_from_candidate(candidate: Mapping[str, Any], action: str, config: Mapping[str, Any], rationale: str, label: str = "primary") -> dict[str, Any]:
    return {
        "action": action,
        "instrument": candidate["instrument"],
        "side": candidate["side"],
        "units": int(config["session"]["normalized_units"]),
        "confidence": candidate["confidence"],
        "expected_move_pips": candidate["expected_move_pips"],
        "horizon_min": int(config["session"]["feedback_horizon_min"]),
        "entry_condition": f"frozen_rank_score>={config['frozen_policy']['minimum_score_cost_ratio']}",
        "invalidation": "frozen_rank_reversal_or_maximum_holding",
        "rationale": rationale,
        "candidate_snapshot_id": candidate["snapshot_id"],
        "branch_label": label,
    }


def blank_decision(action: str, rationale: str, instrument: str | None = None, label: str = "primary") -> dict[str, Any]:
    return {
        "action": action, "instrument": instrument, "side": None, "units": 0,
        "confidence": None, "expected_move_pips": None, "horizon_min": None,
        "entry_condition": "", "invalidation": "", "rationale": rationale,
        "candidate_snapshot_id": None, "branch_label": label,
    }


def choose(state: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], epoch: int, config: Mapping[str, Any], allowed: bool) -> dict[str, Any]:
    best = rows[0] if rows else None
    position = state.get("position")
    if position is None:
        if best is None or not allowed:
            return blank_decision("wait", "no frozen-policy candidate")
        return decision_from_candidate(best, "enter", config, "highest frozen cost-adjusted candidate")
    held = (epoch - int(position["entry_epoch"])) // 60
    if held >= int(config["frozen_policy"]["maximum_holding_min"]):
        return blank_decision("exit", "predeclared maximum holding time", str(position["instrument"]))
    current = next((row for row in rows if row["instrument"] == position["instrument"] and row["side"] == position["side"]), None)
    if best is None:
        return blank_decision("exit", "no continuing frozen-policy candidate", str(position["instrument"]))
    if current is None:
        if not allowed:
            return blank_decision("exit", "new position cannot mature before administrative boundary", str(position["instrument"]))
        return decision_from_candidate(best, "rotate", config, "current thesis invalidated; rotate to best candidate")
    if best["instrument"] == current["instrument"] and best["side"] == current["side"]:
        return {
            "action": "hold", "instrument": None, "side": None, "units": 0,
            "confidence": current["confidence"], "expected_move_pips": current["expected_move_pips"],
            "horizon_min": int(config["session"]["feedback_horizon_min"]), "entry_condition": "",
            "invalidation": "frozen_rank_reversal_or_maximum_holding",
            "rationale": "current position remains highest-ranked",
            "candidate_snapshot_id": current["snapshot_id"], "branch_label": "primary",
        }
    improvement = float(best["score"]) / max(1e-12, float(current["score"]))
    if improvement >= float(config["frozen_policy"]["rotation_improvement_multiple"]):
        if not allowed:
            return {
                "action": "hold", "instrument": None, "side": None, "units": 0,
                "confidence": current["confidence"], "expected_move_pips": current["expected_move_pips"],
                "horizon_min": int(config["session"]["feedback_horizon_min"]), "entry_condition": "",
                "invalidation": "administrative_session_boundary",
                "rationale": "rotation cannot mature before administrative boundary",
                "candidate_snapshot_id": current["snapshot_id"], "branch_label": "primary",
            }
        return decision_from_candidate(best, "rotate", config, "alternative clears frozen rotation-improvement multiple")
    return {
        "action": "hold", "instrument": None, "side": None, "units": 0,
        "confidence": current["confidence"], "expected_move_pips": current["expected_move_pips"],
        "horizon_min": int(config["session"]["feedback_horizon_min"]), "entry_condition": "",
        "invalidation": "frozen_rank_reversal_or_maximum_holding",
        "rationale": "alternative does not clear rotation cost hurdle",
        "candidate_snapshot_id": current["snapshot_id"], "branch_label": "primary",
    }


def quote(market: Market, epoch: int) -> tuple[float, float, float]:
    row = market.row(epoch)
    if row is None:
        raise ValueError("missing exact executable quote")
    bid, ask = float(row["bid_open"]), float(row["ask_open"])
    spread = (ask - bid) / market.pip
    if spread <= 0:
        raise ValueError("crossed executable quote")
    return bid, ask, spread


def close_position(state: Mapping[str, Any], markets: Mapping[str, Market], epoch: int, slip: float) -> tuple[dict[str, Any], dict[str, Any]]:
    position = state["position"]
    market = markets[str(position["instrument"])]
    bid, ask, spread = quote(market, epoch)
    if int(position["side"]) > 0:
        raw, executed = bid, bid - slip * market.pip
    else:
        raw, executed = ask, ask + slip * market.pip
    realized = int(position["side"]) * (executed - float(position["entry_price"])) / market.pip * int(position["units"])
    leg = {
        "leg_kind": "close", "instrument": position["instrument"], "side": position["side"],
        "units": position["units"], "execution_epoch": epoch, "raw_price": raw,
        "executed_price": executed, "spread_pips": spread, "slippage_pips": slip,
        "realized_pips": realized, "thesis_id": position["thesis_id"],
    }
    return {"realized_pips": float(state["realized_pips"]) + realized, "position": None}, leg


def open_position(state: Mapping[str, Any], decision: Mapping[str, Any], markets: Mapping[str, Market], epoch: int, slip: float, clock_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    market = markets[str(decision["instrument"])]
    bid, ask, spread = quote(market, epoch)
    if int(decision["side"]) > 0:
        raw, executed = ask, ask + slip * market.pip
    else:
        raw, executed = bid, bid - slip * market.pip
    thesis = "thesis_" + stable_hash("aligned_multiscale_momentum_rank_v1", decision["instrument"], decision["side"], clock_id)[:28]
    position = {
        "instrument": decision["instrument"], "side": decision["side"], "units": decision["units"],
        "entry_epoch": epoch, "entry_price": executed, "entry_raw_price": raw,
        "entry_spread_pips": spread, "entry_slippage_pips": slip,
        "entry_clock_id": clock_id, "thesis_id": thesis,
    }
    leg = {
        "leg_kind": "open", "instrument": decision["instrument"], "side": decision["side"],
        "units": decision["units"], "execution_epoch": epoch, "raw_price": raw,
        "executed_price": executed, "spread_pips": spread, "slippage_pips": slip,
        "realized_pips": 0.0, "thesis_id": thesis,
    }
    return {"realized_pips": float(state["realized_pips"]), "position": position}, leg


def apply_action(state: Mapping[str, Any], decision: Mapping[str, Any], markets: Mapping[str, Market], epoch: int, config: Mapping[str, Any], clock_id: str) -> dict[str, Any]:
    action = str(decision["action"])
    slip = float(config["costs"]["slippage_per_execution_leg_pips"])
    if action in {"wait", "hold"}:
        return {"state": json.loads(canonical_json(state)), "legs": [], "realized_delta_pips": 0.0, "status": "applied", "rejection_reason": ""}
    if action in {"enter", "rotate"}:
        _, _, spread = quote(markets[str(decision["instrument"])], epoch)
        if spread > float(config["source"]["maximum_entry_spread_pips"]):
            return {"state": json.loads(canonical_json(state)), "legs": [], "realized_delta_pips": 0.0, "status": "rejected", "rejection_reason": "execution_spread_above_limit"}
    if action == "exit":
        new_state, leg = close_position(state, markets, epoch, slip)
        return {"state": new_state, "legs": [leg], "realized_delta_pips": leg["realized_pips"], "status": "applied", "rejection_reason": ""}
    if action == "enter":
        new_state, leg = open_position(state, decision, markets, epoch, slip, clock_id)
        return {"state": new_state, "legs": [leg], "realized_delta_pips": 0.0, "status": "applied", "rejection_reason": ""}
    closed, close_leg = close_position(state, markets, epoch, slip)
    opened, open_leg = open_position(closed, decision, markets, epoch, slip, clock_id)
    return {"state": opened, "legs": [close_leg, open_leg], "realized_delta_pips": close_leg["realized_pips"], "status": "applied", "rejection_reason": ""}


def liquidation_equity(state: Mapping[str, Any], markets: Mapping[str, Market], epoch: int, config: Mapping[str, Any]) -> float:
    if state.get("position") is None:
        return float(state["realized_pips"])
    closed, _ = close_position(state, markets, epoch, float(config["costs"]["slippage_per_execution_leg_pips"]))
    return float(closed["realized_pips"])


def branches(state: Mapping[str, Any], primary: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], config: Mapping[str, Any]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    best = rows[0] if rows else None
    second = rows[1] if len(rows) > 1 else None
    if state.get("position") is None:
        output.append(blank_decision("wait", "depth-one no-trade branch", label="wait"))
        for label, candidate in (("enter_best", best), ("enter_second", second)):
            if candidate is not None:
                output.append(decision_from_candidate(candidate, "enter", config, f"depth-one {label} branch", label))
        if best is not None:
            flipped = dict(best)
            flipped["side"] = -int(best["side"])
            flipped["snapshot_id"] = "candidate_" + stable_hash(best["snapshot_id"], "flipped")[:28]
            output.append(decision_from_candidate(flipped, "enter", config, "depth-one flipped-best branch", "enter_flipped_best"))
    else:
        output.append(blank_decision("hold", "depth-one hold branch", label="hold"))
        output.append(blank_decision("exit", "depth-one exit branch", str(state["position"]["instrument"]), "exit"))
        if best is not None and (best["instrument"] != state["position"]["instrument"] or best["side"] != state["position"]["side"]):
            output.append(decision_from_candidate(best, "rotate", config, "depth-one rotate-best branch", "rotate_best"))
    primary_id = (primary["action"], primary["instrument"], primary["side"], primary["units"])
    unique: dict[str, dict[str, Any]] = {}
    for row in output:
        identity = (row["action"], row["instrument"], row["side"], row["units"])
        if identity != primary_id:
            unique.setdefault(canonical_json(identity), row)
    return [unique[key] for key in sorted(unique)]


def causal_snapshot(markets: Mapping[str, Market], rows: Sequence[Mapping[str, Any]], state: Mapping[str, Any], epoch: int, config: Mapping[str, Any]) -> dict[str, Any]:
    prices = {}
    for instrument in sorted(markets):
        row = markets[instrument].row(epoch - 60)
        prices[instrument] = {
            "bid_close": float(row["bid_close"]), "ask_close": float(row["ask_close"]),
            "source_sha256": markets[instrument].source_sha256,
        }
    availability = {
        "price": True,
        "news": config["source_snapshots"].get("news_snapshot_id") is not None,
        "rates": config["source_snapshots"].get("rates_snapshot_id") is not None,
        "levels": config["source_snapshots"].get("levels_snapshot_id") is not None,
        "positioning": config["source_snapshots"].get("positioning_snapshot_id") is not None,
    }
    price_id = "pricesnapshot_" + stable_hash(epoch - 60, prices)[:28]
    payload = {
        "decision_epoch": epoch, "knowledge_cutoff_epoch": epoch,
        "price_snapshot_id": price_id, "prices": prices,
        "candidates": list(rows), "portfolio": state, "availability": availability,
        "news_snapshot_id": config["source_snapshots"].get("news_snapshot_id"),
        "rates_snapshot_id": config["source_snapshots"].get("rates_snapshot_id"),
        "levels_snapshot_id": config["source_snapshots"].get("levels_snapshot_id"),
        "positioning_snapshot_id": config["source_snapshots"].get("positioning_snapshot_id"),
    }
    coarse = {
        "session": session_bucket(epoch), "candidate_count": len(rows),
        "candidate_directions": [f"{row['instrument']}:{row['side']}" for row in rows],
        "candidate_score_buckets": [round(float(row["score"]), 1) for row in rows],
        "liquidity_buckets": [row["liquidity_bucket"] for row in rows],
        "portfolio": "flat" if state.get("position") is None else f"{state['position']['instrument']}:{state['position']['side']}",
        "availability": availability,
    }
    payload["exact_situation_id"] = "situation_exact_" + stable_hash(payload)[:28]
    payload["coarse_situation_id"] = "situation_coarse_" + stable_hash(coarse)[:28]
    return payload


def self_hash_checks(connection: sqlite3.Connection, session_id: str, failures: list[str]) -> None:
    json_hash_pairs = {
        "spr_pair_contexts": ("causal_json", "causal_sha256"),
        "spr_clocks": ("causal_json", "causal_sha256"),
        "spr_decisions": ("decision_json", "decision_sha256"),
        "spr_execution_legs": ("leg_json", "leg_sha256"),
        "spr_counterfactuals": ("action_json", "action_sha256"),
        "spr_feedback": ("components_json", "components_sha256"),
        "spr_administrative_events": ("event_json", "event_sha256"),
    }
    for table, (json_column, hash_column) in json_hash_pairs.items():
        for row in connection.execute(f"SELECT {json_column},{hash_column} FROM {table} WHERE session_id=?", (session_id,)):
            if stable_hash(json.loads(row[0])) != str(row[1]):
                failures.append(f"{table}_payload_hash")


def table_root(connection: sqlite3.Connection, table: str, session_id: str) -> dict[str, Any]:
    rows = sorted(str(row[0]) for row in connection.execute(f"SELECT row_sha256 FROM {table} WHERE session_id=?", (session_id,)))
    return {"count": len(rows), "set_sha256": stable_hash(rows)}


def sqlite_snapshot(path: Path) -> sqlite3.Connection:
    source = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=30.0)
    memory = sqlite3.connect(":memory:")
    source.backup(memory)
    source.close()
    memory.row_factory = sqlite3.Row
    memory.execute("PRAGMA foreign_keys=ON")
    return memory


def verifier_imports_forbidden() -> list[str]:
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    forbidden = []
    for node in ast.walk(tree):
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [str(node.module or "")]
        for name in names:
            if "oanda_sequential_portfolio_replay" in name or "sequential_portfolio_replay_v1" in name:
                forbidden.append(name)
    return sorted(set(forbidden))


def verify() -> dict[str, Any]:
    config = read_json(CONFIG)
    state_output = read_json(STATE)
    failures: list[str] = []
    safety = {
        "research_only": True, "execution_eligible": False, "can_promote": False,
        "can_place_orders": False, "can_authorize": False, "broker_access": False,
        "account_access": False, "signal_feed_write": False, "lifecycle_write": False,
        "supported_decision": "no_trade",
    }
    for key, expected in safety.items():
        if config.get(key) != expected or state_output.get(key, expected) != expected:
            failures.append(f"safety_{key}")
    forbidden_imports = verifier_imports_forbidden()
    if forbidden_imports:
        failures.append("verifier_import_isolation")
    database = Path(str(state_output["database"]))
    if not database.is_absolute():
        database = (ROOT / database).resolve()
    if ARTIFACT_ROOT.resolve() not in database.resolve().parents:
        raise ValueError("database escapes portfolio replay artifact root")
    connection = sqlite_snapshot(database)
    try:
        tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables != TABLES:
            failures.append("table_allowlist")
        if str(connection.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
            failures.append("sqlite_integrity")
        if list(connection.execute("PRAGMA foreign_key_check")):
            failures.append("foreign_keys")
        session_id = str(state_output["session_id"])
        cohort_id = str(state_output["cohort_id"])
        cohort = connection.execute("SELECT * FROM spr_cohorts WHERE cohort_id=?", (cohort_id,)).fetchone()
        session = connection.execute("SELECT * FROM spr_sessions WHERE session_id=?", (session_id,)).fetchone()
        if cohort is None or session is None:
            raise ValueError("missing current cohort/session")
        contract = json.loads(cohort["contract_json"])
        if stable_hash(contract) != str(cohort["contract_sha256"]):
            failures.append("cohort_contract_hash")
        if cohort_id != "sequential_portfolio_replay_v1." + stable_hash(contract)[:20]:
            failures.append("cohort_identity")
        current_files = {"config": CONFIG, "runner": PRODUCER, "core": CORE, "verifier": Path(__file__)}
        for label, path in current_files.items():
            frozen = contract["code"].get(label)
            if not frozen or file_sha256(path) != frozen["sha256"]:
                failures.append(f"current_{label}_hash")
            else:
                archive = ARTIFACT_ROOT / frozen["archive_relative_path"]
                if not archive.is_file() or file_sha256(archive) != frozen["sha256"]:
                    failures.append(f"archive_{label}_hash")
        source_root = (ROOT / config["source"]["artifact_relative_root"]).resolve()
        source_state_path = source_root / config["source"]["state_name"]
        source_verifier_path = source_root / config["source"]["verifier_name"]
        source_db_path = source_root / config["source"]["database_name"]
        parent_state = read_json(source_state_path)
        parent_verifier = read_json(source_verifier_path)
        if parent_verifier.get("verified") is not True or parent_verifier.get("failures"):
            failures.append("parent_verifier")
        for label, path in (
            ("source_state_sha256", source_state_path),
            ("source_verifier_sha256", source_verifier_path),
            ("source_database_sha256", source_db_path),
        ):
            if file_sha256(path) != contract["source"][label]:
                failures.append(label)
        manifests = {row["instrument"]: row for row in parent_state["source_manifest"]}
        markets = {
            instrument: load_market(
                source_root / manifests[instrument]["archive_relative_path"],
                manifests[instrument],
                int(config["source"]["maximum_archive_bytes_per_instrument"]),
            )
            for instrument in sorted(config["source"]["initial_instruments"])
        }
        epochs = causal_clocks(markets, config)
        if len(epochs) != int(config["session"]["expected_global_clock_count"]):
            failures.append("clock_count")
        instruments = sorted(markets)
        aliases = {instrument: chr(ord("A") + index) for index, instrument in enumerate(instruments)}
        universe_manifest = stable_hash([(instrument, markets[instrument].source_sha256) for instrument in instruments])
        replay_state: dict[str, Any] = {"realized_pips": 0.0, "position": None}
        for sequence_no, epoch in enumerate(epochs, start=1):
            slices = {instrument: source_slice(markets[instrument], epoch, int(config["session"]["feature_lookback_min"])) for instrument in instruments}
            clock_id = "sprclock_" + stable_hash(
                "oanda_practice_completed_m1_global_clock_v1", epoch,
                sorted((instrument, slices[instrument]["slice_sha256"]) for instrument in instruments),
                universe_manifest,
            )[:28]
            clock = connection.execute("SELECT * FROM spr_clocks WHERE session_id=? AND sequence_no=?", (session_id, sequence_no)).fetchone()
            if clock is None or str(clock["clock_id"]) != clock_id or int(clock["decision_epoch"]) != epoch:
                failures.append(f"clock_{sequence_no}")
                continue
            rows = candidates(markets, epoch, config)
            causal = causal_snapshot(markets, rows, replay_state, epoch, config)
            causal.update({
                "clock_id": clock_id, "pair_aliases": aliases,
                "pair_input_slice_ids": {instrument: slices[instrument]["slice_sha256"] for instrument in instruments},
                "universe_manifest_sha256": universe_manifest,
            })
            if json.loads(clock["causal_json"]) != json.loads(canonical_json(causal)):
                failures.append(f"causal_clock_{sequence_no}")
            for instrument in instruments:
                context = connection.execute(
                    "SELECT * FROM spr_pair_contexts WHERE session_id=? AND decision_epoch=? AND instrument=?",
                    (session_id, epoch, instrument),
                ).fetchone()
                candidate = next((row for row in rows if row["instrument"] == instrument), None)
                expected_context = dict(slices[instrument])
                expected_context.update({"pair_alias": aliases[instrument], "candidate": candidate, "candidate_state": "eligible" if candidate else "not_candidate"})
                if context is None or json.loads(context["causal_json"]) != json.loads(canonical_json(expected_context)):
                    failures.append(f"pair_context_{sequence_no}_{instrument}")
            allowed = epoch + int(config["session"]["execution_delay_min"]) * 60 + int(config["session"]["feedback_horizon_min"]) * 60 <= parse_epoch(config["session"]["end_utc_exclusive"])
            expected_decision = choose(replay_state, rows, epoch, config, allowed)
            decision_row = connection.execute("SELECT * FROM spr_decisions WHERE clock_id=?", (clock_id,)).fetchone()
            if decision_row is None or json.loads(decision_row["decision_json"]) != json.loads(canonical_json(expected_decision)):
                failures.append(f"decision_{sequence_no}")
                continue
            decision_id = str(decision_row["decision_id"])
            execution_epoch = epoch + int(config["session"]["execution_delay_min"]) * 60
            feedback_epoch = epoch + int(config["session"]["feedback_horizon_min"]) * 60
            applied = apply_action(replay_state, expected_decision, markets, execution_epoch, config, clock_id)
            outcome = connection.execute("SELECT * FROM spr_execution_outcomes WHERE decision_id=?", (decision_id,)).fetchone()
            if outcome is None or json.loads(outcome["state_after_json"]) != json.loads(canonical_json(applied["state"])) or outcome["status"] != applied["status"] or outcome["rejection_reason"] != applied["rejection_reason"] or abs(float(outcome["realized_delta_pips"]) - float(applied["realized_delta_pips"])) > 1e-9:
                failures.append(f"execution_outcome_{sequence_no}")
            stored_legs = list(connection.execute("SELECT * FROM spr_execution_legs WHERE parent_kind='primary_decision' AND parent_id=? ORDER BY leg_sequence", (decision_id,)))
            if len(stored_legs) != len(applied["legs"]):
                failures.append(f"execution_leg_count_{sequence_no}")
            else:
                for stored, expected_leg in zip(stored_legs, applied["legs"]):
                    if json.loads(stored["leg_json"]) != json.loads(canonical_json(expected_leg)):
                        failures.append(f"execution_leg_{sequence_no}")
            expected_branches = branches(replay_state, expected_decision, rows, config)
            stored_branches = list(connection.execute("SELECT * FROM spr_counterfactuals WHERE decision_id=? ORDER BY branch_label", (decision_id,)))
            if len(stored_branches) != len(expected_branches):
                failures.append(f"counterfactual_count_{sequence_no}")
            alternative_equities: dict[str, float] = {}
            for branch in expected_branches:
                branch_result = apply_action(replay_state, branch, markets, execution_epoch, config, clock_id)
                equity = liquidation_equity(branch_result["state"], markets, feedback_epoch, config)
                alternative_equities[str(branch["branch_label"])] = equity
                stored = next((row for row in stored_branches if row["branch_label"] == branch["branch_label"]), None)
                if stored is None or json.loads(stored["action_json"]) != json.loads(canonical_json(branch)) or abs(float(stored["terminal_equity_pips"]) - equity) > 1e-9 or int(stored["counts_as_rep"]) != 0:
                    failures.append(f"counterfactual_{sequence_no}_{branch['branch_label']}")
            primary_equity = liquidation_equity(applied["state"], markets, feedback_epoch, config)
            feedback = connection.execute("SELECT * FROM spr_feedback WHERE decision_id=?", (decision_id,)).fetchone()
            if feedback is None or abs(float(feedback["primary_terminal_equity_pips"]) - primary_equity) > 1e-9 or int(feedback["revealed_epoch"]) != feedback_epoch:
                failures.append(f"feedback_{sequence_no}")
            replay_state = applied["state"]
        admin_rows = list(connection.execute("SELECT * FROM spr_administrative_events WHERE session_id=?", (session_id,)))
        end_epoch = parse_epoch(config["session"]["end_utc_exclusive"])
        if replay_state.get("position") is not None:
            admin_decision = blank_decision("exit", "predeclared administrative session close", str(replay_state["position"]["instrument"]))
            closed = apply_action(replay_state, admin_decision, markets, end_epoch, config, "admin")
            if len(admin_rows) != 1:
                failures.append("administrative_event_count")
            replay_state = closed["state"]
        elif admin_rows:
            failures.append("unexpected_administrative_event")
        if replay_state.get("position") is not None:
            failures.append("terminal_not_flat")
        self_hash_checks(connection, session_id, failures)
        roots = {
            name: table_root(connection, table, session_id)
            for name, table in {
                "pair_contexts": "spr_pair_contexts", "clocks": "spr_clocks",
                "decisions": "spr_decisions", "execution_outcomes": "spr_execution_outcomes",
                "execution_legs": "spr_execution_legs", "counterfactuals": "spr_counterfactuals",
                "feedback": "spr_feedback", "administrative_events": "spr_administrative_events",
            }.items()
        }
        if roots != state_output.get("roots"):
            failures.append("state_roots")
        seal = connection.execute("SELECT * FROM spr_session_seals WHERE seal_id=?", (state_output["session_seal_id"],)).fetchone()
        if seal is None or stable_hash(json.loads(seal["seal_json"])) != seal["seal_sha256"]:
            failures.append("session_seal")
        elif json.loads(seal["seal_json"])["roots"] != roots:
            failures.append("session_seal_roots")
        if abs(float(state_output["terminal_realized_pips"]) - float(replay_state["realized_pips"])) > 1e-9:
            failures.append("terminal_realized_pips")
        if int(state_output["global_clock_count"]) != len(epochs):
            failures.append("state_clock_count")
        if int(state_output["pair_context_count"]) != len(epochs) * len(instruments):
            failures.append("state_pair_context_count")
        append_only: dict[str, Any] = {}
        for table in sorted(TABLES):
            triggers = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (table,))}
            expected = {f"{table}_no_update", f"{table}_no_delete"}
            append_only[table] = sorted(triggers)
            if not expected.issubset(triggers):
                failures.append(f"append_only_{table}")
    finally:
        connection.close()
    result = {
        "schema_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "verified": not failures,
        "failures": sorted(set(failures)),
        "cohort_id": state_output.get("cohort_id"),
        "session_id": state_output.get("session_id"),
        "research_only": True,
        "execution_eligible": False,
        "can_promote": False,
        "can_place_orders": False,
        "supported_decision": "no_trade",
        "checks": {
            "verifier_imports_forbidden": forbidden_imports,
            "sqlite_snapshot_includes_committed_wal": True,
            "global_clock_count": state_output.get("global_clock_count"),
            "pair_context_count": state_output.get("pair_context_count"),
            "roots": state_output.get("roots"),
            "terminal_flat": state_output.get("terminal_flat"),
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
