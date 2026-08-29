#!/usr/bin/env python3
"""Independently verify the counterfactual SIM-gym structural evidence boundary.

This verifier intentionally does not import the producer.  It recalculates
content identities, checks source/config/code bytes, confirms append-only
triggers, and verifies aggregate/snapshot hashes from the standalone ledger.
It cannot promote or execute an arm.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
ARTIFACT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "research_ledgers"
    / "counterfactual_sim_gym_v1"
)
DEFAULT_DATABASE = (
    ARTIFACT_ROOT / "counterfactual_sim_gym_v1.sqlite"
)
DEFAULT_OUTPUT = ARTIFACT_ROOT / "counterfactual_sim_gym_verifier_v1.json"
CONTRACT_ID = "counterfactual_sim_gym_v1_20260829"
REQUIRED_APPEND_ONLY_TABLES = (
    "sim_cohorts",
    "sim_cohort_transitions",
    "sim_arms",
    "sim_decision_clocks",
    "sim_signal_observations",
    "sim_outcome_parts",
    "sim_intent_map",
    "sim_cell_aggregates",
    "sim_mistake_samples",
    "sim_run_snapshots",
    "sim_integrity_events",
)
FORBIDDEN_TABLE_TOKENS = (
    "account",
    "authorization",
    "broker_order",
    "lifecycle",
    "practice_trade",
    "signal_feed",
)
MAX_FROZEN_SOURCE_BYTES = 512 * 1024 * 1024
MAX_COMPRESSED_SOURCE_BYTES = 512 * 1024 * 1024
SOURCE_READ_CHUNK_BYTES = 1024 * 1024
SIGNAL_ZERO_TOLERANCE_PIPS = 1e-9
INDEPENDENT_PIP_0_01_INSTRUMENTS = frozenset(
    {
        "AUD_JPY",
        "CAD_JPY",
        "CHF_JPY",
        "EUR_HUF",
        "EUR_JPY",
        "GBP_JPY",
        "NZD_JPY",
        "SGD_JPY",
        "TRY_JPY",
        "USD_HUF",
        "USD_JPY",
        "USD_THB",
        "ZAR_JPY",
    }
)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )


def stable_hash(*parts: Any) -> str:
    payload = "\x1f".join(canonical_json(part) for part in parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def file_prefix_sha256(
    path: Path, byte_count: int, chunk_size: int = 8 * 1024 * 1024
) -> str:
    remaining = int(byte_count)
    if remaining < 0:
        raise ValueError("negative prefix byte count")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while remaining:
            chunk = source.read(min(chunk_size, remaining))
            if not chunk:
                raise ValueError(f"file shorter than frozen prefix: {path}")
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def _is_link_or_junction(path: Path) -> bool:
    return path.is_symlink() or bool(getattr(path, "is_junction", lambda: False)())


def contained_archive_path(relative: Any) -> Path:
    """Resolve one nonempty archive path without following workspace links."""

    text = str(relative or "").strip()
    candidate = Path(text)
    if (
        not text
        or candidate.is_absolute()
        or bool(candidate.drive)
        or not candidate.parts
        or any(part in {"", ".", ".."} for part in candidate.parts)
    ):
        raise ValueError("archive path must be nonempty, relative, and traversal-free")
    if _is_link_or_junction(ARTIFACT_ROOT):
        raise ValueError("artifact root is a link/junction")
    root = ARTIFACT_ROOT.resolve()
    current = ARTIFACT_ROOT
    for part in candidate.parts:
        current = current / part
        if _is_link_or_junction(current):
            raise ValueError(f"archive path traverses a link/junction: {current}")
    resolved = (ARTIFACT_ROOT / candidate).resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("archive path escapes dedicated SIM root")
    return resolved


def bounded_gzip_payload(path: Path, expected_bytes: int) -> tuple[bytes, str]:
    """Read exactly one frozen gzip member with explicit compressed/raw bounds."""

    expected = int(expected_bytes)
    if expected <= 0 or expected > MAX_FROZEN_SOURCE_BYTES:
        raise ValueError(f"invalid frozen source byte count: {expected}")
    compressed_size = int(path.stat().st_size)
    if compressed_size <= 0 or compressed_size > MAX_COMPRESSED_SOURCE_BYTES:
        raise ValueError(f"invalid compressed source byte count: {compressed_size}")
    compressed_sha = file_sha256(path)
    payload = bytearray()
    with gzip.open(path, "rb") as source:
        remaining = expected
        while remaining:
            chunk = source.read(min(SOURCE_READ_CHUNK_BYTES, remaining))
            if not chunk:
                raise ValueError("gzip source is shorter than its frozen prefix")
            payload.extend(chunk)
            remaining -= len(chunk)
        if source.read(1):
            raise ValueError("gzip source is longer than its frozen prefix")
    return bytes(payload), compressed_sha


def independent_parse_epoch(value: Any) -> int:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("naive candle timestamp")
    return int(parsed.astimezone(timezone.utc).timestamp())


def independent_market_from_csv(
    payload: bytes, source: Mapping[str, Any]
) -> dict[str, Any]:
    required = (
        "time",
        "datetime",
        "instrument",
        "granularity",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
    )
    instrument = str(source.get("instrument") or "")
    pip = float(source.get("pip") or 0.0)
    if not instrument or not math.isfinite(pip) or pip <= 0.0:
        raise ValueError("source manifest requires a positive pip and instrument")
    expected_pip = 0.01 if instrument in INDEPENDENT_PIP_0_01_INSTRUMENTS else 0.0001
    if not math.isclose(pip, expected_pip, rel_tol=0.0, abs_tol=1e-15):
        raise ValueError(
            f"source manifest pip disagrees with standalone instrument map: "
            f"{instrument}:{pip}!={expected_pip}"
        )
    rows_by_epoch: dict[int, dict[str, float]] = {}
    with io.StringIO(payload.decode("utf-8"), newline="") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(set(required) - set(reader.fieldnames or ()))
        if missing:
            raise ValueError(f"archived source missing columns: {missing}")
        for row_number, raw in enumerate(reader, start=2):
            time_epoch = independent_parse_epoch(raw.get("time"))
            if time_epoch != independent_parse_epoch(raw.get("datetime")):
                raise ValueError(f"archived source clock mismatch at row {row_number}")
            if time_epoch % 60:
                raise ValueError(f"archived source has non-M1 clock at row {row_number}")
            if str(raw.get("instrument") or "") != instrument:
                raise ValueError(f"archived source instrument mismatch at row {row_number}")
            if str(raw.get("granularity") or "") != "M1":
                raise ValueError(f"archived source granularity mismatch at row {row_number}")
            if time_epoch in rows_by_epoch:
                raise ValueError(f"archived source duplicate clock at row {row_number}")
            quote = {name: float(raw[name]) for name in required[4:]}
            if any(not math.isfinite(value) or value <= 0.0 for value in quote.values()):
                raise ValueError(f"archived source invalid quote at row {row_number}")
            for prefix in ("bid", "ask"):
                open_value = quote[f"{prefix}_open"]
                high = quote[f"{prefix}_high"]
                low = quote[f"{prefix}_low"]
                close = quote[f"{prefix}_close"]
                if high < max(open_value, close) or low > min(open_value, close):
                    raise ValueError(f"archived source invalid OHLC at row {row_number}")
            if any(quote[f"ask_{kind}"] <= quote[f"bid_{kind}"] for kind in ("open", "high", "low", "close")):
                raise ValueError(f"archived source crossed quote at row {row_number}")
            quote["mid_close"] = (quote["bid_close"] + quote["ask_close"]) / 2.0
            rows_by_epoch[time_epoch] = quote
    epochs = sorted(rows_by_epoch)
    if not epochs:
        raise ValueError("empty archived source")
    if len(epochs) != int(source.get("row_count") or -1):
        raise ValueError("archived source row count disagrees with manifest")
    if independent_parse_epoch(source.get("first_candle_utc")) != epochs[0]:
        raise ValueError("archived source first clock disagrees with manifest")
    if independent_parse_epoch(source.get("last_candle_utc")) != epochs[-1]:
        raise ValueError("archived source last clock disagrees with manifest")
    return {
        "instrument": instrument,
        "pip": pip,
        "epochs": epochs,
        "index_by_epoch": {epoch: index for index, epoch in enumerate(epochs)},
        "rows_by_epoch": rows_by_epoch,
    }


def _market_contiguous(market: Mapping[str, Any], start: int, end: int) -> bool:
    epochs = market["epochs"]
    if start < 0 or end < start or end >= len(epochs):
        return False
    return all(int(epochs[index]) - int(epochs[index - 1]) == 60 for index in range(start + 1, end + 1))


def independent_signal_observation(
    market: Mapping[str, Any], source_epoch: int, rule: Mapping[str, Any], slippage: float
) -> dict[str, Any] | None:
    index = market["index_by_epoch"].get(int(source_epoch))
    if index is None or index < 1:
        return None
    epochs = market["epochs"]
    rows = market["rows_by_epoch"]
    pip = float(market["pip"])
    current = float(rows[epochs[index]]["mid_close"])
    close_quote = rows[epochs[index]]
    spread = (float(close_quote["ask_close"]) - float(close_quote["bid_close"])) / pip
    cost = max(0.01, spread + max(0.0, float(slippage)))
    kind = str(rule.get("kind") or "")
    rule_id = str(rule.get("id") or kind)
    metadata: dict[str, Any] = {"kind": kind, "decision_candle_epoch": int(source_epoch)}
    if kind in {"momentum", "reversion"}:
        lookback = int(rule.get("lookback_min") or 0)
        prior_index = index - lookback
        if lookback <= 0 or not _market_contiguous(market, prior_index, index):
            return None
        move = (current - float(rows[epochs[prior_index]]["mid_close"])) / pip
        side = (
            0
            if abs(move) <= SIGNAL_ZERO_TOLERANCE_PIPS
            else 1 if move > 0.0 else -1
        )
        if kind == "reversion":
            side *= -1
        score = abs(move) / cost
        if side == 0 or score < float(rule.get("minimum_move_cost_ratio") or 0.0):
            return None
        metadata.update({"lookback_min": lookback, "trailing_move_pips": move})
        return {"rule_id": rule_id, "raw_side": side, "score": score, "feature_value_pips": move, "metadata": metadata}
    if kind == "moving_average_spread":
        fast = int(rule.get("fast_min") or 0)
        slow = int(rule.get("slow_min") or 0)
        if fast <= 0 or slow <= fast or not _market_contiguous(market, index - slow + 1, index):
            return None
        fast_values = [float(rows[epochs[position]]["mid_close"]) for position in range(index - fast + 1, index + 1)]
        slow_values = [float(rows[epochs[position]]["mid_close"]) for position in range(index - slow + 1, index + 1)]
        difference = (sum(fast_values) / fast - sum(slow_values) / slow) / pip
        side = (
            0
            if abs(difference) <= SIGNAL_ZERO_TOLERANCE_PIPS
            else 1 if difference > 0.0 else -1
        )
        score = abs(difference) / cost
        if side == 0 or score < float(rule.get("minimum_move_cost_ratio") or 0.0):
            return None
        metadata.update({"fast_min": fast, "slow_min": slow, "ma_difference_pips": difference})
        return {"rule_id": rule_id, "raw_side": side, "score": score, "feature_value_pips": difference, "metadata": metadata}
    if kind == "level_reaction":
        window = int(rule.get("window_min") or 0)
        approach = int(rule.get("approach_min") or 0)
        start = index - window
        approach_index = index - approach
        if window <= 1 or approach <= 0 or not _market_contiguous(market, start, index):
            return None
        history = [float(rows[epochs[position]]["mid_close"]) for position in range(start, index)]
        support, resistance = min(history), max(history)
        approach_move = (current - float(rows[epochs[approach_index]]["mid_close"])) / pip
        support_distance = abs(current - support) / pip
        resistance_distance = abs(resistance - current) / pip
        maximum_distance = cost * float(rule.get("maximum_distance_cost_multiple") or 1.0)
        physical_side, level_kind, distance = 0, "", math.inf
        if approach_move < 0.0 and support_distance <= maximum_distance:
            physical_side, level_kind, distance = -1, "support", support_distance
        elif approach_move > 0.0 and resistance_distance <= maximum_distance:
            physical_side, level_kind, distance = 1, "resistance", resistance_distance
        if physical_side == 0:
            return None
        mode = str(rule.get("mode") or "bounce")
        side = -physical_side if mode == "bounce" else physical_side
        score = (abs(approach_move) + max(0.0, maximum_distance - distance)) / cost
        metadata.update(
            {
                "window_min": window,
                "approach_min": approach,
                "mode": mode,
                "level_kind": level_kind,
                "level_price": support if level_kind == "support" else resistance,
                "distance_to_level_pips": distance,
                "approach_move_pips": approach_move,
            }
        )
        return {"rule_id": rule_id, "raw_side": side, "score": score, "feature_value_pips": approach_move, "metadata": metadata}
    raise ValueError(f"unsupported independently replayed signal kind: {kind}")


def _independent_excluded_outcome(
    status: str,
    reason: str,
    *,
    entry_epoch: int | None = None,
    entry_bid: float | None = None,
    entry_ask: float | None = None,
    entry_spread_pips: float | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "exclusion_reason": reason,
        "entry_epoch": entry_epoch,
        "exit_epoch": None,
        "entry_bid": entry_bid,
        "entry_ask": entry_ask,
        "exit_bid": None,
        "exit_ask": None,
        "exit_price_kind": "unavailable",
        "observed_exit_bid": None,
        "observed_exit_ask": None,
        "entry_spread_pips": entry_spread_pips,
        "exit_spread_pips": None,
        "gross_mid_pips": None,
        "executable_net_pips": None,
        "terminal_endpoint_net_pips": None,
        "moderate_stress_net_pips": None,
        "severe_stress_net_pips": None,
        "mfe_pips": None,
        "mae_pips": None,
        "first_positive_sec": None,
        "hold_sec": None,
        "exit_reason": status,
        "missed_entry_slippage_pips": None,
        "mistake_clusters": [reason],
    }


def independent_endpoint_outcome(
    market: Mapping[str, Any],
    *,
    decision_epoch: int,
    side: int,
    entry_delay_min: int,
    horizon_min: int,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    costs = config.get("costs") or {}
    source_config = config.get("source") or {}
    mistake = config.get("mistake_clustering") or {}
    multipliers = [float(value) for value in costs.get("spread_stress_multipliers") or ()]
    if len(multipliers) < 3:
        raise ValueError("three spread stress multipliers required for replay")
    direction = 1 if int(side) > 0 else -1
    entry_epoch = int(decision_epoch) + max(0, int(entry_delay_min)) * 60
    index_by_epoch = market["index_by_epoch"]
    immediate_index = index_by_epoch.get(int(decision_epoch))
    entry_index = index_by_epoch.get(entry_epoch)
    if immediate_index is None or entry_index is None:
        return _independent_excluded_outcome("not_filled", "missing_exact_entry_quote")
    horizon = max(1, int(horizon_min))
    final_index = int(entry_index) + horizon - 1
    if not _market_contiguous(market, int(entry_index), final_index):
        return _independent_excluded_outcome("invalid", "incomplete_exact_m1_path", entry_epoch=entry_epoch)
    epochs = market["epochs"]
    rows = market["rows_by_epoch"]
    pip = float(market["pip"])
    entry = rows[epochs[entry_index]]
    immediate = rows[epochs[immediate_index]]
    entry_bid = float(entry["bid_open"])
    entry_ask = float(entry["ask_open"])
    entry_spread = (entry_ask - entry_bid) / pip
    if entry_spread <= 0.0:
        return _independent_excluded_outcome(
            "invalid", "crossed_or_zero_entry_quote", entry_epoch=entry_epoch,
            entry_bid=entry_bid, entry_ask=entry_ask, entry_spread_pips=entry_spread,
        )
    maximum_spread = float(source_config.get("maximum_entry_spread_pips") or 0.0)
    if entry_spread > maximum_spread:
        return _independent_excluded_outcome(
            "invalid", "entry_spread_above_limit", entry_epoch=entry_epoch,
            entry_bid=entry_bid, entry_ask=entry_ask, entry_spread_pips=entry_spread,
        )
    path = [rows[epochs[position]] for position in range(entry_index, final_index + 1)]
    if direction > 0:
        executable = [(float(row["bid_close"]) - entry_ask) / pip for row in path]
        favorable = [(float(row["bid_high"]) - entry_ask) / pip for row in path]
        adverse = [(float(row["bid_low"]) - entry_ask) / pip for row in path]
        missed = (entry_ask - float(immediate["ask_open"])) / pip
    else:
        executable = [(entry_bid - float(row["ask_close"])) / pip for row in path]
        favorable = [(entry_bid - float(row["ask_low"])) / pip for row in path]
        adverse = [(entry_bid - float(row["ask_high"])) / pip for row in path]
        missed = (float(immediate["bid_open"]) - entry_bid) / pip
    slippage = max(0.0, float(costs.get("round_trip_slippage_pips") or 0.0))
    executable = [value - slippage for value in executable]
    favorable = [value - slippage for value in favorable]
    adverse = [value - slippage for value in adverse]
    value = float(executable[-1])
    observed_exit = path[-1]
    exit_bid = float(observed_exit["bid_close"])
    exit_ask = float(observed_exit["ask_close"])
    exit_spread = (exit_ask - exit_bid) / pip
    entry_mid = (entry_bid + entry_ask) / 2.0
    exit_mid = (exit_bid + exit_ask) / 2.0
    gross_mid = direction * (exit_mid - entry_mid) / pip
    moderate = value - max(0.0, float(costs.get("moderate_stress_slippage_pips") or 0.0) - slippage)
    moderate -= max(0.0, multipliers[1] - 1.0) * (entry_spread + exit_spread) / 2.0
    severe = value - max(0.0, float(costs.get("severe_stress_slippage_pips") or 0.0) - slippage)
    severe -= max(0.0, multipliers[2] - 1.0) * (entry_spread + exit_spread) / 2.0
    first_positive = next((int((index + 1) * 60) for index, number in enumerate(executable) if number > 0.0), None)
    clusters: list[str] = []
    if gross_mid > 0.0 and value <= 0.0:
        clusters.append("cost_consumed_move")
    elif gross_mid <= 0.0:
        clusters.append("wrong_direction")
    elif value > 0.0:
        clusters.append("captured_after_cost")
    if missed > float(mistake.get("latency_decay_threshold_pips") or 1.0):
        clusters.append("latency_decay")
    if not clusters:
        clusters.append("uncategorized")
    return {
        "status": "filled",
        "exclusion_reason": "",
        "entry_epoch": entry_epoch,
        "exit_epoch": int(epochs[final_index]) + 60,
        "entry_bid": entry_bid,
        "entry_ask": entry_ask,
        "exit_bid": exit_bid,
        "exit_ask": exit_ask,
        "exit_price_kind": "observed_m1_close",
        "observed_exit_bid": exit_bid,
        "observed_exit_ask": exit_ask,
        "entry_spread_pips": entry_spread,
        "exit_spread_pips": exit_spread,
        "gross_mid_pips": gross_mid,
        "executable_net_pips": value,
        "terminal_endpoint_net_pips": value,
        "moderate_stress_net_pips": moderate,
        "severe_stress_net_pips": severe,
        "mfe_pips": max(favorable),
        "mae_pips": min(adverse),
        "first_positive_sec": first_positive,
        "hold_sec": horizon * 60,
        "exit_reason": "endpoint",
        "missed_entry_slippage_pips": missed,
        "mistake_clusters": sorted(set(clusters)),
    }


def independent_arm_definitions(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    grid = config.get("execution_grid") or {}
    rule_ids = [str(row.get("id") or "") for row in config.get("signal_rules") or ()]
    delays = [int(value) for value in grid.get("entry_delay_min") or ()]
    horizons = [int(value) for value in grid.get("horizon_min") or ()]
    gaps = [int(value) for value in grid.get("reentry_gap_min") or (0,)]
    policies = [dict(value) for value in grid.get("exit_policies") or ()]
    output: list[dict[str, Any]] = []

    def add(
        rule_id: str,
        comparator: str,
        delay: int,
        horizon: int,
        gap: int,
        policy: Mapping[str, Any],
    ) -> None:
        material = {
            "signal_rule_id": rule_id,
            "comparator": comparator,
            "entry_delay_min": int(delay),
            "horizon_min": int(horizon),
            "reentry_gap_min": int(gap),
            "exit_policy": dict(policy),
        }
        digest = stable_hash(CONTRACT_ID, material)
        output.append(
            {
                "arm_id": "simarm_" + digest[:28],
                **material,
                "definition_sha256": digest,
            }
        )

    for rule_id in rule_ids:
        for delay in delays:
            for horizon in horizons:
                for gap in gaps:
                    for policy in policies:
                        add(rule_id, "as_signaled", delay, horizon, gap, policy)
    comparator_config = config.get("comparators") or {}
    for comparator_name in ("flipped_direction", "deterministic_random_side"):
        comparator = comparator_config.get(comparator_name) or {}
        if not comparator.get("enabled"):
            continue
        for rule_id in rule_ids:
            for delay in delays:
                for horizon in horizons:
                    for gap in gaps:
                        for policy in policies:
                            add(
                                rule_id,
                                comparator_name,
                                delay,
                                horizon,
                                gap,
                                policy,
                            )
    if len({row["arm_id"] for row in output}) != len(output):
        raise ValueError("duplicate independently reconstructed arm definitions")
    return sorted(output, key=lambda row: str(row["arm_id"]))


def independent_partition_interval(
    interval_start: int,
    interval_end: int,
    first_epoch: int,
    last_epoch: int,
    config: Mapping[str, Any],
) -> str | None:
    partition = config.get("historical_partitions") or {}
    labels = [str(value) for value in partition.get("labels") or ()]
    fractions = [float(value) for value in partition.get("fractions") or ()]
    if len(labels) != len(fractions) or not labels or abs(sum(fractions) - 1.0) > 1e-9:
        raise ValueError("invalid historical partition contract")
    start, end = int(interval_start), int(interval_end)
    first, last = int(first_epoch), int(last_epoch)
    if start < first or end <= start or end > last:
        return None
    span = last - first
    boundaries: list[int] = []
    cumulative = 0.0
    for fraction in fractions[:-1]:
        cumulative += fraction
        boundaries.append(first + int(span * cumulative))
    embargo = max(0, int(partition.get("embargo_min") or 0)) * 60
    starts = [first, *(boundary + embargo for boundary in boundaries)]
    ends = [*boundaries, last]
    for label, block_start, block_end in zip(labels, starts, ends):
        if start >= block_start and end <= block_end:
            return label
    return None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    root = ARTIFACT_ROOT.resolve()
    if ARTIFACT_ROOT.exists() and (
        ARTIFACT_ROOT.is_symlink()
        or bool(getattr(ARTIFACT_ROOT, "is_junction", lambda: False)())
    ):
        raise ValueError("verifier artifact root is a link/junction")
    resolved = path.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("verifier receipt must remain in dedicated SIM root")
    if resolved.exists() and (
        resolved.is_symlink() or bool(getattr(resolved, "is_junction", lambda: False)())
    ):
        raise ValueError("verifier receipt target is a link/junction")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def independent_session_bucket(epoch: int) -> str:
    hour = datetime.fromtimestamp(int(epoch), timezone.utc).hour
    if 7 <= hour < 12:
        return "london"
    if 12 <= hour < 16:
        return "london_new_york_overlap"
    if 16 <= hour < 21:
        return "new_york"
    if 21 <= hour or hour < 7:
        return "asia_or_rollover"
    return "other"


def independent_liquidity_bucket(spread_pips: float) -> str:
    spread = float(spread_pips)
    if spread <= 2.0:
        return "liquid_le_2"
    if spread <= 3.0:
        return "normal_2_to_3"
    if spread <= 5.0:
        return "elevated_3_to_5"
    return "wide_gt_5"


def _independent_effective_values(rows: list[dict[str, Any]]) -> tuple[list[float], int]:
    by_start: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_start[int(row["start"])].append(row)
    next_available: dict[str, int] = {}
    values: list[float] = []
    purged = 0
    for start in sorted(by_start):
        same_clock = by_start[start]
        remaining = set(range(len(same_clock)))
        while remaining:
            seed = min(remaining)
            component = {seed}
            resources = set(same_clock[seed]["factors"])
            resources.add(f"PAIR:{same_clock[seed]['instrument']}")
            remaining.remove(seed)
            changed = True
            while changed:
                changed = False
                for candidate in sorted(remaining):
                    candidate_resources = set(same_clock[candidate]["factors"])
                    candidate_resources.add(f"PAIR:{same_clock[candidate]['instrument']}")
                    if not candidate_resources.isdisjoint(resources):
                        remaining.remove(candidate)
                        component.add(candidate)
                        resources.update(candidate_resources)
                        changed = True
            end = max(int(same_clock[index]["end"]) for index in component)
            if any(start < next_available.get(resource, -2**63) for resource in resources):
                purged += len(component)
                continue
            value = sum(float(same_clock[index]["value"]) for index in component) / len(component)
            values.append(value)
            for resource in resources:
                next_available[resource] = end
    return values, purged


def independent_cell_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(row["value"]) for row in rows]
    effective, overlap_purged = _independent_effective_values(rows)
    effective_n = len(effective)
    effective_average = sum(effective) / effective_n if effective_n else None
    if effective_n > 1:
        variance = max(
            0.0,
            (
                sum(value * value for value in effective)
                - sum(effective) ** 2 / effective_n
            )
            / (effective_n - 1),
        )
    else:
        variance = 0.0
    effective_lcb = (
        effective_average - 1.96 * math.sqrt(variance) / math.sqrt(effective_n)
        if effective_average is not None
        else None
    )
    profit = sum(max(0.0, value) for value in values)
    loss = sum(max(0.0, -value) for value in values)
    positive_effective = sum(max(0.0, value) for value in effective)
    return {
        "raw_n": len(rows),
        "effective_n": effective_n,
        "effective_overlap_purged_n": overlap_purged,
        "win_rate": sum(value > 0.0 for value in values) / len(values),
        "average_net_pips": sum(values) / len(values),
        "no_trade_average_net_pips": 0.0,
        "average_delta_vs_no_trade_pips": sum(values) / len(values),
        "effective_average_net_pips": effective_average,
        "unadjusted_normal_lower_95_mean_pips": effective_lcb,
        "profit_factor": profit / loss if loss > 0.0 else None,
        "moderate_stress_average_net_pips": sum(float(row["moderate"]) for row in rows) / len(rows),
        "severe_stress_average_net_pips": sum(float(row["severe"]) for row in rows) / len(rows),
        "average_mfe_pips": sum(float(row["mfe"]) for row in rows) / len(rows),
        "average_mae_pips": sum(float(row["mae"]) for row in rows) / len(rows),
        "average_missed_entry_slippage_pips": sum(float(row["latency"]) for row in rows) / len(rows),
        "minimum_net_pips": min(values),
        "maximum_net_pips": max(values),
        "best_effective_episode_positive_share": (
            max([0.0, *effective]) / positive_effective if positive_effective > 0.0 else None
        ),
        "exit_reasons": dict(sorted(Counter(str(row["exit_reason"]) for row in rows).items())),
    }


def equivalent_aggregate(expected: Any, actual: Any, tolerance: float = 1e-9) -> bool:
    if expected is None or actual is None:
        return expected is actual
    if isinstance(expected, bool) or isinstance(actual, bool):
        return expected == actual
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        return math.isclose(float(expected), float(actual), rel_tol=tolerance, abs_tol=tolerance)
    if isinstance(expected, dict) and isinstance(actual, dict):
        return set(expected) == set(actual) and all(
            equivalent_aggregate(expected[key], actual[key], tolerance) for key in expected
        )
    return expected == actual


def verify(
    database: Path = DEFAULT_DATABASE,
    *,
    cohort_id: str | None = None,
    verify_source_bytes: bool = True,
) -> dict[str, Any]:
    failures: list[str] = []
    checks: dict[str, Any] = {
        "verifier_sha256": file_sha256(Path(__file__).resolve()),
        "verifier_imports_producer": False,
    }
    if not database.is_file():
        return {
            "schema_version": 1,
            "generated_utc": utc_now(),
            "database": str(database),
            "verified": False,
            "failures": ["database_missing"],
            "supported_decision": "no_trade",
        }
    connection = sqlite3.connect(
        f"file:{database.resolve().as_posix()}?mode=ro", uri=True, timeout=60.0
    )
    connection.row_factory = sqlite3.Row
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    checks["sqlite_integrity"] = integrity
    if integrity != "ok":
        failures.append(f"sqlite_integrity:{integrity}")
    foreign_key_rows = [tuple(row) for row in connection.execute("PRAGMA foreign_key_check")]
    checks["foreign_key_check"] = foreign_key_rows
    if foreign_key_rows:
        failures.append(f"foreign_key_failures:{len(foreign_key_rows)}")
    tables = {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    checks["tables"] = sorted(tables)
    for token in FORBIDDEN_TABLE_TOKENS:
        matches = sorted(table for table in tables if token in table.lower())
        if matches:
            failures.append(f"forbidden_operational_table:{token}:{matches}")
    trigger_rows = list(
        connection.execute(
            "SELECT name,tbl_name,sql FROM sqlite_master WHERE type='trigger'"
        )
    )
    append_only: dict[str, dict[str, bool]] = {}
    for table in REQUIRED_APPEND_ONLY_TABLES:
        relevant = [row for row in trigger_rows if str(row["tbl_name"]) == table]
        sql = "\n".join(str(row["sql"] or "").lower() for row in relevant)
        status = {
            "update_blocked": "before update" in sql and "append_only" in sql,
            "delete_blocked": "before delete" in sql and "append_only" in sql,
        }
        append_only[table] = status
        if not all(status.values()):
            failures.append(f"append_only_trigger_missing:{table}")
    checks["append_only"] = append_only

    if cohort_id is None:
        latest = connection.execute(
            "SELECT next_cohort_id FROM sim_cohort_transitions ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        cohort_id = str(latest[0]) if latest else None
    if not cohort_id:
        failures.append("no_registered_cohort")
        connection.close()
        return {
            "schema_version": 1,
            "generated_utc": utc_now(),
            "database": str(database),
            "verified": False,
            "cohort_id": None,
            "checks": checks,
            "failures": failures,
            "supported_decision": "no_trade",
        }
    cohort = connection.execute(
        "SELECT * FROM sim_cohorts WHERE cohort_id=?", (cohort_id,)
    ).fetchone()
    if cohort is None:
        failures.append("cohort_missing")
        connection.close()
        return {
            "schema_version": 1,
            "generated_utc": utc_now(),
            "database": str(database),
            "verified": False,
            "cohort_id": cohort_id,
            "checks": checks,
            "failures": failures,
            "supported_decision": "no_trade",
        }
    contract = json.loads(str(cohort["material_contract_json"]))
    calculated_material = stable_hash(contract)
    checks["material_contract_sha256"] = calculated_material
    if calculated_material != str(cohort["material_contract_sha256"]):
        failures.append("material_contract_hash_mismatch")
    if str(cohort_id) != "counterfactual_sim_gym_v1." + calculated_material[:20]:
        failures.append("cohort_id_material_hash_mismatch")
    policy = contract.get("policy") or {}
    required_policy = {
        "research_only": True,
        "execution_eligible": False,
        "can_promote": False,
        "can_place_orders": False,
        "can_authorize": False,
        "broker_access": False,
        "account_access": False,
        "signal_feed_write": False,
        "lifecycle_write": False,
        "supported_decision": "no_trade",
    }
    if any(policy.get(key) != value for key, value in required_policy.items()):
        failures.append("research_isolation_policy_mismatch")
    if contract.get("contract_id") != CONTRACT_ID:
        failures.append("contract_id_mismatch")
    effective_config = contract.get("effective_config") or {}
    if not isinstance(effective_config, dict):
        failures.append("effective_config_missing")
        effective_config = {}
    experiment_key = str(cohort["experiment_key"])
    if experiment_key != str(effective_config.get("experiment_key") or ""):
        failures.append("experiment_key_effective_config_mismatch")
    transition_rows = list(
        connection.execute(
            "SELECT transition_id,previous_cohort_id,next_cohort_id,reason "
            "FROM sim_cohort_transitions WHERE experiment_key=? ORDER BY rowid",
            (experiment_key,),
        )
    )
    lineage_current: str | None = None
    lineage_seen: set[str] = set()
    lineage_failures = 0
    for position, row in enumerate(transition_rows):
        previous = None if row["previous_cohort_id"] is None else str(row["previous_cohort_id"])
        next_id = str(row["next_cohort_id"])
        expected_transition = "simtransition_" + stable_hash(
            experiment_key, lineage_current, next_id
        )[:28]
        expected_reason = "initial_registration" if position == 0 else "material_contract_change"
        referenced = connection.execute(
            "SELECT experiment_key FROM sim_cohorts WHERE cohort_id=?", (next_id,)
        ).fetchone()
        if (
            previous != lineage_current
            or next_id in lineage_seen
            or str(row["transition_id"]) != expected_transition
            or str(row["reason"]) != expected_reason
            or referenced is None
            or str(referenced[0]) != experiment_key
        ):
            lineage_failures += 1
        lineage_seen.add(next_id)
        lineage_current = next_id
    if not transition_rows or cohort_id not in lineage_seen:
        lineage_failures += 1
    checks["cohort_lineage"] = {
        "transition_count": len(transition_rows),
        "failures": lineage_failures,
        "current_cohort_id": lineage_current,
        "verified_cohort_in_chain": cohort_id in lineage_seen,
    }
    if lineage_failures:
        failures.append(f"cohort_lineage_failures:{lineage_failures}")
    recorded_config = Path(str(contract.get("config_path") or ""))
    if not recorded_config.is_absolute():
        recorded_config = ROOT / recorded_config
    code_paths = {
        "runner": ("runner_sha256", ROOT / "oanda_counterfactual_sim_gym.py"),
        "core": (
            "core_sha256",
            ROOT / "src" / "forex_system" / "research" / "counterfactual_sim_gym_v1.py",
        ),
        "config": ("config_sha256", recorded_config),
    }
    code_checks = {}
    contract_artifacts = contract.get("contract_artifacts") or {}
    archived_config_payload: dict[str, Any] | None = None
    for label, (field, path) in code_paths.items():
        expected = str(contract.get(field) or "")
        artifact = contract_artifacts.get(label) or {}
        archive_path: Path | None = None
        archive_path_error = None
        try:
            archive_path = contained_archive_path(artifact.get("archive_relative_path"))
        except (OSError, ValueError) as exc:
            archive_path_error = str(exc)
        archived = (
            file_sha256(archive_path)
            if archive_path is not None and archive_path.is_file()
            else None
        )
        if label == "config" and archive_path is not None and archived == expected:
            try:
                parsed_config = json.loads(archive_path.read_text(encoding="utf-8"))
                if not isinstance(parsed_config, dict):
                    raise ValueError("archived config must be a JSON object")
                archived_config_payload = parsed_config
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                code_checks["config_parse_error"] = str(exc)
        current = file_sha256(path) if path.is_file() else None
        code_checks[field] = {
            "current_path": str(path),
            "current_sha256": current,
            "archive_path": None if archive_path is None else str(archive_path),
            "archive_path_error": archive_path_error,
            "archive_sha256": archived,
            "expected": expected,
            "current_matches_frozen": current == expected,
        }
        if (
            archived != expected
            or str(artifact.get("sha256") or "") != expected
            or str(cohort[field]) != expected
        ):
            failures.append(f"contract_archive_hash_mismatch:{field}")
    checks["code_and_config"] = code_checks
    effective_config_binding_ok = False
    if archived_config_payload is not None:
        archived_effective = json.loads(canonical_json(archived_config_payload))
        archived_sampling = archived_effective.setdefault("sampling", {})
        effective_sampling = effective_config.get("sampling") or {}
        for key in ("history_days", "cadence_min"):
            if key in effective_sampling:
                archived_sampling[key] = effective_sampling[key]
        effective_config_binding_ok = archived_effective == effective_config
    checks["effective_config_matches_archive_plus_cli_overrides"] = (
        effective_config_binding_ok
    )
    if not effective_config_binding_ok:
        failures.append("effective_config_archive_mismatch")
    source_checks = []
    source_markets: dict[str, dict[str, Any]] = {}
    source_manifest = contract.get("source_manifest") or []
    if not source_manifest:
        failures.append("source_manifest_empty")
    for source in source_manifest:
        instrument = str(source.get("instrument") or "")
        path = Path(str(source.get("relative_path") or ""))
        if not path.is_absolute():
            path = ROOT / path
        frozen_bytes = int(source.get("frozen_prefix_bytes") or 0)
        expected = str(source.get("sha256") or "")
        archive_path = None
        archive_path_error = None
        try:
            archive_path = contained_archive_path(source.get("archive_relative_path"))
        except (OSError, ValueError) as exc:
            archive_path_error = str(exc)
        archive_gzip_sha = None
        archived_prefix = None
        replay_parse_error = None
        try:
            if verify_source_bytes and archive_path is not None and archive_path.is_file():
                archived_prefix, archive_gzip_sha = bounded_gzip_payload(
                    archive_path, frozen_bytes
                )
        except (OSError, EOFError, ValueError) as exc:
            replay_parse_error = str(exc)
            archived_prefix = None
        archive_raw_sha = hashlib.sha256(archived_prefix).hexdigest() if archived_prefix is not None else None
        if (
            archived_prefix is not None
            and archive_raw_sha == expected
            and archive_gzip_sha == str(source.get("archive_gzip_sha256") or "")
        ):
            try:
                parsed_market = independent_market_from_csv(archived_prefix, source)
                if instrument in source_markets:
                    raise ValueError("duplicate source manifest instrument")
                source_markets[instrument] = parsed_market
            except (UnicodeDecodeError, csv.Error, KeyError, TypeError, ValueError) as exc:
                replay_parse_error = str(exc)
        current_prefix = None
        if path.is_file() and path.stat().st_size >= frozen_bytes:
            try:
                current_prefix = file_prefix_sha256(path, frozen_bytes)
            except (OSError, ValueError):
                current_prefix = None
        source_checks.append(
            {
                "instrument": instrument,
                "intake_path": str(path),
                "archive_path": None if archive_path is None else str(archive_path),
                "archive_path_error": archive_path_error,
                "expected": expected,
                "archive_raw_sha256": archive_raw_sha,
                "archive_gzip_sha256": archive_gzip_sha,
                "current_prefix_sha256": current_prefix,
                "current_prefix_matches": current_prefix == expected,
                "frozen_prefix_bytes": frozen_bytes,
                "replay_parse_error": replay_parse_error,
            }
        )
        if (
            archive_path_error is not None
            or replay_parse_error is not None
            or archive_raw_sha != expected
            or len(archived_prefix or b"") != frozen_bytes
            or archive_gzip_sha != str(source.get("archive_gzip_sha256") or "")
        ):
            failures.append(f"source_archive_hash_mismatch:{instrument}")
    checks["source_files"] = source_checks

    arm_failures = 0
    arm_count = 0
    arm_definitions = []
    arms_by_id: dict[str, dict[str, Any]] = {}
    for row in connection.execute(
        "SELECT arm_id,definition_sha256,definition_json FROM sim_arms "
        "WHERE cohort_id=? ORDER BY arm_id",
        (cohort_id,),
    ):
        arm_count += 1
        definition = json.loads(str(row["definition_json"]))
        material = {
            key: definition[key]
            for key in (
                "signal_rule_id", "comparator", "entry_delay_min", "horizon_min",
                "reentry_gap_min", "exit_policy",
            )
        }
        expected = stable_hash(CONTRACT_ID, material)
        arm_definitions.append(definition)
        arms_by_id[str(row["arm_id"])] = definition
        if (
            expected != str(row["definition_sha256"])
            or definition.get("definition_sha256") != expected
            or definition.get("arm_id") != "simarm_" + expected[:28]
        ):
            arm_failures += 1
    arm_set_sha = stable_hash(arm_definitions)
    if arm_set_sha != str(contract.get("arm_definition_sha256") or ""):
        arm_failures += 1
    expected_arm_count = 0
    expected_arms_by_id: dict[str, dict[str, Any]] = {}
    try:
        expected_arm_definitions = independent_arm_definitions(effective_config)
        expected_arm_count = len(expected_arm_definitions)
        expected_arms_by_id = {
            str(definition["arm_id"]): definition
            for definition in expected_arm_definitions
        }
        if expected_arms_by_id != arms_by_id:
            arm_failures += 1
    except (KeyError, TypeError, ValueError) as exc:
        expected_arm_count = -1
        checks["arm_reconstruction_error"] = str(exc)
        arm_failures += 1
    checks["arms"] = {
        "count": arm_count,
        "independently_expected_count": expected_arm_count,
        "failures": arm_failures,
        "definition_set_sha256": arm_set_sha,
    }
    if arm_failures:
        failures.append(f"arm_definition_failures:{arm_failures}")

    expected_clock_map: dict[str, dict[str, Any]] = {}
    expected_signal_map: dict[str, dict[str, Any]] = {}
    expected_outcome_map: dict[str, dict[str, Any]] = {}
    expected_intent_map: dict[str, dict[str, Any]] = {}
    replay_construction_error = None
    expected_window = contract.get("window") or {}
    try:
        if not source_markets:
            raise ValueError("no independently parsed source markets")
        rules = [dict(row) for row in effective_config.get("signal_rules") or ()]
        rules_by_id = {str(row.get("id") or ""): row for row in rules}
        if len(rules_by_id) != len(rules) or not rules_by_id:
            raise ValueError("unique signal rules required for independent replay")
        sampling = effective_config.get("sampling") or {}
        cadence = max(1, int(sampling.get("cadence_min") or 0)) * 60
        minimum_warmup = max(
            int(
                rule.get("lookback_min")
                or rule.get("slow_min")
                or rule.get("window_min")
                or 1
            )
            for rule in rules
        )
        common_first = max(int(market["epochs"][0]) for market in source_markets.values())
        common_last = min(int(market["epochs"][-1]) + 60 for market in source_markets.values())
        replay_start = max(
            common_first,
            common_last - int(sampling.get("history_days") or 0) * 86400,
        )
        contract_start = int(expected_window.get("start_epoch") or -1)
        contract_end = int(expected_window.get("end_epoch") or -1)
        if (contract_start, contract_end) != (replay_start, common_last):
            raise ValueError("contract replay window disagrees with frozen sources/config")
        for instrument, market in sorted(source_markets.items()):
            epochs = market["epochs"]
            rows = market["rows_by_epoch"]
            for index in range(minimum_warmup, len(epochs)):
                decision_epoch = int(epochs[index]) + 60
                if (
                    decision_epoch < replay_start
                    or decision_epoch >= common_last
                    or decision_epoch % cadence
                ):
                    continue
                identity = {
                    "cohort_id": cohort_id,
                    "instrument": instrument,
                    "decision_epoch": decision_epoch,
                    "knowledge_cutoff_epoch": decision_epoch,
                    "source_candle_epoch": int(epochs[index]),
                    "session_bucket": independent_session_bucket(decision_epoch),
                    "decision_bid_close": float(rows[epochs[index]]["bid_close"]),
                    "decision_ask_close": float(rows[epochs[index]]["ask_close"]),
                }
                clock_id = "simclock_" + stable_hash(
                    cohort_id, instrument, decision_epoch
                )[:28]
                expected_clock_map[clock_id] = identity
                for rule in rules:
                    replayed_signal = independent_signal_observation(
                        market,
                        int(epochs[index]),
                        rule,
                        float((effective_config.get("costs") or {}).get("round_trip_slippage_pips") or 0.0),
                    )
                    if replayed_signal is None:
                        continue
                    metadata = replayed_signal["metadata"]
                    metadata_json = canonical_json(metadata)
                    signal_identity = {
                        "cohort_id": cohort_id,
                        "clock_id": clock_id,
                        "rule_id": str(replayed_signal["rule_id"]),
                        "raw_side": int(replayed_signal["raw_side"]),
                        "score": float(replayed_signal["score"]),
                        "feature_value_pips": float(replayed_signal["feature_value_pips"]),
                        "knowledge_cutoff_epoch": decision_epoch,
                        "metadata_sha256": stable_hash(metadata),
                        "metadata_json": metadata_json,
                    }
                    signal_id = "simsignal_" + stable_hash(
                        clock_id, signal_identity["rule_id"]
                    )[:28]
                    expected_signal_map[signal_id] = signal_identity

        arms_by_rule: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for definition in expected_arms_by_id.values():
            arms_by_rule[str(definition["signal_rule_id"])].append(definition)
        for definitions in arms_by_rule.values():
            definitions.sort(key=lambda value: str(value["arm_id"]))
        outcome_cache: dict[tuple[Any, ...], tuple[str, dict[str, Any]]] = {}
        last_reentry: dict[tuple[str, str, int, str], int] = {}
        ordered_signals = sorted(
            expected_signal_map.items(),
            key=lambda item: (
                expected_clock_map[str(item[1]["clock_id"])]["instrument"],
                expected_clock_map[str(item[1]["clock_id"])]["decision_epoch"],
                str(item[0]),
            ),
        )
        for signal_id, signal in ordered_signals:
            clock_id = str(signal["clock_id"])
            clock = expected_clock_map[clock_id]
            instrument = str(clock["instrument"])
            decision_epoch = int(clock["decision_epoch"])
            market = source_markets[instrument]
            for arm in arms_by_rule.get(str(signal["rule_id"]), ()):
                comparator = str(arm.get("comparator") or "")
                if comparator == "flipped_direction":
                    side = -int(signal["raw_side"])
                elif comparator == "deterministic_random_side":
                    side = (
                        1
                        if int(stable_hash("random_side", signal_id)[:16], 16) % 2 == 0
                        else -1
                    )
                else:
                    side = int(signal["raw_side"])
                base, quote = instrument.split("_", 1)
                factors = tuple(
                    sorted(
                        (
                            f"{base}:{'+' if side > 0 else '-'}",
                            f"{quote}:{'-' if side > 0 else '+'}",
                        )
                    )
                )
                influence_end = decision_epoch + (
                    int(arm["entry_delay_min"]) + int(arm["horizon_min"])
                ) * 60
                partition = independent_partition_interval(
                    decision_epoch,
                    influence_end,
                    replay_start,
                    common_last,
                    effective_config,
                )
                reentry_key = (
                    instrument,
                    str(arm["arm_id"]),
                    side,
                    partition or "purged",
                )
                previous = last_reentry.get(reentry_key)
                gap = int(arm["reentry_gap_min"])
                intent_id = "simintent_" + stable_hash(signal_id, arm["arm_id"])[:28]
                if previous is not None and decision_epoch - previous < gap * 60:
                    expected_intent_map[intent_id] = {
                        "cohort_id": cohort_id,
                        "signal_id": signal_id,
                        "arm_id": str(arm["arm_id"]),
                        "outcome_id": None,
                        "realized_side": side,
                        "partition_label": partition,
                        "eligible_for_diagnostics": 0,
                        "exclusion_reason": "reentry_filtered",
                        "signed_factor_1": factors[0],
                        "signed_factor_2": factors[1],
                    }
                    continue
                policy = arm.get("exit_policy") or {}
                if str(policy.get("kind") or "") != "endpoint":
                    raise ValueError("independent replay encountered non-endpoint arm")
                outcome_key = (
                    clock_id,
                    side,
                    int(arm["entry_delay_min"]),
                    int(arm["horizon_min"]),
                    canonical_json(policy),
                )
                cached = outcome_cache.get(outcome_key)
                if cached is None:
                    replayed_outcome = independent_endpoint_outcome(
                        market,
                        decision_epoch=decision_epoch,
                        side=side,
                        entry_delay_min=int(arm["entry_delay_min"]),
                        horizon_min=int(arm["horizon_min"]),
                        config=effective_config,
                    )
                    outcome_id = "simoutcome_" + stable_hash(
                        cohort_id,
                        clock_id,
                        side,
                        int(arm["entry_delay_min"]),
                        int(arm["horizon_min"]),
                        policy,
                    )[:28]
                    outcome_json = canonical_json(replayed_outcome)
                    outcome_identity = {
                        "cohort_id": cohort_id,
                        "clock_id": clock_id,
                        "side": side,
                        "entry_delay_min": int(arm["entry_delay_min"]),
                        "horizon_min": int(arm["horizon_min"]),
                        "exit_policy_id": str(policy.get("id") or ""),
                        "status": str(replayed_outcome["status"]),
                        "exclusion_reason": str(replayed_outcome["exclusion_reason"]),
                        "outcome_sha256": stable_hash(replayed_outcome),
                        "outcome_json": outcome_json,
                    }
                    expected_outcome_map[outcome_id] = {
                        **outcome_identity,
                        "outcome": replayed_outcome,
                    }
                    cached = (outcome_id, replayed_outcome)
                    outcome_cache[outcome_key] = cached
                outcome_id, replayed_outcome = cached
                eligible = replayed_outcome["status"] == "filled" and partition is not None
                exclusion_reason = str(replayed_outcome["exclusion_reason"])
                if partition is None:
                    exclusion_reason = "partition_boundary_or_embargo"
                expected_intent_map[intent_id] = {
                    "cohort_id": cohort_id,
                    "signal_id": signal_id,
                    "arm_id": str(arm["arm_id"]),
                    "outcome_id": outcome_id,
                    "realized_side": side,
                    "partition_label": partition,
                    "eligible_for_diagnostics": int(eligible),
                    "exclusion_reason": exclusion_reason,
                    "signed_factor_1": factors[0],
                    "signed_factor_2": factors[1],
                }
                if eligible:
                    last_reentry[reentry_key] = decision_epoch
    except (KeyError, TypeError, ValueError) as exc:
        replay_construction_error = str(exc)
        failures.append("independent_replay_construction_failure")
    preflight = contract.get("preflight") or {}
    expected_maximum_intent_estimate = len(expected_clock_map) * len(expected_arms_by_id)
    configured_maximum_intents = int(
        (effective_config.get("storage") or {}).get("maximum_virtual_order_intents")
        or 0
    )
    preflight_ok = (
        int(preflight.get("potential_pair_clock_count", -1))
        == len(expected_clock_map)
        and int(preflight.get("maximum_intent_estimate", -1))
        == expected_maximum_intent_estimate
        and int(preflight.get("maximum_virtual_order_intents", -1))
        == configured_maximum_intents
        and expected_maximum_intent_estimate <= configured_maximum_intents
    )
    if not preflight_ok:
        failures.append("independent_preflight_mismatch")
    checks["independent_replay_expected"] = {
        "clock_count": len(expected_clock_map),
        "signal_count": len(expected_signal_map),
        "outcome_count": len(expected_outcome_map),
        "intent_count": len(expected_intent_map),
        "maximum_intent_estimate": expected_maximum_intent_estimate,
        "preflight_matches": preflight_ok,
        "error": replay_construction_error,
    }

    normalized_roots: dict[str, dict[str, Any]] = {}
    normalized_failures: dict[str, int] = {}

    def finish_normalized(name: str, hashes: list[str], failure_count: int) -> None:
        normalized_roots[name] = {
            "count": len(hashes),
            "set_sha256": stable_hash(sorted(hashes)),
        }
        normalized_failures[name] = failure_count
        if failure_count:
            failures.append(f"normalized_{name}_failures:{failure_count}")

    clock_map: dict[str, dict[str, Any]] = {}
    observed_clock_ids: set[str] = set()
    row_hashes: list[str] = []
    row_failures = 0
    for row in connection.execute(
        "SELECT * FROM sim_decision_clocks WHERE cohort_id=? ORDER BY clock_id",
        (cohort_id,),
    ):
        identity = {
            "cohort_id": str(row["cohort_id"]),
            "instrument": str(row["instrument"]),
            "decision_epoch": int(row["decision_epoch"]),
            "knowledge_cutoff_epoch": int(row["knowledge_cutoff_epoch"]),
            "source_candle_epoch": int(row["source_candle_epoch"]),
            "session_bucket": str(row["session_bucket"]),
            "decision_bid_close": float(row["decision_bid_close"]),
            "decision_ask_close": float(row["decision_ask_close"]),
        }
        calculated = stable_hash(identity)
        row_hashes.append(calculated)
        clock_id = str(row["clock_id"])
        observed_clock_ids.add(clock_id)
        expected_clock = expected_clock_map.get(clock_id)
        if (
            calculated != str(row["row_sha256"])
            or expected_clock is None
            or not equivalent_aggregate(expected_clock, identity)
            or identity["source_candle_epoch"] + 60 != identity["decision_epoch"]
            or identity["knowledge_cutoff_epoch"] != identity["decision_epoch"]
            or identity["decision_bid_close"] >= identity["decision_ask_close"]
        ):
            row_failures += 1
        clock_map[clock_id] = identity
    row_failures += len(set(expected_clock_map) - observed_clock_ids)
    finish_normalized("decision_clocks", row_hashes, row_failures)

    signal_map: dict[str, dict[str, Any]] = {}
    observed_signal_ids: set[str] = set()
    row_hashes, row_failures = [], 0
    for row in connection.execute(
        "SELECT * FROM sim_signal_observations WHERE cohort_id=? ORDER BY signal_id",
        (cohort_id,),
    ):
        metadata = json.loads(str(row["metadata_json"]))
        metadata_sha = stable_hash(metadata)
        identity = {
            "cohort_id": str(row["cohort_id"]),
            "clock_id": str(row["clock_id"]),
            "rule_id": str(row["rule_id"]),
            "raw_side": int(row["raw_side"]),
            "score": float(row["score"]),
            "feature_value_pips": float(row["feature_value_pips"]),
            "knowledge_cutoff_epoch": int(row["knowledge_cutoff_epoch"]),
            "metadata_sha256": str(row["metadata_sha256"]),
            "metadata_json": str(row["metadata_json"]),
        }
        calculated = stable_hash(identity)
        row_hashes.append(calculated)
        signal_id = str(row["signal_id"])
        observed_signal_ids.add(signal_id)
        expected_signal = expected_signal_map.get(signal_id)
        clock = clock_map.get(identity["clock_id"])
        independently_matches = (
            expected_signal is not None
            and expected_signal["cohort_id"] == identity["cohort_id"]
            and expected_signal["clock_id"] == identity["clock_id"]
            and expected_signal["rule_id"] == identity["rule_id"]
            and expected_signal["raw_side"] == identity["raw_side"]
            and equivalent_aggregate(expected_signal["score"], identity["score"])
            and equivalent_aggregate(
                expected_signal["feature_value_pips"], identity["feature_value_pips"]
            )
            and expected_signal["knowledge_cutoff_epoch"]
            == identity["knowledge_cutoff_epoch"]
            and equivalent_aggregate(
                json.loads(str(expected_signal["metadata_json"])), metadata
            )
        )
        if (
            calculated != str(row["row_sha256"])
            or not independently_matches
            or metadata_sha != identity["metadata_sha256"]
            or identity["raw_side"] not in (-1, 1)
            or clock is None
            or identity["knowledge_cutoff_epoch"] != (clock or {}).get("decision_epoch")
        ):
            row_failures += 1
        signal_map[signal_id] = identity
    row_failures += len(set(expected_signal_map) - observed_signal_ids)
    finish_normalized("signal_observations", row_hashes, row_failures)

    outcome_map: dict[str, dict[str, Any]] = {}
    observed_outcome_ids: set[str] = set()
    row_hashes, row_failures = [], 0
    for row in connection.execute(
        "SELECT * FROM sim_outcome_parts WHERE cohort_id=? ORDER BY outcome_id",
        (cohort_id,),
    ):
        outcome = json.loads(str(row["outcome_json"]))
        outcome_sha = stable_hash(outcome)
        identity = {
            "cohort_id": str(row["cohort_id"]),
            "clock_id": str(row["clock_id"]),
            "side": int(row["side"]),
            "entry_delay_min": int(row["entry_delay_min"]),
            "horizon_min": int(row["horizon_min"]),
            "exit_policy_id": str(row["exit_policy_id"]),
            "status": str(row["status"]),
            "exclusion_reason": str(row["exclusion_reason"]),
            "outcome_sha256": str(row["outcome_sha256"]),
            "outcome_json": str(row["outcome_json"]),
        }
        calculated = stable_hash(identity)
        row_hashes.append(calculated)
        outcome_id = str(row["outcome_id"])
        observed_outcome_ids.add(outcome_id)
        expected_outcome = expected_outcome_map.get(outcome_id)
        independently_matches = (
            expected_outcome is not None
            and all(
                expected_outcome[key] == identity[key]
                for key in (
                    "cohort_id",
                    "clock_id",
                    "side",
                    "entry_delay_min",
                    "horizon_min",
                    "exit_policy_id",
                    "status",
                    "exclusion_reason",
                )
            )
            and equivalent_aggregate(expected_outcome["outcome"], outcome)
        )
        if (
            calculated != str(row["row_sha256"])
            or not independently_matches
            or outcome_sha != identity["outcome_sha256"]
            or identity["clock_id"] not in clock_map
            or identity["side"] not in (-1, 1)
            or str(outcome.get("status")) != identity["status"]
            or str(outcome.get("exclusion_reason") or "") != identity["exclusion_reason"]
        ):
            row_failures += 1
        outcome_map[outcome_id] = {**identity, "outcome": outcome}
    row_failures += len(set(expected_outcome_map) - observed_outcome_ids)
    finish_normalized("outcome_parts", row_hashes, row_failures)

    row_hashes, row_failures = [], 0
    observed_intent_ids: set[str] = set()
    for row in connection.execute(
        "SELECT * FROM sim_intent_map WHERE cohort_id=? ORDER BY intent_id",
        (cohort_id,),
    ):
        identity = {
            "cohort_id": str(row["cohort_id"]),
            "signal_id": str(row["signal_id"]),
            "arm_id": str(row["arm_id"]),
            "outcome_id": None if row["outcome_id"] is None else str(row["outcome_id"]),
            "realized_side": int(row["realized_side"]),
            "partition_label": None if row["partition_label"] is None else str(row["partition_label"]),
            "eligible_for_diagnostics": int(row["eligible_for_diagnostics"]),
            "exclusion_reason": str(row["exclusion_reason"]),
            "signed_factor_1": str(row["signed_factor_1"]),
            "signed_factor_2": str(row["signed_factor_2"]),
        }
        calculated = stable_hash(identity)
        row_hashes.append(calculated)
        intent_id = str(row["intent_id"])
        observed_intent_ids.add(intent_id)
        independently_expected_intent = expected_intent_map.get(intent_id)
        signal = signal_map.get(identity["signal_id"])
        arm = arms_by_id.get(identity["arm_id"])
        outcome = outcome_map.get(identity["outcome_id"] or "")
        expected_side = None
        if signal is not None and arm is not None:
            comparator = str(arm.get("comparator") or "")
            if comparator == "flipped_direction":
                expected_side = -int(signal["raw_side"])
            elif comparator == "deterministic_random_side":
                expected_side = (
                    1
                    if int(stable_hash("random_side", identity["signal_id"])[:16], 16) % 2 == 0
                    else -1
                )
            else:
                expected_side = int(signal["raw_side"])
        clock = clock_map.get((signal or {}).get("clock_id", ""))
        expected_factors: tuple[str, str] | tuple[()] = ()
        if clock is not None and expected_side in (-1, 1):
            base, quote = str(clock["instrument"]).split("_", 1)
            expected_factors = tuple(
                sorted(
                    (
                        f"{base}:{'+' if expected_side > 0 else '-'}",
                        f"{quote}:{'-' if expected_side > 0 else '+'}",
                    )
                )
            )
        eligible = identity["eligible_for_diagnostics"] == 1
        logical_ok = (
            signal is not None
            and arm is not None
            and str(arm.get("signal_rule_id")) == str(signal.get("rule_id"))
            and expected_side == identity["realized_side"]
            and expected_factors
            == (identity["signed_factor_1"], identity["signed_factor_2"])
        )
        if outcome is not None and arm is not None:
            logical_ok = logical_ok and (
                int(outcome["side"]) == identity["realized_side"]
                and int(outcome["entry_delay_min"]) == int(arm["entry_delay_min"])
                and int(outcome["horizon_min"]) == int(arm["horizon_min"])
                and str(outcome["exit_policy_id"])
                == str((arm.get("exit_policy") or {}).get("id") or "")
            )
        if eligible:
            logical_ok = logical_ok and (
                outcome is not None
                and outcome["status"] == "filled"
                and identity["partition_label"] is not None
                and identity["exclusion_reason"] == ""
            )
        elif identity["exclusion_reason"] == "reentry_filtered":
            logical_ok = logical_ok and outcome is None
        if (
            calculated != str(row["row_sha256"])
            or independently_expected_intent != identity
            or not logical_ok
        ):
            row_failures += 1
    row_failures += len(set(expected_intent_map) - observed_intent_ids)
    finish_normalized("intent_map", row_hashes, row_failures)
    checks["normalized_rows"] = {
        "roots": normalized_roots,
        "failures": normalized_failures,
    }

    rebuilt_cells: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    rebuild_source_rows = 0
    for row in connection.execute(
        """
        SELECT i.arm_id,i.partition_label,i.signed_factor_1,i.signed_factor_2,
               c.instrument,c.decision_epoch,o.outcome_json,a.definition_json
        FROM sim_intent_map i
        JOIN sim_signal_observations s ON s.signal_id=i.signal_id
        JOIN sim_decision_clocks c ON c.clock_id=s.clock_id
        JOIN sim_outcome_parts o ON o.outcome_id=i.outcome_id
        JOIN sim_arms a ON a.cohort_id=i.cohort_id AND a.arm_id=i.arm_id
        WHERE i.cohort_id=? AND i.eligible_for_diagnostics=1
        ORDER BY c.instrument,c.decision_epoch,i.arm_id
        """,
        (cohort_id,),
    ):
        rebuild_source_rows += 1
        outcome = json.loads(str(row["outcome_json"]))
        arm = json.loads(str(row["definition_json"]))
        instrument = str(row["instrument"])
        decision_epoch = int(row["decision_epoch"])
        spread = float(outcome["entry_spread_pips"])
        session = independent_session_bucket(decision_epoch)
        liquidity = independent_liquidity_bucket(spread)
        datum = {
            "start": decision_epoch,
            "end": decision_epoch
            + (int(arm["entry_delay_min"]) + int(arm["horizon_min"])) * 60,
            "factors": (str(row["signed_factor_1"]), str(row["signed_factor_2"])),
            "instrument": instrument,
            "value": float(outcome["executable_net_pips"]),
            "moderate": float(outcome["moderate_stress_net_pips"]),
            "severe": float(outcome["severe_stress_net_pips"]),
            "mfe": float(outcome["mfe_pips"]),
            "mae": float(outcome["mae_pips"]),
            "latency": float(outcome["missed_entry_slippage_pips"]),
            "exit_reason": str(outcome["exit_reason"]),
        }
        for key in (
            ("pair", instrument, str(row["arm_id"]), str(row["partition_label"]), session, liquidity),
            ("pair", instrument, str(row["arm_id"]), str(row["partition_label"]), "all", "all"),
            ("all_pairs", "ALL", str(row["arm_id"]), str(row["partition_label"]), session, liquidity),
            ("all_pairs", "ALL", str(row["arm_id"]), str(row["partition_label"]), "all", "all"),
        ):
            rebuilt_cells[key].append(datum)
    rebuilt_payloads = {
        key: independent_cell_summary(rows) for key, rows in rebuilt_cells.items()
    }

    aggregate_hashes = []
    aggregate_failures = 0
    observed_aggregate_keys: set[tuple[str, ...]] = set()
    for row in connection.execute(
        "SELECT * FROM sim_cell_aggregates WHERE cohort_id=?", (cohort_id,)
    ):
        aggregate = json.loads(str(row["aggregate_json"]))
        key = (
            str(row["scope"]),
            str(row["instrument"]),
            str(row["arm_id"]),
            str(row["partition_label"]),
            str(row["session_bucket"]),
            str(row["liquidity_bucket"]),
        )
        duplicate_key = key in observed_aggregate_keys
        observed_aggregate_keys.add(key)
        identity = {
            "cohort_id": cohort_id,
            "scope": str(row["scope"]),
            "instrument": str(row["instrument"]),
            "arm_id": str(row["arm_id"]),
            "partition_label": str(row["partition_label"]),
            "session_bucket": str(row["session_bucket"]),
            "liquidity_bucket": str(row["liquidity_bucket"]),
            "aggregate": aggregate,
        }
        calculated = stable_hash(identity)
        aggregate_hashes.append(calculated)
        if (
            calculated != str(row["aggregate_sha256"])
            or duplicate_key
            or str(row["cell_id"])
            != "simcell_" + stable_hash(cohort_id, key)[:28]
            or str(row["arm_id"]) not in arms_by_id
            or key not in rebuilt_payloads
            or not equivalent_aggregate(rebuilt_payloads.get(key), aggregate)
        ):
            aggregate_failures += 1
    missing_rebuilt_cells = len(set(rebuilt_payloads) - observed_aggregate_keys)
    aggregate_failures += missing_rebuilt_cells
    checks["aggregates"] = {
        "count": len(aggregate_hashes),
        "failures": aggregate_failures,
        "set_sha256": stable_hash(sorted(aggregate_hashes)),
        "independently_rebuilt_source_rows": rebuild_source_rows,
        "independently_rebuilt_cell_count": len(rebuilt_payloads),
        "missing_rebuilt_cells": missing_rebuilt_cells,
    }
    if aggregate_failures:
        failures.append(f"aggregate_hash_failures:{aggregate_failures}")

    snapshot_failures = 0
    snapshots = 0
    actual_normalized_roots = {
        "arms": {
            "count": arm_count,
            "set_sha256": stable_hash(
                sorted(str(value["definition_sha256"]) for value in arms_by_id.values())
            ),
        },
        **normalized_roots,
        "cell_aggregates": {
            "count": len(aggregate_hashes),
            "set_sha256": stable_hash(sorted(aggregate_hashes)),
        },
    }
    # The definitions dict stores its digest inside each payload; the producer
    # roots the table's dedicated digest column, not the whole payload hash.
    actual_normalized_roots["arms"]["set_sha256"] = stable_hash(
        sorted(str(value.get("definition_sha256") or "") for value in arms_by_id.values())
    )
    eligible_intents = int(
        connection.execute(
            "SELECT COUNT(*) FROM sim_intent_map WHERE cohort_id=? AND eligible_for_diagnostics=1",
            (cohort_id,),
        ).fetchone()[0]
    )
    invalid_intents = int(
        connection.execute(
            "SELECT COUNT(*) FROM sim_intent_map i JOIN sim_outcome_parts o "
            "ON o.outcome_id=i.outcome_id WHERE i.cohort_id=? AND o.status!='filled'",
            (cohort_id,),
        ).fetchone()[0]
    )
    independently_eligible = sum(
        int(intent["eligible_for_diagnostics"] == 1)
        for intent in expected_intent_map.values()
    )
    independently_reentry_filtered = sum(
        int(intent["exclusion_reason"] == "reentry_filtered")
        for intent in expected_intent_map.values()
    )
    independently_partition_purged = sum(
        int(
            intent["outcome_id"] is not None
            and intent["partition_label"] is None
        )
        for intent in expected_intent_map.values()
    )
    independently_excluded = 0
    independently_exclusion_counts: Counter[str] = Counter()
    independently_cluster_counts: Counter[str] = Counter()
    for intent in expected_intent_map.values():
        outcome_id = intent["outcome_id"]
        if outcome_id is None:
            continue
        outcome_record = expected_outcome_map[str(outcome_id)]
        outcome_payload = outcome_record["outcome"]
        if str(outcome_record["status"]) != "filled":
            independently_excluded += 1
            independently_exclusion_counts[
                str(outcome_record["exclusion_reason"] or "invalid_outcome")
            ] += 1
        if int(intent["eligible_for_diagnostics"]) == 1:
            for cluster in outcome_payload.get("mistake_clusters") or ():
                independently_cluster_counts[str(cluster)] += 1
    checks["independent_statistics"] = {
        "decision_count": len(expected_clock_map),
        "virtual_order_intent_count": len(expected_intent_map),
        "unique_outcome_part_count": len(expected_outcome_map),
        "filled_outcome_count": independently_eligible,
        "excluded_outcome_count": independently_excluded,
        "reentry_filtered_count": independently_reentry_filtered,
        "partition_boundary_purged_count": independently_partition_purged,
        "ineligible_intent_count": len(expected_intent_map) - independently_eligible,
        "exclusion_counts": dict(sorted(independently_exclusion_counts.items())),
        "mistake_cluster_counts": dict(sorted(independently_cluster_counts.items())),
    }
    for row in connection.execute(
        "SELECT * FROM sim_run_snapshots WHERE cohort_id=?", (cohort_id,)
    ):
        snapshots += 1
        statistics = json.loads(str(row["statistics_json"]))
        expected_roots = statistics.get("normalized_ledger_roots") or {}
        if (
            int(row["aggregate_count"]) != len(aggregate_hashes)
            or str(row["aggregate_set_sha256"]) != stable_hash(sorted(aggregate_hashes))
            or str(row["statistics_sha256"]) != stable_hash(statistics)
            or expected_roots != actual_normalized_roots
            or int(statistics.get("decision_count", -1))
            != normalized_roots["decision_clocks"]["count"]
            or int(statistics.get("virtual_order_intent_count", -1))
            != normalized_roots["intent_map"]["count"]
            or int(statistics.get("unique_outcome_part_count", -1))
            != normalized_roots["outcome_parts"]["count"]
            or int(statistics.get("filled_outcome_count", -1)) != eligible_intents
            or int(statistics.get("excluded_outcome_count", -1)) != invalid_intents
            or int(statistics.get("decision_count", -1)) != len(expected_clock_map)
            or int(statistics.get("virtual_order_intent_count", -1))
            != len(expected_intent_map)
            or int(statistics.get("unique_outcome_part_count", -1))
            != len(expected_outcome_map)
            or int(statistics.get("filled_outcome_count", -1))
            != independently_eligible
            or int(statistics.get("excluded_outcome_count", -1))
            != independently_excluded
            or int(statistics.get("reentry_filtered_count", -1))
            != independently_reentry_filtered
            or int(statistics.get("partition_boundary_purged_count", -1))
            != independently_partition_purged
            or (statistics.get("exclusion_counts") or {})
            != dict(sorted(independently_exclusion_counts.items()))
            or (statistics.get("mistake_cluster_counts") or {})
            != dict(sorted(independently_cluster_counts.items()))
        ):
            snapshot_failures += 1
    checks["snapshots"] = {"count": snapshots, "failures": snapshot_failures}
    if snapshots == 0:
        failures.append("snapshot_missing")
    if snapshot_failures:
        failures.append(f"snapshot_hash_failures:{snapshot_failures}")
    connection.close()
    return {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "database": str(database),
        "cohort_id": cohort_id,
        "verified": not failures,
        "checks": checks,
        "failures": failures,
        "research_only": True,
        "execution_eligible": False,
        "can_promote": False,
        "can_place_orders": False,
        "supported_decision": "no_trade",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--cohort-id")
    parser.add_argument("--skip-source-bytes", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = verify(
        args.database,
        cohort_id=args.cohort_id,
        verify_source_bytes=not args.skip_source_bytes,
    )
    atomic_json(DEFAULT_OUTPUT, result)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
