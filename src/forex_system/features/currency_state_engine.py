"""Pure research-only currency-state reconstruction.

The module deliberately has no filesystem, database, broker, authorization, or
execution imports.  It turns timestamped pair observations into an identified
currency vector and then projects that vector back onto the canonical pair
edges.  Observed response is never represented as a forward forecast.
"""

from __future__ import annotations

import bisect
import datetime as dt
import math
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from ..contracts.currency_state import stable_hash, validate_contract


UTC = dt.timezone.utc


def _float(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def parse_epoch(value: Any) -> float | None:
    if value is None or value == "":
        return None
    numeric = _float(value)
    if numeric is not None and not isinstance(value, str):
        return numeric
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return numeric
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def iso_utc(epoch: float) -> str:
    return dt.datetime.fromtimestamp(epoch, tz=UTC).isoformat()


@dataclass(frozen=True)
class PairObservation:
    instrument: str
    base_currency: str
    quote_currency: str
    return_bps: float
    spread_bps: float
    weight: float
    start_epoch: int
    end_epoch: int
    actual_observation_duration_sec: int
    endpoint_age_sec: float
    start_alignment_sec: float
    bid: float
    ask: float
    pip: float

    def payload(self) -> dict[str, Any]:
        return asdict(self)


def _mid(row: Mapping[str, Any]) -> float | None:
    bid = _float(row.get("close_bid"))
    ask = _float(row.get("close_ask"))
    if bid is None or ask is None or bid <= 0 or ask <= bid:
        return None
    return (bid + ask) / 2.0


def pair_observations_from_history(
    rows: Sequence[Mapping[str, Any]],
    *,
    horizon_sec: int,
    cutoff_epoch: int,
    contract: Mapping[str, Any],
) -> tuple[list[PairObservation], dict[str, int]]:
    """Build synchronized observations without carrying stale legs forward."""

    validate_contract(contract)
    policy = contract["price_response"]
    allowed = set(str(value) for value in contract["instruments"])
    grouped: dict[str, dict[int, Mapping[str, Any]]] = defaultdict(dict)
    rejected: dict[str, int] = defaultdict(int)
    for row in rows:
        instrument = str(row.get("instrument") or "")
        if instrument not in allowed:
            if instrument:
                rejected["outside_universe"] += 1
            continue
        minute = int(_float(row.get("minute_epoch"), 0.0) or 0)
        if minute <= 0:
            rejected["missing_time"] += 1
            continue
        if minute > cutoff_epoch:
            rejected["future_bar"] += 1
            continue
        existing = grouped[instrument].get(minute)
        if existing is None or (_float(row.get("last_epoch"), 0.0) or 0.0) >= (
            _float(existing.get("last_epoch"), 0.0) or 0.0
        ):
            grouped[instrument][minute] = row

    maximum_endpoint_age = float(policy["maximum_endpoint_age_sec"])
    maximum_start_alignment = float(policy["maximum_start_alignment_sec"])
    spread_floor = float(policy["spread_floor_bps"])
    maximum_weight = float(policy["maximum_weight"])
    target_epoch = cutoff_epoch - int(horizon_sec)
    observations: list[PairObservation] = []
    for instrument in sorted(allowed):
        by_time = grouped.get(instrument)
        if not by_time:
            rejected["missing_instrument"] += 1
            continue
        times = sorted(by_time)
        end_index = bisect.bisect_right(times, cutoff_epoch) - 1
        if end_index < 0:
            rejected["missing_endpoint"] += 1
            continue
        end_epoch = times[end_index]
        endpoint_age = float(cutoff_epoch - end_epoch)
        if endpoint_age < 0 or endpoint_age > maximum_endpoint_age:
            rejected["stale_endpoint"] += 1
            continue
        start_index = bisect.bisect_right(times, target_epoch) - 1
        if start_index < 0:
            rejected["missing_start"] += 1
            continue
        start_epoch = times[start_index]
        start_alignment = float(target_epoch - start_epoch)
        if start_alignment < 0 or start_alignment > maximum_start_alignment:
            rejected["stale_start"] += 1
            continue

        start_row = by_time[start_epoch]
        end_row = by_time[end_epoch]
        start_mid = _mid(start_row)
        end_mid = _mid(end_row)
        bid = _float(end_row.get("close_bid"))
        ask = _float(end_row.get("close_ask"))
        if start_mid is None or end_mid is None or bid is None or ask is None:
            rejected["invalid_bid_ask"] += 1
            continue
        pip = _float(end_row.get("pip"), 0.0001) or 0.0001
        spread_bps = (ask - bid) / end_mid * 10_000.0
        if spread_bps < 0 or not math.isfinite(spread_bps):
            rejected["invalid_spread"] += 1
            continue
        base, quote = instrument.split("_")
        return_bps = math.log(end_mid / start_mid) * 10_000.0
        freshness = math.exp(-endpoint_age / max(maximum_endpoint_age, 1.0))
        alignment = math.exp(-start_alignment / max(maximum_start_alignment, 1.0))
        weight = min(maximum_weight, 1.0 / max(spread_bps, spread_floor))
        weight = max(1e-9, weight * freshness * alignment)
        observations.append(
            PairObservation(
                instrument=instrument,
                base_currency=base,
                quote_currency=quote,
                return_bps=round(return_bps, 12),
                spread_bps=round(spread_bps, 12),
                weight=round(weight, 12),
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                actual_observation_duration_sec=int(end_epoch - start_epoch),
                endpoint_age_sec=round(endpoint_age, 6),
                start_alignment_sec=round(start_alignment, 6),
                bid=bid,
                ask=ask,
                pip=pip,
            )
        )
    return observations, dict(sorted(rejected.items()))


def _connected_components(observations: Sequence[PairObservation]) -> list[tuple[str, ...]]:
    adjacency: dict[str, set[str]] = defaultdict(set)
    for row in observations:
        adjacency[row.base_currency].add(row.quote_currency)
        adjacency[row.quote_currency].add(row.base_currency)
    remaining = set(adjacency)
    output: list[tuple[str, ...]] = []
    while remaining:
        pending = [min(remaining)]
        component: set[str] = set()
        while pending:
            currency = pending.pop()
            if currency in component:
                continue
            component.add(currency)
            pending.extend(sorted(adjacency[currency] - component, reverse=True))
        remaining -= component
        output.append(tuple(sorted(component)))
    return sorted(output, key=lambda value: (-len(value), value))


def solve_weighted_currency_state(
    observations: Sequence[PairObservation],
    *,
    currencies: Sequence[str],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    """Solve r(base)-r(quote)=r(pair) on the largest connected component."""

    deduplicated: dict[str, PairObservation] = {}
    duplicate_count = 0
    for row in sorted(observations, key=lambda value: (value.instrument, value.end_epoch)):
        if row.instrument in deduplicated:
            duplicate_count += 1
        deduplicated[row.instrument] = row
    rows = list(deduplicated.values())
    components = _connected_components(rows)
    if not components:
        return {
            "status": "insufficient_observations",
            "active_currencies": [],
            "components": [],
            "observation_count": 0,
            "duplicate_observation_count": duplicate_count,
        }
    primary = components[0]
    active = set(primary)
    rows = [
        row
        for row in rows
        if row.base_currency in active and row.quote_currency in active
    ]
    minimum_observations = int(policy["minimum_observations"])
    minimum_currencies = int(policy["minimum_component_currencies"])
    if len(rows) < max(minimum_observations, len(primary) - 1) or len(primary) < minimum_currencies:
        return {
            "status": "insufficient_observations",
            "active_currencies": list(primary),
            "components": [list(value) for value in components],
            "observation_count": len(rows),
            "duplicate_observation_count": duplicate_count,
        }

    index = {currency: position for position, currency in enumerate(primary)}
    design = np.zeros((len(rows), len(primary)), dtype=float)
    target = np.zeros(len(rows), dtype=float)
    weights = np.zeros(len(rows), dtype=float)
    raw_moves = [row.return_bps for row in rows]
    median_abs = statistics.median(abs(value) for value in raw_moves)
    clip = max(
        float(policy["return_clip_minimum_bps"]),
        median_abs * float(policy["return_clip_median_multiple"]),
    )
    for row_index, row in enumerate(rows):
        design[row_index, index[row.base_currency]] = 1.0
        design[row_index, index[row.quote_currency]] = -1.0
        target[row_index] = max(-clip, min(clip, row.return_bps))
        weights[row_index] = max(row.weight, 1e-12)

    root_weights = np.sqrt(weights)
    weighted_design = design * root_weights[:, None]
    weighted_target = target * root_weights
    constraint = np.ones((1, len(primary)), dtype=float)
    augmented_design = np.vstack([weighted_design, constraint])
    augmented_target = np.concatenate([weighted_target, np.asarray([0.0])])
    solution, _, rank, singular_values = np.linalg.lstsq(
        augmented_design, augmented_target, rcond=None
    )
    predictions = design @ solution
    residual_values = target - predictions
    clipped_returns = {
        row.instrument: round(float(target[position]), 12)
        for position, row in enumerate(rows)
    }
    residuals = {
        row.instrument: round(float(residual_values[position]), 12)
        for position, row in enumerate(rows)
    }
    weighted_residual_variance = float(
        np.sum(weights * residual_values * residual_values) / max(np.sum(weights), 1e-12)
    )
    floor = float(policy["measurement_uncertainty_floor_bps"])
    sigma2 = max(floor * floor, weighted_residual_variance)
    normal = design.T @ (weights[:, None] * design)
    covariance = np.linalg.pinv(normal, hermitian=True) * sigma2
    covariance = (covariance + covariance.T) / 2.0
    uncertainty = {
        currency: round(max(floor, math.sqrt(max(0.0, float(covariance[pos, pos])))), 12)
        for currency, pos in index.items()
    }
    strengths = {
        currency: round(float(solution[position]), 12)
        for currency, position in index.items()
    }
    pair_counts = {currency: 0 for currency in primary}
    for row in rows:
        pair_counts[row.base_currency] += 1
        pair_counts[row.quote_currency] += 1
    positive_singular = [float(value) for value in singular_values if value > 1e-12]
    condition = (
        max(positive_singular) / min(positive_singular)
        if positive_singular
        else None
    )
    weight_sum = float(np.sum(weights))
    weight_square_sum = float(np.sum(weights * weights))
    equivalent = (
        weight_sum * weight_sum / weight_square_sum if weight_square_sum > 0 else 0.0
    )
    all_currencies = tuple(str(value) for value in currencies)
    return {
        "status": "ok" if len(components) == 1 else "degraded_disconnected",
        "active_currencies": list(primary),
        "unavailable_currencies": sorted(set(all_currencies) - set(primary)),
        "components": [list(value) for value in components],
        "discarded_component_currencies": sorted(
            {currency for component in components[1:] for currency in component}
        ),
        "observation_count": len(rows),
        "duplicate_observation_count": duplicate_count,
        "weighted_observation_equivalent": round(equivalent, 6),
        "matrix_rank": int(rank),
        "condition_number": None if condition is None else round(condition, 6),
        "return_clip_bps": round(clip, 12),
        "median_abs_pair_return_bps": round(median_abs, 12),
        "residual_rmse_bps": round(math.sqrt(float(np.mean(residual_values**2))), 12),
        "weighted_residual_sigma_bps": round(math.sqrt(sigma2), 12),
        "strengths_bps": strengths,
        "uncertainty_bps": uncertainty,
        "pair_counts": pair_counts,
        "clipped_pair_returns_bps": clipped_returns,
        "pair_residuals_clipped_bps": residuals,
        "pair_residuals_bps": residuals,
        "pair_residual_basis": "clipped_solver_return_minus_factor",
        "covariance_currencies": list(primary),
        "covariance_bps2": [
            [round(float(value), 12) for value in row]
            for row in covariance.tolist()
        ],
    }


def _edge_uncertainty(
    result: Mapping[str, Any], base: str, quote: str
) -> float | None:
    currencies = list(result.get("covariance_currencies") or [])
    if base not in currencies or quote not in currencies:
        return None
    covariance = result.get("covariance_bps2") or []
    base_index = currencies.index(base)
    quote_index = currencies.index(quote)
    variance = (
        float(covariance[base_index][base_index])
        + float(covariance[quote_index][quote_index])
        - 2.0 * float(covariance[base_index][quote_index])
    )
    return round(math.sqrt(max(0.0, variance)), 12)


def _component_rows(
    contract: Mapping[str, Any],
    *,
    observed_value: float | None,
    observed_uncertainty: float | None,
    source_age_sec: float | None,
    pair_count: int,
) -> list[dict[str, Any]]:
    output = []
    for definition in contract.get("component_channels") or []:
        component_id = str(definition["id"])
        role = str(definition["role"])
        if component_id == "latent_price_response" and observed_value is not None:
            output.append(
                {
                    "component_id": component_id,
                    "role": role,
                    "state": "available",
                    "observed_signed_move_bps": observed_value,
                    "forecast_mean_bps": None,
                    "forecast_absolute_move_bps": None,
                    "cost_clear_probability": None,
                    "uncertainty_bps": observed_uncertainty,
                    "source_age_sec": source_age_sec,
                    "pair_observation_count": pair_count,
                    "reason": "observed_response_is_not_a_forward_forecast",
                }
            )
        else:
            output.append(
                {
                    "component_id": component_id,
                    "role": role,
                    "state": "unavailable",
                    "observed_signed_move_bps": None,
                    "forecast_mean_bps": None,
                    "forecast_absolute_move_bps": None,
                    "cost_clear_probability": None,
                    "uncertainty_bps": None,
                    "source_age_sec": None,
                    "pair_observation_count": 0,
                    "reason": "adapter_not_connected",
                }
            )
    return output


def _leave_one_edge_out(
    observations: Sequence[PairObservation],
    instrument: str,
    *,
    currencies: Sequence[str],
    policy: Mapping[str, Any],
) -> float | None:
    base, quote = instrument.split("_")
    result = solve_weighted_currency_state(
        [row for row in observations if row.instrument != instrument],
        currencies=currencies,
        policy=policy,
    )
    strengths = result.get("strengths_bps") or {}
    if base not in strengths or quote not in strengths:
        return None
    return round(float(strengths[base]) - float(strengths[quote]), 12)


def _stable_input_identity(input_refs: Mapping[str, Any]) -> dict[str, Any]:
    """Exclude machine-specific paths while preserving immutable identities."""

    identity: dict[str, Any] = {}
    for key, value in sorted(input_refs.items()):
        normalized = str(key).lower()
        if normalized.endswith("_sha256") or normalized.endswith("_id"):
            identity[str(key)] = value
    return identity


def build_currency_state_snapshot(
    quote_history_payload: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    decision_cutoff_utc: str | None = None,
    input_refs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the full 21-node/68-edge diagnostic snapshot."""

    validate_contract(contract)
    cutoff = parse_epoch(decision_cutoff_utc or quote_history_payload.get("generated_utc"))
    if cutoff is None:
        raise ValueError("a valid decision cutoff is required")
    knowledge_cutoff = float(cutoff)
    complete_minute_cutoff = int(knowledge_cutoff // 60) * 60 - 60
    currencies = tuple(str(value) for value in contract["currencies"])
    instruments = tuple(str(value) for value in contract["instruments"])
    expected_pair_counts = {
        currency: sum(currency in instrument.split("_") for instrument in instruments)
        for currency in currencies
    }
    source_rows = [
        dict(row)
        for row in quote_history_payload.get("rows") or []
        if isinstance(row, Mapping)
    ]
    horizons: dict[str, Any] = {}
    for horizon_sec in contract["horizons_sec"]:
        observations, rejected = pair_observations_from_history(
            source_rows,
            horizon_sec=int(horizon_sec),
            cutoff_epoch=complete_minute_cutoff,
            contract=contract,
        )
        result = solve_weighted_currency_state(
            observations,
            currencies=currencies,
            policy=contract["price_response"],
        )
        strengths = result.get("strengths_bps") or {}
        uncertainty = result.get("uncertainty_bps") or {}
        pair_counts = result.get("pair_counts") or {}
        latest_endpoint = max((row.end_epoch for row in observations), default=0)
        source_age = (
            float(complete_minute_cutoff - latest_endpoint) if latest_endpoint else None
        )
        currency_rows: dict[str, Any] = {}
        for currency in currencies:
            observed = _float(strengths.get(currency))
            observed_uncertainty = _float(uncertainty.get(currency))
            pair_count = int(pair_counts.get(currency) or 0)
            currency_rows[currency] = {
                "currency": currency,
                "horizon_sec": int(horizon_sec),
                "state": "observed_response_only" if observed is not None else "unavailable",
                "component_id": (
                    "primary_connected_component" if observed is not None else None
                ),
                "observed_currency_return_bps": observed,
                "forecast_mean_bps": None,
                "forecast_absolute_move_bps": None,
                "cost_clear_probability": None,
                "forecast_uncertainty_bps": None,
                "observed_response_uncertainty_bps": observed_uncertainty,
                "pair_observation_count": pair_count,
                "expected_pair_count": int(expected_pair_counts[currency]),
                "coverage_ratio": round(
                    pair_count / max(1, expected_pair_counts[currency]), 6
                ),
                "components": _component_rows(
                    contract,
                    observed_value=observed,
                    observed_uncertainty=observed_uncertainty,
                    source_age_sec=source_age,
                    pair_count=pair_count,
                ),
                "abstention_reasons": [
                    "no_calibrated_forecast_component"
                    if observed is not None
                    else "currency_not_in_primary_fresh_component"
                ],
            }

        by_instrument = {row.instrument: row for row in observations}
        edge_rows: dict[str, Any] = {}
        for instrument in instruments:
            base, quote = instrument.split("_")
            base_value = _float(strengths.get(base))
            quote_value = _float(strengths.get(quote))
            observation = by_instrument.get(instrument)
            factor_value = (
                round(base_value - quote_value, 12)
                if base_value is not None and quote_value is not None
                else None
            )
            edge_uncertainty = (
                _edge_uncertainty(result, base, quote)
                if factor_value is not None
                else None
            )
            leave_one_out = None
            if observation is not None and contract["price_response"].get(
                "leave_one_edge_out"
            ):
                leave_one_out = _leave_one_edge_out(
                    observations,
                    instrument,
                    currencies=currencies,
                    policy=contract["price_response"],
                )
            residual = (
                round(observation.return_bps - factor_value, 12)
                if observation is not None and factor_value is not None
                else None
            )
            clipped_return = _float(
                (result.get("clipped_pair_returns_bps") or {}).get(instrument)
            )
            solver_residual = _float(
                (result.get("pair_residuals_clipped_bps") or {}).get(instrument)
            )
            edge_rows[instrument] = {
                "instrument": instrument,
                "base_currency": base,
                "quote_currency": quote,
                "horizon_sec": int(horizon_sec),
                "state": "observed_response_only" if observation is not None else "unavailable",
                "observed_pair_return_bps": (
                    observation.return_bps if observation is not None else None
                ),
                "observed_solver_clipped_return_bps": clipped_return,
                "observed_factor_move_bps": factor_value,
                "observed_residual_bps": residual,
                "observed_residual_basis": "raw_observed_return_minus_factor",
                "observed_solver_residual_bps": solver_residual,
                "observed_solver_residual_basis": "clipped_solver_return_minus_factor",
                "observed_return_was_clipped": (
                    not math.isclose(
                        float(observation.return_bps),
                        float(clipped_return),
                        abs_tol=1e-12,
                    )
                    if observation is not None and clipped_return is not None
                    else None
                ),
                "leave_one_edge_out_factor_bps": leave_one_out,
                "self_influence_bps": (
                    round(factor_value - leave_one_out, 12)
                    if factor_value is not None and leave_one_out is not None
                    else None
                ),
                "observed_edge_uncertainty_bps": edge_uncertainty,
                "forecast_mean_bps": None,
                "forecast_absolute_move_bps": None,
                "cost_clear_probability": None,
                "expected_net_pips": None,
                "allocator_rank": None,
                "bid": observation.bid if observation is not None else None,
                "ask": observation.ask if observation is not None else None,
                "pip": observation.pip if observation is not None else None,
                "spread_bps": observation.spread_bps if observation is not None else None,
                "start_epoch": observation.start_epoch if observation is not None else None,
                "end_epoch": observation.end_epoch if observation is not None else None,
                "actual_observation_duration_sec": (
                    observation.actual_observation_duration_sec
                    if observation is not None
                    else None
                ),
                "declared_horizon_sec": int(horizon_sec),
                "exact_horizon_observation": (
                    observation.actual_observation_duration_sec == int(horizon_sec)
                    if observation is not None
                    else None
                ),
                "endpoint_age_sec": (
                    observation.endpoint_age_sec if observation is not None else None
                ),
                "start_alignment_sec": (
                    observation.start_alignment_sec if observation is not None else None
                ),
                "observation_weight": observation.weight if observation is not None else None,
                "research_observable": observation is not None,
                "execution_eligible": False,
                "abstention_reasons": (
                    ["no_calibrated_forecast_component"]
                    if observation is not None
                    else ["missing_or_stale_pair_observation"]
                ),
            }
        horizons[str(int(horizon_sec))] = {
            "horizon_sec": int(horizon_sec),
            "decision_cutoff_utc": iso_utc(knowledge_cutoff),
            "completed_bar_cutoff_utc": iso_utc(complete_minute_cutoff),
            "solver": result,
            "rejected_observation_counts": rejected,
            "currencies": currency_rows,
            "pair_edges": edge_rows,
        }

    refs = dict(input_refs or {})
    stable_input_identity = _stable_input_identity(refs)
    snapshot_material = {
        "contract_id": contract["contract_id"],
        "contract_sha256": contract.get("contract_sha256"),
        "decision_cutoff_utc": iso_utc(knowledge_cutoff),
        "completed_bar_cutoff_utc": iso_utc(complete_minute_cutoff),
        "input_identity": stable_input_identity,
        "horizons": horizons,
    }
    snapshot = {
        "schema_version": 1,
        "snapshot_schema": contract["snapshot_schema"],
        "snapshot_id": "currency_state_snapshot_" + stable_hash(snapshot_material)[:24],
        "contract_id": contract["contract_id"],
        "contract_sha256": contract.get("contract_sha256"),
        "decision_cutoff_utc": iso_utc(knowledge_cutoff),
        "completed_bar_cutoff_utc": iso_utc(complete_minute_cutoff),
        "event_time_watermark_utc": iso_utc(complete_minute_cutoff),
        "input_refs": refs,
        "input_identity": stable_input_identity,
        "currency_count": len(currencies),
        "instrument_count": len(instruments),
        "horizons": horizons,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
        "status": "diagnostic_ready",
        "limitations": [
            "latent price response is observed state, not a forward forecast",
            "official facts, causal consensus, intraday rate repricing, and semantic analog adapters are not connected",
            "no pair has calibrated after-cost expected value or allocator eligibility",
        ],
    }
    validate_snapshot(snapshot, contract=contract)
    return snapshot


def validate_snapshot(
    snapshot: Mapping[str, Any], *, contract: Mapping[str, Any]
) -> None:
    validate_contract(contract)
    if snapshot.get("research_only") is not True:
        raise ValueError("currency-state snapshot must remain research-only")
    if snapshot.get("execution_eligible") is not False:
        raise ValueError("currency-state snapshot cannot be execution eligible")
    if snapshot.get("can_place_orders") is not False:
        raise ValueError("currency-state snapshot cannot place orders")
    if snapshot.get("supported_execution_decision") != "no_trade":
        raise ValueError("currency-state snapshot must remain no_trade")
    expected_currencies = set(str(value) for value in contract["currencies"])
    expected_instruments = set(str(value) for value in contract["instruments"])
    expected_horizons = {str(int(value)) for value in contract["horizons_sec"]}
    horizons = snapshot.get("horizons") or {}
    if set(horizons) != expected_horizons:
        raise ValueError("snapshot horizons do not match the contract")
    for horizon, payload in horizons.items():
        currencies = payload.get("currencies") or {}
        edges = payload.get("pair_edges") or {}
        if set(currencies) != expected_currencies:
            raise ValueError(f"horizon {horizon} does not emit all 21 currencies")
        if set(edges) != expected_instruments:
            raise ValueError(f"horizon {horizon} does not emit all 68 edges")
        for instrument, edge in edges.items():
            if edge.get("execution_eligible") is not False:
                raise ValueError(f"edge {instrument} became execution eligible")
            base, quote = instrument.split("_")
            base_value = currencies[base].get("observed_currency_return_bps")
            quote_value = currencies[quote].get("observed_currency_return_bps")
            factor = edge.get("observed_factor_move_bps")
            if base_value is None or quote_value is None:
                if factor is not None:
                    raise ValueError(f"edge {instrument} has an invented factor")
            elif factor is None or not math.isclose(
                float(factor), float(base_value) - float(quote_value), abs_tol=1e-9
            ):
                raise ValueError(f"edge {instrument} violates base-minus-quote algebra")


__all__ = [
    "PairObservation",
    "build_currency_state_snapshot",
    "iso_utc",
    "pair_observations_from_history",
    "parse_epoch",
    "solve_weighted_currency_state",
    "validate_snapshot",
]
