#!/usr/bin/env python3
"""Statistical helpers for chronological, cost-aware Forex validation."""

from __future__ import annotations

import math
import statistics
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence


EULER_MASCHERONI = 0.5772156649015329


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(float(value) / math.sqrt(2.0)))


def normal_ppf(probability: float) -> float:
    bounded = min(1.0 - 1e-12, max(1e-12, float(probability)))
    return statistics.NormalDist().inv_cdf(bounded)


def timestamp_epoch(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def independent_time_block_means(
    records: Iterable[dict[str, Any]],
    *,
    value_key: str = "endpoint_pips",
    horizon_sec: int | None = None,
) -> list[float]:
    """Collapse overlapping outcomes into non-overlapping maturity-sized blocks."""

    buckets: dict[int, list[float]] = {}
    fallback: list[float] = []
    for row in records:
        value = finite(row.get(value_key), math.nan)
        if not math.isfinite(value):
            continue
        epoch = timestamp_epoch(row.get("entry_time"))
        horizon = max(1, int(horizon_sec or finite(row.get("horizon_sec"), 1.0)))
        if epoch is None:
            fallback.append(value)
            continue
        bucket = int(epoch // horizon)
        buckets.setdefault(bucket, []).append(value)
    output = [statistics.fmean(buckets[key]) for key in sorted(buckets)]
    if output:
        return output
    return fallback


def purged_before_boundary(
    records: Sequence[dict[str, Any]],
    boundary_epoch: float,
    horizon_sec: int,
) -> list[dict[str, Any]]:
    """Keep only records whose labels mature strictly before a split boundary."""

    horizon = max(1, int(horizon_sec))
    kept: list[dict[str, Any]] = []
    for row in records:
        epoch = timestamp_epoch(row.get("entry_time"))
        if epoch is None or epoch + horizon < boundary_epoch:
            kept.append(row)
    return kept


def _sample_moments(values: Sequence[float]) -> tuple[float, float, float, float]:
    sample = [float(value) for value in values if math.isfinite(float(value))]
    if not sample:
        return 0.0, 0.0, 0.0, 3.0
    mean = statistics.fmean(sample)
    if len(sample) < 2:
        return mean, 0.0, 0.0, 3.0
    stdev = statistics.stdev(sample)
    if stdev <= 1e-12:
        return mean, stdev, 0.0, 3.0
    centered = [(value - mean) / stdev for value in sample]
    skew = statistics.fmean(value**3 for value in centered)
    kurtosis = statistics.fmean(value**4 for value in centered)
    return mean, stdev, skew, max(1.0, kurtosis)


def deflated_sharpe_probability(
    values: Sequence[float],
    *,
    trial_count: int = 1,
) -> dict[str, float | int | None]:
    """Return a conservative, unannualized DSR-style selection probability.

    The reference Sharpe is the expected maximum null Sharpe over the tested
    variants. Values should already be collapsed into approximately independent
    time blocks; this avoids pretending overlapping horizon outcomes are IID.
    """

    sample = [float(value) for value in values if math.isfinite(float(value))]
    count = len(sample)
    trials = max(1, int(trial_count))
    mean, stdev, skew, kurtosis = _sample_moments(sample)
    if count < 3 or stdev <= 1e-12:
        probability = 1.0 if count >= 3 and mean > 0.0 and stdev <= 1e-12 else 0.0
        return {
            "independent_n": count,
            "trial_count": trials,
            "observed_sharpe": None if stdev <= 1e-12 else mean / stdev,
            "deflated_sharpe_reference": None,
            "deflated_sharpe_probability": probability,
            "skew": round(skew, 6),
            "kurtosis": round(kurtosis, 6),
        }
    observed = mean / stdev
    if trials == 1:
        expected_max_standard_normal = 0.0
    else:
        expected_max_standard_normal = (
            (1.0 - EULER_MASCHERONI) * normal_ppf(1.0 - 1.0 / trials)
            + EULER_MASCHERONI
            * normal_ppf(1.0 - 1.0 / (trials * math.e))
        )
    reference = expected_max_standard_normal / math.sqrt(max(1.0, count - 1.0))
    denominator = max(
        1e-12,
        1.0 - skew * observed + ((kurtosis - 1.0) / 4.0) * observed * observed,
    )
    statistic = (
        (observed - reference) * math.sqrt(count - 1.0) / math.sqrt(denominator)
    )
    return {
        "independent_n": count,
        "trial_count": trials,
        "observed_sharpe": round(observed, 8),
        "deflated_sharpe_reference": round(reference, 8),
        "deflated_sharpe_probability": round(normal_cdf(statistic), 8),
        "skew": round(skew, 6),
        "kurtosis": round(kurtosis, 6),
    }


def multiple_testing_metrics(
    values: Sequence[float],
    *,
    trial_count: int = 1,
    family_alpha: float = 0.05,
) -> dict[str, float | int | None]:
    """Compute mean uncertainty with Bonferroni and DSR-style corrections."""

    sample = [float(value) for value in values if math.isfinite(float(value))]
    count = len(sample)
    trials = max(1, int(trial_count))
    if not sample:
        return {
            "independent_n": 0,
            "trial_count": trials,
            "mean_pips": None,
            "standard_error_pips": None,
            "one_sided_p_value": 1.0,
            "selection_adjusted_lower_mean_pips": None,
            "selection_adjusted_upper_mean_pips": None,
            **deflated_sharpe_probability([], trial_count=trials),
        }
    mean = statistics.fmean(sample)
    if count < 2:
        standard_error = math.inf
        lower = -math.inf
        upper = math.inf
        p_value = 1.0
    else:
        stdev = statistics.stdev(sample)
        standard_error = stdev / math.sqrt(count)
        if standard_error <= 1e-12:
            p_value = 0.0 if mean > 0.0 else 1.0
            lower = mean
            upper = mean
        else:
            p_value = 1.0 - normal_cdf(mean / standard_error)
            tail_alpha = min(0.499999, max(1e-12, family_alpha / trials))
            z_value = normal_ppf(1.0 - tail_alpha)
            lower = mean - z_value * standard_error
            upper = mean + z_value * standard_error
    dsr = deflated_sharpe_probability(sample, trial_count=trials)
    return {
        "independent_n": count,
        "trial_count": trials,
        "mean_pips": round(mean, 8),
        "standard_error_pips": (
            None if not math.isfinite(standard_error) else round(standard_error, 8)
        ),
        "one_sided_p_value": round(p_value, 10),
        "selection_adjusted_lower_mean_pips": (
            None if not math.isfinite(lower) else round(lower, 8)
        ),
        "selection_adjusted_upper_mean_pips": (
            None if not math.isfinite(upper) else round(upper, 8)
        ),
        **dsr,
    }


def benjamini_hochberg(p_values: Sequence[float]) -> list[float]:
    """Return monotone Benjamini-Hochberg q-values in original order."""

    count = len(p_values)
    if not count:
        return []
    ordered = sorted(
        enumerate(min(1.0, max(0.0, finite(value, 1.0))) for value in p_values),
        key=lambda item: item[1],
    )
    adjusted = [1.0] * count
    running = 1.0
    for rank_index in range(count - 1, -1, -1):
        original_index, value = ordered[rank_index]
        rank = rank_index + 1
        running = min(running, value * count / rank)
        adjusted[original_index] = min(1.0, running)
    return adjusted


__all__ = [
    "benjamini_hochberg",
    "deflated_sharpe_probability",
    "finite",
    "independent_time_block_means",
    "multiple_testing_metrics",
    "normal_cdf",
    "normal_ppf",
    "purged_before_boundary",
    "timestamp_epoch",
]
