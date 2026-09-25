"""Descriptive event-sampled currency exposure for one preserved policy path."""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal


def _gross(snapshot):
    result = {}
    for currency, item in snapshot["currency_exposures"].items():
        amount = Decimal(item["gross_usd_conservative"])
        if amount < 0:
            raise ValueError("negative_currency_gross_exposure")
        conversion = item["conversion"]
        if conversion["currency"] != currency:
            raise ValueError("exposure_conversion_currency_mismatch")
        result[currency] = amount
    return result


def summarize_event_exposures(events, arms, *, terminal_epoch):
    """Hold the final state at each event epoch until the next observed event epoch."""
    if not events or not arms:
        raise ValueError("nonempty_event_population_required")
    ordered = sorted(events, key=lambda x: (x["epoch"], x["sequence"]))
    keys = [(x["epoch"], x["sequence"]) for x in ordered]
    if keys != sorted(set(keys)) or terminal_epoch < ordered[-1]["epoch"]:
        raise ValueError("event_order_or_terminal_mismatch")
    first = ordered[0]["epoch"]
    if terminal_epoch <= first:
        raise ValueError("positive_observed_span_required")
    by_epoch = {}
    for event in ordered:
        if set(event["arms"]) != set(arms):
            raise ValueError("all_declared_arms_required")
        by_epoch[event["epoch"]] = event  # last sequence at this epoch is the post-event state
    epochs = sorted(by_epoch)
    if epochs[-1] != terminal_epoch:
        raise ValueError("explicit_terminal_event_required")
    span = terminal_epoch - first
    result = {}
    for arm in arms:
        weighted = defaultdict(Decimal)
        active_seconds = defaultdict(int)
        peak = defaultdict(Decimal)
        peak_combined = Decimal(0)
        active_portfolio_seconds = 0
        for start, end in zip(epochs, epochs[1:]):
            duration = end - start
            if duration <= 0:
                raise ValueError("nonpositive_exposure_interval")
            snapshot = by_epoch[start]["arms"][arm]
            gross = _gross(snapshot)
            if gross:
                active_portfolio_seconds += duration
            peak_combined = max(peak_combined, sum(gross.values(), Decimal(0)))
            for currency, amount in gross.items():
                weighted[currency] += amount * duration
                if amount > 0:
                    active_seconds[currency] += duration
                peak[currency] = max(peak[currency], amount)
        result[arm] = {
            "active_portfolio_seconds": active_portfolio_seconds,
            "peak_combined_gross_usd": str(peak_combined),
            "currencies": {currency: {
                "time_weighted_mean_gross_usd_over_full_span": str(weighted[currency] / span),
                "active_seconds": active_seconds[currency],
                "peak_gross_usd": str(peak[currency]),
            } for currency in sorted(weighted)},
        }
    return {"schema": "forex.event_sampled_currency_exposure.v1",
            "first_epoch": first, "terminal_epoch": terminal_epoch,
            "observed_span_seconds": span, "event_rows": len(ordered),
            "distinct_event_epochs": len(epochs), "arms": result,
            "assumption": "post-event exposure held constant until the next recorded event; no intrainterval mark or unseen-market inference"}
