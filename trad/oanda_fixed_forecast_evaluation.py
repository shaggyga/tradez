"""Offline fixed forecast comparison; no worker, database or broker imports.

Inputs must retain original forecast and quote clocks. This evaluator never
promotes a strategy. Even valid input-clock assertions are engineering evidence,
not independently attested prospective publication or independent samples.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics
from typing import Any

try:
    from oanda_causal_prediction_baselines import decide_baselines
except ModuleNotFoundError:
    from trad.oanda_causal_prediction_baselines import decide_baselines

SCHEMA = "fixed_forecast_evaluation_input_v1"
PROTOCOL_SCHEMA = "fixed_forecast_evaluation_protocol_v1"


def finite(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("missing_or_non_numeric_value")
    try:
        converted = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError("nonfinite_value") from exc
    if not math.isfinite(converted):
        raise ValueError("nonfinite_value")
    return converted


def epoch(value: str) -> float:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone_required")
    return parsed.timestamp()


def content_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()


def validate_protocol(protocol: dict) -> None:
    if protocol.get("schema_version") != PROTOCOL_SCHEMA:
        raise ValueError("protocol_schema")
    for flag in ("proof_eligible", "account_eligible", "collection_enabled"):
        if protocol.get(flag) is not False:
            raise ValueError("offline_only_protocol_required")
    if protocol.get("instrument") != "EUR_USD" or protocol.get("horizon_sec") != 3600:
        raise ValueError("fixed_pair_and_horizon_required")
    if len(protocol.get("cohorts", {})) != 4 or not all(protocol["cohorts"].values()):
        raise ValueError("four_exact_cohorts_required")
    if protocol.get("baselines") != ["fair_coin", "zero_move", "no_trade", "rolling_class_rate"]:
        raise ValueError("fixed_baselines_required")
    if epoch(protocol["historical_start_utc"]) >= epoch(protocol["historical_end_utc"]):
        raise ValueError("invalid_evaluation_window")
    for key in ("quote_max_age_sec", "maximum_entry_delay_sec", "maximum_target_quote_delay_sec"):
        if finite(protocol[key]) <= 0:
            raise ValueError("positive_tolerances_required")
    if not protocol.get("extra_cost_stress_bps") or any(finite(x) < 0 for x in protocol["extra_cost_stress_bps"]):
        raise ValueError("nonnegative_cost_stress_required")


def forecast_errors(forecast: dict, decision: dict, protocol: dict) -> list[str]:
    errors = []
    family = forecast.get("family")
    for field, expected in (
        ("instrument", protocol["instrument"]), ("horizon_sec", protocol["horizon_sec"]),
        ("input_timeframe", protocol["input_timeframe"]),
        ("cohort_id", protocol["cohorts"].get(family)),
        ("model_version", protocol["model_version"]),
        ("feature_version", protocol["feature_version"]),
    ):
        if expected is None or forecast.get(field) != expected:
            errors.append("identity_mismatch:"+field)
    clocks = {}
    for field in ("issued_epoch", "committed_available_epoch", "feature_cutoff_epoch",
                  "features_available_epoch", "training_label_maturity_max_epoch",
                  "training_labels_available_max_epoch", "reference_epoch", "target_epoch"):
        try:
            clocks[field] = finite(forecast.get(field))
        except ValueError:
            errors.append("missing_or_invalid_clock:"+field)
    try:
        decision_reference = finite(decision.get("reference_epoch"))
        decision_target = finite(decision.get("target_epoch"))
    except ValueError:
        errors.append("missing_or_invalid_decision_clock")
        decision_reference = decision_target = None
    if len(clocks) == 8 and decision_reference is not None:
        issue = clocks["issued_epoch"]
        if not decision_reference <= issue <= clocks["committed_available_epoch"] < decision_target:
            errors.append("issue_publication_target_order")
        if not clocks["feature_cutoff_epoch"] <= clocks["features_available_epoch"] <= issue:
            errors.append("features_unavailable_at_issue")
        if not clocks["training_label_maturity_max_epoch"] <= clocks["training_labels_available_max_epoch"] < issue:
            errors.append("training_labels_unavailable_at_issue")
        if clocks["reference_epoch"] != decision_reference or clocks["target_epoch"] != decision_target:
            errors.append("original_forecast_target_mismatch")
    try:
        if not 0 <= finite(forecast.get("probability_up")) <= 1:
            errors.append("probability_out_of_range")
        finite(forecast.get("predicted_return_bps"))
    except ValueError:
        errors.append("invalid_prediction")
    if not isinstance(forecast.get("forecast_id"), str) or not forecast["forecast_id"]:
        errors.append("missing_forecast_id")
    if type(forecast.get("side")) is not int or forecast["side"] not in (-1, 0, 1):
        errors.append("invalid_emitted_side")
    try:
        if finite(forecast.get("reference_mid")) <= 0:
            raise ValueError("nonpositive_reference_mid")
    except ValueError:
        errors.append("invalid_original_reference_mid")
    return errors


def validate_quotes(rows: list[dict], cutoff: float, protocol: dict) -> tuple[list[dict], Counter]:
    valid, excluded, identities = [], Counter(), {}
    for row in rows:
        try:
            q = dict(row)
            if not isinstance(q.get("quote_id"), str) or not q["quote_id"]:
                raise ValueError("missing_quote_id")
            # Conflicting identities are an input-integrity failure even when
            # one of the conflicting records also has invalid prices/clocks.
            # Check before validity filtering so order cannot select a winner.
            canonical_fields = ("quote_id", "instrument", "market_epoch", "available_epoch", "bid", "ask", "tradeable")
            identity_record = {key:q.get(key) for key in canonical_fields}
            if q["quote_id"] in identities:
                if identities[q["quote_id"]] != identity_record:
                    raise ValueError("conflicting_quote_identity")
                continue
            identities[q["quote_id"]] = identity_record
            for field in ("market_epoch", "available_epoch", "bid", "ask"):
                q[field] = finite(q.get(field))
            if q.get("instrument") != protocol["instrument"] or q.get("tradeable") is not True:
                raise ValueError("wrong_instrument_or_not_tradeable")
            if not 0 < q["bid"] <= q["ask"]:
                raise ValueError("invalid_bid_ask")
            if not q["market_epoch"] <= q["available_epoch"] <= cutoff:
                raise ValueError("invalid_quote_availability")
            if q["available_epoch"]-q["market_epoch"] > protocol["quote_max_age_sec"]:
                raise ValueError("stale_quote")
            canonical = {k:q[k] for k in canonical_fields}
            valid.append(canonical)
        except (TypeError, ValueError) as exc:
            if str(exc) == "conflicting_quote_identity":
                raise
            excluded[str(exc)] += 1
    return sorted(valid, key=lambda q:(q["available_epoch"],q["market_epoch"],q["quote_id"])), excluded


def select_entry(quotes: list[dict], decision_epoch: float, target: float, protocol: dict) -> dict | None:
    return next((q for q in quotes
                 if decision_epoch < q["available_epoch"] < target
                 and q["market_epoch"] > decision_epoch
                 and q["available_epoch"]-decision_epoch <= protocol["maximum_entry_delay_sec"]), None)


def select_target(quotes: list[dict], target: float, protocol: dict) -> dict | None:
    return next((q for q in quotes
                 if target <= q["market_epoch"] <= q["available_epoch"] <= target+protocol["maximum_target_quote_delay_sec"]), None)


def side(probability: float) -> int:
    return 1 if probability > .5 else -1 if probability < .5 else 0


def scored_prediction(probability: float, predicted_bps: float | None, actual_bps: float,
                      entry: dict, target: dict, stress: list[float], *, direction: int | None = None) -> dict:
    probability_direction = side(probability)
    direction = probability_direction if direction is None else direction
    denominator = (entry["bid"]+entry["ask"])/2
    raw_net = (target["bid"]-entry["ask"] if direction > 0 else
               entry["bid"]-target["ask"] if direction < 0 else 0)
    net = 10000*raw_net/denominator
    error = None if predicted_bps is None else predicted_bps-actual_bps
    return {"probability_up":probability, "probability_direction":probability_direction,
            "side":direction, "emitted_side_differs_from_probability_direction":direction != probability_direction,
            "brier":(probability-int(actual_bps > 0))**2,
            "direction_correct":bool(direction and direction*actual_bps > 0),
            "absolute_error_bps":None if error is None else abs(error),
            "squared_error_bps":None if error is None else error*error,
            "net_bps":net, "positive_after_spread":bool(direction and net > 0),
            "stress_net_bps":{str(cost):net-cost*bool(direction) for cost in stress}}


def summarize(rows: list[dict]) -> dict:
    active = [row for row in rows if row["side"]]
    errors = [row for row in rows if row["absolute_error_bps"] is not None]
    mean = lambda values: statistics.fmean(values) if values else None
    return {"decisions":len(rows), "directional_decisions":len(active),
            "abstentions":len(rows)-len(active),
            "mean_brier":mean([r["brier"] for r in rows]),
            "direction_hit_rate_when_directional":mean([r["direction_correct"] for r in active]),
            "positive_after_spread_rate_when_directional":mean([r["positive_after_spread"] for r in active]),
            "mean_net_bps_per_decision":mean([r["net_bps"] for r in rows]),
            "mae_bps":mean([r["absolute_error_bps"] for r in errors]),
            "rmse_bps":math.sqrt(statistics.fmean([r["squared_error_bps"] for r in errors])) if errors else None,
            "stress_mean_net_bps_per_decision":{
                cost:mean([r["stress_net_bps"][cost] for r in rows])
                for cost in (rows[0]["stress_net_bps"] if rows else {})}}


def evaluate(dataset: dict, protocol: dict) -> dict:
    validate_protocol(protocol)
    if dataset.get("schema_version") != SCHEMA:
        raise ValueError("input_schema")
    cutoff = finite(dataset.get("observed_cutoff_epoch"))
    quotes, quote_exclusions = validate_quotes(dataset.get("quotes",[]), cutoff, protocol)
    quote_by_id = {q["quote_id"]:q for q in quotes}
    families = sorted(protocol["cohorts"])
    window_start, window_end = epoch(protocol["historical_start_utc"]), epoch(protocol["historical_end_utc"])
    funnel = Counter(total_decisions=0, total_forecasts=0, complete_four_family_decisions=0,
                     complete_clock_valid_decisions=0, paired_scored_decisions=0)
    family_coverage = {f:Counter(issued=0, clock_valid=0, paired_scored=0) for f in families}
    seen_decisions, seen_references, seen_forecasts = set(), set(), set()
    exclusions, prepared, labels = [], [], []
    for original in dataset.get("decisions",[]):
        decision = dict(original)
        identity = decision.get("decision_id")
        if not isinstance(identity,str) or not identity or identity in seen_decisions:
            raise ValueError("missing_or_duplicate_decision_id")
        seen_decisions.add(identity)
        funnel["total_decisions"] += 1
        reason, errors_by_family = [], {}
        try:
            reference = finite(decision.get("reference_epoch"))
            target_epoch = finite(decision.get("target_epoch"))
            if reference in seen_references:
                raise ValueError("duplicate_market_reference_epoch")
            seen_references.add(reference)
            if not window_start <= reference <= window_end:
                reason.append("reference_outside_fixed_window")
            if target_epoch != reference+protocol["horizon_sec"]:
                reason.append("target_shifted_from_original_reference")
        except ValueError as exc:
            if str(exc) == "duplicate_market_reference_epoch":
                raise
            reason.append("missing_or_invalid_reference_target")
        forecasts = decision.get("forecasts",[])
        funnel["total_forecasts"] += len(forecasts)
        grouped = {family:[f for f in forecasts if f.get("family")==family] for family in families}
        if len(forecasts) != 4 or any(len(grouped[f]) != 1 for f in families):
            reason.append("missing_duplicate_or_unexpected_family")
        else:
            funnel["complete_four_family_decisions"] += 1
        for forecast in forecasts:
            fid = forecast.get("forecast_id")
            if fid and fid in seen_forecasts:
                raise ValueError("duplicate_forecast_id")
            if fid: seen_forecasts.add(fid)
            family = forecast.get("family")
            if family not in family_coverage:
                continue
            family_coverage[family]["issued"] += 1
            errors = forecast_errors(forecast, decision, protocol)
            if errors:
                errors_by_family.setdefault(family,[]).extend(errors)
            else:
                family_coverage[family]["clock_valid"] += 1
        if errors_by_family:
            reason.append("forecast_clock_identity_or_value_invalid")
        if reason:
            exclusions.append({"decision_id":identity,"reasons":reason,"forecast_errors":errors_by_family})
            continue
        funnel["complete_clock_valid_decisions"] += 1
        by_family = {f:grouped[f][0] for f in families}
        publication = max(f["committed_available_epoch"] for f in forecasts)
        issue_cutoff = min(f["issued_epoch"] for f in forecasts)
        reference_quote = quote_by_id.get(decision.get("reference_quote_id"))
        if (reference_quote is None or reference_quote["market_epoch"] > reference
            or reference_quote["available_epoch"] > issue_cutoff
            or reference-reference_quote["market_epoch"] > protocol["quote_max_age_sec"]):
            reason.append("missing_or_unavailable_reference_quote")
        elif any(not math.isclose(forecast["reference_mid"],
                    (reference_quote["bid"]+reference_quote["ask"])/2,
                    rel_tol=0.0, abs_tol=1e-12) for forecast in forecasts):
            reason.append("forecast_reference_price_mismatch")
        entry = select_entry(quotes,publication,target_epoch,protocol)
        target = select_target(quotes,target_epoch,protocol)
        if entry is None: reason.append("missing_postpublication_entry_quote")
        if target is None: reason.append("missing_original_target_quote")
        if reason:
            exclusions.append({"decision_id":identity,"reasons":reason,"forecast_errors":{}})
            continue
        reference_mid = (reference_quote["bid"]+reference_quote["ask"])/2
        target_mid = (target["bid"]+target["ask"])/2
        actual = 10000*(target_mid-reference_mid)/reference_mid
        event_id = f'{protocol["instrument"]}:{reference:.6f}:{target_epoch:.6f}'
        labels.append({"event_id":event_id,"instrument":protocol["instrument"],
                       "horizon_sec":protocol["horizon_sec"],"target_epoch":target_epoch,
                       "available_epoch":target["available_epoch"],"up":actual > 0})
        prepared.append({"decision_id":identity,"reference_epoch":reference,"target_epoch":target_epoch,
                         "baseline_issue_cutoff_epoch":issue_cutoff,"common_decision_epoch":publication,
                         "actual_return_bps":actual,"entry":entry,"target":target,"forecasts":by_family})

    results, samples = [], {f:[] for f in families+protocol["baselines"]}
    for row in sorted(prepared,key=lambda r:(r["reference_epoch"],r["decision_id"])):
        base = decide_baselines(row["baseline_issue_cutoff_epoch"],labels,
                                lookback=protocol["rolling_lookback"],min_labels=protocol["rolling_min_labels"],
                                instrument=protocol["instrument"],horizon_sec=protocol["horizon_sec"])
        scores = {}
        for family, forecast in row["forecasts"].items():
            scores[family] = scored_prediction(forecast["probability_up"],forecast["predicted_return_bps"],
                row["actual_return_bps"],row["entry"],row["target"],protocol["extra_cost_stress_bps"],
                direction=forecast["side"])
            scores[family]["forecast_id"] = forecast["forecast_id"]
            scores[family]["abstention_reason"] = forecast.get("abstention_reason")
            family_coverage[family]["paired_scored"] += 1
        for name in protocol["baselines"]:
            probability = base[name].get("probability_up",.5)
            predicted = 0.0 if name == "zero_move" else None
            scores[name] = scored_prediction(probability,predicted,row["actual_return_bps"],
                row["entry"],row["target"],protocol["extra_cost_stress_bps"])
            # Probability and magnitude baselines have no economic trading arm.
            # no_trade has no probability/magnitude forecast interpretation.
        for name, score in scores.items(): samples[name].append(score)
        results.append({k:v for k,v in row.items() if k != "forecasts"} | {
            "scores":scores,"rolling_baseline_training":base["rolling_class_rate"],
            "actual_holding_sec":row["target"]["market_epoch"]-row["entry"]["market_epoch"]})
    funnel["paired_scored_decisions"] = len(results)
    summaries = {name:summarize(rows) for name,rows in samples.items()}
    # Do not imply that zero-move/no-trade are probabilistic forecasts.
    for name in ("zero_move","no_trade"):
        summaries[name]["mean_brier"] = None
    for row in results:
        for name in ("zero_move","no_trade"):
            row["scores"][name]["brier"] = None
    paired_deltas = {}
    for family in families:
        paired_deltas[family] = {
            "decisions":len(results),
            "mean_brier_minus_fair_coin":statistics.fmean(r["scores"][family]["brier"]-.25 for r in results) if results else None,
            "mean_brier_minus_rolling_class_rate":statistics.fmean(r["scores"][family]["brier"]-r["scores"]["rolling_class_rate"]["brier"] for r in results) if results else None,
            "mae_bps_minus_zero_move":statistics.fmean(r["scores"][family]["absolute_error_bps"]-r["scores"]["zero_move"]["absolute_error_bps"] for r in results) if results else None,
            "mean_net_bps_minus_no_trade":summaries[family]["mean_net_bps_per_decision"]}
    reasons = Counter(reason for row in exclusions for reason in row["reasons"])
    return {"schema_version":"fixed_forecast_evaluation_report_v1", "generated_utc":datetime.now(timezone.utc).isoformat(),
            "contract_id":protocol["contract_id"],"protocol_sha256":content_hash(protocol),"input_sha256":content_hash(dataset),
            "status":"clock_valid_engineering_diagnostic" if results else "no_clock_valid_paired_decisions",
            "proof_eligible":False,"account_eligible":False,"runtime_started":False,
            "coverage":dict(funnel),"per_family_coverage":{k:dict(v) for k,v in family_coverage.items()},
            "quote_exclusions":dict(quote_exclusions),"decision_exclusion_counts":dict(reasons),"exclusions":exclusions,
            "paired_summaries":summaries,"paired_deltas":paired_deltas,"decisions":results,
            "outcome_counts":dict(Counter("up" if r["actual_return_bps"] > 0 else "down" if r["actual_return_bps"] < 0 else "flat" for r in results)),
            "observed_utc_day_blocks":len({int(r["reference_epoch"])//86400 for r in results}),
            "independent_sample_size":None,
            "limitations":["Input clocks are internally checked assertions; not independent proof of past availability.",
                "Historical scope was chosen after inspection and cannot become prospective confirmation.",
                "Only paired common decisions are scored; missing forecasts and quotes remain visible in coverage.",
                "Rolling baseline uses unique valid paired target events with labels available strictly before earliest issue.",
                "Bid/ask and explicit stress only; no financing, guaranteed fills or account-currency portfolio P/L.",
                "Overlapping forecasts and UTC blocks do not establish independent trials.",
                "No automatic model selection, retraining, promotion, collection or execution is provided."]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input",required=True,type=Path)
    parser.add_argument("--protocol",required=True,type=Path)
    parser.add_argument("--output",required=True,type=Path)
    args = parser.parse_args()
    source = Path(__file__).resolve().parent
    output = args.output.resolve()
    if output.is_relative_to(source/"data") or output.is_relative_to(source/"config"):
        raise SystemExit("Choose a disposable review output outside runtime data/config.")
    if output in {args.input.resolve(),args.protocol.resolve()} or output.exists():
        raise SystemExit("Output must be a new file distinct from inputs.")
    dataset = json.loads(args.input.read_text(encoding="utf-8-sig"))
    protocol = json.loads(args.protocol.read_text(encoding="utf-8-sig"))
    result = evaluate(dataset,protocol)
    output.parent.mkdir(parents=True,exist_ok=True)
    with output.open("x",encoding="utf-8") as handle:
        json.dump(result,handle,indent=2,allow_nan=False)
        handle.write("\n")
    print(json.dumps({"output":str(output),"status":result["status"],"coverage":result["coverage"],"proof_eligible":False}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
