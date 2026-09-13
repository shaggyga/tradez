"""One-shot construction of a separate offline evaluator; old source is read-only."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / 'trad'
text = (ROOT/'oanda_fixed_forecast_evaluation.py').read_text(encoding='utf-8')


def replace(old, new):
    global text
    assert text.count(old) == 1, (old[:100], text.count(old))
    text = text.replace(old, new)


replace('"""Offline fixed forecast comparison; no worker, database or broker imports.',
        '"""Separate exact-price offline evaluator v2; no worker, database or broker imports.')
replace('import statistics\n', 'from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext\n')
replace('    from oanda_causal_prediction_baselines import decide_baselines',
        '    from oanda_causal_prediction_baselines import decide_baselines\n    from oanda_exact_price_scoring import (METRIC_PRECISION, SCORING_VERSION, decimal_value,\n        quote_midpoint, score_prediction)')
replace('    from trad.oanda_causal_prediction_baselines import decide_baselines',
        '    from trad.oanda_causal_prediction_baselines import decide_baselines\n    from trad.oanda_exact_price_scoring import (METRIC_PRECISION, SCORING_VERSION, decimal_value,\n        quote_midpoint, score_prediction)')
replace('SCHEMA = "fixed_forecast_evaluation_input_v1"', '''EVALUATOR_VERSION = "fixed_forecast_evaluation_exact_v2_20260906"
# Compatibility schemas describe the input evidence and offline clock protocol;
# they do not authorize this evaluator for a registered/live collection.
SCHEMA = "fixed_forecast_evaluation_input_v1"''')
replace('def content_hash(value: Any) -> str:\n    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()', '''def jsonable(value: Any) -> Any:
    """Decimal metrics/prices become strings; finite native clock floats remain numbers."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("nonfinite_decimal_serialization")
        return str(value)
    if isinstance(value, dict):
        if any(type(key) is not str for key in value):
            raise ValueError("non_string_json_key")
        return {key:jsonable(item) for key,item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if type(value) is float and not math.isfinite(value):
        raise ValueError("nonfinite_float_serialization")
    if value is None or type(value) in (str, int, float, bool):
        return value
    raise ValueError("unsupported_json_value")


def loads_exact_prices(text: str) -> Any:
    """Preserve monetary/probability JSON numbers; retain v1 float clock semantics."""
    precise_fields = {"bid", "ask", "reference_mid", "probability_up", "predicted_return_bps",
                      "pip_size", "expected_signed_pips", "extra_cost_stress_bps"}
    def restore(value, key=None):
        if isinstance(value, dict):
            return {k:restore(v, k) for k,v in value.items()}
        if isinstance(value, list):
            return [restore(v, key) for v in value]
        if isinstance(value, Decimal):
            return value if key in precise_fields else float(value)
        return value
    return restore(json.loads(text, parse_float=Decimal))


def decimal_mean(values) -> Decimal | None:
    values = list(values)
    if not values:
        return None
    with localcontext(Context(prec=METRIC_PRECISION, rounding=ROUND_HALF_EVEN)):
        return sum((Decimal(int(v)) if type(v) is bool else decimal_value(v) for v in values), Decimal(0))/len(values)


def exact_baselines(issue_epoch, labels, **kwargs):
    # Reuse only the pure baseline's identity/clock selection. Its selected
    # labels were rebuilt from exact quote outcomes below. Reconstruct the
    # smoothed probability from integer counts, without retaining float ratios.
    result = decide_baselines(issue_epoch, labels, **kwargs)
    rolling = result["rolling_class_rate"]
    up_by_event = {row["event_id"]:row["up"] for row in labels}
    with localcontext(Context(prec=METRIC_PRECISION, rounding=ROUND_HALF_EVEN)):
        probability = (Decimal('.5') if rolling["warmup_fallback"] else
            Decimal(sum(up_by_event[event] for event in rolling["training_event_ids"])+1) /
            Decimal(rolling["n_training_labels"]+2))
    rolling["probability_up"] = probability
    rolling["side"] = side(probability)
    result["fair_coin"]["probability_up"] = Decimal('.5')
    result["zero_move"]["predicted_return_bps"] = Decimal(0)
    return result


def content_hash(value: Any) -> str:
    # Semantic in-memory evidence hash, with Decimal values represented as
    # strings. The CLI additionally binds exact input/protocol file bytes.
    raw = json.dumps(jsonable(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()''')
replace('    if not protocol.get("extra_cost_stress_bps") or any(finite(x) < 0 for x in protocol["extra_cost_stress_bps"]):\n        raise ValueError("nonnegative_cost_stress_required")', '''    if not protocol.get("extra_cost_stress_bps"):
        raise ValueError("nonnegative_cost_stress_required")
    score_prediction({"bid":1,"ask":1}, {"bid":1,"ask":1}, direction=0,
                     probability_up='.5', extra_cost_stress_bps=protocol["extra_cost_stress_bps"])''')
replace('if not 0 <= finite(forecast.get("probability_up")) <= 1:',
        'if not 0 <= decimal_value(forecast.get("probability_up")) <= 1:')
replace('        finite(forecast.get("predicted_return_bps"))',
        '        decimal_value(forecast.get("predicted_return_bps"))')
replace('        if finite(forecast.get("reference_mid")) <= 0:',
        '        if decimal_value(forecast.get("reference_mid")) <= 0:')
replace('            for field in ("market_epoch", "available_epoch", "bid", "ask"):\n                q[field] = finite(q.get(field))',
        '            for field in ("market_epoch", "available_epoch"):\n                q[field] = finite(q.get(field))\n            for field in ("bid", "ask"):\n                q[field] = decimal_value(q.get(field), field)')
start = text.index('def side(')
end = text.index('\ndef evaluate(', start)
text = text[:start] + '''def side(probability) -> int:
    probability = decimal_value(probability)
    return 1 if probability > Decimal('.5') else -1 if probability < Decimal('.5') else 0


def scored_prediction(probability, predicted_bps, reference, entry, target, stress, *, direction=None):
    direction = side(probability) if direction is None else direction
    exact = score_prediction(reference, target, entry_quote=entry, direction=direction,
        probability_up=probability, predicted_return_bps=predicted_bps, extra_cost_stress_bps=stress)
    return {"probability_up":exact["probability_up"], "probability_direction":exact["probability_direction"],
            "side":direction, "emitted_side_differs_from_probability_direction":exact["emitted_side_differs_from_probability_direction"],
            "brier":exact["brier_up_vs_not_up"], "brier_label_definition":"up_vs_not_up_flat_is_zero",
            "outcome_class":exact["outcome_class"], "up_label":exact["up_label"],
            "direction_correct":exact["direction_correct"],
            "absolute_error_bps":exact["absolute_error_bps"], "squared_error_bps":exact["squared_error_bps"],
            "net_price_move":exact["net_price_move"], "net_bps":exact["net_bps"],
            "positive_after_spread":exact["positive_after_spread"], "stress_net_bps":exact["stress_net_bps"]}


def summarize(rows: list[dict]) -> dict:
    active = [row for row in rows if row["side"]]
    errors = [row for row in rows if row["absolute_error_bps"] is not None]
    with localcontext(Context(prec=METRIC_PRECISION, rounding=ROUND_HALF_EVEN)):
        return {"decisions":len(rows), "directional_decisions":len(active),
                "abstentions":len(rows)-len(active),
                "mean_brier":decimal_mean(r["brier"] for r in rows),
                "direction_hit_rate_when_directional":decimal_mean(r["direction_correct"] for r in active),
                "positive_after_spread_rate_when_directional":decimal_mean(r["positive_after_spread"] for r in active),
                "mean_net_bps_per_decision":decimal_mean(r["net_bps"] for r in rows),
                "mae_bps":decimal_mean(r["absolute_error_bps"] for r in errors),
                "rmse_bps":decimal_mean(r["squared_error_bps"] for r in errors).sqrt() if errors else None,
                "stress_mean_net_bps_per_decision":{
                    cost:decimal_mean(r["stress_net_bps"][cost] for r in rows)
                    for cost in (rows[0]["stress_net_bps"] if rows else {})}}


def evaluate(dataset: dict, protocol: dict) -> dict:
    """Evaluate offline; metric Decimals are serialized explicitly by jsonable."""
    with localcontext(Context(prec=METRIC_PRECISION, rounding=ROUND_HALF_EVEN)):
        return _evaluate(dataset, protocol)

''' + text[end:]
replace('def evaluate(dataset: dict, protocol: dict) -> dict:\n    validate_protocol(protocol)',
        'def _evaluate(dataset: dict, protocol: dict) -> dict:\n    validate_protocol(protocol)')
replace('''        elif any(not math.isclose(forecast["reference_mid"],
                    (reference_quote["bid"]+reference_quote["ask"])/2,
                    rel_tol=0.0, abs_tol=1e-12) for forecast in forecasts):''',
        '''        elif any(decimal_value(forecast["reference_mid"]) != quote_midpoint(reference_quote)
                 for forecast in forecasts):''')
replace('''        reference_mid = (reference_quote["bid"]+reference_quote["ask"])/2
        target_mid = (target["bid"]+target["ask"])/2
        actual = 10000*(target_mid-reference_mid)/reference_mid''',
        '''        actual_score = score_prediction(reference_quote, target, direction=0, probability_up='.5')
        actual = actual_score["actual_return_bps"]''')
replace('"available_epoch":target["available_epoch"],"up":actual > 0}',
        '"available_epoch":target["available_epoch"],"up":bool(actual_score["up_label"])}')
replace('"actual_return_bps":actual,"entry":entry,"target":target,"forecasts":by_family}',
        '"actual_return_bps":actual,"actual_midpoint_move":actual_score["actual_midpoint_move"],\n                         "outcome_class":actual_score["outcome_class"],"reference_quote":reference_quote,\n                         "entry":entry,"target":target,"forecasts":by_family}')
replace('        base = decide_baselines(row["baseline_issue_cutoff_epoch"],labels,',
        '        base = exact_baselines(row["baseline_issue_cutoff_epoch"],labels,')
replace('row["actual_return_bps"],row["entry"],row["target"],protocol["extra_cost_stress_bps"],',
        'row["reference_quote"],row["entry"],row["target"],protocol["extra_cost_stress_bps"],')
replace('            probability = base[name].get("probability_up",.5)\n            predicted = 0.0 if name == "zero_move" else None',
        '            probability = base[name].get("probability_up",Decimal(".5"))\n            predicted = Decimal(0) if name == "zero_move" else None')
replace('scores[name] = scored_prediction(probability,predicted,row["actual_return_bps"],',
        'scores[name] = scored_prediction(probability,predicted,row["reference_quote"],')
text = text.replace('statistics.fmean(', 'decimal_mean(')
replace('r["scores"][family]["brier"]-.25', 'r["scores"][family]["brier"]-Decimal(".25")')
replace('return {"schema_version":"fixed_forecast_evaluation_report_v1",',
        'return {"schema_version":"fixed_forecast_evaluation_report_exact_v2",\n            "evaluator_version":EVALUATOR_VERSION,"price_scoring_version":SCORING_VERSION,\n            "decimal_metric_precision_digits":METRIC_PRECISION,"decimal_json_representation":"strings",')
replace('"outcome_counts":dict(Counter("up" if r["actual_return_bps"] > 0 else "down" if r["actual_return_bps"] < 0 else "flat" for r in results)),',
        '"outcome_counts":dict(Counter(r["outcome_class"] for r in results)),')
replace('"limitations":["Input clocks are internally checked assertions; not independent proof of past availability.",',
        '"limitations":["Separate offline evaluator; this revision is not integrated into or authorized by any registered collection.",\n                "Prices use exact supplied decimals; float inputs cannot recover precision already lost before this evaluator.",\n                "Metrics use 80-digit Decimal ratios; JSON metric/price values are strings. Clocks retain v1 float semantics.",\n                "Exact reference anchors reject mismatches without the previous 1e-12 tolerance.",\n                "Input clocks are internally checked assertions; not independent proof of past availability.",')
replace('dataset = json.loads(args.input.read_text(encoding="utf-8-sig"))',
        'dataset = loads_exact_prices(args.input.read_text(encoding="utf-8-sig"))')
replace('protocol = json.loads(args.protocol.read_text(encoding="utf-8-sig"))',
        'protocol = loads_exact_prices(args.protocol.read_text(encoding="utf-8-sig"))')
replace('    result = evaluate(dataset,protocol)',
        '    result = evaluate(dataset,protocol)\n    result["input_file_sha256"] = hashlib.sha256(args.input.read_bytes()).hexdigest()\n    result["protocol_file_sha256"] = hashlib.sha256(args.protocol.read_bytes()).hexdigest()')
replace('json.dump(result,handle,indent=2,allow_nan=False)', 'json.dump(jsonable(result),handle,indent=2,allow_nan=False)')
target = ROOT/'oanda_fixed_forecast_evaluation_exact.py'
with target.open('x', encoding='utf-8', newline='\n') as handle:
    handle.write(text)
print(target)
