"""Convert the preserved availability inventory without inventing missing clocks.

The input is normalized_forecasts.json from the dated bounded SQLite extract,
not a production database. Original quotes remain in that hash-bound input;
they lack the availability/tradeability attestations required by the strict
quote stream and are never silently reclassified into it.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path


def timestamp(value):
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone_required")
    return parsed.timestamp()


def build(normalized: dict, *, source_sha256: str, cutoff_epoch: float) -> dict:
    groups = defaultdict(list)
    for source in normalized["rows"]:
        reference = timestamp(source["reference_time_utc"])
        quote = source["entry_quote"]
        midpoint = (quote["bid"]+quote["ask"])/2
        if midpoint <= 0:
            raise ValueError("invalid_reference_midpoint")
        forecast = {
            key:source[key] for key in ("forecast_id","family","cohort_id","model_version",
                "feature_version","instrument","horizon_sec","input_timeframe","probability_up")}
        forecast.update({
            "reference_mid":midpoint,
            "side":1 if source["direction"]=="buy" else -1 if source["direction"]=="sell" else 0,
            "abstention_reason":None if source["direction"] in {"buy","sell"} else "original_forecast_abstained",
            "reference_epoch":reference, "target_epoch":reference+source["horizon_sec"],
            "issued_epoch":timestamp(source.get("issued_utc")),
            "committed_available_epoch":timestamp(source.get("committed_utc")),
            "feature_cutoff_epoch":timestamp(source.get("data_cutoff_utc")),
            "features_available_epoch":timestamp(source.get("features_available_utc")),
            "training_label_maturity_max_epoch":timestamp(source.get("max_training_label_maturity_utc")),
            "training_labels_available_max_epoch":timestamp(source.get("max_training_label_available_utc")),
            "predicted_return_bps":source["expected_signed_pips"]*source["pip"]/midpoint*10000,
            "source_payload_sha256":source["payload_sha256"],
            "recorded_utc_unverified_for_publication":source["recorded_utc"],
            "original_stored_outcome_present":source.get("outcome") is not None,
        })
        groups[reference].append(forecast)
    decisions = [{"decision_id":f"EUR_USD:{reference:.6f}:3600", "reference_epoch":reference,
                  "target_epoch":reference+3600,"reference_quote_id":None,"forecasts":rows}
                 for reference, rows in sorted(groups.items())]
    return {"schema_version":"fixed_forecast_evaluation_input_v1",
            "observed_cutoff_epoch":cutoff_epoch,"decisions":decisions,"quotes":[],
            "provenance":{"normalized_source_sha256":source_sha256,"original_forecast_count":len(normalized["rows"]),
                "historical_targets_reinterpreted_as_valid":False,
                "missing_timestamps_inferred":False,
                "quote_omission_reason":"Original quote prices/provider clocks are preserved in normalized source, but verified availability and tradeability certificates are absent. No strict quote stream fabricated.",
                "classification":"already_inspected_archive_for_availability_rejection_test"}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--normalized",type=Path,required=True)
    parser.add_argument("--cutoff-utc",required=True)
    parser.add_argument("--output",type=Path,required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.resolve().is_relative_to(Path(__file__).resolve().parents[1]/"data"):
        raise SystemExit("Use a new output in a disposable review directory.")
    raw = args.normalized.read_bytes()
    result = build(json.loads(raw),source_sha256=hashlib.sha256(raw).hexdigest(),cutoff_epoch=timestamp(args.cutoff_utc))
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open("x",encoding="utf-8") as stream:
        json.dump(result,stream,indent=2,allow_nan=False)
        stream.write("\n")
    print(json.dumps({"decisions":len(result["decisions"]),"forecasts":result["provenance"]["original_forecast_count"],
                      "missing_timestamps_inferred":False,"output":str(args.output)}))


if __name__ == "__main__":
    main()
