"""Immutable causal forecast-tape assembly from preserved forecast outputs."""
import hashlib
import json

from contracts import fingerprint


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _origin(row):
    value = row.get("origin_epoch", row.get("decision_epoch"))
    if not isinstance(value, int):
        raise ValueError("forecast_tape_origin_required")
    return value


def _validate(row):
    for name in ("forecast_id", "record_id", "instrument", "target_id", "available_epoch", "prediction_bps"):
        if name not in row:
            raise ValueError("forecast_tape_required_field_missing")
    if row["record_id"] != f"{row['instrument']}:{_origin(row)}":
        raise ValueError("forecast_tape_record_identity")
    if row["forecast_id"] != fingerprint({k: v for k, v in row.items() if k != "forecast_id"}):
        raise ValueError("forecast_tape_forecast_identity")


def assemble(sources):
    """Return one deterministic tape; conflicting forecast IDs and duplicate keys refuse."""
    records, seen_ids, seen_keys = [], {}, set()
    for source_name, rows in sorted(sources.items()):
        if not isinstance(rows, list):
            raise ValueError("forecast_tape_rows_required")
        for row in rows:
            _validate(row)
            identifier = row["forecast_id"]
            if identifier in seen_ids and seen_ids[identifier] != row:
                raise ValueError("forecast_tape_conflicting_forecast_id")
            seen_ids[identifier] = row
            key = (identifier, source_name)
            if key in seen_keys:
                raise ValueError("forecast_tape_duplicate_source_record")
            seen_keys.add(key)
            records.append({"source": source_name, "origin_epoch": _origin(row),
                            "available_epoch": row["available_epoch"], "instrument": row["instrument"],
                            "record_id": row["record_id"], "target_id": row["target_id"],
                            "variant": row.get("variant", "unspecified"), "forecast_id": identifier,
                            "model_id": row.get("model_id", row.get("source_model_id")),
                            "prediction_bps": row["prediction_bps"], "row": row})
    records.sort(key=lambda x: (x["origin_epoch"], x["available_epoch"], x["instrument"], x["forecast_id"], x["source"]))
    result = {"schema_version": "forex_forecast_tape.v1", "records": records,
              "coverage": {"records": len(records), "sources": {name: len(rows) for name, rows in sorted(sources.items())},
                           "instruments": len({x["instrument"] for x in records}),
                           "origins": len({x["origin_epoch"] for x in records})},
              "scope": "offline immutable forecast identity tape; outcomes are not inserted"}
    result["tape_sha256"] = fingerprint(result)
    return result


def matched_source_coverage(tape, source_names):
    """Exact record/target/base/horizon intersection without using outcomes."""
    if tape.get("tape_sha256") != fingerprint({k: v for k, v in tape.items() if k != "tape_sha256"}):
        raise ValueError("forecast_tape_identity")
    requested = tuple(sorted(source_names))
    if len(requested) < 2:
        raise ValueError("forecast_tape_two_sources_required")
    groups = {source: set() for source in requested}
    for entry in tape["records"]:
        if entry["source"] not in groups:
            continue
        row = entry["row"]
        key = (entry["record_id"], entry["target_id"], row.get("base_method"), row.get("horizon_minutes"))
        groups[entry["source"]].add(key)
    common = set.intersection(*groups.values())
    return {"sources": list(requested), "source_rows": {key: len(value) for key, value in groups.items()},
            "matched_rows": len(common), "matched_origins": len({key[0].rsplit(":", 1)[1] for key in common}),
            "matched_instruments": len({key[0].rsplit(":", 1)[0] for key in common}),
            "support_sha256": fingerprint(sorted(common))}
