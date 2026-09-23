"""Read-only publication adapters for the versioned research observation lane.

News uses the existing repaired-snapshot replay and original eight-value
projection. Model values are observations of independently consumed ledger
outputs, never new forecasts or additional fitted-model inputs. Receipt clocks
and expiry remain separate from this reader's actual observation time.
"""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time

from oanda_feature_observations_v1 import capture_feature_group

SCHEMA = "research_event_observation_inputs_v1_20260913"
ROOT = Path(__file__).resolve().parent
NEWS_NAMES = ("context_balance", "context_volume_log", "context_signed_fraction", "context_mean_age_hours",
              "vetted_balance", "vetted_volume_log", "vetted_conflict_fraction", "vetted_remaining_hours")
MAX_LEDGER_PAYLOAD = 2*1024*1024
_SOURCE_HASHES = {}


def source_hash(name):
    if not re.fullmatch(r"[A-Za-z0-9_]+\.py", name):
        raise ValueError("model_source_binding_path")
    path = ROOT/name
    before = path.stat()
    signature = (before.st_ino,before.st_size,before.st_mtime_ns)
    cached = _SOURCE_HASHES.get(name)
    if cached and cached[0] == signature:
        return cached[1]
    if before.st_size > 2*1024*1024:
        raise ValueError("model_source_byte_bound")
    value = hashlib.sha256(path.read_bytes()).hexdigest()
    after = path.stat()
    if signature != (after.st_ino,after.st_size,after.st_mtime_ns):
        raise ValueError("model_source_changed_during_read")
    _SOURCE_HASHES[name] = (signature,value)
    return value


def source_identity():
    """Projection/evaluator code participates in the new observation cohort."""
    import oanda_causal_forecast_inputs_joint_news_v3 as projection
    import native_m1_outcome_v1 as native
    values = {**projection._bindings(), **native.source_bindings()}
    for name in ("oanda_fixed_forecast_evaluation_pair_v2.py", "oanda_fixed_forecast_evaluation_joint_news_v3.py",
                 "oanda_exact_price_scoring.py", "native_m1_ledger_v1.py", "joint_native_anchor_v1.py"):
        values[name] = source_hash(name)
    return values


def epoch(value):
    if isinstance(value, str):
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError("event_timezone_required")
        value = result.timestamp()
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError("positive_event_clock_required")
    return float(value)


def iso(value):
    return datetime.fromtimestamp(epoch(value), timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def event_group(values, *, publication, observed, expires, evidence):
    publication, observed, expires = map(epoch, (publication, observed, expires))
    if not publication <= observed < expires:
        raise ValueError("event_publication_observation_expiry_order")
    return capture_feature_group(values, input_timeframe="EVENT", clock={
        "observed_utc": iso(observed), "bar_complete_utc": iso(publication),
        "clock_basis": "verified_original_publication_then_actual_local_read",
        "component_clocks": {"source_publication_utc": iso(publication), "valid_until_utc": iso(expires),
                             "publication_receipt_verified": True, "event_evidence": evidence}})


def news_observations(snapshot, pairs, *, read_completed_utc, clock):
    # Import only the existing input projection/validator, never a worker or a
    # fit method. This path does not open its history database or store captures.
    import oanda_causal_forecast_inputs_joint_news_v3 as projection
    guard = projection._module("oanda_news_causal_aggregation_guard_v2")
    topics = projection._validate_current(snapshot, projection._bindings(), guard)
    read_at = epoch(read_completed_utc)
    observed = epoch(clock())  # Validation/computation availability is actual.
    published, evidence_at = epoch(snapshot["generated_utc"]), epoch(snapshot["as_of_utc"])
    if not evidence_at <= published <= read_at <= observed < evidence_at+300:
        raise ValueError("current_news_stale_or_future")
    members = {}
    for topic in topics:
        for member in topic["causal_aggregation_guard"]["members"]:
            key = member["event_id"]
            if key in members and members[key] != member:
                raise ValueError("conflicting_current_member_identity")
            members[key] = member
    # The projection keeps original story/derived clocks and original expiry.
    # Only this reader's availability is new; no source member is retimestamped.
    capture = {"news_capture_sha256": projection._digest(snapshot), "first_observed_epoch": observed,
               "current_members": list(members.values())}
    frame = projection._frame(capture, observed, live=True)
    output = {}
    for pair in pairs:
        vector, expiry = projection._pair_features(frame, pair, observed)
        pair_members = sum(any(currency in row["scores"] for currency in pair.split("_")) for row in frame["members"])
        pair_directional = sum(any(currency in row["scores"] for currency in pair.split("_")) for row in frame["directional"])
        metadata = {"adapter_schema": SCHEMA, "snapshot_sha256": capture["news_capture_sha256"],
                    "source_evidence_utc": iso(evidence_at), "source_read_completed_utc": iso(read_at),
                    "context_member_count": pair_members, "directional_claim_count": pair_directional,
                    "empty_population_defaults_are_model_encodings": True,
                    "projection": "oanda_causal_forecast_inputs_joint_news_v3._frame/_pair_features"}
        output[pair] = event_group(dict(zip(NEWS_NAMES, vector)), publication=published, observed=observed,
                                   expires=min(evidence_at+300, expiry) if expiry is not None else evidence_at+300,
                                   evidence=metadata)
        for name in NEWS_NAMES:
            if (name.startswith("context_") and pair_members == 0) or (name.startswith("vetted_") and pair_directional == 0):
                output[pair]["value_states"][name] = "default_no_evidence"
    return output, {"status": "verified_publication_observed", "observed_utc": iso(observed),
                    "context_members": len(frame["members"]), "directional_claims": len(frame["directional"]),
                    "snapshot_payload_sha256": capture["news_capture_sha256"],
                    "projection_source_sha256": hashlib.sha256(Path(projection.__file__).read_bytes()).hexdigest()}


def _bounded_json(raw):
    if not isinstance(raw, str) or len(raw.encode()) > MAX_LEDGER_PAYLOAD:
        raise ValueError("ledger_json_byte_bound")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("ledger_json_object_required")
    return value


def verify_model_record(saved, activation, row, *, pair, observed, current_bindings=None):
    """Same pure forecast contract checks as current summaries, plus receipts.

    Intentionally does not fit, settle, recover publications, instantiate a
    ledger, or count this observation as independent forecasting performance.
    """
    contract = _bounded_json(saved["payload"])
    if digest(contract) != saved["sha"] or activation["contract_sha"] != saved["sha"]:
        raise ValueError("model_contract_or_activation_digest")
    bindings = contract.get("source_bindings")
    if not isinstance(bindings, dict) or not bindings:
        raise ValueError("model_source_bindings_required")
    native_bindings = contract.get("native_source_bindings",{})
    if not isinstance(native_bindings,dict) or any(name in bindings and bindings[name] != value for name,value in native_bindings.items()):
        raise ValueError("native_model_source_binding_conflict")
    bindings = {**bindings,**native_bindings}
    if current_bindings is None:
        current_bindings = {name:source_hash(name) for name in bindings}
    if bindings != current_bindings:
        raise ValueError("model_source_binding_changed")
    payload = _bounded_json(row["payload"])
    published, consumed = epoch(row["published"]), epoch(row["consumed"])
    receipt = {"epoch": published, "forecast_sha": row["sha"]}
    if (digest(payload) != row["sha"] or row["sha"] != row["publication_forecast_sha"]
            or row["sha"] != row["consumption_forecast_sha"] or row["consumption_publication_sha"] != digest(receipt)):
        raise ValueError("model_forecast_publication_consumption_digest")
    family = contract["family"]
    arms = payload["forecasts"]
    if (payload.get("decision_id") != row["id"] or payload.get("instrument") != pair or contract["instrument"] != pair
            or payload.get("family") != family or len(arms) != 1 or arms[0].get("family") != family):
        raise ValueError("model_pair_family_identity")
    arm = arms[0]
    from oanda_exact_price_scoring import decimal_value, quote_midpoint
    quote = _bounded_json(row["reference_quote"])
    if (payload.get("attempt_id") != row["attempt_id"] or payload.get("reference_quote_id") != quote.get("quote_id")
            or quote.get("quote_id") != row["reference_quote_id"] or quote.get("instrument") != pair
            or decimal_value(quote.get("pip_size")) != decimal_value(contract["pip_size"])
            or decimal_value(payload.get("pip_size")) != decimal_value(contract["pip_size"])):
        raise ValueError("model_original_reference_quote_binding")
    issue, target, reference = map(epoch, (arm["issued_epoch"], payload["target_epoch"], payload["reference_epoch"]))
    if not epoch(activation["epoch"]) < issue <= published <= consumed <= epoch(observed) < target:
        raise ValueError("model_original_clocks_stale_or_future")
    if target != reference+3600 or arm.get("target_epoch") != target or arm.get("reference_epoch") != reference:
        raise ValueError("model_original_horizon_binding")
    if arm.get("cohort_id") != contract["cohorts"][family]:
        raise ValueError("model_cohort_binding")
    protocol = dict(contract["evaluation_protocol"])
    if "native_anchor" in arm:
        from oanda_fixed_forecast_evaluation_joint_news_v3 import forecast_errors
    else:
        from oanda_fixed_forecast_evaluation_pair_v2 import forecast_errors
        protocol["historical_start_utc"] = iso(activation["epoch"])
        market, available = epoch(quote["market_epoch"]),epoch(quote["available_epoch"])
        if (reference != market or not epoch(activation["epoch"]) < available <= issue
                or not 0 <= available-market <= 60 or decimal_value(arm["reference_mid"]) != quote_midpoint(quote)):
            raise ValueError("model_price_reference_clock_or_midpoint_binding")
    errors = forecast_errors({**arm, "committed_available_epoch": consumed}, payload, protocol)
    if errors:
        raise ValueError("model_pure_forecast_contract:"+"|".join(errors)[:180])
    if any(arm.get(k) is not False for k in ("can_place_orders", "account_eligible", "proof_eligible")):
        raise ValueError("model_output_inert_flags")
    values = {key: arm[key] for key in ("predicted_return_bps", "probability_up", "side")}
    if any(type(v) not in (int, float) or not math.isfinite(v) for v in values.values()) or not 0 <= values["probability_up"] <= 1 or values["side"] not in (-1,0,1):
        raise ValueError("model_output_numeric_range")
    return values, {"adapter_schema": SCHEMA, "contract_sha256": saved["sha"], "cohort_id": arm["cohort_id"],
        "decision_id": row["id"], "forecast_sha256": row["sha"], "publication_receipt_sha256": digest(receipt),
        "original_issue_utc": iso(issue), "original_reference_utc": iso(reference), "original_publication_utc": iso(published),
        "independent_consumption_utc": iso(consumed), "original_target_utc": iso(target),
        "validation_scope": "read_only_committed_receipts_and_pure_forecast_contract; not_model_refit_or_performance_proof"}


def model_observations(study_roots, pairs, *, clock, max_seconds=5.0):
    started = time.monotonic()
    output, report, evidence = {}, [], {}
    for index, root in enumerate(study_roots):
        root = Path(root).absolute()
        for pair in pairs:
            candidates = sorted((root/"pairs"/pair).glob("*/study.sqlite"))[:4]
            if not candidates:
                report.append({"study": root.name, "instrument": pair, "status": "no_ledger"})
            for path in candidates:
                if time.monotonic()-started > max_seconds:
                    report.append({"study": root.name, "status": "adapter_time_bound"})
                    return output, report, evidence
                try:
                    if any(p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction()) for p in (path,*path.parents)):
                        raise ValueError("model_ledger_reparse_path")
                    with closing(sqlite3.connect(path.as_uri()+"?mode=ro", uri=True, timeout=.15)) as db:
                        db.row_factory = sqlite3.Row
                        db.execute("PRAGMA query_only=ON")
                        db.set_progress_handler(lambda: int(time.monotonic()-started > max_seconds), 1000)
                        db.execute("BEGIN")
                        saved = db.execute("SELECT sha,payload FROM contract WHERE id=1").fetchone()
                        activation = db.execute("SELECT epoch,contract_sha FROM activation WHERE id=1").fetchone()
                        row = db.execute('''SELECT f.id,f.sha,f.payload,p.epoch published,p.forecast_sha publication_forecast_sha,
                            c.epoch consumed,c.forecast_sha consumption_forecast_sha,c.publication_sha consumption_publication_sha,
                            a.id attempt_id,a.reference_id reference_quote_id,q.payload reference_quote
                            FROM forecasts f JOIN publication p ON p.id=f.id JOIN consumption c ON c.id=f.id
                            JOIN attempts a ON a.id=f.attempt_id JOIN quotes q ON q.id=a.reference_id
                            ORDER BY f.bucket DESC LIMIT 1''').fetchone()
                        db.rollback()
                    if saved is None or activation is None or row is None:
                        raise ValueError("no_committed_consumed_forecast")
                    observed = epoch(clock())
                    values, receipt = verify_model_record(saved, activation, row, pair=pair, observed=observed)
                    key = f"study_{index}_{path.parent.name}"
                    group = event_group({"supervised_"+key+"_"+k:v for k,v in values.items()},
                                        publication=row["consumed"], observed=epoch(clock()), expires=receipt["original_target_utc"], evidence=receipt)
                    output.setdefault(pair,{})["forecast:"+key] = group
                    evidence[str(path)] = {"contract": json.loads(saved["payload"]), "forecast": json.loads(row["payload"]),
                                           "activation":dict(activation),"contract_sha256":saved["sha"],
                                           "reference_quote":json.loads(row["reference_quote"]),
                                           "publication": {"epoch":row["published"],"forecast_sha":row["sha"]},
                                           "consumption": {"epoch":row["consumed"],"forecast_sha":row["sha"],"publication_sha":row["consumption_publication_sha"]}}
                    report.append({"study":root.name,"instrument":pair,"family":path.parent.name,"status":"verified_publication_observed"})
                except (OSError, ValueError, TypeError, KeyError, sqlite3.Error) as exc:
                    report.append({"study":root.name,"instrument":pair,"family":path.parent.name,"status":"unavailable",
                                   "reason":type(exc).__name__+":"+str(exc)[:220]})
    return output, report, evidence
