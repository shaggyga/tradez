"""One prespecified Extra Trees regression comparison on retained matched inputs."""
from __future__ import annotations

import hashlib
import base64
import io
import json
import os
import time
from collections import defaultdict
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import ExtraTreesRegressor
from threadpoolctl import threadpool_limits

from contracts import TrainingView, fingerprint, forecast_record
from matched_campaign_models_v2 import FEATURES, canonicalize_model_strings, contract, population
from matched_campaign_runner_v2 import load_inputs
from publication import RunPublisher, effective_run_identity, verify_completed_run

METHOD = "extra_trees"
CONTROLS = ("zero", "history_mean", "ridge", "recovered_hgb")
MAX_SECONDS = 600
MAX_OUTPUT_BYTES = 536_870_912
QUALIFICATION_SHA256 = '208c12b14a23fae23cc633705a6893ab418d622afd11f9af357fd509d26c4339'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def encoded(obj):
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def fit_one(observations, outcomes, baseline_meta, minutes, cutoff, c, params):
    target = {"target_id": f"technical_endpoint_midpoint_elapsed_{minutes}m", "horizon_seconds": minutes * 60}
    view = TrainingView(c["training_start"], cutoff, cutoff, cutoff, c["evaluation_asof"])
    selected = population(observations, outcomes, target, view)
    if len(selected) < c["minimum_training_rows"] or fingerprint(selected) != baseline_meta["training_population_sha256"]:
        raise ValueError("extra_trees_baseline_training_population_mismatch")
    if baseline_meta["target"] != target or baseline_meta["fit_cutoff"] != cutoff:
        raise ValueError("extra_trees_baseline_target_cutoff_mismatch")
    if any(o["available_epoch"] > cutoff or o["label_end_epoch"] > cutoff for _, o in selected):
        raise ValueError("extra_trees_unmatured_label")
    x = np.asarray([r["features"] for r, _ in selected], dtype=np.float64)
    y = np.asarray([o["value"] for _, o in selected], dtype=np.float64)
    if x.shape[1] != len(FEATURES) or not np.isfinite(x).all() or not np.isfinite(y).all():
        raise ValueError("extra_trees_nonfinite_or_wrong_feature_width")
    with threadpool_limits(limits=1):
        model = ExtraTreesRegressor(**params)
        model.fit(x, y, sample_weight=np.ones(len(y)))
    canonicalize_model_strings(model)
    buffer = io.BytesIO()
    joblib.dump(model, buffer, compress=0, protocol=5)
    model_bytes = buffer.getvalue()
    meta = {"schema": "forex.extra_trees_matched_fit.v1", "target": target, "fit_cutoff": cutoff,
            "ready_epoch": cutoff + c["fit_latency_seconds"], "training_view": view.identity(),
            "training_rows": len(selected), "training_population_sha256": fingerprint(selected),
            "maximum_outcome_available_epoch": max(o["available_epoch"] for _, o in selected),
            "feature_schema_sha256": fingerprint(FEATURES), "baseline_fit_id": baseline_meta["fit_id"],
            "parameters": params, "model_sha256": hashlib.sha256(model_bytes).hexdigest(),
            "equal_row_weights": True, "probability_scope": "not_provided"}
    meta["fit_id"] = fingerprint(meta)
    return meta, model_bytes


def validate_fit(meta, model_bytes):
    if meta["fit_id"] != fingerprint({k: v for k, v in meta.items() if k != "fit_id"}):
        raise ValueError("extra_trees_fit_identity_mismatch")
    if hashlib.sha256(model_bytes).hexdigest() != meta["model_sha256"]:
        raise ValueError("extra_trees_model_bytes_changed")
    if meta["maximum_outcome_available_epoch"] > meta["fit_cutoff"]:
        raise ValueError("extra_trees_unmatured_fit")


def issue(meta, model_bytes, observations, *, procedure, c):
    validate_fit(meta, model_bytes)
    coverage = []
    valid = []
    for row in observations:
        reason = "eligible"
        if meta["ready_epoch"] > row["origin_epoch"]:
            reason = "model_not_ready"
        elif row["available_epoch"] > row["origin_epoch"]:
            reason = "feature_not_ready"
        elif row["features"] is None:
            reason = "missing_features"
        elif len(row["features"]) != len(FEATURES) or not np.isfinite(row["features"]).all():
            raise ValueError("extra_trees_invalid_assessment_features")
        coverage.append({"record_id": row["record_id"], "instrument": row["instrument"],
                         "decision_epoch": row["origin_epoch"], "target_id": meta["target"]["target_id"],
                         "procedure": procedure, "method": METHOD, "reason": reason})
        if reason == "eligible":
            valid.append(row)
    forecasts = []
    if valid:
        x = np.asarray([r["features"] for r in valid], dtype=np.float64)
        with threadpool_limits(limits=1):
            values = joblib.load(io.BytesIO(model_bytes)).predict(x)
        if not np.isfinite(values).all():
            raise ValueError("extra_trees_nonfinite_prediction")
        tv = TrainingView(**{k: v for k, v in meta["training_view"].items() if k != "schema"})
        model_id = fingerprint({"fit_id": meta["fit_id"], "method": METHOD})
        for row, prediction in zip(valid, values):
            forecast = forecast_record(forecast_id=fingerprint({"model": model_id, "observation": row, "procedure": procedure}),
                instrument=row["instrument"], decision_epoch=row["origin_epoch"],
                available_epoch=row["origin_epoch"] + c["prediction_latency_seconds"],
                model_id=model_id, model_ready_epoch=meta["ready_epoch"], training_view=tv,
                target_id=meta["target"]["target_id"], prediction=float(prediction))
            forecasts.append({"record_id": row["record_id"], "method": METHOD,
                              "procedure": procedure, "forecast": forecast})
    return forecasts, coverage


def paired_scores(new_forecasts, baseline_forecasts, outcomes, c):
    old = {(x["record_id"], x["forecast"]["target_id"], x["procedure"], x["method"]): x for x in baseline_forecasts}
    if len(old) != len(baseline_forecasts):
        raise ValueError("extra_trees_duplicate_baseline_forecast")
    mature = {(x["record_id"], x["target_id"]): x for x in outcomes}
    if len(mature) != len(outcomes):
        raise ValueError('extra_trees_duplicate_outcome')
    new_keys = {(r['record_id'], r['forecast']['target_id'], r['procedure']) for r in new_forecasts}
    if len(new_keys) != len(new_forecasts):
        raise ValueError('extra_trees_duplicate_new_forecast')
    groups = defaultdict(list)
    matched = 0
    for row in new_forecasts:
        forecast = row["forecast"]
        stem = (row["record_id"], forecast["target_id"], row["procedure"])
        controls = {m: old.get((*stem, m)) for m in CONTROLS}
        if any(v is None for v in controls.values()):
            raise ValueError("extra_trees_missing_matched_control")
        if any(v["forecast"]["decision_epoch"] != forecast["decision_epoch"] or
               v["forecast"]["instrument"] != forecast["instrument"] or
               v["forecast"]["available_epoch"] != forecast["available_epoch"] for v in controls.values()):
            raise ValueError("extra_trees_control_identity_mismatch")
        outcome = mature.get((row["record_id"], forecast["target_id"]))
        if outcome is None or outcome["value"] is None or outcome["available_epoch"] > c["evaluation_asof"]:
            continue
        if max([forecast["available_epoch"]] + [v["forecast"]["available_epoch"] for v in controls.values()]) > c["evaluation_asof"]:
            continue
        matched += 1
        origin = forecast["decision_epoch"]
        values = {"new": forecast["prediction"] - outcome["value"]}
        values.update({m: controls[m]["forecast"]["prediction"] - outcome["value"] for m in CONTROLS})
        for stratum, value in (("overall", None), ("origin", origin), ("utc_day", origin // 86400)):
            groups[(forecast["target_id"], row["procedure"], stratum, value)].append((row["record_id"], forecast["instrument"], values))
    result = []
    for (target, procedure, stratum, value), rows in sorted(groups.items(), key=lambda x: str(x[0])):
        n = len(rows)
        newer = [r[2]["new"] for r in rows]
        control = {}
        for method in CONTROLS:
            prior = [r[2][method] for r in rows]
            control[method] = {"mae_delta_bps": (sum(map(abs, newer)) - sum(map(abs, prior))) / n,
                               "mse_delta_bps2": (sum(x*x for x in newer) - sum(x*x for x in prior)) / n,
                               "bias_delta_bps": (sum(newer) - sum(prior)) / n}
        result.append({"target_id": target, "procedure": procedure, "stratum": stratum,
                       "stratum_value": value, "matched_mature_rows": n,
                       "distinct_pairs": len({r[1] for r in rows}),
                       "support_sha256": fingerprint(sorted(r[0] for r in rows)),
                       "new_mae_bps": sum(map(abs, newer)) / n,
                       "new_mse_bps2": sum(x*x for x in newer) / n,
                       "new_bias_bps": sum(newer) / n, "paired_controls": control})
    if len([x for x in result if x["stratum"] == "overall"]) != 14 or matched == 0:
        raise ValueError("extra_trees_mature_matched_score_population_missing")
    return result


def fit_transaction(pub, prefix, make_fit, expected, phase):
    """A single atomic fit bundle preserves weights AND actual timing on crash.

    The later metadata/weight exports can be resumed without fitting again.
    An orphan atomic bundle is validated before entering the payload journal.
    """
    name=prefix+'.bundle.json'
    raw=pub.read_verified_payload(name)
    if raw is None and (pub.root/name).exists():
        if (pub.root/name).stat().st_size>MAX_OUTPUT_BYTES:raise ValueError('fit_bundle_size')
        raw=(pub.root/name).read_bytes()
    if raw is None:
        phase('fit',30)
        start=time.monotonic();meta,model=make_fit();elapsed=time.monotonic()-start
        phase('publication',None)
        if elapsed>30:raise TimeoutError('extra_trees_fit_latency_exceeded')
        raw=encoded({'run_identity':pub.identity['fingerprint'],'expected':expected,'metadata':meta,
                     'model_base64':base64.b64encode(model).decode('ascii'),'elapsed_seconds':elapsed})
    bundle=json.loads(raw)
    if bundle['run_identity']!=pub.identity['fingerprint'] or bundle['expected']!=expected:
        raise ValueError('extra_trees_cached_fit_context_mismatch')
    meta=bundle['metadata'];model=base64.b64decode(bundle['model_base64'],validate=True)
    validate_fit(meta,model)
    if not 0<=bundle['elapsed_seconds']<=30:raise ValueError('extra_trees_invalid_fit_receipt')
    for key,value in expected.items():
        if meta.get(key)!=value:raise ValueError('extra_trees_cached_fit_metadata_mismatch:'+key)
    payload=pub.write_or_validate_payload(name,raw)
    return meta,model,{'fit_id':meta['fit_id'],'elapsed_seconds':bundle['elapsed_seconds']},payload


def replay_saved(input_root, baseline_root, run_root):
    """Authenticate relocated dependencies; reproduce assessment with saved fits.

    This is numerical replication, not another experiment or model refit.
    """
    run_root=Path(run_root);baseline_root=Path(baseline_root)
    identity=json.loads((run_root/'RUN_IDENTITY.json').read_text())
    verify_completed_run(run_root,identity)
    experiment=identity['contract']['experiment']
    for name,digest in experiment['source_hashes'].items():
        if sha(Path(__file__).parent/name)!=digest:raise ValueError('replay_source_drift:'+name)
    baseline=json.loads((baseline_root/'RUN_IDENTITY.json').read_text())
    if baseline['fingerprint']!=experiment['baseline_identity']:raise ValueError('replay_parent_drift')
    verify_completed_run(baseline_root,baseline)
    recipe=baseline['contract']['recipe']
    if recipe['input_identity']['fingerprint']!=experiment['input_identity']:raise ValueError('replay_input_drift')
    observations,outcomes=load_inputs(Path(input_root),recipe);c=contract();fs=[];cov=[]
    for h in c['horizon_minutes']:
        for procedure in c['procedures']:
            for cutoff in c['fit_cutoffs']:
                if procedure=='frozen' and cutoff!=c['fit_cutoffs'][0]:continue
                rows=[]
                for row in observations:
                    if row['origin_epoch'] not in c['decision_epochs']:continue
                    chosen=max(t for t in c['fit_cutoffs'] if t+c['fit_latency_seconds']<=row['origin_epoch']) if procedure=='adaptive' else c['fit_cutoffs'][0]
                    if chosen==cutoff:rows.append(row)
                prefix=f'fit_{h}_{cutoff}'
                f,coverage=issue(json.loads((run_root/(prefix+'.json')).read_text()),(run_root/(prefix+'.joblib')).read_bytes(),rows,procedure=procedure,c=c)
                fs.extend(f);cov.extend(coverage)
    fs.sort(key=lambda r:(r['forecast']['decision_epoch'],r['forecast']['instrument'],r['forecast']['target_id'],r['procedure']))
    cov.sort(key=lambda r:(r['decision_epoch'],r['instrument'],r['target_id'],r['procedure']))
    scores=paired_scores(fs,json.loads((baseline_root/'forecasts.json').read_text()),outcomes,c)
    matches={}
    for name,obj in [('forecasts.json',fs),('coverage.json',cov),('scores.json',scores)]:
        raw=encoded(obj)
        if raw!=(run_root/name).read_bytes():raise ValueError('saved_model_replay_mismatch:'+name)
        matches[name]=hashlib.sha256(raw).hexdigest()
    return {'status':'pass','replication_of':identity['fingerprint'],'models_fitted':0,'saved_models':14,'forecasts_replayed':len(fs),'exact_payloads':matches}


def run(input_root, baseline_root, qualification_path, runs_root, *, resume=False, phase=lambda *args:None):
    started = time.monotonic()
    if sha(qualification_path)!=QUALIFICATION_SHA256:
        raise ValueError('extra_trees_qualification_pin_mismatch')
    q = json.loads(Path(qualification_path).read_text(encoding="utf-8"))
    if q["mechanism"] != "ExtraTreesRegressor" or not q["qualified_for_bounded_new_fit"]:
        raise ValueError("extra_trees_unqualified_contract")
    baseline_identity = json.loads((Path(baseline_root) / "RUN_IDENTITY.json").read_text())
    recipe = baseline_identity["contract"]["recipe"]
    if baseline_identity["fingerprint"] != q["baseline_run_identity"] or recipe["input_identity"]["fingerprint"] != q["input_run_identity"]:
        raise ValueError("extra_trees_parent_identity_mismatch")
    verify_completed_run(Path(input_root), recipe["input_identity"])
    verify_completed_run(Path(baseline_root), baseline_identity)
    c = contract()
    if (q["features"] != FEATURES or q["universe"] != recipe["universe"] or q["decision_epochs"] != c["decision_epochs"]
        or q['procedures']!=c['procedures'] or q['evaluation_asof']!=c['evaluation_asof'] or q['controls']!=list(CONTROLS)):
        raise ValueError("extra_trees_population_contract_drift")
    source = Path(__file__).resolve().parent
    source_hashes = {n: sha(source / n) for n in set(recipe['sources']) | {"extra_trees_matched_v1.py", "extra_trees_operator_v1.py", "matched_campaign_models_v2.py",
        "matched_campaign_runner_v2.py", "contracts.py", "publication.py"}}
    for n,digest in recipe['sources'].items():
        if source_hashes[n]!=digest:raise ValueError('extra_trees_baseline_source_changed:'+n)
    import platform,importlib.metadata
    environment={'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ['numpy','scipy','pandas','scikit-learn','joblib','threadpoolctl','psutil']}}
    if environment['python']!=q['environment']['python'] or environment['scikit-learn']!=q['environment']['scikit_learn']:
        raise ValueError('extra_trees_environment_mismatch')
    contract_record = {"schema": "forex.extra_trees_matched_contract.v1", "qualification_sha256": sha(qualification_path),
                       "mechanism": METHOD, "parameters": q["parameters"], "source_hashes": source_hashes,
                       "input_identity": q["input_run_identity"], "baseline_identity": q["baseline_run_identity"],
                       "universe": q["universe"], "features": q["features"], "horizons": c["horizon_minutes"],
                       "fit_cutoffs": c["fit_cutoffs"], "procedures": c["procedures"],
                       "decision_epochs": c["decision_epochs"], "evaluation_asof": c["evaluation_asof"],
                       "controls": list(CONTROLS), "resource_limits": q["resource_limits"],
                       "scope": q["interpretation"], 'environment':environment}
    required = ["experiment_contract.json", "forecasts.json", "coverage.json", "scores.json", "run_report.json", "fit_resources.json"]
    for h in c["horizon_minutes"]:
        for cutoff in c["fit_cutoffs"]:
            required += [f"fit_{h}_{cutoff}.json", f"fit_{h}_{cutoff}.joblib",f"fit_{h}_{cutoff}.bundle.json"]
    identity = effective_run_identity(contract={"experiment": contract_record, "required_payloads": sorted(required)},
                                      dependency_hashes={**source_hashes, "inputs": q["input_run_identity"],
                                                         "baseline": q["baseline_run_identity"], "qualification": sha(qualification_path)})
    pub = RunPublisher(Path(runs_root), "extra-trees-matched-development", identity)
    if (pub.root / "COMPLETION_MANIFEST.json").exists():
        verify_completed_run(pub.root, identity)
        return {"status": "completed_verified", "run_identity": identity["fingerprint"], "run_path": str(pub.root)}
    observations, outcomes = load_inputs(Path(input_root), recipe)
    if len({r['record_id'] for r in observations})!=len(observations) or len({(r['record_id'],r['target_id']) for r in outcomes})!=len(outcomes):
        raise ValueError('extra_trees_duplicate_input')
    pub.acquire(recover=resume)
    try:
        payloads = [pub.write_or_validate_payload("experiment_contract.json", encoded(contract_record))]
        fits = {}; resources=[]
        for h in c["horizon_minutes"]:
            for cutoff in c["fit_cutoffs"]:
                if time.monotonic() - started > MAX_SECONDS:
                    raise TimeoutError("extra_trees_total_time_exceeded")
                prefix = f"fit_{h}_{cutoff}"
                baseline_meta = json.loads((Path(baseline_root) / (prefix + ".json")).read_text())
                declared=next(r for r in q['fit_populations'] if r['horizon_minutes']==h and r['cutoff']==cutoff)
                if baseline_meta['training_population_sha256']!=declared['population_sha256'] or baseline_meta['training_rows']!=declared['training_rows']:
                    raise ValueError('extra_trees_qualified_fit_population_changed')
                expected={'target':baseline_meta['target'],'fit_cutoff':cutoff,'baseline_fit_id':baseline_meta['fit_id'],
                          'training_population_sha256':declared['population_sha256'],'training_rows':declared['training_rows'],
                          'parameters':q['parameters'],'feature_schema_sha256':fingerprint(FEATURES)}
                meta,model,timing,bundle=fit_transaction(pub,prefix,lambda:fit_one(observations,outcomes,baseline_meta,h,cutoff,c,q['parameters']),expected,phase)
                resources.append(dict(timing,horizon=h,cutoff=cutoff));payloads.append(bundle)
                raw=encoded(meta)
                payloads.append(pub.write_or_validate_payload(prefix + ".json", raw))
                payloads.append(pub.write_or_validate_payload(prefix + ".joblib", model))
                fits[(h, cutoff)] = (meta, model)
        if len(resources) != 14 or len({x["fit_id"] for x in resources}) != 14:
            raise ValueError("extra_trees_fit_resource_receipts_missing")
        forecasts, coverage, prediction_timings = [], [], []
        for h in c["horizon_minutes"]:
            for procedure in c["procedures"]:
                for cutoff in c["fit_cutoffs"]:
                    if procedure == "frozen" and cutoff != c["fit_cutoffs"][0]:
                        continue
                    current = []
                    for row in observations:
                        t = row["origin_epoch"]
                        if t not in c["decision_epochs"]:
                            continue
                        ready = [x for x in c["fit_cutoffs"] if x + c["fit_latency_seconds"] <= t]
                        chosen = max(ready) if procedure == "adaptive" else c["fit_cutoffs"][0]
                        if chosen == cutoff:
                            current.append(row)
                    t0 = time.monotonic()
                    phase('prediction',c['prediction_latency_seconds'])
                    f, cov = issue(*fits[(h, cutoff)], current, procedure=procedure, c=c)
                    elapsed = time.monotonic() - t0
                    phase('scoring',None)
                    if elapsed > c["prediction_latency_seconds"]:
                        raise TimeoutError("extra_trees_prediction_latency_exceeded")
                    prediction_timings.append({"horizon": h, "procedure": procedure, "cutoff": cutoff,
                                               "rows": len(current), "elapsed_seconds": elapsed})
                    forecasts.extend(f); coverage.extend(cov)
        forecasts.sort(key=lambda r: (r["forecast"]["decision_epoch"], r["forecast"]["instrument"], r["forecast"]["target_id"], r["procedure"]))
        coverage.sort(key=lambda r: (r["decision_epoch"], r["instrument"], r["target_id"], r["procedure"]))
        baseline_forecasts = json.loads((Path(baseline_root) / "forecasts.json").read_text())
        baseline_coverage = json.loads((Path(baseline_root) / "coverage.json").read_text())
        old = {(r["record_id"], r["target_id"], r["procedure"]): r for r in baseline_coverage if r["method"] == "ridge"}
        if len(old)!=sum(r['method']=='ridge' for r in baseline_coverage) or len({(r['record_id'],r['target_id'],r['procedure']) for r in coverage})!=len(coverage):
            raise ValueError('extra_trees_duplicate_coverage')
        if len(old) != len(coverage) or any(old.get((r["record_id"], r["target_id"], r["procedure"]), {}).get("reason") != r["reason"] for r in coverage):
            raise ValueError("extra_trees_native_all68_coverage_mismatch")
        if len(forecasts) != 18872 or len(coverage) != 19040:
            raise ValueError("extra_trees_declared_forecast_population_missing")
        scores = paired_scores(forecasts, baseline_forecasts, outcomes, c)
        if len({r["instrument"] for r in coverage}) != 68:
            raise ValueError("extra_trees_all68_instrument_coverage_missing")
        report = {"status": "completed_matched_development_comparison", "method": METHOD,
                  "models_fitted": 14, "baseline_models_refitted": 0, "forecast_rows": len(forecasts),
                  "coverage_rows": len(coverage), "matched_overall_slices": 14,
                  "selection": None, "confirmation": False, "policy_replays": 0,
                  "trading_ready": False, "fit_timings": resources,
                  "prediction_timings": prediction_timings, "elapsed_seconds": time.monotonic() - started}
        saved_report=pub.read_verified_payload('run_report.json')
        if saved_report is None and (pub.root/'run_report.json').exists():
            saved_report=(pub.root/'run_report.json').read_bytes()
        if saved_report is not None:
            prior=json.loads(saved_report)
            timing_fields={'prediction_timings','elapsed_seconds'}
            if {k:v for k,v in prior.items() if k not in timing_fields}!={k:v for k,v in report.items() if k not in timing_fields}:
                raise ValueError('extra_trees_resumed_report_identity_mismatch')
            # Timing belongs to the original issuance attempt, not the resume.
            for key in ['prediction_timings','elapsed_seconds']:report[key]=prior[key]
        for name, obj in (("forecasts.json", forecasts), ("coverage.json", coverage),
                          ("scores.json", scores), ("run_report.json", report), ("fit_resources.json", resources)):
            payloads.append(pub.write_or_validate_payload(name, encoded(obj)))
        if sum((pub.root / name).stat().st_size for name in required) > MAX_OUTPUT_BYTES:
            raise ValueError("extra_trees_output_budget_exceeded")
        pub.complete(payloads, set(required))
        verify_completed_run(pub.root, identity)
        return {"status": "completed_verified", "run_identity": identity["fingerprint"], "run_path": str(pub.root), "report": report}
    except BaseException:
        if pub._owner_token is not None:
            pub.release()
        raise
