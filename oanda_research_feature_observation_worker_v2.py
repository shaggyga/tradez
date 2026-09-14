"""Versioned research observations with native candle and publication adapters.

Reuses the original isolated feature calculators. Strict contiguous M1 MA
warm-up remains 204 real minutes; higher timeframe state may cross explicitly
scheduled weekends, never unknown gaps. No model weights or forecasts consume
this new schema. Original v1 observations and production studies stay separate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import time

import oanda_research_feature_observation_worker_v1 as base
import oanda_feature_candle_inputs_v2 as candles
import oanda_feature_event_inputs_v1 as events
from oanda_feature_observations_v1 import archive_observation_snapshot, build_observation_frame, canonical_bytes, payload_sha256
from oanda_feature_research_clock_v1 import validate_clock_state

ROOT = Path(__file__).resolve().parent
WORKER = "oanda_research_feature_observation_worker_v2"
SCHEMA = "research_existing_feature_observations_v2_20260914_publication_headroom"
MAX_SOURCE_BYTES = 128*1024*1024
MAX_ENVELOPE_BYTES = 64*1024*1024
PUBLICATION_RESERVE_SECONDS = 15.0
SCHEDULING_CONTRACT = "minute_rotated_exact_universe_v1"
SOURCE_FILES = (Path(__file__).name, Path(base.__file__).name, Path(candles.__file__).name, Path(events.__file__).name)


def build_research_observation(quotes, candle_sets, *, news_groups=None, model_groups=None, event_evidence=None, input_configuration=None, **kwargs):
    result = base.build_research_observation(quotes, candle_sets, **kwargs)
    result["observation_source"].update(producer_id=WORKER, producer_contract_id=SCHEMA,
        worker_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        source_dependency_sha256={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in SOURCE_FILES},
        candle_calendar_contract=candles.CALENDAR, candle_input_schema=candles.SCHEMA, event_input_schema=events.SCHEMA,
        history_policy="M1 contiguous actual elapsed minutes; M5/H1 native completed bars across scheduled weekend closure only",
        observation_scope="existing_calculators_and_verified_published_outputs; no_new_fitted_model_inputs")
    result["observation_source"]["input_configuration"] = input_configuration or {}
    result["observation_source"]["event_adapter_source_dependencies"] = events.source_identity()
    result["observation_inputs"]["event_component_clocks_v1"] = True
    result["source_inputs"]["event_evidence"] = event_evidence or {}
    readiness = result["coverage"]["family_readiness"]
    readiness.update(news_ready_pairs=len(news_groups or {}), model_output_pairs=len(model_groups or {}),
                     native_M1_required_contiguous_rows=204,
                     M5_required_native_rows=40, H4_required_completed_H1_rows_at_least=240,
                     scheduled_calendar=candles.CALENDAR)
    receipts = (result["source_inputs"].get("source_receipts") or {}).get("candles") or {}
    readiness["native_input_rows_by_pair"] = {pair:{tf:len(rows) for tf,rows in sets.items()} for pair,sets in candle_sets.items()}
    for pair, row in result["observation_inputs"]["instruments"].items():
        row["coverage"]["native_source_status"] = receipts.get(pair,{})
        if pair in (news_groups or {}):
            row["groups"]["news"] = news_groups[pair]
            row["coverage"]["families"]["news_classifier"] = "verified_repaired_publication_projection_observed"
        if pair in (model_groups or {}):
            row["groups"].update(model_groups[pair])
            row["coverage"]["families"]["supervised_and_pattern_forecasts"] = "verified_published_supervised_outputs; pattern_outputs_not_configured"
    # Calendar-aware freshness changes membership of the existing higher-TF
    # cross-sectional calculation. Reuse that exact pure scalar calculation.
    # M1 remains strictly elapsed-time based, including its MA initialization.
    calculated = kwargs.get("clock",base.utc_now)()
    for timeframe, seconds in base.GROUP_SECONDS.items():
        if timeframe == "M1":
            continue
        selected = {}
        for pair, row in result["observation_inputs"]["instruments"].items():
            group = row["groups"].get("timeframe:"+timeframe)
            if group is None:
                continue
            group["clock"].setdefault("component_clocks",{})["candle_calendar_contract"] = candles.CALENDAR
            origin = base.parse_utc(group["feature_origin_utc"])
            if origin is None:
                continue
            try:
                age = candles.open_elapsed_seconds(origin.timestamp()+seconds, calculated)
            except ValueError:
                continue
            if 0 <= age <= seconds+75 and all(type(group["values"].get(k)) in (int,float) and math.isfinite(group["values"][k]) for k in ("r1_pips","r3_pips","r5_pips","m1_atr14_pips")):
                selected[pair] = dict(group["values"])
        base.calculator.calculators()["augment_cross_sectional_features"](selected)
        for pair, values in selected.items():
            group = result["observation_inputs"]["instruments"][pair]["groups"]["timeframe:"+timeframe]
            for name, value in values.items():
                if name.startswith(("cross_","pair_norm_","pair_rank_","relative_residual_")):
                    group["values"][name] = value
                    group["value_states"][name] = "missing" if value is None else "observed"
        readiness["structural_fresh_pairs_by_timeframe"][timeframe] = len(selected)
    generated = kwargs.get("clock",base.utc_now)()
    for row in result["observation_inputs"]["instruments"].values():
        for group in row["groups"].values():
            if group["input_timeframe"] in base.GROUP_SECONDS and group["input_timeframe"] != "M1":
                group["clock"]["observed_utc"] = generated
    result["generated_utc"], result["generated_epoch"] = generated,base.parse_utc(generated).timestamp()
    readiness["basis"] = "actual source bar ends; M1 elapsed freshness; higher TF explicit NY weekend-only active-session freshness; final mapper checks authoritative"
    return result


def publication_reserve(max_cycle_sec):
    # Real68-pair25MiB probe:8.3s after build, plus post-loop frame work.
    # Keep the original total45s guard; never extend it to hide slow work.
    return min(PUBLICATION_RESERVE_SECONDS, float(max_cycle_sec)*.4)


def rotated_pairs(pairs, observed_utc):
    ordered = sorted(pairs)
    if not ordered:
        return []
    start = int(base.parse_utc(observed_utc).timestamp())//60 % len(ordered)
    return ordered[start:] + ordered[:start]


def run_cycle(args, *, clock=base.utc_now, monotonic=time.monotonic):
    started = monotonic()
    reserved = publication_reserve(args.max_cycle_sec)
    work_deadline = started + args.max_cycle_sec - reserved
    initial_clock = base.require_verified_clock(args.clock_state, clock=clock)
    budget = base.check_storage(args.archive_root, now=base.parse_utc(clock()),
                                max_daily_bytes=args.max_daily_archive_mib*1024*1024,
                                minimum_free_bytes=args.minimum_free_mib*1024*1024)
    quotes, quote_receipt = base.read_json_source(args.quote_snapshot, clock=clock)
    pairs = sorted(quotes.get("quotes") or {})
    if not 1 <= len(pairs) <= base.MAX_PAIRS or any(not base.PAIR.fullmatch(pair) for pair in pairs):
        raise ValueError("bounded_quote_universe_required")
    order = rotated_pairs(pairs, quote_receipt["source_read_completed_utc"])
    receipts, candle_sets, source_bytes = {"quotes":quote_receipt,"candles":{}}, {}, quote_receipt["source_bytes"]
    for pair in order:
        candle_sets[pair], receipts["candles"][pair] = {}, {}
        for timeframe in candles.SECONDS:
            if monotonic() >= work_deadline or source_bytes+candles.MAX_BYTES+4096 > MAX_SOURCE_BYTES:
                receipts["candles"][pair][timeframe] = {"status":"unavailable","reason":"cycle_input_bound"}
                continue
            try:
                root = args.candle_root if timeframe == "M1" else args.native_candle_root
                rows, receipt = candles.read_tail(base.checked_path(root/f"{pair}_{timeframe}.csv"), pair, timeframe, clock=clock)
                candle_sets[pair][timeframe] = rows
                receipts["candles"][pair][timeframe] = receipt
                source_bytes += receipt["source_bytes"]
            except (OSError, ValueError, KeyError, UnicodeError) as exc:
                receipts["candles"][pair][timeframe] = {"status":"unavailable","reason":type(exc).__name__+":"+str(exc)[:160]}
    books, news_groups, model_groups, event_evidence = None, {}, {}, {}
    if args.book_snapshot and monotonic() < work_deadline:
        try:
            books, receipt = base.read_json_source(args.book_snapshot, clock=clock)
            receipts["books"] = receipt
            source_bytes += receipt["source_bytes"]
        except (OSError, ValueError) as exc:
            receipts["books"] = {"status":"unavailable","reason":type(exc).__name__}
    elif args.book_snapshot:
        receipts["books"] = {"status":"unavailable","reason":"cycle_input_bound"}
    if args.news_snapshot and monotonic() < work_deadline:
        try:
            news, receipt = base.read_json_source(args.news_snapshot, max_bytes=16*1024*1024, clock=clock)
            news_groups, status = events.news_observations(news, pairs, read_completed_utc=receipt["source_read_completed_utc"], clock=clock)
            receipts["news"] = {**receipt, **status}
            event_evidence["repaired_news_snapshot"] = news
            source_bytes += receipt["source_bytes"]
        except (OSError, ValueError, TypeError, KeyError) as exc:
            receipts["news"] = {"status":"unavailable","reason":type(exc).__name__+":"+str(exc)[:220]}
    elif args.news_snapshot:
        receipts["news"] = {"status":"unavailable","reason":"cycle_input_bound"}
    if args.model_study and monotonic() < work_deadline:
        remaining = max(0., min(5., work_deadline-monotonic()))
        model_groups, receipts["models"], event_evidence["model_publications"] = events.model_observations(args.model_study, pairs, clock=clock, max_seconds=remaining)
    elif args.model_study:
        receipts["models"] = [{"status":"unavailable","reason":"cycle_input_bound","study_root":str(path)} for path in args.model_study]
    read_complete = clock()
    read_finished = monotonic()
    calculator_budget = max(0., work_deadline-read_finished)
    result = build_research_observation(quotes, candle_sets, source_read_completed_utc=read_complete,
        source_receipts=receipts, book_payload=books, clock=clock,
        max_cycle_seconds=calculator_budget, monotonic=monotonic, calculation_order=order,
        news_groups=news_groups, model_groups=model_groups, event_evidence=event_evidence,
        input_configuration={"native_candle_root":str(Path(args.native_candle_root).absolute()),
            "M1_candle_root":str(Path(args.candle_root).absolute()),
            "news_snapshot":str(Path(args.news_snapshot).absolute()) if args.news_snapshot else None,
            "model_study_roots":[str(Path(p).absolute()) for p in args.model_study]})
    built = monotonic()
    result["observation_source"]["scheduling_contract"] = SCHEDULING_CONTRACT
    deferred = [pair for pair,row in result["observation_inputs"]["instruments"].items()
                if row["coverage"].get("rich_ma_reason") == "invalid_pip_or_cycle_time_bound"]
    result["coverage"]["cycle_budget"] = {
        "total_limit_seconds":args.max_cycle_sec,
        "publication_reserve_seconds":reserved,
        "calculator_allowance_seconds":calculator_budget,
        "calculation_order":order,
        "unavailable_invalid_pip_or_budget_pairs":deferred,
        "all_pairs_retained":len(result["observation_inputs"]["instruments"]) == len(pairs),
        "scheduling_contract":SCHEDULING_CONTRACT}
    frame = build_observation_frame(result)
    if source_bytes > MAX_SOURCE_BYTES or len(canonical_bytes({"original_snapshot":result,"frame":frame})) > MAX_ENVELOPE_BYTES:
        raise ValueError("research_v2_source_or_envelope_byte_bound")
    clock_verdict = None
    def before_publish():
        nonlocal clock_verdict
        if monotonic()-started > args.max_cycle_sec:
            raise ValueError("cycle_time_bound_before_publication")
        proof = validate_clock_state(initial_clock["evidence"]["clock_state"], now_epoch=base.parse_utc(clock()).timestamp())
        if not proof["valid"]:
            raise ValueError("initial_research_clock_expired_before_publication:"+proof["reason"])
        clock_verdict = base.require_verified_clock(args.clock_state, clock=clock)
    path = archive_observation_snapshot(result, args.archive_root, before_publish=before_publish)
    archive_finished = monotonic()
    published = clock()
    receipt = {"schema_version":"feature_observation_publication_receipt_v1","snapshot_id":frame["snapshot_id"],
        "source_schema_id":frame["source_schema_id"],"payload_sha256":frame["source_payload_sha256"],"frame_sha256":payload_sha256(frame),
        "archive_relative_path":path.relative_to(args.archive_root).as_posix(),"archive_sha256":hashlib.sha256(path.read_bytes()).hexdigest(),
        "source_read_completed_utc":read_complete,"publication_completed_utc":published,
        "publication_scope":"archive_visible_before_this_receipt","producer_source":result["observation_source"],**base.FLAGS}
    receipt_path = path.with_suffix(".publication.json")
    base.immutable_json(receipt_path, receipt)
    return {"status":"published","archive":str(path),"publication_receipt":str(receipt_path),
            "last_publication_completed_utc":published,"coverage":result["coverage"],"event_status":{k:receipts[k] for k in ("news","models") if k in receipts},
            "source_bytes":source_bytes,"clock_verification":clock_verdict,
            "stage_elapsed_sec":{"sources":round(read_finished-started,3),"build":round(built-read_finished,3),
                "frame_and_archive":round(archive_finished-built,3),"publication_receipt":round(monotonic()-archive_finished,3)},
            "elapsed_sec":round(monotonic()-started,3),**budget}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("quote-snapshot","candle-root","native-candle-root","archive-root","heartbeat","clock-state"):
        parser.add_argument("--"+name,type=Path,required=True)
    for name in ("book-snapshot","news-snapshot"):
        parser.add_argument("--"+name,type=Path)
    parser.add_argument("--model-study",type=Path,action="append",default=[])
    parser.add_argument("--interval-sec",type=float,default=60)
    parser.add_argument("--duration-sec",type=float,default=3600)
    parser.add_argument("--max-cycle-sec",type=float,default=45)
    parser.add_argument("--max-daily-archive-mib",type=int,default=4096)
    parser.add_argument("--minimum-free-mib",type=int,default=4096)
    parser.add_argument("--once",action="store_true")
    args = parser.parse_args(argv)
    if not 60 <= args.interval_sec <= 3600 or not 1 <= args.duration_sec <= 604800 or not 5 <= args.max_cycle_sec <= 50 or not 128 <= args.max_daily_archive_mib <= 4096 or not 128 <= args.minimum_free_mib <= 65536 or len(args.model_study)>3:
        raise ValueError("bounded_v2_worker_parameters_required")
    if base.checked_path(args.archive_root) == base.checked_path(ROOT/"data/oanda_training_manager/feature_observations_v1"):
        raise ValueError("successor_observation_archive_required")
    return args


def retained_failure(path):
    """A restart must not erase the last failed cycle's original timestamp."""
    try:
        with base.checked_path(path).open("rb") as handle:
            raw = handle.read(1024*1024+1)
        if len(raw)>1024*1024:
            return None
        value = json.loads(raw)
        failure = value.get("last_failure")
        if value.get("schema_version")!=SCHEMA or value.get("worker")!=WORKER or not isinstance(failure,dict):
            return None
        begun,ended = base.parse_utc(failure.get("cycle_started_utc")),base.parse_utc(failure.get("failed_utc"))
        if begun is None or ended is None or begun>ended or not isinstance(failure.get("reason"),str):
            return None
        return {"cycle_started_utc":failure["cycle_started_utc"],"failed_utc":failure["failed_utc"],
                "error_type":str(failure.get("error_type") or "")[:80],"reason":failure["reason"][:240]}
    except (OSError,ValueError,TypeError,KeyError,AttributeError):
        return None


def main(argv=None):
    args = parse_args(argv)
    deadline, last_success = time.monotonic()+args.duration_sec, None
    last_failure = retained_failure(args.heartbeat)
    while time.monotonic() < deadline:
        begun = time.monotonic()
        cycle_started = base.utc_now()
        heartbeat = {"schema_version":SCHEMA,"worker":WORKER,"updated_at":cycle_started,
            "phase":"reading_sources","cycle_started_utc":cycle_started,
            "last_success":last_success,"last_failure":last_failure,**base.FLAGS}
        base.atomic_json(args.heartbeat, heartbeat)
        try:
            result = run_cycle(args)
            last_success = result
            last_failure = None
            heartbeat.update(phase="cycle_complete",result=result,last_success=last_success,last_failure=None)
        except (OSError,ValueError,TypeError,KeyError) as exc:
            last_failure = {"cycle_started_utc":cycle_started,"failed_utc":base.utc_now(),
                "error_type":type(exc).__name__,"reason":str(exc)[:240]}
            heartbeat.update(phase="cycle_failed",result={"status":"unavailable","error_type":type(exc).__name__,"reason":str(exc)[:240]})
            heartbeat["last_failure"] = last_failure
        heartbeat["updated_at"] = base.utc_now()
        base.atomic_json(args.heartbeat,heartbeat)
        if args.once:
            return 0 if heartbeat["phase"] == "cycle_complete" else 1
        pause = min(args.interval_sec-(time.monotonic()-begun),deadline-time.monotonic())
        if pause > 0:
            time.sleep(pause)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
