"""Practice-only native M5/H1 research cache using the existing candle GET client.

No account query/order surface is called. Bootstrap and refresh are bounded;
the dedicated cache is atomically replaced, old project archives untouched.
Only explicit complete broker candles are admitted. Each output retains actual
source retrieval clocks and content identity in its adjacent receipt.
"""
from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import time

import oanda_all68_m1_forward_updater as existing
from oanda_feature_candle_inputs_v2 import SECONDS, stamp, validate_candle, read_tail

SCHEMA = "native_feature_candle_cache_v1_20260913"
FIELDS = ["time", "datetime", "instrument", "granularity", "complete", "open", "high", "low", "close",
          "bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low", "ask_close", "volume"]
FLAGS = {"research_only": True, "can_place_orders": False, "can_authorize": False, "can_promote": False}
ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data/oanda_training_manager"
MAX_ROWS = 1024
SCHEDULE = "native_boundary_priority_four_gets_v2_20260914"
MAX_INFLIGHT = 4
MAX_STARTS_PER_SECOND = 4
QUEUE_POLL_SEC = 5
RETRY_SEC = 15
MAX_CYCLE_SEC = 50


def utc():
    return datetime.now(timezone.utc)


def atomic(path, raw):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name+f".{os.getpid()}.tmp")
    try:
        with temporary.open("wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        for attempt in range(6):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(.02*(attempt+1))
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path, value):
    atomic(path, json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())


def response_rows(payload, pair, timeframe, observed):
    if payload.get("_error"):
        raise ValueError("broker_candle_get_failed_http_"+str(payload.get("_http_status")))
    if payload.get("instrument") != pair or payload.get("granularity") != timeframe:
        raise ValueError("broker_candle_response_identity")
    candles = payload.get("candles")
    if not isinstance(candles, list) or len(candles) > MAX_ROWS:
        raise ValueError("bounded_broker_candles_required")
    result, incomplete = [], 0
    for source in candles:
        if source.get("complete") is not True:
            incomplete += 1
            continue
        row = {"time": stamp(source["time"]).isoformat(), "complete": True, "volume": float(source["volume"])}
        for side in ("mid", "bid", "ask"):
            row[side] = {k: float(source[side][k]) for k in "ohlc"}
        validate_candle(row, SECONDS[timeframe], observed)
        if result and stamp(row["time"]) <= stamp(result[-1]["time"]):
            raise ValueError("broker_candles_duplicate_or_unsorted")
        flat = {"time": row["time"], "datetime": row["time"], "instrument": pair,
                "granularity": timeframe, "complete": "true", "volume": format(row["volume"], ".15g")}
        for side, prefix in (("mid", ""), ("bid", "bid_"), ("ask", "ask_")):
            for short, long in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close")):
                # Preserve exact decimal broker prices rather than float text.
                flat[prefix+long] = str(source[side][short])
        result.append(flat)
    return result, incomplete


def refresh_one(client, root, pair, timeframe, *, clock=utc):
    path = Path(root) / f"{pair}_{timeframe}.csv"
    old = []
    if path.exists():
        if path.is_symlink() or path.is_junction() or path.stat().st_size > 512*1024:
            raise ValueError("native_cache_path_or_byte_bound")
        old = list(csv.DictReader(io.StringIO(path.read_text(encoding="utf-8")) ))
        if len(old) > MAX_ROWS or any(r.get("instrument") != pair or r.get("granularity") != timeframe for r in old):
            raise ValueError("native_cache_identity_or_row_bound")
        read_tail(path,pair,timeframe,observed_utc=clock().isoformat())
    started = clock()
    # 720 completed H1 bars support 60 H4 primary bars across real weekends;
    # M5 retains enough history for all native/resampled structural windows.
    # The reused client clamps count to at least10; retain that actual GET count.
    count = 720 if len(old) < 300 else min(MAX_ROWS, max(10, math.ceil((started-stamp(old[-1]["time"])).total_seconds()/SECONDS[timeframe])+3))
    payload = client.candles(pair, granularity=timeframe, count=count, end_time=None, price="BAM")
    observed = clock()
    incoming, incomplete = response_rows(payload, pair, timeframe, observed)
    merged = {r["time"]: r for r in old}
    revisions = 0
    for row in incoming:
        if row["time"] in merged and merged[row["time"]] != row:
            revisions += 1
        merged[row["time"]] = row
    # This is a named bounded cache, never the immutable observation archive.
    rows = [merged[k] for k in sorted(merged)][-MAX_ROWS:]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    raw = output.getvalue().encode()
    if len(raw) > 512*1024:
        raise ValueError("native_cache_byte_bound")
    changed = not path.exists() or hashlib.sha256(path.read_bytes()).digest() != hashlib.sha256(raw).digest()
    if changed:
        atomic(path, raw)
    supported, history = read_tail(path,pair,timeframe,observed_utc=observed.isoformat())
    receipt = {"schema_version": SCHEMA, "instrument": pair, "timeframe": timeframe, "status": "current_source_read",
               "retrieval_started_utc": started.isoformat(), "retrieval_completed_utc": observed.isoformat(),
               "requested_count": count, "received_completed_rows": len(incoming), "incomplete_rows_withheld": incomplete,
               "retained_rows": len(rows), "earliest_bar_utc": rows[0]["time"] if rows else None,
               "latest_bar_utc": rows[-1]["time"] if rows else None, "cache_sha256": hashlib.sha256(raw).hexdigest(),
               "changed": changed, "broker_revisions_in_this_read": revisions,
               "supported_native_suffix_rows":len(supported),
               "calendar_contract":history["calendar_contract"],
               "scheduled_weekend_gaps_retained":history["scheduled_weekend_gaps_retained"],
               "discarded_rows_before_intraday_or_unknown_gap":history["discarded_rows_before_intraday_or_unknown_gap"],
               "cache_policy": "latest_1024_complete_native_bars; revisions_observed_now; old_feature_archives_immutable",
               "endpoint_scope": "practice_instrument_candles_GET_only", **FLAGS}
    write_json(path.with_suffix(".receipt.json"), receipt)
    return receipt


def due_priority(timeframe, receipt, now_epoch, last_attempt=None):
    """Original closed-bar boundaries override the maintenance polling delay."""
    if last_attempt is not None and now_epoch-last_attempt < RETRY_SEC:
        return None
    seconds = SECONDS[timeframe]
    expected_close = int(now_epoch)//seconds*seconds
    if not receipt:
        return (0 if timeframe == "H1" else 1, expected_close)
    retrieved = stamp(receipt["retrieval_completed_utc"]).timestamp()
    latest = stamp(receipt["latest_bar_utc"]).timestamp()+seconds if receipt.get("latest_bar_utc") else 0
    if retrieved > now_epoch or latest > now_epoch:
        raise ValueError("native_receipt_clock_from_future")
    if latest < expected_close:
        return (0 if timeframe == "H1" else 1, expected_close)
    cadence = 60 if timeframe == "M5" else 300
    if now_epoch-retrieved >= cadence:
        return (2 if timeframe == "M5" else 3, expected_close)
    return None


class DispatchBudget:
    """One shared rate window; never prequeue an unbounded executor backlog."""
    def __init__(self):
        self.starts = deque()

    def slots(self, now, inflight):
        while self.starts and self.starts[0] <= now-1:
            self.starts.popleft()
        return max(0,min(MAX_INFLIGHT-inflight,MAX_STARTS_PER_SECOND-len(self.starts)))

    def reserve(self, now):
        if not self.slots(now,0):
            raise ValueError("native_dispatch_rate_bound")
        self.starts.append(now)


def select_jobs(pairs, receipts, now_epoch, last_attempts, inflight, completed, limit):
    """Pure scheduling decision, including hour changes during an M5 cycle."""
    candidates=[]
    for pair in pairs:
        for timeframe in ("H1","M5"):
            key=(pair,timeframe)
            if key in inflight:
                continue
            priority=due_priority(timeframe,receipts.get(key),now_epoch,last_attempts.get(key))
            if priority is None or (*key,priority[1]) in completed:
                continue
            candidates.append((priority[0],pair,timeframe,priority[1]))
    return [(pair,timeframe,boundary) for _,pair,timeframe,boundary in sorted(candidates)[:limit]]


def run_cycle(client, args, *, clock=utc, scheduler_state=None):
    started, wall_started = clock(),time.monotonic()
    pairs = sorted(set(existing.priced_instruments(args.quote_snapshot)))
    if not 1 <= len(pairs) <= 68 or any(not re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", p) for p in pairs):
        raise ValueError("bounded_priced_pair_universe_required")
    state=scheduler_state if scheduler_state is not None else {}
    attempts=state.setdefault("last_attempts",{})
    rate=state.setdefault("rate",DispatchBudget())
    receipts,results,errors,completed={},[],[],set()
    for pair in pairs:
        for timeframe in ("H1","M5"):
            path=args.candle_root/f"{pair}_{timeframe}.receipt.json"
            if path.exists():
                try:
                    if path.stat().st_size>32768:
                        raise ValueError("native_receipt_byte_bound")
                    value=json.loads(path.read_text())
                    if value.get("schema_version")!=SCHEMA or value.get("instrument")!=pair or value.get("timeframe")!=timeframe:
                        raise ValueError("native_receipt_identity_mismatch")
                    due_priority(timeframe,value,clock().timestamp())
                    receipts[(pair,timeframe)]=value
                except (OSError,ValueError,KeyError,TypeError) as exc:
                    # A bad receipt is unavailable evidence for this input,
                    # not a reason to stall the other135 native inputs. A fresh
                    # GET may rebuild it only after original CSV validation.
                    errors.append({"instrument":pair,"timeframe":timeframe,
                        "reason":"prior_receipt_unavailable:"+type(exc).__name__+":"+str(exc)[:120]})
    submitted,active=0,{}
    last_heartbeat=-float("inf")
    def heartbeat():
        nonlocal last_heartbeat
        at=time.monotonic()
        if at-last_heartbeat<1:
            return
        last_heartbeat=at
        write_json(args.heartbeat,{"schema_version":SCHEMA,"scheduling_contract":SCHEDULE,
            "status":"updating","generated_utc":clock().isoformat(),"cycle_started_utc":started.isoformat(),
            "completed_requests":len(results),"error_count":len(errors),"inflight_gets":len(active),
            "max_inflight_gets":MAX_INFLIGHT,"max_get_starts_per_second":MAX_STARTS_PER_SECOND,
            "active_inputs":[{"instrument":p,"timeframe":tf} for p,tf,_ in active.values()],**FLAGS})
    # OandaClient.candles performs stateless requests.request calls. Its shared
    # credential/base URL fields are read-only; no mutable HTTP Session exists.
    with ThreadPoolExecutor(max_workers=MAX_INFLIGHT,thread_name_prefix="native-candle-get") as pool:
        while True:
            for future in [f for f in active if f.done()]:
                pair,timeframe,boundary=active.pop(future)
                try:
                    value=future.result();results.append(value);receipts[(pair,timeframe)]=value
                except (OSError,ValueError,KeyError,TypeError) as exc:
                    errors.append({"instrument":pair,"timeframe":timeframe,"reason":type(exc).__name__+":"+str(exc)[:150]})
                completed.add((pair,timeframe,boundary))
            now_epoch=clock().timestamp();now_wall=time.monotonic()
            slots=rate.slots(now_wall,len(active))
            can_launch=now_wall-wall_started<MAX_CYCLE_SEC and submitted<2*len(pairs)
            jobs=select_jobs(pairs,receipts,now_epoch,attempts,{(p,tf) for p,tf,_ in active.values()},completed,
                             min(slots,2*len(pairs)-submitted)) if can_launch else []
            for pair,timeframe,boundary in jobs:
                attempts[(pair,timeframe)]=now_epoch;rate.reserve(now_wall)
                future=pool.submit(refresh_one,client,args.candle_root,pair,timeframe,clock=clock)
                active[future]=(pair,timeframe,boundary);submitted+=1
            heartbeat()
            if not active:
                pending=select_jobs(pairs,receipts,now_epoch,attempts,set(),completed,1) if can_launch else []
                if not pending:
                    break
                # Rate window only; a new boundary is reconsidered after this
                # bounded pause. Never sleep through a queued H1 refresh.
                time.sleep(.1)
            else:
                wait(active,timeout=min(QUEUE_POLL_SEC,.25 if slots==0 else QUEUE_POLL_SEC),return_when=FIRST_COMPLETED)
    pending=select_jobs(pairs,receipts,clock().timestamp(),attempts,set(),completed,2*len(pairs))
    report={"schema_version":SCHEMA,"scheduling_contract":SCHEDULE,
        "status":"cycle_complete" if not errors else "partial_unavailable","generated_utc":clock().isoformat(),
        "cycle_started_utc":started.isoformat(),"pairs":len(pairs),"completed_requests":len(results),
        "submitted_requests":submitted,"configured_cache_receipts":len(receipts),
        "max_inflight_gets":MAX_INFLIGHT,"max_get_starts_per_second":MAX_STARTS_PER_SECOND,
        "boundary_queue_poll_sec":QUEUE_POLL_SEC,"minimum_retry_sec":RETRY_SEC,
        "skipped_not_due":2*len(pairs)-submitted,"cycle_launch_budget_exhausted":time.monotonic()-wall_started>=MAX_CYCLE_SEC,
        "deferred_due_inputs":len(pending),
        "errors":errors,"results":results,**FLAGS}
    write_json(args.report,report)
    write_json(args.heartbeat,{k:v for k,v in report.items() if k!="results"})
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candle-root", type=Path, default=DATA/"native_feature_candles_v1")
    parser.add_argument("--quote-snapshot", type=Path, default=existing.QUOTE_SNAPSHOT)
    parser.add_argument("--heartbeat", type=Path, default=DATA/"state/native_feature_candles_heartbeat_v1.json")
    parser.add_argument("--report", type=Path, default=DATA/"state/native_feature_candles_latest_v1.json")
    parser.add_argument("--duration-sec", type=float, default=172800)
    parser.add_argument("--interval-sec", type=float, default=60)
    parser.add_argument("--pause-sec", type=float, default=.1)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if not 60 <= args.interval_sec <= 3600 or not 0 <= args.pause_sec <= 2 or not 1 <= args.duration_sec <= 604800:
        raise ValueError("bounded_native_collector_parameters")
    client, _ = existing.resolve_readonly_oanda_client()
    stop = time.monotonic()+args.duration_sec
    scheduler_state={}
    while time.monotonic() < stop:
        begun = time.monotonic()
        run_cycle(client, args,scheduler_state=scheduler_state)
        if args.once:
            break
        time.sleep(max(0,min(QUEUE_POLL_SEC-(time.monotonic()-begun),stop-time.monotonic())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
