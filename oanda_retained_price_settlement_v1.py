"""Settle retained v1/v2 forecasts without creating another forecast or fit.

Uses each original registered worker, ledger, quote checks and worker lock.
No old source/contract is changed and no missing ledger may be initialized.
"""
import argparse
from contextlib import ExitStack
import importlib
import json
import os
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data/oanda_training_manager"
SCHEMA = "retained_price_settlement_v1_20260913"


def settlement_class(owner, version):
    if version == 1:
        class Drain(owner.PairRunner):
            def schedule_fit(self):
                return None
    elif version == 2:
        class Drain(owner.PairRunner):
            def schedule_work(self):
                return None
    else:
        raise ValueError("retained_price_version_required")
    return Drain


def ledger_paths(registry, study, version):
    if version == 1:
        return [study / "pairs" / pair / "study.sqlite" for pair in registry["pairs"]]
    return [study / "pairs" / pair / family / "study.sqlite"
            for pair, item in registry["pairs"].items() for family in item["families"]]


def slots(runner, version):
    for state in runner.states.values():
        yield from ([state] if version == 1 else state["families"].values())


def counts(runner, version):
    result = {"forecasts": 0, "outcomes": 0, "exclusions": 0, "unresolved": 0}
    for slot in slots(runner, version):
        db = slot["ledger"].db
        for table in ("forecasts", "outcomes", "exclusions"):
            result[table] += db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
        result["unresolved"] += db.execute(
            "SELECT COUNT(*) FROM forecasts f LEFT JOIN outcomes o ON o.id=f.id "
            "LEFT JOIN exclusions x ON x.id=f.id WHERE o.id IS NULL AND x.id IS NULL").fetchone()[0]
    return result


def run(*, duration_sec=604800, heartbeat=None, versions=(1, 2), once=False):
    import msvcrt
    heartbeat = Path(heartbeat or DATA / "state/retained_price_settlement_v1.json")
    os.environ.update({key: "1" for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "LOKY_MAX_CPU_COUNT")})
    # Atomic writer is an original inert helper, not a new study publisher.
    import oanda_pair_local_forecast_study_v2 as writer
    start = time.monotonic()
    owners = []
    with ExitStack() as stack:
        for version in versions:
            owner = importlib.import_module(f"oanda_pair_local_forecast_study_v{version}")
            registry = owner.load_registry(owner.DEFAULT_CONFIG)
            study = owner.STUDY
            paths = ledger_paths(registry, study, version)
            if not paths or any(not p.is_file() for p in paths):
                raise ValueError("all_retained_ledgers_must_already_exist")
            lock = stack.enter_context((study / "worker.lock").open("r+b"))
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            stack.callback(lambda handle=lock: (handle.seek(0), msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)))
            runner = settlement_class(owner, version)(registry, study, DATA / "candles",
                DATA / "state/practice_007_market_quotes_v1.json")
            stack.callback(runner.close)
            original = counts(runner, version)
            owners.append((version, runner, original["forecasts"]))
        last_counts = 0
        records = []
        while True:
            began = time.monotonic()
            for version, runner, baseline in owners:
                runner.tick()
            if began - last_counts >= 15 or not records:
                records = []
                for version, runner, baseline in owners:
                    current = counts(runner, version)
                    if current["forecasts"] != baseline:
                        raise RuntimeError("retained_forecast_count_changed_during_settlement_only_run")
                    records.append({"version": version, **current, "errors": runner.errors,
                                    "last_error": runner.last_error})
                last_counts = began
            now = time.time()
            writer.atomic_json(heartbeat, {"schema_version": SCHEMA, "generated_epoch": now,
                "generated_utc": writer.utc(now), "status": "settling" if any(r["unresolved"] for r in records) else "settled",
                "pid": os.getpid(), "research_only": True, "can_place_orders": False,
                "new_forecasts_enabled": False, "studies": records})
            if once or began - start >= duration_sec:
                return records
            time.sleep(max(.05, 2 - (time.monotonic() - began)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-sec", type=float, default=604800)
    parser.add_argument("--heartbeat", type=Path)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(duration_sec=args.duration_sec, heartbeat=args.heartbeat, once=args.once)))
