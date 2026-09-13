#!/usr/bin/env python3
"""Isolated, inactive-by-default source analog cohort with publication clocks.

Raw-event response labels remain diagnostic analog training data. A forecast
becomes usable only after its immutable row commits and an availability receipt
is visible. The rank consumer additionally records when it first observed that
receipt. No predecessor source ledger is read or imported.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import hashlib
from pathlib import Path
import sqlite3
import tempfile
import time
from typing import Any, Callable, Iterator, Mapping

import oanda_causal_source_factor_response_map_v8 as parent

base = parent.v7.v6.v1
ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config/source_factor_response_v9.json"
OUTPUT_DATABASE = parent.LOCAL_NEWS / "causal_source_factor_response_map_v9.sqlite"
SNAPSHOT_PATH = parent.LOCAL_NEWS / "causal_source_factor_response_map_latest_v9.json"
REPORT_PATH = parent.REPORT_ROOT / "CAUSAL_SOURCE_FACTOR_RESPONSE_MAP_V9.md"
SCHEMA_VERSION = "causal_source_factor_response_map_v9"
CONTRACT_ID = "causal_source_factor_response_map_v9_publication_availability_20260905"
PARENT_CONTRACT_ID = parent.CONTRACT_ID
REQUIRED_CLASSIFICATION_VERSION = "local_fx_news_rules_20260904_v164_conflict_duration_recap_guard"
REQUIRED_MAPPER_CONTRACT = parent.REQUIRED_MAPPER_CONTRACT
REQUIRED_PRE_MAP_QUOTE_CONTRACT = parent.REQUIRED_PRE_MAP_QUOTE_CONTRACT
HORIZONS_MIN = parent.HORIZONS_MIN
AVAILABILITY_CONTRACT = "source_forecast_committed_row_availability_v1_20260905"
_BASE_OPEN = parent._BASE_OPEN_OUTPUT_DATABASE
_BASE_INSERT_EVENTS = base.insert_events
_BASE_INSERT_RESPONSES = base.insert_responses
_BASE_BUILD_FORECAST = base.build_prequential_forecast
_BASE_RUN = base.run_cycle
_BASE_CANONICALIZE = parent._BASE_CANONICALIZE_OBSERVATIONS
_BASE_LOAD = parent._BASE_LOAD_CURRENT_OBSERVATIONS


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def clock_now(clock: Callable[[], datetime]) -> datetime:
    value=clock()
    if not isinstance(value,datetime) or value.tzinfo is None:
        raise ValueError("an explicit timezone-aware observed clock is required")
    return value.astimezone(timezone.utc)


def read_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if value.get("contract_id") != CONTRACT_ID or value.get("required_classification_version") != REQUIRED_CLASSIFICATION_VERSION:
        raise ValueError("V9 configuration contract mismatch")
    if value.get("historical_source_ledger_import_allowed") is not False:
        raise ValueError("V9 predecessor import forbidden")
    if type(value.get("collection_enabled")) is not bool:
        raise ValueError("V9 collection_enabled must be an explicit boolean")
    if value.get("research_only") is not True or any(value.get(key) is not False for key in ("can_place_orders", "can_authorize", "can_promote")):
        raise ValueError("V9 must remain research only")
    return value


def config_hash(config: Mapping[str, Any]) -> str:
    return base.digest(json.dumps({k: v for k, v in config.items() if k != "collection_enabled"}, sort_keys=True))


def implementation_hash() -> str:
    paths = [ROOT / f"oanda_causal_source_factor_response_map_v{version}.py" for version in range(1, 10)]
    paths += [ROOT / "src/forex_system/features/currency_state_engine.py"]
    values = {str(path.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    return base.digest(json.dumps(values, sort_keys=True))


def open_output_database(path: Path = OUTPUT_DATABASE) -> sqlite3.Connection:
    if path.exists():
        check = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
        try:
            tables = {row[0] for row in check.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "source_event_observation" in tables and "source_cohort_activation" not in tables:
                raise ValueError("V9 refuses a predecessor or foreign ledger schema")
            for table in ("source_event_observation", "source_factor_forecast", "source_event_response"):
                if table in tables and check.execute(f"SELECT 1 FROM {table} WHERE contract_id<>? LIMIT 1", (CONTRACT_ID,)).fetchone():
                    raise ValueError("V9 refuses predecessor or foreign ledger rows")
        finally:
            check.close()
    connection = _BASE_OPEN(path)
    try:
        # Reject foreign rows even when an explicit path is supplied.
        for table in ("source_event_observation", "source_factor_forecast", "source_event_response"):
            foreign = connection.execute(f"SELECT 1 FROM {table} WHERE contract_id<>? LIMIT 1", (CONTRACT_ID,)).fetchone()
            if foreign:
                raise ValueError("V9 refuses predecessor or foreign ledger rows")
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS source_cohort_activation(
              singleton INTEGER PRIMARY KEY CHECK(singleton=1),
              cohort_id TEXT NOT NULL, activated_utc TEXT NOT NULL,
              recorded_utc TEXT NOT NULL, config_sha256 TEXT NOT NULL,
              contract_id TEXT NOT NULL, implementation_sha256 TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS source_response_availability(
              response_id TEXT PRIMARY KEY, available_utc TEXT NOT NULL,
              contract_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS source_activation_visibility(
              singleton INTEGER PRIMARY KEY CHECK(singleton=1),
              registration_committed_utc TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS source_forecast_availability(
              forecast_id TEXT PRIMARY KEY, available_utc TEXT NOT NULL,
              forecast_payload_sha256 TEXT NOT NULL,
              availability_contract TEXT NOT NULL, contract_id TEXT NOT NULL,
              cohort_id TEXT NOT NULL,
              FOREIGN KEY(forecast_id) REFERENCES source_factor_forecast(forecast_id));
        """)
        for table in ("source_cohort_activation", "source_activation_visibility", "source_response_availability", "source_forecast_availability"):
            for action in ("UPDATE", "DELETE"):
                connection.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()} BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'append_only:{table}'); END")
        connection.commit()
        return connection
    except BaseException:
        connection.close()
        raise


def register_activation(
    output_database: Path, *, activated_utc: datetime,
    config_path: Path = CONFIG_PATH, clock: Callable[[], datetime] = utc_now,
) -> dict[str, Any]:
    """Explicit deployment step; never called implicitly by run_cycle.

    The injected clock is for offline fixtures. Production callers use the
    actual clock. A delayed deployment must register its own later activation.
    """
    config = read_config(config_path)
    recorded = clock_now(clock)
    if not isinstance(activated_utc,datetime) or activated_utc.tzinfo is None:
        raise ValueError("activation must be timezone-aware")
    activated = activated_utc.astimezone(timezone.utc)
    if activated <= recorded:
        raise ValueError("activation must follow, never precede, its actual registration")
    connection = open_output_database(output_database)
    try:
        if connection.execute("SELECT 1 FROM source_cohort_activation").fetchone():
            raise ValueError("activation is immutable; use a new cohort ledger")
        if connection.execute("SELECT 1 FROM source_event_observation LIMIT 1").fetchone():
            raise ValueError("activation requires an empty successor ledger")
        receipt = {"cohort_id": "source_v9_" + base.digest(CONTRACT_ID, base.iso(activated), config_hash(config), implementation_hash())[:24],
                   "activated_utc": base.iso(activated), "recorded_utc": base.iso(recorded),
                   "config_sha256": config_hash(config), "contract_id": CONTRACT_ID, "implementation_sha256": implementation_hash()}
        connection.execute("INSERT INTO source_cohort_activation VALUES (1,?,?,?,?,?,?)", tuple(receipt[k] for k in ("cohort_id", "activated_utc", "recorded_utc", "config_sha256", "contract_id", "implementation_sha256")))
        connection.commit()
        committed = clock_now(clock)
        if committed < recorded or committed > activated:
            raise ValueError("activation registration did not commit before its activation boundary; use a new cohort ledger")
        connection.execute("INSERT INTO source_activation_visibility VALUES (1,?)",(base.iso(committed),))
        connection.commit()
        receipt["registration_committed_utc"] = base.iso(committed)
        return receipt
    finally:
        connection.close()


def load_activation(path: Path, config: Mapping[str, Any]) -> dict[str, Any]:
    if not path.exists():
        raise ValueError("V9 has no registered activation")
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute("SELECT * FROM source_cohort_activation WHERE singleton=1").fetchone()
        if not row or row["contract_id"] != CONTRACT_ID or row["config_sha256"] != config_hash(config) or row["implementation_sha256"] != implementation_hash():
            raise ValueError("V9 activation/configuration binding mismatch")
        activated, recorded = base.parse_time(row["activated_utc"]), base.parse_time(row["recorded_utc"])
        if activated is None or recorded is None or activated <= recorded:
            raise ValueError("invalid V9 activation clocks")
        visibility = connection.execute("SELECT registration_committed_utc FROM source_activation_visibility WHERE singleton=1").fetchone()
        committed = base.parse_time(visibility[0]) if visibility else None
        if committed is None or not recorded <= committed <= activated:
            raise ValueError("activation lacks pre-boundary durable registration")
        expected = "source_v9_" + base.digest(CONTRACT_ID,base.iso(activated),config_hash(config),implementation_hash())[:24]
        if row["cohort_id"] != expected:
            raise ValueError("V9 activation identity mismatch")
        return {**dict(row),"registration_committed_utc":base.iso(committed)}
    finally:
        connection.close()


def canonicalize_observations(observations: Any, *, activation_utc: datetime) -> list[dict[str, Any]]:
    events = _BASE_CANONICALIZE(observations, activation_utc=activation_utc)
    for event in events:
        transports = event.get("transport_observations") or []
        clock_by_factor: dict[str, datetime] = {}
        exclusions = parent.v7.v6.v5._classifier_proof_exclusions(event)
        for transport in transports:
            raw, mapped = base.parse_time(transport.get("first_seen_utc")), base.parse_time(transport.get("mapped_utc"))
            if raw is None or mapped is None or mapped < raw:
                exclusions.append("semantic_availability_clock_invalid")
                continue
            for factor in base.structured_factors(transport.get("mapping_payload") or {}, raw):
                known = max(mapped, base.parse_time(factor["factor_known_utc"]))
                key = factor["factor_key"]
                clock_by_factor[key] = min(clock_by_factor.get(key, known), known)
        for factor in event["factors"]:
            factor["raw_input_known_utc"] = factor["factor_known_utc"]
            semantic = clock_by_factor.get(factor["factor_key"])
            if semantic is None:
                exclusions.append("factor_semantic_availability_missing")
            else:
                factor["factor_known_utc"] = base.iso(semantic)
        prospective = base.parse_time(event["first_known_utc"]) >= activation_utc
        event.update(prospective_proof_eligible=prospective and not exclusions,
                     evidence_class=("prospective_v9" if not exclusions else "prospective_v9_excluded_diagnostic") if prospective else "preactivation_diagnostic",
                     proof_exclusion_reasons=sorted(set(exclusions)),
                     response_label_role="raw_event_response_diagnostic_analog_only")
    return events


def insert_events(connection: sqlite3.Connection, events: Any) -> dict[str, int]:
    # Event response measurements retain their raw clock. Factors have a
    # separate semantic clock and cannot borrow the pre-map quote as an entry.
    counts = _BASE_INSERT_EVENTS(connection, [{**event, "factors": []} for event in events])
    for event in events:
        for factor in event["factors"]:
            factor_id = "source_factor_" + base.digest(event["canonical_event_id"], event["currency"], factor["factor_key"], CONTRACT_ID)[:32]
            before = connection.total_changes
            connection.execute("""INSERT OR IGNORE INTO source_factor_observation VALUES (?,?,?,?,?,?,?,?,?,1,0,?,?)""",
                (factor_id,event["canonical_event_id"],event["currency"],factor["factor_key"],factor["factor_type"],factor["factor_value"],factor["factor_known_utc"],event["evidence_class"],int(event["prospective_proof_eligible"]),CONTRACT_ID,base.COHORT_ID))
            counts["factors"] += connection.total_changes - before
    connection.commit()
    return counts


def training_rows(connection: sqlite3.Connection, factor: Mapping[str, Any], horizon: int, cutoff: datetime) -> list[dict[str, Any]]:
    rows = connection.execute("""
        SELECT sf.canonical_event_id,ep.market_episode_id,sf.currency,sf.factor_key,sf.factor_type,e.authority,
               r.currency_factor_bps,r.absolute_currency_factor_bps,r.maturity_utc
        FROM source_factor_observation sf JOIN source_event_observation e USING(canonical_event_id)
        JOIN source_event_episode ep USING(canonical_event_id) JOIN source_event_response r USING(canonical_event_id)
        JOIN source_response_availability a USING(response_id)
        WHERE r.horizon_min=? AND r.maturity_state='valid_canonical_ls_factor_and_bid_ask_paths'
          AND julianday(r.maturity_utc)<=julianday(?) AND julianday(a.available_utc)<=julianday(?)
          AND julianday(sf.factor_known_utc)<=julianday(?) AND sf.canonical_event_id<>? AND ep.market_episode_id<>?
          AND r.contract_id=? AND a.contract_id=?
    """, (horizon,base.iso(cutoff),base.iso(cutoff),base.iso(cutoff),factor["canonical_event_id"],factor["market_episode_id"],CONTRACT_ID,CONTRACT_ID)).fetchall()
    keys = ("canonical_event_id","market_episode_id","currency","factor_key","factor_type","authority","factor_bps","absolute_bps","maturity_utc")
    return [dict(zip(keys,row)) for row in rows]


def publish_availability(connection: sqlite3.Connection, *, clock: Callable[[], datetime]) -> int:
    # This commit must return before sampling the attested row availability.
    connection.commit()
    available = clock_now(clock)
    rows = connection.execute("""SELECT f.forecast_id,f.issued_utc,f.forecast_payload_json,f.cohort_id
        FROM source_factor_forecast f LEFT JOIN source_forecast_availability a USING(forecast_id)
        WHERE a.forecast_id IS NULL AND f.contract_id=?""", (CONTRACT_ID,)).fetchall()
    for forecast_id, issued, payload, cohort in rows:
        if base.parse_time(issued) > available:
            raise ValueError("forecast availability precedes actual issue")
        connection.execute("INSERT INTO source_forecast_availability VALUES (?,?,?,?,?,?)", (forecast_id,base.iso(available),base.digest(payload),AVAILABILITY_CONTRACT,CONTRACT_ID,cohort))
    connection.commit()
    return len(rows)


def insert_forecasts(connection: sqlite3.Connection, *, clock: Callable[[], datetime], minimum_n: int = base.MIN_EFFECTIVE_N) -> int:
    rows = connection.execute("""SELECT sf.factor_observation_id,sf.canonical_event_id,sf.currency,sf.factor_key,sf.factor_type,
        sf.factor_known_utc,sf.evidence_class,sf.prospective_proof_eligible,e.authority,ep.market_episode_id
        FROM source_factor_observation sf JOIN source_event_observation e USING(canonical_event_id)
        JOIN source_event_episode ep USING(canonical_event_id) ORDER BY sf.factor_known_utc,sf.factor_observation_id""").fetchall()
    keys = ("factor_observation_id","canonical_event_id","currency","factor_key","factor_type","factor_known_utc","evidence_class","prospective_proof_eligible","authority","market_episode_id")
    inserted = 0
    for row in rows:
        factor = dict(zip(keys,row))
        for horizon in HORIZONS_MIN:
            if connection.execute("SELECT 1 FROM source_factor_forecast WHERE factor_observation_id=? AND horizon_min=?", (factor["factor_observation_id"],horizon)).fetchone():
                continue
            # Training cutoff is observed before calculation; issue time is
            # sampled after calculation. Neither is raw first-seen time.
            cutoff = clock_now(clock)
            if base.parse_time(factor["factor_known_utc"]) > cutoff:
                continue
            forecast = _BASE_BUILD_FORECAST(connection, {**factor,"factor_known_utc":base.iso(cutoff)}, horizon, minimum_n=minimum_n)
            issued = clock_now(clock)
            if issued < cutoff:
                raise ValueError("forecast calculation clock moved backwards")
            timely = (issued - base.parse_time(factor["factor_known_utc"])).total_seconds() <= min(120.0, 60.0*horizon)
            eligible = bool(factor["prospective_proof_eligible"]) and timely
            forecast.update(issued_utc=base.iso(issued),training_cutoff_utc=base.iso(cutoff),semantic_available_utc=factor["factor_known_utc"],prospective_proof_eligible=eligible,
                            response_label_role="raw_event_response_diagnostic_analog_only",availability_contract=AVAILABILITY_CONTRACT)
            cols=("forecast_id","factor_observation_id","canonical_event_id","currency","factor_key","horizon_min","issued_utc","training_cutoff_utc","forecast_state","abstain_reason","backoff_level","backoff_key","raw_n","effective_event_n","probability_strengthening","predicted_currency_factor_bps","predicted_absolute_factor_bps","training_latest_maturity_utc")
            connection.execute("INSERT INTO source_factor_forecast VALUES ("+",".join("?" for _ in range(27))+")",tuple(forecast[k] for k in cols)+(json.dumps(forecast,sort_keys=True,separators=(",",":")),factor["evidence_class"],int(eligible),1,0,0,0,CONTRACT_ID,base.COHORT_ID))
            inserted += 1
    publish_availability(connection, clock=clock)
    return inserted


@contextmanager
def configured(activation: Mapping[str, Any], *, clock: Callable[[], datetime]) -> Iterator[None]:
    activated = base.parse_time(activation["activated_utc"])
    def responses(connection, rows):
        inserted = _BASE_INSERT_RESPONSES(connection, rows)
        # The base insertion has committed before this availability sample.
        available = base.iso(clock_now(clock))
        connection.execute("""INSERT OR IGNORE INTO source_response_availability SELECT response_id,?,? FROM source_event_response WHERE contract_id=?""", (available,CONTRACT_ID,CONTRACT_ID))
        connection.commit()
        return inserted
    replacements = {
        "SCHEMA_VERSION":SCHEMA_VERSION,"CONTRACT_ID":CONTRACT_ID,"COHORT_ID":activation["cohort_id"],"ACTIVATED_UTC":activated,
        "HORIZONS_MIN":HORIZONS_MIN,"OUTPUT_DATABASE":OUTPUT_DATABASE,"SNAPSHOT_PATH":SNAPSHOT_PATH,"REPORT_PATH":REPORT_PATH,
        "REQUIRED_CLASSIFICATION_VERSION":REQUIRED_CLASSIFICATION_VERSION,"REQUIRED_MAPPER_CONTRACT":REQUIRED_MAPPER_CONTRACT,
        "REQUIRED_PRE_MAP_QUOTE_CONTRACT":REQUIRED_PRE_MAP_QUOTE_CONTRACT,
        "POLICY":{**parent.POLICY,"parent_contract_id":PARENT_CONTRACT_ID,"required_classification_version":REQUIRED_CLASSIFICATION_VERSION,"v8_rows_imported":False,"availability_contract":AVAILABILITY_CONTRACT,"raw_event_response_is_diagnostic_only":True},
        "load_current_observations":lambda mapping,raw:_BASE_LOAD(mapping,raw,required_classifier=REQUIRED_CLASSIFICATION_VERSION,required_mapper_contract=REQUIRED_MAPPER_CONTRACT),
        "canonicalize_observations":lambda rows:canonicalize_observations(rows,activation_utc=activated),
        "open_output_database":open_output_database,"insert_events":insert_events,"insert_responses":responses,
        "_eligible_training_rows":training_rows,"insert_forecasts":lambda connection:insert_forecasts(connection,clock=clock),
    }
    previous={key:getattr(base,key) for key in replacements}
    try:
        for key,value in replacements.items(): setattr(base,key,value)
        yield
    finally:
        for key,value in previous.items(): setattr(base,key,value)


def run_cycle(*, config_path: Path = CONFIG_PATH, output_database: Path = OUTPUT_DATABASE,
              snapshot_path: Path = SNAPSHOT_PATH, report_path: Path = REPORT_PATH,
              clock: Callable[[], datetime] = utc_now, **inputs: Any) -> dict[str, Any]:
    config=read_config(config_path)
    now=clock_now(clock)
    if config.get("collection_enabled") is not True:
        return {"schema_version":SCHEMA_VERSION,"contract_id":CONTRACT_ID,"status":"inactive","generated_utc":base.iso(now),"research_only":True,"can_place_orders":False,"reason":"explicit_activation_and_collection_enable_required"}
    activation=load_activation(output_database,config)
    if now < base.parse_time(activation["activated_utc"]):
        return {"schema_version":SCHEMA_VERSION,"contract_id":CONTRACT_ID,"status":"awaiting_activation","generated_utc":base.iso(now),"research_only":True,"can_place_orders":False}
    # The inherited runner publishes an unwrapped snapshot before its report.
    # Keep those internal outputs private so partial work cannot replace the
    # last completed V9 state, including when either report write fails.
    snapshot_path.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".source-v9-",dir=snapshot_path.parent) as staging:
        staging_path=Path(staging)
        with configured(activation,clock=clock):
            result=_BASE_RUN(output_database=output_database,snapshot_path=staging_path/"snapshot.json",report_path=staging_path/"report.md",observed_utc=now,**inputs)
    result.update(status="ok",generated_utc=base.iso(clock_now(clock)),activation=activation,raw_event_response_is_diagnostic_only=True)
    counts=result.get("counts") or {}
    base.atomic_write(report_path,"# Source-factor response V9\n\nResearch only. Raw-event responses are diagnostic analog labels; selected entry performance belongs to publication-aware rank V8.\n\n"+
                      f"Contract: `{CONTRACT_ID}`. Cohort: `{activation['cohort_id']}`. Generated: `{result['generated_utc']}`.\n\n"+
                      f"Events: {counts.get('canonical_events',0)}. Prospective input events: {counts.get('prospective_proof_events',0)}. Forecasts: {counts.get('forecasts',0)}. No performance confirmation is implied.\n")
    base.atomic_write(snapshot_path,json.dumps(result,indent=2,sort_keys=True)+"\n")
    return result


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",type=Path,default=CONFIG_PATH)
    parser.add_argument("--once",action="store_true")
    parser.add_argument("--interval-sec",type=float,default=5.0)
    parser.add_argument("--duration-sec",type=float,default=0.0)
    args=parser.parse_args()
    started=time.monotonic()
    while True:
        result=run_cycle(config_path=args.config)
        print(json.dumps({key:result.get(key) for key in ("status","contract_id","generated_utc","counts")},sort_keys=True),flush=True)
        if args.once or result["status"]=="inactive" or (args.duration_sec>0 and time.monotonic()-started>=args.duration_sec):
            break
        time.sleep(max(1.0,args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
