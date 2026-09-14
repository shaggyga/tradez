"""Prepare a new operational news/native-H1 cohort from the reviewed v7 kit.

No model, collector, transport, broker, or ledger is started. Existing source
registrations and study records remain untouched. Paths bind the actual existing
collector database and fresh canonical clock; no old observation is relabelled.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data/oanda_training_manager"
SCOPE = "operational_repair_20260913_v1"
RUNTIME = DATA / SCOPE
CONFIG = ROOT / "config"


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def rewrite(value, replacements):
    if isinstance(value, str):
        return replacements.get(value, value)
    if isinstance(value, list):
        return [rewrite(v, replacements) for v in value]
    if isinstance(value, dict):
        return {k: rewrite(v, replacements) for k, v in value.items()}
    return value


def prepare(*, version="v1", fresh_source=False):
    if version not in ("v1", "v2", "v3"):
        raise ValueError("explicit_operational_version_required")
    scope = "operational_repair_20260913_" + version
    runtime = DATA / scope
    original_path = CONFIG / "joint_price_news_study_v7_20260913_revision.json"
    original_raw = original_path.read_bytes()
    original = json.loads(original_raw)
    # Check every reviewed source byte before importing any project owner.
    mismatches = [name for name, expected in original["source_bindings"].items()
                  if sha((ROOT / name).read_bytes()) != expected]
    if mismatches:
        raise ValueError("reviewed_v7_source_mismatch:" + ",".join(mismatches))
    import oanda_joint_price_news_forecast_study_v7 as worker
    import revision_news_io_base_v1 as base_io
    import revision_news_io_v10 as io
    worker.load_registry(original_path)

    paths = {
        "base": CONFIG / f"revision_news_io_base_operational_{version}_20260913.json",
        "transport": CONFIG / f"revision_transport_operational_{version}_20260913.json",
        "io": CONFIG / f"revision_news_io_operational_{version}_20260913.json",
        "joint": CONFIG / f"joint_price_news_operational_{version}_20260913.json",
    }
    if any(p.exists() for p in paths.values()):
        raise ValueError("operational_registration_already_exists")
    collector_root = DATA / ("market_open_20260913_v1/local_news_sentiment" if fresh_source else "local_news_sentiment")
    source_db = collector_root / "local_news_sentiment_v1.sqlite"
    source_info = source_db.stat()
    news_root = runtime / "revision_news_v1"
    for directory in (news_root, news_root / "capture_archive", news_root / "transport_state"):
        directory.mkdir(parents=True, exist_ok=True)
    base = json.loads((CONFIG / "revision_news_io_base_v1_20260913.json").read_bytes())
    if version == "v3":
        if not fresh_source:
            raise ValueError("verified_fresh_revision_source_required")
        # The source-bound collector emits these optional identity fields as
        # absent, and uses the BLS output-kind alias below. Full immutable-row
        # replay verified this exact projection; source IDs, contracts/cohorts,
        # classifier, clocks and all code bindings remain unchanged.
        for identity in base["policy"]["sources"].values():
            for field in ("source_config_sha256", "source_lineage_version", "source_contract_derived"):
                identity[field] = None
            if identity.get("source_kind") == "bls_timeseries_batch":
                identity["source_kind"] = "bls_timeseries"
        if sha(encoded(base["policy"])) != "ce43c31699ee01459b7d0d25e4dc0049f4986d20e09784e38b7ca5e08d23ca4f":
            raise ValueError("replayed_emitted_identity_policy_changed")
    base.update(cohort_id=scope, consumer_id="joint_all68_native_v7_operational_"+version,
        publication_path=str(news_root / "publication.sqlite"),
        observation_path=str(news_root / "consumer_observations.sqlite"),
        archive_root=str(news_root / "capture_archive"),
        clock_path=str(DATA / "state/clock_integrity_v1.json"),
        heartbeat_path=str(collector_root / "collector_heartbeat_v1.json"),
        latest_path=str(collector_root / "collector_latest_v1.json"),
        input_identity={"path": str(source_db), "device": source_info.st_dev, "inode": source_info.st_ino})
    base_io.create_session(base)
    base_raw = encoded(base)
    transport = json.loads((CONFIG / "revision_transport_v4_20260913.json").read_bytes())
    transport.update(news_io_config_path=str(paths["base"]), news_io_config_sha256=sha(base_raw),
                     state_root=str(news_root / "transport_state"))
    transport_raw = encoded(transport)
    derived = deepcopy(base)
    derived.update(schema_version=io.CONFIG, transport_config_path=str(paths["transport"]),
                   transport_config_sha256=sha(transport_raw))
    io_raw = encoded(derived)
    registry = deepcopy(original)
    registry["news_io_config_sha256"] = sha(io_raw)
    for pair, item in registry["pairs"].items():
        for family, slot in item["families"].items():
            contract = slot["contract"]
            old_id = contract["contract_id"]
            new_id = old_id.rsplit(".", 1)[0] + ".operational_repair_" + version
            replacements = {
                old_id: new_id, old_id + ".evaluation": new_id + ".evaluation",
                contract["native_candle_path"]: str(DATA / "candles" / (pair + "_M1.csv")),
                contract["native_clock_path"]: base["clock_path"],
            }
            slot["contract"] = rewrite(contract, replacements)
            worker.validate_contract(slot["contract"])
            slot["contract_sha256"] = worker.digest(slot["contract"])
    blobs = {"base": base_raw, "transport": transport_raw, "io": io_raw, "joint": encoded(registry)}
    receipt = {"schema_version": "operational_news_preparation_v1_20260913",
        "prepared_utc": datetime.now(timezone.utc).isoformat(), "status": "prepared_not_activated",
        "source_registration": str(original_path), "source_registration_sha256": sha(original_raw),
        "source_files_verified": len(original["source_bindings"]), "pairs": len(registry["pairs"]),
        "runtime_root": str(runtime), "joint_study": str(runtime / "joint_price_news_study_v7"),
        "fresh_revision_source": fresh_source, "source_database": str(source_db),
        "historical_rows_imported": False, "workers_started": False, "orders_enabled": False,
        "configurations": {key: {"path": str(paths[key]), "sha256": sha(raw)} for key, raw in blobs.items()}}
    for key, raw in blobs.items():
        with paths[key].open("xb") as handle:
            handle.write(raw)
    io.create_session(derived)
    worker.load_registry(paths["joint"])
    receipt_path = runtime / "NEWS_PREPARATION_RECEIPT.json"
    with receipt_path.open("xb") as handle:
        handle.write(encoded(receipt))
    return receipt


if __name__ == "__main__":
    result = prepare()
    print(json.dumps({k: result[k] for k in ("status", "source_files_verified", "pairs", "runtime_root", "joint_study")}))
