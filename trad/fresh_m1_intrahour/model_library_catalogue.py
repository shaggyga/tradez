from __future__ import annotations

import argparse
import ast
import csv
import gzip
import hashlib
import importlib.metadata
import json
import math
import os
import re
import shutil
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Iterator


SCHEMA_VERSION = "forex_model_library_v1"
ASSET_DOMAIN = "forex_project"
STATUS_VALUES = {
    "VALIDATED",
    "RESEARCH_LEAD",
    "FAILED",
    "INVALIDATED",
    "SUPERSEDED",
    "UNVERIFIED",
    "SMOKE_ONLY",
}

EXCLUDED_DIR_NAMES = {
    ".git",
    ".pytest_cache",
    ".research_py313",
    ".venv",
    "..venv",
    "__pycache__",
    "env",
    "node_modules",
    "venv",
}

BULK_MODEL_EVIDENCE_RE = re.compile(
    r"(?:promoted_model_manifest|ensemble_shadow_manifest|model_manifest|model_scorecard|model_validation|arima|sarima)",
    re.IGNORECASE,
)

SMOKE_REPORT_DIRS = {
    "arima_baseline_20260710_144310",
    "htf_corrected_rebuild_tier1_20260710_v2",
    "sarima_full_tier1_2025_2026_20260710",
    "two_pending_oco_breakout_tier1_bounded_20260709",
    "validation_replay_leakfree_walkforward_20260710_v3",
}

MODEL_KEYS = {
    "algorithm",
    "estimator",
    "family",
    "model",
    "model_family",
    "model_name",
    "model_type",
    "strategy",
    "strategy_family",
    "strategy_id",
    "variant",
}

IDENTITY_KEYS = {
    "candidate_id",
    "experiment_id",
    "model_id",
    "run_id",
    "spec_id",
    "strategy_id",
}

SPEC_KEYS = {
    "allow_short",
    "calibration",
    "calibration_method",
    "classifier",
    "entry_policy",
    "feature_family",
    "feature_set",
    "features",
    "gate",
    "horizon",
    "horizons",
    "label",
    "label_family",
    "lag",
    "lags",
    "lookback",
    "max_depth",
    "max_iter",
    "max_leaf_nodes",
    "min_samples_leaf",
    "min_samples_split",
    "min_members",
    "members",
    "member_ids",
    "base_models",
    "components",
    "ensemble_mode",
    "n_estimators",
    "order",
    "outcome_horizon",
    "p",
    "parameters",
    "parameters_json",
    "params",
    "policy",
    "q",
    "regressor",
    "seasonal_order",
    "seasonal_period",
    "threshold",
    "timeframe",
    "timeframes",
    "weights",
    "window",
    "windows",
}

ASSET_KEYS = {
    "asset",
    "assets",
    "instrument",
    "instrument_scope",
    "instrument_subset",
    "instruments",
    "pair",
    "pairs",
    "pairs_requested",
    "symbol",
    "symbols",
}

TIMEFRAME_KEYS = {
    "bar_size",
    "granularity",
    "horizon",
    "horizons",
    "outcome_horizon",
    "timeframe",
    "timeframes",
}

DATE_KEY_RE = re.compile(
    r"(?:^|_)(?:start|end|from|to|date|time|timestamp|generated|created|updated|train|test|validation|calibration|holdout)(?:_|$)",
    re.IGNORECASE,
)

SENSITIVE_KEYS = {
    "access_token",
    "account_id",
    "account_number",
    "api_key",
    "api_secret",
    "authorization",
    "client_secret",
    "credential",
    "credentials",
    "oanda_account_id",
    "oanda_token",
    "password",
    "private_key",
    "refresh_token",
    "secret",
    "token",
}

SENSITIVE_PATH_RE = re.compile(
    r"(?:^|[_.-])(?:credential|credentials|secret|secrets|token|tokens|password|private[_-]?key)(?:[_.-]|$)",
    re.IGNORECASE,
)

UNSAFE_SOURCE_RE = re.compile(
    r"(?:account_manager|live_account|live_day|live_recap|live_status|rotation_bot|executor|flatten_all|order_adapter|promoter)",
    re.IGNORECASE,
)

SAFE_SOURCE_HINT_RE = re.compile(
    r"(?:research|backtest|model|pipeline|validation|arima|sarima|strategy|ensemble|forecast|indicator|spike|sweep|replay|ruleset|feature|metric|audit|label|economics)",
    re.IGNORECASE,
)

FORECAST_METRIC_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("rmse", re.compile(r"(?:^|_)(?:forecast_)?rmse(?:_|$)", re.I)),
    ("mae", re.compile(r"(?:^|_)(?:forecast_)?mae(?:_|$)", re.I)),
    ("median_absolute_error", re.compile(r"median.*absolute.*error", re.I)),
    ("mape", re.compile(r"(?:^|_)mape(?:_|$)", re.I)),
    ("bias", re.compile(r"(?:forecast_)?bias", re.I)),
    ("rank_ic", re.compile(r"(?:rank_?ic|spearman)", re.I)),
    ("correlation", re.compile(r"(?:forecast_)?(?:correlation|pearson)", re.I)),
    ("direction_accuracy", re.compile(r"(?:direction.*accuracy|accuracy.*direction|hit_rate)", re.I)),
    ("balanced_accuracy", re.compile(r"balanced_accuracy", re.I)),
    ("precision", re.compile(r"precision(?:_at_?\d+|@\d+|$)", re.I)),
    ("recall", re.compile(r"(?:^|_)recall(?:_|$)", re.I)),
    ("f1", re.compile(r"(?:^|_)f1(?:_|$)", re.I)),
    ("roc_auc", re.compile(r"(?:roc_?auc|auc_roc)", re.I)),
    ("pr_auc", re.compile(r"(?:pr_?auc|average_precision)", re.I)),
    ("brier", re.compile(r"brier", re.I)),
    ("log_loss", re.compile(r"log_?loss", re.I)),
    ("calibration_error", re.compile(r"(?:ece|calibration.*error)", re.I)),
    ("actual_ev", re.compile(r"actual.*ev|net.*ev|gross.*ev", re.I)),
    ("movement_quality", re.compile(r"movement.*(?:quality|accuracy|rmse|mae|ic)", re.I)),
    ("mfe_quality", re.compile(r"mfe.*(?:quality|accuracy|rmse|mae|ic)", re.I)),
]

TRADING_METRIC_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("pnl", re.compile(r"(?:^|_)(?:pnl|p_l|pl|profit_usd|net_profit|account_currency_p_l)(?:_|$)", re.I)),
    ("return_pct", re.compile(r"return.*pct|return_percent", re.I)),
    ("trades", re.compile(r"(?:^|_)(?:trades|total_trades|trade_count)(?:_|$)", re.I)),
    ("win_rate", re.compile(r"win_rate", re.I)),
    ("profit_factor", re.compile(r"profit_factor", re.I)),
    ("max_drawdown", re.compile(r"max.*drawdown", re.I)),
    ("cost_drag", re.compile(r"cost.*drag|spread.*cost|slippage.*cost", re.I)),
    ("gross_ev", re.compile(r"gross.*ev", re.I)),
    ("net_ev", re.compile(r"net.*ev", re.I)),
]

KNOWN_ESTIMATORS = {
    "ARIMA",
    "AutoReg",
    "CalibratedClassifierCV",
    "ExtraTreesClassifier",
    "ExtraTreesRegressor",
    "GradientBoostingClassifier",
    "GradientBoostingRegressor",
    "HistGradientBoostingClassifier",
    "HistGradientBoostingRegressor",
    "LinearRegression",
    "LogisticRegression",
    "RandomForestClassifier",
    "RandomForestRegressor",
    "Ridge",
    "RidgeClassifier",
    "SARIMAX",
}


@dataclass(frozen=True)
class BuildPaths:
    library_root: Path
    build_root: Path
    build_id: str
    mode: str


class EvidenceSnapshotWriter:
    def __init__(self, build_root: Path, mode: str, shard_size: int = 250) -> None:
        self.build_root = build_root
        self.mode = mode
        self.shard_size = shard_size
        self.shard_index = -1
        self.line_in_shard = 0
        self.handle: Any = None
        self.current_relative: str | None = None
        self.shard_hashes: dict[str, str] = {}

    def _open_next_shard(self) -> None:
        self._close_current()
        self.shard_index += 1
        self.line_in_shard = 0
        self.current_relative = f"snapshots/evidence_shards/evidence_{self.shard_index:05d}.jsonl.gz"
        path = self.build_root / self.current_relative
        path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = gzip.open(path, "wt", encoding="utf-8", newline="\n", compresslevel=6)

    def _close_current(self) -> None:
        if self.handle is None or self.current_relative is None:
            return
        self.handle.close()
        self.shard_hashes[self.current_relative] = file_sha256(self.build_root / self.current_relative)
        self.handle = None

    def add(self, artifact_id: str, payload: Any) -> dict[str, Any]:
        payload_json = canonical_json(payload)
        payload_sha = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        if self.mode == "smoke":
            relative = f"snapshots/evidence/{artifact_id.removeprefix('artifact_')}.json"
            path = self.build_root / relative
            if not path.exists():
                write_json(path, payload, pretty=False)
            return {
                "snapshot_format": "individual_json",
                "sanitized_snapshot_path": relative,
                "sanitized_snapshot_line": None,
                "sanitized_payload_sha256": payload_sha,
                "sanitized_snapshot_sha256": file_sha256(path),
            }
        if self.handle is None or self.line_in_shard >= self.shard_size:
            self._open_next_shard()
        assert self.handle is not None and self.current_relative is not None
        line_number = self.line_in_shard + 1
        self.handle.write(canonical_json({"artifact_id": artifact_id, "payload": payload}) + "\n")
        self.line_in_shard += 1
        return {
            "snapshot_format": "gzip_jsonl_shard",
            "sanitized_snapshot_path": self.current_relative,
            "sanitized_snapshot_line": line_number,
            "sanitized_payload_sha256": payload_sha,
            "sanitized_snapshot_sha256": None,
        }

    def close(self) -> dict[str, str]:
        self._close_current()
        return dict(self.shard_hashes)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def slug(value: Any, *, fallback: str = "unknown") -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text[:96] or fallback


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def stable_id(prefix: str, value: Any, length: int = 24) -> str:
    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()[:length]
    return f"{prefix}_{digest}"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(8 * 1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, payload: Any, *, pretty: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(
            payload,
            handle,
            indent=2 if pretty else None,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
            separators=None if pretty else (",", ":"),
        )
        handle.write("\n")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8", newline="\n")


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(canonical_json(row) + "\n")
            count += 1
    return count


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL at {path}:{line_number}: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"JSONL row is not an object at {path}:{line_number}")
            rows.append(value)
    return rows


def find_unredacted_sensitive_values(value: Any, pointer: str = "") -> list[str]:
    findings: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{pointer}/{key}"
            if str(key).lower() in SENSITIVE_KEYS and item != "[REDACTED]":
                findings.append(child)
            findings.extend(find_unredacted_sensitive_values(item, child))
            if len(findings) >= 20:
                break
    elif isinstance(value, list):
        for index, item in enumerate(value):
            findings.extend(find_unredacted_sensitive_values(item, f"{pointer}/{index}"))
            if len(findings) >= 20:
                break
    return findings


def finite_json(value: Any, counters: Counter[str] | None = None) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        if counters is not None:
            counters["nonfinite_values_replaced"] += 1
        return None
    if isinstance(value, dict):
        return {str(key): finite_json(item, counters) for key, item in value.items()}
    if isinstance(value, list):
        return [finite_json(item, counters) for item in value]
    return value


def sanitize_json(value: Any, counters: Counter[str] | None = None, key: str | None = None) -> Any:
    if key and key.lower() in SENSITIVE_KEYS:
        if counters is not None:
            counters["sensitive_values_redacted"] += 1
        return "[REDACTED]"
    if isinstance(value, float) and not math.isfinite(value):
        if counters is not None:
            counters["nonfinite_values_replaced"] += 1
        return None
    if isinstance(value, dict):
        return {str(k): sanitize_json(v, counters, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_json(item, counters, key) for item in value]
    return value


def is_sensitive_path(path: Path) -> bool:
    return any(SENSITIVE_PATH_RE.search(part) for part in path.parts)


def relative_posix(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def iter_project_files(project_root: Path, mode: str) -> Iterator[Path]:
    for current, dirs, files in os.walk(project_root, topdown=True, onerror=lambda _: None):
        current_path = Path(current)
        dirs[:] = [
            name
            for name in dirs
            if name not in EXCLUDED_DIR_NAMES
            and not (current_path == project_root and name == "FOREX_MODEL_LIBRARY")
        ]
        for name in files:
            path = current_path / name
            rel = relative_posix(path, project_root)
            if rel.startswith("data/oanda_training_manager/.research_py313/"):
                continue
            if rel.startswith(".pytest_cache/") or "/__pycache__/" in rel:
                continue
            if Path(rel).name.lower().startswith("kraken_"):
                continue
            if mode == "smoke" and not smoke_file_in_scope(rel):
                continue
            yield path


def smoke_file_in_scope(rel: str) -> bool:
    posix = rel.replace("\\", "/")
    parts = posix.split("/")
    if posix == "fresh_m1_intrahour/config.json":
        return True
    if posix.startswith("fresh_m1_intrahour/src/") and posix.endswith(".py"):
        return True
    if posix in {
        "fresh_m1_intrahour/run_pipeline.py",
        "fresh_m1_intrahour/sarima_sweep_worker.py",
        "fresh_m1_intrahour/model_library_catalogue.py",
    }:
        return True
    if len(parts) >= 3 and parts[:2] == ["fresh_m1_intrahour", "reports"]:
        return parts[2] in SMOKE_REPORT_DIRS and Path(posix).suffix.lower() in {".json", ".md", ".csv"}
    source_name = Path(posix).name.lower()
    return source_name in {
        "oanda_arima_baseline_grid.py",
        "oanda_arima_h1_baseline_grid.py",
        "oanda_ensemble_candidate_backtester.py",
        "seven_major_multihorizon_models.py",
    }


def bulk_event_scope(rel: str) -> str | None:
    normalized = rel.replace("\\", "/")
    if normalized.startswith("data/forex/logging/"):
        return "data/forex/logging"
    match = re.match(r"(data/archive/partial_raw_event_json_from_migration_[^/]+)/", normalized)
    if match:
        return match.group(1)
    return None


def bulk_model_evidence_hint(rel: str) -> bool:
    return bool(BULK_MODEL_EVIDENCE_RE.search(Path(rel).name))


def bulk_event_type(rel: str) -> str:
    name = Path(rel).name
    stem = re.sub(r"\.json$", "", name, flags=re.I)
    stem = re.sub(r"^\d{8}_\d{6}_", "", stem)
    stem = re.sub(r"_[0-9a-f]{12,64}$", "", stem, flags=re.I)
    return slug(stem, fallback="unknown_event")


def bulk_filename_timestamp(rel: str) -> str | None:
    match = re.search(r"(?:^|/)(\d{8})_(\d{6})_", rel.replace("\\", "/"))
    if not match:
        return None
    date, time = match.groups()
    return f"{date[:4]}-{date[4:6]}-{date[6:8]}T{time[:2]}:{time[2:4]}:{time[4:6]}"


def add_bulk_event(
    collections: dict[str, dict[str, Any]], path: Path, rel: str, scope: str
) -> None:
    event_type = bulk_event_type(rel)
    key = f"{scope}|{event_type}"
    if key not in collections:
        collections[key] = {
            "scope": scope,
            "event_type": event_type,
            "file_count": 0,
            "bytes": 0,
            "start": None,
            "end": None,
            "suffix_counts": Counter(),
            "metadata_hasher": hashlib.sha256(),
        }
    record = collections[key]
    stat = path.stat()
    record["file_count"] += 1
    record["bytes"] += stat.st_size
    record["suffix_counts"][path.suffix.lower() or "[none]"] += 1
    timestamp = bulk_filename_timestamp(rel)
    if timestamp:
        record["start"] = timestamp if record["start"] is None else min(record["start"], timestamp)
        record["end"] = timestamp if record["end"] is None else max(record["end"], timestamp)
    record["metadata_hasher"].update(f"{rel}|{stat.st_size}\n".encode("utf-8"))


def finalize_bulk_collections(
    collections: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    artifacts: list[dict[str, Any]] = []
    datasets: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for key, source in sorted(collections.items()):
        digest = source["metadata_hasher"].hexdigest()
        artifact_id = stable_id(
            "artifact_collection",
            {"scope": source["scope"], "event_type": source["event_type"], "metadata_sha256": digest},
            length=32,
        )
        pattern = f"{source['scope']}/**/*{source['event_type']}*.json"
        artifact = {
            "artifact_id": artifact_id,
            "sha256": digest,
            "hash_status": "collection_relative_path_and_size_manifest",
            "bytes": source["bytes"],
            "role": "event_log_collection",
            "suffix": ".json",
            "paths": [pattern],
            "path_count": source["file_count"],
            "copied_to_vault": False,
        }
        dataset_id = stable_id("dataset", {"artifact_id": artifact_id, "role": "event_log_collection"})
        dataset = {
            "dataset_id": dataset_id,
            "artifact_id": artifact_id,
            "role": "event_log_collection",
            "path": pattern,
            "format": "json_collection",
            "pair": None,
            "timeframe": None,
            "date_coverage": {
                "method": "filename_timestamp_range",
                "start": source["start"],
                "end": source["end"],
            },
            "file_count": source["file_count"],
            "bytes": source["bytes"],
            "event_type": source["event_type"],
            "metadata_identity_sha256": digest,
            "content_hash_status": "not_computed_for_non_model_runtime_events",
            "copied_to_vault": False,
            "replication_contract": "Recover the collection at the recorded scope; metadata identity covers sorted relative paths and byte sizes, not event contents.",
        }
        row = {
            "collection_id": stable_id("event_collection", key),
            "scope": source["scope"],
            "event_type": source["event_type"],
            "file_count": source["file_count"],
            "bytes": source["bytes"],
            "date_coverage": dataset["date_coverage"],
            "suffix_counts": dict(sorted(source["suffix_counts"].items())),
            "artifact_id": artifact_id,
            "dataset_id": dataset_id,
            "catalogue_treatment": "associated_event_dataset_not_model_run",
        }
        artifacts.append(artifact)
        datasets.append(dataset)
        rows.append(row)
    return artifacts, datasets, rows


def artifact_role(rel: str) -> str:
    lower = rel.lower()
    suffix = Path(lower).suffix
    if "/candles/" in f"/{lower}":
        return "raw_candle_dataset"
    if "/training_sets/" in f"/{lower}":
        return "training_dataset"
    if "/features/" in f"/{lower}":
        return "feature_dataset"
    if suffix in {".joblib", ".pkl", ".pickle"}:
        return "serialized_model"
    if suffix == ".py" and "/reports/" in f"/{lower}":
        return "source_snapshot_evidence"
    if suffix == ".py":
        return "source_code"
    if suffix == ".json":
        return "json_evidence"
    if suffix in {".parquet", ".csv", ".feather"}:
        return "research_table"
    if suffix in {".md", ".txt"}:
        return "documentation"
    if suffix in {".zip", ".7z", ".tar", ".gz"}:
        return "archive"
    return "project_artifact"


def evidence_era(rel: str) -> str:
    lower = rel.lower()
    if lower.startswith("fresh_m1_intrahour/"):
        if any(
            marker in lower
            for marker in (
                "phase1_",
                "validation_replay_leakfree",
                "htf_corrected",
                "sarima_full_tier1_2025_2026",
            )
        ):
            return "corrected_execution_research"
        return "fresh_engine_prevalidation_research"
    if "/_archive/" in f"/{lower}" or "/archive/" in f"/{lower}":
        return "legacy_archived_accounting_unverified"
    return "legacy_accounting_unverified"


def infer_asset_class(assets: list[str]) -> str:
    joined = " ".join(assets).upper()
    if any(token in joined for token in ("XAU", "XAG", "GOLD", "SILVER")):
        return "commodities_metals"
    if any(re.fullmatch(r"[A-Z]{3}[_/-][A-Z]{3}", item.upper()) for item in assets):
        return "foreign_exchange"
    return "foreign_exchange_or_unspecified"


def normalize_scalar(value: Any) -> Any:
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return str(value)


def short_value(value: Any, limit: int = 4000) -> Any:
    if isinstance(value, dict):
        return {str(k): short_value(v, limit) for k, v in list(value.items())[:128]}
    if isinstance(value, list):
        return [short_value(v, limit) for v in value[:128]]
    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + "...[truncated]"
    return normalize_scalar(value)


def iter_objects(value: Any, pointer: str = "", depth: int = 0) -> Iterator[tuple[str, dict[str, Any]]]:
    if depth > 12:
        return
    if isinstance(value, dict):
        yield pointer or "/", value
        for key, item in value.items():
            escaped = str(key).replace("~", "~0").replace("/", "~1")
            yield from iter_objects(item, f"{pointer}/{escaped}", depth + 1)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from iter_objects(item, f"{pointer}/{index}", depth + 1)


def flatten_scalars(value: Any, prefix: str = "", depth: int = 0) -> Iterator[tuple[str, Any]]:
    if depth > 6:
        return
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            yield from flatten_scalars(item, path, depth + 1)
    elif isinstance(value, list):
        if len(value) <= 32:
            for index, item in enumerate(value):
                yield from flatten_scalars(item, f"{prefix}[{index}]", depth + 1)
    elif isinstance(value, (str, int, float, bool)) or value is None:
        yield prefix, normalize_scalar(value)


def metric_semantic(path: str) -> tuple[str, str] | None:
    key = path.split(".")[-1]
    for semantic, pattern in FORECAST_METRIC_PATTERNS:
        if pattern.search(key):
            return "forecast", semantic
    for semantic, pattern in TRADING_METRIC_PATTERNS:
        if pattern.search(key):
            return "trading", semantic
    return None


def extract_metrics(obj: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {"forecast": [], "trading": []}
    seen: set[tuple[str, str, str]] = set()
    for path, value in flatten_scalars(obj):
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            continue
        semantic = metric_semantic(path)
        if semantic is None:
            continue
        group, name = semantic
        marker = (group, name, path)
        if marker in seen:
            continue
        seen.add(marker)
        result[group].append({"metric": name, "path": path, "value": value})
        if len(result[group]) >= 128:
            break
    return result


def collect_values(obj: dict[str, Any], keys: set[str], *, max_items: int = 128) -> list[str]:
    values: list[str] = []
    for path, value in flatten_scalars(obj):
        key = re.sub(r"\[\d+\]$", "", path.split(".")[-1]).lower()
        if key not in keys or value in (None, ""):
            continue
        text = str(value).strip()
        if text and text not in values:
            values.append(text)
        if len(values) >= max_items:
            break
    return values


def extract_dates(obj: dict[str, Any]) -> dict[str, str]:
    dates: dict[str, str] = {}
    for path, value in flatten_scalars(obj):
        normalized_path = re.sub(r"[^A-Za-z0-9]+", "_", path)
        if not isinstance(value, str) or not DATE_KEY_RE.search(normalized_path):
            continue
        if re.search(r"\d{4}[-/]\d{2}[-/]\d{2}", value):
            dates[path] = value[:128]
        if len(dates) >= 32:
            break
    return dates


def model_descriptor(obj: dict[str, Any], rel: str) -> list[str]:
    descriptors: list[str] = []
    for key in MODEL_KEYS:
        value = obj.get(key)
        if isinstance(value, (str, int, float)) and str(value).strip():
            descriptors.append(str(value).strip())
    nested_spec = obj.get("spec")
    if isinstance(nested_spec, dict):
        for key in MODEL_KEYS | {"name", "type"}:
            value = nested_spec.get(key)
            if isinstance(value, (str, int, float)) and str(value).strip():
                descriptors.append(str(value).strip())
    lower = rel.lower()
    keys = {str(key).lower() for key in obj}
    if not descriptors:
        if "sarima" in lower or "seasonal_order" in keys:
            descriptors.append("SARIMA")
        elif "arima" in lower or "arma" in lower or ("order" in keys and {"p", "q"}.intersection(keys)):
            descriptors.append("ARIMA")
    if not descriptors:
        for marker, label in (
            ("two_pending_oco", "Two Pending OCO Breakout"),
            ("validation_replay", "Validation Replay Candidate"),
            ("signal_regression", "Signal Regression Candidate"),
            ("htf_corrected", "HTF Corrected Candidate"),
        ):
            if marker in lower:
                descriptors.append(label)
                break
    return list(dict.fromkeys(descriptors))[:16]


def is_model_candidate(obj: dict[str, Any], rel: str) -> bool:
    direct_keys = {str(key).lower() for key in obj}
    descriptors = model_descriptor(obj, rel)
    if not descriptors:
        return False
    nested_spec = obj.get("spec")
    explicit_model = bool(direct_keys & MODEL_KEYS) or (
        isinstance(nested_spec, dict)
        and bool({str(key).lower() for key in nested_spec} & (MODEL_KEYS | {"name", "type"}))
    )
    has_identity = bool(direct_keys & IDENTITY_KEYS)
    has_spec = bool(direct_keys & SPEC_KEYS) or isinstance(nested_spec, dict)
    has_direct_metric = any(
        metric_semantic(str(key))
        for key, value in obj.items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    )
    if explicit_model:
        has_nested_metric = any(metric_semantic(path) for path, _ in flatten_scalars(obj))
        return has_identity or has_spec or has_direct_metric or has_nested_metric or len(obj) > 1
    return has_identity or has_spec


def canonical_family(descriptors: list[str], rel: str) -> tuple[str, str, str]:
    text = " ".join(descriptors + [rel]).lower()
    rules = [
        (r"sarima|sarimax|seasonal.*arima", "sarima", "SARIMA / SARIMAX", "classical_time_series"),
        (r"arima|arma|autoreg|\bar\(?\d", "arima_ar", "ARIMA / AR / ARMA", "classical_time_series"),
        (r"extra.?trees", "extra_trees", "Extra Trees", "machine_learning"),
        (r"hist.*gradient|histgradient|(?:^|[_\s])hgb(?:[_\s]|$)", "hist_gradient_boosting", "Histogram Gradient Boosting", "machine_learning"),
        (r"random.?forest|trade_quality_rf", "random_forest", "Random Forest", "machine_learning"),
        (r"gradient.?boost", "gradient_boosting", "Gradient Boosting", "machine_learning"),
        (r"xgboost|\bxgb", "xgboost", "XGBoost", "machine_learning"),
        (r"lightgbm|\blgbm", "lightgbm", "LightGBM", "machine_learning"),
        (r"ridge|linearregression|linear_regression", "linear_models", "Linear / Ridge Models", "machine_learning"),
        (r"logistic|calibrated.?classifier", "calibrated_classifier", "Calibrated / Logistic Classifier", "machine_learning"),
        (r"ensemble|stack|meta_model|meta model|multihorizon", "ensemble_meta", "Ensemble / Meta Model", "ensemble_meta"),
        (r"two.pending.oco|oco.breakout", "two_pending_oco", "Two Pending OCO Breakout", "rule_strategy"),
        (r"movement.first|expected_mfe|tp_before_sl", "movement_path", "Movement / Path Model", "machine_learning"),
        (r"random.walk|no.?change", "random_walk", "Random Walk / No Change", "baseline"),
        (r"no.?trade", "no_trade", "No Trade", "baseline"),
        (r"drift", "drift", "Rolling Drift", "baseline"),
    ]
    for pattern, family_id, display_name, taxonomy in rules:
        if re.search(pattern, text):
            return family_id, display_name, taxonomy
    raw = descriptors[0] if descriptors else Path(rel).stem
    family_id = slug(raw)
    display = re.sub(r"[_-]+", " ", str(raw)).strip().title() or "Unclassified Model"
    strategy_hint = any(token in text for token in ("strategy", "breakout", "momentum", "reversion", "trend", "scalp", "rule"))
    return family_id, display[:120], "rule_strategy" if strategy_hint else "unclassified"


def extract_specification(obj: dict[str, Any], descriptors: list[str]) -> dict[str, Any]:
    spec: dict[str, Any] = {"descriptors": descriptors}
    for key, value in obj.items():
        lower = str(key).lower()
        if lower in SPEC_KEYS or lower in MODEL_KEYS or lower.endswith("_params") or lower.endswith("_parameters"):
            spec[str(key)] = short_value(value)
    nested = obj.get("spec")
    if isinstance(nested, dict):
        spec["spec"] = short_value(nested)
    return finite_json(spec)


def extract_ensemble_definition(obj: dict[str, Any], taxonomy: str) -> dict[str, Any]:
    member_value = None
    member_key = None
    for key in ("members", "member_ids", "base_models", "components"):
        if key in obj:
            member_key = key
            member_value = obj[key]
            break
    member_count = 0
    if isinstance(member_value, (list, tuple, dict)):
        member_count = len(member_value)
    elif member_value not in (None, ""):
        member_count = 1
    weights = obj.get("weights")
    return {
        "is_ensemble": taxonomy == "ensemble_meta" or member_value is not None,
        "member_field": member_key,
        "member_count": member_count,
        "members": short_value(member_value) if member_value is not None else None,
        "weights": short_value(weights) if weights is not None else None,
        "ensemble_mode": short_value(obj.get("ensemble_mode")) if "ensemble_mode" in obj else None,
        "min_members": short_value(obj.get("min_members")) if "min_members" in obj else None,
    }


def explicit_text(obj: dict[str, Any], keys: set[str]) -> str:
    values: list[str] = []
    for path, value in flatten_scalars(obj):
        key = path.split(".")[-1].lower()
        if key in keys and value is not None:
            values.append(str(value))
    return " ".join(values)[:8000]


def infer_validation_status(obj: dict[str, Any], rel: str, era: str, metrics: dict[str, Any]) -> tuple[str, list[str]]:
    lower_path = rel.lower()
    status_text = explicit_text(
        obj,
        {"status", "stage", "verdict", "result", "reason", "promotion_status", "validation_status"},
    ).lower()
    notes: list[str] = []
    if "smoke" in lower_path or re.search(r"\bsmoke(?:_only)?\b", status_text):
        return "SMOKE_ONLY", ["Smoke evidence is retained but excluded from primary performance claims."]
    if "superseded" in lower_path or "superseded" in status_text:
        return "SUPERSEDED", ["A later run or explicit marker supersedes this evidence."]
    if any(token in status_text for token in ("invalidated", "lookahead leakage", "accounting bug", "miscounted")):
        return "INVALIDATED", ["Evidence explicitly identifies invalid accounting, leakage, or invalidation."]
    legacy_returns = [
        abs(float(metric["value"]))
        for metric in metrics["trading"]
        if metric["metric"] == "return_pct" and isinstance(metric.get("value"), (int, float))
    ]
    if era.startswith("legacy") and legacy_returns and max(legacy_returns) >= 100_000:
        return "INVALIDATED", [
            "Legacy return exceeds 100,000%; it is quarantined as an accounting/counting failure until a corrected-engine rerun proves otherwise."
        ]
    if re.search(r"(?:^|\s|:)fail(?:ed)?(?:\s|$)", status_text):
        return "FAILED", ["The source evidence records a failed verdict."]
    if era.startswith("legacy"):
        return "UNVERIFIED", ["Legacy execution/accounting semantics have not been revalidated under the corrected engine."]
    if metrics["forecast"] or metrics["trading"]:
        notes.append("Fresh research evidence; not promoted to canonical without a complete untouched-holdout validation contract.")
        return "RESEARCH_LEAD", notes
    return "UNVERIFIED", ["No complete forecast and trading validation record was detected."]


def infer_run_kind(obj: dict[str, Any], metrics: dict[str, Any]) -> str:
    if metrics["forecast"] or metrics["trading"]:
        return "evaluation"
    if any(key in obj for key in IDENTITY_KEYS):
        return "candidate_or_experiment"
    return "model_definition"


def extract_declared_paths(obj: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for path, value in flatten_scalars(obj):
        key = path.split(".")[-1].lower()
        if not isinstance(value, str):
            continue
        if not (
            key.endswith("_path")
            or key.endswith("_file")
            or key in {"dataset", "model_artifact", "results_csv", "script", "source"}
        ):
            continue
        if any(separator in value for separator in ("/", "\\")) or Path(value).suffix:
            normalized = value.strip()
            if normalized and normalized not in values:
                values.append(normalized[:1000])
        if len(values) >= 64:
            break
    return values


def extract_rerun_command(obj: dict[str, Any]) -> str | None:
    for path, value in flatten_scalars(obj):
        key = path.split(".")[-1].lower()
        if key in {"command", "command_line", "cmd", "argv", "invocation"} and isinstance(value, str):
            return value[:4000]
    script = obj.get("script")
    if isinstance(script, str) and script.strip():
        return f"python {script.strip()}"
    return None


def extract_run(obj: dict[str, Any], rel: str, pointer: str, artifact_id: str) -> dict[str, Any] | None:
    if not is_model_candidate(obj, rel):
        return None
    descriptors = model_descriptor(obj, rel)
    family_id, display_name, taxonomy = canonical_family(descriptors, rel)
    if any(key in obj and obj[key] not in (None, [], {}, "") for key in ("members", "member_ids", "base_models", "components")):
        family_id, display_name, taxonomy = "ensemble_meta", "Ensemble / Meta Model", "ensemble_meta"
    metrics = extract_metrics(obj)
    specification = extract_specification(obj, descriptors)
    variant_id = stable_id("var", {"family_id": family_id, "specification": specification})
    assets = collect_values(obj, ASSET_KEYS)
    timeframes = collect_values(obj, TIMEFRAME_KEYS)
    labels = collect_values(obj, {"label", "label_family", "outcome", "target"})
    policies = collect_values(obj, {"entry_policy", "exit_policy", "policy"})
    ensemble = extract_ensemble_definition(obj, taxonomy)
    application_signature = {
        "variant_id": variant_id,
        "assets": assets,
        "timeframes_horizons": timeframes,
        "labels_targets": labels,
        "policies": policies,
        "ensemble": ensemble,
    }
    application_id = stable_id("app", application_signature)
    era = evidence_era(rel)
    status, validation_notes = infer_validation_status(obj, rel, era, metrics)
    identity_payload = {
        key: value
        for key, value in obj.items()
        if key != "reproducibility"
    }
    run_fingerprint = stable_id("run", finite_json(identity_payload), length=32)
    explicit_ids = {
        key: normalize_scalar(obj[key])
        for key in IDENTITY_KEYS
        if key in obj and isinstance(obj[key], (str, int, float))
    }
    return {
        "run_id": run_fingerprint,
        "record_type": infer_run_kind(obj, metrics),
        "domain": ASSET_DOMAIN,
        "asset_class": infer_asset_class(assets),
        "family_id": family_id,
        "family_display_name": display_name,
        "taxonomy": taxonomy,
        "variant_id": variant_id,
        "application_id": application_id,
        "explicit_ids": explicit_ids,
        "descriptors": descriptors,
        "assets": assets,
        "timeframes_horizons": timeframes,
        "labels_targets": labels,
        "policies": policies,
        "ensemble": ensemble,
        "date_coverage": extract_dates(obj),
        "metrics": metrics,
        "validation_status": status,
        "validation_notes": validation_notes,
        "evidence_era": era,
        "evidence": [{"artifact_id": artifact_id, "path": rel, "json_pointer": pointer}],
        "specification": specification,
        "reproducibility": {
            "exact_evidence_sha256": artifact_id.removeprefix("artifact_"),
            "source_path": rel,
            "json_pointer": pointer,
            "declared_paths": extract_declared_paths(obj),
            "resolved_artifact_ids": [],
            "serialized_model_loaded": False,
            "rerun_command": extract_rerun_command(obj),
            "rerun_status": "source_and_data_mapping_required" if not explicit_ids.get("run_id") else "run_identity_available",
            "grade": "D_unresolved",
        },
    }


def merge_run(existing: dict[str, Any], incoming: dict[str, Any]) -> None:
    evidence_markers = {
        (item["artifact_id"], item["path"], item["json_pointer"])
        for item in existing["evidence"]
    }
    for item in incoming["evidence"]:
        marker = (item["artifact_id"], item["path"], item["json_pointer"])
        if marker not in evidence_markers:
            existing["evidence"].append(item)
            evidence_markers.add(marker)
    existing_dates = existing.setdefault("date_coverage", {})
    for key, value in incoming.get("date_coverage", {}).items():
        existing_dates.setdefault(key, value)
    existing_reproduction = existing["reproducibility"]
    incoming_reproduction = incoming["reproducibility"]
    declared_paths = list(existing_reproduction.get("declared_paths", []))
    for declared in incoming_reproduction.get("declared_paths", []):
        if declared not in declared_paths:
            declared_paths.append(declared)
    existing_reproduction["declared_paths"] = declared_paths
    if incoming_reproduction.get("rerun_command") and not existing_reproduction.get("rerun_command"):
        for key in (
            "exact_evidence_sha256",
            "source_path",
            "json_pointer",
            "rerun_command",
        ):
            existing_reproduction[key] = incoming_reproduction.get(key)


def normalize_declared_path(value: str, project_root: Path) -> str:
    normalized = value.strip().replace("\\", "/")
    project_text = project_root.as_posix().rstrip("/")
    if normalized.lower().startswith(project_text.lower() + "/"):
        normalized = normalized[len(project_text) + 1 :]
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.lower()


def resolve_run_dependencies(
    runs: list[dict[str, Any]], artifacts: list[dict[str, Any]], project_root: Path
) -> None:
    path_lookup: dict[str, str] = {}
    basename_candidates: dict[str, set[str]] = defaultdict(set)
    for artifact in artifacts:
        for rel in artifact["paths"]:
            normalized = normalize_declared_path(rel, project_root)
            path_lookup[normalized] = artifact["artifact_id"]
            basename_candidates[Path(normalized).name].add(artifact["artifact_id"])
    for run in runs:
        resolved: set[str] = {item["artifact_id"] for item in run["evidence"]}
        unresolved: list[str] = []
        for declared in run["reproducibility"]["declared_paths"]:
            normalized = normalize_declared_path(declared, project_root)
            artifact_id = path_lookup.get(normalized)
            if artifact_id is None:
                basename_matches = basename_candidates.get(Path(normalized).name, set())
                if len(basename_matches) == 1:
                    artifact_id = next(iter(basename_matches))
            if artifact_id:
                resolved.add(artifact_id)
            else:
                unresolved.append(declared)
        run["reproducibility"]["resolved_artifact_ids"] = sorted(resolved)
        run["reproducibility"]["unresolved_declared_paths"] = unresolved
        has_command = bool(run["reproducibility"]["rerun_command"])
        has_dates = bool(run["date_coverage"])
        has_dependencies = len(resolved) > len(run["evidence"])
        if has_command and has_dates and has_dependencies and not unresolved:
            grade = "B_reconstructable_not_revalidated"
            rerun_status = "reconstructable_from_catalogued_evidence"
        elif has_dates and has_dependencies:
            grade = "C_partial_reconstruction"
            rerun_status = "command_or_dependency_mapping_required"
        else:
            grade = "D_unresolved"
            rerun_status = "source_split_or_data_mapping_required"
        run["reproducibility"]["grade"] = grade
        run["reproducibility"]["rerun_status"] = rerun_status


def source_snapshot_allowed(rel: str) -> bool:
    path = Path(rel)
    if path.suffix.lower() != ".py" or is_sensitive_path(path):
        return False
    if rel.startswith("fresh_m1_intrahour/"):
        return True
    name = path.name
    return bool(SAFE_SOURCE_HINT_RE.search(name) and not UNSAFE_SOURCE_RE.search(name))


def inspect_source(path: Path, rel: str, artifact_id: str) -> dict[str, Any]:
    detected_estimators: set[str] = set()
    functions: list[str] = []
    classes: list[str] = []
    parse_error: str | None = None
    try:
        tree = ast.parse(path.read_text(encoding="utf-8-sig", errors="replace"), filename=rel)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith("_"):
                functions.append(node.name)
            elif isinstance(node, ast.ClassDef):
                classes.append(node.name)
            elif isinstance(node, ast.Call):
                call_name = ""
                if isinstance(node.func, ast.Name):
                    call_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    call_name = node.func.attr
                if call_name in KNOWN_ESTIMATORS:
                    detected_estimators.add(call_name)
    except (SyntaxError, OSError, UnicodeError) as exc:
        parse_error = f"{type(exc).__name__}: {exc}"
    return {
        "source_module_id": stable_id("src", {"artifact_id": artifact_id, "path": rel}),
        "artifact_id": artifact_id,
        "path": rel,
        "detected_estimators": sorted(detected_estimators),
        "public_functions": sorted(set(functions))[:256],
        "classes": sorted(set(classes))[:128],
        "ast_parse_error": parse_error,
        "snapshot_path": f"snapshots/source/{rel}" if source_snapshot_allowed(rel) else None,
        "execution_source_excluded_from_snapshot": not source_snapshot_allowed(rel),
    }


def csv_date_coverage(path: Path) -> dict[str, Any]:
    coverage: dict[str, Any] = {"method": "not_inspected"}
    try:
        with path.open("rb") as handle:
            head = handle.read(256 * 1024)
            handle.seek(max(0, path.stat().st_size - 256 * 1024))
            tail = handle.read(256 * 1024)
        head_lines = head.decode("utf-8", errors="replace").splitlines()
        tail_lines = tail.decode("utf-8", errors="replace").splitlines()
        if not head_lines:
            return coverage
        header = next(csv.reader([head_lines[0]]), [])
        date_indices = [
            index
            for index, name in enumerate(header)
            if str(name).strip().lower() in {"time", "timestamp", "date", "datetime", "start_utc", "event_utc"}
        ]
        if not date_indices:
            coverage["method"] = "no_recognized_date_column"
            return coverage
        index = date_indices[0]
        first_value = None
        last_value = None
        for line in head_lines[1:]:
            row = next(csv.reader([line]), [])
            if len(row) > index and row[index].strip():
                first_value = row[index].strip()
                break
        for line in reversed(tail_lines):
            row = next(csv.reader([line]), [])
            if len(row) > index and row[index].strip() and row[index].strip() != header[index]:
                last_value = row[index].strip()
                break
        coverage = {
            "method": "first_last_row_assuming_time_sorted",
            "column": header[index],
            "start": first_value,
            "end": last_value,
        }
    except (OSError, csv.Error) as exc:
        coverage = {"method": "inspection_error", "error": f"{type(exc).__name__}: {exc}"}
    return coverage


def dataset_record(path: Path, rel: str, artifact_id: str, role: str) -> dict[str, Any] | None:
    if role not in {"raw_candle_dataset", "training_dataset", "feature_dataset"}:
        return None
    coverage: dict[str, Any] = {"method": "manifest_or_run_record_required"}
    if path.suffix.lower() == ".csv":
        coverage = csv_date_coverage(path)
    stem_parts = path.stem.upper().split("_")
    pair = "_".join(stem_parts[:2]) if len(stem_parts) >= 3 and len(stem_parts[0]) == 3 and len(stem_parts[1]) == 3 else None
    timeframe = stem_parts[2] if pair and len(stem_parts) >= 3 else None
    return {
        "dataset_id": stable_id("dataset", {"artifact_id": artifact_id, "role": role}),
        "artifact_id": artifact_id,
        "role": role,
        "path": rel,
        "format": path.suffix.lower().lstrip("."),
        "pair": pair,
        "timeframe": timeframe,
        "date_coverage": coverage,
        "copied_to_vault": False,
        "replication_contract": "Recreate or recover this exact file and verify SHA-256 before rerunning dependent models.",
    }


def package_inventory() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for dist in importlib.metadata.distributions():
        name = dist.metadata.get("Name")
        if name:
            rows.append({"name": name, "version": dist.version})
    return sorted(rows, key=lambda row: row["name"].lower())


def assert_research_safety(project_root: Path) -> dict[str, Any]:
    config_path = project_root / "fresh_m1_intrahour" / "config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    execution = config.get("execution", {})
    checks = {
        "live_execution_enabled": bool(execution.get("live_execution_enabled", False)),
        "demo_execution_enabled": bool(execution.get("demo_execution_enabled", False)),
        "oanda_execution_enabled": bool(execution.get("oanda_execution_enabled", False)),
    }
    enabled = [key for key, value in checks.items() if value]
    if enabled:
        raise RuntimeError(f"Refusing catalogue build because execution flags are enabled: {enabled}")
    return {
        **checks,
        "order_placement_used": False,
        "oanda_api_used": False,
        "serialized_models_loaded": False,
        "credentials_read": False,
        "credentials_modified": False,
        "account_files_modified": False,
        "raw_candles_copied": False,
    }


def build_paths(vault_root: Path, mode: str) -> BuildPaths:
    library_root = vault_root / "FOREX_MODEL_LIBRARY"
    timestamp = utc_now().strftime("%Y%m%d_%H%M%S")
    prefix = "forex_catalogue_smoke" if mode == "smoke" else "forex_catalogue"
    build_id = f"{prefix}_{timestamp}_v1"
    container = "smoke_builds" if mode == "smoke" else "builds"
    build_root = library_root / container / build_id
    if build_root.exists():
        raise FileExistsError(f"Immutable build already exists: {build_root}")
    return BuildPaths(library_root=library_root, build_root=build_root, build_id=build_id, mode=mode)


def copy_source_snapshot(source: Path, rel: str, build_root: Path) -> None:
    destination = build_root / "snapshots" / "source" / Path(rel)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def parse_json_evidence(
    source_path: Path,
    rel: str,
    artifact_id: str,
    snapshot_writer: EvidenceSnapshotWriter,
    counters: Counter[str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    evidence: dict[str, Any] = {
        "evidence_id": stable_id("evidence", {"artifact_id": artifact_id}),
        "artifact_id": artifact_id,
        "path": rel,
        "evidence_era": evidence_era(rel),
        "parse_status": "ok",
        "top_level_type": None,
        "top_level_keys": [],
        "snapshot_format": None,
        "sanitized_snapshot_path": None,
        "sanitized_snapshot_line": None,
        "sanitized_payload_sha256": None,
        "sanitized_snapshot_sha256": None,
        "model_record_count": 0,
    }
    try:
        raw_value = json.loads(source_path.read_text(encoding="utf-8-sig", errors="strict"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        evidence["parse_status"] = "error"
        evidence["parse_error"] = f"{type(exc).__name__}: {exc}"
        counters["json_parse_errors"] += 1
        return evidence, []
    sanitized = sanitize_json(raw_value, counters)
    evidence["top_level_type"] = type(sanitized).__name__
    if isinstance(sanitized, dict):
        evidence["top_level_keys"] = sorted(str(key) for key in sanitized)[:256]
    evidence.update(snapshot_writer.add(artifact_id, sanitized))
    runs: list[dict[str, Any]] = []
    for pointer, obj in iter_objects(sanitized):
        run = extract_run(obj, rel, pointer, artifact_id)
        if run is not None:
            runs.append(run)
    evidence["model_record_count"] = len(runs)
    return evidence, runs


def aggregate_artifact(
    artifacts: dict[str, dict[str, Any]],
    path: Path,
    rel: str,
    digest: str | None,
    role: str,
) -> tuple[str, dict[str, Any]]:
    artifact_id = f"artifact_{digest}" if digest else stable_id("artifact_skipped", {"path": rel})
    if artifact_id not in artifacts:
        artifacts[artifact_id] = {
            "artifact_id": artifact_id,
            "sha256": digest,
            "hash_status": "computed" if digest else "skipped_sensitive_path",
            "bytes": path.stat().st_size,
            "role": role,
            "suffix": path.suffix.lower(),
            "paths": [rel],
            "copied_to_vault": False,
        }
    else:
        record = artifacts[artifact_id]
        if rel not in record["paths"]:
            record["paths"].append(rel)
    return artifact_id, artifacts[artifact_id]


def summarize_metrics(runs: list[dict[str, Any]]) -> dict[str, Any]:
    values: dict[str, list[float]] = defaultdict(list)
    for run in runs:
        for group in ("forecast", "trading"):
            for metric in run["metrics"][group]:
                value = metric.get("value")
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    values[f"{group}.{metric['metric']}"].append(float(value))
    summary: dict[str, Any] = {}
    for name, series in sorted(values.items()):
        summary[name] = {
            "observations": len(series),
            "minimum": min(series),
            "median": median(series),
            "maximum": max(series),
            "comparability_warning": "Values may span different assets, horizons, splits, units, and cost contracts; use run-level records for comparison.",
        }
    return summary


def build_entities(runs: list[dict[str, Any]], source_modules: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    variants_by_id: dict[str, dict[str, Any]] = {}
    applications_by_id: dict[str, dict[str, Any]] = {}
    family_runs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        family_runs[run["family_id"]].append(run)
        variants_by_id.setdefault(
            run["variant_id"],
            {
                "variant_id": run["variant_id"],
                "family_id": run["family_id"],
                "specification": run["specification"],
                "specification_sha256": hashlib.sha256(canonical_json(run["specification"]).encode("utf-8")).hexdigest(),
                "run_ids": [],
            },
        )["run_ids"].append(run["run_id"])
        applications_by_id.setdefault(
            run["application_id"],
            {
                "application_id": run["application_id"],
                "variant_id": run["variant_id"],
                "asset_class": run["asset_class"],
                "assets": run["assets"],
                "timeframes_horizons": run["timeframes_horizons"],
                "labels_targets": run["labels_targets"],
                "policies": run["policies"],
                "ensemble": run["ensemble"],
                "run_ids": [],
            },
        )["run_ids"].append(run["run_id"])
    for record in variants_by_id.values():
        record["run_ids"] = sorted(set(record["run_ids"]))
    for record in applications_by_id.values():
        record["run_ids"] = sorted(set(record["run_ids"]))

    families: list[dict[str, Any]] = []
    status_rank = {
        "VALIDATED": 0,
        "RESEARCH_LEAD": 1,
        "FAILED": 2,
        "UNVERIFIED": 3,
        "SUPERSEDED": 4,
        "INVALIDATED": 5,
        "SMOKE_ONLY": 6,
    }
    for family_id, related in sorted(family_runs.items()):
        statuses = Counter(run["validation_status"] for run in related)
        metric_summary = summarize_metrics(related)
        canonical = next((run for run in related if run["validation_status"] == "VALIDATED"), None)
        reference = sorted(
            related,
            key=lambda run: (status_rank[run["validation_status"]], run["run_id"]),
        )[0]
        estimator_tokens: set[str] = set()
        source_ids: list[str] = []
        for module in source_modules:
            detected = module["detected_estimators"]
            module_family_ids = {
                canonical_family([descriptor], module["path"])[0]
                for descriptor in [*detected, Path(module["path"]).stem]
            }
            if family_id in module_family_ids or family_id in slug(Path(module["path"]).stem):
                source_ids.append(module["source_module_id"])
                estimator_tokens.update(detected)
        families.append(
            {
                "family_id": family_id,
                "display_name": reference["family_display_name"],
                "domain": ASSET_DOMAIN,
                "taxonomy": reference["taxonomy"],
                "summary": f"{reference['family_display_name']} evidence discovered in the forex project. Run-level records preserve pair, horizon, forecast, trading, gate, and validation context where the source reported it.",
                "variant_count": len({run["variant_id"] for run in related}),
                "application_count": len({run["application_id"] for run in related}),
                "run_count": len(related),
                "status_counts": dict(sorted(statuses.items())),
                "canonical_run_id": canonical["run_id"] if canonical else None,
                "canonical_status": "validated_source_of_truth" if canonical else "no_fully_validated_source_of_truth",
                "reference_run_id": reference["run_id"],
                "reference_run_is_canonical": canonical is not None and reference["run_id"] == canonical["run_id"],
                "assets": sorted({asset for run in related for asset in run["assets"]}),
                "timeframes_horizons": sorted({item for run in related for item in run["timeframes_horizons"]}),
                "ensemble_run_count": sum(1 for run in related if run["ensemble"]["is_ensemble"]),
                "maximum_ensemble_member_count": max(
                    (int(run["ensemble"]["member_count"]) for run in related),
                    default=0,
                ),
                "forecast_metric_summary": {
                    key.removeprefix("forecast."): value
                    for key, value in metric_summary.items()
                    if key.startswith("forecast.")
                },
                "trading_metric_summary": {
                    key.removeprefix("trading."): value
                    for key, value in metric_summary.items()
                    if key.startswith("trading.")
                },
                "detected_estimators": sorted(estimator_tokens),
                "source_module_ids": sorted(set(source_ids))[:64],
                "replication_contract": {
                    "variant_specification_required": True,
                    "source_hash_required": True,
                    "dataset_hash_required": True,
                    "date_split_required": True,
                    "cost_contract_required_for_trading_claims": True,
                    "untouched_holdout_required_for_canonical_status": True,
                },
            }
        )
    return (
        families,
        sorted(variants_by_id.values(), key=lambda row: row["variant_id"]),
        sorted(applications_by_id.values(), key=lambda row: row["application_id"]),
        make_validation_queue(runs),
    )


def make_validation_queue(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    queue: list[dict[str, Any]] = []
    for run in runs:
        status = run["validation_status"]
        if status == "VALIDATED" or status == "SMOKE_ONLY":
            continue
        requirements = [
            "Recover exact source/configuration and data SHA-256 dependencies.",
            "Rebuild labels with next-bar executable bid/ask timing and adverse-first same-bar ambiguity.",
            "Select model, threshold, policy, and allocator on train/validation only.",
            "Evaluate once on an untouched chronological holdout with realistic spread/slippage.",
            "Report forecast error separately from trading/gating performance.",
        ]
        if status == "INVALIDATED":
            requirements.insert(0, "Do not reuse reported performance; rerun from raw data after correcting the identified defect.")
        queue.append(
            {
                "queue_id": stable_id("queue", {"run_id": run["run_id"], "status": status}),
                "run_id": run["run_id"],
                "family_id": run["family_id"],
                "current_status": status,
                "priority": "high" if status in {"RESEARCH_LEAD", "INVALIDATED"} else "normal",
                "requirements": requirements,
            }
        )
    return sorted(queue, key=lambda row: (row["priority"] != "high", row["family_id"], row["run_id"]))


def write_csv_rows(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def build_sqlite(
    path: Path,
    families: list[dict[str, Any]],
    variants: list[dict[str, Any]],
    applications: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    artifacts: list[dict[str, Any]],
    datasets: list[dict[str, Any]],
    source_modules: list[dict[str, Any]],
    queue: list[dict[str, Any]],
    event_collections: list[dict[str, Any]],
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            PRAGMA journal_mode=DELETE;
            CREATE TABLE families (family_id TEXT PRIMARY KEY, display_name TEXT, taxonomy TEXT, canonical_run_id TEXT, record_json TEXT NOT NULL);
            CREATE TABLE variants (variant_id TEXT PRIMARY KEY, family_id TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE applications (application_id TEXT PRIMARY KEY, variant_id TEXT NOT NULL, asset_class TEXT, record_json TEXT NOT NULL);
            CREATE TABLE runs (run_id TEXT PRIMARY KEY, family_id TEXT NOT NULL, variant_id TEXT NOT NULL, application_id TEXT NOT NULL, validation_status TEXT NOT NULL, record_type TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE artifacts (artifact_id TEXT PRIMARY KEY, role TEXT NOT NULL, bytes INTEGER NOT NULL, sha256 TEXT, record_json TEXT NOT NULL);
            CREATE TABLE datasets (dataset_id TEXT PRIMARY KEY, artifact_id TEXT NOT NULL, role TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE source_modules (source_module_id TEXT PRIMARY KEY, artifact_id TEXT NOT NULL, path TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE validation_queue (queue_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, current_status TEXT NOT NULL, priority TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE event_collections (collection_id TEXT PRIMARY KEY, artifact_id TEXT NOT NULL, dataset_id TEXT NOT NULL, event_type TEXT NOT NULL, file_count INTEGER NOT NULL, record_json TEXT NOT NULL);
            CREATE INDEX idx_runs_family ON runs(family_id);
            CREATE INDEX idx_runs_variant ON runs(variant_id);
            CREATE INDEX idx_runs_status ON runs(validation_status);
            CREATE INDEX idx_artifacts_role ON artifacts(role);
            CREATE VIEW canonical_models AS SELECT family_id, display_name, canonical_run_id FROM families WHERE canonical_run_id IS NOT NULL;
            CREATE VIEW unresolved_validation AS SELECT queue_id, run_id, current_status, priority FROM validation_queue;
            """
        )
        connection.executemany(
            "INSERT INTO families VALUES (?,?,?,?,?)",
            [(r["family_id"], r["display_name"], r["taxonomy"], r["canonical_run_id"], canonical_json(r)) for r in families],
        )
        connection.executemany(
            "INSERT INTO variants VALUES (?,?,?)",
            [(r["variant_id"], r["family_id"], canonical_json(r)) for r in variants],
        )
        connection.executemany(
            "INSERT INTO applications VALUES (?,?,?,?)",
            [(r["application_id"], r["variant_id"], r["asset_class"], canonical_json(r)) for r in applications],
        )
        connection.executemany(
            "INSERT INTO runs VALUES (?,?,?,?,?,?,?)",
            [
                (
                    r["run_id"],
                    r["family_id"],
                    r["variant_id"],
                    r["application_id"],
                    r["validation_status"],
                    r["record_type"],
                    canonical_json(r),
                )
                for r in runs
            ],
        )
        connection.executemany(
            "INSERT INTO artifacts VALUES (?,?,?,?,?)",
            [(r["artifact_id"], r["role"], r["bytes"], r["sha256"], canonical_json(r)) for r in artifacts],
        )
        connection.executemany(
            "INSERT INTO datasets VALUES (?,?,?,?)",
            [(r["dataset_id"], r["artifact_id"], r["role"], canonical_json(r)) for r in datasets],
        )
        connection.executemany(
            "INSERT INTO source_modules VALUES (?,?,?,?)",
            [(r["source_module_id"], r["artifact_id"], r["path"], canonical_json(r)) for r in source_modules],
        )
        connection.executemany(
            "INSERT INTO validation_queue VALUES (?,?,?,?,?)",
            [(r["queue_id"], r["run_id"], r["current_status"], r["priority"], canonical_json(r)) for r in queue],
        )
        connection.executemany(
            "INSERT INTO event_collections VALUES (?,?,?,?,?,?)",
            [
                (
                    r["collection_id"],
                    r["artifact_id"],
                    r["dataset_id"],
                    r["event_type"],
                    r["file_count"],
                    canonical_json(r),
                )
                for r in event_collections
            ],
        )
        connection.commit()
    finally:
        connection.close()


def documentation_payloads(summary: dict[str, Any], families: list[dict[str, Any]]) -> dict[str, str]:
    status_lines = "\n".join(
        f"- `{key}`: {value}" for key, value in summary["validation_status_counts"].items()
    ) or "- No model runs detected."
    family_rows = []
    for family in sorted(families, key=lambda row: (-row["run_count"], row["display_name"]))[:250]:
        family_rows.append(
            f"| `{family['family_id']}` | {family['display_name']} | {family['taxonomy']} | {family['variant_count']} | {family['run_count']} | {family['canonical_status']} |"
        )
    family_table = "\n".join(family_rows)
    index = f"""# Forex Model Knowledge Library Build

- Build: `{summary['build_id']}`
- Mode: `{summary['mode']}`
- Created UTC: `{summary['created_at_utc']}`
- Model families: {summary['counts']['model_families']}
- Variants: {summary['counts']['variants']}
- Applications: {summary['counts']['applications']}
- Distinct run records: {summary['counts']['runs']}
- Unique artifacts: {summary['counts']['artifacts']}
- Original artifact bytes referenced: {summary['counts']['artifact_bytes']}
- Sanitized JSON evidence snapshots: {summary['counts']['json_evidence_snapshots']}
- Canonical validated model families: {summary['counts']['canonical_families']}
- Trade ready: `false`

## Start Here

- [Machine index](build_summary.json)
- [SQLite catalogue](catalog/catalog.sqlite)
- [Model family catalogue](catalog/model_families.json)
- [Variants](catalog/variants.jsonl)
- [Applications](catalog/applications.jsonl)
- [Runs](catalog/runs.jsonl)
- [Compact runs CSV](catalog/runs.csv)
- [Artifacts](catalog/artifacts.jsonl)
- [Datasets](catalog/datasets.jsonl)
- [Associated event collections](catalog/event_collections.jsonl)
- [Source modules](catalog/source_modules.jsonl)
- [Validation queue](catalog/validation_queue.jsonl)
- [Validation policy](VALIDATION_POLICY.md)
- [Replication guide](REPLICATION_GUIDE.md)
- [Safety and scope](SAFETY_AND_SCOPE.md)

## Evidence Status

{status_lines}

`UNVERIFIED` does not mean a model is bad. It means its reported performance has not been reproduced under the current corrected timing, pricing, leakage, cost, and holdout contracts. `FAILED` means a credible test found no promotable edge. `INVALIDATED` means the old reported performance itself must not be reused.

## Model Families

| ID | Family | Taxonomy | Variants | Runs | Source of truth |
|---|---|---:|---:|---:|---|
{family_table}
"""
    schema = """# Schema

The primary hierarchy is `model family -> variant -> application -> run`.

- A family groups a modeling method, such as SARIMA, Extra Trees, or an OCO strategy.
- A variant is the exact model/feature/label/horizon/policy specification recovered from evidence.
- An application binds a variant to pair or ensemble, timeframe/horizon, target, and policy.
- A run is immutable evaluation or candidate evidence and points to exact source artifacts by SHA-256.
- Artifacts are deduplicated by content hash; path aliases preserve duplicate locations without duplicating evidence.
- Forecast metrics and trading/gating metrics are separate. Values remain attached to their original nested metric path because units and splits vary across generations.

Statuses: `VALIDATED`, `RESEARCH_LEAD`, `FAILED`, `INVALIDATED`, `SUPERSEDED`, `UNVERIFIED`, and `SMOKE_ONLY`.
"""
    policy = """# Validation Policy

`VALIDATED` is reserved for a reproducible run with exact source/config/data identity, corrected next-bar executable bid/ask timing, adverse-first TP/SL ambiguity, realistic round-trip costs, train/validation-only selection, and one untouched chronological final evaluation. Forecast error must be reported separately from allocator and account results.

`RESEARCH_LEAD` is useful corrected-engine evidence that is not a canonical profitability claim. `FAILED` is a valid negative result. `INVALIDATED` records a known accounting, leakage, or counting defect. `UNVERIFIED` preserves legacy knowledge while preventing it from being mistaken for a current result. Smoke evidence never enters primary performance claims.

Old headline returns are not normalized or promoted merely because they are positive. A historical model must be rerun under the current contract before it can replace a canonical source of truth.
"""
    replication = """# Replication Guide

1. Find the family, variant, application, and run IDs in `catalog/catalog.sqlite` or the JSONL files.
2. Recover every evidence, source, model, and dataset artifact listed by the run and verify SHA-256.
3. Recreate the recorded Python environment from `environment/packages.json`; do not load serialized model files until provenance and compatibility are trusted.
4. Rebuild the recorded date splits and feature/label contracts. Missing split, data, or command metadata is a validation blocker, not permission to guess.
5. Rerun with execution disabled. Select all hyperparameters and gates on train/validation only, then evaluate one untouched chronological holdout.
6. Add the rerun as a new immutable record. Never overwrite or delete the historical run.

Large candle, feature, training, CSV, Parquet, archive, and serialized model artifacts are referenced rather than copied to keep the vault under its storage budget. High-volume runtime/migration JSON is grouped into event-dataset collections; model manifests embedded in those streams are still catalogued individually with exact hashes.
"""
    safety = """# Safety and Scope

This build is research infrastructure only. The builder performs local file reads, hashes, JSON sanitization, source AST parsing, and catalogue writes. It does not import project modules, connect to OANDA, start bots, place orders, mutate credentials or account files, or deserialize joblib/pickle artifacts.

The current fresh engine configuration was checked before the build. Live, demo, and OANDA execution remained disabled. Historical evidence may describe prior demo/live settings; that is archival metadata and does not enable execution.

Crypto research is excluded from this forex-project library and remains in its own vault library. Gold/XAU evidence discovered in the forex project is labeled `commodities_metals` at the application level.
"""
    return {
        "INDEX.md": index,
        "SCHEMA.md": schema,
        "VALIDATION_POLICY.md": policy,
        "REPLICATION_GUIDE.md": replication,
        "SAFETY_AND_SCOPE.md": safety,
    }


def write_family_docs(build_root: Path, families: list[dict[str, Any]], runs_by_id: dict[str, dict[str, Any]]) -> None:
    models_root = build_root / "models"
    models_root.mkdir(parents=True, exist_ok=True)
    for family in families:
        reference = runs_by_id[family["reference_run_id"]]
        metric_lines: list[str] = []
        for group in ("forecast", "trading"):
            for metric in reference["metrics"][group][:20]:
                metric_lines.append(f"- `{group}.{metric['metric']}` at `{metric['path']}`: `{metric['value']}`")
        if not metric_lines:
            metric_lines.append("- No normalized performance metric was recoverable from the reference evidence.")
        assets = ", ".join(family["assets"][:40]) or "unspecified"
        horizons = ", ".join(family["timeframes_horizons"][:40]) or "unspecified"
        text = f"""# {family['display_name']}

- Family ID: `{family['family_id']}`
- Taxonomy: `{family['taxonomy']}`
- Variants: {family['variant_count']}
- Applications: {family['application_count']}
- Runs: {family['run_count']}
- Ensemble-bearing runs: {family['ensemble_run_count']}
- Maximum recorded ensemble members: {family['maximum_ensemble_member_count']}
- Canonical run: `{family['canonical_run_id']}`
- Source-of-truth state: `{family['canonical_status']}`
- Reference run: `{family['reference_run_id']}`
- Assets/pairs: {assets}
- Timeframes/horizons: {horizons}

## Summary

{family['summary']}

## Reference Metrics

{chr(10).join(metric_lines)}

These values are evidence, not automatically comparable scores. Use the run record's original metric path, split, units, costs, pair, and horizon before drawing a conclusion.

## Replication

Resolve the reference run in `catalog/runs.jsonl`, then verify every evidence and dataset hash. The exact variant specification is in `catalog/variants.jsonl`. A missing canonical run means this family must be rerun under `VALIDATION_POLICY.md` before it can be treated as a source of truth.
"""
        write_text(models_root / f"{family['family_id']}.md", text)


def build_catalogue(project_root: Path, vault_root: Path, mode: str) -> BuildPaths:
    safety = assert_research_safety(project_root)
    paths = build_paths(vault_root, mode)
    paths.build_root.mkdir(parents=True, exist_ok=False)
    counters: Counter[str] = Counter()
    artifacts: dict[str, dict[str, Any]] = {}
    runs_by_id: dict[str, dict[str, Any]] = {}
    evidence_rows: list[dict[str, Any]] = []
    source_modules: list[dict[str, Any]] = []
    datasets_by_id: dict[str, dict[str, Any]] = {}
    parsed_json_artifacts: set[str] = set()
    bulk_collections: dict[str, dict[str, Any]] = {}
    snapshot_writer = EvidenceSnapshotWriter(paths.build_root, mode)
    bytes_hashed = 0
    files_seen = 0
    print(f"BUILD_START mode={mode} id={paths.build_id}", flush=True)
    for source_path in iter_project_files(project_root, mode):
        files_seen += 1
        rel = relative_posix(source_path, project_root)
        bulk_scope = bulk_event_scope(rel) if mode == "full" else None
        if bulk_scope and not bulk_model_evidence_hint(rel):
            add_bulk_event(bulk_collections, source_path, rel, bulk_scope)
            counters["bulk_event_files_grouped"] += 1
            counters["bulk_event_bytes_grouped"] += source_path.stat().st_size
            if files_seen % 25_000 == 0:
                print(
                    f"PROGRESS files={files_seen} grouped_events={counters['bulk_event_files_grouped']} unique_artifacts={len(artifacts)} runs={len(runs_by_id)}",
                    flush=True,
                )
            continue
        role = artifact_role(rel)
        sensitive = is_sensitive_path(Path(rel))
        digest = None if sensitive else file_sha256(source_path)
        if digest:
            bytes_hashed += source_path.stat().st_size
        artifact_id, artifact = aggregate_artifact(artifacts, source_path, rel, digest, role)
        if sensitive:
            counters["sensitive_artifacts_not_read"] += 1
        if role == "source_code" and digest:
            module = inspect_source(source_path, rel, artifact_id)
            source_modules.append(module)
            if module["snapshot_path"]:
                copy_source_snapshot(source_path, rel, paths.build_root)
                artifact["copied_to_vault"] = True
        dataset = dataset_record(source_path, rel, artifact_id, role) if digest else None
        if dataset:
            datasets_by_id.setdefault(dataset["dataset_id"], dataset)
        if role == "json_evidence" and digest and artifact_id not in parsed_json_artifacts:
            parsed_json_artifacts.add(artifact_id)
            evidence, extracted = parse_json_evidence(
                source_path,
                rel,
                artifact_id,
                snapshot_writer,
                counters,
            )
            evidence_rows.append(evidence)
            artifact["copied_to_vault"] = evidence["sanitized_snapshot_path"] is not None
            for run in extracted:
                if run["run_id"] in runs_by_id:
                    merge_run(runs_by_id[run["run_id"]], run)
                else:
                    runs_by_id[run["run_id"]] = run
        if files_seen % 500 == 0 or bytes_hashed // (5 * 1024**3) > counters["hash_progress_gb"]:
            counters["hash_progress_gb"] = bytes_hashed // (5 * 1024**3)
            print(
                f"PROGRESS files={files_seen} unique_artifacts={len(artifacts)} runs={len(runs_by_id)} hashed_gb={bytes_hashed / 1024**3:.2f}",
                flush=True,
            )

    shard_hashes = snapshot_writer.close()
    for evidence in evidence_rows:
        snapshot_path = evidence.get("sanitized_snapshot_path")
        if evidence.get("snapshot_format") == "gzip_jsonl_shard" and snapshot_path:
            evidence["sanitized_snapshot_sha256"] = shard_hashes[snapshot_path]
    bulk_artifacts, bulk_datasets, event_collection_rows = finalize_bulk_collections(bulk_collections)
    for artifact in bulk_artifacts:
        artifacts[artifact["artifact_id"]] = artifact
    for dataset in bulk_datasets:
        datasets_by_id[dataset["dataset_id"]] = dataset
    runs = sorted(runs_by_id.values(), key=lambda row: row["run_id"])
    artifacts_rows = sorted(artifacts.values(), key=lambda row: row["artifact_id"])
    datasets = sorted(datasets_by_id.values(), key=lambda row: row["dataset_id"])
    source_modules = sorted(source_modules, key=lambda row: row["source_module_id"])
    resolve_run_dependencies(runs, artifacts_rows, project_root)
    families, variants, applications, queue = build_entities(runs, source_modules)
    status_counts = Counter(run["validation_status"] for run in runs)
    role_counts = Counter(row["role"] for row in artifacts_rows)
    era_counts = Counter(run["evidence_era"] for run in runs)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "build_id": paths.build_id,
        "mode": mode,
        "created_at_utc": iso_now(),
        "project_root": project_root.as_posix(),
        "library_root": paths.library_root.as_posix(),
        "domain": ASSET_DOMAIN,
        "archive_purpose": "reproducible_forex_model_knowledge_and_research_evidence",
        "counts": {
            "files_seen": files_seen,
            "artifacts": len(artifacts_rows),
            "artifact_path_aliases": sum(int(row.get("path_count", len(row["paths"]))) for row in artifacts_rows),
            "artifact_bytes": sum(int(row["bytes"]) for row in artifacts_rows),
            "bytes_hashed": bytes_hashed,
            "json_evidence": len(evidence_rows),
            "json_evidence_snapshots": sum(1 for row in evidence_rows if row["sanitized_snapshot_path"]),
            "json_evidence_shards": len(shard_hashes),
            "event_log_collections": len(event_collection_rows),
            "event_log_files_grouped": sum(row["file_count"] for row in event_collection_rows),
            "source_modules": len(source_modules),
            "source_snapshots": sum(1 for row in source_modules if row["snapshot_path"]),
            "datasets": len(datasets),
            "model_families": len(families),
            "variants": len(variants),
            "applications": len(applications),
            "runs": len(runs),
            "canonical_families": sum(1 for row in families if row["canonical_run_id"]),
            "validation_queue": len(queue),
        },
        "validation_status_counts": dict(sorted(status_counts.items())),
        "artifact_role_counts": dict(sorted(role_counts.items())),
        "evidence_era_counts": dict(sorted(era_counts.items())),
        "sanitization_counts": dict(sorted(counters.items())),
        "safety": safety,
        "trade_ready": False,
        "profitability_claimed": False,
        "serialized_models_loaded": False,
        "raw_candles_copied": False,
        "large_artifact_policy": "hash_and_reference_only",
    }

    catalog_root = paths.build_root / "catalog"
    write_json(catalog_root / "model_families.json", families)
    write_jsonl(catalog_root / "variants.jsonl", variants)
    write_jsonl(catalog_root / "applications.jsonl", applications)
    write_jsonl(catalog_root / "runs.jsonl", runs)
    write_jsonl(catalog_root / "artifacts.jsonl", artifacts_rows)
    write_jsonl(catalog_root / "datasets.jsonl", datasets)
    write_jsonl(catalog_root / "source_modules.jsonl", source_modules)
    write_jsonl(catalog_root / "evidence.jsonl", sorted(evidence_rows, key=lambda row: row["evidence_id"]))
    write_jsonl(catalog_root / "event_collections.jsonl", event_collection_rows)
    write_jsonl(catalog_root / "validation_queue.jsonl", queue)
    write_csv_rows(
        catalog_root / "runs.csv",
        [
            {
                "run_id": row["run_id"],
                "family_id": row["family_id"],
                "variant_id": row["variant_id"],
                "application_id": row["application_id"],
                "record_type": row["record_type"],
                "validation_status": row["validation_status"],
                "asset_class": row["asset_class"],
                "assets": "|".join(row["assets"]),
                "timeframes_horizons": "|".join(row["timeframes_horizons"]),
                "evidence_count": len(row["evidence"]),
            }
            for row in runs
        ],
        [
            "run_id",
            "family_id",
            "variant_id",
            "application_id",
            "record_type",
            "validation_status",
            "asset_class",
            "assets",
            "timeframes_horizons",
            "evidence_count",
        ],
    )
    write_csv_rows(
        catalog_root / "validation_queue.csv",
        queue,
        ["queue_id", "run_id", "family_id", "current_status", "priority"],
    )
    build_sqlite(
        catalog_root / "catalog.sqlite",
        families,
        variants,
        applications,
        runs,
        artifacts_rows,
        datasets,
        source_modules,
        queue,
        event_collection_rows,
    )
    write_json(paths.build_root / "build_summary.json", summary)
    write_json(paths.build_root / "environment" / "packages.json", package_inventory())
    write_json(
        paths.build_root / "environment" / "python.json",
        {"version": sys.version, "executable": sys.executable, "platform": sys.platform},
    )
    for relative, content in documentation_payloads(summary, families).items():
        write_text(paths.build_root / relative, content)
    write_family_docs(paths.build_root, families, runs_by_id)
    write_checksums(paths.build_root)
    print(
        f"BUILD_COMPLETE mode={mode} path={paths.build_root} families={len(families)} runs={len(runs)} artifacts={len(artifacts_rows)}",
        flush=True,
    )
    return paths


def write_checksums(build_root: Path) -> None:
    checksum_root = build_root / "checksums"
    checksum_root.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for path in sorted(build_root.rglob("*")):
        if not path.is_file() or checksum_root in path.parents:
            continue
        lines.append(f"{file_sha256(path)}  {relative_posix(path, build_root)}")
    write_text(checksum_root / "vault_file_sha256.txt", "\n".join(lines))


def directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def validate_catalogue(paths: BuildPaths) -> dict[str, Any]:
    build_root = paths.build_root
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": finite_json(detail)})

    summary = json.loads((build_root / "build_summary.json").read_text(encoding="utf-8"))
    families = json.loads((build_root / "catalog" / "model_families.json").read_text(encoding="utf-8"))
    variants = read_jsonl(build_root / "catalog" / "variants.jsonl")
    applications = read_jsonl(build_root / "catalog" / "applications.jsonl")
    runs = read_jsonl(build_root / "catalog" / "runs.jsonl")
    artifacts = read_jsonl(build_root / "catalog" / "artifacts.jsonl")
    datasets = read_jsonl(build_root / "catalog" / "datasets.jsonl")
    sources = read_jsonl(build_root / "catalog" / "source_modules.jsonl")
    evidence = read_jsonl(build_root / "catalog" / "evidence.jsonl")
    queue = read_jsonl(build_root / "catalog" / "validation_queue.jsonl")
    event_collections = read_jsonl(build_root / "catalog" / "event_collections.jsonl")

    def unique(rows: list[dict[str, Any]], key: str) -> bool:
        values = [row[key] for row in rows]
        return len(values) == len(set(values))

    family_ids = {row["family_id"] for row in families}
    variant_ids = {row["variant_id"] for row in variants}
    application_ids = {row["application_id"] for row in applications}
    run_ids = {row["run_id"] for row in runs}
    artifact_ids = {row["artifact_id"] for row in artifacts}
    check("schema_version", summary.get("schema_version") == SCHEMA_VERSION, summary.get("schema_version"))
    check("family_ids_unique", unique(families, "family_id"), len(families))
    check("variant_ids_unique", unique(variants, "variant_id"), len(variants))
    check("application_ids_unique", unique(applications, "application_id"), len(applications))
    check("run_ids_unique", unique(runs, "run_id"), len(runs))
    check("artifact_ids_unique", unique(artifacts, "artifact_id"), len(artifacts))
    check("run_family_refs", all(row["family_id"] in family_ids for row in runs), len(runs))
    check("run_variant_refs", all(row["variant_id"] in variant_ids for row in runs), len(runs))
    check("run_application_refs", all(row["application_id"] in application_ids for row in runs), len(runs))
    check("variant_family_refs", all(row["family_id"] in family_ids for row in variants), len(variants))
    check("application_variant_refs", all(row["variant_id"] in variant_ids for row in applications), len(applications))
    check("evidence_artifact_refs", all(row["artifact_id"] in artifact_ids for row in evidence), len(evidence))
    check("dataset_artifact_refs", all(row["artifact_id"] in artifact_ids for row in datasets), len(datasets))
    check("source_artifact_refs", all(row["artifact_id"] in artifact_ids for row in sources), len(sources))
    check("queue_run_refs", all(row["run_id"] in run_ids for row in queue), len(queue))
    dataset_ids = {row["dataset_id"] for row in datasets}
    check(
        "event_collection_refs",
        all(row["artifact_id"] in artifact_ids and row["dataset_id"] in dataset_ids for row in event_collections),
        len(event_collections),
    )
    check("status_vocabulary", all(row["validation_status"] in STATUS_VALUES for row in runs), sorted({row["validation_status"] for row in runs}))
    check(
        "canonical_runs_validated",
        all(
            row["canonical_run_id"] is None
            or any(run["run_id"] == row["canonical_run_id"] and run["validation_status"] == "VALIDATED" for run in runs)
            for row in families
        ),
        summary["counts"]["canonical_families"],
    )
    bad_hashes = [row["artifact_id"] for row in artifacts if row["hash_status"] == "computed" and not re.fullmatch(r"[0-9a-f]{64}", row.get("sha256") or "")]
    check("artifact_hash_format", not bad_hashes, bad_hashes[:10])
    snapshot_errors: list[str] = []
    secret_snapshot_errors: list[str] = []
    snapshot_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evidence:
        if row.get("sanitized_snapshot_path"):
            snapshot_groups[row["sanitized_snapshot_path"]].append(row)
    for rel, group in snapshot_groups.items():
        snapshot = build_root / rel
        expected_hashes = {row.get("sanitized_snapshot_sha256") for row in group}
        if not snapshot.is_file() or len(expected_hashes) != 1 or file_sha256(snapshot) not in expected_hashes:
            snapshot_errors.append(rel)
            continue
        snapshot_format = group[0].get("snapshot_format")
        try:
            if snapshot_format == "individual_json":
                snapshot_value = json.loads(snapshot.read_text(encoding="utf-8"))
                payload_sha = hashlib.sha256(canonical_json(snapshot_value).encode("utf-8")).hexdigest()
                if payload_sha != group[0].get("sanitized_payload_sha256"):
                    snapshot_errors.append(f"{rel}: payload hash mismatch")
                leaked = find_unredacted_sensitive_values(snapshot_value)
                if leaked:
                    secret_snapshot_errors.append(f"{rel}: {leaked[:3]}")
            elif snapshot_format == "gzip_jsonl_shard":
                expected_by_line = {int(row["sanitized_snapshot_line"]): row for row in group}
                seen_lines: set[int] = set()
                with gzip.open(snapshot, "rt", encoding="utf-8") as handle:
                    for line_number, line in enumerate(handle, start=1):
                        if line_number not in expected_by_line:
                            continue
                        container = json.loads(line)
                        row = expected_by_line[line_number]
                        payload = container.get("payload")
                        payload_sha = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
                        if container.get("artifact_id") != row["artifact_id"] or payload_sha != row.get("sanitized_payload_sha256"):
                            snapshot_errors.append(f"{rel}:{line_number}: indexed payload mismatch")
                        leaked = find_unredacted_sensitive_values(payload)
                        if leaked:
                            secret_snapshot_errors.append(f"{rel}:{line_number}: {leaked[:3]}")
                        seen_lines.add(line_number)
                missing_lines = set(expected_by_line) - seen_lines
                if missing_lines:
                    snapshot_errors.append(f"{rel}: missing lines {sorted(missing_lines)[:10]}")
            else:
                snapshot_errors.append(f"{rel}: unknown snapshot format {snapshot_format}")
        except (OSError, UnicodeError, json.JSONDecodeError, EOFError) as exc:
            secret_snapshot_errors.append(f"{rel}: {type(exc).__name__}: {exc}")
    check("evidence_snapshot_integrity", not snapshot_errors, snapshot_errors[:10])
    check("sensitive_json_values_redacted", not secret_snapshot_errors, secret_snapshot_errors[:10])
    forbidden_copies = [
        relative_posix(path, build_root)
        for path in build_root.rglob("*")
        if path.is_file() and path.suffix.lower() in {".joblib", ".pkl", ".pickle", ".parquet", ".feather"}
    ]
    check("no_large_or_serialized_payload_copies", not forbidden_copies, forbidden_copies[:10])
    candle_copies = [
        relative_posix(path, build_root)
        for path in build_root.rglob("*.csv")
        if "snapshot" in relative_posix(path, build_root).lower()
    ]
    check("no_raw_candles_copied", not candle_copies and not summary["safety"]["raw_candles_copied"], candle_copies[:10])
    check("serialized_models_not_loaded", not summary["safety"]["serialized_models_loaded"], summary["safety"])
    check(
        "execution_disabled",
        not any(
            summary["safety"][key]
            for key in ("live_execution_enabled", "demo_execution_enabled", "oanda_execution_enabled", "order_placement_used", "oanda_api_used")
        ),
        summary["safety"],
    )
    check("credentials_untouched", not summary["safety"]["credentials_read"] and not summary["safety"]["credentials_modified"], summary["safety"])
    check("account_files_untouched", not summary["safety"]["account_files_modified"], summary["safety"])
    check("vault_size_below_4gb", directory_size(paths.library_root) < 4 * 1024**3, directory_size(paths.library_root))
    check("sqlite_exists", (build_root / "catalog" / "catalog.sqlite").is_file(), str(build_root / "catalog" / "catalog.sqlite"))
    with sqlite3.connect(build_root / "catalog" / "catalog.sqlite") as connection:
        sqlite_counts = {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("families", "variants", "applications", "runs", "artifacts", "datasets", "source_modules", "validation_queue", "event_collections")
        }
    check(
        "sqlite_counts_match",
        sqlite_counts
        == {
            "families": len(families),
            "variants": len(variants),
            "applications": len(applications),
            "runs": len(runs),
            "artifacts": len(artifacts),
            "datasets": len(datasets),
            "source_modules": len(sources),
            "validation_queue": len(queue),
            "event_collections": len(event_collections),
        },
        sqlite_counts,
    )
    minimums = {"smoke": {"families": 5, "runs": 20, "artifacts": 20, "json": 10}, "full": {"families": 20, "runs": 500, "artifacts": 1000, "json": 1000}}[paths.mode]
    scale_ok = len(families) >= minimums["families"] and len(runs) >= minimums["runs"] and len(artifacts) >= minimums["artifacts"] and len(evidence) >= minimums["json"]
    check("catalogue_scale_sanity", scale_ok, {"actual": {"families": len(families), "runs": len(runs), "artifacts": len(artifacts), "json": len(evidence)}, "minimum": minimums})
    passed = all(row["passed"] for row in checks)
    validation = {
        "schema_version": SCHEMA_VERSION,
        "build_id": paths.build_id,
        "mode": paths.mode,
        "validated_at_utc": iso_now(),
        "passed": passed,
        "checks_passed": sum(row["passed"] for row in checks),
        "checks_total": len(checks),
        "checks": checks,
    }
    write_json(build_root / "BUILD_VALIDATION.json", validation)
    lines = [
        f"# {'PASS' if passed else 'FAIL'} - Forex Catalogue Validation",
        "",
        f"- Build: `{paths.build_id}`",
        f"- Checks: {validation['checks_passed']}/{validation['checks_total']}",
        f"- Verdict: `{'PASS' if passed else 'FAIL'}`",
        "",
        "## Checks",
        "",
    ]
    lines.extend(f"- [{'x' if row['passed'] else ' '}] `{row['name']}`: `{row['detail']}`" for row in checks)
    write_text(build_root / "BUILD_VALIDATION.md", "\n".join(lines))
    print(f"VALIDATION_{'PASS' if passed else 'FAIL'} mode={paths.mode} checks={validation['checks_passed']}/{validation['checks_total']}", flush=True)
    return validation


def update_library_index(paths: BuildPaths, validation: dict[str, Any]) -> None:
    if not validation["passed"]:
        raise RuntimeError(f"Cannot publish failed build {paths.build_id}")
    library_root = paths.library_root
    library_root.mkdir(parents=True, exist_ok=True)
    if paths.mode == "smoke":
        write_text(library_root / "SMOKE_LATEST.txt", paths.build_id)
        write_json(
            library_root / "SMOKE_VALIDATION_LATEST.json",
            {"build_id": paths.build_id, "path": f"smoke_builds/{paths.build_id}", "validation": validation},
        )
        write_text(
            library_root / "SMOKE_VALIDATION_LATEST.md",
            f"# Smoke Catalogue PASS\n\n- Build: [`{paths.build_id}`](smoke_builds/{paths.build_id}/INDEX.md)\n- Checks: {validation['checks_passed']}/{validation['checks_total']}\n",
        )
        return
    summary = json.loads((paths.build_root / "build_summary.json").read_text(encoding="utf-8"))
    write_text(library_root / "LATEST_BUILD.txt", paths.build_id)
    index_payload = {
        "schema_version": SCHEMA_VERSION,
        "latest_build_id": paths.build_id,
        "latest_build_path": f"builds/{paths.build_id}",
        "build_summary": summary,
        "validation_passed": True,
        "trade_ready": False,
        "live_execution_enabled": False,
        "demo_execution_enabled": False,
        "oanda_execution_enabled": False,
    }
    write_json(library_root / "index.json", index_payload)
    write_text(
        library_root / "INDEX.md",
        f"""# Forex Model Knowledge Library

This library catalogues model families, exact variants, pair/ensemble applications, runs, failures, invalidated evidence, source modules, datasets, and reproducibility blockers from the forex project.

- Latest immutable build: [`{paths.build_id}`](builds/{paths.build_id}/INDEX.md)
- Model families: {summary['counts']['model_families']}
- Variants: {summary['counts']['variants']}
- Runs: {summary['counts']['runs']}
- Unique artifacts: {summary['counts']['artifacts']}
- Validation queue: {summary['counts']['validation_queue']}
- Canonical validated families: {summary['counts']['canonical_families']}
- Trade ready: `false`

## Start Here

- [Build index](builds/{paths.build_id}/INDEX.md)
- [Machine-readable root index](index.json)
- [SQLite catalogue](builds/{paths.build_id}/catalog/catalog.sqlite)
- [Complete runs](builds/{paths.build_id}/catalog/runs.jsonl)
- [Model families](builds/{paths.build_id}/catalog/model_families.json)
- [Validation queue](builds/{paths.build_id}/catalog/validation_queue.jsonl)
- [Latest smoke validation](SMOKE_VALIDATION_LATEST.md)
""",
    )
    validation_dir = library_root / "validations"
    validation_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(paths.build_root / "BUILD_VALIDATION.json", validation_dir / f"{paths.build_id}_VALIDATION.json")
    shutil.copy2(paths.build_root / "BUILD_VALIDATION.md", validation_dir / f"{paths.build_id}_VALIDATION.md")


def update_vault_root_index(vault_root: Path) -> None:
    libraries: list[dict[str, Any]] = []
    for name, domain in (("BIGTRIAD_MODEL_LIBRARY", "crypto"), ("FOREX_MODEL_LIBRARY", "forex_and_related_metals")):
        root = vault_root / name
        index_path = root / "index.json"
        if not index_path.is_file():
            continue
        payload = json.loads(index_path.read_text(encoding="utf-8"))
        libraries.append(
            {
                "library_id": name,
                "domain": domain,
                "path": name,
                "schema_version": payload.get("schema_version"),
                "latest_build_id": payload.get("latest_build_id"),
                "trade_ready": bool(payload.get("trade_ready", False)),
            }
        )
    write_json(
        vault_root / "index.json",
        {
            "schema_version": "model_knowledge_vault_v1",
            "updated_at_utc": iso_now(),
            "libraries": libraries,
            "storage_policy": "immutable_builds_with_large_artifacts_referenced_by_sha256",
        },
    )
    rows = "\n".join(
        f"| [{row['library_id']}]({row['path']}/INDEX.md) | {row['domain']} | `{row['latest_build_id']}` | `{str(row['trade_ready']).lower()}` |"
        for row in libraries
    )
    write_text(
        vault_root / "INDEX.md",
        f"""# Model Knowledge Vault

This is the top-level index for independent asset-domain model libraries. Each library uses immutable builds and keeps forecast evidence separate from trading, gating, and account results.

| Library | Domain | Latest build | Trade ready |
|---|---|---|---|
{rows}

Failed, invalidated, superseded, and unverified work remains searchable to prevent duplicate research. A positive historical result is not a canonical claim unless its library marks the exact run `VALIDATED`.
""",
    )


def paths_from_existing(build_root: Path) -> BuildPaths:
    summary = json.loads((build_root / "build_summary.json").read_text(encoding="utf-8"))
    return BuildPaths(
        library_root=build_root.parent.parent,
        build_root=build_root,
        build_id=summary["build_id"],
        mode=summary["mode"],
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    default_project = Path(__file__).resolve().parents[1]
    default_vault = Path.home() / "OneDrive" / "thevault"
    parser = argparse.ArgumentParser(description="Build and validate the immutable forex model knowledge catalogue.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("smoke", "full", "all"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--project-root", type=Path, default=default_project)
        subparser.add_argument("--vault-root", type=Path, default=default_vault)
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("build_root", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "validate":
        paths = paths_from_existing(args.build_root.resolve())
        validation = validate_catalogue(paths)
        return 0 if validation["passed"] else 1
    project_root = args.project_root.resolve()
    vault_root = args.vault_root.resolve()
    modes = ["smoke", "full"] if args.command == "all" else [args.command]
    for mode in modes:
        paths = build_catalogue(project_root, vault_root, mode)
        validation = validate_catalogue(paths)
        if not validation["passed"]:
            return 1
        update_library_index(paths, validation)
        if mode == "full":
            update_vault_root_index(vault_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
