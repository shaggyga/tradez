#!/usr/bin/env python3
"""Typed side-classification research adapter for a separately named cohort.

Reuses original feature-row and model inference plumbing. It emits source
side probabilities and labelled ranking scores, never midpoint probabilities
or entry promotion. No old worker, ledger or consensus caller is upgraded by
importing this module; a reviewed typed intake is required.
"""

from __future__ import annotations

import hashlib
import io
import importlib
import json
import math
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from oanda_intrahour_forecast_contract import (
        CONTRACT_VERSION as INTRAHOUR_CONTRACT_VERSION,
        DECISION_TIMEFRAME,
        FORECAST_CONTEXT_TIMEFRAMES,
        FORECAST_HORIZONS_SEC,
        is_forecast_cell,
    )
    from oanda_model_predictor_auto_promotion import (
        ACCOUNT_SCOPE,
        DEFAULT_STATE as DEFAULT_PROMOTION_STATE,
        cell_id as promotion_cell_id,
        verify_state_checksum,
    )
except ModuleNotFoundError:
    from trad.oanda_intrahour_forecast_contract import (
        CONTRACT_VERSION as INTRAHOUR_CONTRACT_VERSION,
        DECISION_TIMEFRAME,
        FORECAST_CONTEXT_TIMEFRAMES,
        FORECAST_HORIZONS_SEC,
        is_forecast_cell,
    )
    from trad.oanda_model_predictor_auto_promotion import (
        ACCOUNT_SCOPE,
        DEFAULT_STATE as DEFAULT_PROMOTION_STATE,
        cell_id as promotion_cell_id,
        verify_state_checksum,
    )



try:
    from oanda_signal_probability_semantics_v2 import (
        CONTRACT as TYPED_CONTRACT, COHORT as TYPED_COHORT,
        TARGETS as TYPED_TARGETS, positive_probability as typed_positive_probability,
        side_classifier_point, typed_probability_batch, original_generation_clock,
    )
except ModuleNotFoundError:
    from .oanda_signal_probability_semantics_v2 import (
        CONTRACT as TYPED_CONTRACT, COHORT as TYPED_COHORT,
        TARGETS as TYPED_TARGETS, positive_probability as typed_positive_probability,
        side_classifier_point, typed_probability_batch, original_generation_clock,
    )

ROOT = Path(__file__).resolve().parent
DEFAULT_MODEL_ROOT = (
    ROOT / "data" / "oanda_training_manager" / "models" / "modern_model_gap"
)
DEFAULT_REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "modern_model_gap"
    / "shared_panel_model_benchmark_latest.json"
)
DEFAULT_MODELS = (
    "logistic_baseline",
    "hist_gradient_boosting",
    "catboost",
    "ngboost",
)
TIMEFRAME_SECONDS = {
    "S5": 5,
    "S10": 10,
    "S15": 15,
    "S30": 30,
    "M1": 60,
    "M2": 120,
    "M3": 180,
    "M4": 240,
    "M5": 300,
    "M6": 360,
    "M7": 420,
    "M8": 480,
    "M9": 540,
    "M10": 600,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H2": 7200,
    "H3": 10800,
    "H4": 14400,
    "H6": 21600,
    "H8": 28800,
    "H12": 43200,
    "D1": 86400,
}


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_timestamp(value: Any) -> datetime:
    text = str(value or "").strip()
    if text:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def relative_up_probability(long_probability: float, short_probability: float) -> float:
    """Legacy directional-probability API is unsupported in this typed cohort."""
    raise ValueError("side probabilities do not supply calibrated midpoint P(up); use the labelled side score")


@dataclass
class ArtifactRuntime:
    model_id: str
    path: Path
    sha256: str
    artifact: dict[str, Any]
    cells: tuple[tuple[str, int], ...]
    cell_metrics: dict[tuple[str, int], dict[str, Any]]


class PredictorPromotionPolicy:
    """Fail-closed reader for exact practice_007 predictor eligibility."""

    def __init__(self, path: Path | None, max_age_sec: float = 1800.0) -> None:
        self.path = None if path is None else Path(path)
        self.max_age_sec = max(1.0, float(max_age_sec))
        self.state: dict[str, Any] = {}
        self.cells: dict[str, dict[str, Any]] = {}
        self.status = "disabled" if self.path is None else "not_loaded"
        self.reload()

    def reload(self) -> None:
        self.state = {}
        self.cells = {}
        if self.path is None:
            self.status = "disabled"
            return
        payload = _read_json(self.path)
        if not payload:
            self.status = "missing"
            return
        if not verify_state_checksum(payload):
            self.status = "checksum_mismatch"
            return
        if payload.get("account_scope") != ACCOUNT_SCOPE:
            self.status = "wrong_account_scope"
            return
        if bool(payload.get("real_account_authorized")):
            self.status = "real_account_policy_rejected"
            return
        if payload.get("status") != "active":
            self.status = "inactive"
            return
        generated = _finite(payload.get("generated_epoch"))
        expires = min(
            self.max_age_sec,
            max(1.0, _finite(payload.get("expires_after_sec"), self.max_age_sec)),
        )
        if generated <= 0.0 or time.time() - generated > expires:
            self.status = "stale"
            return
        self.state = payload
        self.cells = {
            str(row.get("cell_id") or ""): row
            for row in payload.get("eligible_cells") or []
            if isinstance(row, dict) and row.get("cell_id") and row.get("eligible")
        }
        self.status = "active"

    def _fresh(self) -> bool:
        if self.status != "active":
            return False
        generated = _finite(self.state.get("generated_epoch"))
        expires = min(
            self.max_age_sec,
            max(1.0, _finite(self.state.get("expires_after_sec"), self.max_age_sec)),
        )
        if generated <= 0.0 or time.time() - generated > expires:
            self.status = "stale"
            self.cells = {}
            return False
        return True

    def evidence(
        self,
        model_id: str,
        artifact_sha256: str,
        instrument: str,
        input_timeframe: str,
        horizon_sec: int,
    ) -> dict[str, Any]:
        if not self._fresh():
            return {}
        identity = promotion_cell_id(
            model_id,
            artifact_sha256,
            instrument,
            input_timeframe,
            horizon_sec,
        )
        row = self.cells.get(identity) or {}
        if str(row.get("artifact_sha256") or "").lower() != artifact_sha256.lower():
            return {}
        return row

    def promoted_models(self, runtimes: dict[str, ArtifactRuntime]) -> set[str]:
        if not self._fresh():
            return set()
        hashes = {model_id: runtime.sha256.lower() for model_id, runtime in runtimes.items()}
        return {
            str(row.get("model_id") or "")
            for row in self.cells.values()
            if hashes.get(str(row.get("model_id") or ""))
            == str(row.get("artifact_sha256") or "").lower()
        }

    def summary(self, runtimes: dict[str, ArtifactRuntime]) -> dict[str, Any]:
        return {
            "path": "" if self.path is None else str(self.path.resolve()),
            "status": self.status,
            "account_scope": self.state.get("account_scope"),
            "eligible_cell_count": len(self.cells) if self._fresh() else 0,
            "promoted_models": sorted(self.promoted_models(runtimes)),
            "real_account_authorized": False,
        }


class ModelGapArtifactProducerV2:
    """Batch live inference for model-gap artifacts with strict provenance."""

    def __init__(
        self,
        model_root: Path = DEFAULT_MODEL_ROOT,
        report_path: Path = DEFAULT_REPORT,
        models: Iterable[str] = DEFAULT_MODELS,
        promotion_state_path: Path | None = None,
        promotion_max_age_sec: float = 1800.0,
    ) -> None:
        if promotion_state_path is not None:
            raise ValueError('typed side cohort refuses legacy promotion state')
        self.model_root = Path(model_root)
        self.report_path = Path(report_path)
        self.requested_models = tuple(dict.fromkeys(str(value) for value in models))
        self.runtimes: dict[str, ArtifactRuntime] = {}
        self.load_errors: dict[str, str] = {}
        self.last_generated_epoch = 0.0
        self.last_summary: dict[str, Any] = {}
        self.promotion = PredictorPromotionPolicy(
            promotion_state_path,
            promotion_max_age_sec,
        )
        self.reload()

    @staticmethod
    def _dependencies() -> tuple[Any, Any, Any]:
        trad_path = str(ROOT)
        if trad_path not in sys.path:
            sys.path.insert(0, trad_path)
        try:
            importlib.import_module("oanda_event_meta_model_pipeline")
        except ModuleNotFoundError:
            module = importlib.import_module("trad.oanda_event_meta_model_pipeline")
            sys.modules.setdefault("oanda_event_meta_model_pipeline", module)
        import joblib
        import numpy as np
        import pandas as pd

        return joblib, np, pd

    def reload(self) -> None:
        self.runtimes.clear()
        self.load_errors.clear()
        self.promotion.reload()
        try:
            with self.report_path.open('rb') as report_handle:
                report_bytes = report_handle.read(16 * 1024 * 1024 + 1)
            if len(report_bytes) > 16 * 1024 * 1024:
                raise ValueError('source report exceeds16MiB')
            report = json.loads(report_bytes)
            if not isinstance(report, dict):
                raise ValueError('source report object required')
            self.source_report_sha256 = hashlib.sha256(report_bytes).hexdigest()
        except (OSError, ValueError):
            self.source_report_sha256 = ''
            return
        result_rows = {
            str(row.get("model") or ""): row
            for row in report.get("results") or []
            if isinstance(row, dict)
        }
        artifact_rows = {
            str(row.get("model") or ""): row
            for row in report.get("artifacts") or []
            if isinstance(row, dict)
        }
        try:
            joblib, _, _ = self._dependencies()
        except Exception as exc:
            message = f"dependency_error:{type(exc).__name__}:{exc}"
            self.load_errors.update({model_id: message for model_id in self.requested_models})
            return

        for model_id in self.requested_models:
            path = self.model_root / f"{model_id}_shared_panel_latest.joblib"
            result = result_rows.get(model_id) or {}
            record = artifact_rows.get(model_id) or {}
            expected_sha = str(record.get("sha256") or "")
            try:
                if not path.is_file():
                    raise FileNotFoundError(path)
                with path.open('rb') as artifact_handle:
                    artifact_bytes = artifact_handle.read(64 * 1024 * 1024 + 1)
                if len(artifact_bytes) > 64 * 1024 * 1024:
                    raise ValueError('source artifact exceeds64MiB serialized-byte bound')
                observed_sha = hashlib.sha256(artifact_bytes).hexdigest()
                if not expected_sha:
                    raise RuntimeError("validation report has no artifact hash")
                if observed_sha.lower() != expected_sha.lower():
                    raise RuntimeError("artifact hash does not match validation report")
                artifact = joblib.load(io.BytesIO(artifact_bytes))
                if not isinstance(artifact, dict) or artifact.get("estimator") is None:
                    raise RuntimeError("artifact has no fitted estimator")
                if str(artifact.get("execution_policy") or "") != "shadow_only":
                    raise RuntimeError("artifact execution policy is not shadow_only")
                metrics = {}
                for row in (result.get("holdout") or {}).get("all_prediction_cell_metrics", []):
                    if not isinstance(row, dict):
                        raise ValueError('source validation cell object required')
                    horizon_sec = row.get('horizon_sec')
                    if type(horizon_sec) is not int or not 0 < horizon_sec <= 7 * 86400:
                        raise ValueError('source validation cell requires exact integer horizon_sec')
                    timeframe = str(row.get('input_timeframe') or '').upper()
                    if timeframe in TIMEFRAME_SECONDS and is_forecast_cell(timeframe, horizon_sec):
                        cell = (timeframe, horizon_sec)
                        if cell in metrics:
                            raise ValueError('duplicate source validation cell')
                        metrics[cell] = row
                if not metrics:
                    raise RuntimeError("validation report has no supported live cells")
                self.runtimes[model_id] = ArtifactRuntime(
                    model_id=model_id,
                    path=path,
                    sha256=observed_sha,
                    artifact=artifact,
                    cells=tuple(sorted(metrics, key=lambda cell: (TIMEFRAME_SECONDS[cell[0]], cell[1]))),
                    cell_metrics=metrics,
                )
            except Exception as exc:
                self.load_errors[model_id] = f"{type(exc).__name__}: {exc}"

    def reload_promotion(self) -> None:
        self.promotion.reload()

    @staticmethod
    def _row(
        runtime: ArtifactRuntime,
        instrument: str,
        features: dict[str, Any],
        quote: dict[str, Any],
        timeframe: str,
        horizon_sec: int,
        direction: str,
        generated: datetime,
    ) -> dict[str, Any]:
        pip = max(1e-12, _finite(features.get("pip"), 0.01 if instrument.endswith("_JPY") else 0.0001))
        bid = _finite(quote.get("bid"))
        ask = _finite(quote.get("ask"))
        spread = max(0.0, (ask - bid) / pip)
        seconds = TIMEFRAME_SECONDS[timeframe]
        hour = generated.hour + generated.minute / 60.0 + generated.second / 3600.0
        weekday = generated.weekday()
        derived = {
            "input_timeframe_seconds": seconds,
            "horizon_sec": horizon_sec,
            "side_sign": 1.0 if direction == "LONG" else -1.0,
            "entry_spread_pips": spread,
            "hour_sin": math.sin(2.0 * math.pi * hour / 24.0),
            "hour_cos": math.cos(2.0 * math.pi * hour / 24.0),
            "weekday_sin": math.sin(2.0 * math.pi * weekday / 7.0),
            "weekday_cos": math.cos(2.0 * math.pi * weekday / 7.0),
            "instrument": instrument,
            "input_timeframe": timeframe,
            "direction": direction,
            "source": "live_feature_snapshot",
            "spread_mode": "observed_oanda_stream_depth",
        }
        row: dict[str, Any] = {}
        for name in runtime.artifact.get("numeric_features") or []:
            value = derived.get(name, features.get(name))
            row[str(name)] = float("nan") if value is None else _finite(value, float("nan"))
        for name in runtime.artifact.get("categorical_features") or []:
            row[str(name)] = str(derived.get(name, features.get(name, "unknown")))
        return row

    @staticmethod
    def _probabilities(runtime: ArtifactRuntime, frame: Any, np: Any) -> Any:
        artifact = runtime.artifact
        names = [
            *(str(value) for value in artifact.get("numeric_features") or []),
            *(str(value) for value in artifact.get("categorical_features") or []),
        ]
        if not isinstance(artifact.get("target"),str) or artifact.get("target") not in TYPED_TARGETS:
            raise ValueError("unsupported artifact target")
        if artifact.get("schema_version") != 1 or type(artifact.get("schema_version")) is not int:
            raise ValueError("unsupported source artifact schema")
        if artifact.get("model") != runtime.model_id:
            raise ValueError("source artifact model identity mismatch")
        return typed_probability_batch(artifact["estimator"], artifact.get("calibrator"), frame[names])

    def forecast(self, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        generated_epoch, generated = original_generation_clock(snapshot)
        instruments = snapshot.get("instruments") or {}
        if not isinstance(instruments, dict):
            instruments = {}
        if generated_epoch <= self.last_generated_epoch:
            self.last_summary = {
                "status": "duplicate_snapshot",
                "generated_epoch": generated_epoch,
                "forecasts": 0,
            }
            return []
        feature_origins = {}
        for instrument, raw in instruments.items():
            if not isinstance(raw, dict):
                continue
            timeframe_features = raw.get('timeframe_features') or {}
            if not isinstance(timeframe_features, dict):
                continue
            for runtime in self.runtimes.values():
                for timeframe, _ in runtime.cells:
                    features = timeframe_features.get(timeframe) or {}
                    if not isinstance(features, dict) or not features:
                        continue
                    text = features.get('candle_time')
                    if not isinstance(text, str) or not text.strip():
                        raise ValueError('explicit source candle_time required')
                    try:
                        clock = datetime.fromisoformat(text.replace('Z', '+00:00'))
                    except ValueError:
                        raise ValueError('invalid source candle_time') from None
                    if clock.tzinfo is None or clock.utcoffset() is None:
                        raise ValueError('aware source candle_time required')
                    clock = clock.astimezone(timezone.utc)
                    if clock.timestamp() <= 0 or clock.timestamp() > generated_epoch:
                        raise ValueError('source candle_time outside original generation order')
                    feature_origins[(instrument, timeframe)] = clock.isoformat()
        _, np, pd = self._dependencies()
        forecasts: list[dict[str, Any]] = []
        model_counts: dict[str, int] = {}
        depth_ready = 0
        missing_timeframe_feature_cells = 0
        for model_id, runtime in self.runtimes.items():
            rows: list[dict[str, Any]] = []
            keys: list[tuple[str, str, int, str]] = []
            for instrument, raw in sorted(instruments.items()):
                if not isinstance(raw, dict):
                    continue
                timeframe_features = raw.get("timeframe_features") or {}
                quote = raw.get("quote") or {}
                if not isinstance(timeframe_features, dict) or not isinstance(quote, dict):
                    continue
                bid = _finite(quote.get("bid"))
                ask = _finite(quote.get("ask"))
                if bid <= 0.0 or ask < bid:
                    continue
                for timeframe, horizon_sec in runtime.cells:
                    features = timeframe_features.get(timeframe) or {}
                    if not isinstance(features, dict) or not features:
                        missing_timeframe_feature_cells += 1
                        continue
                    if abs(_finite(features.get("depth_imbalance"))) > 0.0:
                        depth_ready += 1
                    for direction in ("LONG", "SHORT"):
                        rows.append(
                            self._row(
                                runtime,
                                str(instrument),
                                features,
                                quote,
                                timeframe,
                                horizon_sec,
                                direction,
                                generated,
                            )
                        )
                        keys.append((str(instrument), timeframe, horizon_sec, direction))
            if not rows:
                model_counts[model_id] = 0
                continue
            probabilities, class_mapping = self._probabilities(runtime, pd.DataFrame(rows), np)
            sides: dict[tuple[str, str, int], dict[str, float]] = {}
            for key, probability in zip(keys, probabilities):
                instrument, timeframe, horizon_sec, direction = key
                sides.setdefault((instrument, timeframe, horizon_sec), {})[direction] = float(probability)
            grouped: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
            for (instrument, timeframe, horizon_sec), side_values in sides.items():
                if set(side_values) != {"LONG", "SHORT"}:
                    continue
                point = side_classifier_point(
                    horizon_sec=horizon_sec,
                    target=runtime.artifact.get("target"),
                    long_probability=side_values["LONG"],
                    short_probability=side_values["SHORT"],
                    metrics=runtime.cell_metrics[(timeframe, horizon_sec)],
                    artifact_sha256=runtime.sha256,
                    report_sha256=self.source_report_sha256,
                    calibrator_applied=runtime.artifact.get("calibrator") is not None,
                    class_mapping=class_mapping,
                )
                grouped.setdefault((instrument, timeframe), {})[
                    str(horizon_sec)
                ] = point
            emitted = 0
            for (instrument, timeframe), curve in grouped.items():
                source = instruments[instrument]
                quote = source.get("quote") or {}
                features = (
                    (source.get("timeframe_features") or {}).get(timeframe) or {}
                )
                feature_origin = feature_origins[(instrument, timeframe)]
                decision_identity = json.dumps(
                    {
                        "model_id": model_id,
                        "artifact_sha256": runtime.sha256,
                        "instrument": instrument,
                        "input_timeframe": timeframe,
                        "feature_origin": feature_origin,
                        "signal_semantics_contract": TYPED_CONTRACT,
                        "cohort_id": TYPED_COHORT,
                        "source_target": runtime.artifact.get("target"),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
                forecasts.append(
                    {
                        "id": (
                            "model-gap-side-v2-"
                            + hashlib.sha256(
                                decision_identity.encode("utf-8")
                            ).hexdigest()[:32]
                        ),
                        "model_id": model_id,
                        "family": "modern_tabular_probabilistic",
                        "profile": "typed_side_classification_v2",
                        "signal_semantics_contract": TYPED_CONTRACT,
                        "cohort_id": TYPED_COHORT,
                        "research_only": True,
                        "signal_role": "structural",
                        "instrument": instrument,
                        "input_timeframe": timeframe,
                        "training_timeframe": timeframe,
                        "generated_epoch": generated_epoch,
                        "generated_utc": generated.isoformat(),
                        "bid": quote.get("bid"),
                        "ask": quote.get("ask"),
                        "pip": features.get("pip"),
                        "account_eligible": any(
                            bool(point.get("account_eligible")) for point in curve.values()
                        ),
                        "forecast_curve": curve,
                        "producer_metadata": {
                            "artifact": runtime.path.name,
                            "artifact_sha256": runtime.sha256,
                            "artifact_trained_utc": runtime.artifact.get("trained_utc"),
                            "artifact_execution_policy": runtime.artifact.get("execution_policy"),
                            "classification_only": True,
                            "source_target": runtime.artifact.get("target"),
                            "source_report_sha256": self.source_report_sha256,
                            "signed_midpoint_probability_supplied": False,
                            "legacy_class_mapping_historically_verified": False,
                            "historical_values_used_as_live_predictions": False,
                            "live_paper_promotion_used_as_execution_calibration": any(
                                bool(point.get("account_eligible"))
                                for point in curve.values()
                            ),
                            "input_feature_contract": "inherited_feature_row_training_live_parity_unverified",
                            "input_feature_training_live_parity_verified": False,
                            "intrahour_contract_version": INTRAHOUR_CONTRACT_VERSION,
                            "decision_timeframe": DECISION_TIMEFRAME,
                            "context_timeframes": list(FORECAST_CONTEXT_TIMEFRAMES),
                            "forecast_horizons_sec": list(FORECAST_HORIZONS_SEC),
                            "execution_microstructure_used_by_structural_model": None,
                            "structural_microstructure_use_status": "unknown_unverified_artifact_feature_selection",
                            "candle_volume_semantics": (
                                "oanda_price_update_count_not_exchange_volume"
                            ),
                            "input_feature_origin_utc": feature_origin,
                            "source_candle_time_supplied": features.get("candle_time"),
                            "input_feature_origin_status": "source_reported_aware_clock_not_future",
                            "input_feature_completion_verified": False,
                            "input_feature_original_availability_verified": False,
                            "decision_identity": (
                                "artifact_pair_timeframe_source_reported_candle_clock"
                            ),
                            "depth_features_available_in_snapshot": bool(
                                source.get("microstructure")
                            ),
                        },
                    }
                )
                emitted += 1
            model_counts[model_id] = emitted
        self.last_generated_epoch = generated_epoch
        self.last_summary = {
            "status": "forecasted",
            "generated_epoch": generated_epoch,
            "loaded_models": sorted(self.runtimes),
            "load_errors": dict(self.load_errors),
            "snapshot_instruments": len(instruments),
            "depth_ready_rows": depth_ready,
            "missing_timeframe_feature_cells": missing_timeframe_feature_cells,
            "forecasts": len(forecasts),
            "model_forecasts": model_counts,
            "supported_cells": {
                model_id: [f"{timeframe}:{horizon}" for timeframe, horizon in runtime.cells]
                for model_id, runtime in self.runtimes.items()
            },
            "promotion": self.promotion.summary(self.runtimes),
        }
        return forecasts

    def summary(self) -> dict[str, Any]:
        return {
            "requested_models": list(self.requested_models),
            "loaded_models": sorted(self.runtimes),
            "load_errors": dict(self.load_errors),
            "promotion": self.promotion.summary(self.runtimes),
            **self.last_summary,
        }
