#!/usr/bin/env python3
"""Canonical, audited signal-contribution feed for the FX signal engine.

The feed intentionally separates model implementation from model execution.
Any producer can publish a fresh, finite forecast into the research consensus,
but account eligibility is constrained by the registered production policy.
Historical benchmark rows are never converted into live forecasts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


INSTRUMENT_PATTERN = re.compile(r"^[A-Z]{3}_[A-Z]{3}$")
TIMEFRAME_PATTERN = re.compile(r"^(?:S\d+|M\d+|H\d+|D\d+|MULTI)$")
MAX_HORIZON_SEC = 7 * 86400
DEFAULT_RECENT_LIMIT = 50_000
# The consolidated feed writes several thousand compact forecast rows per
# strategy cycle. Synchronous auto-checkpoints forced the committing producer
# to copy the WAL on the latency-sensitive path. A dedicated supervised PASSIVE
# checkpointer owns that I/O without blocking writers.
WAL_AUTOCHECKPOINT_FRAMES = 0


class ContributionError(ValueError):
    """Raised when a model output cannot be represented as a live signal."""


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _epoch(value: Any) -> float | None:
    number = _finite(value)
    if number is not None:
        return number
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _compact_transport_payload(
    candidate: dict[str, Any],
    source: str,
    published_epoch: float,
) -> dict[str, Any]:
    """Remove curve fields that the consumer deterministically reconstructs."""

    payload = {
        **candidate,
        "feed_source": source,
        "feed_published_epoch": published_epoch,
    }
    curve = payload.get("forecast_curve")
    if not isinstance(curve, dict):
        return payload
    compact_curve: dict[Any, Any] = {}
    for raw_horizon, raw_point in curve.items():
        if not isinstance(raw_point, dict):
            compact_curve[raw_horizon] = raw_point
            continue
        point = dict(raw_point)
        horizon = _finite(raw_horizon)
        point_horizon = _finite(point.get("horizon_sec"))
        if (
            horizon is not None
            and point_horizon is not None
            and int(round(horizon)) == int(round(point_horizon))
        ):
            point.pop("horizon_sec", None)
        probability_up = _finite(point.get("probability_up"))
        if probability_up is not None:
            implied_direction = "buy" if probability_up >= 0.5 else "sell"
            if str(point.get("direction") or "").lower() == implied_direction:
                point.pop("direction", None)
        if point.get("filter_reasons") == []:
            point.pop("filter_reasons", None)
        if point.get("research_only") is False:
            point.pop("research_only", None)
        if (
            "account_eligible" in payload
            and "account_eligible" in point
            and bool(point.get("account_eligible"))
            == bool(payload.get("account_eligible"))
        ):
            point.pop("account_eligible", None)
        if (
            "research_only" in payload
            and "research_only" in point
            and bool(point.get("research_only"))
            == bool(payload.get("research_only"))
        ):
            point.pop("research_only", None)
        calibrated_probability = _finite(point.get("calibrated_probability_up"))
        if (
            calibrated_probability is not None
            and probability_up is not None
            and math.isclose(
                calibrated_probability,
                probability_up,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            point.pop("calibrated_probability_up", None)
        direction_threshold = _finite(point.get("direction_threshold"))
        if direction_threshold is not None and math.isclose(
            direction_threshold,
            0.5,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            point.pop("direction_threshold", None)
        if (
            _finite(point.get("predicted_signed_pips")) is not None
            and _finite(point.get("projected_net_pips")) is not None
        ):
            # Ranking reprices the signed forecast against the current spread.
            point.pop("projected_net_pips", None)
        compact_curve[raw_horizon] = point
    payload["forecast_curve"] = compact_curve
    provenance = payload.get("signal_provenance")
    valid_horizons = provenance.get("valid_horizons_sec") if isinstance(provenance, dict) else None
    if isinstance(valid_horizons, list):
        curve_horizons = {
            int(round(value))
            for value in (_finite(key) for key in compact_curve)
            if value is not None
        }
        provenance_horizons = {
            int(round(value))
            for value in (_finite(item) for item in valid_horizons)
            if value is not None
        }
        if curve_horizons and curve_horizons == provenance_horizons:
            compact_provenance = dict(provenance)
            compact_provenance.pop("valid_horizons_sec", None)
            payload["signal_provenance"] = compact_provenance
    return payload


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _pip_size(instrument: str) -> float:
    return 0.01 if instrument.endswith("_JPY") else 0.0001


def _forecast_points(raw: dict[str, Any]) -> list[dict[str, Any]]:
    curve = raw.get("forecast_curve")
    if isinstance(curve, dict):
        rows: list[dict[str, Any]] = []
        for horizon, point in curve.items():
            if isinstance(point, dict):
                rows.append({"horizon_sec": horizon, **point})
        return rows
    if isinstance(curve, list):
        # Some prospective adapters naturally emit an ordered list under the
        # canonical field name.  Treat it exactly like the already-supported
        # ``forecasts``/``points`` aliases; normalization below still validates
        # every horizon, probability, direction, and magnitude independently.
        return [dict(row) for row in curve if isinstance(row, dict)]
    for key in ("forecasts", "points", "horizon_curve"):
        value = raw.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, dict)]
    if raw.get("horizon_sec") is not None:
        return [dict(raw)]
    return []


def _normalize_point(
    point: dict[str, Any],
    default_direction: str,
    account_eligible: bool,
) -> dict[str, Any]:
    horizon_value = _finite(point.get("horizon_sec"))
    if horizon_value is None:
        raise ContributionError("missing_horizon")
    horizon = int(round(horizon_value))
    if horizon <= 0 or horizon > MAX_HORIZON_SEC:
        raise ContributionError("invalid_horizon")

    probability_up = _finite(point.get("probability_up"))
    signed_pips = _finite(
        point.get("predicted_signed_pips", point.get("expected_signed_move_pips"))
    )
    direction = str(point.get("direction") or default_direction).lower()
    confidence = _finite(point.get("signal_confidence", point.get("confidence")))
    if probability_up is None and direction in {"buy", "sell"} and confidence is not None:
        side_probability = max(0.001, min(0.999, confidence))
        probability_up = side_probability if direction == "buy" else 1.0 - side_probability
    if probability_up is None and signed_pips is not None:
        probability_up = 0.500001 if signed_pips >= 0.0 else 0.499999
    if probability_up is None or not 0.0 <= probability_up <= 1.0:
        raise ContributionError("invalid_probability")
    probability_up = max(0.001, min(0.999, probability_up))

    implied_direction = "buy" if probability_up >= 0.5 else "sell"
    filter_reasons = [str(value) for value in point.get("filter_reasons") or []]
    if (
        signed_pips is not None
        and abs(probability_up - 0.5) >= 0.02
        and (signed_pips >= 0.0) != (probability_up >= 0.5)
    ):
        filter_reasons.append("direction_magnitude_conflict")
    point_account_eligible = bool(
        account_eligible and point.get("account_eligible", True)
    )
    normalized = {
        "horizon_sec": horizon,
        "direction": implied_direction,
        "probability_up": round(probability_up, 8),
        "account_eligible": bool(point_account_eligible and not filter_reasons),
        "research_only": bool(not point_account_eligible or filter_reasons),
        "filter_reasons": sorted(set(filter_reasons)),
    }
    if signed_pips is not None:
        normalized["predicted_signed_pips"] = round(signed_pips, 8)
    projected_net = _finite(point.get("projected_net_pips"))
    if projected_net is not None:
        normalized["projected_net_pips"] = round(projected_net, 8)
    for key in (
        "predicted_magnitude_pips",
        "calibrated_probability_up",
        "direction_threshold",
        "raw_probability_up",
        "quantile_low_pips",
        "quantile_high_pips",
        "movement_coefficient",
        "calibrated_brier",
        "calibration_n",
        "promotion_expected_net_pips",
    ):
        value = _finite(point.get(key))
        if value is not None:
            normalized[key] = value
    promotion_cell = str(point.get("promotion_cell_id") or "")
    if promotion_cell:
        normalized["promotion_cell_id"] = promotion_cell
    predictor_evidence = point.get("predictor_promotion_evidence")
    if isinstance(predictor_evidence, dict):
        normalized["predictor_promotion_evidence"] = predictor_evidence
    return normalized


class SignalContributionFeed:
    """SQLite WAL shared by strategy, equation, neural, and GPT producers."""

    def __init__(
        self,
        path: Path,
        model_report_path: Path | None = None,
        *,
        maintenance_batch_size: int = 5000,
        busy_timeout_ms: int = 3000,
        initialize_schema: bool = True,
        register_model_gap_inventory: bool = True,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        new_database = not self.path.exists() or self.path.stat().st_size == 0
        self.busy_timeout_ms = max(100, int(busy_timeout_ms))
        self.connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1000.0,
        )
        if new_database and initialize_schema:
            self.connection.execute("PRAGMA auto_vacuum=INCREMENTAL")
        if initialize_schema:
            self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        self.connection.execute(
            f"PRAGMA wal_autocheckpoint={WAL_AUTOCHECKPOINT_FRAMES}"
        )
        self.connection.execute("PRAGMA journal_size_limit=134217728")
        self.connection.execute("PRAGMA cache_size=-32768")
        self.connection.execute("PRAGMA mmap_size=268435456")
        if initialize_schema:
            self.connection.executescript(
                """
            CREATE TABLE IF NOT EXISTS candidates (
                candidate_id TEXT PRIMARY KEY,
                published_epoch REAL NOT NULL,
                expires_epoch REAL NOT NULL,
                source TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_signal_feed_expiry
                ON candidates(expires_epoch, published_epoch);
            CREATE TABLE IF NOT EXISTS executions (
                client_id TEXT PRIMARY KEY,
                candidate_id TEXT NOT NULL,
                submitted_epoch REAL NOT NULL,
                status TEXT NOT NULL,
                trade_id TEXT,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_signal_feed_execution_time
                ON executions(submitted_epoch);
            CREATE TABLE IF NOT EXISTS contributor_registry (
                contributor_id TEXT PRIMARY KEY,
                family TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                adapter_module TEXT NOT NULL,
                runtime_policy TEXT NOT NULL,
                expected INTEGER NOT NULL,
                account_eligible INTEGER NOT NULL,
                registered_epoch REAL NOT NULL,
                last_seen_epoch REAL,
                valid_count INTEGER NOT NULL DEFAULT 0,
                rejected_count INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_contributor_last_seen
                ON contributor_registry(last_seen_epoch, source_kind);
            CREATE TABLE IF NOT EXISTS contribution_audit (
                event_id TEXT PRIMARY KEY,
                contributor_id TEXT NOT NULL,
                observed_epoch REAL NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_contribution_audit_time
                ON contribution_audit(observed_epoch, status, reason);
            CREATE TABLE IF NOT EXISTS contribution_audit_rollup (
                event_id TEXT PRIMARY KEY,
                contributor_id TEXT NOT NULL,
                bucket_epoch INTEGER NOT NULL,
                first_observed_epoch REAL NOT NULL,
                last_observed_epoch REAL NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                occurrences INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_contribution_audit_rollup_time
                ON contribution_audit_rollup(last_observed_epoch, status, reason);
                """
            )
            self.connection.commit()
        else:
            required_tables = {
                str(row[0])
                for row in self.connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name IN (
                        'candidates', 'executions', 'contributor_registry'
                    )
                    """
                ).fetchall()
            }
            missing = {
                "candidates",
                "executions",
                "contributor_registry",
            }.difference(required_tables)
            if missing:
                self.connection.close()
                raise RuntimeError(
                    "signal feed is not initialized: "
                    + ",".join(sorted(missing))
                )
        self.last_prune_epoch = 0.0
        self.last_checkpoint_epoch = time.time()
        self.last_checkpoint = {
            "busy": 0,
            "wal_frames": 0,
            "checkpointed_frames": 0,
        }
        self.maintenance_batch_size = max(1, int(maintenance_batch_size))
        self.last_maintenance = {
            "candidate_rows_deleted": 0,
            "audit_rollup_rows_deleted": 0,
            "legacy_audit_rows_deleted": 0,
        }
        self._policy_cache: dict[str, dict[str, Any]] = {}
        self.model_report_path = model_report_path or (
            Path(__file__).resolve().parent
            / "data/oanda_training_manager/reports/modern_model_gap/unified_model_gap_market_latest.json"
        )
        if register_model_gap_inventory:
            self.register_model_gap_inventory(self.model_report_path)

    def register_model_gap_inventory(self, report_path: Path | None = None) -> int:
        try:
            from oanda_model_gap_registry import MODEL_SPECS
        except ModuleNotFoundError:
            from trad.oanda_model_gap_registry import MODEL_SPECS

        report = _read_json(report_path)
        evidence = {
            str(row.get("model") or row.get("model_id") or ""): row
            for row in report.get("models") or []
            if isinstance(row, dict)
        }
        rows = []
        for spec in MODEL_SPECS:
            model_evidence = evidence.get(spec.model_id) or {}
            current_policy = self._policy(spec.model_id)
            current_metadata = current_policy.get("metadata") or {}
            active_practice_promotion = (
                current_metadata.get("predictor_auto_promotion") or {}
            )
            rows.append(
                {
                    "contributor_id": spec.model_id,
                    "family": spec.family,
                    "source_kind": "model_gap",
                    "adapter_module": spec.adapter_module,
                    "runtime_policy": spec.runtime_policy,
                    "expected": True,
                    "account_eligible": bool(
                        model_evidence.get("production_eligible")
                        or (
                            current_policy.get("account_eligible")
                            and active_practice_promotion.get("account_scope")
                            == "practice_007_only"
                            and not active_practice_promotion.get(
                                "real_account_authorized", False
                            )
                        )
                    ),
                    "metadata": {
                        "provider": spec.provider,
                        "adapter_implemented": True,
                        "runtime_status": model_evidence.get("runtime_status"),
                        "market_backtest_complete": model_evidence.get(
                            "market_backtest_complete"
                        ),
                        "production_gate": model_evidence.get("production_gate"),
                        "historical_report_only": True,
                        **(
                            {"predictor_auto_promotion": active_practice_promotion}
                            if active_practice_promotion
                            else {}
                        ),
                    },
                }
            )
        self.register_contributors(rows)
        return len(rows)

    def register_contributors(self, rows: Iterable[dict[str, Any]]) -> int:
        now = time.time()
        values = []
        for row in rows:
            contributor_id = str(
                row.get("contributor_id")
                or row.get("model_id")
                or row.get("lane_id")
                or ""
            ).strip()
            if not contributor_id:
                continue
            values.append(
                (
                    contributor_id,
                    str(row.get("family") or "unknown"),
                    str(row.get("source_kind") or "dynamic"),
                    str(row.get("adapter_module") or ""),
                    str(row.get("runtime_policy") or "research_then_filter"),
                    int(bool(row.get("expected", True))),
                    int(bool(row.get("account_eligible", False))),
                    now,
                    _json(row.get("metadata") or {}),
                )
            )
        if not values:
            return 0
        self.connection.executemany(
            """
            INSERT INTO contributor_registry(
                contributor_id, family, source_kind, adapter_module,
                runtime_policy, expected, account_eligible,
                registered_epoch, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(contributor_id) DO UPDATE SET
                family=excluded.family,
                source_kind=excluded.source_kind,
                adapter_module=excluded.adapter_module,
                runtime_policy=excluded.runtime_policy,
                expected=excluded.expected,
                account_eligible=excluded.account_eligible,
                metadata_json=excluded.metadata_json
            """,
            values,
        )
        self.connection.commit()
        for row in values:
            self._policy_cache.pop(str(row[0]), None)
        return len(values)

    def set_contributor_account_eligibility(
        self,
        eligibility: dict[str, bool],
        *,
        account_scope: str,
        policy_status: str,
    ) -> int:
        """Apply a practice-only model gate without changing inventory identity."""

        if account_scope != "practice_007_only":
            raise ContributionError("invalid_account_promotion_scope")
        changed = 0
        for contributor_id, enabled in eligibility.items():
            row = self.connection.execute(
                "SELECT metadata_json FROM contributor_registry WHERE contributor_id = ?",
                (str(contributor_id),),
            ).fetchone()
            if row is None:
                continue
            try:
                metadata = json.loads(row[0])
            except (TypeError, json.JSONDecodeError):
                metadata = {}
            metadata["predictor_auto_promotion"] = {
                "account_scope": account_scope,
                "policy_status": str(policy_status),
                "account_eligible": bool(enabled),
                "real_account_authorized": False,
            }
            before = self.connection.total_changes
            self.connection.execute(
                """
                UPDATE contributor_registry
                SET account_eligible = ?, metadata_json = ?
                WHERE contributor_id = ?
                """,
                (int(bool(enabled)), _json(metadata), str(contributor_id)),
            )
            changed += self.connection.total_changes - before
            self._policy_cache.pop(str(contributor_id), None)
        self.connection.commit()
        return changed

    def _policy(self, contributor_id: str) -> dict[str, Any]:
        cached = self._policy_cache.get(contributor_id)
        if cached is not None:
            return cached
        row = self.connection.execute(
            """
            SELECT family, source_kind, adapter_module, runtime_policy,
                   expected, account_eligible, metadata_json
            FROM contributor_registry WHERE contributor_id = ?
            """,
            (contributor_id,),
        ).fetchone()
        if row is None:
            return {}
        try:
            metadata = json.loads(row[6])
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        policy = {
            "family": row[0],
            "source_kind": row[1],
            "adapter_module": row[2],
            "runtime_policy": row[3],
            "expected": bool(row[4]),
            "account_eligible": bool(row[5]),
            "metadata": metadata,
        }
        self._policy_cache[contributor_id] = policy
        return policy

    def normalize_forecast(
        self,
        raw: dict[str, Any],
        source: str,
        *,
        now: float | None = None,
        max_age_sec: float = 120.0,
    ) -> dict[str, Any]:
        now = time.time() if now is None else float(now)
        model_id = str(raw.get("model_id") or raw.get("model") or "").strip()
        if not model_id:
            raise ContributionError("missing_model_id")
        instrument = str(raw.get("instrument") or "").upper().strip()
        if not INSTRUMENT_PATTERN.fullmatch(instrument):
            raise ContributionError("invalid_instrument")
        timeframe = str(raw.get("input_timeframe") or "multi").upper().strip()
        if not TIMEFRAME_PATTERN.fullmatch(timeframe):
            raise ContributionError("invalid_timeframe")

        generated_epoch = None
        for key in (
            "generated_epoch",
            "prediction_epoch",
            "observed_epoch",
            "generated_utc",
            "prediction_time_utc",
            "entry_time",
        ):
            generated_epoch = _epoch(raw.get(key))
            if generated_epoch is not None:
                break
        generation_time_source = "producer"
        if generated_epoch is None:
            generated_epoch = now
            generation_time_source = "publish_time"
        age_sec = now - generated_epoch
        if age_sec < -30.0:
            raise ContributionError("future_timestamp")
        if age_sec > max(1.0, float(max_age_sec)):
            raise ContributionError("stale_forecast")

        policy = self._policy(model_id)
        if not policy:
            self.register_contributors(
                [
                    {
                        "contributor_id": model_id,
                        "family": str(raw.get("family") or "external_model"),
                        "source_kind": "external_unregistered",
                        "expected": False,
                        "account_eligible": False,
                        "metadata": {"first_source": source},
                    }
                ]
            )
            policy = self._policy(model_id)
        policy_account_eligible = bool(policy.get("account_eligible"))
        requested_account_eligible = bool(raw.get("account_eligible", True))
        account_eligible = policy_account_eligible and requested_account_eligible

        point_errors: list[str] = []
        points: dict[str, dict[str, Any]] = {}
        default_direction = str(raw.get("direction") or "").lower()
        for point in _forecast_points(raw):
            try:
                normalized = _normalize_point(
                    point,
                    default_direction,
                    account_eligible,
                )
            except ContributionError as exc:
                point_errors.append(str(exc))
                continue
            points[str(normalized["horizon_sec"])] = normalized
        if not points:
            reason = point_errors[0] if point_errors else "missing_forecast_curve"
            raise ContributionError(reason)

        horizons = sorted(int(value) for value in points)
        anchor_horizon = min(horizons, key=lambda value: (abs(value - 300), value))
        anchor = points[str(anchor_horizon)]
        direction = str(anchor["direction"])
        pip = _finite(raw.get("pip")) or _pip_size(instrument)
        bid = _finite(raw.get("bid"))
        ask = _finite(raw.get("ask"))
        spread_pips = _finite(raw.get("spread_pips"))
        if spread_pips is None and bid is not None and ask is not None and ask >= bid:
            spread_pips = (ask - bid) / pip
        signed_anchor = _finite(anchor.get("predicted_signed_pips"))
        signal_to_spread = 0.0
        if signed_anchor is not None and spread_pips is not None:
            signal_to_spread = abs(signed_anchor) / max(0.10, spread_pips)

        filtered_points = sum(bool(row.get("filter_reasons")) for row in points.values())
        eligible_points = sum(bool(row.get("account_eligible")) for row in points.values())
        candidate_id = str(raw.get("id") or "").strip()
        if not candidate_id:
            identity = _json(
                {
                    "model": model_id,
                    "instrument": instrument,
                    "timeframe": timeframe,
                    "generated_epoch": round(generated_epoch, 6),
                    "curve": points,
                }
            )
            candidate_id = "model-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
        family = str(raw.get("family") or policy.get("family") or "external_model")
        contributor_policy = {
            "registered": bool(policy),
            "source_kind": policy.get("source_kind"),
            "adapter_module": policy.get("adapter_module"),
            "runtime_policy": policy.get("runtime_policy"),
            "policy_account_eligible": policy_account_eligible,
            "requested_account_eligible": requested_account_eligible,
            "historical_report_used_as_live_value": False,
        }
        producer_metadata = raw.get("producer_metadata")
        if not isinstance(producer_metadata, dict):
            producer_metadata = {}
        candidate: dict[str, Any] = {
            "id": candidate_id,
            "feed_dedupe_key": str(raw.get("feed_dedupe_key") or (
                "latest-model-"
                + hashlib.sha256(
                    _json(
                        {
                            "model": model_id,
                            "instrument": instrument,
                            "timeframe": timeframe,
                        }
                    ).encode("utf-8")
                ).hexdigest()[:32]
            )),
            "lane_id": str(raw.get("lane_id") or f"model_signal.{model_id}.{timeframe.lower()}"),
            "family": family,
            "profile": str(raw.get("profile") or "live_model"),
            "model_id": model_id,
            "input_timeframe": timeframe,
            "training_timeframe": str(raw.get("training_timeframe") or timeframe),
            "signal_role": str(raw.get("signal_role") or "structural"),
            "instrument": instrument,
            "direction": direction,
            "pip": pip,
            "signal_confidence": (
                anchor["probability_up"]
                if direction == "buy"
                else 1.0 - anchor["probability_up"]
            ),
            "probability_up": anchor["probability_up"],
            "forecast_curve": points,
            "forecast_horizon_sec": anchor_horizon if len(points) == 1 else 0,
            "signal_reference_horizon_sec": anchor_horizon,
            "predicted_signed_pips": anchor.get("predicted_signed_pips"),
            "projected_net_pips": anchor.get("projected_net_pips"),
            "signal_to_spread": signal_to_spread,
            "spread_pips": spread_pips or 0.0,
            "atr_pips": _finite(raw.get("atr_pips")) or 0.0,
            "stop_loss_pips": _finite(raw.get("stop_loss_pips")) or 5.0,
            "take_profit_r": _finite(raw.get("take_profit_r")) or 1.5,
            "account_eligible": bool(eligible_points),
            "research_only": not bool(eligible_points),
            "research_blocked_reason": (
                "model_production_policy"
                if not policy_account_eligible
                else "predictor_cell_not_promoted"
                if not requested_account_eligible or not eligible_points
                else "forecast_data_quality"
                if filtered_points
                else ""
            ),
            "preconsensus_class": "accepted" if filtered_points == 0 else "near_threshold",
            "preconsensus_blockers": (
                ["model_forecast_data_quality"] if filtered_points else []
            ),
            "matrix_input_weight": 1.0 if filtered_points == 0 else 0.25,
            "source_kind": "live_model_forecast",
            "forecast_generated_epoch": generated_epoch,
            "forecast_age_sec_at_publish": round(max(0.0, age_sec), 6),
            "generation_time_source": generation_time_source,
            "signal_provenance": {
                "producer": source,
                "contributor_id": model_id,
                "policy": contributor_policy,
                "valid_horizons_sec": horizons,
                "discarded_point_reasons": sorted(set(point_errors)),
                "producer_metadata": producer_metadata,
            },
        }
        if bid is not None:
            candidate["bid"] = bid
        if ask is not None:
            candidate["ask"] = ask
        entry_time = raw.get("entry_time") or raw.get("prediction_time_utc")
        if entry_time:
            candidate["entry_time"] = str(entry_time)
        return candidate

    def _audit(
        self,
        contributor_id: str,
        status: str,
        reason: str,
        payload: dict[str, Any],
        observed_epoch: float,
        occurrences: int = 1,
    ) -> None:
        # Forecast/outcome ledgers retain every prediction. The contribution
        # audit only needs coverage and rejection counts, so store one compact
        # row per contributor/status/reason/minute instead of duplicating full
        # forecast curves on every cycle.
        bucket_epoch = int(observed_epoch // 60.0) * 60
        identity = _json(
            {
                "contributor": contributor_id,
                "status": status,
                "reason": reason,
                "bucket": bucket_epoch,
            }
        )
        event_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        points = _forecast_points(payload)
        compact_payload = {
            "id": str(payload.get("id") or ""),
            "model_id": str(payload.get("model_id") or payload.get("model") or ""),
            "lane_id": str(payload.get("lane_id") or ""),
            "family": str(payload.get("family") or ""),
            "instrument": str(payload.get("instrument") or ""),
            "input_timeframe": str(payload.get("input_timeframe") or ""),
            "horizons_sec": sorted(
                {
                    int(value)
                    for row in points
                    if (value := (_finite(row.get("horizon_sec")) or 0.0)) > 0.0
                }
            ),
            "account_eligible": bool(payload.get("account_eligible", False)),
            "research_only": bool(payload.get("research_only", False)),
        }
        self.connection.execute(
            """
            INSERT INTO contribution_audit_rollup(
                event_id, contributor_id, bucket_epoch,
                first_observed_epoch, last_observed_epoch,
                status, reason, occurrences, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                last_observed_epoch=excluded.last_observed_epoch,
                occurrences=contribution_audit_rollup.occurrences + excluded.occurrences,
                payload_json=excluded.payload_json
            """,
            (
                event_id,
                contributor_id,
                bucket_epoch,
                observed_epoch,
                observed_epoch,
                status,
                reason,
                max(1, int(occurrences)),
                _json(compact_payload),
            ),
        )

    def publish_forecasts(
        self,
        forecasts: Iterable[dict[str, Any]],
        source: str,
        ttl_sec: float,
        *,
        max_age_sec: float = 120.0,
    ) -> dict[str, Any]:
        now = time.time()
        raw_forecasts = list(forecasts)
        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, str]] = []
        audit_rollups: dict[
            tuple[str, str, str],
            tuple[dict[str, Any], int],
        ] = {}
        rejected_contributors: dict[str, int] = {}

        def queue_audit(
            contributor_id: str,
            status: str,
            reason: str,
            payload: dict[str, Any],
        ) -> None:
            key = (contributor_id, status, reason)
            previous = audit_rollups.get(key)
            audit_rollups[key] = (
                payload,
                1 if previous is None else previous[1] + 1,
            )

        # Register a new producer set in one transaction. Calling
        # normalize_forecast() directly still self-registers safely, while
        # the batch path avoids one WAL-writer acquisition per model.
        unregistered: dict[str, dict[str, Any]] = {}
        for raw in raw_forecasts:
            if not isinstance(raw, dict):
                continue
            model_id = str(raw.get("model_id") or raw.get("model") or "").strip()
            if not model_id or model_id in unregistered or self._policy(model_id):
                continue
            unregistered[model_id] = {
                "contributor_id": model_id,
                "family": str(raw.get("family") or "external_model"),
                "source_kind": "external_unregistered",
                "expected": False,
                "account_eligible": False,
                "metadata": {"first_source": source},
            }
        if unregistered:
            self.register_contributors(unregistered.values())

        for raw in raw_forecasts:
            if not isinstance(raw, dict):
                rejected.append({"model_id": "", "reason": "non_object_forecast"})
                continue
            model_id = str(raw.get("model_id") or raw.get("model") or "unknown")
            try:
                candidate = self.normalize_forecast(
                    raw,
                    source,
                    now=now,
                    max_age_sec=max_age_sec,
                )
            except ContributionError as exc:
                reason = str(exc)
                rejected.append({"model_id": model_id, "reason": reason})
                queue_audit(model_id, "rejected", reason, raw)
                rejected_contributors[model_id] = (
                    rejected_contributors.get(model_id, 0) + 1
                )
                continue
            accepted.append(candidate)
            queue_audit(model_id, "accepted", "", candidate)
        for (contributor_id, status, reason), (
            payload,
            occurrences,
        ) in audit_rollups.items():
            self._audit(
                contributor_id,
                status,
                reason,
                payload,
                now,
                occurrences,
            )
        for contributor_id, count in rejected_contributors.items():
            self.connection.execute(
                """
                UPDATE contributor_registry
                SET rejected_count = rejected_count + ?
                WHERE contributor_id = ?
                """,
                (count, contributor_id),
            )
        # Do not hold SQLite's single WAL writer while publish() normalizes
        # policies and serializes forecast curves. Other producers share this
        # feed, so release the short audit transaction first.
        self.connection.commit()
        maintenance = self.publish(accepted, source, ttl_sec)
        reasons: dict[str, int] = {}
        for row in rejected:
            reasons[row["reason"]] = reasons.get(row["reason"], 0) + 1
        return {
            "source": source,
            "received": len(accepted) + len(rejected),
            "accepted": len(accepted),
            "rejected": len(rejected),
            "rejection_reasons": dict(sorted(reasons.items())),
            "candidate_ids": [str(row["id"]) for row in accepted],
            "maintenance": maintenance,
        }

    def publish(
        self,
        candidates: Iterable[dict[str, Any]],
        source: str,
        ttl_sec: float,
    ) -> dict[str, Any]:
        publish_started = time.perf_counter()
        now = time.time()
        expiry = now + max(1.0, float(ttl_sec))
        rows = []
        contributor_counts: dict[str, int] = {}
        dynamic: list[dict[str, Any]] = []
        for candidate in candidates:
            candidate_id = str(candidate.get("id") or "")
            if not candidate_id:
                continue
            storage_id = str(candidate.get("feed_dedupe_key") or candidate_id)
            contributor_id = str(
                candidate.get("model_id")
                or candidate.get("lane_id")
                or candidate.get("family")
                or source
            )
            if not self._policy(contributor_id):
                dynamic.append(
                    {
                        "contributor_id": contributor_id,
                        "family": str(candidate.get("family") or "unknown"),
                        "source_kind": str(candidate.get("source_kind") or "strategy_signal"),
                        "expected": True,
                        "account_eligible": bool(candidate.get("account_eligible", True)),
                        "metadata": {"first_source": source},
                    }
                )
            contributor_counts[contributor_id] = contributor_counts.get(contributor_id, 0) + 1
            payload = _compact_transport_payload(candidate, source, now)
            rows.append((storage_id, now, expiry, source, _json(payload)))
        normalized_at = time.perf_counter()
        if dynamic:
            self.register_contributors(dynamic)
        registered_at = time.perf_counter()
        if rows:
            self.connection.executemany(
                """
                INSERT INTO candidates(candidate_id, published_epoch, expires_epoch, source, payload_json)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(candidate_id) DO UPDATE SET
                    published_epoch=excluded.published_epoch,
                    expires_epoch=excluded.expires_epoch,
                    source=excluded.source,
                    payload_json=excluded.payload_json
                """,
                rows,
            )
        candidates_written_at = time.perf_counter()
        for contributor_id, count in contributor_counts.items():
            self.connection.execute(
                """
                UPDATE contributor_registry
                SET last_seen_epoch = ?, valid_count = valid_count + ?
                WHERE contributor_id = ?
                """,
                (now, count, contributor_id),
            )
        registry_written_at = time.perf_counter()
        if now - self.last_prune_epoch >= 30.0:
            candidate_cursor = self.connection.execute(
                """
                DELETE FROM candidates
                WHERE candidate_id IN (
                    SELECT candidate_id FROM candidates
                    WHERE expires_epoch < ?
                    ORDER BY expires_epoch
                    LIMIT ?
                )
                """,
                (now - 60.0, self.maintenance_batch_size),
            )
            audit_cursor = self.connection.execute(
                """
                DELETE FROM contribution_audit_rollup
                WHERE event_id IN (
                    SELECT event_id FROM contribution_audit_rollup
                    WHERE last_observed_epoch < ?
                    ORDER BY last_observed_epoch
                    LIMIT ?
                )
                """,
                (now - 7 * 86400.0, self.maintenance_batch_size),
            )
            legacy_audit_cursor = self.connection.execute(
                """
                DELETE FROM contribution_audit
                WHERE event_id IN (
                    SELECT event_id FROM contribution_audit
                    WHERE observed_epoch < ?
                    ORDER BY observed_epoch
                    LIMIT ?
                )
                """,
                (now - 7 * 86400.0, self.maintenance_batch_size),
            )
            self.last_maintenance = {
                "candidate_rows_deleted": max(0, candidate_cursor.rowcount),
                "audit_rollup_rows_deleted": max(0, audit_cursor.rowcount),
                "legacy_audit_rows_deleted": max(0, legacy_audit_cursor.rowcount),
            }
            self.last_prune_epoch = now
        maintenance_at = time.perf_counter()
        self.connection.commit()
        committed_at = time.perf_counter()
        if now - self.last_checkpoint_epoch >= 60.0:
            wal_path = self.path.with_name(self.path.name + "-wal")
            try:
                wal_bytes = wal_path.stat().st_size
            except OSError:
                wal_bytes = 0
            # Never issue a synchronous checkpoint from a hot producer.
            # On Windows, a pinned reader can make even PASSIVE checkpoint
            # calls stall far beyond busy_timeout. SQLite's incremental
            # autocheckpoint runs on commit and keeps the writer path bounded.
            self.last_checkpoint = {
                "mode": "sqlite_autocheckpoint",
                "autocheckpoint_frames": WAL_AUTOCHECKPOINT_FRAMES,
                "wal_bytes": wal_bytes,
            }
            self.last_checkpoint_epoch = now
        return {
            **self.last_maintenance,
            "wal_checkpoint": dict(self.last_checkpoint),
            "latency_ms": {
                "normalize": round((normalized_at - publish_started) * 1000.0, 3),
                "register": round((registered_at - normalized_at) * 1000.0, 3),
                "candidate_upsert": round(
                    (candidates_written_at - registered_at) * 1000.0,
                    3,
                ),
                "registry_update": round(
                    (registry_written_at - candidates_written_at) * 1000.0,
                    3,
                ),
                "maintenance": round(
                    (maintenance_at - registry_written_at) * 1000.0,
                    3,
                ),
                "commit": round((committed_at - maintenance_at) * 1000.0, 3),
                "total": round((committed_at - publish_started) * 1000.0, 3),
            },
        }

    def recent(self, limit: int = DEFAULT_RECENT_LIMIT) -> list[dict[str, Any]]:
        cursor = self.connection.execute(
            """
            SELECT payload_json FROM candidates
            WHERE expires_epoch >= ?
            ORDER BY published_epoch DESC
            LIMIT ?
            """,
            (time.time(), max(1, int(limit))),
        )
        output: list[dict[str, Any]] = []
        for (payload_json,) in cursor.fetchall():
            try:
                payload = json.loads(payload_json)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                output.append(payload)
        return output

    def coverage(self, fresh_sec: float = 300.0) -> dict[str, Any]:
        now = time.time()
        registry = self.connection.execute(
            """
            SELECT contributor_id, family, source_kind, expected,
                   account_eligible, last_seen_epoch, valid_count, rejected_count
            FROM contributor_registry ORDER BY source_kind, contributor_id
            """
        ).fetchall()
        fresh_cutoff = now - max(1.0, float(fresh_sec))
        fresh = [row for row in registry if row[5] is not None and row[5] >= fresh_cutoff]
        model_gap = [row for row in registry if row[2] == "model_gap"]
        active_sources = [
            {"source": row[0], "candidates": int(row[1])}
            for row in self.connection.execute(
                """
                SELECT source, COUNT(*) FROM candidates
                WHERE expires_epoch >= ? GROUP BY source ORDER BY COUNT(*) DESC, source
                """,
                (now,),
            ).fetchall()
        ]
        audit_reasons = [
            {
                "status": row[0],
                "reason": row[1],
                "count": int(row[2]),
            }
            for row in self.connection.execute(
                """
                SELECT status, reason, SUM(occurrences)
                FROM contribution_audit_rollup
                WHERE last_observed_epoch >= ?
                GROUP BY status, reason
                ORDER BY SUM(occurrences) DESC, status, reason
                """,
                (now - 86400.0,),
            ).fetchall()
        ]
        return {
            "schema_version": 1,
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "database": str(self.path.resolve()),
            "registered_contributors": len(registry),
            "expected_contributors": sum(bool(row[3]) for row in registry),
            "fresh_contributors": len(fresh),
            "fresh_window_sec": float(fresh_sec),
            "account_eligible_contributors": sum(bool(row[4]) for row in registry),
            "model_gap": {
                "registered": len(model_gap),
                "adapter_implemented": len(model_gap),
                "fresh_live_contributors": sum(
                    row[5] is not None and row[5] >= fresh_cutoff for row in model_gap
                ),
                "account_eligible": sum(bool(row[4]) for row in model_gap),
                "never_contributed": [row[0] for row in model_gap if row[5] is None],
            },
            "active_feed_sources": active_sources,
            "audit_24h": audit_reasons,
            "fresh_contributor_ids": [row[0] for row in fresh],
            "contract": {
                "all_fresh_finite_predictions_enter_research_consensus": True,
                "execution_requires_registered_account_eligibility": True,
                "historical_reports_are_not_live_predictions": True,
                "stale_and_invalid_outputs_are_audited_not_ranked": True,
                "audit_payloads_are_minute_rollups": True,
                "latest_model_snapshot_replaces_prior_feed_snapshot": True,
            },
        }

    def seconds_since_last_fill(self) -> float:
        row = self.connection.execute(
            "SELECT MAX(submitted_epoch) FROM executions WHERE status = 'filled'"
        ).fetchone()
        value = _finite(row[0]) if row and row[0] is not None else None
        return math.inf if value is None or value <= 0.0 else max(0.0, time.time() - value)

    def record_execution(
        self,
        client_id: str,
        candidate: dict[str, Any],
        status: str,
        trade_id: str = "",
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO executions(
                client_id, candidate_id, submitted_epoch, status, trade_id, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                str(candidate.get("id") or ""),
                time.time(),
                status,
                trade_id,
                _json(candidate),
            ),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.commit()
        self.connection.close()


def _input_rows(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        rows = []
        for line in text.splitlines():
            if not line.strip():
                continue
            value = json.loads(line)
            if isinstance(value, dict):
                rows.append(value)
        return rows
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        rows = payload.get("forecasts") or payload.get("signals")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        return [payload]
    return []


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--ttl-sec", type=float, default=65.0)
    parser.add_argument("--max-age-sec", type=float, default=120.0)
    parser.add_argument("--model-report", type=Path)
    parser.add_argument("--coverage-output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    feed = SignalContributionFeed(args.database, args.model_report)
    try:
        result = feed.publish_forecasts(
            _input_rows(args.input),
            args.source,
            args.ttl_sec,
            max_age_sec=args.max_age_sec,
        )
        coverage = feed.coverage(max(args.max_age_sec, args.ttl_sec))
        payload = {"publish": result, "coverage": coverage}
        if args.coverage_output:
            _atomic_json(args.coverage_output, payload)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if not result["rejected"] else 2
    finally:
        feed.close()


if __name__ == "__main__":
    raise SystemExit(main())
