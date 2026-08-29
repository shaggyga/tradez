"""Read-only, point-in-time normalization of official FX research facts.

This adapter is intentionally small and conservative.  It reads the existing
append-only ledgers, normalizes their provenance, and reports unavailable or
degraded evidence explicitly.  It has no broker, authorization, lifecycle,
supervisor, or execution imports and never assigns a trading direction.

The live SQLite databases use WAL mode.  Connections therefore use URI
``mode=ro`` rather than ``immutable=1`` so committed WAL pages remain visible.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .immutable_event_clock import (
    CONTRACT_ID as IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
    EventClockLedgerError,
    reconstruct_as_of as reconstruct_event_clock_as_of,
)


UTC = dt.timezone.utc
SOURCE_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = SOURCE_ROOT / "data" / "oanda_training_manager"
STATE_ROOT = DATA_ROOT / "state"

ADAPTER_CONTRACT_ID = "official_fact_adapter_v1_20260817"
CANONICAL_CURRENCIES = (
    "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD",
    "HUF", "JPY", "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB",
    "TRY", "USD", "ZAR",
)


@dataclass(frozen=True)
class OfficialFactPaths:
    """All inputs are existing research artifacts; none are write targets."""

    source_governance_db: Path = STATE_ROOT / "source_governance_v1.sqlite"
    macro_surprise_db: Path = STATE_ROOT / "macro_surprise_v1.sqlite"
    daily_rates_db: Path = STATE_ROOT / "official_daily_rate_context_v1.sqlite"
    internal_expectations_db: Path = STATE_ROOT / "internal_macro_expectation_v1.sqlite"
    policy_baselines_json: Path = (
        SOURCE_ROOT / "config" / "official_policy_statement_baselines_v2_20260816.json"
    )
    event_preflight_json: Path = STATE_ROOT / "event_technical_preflight_v1.json"
    immutable_event_clock_db: Path | None = (
        DATA_ROOT / "research_ledgers" / "immutable_event_clock_v1.sqlite"
    )
    source_coverage_json: Path = (
        DATA_ROOT / "local_news_sentiment" / "source_coverage_latest.json"
    )
    clock_integrity_json: Path = STATE_ROOT / "clock_integrity_v1.json"
    intraday_rates_config_json: Path = SOURCE_ROOT / "config" / "rates_policy_repricing_v1.json"


def _parse_utc(value: Any, *, required: bool = False) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        if required:
            raise ValueError("a timezone-aware UTC timestamp is required")
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        if required:
            raise ValueError(f"invalid timestamp: {text!r}") from exc
        return None
    if parsed.tzinfo is None:
        if required:
            raise ValueError("timestamp must include a timezone")
        return None
    return parsed.astimezone(UTC)


def _iso(value: dt.datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False, default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


class OfficialFactAdapter:
    """Build a bounded, immutable information snapshot at an exact cutoff."""

    def __init__(
        self,
        paths: OfficialFactPaths | None = None,
        *,
        sqlite_timeout_sec: float = 5.0,
        maximum_rows_per_input: int = 5_000,
        timely_release_latency_sec: float = 300.0,
    ) -> None:
        self.paths = paths or OfficialFactPaths()
        self.sqlite_timeout_sec = max(0.1, min(float(sqlite_timeout_sec), 30.0))
        self.maximum_rows_per_input = max(1, min(int(maximum_rows_per_input), 100_000))
        self.timely_release_latency_sec = max(0.0, float(timely_release_latency_sec))

    def _connect(self, path: Path) -> sqlite3.Connection:
        if not path.is_file():
            raise FileNotFoundError(path)
        connection = sqlite3.connect(
            f"file:{path.resolve().as_posix()}?mode=ro",
            uri=True,
            timeout=self.sqlite_timeout_sec,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute(
            f"PRAGMA busy_timeout={int(round(self.sqlite_timeout_sec * 1000.0))}"
        )
        return connection

    @staticmethod
    def _relation_exists(connection: sqlite3.Connection, name: str) -> bool:
        return connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name=? AND type IN ('table','view')",
            (name,),
        ).fetchone() is not None

    def _quarantined_source_events(self, gaps: list[dict[str, Any]]) -> set[str]:
        path = self.paths.source_governance_db
        try:
            connection = self._connect(path)
        except (FileNotFoundError, sqlite3.Error) as exc:
            gaps.append(self._gap("source_governance_unavailable", path, exc))
            return set()
        try:
            if not self._relation_exists(connection, "source_events_causal_v1"):
                gaps.append(self._gap("causal_source_view_unavailable", path))
            if not self._relation_exists(connection, "source_event_quarantines"):
                gaps.append(self._gap("source_quarantine_table_unavailable", path))
                return set()
            rows = connection.execute(
                "SELECT source_event_id FROM source_event_quarantines LIMIT ?",
                (self.maximum_rows_per_input + 1,),
            ).fetchall()
            if len(rows) > self.maximum_rows_per_input:
                gaps.append(self._gap("source_quarantine_limit_reached", path))
                rows = rows[: self.maximum_rows_per_input]
            return {str(row[0]) for row in rows}
        except sqlite3.Error as exc:
            gaps.append(self._gap("source_quarantine_query_failed", path, exc))
            return set()
        finally:
            connection.close()

    @staticmethod
    def _gap(code: str, path: Path | None = None, error: Exception | None = None) -> dict[str, Any]:
        result: dict[str, Any] = {"code": code}
        if path is not None:
            # Gap identities must survive snapshot restoration on another
            # machine.  Keep only the stable artifact name; absolute local
            # paths belong in operator logs, not evidence hashes.
            result["artifact_name"] = path.name
        if error is not None:
            result["error_class"] = type(error).__name__
        return result

    def _macro_facts(
        self,
        cutoff: dt.datetime,
        currencies: set[str],
        quarantined: set[str],
        gaps: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        path = self.paths.macro_surprise_db
        try:
            connection = self._connect(path)
        except (FileNotFoundError, sqlite3.Error) as exc:
            gaps.append(self._gap("macro_surprise_ledger_unavailable", path, exc))
            return []
        try:
            if not self._relation_exists(connection, "macro_release_revisions"):
                gaps.append(self._gap("macro_release_table_unavailable", path))
                return []
            query = """
                SELECT r.row_id,r.revision_id,r.release_key,r.source_event_id,
                       r.recorded_utc,r.causal_known_utc,r.scheduled_utc,
                       r.source_reported_update_utc,r.event_series_id,r.event_name,
                       r.event_country,r.currencies_json,r.reference_period,
                       r.reference_date,r.importance,r.unit,r.actual_value,
                       r.consensus_value,r.previous_value,r.revised_previous_value,
                       r.surprise_raw,r.standardized_surprise,
                       r.known_before_recorded_timestamp,r.source_id,r.source_name,
                       r.source_url,r.source_verified,r.source_direct,
                       r.payload_sha256,r.payload_json
                  FROM macro_release_revisions AS r
                 WHERE r.actual_value IS NOT NULL
                   AND julianday(r.causal_known_utc)<=julianday(?)
                   AND julianday(r.recorded_utc)<=julianday(?)
                   AND r.row_id=(
                       SELECT MAX(r2.row_id) FROM macro_release_revisions AS r2
                        WHERE r2.release_key=r.release_key
                          AND julianday(r2.causal_known_utc)<=julianday(?)
                          AND julianday(r2.recorded_utc)<=julianday(?)
                   )
                 ORDER BY r.causal_known_utc DESC,r.row_id DESC
                 LIMIT ?
            """
            rows = connection.execute(
                query,
                (
                    _iso(cutoff), _iso(cutoff), _iso(cutoff), _iso(cutoff),
                    self.maximum_rows_per_input + 1,
                ),
            ).fetchall()
        except sqlite3.Error as exc:
            gaps.append(self._gap("macro_release_query_failed", path, exc))
            return []
        finally:
            connection.close()
        if len(rows) > self.maximum_rows_per_input:
            gaps.append(self._gap("macro_release_limit_reached", path))
            rows = rows[: self.maximum_rows_per_input]

        output: list[dict[str, Any]] = []
        for row in rows:
            source_event_id = str(row[3] or "")
            if source_event_id in quarantined:
                continue
            payload = _json_object(row[29])
            try:
                affected = [str(value).upper() for value in json.loads(row[11] or "[]")]
            except (TypeError, ValueError, json.JSONDecodeError):
                affected = []
            known = _parse_utc(row[5])
            recorded = _parse_utc(row[4])
            if known is None or recorded is None:
                continue
            effective = max(known, recorded)
            if effective > cutoff:
                continue
            published = _parse_utc(
                payload.get("source_native_published_utc")
                or payload.get("published_utc")
                or row[7]
            )
            retrieved = _parse_utc(payload.get("first_seen_utc")) or recorded
            degradation: list[str] = []
            if not bool(row[26]) or not bool(row[27]):
                evidence_class = "unverified_source_context"
                degradation.append("source_not_both_direct_and_verified")
            elif bool(payload.get("source_listing_bootstrap")):
                evidence_class = "bootstrap_context"
                degradation.append("source_listing_bootstrap")
            elif bool(payload.get("published_time_inferred")):
                evidence_class = "inferred_publication_context"
                degradation.append("published_time_inferred")
            elif published is None:
                evidence_class = "publication_time_unavailable"
                degradation.append("no_source_native_publication_timestamp")
            else:
                latency = (known - published).total_seconds()
                if latency < -5.0:
                    evidence_class = "clock_inconsistent"
                    degradation.append("first_seen_precedes_publication")
                elif latency > self.timely_release_latency_sec:
                    evidence_class = "late_observation_context"
                    degradation.append("observed_after_timely_release_window")
                else:
                    evidence_class = "prospective_causal"

            raw_consensus = _finite(row[17])
            consensus_causal = bool(row[22]) and raw_consensus is not None
            raw_surprise = _finite(row[20])
            standardized = _finite(row[21])
            if raw_consensus is not None and not consensus_causal:
                degradation.append("noncausal_consensus_ignored")
            if standardized is not None:
                # The ledger's stored z-score predates causal-only filtering of
                # its reference distribution.  Preserve it as a diagnostic but
                # never promote it as canonical point-in-time evidence.
                degradation.append("stored_standardization_prior_not_causal_filtered")
            for currency in affected:
                if currency not in currencies:
                    continue
                fact = {
                    "fact_id": f"{row[1]}:{currency}",
                    "currency": currency,
                    "fact_type": "official_macro_actual",
                    "series_id": str(row[8] or ""),
                    "event_name": str(row[9] or ""),
                    "release_key": str(row[2] or ""),
                    "reference_period": str(row[12] or ""),
                    "reference_date": str(row[13] or ""),
                    "unit": str(row[15] or ""),
                    "importance": str(row[14] or ""),
                    "actual_value": _finite(row[16]),
                    "previous_value": _finite(row[18]),
                    "revised_previous_value": _finite(row[19]),
                    "consensus_value": raw_consensus if consensus_causal else None,
                    "noncausal_consensus_value": raw_consensus if not consensus_causal else None,
                    "consensus_causal": consensus_causal,
                    "surprise_raw": raw_surprise if consensus_causal else None,
                    "standardized_surprise": None,
                    "noncanonical_ledger_standardized_surprise": standardized,
                    "scheduled_utc": str(row[6] or "") or None,
                    "published_at_utc": _iso(published),
                    "first_seen_at_utc": _iso(known),
                    "retrieved_at_utc": _iso(retrieved),
                    "ledger_recorded_at_utc": _iso(recorded),
                    "effective_from_utc": _iso(effective),
                    "source_event_id": source_event_id,
                    "source_id": str(row[23] or ""),
                    "source_name": str(row[24] or ""),
                    "source_url": str(row[25] or ""),
                    "source_contract_id": str(payload.get("source_contract_id") or ""),
                    "source_cohort_id": str(payload.get("source_cohort_id") or ""),
                    "collector_contract_id": str(payload.get("collector_contract_id") or ""),
                    "collector_cohort_id": str(payload.get("collector_cohort_id") or ""),
                    "raw_payload_sha256": str(row[28] or ""),
                    "evidence_class": evidence_class,
                    "degradation_reasons": sorted(set(degradation)),
                    "direction_policy": "abstain",
                }
                output.append(fact)
        return output

    def _rate_facts(
        self,
        cutoff: dt.datetime,
        currencies: set[str],
        gaps: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        path = self.paths.daily_rates_db
        try:
            connection = self._connect(path)
        except (FileNotFoundError, sqlite3.Error) as exc:
            gaps.append(self._gap("daily_rate_ledger_unavailable", path, exc))
            return []
        try:
            if not self._relation_exists(connection, "daily_rate_observations"):
                gaps.append(self._gap("daily_rate_table_unavailable", path))
                return []
            rows = connection.execute(
                """
                WITH ranked AS (
                  SELECT rowid AS source_rowid,*,
                         ROW_NUMBER() OVER (
                           PARTITION BY currency,series_id
                           ORDER BY rate_date DESC,version DESC,rowid DESC
                         ) AS rank_at_cutoff
                    FROM daily_rate_observations
                   WHERE julianday(first_seen_utc)<=julianday(?)
                )
                SELECT * FROM ranked WHERE rank_at_cutoff=1
                 ORDER BY currency,series_id LIMIT ?
                """,
                (_iso(cutoff), self.maximum_rows_per_input + 1),
            ).fetchall()
        except sqlite3.Error as exc:
            gaps.append(self._gap("daily_rate_query_failed", path, exc))
            return []
        finally:
            connection.close()
        if len(rows) > self.maximum_rows_per_input:
            gaps.append(self._gap("daily_rate_limit_reached", path))
            rows = rows[: self.maximum_rows_per_input]
        output = []
        for row in rows:
            value = dict(row)
            currency = str(value.get("currency") or "").upper()
            if currency not in currencies:
                continue
            prospective = bool(value.get("prospective_eligible")) and not bool(
                value.get("bootstrap_current_view")
            )
            degradation = ["daily_rate_is_not_intraday_repricing"]
            if not prospective:
                degradation.append("bootstrap_or_revision_context_only")
            output.append(
                {
                    "fact_id": str(value.get("observation_id") or ""),
                    "currency": currency,
                    "fact_type": "official_daily_rate",
                    "series_id": str(value.get("series_id") or ""),
                    "rate_date": str(value.get("rate_date") or ""),
                    "rate_pct": _finite(value.get("rate_pct")),
                    "consensus_value": None,
                    "consensus_causal": False,
                    "published_at_utc": None,
                    "first_seen_at_utc": str(value.get("first_seen_utc") or ""),
                    "retrieved_at_utc": str(value.get("first_seen_utc") or ""),
                    "effective_from_utc": str(value.get("first_seen_utc") or ""),
                    "source_id": str(value.get("source_id") or ""),
                    "source_name": str(value.get("provider") or ""),
                    "source_contract_id": str(value.get("source_contract_id") or ""),
                    "source_cohort_id": str(value.get("cohort_id") or ""),
                    "raw_payload_sha256": str(value.get("raw_archive_sha256") or ""),
                    "observation_kind": str(value.get("observation_kind") or ""),
                    "evidence_class": (
                        "prospective_daily_context" if prospective else "bootstrap_context"
                    ),
                    "degradation_reasons": degradation,
                    "direction_policy": "abstain",
                }
            )
        return output

    def _expectation_facts(
        self,
        cutoff: dt.datetime,
        currencies: set[str],
        gaps: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        path = self.paths.internal_expectations_db
        try:
            connection = self._connect(path)
        except (FileNotFoundError, sqlite3.Error) as exc:
            gaps.append(self._gap("internal_expectation_ledger_unavailable", path, exc))
            return []
        try:
            if not self._relation_exists(connection, "expectation_forecasts"):
                gaps.append(self._gap("internal_expectation_table_unavailable", path))
                return []
            rows = connection.execute(
                """
                WITH ranked AS (
                  SELECT *,ROW_NUMBER() OVER (
                    PARTITION BY currency,event_series_id ORDER BY issued_utc DESC,forecast_id DESC
                  ) AS rank_at_cutoff
                    FROM expectation_forecasts
                   WHERE julianday(issued_utc)<=julianday(?)
                     AND julianday(expires_utc)>julianday(?)
                )
                SELECT * FROM ranked WHERE rank_at_cutoff=1
                 ORDER BY currency,event_series_id LIMIT ?
                """,
                (_iso(cutoff), _iso(cutoff), self.maximum_rows_per_input + 1),
            ).fetchall()
        except sqlite3.Error as exc:
            gaps.append(self._gap("internal_expectation_query_failed", path, exc))
            return []
        finally:
            connection.close()
        if len(rows) > self.maximum_rows_per_input:
            gaps.append(self._gap("internal_expectation_limit_reached", path))
            rows = rows[: self.maximum_rows_per_input]
        output = []
        for row in rows:
            value = dict(row)
            currency = str(value.get("currency") or "").upper()
            if currency not in currencies:
                continue
            issued = _parse_utc(value.get("issued_utc"))
            training_cutoff = _parse_utc(value.get("training_cutoff_utc"))
            effective_candidates = [
                moment for moment in (issued, training_cutoff) if moment is not None
            ]
            if not effective_candidates:
                continue
            effective = max(effective_candidates)
            if effective > cutoff:
                continue
            output.append(
                {
                    "fact_id": str(value.get("forecast_id") or ""),
                    "currency": currency,
                    "fact_type": "internal_macro_expectation",
                    "series_id": str(value.get("event_series_id") or ""),
                    "event_name": str(value.get("event_name") or ""),
                    "unit": str(value.get("unit") or ""),
                    "expected_value": _finite(value.get("expected_value")),
                    "training_episode_count": int(value.get("training_episode_count") or 0),
                    "training_cutoff_utc": str(value.get("training_cutoff_utc") or ""),
                    "expires_utc": str(value.get("expires_utc") or ""),
                    "consensus_value": None,
                    "consensus_causal": False,
                    "published_at_utc": None,
                    "first_seen_at_utc": _iso(issued),
                    "retrieved_at_utc": _iso(issued),
                    "effective_from_utc": _iso(effective),
                    "source_id": "internal_macro_expectation",
                    "source_name": str(value.get("model_name") or ""),
                    "source_contract_id": ADAPTER_CONTRACT_ID,
                    "source_cohort_id": str(value.get("cohort_id") or ""),
                    "raw_payload_sha256": str(value.get("training_fingerprint_sha256") or ""),
                    "evidence_class": "internal_baseline_nonconsensus",
                    "degradation_reasons": ["not_market_consensus", "research_baseline_only"],
                    "direction_policy": "abstain",
                }
            )
        return output

    def _policy_facts(
        self,
        cutoff: dt.datetime,
        currencies: set[str],
        gaps: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        path = self.paths.policy_baselines_json
        payload = _read_json(path)
        if not payload:
            gaps.append(self._gap("policy_baselines_unavailable", path))
            return []
        created = _parse_utc(payload.get("created_utc"))
        output = []
        for index, row in enumerate(payload.get("baselines") or []):
            if not isinstance(row, Mapping):
                continue
            currency = str(row.get("currency") or "").upper()
            if currency not in currencies:
                continue
            known = _parse_utc(row.get("known_utc"))
            detail = _parse_utc(row.get("detail_available_utc"))
            published = _parse_utc(row.get("published_utc"))
            effective_candidates = [
                value for value in (known, detail, published, created) if value is not None
            ]
            if not effective_candidates:
                continue
            effective = max(effective_candidates)
            if effective > cutoff:
                continue
            summary = str(row.get("summary") or "")
            degradation = ["prior_policy_context_only", "semantic_direction_not_assigned"]
            if bool(row.get("source_listing_bootstrap")):
                degradation.append("source_listing_bootstrap")
            output.append(
                {
                    "fact_id": f"{row.get('event_id') or index}:{currency}",
                    "currency": currency,
                    "fact_type": "official_policy_document_context",
                    "series_id": str(row.get("document_class") or ""),
                    "event_name": str(row.get("headline") or ""),
                    "text_excerpt": summary[:512],
                    "payload_ref": f"config/{path.name}#baseline={index}",
                    "consensus_value": None,
                    "consensus_causal": False,
                    "published_at_utc": _iso(published),
                    "first_seen_at_utc": _iso(known),
                    "retrieved_at_utc": _iso(detail or known),
                    "effective_from_utc": _iso(effective),
                    "source_id": str(row.get("source_id") or ""),
                    "source_name": "official_policy_publisher",
                    "source_url": str(row.get("source_url") or ""),
                    "source_contract_id": str(payload.get("contract_id") or ""),
                    "source_cohort_id": str(payload.get("contract_id") or ""),
                    "raw_payload_sha256": str(row.get("raw_payload_sha256") or ""),
                    "evidence_class": "policy_context_only",
                    "degradation_reasons": degradation,
                    "direction_policy": "abstain",
                }
            )
        return output

    @staticmethod
    def _is_policy_clock(row: Mapping[str, Any]) -> bool:
        text = " ".join(
            (
                str(row.get("category") or ""),
                str(row.get("headline") or ""),
                str(row.get("upstream_event_id") or row.get("event_id") or ""),
            )
        ).lower()
        return any(
            token in text
            for token in (
                "monetary_policy",
                "monetary policy",
                "policy decision",
                "policy rate",
                "interest rate decision",
                "loan prime rate",
            )
        )

    def _immutable_upcoming_events(
        self,
        cutoff: dt.datetime,
        currencies: set[str],
        gaps: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | None:
        path = self.paths.immutable_event_clock_db
        if path is None:
            gaps.append({"code": "immutable_event_clock_not_configured"})
            return None
        if not path.is_file():
            gaps.append(self._gap("immutable_event_clock_ledger_unavailable", path))
            return None
        try:
            snapshot = reconstruct_event_clock_as_of(path, cutoff)
        except (OSError, sqlite3.Error, EventClockLedgerError, ValueError) as exc:
            gaps.append(self._gap("immutable_event_clock_query_failed", path, exc))
            return None
        snapshot_id = snapshot.get("snapshot_id")
        if not snapshot_id:
            gaps.append(self._gap("immutable_event_clock_no_snapshot_at_cutoff", path))
            return None
        capture_attestation = dict(snapshot.get("clock_attestation") or {})
        if not bool(capture_attestation.get("attested")):
            gaps.append(self._gap("immutable_event_clock_capture_unattested", path))
        output: list[dict[str, Any]] = []
        for row in snapshot.get("events") or []:
            if not isinstance(row, Mapping):
                continue
            scheduled = _parse_utc(row.get("scheduled_utc"))
            if scheduled is None or scheduled < cutoff:
                continue
            direct_currencies = {
                str(value).upper()
                for value in row.get("direct_currencies") or []
                if str(value).upper() in currencies
            }
            event_currencies = sorted(
                {
                    str(value).upper()
                    for value in row.get("currencies") or []
                    if str(value).upper() in currencies
                }
            )
            for currency in event_currencies:
                material = {
                    "event_id": str(row.get("upstream_event_id") or ""),
                    "event_version_id": str(row.get("event_version_id") or ""),
                    "currency": currency,
                    "category": str(row.get("category") or ""),
                    "scheduled_utc": _iso(scheduled),
                    "schedule_window_end_utc": (
                        str(row.get("schedule_window_end_utc") or "") or None
                    ),
                    "timing_precision": str(row.get("timing_precision") or ""),
                    "policy_event": self._is_policy_clock(row),
                    "policy_dependency": False,
                    "direct_event_currency": (
                        currency in direct_currencies if direct_currencies else None
                    ),
                    "driver_currency": currency,
                    "headline": str(row.get("headline") or ""),
                }
                output.append(
                    {
                        **material,
                        "fact_type": "official_event_clock",
                        "known_from_snapshot_utc": str(
                            snapshot.get("snapshot_captured_utc") or ""
                        ),
                        "ledger_effective_known_utc": str(
                            row.get("ledger_effective_known_utc") or ""
                        ),
                        "clock_provenance_state": "immutable_ledger_snapshot",
                        "clock_snapshot_id": str(snapshot_id),
                        "clock_source_contract_id": IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
                        "consensus_causal": False,
                        "evidence_class": "scheduled_clock_only",
                        "degradation_reasons": ["direction_unknown_until_release"],
                        "direction_policy": "abstain",
                        "raw_payload_sha256": _stable_hash(material),
                    }
                )
                if len(output) >= self.maximum_rows_per_input:
                    gaps.append(self._gap("immutable_event_clock_limit_reached", path))
                    break
            if len(output) >= self.maximum_rows_per_input:
                break
        provenance = {
            "state": "immutable_ledger_snapshot",
            "contract_id": IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
            "snapshot_id": str(snapshot_id),
            "snapshot_captured_utc": str(snapshot.get("snapshot_captured_utc") or ""),
            "source_generated_utc": str(snapshot.get("source_generated_utc") or ""),
            "events_sha256": str(snapshot.get("events_sha256") or ""),
            "clock_attestation": capture_attestation,
            "fallback_used": False,
            "complete_snapshot_at_cutoff": True,
        }
        return output, provenance

    def _mutable_upcoming_events(
        self,
        cutoff: dt.datetime,
        currencies: set[str],
        gaps: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        path = self.paths.event_preflight_json
        payload = _read_json(path)
        generated = _parse_utc(payload.get("generated_utc")) if payload else None
        if not payload:
            gaps.append(self._gap("event_preflight_unavailable", path))
            return [], {
                "state": "unavailable",
                "fallback_used": False,
                "complete_snapshot_at_cutoff": False,
            }
        if generated is None or generated > cutoff:
            gaps.append(self._gap("event_preflight_not_known_at_cutoff", path))
            return [], {
                "state": "mutable_view_not_known_at_cutoff",
                "generated_utc": _iso(generated),
                "fallback_used": False,
                "complete_snapshot_at_cutoff": False,
            }
        gaps.append(self._gap("mutable_event_preflight_fallback_used", path))
        output = []
        for row in payload.get("events") or []:
            if not isinstance(row, Mapping):
                continue
            currency = str(row.get("currency") or "").upper()
            scheduled = _parse_utc(row.get("scheduled_utc"))
            if currency not in currencies or scheduled is None or scheduled < cutoff:
                continue
            material = {
                "event_id": str(row.get("event_id") or ""),
                "currency": currency,
                "category": str(row.get("category") or ""),
                "scheduled_utc": _iso(scheduled),
                "schedule_window_end_utc": str(row.get("schedule_window_end_utc") or "") or None,
                "timing_precision": str(row.get("timing_precision") or ""),
                "policy_event": bool(row.get("policy_event")),
                "policy_dependency": bool(row.get("policy_dependency")),
                "driver_currency": str(row.get("driver_currency") or currency),
                "headline": str(row.get("headline") or ""),
            }
            output.append(
                {
                    **material,
                    "fact_type": "official_event_clock",
                    "known_from_snapshot_utc": _iso(generated),
                    "clock_provenance_state": "mutable_current_cutoff_safe_fallback",
                    "clock_snapshot_id": None,
                    "clock_source_contract_id": str(payload.get("contract_id") or ""),
                    "consensus_causal": False,
                    "evidence_class": "scheduled_clock_only",
                    "degradation_reasons": ["direction_unknown_until_release"],
                    "direction_policy": "abstain",
                    "raw_payload_sha256": _stable_hash(material),
                }
            )
            if len(output) >= self.maximum_rows_per_input:
                gaps.append(self._gap("event_preflight_limit_reached", path))
                break
        return output, {
            "state": "mutable_current_cutoff_safe_fallback",
            "contract_id": str(payload.get("contract_id") or ""),
            "generated_utc": _iso(generated),
            "fallback_used": True,
            "complete_snapshot_at_cutoff": False,
            "historical_proof": False,
        }

    def _upcoming_events(
        self,
        cutoff: dt.datetime,
        currencies: set[str],
        gaps: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        immutable = self._immutable_upcoming_events(cutoff, currencies, gaps)
        if immutable is not None:
            return immutable
        return self._mutable_upcoming_events(cutoff, currencies, gaps)

    def _source_health(
        self,
        cutoff: dt.datetime,
        currencies: Sequence[str],
        gaps: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        path = self.paths.source_coverage_json
        payload = _read_json(path)
        generated = _parse_utc(payload.get("generated_utc")) if payload else None
        if not payload:
            gaps.append(self._gap("source_coverage_unavailable", path))
            return {}
        if generated is None or generated > cutoff:
            gaps.append(self._gap("source_coverage_not_known_at_cutoff", path))
            return {}
        raw_currencies = payload.get("currencies") or {}
        output = {}
        for currency in currencies:
            row = raw_currencies.get(currency) if isinstance(raw_currencies, Mapping) else None
            if not isinstance(row, Mapping):
                output[currency] = {
                    "state": "missing", "as_of_utc": _iso(generated),
                    "official_source_issues": ["currency_coverage_missing"],
                }
                continue
            issues = []
            for source in row.get("sources") or []:
                if not isinstance(source, Mapping):
                    continue
                role = str(source.get("source_role") or "")
                if not bool(source.get("configured_verified")) or not bool(source.get("direct")):
                    continue
                if "policy" not in role and "statistical" not in role:
                    continue
                if not bool(source.get("healthy")):
                    issues.append(
                        {
                            "source_id": str(source.get("source_id") or ""),
                            "role": role,
                            "health_state": str(source.get("health_state") or "unknown"),
                            "runtime_status": str(source.get("runtime_status") or "unknown"),
                            "operational": bool(source.get("operational")),
                            "usable_recent_success": bool(source.get("usable_recent_success")),
                        }
                    )
            output[currency] = {
                "state": str(row.get("coverage_tier") or "unknown"),
                "as_of_utc": _iso(generated),
                "configured_source_count": int(row.get("configured_source_count") or 0),
                "operational_source_count": int(row.get("operational_source_count") or 0),
                "healthy_direct_source_count": int(row.get("healthy_direct_source_count") or 0),
                "degraded_recent_direct_source_count": int(
                    row.get("degraded_recent_direct_source_count") or 0
                ),
                "official_source_issues": issues,
            }
        return output

    def _clock_state(self, cutoff: dt.datetime, gaps: list[dict[str, Any]]) -> dict[str, Any]:
        path = self.paths.clock_integrity_json
        payload = _read_json(path)
        generated = _parse_utc(payload.get("generated_utc")) if payload else None
        if not payload:
            gaps.append(self._gap("clock_integrity_unavailable", path))
            return {"state": "unavailable", "trusted_for_prospective_evidence": False}
        if generated is None or generated > cutoff:
            gaps.append(self._gap("clock_integrity_not_known_at_cutoff", path))
            return {"state": "not_known_at_cutoff", "trusted_for_prospective_evidence": False}
        trusted = bool(payload.get("timestamp_normalization_trusted"))
        if not trusted:
            gaps.append(self._gap("clock_not_trusted_for_prospective_evidence", path))
        source = str(payload.get("source") or "")
        return {
            "state": str(payload.get("status") or "unknown"),
            "as_of_utc": _iso(generated),
            "source_artifact": source.replace("\\", "/").rsplit("/", 1)[-1],
            "trusted_for_prospective_evidence": trusted,
            "host_clock_synchronized": bool(payload.get("host_clock_synchronized")),
            "reasons": list(payload.get("reasons") or []),
        }

    def _intraday_rate_state(
        self, cutoff: dt.datetime, gaps: list[dict[str, Any]]
    ) -> dict[str, Any]:
        path = self.paths.intraday_rates_config_json
        payload = _read_json(path)
        state = str(payload.get("state") or "unavailable") if payload else "unavailable"
        known = _parse_utc(
            payload.get("generated_utc") or payload.get("created_utc")
        ) if payload else None
        claims_connected = state in {"connected", "ready", "ok"} and bool(
            payload.get("currencies")
        )
        # A positive connected state is evidence and therefore needs an exact
        # knowledge timestamp.  An undated negative placeholder remains safe.
        connected = bool(claims_connected and known is not None and known <= cutoff)
        if claims_connected and not connected:
            gaps.append(self._gap("intraday_rate_config_not_known_at_cutoff", path))
        if not connected:
            gaps.append(self._gap("intraday_rate_repricing_unavailable", path))
        return {
            "state": state,
            "connected": connected,
            "as_of_utc": _iso(known),
            "contract": dict(payload.get("contract") or {}) if payload else {},
            "blocker": str(payload.get("blocker") or "") if payload else "artifact_unavailable",
        }

    def as_of(
        self,
        decision_cutoff_utc: str | dt.datetime,
        *,
        currencies: Sequence[str] = CANONICAL_CURRENCIES,
    ) -> dict[str, Any]:
        """Return facts that were available no later than ``decision_cutoff_utc``."""

        cutoff = (
            decision_cutoff_utc.astimezone(UTC)
            if isinstance(decision_cutoff_utc, dt.datetime) and decision_cutoff_utc.tzinfo
            else _parse_utc(decision_cutoff_utc, required=True)
        )
        if cutoff is None:
            raise ValueError("a valid decision cutoff is required")
        normalized_currencies = tuple(str(value).upper() for value in currencies)
        if not normalized_currencies or len(set(normalized_currencies)) != len(normalized_currencies):
            raise ValueError("currencies must be nonempty and unique")
        unknown = sorted(set(normalized_currencies) - set(CANONICAL_CURRENCIES))
        if unknown:
            raise ValueError(f"currencies outside the canonical universe: {unknown}")

        gaps: list[dict[str, Any]] = []
        quarantined = self._quarantined_source_events(gaps)
        allowed = set(normalized_currencies)
        facts = [
            *self._macro_facts(cutoff, allowed, quarantined, gaps),
            *self._rate_facts(cutoff, allowed, gaps),
            *self._expectation_facts(cutoff, allowed, gaps),
            *self._policy_facts(cutoff, allowed, gaps),
        ]
        facts.sort(
            key=lambda row: (
                str(row.get("currency") or ""),
                str(row.get("fact_type") or ""),
                str(row.get("effective_from_utc") or ""),
                str(row.get("fact_id") or ""),
            )
        )
        # Preserve every normalized provenance row, but do not imply that
        # alternate source/release keys for the same series/reference period
        # are independent economic catalysts.  These identities are reporting
        # diagnostics only; they do not collapse or rewrite the raw facts.
        def macro_observation_identity(row: Mapping[str, Any]) -> tuple[str, ...] | None:
            if row.get("fact_type") != "official_macro_actual":
                return None
            reference = str(
                row.get("reference_period")
                or row.get("reference_date")
                or row.get("release_key")
                or row.get("fact_id")
                or ""
            )
            return (
                str(row.get("currency") or ""),
                str(row.get("series_id") or ""),
                reference,
                str(row.get("unit") or ""),
            )

        macro_fact_record_count = sum(
            row.get("fact_type") == "official_macro_actual" for row in facts
        )
        distinct_macro_observation_count = len(
            {
                identity
                for row in facts
                if (identity := macro_observation_identity(row)) is not None
            }
        )
        upcoming, event_clock_provenance = self._upcoming_events(cutoff, allowed, gaps)
        upcoming.sort(key=lambda row: (str(row.get("scheduled_utc")), str(row.get("event_id"))))
        health = self._source_health(cutoff, normalized_currencies, gaps)
        clock = self._clock_state(cutoff, gaps)
        intraday_rates = self._intraday_rate_state(cutoff, gaps)

        by_currency: dict[str, dict[str, Any]] = {}
        for currency in normalized_currencies:
            currency_facts = [row for row in facts if row["currency"] == currency]
            currency_events = [row for row in upcoming if row["currency"] == currency]
            fact_types = sorted({str(row["fact_type"]) for row in currency_facts})
            consensus_count = sum(bool(row.get("consensus_causal")) for row in currency_facts)
            currency_macro_records = [
                row
                for row in currency_facts
                if row.get("fact_type") == "official_macro_actual"
            ]
            currency_macro_observations = {
                identity
                for row in currency_macro_records
                if (identity := macro_observation_identity(row)) is not None
            }
            missing = []
            if not any(row["fact_type"] == "official_macro_actual" for row in currency_facts):
                missing.append("no_official_numeric_actual_at_cutoff")
            if not any(row["fact_type"] == "official_policy_document_context" for row in currency_facts):
                missing.append("no_policy_document_context_at_cutoff")
            if not any(row["fact_type"] == "official_daily_rate" for row in currency_facts):
                missing.append("no_daily_rate_context")
            if consensus_count == 0:
                missing.append("no_causal_pre_release_consensus")
            if not currency_events:
                missing.append("no_upcoming_event_in_current_clock_snapshot")
            source_health = health.get(currency) or {
                "state": "missing", "official_source_issues": ["source_health_unavailable"]
            }
            if source_health.get("official_source_issues"):
                missing.append("official_source_health_degraded")
            by_currency[currency] = {
                "currency": currency,
                "fact_count": len(currency_facts),
                "macro_fact_record_count": len(currency_macro_records),
                "distinct_macro_observation_count": len(currency_macro_observations),
                "fact_types": fact_types,
                "causal_consensus_count": consensus_count,
                "upcoming_event_count": len(currency_events),
                "source_health": source_health,
                "missing_or_degraded": missing,
            }

        causal_consensus_count = sum(bool(row.get("consensus_causal")) for row in facts)
        if causal_consensus_count == 0:
            gaps.append({"code": "no_causal_pre_release_consensus"})
        degraded = bool(gaps) or any(
            row.get("missing_or_degraded") for row in by_currency.values()
        )
        material = {
            "adapter_contract_id": ADAPTER_CONTRACT_ID,
            "decision_cutoff_utc": _iso(cutoff),
            "currencies": list(normalized_currencies),
            "facts": facts,
            "upcoming_events": upcoming,
            "event_clock_provenance": event_clock_provenance,
            "currency_evidence": by_currency,
            "global_gaps": gaps,
            "clock": clock,
            "intraday_rates": intraday_rates,
        }
        return {
            "schema_version": 1,
            "adapter_contract_id": ADAPTER_CONTRACT_ID,
            "snapshot_id": "official_fact_snapshot_" + _stable_hash(material)[:24],
            "decision_cutoff_utc": _iso(cutoff),
            "currency_count": len(normalized_currencies),
            "fact_count": len(facts),
            "fact_count_semantics": "normalized_provenance_records_not_independent_events",
            "macro_fact_record_count": macro_fact_record_count,
            "distinct_macro_observation_count": distinct_macro_observation_count,
            "upcoming_event_count": len(upcoming),
            "causal_consensus_count": causal_consensus_count,
            "facts": facts,
            "upcoming_events": upcoming,
            "event_clock_provenance": event_clock_provenance,
            "currency_evidence": by_currency,
            "global_gaps": gaps,
            "clock": clock,
            "intraday_rates": intraday_rates,
            "quarantined_source_event_count": len(quarantined),
            "status": "degraded" if degraded else "ready",
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "supported_execution_decision": "no_trade",
        }


__all__ = [
    "ADAPTER_CONTRACT_ID",
    "CANONICAL_CURRENCIES",
    "OfficialFactAdapter",
    "OfficialFactPaths",
]
