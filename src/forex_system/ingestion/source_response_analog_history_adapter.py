"""Point-in-time adapter from immutable FX ledgers to analog-selector rows.

The adapter is deliberately read-only and research-only.  It maps official
numeric macro releases and immutable quote-reaction samples when their exact
knowledge clocks are present.  It explicitly withholds mutable direct-response
outcomes that lack an exact outcome-known timestamp, non-causal consensus, and
daily-rate context that is not numeric intraday repricing.

SQLite inputs are opened with URI ``mode=ro`` and ``PRAGMA query_only=ON``.
The module has no broker, executor, lifecycle, authorization, or supervisor
dependency and supports only ``no_trade``.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


UTC = dt.timezone.utc
SOURCE_ROOT = Path(__file__).resolve().parents[3]
STATE_ROOT = SOURCE_ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_CONFIG = SOURCE_ROOT / "config" / "source_response_analog_history_adapter_v1.json"
DEFAULT_SELECTOR_CONTRACT = SOURCE_ROOT / "config" / "source_response_analog_selector_v1.json"


class HistoryAdapterError(ValueError):
    """Raised when the adapter's fail-closed contract is violated."""


@dataclass(frozen=True)
class HistoryAdapterPaths:
    macro_surprise_db: Path = STATE_ROOT / "macro_surprise_v1.sqlite"
    source_governance_db: Path = STATE_ROOT / "source_governance_v1.sqlite"
    direct_source_response_db: Path = STATE_ROOT / "direct_source_response_v1.sqlite"
    daily_rates_db: Path = STATE_ROOT / "official_daily_rate_context_v1.sqlite"
    movement_episode_db: Path = STATE_ROOT / "movement_news_episode_research_v1.sqlite"


def _parse_utc(value: Any, *, required: bool = False) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        if required:
            raise HistoryAdapterError("timezone-aware timestamp required")
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        if required:
            raise HistoryAdapterError(f"invalid timestamp: {text!r}") from exc
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        if required:
            raise HistoryAdapterError("timestamp must be timezone-aware")
        return None
    return parsed.astimezone(UTC)


def _iso(value: dt.datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z") if value else None


def _finite(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _rounded(value: float) -> float:
    """Normalize harmless IEEE-754 residue before hashing evidence rows."""

    return round(float(value), 10)


def load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise HistoryAdapterError(f"unable to load JSON contract {path.name}: {type(exc).__name__}") from exc
    if not isinstance(payload, Mapping):
        raise HistoryAdapterError(f"JSON contract {path.name} must contain an object")
    return dict(payload)


def validate_adapter_config(config: Mapping[str, Any]) -> None:
    if int(config.get("schema_version", 0)) != 1:
        raise HistoryAdapterError("unsupported adapter schema_version")
    if not str(config.get("adapter_contract_id") or ""):
        raise HistoryAdapterError("adapter_contract_id is required")
    if config.get("research_only") is not True:
        raise HistoryAdapterError("adapter must remain research_only")
    if config.get("execution_eligible") is not False:
        raise HistoryAdapterError("adapter must remain execution-ineligible")
    if config.get("can_place_orders") is not False:
        raise HistoryAdapterError("adapter cannot place orders")
    if config.get("supported_execution_decision") != "no_trade":
        raise HistoryAdapterError("adapter supports only no_trade")
    for field in (
        "maximum_release_rows",
        "maximum_reaction_rows",
        "maximum_direct_response_rows",
        "maximum_quarantine_rows",
        "maximum_exclusion_examples",
    ):
        value = int(config.get(field, 0))
        if value <= 0 or value > 100_000:
            raise HistoryAdapterError(f"{field} must be between 1 and 100000")


def _session_at(value: dt.datetime, config: Mapping[str, Any]) -> str:
    hour = value.astimezone(UTC).hour
    for row in config.get("utc_session_buckets") or []:
        if not isinstance(row, Mapping):
            continue
        start = int(row.get("start_hour_inclusive", -1))
        end = int(row.get("end_hour_exclusive", -1))
        if 0 <= start < end <= 24 and start <= hour < end:
            return str(row.get("session") or "off_hours")
    return "off_hours"


def _event_class(series_id: str, event_name: str, config: Mapping[str, Any]) -> str:
    haystack = f"{series_id} {event_name}".lower()
    for row in config.get("event_class_patterns") or []:
        if not isinstance(row, Mapping):
            continue
        needles = [str(value).strip().lower() for value in row.get("contains_any") or []]
        if any(needle and needle in haystack for needle in needles):
            return str(row.get("event_class") or config.get("default_event_class"))
    return str(config.get("default_event_class") or "other_official_macro_release")


def _pair_orientation(instrument: str, currency: str) -> int | None:
    parts = str(instrument).upper().split("_")
    if len(parts) != 2:
        return None
    if parts[0] == currency:
        return 1
    if parts[1] == currency:
        return -1
    return None


class SourceResponseAnalogHistoryAdapter:
    """Build a bounded selector-ready snapshot at an exact knowledge cutoff."""

    def __init__(
        self,
        *,
        paths: HistoryAdapterPaths | None = None,
        config: Mapping[str, Any] | None = None,
        selector_contract: Mapping[str, Any] | None = None,
        sqlite_timeout_sec: float = 5.0,
    ) -> None:
        self.paths = paths or HistoryAdapterPaths()
        self.config = dict(config) if config is not None else load_json_object(DEFAULT_CONFIG)
        self.selector_contract = (
            dict(selector_contract)
            if selector_contract is not None
            else load_json_object(DEFAULT_SELECTOR_CONTRACT)
        )
        validate_adapter_config(self.config)
        if str(self.selector_contract.get("selector_contract_id") or "") != str(
            self.config.get("selector_contract_id") or ""
        ):
            raise HistoryAdapterError("selector contract ID does not match adapter contract")
        self.sqlite_timeout_sec = max(0.1, min(float(sqlite_timeout_sec), 30.0))
        self._exclusion_counts: Counter[str] = Counter()
        self._exclusion_examples: list[dict[str, Any]] = []

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
        connection.execute(f"PRAGMA busy_timeout={int(self.sqlite_timeout_sec * 1000)}")
        return connection

    @staticmethod
    def _relation_exists(connection: sqlite3.Connection, name: str) -> bool:
        return connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name=? AND type IN ('table','view')",
            (name,),
        ).fetchone() is not None

    def _exclude(self, reason: str, source: str, record_id: str = "", **details: Any) -> None:
        self._exclusion_counts[reason] += 1
        limit = int(self.config["maximum_exclusion_examples"])
        if len(self._exclusion_examples) < limit:
            row: dict[str, Any] = {
                "reason": reason,
                "source": source,
                "record_id": str(record_id),
            }
            row.update({key: value for key, value in details.items() if value is not None})
            self._exclusion_examples.append(row)

    def _load_quarantines(self) -> tuple[set[str], dict[str, Any]]:
        maximum = int(self.config["maximum_quarantine_rows"])
        path = self.paths.source_governance_db
        try:
            connection = self._connect(path)
        except (FileNotFoundError, sqlite3.Error) as exc:
            return set(), {"state": "unavailable", "artifact_name": path.name, "error_class": type(exc).__name__}
        try:
            if not self._relation_exists(connection, "source_event_quarantines"):
                return set(), {"state": "relation_missing", "artifact_name": path.name}
            rows = connection.execute(
                "SELECT source_event_id FROM source_event_quarantines ORDER BY source_event_id LIMIT ?",
                (maximum + 1,),
            ).fetchall()
        finally:
            connection.close()
        truncated = len(rows) > maximum
        rows = rows[:maximum]
        return {str(row[0]) for row in rows}, {
            "state": "available",
            "artifact_name": path.name,
            "quarantined_source_event_count_loaded": len(rows),
            "bounded_read_truncated": truncated,
        }

    def _load_macro_rows(
        self, cutoff: dt.datetime
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        maximum_releases = int(self.config["maximum_release_rows"])
        maximum_reactions = int(self.config["maximum_reaction_rows"])
        path = self.paths.macro_surprise_db
        connection = self._connect(path)
        try:
            required = {
                "macro_release_revisions",
                "macro_consensus_observations",
                "macro_reaction_samples",
            }
            missing = sorted(name for name in required if not self._relation_exists(connection, name))
            if missing:
                raise HistoryAdapterError(f"macro ledger missing relations: {missing}")
            releases = connection.execute(
                """
                WITH ranked AS (
                  SELECT row_id,revision_id,release_key,source_event_id,
                         recorded_utc,causal_known_utc,scheduled_utc,event_series_id,
                         event_name,event_country,currencies_json,importance,unit,
                         actual_value,consensus_value,previous_value,
                         revised_previous_value,standardized_surprise,
                         known_before_recorded_timestamp,source_id,source_name,
                         source_url,source_verified,source_direct,payload_sha256,
                         payload_json,
                         ROW_NUMBER() OVER (
                           PARTITION BY release_key ORDER BY row_id DESC
                         ) AS rank_at_cutoff
                    FROM macro_release_revisions
                   WHERE actual_value IS NOT NULL
                     AND julianday(causal_known_utc)<=julianday(?)
                     AND julianday(recorded_utc)<=julianday(?)
                )
                SELECT * FROM ranked WHERE rank_at_cutoff=1
                 ORDER BY scheduled_utc,release_key LIMIT ?
                """,
                (_iso(cutoff), _iso(cutoff), maximum_releases + 1),
            ).fetchall()
            consensus_rows = connection.execute(
                """
                SELECT observation_id,release_key,event_series_id,scheduled_utc,
                       captured_utc,source_timestamp_utc,consensus_value,
                       source_id,source_verified,causal_valid,payload_sha256
                  FROM macro_consensus_observations
                 WHERE julianday(captured_utc)<=julianday(?)
                 ORDER BY release_key,captured_utc,observation_id LIMIT ?
                """,
                (_iso(cutoff), maximum_releases + 1),
            ).fetchall()
            reactions = connection.execute(
                """
                SELECT sample_id,release_key,instrument,target_offset_sec,
                       scheduled_utc,sampled_utc,quote_time_utc,bid,ask,mid,pip,
                       distance_from_target_sec,payload_sha256
                  FROM macro_reaction_samples
                 WHERE julianday(sampled_utc)<=julianday(?)
                 ORDER BY release_key,instrument,target_offset_sec,sampled_utc,sample_id
                 LIMIT ?
                """,
                (_iso(cutoff), maximum_reactions + 1),
            ).fetchall()
        finally:
            connection.close()

        release_truncated = len(releases) > maximum_releases
        consensus_truncated = len(consensus_rows) > maximum_releases
        reaction_truncated = len(reactions) > maximum_reactions
        release_dicts = [dict(row) for row in releases[:maximum_releases]]
        reaction_dicts = [dict(row) for row in reactions[:maximum_reactions]]

        consensus_map: dict[str, dict[str, Any]] = {}
        for raw in consensus_rows[:maximum_releases]:
            row = dict(raw)
            scheduled = _parse_utc(row.get("scheduled_utc"))
            captured = _parse_utc(row.get("captured_utc"))
            source_time = _parse_utc(row.get("source_timestamp_utc"))
            value = _finite(row.get("consensus_value"))
            valid = bool(row.get("causal_valid")) and bool(row.get("source_verified"))
            valid = bool(
                valid
                and scheduled is not None
                and captured is not None
                and source_time is not None
                and value is not None
                and captured < scheduled
                and source_time < scheduled
                and captured <= cutoff
                and source_time <= cutoff
            )
            if not valid:
                self._exclude(
                    "missing_causal_consensus_provenance",
                    "macro_consensus_observations",
                    str(row.get("observation_id") or ""),
                    release_key=str(row.get("release_key") or ""),
                )
                continue
            key = str(row.get("release_key") or "")
            previous = consensus_map.get(key)
            if previous is None or str(previous["consensus_observed_at_utc"]) < str(
                _iso(max(captured, source_time))
            ):
                consensus_map[key] = {
                    "consensus_value": value,
                    "consensus_observed_at_utc": _iso(max(captured, source_time)),
                    "consensus_source_type": "market_consensus",
                    "market_consensus": True,
                    "scheduled_utc": _iso(scheduled),
                    "event_series_id": str(row.get("event_series_id") or ""),
                    "consensus_observation_id": str(row.get("observation_id") or ""),
                    "consensus_payload_sha256": str(row.get("payload_sha256") or ""),
                }
        diagnostics = {
            "artifact_name": path.name,
            "release_rows_loaded": len(release_dicts),
            "reaction_rows_loaded": len(reaction_dicts),
            "causal_consensus_rows_loaded": len(consensus_map),
            "release_read_truncated": release_truncated,
            "consensus_read_truncated": consensus_truncated,
            "reaction_read_truncated": reaction_truncated,
        }
        return release_dicts, consensus_map, reaction_dicts, diagnostics

    def _reaction_outcomes(
        self,
        reactions: Sequence[Mapping[str, Any]],
        cutoff: dt.datetime,
    ) -> dict[tuple[str, str], list[dict[str, Any]]]:
        grouped: dict[tuple[str, str], dict[int, dict[str, Any]]] = defaultdict(dict)
        duplicate_offsets: set[tuple[tuple[str, str], int]] = set()
        for raw in reactions:
            row = dict(raw)
            key = (str(row.get("release_key") or ""), str(row.get("instrument") or "").upper())
            offset = int(row.get("target_offset_sec") or 0)
            if offset in grouped[key]:
                duplicate_offsets.add((key, offset))
                self._exclude(
                    "duplicate_reaction_offset_withheld",
                    "macro_reaction_samples",
                    str(row.get("sample_id") or ""),
                    release_key=key[0],
                    instrument=key[1],
                    target_offset_sec=offset,
                )
                continue
            grouped[key][offset] = row

        # A duplicate makes that clock ambiguous.  Retaining whichever row was
        # encountered first would turn ingestion order into an outcome choice.
        for key, offset in duplicate_offsets:
            grouped[key].pop(offset, None)

        baseline_offset = int(self.config.get("reaction_baseline_offset_sec", 0))
        minimum_offset = int(self.config.get("minimum_outcome_offset_sec", 1))
        output: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for key, by_offset in sorted(grouped.items()):
            baseline = by_offset.get(baseline_offset)
            if baseline is None:
                self._exclude(
                    "missing_reaction_baseline",
                    "macro_reaction_samples",
                    f"{key[0]}|{key[1]}",
                )
                continue
            entry_sampled = _parse_utc(baseline.get("sampled_utc"))
            entry_quote_time = _parse_utc(baseline.get("quote_time_utc"))
            entry_bid = _finite(baseline.get("bid"))
            entry_ask = _finite(baseline.get("ask"))
            entry_mid = _finite(baseline.get("mid"))
            pip = _finite(baseline.get("pip"))
            if (
                entry_sampled is None
                or entry_quote_time is None
                or max(entry_sampled, entry_quote_time) > cutoff
                or entry_bid is None
                or entry_ask is None
                or entry_mid is None
                or pip is None
                or pip <= 0.0
                or entry_ask <= entry_bid
            ):
                self._exclude(
                    "invalid_reaction_baseline_clock_or_quote",
                    "macro_reaction_samples",
                    str(baseline.get("sample_id") or ""),
                )
                continue
            for offset, row in sorted(by_offset.items()):
                if offset < minimum_offset:
                    continue
                sampled = _parse_utc(row.get("sampled_utc"))
                quote_time = _parse_utc(row.get("quote_time_utc"))
                bid = _finite(row.get("bid"))
                ask = _finite(row.get("ask"))
                mid = _finite(row.get("mid"))
                exit_pip = _finite(row.get("pip"))
                if sampled is None:
                    self._exclude(
                        "missing_exact_outcome_known_timestamp",
                        "macro_reaction_samples",
                        str(row.get("sample_id") or ""),
                    )
                    continue
                if (
                    quote_time is None
                    or max(sampled, quote_time) > cutoff
                    or bid is None
                    or ask is None
                    or mid is None
                    or exit_pip is None
                    or abs(exit_pip - pip) > 1e-12
                    or ask <= bid
                ):
                    self._exclude(
                        "invalid_outcome_clock_or_quote",
                        "macro_reaction_samples",
                        str(row.get("sample_id") or ""),
                    )
                    continue
                output[key].append(
                    {
                        "sample_id": str(row.get("sample_id") or ""),
                        "release_key": key[0],
                        "instrument": key[1],
                        "horizon_sec": offset,
                        "matured_utc": _iso(quote_time),
                        "outcome_known_utc": _iso(max(sampled, quote_time)),
                        "collector_observed_utc": _iso(sampled),
                        "entry_quote_time_utc": _iso(entry_quote_time),
                        "entry_sampled_utc": _iso(entry_sampled),
                        "entry_bid": entry_bid,
                        "entry_ask": entry_ask,
                        "entry_mid": entry_mid,
                        "exit_bid": bid,
                        "exit_ask": ask,
                        "exit_mid": mid,
                        "pip": pip,
                        "entry_spread_pips": (entry_ask - entry_bid) / pip,
                        "exit_spread_pips": (ask - bid) / pip,
                        "actual_observation_duration_sec": (quote_time - entry_quote_time).total_seconds(),
                        "target_distance_from_schedule_sec": _finite(row.get("distance_from_target_sec")),
                        "entry_payload_sha256": str(baseline.get("payload_sha256") or ""),
                        "outcome_payload_sha256": str(row.get("payload_sha256") or ""),
                        "clock_reconciliation": (
                            "outcome_known_is_max_of_collector_and_broker_quote_clock"
                        ),
                        "quote_clock_ahead_of_collector_clock": quote_time > sampled,
                        "entry_quote_clock_ahead_of_collector_clock": entry_quote_time > entry_sampled,
                        "outcome_time_provenance": "immutable_macro_reaction_sample",
                    }
                )
        return output

    def _audit_direct_response(self) -> dict[str, Any]:
        maximum = int(self.config["maximum_direct_response_rows"])
        path = self.paths.direct_source_response_db
        try:
            connection = self._connect(path)
        except (FileNotFoundError, sqlite3.Error) as exc:
            return {"state": "unavailable", "artifact_name": path.name, "error_class": type(exc).__name__}
        try:
            if not self._relation_exists(connection, "response_targets"):
                return {"state": "relation_missing", "artifact_name": path.name}
            columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(response_targets)")
            }
            rows = connection.execute(
                """
                SELECT target_id,status,outcome_quote_time,source_observation_id,
                       horizon_sec,instrument
                  FROM response_targets ORDER BY target_id LIMIT ?
                """,
                (maximum + 1,),
            ).fetchall()
        finally:
            connection.close()
        truncated = len(rows) > maximum
        rows = rows[:maximum]
        status_counts = Counter(str(row[1] or "unknown") for row in rows)
        exact_field_present = "outcome_known_utc" in columns
        withheld = 0
        for row in rows:
            if str(row[1]) == "matured" and row[2] and not exact_field_present:
                withheld += 1
                self._exclude(
                    "missing_exact_outcome_known_timestamp",
                    "direct_source_response.response_targets",
                    str(row[0]),
                    instrument=str(row[5] or ""),
                    horizon_sec=int(row[4] or 0),
                )
        return {
            "state": "available",
            "artifact_name": path.name,
            "rows_loaded": len(rows),
            "bounded_read_truncated": truncated,
            "status_counts": dict(sorted(status_counts.items())),
            "outcome_known_timestamp_column_present": exact_field_present,
            "mapped_outcome_count": 0,
            "withheld_matured_outcome_count": withheld,
            "mapping_policy": "withhold_without_exact_outcome_known_timestamp",
        }

    def _audit_daily_rates(self) -> dict[str, Any]:
        path = self.paths.daily_rates_db
        try:
            connection = self._connect(path)
        except (FileNotFoundError, sqlite3.Error) as exc:
            return {"state": "unavailable", "artifact_name": path.name, "error_class": type(exc).__name__}
        try:
            if not self._relation_exists(connection, "daily_rate_observations"):
                return {"state": "relation_missing", "artifact_name": path.name}
            row = connection.execute(
                "SELECT COUNT(*),COUNT(DISTINCT currency),SUM(prospective_eligible) FROM daily_rate_observations"
            ).fetchone()
        finally:
            connection.close()
        return {
            "state": "available_context_only",
            "artifact_name": path.name,
            "row_count": int(row[0] or 0),
            "currency_count": int(row[1] or 0),
            "prospective_daily_row_count": int(row[2] or 0),
            "numeric_intraday_repricing_available": False,
            "mapping_policy": "daily_rate_context_is_not_intraday_rate_repricing",
        }

    def _audit_movement_episodes(self) -> dict[str, Any]:
        """Reject outcome-selected episodes as an analog evidence universe."""

        path = self.paths.movement_episode_db
        try:
            connection = self._connect(path)
        except (FileNotFoundError, sqlite3.Error) as exc:
            return {
                "state": "unavailable",
                "artifact_name": path.name,
                "error_class": type(exc).__name__,
            }
        try:
            if not self._relation_exists(connection, "movement_episodes"):
                return {"state": "relation_missing", "artifact_name": path.name}
            episode_count = int(
                connection.execute("SELECT COUNT(*) FROM movement_episodes").fetchone()[0]
            )
            causal_links = 0
            if self._relation_exists(connection, "episode_source_links"):
                causal_links = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM episode_source_links WHERE causal_entry_eligible=1"
                    ).fetchone()[0]
                )
        finally:
            connection.close()
        if episode_count:
            self._exclusion_counts[
                "outcome_selected_movement_episode_not_admissible"
            ] += episode_count
            self._exclude(
                "outcome_selected_movement_episode_universe_withheld",
                "movement_news_episode_research.movement_episodes",
                "aggregate",
                episode_count=episode_count,
            )
        return {
            "state": "available_but_outcome_selected",
            "artifact_name": path.name,
            "movement_episode_count": episode_count,
            "causal_source_link_count": causal_links,
            "mapped_outcome_count": 0,
            "mapping_policy": "withhold_outcome_selected_candidate_universe",
        }

    def _events_from_releases(
        self,
        releases: Iterable[Mapping[str, Any]],
        consensus_map: Mapping[str, Mapping[str, Any]],
        reaction_map: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
        quarantined: set[str],
        cutoff: dt.datetime,
    ) -> list[dict[str, Any]]:
        currencies = {str(value).upper() for value in self.config.get("canonical_currencies") or []}
        events: list[dict[str, Any]] = []
        for raw in releases:
            row = dict(raw)
            release_key = str(row.get("release_key") or "")
            revision_id = str(row.get("revision_id") or "")
            source_event_id = str(row.get("source_event_id") or "")
            if source_event_id in quarantined:
                self._exclude("quarantined_source_event", "macro_release_revisions", revision_id)
                continue
            payload = _json_object(row.get("payload_json"))
            if bool(payload.get("source_listing_bootstrap")):
                self._exclude("bootstrap_context_not_prospective", "macro_release_revisions", revision_id)
                continue
            if bool(payload.get("published_time_inferred")):
                self._exclude("inferred_publication_clock_not_prospective", "macro_release_revisions", revision_id)
                continue
            if not bool(row.get("source_verified")) or not bool(row.get("source_direct")):
                self._exclude("source_not_both_direct_and_verified", "macro_release_revisions", revision_id)
                continue
            scheduled = _parse_utc(row.get("scheduled_utc"))
            causal_known = _parse_utc(row.get("causal_known_utc"))
            recorded = _parse_utc(row.get("recorded_utc"))
            actual = _finite(row.get("actual_value"))
            if scheduled is None:
                self._exclude("missing_scheduled_release_timestamp", "macro_release_revisions", revision_id)
                continue
            if causal_known is None or recorded is None:
                self._exclude("missing_exact_feature_known_timestamp", "macro_release_revisions", revision_id)
                continue
            feature_known = max(causal_known, recorded)
            if feature_known > cutoff:
                self._exclude("feature_known_after_cutoff", "macro_release_revisions", revision_id)
                continue
            if actual is None or feature_known < scheduled:
                self._exclude("actual_not_causally_known_after_release", "macro_release_revisions", revision_id)
                continue
            affected = sorted({str(value).upper() for value in _json_list(row.get("currencies_json"))})
            affected = [currency for currency in affected if currency in currencies]
            if not affected:
                self._exclude("missing_supported_currency", "macro_release_revisions", revision_id)
                continue

            series_id = str(row.get("event_series_id") or "")
            event_name = str(row.get("event_name") or "")
            consensus = dict(consensus_map.get(release_key) or {})
            raw_revision_consensus = _finite(row.get("consensus_value"))
            degradation = ["missing_numeric_intraday_rate_repricing"]
            consensus_schedule = _parse_utc(consensus.get("scheduled_utc"))
            consensus_series = str(consensus.get("event_series_id") or "")
            if consensus and (
                consensus_schedule != scheduled
                or (consensus_series and series_id and consensus_series != series_id)
            ):
                self._exclude(
                    "causal_consensus_release_identity_mismatch",
                    "macro_consensus_observations",
                    str(consensus.get("consensus_observation_id") or ""),
                    release_key=release_key,
                )
                consensus = {}
                degradation.append("causal_consensus_release_identity_mismatch")
            if not consensus:
                degradation.append("missing_causal_consensus_provenance")
                if raw_revision_consensus is not None:
                    degradation.append("noncausal_revision_consensus_withheld")
            if _finite(row.get("standardized_surprise")) is not None:
                degradation.append("noncausal_standardization_withheld")
            for currency in affected:
                outcomes: list[dict[str, Any]] = []
                for (reaction_release, instrument), rows in reaction_map.items():
                    if reaction_release != release_key:
                        continue
                    orientation = _pair_orientation(instrument, currency)
                    if orientation is None:
                        continue
                    for outcome_raw in rows:
                        outcome = dict(outcome_raw)
                        pip = float(outcome["pip"])
                        entry_bid = float(outcome["entry_bid"])
                        entry_ask = float(outcome["entry_ask"])
                        entry_mid = float(outcome["entry_mid"])
                        exit_bid = float(outcome["exit_bid"])
                        exit_ask = float(outcome["exit_ask"])
                        exit_mid = float(outcome["exit_mid"])
                        currency_mid = _rounded(orientation * (exit_mid - entry_mid) / pip)
                        if orientation == 1:
                            strengthen_net = _rounded((exit_bid - entry_ask) / pip)
                            weaken_net = _rounded((entry_bid - exit_ask) / pip)
                        else:
                            strengthen_net = _rounded((entry_bid - exit_ask) / pip)
                            weaken_net = _rounded((exit_bid - entry_ask) / pip)
                        outcome.update(
                            {
                                "outcome_id": f"{outcome['sample_id']}:{currency}",
                                "currency": currency,
                                "currency_orientation": orientation,
                                "currency_mid_return_pips": currency_mid,
                                "strengthening_after_cost_pips": strengthen_net,
                                "weakening_after_cost_pips": weaken_net,
                                "best_after_cost_pips": max(strengthen_net, weaken_net),
                                "movement_cleared_cost": max(strengthen_net, weaken_net) > 0.0,
                                "direction_policy": "abstain",
                                "research_only": True,
                                "execution_eligible": False,
                            }
                        )
                        outcomes.append(outcome)
                outcomes.sort(
                    key=lambda outcome: (
                        int(outcome["horizon_sec"]),
                        str(outcome["instrument"]),
                        str(outcome["outcome_id"]),
                    )
                )
                event = {
                    "event_id": f"official_macro:{release_key}:{currency}",
                    "event_time_utc": _iso(scheduled),
                    "feature_known_utc": _iso(feature_known),
                    "scheduled_release_time_utc": _iso(scheduled),
                    "actual_known_utc": _iso(feature_known),
                    "currency": currency,
                    "event_series_id": series_id,
                    "event_class": _event_class(series_id, event_name, self.config),
                    "source_family": str(self.config.get("default_source_family")),
                    "policy_regime": None,
                    "session": _session_at(scheduled, self.config),
                    "liquidity_bucket": None,
                    "initial_actual_value": actual,
                    "previous_value": _finite(row.get("previous_value")),
                    "revised_previous_value": _finite(row.get("revised_previous_value")),
                    "consensus_value": consensus.get("consensus_value"),
                    "consensus_observed_at_utc": consensus.get("consensus_observed_at_utc"),
                    "consensus_source_type": consensus.get("consensus_source_type"),
                    "market_consensus": bool(consensus.get("market_consensus")),
                    "standardized_surprise": None,
                    "standardized_surprise_known_utc": None,
                    "surprise_scale_known_utc": None,
                    "internal_expectation_error_standardized": None,
                    "internal_expectation_error_known_utc": None,
                    "internal_expectation_scale_known_utc": None,
                    "statement_delta_score": None,
                    "rates_state": "missing",
                    "rates_known_utc": None,
                    "rates_repricing_bps": None,
                    "pre_event_currency_return_bps": None,
                    "pre_event_volatility_percentile": None,
                    "spread_percentile": None,
                    "liquidity_percentile": None,
                    "release_key": release_key,
                    "revision_id": revision_id,
                    "source_event_id": source_event_id,
                    "source_id": str(row.get("source_id") or ""),
                    "source_name": str(row.get("source_name") or ""),
                    "source_url": str(row.get("source_url") or ""),
                    "source_payload_sha256": str(row.get("payload_sha256") or ""),
                    "consensus_observation_id": consensus.get("consensus_observation_id"),
                    "degradation_reasons": sorted(set(degradation)),
                    "outcomes": outcomes,
                    "direction_policy": "abstain",
                    "research_only": True,
                    "execution_eligible": False,
                    "can_place_orders": False,
                    "supported_execution_decision": "no_trade",
                }
                events.append(event)
        events.sort(key=lambda row: (str(row["event_time_utc"]), str(row["event_id"])))
        return events

    def build_snapshot(self, *, decision_cutoff_utc: str | dt.datetime) -> dict[str, Any]:
        self._exclusion_counts.clear()
        self._exclusion_examples.clear()
        cutoff = (
            decision_cutoff_utc.astimezone(UTC)
            if isinstance(decision_cutoff_utc, dt.datetime) and decision_cutoff_utc.tzinfo
            else _parse_utc(decision_cutoff_utc, required=True)
        )
        if cutoff is None:
            raise HistoryAdapterError("decision cutoff is required")
        quarantined, quarantine_state = self._load_quarantines()
        releases, consensus, reactions, macro_state = self._load_macro_rows(cutoff)
        reaction_map = self._reaction_outcomes(reactions, cutoff)
        events = self._events_from_releases(
            releases, consensus, reaction_map, quarantined, cutoff
        )
        direct_state = self._audit_direct_response()
        daily_rates_state = self._audit_daily_rates()
        movement_episode_state = self._audit_movement_episodes()

        mapped_outcomes = sum(len(event.get("outcomes") or []) for event in events)
        events_with_outcomes = sum(bool(event.get("outcomes")) for event in events)
        events_with_causal_consensus = sum(bool(event.get("market_consensus")) for event in events)
        degradation_counts: Counter[str] = Counter()
        for event in events:
            degradation_counts.update(str(value) for value in event.get("degradation_reasons") or [])
        coverage = {
            "mapped_event_count": len(events),
            "mapped_event_with_outcome_count": events_with_outcomes,
            "mapped_outcome_count": mapped_outcomes,
            "mapped_event_with_causal_consensus_count": events_with_causal_consensus,
            "mapped_event_with_numeric_intraday_rate_repricing_count": 0,
            "excluded_count_by_reason": dict(sorted(self._exclusion_counts.items())),
            "mapped_event_degradation_count_by_reason": dict(sorted(degradation_counts.items())),
        }
        identity = {
            "adapter_contract_id": self.config["adapter_contract_id"],
            "adapter_contract_fingerprint": _stable_hash(self.config),
            "selector_contract_id": self.selector_contract["selector_contract_id"],
            "selector_contract_fingerprint": _stable_hash(self.selector_contract),
            "decision_cutoff_utc": _iso(cutoff),
            "event_hashes": [
                _stable_hash(event) for event in events
            ],
            "coverage": coverage,
        }
        snapshot = {
            "schema_version": 1,
            "adapter_contract_id": self.config["adapter_contract_id"],
            "adapter_contract_fingerprint": identity["adapter_contract_fingerprint"],
            "selector_contract_id": self.selector_contract["selector_contract_id"],
            "selector_contract_fingerprint": identity["selector_contract_fingerprint"],
            "decision_cutoff_utc": _iso(cutoff),
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "supported_execution_decision": "no_trade",
            "direction_policy": "abstain",
            "coverage": coverage,
            "input_states": {
                "macro_surprise": macro_state,
                "source_quarantines": quarantine_state,
                "direct_source_response": direct_state,
                "daily_rates": daily_rates_state,
                "movement_episodes": movement_episode_state,
                "intraday_rate_repricing": {
                    "state": "unavailable",
                    "numeric_intraday_rate_repricing_available": False,
                    "reason": "missing_numeric_intraday_rate_repricing",
                },
            },
            "exclusion_examples": sorted(
                self._exclusion_examples,
                key=lambda row: (str(row.get("reason")), str(row.get("source")), str(row.get("record_id"))),
            ),
            "events": events,
        }
        snapshot["snapshot_id"] = "source_response_history_" + _stable_hash(identity)[:24]
        snapshot["snapshot_sha256"] = _stable_hash(snapshot)
        return snapshot


__all__ = [
    "HistoryAdapterError",
    "HistoryAdapterPaths",
    "SourceResponseAnalogHistoryAdapter",
    "load_json_object",
    "validate_adapter_config",
]
