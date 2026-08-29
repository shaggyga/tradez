#!/usr/bin/env python3
"""Low-latency practice-007 executor for the consolidated signal feed.

The strategy lab remains responsible for research, feature construction, and
publishing candidates.  This process only reads the shared candidate feed,
refreshes quotes, applies the existing frozen execution policy and validation
gates, and manages trades owned by the strategy-lab execution tag.

Order writes remain restricted by the imported PracticeExecutor to the OANDA
practice endpoint.  This process is never a real-money router.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import oanda_practice_shadow_strategy_lab as lab
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad import oanda_practice_shadow_strategy_lab as lab


SIGNAL_FEED_WAL_AUTOCHECKPOINT_FRAMES = 0
EXECUTION_SIGNAL_CONTRACT_SCHEMA = "practice_execution_aggregate_signal_contract_v1"
INDEPENDENT_VERIFIER_PUBLICATION_CONTRACT = (
    "independent_evidence_verifier_publication_v2"
)


def independent_verifier_guard_path(state_path: Path) -> Path:
    return state_path.with_name(f"{state_path.stem}_guard_v2.sqlite")


def bind_execution_signal_contract(candidate: dict[str, Any]) -> dict[str, Any]:
    """Bind an execution-only ID to the exact aggregate decision contract.

    The strategy laboratory deliberately retains the representative component
    forecast ID when it aggregates all contributors for an instrument.  The
    representative can remain unchanged while the preferred aggregate
    direction, horizon, or contributor generation changes.  That identity is
    useful for research lineage, but it is too weak for a one-time canary
    authorization.  This execution-local binding leaves the frozen producer
    and proof-cohort model versions untouched while making such changes create
    a different signal ID at the final routing boundary.
    """
    bound = dict(candidate)
    representative_id = str(
        candidate.get("representative_component_id")
        or candidate.get("id")
        or ""
    )
    horizon = int(
        lab.safe_float(
            candidate.get("execution_horizon_sec")
            or candidate.get("preferred_horizon_sec")
        )
    )
    preferred_contributors: list[dict[str, Any]] = []
    for point in candidate.get("horizon_breakdown") or []:
        if not isinstance(point, dict):
            continue
        if int(lab.safe_float(point.get("horizon_sec"))) != horizon:
            continue
        for contributor in point.get("contributors") or []:
            if not isinstance(contributor, dict):
                continue
            preferred_contributors.append(
                {
                    "family": str(contributor.get("family") or ""),
                    "model_id": str(contributor.get("model_id") or ""),
                    "lane_id": str(contributor.get("lane_id") or ""),
                    "input_timeframe": str(
                        contributor.get("input_timeframe") or ""
                    ),
                    "direction": str(contributor.get("direction") or ""),
                    "signal_role": str(contributor.get("signal_role") or ""),
                    "source_kind": str(contributor.get("source_kind") or ""),
                    "feed_source": str(contributor.get("feed_source") or ""),
                    "forecast_generated_epoch": contributor.get(
                        "forecast_generated_epoch"
                    ),
                }
            )
        break
    preferred_contributors.sort(
        key=lambda row: json.dumps(
            row, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
    )
    contract = {
        "schema": EXECUTION_SIGNAL_CONTRACT_SCHEMA,
        "instrument": str(candidate.get("instrument") or ""),
        "direction": str(candidate.get("direction") or ""),
        "execution_horizon_sec": horizon,
        "representative_component_id": representative_id,
        "contributors": preferred_contributors,
    }
    canonical = json.dumps(
        contract, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    bound["representative_component_id"] = representative_id
    bound["execution_signal_contract_schema"] = (
        EXECUTION_SIGNAL_CONTRACT_SCHEMA
    )
    bound["execution_signal_contract_id"] = (
        "exec_signal_" + hashlib.sha256(canonical).hexdigest()[:40]
    )
    bound["id"] = bound["execution_signal_contract_id"]
    return bound


def canary_payload_signature(payload: dict[str, Any], key: str | bytes) -> str:
    """Return the canonical HMAC for a canary payload (signature excluded)."""
    unsigned = dict(payload)
    unsigned.pop("signature_hmac_sha256", None)
    canonical = json.dumps(
        unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    secret = key.encode("utf-8") if isinstance(key, str) else key
    return "sha256:" + hmac.new(secret, canonical, hashlib.sha256).hexdigest()


class GovernedCanaryAuthorizer:
    """Fail-closed final-entry authorization for frozen Practice-007 canaries.

    Qualification in the legacy signal matrix is deliberately insufficient.
    A routed signal must match an unexpired authorization for the exact signal,
    proof cohort, and allocator cohort.  This object has no broker dependency
    and cannot submit or close an order.
    """

    def __init__(
        self,
        path: Path,
        *,
        lifecycle_database: Path,
        independent_verifier_state: Path | None = None,
        independent_verifier_guard: Path | None = None,
        consumption_database: Path | None = None,
        hmac_key: str | None = None,
        maximum_age_sec: float = 900.0,
    ) -> None:
        self.path = Path(path)
        self.lifecycle_database = Path(lifecycle_database)
        self.independent_verifier_state = (
            Path(independent_verifier_state)
            if independent_verifier_state is not None
            else self.path.with_name("independent_evidence_verifier_v1.json")
        )
        self.independent_verifier_guard = (
            Path(independent_verifier_guard)
            if independent_verifier_guard is not None
            else independent_verifier_guard_path(self.independent_verifier_state)
        )
        self.consumption_database = (
            Path(consumption_database)
            if consumption_database is not None
            else self.path.with_name("practice_007_canary_consumptions_v1.sqlite")
        )
        self.hmac_key = hmac_key if hmac_key is not None else os.environ.get("FOREX_CANARY_HMAC_KEY", "")
        self.maximum_age_sec = max(1.0, float(maximum_age_sec))
        self._cached_at_monotonic = -math.inf
        self._cached_payload: dict[str, Any] | None = None
        self._cached_error = "not_loaded"
        self.last_decision: dict[str, Any] = {
            "allowed": False,
            "reason": "not_evaluated",
        }

    def _independent_verification(
        self, hypothesis_id: str, proof_cohort_id: str
    ) -> tuple[bool, str]:
        def guard_snapshot() -> dict[str, Any]:
            connection = sqlite3.connect(
                f"file:{self.independent_verifier_guard.resolve().as_posix()}?mode=ro",
                uri=True,
                timeout=3.0,
            )
            connection.row_factory = sqlite3.Row
            try:
                connection.execute("PRAGMA query_only=ON")
                row = connection.execute(
                    """
                    SELECT generation,pass_id,owner_id,phase,status,
                           authorization_safe,completed_utc,state_sha256
                    FROM verifier_publication_guard WHERE singleton=1
                    """
                ).fetchone()
            finally:
                connection.close()
            return dict(row) if row is not None else {}

        try:
            first_guard = guard_snapshot()
        except sqlite3.OperationalError as exc:
            if "unable to open database file" in str(exc).lower():
                return False, "independent_verifier_guard_missing"
            return False, "independent_verifier_guard_unreadable"
        except sqlite3.Error:
            return False, "independent_verifier_guard_unreadable"
        if not first_guard:
            return False, "independent_verifier_guard_invalid"
        if (
            first_guard.get("phase") != "completed"
            or first_guard.get("status") != "match"
            or int(first_guard.get("authorization_safe") or 0) != 1
        ):
            return False, "independent_verifier_guard_not_safe"
        try:
            state_bytes = self.independent_verifier_state.read_bytes()
            payload = json.loads(state_bytes.decode("utf-8"))
        except FileNotFoundError:
            return False, "independent_verifier_missing"
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return False, "independent_verifier_invalid"
        completed_epoch = self._timestamp_epoch(
            payload.get("completed_verification_utc")
        )
        completed_age_sec = (
            math.inf if completed_epoch is None else time.time() - completed_epoch
        )
        if completed_age_sec < -60.0 or completed_age_sec > self.maximum_age_sec:
            return False, "independent_verifier_stale"
        if (
            int(lab.safe_float(payload.get("schema_version"), 0.0)) < 2
            or payload.get("publication_contract")
            != INDEPENDENT_VERIFIER_PUBLICATION_CONTRACT
        ):
            return False, "independent_verifier_publication_contract_invalid"
        if payload.get("status") != "match" or payload.get("authorization_safe") is not True:
            return False, "independent_verifier_mismatch"
        state_sha256 = "sha256:" + hashlib.sha256(state_bytes).hexdigest()
        identity_matches = (
            int(lab.safe_float(payload.get("publication_generation"), -1.0))
            == int(first_guard.get("generation") or -2)
            and str(payload.get("verification_pass_id") or "")
            == str(first_guard.get("pass_id") or "")
            and str(payload.get("verification_owner_id") or "")
            == str(first_guard.get("owner_id") or "")
            and str(payload.get("completed_verification_utc") or "")
            == str(first_guard.get("completed_utc") or "")
            and state_sha256 == str(first_guard.get("state_sha256") or "")
        )
        if not identity_matches:
            return False, "independent_verifier_guard_state_mismatch"
        try:
            second_guard = guard_snapshot()
        except sqlite3.Error:
            return False, "independent_verifier_guard_unreadable"
        if second_guard != first_guard:
            return False, "independent_verifier_guard_changed_during_read"
        verified = payload.get("verified_confirmed_candidates")
        if not isinstance(verified, list):
            return False, "independent_verifier_candidates_invalid"
        for row in verified:
            if not isinstance(row, dict):
                continue
            if (
                str(row.get("governed_hypothesis_id") or "") == hypothesis_id
                and str(row.get("proof_cohort_id") or "") == proof_cohort_id
            ):
                return True, "independently_verified_confirmed_candidate"
        return False, "candidate_not_independently_verified"

    def _consume_once(
        self,
        *,
        nonce: str,
        authorization_id: str,
        account_id: str,
        signal_id: str,
    ) -> tuple[bool, str]:
        if not nonce or not authorization_id:
            return False, "authorization_nonce_or_id_missing"
        self.consumption_database.parent.mkdir(parents=True, exist_ok=True)
        try:
            connection = sqlite3.connect(self.consumption_database, timeout=5.0)
            try:
                connection.execute("PRAGMA busy_timeout=5000")
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS canary_authorization_consumptions(
                        one_time_nonce TEXT PRIMARY KEY,
                        authorization_id TEXT NOT NULL,
                        account_id TEXT NOT NULL,
                        signal_id TEXT NOT NULL,
                        consumed_utc TEXT NOT NULL
                    );
                    CREATE TRIGGER IF NOT EXISTS canary_consumptions_no_update
                    BEFORE UPDATE ON canary_authorization_consumptions
                    BEGIN SELECT RAISE(ABORT,'append_only'); END;
                    CREATE TRIGGER IF NOT EXISTS canary_consumptions_no_delete
                    BEFORE DELETE ON canary_authorization_consumptions
                    BEGIN SELECT RAISE(ABORT,'append_only'); END;
                    """
                )
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO canary_authorization_consumptions VALUES (?,?,?,?,?)",
                    (
                        nonce,
                        authorization_id,
                        account_id,
                        signal_id,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                connection.commit()
            finally:
                connection.close()
        except sqlite3.IntegrityError:
            return False, "authorization_nonce_already_consumed"
        except sqlite3.Error:
            return False, "authorization_consumption_database_error"
        return True, "authorization_nonce_consumed"

    def _confirmed_lifecycle_state(
        self,
        hypothesis_id: str,
        proof_cohort_id: str,
    ) -> tuple[bool, str]:
        if not hypothesis_id:
            return False, "governed_hypothesis_id_missing"
        if not self.lifecycle_database.is_file():
            return False, "lifecycle_database_missing"
        try:
            connection = sqlite3.connect(
                f"file:{self.lifecycle_database.resolve().as_posix()}?mode=ro",
                uri=True,
                timeout=3.0,
            )
            try:
                row = connection.execute(
                    """
                    SELECT event.next_state, hypothesis.cohort_id
                    FROM hypotheses AS hypothesis
                    JOIN lifecycle_events AS event
                      ON event.hypothesis_id = hypothesis.hypothesis_id
                    WHERE hypothesis.hypothesis_id = ?
                    ORDER BY event.rowid DESC
                    LIMIT 1
                    """,
                    (hypothesis_id,),
                ).fetchone()
            finally:
                connection.close()
        except sqlite3.Error:
            return False, "lifecycle_database_unreadable"
        if row is None:
            return False, "governed_hypothesis_not_found"
        if str(row[0] or "") != "confirmed_candidate":
            return False, "governed_hypothesis_not_confirmed"
        if str(row[1] or "") != proof_cohort_id:
            return False, "governed_hypothesis_cohort_mismatch"
        return True, "confirmed_candidate"

    @staticmethod
    def _timestamp_epoch(value: Any) -> float | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None

    def _load(self) -> tuple[dict[str, Any] | None, str]:
        now_monotonic = time.monotonic()
        if now_monotonic - self._cached_at_monotonic < 1.0:
            return self._cached_payload, self._cached_error
        self._cached_at_monotonic = now_monotonic
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            self._cached_payload = None
            self._cached_error = "authorization_file_missing"
            return None, self._cached_error
        except (OSError, json.JSONDecodeError):
            self._cached_payload = None
            self._cached_error = "authorization_file_invalid"
            return None, self._cached_error
        if not isinstance(payload, dict):
            self._cached_payload = None
            self._cached_error = "authorization_payload_invalid"
            return None, self._cached_error
        generated_epoch = self._timestamp_epoch(payload.get("generated_utc"))
        if generated_epoch is None:
            self._cached_payload = None
            self._cached_error = "authorization_timestamp_invalid"
            return None, self._cached_error
        age_sec = time.time() - generated_epoch
        if age_sec < -60.0 or age_sec > self.maximum_age_sec:
            self._cached_payload = None
            self._cached_error = "authorization_stale"
            return None, self._cached_error
        self._cached_payload = payload
        self._cached_error = ""
        return payload, ""

    def authorize(
        self,
        candidate: dict[str, Any],
        *,
        account_id: str,
    ) -> dict[str, Any]:
        payload, error = self._load()
        decision: dict[str, Any] = {
            "allowed": False,
            "reason": error or "governed_canary_not_authorized",
            "candidate_id": str(candidate.get("id") or ""),
            "proof_cohort_id": str(candidate.get("proof_cohort_id") or ""),
            "governed_hypothesis_id": str(
                candidate.get("governed_hypothesis_id") or ""
            ),
            "allocator_cohort_id": str(
                candidate.get("allocator_cohort_id") or ""
            ),
        }
        if payload is None:
            self.last_decision = decision
            return decision
        if str(payload.get("environment") or "").lower() != "practice":
            decision["reason"] = "authorization_environment_not_practice"
            self.last_decision = decision
            return decision
        if str(payload.get("account_suffix") or "") != account_id[-4:]:
            decision["reason"] = "authorization_account_mismatch"
            self.last_decision = decision
            return decision
        if not bool(payload.get("entry_authorized")):
            decision["reason"] = str(
                payload.get("reason") or "governed_canary_entries_disabled"
            )
            self.last_decision = decision
            return decision
        if int(lab.safe_float(payload.get("schema_version"), 0.0)) < 2:
            decision["reason"] = "signed_authorization_schema_required"
            self.last_decision = decision
            return decision
        if str(payload.get("account_id") or "") != account_id:
            decision["reason"] = "authorization_full_account_mismatch"
            self.last_decision = decision
            return decision
        if not self.hmac_key:
            decision["reason"] = "authorization_hmac_key_missing"
            self.last_decision = decision
            return decision
        expected_signature = canary_payload_signature(payload, self.hmac_key)
        if not hmac.compare_digest(
            str(payload.get("signature_hmac_sha256") or ""), expected_signature
        ):
            decision["reason"] = "authorization_hmac_invalid"
            self.last_decision = decision
            return decision
        entries = payload.get("authorized_entries")
        if not isinstance(entries, list):
            decision["reason"] = "authorization_entries_invalid"
            self.last_decision = decision
            return decision
        now_epoch = time.time()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("signal_id") or "") != decision["candidate_id"]:
                continue
            if str(entry.get("proof_cohort_id") or "") != decision["proof_cohort_id"]:
                continue
            if str(entry.get("governed_hypothesis_id") or "") != decision["governed_hypothesis_id"]:
                continue
            if str(entry.get("allocator_cohort_id") or "") != decision["allocator_cohort_id"]:
                continue
            if str(entry.get("account_id") or "") != account_id:
                decision["reason"] = "candidate_authorization_account_mismatch"
                continue
            if str(entry.get("allowed_instrument") or "") != str(candidate.get("instrument") or ""):
                decision["reason"] = "candidate_authorization_instrument_mismatch"
                continue
            if str(entry.get("allowed_direction") or "").lower() != str(candidate.get("direction") or "").lower():
                decision["reason"] = "candidate_authorization_direction_mismatch"
                continue
            issued_epoch = self._timestamp_epoch(entry.get("issued_at"))
            expires_epoch = self._timestamp_epoch(entry.get("expires_at"))
            if issued_epoch is None or expires_epoch is None or issued_epoch > now_epoch + 60.0:
                decision["reason"] = "candidate_authorization_timestamp_invalid"
                continue
            if expires_epoch <= now_epoch:
                decision["reason"] = "candidate_authorization_expired"
                continue
            if not bool(entry.get("confirmed_candidate")):
                decision["reason"] = "candidate_not_confirmed"
                continue
            maximum_units = int(lab.safe_float(entry.get("maximum_units"), 0.0))
            maximum_notional = lab.safe_float(entry.get("maximum_notional"), 0.0)
            maximum_total_exposure = lab.safe_float(entry.get("maximum_total_exposure"), 0.0)
            if maximum_units <= 0 or maximum_notional <= 0.0 or maximum_total_exposure <= 0.0:
                decision["reason"] = "candidate_authorization_limits_invalid"
                continue
            lifecycle_confirmed, lifecycle_reason = (
                self._confirmed_lifecycle_state(
                    decision["governed_hypothesis_id"],
                    decision["proof_cohort_id"],
                )
            )
            if not lifecycle_confirmed:
                decision["reason"] = lifecycle_reason
                continue
            independently_confirmed, independent_reason = self._independent_verification(
                decision["governed_hypothesis_id"], decision["proof_cohort_id"]
            )
            if not independently_confirmed:
                decision["reason"] = independent_reason
                continue
            consumed, consumption_reason = self._consume_once(
                nonce=str(entry.get("one_time_nonce") or ""),
                authorization_id=str(entry.get("authorization_id") or ""),
                account_id=account_id,
                signal_id=decision["candidate_id"],
            )
            if not consumed:
                decision["reason"] = consumption_reason
                continue
            decision.update(
                {
                    "allowed": True,
                    "reason": "confirmed_canary_authorized",
                    "authorization_id": str(entry.get("authorization_id") or ""),
                    "one_time_nonce": str(entry.get("one_time_nonce") or ""),
                    "expires_epoch": expires_epoch,
                    "maximum_units": maximum_units,
                    "maximum_notional": maximum_notional,
                    "maximum_total_exposure": maximum_total_exposure,
                }
            )
            break
        self.last_decision = decision
        return decision

    def summary(self, *, account_id: str) -> dict[str, Any]:
        payload, error = self._load()
        entries = (
            payload.get("authorized_entries")
            if isinstance(payload, dict)
            and isinstance(payload.get("authorized_entries"), list)
            else []
        )
        return {
            "configured": True,
            "fail_closed": True,
            "authorization_file": str(self.path.resolve()),
            "lifecycle_database": str(self.lifecycle_database.resolve()),
            "independent_verifier_state": str(self.independent_verifier_state.resolve()),
            "independent_verifier_guard": str(self.independent_verifier_guard.resolve()),
            "consumption_database": str(self.consumption_database.resolve()),
            "signed_entry_schema_required": 2,
            "authorization_fresh": payload is not None,
            "entry_authorized": bool(
                payload
                and payload.get("entry_authorized")
                and str(payload.get("environment") or "").lower() == "practice"
                and str(payload.get("account_suffix") or "") == account_id[-4:]
            ),
            "authorized_entry_count": len(entries),
            "state_reason": error or str(payload.get("reason") or ""),
            "last_decision": dict(self.last_decision),
        }


class GovernedPracticeExecutor(lab.PracticeExecutor):
    """Practice executor whose final order call is governed by an exact canary."""

    @staticmethod
    def owns_trade(trade: dict[str, Any]) -> bool:
        """Manage governed trades, never the isolated news challenger.

        Both lanes deliberately retain the broker tag used by the common
        PracticeExecutor so OANDA history remains easy to audit.  The lane
        prefix in the immutable trade comment is the ownership boundary.
        Without this exclusion the governed worker could time-close a tiny
        news experiment using the governed model's unrelated horizon policy.
        """
        extensions = trade.get("clientExtensions") or {}
        if str(extensions.get("tag") or "") != "strategy_lab_top":
            return False
        return not str(extensions.get("comment") or "").startswith("news007.")

    def __init__(self, *args: Any, canary_authorizer: GovernedCanaryAuthorizer, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.canary_authorizer = canary_authorizer

    def ranked_candidate(
        self,
        candidates: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """Apply immutable execution IDs after research-side consolidation."""
        selected, top = super().ranked_candidate(candidates)
        bound_by_object: dict[int, dict[str, Any]] = {}

        def bind(row: dict[str, Any]) -> dict[str, Any]:
            key = id(row)
            if key not in bound_by_object:
                bound_by_object[key] = bind_execution_signal_contract(row)
            return bound_by_object[key]

        bound_top = [bind(row) for row in top]
        self.last_qualified_candidates = [
            bind(row) for row in self.last_qualified_candidates
        ]
        bound_selected = None if selected is None else bind(selected)
        return bound_selected, bound_top

    def submit_selected_locked(self, candidate: dict[str, Any]) -> None:
        decision = self.canary_authorizer.authorize(
            candidate,
            account_id=self.account_id,
        )
        if not decision["allowed"]:
            self.log_execution_skip(
                reason="governed_canary_not_authorized",
                authorization_reason=decision["reason"],
                selected_id=decision["candidate_id"],
                proof_cohort_id=decision["proof_cohort_id"],
                governed_hypothesis_id=decision["governed_hypothesis_id"],
                allocator_cohort_id=decision["allocator_cohort_id"],
            )
            return
        governed_candidate = dict(candidate)
        governed_candidate["_governed_canary_authorization"] = decision
        super().submit_selected_locked(governed_candidate)

    def portfolio_blocker(self, candidate: dict[str, Any], trades: list[dict[str, Any]]) -> str:
        blocker = super().portfolio_blocker(candidate, trades)
        if blocker:
            return blocker
        # Selection calls this hook before the one-time canary is consumed and
        # attached.  Apply the ordinary portfolio gates in that pass, then
        # defer the canary-specific exposure cap to the locked submission pass
        # where submit_selected_locked() has attached the exact authorization.
        # Treating the absent preselection authorization as zero limits makes
        # every otherwise routeable canary fail before it can be authorized.
        authorization = candidate.get("_governed_canary_authorization")
        if not isinstance(authorization, dict):
            return ""
        maximum_total = lab.safe_float(authorization.get("maximum_total_exposure"), 0.0)
        current_total = 0.0
        for trade in trades:
            instrument = str(trade.get("instrument") or "")
            units = abs(lab.safe_float(trade.get("currentUnits")))
            price = lab.safe_float(trade.get("price"))
            if units > 0.0 and price > 0.0:
                current_total += units * price * self.quote_to_account_rate(instrument, price)
        candidate["_governed_current_total_exposure"] = current_total
        if maximum_total <= 0.0 or current_total >= maximum_total:
            return "governed_canary_total_exposure_limit"
        return ""

    def dynamic_sizing(self, candidate: dict[str, Any], summary: dict[str, Any]) -> tuple[int, dict[str, float]]:
        units, sizing = super().dynamic_sizing(candidate, summary)
        authorization = candidate.get("_governed_canary_authorization") or {}
        maximum_units = int(lab.safe_float(authorization.get("maximum_units"), 0.0))
        maximum_notional = lab.safe_float(authorization.get("maximum_notional"), 0.0)
        maximum_total = lab.safe_float(authorization.get("maximum_total_exposure"), 0.0)
        current_total = lab.safe_float(candidate.get("_governed_current_total_exposure"), 0.0)
        instrument = str(candidate.get("instrument") or "")
        mid = (lab.safe_float(candidate.get("bid")) + lab.safe_float(candidate.get("ask"))) / 2.0
        conversion = self.quote_to_account_rate(instrument, mid) if mid > 0.0 else 0.0
        unit_notional = mid * conversion
        caps = [units, maximum_units]
        if unit_notional > 0.0:
            caps.append(int(maximum_notional / unit_notional))
            caps.append(int(max(0.0, maximum_total-current_total) / unit_notional))
        governed_units = max(0, min(caps))
        governed_sizing = dict(sizing)
        governed_sizing.update(
            {
                "governed_maximum_units": maximum_units,
                "governed_maximum_notional": maximum_notional,
                "governed_maximum_total_exposure": maximum_total,
                "governed_current_total_exposure": current_total,
                "governed_units": governed_units,
            }
        )
        return governed_units, governed_sizing

    def final_submission_blocker(
        self,
        candidate: dict[str, Any],
        units: int,
        order_body: dict[str, Any],
    ) -> str:
        decision = candidate.get("_governed_canary_authorization") or {}
        if not decision.get("allowed"):
            return "governed_canary_final_decision_missing"
        if time.time() >= lab.safe_float(decision.get("expires_epoch"), 0.0):
            return "governed_canary_expired_before_submission"
        quote_epoch = self.canary_authorizer._timestamp_epoch(candidate.get("entry_time"))
        if quote_epoch is None or time.time() - quote_epoch > 15.0 or quote_epoch > time.time() + 60.0:
            return "governed_canary_quote_stale_before_submission"
        # Force a disk re-read so an edit or revocation after initial
        # selection cannot hide behind the one-second monitoring cache.
        self.canary_authorizer._cached_at_monotonic = -math.inf
        payload, error = self.canary_authorizer._load()
        if payload is None:
            return "governed_canary_final_" + error
        if not self.canary_authorizer.hmac_key:
            return "governed_canary_final_hmac_key_missing"
        expected = canary_payload_signature(payload, self.canary_authorizer.hmac_key)
        if not hmac.compare_digest(str(payload.get("signature_hmac_sha256") or ""), expected):
            return "governed_canary_final_hmac_invalid"
        lifecycle_ok, lifecycle_reason = self.canary_authorizer._confirmed_lifecycle_state(
            str(decision.get("governed_hypothesis_id") or ""),
            str(decision.get("proof_cohort_id") or ""),
        )
        if not lifecycle_ok:
            return "governed_canary_final_" + lifecycle_reason
        independent_ok, independent_reason = self.canary_authorizer._independent_verification(
            str(decision.get("governed_hypothesis_id") or ""),
            str(decision.get("proof_cohort_id") or ""),
        )
        if not independent_ok:
            return "governed_canary_final_" + independent_reason
        if units <= 0 or units > int(lab.safe_float(decision.get("maximum_units"), 0.0)):
            return "governed_canary_final_units_limit"
        return ""


def parse_executor_args(argv: list[str] | None) -> Any:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--governed-canary-authorization",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--governed-canary-maximum-age-sec",
        type=float,
        default=900.0,
    )
    parser.add_argument(
        "--governed-lifecycle-database",
        type=Path,
        required=True,
    )
    parser.add_argument("--governed-independent-verifier-state", type=Path, required=True)
    parser.add_argument("--governed-independent-verifier-guard", type=Path)
    parser.add_argument("--governed-canary-consumption-database", type=Path, required=True)
    parser.add_argument("--research-market-quote-seed-snapshot", type=Path)
    governed, remaining = parser.parse_known_args(argv)
    args = lab.parse_args(remaining)
    args.governed_canary_authorization = governed.governed_canary_authorization
    args.governed_canary_maximum_age_sec = governed.governed_canary_maximum_age_sec
    args.governed_lifecycle_database = governed.governed_lifecycle_database
    args.governed_independent_verifier_state = governed.governed_independent_verifier_state
    args.governed_independent_verifier_guard = governed.governed_independent_verifier_guard
    args.governed_canary_consumption_database = governed.governed_canary_consumption_database
    args.research_market_quote_seed_snapshot = governed.research_market_quote_seed_snapshot
    return args


class ExecutionSignalFeed:
    """Minimal consolidated-feed interface for the dedicated executor."""

    def __init__(
        self,
        path: Path,
        *,
        busy_timeout_ms: int = 3000,
    ) -> None:
        self.path = Path(path)
        if not self.path.is_file():
            raise RuntimeError(
                f"initialized signal feed is missing: {self.path}"
            )
        self.connection = sqlite3.connect(
            self.path,
            timeout=max(0.1, busy_timeout_ms / 1000.0),
        )
        self.connection.execute(
            f"PRAGMA busy_timeout={max(100, int(busy_timeout_ms))}"
        )
        # Execution audit writes share the forecast WAL. Match producers so a
        # single fill/rejection record cannot unexpectedly trigger a large
        # synchronous checkpoint inside the low-latency executor loop. The
        # supervised PASSIVE checkpointer owns maintenance.
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute(
            "PRAGMA wal_autocheckpoint="
            f"{SIGNAL_FEED_WAL_AUTOCHECKPOINT_FRAMES}"
        )
        self.connection.execute("PRAGMA journal_size_limit=134217728")
        required = {
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
        }.difference(required)
        if missing:
            self.connection.close()
            raise RuntimeError(
                "signal feed is not initialized: "
                + ",".join(sorted(missing))
            )

    def recent(self, limit: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT payload_json FROM candidates
            WHERE expires_epoch >= ?
            ORDER BY published_epoch DESC
            LIMIT ?
            """,
            (time.time(), max(1, int(limit))),
        ).fetchall()
        output: list[dict[str, Any]] = []
        for (payload_json,) in rows:
            try:
                payload = json.loads(payload_json)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                output.append(payload)
        return output

    def coverage(self, fresh_sec: float = 300.0) -> dict[str, Any]:
        now = time.time()
        fresh_cutoff = now - max(1.0, float(fresh_sec))
        registered, expected, fresh, eligible = self.connection.execute(
            """
            SELECT
                COUNT(*),
                COALESCE(SUM(expected), 0),
                COALESCE(SUM(
                    CASE WHEN last_seen_epoch >= ? THEN 1 ELSE 0 END
                ), 0),
                COALESCE(SUM(account_eligible), 0)
            FROM contributor_registry
            """,
            (fresh_cutoff,),
        ).fetchone()
        active_sources = [
            {"source": str(source), "candidates": int(count)}
            for source, count in self.connection.execute(
                """
                SELECT source, COUNT(*) FROM candidates
                WHERE expires_epoch >= ?
                GROUP BY source
                ORDER BY COUNT(*) DESC, source
                """,
                (now,),
            ).fetchall()
        ]
        return {
            "schema_version": 1,
            "generated_utc": lab.utc_now(),
            "database": str(self.path.resolve()),
            "registered_contributors": int(registered),
            "expected_contributors": int(expected),
            "fresh_contributors": int(fresh),
            "fresh_window_sec": float(fresh_sec),
            "account_eligible_contributors": int(eligible),
            "active_feed_sources": active_sources,
            "contract": {
                "executor_consumer_only": True,
                "schema_migrations": False,
                "checkpoint_owner": False,
                "execution_audit_writes": True,
            },
        }

    def seconds_since_last_fill(self) -> float:
        row = self.connection.execute(
            """
            SELECT MAX(submitted_epoch) FROM executions
            WHERE status = 'filled'
            """
        ).fetchone()
        submitted = (
            lab.safe_float(row[0])
            if row and row[0] is not None
            else 0.0
        )
        return (
            math.inf
            if submitted <= 0.0
            else max(0.0, time.time() - submitted)
        )

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
                client_id, candidate_id, submitted_epoch,
                status, trade_id, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                str(candidate.get("id") or ""),
                time.time(),
                status,
                trade_id,
                json.dumps(
                    candidate,
                    separators=(",", ":"),
                    sort_keys=True,
                    default=str,
                ),
            ),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


def build_promotion(args: Any) -> Any:
    promotion = lab.LanePromotionModel(
        args.promotion_database,
        args.promotion_state,
        args.execution_horizons,
        thresholds=lab.PromotionThresholds(
            min_samples=args.execution_min_samples,
            min_average_pips=args.execution_min_average_pips,
            min_median_pips=args.execution_min_median_pips,
            min_win_rate=args.execution_min_win_rate,
            min_lower_confidence_pips=args.execution_min_lower_confidence_pips,
            min_independent_blocks=args.execution_min_independent_blocks,
            min_holdout_blocks=args.execution_min_holdout_blocks,
            min_pairs=args.execution_min_pairs,
            min_sessions=args.execution_min_sessions,
            min_segment_samples=args.execution_min_segment_samples,
        ),
        refresh_sec=args.promotion_refresh_sec,
        fit_enabled=False,
    )
    promotion.load_state()
    return promotion


def usable_price_map(prices: dict[str, Any]) -> dict[str, Any]:
    return {
        instrument: quote
        for instrument, quote in prices.items()
        if not lab.outcome_quote_rejection_reason(quote)
    }


def research_quote_payload(
    prices: dict[str, Any],
    pip_sizes: dict[str, float],
    account_id: str,
) -> dict[str, Any]:
    quote_rows = {
        instrument: {
            "bid": lab.safe_float(quote.bid),
            "ask": lab.safe_float(quote.ask),
            "time": str(quote.time or ""),
            "pip": lab.safe_float(
                pip_sizes.get(
                    instrument,
                    0.01 if instrument.endswith("_JPY") else 0.0001,
                )
            ),
            "source": str(
                getattr(quote, "source", "") or "fast_executor_rest_poll"
            ),
        }
        for instrument, quote in usable_price_map(prices).items()
    }
    return {
        "schema_version": 1,
        "generated_utc": lab.utc_now(),
        "account_suffix": account_id[-4:],
        "producer": "practice_007_fast_executor",
        "quote_count": len(quote_rows),
        "quotes": quote_rows,
        "research_only": True,
    }


def main_loop_owns_quote_snapshot(
    stream: Any,
    stream_healthy: bool,
) -> bool:
    """REST fallback publishes only when no healthy stream writer exists."""
    return stream is None or not stream_healthy


def report_cycle_error(
    heartbeat: Any,
    log_path: Path,
    errors: int,
    exc: BaseException,
) -> dict[str, Any]:
    """Publish cycle failures immediately instead of waiting for recovery."""
    payload = {
        "time": lab.utc_now(),
        "kind": type(exc).__name__,
        "message": str(exc)[:500],
    }
    lab.log_line(
        log_path,
        "fast_executor_cycle_error",
        **payload,
    )
    heartbeat.update(
        phase="streaming",
        session_errors=errors,
        last_error=payload,
    )
    return payload


def capture_slow_cycle(
    previous: dict[str, Any] | None,
    *,
    cycle_ms: float,
    cycle_timings: dict[str, float],
    rank_timings: dict[str, Any],
    priced_instruments: int,
    usable_priced_instruments: int,
    prefiltered_candidates: int = 0,
    snapshot_timings: dict[str, Any] | None = None,
    feed_cache: dict[str, Any] | None = None,
    threshold_ms: float = 10_000.0,
) -> dict[str, Any] | None:
    """Retain the most recent slow cycle after faster cycles resume."""
    if cycle_ms < max(1.0, float(threshold_ms)):
        return previous
    return {
        "time": lab.utc_now(),
        "cycle_ms": round(float(cycle_ms), 3),
        "cycle_timings": dict(cycle_timings),
        "rank_timings": dict(rank_timings),
        "snapshot_timings": dict(snapshot_timings or {}),
        "feed_cache": dict(feed_cache or {}),
        "priced_instruments": int(priced_instruments),
        "usable_priced_instruments": int(usable_priced_instruments),
        "prefiltered_candidates": int(prefiltered_candidates),
    }


def active_quote_transport_stats(
    price_stream_stats: dict[str, Any],
    fallback_quote_publisher: Any,
) -> dict[str, Any]:
    """Report the transport that actually owns the shared quote snapshot."""

    if price_stream_stats:
        result = dict(price_stream_stats.get("research_snapshot_transport") or {})
        result["mode"] = "price_stream_async_snapshot"
        return result
    if fallback_quote_publisher is not None:
        result = dict(fallback_quote_publisher.stats())
        result["mode"] = "rest_fallback_async_snapshot"
        return result
    return {"enabled": False, "mode": "disabled"}


def qualified_candidate_preview(
    candidates: list[dict[str, Any]],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Return compact, read-only evidence for availability-gap attribution.

    The preview is heartbeat telemetry only.  It deliberately omits order size,
    credentials, and mutable execution state, and it cannot feed back into
    ranking or order submission.
    """

    fields = (
        "id",
        "instrument",
        "direction",
        "family",
        "lane_id",
        "strategy_archetype",
        "execution_horizon_sec",
        "preferred_horizon_sec",
        "signal_confidence",
        "instant_projected_net_pips",
        "projected_net_pips",
        "projected_gross_movement_pips",
        "spread_pips",
        "gross_to_spread",
        "direction_conflict",
        "opposing_direction",
        "signal_eligible",
        "signal_blocked_by",
        "feed_source",
        "feed_published_epoch",
        "entry_time",
    )
    preview: list[dict[str, Any]] = []
    for candidate in candidates[: max(0, int(limit))]:
        row = {
            field: candidate.get(field)
            for field in fields
            if candidate.get(field) is not None
        }
        row["execution_permitted_after_conflict_gate"] = bool(
            candidate.get("signal_eligible")
            and not candidate.get("direction_conflict")
        )
        preview.append(row)
    return preview


def main(argv: list[str] | None = None) -> int:
    args = parse_executor_args(argv)
    if not args.execute_top_signals:
        raise SystemExit("The fast practice executor requires --execute-top-signals.")
    if "practice" not in str(lab.BASE_URL).lower():
        raise SystemExit("The fast executor is restricted to the OANDA practice endpoint.")

    args.log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    selector = (
        lab.re.sub(
            r"[^a-z0-9]+",
            "_",
            str(args.account_key or "account").lower(),
        ).strip("_")
    )
    log_path = (
        args.log_dir
        / f"practice_top_signal_executor_{selector}_{stamp}.jsonl"
    )
    heartbeat = lab.WorkerHeartbeat(
        args.heartbeat_state,
        worker="oanda_practice_top_signal_executor",
        role="practice_007_fast_executor",
    ).start()
    heartbeat.update(phase="reading_practice_credentials")
    token, account_id = lab.read_credentials(
        args.creds,
        args.account_key,
        args.account_id,
    )
    heartbeat.update(phase="loading_practice_instruments")
    instruments = lab.selected_instruments(args, token, account_id)
    if not instruments:
        raise SystemExit("No tradeable FX instruments selected.")

    heartbeat.update(
        phase="loading_execution_support",
        account_suffix=account_id[-4:],
        instrument_count=len(instruments),
    )
    client = lab.MarketDataClient(token)
    pip_sizes = lab.account_pip_sizes(client, account_id)
    performance = lab.LanePerformance(args.execution_ranking_horizon_sec)
    if args.execution_ranking_log is not None:
        performance.load(args.execution_ranking_log)
    exit_fit = lab.StrategyExitFit(
        args.exit_fit_database,
        args.exit_fit_state,
        horizon_sec=args.execution_ranking_horizon_sec,
        horizons_sec=args.execution_horizons,
        fit_enabled=False,
        record_enabled=False,
    )
    promotion = build_promotion(args)
    feed_path = Path(args.execution_signal_feed_database)
    args.execution_signal_feed_database = None
    canary_authorizer = GovernedCanaryAuthorizer(
        args.governed_canary_authorization,
        lifecycle_database=args.governed_lifecycle_database,
        independent_verifier_state=args.governed_independent_verifier_state,
        independent_verifier_guard=args.governed_independent_verifier_guard,
        consumption_database=args.governed_canary_consumption_database,
        maximum_age_sec=args.governed_canary_maximum_age_sec,
    )
    executor = GovernedPracticeExecutor(
        client,
        account_id,
        log_path,
        performance,
        args,
        exit_fit,
        promotion,
        canary_authorizer=canary_authorizer,
    )
    heartbeat.update(phase="opening_signal_feed")
    executor.signal_feed = ExecutionSignalFeed(feed_path)
    args.execution_signal_feed_database = feed_path
    heartbeat.update(phase="initializing_practice_account")
    executor.initialize()
    if args.execution_feed_consumer_only and executor.signal_feed_cache is None:
        raise RuntimeError(
            "Consumer-only practice executor requires the asynchronous "
            "signal-feed cache."
        )
    heartbeat.update(phase="starting_price_stream")

    stream: Any = None
    fallback_quote_publisher: Any = None
    if args.use_price_stream:
        stream = lab.MultiPriceStream(
            lambda: lab.read_credentials(
                args.creds,
                args.account_key,
                args.account_id,
            ),
            instruments,
            lambda event, **fields: lab.log_line(
                log_path,
                event,
                **fields,
            ),
            research_snapshot_path=args.research_market_quote_snapshot,
            research_snapshot_interval_sec=(
                args.research_market_quote_snapshot_sec
            ),
            research_snapshot_producer=(
                "practice_007_fast_executor_price_stream"
            ),
            research_snapshot_min_instruments=max(
                1,
                math.ceil(len(instruments) * 0.95),
            ),
            research_snapshot_seed_paths=(
                [args.research_market_quote_seed_snapshot]
                if args.research_market_quote_seed_snapshot is not None
                else []
            ),
            pip_sizes=pip_sizes,
        )
        stream.start()
        stream.wait_ready(args.stream_start_timeout_sec)
        executor.set_price_snapshot_provider(stream.snapshot)
    elif args.research_market_quote_snapshot is not None:
        fallback_quote_publisher = lab.QuoteSnapshotPublisher(
            args.research_market_quote_snapshot
        )

    lab.log_line(
        log_path,
        "fast_executor_start",
        account_suffix=account_id[-4:],
        instrument_count=len(instruments),
        signal_feed=str(args.execution_signal_feed_database.resolve()),
        frozen_policy=str(args.execution_policy_state.resolve()),
        scan_pause_sec=args.scan_pause_sec,
        real_money_routing=False,
        practice_execution=True,
        governed_canary_gate=canary_authorizer.summary(account_id=account_id),
    )
    heartbeat.update(
        phase="streaming",
        account_suffix=account_id[-4:],
        instrument_count=len(instruments),
        execution_enabled=True,
        new_entry_authorized=False,
        real_money_routing=False,
        signal_feed=str(args.execution_signal_feed_database.resolve()),
    )

    stop_at = time.monotonic() + args.duration_sec
    next_rest_fallback = 0.0
    next_heartbeat = 0.0
    next_quote_snapshot = 0.0
    prices: dict[str, Any] = {}
    cycles = 0
    errors = 0
    last_error: dict[str, Any] | None = None
    last_slow_cycle: dict[str, Any] | None = None
    try:
        while time.monotonic() < stop_at:
            cycle_started = time.perf_counter()
            cycle_timings: dict[str, float] = {}
            try:
                stream_prices = stream.snapshot() if stream is not None else {}
                stream_healthy = bool(
                    stream
                    and stream.healthy(args.stream_stale_sec)
                )
                stream_complete = len(stream_prices) >= max(
                    1,
                    math.ceil(len(instruments) * 0.95),
                )
                if stream_healthy and stream_complete:
                    prices = stream_prices
                elif time.monotonic() >= next_rest_fallback:
                    rest_prices = client.pricing_snapshot(
                        account_id,
                        instruments,
                    )
                    prices = lab.merge_price_maps(
                        rest_prices,
                        stream_prices,
                    )
                    next_rest_fallback = (
                        time.monotonic()
                        + max(0.5, args.rest_fallback_sec)
                    )
                else:
                    prices = lab.merge_price_maps(prices, stream_prices)
                if (
                    args.research_market_quote_snapshot is not None
                    and main_loop_owns_quote_snapshot(
                        stream,
                        stream_healthy,
                    )
                    and time.monotonic() >= next_quote_snapshot
                ):
                    quote_payload = research_quote_payload(
                        prices,
                        pip_sizes,
                        account_id,
                    )
                    if quote_payload["quote_count"]:
                        if stream is not None:
                            stream.publish_research_snapshot(quote_payload)
                        elif fallback_quote_publisher is not None:
                            fallback_quote_publisher.submit(quote_payload)
                    next_quote_snapshot = time.monotonic() + max(
                        1.0,
                        args.research_market_quote_snapshot_sec,
                    )
                after_prices = time.perf_counter()
                cycle_timings["price_refresh_ms"] = round(
                    (after_prices - cycle_started) * 1000.0,
                    3,
                )

                executor.manage_open_trades(
                    prices=usable_price_map(prices),
                )
                after_manage = time.perf_counter()
                cycle_timings["trade_management_ms"] = round(
                    (after_manage - after_prices) * 1000.0,
                    3,
                )
                # Passing no local candidates makes this process a pure
                # consumer of the consolidated shared feed. Management runs
                # first so an expensive ranking pass cannot delay protection.
                executor.maybe_execute([])
                after_execution = time.perf_counter()
                cycle_timings["selection_ms"] = round(
                    (after_execution - after_manage) * 1000.0,
                    3,
                )
                cycles += 1
                cycle_ms = (
                    time.perf_counter() - cycle_started
                ) * 1000.0
                usable_prices = usable_price_map(prices)
                last_slow_cycle = capture_slow_cycle(
                    last_slow_cycle,
                    cycle_ms=cycle_ms,
                    cycle_timings=cycle_timings,
                    rank_timings=executor.last_rank_timings,
                    snapshot_timings=executor.last_signal_snapshot_timings,
                    feed_cache=executor.last_feed_cache_stats,
                    priced_instruments=len(prices),
                    usable_priced_instruments=len(usable_prices),
                    prefiltered_candidates=(
                        executor.last_execution_prefilter_count
                    ),
                )
                if time.monotonic() >= next_heartbeat:
                    price_stream_stats = (
                        {} if stream is None else stream.stats()
                    )
                    heartbeat.update(
                        phase="streaming",
                        cycles=cycles,
                        session_errors=errors,
                        last_error=last_error,
                        priced_instruments=len(prices),
                        usable_priced_instruments=len(usable_prices),
                        feed_candidates=executor.last_feed_candidate_count,
                        prefiltered_candidates=(
                            executor.last_execution_prefilter_count
                        ),
                        qualified_candidates=len(
                            executor.last_qualified_candidates
                        ),
                        nonconflicting_qualified_candidates=sum(
                            1
                            for row in executor.last_qualified_candidates
                            if not bool(row.get("direction_conflict"))
                        ),
                        qualified_candidate_preview=(
                            qualified_candidate_preview(
                                executor.last_qualified_candidates
                            )
                        ),
                        final_selection_blocks=(
                            executor.last_execution_selection_blocks
                        ),
                        last_execution_skip_reason=(
                            executor.last_execution_skip_reason
                        ),
                        last_execution_skip_age_sec=(
                            None
                            if not math.isfinite(
                                executor.last_execution_skip_log_monotonic
                            )
                            else round(
                                max(
                                    0.0,
                                    time.monotonic()
                                    - executor.last_execution_skip_log_monotonic,
                                ),
                                3,
                            )
                        ),
                        fills=executor.fills,
                        execution_disabled_reason=executor.disabled_reason,
                        last_cycle_ms=round(cycle_ms, 3),
                        cycle_timings=cycle_timings,
                        rank_timings=executor.last_rank_timings,
                        snapshot_timings=executor.last_signal_snapshot_timings,
                        feed_cache=executor.last_feed_cache_stats,
                        last_slow_cycle=last_slow_cycle,
                        price_stream=price_stream_stats,
                        quote_transport=active_quote_transport_stats(
                            price_stream_stats,
                            fallback_quote_publisher,
                        ),
                        order_telemetry=executor.execution_telemetry_summary(),
                        reentry_guard=executor.reentry_guard_summary(),
                        governed_canary_gate=(
                            canary_authorizer.summary(account_id=account_id)
                        ),
                        new_entry_authorized=(
                            canary_authorizer.summary(account_id=account_id)[
                                "entry_authorized"
                            ]
                        ),
                    )
                    next_heartbeat = time.monotonic() + 5.0
            except Exception as exc:
                errors += 1
                last_error = report_cycle_error(
                    heartbeat,
                    log_path,
                    errors,
                    exc,
                )
            elapsed = time.perf_counter() - cycle_started
            time.sleep(
                min(
                    max(0.01, args.scan_pause_sec - elapsed),
                    max(0.0, stop_at - time.monotonic()),
                )
            )
    finally:
        if stream is not None:
            stream.stop()
        if fallback_quote_publisher is not None:
            fallback_quote_publisher.close(timeout_sec=5.0)
        executor.close()
        exit_fit.close()
        heartbeat.close()
        client.close()
        lab.close_log_handles()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
