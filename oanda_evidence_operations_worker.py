#!/usr/bin/env python3
"""Hidden evidence-operations worker.

Runs passive lifecycle, allocator-shadow, and Practice-007 accounting cycles.
It neither imports a broker client nor exposes an order-submission path.
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from oanda_allocator_proof import run_allocator_cycle
from oanda_governed_practice_accounting import run_accounting_cycle
from oanda_hypothesis_lifecycle import run_lifecycle
from oanda_opportunity_decision_level_monitor import run_once as run_opportunity_decision_monitor


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "evidence_operations" / "EVIDENCE_OPERATIONS_CURRENT.md"
CANARY_AUTHORIZATION = STATE / "practice_007_governed_canary_authorization_v1.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    for attempt in range(10):
        try:
            temporary.replace(path)
            return
        except PermissionError:
            # Windows readers can briefly hold the destination without delete
            # sharing. Retrying preserves atomic replacement and heartbeat
            # continuity without ever exposing a partially written JSON file.
            if attempt == 9:
                raise
            time.sleep(0.02 * (attempt + 1))


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(path)


class OperationsProgressHeartbeat:
    """Publish worker liveness without replacing the last completed report.

    Lifecycle and allocator calculations can take longer than the supervisor's
    file-age window.  A separate heartbeat keeps ordinary liveness observable,
    while ``progress_age_sec`` still lets the supervisor terminate a genuinely
    stuck stage.
    """

    def __init__(self, path: Path, *, interval_sec: float = 15.0) -> None:
        self.path = Path(path)
        self.interval_sec = max(0.01, float(interval_sec))
        self.started_monotonic = time.monotonic()
        self.started_utc = utc_now()
        self.phase_started_monotonic = self.started_monotonic
        self.last_progress_monotonic = self.started_monotonic
        self.last_progress_utc = self.started_utc
        self.phase = "starting"
        self.status = "running"
        self.progress_sequence = 0
        self.cycles = 0
        self.errors = 0
        self.last_error = ""
        self.details: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="evidence-operations-heartbeat",
            daemon=True,
        )

    def start(self) -> None:
        self.publish()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_sec * 2.0))
        self.publish()

    def update(self, phase: str, details: dict[str, Any] | None = None) -> None:
        with self._lock:
            now = time.monotonic()
            if phase != self.phase:
                self.phase_started_monotonic = now
            self.phase = str(phase)
            self.last_progress_monotonic = now
            self.last_progress_utc = utc_now()
            self.progress_sequence += 1
            self.status = "running"
            self.last_error = ""
            if details:
                self.details.update(details)
        self.publish()

    def set_cycle_complete(self, details: dict[str, Any] | None = None) -> None:
        with self._lock:
            self.cycles += 1
        self.update("idle_between_cycles", details)

    def set_error(self, exc: BaseException) -> None:
        with self._lock:
            now = time.monotonic()
            self.phase = "cycle_error"
            self.phase_started_monotonic = now
            self.last_progress_monotonic = now
            self.last_progress_utc = utc_now()
            self.progress_sequence += 1
            self.errors += 1
            self.status = "degraded"
            self.last_error = f"{type(exc).__name__}: {exc}"
        self.publish()

    def payload(self) -> dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            return {
                "schema_version": 1,
                "generated_utc": utc_now(),
                "started_at": self.started_utc,
                "pid": os.getpid(),
                "worker": "oanda_evidence_operations_worker",
                "role": "research_only_evidence_operations",
                "research_only": True,
                "can_place_orders": False,
                "can_submit_orders": False,
                "can_promote": False,
                "real_money_routing": False,
                "status": self.status,
                "phase": self.phase,
                "phase_age_sec": round(now - self.phase_started_monotonic, 3),
                "progress_age_sec": round(now - self.last_progress_monotonic, 3),
                "last_progress_utc": self.last_progress_utc,
                "progress_sequence": self.progress_sequence,
                "uptime_sec": round(now - self.started_monotonic, 3),
                "cycles": self.cycles,
                "errors": self.errors,
                "last_error": self.last_error,
                "details": dict(self.details),
            }

    def publish(self) -> None:
        with self._publish_lock:
            atomic_json(self.path, self.payload())

    def wait(self, seconds: float) -> bool:
        return self._stop.wait(max(0.0, float(seconds)))

    def _run(self) -> None:
        while not self._stop.wait(self.interval_sec):
            self.publish()


def report_markdown(payload: dict[str, Any]) -> str:
    lifecycle = payload.get("lifecycle") or {}
    allocator = payload.get("allocator") or {}
    accounting = payload.get("accounting") or {}
    counts = (lifecycle.get("lifecycle") or {}).get("states") or {}
    evidence = allocator.get("evidence") or {}
    acct = (accounting.get("accounting") or {}).get("account_operational_continuity") or {}
    sentinel = accounting.get("routeability_sentinel") or {}
    canary = payload.get("practice_canary_authorization") or {}
    velocity = lifecycle.get("proof_cohort_evidence_velocity") or []
    opportunity = payload.get("opportunity_decision_level") or {}
    top_one = opportunity.get("top_one") or {}
    basket = opportunity.get("exactly_three_currency_disjoint") or {}
    lines = [
        "# Evidence Accumulation, Futility Retirement, and Allocator Proof",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        "This is a passive, fail-closed evidence report. It cannot place orders, promote a hypothesis, or enable real money.",
        "",
        "## Terminal lifecycle",
        "",
        f"- Governed hypotheses: **{(lifecycle.get('lifecycle') or {}).get('hypothesis_count', 0):,}**",
        f"- Permanently retired for futility: **{counts.get('futility_rejected', 0):,}**",
        f"- Confirmed candidates: **{counts.get('confirmed_candidate', 0):,}**",
        f"- Continue collecting: **{counts.get('continue_collecting', 0):,}**",
        "",
        "## Frozen allocator proof",
        "",
        f"- Cohort: `{evidence.get('cohort_id', 'not initialized')}`",
        f"- Lifecycle: **{evidence.get('lifecycle_state', 'unavailable')}**",
        f"- Decisions / fully matured: **{evidence.get('decision_count', 0):,} / {evidence.get('fully_matured_decision_count', 0):,}**",
        "- Same discovery window cannot confirm; Practice-007 routing is disabled for this cohort.",
        "",
        "## Executable-opportunity decision level",
        "",
        f"- Exact collector: `{opportunity.get('collector_cohort_id', 'unavailable')}`",
        f"- Decision epochs / raw pair rows: **{opportunity.get('decision_epochs', 0):,} / {opportunity.get('raw_pair_rows', 0):,}**",
        f"- Top-one matured / cost-clear / average signed net: **{top_one.get('matured_rows', 0):,} / {top_one.get('cost_clearing_rows', 0):,} / {top_one.get('average_predicted_side_net_pips')}**",
        f"- Three-independent basket decisions / matured legs / average signed net: **{basket.get('decisions', 0):,} / {basket.get('matured_rows', 0):,} / {basket.get('average_predicted_side_net_pips')}**",
        "- Decision epochs remain correlated observations, not independent proof episodes.",
        "",
        "## Practice-007 accounting",
        "",
        f"- Broker snapshot current: **{acct.get('account_state_current', False)}**; state/reason: **{acct.get('snapshot_state', 'unavailable')} / {acct.get('reason', 'unavailable')}**",
        f"- Balance / NAV / cumulative operational P&L: **{acct.get('balance')} / {acct.get('nav')} / {acct.get('cumulative_pl')}**",
        f"- Open trades / pending orders: **{acct.get('open_trade_count')} / {acct.get('pending_order_count')}**",
        f"- Routeability dry-run: **{'PASS' if sentinel.get('passed') else 'FAIL'}**; submission attempted: **{sentinel.get('submission_attempted', False)}**",
        f"- Governed new-entry authorization: **{canary.get('entry_authorized', False)}**; exact authorized entries: **{len(canary.get('authorized_entries') or [])}**; reason: `{canary.get('reason', 'unavailable')}`",
        "- Account cumulative P&L is operational context only, not governed-cohort evidence.",
        "",
        "## Proof-cohort evidence velocity",
        "",
        "| Cohort | Produced | Matured | Effective N | Episodes | Factor clusters | Effective N/day | State |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in velocity:
        lines.append(
            "| {cohort} | {produced} | {matured} | {effective} | {episodes} | {factors} | {rate} | {state} |".format(
                cohort=row.get("cohort_id", ""), produced=row.get("produced_forecasts", 0),
                matured=row.get("matured_forecasts", 0), effective=row.get("effective_n", 0),
                episodes=row.get("independent_episodes", 0), factors=row.get("independent_currency_factor_clusters", 0),
                rate=row.get("effective_observations_per_day", 0), state=row.get("lifecycle_state", "continue_collecting"),
            )
        )
    lines.extend(
        [
            "", "## Locked controls", "",
            "- Four predictor cohorts remain immutable; interim results cannot retune them.",
            "- Futility retirement is permanent unless a materially new hypothesis receives a new cohort ID.",
            "- Macro consensus remains blocked until a provable pre-release consensus snapshot exists.",
            "- Real-money routing and discretionary/manual order actions remain disabled.",
            "",
        ]
    )
    return "\n".join(lines)


def canary_authorization_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Publish a fresh, explicit no-entry gate until a canary is activated.

    Confirmation never auto-routes. A later canary activation must be an
    explicit, version-locked operation that supplies exact authorized entries.
    """
    lifecycle = payload.get("lifecycle") or {}
    allocator = payload.get("allocator") or {}
    counts = (lifecycle.get("lifecycle") or {}).get("states") or {}
    evidence = allocator.get("evidence") or {}
    accounting = payload.get("accounting") or {}
    continuity = (accounting.get("accounting") or {}).get(
        "account_operational_continuity"
    ) or {}
    routeability = accounting.get("routeability_sentinel") or {}
    account_state_current = bool(continuity.get("account_state_current"))
    confirmed = int(counts.get("confirmed_candidate", 0) or 0)
    return {
        "schema_version": 2,
        "generated_utc": payload.get("generated_utc") or utc_now(),
        "environment": "practice",
        "account_suffix": "-007",
        "account_id": "101-001-37981792-007",
        "entry_authorized": False,
        "account_state_current": account_state_current,
        "routeability_current_passed": bool(routeability.get("passed")),
        "authorized_entries": [],
        "confirmed_candidate_count": confirmed,
        "active_allocator_cohort_id": str(evidence.get("cohort_id") or ""),
        "reason": (
            "account_state_unavailable"
            if not account_state_current
            else (
                "routeability_sentinel_failed"
                if not bool(routeability.get("passed"))
                else (
                    "zero_confirmed_candidates"
                    if confirmed == 0
                    else "explicit_version_locked_practice_canary_activation_required"
                )
            )
        ),
        "auto_route": False,
        "real_money_routing": False,
        "manual_orders": False,
        "signed_entry_payload_required": True,
        "independent_verifier_required": True,
    }


def cycle(
    state_path: Path,
    report_path: Path,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    progress = progress_callback or (lambda _phase, _details=None: None)
    progress("running_lifecycle", None)
    lifecycle = run_lifecycle(
        progress_callback=lambda step, details=None: progress(
            "running_lifecycle",
            {"lifecycle_step": step, **dict(details or {})},
        )
    )
    progress("running_allocator", None)
    allocator = run_allocator_cycle()
    progress("running_accounting", None)
    accounting = run_accounting_cycle()
    progress("running_opportunity_monitor", None)
    opportunity_decision_level = run_opportunity_decision_monitor()
    progress("publishing_final_state", None)
    payload = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "status": "ok",
        "research_only": True,
        "can_submit_orders": False,
        "can_promote": False,
        "real_money_routing": False,
        "lifecycle": lifecycle,
        "allocator": allocator,
        "accounting": accounting,
        "opportunity_decision_level": opportunity_decision_level,
    }
    authorization = canary_authorization_payload(payload)
    payload["practice_canary_authorization"] = authorization
    atomic_json(state_path, payload)
    atomic_json(CANARY_AUTHORIZATION, authorization)
    atomic_text(report_path, report_markdown(payload))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--heartbeat", type=Path, default=STATE / "evidence_operations_worker_v1.json")
    parser.add_argument(
        "--liveness-heartbeat",
        type=Path,
        default=STATE / "evidence_operations_worker_heartbeat_v2.json",
    )
    parser.add_argument("--heartbeat-sec", type=float, default=15.0)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--interval-sec", type=float, default=300.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.time()
    heartbeat = OperationsProgressHeartbeat(
        args.liveness_heartbeat,
        interval_sec=args.heartbeat_sec,
    )
    heartbeat.start()
    try:
        while True:
            try:
                payload = cycle(args.heartbeat, args.report, heartbeat.update)
                heartbeat.set_cycle_complete(
                    {"last_completed_utc": payload.get("generated_utc")}
                )
            except Exception as exc:  # preserve a fail-closed completed-state error
                heartbeat.set_error(exc)
                atomic_json(
                    args.heartbeat,
                    {
                        "schema_version": 1, "generated_utc": utc_now(), "status": "error",
                        "error_type": type(exc).__name__, "error": str(exc),
                        "traceback": traceback.format_exc(), "can_submit_orders": False,
                        "can_promote": False, "real_money_routing": False,
                    },
                )
            if args.duration_sec > 0 and time.time() - started >= args.duration_sec:
                break
            if heartbeat.wait(max(5.0, args.interval_sec)):
                break
    finally:
        heartbeat.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
