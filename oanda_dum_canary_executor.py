#!/usr/bin/env python3
"""DUM canary executor for one explicitly assigned model candidate.

This is deliberately separate from the research trainer.  It monitors the DUM
practice account and may place very small canary trades only when:

1. `data/oanda_training_manager/promotions/canary_candidate.json` exists,
2. the manifest stage is `canary` and came from a shadow-passed candidate, and
3. the candidate model approves each live technical signal.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, Tuple

import joblib

import oanda_technical_account_manager_auto as tech


CANARY_MANIFEST = (
    tech.SCRIPT_DIR
    / "data"
    / "oanda_training_manager"
    / "promotions"
    / "canary_candidate.json"
)


class DUMCanaryManager(tech.ForexManager):
    def sync_promoted_model_manifest(self) -> Dict[str, Any]:
        path = Path(CANARY_MANIFEST)
        if not path.exists():
            self._promoted_model_manifest = {}
            self._promoted_model_bundle = {}
            return {}
        try:
            mtime = path.stat().st_mtime_ns
            if mtime == self._promoted_model_manifest_mtime:
                return self._promoted_model_manifest
            manifest = tech.read_json(path, {})
            artifact = Path(str(manifest.get("model_artifact_path", "")))
            canary_stage = str(manifest.get("stage", "")).lower() == "canary"
            shadow_passed = bool(manifest.get("shadow_passed", False))
            hosted = bool(artifact.exists() and artifact.is_file())
            activation = bool(hosted and canary_stage and shadow_passed)
            manifest["hosted_by_canary_account"] = hosted
            manifest["activation_effective"] = activation
            bundle: Dict[str, Any] = {}
            if activation:
                loaded = joblib.load(artifact)
                if isinstance(loaded, dict):
                    bundle = loaded
                else:
                    activation = False
                    manifest["activation_effective"] = False
                    manifest["model_load_error"] = "artifact did not contain a bundle dictionary"
            self._promoted_model_manifest = manifest
            self._promoted_model_bundle = bundle
            self._promoted_model_manifest_mtime = mtime
            self.full_logger.log_event(
                "canary_candidate_manifest",
                status="active" if activation else "inactive",
                reason=str(manifest.get("reason", ""))[:1000],
                result=(
                    f"experiment={manifest.get('experiment_id','')} "
                    f"stage={manifest.get('stage','')} hosted={hosted}"
                ),
                raw=manifest,
            )
            return manifest
        except Exception as exc:
            self.log_error("sync canary candidate manifest", exc)
            return self._promoted_model_manifest

    def production_model_allows_new_entries(self) -> Tuple[bool, str]:
        manifest = self.sync_promoted_model_manifest()
        if manifest.get("activation_effective", False):
            return True, (
                f"canary model active experiment="
                f"{manifest.get('experiment_id','')}"
            )
        return False, "new entries blocked: no active canary_candidate.json"


def build_canary_config(base_cfg: tech.BotConfig) -> tech.BotConfig:
    dum_id = tech.cfg_str(
        "OANDA_ACCOUNT_ID_DUM1",
        "OANDA_ACCOUNT_ID_DUM",
        default="101-001-37981792-004",
    )
    if not dum_id:
        raise RuntimeError("Missing OANDA_ACCOUNT_ID_DUM1 for canary executor")
    cfg = replace(
        base_cfg,
        oanda_account_id=dum_id,
        account_lane="canary",
        account_display_name="OANDA_ACCOUNT_ID_DUM1",
        execute_trades=True,
        auto_execute_research_actions=False,
        scan_on_launch=False,
        calls_per_trading_day=0,
        call_times_ny=[],
        friday_call_times_ny=[],
        event_trigger_research_enabled=False,
        min_minutes_between_research_scans=999999,
        max_open_trades=1,
        max_new_trades_per_scan=1,
        max_total_new_risk_pct_per_scan=0.10,
        event_max_total_scout_risk_pct=0.10,
        event_max_scout_trades_per_event=1,
        event_scout_risk_pct=0.05,
        target_margin_used_pct=5.0,
        max_margin_used_pct=12.0,
        min_risk_pct_per_trade=0.01,
    )
    return tech.with_data_dir(
        cfg,
        base_cfg.data_dir / "account_dum_canary_model_candidate",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run DUM candidate canary executor.")
    parser.add_argument("--once", action="store_true", help="Run one loop iteration and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Disable broker execution for this run.")
    args = parser.parse_args()
    base = tech.BotConfig.load()
    cfg = build_canary_config(base)
    if args.dry_run:
        cfg = replace(cfg, execute_trades=False)
    bot = DUMCanaryManager(cfg)
    bot.validate_config()
    tech.ensure_bot_ready(bot)
    if args.once:
        tech.run_bot_loop_iteration(bot)
        return 0
    bot.loop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
