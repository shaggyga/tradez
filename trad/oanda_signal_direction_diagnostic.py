#!/usr/bin/env python3
"""Check whether H1/H4 forecast direction survives OANDA-style execution."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from oanda_broker_env_sweep import EXECUTION_PROFILES, merged_live_cfg, merged_profile
from oanda_broker_style_portfolio_replay import (
    BrokerReplay,
    CANDLE_ROOT,
    CandleStore,
    DEFAULT_OUTPUT as BROKER_OUTPUT,
    PRIMARY_CONFIG,
    direction_int,
    instrument_parts,
    load_candidates,
    load_margin_rates,
    objective,
    read_json,
    safe_int,
)
from oanda_h1_h4_guard_recalibration import SCORE_GATES, STREAMS


DEFAULT_OUTPUT = BROKER_OUTPUT.parent / "h1_h4_oanda_direction_diagnostic"

FOCUS_ROWS: List[Tuple[str, str, str, str]] = [
    ("h4", "all", "h_tf_balanced", "quality_strict"),
    ("h4", "score_ge_012", "h_tf_balanced", "quality_strict"),
    ("h4", "score_ge_012", "h_tf_balanced", "very_wide_stop"),
    ("h4", "all", "h_tf_balanced", "very_wide_stop"),
    ("h4", "score_ge_012", "h_tf_growth", "quality_strict"),
    ("h4", "all", "h_tf_growth", "quality_strict"),
    ("h4", "score_ge_012", "h_tf_growth", "very_wide_stop"),
    ("h4", "all", "h_tf_growth", "very_wide_stop"),
]


def flip_directions(candidates: pd.DataFrame) -> pd.DataFrame:
    out = candidates.copy()
    out["direction"] = out["direction"].map(lambda value: "SHORT" if direction_int(value) > 0 else "LONG")
    if "campaign" in out.columns:
        out["campaign"] = out["instrument"].astype(str) + ":" + out["direction"].astype(str)
    return out


def run(output_root: Path, include_normal: bool) -> pd.DataFrame:
    selected = FOCUS_ROWS
    live_cfg_base = read_json(PRIMARY_CONFIG, {})
    loaded_candidates: Dict[Tuple[str, str], pd.DataFrame] = {}
    all_times: List[pd.Timestamp] = []
    all_instruments: set[str] = set()
    end_time = pd.Timestamp.min.tz_localize("UTC")
    for stream, gate, _guard, _exec in selected:
        key = (stream, gate)
        if key not in loaded_candidates:
            candidates = load_candidates(STREAMS[stream], SCORE_GATES[gate])
            loaded_candidates[key] = candidates
            all_times.extend(pd.to_datetime(candidates["time_utc"], utc=True).tolist())
            all_instruments.update(candidates["instrument"].astype(str).unique())
            end_time = max(end_time, pd.to_datetime(candidates["planned_exit_time"], utc=True).max())
    for path in CANDLE_ROOT.glob("*_M1.csv"):
        name = path.name.removesuffix("_M1.csv")
        base, quote = instrument_parts(name)
        if base == "USD" or quote == "USD":
            all_instruments.add(name)
    candles = CandleStore(CANDLE_ROOT, all_times, end_time)
    candles.load_all(all_instruments)
    margin_rates = load_margin_rates()
    output_root.mkdir(parents=True, exist_ok=True)

    direction_modes = ["normal", "inverted"] if include_normal else ["inverted"]
    rows: List[Dict[str, Any]] = []
    for direction_mode in direction_modes:
        for stream, gate, guard_name, exec_name in selected:
            base_candidates = loaded_candidates[(stream, gate)]
            candidates = base_candidates if direction_mode == "normal" else flip_directions(base_candidates)
            exec_cfg = EXECUTION_PROFILES[exec_name]
            profile = merged_profile(guard_name, exec_cfg)
            live_cfg = merged_live_cfg(live_cfg_base, exec_cfg)
            replay = BrokerReplay(
                profile,
                candles,
                margin_rates=margin_rates,
                live_cfg=live_cfg,
                max_new_positions_per_timestamp=safe_int(live_cfg_base.get("max_new_positions_per_cycle"), 2),
            )
            out_dir = output_root / f"{direction_mode}_{stream}_{gate}_{guard_name}_{exec_name}"
            summary = replay.run(candidates)
            row = {
                "direction_mode": direction_mode,
                "stream": stream,
                "score_gate": gate,
                "guard_profile": guard_name,
                "execution_profile": exec_name,
                "objective": objective(summary),
                **summary,
                **{f"exec_{key}": value for key, value in exec_cfg.items()},
                "guard_target_margin_pct": profile.get("target_margin_pct"),
                "guard_hard_margin_pct": profile.get("hard_margin_pct"),
                "guard_max_open_risk_pct": profile.get("max_open_risk_pct"),
                "output_dir": str(out_dir),
            }
            replay.write_outputs(out_dir, row, candidates)
            rows.append(row)
            print(json.dumps(row, sort_keys=True, default=str), flush=True)
    frame = pd.DataFrame(rows).sort_values(["return_pct", "profit_factor"], ascending=[False, False])
    frame.to_csv(output_root / "oanda_signal_direction_diagnostic_summary.csv", index=False)
    (output_root / "oanda_signal_direction_diagnostic_summary.json").write_text(
        json.dumps(rows, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return frame


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--include-normal", action="store_true")
    args = parser.parse_args()
    frame = run(args.output_root, include_normal=args.include_normal)
    print(frame.head(20).to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
