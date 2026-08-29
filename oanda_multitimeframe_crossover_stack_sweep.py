#!/usr/bin/env python3
"""Test multi-timeframe moving-average crossover entry stacks.

A primary completed-bar crossover creates the entry event. Zero, one, or two
other completed-timeframe crossover states may be required to agree with its
direction. Context crossovers can use different MA periods and kinds, so a
stack is not limited to repeating one crossover on every timeframe.

Entry-path scoring is delegated to the executable bid/ask MFE/MAE and
conservative first-touch machinery in
``oanda_moving_average_crossover_entry_quality.py``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import gc
import itertools
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

import oanda_moving_average_crossover_entry_quality as entry_quality
import oanda_moving_average_crossover_sweep as crossover


ROOT = Path(__file__).resolve().parent
DEFAULT_REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "multitimeframe_crossover_stack"
)
TIMEFRAME_HIERARCHY = (
    "M1",
    "M2",
    "M3",
    "M4",
    "M5",
    "M6",
    "M8",
    "M10",
    "M12",
    "M15",
    "M20",
    "M30",
    "M45",
    "H1",
    "H2",
    "H3",
    "H4",
    "H6",
    "H8",
    "H12",
    "D1",
)
DEFAULT_TRIGGER_TIMEFRAMES = TIMEFRAME_HIERARCHY[:-3]
DEFAULT_TRIGGER_SPECS = (
    "ema:3:8",
    "ema:3:9",
    "ema:5:13",
    "ema:8:21",
    "ema:12:26",
    "ema:20:50",
    "sma:8:21",
    "sma:20:50",
)
DEFAULT_CONTEXT_TEMPLATES = (
    "same",
    "ema:8:21",
    "sma:20:50",
)
DEFAULT_DOUBLE_CONTEXT_TEMPLATES = (
    ("same", "same"),
    ("ema:8:21", "sma:20:50"),
    ("ema:20:50", "ema:50:200"),
)
DEFAULT_TRIGGER_CONFIRMATIONS = ("cross_only", "slow_slope_volume")


@dataclass(frozen=True)
class CrossSpec:
    fast_kind: str
    slow_kind: str
    fast_window: int
    slow_window: int

    @property
    def label(self) -> str:
        return (
            f"{self.fast_kind}{self.fast_window}_"
            f"{self.slow_kind}{self.slow_window}"
        )


@dataclass(frozen=True)
class StackConfiguration:
    config_id: int
    timeframe: str
    trigger_spec: CrossSpec
    trigger_confirmation: str
    context_timeframes: tuple[str, ...]
    context_specs: tuple[CrossSpec, ...]

    @property
    def confirmation(self) -> str:
        return (
            f"{self.trigger_confirmation}|"
            f"{len(self.context_timeframes)}_context"
        )

    def as_dict(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "config_id": self.config_id,
            "timeframe": self.timeframe,
            "fast_kind": self.trigger_spec.fast_kind,
            "slow_kind": self.trigger_spec.slow_kind,
            "fast_window": self.trigger_spec.fast_window,
            "slow_window": self.trigger_spec.slow_window,
            "confirmation": self.confirmation,
            "trigger_confirmation": self.trigger_confirmation,
            "trigger_cross": self.trigger_spec.label,
            "stack_depth": len(self.context_timeframes),
        }
        for index in range(2):
            prefix = f"context_{index + 1}"
            if index < len(self.context_timeframes):
                spec = self.context_specs[index]
                row.update(
                    {
                        f"{prefix}_timeframe": self.context_timeframes[index],
                        f"{prefix}_cross": spec.label,
                        f"{prefix}_fast_kind": spec.fast_kind,
                        f"{prefix}_slow_kind": spec.slow_kind,
                        f"{prefix}_fast_window": spec.fast_window,
                        f"{prefix}_slow_window": spec.slow_window,
                    }
                )
            else:
                row.update(
                    {
                        f"{prefix}_timeframe": None,
                        f"{prefix}_cross": None,
                        f"{prefix}_fast_kind": None,
                        f"{prefix}_slow_kind": None,
                        f"{prefix}_fast_window": None,
                        f"{prefix}_slow_window": None,
                    }
                )
        return row


def parse_cross_spec(value: str) -> CrossSpec:
    parts = [part.strip().lower() for part in value.split(":")]
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            "cross specs use kind:fast:slow, for example ema:5:13"
        )
    kinds = parts[0].replace("/", "-").split("-")
    if len(kinds) == 1:
        fast_kind = slow_kind = kinds[0]
    elif len(kinds) == 2:
        fast_kind, slow_kind = kinds
    else:
        raise argparse.ArgumentTypeError(f"invalid MA kinds in {value!r}")
    if fast_kind not in {"ema", "sma"} or slow_kind not in {"ema", "sma"}:
        raise argparse.ArgumentTypeError("MA kinds must be ema or sma")
    fast_window, slow_window = int(parts[1]), int(parts[2])
    if fast_window <= 0 or slow_window <= fast_window:
        raise argparse.ArgumentTypeError("cross windows require 0 < fast < slow")
    return CrossSpec(fast_kind, slow_kind, fast_window, slow_window)


def parse_cross_specs(value: str) -> list[CrossSpec]:
    specs = {
        parse_cross_spec(item)
        for item in crossover.parse_csv_strings(value)
    }
    if not specs:
        raise argparse.ArgumentTypeError("provide at least one crossover spec")
    return sorted(
        specs,
        key=lambda item: (
            item.fast_kind,
            item.slow_kind,
            item.fast_window,
            item.slow_window,
        ),
    )


def context_timeframes_for(
    trigger_timeframe: str,
    multipliers: Sequence[int] = (3, 12, 48),
) -> tuple[str, ...]:
    trigger_minutes = crossover.TIMEFRAME_MINUTES[trigger_timeframe]
    higher = [
        timeframe
        for timeframe in TIMEFRAME_HIERARCHY
        if crossover.TIMEFRAME_MINUTES[timeframe] > trigger_minutes
    ]
    selected: list[str] = []
    for multiplier in multipliers:
        target = trigger_minutes * multiplier
        candidate = next(
            (
                timeframe
                for timeframe in higher
                if crossover.TIMEFRAME_MINUTES[timeframe] >= target
            ),
            None,
        )
        if candidate is None and higher:
            candidate = higher[-1]
        if candidate is not None and candidate not in selected:
            selected.append(candidate)
    return tuple(selected)


def resolve_context_template(template: str, trigger_spec: CrossSpec) -> CrossSpec:
    return trigger_spec if template == "same" else parse_cross_spec(template)


def build_stack_configurations(
    trigger_timeframes: Sequence[str],
    trigger_specs: Sequence[CrossSpec],
    trigger_confirmations: Sequence[str],
    include_single_context: bool = True,
    include_double_context: bool = True,
) -> list[StackConfiguration]:
    configurations: list[StackConfiguration] = []
    seen: set[tuple[Any, ...]] = set()

    def append(
        trigger_timeframe: str,
        trigger_spec: CrossSpec,
        trigger_confirmation: str,
        context_timeframes: tuple[str, ...],
        context_specs: tuple[CrossSpec, ...],
    ) -> None:
        key = (
            trigger_timeframe,
            trigger_spec,
            trigger_confirmation,
            context_timeframes,
            context_specs,
        )
        if key in seen:
            return
        seen.add(key)
        configurations.append(
            StackConfiguration(
                config_id=len(configurations),
                timeframe=trigger_timeframe,
                trigger_spec=trigger_spec,
                trigger_confirmation=trigger_confirmation,
                context_timeframes=context_timeframes,
                context_specs=context_specs,
            )
        )

    for trigger_timeframe in trigger_timeframes:
        available_contexts = context_timeframes_for(trigger_timeframe)
        for trigger_spec in trigger_specs:
            for trigger_confirmation in trigger_confirmations:
                append(
                    trigger_timeframe,
                    trigger_spec,
                    trigger_confirmation,
                    (),
                    (),
                )
                if include_single_context:
                    for context_timeframe in available_contexts:
                        for template in DEFAULT_CONTEXT_TEMPLATES:
                            append(
                                trigger_timeframe,
                                trigger_spec,
                                trigger_confirmation,
                                (context_timeframe,),
                                (
                                    resolve_context_template(
                                        template,
                                        trigger_spec,
                                    ),
                                ),
                            )
                if include_double_context and len(available_contexts) >= 2:
                    for context_pair in itertools.combinations(
                        available_contexts,
                        2,
                    ):
                        for templates in DEFAULT_DOUBLE_CONTEXT_TEMPLATES:
                            append(
                                trigger_timeframe,
                                trigger_spec,
                                trigger_confirmation,
                                context_pair,
                                tuple(
                                    resolve_context_template(
                                        template,
                                        trigger_spec,
                                    )
                                    for template in templates
                                ),
                            )
    return configurations


def combine_alignment_masks(
    trigger_mask: np.ndarray,
    context_masks: Sequence[np.ndarray],
) -> np.ndarray:
    combined = trigger_mask.copy()
    for context_mask in context_masks:
        combined &= context_mask
    return combined


def evaluate_pair(
    instrument: str,
    frame: pd.DataFrame,
    configurations: Sequence[StackConfiguration],
    path_windows_min: Sequence[int],
    barrier_pips: Sequence[float],
    cube: entry_quality.EntryAggregateCube,
    development_fraction: float,
    validation_fraction: float,
    minimum_pair_events: int,
) -> dict[str, Any]:
    pip_size = crossover.infer_pip_size(instrument)
    proxy_spread, spread_sample_count = crossover.estimate_spread_pips(
        frame,
        pip_size,
    )
    required_specs: dict[str, set[CrossSpec]] = {}
    for config in configurations:
        required_specs.setdefault(config.timeframe, set()).add(config.trigger_spec)
        for timeframe, spec in zip(
            config.context_timeframes,
            config.context_specs,
        ):
            required_specs.setdefault(timeframe, set()).add(spec)
    bar_cache = crossover.prepare_bar_cache(frame, required_specs)
    ma_cache: dict[str, dict[tuple[str, int], np.ndarray]] = {}
    for timeframe, specs in required_specs.items():
        required_ma = {
            (spec.fast_kind, spec.fast_window)
            for spec in specs
        } | {
            (spec.slow_kind, spec.slow_window)
            for spec in specs
        }
        ma_cache[timeframe] = crossover.prepare_ma_cache(
            bar_cache[timeframe],
            required_ma,
        )

    grouped: dict[tuple[str, CrossSpec], list[StackConfiguration]] = {}
    for config in configurations:
        grouped.setdefault((config.timeframe, config.trigger_spec), []).append(
            config
        )

    evaluated_configs = 0
    mapped_events = 0
    for (trigger_timeframe, trigger_spec), group in grouped.items():
        trigger_bars = bar_cache[trigger_timeframe]
        trigger_ma = ma_cache[trigger_timeframe]
        fast = trigger_ma[
            (trigger_spec.fast_kind, trigger_spec.fast_window)
        ]
        slow = trigger_ma[
            (trigger_spec.slow_kind, trigger_spec.slow_window)
        ]
        positions, directions = crossover.detect_crosses(fast, slow)
        evaluated_configs += len(group)
        if positions.size == 0:
            continue
        base_events = crossover.map_signal_entries(
            trigger_bars.index,
            positions,
            directions,
            crossover.TIMEFRAME_MINUTES[trigger_timeframe],
            frame.index,
        )
        if (
            base_events.entry_positions.size == 0
            or base_events.source_indices is None
        ):
            continue
        source_indices = base_events.source_indices
        decision_ns = (
            trigger_bars.index.asi8[positions]
            + crossover.TIMEFRAME_MINUTES[trigger_timeframe]
            * 60
            * 1_000_000_000
        )
        base_segments = crossover.split_for_entries(
            base_events.entry_positions,
            len(frame),
            development_fraction,
            validation_fraction,
        )
        paths = entry_quality.entry_path_outcomes(
            frame,
            base_events,
            path_windows_min,
            barrier_pips,
            pip_size,
            proxy_spread,
        )
        close = trigger_bars["close"].to_numpy(dtype=np.float64, copy=False)
        volume = trigger_bars["volume"].to_numpy(dtype=np.float64, copy=False)
        volume_median = (
            pd.Series(volume, copy=False)
            .rolling(window=20, min_periods=10)
            .median()
            .to_numpy(dtype=np.float64, copy=False)
        )
        trigger_mask_cache: dict[str, np.ndarray] = {}
        context_mask_cache: dict[tuple[str, CrossSpec], np.ndarray] = {}

        for config in group:
            if config.trigger_confirmation not in trigger_mask_cache:
                raw_trigger = crossover.apply_confirmation(
                    config.trigger_confirmation,
                    positions,
                    directions,
                    close,
                    volume,
                    volume_median,
                    fast,
                    slow,
                )
                trigger_mask_cache[config.trigger_confirmation] = raw_trigger[
                    source_indices
                ]
            context_masks: list[np.ndarray] = []
            for context_timeframe, context_spec in zip(
                config.context_timeframes,
                config.context_specs,
            ):
                cache_key = (context_timeframe, context_spec)
                if cache_key not in context_mask_cache:
                    context_bars = bar_cache[context_timeframe]
                    context_ma = ma_cache[context_timeframe]
                    raw_alignment = crossover.completed_higher_tf_alignment(
                        decision_ns,
                        directions,
                        context_bars.index,
                        crossover.TIMEFRAME_MINUTES[context_timeframe],
                        context_ma[
                            (
                                context_spec.fast_kind,
                                context_spec.fast_window,
                            )
                        ],
                        context_ma[
                            (
                                context_spec.slow_kind,
                                context_spec.slow_window,
                            )
                        ],
                    )
                    context_mask_cache[cache_key] = raw_alignment[source_indices]
                context_masks.append(context_mask_cache[cache_key])
            confirmation = combine_alignment_masks(
                trigger_mask_cache[config.trigger_confirmation],
                context_masks,
            )
            mapped_events += int(np.count_nonzero(confirmation))
            if not confirmation.any():
                continue
            for window_index in range(len(path_windows_min)):
                for segment_index in range(len(crossover.SEGMENTS)):
                    mask = confirmation & (base_segments == segment_index)
                    cube.update(
                        config.config_id,
                        window_index,
                        segment_index,
                        paths,
                        mask,
                        minimum_pair_events,
                    )
        del paths
        gc.collect()

    exact_columns = [
        "bid_open",
        "bid_high",
        "bid_low",
        "ask_open",
        "ask_high",
        "ask_low",
    ]
    exact = frame[exact_columns].notna().all(axis=1)
    return {
        "instrument": instrument,
        "rows": int(len(frame)),
        "start": frame.index.min().isoformat(),
        "end": frame.index.max().isoformat(),
        "exact_path_rows": int(exact.sum()),
        "exact_path_fraction": float(exact.mean()),
        "proxy_spread_pips": proxy_spread,
        "spread_sample_count": spread_sample_count,
        "evaluated_configs": evaluated_configs,
        "mapped_signal_events_across_configs": mapped_events,
    }


_WORKER_CONTEXT: dict[str, Any] | None = None


def initialize_worker(context: dict[str, Any]) -> None:
    global _WORKER_CONTEXT
    _WORKER_CONTEXT = context


def evaluate_worker(instrument: str) -> dict[str, Any]:
    if _WORKER_CONTEXT is None:
        raise RuntimeError("worker context was not initialized")
    context = _WORKER_CONTEXT
    configurations = context["configurations"]
    cube = entry_quality.EntryAggregateCube(
        len(configurations),
        len(context["path_windows_min"]),
        len(context["barrier_pips"]),
    )
    path = Path(context["source_dir"]) / f"{instrument}_M1.parquet"
    try:
        frame = entry_quality.read_entry_frame(
            path,
            context["start"],
            context["end"],
            context["exact_only"],
        )
        inventory = evaluate_pair(
            instrument,
            frame,
            configurations,
            context["path_windows_min"],
            context["barrier_pips"],
            cube,
            context["development_fraction"],
            context["validation_fraction"],
            context["minimum_pair_events"],
        )
        return {
            "instrument": instrument,
            "inventory": inventory,
            "aggregate": cube.payload(),
            "error": None,
        }
    except Exception as exc:
        return {
            "instrument": instrument,
            "inventory": None,
            "aggregate": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def attach_stack_metadata(
    selected: pd.DataFrame,
    configurations: Sequence[StackConfiguration],
) -> pd.DataFrame:
    if selected.empty:
        return selected
    metadata = pd.DataFrame(config.as_dict() for config in configurations)
    extra_columns = [
        column
        for column in metadata.columns
        if column
        not in {
            "timeframe",
            "fast_kind",
            "slow_kind",
            "fast_window",
            "slow_window",
            "confirmation",
        }
    ]
    return selected.merge(
        metadata[extra_columns],
        on="config_id",
        how="left",
        validate="one_to_one",
    )


def render_summary(manifest: dict[str, Any], selected: pd.DataFrame) -> str:
    lines = [
        "# Multi-Timeframe Crossover Stack Sweep",
        "",
        "A primary completed crossover triggers entry. Optional context crossover",
        "states use their last completed bars and may use different MA definitions.",
        "",
        "## Coverage",
        "",
        f"- Pairs loaded: {manifest['loaded_pair_count']}.",
        f"- Stack configurations: {manifest['configuration_count']}.",
        f"- Trigger timeframes: {', '.join(manifest['trigger_timeframes'])}.",
        f"- Context timeframes: {', '.join(manifest['observed_context_timeframes'])}.",
        f"- Development/validation candidates: {manifest['candidate_count']}.",
        f"- Holdout-confirmed candidates: {manifest['holdout_confirmed_count']}.",
        "",
        "## Top Candidates",
        "",
    ]
    if selected.empty:
        lines.append("No stack passed the development and validation entry-quality gate.")
    else:
        columns = [
            "timeframe",
            "trigger_cross",
            "trigger_confirmation",
            "stack_depth",
            "context_1_timeframe",
            "context_1_cross",
            "context_2_timeframe",
            "context_2_cross",
            "path_window_min",
            "barrier_pips",
            "selection_score",
            "validation_first_touch_win_rate",
            "holdout_first_touch_win_rate",
            "holdout_passed",
        ]
        lines.append(crossover.markdown_table(selected[columns].head(20)))
    lines.extend(
        [
            "",
            "This remains an overlapping event study, not account-equity P/L.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=crossover.DEFAULT_SOURCE_DIR,
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--instrument", default="all")
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument(
        "--trigger-timeframes",
        default=",".join(DEFAULT_TRIGGER_TIMEFRAMES),
    )
    parser.add_argument(
        "--trigger-specs",
        default=",".join(DEFAULT_TRIGGER_SPECS),
    )
    parser.add_argument(
        "--trigger-confirmations",
        default=",".join(DEFAULT_TRIGGER_CONFIRMATIONS),
    )
    parser.add_argument("--no-single-context", action="store_true")
    parser.add_argument("--no-double-context", action="store_true")
    parser.add_argument(
        "--path-windows-min",
        type=entry_quality.parse_csv_floats,
        default=[5, 15, 30, 60],
    )
    parser.add_argument(
        "--barrier-pips",
        type=entry_quality.parse_csv_floats,
        default=[2, 3, 5, 8],
    )
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--exact-only", action="store_true")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--development-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--minimum-segment-events", type=int, default=25)
    parser.add_argument("--minimum-pairs", type=int, default=10)
    parser.add_argument("--minimum-pair-events", type=int, default=5)
    parser.add_argument("--minimum-resolved-rate", type=float, default=0.25)
    parser.add_argument("--top-n", type=int, default=1000)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started = entry_quality.utc_now()
    start = crossover.parse_utc(args.start or "")
    end = crossover.parse_utc(args.end or "")
    trigger_timeframes = crossover.parse_csv_strings(args.trigger_timeframes)
    invalid_timeframes = sorted(
        set(trigger_timeframes) - set(TIMEFRAME_HIERARCHY)
    )
    if invalid_timeframes:
        raise ValueError(
            f"Unsupported trigger timeframes: {', '.join(invalid_timeframes)}"
        )
    trigger_specs = parse_cross_specs(args.trigger_specs)
    trigger_confirmations = crossover.parse_csv_strings(
        args.trigger_confirmations
    )
    invalid_confirmations = sorted(
        set(trigger_confirmations)
        - {"cross_only", "slow_slope", "dual_slope", "slow_slope_volume"}
    )
    if invalid_confirmations:
        raise ValueError(
            f"Unsupported trigger confirmations: {', '.join(invalid_confirmations)}"
        )
    path_windows_min = [int(value) for value in args.path_windows_min]
    if any(float(value) != int(value) for value in args.path_windows_min):
        raise ValueError("path windows must be whole minutes")
    barrier_pips = [float(value) for value in args.barrier_pips]
    configurations = build_stack_configurations(
        trigger_timeframes,
        trigger_specs,
        trigger_confirmations,
        include_single_context=not args.no_single_context,
        include_double_context=not args.no_double_context,
    )
    instruments = crossover.discover_instruments(
        args.source_dir,
        args.instrument,
        args.max_pairs,
    )
    output_dir = args.output_dir or (
        DEFAULT_REPORT_ROOT
        / f"all68_stack_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)

    context = {
        "source_dir": str(args.source_dir.resolve()),
        "configurations": configurations,
        "path_windows_min": path_windows_min,
        "barrier_pips": barrier_pips,
        "start": start,
        "end": end,
        "exact_only": bool(args.exact_only),
        "development_fraction": args.development_fraction,
        "validation_fraction": args.validation_fraction,
        "minimum_pair_events": args.minimum_pair_events,
    }
    cube = entry_quality.EntryAggregateCube(
        len(configurations),
        len(path_windows_min),
        len(barrier_pips),
    )
    inventories: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    workers = max(1, min(args.workers, len(instruments)))
    completed = 0
    if workers == 1:
        initialize_worker(context)
        results = (evaluate_worker(instrument) for instrument in instruments)
        for result in results:
            completed += 1
            if result["error"]:
                errors.append(
                    {"instrument": result["instrument"], "error": result["error"]}
                )
            else:
                cube.merge_payload(result["aggregate"])
                inventories.append(result["inventory"])
            print(
                f"[{completed}/{len(instruments)}] {result['instrument']} "
                f"{'ERROR ' + result['error'] if result['error'] else 'ok'}",
                flush=True,
            )
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers,
            initializer=initialize_worker,
            initargs=(context,),
        ) as executor:
            futures = {
                executor.submit(evaluate_worker, instrument): instrument
                for instrument in instruments
            }
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                completed += 1
                if result["error"]:
                    errors.append(
                        {
                            "instrument": result["instrument"],
                            "error": result["error"],
                        }
                    )
                else:
                    cube.merge_payload(result["aggregate"])
                    inventories.append(result["inventory"])
                print(
                    f"[{completed}/{len(instruments)}] {result['instrument']} "
                    f"{'ERROR ' + result['error'] if result['error'] else 'ok'}",
                    flush=True,
                )

    aggregate = cube.frame(
        configurations,
        path_windows_min,
        barrier_pips,
    )
    selected = entry_quality.build_selected_candidates(
        aggregate,
        args.minimum_segment_events,
        args.minimum_pairs,
        args.minimum_resolved_rate,
    )
    selected = attach_stack_metadata(selected, configurations)
    aggregate_path = output_dir / "stack_entry_quality_aggregate.parquet"
    selected_path = output_dir / "stack_candidates.csv"
    inventory_path = output_dir / "data_inventory.csv"
    aggregate.to_parquet(aggregate_path, index=False)
    selected.head(args.top_n).to_csv(selected_path, index=False)
    pd.DataFrame(inventories).sort_values("instrument").to_csv(
        inventory_path,
        index=False,
    )
    observed_context_timeframes = sorted(
        {
            timeframe
            for config in configurations
            for timeframe in config.context_timeframes
        },
        key=lambda item: crossover.TIMEFRAME_MINUTES[item],
    )
    depth_counts = {
        str(depth): sum(
            len(config.context_timeframes) == depth
            for config in configurations
        )
        for depth in (0, 1, 2)
    }
    manifest = {
        "schema_version": 1,
        "run_type": "multitimeframe_crossover_stack_entry_quality",
        "started_at_utc": started,
        "finished_at_utc": entry_quality.utc_now(),
        "source_dir": str(args.source_dir.resolve()),
        "requested_pair_count": len(instruments),
        "loaded_pair_count": len(inventories),
        "load_errors": errors,
        "configuration_count": len(configurations),
        "observed_configuration_count": (
            int(aggregate["config_id"].nunique()) if not aggregate.empty else 0
        ),
        "stack_depth_counts": depth_counts,
        "workers": workers,
        "trigger_timeframes": trigger_timeframes,
        "observed_context_timeframes": observed_context_timeframes,
        "trigger_specs": [spec.label for spec in trigger_specs],
        "trigger_confirmations": trigger_confirmations,
        "context_templates": list(DEFAULT_CONTEXT_TEMPLATES),
        "double_context_templates": [
            list(item) for item in DEFAULT_DOUBLE_CONTEXT_TEMPLATES
        ],
        "path_windows_min": path_windows_min,
        "barrier_pips": barrier_pips,
        "exact_only": bool(args.exact_only),
        "date_range": {
            "start": start.isoformat() if start is not None else None,
            "end": end.isoformat() if end is not None else None,
        },
        "splits": {
            "development_fraction": args.development_fraction,
            "validation_fraction": args.validation_fraction,
            "holdout_fraction": (
                1.0 - args.development_fraction - args.validation_fraction
            ),
        },
        "selection_gates": {
            "minimum_segment_events": args.minimum_segment_events,
            "minimum_pairs": args.minimum_pairs,
            "minimum_pair_events": args.minimum_pair_events,
            "minimum_resolved_rate": args.minimum_resolved_rate,
            "same_as_entry_quality_sweep": True,
        },
        "candidate_count": int(len(selected)),
        "holdout_confirmed_count": (
            int(selected["holdout_passed"].sum()) if not selected.empty else 0
        ),
        "method": {
            "primary_signal": "completed trigger-timeframe crossover",
            "context_state": (
                "last completed context-timeframe MA state at trigger decision"
            ),
            "entry": "next observed M1 open, maximum mapping delay 2 minutes",
            "path": "executable bid/ask M1 extrema",
            "same_candle_barrier_collision": "loss first",
            "overlapping_events": True,
        },
        "outputs": {
            "aggregate": str(aggregate_path.resolve()),
            "candidates": str(selected_path.resolve()),
            "inventory": str(inventory_path.resolve()),
        },
        "top_candidates": crossover.dataframe_records(selected, limit=20),
    }
    crossover.atomic_write_json(output_dir / "manifest.json", manifest)
    (output_dir / "SUMMARY.md").write_text(
        render_summary(manifest, selected),
        encoding="utf-8",
    )
    print(json.dumps(manifest, indent=2, default=str), flush=True)
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
