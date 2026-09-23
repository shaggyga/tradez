from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
sys.path.insert(0, str(ENGINE_ROOT))

from src.backtest import run_backtest
from src.audit import write_horizon_integrity_report, write_top_ev_failure_diagnostics
from src.arima_baseline import run_arima_baseline
from src.common import available_m1_pairs, load_config, tier_for, utc_now_stamp, write_json
from src.dataset import build_dataset
from src.forecast_simplification import run_forecast_simplification
from src.labeling import run_label_audit
from src.htf_corrected_rebuild import run_corrected_h1_rebuild
from src.htf_rotation_validation import run_htf_rotation_validation
from src.htf_validation_replay import run_htf_validation_replay
from src.mandatory_model_launcher import run_mandatory_model_validation
from src.post_fail_diagnostic import run_post_fail_diagnostic
from src.research_suite import run_research_suite
from src.sarima_sweep_launcher import run_full_sarima_sweep
from src.signal_regression_audit import run_signal_regression_audit
from src.surface import train_surface_models
from src.summary import write_final_research_summary
from src.train import train_models
from src.two_pending_oco_breakout import run_two_pending_oco_breakout
from src.unified_forecast import run_unified_forecast
from src.unified_signal_benchmark import run_unified_signal_benchmark
from src.validation_replay import run_validation_replay

warnings.filterwarnings("ignore", category=Warning, message="DataFrame is highly fragmented.*")


def make_run_dir(cfg: dict, name: str | None = None) -> Path:
    reports_root = PROJECT_ROOT / cfg["reports_dir"]
    run_name = name or f"run_{utc_now_stamp()}"
    out = reports_root / run_name
    out.mkdir(parents=True, exist_ok=True)
    return out


def run_all(args: argparse.Namespace) -> None:
    cfg = load_config(args.config)
    if getattr(args, "quick_surface_smoke", False):
        cfg["surface"]["feature_families"] = ["A_m1_price_action", "E_full_session_htf_context"]
        cfg["surface"]["model_names"] = ["simple_baseline", "tree_gradient_boosting"]
    if getattr(args, "write_all_candidates", False):
        cfg["surface"]["write_all_candidates"] = True
    if getattr(args, "chunk_by_tier", False) or getattr(args, "pair_chunk_size", None):
        run_chunked(args, cfg)
        return
    run_dir = make_run_dir(cfg, args.run_name)
    dataset_path = build_dataset(
        cfg,
        run_dir,
        start=args.start,
        end=args.end,
        tier=args.tier,
        pairs=args.pairs.split(",") if args.pairs else None,
        max_rows_per_pair=args.max_rows_per_pair,
    )
    model_path = train_surface_models(cfg, dataset_path, run_dir)
    summary_path = run_dir / "surface_sweep_report.json"
    md_summary, json_summary = write_final_research_summary(run_dir, cfg)
    write_json(run_dir / "run_complete.json", {
        "run_dir": str(run_dir),
        "dataset_path": str(dataset_path),
        "model_path": str(model_path),
        "surface_sweep_report_path": str(summary_path),
        "forecast_surface_path": str(run_dir / "forecast_surface.csv"),
        "final_research_summary_md": str(md_summary),
        "final_research_summary_json": str(json_summary),
    })
    print(f"run_dir={run_dir}")
    print(f"dataset={dataset_path}")
    print(f"model={model_path}")
    print(f"summary={summary_path}")


def _chunk_specs(args: argparse.Namespace, cfg: dict) -> list[tuple[str, list[str]]]:
    explicit = args.pairs.split(",") if args.pairs else None
    pairs = explicit or available_m1_pairs(cfg)
    if args.tier != "all" and not explicit:
        pairs = [p for p in pairs if tier_for(p, cfg) == args.tier]
    if getattr(args, "chunk_by_tier", False):
        specs = []
        for tier in ["tier1", "tier2", "tier3"]:
            tier_pairs = [p for p in pairs if tier_for(p, cfg) == tier]
            if tier_pairs:
                specs.append((tier, tier_pairs))
        return specs
    size = int(getattr(args, "pair_chunk_size", 0) or len(pairs))
    return [(f"pairs_{i // size + 1:03d}", pairs[i:i + size]) for i in range(0, len(pairs), size)]


def run_chunked(args: argparse.Namespace, cfg: dict) -> None:
    parent = make_run_dir(cfg, args.run_name)
    chunks_dir = parent / "chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    summaries = []
    for name, pairs in _chunk_specs(args, cfg):
        chunk_dir = chunks_dir / name
        complete = chunk_dir / "chunk_complete.json"
        if complete.exists() and not getattr(args, "force", False):
            summaries.append({"chunk": name, "pairs": pairs, "status": "skipped_completed", "complete": str(complete)})
            continue
        chunk_dir.mkdir(parents=True, exist_ok=True)
        dataset_path = build_dataset(
            cfg,
            chunk_dir,
            start=args.start,
            end=args.end,
            tier="all",
            pairs=pairs,
            max_rows_per_pair=args.max_rows_per_pair,
        )
        model_path = train_surface_models(cfg, dataset_path, chunk_dir)
        payload = {
            "chunk": name,
            "pairs": pairs,
            "status": "completed",
            "dataset_path": str(dataset_path),
            "model_path": str(model_path),
            "surface_sweep_report_path": str(chunk_dir / "surface_sweep_report.json"),
            "forecast_surface_path": str(chunk_dir / "forecast_surface.csv"),
        }
        write_json(complete, payload)
        summaries.append(payload)
    write_json(parent / "chunked_run_manifest.json", {
        "run_dir": str(parent),
        "start": args.start,
        "end": args.end,
        "tier": args.tier,
        "chunk_by_tier": bool(getattr(args, "chunk_by_tier", False)),
        "pair_chunk_size": getattr(args, "pair_chunk_size", None),
        "chunks": summaries,
    })
    print(f"run_dir={parent}")
    print(f"manifest={parent / 'chunked_run_manifest.json'}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fresh M1 intrahour OANDA opportunity engine")
    parser.add_argument("--config", default=str(ENGINE_ROOT / "config.json"))
    parser.add_argument("--smoke", action="store_true", help="Run a small Tier 1 smoke pipeline.")
    parser.add_argument("--run-name")
    sub = parser.add_subparsers(dest="cmd")

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--start", default="2026-06-23")
        p.add_argument("--end", default="2026-07-07")
        p.add_argument("--tier", default="all", choices=["tier1", "tier2", "all"])
        p.add_argument("--pairs", help="Comma-separated pair list.")
        p.add_argument("--max-rows-per-pair", type=int)
        p.add_argument("--run-name")
        p.add_argument("--quick-surface-smoke", action="store_true", help="Validation-only narrow surface sweep.")
        p.add_argument("--chunk-by-tier", action="store_true", help="Run resumable chunks by tier.")
        p.add_argument("--pair-chunk-size", type=int, help="Run resumable chunks with N pairs each.")
        p.add_argument("--force", action="store_true", help="Rerun completed chunks.")
        p.add_argument("--write-all-candidates", action="store_true", help="Materialize all feature/model candidate surfaces.")

    def add_research_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--start", required=True)
        p.add_argument("--end", required=True)
        p.add_argument("--tier", default="tier1", choices=["tier1", "tier2", "all"])
        p.add_argument("--pairs", help="Comma-separated pair list.")
        p.add_argument("--max-rows-per-pair", type=int)
        p.add_argument("--run-name")

    p_all = sub.add_parser("all")
    add_common(p_all)
    p_build = sub.add_parser("build")
    add_common(p_build)
    p_train = sub.add_parser("train")
    p_train.add_argument("--dataset", required=True)
    p_train.add_argument("--output-dir")
    p_train.add_argument("--legacy-fixed-policy", action="store_true")
    p_backtest = sub.add_parser("backtest")
    p_backtest.add_argument("--dataset", required=True)
    p_backtest.add_argument("--model", required=True)
    p_backtest.add_argument("--output-dir")
    p_audit = sub.add_parser("audit-horizon")
    p_audit.add_argument("--surface", required=True)
    p_audit.add_argument("--dataset")
    p_audit.add_argument("--output-dir", required=True)
    p_audit.add_argument("--max-rows", type=int)
    p_failure = sub.add_parser("top-ev-failure")
    p_failure.add_argument("--surface", required=True)
    p_failure.add_argument("--output-dir", required=True)
    p_failure.add_argument("--max-rows", type=int)
    p_baselines = sub.add_parser("baselines")
    add_research_common(p_baselines)
    p_model_zoo = sub.add_parser("model-zoo")
    add_research_common(p_model_zoo)
    p_branch_oco = sub.add_parser("branch-oco")
    add_research_common(p_branch_oco)
    p_two_pending_oco = sub.add_parser("two-pending-oco-breakout")
    add_research_common(p_two_pending_oco)
    p_branch_currency = sub.add_parser("branch-currency-strength")
    add_research_common(p_branch_currency)
    p_branch_residual = sub.add_parser("branch-residual")
    add_research_common(p_branch_residual)
    p_branch_exit = sub.add_parser("branch-exit-alpha")
    add_research_common(p_branch_exit)
    p_research_suite = sub.add_parser("research-suite")
    add_research_common(p_research_suite)
    p_forecast_research = sub.add_parser("forecast-research")
    add_common(p_forecast_research)
    p_post_fail_diagnostic = sub.add_parser("post-fail-diagnostic")
    add_research_common(p_post_fail_diagnostic)
    p_signal_regression = sub.add_parser("signal-regression-audit")
    add_research_common(p_signal_regression)
    p_arima_baseline = sub.add_parser("arima-baseline")
    add_research_common(p_arima_baseline)
    p_sarima_sweep = sub.add_parser("sarima-sweep")
    add_research_common(p_sarima_sweep)
    p_sarima_sweep.add_argument("--labels-path", type=Path)
    p_sarima_sweep.add_argument("--workers", type=int, default=3)
    p_sarima_sweep.add_argument("--maxiter-screen", type=int, default=8)
    p_sarima_sweep.add_argument("--maxiter-final", type=int, default=35)
    p_sarima_sweep.add_argument("--refine-per-category", type=int, default=5)
    p_sarima_sweep.add_argument("--max-specs", type=int)
    p_sarima_sweep.add_argument("--quick", action="store_true")
    p_sarima_sweep.add_argument("--force-data", action="store_true")
    p_sarima_sweep.add_argument("--force-screen", action="store_true")
    p_sarima_sweep.add_argument("--force-refinement", action="store_true")
    p_mandatory_models = sub.add_parser("mandatory-model-validation")
    add_research_common(p_mandatory_models)
    p_mandatory_models.add_argument("--workers", type=int, default=3)
    p_mandatory_models.add_argument("--maxiter", type=int, default=35)
    p_mandatory_models.add_argument("--quick", action="store_true")
    p_validation_replay = sub.add_parser("validation-replay")
    add_research_common(p_validation_replay)
    p_unified_forecast = sub.add_parser(
        "unified-forecast",
        help="Build and validate the shadow-only M1/M30/H1/H4 next-hour forecast.",
    )
    p_unified_forecast.add_argument("--source-dir", type=Path, required=True)
    p_unified_forecast.add_argument("--data-root", type=Path, required=True)
    p_unified_forecast.add_argument(
        "--mode", choices=["all", "build", "train"], default="all"
    )
    p_unified_forecast.add_argument("--start")
    p_unified_forecast.add_argument("--end")
    p_unified_forecast.add_argument("--pairs", help="Comma-separated pair list.")
    p_unified_forecast.add_argument("--max-matrix-rows", type=int)
    p_unified_forecast.add_argument("--max-train-rows", type=int)
    p_unified_forecast.add_argument("--force", action="store_true")
    p_unified_forecast.add_argument("--run-name")
    p_unified_signal_benchmark = sub.add_parser(
        "unified-signal-benchmark",
        help="Benchmark and deduplicate every causal unified forecast feature.",
    )
    p_unified_signal_benchmark.add_argument("--matrix", type=Path)
    p_unified_signal_benchmark.add_argument("--feature-registry", type=Path)
    p_unified_signal_benchmark.add_argument("--horizon", type=int, default=60)
    p_unified_signal_benchmark.add_argument(
        "--correlation-threshold", type=float, default=0.995
    )
    p_unified_signal_benchmark.add_argument("--run-name")
    p_forecast_simplification = sub.add_parser(
        "forecast-simplification",
        help="Validation-freeze a compact version of the one-hour forecast.",
    )
    p_forecast_simplification.add_argument("--benchmark-report", type=Path)
    p_forecast_simplification.add_argument("--matrix", type=Path)
    p_forecast_simplification.add_argument("--horizon", type=int, default=60)
    p_forecast_simplification.add_argument("--run-name")
    p_htf_validation_replay = sub.add_parser("htf-validation-replay")
    add_research_common(p_htf_validation_replay)
    p_htf_validation_replay.add_argument("--development-root", type=Path)
    p_htf_validation_replay.add_argument("--evaluation-root", type=Path)
    p_htf_validation_replay.add_argument("--fixed-units", type=int, default=10000)
    p_htf_corrected_rebuild = sub.add_parser("htf-corrected-rebuild")
    add_research_common(p_htf_corrected_rebuild)
    p_htf_corrected_rebuild.add_argument("--fixed-units", type=int, default=10000)
    p_htf_corrected_rebuild.add_argument("--max-model-train-rows", type=int, default=100000)
    p_htf_rotation_validation = sub.add_parser("htf-rotation-validation")
    add_research_common(p_htf_rotation_validation)
    p_htf_rotation_validation.add_argument("--development-report", type=Path)
    p_htf_rotation_validation.add_argument(
        "--evaluation-reports",
        type=Path,
        nargs="+",
        help="Corrected rebuild report folders, in chronological order.",
    )
    p_htf_rotation_validation.add_argument(
        "--skip-path-grid",
        action="store_true",
        help="Run only the narrow 1/8/12 pacing smoke grid.",
    )
    p_label_audit = sub.add_parser("audit-labels")
    p_label_audit.add_argument("--start", required=True)
    p_label_audit.add_argument("--end", required=True)
    p_label_audit.add_argument("--tier", default="tier1", choices=["tier1", "tier2", "all"])
    p_label_audit.add_argument("--pairs", nargs="*", help="Pair list, either space-separated or comma-separated.")
    p_label_audit.add_argument("--max-rows-per-pair", type=int)
    p_label_audit.add_argument("--max-horizon", type=int)
    p_label_audit.add_argument("--run-name")
    p_label_audit.add_argument("--fixed-only", action="store_true", help="Disable dynamic ATR/spread/range TP/SL audit policies.")
    p_label_audit.add_argument("--cost-mode", default="base", choices=[
        "base",
        "spread_1p25x",
        "spread_1p5x",
        "spread_2x",
        "slippage_plus_0p1",
        "slippage_plus_0p3",
        "next_bar_entry",
        "one_bar_delay",
        "same_bar_adverse_first",
        "exclude_same_bar_ambiguous",
    ])
    p_label_audit.add_argument("--sample-minutes", type=int, help="Evaluate every Nth eligible minute for fast probes.")
    p_legacy = sub.add_parser("legacy-fixed-policy-all")
    add_common(p_legacy)

    args = parser.parse_args()
    if args.smoke:
        args.cmd = "all"
        args.start = "2026-07-03"
        args.end = "2026-07-07"
        args.tier = "tier1"
        args.pairs = "EUR_USD"
        args.max_rows_per_pair = 4000
        args.quick_surface_smoke = True
        args.chunk_by_tier = False
        args.pair_chunk_size = None
        args.force = False
        args.write_all_candidates = False
        args.run_name = args.run_name or f"smoke_{utc_now_stamp()}"

    cfg = load_config(args.config)
    if args.cmd in {None, "all"}:
        if args.cmd is None:
            # Full default uses all available pairs; use --tier tier1 for a narrower run.
            args.start = getattr(args, "start", "2026-06-23")
            args.end = getattr(args, "end", "2026-07-07")
            args.tier = getattr(args, "tier", "all")
            args.pairs = getattr(args, "pairs", None)
            args.max_rows_per_pair = getattr(args, "max_rows_per_pair", None)
            args.run_name = getattr(args, "run_name", None)
        run_all(args)
    elif args.cmd == "forecast-research":
        run_all(args)
    elif args.cmd == "build":
        run_dir = make_run_dir(cfg, args.run_name)
        path = build_dataset(cfg, run_dir, args.start, args.end, args.tier, args.pairs.split(",") if args.pairs else None, args.max_rows_per_pair)
        print(path)
    elif args.cmd == "train":
        output = Path(args.output_dir) if args.output_dir else Path(args.dataset).parent
        if args.legacy_fixed_policy:
            print(train_models(cfg, Path(args.dataset), output))
        else:
            model_path = train_surface_models(cfg, Path(args.dataset), output)
            write_final_research_summary(output, cfg)
            print(model_path)
    elif args.cmd == "backtest":
        output = Path(args.output_dir) if args.output_dir else Path(args.dataset).parent
        print(run_backtest(cfg, Path(args.dataset), Path(args.model), output))
    elif args.cmd == "audit-horizon":
        print(write_horizon_integrity_report(Path(args.surface), Path(args.output_dir), Path(args.dataset) if args.dataset else None, args.max_rows))
    elif args.cmd == "top-ev-failure":
        print(write_top_ev_failure_diagnostics(Path(args.surface), Path(args.output_dir), args.max_rows))
    elif args.cmd in {
        "baselines",
        "model-zoo",
        "branch-oco",
        "branch-currency-strength",
        "branch-residual",
        "branch-exit-alpha",
        "research-suite",
    }:
        prefix = "research_suite" if args.cmd == "research-suite" else args.cmd.replace("-", "_")
        run_dir = make_run_dir(cfg, args.run_name or f"{prefix}_{utc_now_stamp()}")
        pairs = args.pairs.split(",") if args.pairs else None
        out = run_research_suite(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            pairs=pairs,
            mode=args.cmd,
            max_rows_per_pair=args.max_rows_per_pair,
        )
        print(out)
    elif args.cmd == "two-pending-oco-breakout":
        run_dir = make_run_dir(cfg, args.run_name or f"two_pending_oco_breakout_{utc_now_stamp()}")
        pairs = args.pairs.split(",") if args.pairs else None
        out = run_two_pending_oco_breakout(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            pairs=pairs,
            max_rows_per_pair=args.max_rows_per_pair,
        )
        print(out)
    elif args.cmd == "post-fail-diagnostic":
        run_dir = make_run_dir(cfg, args.run_name or f"post_fail_diagnostic_{utc_now_stamp()}")
        pairs = args.pairs.split(",") if args.pairs else None
        out = run_post_fail_diagnostic(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            pairs=pairs,
            max_rows_per_pair=args.max_rows_per_pair,
        )
        print(out)
    elif args.cmd == "signal-regression-audit":
        run_dir = make_run_dir(cfg, args.run_name or f"signal_regression_audit_{utc_now_stamp()}")
        pairs = args.pairs.split(",") if args.pairs else None
        out = run_signal_regression_audit(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            pairs=pairs,
            max_rows_per_pair=args.max_rows_per_pair,
        )
        print(out)
    elif args.cmd == "arima-baseline":
        run_dir = make_run_dir(cfg, args.run_name or f"arima_baseline_{utc_now_stamp()}")
        pairs = args.pairs.split(",") if args.pairs else None
        out = run_arima_baseline(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            pairs=pairs,
            max_rows_per_pair=args.max_rows_per_pair,
        )
        print(out)
    elif args.cmd == "sarima-sweep":
        run_dir = make_run_dir(cfg, args.run_name or f"sarima_sweep_{utc_now_stamp()}")
        out = run_full_sarima_sweep(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            pairs=args.pairs,
            labels_path=args.labels_path,
            workers=args.workers,
            maxiter_screen=args.maxiter_screen,
            maxiter_final=args.maxiter_final,
            refine_per_category=args.refine_per_category,
            max_specs=args.max_specs,
            quick=args.quick,
            force_data=args.force_data,
            force_screen=args.force_screen,
            force_refinement=args.force_refinement,
        )
        print(out)
    elif args.cmd == "validation-replay":
        run_dir = make_run_dir(cfg, args.run_name or f"validation_replay_{utc_now_stamp()}")
        pairs = args.pairs.split(",") if args.pairs else None
        out = run_validation_replay(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            pairs=pairs,
            max_rows_per_pair=args.max_rows_per_pair,
        )
        print(out)
    elif args.cmd == "unified-forecast":
        run_dir = make_run_dir(
            cfg, args.run_name or f"unified_forecast_{utc_now_stamp()}"
        )
        out = run_unified_forecast(
            cfg,
            run_dir,
            source_dir=args.source_dir,
            data_root=args.data_root,
            mode=args.mode,
            start=args.start,
            end=args.end,
            pairs=args.pairs.split(",") if args.pairs else None,
            max_matrix_rows=args.max_matrix_rows,
            max_train_rows=args.max_train_rows,
            force=args.force,
        )
        print(out)
    elif args.cmd == "unified-signal-benchmark":
        run_dir = make_run_dir(
            cfg, args.run_name or f"unified_signal_benchmark_{utc_now_stamp()}"
        )
        out = run_unified_signal_benchmark(
            cfg,
            run_dir,
            matrix_path=args.matrix,
            feature_registry_path=args.feature_registry,
            horizon=args.horizon,
            correlation_threshold=args.correlation_threshold,
        )
        print(out)
    elif args.cmd == "forecast-simplification":
        run_dir = make_run_dir(
            cfg, args.run_name or f"forecast_simplification_{utc_now_stamp()}"
        )
        out = run_forecast_simplification(
            cfg,
            run_dir,
            benchmark_report_path=args.benchmark_report,
            matrix_path=args.matrix,
            horizon=args.horizon,
        )
        print(out)
    elif args.cmd == "mandatory-model-validation":
        run_dir = make_run_dir(cfg, args.run_name or f"mandatory_model_validation_{utc_now_stamp()}")
        out = run_mandatory_model_validation(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            pairs=args.pairs.split(",") if args.pairs else None,
            workers=args.workers,
            maxiter=args.maxiter,
            quick=args.quick,
        )
        print(out)
    elif args.cmd == "htf-validation-replay":
        run_dir = make_run_dir(cfg, args.run_name or f"htf_validation_replay_{utc_now_stamp()}")
        out = run_htf_validation_replay(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            development_root=args.development_root,
            evaluation_root=args.evaluation_root,
            fixed_units=args.fixed_units,
        )
        print(out)
    elif args.cmd == "htf-corrected-rebuild":
        run_dir = make_run_dir(cfg, args.run_name or f"htf_corrected_rebuild_{utc_now_stamp()}")
        out = run_corrected_h1_rebuild(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            pairs=args.pairs.split(",") if args.pairs else None,
            max_rows_per_pair=args.max_rows_per_pair,
            fixed_units=args.fixed_units,
            max_train_rows=args.max_model_train_rows,
        )
        print(out)
    elif args.cmd == "htf-rotation-validation":
        run_dir = make_run_dir(cfg, args.run_name or f"htf_rotation_validation_{utc_now_stamp()}")
        out = run_htf_rotation_validation(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            pairs=args.pairs.split(",") if args.pairs else None,
            development_report=args.development_report,
            evaluation_reports=args.evaluation_reports,
            skip_path_grid=args.skip_path_grid,
        )
        print(out)
    elif args.cmd == "audit-labels":
        run_dir = make_run_dir(cfg, args.run_name or f"label_audit_{utc_now_stamp()}")
        out = run_label_audit(
            cfg,
            run_dir,
            start=args.start,
            end=args.end,
            tier=args.tier,
            pairs=args.pairs,
            max_rows_per_pair=args.max_rows_per_pair,
            max_horizon=args.max_horizon,
            include_dynamic_policies=not args.fixed_only,
            cost_mode=args.cost_mode,
            sample_minutes=args.sample_minutes,
        )
        print(out)
    elif args.cmd == "legacy-fixed-policy-all":
        run_dir = make_run_dir(cfg, args.run_name)
        dataset_path = build_dataset(cfg, run_dir, args.start, args.end, args.tier, args.pairs.split(",") if args.pairs else None, args.max_rows_per_pair)
        model_path = train_models(cfg, dataset_path, run_dir)
        print(run_backtest(cfg, dataset_path, model_path, run_dir))


if __name__ == "__main__":
    main()
