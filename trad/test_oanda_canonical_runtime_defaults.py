from pathlib import Path

try:
    import oanda_lane_promotion_fit as lane_promotion
    import oanda_runtime_audit as runtime_audit
    import oanda_signal_combination_fit as combination_fit
    import oanda_signal_combination_walkforward_benchmark as combination_benchmark
    import oanda_strategy_exit_fit_worker as exit_fit
except ModuleNotFoundError:
    from trad import oanda_lane_promotion_fit as lane_promotion
    from trad import oanda_runtime_audit as runtime_audit
    from trad import oanda_signal_combination_fit as combination_fit
    from trad import oanda_signal_combination_walkforward_benchmark as combination_benchmark
    from trad import oanda_strategy_exit_fit_worker as exit_fit


MODULES = (
    lane_promotion,
    runtime_audit,
    combination_fit,
    combination_benchmark,
    exit_fit,
)


def test_runtime_defaults_are_anchored_to_each_module_project_root() -> None:
    for module in MODULES:
        assert module.PROJECT_ROOT == Path(module.__file__).resolve().parent

    state_dir = lane_promotion.PROJECT_ROOT / "data" / "oanda_training_manager" / "state"
    assert lane_promotion.DEFAULT_STATE_DIR == state_dir
    assert runtime_audit.DEFAULT_ROOT == runtime_audit.PROJECT_ROOT / "data" / "oanda_training_manager"
    assert combination_fit.DEFAULT_STATE_DIR == state_dir
    assert combination_fit.DEFAULT_DATABASE == state_dir / "signal_combination_audit_v1.sqlite"
    assert combination_fit.DEFAULT_STATE == state_dir / "signal_combination_audit_v1.json"
    assert combination_benchmark.DEFAULT_DATABASE == state_dir / "signal_combination_audit_v1.sqlite"
    assert exit_fit.DEFAULT_ROOT == state_dir


def test_runtime_sources_do_not_reference_the_legacy_project_root() -> None:
    legacy_root = r"d:\forex\trad"
    for module in MODULES:
        source = Path(module.__file__).read_text(encoding="utf-8").lower()
        assert legacy_root not in source
