from __future__ import annotations

import oanda_advisor_account_manager_auto as advisor
import oanda_gpt_exp_account_manager as demo
import oanda_gpt_prod_live_account_manager as live_prod


def test_demo_manager_inherits_news_watch_and_technical_gate() -> None:
    assert issubclass(demo.BroadNewsForexManager, live_prod.LiveGPTProdManager)
    assert hasattr(demo.BroadNewsForexManager, "run_news_watch_scan")
    assert hasattr(demo.BroadNewsForexManager, "run_news_watch_technical_scan")
    assert hasattr(demo.BroadNewsForexManager, "apply_news_watch_technical_entry_gate")


def test_demo_config_fully_enables_practice_execution() -> None:
    live_prod.apply_live_runtime_overrides()
    base = advisor.BotConfig.load()

    cfg = demo.build_exp_config(base, execute_requested=True)

    assert cfg.oanda_env == "practice"
    assert cfg.oanda_account_id.endswith("-005")
    assert cfg.account_lane == "gpt_exp"
    assert cfg.execute_trades is True
    assert cfg.allow_live is False
    assert cfg.event_scanner_enabled is True
    assert cfg.event_trigger_gpt_enabled is True
    assert cfg.event_scout_trades_enabled is False
    assert cfg.event_candle_granularity == "M1"
    assert cfg.event_windows_minutes == [1, 3, 5, 10, 15]
    assert cfg.min_non_usd_candidates == 0


def test_demo_config_still_supports_explicit_dry_run() -> None:
    base = advisor.BotConfig.load()

    cfg = demo.build_exp_config(base, execute_requested=False)

    assert cfg.execute_trades is False
    assert cfg.allow_live is False


def test_demo_parser_exposes_manual_news_watch() -> None:
    args = demo.build_arg_parser().parse_args(["--news-watch-now", "--once"])

    assert args.news_watch_now is True
    assert args.once is True


def test_demo_prompt_uses_news_as_watch_only() -> None:
    assert "news is watch-only context" in demo.EXP_SYSTEM_PROMPT_APPEND
    assert "There is no USD/non-USD quota" in demo.EXP_SYSTEM_PROMPT_APPEND
    assert "expected_R >= 1.0" in demo.EXP_SYSTEM_PROMPT_APPEND
