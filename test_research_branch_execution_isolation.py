"""Guard the new source/response research branch from operational imports.

This is intentionally a simple, conservative source-level sentinel.  The new
contracts may be consumed by research reports and shadow workers, but not by
the lifecycle, authorization, or execution boundary while their evidence is
unconfirmed.
"""

from pathlib import Path


ROOT = Path(__file__).resolve().parent

RESEARCH_TOKENS = (
    "currency_state_official_context",
    "official_fact_adapter",
    "source_response_analog_selector",
    "currency_state_after_cost_counterfactual",
    "prospective_event_response",
    "counterfactual_sim_gym",
    "sequential_portfolio_replay",
)

OPERATIONAL_FILES = (
    "oanda_technical_account_manager_auto.py",
    "oanda_execution_policy.py",
    "oanda_hypothesis_lifecycle.py",
    "oanda_prospective_governance.py",
    "oanda_practice_top_signal_executor.py",
    "oanda_practice_all_pairs_opportunity_scalper.py",
    "oanda_always_on_supervisor.ps1",
    "oanda_local_news_supervisor.ps1",
)


def test_unconfirmed_research_branch_is_not_wired_to_operational_boundary():
    violations = []
    for relative in OPERATIONAL_FILES:
        path = ROOT / relative
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="replace").lower()
        for token in RESEARCH_TOKENS:
            if token in text:
                violations.append(f"{relative}: {token}")

    assert violations == []
