import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parent
FORBIDDEN_ROOTS = {"requests", "http", "urllib", "socket", "oandapyV20", "openai"}
FORBIDDEN_NAMES = {"oanda_gpt_training_strategy_manager", "oanda_broker_style_portfolio_replay"}


def test_stage_c_sources_have_no_live_or_advisor_imports():
    violations = []
    for path in ROOT.glob("*.py"):
        if path.name.startswith("test_"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [item.name for item in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            for name in names:
                root = name.split(".")[0]
                if root in FORBIDDEN_ROOTS or name in FORBIDDEN_NAMES:
                    violations.append(f"{path.name}:{name}")
    assert not violations, "offline isolation violation: " + ", ".join(violations)


def test_stage_c_powershell_launchers_have_no_live_or_advisor_commands():
    forbidden = ("invoke-webrequest", "invoke-restmethod", "curl", "oanda", "openai", "gpt")
    violations = []
    for path in ROOT.glob("*.ps1"):
        text = path.read_text(encoding="utf-8").lower()
        for token in forbidden:
            if token in text:
                violations.append(f"{path.name}:{token}")
    assert not violations, "offline isolation violation: " + ", ".join(violations)
