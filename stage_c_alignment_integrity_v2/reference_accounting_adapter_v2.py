"""Synthetic two-arm ledger using the existing reviewed Decimal replay engine.

This adapter supplies declared quotes/actions to the predecessor; it implements
no pricing, conversion, execution, financing, or margin engine of its own.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from decimal import Decimal, localcontext
import hashlib
import importlib
import importlib.machinery
import json
from pathlib import Path
import sys
import types
from typing import Any

ROOT = Path(__file__).resolve().parent
DEFAULT_TRAD = ROOT.parent / "trad"
TIER = "synthetic_executable_quotes.v2"
ARMS = ("fixed_hold_fixture", "rotation_fixture")
REFERENCE_SOURCE_SHA256 = "52fd04c6b087df4be775e490002afd0744caeb245604b8f577ab0fcff41de8f3"
LEGACY_SOURCE_SHA256 = "e1201fdd3387825c9295f13045aeb00e2f87cce9ff819e789faf922ea2e514a6"
PACKAGE_SOURCE_SHA256 = "e885437bab2a9de53fd7884bdc1d16c16e6bea31b3c78f548afbc44d24050c77"
ABSENT_IMPORT_PATHS = ("src/__init__.py", "src/__init__.pyc", "src.py", "src.pyc",
                       "src/forex_system.py", "src/forex_system.pyc",
                       "src/forex_system/research/__init__.py", "src/forex_system/research/__init__.pyc",
                       "src/forex_system/research.py", "src/forex_system/research.pyc")


def reference_dependency_identity(trad_root: Path):
    expected = {"oanda_curve_management_replay_v1.py": REFERENCE_SOURCE_SHA256,
                "src/forex_system/research/sequential_portfolio_replay_v1.py": LEGACY_SOURCE_SHA256,
                "src/forex_system/__init__.py": PACKAGE_SOURCE_SHA256}
    for name, digest in expected.items():
        path = trad_root / name
        if not path.is_file() or sha256(path) != digest:
            raise ValueError("unreviewed_reference_dependency_refused_before_import:" + name)
    for name in ABSENT_IMPORT_PATHS:
        if (trad_root / name).exists() or (trad_root / name).is_symlink():
            raise ValueError("unreviewed_reference_dependency_refused_before_import:" + name)
    return {**expected, **{name: None for name in ABSENT_IMPORT_PATHS}}


def load_reference(trad_root: Path):
    reference_dependency_identity(trad_root)
    source = (trad_root / "oanda_curve_management_replay_v1.py").resolve(strict=True)
    legacy_source = (trad_root / "src" / "forex_system" / "research" / "sequential_portfolio_replay_v1.py").resolve(strict=True)
    # Verify the reviewed inert import closure before executing either module.
    # A changed dependency requires review/versioning, not silent acceptance.
    if sha256(source) != REFERENCE_SOURCE_SHA256 or sha256(legacy_source) != LEGACY_SOURCE_SHA256:
        raise ValueError("unreviewed_reference_dependency_refused_before_import")
    # Reject a foreign cached package before import can use its search path.
    for name, relative in (("src", "src"), ("src.forex_system", "src/forex_system"),
                           ("src.forex_system.research", "src/forex_system/research")):
        cached = sys.modules.get(name)
        if cached is not None and (not getattr(cached, "__path__", ()) or
                any(Path(path).resolve() != (trad_root / relative).resolve()
                    for path in cached.__path__)):
            raise ValueError("cached_reference_package_from_unexpected_root:" + name)
    sys.path.insert(0, str(trad_root.resolve()))
    # Freeze namespace search locations; an unrelated sys.path package must not
    # override a deliberately absent initializer in the reviewed source tree.
    for name, relative in (("src", "src"), ("src.forex_system.research", "src/forex_system/research")):
        if name == "src.forex_system.research":
            importlib.import_module("src.forex_system")
        if name not in sys.modules:
            namespace = types.ModuleType(name)
            namespace.__path__ = [str((trad_root / relative).resolve())]
            namespace.__package__ = name
            namespace.__spec__ = importlib.machinery.ModuleSpec(name, loader=None, is_package=True)
            namespace.__spec__.submodule_search_locations = namespace.__path__
            sys.modules[name] = namespace
            if "." in name:
                parent, child = name.rsplit(".", 1)
                setattr(sys.modules[parent], child, namespace)
    reference = importlib.import_module("oanda_curve_management_replay_v1")
    if Path(reference.__file__).resolve() != source:
        raise ValueError("reference engine imported from an unexpected source root")
    if Path(reference.legacy.__file__).resolve() != legacy_source:
        raise ValueError("reference dependency imported from an unexpected source root")
    return reference


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def synthetic_contract(reference) -> dict[str, Any]:
    return {
        "input_tier": TIER, "initial_capital_usd": "10000", "policy_source": "predeclared_synthetic_actions",
        "financing": "excluded_intraday_fixture_only", "margin": "not_modeled_no_margin_evidence",
        "capital_semantics": "initial_capital_plus_realized_pnl_not_a_broker_cash_account",
        "reference_config": {
            "schema_version": reference.CONFIG_SCHEMA, **reference.SAFETY, "account_currency": "USD",
            "maximum_open_positions": 1, "notional_usd": "1000",
            "metadata": {pair: {"base_currency": pair[:3], "quote_currency": pair[4:], "pip_size": pip, "unit_increment": 1}
                         for pair, pip in (("EUR_USD", ".0001"), ("GBP_JPY", ".01"))},
            "conversion_policy": "direct_or_inverse_executable_bid_ask_no_triangulation",
            "sizing_policy": "fixed_usd_notional_integer_base_units_at_decision",
            "curve_target_window_policy": "exact_only", "maximum_entry_spread_bps": "100",
            "maximum_holding_sec": 3600, "cadence_sec": 300, "execution_delay_sec": 60, "quote_max_age_sec": 60,
            "minimum_entry_cost_ratio": "1", "slippage_bps_per_leg": "0", "switch_incremental_hurdle_usd": "0",
            "legacy_policy": {},
        },
    }


def quote(pair: str, bid: str, ask: str, epoch: int) -> dict[str, Any]:
    return {"instrument": pair, "quote_id": f"synthetic:{pair}:{epoch}:{bid}:{ask}", "bid": bid, "ask": ask,
            "market_epoch": epoch, "available_epoch": epoch, "tradeable": True}


def quote_panel(epoch: int, eur: tuple[str, str], gbp_jpy: tuple[str, str], *, gbp_usd_ask: str = "2.00") -> dict[str, Any]:
    return {"EUR_USD": quote("EUR_USD", *eur, epoch), "GBP_JPY": quote("GBP_JPY", *gbp_jpy, epoch),
            "GBP_USD": quote("GBP_USD", "1.90", gbp_usd_ask, epoch),
            "USD_JPY": quote("USD_JPY", "100", "125", epoch)}


def fixture_frames() -> list[dict[str, Any]]:
    enter_eur = {"action": "enter", "instrument": "EUR_USD", "side": 1, "original_target_epoch": 1660}
    actions = [
        {ARMS[0]: enter_eur, ARMS[1]: enter_eur},
        {ARMS[0]: {"action": "hold"}, ARMS[1]: {"action": "rotate", "instrument": "GBP_JPY", "side": -1, "original_target_epoch": 1660}},
        {ARMS[0]: {"action": "exit"}, ARMS[1]: {"action": "exit"}},
        {ARMS[0]: {"action": "wait"}, ARMS[1]: {"action": "enter", "instrument": "GBP_JPY", "side": 1, "original_target_epoch": 2260}},
        {ARMS[0]: {"action": "wait"}, ARMS[1]: {"action": "exit"}},
    ]
    frames = []
    execution_prices = [(("1.01", "1.02"), ("200.00", "200.02")),
                        (("1.04", "1.05"), ("200.00", "200.02")),
                        (("1.06", "1.07"), ("197.98", "198.00")),
                        (("1.06", "1.07"), ("200.00", "200.02")),
                        (("1.06", "1.07"), ("198.02", "198.04"))]
    for index, epoch in enumerate((1000, 1300, 1600, 1900, 2200)):
        eur, gbp_jpy = execution_prices[index]
        frames.append({"input_tier": TIER, "decision_epoch": epoch, "execution_epoch": epoch + 60,
                       "decision_quotes": quote_panel(epoch, (".99", "1.00"), ("199.98", "200.00")),
                       "execution_quotes": quote_panel(epoch + 60, eur, gbp_jpy, gbp_usd_ask="3.01"),
                       "actions": deepcopy(actions[index])})
    return frames


def fixture_oracle() -> dict[str, Any]:
    return {
        "method": "independent_hand_calculation_at_each_execution_epoch",
        "columns": ["realized_balance_usd", "liquidation_equity_usd"],
        "fixed_hold_fixture": [["10000", "9990"], ["10000", "10020"], ["10040", "10040"], ["10040", "10040"], ["10040", "10040"]],
        "rotation_fixture": [["10000", "9990"], ["10020", "10019.90"], ["10028", "10028"], ["10028", "10027.90"], ["10018", "10018"]],
        "arithmetic": ["EUR sizing floor(1000/1.00)=1000; entry ask1.02; same-quote mark bid1.01 gives -10USD",
                       "hold EUR close bid1.06: 1000*(1.06-1.02)=40USD",
                       "rotation EUR close bid1.04: 1000*(1.04-1.02)=20USD",
                       "GBP sizing floor(1000/2.00)=500; later execution conversion ask3.01 does not resize",
                       "GBPJPY short bid200 to ask198: 500*(200-198)=1000JPY; sell profit via inverse ask125 gives8USD",
                       "GBPJPY long ask200.02 to bid198.02: 500*(198.02-200.02)=-1000JPY; buy loss via inverse bid100 gives-10USD",
                       "each GBPJPY immediate liquidation spread costs 500*0.02=10JPY; buy loss via bid100 costs0.10USD"],
    }


def _freeze_decision(reference, raw: dict[str, Any], quotes: dict[str, Any], epoch: int, config: dict[str, Any]) -> dict[str, Any]:
    action = raw.get("action")
    allowed = {"action", "instrument", "side", "original_target_epoch"} if action in {"enter", "rotate"} else {"action"}
    if set(raw) != allowed or action not in {"enter", "rotate", "hold", "exit", "wait"}:
        raise ValueError("explicit_action_schema_required_without_caller_sizing")
    decision = {**deepcopy(raw), "decision_epoch": epoch, "reason": "predeclared_synthetic_policy_fixture"}
    if action in {"enter", "rotate"}:
        units, sizing = reference.size_at_decision(raw["instrument"], quotes, epoch, config)
        decision.update({"base_units": units, "sizing": sizing, "candidate_id": f"synthetic:{raw['instrument']}:{epoch}"})
    return decision


def replay_synthetic(frames: list[dict[str, Any]], contract: dict[str, Any], *, trad_root: Path = DEFAULT_TRAD) -> dict[str, Any]:
    reference = load_reference(trad_root)
    if contract.get("input_tier") != TIER or any(frame.get("input_tier") != TIER for frame in frames):
        raise ValueError("actual_candle_execution_unsupported_explicit_synthetic_tier_required")
    if contract.get("financing") != "excluded_intraday_fixture_only" or contract.get("margin") != "not_modeled_no_margin_evidence":
        raise ValueError("unsupported_financing_or_margin_contract")
    if not frames or len(frames) > 100 or any(set(frame["actions"]) != set(ARMS) for frame in frames):
        raise ValueError("bounded_exact_two_arm_schedule_required")
    epochs = [reference.clock(frame["decision_epoch"]) for frame in frames]
    if epochs != sorted(set(epochs)) or frames[-1]["execution_epoch"] - frames[0]["decision_epoch"] > 3600:
        raise ValueError("strictly_ordered_intraday_fixture_required")
    config = reference.validate_config(contract["reference_config"])
    capital = reference.number(contract["initial_capital_usd"])
    if not capital > 0 or config["notional_usd"] > capital:
        raise ValueError("positive_capital_and_notional_within_initial_capital_required")
    with localcontext(reference.CTX):
        states = {arm: reference.flat_state() for arm in ARMS}
        ledger = []
        for frame in frames:
            for arm in ARMS:
                before = deepcopy(states[arm])
                decision = _freeze_decision(reference, frame["actions"][arm], frame["decision_quotes"], frame["decision_epoch"], config)
                try:
                    after, receipt = reference.apply_action(before, decision, frame["execution_quotes"], frame["execution_epoch"], config)
                except (ValueError, KeyError, TypeError) as exc:
                    # The predecessor raises on wait/hold position mismatches
                    # but returns refusals for execution failures. Normalize
                    # both into the same unchanged-state rejection ledger.
                    after = deepcopy(before)
                    receipt = {"status": "rejected", "reason": str(exc), "legs": [], "realized_delta_usd": Decimal(0)}
                mark = reference.liquidation_equity(after, frame["execution_quotes"], frame["execution_epoch"], config)
                states[arm] = after
                ledger.append({"arm": arm, "decision_epoch": frame["decision_epoch"], "execution_epoch": frame["execution_epoch"],
                               "decision": decision, "receipt": receipt, "before_state": before, "after_state": deepcopy(after),
                               "realized_balance_usd": capital + after["realized_usd"],
                               "liquidation_equity_usd": None if mark["equity_usd"] is None else capital + mark["equity_usd"],
                               "mark": mark})
        result = {"schema_version": "forex_reference_accounting_adapter.v2", "status": "synthetic_ledger_only_not_market_evidence",
                  "contract": deepcopy(contract), "frames": deepcopy(frames), "ledger": ledger,
                  "final_states": states, "accounting_source": "oanda_curve_management_replay_v1.py",
                  "source_hashes": {"adapter": sha256(Path(__file__)), "reference_engine": sha256(Path(reference.__file__)),
                                    "legacy_policy_dependency": sha256(Path(reference.legacy.__file__))},
                  "limitations": ["synthetic_quotes_and_predeclared_actions_only", "two_instruments_not_all68_execution",
                                  "no_financing_margin_liquidity_or_real_fill_claim", "no_strategy_performance_evidence",
                                  "actual_bid_ask_provenance_and_execution_adapter_remain_unimplemented"]}
    return reference.jsonable(result)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trad-root", type=Path, default=DEFAULT_TRAD)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reference = load_reference(args.trad_root)
    report = replay_synthetic(fixture_frames(), synthetic_contract(reference), trad_root=args.trad_root)
    oracle = fixture_oracle()
    for arm in ARMS:
        rows = [row for row in report["ledger"] if row["arm"] == arm]
        actual = [(Decimal(row["realized_balance_usd"]), Decimal(row["liquidation_equity_usd"])) for row in rows]
        expected = [(Decimal(balance), Decimal(equity)) for balance, equity in oracle[arm]]
        if actual != expected or any(row["receipt"]["status"] != "applied" for row in rows):
            raise ValueError("synthetic_reference_ledger_disagrees_with_hand_calculation")
    report["hand_calculation_oracle"] = {**oracle, "verified_event_arm_rows": 10}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, sort_keys=True, indent=2, allow_nan=False)
        handle.write("\n")
    print(json.dumps({"status": report["status"], "ledger_rows": len(report["ledger"]),
                      "final_realized_pnl_usd": {arm: state["realized_usd"] for arm, state in report["final_states"].items()},
                      "output": str(args.output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
