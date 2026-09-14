from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json

import pytest

import oanda_practice_trial_policy_v2 as policy

NOW = datetime(2026, 9, 9, 18, 0, tzinfo=timezone.utc).timestamp()


@pytest.fixture
def case():
    return dict(
        signal=dict(instrument="GBP_USD", side=1, reference_epoch=NOW-300, reference_price="1.2500",
                    original_target_epoch=NOW+3300, expected_terminal_price="1.2520", probability_up="0.60",
                    issued_epoch=NOW-290, available_epoch=NOW-289, forecast_sha256="a"*64,
                    source_authority={"can_place_orders": False, "proof_eligible": False}),
        quote=dict(instrument="GBP_USD", bid="1.2500", ask="1.2502", tradeable=True,
                   market_epoch=NOW-2, observed_epoch=NOW-1),
        account=dict(currency="USD", NAV="41.60", marginUsed="0", marginAvailable="41.60", marginRate="0.02",
                     openTradeCount=0, pendingOrderCount=0, observed_epoch=NOW-1),
        metadata=dict(name="GBP_USD", type="CURRENCY", marginRate="0.02", minimumTradeSize="1",
                      tradeUnitsPrecision=0, pipLocation=-4, displayPrecision=5,
                      maximumOrderUnits="100000000", maximumPositionSize="0"),
        home_conversions=dict(observed_epoch=NOW-1, rows=[]),
        candles=dict(instrument="GBP_USD", observed_epoch=NOW-1, source_sha256="b"*64,
                     rows=[dict(label_epoch=NOW-960+i*60, complete=True,
                                mid=dict(h="1.2502", l="1.2498", c="1.2500")) for i in range(15)]),
        session=dict(started_epoch=NOW-3600, start_nav_usd="41.60", stop_epoch=(NOW+47*3600),
                     loss_stop_latched=False, day_utc="2026-09-09", entries_today=0,
                     last_entry_epoch_by_instrument={}), decision_epoch=NOW)


def call(case):
    return policy.evaluate_entry(**case)


def position(case):
    return dict(trade_id="123", instrument="GBP_USD", currentUnits="100", price="1.2502",
                stop_loss_price="1.2492", original_target_epoch=NOW+3300, opened_epoch=NOW-250,
                observed_epoch=NOW-1, forecast_sha256="a"*64)


def manage(case, pos=None, quote="default", account="default", now=None):
    return policy.manage_position(pos or position(case), case["quote"] if quote == "default" else quote,
                                  case["account"] if account == "default" else account,
                                  session=case["session"], decision_epoch=NOW if now is None else now)


def test_small_account_actual_risk_sizing_and_broker_rounding(case):
    out = call(case)
    assert out["status"] == "available", out
    assert out["units"] == "201"
    assert out["stop_loss_price"] == "1.24920"
    assert out["entry_price_bound"] == "1.25022"
    assert Decimal(out["modeled_stop_loss_usd"]) <= Decimal("0.208")
    assert Decimal(out["modeled_new_margin_usd"]) <= Decimal("8.32")
    assert Decimal(out["expected_net_usd"]) > 0
    assert out["original_target_epoch"] == case["signal"]["original_target_epoch"]


def test_next_unit_would_exceed_loss_budget(case):
    out = call(case)
    units = Decimal(out["units"])
    one_loss = Decimal(out["modeled_stop_loss_usd"])/units
    assert (units+1)*one_loss > Decimal(out["risk_budget_usd"])


def test_margin_headroom_uses_actual_used_and_available(case):
    case["account"]["marginUsed"] = "8.30"
    assert call(case)["reason"] == "risk_sized_units_below_broker_minimum"
    case["account"]["marginUsed"] = "8.32"
    assert call(case)["reason"] == "account_risk_or_margin_capacity_unavailable"
    case["account"]["marginUsed"] = "0"
    case["account"]["marginAvailable"] = "1.00"
    assert Decimal(call(case)["modeled_new_margin_usd"]) <= 1


def test_instrument_and_account_margin_maximum(case):
    case["metadata"]["marginRate"] = "0.20"
    a = call(case)
    case["metadata"]["marginRate"] = "0.02"
    case["account"]["marginRate"] = "0.20"
    b = call(case)
    assert a["units"] == b["units"] == "33"
    assert a["effective_margin_rate"] == "0.20"


def test_precision_and_maximum_units(case):
    case["metadata"].update(tradeUnitsPrecision=2, minimumTradeSize="0.01", maximumOrderUnits="1.234")
    assert call(case)["units"] == "1.23"
    case["metadata"].update(maximumOrderUnits="100000", maximumPositionSize="1.12")
    assert call(case)["units"] == "1.12"
    case["metadata"]["maximumPositionSize"] = "0"
    assert Decimal(call(case)["units"]) > 100


def test_short_orientation_and_price_precision(case):
    case["signal"].update(side=-1, expected_terminal_price="1.2480", probability_up="0.40")
    out = call(case)
    assert out["status"] == "available", out
    assert Decimal(out["units"]) < 0
    assert out["stop_loss_price"] == "1.25100"
    assert out["entry_price_bound"] == "1.24998"
    assert out["signed_currency_directions"] == {"GBP":-1, "USD":1}


def jpy(case):
    case["signal"].update(instrument="USD_JPY", reference_price="150.00", expected_terminal_price="150.30")
    case["quote"].update(instrument="USD_JPY", bid="150.00", ask="150.02")
    case["metadata"].update(name="USD_JPY", pipLocation=-2, displayPrecision=3, marginRate="0.04")
    case["candles"]["instrument"] = "USD_JPY"
    for row in case["candles"]["rows"]: row["mid"] = dict(h="150.04", l="149.96", c="150.00")
    case["home_conversions"]["rows"] = [dict(currency="JPY", accountGain="0.0066", accountLoss="0.0068", positionValue="0.0067")]
    return case


def test_inverse_usd_uses_three_broker_home_conversion_semantics(case):
    out = call(jpy(case)); assert out["status"] == "available", out
    units = Decimal(out["units"])
    assert Decimal(out["modeled_new_margin_usd"]) == units*Decimal(out["entry_price_bound"])*Decimal("0.0067")*Decimal("0.04")
    risk = (Decimal(out["entry_price_bound"])-Decimal(out["stop_loss_price"])+Decimal("150.01")*Decimal("0.1")/10000)*Decimal("0.0068")
    assert Decimal(out["modeled_stop_loss_usd"]) == units*risk
    # Independent terminal executable-price expectation less rounded entry bound.
    net = (Decimal("150.30")-Decimal("0.01")-Decimal("150.01")*Decimal("0.1")/10000-Decimal(out["entry_price_bound"]))*Decimal("0.0066")
    assert Decimal(out["expected_net_usd"]) == units*net
    assert Decimal(out["expected_gross_usd"])-Decimal(out["modeled_round_trip_cost_usd"])==Decimal(out["expected_net_usd"])
    assert Decimal(out["loss_side_cost_stress_usd"])>Decimal(out["modeled_round_trip_cost_usd"])


@pytest.mark.parametrize("pair", ["EUR_CZK", "USD_TRY", "TRY_JPY", "CHF_ZAR"])
def test_no_arbitrary_exotic_pair_exclusion_with_complete_valid_metadata(case, pair):
    base, quote = pair.split("_")
    case["signal"]["instrument"] = pair; case["quote"]["instrument"] = pair
    case["metadata"]["name"] = pair; case["candles"]["instrument"] = pair
    case["home_conversions"]["rows"] = [dict(currency=quote, accountGain="1", accountLoss="1", positionValue="1")]
    assert call(case)["status"] == "available"


@pytest.mark.parametrize("path,value,reason", [
    ("signal.side",0,"signal_neutral_or_invalid_side"),
    ("signal.side",True,"signal_neutral_or_invalid_side"),
    ("signal.side","buy","signal_neutral_or_invalid_side"),
    ("signal.probability_up","0.54999","side_probability_below_minimum"),
    ("signal.probability_up",True,"probability_invalid"),
    ("signal.probability_up","NaN","probability_bound"),
    ("signal.expected_terminal_price","1.2490","original_direction_terminal_mismatch"),
    ("signal.expected_terminal_price","1.2503","expected_gross_below_cost_hurdle"),
    ("signal.reference_price",1.25,"reference_price_decimal_text"),
    ("quote.tradeable",False,"quote_not_explicitly_tradeable"),
    ("quote.tradeable","true","quote_not_explicitly_tradeable"),
    ("quote.ask","1.2510","spread_bps_above_limit"),
    ("quote.ask","1.2490","crossed_quote"),
    ("quote.instrument","EUR_USD","quote_instrument_mismatch"),
    ("account.currency","EUR","account_currency_not_usd"),
    ("account.openTradeCount",1,"existing_position_blocks_entry_or_adding"),
    ("account.pendingOrderCount",1,"pending_order_blocks_entry"),
    ("account.openTradeCount",None,"open_trade_count_invalid"),
    ("metadata.type","METAL","metadata_instrument_type"),
    ("metadata.minimumTradeSize","1000","risk_sized_units_below_broker_minimum"),
    ("metadata.marginRate","0","instrument_margin_rate_positive"),
    ("metadata.tradeUnitsPrecision",-1,"trade_units_precision_invalid"),
    ("session.entries_today",8,"daily_entry_limit"),
    ("session.day_utc","2026-09-08","session_daily_counters_stale"),
])
def test_entry_refusal_gates(case,path,value,reason):
    row,key = path.split("."); case[row][key] = value
    assert call(case)["reason"] == reason


@pytest.mark.parametrize("path,delta,reason", [
    ("quote.market_epoch",-15.001,"quote_market_stale"),
    ("quote.observed_epoch",-15.001,"quote_observation_stale"),
    ("quote.market_epoch",1,"quote_market_future"),
    ("quote.observed_epoch",1,"quote_observation_future"),
    ("account.observed_epoch",-30.001,"account_observation_stale"),
    ("account.observed_epoch",1,"account_observation_future"),
    ("home_conversions.observed_epoch",-30.001,"conversion_observation_stale"),
    ("signal.available_epoch",1,"signal_clock_order"),
])
def test_original_clock_freshness(case,path,delta,reason):
    row,key = path.split(".");case[row][key]=NOW+delta
    assert call(case)["reason"] == reason


def test_inclusive_age_and_probability_boundaries(case):
    case["quote"].update(market_epoch=NOW-15, observed_epoch=NOW-15)
    case["account"]["observed_epoch"] = NOW-30
    case["home_conversions"]["observed_epoch"] = NOW-30
    case["signal"]["probability_up"] = "0.55"
    assert call(case)["status"] == "available"


def test_market_time_must_not_follow_observation(case):
    case["quote"].update(market_epoch=NOW-1, observed_epoch=NOW-2)
    assert call(case)["reason"] == "quote_market_after_observation"


def test_missing_foreign_conversion_and_duplicate_fail_closed(case):
    jpy(case);case["home_conversions"]["rows"] = []
    assert call(case)["reason"] == "quote_currency_conversion_missing"
    jpy(case);case["home_conversions"]["rows"] *= 2
    assert call(case)["reason"] == "conversion_currency_duplicate_or_invalid"


def test_usd_identity_may_be_omitted_but_wrong_actual_row_is_rejected(case):
    assert call(case)["status"] == "available"
    case["home_conversions"]["rows"] = [dict(currency="USD",accountGain="1.1",accountLoss="1",positionValue="1")]
    assert call(case)["reason"] == "home_currency_conversion_not_one"


def test_atr_uses_previous_close_gap_as_true_range(case):
    for row in case["candles"]["rows"]:
        row["mid"] = dict(h="1.2500", l="1.2500", c="1.2500")
    case["candles"]["rows"][0]["mid"] = dict(h="1.2514",l="1.2514",c="1.2514")
    assert Decimal(call(case)["atr_price"]) == Decimal("0.0001")


@pytest.mark.parametrize("mutation,reason", [
    ("missing","atr_requires_15_real_rows"), ("gap","candle_gap_or_duplicate"),
    ("duplicate","candle_gap_or_duplicate"), ("incomplete","candle_incomplete"),
    ("future","candle_close_not_yet_observed"), ("zero","atr_zero_or_unavailable"),
    ("invalid","candle_ohlc_invalid"), ("old","atr_latest_complete_bar_stale"),
])
def test_complete_candle_grid_guards(case,mutation,reason):
    rows=case["candles"]["rows"]
    if mutation=="missing":rows.pop()
    if mutation=="gap":rows[5]["label_epoch"] += 60
    if mutation=="duplicate":rows[5]["label_epoch"] -= 60
    if mutation=="incomplete":rows[-1]["complete"] = False
    if mutation=="future":rows[-1]["label_epoch"] = NOW
    if mutation=="zero":
        for row in rows:row["mid"] = dict(h="1.25",l="1.25",c="1.25")
    if mutation=="invalid":rows[-1]["mid"]["l"] = "1.26"
    if mutation=="old":
        for row in rows:row["label_epoch"] -= 60
    assert call(case)["reason"] == reason


def test_remaining_edge_reversal_does_not_flip_original_forecast(case):
    case["quote"].update(bid="1.2530",ask="1.2532")
    assert call(case)["reason"] == "remaining_edge_opposes_original_side"


def test_cooldown_exact_boundary_and_future_record(case):
    case["session"]["last_entry_epoch_by_instrument"] = {"GBP_USD":NOW-1799.999}
    assert call(case)["reason"] == "pair_cooldown"
    case["session"]["last_entry_epoch_by_instrument"]["GBP_USD"] = NOW-1800
    assert call(case)["status"] == "available"
    case["session"]["last_entry_epoch_by_instrument"]["GBP_USD"] = NOW+1
    assert call(case)["reason"] == "last_entry_clock"


def test_fractional_original_clock_preserved(case):
    case["signal"].update(reference_epoch=NOW-300+.0059996,original_target_epoch=NOW+3300+.0059996)
    out=call(case)
    assert out["status"]=="available", out
    assert out["original_target_epoch"]==case["signal"]["original_target_epoch"]


def test_exact_native_terminal_product_above_24_decimal_places(case):
    with localcontext() as context:
        context.prec=192
        terminal=Decimal("1.123455")*(1+Decimal("12.12345678901234567")/10000)
    assert len(str(terminal).split(".")[1]) > 24
    case["signal"].update(reference_price="1.123455",expected_terminal_price=str(terminal))
    case["quote"].update(bid="1.123455",ask="1.123555")
    for row in case["candles"]["rows"]:row["mid"]=dict(h="1.1236",l="1.1232",c="1.1234")
    assert call(case)["status"]=="available"


def test_non_h1_and_elapsed_or_short_target(case):
    case["signal"]["original_target_epoch"] -= 1
    assert call(case)["reason"]=="signal_not_original_h1"
    case["signal"].update(reference_epoch=NOW-3541,original_target_epoch=NOW+59)
    assert call(case)["reason"]=="insufficient_remaining_horizon"
    case["signal"].update(reference_epoch=NOW-3600,original_target_epoch=NOW)
    assert call(case)["reason"]=="signal_clock_order"


def test_original_signal_age_rechecked_after_fresh_quote_and_later_consumption(case):
    case["signal"].update(reference_epoch=NOW-950,original_target_epoch=NOW+2650,
                          issued_epoch=NOW-900,available_epoch=NOW-1)
    assert call(case)["status"]=="available"
    case["signal"]["issued_epoch"]-=.001
    assert call(case)["reason"]=="original_signal_stale"


def test_session_loss_threshold_is_nav_equity_and_permanently_latched_by_caller(case):
    case["account"]["NAV"]="37.44"
    out=policy.session_guard(case["account"],case["session"],NOW)
    assert out["status"]=="halt" and out["loss_stop_latched"] is True
    assert out["session_loss_limit_usd"]=="4.1600"
    case["session"]["loss_stop_latched"]=True;case["account"]["NAV"]="50"
    assert policy.session_guard(case["account"],case["session"],NOW)["loss_stop_latched"] is True
    assert call(case)["reason"]=="session_loss_stop"
    assert call(case)["loss_stop_latched"] is True


def test_cost_hurdle_includes_outward_tick_rounding_slippage(case):
    # Old unrounded two-leg allowance would pass; actual bound rounding refuses.
    case["signal"]["expected_terminal_price"]="1.250555"
    assert call(case)["reason"]=="expected_gross_below_cost_hurdle"
    case["signal"]["expected_terminal_price"]="1.250566"
    assert call(case)["status"]=="available"


def test_five_dollar_session_cap_and_remaining_budget(case):
    case["session"]["start_nav_usd"]="100"
    case["account"]["NAV"]="95.10"
    out=call(case)
    assert Decimal(out["risk_budget_usd"])==Decimal(".10")
    case["account"]["NAV"]="95"
    assert call(case)["reason"]=="session_loss_stop"


def test_finite_target_deadline_and_48_hour_session_end(case):
    now=(NOW+47*3600-300)-1800
    shift=now-NOW;case["decision_epoch"]=now
    case["signal"].update(reference_epoch=now-1800,original_target_epoch=now+1800,issued_epoch=now-100,available_epoch=now-99)
    case["quote"].update(market_epoch=now-2,observed_epoch=now-1)
    case["account"]["observed_epoch"]=now-1;case["home_conversions"]["observed_epoch"]=now-1
    case["candles"]["observed_epoch"]=now-1
    for row in case["candles"]["rows"]:row["label_epoch"]+=shift
    case["session"]["day_utc"]="2026-09-11"
    assert call(case)["status"]=="available"
    case["signal"]["original_target_epoch"]+=1;case["signal"]["reference_epoch"]+=1
    assert call(case)["reason"]=="target_beyond_entry_deadline"
    case["session"]["stop_epoch"]=(NOW+47*3600)+1
    assert call(case)["reason"]=="session_clock_order"


def test_management_holds_exact_stop_target_without_adding(case):
    out=manage(case)
    assert out["action"]=="hold" and out["add_units"]=="0"
    assert out["stop_loss_price"]=="1.2492" and out["original_target_epoch"]==NOW+3300


def test_deadline_exit_independent_missing_quote_account_and_daily_counters(case):
    pos=position(case);pos["original_target_epoch"]=NOW
    del case["session"]["day_utc"];del case["session"]["entries_today"]
    assert manage(case,pos,quote=None,account=None)["action"]=="close"


def test_protection_missing_closes_even_with_missing_quote_and_account(case):
    pos=position(case);pos["stop_loss_price"]=None
    out=manage(case,pos,quote=None,account=None)
    assert out["action"]=="close" and out["reason"]=="protective_stop_missing"


def test_stale_quote_is_unknown_not_flat_or_success(case):
    out=manage(case,quote=None)
    assert out["action"]=="wait_unknown" and out["existing_position_preserved"] is True


@pytest.mark.parametrize("units,bid,ask,stop", [("100","1.2490","1.2492","1.2492"),("-100","1.2510","1.2512","1.2510")])
def test_protective_stop_crossing_both_sides(case,units,bid,ask,stop):
    pos=position(case);pos.update(currentUnits=units,stop_loss_price=stop)
    case["quote"].update(bid=bid,ask=ask)
    assert manage(case,pos)["reason"]=="protective_stop_crossed"


def test_management_loss_stop_and_exact_position_identity(case):
    case["account"]["NAV"]="37"
    out=manage(case)
    assert out["action"]=="close" and out["loss_stop_latched"] is True and out["trade_id"]=="123"
    pos=position(case);pos["currentUnits"]="0"
    assert manage(case,pos)["reason"]=="position_not_confirmed_open"


def test_margin_cap_is_entry_only_not_churning_existing_position(case):
    case["account"].update(marginUsed="8.33",openTradeCount=1)
    out=manage(case)
    assert out["action"]=="hold"
    assert out["loss_stop_latched"] is False
    case["account"]["marginUsed"]="8.32"
    assert manage(case)["action"]=="hold"


def test_choose_best_normalizes_usd_not_native_pips(case):
    gbp=call(case);other=deepcopy(case);jpy(other);yen=call(other)
    winner=policy.choose_best([gbp,yen])
    expected=max([gbp,yen],key=lambda x:Decimal(x["expected_net_usd"]))
    assert winner["assessment_sha256"]==expected["assessment_sha256"]


def test_choose_best_rejects_tamper_duplicates_and_mixed_clock(case):
    row=call(case);bad=deepcopy(row);bad["units"]="9999999"
    assert policy.choose_best([bad])["reason"]=="assessment_seal_mismatch"
    assert policy.choose_best([row,row])["reason"]=="duplicate_instrument_assessment"
    other=deepcopy(case);other["decision_epoch"]=NOW+1
    assert policy.choose_best([row,call(other)])["reason"]=="mixed_assessment_clocks"


def test_choose_best_rejects_different_account_snapshot_or_session_state(case):
    row=call(case);other=deepcopy(case);jpy(other)
    other["account"]["NAV"]="42"
    assert policy.choose_best([row,call(other)])["reason"]=="mixed_account_session_contexts"
    other["account"]["NAV"]="41.60";other["session"]["entries_today"]=1
    assert policy.choose_best([row,call(other)])["reason"]=="mixed_account_session_contexts"


def test_mutation_ownership_and_low_decimal_precision(case):
    before=deepcopy(case);ordinary=call(case)
    with localcontext() as ctx:
        ctx.prec=3
        low=call(case)
        selected=policy.choose_best([ordinary])
    assert low==ordinary and selected==ordinary and case==before
    ordinary["policy"]["risk_nav_fraction"]="1"
    assert call(case)["policy"]["risk_nav_fraction"]=="0.005"
    assert case["signal"]["source_authority"]["can_place_orders"] is False


@pytest.mark.parametrize("bad", [None, [], {"bad":True}, float("nan")])
def test_malformed_input_has_explicit_bounded_refusal(case,bad):
    case["account"]=bad
    out=call(case)
    assert out["status"]=="refused" and out["action"]=="none"


def test_public_boundaries_do_no_filesystem_or_transport(case,monkeypatch):
    import builtins,socket
    def forbidden(*args,**kwargs):raise AssertionError("unexpected I/O")
    monkeypatch.setattr(builtins,"open",forbidden);monkeypatch.setattr(socket,"socket",forbidden)
    assert call(case)["status"]=="available"
    assert manage(case)["action"]=="hold"
    assert policy.session_guard(case["account"],case["session"],NOW)["status"]=="within_limits"
