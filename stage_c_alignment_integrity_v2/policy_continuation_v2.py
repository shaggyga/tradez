"""Bounded policy controller over the existing event ledger and recovered selectors.

Forecasts are declared synthetic, not fitted market evidence. No price is fetched
and an execution frame is required for every fill. R06/R08/R14, design section 18.
"""
from copy import deepcopy
from decimal import Decimal, localcontext

from accounting_events_v2 import EventLedger, event_contract, retrospective_event_contract
from accounting_fastpath_v2 import OptimizedEventLedger
from accounting_event_fixtures_v2 import event
from reference_accounting_adapter_v2 import DEFAULT_TRAD

POLICIES = ("fixed_hold", "recovered", "naive", "continuation", "hysteresis", "cash")
RETROSPECTIVE_POLICY_TIER = "historical_fitted_candle_policy_scenario.v1"


def policy_contract(trad_root=DEFAULT_TRAD):
    ledger = event_contract(trad_root)
    ledger["arms"] = list(POLICIES)
    ledger["reference_config"]["metadata"]["GBP_USD"] = {
        "base_currency": "GBP", "quote_currency": "USD", "pip_size": ".0001", "unit_increment": 1}
    ledger["reference_config"]["legacy_policy"] = {
        "minimum_score_cost_ratio": .75, "rotation_improvement_multiple": 1.25,
        "maximum_holding_min": 7200}
    return {"schema_version": "forex_policy_continuation.v2", "ledger": ledger,
        "policies": {name: name for name in POLICIES}, "fee_usd_per_fill": "0.10",
        "switch_buffer_usd": "0.25", "persistence_observations": 2, "grace_fraction": "0.05",
        "valuation_assumption": "frozen_current_spread_slippage_and_conversion_to_common_target",
        "financing_assumption": "declared_remaining_usd_per_base_unit_by_candidate",
        "continuation": "hold_selected_position_to_common_target_then_liquidate; cash_yield_zero",
        "input_tier": "synthetic_forecast_execution_fixture_only", "max_frames": 128}


def retrospective_policy_contract(trad_root=DEFAULT_TRAD):
    result=policy_contract(trad_root)
    ledger=retrospective_event_contract(trad_root)
    ledger['arms']=list(POLICIES)
    ledger['reference_config']=result['ledger']['reference_config']
    result.update(ledger=ledger,input_tier=RETROSPECTIVE_POLICY_TIER)
    return result


class PolicyReplay:
    def __init__(self, contract, *, trad_root=DEFAULT_TRAD, engine="reference"):
        defaults = (retrospective_policy_contract(trad_root) if contract.get('input_tier')==RETROSPECTIVE_POLICY_TIER
                    else policy_contract(trad_root))
        if set(contract) != set(defaults) or any(contract[k] != defaults[k] for k in (
                "schema_version", "valuation_assumption", "financing_assumption", "continuation", "input_tier", "max_frames")):
            raise ValueError("unsupported_policy_contract")
        if contract['ledger'].get('input_tier')!=defaults['ledger']['input_tier']:
            raise ValueError('policy_ledger_input_tier_mismatch')
        if engine not in {"reference", "optimized"}:
            raise ValueError("unsupported_accounting_engine")
        self.book = (EventLedger if engine == "reference" else OptimizedEventLedger)(contract["ledger"], trad_root=trad_root)
        self.ref = self.book.reference
        self.trad_root = trad_root
        self.contract = deepcopy(contract)
        if set(contract["policies"]) != set(contract["ledger"]["arms"]) or any(p not in POLICIES for p in contract["policies"].values()):
            raise ValueError("policy_arm_mapping_mismatch")
        self.fee = self.book.decimal(contract["fee_usd_per_fill"])
        self.buffer = self.book.decimal(contract["switch_buffer_usd"])
        self.grace = self.book.decimal(contract["grace_fraction"])
        if min(self.fee,self.buffer,self.grace) < 0 or self.grace > 1 or type(contract["persistence_observations"]) is not int or not 1 <= contract["persistence_observations"] <= 10:
            raise ValueError("invalid_policy_limits")
        self.memory = {a:{"original_thesis":None,"current_thesis":None,"pending":None,
                          "replacement":None,"challenger_key":None,"streak":0,"episodes":[],"orders":{},
                          "observed_mfe_usd":Decimal(0),"observed_mae_usd":Decimal(0)} for a in sorted(contract["policies"])}
        self.events=[]; self.rows=[]; self.decisions=[]; self.cursor=None

    def emit(self, kind, frame, arm=None, **fields):
        row=event(kind, frame["epoch"], len(self.events), arm=arm, quotes=frame["quotes"], **fields)
        row['input_tier']=self.book.contract['input_tier']
        self.events.append(row); result=self.book.apply(row); self.rows.append(result)
        return result

    def position(self, arm):
        lots=self.book.state["arms"][arm]["lots"]
        if not lots: return None
        positions=[lot["position"] for lot in lots.values()]
        if len({(p["instrument"],p["side"],p["original_target_epoch"]) for p in positions}) != 1:
            raise ValueError("single_thesis_position_required")
        result=deepcopy(positions[0]); result["base_units"]=sum(p["base_units"] for p in positions)
        result["entry_epoch"]=min(p["entry_epoch"] for p in positions)
        return result

    def values(self, arm, candidates, frame):
        """All values are changes from executable liquidation wealth, not entry cost.

        Conversion is frozen at current executable gain/loss rates. Holding value
        subtracts current and terminal *total* converted PnL so sign-dependent
        conversion is respected, including a change across the zero-PnL boundary.
        """
        state=self.book.state["arms"][arm]; epoch=frame["epoch"]; quotes=frame["quotes"]
        report=self.book.reconcile(state,quotes,epoch)
        wealth=report["equity_usd"]
        if wealth is None:
            return {"baseline_liquidation_wealth_usd":None,"common_target_epoch":frame['target_epoch'],
                "continuation":self.contract['continuation'],"assumption":self.contract['valuation_assumption'],
                "exit_value_usd":None,"hold_value_usd":None,"alternatives":[],
                "valuation_status":"unavailable","unavailable":deepcopy(report['unavailable'])}
        if state["lots"]: wealth -= self.fee
        result={"baseline_liquidation_wealth_usd":wealth,"common_target_epoch":frame["target_epoch"],
                "continuation":self.contract["continuation"],"assumption":self.contract["valuation_assumption"],
                "exit_value_usd":Decimal(0),"hold_value_usd":None,"alternatives":[]}
        position=self.position(arm)
        if position:
            current=next((c for c in candidates if c["instrument"]==position["instrument"]),None)
            if current is not None and frame["target_epoch"]==position["original_target_epoch"]:
                total=Decimal(0)
                for lot in state["lots"].values():
                    p=lot["position"]; q=self.ref.quote_at(quotes,p["instrument"],epoch,self.book.config["quote_max_age_sec"])
                    future=self.terminal_quote(q,current["expected_terminal_price"])
                    now_price,_=self.ref._executed_price(q,p["side"],False,self.book.config)
                    then_price,_=self.ref._executed_price(future,p["side"],False,self.book.config)
                    currency=self.book.config["metadata"][p["instrument"]]["quote_currency"]
                    entry=self.book.decimal(p["entry_price"])
                    now,_=self.ref.convert_pnl_to_usd(p["side"]*p["base_units"]*(now_price-entry),currency,quotes,epoch,self.book.config["quote_max_age_sec"])
                    then,_=self.ref.convert_pnl_to_usd(p["side"]*p["base_units"]*(then_price-entry),currency,quotes,epoch,self.book.config["quote_max_age_sec"])
                    total += then-now + p["base_units"]*self.financing(current,p["side"])
                result["hold_value_usd"]=total  # identical immediate/future close fee cancels
        for c in candidates:
            if not c["side"]: continue
            q=self.ref.quote_at(quotes,c["instrument"],epoch,self.book.config["quote_max_age_sec"])
            future=self.terminal_quote(q,c["expected_terminal_price"])
            entry,_=self.ref._executed_price(q,c["side"],True,self.book.config)
            close,_=self.ref._executed_price(future,c["side"],False,self.book.config)
            value,conversion=self.ref.convert_pnl_to_usd(c["side"]*c["base_units"]*(close-entry),
                self.book.config["metadata"][c["instrument"]]["quote_currency"],quotes,epoch,self.book.config["quote_max_age_sec"])
            value += c["base_units"]*self.financing(c,c["side"]) - 2*self.fee
            result["alternatives"].append({"candidate":deepcopy(c),"value_usd":value,"conversion":conversion})
        result["alternatives"].sort(key=lambda r:(-r["value_usd"],r["candidate"]["instrument"],-r["candidate"]["side"]))
        return result

    def terminal_quote(self,q,mid):
        future=deepcopy(q); mid=self.book.decimal(mid); spread=q["ask"]-q["bid"]
        future.update(mid=mid,bid=mid-spread/2,ask=mid+spread/2)
        if future["bid"] <= 0: raise ValueError("invalid_terminal_scenario")
        return future

    def financing(self,candidate,side):
        rates=candidate["source_payload"]["remaining_financing_usd_per_base_unit"]
        if set(rates)!={"long","short"}: raise ValueError("explicit_signed_financing_scenario_required")
        return self.book.decimal(rates["long" if side>0 else "short"])

    def propose(self, arm, candidates, frame):
        policy=self.contract["policies"][arm]; memory=self.memory[arm]; position=self.position(arm)
        report=self.book.reconcile(self.book.state["arms"][arm],frame["quotes"],frame["epoch"])
        values=self.values(arm,candidates,frame)
        if position and (frame.get("terminal",False) or report["risk_status"]=="breached" or
                frame["epoch"]+int(self.book.config["execution_delay_sec"])>=position["original_target_epoch"]):
            return "EXIT",None,"risk_or_predeclared_deadline",values
        if values.get('valuation_status')=='unavailable':
            memory['streak']=0;memory['challenger_key']=None
            return "WAIT",None,"liquidation_wealth_unavailable_no_discretionary_action",values
        if memory["pending"]: return "WAIT",None,"pending_order_requires_execution_or_ack",values
        if policy=="cash" or frame.get("terminal",False): return "WAIT",None,"cash_or_terminal",values
        if position and frame["target_epoch"]!=position["original_target_epoch"]:
            return "HOLD",None,"incomparable_horizon_no_rotation",values
        if position and policy=="fixed_hold": return "HOLD",None,"fixed_thesis_until_deadline_or_risk",values
        if policy=="recovered":
            if any(c["kind"]!="momentum" for c in candidates):
                return "WAIT",None,"recovered_selector_native_curve_fields_unavailable",values
            decision,diagnostics=self.ref.choose_legacy_action({"position":position},candidates,frame["epoch"],self.book.config,terminal=False)
            picked=next((c for c in candidates if c["candidate_id"]==decision.get("candidate_id")),None)
            values["recovered_diagnostics"]=diagnostics
            return {"wait":"WAIT","enter":"ENTER","hold":"HOLD","exit":"EXIT","rotate":"REPLACE"}[decision["action"]],picked,decision["reason"],values
        if position and policy=="naive":
            best=values["alternatives"][0] if values["alternatives"] else None
            if best is None or best["value_usd"]<=0:return "EXIT",None,"naive_cash_dominates",values
            c=best["candidate"]
            if (c["instrument"],c["side"])!=(position["instrument"],position["side"]):
                return "REPLACE",c,"naive_top_rank_without_continuation_hurdle",values
            return "HOLD",None,"naive_current_top_rank",values
        choices=[r for r in values["alternatives"] if position is None or
                 (r["candidate"]["instrument"],r["candidate"]["side"])!=(position["instrument"],position["side"])]
        best=choices[0] if choices else None
        if position is None:
            return ("ENTER",best["candidate"],"positive_net_entry",values) if best and best["value_usd"]>0 else ("WAIT",None,"cash_dominates",values)
        hold=values["hold_value_usd"]
        if hold is None: return "EXIT",None,"incumbent_forecast_unavailable",values
        if best and best["value_usd"]>max(Decimal(0),hold+self.buffer):
            c=best["candidate"]; key=(c["instrument"],c["side"],c["original_target_epoch"],c["source_payload"].get("thesis_version"))
            memory["streak"] = memory["streak"]+1 if memory["challenger_key"]==list(key) else 1
            memory["challenger_key"]=list(key)
            age=frame["epoch"]-position["entry_epoch"]; horizon=position["original_target_epoch"]-position["entry_epoch"]
            if policy!="hysteresis" or (memory["streak"]>=self.contract["persistence_observations"] and age>=self.grace*horizon):
                return "REPLACE",c,"common_horizon_advantage",values
            return ("EXIT",None,"negative_hold_cash_dominates",values) if hold<0 else ("HOLD",None,"persistence_or_grace",values)
        memory["challenger_key"]=None; memory["streak"]=0
        return ("HOLD",None,"hold_dominates",values) if hold>=0 else ("EXIT",None,"cash_dominates",values)

    def intent(self,arm,action,candidate,frame,decision_id):
        memory=self.memory[arm]; state=self.book.state["arms"][arm]
        if memory["pending"]: return {"status":"pending_order_preserved"}
        order_id=decision_id+"-order"
        if action in {"EXIT","REPLACE"}:
            p=self.position(arm)
            fields={"instrument":p["instrument"],"action":"reduce","units":p["base_units"],"lot_ids":sorted(state["lots"])}
        else:
            fields={"instrument":candidate["instrument"],"action":"open","side":candidate["side"],
                    "notional_usd":str(self.book.config["notional_usd"]),"target_epoch":int(candidate["original_target_epoch"])}
        receipt=self.emit("intent",frame,arm,order_id=order_id,**fields)["receipt"]
        if receipt["status"]=="accepted":
            memory["pending"]=order_id; memory["orders"][order_id]={"decision_id":decision_id,"action":action,"thesis":deepcopy(candidate)}
            if action=="REPLACE": memory["replacement"]={"decision_id":decision_id,"candidate":deepcopy(candidate),"status":"exit_pending"}
        return receipt

    def apply(self,frame):
        with localcontext(self.ref.CTX): return self._apply(frame)

    def _apply(self,frame):
        if not isinstance(frame,dict) or type(frame.get("epoch")) is not int or (self.cursor is not None and frame["epoch"]<=self.cursor):
            raise ValueError("strictly_increasing_policy_frame_clock_required")
        if frame.get("kind") not in {"decision","execution","financing","mark","order_event"}:
            raise ValueError("unsupported_policy_frame")
        if self.contract['input_tier']==RETROSPECTIVE_POLICY_TIER:
            from historical_native_input_v2 import validate_historical_frame
            validate_historical_frame(frame,self.book.config,self.trad_root)
        elif frame.get("kind")=="decision" and frame.get("candidate_kind")=="curve":
            from native_policy_input_v2 import validate_adapted_frame
            validate_adapted_frame(frame,self.book.config,self.trad_root)
        self.emit("mark",frame)
        if frame["kind"]=="execution":
            for arm,fill in sorted(frame["fills"].items()):
                memory=self.memory[arm]; order_id=memory["pending"]
                if order_id is None: continue  # explicit unused observation, never manufacture an order
                order=self.book.state["arms"][arm]["orders"][order_id]
                if "active_epoch" not in order:self.emit("activate",frame,arm,order_id=order_id)
                units=self.book._remaining(order) if fill["units"]=="remaining" else fill["units"]
                row=self.emit("fill",frame,arm,order_id=order_id,fill_id=order_id+"-"+str(frame["epoch"]),
                    units=units,filled_epoch=frame["epoch"],fee_usd=str(self.fee),execution_evidence_id=fill["evidence_id"])
                if row["receipt"]["status"]!="rejected":
                    info=memory["orders"][order_id]
                    if info["action"]=="ENTER" and memory["original_thesis"] is None:
                        memory["observed_mfe_usd"]=Decimal(0);memory["observed_mae_usd"]=Decimal(0)
                        memory["challenger_key"]=None;memory["streak"]=0
                        memory["original_thesis"]=deepcopy(info["thesis"])
                        memory["episodes"].append({"original_thesis":deepcopy(info["thesis"]),"entry_decision":info["decision_id"],
                            "replacement_parent":deepcopy(memory["replacement"])})
                        memory["replacement"]=None
                    if not self.position(arm):
                        if memory["episodes"]:memory["episodes"][-1]["closed_epoch"]=frame["epoch"]
                        memory["challenger_key"]=None;memory["streak"]=0
                        memory["original_thesis"]=None; memory["current_thesis"]=None
                        if memory["replacement"]:memory["replacement"]["status"]="exit_filled_revalidate_at_next_decision"
                if not self.book._remaining(self.book.state["arms"][arm]["orders"][order_id]):memory["pending"]=None
        elif frame["kind"]=="order_event":
            if frame["event"] not in {"cancel_request","cancel_ack","expire"}:raise ValueError("unsupported_external_order_event")
            for arm in frame["arms"]:
                memory=self.memory[arm];order_id=memory["pending"]
                if order_id:
                    self.emit(frame["event"],frame,arm,order_id=order_id)
                    if not self.book._remaining(self.book.state["arms"][arm]["orders"][order_id]):memory["pending"]=None
        elif frame["kind"]=="financing":
            for arm in self.memory:
                self.emit("financing",frame,arm,financing_id=f"{arm}-{frame['epoch']}",rates=frame["rates"],
                    provenance_id=frame["provenance_id"],accrual_period_id=frame["accrual_period_id"])
        elif frame["kind"]=="decision":
            if type(frame.get("target_epoch")) is not int or frame["target_epoch"]<=frame["epoch"]:raise ValueError("future_common_target_required")
            candidates,rejected=self.ref.prepare_candidates(frame["candidates"],frame.get("candidate_kind","momentum"),frame["quotes"],frame["epoch"],frame["target_epoch"],self.book.config)
            for c in candidates:
                if not isinstance(c["source_payload"].get("thesis_version"),str):raise ValueError("thesis_version_required")
                self.financing(c,1); self.financing(c,-1)
            for arm in self.memory:
                position=self.position(arm)
                self.memory[arm]["current_thesis"]=deepcopy(next((c for c in candidates if position and c["instrument"]==position["instrument"]),None))
                action,candidate,reason,values=self.propose(arm,candidates,frame)
                decision_id=f"{arm}-{frame['epoch']}"
                record={"decision_id":decision_id,"arm":arm,"epoch":frame["epoch"],"action":action,"reason":reason,
                    "snapshot_sha256":self.ref.digest(frame),
                    "values":values,"candidate":deepcopy(candidate),"original_thesis":deepcopy(self.memory[arm]["original_thesis"]),
                    "current_thesis":deepcopy(self.memory[arm]["current_thesis"]),"rejected_candidates":deepcopy(rejected)}
                if action in {"ENTER","EXIT","REPLACE"}:record["intent_receipt"]=self.intent(arm,action,candidate,frame,decision_id)
                self.decisions.append(record)
        self.cursor=frame["epoch"]
        for arm,memory in self.memory.items():
            if self.position(arm):
                mark=self.book.reconcile(self.book.state["arms"][arm],frame["quotes"],frame["epoch"])["unrealized_usd"]
                if mark is not None:
                    memory["observed_mfe_usd"]=max(memory["observed_mfe_usd"],mark)
                    memory["observed_mae_usd"]=max(memory["observed_mae_usd"],-mark)
                    if memory["episodes"]:memory["episodes"][-1].update(observed_mfe_usd=memory["observed_mfe_usd"],observed_mae_usd=memory["observed_mae_usd"])
        return {"epoch":self.cursor,"accounting_event_count":len(self.events),"decision_count":len(self.decisions)}
