"""Declared two-day, five-arm synthetic policy integration fixture."""
from policy_continuation_v2 import policy_contract
from accounting_event_fixtures_v2 import panel
from reference_accounting_adapter_v2 import DEFAULT_TRAD


def fixture(trad_root=DEFAULT_TRAD):
    contract=policy_contract(trad_root)
    target=173000
    def decision(epoch, eur, gbp, terminal=False):
        rows=[]
        for pair,move in (("EUR_USD",eur),("GBP_USD",gbp)):
            rows.append({"instrument":pair,"decision_epoch":epoch,"available_epoch":epoch,
                "original_target_epoch":target,"side":1,"expected_move_pips":str(move),
                "confidence":"0.6","score":str(move),"snapshot_id":f"{pair}-{epoch}",
                "thesis_version":"declared-two-day-v1",
                "remaining_financing_usd_per_base_unit":{"long":"-0.00005","short":"0.00002"}})
        return {"kind":"decision","epoch":epoch,"target_epoch":target,"quotes":panel(epoch),"candidates":rows,"terminal":terminal}
    def execution(epoch):
        return {"kind":"execution","epoch":epoch,"quotes":panel(epoch),
            "fills":{a:{"units":"remaining","evidence_id":f"explicit-{a}-{epoch}"} for a in contract["policies"]}}
    frames=[decision(1000,100,10),execution(1060),decision(20000,10,400),execution(20060),
            decision(20100,10,400),execution(20160),decision(20200,10,400),execution(20260),
            {"kind":"financing","epoch":86400,"quotes":panel(86400),
             "rates":{p:{"long":"-0.00005","short":"0.00002"} for p in ("EUR_USD","GBP_USD")},
             "provenance_id":"declared-synthetic-rate","accrual_period_id":"first-rollover"},
            decision(172940,10,400,True),execution(173000)]
    return contract,frames
