import numpy as np
from oanda_spike_blurb_factor_response_analogs_v2 import CONTRACT_ID,build_rows_v2
from oanda_spike_blurb_response_entry_replay import PairSeries


def _pair(name,values):
    base,quote=name.split("_");mid=np.asarray(values,float);epochs=np.arange(1_800_000_000,1_800_000_000+len(mid)*60,60,dtype=np.int64)
    return PairSeries(name,base,quote,.0001,epochs,mid,mid,mid-.00005,mid,mid,mid+.00005,mid,"a"*64)


def test_v2_rows_bind_new_contract_without_changing_rules():
    market={"EUR_USD":_pair("EUR_USD",[1,1.01]),"EUR_GBP":_pair("EUR_GBP",[1,1.02]),"USD_EUR":_pair("USD_EUR",[1,.99])}
    watch={"factor_id":"f","source_evidence_id":"s","source_id":"official","underlying_event_id":"e","source_batch_id":"b","currency":"EUR","factor_type":"policy_rate_change","relevance_state":"direct_action","numeric_measurement_state":"rate_level","signed_factor_score":1.0,"known_utc":"x","published_utc":"x","publication_lag_sec":0,"known_epoch":1_800_000_000}
    rows=build_rows_v2([watch],market)
    assert len(rows)==1 and rows[0]["contract_id"]==CONTRACT_ID and rows[0]["currency_response_sign"]==1
