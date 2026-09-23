import gzip,hashlib,json
from oanda_spike_blurb_legacy_price_verifier import recompute,verify_window


def _record():
    candles=[{"epoch":60,"time":"1970-01-01T00:01:00+00:00","bid_o":1.0,"bid_h":1.002,"bid_l":.999,"bid_c":1.001,"ask_o":1.0002,"ask_h":1.0022,"ask_l":.9992,"ask_c":1.0012},
             {"epoch":120,"time":"1970-01-01T00:02:00+00:00","bid_o":1.001,"bid_h":1.003,"bid_l":1.0,"bid_c":1.002,"ask_o":1.0012,"ask_h":1.0032,"ask_l":1.0002,"ask_c":1.0022}]
    raw=json.dumps({"instrument":"EUR_USD","candles":candles},sort_keys=True,separators=(",",":")).encode();values=recompute("EUR_USD",candles)
    return {"instrument":"EUR_USD","coverage_state":"exact_window","payload_gzip":gzip.compress(raw),"payload_sha256":hashlib.sha256(raw).hexdigest(),**values}


def test_independent_window_round_trip():
    assert verify_window(_record())==[]


def test_tampering_is_detected():
    record=_record();record["long_net_pips"]+=1
    assert "long_net_pips_mismatch" in verify_window(record)
    record=_record();record["payload_sha256"]="0"*64
    assert "payload_hash_mismatch" in verify_window(record)
