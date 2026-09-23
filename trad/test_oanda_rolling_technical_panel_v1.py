import copy
import json

import pytest

import oanda_rolling_technical_panel_v1 as panel


AT = 1700000040


def row(value,at=AT):
    return {"bar_start_epoch":at,"values":{f"m1__return_{h}_bps":value*h for h in panel.HORIZONS}}


def sample():
    return {"EUR_USD":row(99),"EUR_GBP":row(2),"EUR_JPY":row(4),
            "GBP_USD":row(6),"AUD_USD":row(8),"USD_CHF":row(10)}


def test_peer_sign_self_exclusion_and_differential():
    result = panel.compute_panel(sample(),AT)["by_pair"]["EUR_USD"]
    assert result["status"] == "available"
    assert result["values"]["peer__base_mean_1_bps"] == 3
    assert result["values"]["peer__quote_mean_1_bps"] == pytest.approx((-6-8+10)/3)
    assert result["values"]["peer__diff_1_bps"] == pytest.approx(3-(-6-8+10)/3)
    assert result["support"]["1"]["base"]["count"] == 2
    assert result["support"]["1"]["quote"]["count"] == 3
    for horizon in result["support"].values():
        for leg in horizon.values():
            assert "EUR_USD" not in [x["pair"] for x in leg["constituents"]]


def test_future_and_asynchronous_sources_excluded_even_with_strong_returns():
    rows = sample()
    rows["EUR_GBP"] = row(1e8,AT+60)
    rows["EUR_JPY"] = row(1e8,AT-60)
    result = panel.compute_panel(rows,AT)
    target = result["by_pair"]["EUR_USD"]
    assert target["values"]["peer__base_mean_1_bps"] is None
    assert target["values"]["peer__diff_1_bps"] is None
    assert target["support"]["1"]["base"]["count"] == 0
    assert result["by_pair"]["EUR_GBP"]["reason"] == "future_bar_start"
    assert result["by_pair"]["EUR_JPY"]["reason"] == "asynchronous_bar_start"
    assert {r["reason"] for r in result["excluded_sources"]} == {"future_bar_start","asynchronous_bar_start"}


def test_missing_15m_never_substitutes_1m_and_minimum_two_peers_each_leg():
    rows = sample()
    del rows["EUR_GBP"]["values"]["m1__return_15_bps"]
    rows["EUR_JPY"]["values"]["m1__return_15_bps"] = float("nan")
    result = panel.compute_panel(rows,AT)["by_pair"]["EUR_USD"]
    assert result["values"]["peer__base_mean_1_bps"] == 3
    assert result["values"]["peer__base_mean_15_bps"] is None
    assert result["support"]["15"]["base"]["count"] == 0
    assert result["support"]["15"]["base"]["missing_horizon_peers"] == ["EUR_GBP","EUR_JPY"]
    del rows["EUR_JPY"]
    result = panel.compute_panel(rows,AT)["by_pair"]["EUR_USD"]
    assert result["support"]["1"]["base"]["count"] == 1
    assert result["values"]["peer__base_mean_1_bps"] is None


def test_missing_target_return_does_not_change_independent_peer_context():
    rows = sample()
    expected = panel.compute_panel(rows,AT)["by_pair"]["EUR_USD"]
    rows["EUR_USD"]["values"] = {}
    assert panel.compute_panel(rows,AT)["by_pair"]["EUR_USD"] == expected


def test_deterministic_order_and_provenance_no_mutation_finite_json():
    rows = sample()
    untouched = copy.deepcopy(rows)
    first = panel.compute_panel(rows,AT)
    second = panel.compute_panel(dict(reversed(list(rows.items()))),AT)
    assert first == second
    assert rows == untouched
    assert len(first["provenance_sha256"]) == 64
    assert len(first["by_pair"]["EUR_USD"]["provenance_sha256"]) == 64
    json.dumps(first,allow_nan=False)
    rows["EUR_GBP"]["values"]["m1__return_1_bps"] += 1
    changed = panel.compute_panel(rows,AT)
    assert changed["by_pair"]["EUR_USD"]["provenance_sha256"] != first["by_pair"]["EUR_USD"]["provenance_sha256"]


def test_zero_means_are_observations_not_missing():
    rows = {p:row(0) for p in sample()}
    result = panel.compute_panel(rows,AT)["by_pair"]["EUR_USD"]
    assert result["values"]["peer__diff_60_bps"] == 0
    assert result["status"] == "available"


def test_registry_excludes_models_and_metadata_counts():
    registry = panel.panel_registry()
    assert len(registry) == len({r["name"] for r in registry}) == 12
    assert {r["family"] for r in registry} == {"currency_peer"}
    assert {r["source_feature"] for r in registry} == {f"m1__return_{h}_bps" for h in panel.HORIZONS}
    assert all(r["minimum_distinct_peer_pairs_per_leg"] == 2 for r in registry)
    assert panel.panel_metadata()["can_place_orders"] is False


@pytest.mark.parametrize("epoch",[None,True,AT+1,float("nan"),-60])
def test_invalid_target_clock_rejected(epoch):
    with pytest.raises(ValueError):
        panel.compute_panel(sample(),epoch)
