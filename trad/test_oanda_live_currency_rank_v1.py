import copy
import json
import math
import sqlite3

import pytest
import oanda_live_currency_rank_v1 as live


def inputs(now=100000):
    rows, quotes = [], {}
    strengths = {c: (i-10)*.25 for i, c in enumerate(live.ranker.EXPECTED_CURRENCIES)}
    for pair in live.ranker.EXPECTED_INSTRUMENTS:
        a, b = pair.split('_')
        mid = 100 if b == 'JPY' else 1.1
        quotes[pair] = dict(market_epoch=now-1, bid=str(mid-.00001),
                            ask=str(mid+.00001), quote_id=pair)
        for m in live.WINDOWS:
            old = mid / math.exp((strengths[a]-strengths[b])*m/5/10000)
            rows.append((pair, now-1-m*60-10, old))
    return rows, dict(quotes=quotes, refusals={}, snapshot_sha256='fixture')


def test_original_full_ranker_exact_scoring_and_no_orders():
    rows, mapped = inputs()
    result = live.calculate(rows, mapped, 100000)
    pairs, refused = live.build_pairs(rows, mapped, 100000)
    direct = live.cohort(pairs, live.ranker.EXPECTED_CURRENCIES, 100000)
    assert not refused
    assert result['selected_cohort'] == 'full21'
    assert result['cohorts']['full21']['currency_ranks'] == direct['currency_ranks']
    assert len(direct['currency_ranks']) == 21
    assert result['can_place_orders'] is False
    assert result['execution_eligible'] is False


def test_missing_try_does_not_fake_full_population_or_block_major8():
    rows, mapped = inputs()
    for pair in ('EUR_TRY','TRY_JPY','USD_TRY'):
        del mapped['quotes'][pair]
    result = live.calculate(rows, mapped, 100000)
    assert result['selected_cohort'] == 'major8'
    assert result['cohorts']['full21']['missing_pairs'] == ['EUR_TRY','TRY_JPY','USD_TRY']
    assert {r['currency'] for r in result['cohorts']['major8']['currency_ranks']} == set(live.MAJORS)
    assert result['cohorts']['major8']['expected_pairs'] == 28


@pytest.mark.parametrize('age', [61, -1, float('nan')])
def test_one_stale_or_future_major_refuses_both_cohorts(age):
    rows, mapped = inputs()
    mapped['quotes']['EUR_USD']['market_epoch'] = 100000-age
    pairs, refused = live.build_pairs(rows, mapped, 100000)
    assert 'EUR_USD' not in pairs
    assert refused['EUR_USD'] == 'quote_stale_or_future'


def test_future_mid_does_not_leak_into_cutoff():
    rows, mapped = inputs()
    pairs, _ = live.build_pairs(rows, mapped, 100000)
    rows += [('EUR_USD', 99999-300+1, 999), ('EUR_USD', 100001, 999)]
    after, _ = live.build_pairs(rows, mapped, 100000)
    assert after['EUR_USD']['windows']['5'] == pairs['EUR_USD']['windows']['5']


def test_missing_hour_and_stale_endpoint_not_filled_with_zero():
    rows, mapped = inputs()
    rows = [(p, t-100 if p == 'EUR_USD' and t < 97000 else t, x) for p,t,x in rows]
    result = live.calculate(rows, mapped, 100000)
    assert 'EUR_USD' in result['cohorts']['major8']['missing_pairs']
    assert all(p['instrument'] != 'EUR_USD' for p in result['cohorts']['major8']['pair_candidates'])
    assert result['pair_refusals']['EUR_USD'] == 'historical_endpoint_unavailable_60'


def test_restart_same_inputs_same_result():
    rows, mapped = inputs()
    a = live.calculate(rows, mapped, 100000)
    b = live.calculate(*json.loads(json.dumps([rows,mapped])), 100000)
    assert a == b


def test_dashboard_refuses_old_publication_and_expired_quotes(tmp_path):
    rows, mapped = inputs()
    path = tmp_path/'rank.json'
    result = live.calculate(rows, mapped, 100000)
    live.atomic_write(path,result)
    assert live.read_current(path,100001)['selected_cohort'] == 'full21'
    assert live.read_current(path,100046)['status'] == 'unavailable'
    result['pairs']['EUR_USD']['latest_epoch'] = 99941
    live.atomic_write(path,result)
    got = live.read_current(path,100002)
    assert got['status'] == 'unavailable'
    assert got['cohorts']['major8']['currency_ranks'] == []


def test_database_read_is_bounded_and_read_only(tmp_path):
    p = tmp_path/'observations.sqlite'
    with sqlite3.connect(p) as db:
        db.execute('create table quote_intensity_minutes_v1 (instrument,last_event_epoch,last_mid,minute_epoch)')
        db.executemany('insert into quote_intensity_minutes_v1 values (?,?,?,?)',
                       [('EUR_USD',99900,1.1,99900),('EUR_USD',100001,1.2,100001),('EUR_USD',1,9,1)])
    before = p.read_bytes()
    assert live.history_rows(p,100000) == [('EUR_USD',99900,1.1)]
    assert p.read_bytes() == before


def test_failed_cycle_replaces_previous_rank_with_unavailable(tmp_path):
    p = tmp_path/'rank.json'
    live.atomic_write(p, live.calculate(*inputs(),100000))
    assert live.main(['--database',str(tmp_path/'missing.sqlite'),'--output',str(p)]) == 0
    assert live.read_current(p)['status'] == 'unavailable'
    assert json.loads(p.read_bytes())['cohorts'] == {}


def test_operational_heartbeat_does_not_restart_for_missing_market_data(tmp_path):
    p, hb = tmp_path/'rank.json', tmp_path/'heartbeat.json'
    live.main(['--database',str(tmp_path/'missing.sqlite'),'--output',str(p),'--heartbeat',str(hb)])
    assert json.loads(hb.read_bytes())['status'] == 'running'
    assert json.loads(hb.read_bytes())['rank_status'] == 'unavailable'


def test_dashboard_route_uses_fresh_read_boundary(monkeypatch):
    import oanda_practice_live_dashboard as dashboard
    result = {'status':'unavailable','reason':'rank_publication_stale_or_invalid'}
    monkeypatch.setattr(live,'read_current',lambda:result)
    class Request:
        path = '/api/currency-rank'
        def send_json(self, payload):
            self.payload = payload
    request = Request()
    dashboard.DashboardHandler.do_GET(request)
    assert request.payload == result


def test_dependency_change_refuses_display(monkeypatch,tmp_path):
    p = tmp_path/'rank.json'
    live.atomic_write(p,live.calculate(*inputs(),100000))
    monkeypatch.setattr(live,'DEPENDENCIES',{'oanda_currency_rank_model.py':'0'*64})
    assert live.read_current(p,100001)['status'] == 'unavailable'


def test_connected_partial_edges_rank_without_zero_filling():
    rows, mapped = inputs()
    del mapped['quotes']['CAD_CHF']
    result = live.calculate(rows,mapped,100000)['cohorts']['major8']
    assert result['status'] == 'ready' and result['coverage'] == 'partial'
    assert result['supported_pairs'] == 27
    assert 'CAD_CHF' not in result['support_edges']
    assert all(c['instrument'] != 'CAD_CHF' for c in result['pair_candidates'])


def test_isolated_currency_or_thin_graph_never_ranked():
    rows, mapped = inputs()
    for pair in list(mapped['quotes']):
        if 'NZD' in pair:
            del mapped['quotes'][pair]
    result = live.calculate(rows,mapped,100000)
    assert result['cohorts']['major8']['status'] == 'unavailable'
    assert result['cohorts']['full21']['status'] == 'unavailable'
