from copy import deepcopy
from decimal import Decimal, Context, localcontext
import pytest
import oanda_m1_risk_distribution_outcomes_v1 as o

START = 1800000000


def row(i, *, ba=True):
    close = Decimal('1.1') + Decimal(i)/10000
    def prices(value):
        return dict(open=str(value), high=str(value+Decimal('.0002')),
                    low=str(value-Decimal('.0002')), close=str(value))
    return dict(label_epoch=START+i*60, price_epoch=START+i*60+60,
        mid=prices(close), bid=prices(close-Decimal('.00005')) if ba else None,
        ask=prices(close+Decimal('.00005')) if ba else None,
        bid_ask_unavailable_reason=None if ba else 'real_bid_ask_OHLC_unavailable')


def capture(begin=0, end=61, *, read=None, observed=None, identity='a'):
    read = START+end*60+61 if read is None else read
    return dict(instrument='EUR_USD', rows=[row(i) for i in range(begin,end+1)],
        first_label_epoch=START+begin*60,last_label_epoch=START+end*60,
        source_read_completed_epoch=read,derived_observed_epoch=read if observed is None else observed,
        source_sha256=identity*64,receipt_sha256=identity*64,capture_sha256=identity*64,
        mapping_document={'mapping_sha256':identity*64})


def select(captures, h=15, origin=None, now=START+10000):
    return o.label_from_captures(origin or row(0),'EUR_USD',h,captures,as_of_epoch=now)


def test_exact_targets_all_labels_reuse_frozen_engine():
    source=capture(); out=select([source]); expected=o.labels.label_origin(source['rows'],0,15)
    assert all(v['status']=='scored' for v in out.values())
    assert {k:v['value_bps'] for k,v in out.items()}=={k:expected['values'][k] for k in o.labels.CONTINUOUS_LABELS}
    assert all(v['target_price_epoch']==START+16*60 for v in out.values())


def test_decisive_missing_exact_target_is_not_replaced_by_later_complete_source():
    first=capture(read=START+4000);first['rows']=[r for r in first['rows'] if r['label_epoch']!=START+15*60]
    later=capture(read=START+5000,identity='b')
    out=select([later,first]);assert all(v['reason_code']=='exact_target_missing' for v in out.values())
    assert all(v['source']['source_sha256']=='a'*64 for v in out.values())


def test_original_source_clock_not_later_derived_seal_controls_selection():
    first=capture(read=START+4000,observed=START+8000)
    later=capture(read=START+5000,observed=START+7000,identity='b')
    assert select([later,first])['signed_terminal_bps']['source']['source_sha256']=='a'*64


def test_missing_interior_keeps_endpoint_but_withholds_whole_path():
    source=capture();source['rows']=[r for r in source['rows'] if r['label_epoch']!=START+5*60]
    out=select([source]);assert out['signed_terminal_bps']['status']=='scored'
    assert out['mid_future_range_bps']['reason_code']=='complete_future_minute_path_missing'


def test_source_domains_not_merged_into_fictional_complete_query():
    out=select([capture(0,8),capture(9,61,identity='b')])
    assert out['signed_terminal_bps']['status']=='scored'
    assert out['mid_future_range_bps']['reason_code']=='no_attested_source_covers_required_domain'


def test_endpoint_and_path_can_select_distinct_earliest_decisive_sources():
    late_domain=capture(10,61,read=START+4000)
    full=capture(0,61,read=START+5000,identity='b')
    out=select([full,late_domain])
    assert out['signed_terminal_bps']['source']['source_sha256']=='a'*64
    assert out['long_close_MAE_bps']['source']['source_sha256']=='b'*64


def test_later_origin_revision_disclosed_but_original_reference_not_replaced():
    source=capture();source['rows'][0]['mid']['close']='1.1001'
    out=select([source]);expected=o.labels.label_origin([row(0),*source['rows'][1:]],0,15)
    assert out['signed_terminal_bps']['value_bps']==expected['values']['signed_terminal_bps']
    assert out['signed_terminal_bps']['source']['later_origin_equals_original'] is False


def test_source_need_not_contain_original_row_if_full_future_domain_is_attested():
    out=select([capture(1,61)])
    assert all(v['status']=='scored' for v in out.values())
    assert out['signed_terminal_bps']['source']['later_origin_present'] is False


def test_original_bar_high_low_never_leak_into_future_range():
    origin=row(0);origin['mid'].update(high='2',low='0.5')
    assert select([capture()],origin=origin)['mid_future_range_bps']['value_bps']==select([capture()])['mid_future_range_bps']['value_bps']


@pytest.mark.parametrize('which',['original','target','interior'])
def test_missing_bidask_remains_missing_without_losing_midpoint_endpoint(which):
    source=capture();origin=row(0)
    if which=='original':origin=row(0,ba=False)
    elif which=='target':source['rows'][15]=row(15,ba=False)
    else:source['rows'][5]=row(5,ba=False)
    out=select([source],origin=origin)
    assert out['signed_terminal_bps']['status']=='scored'
    assert out['mid_future_range_bps']['status']=='scored'
    assert out['long_close_MAE_bps']['reason_code']=='real_bid_ask_complete_path_unavailable'


def test_future_read_or_derived_observation_cannot_supply_earlier_result():
    assert select([capture()],now=START+900)['signed_terminal_bps']['status']=='pending'
    assert select([capture(observed=START+20000)])['signed_terminal_bps']['status']=='unavailable'


@pytest.mark.parametrize('h',[0,14,True,15.,61])
def test_only_exact_predeclared_horizons(h):
    with pytest.raises(ValueError,match='horizon'):select([capture()],h=h)


def test_quantile_metrics_are_pinball_and_interval_not_direction_probability():
    score=o._quantile_scores(2.,[0.,1.,3.])
    assert list(map(Decimal,score['pinball_loss_bps']))==list(map(Decimal,['0.2','0.5','0.1']))
    assert score['median_absolute_error_bps']=='1.0'
    assert score['interval_80_covered'] is True and 'brier' not in score
    with localcontext(Context(prec=2)):assert o._quantile_scores(2.,[0.,1.,3.])==score


def issued():
    nodes=[]
    for h in o.labels.HORIZONS:
        for name in o.labels.CONTINUOUS_LABELS:
            for method in o.METHODS:
                nodes.append(dict(horizon_minutes=h,horizon_sec=h*60,target_label_epoch=START+h*60,
                    target_price_epoch=START+(h+1)*60,label=name,method=method,
                    quantile_levels=list(o.QUANTILES),quantile_bps=[0.,10.,100.]))
    return dict(instrument='EUR_USD',origin_row=row(0),reference_label_epoch=START,
        reference_price_epoch=START+60,reference_mid=row(0)['mid']['close'],
        issued_epoch=START+65,issued_sha256='d'*64,cohort_id='synthetic_fixture_only',
        input_consumption={'read_completed_epoch':START+63},nodes=nodes)


@pytest.fixture
def scoring_seam(monkeypatch):
    monkeypatch.setattr(o,'_validate_chain',lambda value,*args,**kwargs:deepcopy(value))
    monkeypatch.setattr(o,'validate_future_capture',lambda value:deepcopy(value))


def score(value=None,captures=None,as_of=START+10000):
    return o.score_distribution_from_captures(value or issued(),
        {'publication_completed_epoch':START+66},{'read_completed_epoch':START+67},
        [capture()] if captures is None else captures,input_raw=b'',input_receipt={},metadata={},fit_raw=b'',
        expected_source_bindings={'synthetic_only':'e'*64},as_of_epoch=as_of,clock=lambda:as_of+1)


def test_all72_nodes_original_clock_and_source_bindings(scoring_seam):
    out=score();assert out['status_counts']=={'scored':72}
    assert out['unique_capture_count']==1 and out['evaluation_completed_epoch']>out['as_of_epoch']
    assert out['manager_efficacy_claim'] is False and out['broker_requests'] is False


def test_duplicate_derived_captures_do_not_inflate_source_count(scoring_seam):
    first=capture();later=deepcopy(first);later['derived_observed_epoch']+=1;later['capture_sha256']='f'*64
    out=score(captures=[later,first]);assert out['unique_capture_count']==1


def test_same_source_conflicting_mapping_rejected(scoring_seam):
    first=capture();other=deepcopy(first);other['rows'][0]['mid']['close']='1.1001'
    with pytest.raises(ValueError,match='conflicting_same_source_mapping'):score(captures=[first,other])


def test_future_chain_refused_even_when_targets_not_mature(scoring_seam):
    with pytest.raises(ValueError,match='chain_unavailable'):score(as_of=START+66.5)


@pytest.mark.parametrize('field,value',[('target_price_epoch',START+961),('horizon_sec',901),('quantile_levels',[.2,.5,.8])])
def test_original_target_and_quantiles_cannot_change(scoring_seam,field,value):
    record=issued();record['nodes'][0][field]=value
    with pytest.raises(ValueError,match='original_node'):score(record)


def test_repeated_reports_and_repeat_origin_never_inflate_denominator(scoring_seam):
    first=score();later=score(as_of=START+10001)
    repeat=issued();repeat['issued_sha256']='f'*64;repeat['issued_epoch']+=1
    duplicate=score(repeat)
    out=o.aggregate_reports([first,later,duplicate],as_of_epoch=START+20000)
    assert out['unique_issued_count']==2 and out['unique_pair_origin_count']==1
    assert len(out['repeated_origin_issues'])==1
    assert all(r['scored_count']==1 for r in out['groups'])
    assert all(r['matched_origin_count']==1 for r in out['paired_methods'])
    assert out['independent_sample_size'] is None


def test_missing_outcomes_are_null_metrics_not_measured_zero(scoring_seam):
    out=score(captures=[]);assert all(r['metrics'] is None for r in out['nodes'])
    agg=o.aggregate_reports([out],as_of_epoch=START+20000)
    assert all(r['mean_median_absolute_error_bps'] is None and r['scored_count']==0 for r in agg['groups'])
    assert agg['paired_methods']==[]


def test_aggregate_rejects_tamper_future_clock_and_changed_source(scoring_seam):
    report=score();report['nodes'][0]['metrics']['median_absolute_error_bps']='999'
    with pytest.raises(ValueError,match='schema_or_seal'):o.aggregate_reports([report],as_of_epoch=START+20000)
    with pytest.raises(ValueError,match='future_report'):o.aggregate_reports([score()],as_of_epoch=START+9999)


@pytest.mark.parametrize('which',['cohort','node_inventory','metric','missingness','target'])
def test_resealed_report_cannot_mix_cohorts_or_rewrite_metrics(scoring_seam,which):
    first=score();other=deepcopy(first)
    if which=='cohort':other['cohort_id']='different_cohort'
    elif which=='node_inventory':other['nodes'][0]['method']=o.METHODS[1]
    elif which=='metric':other['nodes'][0]['metrics']['median_absolute_error_bps']='999'
    elif which=='target':other['nodes'][0]['label_outcome']['target_price_epoch']+=60
    else:other['nodes'][0]['label_outcome']['status']='pending'
    other=o.sealed({k:v for k,v in other.items() if k!='report_sha256'},'report_sha256')
    with pytest.raises(ValueError):o.aggregate_reports([first,other],as_of_epoch=START+20000)


def test_actual_mapper_replay_raw_and_receipt_observation(monkeypatch):
    import test_oanda_m1_mba_research_capture_v1 as fixture
    raw,receipt,mapping=fixture.make_capture(monkeypatch)
    value=o.capture_future_m1(raw,receipt,fixture.metadata(),clock=lambda:fixture.NOW+1)
    assert o.validate_future_capture(value)==value
    assert value['source_read_completed_epoch']==receipt['read_completed_epoch']
    assert value['derived_observed_epoch']>value['source_read_completed_epoch']
    assert value['rows'][-1]['price_epoch']==mapping['complete_rows'][-1]['price_epoch']
    assert value['mapping_document']['semantics']['midpoint_training_ingestion_equivalence_proven'] is False
    with pytest.raises(ValueError,match='derived_observation_before_source'):
        o.capture_future_m1(raw,receipt,fixture.metadata(),clock=lambda:receipt['read_completed_epoch'])


def test_derived_clock_follows_parser_and_body_calculation(monkeypatch):
    import test_oanda_m1_mba_research_capture_v1 as fixture
    raw,receipt,_=fixture.make_capture(monkeypatch)
    now=[fixture.NOW+1];parse=o.labels.parse_csv
    def later_parse(*args,**kwargs):
        result=parse(*args,**kwargs);now[0]+=2;return result
    monkeypatch.setattr(o.labels,'parse_csv',later_parse)
    value=o.capture_future_m1(raw,receipt,fixture.metadata(),clock=lambda:now[0])
    assert value['derived_observed_epoch']==fixture.NOW+3
    assert value['source_read_completed_epoch']==receipt['read_completed_epoch']


@pytest.mark.parametrize('field',['instrument','reference','issue_clock','consumer_clock','quantiles','publication'])
def test_repeated_issued_identity_cannot_change_across_later_report(scoring_seam,field):
    first=score();later=score(as_of=START+10001)
    if field=='instrument':later['instrument']='GBP_USD'
    elif field=='reference':later['reference_label_epoch']-=60
    elif field=='issue_clock':later['original_issued_epoch']+=.1
    elif field=='consumer_clock':later['original_consumed_epoch']+=.1
    elif field=='publication':later['publication_sha256']='f'*64
    else:
        later['nodes'][0]['quantile_bps']=[0.,11.,100.]
        later['nodes'][0]['metrics']=o._quantile_scores(later['nodes'][0]['label_outcome']['value_bps'],later['nodes'][0]['quantile_bps'])
    later=o.sealed({k:v for k,v in later.items() if k!='report_sha256'},'report_sha256')
    with pytest.raises(ValueError,match='same_issued_identity_redefined'):
        o.aggregate_reports([first,later],as_of_epoch=START+20000)


@pytest.mark.parametrize('which',['price','availability','domain'])
def test_resealed_capture_cannot_rewrite_source_derived_semantics(monkeypatch,which):
    import test_oanda_m1_mba_research_capture_v1 as fixture
    raw,receipt,_=fixture.make_capture(monkeypatch)
    value=o.capture_future_m1(raw,receipt,fixture.metadata(),clock=lambda:fixture.NOW+1)
    if which=='price':value['rows'][-1]['mid']['close']='1.1'
    elif which=='availability':value['source_read_completed_epoch']+=.01
    else:value['first_label_epoch']-=60
    value=o.sealed({k:v for k,v in value.items() if k!='capture_sha256'},'capture_sha256')
    with pytest.raises(ValueError,match='capture_raw_mapping_replay'):o.validate_future_capture(value)


@pytest.fixture
def real_chain(monkeypatch):
    import test_oanda_m1_mba_research_capture_v1 as fixture
    import oanda_m1_risk_distribution_contract_v1 as c
    monkeypatch.setattr(fixture,'NOW',START+18061.)
    raw,receipt,_=fixture.make_capture(monkeypatch)
    meta=fixture.metadata();fits={}
    for h in o.labels.HORIZONS:
        for name in o.labels.CONTINUOUS_LABELS:
            fits[f'EUR_USD/{h}/{name}']=dict(status='fitted_training_quantiles',
                fit_scope='training_only_common_support',method='inverted_cdf',original_probability_available=False,
                quantiles=list(o.QUANTILES),training_rows=300,training_origin_sha256='a'*64,
                training_reference_max_epoch=START-100000,training_target_max_epoch=START-90000,
                empirical=[-1.,0.,1.] if name=='signed_terminal_bps' else [0.,1.,2.],
                volatility_normalized=[-.1,0.,.1] if name=='signed_terminal_bps' else [0.,.1,.2])
    artifact=dict(created_epoch=START-80000,fits=fits,**o.labels.FLAGS)
    fit_raw=c.canonical(artifact)
    # Explicit synthetic training artifact identity seam; all numerical and
    # source/publication/consumer replay is the actual new contract.
    monkeypatch.setattr(c,'TRAINING_ARTIFACT_SHA256',c.sha(fit_raw))
    bindings={'oanda_synthetic_fixture.py':'a'*64}
    consumed=dict(raw_sha256=c.sha(raw),receipt_sha256=c.digest(receipt),
        read_started_epoch=fixture.NOW+.3,read_completed_epoch=fixture.NOW+.4)
    ticks=iter([fixture.NOW+.5,fixture.NOW+.6])
    value=c.issue_distribution(raw,receipt,meta,fit_raw,input_consumption=consumed,cohort_id=c.COHORT_ID,
        scheduled_reference_price_epoch=START+18060,expected_source_bindings=bindings,clock=lambda:next(ticks))
    pub=c.publication_receipt(value,persisted_bytes_sha256=c.digest(value),publication_started_epoch=fixture.NOW+.7,
        expected_source_bindings=bindings,clock=lambda:fixture.NOW+.8)
    consume=c.consumption_receipt(value,pub,read_started_epoch=fixture.NOW+.9,
        expected_source_bindings=bindings,clock=lambda:fixture.NOW+1)
    monkeypatch.setattr(fixture,'NOW',fixture.NOW+3660)
    future_raw,future_receipt,_=fixture.make_capture(monkeypatch)
    future=o.capture_future_m1(future_raw,future_receipt,meta,clock=lambda:fixture.NOW+1)
    return dict(issued=value,publication=pub,consumption=consume,captures=[future],input_raw=raw,input_receipt=receipt,
        metadata=meta,fit_raw=fit_raw,expected_source_bindings=bindings,as_of_epoch=fixture.NOW+2,clock=lambda:fixture.NOW+3)


def test_real_mapper_contract_issue_publication_consumer_and_all72_outcomes(real_chain):
    out=o.score_distribution_from_captures(**real_chain)
    assert out['status_counts']=={'scored':72}
    assert all(r['label_outcome']['source']['original_origin_preserved'] is True for r in out['nodes'])
    assert o.aggregate_reports([out],as_of_epoch=real_chain['as_of_epoch']+10)['unique_pair_origin_count']==1


@pytest.mark.parametrize('field',['publication','consumption','issued'])
def test_real_chain_missing_or_tampered_stage_cannot_score(real_chain,field):
    real_chain[field]=deepcopy(real_chain[field]);real_chain[field]['source_bindings']={}
    with pytest.raises(ValueError):o.score_distribution_from_captures(**real_chain)
